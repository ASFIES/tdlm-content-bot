import os
import json
from datetime import datetime
from zoneinfo import ZoneInfo

from openai import OpenAI

from utils.sheets import (
    open_spreadsheet, open_worksheet, build_header_map, col_idx,
    get_all_values_safe, row_to_dict, with_backoff, update_row_cells
)
from utils.wp import WordPressClient

MX_TZ = ZoneInfo(os.environ.get("TZ", "America/Mexico_City").strip() or "America/Mexico_City")

CONTENT_SHEET_NAME = os.environ.get("CONTENT_SHEET_NAME", "").strip()
TAB_CONTENT_PLAN = os.environ.get("TAB_CONTENT_PLAN", "Content_Plan").strip()
TAB_KNOWLEDGE = os.environ.get("TAB_KNOWLEDGE", "Conocimiento_AI").strip()

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini").strip()

WP_BASE_URL = os.environ.get("WP_BASE_URL", "").strip()
WP_USER = os.environ.get("WP_USER", "").strip()
WP_APP_PASSWORD = os.environ.get("WP_APP_PASSWORD", "").strip()
DEFAULT_WP_STATUS = (os.environ.get("DEFAULT_WP_STATUS", "draft").strip() or "draft")

CTA_WHATSAPP = os.environ.get("CTA_WHATSAPP", "").strip()  # opcional: link wa.me
CTA_ABOGADOS_URL = os.environ.get("CTA_ABOGADOS_URL", "https://tuderecholaboralmexico.com/abogados/").strip()

def now_iso():
    return datetime.now(MX_TZ).strftime("%Y-%m-%dT%H:%M:%S%z")

def _norm(s: str) -> str:
    return (s or "").strip()

def _pick_knowledge(knowledge_rows: list[dict], tema: str, palabras: str, id_tema_ai: str) -> list[dict]:
    if id_tema_ai:
        for r in knowledge_rows:
            if _norm(r.get("ID_Tema")) == _norm(id_tema_ai):
                return [r]

    q = f"{tema} {palabras}".lower()
    q_tokens = set([t for t in q.replace(",", " ").split() if len(t) >= 4])

    scored = []
    for r in knowledge_rows:
        text = f"{r.get('Titulo_Visible','')} {r.get('Palabras_Clave','')} {r.get('Contenido_Legal','')}".lower()
        tokens = set([t for t in text.replace(",", " ").split() if len(t) >= 4])
        score = len(q_tokens.intersection(tokens))
        if score > 0:
            scored.append((score, r))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [x[1] for x in scored[:2]]  # top 2

def _compose_prompt(tema: str, palabras: str, knowledge: list[dict]) -> str:
    base = ""
    for k in knowledge:
        base += (
            f"- TEMA_BASE: {k.get('Titulo_Visible','')}\n"
            f"  CONTENIDO_LEGAL: {k.get('Contenido_Legal','')}\n"
            f"  FUENTE: {k.get('Fuente','')}\n\n"
        )

    cta = ""
    if CTA_WHATSAPP:
        cta = f"CTA WhatsApp: {CTA_WHATSAPP}"
    else:
        cta = f"CTA Abogados: {CTA_ABOGADOS_URL}"

    return f"""
Eres redactora legal y consultiva del despacho "Tu Derecho Laboral México".
Objetivo: crear un borrador de blog MUY humano, sensible y útil para una persona trabajadora.
Debe ser orientativo, sin prometer resultados, y con disclaimer claro: "orientación informativa; no constituye asesoría legal".

TEMA: {tema}
PALABRAS_CLAVE: {palabras}

BASE (Conocimiento_AI; úsala como fundamento y NO inventes leyes):
{base}

FORMATO:
- Devuelve ÚNICAMENTE JSON válido con estas llaves:
  {{
    "title": "...",
    "excerpt": "...",
    "html": "..."
  }}
- "html" debe incluir:
  - H2/H3, bullets, y una sección "Qué hacer hoy"
  - una sección "Documentos que ayudan"
  - una sección "Errores comunes"
  - cierre con CTA: {cta}
  - tono McKinsey: ordenado, claro, ejecutivo pero empático

RESTRICCIONES:
- Español (México)
- Nada de "garantizamos" / nada de promesas
- No des consejos ilegales
- Incluye disclaimer al final
""".strip()

def _openai_generate_post(tema: str, palabras: str, knowledge: list[dict]) -> dict:
    if not OPENAI_API_KEY:
        raise RuntimeError("Falta OPENAI_API_KEY")

    client = OpenAI(api_key=OPENAI_API_KEY)
    prompt = _compose_prompt(tema, palabras, knowledge)

    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[
            {"role": "system", "content": "Responde siempre en JSON válido, sin markdown."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.5,
    )

    content = (resp.choices[0].message.content or "").strip()
    try:
        return json.loads(content)
    except Exception:
        # fallback: si vino texto, no tiramos el flujo sin razón
        return {
            "title": tema[:120],
            "excerpt": "Guía informativa y orientativa sobre tu situación laboral.",
            "html": f"<p>{content}</p>"
        }

def run_once():
    if not CONTENT_SHEET_NAME:
        raise RuntimeError("Falta CONTENT_SHEET_NAME")

    sh = open_spreadsheet(CONTENT_SHEET_NAME)
    ws_plan = open_worksheet(sh, TAB_CONTENT_PLAN)

    values = get_all_values_safe(ws_plan)
    if not values or len(values) < 2:
        return {"status": "no_rows"}

    hdr = values[0]
    rows = values[1:]

    # buscar primera fila READY
    target_idx = None
    target_row = None
    for i, r in enumerate(rows, start=2):  # row number in sheet
        d = row_to_dict(hdr, r)
        if _norm(d.get("Estatus")).upper() == "READY":
            target_idx = i
            target_row = d
            break

    if not target_idx:
        return {"status": "nothing_ready"}

    h = build_header_map(ws_plan)

    # marcar RUNNING
    update_row_cells(ws_plan, target_idx, {
        "Estatus": "RUNNING",
        "Ultimo_Error": "",
        "Actualizado_En": now_iso(),
    }, hmap=h)

    tema = _norm(target_row.get("Tema"))
    palabras = _norm(target_row.get("Palabras_Clave"))
    wp_cat = _norm(target_row.get("WP_Categoria"))
    wp_status = _norm(target_row.get("WP_Estatus")) or DEFAULT_WP_STATUS
    id_tema_ai = _norm(target_row.get("ID_Tema_AI"))

    # leer conocimiento
    ws_k = open_worksheet(sh, TAB_KNOWLEDGE)
    k_values = get_all_values_safe(ws_k)
    knowledge_rows = []
    if k_values and len(k_values) >= 2:
        kh = k_values[0]
        for rr in k_values[1:]:
            knowledge_rows.append(row_to_dict(kh, rr))

    picked = _pick_knowledge(knowledge_rows, tema, palabras, id_tema_ai)

    # GPT genera post
    post = _openai_generate_post(tema, palabras, picked)
    title = _norm(post.get("title")) or tema
    excerpt = _norm(post.get("excerpt")) or ""
    html = _norm(post.get("html")) or ""

    # publicar en WordPress
    if not (WP_BASE_URL and WP_USER and WP_APP_PASSWORD):
        raise RuntimeError("Faltan variables WP_BASE_URL / WP_USER / WP_APP_PASSWORD")

    wp = WordPressClient(WP_BASE_URL, WP_USER, WP_APP_PASSWORD)
    cat_id = None
    if wp_cat:
        cat_id = wp.get_or_create_category(wp_cat)

    created = wp.create_post(
        title=title,
        content_html=html,
        status=wp_status,
        excerpt=excerpt,
        category_id=cat_id
    )

    post_id = created.get("id")
    link = created.get("link") or ""

    # actualizar plan
    update_row_cells(ws_plan, target_idx, {
        "Estatus": "PUBLISHED" if wp_status == "publish" else "PUBLISHED",
        "Titulo_Final": title,
        "URL_Publicado": link,
        "WP_Post_ID": str(post_id or ""),
        "Ultimo_Error": "",
        "Actualizado_En": now_iso(),
    }, hmap=h)

    return {"status": "ok", "row": target_idx, "wp_post_id": post_id, "link": link}
