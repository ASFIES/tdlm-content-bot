# src/utils/sheets.py
import os
import json
import time
import random
import base64
import ast
from typing import Any, Dict, Optional, Callable

import gspread
from google.oauth2.service_account import Credentials


DEFAULT_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name, default) or "").strip()


def _strip_wrapping_quotes(s: str) -> str:
    s = (s or "").strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"'):
        return s[1:-1].strip()
    return s


def _looks_base64(s: str) -> bool:
    s = (s or "").strip()
    if len(s) < 40:
        return False
    # heurística simple: base64 suele ser alfanum + / + + + =, sin llaves
    if "{" in s or "}" in s:
        return False
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=\n\r")
    return all(c in allowed for c in s)


def _try_decode_b64(s: str) -> Optional[str]:
    try:
        raw = base64.b64decode(s).decode("utf-8", errors="replace").strip()
        return raw
    except Exception:
        return None


def _load_creds_info() -> Dict[str, Any]:
    """
    Lee credenciales de Google Service Account desde variables de entorno.
    Acepta:
      - JSON válido (comillas dobles)
      - dict tipo Python (comillas simples) mediante ast.literal_eval
      - base64 (opcional)
      - ruta a archivo .json (opcional)
    """
    raw = _env("GOOGLE_CREDENTIALS_JSON") or _env("GOOGLE_CREDENTIALS")

    if not raw:
        raw_b64 = _env("GOOGLE_CREDENTIALS_B64")
        if raw_b64:
            decoded = _try_decode_b64(raw_b64)
            if decoded:
                raw = decoded

    if not raw:
        raise RuntimeError(
            "Faltan credenciales. Define GOOGLE_CREDENTIALS_JSON (o GOOGLE_CREDENTIALS)."
        )

    raw = _strip_wrapping_quotes(raw)

    # Si te pasaran una ruta a archivo, también lo soportamos
    if raw.lower().endswith(".json") and os.path.exists(raw):
        with open(raw, "r", encoding="utf-8") as f:
            raw = f.read().strip()

    # Si parece base64, intenta decodificar y luego parsear
    if _looks_base64(raw):
        decoded = _try_decode_b64(raw)
        if decoded and "{" in decoded:
            raw = decoded

    # 1) Intento JSON normal
    try:
        info = json.loads(raw)
        if not isinstance(info, dict):
            raise RuntimeError("Las credenciales no son un objeto JSON (dict).")
        return info
    except json.JSONDecodeError:
        pass

    # 2) Intento dict estilo Python (comillas simples)
    try:
        info = ast.literal_eval(raw)
        if not isinstance(info, dict):
            raise RuntimeError("Las credenciales no son un dict.")
        return info
    except Exception as e:
        snippet = raw[:200].replace("\n", "\\n")
        raise RuntimeError(
            "GOOGLE_CREDENTIALS_JSON inválido. Debe ser JSON real (comillas dobles) "
            "o un dict Python. Inicio recibido: "
            f"{snippet}"
        ) from e


def get_gspread_client(scopes: Optional[list] = None) -> gspread.Client:
    scopes = scopes or DEFAULT_SCOPES
    info = _load_creds_info()
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def with_backoff(fn: Callable, *args, tries: int = 5, base_sleep: float = 0.7, **kwargs):
    """
    Reintentos simples con backoff para llamadas a gspread/Google APIs.
    """
    last_err = None
    for i in range(tries):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            last_err = e
            sleep_s = base_sleep * (2 ** i) + random.random() * 0.25
            time.sleep(sleep_s)
    raise last_err


def open_spreadsheet(name_or_key_or_url: str) -> gspread.Spreadsheet:
    gc = get_gspread_client()
    s = (name_or_key_or_url or "").strip()

    # URL tipo https://docs.google.com/spreadsheets/d/<KEY>/edit...
    if "docs.google.com" in s and "/spreadsheets/d/" in s:
        return with_backoff(gc.open_by_url, s)

    # Key típica (no perfecta, pero funciona bien)
    if len(s) >= 25 and all(c.isalnum() or c in "-_" for c in s):
        # intenta por key primero; si falla, cae a open(name)
        try:
            return with_backoff(gc.open_by_key, s)
        except Exception:
            pass

    return with_backoff(gc.open, s)


def build_header_map(ws: gspread.Worksheet) -> Dict[str, int]:
    """
    Lee la primera fila como encabezados y devuelve map: header -> índice (1-based).
    """
    headers = with_backoff(ws.row_values, 1)
    return {h.strip(): (i + 1) for i, h in enumerate(headers) if (h or "").strip()}


def col_idx(header_map: Dict[str, int], header_name: str) -> int:
    """
    Devuelve índice 1-based de una columna por nombre de encabezado.
    """
    key = (header_name or "").strip()
    if key in header_map:
        return header_map[key]
    # tolera match case-insensitive
    low = key.lower()
    for k, v in header_map.items():
        if (k or "").strip().lower() == low:
            return v
    raise KeyError(f"Columna no encontrada en encabezados: {header_name}")
