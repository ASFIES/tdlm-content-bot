import os
import time
import json
import base64
import gspread
from google.oauth2.service_account import Credentials

def with_backoff(fn, *args, **kwargs):
    delay = 0.8
    for i in range(6):
        try:
            return fn(*args, **kwargs)
        except Exception:
            if i == 5:
                raise
            time.sleep(delay)
            delay *= 1.8

def _load_creds_info():
    raw = os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
    if not raw:
        raise RuntimeError("Falta GOOGLE_CREDENTIALS_JSON")
    # acepta JSON directo o base64
    if raw.startswith("{"):
        return json.loads(raw)
    try:
        decoded = base64.b64decode(raw).decode("utf-8")
        return json.loads(decoded)
    except Exception:
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON no es JSON ni base64 válido")

def get_gspread_client():
    info = _load_creds_info()
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def open_spreadsheet(sheet_name: str):
    gc = get_gspread_client()
    return with_backoff(gc.open, sheet_name)

def open_worksheet(sh, tab_name: str):
    return with_backoff(sh.worksheet, tab_name)

def get_all_values_safe(ws):
    try:
        return with_backoff(ws.get_all_values)
    except Exception:
        return []

def build_header_map(ws):
    hdr = with_backoff(ws.row_values, 1)
    m = {}
    for i, name in enumerate(hdr, start=1):
        if name and str(name).strip():
            m[str(name).strip()] = i
    return m

def col_idx(hmap, name: str):
    return hmap.get(name)

def row_to_dict(header, row):
    d = {}
    for i, h in enumerate(header):
        if not h:
            continue
        d[str(h).strip()] = (row[i] if i < len(row) else "")
    return d

def update_row_cells(ws, row_num: int, updates: dict, hmap=None):
    if hmap is None:
        hmap = build_header_map(ws)

    # construir batch update (range por celda)
    for k, v in updates.items():
        c = col_idx(hmap, k)
        if not c:
            continue
        with_backoff(ws.update_cell, row_num, c, v)
