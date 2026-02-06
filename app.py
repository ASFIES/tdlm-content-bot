import os
from flask import Flask, request, jsonify, make_response

from content_bot import run_once

app = Flask(__name__)

JOB_TOKEN = os.environ.get("JOB_TOKEN", "").strip()

def _cors_json(payload, status=200):
    resp = make_response(jsonify(payload), status)
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type, X-Job-Token"
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    return resp

@app.get("/")
def home():
    return _cors_json({
        "ok": True,
        "service": "tdlm-content-bot",
        "hint": "Usa POST /run_once para publicar 1 fila READY."
    })

@app.get("/health")
def health():
    return _cors_json({"ok": True})

@app.route("/run_once", methods=["POST", "OPTIONS"])
def run_once_route():
    if request.method == "OPTIONS":
        return _cors_json({"ok": True})

    if JOB_TOKEN:
        got = (request.headers.get("X-Job-Token") or "").strip()
        if got != JOB_TOKEN:
            return _cors_json({"ok": False, "error": "unauthorized"}, 401)

    try:
        result = run_once()
        return _cors_json({"ok": True, "result": result}, 200)
    except Exception as e:
        app.logger.exception(f"[ERROR] run_once: {e}")
        return _cors_json({"ok": False, "error": f"{type(e).__name__}: {e}"}, 500)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
