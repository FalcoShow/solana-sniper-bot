# Procfile — God-Tier Overhaul (single source of truth)
# Purpose:
#   - Keep one canonical bootstrap path: start.sh (env load → gates → supervisor → run module)
#   - Portable across Procfile-based runners (Heroku-ish tooling, local proc managers, etc.)
#
# Notes:
#   - Render typically uses render.yaml "startCommand" and may ignore Procfile if overridden in dashboard.
#   - We still keep this for portability + local tooling parity.
#   - Use `bash` explicitly to avoid exec-bit / line-ending pitfalls.

worker: bash ./start.sh

# Optional web process (ONLY if you actually run a web server module, e.g., app.py / FastAPI).
# Keep commented unless you deploy a separate web service.
# Examples (choose ONE pattern that matches your repo wiring):
#
# 1) If you have app.py (module: app):
# web: RUN_MODULE_OVERRIDE=app bash ./start.sh
#
# 2) If you have a scripts.health_http module that serves HTTP:
# web: RUN_MODULE_OVERRIDE=scripts.health_http bash ./start.sh
#
# 3) If your web server needs args, use RUN_ARGS (simple string split) or RUN_ARGS_JSON:
# web: RUN_MODULE_OVERRIDE=app RUN_ARGS="--host 0.0.0.0 --port ${PORT:-8000}" bash ./start.sh
