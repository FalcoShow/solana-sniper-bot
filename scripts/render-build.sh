#!/usr/bin/env bash
set -euo pipefail

# Paths
VENV="/opt/render/project/src/.venv"
PWB="/opt/render/project/src/.cache/ms-playwright"
export PLAYWRIGHT_BROWSERS_PATH="$PWB"

echo ">>> creating venv + installing python deps..."
python -m venv "$VENV"
"$VENV/bin/python" -m pip install --upgrade pip setuptools wheel
# respect any PIP_* envs you set in Render (e.g., PIP_ONLY_BINARY)
"$VENV/bin/python" -m pip install --no-cache-dir -r requirements.txt

echo ">>> installing Playwright browsers (no proxies during install)..."
# Clear env so Node/Playwright doesn't inherit HTTP(S)_PROXY
env -i \
  PATH="/usr/local/bin:/usr/bin:/bin:/usr/local/games:/usr/games" \
  HOME="/opt/render" \
  PLAYWRIGHT_BROWSERS_PATH="$PWB" \
  NODE_OPTIONS="--dns-result-order=ipv4first" \
  "$VENV/bin/python" -m playwright install chromium

echo ">>> versions:"
"$VENV/bin/python" - <<'PY'
import sys, pkgutil
print("PYTHON:", sys.version)
print("EXE   :", sys.executable)
print("has_aiohttp:", "aiohttp" in {m.name for m in pkgutil.iter_modules()})
import playwright
print("playwright pkg ok")
PY
