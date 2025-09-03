#!/usr/bin/env bash
set -euxo pipefail

PWB="/opt/render/project/src/.cache/ms-playwright"
VENV="/opt/render/project/src/.venv"
PROXY="http://sprufb1u6o:vIg23nhNmScp1q%7EJ9l@gate.decodo.com:10004"

export PLAYWRIGHT_BROWSERS_PATH="$PWB"

# 1) venv
python -m venv "$VENV"

# 2) pip WITHOUT any proxies
env -i PATH="$PATH" HOME="$HOME" \
  "$VENV/bin/python" -m pip install --upgrade pip setuptools wheel

env -i PATH="$PATH" HOME="$HOME" \
  "$VENV/bin/python" -m pip install --no-cache-dir -r requirements.txt

# 3) Playwright: try direct first, fall back to proxy only if needed
install_playwright () {
  local mode="$1"
  if [[ "$mode" == "proxy" ]]; then
    echo ">>> Playwright install via Decodo proxy..."
    env -i PATH="$PATH" HOME="$HOME" \
      PLAYWRIGHT_BROWSERS_PATH="$PWB" NODE_OPTIONS="--dns-result-order=ipv4first" \
      HTTP_PROXY="$PROXY"  http_proxy="$PROXY" \
      HTTPS_PROXY="$PROXY" https_proxy="$PROXY" \
      ALL_PROXY="$PROXY"  all_proxy="$PROXY" \
      "$VENV/bin/python" -m playwright install chromium
  else
    echo ">>> Playwright install direct (no proxy)..."
    env -i PATH="$PATH" HOME="$HOME" \
      PLAYWRIGHT_BROWSERS_PATH="$PWB" NODE_OPTIONS="--dns-result-order=ipv4first" \
      "$VENV/bin/python" -m playwright install chromium
  fi
}

if ! install_playwright direct; then
  echo "Direct download failed — falling back to proxy."
  install_playwright proxy
fi

# 4) sanity
"$VENV/bin/python" - <<'PY'
import os, sys, playwright
print("OK Playwright", playwright.__version__)
print("BROWSERS_PATH", os.environ.get("PLAYWRIGHT_BROWSERS_PATH"))
PY
