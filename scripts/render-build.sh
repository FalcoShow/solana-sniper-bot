#!/usr/bin/env bash
set -euxo pipefail

# Where Playwright caches downloaded browsers
export PLAYWRIGHT_BROWSERS_PATH="/opt/render/.cache/ms-playwright"

# Use a venv that ships with your deploy
PYV="/opt/render/project/src/.venv"
python -m venv "$PYV"
"$PYV/bin/python" -m pip install --upgrade pip setuptools wheel
"$PYV/bin/python" -m pip install --no-cache-dir -r requirements.txt

# Your proxy (already URL-encoded ~ as %7E)
PROXY="${PLAYWRIGHT_PROXY:-http://sprufb1u6o:vIg23nhNmScp1q%7EJ9l@gate.decodo.com:10004}"

# Two mirrors; try Azure edge first, then CDN
URL1="https://playwright.azureedge.net/builds/chromium/1187/chromium-linux.zip"
URL2="https://cdn.playwright.dev/builds/chromium/1187/chromium-linux.zip"
mkdir -p "$PLAYWRIGHT_BROWSERS_PATH/chromium-1187"

# Download Chromium via your proxy
curl -fsSL4 --connect-timeout 20 --max-time 900 --retry 5 --retry-connrefused --retry-delay 2 \
  -x "$PROXY" -o /tmp/chromium.zip "$URL1" \
|| curl -fsSL4 --connect-timeout 20 --max-time 900 --retry 5 --retry-connrefused --retry-delay 2 \
  -x "$PROXY" -o /tmp/chromium.zip "$URL2"

# Unpack exactly where Playwright expects
"$PYV/bin/python" - <<'PY'
import os, zipfile
dest = os.path.join(os.environ['PLAYWRIGHT_BROWSERS_PATH'], 'chromium-1187')
zipfile.ZipFile('/tmp/chromium.zip').extractall(dest)
print('Unpacked to', dest)
PY

# Let Playwright register the already-downloaded browser (no download)
"$PYV/bin/python" -m playwright install chromium || true
