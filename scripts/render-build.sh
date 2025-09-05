#!/usr/bin/env bash
set -euo pipefail

# ---------- Save & clear proxies during build ----------
PROXY_VARS=(HTTP_PROXY http_proxy HTTPS_PROXY https_proxy ALL_PROXY all_proxy NO_PROXY no_proxy)
for v in "${PROXY_VARS[@]}"; do
  if [[ -n "${!v:-}" ]]; then
    export "ORIG_$v=${!v}"
  fi
  unset "$v" || true
done
export NO_PROXY="127.0.0.1,localhost,::1,.render.internal"

echo ">>> creating venv + installing python deps..."
python -m venv .venv
source .venv/bin/activate

# Speed up/quiet pip
export PIP_DISABLE_PIP_VERSION_CHECK=1
python -m pip install --upgrade pip setuptools wheel

# Poetry if present; otherwise requirements.txt
if [[ -f poetry.lock ]]; then
  python -m pip install "poetry==2.1.3" || true
  poetry config virtualenvs.create false
  poetry install --no-interaction --no-ansi
elif [[ -f requirements.txt ]]; then
  pip install -r requirements.txt
fi

# Optional: Playwright bits (ignore failure on install-deps since we're not root)
python -m playwright install-deps || true
python -m playwright install chromium || true

echo ">>> build step finished"

# ---------- Restore proxies (correct indirect expansion) ----------
for v in "${PROXY_VARS[@]}"; do
  ov="ORIG_$v"
  if [[ -n "${!ov:-}" ]]; then
    export "$v=${!ov}"
  fi
done
