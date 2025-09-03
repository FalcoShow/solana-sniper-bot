#!/usr/bin/env bash
set -euo pipefail

# Disable proxies for build time
for v in HTTP_PROXY http_proxy HTTPS_PROXY https_proxy ALL_PROXY all_proxy NO_PROXY no_proxy; do
  if [[ -n "${!v:-}" ]]; then export "ORIG_$v=${!v}"; fi
  unset "$v" || true
done
export NO_PROXY="127.0.0.1,localhost,::1,.render.internal"

echo ">>> creating venv + installing python deps..."
python -m venv .venv
source .venv/bin/activate

# Speed up/quiet pip a bit
export PIP_DISABLE_PIP_VERSION_CHECK=1
python -m pip install --upgrade pip setuptools wheel

# If you use Poetry, keep your existing poetry install line; otherwise requirements.txt is fine:
if [[ -f poetry.lock ]]; then
  poetry config virtualenvs.create false
  poetry install --no-interaction --no-ansi
elif [[ -f requirements.txt ]]; then
  pip install -r requirements.txt
fi

# Optional: cache PW browser at build; if it fails, ignore (runtime will self-install)
python -m playwright install-deps || true
python -m playwright install chromium || true

echo ">>> build step finished"
