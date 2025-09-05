#!/usr/bin/env bash
set -euxo pipefail

# ---- Keep build clean of outbound proxies (can break pip & PW downloads)
# Save and unset any proxy vars for the duration of the build.
for v in HTTP_PROXY http_proxy HTTPS_PROXY https_proxy ALL_PROXY all_proxy NO_PROXY no_proxy; do
  if [[ -n "${!v:-}" ]]; then export "ORIG_$v=${!v}"; fi
  unset "$v" || true
done
export NO_PROXY="127.0.0.1,localhost,::1,.render.internal"

# ---- Optional: ensure PW browser cache path is consistent with runtime
export PLAYWRIGHT_BROWSERS_PATH="${PLAYWRIGHT_BROWSERS_PATH:-/opt/render/project/src/.cache/ms-playwright}"
# Skip host validation (no sudo available on Render)
export PLAYWRIGHT_SKIP_VALIDATE_HOST_REQUIREMENTS=1

echo ">>> creating venv + installing python deps..."
python -m venv .venv
source .venv/bin/activate

# Speed up/quiet pip a bit
export PIP_DISABLE_PIP_VERSION_CHECK=1
python -m pip install --upgrade pip setuptools wheel

# Install project deps
if [[ -f poetry.lock ]]; then
  pip install poetry
  poetry config virtualenvs.create false
  poetry install --no-interaction --no-ansi
elif [[ -f requirements.txt ]]; then
  pip install -r requirements.txt
fi

# ---- Pre-fetch Playwright browser ONLY (no system deps, no sudo)
# Do NOT run: `playwright install-deps` (requires sudo; fails on Render)
python -m playwright install chromium

echo ">>> build step finished"

# ---- Restore original proxy vars (if needed by anything after build)
for v in HTTP_PROXY http_proxy HTTPS_PROXY https_proxy ALL_PROXY all_proxy NO_PROXY no_proxy; do
  if [[ -n "${!("ORIG_$v"):-}" ]]; then
    export "$v=${!("ORIG_$v")}"
    unset "ORIG_$v" || true
  fi
done
