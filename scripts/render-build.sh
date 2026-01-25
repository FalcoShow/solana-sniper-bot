#!/usr/bin/env bash
# scripts/render-build.sh
set -euo pipefail

# ==============================================================================
# Render build script (P0 contract)
# - Deterministic installs
# - No secret leakage (mask proxy vars; avoid printing DSNs/tokens)
# - Works with Poetry OR requirements.txt
# - Optional Playwright install (guarded)
# - Lightweight sanity checks (compileall + optional manifest)
# ==============================================================================

log() { printf "\n[%s] %s\n" "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" "$*"; }

# ---------- Save & clear proxies during build ----------
PROXY_VARS=(HTTP_PROXY http_proxy HTTPS_PROXY https_proxy ALL_PROXY all_proxy NO_PROXY no_proxy)
for v in "${PROXY_VARS[@]}"; do
  if [[ -n "${!v:-}" ]]; then
    export "ORIG_${v}=${!v}"
  fi
  unset "${v}" >/dev/null 2>&1 || true
done
# Render internal + loopback should never proxy
export NO_PROXY="127.0.0.1,localhost,::1,.render.internal"

# ---------- Pick python ----------
PYBIN=""
if command -v python3 >/dev/null 2>&1; then
  PYBIN="python3"
elif command -v python >/dev/null 2>&1; then
  PYBIN="python"
else
  echo "ERROR: python not found on PATH" >&2
  exit 1
fi

# ---------- Config knobs ----------
VENV_DIR="${VENV_DIR:-.venv}"
PIP_DISABLE_PIP_VERSION_CHECK=1
export PIP_DISABLE_PIP_VERSION_CHECK
export PYTHONDONTWRITEBYTECODE=1

# If you want pip caching on Render, set PIP_NO_CACHE_DIR=0
export PIP_NO_CACHE_DIR="${PIP_NO_CACHE_DIR:-1}"

# Poetry
POETRY_VERSION="${POETRY_VERSION:-2.1.3}"
INSTALL_DEV_DEPS="${INSTALL_DEV_DEPS:-0}"  # 1 = include dev deps

# Optional Playwright
ENABLE_PLAYWRIGHT="${ENABLE_PLAYWRIGHT:-0}"            # 1 = force install browsers
PLAYWRIGHT_BROWSER="${PLAYWRIGHT_BROWSER:-chromium}"   # chromium|firefox|webkit
PLAYWRIGHT_INSTALL_DEPS="${PLAYWRIGHT_INSTALL_DEPS:-0}" # 1 = attempt install-deps (may fail)

# Optional checks
RUN_COMPILEALL="${RUN_COMPILEALL:-1}"     # 1 = syntax compile check
RUN_PRINT_MANIFEST="${RUN_PRINT_MANIFEST:-1}" # 1 = run scripts/print_manifest.py if present
RUN_TESTS="${RUN_TESTS:-0}"               # 1 = run pytest (only if configured)

# ---------- Create venv ----------
log "Creating virtualenv: ${VENV_DIR}"
if [[ -d "${VENV_DIR}" ]]; then
  # In Render builds this is usually fresh, but be safe if cached
  rm -rf "${VENV_DIR}"
fi
"${PYBIN}" -m venv "${VENV_DIR}"

# shellcheck disable=SC1090
source "${VENV_DIR}/bin/activate"

log "Upgrading pip tooling"
python -m pip install --upgrade pip setuptools wheel

# ---------- Install deps ----------
if [[ -f "poetry.lock" && -f "pyproject.toml" ]]; then
  log "Installing deps via Poetry (v${POETRY_VERSION})"
  python -m pip install "poetry==${POETRY_VERSION}"

  # Force Poetry to install into THIS venv (no nested venv)
  poetry config virtualenvs.create false --local >/dev/null 2>&1 || true

  if [[ "${INSTALL_DEV_DEPS}" == "1" ]]; then
    poetry install --no-interaction --no-ansi
  else
    # Most compatible across Poetry versions
    poetry install --no-interaction --no-ansi --without dev
  fi

elif [[ -f "requirements.txt" ]]; then
  log "Installing deps via requirements.txt"
  python -m pip install -r requirements.txt

else
  log "No poetry.lock/pyproject.toml or requirements.txt found. Nothing to install."
fi

# ---------- Optional: Playwright ----------
# Only run if explicitly enabled OR if playwright is installed and you want browsers.
if [[ "${ENABLE_PLAYWRIGHT}" == "1" ]]; then
  log "Playwright enabled: attempting browser install (${PLAYWRIGHT_BROWSER})"
  if python -c "import playwright" >/dev/null 2>&1; then
    if [[ "${PLAYWRIGHT_INSTALL_DEPS}" == "1" ]]; then
      python -m playwright install-deps || true
    fi
    python -m playwright install "${PLAYWRIGHT_BROWSER}" || true
  else
    log "Playwright not installed as a Python dep; skipping browser install."
  fi
fi

# ---------- Sanity checks ----------
if [[ "${RUN_COMPILEALL}" == "1" ]]; then
  log "Sanity: python -m compileall (syntax check)"
  python -m compileall -q .
fi

if [[ "${RUN_PRINT_MANIFEST}" == "1" ]]; then
  if [[ -f "scripts/print_manifest.py" ]]; then
    log "Sanity: print manifest"
    # best-effort; don't hard fail builds on manifest-only issues unless you want to
    python -m scripts.print_manifest --json >/dev/null 2>&1 || true
  fi
fi

if [[ "${RUN_TESTS}" == "1" ]]; then
  if python -c "import pytest" >/dev/null 2>&1; then
    log "Running tests (pytest)"
    pytest -q
  else
    log "RUN_TESTS=1 but pytest not installed; skipping."
  fi
fi

log "Build step finished ✅"

# ---------- Restore proxies ----------
for v in "${PROXY_VARS[@]}"; do
  ov="ORIG_${v}"
  if [[ -n "${!ov:-}" ]]; then
    export "${v}=${!ov}"
  fi
done
