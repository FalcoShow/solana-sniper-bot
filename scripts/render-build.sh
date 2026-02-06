#!/usr/bin/env bash
# scripts/render-build.sh
set -euo pipefail
IFS=$'\n\t'

# ==============================================================================
# Render build script (P0 contract)
# - Deterministic, failure-obvious, no secret leakage
# - Clears proxy vars during build (opt-out supported)
# - Works with Poetry OR requirements.txt
# - Optional Playwright install (guarded)
# - Sanity checks: compileall (project only) + optional pip check + optional tests
# ==============================================================================

ts() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
log() { printf "\n[%s] %s\n" "$(ts)" "$*"; }
die() { echo "ERROR: $*" >&2; exit 1; }

# ---------- Repo root (run from repo root deterministically) ----------
REPO_ROOT="${REPO_ROOT:-}"
if [[ -z "${REPO_ROOT}" ]]; then
  # Resolve repo root from script location: <repo>/scripts/render-build.sh
  SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
  REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
fi
cd "${REPO_ROOT}"

# ---------- Build knobs ----------
VENV_DIR="${VENV_DIR:-.venv}"

# pip behavior
export PIP_DISABLE_PIP_VERSION_CHECK=1
export PIP_NO_CACHE_DIR="${PIP_NO_CACHE_DIR:-1}"   # 1 = no cache (deterministic), 0 = cache (faster)
export PYTHONDONTWRITEBYTECODE=1
export PYTHONUNBUFFERED=1

# Optional determinism hardening
export PYTHONHASHSEED="${PYTHONHASHSEED:-0}"

# Poetry
POETRY_VERSION="${POETRY_VERSION:-2.1.3}"
INSTALL_DEV_DEPS="${INSTALL_DEV_DEPS:-0}"  # 1 = include dev deps

# Optional Playwright
ENABLE_PLAYWRIGHT="${ENABLE_PLAYWRIGHT:-0}"               # 1 = force install browsers
PLAYWRIGHT_BROWSER="${PLAYWRIGHT_BROWSER:-chromium}"      # chromium|firefox|webkit
PLAYWRIGHT_INSTALL_DEPS="${PLAYWRIGHT_INSTALL_DEPS:-0}"   # 1 = attempt install-deps (may fail)

# Optional checks
RUN_COMPILEALL="${RUN_COMPILEALL:-1}"        # 1 = syntax compile check (project only)
RUN_PIP_CHECK="${RUN_PIP_CHECK:-1}"          # 1 = pip check (dependency integrity)
RUN_PRINT_MANIFEST="${RUN_PRINT_MANIFEST:-1}"# 1 = scripts/print_manifest.py if present (best-effort)
RUN_TESTS="${RUN_TESTS:-0}"                  # 1 = run pytest (only if installed/configured)

# Proxy behavior (default: clear proxies during build)
CLEAR_PROXIES="${CLEAR_PROXIES:-1}"

# ---------- Save & clear proxies during build (with guaranteed restore) ----------
PROXY_VARS=(HTTP_PROXY http_proxy HTTPS_PROXY https_proxy ALL_PROXY all_proxy NO_PROXY no_proxy)

_restore_proxies() {
  if [[ "${CLEAR_PROXIES}" != "1" ]]; then
    return 0
  fi
  for v in "${PROXY_VARS[@]}"; do
    local ov="ORIG_${v}"
    if [[ -n "${!ov:-}" ]]; then
      export "${v}=${!ov}"
    else
      unset "${v}" >/dev/null 2>&1 || true
    fi
  done
}

_on_exit() {
  local rc=$?
  _restore_proxies || true
  if [[ $rc -ne 0 ]]; then
    echo "" >&2
    echo "[${ts}] Build failed (rc=${rc})." >&2
  fi
  exit $rc
}
trap _on_exit EXIT

if [[ "${CLEAR_PROXIES}" == "1" ]]; then
  for v in "${PROXY_VARS[@]}"; do
    if [[ -n "${!v:-}" ]]; then
      export "ORIG_${v}=${!v}"
    fi
    unset "${v}" >/dev/null 2>&1 || true
  done
  # Render internal + loopback should never proxy
  export NO_PROXY="127.0.0.1,localhost,::1,.render.internal"
fi

# ---------- Pick python ----------
PYBIN=""
if command -v python3 >/dev/null 2>&1; then
  PYBIN="python3"
elif command -v python >/dev/null 2>&1; then
  PYBIN="python"
else
  die "python not found on PATH"
fi

log "Repo root: ${REPO_ROOT}"
log "Python bin: ${PYBIN}"

# ---------- Create venv ----------
log "Creating virtualenv: ${VENV_DIR}"
rm -rf "${VENV_DIR}" >/dev/null 2>&1 || true
"${PYBIN}" -m venv "${VENV_DIR}"

# shellcheck disable=SC1090
source "${VENV_DIR}/bin/activate"

log "Python version: $(python -V 2>&1)"
log "Upgrading pip tooling"
python -m pip install --upgrade pip setuptools wheel

log "pip: $(python -m pip --version 2>&1)"

# ---------- Install deps ----------
if [[ -f "poetry.lock" && -f "pyproject.toml" ]]; then
  log "Installing deps via Poetry (v${POETRY_VERSION})"
  python -m pip install "poetry==${POETRY_VERSION}"

  # Force Poetry to install into THIS venv (no nested venv)
  poetry config virtualenvs.create false --local >/dev/null 2>&1 || true

  if [[ "${INSTALL_DEV_DEPS}" == "1" ]]; then
    log "Poetry install (including dev deps)"
    poetry install --no-interaction --no-ansi
  else
    log "Poetry install (prod deps only)"
    # Cross-version compatible fallbacks
    set +e
    poetry install --no-interaction --no-ansi --without dev
    rc=$?
    if [[ $rc -ne 0 ]]; then
      poetry install --no-interaction --no-ansi --only main
      rc=$?
    fi
    if [[ $rc -ne 0 ]]; then
      poetry install --no-interaction --no-ansi --no-dev
      rc=$?
    fi
    set -e
    if [[ $rc -ne 0 ]]; then
      die "Poetry install failed (tried --without dev, --only main, --no-dev)"
    fi
  fi

elif [[ -f "requirements.txt" ]]; then
  log "Installing deps via requirements.txt"
  python -m pip install -r requirements.txt
else
  die "No dependency file found (expected pyproject.toml+poetry.lock or requirements.txt)"
fi

# ---------- Optional: Playwright ----------
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
if [[ "${RUN_PIP_CHECK}" == "1" ]]; then
  log "Sanity: pip check (dependency integrity)"
  python -m pip check
fi

if [[ "${RUN_COMPILEALL}" == "1" ]]; then
  log "Sanity: python -m compileall (project only)"
  # Avoid compiling the venv and caches; compile only known project dirs/files if present.
  TARGETS=()
  for p in utils_pkg daytrader scanner sniper scripts route_telemetry.py routeguard.py config.py db; do
    if [[ -e "${p}" ]]; then
      TARGETS+=("${p}")
    fi
  done
  if [[ ${#TARGETS[@]} -gt 0 ]]; then
    python -m compileall -q "${TARGETS[@]}"
  else
    log "No compile targets found; skipping compileall."
  fi
fi

if [[ "${RUN_PRINT_MANIFEST}" == "1" ]]; then
  if [[ -f "scripts/print_manifest.py" ]]; then
    log "Sanity: print manifest (best-effort)"
    python -m scripts.print_manifest --json >/dev/null 2>&1 || true
  fi
fi

if [[ "${RUN_TESTS}" == "1" ]]; then
  if python -c "import pytest" >/dev/null 2>&1; then
    log "Running tests (pytest)"
    # Reduce surprises: disable plugin autoload unless explicitly needed
    export PYTEST_DISABLE_PLUGIN_AUTOLOAD="${PYTEST_DISABLE_PLUGIN_AUTOLOAD:-1}"
    pytest -q
  else
    log "RUN_TESTS=1 but pytest not installed; skipping."
  fi
fi

log "Build step finished ✅"
