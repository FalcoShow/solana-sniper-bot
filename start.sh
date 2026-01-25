#!/usr/bin/env bash
# =============================================================================
# start.sh — Canonical Bootstrap Spine (P0 LOCK-IN / God-Tier)
#
# Hard guarantees:
#   - Render fail-closed: gates MUST pass or we EXIT 1 (no broken boots)
#   - No secret leaks in logs (even on ERR traps)
#   - Deterministic env + path + DSN normalization + wallet materialization
#   - Safe dotenv loading (no eval; source only if explicitly allowed)
#   - One canonical Python module launch + crash supervisor (optional restart)
#
# Subcommands:
#   ./start.sh help
#   ./start.sh bash-syntax
#   ./start.sh preflight [args...]
#   ./start.sh check-env [args...]
#   ./start.sh ci-smoke [args...]
#   ./start.sh ensure-schema
#   ./start.sh pytest-contract
#   ./start.sh compileall
#   ./start.sh check-router [args...]
#   ./start.sh check-jupiter [args...]
#
# Default run:
#   ./start.sh
#   ./start.sh bot|main|daytrader|backtest|paper_label   (sets RUN_MODE)
# =============================================================================

# -----------------------------------------------------------------------------
# SH-SAFE prologue: if invoked by sh/dash, re-exec with bash.
# -----------------------------------------------------------------------------
if [ -z "${BASH_VERSION:-}" ]; then
  if command -v bash >/dev/null 2>&1; then
    exec bash "$0" "$@"
  fi
  echo "[start] FATAL: bash not available." >&2
  exit 1
fi

set -Eeuo pipefail
IFS=$'\n\t'
umask 077
shopt -s inherit_errexit 2>/dev/null || true

# -----------------------------------------------------------------------------
# Logging helpers (never print secrets)
# -----------------------------------------------------------------------------
_ts()   { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
_log()  { printf '[start] %s %s\n' "$(_ts)" "$*" >&2; }
_warn() { printf '[start] %s WARN: %s\n' "$(_ts)" "$*" >&2; }
_die()  { printf '[start] %s FATAL: %s\n' "$(_ts)" "$*" >&2; exit 1; }

_on_err() {
  local rc="${1:-1}" line="${2:-0}"
  # DO NOT print $BASH_COMMAND (could contain secrets)
  _log "ERROR rc=${rc} line=${line}"
  exit "${rc}"
}

trap '_on_err "$?" "$LINENO"' ERR
trap 'rc=$?; _log "EXIT rc=$rc"' EXIT

_is_true() {
  case "${1:-}" in
    1|true|TRUE|yes|YES|y|Y|on|ON|t|T) return 0 ;;
    *) return 1 ;;
  esac
}

_mask_url() {
  # Mask creds in URL-like strings:
  # - Masks ANY userinfo@ portion in scheme://userinfo@host
  # - Trims query
  local u="${1:-}"
  [[ -z "$u" ]] && { printf '%s' ""; return 0; }

  local base="$u" q=""
  if [[ "$u" == *\?* ]]; then
    base="${u%%\?*}"
    q="?...";
  fi

  if [[ "$base" == *"://"* && "$base" == *"@"* ]]; then
    local head="${base%%://*}://"
    local rest="${base#*://}"
    local tail="${rest#*@}"
    base="${head}***@${tail}"
  fi

  printf '%s' "${base}${q}"
}

_join_by() {
  local sep="${1:-:}"
  shift || true
  local out="" first=1 x
  for x in "$@"; do
    [[ -z "$x" ]] && continue
    if [[ "$first" == "1" ]]; then out="$x"; first=0; else out="${out}${sep}${x}"; fi
  done
  printf '%s' "$out"
}

# -----------------------------------------------------------------------------
# 0) Repo root + Render detect + CRLF guard
# -----------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
export REPO_ROOT="${REPO_ROOT:-$SCRIPT_DIR}"

export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:${PATH:-}"

IS_RENDER=0
if [[ -n "${RENDER_SERVICE_ID:-}" || -n "${RENDER_INSTANCE_ID:-}" || -n "${RENDER_EXTERNAL_URL:-}" ]]; then
  IS_RENDER=1
elif _is_true "${RENDER:-}"; then
  IS_RENDER=1
fi
# Make render-ness visible to Python too (harmless locally)
if [[ "$IS_RENDER" == "1" ]]; then
  export RENDER=1
fi

if command -v grep >/dev/null 2>&1; then
  if LC_ALL=C grep -q $'\r' "$0" 2>/dev/null; then
    _die "start.sh has CRLF line endings. Convert to LF before deploying to Render."
  fi
fi

# -----------------------------------------------------------------------------
# 1) Defaults / knobs (env-controlled; compat preserved)
# -----------------------------------------------------------------------------
: "${RUN_MODE:=bot}"
: "${RUN_MODULE_OVERRIDE:=}"

: "${RUN_ARGS_JSON:=}"
: "${RUN_ARGS:=}"
: "${RUN_ARGS_FILE:=}"

# Legacy module override knobs (compat; do not remove)
if [[ -z "${RUN_MODULE_OVERRIDE:-}" ]]; then
  [[ -n "${ENTRYPOINT_MODULE:-}" ]] && RUN_MODULE_OVERRIDE="${ENTRYPOINT_MODULE}"
  [[ -n "${APP_ENTRYPOINT_MODULE:-}" ]] && RUN_MODULE_OVERRIDE="${APP_ENTRYPOINT_MODULE}"
fi

# Render fail-closed policy (forced ON unless START_ALLOW_UNSAFE_RENDER=1)
: "${START_ALLOW_UNSAFE_RENDER:=0}"

if [[ "$IS_RENDER" == "1" ]] && ! _is_true "${START_ALLOW_UNSAFE_RENDER}"; then
  START_STRICT=1
  START_PRECHECKS=1
  START_RUN_CHECK_ENV=1
else
  : "${START_STRICT:=$IS_RENDER}"
  : "${START_PRECHECKS:=1}"
  : "${START_RUN_CHECK_ENV:=1}"
fi

: "${START_RUN_CI_SMOKE:=0}"

: "${START_ALLOW_SOURCE_ENV:=0}"
: "${START_ENV_OVERRIDE:=0}"
: "${START_ENV_FILES:=}"
: "${START_NO_DOTENV:=0}"

: "${START_SKIP_WALLET:=0}"
: "${START_WALLET_OVERWRITE:=0}"

: "${START_REDIS_OVERRIDE:=0}"
: "${REDIS_DB:=0}"
: "${REDIS_TLS:=0}"

: "${START_NORMALIZE_DSN:=1}"
: "${START_ADD_SSLMODE:=}"
if [[ -z "${START_ADD_SSLMODE}" ]]; then
  START_ADD_SSLMODE="$IS_RENDER"
fi

# Preserve prior behavior: local can skip DSN, Render requires DSN unless overridden
if [[ -z "${SKIP_DSN_REQUIRED:-}" ]]; then
  if [[ "$IS_RENDER" == "1" ]]; then SKIP_DSN_REQUIRED="0"; else SKIP_DSN_REQUIRED="1"; fi
fi

: "${START_ULIMIT_NOFILE:=0}"

: "${MAIN_RESTART_ON_CRASH:=1}"
: "${MAIN_MAX_RESTARTS:=0}"        # 0=infinite
: "${MAIN_RESTART_BACKOFF_S:=3}"
: "${MAIN_RESTART_MAX_BACKOFF_S:=30}"
: "${MAIN_RESTART_JITTER_S:=2}"
: "${MAIN_NO_RESTART_EXIT_CODES:=130,143}"
: "${MAIN_USE_SETSID:=1}"

# Preflight behavior (hard gate defaults)
: "${START_PREFLIGHT_FORCE:=1}"
: "${START_PREFLIGHT_JSON_ONLY:=1}"
: "${START_PREFLIGHT_PROFILE:=deploy}"

# IMPORTANT: export contract vars that Python gatekeepers must see
export RUN_MODE RUN_MODULE_OVERRIDE
export SKIP_DSN_REQUIRED
export REDIS_DB REDIS_TLS

_log "────────────────────────────────────────────"
_log "   Bootstrap Spine (Canonical) — Render=${IS_RENDER}"
_log "────────────────────────────────────────────"
_log "Mode: RUN_MODE=${RUN_MODE} strict=${START_STRICT} prechecks=${START_PRECHECKS} check_env=${START_RUN_CHECK_ENV}"

# -----------------------------------------------------------------------------
# 2) Python selection + deterministic runtime + PYTHONPATH (OS-aware separator)
# -----------------------------------------------------------------------------
PYTHON="${PYTHON:-}"

if [[ -z "$PYTHON" ]]; then
  if [[ -x ".venv/bin/python" ]]; then
    PYTHON=".venv/bin/python"
  elif [[ -x ".venv/Scripts/python.exe" ]]; then
    PYTHON=".venv/Scripts/python.exe"
  elif command -v python3 >/dev/null 2>&1; then
    PYTHON="python3"
  elif command -v python >/dev/null 2>&1; then
    PYTHON="python"
  else
    _die "Python executable not found"
  fi
fi

export PYTHONUNBUFFERED=1
export PYTHONDONTWRITEBYTECODE=1
export PYTHONUTF8="${PYTHONUTF8:-1}"
export PYTHONIOENCODING="${PYTHONIOENCODING:-utf-8}"
export PYTHONHASHSEED="${PYTHONHASHSEED:-0}"
export PYTHONFAULTHANDLER="${PYTHONFAULTHANDLER:-1}"

PY_PATHSEP="$("$PYTHON" - <<'PY'
import os
print(os.pathsep)
PY
)" || PY_PATHSEP=":"

_paths=("$REPO_ROOT")
[[ -d "$REPO_ROOT/src" ]] && _paths+=("$REPO_ROOT/src")

if [[ -n "${PYTHONPATH:-}" ]]; then
  export PYTHONPATH="$(_join_by "${PY_PATHSEP}" "${_paths[@]}")${PY_PATHSEP}${PYTHONPATH}"
else
  export PYTHONPATH="$(_join_by "${PY_PATHSEP}" "${_paths[@]}")"
fi

_log "Python: $("$PYTHON" -V 2>&1)"

_py_has_module() {
  local mod="${1:-}"
  [[ -z "$mod" ]] && return 1
  "$PYTHON" - "$mod" >/dev/null 2>&1 <<'PY'
import importlib.util, sys
m = sys.argv[1]
sys.exit(0 if importlib.util.find_spec(m) is not None else 1)
PY
}

_supports_arg() {
  # Usage: _supports_arg "scripts.preflight" "--strict"
  local mod="${1:-}" arg="${2:-}"
  [[ -z "$mod" || -z "$arg" ]] && return 1
  command -v grep >/dev/null 2>&1 || return 1
  "$PYTHON" -m "$mod" --help 2>&1 | grep -Fq -- "$arg"
}

# -----------------------------------------------------------------------------
# 3) Runtime dirs
# -----------------------------------------------------------------------------
DATA_DIR="${DATA_DIR:-/opt/render/data}"
SECRETS_DIR="${SECRETS_DIR:-/opt/render/.secrets}"
TMP_DIR="${TMP_DIR:-/tmp}"

_ensure_dir() {
  local d="${1:-}"
  [[ -z "$d" ]] && return 1
  mkdir -p "$d" 2>/dev/null || return 1
  chmod 700 "$d" 2>/dev/null || true
  return 0
}

_ensure_dir "$DATA_DIR"    || DATA_DIR="${TMP_DIR%/}/sniper_data"
_ensure_dir "$SECRETS_DIR" || SECRETS_DIR="${TMP_DIR%/}/sniper_secrets"
_ensure_dir "$DATA_DIR"    || _die "Cannot create DATA_DIR"
_ensure_dir "$SECRETS_DIR" || _die "Cannot create SECRETS_DIR"

export DATA_DIR SECRETS_DIR TMP_DIR
export XDG_CACHE_HOME="${XDG_CACHE_HOME:-$DATA_DIR/.cache}"
export XDG_CONFIG_HOME="${XDG_CONFIG_HOME:-$DATA_DIR/.config}"
_ensure_dir "$XDG_CACHE_HOME" 2>/dev/null || true
_ensure_dir "$XDG_CONFIG_HOME" 2>/dev/null || true

if [[ "${START_ULIMIT_NOFILE}" != "0" ]]; then
  (ulimit -n "${START_ULIMIT_NOFILE}" 2>/dev/null) || true
fi

# -----------------------------------------------------------------------------
# 4) Dotenv loading (safe; no eval/source unless allowed)
# -----------------------------------------------------------------------------
_HAS_PY_DOTENV=0
if "$PYTHON" - >/dev/null 2>&1 <<'PY'
try:
  import dotenv  # type: ignore
  raise SystemExit(0)
except Exception:
  raise SystemExit(1)
PY
then
  _HAS_PY_DOTENV=1
fi

_file_has_crlf() {
  local f="${1:-}"
  [[ -z "$f" || ! -f "$f" ]] && return 1
  command -v grep >/dev/null 2>&1 || return 1
  LC_ALL=C grep -q $'\r' "$f" 2>/dev/null
}

_load_env_source() {
  local f="${1:-}"
  [[ -z "$f" || ! -f "$f" ]] && return 0
  _file_has_crlf "$f" && _die "Env file has CRLF line endings: $f (convert to LF)"
  _log "Loading env file (source): $f"
  set -a
  # shellcheck disable=SC1090
  . "$f"
  set +a
}

_load_env_safe_bash() {
  local f="${1:-}"
  [[ -z "$f" || ! -f "$f" ]] && return 0
  _file_has_crlf "$f" && _die "Env file has CRLF line endings: $f (convert to LF)"
  _log "Loading env file (bash-safe): $f"

  local line k v
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line//$'\r'/}"
    [[ -z "$line" ]] && continue
    [[ "$line" =~ ^[[:space:]]*# ]] && continue

    line="${line#"${line%%[![:space:]]*}"}"
    [[ "$line" == export\ * ]] && line="${line#export }"
    [[ "$line" != *"="* ]] && continue

    k="${line%%=*}"
    v="${line#*=}"

    k="${k%"${k##*[![:space:]]}"}"
    k="${k#"${k%%[![:space:]]*}"}"
    [[ -z "$k" ]] && continue
    [[ "$k" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue

    if [[ "${START_ENV_OVERRIDE}" != "1" && -n "${!k:-}" ]]; then
      continue
    fi

    v="${v#"${v%%[![:space:]]*}"}"
    if [[ "${v:0:1}" != "'" && "${v:0:1}" != '"' ]]; then
      v="${v%%#*}"
      v="${v%"${v##*[![:space:]]}"}"
    fi

    if [[ "${v:0:1}" == "\"" && "${v: -1}" == "\"" ]]; then
      v="${v:1:-1}"
    elif [[ "${v:0:1}" == "'" && "${v: -1}" == "'" ]]; then
      v="${v:1:-1}"
    fi

    export "$k=$v"
  done <"$f"
}

_dotenv_stream_python() {
  local f="${1:-}"
  "$PYTHON" - "$f" "${START_ENV_OVERRIDE:-0}" <<'PY'
import os, sys
from dotenv import dotenv_values  # type: ignore
path = sys.argv[1]
override = (sys.argv[2] == "1")
vals = dotenv_values(path)
out = sys.stdout.buffer
for k, v in vals.items():
    if not k or not isinstance(k, str):
        continue
    if not (k[0].isalpha() or k[0] == "_"):
        continue
    if not all((c.isalnum() or c == "_") for c in k):
        continue
    if (not override) and (k in os.environ) and (os.environ.get(k) not in (None, "")):
        continue
    if v is None:
        v = ""
    if not isinstance(v, str):
        v = str(v)
    out.write(k.encode("utf-8", "ignore") + b"\x00")
    out.write(v.encode("utf-8", "ignore") + b"\x00")
PY
}

load_env_file() {
  local f="${1:-}"
  [[ -z "$f" || ! -f "$f" ]] && return 0

  if _is_true "${START_ALLOW_SOURCE_ENV}"; then
    _load_env_source "$f"
    return 0
  fi

  if [[ "$_HAS_PY_DOTENV" == "1" ]]; then
    _log "Loading env file (dotenv-safe): $f"
    local k v
    while IFS= read -r -d '' k; do
      IFS= read -r -d '' v || v=""
      [[ -z "$k" ]] && continue
      export "$k=$v"
    done < <(_dotenv_stream_python "$f")
    return 0
  fi

  _load_env_safe_bash "$f"
}

_env_files=(
  "/etc/secrets/.env.extra"
  "/etc/secrets/.env"
  "/opt/render/project/.env"
  "./.env.extra"
  "./.env"
)

if [[ -n "${START_ENV_FILES:-}" ]]; then
  _env_files=()
  _tmp="${START_ENV_FILES//;/,}"
  IFS=',' read -r -a _arr <<<"$_tmp"
  for f in "${_arr[@]}"; do
    f="${f#"${f%%[![:space:]]*}"}"
    f="${f%"${f##*[![:space:]]}"}"
    [[ -z "$f" ]] && continue
    _env_files+=("$f")
  done
fi

if ! _is_true "${START_NO_DOTENV}"; then
  for f in "${_env_files[@]}"; do
    load_env_file "$f"
  done
else
  _warn "Dotenv loading disabled (START_NO_DOTENV=1)"
fi

# -----------------------------------------------------------------------------
# 5) Subcommands (run after env load)
# -----------------------------------------------------------------------------
cmd="${1:-}"

# Allow "./start.sh bot|main|daytrader|backtest|paper_label" to set RUN_MODE
case "$cmd" in
  bot|main|daytrader|backtest|paper_label|paperlabel|paper-label)
    export RUN_MODE="$cmd"
    shift || true
    cmd=""
    ;;
esac

case "${cmd}" in
  -h|--help|help)
    cat >&2 <<'TXT'
[start] Usage:
  ./start.sh
  ./start.sh bot|main|daytrader|backtest|paper_label
  ./start.sh preflight [args...]
  ./start.sh check-env [args...]
  ./start.sh ci-smoke [args...]
  ./start.sh ensure-schema
  ./start.sh pytest-contract
  ./start.sh compileall
  ./start.sh bash-syntax
  ./start.sh check-router [args...]
  ./start.sh check-jupiter [args...]
TXT
    exit 0
    ;;
  bash-syntax)
    bash -n "$0"
    _log "bash syntax OK"
    exit 0
    ;;
  preflight)
    shift || true
    exec "$PYTHON" -m scripts.preflight "$@"
    ;;
  check-env|check_env)
    shift || true
    _py_has_module "scripts.check_env" || { _warn "scripts.check_env not found"; exit 0; }
    exec "$PYTHON" -m scripts.check_env "$@"
    ;;
  ci-smoke|ci_smoke)
    shift || true
    if _py_has_module "scripts.ci_smoke"; then exec "$PYTHON" -m scripts.ci_smoke "$@"; fi
    if [[ -f "scripts/ci_smoke.py" ]]; then exec "$PYTHON" "scripts/ci_smoke.py" "$@"; fi
    _warn "scripts.ci_smoke not found"; exit 0
    ;;
  check-router|check_router)
    shift || true
    if _py_has_module "scripts.check_router"; then exec "$PYTHON" -m scripts.check_router "$@"; fi
    if [[ -f "scripts/check_router.py" ]]; then exec "$PYTHON" "scripts/check_router.py" "$@"; fi
    _warn "scripts.check_router not found"; exit 0
    ;;
  check-jupiter|check_jupiter)
    shift || true
    if _py_has_module "scripts.check_jupiter"; then exec "$PYTHON" -m scripts.check_jupiter "$@"; fi
    if [[ -f "scripts/check_jupiter.py" ]]; then exec "$PYTHON" "scripts/check_jupiter.py" "$@"; fi
    _warn "scripts.check_jupiter not found"; exit 0
    ;;
  ensure-schema|ensure_schema)
    exec "$PYTHON" - <<'PY'
import importlib.util, sys
def has(m): return importlib.util.find_spec(m) is not None
if has("db.schema"):
    import asyncio
    from db.schema import ensure_schema
    asyncio.run(ensure_schema()); print("ensure_schema OK"); raise SystemExit(0)
if has("utils_pkg.ai_db"):
    import asyncio
    from utils_pkg.ai_db import ensure_schema  # type: ignore
    asyncio.run(ensure_schema()); print("ensure_schema OK (utils_pkg.ai_db)"); raise SystemExit(0)
print("ensure_schema: no schema module found", file=sys.stderr); raise SystemExit(1)
PY
    ;;
  pytest-contract)
    exec "$PYTHON" -m pytest -q -m "not integration and not network"
    ;;
  compileall)
    exec "$PYTHON" -m compileall -q .
    ;;
esac

# -----------------------------------------------------------------------------
# 6) Safe-mode enforcement
# -----------------------------------------------------------------------------
if _is_true "${DRYRUN:-}" || _is_true "${SAFE_MODE:-}"; then
  export SAFE_MODE=1 DRYRUN=1
  export NO_NETWORK="${NO_NETWORK:-1}"
  export DISABLE_NETWORK="${DISABLE_NETWORK:-1}"
fi

# -----------------------------------------------------------------------------
# 7) Redis synthesis
# -----------------------------------------------------------------------------
_build_redis_url() {
  local host="${REDIS_HOST:-}" port="${REDIS_PORT:-}" db="${REDIS_DB:-0}"
  [[ -z "$host" || -z "$port" ]] && return 1

  local scheme="redis"
  _is_true "${REDIS_TLS}" && scheme="rediss"

  local userinfo=""
  if [[ -n "${REDIS_PASSWORD:-}" ]]; then
    if [[ -n "${REDIS_USER:-}" ]]; then
      userinfo="${REDIS_USER}:${REDIS_PASSWORD}@"
    else
      userinfo=":${REDIS_PASSWORD}@"
    fi
  fi

  export REDIS_URL="${scheme}://${userinfo}${host}:${port}/${db}"
  return 0
}

if [[ -n "${REDIS_URL:-}" ]] && ! _is_true "${START_REDIS_OVERRIDE}"; then
  _log "Redis: REDIS_URL preset: $(_mask_url "${REDIS_URL}")"
else
  if _build_redis_url; then
    _log "Redis: synthesized REDIS_URL: $(_mask_url "${REDIS_URL}")"
  else
    _warn "Redis: not configured (OK only if Redis is optional)"
  fi
fi

# -----------------------------------------------------------------------------
# 8) Wallet materialization (safe)
# -----------------------------------------------------------------------------
WALLET_PATH="${SECRETS_DIR%/}/wallet.json"
b64_val=""

_write_wallet_file() {
  local content="${1:-}"
  [[ -z "$content" ]] && return 0
  if [[ -f "$WALLET_PATH" ]] && ! _is_true "${START_WALLET_OVERWRITE}"; then
    return 0
  fi
  content="${content//$'\r'/}"
  printf '%s' "$content" >"$WALLET_PATH"
  chmod 600 "$WALLET_PATH" 2>/dev/null || true
  return 0
}

_decode_b64_to_file() {
  local b64="${1:-}"
  [[ -z "$b64" ]] && return 1

  if command -v base64 >/dev/null 2>&1; then
    if printf '%s' "$b64" | base64 -d >"$WALLET_PATH" 2>/dev/null; then return 0; fi
    if printf '%s' "$b64" | base64 -D >"$WALLET_PATH" 2>/dev/null; then return 0; fi
  fi

  "$PYTHON" - "$b64" "$WALLET_PATH" <<'PY'
import base64, sys
raw = (sys.argv[1] or "").strip()
out = sys.argv[2]
pad = (-len(raw)) % 4
raw2 = raw + ("=" * pad)
try:
    data = base64.b64decode(raw2, validate=False)
except Exception:
    data = base64.urlsafe_b64decode(raw2)
with open(out, "wb") as f:
    f.write(data)
PY
  return 0
}

_verify_wallet_json() {
  [[ ! -f "$WALLET_PATH" ]] && return 1
  "$PYTHON" - "$WALLET_PATH" >/dev/null 2>&1 <<'PY'
import json, sys
p = sys.argv[1]
with open(p, "rb") as f:
    s = f.read().decode("utf-8", "ignore").strip()
json.loads(s)
PY
}

if ! _is_true "${START_SKIP_WALLET}"; then
  # Compat: if KEYPAIR_PATH points to a file, prefer it.
  if [[ -n "${KEYPAIR_PATH:-}" && -f "${KEYPAIR_PATH}" ]]; then
    export WALLET_PRIVATE_KEY="${KEYPAIR_PATH}"
  fi

  if [[ -n "${WALLET_PRIVATE_KEY:-}" && -f "${WALLET_PRIVATE_KEY}" ]]; then
    : # already a path
  else
    # Inline JSON in WALLET_PRIVATE_KEY
    if [[ -n "${WALLET_PRIVATE_KEY:-}" ]]; then
      case "${WALLET_PRIVATE_KEY}" in
        \[*|\{*)
          _write_wallet_file "${WALLET_PRIVATE_KEY}" || true
          _verify_wallet_json && export WALLET_PRIVATE_KEY="$WALLET_PATH" || _warn "Wallet: inline JSON parse failed"
          ;;
      esac
    fi

    # Inline JSON in WALLET_JSON
    if [[ -z "${WALLET_PRIVATE_KEY:-}" || ! -f "${WALLET_PRIVATE_KEY:-}" ]]; then
      if [[ -n "${WALLET_JSON:-}" ]]; then
        _write_wallet_file "${WALLET_JSON}" || true
        _verify_wallet_json && export WALLET_PRIVATE_KEY="$WALLET_PATH" || _warn "Wallet: WALLET_JSON parse failed"
      fi
    fi

    # Base64 wallet JSON
    if [[ -z "${WALLET_PRIVATE_KEY:-}" || ! -f "${WALLET_PRIVATE_KEY:-}" ]]; then
      b64_val="${WALLET_JSON_B64:-${KEYPAIR_B64:-}}"
      if [[ -n "${b64_val:-}" ]]; then
        if [[ ! -f "$WALLET_PATH" ]] || _is_true "${START_WALLET_OVERWRITE}"; then
          _decode_b64_to_file "$b64_val" || true
          chmod 600 "$WALLET_PATH" 2>/dev/null || true
          _verify_wallet_json && export WALLET_PRIVATE_KEY="$WALLET_PATH" || _warn "Wallet: base64 decoded but JSON parse failed"
        else
          export WALLET_PRIVATE_KEY="$WALLET_PATH"
        fi
      fi
    fi
  fi

  if [[ -n "${WALLET_PRIVATE_KEY:-}" && -f "${WALLET_PRIVATE_KEY}" ]]; then
    [[ -z "${KEYPAIR_PATH:-}" ]] && export KEYPAIR_PATH="${WALLET_PRIVATE_KEY}"
    _log "Wallet: present (path) $(basename "${WALLET_PRIVATE_KEY}")"
  else
    _warn "Wallet: not materialized (OK for DRYRUN/no-trade; NOT OK for live)"
  fi
else
  _log "Wallet: skipped (START_SKIP_WALLET=1)"
fi

# -----------------------------------------------------------------------------
# 9) DB DSN normalization/export (no secret printing)
# -----------------------------------------------------------------------------
if _is_true "${START_NORMALIZE_DSN}"; then
  while IFS= read -r -d '' k; do
    IFS= read -r -d '' v || v=""
    [[ -z "$k" ]] && continue
    export "$k=$v"
  done < <(
    START_ADD_SSLMODE="${START_ADD_SSLMODE:-0}" "$PYTHON" - <<'PY'
import os, sys
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

def strip_quotes(s: str) -> str:
    s = (s or "").strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        return s[1:-1].strip()
    return s

def base_scheme(scheme: str) -> str:
    scheme = (scheme or "").lower().strip()
    return scheme.split("+", 1)[0] if "+" in scheme else scheme

def with_scheme(dsn: str, new_scheme: str) -> str:
    try:
        p = urlparse(dsn)
        return urlunparse(p._replace(scheme=new_scheme)) if p.scheme else dsn
    except Exception:
        return dsn

def has_sslmode(dsn: str) -> bool:
    try:
        q = dict(parse_qsl(urlparse(dsn).query or "", keep_blank_values=True))
        return bool(q.get("sslmode"))
    except Exception:
        return "sslmode=" in (dsn or "").lower()

def add_sslmode_require(dsn: str) -> str:
    try:
        p = urlparse(dsn)
        q = dict(parse_qsl(p.query or "", keep_blank_values=True))
        if "sslmode" not in q:
            q["sslmode"] = "require"
            return urlunparse(p._replace(query=urlencode(q)))
    except Exception:
        pass
    return dsn

add_ssl = (os.getenv("START_ADD_SSLMODE", "0").strip() == "1")

DATABASE_URL     = strip_quotes(os.getenv("DATABASE_URL", ""))
POSTGRES_DSN     = strip_quotes(os.getenv("POSTGRES_DSN", ""))
PG_DSN           = strip_quotes(os.getenv("PG_DSN", ""))
PG_DSN_INTERNAL  = strip_quotes(os.getenv("PG_DSN_INTERNAL", ""))
PG_DSN_EXTERNAL  = strip_quotes(os.getenv("PG_DSN_EXTERNAL", ""))
PNL_DSN          = strip_quotes(os.getenv("PNL_DSN", ""))

POSTGRES = POSTGRES_DSN or DATABASE_URL or PG_DSN_INTERNAL or PG_DSN_EXTERNAL or PNL_DSN or PG_DSN

def normalize_postgres(dsn: str) -> str:
    if not dsn: return dsn
    try:
        p = urlparse(dsn)
        sch = (p.scheme or "").lower().strip()
        b = base_scheme(sch)
        if b == "postgres":
            dsn = with_scheme(dsn, "postgresql")
            p = urlparse(dsn); sch = (p.scheme or "").lower().strip()
        if "+asyncpg" in sch:
            dsn = with_scheme(dsn, "postgresql")
    except Exception:
        pass
    if add_ssl and dsn and (not has_sslmode(dsn)):
        dsn = add_sslmode_require(dsn)
    return dsn

def normalize_asyncpg(dsn: str) -> str:
    if not dsn: return dsn
    try:
        p = urlparse(dsn)
        sch = (p.scheme or "").lower().strip()
        b = base_scheme(sch)
        if b == "postgres":
            dsn = with_scheme(dsn, "postgresql")
            p = urlparse(dsn); sch = (p.scheme or "").lower().strip()
        if b in ("postgres", "postgresql") and "+asyncpg" not in sch:
            dsn = with_scheme(dsn, "postgresql+asyncpg")
    except Exception:
        pass
    if add_ssl and dsn and (not has_sslmode(dsn)):
        dsn = add_sslmode_require(dsn)
    return dsn

POSTGRES_N = normalize_postgres(POSTGRES)
DATABASE_N = normalize_postgres(DATABASE_URL) if DATABASE_URL else ""
POSTGRES_DSN_N = normalize_postgres(POSTGRES_DSN) if POSTGRES_DSN else ""
PG_DSN_N = normalize_asyncpg(PG_DSN) if PG_DSN else normalize_asyncpg(POSTGRES_N)

out = sys.stdout.buffer
def emit(k: str, v: str):
    out.write(k.encode("utf-8") + b"\x00")
    out.write((v or "").encode("utf-8") + b"\x00")

if POSTGRES_N:
    if DATABASE_URL and DATABASE_N and (DATABASE_N != DATABASE_URL):
        emit("DATABASE_URL", DATABASE_N)
    if POSTGRES_DSN and POSTGRES_DSN_N and (POSTGRES_DSN_N != POSTGRES_DSN):
        emit("POSTGRES_DSN", POSTGRES_DSN_N)
    if DATABASE_URL and not POSTGRES_DSN:
        emit("POSTGRES_DSN", DATABASE_N)
    if POSTGRES_DSN and not DATABASE_URL:
        emit("DATABASE_URL", POSTGRES_DSN_N)

if PG_DSN_N and (PG_DSN_N != PG_DSN):
    emit("PG_DSN", PG_DSN_N)
PY
  ) || true
fi

if _is_true "${START_STRICT}" && ! _is_true "${SKIP_DSN_REQUIRED}"; then
  if [[ -z "${PG_DSN_INTERNAL:-}" && -z "${PG_DSN_EXTERNAL:-}" && -z "${POSTGRES_DSN:-}" && -z "${DATABASE_URL:-}" && -z "${PG_DSN:-}" && -z "${PNL_DSN:-}" ]]; then
    _die "Missing DB DSN. Set PG_DSN_INTERNAL/PG_DSN_EXTERNAL or POSTGRES_DSN/DATABASE_URL (or PG_DSN)."
  fi
fi

[[ -n "${POSTGRES_DSN:-}" ]] && _log "DB SQLA        : $(_mask_url "${POSTGRES_DSN}")"
[[ -n "${DATABASE_URL:-}" ]] && _log "DB DATABASE_URL: $(_mask_url "${DATABASE_URL}")"
[[ -n "${PG_DSN:-}" ]]       && _log "DB asyncpg     : $(_mask_url "${PG_DSN}")"

# -----------------------------------------------------------------------------
# 10) Gates: check_env + preflight + optional ci_smoke
# -----------------------------------------------------------------------------
_run_check_env() {
  if ! _py_has_module "scripts.check_env"; then
    _is_true "${START_STRICT}" && return 1
    _warn "scripts.check_env not found; skipping"
    return 0
  fi

  local -a args=(--json-only)
  _is_true "${START_STRICT}" && args+=(--strict)
  CHECK_ENV_JSON=1 CHECK_ENV_JSON_ONLY=1 "$PYTHON" -m scripts.check_env "${args[@]}"
}

_run_preflight() {
  if ! _py_has_module "scripts.preflight"; then
    _is_true "${START_STRICT}" && return 1
    _warn "scripts.preflight not found; skipping"
    return 0
  fi

  local -a args=()

  if _is_true "${START_STRICT}"; then
    _supports_arg "scripts.preflight" "--strict" && args+=(--strict)
  else
    if _supports_arg "scripts.preflight" "--no-strict"; then
      args+=(--no-strict)
    elif _supports_arg "scripts.preflight" "--non-strict"; then
      args+=(--non-strict)
    fi
  fi

  _is_true "${START_PREFLIGHT_FORCE}" && _supports_arg "scripts.preflight" "--force" && args+=(--force)
  _is_true "${START_PREFLIGHT_JSON_ONLY}" && _supports_arg "scripts.preflight" "--json-only" && args+=(--json-only)

  if [[ -n "${START_PREFLIGHT_PROFILE:-}" ]] && _supports_arg "scripts.preflight" "--profile"; then
    args+=(--profile "${START_PREFLIGHT_PROFILE}")
  fi

  "$PYTHON" -m scripts.preflight "${args[@]}"
}

_run_ci_smoke() {
  if _py_has_module "scripts.ci_smoke"; then
    "$PYTHON" -m scripts.ci_smoke
  elif [[ -f "scripts/ci_smoke.py" ]]; then
    "$PYTHON" "scripts/ci_smoke.py"
  else
    _warn "ci_smoke not found; skipping"
    return 0
  fi
}

if _is_true "${START_RUN_CHECK_ENV}"; then
  _log "Gate: check_env"
  _run_check_env || _die "check_env failed"
  _log "Gate: check_env OK"
else
  _warn "Gate: check_env disabled (not recommended)"
fi

_gate_preflight_or_die() {
  _log "Gate: preflight"
  if _run_preflight; then
    _log "Gate: preflight OK"
    return 0
  fi

  _warn "preflight failed; running check_env diagnostics (sanitized)"
  if _py_has_module "scripts.check_env"; then
    local -a dargs=(--json-only)
    _is_true "${START_STRICT}" && dargs+=(--strict)
    CHECK_ENV_INCLUDE_CONFIG_DETAIL=1 CHECK_ENV_JSON=1 CHECK_ENV_JSON_ONLY=1 \
      "$PYTHON" -m scripts.check_env "${dargs[@]}" || true
  fi

  _die "preflight failed"
}

if _is_true "${START_PRECHECKS}"; then
  _gate_preflight_or_die
else
  _warn "Gate: preflight disabled (not recommended)"
fi

if _is_true "${START_RUN_CI_SMOKE}"; then
  _log "Gate: ci_smoke"
  _run_ci_smoke || _die "ci_smoke failed"
  _log "Gate: ci_smoke OK"
fi

# -----------------------------------------------------------------------------
# 11) Resolve module for RUN_MODE (self-healing)
# -----------------------------------------------------------------------------
_resolve_module_for_mode() {
  local mode="${1:-bot}"

  if [[ "$mode" == "bot" || "$mode" == "main" ]]; then
    _py_has_module "scripts.main" && { printf '%s' "scripts.main"; return 0; }
    _py_has_module "scripts.entrypoint" && { printf '%s' "scripts.entrypoint"; return 0; }
    _py_has_module "main" && { printf '%s' "main"; return 0; }
    _py_has_module "app" && { printf '%s' "app"; return 0; }
    printf '%s' "scripts.main"
    return 0
  fi

  case "$mode" in
    paper_label|paperlabel|paper-label) printf '%s' "scripts.paper_label_worker" ;;
    backtest)                           printf '%s' "scripts.backtest" ;;
    daytrader)                          printf '%s' "daytrader.runner" ;;
    *)                                  printf '%s' "scripts.main" ;;
  esac
}

RUN_MODULE="$(_resolve_module_for_mode "${RUN_MODE}")"
[[ -n "${RUN_MODULE_OVERRIDE:-}" ]] && RUN_MODULE="${RUN_MODULE_OVERRIDE}"
_py_has_module "${RUN_MODULE}" || _die "Python module not found: ${RUN_MODULE} (set RUN_MODULE_OVERRIDE if needed)"

# -----------------------------------------------------------------------------
# 12) Build argv (JSON-safe, file-safe, shlex-safe)
# -----------------------------------------------------------------------------
run_argv=()

if [[ -n "${RUN_ARGS_JSON:-}" ]]; then
  while IFS= read -r a; do [[ -n "$a" ]] && run_argv+=("$a"); done < <(
    "$PYTHON" - <<'PY'
import json, os, sys
raw = (os.environ.get("RUN_ARGS_JSON") or "").strip()
try:
    arr = json.loads(raw) if raw else []
    if not isinstance(arr, list): arr = []
except Exception:
    arr = []
for x in arr:
    if x is None: continue
    s = str(x).replace("\r", " ").replace("\n", " ")
    sys.stdout.write(s + "\n")
PY
  )
elif [[ -n "${RUN_ARGS_FILE:-}" && -f "${RUN_ARGS_FILE:-}" ]]; then
  while IFS= read -r ln || [[ -n "$ln" ]]; do
    ln="${ln//$'\r'/}"
    [[ -z "$ln" || "$ln" =~ ^[[:space:]]*# ]] && continue
    run_argv+=("$ln")
  done < "${RUN_ARGS_FILE}"
elif [[ -n "${RUN_ARGS:-}" ]]; then
  while IFS= read -r a; do [[ -n "$a" ]] && run_argv+=("$a"); done < <(
    RUN_ARGS_RAW="${RUN_ARGS}" "$PYTHON" - <<'PY'
import os, shlex, sys
raw = os.environ.get("RUN_ARGS_RAW", "") or ""
try:
    parts = shlex.split(raw)
except Exception:
    parts = raw.split()
for p in parts:
    p = str(p).replace("\r", " ").replace("\n", " ")
    sys.stdout.write(p + "\n")
PY
  )
fi

# -----------------------------------------------------------------------------
# 13) Supervisor launch (signal forwarding + restart policy)
# -----------------------------------------------------------------------------
_no_restart_csv="${MAIN_NO_RESTART_EXIT_CODES//;/,}"
_no_restart_csv="${_no_restart_csv// /,}"

child_pid=""
child_pgid=""

_get_pgid() {
  local pid="${1:-}"
  [[ -z "$pid" ]] && return 1
  if command -v ps >/dev/null 2>&1; then
    ps -o pgid= -p "$pid" 2>/dev/null | tr -d ' ' | head -n 1
    return 0
  fi
  return 1
}

_forward_signal() {
  local sig="${1:-TERM}"
  _log "Signal ${sig} received; forwarding"
  if [[ -n "${child_pgid:-}" ]]; then
    kill "-${sig}" -- "-${child_pgid}" 2>/dev/null || true
  elif [[ -n "${child_pid:-}" ]]; then
    kill "-${sig}" -- "${child_pid}" 2>/dev/null || true
  fi
}

trap '_forward_signal TERM' TERM
trap '_forward_signal INT'  INT
trap '_forward_signal HUP'  HUP
trap '_forward_signal QUIT' QUIT

_should_stop_on_rc() {
  local rc="${1:-1}" csv="${2:-}"
  local IFS=, x
  for x in $csv; do
    [[ -n "$x" && "$rc" == "$x" ]] && return 0
  done
  return 1
}

_start_child() {
  child_pid=""
  child_pgid=""

  if _is_true "${MAIN_USE_SETSID}" && command -v setsid >/dev/null 2>&1; then
    setsid "${PYTHON}" -m "${RUN_MODULE}" "${run_argv[@]}" &
    child_pid="$!"
  else
    "${PYTHON}" -m "${RUN_MODULE}" "${run_argv[@]}" &
    child_pid="$!"
  fi

  child_pgid="$(_get_pgid "$child_pid" || true)"
}

_log "Launch: module=${RUN_MODULE} args=${#run_argv[@]} restart=${MAIN_RESTART_ON_CRASH}"

if ! _is_true "${MAIN_RESTART_ON_CRASH}"; then
  exec "${PYTHON}" -m "${RUN_MODULE}" "${run_argv[@]}"
fi

restarts=0
backoff="${MAIN_RESTART_BACKOFF_S}"

while true; do
  rc=0
  _start_child
  wait "${child_pid}" || rc=$?

  child_pid=""
  child_pgid=""

  if [[ "$rc" == "0" ]]; then
    _log "Process exited cleanly (rc=0). Stopping."
    exit 0
  fi

  if _should_stop_on_rc "$rc" "$_no_restart_csv"; then
    _log "Process exited rc=${rc} (stop codes=${_no_restart_csv}). Stopping."
    exit "$rc"
  fi

  restarts=$((restarts + 1))
  if [[ "${MAIN_MAX_RESTARTS}" != "0" && "$restarts" -ge "${MAIN_MAX_RESTARTS}" ]]; then
    _log "Process crashed rc=${rc}. Max restarts reached (${MAIN_MAX_RESTARTS})."
    exit "$rc"
  fi

  jitter=0
  if [[ "${MAIN_RESTART_JITTER_S}" -gt 0 ]]; then
    jitter=$((RANDOM % (MAIN_RESTART_JITTER_S + 1)))
  fi

  _log "Process crashed rc=${rc}. Restarting in ${backoff}s (+${jitter}s) (attempt ${restarts})"
  sleep "$((backoff + jitter))"

  backoff=$((backoff * 2))
  if [[ "$backoff" -gt "${MAIN_RESTART_MAX_BACKOFF_S}" ]]; then
    backoff="${MAIN_RESTART_MAX_BACKOFF_S}"
  fi
done
