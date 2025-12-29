#!/usr/bin/env bash
# start.sh — God-Tier Bootstrap (Phase 0–7 LOCK-IN)
#
# CONTROL PLANE ONLY:
#   - secret-safe env loading (no eval by default)
#   - deterministic DSN selection (Render internal vs local external)
#   - wallet materialization
#   - check_env + preflight gates
#   - crash supervision + signal forwarding

set -Eeuo pipefail
IFS=$'\n\t'
umask 077
shopt -s inherit_errexit 2>/dev/null || true

ts()  { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
log() { printf '[start] %s %s\n' "$(ts)" "$*" >&2; }
die() { log "FATAL: $*"; exit 1; }

trap 'rc=$?; log "ERROR rc=$rc line=$LINENO"; exit $rc' ERR

is_true() {
  case "${1:-}" in
    1|true|TRUE|yes|YES|y|Y|on|ON|t|T) return 0 ;;
    *) return 1 ;;
  esac
}

usage() {
  cat >&2 <<'TXT'
[start] Usage:
  ./start.sh
  ./start.sh check-env [args...]
  ./start.sh preflight [args...]
  ./start.sh healthcheck [args...]
  ./start.sh smoke [args...]
  ./start.sh canary [args...]
  ./start.sh canary-quote [args...]

Key env:
  START_STRICT=0|1                 (default: 1 on Render, 0 local)
  START_PRECHECKS=0|1              (default: 1)
  START_PRECHECKS_STRICT=0|1       (default: START_STRICT)
  START_RUN_CHECK_ENV=0|1          (default: 1)

  START_ALLOW_SOURCE_ENV=0|1       (default: 0; safe parsing)
  START_ENV_OVERRIDE=0|1           (default: 0; env wins over file)

  # DSN routing
  SKIP_DSN_REQUIRED=0|1
  PG_DSN_INTERNAL=...              (Render internal)
  PG_DSN_EXTERNAL=...              (External)
  PNL_DSN=...                      (External asyncpg)
  DATABASE_URL / POSTGRES_DSN=...  (SQLAlchemy async)
TXT
}

log "────────────────────────────────"
log "   Solana Sniper Bootstrap V3+++"
log "────────────────────────────────"

# =============================================================================
# 0) REPO ROOT + RENDER DETECT
# =============================================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
export REPO_ROOT="${REPO_ROOT:-$SCRIPT_DIR}"

is_render=0
if [[ -n "${RENDER_SERVICE_ID:-}" || -n "${RENDER_INSTANCE_ID:-}" || -n "${RENDER_EXTERNAL_URL:-}" ]]; then
  is_render=1
elif is_true "${RENDER:-}"; then
  is_render=1
fi

# =============================================================================
# 0.1) ENV LOADING (SECRET-SAFE)
# =============================================================================
: "${START_ALLOW_SOURCE_ENV:=0}"
: "${START_ENV_OVERRIDE:=0}"

_load_env_safe() {
  local f="$1"
  [[ -z "$f" || ! -f "$f" ]] && return 0

  log "Loading env file (safe): $f"
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line//$'\r'/}"
    [[ -z "$line" ]] && continue
    [[ "$line" =~ ^[[:space:]]*# ]] && continue

    line="${line#"${line%%[![:space:]]*}"}"
    [[ "$line" == export\ * ]] && line="${line#export }"
    [[ "$line" != *"="* ]] && continue

    local k="${line%%=*}"
    local v="${line#*=}"

    k="${k%"${k##*[![:space:]]}"}"
    k="${k#"${k%%[![:space:]]*}"}"
    [[ -z "$k" ]] && continue
    [[ "$k" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue

    if [[ "${START_ENV_OVERRIDE}" != "1" && -n "${!k:-}" ]]; then
      continue
    fi

    v="${v#"${v%%[![:space:]]*}"}"

    if [[ "${v:0:1}" != "'" && "${v:0:1}" != '"' ]]; then
      v="${v%%[[:space:]]#*}"
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

_load_env_source() {
  local f="$1"
  [[ -z "$f" || ! -f "$f" ]] && return 0
  log "Loading env file (source): $f"
  set -a
  # shellcheck disable=SC1090
  . "$f"
  set +a
}

load_env_file() {
  local f="$1"
  [[ -z "$f" || ! -f "$f" ]] && return 0
  if is_true "${START_ALLOW_SOURCE_ENV}"; then
    _load_env_source "$f"
  else
    _load_env_safe "$f"
  fi
}

load_env_file "/etc/secrets/.env.extra"
load_env_file "/etc/secrets/.env"
load_env_file "./.env.extra"
load_env_file "./.env"

# =============================================================================
# 1) HARD DEFAULTS (LOCK-IN)
# =============================================================================
: "${MASTER_PHASE:=7}"

: "${MAIN_MODULE:=scripts.main}"
: "${MAIN_ARGS:=}"
: "${MAIN_ARGS_JSON:=}"
: "${MAIN_ARGS_FILE:=}"

: "${MAIN_RESTART_ON_CRASH:=1}"
: "${MAIN_MAX_RESTARTS:=0}"                 # 0 = infinite
: "${MAIN_RESTART_BACKOFF_S:=3}"
: "${MAIN_RESTART_MAX_BACKOFF_S:=30}"
: "${MAIN_RESTART_JITTER_S:=2}"
: "${MAIN_USE_SETSID:=1}"
: "${MAIN_NO_RESTART_EXIT_CODES:=130,143}"

if [[ -z "${START_STRICT:-}" ]]; then
  START_STRICT="$is_render"
fi

: "${START_RUN_CHECK_ENV:=1}"
: "${START_CHECK_ENV_STRICT:=$START_STRICT}"
: "${START_CHECK_ENV_JSON_FILE:=}"
: "${START_CHECK_ENV_SHOW_ENV:=0}"

: "${START_PRECHECKS:=1}"
: "${START_PRECHECKS_STRICT:=$START_STRICT}"
: "${START_PRECHECKS_CANARY:=0}"

: "${START_COMPILEALL:=0}"

export MASTER_PHASE MAIN_MODULE MAIN_ARGS MAIN_ARGS_JSON MAIN_ARGS_FILE \
  MAIN_RESTART_ON_CRASH MAIN_MAX_RESTARTS MAIN_RESTART_BACKOFF_S MAIN_RESTART_MAX_BACKOFF_S MAIN_RESTART_JITTER_S MAIN_USE_SETSID MAIN_NO_RESTART_EXIT_CODES \
  START_STRICT START_RUN_CHECK_ENV START_CHECK_ENV_STRICT START_CHECK_ENV_JSON_FILE START_CHECK_ENV_SHOW_ENV \
  START_PRECHECKS START_PRECHECKS_STRICT START_PRECHECKS_CANARY START_COMPILEALL

log "Boot flags: RENDER=${is_render} PHASE=${MASTER_PHASE} MAIN=${MAIN_MODULE} STRICT=${START_STRICT}"

# =============================================================================
# 2) PYTHON SELECTION (deterministic)
# =============================================================================
PYTHON="${PYTHON:-python}"

if [[ -x ".venv/bin/python" ]]; then
  PYTHON=".venv/bin/python"
elif [[ -x ".venv/Scripts/python.exe" ]]; then
  PYTHON=".venv/Scripts/python.exe"
elif command -v "$PYTHON" >/dev/null 2>&1; then
  :
elif command -v python3 >/dev/null 2>&1; then
  PYTHON="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON="python"
else
  die "Python executable not found (python/python3/.venv/bin/python)"
fi

export PYTHONUNBUFFERED=1
export PYTHONDONTWRITEBYTECODE=1
export PYTHONHASHSEED="${PYTHONHASHSEED:-0}"
export PYTHONUTF8="${PYTHONUTF8:-1}"

PY_PATHS=("$SCRIPT_DIR")
[[ -d "$SCRIPT_DIR/src" ]] && PY_PATHS+=("$SCRIPT_DIR/src")
if [[ -n "${PYTHONPATH:-}" ]]; then
  export PYTHONPATH="$(IFS=:; echo "${PY_PATHS[*]}"):$PYTHONPATH"
else
  export PYTHONPATH="$(IFS=:; echo "${PY_PATHS[*]}")"
fi

log "Using python: $($PYTHON -V 2>&1)"

if is_true "${START_COMPILEALL}"; then
  log "compileall enabled (best-effort)"
  "$PYTHON" -m compileall -q . || die "compileall failed"
fi

# =============================================================================
# 3) COMMAND MODE
# =============================================================================
cmd="${1:-}"
case "$cmd" in
  -h|--help|help) usage; exit 0 ;;
  check-env|check_env) shift || true; exec "$PYTHON" -m scripts.check_env "$@" ;;
  preflight)        shift || true; exec "$PYTHON" -m scripts.preflight "$@" ;;
  healthcheck)      shift || true; exec "$PYTHON" -m scripts.healthcheck "$@" ;;
  smoke|smoke_lightning) shift || true; exec "$PYTHON" -m scripts.smoke_lightning "$@" ;;
  canary|canary_trade) shift || true; exec "$PYTHON" -m scripts.canary_trade "$@" ;;
  canary-quote|canary_quote)
    shift || true
    CANARY_MODE=dryrun DRYRUN=1 exec "$PYTHON" -m scripts.canary_trade --stage quote "$@" ;;
esac

# =============================================================================
# 4) RUNTIME DIRS (Render + local safe)
# =============================================================================
DATA_DIR="${DATA_DIR:-/opt/render/data}"
SECRETS_DIR="${SECRETS_DIR:-/opt/render/.secrets}"
TMP_DIR="${TMP_DIR:-/tmp}"

ensure_dir() {
  local d="$1"
  [[ -z "$d" ]] && return 1
  mkdir -p "$d" 2>/dev/null || return 1
  chmod 700 "$d" 2>/dev/null || true
  return 0
}

if ! ensure_dir "$SECRETS_DIR"; then
  SECRETS_DIR="${TMP_DIR%/}/sniper_secrets"
  ensure_dir "$SECRETS_DIR" || die "Cannot create secrets dir"
  log "Using fallback secrets dir: $SECRETS_DIR"
fi

if ! ensure_dir "$DATA_DIR"; then
  DATA_DIR="${TMP_DIR%/}/sniper_data"
  ensure_dir "$DATA_DIR" || die "Cannot create data dir"
  log "Using fallback data dir: $DATA_DIR"
fi

export DATA_DIR SECRETS_DIR TMP_DIR
export XDG_CACHE_HOME="${XDG_CACHE_HOME:-$DATA_DIR/.cache}"
export XDG_CONFIG_HOME="${XDG_CONFIG_HOME:-$DATA_DIR/.config}"
mkdir -p "$XDG_CACHE_HOME" "$XDG_CONFIG_HOME" 2>/dev/null || true
chmod 700 "$XDG_CACHE_HOME" "$XDG_CONFIG_HOME" 2>/dev/null || true

# =============================================================================
# 5) Masking + DSN normalization
# =============================================================================
mask_url() {
  local u="${1:-}"
  [[ -z "$u" ]] && { printf '%s' ""; return; }
  if command -v sed >/dev/null 2>&1; then
    u="$(printf '%s' "$u" | sed -E 's#(://)[^/@]+@#\1***@#')"
  fi
  if [[ "$u" == *\?* ]]; then printf '%s' "${u%%\?*}?..."; else printf '%s' "$u"; fi
}

extract_host() {
  local u="${1:-}"
  [[ -z "$u" ]] && { printf '%s' ""; return; }
  if command -v sed >/dev/null 2>&1; then
    printf '%s' "$u" | sed -E 's#^[a-zA-Z0-9+.-]+://([^/@]+@)?([^/:?]+).*#\2#' 2>/dev/null || true
  else
    printf '%s' ""
  fi
}

is_render_internal_host() {
  # Render internal postgres host often looks like: dpg-xxxx-a (no dot)
  local u="${1:-}"
  local host
  host="$(extract_host "$u")"
  host="${host,,}"
  [[ -n "$host" && "$host" == dpg-* && "$host" != *.* ]]
}

is_render_external_host() {
  local u="${1:-}"
  local host
  host="$(extract_host "$u")"
  host="${host,,}"
  [[ -n "$host" && ( "$host" == *.render.com || "$host" == *postgres.render.com* ) ]]
}

ensure_sslmode_require() {
  local v="$1"
  v="${v//ssl=true/sslmode=require}"
  v="${v//ssl=1/sslmode=require}"
  if [[ "$v" == *"sslmode="* ]] && command -v sed >/dev/null 2>&1; then
    printf '%s' "$v" | sed -E 's/(sslmode=)[^&]*/\1require/g'
    return 0
  fi
  if [[ "$v" != *"sslmode="* ]]; then
    if [[ "$v" == *\?* ]]; then printf '%s' "${v}&sslmode=require"; else printf '%s' "${v}?sslmode=require"; fi
  else
    printf '%s' "$v"
  fi
}

strip_sqlalchemy_driver() {
  local v="$1"
  if [[ "$v" =~ ^postgresql\+ ]]; then
    if command -v sed >/dev/null 2>&1; then
      v="$(printf '%s' "$v" | sed -E 's#^postgresql\+[^:]+://#postgresql://#')"
    else
      v="postgresql://${v#postgresql+*://}"
    fi
  fi
  printf '%s' "$v"
}

normalize_sqlalchemy_async_dsn() {
  local v="$1"
  [[ -z "$v" ]] && { printf '%s' ""; return; }
  [[ "$v" =~ ^postgres:// ]] && v="${v/postgres:\/\//postgresql:\/\/}"
  v="$(strip_sqlalchemy_driver "$v")"
  [[ "$v" =~ ^postgresql:// ]] && v="${v/postgresql:\/\//postgresql+asyncpg://}"
  if is_render_external_host "$v" || is_true "${PG_FORCE_SSLMODE_REQUIRE:-0}"; then
    v="$(ensure_sslmode_require "$v")"
  fi
  printf '%s' "$v"
}

normalize_asyncpg_dsn() {
  local v="$1"
  [[ -z "$v" ]] && { printf '%s' ""; return; }
  [[ "$v" =~ ^postgres:// ]] && v="${v/postgres:\/\//postgresql:\/\/}"
  if [[ "$v" =~ ^postgresql\+ ]]; then
    v="$(strip_sqlalchemy_driver "$v")"
  fi
  v="${v/postgresql+asyncpg:\/\//postgresql:\/\/}"
  if is_render_external_host "$v" || is_true "${PG_FORCE_SSLMODE_REQUIRE:-0}"; then
    v="$(ensure_sslmode_require "$v")"
  fi
  printf '%s' "$v"
}

to_asyncpg_from_sqlalchemy() {
  local v="$1"
  v="$(strip_sqlalchemy_driver "$v")"
  normalize_asyncpg_dsn "$v"
}

to_sqlalchemy_from_asyncpg() {
  local v="$1"
  [[ "$v" =~ ^postgresql:// ]] && v="${v/postgresql:\/\//postgresql+asyncpg://}"
  normalize_sqlalchemy_async_dsn "$v"
}

# =============================================================================
# 6) WALLET BOOTSTRAP
# =============================================================================
wallet_path="${SECRETS_DIR%/}/wallet.json"
: "${START_WALLET_OVERWRITE:=0}"

write_wallet_file() {
  local content="$1"
  [[ -z "$content" ]] && return 0
  if [[ ! -f "$wallet_path" ]] || is_true "${START_WALLET_OVERWRITE}"; then
    printf '%s' "$content" >"$wallet_path"
    chmod 600 "$wallet_path" 2>/dev/null || true
    log "Created wallet.json (bootstrap)"
  fi
}

if [[ -n "${KEYPAIR_PATH:-}" && -f "${KEYPAIR_PATH:-}" ]]; then
  export WALLET_PRIVATE_KEY="${KEYPAIR_PATH}"
fi

if [[ -n "${WALLET_PRIVATE_KEY:-}" && -f "${WALLET_PRIVATE_KEY:-}" ]]; then
  :
else
  if [[ -n "${WALLET_JSON:-}" ]]; then
    write_wallet_file "${WALLET_JSON}"
  fi

  b64_src="${WALLET_JSON_B64:-${KEYPAIR_B64:-}}"
  if [[ -n "${b64_src:-}" ]] && ( [[ ! -f "$wallet_path" ]] || is_true "${START_WALLET_OVERWRITE}" ); then
    decoded=""
    if command -v base64 >/dev/null 2>&1; then
      decoded="$(printf '%s' "$b64_src" | base64 -d 2>/dev/null || true)"
    elif command -v openssl >/dev/null 2>&1; then
      decoded="$(printf '%s' "$b64_src" | openssl base64 -d 2>/dev/null || true)"
    fi
    [[ -n "$decoded" ]] && write_wallet_file "$decoded"
  fi

  if [[ -n "${WALLET_PRIVATE_KEY:-}" && ( ! -f "$wallet_path" || is_true "${START_WALLET_OVERWRITE}" ) ]]; then
    if [[ "${WALLET_PRIVATE_KEY:0:1}" == "[" || "${WALLET_PRIVATE_KEY:0:1}" == "{" ]]; then
      write_wallet_file "${WALLET_PRIVATE_KEY}"
      log "Wallet detected in WALLET_PRIVATE_KEY (raw json) → wrote wallet.json"
    fi
  fi

  [[ -f "$wallet_path" ]] && export WALLET_PRIVATE_KEY="$wallet_path"
fi

if [[ -n "${WALLET_PRIVATE_KEY:-}" && -f "${WALLET_PRIVATE_KEY:-}" ]]; then
  log "Wallet: present (path) $(basename "${WALLET_PRIVATE_KEY}")"
fi

# =============================================================================
# 7) DSN ROUTING (Render internal vs local external) — LOCK-IN
# =============================================================================
START_REQUIRE_DSN="${START_STRICT}"
is_true "${SKIP_DSN_REQUIRED:-0}" && START_REQUIRE_DSN=0

pick_dsn=""   # base DSN in whichever form user provided

try_pick() {
  local candidate="${1:-}"
  [[ -z "$candidate" ]] && return 1

  # If local, ignore render-internal host DSNs
  if [[ "$is_render" == "0" ]] && is_render_internal_host "$candidate"; then
    return 1
  fi

  pick_dsn="$candidate"
  return 0
}

# Priority:
# - Render: prefer internal -> then SQLA -> then external
# - Local : prefer external -> then asyncpg -> then SQLA (but never internal)
if [[ "$is_render" == "1" ]]; then
  try_pick "${PG_DSN_INTERNAL:-}" || true
  try_pick "${POSTGRES_DSN:-}" || true
  try_pick "${DATABASE_URL:-}" || true
  try_pick "${PG_DSN:-}" || true
  try_pick "${PNL_DSN:-}" || true
  try_pick "${PG_DSN_EXTERNAL:-}" || true
else
  try_pick "${PG_DSN_EXTERNAL:-}" || true
  try_pick "${PNL_DSN:-}" || true
  try_pick "${PG_DSN:-}" || true
  try_pick "${POSTGRES_DSN:-}" || true
  try_pick "${DATABASE_URL:-}" || true
fi

if is_true "${START_REQUIRE_DSN}" && [[ -z "${pick_dsn:-}" ]]; then
  die "Missing DB DSN. Provide PG_DSN_EXTERNAL (local) or PG_DSN_INTERNAL (Render) or DATABASE_URL/POSTGRES_DSN (or set SKIP_DSN_REQUIRED=1)."
fi

# Normalize/derive canonical env:
# - DATABASE_URL / POSTGRES_DSN: SQLAlchemy async (postgresql+asyncpg://)
# - PG_DSN / PNL_DSN: asyncpg (postgresql://)
if [[ -n "${pick_dsn:-}" ]]; then
  if [[ "$pick_dsn" =~ ^postgresql\+ ]]; then
    DATABASE_URL="$(normalize_sqlalchemy_async_dsn "$pick_dsn")"
    PG_DSN="$(to_asyncpg_from_sqlalchemy "$DATABASE_URL")"
  else
    # assume asyncpg-ish
    PG_DSN="$(normalize_asyncpg_dsn "$pick_dsn")"
    DATABASE_URL="$(to_sqlalchemy_from_asyncpg "$PG_DSN")"
  fi

  POSTGRES_DSN="${POSTGRES_DSN:-$DATABASE_URL}"
  PNL_DSN="${PNL_DSN:-$PG_DSN}"

  export DATABASE_URL POSTGRES_DSN PG_DSN PNL_DSN
  log "DB SQLA async : $(mask_url "${DATABASE_URL}")"
  log "DB asyncpg   : $(mask_url "${PG_DSN}")"
fi

# =============================================================================
# 8) JUPITER NORMALIZATION (no key logging)
# =============================================================================
: "${JUP_BASE_URL:=https://api.jup.ag}"
: "${JUP_ALT_BASE_URLS:=$JUP_BASE_URL}"
: "${JUPITER_SWAP_API:=$JUP_BASE_URL}"
export JUP_BASE_URL JUP_ALT_BASE_URLS JUPITER_SWAP_API
log "Jupiter base URL: ${JUP_BASE_URL}"

# =============================================================================
# 9) CHECK_ENV
# =============================================================================
if is_true "${START_RUN_CHECK_ENV}"; then
  log "check_env enabled (strict=${START_CHECK_ENV_STRICT})"
  ce_args=(--json-only)
  is_true "${START_CHECK_ENV_STRICT}" && ce_args+=(--strict)

  set +e
  if [[ -n "${START_CHECK_ENV_JSON_FILE:-}" ]]; then
    mkdir -p "$(dirname "${START_CHECK_ENV_JSON_FILE}")" 2>/dev/null || true
    CHECK_ENV_JSON=1 CHECK_ENV_JSON_ONLY=1 "$PYTHON" -m scripts.check_env "${ce_args[@]}" >"${START_CHECK_ENV_JSON_FILE}"
    ce_rc=$?
  else
    CHECK_ENV_JSON=1 CHECK_ENV_JSON_ONLY=1 "$PYTHON" -m scripts.check_env "${ce_args[@]}" >/dev/null
    ce_rc=$?
  fi
  set -e
  [[ "$ce_rc" == "0" ]] || die "check_env failed (rc=$ce_rc)"
else
  log "check_env disabled (START_RUN_CHECK_ENV=0)"
fi

# =============================================================================
# 10) PREFLIGHT
# =============================================================================
if is_true "${START_PRECHECKS}"; then
  log "Preflight enabled (strict=${START_PRECHECKS_STRICT} canary=${START_PRECHECKS_CANARY})"
  preflight_args=()
  is_true "${START_PRECHECKS_STRICT}" && preflight_args+=(--strict)
  is_true "${START_PRECHECKS_CANARY}" && preflight_args+=(--canary)

  set +e
  "$PYTHON" -m scripts.preflight "${preflight_args[@]}"
  pfrc=$?
  set -e

  if [[ "$pfrc" == "0" ]]; then
    log "Preflight OK"
  elif [[ "$pfrc" == "1" && ! is_true "${START_PRECHECKS_STRICT}" ]]; then
    log "Preflight WARN (optional failed) → continuing (strict=0)"
  else
    die "preflight failed (rc=$pfrc)"
  fi
else
  log "Preflight disabled (START_PRECHECKS=0)"
fi

# =============================================================================
# 11) LAUNCH (restart supervisor + signal forwarding)
# =============================================================================
log "Launching: ${PYTHON} -m ${MAIN_MODULE} [args]"

main_argv=()

if [[ -n "${MAIN_ARGS_JSON:-}" ]]; then
  while IFS= read -r a; do
    [[ -z "$a" ]] && continue
    main_argv+=( "$a" )
  done < <(
    "$PYTHON" - <<'PY'
import json, os, sys
raw = os.environ.get("MAIN_ARGS_JSON","").strip()
try:
    arr = json.loads(raw) if raw else []
    if not isinstance(arr, list):
        arr = []
except Exception:
    arr = []
for x in arr:
    if x is None:
        continue
    s = str(x).replace("\r"," ").replace("\n"," ")
    sys.stdout.write(s + "\n")
PY
  )
elif [[ -n "${MAIN_ARGS_FILE:-}" && -f "${MAIN_ARGS_FILE:-}" ]]; then
  while IFS= read -r ln || [[ -n "$ln" ]]; do
    ln="${ln//$'\r'/}"
    [[ -z "$ln" ]] && continue
    [[ "$ln" =~ ^[[:space:]]*# ]] && continue
    main_argv+=( "$ln" )
  done < "${MAIN_ARGS_FILE}"
elif [[ -n "${MAIN_ARGS:-}" ]]; then
  # shellcheck disable=SC2206
  main_argv=( ${MAIN_ARGS} )
fi

child_pid=""
child_pgid=""

_forward_signal() {
  local sig="$1"
  log "Signal ${sig} received; forwarding"
  if [[ -n "${child_pgid:-}" ]]; then
    kill "-${sig}" -- "-${child_pgid}" 2>/dev/null || true
  elif [[ -n "${child_pid:-}" ]]; then
    kill "-${sig}" "${child_pid}" 2>/dev/null || true
  fi
}

trap '_forward_signal TERM' TERM
trap '_forward_signal INT'  INT
trap '_forward_signal HUP'  HUP
trap '_forward_signal QUIT' QUIT

if ! is_true "${MAIN_RESTART_ON_CRASH}"; then
  exec "${PYTHON}" -m "${MAIN_MODULE}" "${main_argv[@]}"
fi

no_restart_codes_csv="${MAIN_NO_RESTART_EXIT_CODES:-130,143}"
no_restart_codes_csv="${no_restart_codes_csv//;/,}"
no_restart_codes_csv="${no_restart_codes_csv// /,}"

should_stop_on_rc() {
  local rc="$1"
  local csv="$2"
  local IFS=,
  for x in $csv; do
    [[ -z "$x" ]] && continue
    [[ "$rc" == "$x" ]] && return 0
  done
  return 1
}

[[ "${MAIN_MAX_RESTARTS:-0}" =~ ^[0-9]+$ ]] || MAIN_MAX_RESTARTS=0
[[ "${MAIN_RESTART_JITTER_S:-2}" =~ ^[0-9]+$ ]] || MAIN_RESTART_JITTER_S=2
[[ "${MAIN_RESTART_MAX_BACKOFF_S:-30}" =~ ^[0-9]+$ ]] || MAIN_RESTART_MAX_BACKOFF_S=30
[[ "${MAIN_RESTART_BACKOFF_S:-3}" =~ ^[0-9]+$ ]] || MAIN_RESTART_BACKOFF_S=3

restarts=0
backoff="${MAIN_RESTART_BACKOFF_S}"

_start_child() {
  child_pid=""
  child_pgid=""
  if is_true "${MAIN_USE_SETSID}" && command -v setsid >/dev/null 2>&1; then
    setsid "${PYTHON}" -m "${MAIN_MODULE}" "${main_argv[@]}" &
    child_pid="$!"
    child_pgid="$child_pid"
  else
    "${PYTHON}" -m "${MAIN_MODULE}" "${main_argv[@]}" &
    child_pid="$!"
  fi
}

while true; do
  rc=0
  _start_child
  wait "${child_pid}" || rc=$?
  child_pid=""; child_pgid=""

  if [[ "$rc" == "0" ]]; then
    log "Main exited cleanly (rc=0). Stopping."
    exit 0
  fi

  if should_stop_on_rc "$rc" "$no_restart_codes_csv"; then
    log "Main exited rc=${rc} (stop-on-exit-codes=${no_restart_codes_csv}). Stopping."
    exit "$rc"
  fi

  restarts=$((restarts + 1))
  if [[ "${MAIN_MAX_RESTARTS}" != "0" && "$restarts" -ge "${MAIN_MAX_RESTARTS}" ]]; then
    log "Main crashed rc=${rc}. Max restarts reached (${MAIN_MAX_RESTARTS})."
    exit "$rc"
  fi

  jitter=0
  if [[ "${MAIN_RESTART_JITTER_S}" -gt 0 ]]; then
    jitter=$((RANDOM % (MAIN_RESTART_JITTER_S + 1)))
  fi

  log "Main crashed rc=${rc}. Restarting in ${backoff}s (+${jitter}s jitter) (attempt ${restarts})…"
  sleep "$((backoff + jitter))"

  backoff=$((backoff * 2))
  if [[ "$backoff" -gt "${MAIN_RESTART_MAX_BACKOFF_S}" ]]; then
    backoff="${MAIN_RESTART_MAX_BACKOFF_S}"
  fi
done
