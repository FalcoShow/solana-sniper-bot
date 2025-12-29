#!/usr/bin/env bash
# start.sh — God-Tier Bootstrap (Phase 0–7 LOCK-IN)
#
# CONTROL PLANE ONLY:
#   - bulletproof env loading (dotenv-safe; never source secrets as shell)
#   - deterministic DSN selection (Render internal vs external/local)
#   - wallet materialization to a private file
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

# -----------------------------------------------------------------------------
# 0) REPO ROOT + RENDER DETECT
# -----------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
export REPO_ROOT="${REPO_ROOT:-$SCRIPT_DIR}"

is_render=0
if [[ -n "${RENDER_SERVICE_ID:-}" || -n "${RENDER_INSTANCE_ID:-}" || -n "${RENDER_EXTERNAL_URL:-}" ]]; then
  is_render=1
elif is_true "${RENDER:-}"; then
  is_render=1
fi

log "────────────────────────────────"
log "   Solana Sniper Bootstrap V3+++"
log "────────────────────────────────"
log "Boot: RENDER=${is_render} cwd=${REPO_ROOT}"

# -----------------------------------------------------------------------------
# 1) PYTHON SELECTION (deterministic, minimal assumptions)
# -----------------------------------------------------------------------------
PYTHON="${PYTHON:-python3}"
if command -v "$PYTHON" >/dev/null 2>&1; then
  :
elif command -v python >/dev/null 2>&1; then
  PYTHON="python"
else
  die "Python executable not found (python3/python)"
fi

# Prefer venv python if present (harmless on Render; useful locally)
if [[ -x ".venv/bin/python" ]]; then
  PYTHON=".venv/bin/python"
elif [[ -x ".venv/Scripts/python.exe" ]]; then
  PYTHON=".venv/Scripts/python.exe"
fi

export PYTHONUNBUFFERED=1
export PYTHONDONTWRITEBYTECODE=1
export PYTHONHASHSEED="${PYTHONHASHSEED:-0}"
export PYTHONUTF8="${PYTHONUTF8:-1}"

log "Using python: $($PYTHON -V 2>&1)"

# -----------------------------------------------------------------------------
# 2) BULLETPROOF ENV LOADER (dotenv → exports; NEVER source secrets directly)
# -----------------------------------------------------------------------------
: "${START_ENV_OVERRIDE:=0}"          # 1 => file overrides existing env
: "${START_ENV_LINT:=1}"              # 1 => print loaded count + ignored lines summary

# Parses KEY=VALUE lines, emits: export KEY='value'
# - ignores invalid lines
# - strips CRLF + BOM
# - supports optional "export KEY=..."
# - strips matching surrounding quotes on values (single/double)
# - does NOT execute anything, no $(...), no backticks, no redirects
dotenv_to_exports() {
  local f="$1"
  [[ -z "$f" || ! -f "$f" ]] && return 0

  "$PYTHON" - <<'PY' "$f" "${START_ENV_OVERRIDE:-0}" "${START_ENV_LINT:-1}"
import os, re, shlex, sys

path = sys.argv[1]
override = sys.argv[2] == "1"
lint = sys.argv[3] == "1"

key_re = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

loaded = 0
ignored = 0
ignored_samples = []

def strip_bom(s: str) -> str:
  return s[1:] if s.startswith("\ufeff") else s

with open(path, "r", encoding="utf-8", errors="ignore") as f:
  for lineno, raw in enumerate(f, 1):
    raw = raw.replace("\r", "")
    s = strip_bom(raw).strip()
    if not s or s.startswith("#"):
      continue
    if s.startswith("export "):
      s = s[7:].lstrip()
    if "=" not in s:
      ignored += 1
      if len(ignored_samples) < 5:
        ignored_samples.append((lineno, raw.rstrip("\n")))
      continue

    k, v = s.split("=", 1)
    k = k.strip()
    if not key_re.match(k):
      ignored += 1
      if len(ignored_samples) < 5:
        ignored_samples.append((lineno, raw.rstrip("\n")))
      continue

    if (not override) and (k in os.environ) and os.environ.get(k, "") != "":
      continue

    v = v.lstrip()

    # If the value is quoted, strip surrounding quotes only.
    if len(v) >= 2 and ((v[0] == v[-1] == '"') or (v[0] == v[-1] == "'")):
      v = v[1:-1]

    print(f"export {k}={shlex.quote(v)}")
    loaded += 1

if lint:
  # Write lint info to stderr so it shows in logs but doesn't affect sourcing
  sys.stderr.write(f"[start] dotenv: {path} loaded={loaded} ignored={ignored}\n")
  if ignored_samples:
    sys.stderr.write("[start] dotenv: ignored sample lines (lineno: text):\n")
    for ln, txt in ignored_samples:
      sys.stderr.write(f"[start]   {ln}: {txt}\n")
PY
}

load_env_file() {
  local f="$1"
  [[ -z "$f" || ! -f "$f" ]] && return 0
  log "Loading env file (dotenv-safe): $f"
  local tmp="/tmp/env_export_$(basename "$f").sh"
  dotenv_to_exports "$f" >"$tmp"
  # shellcheck disable=SC1090
  source "$tmp"
  rm -f "$tmp" || true
}

# Load order (Render secret files first)
load_env_file "/etc/secrets/.env.extra"
load_env_file "/etc/secrets/.env"
load_env_file "./.env.extra"
load_env_file "./.env"

# -----------------------------------------------------------------------------
# 3) HARD DEFAULTS (LOCK-IN)
# -----------------------------------------------------------------------------
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

: "${START_PRECHECKS:=1}"
: "${START_PRECHECKS_STRICT:=$START_STRICT}"
: "${START_PRECHECKS_CANARY:=0}"

export MASTER_PHASE MAIN_MODULE MAIN_ARGS MAIN_ARGS_JSON MAIN_ARGS_FILE \
  MAIN_RESTART_ON_CRASH MAIN_MAX_RESTARTS MAIN_RESTART_BACKOFF_S MAIN_RESTART_MAX_BACKOFF_S MAIN_RESTART_JITTER_S MAIN_USE_SETSID MAIN_NO_RESTART_EXIT_CODES \
  START_STRICT START_RUN_CHECK_ENV START_CHECK_ENV_STRICT \
  START_PRECHECKS START_PRECHECKS_STRICT START_PRECHECKS_CANARY

log "Flags: PHASE=${MASTER_PHASE} MAIN=${MAIN_MODULE} STRICT=${START_STRICT}"

# -----------------------------------------------------------------------------
# 4) COMMAND MODE (utility entrypoints)
# -----------------------------------------------------------------------------
cmd="${1:-}"
case "$cmd" in
  -h|--help|help)
    cat >&2 <<'TXT'
[start] Commands:
  ./start.sh
  ./start.sh check-env [args...]
  ./start.sh preflight [args...]
  ./start.sh healthcheck [args...]
  ./start.sh smoke [args...]
  ./start.sh canary [args...]
TXT
    exit 0
    ;;
  check-env|check_env) shift || true; exec "$PYTHON" -m scripts.check_env "$@" ;;
  preflight)           shift || true; exec "$PYTHON" -m scripts.preflight "$@" ;;
  healthcheck)         shift || true; exec "$PYTHON" -m scripts.healthcheck "$@" ;;
  smoke|smoke_lightning) shift || true; exec "$PYTHON" -m scripts.smoke_lightning "$@" ;;
  canary|canary_trade) shift || true; exec "$PYTHON" -m scripts.canary_trade "$@" ;;
esac

# -----------------------------------------------------------------------------
# 5) RUNTIME DIRS (Render + local safe)
# -----------------------------------------------------------------------------
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

# -----------------------------------------------------------------------------
# 6) DSN NORMALIZATION + ROUTING (Render-first)
# -----------------------------------------------------------------------------
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
  local host
  host="$(extract_host "${1:-}")"
  host="${host,,}"
  [[ -n "$host" && "$host" == dpg-* && "$host" != *.* ]]
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
  v="$(ensure_sslmode_require "$v")"
  printf '%s' "$v"
}

normalize_asyncpg_dsn() {
  local v="$1"
  [[ -z "$v" ]] && { printf '%s' ""; return; }
  [[ "$v" =~ ^postgres:// ]] && v="${v/postgres:\/\//postgresql:\/\/}"
  v="$(strip_sqlalchemy_driver "$v")"
  v="${v/postgresql+asyncpg:\/\//postgresql:\/\/}"
  v="$(ensure_sslmode_require "$v")"
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

START_REQUIRE_DSN="${START_STRICT}"
is_true "${SKIP_DSN_REQUIRED:-0}" && START_REQUIRE_DSN=0

pick_dsn=""

try_pick() {
  local candidate="${1:-}"
  [[ -z "$candidate" ]] && return 1
  if [[ "$is_render" == "0" ]] && is_render_internal_host "$candidate"; then
    return 1
  fi
  pick_dsn="$candidate"
  return 0
}

if [[ "$is_render" == "1" ]]; then
  try_pick "${PG_DSN_INTERNAL:-}" || true
  try_pick "${POSTGRES_DSN:-}"   || true
  try_pick "${DATABASE_URL:-}"   || true
  try_pick "${PG_DSN_EXTERNAL:-}"|| true
  try_pick "${PG_DSN:-}"         || true
  try_pick "${PNL_DSN:-}"        || true
else
  try_pick "${PG_DSN_EXTERNAL:-}"|| true
  try_pick "${PG_DSN:-}"         || true
  try_pick "${PNL_DSN:-}"        || true
  try_pick "${POSTGRES_DSN:-}"   || true
  try_pick "${DATABASE_URL:-}"   || true
fi

if is_true "${START_REQUIRE_DSN}" && [[ -z "${pick_dsn:-}" ]]; then
  die "Missing DB DSN. Set PG_DSN_INTERNAL (Render) or DATABASE_URL/POSTGRES_DSN (or SKIP_DSN_REQUIRED=1)."
fi

if [[ -n "${pick_dsn:-}" ]]; then
  if [[ "$pick_dsn" =~ ^postgresql\+ ]]; then
    DATABASE_URL="$(normalize_sqlalchemy_async_dsn "$pick_dsn")"
    PG_DSN="$(to_asyncpg_from_sqlalchemy "$DATABASE_URL")"
  else
    PG_DSN="$(normalize_asyncpg_dsn "$pick_dsn")"
    DATABASE_URL="$(to_sqlalchemy_from_asyncpg "$PG_DSN")"
  fi

  POSTGRES_DSN="${POSTGRES_DSN:-$DATABASE_URL}"
  PNL_DSN="${PNL_DSN:-$PG_DSN}"

  export DATABASE_URL POSTGRES_DSN PG_DSN PNL_DSN
  log "DB SQLA async : $(mask_url "${DATABASE_URL}")"
  log "DB asyncpg    : $(mask_url "${PG_DSN}")"
fi

# -----------------------------------------------------------------------------
# 7) WALLET BOOTSTRAP
# -----------------------------------------------------------------------------
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

# -----------------------------------------------------------------------------
# 8) CHECK_ENV + PREFLIGHT GATES
# -----------------------------------------------------------------------------
if is_true "${START_RUN_CHECK_ENV}"; then
  log "check_env enabled (strict=${START_CHECK_ENV_STRICT})"
  ce_args=(--json-only)
  is_true "${START_CHECK_ENV_STRICT}" && ce_args+=(--strict)
  "$PYTHON" -m scripts.check_env "${ce_args[@]}" >/dev/null
else
  log "check_env disabled (START_RUN_CHECK_ENV=0)"
fi

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

# -----------------------------------------------------------------------------
# 9) LAUNCH (restart supervisor + signal forwarding)
# -----------------------------------------------------------------------------
log "Launching: ${PYTHON} -m ${MAIN_MODULE}"

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
  if not isinstance(arr, list): arr = []
except Exception:
  arr = []
for x in arr:
  if x is None: continue
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
