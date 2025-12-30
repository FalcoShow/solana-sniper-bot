#!/usr/bin/env bash
# shellcheck shell=bash
# start.sh — God-Tier Bootstrap (Phase 0–7 LOCK-IN)
#
# Render-first production launcher:
#   - dotenv-safe env loading (NO eval by default)
#   - wallet materialization (optional; never logs secrets)
#   - DSN normalization for SQLAlchemy(asyncpg) + asyncpg
#   - check_env + preflight gates
#   - optional DB canary on boot (Render)
#   - crash supervision + signal forwarding

set -Eeuo pipefail
IFS=$'\n\t'
umask 077
shopt -s inherit_errexit 2>/dev/null || true

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
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
  ./start.sh test-pg [args...]
  ./start.sh canary [args...]

Key env:
  START_STRICT=0|1                 (default: 1 on Render, 0 locally)
  START_RUN_CHECK_ENV=0|1          (default: 1)
  START_PRECHECKS=0|1              (default: 1)
  START_DB_CANARY=0|1              (default: 1 on Render, 0 locally)
  SKIP_DSN_REQUIRED=0|1            (default: 0 on Render, 1 locally)

  START_ALLOW_SOURCE_ENV=0|1       (default: 0; if 1, will 'source' env files)
  START_ENV_OVERRIDE=0|1           (default: 0; if 0, existing env wins)

  MAIN_MODULE=scripts.main
  MAIN_ARGS_JSON='["--foo","bar baz"]'  (preferred)
  MAIN_ARGS='--foo bar'                 (fallback)
  MAIN_ARGS_FILE=./args.txt             (one arg per line)

DB DSNs (recommend):
  PG_DSN_INTERNAL   (Render internal host)
  PG_DSN_EXTERNAL   (External host; local-friendly)
  POSTGRES_DSN / DATABASE_URL / PNL_DSN / PG_DSN supported.

Wallet:
  KEYPAIR_PATH=path/to/wallet.json
  WALLET_PRIVATE_KEY=path/or/raw-json
  WALLET_JSON=raw-json
  WALLET_JSON_B64=base64(raw-json)
TXT
}

mask_url() {
  local u="${1:-}"
  [[ -z "$u" ]] && { printf '%s' ""; return; }
  if command -v sed >/dev/null 2>&1; then
    u="$(printf '%s' "$u" | sed -E 's#(://)[^/@]+@#\1***@#')"
  fi
  if [[ "$u" == *\?* ]]; then printf '%s' "${u%%\?*}?..."; else printf '%s' "$u"; fi
}

# -----------------------------------------------------------------------------
# Banner
# -----------------------------------------------------------------------------
log "────────────────────────────────"
log "   Solana Sniper Bootstrap V3+++"
log "────────────────────────────────"

# -----------------------------------------------------------------------------
# 0) Repo root + Render detect
# -----------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
export REPO_ROOT="${REPO_ROOT:-$SCRIPT_DIR}"

is_render=0
if [[ -n "${RENDER_SERVICE_ID:-}" || -n "${RENDER_INSTANCE_ID:-}" || -n "${RENDER_EXTERNAL_URL:-}" ]]; then
  is_render=1
else
  if is_true "${RENDER:-}"; then
    is_render=1
  fi
fi

# -----------------------------------------------------------------------------
# 1) Defaults (Render strict, local permissive)
# -----------------------------------------------------------------------------
: "${MAIN_MODULE:=scripts.main}"
: "${MAIN_ARGS:=}"
: "${MAIN_ARGS_JSON:=}"
: "${MAIN_ARGS_FILE:=}"

if [[ -z "${START_STRICT:-}" ]]; then
  START_STRICT="$is_render"
fi

: "${START_RUN_CHECK_ENV:=1}"
: "${START_PRECHECKS:=1}"

if [[ -z "${START_DB_CANARY:-}" ]]; then
  START_DB_CANARY="$is_render"
fi

if [[ -z "${SKIP_DSN_REQUIRED:-}" ]]; then
  if [[ "$is_render" == "1" ]]; then
    SKIP_DSN_REQUIRED="0"
  else
    SKIP_DSN_REQUIRED="1"
  fi
fi

: "${START_ALLOW_SOURCE_ENV:=0}"
: "${START_ENV_OVERRIDE:=0}"

log "Boot: RENDER=${is_render} cwd=$(pwd)"
log "Flags: MAIN=${MAIN_MODULE} STRICT=${START_STRICT} PRECHECKS=${START_PRECHECKS} DB_CANARY=${START_DB_CANARY} SKIP_DSN_REQUIRED=${SKIP_DSN_REQUIRED}"

# -----------------------------------------------------------------------------
# 2) Python selection
# -----------------------------------------------------------------------------
PYTHON="${PYTHON:-python}"
if [[ -x ".venv/bin/python" ]]; then
  PYTHON=".venv/bin/python"
elif [[ -x ".venv/Scripts/python.exe" ]]; then
  PYTHON=".venv/Scripts/python.exe"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON="python"
else
  die "Python executable not found"
fi

export PYTHONUNBUFFERED=1
export PYTHONDONTWRITEBYTECODE=1
export PYTHONUTF8="${PYTHONUTF8:-1}"

PY_PATHS=("$SCRIPT_DIR")
[[ -d "$SCRIPT_DIR/src" ]] && PY_PATHS+=("$SCRIPT_DIR/src")
if [[ -n "${PYTHONPATH:-}" ]]; then
  export PYTHONPATH="$(IFS=:; echo "${PY_PATHS[*]}"):$PYTHONPATH"
else
  export PYTHONPATH="$(IFS=:; echo "${PY_PATHS[*]}")"
fi

log "Using python: $($PYTHON -V 2>&1)"

# -----------------------------------------------------------------------------
# 3) Runtime dirs (Render + local safe; best-effort chmod)
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

ensure_dir "$DATA_DIR" || DATA_DIR="${TMP_DIR%/}/sniper_data"
ensure_dir "$SECRETS_DIR" || SECRETS_DIR="${TMP_DIR%/}/sniper_secrets"
ensure_dir "$DATA_DIR" || die "Cannot create DATA_DIR"
ensure_dir "$SECRETS_DIR" || die "Cannot create SECRETS_DIR"

export DATA_DIR SECRETS_DIR TMP_DIR
export XDG_CACHE_HOME="${XDG_CACHE_HOME:-$DATA_DIR/.cache}"
export XDG_CONFIG_HOME="${XDG_CONFIG_HOME:-$DATA_DIR/.config}"
ensure_dir "$XDG_CACHE_HOME" || true
ensure_dir "$XDG_CONFIG_HOME" || true

# -----------------------------------------------------------------------------
# 4) Env loading (NO eval). Prefers python-dotenv streaming NUL pairs.
# -----------------------------------------------------------------------------
_has_python_dotenv=0
if "$PYTHON" - <<'PY' >/dev/null 2>&1
try:
    import dotenv  # noqa: F401
    raise SystemExit(0)
except Exception:
    raise SystemExit(1)
PY
then
  _has_python_dotenv=1
fi

_load_env_source() {
  local f="$1"
  [[ -z "$f" || ! -f "$f" ]] && return 0
  log "Loading env file (source): $f"
  set -a
  # shellcheck disable=SC1090
  . "$f"
  set +a
}

_load_env_safe_bash() {
  local f="$1"
  [[ -z "$f" || ! -f "$f" ]] && return 0

  log "Loading env file (bash-safe): $f"
  local loaded=0 ignored=0 line k v

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
      ignored=$((ignored + 1))
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
    loaded=$((loaded + 1))
  done <"$f"

  log "dotenv(bash): $f loaded=$loaded ignored=$ignored"
}

_dotenv_stream_python() {
  # emits: k\0v\0 ... __DOTENV_LOADED__\0N\0 __DOTENV_IGNORED__\0N\0
  local f="$1"
  "$PYTHON" - <<'PY' "$f" "${START_ENV_OVERRIDE:-0}"
import os, sys
from dotenv import dotenv_values

path = sys.argv[1]
override = (sys.argv[2] == "1")

vals = dotenv_values(path)
loaded = 0
ignored = 0
out = sys.stdout.buffer

for k, v in vals.items():
    if not k or not isinstance(k, str):
        continue
    if not (k[0].isalpha() or k[0] == "_"):
        continue
    if not all((c.isalnum() or c == "_") for c in k):
        continue
    if (not override) and (k in os.environ) and (os.environ.get(k) not in (None, "")):
        ignored += 1
        continue
    if v is None:
        v = ""
    if not isinstance(v, str):
        v = str(v)
    out.write(k.encode("utf-8", "ignore") + b"\x00")
    out.write(v.encode("utf-8", "ignore") + b"\x00")
    loaded += 1

out.write(b"__DOTENV_LOADED__\x00" + str(loaded).encode() + b"\x00")
out.write(b"__DOTENV_IGNORED__\x00" + str(ignored).encode() + b"\x00")
PY
}

load_env_file() {
  local f="$1"
  [[ -z "$f" || ! -f "$f" ]] && return 0

  if is_true "${START_ALLOW_SOURCE_ENV}"; then
    _load_env_source "$f"
    return 0
  fi

  if [[ "$_has_python_dotenv" == "1" ]]; then
    log "Loading env file (dotenv-safe): $f"
    local k v loaded="?" ignored="?"
    while IFS= read -r -d '' k; do
      IFS= read -r -d '' v || v=""
      if [[ "$k" == "__DOTENV_LOADED__" ]]; then
        loaded="$v"
        continue
      fi
      if [[ "$k" == "__DOTENV_IGNORED__" ]]; then
        ignored="$v"
        continue
      fi
      [[ -z "$k" ]] && continue
      export "$k=$v"
    done < <(_dotenv_stream_python "$f")
    log "dotenv: $f loaded=${loaded} ignored=${ignored}"
    return 0
  fi

  _load_env_safe_bash "$f"
}

load_env_file "/etc/secrets/.env.extra"
load_env_file "/etc/secrets/.env"
load_env_file "./.env.extra"
load_env_file "./.env"

# -----------------------------------------------------------------------------
# 5) Command mode (utility invocations) — AFTER env load
# -----------------------------------------------------------------------------
cmd="${1:-}"
case "$cmd" in
  -h|--help|help) usage; exit 0 ;;
  check-env|check_env) shift || true; exec "$PYTHON" -m scripts.check_env "$@" ;;
  preflight)          shift || true; exec "$PYTHON" -m scripts.preflight "$@" ;;
  healthcheck)        shift || true; exec "$PYTHON" -m scripts.healthcheck "$@" ;;
  smoke|smoke_lightning) shift || true; exec "$PYTHON" -m scripts.smoke_lightning "$@" ;;
  test-pg|test_pg)    shift || true; exec "$PYTHON" -m scripts.test_pg "$@" ;;
  canary|canary_trade) shift || true; exec "$PYTHON" -m scripts.canary_trade "$@" ;;
esac

# -----------------------------------------------------------------------------
# 6) Wallet bootstrap (NO function calls inside [[ ... ]])
# -----------------------------------------------------------------------------
wallet_path="${SECRETS_DIR%/}/wallet.json"
: "${START_WALLET_OVERWRITE:=0}"

_write_wallet_file() {
  local content="$1"
  [[ -z "$content" ]] && return 0

  if [[ ! -f "$wallet_path" ]]; then
    printf '%s' "$content" >"$wallet_path"
    chmod 600 "$wallet_path" 2>/dev/null || true
    log "Wallet: wrote wallet.json"
    return 0
  fi

  if is_true "${START_WALLET_OVERWRITE}"; then
    printf '%s' "$content" >"$wallet_path"
    chmod 600 "$wallet_path" 2>/dev/null || true
    log "Wallet: overwrote wallet.json"
  fi
}

# KEYPAIR_PATH wins if it exists
if [[ -n "${KEYPAIR_PATH:-}" && -f "${KEYPAIR_PATH:-}" ]]; then
  export WALLET_PRIVATE_KEY="${KEYPAIR_PATH}"
fi

# If WALLET_PRIVATE_KEY points to an existing file, keep it.
if [[ -n "${WALLET_PRIVATE_KEY:-}" && -f "${WALLET_PRIVATE_KEY:-}" ]]; then
  :
else
  # If WALLET_PRIVATE_KEY looks like raw JSON, materialize it.
  if [[ -n "${WALLET_PRIVATE_KEY:-}" ]]; then
    case "${WALLET_PRIVATE_KEY}" in
      \[*|\{*)
        if [[ ! -f "$wallet_path" ]]; then
          _write_wallet_file "${WALLET_PRIVATE_KEY}"
        else
          if is_true "${START_WALLET_OVERWRITE}"; then
            _write_wallet_file "${WALLET_PRIVATE_KEY}"
          fi
        fi
        if [[ -f "$wallet_path" ]]; then
          export WALLET_PRIVATE_KEY="$wallet_path"
        fi
        ;;
    esac
  fi

  # Materialize from WALLET_JSON if still not a file
  if [[ -z "${WALLET_PRIVATE_KEY:-}" || ! -f "${WALLET_PRIVATE_KEY:-}" ]]; then
    if [[ -n "${WALLET_JSON:-}" ]]; then
      if [[ ! -f "$wallet_path" ]]; then
        _write_wallet_file "${WALLET_JSON}"
      else
        if is_true "${START_WALLET_OVERWRITE}"; then
          _write_wallet_file "${WALLET_JSON}"
        fi
      fi
      [[ -f "$wallet_path" ]] && export WALLET_PRIVATE_KEY="$wallet_path"
    fi
  fi

  # Materialize from base64 if still not a file
  if [[ -z "${WALLET_PRIVATE_KEY:-}" || ! -f "${WALLET_PRIVATE_KEY:-}" ]]; then
    b64_src="${WALLET_JSON_B64:-${KEYPAIR_B64:-}}"
    if [[ -n "${b64_src:-}" ]]; then
      if [[ ! -f "$wallet_path" ]]; then
        :
      else
        if ! is_true "${START_WALLET_OVERWRITE}"; then
          b64_src=""
        fi
      fi

      if [[ -n "${b64_src:-}" ]]; then
        decoded=""
        if command -v base64 >/dev/null 2>&1; then
          decoded="$(printf '%s' "$b64_src" | base64 -d 2>/dev/null || true)"
        elif command -v openssl >/dev/null 2>&1; then
          decoded="$(printf '%s' "$b64_src" | openssl base64 -d 2>/dev/null || true)"
        fi
        if [[ -n "$decoded" ]]; then
          _write_wallet_file "$decoded"
        fi
      fi

      [[ -f "$wallet_path" ]] && export WALLET_PRIVATE_KEY="$wallet_path"
    fi
  fi

  # If we ended up creating wallet.json, use it.
  [[ -f "$wallet_path" ]] && export WALLET_PRIVATE_KEY="$wallet_path"
fi

if [[ -n "${WALLET_PRIVATE_KEY:-}" && -f "${WALLET_PRIVATE_KEY:-}" ]]; then
  log "Wallet: present (path) $(basename "${WALLET_PRIVATE_KEY}")"
fi

# -----------------------------------------------------------------------------
# 7) DB DSN normalization (asyncpg-safe). Stream NUL pairs, no eval.
# -----------------------------------------------------------------------------
_norm_db_stream_python() {
  "$PYTHON" - <<'PY'
import os, sys
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

def norm_scheme(u: str) -> str:
    u = (u or "").strip()
    if u.startswith("postgres://"):
        u = "postgresql://" + u[len("postgres://"):]
    return u

def strip_driver(u: str) -> str:
    u = (u or "").strip()
    if u.startswith("postgresql+"):
        _, rest = u.split("://", 1)
        u = "postgresql://" + rest
    return u

def looks_internal(host: str) -> bool:
    h = (host or "").lower()
    return h.startswith("dpg-") and "." not in h

def sanitize_query(u: str) -> str:
    p = urlsplit(u)
    q = parse_qsl(p.query, keep_blank_values=True)
    out = []
    has_ssl = any((k or "").lower() == "ssl" for k, _ in q)

    for k, v in q:
        kl = (k or "").lower()

        # Convert sslmode=require -> ssl=true
        if kl == "sslmode":
            if not has_ssl:
                out.append(("ssl", "true"))
                has_ssl = True
            continue

        # Drop params that frequently break asyncpg/sqlalchemy plumbing
        if kl in {"sslrootcert", "sslcert", "sslkey"}:
            continue

        # Keep statement_cache_size only if numeric
        if kl == "statement_cache_size":
            try:
                int(v)
                out.append((k, v))
            except Exception:
                pass
            continue

        out.append((k, v))

    return urlunsplit((p.scheme, p.netloc, p.path, urlencode(out, doseq=True), p.fragment))

def to_sqlalchemy_asyncpg(u: str) -> str:
    u = sanitize_query(strip_driver(norm_scheme(u)))
    if u.startswith("postgresql://"):
        u = "postgresql+asyncpg://" + u[len("postgresql://"):]
    return u

def to_asyncpg(u: str) -> str:
    u = sanitize_query(strip_driver(norm_scheme(u)))
    if u.startswith("postgresql+asyncpg://"):
        u = "postgresql://" + u[len("postgresql+asyncpg://"):]
    return u

is_render = bool(os.environ.get("RENDER_SERVICE_ID") or os.environ.get("RENDER_INSTANCE_ID") or os.environ.get("RENDER_EXTERNAL_URL"))
if not is_render:
    is_render = os.environ.get("RENDER", "").strip().lower() in ("1", "true", "yes", "on")

def pick() -> str:
    if is_render:
        keys = ("PG_DSN_INTERNAL", "PG_DSN_EXTERNAL", "POSTGRES_DSN", "DATABASE_URL", "PG_DSN", "PNL_DSN")
    else:
        keys = ("PG_DSN_EXTERNAL", "POSTGRES_DSN", "DATABASE_URL", "PNL_DSN", "PG_DSN", "PG_DSN_INTERNAL")

    for k in keys:
        v = os.environ.get(k, "").strip()
        if not v:
            continue

        # Skip internal DSN for local
        if (not is_render) and k == "PG_DSN_INTERNAL":
            try:
                p = urlsplit(strip_driver(norm_scheme(v)))
                if looks_internal(p.hostname or ""):
                    continue
            except Exception:
                continue

        return v

    return ""

base = os.environ.get("BASE_DSN", "").strip() or pick()

out = sys.stdout.buffer
out.write(b"BASE_DSN\x00" + base.encode() + b"\x00")
if base:
    sql = to_sqlalchemy_asyncpg(base)
    apg = to_asyncpg(base)
    out.write(b"POSTGRES_DSN\x00" + sql.encode() + b"\x00")
    out.write(b"DATABASE_URL\x00" + sql.encode() + b"\x00")
    out.write(b"PG_DSN\x00" + apg.encode() + b"\x00")
    out.write(b"PNL_DSN\x00" + apg.encode() + b"\x00")
PY
}

BASE_DSN="${BASE_DSN:-}"
if [[ -z "${BASE_DSN:-}" ]]; then
  while IFS= read -r -d '' k; do
    IFS= read -r -d '' v || v=""
    [[ -z "$k" ]] && continue
    export "$k=$v"
  done < <(_norm_db_stream_python)
fi

if is_true "${START_STRICT}"; then
  if ! is_true "${SKIP_DSN_REQUIRED}"; then
    if [[ -z "${BASE_DSN:-}" ]]; then
      die "Missing DB DSN. Set PG_DSN_INTERNAL/PG_DSN_EXTERNAL (recommended) or POSTGRES_DSN/DATABASE_URL."
    fi
  fi
fi

if [[ -n "${POSTGRES_DSN:-}" ]]; then
  log "DB SQLA async : $(mask_url "${POSTGRES_DSN}")"
fi
if [[ -n "${PG_DSN:-}" ]]; then
  log "DB asyncpg    : $(mask_url "${PG_DSN}")"
fi

# -----------------------------------------------------------------------------
# 8) Gates: check_env + preflight
# -----------------------------------------------------------------------------
if is_true "${START_RUN_CHECK_ENV}"; then
  log "check_env enabled"
  ce_args=(--json-only)
  if is_true "${START_STRICT}"; then
    ce_args+=(--strict)
  fi
  CHECK_ENV_JSON=1 CHECK_ENV_JSON_ONLY=1 "$PYTHON" -m scripts.check_env "${ce_args[@]}" \
    || die "check_env failed"
  log "check_env OK"
else
  log "check_env disabled"
fi

if is_true "${START_PRECHECKS}"; then
  log "preflight enabled"
  pf_args=()
  if is_true "${START_STRICT}"; then
    pf_args+=(--strict)
  fi

  pfrc=0
  "$PYTHON" -m scripts.preflight "${pf_args[@]}" || pfrc=$?

  if [[ "$pfrc" != "0" ]]; then
    if [[ "$pfrc" == "1" ]]; then
      if ! is_true "${START_STRICT}"; then
        log "preflight WARN (strict=0) — continuing"
        pfrc=0
      fi
    fi
  fi

  if [[ "$pfrc" != "0" ]]; then
    die "preflight failed (rc=$pfrc)"
  fi

  log "preflight OK"
else
  log "preflight disabled"
fi

if is_true "${START_DB_CANARY}"; then
  if ! is_true "${SKIP_DSN_REQUIRED}"; then
    log "DB canary: scripts.test_pg ..."
    "$PYTHON" -m scripts.test_pg --try-all --json-only >/dev/null || die "DB canary failed"
    log "DB canary OK"
  fi
fi

# -----------------------------------------------------------------------------
# 9) Build main argv (JSON-safe)
# -----------------------------------------------------------------------------
main_argv=()
if [[ -n "${MAIN_ARGS_JSON:-}" ]]; then
  while IFS= read -r a; do
    [[ -n "$a" ]] && main_argv+=("$a")
  done < <(
    "$PYTHON" - <<'PY'
import json, os, sys
raw = os.environ.get("MAIN_ARGS_JSON", "").strip()
try:
    arr = json.loads(raw) if raw else []
    if not isinstance(arr, list):
        arr = []
except Exception:
    arr = []
for x in arr:
    if x is None:
        continue
    s = str(x).replace("\r", " ").replace("\n", " ")
    sys.stdout.write(s + "\n")
PY
  )
elif [[ -n "${MAIN_ARGS_FILE:-}" && -f "${MAIN_ARGS_FILE:-}" ]]; then
  while IFS= read -r ln || [[ -n "$ln" ]]; do
    ln="${ln//$'\r'/}"
    [[ -z "$ln" || "$ln" =~ ^[[:space:]]*# ]] && continue
    main_argv+=("$ln")
  done < "${MAIN_ARGS_FILE}"
elif [[ -n "${MAIN_ARGS:-}" ]]; then
  # shellcheck disable=SC2206
  main_argv=( ${MAIN_ARGS} )
fi

# -----------------------------------------------------------------------------
# 10) Launch supervisor (signal forwarding + restart policy)
# -----------------------------------------------------------------------------
: "${MAIN_RESTART_ON_CRASH:=1}"
: "${MAIN_MAX_RESTARTS:=0}"          # 0=infinite
: "${MAIN_RESTART_BACKOFF_S:=3}"
: "${MAIN_RESTART_MAX_BACKOFF_S:=30}"
: "${MAIN_RESTART_JITTER_S:=2}"
: "${MAIN_NO_RESTART_EXIT_CODES:=130,143}"
: "${MAIN_USE_SETSID:=1}"

log "Launching: ${PYTHON} -m ${MAIN_MODULE}"

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

# numeric guardrails
[[ "${MAIN_MAX_RESTARTS}" =~ ^[0-9]+$ ]] || MAIN_MAX_RESTARTS=0
[[ "${MAIN_RESTART_BACKOFF_S}" =~ ^[0-9]+$ ]] || MAIN_RESTART_BACKOFF_S=3
[[ "${MAIN_RESTART_MAX_BACKOFF_S}" =~ ^[0-9]+$ ]] || MAIN_RESTART_MAX_BACKOFF_S=30
[[ "${MAIN_RESTART_JITTER_S}" =~ ^[0-9]+$ ]] || MAIN_RESTART_JITTER_S=2

no_restart_codes_csv="${MAIN_NO_RESTART_EXIT_CODES//;/,}"
no_restart_codes_csv="${no_restart_codes_csv// /,}"

_should_stop_on_rc() {
  local rc="$1" csv="$2"
  local IFS=,
  for x in $csv; do
    [[ -n "$x" && "$rc" == "$x" ]] && return 0
  done
  return 1
}

restarts=0
backoff="${MAIN_RESTART_BACKOFF_S}"

_start_child() {
  child_pid=""
  child_pgid=""
  if is_true "${MAIN_USE_SETSID}"; then
    if command -v setsid >/dev/null 2>&1; then
      setsid "${PYTHON}" -m "${MAIN_MODULE}" "${main_argv[@]}" &
      child_pid="$!"
      child_pgid="$child_pid"
      return 0
    fi
  fi
  "${PYTHON}" -m "${MAIN_MODULE}" "${main_argv[@]}" &
  child_pid="$!"
}

while true; do
  rc=0
  _start_child
  wait "${child_pid}" || rc=$?
  child_pid=""
  child_pgid=""

  if [[ "$rc" == "0" ]]; then
    log "Main exited cleanly (rc=0). Stopping."
    exit 0
  fi

  if _should_stop_on_rc "$rc" "$no_restart_codes_csv"; then
    log "Main exited rc=${rc} (stop codes=${no_restart_codes_csv}). Stopping."
    exit "$rc"
  fi

  restarts=$((restarts + 1))
  if [[ "${MAIN_MAX_RESTARTS}" != "0" ]]; then
    if [[ "$restarts" -ge "${MAIN_MAX_RESTARTS}" ]]; then
      log "Main crashed rc=${rc}. Max restarts reached (${MAIN_MAX_RESTARTS})."
      exit "$rc"
    fi
  fi

  jitter=0
  if [[ "${MAIN_RESTART_JITTER_S}" -gt 0 ]]; then
    jitter=$((RANDOM % (MAIN_RESTART_JITTER_S + 1)))
  fi

  log "Main crashed rc=${rc}. Restarting in ${backoff}s (+${jitter}s) (attempt ${restarts})"
  sleep "$((backoff + jitter))"

  backoff=$((backoff * 2))
  if [[ "$backoff" -gt "${MAIN_RESTART_MAX_BACKOFF_S}" ]]; then
    backoff="${MAIN_RESTART_MAX_BACKOFF_S}"
  fi
done
