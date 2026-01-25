#!/usr/bin/env bash
# deploy_tools/go_live.sh — V3+++ GO-LIVE Orchestrator (LOCK-IN)
# =============================================================================
# Purpose:
#   Run preflight + smoke suite, then start the service.
#   Optionally flip DRYRUN off (locally via .env edit, or process env override).
#
# Design rules:
#   - Deterministic exit codes (0 success, 2 failure)
#   - No secret leaks (never prints full DSNs / tokens)
#   - Render-safe: does NOT require .env to exist or be writable
#   - No "pkill -f start.sh" foot-gun. Optional PIDFILE support only.
#   - Preflight/smokes MUST pass before starting unless --no-smoke is used.
#
# Usage:
#   bash deploy_tools/go_live.sh
#   SMOKE_ONLY=1 bash deploy_tools/go_live.sh
#
# Flags:
#   --smoke-only            Run checks only; do not start, do not flip DRYRUN
#   --no-smoke              Skip preflight/smokes (NOT recommended)
#   --start-only            Start only (skip flips + smokes)
#   --set-dryrun=0|1        Desired DRYRUN value (default: 0 if not smoke-only)
#   --env-file=PATH         .env file to edit (default: repo/.env)
#   --pidfile=PATH          PID file to stop before starting (default: repo/.run/sniper.pid)
#   --background            Start in background (local dev); default is exec (Render-friendly)
#   --log=PATH              Log file path (default: /tmp/go_live.log)
# =============================================================================

set -Eeuo pipefail

# bash niceties
shopt -s inherit_errexit 2>/dev/null || true

# ──────────────────────────────────────────────────────────────────────────────
# Repo root + defaults
# ──────────────────────────────────────────────────────────────────────────────
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

LOG_DEFAULT="/tmp/go_live.log"
RUN_DIR_DEFAULT="$ROOT/.run"
PIDFILE_DEFAULT="$RUN_DIR_DEFAULT/sniper.pid"
ENVFILE_DEFAULT="$ROOT/.env"

SMOKE_ONLY="${SMOKE_ONLY:-0}"
NO_SMOKE="${NO_SMOKE:-0}"
START_ONLY="${START_ONLY:-0}"
BACKGROUND="${BACKGROUND:-0}"

# Default target: go-live (DRYRUN=0) unless smoke-only
SET_DRYRUN_DEFAULT="0"
[[ "${SMOKE_ONLY}" == "1" ]] && SET_DRYRUN_DEFAULT="1"
SET_DRYRUN="${SET_DRYRUN:-$SET_DRYRUN_DEFAULT}"

ENVFILE="${ENVFILE:-$ENVFILE_DEFAULT}"
PIDFILE="${PIDFILE:-$PIDFILE_DEFAULT}"
LOG="${LOG:-$LOG_DEFAULT}"

# Optional Telegram (SAFE: no chat autodiscovery here)
TELEGRAM_BOT_TOKEN="${TELEGRAM_BOT_TOKEN:-}"
TELEGRAM_CHAT_ID="${TELEGRAM_CHAT_ID:-}"

# Keep track of whether we actually edited ENVFILE (for rollback)
ENV_EDITED="0"
BACKUP_PATH=""

# ──────────────────────────────────────────────────────────────────────────────
# Logging + safety helpers
# ──────────────────────────────────────────────────────────────────────────────
ts() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

log() {
  local msg="$1"
  echo "[go_live] $(ts) ${msg}" | tee -a "$LOG" >&2
}

die() {
  local msg="$1"
  log "ERROR: ${msg}"
  return 2
}

mask() {
  # Mask long strings (tokens/keys). Keep first 4 chars only.
  local s="${1:-}"
  local n="${#s}"
  if [[ $n -le 4 ]]; then
    printf "%s" "****"
  else
    printf "%s" "${s:0:4}****"
  fi
}

send_tg() {
  local msg="$1"
  [[ -z "$TELEGRAM_BOT_TOKEN" ]] && return 0
  [[ -z "$TELEGRAM_CHAT_ID" ]] && return 0
  # best-effort, never fail the pipeline
  curl -sS -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
    -d "chat_id=${TELEGRAM_CHAT_ID}" -d "text=${msg}" >/dev/null 2>&1 || true
}

python_bin() {
  # prefer python3 if available
  if command -v python3 >/dev/null 2>&1; then echo "python3"; return 0; fi
  if command -v python  >/dev/null 2>&1; then echo "python";  return 0; fi
  echo "python"
}

# ──────────────────────────────────────────────────────────────────────────────
# Locking (avoid concurrent runs)
# ──────────────────────────────────────────────────────────────────────────────
LOCKFILE="$RUN_DIR_DEFAULT/go_live.lock"

acquire_lock() {
  mkdir -p "$RUN_DIR_DEFAULT"
  exec 9>"$LOCKFILE"
  if ! flock -n 9; then
    log "Another go_live is already running (lock: $LOCKFILE)"
    exit 2
  fi
}

# ──────────────────────────────────────────────────────────────────────────────
# Env loading / editing
# ──────────────────────────────────────────────────────────────────────────────
load_envfile_if_present() {
  if [[ -f "$ENVFILE" ]]; then
    # shellcheck disable=SC1090
    set -a
    . "$ENVFILE"
    set +a
    log "Loaded env file: $(basename "$ENVFILE")"
  else
    log "No env file found at $ENVFILE (OK on Render; using process env)"
  fi
}

backup_envfile_if_present() {
  [[ -f "$ENVFILE" ]] || return 0
  local stamp
  stamp="$(date -u +"%Y%m%d-%H%M%S")"
  mkdir -p "$RUN_DIR_DEFAULT/backups"
  BACKUP_PATH="$RUN_DIR_DEFAULT/backups/.env.bak.${stamp}"
  cp -p "$ENVFILE" "$BACKUP_PATH" || true
  log "Backup created: $BACKUP_PATH"
}

edit_envfile_set_key() {
  local key="$1"
  local val="$2"

  [[ -f "$ENVFILE" ]] || return 1
  [[ -w "$ENVFILE" ]] || return 1

  local tmp="${ENVFILE}.tmp.$$"
  local found="0"

  # Replace first occurrence; else append
  awk -v k="$key" -v v="$val" '
    BEGIN { found=0 }
    $0 ~ "^"k"=" && found==0 { print k"="v; found=1; next }
    { print }
    END { if(found==0) print k"="v }
  ' "$ENVFILE" > "$tmp"

  mv -f "$tmp" "$ENVFILE"
  return 0
}

set_dryrun_mode() {
  # Desired DRYRUN set via SET_DRYRUN (0/1)
  local want="${SET_DRYRUN}"
  if [[ "$want" != "0" && "$want" != "1" ]]; then
    die "Invalid --set-dryrun value: $want (must be 0 or 1)"
    return 2
  fi

  # Smoke-only never edits env or starts
  if [[ "$SMOKE_ONLY" == "1" ]]; then
    log "SMOKE_ONLY=1 -> not flipping DRYRUN"
    return 0
  fi

  # Start-only: don’t change DRYRUN unless user explicitly sets it via env
  if [[ "$START_ONLY" == "1" ]]; then
    log "START_ONLY=1 -> not flipping DRYRUN"
    return 0
  fi

  # If ENVFILE is editable, persist change there. Otherwise export for this run.
  if edit_envfile_set_key "DRYRUN" "$want"; then
    # Keep IS_DRYRUN aligned too if present/wanted
    edit_envfile_set_key "IS_DRYRUN" "$want" || true
    ENV_EDITED="1"
    log "Persisted DRYRUN=${want} in $(basename "$ENVFILE")"
  else
    export DRYRUN="$want"
    export IS_DRYRUN="$want"
    log "ENVFILE not writable/present; using process env DRYRUN=${want} (Render-safe)"
  fi

  return 0
}

# ──────────────────────────────────────────────────────────────────────────────
# Rollback (only if we actually edited ENVFILE)
# ──────────────────────────────────────────────────────────────────────────────
rollback_envfile() {
  local why="$1"
  log "Rollback requested: $why"

  if [[ "$ENV_EDITED" != "1" ]]; then
    log "No persisted env edit occurred; skipping rollback."
    return 0
  fi

  if [[ -n "$BACKUP_PATH" && -f "$BACKUP_PATH" ]]; then
    if [[ -x "$ROOT/deploy_tools/rollback.sh" ]]; then
      bash "$ROOT/deploy_tools/rollback.sh" "$BACKUP_PATH" >>"$LOG" 2>&1 || true
      log "Rollback attempted via rollback.sh (backup: $BACKUP_PATH)"
    else
      # fallback: restore directly
      cp -f "$BACKUP_PATH" "$ENVFILE" >>"$LOG" 2>&1 || true
      log "Rollback attempted via direct restore (backup: $BACKUP_PATH)"
    fi
    send_tg "[ROLLBACK] ${why} — env restored from backup."
  else
    log "No backup available; cannot rollback ENVFILE safely."
    send_tg "[ROLLBACK-FAILED] ${why} — no backup available."
  fi
}

# ──────────────────────────────────────────────────────────────────────────────
# Preflight + Smokes
# ──────────────────────────────────────────────────────────────────────────────
run_cmd() {
  local name="$1"; shift
  log "RUN: ${name} :: $*"
  "$@" >>"$LOG" 2>&1
}

run_python_module() {
  local mod="$1"
  local py
  py="$(python_bin)"

  # If module doesn't exist, fail hard (LOCK-IN) unless NO_SMOKE
  if ! "$py" -c "import importlib; import sys; sys.exit(0 if importlib.util.find_spec('$mod') else 2)" >>"$LOG" 2>&1; then
    log "MISSING MODULE: $mod"
    return 2
  fi

  run_cmd "python -m $mod" "$py" -m "$mod"
}

run_smoke_suite() {
  [[ "$NO_SMOKE" == "1" ]] && { log "NO_SMOKE=1 -> skipping preflight/smokes (NOT recommended)"; return 0; }
  [[ "$START_ONLY" == "1" ]] && { log "START_ONLY=1 -> skipping preflight/smokes"; return 0; }

  log "────────────────────────────────────────────────────────────"
  log "Preflight + Smoke Suite (LOCK-IN)"
  log "────────────────────────────────────────────────────────────"

  # Preflight (if present)
  if ! run_python_module "scripts.preflight"; then
    log "Preflight module missing; falling back to check_router only"
    run_python_module "scripts.check_router" || return 2
  fi

  # Canonical smoke stack
  run_python_module "scripts.smoke_init"      || return 2
  run_python_module "scripts.smoke_core"      || return 2
  run_python_module "scripts.smoke_lightning" || return 2
  run_python_module "scripts.smoke_pg_only"   || return 2

  # Integration smoke (unit runner)
  local py
  py="$(python_bin)"
  run_cmd "integration smoke" "$py" "tests/integration_smoke.py" || return 2

  log "Smoke suite: OK"
  return 0
}

# ──────────────────────────────────────────────────────────────────────────────
# Start service (Render-friendly default = exec)
# ──────────────────────────────────────────────────────────────────────────────
stop_existing_if_pidfile() {
  [[ -f "$PIDFILE" ]] || return 0
  local pid
  pid="$(cat "$PIDFILE" 2>/dev/null || true)"
  [[ -n "$pid" ]] || return 0
  if kill -0 "$pid" >/dev/null 2>&1; then
    log "Stopping existing process from PIDFILE=$PIDFILE (pid=$pid)"
    kill "$pid" >/dev/null 2>&1 || true
    sleep 1
    kill -9 "$pid" >/dev/null 2>&1 || true
  fi
  rm -f "$PIDFILE" >/dev/null 2>&1 || true
}

start_service() {
  [[ "$SMOKE_ONLY" == "1" ]] && { log "SMOKE_ONLY=1 -> not starting service"; return 0; }

  local start_cmd=("$ROOT/start.sh")

  if [[ ! -x "${start_cmd[0]}" ]]; then
    die "Missing or non-executable start.sh at ${start_cmd[0]}"
    return 2
  fi

  mkdir -p "$RUN_DIR_DEFAULT"

  stop_existing_if_pidfile

  if [[ "$BACKGROUND" == "1" ]]; then
    log "Starting in background (local dev mode)"
    nohup bash "${start_cmd[0]}" >>"$ROOT/.run/sniper.out" 2>&1 &
    echo $! > "$PIDFILE"
    log "Started pid=$(cat "$PIDFILE") output=$ROOT/.run/sniper.out"
    return 0
  fi

  # Render-friendly: exec so this script becomes the service process
  log "Starting in foreground (exec start.sh)"
  exec bash "${start_cmd[0]}"
}

# ──────────────────────────────────────────────────────────────────────────────
# CLI parsing
# ──────────────────────────────────────────────────────────────────────────────
usage() {
  cat >&2 <<'USAGE'
Usage: bash deploy_tools/go_live.sh [flags]

Flags:
  --smoke-only            Run preflight/smokes only; do not start, do not flip DRYRUN
  --no-smoke              Skip preflight/smokes (NOT recommended)
  --start-only            Start only (skip flips + smokes)
  --set-dryrun=0|1        Desired DRYRUN value (default 0 unless smoke-only)
  --env-file=PATH         Env file to edit (default repo/.env)
  --pidfile=PATH          PID file to stop before starting (default repo/.run/sniper.pid)
  --background            Start in background (local dev); default is exec (Render-friendly)
  --log=PATH              Log file (default /tmp/go_live.log)
USAGE
}

parse_args() {
  for a in "$@"; do
    case "$a" in
      --smoke-only) SMOKE_ONLY="1" ;;
      --no-smoke) NO_SMOKE="1" ;;
      --start-only) START_ONLY="1" ;;
      --background) BACKGROUND="1" ;;
      --set-dryrun=*) SET_DRYRUN="${a#*=}" ;;
      --env-file=*) ENVFILE="${a#*=}" ;;
      --pidfile=*) PIDFILE="${a#*=}" ;;
      --log=*) LOG="${a#*=}" ;;
      -h|--help) usage; exit 0 ;;
      *)
        log "Unknown arg: $a"
        usage
        exit 2
        ;;
    esac
  done
}

# ──────────────────────────────────────────────────────────────────────────────
# Trap handlers
# ──────────────────────────────────────────────────────────────────────────────
on_err() {
  local rc=$?
  log "FAIL (rc=$rc). See log: $LOG"
  rollback_envfile "go_live failed (rc=$rc)"
  exit 2
}

on_int() {
  log "Interrupted (SIGINT)"
  rollback_envfile "go_live interrupted"
  exit 2
}

main() {
  parse_args "$@"
  acquire_lock

  mkdir -p "$(dirname "$LOG")" 2>/dev/null || true
  touch "$LOG" 2>/dev/null || true

  trap on_err ERR
  trap on_int INT

  log "repo_root=$ROOT"
  log "python=$("$(python_bin)" -V 2>&1 | head -n 1 || true)"
  log "env_file=$ENVFILE (editable=$( [[ -f "$ENVFILE" && -w "$ENVFILE" ]] && echo yes || echo no ))"
  log "mode smoke_only=$SMOKE_ONLY start_only=$START_ONLY no_smoke=$NO_SMOKE background=$BACKGROUND"
  log "target DRYRUN=$SET_DRYRUN"

  backup_envfile_if_present
  load_envfile_if_present
  set_dryrun_mode
  load_envfile_if_present

  # Enforce dryrun contract in process env too (for safety)
  export DRYRUN="${DRYRUN:-$SET_DRYRUN}"
  export IS_DRYRUN="${IS_DRYRUN:-$SET_DRYRUN}"

  # Run preflight/smokes
  run_smoke_suite

  # Notify success of checks (optional)
  if [[ "$SMOKE_ONLY" == "1" ]]; then
    log "SMOKE-ONLY SUCCESS"
    send_tg "[SMOKE] Preflight + smokes PASSED. DRYRUN unchanged."
    log "Log: $LOG"
    exit 0
  fi

  send_tg "[GO-LIVE] Preflight + smokes PASSED. Starting service. (DRYRUN=${DRYRUN})"
  log "GO-LIVE checks OK. Starting…"

  # If background mode, start_service returns. Foreground exec never returns.
  start_service
  log "Started (background). Log: $LOG"
  exit 0
}

main "$@"
