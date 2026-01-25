#!/usr/bin/env bash
# deploy_tools/go_live_smoke.sh — V3+++ Smoke Gate Runner (LOCK-IN)
# =============================================================================
# Purpose:
#   Run the "go live" smoke gate safely:
#     - load env (if .env exists)
#     - optional DRYRUN flip (persist only if env file is writable)
#     - run preflight + smoke modules + integration smoke
#     - optional restart using PIDFILE (no pkill -f)
#
# Defaults:
#   - SMOKE_ONLY=1 (do NOT flip DRYRUN; do NOT restart)
#
# Usage:
#   bash deploy_tools/go_live_smoke.sh
#   SMOKE_ONLY=0 bash deploy_tools/go_live_smoke.sh         # flip DRYRUN -> 0 (best-effort)
#   RESTART=1 bash deploy_tools/go_live_smoke.sh            # restart after smokes (pidfile-based)
#
# Flags:
#   --smoke-only            Do checks only (default)
#   --flip-dryrun=0|1       Set DRYRUN value for this run (persist if possible)
#   --persist               Require env-file persistence (fail if cannot write)
#   --restart               Restart service after checks (pidfile-based)
#   --env-file=PATH         Default: repo/.env
#   --pidfile=PATH          Default: repo/.run/sniper.pid
#   --log=PATH              Default: /tmp/go_live_smoke.log
#   --no-preflight           Skip scripts.preflight if present
#   --no-integration         Skip tests/integration_smoke.py
#
# Exit codes:
#   0 success
#   2 failure
# =============================================================================

set -Eeuo pipefail
shopt -s inherit_errexit 2>/dev/null || true

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

RUN_DIR="${RUN_DIR:-$ROOT/.run}"
ENVFILE="${ENVFILE:-$ROOT/.env}"
PIDFILE="${PIDFILE:-$RUN_DIR/sniper.pid}"
LOG="${LOG:-/tmp/go_live_smoke.log}"

# Env-compatible defaults (keep your previous behavior)
SMOKE_ONLY="${SMOKE_ONLY:-1}"      # 1 = do NOT flip DRYRUN
RESTART="${RESTART:-0}"            # 1 = restart after checks
FLIP_DRYRUN="${FLIP_DRYRUN:-}"     # if set, overrides SMOKE_ONLY default
PERSIST="${PERSIST:-0}"            # 1 = require writable ENVFILE to apply flip
NO_PREFLIGHT="${NO_PREFLIGHT:-0}"  # 1 = skip preflight
NO_INTEGRATION="${NO_INTEGRATION:-0}"  # 1 = skip integration smoke

# Telegram optional (SAFE: no chat autodiscovery)
TELEGRAM_BOT_TOKEN="${TELEGRAM_BOT_TOKEN:-}"
TELEGRAM_CHAT_ID="${TELEGRAM_CHAT_ID:-}"

# internal
ENV_EDITED="0"
BACKUP_PATH=""

ts() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

log() {
  local msg="$1"
  echo "[go_live_smoke] $(ts) ${msg}" | tee -a "$LOG" >&2
}

send_tg() {
  local msg="$1"
  [[ -z "$TELEGRAM_BOT_TOKEN" ]] && return 0
  [[ -z "$TELEGRAM_CHAT_ID" ]] && return 0
  curl -sS -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
    -d "chat_id=${TELEGRAM_CHAT_ID}" -d "text=${msg}" >/dev/null 2>&1 || true
}

python_bin() {
  if command -v python3 >/dev/null 2>&1; then echo "python3"; return 0; fi
  if command -v python  >/dev/null 2>&1; then echo "python";  return 0; fi
  echo "python"
}

# ──────────────────────────────────────────────────────────────────────────────
# Lock (avoid concurrent smoke runs)
# ──────────────────────────────────────────────────────────────────────────────
LOCKFILE="$RUN_DIR/go_live_smoke.lock"
acquire_lock() {
  mkdir -p "$RUN_DIR"
  exec 9>"$LOCKFILE"
  if ! flock -n 9; then
    log "Another go_live_smoke is running (lock: $LOCKFILE)"
    exit 2
  fi
}

# ──────────────────────────────────────────────────────────────────────────────
# Env helpers
# ──────────────────────────────────────────────────────────────────────────────
load_envfile_if_present() {
  if [[ -f "$ENVFILE" ]]; then
    set -a
    # shellcheck disable=SC1090
    . "$ENVFILE"
    set +a
    log "Loaded env file: $(basename "$ENVFILE")"
  else
    log "No env file at $ENVFILE (OK on Render; using process env)"
  fi
}

backup_envfile_if_present() {
  [[ -f "$ENVFILE" ]] || return 0
  mkdir -p "$RUN_DIR/backups"
  local stamp
  stamp="$(date -u +"%Y%m%d-%H%M%S")"
  BACKUP_PATH="$RUN_DIR/backups/.env.bak.${stamp}"
  cp -p "$ENVFILE" "$BACKUP_PATH" >/dev/null 2>&1 || true
  log "Backup created: $BACKUP_PATH"
}

persist_env_kv() {
  # Persist key=value into ENVFILE safely (tmp + mv). Returns 0 if persisted.
  local key="$1"
  local val="$2"

  [[ -f "$ENVFILE" ]] || return 1
  [[ -w "$ENVFILE" ]] || return 1

  local tmp="${ENVFILE}.tmp.$$"
  awk -v k="$key" -v v="$val" '
    BEGIN { found=0 }
    $0 ~ "^"k"=" && found==0 { print k"="v; found=1; next }
    { print }
    END { if(found==0) print k"="v }
  ' "$ENVFILE" > "$tmp"
  mv -f "$tmp" "$ENVFILE"
  return 0
}

apply_dryrun() {
  # Determine desired DRYRUN:
  # - If FLIP_DRYRUN set -> use it
  # - Else if SMOKE_ONLY=1 -> do nothing
  # - Else SMOKE_ONLY=0 -> set DRYRUN=0
  local want=""
  if [[ -n "$FLIP_DRYRUN" ]]; then
    want="$FLIP_DRYRUN"
  elif [[ "$SMOKE_ONLY" == "1" ]]; then
    log "SMOKE_ONLY=1 -> leaving DRYRUN unchanged"
    return 0
  else
    want="0"
  fi

  if [[ "$want" != "0" && "$want" != "1" ]]; then
    log "ERROR: invalid --flip-dryrun=$want (must be 0 or 1)"
    return 2
  fi

  # If persistence required, enforce it
  if [[ "$PERSIST" == "1" ]]; then
    if ! persist_env_kv "DRYRUN" "$want"; then
      log "ERROR: --persist requested but ENVFILE is missing or not writable: $ENVFILE"
      return 2
    fi
    persist_env_kv "IS_DRYRUN" "$want" || true
    ENV_EDITED="1"
    log "Persisted DRYRUN=$want in $(basename "$ENVFILE")"
  else
    # Best-effort persistence; otherwise process env override
    if persist_env_kv "DRYRUN" "$want"; then
      persist_env_kv "IS_DRYRUN" "$want" || true
      ENV_EDITED="1"
      log "Persisted DRYRUN=$want in $(basename "$ENVFILE")"
    else
      export DRYRUN="$want"
      export IS_DRYRUN="$want"
      log "ENVFILE not writable/present; using process env DRYRUN=$want (Render-safe)"
    fi
  fi

  return 0
}

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
      cp -f "$BACKUP_PATH" "$ENVFILE" >>"$LOG" 2>&1 || true
      log "Rollback attempted via direct restore (backup: $BACKUP_PATH)"
    fi
    send_tg "[ROLLBACK] ${why} — env restored from backup."
  else
    log "No backup available; cannot rollback ENVFILE."
    send_tg "[ROLLBACK-FAILED] ${why} — no backup available."
  fi
}

# ──────────────────────────────────────────────────────────────────────────────
# Smoke runner helpers
# ──────────────────────────────────────────────────────────────────────────────
module_exists() {
  local mod="$1"
  local py
  py="$(python_bin)"
  "$py" -c "import importlib.util,sys; sys.exit(0 if importlib.util.find_spec('$mod') else 1)" >/dev/null 2>&1
}

run_mod() {
  local mod="$1"
  local py
  py="$(python_bin)"
  log "RUN: python -m $mod"
  "$py" -m "$mod" >>"$LOG" 2>&1
}

run_file() {
  local rel="$1"
  local py
  py="$(python_bin)"
  log "RUN: $py $rel"
  "$py" "$rel" >>"$LOG" 2>&1
}

smoke_suite() {
  log "────────────────────────────────────────────────────────────"
  log "Smoke Suite (LOCK-IN)"
  log "────────────────────────────────────────────────────────────"

  # Preflight if present (optional, but preferred)
  if [[ "$NO_PREFLIGHT" != "1" ]] && module_exists "scripts.preflight"; then
    run_mod "scripts.preflight"
  else
    log "Preflight skipped (missing or disabled); running check_router"
    run_mod "scripts.check_router"
  fi

  # Hard requirements in your current pipeline
  run_mod "scripts.check_router"
  run_mod "scripts.smoke_lightning"
  run_mod "scripts.smoke_pg_only"

  # Optional local-path integration helper (if you keep it)
  if module_exists "scripts.smoke_run_local"; then
    run_mod "scripts.smoke_run_local"
  else
    log "Optional: scripts.smoke_run_local not found; skipping"
  fi

  # Integration smoke (file path, not module)
  if [[ "$NO_INTEGRATION" != "1" ]]; then
    if [[ -f "$ROOT/tests/integration_smoke.py" ]]; then
      run_file "tests/integration_smoke.py"
    else
      log "ERROR: tests/integration_smoke.py not found"
      return 2
    fi
  else
    log "Integration smoke skipped (--no-integration)"
  fi

  log "Smoke suite OK"
  return 0
}

# ──────────────────────────────────────────────────────────────────────────────
# Restart (pidfile-based, no pkill -f)
# ──────────────────────────────────────────────────────────────────────────────
stop_existing_if_pidfile() {
  [[ -f "$PIDFILE" ]] || return 0
  local pid
  pid="$(cat "$PIDFILE" 2>/dev/null || true)"
  [[ -n "$pid" ]] || return 0

  if kill -0 "$pid" >/dev/null 2>&1; then
    log "Stopping existing process (pid=$pid from PIDFILE=$PIDFILE)"
    kill "$pid" >/dev/null 2>&1 || true
    sleep 1
    kill -9 "$pid" >/dev/null 2>&1 || true
  fi

  rm -f "$PIDFILE" >/dev/null 2>&1 || true
}

restart_service() {
  [[ "$RESTART" == "1" ]] || { log "RESTART=0 -> not restarting"; return 0; }

  if [[ ! -x "$ROOT/start.sh" ]]; then
    log "ERROR: start.sh missing or not executable"
    return 2
  fi

  mkdir -p "$RUN_DIR"
  stop_existing_if_pidfile

  log "Starting service in background (nohup). Output: $RUN_DIR/sniper.out"
  nohup bash "$ROOT/start.sh" >>"$RUN_DIR/sniper.out" 2>&1 &
  echo $! > "$PIDFILE"
  log "Started pid=$(cat "$PIDFILE")"
  return 0
}

# ──────────────────────────────────────────────────────────────────────────────
# CLI parsing
# ──────────────────────────────────────────────────────────────────────────────
usage() {
  cat >&2 <<'USAGE'
Usage: bash deploy_tools/go_live_smoke.sh [flags]

Flags:
  --smoke-only            Do checks only (default)
  --flip-dryrun=0|1       Set DRYRUN value for this run (persist if possible)
  --persist               Require env-file persistence (fail if cannot write)
  --restart               Restart after checks (pidfile-based)
  --env-file=PATH         Env file to load/edit (default repo/.env)
  --pidfile=PATH          PID file (default repo/.run/sniper.pid)
  --log=PATH              Log file (default /tmp/go_live_smoke.log)
  --no-preflight          Skip scripts.preflight even if present
  --no-integration        Skip tests/integration_smoke.py
USAGE
}

parse_args() {
  for a in "$@"; do
    case "$a" in
      --smoke-only) SMOKE_ONLY="1" ;;
      --restart) RESTART="1" ;;
      --persist) PERSIST="1" ;;
      --flip-dryrun=*) FLIP_DRYRUN="${a#*=}" ;;
      --env-file=*) ENVFILE="${a#*=}" ;;
      --pidfile=*) PIDFILE="${a#*=}" ;;
      --log=*) LOG="${a#*=}" ;;
      --no-preflight) NO_PREFLIGHT="1" ;;
      --no-integration) NO_INTEGRATION="1" ;;
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
# Main + traps
# ──────────────────────────────────────────────────────────────────────────────
on_err() {
  local rc=$?
  log "FAIL (rc=$rc). See log: $LOG"
  rollback_envfile "go_live_smoke failed (rc=$rc)"
  exit 2
}

on_int() {
  log "Interrupted (SIGINT)"
  rollback_envfile "go_live_smoke interrupted"
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
  log "env_file=$ENVFILE (exists=$( [[ -f "$ENVFILE" ]] && echo yes || echo no ), writable=$( [[ -f "$ENVFILE" && -w "$ENVFILE" ]] && echo yes || echo no ))"
  log "pidfile=$PIDFILE restart=$RESTART smoke_only=$SMOKE_ONLY persist=$PERSIST flip_dryrun=${FLIP_DRYRUN:-'(none)'}"

  # Only backup if we might edit
  if [[ "$SMOKE_ONLY" != "1" || -n "$FLIP_DRYRUN" || "$PERSIST" == "1" ]]; then
    backup_envfile_if_present
  fi

  load_envfile_if_present
  apply_dryrun
  load_envfile_if_present

  # Enforce DRYRUN contract in process env too (safety)
  if [[ -n "${FLIP_DRYRUN:-}" ]]; then
    export DRYRUN="$FLIP_DRYRUN"
    export IS_DRYRUN="$FLIP_DRYRUN"
  fi

  smoke_suite

  # Restart only if requested
  restart_service

  log "SUCCESS"
  if [[ "$SMOKE_ONLY" == "1" && -z "${FLIP_DRYRUN:-}" ]]; then
    send_tg "[SMOKE] Passed (no DRYRUN change)."
  else
    send_tg "[SMOKE] Passed (DRYRUN=${DRYRUN:-'unknown'})."
  fi
  log "Log: $LOG"
  exit 0
}

main "$@"
