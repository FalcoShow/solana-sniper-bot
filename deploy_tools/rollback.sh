#!/usr/bin/env bash
# deploy_tools/rollback.sh — V3+++ Safe Env Rollback + Optional Restart (LOCK-IN)
# =============================================================================
# Purpose:
#   Restore repo/.env from a provided backup file (atomic), optionally restart
#   the service safely (PIDFILE-based), and optionally notify Telegram.
#
# Default behavior:
#   - Restore ENV
#   - Restart service (RESTART=1 by default)
#
# Usage:
#   bash deploy_tools/rollback.sh /path/to/.env.bak.20260102-120000
#
# Common flags:
#   --no-restart
#   --restart
#   --env-file=PATH
#   --pidfile=PATH
#   --log=PATH
#   --out=PATH
#
# Telegram (optional; NO hardcoded secrets):
#   export TELEGRAM_BOT_TOKEN="..."
#   export TELEGRAM_CHAT_ID="..."
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
LOG="${LOG:-/tmp/rollback.log}"
OUT="${OUT:-$RUN_DIR/sniper_rollback.out}"

RESTART="${RESTART:-1}"  # 1 restart by default
BACKUP_PATH="${1:-}"

# Telegram optional (must be explicitly provided)
TELEGRAM_BOT_TOKEN="${TELEGRAM_BOT_TOKEN:-}"
TELEGRAM_CHAT_ID="${TELEGRAM_CHAT_ID:-}"

ts() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

log() {
  local msg="$1"
  echo "[rollback] $(ts) ${msg}" | tee -a "$LOG" >&2
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

usage() {
  cat >&2 <<'USAGE'
Usage:
  bash deploy_tools/rollback.sh <backup_env_path> [flags]

Flags:
  --restart              Restart after restore (default)
  --no-restart           Do not restart after restore
  --env-file=PATH        Env file to restore (default: repo/.env)
  --pidfile=PATH         PID file used to stop/start (default: repo/.run/sniper.pid)
  --log=PATH             Log file (default: /tmp/rollback.log)
  --out=PATH             Service output file (default: repo/.run/sniper_rollback.out)

Examples:
  bash deploy_tools/rollback.sh .run/backups/.env.bak.20260102-120000
  RESTART=0 bash deploy_tools/rollback.sh /tmp/.env.bak
USAGE
}

parse_args() {
  # first arg is backup path; the rest are flags
  shift || true
  for a in "$@"; do
    case "$a" in
      --restart) RESTART="1" ;;
      --no-restart) RESTART="0" ;;
      --env-file=*) ENVFILE="${a#*=}" ;;
      --pidfile=*) PIDFILE="${a#*=}" ;;
      --log=*) LOG="${a#*=}" ;;
      --out=*) OUT="${a#*=}" ;;
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
# Lock (avoid concurrent edits with go_live / smoke)
# ──────────────────────────────────────────────────────────────────────────────
LOCKFILE="$RUN_DIR/deploy.lock"
acquire_lock() {
  mkdir -p "$RUN_DIR"
  exec 9>"$LOCKFILE"
  if ! flock -n 9; then
    log "Another deploy tool is running (lock: $LOCKFILE)"
    exit 2
  fi
}

# ──────────────────────────────────────────────────────────────────────────────
# Env restore (atomic)
# ──────────────────────────────────────────────────────────────────────────────
backup_current_env_best_effort() {
  [[ -f "$ENVFILE" ]] || return 0
  mkdir -p "$RUN_DIR/backups"
  local stamp
  stamp="$(date -u +"%Y%m%d-%H%M%S")"
  local cur="$RUN_DIR/backups/.env.pre_rollback.${stamp}"
  cp -p "$ENVFILE" "$cur" >/dev/null 2>&1 || true
  log "Saved current env snapshot (best-effort): $cur"
}

restore_env_atomic() {
  local src="$1"
  local dst="$2"

  [[ -f "$src" ]] || { log "ERROR: backup not found: $src"; return 2; }

  # Determine perms to apply
  local mode="600"
  if [[ -f "$dst" ]]; then
    # preserve existing perms if possible
    mode="$(stat -c "%a" "$dst" 2>/dev/null || echo "600")"
  fi

  local tmp="${dst}.tmp.$$"
  umask 077
  cp "$src" "$tmp"
  chmod "$mode" "$tmp" >/dev/null 2>&1 || chmod 600 "$tmp" || true

  # Atomic move into place
  mv -f "$tmp" "$dst"

  log "Restored env: $(basename "$src") -> $(basename "$dst") (mode=$mode)"
  return 0
}

load_envfile_if_present() {
  if [[ -f "$ENVFILE" ]]; then
    set -a
    # shellcheck disable=SC1090
    . "$ENVFILE"
    set +a
    log "Loaded env file after restore: $(basename "$ENVFILE")"
  else
    log "Env file missing after restore (unexpected): $ENVFILE"
    return 2
  fi
}

# ──────────────────────────────────────────────────────────────────────────────
# Restart (PIDFILE-based; no pkill -f)
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

start_service() {
  if [[ ! -x "$ROOT/start.sh" ]]; then
    log "ERROR: start.sh missing or not executable at $ROOT/start.sh"
    return 2
  fi

  mkdir -p "$RUN_DIR"
  : > "$OUT" 2>/dev/null || true

  log "Starting service: nohup bash ./start.sh >> $OUT"
  nohup bash "$ROOT/start.sh" >>"$OUT" 2>&1 &
  echo $! > "$PIDFILE"
  log "Started pid=$(cat "$PIDFILE") (PIDFILE=$PIDFILE)"
  return 0
}

restart_service_if_enabled() {
  if [[ "$RESTART" != "1" ]]; then
    log "RESTART=0 -> not restarting"
    return 0
  fi

  stop_existing_if_pidfile
  start_service
}

main() {
  if [[ -z "$BACKUP_PATH" ]]; then
    usage
    exit 2
  fi

  parse_args "$@"
  acquire_lock

  mkdir -p "$(dirname "$LOG")" 2>/dev/null || true
  touch "$LOG" 2>/dev/null || true

  log "repo_root=$ROOT"
  log "backup=$BACKUP_PATH"
  log "env_file=$ENVFILE"
  log "pidfile=$PIDFILE restart=$RESTART"
  log "python=$("$(python_bin)" -V 2>&1 | head -n 1 || true)"

  backup_current_env_best_effort
  restore_env_atomic "$BACKUP_PATH" "$ENVFILE"
  load_envfile_if_present

  # Optional: run a minimal sanity check if preflight exists (won't fail if missing)
  if "$("$(python_bin)")" -c "import importlib.util; import sys; sys.exit(0 if importlib.util.find_spec('scripts.preflight') else 1)" >/dev/null 2>&1; then
    log "RUN: python -m scripts.preflight"
    "$("$(python_bin)")" -m scripts.preflight >>"$LOG" 2>&1 || {
      log "WARNING: scripts.preflight failed after restore (continuing to restart anyway)"
    }
  else
    log "scripts.preflight not found; skipping"
  fi

  restart_service_if_enabled

  log "SUCCESS"
  send_tg "[ROLLBACK] Restored env from backup and ${RESTART/1/restarted}${RESTART/0/did not restart}."
  log "Output: $OUT | Log: $LOG"
  exit 0
}

main "$@"
