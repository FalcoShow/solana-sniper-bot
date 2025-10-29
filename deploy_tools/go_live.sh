#!/usr/bin/env bash
# go_live.sh — flip DRYRUN off and restart safely (or smoke test only)
set -Eeuo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

ENVFILE="$ROOT/.env"
BACKUP="$ROOT/.env.bak.$(date +%Y%m%d-%H%M%S)"
LOG="/tmp/go_live.log"

# Optional Telegram notify (skip if not configured)
TELEGRAM_BOT_TOKEN="${TELEGRAM_BOT_TOKEN:-}"
TELEGRAM_CHAT_ID="${TELEGRAM_CHAT_ID:-}"

# SMOKE_ONLY=1 → do NOT flip DRYRUN, just run checks & restart
SMOKE_ONLY="${SMOKE_ONLY:-0}"

send_tg () {
  local msg="$1"
  if [[ -n "$TELEGRAM_BOT_TOKEN" && -n "$TELEGRAM_CHAT_ID" ]]; then
    curl -s -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
      -d chat_id="$TELEGRAM_CHAT_ID" -d text="$msg" >/dev/null || true
  fi
}

snapshot_env_if_missing () {
  if [[ -f "$ENVFILE" ]]; then return 0; fi
  echo "[go_live] .env not found; snapshotting current environment -> $ENVFILE" | tee -a "$LOG"
  python - <<'PY'
import os, shlex
prefixes=("PF_","JUP_","JUPITER_","ROUTER_","ROUTEGUARD_","ENTRY_",
          "RAYDIUM_","PUMP_","JITO_","TELEGRAM_","PG_","DB_",
          "BUDGET_","TICKET_","BUY_","SELL_","HEALTH","RELAY_",
          "START_","DRYRUN")
keys=[k for k in os.environ if k.startswith(prefixes) or k in ("DATABASE_URL","PNL_DSN")]
with open(".env","w") as f:
  for k in sorted(keys):
    v=os.environ[k]
    if any(c in v for c in ' #&$`"\\'): v=shlex.quote(v)
    f.write(f"{k}={v}\n")
print("snapshot complete")
PY
}

backup_env () {
  echo "[go_live] Backing up $ENVFILE -> $BACKUP" | tee -a "$LOG"
  cp "$ENVFILE" "$BACKUP"
}

load_env () {
  # export everything from .env so Python sees it
  set -a
  [ -f "$ENVFILE" ] && . "$ENVFILE"
  set +a
}

flip_dryrun_if_needed () {
  if [[ "$SMOKE_ONLY" == "1" ]]; then
    echo "[go_live] SMOKE_ONLY=1 → leaving DRYRUN unchanged" | tee -a "$LOG"
    return 0
  fi
  if grep -q '^DRYRUN=True' "$ENVFILE"; then
    sed -i '0,/^DRYRUN=True/{s/^DRYRUN=True/DRYRUN=False/}' "$ENVFILE"
    echo "[go_live] DRYRUN=True → DRYRUN=False" | tee -a "$LOG"
  elif grep -q '^DRYRUN=False' "$ENVFILE"; then
    echo "[go_live] DRYRUN already False" | tee -a "$LOG"
  else
    echo "[go_live] DRYRUN key not found; appending DRYRUN=False" | tee -a "$LOG"
    echo "DRYRUN=False" >> "$ENVFILE"
  fi
}

rollback_and_exit () {
  local why="$1"
  echo "[go_live] ERROR: $why — rolling back env" | tee -a "$LOG"
  bash "$ROOT/deploy_tools/rollback.sh" "$BACKUP" || true
  send_tg "[ROLLBACK] $why — env restored from backup and service restarted."
  exit 2
}

preflight_checks () {
  echo "[go_live] Running pre-live checks…" | tee -a "$LOG"
  python -m scripts.check_router 2>&1 | tee -a "$LOG" || rollback_and_exit "check_router failed"
  python -m scripts.canary_trade 2>&1 | tee -a "$LOG" || rollback_and_exit "canary_trade failed"
}

restart_service () {
  echo "[go_live] Restarting service via ./start.sh" | tee -a "$LOG"
  pkill -f '[s]tart.sh' || true
  nohup bash ./start.sh > /tmp/sniper_live.out 2>&1 &
}

main () {
  snapshot_env_if_missing
  backup_env
  load_env
  flip_dryrun_if_needed
  load_env                # re-load after we changed DRYRUN
  preflight_checks
  restart_service
  if [[ "$SMOKE_ONLY" == "1" ]]; then
    echo "[SMOKE] Complete. Backup at $BACKUP" | tee -a "$LOG"
    send_tg "[SMOKE] Pre-live checks passed. Service restarted with DRYRUN unchanged."
  else
    echo "[GO-LIVE] Complete. DRYRUN=False. Backup at $BACKUP" | tee -a "$LOG"
    send_tg "[GO-LIVE] DRYRUN=False applied. Service restarted."
  fi
  echo "[go_live] Tail: /tmp/sniper_live.out  | Log: $LOG" | tee -a "$LOG"
}

main "$@"
