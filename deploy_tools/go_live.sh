#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENVFILE="$ROOT/.env"
BACKUP="$ROOT/.env.bak.$(date +%s)"
LOG="/tmp/go_live.log"

TELEGRAM_BOT_TOKEN="${TELEGRAM_BOT_TOKEN:-8280288862:AAFRrxCIfJoRCkI49QcJeChaKh9I_Q9xwKY}" # replace if not in env
TELEGRAM_CHAT_ID="${TELEGRAM_CHAT_ID:-<YOUR_CHAT_ID>}" # fill in

echo "Backing up $ENVFILE -> $BACKUP" | tee -a "$LOG"
cp "$ENVFILE" "$BACKUP"

# Replace DRYRUN=True -> DRYRUN=False safely (only first match)
if grep -q '^DRYRUN=True' "$ENVFILE"; then
    sed -i '0,/^DRYRUN=True/{s/^DRYRUN=True/DRYRUN=False/}' "$ENVFILE"
else
    echo "DRYRUN not set to True in $ENVFILE — adding DRYRUN=False" | tee -a "$LOG"
    echo "DRYRUN=False" >> "$ENVFILE"
fi

echo "Env updated. Running quick pre-live checks..." | tee -a "$LOG"

# 1) Router check (quotes sanity)
python -m scripts.check_router 2>&1 | tee -a "$LOG"
RC=${PIPESTATUS[0]}
if [ "$RC" -ne 0 ]; then
    echo "check_router failed (rc=$RC). Rolling back." | tee -a "$LOG"
    bash "$ROOT/deploy_tools/rollback.sh" "$BACKUP"
    exit 2
fi

# 2) Canary dry-run trade (must succeed)
python -m scripts.canary_trade 2>&1 | tee -a "$LOG"
RC=${PIPESTATUS[0]}
if [ "$RC" -ne 0 ]; then
    echo "canary_trade failed (rc=$RC). Rolling back." | tee -a "$LOG"
    bash "$ROOT/deploy_tools/rollback.sh" "$BACKUP"
    exit 3
fi

# 3) final: restart service (use your start script)
echo "Starting live process via ./start.sh" | tee -a "$LOG"
pkill -f start.sh || true
nohup bash ./start.sh > /tmp/sniper_live.out 2>&1 &

# Send Telegram message that go-live succeeded
if [ -n "$TELEGRAM_BOT_TOKEN" ] && [ -n "$TELEGRAM_CHAT_ID" ]; then
  curl -s -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
    -d chat_id="$TELEGRAM_CHAT_ID" \
    -d text="[GO-LIVE] DRYRUN->FALSE applied and service restarted. Check logs." > /dev/null || true
fi

echo "GO-LIVE complete. Logs at /tmp/sniper_live.out and $LOG" | tee -a "$LOG"
