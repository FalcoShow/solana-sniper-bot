#!/usr/bin/env bash
set -euo pipefail

BACKUP="${1:-}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENVFILE="$ROOT/.env"
LOG="/tmp/rollback.log"

if [ -z "$BACKUP" ]; then
  echo "Usage: rollback.sh <path-to-backup>" | tee -a "$LOG"
  exit 1
fi

if [ ! -f "$BACKUP" ]; then
  echo "Backup file $BACKUP not found!" | tee -a "$LOG"
  exit 2
fi

echo "Restoring $BACKUP -> $ENVFILE" | tee -a "$LOG"
cp "$BACKUP" "$ENVFILE"

echo "Restarting with restored env" | tee -a "$LOG"
pkill -f start.sh || true
nohup bash ./start.sh > /tmp/sniper_rollback.out 2>&1 &

# Telegram notification (optional)
TELEGRAM_BOT_TOKEN="${TELEGRAM_BOT_TOKEN:-8280288862:AAFRrxCIfJoRCkI49QcJeChaKh9I_Q9xwKY}"
TELEGRAM_CHAT_ID="${TELEGRAM_CHAT_ID:-<YOUR_CHAT_ID>}"

if [ -n "$TELEGRAM_BOT_TOKEN" ] && [ -n "$TELEGRAM_CHAT_ID" ]; then
  curl -s -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
    -d chat_id="$TELEGRAM_CHAT_ID" \
    -d text="[ROLLBACK] Restored env from backup and restarted service." > /dev/null || true
fi

echo "Rollback complete, tail logs at /tmp/sniper_rollback.out" | tee -a "$LOG"
