#!/usr/bin/env bash
set -euo pipefail

SMOKE_ONLY="${SMOKE_ONLY:-0}"   # export SMOKE_ONLY=1 to skip DRYRUN=False
ENV_FILE=".env"
BACKUP="${ENV_FILE}.bak.$(date +%Y%m%d-%H%M%S)"

echo "[GO-LIVE] Backing up ${ENV_FILE} -> ${BACKUP}"
cp "${ENV_FILE}" "${BACKUP}"

if [[ "${SMOKE_ONLY}" != "1" ]]; then
  echo "[GO-LIVE] Setting DRYRUN=False"
  sed -i '0,/^DRYRUN=True/{s/^DRYRUN=True/DRYRUN=False/}' "${ENV_FILE}"
else
  echo "[SMOKE] DRYRUN unchanged (still True)"
fi

echo "[SMOKE] Running router + canary checks…"
python -m scripts.check_router
python -m scripts.canary_trade

echo "[GO-LIVE] Restarting service…"
nohup bash ./start.sh > /tmp/sniper_live.out 2>&1 &

if [[ "${SMOKE_ONLY}" == "1" ]]; then
  echo "[SMOKE] Complete. DRYRUN still True. Backup at ${BACKUP}"
else
  echo "[GO-LIVE] Complete. DRYRUN=False. Backup at ${BACKUP}"
fi
