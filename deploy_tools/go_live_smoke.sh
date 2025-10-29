#!/usr/bin/env bash
# go_live_smoke.sh — smoke test with optional DRYRUN flip
set -Eeuo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

ENVFILE="$ROOT/.env"
BACKUP="$ROOT/.env.bak.$(date +%Y%m%d-%H%M%S)"

SMOKE_ONLY="${SMOKE_ONLY:-1}"   # default to smoke; set 0 to actually flip
echo "[SMOKE] SMOKE_ONLY=$SMOKE_ONLY"

snapshot_env_if_missing () {
  if [[ -f "$ENVFILE" ]]; then return 0; fi
  echo "[SMOKE] .env not found; snapshotting current environment -> $ENVFILE"
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

load_env () {
  set -a
  [ -f "$ENVFILE" ] && . "$ENVFILE"
  set +a
}

flip_dryrun_optionally () {
  if [[ "$SMOKE_ONLY" == "1" ]]; then
    echo "[SMOKE] Leaving DRYRUN unchanged" ; return 0
  fi
  if grep -q '^DRYRUN=True' "$ENVFILE"; then
    sed -i '0,/^DRYRUN=True/{s/^DRYRUN=True/DRYRUN=False/}' "$ENVFILE"
    echo "[SMOKE] DRYRUN=True → DRYRUN=False"
  elif grep -q '^DRYRUN=False' "$ENVFILE"; then
    echo "[SMOKE] DRYRUN already False"
  else
    echo "DRYRUN=False" >> "$ENVFILE"
    echo "[SMOKE] DRYRUN key appended as False"
  fi
}

preflight_checks () {
  echo "[SMOKE] Router + Canary checks…"
  python -m scripts.check_router
  python -m scripts.canary_trade
}

restart_service () {
  echo "[SMOKE] Restarting service…"
  pkill -f '[s]tart.sh' || true
  nohup bash ./start.sh > /tmp/sniper_live.out 2>&1 &
}

main () {
  snapshot_env_if_missing
  echo "[SMOKE] Backing up $ENVFILE -> $BACKUP"
  cp "$ENVFILE" "$BACKUP"

  load_env
  flip_dryrun_optionally
  load_env

  preflight_checks
  restart_service

  if [[ "$SMOKE_ONLY" == "1" ]]; then
    echo "[SMOKE] Complete. DRYRUN unchanged. Backup at $BACKUP"
  else
    echo "[GO-LIVE] Complete. DRYRUN=False. Backup at $BACKUP"
  fi
  echo "[SMOKE] Tail: /tmp/sniper_live.out"
}

main "$@"
