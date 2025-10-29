#!/usr/bin/env bash
# go_live_smoke.sh — smoke test with optional DRYRUN flip
set -Eeuo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

ENVFILE="$ROOT/.env"
BACKUP_TMP="$(mktemp -u "$ROOT/.env.bak.XXXXXXXX")"
LOG="${LOG:-/tmp/go_live_smoke.log}"

# default to smoke; set SMOKE_ONLY=0 to actually flip DRYRUN
SMOKE_ONLY="${SMOKE_ONLY:-1}"
echo "[SMOKE] SMOKE_ONLY=$SMOKE_ONLY" | tee -a "$LOG"

snapshot_env_if_missing () {
  if [[ -f "$ENVFILE" ]]; then return 0; fi
  echo "[SMOKE] .env not found; snapshotting current environment -> $ENVFILE" | tee -a "$LOG"
  python - <<'PY' > "$ENVFILE"
import os, shlex
prefixes=("PF_","JUP_","JUPITER_","ROUTER_","ROUTEGUARD_","ENTRY_",
          "RAYDIUM_","PUMP_","JITO_","TELEGRAM_","PG_","DB_",
          "BUDGET_","TICKET_","BUY_","SELL_","HEALTH","RELAY_",
          "START_","DRYRUN")
def needs_quote(v: str) -> bool:
    return any(c in v for c in '=; #&$`"\\')
keys=[k for k in os.environ if k.startswith(prefixes) or k in ("DATABASE_URL","PNL_DSN")]
with open(".env","w") as f:
  for k in sorted(keys):
    v=os.environ[k]
    if needs_quote(v): v = shlex.quote(v)
    f.write(f"{k}={v}\n")
print("snapshot complete")
PY
  echo "[SMOKE] snapshot complete" | tee -a "$LOG"
}

backup_env () {
  local stamp
  stamp="$(date +%Y%m%d-%H%M%S)"
  BACKUP="$ROOT/.env.bak.${stamp}"
  echo "[SMOKE] Backing up $ENVFILE -> $BACKUP" | tee -a "$LOG"
  if ! cp -p "$ENVFILE" "$BACKUP"; then
    echo "[SMOKE] Warning: failed to copy $ENVFILE to $BACKUP" | tee -a "$LOG"
  fi
}

load_env () {
  if [[ -f "$ENVFILE" ]]; then
    set -a
    # shellcheck disable=SC1090
    . "$ENVFILE"
    set +a
  else
    echo "[SMOKE] load_env: $ENVFILE not present" | tee -a "$LOG"
  fi
}

flip_dryrun_optionally () {
  if [[ "$SMOKE_ONLY" == "1" ]]; then
    echo "[SMOKE] Leaving DRYRUN unchanged" | tee -a "$LOG"
    return 0
  fi
  if [[ ! -f "$ENVFILE" ]]; then
    echo "[SMOKE] ERROR: cannot flip DRYRUN because $ENVFILE is missing" | tee -a "$LOG"
    return 1
  fi
  if grep -q '^DRYRUN=True' "$ENVFILE"; then
    sed -i '0,/^DRYRUN=True/{s/^DRYRUN=True/DRYRUN=False/}' "$ENVFILE"
    echo "[SMOKE] DRYRUN=True → DRYRUN=False" | tee -a "$LOG"
  elif grep -q '^DRYRUN=False' "$ENVFILE"; then
    echo "[SMOKE] DRYRUN already False" | tee -a "$LOG"
  else
    echo "DRYRUN=False" >> "$ENVFILE"
    echo "[SMOKE] DRYRUN key appended as False" | tee -a "$LOG"
  fi
}

preflight_checks () {
  echo "[SMOKE] Router + Canary checks…" | tee -a "$LOG"
  python -m scripts.check_router 2>&1 | tee -a "$LOG" || return 1
  python -m scripts.canary_trade 2>&1 | tee -a "$LOG" || return 1
}

restart_service () {
  echo "[SMOKE] Restarting service…" | tee -a "$LOG"
  pkill -f '[s]tart.sh' || true
  nohup bash ./start.sh > /tmp/sniper_live.out 2>&1 &
}

main () {
  snapshot_env_if_missing
  backup_env

  load_env
  flip_dryrun_optionally
  load_env

  if ! preflight_checks; then
    echo "[SMOKE] Preflight checks failed" | tee -a "$LOG"
    exit 2
  fi

  restart_service

  if [[ "$SMOKE_ONLY" == "1" ]]; then
    echo "[SMOKE] Complete. DRYRUN unchanged. Backup at $BACKUP" | tee -a "$LOG"
  else
    echo "[GO-LIVE] Complete. DRYRUN=False. Backup at $BACKUP" | tee -a "$LOG"
  fi
  echo "[SMOKE] Tail: /tmp/sniper_live.out  | Log: $LOG" | tee -a "$LOG"
}

main "$@"
