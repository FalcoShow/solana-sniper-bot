#!/usr/bin/env bash
# go_live.sh — flip DRYRUN off and restart safely (or smoke test only)
set -Eeuo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

ENVFILE="$ROOT/.env"
BACKUP_TMP="$(mktemp -u "$ROOT/.env.bak.XXXXXXXX")"
LOG="${LOG:-/tmp/go_live.log}"

# Optional Telegram notify (skip if not configured)
TELEGRAM_BOT_TOKEN="${TELEGRAM_BOT_TOKEN:-}"
TELEGRAM_CHAT_ID="${TELEGRAM_CHAT_ID:-}"

# SMOKE_ONLY=1 → do NOT flip DRYRUN, just run checks & restart
SMOKE_ONLY="${SMOKE_ONLY:-0}"

send_tg () {
  local msg="$1"
  if [[ -n "$TELEGRAM_BOT_TOKEN" ]]; then
    # try provided chat id, else attempt to auto-discover the last chat if possible
    if [[ -z "$TELEGRAM_CHAT_ID" ]]; then
      local _chat
      _chat="$(curl -s "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/getUpdates" \
        | python -c "import sys,json;d=json.load(sys.stdin);r=d.get('result',[]);print(r[-1]['message']['chat']['id'] if r else '')" 2>/dev/null || true)"
      TELEGRAM_CHAT_ID="${TELEGRAM_CHAT_ID:-$_chat}"
    fi
    if [[ -n "$TELEGRAM_CHAT_ID" ]]; then
      curl -s -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
        -d chat_id="${TELEGRAM_CHAT_ID}" -d text="$msg" >/dev/null || true
    fi
  fi
}

# safe snapshot: write a quoted .env when none exists
snapshot_env_if_missing () {
  if [[ -f "$ENVFILE" ]]; then return 0; fi
  echo "[go_live] .env not found; snapshotting current environment -> $ENVFILE" | tee -a "$LOG"
  python - <<'PY' > "$ENVFILE"
import os, shlex
prefixes=("PF_","JUP_","JUPITER_","ROUTER_","ROUTEGUARD_","ENTRY_",
          "RAYDIUM_","PUMP_","JITO_","TELEGRAM_","PG_","DB_",
          "BUDGET_","TICKET_","BUY_","SELL_","HEALTH","RELAY_",
          "START_","DRYRUN")
def needs_quote(v: str) -> bool:
    # quote if value contains characters that will break shell parsing, including '=' and ';'
    return any(c in v for c in '=; #&$`"\\')
keys=[k for k in os.environ if k.startswith(prefixes) or k in ("DATABASE_URL","PNL_DSN")]
with open(".env","w") as f:
  for k in sorted(keys):
    v=os.environ[k]
    if needs_quote(v): v = shlex.quote(v)
    f.write(f"{k}={v}\n")
print("snapshot complete")
PY
  echo "[go_live] snapshot complete" | tee -a "$LOG"
}

backup_env () {
  # create timestamped backup; use safe move to avoid partial copies
  local stamp
  stamp="$(date +%Y%m%d-%H%M%S)"
  BACKUP="$ROOT/.env.bak.${stamp}"
  echo "[go_live] Backing up $ENVFILE -> $BACKUP" | tee -a "$LOG"
  if ! cp -p "$ENVFILE" "$BACKUP"; then
    echo "[go_live] Warning: failed to copy $ENVFILE to $BACKUP" | tee -a "$LOG"
  fi
}

load_env () {
  # export everything from .env so Python modules see it; don't fail if file missing
  if [[ -f "$ENVFILE" ]]; then
    # use a subshell to avoid polluting caller env if needed — but we want to export to current shell:
    set -a
    # shellcheck disable=SC1090
    . "$ENVFILE"
    set +a
  else
    echo "[go_live] load_env: $ENVFILE not present, nothing loaded" | tee -a "$LOG"
  fi
}

flip_dryrun_if_needed () {
  if [[ "$SMOKE_ONLY" == "1" ]]; then
    echo "[go_live] SMOKE_ONLY=1 → leaving DRYRUN unchanged" | tee -a "$LOG"
    return 0
  fi
  if [[ ! -f "$ENVFILE" ]]; then
    echo "[go_live] ERROR: cannot flip DRYRUN because $ENVFILE is missing" | tee -a "$LOG"
    return 1
  fi
  if grep -q '^DRYRUN=True' "$ENVFILE"; then
    # atomic edit: write to tmp and move
    local tmp="${ENVFILE}.tmp.$$"
    sed '0,/^DRYRUN=True/{s/^DRYRUN=True/DRYRUN=False/}' "$ENVFILE" > "$tmp"
    mv -f "$tmp" "$ENVFILE"
    echo "[go_live] DRYRUN=True → DRYRUN=False" | tee -a "$LOG"
  elif grep -q '^DRYRUN=False' "$ENVFILE"; then
    echo "[go_live] DRYRUN already False" | tee -a "$LOG"
  else
    echo "DRYRUN=False" >> "$ENVFILE"
    echo "[go_live] DRYRUN key not found; appended DRYRUN=False" | tee -a "$LOG"
  fi
}

rollback_and_exit () {
  local why="$1"
  echo "[go_live] ERROR: $why — attempting rollback" | tee -a "$LOG"
  if [[ -n "${BACKUP:-}" && -f "$BACKUP" ]]; then
    bash "$ROOT/deploy_tools/rollback.sh" "$BACKUP" 2>&1 | tee -a "$LOG" || true
    send_tg "[ROLLBACK] $why — env restored from backup."
  else
    echo "[go_live] No backup found to rollback to." | tee -a "$LOG"
    send_tg "[ROLLBACK-FAILED] $why — no backup available."
  fi
  exit 2
}

preflight_checks () {
  echo "[go_live] Running pre-live checks…" | tee -a "$LOG"
  # capture failures and rollback
  if ! python -m scripts.check_router 2>&1 | tee -a "$LOG"; then
    rollback_and_exit "check_router failed"
  fi
  if ! python -m scripts.canary_trade 2>&1 | tee -a "$LOG"; then
    rollback_and_exit "canary_trade failed"
  fi
}

restart_service () {
  echo "[go_live] Restarting service via ./start.sh" | tee -a "$LOG"
  # kill only the start.sh process (pid file could be used instead)
  pkill -f '[s]tart.sh' || true
  nohup bash ./start.sh > /tmp/sniper_live.out 2>&1 &
}

# trap - if script exits with error, attempt rollback
trap 'rc=$?; if [[ $rc -ne 0 ]]; then echo "[go_live] EXIT with rc=$rc" | tee -a "$LOG"; rollback_and_exit "script exited (rc=$rc)"; fi' EXIT

main () {
  snapshot_env_if_missing
  backup_env
  load_env
  flip_dryrun_if_needed
  load_env                # reload after change
  preflight_checks
  restart_service

  # Successful end — cancel trap rollback and send success notice
  trap - EXIT

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
