#!/usr/bin/env bash
# start.sh — Render entrypoint (end-to-end, hardened)
# - Loads .env
# - Pins quote-api.jup.ag to IPv4 (if DNS is broken)
# - Boots optional local Jupiter relay (IPv4 only)
# - Normalizes DSNs, light health checks, verbose logging
# - Starts main app

set -Eeuo pipefail
IFS=$'\n\t'

ts() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
log(){ printf '[start] %s %s\n' "$(ts)" "$*"; }

# ──────────────────────────────────────────────────────────────────────────────
# 0) Load .env FIRST
# ──────────────────────────────────────────────────────────────────────────────
set -a
if [[ -f .env ]]; then
  # shellcheck disable=SC1091
  . ./.env
  log "loaded .env"
else
  log "no .env found (ok if using Render dashboard env)"
fi
set +a

log "booting…"

# ──────────────────────────────────────────────────────────────────────────────
# 1) Force quote-api.jup.ag to resolve even if DNS is broken
#    We pin it to the CloudFront IPv4 we can resolve from api.jup.ag
# ──────────────────────────────────────────────────────────────────────────────
: "${RELAY_FORCE_IPV4:=1}"   # default: force IPv4 everywhere we can
if [[ "${RELAY_FORCE_IPV4}" = "1" ]]; then
  API4="$(getent ahostsv4 api.jup.ag | awk '/STREAM/ {print $1; exit}' || true)"
  if [[ -n "${API4}" ]]; then
    log "using api.jup.ag IPv4: ${API4} → pinning quote-api.jup.ag"
    # Try to append to /etc/hosts (works on Render). Fall back to temp file if needed.
    if grep -q 'quote-api.jup.ag' /etc/hosts 2>/dev/null; then
      log "/etc/hosts already has quote-api.jup.ag"
    else
      if (echo "${API4} quote-api.jup.ag" | sudo tee -a /etc/hosts >/dev/null) || \
         (echo "${API4} quote-api.jup.ag" | tee -a /etc/hosts >/dev/null); then
        log "pinned quote-api.jup.ag → ${API4}"
      else
        log "WARN: couldn't write /etc/hosts; continuing (relay may still work if DNS is OK)"
      fi
    fi
  else
    log "WARN: couldn't resolve api.jup.ag to IPv4; skipping pin (relay may still work)"
  fi
fi

# Quick upstream ping (expect JSON or 401). Non-fatal.
if command -v curl >/dev/null 2>&1; then
  curl ${RELAY_FORCE_IPV4:+-4} -m 5 -sS -H "x-api-key: ${JUP_API_KEY:-}" \
    "https://quote-api.jup.ag/v6/ping" >/dev/null || \
  log "note: /v6/ping didn't return 200 (401 is also fine) — continuing"
fi

# ──────────────────────────────────────────────────────────────────────────────
# 2) Wallet bootstrap (optional)
# ──────────────────────────────────────────────────────────────────────────────
mkdir -p /opt/render/.secrets
chmod 700 /opt/render/.secrets
if [[ -n "${WALLET_JSON:-}" && ! -f /opt/render/.secrets/wallet.json ]]; then
  printf '%s' "$WALLET_JSON" > /opt/render/.secrets/wallet.json
  chmod 600 /opt/render/.secrets/wallet.json
fi
export WALLET_PRIVATE_KEY="${WALLET_PRIVATE_KEY:-/opt/render/.secrets/wallet.json}"

# ──────────────────────────────────────────────────────────────────────────────
# 3) Local data dirs
# ──────────────────────────────────────────────────────────────────────────────
mkdir -p /opt/render/data /tmp
chmod 755 /opt/render/data || true

# ──────────────────────────────────────────────────────────────────────────────
# 4) Python selection
# ──────────────────────────────────────────────────────────────────────────────
PYTHON="python"
[[ -x ".venv/bin/python" ]] && PYTHON=".venv/bin/python"
log "python: $(${PYTHON} -V 2>&1)"

# ──────────────────────────────────────────────────────────────────────────────
# 5) DSN normalizers
# ──────────────────────────────────────────────────────────────────────────────
normalize_sqlalchemy_dsn() {
  local v="$1"
  [[ "$v" =~ ^postgresql:// ]] && v="${v/postgresql:\/\//postgresql+asyncpg://}"
  v="${v//ssl=true/sslmode=require}"
  if [[ "$v" == *\?* ]]; then
    [[ "$v" == *sslmode=* ]] || v="${v}&sslmode=require"
  else
    v="${v}?sslmode=require"
  fi
  printf '%s' "$v"
}
normalize_asyncpg_dsn() {
  local v="$1"
  v="${v/postgresql+asyncpg:\/\//postgresql://}"
  v="${v//ssl=true/sslmode=require}"
  if [[ "$v" == *\?* ]]; then
    [[ "$v" == *sslmode=* ]] || v="${v}&sslmode=require"
  else
    v="${v}?sslmode=require"
  fi
  printf '%s' "$v"
}

: "${DATABASE_URL:?DATABASE_URL must be set}"
: "${PG_DSN_INTERNAL:?PG_DSN_INTERNAL must be set}"
: "${PG_DSN_EXTERNAL:?PG_DSN_EXTERNAL must be set}"
: "${PG_DSN:?PG_DSN must be set}"

PNL_DSN="${PNL_DSN:-$PG_DSN}"

DATABASE_URL="$(normalize_sqlalchemy_dsn "${DATABASE_URL}")"
PG_DSN_INTERNAL="$(normalize_sqlalchemy_dsn "${PG_DSN_INTERNAL}")"
PG_DSN_EXTERNAL="$(normalize_sqlalchemy_dsn "${PG_DSN_EXTERNAL}")"
PG_DSN="$(normalize_asyncpg_dsn "${PG_DSN}")"
PNL_DSN="$(normalize_asyncpg_dsn "${PNL_DSN}")"

export DATABASE_URL PG_DSN_INTERNAL PG_DSN_EXTERNAL PG_DSN PNL_DSN

log "DB(SQLA): ${DATABASE_URL%%\?*}?…"
log "DB(asyncpg): ${PNL_DSN%%\?*}?…"

# Non-fatal asyncpg probe
${PYTHON} - <<'PY' || true
import os, asyncio, sys
try:
    import asyncpg
except Exception:
    print("[start] asyncpg not installed; skipping DSN probe", flush=True); sys.exit(0)
dsn = os.environ.get("PNL_DSN") or os.environ.get("PG_DSN")
async def go():
    try:
        conn = await asyncpg.connect(dsn)
        print("[start] asyncpg DSN OK", flush=True)
        await conn.close()
    except Exception as e:
        print(f"[start] asyncpg DSN PROBE FAILED: {e}", flush=True)
asyncio.run(go())
PY

# ──────────────────────────────────────────────────────────────────────────────
# 6) Local Jupiter relay (if enabled)
#    - binds 127.0.0.1:8081
#    - upstream: https://quote-api.jup.ag/v6  (correct host for /v6/quote)
# ──────────────────────────────────────────────────────────────────────────────
: "${RELAY_ENABLED:=0}"
: "${RELAY_BIND:=127.0.0.1}"
: "${RELAY_PORT:=8081}"
: "${RELAY_UPSTREAM:=https://quote-api.jup.ag/v6}"

if [[ "${RELAY_ENABLED}" = "1" ]]; then
  if [[ -f "./jup_relay.py" ]]; then
    log "starting local Jupiter relay (RELAY_ENABLED=1)…"
    # pass hints to the relay through env
    export RELAY_BIND RELAY_PORT RELAY_UPSTREAM RELAY_FORCE_IPV4
    export JUP_API_KEY="${JUP_API_KEY:-}"  # relay forwards this header

    pkill -f '[j]up_relay.py' || true
    nohup "${PYTHON}" -u ./jup_relay.py > /tmp/jup_relay.log 2>&1 &
    echo $! > /tmp/jup_relay.pid

    # Wait up to 12s for health
    for _ in {1..12}; do
      if curl ${RELAY_FORCE_IPV4:+-4} -fsS "http://${RELAY_BIND}:${RELAY_PORT}/__debug" >/dev/null 2>&1; then
        log "relay up on ${RELAY_BIND}:${RELAY_PORT}"
        export JUP_BASE_URL="http://${RELAY_BIND}:${RELAY_PORT}"
        export JUP_ALT_BASE_URLS="http://${RELAY_BIND}:${RELAY_PORT}"
        break
      fi
      sleep 1
    done

    if [[ "${JUP_BASE_URL:-}" != "http://${RELAY_BIND}:${RELAY_PORT}" ]]; then
      log "relay failed to start; head log:"
      (head -n 120 /tmp/jup_relay.log || true) | sed 's/^/[relay] /'
      log "falling back to public Jupiter endpoints"
      export JUP_BASE_URL="https://quote-api.jup.ag/v6"
      export JUP_ALT_BASE_URLS="https://quote-api.jup.ag/v6"
    fi
  else
    log "RELAY_ENABLED=1 but jup_relay.py not found; using public Jupiter"
    export JUP_BASE_URL="https://quote-api.jup.ag/v6"
    export JUP_ALT_BASE_URLS="https://quote-api.jup.ag/v6"
  fi
else
  log "skipping local Jupiter relay (RELAY_ENABLED!=1)"
  : "${JUP_BASE_URL:=https://quote-api.jup.ag/v6}"
  : "${JUP_ALT_BASE_URLS:=https://quote-api.jup.ag/v6}"
  export JUP_BASE_URL JUP_ALT_BASE_URLS
fi

log "JUP_BASE_URL: ${JUP_BASE_URL}"
[[ -n "${JUP_ALT_BASE_URLS}" ]] && log "JUP_ALT_BASE_URLS: ${JUP_ALT_BASE_URLS}"

# ──────────────────────────────────────────────────────────────────────────────
# 7) Positions storage guard (sqlite shortcut)
# ──────────────────────────────────────────────────────────────────────────────
if [[ "${PF_POSITIONS_BACKEND:-pg}" != "pg" ]]; then
  export POSITIONS_PG_ENABLED=0
  export POSITIONS_PG_DSN=""
fi

# ──────────────────────────────────────────────────────────────────────────────
# 8) Optional prechecks (set START_PRECHECKS=1)
# ──────────────────────────────────────────────────────────────────────────────
: "${START_PRECHECKS:=0}"
if [[ "${START_PRECHECKS}" = "1" ]]; then
  log "precheck: scripts.check_router"
  "${PYTHON}" -m scripts.check_router || log "check_router returned non-zero"
  log "precheck: scripts.canary_trade"
  "${PYTHON}" -m scripts.canary_trade || log "canary_trade returned non-zero"
fi

# ──────────────────────────────────────────────────────────────────────────────
# 9) Final launch
# ──────────────────────────────────────────────────────────────────────────────
if [[ "${PRINT_ENV:-0}" = "1" ]]; then
  log "env snapshot (selected):"
  printf '  RELAY_ENABLED=%s\n  RELAY_BIND=%s\n  RELAY_PORT=%s\n  RELAY_UPSTREAM=%s\n' \
    "${RELAY_ENABLED}" "${RELAY_BIND}" "${RELAY_PORT}" "${RELAY_UPSTREAM}"
  printf '  JUP_BASE_URL=%s\n  JUP_ALT_BASE_URLS=%s\n' "${JUP_BASE_URL}" "${JUP_ALT_BASE_URLS}"
fi

exec "${PYTHON}" -m scripts.main
