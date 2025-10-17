#!/usr/bin/env bash
# start.sh — render entrypoint
# robust boot with local Jupiter relay + async Postgres sanity

set -Eeuo pipefail

log(){ printf '[start] %s\n' "$*"; }

log "booting…"

# ---------- Wallet bootstrap ----------
mkdir -p /opt/render/.secrets
chmod 700 /opt/render/.secrets

# If WALLET_JSON provided and no wallet file exists, write it
if [[ -n "${WALLET_JSON:-}" && ! -f /opt/render/.secrets/wallet.json ]]; then
  printf '%s' "$WALLET_JSON" > /opt/render/.secrets/wallet.json
  chmod 600 /opt/render/.secrets/wallet.json
fi

# Default to the wallet file unless explicitly overridden
export WALLET_PRIVATE_KEY="${WALLET_PRIVATE_KEY:-/opt/render/.secrets/wallet.json}"

# ---------- Environment sanity (DB DSNs) ----------
# SQLAlchemy async URLs MUST use +asyncpg and sslmode=require
: "${DATABASE_URL:?DATABASE_URL must be set (e.g. postgresql+asyncpg://.../db?sslmode=require)}"
: "${PG_DSN_INTERNAL:?PG_DSN_INTERNAL must be set (e.g. postgresql+asyncpg://.../db?sslmode=require)}"
: "${PG_DSN_EXTERNAL:?PG_DSN_EXTERNAL must be set (e.g. postgresql+asyncpg://.../db?sslmode=require)}"

# Direct asyncpg DSN MUST NOT use +asyncpg, and should use sslmode=require (NOT ssl=true)
: "${PNL_DSN:?PNL_DSN must be set (e.g. postgresql://.../db?sslmode=require)}"

# ---------- Ensure local data dirs exist (e.g., sqlite, logs) ----------
mkdir -p data
mkdir -p /tmp
chmod 755 data || true

# ---------- Choose Python (prefer Render venv if present) ----------
PYTHON="python"
if [[ -x ".venv/bin/python" ]]; then
  PYTHON=".venv/bin/python"
fi
log "python: $(${PYTHON} -V 2>&1)"

# ---------- Optional local Jupiter relay ----------
# Using a local relay avoids DNS/CNAME/CF quirks in restricted egress envs.
RELAY_BIND="${RELAY_BIND:-127.0.0.1}"
RELAY_PORT="${RELAY_PORT:-18080}"

if [[ -f "./jup_relay.py" ]]; then
  log "starting local Jupiter relay…"
  # Clean any prior process
  pkill -f '[j]up_relay.py' || true

  # Start relay with same interpreter as app
  nohup "${PYTHON}" -u ./jup_relay.py \
    --bind "${RELAY_BIND}" \
    --port "${RELAY_PORT}" \
    > /tmp/jup_relay.log 2>&1 &

  echo $! > /tmp/jup_relay.pid

  # Wait up to ~12s for health
  for i in {1..12}; do
    if curl -fsS "http://${RELAY_BIND}:${RELAY_PORT}/__debug" >/dev/null 2>&1; then
      log "relay up on ${RELAY_BIND}:${RELAY_PORT}"
      # Force app to use local relay
      export JUP_BASE_URL="http://${RELAY_BIND}:${RELAY_PORT}"
      export JUP_ALT_BASE_URLS=""
      break
    fi
    sleep 1
  done

  # Fallback if relay didn’t come up
  if [[ "${JUP_BASE_URL:-}" != "http://${RELAY_BIND}:${RELAY_PORT}" ]]; then
    log "relay failed to start; printing first 120 lines of log:"
    (head -n 120 /tmp/jup_relay.log || true) | sed 's/^/[relay] /'
    log "falling back to public Jupiter"
    export JUP_BASE_URL="${JUP_BASE_URL:-https://quote-api.jup.ag}"
    export JUP_ALT_BASE_URLS=""
  fi
else
  log "jup_relay.py not found; using public Jupiter API"
  : "${JUP_BASE_URL:=https://quote-api.jup.ag}"
  export JUP_BASE_URL
  export JUP_ALT_BASE_URLS="${JUP_ALT_BASE_URLS:-}"
fi

# Show a tiny boot summary (useful in Render logs)
log "DB(SQLA): ${DATABASE_URL%%\?*}?…"
log "DB(asyncpg): ${PNL_DSN%%\?*}?…"
log "JUP_BASE_URL: ${JUP_BASE_URL}"

# ---------- Final launch ----------
# Run the app module
exec "${PYTHON}" -m scripts.main
