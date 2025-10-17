#!/usr/bin/env bash
set -euo pipefail

echo "[start] booting…"

# --- Wallet bootstrap ---
mkdir -p /opt/render/.secrets
chmod 700 /opt/render/.secrets
if [ -n "${WALLET_JSON:-}" ] && [ ! -f /opt/render/.secrets/wallet.json ]; then
  printf '%s' "$WALLET_JSON" > /opt/render/.secrets/wallet.json
  chmod 600 /opt/render/.secrets/wallet.json
fi
export WALLET_PRIVATE_KEY="${WALLET_PRIVATE_KEY:-/opt/render/.secrets/wallet.json}"

# --- Jupiter relay env ---
export JUP_HOST="${JUP_HOST:-quote-api.jup.ag}"
export JUP_IPS="${JUP_IPS:-104.18.24.229,104.18.25.229}"
export RELAY_BIND="${RELAY_BIND:-127.0.0.1}"
export RELAY_PORT="${RELAY_PORT:-18080}"

# --- Launch relay if present ---
if [ -f ./jup_relay.py ]; then
  chmod +x ./jup_relay.py
  pkill -f '[j]up_relay.py' || true
  nohup ./jup_relay.py >/tmp/jup_relay.log 2>&1 &
  echo $! >/tmp/jup_relay.pid

  # health check (up to ~8s)
  for i in {1..16}; do
    if curl -s "http://127.0.0.1:${RELAY_PORT}/__debug" >/dev/null; then
      echo "[relay] up on 127.0.0.1:${RELAY_PORT}"
      break
    fi
    sleep 0.5
  done

  export JUP_BASE_URL="http://127.0.0.1:${RELAY_PORT}"
  export JUP_ALT_BASE_URLS=""
else
  echo "[relay] jup_relay.py not found; using public API directly"
  # If you truly can’t commit the relay, fall back to public domain:
  export JUP_BASE_URL="${JUP_BASE_URL:-https://quote-api.jup.ag}"
fi

# --- Run app ---
exec python -m scripts.main
