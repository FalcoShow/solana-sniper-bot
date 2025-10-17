#!/usr/bin/env bash
set -euo pipefail

echo "[start] booting…"

# --- Wallet bootstrap ---
mkdir -p /opt/render/.secrets && chmod 700 /opt/render/.secrets
if [ -n "${WALLET_JSON:-}" ] && [ ! -f /opt/render/.secrets/wallet.json ]; then
  printf '%s' "$WALLET_JSON" > /opt/render/.secrets/wallet.json
  chmod 600 /opt/render/.secrets/wallet.json
fi
export WALLET_PRIVATE_KEY="${WALLET_PRIVATE_KEY:-/opt/render/.secrets/wallet.json}"

# --- Jupiter relay env (local first) ---
export JUP_BASE_URL="${JUP_BASE_URL:-http://127.0.0.1:18080}"
export JUP_ALT_BASE_URLS="${JUP_ALT_BASE_URLS:-}"
export JUP_IPS="${JUP_IPS:-104.18.24.229,104.18.25.229}"

# --- (Re)start relay robustly ---
pkill -f '[j]up_relay.py' || true
chmod +x jup_relay.py || true
nohup python jup_relay.py >/tmp/jup_relay.log 2>&1 & echo $! >/tmp/jup_relay.pid

# Wait for relay (max ~12s). If it fails, dump logs and fall back to direct HTTPS.
ok=0
for i in {1..24}; do
  if curl -s --max-time 1 http://127.0.0.1:18080/__debug >/tmp/jup_dbg.json; then
    echo "[relay] up: $(cat /tmp/jup_dbg.json)"
    ok=1
    break
  fi
  sleep 0.5
done

if [ "$ok" -ne 1 ]; then
  echo "[relay] failed to start; showing log (first 120 lines):"
  sed -n '1,120p' /tmp/jup_relay.log || true
  export JUP_BASE_URL="https://quote-api.jup.ag"
  echo "[relay] falling back to $JUP_BASE_URL"
fi

# --- Run app ---
exec python main.py
