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

# --- Jupiter base URLs: prefer local relay, but fall back automatically ---
export JUP_BASE_URL="${JUP_BASE_URL:-http://127.0.0.1:18080}"
export JUP_ALT_BASE_URLS="${JUP_ALT_BASE_URLS:-}"
export JUP_IPS="${JUP_IPS:-104.18.24.229,104.18.25.229}"

relay_ok=0
if [ -f "./jup_relay.py" ]; then
  pkill -f '[j]up_relay.py' || true
  echo "[relay] starting local relay…"
  nohup python ./jup_relay.py >/tmp/jup_relay.log 2>&1 & echo $! >/tmp/jup_relay.pid

  # Wait up to ~12s for the relay to come up
  for i in {1..24}; do
    if curl -s --max-time 1 http://127.0.0.1:18080/__debug >/tmp/jup_dbg.json; then
      echo "[relay] up: $(cat /tmp/jup_dbg.json)"
      relay_ok=1
      break
    fi
    sleep 0.5
  done
else
  echo "[relay] jup_relay.py not found in repo – skipping local relay"
fi

if [ "$relay_ok" -ne 1 ]; then
  echo "[relay] not running; using public API"
  export JUP_BASE_URL="https://quote-api.jup.ag"
  # (optional) show relay log if it exists so failures are visible in Render logs
  [ -f /tmp/jup_relay.log ] && sed -n '1,120p' /tmp/jup_relay.log || true
fi

# --- Run app ---
exec python -m scripts.main
