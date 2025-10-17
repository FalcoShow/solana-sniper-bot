#!/usr/bin/env bash
set -euo pipefail

echo "[start] booting…"

# --- Wallet file bootstrap (optional) ---
mkdir -p /opt/render/.secrets
chmod 700 /opt/render/.secrets

if [ -n "${WALLET_JSON:-}" ] && [ ! -f /opt/render/.secrets/wallet.json ]; then
  printf '%s' "$WALLET_JSON" > /opt/render/.secrets/wallet.json
  chmod 600 /opt/render/.secrets/wallet.json
fi

# If not set, default to the wallet file we just wrote
export WALLET_PRIVATE_KEY="${WALLET_PRIVATE_KEY:-/opt/render/.secrets/wallet.json}"

# --- Jupiter relay ---
export JUP_BASE_URL="${JUP_BASE_URL:-http://127.0.0.1:18080}"
export JUP_ALT_BASE_URLS="${JUP_ALT_BASE_URLS:-}"
export JUP_IPS="${JUP_IPS:-104.18.24.229,104.18.25.229}"

pkill -f '[j]up_relay.py' || true
nohup ./jup_relay.py >/tmp/jup_relay.log 2>&1 & echo $! >/tmp/jup_relay.pid
sleep 1

# (Optional) quick relay check
python - <<'PY'
import json, os, urllib.request
u=os.environ.get("JUP_BASE_URL","http://127.0.0.1:18080")+"/__debug"
try:
    print("[relay]", json.load(urllib.request.urlopen(u)))
except Exception as e:
    print("[relay] check failed:", e)
PY

# --- Run app ---
exec python main.py
