#!/usr/bin/env bash
set -euo pipefail

# 1) Prepare wallet secret on disk (only if WALLET_JSON is provided)
mkdir -p /opt/render/.secrets
chmod 700 /opt/render/.secrets

if [[ -n "${WALLET_JSON:-}" && ! -f /opt/render/.secrets/wallet.json ]]; then
  printf '%s' "${WALLET_JSON}" > /opt/render/.secrets/wallet.json
  chmod 600 /opt/render/.secrets/wallet.json
fi

# Point app at the secret file (your code already knows how to read this)
export WALLET_PRIVATE_KEY=/opt/render/.secrets/wallet.json

# 2) Start local Jupiter relay in the background
pkill -f '\[j]up_relay.py' || true
nohup ./jup_relay.py >/tmp/jup_relay.log 2>&1 &
echo $! >/tmp/jup_relay.pid
sleep 1

# 3) Run the long-lived worker (Render watches this process)
exec python -m scripts.main
