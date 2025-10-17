#!/usr/bin/env bash
set -euo pipefail

echo "[start] booting…"

###############################################################################
# 1) Wallet bootstrap (always overwrite from WALLET_JSON if provided)
###############################################################################
mkdir -p /opt/render/.secrets
chmod 700 /opt/render/.secrets

if [ -n "${WALLET_JSON:-}" ]; then
  # write/overwrite on every deploy to avoid stale/bad files
  umask 077
  printf '%s' "$WALLET_JSON" > /opt/render/.secrets/wallet.json
fi

# If caller didn’t point to an absolute key path, use the file we maintain
export WALLET_PRIVATE_KEY="${WALLET_PRIVATE_KEY:-/opt/render/.secrets/wallet.json}"

# Preflight sanity check so we fail fast with a clear message
python - <<'PY'
import os, json, sys
p = os.environ.get("WALLET_PRIVATE_KEY") or ""
if not p or not os.path.isabs(p) or not os.path.exists(p):
    print(f"[wallet] missing file or not absolute: {p!r}", file=sys.stderr)
    sys.exit(1)
raw = open(p, "r", encoding="utf-8").read().strip()
try:
    arr = json.loads(raw)
    ok = isinstance(arr, list) and len(arr) in (32, 64) and all(isinstance(x,int) for x in arr)
except Exception:
    ok = False
if not ok:
    print("[wallet] invalid content. Expect JSON array of 32/64 ints.", file=sys.stderr)
    print("[wallet] first 100 bytes:", raw[:100], file=sys.stderr)
    sys.exit(1)
print(f"[wallet] OK: {len(arr)}-int key at {p}")
PY

###############################################################################
# 2) Jupiter relay (local proxy). Non-fatal if it can’t validate.
###############################################################################
export JUP_BASE_URL="${JUP_BASE_URL:-http://127.0.0.1:18080}"
export JUP_ALT_BASE_URLS="${JUP_ALT_BASE_URLS:-}"
export JUP_IPS="${JUP_IPS:-104.18.24.229,104.18.25.229}"

# clean previous relay if present
pkill -f '[j]up_relay.py' || true
nohup python jup_relay.py >/tmp/jup_relay.log 2>&1 & echo $! >/tmp/jup_relay.pid
sleep 1

# soft check (don’t fail app if relay isn’t ready yet)
python - <<'PY' || true
import os, json, urllib.request
base = os.environ.get("JUP_BASE_URL","http://127.0.0.1:18080").rstrip("/")
u = f"{base}/__debug"
try:
    with urllib.request.urlopen(u, timeout=2) as r:
        print("[relay]", json.load(r))
except Exception as e:
    print("[relay] check skipped:", e)
PY

###############################################################################
# 3) Launch app
###############################################################################
exec python -m scripts.main
