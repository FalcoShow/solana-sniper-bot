# scripts/check_env.py
from __future__ import annotations
import os, socket, json, sys
req = ["SOL_RPC_URL","PF_TRADE_SOL","PF_SLIPPAGE_BPS","JUP_BASE_URL"]
warn = []
missing = [k for k in req if not os.getenv(k)]
if missing: warn.append(("missing_required_env", missing))
hosts = ["api.jup.ag","lite-api.jup.ag","public.jupiterapi.com","quote-api.jup.ag"]
dns = {}
for h in hosts:
    try:
        dns[h] = socket.getaddrinfo(h, 443)[0][4][0]
    except Exception as e:
        dns[h] = f"DNS ERROR: {e}"
print(json.dumps({
  "cwd": os.getcwd(),
  "py_ver": sys.version.split()[0],
  "PYTHONPATH": os.getenv("PYTHONPATH"),
  "env_core": {k: os.getenv(k) for k in ["JUP_BASE_URL","JUP_ALT_BASE_URLS","JUP_MAX_QPS",
                                         "JUP_FAIL_LIMIT","JUP_BREAKER_COOLDOWN_SEC",
                                         "PF_TRADE_SOL","PF_SLIPPAGE_BPS","DRYRUN"]},
  "dns": dns,
  "warnings": warn
}, indent=2))
