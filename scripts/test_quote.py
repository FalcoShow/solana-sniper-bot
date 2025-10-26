# scripts/test_quote.py
from __future__ import annotations
import os
import json
import sys
import requests

# ----------------------------
# Config (env overrides)
# ----------------------------
SOL_MINT  = os.getenv("SOL_MINT",  "So11111111111111111111111111111111111111112")
USDC_MINT = os.getenv("USDC_MINT", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")

# 0.05 SOL default (in SOL); can override with TEST_QUOTE_SOL like "0.10"
QUOTE_SOL = float(os.getenv("TEST_QUOTE_SOL", "0.05"))
LAMPORTS  = int(QUOTE_SOL * 1_000_000_000)

SLIPPAGE_BPS = int(float(os.getenv("PF_SLIPPAGE_BPS", "120")))
ONLY_DIRECT  = os.getenv("JUP_ONLY_DIRECT", "0").lower() in ("1", "true", "yes")
AS_LEGACY    = os.getenv("JUP_AS_LEGACY", "0").lower() in ("1", "true", "yes")

# Endpoints (prefer Pro → Lite; legacy for emergency)
PRO   = (os.getenv("JUP_UPSTREAM") or "https://api.jup.ag").rstrip("/")
LITE  = (os.getenv("JUP_LITE")     or "https://lite-api.jup.ag").rstrip("/")
V6    = (os.getenv("JUP_LEGACY")   or "https://quote-api.jup.ag").rstrip("/")

# Optional Pro key
PRO_KEY = os.getenv("JUP_PRO_KEY")

TIMEOUT_S = float(os.getenv("JUP_TEST_TIMEOUT_SEC", "12"))

# ----------------------------
# Helpers
# ----------------------------
def _bool_str(flag: bool) -> str:
    """Jupiter v1 expects lowercase 'true'|'false' strings."""
    return "true" if flag else "false"

def _pretty_err(ctx: str, exc: Exception, resp: requests.Response | None = None) -> str:
    msg = f"{ctx}: {repr(exc)}"
    if resp is not None:
        try:
            j = resp.json()
            msg += f"\n  body: {json.dumps(j)[:500]}"
        except Exception:
            msg += f"\n  body(raw): {resp.text[:500]}"
    return msg

# ----------------------------
# Callers
# ----------------------------
def try_quote_v1(base: str) -> dict:
    """Jupiter v1 /swap/v1/quote"""
    url = f"{base}/swap/v1/quote"
    params = {
        "inputMint": SOL_MINT,
        "outputMint": USDC_MINT,
        "amount": str(LAMPORTS),
        "slippageBps": SLIPPAGE_BPS,
        "swapMode": "ExactIn",                 # required by v1
        "onlyDirectRoutes": _bool_str(ONLY_DIRECT),
        "asLegacyTransaction": _bool_str(AS_LEGACY),
    }
    headers = {"Accept": "application/json"}
    if PRO_KEY and ("api.jup.ag" in base):
        headers["x-api-key"] = PRO_KEY

    try:
        r = requests.get(url, params=params, headers=headers, timeout=TIMEOUT_S)
        if r.status_code >= 400:
            raise requests.HTTPError(f"{r.status_code}", response=r)
        return r.json()
    except requests.HTTPError as he:
        raise RuntimeError(_pretty_err(f"HTTP {base}", he, he.response))
    except Exception as e:
        raise RuntimeError(_pretty_err(f"REQ {base}", e))

def try_quote_v6(base: str) -> dict:
    """Legacy v6 /v6/quote (DNS to quote-api.jup.ag may fail in some containers)"""
    url = f"{base}/v6/quote"
    params = {
        "inputMint": SOL_MINT,
        "outputMint": USDC_MINT,
        "amount": str(LAMPORTS),
        "slippageBps": SLIPPAGE_BPS,
        "onlyDirectRoutes": _bool_str(ONLY_DIRECT),
        "asLegacyTransaction": _bool_str(AS_LEGACY),
    }
    try:
        r = requests.get(url, params=params, timeout=TIMEOUT_S)
        if r.status_code >= 400:
            raise requests.HTTPError(f"{r.status_code}", response=r)
        return r.json()
    except requests.HTTPError as he:
        raise RuntimeError(_pretty_err(f"HTTP {base}", he, he.response))
    except Exception as e:
        raise RuntimeError(_pretty_err(f"REQ {base}", e))

# ----------------------------
# Main
# ----------------------------
def main() -> None:
    print(f"[test_quote] amount={QUOTE_SOL} SOL | slippageBps={SLIPPAGE_BPS} | onlyDirect={ONLY_DIRECT} | legacyTx={AS_LEGACY}")
    order = [("PRO", try_quote_v1, PRO), ("LITE", try_quote_v1, LITE), ("LEGACY", try_quote_v6, V6)]

    last_err = None
    for label, fn, base in order:
        try:
            data = fn(base)
            out = {
                "endpoint": label,
                "base": base,
                "routePlan_len": (
                    len(data.get("routePlan", []))
                    if isinstance(data.get("routePlan"), list) else "n/a"
                ),
                "outAmount_raw": data.get("outAmount"),
            }
            print("QUOTE OK:", json.dumps(out, indent=2))
            return
        except Exception as e:
            last_err = e
            print(f"{label} FAIL:", e)

    # If we exhausted all endpoints
    print("All endpoints failed.")
    if last_err:
        print("Last error:", last_err)
    sys.exit(2)

if __name__ == "__main__":
    main()
