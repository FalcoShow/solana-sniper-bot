# scripts/test_quote.py
from __future__ import annotations
import os, json, requests

SOL_MINT  = os.getenv("SOL_MINT",  "So11111111111111111111111111111111111111112")
USDC_MINT = os.getenv("USDC_MINT", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")

SLIPPAGE_BPS = int(float(os.getenv("PF_SLIPPAGE_BPS", "120")))
ONLY_DIRECT  = os.getenv("JUP_ONLY_DIRECT", "0")
AS_LEGACY    = os.getenv("JUP_AS_LEGACY", "0")

PRO   = (os.getenv("JUP_UPSTREAM") or "https://api.jup.ag").rstrip("/")
LITE  = (os.getenv("JUP_LITE")     or "https://lite-api.jup.ag").rstrip("/")
V6    = (os.getenv("JUP_LEGACY")   or "https://quote-api.jup.ag").rstrip("/")

def try_quote(base: str) -> dict:
    # pick path depending on base
    if "api.jup.ag" in base or "lite-api.jup.ag" in base:
        url = f"{base}/swap/v1/quote"
        params = {
            "inputMint": SOL_MINT,
            "outputMint": USDC_MINT,
            "amount": str(int(0.05 * 1_000_000_000)),
            "slippageBps": str(SLIPPAGE_BPS),
            "onlyDirectRoutes": ONLY_DIRECT,
            "asLegacyTransaction": AS_LEGACY,
        }
    else:
        # legacy v6
        url = f"{base}/v6/quote"
        params = {
            "inputMint": SOL_MINT,
            "outputMint": USDC_MINT,
            "amount": str(int(0.05 * 1_000_000_000)),
            "slippageBps": str(SLIPPAGE_BPS),
            "onlyDirectRoutes": ONLY_DIRECT,
            "asLegacyTransaction": AS_LEGACY,
        }

    headers = {"Accept": "application/json"}
    # If you have a Pro key, add it (or set JUP_PRO_KEY in env)
    if os.getenv("JUP_PRO_KEY"):
        headers["x-api-key"] = os.getenv("JUP_PRO_KEY")

    r = requests.get(url, params=params, headers=headers, timeout=12)
    r.raise_for_status()
    return r.json()

def main():
    for name, base in [("PRO", PRO), ("LITE", LITE), ("LEGACY", V6)]:
        try:
            data = try_quote(base)
            out = {
                "endpoint": name,
                "base": base,
                "routePlan_len": len(data.get("routePlan", [])) if isinstance(data.get("routePlan"), list) else "n/a",
                "outAmount_raw": data.get("outAmount"),
            }
            print("QUOTE OK:", json.dumps(out, indent=2))
            return
        except Exception as e:
            print(f"{name} FAIL: {repr(e)}")
    raise SystemExit(2)

if __name__ == "__main__":
    main()
