# scripts/test_quote.py
from __future__ import annotations
import os, json, requests

SOL_MINT  = os.getenv("SOL_MINT",  "So11111111111111111111111111111111111111112")
USDC_MINT = os.getenv("USDC_MINT", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")

SLIPPAGE_BPS = int(float(os.getenv("PF_SLIPPAGE_BPS", "120")))
ONLY_DIRECT  = os.getenv("JUP_ONLY_DIRECT", "0") in ("1","true","True")  # boolean
AS_LEGACY    = os.getenv("JUP_AS_LEGACY", "0") in ("1","true","True")    # boolean

PRO   = (os.getenv("JUP_UPSTREAM") or "https://api.jup.ag").rstrip("/")
LITE  = (os.getenv("JUP_LITE")     or "https://lite-api.jup.ag").rstrip("/")
V6    = (os.getenv("JUP_LEGACY")   or "https://quote-api.jup.ag").rstrip("/")

def try_quote_v1(base: str) -> dict:
    url = f"{base}/swap/v1/quote"
    params = {
        "inputMint": SOL_MINT,
        "outputMint": USDC_MINT,
        "amount": str(int(0.05 * 1_000_000_000)),  # 0.05 SOL in lamports
        "slippageBps": SLIPPAGE_BPS,
        "swapMode": "ExactIn",                     # <- REQUIRED in v1
        "onlyDirectRoutes": ONLY_DIRECT,
        "asLegacyTransaction": AS_LEGACY,
    }
    headers = {"Accept": "application/json"}
    if os.getenv("JUP_PRO_KEY"):
        headers["x-api-key"] = os.getenv("JUP_PRO_KEY")
    r = requests.get(url, params=params, headers=headers, timeout=12)
    if r.status_code >= 400:
        raise RuntimeError(f"{r.status_code} {r.text[:400]}")
    return r.json()

def try_quote_v6(base: str) -> dict:
    url = f"{base}/v6/quote"
    params = {
        "inputMint": SOL_MINT,
        "outputMint": USDC_MINT,
        "amount": str(int(0.05 * 1_000_000_000)),
        "slippageBps": SLIPPAGE_BPS,
        "onlyDirectRoutes": int(ONLY_DIRECT),
        "asLegacyTransaction": int(AS_LEGACY),
    }
    r = requests.get(url, params=params, timeout=12)
    if r.status_code >= 400:
        raise RuntimeError(f"{r.status_code} {r.text[:400]}")
    return r.json()

def main():
    # PRO -> LITE -> LEGACY
    for name, fn in [("PRO", try_quote_v1), ("LITE", try_quote_v1), ("LEGACY", try_quote_v6)]:
        base = PRO if name=="PRO" else (LITE if name=="LITE" else V6)
        try:
            data = fn(base)
            out = {
                "endpoint": name,
                "base": base,
                "routePlan_len": len(data.get("routePlan", [])) if isinstance(data.get("routePlan"), list) else "n/a",
                "outAmount_raw": data.get("outAmount"),
            }
            print("QUOTE OK:", json.dumps(out, indent=2))
            return
        except Exception as e:
            print(f"{name} FAIL:", e)
    raise SystemExit(2)

if __name__ == "__main__":
    main()
