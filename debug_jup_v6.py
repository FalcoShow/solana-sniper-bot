# debug_jup_v6.py
import os, json, requests
from utils_pkg.solana_wallet import load_keypair, get_pubkey_str

BASE = os.getenv("JUP_UPSTREAM", "https://public.jupiterapi.com").rstrip("/")
AS_LEGACY = (os.getenv("JUP_AS_LEGACY", "false").strip().lower() in ("1","true","yes","on"))
ONLY_DIRECT = (os.getenv("JUP_ONLY_DIRECT", "false").strip().lower() in ("1","true","yes","on"))

INPUT  = "So11111111111111111111111111111111111111112"   # SOL
OUTPUT = os.getenv("TEST_MINT", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")  # USDC by default
AMOUNT_LAMPORTS = int(float(os.getenv("TEST_SOL", "0.004")) * 1_000_000_000)      # e.g. 0.004 SOL
SLIPPAGE_BPS = int(os.getenv("TEST_SLIPPAGE_BPS", "30"))

def jup_headers():
    h = {
        "accept": "application/json",
        "content-type": "application/json",
        "origin": "https://jup.ag",
        "referer": "https://jup.ag/",
        "user-agent": "sniper-debug/1.0",
    }
    k = (os.getenv("JUP_API_KEY") or os.getenv("JUP_PRO_KEY") or "").strip()
    if k: h["x-api-key"] = k
    b = (os.getenv("JUP_BEARER") or "").strip()
    if b: h["x-jup-auth"] = f"Bearer {b}"
    return h

def main():
    print("BASE:", BASE, "AS_LEGACY:", AS_LEGACY, "ONLY_DIRECT:", ONLY_DIRECT)
    print("AMOUNT_LAMPORTS:", AMOUNT_LAMPORTS, "SLIPPAGE_BPS:", SLIPPAGE_BPS)

    # get a real user pubkey
    kp = load_keypair()
    user_pubkey = get_pubkey_str(kp)
    print("USER_PUBKEY:", user_pubkey)

    # 1) QUOTE
    quote_url = f"{BASE}/quote" if not AS_LEGACY else f"{BASE}/swap/v1/quote"
    qparams = {
        "inputMint": INPUT,
        "outputMint": OUTPUT,
        "amount": str(AMOUNT_LAMPORTS),
        "slippageBps": SLIPPAGE_BPS,
        "onlyDirectRoutes": str(ONLY_DIRECT).lower(),
        "asLegacyTransaction": str(AS_LEGACY).lower(),
        "swapMode": "ExactIn",
        # NOTE: if your impl adds platformFeeBps, try removing it here to isolate errors.
        # "platformFeeBps": 0,
    }
    r = requests.get(quote_url, params=qparams, headers=jup_headers(), timeout=20)
    print("QUOTE STATUS:", r.status_code)
    try:
        quote = r.json()
    except Exception:
        print("QUOTE TEXT:", r.text[:2000])
        r.raise_for_status()
        return
    print("QUOTE KEYS:", list(quote.keys())[:12])
    print("QUOTE PREVIEW:", json.dumps({k: quote.get(k) for k in ("inAmount","outAmount","routePlan","routes","swapMode","otherAmountThreshold","priceImpactPct")}, default=str, indent=2)[:1500])

    if r.status_code != 200:
        print("QUOTE FULL:", json.dumps(quote, indent=2)[:4000])
        return

    # 2) SWAP
    swap_url = f"{BASE}/swap" if not AS_LEGACY else f"{BASE}/swap/v1/swap"
    body = {
        "userPublicKey": user_pubkey,
        "wrapAndUnwrapSol": True,
        "dynamicComputeUnitLimit": True,
        "quoteResponse": quote,
        "asLegacyTransaction": AS_LEGACY,
        # Comment these to isolate issues; add back once basic swap passes:
        # "prioritizationFeeLamports": None,
        # "computeUnitLimit": None,
        # If you used platformFeeBps during /quote and your relay expects fee accounts, you may need to pass platformFee here.
        # "platformFee": {"amount": 0, "feeBps": 0, "platform": "your-tag"}
    }
    r2 = requests.post(swap_url, json=body, headers=jup_headers(), timeout=25)
    print("SWAP STATUS:", r2.status_code)
    try:
        js2 = r2.json()
    except Exception:
        print("SWAP TEXT:", r2.text[:4000])
        r2.raise_for_status()
        return

    if r2.status_code != 200:
        print("SWAP ERROR JSON:", json.dumps(js2, indent=2)[:4000])
    else:
        print("SWAP RESPONSE KEYS:", list(js2.keys()))
        print("HAS swapTransaction?", "swapTransaction" in js2)

if __name__ == "__main__":
    main()
