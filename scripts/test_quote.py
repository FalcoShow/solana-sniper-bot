# scripts/test_quote.py
from __future__ import annotations
import os, sys, json, time
import requests

SOL_MINT = os.getenv("SOL_MINT", "So11111111111111111111111111111111111111112")
USDC_MINT = os.getenv("USDC_MINT", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")
JUP_BASE = (os.getenv("JUP_BASE_URL") or "").strip()
JUP_UPSTREAM = (os.getenv("JUP_UPSTREAM") or "https://quote-api.jup.ag").strip()
BASE = JUP_BASE or JUP_UPSTREAM

def get(url, params=None, ip_override=None):
    headers = {"Accept": "application/json"}
    # If you want to force Host header to bypass DNS, set JUP_HOST
    host = os.getenv("JUP_HOST", "quote-api.jup.ag")
    if ip_override:
        # Talk to IP but set Host header for SNI/Cloudflare
        url = f"https://{ip_override}{requests.utils.urlparse(url).path}"
        headers["Host"] = host
    r = requests.get(url, params=params, headers=headers, timeout=10)
    r.raise_for_status()
    return r.json()

def main():
    # 0. ping root
    try:
        print(f"PING {BASE} ...")
        r = requests.get(BASE, timeout=10)
        print("PING OK:", r.status_code)
    except Exception as e:
        print("PING FAIL:", repr(e))

    # 1. build a simple quote: 0.05 SOL -> USDC
    amount_lamports = int(0.05 * 1_000_000_000)
    params = {
        "inputMint": SOL_MINT,
        "outputMint": USDC_MINT,
        "amount": str(amount_lamports),
        "slippageBps": str(int(float(os.getenv("PF_SLIPPAGE_BPS", "120")))),
        "onlyDirectRoutes": os.getenv("JUP_ONLY_DIRECT", "0"),
        "asLegacyTransaction": os.getenv("JUP_AS_LEGACY", "0"),
    }

    url = f"{BASE}/v6/quote"
    try:
        data = get(url, params=params)
        print("QUOTE OK:", json.dumps({
            "in": "0.05 SOL",
            "out_usdc": data.get("outAmount"),
            "routes": len(data.get("routePlan", [])) if isinstance(data.get("routePlan"), list) else "n/a"
        }, indent=2))
    except Exception as e:
        print("QUOTE via DNS FAIL:", repr(e))
        # Try IP override(s) if provided
        ips = [ip.strip() for ip in os.getenv("JUP_RESOLVE_IPS", "").split(",") if ip.strip()]
        for ip in ips:
            try:
                print(f"Trying IP override {ip} ...")
                d = get(url, params=params, ip_override=ip)
                print("QUOTE via IP OK:", json.dumps({
                    "ip": ip,
                    "out_usdc": d.get("outAmount"),
                    "routes": len(d.get("routePlan", [])) if isinstance(d.get("routePlan"), list) else "n/a"
                }, indent=2))
                break
            except Exception as e2:
                print(f"IP {ip} FAIL:", repr(e2))
        else:
            sys.exit(2)

if __name__ == "__main__":
    main()
