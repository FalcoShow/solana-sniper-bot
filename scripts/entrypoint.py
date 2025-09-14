# scripts/entrypoint.py
from __future__ import annotations
import os, asyncio, requests

from utils_pkg.solana_wallet import load_keypair, get_balance_sol

import math, os, asyncio
import httpx
from utils_pkg.solana_wallet import get_balance_sol, load_keypair

JUP_PRICE_URL = os.getenv("PRICE_URL_JUP", "https://price.jup.ag/v4/price?ids=SOL")
COINGECKO_URL = os.getenv("PRICE_URL_CG", "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd")

BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY", "")
REFRESH_SEC = int(os.getenv("PF_TRADE_AUTOSIZE_REFRESH_SEC", "600"))
MIN_SOL = float(os.getenv("PF_TRADE_MIN_SOL", "0.005"))
RISK_PCT = float(os.getenv("PF_TRADE_RISK_PCT", "0.25"))  # optional, default 25%

SOL_MINT = "So11111111111111111111111111111111111111112"

async def get_sol_usd() -> float:
    # optional override if you want to force a price
    static = os.getenv("PRICE_STATIC_USD")
    if static:
        try:
            return float(static)
        except:
            pass
    timeout = httpx.Timeout(3.5, connect=2.0)
    async with httpx.AsyncClient(timeout=timeout) as xc:
        # Jupiter first
        try:
            r = await xc.get(JUP_PRICE_URL)
            if r.status_code == 200:
                js = r.json()
                p = js.get("data", {}).get("SOL", {}).get("price")
                if p and p > 0:
                    return float(p)
        except Exception:
            pass
        # CoinGecko fallback
        try:
            r = await xc.get(COINGECKO_URL, headers={"accept": "application/json"})
            if r.status_code == 200:
                js = r.json()
                p = js.get("solana", {}).get("usd")
                if p and p > 0:
                    return float(p)
        except Exception:
            pass
    return 0.0  # graceful fallback

def clamp_round_size(size: float) -> float:
    min_sol = float(os.getenv("PF_TRADE_MIN_SOL", "0.005"))
    max_sol = float(os.getenv("PF_TRADE_MAX_SOL", "0"))  # 0 = no cap
    step    = float(os.getenv("PF_TRADE_STEP_SOL", "0.0"))

    if max_sol > 0:
        size = min(size, max_sol)
    size = max(size, min_sol)
    if step > 0:
        size = math.floor(size / step) * step
    return max(size, min_sol)

async def autosize_loop():
    if os.getenv("PF_TRADE_AUTOSIZE", "0") != "1":
        return  # no-op

    kp = load_keypair()
    refresh = float(os.getenv("PF_TRADE_AUTOSIZE_REFRESH_SEC", "600"))
    risk_pct = float(os.getenv("PF_TRADE_RISK_PCT", "0.20"))

    while True:
        try:
            bal_sol = await get_balance_sol(kp.pubkey())
            sol_usd = await get_sol_usd()           # << will now be non-zero most of the time
            equity  = bal_sol * sol_usd

            # size off SOL balance * risk; USD price is only for logging/visibility
            raw_size = bal_sol * risk_pct
            size_sol = clamp_round_size(raw_size)

            os.environ["PF_TRADE_SOL"] = f"{size_sol:.9f}"
            print(f"[autosize] bal_sol={bal_sol:.4f}, sol_usd≈{sol_usd:.2f}, equity≈{equity:.2f} -> PF_TRADE_SOL={size_sol}")

        except Exception as e:
            print("[autosize] error:", e)

        await asyncio.sleep(refresh)

# ...and ensure your main() awaits autosize_loop() in a background task before starting the stream
# e.g.
# asyncio.create_task(autosize_loop())
# await run_stream()