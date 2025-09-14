# scripts/entrypoint.py
from __future__ import annotations
import os, asyncio, requests

from utils_pkg.solana_wallet import load_keypair, get_balance_sol

BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY", "")
REFRESH_SEC = int(os.getenv("PF_TRADE_AUTOSIZE_REFRESH_SEC", "600"))
MIN_SOL = float(os.getenv("PF_TRADE_MIN_SOL", "0.005"))
RISK_PCT = float(os.getenv("PF_TRADE_RISK_PCT", "0.25"))  # optional, default 25%

SOL_MINT = "So11111111111111111111111111111111111111112"

def fetch_sol_usd() -> float:
    # 1) Birdeye (preferred – you already have a key)
    try:
        r = requests.get(
            "https://public-api.birdeye.so/defi/price",
            params={"address": SOL_MINT},
            headers={"x-chain": "solana", "X-API-KEY": BIRDEYE_API_KEY},
            timeout=6,
        )
        v = float(r.json()["data"]["value"])
        if v > 0:
            return v
    except Exception:
        pass
    # 2) Jupiter price fallback
    try:
        r = requests.get("https://price.jup.ag/v6/price?ids=SOL", timeout=6)
        return float(r.json()["data"]["SOL"]["price"])
    except Exception:
        return 0.0

async def autosize_once() -> float:
    kp = load_keypair()
    bal = await get_balance_sol(str(kp.pubkey()))
    sol_usd = fetch_sol_usd()
    # If price fails, size off SOL balance directly as a percentage
    if sol_usd <= 0:
        trade = max(MIN_SOL, round(bal * RISK_PCT, 9))
        equity = 0.0
    else:
        equity = bal * sol_usd
        trade = max(MIN_SOL, round((equity * RISK_PCT) / sol_usd, 9))
    os.environ["PF_TRADE_SOL"] = f"{trade:.9f}"
    print(f"[autosize] bal_sol={bal:.4f}, sol_usd≈{sol_usd:.2f}, equity≈{equity:.2f} -> PF_TRADE_SOL={trade}")
    return trade

async def autosize_loop():
    while True:
        await autosize_once()
        await asyncio.sleep(REFRESH_SEC)

async def run():
    # Set PF_TRADE_SOL once BEFORE importing scripts.main
    await autosize_once()
    from scripts.main import main as run_main  # <- import AFTER first autosize
    await asyncio.gather(
        autosize_loop(),
        run_main(),
    )

if __name__ == "__main__":
    asyncio.run(run())
