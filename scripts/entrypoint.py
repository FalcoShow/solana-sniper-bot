# scripts/entrypoint.py
import os, asyncio, requests

from utils_pkg.solana_wallet import load_keypair, get_balance_sol
from scripts.main import main as run_main  # ensure scripts/main.py exposes async def main()

WSOL_MINT = "So11111111111111111111111111111111111111112"

def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default

def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "on")

def get_sol_usd_blocking() -> float | None:
    """Try Birdeye (needs API key), then Jupiter price API."""
    be_key = os.getenv("BIRDEYE_API_KEY")
    try:
        if be_key:
            r = requests.get(
                "https://public-api.birdeye.so/defi/price",
                params={"chain": "solana", "address": WSOL_MINT},
                headers={"X-API-KEY": be_key, "accept": "application/json"},
                timeout=6,
            )
            if r.ok:
                j = r.json() or {}
                data = j.get("data") or {}
                # Birdeye returns one of these depending on version
                p = data.get("value") or data.get("price") or data.get("priceUsd")
                if p is not None:
                    return float(p)
    except Exception:
        pass

    # Jupiter fallbacks (no key)
    for url in (
        "https://price.jup.ag/v6/price?ids=SOL",
        "https://price.jup.ag/v4/price?ids=SOL",
    ):
        try:
            j = requests.get(url, timeout=6).json()
            p = j.get("data", {}).get("SOL", {}).get("price")
            if p is not None:
                return float(p)
        except Exception:
            continue

    return None

async def autosize_once() -> float:
    kp = load_keypair()
    bal_sol = await get_balance_sol(kp.pubkey())
    sol_usd = await asyncio.to_thread(get_sol_usd_blocking)

    pct            = env_float("POSITION_PCT_OF_EQUITY", 0.05)   # 5% default
    cap_usd        = env_float("TICKET_USD_CAP", 250.0)
    min_keep_sol   = env_float("MIN_SOL_BALANCE", 0.20)
    min_trade_sol  = env_float("PF_TRADE_MIN_SOL", 0.005)
    equity_floor   = env_float("EQUITY_USD_FALLBACK", 2000.0)

    # equity_usd = max(floor, balance * price)
    if sol_usd is None:
        equity_usd = equity_floor
    else:
        equity_usd = max(equity_floor, bal_sol * sol_usd)

    ticket_usd = min(cap_usd, equity_usd * pct)

    if sol_usd:
        trade_sol = ticket_usd / sol_usd
    else:
        trade_sol = min_trade_sol  # conservative if price missing

    # don’t dip below your SOL reserve for fees/health
    spendable_sol = max(0.0, bal_sol - min_keep_sol)
    trade_sol = max(0.0, min(trade_sol, spendable_sol))
    trade_sol = max(trade_sol, 0.0)

    # round to avoid dust; export so downstream code sees it
    trade_sol = float(f"{trade_sol:.6f}")
    os.environ["PF_TRADE_SOL"] = str(trade_sol)
    print(f"[autosize] bal_sol={bal_sol:.4f}, sol_usd≈{(sol_usd or 0):.2f}, "
          f"equity≈{equity_usd:.2f} -> PF_TRADE_SOL={trade_sol}")
    return trade_sol

async def autosize_loop():
    refresh = int(os.getenv("PF_TRADE_AUTOSIZE_REFRESH_SEC", "600"))
    while True:
        try:
            await autosize_once()
        except Exception as e:
            print(f"[autosize] error: {e}")
        await asyncio.sleep(refresh)

async def boot():
    if env_bool("PF_TRADE_AUTOSIZE", True):
        # set immediately and then refresh in background
        await autosize_once()
        asyncio.create_task(autosize_loop())
    await run_main()

if __name__ == "__main__":
    asyncio.run(boot())
