import asyncio, os
from utils_pkg import core_utils as cu

SOL = "So11111111111111111111111111111111111111112"

async def main():
    await cu.init_resources(require_wallet=False)
    print("session:", bool(cu.get_session()))
    print("rpc:", os.getenv("RPC_URL", ""))

    r = await cu.get_redis_async()
    print("redis:", type(r).__name__ if r else None)
    if r:
        await r.set("smoke:key", "1", ex=60)
        print("redis get:", await r.get("smoke:key"))

    # SQLite entry cache
    await cu.remember_entry_price("TESTMINT", 1.23, unit="usd")
    px = await cu.get_entry_price("TESTMINT")
    print("sqlite entry:", px)
    await cu.clear_entry_price("TESTMINT")

    # Pricing
    sol_usd = await cu.get_sol_usd()
    print("sol_usd:", sol_usd)

    # Jupiter quote (SOL->SOL just for sanity; you can swap output mint to a real token too)
    q = await cu.get_jupiter_quote(SOL, SOL, int(0.001 * 1e9), slippage_pct=0.005)
    print("quote type:", type(q).__name__, "keys:", list(q.keys())[:10] if isinstance(q, dict) else None)

    await cu.close_resources()
    print("closed OK")

asyncio.run(main())
