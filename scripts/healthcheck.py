# scripts/healthcheck.py  (TUNABLE: skip flags + detailed errors)
"""
Tunable healthcheck:
  - RPC failover selects a working HTTP RPC and sets RPC_URL
  - init_resources()
  - Redis ping (skip with HEALTHCHECK_SKIP_REDIS=1)
  - Solana blockhash (skip with HEALTHCHECK_SKIP_SOLANA=1)
  - Wallet balance (always on)
  - Optional price checks (HEALTHCHECK_INCLUDE_PRICE=1)
Exit 1 only if a *non-skipped* fatal probe fails.
"""
from __future__ import annotations
import os, sys, asyncio, time

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from utils_pkg.rpc_failover import apply_failover_to_env
from utils_pkg.core_utils import (
    init_resources, close_resources,
    get_redis, get_solana_client,
    get_wallet_pubkey, get_sol_balance,
    convert_sol_to_usd, get_token_price,
)

def _b(name: str, default: bool = False) -> bool:
    return (os.getenv(name, "1" if default else "0").strip().lower() in ("1","true","yes","y","on"))

async def main() -> None:
    include_price   = _b("HEALTHCHECK_INCLUDE_PRICE", False)
    skip_redis      = _b("HEALTHCHECK_SKIP_REDIS", False)
    skip_solana     = _b("HEALTHCHECK_SKIP_SOLANA", False)

    fatals = []
    nonfatals = []

    # RPC failover
    try:
        chosen, notes = await apply_failover_to_env(timeout=4.0)
        print("🔁 RPC failover results:")
        for url, ok, why in notes:
            mark = "✓" if ok else "×"
            print(f"   {mark} {url}  —  {why}")
        print(f"➡ Using RPC_URL = {chosen}")
    except Exception as e:
        print(f"❌ RPC failover failed: {type(e).__name__}: {e}")
        sys.exit(1)

    # init
    try:
        await init_resources()
        print("✅ init_resources OK")
    except Exception as e:
        print(f"❌ init_resources failed: {type(e).__name__}: {e}")
        sys.exit(1)

    try:
        # Redis
        if skip_redis:
            print("ℹ️  Skipping Redis (HEALTHCHECK_SKIP_REDIS=1)")
        else:
            try:
                r = get_redis()
                if not r:
                    raise RuntimeError("redis client not initialized")
                key = "hc:ping"; ts = str(time.time())
                await r.set(key, ts, ex=30)
                got = await r.get(key)
                if not got:
                    raise RuntimeError("redis get failed")
                print("✅ Redis ping OK")
            except Exception as e:
                fatals.append(f"Redis: {type(e).__name__}: {e}")

        # Solana
        if skip_solana:
            print("ℹ️  Skipping Solana RPC (HEALTHCHECK_SKIP_SOLANA=1)")
        else:
            try:
                client = get_solana_client()
                if not client:
                    raise RuntimeError("solana client not initialized")
                res = await client.get_latest_blockhash()
                bh = getattr(getattr(res, "value", None), "blockhash", None)
                if not bh:
                    try:
                        bh = res['result']['value']['blockhash']
                    except Exception:
                        pass
                if not bh:
                    raise RuntimeError("failed to fetch latest blockhash")
                print("✅ Solana RPC ping OK")
            except Exception as e:
                fatals.append(f"Solana RPC: {type(e).__name__}: {e}")

        # Wallet
        try:
            pub = get_wallet_pubkey()
            if not pub:
                raise RuntimeError("wallet pubkey not available")
            bal = await get_sol_balance(pub)
            print(f"✅ Wallet OK: {pub} | SOL balance: {bal:.4f}")
        except Exception as e:
            fatals.append(f"Wallet: {type(e).__name__}: {e}")

        # Prices (optional)
        if include_price:
            try:
                usd = await convert_sol_to_usd(1.0)
                if not usd or usd <= 0:
                    raise RuntimeError("convert_sol_to_usd failed")
                print(f"✅ Price infra OK (1 SOL ≈ {usd:.4f} USD)")
            except Exception as e:
                nonfatals.append(f"Price(USD): {type(e).__name__}: {e}")
            try:
                WSOL = "So11111111111111111111111111111111111111112"
                p = await get_token_price(WSOL)
                if p is None or p <= 0:
                    raise RuntimeError("token_price failed")
                print(f"✅ Price Birdeye OK (wSOL): {p}")
            except Exception as e:
                nonfatals.append(f"Price(wSOL): {type(e).__name__}: {e}")
        else:
            print("ℹ️  Skipping price checks (HEALTHCHECK_INCLUDE_PRICE=0)")

    finally:
        try:
            await close_resources()
            print("✅ close_resources OK")
        except Exception:
            pass

    if nonfatals:
        print("⚠️  Non-fatal issues:"); [print("   - "+m) for m in nonfatals]
    if fatals:
        print("❌ Fatal issues:"); [print("   - "+m) for m in fatals]; sys.exit(1)
    print("✅ Healthcheck OK")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except SystemExit:
        raise
    except Exception as e:
        print(f"❌ Healthcheck failed: {type(e).__name__}: {e}")
        sys.exit(1)
