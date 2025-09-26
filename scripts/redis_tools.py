# scripts/redis_tools.py
import argparse
import asyncio
from datetime import date, datetime
import os
import sys

# Make repo root importable when running as a script
ROOT = os.path.dirname(os.path.abspath(os.path.join(__file__, os.pardir)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

# Shared infra (may or may not expose a bound `redis`)
from utils_pkg.core_utils import init_resources, close_resources  # type: ignore

_aioredis = None         # fallback client we create if needed
_coreutils_redis = None  # reference to core_utils' redis if present

HALT_KEYS = ["halt:infra", "halt:manual", "halt:daily_loss"]

def _datestr(s: str | None) -> str:
    if not s:
        return date.today().strftime("%Y%m%d")
    s = s.strip()
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y%m%d")
        except ValueError:
            pass
    raise SystemExit("Bad --date format. Use YYYY-MM-DD or YYYYMMDD")

def _decode(v):
    if v is None:
        return None
    if isinstance(v, (bytes, bytearray)):
        return v.decode()
    return v

async def _get_redis():
    """
    Return a usable redis client:
    1) try the one exported by utils_pkg.core_utils (after init_resources)
    2) otherwise create our own from REDIS_URL
    """
    global _aioredis, _coreutils_redis
    if _coreutils_redis is not None:
        return _coreutils_redis

    # Try import the symbol after init_resources has run
    try:
        from utils_pkg.core_utils import redis as cu_redis  # type: ignore
        if cu_redis is not None:
            _coreutils_redis = cu_redis
            return _coreutils_redis
    except Exception:
        pass

    # Fallback: build our own client from env
    if _aioredis is None:
        url = os.getenv("REDIS_URL")
        if not url:
            raise SystemExit("REDIS_URL not set; cannot connect to Redis.")
        try:
            import redis.asyncio as redis_async
        except Exception as e:
            raise SystemExit(f"redis.asyncio not available. pip install redis>=4.2.0. Error: {e}")

        # decode_responses=True -> str in/out (works with rediss:// as well)
        _aioredis = redis_async.from_url(url, decode_responses=True)
        try:
            await _aioredis.ping()
        except Exception as e:
            raise SystemExit(f"Failed to connect to Redis at {url}: {e}")

    return _aioredis

async def show_pnl(day: str | None):
    r = await _get_redis()
    ds = _datestr(day)
    key = f"pnl:day:{ds}:realized_usd"
    val = await r.get(key)
    print(f"{key} = {_decode(val)}")

async def show_halts():
    r = await _get_redis()
    found = {}
    for k in HALT_KEYS:
        v = await r.get(k)
        dv = _decode(v)
        if dv is not None:
            found[k] = dv
    print("Halts:", found or "(none)")

async def clear_halts():
    r = await _get_redis()
    deleted = 0
    for k in HALT_KEYS:
        try:
            deleted += await r.delete(k)
        except Exception:
            pass
    print(f"Cleared {deleted} halt key(s).")

async def main():
    p = argparse.ArgumentParser(description="Redis helper for PnL and halt flags.")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("show-pnl", help="Show realized PnL for a day")
    sp.add_argument("--date", help="YYYY-MM-DD or YYYYMMDD (default: today)")

    sub.add_parser("show-halts", help="Show active halt flags")
    sub.add_parser("clear-halts", help="Clear infra/manual/daily_loss halt flags")

    args = p.parse_args()

    # Bring up shared infra (RPC/HTTP/Redis etc.)
    await init_resources()
    try:
        if args.cmd == "show-pnl":
            await show_pnl(getattr(args, "date", None))
        elif args.cmd == "show-halts":
            await show_halts()
        elif args.cmd == "clear-halts":
            await clear_halts()
    finally:
        # Close shared infra
        await close_resources()
        # If we created our own fallback redis client, close it too
        global _aioredis
        if _aioredis is not None:
            try:
                await _aioredis.close()
            except Exception:
                pass

if __name__ == "__main__":
    asyncio.run(main())
