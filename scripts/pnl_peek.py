#!/usr/bin/env python3
import sys, asyncio
from pathlib import Path
from datetime import datetime, timezone

# Ensure repo root is importable
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

import utils_pkg.core_utils as cu  # <-- import module, not symbols

async def main():
    await cu.init_resources()
    try:
        # safety: make sure redis got initialized
        if cu.redis is None:
            print("Redis client is not initialized. Check REDIS_URL / init_resources().")
            return

        today_key = datetime.now(timezone.utc).strftime("%Y%m%d")
        key = f"pnl:day:{today_key}:realized_usd"
        val = await cu.redis.get(key)
        if isinstance(val, bytes):
            val = val.decode()
        print(f"{key} = {val}")
    finally:
        await cu.close_resources()

if __name__ == "__main__":
    asyncio.run(main())
