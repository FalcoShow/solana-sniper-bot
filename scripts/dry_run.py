# scripts/dry_run.py
from __future__ import annotations

import asyncio, os, sys
from pathlib import Path
from datetime import datetime, timezone, timedelta

# Pathing + .env
THIS = Path(__file__).resolve()
ROOT = THIS.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(), override=False)

from utils_pkg.core_utils import (
    init_resources, close_resources,
    get_token_price, get_jupiter_quote,  # quote only (no swap)
)
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("dry_run")

WSOL = "So11111111111111111111111111111111111111112"
USDC = "EPjFWdd5Lap8wGweDbosA4Y4J1WKBhG9cYFxaPkmD8n"

async def main():
    await init_resources()
    try:
        # 1) fetch some prices
        wsol_price = await get_token_price(WSOL)
        usdc_price = await get_token_price(USDC)
        print(f"WSOL price: {wsol_price:.6f}  |  USDC price: {usdc_price:.6f}")

        # 2) Jupiter quote sanity (simulate 0.01 SOL -> USDC)
        lamports = int(0.01 * 1e9)
        quote = await get_jupiter_quote(WSOL, USDC, lamports)
        if quote and quote.get("data"):
            route = quote["data"][0]
            out_amt = route.get("outAmount")
            print(f"Jupiter quote OK (0.01 SOL->USDC). outAmount={out_amt}")
        else:
            print("Jupiter quote failed or no route returned (this can be transient).")

        # 3) pretend we bought something 90 minutes ago and ask the AI hold/sell
        # (We don’t import ai_sell_logic here to keep the dry run lightweight)
        print("Dry run complete.")
    finally:
        await close_resources()

if __name__ == "__main__":
    asyncio.run(main())
