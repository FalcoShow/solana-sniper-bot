# scripts/sell_once.py
# One-off sell driver (Jupiter v6 path via scripts/sell_adapter.execute_sell)
#
# Usage examples:
#   python -m scripts.sell_once --mint EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v --symbol USDC --amount 0.001 --slip 0.01
#   SELL_MINT=EPjFW... SELL_SYMBOL=USDC SELL_AMOUNT=0.001 SELL_SLIPPAGE=0.01 python -m scripts.sell_once
#
# Notes:
# - Tx signing/sending + trade logging are handled in sell_adapter.execute_sell().
# - This script validates inputs, loads env, and wraps init/close safely.

import os
import sys
import argparse
import asyncio
from typing import Optional

# Make repo root importable (Windows friendly)
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from dotenv import load_dotenv, find_dotenv  # type: ignore
load_dotenv(find_dotenv())

from utils_pkg.core_utils import init_resources, close_resources
from scripts.sell_adapter import execute_sell


def env_str(key: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(key)
    return v if v is not None and v != "" else default


def env_float(key: str, default: Optional[float] = None) -> Optional[float]:
    v = os.getenv(key)
    if v is None or v == "":
        return default
    try:
        return float(v)
    except ValueError:
        return default


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="sell_once.py",
        description="Execute a single sell via Jupiter v6 (uses scripts/sell_adapter.execute_sell).",
    )
    p.add_argument("--mint", "-m", type=str, default=env_str("SELL_MINT"),
                   help="Token mint to SELL (e.g., USDC mint). Falls back to SELL_MINT.")
    p.add_argument("--symbol", "-s", type=str, default=env_str("SELL_SYMBOL"),
                   help="Readable token symbol for logs (e.g., USDC). Falls back to SELL_SYMBOL.")
    p.add_argument("--amount", "-a", type=float, default=env_float("SELL_AMOUNT"),
                   help="Token amount to sell (float). Falls back to SELL_AMOUNT.")
    p.add_argument("--slip", "--slippage", type=float, default=env_float("SELL_SLIPPAGE", 0.01),
                   help="Slippage fraction (e.g., 0.01 for 1%). Falls back to SELL_SLIPPAGE (default 0.01).")
    p.add_argument("--reason", type=str, default=env_str("SELL_REASON", "sell_once"),
                   help="Reason string to include in logs/Notion/PG where supported.")
    p.add_argument("--dry-run", action="store_true", default=os.getenv("SELL_DRY_RUN", "false").lower() == "true",
                   help="If set, skip the actual swap (adapter may still quote).")
    p.add_argument("--retries", type=int, default=int(os.getenv("SELL_RETRIES", "0")),
                   help="Number of retry attempts on failure (default 0).")
    return p.parse_args()


def validate_inputs(mint: Optional[str], symbol: Optional[str], amount: Optional[float], slip: Optional[float]) -> None:
    if not mint or len(mint) < 32:  # full base58 mint is 32+ chars
        raise ValueError("Invalid --mint (full token mint address required).")
    if not symbol or len(symbol) < 2:
        raise ValueError("Invalid --symbol (readable token symbol required).")
    if amount is None or amount <= 0:
        raise ValueError("Invalid --amount (must be > 0).")
    if slip is None or slip <= 0 or slip > 0.5:
        raise ValueError("Invalid --slip/--slippage (0 < slip ≤ 0.5; try 0.005 for 0.5%).")


async def run_once(
    mint: str,
    symbol: str,
    amount: float,
    slippage: float,
    reason: str,
    dry_run: bool,
    retries: int,
) -> bool:
    """
    Wraps execute_sell() with simple retries. Returns True on success.
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            ok = await execute_sell(
                mint,
                symbol,
                amount,
                slippage=slippage,
                reason=reason,
                dry_run=dry_run,
            )
            if ok:
                return True
            if attempt > retries:
                return False
        except Exception as e:
            print(f"[sell_once] attempt {attempt} failed: {e}")
            if attempt > retries:
                return False
        await asyncio.sleep(min(2 * attempt, 10))


async def main():
    args = parse_args()
    validate_inputs(args.mint, args.symbol, args.amount, args.slip)

    # Show effective config
    print("Sell config:")
    print(f"  mint     : {args.mint}")
    print(f"  symbol   : {args.symbol}")
    print(f"  amount   : {args.amount}")
    print(f"  slippage : {args.slip}")
    print(f"  reason   : {args.reason}")
    print(f"  dry_run  : {args.dry_run}")
    print(f"  retries  : {args.retries}")

    await init_resources()
    try:
        ok = await run_once(
            mint=args.mint,
            symbol=args.symbol,
            amount=args.amount,
            slippage=args.slip,
            reason=args.reason,
            dry_run=args.dry_run,
            retries=args.retries,
        )
        print("sell ok?", ok)
    finally:
        await close_resources()


if __name__ == "__main__":
    asyncio.run(main())
