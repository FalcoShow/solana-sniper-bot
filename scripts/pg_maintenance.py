import asyncio
import argparse

from utils_pkg.core_utils import init_resources, close_resources, pg_prune_trades, PG_TRADES_RETENTION_DAYS

async def main():
    ap = argparse.ArgumentParser(description="Postgres maintenance tasks")
    ap.add_argument("--prune", action="store_true", help="Prune old trades based on retention days")
    ap.add_argument("--days", type=int, default=None, help="Override retention days")
    args = ap.parse_args()

    await init_resources()
    try:
        if args.prune:
            removed = await pg_prune_trades(args.days)
            print(f"Pruned {removed} rows (retention={args.days or PG_TRADES_RETENTION_DAYS} days)")
        else:
            print("Nothing to do. Use --prune to delete old trades.")
    finally:
        await close_resources()

if __name__ == "__main__":
    asyncio.run(main())