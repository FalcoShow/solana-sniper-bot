# scripts/pg_migrate_trades.py
"""
Postgres migration for the `trades` table.

Usage:
  # auto-select (internal then external fallback via core_utils)
  python scripts/pg_migrate_trades.py

  # force external (Render) DSN from env
  python scripts/pg_migrate_trades.py --external

  # force internal DSN from env
  python scripts/pg_migrate_trades.py --internal
"""

from __future__ import annotations

import os
import sys
import argparse
import asyncio
from typing import List

# Allow running as a standalone file OR -m scripts.pg_migrate_trades
if __name__ == "__main__" and __package__ is None:
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from sqlalchemy import text  # type: ignore
from utils_pkg.core_utils import init_resources, close_resources, get_pg_engine


DDL_CREATE_TRADES = """
CREATE TABLE IF NOT EXISTS trades (
    id BIGSERIAL PRIMARY KEY,
    ts TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    token VARCHAR(64) NOT NULL,
    symbol VARCHAR(32),
    side VARCHAR(8)
);
"""

# Columns you’ve been writing in recent runs (added if missing)
DDL_ALTERS: List[str] = [
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS amount DOUBLE PRECISION",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS entry DOUBLE PRECISION",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS current DOUBLE PRECISION",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS profit DOUBLE PRECISION",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS reason VARCHAR(64)",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS signature VARCHAR(128)",
]

# Helpful indexes (no-ops if already present)
DDL_INDEXES: List[str] = [
    "CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades (ts)",
    "CREATE INDEX IF NOT EXISTS idx_trades_token ON trades (token)",
    "CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades (symbol)",
    "CREATE INDEX IF NOT EXISTS idx_trades_side ON trades (side)",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Migrate/patch the `trades` table schema.")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--external", action="store_true", help="Force PG_DSN to PG_DSN_EXTERNAL")
    g.add_argument("--internal", action="store_true", help="Force PG_DSN to PG_DSN_INTERNAL")
    return p.parse_args()


def maybe_force_dsn(args: argparse.Namespace) -> None:
    """
    Optionally force DSN selection by setting PG_DSN prior to init_resources().
    This keeps behavior consistent with your healthcheck.
    """
    if args.external:
        ext = os.getenv("PG_DSN_EXTERNAL", "")
        if ext:
            os.environ["PG_DSN"] = ext
            print("Using DSN [FORCED EXTERNAL] via PG_DSN_EXTERNAL")
        else:
            print("⚠️  --external passed, but PG_DSN_EXTERNAL is not set.")
    elif args.internal:
        intr = os.getenv("PG_DSN_INTERNAL", "")
        if intr:
            os.environ["PG_DSN"] = intr
            print("Using DSN [FORCED INTERNAL] via PG_DSN_INTERNAL")
        else:
            print("⚠️  --internal passed, but PG_DSN_INTERNAL is not set.")


async def migrate() -> None:
    # Bring up PG/Redis/Solana/etc. Core utils will select DSN (or use the forced one above)
    await init_resources()

    eng = get_pg_engine()
    if eng is None:
        print("❌ Postgres engine is not available. Check your .env PG_* variables and try again.")
        await close_resources()
        return

    async with eng.begin() as conn:
        # Create base table if not exists
        try:
            await conn.execute(text(DDL_CREATE_TRADES))
            print("✔ CREATE TABLE IF NOT EXISTS trades")
        except Exception as e:
            print(f"⚠ Failed creating base table: {e}")

        # Add/ensure columns
        for ddl in DDL_ALTERS:
            try:
                await conn.execute(text(ddl))
                print(f"✔ {ddl} ...")
            except Exception as e:
                print(f"⚠ Failed: {ddl}\n   -> {e}")

        # Create indexes
        for ddl in DDL_INDEXES:
            try:
                await conn.execute(text(ddl))
                print(f"✔ {ddl} ...")
            except Exception as e:
                print(f"⚠ Failed: {ddl}\n   -> {e}")

    await close_resources()
    print("✅ Migration complete.")


def main() -> None:
    args = parse_args()
    maybe_force_dsn(args)
    asyncio.run(migrate())


if __name__ == "__main__":
    main()
