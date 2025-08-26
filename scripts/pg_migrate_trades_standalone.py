# scripts/pg_migrate_trades_standalone.py
import os
import argparse
import asyncio
from typing import Optional, Tuple

from dotenv import load_dotenv, find_dotenv
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.engine.url import make_url
from sqlalchemy import text


def _valid_dsn(s: Optional[str]) -> bool:
    if not s or not isinstance(s, str):
        return False
    try:
        make_url(s.strip())
        return True
    except Exception:
        return False


def _pick_dsn(
    dsn_override: Optional[str] = None,
    force_external: bool = False,
    force_internal: bool = False,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (label, dsn)
    label in {"OVERRIDE","EXTERNAL","PG_DSN","INTERNAL"} or None
    """
    load_dotenv(find_dotenv())

    if _valid_dsn(dsn_override):
        return "OVERRIDE", dsn_override.strip()

    pg_dsn = os.getenv("PG_DSN", "").strip()
    dsn_i  = os.getenv("PG_DSN_INTERNAL", "").strip()
    dsn_e  = os.getenv("PG_DSN_EXTERNAL", "").strip()

    if force_external and _valid_dsn(dsn_e):
        return "EXTERNAL", dsn_e
    if force_internal and _valid_dsn(dsn_i):
        return "INTERNAL", dsn_i

    # Default preference: external → PG_DSN → internal
    if _valid_dsn(dsn_e):
        return "EXTERNAL", dsn_e
    if _valid_dsn(pg_dsn):
        return "PG_DSN", pg_dsn
    if _valid_dsn(dsn_i):
        return "INTERNAL", dsn_i

    return None, None


MIGRATIONS = [
    # Base table (idempotent)
    """
    CREATE TABLE IF NOT EXISTS trades (
        id BIGSERIAL PRIMARY KEY,
        ts TIMESTAMP NOT NULL,
        token VARCHAR(64) NOT NULL,
        symbol VARCHAR(32),
        side VARCHAR(8),
        amount DOUBLE PRECISION,
        entry DOUBLE PRECISION,
        current DOUBLE PRECISION,
        profit DOUBLE PRECISION,
        reason VARCHAR(64),
        signature VARCHAR(128)
    )
    """,
    # Add columns defensively (your data showed these are used)
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS amount DOUBLE PRECISION",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS entry DOUBLE PRECISION",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS current DOUBLE PRECISION",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS profit DOUBLE PRECISION",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS reason VARCHAR(64)",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS signature VARCHAR(128)",
    # Helpful indexes
    "CREATE INDEX IF NOT EXISTS idx_trades_ts     ON trades (ts)",
    "CREATE INDEX IF NOT EXISTS idx_trades_token  ON trades (token)",
    "CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades (symbol)",
    "CREATE INDEX IF NOT EXISTS idx_trades_side   ON trades (side)",
]


async def _run_migrations(dsn: str, echo: bool = False) -> None:
    eng = create_async_engine(dsn, echo=echo, pool_pre_ping=True)
    try:
        async with eng.begin() as conn:
            for sql in MIGRATIONS:
                head = sql.strip().splitlines()[0][:80]
                try:
                    await conn.execute(text(sql))
                    print(f"✔ {head} …")
                except Exception as e:
                    print(f"⚠ Migration failed at: {head} -> {e}")
    finally:
        await eng.dispose()
    print("✅ Migration complete.")


def main():
    ap = argparse.ArgumentParser(description="Idempotent migrations for the trades table.")
    ap.add_argument("--external", action="store_true", help="Force use of PG_DSN_EXTERNAL")
    ap.add_argument("--internal", action="store_true", help="Force use of PG_DSN_INTERNAL")
    ap.add_argument("--dsn",      type=str, default=None, help="Explicit SQLAlchemy DSN override")
    ap.add_argument("--echo",     action="store_true", help="SQLAlchemy echo")
    args = ap.parse_args()

    label, dsn = _pick_dsn(args.dsn, args.external, args.internal)
    if not dsn:
        print("❌ No valid PG DSN found. Set one of PG_DSN / PG_DSN_EXTERNAL / PG_DSN_INTERNAL, "
              "or pass --dsn.")
        return

    print(f"Using DSN [{label}]: {dsn}")
    asyncio.run(_run_migrations(dsn, echo=args.echo))


if __name__ == "__main__":
    main()
