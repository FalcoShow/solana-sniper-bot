#!/usr/bin/env python3
from __future__ import annotations

"""
scripts/pg_migrate_trades_standalone.py
======================================

Idempotent Postgres migrations for the canonical `trades` table (P0 contract).

Goals
-----
- Single-source DSN behavior (Render + local) with asyncpg normalization.
- Safe, idempotent, *versioned* migrations (schema_migrations table).
- Concurrency-safe (advisory lock) so two deploys don’t race.
- Works even if utils_pkg.ai_db / core_utils shapes vary.

Typical use
-----------
  # Auto-pick DSN via ai_db/core_utils/env
  python -m scripts.pg_migrate_trades_standalone

  # Force external/internal env DSN
  python -m scripts.pg_migrate_trades_standalone --external
  python -m scripts.pg_migrate_trades_standalone --internal

  # Explicit DSN override
  python -m scripts.pg_migrate_trades_standalone --dsn "postgresql+asyncpg://..."

  # Print SQL without applying
  python -m scripts.pg_migrate_trades_standalone --dry-run

Notes
-----
- This script is intentionally *non-destructive*. No DROP/DELETE.
- Column widths are chosen to be forward-compatible (bigger is fine).
"""

import argparse
import contextlib
import hashlib
import logging
import os
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

# ──────────────────────────────────────────────────────────────
# Repo root / dotenv
# ──────────────────────────────────────────────────────────────

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

with contextlib.suppress(Exception):
    from dotenv import find_dotenv, load_dotenv  # type: ignore

    load_dotenv(find_dotenv(), override=False)


# ──────────────────────────────────────────────────────────────
# Logging (canonical if available)
# ──────────────────────────────────────────────────────────────


def _get_logger() -> logging.Logger:
    try:
        from logging_setup import get_logger  # type: ignore

        return get_logger()
    except Exception:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
        return logging.getLogger("pg_migrate_trades")


log = _get_logger()


# ──────────────────────────────────────────────────────────────
# DSN resolution / normalization
# ──────────────────────────────────────────────────────────────


def _normalize_asyncpg_dsn(dsn: str) -> str:
    """
    Ensure SQLAlchemy asyncpg driver prefix.

    Accepts:
      - postgres://...
      - postgresql://...
      - postgresql+asyncpg://...
    """
    dsn = (dsn or "").strip()
    if not dsn:
        return dsn

    if dsn.startswith("postgres://"):
        return "postgresql+asyncpg://" + dsn[len("postgres://") :]
    if dsn.startswith("postgresql://"):
        return "postgresql+asyncpg://" + dsn[len("postgresql://") :]
    return dsn


def _valid_dsn(dsn: str | None) -> bool:
    if not dsn or not isinstance(dsn, str):
        return False
    try:
        make_url(dsn.strip())
        return True
    except Exception:
        return False


def _env_pick(*names: str) -> str | None:
    for n in names:
        v = os.getenv(n)
        if v and str(v).strip():
            return str(v).strip()
    return None


def _resolve_dsn(
    *,
    dsn_override: str | None,
    force_external: bool,
    force_internal: bool,
) -> tuple[str, str]:
    """
    Returns (label, dsn). Raises on failure.
    Prefers ai_db resolver when present, else env contract.
    """
    if dsn_override and _valid_dsn(_normalize_asyncpg_dsn(dsn_override)):
        return "OVERRIDE", _normalize_asyncpg_dsn(dsn_override.strip())

    # Prefer ai_db resolver if available
    with contextlib.suppress(Exception):
        from utils_pkg.ai_db import resolve_postgres_dsn  # type: ignore

        # If forcing, express preference through args where supported; otherwise env will do.
        label, dsn = resolve_postgres_dsn(
            prefer_external=bool(force_external),
            prefer_internal=bool(force_internal),
        )
        dsn = _normalize_asyncpg_dsn(str(dsn or "").strip())
        if _valid_dsn(dsn):
            return str(label or "AI_DB"), dsn

    # Env contract fallback (P0)
    # NOTE: POSTGRES_DSN is the canonical name in some parts of the stack.
    if force_external:
        dsn = _env_pick("PG_DSN_EXTERNAL", "POSTGRES_DSN", "PG_DSN")
        dsn = _normalize_asyncpg_dsn(dsn or "")
        if _valid_dsn(dsn):
            return "PG_DSN_EXTERNAL/POSTGRES_DSN", dsn
    if force_internal:
        dsn = _env_pick("PG_DSN_INTERNAL", "POSTGRES_DSN", "PG_DSN")
        dsn = _normalize_asyncpg_dsn(dsn or "")
        if _valid_dsn(dsn):
            return "PG_DSN_INTERNAL/POSTGRES_DSN", dsn

    dsn = _env_pick("POSTGRES_DSN", "PG_DSN", "PG_DSN_EXTERNAL", "PG_DSN_INTERNAL")
    dsn = _normalize_asyncpg_dsn(dsn or "")
    if _valid_dsn(dsn):
        return "ENV", dsn

    raise RuntimeError(
        "No valid Postgres DSN found. Set POSTGRES_DSN or PG_DSN (or PG_DSN_EXTERNAL/PG_DSN_INTERNAL), "
        "or pass --dsn."
    )


# ──────────────────────────────────────────────────────────────
# Migration model (versioned + idempotent)
# ──────────────────────────────────────────────────────────────

MIGRATION_TABLE = "schema_migrations"
LOCK_NAMESPACE = "solana_sniper_trades_migrations_v1"


@dataclass(frozen=True)
class Migration:
    mid: str
    description: str
    statements: Sequence[str]


def _lock_id(namespace: str) -> int:
    """
    Produce a stable 63-bit advisory lock id from a namespace string.
    """
    h = hashlib.sha256(namespace.encode("utf-8")).digest()
    # 63-bit positive int
    return int.from_bytes(h[:8], "big") & ((1 << 63) - 1)


def _sql(*lines: str) -> str:
    return "\n".join(lines).strip() + "\n"


MIGRATIONS: list[Migration] = [
    Migration(
        mid="0001_create_schema_migrations",
        description="Create schema_migrations table",
        statements=[
            _sql(
                f"CREATE TABLE IF NOT EXISTS {MIGRATION_TABLE} (",
                "  id TEXT PRIMARY KEY,",
                "  applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),",
                "  description TEXT NOT NULL",
                ")",
            )
        ],
    ),
    Migration(
        mid="0002_create_trades_base",
        description="Create trades table (base schema)",
        statements=[
            _sql(
                "CREATE TABLE IF NOT EXISTS trades (",
                "  id BIGSERIAL PRIMARY KEY,",
                "  ts TIMESTAMP NOT NULL,",
                "  token VARCHAR(64) NOT NULL,",
                "  symbol VARCHAR(32),",
                "  side VARCHAR(8),",
                "  amount DOUBLE PRECISION,",
                "  entry DOUBLE PRECISION,",
                "  current DOUBLE PRECISION,",
                "  profit DOUBLE PRECISION,",
                "  reason VARCHAR(128),",
                "  signature VARCHAR(256)",
                ")",
            ),
        ],
    ),
    Migration(
        mid="0003_trades_columns_defensive",
        description="Ensure commonly used trades columns exist (defensive ALTERs)",
        statements=[
            "ALTER TABLE trades ADD COLUMN IF NOT EXISTS ts TIMESTAMP",
            "ALTER TABLE trades ADD COLUMN IF NOT EXISTS token VARCHAR(64)",
            "ALTER TABLE trades ADD COLUMN IF NOT EXISTS symbol VARCHAR(32)",
            "ALTER TABLE trades ADD COLUMN IF NOT EXISTS side VARCHAR(8)",
            "ALTER TABLE trades ADD COLUMN IF NOT EXISTS amount DOUBLE PRECISION",
            "ALTER TABLE trades ADD COLUMN IF NOT EXISTS entry DOUBLE PRECISION",
            "ALTER TABLE trades ADD COLUMN IF NOT EXISTS current DOUBLE PRECISION",
            "ALTER TABLE trades ADD COLUMN IF NOT EXISTS profit DOUBLE PRECISION",
            "ALTER TABLE trades ADD COLUMN IF NOT EXISTS reason VARCHAR(128)",
            "ALTER TABLE trades ADD COLUMN IF NOT EXISTS signature VARCHAR(256)",
        ],
    ),
    Migration(
        mid="0004_trades_indexes",
        description="Create helpful indexes for trades queries",
        statements=[
            "CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades (ts)",
            "CREATE INDEX IF NOT EXISTS idx_trades_token ON trades (token)",
            "CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades (symbol)",
            "CREATE INDEX IF NOT EXISTS idx_trades_side ON trades (side)",
            "CREATE INDEX IF NOT EXISTS idx_trades_token_ts ON trades (token, ts DESC)",
            "CREATE INDEX IF NOT EXISTS idx_trades_symbol_ts ON trades (symbol, ts DESC)",
        ],
    ),
    Migration(
        mid="0005_trades_constraints_best_effort",
        description="Best-effort constraints (non-fatal if unsupported)",
        statements=[
            # Keep these constraints gentle. If older data violates them, they can fail.
            # We run them in a best-effort way below.
            _sql(
                "DO $$ BEGIN",
                "  ALTER TABLE trades",
                "    ADD CONSTRAINT trades_side_check",
                "    CHECK (side IS NULL OR side IN ('buy','sell'));",
                "EXCEPTION WHEN duplicate_object THEN NULL; END $$;",
            ),
            _sql(
                "DO $$ BEGIN",
                "  ALTER TABLE trades",
                "    ADD CONSTRAINT trades_token_not_empty",
                "    CHECK (token IS NOT NULL AND length(token) >= 32);",
                "EXCEPTION WHEN duplicate_object THEN NULL; END $$;",
            ),
        ],
    ),
]


# ──────────────────────────────────────────────────────────────
# DB helpers
# ──────────────────────────────────────────────────────────────


async def _scalar(conn: AsyncConnection, sql: str, params: dict[str, Any] | None = None) -> Any:
    res = await conn.execute(text(sql), params or {})
    row = res.first()
    return None if row is None else row[0]


async def _already_applied(conn: AsyncConnection, mid: str) -> bool:
    q = f"SELECT 1 FROM {MIGRATION_TABLE} WHERE id = :id LIMIT 1"
    v = await _scalar(conn, q, {"id": mid})
    return v is not None


async def _mark_applied(conn: AsyncConnection, m: Migration) -> None:
    q = f"INSERT INTO {MIGRATION_TABLE} (id, description) VALUES (:id, :desc) ON CONFLICT (id) DO NOTHING"
    await conn.execute(text(q), {"id": m.mid, "desc": m.description})


async def _acquire_lock(conn: AsyncConnection, lock_id: int) -> None:
    # pg_advisory_lock blocks until lock acquired
    await conn.execute(text("SELECT pg_advisory_lock(:k)"), {"k": lock_id})


async def _release_lock(conn: AsyncConnection, lock_id: int) -> None:
    await conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": lock_id})


async def _verify(conn: AsyncConnection) -> dict[str, Any]:
    out: dict[str, Any] = {}

    # Columns
    cols = await conn.execute(
        text(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'trades'
            ORDER BY ordinal_position
            """
        )
    )
    out["columns"] = [{"name": r[0], "type": r[1]} for r in cols.fetchall()]

    # Indexes
    idx = await conn.execute(
        text(
            """
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = 'public' AND tablename = 'trades'
            ORDER BY indexname
            """
        )
    )
    out["indexes"] = [{"name": r[0], "def": r[1]} for r in idx.fetchall()]

    # Migrations
    mig = await conn.execute(
        text(f"SELECT id, applied_at, description FROM {MIGRATION_TABLE} ORDER BY id")
    )
    out["migrations"] = [
        {"id": r[0], "applied_at": str(r[1]), "description": r[2]} for r in mig.fetchall()
    ]

    return out


def _print_verify(v: dict[str, Any]) -> None:
    print("\n=== verify: trades schema ===")
    print("columns:")
    for c in v.get("columns", []):
        print(f"  - {c['name']}: {c['type']}")
    print("\nindexes:")
    for i in v.get("indexes", []):
        print(f"  - {i['name']}")
    print("\nmigrations:")
    for m in v.get("migrations", []):
        print(f"  - {m['id']} @ {m['applied_at']} :: {m['description']}")


# ──────────────────────────────────────────────────────────────
# Runner
# ──────────────────────────────────────────────────────────────


async def run_migrations(
    dsn: str,
    *,
    echo: bool = False,
    dry_run: bool = False,
    verify: bool = True,
) -> None:
    engine: AsyncEngine = create_async_engine(
        dsn,
        echo=echo,
        pool_pre_ping=True,
        future=True,
    )

    lock_id = _lock_id(LOCK_NAMESPACE)
    applied_any = False

    try:
        async with engine.connect() as conn:
            # Ensure migration table exists before lock bookkeeping
            if dry_run:
                print(f"[dry-run] would acquire advisory lock id={lock_id}")
            else:
                await _acquire_lock(conn, lock_id)
            try:
                for m in MIGRATIONS:
                    if m.mid != "0001_create_schema_migrations":
                        # Make sure migration table exists (idempotent)
                        # If the first migration somehow isn't present, run it implicitly.
                        if not dry_run:
                            async with conn.begin():
                                # Apply 0001 statements (safe)
                                for s in MIGRATIONS[0].statements:
                                    await conn.execute(text(s))
                                await _mark_applied(conn, MIGRATIONS[0])

                    if not dry_run and await _already_applied(conn, m.mid):
                        print(f"✓ {m.mid} (already applied) — {m.description}")
                        continue
                    if dry_run:
                        print(f"\n--- {m.mid} :: {m.description} ---")
                        for s in m.statements:
                            print(s.strip())
                        continue

                    # Apply each migration atomically
                    print(f"\n→ applying {m.mid} — {m.description}")
                    async with conn.begin():
                        for s in m.statements:
                            # Some statements are DO $$ blocks or DDL; run as-is.
                            await conn.execute(text(s))
                        await _mark_applied(conn, m)
                    applied_any = True
                    print(f"✔ applied {m.mid}")

                if verify and not dry_run:
                    v = await _verify(conn)
                    _print_verify(v)

            finally:
                if not dry_run:
                    await _release_lock(conn, lock_id)

    finally:
        await engine.dispose()

    if dry_run:
        print("\n[dry-run] done (no changes applied).")
    else:
        print("\n✅ Migration complete." + (" (changes applied)" if applied_any else " (no-op)"))


# ──────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Idempotent Postgres migrations for the trades table (standalone)."
    )
    ap.add_argument(
        "--external",
        action="store_true",
        help="Prefer PG_DSN_EXTERNAL (or ai_db external) when resolving DSN",
    )
    ap.add_argument(
        "--internal",
        action="store_true",
        help="Prefer PG_DSN_INTERNAL (or ai_db internal) when resolving DSN",
    )
    ap.add_argument("--dsn", type=str, default=None, help="Explicit SQLAlchemy DSN override")
    ap.add_argument("--echo", action="store_true", help="SQLAlchemy echo (verbose SQL logging)")
    ap.add_argument(
        "--dry-run", action="store_true", help="Print SQL that would run, but do not apply"
    )
    ap.add_argument("--no-verify", action="store_true", help="Skip schema verification printout")
    return ap


def main() -> None:
    args = build_parser().parse_args()
    if args.external and args.internal:
        raise SystemExit("Choose only one of --external or --internal.")

    label, dsn = _resolve_dsn(
        dsn_override=args.dsn,
        force_external=bool(args.external),
        force_internal=bool(args.internal),
    )
    print(f"Using DSN [{label}]: {dsn}")

    import asyncio

    asyncio.run(
        run_migrations(
            dsn,
            echo=bool(args.echo),
            dry_run=bool(args.dry_run),
            verify=not bool(args.no_verify),
        )
    )


if __name__ == "__main__":
    main()
