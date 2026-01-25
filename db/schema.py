from __future__ import annotations

"""
db/schema.py

Canonical DB Schema (LOCK-IN / PROD)
===================================

Owns the "base layer" operational schema for the sniper system:

  - schema_migrations (ordering + idempotency)
  - scan_batches, scan_rows
  - trades
  - errors
  - pnl_snapshots

Hard rule:
  - DSN selection MUST go through utils_pkg.db_dsn when available, so DSN query
    params never leak into asyncpg/SQLAlchemy in unsafe ways.

Windows note:
  - Your pyproject deps skip asyncpg on Windows. Therefore this module MUST be
    import-safe even when asyncpg is not installed. We only require asyncpg
    at runtime when ensure_schema/get_schema_status are called.

Public API:
  - ensure_schema(...)  -> applies migrations (safe/idempotent)
  - get_schema_status() -> quick debug metadata

Env:
  - PG_SCHEMA (optional; default "public")
  - PG_APP_NAME (optional; recorded in schema_migrations.app)
"""

import contextlib
import logging
import os
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

log = logging.getLogger("db.schema")

# asyncpg is optional (Windows envs may not have it installed)
try:
    import asyncpg  # type: ignore
except Exception:  # pragma: no cover
    asyncpg = None  # type: ignore


# Canonical DSN brain (best-effort import; do not explode at import-time)
try:
    from utils_pkg.db_dsn import mask_dsn, pick_asyncpg_dsn  # type: ignore
except Exception:  # pragma: no cover
    pick_asyncpg_dsn = None  # type: ignore

    def mask_dsn(s: str) -> str:  # type: ignore
        return "<dsn>"


# ──────────────────────────────────────────────────────────────────────────────
# Migration model
# ──────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Migration:
    id: str
    name: str
    statements: tuple[str, ...]


def _env_schema() -> str:
    raw = (os.getenv("PG_SCHEMA", "") or "").strip()
    if not raw:
        return "public"
    # conservative sanitize: letters/digits/underscore only
    clean = "".join(ch for ch in raw if ch.isalnum() or ch == "_")
    return clean or "public"


def _qident(ident: str) -> str:
    # Quote identifier safely (minimal)
    return '"' + ident.replace('"', '""') + '"'


def _tbl(schema: str, name: str) -> str:
    if schema and schema != "public":
        return f"{_qident(schema)}.{_qident(name)}"
    return _qident(name)


def _nonempty(stmts: Iterable[str]) -> list[str]:
    out: list[str] = []
    for s in stmts:
        s2 = (s or "").strip()
        if s2:
            out.append(s2)
    return out


async def _exec_many(conn: Any, stmts: Sequence[str]) -> None:
    for sql in _nonempty(stmts):
        await conn.execute(sql)


async def _column_exists(conn: Any, schema: str, table: str, column: str) -> bool:
    q = """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = $1
          AND table_name   = $2
          AND column_name  = $3
        LIMIT 1
    """
    row = await conn.fetchrow(q, schema, table, column)
    return row is not None


# ──────────────────────────────────────────────────────────────────────────────
# Migrations
# ──────────────────────────────────────────────────────────────────────────────


def _migrations(schema: str) -> list[Migration]:
    """
    Ordered, idempotent migrations.

    We still record applied migrations in schema_migrations for deterministic
    rollouts and visibility.
    """
    m1 = Migration(
        id="0001",
        name="schema_migrations_table",
        statements=(
            f"""
            CREATE TABLE IF NOT EXISTS {_tbl(schema, "schema_migrations")}(
              id TEXT PRIMARY KEY,
              name TEXT NOT NULL,
              applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              app TEXT,
              extra JSONB
            );
            """,
            f"""
            CREATE INDEX IF NOT EXISTS idx_schema_migrations_applied_at
              ON {_tbl(schema, "schema_migrations")}(applied_at DESC);
            """,
        ),
    )

    m2 = Migration(
        id="0002",
        name="core_operational_tables",
        statements=(
            # scan_batches
            f"""
            CREATE TABLE IF NOT EXISTS {_tbl(schema, "scan_batches")}(
              id BIGSERIAL PRIMARY KEY,
              ts TIMESTAMPTZ NOT NULL DEFAULT now(),
              source TEXT,
              scanner_version TEXT,
              total INTEGER NOT NULL DEFAULT 0,
              kept INTEGER NOT NULL DEFAULT 0,
              dropped INTEGER NOT NULL DEFAULT 0,
              elapsed_ms INTEGER,
              reason_top TEXT,
              reason_counts JSONB,
              meta JSONB
            );
            """,
            f"""
            CREATE INDEX IF NOT EXISTS idx_scan_batches_ts
              ON {_tbl(schema, "scan_batches")}(ts DESC);
            """,
            # scan_rows
            f"""
            CREATE TABLE IF NOT EXISTS {_tbl(schema, "scan_rows")}(
              id BIGSERIAL PRIMARY KEY,
              ts TIMESTAMPTZ NOT NULL DEFAULT now(),
              batch_id BIGINT REFERENCES {_tbl(schema, "scan_batches")}(id) ON DELETE SET NULL,
              source TEXT,
              mint TEXT,
              symbol TEXT,

              score DOUBLE PRECISION,

              liq_usd DOUBLE PRECISION,
              vol_5m_usd DOUBLE PRECISION,
              vol_1h_usd DOUBLE PRECISION,

              buys_5m INTEGER,
              sells_5m INTEGER,
              chg_5m_pct DOUBLE PRECISION,

              age_min DOUBLE PRECISION,
              impact_1k_bps DOUBLE PRECISION,

              route_hint TEXT,
              raw JSONB
            );
            """,
            f"CREATE INDEX IF NOT EXISTS idx_scan_rows_ts ON {_tbl(schema, 'scan_rows')}(ts DESC);",
            f"CREATE INDEX IF NOT EXISTS idx_scan_rows_mint ON {_tbl(schema, 'scan_rows')}(mint);",
            f"CREATE INDEX IF NOT EXISTS idx_scan_rows_batch ON {_tbl(schema, 'scan_rows')}(batch_id);",
            f"""
            CREATE INDEX IF NOT EXISTS idx_scan_rows_source_ts
              ON {_tbl(schema, "scan_rows")}(source, ts DESC);
            """,
            # trades
            f"""
            CREATE TABLE IF NOT EXISTS {_tbl(schema, "trades")}(
              id BIGSERIAL PRIMARY KEY,
              ts TIMESTAMPTZ NOT NULL DEFAULT now(),

              type TEXT NOT NULL,
              status TEXT,
              mint TEXT,
              symbol TEXT,

              qty_token NUMERIC,
              sol_spent NUMERIC,
              sol_received NUMERIC,
              usd_value NUMERIC,

              entry_sol_per_token NUMERIC,
              slippage_bps INTEGER,

              signature TEXT,
              route TEXT,
              cu_price INTEGER,
              jito_tip INTEGER,

              err TEXT,
              meta JSONB
            );
            """,
            f"CREATE INDEX IF NOT EXISTS idx_trades_ts ON {_tbl(schema, 'trades')}(ts DESC);",
            f"CREATE INDEX IF NOT EXISTS idx_trades_mint ON {_tbl(schema, 'trades')}(mint);",
            f"CREATE INDEX IF NOT EXISTS idx_trades_sig ON {_tbl(schema, 'trades')}(signature);",
            f"""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_trades_signature_not_null
              ON {_tbl(schema, "trades")}(signature)
              WHERE signature IS NOT NULL;
            """,
            # errors
            f"""
            CREATE TABLE IF NOT EXISTS {_tbl(schema, "errors")}(
              id BIGSERIAL PRIMARY KEY,
              ts TIMESTAMPTZ NOT NULL DEFAULT now(),
              where_ TEXT,
              err TEXT,
              fingerprint TEXT,
              context JSONB
            );
            """,
            f"CREATE INDEX IF NOT EXISTS idx_errors_ts ON {_tbl(schema, 'errors')}(ts DESC);",
            f"CREATE INDEX IF NOT EXISTS idx_errors_where ON {_tbl(schema, 'errors')}(where_);",
            f"CREATE INDEX IF NOT EXISTS idx_errors_fingerprint ON {_tbl(schema, 'errors')}(fingerprint);",
            # pnl_snapshots
            f"""
            CREATE TABLE IF NOT EXISTS {_tbl(schema, "pnl_snapshots")}(
              id BIGSERIAL PRIMARY KEY,
              ts TIMESTAMPTZ NOT NULL,
              equity_usd NUMERIC,
              open_risk_usd NUMERIC,
              realized_pnl_usd NUMERIC,
              unrealized_pnl_usd NUMERIC,
              positions_count INTEGER,
              meta JSONB
            );
            """,
            f"""
            CREATE INDEX IF NOT EXISTS idx_pnl_snapshots_ts
              ON {_tbl(schema, "pnl_snapshots")}(ts DESC);
            """,
        ),
    )

    m3 = Migration(
        id="0003",
        name="compat_columns",
        statements=(
            f"ALTER TABLE {_tbl(schema, 'scan_rows')} ADD COLUMN IF NOT EXISTS mint TEXT;",
            f"ALTER TABLE {_tbl(schema, 'trades')}    ADD COLUMN IF NOT EXISTS mint TEXT;",
        ),
    )

    return [m1, m2, m3]


# ──────────────────────────────────────────────────────────────────────────────
# Apply migrations
# ──────────────────────────────────────────────────────────────────────────────


async def _ensure_schema_namespace(conn: Any, schema: str) -> None:
    if schema and schema != "public":
        try:
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {_qident(schema)};")
        except Exception as e:
            log.warning("CREATE_SCHEMA_FAILED schema=%s err=%s", schema, str(e))


async def _already_applied(conn: Any, schema: str, mid: str) -> bool:
    q = f"SELECT 1 FROM {_tbl(schema, 'schema_migrations')} WHERE id=$1"
    row = await conn.fetchrow(q, mid)
    return row is not None


async def _mark_applied(
    conn: Any, schema: str, m: Migration, *, extra: dict[str, Any] | None
) -> None:
    app = (os.getenv("PG_APP_NAME", "") or "").strip() or None
    await conn.execute(
        f"""
        INSERT INTO {_tbl(schema, "schema_migrations")}(id, name, app, extra)
        VALUES($1, $2, $3, $4)
        ON CONFLICT (id) DO NOTHING
        """,
        m.id,
        m.name,
        app,
        extra,
    )


async def _compat_backfill(conn: Any, schema: str) -> None:
    """
    Best-effort legacy backfills (idempotent).
    Only runs if legacy token_address columns exist.
    """
    try:
        if await _column_exists(conn, schema, "scan_rows", "token_address"):
            await conn.execute(
                f"""
                UPDATE {_tbl(schema, "scan_rows")}
                SET mint = COALESCE(mint, token_address)
                WHERE mint IS NULL
                """
            )
        if await _column_exists(conn, schema, "trades", "token_address"):
            await conn.execute(
                f"""
                UPDATE {_tbl(schema, "trades")}
                SET mint = COALESCE(mint, token_address)
                WHERE mint IS NULL
                """
            )
    except Exception as e:
        log.warning("compat_backfill_failed schema=%s err=%s", schema, str(e))


async def ensure_schema(
    *,
    dsn: str | None = None,
    connect_kwargs: dict[str, Any] | None = None,
    timeout_s: float | None = 12.0,
    app_name: str | None = None,
    dry_run: bool = False,
) -> None:
    """
    Apply canonical schema migrations (idempotent).

    - If dsn/connect_kwargs not provided, uses utils_pkg.db_dsn.pick_asyncpg_dsn().
    - Requires asyncpg at runtime. Import is safe even if asyncpg isn't installed.
    """
    schema = _env_schema()

    if asyncpg is None:
        raise RuntimeError(
            "asyncpg is not installed. On Windows, your deps may skip it; "
            "either run schema ops on Linux/Render or install asyncpg manually."
        )

    if dsn is None or connect_kwargs is None:
        if pick_asyncpg_dsn is None:
            raise RuntimeError("utils_pkg.db_dsn not available; cannot pick DSN safely")
        dsn2, kw2 = pick_asyncpg_dsn(timeout_s=timeout_s, app_name=app_name)
        dsn = dsn or dsn2
        connect_kwargs = connect_kwargs or kw2

    if not dsn:
        raise RuntimeError("Postgres DSN not set (expected DATABASE_URL / PG_DSN_* / etc.)")

    connect_kwargs = connect_kwargs or {}
    log.info("ensure_schema: connecting dsn=%s schema=%s", mask_dsn(dsn), schema)

    conn: Any | None = None
    try:
        conn = await asyncpg.connect(dsn, **connect_kwargs)  # type: ignore[attr-defined]
        await _ensure_schema_namespace(conn, schema)

        migs = _migrations(schema)

        # Ensure migrations table exists first
        await _exec_many(conn, migs[0].statements)

        if dry_run:
            pending: list[str] = []
            for m in migs:
                if not await _already_applied(conn, schema, m.id):
                    pending.append(f"{m.id}:{m.name}")
            log.info("ensure_schema(dry_run): pending=%s", pending)
            return

        async with conn.transaction():
            for m in migs:
                if await _already_applied(conn, schema, m.id):
                    continue
                await _exec_many(conn, m.statements)
                await _mark_applied(conn, schema, m, extra={"statements": len(m.statements)})
                log.info("migration applied: %s %s", m.id, m.name)

            # legacy backfills (safe + idempotent)
            await _compat_backfill(conn, schema)

        log.info("db.schema ensured schema=%s", schema)

    finally:
        if conn is not None:
            with contextlib.suppress(Exception):
                await conn.close()


async def get_schema_status(
    *, timeout_s: float | None = 6.0, app_name: str | None = None
) -> dict[str, Any]:
    """
    Quick schema debug info (safe for health checks).
    """
    schema = _env_schema()

    if asyncpg is None:
        return {"ok": False, "schema": schema, "error": "asyncpg not installed"}

    if pick_asyncpg_dsn is None:
        return {"ok": False, "schema": schema, "error": "utils_pkg.db_dsn missing"}

    dsn, kw = pick_asyncpg_dsn(timeout_s=timeout_s, app_name=app_name)
    if not dsn:
        return {"ok": False, "schema": schema, "error": "no dsn"}

    conn: Any | None = None
    try:
        conn = await asyncpg.connect(dsn, **kw)  # type: ignore[attr-defined]
        await _ensure_schema_namespace(conn, schema)

        migs = _migrations(schema)
        await _exec_many(conn, migs[0].statements)

        rows = await conn.fetch(
            f"""
            SELECT id, name, applied_at
            FROM {_tbl(schema, "schema_migrations")}
            ORDER BY applied_at DESC
            LIMIT 50
            """
        )
        return {
            "ok": True,
            "schema": schema,
            "dsn": mask_dsn(dsn),
            "applied": [
                {"id": r["id"], "name": r["name"], "applied_at": str(r["applied_at"])} for r in rows
            ],
        }
    except Exception as e:
        return {"ok": False, "schema": schema, "dsn": mask_dsn(dsn), "error": str(e)}
    finally:
        if conn is not None:
            with contextlib.suppress(Exception):
                await conn.close()


__all__ = ["Migration", "ensure_schema", "get_schema_status"]
