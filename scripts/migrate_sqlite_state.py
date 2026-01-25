from __future__ import annotations

"""
scripts/migrate_sqlite_state.py

Migrate legacy SQLite state (state.sqlite3) into Postgres, then treat SQLite as legacy-only.

- Idempotent: safe to run multiple times (UPSERT).
- Minimal assumptions: looks for table(s) and migrates known ones.
- Uses POSTGRES_DSN/PG_DSN/PNL_DSN/DATABASE_URL (same family as the rest of stack).
"""

import argparse
import asyncio
import os
import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

_TRUE = {"1", "true", "yes", "y", "on", "t"}


def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    return (v if v is not None else d).strip()


def _envb(k: str, d: bool = False) -> bool:
    v = _envs(k, "")
    if not v:
        return bool(d)
    return v.lower() in _TRUE


def _normalize_dsn(raw: str) -> str:
    v = (raw or "").strip()
    if v.startswith("postgresql+asyncpg://"):
        v = "postgresql://" + v[len("postgresql+asyncpg://") :]
    if v.startswith("postgres://"):
        v = "postgresql://" + v[len("postgres://") :]
    # Render PG often requires sslmode=require; harmless elsewhere.
    if "render.com" in v and "sslmode=" not in v:
        v += "&sslmode=require" if "?" in v else "?sslmode=require"
    return v


def _pick_dsn() -> str:
    for k in (
        "PNL_DSN",
        "POSTGRES_DSN",
        "PG_DSN",
        "DATABASE_URL",
        "PG_DSN_INTERNAL",
        "PG_DSN_EXTERNAL",
        "PG_DSN_PUBLIC",
    ):
        v = _envs(k, "")
        if v:
            return _normalize_dsn(v)
    return ""


@dataclass(frozen=True)
class SnipedTokenRow:
    token: str
    price: float | None
    ts: int | None
    unit: str | None


def _sqlite_tables(db_path: str) -> list[str]:
    con = sqlite3.connect(db_path)
    try:
        cur = con.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return [r[0] for r in cur.fetchall()]
    finally:
        con.close()


def _read_sniped_tokens(db_path: str) -> list[SnipedTokenRow]:
    con = sqlite3.connect(db_path)
    try:
        cur = con.cursor()
        cur.execute("SELECT token, price, timestamp, unit FROM sniped_tokens")
        out: list[SnipedTokenRow] = []
        for token, price, ts, unit in cur.fetchall():
            out.append(
                SnipedTokenRow(
                    token=str(token),
                    price=float(price) if price is not None else None,
                    ts=int(ts) if ts is not None else None,
                    unit=str(unit) if unit is not None else None,
                )
            )
        return out
    finally:
        con.close()


async def _pg_exec(conn: Any, sql: str, *args: Any) -> None:
    await conn.execute(sql, *args)


async def _pg_many(conn: Any, sql: str, rows: Sequence[tuple[Any, ...]]) -> None:
    # asyncpg executemany is fine for moderate volumes.
    await conn.executemany(sql, rows)


async def _connect_pg(dsn: str) -> Any:
    # Prefer your canonical ai_db if available.
    try:
        from utils_pkg import ai_db  # type: ignore

        # common patterns supported in your stack (best-effort)
        if hasattr(ai_db, "get_pool_async"):
            pool = await ai_db.get_pool_async()
            return ("pool", pool)
        if hasattr(ai_db, "get_pool"):
            pool = ai_db.get_pool()
            return ("pool", pool)
        if hasattr(ai_db, "pool") and ai_db.pool is not None:
            return ("pool", ai_db.pool)
    except Exception:
        pass

    try:
        import asyncpg  # type: ignore
    except Exception as e:
        raise RuntimeError("asyncpg not installed; pip install asyncpg") from e

    conn = await asyncpg.connect(dsn=dsn)
    return ("conn", conn)


async def _with_conn(pg_handle: tuple[str, Any], fn):
    kind, obj = pg_handle
    if kind == "pool":
        async with obj.acquire() as conn:
            return await fn(conn)
    else:
        return await fn(obj)


async def migrate(db_path: str, *, dry_run: bool = False) -> None:
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"SQLite file not found: {db_path}")

    tables = set(_sqlite_tables(db_path))
    print(f"[sqlite] path={db_path} tables={sorted(tables)}")

    dsn = _pick_dsn()
    if not dsn:
        raise RuntimeError(
            "No Postgres DSN found. Set POSTGRES_DSN (or PNL_DSN/PG_DSN/DATABASE_URL)."
        )

    pg = await _connect_pg(dsn)

    async def _do(conn: Any) -> None:
        await _pg_exec(
            conn,
            """
            CREATE TABLE IF NOT EXISTS legacy_sniped_tokens (
              token TEXT PRIMARY KEY,
              price DOUBLE PRECISION,
              ts BIGINT,
              unit TEXT,
              source TEXT NOT NULL DEFAULT 'sqlite_legacy',
              migrated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """,
        )
        await _pg_exec(
            conn,
            "CREATE INDEX IF NOT EXISTS legacy_sniped_tokens_ts_idx ON legacy_sniped_tokens(ts);",
        )

        if "sniped_tokens" in tables:
            rows = _read_sniped_tokens(db_path)
            print(f"[sqlite] sniped_tokens rows={len(rows)}")

            if dry_run:
                print("[dry-run] would upsert into legacy_sniped_tokens (skipping writes)")
                return

            payload = [(r.token, r.price, r.ts, r.unit) for r in rows]
            if payload:
                await _pg_many(
                    conn,
                    """
                    INSERT INTO legacy_sniped_tokens (token, price, ts, unit)
                    VALUES ($1,$2,$3,$4)
                    ON CONFLICT (token) DO UPDATE SET
                      price = EXCLUDED.price,
                      ts    = EXCLUDED.ts,
                      unit  = EXCLUDED.unit,
                      migrated_at = now();
                    """,
                    payload,
                )
            print("[pg] migrated legacy_sniped_tokens ok")
        else:
            print("[sqlite] no sniped_tokens table found; nothing to migrate")

    await _with_conn(pg, _do)

    # cleanup connection if direct conn
    kind, obj = pg
    if kind == "conn":
        await obj.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sqlite", default=_envs("SQLITE_STATE_PATH", "state.sqlite3"))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    asyncio.run(migrate(args.sqlite, dry_run=bool(args.dry_run)))


if __name__ == "__main__":
    main()
