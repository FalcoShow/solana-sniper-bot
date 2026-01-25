#!/usr/bin/env python3
from __future__ import annotations

"""
scripts/pg_maintenance.py — Postgres maintenance (P0 DSN contract, V3+++)
========================================================================

Safe maintenance entrypoint for your Postgres trade ledger.

Primary job:
  • Prune old rows from the canonical `trades` table using retention policy.

Secondary (optional):
  • Print table stats (row count / oldest / newest / size)
  • VACUUM (ANALYZE) after pruning (best-effort, autocommit)

Design goals
------------
- Uses the same DSN/runtime contract as the rest of the stack (via core_utils / ai_db / env).
- Destructive operations require --yes (non-interactive safe).
- Dry-run mode to see impact before deleting.
- Robust across “different core_utils shapes” using signature inspection.
- Safe identifier validation/quoting for --table.

Examples
--------
python -m scripts.pg_maintenance --info
python -m scripts.pg_maintenance --prune --dry-run
python -m scripts.pg_maintenance --prune --yes
python -m scripts.pg_maintenance --prune --days 21 --yes --vacuum

Notes
-----
- Retention default is taken from utils_pkg.core_utils.PG_TRADES_RETENTION_DAYS when present,
  otherwise falls back to env PG_TRADES_RETENTION_DAYS, else 30.

DSN contract (canonical)
------------------------
1) --dsn (CLI) if provided
2) utils_pkg.ai_db.resolve_postgres_dsn(...) if available
3) Env:
     POSTGRES_DSN (preferred), PG_DSN
     PG_DSN_EXTERNAL / PG_DSN_INTERNAL
"""

import argparse
import asyncio
import contextlib
import inspect
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
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
# Logging (canonical if present)
# ──────────────────────────────────────────────────────────────


def _get_logger():
    try:
        from logging_setup import get_logger  # type: ignore

        return get_logger()
    except Exception:
        import logging

        return logging.getLogger("pg_maintenance")


log = _get_logger()

# ──────────────────────────────────────────────────────────────
# Core utils (canonical runtime wiring)
# ──────────────────────────────────────────────────────────────

import utils_pkg.core_utils as cu  # type: ignore

# ──────────────────────────────────────────────────────────────
# Defaults / helpers
# ──────────────────────────────────────────────────────────────

DEFAULT_RETENTION_DAYS = 30
MIN_RETENTION_DAYS = 1
MAX_RETENTION_DAYS = 3650

DEFAULT_BATCH = 25_000
MAX_BATCH = 250_000
DEFAULT_MAX_DELETE = 5_000_000  # safety cap

_VALID_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _qident(name: str) -> str:
    n = (name or "").strip().strip('"')
    if not _VALID_IDENT.match(n):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return f'"{n}"'


def _qtable(ref: str) -> str:
    raw = (ref or "").strip().strip('"')
    if not raw:
        raise ValueError("Empty table name")
    if "." in raw:
        s, t = raw.split(".", 1)
        return f"{_qident(s)}.{_qident(t)}"
    return _qident(raw)


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except Exception:
        return default


def _clamp_int(v: int, lo: int, hi: int) -> int:
    try:
        v = int(v)
    except Exception:
        v = lo
    return max(lo, min(hi, v))


def _resolve_retention_days(override: int | None) -> int:
    if override is not None:
        return _clamp_int(override, MIN_RETENTION_DAYS, MAX_RETENTION_DAYS)

    days = getattr(cu, "PG_TRADES_RETENTION_DAYS", None)
    if isinstance(days, int) and days > 0:
        return _clamp_int(days, MIN_RETENTION_DAYS, MAX_RETENTION_DAYS)

    return _clamp_int(
        _env_int("PG_TRADES_RETENTION_DAYS", DEFAULT_RETENTION_DAYS),
        MIN_RETENTION_DAYS,
        MAX_RETENTION_DAYS,
    )


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _cutoff_ts(days: int) -> datetime:
    # trades.ts is typically TIMESTAMP WITHOUT TIME ZONE; pass naive UTC to PG
    return (_utc_now() - timedelta(days=days)).astimezone(UTC).replace(tzinfo=None)


def _valid_url(s: str) -> bool:
    if not s:
        return False
    try:
        make_url(s.strip())
        return True
    except Exception:
        return False


def _to_asyncpg(dsn: str) -> str:
    """
    Normalize to SQLAlchemy asyncpg DSN (minimal, safe).
    """
    dsn = (dsn or "").strip()
    if not dsn:
        return dsn
    if dsn.startswith("postgres://"):
        return "postgresql+asyncpg://" + dsn[len("postgres://") :]
    if dsn.startswith("postgresql://"):
        return "postgresql+asyncpg://" + dsn[len("postgresql://") :]
    if dsn.startswith("postgresql+asyncpg://"):
        return dsn
    if dsn.startswith("postgresql+psycopg"):
        return "postgresql+asyncpg://" + dsn.split("://", 1)[1]
    if dsn.startswith("postgresql+psycopg2"):
        return "postgresql+asyncpg://" + dsn.split("://", 1)[1]
    return dsn


def _resolve_dsn(
    *,
    explicit: str,
    force_external: bool,
    force_internal: bool,
) -> tuple[str, str]:
    """
    P0 DSN contract:
      1) --dsn
      2) ai_db.resolve_postgres_dsn
      3) env: POSTGRES_DSN/PG_DSN, PG_DSN_EXTERNAL, PG_DSN_INTERNAL (unless forced)
    """
    if explicit and _valid_url(explicit):
        return "CLI_DSN", _to_asyncpg(explicit)

    with contextlib.suppress(Exception):
        from utils_pkg.ai_db import resolve_postgres_dsn  # type: ignore

        label, dsn = resolve_postgres_dsn(
            prefer_external=force_external, prefer_internal=force_internal
        )  # type: ignore
        if dsn and _valid_url(str(dsn)):
            return str(label or "AI_DB"), _to_asyncpg(str(dsn))

    dsn_main = (os.getenv("POSTGRES_DSN") or os.getenv("PG_DSN") or "").strip()
    dsn_ext = (os.getenv("PG_DSN_EXTERNAL") or "").strip()
    dsn_int = (os.getenv("PG_DSN_INTERNAL") or "").strip()

    if force_external and _valid_url(dsn_ext):
        return "PG_DSN_EXTERNAL", _to_asyncpg(dsn_ext)
    if force_internal and _valid_url(dsn_int):
        return "PG_DSN_INTERNAL", _to_asyncpg(dsn_int)

    if _valid_url(dsn_main):
        return ("POSTGRES_DSN" if os.getenv("POSTGRES_DSN") else "PG_DSN"), _to_asyncpg(dsn_main)
    if _valid_url(dsn_ext):
        return "PG_DSN_EXTERNAL", _to_asyncpg(dsn_ext)
    if _valid_url(dsn_int):
        return "PG_DSN_INTERNAL", _to_asyncpg(dsn_int)

    return "UNKNOWN", ""


def _find_core_engine() -> AsyncEngine | None:
    for name in (
        "pg_engine",
        "PG_ENGINE",
        "engine",
        "db_engine",
        "pg_async_engine",
        "async_engine",
    ):
        eng = getattr(cu, name, None)
        if isinstance(eng, AsyncEngine):
            return eng
    return None


@dataclass
class _EngineLease:
    engine: AsyncEngine
    owned: bool
    label: str


async def _get_engine(*, dsn: str, label: str) -> _EngineLease:
    eng = _find_core_engine()
    if eng is not None:
        return _EngineLease(engine=eng, owned=False, label="CORE_UTILS")

    if not dsn:
        raise RuntimeError(
            "No Postgres engine found from core_utils, and no DSN provided/available to build one."
        )

    log.info(f"[pg_maintenance] building standalone engine via {label}")
    eng2 = create_async_engine(dsn, echo=False, pool_pre_ping=True, pool_size=2, max_overflow=0)
    return _EngineLease(engine=eng2, owned=True, label=label)


async def _scalar(conn: AsyncConnection, sql: str, params: dict[str, Any] | None = None) -> Any:
    res = await conn.execute(text(sql), params or {})
    row = res.first()
    return None if row is None else row[0]


# ──────────────────────────────────────────────────────────────
# Maintenance actions
# ──────────────────────────────────────────────────────────────


async def _info(conn: AsyncConnection, table_ref: str) -> dict[str, Any]:
    qt = _qtable(table_ref)
    out: dict[str, Any] = {"table": table_ref}

    out["rows"] = await _scalar(conn, f"SELECT COUNT(*)::bigint FROM {qt}")
    out["oldest_ts"] = await _scalar(conn, f"SELECT MIN({_qident('ts')}) FROM {qt}")
    out["newest_ts"] = await _scalar(conn, f"SELECT MAX({_qident('ts')}) FROM {qt}")

    with contextlib.suppress(Exception):
        out["bytes"] = await _scalar(
            conn, "SELECT pg_total_relation_size(:tname)", {"tname": table_ref}
        )

    return out


async def _count_prunable(conn: AsyncConnection, cutoff: datetime, table_ref: str) -> int:
    qt = _qtable(table_ref)
    n = await _scalar(
        conn,
        f"SELECT COUNT(*)::bigint FROM {qt} WHERE {_qident('ts')} < :cutoff",
        {"cutoff": cutoff},
    )
    try:
        return int(n or 0)
    except Exception:
        return 0


async def _prune_in_batches(
    conn: AsyncConnection,
    cutoff: datetime,
    *,
    table_ref: str,
    batch: int,
    max_delete: int,
) -> int:
    qt = _qtable(table_ref)
    qid = _qident("id")
    qts = _qident("ts")

    batch = _clamp_int(batch, 100, MAX_BATCH)
    max_delete = _clamp_int(max_delete, 1, 100_000_000)

    total = 0
    while total < max_delete:
        remaining = max_delete - total
        this_batch = min(batch, remaining)

        sql = f"""
            WITH doomed AS (
                SELECT {qid}
                FROM {qt}
                WHERE {qts} < :cutoff
                ORDER BY {qts} ASC, {qid} ASC
                LIMIT :lim
            )
            DELETE FROM {qt} t
            USING doomed d
            WHERE t.{qid} = d.{qid}
            RETURNING t.{qid}
        """.strip()

        async with conn.begin():
            res = await conn.execute(text(sql), {"cutoff": cutoff, "lim": int(this_batch)})
            deleted = res.rowcount or 0

        if deleted <= 0:
            break

        total += int(deleted)

    return total


async def _call_pg_prune_trades(days: int, *, dry_run: bool) -> int | None:
    fn = getattr(cu, "pg_prune_trades", None)
    if fn is None:
        return None

    try:
        sig = inspect.signature(fn)
    except Exception:
        sig = None

    kwargs: dict[str, Any] = {}
    if sig is not None:
        if "days" in sig.parameters:
            kwargs["days"] = days
        if "dry_run" in sig.parameters:
            kwargs["dry_run"] = bool(dry_run)
        if "cutoff" in sig.parameters:
            kwargs["cutoff"] = _cutoff_ts(days)

    try:
        if inspect.iscoroutinefunction(fn):
            if kwargs:
                return int(await fn(**kwargs))  # type: ignore[misc]
            return int(await fn(days))  # type: ignore[misc]
        if kwargs:
            return int(fn(**kwargs))  # type: ignore[misc]
        return int(fn(days))  # type: ignore[misc]
    except TypeError:
        with contextlib.suppress(Exception):
            if inspect.iscoroutinefunction(fn):
                return int(await fn(days))  # type: ignore[misc]
            return int(fn(days))  # type: ignore[misc]
        return None
    except Exception as e:
        log.warning(f"[pg_maintenance] pg_prune_trades failed: {e}")
        return None


async def _vacuum_analyze(engine: AsyncEngine, table_ref: str) -> None:
    qt = _qtable(table_ref)
    try:
        async with engine.connect() as conn:
            ac = conn.execution_options(isolation_level="AUTOCOMMIT")
            await ac.execute(text(f"VACUUM (ANALYZE) {qt}"))
    except Exception as e:
        log.warning(f"[pg_maintenance] vacuum/analyze skipped: {e}")


# ──────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Postgres maintenance tasks (prune trades, stats, vacuum)."
    )

    gdsn = ap.add_mutually_exclusive_group()
    gdsn.add_argument("--external", action="store_true", help="Prefer PG_DSN_EXTERNAL.")
    gdsn.add_argument("--internal", action="store_true", help="Prefer PG_DSN_INTERNAL.")
    ap.add_argument("--dsn", type=str, default="", help="Explicit DSN override (optional).")

    ap.add_argument(
        "--info", action="store_true", help="Print table stats (rows/oldest/newest/size)."
    )
    ap.add_argument(
        "--prune", action="store_true", help="Prune old trades based on retention days."
    )
    ap.add_argument("--days", type=int, default=None, help="Override retention days (integer).")

    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Show how many rows would be pruned, but do not delete.",
    )
    ap.add_argument(
        "--yes",
        action="store_true",
        help="Required to actually delete rows (non-interactive safety).",
    )

    ap.add_argument(
        "--batch", type=int, default=DEFAULT_BATCH, help=f"Batch size (default {DEFAULT_BATCH})."
    )
    ap.add_argument(
        "--max-delete",
        type=int,
        default=DEFAULT_MAX_DELETE,
        help=f"Safety cap (default {DEFAULT_MAX_DELETE}).",
    )

    ap.add_argument(
        "--vacuum", action="store_true", help="VACUUM (ANALYZE) after prune (best-effort)."
    )
    ap.add_argument(
        "--table",
        type=str,
        default="trades",
        help="Table name (default trades). Accepts schema.table.",
    )

    return ap


def _set_windows_asyncio_policy() -> None:
    with contextlib.suppress(Exception):
        if os.name == "nt" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]


async def _run(args: argparse.Namespace) -> int:
    inited = False
    try:
        await cu.init_resources()
        inited = True
    except Exception as e:
        log.warning(
            f"[pg_maintenance] core_utils.init_resources failed; continuing standalone if possible: {e}"
        )

    label, dsn = _resolve_dsn(
        explicit=str(args.dsn or ""),
        force_external=bool(args.external),
        force_internal=bool(args.internal),
    )
    dsn = _to_asyncpg(dsn)

    lease: _EngineLease | None = None
    try:
        lease = await _get_engine(dsn=dsn, label=label)
        engine = lease.engine
        table_ref = str(args.table).strip() or "trades"

        async with engine.connect() as conn:
            did_any = False

            if args.info:
                did_any = True
                info = await _info(conn, table_ref)
                print("=== Postgres: table info ===")
                print(f"table     : {info.get('table')}")
                print(f"rows      : {info.get('rows')}")
                print(f"oldest_ts : {info.get('oldest_ts')}")
                print(f"newest_ts : {info.get('newest_ts')}")
                size_b = info.get("bytes")
                if size_b is not None:
                    try:
                        mb = float(size_b) / (1024.0 * 1024.0)
                        print(f"size      : {int(size_b)} bytes ({mb:.2f} MiB)")
                    except Exception:
                        print(f"size      : {size_b}")
                print("")

            if args.prune:
                did_any = True
                days = _resolve_retention_days(args.days)
                cutoff = _cutoff_ts(days)

                prunable = await _count_prunable(conn, cutoff, table_ref)
                print("=== Prune plan ===")
                print(f"table        : {table_ref}")
                print(f"retention    : {days} days")
                print(f"cutoff (UTC) : {cutoff.isoformat()}  (naive UTC for TIMESTAMP)")
                print(f"rows prunable: {prunable}")
                print("")

                if prunable <= 0:
                    print("Nothing to prune ✅")
                    return 0

                if args.dry_run:
                    print("Dry-run enabled — no rows deleted ✅")
                    return 0

                if not args.yes:
                    print("Refusing to prune without --yes (safety). Re-run with --yes to delete.")
                    return 3

                t0 = time.time()

                removed = await _call_pg_prune_trades(days, dry_run=False)
                if removed is None:
                    removed = await _prune_in_batches(
                        conn,
                        cutoff,
                        table_ref=table_ref,
                        batch=int(args.batch),
                        max_delete=int(args.max_delete),
                    )

                dt = time.time() - t0
                print(f"Pruned {removed} rows in {dt:.2f}s ✅")

                if args.vacuum:
                    print("Running VACUUM (ANALYZE) (best-effort)…")
                    await _vacuum_analyze(engine, table_ref)
                    print("VACUUM done (or skipped).")

            if not did_any:
                print("Nothing to do. Use --info and/or --prune. See --help.")
                return 0

        return 0

    finally:
        if inited:
            with contextlib.suppress(Exception):
                await cu.close_resources()
        if lease is not None and lease.owned:
            with contextlib.suppress(Exception):
                await lease.engine.dispose()


def main(argv: list[str] | None = None) -> int:
    _set_windows_asyncio_policy()
    args = build_parser().parse_args(argv)

    try:
        return asyncio.run(_run(args))
    except KeyboardInterrupt:
        print("\n⏹ stopped")
        return 0
    except Exception as e:
        print(f"❌ pg_maintenance failed: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
