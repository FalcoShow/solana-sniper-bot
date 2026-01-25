#!/usr/bin/env python3
from __future__ import annotations

"""
scripts/pg_tail.py — Query / tail Postgres trades (V3+++ / P0 DSN contract)
==========================================================================

Purpose
-------
Production-safe CLI for inspecting the canonical `trades` table:

  • One-shot query: latest N rows with filters
  • Follow (tail) mode: poll for *new* rows after a stable watermark

P0 lock-ins (env contract)
--------------------------
- DSN contract matches DB baselayer:
    Prefer: POSTGRES_DSN
    Fallbacks: PG_DSN, PG_DSN_EXTERNAL, PG_DSN_INTERNAL
  plus:
    --dsn (CLI) overrides env
    utils_pkg.ai_db.resolve_postgres_dsn(...) if present

Async driver normalization
--------------------------
Accepts postgres/postgresql DSNs and upgrades to `postgresql+asyncpg://...`

Safety / determinism
--------------------
- Parameterized SQL for user-provided values.
- Identifiers (table/columns) are validated & quoted (no injection).
- Stable watermark: (ts, id) tuple.
- Graceful degradation if optional modules are missing.

Examples
--------
python -m scripts.pg_tail --limit 20
python -m scripts.pg_tail --token So11111111111111111111111111111111111111112 --follow --interval 3
python -m scripts.pg_tail --side sell --since 2026-01-01 --limit 50
python -m scripts.pg_tail --follow --jsonl
python -m scripts.pg_tail --external
python -m scripts.pg_tail --internal
"""

import argparse
import asyncio
import contextlib
import csv
import json
import os
import re
import sys
import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

# ──────────────────────────────────────────────────────────────
# Repo root / dotenv (best-effort)
# ──────────────────────────────────────────────────────────────

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

with contextlib.suppress(Exception):
    from dotenv import find_dotenv, load_dotenv  # type: ignore

    load_dotenv(find_dotenv(), override=False)

# ──────────────────────────────────────────────────────────────
# Logging (canonical)
# ──────────────────────────────────────────────────────────────


def _get_logger():
    try:
        from logging_setup import get_logger  # type: ignore

        return get_logger()
    except Exception:
        import logging

        return logging.getLogger("pg_tail")


log = _get_logger()

# ──────────────────────────────────────────────────────────────
# Defaults / constants
# ──────────────────────────────────────────────────────────────

DEFAULT_TABLE = "trades"
DEFAULT_COLUMNS = [
    "id",
    "ts",
    "token",
    "symbol",
    "side",
    "amount",
    "entry",
    "current",
    "profit",
    "reason",
    "signature",
]

MAX_LIMIT = 10_000
MIN_INTERVAL = 1.0

_EPOCH_UTC = datetime(1970, 1, 1, tzinfo=UTC)
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


# ──────────────────────────────────────────────────────────────
# DSN resolution + normalization (asyncpg)
# ──────────────────────────────────────────────────────────────


def _env_str(*keys: str) -> str:
    for k in keys:
        v = os.getenv(k)
        if v and str(v).strip():
            return str(v).strip()
    return ""


def _valid_url(s: str) -> bool:
    if not s:
        return False
    try:
        make_url(s.strip())
        return True
    except Exception:
        return False


def _as_asyncpg_dsn(dsn: str) -> str:
    """
    Convert common Postgres DSNs to asyncpg DSN understood by SQLAlchemy:
      postgres://...                -> postgresql+asyncpg://...
      postgresql://...              -> postgresql+asyncpg://...
      postgresql+psycopg2://...     -> postgresql+asyncpg://...
      postgresql+psycopg://...      -> postgresql+asyncpg://...
      postgresql+asyncpg://...      -> unchanged
    """
    dsn = (dsn or "").strip()
    if not dsn:
        return dsn

    url: URL = make_url(dsn)
    driver = (url.drivername or "").lower()
    if driver == "postgres":
        driver = "postgresql"

    if driver.startswith("postgresql+asyncpg"):
        return str(url)

    if driver.startswith("postgresql"):
        url = url.set(drivername="postgresql+asyncpg")
        return str(url)

    return str(url)


def _pick_dsn(*, force_external: bool, force_internal: bool, explicit: str) -> tuple[str, str]:
    """
    Returns (label, dsn). Raises on failure.

    P0 env contract keys only:
      POSTGRES_DSN, PG_DSN, PG_DSN_EXTERNAL, PG_DSN_INTERNAL
    """
    # 1) Explicit CLI DSN wins
    if explicit and _valid_url(explicit):
        return "CLI_DSN", _as_asyncpg_dsn(explicit)

    # 2) Optional: canonical resolver from utils_pkg.ai_db
    with contextlib.suppress(Exception):
        from utils_pkg.ai_db import resolve_postgres_dsn  # type: ignore

        label, dsn = resolve_postgres_dsn(
            prefer_external=force_external, prefer_internal=force_internal
        )  # type: ignore
        if dsn and _valid_url(str(dsn)):
            return str(label or "AI_DB"), _as_asyncpg_dsn(str(dsn))

    # 3) Env contract (P0) — DO NOT reference non-contract keys.
    dsn_main = _env_str("POSTGRES_DSN", "PG_DSN")
    dsn_ext = _env_str("PG_DSN_EXTERNAL")
    dsn_int = _env_str("PG_DSN_INTERNAL")

    if force_external and _valid_url(dsn_ext):
        return "PG_DSN_EXTERNAL", _as_asyncpg_dsn(dsn_ext)
    if force_internal and _valid_url(dsn_int):
        return "PG_DSN_INTERNAL", _as_asyncpg_dsn(dsn_int)

    # Convenience preference: external, then main, then internal
    if _valid_url(dsn_ext):
        return "PG_DSN_EXTERNAL", _as_asyncpg_dsn(dsn_ext)
    if _valid_url(dsn_main):
        return (
            "POSTGRES_DSN" if _env_str("POSTGRES_DSN") else "PG_DSN",
            _as_asyncpg_dsn(dsn_main),
        )
    if _valid_url(dsn_int):
        return "PG_DSN_INTERNAL", _as_asyncpg_dsn(dsn_int)

    raise RuntimeError(
        "No valid Postgres DSN found. Set one of: POSTGRES_DSN (preferred), PG_DSN, "
        "PG_DSN_EXTERNAL, PG_DSN_INTERNAL or pass --dsn."
    )


# ──────────────────────────────────────────────────────────────
# Time parsing
# ──────────────────────────────────────────────────────────────


def _parse_dateish(s: str | None) -> datetime | None:
    """
    Accepts:
      - YYYY-MM-DD (treated as 00:00:00 UTC)
      - ISO8601 datetime (Z or +00:00). If tz missing, assumed UTC.
    Returns tz-aware UTC datetime.
    """
    if not s:
        return None
    s = s.strip()
    try:
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            d = date.fromisoformat(s)
            return datetime(d.year, d.month, d.day, tzinfo=UTC)
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except Exception as e:
        raise ValueError(f"Invalid date/datetime '{s}': {e}") from e


def _iso(dt: Any) -> Any:
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC).isoformat()
    return dt


# ──────────────────────────────────────────────────────────────
# Query builder
# ──────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Query:
    sql: str
    params: dict[str, Any]


def _clamp_limit(n: int) -> int:
    try:
        n = int(n)
    except Exception:
        n = 10
    return max(1, min(n, MAX_LIMIT))


def _build_base_where(
    *,
    token: str | None,
    symbol: str | None,
    side: str | None,
    since: datetime | None,
    until: datetime | None,
) -> tuple[str, dict[str, Any]]:
    where: list[str] = []
    params: dict[str, Any] = {}

    if token:
        where.append(f"{_qident('token')} = :token")
        params["token"] = token.strip()

    if symbol:
        where.append(f"{_qident('symbol')} = :symbol")
        params["symbol"] = symbol.strip()

    if side:
        where.append(f"{_qident('side')} = :side")
        params["side"] = side.strip().lower()

    # trades.ts is typically TIMESTAMP WITHOUT TIME ZONE; pass naive UTC.
    if since:
        where.append(f"{_qident('ts')} >= :since")
        params["since"] = since.astimezone(UTC).replace(tzinfo=None)

    if until:
        where.append(f"{_qident('ts')} <= :until")
        params["until"] = until.astimezone(UTC).replace(tzinfo=None)

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    return where_sql, params


def _validate_columns(cols: Sequence[str]) -> list[str]:
    out: list[str] = []
    for c in cols:
        out.append(_qident(c))
    return out


def _build_query(
    *,
    table: str,
    columns: Sequence[str],
    token: str | None,
    symbol: str | None,
    side: str | None,
    since: datetime | None,
    until: datetime | None,
    limit: int,
    oldest_first: bool,
) -> Query:
    qt = _qtable(table)
    qcols = _validate_columns(columns)
    where_sql, params = _build_base_where(
        token=token, symbol=symbol, side=side, since=since, until=until
    )
    order = "ASC" if oldest_first else "DESC"

    sql = f"""
        SELECT {", ".join(qcols)}
        FROM {qt}
        {where_sql}
        ORDER BY {_qident("ts")} {order}, {_qident("id")} {order}
        LIMIT :limit
    """.strip()
    params["limit"] = _clamp_limit(limit)
    return Query(sql=sql, params=params)


def _build_follow_query(
    *,
    table: str,
    columns: Sequence[str],
    token: str | None,
    symbol: str | None,
    side: str | None,
    until: datetime | None,
    watermark_ts: datetime,
    watermark_id: int,
    limit: int,
) -> Query:
    qt = _qtable(table)
    qcols = _validate_columns(columns)
    where_sql, params = _build_base_where(
        token=token, symbol=symbol, side=side, since=None, until=until
    )

    # Strictly newer than watermark
    extra = f"({_qident('ts')} > :wm_ts OR ({_qident('ts')} = :wm_ts AND {_qident('id')} > :wm_id))"
    params["wm_ts"] = watermark_ts.astimezone(UTC).replace(tzinfo=None)
    params["wm_id"] = int(watermark_id)

    if where_sql:
        where_sql = f"{where_sql} AND {extra}"
    else:
        where_sql = f"WHERE {extra}"

    sql = f"""
        SELECT {", ".join(qcols)}
        FROM {qt}
        {where_sql}
        ORDER BY {_qident("ts")} ASC, {_qident("id")} ASC
        LIMIT :limit
    """.strip()
    params["limit"] = _clamp_limit(limit)
    return Query(sql=sql, params=params)


# ──────────────────────────────────────────────────────────────
# Output formatting
# ──────────────────────────────────────────────────────────────


def _rows_to_dicts(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for m in rows:
        d = dict(m)
        if "ts" in d:
            d["ts"] = _iso(d["ts"])
        for k in ("amount", "entry", "current", "profit"):
            if k in d and d[k] is not None:
                with contextlib.suppress(Exception):
                    d[k] = float(d[k])
        out.append(d)
    return out


def _print_table(dict_rows: list[dict[str, Any]], cols: Sequence[str]) -> None:
    if not dict_rows:
        print("(no rows)")
        return

    widths: dict[str, int] = {}
    for c in cols:
        maxv = len(str(c))
        for r in dict_rows:
            v = r.get(c, "")
            maxv = max(maxv, len(str(v)))
        widths[c] = maxv

    header = " | ".join(f"{c:<{widths[c]}}" for c in cols)
    sep = "-+-".join("-" * widths[c] for c in cols)
    print(header)
    print(sep)
    for r in dict_rows:
        line = " | ".join(f"{r.get(c, '')!s:<{widths[c]}}" for c in cols)
        print(line)


def _write_csv(
    path: str | None,
    dict_rows: list[dict[str, Any]],
    cols: Sequence[str],
    *,
    append: bool,
) -> Path | None:
    if not path or not dict_rows:
        return None
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    write_header = not (append and p.exists())
    mode = "a" if append else "w"
    with p.open(mode, newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(cols), extrasaction="ignore")
        if write_header:
            w.writeheader()
        w.writerows(dict_rows)
    return p


def _print_json(dict_rows: list[dict[str, Any]]) -> None:
    print(json.dumps(dict_rows, indent=2, default=str))


def _print_jsonl(dict_rows: list[dict[str, Any]]) -> None:
    for r in dict_rows:
        print(json.dumps(r, default=str, ensure_ascii=False))


# ──────────────────────────────────────────────────────────────
# DB helpers
# ──────────────────────────────────────────────────────────────


def _make_engine(dsn: str) -> AsyncEngine:
    # No extra env keys here (contract safety)
    return create_async_engine(
        dsn,
        echo=False,
        pool_pre_ping=True,
        pool_size=2,
        max_overflow=0,
        pool_recycle=300,
    )


async def _ping(conn: AsyncConnection) -> None:
    await conn.execute(text("SELECT 1"))


async def _fetch(conn: AsyncConnection, q: Query) -> list[dict[str, Any]]:
    res = await conn.execute(text(q.sql), q.params)
    return [dict(r) for r in res.mappings().all()]


def _now_s() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


# ──────────────────────────────────────────────────────────────
# Tail logic
# ──────────────────────────────────────────────────────────────


async def _run_once(conn: AsyncConnection, args: argparse.Namespace) -> list[dict[str, Any]]:
    since = _parse_dateish(args.since)
    until = _parse_dateish(args.until)

    q = _build_query(
        table=args.table,
        columns=args.columns,
        token=args.token,
        symbol=args.symbol,
        side=args.side,
        since=since,
        until=until,
        limit=args.limit,
        oldest_first=bool(args.reverse),
    )

    rows = await _fetch(conn, q)
    dict_rows = _rows_to_dicts(rows)

    if args.jsonl:
        _print_jsonl(dict_rows)
    elif args.json:
        _print_json(dict_rows)
    else:
        _print_table(dict_rows, args.columns)

    p = _write_csv(args.out, dict_rows, args.columns, append=False)
    if p and not (args.json or args.jsonl):
        print(f"✅ CSV written -> {p}")

    return rows


def _compute_watermark(
    rows: list[dict[str, Any]], *, newest_first: bool
) -> tuple[datetime | None, int]:
    if not rows:
        return None, 0

    r = rows[0] if newest_first else rows[-1]
    ts = r.get("ts")
    rid = r.get("id")
    wm_id = int(rid) if rid is not None else 0

    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        return ts.astimezone(UTC), wm_id

    return None, wm_id


async def _follow(conn: AsyncConnection, args: argparse.Namespace) -> None:
    since = _parse_dateish(args.since)
    until = _parse_dateish(args.until)

    # Initial window
    initial_q = _build_query(
        table=args.table,
        columns=args.columns,
        token=args.token,
        symbol=args.symbol,
        side=args.side,
        since=since,
        until=until,
        limit=args.limit,
        oldest_first=bool(args.reverse),
    )
    initial_rows = await _fetch(conn, initial_q)
    dict_rows = _rows_to_dicts(initial_rows)

    if args.jsonl:
        _print_jsonl(dict_rows)
    elif args.json:
        _print_json(dict_rows)
    else:
        _print_table(dict_rows, args.columns)

    _write_csv(args.out, dict_rows, args.columns, append=False)

    newest_first = not bool(args.reverse)
    wm_ts, wm_id = _compute_watermark(initial_rows, newest_first=newest_first)

    # If no rows exist yet, start watermark at epoch to avoid refetching the same earliest rows forever.
    wm_ts_eff = wm_ts or _EPOCH_UTC

    interval = max(MIN_INTERVAL, float(args.interval))
    if not (args.json or args.jsonl):
        print(
            f"[pg_tail] {_now_s()} follow=ON interval={interval:.1f}s watermark=({wm_ts},{wm_id})  Ctrl+C to stop"
        )

    while True:
        await asyncio.sleep(interval)

        fq = _build_follow_query(
            table=args.table,
            columns=args.columns,
            token=args.token,
            symbol=args.symbol,
            side=args.side,
            until=until,
            watermark_ts=wm_ts_eff,
            watermark_id=wm_id,
            limit=args.limit,
        )

        try:
            new_rows = await _fetch(conn, fq)
        except Exception as e:
            if not (args.json or args.jsonl):
                print(f"⚠ follow query failed: {e}")
            continue

        if not new_rows:
            continue

        dict_new = _rows_to_dicts(new_rows)

        if args.jsonl:
            _print_jsonl(dict_new)
        elif args.json:
            _print_json(dict_new)
        else:
            _print_table(dict_new, args.columns)

        _write_csv(args.out, dict_new, args.columns, append=True)

        # Follow queries are ASC, so the last row is newest.
        wm_ts, wm_id = _compute_watermark(new_rows, newest_first=False)
        wm_ts_eff = wm_ts or wm_ts_eff


# ──────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────


def _parse_columns(s: str | None) -> list[str]:
    if not s:
        return list(DEFAULT_COLUMNS)
    cols = [c.strip() for c in s.split(",") if c.strip()]
    return cols or list(DEFAULT_COLUMNS)


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Query/tail rows from Postgres trades table (async).")

    gdsn = ap.add_mutually_exclusive_group()
    gdsn.add_argument("--external", action="store_true", help="Force PG_DSN_EXTERNAL.")
    gdsn.add_argument("--internal", action="store_true", help="Force PG_DSN_INTERNAL.")
    ap.add_argument("--dsn", type=str, default="", help="Explicit DSN (overrides env).")

    ap.add_argument(
        "--table", type=str, default=DEFAULT_TABLE, help="Table name (default: trades)."
    )
    ap.add_argument(
        "--columns",
        type=_parse_columns,
        default=list(DEFAULT_COLUMNS),
        help="Comma-separated columns to select (default canonical trades columns).",
    )

    ap.add_argument("--limit", type=int, default=10, help="Max rows per fetch (default 10).")
    ap.add_argument("--token", type=str, default=None, help="Filter by token mint.")
    ap.add_argument("--symbol", type=str, default=None, help="Filter by symbol (e.g., USDC).")
    ap.add_argument(
        "--side", type=str, choices=["buy", "sell"], default=None, help="Filter by trade side."
    )
    ap.add_argument(
        "--since", type=str, default=None, help="YYYY-MM-DD or ISO datetime (inclusive)."
    )
    ap.add_argument(
        "--until", type=str, default=None, help="YYYY-MM-DD or ISO datetime (inclusive)."
    )

    ap.add_argument(
        "--reverse", action="store_true", help="Oldest first (ASC) instead of newest first (DESC)."
    )

    ap.add_argument("--json", action="store_true", help="Print JSON array.")
    ap.add_argument("--jsonl", action="store_true", help="Print JSON Lines (one object per line).")
    ap.add_argument("--out", type=str, default=None, help="Optional CSV output path (writes rows).")

    ap.add_argument("--follow", action="store_true", help="Tail mode: poll for new rows.")
    ap.add_argument(
        "--interval", type=float, default=5.0, help="Poll seconds for --follow (default 5.0)."
    )

    return ap


async def _run(args: argparse.Namespace) -> int:
    args.limit = _clamp_limit(args.limit)

    # Validate table & columns early (clear errors before DB connect)
    args.table = str(args.table).strip() or DEFAULT_TABLE
    _qtable(args.table)  # raises if invalid
    for c in args.columns:
        _qident(c)  # raises if invalid

    label, dsn = _pick_dsn(
        force_external=bool(args.external),
        force_internal=bool(args.internal),
        explicit=str(args.dsn or ""),
    )
    eng = _make_engine(dsn)

    if not (args.json or args.jsonl):
        print(f"Using DSN [{label}] (async)  table={args.table}  limit={args.limit}")

    try:
        async with eng.connect() as conn:
            await _ping(conn)

            if args.follow:
                await _follow(conn, args)
            else:
                await _run_once(conn, args)

        return 0
    finally:
        await eng.dispose()


def main() -> None:
    args = build_arg_parser().parse_args()

    # json + jsonl: jsonl wins
    if args.json and args.jsonl:
        args.json = False

    try:
        asyncio.run(_run(args))
    except KeyboardInterrupt:
        if not (args.json or args.jsonl):
            print("\n⏹ stopped")
        raise SystemExit(0) from None
    except Exception as e:
        print(f"❌ pg_tail failed: {e}")
        raise SystemExit(1) from e


if __name__ == "__main__":
    main()
