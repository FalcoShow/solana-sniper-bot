#!/usr/bin/env python3
from __future__ import annotations

"""
scripts/pg_export_csv.py — Export trades from Postgres → CSV/JSONL (P0 DSN contract, V3+++)
===========================================================================================

Goal
----
A production-safe exporter for the canonical `trades` table that:
  • Uses the SAME DSN contract as the rest of the stack (P0 lock-in)
  • Streams results in chunks (no giant fetchall RAM spikes)
  • Writes deterministic CSV/JSONL with a stable schema (default columns match pg_tail)
  • Supports filters: token/symbol/side/since/until + limit
  • Works identically on Render + local

DSN contract (order)
--------------------
1) --dsn (CLI)  (if provided)
2) utils_pkg.ai_db.resolve_postgres_dsn(...) (if available)
3) Env:
     POSTGRES_DSN (preferred), PG_DSN
     PG_DSN_EXTERNAL / PG_DSN_INTERNAL
   plus Render-ish alternates:
     POSTGRES_DSN_EXTERNAL / POSTGRES_DSN_INTERNAL

Async driver normalization
--------------------------
Accepts postgres/postgresql DSNs and upgrades to: postgresql+asyncpg://...

Examples
--------
python -m scripts.pg_export_csv --external --limit 1000 --out exports/trades_latest.csv
python -m scripts.pg_export_csv --since 2025-08-01 --until 2025-08-12 --symbol USDC
python -m scripts.pg_export_csv --side sell --token So11111111111111111111111111111111111111112 --gzip
python -m scripts.pg_export_csv --out - --jsonl --limit 50     # stream JSONL to stdout
python -m scripts.pg_export_csv --out - --gzip --jsonl         # gzip JSONL to stdout (bytes)
"""

import argparse
import asyncio
import contextlib
import csv
import gzip
import io
import json
import os
import re
import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, TextIO

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
# Logging (canonical if present)
# ──────────────────────────────────────────────────────────────


def _get_logger():
    try:
        from logging_setup import get_logger  # type: ignore

        return get_logger()
    except Exception:
        import logging

        return logging.getLogger("pg_export_csv")


log = _get_logger()

# ──────────────────────────────────────────────────────────────
# Defaults
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

MAX_LIMIT = 250_000
DEFAULT_LIMIT = 10_000
DEFAULT_CHUNK = 2_000

# ──────────────────────────────────────────────────────────────
# Safety: identifiers
# ──────────────────────────────────────────────────────────────

_VALID_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _qident(name: str) -> str:
    n = (name or "").strip().strip('"')
    if not _VALID_IDENT.match(n):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return f'"{n}"'


def _qtable(ref: str) -> str:
    """
    Accepts: table or schema.table (no quotes). Returns safely quoted.
    """
    raw = (ref or "").strip().strip('"')
    if not raw:
        raise ValueError("Empty table name")
    if "." in raw:
        s, t = raw.split(".", 1)
        return f"{_qident(s)}.{_qident(t)}"
    return _qident(raw)


def _qcols(cols: Sequence[str]) -> str:
    if not cols:
        raise ValueError("No columns selected")
    return ", ".join(_qident(c) for c in cols)


# ──────────────────────────────────────────────────────────────
# Small helpers
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
    Normalize to SQLAlchemy asyncpg DSN:
      postgres://...                 -> postgresql+asyncpg://...
      postgresql://...               -> postgresql+asyncpg://...
      postgresql+psycopg2://...      -> postgresql+asyncpg://...
      postgresql+psycopg://...       -> postgresql+asyncpg://...
    """
    dsn = (dsn or "").strip()
    if not dsn:
        return dsn

    url: URL = make_url(dsn)
    drv = (url.drivername or "").lower()

    if drv == "postgres":
        drv = "postgresql"

    if drv.startswith("postgresql+asyncpg"):
        return str(url)

    if drv.startswith("postgresql"):
        url = url.set(drivername="postgresql+asyncpg")
        return str(url)

    # Unknown scheme: leave as-is (make_url succeeded); engine may fail later.
    return str(url)


def _pick_dsn(*, force_external: bool, force_internal: bool, explicit: str) -> tuple[str, str]:
    # 1) CLI
    if explicit and _valid_url(explicit):
        return "CLI_DSN", _as_asyncpg_dsn(explicit)

    # 2) Canonical resolver (if available)
    with contextlib.suppress(Exception):
        from utils_pkg.ai_db import resolve_postgres_dsn  # type: ignore

        label, dsn = resolve_postgres_dsn(
            prefer_external=force_external, prefer_internal=force_internal
        )  # type: ignore
        if dsn and _valid_url(dsn):
            return str(label or "AI_DB"), _as_asyncpg_dsn(dsn)

    # 3) Env contract
    dsn_main = _env_str("POSTGRES_DSN", "PG_DSN")
    dsn_ext = _env_str("PG_DSN_EXTERNAL", "POSTGRES_DSN_EXTERNAL")
    dsn_int = _env_str("PG_DSN_INTERNAL", "POSTGRES_DSN_INTERNAL")

    if force_external and _valid_url(dsn_ext):
        return "EXTERNAL", _as_asyncpg_dsn(dsn_ext)
    if force_internal and _valid_url(dsn_int):
        return "INTERNAL", _as_asyncpg_dsn(dsn_int)

    # convenience preference
    if _valid_url(dsn_ext):
        return "EXTERNAL", _as_asyncpg_dsn(dsn_ext)
    if _valid_url(dsn_main):
        return ("POSTGRES_DSN" if _env_str("POSTGRES_DSN") else "PG_DSN"), _as_asyncpg_dsn(dsn_main)
    if _valid_url(dsn_int):
        return "INTERNAL", _as_asyncpg_dsn(dsn_int)

    raise RuntimeError(
        "No valid Postgres DSN found. Set POSTGRES_DSN (preferred) or PG_DSN / PG_DSN_EXTERNAL / PG_DSN_INTERNAL, "
        "or pass --dsn."
    )


def _parse_dateish(s: str | None) -> datetime | None:
    """
    Accepts:
      - YYYY-MM-DD (treated as start-of-day UTC)
      - ISO8601 datetime (Z or offset). If tz missing, assume UTC.
    Returns tz-aware UTC datetime.
    """
    if not s:
        return None
    raw = s.strip()
    try:
        if len(raw) == 10 and raw[4] == "-" and raw[7] == "-":
            d = date.fromisoformat(raw)
            return datetime(d.year, d.month, d.day, tzinfo=UTC)

        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except Exception as e:
        raise ValueError(f"Invalid date/datetime '{raw}': {e}") from e


def _clamp_int(n: int, lo: int, hi: int) -> int:
    try:
        n = int(n)
    except Exception:
        n = lo
    return max(lo, min(hi, n))


def _parse_columns(s: str | None) -> list[str]:
    if not s:
        return list(DEFAULT_COLUMNS)
    cols = [c.strip() for c in s.split(",") if c.strip()]
    return cols or list(DEFAULT_COLUMNS)


def _ensure_parent(path: Path) -> None:
    if path.parent and not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)


def _set_windows_asyncio_policy() -> None:
    with contextlib.suppress(Exception):
        if os.name == "nt" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]


# ──────────────────────────────────────────────────────────────
# Query builder
# ──────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ExportSpec:
    table: str
    columns: Sequence[str]
    token: str | None
    symbol: str | None
    side: str | None
    since: datetime | None
    until: datetime | None
    limit: int
    oldest_first: bool


def _build_sql(spec: ExportSpec) -> tuple[str, dict[str, Any]]:
    where: list[str] = []
    params: dict[str, Any] = {}

    if spec.token:
        where.append(f"{_qident('token')} = :token")
        params["token"] = spec.token.strip()

    if spec.symbol:
        where.append(f"{_qident('symbol')} = :symbol")
        params["symbol"] = spec.symbol.strip()

    if spec.side:
        where.append(f"{_qident('side')} = :side")
        params["side"] = spec.side.strip().lower()

    # trades.ts is typically TIMESTAMP WITHOUT TIME ZONE → pass naive UTC
    if spec.since:
        where.append(f"{_qident('ts')} >= :since")
        params["since"] = spec.since.astimezone(UTC).replace(tzinfo=None)
    if spec.until:
        where.append(f"{_qident('ts')} <= :until")
        params["until"] = spec.until.astimezone(UTC).replace(tzinfo=None)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    order = "ASC" if spec.oldest_first else "DESC"

    cols_sql = _qcols(spec.columns)
    table_sql = _qtable(spec.table)

    sql = f"""
        SELECT {cols_sql}
        FROM {table_sql}
        {where_sql}
        ORDER BY {_qident("ts")} {order}, {_qident("id")} {order}
        LIMIT :limit
    """.strip()

    params["limit"] = _clamp_int(spec.limit, 1, MAX_LIMIT)
    return sql, params


# ──────────────────────────────────────────────────────────────
# Export writers
# ──────────────────────────────────────────────────────────────


class _CsvWriter:
    def __init__(self, fp: TextIO, columns: Sequence[str]):
        self._fp = fp
        self._cols = list(columns)
        # Use \n terminator to avoid Windows stdout blank-line issues.
        self._w = csv.DictWriter(
            fp,
            fieldnames=self._cols,
            extrasaction="ignore",
            lineterminator="\n",
        )
        self._w.writeheader()

    def write_rows(self, rows: Iterable[dict[str, Any]]) -> int:
        n = 0
        for r in rows:
            out = dict(r)
            ts = out.get("ts")
            if isinstance(ts, datetime):
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=UTC)
                out["ts"] = ts.astimezone(UTC).isoformat()
            self._w.writerow(out)
            n += 1
        return n


class _JsonlWriter:
    def __init__(self, fp: TextIO):
        self._fp = fp

    def write_rows(self, rows: Iterable[dict[str, Any]]) -> int:
        n = 0
        for r in rows:
            out = dict(r)
            ts = out.get("ts")
            if isinstance(ts, datetime):
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=UTC)
                out["ts"] = ts.astimezone(UTC).isoformat()
            self._fp.write(json.dumps(out, default=str, ensure_ascii=False) + "\n")
            n += 1
        return n


# ──────────────────────────────────────────────────────────────
# DB execution (streaming)
# ──────────────────────────────────────────────────────────────


def _make_engine(dsn: str) -> AsyncEngine:
    return create_async_engine(
        dsn,
        echo=False,
        pool_pre_ping=True,
        pool_size=int(os.getenv("PG_EXPORT_POOL_SIZE", "2") or "2"),
        max_overflow=int(os.getenv("PG_EXPORT_MAX_OVERFLOW", "0") or "0"),
        pool_recycle=int(os.getenv("PG_EXPORT_POOL_RECYCLE_S", "300") or "300"),
    )


async def _ping(conn: AsyncConnection) -> None:
    await conn.execute(text("SELECT 1"))


def _open_output_text(
    *,
    out_path: str,
    fmt: str,
    gzip_output: bool,
) -> tuple[TextIO, bool, str]:
    """
    Returns (fp, should_close, final_path).
    - out_path == "-" => stdout (or gzip to stdout.buffer).
    - gzip_output True (or .gz suffix) => gzip stream.
    """
    to_stdout = out_path.strip() == "-"
    final_path = "-" if to_stdout else str(Path(out_path).expanduser().resolve())

    # stdout
    if to_stdout:
        if gzip_output:
            # gzip bytes to stdout, wrap as text for csv/jsonl
            gz = gzip.GzipFile(fileobj=sys.stdout.buffer, mode="wb")
            fp = io.TextIOWrapper(gz, encoding="utf-8", newline="")
            return fp, True, final_path
        return sys.stdout, False, final_path

    # file path
    p = Path(final_path)
    _ensure_parent(p)
    if gzip_output or final_path.endswith(".gz"):
        fp2 = gzip.open(p, "wt", encoding="utf-8", newline="")
        return fp2, True, final_path

    fp3 = p.open("w", encoding="utf-8", newline="")
    return fp3, True, final_path


async def export_trades(
    dsn: str,
    spec: ExportSpec,
    *,
    out_path: str,
    fmt: str,
    gzip_output: bool,
    chunk: int,
) -> tuple[int, str]:
    """
    Stream rows from Postgres and write to CSV or JSONL.

    out_path:
      - "-" writes to stdout
      - otherwise path on disk
    """
    eng = _make_engine(dsn)
    sql, params = _build_sql(spec)
    chunk = _clamp_int(chunk, 100, 50_000)

    fp: TextIO | None = None
    should_close = False
    final_path = "-"

    try:
        async with eng.connect() as conn:
            await _ping(conn)

            fp, should_close, final_path = _open_output_text(
                out_path=out_path,
                fmt=fmt,
                gzip_output=gzip_output,
            )

            writer = _CsvWriter(fp, spec.columns) if fmt == "csv" else _JsonlWriter(fp)

            result = await conn.stream(
                text(sql),
                params,
                execution_options={"stream_results": True, "yield_per": chunk},
            )

            total = 0
            buffer: list[dict[str, Any]] = []

            async for row in result.mappings():
                buffer.append(dict(row))
                if len(buffer) >= chunk:
                    total += writer.write_rows(buffer)
                    buffer.clear()

            if buffer:
                total += writer.write_rows(buffer)
                buffer.clear()

            with contextlib.suppress(Exception):
                fp.flush()

            return total, final_path

    finally:
        with contextlib.suppress(Exception):
            await eng.dispose()
        if fp is not None and should_close:
            with contextlib.suppress(Exception):
                fp.close()


# ──────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Export trades table from Postgres to CSV/JSONL (streaming)."
    )

    # DSN
    gdsn = ap.add_mutually_exclusive_group()
    gdsn.add_argument("--external", action="store_true", help="Force PG_DSN_EXTERNAL.")
    gdsn.add_argument("--internal", action="store_true", help="Force PG_DSN_INTERNAL.")
    ap.add_argument("--dsn", type=str, default="", help="Explicit DSN (overrides env).")

    # Selection
    ap.add_argument(
        "--table", type=str, default=DEFAULT_TABLE, help="Table name (default: trades)."
    )
    ap.add_argument(
        "--columns",
        type=_parse_columns,
        default=list(DEFAULT_COLUMNS),
        help="Comma-separated columns (default canonical trades columns).",
    )

    # Filters
    ap.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Max rows to export (default {DEFAULT_LIMIT}).",
    )
    ap.add_argument("--token", type=str, default=None, help="Filter by token mint.")
    ap.add_argument("--symbol", type=str, default=None, help="Filter by symbol.")
    ap.add_argument(
        "--side", type=str, choices=["buy", "sell"], default=None, help="Filter by trade side."
    )
    ap.add_argument(
        "--since", type=str, default=None, help="ts >= (YYYY-MM-DD or ISO datetime; UTC assumed)."
    )
    ap.add_argument(
        "--until", type=str, default=None, help="ts <= (YYYY-MM-DD or ISO datetime; UTC assumed)."
    )
    ap.add_argument(
        "--reverse", action="store_true", help="Oldest first (ASC) instead of newest first (DESC)."
    )

    # Output
    ap.add_argument("--out", type=str, default="", help="Output path. Use '-' for stdout.")
    ap.add_argument(
        "--format", choices=["csv", "jsonl"], default="csv", help="Output format (default csv)."
    )
    ap.add_argument("--jsonl", action="store_true", help="Back-compat: same as --format jsonl.")
    ap.add_argument("--gzip", action="store_true", help="Gzip the output (or use .gz extension).")
    ap.add_argument(
        "--chunk",
        type=int,
        default=DEFAULT_CHUNK,
        help=f"Rows per write chunk (default {DEFAULT_CHUNK}).",
    )

    return ap


def _default_out_path(fmt: str, gzip_output: bool) -> str:
    ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    ext = "csv" if fmt == "csv" else "jsonl"
    name = f"trades_{ts}.{ext}"
    if gzip_output:
        name += ".gz"
    return str(Path("exports") / name)


def main(argv: Sequence[str] | None = None) -> int:
    _set_windows_asyncio_policy()

    args = build_arg_parser().parse_args(list(argv) if argv is not None else None)

    fmt = "jsonl" if bool(getattr(args, "jsonl", False)) else str(args.format)
    limit = _clamp_int(args.limit, 1, MAX_LIMIT)
    since = _parse_dateish(args.since)
    until = _parse_dateish(args.until)

    # Resolve DSN (P0)
    label, dsn = _pick_dsn(
        force_external=bool(args.external), force_internal=bool(args.internal), explicit=args.dsn
    )

    out = (args.out or "").strip()
    if not out:
        out = _default_out_path(fmt, bool(args.gzip))

    spec = ExportSpec(
        table=str(args.table).strip() or DEFAULT_TABLE,
        columns=list(args.columns),
        token=args.token,
        symbol=args.symbol,
        side=args.side,
        since=since,
        until=until,
        limit=limit,
        oldest_first=bool(args.reverse),
    )

    # Human-friendly banner (skip if streaming to stdout)
    if out != "-":
        print(f"Using DSN [{label}]")
        print(f"Exporting up to {spec.limit} rows -> {out}  (format={fmt}, gzip={bool(args.gzip)})")

    try:
        total, final_path = asyncio.run(
            export_trades(
                dsn,
                spec,
                out_path=out,
                fmt=fmt,
                gzip_output=bool(args.gzip),
                chunk=int(args.chunk),
            )
        )
    except KeyboardInterrupt:
        print("\n⏹ stopped")
        return 0
    except Exception as e:
        print(f"❌ export failed: {e}", file=sys.stderr)
        return 1

    if out != "-":
        print(f"✅ exported {total} rows -> {final_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
