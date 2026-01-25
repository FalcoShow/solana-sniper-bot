# scripts/trades_viewer.py
import argparse
import asyncio
import csv
import json
import os
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import text

from utils_pkg.core_utils import (
    PG_ENABLED,
    close_resources,
    get_pg_engine,
    init_resources,
)


def _parse_rel_ts(s: str | None) -> datetime | None:
    """
    Parse relative/absolute timestamps.
      - '24h', '7d', '90m'
      - ISO '2025-08-01' or '2025-08-01T12:34:56'
    Returns UTC-aware datetime or None.
    """
    if not s:
        return None
    s = s.strip()
    now = datetime.now(UTC)
    try:
        if s.endswith(("h", "H")):
            n = float(s[:-1])
            return now - timedelta(hours=n)
        if s.endswith(("d", "D")):
            n = float(s[:-1])
            return now - timedelta(days=n)
        if s.endswith(("m", "M")):
            n = float(s[:-1])
            return now - timedelta(minutes=n)
        # Date or datetime
        dt = datetime.fromisoformat(s)
        # If naive, assume UTC
        dt = dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)
        return dt
    except Exception:
        return None


def _row_to_mapping(row) -> dict[str, Any]:
    # SQLAlchemy 2.0: Row has ._mapping
    if hasattr(row, "_mapping"):
        return dict(row._mapping)
    # Fallback
    try:
        return dict(row)
    except Exception:
        # last resort: treat as tuple with unknown columns
        return {str(i): v for i, v in enumerate(row)}


async def _detect_schema() -> tuple[str, list[str]]:
    """
    Detect which trade schema exists:
      - 'new'  -> uses columns: ts, symbol, token, side, sol_amount, price_sol, price_usd, pnl_sol, pnl_usd, reason, signature
      - 'old'  -> uses columns: ts, symbol, token, side, amount, entry, current, profit, reason, signature
    Returns (schema_name, columns)
    """
    eng = get_pg_engine()
    async with eng.connect() as conn:
        res = await conn.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'trades'
                """
            )
        )
        cols = {r[0] for r in res.fetchall()}

    if {"sol_amount", "price_sol", "price_usd", "pnl_sol", "pnl_usd"}.issubset(cols):
        return "new", [
            "ts",
            "symbol",
            "token",
            "side",
            "sol_amount",
            "price_sol",
            "price_usd",
            "pnl_sol",
            "pnl_usd",
            "reason",
            "signature",
        ]
    # legacy
    return "old", [
        "ts",
        "symbol",
        "token",
        "side",
        "amount",
        "entry",
        "current",
        "profit",
        "reason",
        "signature",
    ]


async def _fetch_rows(
    limit: int,
    side: str | None,
    symbol: str | None,
    token: str | None,
    since_dt: datetime | None,
    until_dt: datetime | None,
    min_amount: float | None,
    min_pnl_usd: float | None,
    order: str,
) -> tuple[str, list[str], list[dict[str, Any]]]:
    schema, cols = await _detect_schema()
    eng = get_pg_engine()

    where = ["1=1"]
    params: dict[str, Any] = {}

    if side:
        where.append("side = :side")
        params["side"] = side

    if symbol:
        where.append("symbol ILIKE :symbol")
        params["symbol"] = symbol

    if token:
        where.append("token = :token")
        params["token"] = token

    if since_dt is not None:
        where.append("ts >= :since_ts")
        params["since_ts"] = since_dt

    if until_dt is not None:
        where.append("ts <= :until_ts")
        params["until_ts"] = until_dt

    # Filters that differ by schema
    if min_amount is not None:
        amt_col = "sol_amount" if schema == "new" else "amount"
        where.append(f"{amt_col} >= :min_amount")
        params["min_amount"] = float(min_amount)

    if min_pnl_usd is not None and schema == "new":
        where.append("pnl_usd >= :min_pnl_usd")
        params["min_pnl_usd"] = float(min_pnl_usd)

    order = order.lower()
    if order not in ("asc", "desc"):
        order = "desc"

    select_cols = ", ".join(cols)
    sql = f"SELECT {select_cols} FROM trades WHERE {' AND '.join(where)} ORDER BY ts {order} LIMIT :limit"
    params["limit"] = int(limit)

    async with eng.connect() as conn:
        res = await conn.execute(text(sql), params)
        rows = res.fetchall()

    # Convert to mappings for safe printing/export
    mapped = [_row_to_mapping(r) for r in rows]
    return schema, cols, mapped


def _fmt_table(cols: list[str], rows: list[dict[str, Any]]) -> str:
    # Make a simple fixed-width table
    # Truncate long signature for readability
    def fmt_val(k: str, v: Any) -> str:
        if v is None:
            return ""
        if k == "signature":
            s = str(v)
            return s if len(s) <= 16 else s[:8] + "…" + s[-7:]
        if isinstance(v, float):
            # light formatting for floats
            if abs(v) >= 1:
                return f"{v:.6g}"
            return f"{v:.9g}"
        return str(v)

    # Build matrix
    header = cols[:]
    data = [[fmt_val(k, row.get(k)) for k in cols] for row in rows]

    # Column widths
    widths = [max(len(h), *(len(str(r[i])) for r in data)) for i, h in enumerate(header)]
    sep = " | "
    line = sep.join(h.ljust(widths[i]) for i, h in enumerate(header))
    dash = "-" * len(line)
    out = [line, dash]
    for r in data:
        out.append(sep.join(str(r[i]).ljust(widths[i]) for i in range(len(header))))
    return "\n".join(out)


def _write_csv(path: str, cols: list[str], rows: list[dict[str, Any]]) -> str:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in rows:
            w.writerow([r.get(c) for c in cols])
    return path


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="View recent trades from Postgres")
    ap.add_argument("--limit", type=int, default=50, help="Max rows (default 50)")
    ap.add_argument("--side", choices=["buy", "sell"], default=None)
    ap.add_argument("--symbol", type=str, default=None, help="Symbol filter, supports ILIKE")
    ap.add_argument("--token", type=str, default=None, help="Exact token mint")
    ap.add_argument(
        "--since", type=str, default=None, help="e.g. 24h, 7d, 90m, 2025-08-01, 2025-08-01T12:34:56"
    )
    ap.add_argument(
        "--until", type=str, default=None, help="same formats as --since (default: now)"
    )
    ap.add_argument(
        "--min-amount",
        type=float,
        default=None,
        help="Min trade amount (SOL for new schema, token units for legacy)",
    )
    ap.add_argument(
        "--min-pnl-usd", type=float, default=None, help="Min realized PnL in USD (new schema only)"
    )
    ap.add_argument("--order", choices=["asc", "desc"], default="desc", help="Sort by ts")
    ap.add_argument("--json", action="store_true", help="Emit JSON instead of table")
    ap.add_argument("--csv", type=str, default=None, help="Write CSV to this path")
    return ap.parse_args()


async def main_async(args: argparse.Namespace) -> None:
    await init_resources()
    try:
        if not PG_ENABLED or get_pg_engine() is None:
            print("Postgres is not enabled or engine not available.")
            return

        since_dt = _parse_rel_ts(args.since)
        until_dt = _parse_rel_ts(args.until) or datetime.now(UTC)

        schema, cols, rows = await _fetch_rows(
            limit=args.limit,
            side=args.side,
            symbol=args.symbol,
            token=args.token,
            since_dt=since_dt,
            until_dt=until_dt,
            min_amount=args.min_amount,
            min_pnl_usd=args.min_pnl_usd,
            order=args.order,
        )

        if not rows:
            print("No trades found.")
            return

        if args.csv:
            path = _write_csv(args.csv, cols, rows)
            print(f"✅ Wrote {len(rows)} rows -> {path}")

        if args.json:
            print(json.dumps([{k: r.get(k) for k in cols} for r in rows], indent=2, default=str))
        else:
            print(_fmt_table(cols, rows))

    finally:
        await close_resources()


def main() -> None:
    args = parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
