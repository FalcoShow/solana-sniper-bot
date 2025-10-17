#!/usr/bin/env python3
"""
Export recent trades from Postgres to CSV.

Examples:
  python scripts/pg_export_csv.py --external --limit 100 --out exports/trades_latest.csv
  python scripts/pg_export_csv.py --since 2025-08-01 --until 2025-08-12 --symbol USDC
"""

import os
import sys
import csv
import argparse
import asyncio
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv, find_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from sqlalchemy import text

load_dotenv(find_dotenv())

# ---------- Helpers ----------
def getenv(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name)
    return v if v not in (None, "") else default

def choose_dsn(use_external: bool | None, use_internal: bool | None) -> tuple[str | None, str]:
    """Pick a DSN in this order:
       explicit PG_DSN, then EXTERNAL/INTERNAL by flags/env, else None.
    """
    explicit = getenv("PG_DSN")
    dsn_ext = getenv("PG_DSN_EXTERNAL")
    dsn_int = getenv("PG_DSN_INTERNAL")
    label = "PG_DSN"

    if explicit:
        return explicit, label

    # Flags have priority over presence
    if use_external:
        if dsn_ext:
            return dsn_ext, "PG_DSN_EXTERNAL"
        # fall back to internal if external missing
        if dsn_int:
            return dsn_int, "PG_DSN_INTERNAL"
    if use_internal:
        if dsn_int:
            return dsn_int, "PG_DSN_INTERNAL"
        if dsn_ext:
            return dsn_ext, "PG_DSN_EXTERNAL"

    # No flags: prefer EXTERNAL if present
    if dsn_ext:
        return dsn_ext, "PG_DSN_EXTERNAL"
    if dsn_int:
        return dsn_int, "PG_DSN_INTERNAL"
    return None, ""

def parse_date(s: str | None) -> datetime | None:
    if not s:
        return None
    s = s.strip()
    # Accept YYYY-MM-DD or full ISO timestamps
    fmts = ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S")
    for f in fmts:
        try:
            dt = datetime.strptime(s, f)
            return dt
        except ValueError:
            continue
    # last resort: try fromisoformat (may raise)
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        raise ValueError(f"Could not parse date/time: {s}")

def ensure_parent_dir(path: Path) -> None:
    if path.parent and not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)

# ---------- DB ----------
async def fetch_rows(
    dsn: str,
    limit: int,
    token: str | None,
    symbol: str | None,
    side: str | None,
    since: datetime | None,
    until: datetime | None,
):
    eng: AsyncEngine = create_async_engine(dsn, pool_pre_ping=True, future=True)

    where = []
    params = {}

    if token:
        where.append("token = :token")
        params["token"] = token
    if symbol:
        where.append("symbol = :symbol")
        params["symbol"] = symbol
    if side:
        where.append("side = :side")
        params["side"] = side

    if since:
        where.append("ts >= :since")
        params["since"] = since
    if until:
        where.append("ts <= :until")
        params["until"] = until

    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    sql = f"""
        SELECT id, ts, token, symbol, side, amount, entry, current, profit, reason, signature
        FROM trades
        {where_sql}
        ORDER BY ts DESC
        LIMIT :limit
    """
    params["limit"] = max(1, min(100_000, limit))

    try:
        async with eng.connect() as conn:
            res = await conn.execute(text(sql), params)
            # Prefer dict-like rows (SQLAlchemy 1.4+/2.0)
            try:
                rows = res.mappings().all()
            except Exception:
                # Fallback to tuples → dicts
                tuples = res.fetchall()
                keys = res.keys()
                rows = [dict(zip(keys, t)) for t in tuples]
        await eng.dispose()
        return rows
    except Exception:
        await eng.dispose()
        raise

# ---------- CSV ----------
FIELDS = [
    "id", "ts", "token", "symbol", "side",
    "amount", "entry", "current", "profit", "reason", "signature"
]

def write_csv(rows, out_path: Path) -> Path:
    ensure_parent_dir(out_path)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        wr = csv.writer(f)
        wr.writerow(FIELDS)
        for r in rows:
            wr.writerow([r.get(k) if isinstance(r, dict) else "" for k in FIELDS])
    return out_path

# ---------- CLI ----------
def main():
    ap = argparse.ArgumentParser(description="Export trades table to CSV.")
    ap.add_argument("--limit", type=int, default=1000, help="max rows (default 1000)")
    ap.add_argument("--token", help="filter by token mint")
    ap.add_argument("--symbol", help="filter by symbol")
    ap.add_argument("--side", choices=["buy", "sell"], help="filter by side")
    ap.add_argument("--since", help="filter ts >= (YYYY-MM-DD or ISO)")
    ap.add_argument("--until", help="filter ts <= (YYYY-MM-DD or ISO)")
    ap.add_argument("--external", action="store_true", help="force PG_DSN_EXTERNAL")
    ap.add_argument("--internal", action="store_true", help="force PG_DSN_INTERNAL")
    ap.add_argument("--out", default=None, help="output CSV path")
    args = ap.parse_args()

    dsn, label = choose_dsn(args.external, args.internal)
    if not dsn:
        print("No DSN found. Set PG_DSN, PG_DSN_EXTERNAL or PG_DSN_INTERNAL.", file=sys.stderr)
        sys.exit(2)

    since = parse_date(args.since) if args.since else None
    until = parse_date(args.until) if args.until else None

    rows = asyncio.run(fetch_rows(dsn, args.limit, args.token, args.symbol, args.side, since, until))

    if args.out:
        out_path = Path(args.out)
    else:
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        out_path = Path("exports") / f"trades_{ts}.csv"

    print(f"Using DSN [{label}]")
    print(f"Exporting {len(rows)} rows -> {out_path}")
    path = write_csv(rows, out_path)
    print(f"✅ Wrote {path}")

if __name__ == "__main__":
    main()
