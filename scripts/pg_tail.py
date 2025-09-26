# scripts/pg_tail.py
import os, sys, argparse, asyncio, csv, json
from datetime import datetime, date, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.engine.url import make_url
from sqlalchemy import text
from dotenv import load_dotenv, find_dotenv


# -----------------------------
# DSN helpers
# -----------------------------
def _valid_dsn(s: str) -> bool:
    if not s or not isinstance(s, str):
        return False
    try:
        make_url(s.strip())
        return True
    except Exception:
        return False


def _pick_dsn(force_external: bool = False, force_internal: bool = False) -> Tuple[Optional[str], Optional[str]]:
    load_dotenv(find_dotenv())
    pg_dsn = (os.getenv("PG_DSN") or "").strip()
    dsn_i  = (os.getenv("PG_DSN_INTERNAL") or "").strip()
    dsn_e  = (os.getenv("PG_DSN_EXTERNAL") or "").strip()

    if force_external and _valid_dsn(dsn_e):
        return "EXTERNAL", dsn_e
    if force_internal and _valid_dsn(dsn_i):
        return "INTERNAL", dsn_i

    # Local convenience: prefer EXTERNAL, then explicit PG_DSN, then INTERNAL
    if _valid_dsn(dsn_e):
        return "EXTERNAL", dsn_e
    if _valid_dsn(pg_dsn):
        return "PG_DSN", pg_dsn
    if _valid_dsn(dsn_i):
        return "INTERNAL", dsn_i
    return None, None


# -----------------------------
# Parsing + formatting helpers
# -----------------------------
def _parse_dateish(s: Optional[str]) -> Optional[datetime]:
    """Accepts 'YYYY-MM-DD' or ISO8601 datetime; returns timezone-aware UTC datetime when possible."""
    if not s:
        return None
    s = s.strip()
    try:
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            # Interpret date as start-of-day UTC
            d = date.fromisoformat(s)
            return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
        # Full ISO; if no tzinfo, assume UTC
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        raise ValueError(f"Invalid date/datetime: {s}")


def _iso(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _as_float(x: Any) -> Optional[float]:
    try:
        return float(x) if x is not None else None
    except Exception:
        return None


# -----------------------------
# Query builder
# -----------------------------
COLUMNS = [
    "id", "ts", "token", "symbol", "side",
    "amount", "entry", "current", "profit", "reason", "signature"
]

def _build_sql(args) -> Tuple[str, Dict[str, Any]]:
    where = []
    params: Dict[str, Any] = {}

    if args.token:
        where.append("token = :token")
        params["token"] = args.token.strip()
    if args.symbol:
        where.append("symbol = :symbol")
        params["symbol"] = args.symbol.strip()
    if args.side:
        where.append("side = :side")
        params["side"] = args.side.strip().lower()

    since_dt = _parse_dateish(args.since)
    until_dt = _parse_dateish(args.until)

    if since_dt:
        where.append("ts >= :since")
        params["since"] = since_dt.replace(tzinfo=None)  # let PG interpret as timestamp w/o tz; your column is TIMESTAMP
    if until_dt:
        where.append("ts <= :until")
        params["until"] = until_dt.replace(tzinfo=None)

    where_sql = ""
    if where:
        where_sql = "WHERE " + " AND ".join(where)

    order = "ASC" if args.reverse else "DESC"

    # LIMIT must be bound to avoid SQL injection and to let driver handle typing
    sql = f"""
        SELECT {", ".join(COLUMNS)}
        FROM trades
        {where_sql}
        ORDER BY ts {order}, id {order}
        LIMIT :limit
    """.strip()
    params["limit"] = int(args.limit)
    return sql, params


# -----------------------------
# Output helpers
# -----------------------------
def _rows_to_dicts(rows) -> List[Dict[str, Any]]:
    out = []
    for r in rows:
        # r is Row; supports ._mapping for dict-like access with column names
        m = dict(r._mapping)
        # normalize datetimes to ISO Z
        if isinstance(m.get("ts"), datetime):
            m["ts"] = _iso(m["ts"])
        # ensure floats
        for k in ("amount", "entry", "current", "profit"):
            if k in m:
                m[k] = _as_float(m[k])
        out.append(m)
    return out


def _print_table(dict_rows: List[Dict[str, Any]], cols: List[str]) -> None:
    if not dict_rows:
        print("(no rows)")
        return
    # widths
    widths = {c: max(len(c), max(len(str(d.get(c, ""))) for d in dict_rows)) for c in cols}
    header = " | ".join(f"{c:<{widths[c]}}" for c in cols)
    sep = "-".join("-" * (widths[c] + 2) for c in cols)
    print(header)
    print(sep)
    for d in dict_rows:
        print(" | ".join(f"{str(d.get(c, '')):<{widths[c]}}" for c in cols))


def _maybe_write_csv(path: Optional[str], dict_rows: List[Dict[str, Any]], cols: List[str], append: bool = False) -> Optional[Path]:
    if not path or not dict_rows:
        return None
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    write_header = not (append and p.exists())
    mode = "a" if append else "w"
    with p.open(mode, newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        if write_header:
            w.writeheader()
        w.writerows(dict_rows)
    return p


# -----------------------------
# Follow (tail) mode
# -----------------------------
async def _tail(conn, args) -> None:
    """Poll for new rows after the newest ts/id in the initial window."""
    cols = COLUMNS
    # Start from initial window
    base_sql, base_params = _build_sql(args)
    res = await conn.execute(text(base_sql), base_params)
    rows = res.fetchall()
    dict_rows = _rows_to_dicts(rows)

    if args.json:
        print(json.dumps(dict_rows, indent=2))
    else:
        _print_table(dict_rows, cols)

    _maybe_write_csv(args.out, dict_rows, cols, append=False)

    # Track watermark
    last_ts: Optional[datetime] = None
    last_id: Optional[int] = None
    if rows:
        last_ts = rows[-1]._mapping["ts"]  # the last row in current order
        last_id = rows[-1]._mapping["id"]

    # Switch to forward order for newer > watermark
    poll_every = max(2.0, float(args.interval))
    while True:
        await asyncio.sleep(poll_every)
        where = []
        params: Dict[str, Any] = {}

        if args.token:
            where.append("token = :token")
            params["token"] = args.token.strip()
        if args.symbol:
            where.append("symbol = :symbol")
            params["symbol"] = args.symbol.strip()
        if args.side:
            where.append("side = :side")
            params["side"] = args.side.strip().lower()

        # only fetch strictly newer than watermark
        if last_ts is not None:
            where.append("(ts > :last_ts OR (ts = :last_ts AND id > :last_id))")
            params["last_ts"] = last_ts.replace(tzinfo=None) if isinstance(last_ts, datetime) else last_ts
            params["last_id"] = last_id if last_id is not None else 0

        where_sql = "WHERE " + " AND ".join(where) if where else ""
        sql = f"""
            SELECT {", ".join(cols)}
            FROM trades
            {where_sql}
            ORDER BY ts ASC, id ASC
            LIMIT :limit
        """.strip()
        params["limit"] = int(args.limit)

        try:
            res = await conn.execute(text(sql), params)
            new_rows = res.fetchall()
        except Exception as e:
            print("⚠ follow query failed:", e)
            continue

        if not new_rows:
            continue

        dict_new = _rows_to_dicts(new_rows)
        if args.json:
            print(json.dumps(dict_new, indent=2))
        else:
            _print_table(dict_new, cols)

        _maybe_write_csv(args.out, dict_new, cols, append=True)

        # Update watermark
        last_ts = new_rows[-1]._mapping["ts"]
        last_id = new_rows[-1]._mapping["id"]


# -----------------------------
# Main
# -----------------------------
async def _run(args):
    label, dsn = _pick_dsn(force_external=args.external, force_internal=args.internal)
    if not dsn:
        print("❌ No valid PG DSN found. Set PG_DSN / PG_DSN_EXTERNAL / PG_DSN_INTERNAL.")
        return

    print(f"Using DSN [{label}]")
    eng = create_async_engine(dsn, echo=False, pool_pre_ping=True)

    try:
        async with eng.connect() as conn:
            if args.follow:
                await _tail(conn, args)
                return

            sql, params = _build_sql(args)
            try:
                res = await conn.execute(text(sql), params)
                rows = res.fetchall()
            except Exception as e:
                print("⚠ Query failed:", e)
                return

            dict_rows = _rows_to_dicts(rows)
            if args.json:
                print(json.dumps(dict_rows, indent=2))
            else:
                _print_table(dict_rows, COLUMNS)

            p = _maybe_write_csv(args.out, dict_rows, COLUMNS, append=False)
            if p:
                print(f"✅ CSV written -> {p}")
    finally:
        await eng.dispose()


def main():
    ap = argparse.ArgumentParser(description="Tail or query recent rows from the trades table.")
    gdsn = ap.add_mutually_exclusive_group()
    gdsn.add_argument("--external", action="store_true", help="force PG_DSN_EXTERNAL")
    gdsn.add_argument("--internal", action="store_true", help="force PG_DSN_INTERNAL")

    ap.add_argument("--limit", type=int, default=10, help="max rows to show each fetch (default 10)")
    ap.add_argument("--token", type=str, help="filter by token mint")
    ap.add_argument("--symbol", type=str, help="filter by symbol (e.g., USDC)")
    ap.add_argument("--side", type=str, choices=["buy", "sell"], help="filter by trade side")

    ap.add_argument("--since", type=str, help="YYYY-MM-DD or ISO datetime (inclusive)")
    ap.add_argument("--until", type=str, help="YYYY-MM-DD or ISO datetime (inclusive)")
    ap.add_argument("--reverse", action="store_true", help="oldest first instead of newest first")

    ap.add_argument("--json", action="store_true", help="print JSON instead of table")
    ap.add_argument("--out", type=str, help="optional CSV output path, e.g., exports/trades_latest.csv")

    ap.add_argument("--follow", action="store_true", help="tail mode: keep polling for new rows")
    ap.add_argument("--interval", type=float, default=5.0, help="poll seconds for --follow (default 5.0)")

    args = ap.parse_args()

    try:
        asyncio.run(_run(args))
    except KeyboardInterrupt:
        print("\n⏹ stopped")


if __name__ == "__main__":
    main()
