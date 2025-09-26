#!/usr/bin/env python3
"""
Generate a CSV from the Postgres trades table and (optionally) send it to Telegram.

Examples:
  python -m scripts.telegram_trades --limit 50 --side sell --symbol USDC --external --no-send
  python -m scripts.telegram_trades --since 2025-08-01 --until 2025-08-12 --external
"""
import os, sys, csv, argparse, asyncio, datetime, pathlib, requests
from typing import Optional, Tuple
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

def getenv(name, default=None):
    v = os.getenv(name)
    return v if v not in (None, "") else default

def pick_dsn(use_external: bool) -> Optional[str]:
    if getenv("PG_DSN"):
        return getenv("PG_DSN")
    return getenv("PG_DSN_EXTERNAL") if use_external else getenv("PG_DSN_INTERNAL")

# --- robust date/time parsing for --since / --until ---
def _parse_iso_any(s: str) -> Optional[datetime.datetime]:
    """Accepts YYYY-MM-DD, YYYY-MM-DDTHH:MM[:SS][.us][Z|+00:00]. Returns naive UTC datetime."""
    if not s:
        return None
    s = s.strip().replace("Z", "+00:00")
    try:
        dt = datetime.datetime.fromisoformat(s)
        if dt.tzinfo:
            dt = dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return dt
    except Exception:
        pass
    try:
        d = datetime.datetime.strptime(s, "%Y-%m-%d").date()
        return datetime.datetime(d.year, d.month, d.day)
    except Exception:
        return None

def _normalize_bounds(since: Optional[str], until: Optional[str]) -> Tuple[Optional[datetime.datetime], Optional[datetime.datetime]]:
    dt_since = _parse_iso_any(since) if since else None
    dt_until = _parse_iso_any(until) if until else None
    if dt_until and (until and len(until.strip()) == 10):  # date-only => end of day
        dt_until = dt_until.replace(hour=23, minute=59, second=59, microsecond=999999)
    return dt_since, dt_until
# ------------------------------------------------------

async def fetch_trades(
    dsn: str,
    limit: int,
    token: Optional[str],
    symbol: Optional[str],
    side: Optional[str],
    since: Optional[str],
    until: Optional[str],
):
    eng = create_async_engine(dsn, echo=False, pool_pre_ping=True)
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

    dt_since, dt_until = _normalize_bounds(since, until)
    if dt_since:
        where.append("ts >= :since")
        params["since"] = dt_since
    if dt_until:
        where.append("ts <= :until")
        params["until"] = dt_until

    sql = (
        "SELECT id, ts, token, symbol, side, amount, entry, current, profit, reason, signature "
        "FROM trades"
    )
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY ts DESC LIMIT :limit"
    params["limit"] = limit

    async with eng.connect() as conn:
        res = await conn.execute(text(sql), params)
        rows = res.fetchall()
    await eng.dispose()
    return rows

def write_csv(rows, out_path: str) -> str:
    p = pathlib.Path(out_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id","ts","token","symbol","side","amount","entry","current","profit","reason","signature"])
        for r in rows:
            w.writerow(list(r))  # SA Row is tuple-like
    return str(p)

def tg_send_file(token: str, chat_id: str, path: str, caption: str = ""):
    url = f"https://api.telegram.org/bot{token}/sendDocument"
    with open(path, "rb") as f:
        files = {"document": (os.path.basename(path), f, "text/csv")}
        data = {"chat_id": chat_id, "caption": caption}
        r = requests.post(url, data=data, files=files, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"Telegram sendDocument failed {r.status_code}: {r.text}")
    return r.json()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=50, help="Row limit (default 50)")
    ap.add_argument("--token", help="Filter by token mint")
    ap.add_argument("--symbol", help="Filter by symbol")
    ap.add_argument("--side", choices=["buy","sell"], help="Filter by side")
    ap.add_argument("--since", help="YYYY-MM-DD or ISO timestamp")
    ap.add_argument("--until", help="YYYY-MM-DD or ISO timestamp")
    ap.add_argument("--external", action="store_true", help="Use PG_DSN_EXTERNAL")
    ap.add_argument("--out", default=None, help="Output CSV path (default exports/trades_<ts>.csv)")
    ap.add_argument("--no-send", action="store_true", help="Only write CSV, don't send to Telegram")
    ap.add_argument("--caption", default="Latest trades", help="Telegram caption")
    args = ap.parse_args()

    dsn = pick_dsn(args.external)
    if not dsn:
        print("No DSN found. Set PG_DSN, PG_DSN_EXTERNAL or PG_DSN_INTERNAL.", file=sys.stderr)
        sys.exit(2)

    rows = asyncio.run(fetch_trades(dsn, args.limit, args.token, args.symbol, args.side, args.since, args.until))
    if not rows:
        print("No rows found.")
        return

    ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_path = args.out or f"exports/trades_{ts}.csv"
    path = write_csv(rows, out_path)
    print(f"CSV written -> {path}")

    if args.no_send:
        return

    tg_token = getenv("TELEGRAM_BOT_TOKEN")
    chat_id = getenv("TELEGRAM_CHAT_ID")
    if not tg_token or not chat_id:
        print("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID. Skipping send.", file=sys.stderr)
        return

    resp = tg_send_file(tg_token, chat_id, path, caption=args.caption)
    mid = resp.get("result", {}).get("message_id")
    print(f"✅ Sent to Telegram (message_id={mid})")

if __name__ == "__main__":
    main()
