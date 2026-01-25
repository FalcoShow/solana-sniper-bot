#!/usr/bin/env python3
"""
scripts/telegram_trades.py

Export trades from Postgres to CSV/JSON and optionally send to Telegram.

Examples:
  py -m scripts.telegram_trades --limit 50 --side sell --external --no-send
  py -m scripts.telegram_trades --since 2025-08-01 --until 2025-08-12 --external
  py -m scripts.telegram_trades --last-hours 24 --format csv --external
  py -m scripts.telegram_trades --format json --no-send --out exports/trades.json
  py -m scripts.telegram_trades --external --limit 25 --no-send --preview 5

Env:
  - PG_DSN                         (preferred single DSN)
  - POSTGRES_DSN / DATABASE_URL    (fallbacks)
  - PG_DSN_EXTERNAL / PG_DSN_INTERNAL (if using --external)
  - TELEGRAM_BOT_TOKEN
  - TELEGRAM_CHAT_ID               (single id or comma-separated list)

Notes:
  - DSN is normalized for SQLAlchemy async (postgresql+asyncpg://).
  - asyncpg does NOT accept libpq sslmode=... as a keyword arg.
    We strip sslmode from the DSN query string and enable SSL using connect_args.
  - Date parsing accepts YYYY-MM-DD or ISO timestamps (with/without Z).
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import csv
import datetime as dt
import json
import os
import re
import ssl
import sys
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests
from dotenv import find_dotenv, load_dotenv
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

# Load .env if present
with contextlib.suppress(Exception):
    load_dotenv(find_dotenv())

UTC = getattr(dt, "UTC", dt.UTC)


# -------------------------
# Helpers: logging (light)
# -------------------------
def _now_utc() -> dt.datetime:
    return dt.datetime.now(UTC)


def _ts() -> str:
    return _now_utc().strftime("%Y-%m-%dT%H:%M:%SZ")


def _log(msg: str) -> None:
    print(f"[telegram_trades] {_ts()} {msg}")


def getenv(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name)
    return v if v not in (None, "") else default


# -------------------------
# DSN resolution + normalize
# -------------------------
def _normalize_async_dsn(dsn: str) -> str:
    """
    Convert postgres DSNs into SQLAlchemy asyncpg DSN.

    Accepts:
      - postgres://user:pass@host/db?...
      - postgresql://user:pass@host/db?...
      - postgresql+asyncpg://...
      - user:pass@host/db?...
    Returns:
      - postgresql+asyncpg://...
    """
    dsn = (dsn or "").strip()
    if not dsn:
        return dsn

    # If no scheme but looks like netloc/path, assume postgres
    if "://" not in dsn and ("@" in dsn or dsn.startswith("dpg-")):
        dsn = "postgresql://" + dsn

    if dsn.startswith("postgres://"):
        dsn = "postgresql://" + dsn[len("postgres://") :]

    if dsn.startswith("postgresql+asyncpg://"):
        return dsn

    if dsn.startswith("postgresql+"):
        # force to base postgresql:// then upgrade to asyncpg
        dsn = "postgresql://" + dsn.split("://", 1)[1]

    if dsn.startswith("postgresql://"):
        return "postgresql+asyncpg://" + dsn[len("postgresql://") :]

    return dsn


def _sanitize_asyncpg_ssl(dsn: str) -> tuple[str, dict[str, Any]]:
    """
    asyncpg doesn't understand libpq's sslmode query param.
    If sslmode=require/verify-* present, strip it and supply an SSLContext via connect_args.
    """
    connect_args: dict[str, Any] = {}
    dsn = (dsn or "").strip()
    if not dsn:
        return dsn, connect_args

    try:
        parts = urlsplit(dsn)
        q = dict(parse_qsl(parts.query, keep_blank_values=True))

        sslmode = (q.get("sslmode") or "").lower().strip()
        if sslmode:
            q.pop("sslmode", None)

            if sslmode in ("require", "verify-ca", "verify-full"):
                connect_args["ssl"] = ssl.create_default_context()

        # Also respect common boolean SSL flags if present
        ssl_flag = (q.get("ssl") or q.get("sslrequired") or "").lower().strip()
        if ssl_flag in ("1", "true", "yes", "on"):
            connect_args["ssl"] = ssl.create_default_context()

        new_query = urlencode(q, doseq=True)
        sanitized = urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))
        return sanitized, connect_args
    except Exception:
        return dsn, connect_args


def pick_dsn(use_external: bool, explicit_dsn: str | None = None) -> str | None:
    """
    Prefer explicit DSN, then PG_DSN, then external/internal, then common fallbacks.
    """
    if explicit_dsn:
        return _normalize_async_dsn(explicit_dsn)

    if getenv("PG_DSN"):
        return _normalize_async_dsn(getenv("PG_DSN") or "")

    if use_external and getenv("PG_DSN_EXTERNAL"):
        return _normalize_async_dsn(getenv("PG_DSN_EXTERNAL") or "")

    if (not use_external) and getenv("PG_DSN_INTERNAL"):
        return _normalize_async_dsn(getenv("PG_DSN_INTERNAL") or "")

    for k in ("POSTGRES_DSN", "DATABASE_URL"):
        if getenv(k):
            return _normalize_async_dsn(getenv(k) or "")

    return None


def _redact_dsn(dsn: str) -> str:
    """
    Redact credentials in DSN for logging.
    """
    try:
        parts = urlsplit(dsn)
        netloc = parts.netloc
        if "@" in netloc:
            netloc = netloc.split("@", 1)[1]
        return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))
    except Exception:
        # fallback: best effort
        return dsn.split("@")[-1]


# -------------------------
# Time parsing
# -------------------------
def _to_naive_utc(dtx: dt.datetime) -> dt.datetime:
    """
    Convert aware dt to naive UTC, or treat naive as UTC.
    """
    if dtx.tzinfo is not None:
        return dtx.astimezone(UTC).replace(tzinfo=None)
    return dtx


def _parse_iso_any(s: str) -> dt.datetime | None:
    """
    Accept:
      - YYYY-MM-DD
      - YYYY-MM-DDTHH:MM[:SS][.us][Z|+00:00]
    Returns naive UTC datetime (best-effort).
    """
    if not s:
        return None
    s = s.strip()
    if not s:
        return None

    s2 = s.replace("Z", "+00:00")
    try:
        dtx = dt.datetime.fromisoformat(s2)
        return _to_naive_utc(dtx)
    except Exception:
        pass

    try:
        d = dt.datetime.strptime(s, "%Y-%m-%d").date()
        return dt.datetime(d.year, d.month, d.day)
    except Exception:
        return None


def _normalize_bounds(
    since: str | None,
    until: str | None,
    last_hours: float | None,
) -> tuple[dt.datetime | None, dt.datetime | None]:
    """
    - If last_hours is set, overrides since/until.
    - If until is date-only, expands to end-of-day.
    Returns naive UTC datetimes (safe for most Postgres ts columns).
    """
    if last_hours and last_hours > 0:
        now = _now_utc()
        a = (now - dt.timedelta(hours=float(last_hours))).replace(tzinfo=None)
        b = now.replace(tzinfo=None)
        return a, b

    dt_since = _parse_iso_any(since) if since else None
    dt_until = _parse_iso_any(until) if until else None

    if dt_until and until and len(until.strip()) == 10:
        dt_until = dt_until.replace(hour=23, minute=59, second=59, microsecond=999_999)

    return dt_since, dt_until


# -------------------------
# DB query
# -------------------------
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

_IDENT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_columns(cols: Sequence[str]) -> list[str]:
    """
    Prevent SQL injection via --columns.
    Only allow simple identifiers (no functions, commas, quotes, etc).
    """
    out: list[str] = []
    for c in cols:
        c = (c or "").strip()
        if not c:
            continue
        if not _IDENT_RE.match(c):
            raise ValueError(f"Invalid column name: {c!r}")
        out.append(c)
    return out


def _build_query(
    columns: Sequence[str],
    token: str | None,
    symbol: str | None,
    side: str | None,
    dt_since: dt.datetime | None,
    dt_until: dt.datetime | None,
) -> tuple[str, dict[str, Any]]:
    where: list[str] = []
    params: dict[str, Any] = {}

    if token:
        where.append("token = :token")
        params["token"] = token
    if symbol:
        where.append("symbol = :symbol")
        params["symbol"] = symbol
    if side:
        where.append("side = :side")
        params["side"] = side
    if dt_since:
        where.append("ts >= :since")
        params["since"] = dt_since
    if dt_until:
        where.append("ts <= :until")
        params["until"] = dt_until

    col_sql = ", ".join(columns)
    sql = f"SELECT {col_sql} FROM trades"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY ts DESC LIMIT :limit"
    return sql, params


async def fetch_trades(
    dsn: str,
    limit: int,
    columns: Sequence[str],
    token: str | None,
    symbol: str | None,
    side: str | None,
    since: str | None,
    until: str | None,
    last_hours: float | None,
) -> tuple[list[tuple[Any, ...]], list[str]]:
    dt_since, dt_until = _normalize_bounds(since, until, last_hours)
    sql, params = _build_query(columns, token, symbol, side, dt_since, dt_until)
    params["limit"] = int(limit)

    dsn2, connect_args = _sanitize_asyncpg_ssl(dsn)

    eng = create_async_engine(
        dsn2,
        echo=False,
        pool_pre_ping=True,
        pool_recycle=1800,
        poolclass=NullPool,  # one-shot script: no lingering pool
        connect_args=connect_args,  # SSL context when sslmode=require/verify-*
    )

    try:
        async with eng.connect() as conn:
            res = await conn.execute(text(sql), params)
            rows = res.fetchall()
            # Convert SQLAlchemy Rows to tuples for stable output
            out_rows = [tuple(r) for r in rows]
            return out_rows, list(columns)
    finally:
        await eng.dispose()


# -------------------------
# Output writers
# -------------------------
def write_csv(rows: Sequence[Sequence[Any]], columns: Sequence[str], out_path: str) -> str:
    p = Path(out_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(list(columns))
        for r in rows:
            w.writerow(list(r))
    return str(p)


def write_json(rows: Sequence[Sequence[Any]], columns: Sequence[str], out_path: str) -> str:
    p = Path(out_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    data = [dict(zip(columns, list(r), strict=False)) for r in rows]
    with p.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    return str(p)


# -------------------------
# Telegram sender
# -------------------------
def _split_chat_ids(raw: str) -> list[str]:
    return [x.strip() for x in (raw or "").split(",") if x.strip()]


def tg_send_document(
    bot_token: str, chat_id: str, path: str, caption: str = "", mime: str = "text/csv"
) -> None:
    url = f"https://api.telegram.org/bot{bot_token}/sendDocument"
    with open(path, "rb") as f:
        files = {"document": (os.path.basename(path), f, mime)}
        data = {"chat_id": chat_id, "caption": caption}
        r = requests.post(url, data=data, files=files, timeout=45)
    if r.status_code != 200:
        raise RuntimeError(f"Telegram sendDocument failed {r.status_code}: {r.text}")


def tg_send_message(bot_token: str, chat_id: str, text_msg: str) -> None:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    data = {"chat_id": chat_id, "text": text_msg}
    r = requests.post(url, data=data, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"Telegram sendMessage failed {r.status_code}: {r.text}")


def tg_send_with_retries(fn, attempts: int = 3, base_sleep: float = 1.2) -> None:
    last_err: Exception | None = None
    for i in range(attempts):
        try:
            fn()
            return
        except Exception as e:
            last_err = e
            time.sleep(base_sleep * (2**i))
    raise last_err or RuntimeError("Telegram send failed (unknown)")


# -------------------------
# CLI
# -------------------------
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Export trades from Postgres and optionally send to Telegram."
    )
    ap.add_argument("--limit", type=int, default=50, help="Row limit (default 50)")
    ap.add_argument("--token", help="Filter by token mint")
    ap.add_argument("--symbol", help="Filter by symbol")
    ap.add_argument("--side", choices=["buy", "sell"], help="Filter by side")
    ap.add_argument("--since", help="YYYY-MM-DD or ISO timestamp (UTC assumed if naive)")
    ap.add_argument("--until", help="YYYY-MM-DD or ISO timestamp (UTC assumed if naive)")
    ap.add_argument(
        "--last-hours", type=float, default=None, help="Override since/until with last N hours"
    )
    ap.add_argument("--external", action="store_true", help="Use external DSN (PG_DSN_EXTERNAL)")
    ap.add_argument("--dsn", default=None, help="Explicit DSN override (normalized to asyncpg)")
    ap.add_argument(
        "--columns",
        default=",".join(DEFAULT_COLUMNS),
        help="Comma-separated column list (default is standard trades columns)",
    )
    ap.add_argument("--format", choices=["csv", "json"], default="csv", help="Output format")
    ap.add_argument(
        "--out", default=None, help="Output path (default exports/trades_<ts>.csv|json)"
    )
    ap.add_argument(
        "--no-send", action="store_true", help="Only write output; don't send to Telegram"
    )
    ap.add_argument(
        "--send-mode",
        choices=["document", "message"],
        default="document",
        help="Telegram send mode: document (file) or message (text preview)",
    )
    ap.add_argument("--caption", default="Latest trades", help="Telegram caption/message prefix")
    ap.add_argument("--preview", type=int, default=0, help="Print first N rows to console (0=off)")
    return ap.parse_args()


def _default_out_path(fmt: str) -> str:
    ts = _now_utc().strftime("%Y%m%d_%H%M%S")
    ext = "csv" if fmt == "csv" else "json"
    return f"exports/trades_{ts}.{ext}"


def _render_preview(rows: Sequence[Sequence[Any]], columns: Sequence[str], n: int) -> str:
    n = max(0, int(n))
    if n <= 0:
        return ""
    head = rows[:n]
    lines: list[str] = []
    lines.append(" | ".join(columns))
    lines.append("-" * min(120, max(10, len(lines[0]))))
    for r in head:
        lines.append(" | ".join(str(x) for x in r))
    return "\n".join(lines)


def main() -> None:
    args = parse_args()

    dsn = pick_dsn(args.external, args.dsn)
    if not dsn:
        _log(
            "No DSN found. Set PG_DSN, PG_DSN_EXTERNAL/INTERNAL, POSTGRES_DSN, or DATABASE_URL (or pass --dsn)."
        )
        sys.exit(2)

    try:
        raw_cols = [c.strip() for c in str(args.columns).split(",") if c.strip()]
        columns = _validate_columns(raw_cols)
    except Exception as e:
        _log(f"Invalid --columns: {e}")
        sys.exit(2)

    _log(f"Using DSN: {_redact_dsn(dsn)}")
    _log(
        "Filters: "
        f"token={args.token or '-'} "
        f"symbol={args.symbol or '-'} "
        f"side={args.side or '-'} "
        f"since={args.since or '-'} "
        f"until={args.until or '-'} "
        f"last_hours={args.last_hours or '-'} "
        f"limit={args.limit}"
    )

    try:
        rows, cols = asyncio.run(
            fetch_trades(
                dsn=dsn,
                limit=args.limit,
                columns=columns,
                token=args.token,
                symbol=args.symbol,
                side=args.side,
                since=args.since,
                until=args.until,
                last_hours=args.last_hours,
            )
        )
    except Exception as e:
        _log(f"DB fetch failed: {e}")
        raise

    if not rows:
        _log("No rows found.")
        return

    if args.preview:
        print(_render_preview(rows, cols, args.preview))

    out_path = args.out or _default_out_path(args.format)

    if args.format == "csv":
        path = write_csv(rows, cols, out_path)
        mime = "text/csv"
    else:
        path = write_json(rows, cols, out_path)
        mime = "application/json"

    _log(f"Wrote {len(rows)} rows -> {path}")

    if args.no_send:
        return

    tg_token = getenv("TELEGRAM_BOT_TOKEN")
    chat_raw = getenv("TELEGRAM_CHAT_ID")
    if not tg_token or not chat_raw:
        _log("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID; skipping Telegram send.")
        return

    chat_ids = _split_chat_ids(chat_raw)
    if not chat_ids:
        _log("TELEGRAM_CHAT_ID is empty after parsing; skipping Telegram send.")
        return

    caption = args.caption

    if args.send_mode == "message":
        preview_text = _render_preview(rows, cols, min(10, len(rows)))
        msg = f"{caption}\n\n{preview_text}"
        for cid in chat_ids:
            tg_send_with_retries(lambda cid=cid: tg_send_message(tg_token, cid, msg))
        _log(f"Sent preview message to Telegram ({len(chat_ids)} chat(s)).")
        return

    for cid in chat_ids:
        tg_send_with_retries(
            lambda cid=cid: tg_send_document(tg_token, cid, path, caption=caption, mime=mime)
        )
    _log(f"Sent file to Telegram ({len(chat_ids)} chat(s)).")


if __name__ == "__main__":
    main()
