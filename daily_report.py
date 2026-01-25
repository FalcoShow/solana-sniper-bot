# daily_report.py
from __future__ import annotations

"""
daily_report.py

God-Tier Daily Report Worker (P0)
=================================

What this does
--------------
A resilient “daily summary” worker that:

1) Reads *daily performance* primarily from Postgres (preferred).
2) Falls back to Redis (legacy / safety net) if PG is unavailable.
3) Sends a Telegram summary (never crashes the process).
4) Optionally pushes a Notion PnL row via utils_pkg.notion_pnl.push_daily_pnl().
5) Dedupes so it only sends once per UTC day.

Why this exists
---------------
If the bot isn’t trading, this report still tells you why:
- halts / budgets
- winrate / rolling DD / HWM
- trade counts
- worker modes (off/shadow/live)

Env
---
Core:
  REPORT_ENABLED=true
  REPORT_HOUR_UTC=23
  REPORT_MINUTE_UTC=59
  REPORT_POLL_SEC=20
  REPORT_SEND_ON_START=false        (if true, sends once immediately for "today")
  REPORT_TZ=UTC                     (only used for display; scheduling stays UTC)
  REPORT_DEDUP_TTL_SEC=93600        (26h)

Data sources:
  Postgres DSN (any one):
    POSTGRES_DSN / PG_DSN / PG_DSN_INTERNAL / PG_DSN_EXTERNAL / DATABASE_URL

  Redis:
    REDIS_URL=redis://localhost:6379

Optional integrations:
  Notion:
    NOTION_ENABLED=1
    NOTION_API_KEY=...
    NOTION_PNL_DB_ID=...

Telegram:
  TELEGRAM_ENABLED=true
  TELEGRAM_BOT_TOKEN=...
  TELEGRAM_CHAT_ID=...

Notes
-----
- Scheduling is based on UTC hour/minute (simple and deterministic on Render).
- Postgres probing is best-effort and safe; if schema differs we fall back to Redis.
"""

import argparse
import asyncio
import contextlib
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

# ──────────────────────────────────────────────────────────────────────────────
# Logging (prefer canonical root logging_setup)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from logging_setup import get_logger  # type: ignore
except Exception:  # pragma: no cover
    import logging

    def get_logger(name: str | None = None):  # type: ignore
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(name or "daily_report")


log = get_logger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Telegram sending (prefer AlertManager -> telegram_utils -> core_utils.send_telegram)
# ──────────────────────────────────────────────────────────────────────────────
SendFn = Callable[[str], Awaitable[None]]
_send_message: SendFn | None = None

try:
    from utils_pkg.alert_manager import get_alert_manager  # type: ignore

    async def _am_send(text: str) -> None:
        am = get_alert_manager()
        await am.send_text(text)

    _send_message = _am_send
except Exception:
    _send_message = None

if _send_message is None:
    try:
        from utils_pkg.telegram_utils import send_message as _tg_send_message  # type: ignore

        async def _tg_send(text: str) -> None:
            await _tg_send_message(text, parse_mode="Markdown", disable_web_page_preview=True)

        _send_message = _tg_send
    except Exception:
        _send_message = None

if _send_message is None:
    try:
        from utils_pkg.core_utils import send_telegram as _legacy_send_telegram  # type: ignore

        async def _legacy_send(text: str) -> None:
            await _legacy_send_telegram(text)

        _send_message = _legacy_send
    except Exception:
        _send_message = None


async def send_report(text: str) -> None:
    """Best-effort report sender. Never raises."""
    try:
        if _send_message is None:
            log.warning("No Telegram sender available; report text:\n%s", text)
            return
        await _send_message(text)
    except Exception as e:
        log.error("send_report failed: %s", e, exc_info=True)


# ──────────────────────────────────────────────────────────────────────────────
# Redis access (prefer core_utils.get_redis_async)
# ──────────────────────────────────────────────────────────────────────────────
try:
    import redis.asyncio as aioredis  # type: ignore
except Exception:  # pragma: no cover
    aioredis = None

try:
    from utils_pkg.core_utils import get_redis_async as _get_redis_async  # type: ignore
except Exception:  # pragma: no cover
    _get_redis_async = None  # type: ignore


async def get_redis():
    if _get_redis_async is not None:
        try:
            return await _get_redis_async()
        except Exception as e:
            log.warning("core_utils.get_redis_async failed, falling back: %s", e)

    if aioredis is None:
        return None
    url = os.getenv("REDIS_URL", "redis://localhost:6379")
    try:
        return aioredis.from_url(url, decode_responses=True)
    except Exception:
        return None


async def r_get_float(r, key: str, default: float = 0.0) -> float:
    try:
        v = await r.get(key)
        return float(v) if v is not None else default
    except Exception:
        return default


async def r_get_str(r, key: str) -> str | None:
    try:
        v = await r.get(key)
        return None if v is None else str(v)
    except Exception:
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Postgres (best-effort)
#   - Prefer psycopg3 async if available (works well on Windows/Py3.13 too)
#   - Fall back to asyncpg if present
# ──────────────────────────────────────────────────────────────────────────────
try:
    import psycopg  # type: ignore
    from psycopg.rows import dict_row  # type: ignore
except Exception:  # pragma: no cover
    psycopg = None  # type: ignore
    dict_row = None  # type: ignore

try:
    import asyncpg  # type: ignore
except Exception:  # pragma: no cover
    asyncpg = None  # type: ignore


def resolve_pg_dsn() -> str | None:
    for k in ("POSTGRES_DSN", "PG_DSN", "PG_DSN_INTERNAL", "PG_DSN_EXTERNAL", "DATABASE_URL"):
        v = (os.getenv(k) or "").strip()
        if v:
            return v
    return None


PgDriver = Literal["psycopg", "asyncpg"]
ProbeMode = Literal["range", "date", "none"]


@dataclass
class _PgConn:
    driver: PgDriver
    conn: Any

    async def close(self) -> None:
        if self.driver == "psycopg":
            with contextlib.suppress(Exception):
                await self.conn.close()
        else:
            with contextlib.suppress(Exception):
                await self.conn.close()

    async def fetchrow(self, sql: str, params: tuple[Any, ...]) -> dict[str, Any] | None:
        if self.driver == "psycopg":
            # psycopg uses %s placeholders (not $1/$2)
            sql2 = sql.replace("$1", "%s").replace("$2", "%s")
            async with self.conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(sql2, params)
                row = await cur.fetchone()
                return None if row is None else dict(row)
        # asyncpg uses $1/$2 placeholders
        row = await self.conn.fetchrow(sql, *params)
        return None if row is None else dict(row)


async def pg_connect() -> _PgConn | None:
    dsn = resolve_pg_dsn()
    if not dsn:
        return None

    # 1) psycopg async (preferred)
    if psycopg is not None:
        last_err: Exception | None = None
        for _ in range(2):
            try:
                c = await psycopg.AsyncConnection.connect(dsn, connect_timeout=10)  # type: ignore[attr-defined]
                return _PgConn("psycopg", c)
            except Exception as e:
                last_err = e
        log.warning("PG connect (psycopg) failed: %s", last_err)

    # 2) asyncpg (fallback)
    if asyncpg is not None:
        last_err2: Exception | None = None
        for ssl_mode in ("require", None):
            try:
                if ssl_mode == "require":
                    c2 = await asyncpg.connect(dsn, ssl="require", timeout=10)
                else:
                    c2 = await asyncpg.connect(dsn, timeout=10)
                return _PgConn("asyncpg", c2)
            except Exception as e:
                last_err2 = e
        log.warning("PG connect (asyncpg) failed: %s", last_err2)

    return None


def _utc_day_bounds(day: datetime) -> tuple[datetime, datetime]:
    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    end = start + timedelta(days=1)
    return start, end


def _build_pg_probes() -> tuple[list[tuple[str, str, ProbeMode]], list[tuple[str, str, ProbeMode]]]:
    """
    Build best-effort probes against common schema variants.

    Returns:
      (probes, pos_probes)
    """
    ts_cols = ("ts", "time", "created_at", "filled_at", "executed_at")
    trade_tables = ("trades", "bot_trades", "trade_fills", "fills")

    probes: list[tuple[str, str, ProbeMode]] = []
    probes.extend(
        [
            (
                f"{table}:{ts_col}:realized_pnl_usd",
                f"""
                SELECT
                  COALESCE(SUM(realized_pnl_usd), 0) AS realized_usd,
                  COUNT(*)::bigint AS trades,
                  COALESCE(SUM(CASE WHEN realized_pnl_usd > 0 THEN 1 ELSE 0 END), 0)::bigint AS wins
                FROM {table}
                WHERE {ts_col} >= $1 AND {ts_col} < $2
                """,
                "range",
            )
            for table in trade_tables
            for ts_col in ts_cols
        ]
    )
    probes.extend(
        [
            (
                f"{table}:{ts_col}:pnl_usd",
                f"""
                SELECT
                  COALESCE(SUM(pnl_usd), 0) AS realized_usd,
                  COUNT(*)::bigint AS trades,
                  COALESCE(SUM(CASE WHEN pnl_usd > 0 THEN 1 ELSE 0 END), 0)::bigint AS wins
                FROM {table}
                WHERE {ts_col} >= $1 AND {ts_col} < $2
                """,
                "range",
            )
            for table in trade_tables
            for ts_col in ts_cols
        ]
    )

    daily_tables = ("daily_pnl", "pnl_daily", "bot_daily_pnl")
    date_cols = ("date", "day", "date_utc")
    probes.extend(
        [
            (
                f"{table}:{date_col}",
                f"""
                SELECT
                  COALESCE(realized_usd, 0) AS realized_usd,
                  COALESCE(unrealized_usd, 0) AS unrealized_usd,
                  COALESCE(trades, 0)::bigint AS trades,
                  COALESCE(wins, 0)::bigint AS wins,
                  COALESCE(open_positions, 0)::bigint AS open_positions
                FROM {table}
                WHERE {date_col} = $1
                LIMIT 1
                """,
                "date",
            )
            for table in daily_tables
            for date_col in date_cols
        ]
    )

    pos_tables = ("positions", "bot_positions", "open_positions")
    open_cols = ("is_open", "open")
    pnl_cols = ("unrealized_pnl_usd", "pnl_unrealized_usd", "unrealized_usd")

    pos_probes: list[tuple[str, str, ProbeMode]] = []
    pos_probes.extend(
        [
            (
                f"{table}:status",
                f"SELECT COUNT(*)::bigint AS open_positions FROM {table} WHERE status = 'open'",
                "none",
            )
            for table in pos_tables
        ]
    )
    pos_probes.extend(
        [
            (
                f"{table}:{open_col}",
                f"SELECT COUNT(*)::bigint AS open_positions FROM {table} WHERE {open_col} = true",
                "none",
            )
            for table in pos_tables
            for open_col in open_cols
        ]
    )
    pos_probes.extend(
        [
            (
                f"{table}:{open_col}:{pnl_col}",
                f"""
                SELECT
                  COUNT(*)::bigint AS open_positions,
                  COALESCE(SUM({pnl_col}), 0) AS unrealized_usd
                FROM {table}
                WHERE {open_col} = true
                """,
                "none",
            )
            for table in pos_tables
            for open_col in open_cols
            for pnl_col in pnl_cols
        ]
    )

    return probes, pos_probes


async def pg_fetch_daily_summary(conn: _PgConn, day_utc: datetime) -> dict[str, Any] | None:
    """
    Best-effort probes against common table/column layouts.

    Returns dict with keys (some may be missing):
      realized_usd, unrealized_usd, trades, wins, losses, open_positions

    If nothing works, returns None.
    """
    start, end = _utc_day_bounds(day_utc)
    probes, pos_probes = _build_pg_probes()

    out: dict[str, Any] = {}

    for _label, sql, mode in probes:
        try:
            if mode == "range":
                row = await conn.fetchrow(sql, (start, end))
            elif mode == "date":
                row = await conn.fetchrow(sql, (start.date(),))
            else:
                row = await conn.fetchrow(sql, ())
            if row is None:
                continue

            realized = float(row.get("realized_usd", 0) or 0)
            trades = int(row.get("trades", 0) or 0)
            wins = int(row.get("wins", 0) or 0)

            out["realized_usd"] = realized
            out["trades"] = trades
            out["wins"] = wins
            out["losses"] = max(0, trades - wins)

            if "unrealized_usd" in row:
                out["unrealized_usd"] = float(row.get("unrealized_usd", 0) or 0)
            if "open_positions" in row:
                out["open_positions"] = int(row.get("open_positions", 0) or 0)
            break
        except Exception:
            continue

    if not out:
        return None

    # optional: open positions + unrealized enrichment
    for _label, sql, _mode in pos_probes:
        try:
            row = await conn.fetchrow(sql, ())
            if row is None:
                continue
            if "open_positions" in row and "open_positions" not in out:
                out["open_positions"] = int(row.get("open_positions", 0) or 0)
            if "unrealized_usd" in row and "unrealized_usd" not in out:
                out["unrealized_usd"] = float(row.get("unrealized_usd", 0) or 0)
        except Exception:
            continue

    return out


# ──────────────────────────────────────────────────────────────────────────────
# Notion PnL push (optional)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from utils_pkg.notion_pnl import push_daily_pnl  # type: ignore
except Exception:  # pragma: no cover
    push_daily_pnl = None  # type: ignore


# ──────────────────────────────────────────────────────────────────────────────
# Legacy risk limits (optional; used for equity/ticket/budget summary)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from risk_manager import current_limits  # type: ignore
except Exception:  # pragma: no cover
    current_limits = None  # type: ignore


# ──────────────────────────────────────────────────────────────────────────────
# Config + helpers
# ──────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class ReportConfig:
    enabled: bool
    hour_utc: int
    minute_utc: int
    poll_sec: float
    send_on_start: bool
    dedup_ttl_sec: int
    display_tz: str


def load_report_config() -> ReportConfig:
    enabled = os.getenv("REPORT_ENABLED", "true").lower() == "true"
    hour_utc = int(os.getenv("REPORT_HOUR_UTC", "23"))
    minute_utc = int(os.getenv("REPORT_MINUTE_UTC", "59"))
    poll_sec = float(os.getenv("REPORT_POLL_SEC", "20"))
    send_on_start = os.getenv("REPORT_SEND_ON_START", "false").lower() == "true"
    dedup_ttl_sec = int(os.getenv("REPORT_DEDUP_TTL_SEC", str(26 * 3600)))
    display_tz = (os.getenv("REPORT_TZ", "UTC") or "UTC").strip()
    return ReportConfig(
        enabled=enabled,
        hour_utc=max(0, min(23, hour_utc)),
        minute_utc=max(0, min(59, minute_utc)),
        poll_sec=max(5.0, poll_sec),
        send_on_start=send_on_start,
        dedup_ttl_sec=max(3600, dedup_ttl_sec),
        display_tz=display_tz,
    )


def _today_tag_utc(dt: datetime | None = None) -> str:
    now = dt or datetime.now(UTC)
    return f"{now.year:04d}{now.month:02d}{now.day:02d}"


def _sent_key(day_tag: str) -> str:
    return f"report:sent:{day_tag}"


def _worker_status(name: str, enabled_var: str, shadow_var: str) -> str:
    en = os.getenv(enabled_var, "true").lower() == "true"
    sh = os.getenv(shadow_var, "false").lower() == "true"
    if not en:
        return f"{name}: off"
    return f"{name}: {'shadow' if sh else 'live'}"


async def _fetch_redis_metrics(r) -> dict[str, Any]:
    """
    Legacy fallback: mirrors the v1 daily_report behavior (Redis-only).
    """
    METRICS_WINRATE20 = "metrics:winrate20"
    METRICS_ROLLING_DD = "metrics:rolling_dd_pct"
    EQUITY_HWM_KEY = "metrics:equity_hwm_usd"
    pnl_key = f"pnl:day:realized_usd:{_today_tag_utc()}"

    realized = await r_get_float(r, pnl_key, 0.0)
    winrate20 = await r_get_float(r, METRICS_WINRATE20, 0.0)
    rolling_dd = await r_get_float(r, METRICS_ROLLING_DD, 0.0)
    hwm = await r_get_float(r, EQUITY_HWM_KEY, 0.0)

    return {
        "realized_usd": realized,
        "winrate20": winrate20,
        "rolling_dd": rolling_dd,
        "equity_hwm_usd": hwm,
    }


async def build_daily_snapshot(day_utc: datetime) -> dict[str, Any]:
    """
    Build a unified snapshot dict suitable for:
      - Telegram daily report
      - Notion push_daily_pnl
    """
    snapshot: dict[str, Any] = {
        "date": day_utc.date().isoformat(),
        "time": datetime.now(UTC).isoformat(),
    }

    # 1) Postgres preferred
    conn = await pg_connect()
    if conn is not None:
        try:
            pg_sum = await pg_fetch_daily_summary(conn, day_utc)
            if pg_sum:
                snapshot.update(pg_sum)
        except Exception as e:
            log.warning("PG summary failed: %s", e, exc_info=True)
        finally:
            await conn.close()

    # 2) Redis fallback / enrichment
    r = await get_redis()
    if r is not None:
        try:
            rm = await _fetch_redis_metrics(r)
            for k, v in rm.items():
                snapshot.setdefault(k, v)

            # optional: unrealized if your system writes it
            for key in (
                f"pnl:day:unrealized_usd:{_today_tag_utc(day_utc)}",
                f"pnl:day:unrealized_usd:{_today_tag_utc()}",
            ):
                u = await r_get_float(r, key, 0.0)
                if u and "unrealized_usd" not in snapshot:
                    snapshot["unrealized_usd"] = u

            # optional SOL price if present
            for key in ("price:sol_usd", "sol:price_usd", "price_usd:sol", "sol_price_usd"):
                p = await r_get_float(r, key, 0.0)
                if p and "price_usd" not in snapshot:
                    snapshot["price_usd"] = p
                    break
        except Exception:
            pass

    # 3) Risk limits (optional)
    if current_limits is not None and r is not None:
        try:
            limits = await current_limits(r)
            if isinstance(limits, dict):
                snapshot["limits"] = limits
        except Exception:
            pass

    # 4) Derived
    realized_usd = float(snapshot.get("realized_usd", 0.0) or 0.0)
    unrealized_usd = float(snapshot.get("unrealized_usd", 0.0) or 0.0)
    trades = int(snapshot.get("trades", 0) or 0)
    wins = int(snapshot.get("wins", 0) or 0)

    snapshot["net_usd"] = realized_usd + unrealized_usd
    snapshot["day_winrate"] = (wins / trades) if trades > 0 else 0.0

    # If notion expects SOL values, try to supply them (convert if we can)
    price_usd = snapshot.get("price_usd")
    if price_usd:
        try:
            px = float(price_usd)
            snapshot.setdefault("realized_sol", realized_usd / px if px > 0 else 0.0)
            snapshot.setdefault("unrealized_sol", unrealized_usd / px if px > 0 else 0.0)
        except Exception:
            pass

    snapshot["workers"] = [
        _worker_status("scalper", "DAYTRADER_ENABLED", "SCALPER_SHADOW_MODE"),
        _worker_status("whales", "WHALE_FEED_ENABLED", "WHALE_SHADOW_MODE"),
        _worker_status("velocity", "VELOCITY_ENABLED", "VELOCITY_SHADOW_MODE"),
        _worker_status("hotlist", "HOTLIST_ENABLED", "HOTLIST_SHADOW_MODE"),
        _worker_status("sniper", "SNIPER_ENABLED", "SNIPER_SHADOW_MODE"),
    ]

    return snapshot


def format_report(snapshot: dict[str, Any]) -> str:
    day = (
        snapshot.get("date")
        or snapshot.get("date_utc")
        or (snapshot.get("time") or "")[:10]
        or "today"
    )
    realized = float(snapshot.get("realized_usd", 0.0) or 0.0)
    unrealized = float(snapshot.get("unrealized_usd", 0.0) or 0.0)
    net = float(snapshot.get("net_usd", realized + unrealized) or 0.0)

    trades = int(snapshot.get("trades", 0) or 0)
    wins = int(snapshot.get("wins", 0) or 0)
    losses = int(snapshot.get("losses", max(0, trades - wins)) or 0)
    day_wr = float(snapshot.get("day_winrate", 0.0) or 0.0) * 100.0

    winrate20 = float(snapshot.get("winrate20", 0.0) or 0.0) * 100.0
    rolling_dd = float(snapshot.get("rolling_dd", 0.0) or 0.0) * 100.0
    hwm = float(snapshot.get("equity_hwm_usd", 0.0) or 0.0)

    limits = snapshot.get("limits") or {}
    equity = limits.get("equity_usd")
    ticket = limits.get("ticket_usd")
    max_conc = limits.get("max_concurrent")
    daily_budget = limits.get("daily_spend_usd")

    open_pos = snapshot.get("open_positions")
    workers = snapshot.get("workers") or []

    stop_after_profit = os.getenv("STOP_AFTER_DAILY_PROFIT_PCT", "")
    spend_cap_pct = os.getenv("DAILY_SPEND_CAP_PCT", "")
    spend_cap_str = ""
    try:
        if spend_cap_pct:
            spend_cap_str = f"{int(float(spend_cap_pct) * 100)}%"
    except Exception:
        spend_cap_str = str(spend_cap_pct)

    lines: list[str] = []
    lines.append(f"📊 *Daily Summary* (UTC {day})")
    lines.append("")
    if equity is not None:
        lines.append(f"• Equity: `${float(equity):,.2f}`  (HWM `${hwm:,.2f}`)")
    else:
        lines.append(f"• Equity HWM: `${hwm:,.2f}`")
    lines.append(
        f"• PnL: realized `${realized:,.2f}`  | unrealized `${unrealized:,.2f}`  | net `${net:,.2f}`"
    )
    lines.append(f"• Trades: `{trades}`  (W `{wins}` / L `{losses}`)  | Day WR: `{day_wr:.1f}%`")
    lines.append(f"• Winrate-20: `{winrate20:.1f}%`  | Rolling DD: `{rolling_dd:.1f}%`")

    if ticket is not None or max_conc is not None or daily_budget is not None:
        t = f"{float(ticket):,.2f}" if ticket is not None else "?"
        mc = f"{int(max_conc)}" if max_conc is not None else "?"
        db = f"{float(daily_budget):,.0f}" if daily_budget is not None else "?"
        lines.append(f"• Limits: ticket `${t}`  | max_conc `{mc}`  | daily_budget `${db}`")

    if open_pos is not None:
        lines.append(f"• Open positions: `{int(open_pos)}`")

    if workers:
        lines.append(f"• Workers: {', '.join(workers)}")

    if stop_after_profit or spend_cap_str:
        extra: list[str] = []
        if stop_after_profit:
            extra.append(f"stop-after-profit `{stop_after_profit}%`")
        if spend_cap_str:
            extra.append(f"spend-cap `{spend_cap_str}`")
        lines.append("• Playbook: " + "  |  ".join(extra))

    return "\n".join(lines)


async def maybe_push_notion(snapshot: dict[str, Any]) -> None:
    if push_daily_pnl is None:
        return
    try:
        # push_daily_pnl reads env NOTION_ENABLED/NOTION_API_KEY/NOTION_PNL_DB_ID
        push_daily_pnl(snapshot)
    except Exception as e:
        log.warning("Notion push failed: %s", e, exc_info=True)


async def send_once_for_day(day_utc: datetime, *, dedup: bool = True) -> bool:
    """
    Returns True if sent, False if skipped due to dedup.
    """
    r = await get_redis()
    day_tag = _today_tag_utc(day_utc)
    sent_key = _sent_key(day_tag)

    if dedup and r is not None:
        try:
            if await r_get_str(r, sent_key) == "1":
                return False
        except Exception:
            pass

    snapshot = await build_daily_snapshot(day_utc)
    text = format_report(snapshot)

    await send_report(text)
    await maybe_push_notion(snapshot)

    if dedup and r is not None:
        with contextlib.suppress(Exception):
            await r.set(sent_key, "1", ex=int(load_report_config().dedup_ttl_sec))

    return True


async def run_worker() -> None:
    cfg = load_report_config()
    if not cfg.enabled:
        log.info("Daily report disabled (REPORT_ENABLED=false)")
        return

    await send_report("📝 Daily report worker started ✅")

    if cfg.send_on_start:
        with contextlib.suppress(Exception):
            await send_once_for_day(datetime.now(UTC), dedup=True)

    while True:
        try:
            now = datetime.now(UTC)
            if now.hour == cfg.hour_utc and now.minute == cfg.minute_utc:
                sent = await send_once_for_day(now, dedup=True)
                if sent:
                    await asyncio.sleep(65)
                    continue
            await asyncio.sleep(cfg.poll_sec)
        except Exception as e:
            log.error("[daily_report] loop error: %s", e, exc_info=True)
            await asyncio.sleep(60)


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────
def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Daily report worker / one-shot sender")
    p.add_argument("--once", action="store_true", help="Send once and exit (dedup respected)")
    p.add_argument("--no-dedup", action="store_true", help="Disable dedup for --once")
    p.add_argument(
        "--date",
        type=str,
        default="",
        help="UTC date YYYY-MM-DD (only for --once). Default: today UTC.",
    )
    return p.parse_args()


def _parse_date_utc(s: str) -> datetime:
    s2 = (s or "").strip()
    if not s2:
        return datetime.now(UTC)
    y, m, d = [int(x) for x in s2.split("-")]
    return datetime(y, m, d, tzinfo=UTC)


async def _main_async() -> None:
    args = _parse_args()
    if args.once:
        day = _parse_date_utc(args.date)
        sent = await send_once_for_day(day, dedup=not args.no_dedup)
        log.info("daily_report --once done (sent=%s)", sent)
        return
    await run_worker()


def main() -> None:
    asyncio.run(_main_async())


if __name__ == "__main__":
    main()
