# -*- coding: utf-8 -*-
from __future__ import annotations

"""
daily_report.py

God-Tier Daily Report Worker (P0)
=================================

A resilient daily summary worker that:

1) Reads daily performance primarily from Postgres (preferred).
2) Falls back to Redis metrics (legacy / safety net) if PG is unavailable.
3) Sends a Telegram summary (best-effort; never crashes the process).
4) Optionally pushes a Notion PnL row via utils_pkg.notion_pnl.push_daily_pnl().
5) Dedupes so it only sends once per UTC day (atomic set-if-not-exists when possible).

Guarantees
----------
- Import-safe: no network connections at import time.
- Runtime-safe: ALL external I/O is best-effort and failure-isolated.
- Deterministic scheduling: by UTC hour/minute (Render-friendly).
- Typing-safe: mypy-friendly protocols for Redis and clean async boundaries.
"""

import argparse
import asyncio
import contextlib
import importlib
import inspect
import logging
import os
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Literal, Protocol, cast

# ──────────────────────────────────────────────────────────────────────────────
# Logging (compat: utils_pkg.log_compat if present; stdlib logger fallback)
# ──────────────────────────────────────────────────────────────────────────────

if TYPE_CHECKING:
    from utils_pkg.log_compat import EventLogger, get_logger
else:  # pragma: no cover
    try:
        from utils_pkg.log_compat import EventLogger, get_logger  # type: ignore
    except Exception:  # pragma: no cover
        EventLogger = Any  # type: ignore[misc,assignment]

        def get_logger(  # type: ignore[no-redef]
            name: str | None = None,
            *,
            service: str | None = None,
            version: str | None = None,
            env: str | None = None,
            level: str | int | None = None,
            json_logs: bool | None = None,
            use_utc: bool | None = None,
            include_src: bool | None = None,
            stream: str | None = None,
            force: bool = False,
        ) -> Any:
            lg = logging.getLogger(name or "daily_report")
            if force or not lg.handlers:
                logging.basicConfig(level=logging.INFO)
            if level is not None:
                with contextlib.suppress(Exception):
                    lg.setLevel(level)  # type: ignore[arg-type]
            return lg


log: "EventLogger" = get_logger(__name__)


def _log_info(msg: str, *args: Any) -> None:
    fn = getattr(log, "info", None)
    if callable(fn):
        with contextlib.suppress(Exception):
            fn(msg, *args)  # type: ignore[misc]


def _log_warn(msg: str, *args: Any) -> None:
    fn = getattr(log, "warning", None) or getattr(log, "warn", None)
    if callable(fn):
        with contextlib.suppress(Exception):
            fn(msg, *args)  # type: ignore[misc]


def _log_error(msg: str, *args: Any, exc_info: bool = False) -> None:
    fn = getattr(log, "error", None)
    if callable(fn):
        with contextlib.suppress(Exception):
            fn(msg, *args, exc_info=exc_info)  # type: ignore[misc]


async def _maybe_await(x: Any) -> Any:
    return await x if inspect.isawaitable(x) else x


# ──────────────────────────────────────────────────────────────────────────────
# Env helpers (no contract drift: only use existing keys already in .env.example)
# ──────────────────────────────────────────────────────────────────────────────


def _env_str(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or default).strip()


def _env_bool(name: str, default: bool) -> bool:
    v = _env_str(name, "")
    if not v:
        return default
    return v.lower() in ("1", "true", "yes", "on", "y")


def _env_int(name: str, default: int, *, lo: int | None = None, hi: int | None = None) -> int:
    v = _env_str(name, "")
    try:
        n = int(v) if v else int(default)
    except Exception:
        n = int(default)
    if lo is not None:
        n = max(lo, n)
    if hi is not None:
        n = min(hi, n)
    return n


def _env_float(name: str, default: float, *, lo: float | None = None) -> float:
    v = _env_str(name, "")
    try:
        x = float(v) if v else float(default)
    except Exception:
        x = float(default)
    if lo is not None:
        x = max(lo, x)
    return x


# ──────────────────────────────────────────────────────────────────────────────
# Telegram sending (AlertManager -> telegram_utils -> core_utils.send_telegram)
# ──────────────────────────────────────────────────────────────────────────────

SendFn = Callable[[str], Awaitable[None]]
_sender: SendFn | None = None
_sender_lock: asyncio.Lock | None = None


async def _build_sender() -> SendFn | None:
    # Keep behavior: default enabled unless TELEGRAM_ENABLED explicitly false.
    if not _env_bool("TELEGRAM_ENABLED", True):
        return None

    # 1) AlertManager
    try:
        am_mod = importlib.import_module("utils_pkg.alert_manager")
        get_am = getattr(am_mod, "get_alert_manager", None)
        if callable(get_am):

            async def _am_send(text: str) -> None:
                am = get_am()
                send_text = getattr(am, "send_text", None) or getattr(am, "send", None)
                if callable(send_text):
                    await _maybe_await(send_text(text))
                    return
                tg = getattr(am, "telegram", None)
                if tg is not None:
                    fn = getattr(tg, "send", None) or getattr(tg, "send_text", None)
                    if callable(fn):
                        await _maybe_await(fn(text))
                        return
                raise RuntimeError("AlertManager has no send method")

            return _am_send

        AlertManagerCls = getattr(am_mod, "AlertManager", None)
        if AlertManagerCls is not None:

            async def _am_cls_send(text: str) -> None:
                am: Any | None = None
                for attr in ("get_instance", "instance", "get"):
                    fn = getattr(AlertManagerCls, attr, None)
                    if callable(fn):
                        am = fn()
                        break
                if am is None:
                    am = AlertManagerCls()
                send_text = getattr(am, "send_text", None) or getattr(am, "send", None)
                if callable(send_text):
                    await _maybe_await(send_text(text))
                    return
                raise RuntimeError("AlertManager has no send method")

            return _am_cls_send
    except Exception:
        pass

    # 2) telegram_utils
    try:
        tg_mod = importlib.import_module("utils_pkg.telegram_utils")
        send_message = getattr(tg_mod, "send_message", None) or getattr(tg_mod, "send", None)
        if callable(send_message):

            async def _tg_send(text: str) -> None:
                # Best effort kwargs (filtered if function doesn't accept)
                wide: dict[str, Any] = {
                    "parse_mode": "Markdown",
                    "disable_web_page_preview": True,
                    "disable_preview": True,
                }
                wide2 = wide
                try:
                    sig = inspect.signature(send_message)
                    params = sig.parameters
                    has_varkw = any(
                        p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()
                    )
                    if not has_varkw:
                        wide2 = {k: v for k, v in wide.items() if k in params}
                except Exception:
                    wide2 = wide
                await _maybe_await(send_message(text, **wide2))

            return _tg_send
    except Exception:
        pass

    # 3) core_utils.send_telegram (legacy)
    try:
        cu_mod = importlib.import_module("utils_pkg.core_utils")
        send_telegram = getattr(cu_mod, "send_telegram", None)
        if callable(send_telegram):

            async def _legacy_send(text: str) -> None:
                try:
                    sig = inspect.signature(send_telegram)
                    params = sig.parameters
                    if "msg" in params:
                        await _maybe_await(send_telegram(msg=text))
                        return
                    if "message" in params:
                        await _maybe_await(send_telegram(message=text))
                        return
                    if "text" in params:
                        await _maybe_await(send_telegram(text=text))
                        return
                    await _maybe_await(send_telegram(text))
                except Exception:
                    await _maybe_await(send_telegram(text))

            return _legacy_send
    except Exception:
        pass

    return None


async def _get_sender() -> SendFn | None:
    global _sender, _sender_lock

    s = _sender
    if s is not None:
        return s

    if _sender_lock is None:
        _sender_lock = asyncio.Lock()
    lock = _sender_lock

    async with lock:
        s2 = _sender
        if s2 is None:
            s2 = await _build_sender()
            _sender = s2
        return s2


async def send_report(text: str) -> None:
    """Best-effort report sender. Never raises."""
    try:
        sender = await _get_sender()
        if sender is None:
            _log_warn("No Telegram sender available; report text:\n%s", text)
            return
        await sender(text)
    except Exception as e:  # pragma: no cover
        _log_error("send_report failed: %s", e, exc_info=True)


# ──────────────────────────────────────────────────────────────────────────────
# Redis access (prefer core_utils.get_redis_async) + typing contract
# ──────────────────────────────────────────────────────────────────────────────


class RedisLike(Protocol):
    async def get(self, key: str) -> str | None: ...
    async def set(self, key: str, value: str, **kwargs: Any) -> Any: ...


GetRedisAsync = Callable[[], Awaitable[Any]]

_get_redis_async: GetRedisAsync | None = None
_redis_singleton: RedisLike | None = None
_redis_lock: asyncio.Lock | None = None


def _try_load_core_get_redis_async() -> GetRedisAsync | None:
    try:
        cu_mod = importlib.import_module("utils_pkg.core_utils")
        fn = getattr(cu_mod, "get_redis_async", None)
        return cast(GetRedisAsync, fn) if callable(fn) else None
    except Exception:
        return None


def _redis_from_url(url: str) -> Any | None:
    # Lazy import keeps module import light.
    try:
        import redis.asyncio as redis_async  # type: ignore[import-not-found,import-untyped]
    except Exception:
        return None
    try:
        return redis_async.from_url(
            url,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
    except Exception:
        return None


async def get_redis() -> RedisLike | None:
    global _redis_singleton, _redis_lock, _get_redis_async

    cached = _redis_singleton
    if cached is not None:
        return cached

    if _redis_lock is None:
        _redis_lock = asyncio.Lock()

    async with _redis_lock:
        cached2 = _redis_singleton
        if cached2 is not None:
            return cached2

        if _get_redis_async is None:
            _get_redis_async = _try_load_core_get_redis_async()

        if _get_redis_async is not None:
            with contextlib.suppress(Exception):
                r = await _get_redis_async()
                if r is not None:
                    _redis_singleton = cast(RedisLike, r)
                    return _redis_singleton

        url = _env_str("REDIS_URL", "redis://localhost:6379") or "redis://localhost:6379"
        r2 = _redis_from_url(url)
        if r2 is not None:
            _redis_singleton = cast(RedisLike, r2)
            return _redis_singleton

        return None


async def r_get_float(r: RedisLike, key: str, default: float = 0.0) -> float:
    try:
        v = await r.get(key)
        return float(v) if v is not None else default
    except Exception:
        return default


async def r_set_once(r: RedisLike, key: str, value: str, ttl_sec: int) -> bool:
    """
    Best-effort NX set for dedupe.
    Returns True if caller should proceed; False if already set (when NX works).
    """
    ttl = max(60, int(ttl_sec))
    try:
        res = await r.set(key, value, ex=ttl, nx=True)
        return bool(res)
    except TypeError:
        with contextlib.suppress(Exception):
            await r.set(key, value)
        return True
    except Exception:
        return True


# In-process dedupe fallback (prevents spam if Redis is down/unavailable).
_LAST_SENT_DAY_TAG: str | None = None
_LAST_SENT_LOCK: asyncio.Lock | None = None


async def _inproc_set_once(day_tag: str) -> bool:
    global _LAST_SENT_DAY_TAG, _LAST_SENT_LOCK
    if _LAST_SENT_LOCK is None:
        _LAST_SENT_LOCK = asyncio.Lock()
    async with _LAST_SENT_LOCK:
        if _LAST_SENT_DAY_TAG == day_tag:
            return False
        _LAST_SENT_DAY_TAG = day_tag
        return True


# ──────────────────────────────────────────────────────────────────────────────
# Postgres (best-effort): psycopg3 async preferred, asyncpg fallback
# ──────────────────────────────────────────────────────────────────────────────

try:
    import psycopg as _psycopg  # type: ignore[import-not-found,import-untyped]
    from psycopg.rows import dict_row as _dict_row  # type: ignore[import-not-found,import-untyped]
except Exception:  # pragma: no cover
    _psycopg = None
    _dict_row = None

try:
    import asyncpg as _asyncpg  # type: ignore[import-not-found,import-untyped]
except Exception:  # pragma: no cover
    _asyncpg = None

psycopg = _psycopg
dict_row = _dict_row
asyncpg = _asyncpg


def resolve_pg_dsn() -> str | None:
    for k in ("POSTGRES_DSN", "PG_DSN", "PG_DSN_INTERNAL", "PG_DSN_EXTERNAL", "DATABASE_URL"):
        v = _env_str(k, "")
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
        with contextlib.suppress(Exception):
            await _maybe_await(self.conn.close())

    @staticmethod
    def _psycopg_sql(sql: str) -> str:
        # Convert asyncpg $1/$2 to psycopg %s placeholders.
        return re.sub(r"\$\d+", "%s", sql)

    async def fetchrow(self, sql: str, params: tuple[Any, ...]) -> dict[str, Any] | None:
        if self.driver == "psycopg":
            sql2 = self._psycopg_sql(sql)
            rf = dict_row if dict_row is not None else None
            cur = self.conn.cursor(row_factory=rf) if rf is not None else self.conn.cursor()
            async with cur as c:
                await c.execute(sql2, params)
                row = await c.fetchone()
                if row is None:
                    return None
                with contextlib.suppress(Exception):
                    return dict(row)
                return {"row": row}

        row = await self.conn.fetchrow(sql, *params)
        return None if row is None else dict(row)


async def pg_connect() -> _PgConn | None:
    dsn = resolve_pg_dsn()
    if not dsn:
        return None

    if psycopg is not None:
        last_err: Exception | None = None
        for _ in range(2):
            try:
                c = await psycopg.AsyncConnection.connect(  # type: ignore[attr-defined]
                    dsn,
                    connect_timeout=10,
                )
                return _PgConn("psycopg", c)
            except Exception as e:
                last_err = e
        _log_warn("PG connect (psycopg) failed: %s", last_err)

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
        _log_warn("PG connect (asyncpg) failed: %s", last_err2)

    return None


def _utc_day_bounds(day: datetime) -> tuple[datetime, datetime]:
    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    end = start + timedelta(days=1)
    return start, end


@lru_cache(maxsize=1)
def _cached_pg_probes() -> tuple[
    list[tuple[str, str, ProbeMode]], list[tuple[str, str, ProbeMode]]
]:
    ts_cols = ("ts", "time", "created_at", "filled_at", "executed_at")
    trade_tables = ("trades", "bot_trades", "trade_fills", "fills")

    probes: list[tuple[str, str, ProbeMode]] = []
    for table in trade_tables:
        for ts_col in ts_cols:
            probes.append(
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
            )
            probes.append(
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
            )

    daily_tables = ("daily_pnl", "pnl_daily", "bot_daily_pnl")
    date_cols = ("date", "day", "date_utc")
    for table in daily_tables:
        for date_col in date_cols:
            probes.append(
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
            )

    pos_tables = ("positions", "bot_positions", "open_positions")
    open_cols = ("is_open", "open")
    pnl_cols = ("unrealized_pnl_usd", "pnl_unrealized_usd", "unrealized_usd")

    pos_probes: list[tuple[str, str, ProbeMode]] = []
    for table in pos_tables:
        pos_probes.append(
            (
                f"{table}:status",
                f"SELECT COUNT(*)::bigint AS open_positions FROM {table} WHERE status = 'open'",
                "none",
            )
        )
        for open_col in open_cols:
            pos_probes.append(
                (
                    f"{table}:{open_col}",
                    f"SELECT COUNT(*)::bigint AS open_positions FROM {table} WHERE {open_col} = true",
                    "none",
                )
            )
            for pnl_col in pnl_cols:
                pos_probes.append(
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
                )

    return probes, pos_probes


async def pg_fetch_daily_summary(conn: _PgConn, day_utc: datetime) -> dict[str, Any] | None:
    start, end = _utc_day_bounds(day_utc)
    probes, pos_probes = _cached_pg_probes()

    best: dict[str, Any] | None = None
    best_score: tuple[int, float] = (-1, -1.0)

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

            cand: dict[str, Any] = {
                "realized_usd": realized,
                "trades": trades,
                "wins": wins,
                "losses": max(0, trades - wins),
            }
            if "unrealized_usd" in row:
                cand["unrealized_usd"] = float(row.get("unrealized_usd", 0) or 0)
            if "open_positions" in row:
                cand["open_positions"] = int(row.get("open_positions", 0) or 0)

            score = (trades, abs(realized))
            if score > best_score:
                best = cand
                best_score = score
        except Exception:
            continue

    if best is None:
        return None

    # Fill open_positions/unrealized if missing (best-effort)
    for _label, sql, _mode in pos_probes:
        try:
            row = await conn.fetchrow(sql, ())
            if row is None:
                continue
            if "open_positions" in row and "open_positions" not in best:
                best["open_positions"] = int(row.get("open_positions", 0) or 0)
            if "unrealized_usd" in row and "unrealized_usd" not in best:
                best["unrealized_usd"] = float(row.get("unrealized_usd", 0) or 0)
        except Exception:
            continue

    return best


# ──────────────────────────────────────────────────────────────────────────────
# Optional integrations (lazy import; keeps import-time safe)
# ──────────────────────────────────────────────────────────────────────────────


def _get_push_daily_pnl_fn() -> Callable[[dict[str, Any]], Any] | None:
    try:
        mod = importlib.import_module("utils_pkg.notion_pnl")
        fn = getattr(mod, "push_daily_pnl", None)
        return cast(Callable[[dict[str, Any]], Any], fn) if callable(fn) else None
    except Exception:
        return None


def _get_current_limits_fn() -> Callable[[Any], Any] | None:
    # Prefer utils_pkg.risk_manager, fallback to legacy risk_manager.
    for mod_name in ("utils_pkg.risk_manager", "risk_manager"):
        try:
            mod = importlib.import_module(mod_name)
            fn = getattr(mod, "current_limits", None)
            if callable(fn):
                return cast(Callable[[Any], Any], fn)
        except Exception:
            continue
    return None


async def _call_best_effort(func: Any, *args: Any, **kwargs: Any) -> None:
    if func is None:
        return
    try:
        if inspect.iscoroutinefunction(func):
            await func(*args, **kwargs)
            return
        res = await asyncio.to_thread(func, *args, **kwargs)
        if inspect.isawaitable(res):
            await res
    except Exception:
        return


# ──────────────────────────────────────────────────────────────────────────────
# Config + snapshot/report building
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
    enabled = _env_bool("REPORT_ENABLED", True)
    hour_utc = _env_int("REPORT_HOUR_UTC", 23, lo=0, hi=23)
    minute_utc = _env_int("REPORT_MINUTE_UTC", 59, lo=0, hi=59)
    poll_sec = _env_float("REPORT_POLL_SEC", 20.0, lo=5.0)
    send_on_start = _env_bool("REPORT_SEND_ON_START", False)
    dedup_ttl_sec = _env_int("REPORT_DEDUP_TTL_SEC", 26 * 3600, lo=3600)
    display_tz = _env_str("REPORT_TZ", "UTC") or "UTC"
    return ReportConfig(
        enabled=enabled,
        hour_utc=hour_utc,
        minute_utc=minute_utc,
        poll_sec=poll_sec,
        send_on_start=send_on_start,
        dedup_ttl_sec=dedup_ttl_sec,
        display_tz=display_tz,
    )


def _day_tag_utc(dt: datetime) -> str:
    return f"{dt.year:04d}{dt.month:02d}{dt.day:02d}"


def _sent_key(day_tag: str) -> str:
    return f"report:sent:{day_tag}"


def _worker_status(name: str, enabled_var: str, shadow_var: str) -> str:
    en = _env_bool(enabled_var, True)
    sh = _env_bool(shadow_var, False)
    if not en:
        return f"{name}: off"
    return f"{name}: {'shadow' if sh else 'live'}"


async def _fetch_redis_metrics(r: RedisLike, day_utc: datetime) -> dict[str, Any]:
    day_tag = _day_tag_utc(day_utc)
    realized_key = f"pnl:day:realized_usd:{day_tag}"
    return {
        "realized_usd": await r_get_float(r, realized_key, 0.0),
        "winrate20": await r_get_float(r, "metrics:winrate20", 0.0),
        "rolling_dd": await r_get_float(r, "metrics:rolling_dd_pct", 0.0),
        "equity_hwm_usd": await r_get_float(r, "metrics:equity_hwm_usd", 0.0),
    }


async def build_daily_snapshot(day_utc: datetime) -> dict[str, Any]:
    snapshot: dict[str, Any] = {
        "date": day_utc.date().isoformat(),
        "time": datetime.now(UTC).isoformat(),
    }

    # Postgres preferred
    conn = await pg_connect()
    if conn is not None:
        try:
            pg_sum = await pg_fetch_daily_summary(conn, day_utc)
            if pg_sum:
                snapshot.update(pg_sum)
        except Exception as e:
            _log_warn("PG summary failed: %s", e)
        finally:
            await conn.close()

    # Redis fallback/augmentation
    r = await get_redis()
    if r is not None:
        with contextlib.suppress(Exception):
            rm = await _fetch_redis_metrics(r, day_utc)
            for k, v in rm.items():
                snapshot.setdefault(k, v)

            day_tag = _day_tag_utc(day_utc)
            u = await r_get_float(r, f"pnl:day:unrealized_usd:{day_tag}", 0.0)
            if u and "unrealized_usd" not in snapshot:
                snapshot["unrealized_usd"] = u

            for key in ("price:sol_usd", "sol:price_usd", "price_usd:sol", "sol_price_usd"):
                p = await r_get_float(r, key, 0.0)
                if p and "price_usd" not in snapshot:
                    snapshot["price_usd"] = p
                    break

    # Optional risk limits (legacy+new)
    cur_limits = _get_current_limits_fn()
    if cur_limits is not None and r is not None:
        with contextlib.suppress(Exception):
            limits_obj = cur_limits(r)
            if inspect.isawaitable(limits_obj):
                limits_obj = await limits_obj
            if isinstance(limits_obj, dict):
                snapshot["limits"] = limits_obj

    realized_usd = float(snapshot.get("realized_usd", 0.0) or 0.0)
    unrealized_usd = float(snapshot.get("unrealized_usd", 0.0) or 0.0)
    trades = int(snapshot.get("trades", 0) or 0)
    wins = int(snapshot.get("wins", 0) or 0)

    snapshot["net_usd"] = realized_usd + unrealized_usd
    snapshot["day_winrate"] = (wins / trades) if trades > 0 else 0.0

    price_usd = snapshot.get("price_usd")
    if price_usd:
        with contextlib.suppress(Exception):
            px = float(price_usd)
            snapshot.setdefault("realized_sol", realized_usd / px if px > 0 else 0.0)
            snapshot.setdefault("unrealized_sol", unrealized_usd / px if px > 0 else 0.0)

    snapshot["workers"] = [
        _worker_status("scalper", "DAYTRADER_ENABLED", "SCALPER_SHADOW_MODE"),
        _worker_status("whales", "WHALE_FEED_ENABLED", "WHALE_SHADOW_MODE"),
        _worker_status("velocity", "VELOCITY_ENABLED", "VELOCITY_SHADOW_MODE"),
        _worker_status("hotlist", "HOTLIST_ENABLED", "HOTLIST_SHADOW_MODE"),
        _worker_status("sniper", "SNIPER_ENABLED", "SNIPER_SHADOW_MODE"),
    ]
    return snapshot


def _format_as_of(snapshot: dict[str, Any]) -> str:
    ts_raw = str(snapshot.get("time") or "").strip()
    if not ts_raw:
        return ""
    try:
        dt_utc = datetime.fromisoformat(ts_raw.replace("Z", "+00:00")).astimezone(UTC)
    except Exception:
        return ""

    tz_name = _env_str("REPORT_TZ", "UTC") or "UTC"
    if tz_name.upper() == "UTC":
        return f"As of: `{dt_utc.strftime('%Y-%m-%d %H:%M')} UTC`"

    try:
        from zoneinfo import ZoneInfo

        dt_local = dt_utc.astimezone(ZoneInfo(tz_name))
        return (
            f"As of: `{dt_utc.strftime('%Y-%m-%d %H:%M')} UTC`"
            f"  (`{dt_local.strftime('%Y-%m-%d %H:%M')} {tz_name}`)"
        )
    except Exception:
        return f"As of: `{dt_utc.strftime('%Y-%m-%d %H:%M')} UTC`"


def format_report(snapshot: dict[str, Any]) -> str:
    day = str(snapshot.get("date") or snapshot.get("date_utc") or "")[:10].strip() or "today"
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

    limits = snapshot.get("limits") if isinstance(snapshot.get("limits"), dict) else {}
    equity = (limits or {}).get("equity_usd")
    ticket = (limits or {}).get("ticket_usd")
    max_conc = (limits or {}).get("max_concurrent")
    daily_budget = (limits or {}).get("daily_spend_usd")

    open_pos = snapshot.get("open_positions")
    workers = snapshot.get("workers") if isinstance(snapshot.get("workers"), list) else []

    stop_after_profit = _env_str("STOP_AFTER_DAILY_PROFIT_PCT", "")
    spend_cap_pct = _env_str("DAILY_SPEND_CAP_PCT", "")

    lines: list[str] = []
    lines.append(f"📊 *Daily Summary* (UTC {day})")

    as_of = _format_as_of(snapshot)
    if as_of:
        lines.append(as_of)

    lines.append("")

    if equity is not None:
        with contextlib.suppress(Exception):
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
        with contextlib.suppress(Exception):
            lines.append(f"• Open positions: `{int(open_pos)}`")

    if workers:
        lines.append(f"• Workers: {', '.join([str(x) for x in workers])}")

    extra: list[str] = []
    if stop_after_profit:
        extra.append(f"stop-after-profit `{stop_after_profit}%`")
    if spend_cap_pct:
        with contextlib.suppress(Exception):
            extra.append(f"spend-cap `{int(float(spend_cap_pct) * 100)}%`")
    if extra:
        lines.append("• Playbook: " + "  |  ".join(extra))

    return "\n".join(lines)


async def maybe_push_notion(snapshot: dict[str, Any]) -> None:
    fn = _get_push_daily_pnl_fn()
    if fn is None:
        return
    await _call_best_effort(fn, snapshot)


async def send_once_for_day(day_utc: datetime, *, dedup: bool = True) -> bool:
    cfg = load_report_config()
    r = await get_redis()

    day_tag = _day_tag_utc(day_utc)
    key = _sent_key(day_tag)

    if dedup:
        ok_inproc = await _inproc_set_once(day_tag)
        if not ok_inproc:
            return False

        if r is not None:
            ok = await r_set_once(r, key, "1", ttl_sec=cfg.dedup_ttl_sec)
            if not ok:
                return False

    snapshot = await build_daily_snapshot(day_utc)
    text = format_report(snapshot)

    await send_report(text)
    await maybe_push_notion(snapshot)

    # Re-assert the sent key (best-effort)
    if dedup and r is not None:
        with contextlib.suppress(Exception):
            await r.set(key, "1", ex=int(cfg.dedup_ttl_sec))

    return True


def _scheduled_dt_for_today(now_utc: datetime, cfg: ReportConfig) -> datetime:
    return datetime(
        now_utc.year,
        now_utc.month,
        now_utc.day,
        cfg.hour_utc,
        cfg.minute_utc,
        tzinfo=UTC,
    )


async def run_worker() -> None:
    cfg = load_report_config()
    if not cfg.enabled:
        _log_info("Daily report disabled (REPORT_ENABLED=false)")
        return

    await send_report("📝 Daily report worker started ✅")

    if cfg.send_on_start:
        with contextlib.suppress(Exception):
            await send_once_for_day(datetime.now(UTC), dedup=True)

    while True:
        try:
            now = datetime.now(UTC)
            sched = _scheduled_dt_for_today(now, cfg)

            # Send if at/after scheduled time and not yet sent today.
            if now >= sched:
                sent = await send_once_for_day(now, dedup=True)
                if sent:
                    await asyncio.sleep(65)
                    continue

            await asyncio.sleep(cfg.poll_sec)
        except Exception as e:  # pragma: no cover
            _log_error("[daily_report] loop error: %s", e, exc_info=True)
            await asyncio.sleep(60)


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Daily report worker / one-shot sender")
    p.add_argument("--once", action="store_true", help="Send once and exit (dedup respected)")
    p.add_argument("--no-dedup", action="store_true", help="Disable dedup for --once")
    p.add_argument("--date", type=str, default="", help="UTC date YYYY-MM-DD (only for --once)")
    return p.parse_args()


def _parse_date_utc(s: str) -> datetime:
    s2 = (s or "").strip()
    if not s2:
        return datetime.now(UTC)
    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", s2)
    if not m:
        raise ValueError("date must be YYYY-MM-DD")
    y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    return datetime(y, mo, d, tzinfo=UTC)


async def _main_async() -> None:
    args = _parse_args()
    if args.once:
        day = _parse_date_utc(args.date)
        sent = await send_once_for_day(day, dedup=not args.no_dedup)
        _log_info("daily_report --once done (sent=%s)", sent)
        return
    await run_worker()


def main() -> None:
    asyncio.run(_main_async())


if __name__ == "__main__":
    main()
