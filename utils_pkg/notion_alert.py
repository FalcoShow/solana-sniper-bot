# utils_pkg/notion_alert.py
from __future__ import annotations

"""
utils_pkg/notion_alert.py

Notion trade/alert logger (God-Tier / production-grade)
=======================================================

Role
----
Canonical, resilient Notion writer for per-trade rows (fills, exits, rejects if desired).
This module is **best-effort** by default: it should never crash the trading loop.

Design
------
- Uses utils_pkg.notion_client.NotionAsync (httpx + retries + light RPS throttle).
- Auto-discovers DB schema and caches it.
- Supports BOTH:
    A) explicit env property mapping (your existing NOTION_PROP_* style), and
    B) auto-matching common property names if envs are not provided.
- Optional dedupe-by-signature if your DB has a Sig/Tx/Signature column.

Env (DB + token)
----------------
NOTION_TOKEN / NOTION_API_KEY
NOTION_TRADES_DB_ID   (preferred)
NOTION_DB_TRADES      (legacy)
NOTION_TRADES_DB      (legacy)
NOTION_DATABASE_ID    (legacy fallback; only if it points at trades DB)

Env (write controls)
--------------------
NOTION_ALERT_ENABLED           (default true)
NOTION_ALERT_ALLOW_DRYRUN      (default true)
NOTION_ALERT_DEDUPE_BY_SIG     (default true)
NOTION_ALERT_DEDUPE_PROP       (optional: exact property name used for signature/tx; overrides auto)

Env (property mapping; leave blank to skip)
------------------------------------------
NOTION_TITLE_PROP              (optional override; otherwise discovered)
NOTION_DATE_PROP               (optional override; otherwise discovered)

Legacy mapping keys you already use:
NOTION_PROP_TOKEN
NOTION_PROP_SYMBOL
NOTION_PROP_BALANCE
NOTION_PROP_ENTRY
NOTION_PROP_CURRENT
NOTION_PROP_PROFIT
NOTION_PROP_REASON
NOTION_PROP_TIME

Additional supported mapping keys:
NOTION_PROP_SIDE
NOTION_PROP_SIGNATURE
NOTION_PROP_POOL
NOTION_PROP_PNL_PCT

Public API
----------
- async log_trade_event(...): main entry point (best-effort by default)
- async log_trade(...): backwards compatible alias
- async log_trade_from_env(...): convenience wrapper

Notes
-----
- This module does NOT create its own aiohttp session (NotionAsync uses httpx internally).
- For scripts, you can still use utils_pkg.notion_client.Notion (sync), but for runtime use NotionAsync.
"""

import os
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

# logger fallback
try:  # pragma: no cover
    from logging_setup import get_logger  # type: ignore
except Exception:  # pragma: no cover
    import logging

    def get_logger(name: str | None = None):  # type: ignore
        return logging.getLogger(name or "notion_alert")


log = get_logger(__name__)

import contextlib

from .notion_client import DbSchema, NotionAsync, NotionError, normalize_notion_id


def _b(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "t", "yes", "y", "on")


def _s(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()


def _f(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    try:
        return float(str(v).strip())
    except Exception:
        return default


def _dryrun_on() -> bool:
    return _b("DRYRUN", False) or _b("DRY_RUN", False) or _b("IS_DRYRUN", False)


def _clip(s: str, n: int) -> str:
    s = (s or "").strip()
    return s if len(s) <= n else s[: max(0, n - 1)] + "…"


def _utc_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _num(x: Any) -> float | None:
    try:
        v = float(x)
        # clamp tiny noise
        return 0.0 if abs(v) < 1e-15 else v
    except Exception:
        return None


def _get_token() -> str:
    return _s("NOTION_TOKEN") or _s("NOTION_API_KEY")


def _get_trades_db_id() -> str:
    return (
        _s("NOTION_TRADES_DB_ID")
        or _s("NOTION_DB_TRADES")
        or _s("NOTION_TRADES_DB")
        or _s("NOTION_DATABASE_ID")
    )


def _pick_prop(schema: DbSchema, candidates: Sequence[str]) -> str:
    """
    Find first property name in schema that matches any candidate (case-insensitive).
    """
    if not candidates:
        return ""
    lower = {k.lower(): k for k in schema.has_prop}
    for c in candidates:
        key = (c or "").strip().lower()
        if not key:
            continue
        if key in lower:
            return lower[key]
    return ""


def _env_prop_name(env_key: str) -> str:
    """
    Read a property name from env. Blank -> skip.
    """
    return _s(env_key, "").strip()


@dataclass(frozen=True)
class TradeEvent:
    token: str
    symbol: str = ""
    balance: float = 0.0
    entry: float = 0.0
    current: float = 0.0
    profit: float = 0.0
    reason: str = ""
    side: str = ""  # "buy"/"sell"/"exit"/etc
    signature: str | None = None
    pool: str = ""
    pnl_pct: float | None = None
    ts_iso: str | None = None  # if provided, will use it; else now()


# ──────────────────────────────────────────────────────────────────────────────
# Schema cache (per-process)
# ──────────────────────────────────────────────────────────────────────────────
_SCHEMA_CACHE: dict[str, tuple[DbSchema, float]] = {}
_SCHEMA_TTL_SEC = 30 * 60  # refresh every 30 minutes (lightweight; avoids drift)
_SCHEMA_LOCK = None  # set lazily to avoid import-time event loop issues


async def _get_schema(notion: NotionAsync, database_id: str) -> DbSchema:
    global _SCHEMA_LOCK
    if _SCHEMA_LOCK is None:
        import asyncio

        _SCHEMA_LOCK = asyncio.Lock()

    dbid = normalize_notion_id(database_id)
    now = time.time()
    cached = _SCHEMA_CACHE.get(dbid)
    if cached and (now - cached[1]) < _SCHEMA_TTL_SEC:
        return cached[0]

    async with _SCHEMA_LOCK:  # type: ignore[misc]
        cached = _SCHEMA_CACHE.get(dbid)
        if cached and (time.time() - cached[1]) < _SCHEMA_TTL_SEC:
            return cached[0]
        schema = await notion.discover_schema(dbid, use_cache=True)
        _SCHEMA_CACHE[dbid] = (schema, time.time())
        return schema


async def _find_page_by_value(
    notion: NotionAsync,
    database_id: str,
    schema: DbSchema,
    prop_name: str,
    value: str,
) -> str | None:
    """
    Optional dedupe helper: find first page where prop equals value.
    Supports title/rich_text/url equality filters.
    """
    prop = (prop_name or "").strip()
    val = (value or "").strip()
    if not prop or not val:
        return None

    ptype = (schema.prop_types.get(prop) or "").strip()
    if ptype == "title":
        flt = {"property": prop, "title": {"equals": val}}
    elif ptype == "rich_text":
        flt = {"property": prop, "rich_text": {"equals": val}}
    elif ptype == "url":
        flt = {"property": prop, "url": {"equals": val}}
    else:
        return None  # can't filter reliably

    rows = await notion.query_database(database_id, filter_obj=flt, page_size=1, max_pages=1)
    if not rows:
        return None
    pid = rows[0].get("id")
    return normalize_notion_id(str(pid)) if pid else None


def _resolve_trade_prop_names(schema: DbSchema) -> dict[str, str]:
    """
    Resolve property names used to write a trade row.
    Env overrides take priority; otherwise we auto-match common names.
    Blank means "skip".
    """
    # legacy env mapping (keep compatibility)
    p_token = _env_prop_name("NOTION_PROP_TOKEN")
    p_symbol = _env_prop_name("NOTION_PROP_SYMBOL")
    p_balance = _env_prop_name("NOTION_PROP_BALANCE")
    p_entry = _env_prop_name("NOTION_PROP_ENTRY")
    p_current = _env_prop_name("NOTION_PROP_CURRENT")
    p_profit = _env_prop_name("NOTION_PROP_PROFIT")
    p_reason = _env_prop_name("NOTION_PROP_REASON")
    p_time = _env_prop_name("NOTION_PROP_TIME")

    # extended mapping
    p_side = _env_prop_name("NOTION_PROP_SIDE")
    p_sig = _env_prop_name("NOTION_PROP_SIGNATURE")
    p_pool = _env_prop_name("NOTION_PROP_POOL")
    p_pnlpct = _env_prop_name("NOTION_PROP_PNL_PCT")

    # Auto-match if missing
    if not p_token:
        p_token = _pick_prop(schema, ["Mint", "Token", "Address", "Contract", "CA"])
    if not p_symbol:
        p_symbol = _pick_prop(schema, ["Symbol", "Ticker", "Sym"])
    if not p_balance:
        p_balance = _pick_prop(schema, ["Balance", "Size", "Qty", "Amount", "SpendSOL", "Spend"])
    if not p_entry:
        p_entry = _pick_prop(
            schema, ["Entry", "Entry Price", "EntryPrice", "Buy Price", "Avg Entry"]
        )
    if not p_current:
        p_current = _pick_prop(schema, ["Current", "Current Price", "Price", "Mark", "Last"])
    if not p_profit:
        p_profit = _pick_prop(schema, ["Profit", "PnL", "Pnl", "Profit SOL", "ProfitSol"])
    if not p_pnlpct:
        p_pnlpct = _pick_prop(schema, ["PnL %", "Pnl %", "PnL%", "ROI", "ROI %", "Return %"])
    if not p_reason:
        p_reason = _pick_prop(schema, ["Reason", "Notes", "Why", "Tag"])
    if not p_side:
        p_side = _pick_prop(schema, ["Side", "Action", "Type"])
    if not p_sig:
        p_sig = _pick_prop(schema, ["Tx", "TxSig", "Signature", "Sig", "Transaction"])
    if not p_pool:
        p_pool = _pick_prop(schema, ["Pool", "Pair", "Market"])
    # time: if they didn't explicitly map, we can also attempt a common column (but schema.date_prop already exists)
    if not p_time:
        p_time = _pick_prop(schema, ["Time", "Timestamp", "DateTime", "Created"])

    # Never overwrite title/date props accidentally
    for k, v in list(
        {
            "token": p_token,
            "symbol": p_symbol,
            "balance": p_balance,
            "entry": p_entry,
            "current": p_current,
            "profit": p_profit,
            "pnl_pct": p_pnlpct,
            "reason": p_reason,
            "side": p_side,
            "signature": p_sig,
            "pool": p_pool,
            "time": p_time,
        }.items()
    ):
        if v in (schema.title_prop, schema.date_prop):
            # title/date handled centrally
            locals()[f"p_{k}"] = ""  # type: ignore[misc]

    return {
        "token": p_token,
        "symbol": p_symbol,
        "balance": p_balance,
        "entry": p_entry,
        "current": p_current,
        "profit": p_profit,
        "pnl_pct": p_pnlpct,
        "reason": p_reason,
        "side": p_side,
        "signature": p_sig,
        "pool": p_pool,
        "time": p_time,
    }


def _build_title(ev: TradeEvent) -> str:
    side = (ev.side or "").strip().upper()
    sym = (ev.symbol or "").strip()
    if sym:
        return f"{side} {sym}".strip() if side else sym
    # fallback: short mint
    t = (ev.token or "").strip()
    return f"{side} {t[:6]}…".strip() if side else (t[:6] + "…")


async def log_trade_event(
    *,
    token: str,
    symbol: str,
    balance: float,
    entry: float,
    current: float,
    profit: float,
    reason: str,
    side: str = "",
    signature: str | None = None,
    pool: str = "",
    pnl_pct: float | None = None,
    # Notion auth/db
    database_id: str | None = None,
    token_auth: str | None = None,
    notion: NotionAsync | None = None,
    # behavior
    raise_on_error: bool = False,
) -> bool:
    """
    Best-effort trade row insert into Notion.

    Returns:
      True  -> write attempted successfully (or dryrun skip treated as success)
      False -> skipped (missing config) or failed (best-effort mode)
    """
    enabled = _b("NOTION_ALERT_ENABLED", True)
    if not enabled:
        return False

    token_auth = (token_auth or _get_token()).strip()
    database_id = (database_id or _get_trades_db_id()).strip()

    if not token_auth or not database_id:
        # keep quiet; this is common in local tests
        log.debug(
            "NOTION_ALERT_SKIP_MISSING_CONFIG",
            extra={"has_token": bool(token_auth), "has_db": bool(database_id)},
        )
        return False

    allow_dryrun = _b("NOTION_ALERT_ALLOW_DRYRUN", True)
    if _dryrun_on() and not allow_dryrun:
        return True  # treat as success (intentional skip)

    ev = TradeEvent(
        token=token,
        symbol=symbol,
        balance=float(balance or 0.0),
        entry=float(entry or 0.0),
        current=float(current or 0.0),
        profit=float(profit or 0.0),
        reason=reason or "",
        side=side or "",
        signature=signature,
        pool=pool or "",
        pnl_pct=pnl_pct,
        ts_iso=_utc_iso(),
    )

    local_notion = notion is None
    notion = notion or NotionAsync(token=token_auth)

    try:
        schema = await _get_schema(notion, database_id)
        props_map = _resolve_trade_prop_names(schema)

        # Optional dedupe by signature (recommended)
        dedupe = _b("NOTION_ALERT_DEDUPE_BY_SIG", True)
        dedupe_prop = _s("NOTION_ALERT_DEDUPE_PROP", "").strip()
        if dedupe and ev.signature:
            # choose dedupe prop: explicit > mapped signature prop > common names
            prop = (
                dedupe_prop
                or props_map.get("signature")
                or _pick_prop(schema, ["TxSig", "Signature", "Sig", "Tx", "Transaction"])
            )
            if prop:
                existing = await _find_page_by_value(
                    notion, database_id, schema, prop, ev.signature
                )
                if existing:
                    return True

        # Build values mapping for build_properties
        values: dict[str, Any] = {}

        if props_map["token"]:
            values[props_map["token"]] = ev.token
        if props_map["symbol"]:
            values[props_map["symbol"]] = ev.symbol
        if props_map["balance"]:
            n = _num(ev.balance)
            if n is not None:
                values[props_map["balance"]] = n
        if props_map["entry"]:
            n = _num(ev.entry)
            if n is not None:
                values[props_map["entry"]] = n
        if props_map["current"]:
            n = _num(ev.current)
            if n is not None:
                values[props_map["current"]] = n
        if props_map["profit"]:
            n = _num(ev.profit)
            if n is not None:
                values[props_map["profit"]] = n
        if props_map["pnl_pct"] and ev.pnl_pct is not None:
            n = _num(ev.pnl_pct)
            if n is not None:
                values[props_map["pnl_pct"]] = n
        if props_map["reason"] and ev.reason:
            values[props_map["reason"]] = _clip(ev.reason, 1800)
        if props_map["side"] and ev.side:
            values[props_map["side"]] = str(ev.side).strip()
        if props_map["signature"] and ev.signature:
            values[props_map["signature"]] = str(ev.signature).strip()
        if props_map["pool"] and ev.pool:
            values[props_map["pool"]] = str(ev.pool).strip()
        if props_map["time"]:
            values[props_map["time"]] = ev.ts_iso

        title_text = _build_title(ev)

        # Always stamp schema.date_prop (if present) so you can sort/filter by time
        iso_date = ev.ts_iso  # full ISO is allowed; Notion date accepts ISO datetime
        props = notion.build_properties(
            schema, values=values, iso_date=iso_date, title_text=title_text, strict=False
        )

        if _dryrun_on() and not notion.allow_dryrun:
            return True

        _ = await notion.create_page(database_id, props)
        return True

    except Exception as e:
        # best-effort: never crash the bot loop
        if raise_on_error:
            raise
        msg = str(e)
        if isinstance(e, NotionError):
            msg = e.short()
        log.warning(
            "NOTION_ALERT_FAIL",
            extra={"err": _clip(msg, 500), "token": token[:8], "symbol": _clip(symbol, 24)},
        )
        return False
    finally:
        if local_notion:
            with contextlib.suppress(Exception):
                await notion.aclose()


# Backwards compatible alias (your prior module exposed log_trade)
async def log_trade(
    *,
    token: str,
    symbol: str,
    balance: float,
    entry: float,
    current: float,
    profit: float,
    reason: str,
    database_id: str,
    token_auth: str,
    session: Any = None,  # legacy param; ignored
) -> None:
    await log_trade_event(
        token=token,
        symbol=symbol,
        balance=balance,
        entry=entry,
        current=current,
        profit=profit,
        reason=reason,
        database_id=database_id,
        token_auth=token_auth,
        raise_on_error=False,
    )


# Convenience wrapper: read token+db from env (kept for compatibility)
async def log_trade_from_env(
    *,
    token: str,
    symbol: str,
    balance: float,
    entry: float,
    current: float,
    profit: float,
    reason: str,
    side: str = "",
    signature: str | None = None,
    pool: str = "",
    pnl_pct: float | None = None,
    notion: NotionAsync | None = None,
) -> bool:
    return await log_trade_event(
        token=token,
        symbol=symbol,
        balance=balance,
        entry=entry,
        current=current,
        profit=profit,
        reason=reason,
        side=side,
        signature=signature,
        pool=pool,
        pnl_pct=pnl_pct,
        notion=notion,
        raise_on_error=False,
    )


__all__ = [
    "TradeEvent",
    "log_trade",
    "log_trade_event",
    "log_trade_from_env",
]
