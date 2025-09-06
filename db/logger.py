# db/logger.py — v2.1 (pooled, tolerant, exits-aware)
from __future__ import annotations

import os
import json
import asyncio
import asyncpg
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple, Union

# -----------------------------------------------------------------------------
# Pool
# -----------------------------------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL")
_POOL: Optional[asyncpg.Pool] = None
_POOL_LOCK = asyncio.Lock()


async def _get_pool() -> asyncpg.Pool:
    global _POOL
    if _POOL is not None:
        return _POOL
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")

    async with _POOL_LOCK:
        if _POOL is None:
            _POOL = await asyncpg.create_pool(
                dsn=DATABASE_URL,
                min_size=int(os.getenv("DB_POOL_MIN", "1")),
                max_size=int(os.getenv("DB_POOL_MAX", "5")),
                command_timeout=30,
            )
    return _POOL


async def close_pool():
    global _POOL
    if _POOL:
        await _POOL.close()
        _POOL = None


# -----------------------------------------------------------------------------
# Utils
# -----------------------------------------------------------------------------
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _as_ts(dt_or_iso: Optional[Any]) -> datetime:
    if isinstance(dt_or_iso, datetime):
        return dt_or_iso if dt_or_iso.tzinfo else dt_or_iso.replace(tzinfo=timezone.utc)
    if isinstance(dt_or_iso, str):
        try:
            ts = datetime.fromisoformat(dt_or_iso.replace("Z", "+00:00"))
            return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
        except Exception:
            pass
    return _now_utc()


async def _exec(query: str, *args) -> None:
    """
    Execute a single statement using the pool. If a table is missing,
    try to call db.schema.ensure_schema() once, then retry once.
    """
    pool = await _get_pool()
    try:
        async with pool.acquire() as conn:
            await conn.execute(query, *args)
    except asyncpg.UndefinedTableError:
        # Try to ensure schema, then retry once
        try:
            from db.schema import ensure_schema  # type: ignore
            await ensure_schema()
            async with pool.acquire() as conn:
                await conn.execute(query, *args)
        except Exception:
            # swallow to avoid breaking trading loop
            pass
    except Exception:
        # swallow logging errors; never break the trading loop
        pass


def _fire_and_forget(coro):
    try:
        asyncio.create_task(coro)
    except Exception:
        pass


def _f(x: Any) -> Optional[float]:
    if x is None or x == "":
        return None
    try:
        return float(x)
    except Exception:
        return None


def _i(x: Any) -> Optional[int]:
    if x is None or x == "":
        return None
    try:
        return int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return None


def _json_default(o: Any):
    try:
        return str(o)
    except Exception:
        return None


# -----------------------------------------------------------------------------
# Public API used by sniper/sniper.py
# -----------------------------------------------------------------------------
async def log_scan_batch(when_iso: Any, total: int, kept: int, reason_top: str) -> None:
    """
    INSERT scan iteration summary.
    """
    ts = _as_ts(when_iso)
    q = """INSERT INTO scan_batches (ts, total, kept, reason) VALUES ($1,$2,$3,$4)"""
    await _exec(q, ts, int(total), int(kept), str(reason_top or ""))


async def log_scan_row(row: Dict[str, Any]) -> None:
    """
    INSERT one ranked/acted candidate.
    """
    ts = _now_utc()
    mint   = row.get("mint")
    symbol = row.get("symbol")
    source = row.get("source")
    age    = _f(row.get("pool_age_min") or row.get("ageMin"))
    liq    = _f(row.get("liq_usd") or row.get("liquidity_usd") or row.get("liquidityUSD"))
    imp    = _f(row.get("impact_1k_bps") or row.get("priceImpact1kBps"))
    score  = _f(row.get("score"))
    rank   = _i(row.get("rank"))
    preset = (row.get("preset") or os.getenv("STRATEGY_PRESET") or "").lower()
    action = row.get("action")  # e.g. "buy_ok" | "skip_hp" | "rejected_ai" etc.
    extras = json.dumps(row, default=_json_default)

    q = """INSERT INTO scan_rows
           (ts, mint, symbol, source, pool_age_min, liq_usd, impact_1k_bps, score, rank, preset, action, extras)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)"""
    await _exec(q, ts, mint, symbol, source, age, liq, imp, score, rank, preset, action, extras)

    # best-effort legacy mirror
    q_legacy = """INSERT INTO trade_candidates
                  (mint, symbol, source, pool_age_min, liq_usd, impact_1k_bps, score)
                  VALUES ($1,$2,$3,$4,$5,$6,$7)"""
    await _exec(q_legacy, mint, symbol, source, age, liq, imp, score)


# -----------------------------------------------------------------------------
# Exits auto-registration helpers
# -----------------------------------------------------------------------------
_BUY_TYPES = {"buy", "buy_fill", "entry", "buy_ok", "filled_buy"}


def _coerce_entry_price_units(evt: Dict[str, Any]) -> Optional[float]:
    """
    Try to pick a consistent entry price for the exit engine.
    Preference:
      1) explicit USD field in evt or evt['data']
      2) fall back to entry_sol_per_token if EXITS_PRICE_UNITS=SOL
      3) if EXITS_PRICE_UNITS=USD but only SOL price exists and USD_PER_SOL is set, convert
    """
    # evt-level & evt.data-level access
    data = evt.get("data") or {}
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except Exception:
            data = {}

    # 1) direct USD fields
    for k in ("entry_usd_per_token", "entry_price_usd_per_token", "entry_price_usd"):
        v = evt.get(k)
        if v is None and isinstance(data, dict):
            v = data.get(k)
        if v is not None:
            v = _f(v)
            if v and v > 0:
                return v

    # 2) SOL field present?
    sol_per_tok = _f(evt.get("entry_sol_per_token"))
    if sol_per_tok is None and isinstance(data, dict):
        sol_per_tok = _f(data.get("entry_sol_per_token"))

    units = (os.getenv("EXITS_PRICE_UNITS") or "USD").strip().upper()
    if units == "SOL":
        # user wants SOL/token throughout
        return sol_per_tok

    # units = USD (default). If we only have SOL and a USD_PER_SOL env, convert once.
    if sol_per_tok is not None:
        usd_per_sol = _f(os.getenv("USD_PER_SOL"))
        if usd_per_sol and usd_per_sol > 0:
            return sol_per_tok * usd_per_sol

    # last resort: maybe the project is already logging USD into entry_sol_per_token
    if sol_per_tok and sol_per_tok > 0:
        return sol_per_tok

    return None


async def _auto_register_exit_if_buy(evt: Dict[str, Any]) -> None:
    """
    After writing the trade event, auto-register buys with the exit engine.
    Lazy imports avoid circular deps and keep failures non-fatal.
    """
    try:
        evt_type = (evt.get("type") or "").lower()
        if evt_type not in _BUY_TYPES:
            return

        mint = evt.get("mint")
        signature = evt.get("signature")
        symbol = evt.get("symbol")
        usd_spent = _f(evt.get("usd_spent"))
        if not mint or not signature or not usd_spent or usd_spent <= 0:
            return

        entry_price = _coerce_entry_price_units(evt)
        if entry_price is None or entry_price <= 0:
            # no reliable entry price → skip registration to avoid unit mismatch
            return

        # lazy imports
        from execution.exits import register_entry
        from execution.exits_wiring import price_fn, sell_fn

        # Hand off to exit engine (USD-sliced management by default)
        register_entry(
            signature=str(signature),
            mint=str(mint),
            qty=None,  # we manage by USD slice; qty not required
            entry_price=float(entry_price),   # MUST match units returned by price_fn
            symbol=symbol,
            usd_spent=float(usd_spent),
            price_fn=price_fn,
            sell_fn=sell_fn,
        )
    except Exception:
        # never let this break the trading loop
        pass


# -----------------------------------------------------------------------------
# Trade/Event loggers
# -----------------------------------------------------------------------------
async def log_trade_event(
    evt: Optional[Dict[str, Any]] = None,
    /,
    **kwargs: Any,
) -> None:
    """
    Generic trade event log.

    Accepts EITHER:
      - a single dict positional argument: log_trade_event({...})
      - keyword args: log_trade_event(type="buy", mint=..., ...)

    Table suggested schema:
      CREATE TABLE IF NOT EXISTS trade_events(
        id BIGSERIAL PRIMARY KEY,
        ts TIMESTAMPTZ NOT NULL DEFAULT now(),
        type TEXT,          -- e.g. 'buy','sell','buy_fail'
        mint TEXT,
        symbol TEXT,
        entry_sol_per_token DOUBLE PRECISION,
        sol_spent DOUBLE PRECISION,
        usd_spent DOUBLE PRECISION,
        signature TEXT,
        cu_price INT,
        jito_tip BIGINT,
        data JSONB
      );
    """
    # Normalize payload
    payload: Dict[str, Any]
    if evt is not None:
        payload = dict(evt)
    else:
        payload = dict(kwargs)

    ts = _now_utc()

    q = """INSERT INTO trade_events
           (ts, type, mint, symbol, entry_sol_per_token, sol_spent, usd_spent, signature, cu_price, jito_tip, data)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)"""

    data_blob = json.dumps(payload, default=_json_default)

    await _exec(
        q,
        ts,
        str(payload.get("type") or ""),
        payload.get("mint"),
        payload.get("symbol"),
        _f(payload.get("entry_sol_per_token")),
        _f(payload.get("sol_spent")),
        _f(payload.get("usd_spent")),
        payload.get("signature"),
        _i(payload.get("cu_price")),
        _i(payload.get("jito_tip")),
        data_blob,
    )

    # Fire-and-forget: auto-register buy positions with exit engine
    _fire_and_forget(_auto_register_exit_if_buy(payload))


async def log_error_event(where: str, err: str) -> None:
    """
    Error event.
    """
    ts = _now_utc()
    q = """INSERT INTO error_events (ts, "where", error) VALUES ($1,$2,$3)"""
    await _exec(q, ts, str(where or ""), str(err or ""))


async def log_pnl_snapshot(when_iso: Any, equity_usd: float, open_risk_usd: float) -> None:
    """
    Periodic equity snapshot for tuning daily curves.
    """
    ts = _as_ts(when_iso)
    q = """INSERT INTO pnl_snapshots (ts, equity_usd, open_risk_usd) VALUES ($1,$2,$3)"""
    await _exec(q, ts, _f(equity_usd), _f(open_risk_usd))


# -----------------------------------------------------------------------------
# Back-compat shims + legacy rows
# -----------------------------------------------------------------------------
async def log_candidate_async(c: Dict[str, Any]) -> None:
    row = dict(c)
    row.setdefault("action", "candidate")
    await log_scan_row(row)


def log_candidate(c: Dict[str, Any]) -> None:
    _fire_and_forget(log_candidate_async(c))


async def log_trade_async(mint, entry_px, exit_px, size_usd, pnl_usd, reason) -> None:
    """
    Minimal legacy trade record; still writes an event row and an optional legacy table row.
    """
    evt = {
        "type": "legacy_trade",
        "mint": mint,
        "symbol": None,
        "entry_sol_per_token": _f(entry_px),
        "sol_spent": None,
        "usd_spent": _f(size_usd),
        "signature": None,
        "cu_price": None,
        "jito_tip": None,
        "exit_px": _f(exit_px),
        "pnl_usd": _f(pnl_usd),
        "reason": reason,
    }
    await log_trade_event(evt)

    q = """INSERT INTO trades (mint, entry_px, exit_px, size_usd, pnl_usd, reason)
           VALUES ($1,$2,$3,$4,$5,$6)"""
    await _exec(q, mint, _f(entry_px), _f(exit_px), _f(size_usd), _f(pnl_usd), str(reason or ""))


def log_trade(*args, **kwargs) -> None:
    _fire_and_forget(log_trade_async(*args, **kwargs))


# New: exact name/shape used by execution/exits.py
async def record_legacy_trade(
    mint: str,
    entry_px: float,
    exit_px: float,
    size_usd: float,
    pnl_usd: float,
    reason: str,
) -> None:
    """
    Called by exit engine to mirror sells into a compact legacy table.
    """
    q = """INSERT INTO trades (mint, entry_px, exit_px, size_usd, pnl_usd, reason)
           VALUES ($1,$2,$3,$4,$5,$6)"""
    await _exec(q, mint, _f(entry_px), _f(exit_px), _f(size_usd), _f(pnl_usd), str(reason or ""))


# -----------------------------------------------------------------------------
# Tiny console row (unchanged)
# -----------------------------------------------------------------------------
def tiny_row(**cols):
    try:
        line = " | ".join(f"{k}={v}" for k, v in cols.items())
        print(f"[tiny] {line}")
    except Exception:
        pass
