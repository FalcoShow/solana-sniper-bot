# db/logger.py — v2.0 (pooled, tolerant, v7 sniper-compatible)
from __future__ import annotations

import os
import json
import asyncio
import asyncpg
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

DATABASE_URL = os.getenv("DATABASE_URL")
_POOL: Optional[asyncpg.Pool] = None
_POOL_LOCK = asyncio.Lock()

# ---------------------------------------------------------------------------
# Pool helpers
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _as_ts(dt_or_iso: Optional[Any]) -> datetime:
    if isinstance(dt_or_iso, datetime):
        return dt_or_iso
    if isinstance(dt_or_iso, str):
        try:
            # fromisoformat handles `+00:00`; ensure tz-aware
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

# Non-blocking fire-and-forget wrapper
def _fire_and_forget(coro):
    try:
        asyncio.create_task(coro)
    except Exception:
        pass

# ---------------------------------------------------------------------------
# Public API expected by sniper/sniper.py v7
# ---------------------------------------------------------------------------
async def log_scan_batch(when_iso: Any, total: int, kept: int, reason_top: str) -> None:
    """
    Insert a per-iteration summary row.
    Table suggested schema:
      CREATE TABLE IF NOT EXISTS scan_batches(
        id BIGSERIAL PRIMARY KEY,
        ts TIMESTAMPTZ NOT NULL DEFAULT now(),
        total INT,
        kept INT,
        reason TEXT
      );
    """
    ts = _as_ts(when_iso)
    q = """INSERT INTO scan_batches (ts, total, kept, reason) VALUES ($1,$2,$3,$4)"""
    await _exec(q, ts, int(total), int(kept), str(reason_top or ""))

async def log_scan_row(row: Dict[str, Any]) -> None:
    """
    Log a single ranked/acted candidate.
    Table suggested schema:
      CREATE TABLE IF NOT EXISTS scan_rows(
        id BIGSERIAL PRIMARY KEY,
        ts TIMESTAMPTZ NOT NULL DEFAULT now(),
        mint TEXT,
        symbol TEXT,
        source TEXT,
        pool_age_min REAL,
        liq_usd REAL,
        impact_1k_bps REAL,
        score REAL,
        rank INT,
        preset TEXT,
        action TEXT,
        extras JSONB
      );
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

    # store the full row as extras for later analysis
    extras = json.dumps(row, default=_json_default)

    q = """INSERT INTO scan_rows
           (ts, mint, symbol, source, pool_age_min, liq_usd, impact_1k_bps, score, rank, preset, action, extras)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)"""
    await _exec(q, ts, mint, symbol, source, age, liq, imp, score, rank, preset, action, extras)

    # Back-compat: also fill legacy trade_candidates if it exists
    q_legacy = """INSERT INTO trade_candidates
                  (mint, symbol, source, pool_age_min, liq_usd, impact_1k_bps, score)
                  VALUES ($1,$2,$3,$4,$5,$6,$7)"""
    await _exec(q_legacy, mint, symbol, source, age, liq, imp, score)

async def log_trade_event(evt: Dict[str, Any]) -> None:
    """
    Generic trade event log. Pass the dict you build in sniper.
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
    ts = _now_utc()
    data = json.dumps(evt, default=_json_default)
    q = """INSERT INTO trade_events
           (ts, type, mint, symbol, entry_sol_per_token, sol_spent, usd_spent, signature, cu_price, jito_tip, data)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)"""

    await _exec(
        q,
        ts,
        str(evt.get("type") or ""),
        evt.get("mint"),
        evt.get("symbol"),
        _f(evt.get("entry_sol_per_token")),
        _f(evt.get("sol_spent")),
        _f(evt.get("usd_spent")),
        evt.get("signature"),
        _i(evt.get("cu_price")),
        _i(evt.get("jito_tip")),
        data,
    )

async def log_error_event(where: str, err: str) -> None:
    """
    Table suggested schema:
      CREATE TABLE IF NOT EXISTS error_events(
        id BIGSERIAL PRIMARY KEY,
        ts TIMESTAMPTZ NOT NULL DEFAULT now(),
        where TEXT,
        error TEXT
      );
    """
    ts = _now_utc()
    q = """INSERT INTO error_events (ts, "where", error) VALUES ($1,$2,$3)"""
    await _exec(q, ts, str(where or ""), str(err or ""))

async def log_pnl_snapshot(when_iso: Any, equity_usd: float, open_risk_usd: float) -> None:
    """
    Periodic equity snapshot for tuning daily curves.
    Table suggested schema:
      CREATE TABLE IF NOT EXISTS pnl_snapshots(
        id BIGSERIAL PRIMARY KEY,
        ts TIMESTAMPTZ NOT NULL,
        equity_usd DOUBLE PRECISION,
        open_risk_usd DOUBLE PRECISION
      );
    """
    ts = _as_ts(when_iso)
    q = """INSERT INTO pnl_snapshots (ts, equity_usd, open_risk_usd) VALUES ($1,$2,$3)"""
    await _exec(q, ts, _f(equity_usd), _f(open_risk_usd))

# ---------------------------------------------------------------------------
# Back-compat shims (match your original file’s API)
# ---------------------------------------------------------------------------
async def log_candidate_async(c: Dict[str, Any]) -> None:
    # Map to the richer scan_row logger
    row = dict(c)
    row.setdefault("action", "candidate")
    await log_scan_row(row)

def log_candidate(c: Dict[str, Any]) -> None:
    _fire_and_forget(log_candidate_async(c))

async def log_trade_async(mint, entry_px, exit_px, size_usd, pnl_usd, reason) -> None:
    """
    Original minimal trade log. We'll store it in trade_events and also try
    to write to a legacy 'trades' table if it exists.
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

    # Optional legacy table
    q = """INSERT INTO trades (mint, entry_px, exit_px, size_usd, pnl_usd, reason)
           VALUES ($1,$2,$3,$4,$5,$6)"""
    await _exec(q, mint, _f(entry_px), _f(exit_px), _f(size_usd), _f(pnl_usd), str(reason or ""))

def log_trade(*args, **kwargs) -> None:
    _fire_and_forget(log_trade_async(*args, **kwargs))

# ---------------------------------------------------------------------------
# Tiny “tuning table” print helper (optional; for quick console visibility)
# ---------------------------------------------------------------------------
def tiny_row(**cols):
    """
    Print a one-line, low-noise row to stdout/log for quick tuning.
    Example:
        tiny_row(act="BUY", sym="FOO", liq=12_345, age=9, sc=18.2)
    """
    try:
        line = " | ".join(f"{k}={v}" for k, v in cols.items())
        # Use print to avoid coupling with global logging config
        print(f"[tiny] {line}")
    except Exception:
        pass

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
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
