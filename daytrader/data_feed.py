"""
daytrader.data_feed
-------------------
Async market data helpers for the Solana day-trader stack.

Priorities
- Minute OHLCV: Birdeye (if BIRDEYE_API_KEY exists), else synthesize from spot.
- Spot price: Birdeye (if key), else Jupiter v6 price.
- Safe: never raise to caller; return [] / None on failure.

Extras
- TTL caching to avoid hammering APIs.
- Simple volatility (ATR) and beta vs SOL to aid risk sizing.

Env:
  BIRDEYE_API_KEY   = <key or empty>
  DATA_TIMEOUT_SEC  = int (default 12)
  DATA_RETRIES      = int (default 2)
  DATA_CACHE_TTL    = int seconds (default 20)

Usage:
  from daytrader.data_feed import (
      MINTS, get_minute_bars, get_recent_price, estimate_atr, estimate_beta_vs_sol
  )

  bars = await get_minute_bars(MINTS["BONK"], limit=240)
  px, src = await get_recent_price(MINTS["SOL"])
  atr = estimate_atr(bars)
  beta = await estimate_beta_vs_sol(MINTS["BONK"])
"""

from __future__ import annotations

import os
import time
import math
import asyncio
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

# ----------------------------- Constants / Config -----------------------------

MINTS: Dict[str, str] = {
    "SOL":  "So11111111111111111111111111111111111111112",
    "USDC": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    "JUP":  "JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB",
    "RAY":  "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R",
    "BONK": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
    "WIF":  "8V8Nr5az28G8e2x9k3iN3bQJmGm5q9nXYVJHsxgq1D5",
}

BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY", "").strip()
BIRDEYE_BASE = "https://public-api.birdeye.so"
JUP_PRICE_V6 = "https://price.jup.ag/v6/price"

DATA_TIMEOUT_SEC = int(os.getenv("DATA_TIMEOUT_SEC", "12"))
DATA_RETRIES     = int(os.getenv("DATA_RETRIES", "2"))
DATA_CACHE_TTL   = int(os.getenv("DATA_CACHE_TTL", "20"))

# ----------------------------- Lightweight cache -----------------------------

# cache: key -> (expires_epoch, value)
_cache: Dict[str, Tuple[float, Any]] = {}

def _cache_get(key: str) -> Optional[Any]:
    v = _cache.get(key)
    if not v:
        return None
    exp, val = v
    if exp >= time.time():
        return val
    _cache.pop(key, None)
    return None

def _cache_set(key: str, val: Any, ttl: int = DATA_CACHE_TTL) -> None:
    if ttl <= 0:
        return
    _cache[key] = (time.time() + ttl, val)

# ----------------------------- HTTP helpers ----------------------------------

def _timeout() -> aiohttp.ClientTimeout:
    return aiohttp.ClientTimeout(total=DATA_TIMEOUT_SEC)

def _be_headers() -> Dict[str, str]:
    h = {"accept": "application/json"}
    if BIRDEYE_API_KEY:
        h["x-api-key"] = BIRDEYE_API_KEY
    return h

async def _with_retries(fn, *args, **kwargs):
    """
    Run an async HTTP call with simple backoff. fn must be an async callable.
    """
    delay = 0.6
    for attempt in range(DATA_RETRIES + 1):
        try:
            return await fn(*args, **kwargs)
        except Exception:
            if attempt >= DATA_RETRIES:
                return None
            await asyncio.sleep(delay)
            delay *= 1.8

# ----------------------------- Birdeye endpoints -----------------------------

async def _birdeye_ohlcv(session: aiohttp.ClientSession, mint: str, interval: str = "1m", limit: int = 240) -> Optional[List[Dict[str, Any]]]:
    """
    Fetch OHLCV bars newest->oldest from Birdeye; we return oldest->newest normalized.
    Returns: list[{t,o,h,l,c,v}]
    """
    if not BIRDEYE_API_KEY:
        return None
    now = int(time.time())
    time_from = now - limit * 60
    url = f"{BIRDEYE_BASE}/defi/ohlcv?address={mint}&type={interval}&time_from={time_from}&time_to={now}"
    async with session.get(url, headers=_be_headers(), timeout=_timeout()) as r:
        if r.status != 200:
            return None
        js = await r.json()
        items = ((js.get("data") or {}).get("items")) or []
        # Normalize oldest->newest
        items.sort(key=lambda x: x.get("t", 0))
        out: List[Dict[str, Any]] = []
        for x in items[-limit:]:
            out.append({
                "t": int(x.get("t", 0)),
                "o": float(x.get("o", x.get("c", 0)) or 0.0),
                "h": float(x.get("h", x.get("c", 0)) or 0.0),
                "l": float(x.get("l", x.get("c", 0)) or 0.0),
                "c": float(x.get("c", 0.0) or 0.0),
                "v": float(x.get("v", 0.0) or 0.0),
            })
        return out

async def _birdeye_spot(session: aiohttp.ClientSession, mint: str) -> Optional[float]:
    """
    Fetch spot price via Birdeye token price (USD). Returns float or None.
    """
    if not BIRDEYE_API_KEY:
        return None
    url = f"{BIRDEYE_BASE}/defi/price?address={mint}"
    async with session.get(url, headers=_be_headers(), timeout=_timeout()) as r:
        if r.status != 200:
            return None
        js = await r.json()
        # {"success":true,"data":{"value":1.0012,...}}
        val = ((js.get("data") or {}).get("value"))
        try:
            return float(val)
        except Exception:
            return None

# ----------------------------- Jupiter endpoints -----------------------------

async def _jup_spot(session: aiohttp.ClientSession, mint: str) -> Optional[float]:
    """
    Fetch spot via Jupiter v6 price API.
    """
    url = f"{JUP_PRICE_V6}?ids={mint}"
    async with session.get(url, timeout=_timeout()) as r:
        if r.status != 200:
            return None
        js = await r.json()
        info = ((js or {}).get("data") or {}).get(mint) or {}
        px = info.get("price")
        try:
            return float(px)
        except Exception:
            return None

# ----------------------------- Public API ------------------------------------

async def get_recent_price(mint: str) -> Tuple[Optional[float], str]:
    """
    Return (price_usd, source). source in {"birdeye","jupiter","none"}.
    Uses cache by mint for DATA_CACHE_TTL seconds.
    """
    ck = f"px:{mint}"
    cached = _cache_get(ck)
    if cached is not None:
        return cached  # (price, source)

    async with aiohttp.ClientSession(timeout=_timeout()) as s:
        # Try Birdeye first (if key is set)
        if BIRDEYE_API_KEY:
            px = await _with_retries(_birdeye_spot, s, mint)
            if px is not None and px > 0:
                _cache_set(ck, (px, "birdeye"))
                return px, "birdeye"
        # Fallback to Jupiter
        px = await _with_retries(_jup_spot, s, mint)
        if px is not None and px > 0:
            _cache_set(ck, (px, "jupiter"))
            return px, "jupiter"

    return None, "none"

async def get_minute_bars(mint: str, limit: int = 240) -> List[Dict[str, Any]]:
    """
    Return minute bars list of dicts: [{t, o, h, l, c, v}, ...] oldest->newest.

    Order of attempts:
      1) Birdeye 1-minute OHLCV (if key present)
      2) Synthesize from spot (Jupiter -> flat bars w/ tiny jitter)

    Always returns a list; may be empty on total failure.
    Cached by (mint, limit).
    """
    limit = max(5, min(int(limit or 240), 1440))
    ck = f"bars:{mint}:{limit}"
    cached = _cache_get(ck)
    if cached is not None:
        return cached

    try:
        async with aiohttp.ClientSession(timeout=_timeout()) as session:
            items: Optional[List[Dict[str, Any]]] = None
            if BIRDEYE_API_KEY:
                items = await _with_retries(_birdeye_ohlcv, session, mint, "1m", limit)
            if items:
                _cache_set(ck, items)
                return items

            # Fallback: synthesize bars from current spot
            spot = await _with_retries(_jup_spot, session, mint)
            if spot is None or spot <= 0:
                _cache_set(ck, [])
                return []

            now = int(time.time())
            bars: List[Dict[str, Any]] = []
            # Make 'limit' flat bars with tiny jitter to avoid degenerate indicators
            for i in range(limit, 0, -1):
                t = now - i * 60
                jitter = ((i % 7) - 3) * 1e-4 * spot
                o = spot + 0.5 * jitter
                c = spot + 1.0 * jitter
                h = max(o, c) * (1.0 + 6e-5)
                l = min(o, c) * (1.0 - 6e-5)
                bars.append({"t": t, "o": o, "h": h, "l": l, "c": c, "v": 0.0})

            _cache_set(ck, bars)
            return bars
    except Exception:
        _cache_set(ck, [])
        return []

# ----------------------------- Analytics helpers -----------------------------

def _true_range(prev_c: float, h: float, l: float, prev_candle_h: float, prev_candle_l: float) -> float:
    # Classic TR = max(h-l, abs(h-prev_close), abs(l-prev_close))
    return max(h - l, abs(h - prev_c), abs(l - prev_c))

def estimate_atr(bars: List[Dict[str, Any]], period: int = 14) -> Optional[float]:
    """
    Quick ATR estimate from provided bars (oldest->newest).
    Returns ATR in price units (same units as 'c').
    """
    n = len(bars)
    if n < period + 1:
        return None
    trs: List[float] = []
    for i in range(1, n):
        prev = bars[i - 1]
        cur  = bars[i]
        tr = max(cur["h"] - cur["l"], abs(cur["h"] - prev["c"]), abs(cur["l"] - prev["c"]))
        trs.append(tr)
    # Simple moving average of TRs over last 'period'
    if len(trs) < period:
        return None
    return sum(trs[-period:]) / period

async def estimate_beta_vs_sol(mint: str, lookback: int = 240) -> Optional[float]:
    """
    Estimate beta vs SOL using simple OLS slope on log returns.
    Returns None if insufficient data or degenerate variance.
    """
    if mint == MINTS["SOL"]:
        return 1.0
    bars_x = await get_minute_bars(MINTS["SOL"], limit=lookback)
    bars_y = await get_minute_bars(mint,          limit=lookback)
    if len(bars_x) < 20 or len(bars_y) < 20:
        return None
    # Align by length (oldest->newest already)
    n = min(len(bars_x), len(bars_y))
    rx: List[float] = []
    ry: List[float] = []
    for i in range(1, n):
        px0, px1 = bars_x[i - 1]["c"], bars_x[i]["c"]
        py0, py1 = bars_y[i - 1]["c"], bars_y[i]["c"]
        if px0 > 0 and py0 > 0 and px1 > 0 and py1 > 0:
            rx.append(math.log(px1 / px0))
            ry.append(math.log(py1 / py0))
    m = min(len(rx), len(ry))
    if m < 20:
        return None
    # OLS slope = cov(rx, ry) / var(rx)
    mean_x = sum(rx) / m
    mean_y = sum(ry) / m
    cov_xy = sum((rx[i] - mean_x) * (ry[i] - mean_y) for i in range(m))
    var_x  = sum((rx[i] - mean_x) ** 2 for i in range(m))
    if var_x <= 1e-18:
        return None
    return cov_xy / var_x

# ----------------------------- Convenience sync wrappers ---------------------

def get_recent_price_sync(mint: str) -> Tuple[Optional[float], str]:
    return asyncio.get_event_loop().run_until_complete(get_recent_price(mint))

def get_minute_bars_sync(mint: str, limit: int = 240) -> List[Dict[str, Any]]:
    return asyncio.get_event_loop().run_until_complete(get_minute_bars(mint, limit))
