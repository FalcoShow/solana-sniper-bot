# daytrader/data_feed.py
from __future__ import annotations

"""
daytrader.data_feed
-------------------
Async market data helpers for the Solana day-trader stack.

Design goals (God-Tier contract)
--------------------------------
- Safe: never raise to caller on data failures; return [] / None on failure.
- Deterministic: stable normalization and caching; no random jitter.
- Polite: TTL cache + request retries + timeouts.
- Minimal dependencies: aiohttp only.

Primary sources
---------------
1) Birdeye (minute OHLCV + spot) if BIRDEYE_API_KEY is set
2) Jupiter v6 price as spot fallback (no OHLCV)
3) OHLCV fallback: deterministic synthetic bars derived from spot
   (indicator continuity only; NOT real volume)

Public API
----------
- MINTS: dict[symbol] -> mint
- async get_recent_price(mint) -> (price_usd|None, source_str)
- async get_minute_bars(mint, limit=240) -> list[{t,o,h,l,c,v}] oldest->newest
- estimate_atr(bars, period=14) -> atr|None
- estimate_atr_pct(bars, period=14) -> atr_pct|None
- async estimate_beta_vs_sol(mint, lookback=240) -> beta|None
- async estimate_corr_vs_sol(mint, lookback=240) -> corr|None

Env
---
BIRDEYE_API_KEY           = <key or empty>
DATA_TIMEOUT_SEC          = int seconds (default 12)
DATA_RETRIES              = int (default 2)
DATA_CACHE_TTL            = int seconds (default 20)
DATA_CACHE_MAX_KEYS       = int max cache entries (default 1024)
DATA_MAX_LIMIT            = int max candles (default 1440)
DATA_SYNTH_ENABLE         = 0|1 (default 1)
DATA_SYNTH_VOL_PCT        = float (default 0.0006)  # ~0.06% per bar synthetic range
DATA_ALLOW_MINTS_JSON     = optional JSON to extend/override MINTS, e.g. {"POP": "...mint..."}
"""

import asyncio
import json
import math
import os
import time
from collections import OrderedDict
from typing import Any

import aiohttp


# =============================================================================
# Env parsing
# =============================================================================
def _env_str(name: str, default: str = "") -> str:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip()


def _env_int(
    name: str,
    default: int,
    *,
    min_value: int | None = None,
    max_value: int | None = None,
) -> int:
    raw = _env_str(name, "")
    try:
        val = int(raw) if raw else int(default)
    except Exception:
        val = int(default)

    if min_value is not None:
        val = max(int(min_value), val)
    if max_value is not None:
        val = min(int(max_value), val)
    return val


def _env_float(
    name: str,
    default: float,
    *,
    min_value: float | None = None,
    max_value: float | None = None,
) -> float:
    raw = _env_str(name, "")
    try:
        val = float(raw) if raw else float(default)
    except Exception:
        val = float(default)

    if min_value is not None:
        val = max(float(min_value), val)
    if max_value is not None:
        val = min(float(max_value), val)
    return val


def _env_bool(name: str, default: bool = False) -> bool:
    raw = _env_str(name, "")
    if not raw:
        return bool(default)
    return raw.lower() in ("1", "true", "yes", "y", "on")


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else float(default)
    except Exception:
        return float(default)


# =============================================================================
# Config
# =============================================================================
BIRDEYE_API_KEY = _env_str("BIRDEYE_API_KEY", "")
BIRDEYE_BASE = "https://public-api.birdeye.so"
JUP_PRICE_V6 = "https://price.jup.ag/v6/price"

DATA_TIMEOUT_SEC = _env_int("DATA_TIMEOUT_SEC", 12, min_value=3, max_value=60)
DATA_RETRIES = _env_int("DATA_RETRIES", 2, min_value=0, max_value=8)
DATA_CACHE_TTL = _env_int("DATA_CACHE_TTL", 20, min_value=0, max_value=600)
DATA_CACHE_MAX_KEYS = _env_int("DATA_CACHE_MAX_KEYS", 1024, min_value=64, max_value=50_000)
DATA_MAX_LIMIT = _env_int("DATA_MAX_LIMIT", 1440, min_value=60, max_value=10_000)

DATA_SYNTH_ENABLE = _env_bool("DATA_SYNTH_ENABLE", True)
DATA_SYNTH_VOL_PCT = _env_float("DATA_SYNTH_VOL_PCT", 0.0006, min_value=0.0, max_value=0.05)


# =============================================================================
# Mints
# =============================================================================
MINTS: dict[str, str] = {
    "SOL": "So11111111111111111111111111111111111111112",
    "USDC": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    "JUP": "JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB",
    "RAY": "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R",
    "BONK": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
    "WIF": "8V8Nr5az28G8e2x9k3iN3bQJmGm5q9nXYVJHsxgq1D5",
}

_raw_mints_json = _env_str("DATA_ALLOW_MINTS_JSON", "")
if _raw_mints_json:
    try:
        extra = json.loads(_raw_mints_json)
        if isinstance(extra, dict):
            for sym, mint in extra.items():
                if isinstance(sym, str) and isinstance(mint, str) and mint.strip():
                    MINTS[sym.strip().upper()] = mint.strip()
    except Exception:
        pass


# =============================================================================
# TTL cache (bounded)
# =============================================================================
# cache: key -> (expires_monotonic, value)
_cache: "OrderedDict[str, tuple[float, Any]]" = OrderedDict()
_cache_lock = asyncio.Lock()


def _now() -> float:
    return time.monotonic()


async def _cache_get(key: str) -> Any | None:
    async with _cache_lock:
        entry = _cache.get(key)
        if not entry:
            return None
        exp, val = entry
        if exp >= _now():
            _cache.move_to_end(key, last=True)
            return val
        _cache.pop(key, None)
        return None


async def _cache_set(key: str, val: Any, ttl: int = DATA_CACHE_TTL) -> None:
    if ttl <= 0:
        return
    async with _cache_lock:
        _cache[key] = (_now() + float(ttl), val)
        _cache.move_to_end(key, last=True)

        # bound size deterministically
        while len(_cache) > int(DATA_CACHE_MAX_KEYS):
            _cache.popitem(last=False)


# =============================================================================
# HTTP helpers
# =============================================================================
def _timeout() -> aiohttp.ClientTimeout:
    return aiohttp.ClientTimeout(total=float(DATA_TIMEOUT_SEC))


def _headers(extra: dict[str, str] | None = None) -> dict[str, str]:
    out = {
        "accept": "application/json",
        "user-agent": "sol-sniper-daytrader/1.0",
    }
    if extra:
        out.update(extra)
    return out


def _birdeye_headers() -> dict[str, str]:
    return _headers({"x-api-key": BIRDEYE_API_KEY}) if BIRDEYE_API_KEY else _headers()


def _should_retry_status(status: int) -> bool:
    return status == 429 or (500 <= status <= 599)


async def _request_json(
    session: aiohttp.ClientSession,
    url: str,
    *,
    headers: dict[str, str] | None = None,
) -> dict[str, Any] | None:
    """
    Safe JSON GET with retries + deterministic exponential backoff.
    Returns dict or None. (Cancellation propagates.)
    """
    delay = 0.45
    attempts = max(0, int(DATA_RETRIES)) + 1

    for attempt in range(attempts):
        try:
            async with session.get(url, headers=headers) as resp:
                status = int(resp.status)
                if status != 200:
                    if attempt < attempts - 1 and _should_retry_status(status):
                        await asyncio.sleep(delay)
                        delay = min(6.0, delay * 1.8)
                        continue
                    return None

                js = await resp.json(content_type=None)
                return js if isinstance(js, dict) else None

        except asyncio.CancelledError:
            raise
        except Exception:
            if attempt >= attempts - 1:
                return None
            await asyncio.sleep(delay)
            delay = min(6.0, delay * 1.8)

    return None


# =============================================================================
# Normalization
# =============================================================================
def _to_epoch_seconds(t: Any) -> int:
    """Accept seconds or ms epoch; return seconds."""
    try:
        ts = int(t)
    except Exception:
        return 0
    if ts > 2_000_000_000_000:  # ms epoch
        ts //= 1000
    return ts


def _norm_bar(item: dict[str, Any]) -> dict[str, Any] | None:
    """
    Normalize to {t,o,h,l,c,v} with numeric values.
    NOTE: output key is 'l' (low), but we never use ambiguous local var name `l`.
    """
    try:
        ts = _to_epoch_seconds(item.get("t", 0))
        close_px = _safe_float(item.get("c", 0.0), 0.0)
        if ts <= 0 or close_px <= 0.0:
            return None

        open_px = _safe_float(item.get("o", close_px), close_px)
        high_px = _safe_float(item.get("h", close_px), close_px)
        low_px = _safe_float(item.get("l", close_px), close_px)
        vol_px = _safe_float(item.get("v", 0.0), 0.0)

        high_norm = max(high_px, open_px, close_px)
        low_norm = min(low_px, open_px, close_px)

        return {
            "t": int(ts),
            "o": float(open_px),
            "h": float(high_norm),
            "l": float(low_norm),
            "c": float(close_px),
            "v": float(vol_px),
        }
    except Exception:
        return None


def _dedupe_and_sort(bars: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Oldest->newest, unique by timestamp."""
    if not bars:
        return []
    bars_sorted = sorted(bars, key=lambda b: int(b.get("t", 0)))
    out: list[dict[str, Any]] = []
    seen_ts: set[int] = set()
    for bar in bars_sorted:
        ts = int(bar.get("t", 0))
        if ts <= 0 or ts in seen_ts:
            continue
        seen_ts.add(ts)
        out.append(bar)
    return out


# =============================================================================
# Birdeye
# =============================================================================
async def _birdeye_spot(session: aiohttp.ClientSession, mint: str) -> float | None:
    if not BIRDEYE_API_KEY:
        return None
    url = f"{BIRDEYE_BASE}/defi/price?address={mint}"
    js = await _request_json(session, url, headers=_birdeye_headers())
    if not js:
        return None
    val = (js.get("data") or {}).get("value")
    px = _safe_float(val, 0.0)
    return px if px > 0.0 else None


async def _birdeye_ohlcv_1m(
    session: aiohttp.ClientSession,
    mint: str,
    limit: int,
) -> list[dict[str, Any]] | None:
    """Return oldest->newest list of normalized bars, or None."""
    if not BIRDEYE_API_KEY:
        return None

    limit_i = max(5, min(int(limit), DATA_MAX_LIMIT))
    now = int(time.time())
    time_from = now - limit_i * 60

    url = f"{BIRDEYE_BASE}/defi/ohlcv?address={mint}&type=1m&time_from={time_from}&time_to={now}"
    js = await _request_json(session, url, headers=_birdeye_headers())
    if not js:
        return None

    items = ((js.get("data") or {}).get("items")) or []
    if not isinstance(items, list) or not items:
        return None

    out: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        bar = _norm_bar(item)
        if bar:
            out.append(bar)

    out = _dedupe_and_sort(out)
    return out[-limit_i:] if out else None


# =============================================================================
# Jupiter
# =============================================================================
async def _jup_spot(session: aiohttp.ClientSession, mint: str) -> float | None:
    url = f"{JUP_PRICE_V6}?ids={mint}"
    js = await _request_json(session, url, headers=_headers())
    if not js:
        return None
    info = (js.get("data") or {}).get(mint) or {}
    px = _safe_float(info.get("price"), 0.0)
    return px if px > 0.0 else None


# =============================================================================
# Synthetic bars
# =============================================================================
def _synth_bars_from_spot(spot: float, limit: int) -> list[dict[str, Any]]:
    """
    Deterministic synthetic candles around spot price.
    ONLY a fallback to keep indicators from breaking when OHLCV source is unavailable.
    """
    if spot <= 0.0:
        return []

    limit_i = max(5, min(int(limit), DATA_MAX_LIMIT))
    now = int(time.time())

    rng = max(spot * max(0.0, float(DATA_SYNTH_VOL_PCT)), 1e-12)

    bars: list[dict[str, Any]] = []
    for idx in range(limit_i, 0, -1):
        ts = now - idx * 60

        w0 = math.sin(idx * 0.7) + 0.35 * math.sin(idx * 0.19)
        w0 = max(-1.0, min(1.0, w0))

        w1 = math.sin((idx + 1) * 0.7) + 0.35 * math.sin((idx + 1) * 0.19)
        w1 = max(-1.0, min(1.0, w1))

        close_px = spot + w0 * 0.25 * rng
        open_px = spot + w1 * 0.25 * rng
        high_px = max(open_px, close_px) + 0.5 * rng
        low_px = min(open_px, close_px) - 0.5 * rng

        bars.append(
            {
                "t": int(ts),
                "o": float(open_px),
                "h": float(high_px),
                "l": float(low_px),
                "c": float(close_px),
                "v": 0.0,
            }
        )

    return _dedupe_and_sort(bars)


# =============================================================================
# Public API
# =============================================================================
async def get_recent_price(mint: str) -> tuple[float | None, str]:
    """
    Return (price_usd, source) where source in {"birdeye","jupiter","none"}.
    Cached by mint for DATA_CACHE_TTL seconds.
    """
    mint_s = str(mint).strip()
    if not mint_s:
        return None, "none"

    cache_key = f"px:{mint_s}"
    cached = await _cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        async with aiohttp.ClientSession(timeout=_timeout()) as session:
            if BIRDEYE_API_KEY:
                px = await _birdeye_spot(session, mint_s)
                if px is not None:
                    res = (px, "birdeye")
                    await _cache_set(cache_key, res)
                    return res

            px = await _jup_spot(session, mint_s)
            if px is not None:
                res = (px, "jupiter")
                await _cache_set(cache_key, res)
                return res

    except asyncio.CancelledError:
        raise
    except Exception:
        pass

    res = (None, "none")
    await _cache_set(cache_key, res)
    return res


async def get_minute_bars(mint: str, limit: int = 240) -> list[dict[str, Any]]:
    """
    Return minute bars [{t,o,h,l,c,v}, ...] oldest->newest.
    Cached by (mint, limit).

    Attempts:
      1) Birdeye OHLCV (if key)
      2) Synthetic from spot (if DATA_SYNTH_ENABLE=1)
      3) [] on failure
    """
    mint_s = str(mint).strip()
    if not mint_s:
        return []

    limit_i = max(5, min(int(limit or 240), DATA_MAX_LIMIT))
    cache_key = f"bars:{mint_s}:{limit_i}"
    cached = await _cache_get(cache_key)
    if cached is not None:
        return cached

    bars: list[dict[str, Any]] = []
    try:
        async with aiohttp.ClientSession(timeout=_timeout()) as session:
            if BIRDEYE_API_KEY:
                got = await _birdeye_ohlcv_1m(session, mint_s, limit_i)
                if got:
                    bars = got

            if not bars and DATA_SYNTH_ENABLE:
                px = await _jup_spot(session, mint_s)
                if px is not None:
                    bars = _synth_bars_from_spot(px, limit_i)

    except asyncio.CancelledError:
        raise
    except Exception:
        bars = []

    bars = _dedupe_and_sort(bars)
    if len(bars) > limit_i:
        bars = bars[-limit_i:]

    await _cache_set(cache_key, bars)
    return bars


# =============================================================================
# Analytics helpers
# =============================================================================
def estimate_atr(bars: list[dict[str, Any]], period: int = 14) -> float | None:
    """ATR estimate in PRICE units."""
    try:
        n = len(bars)
        p = max(2, int(period))
        if n < p + 1:
            return None

        trs: list[float] = []
        for i in range(1, n):
            prev_close = float(bars[i - 1]["c"])
            high_px = float(bars[i]["h"])
            low_px = float(bars[i]["l"])
            tr = max(high_px - low_px, abs(high_px - prev_close), abs(low_px - prev_close))
            trs.append(tr)

        if len(trs) < p:
            return None
        return sum(trs[-p:]) / float(p)
    except Exception:
        return None


def estimate_atr_pct(bars: list[dict[str, Any]], period: int = 14) -> float | None:
    """ATR as fraction of last close (e.g., 0.02 => ~2%)."""
    try:
        atr = estimate_atr(bars, period=period)
        if atr is None:
            return None
        last_close = float(bars[-1]["c"])
        if last_close <= 0.0:
            return None
        return float(atr) / last_close
    except Exception:
        return None


def _log_returns_from_bars(bars: list[dict[str, Any]]) -> list[float]:
    out: list[float] = []
    for i in range(1, len(bars)):
        p0 = float(bars[i - 1]["c"])
        p1 = float(bars[i]["c"])
        if p0 > 0.0 and p1 > 0.0:
            out.append(math.log(p1 / p0))
    return out


async def estimate_beta_vs_sol(mint: str, lookback: int = 240) -> float | None:
    """Estimate beta vs SOL from minute log returns (OLS slope)."""
    try:
        mint_s = str(mint).strip()
        sol_mint = MINTS.get("SOL")
        if not mint_s or not sol_mint:
            return None
        if mint_s == sol_mint:
            return 1.0

        lb = max(20, min(int(lookback or 240), DATA_MAX_LIMIT))
        bars_x = await get_minute_bars(sol_mint, limit=lb)
        bars_y = await get_minute_bars(mint_s, limit=lb)

        n = min(len(bars_x), len(bars_y))
        if n < 20:
            return None

        rx = _log_returns_from_bars(bars_x[-n:])
        ry = _log_returns_from_bars(bars_y[-n:])
        m = min(len(rx), len(ry))
        if m < 20:
            return None

        mean_x = sum(rx[:m]) / m
        mean_y = sum(ry[:m]) / m
        cov = sum((rx[i] - mean_x) * (ry[i] - mean_y) for i in range(m))
        var_x = sum((rx[i] - mean_x) ** 2 for i in range(m))
        if var_x <= 1e-18:
            return None
        return cov / var_x
    except Exception:
        return None


async def estimate_corr_vs_sol(mint: str, lookback: int = 240) -> float | None:
    """Pearson correlation of log returns vs SOL."""
    try:
        mint_s = str(mint).strip()
        sol_mint = MINTS.get("SOL")
        if not mint_s or not sol_mint:
            return None
        if mint_s == sol_mint:
            return 1.0

        lb = max(20, min(int(lookback or 240), DATA_MAX_LIMIT))
        bars_x = await get_minute_bars(sol_mint, limit=lb)
        bars_y = await get_minute_bars(mint_s, limit=lb)

        n = min(len(bars_x), len(bars_y))
        if n < 20:
            return None

        rx = _log_returns_from_bars(bars_x[-n:])
        ry = _log_returns_from_bars(bars_y[-n:])
        m = min(len(rx), len(ry))
        if m < 20:
            return None

        mean_x = sum(rx[:m]) / m
        mean_y = sum(ry[:m]) / m
        var_x = sum((rx[i] - mean_x) ** 2 for i in range(m))
        var_y = sum((ry[i] - mean_y) ** 2 for i in range(m))
        if var_x <= 1e-18 or var_y <= 1e-18:
            return None

        cov = sum((rx[i] - mean_x) * (ry[i] - mean_y) for i in range(m))
        return cov / math.sqrt(var_x * var_y)
    except Exception:
        return None


__all__ = [
    "MINTS",
    "estimate_atr",
    "estimate_atr_pct",
    "estimate_beta_vs_sol",
    "estimate_corr_vs_sol",
    "get_minute_bars",
    "get_recent_price",
]
