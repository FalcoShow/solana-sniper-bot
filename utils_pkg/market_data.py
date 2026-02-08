from __future__ import annotations

"""
utils_pkg/market_data.py

God-Tier Market Data Facade (LOCK-IN, PRODUCTION)
================================================

Purpose
-------
Single import surface for market data across the entire bot, without coupling
callers to daytrader.* internals.

Hard contract (God-Tier)
------------------------
- Zero network I/O at import time.
- No heavy imports at import time (lazy-load daytrader.data_feed only when used).
- Helpers never raise (return safe defaults).
- Supports both async and sync implementations underneath (best-effort).

Stable public API
-----------------
- MINTS: Mapping[str, str] (symbol -> mint), lazy but safe
- async get_recent_price(mint) -> (price_usd|None, source_str)
- async get_minute_bars(mint, limit=240) -> list[bar] where bar has keys {t,o,h,l,c,v}
- estimate_atr(bars, period=14) -> float|None
- estimate_atr_pct(bars, period=14) -> float|None
- async estimate_beta_vs_sol(mint, lookback=240) -> float|None
- async estimate_corr_vs_sol(mint, lookback=240) -> float|None

Additive helpers (never raise)
------------------------------
- resolve_mint(symbol_or_mint) -> str
- bars_last_ts / bars_first_ts / bars_age_sec / is_stale_bars
- bars_range_sec / bars_is_monotonic / bars_summary

Notes
-----
This module must remain "thin": no networking, no aiohttp import, no redis/db.
"""

import asyncio
import importlib
import inspect
import threading
import time
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Final, Iterator

# =============================================================================
# Lazy load core feed module (avoid circular imports + heavy import at module import time)
# =============================================================================
_FEED_MOD_NAME: Final[str] = "daytrader.data_feed"
_feed_mod: Any | None = None
_feed_tried: bool = False
_feed_lock = threading.Lock()


def _load_feed() -> Any | None:
    """
    Lazy import daytrader.data_feed.
    Safe: never raises.
    Deterministic: caches result after first attempt.

    NOTE: mypy doesn't model other threads mutating globals between checks,
    so we use local snapshots to avoid "Statement is unreachable".
    """
    global _feed_mod, _feed_tried

    tried = _feed_tried  # local snapshot
    mod = _feed_mod
    if tried:
        return mod

    with _feed_lock:
        if _feed_tried:
            return _feed_mod

        _feed_tried = True
        try:
            _feed_mod = importlib.import_module(_FEED_MOD_NAME)
        except Exception:
            _feed_mod = None

        return _feed_mod


def _get_attr(name: str, default: Any) -> Any:
    """
    Fetch attribute from daytrader.data_feed, safe default if unavailable.
    """
    mod = _load_feed()
    if mod is None:
        return default
    try:
        return getattr(mod, name)
    except Exception:
        return default


async def _maybe_await(value: Any) -> Any:
    """
    Await if value is awaitable, otherwise return as-is.
    Safe: never raises to caller (except CancelledError).
    """
    try:
        if inspect.isawaitable(value):
            return await value
        return value
    except asyncio.CancelledError:
        raise
    except Exception:
        return None


# =============================================================================
# MINTS proxy (preserve "MINTS" import surface without hard-importing data_feed)
# =============================================================================
_DEFAULT_MINTS: Final[dict[str, str]] = {
    "SOL": "So11111111111111111111111111111111111111112",
    "USDC": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    "JUP": "JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB",
    "RAY": "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R",
    "BONK": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
    "WIF": "8V8Nr5az28G8e2x9k3iN3bQJmGm5q9nXYVJHsxgq1D5",
}


class LazyMints(Mapping[str, str]):
    """
    Read-only mapping proxy for MINTS with lazy import.
    Provides a safe fallback set if feed is unavailable.

    - No module import at import time.
    - Deterministic: caches computed mapping after first access.
    """

    __slots__ = ("_cache", "_lock")

    def __init__(self) -> None:
        self._cache: dict[str, str] | None = None
        self._lock = threading.Lock()

    def _compute(self) -> dict[str, str]:
        merged: dict[str, str] = dict(_DEFAULT_MINTS)

        mod = _load_feed()
        if mod is None:
            return merged

        try:
            feed_mints = getattr(mod, "MINTS", None)
            if isinstance(feed_mints, Mapping):
                for k, v in feed_mints.items():
                    if isinstance(k, str) and isinstance(v, str):
                        ks = k.strip().upper()
                        vs = v.strip()
                        if ks and vs:
                            merged[ks] = vs
        except Exception:
            return merged

        return merged

    def _value(self) -> Mapping[str, str]:
        cached = self._cache
        if cached is not None:
            return cached

        with self._lock:
            if self._cache is None:
                self._cache = self._compute()
            return self._cache

    def refresh(self) -> None:
        """Clear cached mapping so next access recomputes (safe)."""
        with self._lock:
            self._cache = None

    def __getitem__(self, key: str) -> str:
        return self._value()[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._value())

    def __len__(self) -> int:
        try:
            return len(self._value())
        except Exception:
            return 0

    def get(self, key: str, default: str | None = None) -> str | None:  # type: ignore[override]
        try:
            return self._value().get(key, default)
        except Exception:
            return default


MINTS: Final[LazyMints] = LazyMints()


def resolve_mint(symbol_or_mint: str) -> str:
    """
    Resolve a symbol like 'SOL' to its mint. If input already looks like a mint,
    returns it unchanged. Safe: never raises.
    """
    try:
        s = (symbol_or_mint or "").strip()
        if not s:
            return ""
        upper = s.upper()
        if upper in MINTS:
            return str(MINTS[upper])
        return s
    except Exception:
        return ""


# =============================================================================
# Stable API wrappers (async where underlying is expected async)
# =============================================================================
async def get_recent_price(mint: str) -> tuple[float | None, str]:
    """
    Safe facade to daytrader.data_feed.get_recent_price.
    Returns (price_usd|None, source_str). Never raises (except CancelledError).
    """
    fn = _get_attr("get_recent_price", None)
    if not callable(fn):
        return None, "none"

    try:
        out = await _maybe_await(fn(mint))
        if isinstance(out, tuple) and len(out) == 2:
            price, source = out
            price_f = float(price) if price is not None else None
            source_s = str(source) if source is not None else "none"
            return price_f, source_s
        return None, "none"
    except asyncio.CancelledError:
        raise
    except Exception:
        return None, "none"


async def get_minute_bars(mint: str, limit: int = 240) -> list[dict[str, Any]]:
    """
    Safe facade to daytrader.data_feed.get_minute_bars.
    Returns list of bars oldest->newest. Never raises (except CancelledError).
    """
    fn = _get_attr("get_minute_bars", None)
    if not callable(fn):
        return []

    try:
        lim = max(1, min(int(limit or 240), 10_000))
        out = await _maybe_await(fn(mint, limit=lim))
        if not isinstance(out, list):
            return []
        # keep dict-only; callers assume mapping-like bars
        return [b for b in out if isinstance(b, dict)]
    except asyncio.CancelledError:
        raise
    except Exception:
        return []


def estimate_atr(bars: list[dict[str, Any]], period: int = 14) -> float | None:
    """
    Safe facade to daytrader.data_feed.estimate_atr (usually sync).
    Never raises.
    """
    fn = _get_attr("estimate_atr", None)
    if not callable(fn):
        return None
    try:
        out = fn(bars, period=period)
        return float(out) if out is not None else None
    except Exception:
        return None


def estimate_atr_pct(bars: list[dict[str, Any]], period: int = 14) -> float | None:
    """
    Safe facade to daytrader.data_feed.estimate_atr_pct (usually sync).
    Never raises.
    """
    fn = _get_attr("estimate_atr_pct", None)
    if not callable(fn):
        return None
    try:
        out = fn(bars, period=period)
        return float(out) if out is not None else None
    except Exception:
        return None


async def estimate_beta_vs_sol(mint: str, lookback: int = 240) -> float | None:
    """
    Safe facade to daytrader.data_feed.estimate_beta_vs_sol (async or sync).
    Never raises (except CancelledError).
    """
    fn = _get_attr("estimate_beta_vs_sol", None)
    if not callable(fn):
        return None
    try:
        out = await _maybe_await(fn(mint, lookback=lookback))
        return float(out) if out is not None else None
    except asyncio.CancelledError:
        raise
    except Exception:
        return None


async def estimate_corr_vs_sol(mint: str, lookback: int = 240) -> float | None:
    """
    Safe facade to daytrader.data_feed.estimate_corr_vs_sol (async or sync).
    Never raises (except CancelledError).
    """
    fn = _get_attr("estimate_corr_vs_sol", None)
    if not callable(fn):
        return None
    try:
        out = await _maybe_await(fn(mint, lookback=lookback))
        return float(out) if out is not None else None
    except asyncio.CancelledError:
        raise
    except Exception:
        return None


# =============================================================================
# Bars helpers (never raise)
# =============================================================================
def _coerce_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        try:
            return int(float(str(x)))
        except Exception:
            return int(default)


def _bar_ts(bar: Mapping[str, Any]) -> int:
    """
    Extract a bar timestamp from common keys.
    Supports: t, ts, time, unixTime
    """
    try:
        v = bar.get("t", bar.get("ts", bar.get("time", bar.get("unixTime", 0))))
        return _coerce_int(v, 0)
    except Exception:
        return 0


def bars_last_ts(bars: Iterable[Mapping[str, Any]]) -> int:
    try:
        if isinstance(bars, Sequence):
            if not bars:
                return 0
            return _bar_ts(bars[-1])
        last = 0
        for b in bars:
            last = _bar_ts(b)
        return int(last)
    except Exception:
        return 0


def bars_first_ts(bars: Iterable[Mapping[str, Any]]) -> int:
    try:
        if isinstance(bars, Sequence):
            if not bars:
                return 0
            return _bar_ts(bars[0])
        for b in bars:
            return _bar_ts(b)
        return 0
    except Exception:
        return 0


def bars_age_sec(
    bars: Iterable[Mapping[str, Any]],
    now_ts: int | float | None = None,
) -> float | None:
    try:
        last_ts = bars_last_ts(bars)
        if last_ts <= 0:
            return None
        now_s = _coerce_int(now_ts, int(time.time())) if now_ts is not None else int(time.time())
        return float(max(0, now_s - int(last_ts)))
    except Exception:
        return None


def is_stale_bars(
    bars: Iterable[Mapping[str, Any]],
    *,
    max_age_sec: int = 180,
    now_ts: int | float | None = None,
) -> bool:
    try:
        age = bars_age_sec(bars, now_ts=now_ts)
        if age is None:
            return True
        return float(age) > float(max(1, _coerce_int(max_age_sec, 180)))
    except Exception:
        return True


def bars_range_sec(bars: Iterable[Mapping[str, Any]]) -> float:
    """
    Returns (last_ts - first_ts) in seconds, or 0.0 if unknown.
    """
    try:
        first = bars_first_ts(bars)
        last = bars_last_ts(bars)
        if first <= 0 or last <= 0 or last < first:
            return 0.0
        return float(last - first)
    except Exception:
        return 0.0


def bars_is_monotonic(bars: Iterable[Mapping[str, Any]]) -> bool:
    """
    True if timestamps are non-decreasing; otherwise False.
    """
    try:
        prev = -1
        for b in bars:
            ts = _bar_ts(b)
            if ts < prev:
                return False
            prev = ts
        return True
    except Exception:
        return False


def bars_summary(
    bars: Iterable[Mapping[str, Any]],
    *,
    max_age_sec: int = 180,
    now_ts: int | float | None = None,
) -> dict[str, Any]:
    """
    Small diagnostics bundle for logs/telemetry; safe.
    """
    try:
        seq = list(bars)
        first = bars_first_ts(seq)
        last = bars_last_ts(seq)
        age = bars_age_sec(seq, now_ts=now_ts)
        return {
            "n": len(seq),
            "first_ts": first,
            "last_ts": last,
            "age_sec": age,
            "range_sec": bars_range_sec(seq),
            "monotonic": bars_is_monotonic(seq),
            "stale": is_stale_bars(seq, max_age_sec=max_age_sec, now_ts=now_ts),
        }
    except Exception:
        return {
            "n": 0,
            "first_ts": 0,
            "last_ts": 0,
            "age_sec": None,
            "range_sec": 0.0,
            "monotonic": False,
            "stale": True,
        }


__all__ = [
    "MINTS",
    "resolve_mint",
    "get_recent_price",
    "get_minute_bars",
    "estimate_atr",
    "estimate_atr_pct",
    "estimate_beta_vs_sol",
    "estimate_corr_vs_sol",
    "bars_last_ts",
    "bars_first_ts",
    "bars_age_sec",
    "is_stale_bars",
    "bars_range_sec",
    "bars_is_monotonic",
    "bars_summary",
]
