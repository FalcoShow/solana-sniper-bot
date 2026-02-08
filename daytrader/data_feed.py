# daytrader/data_feed.py
from __future__ import annotations

"""
daytrader.data_feed
-------------------
Async market data helpers for the Solana day-trader stack.

God-Tier contract (DO NOT BREAK)
--------------------------------
- Safe: never raise to caller on data failures; return [] / None on failure.
- Deterministic: stable normalization and caching; no random jitter.
- Polite: TTL cache + request retries + timeouts; bounded memory.
- Minimal dependencies: aiohttp only.

Primary sources (spot + OHLCV)
------------------------------
1) Birdeye (minute OHLCV + spot) if BIRDEYE_API_KEY is set
2) Jupiter price (spot fallback) via JUP_PRICE_URL (?ids=)
3) DexScreener (spot fallback) via /tokens/v1/{chain}/{tokenAddress}
4) OHLCV fallback: deterministic synthetic bars derived from spot
   (indicator continuity only; NOT real volume)

Public API (stable)
-------------------
- MINTS: dict[symbol] -> mint
- async get_recent_price(mint) -> (price_usd|None, source_str)
- async get_minute_bars(mint, limit=240) -> list[{t,o,h,l,c,v}] oldest->newest
- estimate_atr(bars, period=14) -> atr|None
- estimate_atr_pct(bars, period=14) -> atr_pct|None
- async estimate_beta_vs_sol(mint, lookback=240) -> beta|None
- async estimate_corr_vs_sol(mint, lookback=240) -> corr|None

Env (additive)
--------------
BIRDEYE_API_KEY               = <key or empty>
DATA_TIMEOUT_SEC              = int seconds (default 12)
DATA_RETRIES                  = int (default 2)
DATA_CACHE_TTL                = int seconds (default 20)
DATA_CACHE_MAX_KEYS           = int max cache entries (default 1024)
DATA_MAX_LIMIT                = int max candles (default 1440)
DATA_STALE_BARS_SEC           = int (default 180)  # bars older than this => stale => fallback
DATA_SYNTH_ENABLE             = 0|1 (default 1)
DATA_SYNTH_VOL_PCT            = float (default 0.0006)  # ~0.06% per bar synthetic range
DATA_ALLOW_MINTS_JSON         = optional JSON to extend/override MINTS

JUP_PRICE_URL                 = default "https://price.jup.ag/v6/price"
JUP_PRICE_API_KEY             = optional; if set, sent as header "x-api-key"
JUP_PRICE_VS_TOKEN            = optional mint; if set => adds &vsToken=<mint>

DEXSCREENER_ENABLE            = 0|1 (default 1)
DEXSCREENER_CHAIN             = default "solana"
DEXSCREENER_BASE              = default "https://api.dexscreener.com"
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
# Env parsing (safe, no-raise)
# =============================================================================
def _env_str(name: str, default: str = "") -> str:
    raw = os.getenv(name)
    return default if raw is None else str(raw).strip()


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


def _is_safe_mode() -> bool:
    # Must stay aligned with contract tests + offline/safe import expectations.
    return bool(
        _env_bool("PYTEST_MODE", False)
        or _env_bool("SAFE_MODE", False)
        or _env_bool("NO_NETWORK", False)
        or _env_bool("DRYRUN", False)
        or _env_bool("DRY_RUN", False)
        or _env_bool("BACKTEST_MODE", False)
    )


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        val = float(x)
        return val if math.isfinite(val) else float(default)
    except Exception:
        return float(default)


# =============================================================================
# Config
# =============================================================================
SRC_NONE = "none"
SRC_BIRDEYE = "birdeye"
SRC_JUPITER = "jupiter"
SRC_DEXSCREENER = "dexscreener"

BIRDEYE_API_KEY = _env_str("BIRDEYE_API_KEY", "")
BIRDEYE_BASE = "https://public-api.birdeye.so"

DATA_TIMEOUT_SEC = _env_int("DATA_TIMEOUT_SEC", 12, min_value=3, max_value=60)
DATA_RETRIES = _env_int("DATA_RETRIES", 2, min_value=0, max_value=8)
DATA_CACHE_TTL = _env_int("DATA_CACHE_TTL", 20, min_value=0, max_value=600)
DATA_CACHE_MAX_KEYS = _env_int("DATA_CACHE_MAX_KEYS", 1024, min_value=64, max_value=50_000)
DATA_MAX_LIMIT = _env_int("DATA_MAX_LIMIT", 1440, min_value=60, max_value=10_000)
DATA_STALE_BARS_SEC = _env_int("DATA_STALE_BARS_SEC", 180, min_value=30, max_value=3600)

DATA_SYNTH_ENABLE = _env_bool("DATA_SYNTH_ENABLE", True)
DATA_SYNTH_VOL_PCT = _env_float("DATA_SYNTH_VOL_PCT", 0.0006, min_value=0.0, max_value=0.05)

JUP_PRICE_URL = _env_str("JUP_PRICE_URL", "https://price.jup.ag/v6/price")
JUP_PRICE_API_KEY = _env_str("JUP_PRICE_API_KEY", "")
JUP_PRICE_VS_TOKEN = _env_str("JUP_PRICE_VS_TOKEN", "")

DEXSCREENER_ENABLE = _env_bool("DEXSCREENER_ENABLE", True)
DEXSCREENER_BASE = _env_str("DEXSCREENER_BASE", "https://api.dexscreener.com")
DEXSCREENER_CHAIN = _env_str("DEXSCREENER_CHAIN", "solana") or "solana"


# =============================================================================
# Mints (stable defaults + optional override)
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
# TTL cache (bounded, deterministic)
# =============================================================================
# cache: key -> (expires_monotonic, value)
_cache: OrderedDict[str, tuple[float, Any]] = OrderedDict()
_cache_lock = asyncio.Lock()


def _now_mono() -> float:
    return time.monotonic()


async def _cache_get(key: str) -> Any | None:
    try:
        async with _cache_lock:
            entry = _cache.get(key)
            if entry is None:
                return None
            exp, val = entry
            if exp >= _now_mono():
                _cache.move_to_end(key, last=True)
                return val
            _cache.pop(key, None)
            return None
    except Exception:
        return None


async def _cache_set(key: str, val: Any, ttl: int = DATA_CACHE_TTL) -> None:
    if ttl <= 0:
        return
    try:
        async with _cache_lock:
            _cache[key] = (_now_mono() + float(ttl), val)
            _cache.move_to_end(key, last=True)
            while len(_cache) > int(DATA_CACHE_MAX_KEYS):
                _cache.popitem(last=False)
    except Exception:
        return


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


def _jup_headers() -> dict[str, str]:
    return _headers({"x-api-key": JUP_PRICE_API_KEY}) if JUP_PRICE_API_KEY else _headers()


def _should_retry_status(status: int) -> bool:
    return status == 429 or (500 <= status <= 599)


async def _request_json(
    session: aiohttp.ClientSession,
    url: str,
    *,
    headers: dict[str, str] | None = None,
) -> Any | None:
    """
    Safe JSON GET with retries + deterministic exponential backoff.
    Returns parsed JSON (dict/list) or None. Cancellation propagates.
    """
    if _is_safe_mode():
        return None

    attempts = max(0, int(DATA_RETRIES)) + 1
    delay = 0.45

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

                try:
                    js = await resp.json(content_type=None)
                except Exception:
                    return None

                return js if isinstance(js, (dict, list)) else None

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


def _pick_ts(item: dict[str, Any]) -> int:
    """
    Be resilient to vendor field variations.
    Our normalized key is always 't' (seconds epoch).
    """
    ts = _to_epoch_seconds(item.get("t", 0))
    if ts:
        return ts
    ts = _to_epoch_seconds(item.get("unixTime", 0))
    if ts:
        return ts
    ts = _to_epoch_seconds(item.get("time", 0))
    if ts:
        return ts
    return 0


def _norm_bar(item: dict[str, Any]) -> dict[str, Any] | None:
    """
    Normalize to {t,o,h,l,c,v} with numeric values.
    Output key is 'l' (low), but we never use ambiguous local var name `l`.
    """
    try:
        ts = _pick_ts(item)
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


def _bars_last_ts(bars: list[dict[str, Any]]) -> int:
    try:
        return int(bars[-1].get("t", 0)) if bars else 0
    except Exception:
        return 0


def _bars_are_stale(bars: list[dict[str, Any]], *, max_age_sec: int = DATA_STALE_BARS_SEC) -> bool:
    if not bars:
        return True
    last_ts = _bars_last_ts(bars)
    if last_ts <= 0:
        return True
    now_s = int(time.time())
    return (now_s - last_ts) > int(max_age_sec)


# =============================================================================
# Birdeye
# =============================================================================
async def _birdeye_spot(session: aiohttp.ClientSession, mint: str) -> float | None:
    if not BIRDEYE_API_KEY:
        return None
    url = f"{BIRDEYE_BASE}/defi/price?address={mint}"
    js = await _request_json(session, url, headers=_birdeye_headers())
    if not isinstance(js, dict):
        return None
    data = js.get("data")
    if not isinstance(data, dict):
        return None
    px = _safe_float(data.get("value"), 0.0)
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
    now_s = int(time.time())
    time_from = now_s - limit_i * 60

    url = f"{BIRDEYE_BASE}/defi/ohlcv?address={mint}&type=1m&time_from={time_from}&time_to={now_s}"
    js = await _request_json(session, url, headers=_birdeye_headers())
    if not isinstance(js, dict):
        return None

    data = js.get("data")
    if not isinstance(data, dict):
        return None

    items = data.get("items")
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
    if not out or _bars_are_stale(out):
        return None

    return out[-limit_i:]


# =============================================================================
# Jupiter
# =============================================================================
async def _jup_spot(session: aiohttp.ClientSession, mint: str) -> float | None:
    url = f"{JUP_PRICE_URL}?ids={mint}"
    if JUP_PRICE_VS_TOKEN:
        url = f"{url}&vsToken={JUP_PRICE_VS_TOKEN}"

    js = await _request_json(session, url, headers=_jup_headers())
    if not isinstance(js, dict):
        return None

    data = js.get("data")
    if not isinstance(data, dict):
        return None

    info = data.get(mint)
    if not isinstance(info, dict):
        return None

    px = _safe_float(info.get("price"), 0.0)
    return px if px > 0.0 else None


# =============================================================================
# DexScreener (spot fallback)
# =============================================================================
def _dex_tokens_v1_url(mint: str) -> str:
    base = DEXSCREENER_BASE.rstrip("/")
    chain = (DEXSCREENER_CHAIN or "solana").strip()
    return f"{base}/tokens/v1/{chain}/{mint}"


def _pick_best_dex_pair(pairs: list[dict[str, Any]]) -> dict[str, Any] | None:
    best: dict[str, Any] | None = None
    best_liq = -1.0
    best_vol = -1.0

    for pair in pairs:
        if not isinstance(pair, dict):
            continue

        liq_obj = pair.get("liquidity")
        vol_obj = pair.get("volume")

        liq_usd = _safe_float(liq_obj.get("usd"), 0.0) if isinstance(liq_obj, dict) else 0.0
        vol_h24 = _safe_float(vol_obj.get("h24"), 0.0) if isinstance(vol_obj, dict) else 0.0

        if liq_usd > best_liq or (liq_usd == best_liq and vol_h24 > best_vol):
            best = pair
            best_liq = liq_usd
            best_vol = vol_h24

    return best


async def _dex_spot(session: aiohttp.ClientSession, mint: str) -> float | None:
    if not DEXSCREENER_ENABLE:
        return None

    url = _dex_tokens_v1_url(mint)
    js = await _request_json(session, url, headers=_headers())

    pairs: list[dict[str, Any]] = []
    if isinstance(js, list):
        pairs = [p for p in js if isinstance(p, dict)]
    elif isinstance(js, dict):
        raw_pairs = js.get("pairs")
        if isinstance(raw_pairs, list):
            pairs = [p for p in raw_pairs if isinstance(p, dict)]

    if not pairs:
        return None

    best = _pick_best_dex_pair(pairs)
    if not best:
        return None

    px = _safe_float(best.get("priceUsd"), 0.0)
    return px if px > 0.0 else None


# =============================================================================
# Synthetic bars (deterministic fallback)
# =============================================================================
def _synth_bars_from_spot(spot: float, limit: int) -> list[dict[str, Any]]:
    """
    Deterministic synthetic candles around spot price.
    ONLY a fallback to keep indicators from breaking when OHLCV source is unavailable.
    """
    if spot <= 0.0:
        return []

    limit_i = max(5, min(int(limit), DATA_MAX_LIMIT))
    now_s = int(time.time())

    rng = max(spot * max(0.0, float(DATA_SYNTH_VOL_PCT)), 1e-12)

    bars: list[dict[str, Any]] = []
    for idx in range(limit_i, 0, -1):
        ts = now_s - idx * 60

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
    Return (price_usd, source) where source in {"birdeye","jupiter","dexscreener","none"}.
    Cached by mint for DATA_CACHE_TTL seconds.
    """
    mint_s = str(mint).strip()
    if not mint_s:
        return None, SRC_NONE

    cache_key = f"px:{mint_s}"
    cached = await _cache_get(cache_key)
    if cached is not None:
        return cached

    if _is_safe_mode():
        res = (None, SRC_NONE)
        await _cache_set(cache_key, res)
        return res

    try:
        async with aiohttp.ClientSession(timeout=_timeout()) as session:
            if BIRDEYE_API_KEY:
                px_b = await _birdeye_spot(session, mint_s)
                if px_b is not None:
                    res = (px_b, SRC_BIRDEYE)
                    await _cache_set(cache_key, res)
                    return res

            px_j = await _jup_spot(session, mint_s)
            if px_j is not None:
                res = (px_j, SRC_JUPITER)
                await _cache_set(cache_key, res)
                return res

            px_d = await _dex_spot(session, mint_s)
            if px_d is not None:
                res = (px_d, SRC_DEXSCREENER)
                await _cache_set(cache_key, res)
                return res

    except asyncio.CancelledError:
        raise
    except Exception:
        pass

    res = (None, SRC_NONE)
    await _cache_set(cache_key, res)
    return res


async def get_minute_bars(mint: str, limit: int = 240) -> list[dict[str, Any]]:
    """
    Return minute bars [{t,o,h,l,c,v}, ...] oldest->newest.
    Cached by (mint, limit).

    Attempts:
      1) Birdeye OHLCV (if key) + stale check
      2) Synthetic from best available spot (Birdeye spot -> Jupiter -> DexScreener)
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

    if _is_safe_mode():
        await _cache_set(cache_key, [])
        return []

    bars: list[dict[str, Any]] = []
    try:
        async with aiohttp.ClientSession(timeout=_timeout()) as session:
            if BIRDEYE_API_KEY:
                got = await _birdeye_ohlcv_1m(session, mint_s, limit_i)
                if got and not _bars_are_stale(got):
                    bars = got

            if not bars and DATA_SYNTH_ENABLE:
                spot = None
                if BIRDEYE_API_KEY:
                    spot = await _birdeye_spot(session, mint_s)
                if spot is None:
                    spot = await _jup_spot(session, mint_s)
                if spot is None:
                    spot = await _dex_spot(session, mint_s)

                if spot is not None:
                    bars = _synth_bars_from_spot(float(spot), limit_i)

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
        count = len(bars)
        per = max(2, int(period))
        if count < per + 1:
            return None

        trs: list[float] = []
        for idx in range(1, count):
            prev_close = float(bars[idx - 1]["c"])
            high_px = float(bars[idx]["h"])
            low_px = float(bars[idx]["l"])
            tr = max(high_px - low_px, abs(high_px - prev_close), abs(low_px - prev_close))
            trs.append(tr)

        if len(trs) < per:
            return None
        return sum(trs[-per:]) / float(per)
    except Exception:
        return None


def estimate_atr_pct(bars: list[dict[str, Any]], period: int = 14) -> float | None:
    """ATR as fraction of last close (e.g., 0.02 => ~2%)."""
    try:
        atr_val = estimate_atr(bars, period=period)
        if atr_val is None:
            return None
        last_close = float(bars[-1]["c"])
        if last_close <= 0.0:
            return None
        return float(atr_val) / last_close
    except Exception:
        return None


def _log_returns_by_ts(bars: list[dict[str, Any]]) -> dict[int, float]:
    """
    Map ts -> log return from previous close.
    We key by the *current* bar timestamp for alignment.
    """
    out: dict[int, float] = {}
    try:
        for idx in range(1, len(bars)):
            t1 = int(bars[idx]["t"])
            p0 = float(bars[idx - 1]["c"])
            p1 = float(bars[idx]["c"])
            if t1 > 0 and p0 > 0.0 and p1 > 0.0:
                out[t1] = math.log(p1 / p0)
    except Exception:
        return {}
    return out


def _aligned_returns(
    bars_x: list[dict[str, Any]], bars_y: list[dict[str, Any]]
) -> tuple[list[float], list[float]]:
    """
    Align return series by timestamp intersection (more correct than 'last N').
    Deterministic order (sorted timestamps).
    """
    rx_map = _log_returns_by_ts(bars_x)
    ry_map = _log_returns_by_ts(bars_y)
    if not rx_map or not ry_map:
        return [], []

    common = sorted(set(rx_map.keys()) & set(ry_map.keys()))
    if not common:
        return [], []

    rx = [rx_map[t] for t in common]
    ry = [ry_map[t] for t in common]
    return rx, ry


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

        rx, ry = _aligned_returns(bars_x, bars_y)
        m = min(len(rx), len(ry))
        if m < 20:
            return None

        mean_x = sum(rx) / m
        mean_y = sum(ry) / m
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

        rx, ry = _aligned_returns(bars_x, bars_y)
        m = min(len(rx), len(ry))
        if m < 20:
            return None

        mean_x = sum(rx) / m
        mean_y = sum(ry) / m
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
