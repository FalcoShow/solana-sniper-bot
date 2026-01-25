from __future__ import annotations

"""
utils_pkg/price_feed.py

God-Tier Price Feed — V3+++ (production lock-in)
================================================

Purpose
-------
Fast, safe, cached SOL-per-token oracle for a mint, optimized for *realistic exits*.

Key properties
--------------
✅ Conservative pricing: prefer SELL-probe (token -> WSOL) using Jupiter quotes
✅ Safe: never crashes the bot if optional deps are missing or endpoints rate-limit
✅ Cached: in-process TTL cache for price + decimals (monotonic clock)
✅ Anti-stampede: per-mint in-flight locks for decimals + price
✅ Rate-limited: global RPS limiter (default 1 RPS for basic plans)
✅ Router-aware: uses utils_pkg.router.get_sell_quote if present (best internal path)
✅ Proxy-aware: will route requests through utils_pkg.proxy_client.proxy_cfg() when enabled

What "price" means
------------------
We output a SOL-per-1-token(ui) price (float). For robustness we ALSO compute
lamports_per_raw (WSOL lamports per 1 raw token unit) when possible.

This matters because:
- price_sol is great for strategy thresholds / logging
- lamports_per_raw can value *raw balances* without needing decimals (most robust)

Env (safe defaults)
-------------------
PF_ENABLED=1
PF_LOG=0                            # 1 => enable info logs
PF_PRICE_TTL_SEC=5                  # price cache TTL
PF_DECIMALS_TTL_SEC=3600            # decimals cache TTL

# Jupiter quote endpoint (v6)
JUP_QUOTE_URL=https://quote-api.jup.ag/v6/quote

# Global quote budget (default ~1 RPS)
PF_JUP_RPS=1.0
PF_CONCURRENCY=8                    # internal semaphore for HTTP (per process)

# Sell-probe sizing
PF_PROBE_TOKENS_UI=0.001
PF_PROBE_PCT_OF_BALANCE=0.005
PF_MAX_PROBE_RAW=500000000
PF_MIN_PROBE_RAW=1
PF_SLIPPAGE_BPS=50                  # required by API; doesn't "execute" trades

# HTTP + retries/backoff
PF_HTTP_TIMEOUT_TOTAL=10
PF_HTTP_TIMEOUT_CONNECT=4
PF_RETRIES=1
PF_BACKOFF_BASE=0.25
PF_BACKOFF_MAX=3.0

# Optional controls
PF_USE_PROXY=0                      # if 1, uses proxy_cfg() when available
PF_JUP_ONLY_DIRECT_ROUTES=0         # if 1, onlyDirectRoutes=true
PF_JUP_AS_LEGACY_TX=1               # keep stable server behavior
PF_JUP_MAX_ACCOUNTS=0               # 0 => omit (server default)

Compatibility exports
---------------------
- PricePoint dataclass
- get_price_point(...)
- price_in_sol(...) / get_price_ui(...) / price_of_ui(...) / price_of(...)
- value_sol_for_raw(...)            # NEW helper (safe additive API)
- close()                           # graceful aiohttp close

Notes
-----
- This module is designed to be imported anywhere; it does no heavy work at import time.
- For strict global throttling, ensure the process shares this module (singleton import).
"""

import asyncio
import contextlib
import math
import os
import random
import time
from dataclasses import dataclass
from typing import Any

import aiohttp

# -----------------------------------------------------------------------------
# Optional deps (best-effort)
# -----------------------------------------------------------------------------
try:
    # Expected signature (your earlier assumptions):
    #   await fetch_mint_info(mint) -> (decimals, owner) or similar tuple
    from utils_pkg.solana_token import fetch_mint_info  # type: ignore
except Exception:  # pragma: no cover
    fetch_mint_info = None  # type: ignore

try:
    from utils_pkg import router  # type: ignore
except Exception:  # pragma: no cover
    router = None  # type: ignore

try:
    from utils_pkg import lightning_impl as L  # type: ignore
except Exception:  # pragma: no cover
    L = None  # type: ignore


# Proxy config is optional
def _proxy_cfg() -> tuple[str | None, Any, bool]:
    with contextlib.suppress(Exception):
        from utils_pkg.proxy_client import proxy_cfg  # type: ignore

        val = proxy_cfg()
        if isinstance(val, tuple | list) and len(val) >= 3:
            proxy = str(val[0]) if val[0] else None
            return proxy, val[1], bool(val[2])
        if isinstance(val, tuple | list) and len(val) == 2:
            proxy = str(val[0]) if val[0] else None
            return proxy, val[1], False
    return None, None, False


# -----------------------------------------------------------------------------
# Logging (best-effort)
# -----------------------------------------------------------------------------
def _get_logger():
    with contextlib.suppress(Exception):
        from logging_setup import get_logger  # type: ignore

        return get_logger()
    return None


_LOG = _get_logger()


def _log_enabled() -> bool:
    v = os.getenv("PF_LOG", "0")
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on", "t"}


def _ilog(msg: str) -> None:
    if not _log_enabled():
        return
    if _LOG is not None:
        with contextlib.suppress(Exception):
            _LOG.info(msg)  # type: ignore[attr-defined]
            return
    # fallback: quiet print to stderr
    with contextlib.suppress(Exception):
        import sys

        sys.stderr.write(f"[price_feed] {msg}\n")
        sys.stderr.flush()


# -----------------------------------------------------------------------------
# Constants / env
# -----------------------------------------------------------------------------
WSOL = "So11111111111111111111111111111111111111112"

_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _env_bool(k: str, d: bool) -> bool:
    v = os.getenv(k)
    if v is None:
        return d
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return d


def _env_int(k: str, d: int) -> int:
    try:
        return int(float(str(os.getenv(k, str(d))).strip()))
    except Exception:
        return d


def _env_float(k: str, d: float) -> float:
    try:
        return float(str(os.getenv(k, str(d))).strip())
    except Exception:
        return d


PF_ENABLED = _env_bool("PF_ENABLED", True)

TTL_SEC = max(0.25, _env_float("PF_PRICE_TTL_SEC", 5.0))
DEC_TTL_SEC = max(60.0, _env_float("PF_DECIMALS_TTL_SEC", 3600.0))

JUP_QUOTE_URL = (os.getenv("JUP_QUOTE_URL", "https://quote-api.jup.ag/v6/quote") or "").strip()
PF_JUP_RPS = max(0.05, _env_float("PF_JUP_RPS", 1.0))
PF_CONCURRENCY = max(1, min(64, _env_int("PF_CONCURRENCY", 8)))

PF_PROBE_TOKENS_UI = max(1e-9, _env_float("PF_PROBE_TOKENS_UI", 0.001))
PF_PROBE_PCT_OF_BALANCE = max(0.0001, min(0.25, _env_float("PF_PROBE_PCT_OF_BALANCE", 0.005)))
PF_MAX_PROBE_RAW = max(1, _env_int("PF_MAX_PROBE_RAW", 500_000_000))
PF_MIN_PROBE_RAW = max(1, _env_int("PF_MIN_PROBE_RAW", 1))
PF_SLIPPAGE_BPS = max(0, min(5000, _env_int("PF_SLIPPAGE_BPS", 50)))

HTTP_TIMEOUT_TOTAL = max(3, _env_int("PF_HTTP_TIMEOUT_TOTAL", 10))
HTTP_TIMEOUT_CONNECT = max(1, _env_int("PF_HTTP_TIMEOUT_CONNECT", 4))
RETRIES = max(0, min(5, _env_int("PF_RETRIES", 1)))
BACKOFF_BASE = max(0.05, _env_float("PF_BACKOFF_BASE", 0.25))
BACKOFF_MAX = max(BACKOFF_BASE, _env_float("PF_BACKOFF_MAX", 3.0))

PF_USE_PROXY = _env_bool("PF_USE_PROXY", False)
PF_JUP_ONLY_DIRECT_ROUTES = _env_bool("PF_JUP_ONLY_DIRECT_ROUTES", False)
PF_JUP_AS_LEGACY_TX = _env_bool("PF_JUP_AS_LEGACY_TX", True)
PF_JUP_MAX_ACCOUNTS = max(0, _env_int("PF_JUP_MAX_ACCOUNTS", 0))


def _now_mono() -> float:
    return time.monotonic()


def _ts_ms() -> int:
    return int(time.time() * 1000)


def _is_finite_pos(x: float) -> bool:
    return x > 0.0 and math.isfinite(x)


# -----------------------------------------------------------------------------
# Singleton session + concurrency
# -----------------------------------------------------------------------------
_session: aiohttp.ClientSession | None = None
_session_lock = asyncio.Lock()
_http_sem = asyncio.Semaphore(PF_CONCURRENCY)


class _RateLimiter:
    """
    Simple global limiter: ensure at most RPS requests across the whole process.
    """

    def __init__(self, rps: float) -> None:
        self._min_interval = 1.0 / float(rps) if rps > 0 else 0.0
        self._lock = asyncio.Lock()
        self._next_allowed = 0.0  # loop.time()

    async def wait(self) -> None:
        if self._min_interval <= 0:
            return
        loop = asyncio.get_running_loop()
        async with self._lock:
            now = loop.time()
            if now < self._next_allowed:
                await asyncio.sleep(self._next_allowed - now)
                now = loop.time()
            self._next_allowed = now + self._min_interval


_limiter = _RateLimiter(PF_JUP_RPS)


async def _get_session() -> aiohttp.ClientSession:
    global _session
    async with _session_lock:
        if _session is None or _session.closed:
            timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT_TOTAL, connect=HTTP_TIMEOUT_CONNECT)
            connector = aiohttp.TCPConnector(
                limit=50, ttl_dns_cache=300, enable_cleanup_closed=True
            )

            proxy = None
            proxy_auth = None
            insecure_ssl = False
            if PF_USE_PROXY:
                proxy, proxy_auth, insecure_ssl = _proxy_cfg()
                if proxy:
                    _ilog(f"proxy_enabled=1 proxy={proxy} insecure_ssl={int(insecure_ssl)}")
                else:
                    _ilog("proxy_enabled=0 (PF_USE_PROXY=1 but proxy_cfg missing/empty)")

            # If proxy requests insecure SSL, disable SSL verification at connector level
            if insecure_ssl:
                connector = aiohttp.TCPConnector(
                    limit=50, ssl=False, ttl_dns_cache=300, enable_cleanup_closed=True
                )

            _session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers={"user-agent": "pnlsniper/1.0 (+pricefeed)"},
            )
        return _session


# -----------------------------------------------------------------------------
# Caches + locks
# -----------------------------------------------------------------------------
# price cache: key -> (price_sol, lamports_per_raw, expires_mono)
_PRICE_CACHE: dict[str, tuple[float, float, float]] = {}
_PRICE_LOCKS: dict[str, asyncio.Lock] = {}

# decimals cache: mint -> (decimals, expires_mono)
_DEC_CACHE: dict[str, tuple[int, float]] = {}
_DEC_LOCKS: dict[str, asyncio.Lock] = {}

# basic stats for introspection
_STATS: dict[str, int] = {
    "cache_hit": 0,
    "cache_miss": 0,
    "dec_cache_hit": 0,
    "dec_cache_miss": 0,
    "router_ok": 0,
    "router_fail": 0,
    "jup_ok": 0,
    "jup_fail": 0,
    "lightning_ok": 0,
    "lightning_fail": 0,
}


def _bump(k: str, n: int = 1) -> None:
    _STATS[k] = int(_STATS.get(k, 0)) + int(n)


def get_stats() -> dict[str, int]:
    """Non-sensitive counters. Safe to call anywhere."""
    return dict(_STATS)


def _price_lock(key: str) -> asyncio.Lock:
    lk = _PRICE_LOCKS.get(key)
    if lk is None:
        lk = asyncio.Lock()
        _PRICE_LOCKS[key] = lk
    return lk


def _dec_lock(mint: str) -> asyncio.Lock:
    lk = _DEC_LOCKS.get(mint)
    if lk is None:
        lk = asyncio.Lock()
        _DEC_LOCKS[mint] = lk
    return lk


def _cache_get_price(key: str) -> tuple[float, float] | None:
    rec = _PRICE_CACHE.get(key)
    if not rec:
        return None
    p_sol, lpr, exp = rec
    if _now_mono() < exp:
        return (p_sol, lpr)
    _PRICE_CACHE.pop(key, None)
    return None


def _cache_put_price(key: str, price_sol: float, lamports_per_raw: float) -> None:
    _PRICE_CACHE[key] = (float(price_sol), float(lamports_per_raw), _now_mono() + TTL_SEC)


async def _decimals_async(mint: str) -> int | None:
    rec = _DEC_CACHE.get(mint)
    if rec and _now_mono() < rec[1]:
        _bump("dec_cache_hit")
        return rec[0]

    _bump("dec_cache_miss")
    if fetch_mint_info is None:
        return None

    async with _dec_lock(mint):
        rec2 = _DEC_CACHE.get(mint)
        if rec2 and _now_mono() < rec2[1]:
            _bump("dec_cache_hit")
            return rec2[0]

        try:
            info = await fetch_mint_info(mint)  # expected: (decimals, owner) or similar
            if not info:
                return None
            dec = int(info[0])
            if 0 <= dec <= 18:
                _DEC_CACHE[mint] = (dec, _now_mono() + DEC_TTL_SEC)
                return dec
        except Exception:
            return None

    return None


# -----------------------------------------------------------------------------
# Quote parsing
# -----------------------------------------------------------------------------
def _int_or0(x: Any) -> int:
    try:
        return int(x)
    except Exception:
        return 0


def _parse_out_amount_lamports(data: dict | None) -> int:
    if not data or not isinstance(data, dict):
        return 0
    # Jupiter /quote returns outAmount at top-level
    if "outAmount" in data:
        return _int_or0(data.get("outAmount", 0))
    # occasionally nested formats
    if "data" in data and isinstance(data["data"], dict):
        return _int_or0(data["data"].get("outAmount", 0))
    return 0


# -----------------------------------------------------------------------------
# HTTP + Jupiter quote
# -----------------------------------------------------------------------------
async def _http_json(url: str, params: dict[str, Any]) -> dict | None:
    sess = await _get_session()

    # If proxy is enabled, we fetch it each call (cheap) to allow runtime changes.
    proxy = None
    proxy_auth = None
    insecure_ssl = False
    if PF_USE_PROXY:
        proxy, proxy_auth, insecure_ssl = _proxy_cfg()

    for attempt in range(RETRIES + 1):
        try:
            await _limiter.wait()

            async with _http_sem:
                async with sess.get(url, params=params, proxy=proxy, proxy_auth=proxy_auth) as r:
                    status = int(r.status)
                    # Read text first for debugging; json() below still works
                    txt = await r.text()

                    # Retry on 429/5xx
                    if status == 429 or 500 <= status < 600:
                        if attempt < RETRIES:
                            backoff = min(BACKOFF_MAX, BACKOFF_BASE * (2**attempt))
                            await asyncio.sleep(backoff + random.uniform(0.0, 0.25))
                            continue
                        return None

                    if status != 200:
                        if attempt < RETRIES:
                            await asyncio.sleep(BACKOFF_BASE + random.uniform(0.0, 0.15))
                            continue
                        return None

                    with contextlib.suppress(Exception):
                        return await r.json(content_type=None)

                    # bad JSON
                    if attempt < RETRIES:
                        await asyncio.sleep(BACKOFF_BASE + random.uniform(0.0, 0.15))
                        continue
                    _ = txt  # keep for possible future debug
                    return None

        except Exception:
            if attempt < RETRIES:
                backoff = min(BACKOFF_MAX, BACKOFF_BASE * (2**attempt))
                await asyncio.sleep(backoff + random.uniform(0.0, 0.25))
                continue
            return None

    return None


async def _jup_quote(input_mint: str, output_mint: str, amount_raw: int) -> dict | None:
    if amount_raw <= 0:
        return None

    params: dict[str, Any] = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": str(int(amount_raw)),
        "slippageBps": str(int(PF_SLIPPAGE_BPS)),
        "swapMode": "ExactIn",
        "onlyDirectRoutes": "true" if PF_JUP_ONLY_DIRECT_ROUTES else "false",
        "asLegacyTransaction": "true" if PF_JUP_AS_LEGACY_TX else "false",
    }
    if PF_JUP_MAX_ACCOUNTS > 0:
        params["maxAccounts"] = str(int(PF_JUP_MAX_ACCOUNTS))

    return await _http_json(JUP_QUOTE_URL, params)


# -----------------------------------------------------------------------------
# Price model
# -----------------------------------------------------------------------------
@dataclass
class PricePoint:
    mint: str
    ts_ms: int
    price_sol: float
    lamports_per_raw: float
    source: str
    meta: dict[str, Any]


def _price_from_lpr(lamports_per_raw: float, decimals: int) -> float:
    # lamports_per_raw is WSOL lamports per 1 raw token unit
    # 1 token(ui) = 10^dec raw units
    # SOL per token(ui) = (lamports_per_raw * 10^dec) / 1e9
    return (float(lamports_per_raw) * float(10 ** int(decimals))) / 1_000_000_000.0


def _choose_probe_amount_raw(
    *,
    decimals: int | None,
    probe_tokens_ui: float,
    balance_raw_hint: int,
) -> tuple[int, float]:
    """
    Returns (amount_raw, effective_probe_ui).

    If balance_raw_hint is provided, we probe a small fraction (PF_PROBE_PCT_OF_BALANCE)
    bounded by min/max.
    """
    pui = max(1e-9, float(probe_tokens_ui))

    if balance_raw_hint and balance_raw_hint > 0:
        # probe a fraction of balance (bounded)
        frac_raw = int(
            max(
                PF_MIN_PROBE_RAW,
                min(balance_raw_hint, int(balance_raw_hint * PF_PROBE_PCT_OF_BALANCE)),
            )
        )
        frac_raw = int(min(frac_raw, PF_MAX_PROBE_RAW))
        if decimals is not None and 0 <= decimals <= 18:
            pui = max(pui, frac_raw / float(10 ** int(decimals)))
        return int(frac_raw), float(pui)

    if decimals is None or decimals < 0:
        # can't scale ui->raw; pick a conservative raw size
        amt = int(max(PF_MIN_PROBE_RAW, min(PF_MAX_PROBE_RAW, 10_000)))
        return amt, float(pui)

    amt2 = int(max(PF_MIN_PROBE_RAW, round(pui * (10 ** int(decimals)))))
    amt2 = int(min(amt2, PF_MAX_PROBE_RAW))
    return int(amt2), float(pui)


# -----------------------------------------------------------------------------
# Optional fast paths
# -----------------------------------------------------------------------------
async def _try_lightning(mint: str, decimals: int | None) -> PricePoint | None:
    if L is None:
        _bump("lightning_fail")
        return None

    for nm in ("get_token_price_sol", "get_price_sol", "price_sol"):
        fn = getattr(L, nm, None)
        if callable(fn):
            with contextlib.suppress(Exception):
                px = await fn(mint)
                px_f = float(px)
                if _is_finite_pos(px_f):
                    _bump("lightning_ok")
                    lpr = 0.0
                    if decimals is not None and 0 <= decimals <= 18:
                        lpr = (px_f * 1_000_000_000.0) / float(10 ** int(decimals))
                    return PricePoint(
                        mint=mint,
                        ts_ms=_ts_ms(),
                        price_sol=px_f,
                        lamports_per_raw=float(lpr),
                        source=f"lightning:{nm}",
                        meta={},
                    )
    _bump("lightning_fail")
    return None


async def _try_router_sell_quote(
    mint: str,
    amount_raw: int,
    decimals: int | None,
) -> PricePoint | None:
    if router is None:
        _bump("router_fail")
        return None
    fn = getattr(router, "get_sell_quote", None)
    if not callable(fn):
        _bump("router_fail")
        return None

    try:
        # Prefer keyword signature (your modern router)
        q = await fn(mint=mint, amount_raw=int(amount_raw), slippage_bps=int(PF_SLIPPAGE_BPS))
    except TypeError:
        # Legacy/mismatch: try positional
        try:
            q = await fn(mint, int(amount_raw))
        except Exception:
            _bump("router_fail")
            return None
    except Exception:
        _bump("router_fail")
        return None

    lamports = 0
    if isinstance(q, dict):
        lamports = _parse_out_amount_lamports(q)
        if lamports <= 0:
            # sometimes nested
            lamports = _int_or0(q.get("outAmount") or 0)

    if lamports <= 0:
        _bump("router_fail")
        return None

    lpr = lamports / float(max(1, amount_raw))
    px = 0.0
    if decimals is not None and 0 <= decimals <= 18:
        px = _price_from_lpr(lpr, decimals)

    _bump("router_ok")
    return PricePoint(
        mint=mint,
        ts_ms=_ts_ms(),
        price_sol=float(px),
        lamports_per_raw=float(lpr),
        source="router:get_sell_quote",
        meta={"amount_raw": int(amount_raw), "out_lamports": int(lamports)},
    )


# -----------------------------------------------------------------------------
# Jupiter price paths
# -----------------------------------------------------------------------------
async def _jup_sell_probe(
    mint: str,
    *,
    amount_raw: int,
    decimals: int | None,
    effective_probe_ui: float,
) -> PricePoint | None:
    data = await _jup_quote(mint, WSOL, int(amount_raw))
    lamports = _parse_out_amount_lamports(data)
    if lamports <= 0:
        _bump("jup_fail")
        return None

    lpr = lamports / float(max(1, amount_raw))
    px = 0.0
    if decimals is not None and 0 <= decimals <= 18 and effective_probe_ui > 0:
        # direct from effective ui probe is slightly more realistic under rounding
        sol_out = lamports / 1_000_000_000.0
        px = sol_out / float(effective_probe_ui)

    # if px not computed but decimals exist, compute from lpr
    if (not _is_finite_pos(px)) and decimals is not None and 0 <= decimals <= 18:
        px = _price_from_lpr(lpr, decimals)

    if decimals is not None and 0 <= decimals <= 18 and not _is_finite_pos(px):
        _bump("jup_fail")
        return None

    # If decimals unknown, we still return a usable lpr (robust valuation)
    _bump("jup_ok")
    return PricePoint(
        mint=mint,
        ts_ms=_ts_ms(),
        price_sol=float(px) if _is_finite_pos(px) else 0.0,
        lamports_per_raw=float(lpr),
        source="jup:sell_probe",
        meta={
            "amount_raw": int(amount_raw),
            "out_lamports": int(lamports),
            "probe_ui": float(effective_probe_ui),
        },
    )


async def _jup_buy_invert(mint: str) -> PricePoint | None:
    # Buy with exactly 1 SOL (1e9 lamports)
    data = await _jup_quote(WSOL, mint, 1_000_000_000)
    if not data:
        _bump("jup_fail")
        return None

    out_raw = _int_or0(data.get("outAmount", 0))
    if out_raw <= 0:
        _bump("jup_fail")
        return None

    dec = await _decimals_async(mint)
    if dec is None:
        _bump("jup_fail")
        return None

    tokens_ui = out_raw / float(10 ** int(dec))
    if tokens_ui <= 0:
        _bump("jup_fail")
        return None

    px = 1.0 / tokens_ui
    if not _is_finite_pos(px):
        _bump("jup_fail")
        return None

    lpr = 1_000_000_000.0 / float(out_raw)
    _bump("jup_ok")
    return PricePoint(
        mint=mint,
        ts_ms=_ts_ms(),
        price_sol=float(px),
        lamports_per_raw=float(lpr),
        source="jup:buy_invert",
        meta={"out_raw": int(out_raw), "tokens_ui": float(tokens_ui)},
    )


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------
async def get_price_point(
    mint: str,
    *,
    decimals_hint: int | None = None,
    balance_raw_hint: int = 0,
    probe_tokens_ui: float | None = None,
) -> PricePoint:
    """
    Return a PricePoint. On failure returns price_sol=0 and lamports_per_raw=0.

    decimals_hint:
      - if you already know decimals, pass it to skip RPC lookups

    balance_raw_hint:
      - if you pass your position raw balance, we will probe a small % of it
        (PF_PROBE_PCT_OF_BALANCE) for realism (bounded by min/max probe)

    probe_tokens_ui:
      - explicit override for the UI probe size used for pricing
    """
    mint = (mint or "").strip()
    if not mint or not PF_ENABLED:
        return PricePoint(
            mint=mint,
            ts_ms=_ts_ms(),
            price_sol=0.0,
            lamports_per_raw=0.0,
            source="disabled",
            meta={},
        )

    # trivial: WSOL/SOL mint is 1 SOL per token
    if mint == WSOL:
        return PricePoint(
            mint=mint,
            ts_ms=_ts_ms(),
            price_sol=1.0,
            lamports_per_raw=1_000_000_000.0,
            source="wsol",
            meta={},
        )

    # sanitize + derive probe
    probe_ui = float(probe_tokens_ui) if probe_tokens_ui is not None else float(PF_PROBE_TOKENS_UI)
    probe_ui = max(1e-9, probe_ui)

    decimals: int | None = None
    if decimals_hint is not None:
        with contextlib.suppress(Exception):
            d = int(decimals_hint)
            if 0 <= d <= 18:
                decimals = d

    # Cache key: mint + decimals bucket + probe bucket + (balance hint present)
    # balance changes route impact slightly; we bucket it as bool for key stability
    dec_key = int(decimals) if decimals is not None else -1
    p_bucket = round(probe_ui, 10)
    b_flag = 1 if (balance_raw_hint and balance_raw_hint > 0) else 0
    key = f"{mint}:{dec_key}:{p_bucket}:{b_flag}"

    cached = _cache_get_price(key)
    if cached is not None:
        _bump("cache_hit")
        p_sol, lpr = cached
        return PricePoint(
            mint=mint,
            ts_ms=_ts_ms(),
            price_sol=p_sol,
            lamports_per_raw=lpr,
            source="cache",
            meta={"key": key},
        )
    _bump("cache_miss")

    # Coalesce concurrent requests for same key (anti-stampede)
    async with _price_lock(key):
        cached2 = _cache_get_price(key)
        if cached2 is not None:
            _bump("cache_hit")
            p_sol, lpr = cached2
            return PricePoint(
                mint=mint,
                ts_ms=_ts_ms(),
                price_sol=p_sol,
                lamports_per_raw=lpr,
                source="cache",
                meta={"key": key},
            )

        # Resolve decimals (if needed)
        if decimals is None:
            decimals = await _decimals_async(mint)

        # lightning fast path (if available)
        ppL = await _try_lightning(mint, decimals)
        if ppL and _is_finite_pos(ppL.price_sol):
            _cache_put_price(key, ppL.price_sol, ppL.lamports_per_raw or 0.0)
            return ppL

        amount_raw, effective_probe_ui = _choose_probe_amount_raw(
            decimals=decimals,
            probe_tokens_ui=probe_ui,
            balance_raw_hint=int(balance_raw_hint or 0),
        )

        # Router sell-quote (preferred internal path) — gives realistic outAmount
        # If decimals unknown, router can still provide lamports_per_raw which is very useful
        ppR = await _try_router_sell_quote(mint, amount_raw=int(amount_raw), decimals=decimals)
        if ppR and (_is_finite_pos(ppR.lamports_per_raw) or _is_finite_pos(ppR.price_sol)):
            # if router returned lpr but px=0 and decimals known, compute px
            if (
                (not _is_finite_pos(ppR.price_sol))
                and decimals is not None
                and 0 <= decimals <= 18
                and _is_finite_pos(ppR.lamports_per_raw)
            ):
                ppR.price_sol = _price_from_lpr(ppR.lamports_per_raw, decimals)

            # Cache even if price_sol=0 (decimals unknown), because lpr can be used
            _cache_put_price(key, float(ppR.price_sol), float(ppR.lamports_per_raw))
            return ppR

        # Jupiter sell-probe (preferred external)
        ppS = await _jup_sell_probe(
            mint,
            amount_raw=int(amount_raw),
            decimals=decimals,
            effective_probe_ui=float(effective_probe_ui),
        )
        if ppS and (_is_finite_pos(ppS.lamports_per_raw) or _is_finite_pos(ppS.price_sol)):
            if (
                (not _is_finite_pos(ppS.price_sol))
                and decimals is not None
                and 0 <= decimals <= 18
                and _is_finite_pos(ppS.lamports_per_raw)
            ):
                ppS.price_sol = _price_from_lpr(ppS.lamports_per_raw, decimals)
            _cache_put_price(key, float(ppS.price_sol), float(ppS.lamports_per_raw))
            return ppS

        # Jupiter buy invert (fallback; needs decimals)
        ppB = await _jup_buy_invert(mint)
        if ppB and _is_finite_pos(ppB.price_sol):
            _cache_put_price(key, ppB.price_sol, ppB.lamports_per_raw)
            return ppB

        # total failure
        return PricePoint(
            mint=mint,
            ts_ms=_ts_ms(),
            price_sol=0.0,
            lamports_per_raw=0.0,
            source="unavailable",
            meta={"key": key},
        )


async def price_in_sol(
    mint: str, decimals_hint: int | None = None, probe_tokens: float = PF_PROBE_TOKENS_UI
) -> float:
    pp = await get_price_point(
        mint, decimals_hint=decimals_hint, probe_tokens_ui=float(probe_tokens)
    )
    return float(pp.price_sol)


# Compatibility aliases expected elsewhere
async def get_price_ui(mint: str) -> float:
    return await price_in_sol(mint)


async def price_of_ui(mint: str) -> float:
    return await price_in_sol(mint)


async def price_of(mint: str) -> float:
    return await price_in_sol(mint)


async def value_sol_for_raw(
    mint: str,
    amount_raw: int,
    *,
    decimals_hint: int | None = None,
    balance_raw_hint: int = 0,
) -> float:
    """
    Robust valuation in SOL for a raw token amount.

    This works even if decimals are unknown *as long as* we can obtain lamports_per_raw
    from sell-probe quotes. (Decimals only affect the UI token price, not the raw valuation.)
    """
    amt = int(amount_raw or 0)
    if amt <= 0:
        return 0.0
    pp = await get_price_point(mint, decimals_hint=decimals_hint, balance_raw_hint=balance_raw_hint)
    lpr = float(pp.lamports_per_raw or 0.0)
    if lpr <= 0:
        return 0.0
    # lamports_per_raw * raw_amount = out_lamports estimate
    out_lamports = lpr * float(amt)
    return float(out_lamports / 1_000_000_000.0)


async def close() -> None:
    """Graceful aiohttp close (call on shutdown)."""
    global _session
    if _session and not _session.closed:
        await _session.close()
    _session = None


__all__ = [
    "PricePoint",
    "close",
    "get_price_point",
    "get_price_ui",
    "get_stats",
    "price_in_sol",
    "price_of",
    "price_of_ui",
    "value_sol_for_raw",
]
