from __future__ import annotations

"""
utils_pkg/price_feed.py

God-Tier Price Feed — V3+++ (production lock-in)
================================================

Purpose
-------
Fast, safe, cached SOL-per-token oracle for a mint, optimized for realistic exits.

Core properties
---------------
✅ Conservative pricing: prefer SELL-probe (token -> WSOL) using router/Jupiter
✅ Safe: never crashes the bot if optional deps are missing or endpoints fail
✅ Cached: in-process TTL cache for price + decimals (monotonic clock)
✅ Anti-stampede: per-key in-flight locks for price + decimals
✅ Rate-limited: global RPS limiter (default ~1 RPS)
✅ Router-aware: uses utils_pkg.router.get_sell_quote if present (best internal path)
✅ Proxy-aware: routes HTTP through utils_pkg.proxy_client.proxy_cfg() when enabled
✅ Optional fast path: lightning_impl (if available)
✅ Test/SAFE_MODE friendly: can hard-disable network paths via env

Outputs
-------
- price_sol: SOL-per-1-token(ui) (float). Can be 0 if decimals unknown/unavailable.
- lamports_per_raw: WSOL lamports per 1 raw token unit (float). Often available even
  when decimals are unknown; this enables robust valuation of raw balances.

Env (safe defaults)
-------------------
PF_ENABLED=1
PF_LOG=0
PF_PRICE_TTL_SEC=5
PF_DECIMALS_TTL_SEC=3600

JUP_QUOTE_URL=https://quote-api.jup.ag/v6/quote
PF_JUP_RPS=1.0
PF_CONCURRENCY=8

PF_PROBE_TOKENS_UI=0.001
PF_PROBE_PCT_OF_BALANCE=0.005
PF_MAX_PROBE_RAW=500000000
PF_MIN_PROBE_RAW=1
PF_SLIPPAGE_BPS=50

PF_HTTP_TIMEOUT_TOTAL=10
PF_HTTP_TIMEOUT_CONNECT=4
PF_RETRIES=1
PF_BACKOFF_BASE=0.25
PF_BACKOFF_MAX=3.0

PF_USE_PROXY=0
PF_JUP_ONLY_DIRECT_ROUTES=0
PF_JUP_AS_LEGACY_TX=1
PF_JUP_MAX_ACCOUNTS=0

Compatibility exports
---------------------
- PricePoint dataclass
- get_price_point(...)
- price_in_sol(...) / get_price_ui(...) / price_of_ui(...) / price_of(...)
- value_sol_for_raw(...)
- close()
"""

import asyncio
import contextlib
import math
import os
import random
import time
from dataclasses import dataclass
from typing import Any

# -----------------------------------------------------------------------------
# Optional deps (best-effort; never required at import time)
# NOTE: We type these as Optional[Any] to avoid mypy "unreachable" when modules
# exist during type-checking but may be absent at runtime.
# -----------------------------------------------------------------------------
_fetch_mint_info: Any | None = None
_router_mod: Any | None = None
_lightning_mod: Any | None = None
_proxy_cfg_fn: Any | None = None

with contextlib.suppress(Exception):
    from utils_pkg.solana_token import fetch_mint_info as _fmi  # type: ignore

    _fetch_mint_info = _fmi

with contextlib.suppress(Exception):
    from utils_pkg import router as _rt  # type: ignore

    _router_mod = _rt

with contextlib.suppress(Exception):
    from utils_pkg import lightning_impl as _li  # type: ignore

    _lightning_mod = _li

with contextlib.suppress(Exception):
    from utils_pkg.proxy_client import proxy_cfg as _pcfg  # type: ignore

    _proxy_cfg_fn = _pcfg


# -----------------------------------------------------------------------------
# aiohttp (optional)
# -----------------------------------------------------------------------------
try:
    import aiohttp
except Exception:  # pragma: no cover
    aiohttp = None  # type: ignore[assignment]


# -----------------------------------------------------------------------------
# Logging (best-effort; prefer canonical log_compat)
# -----------------------------------------------------------------------------
class _LogShim:
    __slots__ = ("_supplier",)

    def __init__(self, supplier: Any) -> None:
        self._supplier = supplier

    def _log(self) -> Any:
        with contextlib.suppress(Exception):
            return self._supplier()
        return None

    def event(self, name: str, **fields: Any) -> None:
        lg = self._log()
        if lg is None:
            return
        fn = getattr(lg, "event", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                fn(name, **fields)
            return
        info = getattr(lg, "info", None)
        if callable(info):
            with contextlib.suppress(Exception):
                info("%s %s", name, fields)

    def info(self, msg: str, *a: Any) -> None:
        lg = self._log()
        if lg is None:
            return
        fn = getattr(lg, "info", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                fn(msg, *a)

    def warning(self, msg: str, *a: Any) -> None:
        lg = self._log()
        if lg is None:
            return
        fn = getattr(lg, "warning", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                fn(msg, *a)

    def error(self, msg: str, *a: Any) -> None:
        lg = self._log()
        if lg is None:
            return
        fn = getattr(lg, "error", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                fn(msg, *a)


def _resolve_logger() -> Any:
    with contextlib.suppress(Exception):
        from utils_pkg.log_compat import get_logger as gl  # type: ignore

        return gl("utils.price_feed")
    import logging

    lg = logging.getLogger("utils.price_feed")
    if not lg.handlers:
        logging.basicConfig(level=logging.INFO)
    return lg


log = _LogShim(_resolve_logger)


# -----------------------------------------------------------------------------
# Env helpers
# -----------------------------------------------------------------------------
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
    v = os.getenv(k)
    if v is None:
        return d
    s = str(v).strip()
    if not s:
        return d
    with contextlib.suppress(Exception):
        return int(float(s))
    return d


def _env_float(k: str, d: float) -> float:
    v = os.getenv(k)
    if v is None:
        return d
    s = str(v).strip()
    if not s:
        return d
    with contextlib.suppress(Exception):
        return float(s)
    return d


def _now_mono() -> float:
    return time.monotonic()


def _ts_ms() -> int:
    return int(time.time() * 1000)


def _is_finite_pos(x: float) -> bool:
    return x > 0.0 and math.isfinite(x)


def _network_allowed() -> bool:
    """
    Honor repo-wide safety rails commonly used in tests/contract runs.
    """
    if (
        _env_bool("SAFE_MODE", False)
        or _env_bool("NO_NETWORK", False)
        or _env_bool("DISABLE_NETWORK", False)
    ):
        return False
    if os.getenv("NETWORK", "").strip().lower() in _FALSE:
        return False
    if os.getenv("PYTEST_CURRENT_TEST"):
        # Tests should not hit the network unless explicitly enabled.
        return _env_bool("PF_ALLOW_NETWORK_IN_TESTS", False)
    return True


def _ilog(msg: str, **extra: Any) -> None:
    if not _env_bool("PF_LOG", False):
        return
    with contextlib.suppress(Exception):
        log.event("PRICE_FEED", msg=msg, **extra)


# -----------------------------------------------------------------------------
# Constants / env
# -----------------------------------------------------------------------------
WSOL = "So11111111111111111111111111111111111111112"

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


def _proxy_cfg() -> tuple[str | None, Any, bool]:
    """
    Best-effort proxy configuration:
      returns (proxy_url | None, proxy_auth, insecure_ssl)
    """
    if not PF_USE_PROXY or _proxy_cfg_fn is None:
        return None, None, False
    try:
        val = _proxy_cfg_fn()
        if isinstance(val, (tuple, list)) and len(val) >= 3:
            proxy = str(val[0]) if val[0] else None
            return proxy, val[1], bool(val[2])
        if isinstance(val, (tuple, list)) and len(val) == 2:
            proxy = str(val[0]) if val[0] else None
            return proxy, val[1], False
    except Exception:
        return None, None, False
    return None, None, False


# -----------------------------------------------------------------------------
# Singleton session + concurrency
# -----------------------------------------------------------------------------
_session: Any | None = None
_session_lock = asyncio.Lock()
_http_sem = asyncio.Semaphore(PF_CONCURRENCY)


class _RateLimiter:
    """
    Simple global limiter: ensure at most RPS requests across the process.
    """

    def __init__(self, rps: float) -> None:
        self._min_interval = 1.0 / float(rps) if rps > 0 else 0.0
        self._lock = asyncio.Lock()
        self._next_allowed = 0.0

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


async def _get_session() -> Any:
    """
    Create an aiohttp ClientSession lazily and reuse it.
    """
    global _session
    if aiohttp is None:
        raise RuntimeError("aiohttp not available")

    async with _session_lock:
        if _session is None or getattr(_session, "closed", False):
            timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT_TOTAL, connect=HTTP_TIMEOUT_CONNECT)

            connector = aiohttp.TCPConnector(
                limit=50, ttl_dns_cache=300, enable_cleanup_closed=True
            )

            if PF_USE_PROXY:
                proxy, _auth, insecure_ssl = _proxy_cfg()
                _ilog("proxy_check", proxy=proxy, insecure_ssl=int(insecure_ssl))
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
_PRICE_CACHE: dict[str, tuple[float, float, float]] = {}
_PRICE_LOCKS: dict[str, asyncio.Lock] = {}

_DEC_CACHE: dict[str, tuple[int, float]] = {}
_DEC_LOCKS: dict[str, asyncio.Lock] = {}

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
    "no_network": 0,
    "disabled": 0,
}


def _bump(k: str, n: int = 1) -> None:
    _STATS[k] = int(_STATS.get(k, 0)) + int(n)


def get_stats() -> dict[str, int]:
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
        return p_sol, lpr
    _PRICE_CACHE.pop(key, None)
    return None


def _cache_put_price(key: str, price_sol: float, lamports_per_raw: float) -> None:
    _PRICE_CACHE[key] = (float(price_sol), float(lamports_per_raw), _now_mono() + TTL_SEC)


# -----------------------------------------------------------------------------
# Decimals resolution (best-effort)
# -----------------------------------------------------------------------------
def _extract_decimals(info: Any) -> int | None:
    if isinstance(info, (tuple, list)) and info:
        with contextlib.suppress(Exception):
            return int(info[0])
    if isinstance(info, dict):
        with contextlib.suppress(Exception):
            v = info.get("decimals")
            if v is not None:
                return int(v)
    with contextlib.suppress(Exception):
        v = getattr(info, "decimals", None)
        if v is not None:
            return int(v)
    return None


async def _decimals_async(mint: str) -> int | None:
    rec = _DEC_CACHE.get(mint)
    if rec and _now_mono() < rec[1]:
        _bump("dec_cache_hit")
        return rec[0]

    _bump("dec_cache_miss")
    if _fetch_mint_info is None:
        return None

    async with _dec_lock(mint):
        rec2 = _DEC_CACHE.get(mint)
        if rec2 and _now_mono() < rec2[1]:
            _bump("dec_cache_hit")
            return rec2[0]

        try:
            info = await _fetch_mint_info(mint)
            dec = _extract_decimals(info)
            if dec is None:
                return None
            if 0 <= dec <= 18:
                _DEC_CACHE[mint] = (dec, _now_mono() + DEC_TTL_SEC)
                return dec
        except Exception:
            return None

    return None


# -----------------------------------------------------------------------------
# Quote parsing + helpers
# -----------------------------------------------------------------------------
def _int_or0(x: Any) -> int:
    with contextlib.suppress(Exception):
        return int(x)
    return 0


def _parse_out_amount_lamports(payload: Any) -> int:
    if isinstance(payload, dict):
        if "outAmount" in payload:
            return _int_or0(payload.get("outAmount", 0))
        if "data" in payload and isinstance(payload["data"], dict):
            return _int_or0(payload["data"].get("outAmount", 0))
    return 0


def _price_from_lpr(lamports_per_raw: float, decimals: int) -> float:
    return (float(lamports_per_raw) * float(10 ** int(decimals))) / 1_000_000_000.0


def _choose_probe_amount_raw(
    *,
    decimals: int | None,
    probe_tokens_ui: float,
    balance_raw_hint: int,
) -> tuple[int, float]:
    pui = max(1e-9, float(probe_tokens_ui))

    if balance_raw_hint and balance_raw_hint > 0:
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
        amt = int(max(PF_MIN_PROBE_RAW, min(PF_MAX_PROBE_RAW, 10_000)))
        return amt, float(pui)

    amt2 = int(max(PF_MIN_PROBE_RAW, round(pui * (10 ** int(decimals)))))
    amt2 = int(min(amt2, PF_MAX_PROBE_RAW))
    return int(amt2), float(pui)


# -----------------------------------------------------------------------------
# HTTP + Jupiter quote
# -----------------------------------------------------------------------------
async def _http_json(url: str, params: dict[str, Any]) -> dict[str, Any] | None:
    if not _network_allowed():
        return None
    if aiohttp is None:
        return None

    sess = await _get_session()
    proxy = None
    proxy_auth = None
    if PF_USE_PROXY:
        proxy, proxy_auth, _insecure_ssl = _proxy_cfg()

    for attempt in range(RETRIES + 1):
        try:
            await _limiter.wait()
            async with _http_sem:
                async with sess.get(url, params=params, proxy=proxy, proxy_auth=proxy_auth) as r:
                    status = int(r.status)

                    # retry on 429/5xx
                    if status == 429 or 500 <= status < 600:
                        if attempt < RETRIES:
                            backoff = min(BACKOFF_MAX, BACKOFF_BASE * (2**attempt))
                            await asyncio.sleep(backoff + random.uniform(0.0, 0.25))
                            continue
                        break

                    if status != 200:
                        if attempt < RETRIES:
                            await asyncio.sleep(BACKOFF_BASE + random.uniform(0.0, 0.15))
                            continue
                        break

                    data: Any = None
                    with contextlib.suppress(Exception):
                        data = await r.json(content_type=None)

                    if isinstance(data, dict):
                        return data

                    if attempt < RETRIES:
                        await asyncio.sleep(BACKOFF_BASE + random.uniform(0.0, 0.15))
                        continue
                    break

        except Exception:
            if attempt < RETRIES:
                backoff = min(BACKOFF_MAX, BACKOFF_BASE * (2**attempt))
                await asyncio.sleep(backoff + random.uniform(0.0, 0.25))
                continue
            break

    return None


async def _jup_quote(input_mint: str, output_mint: str, amount_raw: int) -> dict[str, Any] | None:
    if amount_raw <= 0 or not JUP_QUOTE_URL:
        return None
    if not _network_allowed():
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


# -----------------------------------------------------------------------------
# Optional fast paths
# -----------------------------------------------------------------------------
async def _try_lightning(mint: str, decimals: int | None) -> PricePoint | None:
    if _lightning_mod is None:
        _bump("lightning_fail")
        return None

    for nm in ("get_token_price_sol", "get_price_sol", "price_sol"):
        fn = getattr(_lightning_mod, nm, None)
        if not callable(fn):
            continue
        try:
            px = await fn(mint)
            px_f = float(px)
            if not _is_finite_pos(px_f):
                continue

            lpr = 0.0
            if decimals is not None and 0 <= decimals <= 18:
                lpr = (px_f * 1_000_000_000.0) / float(10 ** int(decimals))

            _bump("lightning_ok")
            return PricePoint(
                mint=mint,
                ts_ms=_ts_ms(),
                price_sol=px_f,
                lamports_per_raw=float(lpr),
                source=f"lightning:{nm}",
                meta={},
            )
        except Exception:
            continue

    _bump("lightning_fail")
    return None


async def _try_router_sell_quote(
    mint: str, amount_raw: int, decimals: int | None
) -> PricePoint | None:
    if _router_mod is None:
        _bump("router_fail")
        return None

    fn = getattr(_router_mod, "get_sell_quote", None)
    if not callable(fn):
        _bump("router_fail")
        return None

    q: Any = None
    attempts = [
        {"mint": mint, "amount_raw": int(amount_raw), "slippage_bps": int(PF_SLIPPAGE_BPS)},
        {"mint": mint, "amount_raw": int(amount_raw)},
        {"mint": mint, "amount": int(amount_raw), "slippage_bps": int(PF_SLIPPAGE_BPS)},
    ]
    for kw in attempts:
        try:
            q = await fn(**kw)
            break
        except TypeError:
            continue
        except Exception:
            _bump("router_fail")
            return None

    if q is None:
        try:
            q = await fn(mint, int(amount_raw))
        except Exception:
            _bump("router_fail")
            return None

    lamports = _parse_out_amount_lamports(q)
    if lamports <= 0 and isinstance(q, dict):
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
# Jupiter paths
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
        sol_out = lamports / 1_000_000_000.0
        px = sol_out / float(effective_probe_ui)

    if (not _is_finite_pos(px)) and decimals is not None and 0 <= decimals <= 18:
        px = _price_from_lpr(lpr, decimals)

    if decimals is not None and 0 <= decimals <= 18 and not _is_finite_pos(px):
        _bump("jup_fail")
        return None

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
    """
    m = (mint or "").strip()

    if not PF_ENABLED:
        _bump("disabled")
        return PricePoint(
            mint=m, ts_ms=_ts_ms(), price_sol=0.0, lamports_per_raw=0.0, source="disabled", meta={}
        )

    if not m:
        return PricePoint(
            mint=m, ts_ms=_ts_ms(), price_sol=0.0, lamports_per_raw=0.0, source="bad_mint", meta={}
        )

    if m == WSOL:
        return PricePoint(
            mint=m,
            ts_ms=_ts_ms(),
            price_sol=1.0,
            lamports_per_raw=1_000_000_000.0,
            source="wsol",
            meta={},
        )

    if not _network_allowed():
        _bump("no_network")
        return PricePoint(
            mint=m,
            ts_ms=_ts_ms(),
            price_sol=0.0,
            lamports_per_raw=0.0,
            source="no_network",
            meta={},
        )

    probe_ui = float(probe_tokens_ui) if probe_tokens_ui is not None else float(PF_PROBE_TOKENS_UI)
    probe_ui = max(1e-9, probe_ui)

    decimals: int | None = None
    if decimals_hint is not None:
        with contextlib.suppress(Exception):
            d = int(decimals_hint)
            if 0 <= d <= 18:
                decimals = d

    dec_key = int(decimals) if decimals is not None else -1
    p_bucket = round(probe_ui, 10)
    b_flag = 1 if (balance_raw_hint and balance_raw_hint > 0) else 0
    key = f"{m}:{dec_key}:{p_bucket}:{b_flag}"

    cached = _cache_get_price(key)
    if cached is not None:
        _bump("cache_hit")
        p_sol, lpr = cached
        return PricePoint(
            mint=m,
            ts_ms=_ts_ms(),
            price_sol=p_sol,
            lamports_per_raw=lpr,
            source="cache",
            meta={"key": key},
        )

    _bump("cache_miss")

    async with _price_lock(key):
        cached2 = _cache_get_price(key)
        if cached2 is not None:
            _bump("cache_hit")
            p_sol, lpr = cached2
            return PricePoint(
                mint=m,
                ts_ms=_ts_ms(),
                price_sol=p_sol,
                lamports_per_raw=lpr,
                source="cache",
                meta={"key": key},
            )

        if decimals is None:
            decimals = await _decimals_async(m)

        # Fast path: lightning
        ppL = await _try_lightning(m, decimals)
        if ppL and _is_finite_pos(ppL.price_sol):
            _cache_put_price(key, ppL.price_sol, ppL.lamports_per_raw or 0.0)
            return ppL

        amount_raw, effective_probe_ui = _choose_probe_amount_raw(
            decimals=decimals,
            probe_tokens_ui=probe_ui,
            balance_raw_hint=int(balance_raw_hint or 0),
        )

        # Router first (best internal execution-aligned quote)
        ppR = await _try_router_sell_quote(m, amount_raw=int(amount_raw), decimals=decimals)
        if ppR and (_is_finite_pos(ppR.lamports_per_raw) or _is_finite_pos(ppR.price_sol)):
            if (
                (not _is_finite_pos(ppR.price_sol))
                and decimals is not None
                and 0 <= decimals <= 18
                and _is_finite_pos(ppR.lamports_per_raw)
            ):
                ppR.price_sol = _price_from_lpr(ppR.lamports_per_raw, decimals)
            _cache_put_price(key, float(ppR.price_sol), float(ppR.lamports_per_raw))
            return ppR

        # Jupiter sell-probe fallback
        ppS = await _jup_sell_probe(
            m,
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

        # Last-resort buy-invert (less conservative but better than zero)
        ppB = await _jup_buy_invert(m)
        if ppB and _is_finite_pos(ppB.price_sol):
            _cache_put_price(key, ppB.price_sol, ppB.lamports_per_raw)
            return ppB

        return PricePoint(
            mint=m,
            ts_ms=_ts_ms(),
            price_sol=0.0,
            lamports_per_raw=0.0,
            source="unavailable",
            meta={"key": key},
        )


async def price_in_sol(
    mint: str,
    decimals_hint: int | None = None,
    probe_tokens: float = PF_PROBE_TOKENS_UI,
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
    Works even if decimals are unknown (as long as lamports_per_raw is obtained).
    """
    amt = int(amount_raw or 0)
    if amt <= 0:
        return 0.0

    pp = await get_price_point(mint, decimals_hint=decimals_hint, balance_raw_hint=balance_raw_hint)
    lpr = float(pp.lamports_per_raw or 0.0)
    if lpr <= 0:
        return 0.0

    out_lamports = lpr * float(amt)
    return float(out_lamports / 1_000_000_000.0)


async def close() -> None:
    """Graceful aiohttp close (call on shutdown)."""
    global _session
    if _session is not None and not getattr(_session, "closed", False):
        with contextlib.suppress(Exception):
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
