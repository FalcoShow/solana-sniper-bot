# sniper/arbitrage.py
from __future__ import annotations

"""
sniper/arbitrage.py — V3+++ (God-Tier LOCK-IN) ARB SIGNAL ONLY
=============================================================

Purpose
-------
Provide a *signal* for potential cross-DEX price dispersion using Dexscreener's
token endpoint. This module MUST NOT execute trades and MUST NOT compete with
the canonical execution path (sniper/sniper.py + utils_pkg/router + exit_engine).

Design constraints (LOCK-IN)
----------------------------
- No network at import time.
- Uses shared aiohttp.ClientSession from utils_pkg.core_utils if available.
- Uses Redis caching if available; otherwise local TTL cache.
- Filters to Solana pairs only; computes a conservative edge signal.
- Output is a simple dict describing best opportunity or None.

Typical usage
-------------
    from sniper.arbitrage import check_arbitrage_opportunity

    opp = await check_arbitrage_opportunity(mint)
    if opp:
        ... attach to candidate meta / logs ...

Notes
-----
Dexscreener data is not a live order book. Treat as *alpha hint*, not executable arb.
"""

import asyncio
import contextlib
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any

# -----------------------------------------------------------------------------
# Logging (canonical)
# -----------------------------------------------------------------------------
try:
    from logging_setup import get_logger  # type: ignore

    log = get_logger("arbitrage")
except Exception:  # pragma: no cover
    _lvl = (os.getenv("LOG_LEVEL") or "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, _lvl, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s: %(message)s",
    )
    log = logging.getLogger("arbitrage")


def _ensure_event_shim() -> None:
    if not hasattr(log, "event"):

        def _event(name: str, **kw: Any) -> None:  # pragma: no cover
            with contextlib.suppress(Exception):
                log.info("%s %s", name, kw)

        log.event = _event


_ensure_event_shim()

# -----------------------------------------------------------------------------
# Env helpers
# -----------------------------------------------------------------------------
_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _b(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return bool(default)
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return bool(default)


def _i(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(float(v)) if v not in (None, "") else int(default)
    except Exception:
        return int(default)


def _f(name: str, default: float) -> float:
    v = os.getenv(name)
    try:
        return float(v) if v not in (None, "") else float(default)
    except Exception:
        return float(default)


def _s(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()


def _short(x: Any, n: int = 260) -> str:
    try:
        s = str(x)
        return s if len(s) <= n else (s[:n] + "…")
    except Exception:
        return "<?>"


# -----------------------------------------------------------------------------
# Settings (signal-only)
# -----------------------------------------------------------------------------
ARB_ENABLED = _b("ARB_ENABLED", False)  # OFF by default (P1 only)
MIN_ARB_EDGE_BPS = _i("MIN_ARB_EDGE_BPS", 50)  # 50 bps = 0.50%

ARB_TIMEOUT_S = _f("ARB_TIMEOUT_S", 8.0)
ARB_CACHE_TTL_S = max(5, _i("ARB_CACHE_TTL_S", 20))
ARB_MAX_PAIRS = max(2, _i("ARB_MAX_PAIRS", 40))

# Guards: reduce false positives
ARB_MIN_LIQ_USD = _f("ARB_MIN_LIQ_USD", 2_000.0)
ARB_MAX_PAIR_AGE_MIN = _f("ARB_MAX_PAIR_AGE_MIN", 365.0 * 24 * 60)  # default: don't exclude by age
ARB_REQUIRE_DISTINCT_DEX = _b("ARB_REQUIRE_DISTINCT_DEX", True)

# Chain filter
ARB_CHAIN_ID = (_s("ARB_CHAIN_ID", "solana") or "solana").lower()

DEXSCREENER_TOKEN_URL = "https://api.dexscreener.com/latest/dex/tokens/{mint}"

# -----------------------------------------------------------------------------
# In-process TTL cache fallback
# -----------------------------------------------------------------------------
_LOCAL_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}  # key -> (expires_ts, value)

# Basic concurrency cap for arb fetches (avoid stampede if called from many candidates)
_ARB_SEMA = asyncio.Semaphore(max(1, _i("ARB_MAX_CONCURRENCY", 4)))


@dataclass(frozen=True)
class PairQuote:
    dex: str
    price: float  # best-effort: priceNative preferred, else USD-derived
    price_ccy: str  # "NATIVE" or "USD"
    liq_usd: float
    url: str | None
    pair_addr: str | None
    ts: float  # observed timestamp
    created_at_ts: float | None = None


def _to_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        return default if v != v else v
    except Exception:
        return default


def _to_int(x: Any, default: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return default


def _pair_created_ts(pair: dict[str, Any]) -> float | None:
    # Dexscreener often uses pairCreatedAt in ms
    v = pair.get("pairCreatedAt") or pair.get("createdAt") or None
    if v is None:
        return None
    try:
        iv = int(v)
        # heuristic: ms vs seconds
        if iv > 10_000_000_000:
            return iv / 1000.0
        return float(iv)
    except Exception:
        return None


def _extract_price(pair: dict[str, Any]) -> tuple[float | None, str]:
    """
    Prefer priceNative (typically SOL for Solana pairs). Fallback to priceUsd.
    Returns (price, ccy_label).
    """
    pn = pair.get("priceNative")
    if pn not in (None, "", 0, "0"):
        p = _to_float(pn, 0.0)
        if p > 0:
            return p, "NATIVE"

    pu = pair.get("priceUsd")
    if pu not in (None, "", 0, "0"):
        p = _to_float(pu, 0.0)
        if p > 0:
            return p, "USD"

    return None, "NA"


def _extract_liq_usd(pair: dict[str, Any]) -> float:
    liq = pair.get("liquidity") or {}
    # Dexscreener: liquidity: { usd: ..., base: ..., quote: ... }
    if isinstance(liq, dict):
        return _to_float(liq.get("usd"), 0.0)
    return _to_float(pair.get("liqUsd"), 0.0)


def _is_chain_ok(pair: dict[str, Any]) -> bool:
    chain = (pair.get("chainId") or pair.get("chain") or "").lower()
    return (not ARB_CHAIN_ID) or (chain == ARB_CHAIN_ID)


def _normalize_pairs(pairs: list[dict[str, Any]]) -> list[PairQuote]:
    out: list[PairQuote] = []
    now = time.time()

    for p in pairs[: max(1, ARB_MAX_PAIRS)]:
        if not isinstance(p, dict):
            continue
        if not _is_chain_ok(p):
            continue

        price, ccy = _extract_price(p)
        if price is None or price <= 0:
            continue

        dex = str(p.get("dexId") or p.get("dex") or "unknown").strip() or "unknown"
        url = p.get("url")
        pair_addr = p.get("pairAddress") or p.get("pair") or p.get("address")
        liq_usd = float(_extract_liq_usd(p))
        created = _pair_created_ts(p)

        # Liquidity guard
        if liq_usd > 0 and liq_usd < float(ARB_MIN_LIQ_USD):
            continue

        # Optional age guard
        if created is not None and ARB_MAX_PAIR_AGE_MIN > 0:
            age_min = (now - float(created)) / 60.0
            if age_min < 0:
                age_min = 0
            if age_min > float(ARB_MAX_PAIR_AGE_MIN):
                continue

        out.append(
            PairQuote(
                dex=dex,
                price=float(price),
                price_ccy=ccy,
                liq_usd=float(liq_usd),
                url=str(url) if url else None,
                pair_addr=str(pair_addr) if pair_addr else None,
                ts=now,
                created_at_ts=float(created) if created is not None else None,
            )
        )

    return out


def _best_arb(quotes: list[PairQuote], *, min_edge_bps: int) -> dict[str, Any] | None:
    """
    Edge (bps) = (best_sell - best_buy) / mid * 10_000
    Uses conservative mid. Requires distinct DEX if configured.
    """
    if len(quotes) < 2:
        return None

    # sort by price
    qs = sorted(quotes, key=lambda q: q.price)
    best_buy = qs[0]
    best_sell = qs[-1]

    if ARB_REQUIRE_DISTINCT_DEX and best_buy.dex == best_sell.dex:
        return None

    mid = (best_buy.price + best_sell.price) / 2.0
    if mid <= 0:
        return None

    edge_bps = int(((best_sell.price - best_buy.price) / mid) * 10_000)
    if edge_bps < int(min_edge_bps):
        return None

    # Provide extra context: spread %, liquidity hints
    edge_pct = (best_sell.price - best_buy.price) / mid * 100.0

    return {
        "edge_bps": int(edge_bps),
        "edge_pct": float(edge_pct),
        "buy": {
            "dex": best_buy.dex,
            "price": best_buy.price,
            "ccy": best_buy.price_ccy,
            "liq_usd": best_buy.liq_usd,
            "url": best_buy.url,
            "pair": best_buy.pair_addr,
        },
        "sell": {
            "dex": best_sell.dex,
            "price": best_sell.price,
            "ccy": best_sell.price_ccy,
            "liq_usd": best_sell.liq_usd,
            "url": best_sell.url,
            "pair": best_sell.pair_addr,
        },
        "quotes_seen": int(len(quotes)),
        "ccy_note": "NATIVE preferred; USD fallback possible",
    }


# -----------------------------------------------------------------------------
# core_utils accessors (lazy, best-effort)
# -----------------------------------------------------------------------------
def _get_session() -> Any:
    """
    Prefer utils_pkg.core_utils.get_session() if present.
    Fallback to utils_pkg.core_utils.session if present (legacy).
    """
    try:
        import importlib

        m = importlib.import_module("utils_pkg.core_utils")
        gs = getattr(m, "get_session", None)
        if callable(gs):
            return gs()
        return getattr(m, "session", None)
    except Exception:
        return None


def _get_redis() -> Any:
    try:
        import importlib

        m = importlib.import_module("utils_pkg.core_utils")
        gr = getattr(m, "get_redis", None)
        if callable(gr):
            return gr()
        return getattr(m, "redis", None)
    except Exception:
        return None


async def _redis_get(redis: Any, key: str) -> str | None:
    if not redis:
        return None
    try:
        v = redis.get(key)
        if asyncio.iscoroutine(v):
            v = await v
        if v is None:
            return None
        if isinstance(v, bytes | bytearray):
            return v.decode("utf-8", "ignore")
        return str(v)
    except Exception:
        return None


async def _redis_set(redis: Any, key: str, value: str, ex: int) -> None:
    if not redis:
        return
    try:
        out = redis.set(key, value, ex=ex)
        if asyncio.iscoroutine(out):
            await out
    except TypeError:
        # older client signature
        try:
            out2 = redis.set(key, value)
            if asyncio.iscoroutine(out2):
                await out2
            if hasattr(redis, "expire"):
                out3 = redis.expire(key, int(ex))
                if asyncio.iscoroutine(out3):
                    await out3
        except Exception:
            return
    except Exception:
        return


# -----------------------------------------------------------------------------
# HTTP fetch (Dexscreener)
# -----------------------------------------------------------------------------
async def _fetch_dexscreener_token_json(mint: str, *, timeout_s: float) -> dict[str, Any] | None:
    session = _get_session()
    if not session:
        log.event("ARB_NO_SESSION", mint=mint)
        return None

    url = DEXSCREENER_TOKEN_URL.format(mint=mint)
    try:
        # aiohttp-style
        async with _ARB_SEMA, session.get(url, timeout=float(timeout_s)) as resp:
            if int(getattr(resp, "status", 0) or 0) != 200:
                txt = ""
                with contextlib.suppress(Exception):
                    txt = await resp.text()
                log.event("ARB_HTTP_NON200", mint=mint, status=int(resp.status), body=_short(txt))
                return None
            data = await resp.json()
            return data if isinstance(data, dict) else None
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.event("ARB_HTTP_ERR", mint=mint, err=_short(e))
        return None


async def _get_cached_token_json(mint: str) -> dict[str, Any] | None:
    """
    Redis-first caching; otherwise local TTL cache.
    """
    key = f"arb:dexscreener:{mint}"
    int(ARB_CACHE_TTL_S)

    r = _get_redis()
    if r:
        raw = await _redis_get(r, key)
        if raw:
            with contextlib.suppress(Exception):
                obj = json.loads(raw)
                if isinstance(obj, dict):
                    return obj

    # local cache
    now = time.time()
    hit = _LOCAL_CACHE.get(key)
    if hit:
        exp, val = hit
        if now < exp:
            return val
        _LOCAL_CACHE.pop(key, None)
    return None


async def _set_cached_token_json(mint: str, obj: dict[str, Any]) -> None:
    key = f"arb:dexscreener:{mint}"
    ttl = int(ARB_CACHE_TTL_S)
    now = time.time()

    # local cache
    _LOCAL_CACHE[key] = (now + ttl, obj)

    # redis cache (best-effort)
    r = _get_redis()
    if r:
        with contextlib.suppress(Exception):
            await _redis_set(
                r, key, json.dumps(obj, separators=(",", ":"), ensure_ascii=False), ex=ttl
            )


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------
async def check_arbitrage_opportunity(
    token_mint: str,
    *,
    min_edge_bps: int | None = None,
    enabled: bool | None = None,
) -> dict[str, Any] | None:
    """
    Returns best arb opportunity dict if edge >= threshold, else None.

    Dict fields:
      {
        "token": <mint>,
        "edge_bps": int,
        "edge_pct": float,
        "quotes_seen": int,
        "buy": {...},
        "sell": {...},
        "suggestion": "...",
        "source": "dexscreener",
        "ts": "<iso-ish>"
      }
    """
    if enabled is None:
        enabled = bool(ARB_ENABLED)
    if not enabled:
        return None

    mint = str(token_mint or "").strip()
    if not mint:
        return None

    thr = int(MIN_ARB_EDGE_BPS if min_edge_bps is None else int(min_edge_bps))
    if thr <= 0:
        thr = 1

    # cache
    cached = await _get_cached_token_json(mint)
    data = cached
    if data is None:
        data = await _fetch_dexscreener_token_json(mint, timeout_s=float(ARB_TIMEOUT_S))
        if isinstance(data, dict):
            await _set_cached_token_json(mint, data)
        else:
            return None

    pairs = data.get("pairs", []) if isinstance(data, dict) else []
    if not isinstance(pairs, list) or len(pairs) < 2:
        return None

    quotes = _normalize_pairs([p for p in pairs if isinstance(p, dict)])
    if len(quotes) < 2:
        return None

    opp_core = _best_arb(quotes, min_edge_bps=thr)
    if not opp_core:
        return None

    # Compose final opp
    opp = {
        "token": mint,
        "source": "dexscreener",
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        **opp_core,
        "suggestion": (
            f"Signal: Buy on {opp_core['buy']['dex']} @ {opp_core['buy']['price']} "
            f"→ Sell on {opp_core['sell']['dex']} @ {opp_core['sell']['price']} "
            f"(+{opp_core['edge_bps']} bps)"
        ),
    }

    log.event(
        "ARB_OPP",
        token=mint,
        edge_bps=int(opp_core["edge_bps"]),
        buy_dex=opp_core["buy"]["dex"],
        sell_dex=opp_core["sell"]["dex"],
        quotes=int(opp_core["quotes_seen"]),
    )
    return opp


async def arbitrage_hint_for_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
    """
    Convenience: attach an arb hint to a candidate snapshot without changing flow.

    Returns:
      {"ok": bool, "edge_bps": int, "opp": Optional[dict], "reason": str}
    """
    mint = str(
        candidate.get("mint") or candidate.get("base_mint") or candidate.get("address") or ""
    ).strip()
    if not mint:
        return {"ok": False, "edge_bps": 0, "opp": None, "reason": "no_mint"}

    if not ARB_ENABLED:
        return {"ok": False, "edge_bps": 0, "opp": None, "reason": "arb_disabled"}

    opp = await check_arbitrage_opportunity(mint)
    if not opp:
        return {"ok": True, "edge_bps": 0, "opp": None, "reason": "no_edge"}

    return {
        "ok": True,
        "edge_bps": int(opp.get("edge_bps") or 0),
        "opp": opp,
        "reason": "edge_found",
    }


__all__ = ["arbitrage_hint_for_candidate", "check_arbitrage_opportunity"]
