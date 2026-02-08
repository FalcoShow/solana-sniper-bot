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
- Prefer shared aiohttp.ClientSession from utils_pkg.core_utils if available.
- Prefer Redis caching if available; otherwise local TTL cache.
- Filters to Solana pairs only; computes a conservative edge signal.
- Output is a simple dict describing best opportunity or None.

Dexscreener note
----------------
Dexscreener is not a live order book. Treat this as *alpha hint*, not executable arb.
"""

import asyncio
import contextlib
import json
import logging
import os
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final

# -----------------------------------------------------------------------------
# Logging (no import-time side-effects)
# -----------------------------------------------------------------------------
log = logging.getLogger("arbitrage")
if not log.handlers:
    log.addHandler(logging.NullHandler())


def _log_event(name: str, **fields: Any) -> None:
    """
    Structured event logging:
    - If the logger provides .event(name, **fields), we use it.
    - Otherwise we log a compact JSON-ish payload.
    Never raises.
    """
    fn = getattr(log, "event", None)
    if callable(fn):
        with contextlib.suppress(Exception):
            fn(name, **fields)
            return

    with contextlib.suppress(Exception):
        payload = json.dumps(fields, ensure_ascii=False, separators=(",", ":"), default=str)
        log.info("%s %s", name, payload)


# -----------------------------------------------------------------------------
# Env helpers (tiny, robust)
# -----------------------------------------------------------------------------
_TRUE: Final[set[str]] = {"1", "true", "t", "yes", "y", "on"}
_FALSE: Final[set[str]] = {"0", "false", "f", "no", "n", "off"}


def _env_get(key: str) -> str | None:
    # keep it explicit for env-contract tools
    return os.environ.get(key)


def _env_bool(key: str, default: bool) -> bool:
    v = _env_get(key)
    if v is None or not str(v).strip():
        return bool(default)
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return bool(default)


def _env_int(key: str, default: int) -> int:
    v = _env_get(key)
    if v is None or not str(v).strip():
        return int(default)
    try:
        return int(float(str(v).strip()))
    except Exception:
        return int(default)


def _env_float(key: str, default: float) -> float:
    v = _env_get(key)
    if v is None or not str(v).strip():
        return float(default)
    try:
        return float(str(v).strip())
    except Exception:
        return float(default)


def _env_str(key: str, default: str = "") -> str:
    v = _env_get(key)
    return default if v is None else str(v).strip()


def _short(x: Any, n: int = 240) -> str:
    try:
        s = str(x)
    except Exception:
        s = repr(x)
    return s if len(s) <= n else (s[:n] + "…")


# -----------------------------------------------------------------------------
# Config (signal-only)
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class ArbConfig:
    enabled: bool = False  # OFF by default (P1 only)
    min_edge_bps: int = 50

    timeout_s: float = 8.0
    cache_ttl_s: int = 20
    max_pairs: int = 40
    max_concurrency: int = 4

    min_liq_usd: float = 2_000.0
    max_pair_age_min: float = 365.0 * 24.0 * 60.0  # default: don't exclude by age
    require_distinct_dex: bool = True

    chain_id: str = "solana"
    token_url_tmpl: str = "https://api.dexscreener.com/latest/dex/tokens/{mint}"

    @staticmethod
    def from_env() -> ArbConfig:
        return ArbConfig(
            enabled=_env_bool("ARB_ENABLED", False),
            min_edge_bps=max(1, _env_int("MIN_ARB_EDGE_BPS", 50)),
            timeout_s=max(0.25, _env_float("ARB_TIMEOUT_S", 8.0)),
            cache_ttl_s=max(5, _env_int("ARB_CACHE_TTL_S", 20)),
            max_pairs=max(2, _env_int("ARB_MAX_PAIRS", 40)),
            max_concurrency=max(1, _env_int("ARB_MAX_CONCURRENCY", 4)),
            min_liq_usd=max(0.0, _env_float("ARB_MIN_LIQ_USD", 2_000.0)),
            max_pair_age_min=max(0.0, _env_float("ARB_MAX_PAIR_AGE_MIN", 365.0 * 24.0 * 60.0)),
            require_distinct_dex=_env_bool("ARB_REQUIRE_DISTINCT_DEX", True),
            chain_id=(_env_str("ARB_CHAIN_ID", "solana") or "solana").lower(),
            token_url_tmpl="https://api.dexscreener.com/latest/dex/tokens/{mint}",
        )


# -----------------------------------------------------------------------------
# Caching (Redis-first, fallback local TTL)
# -----------------------------------------------------------------------------
_LOCAL_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}  # key -> (expires_ts, value)
_INFLIGHT: dict[str, asyncio.Lock] = {}
_INFLIGHT_GUARD = asyncio.Lock()

_SEMA_BY_N: dict[int, asyncio.Semaphore] = {}


def _get_sema(n: int) -> asyncio.Semaphore:
    n = max(1, int(n))
    sema = _SEMA_BY_N.get(n)
    if sema is None:
        sema = asyncio.Semaphore(n)
        _SEMA_BY_N[n] = sema
    return sema


async def _mint_lock(mint: str) -> asyncio.Lock:
    # prevent cache stampede per mint
    async with _INFLIGHT_GUARD:
        lk = _INFLIGHT.get(mint)
        if lk is None:
            lk = asyncio.Lock()
            _INFLIGHT[mint] = lk
        return lk


# -----------------------------------------------------------------------------
# core_utils accessors (lazy, best-effort)
# -----------------------------------------------------------------------------
def _get_session() -> Any:
    """
    Prefer utils_pkg.core_utils.get_session() if present.
    Fallback to utils_pkg.core_utils.session (legacy).
    """
    try:
        import importlib

        m = importlib.import_module("utils_pkg.core_utils")
        gs = getattr(m, "get_session", None)
        if callable(gs):
            s = gs()
            return s
        return getattr(m, "session", None)
    except Exception:
        return None


def _get_redis() -> Any:
    """
    Prefer utils_pkg.core_utils.get_redis_async/get_redis if present.
    Fallback to utils_pkg.core_utils.redis (legacy).
    """
    try:
        import importlib

        m = importlib.import_module("utils_pkg.core_utils")
        fn = getattr(m, "get_redis_async", None) or getattr(m, "get_redis", None)
        if callable(fn):
            r = fn()
            return r
        return getattr(m, "redis", None)
    except Exception:
        return None


async def _await_if_needed(x: Any) -> Any:
    if asyncio.iscoroutine(x):
        return await x
    return x


async def _redis_get(redis: Any, key: str) -> str | None:
    if not redis:
        return None
    try:
        v = await _await_if_needed(redis.get(key))
        if v is None:
            return None
        if isinstance(v, (bytes, bytearray)):
            return v.decode("utf-8", "ignore")
        return str(v)
    except Exception:
        return None


async def _redis_set(redis: Any, key: str, value: str, ex: int) -> None:
    if not redis:
        return
    ex_i = max(1, int(ex))
    # Try set(key, value, ex=...)
    try:
        await _await_if_needed(redis.set(key, value, ex=ex_i))
        return
    except TypeError:
        pass
    except Exception:
        return

    # Fallback: set + expire
    try:
        await _await_if_needed(redis.set(key, value))
        if hasattr(redis, "expire"):
            await _await_if_needed(redis.expire(key, ex_i))
    except Exception:
        return


def _cache_key(mint: str) -> str:
    return f"arb:dexscreener:{mint}"


async def _cache_get(mint: str) -> dict[str, Any] | None:
    key = _cache_key(mint)

    # Redis first
    r = _get_redis()
    r = await _await_if_needed(r)
    if r:
        raw = await _redis_get(r, key)
        if raw:
            with contextlib.suppress(Exception):
                obj = json.loads(raw)
                if isinstance(obj, dict):
                    return obj

    # Local cache
    now = time.time()
    hit = _LOCAL_CACHE.get(key)
    if hit:
        exp, val = hit
        if now < exp:
            return val
        _LOCAL_CACHE.pop(key, None)
    return None


async def _cache_set(mint: str, obj: dict[str, Any], ttl_s: int) -> None:
    key = _cache_key(mint)
    ttl = max(5, int(ttl_s))
    now = time.time()

    _LOCAL_CACHE[key] = (now + ttl, obj)

    r = _get_redis()
    r = await _await_if_needed(r)
    if r:
        with contextlib.suppress(Exception):
            blob = json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str)
            await _redis_set(r, key, blob, ex=ttl)


# -----------------------------------------------------------------------------
# Pair normalization
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class PairQuote:
    dex: str
    price: float
    price_ccy: str  # "NATIVE" | "USD"
    liq_usd: float
    url: str | None
    pair_addr: str | None
    ts: float
    created_at_ts: float | None = None


def _to_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        return default if v != v else v  # NaN guard
    except Exception:
        return default


def _pair_created_ts(pair: Mapping[str, Any]) -> float | None:
    # Dexscreener usually uses pairCreatedAt in ms
    v = pair.get("pairCreatedAt") or pair.get("createdAt")
    if v is None:
        return None
    try:
        iv = int(float(v))
        # heuristic: ms vs seconds
        if iv > 10_000_000_000:
            return iv / 1000.0
        return float(iv)
    except Exception:
        return None


def _extract_price(pair: Mapping[str, Any]) -> tuple[float | None, str]:
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


def _extract_liq_usd(pair: Mapping[str, Any]) -> float:
    liq = pair.get("liquidity")
    if isinstance(liq, Mapping):
        return _to_float(liq.get("usd"), 0.0)
    return _to_float(pair.get("liqUsd"), 0.0)


def _is_chain_ok(pair: Mapping[str, Any], chain_id: str) -> bool:
    if not chain_id:
        return True
    chain = str(pair.get("chainId") or pair.get("chain") or "").lower()
    return chain == chain_id


def _normalize_pairs(pairs: list[Mapping[str, Any]], cfg: ArbConfig) -> list[PairQuote]:
    out: list[PairQuote] = []
    now = time.time()

    for p in pairs[: max(1, cfg.max_pairs)]:
        if not _is_chain_ok(p, cfg.chain_id):
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
        if liq_usd > 0 and liq_usd < cfg.min_liq_usd:
            continue

        # Optional age guard
        if created is not None and cfg.max_pair_age_min > 0:
            age_min = max(0.0, (now - float(created)) / 60.0)
            if age_min > cfg.max_pair_age_min:
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


def _best_arb(quotes: list[PairQuote], cfg: ArbConfig, min_edge_bps: int) -> dict[str, Any] | None:
    """
    Edge (bps) = (best_sell - best_buy) / mid * 10_000
    Uses conservative mid. Requires distinct DEX if configured.
    """
    if len(quotes) < 2:
        return None

    qs = sorted(quotes, key=lambda q: q.price)
    buy = qs[0]
    sell = qs[-1]

    if cfg.require_distinct_dex and buy.dex == sell.dex:
        return None

    mid = (buy.price + sell.price) / 2.0
    if mid <= 0:
        return None

    edge_bps = int(((sell.price - buy.price) / mid) * 10_000)
    if edge_bps < int(min_edge_bps):
        return None

    edge_pct = (sell.price - buy.price) / mid * 100.0

    return {
        "edge_bps": int(edge_bps),
        "edge_pct": float(edge_pct),
        "buy": {
            "dex": buy.dex,
            "price": buy.price,
            "ccy": buy.price_ccy,
            "liq_usd": buy.liq_usd,
            "url": buy.url,
            "pair": buy.pair_addr,
        },
        "sell": {
            "dex": sell.dex,
            "price": sell.price,
            "ccy": sell.price_ccy,
            "liq_usd": sell.liq_usd,
            "url": sell.url,
            "pair": sell.pair_addr,
        },
        "quotes_seen": int(len(quotes)),
        "ccy_note": "NATIVE preferred; USD fallback possible",
    }


# -----------------------------------------------------------------------------
# HTTP fetch (Dexscreener)
# -----------------------------------------------------------------------------
async def _fetch_dexscreener_token_json(mint: str, cfg: ArbConfig) -> dict[str, Any] | None:
    session = _get_session()
    session = await _await_if_needed(session)
    if not session:
        _log_event("ARB_NO_SESSION", mint=mint)
        return None

    url = cfg.token_url_tmpl.format(mint=mint)
    sema = _get_sema(cfg.max_concurrency)

    await sema.acquire()
    try:
        # Try with timeout; fallback if wrapper doesn't accept it
        try:
            req = session.get(url, timeout=float(cfg.timeout_s))
        except TypeError:
            req = session.get(url)

        async with req as resp:
            status = int(getattr(resp, "status", 0) or 0)
            if status != 200:
                txt = ""
                with contextlib.suppress(Exception):
                    txt = await resp.text()
                _log_event("ARB_HTTP_NON200", mint=mint, status=status, body=_short(txt))
                return None

            try:
                data = await resp.json(content_type=None)
            except TypeError:
                data = await resp.json()

            return data if isinstance(data, dict) else None
    except asyncio.CancelledError:
        raise
    except Exception as e:
        _log_event("ARB_HTTP_ERR", mint=mint, err=_short(e))
        return None
    finally:
        sema.release()


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

    Output:
      {
        "token": <mint>,
        "source": "dexscreener",
        "ts": "<UTC iso-ish>",
        "edge_bps": int,
        "edge_pct": float,
        "quotes_seen": int,
        "buy": {...},
        "sell": {...},
        "suggestion": str,
      }
    """
    cfg = ArbConfig.from_env()

    if enabled is None:
        enabled = cfg.enabled
    if not enabled:
        return None

    mint = str(token_mint or "").strip()
    if not mint:
        return None

    thr = int(cfg.min_edge_bps if min_edge_bps is None else max(1, int(min_edge_bps)))

    # prevent stampede per mint
    lk = await _mint_lock(mint)
    async with lk:
        cached = await _cache_get(mint)
        if cached is None:
            data = await _fetch_dexscreener_token_json(mint, cfg)
            if not isinstance(data, dict):
                return None
            await _cache_set(mint, data, ttl_s=cfg.cache_ttl_s)
        else:
            data = cached

    pairs_any = data.get("pairs") if isinstance(data, dict) else None
    if not isinstance(pairs_any, list) or len(pairs_any) < 2:
        return None

    pairs: list[Mapping[str, Any]] = [p for p in pairs_any if isinstance(p, Mapping)]
    quotes = _normalize_pairs(pairs, cfg)
    if len(quotes) < 2:
        return None

    opp_core = _best_arb(quotes, cfg, min_edge_bps=thr)
    if not opp_core:
        return None

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

    _log_event(
        "ARB_OPP",
        token=mint,
        edge_bps=int(opp_core["edge_bps"]),
        buy_dex=str(opp_core["buy"]["dex"]),
        sell_dex=str(opp_core["sell"]["dex"]),
        quotes=int(opp_core["quotes_seen"]),
    )
    return opp


async def arbitrage_hint_for_candidate(candidate: Mapping[str, Any]) -> dict[str, Any]:
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

    cfg = ArbConfig.from_env()
    if not cfg.enabled:
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
