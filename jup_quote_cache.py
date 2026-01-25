# jup_quote_cache.py
from __future__ import annotations

"""
jup_quote_cache.py (ROOT) — V3+++ (God-Tier Overhaul)
=====================================================

Role
----
A small, *safe*, *bounded*, best-effort quote cache intended for:
- route probing (guards/watchdogs)
- optional pre-warming of top candidates
- minimizing hammering Jupiter when scanners are hot

Design rules
------------
- Never raises (public APIs)
- No network at import time
- Memory cache + optional Redis cache + network fetch fallback
- Stampede protection (per-key in-flight dedupe)
- Negative caching (short TTL) on repeated failures
- Backwards compatible with your older call sites:
    get_quote_with_age(out_mint, lamports, slippage_bps)  # assumes SOL -> out_mint

Important
---------
Your canonical router is utils_pkg/router.py. This cache is *not* a second router.
It only fetches /quote payloads and caches them.

Env
---
Base URL (any one):
  - JUP_BASE_URL               (e.g. https://api.jup.ag)   -> will call /quote
  - JUPITER_SWAP_API           (e.g. https://quote-api.jup.ag/v6) -> will call /quote
  - JUP_QUOTE_API              (explicit, e.g. https://api.jup.ag/quote)

Cache:
  - QUOTE_CACHE_ENABLED=1
  - QUOTE_TTL_SEC=30
  - QUOTE_NEG_TTL_SEC=6
  - QUOTE_MEM_MAX_KEYS=4000
  - QUOTE_NET_TIMEOUT_S=8
  - QUOTE_NET_MAX_PARALLEL=12
  - QUOTE_PREWARM_TOPK=8
  - QUOTE_PREWARM_TTL_SEC (legacy alias for QUOTE_TTL_SEC)

Redis:
  - REDIS_URL=redis://...
  - QUOTE_REDIS_ENABLED=1   (auto-disabled if Redis not available)

Telemetry (optional):
  - ROUTE_TELEM_ENABLED=1   (handled by route_telemetry itself)
"""

import asyncio
import contextlib
import json
import os
import time
from dataclasses import dataclass
from typing import Any

# ──────────────────────────────────────────────────────────────────────────────
# Logging shim-safe
# ──────────────────────────────────────────────────────────────────────────────
try:
    from logging_setup import get_logger  # type: ignore

    log = get_logger(__name__)  # type: ignore
except Exception:  # pragma: no cover
    import logging

    log = logging.getLogger(__name__)  # type: ignore[assignment]


def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    return (v if v is not None else d).strip()


_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}


def _envb(k: str, d: bool = False) -> bool:
    v = _envs(k, "")
    if v == "":
        return bool(d)
    s = v.lower().strip()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return bool(d)


def _envi(k: str, d: int) -> int:
    try:
        return int(float(_envs(k, str(d))))
    except Exception:
        return int(d)


def _envf(k: str, d: float) -> float:
    try:
        return float(_envs(k, str(d)))
    except Exception:
        return float(d)


# ──────────────────────────────────────────────────────────────────────────────
# Constants / defaults
# ──────────────────────────────────────────────────────────────────────────────
SOL_MINT = "So11111111111111111111111111111111111111112"
LAMPORTS_PER_SOL = 1_000_000_000

QUOTE_CACHE_ENABLED = _envb("QUOTE_CACHE_ENABLED", True)

# Legacy alias supported
QUOTE_TTL_SEC = _envi("QUOTE_TTL_SEC", _envi("QUOTE_PREWARM_TTL_SEC", 30))
QUOTE_NEG_TTL_SEC = _envi("QUOTE_NEG_TTL_SEC", 6)

QUOTE_MEM_MAX_KEYS = _envi("QUOTE_MEM_MAX_KEYS", 4000)

QUOTE_NET_TIMEOUT_S = _envf("QUOTE_NET_TIMEOUT_S", 8.0)
QUOTE_NET_MAX_PARALLEL = _envi("QUOTE_NET_MAX_PARALLEL", 12)

PREWARM_TOPK = _envi("QUOTE_PREWARM_TOPK", 8)

QUOTE_REDIS_ENABLED = _envb("QUOTE_REDIS_ENABLED", True)
REDIS_URL = _envs("REDIS_URL", "redis://localhost:6379")

# Hard cap to prevent gigantic payloads from living in Redis forever
_MAX_JSON_BYTES = _envi("QUOTE_MAX_JSON_BYTES", 1_200_000)

# ──────────────────────────────────────────────────────────────────────────────
# Telemetry (optional)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from route_telemetry import count_quote_outcome  # type: ignore
except Exception:  # pragma: no cover

    async def count_quote_outcome(kind: str, ok: bool, status: int | None = None) -> None:  # type: ignore
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Quote endpoint resolution
# ──────────────────────────────────────────────────────────────────────────────
def _quote_url() -> str:
    """
    Resolves to a full /quote endpoint URL.
    Accepts multiple env styles, normalizes trailing slashes.
    """
    explicit = _envs("JUP_QUOTE_API", "")
    if explicit:
        return explicit.rstrip("/")

    base = _envs("JUP_BASE_URL", "")
    if base:
        return (base.rstrip("/") + "/quote").rstrip("/")

    legacy = _envs("JUPITER_SWAP_API", "https://quote-api.jup.ag/v6")
    legacy = legacy.rstrip("/")
    # If legacy already ends with /quote, keep it; else append /quote.
    if legacy.endswith("/quote"):
        return legacy
    return legacy + "/quote"


# ──────────────────────────────────────────────────────────────────────────────
# Memory cache + in-flight dedupe
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class _MemEntry:
    ts: float
    payload: dict | None  # None = negative cache
    neg: bool = False


_mem: dict[str, _MemEntry] = {}
_mem_lock = asyncio.Lock()
_inflight: dict[str, asyncio.Future] = {}
_inflight_lock = asyncio.Lock()

_net_sema = asyncio.Semaphore(max(1, QUOTE_NET_MAX_PARALLEL))


def _now() -> float:
    return time.time()


def _mk_key(
    input_mint: str,
    output_mint: str,
    amount: int,
    slippage_bps: int,
    swap_mode: str,
    only_direct: bool,
    as_legacy: bool,
) -> str:
    # include enough fields to avoid cross-contamination
    return (
        f"jup:quote:"
        f"{'legacy' if as_legacy else 'v6'}:"
        f"{input_mint}:{output_mint}:{int(amount)}:{int(slippage_bps)}:"
        f"{swap_mode}:{'D1' if only_direct else 'D0'}"
    )


def _is_fresh(ts: float, ttl: int) -> bool:
    return (_now() - float(ts)) <= float(max(0, ttl))


async def _mem_get(k: str) -> tuple[dict | None, float | None]:
    if not QUOTE_CACHE_ENABLED:
        return None, None
    async with _mem_lock:
        e = _mem.get(k)
        if not e:
            return None, None
        ttl = QUOTE_NEG_TTL_SEC if e.neg else QUOTE_TTL_SEC
        if not _is_fresh(e.ts, ttl):
            _mem.pop(k, None)
            return None, None
        if e.payload is None:
            return None, None
        return e.payload, (_now() - e.ts)


async def _mem_put(
    k: str, payload: dict | None, ts: float | None = None, neg: bool = False
) -> None:
    if not QUOTE_CACHE_ENABLED:
        return
    async with _mem_lock:
        if len(_mem) >= max(256, QUOTE_MEM_MAX_KEYS):
            # simple eviction: remove ~10% oldest by ts
            try:
                items = sorted(_mem.items(), key=lambda kv: kv[1].ts)
                for kk, _ in items[: max(1, len(items) // 10)]:
                    _mem.pop(kk, None)
            except Exception:
                _mem.clear()
        _mem[k] = _MemEntry(
            ts=float(ts if ts is not None else _now()), payload=payload, neg=bool(neg)
        )


# ──────────────────────────────────────────────────────────────────────────────
# Redis cache (best-effort)
# ──────────────────────────────────────────────────────────────────────────────
class _NoRedis:
    async def get(self, *_a: Any, **_k: Any) -> str | None:
        return None

    async def set(self, *_a: Any, **_k: Any) -> None:
        return None


_redis_client: Any = None
_redis_lock = asyncio.Lock()


async def _redis() -> Any:
    global _redis_client
    if not QUOTE_REDIS_ENABLED:
        return _NoRedis()
    if _redis_client is not None:
        return _redis_client

    async with _redis_lock:
        if _redis_client is not None:
            return _redis_client

        # Prefer shared redis from utils_pkg.core_utils
        try:
            from utils_pkg import core_utils as cu  # type: ignore

            r = await cu.get_redis_async()  # type: ignore[attr-defined]
            _redis_client = r
            return _redis_client
        except Exception:
            pass

        # Fallback: direct aioredis client
        try:
            import redis.asyncio as aioredis  # type: ignore

            _redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
            return _redis_client
        except Exception:
            _redis_client = _NoRedis()
            return _redis_client


async def _redis_get(k: str) -> tuple[dict | None, float | None]:
    if not QUOTE_CACHE_ENABLED or not QUOTE_REDIS_ENABLED:
        return None, None

    r = await _redis()
    try:
        s = await r.get(k)
        if not s:
            return None, None
        # Accept either {"cached_at": ts, "payload": {...}} or {"ts":..., "payload":...}
        obj = json.loads(s)
        ts = float(obj.get("cached_at", obj.get("ts", 0.0)) or 0.0)
        payload = obj.get("payload")
        if not isinstance(payload, dict):
            return None, None
        if not _is_fresh(ts, QUOTE_TTL_SEC):
            return None, None
        return payload, (_now() - ts)
    except Exception:
        return None, None


async def _redis_set(k: str, payload: dict, ts: float | None = None) -> None:
    if not QUOTE_CACHE_ENABLED or not QUOTE_REDIS_ENABLED:
        return

    r = await _redis()
    try:
        body = json.dumps(
            {"cached_at": float(ts if ts is not None else _now()), "payload": payload}
        )
        if len(body.encode("utf-8")) > _MAX_JSON_BYTES:
            return
        await r.set(k, body, ex=int(max(1, QUOTE_TTL_SEC)))
    except Exception:
        return


# ──────────────────────────────────────────────────────────────────────────────
# Network fetch
# ──────────────────────────────────────────────────────────────────────────────
def _build_params(
    input_mint: str,
    output_mint: str,
    amount: int,
    slippage_bps: int,
    only_direct_routes: bool,
    swap_mode: str,
) -> dict[str, str]:
    return {
        "inputMint": str(input_mint),
        "outputMint": str(output_mint),
        "amount": str(int(max(1, amount))),
        "slippageBps": str(int(max(1, slippage_bps))),
        "onlyDirectRoutes": "true" if only_direct_routes else "false",
        "swapMode": str(swap_mode or "ExactIn"),
    }


async def _fetch_quote_http(params: dict[str, str]) -> tuple[dict | None, int | None]:
    """
    Returns (payload, http_status). Never raises.
    """
    url = _quote_url()
    # Prefer a shared aiohttp session if you have one (avoid heavy session churn)
    sess = None
    try:
        from utils_pkg.honeypot import _get_session  # type: ignore

        sess = await _get_session()  # type: ignore[misc]
    except Exception:
        sess = None

    async with _net_sema:
        try:
            if sess is not None:
                async with sess.get(url, params=params, timeout=QUOTE_NET_TIMEOUT_S) as resp:
                    st = int(resp.status)
                    if st != 200:
                        return None, st
                    return await resp.json(), st

            # Fallback: create a small local session
            import aiohttp  # local import for optional dependency tolerance

            timeout = aiohttp.ClientTimeout(total=float(max(1.0, QUOTE_NET_TIMEOUT_S)))
            async with aiohttp.ClientSession(timeout=timeout) as s2:
                async with s2.get(url, params=params) as resp:
                    st = int(resp.status)
                    if st != 200:
                        return None, st
                    return await resp.json(), st
        except Exception:
            return None, None


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────
async def get_quote_with_age(  # Back-compat signature
    out_mint: str,
    lamports: int,
    slippage_bps: int,
) -> tuple[dict | None, float | None]:
    """
    Backwards compatible helper used by older route_guard.py:
      SOL_MINT -> out_mint, ExactIn.
    """
    return await get_quote_with_age_io(
        input_mint=SOL_MINT,
        output_mint=out_mint,
        amount=int(lamports),
        slippage_bps=int(slippage_bps),
        only_direct_routes=False,
        swap_mode="ExactIn",
        as_legacy=False,
    )


async def get_quote_with_age_io(
    *,
    input_mint: str,
    output_mint: str,
    amount: int,
    slippage_bps: int,
    only_direct_routes: bool = False,
    swap_mode: str = "ExactIn",
    as_legacy: bool = False,
) -> tuple[dict | None, float | None]:
    """
    Canonical function:
      Returns (payload, age_sec). If no payload: (None, None).

    Cache order:
      memory -> redis -> network
    With per-key in-flight dedupe + negative caching.
    """
    try:
        input_mint = (input_mint or "").strip()
        output_mint = (output_mint or "").strip()
        if not input_mint or not output_mint or int(amount) <= 0:
            return None, None
        slippage_bps = int(max(1, int(slippage_bps)))

        k = _mk_key(
            input_mint=input_mint,
            output_mint=output_mint,
            amount=int(amount),
            slippage_bps=slippage_bps,
            swap_mode=swap_mode,
            only_direct=bool(only_direct_routes),
            as_legacy=bool(as_legacy),
        )

        # 1) memory
        payload, age = await _mem_get(k)
        if payload:
            return payload, age

        # 2) redis
        payload, age = await _redis_get(k)
        if payload:
            await _mem_put(k, payload, ts=_now() - float(age or 0.0), neg=False)
            return payload, age

        # 3) in-flight dedupe (prevent stampede)
        async with _inflight_lock:
            fut = _inflight.get(k)
            if fut is None:
                fut = asyncio.get_event_loop().create_future()
                _inflight[k] = fut
                leader = True
            else:
                leader = False

        if not leader:
            try:
                res = await asyncio.wait_for(fut, timeout=float(max(1, QUOTE_NET_TIMEOUT_S)))
                if isinstance(res, tuple) and len(res) == 2:
                    return res  # type: ignore[return-value]
            except Exception:
                return None, None

        # leader fetch
        params = _build_params(
            input_mint=input_mint,
            output_mint=output_mint,
            amount=int(amount),
            slippage_bps=slippage_bps,
            only_direct_routes=bool(only_direct_routes),
            swap_mode=swap_mode,
        )
        payload, status = await _fetch_quote_http(params)
        ok = isinstance(payload, dict)
        await count_quote_outcome(kind="quote", ok=bool(ok), status=status)

        if not ok:
            # negative cache to avoid hammering on dead mints
            await _mem_put(k, None, ts=_now(), neg=True)
            res = (None, None)
        else:
            ts = _now()
            await _mem_put(k, payload, ts=ts, neg=False)
            await _redis_set(k, payload, ts=ts)
            res = (payload, 0.0)

        # resolve followers
        async with _inflight_lock:
            f2 = _inflight.pop(k, None)
        if f2 is not None and not f2.done():
            with contextlib.suppress(Exception):
                f2.set_result(res)

        return res

    except Exception:
        return None, None
    finally:
        # ensure inflight doesn't leak on exceptions
        try:
            async with _inflight_lock:
                fut = _inflight.pop(k, None)  # type: ignore[name-defined]
            if fut is not None and not fut.done():
                fut.set_result((None, None))
        except Exception:
            pass


# Back-compat alias
async def get_cached_or_fetch(out_mint: str, lamports: int, slippage_bps: int) -> dict | None:
    payload, _age = await get_quote_with_age(out_mint, lamports, slippage_bps)
    return payload


async def prewarm_quotes(out_mints: list[str], amount_sol: float, slippage_bps: int) -> None:
    """
    Best-effort prewarm for a list of out_mints (assumes SOL->token).
    Uses concurrency caps + caches results.
    """
    try:
        if not out_mints or float(amount_sol) <= 0:
            return
        lamports = max(1, int(float(amount_sol) * LAMPORTS_PER_SOL))
        targets = [m for m in out_mints if m][: max(1, PREWARM_TOPK)]
        if not targets:
            return

        async def _one(m: str) -> None:
            await get_quote_with_age(m, lamports, int(slippage_bps))

        # bounded gather
        await asyncio.gather(*[_one(m) for m in targets], return_exceptions=True)
    except Exception:
        return


__all__ = [
    "LAMPORTS_PER_SOL",
    "SOL_MINT",
    "get_cached_or_fetch",
    "get_quote_with_age",
    "get_quote_with_age_io",
    "prewarm_quotes",
]
