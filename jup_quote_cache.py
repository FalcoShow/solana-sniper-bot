from __future__ import annotations

"""
jup_quote_cache.py (ROOT) — V3+++ (God-Tier Overhaul)
=====================================================

Role
----
A small, safe, bounded, best-effort quote cache intended for:
- route probing (guards/watchdogs)
- optional pre-warming of top candidates
- minimizing hammering Jupiter when scanners are hot

Design rules
------------
- Public APIs never raise (fail-soft), except asyncio.CancelledError propagates
- No network at import time (all IO is lazy)
- Memory cache + optional Redis cache + network fetch fallback
- Stampede protection (per-key in-flight dedupe)
- Negative caching (short TTL) on repeated failures
- Backwards compatible with older call sites:
    get_quote_with_age(out_mint, lamports, slippage_bps)  # assumes SOL -> out_mint

Important
---------
Your canonical router is utils_pkg/router.py. This cache is not a second router.
It only fetches /quote payloads and caches them.

Env
---
Quote endpoint resolution (any one):
  - JUP_QUOTE_API              (explicit full /quote endpoint, e.g. https://api.jup.ag/quote)
  - JUP_BASE_URL               (base URL, e.g. https://api.jup.ag)                -> calls /quote
  - JUPITER_SWAP_API           (legacy base, e.g. https://quote-api.jup.ag/v6)     -> calls /quote

Cache:
  - QUOTE_CACHE_ENABLED=1
  - QUOTE_TTL_SEC=30
  - QUOTE_NEG_TTL_SEC=6
  - QUOTE_MEM_MAX_KEYS=4000
  - QUOTE_NET_TIMEOUT_S=8
  - QUOTE_NET_MAX_PARALLEL=12
  - QUOTE_PREWARM_TOPK=8
  - QUOTE_PREWARM_TTL_SEC      (legacy alias for QUOTE_TTL_SEC)
  - QUOTE_MAX_JSON_BYTES=1200000   (hard cap for Redis stored JSON)

Redis:
  - REDIS_URL=redis://...
  - QUOTE_REDIS_ENABLED=1   (auto-disabled if Redis not available)

Telemetry (optional):
  - ROUTE_TELEM_ENABLED=1   (handled by route_telemetry itself)

Logging contract (Phase A)
--------------------------
This module uses a shim-safe EventLogger contract:
- always has .debug/.info/.warning/.error/.exception/.log
- always has .handlers for typing compatibility
- never monkey-patches logger methods
"""

import asyncio
import contextlib
import json
import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Protocol, TypeAlias, cast

Json: TypeAlias = dict[str, Any]

from collections.abc import Coroutine

# ──────────────────────────────────────────────────────────────────────────────
# Phase A — Logging contract (EventLogger + safe get_logger)
# ──────────────────────────────────────────────────────────────────────────────


class EventLogger(Protocol):
    handlers: list[Any]

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def info(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def error(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def log(self, level: int, msg: str, *args: Any, **kwargs: Any) -> None: ...


class _EventCapable(EventLogger, Protocol):
    def event(self, name: str, **fields: Any) -> None: ...


class _LoggerProxy:
    """
    Adapter that guarantees `.handlers` exists for typing compatibility,
    while forwarding standard logging methods to the underlying logger.
    """

    def __init__(self, base: Any):
        self._base = base
        self.handlers: list[Any] = []
        with contextlib.suppress(Exception):
            h = getattr(base, "handlers", None)
            if isinstance(h, list):
                self.handlers = h

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        cast(Any, self._base).debug(msg, *args, **kwargs)

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        cast(Any, self._base).info(msg, *args, **kwargs)

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        fn = getattr(self._base, "warning", None) or getattr(self._base, "warn", None)
        if callable(fn):
            fn(msg, *args, **kwargs)
            return
        self.info(msg, *args, **kwargs)

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        cast(Any, self._base).error(msg, *args, **kwargs)

    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None:
        fn = getattr(self._base, "exception", None)
        if callable(fn):
            fn(msg, *args, **kwargs)
            return
        self.error(msg, *args, **kwargs)

    def log(self, level: int, msg: str, *args: Any, **kwargs: Any) -> None:
        cast(Any, self._base).log(level, msg, *args, **kwargs)

    def event(self, name: str, **fields: Any) -> None:
        fn = getattr(self._base, "event", None)
        if callable(fn):
            fn(name, **fields)
            return
        self.info("%s %s", name, fields)


def get_logger(name: str | None = None) -> EventLogger:
    """
    Always returns an EventLogger-compatible object.

    Priority:
      1) utils_pkg.log_compat.get_event_logger / get_logger
      2) logging_setup.get_logger
      3) stdlib logging.getLogger (basicConfig if needed)
    """
    nm = (name or "jup_quote_cache").strip() or "jup_quote_cache"

    with contextlib.suppress(Exception):
        from utils_pkg.log_compat import get_event_logger  # type: ignore

        return cast(EventLogger, _LoggerProxy(get_event_logger(nm)))

    with contextlib.suppress(Exception):
        from utils_pkg.log_compat import get_logger as gl  # type: ignore

        return cast(EventLogger, _LoggerProxy(gl(nm)))

    with contextlib.suppress(Exception):
        from logging_setup import get_logger as gl2  # type: ignore

        return cast(EventLogger, _LoggerProxy(gl2(nm)))

    import logging

    lg = logging.getLogger(nm)
    if not lg.handlers:
        level = os.getenv("LOG_LEVEL", "INFO").strip().upper() or "INFO"
        logging.basicConfig(
            level=getattr(logging, level, logging.INFO),
            format="[quote_cache] %(asctime)s %(levelname)s %(name)s %(message)s",
        )
    return cast(EventLogger, _LoggerProxy(lg))


log: EventLogger = get_logger("jup_quote_cache")


def _event(name: str, **fields: Any) -> None:
    """Best-effort structured event. Never raises."""
    try:
        fn = getattr(log, "event", None)
        if callable(fn):
            cast(_EventCapable, log).event(str(name), **fields)
        else:
            log.info("%s %s", str(name), fields)
    except Exception:
        return


# ──────────────────────────────────────────────────────────────────────────────
# Env helpers
# ──────────────────────────────────────────────────────────────────────────────
_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}


def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    return (v if v is not None else d).strip()


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

_net_sema = asyncio.Semaphore(max(1, int(QUOTE_NET_MAX_PARALLEL)))


def _now() -> float:
    return time.time()


def _is_fresh(ts: float, ttl_sec: int) -> bool:
    ttl = float(max(0, int(ttl_sec)))
    return (_now() - float(ts)) <= ttl


# ──────────────────────────────────────────────────────────────────────────────
# Telemetry (optional)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from route_telemetry import count_quote_outcome  # type: ignore[import-not-found]
except Exception:  # pragma: no cover

    async def count_quote_outcome(kind: str, ok: bool, status: int | None = None) -> None:
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

    legacy = _envs("JUPITER_SWAP_API", "https://quote-api.jup.ag/v6").rstrip("/")
    if legacy.endswith("/quote"):
        return legacy
    return legacy + "/quote"


# ──────────────────────────────────────────────────────────────────────────────
# Memory cache + inflight dedupe
# ──────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class _MemEntry:
    ts: float
    payload: Json | None  # None means "negative cached"
    neg: bool = False


_mem: dict[str, _MemEntry] = {}
_mem_lock = asyncio.Lock()

InflightResult: TypeAlias = tuple[Json | None, float | None]
InflightFactory: TypeAlias = Callable[[], Coroutine[Any, Any, InflightResult]]
_inflight: dict[str, asyncio.Task[InflightResult]] = {}
_inflight_lock = asyncio.Lock()


def _mk_key(
    input_mint: str,
    output_mint: str,
    amount: int,
    slippage_bps: int,
    swap_mode: str,
    only_direct: bool,
    as_legacy: bool,
) -> str:
    return (
        "jup:quote:"
        + ("legacy:" if as_legacy else "v6:")
        + f"{input_mint}:{output_mint}:{int(amount)}:{int(slippage_bps)}:"
        + f"{swap_mode}:{'D1' if only_direct else 'D0'}"
    )


async def _mem_get_entry(k: str) -> tuple[bool, _MemEntry | None, float | None]:
    """
    Returns (hit, entry, age_sec).
    hit=True means 'cache decision exists' (including negative).
    """
    if not QUOTE_CACHE_ENABLED:
        return False, None, None

    async with _mem_lock:
        e = _mem.get(k)
        if e is None:
            return False, None, None

        ttl = QUOTE_NEG_TTL_SEC if e.neg else QUOTE_TTL_SEC
        if not _is_fresh(e.ts, ttl):
            _mem.pop(k, None)
            return False, None, None

        return True, e, (_now() - float(e.ts))


async def _mem_put(
    k: str, payload: Json | None, *, ts: float | None = None, neg: bool = False
) -> None:
    if not QUOTE_CACHE_ENABLED:
        return

    async with _mem_lock:
        limit = max(256, int(QUOTE_MEM_MAX_KEYS))
        if len(_mem) >= limit:
            # Evict ~10% oldest entries
            try:
                items = sorted(_mem.items(), key=lambda kv: kv[1].ts)
                n = max(1, len(items) // 10)
                for kk, _ in items[:n]:
                    _mem.pop(kk, None)
            except Exception:
                _mem.clear()

        stored = dict(payload) if isinstance(payload, dict) else None
        _mem[k] = _MemEntry(
            ts=float(ts if ts is not None else _now()), payload=stored, neg=bool(neg)
        )


# ──────────────────────────────────────────────────────────────────────────────
# Redis cache (best-effort)
# ──────────────────────────────────────────────────────────────────────────────
class _NoRedis:
    async def get(self, *_a: Any, **_k: Any) -> str | bytes | None:
        return None

    async def set(self, *_a: Any, **_k: Any) -> None:
        return None


_redis_client: Any = None
_redis_lock = asyncio.Lock()


async def _redis() -> Any:
    """
    Lazy Redis acquisition. Never raises (except CancelledError).
    """
    global _redis_client

    if not QUOTE_REDIS_ENABLED:
        return _NoRedis()

    client = _redis_client
    if client is not None:
        return client

    async with _redis_lock:
        client = _redis_client
        if client is not None:
            return client

        # Prefer shared redis from utils_pkg.core_utils
        with contextlib.suppress(Exception):
            from utils_pkg import core_utils as cu  # type: ignore[import-not-found]

            r = await cu.get_redis_async()  # type: ignore[attr-defined]
            _redis_client = r
            return _redis_client

        # Fallback: direct redis asyncio client
        try:
            import redis.asyncio as aioredis  # type: ignore[import-not-found]

            _redis_client = aioredis.from_url(REDIS_URL, decode_responses=False)
            return _redis_client
        except Exception:
            _redis_client = _NoRedis()
            return _redis_client


def _loads_json_maybe_bytes(s: str | bytes) -> Any:
    if isinstance(s, bytes):
        try:
            s = s.decode("utf-8", errors="ignore")
        except Exception:
            return None
    try:
        return json.loads(s)
    except Exception:
        return None


async def _redis_get(k: str) -> tuple[Json | None, float | None]:
    if not QUOTE_CACHE_ENABLED or not QUOTE_REDIS_ENABLED:
        return None, None

    r = await _redis()
    try:
        raw = await r.get(k)
        if not raw:
            return None, None

        obj = _loads_json_maybe_bytes(raw)
        if not isinstance(obj, dict):
            return None, None

        ts = float(obj.get("cached_at", obj.get("ts", 0.0)) or 0.0)
        payload = obj.get("payload")
        if not isinstance(payload, dict):
            return None, None
        if not _is_fresh(ts, QUOTE_TTL_SEC):
            return None, None
        return cast(Json, payload), (_now() - ts)
    except asyncio.CancelledError:
        raise
    except Exception:
        return None, None


async def _redis_set(k: str, payload: Json, *, ts: float | None = None) -> None:
    if not QUOTE_CACHE_ENABLED or not QUOTE_REDIS_ENABLED:
        return

    r = await _redis()
    try:
        body = json.dumps(
            {"cached_at": float(ts if ts is not None else _now()), "payload": payload}
        )
        if len(body.encode("utf-8")) > int(_MAX_JSON_BYTES):
            return
        await r.set(k, body.encode("utf-8"), ex=int(max(1, int(QUOTE_TTL_SEC))))
    except asyncio.CancelledError:
        raise
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
    amt = int(max(1, int(amount)))
    slip = int(max(1, int(slippage_bps)))
    return {
        "inputMint": str(input_mint),
        "outputMint": str(output_mint),
        "amount": str(amt),
        "slippageBps": str(slip),
        "onlyDirectRoutes": "true" if only_direct_routes else "false",
        "swapMode": str(swap_mode or "ExactIn"),
    }


async def _maybe_shared_session() -> Any | None:
    """
    Best-effort shared aiohttp session. No import-time IO.
    """
    try:
        from utils_pkg.honeypot import _get_session  # type: ignore[import-not-found]

        sess = await _get_session()  # type: ignore[misc]
        return sess
    except asyncio.CancelledError:
        raise
    except Exception:
        return None


async def _fetch_quote_http(params: dict[str, str]) -> tuple[Json | None, int | None]:
    """
    Returns (payload, http_status). Never raises (except CancelledError).
    """
    url = _quote_url()

    async with _net_sema:
        sess = await _maybe_shared_session()
        try:
            import aiohttp  # type: ignore[import-not-found]

            timeout = aiohttp.ClientTimeout(total=float(max(1.0, float(QUOTE_NET_TIMEOUT_S))))

            if sess is not None:
                async with sess.get(url, params=params, timeout=timeout) as resp:
                    st = int(getattr(resp, "status", 0) or 0)
                    if st != 200:
                        return None, st
                    js = await resp.json(content_type=None)
                    return js if isinstance(js, dict) else None, st

            async with aiohttp.ClientSession(timeout=timeout) as s2:
                async with s2.get(url, params=params) as resp:
                    st = int(getattr(resp, "status", 0) or 0)
                    if st != 200:
                        return None, st
                    js = await resp.json(content_type=None)
                    return js if isinstance(js, dict) else None, st

        except asyncio.CancelledError:
            raise
        except Exception:
            return None, None


# ──────────────────────────────────────────────────────────────────────────────
# Inflight worker
# ──────────────────────────────────────────────────────────────────────────────
async def _fetch_and_cache(
    *,
    key: str,
    input_mint: str,
    output_mint: str,
    amount: int,
    slippage_bps: int,
    only_direct_routes: bool,
    swap_mode: str,
) -> InflightResult:
    """
    Leader-only worker. Must never raise (except CancelledError).
    Returns (payload, age_sec).
    """
    try:
        params = _build_params(
            input_mint=input_mint,
            output_mint=output_mint,
            amount=amount,
            slippage_bps=slippage_bps,
            only_direct_routes=only_direct_routes,
            swap_mode=swap_mode,
        )

        payload, status = await _fetch_quote_http(params)
        ok = isinstance(payload, dict)
        await count_quote_outcome(kind="quote", ok=ok, status=status)

        if not isinstance(payload, dict):
            await _mem_put(key, None, ts=_now(), neg=True)
            return None, None

        ts = _now()
        await _mem_put(key, payload, ts=ts, neg=False)
        await _redis_set(key, payload, ts=ts)
        return payload, 0.0

    except asyncio.CancelledError:
        raise
    except Exception:
        await _mem_put(key, None, ts=_now(), neg=True)
        return None, None
    finally:
        # Cleanup inflight map if it still points to this task.
        try:
            cur = asyncio.current_task()
            if cur is not None:
                async with _inflight_lock:
                    if _inflight.get(key) is cur:
                        _inflight.pop(key, None)
        except Exception:
            pass


async def _get_inflight_task(key: str, factory: InflightFactory) -> asyncio.Task[InflightResult]:
    async with _inflight_lock:
        t = _inflight.get(key)
        if t is not None and not t.done():
            return t

        t2: asyncio.Task[InflightResult] = asyncio.create_task(factory())
        _inflight[key] = t2
        return t2


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────
async def get_quote_with_age(
    out_mint: str, lamports: int, slippage_bps: int
) -> tuple[Json | None, float | None]:
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
) -> tuple[Json | None, float | None]:
    """
    Canonical function:
      Returns (payload, age_sec). If no payload: (None, None).

    Cache order:
      memory -> redis -> network
    With per-key inflight dedupe + negative caching.
    """
    try:
        im = (input_mint or "").strip()
        om = (output_mint or "").strip()
        amt = int(amount)

        if not im or not om or amt <= 0:
            return None, None

        slip = int(max(1, int(slippage_bps)))
        mode = str(swap_mode or "ExactIn")

        key = _mk_key(
            input_mint=im,
            output_mint=om,
            amount=amt,
            slippage_bps=slip,
            swap_mode=mode,
            only_direct=bool(only_direct_routes),
            as_legacy=bool(as_legacy),
        )

        # 1) memory (including negative short-circuit)
        hit, entry, age = await _mem_get_entry(key)
        if hit and entry is not None:
            if entry.neg:
                return None, None
            if isinstance(entry.payload, dict):
                return entry.payload, age

        # 2) redis
        payload, rage = await _redis_get(key)
        if isinstance(payload, dict):
            ts = _now() - float(rage or 0.0)
            await _mem_put(key, payload, ts=ts, neg=False)
            return payload, rage

        # 3) inflight dedupe -> network leader worker
        async def _factory() -> InflightResult:
            return await _fetch_and_cache(
                key=key,
                input_mint=im,
                output_mint=om,
                amount=amt,
                slippage_bps=slip,
                only_direct_routes=bool(only_direct_routes),
                swap_mode=mode,
            )

        task = await _get_inflight_task(key, _factory)

        try:
            res = await asyncio.wait_for(task, timeout=float(max(1.0, float(QUOTE_NET_TIMEOUT_S))))
            return res[0], res[1]
        except asyncio.CancelledError:
            raise
        except Exception:
            return None, None

    except asyncio.CancelledError:
        raise
    except Exception:
        return None, None


async def get_cached_or_fetch(out_mint: str, lamports: int, slippage_bps: int) -> Json | None:
    """
    Back-compat alias.
    """
    payload, _age = await get_quote_with_age(out_mint, lamports, slippage_bps)
    return payload


async def prewarm_quotes(out_mints: list[str], amount_sol: float, slippage_bps: int) -> None:
    """
    Best-effort prewarm for a list of out_mints (assumes SOL->token).
    Uses concurrency caps + caches results.
    """
    try:
        if not out_mints or float(amount_sol) <= 0.0:
            return

        lamports = max(1, int(float(amount_sol) * float(LAMPORTS_PER_SOL)))
        targets = [m for m in out_mints if isinstance(m, str) and m.strip()][
            : max(1, int(PREWARM_TOPK))
        ]
        if not targets:
            return

        _event("QUOTE_PREWARM_START", n=len(targets), lamports=lamports, slip=int(slippage_bps))

        async def _one(m: str) -> None:
            with contextlib.suppress(Exception):
                await get_quote_with_age(m.strip(), lamports, int(slippage_bps))

        await asyncio.gather(*[_one(m) for m in targets], return_exceptions=True)
        _event("QUOTE_PREWARM_DONE", n=len(targets))
    except asyncio.CancelledError:
        raise
    except Exception:
        return


__all__ = [
    "EventLogger",
    "LAMPORTS_PER_SOL",
    "SOL_MINT",
    "get_cached_or_fetch",
    "get_quote_with_age",
    "get_quote_with_age_io",
    "prewarm_quotes",
]
