# -*- coding: utf-8 -*-
from __future__ import annotations

"""
entry_gate.py — Entry Idempotency Lock (P2 wrapper/retire)
=========================================================

Architecture lock-in
--------------------
- This module MUST NOT contain scoring / gating logic (no second "entry brain").
- Canonical entry decision remains: utils_pkg.ai_entry_logic.should_enter(...)
- This file is ONLY for *idempotency* (prevent duplicate concurrent entries per mint).

Goals
-----
- Production-safe Redis lock with:
  • fail-soft behavior (configurable fail-open vs fail-closed)
  • owner tokens + safe release (won't delete someone else's lock)
  • short TTL + optional refresh
  • uses utils_pkg.core_utils.get_redis_async() if available (canonical wiring)
  • otherwise falls back to redis.asyncio.from_url

Backwards-compatible API
------------------------
- try_acquire(mint, ttl=ENTRY_LOCK_TTL_SEC) -> bool      (async)
- release(mint) -> None                                 (async)

Recommended helpers
-------------------
- acquire(mint, ttl=...) -> token|None
- release_with_token(mint, token) -> bool
- refresh(mint, token, ttl) -> bool
- entry_lock(mint, ttl) async context manager
"""

import asyncio
import contextlib
import inspect
import os
import threading
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from functools import lru_cache
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from utils_pkg.log_compat import EventLogger
else:
    try:
        from utils_pkg.log_compat import EventLogger  # type: ignore
    except Exception:  # pragma: no cover
        EventLogger = Any  # type: ignore[assignment,misc]


# =============================================================================
# Logging (lazy canonical EventLogger — no local protocol drift)
# =============================================================================

if TYPE_CHECKING:
    from utils_pkg.log_compat import EventLogger
else:
    EventLogger = Any  # type: ignore[assignment,misc]

_LOG: EventLogger | None = None


def _logger() -> EventLogger:
    global _LOG
    if _LOG is None:
        from utils_pkg.logging_setup import get_logger as _get_logger

        _LOG = _get_logger(__name__)
    return _LOG


# =============================================================================
# Env helpers
# =============================================================================

_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _env_str(key: str, default: str = "") -> str:
    v = os.getenv(key)
    return default if v is None else str(v).strip()


def _env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return bool(default)
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return bool(default)


def _env_int(key: str, default: int) -> int:
    v = os.getenv(key)
    if v is None or str(v).strip() == "":
        return int(default)
    with contextlib.suppress(Exception):
        return int(float(str(v).strip()))
    return int(default)


def _env_float(key: str, default: float) -> float:
    v = os.getenv(key)
    if v is None or str(v).strip() == "":
        return float(default)
    with contextlib.suppress(Exception):
        return float(str(v).strip())
    return float(default)


def _clamp_int(x: int, lo: int, hi: int) -> int:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _decode_str(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, (bytes, bytearray, memoryview)):
        with contextlib.suppress(Exception):
            return bytes(v).decode("utf-8", "ignore")
        return None
    return str(v)


async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


# =============================================================================
# Config (lazy; cacheable)
# =============================================================================


@dataclass(frozen=True, slots=True)
class _Cfg:
    redis_url: str
    enabled: bool
    ttl_sec: int
    fail_closed: bool
    namespace: str
    socket_timeout_s: float
    connect_timeout_s: float
    healthcheck_interval_s: int
    ttl_min: int = 5
    ttl_max: int = 60 * 60


@lru_cache(maxsize=1)
def _cfg() -> _Cfg:
    ns = _env_str("ENTRY_LOCK_NAMESPACE", "") or _env_str("SERVICE_NAME", "sniper")
    ttl = _clamp_int(_env_int("ENTRY_LOCK_TTL_SEC", 90), 5, 60 * 60)

    return _Cfg(
        redis_url=_env_str("REDIS_URL", "redis://localhost:6379"),
        enabled=_env_bool("ENTRY_LOCK_ENABLED", True),
        ttl_sec=ttl,
        fail_closed=_env_bool("ENTRY_LOCK_FAIL_CLOSED", False),
        namespace=ns,
        socket_timeout_s=_env_float("REDIS_SOCKET_TIMEOUT_S", 3.0),
        connect_timeout_s=_env_float("REDIS_CONNECT_TIMEOUT_S", 3.0),
        healthcheck_interval_s=_env_int("REDIS_HEALTHCHECK_INTERVAL_S", 15),
    )


def reload_config() -> None:
    _cfg.cache_clear()


# =============================================================================
# Lua scripts (atomic release/refresh)
# =============================================================================

_LUA_RELEASE = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
else
  return 0
end
"""

_LUA_REFRESH = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("EXPIRE", KEYS[1], ARGV[2])
else
  return 0
end
"""


def _norm_mint(mint: str) -> str:
    return (mint or "").strip()


def _lock_key(namespace: str, mint: str) -> str:
    return f"entry:lock:{namespace}:{mint}"


# =============================================================================
# Redis handle wrapper (absorbs signature differences)
# =============================================================================


@dataclass(slots=True)
class _RedisHandle:
    client: Any
    owned: bool  # True only for our redis.asyncio fallback client

    async def get(self, key: str) -> str | None:
        fn = getattr(self.client, "get", None)
        if not callable(fn):
            return None
        try:
            v = await _maybe_await(fn(key))
            return _decode_str(v)
        except Exception:
            return None

    async def set_lock(self, key: str, token: str, ttl: int) -> bool:
        ttl_i = int(ttl)
        c = self.client
        fn = getattr(c, "set", None)
        if not callable(fn):
            return False

        with contextlib.suppress(TypeError):
            res = await _maybe_await(fn(key, token, nx=True, ex=ttl_i))
            return bool(res)

        with contextlib.suppress(TypeError):
            res = await _maybe_await(fn(key, token, ex=ttl_i, nx=True))
            return bool(res)

        with contextlib.suppress(TypeError):
            res = await _maybe_await(fn(key, token, expire=ttl_i, exist="NX"))
            return bool(res)

        try:
            res2 = await _maybe_await(fn(key, token, nx=True))
            if res2:
                exp = getattr(c, "expire", None)
                if callable(exp):
                    with contextlib.suppress(Exception):
                        await _maybe_await(exp(key, ttl_i))
            return bool(res2)
        except Exception:
            return False

    async def eval_int(self, script: str, key: str, *args: Any) -> int | None:
        c = self.client
        fn = getattr(c, "eval", None)
        if not callable(fn):
            return None

        with contextlib.suppress(TypeError):
            out = await _maybe_await(fn(script, 1, key, *args))
            with contextlib.suppress(Exception):
                return int(out or 0)
            return 0

        with contextlib.suppress(TypeError):
            out2 = await _maybe_await(fn(script, keys=[key], args=list(args)))
            with contextlib.suppress(Exception):
                return int(out2 or 0)
            return 0

        return None

    async def close(self) -> None:
        if not self.owned:
            return

        c = self.client
        close_fn = getattr(c, "close", None)
        if callable(close_fn):
            with contextlib.suppress(Exception):
                await _maybe_await(close_fn())

        with contextlib.suppress(Exception):
            pool = getattr(c, "connection_pool", None)
            disc = getattr(pool, "disconnect", None)
            if callable(disc):
                await _maybe_await(disc(inuse_connections=True))


# =============================================================================
# Redis singleton (async)
# =============================================================================

_redis: _RedisHandle | None = None
_redis_lock = asyncio.Lock()

_tokens_lock = threading.Lock()
_tokens: dict[str, str] = {}


async def _get_redis() -> _RedisHandle | None:
    """
    Returns a wrapped async Redis client or None if unavailable.

    Prefers utils_pkg.core_utils.get_redis_async() (canonical wiring).
    Falls back to redis.asyncio.from_url if installed.
    """
    global _redis

    cfg = _cfg()
    if not cfg.enabled:
        return None

    r = _redis
    if r is not None:
        return r

    async with _redis_lock:
        r = _redis
        if r is not None:
            return r

        # 1) Canonical app wiring
        try:
            from utils_pkg import core_utils as cu  # type: ignore

            get_ra = getattr(cu, "get_redis_async", None)
            if callable(get_ra):
                r0 = await _maybe_await(get_ra())
                if r0 is not None:
                    _redis = _RedisHandle(client=r0, owned=False)
                    return _redis
        except Exception as e:
            _logger().debug(
                "entry_gate: core_utils.get_redis_async unavailable (%s)", type(e).__name__
            )

        # 2) Fallback to redis.asyncio
        try:
            import redis.asyncio as redis_async  # type: ignore

            r1 = redis_async.from_url(
                cfg.redis_url,
                decode_responses=True,
                socket_timeout=cfg.socket_timeout_s,
                socket_connect_timeout=cfg.connect_timeout_s,
                health_check_interval=cfg.healthcheck_interval_s,
                retry_on_timeout=True,
            )
            _redis = _RedisHandle(client=r1, owned=True)
            return _redis
        except Exception as e:
            _logger().warning("entry_gate: redis client unavailable (%s).", type(e).__name__)
            _redis = None
            return None


async def close() -> None:
    """Best-effort close for fallback redis client. Safe to call on shutdown."""
    global _redis
    r = _redis
    _redis = None
    if r is None:
        return
    await r.close()


# =============================================================================
# Lock ops
# =============================================================================


async def acquire(mint: str, ttl: int | None = None) -> str | None:
    """
    Acquire a per-mint lock. Returns token if acquired else None.

    Fail behavior when Redis unavailable:
      - fail-open (default): return a local token to proceed
      - fail-closed: return None

    If ENTRY_LOCK_ENABLED is false, behaves as "lock disabled" (always returns a token).
    """
    cfg = _cfg()
    mint_n = _norm_mint(mint)

    token = uuid.uuid4().hex
    if not mint_n:
        return token

    ttl_i = _clamp_int(int(ttl if ttl is not None else cfg.ttl_sec), cfg.ttl_min, cfg.ttl_max)

    if not cfg.enabled:
        with _tokens_lock:
            _tokens[mint_n] = token
        return token

    r = await _get_redis()
    if r is None:
        if cfg.fail_closed:
            return None
        with _tokens_lock:
            _tokens[mint_n] = token
        return token

    key = _lock_key(cfg.namespace, mint_n)
    ok = await r.set_lock(key, token, ttl_i)
    if ok:
        with _tokens_lock:
            _tokens[mint_n] = token
        return token

    return None


async def try_acquire(mint: str, ttl: int | None = None) -> bool:
    """Backwards-compatible helper."""
    return (await acquire(mint, ttl=ttl)) is not None


async def release_with_token(mint: str, token: str) -> bool:
    """
    Release only if token matches current lock value.
    Returns True if deleted (or lock disabled / redis unavailable), False otherwise.
    """
    cfg = _cfg()
    mint_n = _norm_mint(mint)
    token_n = (token or "").strip()
    if not mint_n or not token_n:
        return False

    if not cfg.enabled:
        with _tokens_lock:
            _tokens.pop(mint_n, None)
        return True

    r = await _get_redis()
    if r is None:
        with _tokens_lock:
            _tokens.pop(mint_n, None)
        return True

    key = _lock_key(cfg.namespace, mint_n)
    deleted = await r.eval_int(_LUA_RELEASE, key, token_n)
    if deleted is None:
        with _tokens_lock:
            _tokens.pop(mint_n, None)
        return False

    ok = int(deleted) > 0
    with _tokens_lock:
        _tokens.pop(mint_n, None)
    return ok


async def release(mint: str) -> None:
    """Backwards-compatible release: uses the last token acquired by THIS process (safe)."""
    mint_n = _norm_mint(mint)
    if not mint_n:
        return

    with _tokens_lock:
        token = _tokens.get(mint_n)

    if not token:
        return

    await release_with_token(mint_n, token)


async def refresh(mint: str, token: str, ttl: int | None = None) -> bool:
    """Extend TTL if token matches."""
    cfg = _cfg()
    mint_n = _norm_mint(mint)
    token_n = (token or "").strip()
    if not mint_n or not token_n:
        return False

    ttl_i = _clamp_int(int(ttl if ttl is not None else cfg.ttl_sec), cfg.ttl_min, cfg.ttl_max)

    if not cfg.enabled:
        return True

    r = await _get_redis()
    if r is None:
        return not cfg.fail_closed

    key = _lock_key(cfg.namespace, mint_n)
    out = await r.eval_int(_LUA_REFRESH, key, token_n, ttl_i)
    if out is None:
        return False
    return int(out) > 0


async def is_locked(mint: str) -> bool | None:
    """
    Returns:
      - True/False if Redis available
      - None if Redis not available/disabled
    """
    cfg = _cfg()
    mint_n = _norm_mint(mint)
    if not mint_n or not cfg.enabled:
        return None

    r = await _get_redis()
    if r is None:
        return None

    with contextlib.suppress(Exception):
        v = await r.get(_lock_key(cfg.namespace, mint_n))
        return v is not None
    return None


@asynccontextmanager
async def entry_lock(mint: str, ttl: int | None = None) -> AsyncIterator[tuple[bool, str | None]]:
    """
    Async context manager:

        async with entry_lock(mint) as (ok, token):
            if not ok: return
            ... execute entry ...
    """
    token = await acquire(mint, ttl=ttl)
    ok = token is not None
    try:
        yield ok, token
    finally:
        if token is not None:
            with contextlib.suppress(Exception):
                await release_with_token(mint, token)


__all__ = [
    "acquire",
    "close",
    "entry_lock",
    "is_locked",
    "refresh",
    "release",
    "release_with_token",
    "reload_config",
    "try_acquire",
]
