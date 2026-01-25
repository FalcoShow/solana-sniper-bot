# entry_gate.py
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
- try_acquire(mint, ttl=ENTRY_LOCK_TTL_SEC) -> bool
- release(mint) -> None

Recommended new helpers
-----------------------
- acquire(mint, ttl=...) -> token|None
- release_with_token(mint, token) -> bool
- refresh(mint, token, ttl) -> bool
- entry_lock(mint, ttl) async context manager
"""

import asyncio
import os
import uuid
from contextlib import asynccontextmanager
from typing import Any

# ──────────────────────────────────────────────────────────────────────────────
# Logging (root logging_setup.py is canonical)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from logging_setup import get_logger  # type: ignore
except Exception:  # pragma: no cover
    import logging

    def get_logger(name: str):
        return logging.getLogger(name)


log = get_logger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Env helpers
# ──────────────────────────────────────────────────────────────────────────────
def _env_str(key: str, default: str = "") -> str:
    v = os.getenv(key)
    return default if v is None else str(v).strip()


def _env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def _env_int(key: str, default: int) -> int:
    v = os.getenv(key)
    if v is None or str(v).strip() == "":
        return int(default)
    try:
        return int(float(str(v).strip()))
    except Exception:
        return int(default)


def _clamp_int(x: int, lo: int, hi: int) -> int:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
REDIS_URL = _env_str("REDIS_URL", "redis://localhost:6379")

ENTRY_LOCK_ENABLED = _env_bool("ENTRY_LOCK_ENABLED", True)
ENTRY_LOCK_TTL_SEC = _clamp_int(_env_int("ENTRY_LOCK_TTL_SEC", 90), 5, 60 * 60)

# If Redis is down:
# - fail-open (default): allow trades to proceed (risk: duplicate entries)
# - fail-closed: block entry attempts (risk: fewer trades)
ENTRY_LOCK_FAIL_CLOSED = _env_bool("ENTRY_LOCK_FAIL_CLOSED", False)

# Optional namespace so multiple bots (different wallets) don't collide unintentionally.
# If you WANT cross-bot locking, leave namespace stable/shared.
ENTRY_LOCK_NAMESPACE = _env_str("ENTRY_LOCK_NAMESPACE", "") or _env_str("SERVICE_NAME", "sniper")

# Redis client timeouts
REDIS_SOCKET_TIMEOUT_S = float(os.getenv("REDIS_SOCKET_TIMEOUT_S", "3.0") or "3.0")
REDIS_CONNECT_TIMEOUT_S = float(os.getenv("REDIS_CONNECT_TIMEOUT_S", "3.0") or "3.0")
REDIS_HEALTHCHECK_INTERVAL_S = int(float(os.getenv("REDIS_HEALTHCHECK_INTERVAL_S", "15") or "15"))

# ──────────────────────────────────────────────────────────────────────────────
# Redis client singleton (async)
# ──────────────────────────────────────────────────────────────────────────────
_redis_client: Any = None
_redis_init_lock = asyncio.Lock()

# Per-mint last token acquired by this process (for backwards-compatible release(mint))
_tokens: dict[str, str] = {}

# Lua: release only if value matches token (prevents deleting someone else's lock)
_LUA_RELEASE = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
else
  return 0
end
"""

# Lua: refresh TTL only if value matches token
_LUA_REFRESH = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("EXPIRE", KEYS[1], ARGV[2])
else
  return 0
end
"""


def _lock_key(mint: str) -> str:
    mint = (mint or "").strip()
    return f"entry:lock:{ENTRY_LOCK_NAMESPACE}:{mint}"


async def _get_redis() -> Any:
    """
    Returns an async Redis client or None if unavailable/disabled.

    Prefers utils_pkg.core_utils.get_redis_async() (canonical wiring).
    Falls back to redis.asyncio.from_url if installed.
    """
    global _redis_client

    if not ENTRY_LOCK_ENABLED:
        return None

    if _redis_client is not None:
        return _redis_client

    async with _redis_init_lock:
        if _redis_client is not None:
            return _redis_client

        # 1) Canonical app wiring
        try:
            from utils_pkg import core_utils as cu  # type: ignore

            r = await cu.get_redis_async()
            if r is not None:
                _redis_client = r
                return _redis_client
        except Exception as e:
            log.debug("entry_gate: core_utils.get_redis_async unavailable (%s)", type(e).__name__)

        # 2) Fallback to redis.asyncio
        try:
            import redis.asyncio as redis_async  # type: ignore

            _redis_client = redis_async.from_url(
                REDIS_URL,
                decode_responses=True,
                socket_timeout=REDIS_SOCKET_TIMEOUT_S,
                socket_connect_timeout=REDIS_CONNECT_TIMEOUT_S,
                health_check_interval=REDIS_HEALTHCHECK_INTERVAL_S,
                retry_on_timeout=True,
            )
            return _redis_client
        except Exception as e:
            log.warning("entry_gate: redis client unavailable (%s).", type(e).__name__)
            _redis_client = None
            return None


async def close() -> None:
    """
    Best-effort close for fallback redis client.
    Safe to call on shutdown.
    """
    global _redis_client
    r = _redis_client
    _redis_client = None
    if r is None:
        return
    try:
        # redis.asyncio client has .close() and sometimes .connection_pool.disconnect()
        if hasattr(r, "close"):
            await r.close()
        if hasattr(r, "connection_pool") and hasattr(r.connection_pool, "disconnect"):
            await r.connection_pool.disconnect(inuse_connections=True)
    except Exception:
        pass


# ──────────────────────────────────────────────────────────────────────────────
# Lock ops
# ──────────────────────────────────────────────────────────────────────────────
async def acquire(mint: str, ttl: int = ENTRY_LOCK_TTL_SEC) -> str | None:
    """
    Acquire a per-mint lock. Returns token if acquired else None.

    Fail behavior when Redis unavailable:
      - fail-open (default): return a local token to proceed
      - fail-closed: return None
    """
    mint = (mint or "").strip()
    ttl = _clamp_int(int(ttl), 5, 60 * 60)

    r = await _get_redis()
    token = uuid.uuid4().hex

    if r is None:
        if ENTRY_LOCK_FAIL_CLOSED:
            return None
        # fail-open: proceed with a pseudo token (no global lock)
        _tokens[mint] = token
        return token

    key = _lock_key(mint)
    try:
        ok = await r.set(key, token, nx=True, ex=ttl)
        if ok:
            _tokens[mint] = token
            return token
        return None
    except Exception as e:
        if ENTRY_LOCK_FAIL_CLOSED:
            log.warning("entry_gate: acquire failed (fail-closed): %s", type(e).__name__)
            return None
        log.debug("entry_gate: acquire failed (fail-open): %s", type(e).__name__)
        _tokens[mint] = token
        return token


async def try_acquire(mint: str, ttl: int = ENTRY_LOCK_TTL_SEC) -> bool:
    """
    Backwards-compatible helper.
    """
    return (await acquire(mint, ttl=ttl)) is not None


async def release_with_token(mint: str, token: str) -> bool:
    """
    Release only if token matches current lock value.
    Returns True if deleted, False otherwise.
    """
    mint = (mint or "").strip()
    token = (token or "").strip()
    if not token:
        return False

    r = await _get_redis()
    if r is None:
        # fail-open: nothing to release globally
        _tokens.pop(mint, None)
        return True

    key = _lock_key(mint)
    try:
        deleted = await r.eval(_LUA_RELEASE, 1, key, token)
        if int(deleted or 0) > 0:
            _tokens.pop(mint, None)
            return True
        return False
    except Exception as e:
        # In worst case we don't delete to avoid stomping others; drop local token.
        log.debug("entry_gate: release failed: %s", type(e).__name__)
        _tokens.pop(mint, None)
        return False


async def release(mint: str) -> None:
    """
    Backwards-compatible release.
    Uses the last token acquired by THIS process (safe).
    """
    mint = (mint or "").strip()
    token = _tokens.get(mint)
    if not token:
        return
    await release_with_token(mint, token)


async def refresh(mint: str, token: str, ttl: int = ENTRY_LOCK_TTL_SEC) -> bool:
    """
    Extend TTL if token matches.
    Useful if your entry execution can exceed TTL.
    """
    mint = (mint or "").strip()
    token = (token or "").strip()
    ttl = _clamp_int(int(ttl), 5, 60 * 60)
    if not token:
        return False

    r = await _get_redis()
    if r is None:
        return True

    key = _lock_key(mint)
    try:
        ok = await r.eval(_LUA_REFRESH, 1, key, token, ttl)
        return int(ok or 0) > 0
    except Exception:
        return False


async def is_locked(mint: str) -> bool | None:
    """
    Returns:
      - True/False if Redis available
      - None if Redis not available/disabled
    """
    mint = (mint or "").strip()
    r = await _get_redis()
    if r is None:
        return None
    try:
        v = await r.get(_lock_key(mint))
        return v is not None
    except Exception:
        return None


@asynccontextmanager
async def entry_lock(mint: str, ttl: int = ENTRY_LOCK_TTL_SEC) -> tuple[bool, str | None]:
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
        if token:
            await release_with_token(mint, token)


__all__ = [
    "acquire",
    "close",
    "entry_lock",
    "is_locked",
    "refresh",
    "release",
    "release_with_token",
    "try_acquire",
]
