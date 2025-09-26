# utils.py
# Compatibility shim so older imports like `from utils import ...` still work.
# We re-export public symbols from utils_pkg.core_utils and provide wrappers
# for legacy helper names used by the scanners.

from __future__ import annotations
import asyncio
from typing import Any, Optional

# Re-export everything devs expect to exist on `utils`
from utils_pkg.core_utils import *  # noqa: F401,F403

# Pull specific helpers we'll wrap/delegate to
from utils_pkg.core_utils import (
    log_to_notion as _log_to_notion,
    get_redis as _get_redis,
)

# ------------------------------------------------------------------------------
# Back-compat: Notion profit logger
# ------------------------------------------------------------------------------
async def track_profit_notion(
    token: str,
    symbol: str = "",
    balance: Optional[float] = None,
    entry: Optional[float] = None,
    current: Optional[float] = None,
    profit: Optional[float] = None,
    reason: str = "",
    **kwargs: Any,
) -> None:
    """
    Older scanners call this. We delegate to core_utils.log_to_notion().
    Accepts flexible kw names so callers don't crash.
    """
    if balance is None:
        try:
            balance = float(kwargs.get("amount") or kwargs.get("size") or 0.0)
        except Exception:
            balance = 0.0
    if profit is None:
        try:
            profit = float(kwargs.get("pnl") or kwargs.get("profit_sol") or 0.0)
        except Exception:
            profit = 0.0
    if not reason:
        reason = str(kwargs.get("note") or kwargs.get("reason") or "")

    try:
        await _log_to_notion(
            token=token,
            symbol=symbol,
            balance=float(balance or 0.0),
            entry=entry if entry is not None else 0.0,
            current=current if current is not None else 0.0,
            profit=profit if profit is not None else 0.0,
            reason=reason,
        )
    except Exception:
        # Never let logging break the scanners
        return

# ------------------------------------------------------------------------------
# Back-compat: Redis token-cache helpers
# Some scanner code uses `redis_cache_token(...)` to mark a token as seen so it
# isn’t reprocessed for a while. We provide tolerant wrappers here.
# ------------------------------------------------------------------------------

_DEFAULT_PREFIX = "scan:seen:"  # namespaced to avoid collisions

async def redis_cache_token(token: str, ttl_seconds: int = 3600, *, prefix: str = _DEFAULT_PREFIX) -> None:
    """
    Mark a token as seen in Redis with an expiry.
    """
    r = _get_redis()
    if not r:
        return
    try:
        key = f"{prefix}{token}"
        # set with TTL (overwrite is fine for this use)
        await r.set(key, "1", ex=int(ttl_seconds))
    except Exception:
        # fail-closed: just don't cache if Redis has an issue
        return

async def redis_is_cached(token: str, *, prefix: str = _DEFAULT_PREFIX) -> bool:
    """
    Check if token is currently marked as seen.
    """
    r = _get_redis()
    if not r:
        return False
    try:
        return bool(await r.exists(f"{prefix}{token}"))
    except Exception:
        return False

async def redis_cache_token_once(token: str, ttl_seconds: int = 3600, *, prefix: str = _DEFAULT_PREFIX) -> bool:
    """
    Atomically mark token as seen only if it wasn't already. Returns True if we
    *set* it (i.e., first time), False if it was already present.
    """
    r = _get_redis()
    if not r:
        # If Redis is unavailable, allow processing rather than block
        return True
    key = f"{prefix}{token}"
    try:
        ok = await r.setnx(key, "1")
        if ok:
            await r.expire(key, int(ttl_seconds))
        return bool(ok)
    except Exception:
        # If Redis hiccups, allow processing
        return True

# Optional sync shims in case any old code calls without `await`
def _run(coro):
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    else:
        return loop.create_task(coro)

def redis_cache_token_sync(token: str, ttl_seconds: int = 3600, *, prefix: str = _DEFAULT_PREFIX) -> None:
    _run(redis_cache_token(token, ttl_seconds, prefix=prefix))

def redis_is_cached_sync(token: str, *, prefix: str = _DEFAULT_PREFIX) -> bool:
    res = _run(redis_is_cached(token, prefix=prefix))
    # if running in a loop, the caller probably didn't expect a Task object
    # so we default False here; leave the async version as the primary one.
    return False if hasattr(res, "add_done_callback") else bool(res)
