"""
utils.py (root)

Compatibility shim for legacy imports like:

    from utils import get_redis_async, log_to_notion, ...

Canonical implementations live in:
    utils_pkg.core_utils

This module:
  - Proxies attribute access to utils_pkg.core_utils (PEP 562).
  - Provides a few legacy helper wrappers used by older scanners.
  - Never lets monitoring/logging helpers crash trading logic.

NOTE:
  Prefer importing from utils_pkg.core_utils directly in new code.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
from collections.abc import Sequence
from typing import Any

from utils_pkg import core_utils as _cu


# ---------------------------
# Proxy exports (PEP 562)
# ---------------------------
def _public_names() -> list[str]:
    # Prefer core_utils.__all__ if defined; else export non-private names.
    names: Sequence[str]
    if hasattr(_cu, "__all__") and isinstance(_cu.__all__, list | tuple):
        names = list(_cu.__all__)
    else:
        names = [n for n in dir(_cu) if n and not n.startswith("_")]
    # Keep stable ordering
    return sorted(set(str(n) for n in names))


__all__ = _public_names()


def __getattr__(name: str) -> Any:
    # Allow "from utils import X" to work even if X is not defined in this file.
    try:
        return getattr(_cu, name)
    except AttributeError as e:
        raise AttributeError(f"module 'utils' has no attribute {name!r}") from e


def __dir__() -> list[str]:
    return sorted(set(list(globals().keys()) + __all__))


# Optionally materialize proxy symbols into globals so debuggers/intellisense show them.
# This does NOT copy private names.
for _n in __all__:
    if _n not in globals():
        with contextlib.suppress(Exception):
            globals()[_n] = getattr(_cu, _n)


# ---------------------------
# Legacy wrappers
# ---------------------------


async def track_profit_notion(
    token: str,
    symbol: str = "",
    balance: float | None = None,
    entry: float | None = None,
    current: float | None = None,
    profit: float | None = None,
    reason: str = "",
    **kwargs: Any,
) -> None:
    """
    Older scanner code calls this.

    Delegates to utils_pkg.core_utils.log_to_notion(...) when available.
    Accepts flexible kw names so callers don't crash.
    """
    # Soft mapping for legacy callers
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

    fn = getattr(_cu, "log_to_notion", None)
    if not callable(fn):
        return

    try:
        await fn(
            token=token,
            symbol=symbol or "",
            balance=float(balance or 0.0),
            entry=float(entry or 0.0),
            current=float(current or 0.0),
            profit=float(profit or 0.0),
            reason=reason or "",
        )
    except Exception:
        # Never let logging break scanners/trading
        return


# ---------------------------
# Redis “seen token” helpers
# ---------------------------
_DEFAULT_PREFIX = "scan:seen:"


async def _get_redis_client() -> Any:
    """
    Get an async redis client from core_utils in a tolerant way.
    Supports either get_redis_async() or get_redis().
    """
    for name in ("get_redis_async", "get_redis"):
        fn = getattr(_cu, name, None)
        if not callable(fn):
            continue
        try:
            res = fn()
            if inspect.isawaitable(res):
                res = await res
            return res
        except Exception:
            return None
    return None


async def redis_cache_token(
    token: str, ttl_seconds: int = 3600, *, prefix: str = _DEFAULT_PREFIX
) -> None:
    """
    Mark a token as seen in Redis with an expiry.
    """
    r = await _get_redis_client()
    if not r:
        return
    try:
        key = f"{prefix}{token}"
        await r.set(key, "1", ex=int(ttl_seconds))
    except Exception:
        return


async def redis_is_cached(token: str, *, prefix: str = _DEFAULT_PREFIX) -> bool:
    """
    Check if token is currently marked as seen.
    """
    r = await _get_redis_client()
    if not r:
        return False
    try:
        return bool(await r.exists(f"{prefix}{token}"))
    except Exception:
        return False


async def redis_cache_token_once(
    token: str, ttl_seconds: int = 3600, *, prefix: str = _DEFAULT_PREFIX
) -> bool:
    """
    Atomically mark token as seen only if it wasn't already.
    Returns True if we set it (first time), False if it already existed.
    """
    r = await _get_redis_client()
    if not r:
        # If Redis is unavailable, allow processing rather than blocking
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


# ---------------------------
# Sync shims (avoid breaking ancient callers)
# ---------------------------
def _run(coro: Any) -> Any:
    """
    Run an async coro from sync context.
    If we're already inside an event loop, we schedule and return a Task.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    else:
        return loop.create_task(coro)


def redis_cache_token_sync(
    token: str, ttl_seconds: int = 3600, *, prefix: str = _DEFAULT_PREFIX
) -> None:
    _run(redis_cache_token(token, ttl_seconds, prefix=prefix))


def redis_is_cached_sync(token: str, *, prefix: str = _DEFAULT_PREFIX) -> bool:
    res = _run(redis_is_cached(token, prefix=prefix))
    # If running in a loop, caller likely expected a bool; fail-closed.
    return False if hasattr(res, "add_done_callback") else bool(res)
