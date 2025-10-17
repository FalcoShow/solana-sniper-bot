# utils_pkg/dedupe.py
from __future__ import annotations

import os
import time
import threading
from typing import Optional

# ──────────────────────────────────────────────────────────────────────────────
# Redis client (optional)
# ──────────────────────────────────────────────────────────────────────────────
_redis_client = None
try:
    import redis  # redis-py (v4/v5)
    _REDIS_URL = os.getenv("REDIS_URL") or os.getenv("UPSTASH_REDIS_URL") or ""
    if _REDIS_URL:
        # keep timeouts small so we never stall the event loop for long
        _redis_client = redis.from_url(
            _REDIS_URL,
            socket_timeout=float(os.getenv("PF_REDIS_SOCKET_TIMEOUT_S", "1.2")),
            socket_connect_timeout=float(os.getenv("PF_REDIS_CONNECT_TIMEOUT_S", "1.2")),
            retry_on_timeout=True,
        )
except Exception:
    _redis_client = None

# ──────────────────────────────────────────────────────────────────────────────
# Namespacing (helps when multiple environments share a Redis)
# ──────────────────────────────────────────────────────────────────────────────
_PREFIX = (os.getenv("PF_DEDUPE_PREFIX") or "dedupe").strip().lower()
_NAMESPACE = (
    os.getenv("PF_DEDUPE_NS")
    or os.getenv("RENDER_SERVICE_NAME")
    or os.getenv("FLY_APP_NAME")
    or os.getenv("DYNO")
    or os.getenv("HOSTNAME")
    or "default"
).strip().lower()

def _rk(user_key: str) -> str:
    # Redis key
    return f"{_PREFIX}:{_NAMESPACE}:{user_key}"

# ──────────────────────────────────────────────────────────────────────────────
# Local fallback (process-only)
# ──────────────────────────────────────────────────────────────────────────────
# Use monotonic clock to avoid wall-clock jumps.
def _now() -> float:
    return time.monotonic()

_mem_seen: dict[str, float] = {}
_mem_lock = threading.Lock()
_last_cleanup: float = 0.0
_CLEAN_EVERY = 60.0  # seconds

def _cleanup_expired(now: float) -> None:
    global _last_cleanup
    if (now - _last_cleanup) < _CLEAN_EVERY:
        return
    dead = [k for k, exp in _mem_seen.items() if exp <= now]
    for k in dead:
        _mem_seen.pop(k, None)
    _last_cleanup = now

# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────
def claim_once(key: str, ttl: int = 600) -> bool:
    """
    Attempt to claim `key` for `ttl` seconds. Returns True iff the claim succeeds.

    • With Redis configured (REDIS_URL / UPSTASH_REDIS_URL):
        Uses atomic SET key value NX EX ttl.
    • Without Redis (or on Redis error):
        Uses a process-local memo with monotonic time + periodic cleanup.

    Notes:
      - `ttl` is clamped to at least 1s.
      - Namespaced keys avoid cross-env collisions when sharing Redis.
    """
    ttl = int(ttl) if ttl is not None else 600
    if ttl <= 0:
        ttl = 1

    # Try Redis first
    if _redis_client is not None:
        try:
            ok = _redis_client.set(_rk(key), "1", ex=ttl, nx=True)
            return bool(ok)
        except Exception:
            # Fall back to local memo
            pass

    # Local fallback (process-scoped)
    now = _now()
    with _mem_lock:
        _cleanup_expired(now)
        exp = _mem_seen.get(key, 0.0)
        if exp > now:
            return False
        _mem_seen[key] = now + float(ttl)
        return True

__all__ = ["claim_once"]
