# utils_pkg/dedupe.py
from __future__ import annotations
import os, time
from typing import Optional

# Try Redis first (Render Redis or any managed Redis)
try:
    import redis
    _REDIS_URL = os.getenv("REDIS_URL") or os.getenv("UPSTASH_REDIS_URL") or ""
    _r = redis.from_url(_REDIS_URL, socket_timeout=1.5) if _REDIS_URL else None
except Exception:
    _r = None

# Fallback: process-local (only protects a single instance)
_seen = {}

def claim_once(key: str, ttl: int = 600) -> bool:
    """
    Return True iff this process successfully claims `key` for `ttl` seconds.
    With Redis: uses SET NX EX (atomic). Without: best-effort local memo.
    """
    now = time.time()
    if _r:
        try:
            # NX = only set if not exists, EX = seconds
            ok = _r.set(f"claim:{key}", "1", ex=int(ttl), nx=True)
            return bool(ok)
        except Exception:
            pass  # fall through to local

    # local fallback
    exp = _seen.get(key, 0.0)
    if exp > now:
        return False
    _seen[key] = now + ttl
    return True
