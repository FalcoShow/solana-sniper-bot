# entry_gate.py
import os, asyncio, time
try:
    import redis.asyncio as aioredis
except Exception:
    aioredis = None

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
ENTRY_LOCK_TTL_SEC = int(os.getenv("ENTRY_LOCK_TTL_SEC", "90"))

async def _redis():
    if aioredis is None:
        return None
    return aioredis.from_url(REDIS_URL, decode_responses=True)

def _k(mint: str) -> str:
    return f"entry:lock:{mint}"

async def try_acquire(mint: str, ttl: int = ENTRY_LOCK_TTL_SEC) -> bool:
    r = await _redis()
    if not r:
        return True  # best-effort if no redis
    # SET key NX EX ttl
    ok = await r.set(_k(mint), "1", nx=True, ex=ttl)
    return bool(ok)

async def release(mint: str):
    r = await _redis()
    if not r:
        return
    await r.delete(_k(mint))
