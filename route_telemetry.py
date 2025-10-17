# route_telemetry.py — v1 (DEX histogram counters in Redis)
import os, logging
from typing import Iterable

try:
    import redis.asyncio as aioredis
except Exception:
    aioredis = None

logger = logging.getLogger("route_telemetry")
logging.basicConfig(level=logging.INFO)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

async def _redis():
    if aioredis is None:
        class Dummy:
            async def incrby(self, *_a, **_k): pass
        return Dummy()
    return aioredis.from_url(REDIS_URL, decode_responses=True)

def _norm(label: str) -> str:
    return (label or "OTHER").strip().upper().replace(" ", "_")

async def count_route_used(labels: Iterable[str]) -> None:
    r = await _redis()
    for l in labels:
        try: await r.incrby(f"route:dex_used:{_norm(l)}", 1)
        except Exception: pass

async def count_watchdog_hit(labels: Iterable[str]) -> None:
    r = await _redis()
    for l in labels:
        try: await r.incrby(f"route:dex_watchdog:{_norm(l)}", 1)
        except Exception: pass
