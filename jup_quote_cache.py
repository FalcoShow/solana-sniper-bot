# jup_quote_cache.py — v2 (adds quote age reporting)
import os, json, asyncio, time, logging
from typing import Dict, List, Optional, Tuple

import aiohttp
try:
    import redis.asyncio as aioredis
except Exception:
    aioredis = None  # optional

logger = logging.getLogger("jup_quote_cache")
logging.basicConfig(level=logging.INFO)

JUP_API   = os.getenv("JUPITER_SWAP_API", "https://quote-api.jup.ag/v6")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

QUOTE_TTL_SEC = int(os.getenv("QUOTE_PREWARM_TTL_SEC", "30"))
PREWARM_TOPK  = int(os.getenv("QUOTE_PREWARM_TOPK", "8"))
SOL_MINT      = "So11111111111111111111111111111111111111112"
LAMPORTS      = 1_000_000_000

# memory cache: key -> (cached_at_epoch_s, payload_dict)
_mem_cache: Dict[str, Tuple[float, dict]] = {}

def _key(out_mint: str, lamports: int, slippage_bps: int) -> str:
    return f"jup:quote:{SOL_MINT}:{out_mint}:{lamports}:{slippage_bps}"

async def _redis():
    if aioredis is None: return None
    return aioredis.from_url(REDIS_URL, decode_responses=True)

async def _fetch_quote(session: aiohttp.ClientSession, out_mint: str, lamports: int, slippage_bps: int) -> Optional[dict]:
    params = {
        "inputMint": SOL_MINT,
        "outputMint": out_mint,
        "amount": lamports,
        "slippageBps": max(1, slippage_bps),
        "onlyDirectRoutes": "false",
    }
    try:
        async with session.get(f"{JUP_API}/quote", params=params, timeout=10) as r:
            if r.status != 200:
                return None
            return await r.json()
    except Exception:
        return None

async def get_quote_with_age(out_mint: str, lamports: int, slippage_bps: int) -> Tuple[Optional[dict], Optional[float]]:
    """
    Returns (payload, age_sec). If no payload: (None, None).
    Uses memory -> Redis -> network with TTL behavior.
    """
    k   = _key(out_mint, lamports, slippage_bps)
    now = time.time()

    tup = _mem_cache.get(k)
    if tup and (now - tup[0]) <= QUOTE_TTL_SEC:
        return tup[1], now - tup[0]

    r = await _redis()
    if r:
        try:
            s = await r.get(k)
            if s:
                obj = json.loads(s)  # {"cached_at": ts, "payload": {...}}
                ts = float(obj.get("cached_at", now))
                payload = obj.get("payload")
                if isinstance(payload, dict) and (now - ts) <= QUOTE_TTL_SEC:
                    _mem_cache[k] = (ts, payload)
                    return payload, now - ts
        except Exception:
            pass

    async with aiohttp.ClientSession() as session:
        payload = await _fetch_quote(session, out_mint, lamports, slippage_bps)
        if not payload:
            return None, None
        _mem_cache[k] = (now, payload)
        if r:
            try:
                await r.set(k, json.dumps({"cached_at": now, "payload": payload}), ex=QUOTE_TTL_SEC)
            except Exception:
                pass
        return payload, 0.0

# Back-compat
async def get_cached_or_fetch(out_mint: str, lamports: int, slippage_bps: int) -> Optional[dict]:
    payload, _ = await get_quote_with_age(out_mint, lamports, slippage_bps)
    return payload

async def prewarm_quotes(out_mints: List[str], amount_sol: float, slippage_bps: int) -> None:
    if amount_sol <= 0 or not out_mints: return
    lamports = max(1, int(amount_sol * LAMPORTS))
    targets  = out_mints[:PREWARM_TOPK]
    async with aiohttp.ClientSession() as session:
        tasks = [_fetch_quote(session, m, lamports, slippage_bps) for m in targets]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    now = time.time()
    r = await _redis()
    for m, payload in zip(targets, results):
        if not isinstance(payload, dict): continue
        k = _key(m, lamports, slippage_bps)
        _mem_cache[k] = (now, payload)
        if r:
            try:
                await r.set(k, json.dumps({"cached_at": now, "payload": payload}), ex=QUOTE_TTL_SEC)
            except Exception:
                pass
