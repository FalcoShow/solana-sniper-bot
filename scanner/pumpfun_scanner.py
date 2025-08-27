# pumpfun_scanner.py
import aiohttp
import logging
import json
import time
from pathlib import Path
from typing import AsyncGenerator, Dict, Optional, Set

import redis.asyncio as aioredis

from config import (
    PUMPFUN_API_URL,
    SCANNER_MIN_LIQ_USD,
    SCANNER_MIN_FDV_USD,
    SCANNER_MIN_FRAUD_SCORE,
    SCANNER_ALERTS_ENABLED,
)
from utils import (
    check_rug_pull_risk,   # honeypot/rug risk boolean (or high-level check)
    fraud_score_ai,        # returns numeric score 0..100
    is_whale_wallet,       # returns True if whales present around mint
    send_telegram,         # async telegram sender
    track_profit_notion,   # we reuse to log “discovery” rows (profit=0 at discovery)
)

logger = logging.getLogger("scanner.pumpfun")

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
SNIPED_MINTS_FILE = DATA_DIR / "sniped_mints_pumpfun.json"
REDIS_SET_KEY = "scanner:pumpfun:seen"


# ---------- Local file cache helpers ----------
def _load_file_cache() -> Set[str]:
    try:
        if SNIPED_MINTS_FILE.exists():
            return set(json.loads(SNIPED_MINTS_FILE.read_text()))
    except Exception as e:
        logger.warning(f"[Pump.fun] file cache read failed: {e}")
    return set()


def _save_file_cache(cache: Set[str]) -> None:
    try:
        SNIPED_MINTS_FILE.write_text(json.dumps(sorted(cache)))
    except Exception as e:
        logger.warning(f"[Pump.fun] file cache write failed: {e}")


# ---------- Small parsing helpers ----------
def _read_float(src: dict, *keys: str, default: Optional[float] = None) -> Optional[float]:
    for k in keys:
        v = src.get(k)
        if v is None:
            continue
        try:
            return float(v)
        except (TypeError, ValueError):
            continue
    return default


def _pool_age_min_from(item: dict) -> float:
    """
    Pump.fun payloads often include 'createdAt' or a nested 'bondingCurve.startTime'.
    Normalize to minutes since creation.
    """
    created = item.get("createdAt") or item.get("created_at")
    if not created:
        bc = item.get("bondingCurve") or {}
        created = bc.get("startTime") or bc.get("createdAt")
    try:
        if created:
            ts = float(created)
            # handle ms vs s
            if ts > 10_000_000_000:
                ts = ts / 1000.0
            return max(0.0, (time.time() - ts) / 60.0)
    except Exception:
        pass
    return 0.0


# ---------- Main scanner (async generator) ----------
async def scan_pumpfun(
    redis: aioredis.Redis,
) -> AsyncGenerator[Dict, None]:
    """
    Scan Pump.fun listings and yield vetted token dicts that downstream can buy.
    Yields dict with fields: {"source","mint","symbol","price","liquidity_usd","fdv_usd","pool_age_min"}.
    - Uses Redis + local file cache to skip duplicates
    - Filters by liquidity, FDV, fraud/whale/honeypot checks
    - Sends Telegram + Notion logs for visibility
    """
    local_cache = _load_file_cache()

    # --- fetch Pump.fun feed ---
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(PUMPFUN_API_URL, timeout=15) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.warning(f"[Pump.fun] HTTP {resp.status}: {text[:200]}")
                    return
                payload = await resp.json()
    except Exception as e:
        logger.error(f"[Pump.fun] fetch error: {e}")
        return

    tokens = (
        payload.get("tokens") if isinstance(payload, dict)
        else (payload if isinstance(payload, list) else [])
    )
    if not isinstance(tokens, list):
        logger.warning("[Pump.fun] unexpected JSON format")
        return

    # --- iterate tokens ---
    for item in tokens:
        try:
            mint = item.get("mint") or item.get("address")
            symbol = item.get("symbol") or item.get("name") or "UNKNOWN"

            # Normalize price/liquidity/fdv regardless of field naming
            price_usd = _read_float(item, "priceUsd", "price_usd", "usdPrice", "price")
            fdv_usd   = _read_float(item, "fdv", "marketCap", "fdvUsd", default=0.0) or 0.0

            liq_usd = 0.0
            liq_obj = item.get("liquidity") or {}
            if isinstance(liq_obj, dict):
                liq_usd = _read_float(liq_obj, "usd", "USD", "usdValue", default=0.0) or 0.0
            else:
                liq_usd = _read_float(item, "liquidityUsd", "liquidity_usd", default=0.0) or 0.0

            pool_age_min = _pool_age_min_from(item)

            if not mint or not price_usd or price_usd <= 0:
                continue

            # --- dedupe (Redis + local file) ---
            try:
                if mint in local_cache:
                    continue
                if await redis.sismember(REDIS_SET_KEY, mint):
                    continue
            except Exception as e:
                logger.debug(f"[Pump.fun] redis check failed: {e}")

            # --- Stage 0: Market filters ---
            if liq_usd < SCANNER_MIN_LIQ_USD or fdv_usd < SCANNER_MIN_FDV_USD:
                logger.debug(
                    f"[Pump.fun] reject {symbol} low liq/fdv: "
                    f"liq=${liq_usd:.0f}, fdv=${fdv_usd:,.0f}"
                )
                # mark seen to avoid retesting junk in tight loops
                try:
                    await redis.sadd(REDIS_SET_KEY, mint)
                    await redis.expire(REDIS_SET_KEY, 3 * 24 * 3600)
                except Exception:
                    pass
                local_cache.add(mint)
                _save_file_cache(local_cache)
                continue

            # --- Risk checks ---
            if not await check_rug_pull_risk(mint):
                logger.info(f"[Pump.fun] {symbol} honeypot/rug check failed")
                # mark seen
                try:
                    await redis.sadd(REDIS_SET_KEY, mint)
                    await redis.expire(REDIS_SET_KEY, 3 * 24 * 3600)
                except Exception:
                    pass
                local_cache.add(mint)
                _save_file_cache(local_cache)
                continue

            score = await fraud_score_ai(mint)
            if score < SCANNER_MIN_FRAUD_SCORE:
                logger.info(f"[Pump.fun] {symbol} fraud score {score:.2f} < {SCANNER_MIN_FRAUD_SCORE}")
                # mark seen
                try:
                    await redis.sadd(REDIS_SET_KEY, mint)
                    await redis.expire(REDIS_SET_KEY, 3 * 24 * 3600)
                except Exception:
                    pass
                local_cache.add(mint)
                _save_file_cache(local_cache)
                continue

            if not await is_whale_wallet(mint):
                logger.info(f"[Pump.fun] {symbol} whale filter failed")
                # mark seen
                try:
                    await redis.sadd(REDIS_SET_KEY, mint)
                    await redis.expire(REDIS_SET_KEY, 3 * 24 * 3600)
                except Exception:
                    pass
                local_cache.add(mint)
                _save_file_cache(local_cache)
                continue

            # --- Passed all filters ---
            candidate = {
                "source": "pumpfun",
                "mint": mint,
                "symbol": symbol,
                "price": float(price_usd),       # keep key 'price' to match core/sniper expectations
                "liquidity_usd": float(liq_usd),
                "fdv_usd": float(fdv_usd),
                "pool_age_min": float(pool_age_min),
                "fraud_score": float(score),
            }

            if SCANNER_ALERTS_ENABLED:
                url = f"https://pump.fun/{mint}"
                msg = (
                    "🟢 <b>Pump.fun candidate</b>\n"
                    f"<b>{symbol}</b> ({mint})\n"
                    f"Price ${price_usd:.6f} | FDV ${fdv_usd:,.0f} | Liq ${liq_usd:,.0f}\n"
                    f"Fraud Score: {score:.1f}\n{url}"
                )
                await send_telegram(msg)
                # Log discovery into Notion (profit=0 at discover time)
                try:
                    await track_profit_notion(mint, 0.0)
                except Exception:
                    pass

            # Cache “seen”
            try:
                await redis.sadd(REDIS_SET_KEY, mint)
                await redis.expire(REDIS_SET_KEY, 3 * 24 * 3600)
            except Exception:
                pass
            local_cache.add(mint)
            _save_file_cache(local_cache)

            logger.info(f"✅ [Pump.fun] yield {symbol} at ${price_usd:.6f}")
            yield candidate

        except Exception as e:
            logger.error(f"[Pump.fun] item error: {e}")
            continue
