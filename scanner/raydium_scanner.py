# raydium_scanner.py
import aiohttp
import logging
import json
import time
from pathlib import Path
from typing import AsyncGenerator, Dict, Optional, Set, Tuple

import redis.asyncio as aioredis

from config import (
    RAYDIUM_PAIRS_API,
    SCANNER_MIN_LIQ_USD,
    SCANNER_MIN_FDV_USD,
    SCANNER_MIN_FRAUD_SCORE,
    SCANNER_ALERTS_ENABLED,
)
from utils import (
    check_rug_pull_risk,   # bool-like: True if safe enough to proceed
    fraud_score_ai,        # returns 0..100
    is_whale_wallet,       # bool: whales present
    send_telegram,         # async
    track_profit_notion,   # async (we log discovery w/ profit 0)
)

logger = logging.getLogger("scanner.raydium")

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
SNIPED_MINTS_FILE = DATA_DIR / "sniped_mints_raydium.json"
REDIS_SET_KEY = "scanner:raydium:seen"


# ---------- Local file cache helpers ----------
def _load_file_cache() -> Set[str]:
    try:
        if SNIPED_MINTS_FILE.exists():
            return set(json.loads(SNIPED_MINTS_FILE.read_text()))
    except Exception as e:
        logger.warning(f"[Raydium] file cache read failed: {e}")
    return set()


def _save_file_cache(cache: Set[str]) -> None:
    try:
        SNIPED_MINTS_FILE.write_text(json.dumps(sorted(cache)))
    except Exception as e:
        logger.warning(f"[Raydium] file cache write failed: {e}")


# ---------- Parsing helpers ----------
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


def _parse_pair_item(item: Dict) -> Optional[Tuple[str, str, float, float, float, float]]:
    """
    Returns (mint, symbol, price_usd, liq_usd, fdv_usd, pool_age_min)
    Handles common Raydium/Dex formats defensively.
    """
    mint = (
        item.get("baseMint")
        or item.get("mint")
        or item.get("address")
        or (item.get("baseToken") or {}).get("address")
    )
    symbol = (
        item.get("name")
        or (item.get("baseToken") or {}).get("symbol")
        or item.get("symbol")
        or "UNKNOWN"
    )

    price_usd = _read_float(item, "price", "priceUsd", "price_usd", "basePrice", "lastPriceUsd")

    # liquidity (usd)
    liq_usd = 0.0
    liq_obj = item.get("liquidity") or {}
    if isinstance(liq_obj, dict):
        liq_usd = _read_float(liq_obj, "usd", "USD", "liquidityUsd", "tvlUsd", "tvl_usd", default=0.0) or 0.0
    else:
        liq_usd = _read_float(item, "liquidityUsd", "tvlUsd", "tvl_usd", default=0.0) or 0.0

    # fdv
    fdv_usd = _read_float(item, "fdv", "marketCap", "fdvUsd", "market_cap", default=0.0) or 0.0

    # pool age (minutes)
    pool_age_min = 0.0
    created_at = item.get("createdAt") or item.get("created_at") or item.get("pairCreatedAt")
    try:
        if created_at:
            ts = float(created_at)
            if ts > 10_000_000_000:  # ms -> s
                ts = ts / 1000.0
            pool_age_min = max(0.0, (time.time() - ts) / 60.0)
    except Exception:
        pool_age_min = 0.0

    if not mint or not price_usd or price_usd <= 0:
        return None
    return mint, symbol, float(price_usd), float(liq_usd), float(fdv_usd), float(pool_age_min)


async def _alert_candidate(symbol: str, mint: str, price_usd: float, fdv_usd: float, liq_usd: float) -> None:
    if not SCANNER_ALERTS_ENABLED:
        return
    url = f"https://raydium.io/swap/?inputMint=So11111111111111111111111111111111111111112&outputMint={mint}"
    try:
        await send_telegram(
            "🟢 <b>Raydium candidate</b>\n"
            f"<b>{symbol}</b> ({mint})\n"
            f"Price ${price_usd:.6f} | FDV ${fdv_usd:,.0f} | Liq ${liq_usd:,.0f}\n{url}"
        )
    except Exception as e:
        logger.debug(f"[Raydium] telegram alert failed: {e}")
    try:
        # Discovery log (profit=0 at detection)
        await track_profit_notion(mint, 0.0)
    except Exception as e:
        logger.debug(f"[Raydium] notion log failed: {e}")


# ---------- Main scanner (async generator) ----------
async def scan_raydium(
    redis: aioredis.Redis,
) -> AsyncGenerator[Dict, None]:
    """
    Scan Raydium pairs and yield vetted token dicts for downstream buying.
    Yields dict with: {"source","mint","symbol","price","liquidity_usd","fdv_usd","pool_age_min","fraud_score"}
    """
    local_cache = _load_file_cache()

    # fetch Raydium pairs
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(RAYDIUM_PAIRS_API, timeout=15) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.warning(f"[Raydium] HTTP {resp.status}: {text[:200]}")
                    return
                payload = await resp.json()
    except Exception as e:
        logger.error(f"[Raydium] fetch error: {e}")
        return

    # Raydium sometimes returns {"data": [...]}
    pairs = payload.get("data") if isinstance(payload, dict) else payload
    if not isinstance(pairs, list):
        logger.warning("[Raydium] unexpected JSON format")
        return

    for item in pairs:
        try:
            parsed = _parse_pair_item(item)
            if not parsed:
                continue
            mint, symbol, price_usd, liq_usd, fdv_usd, pool_age_min = parsed

            # dedupe (Redis + local)
            try:
                if mint in local_cache:
                    continue
                if await redis.sismember(REDIS_SET_KEY, mint):
                    continue
            except Exception as e:
                logger.debug(f"[Raydium] redis check failed: {e}")

            # Stage-0 market filters
            if liq_usd < SCANNER_MIN_LIQ_USD or fdv_usd < SCANNER_MIN_FDV_USD:
                logger.debug(
                    f"[Raydium] reject {symbol} low liq/fdv: "
                    f"liq=${liq_usd:.0f}, fdv=${fdv_usd:,.0f}"
                )
                # mark seen to avoid rechecking noisy pairs
                try:
                    await redis.sadd(REDIS_SET_KEY, mint)
                    await redis.expire(REDIS_SET_KEY, 3 * 24 * 3600)
                except Exception:
                    pass
                local_cache.add(mint)
                _save_file_cache(local_cache)
                continue

            # Risk/honeypot check
            if not await check_rug_pull_risk(mint):
                logger.info(f"[Raydium] {symbol} honeypot/rug check failed")
                try:
                    await redis.sadd(REDIS_SET_KEY, mint)
                    await redis.expire(REDIS_SET_KEY, 3 * 24 * 3600)
                except Exception:
                    pass
                local_cache.add(mint)
                _save_file_cache(local_cache)
                continue

            # AI fraud score
            score = await fraud_score_ai(mint)
            if score < SCANNER_MIN_FRAUD_SCORE:
                logger.info(f"[Raydium] {symbol} fraud score {score:.1f} < {SCANNER_MIN_FRAUD_SCORE}")
                try:
                    await redis.sadd(REDIS_SET_KEY, mint)
                    await redis.expire(REDIS_SET_KEY, 3 * 24 * 3600)
                except Exception:
                    pass
                local_cache.add(mint)
                _save_file_cache(local_cache)
                continue

            # Whale presence
            if not await is_whale_wallet(mint):
                logger.info(f"[Raydium] {symbol} whale filter failed")
                try:
                    await redis.sadd(REDIS_SET_KEY, mint)
                    await redis.expire(REDIS_SET_KEY, 3 * 24 * 3600)
                except Exception:
                    pass
                local_cache.add(mint)
                _save_file_cache(local_cache)
                continue

            # Passed all gates
            candidate = {
                "source": "raydium",
                "mint": mint,
                "symbol": symbol,
                "price": float(price_usd),       # keep key 'price' to match sniper/core
                "liquidity_usd": float(liq_usd),
                "fdv_usd": float(fdv_usd),
                "pool_age_min": float(pool_age_min),
                "fraud_score": float(score),
            }

            await _alert_candidate(symbol, mint, price_usd, fdv_usd, liq_usd)

            # cache as seen
            try:
                await redis.sadd(REDIS_SET_KEY, mint)
                await redis.expire(REDIS_SET_KEY, 3 * 24 * 3600)
            except Exception:
                pass
            local_cache.add(mint)
            _save_file_cache(local_cache)

            logger.info(f"✅ [Raydium] yield {symbol} at ${price_usd:.6f}")
            yield candidate

        except Exception as e:
            logger.error(f"[Raydium] item error: {e}")
            continue
