# scanner/dexscreener_scanner.py
# Pulls liquid Solana pairs from Dexscreener's public JSON API.
from __future__ import annotations
import os, asyncio, logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger("scanner.dexscreener")

UA = os.getenv("SCANNER_UA", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) SniperBot/1.0 (+https://github.com/FalcoShow/solana-sniper-bot)")
BASE = os.getenv("DEXSCREENER_BASE_URL", "https://api.dexscreener.com")
MIN_LIQ_USD = float(os.getenv("DEX_MIN_LIQ_USD", "10000"))
LIMIT = int(os.getenv("DEX_LIMIT", "40"))

async def _fetch_pairs(session) -> List[Dict[str, Any]]:
    url = f"{BASE.rstrip('/')}/latest/dex/search"
    params = {"q": "solana"}
    headers = {"User-Agent": UA, "Accept": "application/json"}
    try:
        async with session.get(url, params=params, headers=headers, timeout=8) as r:
            if r.status != 200:
                body = await r.text()
                logger.warning("Dexscreener API failed with status %s", r.status)
                return []
            data = await r.json()
            return data.get("pairs") or []
    except Exception as e:
        logger.warning("Dexscreener fetch error: %s", e)
        return []

def _normalize(pair: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        if pair.get("chainId") != "solana":
            return None
        liq = float((pair.get("liquidity") or {}).get("usd") or 0.0)
        if liq < MIN_LIQ_USD:
            return None
        base = pair.get("baseToken") or {}
        mint = base.get("address") or ""
        if not mint:
            return None
        symbol = base.get("symbol") or mint[:6]
        price_usd = float(pair.get("priceUsd") or 0.0)
        tx5 = (pair.get("txns") or {}).get("m5") or {}
        buys = int(tx5.get("buys", 0) or 0)
        sells = int(tx5.get("sells", 0) or 0)
        skew = (buys + 1) / (sells + 1)
        return {
            "mint": mint,
            "symbol": symbol,
            "liq_usd": liq,
            "price_usd": price_usd,
            "skew5m": skew,
            "source": "dexscreener",
        }
    except Exception:
        return None

async def scan_dexscreener(*args):
    """Flexible signature: scan_dexscreener(redis, session) | (redis) | ()"""
    # Acquire session from args if provided (sniper passes (redis, session))
    session = None
    if len(args) >= 2:
        session = args[1]
    elif len(args) == 1:
        # If only a redis is passed, it will be args[0]; we don't need it here.
        pass

    if session is None:
        # Late import to avoid hard-coupling
        from utils_pkg.core_utils import session as core_session
        session = core_session
    if session is None:
        logger.warning("Dexscreener scanner: no HTTP session available")
        return []

    logger.info("🔎 Starting Dexscreener scan...")
    pairs = await _fetch_pairs(session)
    out: List[Dict[str, Any]] = []
    for p in pairs:
        row = _normalize(p)
        if row:
            out.append(row)
    out.sort(key=lambda x: (x.get("liq_usd", 0), x.get("skew5m", 1.0)), reverse=True)
    return out[:LIMIT]
