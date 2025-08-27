# scanner/raydium_scanner.py
# Finds Raydium pairs via Dexscreener (stable public JSON).
from __future__ import annotations
import os, logging
from typing import List, Dict, Any

logger = logging.getLogger("scanner.raydium")

UA = os.getenv("SCANNER_UA", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) SniperBot/1.0 (+https://github.com/FalcoShow/solana-sniper-bot)")
LIMIT = int(os.getenv("RAYDIUM_LIMIT", "40"))
MIN_LIQ_USD = float(os.getenv("RAYDIUM_MIN_LIQ_USD", "10000"))

async def scan_raydium(*args):
    session = None
    if len(args) >= 2:
        session = args[1]
    else:
        from utils_pkg.core_utils import session as core_session
        session = core_session
    if session is None:
        logger.warning("Raydium scanner: no HTTP session available")
        return []

    url = "https://api.dexscreener.com/latest/dex/search"
    params = {"q": "raydium"}
    headers = {"User-Agent": UA, "Accept": "application/json"}

    try:
        async with session.get(url, params=params, headers=headers, timeout=8) as r:
            if r.status != 200:
                txt = await r.text()
                logger.warning("Raydium via Dexscreener failed: %s", r.status)
                return []
            data = await r.json()
            pairs = data.get("pairs") or []
    except Exception as e:
        logger.warning("Raydium fetch error: %s", e)
        return []

    out: List[Dict[str, Any]] = []
    for p in pairs:
        try:
            if p.get("chainId") != "solana":
                continue
            if (p.get("dexId") or "").lower() != "raydium":
                continue
            liq = float((p.get("liquidity") or {}).get("usd") or 0.0)
            if liq < MIN_LIQ_USD:
                continue
            base = p.get("baseToken") or {}
            mint = base.get("address") or ""
            if not mint:
                continue
            symbol = base.get("symbol") or mint[:6]
            price_usd = float(p.get("priceUsd") or 0.0)
            out.append({
                "mint": mint,
                "symbol": symbol,
                "liq_usd": liq,
                "price_usd": price_usd,
                "source": "raydium(dex)",
            })
        except Exception:
            continue

    out.sort(key=lambda x: x.get("liq_usd", 0), reverse=True)
    return out[:LIMIT]
