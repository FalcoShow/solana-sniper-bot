# scanner/pumpfun_scanner.py
# Cloudflare sometimes blocks direct API calls. We fail soft (return [])
# and optionally source from Dexscreener as a proxy for Pump.fun activity.
from __future__ import annotations
import os, logging
from typing import List, Dict, Any

logger = logging.getLogger("scanner.pumpfun")

UA = os.getenv("SCANNER_UA", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) SniperBot/1.0 (+https://github.com/FalcoShow/solana-sniper-bot)")
ENABLE_FALLBACK = os.getenv("PUMPFUN_USE_DEXSCREENER_FALLBACK", "true").lower() == "true"
LIMIT = int(os.getenv("PUMPFUN_LIMIT", "30"))
MIN_LIQ_USD = float(os.getenv("PUMPFUN_MIN_LIQ_USD", "5000"))

async def _fallback_from_dexscreener(session) -> List[Dict[str, Any]]:
    url = "https://api.dexscreener.com/latest/dex/search"
    params = {"q": "pump"}
    headers = {"User-Agent": UA, "Accept": "application/json"}
    try:
        async with session.get(url, params=params, headers=headers, timeout=8) as r:
            if r.status != 200:
                txt = await r.text()
                logger.warning("Pump.fun fallback via Dexscreener failed: %s", r.status)
                return []
            data = await r.json()
            pairs = data.get("pairs") or []
    except Exception as e:
        logger.warning("Pump.fun fallback error: %s", e)
        return []

    out: List[Dict[str, Any]] = []
    for p in pairs:
        try:
            if p.get("chainId") != "solana":
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
                "source": "pumpfun(dex-fallback)",
            })
        except Exception:
            continue
    out.sort(key=lambda x: x.get("liq_usd", 0), reverse=True)
    return out[:LIMIT]

async def scan_pumpfun(*args):
    # Grab shared session
    session = None
    if len(args) >= 2:
        session = args[1]
    else:
        from utils_pkg.core_utils import session as core_session
        session = core_session

    if session is None:
        logger.warning("Pump.fun scanner: no HTTP session available")
        return []

    # (If you later have an official Pump.fun JSON endpoint, call it here.)
    # For now we use Dexscreener fallback to avoid HTTP 530 Cloudflare blocks.
    if ENABLE_FALLBACK:
        logger.warning("Pump.fun HTTP 530 observed or API not configured; using Dexscreener fallback.")
        return await _fallback_from_dexscreener(session)
    return []
