
# scanner/dexscreener_scanner.py
from __future__ import annotations
import asyncio, logging, os, time
from typing import Any, Dict, List, Optional

try:
    # Prefer shared session for connection pooling
    from utils_pkg.core_utils import get_session
except Exception:
    get_session = lambda: None  # type: ignore

log = logging.getLogger("scanner.dexscreener")

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "sniper/1.0 (+github.com)",
}

# Normalize a single pair to our candidate shape
def _norm_pair(p: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        base = (p.get("baseToken") or {}) if isinstance(p.get("baseToken"), dict) else {}
        mint = base.get("address") or p.get("baseTokenAddress") or p.get("baseMint")
        if not mint:
            return None
        sym  = base.get("symbol") or p.get("baseTokenSymbol") or (mint[:6])
        liq_usd = float(((p.get("liquidity") or {}).get("usd") if isinstance(p.get("liquidity"), dict) else p.get("liquidityUsd")) or 0.0)
        price_usd = float(p.get("priceUsd") or 0.0)
        vol24 = float(((p.get("volume") or {}).get("h24") if isinstance(p.get("volume"), dict) else p.get("volumeUsd24h")) or 0.0)
        tx5 = p.get("txns", {}).get("m5", {}) if isinstance(p.get("txns"), dict) else {}
        buys5 = int(tx5.get("buys", 0) or 0)
        sells5 = int(tx5.get("sells", 0) or 0)
        age_min = None
        if p.get("pairCreatedAt"):
            try:
                age_min = max(1, int((time.time()*1000 - int(p["pairCreatedAt"])) / 60000))
            except Exception:
                age_min = None
        impact = float(p.get("priceImpact1kBps") or p.get("impact1kBps") or 0.0)
        return {
            "mint": mint,
            "symbol": sym,
            "liq_usd": liq_usd,
            "price_usd": price_usd,
            "volumeUsd24h": vol24,
            "buys5m": buys5,
            "sells5m": sells5,
            "pool_age_min": age_min,
            "impact_1k_bps": impact,
            "source": "dexscreener",
        }
    except Exception:
        return None

async def _fetch_json(session, url: str) -> Optional[Dict[str, Any]]:
    try:
        async with session.get(url, headers=HEADERS, timeout=8) as resp:
            if resp.status != 200:
                txt = await resp.text()
                log.warning("Dexscreener API failed with status %s", resp.status)
                return None
            return await resp.json()
    except Exception as e:
        log.warning("Dexscreener request error: %s", e)
        return None

async def scan_dexscreener(*_args, **_kwargs) -> List[Dict[str, Any]]:
    """Try multiple public endpoints; return a merged list of candidates.

    Signature accepts anything to stay compatible with older callers.

    """
    session = get_session()
    if session is None:
        log.warning("No shared session; dexscreener scan skipped")
        return []

    log.info("🟣 Starting Dexscreener scan...")

    endpoints = [
        # 1) Trending pairs on Solana
        "https://api.dexscreener.com/latest/dex/trending?chain=solana",
        # 2) Broad pairs listing (often heavy; keep as fallback)
        "https://api.dexscreener.com/latest/dex/pairs/solana",
    ]

    found: Dict[str, Dict[str, Any]] = {}
    for url in endpoints:
        data = await _fetch_json(session, url)
        if not data:
            continue
        pairs: List[Dict[str, Any]] = []
        if "pairs" in data and isinstance(data["pairs"], list):
            pairs = data["pairs"]
        elif "trending" in data and isinstance(data["trending"], list):
            pairs = data["trending"]
        for p in pairs:
            c = _norm_pair(p) or {}
            mint = c.get("mint")
            if not mint: 
                continue
            if mint not in found:
                found[mint] = c
            else:
                # prefer higher liquidity
                if float(c.get("liq_usd", 0) or 0) > float(found[mint].get("liq_usd", 0) or 0):
                    found[mint] = c

    out = list(found.values())
    # light filter: ensure required fields
    out = [c for c in out if c.get("mint") and c.get("symbol")]
    return out
