# sniper/arbitrage.py
import os
import logging
from typing import Optional, Dict, Any, List

from utils_pkg.core_utils import session  # shared aiohttp.ClientSession

logger = logging.getLogger("arbitrage")

# Env-tunable threshold (in basis points, e.g. 50 = 0.50% spread)
MIN_ARB_EDGE_BPS_DEFAULT = int(os.getenv("MIN_ARB_EDGE_BPS", "50"))

# Dexscreener token endpoint (Solana)
DEXSCREENER_TOKEN_URL = "https://api.dexscreener.com/latest/dex/tokens/{mint}"


async def _fetch_dexscreener_pairs(mint: str) -> List[Dict[str, Any]]:
    """
    Fetch all known pairs for a token on Solana from Dexscreener.
    We prefer priceNative (in SOL) for cross-DEX comparison consistency.
    """
    try:
        if not session:
            logger.warning("HTTP session not initialized in core_utils; arbitrage fetch skipped.")
            return []

        url = DEXSCREENER_TOKEN_URL.format(mint=mint)
        async with session.get(url, timeout=10) as resp:
            if resp.status != 200:
                txt = await resp.text()
                logger.warning(f"Dexscreener fetch [{resp.status}] {url}: {txt}")
                return []
            data = await resp.json()
            return data.get("pairs", []) or []
    except Exception as e:
        logger.error(f"Arb: Dexscreener fetch error for {mint}: {e}")
        return []


def _extract_price(pair: Dict[str, Any]) -> Optional[float]:
    """
    Extract price as float. Prefer 'priceNative' (SOL); fallback to 'priceUsd' if needed.
    """
    try:
        p = pair.get("priceNative")
        if p is None:
            p = pair.get("priceUsd")
        return float(p) if p is not None else None
    except Exception:
        return None


async def check_arbitrage_opportunity(
    token_mint: str,
    min_edge_bps: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    """
    Find a simple cross-DEX arbitrage edge using Dexscreener pairs for the token.
    Returns a dict describing the best opportunity if edge >= min_edge_bps, else None.

    Edge (bps) = (bestSell - bestBuy) / mid * 10_000
    """
    threshold = MIN_ARB_EDGE_BPS_DEFAULT if min_edge_bps is None else int(min_edge_bps)

    pairs = await _fetch_dexscreener_pairs(token_mint)
    if not pairs or len(pairs) < 2:
        return None

    books: List[Dict[str, Any]] = []
    for p in pairs:
        dex = p.get("dexId") or p.get("dex") or "unknown"
        url = p.get("url")
        price = _extract_price(p)
        if price and price > 0:
            books.append({"dex": dex, "price": float(price), "url": url})

    if len(books) < 2:
        return None

    # sort by price to find best buy (min) and best sell (max)
    books.sort(key=lambda x: x["price"])
    best_buy = books[0]
    best_sell = books[-1]

    # if same DEX, no cross-venue arb
    if best_sell["dex"] == best_buy["dex"]:
        return None

    mid = (best_sell["price"] + best_buy["price"]) / 2.0
    if mid <= 0:
        return None

    edge_bps = int(((best_sell["price"] - best_buy["price"]) / mid) * 10_000)

    if edge_bps >= threshold:
        opp = {
            "token": token_mint,
            "books_seen": len(books),
            "edge_bps": edge_bps,
            "buy": {
                "dex": best_buy["dex"],
                "price": best_buy["price"],
                "url": best_buy.get("url"),
            },
            "sell": {
                "dex": best_sell["dex"],
                "price": best_sell["price"],
                "url": best_sell.get("url"),
            },
            # This is *signal only*; actual routing uses Jupiter in your executor.
            "suggestion": f"Buy on {best_buy['dex']} @ {best_buy['price']}, sell on {best_sell['dex']} @ {best_sell['price']}",
        }
        logger.info(f"Arb opp {token_mint}: +{edge_bps} bps ({best_buy['dex']} -> {best_sell['dex']})")
        return opp

    return None
