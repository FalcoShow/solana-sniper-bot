# execution/exits_wiring.py
from typing import Optional, Tuple, Dict, Any

# ⬇️ EDIT these two to your project’s real quote/sell functions
async def price_fn(mint: str) -> Optional[float]:
    # e.g., from quotes import get_price_usd
    # return await get_price_usd(mint)
    from quotes import get_price_usd  # adjust import to your project
    return await get_price_usd(mint)  # USD/token

async def sell_fn(mint: str, usd_to_sell: float, reason: str) -> Tuple[bool, Optional[str], Dict[str, Any]]:
    # e.g., from jupiter import sell_usd
    # return await sell_usd(mint, usd_to_sell, reason)
    from jupiter import sell_usd  # adjust import to your project
    return await sell_usd(mint, usd_to_sell, reason)
