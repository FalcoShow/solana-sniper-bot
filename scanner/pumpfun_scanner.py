
# scanner/pumpfun_scanner.py
from __future__ import annotations
import logging
from typing import Any, Dict, List, Optional

try:
    from utils_pkg.core_utils import get_session
except Exception:
    get_session = lambda: None  # type: ignore

log = logging.getLogger("scanner.pumpfun")

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "sniper/1.0 (+github.com)",
}

def _norm_coin(c: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        mint = c.get("mint") or c.get("address") or c.get("tokenAddress")
        if not mint:
            return None
        sym = c.get("symbol") or c.get("ticker") or mint[:6]
        liq = float(c.get("liquidity_usd") or c.get("liquidityUsd") or c.get("usdLiquidity") or 0.0)
        vol24 = float(c.get("volumeUsd24h") or c.get("volume_usd_24h") or 0.0)
        age_min = c.get("ageMin") or c.get("pool_age_min")
        return {
            "mint": mint,
            "symbol": sym,
            "liq_usd": liq,
            "volumeUsd24h": vol24,
            "pool_age_min": age_min,
            "source": "pump.fun",
        }
    except Exception:
        return None

async def scan_pumpfun(*_args, **_kwargs) -> List[Dict[str, Any]]:
    session = get_session()
    if session is None:
        log.warning("No shared session; pump.fun scan skipped")
        return []

    # Unofficial public endpoints change often; try a few known ones.
    urls = [
        "https://frontend-api.pump.fun/coins/latest?limit=50",
        "https://frontend-api.pump.fun/coins/trending?limit=50",
    ]
    out: List[Dict[str, Any]] = []
    for url in urls:
        try:
            async with session.get(url, headers=HEADERS, timeout=8) as resp:
                if resp.status != 200:
                    txt = await resp.text()
                    log.warning("[Pump.fun] HTTP %s: %s", resp.status, (txt[:120] + "...") if txt else "")
                    continue
                data = await resp.json()
                # Payloads vary; handle list or {items: []}
                items = data if isinstance(data, list) else data.get("items") or data.get("coins") or []
                for it in items:
                    c = _norm_coin(it)
                    if c:
                        out.append(c)
            if out:
                break  # one good endpoint is enough
        except Exception as e:
            log.warning("pump.fun request error: %s", e)
            continue
    return out
