
# scanner/raydium_scanner.py
from __future__ import annotations
import logging
from typing import Any, Dict, List, Optional

try:
    from utils_pkg.core_utils import get_session
except Exception:
    get_session = lambda: None  # type: ignore

log = logging.getLogger("scanner.raydium")

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "sniper/1.0 (+github.com)",
}

def _norm_pair(p: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        mint = p.get("baseMint") or p.get("baseMintAddress") or p.get("mint")
        if not mint:
            return None
        sym = p.get("baseSymbol") or p.get("symbol") or mint[:6]
        liq = float(p.get("liquidityUsd") or p.get("liquidity_usd") or 0.0)
        vol24 = float(p.get("volumeUsd24h") or p.get("day") or 0.0)
        return {
            "mint": mint,
            "symbol": sym,
            "liq_usd": liq,
            "volumeUsd24h": vol24,
            "source": "raydium",
        }
    except Exception:
        return None

async def scan_raydium(*_args, **_kwargs) -> List[Dict[str, Any]]:
    session = get_session()
    if session is None:
        log.warning("No shared session; raydium scan skipped")
        return []

    urls = [
        "https://api.raydium.io/v2/main/pairs",
        "https://api.raydium.io/v2/sdk/liquidity/mainnet.json",
    ]
    out: List[Dict[str, Any]] = []
    for url in urls:
        try:
            async with session.get(url, headers=HEADERS, timeout=10) as resp:
                if resp.status != 200:
                    txt = await resp.text()
                    log.warning("Raydium HTTP %s", resp.status)
                    continue
                data = await resp.json()
                pairs = []
                if isinstance(data, list):
                    pairs = data
                elif isinstance(data, dict):
                    pairs = data.get("data") or data.get("pairs") or []
                for p in pairs:
                    c = _norm_pair(p)
                    if c:
                        out.append(c)
            if out:
                break
        except Exception as e:
            log.warning("Raydium request error: %s", e)
            continue
    return out
