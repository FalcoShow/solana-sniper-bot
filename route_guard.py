# route_guard.py — v2 (quote aging guard + route source histogram)
import os, logging
from typing import Optional, Tuple, Dict, List

from jup_quote_cache import get_quote_with_age
from route_telemetry import count_route_used, count_watchdog_hit

logger = logging.getLogger("route_guard")
logging.basicConfig(level=logging.INFO)

SOL_MINT = "So11111111111111111111111111111111111111112"
LAMPORTS = 1_000_000_000

ROUTE_MAX_HOPS               = int(os.getenv("ROUTE_MAX_HOPS", "5"))
ROUTE_MAX_IMPACT_BPS         = float(os.getenv("ROUTE_MAX_IMPACT_BPS", os.getenv("AI_MAX_IMPACT_BPS", "150")))
WATCHDOG_ALERT_DEVIATION_BPS = int(os.getenv("ROUTE_WATCHDOG_DEVIATION_BPS", "60"))
QUOTE_MAX_AGE_SEC            = int(os.getenv("QUOTE_MAX_AGE_SEC", "12"))

def _extract_best(payload: dict) -> Optional[dict]:
    routes = (payload or {}).get("data") or (payload or {}).get("routes") or []
    if not isinstance(routes, list) or not routes: return None
    return routes[0]

def _dex_labels(route: dict) -> List[str]:
    labels: List[str] = []
    mis = route.get("marketInfos") or []
    for mi in mis:
        raw = mi.get("label") or (mi.get("amm") or {}).get("label") or (mi.get("amm") or {}).get("name") or mi.get("platform")
        if not raw:
            labels.append("OTHER"); continue
        r = str(raw).lower()
        if   "raydium" in r:   labels.append("RAYDIUM")
        elif "orca" in r:      labels.append("ORCA")
        elif "meteora" in r:   labels.append("METEORA")
        elif "openbook" in r:  labels.append("OPENBOOK")
        elif "phoenix" in r:   labels.append("PHOENIX")
        elif "crema" in r:     labels.append("CREMA")
        elif "whirlpool" in r: labels.append("ORCA-WHIRL")
        else:                  labels.append(r.upper())
    if not labels:
        labels.append("OTHER")
    return labels

def _route_meta(route: dict) -> Dict:
    hop_len = len(route.get("marketInfos") or [])
    priceImpactPct = float(route.get("priceImpactPct", 0.0) or 0.0)
    outAmount      = float(route.get("outAmount", 0.0) or 0.0)
    inAmount       = float(route.get("inAmount", 0.0) or 0.0)
    dex_labels     = _dex_labels(route)
    return {
        "hops": hop_len,
        "impact_bps": priceImpactPct * 10000.0,
        "out_amount": outAmount,
        "in_amount": inAmount,
        "labels": dex_labels,
    }

async def precheck_buy(out_mint: str, amount_sol: float, slippage_bps: int) -> Tuple[bool, str, Dict]:
    """
    (ok, reason, meta) — guards: quote freshness, hop count, impact bps.
    Meta includes 'labels' for route histogram.
    """
    lamports = max(1, int(amount_sol * LAMPORTS))
    quote, age = await get_quote_with_age(out_mint, lamports, slippage_bps)
    if not quote:
        return False, "no_quote", {}
    if age is not None and age > QUOTE_MAX_AGE_SEC:
        return False, f"stale_prequote:{int(age)}s", {}

    best = _extract_best(quote)
    if not best:
        return False, "no_routes", {}

    meta = _route_meta(best)
    if meta["hops"] > ROUTE_MAX_HOPS:
        return False, f"too_many_hops:{meta['hops']}", meta
    if meta["impact_bps"] > ROUTE_MAX_IMPACT_BPS:
        return False, f"impact_bps:{meta['impact_bps']:.0f}", meta
    return True, "ok", meta

async def posttrade_watchdog(out_mint: str, amount_sol: float, slippage_bps: int, pre_meta: Dict) -> Tuple[bool, str, Dict]:
    """
    Compare a fresh quote after trade vs prequote; emit route telemetry.
    """
    lamports = max(1, int(amount_sol * LAMPORTS))
    quote2, _age2 = await get_quote_with_age(out_mint, lamports, slippage_bps)
    best_now = _extract_best(quote2) if quote2 else None
    if not best_now:
        await count_route_used(pre_meta.get("labels") or ["OTHER"])
        return False, "no_post_quote", {}

    now_meta = _route_meta(best_now)
    pre_out = float(pre_meta.get("out_amount", 0.0) or 0.0)
    now_out = float(now_meta.get("out_amount", 0.0) or 0.0)
    now_meta["deviation_bps_vs_pre"] = 0.0

    await count_route_used(pre_meta.get("labels") or now_meta.get("labels") or ["OTHER"])

    if pre_out <= 0 or now_out <= 0:
        return True, "insufficient_data", now_meta

    dev = (pre_out - now_out) / pre_out * 10000.0
    now_meta["deviation_bps_vs_pre"] = dev
    if dev > WATCHDOG_ALERT_DEVIATION_BPS:
        await count_watchdog_hit(pre_meta.get("labels") or now_meta.get("labels") or ["OTHER"])
        return False, f"post_deviation_bps:{dev:.0f}", now_meta
    return True, "ok", now_meta
