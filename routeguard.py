# routeguard.py
# Minimal-but-robust RouteGuard around Jupiter v6 quotes.
#
# Responsibilities:
#   - Probe BUY route  (WSOL -> token)
#   - Optionally probe SELL route (token -> WSOL)
#   - Decide "allow" vs "block" based on:
#       * presence of routes
#       * optional max hops
#       * optional max price impact (bps)
#
# Public contract (used by sniper/sniper.py):
#   async def check_routes(mint: str) -> dict
#
# Returned dict shape:
#   {
#     "action": "allow" | "block",
#     "reason": str,
#     "no_routes": bool,           # True if we saw no usable routes
#     "hops": int | None,          # BUY-side hop count
#     "impact_bps": int | None,    # BUY-side price impact in basis points
#     "buy_quote": dict,           # raw Jupiter BUY quote (or {})
#     "sell_quote": dict,          # raw Jupiter SELL quote (or {})
#     "policy": { ... }            # snapshot of env-driven thresholds
#   }
#
from __future__ import annotations

import os
import logging
from typing import Any, Dict, Optional

from utils_pkg.honeypot import _get_session, JUPITER_QUOTE_URL, WSOL_MINT

logger = logging.getLogger("routeguard")
if not logger.handlers:
    logging.basicConfig(
        level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    )

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────

def _env_true(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return int(default)


# Allow trading even when Jupiter reports "no route"
ROUTEGUARD_ALLOW_NO_ROUTE = _env_true("ROUTEGUARD_ALLOW_NO_ROUTE", False)

# Require SELL route (token -> WSOL) in addition to BUY route?
ROUTEGUARD_REQUIRE_SELL = _env_true("ROUTEGUARD_REQUIRE_SELL", True)

# Probe sizes
BUY_PROBE_LAMPORTS = _env_int("ROUTEGUARD_BUY_PROBE_LAMPORTS", 1_000_000)   # 0.001 SOL
SELL_PROBE_UNITS   = _env_int("ROUTEGUARD_SELL_PROBE_UNITS",   1_000_000)   # 1e6 token base units

# Slippage used when probing routes
ROUTEGUARD_QUOTE_SLIPPAGE_BPS = _env_int("ROUTEGUARD_QUOTE_SLIPPAGE_BPS", 300)

# Optional caps (0 => disabled)
ROUTEGUARD_MAX_HOPS = _env_int("ROUTEGUARD_MAX_HOPS", 0)          # e.g. 3 to avoid long paths
ROUTEGUARD_MAX_IMPACT_BPS = _env_int("ROUTEGUARD_MAX_IMPACT_BPS", 0)  # e.g. 1500 (15%)


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _safe_float(x: Any) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None


def _impact_bps(q: Dict[str, Any] | None) -> Optional[int]:
    """
    Convert Jupiter v6 priceImpactPct => basis points.
    Expecting a fraction (0.0123 ~ 1.23%).
    """
    if not q:
        return None
    try:
        v = _safe_float(q.get("priceImpactPct"))
        if v is None:
            return None
        return int(round(v * 10_000))
    except Exception:
        return None


def _routeplan_and_hops(q: Dict[str, Any] | None) -> tuple[list[dict], Optional[int]]:
    """
    Extract routePlan list and hop count from a Jupiter quote.
    """
    if not q:
        return [], None
    plan = q.get("routePlan") or []
    if not isinstance(plan, list) or not plan:
        return [], None
    return plan, len(plan)


async def _jup_quote(params: Dict[str, str]) -> Dict[str, Any] | None:
    """
    Thin wrapper to hit the shared Jupiter quote endpoint.
    Never raises; returns None on error.
    """
    try:
        sess = await _get_session()
    except Exception as e:
        logger.debug("_jup_quote: failed to get session: %s", e)
        return None

    try:
        async with sess.get(JUPITER_QUOTE_URL, params=params) as resp:
            text = await resp.text()
            if resp.status != 200:
                logger.debug("Jupiter quote %s: %s", resp.status, text[:160])
                return None
            try:
                return await resp.json()
            except Exception as je:
                logger.debug("Jupiter quote JSON error: %s (text=%r)", je, text[:160])
                return None
    except Exception as e:
        logger.debug("_jup_quote error: %s", e)
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────

async def check_routes(mint: str) -> Dict[str, Any]:
    """
    Returns a decision dict, never raises.

    {
      "action": "allow" | "block",
      "reason": str,
      "no_routes": bool,
      "hops": Optional[int],
      "impact_bps": Optional[int],
      "buy_quote": {...},
      "sell_quote": {...},
      "policy": {...},
    }
    """
    mint = (mint or "").strip()
    reason = "unknown"
    no_routes = False
    buy_quote: Dict[str, Any] = {}
    sell_quote: Dict[str, Any] = {}
    buy_hops: Optional[int] = None
    buy_impact: Optional[int] = None

    # BUY: WSOL -> token
    buy_quote = await _jup_quote(
        {
            "inputMint": WSOL_MINT,
            "outputMint": mint,
            "amount": str(BUY_PROBE_LAMPORTS),
            "swapMode": "ExactIn",
            "onlyDirectRoutes": "false",
            "slippageBps": str(ROUTEGUARD_QUOTE_SLIPPAGE_BPS),
        }
    ) or {}

    buy_plan, buy_hops = _routeplan_and_hops(buy_quote)
    buy_impact = _impact_bps(buy_quote)

    if not buy_plan:
        no_routes = True
        if ROUTEGUARD_ALLOW_NO_ROUTE:
            reason = "no_routes_allowed"
            decision = "allow"
        else:
            reason = "no_routes"
            decision = "block"
    else:
        # We have at least a BUY route; check optional caps
        decision = "allow"
        reason = "routed"

        if ROUTEGUARD_MAX_HOPS > 0 and buy_hops is not None and buy_hops > ROUTEGUARD_MAX_HOPS:
            reason = f"too_many_hops>{ROUTEGUARD_MAX_HOPS}"
            decision = "block"

        if (
            decision == "allow"
            and ROUTEGUARD_MAX_IMPACT_BPS > 0
            and buy_impact is not None
            and buy_impact > ROUTEGUARD_MAX_IMPACT_BPS
        ):
            reason = f"impact_bps>{ROUTEGUARD_MAX_IMPACT_BPS}"
            decision = "block"

    # SELL: token -> WSOL (optional but recommended)
    if decision == "allow" and ROUTEGUARD_REQUIRE_SELL:
        sell_quote = await _jup_quote(
            {
                "inputMint": mint,
                "outputMint": WSOL_MINT,
                "amount": str(SELL_PROBE_UNITS),
                "swapMode": "ExactIn",
                "onlyDirectRoutes": "false",
                "slippageBps": str(ROUTEGUARD_QUOTE_SLIPPAGE_BPS),
            }
        ) or {}
        sell_plan, _sell_hops = _routeplan_and_hops(sell_quote)
        if not sell_plan:
            no_routes = True
            if ROUTEGUARD_ALLOW_NO_ROUTE:
                reason = "sell_no_routes_allowed"
                # decision stays "allow"
            else:
                reason = "sell_no_routes"
                decision = "block"

    out = {
        "action": decision,
        "reason": reason,
        "no_routes": bool(no_routes),
        "hops": buy_hops,
        "impact_bps": buy_impact,
        "buy_quote": buy_quote,
        "sell_quote": sell_quote,
        "policy": {
            "allow_no_route": ROUTEGUARD_ALLOW_NO_ROUTE,
            "require_sell": ROUTEGUARD_REQUIRE_SELL,
            "buy_probe_lamports": BUY_PROBE_LAMPORTS,
            "sell_probe_units": SELL_PROBE_UNITS,
            "quote_slippage_bps": ROUTEGUARD_QUOTE_SLIPPAGE_BPS,
            "max_hops": ROUTEGUARD_MAX_HOPS,
            "max_impact_bps": ROUTEGUARD_MAX_IMPACT_BPS,
        },
    }

    # Lightweight debug breadcrumb for logs
    logger.debug(
        "RouteGuard %s: %s (%s) hops=%s impact_bps=%s no_routes=%s",
        decision.upper(),
        mint,
        reason,
        buy_hops,
        buy_impact,
        no_routes,
    )
    return out


__all__ = ["check_routes"]
