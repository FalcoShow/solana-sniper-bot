# routeguard.py
# Minimal RouteGuard around Jupiter v6 quotes.
# - Checks BUY (WSOL -> token) and optional SELL (token -> WSOL)
# - Blocks when no route unless override is enabled

from __future__ import annotations

import os
import logging
from typing import Any, Dict, Optional

from utils_pkg.honeypot import _get_session, JUPITER_QUOTE_URL, WSOL_MINT

logger = logging.getLogger("routeguard")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

ROUTEGUARD_ALLOW_NO_ROUTE = os.getenv("ROUTEGUARD_ALLOW_NO_ROUTE", "0").strip() == "1"
ROUTEGUARD_REQUIRE_SELL = os.getenv("ROUTEGUARD_REQUIRE_SELL", "1").strip() == "1"
BUY_PROBE_LAMPORTS = int(os.getenv("ROUTEGUARD_BUY_PROBE_LAMPORTS", str(1_000_000)))     # 0.001 SOL
SELL_PROBE_UNITS   = int(os.getenv("ROUTEGUARD_SELL_PROBE_UNITS",   str(1_000_000)))     # 1e6 token base units

def _impact_bps(q: Dict[str, Any] | None) -> Optional[int]:
    if not q:
        return None
    try:
        v = float(q.get("priceImpactPct"))
        return int(round(v * 10_000))
    except Exception:
        return None

async def _jup_quote(params: Dict[str, str]) -> Dict[str, Any] | None:
    sess = await _get_session()
    try:
        async with sess.get(JUPITER_QUOTE_URL, params=params) as resp:
            if resp.status != 200:
                txt = await resp.text()
                logger.debug(f"Jupiter quote {resp.status}: {txt}")
                return None
            return await resp.json()
    except Exception as e:
        logger.debug(f"_jup_quote error: {e}")
        return None

async def check_routes(mint: str) -> Dict[str, Any]:
    """
    Returns a decision dict:
    {
      "action": "allow" | "block",
      "reason": str,
      "no_routes": bool,
      "hops": Optional[int],
      "impact_bps": Optional[int],
      "buy_quote": {...},
      "sell_quote": {...}
    }
    """
    # BUY: WSOL -> token
    buy_q = await _jup_quote({
        "inputMint": WSOL_MINT,
        "outputMint": mint,
        "amount": str(BUY_PROBE_LAMPORTS),
        "swapMode": "ExactIn",
        "onlyDirectRoutes": "false",
        "slippageBps": "300",
    })
    buy_plan = (buy_q or {}).get("routePlan") or []
    buy_hops = len(buy_plan) if buy_plan else None
    buy_impact = _impact_bps(buy_q)

    if not buy_plan:
        if ROUTEGUARD_ALLOW_NO_ROUTE:
            return {
                "action": "allow",
                "reason": "no_routes_allowed",
                "no_routes": True,
                "hops": None,
                "impact_bps": None,
                "buy_quote": buy_q or {},
                "sell_quote": {},
            }
        return {
            "action": "block",
            "reason": "no_routes",
            "no_routes": True,
            "hops": None,
            "impact_bps": None,
            "buy_quote": buy_q or {},
            "sell_quote": {},
        }

    # SELL: token -> WSOL (optional but recommended)
    sell_q = {}
    if ROUTEGUARD_REQUIRE_SELL:
        sell_q = await _jup_quote({
            "inputMint": mint,
            "outputMint": WSOL_MINT,
            "amount": str(SELL_PROBE_UNITS),
            "swapMode": "ExactIn",
            "onlyDirectRoutes": "false",
            "slippageBps": "300",
        }) or {}
        sell_plan = sell_q.get("routePlan") or []
        if not sell_plan:
            if ROUTEGUARD_ALLOW_NO_ROUTE:
                return {
                    "action": "allow",
                    "reason": "sell_no_routes_allowed",
                    "no_routes": True,
                    "hops": buy_hops,
                    "impact_bps": buy_impact,
                    "buy_quote": buy_q or {},
                    "sell_quote": sell_q or {},
                }
            return {
                "action": "block",
                "reason": "sell_no_routes",
                "no_routes": True,
                "hops": buy_hops,
                "impact_bps": buy_impact,
                "buy_quote": buy_q or {},
                "sell_quote": sell_q or {},
            }

    return {
        "action": "allow",
        "reason": "routed",
        "no_routes": False,
        "hops": buy_hops,
        "impact_bps": buy_impact,
        "buy_quote": buy_q or {},
        "sell_quote": sell_q or {},
    }
