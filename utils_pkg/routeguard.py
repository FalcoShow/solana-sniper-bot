# utils_pkg/routeguard.py
from __future__ import annotations

import asyncio
import os
import time
from typing import Any, Dict, Optional, Tuple

from logging_setup import get_logger
from utils_pkg.router import get_buy_quote, get_sell_quote

log = get_logger()

# ──────────────────────────────────────────────────────────────────────────────
# Env / knobs
# ──────────────────────────────────────────────────────────────────────────────
def _envb(k: str, d: bool = False) -> bool:
    v = (os.getenv(k, "") or "").strip().lower()
    if not v:
        return d
    return v in {"1", "true", "yes", "y", "on"}

def _envi(k: str, d: int) -> int:
    try:
        return int(os.getenv(k, str(d)))
    except Exception:
        return int(d)

def _envf(k: str, d: float) -> float:
    try:
        return float(os.getenv(k, str(d)))
    except Exception:
        return float(d)

# Global policy
ALLOW_NO_BUY_ROUTE     = _envb("ROUTEGUARD_ALLOW_NO_ROUTE", True)
REQUIRE_SELL_ROUTE     = _envb("ROUTEGUARD_REQUIRE_SELL", True)

# Quote hygiene
QUOTE_TTL_SEC          = max(1, _envi("PF_PRICE_TTL_SEC", 5))     # reuse buy quotes for a few seconds
MAX_IMPACT_BPS         = max(0, _envi("ROUTEGUARD_MAX_IMPACT_BPS", 1500))  # ~15% price impact hard cap

# Cooldown to avoid hammering the same mint when no route
NOROUTE_COOLDOWN_SEC   = max(60, _envi("ROUTEGUARD_NOROUTE_COOLDOWN_SEC", 600))

# Sell route probe size (raw token units)
SELL_PROBE_RAW         = max(1_000, _envi("ROUTEGUARD_SELL_PROBE_RAW", 1_000))

# ──────────────────────────────────────────────────────────────────────────────
# Tiny in-memory cache for buy quotes (saves credits)
# ──────────────────────────────────────────────────────────────────────────────
# key: (mint, lamports_in) -> (ts, quote_dict)
_buy_cache: dict[tuple[str, int], tuple[float, Dict[str, Any]]] = {}
_noroute_until: dict[str, float] = {}

def _get_cached_buy(mint: str, lamports_in: int) -> Optional[Dict[str, Any]]:
    key = (mint, int(lamports_in))
    ts_q = _buy_cache.get(key)
    if not ts_q:
        return None
    ts, q = ts_q
    if (time.time() - ts) <= QUOTE_TTL_SEC:
        return q
    _buy_cache.pop(key, None)
    return None

def _put_cached_buy(mint: str, lamports_in: int, quote: Dict[str, Any]) -> None:
    _buy_cache[(mint, int(lamports_in))] = (time.time(), quote)

def clear_cache() -> None:
    _buy_cache.clear()
    _noroute_until.clear()

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _impact_bps(q: Dict[str, Any]) -> int:
    """
    Jupiter v6 returns priceImpactPct (fraction like 0.0123 for 1.23%).
    Convert to BPS for easier thresholding.
    """
    try:
        pip = float(q.get("priceImpactPct") or 0.0)
        return int(abs(pip) * 10_000)
    except Exception:
        return 0

def _cooldown_ok(mint: str) -> bool:
    return time.time() >= _noroute_until.get(mint, 0.0)

def _note_no_route(mint: str) -> None:
    _noroute_until[mint] = time.time() + NOROUTE_COOLDOWN_SEC

# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────
async def preflight(*, mint: str, lamports_in: int) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """
    Decide if we should attempt a Jupiter buy for `mint` with `lamports_in`.

    Returns:
      (True, quote)     → proceed with Jupiter using this quote (fresh or cached)
      (True, None)      → allowed to buy *without* a Jupiter route (use fallback)
      (False, None|q)   → blocked; do not buy (route missing/bad, or no-route cooldown)

    Policy:
      - Serve cached buy quotes for QUOTE_TTL_SEC to save credits.
      - If buy quote missing:
          - if ALLOW_NO_BUY_ROUTE → allow (return (True, None)) so PumpPortal fallback can run;
          - else block.
      - If REQUIRE_SELL_ROUTE → we also require a tiny sell quote to succeed.
      - Block routes with absurd price impact (> MAX_IMPACT_BPS).
      - Apply a per-mint no-route cooldown to reduce spam.
    """
    if not _cooldown_ok(mint):
        log.event("RG_COOLDOWN", mint=mint, secs_left=int(_noroute_until[mint] - time.time()))
        return False, None

    # 1) buy quote (cached then fresh)
    q_buy = _get_cached_buy(mint, lamports_in)
    if q_buy is None:
        try:
            q_buy = await get_buy_quote(mint=mint, lamports_in=int(lamports_in))
            _put_cached_buy(mint, lamports_in, q_buy)
            log.every(f"rg.buy.{mint}", n=12, msg="RG_BUY_QUOTE_OK",
                      mint=mint, lamports_in=int(lamports_in),
                      out_raw=int(q_buy.get("outAmount", 0)),
                      impact_bps=_impact_bps(q_buy))
        except Exception as e:
            if not ALLOW_NO_BUY_ROUTE:
                _note_no_route(mint)
                log.event("RG_BUY_QUOTE_FAIL_BLOCK", mint=mint, err=str(e))
                return False, None
            # allow fallback path (PumpPortal)
            log.event("RG_BUY_QUOTE_FAIL_ALLOW", mint=mint, err=str(e))
            return True, None

    # Sanity: non-zero outAmount
    out_raw = int(q_buy.get("outAmount", 0) or 0)
    if out_raw <= 0:
        if not ALLOW_NO_BUY_ROUTE:
            _note_no_route(mint)
            log.event("RG_NO_BUY_ROUTE", mint=mint)
            return False, None
        log.event("RG_NO_BUY_ROUTE_ALLOW", mint=mint)
        return True, None

    # Impact guard
    ibps = _impact_bps(q_buy)
    if MAX_IMPACT_BPS and ibps > MAX_IMPACT_BPS:
        _note_no_route(mint)
        log.event("RG_BLOCK_IMPACT", mint=mint, impact_bps=ibps, max_bps=MAX_IMPACT_BPS)
        return False, q_buy

    # 2) require a sell path?
    if REQUIRE_SELL_ROUTE:
        try:
            q_sell = await get_sell_quote(mint=mint, amount_raw=int(SELL_PROBE_RAW))
            lam = int(q_sell.get("outAmount", 0) or q_sell.get("lamports_out", 0))
            if lam <= 0:
                _note_no_route(mint)
                log.event("RG_SELL_ROUTE_FAIL", mint=mint)
                return False, q_buy
            log.every(f"rg.sell.{mint}", n=20, msg="RG_SELL_ROUTE_OK", mint=mint, lamports_out=lam)
        except Exception as e:
            _note_no_route(mint)
            log.event("RG_SELL_QUOTE_ERR", mint=mint, err=str(e))
            return False, q_buy

    # Passed all checks: allow Jupiter path with the buy quote we have
    return True, q_buy


__all__ = ["preflight", "clear_cache"]
