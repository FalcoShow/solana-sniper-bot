# ai_gate.py
# Lightweight “decision gate”:
# - safe comparisons (no None crashes)
# - entry price estimation (Jupiter buy quote, Birdeye fallback)
# - combine with honeypot report to accept/skip a candidate

from __future__ import annotations

import os
import base64
import logging
from typing import Any, Dict, Tuple, Optional

import aiohttp

# Reuse bits from honeypot
from utils_pkg.honeypot import (
    _get_session,
    BIRDEYE_API_URL,
    _headers_birdeye,
    JUPITER_QUOTE_URL,
    WSOL_MINT,
    SHARED_SOLANA_CLIENT,
    honeypot_check,
)

logger = logging.getLogger("ai_gate")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

# ---- Config ----
DEFAULT_SOL_IN_LAMPORTS = int(os.getenv("AIGATE_SOL_IN_LAMPORTS", str(1_000_000)))  # 0.001 SOL
MAX_PRICE_IMPACT_BPS = int(os.getenv("AIGATE_MAX_PRICE_IMPACT_BPS", "1000"))        # 10%
REQUIRE_HONEYPOT_OK = os.getenv("AIGATE_REQUIRE_HONEYPOT_OK", "true").lower() == "true"


# ---- safe compare helpers ----
def safe_float(x: Any) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

def safe_int(x: Any) -> Optional[int]:
    try:
        return int(x)
    except Exception:
        return None

def safe_lt(a: Any, b: Any) -> bool:
    fa, fb = safe_float(a), safe_float(b)
    return fa is not None and fb is not None and fa < fb

def safe_gt(a: Any, b: Any) -> bool:
    fa, fb = safe_float(a), safe_float(b)
    return fa is not None and fb is not None and fa > fb

def safe_le(a: Any, b: Any) -> bool:
    fa, fb = safe_float(a), safe_float(b)
    return fa is not None and fb is not None and fa <= fb

def safe_ge(a: Any, b: Any) -> bool:
    fa, fb = safe_float(a), safe_float(b)
    return fa is not None and fb is not None and fa >= fb


# ---- chain helpers ----
async def token_decimals(mint: str) -> int:
    """
    Best-effort read of SPL Mint.decimals. Falls back to 9 if unavailable.
    """
    try:
        if SHARED_SOLANA_CLIENT is None:
            return 9
        from solders.pubkey import Pubkey
        resp = await SHARED_SOLANA_CLIENT.get_account_info(Pubkey.from_string(mint))
        if not resp.value:
            return 9
        data_b64, _encoding = resp.value.data
        raw = base64.b64decode(data_b64)
        # SPL Mint layout: decimals is a single byte at offset 44 (classic SPL).
        dec = raw[44] if len(raw) > 44 else 9
        return int(dec)
    except Exception:
        return 9


# ---- quotes & prices ----
async def jup_quote_buy(mint: str, sol_in_lamports: int = DEFAULT_SOL_IN_LAMPORTS) -> Dict[str, Any] | None:
    """
    Quote BUY route: WSOL -> token (ExactIn).
    Returns Jupiter v6 quote JSON or None.
    """
    sess = await _get_session()
    params = {
        "inputMint": WSOL_MINT,
        "outputMint": mint,
        "amount": str(sol_in_lamports),
        "swapMode": "ExactIn",
        "onlyDirectRoutes": "false",
        "slippageBps": "300",
    }
    try:
        async with sess.get(JUPITER_QUOTE_URL, params=params) as resp:
            if resp.status != 200:
                txt = await resp.text()
                logger.debug(f"Jupiter buy quote {resp.status}: {txt}")
                return None
            return await resp.json()
    except Exception as e:
        logger.debug(f"jup_quote_buy error: {e}")
        return None


async def estimate_entry_sol_per_token(mint: str, sol_in_lamports: int = DEFAULT_SOL_IN_LAMPORTS) -> Optional[float]:
    """
    Prefer Jupiter buy quote to compute SOL/token.
    Fallback to Birdeye USD price for an approximation (skips if SOL-USD unknown here).
    """
    # 1) Jupiter route
    q = await jup_quote_buy(mint, sol_in_lamports)
    if q and q.get("outAmount"):
        dec = await token_decimals(mint)
        out_tokens = safe_float(safe_int(q["outAmount"]))
        if out_tokens and out_tokens > 0:
            tokens = out_tokens / (10 ** dec)
            sol_in = sol_in_lamports / 1_000_000_000  # lamports → SOL
            return sol_in / tokens if tokens > 0 else None

    # 2) Birdeye price fallback (USD) — only if you wire SOL-USD elsewhere; otherwise skip
    if BIRDEYE_API_URL:
        try:
            sess = await _get_session()
            async with sess.get(f"{BIRDEYE_API_URL}/public/price", params={"address": mint}, headers=_headers_birdeye()) as r:
                if r.status == 200:
                    js = await r.json()
                    usd = js.get("data", {}).get("value")
                    # If you keep SOL-USD in env, you can convert; otherwise return None
                    sol_usd = safe_float(os.getenv("AIGATE_SOL_USD"))
                    if usd is not None and sol_usd:
                        usd = safe_float(usd)
                        return (usd / sol_usd) if (usd and sol_usd) else None
        except Exception:
            pass

    return None


def _price_impact_bps(q: Dict[str, Any] | None) -> Optional[int]:
    if not q:
        return None
    v = q.get("priceImpactPct")
    f = safe_float(v)
    if f is None:
        return None
    # 1.0% => 100 bps; 0.10 => 10% in some payloads; v6 returns like 0.0123 (1.23%)
    # Treat f as fraction (0.0123), convert to bps:
    return int(round(f * 10_000))


# ---- public decision ----
async def evaluate_candidate(mint: str) -> Tuple[bool, Dict[str, Any]]:
    """
    Returns (ok, report). Never raises on missing data.
    """
    reasons: list[str] = []
    sources: Dict[str, Any] = {}

    # Honeypot/risk suite
    hp_ok, hp_rep = await honeypot_check(mint)
    sources["honeypot"] = hp_rep
    if REQUIRE_HONEYPOT_OK and not hp_ok:
        reasons.append("honeypot:not_ok")

    # Entry price + impact
    q = await jup_quote_buy(mint)
    sources["jup_buy_quote"] = q or {}
    entry_solin = await estimate_entry_sol_per_token(mint)
    sources["entry_solin"] = entry_solin

    impact_bps = _price_impact_bps(q)
    if impact_bps is not None and safe_gt(impact_bps, MAX_PRICE_IMPACT_BPS):
        reasons.append(f"impact_bps>{MAX_PRICE_IMPACT_BPS}")

    ok = (not REQUIRE_HONEYPOT_OK or hp_ok) and (impact_bps is None or impact_bps <= MAX_PRICE_IMPACT_BPS) and (entry_solin is not None)

    report = {
        "mint": mint,
        "ok": ok,
        "reasons": reasons,
        "entry_solin": entry_solin,
        "impact_bps": impact_bps,
        "sources": sources,
        "policy": {
            "require_honeypot_ok": REQUIRE_HONEYPOT_OK,
            "max_price_impact_bps": MAX_PRICE_IMPACT_BPS,
            "probe_sol_in_lamports": DEFAULT_SOL_IN_LAMPORTS,
        },
    }
    return ok, report
