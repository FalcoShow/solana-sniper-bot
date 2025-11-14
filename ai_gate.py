# ai_gate.py
# Lightweight “decision gate”:
# - uses honeypot_check as the main risk source
# - best-effort Jupiter BUY quote for price impact & entry estimation
# - treats Jupiter/network issues as SOFT by default
#
# Result:
#   ok, report = await evaluate_candidate(mint)
#   - ok: bool (should we consider this mint for entry?)
#   - report: full diagnostic dict

from __future__ import annotations

import os
import base64
import logging
from typing import Any, Dict, Tuple, Optional

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
REQUIRE_HONEYPOT_OK = os.getenv("AIGATE_REQUIRE_HONEYPOT_OK", "true").lower() in {"1", "true", "yes", "on"}
AIGATE_ENABLED = os.getenv("AIGATE_ENABLED", "1").lower() in {"1", "true", "yes", "on"}
AIGATE_REQUIRE_ENTRY_PRICE = os.getenv("AIGATE_REQUIRE_ENTRY_PRICE", "0").lower() in {"1", "true", "yes", "on"}

QUOTE_SLIPPAGE_BPS = int(os.getenv("AIGATE_QUOTE_SLIPPAGE_BPS", "300"))  # 3% probe


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
    SOFT on HTTP/network errors – callers must handle None.
    """
    sess = await _get_session()
    params = {
        "inputMint": WSOL_MINT,
        "outputMint": mint,
        "amount": str(sol_in_lamports),
        "swapMode": "ExactIn",
        "onlyDirectRoutes": "false",
        "slippageBps": str(QUOTE_SLIPPAGE_BPS),
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
    Fallback to Birdeye USD price for an approximation (skips if SOL-USD unknown).
    Returns None on failure (SOFT).
    """
    # 1) Jupiter route
    q = await jup_quote_buy(mint, sol_in_lamports)
    if q and q.get("outAmount"):
        dec = await token_decimals(mint)
        out_tokens_raw = safe_int(q["outAmount"])
        out_tokens = safe_float(out_tokens_raw)
        if out_tokens and out_tokens > 0:
            tokens = out_tokens / (10 ** dec)
            sol_in = sol_in_lamports / 1_000_000_000  # lamports → SOL
            return sol_in / tokens if tokens > 0 else None

    # 2) Birdeye price fallback (USD) — only if you wire SOL-USD elsewhere
    if BIRDEYE_API_URL:
        try:
            sess = await _get_session()
            async with sess.get(
                f"{BIRDEYE_API_URL}/public/price",
                params={"address": mint},
                headers=_headers_birdeye(),
            ) as r:
                if r.status == 200:
                    js = await r.json()
                    usd = js.get("data", {}).get("value")
                    sol_usd = safe_float(os.getenv("AIGATE_SOL_USD"))
                    if usd is not None and sol_usd:
                        usd = safe_float(usd)
                        return (usd / sol_usd) if (usd and sol_usd) else None
        except Exception as e:
            logger.debug(f"Birdeye fallback error: {e}")

    return None


def _price_impact_bps(q: Dict[str, Any] | None) -> Optional[int]:
    if not q:
        return None
    v = q.get("priceImpactPct")
    f = safe_float(v)
    if f is None:
        return None
    # fraction (e.g. 0.0123 = 1.23%) → bps
    return int(round(f * 10_000))


# ---- public decision ----
async def evaluate_candidate(mint: str) -> Tuple[bool, Dict[str, Any]]:
    """
    Returns (ok, report). Never raises on missing data.

    Policy (default):
      - If AIGATE_ENABLED=0 → always ok, just echo honeypot report.
      - Honeypot must be ok if REQUIRE_HONEYPOT_OK=1.
      - Price impact must be <= MAX_PRICE_IMPACT_BPS if we can read it.
      - Entry price:
          * If AIGATE_REQUIRE_ENTRY_PRICE=1 → required (None blocks).
          * If 0 (default) → missing price is SOFT (does NOT block).
    """
    sources: Dict[str, Any] = {}
    reasons: list[str] = []

    if not AIGATE_ENABLED:
        # Gate is effectively turned off, but we still run honeypot for logging.
        hp_ok, hp_rep = await honeypot_check(mint)
        sources["honeypot"] = hp_rep
        report = {
            "mint": mint,
            "ok": True,
            "reasons": ["aigate:disabled"],
            "entry_solin": None,
            "impact_bps": None,
            "sources": sources,
            "policy": {
                "enabled": False,
                "require_honeypot_ok": REQUIRE_HONEYPOT_OK,
                "max_price_impact_bps": MAX_PRICE_IMPACT_BPS,
                "probe_sol_in_lamports": DEFAULT_SOL_IN_LAMPORTS,
                "quote_slippage_bps": QUOTE_SLIPPAGE_BPS,
                "require_entry_price": AIGATE_REQUIRE_ENTRY_PRICE,
            },
        }
        return True, report

    # 1) Honeypot / risk suite
    hp_ok, hp_rep = await honeypot_check(mint)
    sources["honeypot"] = hp_rep
    if REQUIRE_HONEYPOT_OK and not hp_ok:
        reasons.append("honeypot:not_ok")

    # 2) Jupiter buy quote (soft)
    q = await jup_quote_buy(mint)
    sources["jup_buy_quote"] = q or {}
    impact_bps = _price_impact_bps(q)

    if impact_bps is not None and MAX_PRICE_IMPACT_BPS > 0 and safe_gt(impact_bps, MAX_PRICE_IMPACT_BPS):
        reasons.append(f"impact_bps>{MAX_PRICE_IMPACT_BPS}")

    # 3) Entry price (soft or strict depending on env)
    entry_solin = await estimate_entry_sol_per_token(mint)
    sources["entry_solin"] = entry_solin

    if entry_solin is None:
        if AIGATE_REQUIRE_ENTRY_PRICE:
            reasons.append("entry:none")
        else:
            # Soft: we *note* lack of entry price, but do not block.
            if q is None:
                reasons.append("jup:no_quote_soft")
            else:
                reasons.append("entry:none_soft")

    # 4) Final decision
    ok_honeypot = (not REQUIRE_HONEYPOT_OK) or hp_ok
    ok_impact = (MAX_PRICE_IMPACT_BPS <= 0) or (impact_bps is None) or (impact_bps <= MAX_PRICE_IMPACT_BPS)
    ok_entry = (not AIGATE_REQUIRE_ENTRY_PRICE) or (entry_solin is not None)

    ok = ok_honeypot and ok_impact and ok_entry

    report = {
        "mint": mint,
        "ok": ok,
        "reasons": reasons,
        "entry_solin": entry_solin,
        "impact_bps": impact_bps,
        "sources": sources,
        "policy": {
            "enabled": AIGATE_ENABLED,
            "require_honeypot_ok": REQUIRE_HONEYPOT_OK,
            "max_price_impact_bps": MAX_PRICE_IMPACT_BPS,
            "probe_sol_in_lamports": DEFAULT_SOL_IN_LAMPORTS,
            "quote_slippage_bps": QUOTE_SLIPPAGE_BPS,
            "require_entry_price": AIGATE_REQUIRE_ENTRY_PRICE,
        },
    }
    return ok, report
