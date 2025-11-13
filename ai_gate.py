# ai_gate.py
# Lightweight “decision gate” used by sniper/sniper.py:
# - Safe numeric comparisons (no None crashes)
# - Entry price estimation (Jupiter BUY quote, Birdeye fallback)
# - Combine with honeypot report to accept/skip a candidate
#
# Public contract:
#   async def evaluate_candidate(mint: str) -> tuple[bool, dict]
#
# The report dict always includes:
#   {
#       "mint": mint,
#       "ok": bool,
#       "reasons": [str, ...],
#       "entry_solin": float | None,   # SOL per token
#       "impact_bps": int | None,      # buy price impact in basis points
#       "sources": {...},              # raw sub-reports (honeypot, quotes, etc.)
#       "policy": {...},               # env-driven tuning
#   }
from __future__ import annotations

import os
import base64
import logging
from typing import Any, Dict, Tuple, Optional

import aiohttp

# Reuse bits from honeypot (single shared HTTP session + Solana client)
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
    logging.basicConfig(
        level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    )

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────

# Global toggle: if disabled, evaluate_candidate returns ok=True with a minimal report.
AIGATE_ENABLED = os.getenv("AIGATE_ENABLED", "true").strip().lower() in {"1", "true", "yes", "y", "on"}

# Probe amount for quotes (in lamports)
DEFAULT_SOL_IN_LAMPORTS = int(os.getenv("AIGATE_SOL_IN_LAMPORTS", str(1_000_000)))  # 0.001 SOL

# Max acceptable price impact (in basis points)
MAX_PRICE_IMPACT_BPS = int(os.getenv("AIGATE_MAX_PRICE_IMPACT_BPS", "1000"))        # 10%

# Slippage used for the quote itself
AIGATE_QUOTE_SLIPPAGE_BPS = int(os.getenv("AIGATE_QUOTE_SLIPPAGE_BPS", "300"))      # 3%

# Whether honeypot must be strictly OK
REQUIRE_HONEYPOT_OK = os.getenv("AIGATE_REQUIRE_HONEYPOT_OK", "true").lower() in {"1", "true", "yes", "y", "on"}

# Optional SOL-USD override (for Birdeye fallback)
AIGATE_SOL_USD = os.getenv("AIGATE_SOL_USD")  # if set, used to convert Birdeye USD price to SOL


# ──────────────────────────────────────────────────────────────────────────────
# Safe numeric helpers
# ──────────────────────────────────────────────────────────────────────────────

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


# ──────────────────────────────────────────────────────────────────────────────
# Chain helpers
# ──────────────────────────────────────────────────────────────────────────────

async def token_decimals(mint: str) -> int:
    """
    Best-effort read of SPL Mint.decimals via SHARED_SOLANA_CLIENT.
    Falls back to 9 if unavailable or on any error.
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
    except Exception as e:
        logger.debug("token_decimals(%s) error: %s", mint, e)
        return 9


# ──────────────────────────────────────────────────────────────────────────────
# Jupiter quotes & price impact
# ──────────────────────────────────────────────────────────────────────────────

async def jup_quote_buy(
    mint: str,
    sol_in_lamports: int = DEFAULT_SOL_IN_LAMPORTS,
) -> Dict[str, Any] | None:
    """
    Quote BUY route: WSOL -> token (ExactIn).
    Returns Jupiter v6 quote JSON or None.
    """
    try:
        sess: aiohttp.ClientSession = await _get_session()
    except Exception as e:
        logger.debug("jup_quote_buy: failed to get session: %s", e)
        return None

    params = {
        "inputMint": WSOL_MINT,
        "outputMint": mint,
        "amount": str(sol_in_lamports),
        "swapMode": "ExactIn",
        "onlyDirectRoutes": "false",
        "slippageBps": str(AIGATE_QUOTE_SLIPPAGE_BPS),
    }
    try:
        async with sess.get(JUPITER_QUOTE_URL, params=params) as resp:
            text = await resp.text()
            if resp.status != 200:
                logger.debug("Jupiter buy quote %s for %s: %s", resp.status, mint, text[:160])
                return None
            try:
                return await resp.json()
            except Exception as je:
                logger.debug("jup_quote_buy JSON error for %s: %s (text=%r)", mint, je, text[:160])
                return None
    except Exception as e:
        logger.debug("jup_quote_buy(%s) error: %s", mint, e)
        return None


def _price_impact_bps(q: Dict[str, Any] | None) -> Optional[int]:
    """
    Convert Jupiter quote priceImpactPct into basis points (bps).
    v6 usually returns a fraction (0.0123 ~ 1.23%).
    """
    if not q:
        return None
    v = q.get("priceImpactPct")
    f = safe_float(v)
    if f is None:
        return None
    # Treat f as fraction (0.0123 => 1.23% => 123 bps)
    return int(round(f * 10_000))


def _entry_from_quote(
    quote: Dict[str, Any] | None,
    mint_decimals: int,
    sol_in_lamports: int,
) -> Optional[float]:
    """
    Compute SOL per token (entry price) from a Jupiter BUY quote.
    """
    if not quote:
        return None
    out_raw = quote.get("outAmount")
    out_i = safe_int(out_raw)
    if not out_i or out_i <= 0:
        return None

    tokens = out_i / (10 ** mint_decimals)
    if tokens <= 0:
        return None

    sol_in = sol_in_lamports / 1_000_000_000  # lamports -> SOL
    return sol_in / tokens if tokens > 0 else None


async def _birdeye_entry_sol_per_token(mint: str) -> Optional[float]:
    """
    Fallback: use Birdeye USD price and a SOL-USD override to estimate SOL/token.
    Requires:
      - BIRDEYE_API_URL set (handled via honeypot module)
      - AIGATE_SOL_USD env set to current SOL-USD price
    """
    if not BIRDEYE_API_URL or not AIGATE_SOL_USD:
        return None

    sol_usd = safe_float(AIGATE_SOL_USD)
    if not sol_usd or sol_usd <= 0:
        return None

    try:
        sess: aiohttp.ClientSession = await _get_session()
    except Exception:
        return None

    try:
        async with sess.get(
            f"{BIRDEYE_API_URL}/public/price",
            params={"address": mint},
            headers=_headers_birdeye(),
        ) as r:
            if r.status != 200:
                return None
            js = await r.json()
            usd = js.get("data", {}).get("value")
            usd = safe_float(usd)
            if not usd or usd <= 0:
                return None
            # usd/token divided by usd/SOL => SOL/token
            return usd / sol_usd
    except Exception as e:
        logger.debug("birdeye fallback error for %s: %s", mint, e)
        return None


async def estimate_entry_sol_per_token(
    mint: str,
    sol_in_lamports: int = DEFAULT_SOL_IN_LAMPORTS,
    quote: Dict[str, Any] | None = None,
) -> Optional[float]:
    """
    Estimate SOL/token entry price.
    Prefer a provided Jupiter quote; if none, fetch one.
    Fallback to Birdeye (requires AIGATE_SOL_USD).
    """
    # 1) Jupiter route
    if quote is None:
        quote = await jup_quote_buy(mint, sol_in_lamports)

    if quote and quote.get("outAmount"):
        dec = await token_decimals(mint)
        entry = _entry_from_quote(quote, dec, sol_in_lamports)
        if entry is not None and entry > 0:
            return entry

    # 2) Birdeye price fallback (USD)
    fallback = await _birdeye_entry_sol_per_token(mint)
    if fallback and fallback > 0:
        return fallback

    return None


# ──────────────────────────────────────────────────────────────────────────────
# Public decision gate
# ──────────────────────────────────────────────────────────────────────────────

async def evaluate_candidate(mint: str) -> Tuple[bool, Dict[str, Any]]:
    """
    Core entry gate used in sniper/sniper.py::_consider.

    Returns:
        (ok, report)

    - ok: whether the candidate passes gate thresholds
    - report: structured info used by the caller (entry price, impact, reasons, sources)

    Never raises; on any error it returns ok=False with reasons filled, or ok=True if
    the entire AI gate is disabled via AIGATE_ENABLED=0.
    """
    mint = (mint or "").strip()
    reasons: list[str] = []
    sources: Dict[str, Any] = {}

    # If gate is disabled, short-circuit with a minimal report.
    if not AIGATE_ENABLED:
        report = {
            "mint": mint,
            "ok": True,
            "reasons": ["aigate_disabled"],
            "entry_solin": None,
            "impact_bps": None,
            "sources": {},
            "policy": {
                "enabled": False,
                "require_honeypot_ok": REQUIRE_HONEYPOT_OK,
                "max_price_impact_bps": MAX_PRICE_IMPACT_BPS,
                "probe_sol_in_lamports": DEFAULT_SOL_IN_LAMPORTS,
                "quote_slippage_bps": AIGATE_QUOTE_SLIPPAGE_BPS,
            },
        }
        return True, report

    try:
        # 1) Honeypot / safety report (full suite lives in utils_pkg.honeypot)
        try:
            hp_ok, hp_rep = await honeypot_check(mint)
        except Exception as e:
            logger.debug("honeypot_check error for %s: %s", mint, e)
            hp_ok, hp_rep = False, {"error": str(e)}

        sources["honeypot"] = hp_rep
        if REQUIRE_HONEYPOT_OK and not hp_ok:
            reasons.append("honeypot:not_ok")

        # 2) Jupiter BUY quote (impact + entry)
        q = await jup_quote_buy(mint, DEFAULT_SOL_IN_LAMPORTS)
        sources["jup_buy_quote"] = q or {}

        impact_bps = _price_impact_bps(q)
        if impact_bps is not None and safe_gt(impact_bps, MAX_PRICE_IMPACT_BPS):
            reasons.append(f"impact_bps>{MAX_PRICE_IMPACT_BPS}")

        # 3) Entry price (SOL/token)
        entry_solin = await estimate_entry_sol_per_token(
            mint,
            DEFAULT_SOL_IN_LAMPORTS,
            quote=q,
        )
        sources["entry_solin"] = entry_solin

        if entry_solin is None:
            reasons.append("no_entry_price")

        # 4) Decision
        impact_ok = (impact_bps is None) or (impact_bps <= MAX_PRICE_IMPACT_BPS)
        hp_ok_final = (not REQUIRE_HONEYPOT_OK) or hp_ok
        entry_ok = entry_solin is not None and entry_solin > 0

        ok = hp_ok_final and impact_ok and entry_ok

        report = {
            "mint": mint,
            "ok": ok,
            "reasons": reasons,
            "entry_solin": entry_solin,
            "impact_bps": impact_bps,
            "sources": sources,
            "policy": {
                "enabled": True,
                "require_honeypot_ok": REQUIRE_HONEYPOT_OK,
                "max_price_impact_bps": MAX_PRICE_IMPACT_BPS,
                "probe_sol_in_lamports": DEFAULT_SOL_IN_LAMPORTS,
                "quote_slippage_bps": AIGATE_QUOTE_SLIPPAGE_BPS,
            },
        }
        return ok, report

    except Exception as e:
        # Absolute safety net: never let the gate crash the sniper loop.
        logger.warning("evaluate_candidate(%s) fatal error: %s", mint, e)
        report = {
            "mint": mint,
            "ok": False,
            "reasons": ["exception"],
            "entry_solin": None,
            "impact_bps": None,
            "sources": {"error": str(e)},
            "policy": {
                "enabled": bool(AIGATE_ENABLED),
                "require_honeypot_ok": REQUIRE_HONEYPOT_OK,
                "max_price_impact_bps": MAX_PRICE_IMPACT_BPS,
                "probe_sol_in_lamports": DEFAULT_SOL_IN_LAMPORTS,
                "quote_slippage_bps": AIGATE_QUOTE_SLIPPAGE_BPS,
            },
        }
        return False, report
