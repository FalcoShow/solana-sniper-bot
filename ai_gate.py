# ai_gate.py
# Lightweight “decision + AI gate”:
# - uses honeypot_check as the main risk source
# - best-effort Jupiter BUY quote for price impact & entry estimation
# - optional AI risk model (your fine-tuned sniper-risk-v1) to veto bad setups
#
# Result:
#   ok, report = await evaluate_candidate(mint)
#   - ok: bool (should we consider this mint for entry?)
#   - report: full diagnostic dict (sources.honeypot / .jup_buy_quote / .entry_solin / .ai / etc.)

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
from typing import Any, Dict, Optional, Tuple

from openai import OpenAI

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

from config import OPENAI_API_KEY, OPENAI_MODEL, FINE_TUNED_MODEL

logger = logging.getLogger("ai_gate")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

# ──────────────────────────────────────────────────────────────────────────────
# Core gate config
# ──────────────────────────────────────────────────────────────────────────────

DEFAULT_SOL_IN_LAMPORTS = int(
    os.getenv("AIGATE_SOL_IN_LAMPORTS", str(1_000_000))
)  # 0.001 SOL
MAX_PRICE_IMPACT_BPS = int(
    os.getenv("AIGATE_MAX_PRICE_IMPACT_BPS", "1000")
)  # 10%
REQUIRE_HONEYPOT_OK = os.getenv("AIGATE_REQUIRE_HONEYPOT_OK", "true").lower() in {
    "1",
    "true",
    "yes",
    "on",
}
AIGATE_ENABLED = os.getenv("AIGATE_ENABLED", "1").lower() in {
    "1",
    "true",
    "yes",
    "on",
}
AIGATE_REQUIRE_ENTRY_PRICE = os.getenv("AIGATE_REQUIRE_ENTRY_PRICE", "0").lower() in {
    "1",
    "true",
    "yes",
    "on",
}

QUOTE_SLIPPAGE_BPS = int(os.getenv("AIGATE_QUOTE_SLIPPAGE_BPS", "300"))  # 3% probe

# ──────────────────────────────────────────────────────────────────────────────
# AI config (fine-tuned sniper-risk model)
# ──────────────────────────────────────────────────────────────────────────────

_AI_MODEL = FINE_TUNED_MODEL or OPENAI_MODEL
_client: Optional[OpenAI] = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
_AI_ENABLED = bool(_client and _AI_MODEL)

# How confident AI must be that a setup is BAD to hard-reject
AI_BAD_THRESHOLD = float(os.getenv("AI_BAD_THRESHOLD", "0.70"))
# (reserved for future: boost good setups or position sizing)
AI_GOOD_THRESHOLD = float(os.getenv("AI_GOOD_THRESHOLD", "0.70"))

# ──────────────────────────────────────────────────────────────────────────────
# Safe compare helpers
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


# ──────────────────────────────────────────────────────────────────────────────
# Jupiter quotes & entry price estimation
# ──────────────────────────────────────────────────────────────────────────────


async def jup_quote_buy(
    mint: str, sol_in_lamports: int = DEFAULT_SOL_IN_LAMPORTS
) -> Dict[str, Any] | None:
    """
    Quote BUY route: WSOL -> token (ExactIn).
    Returns Jupiter quote JSON or None.
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


async def estimate_entry_sol_per_token(
    mint: str, sol_in_lamports: int = DEFAULT_SOL_IN_LAMPORTS
) -> Optional[float]:
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
            tokens = out_tokens / (10**dec)
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


# ──────────────────────────────────────────────────────────────────────────────
# AI risk scoring helpers
# ──────────────────────────────────────────────────────────────────────────────


def _build_ai_prompt(context: Dict[str, Any]) -> str:
    """
    Turn our candidate context into a compact text description for the fine-tuned model.
    This can evolve as we log more features over time.
    """
    mint = context.get("mint")
    liq = context.get("liquidity_usd")
    impact = context.get("impact_bps")
    reasons = context.get("reasons", []) or []

    sources = context.get("sources", {}) or {}
    hp = sources.get("honeypot", {}) or {}
    hp_reasons = hp.get("reasons", []) or []

    parts = [
        "You are an AI risk and PnL assistant for a Solana meme coin sniper bot.",
        "Given the candidate context, decide if similar future trades are GOOD, BAD, or NEUTRAL.",
        "",
        f"Mint: {mint}",
        f"Approx liquidity USD: {liq}",
        f"Estimated price impact (bps) for entry: {impact}",
        f"Existing reasons/flags: {', '.join(reasons + hp_reasons) or 'none'}",
        "",
        "Respond ONLY with JSON: "
        '{"label": "GOOD|BAD|NEUTRAL", "score": number between 0 and 1}.',
    ]
    return "\n".join(parts)


def _ai_risk_score_sync(context: Dict[str, Any]) -> Dict[str, Any] | None:
    """
    Synchronous call to the fine-tuned model.
    Returns dict with {label, score, raw} or None if AI is disabled / fails.
    NEVER raises.
    """
    if not _AI_ENABLED:
        return None

    prompt = _build_ai_prompt(context)

    try:
        resp = _client.responses.create(  # type: ignore[union-attr]
            model=_AI_MODEL,
            input=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": prompt,
                        }
                    ],
                }
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "RiskLabel",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "label": {
                                "type": "string",
                                "enum": ["GOOD", "BAD", "NEUTRAL"],
                            },
                            "score": {"type": "number"},
                        },
                        "required": ["label", "score"],
                        "additionalProperties": True,
                    },
                    "strict": True,
                },
            },
            temperature=0.1,
        )

        # For json_schema, the parsed JSON is placed here:
        parsed = resp.output[0].parsed  # type: ignore[index, attr-defined]

        label = str(parsed.get("label", "NEUTRAL")).upper()
        score = float(parsed.get("score", 0.5))

        return {
            "model": _AI_MODEL,
            "label": label,
            "score": score,
            "raw": parsed,
        }
    except Exception as e:  # pragma: no cover - defensive
        logger.debug(f"AI risk call failed: {e}")
        return {"error": str(e), "model": _AI_MODEL}


# ──────────────────────────────────────────────────────────────────────────────
# Public decision API
# ──────────────────────────────────────────────────────────────────────────────


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
      - AI layer (if enabled):
          * If AI says BAD with score >= AI_BAD_THRESHOLD → hard reject.
          * Otherwise AI is advisory and attached under sources.ai.
    """
    sources: Dict[str, Any] = {}
    reasons: list[str] = []

    # Disabled gate still runs honeypot for logging
    if not AIGATE_ENABLED:
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
                "ai_enabled": _AI_ENABLED,
                "ai_bad_threshold": AI_BAD_THRESHOLD,
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

    if (
        impact_bps is not None
        and MAX_PRICE_IMPACT_BPS > 0
        and safe_gt(impact_bps, MAX_PRICE_IMPACT_BPS)
    ):
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

    # 4) Base rule decision (before AI)
    ok_honeypot = (not REQUIRE_HONEYPOT_OK) or hp_ok
    ok_impact = (
        (MAX_PRICE_IMPACT_BPS <= 0)
        or (impact_bps is None)
        or (impact_bps <= MAX_PRICE_IMPACT_BPS)
    )
    ok_entry = (not AIGATE_REQUIRE_ENTRY_PRICE) or (entry_solin is not None)

    ok = ok_honeypot and ok_impact and ok_entry

    # 5) AI risk layer (non-fatal)
    ai_info = None
    if _AI_ENABLED:
        # Build compact context for the model
        ai_context: Dict[str, Any] = {
            "mint": mint,
            "liquidity_usd": None,
            "impact_bps": impact_bps,
            "reasons": list(reasons),
            "sources": dict(sources),
        }

        # Try to grab rough liquidity from honeypot/dexscreener
        hp_src = ai_context["sources"].get("honeypot", {}) or {}
        hp_sources = hp_src.get("sources", {}) or {}
        ds = hp_sources.get("dexscreener", {}) or {}
        liq = ds.get("liquidity_usd")
        if liq is not None:
            ai_context["liquidity_usd"] = liq

        # Run sync AI call in a thread so we don't block the event loop
        ai_info = await asyncio.to_thread(_ai_risk_score_sync, ai_context)
        if ai_info:
            sources["ai"] = ai_info
            label = str(ai_info.get("label", "NEUTRAL")).upper()
            score = safe_float(ai_info.get("score", 0.5)) or 0.5

            # Hard reject if AI is confident this is BAD
            if label == "BAD" and score >= AI_BAD_THRESHOLD:
                reasons.append(f"ai_gate:BAD:{score:.2f}")
                ok = False

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
            "ai_enabled": _AI_ENABLED,
            "ai_model": _AI_MODEL,
            "ai_bad_threshold": AI_BAD_THRESHOLD,
            "ai_good_threshold": AI_GOOD_THRESHOLD,
        },
    }
    return ok, report
