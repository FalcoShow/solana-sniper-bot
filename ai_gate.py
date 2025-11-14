# ai_gate.py
# Lightweight “decision gate”:
# - uses honeypot_check as the main risk source
# - best-effort Jupiter BUY quote for price impact & entry estimation
# - optional AI layer (fine-tuned model) to classify setups as GOOD/BAD/NEUTRAL
#
# Usage:
#   ok, report = await evaluate_candidate(mint)
#
#   ok: bool  -> should we consider this mint for entry?
#   report: dict with:
#       - reasons: list[str]
#       - entry_solin: Optional[float]
#       - impact_bps: Optional[int]
#       - sources: { honeypot, jup_buy_quote, entry_solin, ai? }
#       - policy: snapshot of thresholds

from __future__ import annotations

import os
import json
import base64
import logging
import asyncio
from typing import Any, Dict, Tuple, Optional

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

logger = logging.getLogger("ai_gate")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

# ──────────────────────────────────────────────────────────────────────────────
# Config
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

# AI-specific config
AIGATE_AI_ENABLED = os.getenv("AIGATE_AI_ENABLED", "1").lower() in {
    "1",
    "true",
    "yes",
    "on",
}
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY") or ""
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
FINE_TUNED_MODEL = os.getenv("FINE_TUNED_MODEL", "").strip() or None

AI_BAD_THRESHOLD = float(os.getenv("AI_BAD_THRESHOLD", "0.3"))
AI_GOOD_THRESHOLD = float(os.getenv("AI_GOOD_THRESHOLD", "0.7"))

# if API key is missing, we silently disable AI layer
if not OPENAI_API_KEY:
    AIGATE_AI_ENABLED = False
    logger.info("ai_gate: OPENAI_API_KEY missing – AI layer disabled")

_openai_client: Optional[OpenAI] = None


def _get_openai_client() -> OpenAI:
    global _openai_client
    if _openai_client is None:
        _openai_client = OpenAI(api_key=OPENAI_API_KEY)
    return _openai_client


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
# Quotes & prices
# ──────────────────────────────────────────────────────────────────────────────

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
# AI helper
# ──────────────────────────────────────────────────────────────────────────────

async def _call_ai_classifier(context: Dict[str, Any]) -> Dict[str, Any]:
    """
    Call the fine-tuned model (or base model) to classify the setup.
    Never raises – returns {'error': '...'} on failure.
    """
    if not AIGATE_AI_ENABLED or not OPENAI_API_KEY:
        return {"error": "ai_disabled"}

    model = FINE_TUNED_MODEL or OPENAI_MODEL

    # Build a compact description from context (honeypot + quote + liquidity)
    hp = context.get("honeypot") or {}
    hp_src = hp.get("sources") or {}
    ds = hp_src.get("dexscreener") or {}
    rug = hp_src.get("rugcheck") or {}
    jup = context.get("jup_buy_quote") or {}

    liquidity = ds.get("liquidity_usd")
    pair_count = ds.get("pair_count")
    rug_score = rug.get("score")
    price_impact = _price_impact_bps(jup)

    desc_parts = [
        f"Mint: {context.get('mint')}",
        f"Dexscreener liquidity USD: {liquidity}",
        f"Dexscreener pair count: {pair_count}",
        f"Rugcheck score: {rug_score}",
        f"Jupiter price impact bps: {price_impact}",
        f"Entry SOL/token estimate: {context.get('entry_solin')}",
    ]
    description = " | ".join(str(p) for p in desc_parts)

    system_msg = (
        "You are an AI risk and PnL assistant for a Solana meme coin sniper bot. "
        "Given the candidate context, decide if it looks GOOD, BAD, or NEUTRAL for future entries. "
        "Respond ONLY with a JSON object containing 'label' and 'score', "
        "where label is one of GOOD, BAD, NEUTRAL and score is between 0 and 1."
    )
    user_msg = (
        "Here is a candidate setup. Analyze it and decide if this type of setup is GOOD, BAD, or NEUTRAL:\n"
        f"{description}\n\n"
        "Respond with JSON like: "
        '{"label": "GOOD|BAD|NEUTRAL", "score": number between 0 and 1}.'
    )

    client = _get_openai_client()

    try:
        # Use chat.completions for maximum compatibility; no response_format arg.
        resp = await asyncio.to_thread(
            client.chat.completions.create,
            model=model,
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg},
            ],
            temperature=0.1,
            max_tokens=64,
        )
        content = resp.choices[0].message.content or ""
        raw = content.strip()

        label = None
        score = None

        # Primary: parse as JSON
        try:
            data = json.loads(raw)
            label = str(data.get("label") or "").upper()
            score = safe_float(data.get("score"))
        except Exception:
            # Fallback: crude parsing from text
            upper = raw.upper()
            if "GOOD" in upper:
                label = "GOOD"
            elif "BAD" in upper:
                label = "BAD"
            elif "NEUTRAL" in upper:
                label = "NEUTRAL"

        if score is None:
            score = 0.5

        return {
            "model": model,
            "label": label,
            "score": score,
            "raw": raw,
        }
    except Exception as e:
        logger.warning(f"ai_gate: AI call failed: {e}")
        return {
            "error": str(e),
            "model": model,
        }


# ──────────────────────────────────────────────────────────────────────────────
# Public decision
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
      - AI (if enabled):
          * If label == BAD and score >= AI_BAD_THRESHOLD → block and add reason 'ai:bad'.
          * If label == GOOD and score >= AI_GOOD_THRESHOLD → add reason 'ai:good_hint'
            (does not force a buy, just a positive signal).
    """
    sources: Dict[str, Any] = {}
    reasons: list[str] = []

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
                "ai_enabled": False,
                "ai_bad_threshold": AI_BAD_THRESHOLD,
                "ai_good_threshold": AI_GOOD_THRESHOLD,
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
            if q is None:
                reasons.append("jup:no_quote_soft")
            else:
                reasons.append("entry:none_soft")

    # 4) AI classifier (does not raise)
    ai_block = False
    ai_result: Dict[str, Any] | None = None
    if AIGATE_AI_ENABLED:
        ctx_for_ai = {
            "mint": mint,
            "honeypot": hp_rep,
            "jup_buy_quote": q or {},
            "entry_solin": entry_solin,
        }
        ai_result = await _call_ai_classifier(ctx_for_ai)
        sources["ai"] = ai_result

        label = (ai_result or {}).get("label")
        score = safe_float((ai_result or {}).get("score"))

        if label == "BAD" and (score is not None) and score >= AI_BAD_THRESHOLD:
            reasons.append(f"ai:bad@{score:.3f}")
            ai_block = True
        elif label == "GOOD" and (score is not None) and score >= AI_GOOD_THRESHOLD:
            reasons.append(f"ai:good_hint@{score:.3f}")
        elif (ai_result or {}).get("error"):
            reasons.append("ai:error")

    # 5) Final decision
    ok_honeypot = (not REQUIRE_HONEYPOT_OK) or hp_ok
    ok_impact = (
        MAX_PRICE_IMPACT_BPS <= 0
        or impact_bps is None
        or impact_bps <= MAX_PRICE_IMPACT_BPS
    )
    ok_entry = (not AIGATE_REQUIRE_ENTRY_PRICE) or (entry_solin is not None)

    ok = ok_honeypot and ok_impact and ok_entry and not ai_block

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
            "ai_enabled": AIGATE_AI_ENABLED,
            "ai_bad_threshold": AI_BAD_THRESHOLD,
            "ai_good_threshold": AI_GOOD_THRESHOLD,
        },
    }
    return ok, report
