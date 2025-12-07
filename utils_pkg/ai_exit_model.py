from __future__ import annotations

"""
utils_pkg/ai_exit_model.py

V3+++ / quant-grade EXIT model layer
====================================

High-level role
---------------
Thin AI layer under ai_sell_logic:

    - It does NOT execute sells directly.
    - It does NOT talk to Solana or Jupiter.
    - It ONLY takes a rich feature dict and returns:

          score in [0, 1]  (higher = stronger EXIT pressure)
          meta dict with label / action / notes / debug

ai_sell_logic then blends this with:

    - fraud / rug risk
    - rule engine (TP tiers, SL, stagnation, regime, etc.)
    - risk_manager + ai_risk_model overlays
    - self_learning_ai_strategy overrides

Design goals
------------
    * Easy to turn ON/OFF with env flags.
    * Safe fallbacks if OpenAI / HTTP model / keys are missing.
    * JSON-only protocol to the model (no prompt spaghetti).
    * Works even while fine-tunes are WIP.
    * Pure & side-effect-free (aside from the model HTTP calls).

Primary entrypoint
------------------
    async def predict_exit_score(
        *,
        features: dict,
        mint: str | None = None,
    ) -> tuple[float, dict]

Returned score semantics
------------------------
    - 0.00   → very strong HOLD bias
    - 0.40   → mild HOLD bias
    - 0.50   → neutral / no opinion
    - 0.70   → mild EXIT bias
    - 1.00   → very strong EXIT bias
"""

import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, Optional, Tuple

try:  # optional HTTP provider
    import httpx  # type: ignore[import]
except Exception:  # noqa: BLE001
    httpx = None  # type: ignore[assignment]

logger = logging.getLogger("sniper.ai_exit_model")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

__all__ = ["predict_exit_score"]

AI_EXIT_MODEL_VERSION = "v3+++"

# ──────────────────────────────────────────────────────────────────────────────
# Environment config helpers
# ──────────────────────────────────────────────────────────────────────────────


def _env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "t", "yes", "y", "on")


def _env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except Exception:
        return default


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except Exception:
        return default


# ──────────────────────────────────────────────────────────────────────────────
# Environment config
# ──────────────────────────────────────────────────────────────────────────────

# Global enable / disable
AI_EXIT_MODEL_ENABLED = _env_bool("AI_EXIT_MODEL_ENABLED", False)

# Provider:
#   "openai" → AsyncOpenAI chat.completions
#   "http"   → POST to AI_EXIT_HTTP_URL with JSON body
#   "none"   → disable model, use baseline only
AI_EXIT_MODEL_PROVIDER = os.getenv("AI_EXIT_MODEL_PROVIDER", "openai").strip().lower()

# Model & OpenAI settings
AI_EXIT_MODEL_NAME = os.getenv("AI_EXIT_MODEL_NAME", "").strip()
AI_EXIT_MODEL_TIMEOUT_S = _env_float("AI_EXIT_MODEL_TIMEOUT_S", 1.8)
AI_EXIT_MODEL_MAX_TOKENS = _env_int("AI_EXIT_MODEL_MAX_TOKENS", 256)
AI_EXIT_MODEL_TEMPERATURE = _env_float("AI_EXIT_MODEL_TEMPERATURE", 0.10)

# HTTP provider settings (optional local or remote model)
AI_EXIT_HTTP_URL = os.getenv("AI_EXIT_HTTP_URL", "").strip()
AI_EXIT_HTTP_AUTH_HEADER = os.getenv("AI_EXIT_HTTP_AUTH_HEADER", "").strip()

# Fallback & blending
AI_EXIT_MODEL_FALLBACK_SCORE = _env_float("AI_EXIT_MODEL_FALLBACK_SCORE", 0.50)
AI_EXIT_MODEL_MIN_CONF = _env_float("AI_EXIT_MODEL_MIN_CONF", 0.35)  # below → trust baseline more
AI_EXIT_MODEL_MAX_WEIGHT = _env_float("AI_EXIT_MODEL_MAX_WEIGHT", 0.80)  # cap AI contribution

# If set, we call the model but downstream logic may ignore the score.
AI_EXIT_MODEL_DRY_RUN = _env_bool("AI_EXIT_MODEL_DRY_RUN", False)

# Optional extra debug logging (can be noisy)
AI_EXIT_MODEL_DEBUG = _env_bool("AI_EXIT_MODEL_DEBUG", False)


# ──────────────────────────────────────────────────────────────────────────────
# OpenAI client (lazy)
# ──────────────────────────────────────────────────────────────────────────────

try:  # pragma: no cover - import depends on environment
    from openai import AsyncOpenAI  # type: ignore[attr-defined]

    _OPENAI_AVAILABLE = True
except Exception:  # noqa: BLE001
    AsyncOpenAI = None  # type: ignore[assignment]
    _OPENAI_AVAILABLE = False

_client: Optional["AsyncOpenAI"] = None  # type: ignore[name-defined]


def _get_client() -> Optional["AsyncOpenAI"]:  # type: ignore[name-defined]
    """
    Lazy-init AsyncOpenAI client based on OPENAI_API_KEY or AI_EXIT_OPENAI_KEY.

    Returns None if library or key is missing → we gracefully fall back.
    """
    global _client

    if not _OPENAI_AVAILABLE:
        if AI_EXIT_MODEL_DEBUG:
            logger.debug("AsyncOpenAI not available; exit model disabled")
        return None

    api_key = os.getenv("OPENAI_API_KEY") or os.getenv("AI_EXIT_OPENAI_KEY")
    if not api_key:
        if AI_EXIT_MODEL_DEBUG:
            logger.debug("No OPENAI_API_KEY/AI_EXIT_OPENAI_KEY; exit model disabled")
        return None

    if _client is None:
        _client = AsyncOpenAI(api_key=api_key)
    return _client


# ──────────────────────────────────────────────────────────────────────────────
# Small helpers
# ──────────────────────────────────────────────────────────────────────────────

def _clamp(x: float, lo: float, hi: float) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _safe_json(obj: Any, limit: int = 8192) -> str:
    """
    Best-effort JSON dump that never explodes the prompt.
    """
    try:
        s = json.dumps(obj, ensure_ascii=False, allow_nan=False)
    except Exception:
        try:
            s = json.dumps(str(obj))
        except Exception:
            s = "{}"
    if len(s) > limit:
        s = s[: limit - 3] + "..."
    return s


def _safe_float(d: Dict[str, Any], key: str, default: float = 0.0) -> float:
    try:
        return float(d.get(key, default))
    except Exception:
        return default


# ──────────────────────────────────────────────────────────────────────────────
# Baseline rule-based exit pressure
# ──────────────────────────────────────────────────────────────────────────────

def _baseline_exit_score(features: Dict[str, Any]) -> float:
    """
    Deterministic rule-derived exit pressure used to stabilize the AI
    score when confidence is low or the model is disabled.

    Uses generic fields that are cheap and already available from
    FeatureBuilder / ai_sell_logic / risk_manager where possible:

        pnl_pct, max_up_pct
        hold_sec / hold_minutes
        dd_pct / daily_dd_pct
        fraud_risk_pct / rug_prob
        whale_exit_pressure
        vol_pct_5s / vol_pct_30s
        liquidity_usd / liq_usd
        stagnation_min / stagnant_min
        risk_profile, risk_mode, ai_capacity_delta
        regime_label_numeric, regime_stress_score
    """
    pnl_pct = _safe_float(features, "pnl_pct", 0.0)
    max_up_pct = _safe_float(features, "max_up_pct", pnl_pct)
    dd_pct = _safe_float(features, "dd_pct", 0.0)
    if dd_pct > 0:
        dd_pct = -abs(dd_pct)

    hold_sec = _safe_float(features, "hold_sec", 0.0)
    hold_min = _safe_float(features, "hold_minutes", hold_sec / 60.0)
    stagnation_min = _safe_float(features, "stagnation_min", 0.0)
    last_move_pct = _safe_float(features, "last_move_pct", 0.0)

    fraud = max(
        _safe_float(features, "fraud_risk_pct", 0.0) / 100.0,
        _safe_float(features, "fraud_score", 0.0),
    )
    rug_prob = _safe_float(features, "rug_prob_pct", 0.0) / 100.0
    whale_exit = _safe_float(features, "whale_exit_pressure", 0.0)

    vol_5s = _safe_float(features, "vol_pct_5s", 0.0)
    vol_30s = _safe_float(features, "vol_pct_30s", 0.0)
    liq_usd = max(
        _safe_float(features, "liq_usd", 0.0),
        _safe_float(features, "liquidity_usd", 0.0),
    )

    regime_label = _safe_float(features, "regime_label_numeric", 0.0)
    regime_stress = _safe_float(features, "regime_stress_score", 0.0)

    risk_profile = str(features.get("risk_profile") or "").strip() or "balanced"
    risk_mode = int(features.get("risk_mode") or 1)
    ai_capacity_delta = _safe_float(features, "ai_capacity_delta", 0.0)

    score = 0.50  # neutral center

    # 1) Hard risk stuff – fraud / rug / whales / account DD
    score += _clamp(fraud, 0.0, 1.0) * 0.35  # +0.35 at fraud=1
    score += _clamp(rug_prob, 0.0, 1.0) * 0.30
    score += _clamp(whale_exit, 0.0, 1.0) * 0.25

    if dd_pct <= -20.0:
        score += 0.20
    elif dd_pct <= -10.0:
        score += 0.10

    # 2) PnL shape: deep red or big givebacks → exit pressure
    if pnl_pct < 0:
        score += _clamp(-pnl_pct / 40.0, 0.0, 0.35)  # -40% → +0.35

    if max_up_pct > 0 and pnl_pct < max_up_pct - 12.0:
        # round-tripped 12%+ from peak
        giveback = _clamp((max_up_pct - pnl_pct) / 60.0, 0.0, 0.30)
        score += giveback

    # 3) Volatility & liquidity
    if vol_30s < -35.0 and liq_usd < 15_000:
        # vol crush + thin book → GTFO
        score += 0.25
    elif vol_30s < -20.0:
        score += 0.10

    if liq_usd < 8_000:
        score += 0.15  # illiquid
    elif liq_usd < 20_000:
        score += 0.05

    # 4) Stagnation / time-based exits
    if stagnation_min >= 10.0 and abs(last_move_pct) < 4.0:
        score += _clamp((stagnation_min - 10.0) / 60.0, 0.0, 0.25)

    if hold_min >= 90.0:
        score += _clamp((hold_min - 90.0) / 360.0, 0.0, 0.20)

    # 5) Regime / risk overlays
    if regime_label <= -0.7 or regime_stress >= 0.9:
        score += 0.20
    elif regime_label <= -0.4 or regime_stress >= 0.7:
        score += 0.10

    if risk_mode == 0:  # risk_off
        score += 0.15
    elif risk_mode == 2:  # risk_on
        score -= 0.05

    if risk_profile in ("ultra_defensive", "defensive"):
        score += 0.05
    elif risk_profile in ("aggressive", "degenerate"):
        score -= 0.05

    # 6) Capacity tilt – if AI says capacity is crushed, nudge exits up
    if ai_capacity_delta < 0:
        score += _clamp(-ai_capacity_delta / 0.6, 0.0, 0.12)
    elif ai_capacity_delta > 0.2:
        score -= _clamp((ai_capacity_delta - 0.2) / 0.6, 0.0, 0.07)

    return _clamp(score, 0.0, 1.0)


# ──────────────────────────────────────────────────────────────────────────────
# Low-level model calls
# ──────────────────────────────────────────────────────────────────────────────

async def _call_openai_model(
    *,
    client: "AsyncOpenAI",  # type: ignore[name-defined]
    model: str,
    features: Dict[str, Any],
    mint: Optional[str],
    timeout_s: float,
) -> Dict[str, Any]:
    """
    JSON-only call to the OpenAI exit model.

    Expected JSON response schema:
        {
          "score": float 0..1,
          "confidence": float 0..1,
          "action": "hold" | "exit",
          "label": "hard_stop" | "tp_trailing" | ...,
          "notes": "short reasoning"
        }
    """
    system_msg = (
        "You are an AI exit decision model for a high-frequency Solana memecoin "
        "trading system. You NEVER execute trades. You ONLY return JSON with an "
        "exit pressure score and reasoning.\n\n"
        "Higher score = stronger suggestion to EXIT the position.\n"
        "score=0.0 → diamond-hand hold, score=1.0 → urgent exit.\n"
        "Be conservative with exits unless downside risk is significant."
    )

    user_payload = {
        "mint": mint,
        "schema_version": 1,
        "features": features,
    }

    async def _do_call() -> Dict[str, Any]:
        resp = await client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_msg},
                {
                    "role": "user",
                    "content": (
                        "Given the following JSON features for an OPEN position, "
                        "return a SINGLE JSON object with keys: "
                        "score (0..1), confidence (0..1), action ('hold' or 'exit'), "
                        "label (short snake_case label), notes (short reasoning).\n\n"
                        f"features_json: {_safe_json(user_payload)}"
                    ),
                },
            ],
            response_format={"type": "json_object"},
            max_tokens=AI_EXIT_MODEL_MAX_TOKENS,
            temperature=AI_EXIT_MODEL_TEMPERATURE,
        )
        text = resp.choices[0].message.content or "{}"
        try:
            data = json.loads(text)
            if not isinstance(data, dict):
                raise ValueError("model returned non-dict JSON")
            return data
        except Exception as e:  # noqa: BLE001
            if AI_EXIT_MODEL_DEBUG:
                logger.warning("ai_exit_model: JSON parse error: %s | text=%r", e, text)
            return {}

    try:
        return await asyncio.wait_for(_do_call(), timeout=timeout_s)
    except asyncio.TimeoutError:
        if AI_EXIT_MODEL_DEBUG:
            logger.warning("ai_exit_model: timeout after %.2fs", timeout_s)
        return {}
    except Exception as e:  # noqa: BLE001
        if AI_EXIT_MODEL_DEBUG:
            logger.warning("ai_exit_model: model call failed: %s", e, exc_info=True)
        return {}


async def _call_http_model(
    *,
    url: str,
    features: Dict[str, Any],
    mint: Optional[str],
    timeout_s: float,
) -> Dict[str, Any]:
    """
    JSON POST to a custom HTTP model endpoint.

    Body:
        {
          "mint": str | null,
          "features": { ... },
          "schema_version": 1
        }

    Expected response schema is the same as _call_openai_model.
    """
    if httpx is None:
        return {}

    payload = {
        "mint": mint,
        "schema_version": 1,
        "features": features,
    }

    headers = {"content-type": "application/json"}
    if AI_EXIT_HTTP_AUTH_HEADER:
        # Expect "HeaderName: value" style
        try:
            name, val = AI_EXIT_HTTP_AUTH_HEADER.split(":", 1)
            headers[name.strip()] = val.strip()
        except Exception:
            headers["authorization"] = AI_EXIT_HTTP_AUTH_HEADER

    async def _do_call() -> Dict[str, Any]:
        async with httpx.AsyncClient(timeout=timeout_s) as client:
            resp = await client.post(url, json=payload, headers=headers)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, dict):
                return data
            return {}

    try:
        return await asyncio.wait_for(_do_call(), timeout=timeout_s)
    except asyncio.TimeoutError:
        if AI_EXIT_MODEL_DEBUG:
            logger.warning("ai_exit_model: HTTP timeout after %.2fs", timeout_s)
        return {}
    except Exception as e:  # noqa: BLE001
        if AI_EXIT_MODEL_DEBUG:
            logger.warning("ai_exit_model: HTTP model call failed: %s", e, exc_info=True)
        return {}


# ──────────────────────────────────────────────────────────────────────────────
# Core API
# ──────────────────────────────────────────────────────────────────────────────

async def predict_exit_score(
    *,
    features: Dict[str, Any],
    mint: Optional[str] = None,
) -> Tuple[float, Dict[str, Any]]:
    """
    Main entrypoint used by ai_sell_logic.

    Args:
        features: rich feature dict built by ai_sell_logic / exit_engine
                  / FeatureBuilder / risk_manager, e.g.:
            {
              "pnl_pct": ...,
              "max_up_pct": ...,
              "dd_pct": ...,
              "fraud_risk_pct": ...,
              "rug_prob_pct": ...,
              "whale_exit_pressure": ...,
              "vol_pct_5s": ...,
              "vol_pct_30s": ...,
              "liq_usd": ...,
              "stagnation_min": ...,
              "risk_profile": ...,
              "risk_mode": ...,
              "ai_capacity_delta": ...,
              "regime_label_numeric": ...,
              "regime_stress_score": ...,
              ...
            }
        mint: optional token mint (for logging / debugging only).

    Returns:
        score: float in [0,1] (higher = stronger EXIT pressure)
        meta: {
            "version": str,
            "mint": str | None,
            "raw_score": float | None,
            "blended_score": float,
            "confidence": float | None,
            "action": "hold" | "exit" | None,
            "label": str | None,
            "notes": str | None,
            "used_model": str | None,
            "provider": str | None,
            "dry_run": bool,
            "baseline_exit_score": float,
            "reason": str,      # short summary of how we derived score
            "error": str | None,
            "duration_ms": int,
        }
    """
    started = time.time()

    baseline = _baseline_exit_score(features)

    meta: Dict[str, Any] = {
        "version": AI_EXIT_MODEL_VERSION,
        "mint": mint,
        "used_model": None,
        "provider": None,
        "dry_run": bool(AI_EXIT_MODEL_DRY_RUN),
        "baseline_exit_score": float(baseline),
        "raw_score": None,
        "blended_score": None,
        "confidence": None,
        "action": None,
        "label": None,
        "notes": None,
        "reason": "",
        "error": None,
        "duration_ms": 0,
    }

    # Quick disable path (env or provider 'none')
    if (not AI_EXIT_MODEL_ENABLED) or (AI_EXIT_MODEL_PROVIDER == "none"):
        score = _clamp(baseline, 0.0, 1.0)
        meta["reason"] = "model_disabled_use_baseline"
        meta["blended_score"] = score
        meta["duration_ms"] = int((time.time() - started) * 1000)
        return score, meta

    # Choose provider
    provider = AI_EXIT_MODEL_PROVIDER
    meta["provider"] = provider

    model_resp: Dict[str, Any] = {}

    if provider == "openai":
        client = _get_client()
        if not client or not AI_EXIT_MODEL_NAME:
            score = _clamp(baseline, 0.0, 1.0)
            meta["reason"] = "no_client_or_model_use_baseline"
            meta["error"] = "missing_openai_client_or_model"
            meta["blended_score"] = score
            meta["duration_ms"] = int((time.time() - started) * 1000)
            return score, meta

        model_resp = await _call_openai_model(
            client=client,
            model=AI_EXIT_MODEL_NAME,
            features=features,
            mint=mint,
            timeout_s=AI_EXIT_MODEL_TIMEOUT_S,
        )
        meta["used_model"] = AI_EXIT_MODEL_NAME

    elif provider == "http":
        if not AI_EXIT_HTTP_URL or httpx is None:
            score = _clamp(baseline, 0.0, 1.0)
            meta["reason"] = "no_http_model_use_baseline"
            meta["error"] = "missing_http_url_or_httpx"
            meta["blended_score"] = score
            meta["duration_ms"] = int((time.time() - started) * 1000)
            return score, meta

        model_resp = await _call_http_model(
            url=AI_EXIT_HTTP_URL,
            features=features,
            mint=mint,
            timeout_s=AI_EXIT_MODEL_TIMEOUT_S,
        )
        meta["used_model"] = AI_EXIT_HTTP_URL

    else:
        # unknown provider → baseline only
        score = _clamp(baseline, 0.0, 1.0)
        meta["reason"] = "unknown_provider_use_baseline"
        meta["error"] = f"unknown_provider:{provider}"
        meta["blended_score"] = score
        meta["duration_ms"] = int((time.time() - started) * 1000)
        return score, meta

    raw_score = None
    conf = None
    action = None
    label = None
    notes = None

    try:
        if model_resp:
            raw_score = float(model_resp.get("score", AI_EXIT_MODEL_FALLBACK_SCORE))
            conf = float(model_resp.get("confidence", 0.0))
            action = model_resp.get("action")
            label = model_resp.get("label")
            notes = model_resp.get("notes")
    except Exception as e:  # noqa: BLE001
        if AI_EXIT_MODEL_DEBUG:
            logger.warning("ai_exit_model: response parse error: %s | resp=%r", e, model_resp)

    if isinstance(raw_score, (int, float)):
        meta["raw_score"] = _clamp(float(raw_score), 0.0, 1.0)
    else:
        meta["raw_score"] = None

    if isinstance(conf, (int, float)):
        meta["confidence"] = _clamp(float(conf), 0.0, 1.0)
    else:
        meta["confidence"] = None

    meta["action"] = action if isinstance(action, str) else None
    meta["label"] = label if isinstance(label, str) else None
    meta["notes"] = notes if isinstance(notes, str) else None

    # If we failed to get a valid score, fall back.
    if meta["raw_score"] is None:
        score = _clamp(baseline, 0.0, 1.0)
        meta["reason"] = "model_failed_use_baseline"
        meta["error"] = "no_valid_score_from_model"
        meta["blended_score"] = score
        meta["duration_ms"] = int((time.time() - started) * 1000)
        return score, meta

    raw = float(meta["raw_score"])
    conf = float(meta["confidence"] or 0.0)

    # Blend raw AI score with baseline depending on confidence
    if conf <= AI_EXIT_MODEL_MIN_CONF:
        # low confidence → let baseline dominate
        w_ai = min(0.20, AI_EXIT_MODEL_MAX_WEIGHT)
        w_base = 1.0 - w_ai
        reason = "low_conf_blend_baseline"
    else:
        # high confidence → let AI dominate but still keep some rule weight
        w_ai = min(0.65, AI_EXIT_MODEL_MAX_WEIGHT)
        w_base = 1.0 - w_ai
        reason = "normal_conf_blend"

    blended = _clamp(w_ai * raw + w_base * baseline, 0.0, 1.0)
    meta["blended_score"] = blended
    meta["reason"] = reason
    meta["duration_ms"] = int((time.time() - started) * 1000)

    # Dry-run: downstream ai_sell_logic can choose to ignore this,
    # but we still return the blended value for logging / dashboards.
    if AI_EXIT_MODEL_DRY_RUN and AI_EXIT_MODEL_DEBUG:
        logger.info(
            "AI_EXIT_MODEL_DRY_RUN mint=%s raw=%.3f conf=%.3f baseline=%.3f blended=%.3f label=%s action=%s",
            mint,
            raw,
            conf,
            baseline,
            blended,
            label,
            action,
        )

    if AI_EXIT_MODEL_DEBUG and not AI_EXIT_MODEL_DRY_RUN:
        logger.info(
            "AI_EXIT_MODEL mint=%s raw=%.3f conf=%.3f baseline=%.3f blended=%.3f label=%s action=%s",
            mint,
            raw,
            conf,
            baseline,
            blended,
            label,
            action,
        )

    return blended, meta


# ──────────────────────────────────────────────────────────────────────────────
# Optional CLI smoke test
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":  # pragma: no cover
    import asyncio as _asyncio

    async def _main() -> None:
        fake_features = {
            "pnl_pct": 120.0,
            "max_up_pct": 160.0,
            "dd_pct": -15.0,
            "fraud_risk_pct": 10.0,
            "rug_prob_pct": 5.0,
            "whale_exit_pressure": 0.2,
            "vol_pct_5s": -5.0,
            "vol_pct_30s": -10.0,
            "liq_usd": 30_000.0,
            "stagnation_min": 15.0,
            "risk_profile": "balanced",
            "risk_mode": 1,
            "ai_capacity_delta": 0.1,
            "regime_label_numeric": 0.3,
            "regime_stress_score": 0.2,
        }
        score, meta = await predict_exit_score(features=fake_features, mint="TESTMINT")
        print("score:", score)
        print("meta:", json.dumps(meta, indent=2))

    _asyncio.run(_main())
