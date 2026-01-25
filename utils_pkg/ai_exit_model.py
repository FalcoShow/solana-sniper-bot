# utils_pkg/ai_exit_model.py
from __future__ import annotations

"""
utils_pkg/ai_exit_model.py

V3+++ / quant-grade EXIT model layer (LOCK-IN)
=============================================

Role
----
Thin AI layer under ai_sell_logic:

  - DOES NOT execute sells.
  - DOES NOT talk to Solana RPC directly.
  - DOES NOT own any "hard" exit rules (TP/SL/trailing/stagnant/deadcoin).
  - ONLY returns an exit-pressure score in [0, 1] + explainable metadata.

Contract (public)
-----------------
    async def predict_exit_score(*, features: dict, mint: str | None = None) -> tuple[float, dict]

Score semantics
---------------
    0.00  -> strong HOLD bias
    0.50  -> neutral / no opinion
    1.00  -> strong EXIT bias

Design goals
------------
- Safe defaults: model OFF by default, never blocks trading on AI failure.
- Low latency: strict timeouts + concurrency cap + TTL cache.
- Explainable: baseline factors + AI factors + blend diagnostics.
- Provider-agnostic: OpenAI (AsyncOpenAI) or generic HTTP JSON endpoint.
- Side-effect free: no DB writes, no RPC, no global state mutation (beyond cache/circuit-breaker).

NOTE
----
Hard exits still belong in:
  - utils_pkg/exit_engine.py (canonical exit brain)
  - utils_pkg/ai_sell_logic.py (blends this AI score with rules/risk/regime)

"""

import asyncio
import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any

try:  # optional HTTP provider
    import httpx  # type: ignore[import]
except Exception:
    httpx = None  # type: ignore[assignment]

logger = logging.getLogger("sniper.ai_exit_model")

__all__ = ["predict_exit_score"]

AI_EXIT_MODEL_VERSION = "v3+++"
_JSON_LIMIT = 10_000  # bytes-ish for prompt safety


# ──────────────────────────────────────────────────────────────────────────────
# Env helpers
# ──────────────────────────────────────────────────────────────────────────────


def _env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "t", "yes", "y", "on")


def _env_int(key: str, default: int) -> int:
    try:
        return int(str(os.getenv(key, str(default))).strip())
    except Exception:
        return default


def _env_float(key: str, default: float) -> float:
    try:
        return float(str(os.getenv(key, str(default))).strip())
    except Exception:
        return default


def _env_str(key: str, default: str = "") -> str:
    v = os.getenv(key)
    return default if v is None else str(v).strip()


def _clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_json(obj: Any, limit: int = _JSON_LIMIT) -> str:
    try:
        s = json.dumps(obj, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    except Exception:
        try:
            s = json.dumps(str(obj), ensure_ascii=False, separators=(",", ":"))
        except Exception:
            s = "{}"
    if len(s) > limit:
        s = s[: max(0, limit - 3)] + "..."
    return s


def _f(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


# ──────────────────────────────────────────────────────────────────────────────
# Config (env)
# ──────────────────────────────────────────────────────────────────────────────

# Global enable / disable (safe default OFF)
AI_EXIT_MODEL_ENABLED = _env_bool("AI_EXIT_MODEL_ENABLED", False)

# Providers:
#   - "openai"
#   - "http"
#   - "none"
AI_EXIT_MODEL_PROVIDER = _env_str("AI_EXIT_MODEL_PROVIDER", "openai").lower() or "openai"

# OpenAI model
AI_EXIT_MODEL_NAME = _env_str("AI_EXIT_MODEL_NAME", "")
AI_EXIT_OPENAI_KEY = _env_str("AI_EXIT_OPENAI_KEY", "")  # optional override (else OPENAI_API_KEY)
AI_EXIT_MODEL_TIMEOUT_S = _env_float("AI_EXIT_MODEL_TIMEOUT_S", 1.6)
AI_EXIT_MODEL_MAX_TOKENS = _env_int("AI_EXIT_MODEL_MAX_TOKENS", 220)
AI_EXIT_MODEL_TEMPERATURE = _env_float("AI_EXIT_MODEL_TEMPERATURE", 0.10)

# HTTP model endpoint
AI_EXIT_HTTP_URL = _env_str("AI_EXIT_HTTP_URL", "")
AI_EXIT_HTTP_AUTH_HEADER = _env_str("AI_EXIT_HTTP_AUTH_HEADER", "")  # "Header: value" or raw token

# Blending & fallbacks
AI_EXIT_MODEL_FALLBACK_SCORE = _clamp(_env_float("AI_EXIT_MODEL_FALLBACK_SCORE", 0.50))
AI_EXIT_MODEL_MIN_CONF = _clamp(_env_float("AI_EXIT_MODEL_MIN_CONF", 0.35))
AI_EXIT_MODEL_MAX_WEIGHT = _clamp(_env_float("AI_EXIT_MODEL_MAX_WEIGHT", 0.80))
AI_EXIT_MODEL_MIN_WEIGHT = _clamp(
    _env_float("AI_EXIT_MODEL_MIN_WEIGHT", 0.10)
)  # if confident, at least this weight
AI_EXIT_MODEL_DRY_RUN = _env_bool("AI_EXIT_MODEL_DRY_RUN", False)
AI_EXIT_MODEL_DEBUG = _env_bool("AI_EXIT_MODEL_DEBUG", False)

# Latency control
AI_EXIT_MODEL_MAX_PARALLEL = max(1, _env_int("AI_EXIT_MODEL_MAX_PARALLEL", 4))

# Cache (TTL seconds). Cache is safe because this is advisory, not execution.
AI_EXIT_MODEL_CACHE_TTL_S = max(0, _env_int("AI_EXIT_MODEL_CACHE_TTL_S", 15))
AI_EXIT_MODEL_CACHE_MAX = max(64, _env_int("AI_EXIT_MODEL_CACHE_MAX", 2048))

# Circuit breaker
AI_EXIT_MODEL_FAIL_OPEN = _env_bool("AI_EXIT_MODEL_FAIL_OPEN", True)  # baseline-only on failure
AI_EXIT_MODEL_MAX_FAILS = max(1, _env_int("AI_EXIT_MODEL_MAX_FAILS", 6))
AI_EXIT_MODEL_COOLDOWN_S = max(1, _env_int("AI_EXIT_MODEL_COOLDOWN_S", 30))

# Feature selection / prompt minimization
# If empty, we build a compact model_features dict from common keys.
AI_EXIT_MODEL_ALLOWLIST = [
    s.strip() for s in _env_str("AI_EXIT_MODEL_ALLOWLIST", "").split(",") if s.strip()
]


# ──────────────────────────────────────────────────────────────────────────────
# Optional OpenAI client (lazy)
# ──────────────────────────────────────────────────────────────────────────────

_OPENAI_AVAILABLE = False
AsyncOpenAI = None  # type: ignore[assignment]
_client: Any = None

try:  # pragma: no cover
    from openai import AsyncOpenAI as _AsyncOpenAI  # type: ignore[attr-defined]

    AsyncOpenAI = _AsyncOpenAI  # type: ignore[misc]
    _OPENAI_AVAILABLE = True
except Exception:
    _OPENAI_AVAILABLE = False
    AsyncOpenAI = None  # type: ignore[assignment]


def _get_openai_client() -> Any:
    """
    Returns AsyncOpenAI client or None if unavailable / no key.
    """
    global _client
    if not _OPENAI_AVAILABLE or AsyncOpenAI is None:
        return None

    key = AI_EXIT_OPENAI_KEY or _env_str("OPENAI_API_KEY", "")
    if not key:
        return None

    if _client is None:
        _client = AsyncOpenAI(api_key=key)
    return _client


# ──────────────────────────────────────────────────────────────────────────────
# Lightweight metrics hooks (optional)
# ──────────────────────────────────────────────────────────────────────────────


def _metric_inc(name: str, value: int = 1) -> None:
    try:
        from metrics.metrics_exporter import inc_counter  # type: ignore

        inc_counter(name, value)
    except Exception:
        return


# ──────────────────────────────────────────────────────────────────────────────
# Cache + concurrency + circuit breaker
# ──────────────────────────────────────────────────────────────────────────────


@dataclass
class _CacheItem:
    exp_ms: int
    score: float
    meta: dict[str, Any]


_cache: dict[str, _CacheItem] = {}
_cache_order: list[str] = []  # simple FIFO (good enough)
_cache_lock = asyncio.Lock()

_sem = asyncio.Semaphore(AI_EXIT_MODEL_MAX_PARALLEL)

_fail_count = 0
_cooldown_until_ms = 0
_state_lock = asyncio.Lock()


def _hash_key(mint: str | None, features: dict[str, Any]) -> str:
    """
    Stable key for short TTL caching.

    We do NOT hash the entire raw features (could be huge / unstable). We hash the
    compact model_features dict produced by _prepare_model_features.
    """
    base = {
        "mint": mint or "",
        "mf": _prepare_model_features(features),
    }
    b = _safe_json(base, limit=4096).encode("utf-8", "ignore")
    return hashlib.sha256(b).hexdigest()


async def _cache_get(key: str) -> _CacheItem | None:
    if AI_EXIT_MODEL_CACHE_TTL_S <= 0:
        return None
    async with _cache_lock:
        item = _cache.get(key)
        if not item:
            return None
        if item.exp_ms <= _now_ms():
            _cache.pop(key, None)
            return None
        return item


async def _cache_put(key: str, item: _CacheItem) -> None:
    if AI_EXIT_MODEL_CACHE_TTL_S <= 0:
        return
    async with _cache_lock:
        if key not in _cache:
            _cache_order.append(key)
        _cache[key] = item

        # prune
        while len(_cache_order) > AI_EXIT_MODEL_CACHE_MAX:
            old = _cache_order.pop(0)
            _cache.pop(old, None)


async def _circuit_allows_call() -> bool:
    async with _state_lock:
        return _now_ms() >= _cooldown_until_ms


async def _record_success() -> None:
    global _fail_count
    async with _state_lock:
        _fail_count = 0


async def _record_failure() -> None:
    global _fail_count, _cooldown_until_ms
    async with _state_lock:
        _fail_count += 1
        if _fail_count >= AI_EXIT_MODEL_MAX_FAILS:
            _cooldown_until_ms = _now_ms() + int(AI_EXIT_MODEL_COOLDOWN_S * 1000)


# ──────────────────────────────────────────────────────────────────────────────
# Baseline (deterministic) + explanations
# ──────────────────────────────────────────────────────────────────────────────


def _get_common(features: dict[str, Any], *keys: str, default: float = 0.0) -> float:
    for k in keys:
        if k in features:
            return _f(features.get(k), default)
    return default


def _baseline_exit_score(features: dict[str, Any]) -> tuple[float, dict[str, Any]]:
    """
    Deterministic, explainable baseline exit pressure.

    Returns:
        (score in [0,1], explain dict with top factors)
    """
    pnl_pct = _get_common(features, "pnl_pct", default=0.0)
    max_up_pct = _get_common(features, "max_up_pct", default=pnl_pct)
    dd_pct = _get_common(features, "dd_pct", default=0.0)
    hold_sec = _get_common(features, "hold_sec", default=0.0)
    hold_min = _get_common(features, "hold_minutes", default=hold_sec / 60.0)

    # risk signals (normalized 0..1)
    fraud = max(
        _clamp(_get_common(features, "fraud_risk_pct", default=0.0) / 100.0),
        _clamp(_get_common(features, "fraud_score", default=0.0)),
    )
    rug = _clamp(_get_common(features, "rug_prob_pct", "rug_prob", default=0.0) / 100.0)
    whale_exit = _clamp(_get_common(features, "whale_exit_pressure", default=0.0))

    # flow / market
    vol_30s = _get_common(features, "vol_pct_30s", "vol_30s_pct", default=0.0)
    liq_usd = max(
        _get_common(features, "liq_usd", "liquidity_usd", default=0.0),
        _get_common(features, "liq", default=0.0),
    )
    stagnation_min = _get_common(features, "stagnation_min", "stagnant_min", default=0.0)
    last_move_pct = _get_common(features, "last_move_pct", "move_pct", default=0.0)

    regime_stress = _clamp(_get_common(features, "regime_stress_score", default=0.0))
    regime_label = _get_common(features, "regime_label_numeric", default=0.0)  # negative => worse

    # overlays
    risk_profile = str(features.get("risk_profile") or "").strip().lower() or "balanced"
    risk_mode = int(_f(features.get("risk_mode"), 1))  # 0=risk_off, 1=normal, 2=risk_on
    ai_capacity_delta = _get_common(features, "ai_capacity_delta", default=0.0)

    score = 0.50
    reasons: list[tuple[str, float]] = []

    # Fraud / rug / whales are the strongest "get out" nudges
    if fraud > 0:
        add = 0.35 * fraud
        score += add
        reasons.append(("fraud", add))
    if rug > 0:
        add = 0.30 * rug
        score += add
        reasons.append(("rug", add))
    if whale_exit > 0:
        add = 0.25 * whale_exit
        score += add
        reasons.append(("whales", add))

    # Drawdown severity (if supplied negative)
    if dd_pct <= -20.0:
        score += 0.20
        reasons.append(("dd<=-20", 0.20))
    elif dd_pct <= -10.0:
        score += 0.10
        reasons.append(("dd<=-10", 0.10))

    # Negative PnL
    if pnl_pct < 0:
        add = _clamp((-pnl_pct) / 40.0, 0.0, 0.35)
        score += add
        reasons.append(("red_pnl", add))

    # Giveback from peak
    if max_up_pct > 0 and pnl_pct < (max_up_pct - 12.0):
        add = _clamp((max_up_pct - pnl_pct) / 60.0, 0.0, 0.30)
        score += add
        reasons.append(("giveback", add))

    # Liquidity / volatility
    if liq_usd < 8_000:
        score += 0.15
        reasons.append(("thin_liq<8k", 0.15))
    elif liq_usd < 20_000:
        score += 0.05
        reasons.append(("thin_liq<20k", 0.05))

    if vol_30s < -35.0 and liq_usd < 15_000:
        score += 0.25
        reasons.append(("vol_crush+thin", 0.25))
    elif vol_30s < -20.0:
        score += 0.10
        reasons.append(("vol_crush", 0.10))

    # Stagnation
    if stagnation_min >= 10.0 and abs(last_move_pct) < 4.0:
        add = _clamp((stagnation_min - 10.0) / 60.0, 0.0, 0.25)
        score += add
        reasons.append(("stagnation", add))

    # Time decay
    if hold_min >= 90.0:
        add = _clamp((hold_min - 90.0) / 360.0, 0.0, 0.20)
        score += add
        reasons.append(("time_decay", add))

    # Regime / stress
    if regime_label <= -0.7 or regime_stress >= 0.9:
        score += 0.20
        reasons.append(("regime_bad", 0.20))
    elif regime_label <= -0.4 or regime_stress >= 0.7:
        score += 0.10
        reasons.append(("regime_soft_bad", 0.10))

    # Risk mode
    if risk_mode == 0:
        score += 0.15
        reasons.append(("risk_off", 0.15))
    elif risk_mode == 2:
        score -= 0.05
        reasons.append(("risk_on", -0.05))

    # Profile tilt
    if risk_profile in ("ultra_defensive", "defensive"):
        score += 0.05
        reasons.append(("defensive_profile", 0.05))
    elif risk_profile in ("aggressive", "degenerate"):
        score -= 0.05
        reasons.append(("aggressive_profile", -0.05))

    # Capacity tilt
    if ai_capacity_delta < 0:
        add = _clamp(-ai_capacity_delta / 0.6, 0.0, 0.12)
        score += add
        reasons.append(("capacity_down", add))
    elif ai_capacity_delta > 0.2:
        sub = _clamp((ai_capacity_delta - 0.2) / 0.6, 0.0, 0.07)
        score -= sub
        reasons.append(("capacity_up", -sub))

    score = _clamp(score)

    # Keep top factors for explainability
    reasons_sorted = sorted(reasons, key=lambda t: abs(t[1]), reverse=True)
    explain = {
        "baseline_score": score,
        "top_factors": reasons_sorted[:6],
        "inputs": {
            "pnl_pct": pnl_pct,
            "max_up_pct": max_up_pct,
            "dd_pct": dd_pct,
            "hold_min": hold_min,
            "fraud": fraud,
            "rug": rug,
            "whale_exit": whale_exit,
            "vol_30s": vol_30s,
            "liq_usd": liq_usd,
            "stagnation_min": stagnation_min,
            "last_move_pct": last_move_pct,
            "regime_label": regime_label,
            "regime_stress": regime_stress,
            "risk_profile": risk_profile,
            "risk_mode": risk_mode,
            "ai_capacity_delta": ai_capacity_delta,
        },
    }
    return score, explain


def _safety_floor_from_features(features: dict[str, Any]) -> float:
    """
    Safety floors: if certain extreme-risk signals are present, ensure the
    *final* score never gets unrealistically low even if the model says "hold".

    This does NOT replace exit_engine hard rules; it only stabilizes AI output.
    """
    fraud = max(
        _clamp(_get_common(features, "fraud_risk_pct", default=0.0) / 100.0),
        _clamp(_get_common(features, "fraud_score", default=0.0)),
    )
    rug = _clamp(_get_common(features, "rug_prob_pct", "rug_prob", default=0.0) / 100.0)
    pnl_pct = _get_common(features, "pnl_pct", default=0.0)
    liq_usd = max(
        _get_common(features, "liq_usd", default=0.0),
        _get_common(features, "liquidity_usd", default=0.0),
    )

    floor = 0.0
    if fraud >= 0.9:
        floor = max(floor, 0.80)
    if rug >= 0.8:
        floor = max(floor, 0.75)
    if pnl_pct <= -35.0:
        floor = max(floor, 0.70)
    if liq_usd > 0 and liq_usd <= 3_000:
        floor = max(floor, 0.65)

    return _clamp(floor)


# ──────────────────────────────────────────────────────────────────────────────
# Prompt feature preparation
# ──────────────────────────────────────────────────────────────────────────────


def _prepare_model_features(features: dict[str, Any]) -> dict[str, Any]:
    """
    Build a compact, stable, model-friendly feature dict.

    If AI_EXIT_MODEL_ALLOWLIST is set, we send only those keys (if present).
    Otherwise we send a compact summary (recommended).
    """
    if AI_EXIT_MODEL_ALLOWLIST:
        out: dict[str, Any] = {}
        for k in AI_EXIT_MODEL_ALLOWLIST:
            if k in features:
                v = features.get(k)
                # keep JSON-safe primitives
                if isinstance(v, str | int | float | bool) or v is None:
                    out[k] = v
                else:
                    out[k] = str(v)[:256]
        return out

    pnl_pct = _get_common(features, "pnl_pct", default=0.0)
    max_up_pct = _get_common(features, "max_up_pct", default=pnl_pct)
    dd_pct = _get_common(features, "dd_pct", default=0.0)
    hold_sec = _get_common(features, "hold_sec", default=0.0)
    hold_min = _get_common(features, "hold_minutes", default=hold_sec / 60.0)
    giveback = (max_up_pct - pnl_pct) if max_up_pct > 0 else 0.0

    fraud = max(
        _clamp(_get_common(features, "fraud_risk_pct", default=0.0) / 100.0),
        _clamp(_get_common(features, "fraud_score", default=0.0)),
    )
    rug = _clamp(_get_common(features, "rug_prob_pct", "rug_prob", default=0.0) / 100.0)
    whale_exit = _clamp(_get_common(features, "whale_exit_pressure", default=0.0))

    liq_usd = max(
        _get_common(features, "liq_usd", "liquidity_usd", default=0.0),
        _get_common(features, "liq", default=0.0),
    )
    vol_30s = _get_common(features, "vol_pct_30s", "vol_30s_pct", default=0.0)
    stagnation_min = _get_common(features, "stagnation_min", "stagnant_min", default=0.0)
    last_move_pct = _get_common(features, "last_move_pct", "move_pct", default=0.0)

    regime_label = _get_common(features, "regime_label_numeric", default=0.0)
    regime_stress = _clamp(_get_common(features, "regime_stress_score", default=0.0))

    risk_profile = str(features.get("risk_profile") or "").strip().lower() or "balanced"
    risk_mode = int(_f(features.get("risk_mode"), 1))
    ai_capacity_delta = _get_common(features, "ai_capacity_delta", default=0.0)

    # tiny derived buckets (helps models generalize)
    liq_bucket = "unknown"
    if liq_usd > 0:
        if liq_usd < 8_000:
            liq_bucket = "thin"
        elif liq_usd < 30_000:
            liq_bucket = "ok"
        else:
            liq_bucket = "deep"

    return {
        # shape
        "pnl_pct": pnl_pct,
        "max_up_pct": max_up_pct,
        "giveback_pct": giveback,
        "dd_pct": dd_pct,
        "hold_min": hold_min,
        # risk
        "fraud": fraud,
        "rug": rug,
        "whale_exit": whale_exit,
        # market
        "liq_usd": liq_usd,
        "liq_bucket": liq_bucket,
        "vol_30s": vol_30s,
        "stagnation_min": stagnation_min,
        "last_move_pct": last_move_pct,
        # regime & overlays
        "regime_label": regime_label,
        "regime_stress": regime_stress,
        "risk_profile": risk_profile,
        "risk_mode": risk_mode,
        "ai_capacity_delta": ai_capacity_delta,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Provider calls
# ──────────────────────────────────────────────────────────────────────────────


async def _call_openai(
    *, features: dict[str, Any], mint: str | None, timeout_s: float
) -> dict[str, Any]:
    client = _get_openai_client()
    if not client or not AI_EXIT_MODEL_NAME:
        return {}

    system_msg = (
        "You are an EXIT advisory model for a Solana memecoin trading bot.\n"
        "You NEVER execute trades. You ONLY output a JSON object.\n"
        "Return exit pressure score in [0,1] where higher = stronger exit bias.\n"
        "Be conservative: do not suggest exit without a clear downside or deteriorating conditions.\n"
    )

    payload = {
        "mint": mint,
        "schema_version": 1,
        "features": _prepare_model_features(features),
        "output_schema": {
            "score": "float 0..1",
            "confidence": "float 0..1",
            "action": "hold|exit",
            "label": "short_snake_case",
            "notes": "short string",
            "reason_codes": "optional list[str]",
        },
    }

    async def _do() -> dict[str, Any]:
        resp = await client.chat.completions.create(
            model=AI_EXIT_MODEL_NAME,
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": f"Return ONLY JSON:\n{_safe_json(payload)}"},
            ],
            response_format={"type": "json_object"},
            max_tokens=AI_EXIT_MODEL_MAX_TOKENS,
            temperature=AI_EXIT_MODEL_TEMPERATURE,
        )
        txt = (resp.choices[0].message.content or "").strip() or "{}"
        try:
            out = json.loads(txt)
            return out if isinstance(out, dict) else {}
        except Exception:
            return {}

    try:
        return await asyncio.wait_for(_do(), timeout=timeout_s)
    except Exception:
        return {}


async def _call_http(
    *, features: dict[str, Any], mint: str | None, timeout_s: float
) -> dict[str, Any]:
    if not AI_EXIT_HTTP_URL or httpx is None:
        return {}

    headers = {"content-type": "application/json"}
    if AI_EXIT_HTTP_AUTH_HEADER:
        # Accept either "Header: value" or "Bearer xxx"
        if ":" in AI_EXIT_HTTP_AUTH_HEADER:
            try:
                name, val = AI_EXIT_HTTP_AUTH_HEADER.split(":", 1)
                headers[name.strip()] = val.strip()
            except Exception:
                headers["authorization"] = AI_EXIT_HTTP_AUTH_HEADER.strip()
        else:
            headers["authorization"] = AI_EXIT_HTTP_AUTH_HEADER.strip()

    payload = {
        "mint": mint,
        "schema_version": 1,
        "features": _prepare_model_features(features),
    }

    async def _do() -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=timeout_s) as c:
            r = await c.post(AI_EXIT_HTTP_URL, json=payload, headers=headers)
            r.raise_for_status()
            data = r.json()
            return data if isinstance(data, dict) else {}

    try:
        return await asyncio.wait_for(_do(), timeout=timeout_s)
    except Exception:
        return {}


def _parse_model_response(resp: dict[str, Any]) -> tuple[float | None, float, dict[str, Any]]:
    """
    Returns: (raw_score_or_none, confidence_0_1, parsed_meta)
    """
    if not isinstance(resp, dict) or not resp:
        return None, 0.0, {}

    raw = resp.get("score")
    conf = resp.get("confidence", 0.0)

    score: float | None
    try:
        score = float(raw)  # type: ignore[arg-type]
        score = _clamp(score)
    except Exception:
        score = None

    try:
        confidence = _clamp(float(conf))
    except Exception:
        confidence = 0.0

    meta: dict[str, Any] = {}
    for k in ("action", "label", "notes"):
        v = resp.get(k)
        if isinstance(v, str):
            meta[k] = v[:400]  # bound
    rc = resp.get("reason_codes")
    if isinstance(rc, list):
        meta["reason_codes"] = [str(x)[:64] for x in rc[:12]]

    return score, confidence, meta


def _blend(*, baseline: float, ai_score: float, confidence: float) -> tuple[float, dict[str, Any]]:
    """
    Confidence-aware blending, bounded + explainable.

    - If confidence is low, AI weight stays tiny.
    - If confidence is high, AI can dominate (up to MAX_WEIGHT) but never 100%.
    """
    c = _clamp(confidence)
    # smooth confidence curve: c^2 makes medium confidence much less impactful
    w_ai = (c * c) * AI_EXIT_MODEL_MAX_WEIGHT
    if c >= AI_EXIT_MODEL_MIN_CONF:
        w_ai = max(w_ai, AI_EXIT_MODEL_MIN_WEIGHT)
    w_ai = _clamp(w_ai, 0.0, AI_EXIT_MODEL_MAX_WEIGHT)
    w_base = 1.0 - w_ai

    blended = _clamp((w_base * baseline) + (w_ai * ai_score))
    diag = {
        "w_ai": w_ai,
        "w_base": w_base,
        "confidence": c,
    }
    return blended, diag


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────


async def predict_exit_score(
    *, features: dict[str, Any], mint: str | None = None
) -> tuple[float, dict[str, Any]]:
    """
    Main entrypoint (LOCK-IN).
    Returns (score, meta).

    meta fields (stable-ish):
      - version, mint
      - provider, used_model
      - baseline_exit_score, baseline_explain
      - ai_raw_score, ai_confidence, ai_meta
      - blended_score, blend_diag
      - safety_floor_applied
      - cached (bool)
      - dry_run (bool)
      - duration_ms
      - reason, error
    """
    t0 = _now_ms()

    if not isinstance(features, dict):
        features = {}

    baseline, baseline_explain = _baseline_exit_score(features)

    meta: dict[str, Any] = {
        "version": AI_EXIT_MODEL_VERSION,
        "mint": mint,
        "provider": None,
        "used_model": None,
        "dry_run": bool(AI_EXIT_MODEL_DRY_RUN),
        "cached": False,
        "baseline_exit_score": float(baseline),
        "baseline_explain": baseline_explain,
        "ai_raw_score": None,
        "ai_confidence": None,
        "ai_meta": None,
        "blended_score": None,
        "blend_diag": None,
        "safety_floor_applied": 0.0,
        "reason": "",
        "error": None,
        "duration_ms": 0,
    }

    # fast path: disabled or provider none
    if (not AI_EXIT_MODEL_ENABLED) or (AI_EXIT_MODEL_PROVIDER == "none"):
        meta["provider"] = AI_EXIT_MODEL_PROVIDER
        meta["reason"] = "model_disabled_use_baseline"
        meta["blended_score"] = float(baseline)
        meta["duration_ms"] = _now_ms() - t0
        return float(baseline), meta

    # circuit breaker
    if not await _circuit_allows_call():
        meta["provider"] = AI_EXIT_MODEL_PROVIDER
        meta["reason"] = "circuit_open_use_baseline"
        meta["error"] = "circuit_open"
        meta["blended_score"] = float(baseline)
        meta["duration_ms"] = _now_ms() - t0
        _metric_inc("ai_exit_model_circuit_open", 1)
        return float(baseline), meta

    # cache
    ck = _hash_key(mint, features)
    cached = await _cache_get(ck)
    if cached is not None:
        meta.update(cached.meta)
        meta["cached"] = True
        meta["duration_ms"] = _now_ms() - t0
        return float(cached.score), meta

    provider = AI_EXIT_MODEL_PROVIDER
    meta["provider"] = provider

    # provider call (guarded by semaphore)
    async with _sem:
        resp: dict[str, Any] = {}
        try:
            if provider == "openai":
                meta["used_model"] = AI_EXIT_MODEL_NAME or None
                resp = await _call_openai(
                    features=features, mint=mint, timeout_s=AI_EXIT_MODEL_TIMEOUT_S
                )
            elif provider == "http":
                meta["used_model"] = AI_EXIT_HTTP_URL or None
                resp = await _call_http(
                    features=features, mint=mint, timeout_s=AI_EXIT_MODEL_TIMEOUT_S
                )
            else:
                meta["reason"] = "unknown_provider_use_baseline"
                meta["error"] = f"unknown_provider:{provider}"
                meta["blended_score"] = float(baseline)
                meta["duration_ms"] = _now_ms() - t0
                return float(baseline), meta
        except Exception as e:
            resp = {}
            meta["error"] = f"provider_exception:{type(e).__name__}"

    ai_score, ai_conf, ai_meta = _parse_model_response(resp)
    meta["ai_raw_score"] = ai_score
    meta["ai_confidence"] = ai_conf
    meta["ai_meta"] = ai_meta if ai_meta else None

    # failure -> baseline (fail-open)
    if ai_score is None:
        await _record_failure()
        _metric_inc("ai_exit_model_fail", 1)

        if AI_EXIT_MODEL_FAIL_OPEN:
            meta["reason"] = "ai_unavailable_use_baseline"
            meta["blended_score"] = float(baseline)
            meta["duration_ms"] = _now_ms() - t0
            # cache baseline-only briefly too (optional)
            await _cache_put(
                ck,
                _CacheItem(
                    exp_ms=_now_ms() + int(AI_EXIT_MODEL_CACHE_TTL_S * 1000),
                    score=float(baseline),
                    meta=meta,
                ),
            )
            return float(baseline), meta

        # fail-closed is strongly discouraged, but supported
        fb = float(AI_EXIT_MODEL_FALLBACK_SCORE)
        meta["reason"] = "ai_unavailable_use_fallback"
        meta["blended_score"] = fb
        meta["duration_ms"] = _now_ms() - t0
        await _cache_put(
            ck,
            _CacheItem(
                exp_ms=_now_ms() + int(AI_EXIT_MODEL_CACHE_TTL_S * 1000),
                score=fb,
                meta=meta,
            ),
        )
        return fb, meta

    await _record_success()
    _metric_inc("ai_exit_model_ok", 1)

    blended, diag = _blend(baseline=baseline, ai_score=float(ai_score), confidence=float(ai_conf))
    meta["blend_diag"] = diag

    # safety floor
    floor = _safety_floor_from_features(features)
    if floor > 0:
        blended2 = max(blended, floor)
        meta["safety_floor_applied"] = float(floor)
        blended = blended2

    blended = _clamp(blended)
    meta["blended_score"] = float(blended)

    # reason string (stable & compact)
    if ai_conf < AI_EXIT_MODEL_MIN_CONF:
        meta["reason"] = "low_conf_blend_baseline"
    else:
        meta["reason"] = "ai_blended"

    meta["duration_ms"] = _now_ms() - t0

    if AI_EXIT_MODEL_DEBUG and not AI_EXIT_MODEL_DRY_RUN:
        logger.info(
            "AI_EXIT mint=%s provider=%s raw=%.3f conf=%.3f baseline=%.3f blended=%.3f w_ai=%.3f floor=%.3f label=%s",
            mint,
            provider,
            float(ai_score),
            float(ai_conf),
            float(baseline),
            float(blended),
            float((diag or {}).get("w_ai", 0.0)),
            float(floor),
            (ai_meta or {}).get("label") if isinstance(ai_meta, dict) else None,
        )

    # cache result
    await _cache_put(
        ck,
        _CacheItem(
            exp_ms=_now_ms() + int(AI_EXIT_MODEL_CACHE_TTL_S * 1000),
            score=float(blended),
            meta=meta,
        ),
    )

    return float(blended), meta


# ──────────────────────────────────────────────────────────────────────────────
# Local smoke
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":  # pragma: no cover

    async def _smoke() -> None:
        f = {
            "pnl_pct": 22.0,
            "max_up_pct": 55.0,
            "dd_pct": -18.0,
            "fraud_risk_pct": 8.0,
            "rug_prob_pct": 4.0,
            "whale_exit_pressure": 0.10,
            "liq_usd": 18_000,
            "vol_pct_30s": -12.0,
            "stagnation_min": 14.0,
            "last_move_pct": 1.2,
            "risk_profile": "balanced",
            "risk_mode": 1,
            "ai_capacity_delta": 0.05,
            "regime_label_numeric": 0.2,
            "regime_stress_score": 0.1,
        }
        s, m = await predict_exit_score(features=f, mint="TESTMINT")
        print("score:", s)
        print("meta:", json.dumps(m, indent=2))

    asyncio.run(_smoke())
