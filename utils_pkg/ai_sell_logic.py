from __future__ import annotations

"""
utils_pkg/ai_sell_logic.py

V3+++ / QUANT-GRADE SELL INTELLIGENCE ENGINE
===========================================

High-level role
---------------
This module is the "SELL brain" of the system.

Given:
    • Feature dict (FeatureBuilder V3 output at SELL time)
    • Trade state (entry price, size, max_up_pct, hold_time, etc.)
    • Risk context (risk_manager: daily DD, risk profile, regime gates...)
    • Strategy context (self_learning_ai_strategy overrides)
    • Regime / environment info (trend, stress, volatility)
    • LLM / fraud signals (rug / scam probabilities)
    • Microstructure and predictive signals (sell_alpha, vol crush, etc.)

It decides:
    • action: "sell" | "hold" | "partial"
    • pct: fraction of position to sell (0.0–1.0)
    • exit_score in [0, 1]
    • reason string
    • metadata for logging / training (features, ctx, etc.)

Integration points
------------------
Called by:
    • exit_engine.on_buy_filled → schedule_after_buy(payload)
    • exit_engine.on_trade_tick → on_trade_tick(raw_event)
    • exit_engine.follow-through dead coin → schedule_force_exit / request_exit / exit_now

Uses:
    • core_utils.redis for per-mint state: ai_sell:{mint}
    • risk_manager (optional)
    • self_learning_ai_strategy (optional)
    • data_logger (optional)
    • auto_exit_worker.exit_engine to enqueue exits in Redis

Public async API (used by exit_engine / others)
-----------------------------------------------
    async schedule_after_buy(ev: dict) -> None
    async on_trade_tick(raw_event: dict) -> None
    async schedule_force_exit(ev: dict) -> None
    async force_exit(ev: dict) -> None          # alias
    async request_exit(ev: dict) -> None        # alias
    async exit_now(ev: dict) -> None            # alias

All functions are best-effort and never raise up to callers.
"""

import asyncio
import json
import os
import time
from typing import Any, Dict, Optional, Tuple

from . import core_utils

# Optional modules
redis = getattr(core_utils, "redis", None)

try:  # risk manager (optional)
    from utils_pkg import risk_manager as _RM  # type: ignore[attr-defined]
    RISK = getattr(_RM, "risk_manager", _RM)
except Exception:  # noqa: BLE001
    RISK = None  # type: ignore[assignment]

try:  # self-learning strategy brain (optional)
    from utils_pkg import self_learning_ai_strategy as SL  # type: ignore[attr-defined]
except Exception:  # noqa: BLE001
    SL = None  # type: ignore[assignment]

try:  # data logger (optional)
    from utils_pkg import data_logger as DL  # type: ignore[attr-defined]
except Exception:  # noqa: BLE001
    DL = None  # type: ignore[assignment]

# To avoid circular import, we only import auto_exit_worker.exit_engine,
# NOT exit_engine itself.
try:  # noqa: BLE001
    from utils_pkg.auto_exit_worker import exit_engine as EXIT_ENGINE  # type: ignore[attr-defined]
except Exception:  # noqa: BLE001
    EXIT_ENGINE = None  # type: ignore[assignment]

# Basic logger
import logging

log = logging.getLogger("sniper.ai_sell")
if not log.handlers:
    logging.basicConfig(level=logging.INFO)

# ==============================================================================
# Env helpers / feature flags
# ==============================================================================

def _env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "t", "yes", "y", "on")


def _env_float(key: str, default: float) -> float:
    raw = os.getenv(key)
    if raw is None or raw == "":
        return float(default)
    try:
        return float(raw)
    except Exception:  # noqa: BLE001
        return float(default)


def _env_int(key: str, default: int) -> int:
    raw = os.getenv(key)
    if raw is None or raw == "":
        return int(default)
    try:
        return int(raw)
    except Exception:  # noqa: BLE001
        return int(default)


def _env_json(key: str) -> Optional[dict]:
    raw = os.getenv(key)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:  # noqa: BLE001
        log.warning("ai_sell_logic: could not parse JSON from %s", key)
        return None


def _clamp(x: float, lo: float, hi: float) -> float:
    return float(max(lo, min(hi, x)))


def _clip01(x: float) -> float:
    return _clamp(x, 0.0, 1.0)


def _smoothstep(x: float) -> float:
    """0->1 smooth transition."""
    x = _clip01(x)
    return x * x * (3.0 - 2.0 * x)


def _safe_float(d: Dict[str, Any], key: str, default: float = 0.0) -> float:
    try:
        return float(d.get(key, default))
    except Exception:  # noqa: BLE001
        return default


def _safe_bool(d: Dict[str, Any], key: str, default: bool = False) -> bool:
    v = d.get(key, default)
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().lower() in {"1", "true", "yes", "y", "on"}
    return default


def _safe_int(d: Dict[str, Any], key: str, default: int = 0) -> int:
    try:
        return int(d.get(key, default))
    except Exception:  # noqa: BLE001
        return default


# Global feature flags
AI_SELL_ENABLED: bool = _env_bool("AI_SELL_ENABLED", True)
AI_SELL_DRYRUN: bool = _env_bool("AI_SELL_DRYRUN", False)
AI_SELL_DEBUG: bool = _env_bool("AI_SELL_DEBUG", False)
AI_SELL_ATTACH_META: bool = _env_bool("AI_SELL_ATTACH_META", True)

# How long to keep ai_sell state in Redis (seconds)
AI_SELL_STATE_TTL_SEC: int = _env_int("AI_SELL_STATE_TTL_SEC", 7200)

LAMPORTS_PER_SOL: int = 1_000_000_000

# ------------------------------------------------------------------------------
# AI / model config (optional)
# ------------------------------------------------------------------------------

AI_SELL_MODEL = (
    os.getenv("AI_SELL_MODEL_NAME", "").strip()
    or os.getenv("AI_SELL_MODEL", "").strip()
)

AI_SELL_MODEL_ROUTING = _env_json("AI_SELL_MODEL_ROUTING_JSON") or {}
AI_SELL_DEFAULT_HEAD = (
    os.getenv("AI_SELL_DEFAULT_HEAD", "default").strip() or "default"
)

AI_SELL_AI_ENABLED = _env_bool("AI_SELL_AI_ENABLED", True)
AI_SELL_AI_WEIGHT = _clip01(_env_float("AI_SELL_AI_WEIGHT", 0.6))
AI_SELL_MIN_AI_SCORE = _clip01(_env_float("AI_SELL_MIN_AI_SCORE", 0.0))
AI_SELL_TIMEOUT_SEC = max(1.0, _env_float("AI_SELL_TIMEOUT_SEC", 8.0))
AI_SELL_MAX_FEATURE_BYTES = _env_int("AI_SELL_MAX_FEATURE_BYTES", 50_000)
AI_SELL_STRICT = _env_bool("AI_SELL_STRICT", False)

# Thresholds for sell_score→action
AI_SELL_STRONG_SCORE = _clip01(_env_float("AI_SELL_STRONG_SCORE", 0.75))
AI_SELL_MID_SCORE = _clip01(_env_float("AI_SELL_MID_SCORE", 0.55))

# Dynamic profit-protection from peak (giveback thresholds, in %)
AI_SELL_MAX_GIVEBACK_SOFT = _env_float("AI_SELL_MAX_GIVEBACK_SOFT", 35.0)
AI_SELL_MAX_GIVEBACK_HARD = _env_float("AI_SELL_MAX_GIVEBACK_HARD", 55.0)

# ==============================================================================
# OpenAI client shim (shared style with ai_entry_logic)
# ==============================================================================

_CLIENT: Any = None
_USE_RESPONSES_API = False


def _ensure_client() -> Optional[Any]:
    """
    Try to import either the new `OpenAI` client or legacy `openai` module.
    Returns a client object or None if unavailable.
    """
    global _CLIENT, _USE_RESPONSES_API

    if _CLIENT is not None:
        return _CLIENT

    # New-style client
    try:
        from openai import OpenAI  # type: ignore

        _CLIENT = OpenAI()
        _USE_RESPONSES_API = True
        log.info("ai_sell_logic: using OpenAI responses client")
        return _CLIENT
    except Exception:  # noqa: BLE001
        pass

    # Legacy client
    try:
        import openai  # type: ignore

        if not getattr(openai, "api_key", None):
            openai.api_key = os.getenv("OPENAI_API_KEY", "")
        _CLIENT = openai
        _USE_RESPONSES_API = False
        log.info("ai_sell_logic: using legacy openai.ChatCompletion client")
        return _CLIENT
    except Exception:  # noqa: BLE001
        log.warning("ai_sell_logic: no OpenAI client available; AI disabled")
        _CLIENT = None
        return None


def _pick_sell_model(
    features: Dict[str, Any],
    trade: Dict[str, Any],
    risk_ctx: Dict[str, Any],
) -> Optional[str]:
    """
    Decide which fine-tuned model to use.

    Priority:
      1) features["sell_model_name"]
      2) routing map AI_SELL_MODEL_ROUTING_JSON using:
           - features["exit_head"]
           - features["segment"]
           - risk_ctx["risk_profile"]
      3) global AI_SELL_MODEL
    """
    explicit = str(features.get("sell_model_name") or "").strip()
    if explicit:
        return explicit

    if not AI_SELL_MODEL_ROUTING:
        return AI_SELL_MODEL or None

    head = (
        str(features.get("exit_head") or "").strip()
        or str(features.get("segment") or "").strip()
        or str(risk_ctx.get("risk_profile") or "").strip()
        or AI_SELL_DEFAULT_HEAD
    )

    if head in AI_SELL_MODEL_ROUTING:
        return str(AI_SELL_MODEL_ROUTING[head])

    if "default" in AI_SELL_MODEL_ROUTING:
        return str(AI_SELL_MODEL_ROUTING["default"])

    return AI_SELL_MODEL or None


async def _ai_sell_score(
    *,
    features: Dict[str, Any],
    trade: Dict[str, Any],
    risk_ctx: Dict[str, Any],
) -> Optional[float]:
    """
    Optional LLM-based sell score in [0,1].

    - If AI is disabled or model/key missing → returns None.
    - If anything fails → returns None unless AI_SELL_STRICT, in which case we re-raise.
    """
    if not AI_SELL_AI_ENABLED:
        return None
    if not os.getenv("OPENAI_API_KEY"):
        if AI_SELL_DEBUG:
            log.debug("ai_sell_logic: no OPENAI_API_KEY – skipping AI sell call")
        return None

    model = _pick_sell_model(features, trade, risk_ctx)
    if not model:
        return None

    client = _ensure_client()
    if client is None:
        return None

    # Build compact payload
    payload = {
        "features": features,
        "trade": trade,
        "risk_ctx": {k: risk_ctx.get(k) for k in (
            "risk_profile",
            "daily_drawdown_pct",
            "daily_realized_pnl_pct",
            "recent_loss_streak",
            "rolling_win_rate_pct",
            "regime_entry_gate_factor",
            "ai_capacity_delta",
        ) if k in risk_ctx},
        "task": "score_exit_probability_and_action",
    }

    try:
        blob = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
        if len(blob.encode("utf-8")) > AI_SELL_MAX_FEATURE_BYTES:
            # crude prune: keep only top-level trade + a few composite features
            keep_feat = {
                "symbol",
                "mint",
                "pnl_pct",
                "max_up_pct",
                "sell_alpha",
                "buy_alpha",
                "micro_trend_shift",
                "liq_usd",
                "whale_exit_pressure",
                "fraud_risk_pct",
                "exit_quality_score",
                "exit_tier_bucket",
                "exit_micro_alpha",
                "vol_pct_5s",
                "vol_pct_30s",
            }
            compact = {
                "features": {k: v for k, v in features.items() if k in keep_feat},
                "trade": trade,
                "risk_ctx": payload["risk_ctx"],
                "task": payload["task"],
            }
            blob = json.dumps(compact, separators=(",", ":"), ensure_ascii=False)
    except Exception:  # noqa: BLE001
        blob = json.dumps({"error": "encode_fail"})

    prompt = (
        "You are a crypto exit classifier.\n"
        "Input is JSON for a single open trade (features, trade, risk_ctx).\n"
        "Output ONLY JSON of the form:\n"
        '{"sell_score": <0-1>, "action": "sell|hold|partial", "sell_pct": <0-1>}.\n'
        "sell_score is the probability that SELLING NOW is better than holding.\n"
        f"Input:\n{blob}"
    )

    try:
        if _USE_RESPONSES_API:

            def _do_req() -> Any:
                return client.responses.create(  # type: ignore[call-arg]
                    model=model,
                    input=prompt,
                    response_format={"type": "json_object"},
                )

            resp = await asyncio.wait_for(
                asyncio.to_thread(_do_req),
                timeout=AI_SELL_TIMEOUT_SEC,
            )
            content_str = str(resp)
            try:
                out = resp.output[0].content[0].text  # type: ignore[attr-defined,index]
                content_str = getattr(out, "value", str(out))
            except Exception:  # noqa: BLE001
                pass
        else:

            def _do_req_legacy() -> Any:
                return client.ChatCompletion.create(  # type: ignore[call-arg]
                    model=model,
                    messages=[
                        {"role": "system", "content": "You output JSON only."},
                        {"role": "user", "content": prompt},
                    ],
                    temperature=0.0,
                )

            resp = await asyncio.wait_for(
                asyncio.to_thread(_do_req_legacy),
                timeout=AI_SELL_TIMEOUT_SEC,
            )
            content_str = resp["choices"][0]["message"]["content"]  # type: ignore[index]

        try:
            obj = json.loads(content_str)
            score = float(obj.get("sell_score", obj.get("score", 0.0)))
            score = _clip01(score)
        except Exception:  # noqa: BLE001
            # Fallback: try parse raw float
            try:
                score = float(str(content_str).strip())
                score = _clip01(score)
            except Exception:  # noqa: BLE001
                if AI_SELL_DEBUG:
                    log.warning(
                        "ai_sell_logic: could not parse AI sell score content=%r",
                        str(content_str)[:200],
                    )
                return None

        if AI_SELL_MIN_AI_SCORE > 0 and score < AI_SELL_MIN_AI_SCORE:
            return None

        return score

    except Exception as e:  # noqa: BLE001
        if AI_SELL_DEBUG:
            log.warning("ai_sell_logic: OpenAI sell call failed: %s", str(e)[:200])
        if AI_SELL_STRICT:
            raise
        return None


# ==============================================================================
# SELL CONFIG DEFAULTS (can be overridden by strategy / SL brain)
# ==============================================================================

DEFAULT_MIN_PROFIT_PCT        = 2.0       # below this → low profit
DEFAULT_TP1_PCT               = 4.0       # first take profit tier
DEFAULT_TP2_PCT               = 8.0       # second tier
DEFAULT_TP3_PCT               = 15.0      # strong target

DEFAULT_STOP_LOSS_PCT         = -10.0     # emergency hard stop-loss

DEFAULT_MIN_HOLD_SEC          = 10        # don't insta-sell unless disaster
DEFAULT_MAX_HOLD_SEC          = 7200      # after 2h, bias toward exit

DEFAULT_VOL_CRUSH_THRESHOLD   = -35.0     # vol collapse exit
DEFAULT_VOL_EXPAND_THRESHOLD  = +50.0     # if vol expansion + trend strong → hold

# ==============================================================================
# Redis helpers for per-mint ai_sell state
# ==============================================================================

def _redis_available() -> bool:
    return redis is not None


def _state_key(mint: str) -> str:
    return f"ai_sell:{mint}"


async def _load_state(mint: str) -> Dict[str, Any]:
    """
    Load per-mint AI sell state from Redis (or return blank dict).
    """
    if not _redis_available():
        return {}

    try:
        raw = await redis.get(_state_key(mint))
        if not raw:
            return {}
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")
        data = json.loads(raw)
        if not isinstance(data, dict):
            return {}
        return data
    except Exception:  # noqa: BLE001
        return {}


async def _save_state(mint: str, state: Dict[str, Any]) -> None:
    """
    Store AI sell state to Redis (best-effort).
    """
    if not _redis_available():
        return
    try:
        await redis.set(_state_key(mint), json.dumps(state), ex=AI_SELL_STATE_TTL_SEC)
    except Exception:  # noqa: BLE001
        if AI_SELL_DEBUG:
            log.debug("AI_SELL_STATE_SAVE_FAIL", exc_info=True)


async def _delete_state(mint: str) -> None:
    if not _redis_available():
        return
    try:
        await redis.delete(_state_key(mint))
    except Exception:  # noqa: BLE001
        if AI_SELL_DEBUG:
            log.debug("AI_SELL_STATE_DELETE_FAIL", exc_info=True)


# ==============================================================================
# Risk-manager / strategy / regime context helpers
# ==============================================================================

def _get_risk_ctx() -> Dict[str, Any]:
    """
    Ask risk_manager (if any) for AI sell context.

    We try several method names for compatibility:
      - get_ai_ctx()
      - get_sell_ctx()
      - get_ctx()
    """
    if RISK is None:
        return {}

    method_candidates = ("get_ai_ctx", "get_sell_ctx", "get_ctx")
    fn = None
    for name in method_candidates:
        fn = getattr(RISK, name, None)
        if callable(fn):
            break

    if fn is None:
        return {}

    try:
        ctx = fn()  # type: ignore[misc]
        if isinstance(ctx, dict):
            return ctx
    except Exception:  # noqa: BLE001
        log.debug("AI_SELL_RISK_CTX_FAIL", exc_info=True)

    return {}


def _get_strat_overrides(
    mint: str,
    base_cfg: Dict[str, Any],
    state: Dict[str, Any],
    features: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Ask self_learning_ai_strategy (if present) for live overrides.

    We try several hook names:
      - get_live_sell_overrides(...)
      - get_live_overrides(...)
      - get_live_config(...)
    """
    if SL is None:
        return base_cfg

    method_candidates = (
        "get_live_sell_overrides",
        "get_live_overrides",
        "get_live_config",
    )
    fn = None
    for name in method_candidates:
        fn = getattr(SL, name, None)
        if callable(fn):
            break

    if fn is None:
        return base_cfg

    payload = {
        "mint": mint,
        "state": state,
        "features": features,
        "base_cfg": base_cfg,
    }

    try:
        cfg = fn(payload)  # type: ignore[misc]
        if isinstance(cfg, dict):
            merged = dict(base_cfg)
            merged.update(cfg)
            return merged
    except Exception:  # noqa: BLE001
        log.debug("AI_SELL_STRAT_OVERRIDES_FAIL", exc_info=True)

    return base_cfg


def _base_sell_cfg_from_env() -> Dict[str, Any]:
    """
    Build a base sell configuration from env defaults.
    Self-learning / risk manager can override this.
    """
    return {
        "MIN_PROFIT_PCT": float(os.getenv("EXIT_MIN_PROFIT_PCT", DEFAULT_MIN_PROFIT_PCT)),
        "TP1_PCT": float(os.getenv("EXIT_TP1_PCT", DEFAULT_TP1_PCT)),
        "TP2_PCT": float(os.getenv("EXIT_TP2_PCT", DEFAULT_TP2_PCT)),
        "TP3_PCT": float(os.getenv("EXIT_TP3_PCT", DEFAULT_TP3_PCT)),
        "STOP_LOSS_PCT": float(os.getenv("EXIT_STOP_LOSS_PCT", DEFAULT_STOP_LOSS_PCT)),
        "MIN_HOLD_SEC": float(os.getenv("EXIT_MIN_HOLD_SEC", DEFAULT_MIN_HOLD_SEC)),
        "MAX_HOLD_SEC": float(os.getenv("EXIT_MAX_HOLD_SEC", DEFAULT_MAX_HOLD_SEC)),
        "VOL_CRUSH_THRESHOLD": float(
            os.getenv("EXIT_VOL_CRUSH_THRESHOLD", DEFAULT_VOL_CRUSH_THRESHOLD)
        ),
        "VOL_EXPAND_THRESHOLD": float(
            os.getenv("EXIT_VOL_EXPAND_THRESHOLD", DEFAULT_VOL_EXPAND_THRESHOLD)
        ),
    }


# ==============================================================================
# Core SELL decision model (pure logic)
# ==============================================================================

def compute_sell_decision(
    *,
    features: Dict[str, Any],
    trade: Dict[str, Any],
    risk_ctx: Dict[str, Any],
    strat_ctx: Dict[str, Any],
    now_ts: Optional[float] = None,
    ai_score: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Main Sell IQ engine (pure function, testable, no side-effects).

    Inputs:
        features: FeatureBuilder V3 dict
        trade: {
            "entry_price": float,
            "current_price": float,
            "pnl_pct": float,
            "max_up_pct": float,
            "hold_sec": float,
        }
        risk_ctx: risk:ctx from risk_manager
        strat_ctx: decoded strategy:live config (overrides from SL brain)
        ai_score: optional LLM-based probability that SELL NOW is optimal

    Returns:
        {
          "action": "sell" | "partial" | "hold",
          "pct": float 0..1,
          "reason": str,
          "sell_score": float 0..1,
          "sell_alpha": float,
          "meta": {...}
        }
    """

    now_ts = now_ts or time.time()

    # --- Extract trade stats ---
    pnl_pct = _safe_float(trade, "pnl_pct", 0.0)
    max_up_pct = _safe_float(trade, "max_up_pct", pnl_pct)
    hold_sec = _safe_float(trade, "hold_sec", 0.0)

    # Track giveback from peak
    giveback_pct = max(0.0, max_up_pct - pnl_pct)

    # --- Extract predictive & microstructure features ---
    sell_alpha = _safe_float(features, "sell_alpha", 0.0)  # predictive downward pressure
    buy_alpha = _safe_float(features, "buy_alpha", 0.0)   # inverse (trend strength)
    micro_shift = _safe_float(features, "micro_trend_shift", 0.0)  # deterioration?
    vol_5s = _safe_float(features, "vol_pct_5s", 0.0)
    vol_30s = _safe_float(features, "vol_pct_30s", 0.0)
    liquidity = _safe_float(features, "liq_usd", 0.0)
    whale_exit = _safe_float(features, "whale_exit_pressure", 0.0)  # from smart_wallets
    rug_prob = _safe_float(features, "fraud_risk_pct", 0.0)

    exit_quality = _safe_float(features, "exit_quality_score", 0.0)
    exit_micro_alpha = _safe_float(features, "exit_micro_alpha", 0.0)
    exit_tier_bucket = _safe_float(features, "exit_tier_bucket", 0.0)

    # --- Risk manager context ---
    risk_profile = str(risk_ctx.get("risk_profile", "balanced"))
    ai_capacity_delta = _safe_float(risk_ctx, "ai_capacity_delta", 0.0)
    regime_gate = _safe_float(risk_ctx, "regime_entry_gate_factor", 1.0)
    daily_dd_pct = _safe_float(risk_ctx, "daily_drawdown_pct", 0.0)
    daily_pnl_pct = _safe_float(risk_ctx, "daily_realized_pnl_pct", 0.0)
    loss_streak = _safe_float(risk_ctx, "recent_loss_streak", 0.0)
    win_rate_pct = _safe_float(risk_ctx, "rolling_win_rate_pct", 0.0)

    # --- Strategy context (live tunable) ---
    min_profit_pct = float(
        strat_ctx.get("MIN_PROFIT_PCT", DEFAULT_MIN_PROFIT_PCT)
    )
    tp1 = float(strat_ctx.get("TP1_PCT", DEFAULT_TP1_PCT))
    tp2 = float(strat_ctx.get("TP2_PCT", DEFAULT_TP2_PCT))
    tp3 = float(strat_ctx.get("TP3_PCT", DEFAULT_TP3_PCT))

    stop_loss_pct = float(
        strat_ctx.get("STOP_LOSS_PCT", DEFAULT_STOP_LOSS_PCT)
    )
    min_hold_sec = float(
        strat_ctx.get("MIN_HOLD_SEC", DEFAULT_MIN_HOLD_SEC)
    )
    max_hold_sec = float(
        strat_ctx.get("MAX_HOLD_SEC", DEFAULT_MAX_HOLD_SEC)
    )

    vol_crush_thr = float(
        strat_ctx.get("VOL_CRUSH_THRESHOLD", DEFAULT_VOL_CRUSH_THRESHOLD)
    )
    vol_expand_thr = float(
        strat_ctx.get("VOL_EXPAND_THRESHOLD", DEFAULT_VOL_EXPAND_THRESHOLD)
    )

    # 1) HARD RISK-RULES: INSTANT EXITS
    if rug_prob >= 60 or whale_exit >= 0.8:
        return _return_sell("RUG/WHALE EXIT RISK", 1.0, 1.0, sell_alpha, features)

    if pnl_pct <= stop_loss_pct:
        return _return_sell("HARD STOP LOSS", 1.0, 1.0, sell_alpha, features)

    if daily_dd_pct <= -20.0:
        return _return_sell("ACCOUNT DD PROTECTION", 1.0, 1.0, sell_alpha, features)

    # 2) GAIN PROTECTION: EXCESSIVE GIVEBACK FROM PEAK
    if max_up_pct > 0:
        soft_giveback = AI_SELL_MAX_GIVEBACK_SOFT
        hard_giveback = AI_SELL_MAX_GIVEBACK_HARD

        # Slightly tighten for defensive profiles
        if risk_profile in {"ultra_defensive", "defensive"}:
            soft_giveback -= 5.0
            hard_giveback -= 10.0
        elif risk_profile in {"aggressive", "degenerate"}:
            soft_giveback += 5.0
            hard_giveback += 5.0

        if giveback_pct >= hard_giveback and pnl_pct > min_profit_pct:
            return _return_sell(
                f"GIVEBACK_HARD({giveback_pct:.1f}%)",
                1.0,
                1.0,
                sell_alpha,
                features,
            )
        if giveback_pct >= soft_giveback and pnl_pct > min_profit_pct:
            return _return_sell(
                f"GIVEBACK_SOFT({giveback_pct:.1f}%)",
                0.95,
                0.5,
                sell_alpha,
                features,
            )

    # 3) SOFT STOP-LOSS TIGHTENING
    if pnl_pct < 0 and sell_alpha > 0.6 and micro_shift < -0.3:
        return _return_sell(
            "NEGATIVE + DOWNSIDE SIGNAL", 0.95, 1.0, sell_alpha, features
        )

    # 4) VOLATILITY EXIT LOGIC
    if vol_30s <= vol_crush_thr:
        return _return_sell("VOLATILITY CRUSH", 1.0, 1.0, sell_alpha, features)

    # Trend continuation exception: if vol expanding + buy_alpha strong
    if vol_30s >= vol_expand_thr and buy_alpha > 0.55 and pnl_pct > min_profit_pct:
        # hold to maximize upside
        return _return_hold("TREND CONTINUATION", sell_alpha, features)

    # 5) MIN HOLD PERIOD
    if hold_sec < min_hold_sec and pnl_pct > stop_loss_pct:
        return _return_hold("MIN HOLD GUARD", sell_alpha, features)

    # 6) TAKE PROFIT TIERS (PARTIAL + FULL)
    if pnl_pct >= tp3:
        # At ultra high profits, self-learning brain might want more partials,
        # but baseline: take 30%
        return _return_sell("TP3 HIT", 1.0, 0.30, sell_alpha, features)

    if pnl_pct >= tp2:
        return _return_sell("TP2 HIT", 1.0, 0.20, sell_alpha, features)

    if pnl_pct >= tp1:
        # if predictive alpha is negative → exit more
        if sell_alpha > 0.5:
            return _return_sell(
                "TP1 HIT + NEG ALPHA", 1.0, 0.30, sell_alpha, features
            )
        return _return_sell("TP1 HIT", 1.0, 0.15, sell_alpha, features)

    # 7) DEAD-COIN DETECTION
    dead_coin = (
        micro_shift < -0.5
        and vol_5s < -20
        and liquidity < 15_000
        and buy_alpha < 0.2
    )
    if dead_coin and pnl_pct > 0:
        return _return_sell("DEAD COIN EXIT", 1.0, 1.0, sell_alpha, features)

    # 8) PREDICTIVE SELL-SCORE
    sell_score = _compute_sell_score(
        pnl_pct=pnl_pct,
        sell_alpha=sell_alpha,
        micro_shift=micro_shift,
        whale_exit=whale_exit,
        rug_prob=rug_prob,
        liquidity=liquidity,
        risk_profile=risk_profile,
        ai_capacity_delta=ai_capacity_delta,
        regime_gate=regime_gate,
        hold_sec=hold_sec,
        max_hold_sec=max_hold_sec,
        exit_quality=exit_quality,
        exit_micro_alpha=exit_micro_alpha,
        exit_tier_bucket=exit_tier_bucket,
        daily_dd_pct=daily_dd_pct,
        daily_pnl_pct=daily_pnl_pct,
        loss_streak=loss_streak,
        win_rate_pct=win_rate_pct,
    )

    # Optional blend with LLM score
    if ai_score is not None:
        ai_score = _clip01(ai_score)
        sell_score = _clip01(
            AI_SELL_AI_WEIGHT * ai_score + (1.0 - AI_SELL_AI_WEIGHT) * sell_score
        )

    # threshold logic using configurable bands
    high_thr = AI_SELL_STRONG_SCORE
    mid_thr = AI_SELL_MID_SCORE

    # Slight threshold adjustments by risk profile
    if risk_profile in {"ultra_defensive", "defensive"}:
        high_thr = max(0.6, high_thr - 0.05)
        mid_thr = max(0.4, mid_thr - 0.05)
    elif risk_profile in {"aggressive", "degenerate"}:
        high_thr = min(0.95, high_thr + 0.05)
        mid_thr = min(0.9, mid_thr + 0.05)

    if sell_score >= high_thr:
        return _return_sell(
            "SELL_SCORE_HIGH", sell_score, 1.0, sell_alpha, features
        )

    if mid_thr <= sell_score < high_thr:
        # mid tier: partial exit, scaled by exit tier
        base_pct = 0.4
        if exit_tier_bucket >= 3.0:
            base_pct = 0.3
        elif exit_tier_bucket <= 1.0:
            base_pct = 0.5
        return _return_sell(
            "SELL_SCORE_MID", sell_score, base_pct, sell_alpha, features
        )

    # HOLD if not enough momentum + predictive negatives not strong
    return _return_hold("HOLD_SCORE", sell_alpha, features)


def _compute_sell_score(
    *,
    pnl_pct: float,
    sell_alpha: float,
    micro_shift: float,
    whale_exit: float,
    rug_prob: float,
    liquidity: float,
    risk_profile: str,
    ai_capacity_delta: float,
    regime_gate: float,
    hold_sec: float,
    max_hold_sec: float,
    exit_quality: float,
    exit_micro_alpha: float,
    exit_tier_bucket: float,
    daily_dd_pct: float,
    daily_pnl_pct: float,
    loss_streak: float,
    win_rate_pct: float,
) -> float:
    """
    Unified sell scoring model. Output in [0,1].
    """

    # profit factor (negative pnl → force sell)
    if pnl_pct < 0:
        profit_factor = 1.0
    else:
        # map 0..20% → 0..1
        profit_factor = _smoothstep(pnl_pct / 20.0)

    # predictive alpha (sell pressure)
    alpha_factor = _clip01(sell_alpha)

    # microstructure deterioration
    micro_factor = _clip01((-micro_shift) / 1.5)

    # whale exit / fraud
    whale_factor = _clip01(whale_exit)
    rug_factor = _clip01(rug_prob / 100.0)

    # time decay: longer holds → higher sell pressure
    hold_factor = _clip01(hold_sec / max(max_hold_sec, 1.0))

    # structural + exit-quality terms
    exit_quality_term = 0.0
    if exit_quality != 0.0:
        exit_quality_term += (exit_quality - 0.5) * 0.25
    exit_quality_term += exit_micro_alpha * 0.25
    exit_quality_term += max(0.0, exit_tier_bucket - 1.0) * 0.08

    # risk profile adjustment
    profile_mult = {
        "ultra_defensive": 1.35,
        "defensive": 1.15,
        "balanced": 1.00,
        "aggressive": 0.85,
        "degenerate": 0.70,
    }.get(risk_profile, 1.00)

    # regime & capacity mod
    regime_mult = _clip01(regime_gate) * 1.2  # clamp 0..1, make 0.5→0.6 etc
    regime_mult = _clamp(regime_mult, 0.4, 1.4)
    capacity_mult = _clamp(1.0 + ai_capacity_delta, 0.5, 1.5)

    # performance feedback: DD / win-rate / streaks
    perf_term = 0.0
    if daily_dd_pct <= -10.0:
        perf_term += 0.15
    if daily_pnl_pct <= -3.0:
        perf_term += 0.10
    if loss_streak >= 3.0:
        perf_term += min((loss_streak - 2.0) / 3.0, 1.0) * 0.15
    if win_rate_pct >= 60.0 and daily_pnl_pct >= 4.0:
        perf_term -= 0.08  # doing well → slightly more patient

    # liquidity risk: ultra illiquid coins → slightly faster exits
    liq_term = 0.0
    if liquidity < 8_000:
        liq_term += 0.12
    elif liquidity < 20_000:
        liq_term += 0.05

    # blend model
    score = (
        0.22 * profit_factor
        + 0.22 * alpha_factor
        + 0.18 * micro_factor
        + 0.10 * whale_factor
        + 0.08 * rug_factor
        + 0.10 * hold_factor
        + 0.10 * _clip01(exit_quality_term + perf_term + liq_term)
    )

    score *= profile_mult
    score *= regime_mult
    score *= capacity_mult

    return _clip01(score)


# ==============================================================================
# RETURN HELPERS
# ==============================================================================

def _return_sell(
    reason: str,
    score: float,
    pct: float,
    sell_alpha: float,
    features: Dict[str, Any],
) -> Dict[str, Any]:
    meta: Dict[str, Any] = {}
    if AI_SELL_ATTACH_META:
        meta = {
            "reason": reason,
            "label": "sell",
            "features": features,
        }
    return {
        "action": "sell",
        "pct": float(_clamp(pct, 0.0, 1.0)),
        "reason": reason,
        "sell_score": float(_clip01(score)),
        "sell_alpha": float(sell_alpha),
        "meta": meta,
    }


def _return_hold(
    reason: str,
    sell_alpha: float,
    features: Dict[str, Any],
) -> Dict[str, Any]:
    meta: Dict[str, Any] = {}
    if AI_SELL_ATTACH_META:
        meta = {
            "reason": reason,
            "label": "hold",
            "features": features,
        }
    return {
        "action": "hold",
        "pct": 0.0,
        "reason": reason,
        "sell_score": 0.0,
        "sell_alpha": float(sell_alpha),
        "meta": meta,
    }


# ==============================================================================
# Internal helpers: mint / price / trade extraction
# ==============================================================================

def _extract_mint(ev: Dict[str, Any]) -> Optional[str]:
    for k in ("mint", "mintAddress", "mint_address", "id", "tokenMint"):
        v = ev.get(k)
        if isinstance(v, str) and v:
            return v
    return None


def _extract_price(ev: Dict[str, Any]) -> Optional[float]:
    for key in (
        "priceUi",
        "price_ui",
        "price",
        "priceInSol",
        "price_in_sol",
        "ui_price",
    ):
        v = ev.get(key)
        if v is None:
            continue
        try:
            f = float(v)
            if f > 0:
                return f
        except Exception:  # noqa: BLE001
            continue
    return None


def _extract_pnl_from_features(
    features: Dict[str, Any],
    entry_price: Optional[float],
) -> Tuple[float, float, Optional[float]]:
    """
    Try to produce pnl_pct + max_up_pct from features (fallback to entry vs current price).
    """
    pnl = None
    max_up = None

    for k in ("pnl_pct", "pnl_pct_entry", "pnl_pct_from_entry"):
        v = features.get(k)
        try:
            if v is not None:
                pnl = float(v)
                break
        except Exception:  # noqa: BLE001
            continue

    for k in ("max_up_pct", "max_runup_pct"):
        v = features.get(k)
        try:
            if v is not None:
                max_up = float(v)
                break
        except Exception:  # noqa: BLE001
            continue

    current_price = _extract_price(features) or _extract_price(features.get("tick", {}))

    if pnl is None and entry_price and current_price:
        try:
            pnl = (current_price / entry_price - 1.0) * 100.0
        except Exception:  # noqa: BLE001
            pass

    if max_up is None and pnl is not None:
        max_up = pnl

    return float(pnl or 0.0), float(max_up or (pnl or 0.0)), current_price


# ==============================================================================
# Internal helper: log sell signals for AI training
# ==============================================================================

async def _log_sell_signal(
    *,
    mint: str,
    decision: Dict[str, Any],
    features: Dict[str, Any],
    state: Dict[str, Any],
) -> None:
    """
    Optional: log sell decision for AI training / diagnostics.
    """
    if DL is None:
        return

    fn = getattr(DL, "log_sell_signal", None)
    if not callable(fn):
        return

    payload = {
        "mint": mint,
        "decision": decision,
        "features": features,
        "state": state,
        "ts": time.time(),
    }

    try:
        if asyncio.iscoroutinefunction(fn):
            await fn(payload)  # type: ignore[misc]
        else:
            await asyncio.to_thread(fn, payload)  # type: ignore[misc]
    except Exception:  # noqa: BLE001
        log.debug("AI_SELL_LOG_SIGNAL_FAIL", exc_info=True)


# ==============================================================================
# Core hooks used by exit_engine
# ==============================================================================

async def schedule_after_buy(ev: Dict[str, Any]) -> None:
    """
    Called by exit_engine.on_buy_filled once a BUY is confirmed.

    Responsibilities:
      • Store basic per-mint state:
            - entry_ts, entry_price, entry_spend_sol
            - position_raw
            - symbol, pool, candidate_id
      • Optionally let SL brain / risk manager attach metadata.
    """
    if not AI_SELL_ENABLED:
        return

    mint = _extract_mint(ev)
    if not mint:
        return
    mint = mint.strip()

    entry_ts = ev.get("ts")
    try:
        entry_ts = float(entry_ts) if entry_ts is not None else time.time()
    except Exception:  # noqa: BLE001
        entry_ts = time.time()

    entry_price = None
    for key in ("price_entry_ui", "entry_price_ui", "fill_price_ui"):
        v = ev.get(key)
        if v is None:
            continue
        try:
            f = float(v)
            if f > 0:
                entry_price = f
                break
        except Exception:  # noqa: BLE001
            continue

    position_raw = None
    for key in ("amount_raw", "position_raw", "base_amount_raw", "base_size_raw"):
        v = ev.get(key)
        if v is None:
            continue
        try:
            iv = int(v)
            if iv > 0:
                position_raw = iv
                break
        except Exception:  # noqa: BLE001
            continue

    entry_spend_sol = ev.get("spend_sol") or ev.get("entry_spend_sol") or 0.0
    try:
        entry_spend_sol = float(entry_spend_sol)
    except Exception:  # noqa: BLE001
        entry_spend_sol = 0.0

    state: Dict[str, Any] = {
        "mint": mint,
        "symbol": (ev.get("symbol") or mint[:4]).strip(),
        "pool": str(ev.get("pool") or "unknown"),
        "entry_ts": entry_ts,
        "entry_price": entry_price,
        "entry_spend_sol": entry_spend_sol,
        "position_raw": int(position_raw or 0),
        "max_up_pct": 0.0,
        "last_eval_ts": entry_ts,
        "candidate_id": ev.get("candidate_id") or ev.get("entry_candidate_id"),
        "entry_sig": ev.get("signature") or ev.get("tx_sig") or ev.get("tx"),
    }

    # Allow SL brain to annotate initial state
    if SL is not None:
        hook = getattr(SL, "on_buy_registered", None)
        if callable(hook):
            try:
                maybe = hook(dict(state))  # type: ignore[misc]
                if isinstance(maybe, dict):
                    state.update(maybe)
            except Exception:  # noqa: BLE001
                log.debug("AI_SELL_SL_ON_BUY_FAIL", exc_info=True)

    if AI_SELL_DEBUG:
        log.info(
            "AI_SELL_AFTER_BUY %s",
            {
                "mint": mint,
                "symbol": state["symbol"],
                "pool": state["pool"],
                "entry_spend_sol": entry_spend_sol,
                "position_raw": state["position_raw"],
                "entry_price": entry_price,
            },
        )

    await _save_state(mint, state)


async def _build_context_for_tick(
    mint: str,
    raw_event: Dict[str, Any],
    state: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """
    Build (features, trade, ctx) for the sell model from tick + state.
    """
    now_ts = time.time()

    # Features may be pre-computed and attached under 'features' or 'fb'
    features: Dict[str, Any] = {}
    for key in ("features", "fb", "feat"):
        v = raw_event.get(key)
        if isinstance(v, dict):
            features = v
            break

    if not features:
        # fallback: use raw_event as "features"
        features = dict(raw_event)

    entry_price = state.get("entry_price")
    pnl_pct, max_up_pct, current_price = _extract_pnl_from_features(
        features, entry_price
    )

    # Update state max_up_pct
    prev_max = float(state.get("max_up_pct", 0.0))
    if max_up_pct > prev_max:
        state["max_up_pct"] = max_up_pct

    entry_ts = state.get("entry_ts") or now_ts
    try:
        entry_ts_f = float(entry_ts)
    except Exception:  # noqa: BLE001
        entry_ts_f = now_ts

    hold_sec = max(0.0, now_ts - entry_ts_f)

    trade = {
        "entry_price": entry_price,
        "current_price": current_price,
        "pnl_pct": pnl_pct,
        "max_up_pct": state.get("max_up_pct", max_up_pct),
        "hold_sec": hold_sec,
    }

    # risk ctx + strat ctx
    risk_ctx = _get_risk_ctx()

    strat_ctx = _base_sell_cfg_from_env()
    strat_ctx = _get_strat_overrides(mint, strat_ctx, state, features)

    ctx = {
        "risk_ctx": risk_ctx,
        "strat_ctx": strat_ctx,
        "now_ts": now_ts,
    }

    return features, trade, ctx


async def on_trade_tick(raw_event: Dict[str, Any]) -> None:
    """
    Called for every trade tick the Strategy sees.

    Responsibilities:
      • Load per-mint ai_sell state (if exists).
      • Build features / trade / ctx.
      • Run sell model & self-learning overrides.
      • If decision is "sell" or "partial":
            - compute amount_raw and enqueue exit via EXIT_ENGINE.enqueue_exit_request.
      • Optionally log decision for AI training.
    """
    if not AI_SELL_ENABLED:
        return

    mint = _extract_mint(raw_event)
    if not mint:
        return
    mint = mint.strip()

    if EXIT_ENGINE is None:
        # no exit engine wired yet, just skip
        return

    state = await _load_state(mint)
    if not state:
        return

    features, trade, ctx = await _build_context_for_tick(mint, raw_event, state)
    risk_ctx = ctx["risk_ctx"]
    strat_ctx = ctx["strat_ctx"]
    now_ts = ctx["now_ts"]

    # Optional LLM score
    ai_score = await _ai_sell_score(features=features, trade=trade, risk_ctx=risk_ctx)

    decision = compute_sell_decision(
        features=features,
        trade=trade,
        risk_ctx=risk_ctx,
        strat_ctx=strat_ctx,
        now_ts=now_ts,
        ai_score=ai_score,
    )

    # Let SL brain tweak the final decision if it wants
    if SL is not None:
        hook = getattr(SL, "refine_sell_decision", None)
        if callable(hook):
            try:
                refined = hook(
                    {
                        "mint": mint,
                        "state": state,
                        "features": features,
                        "trade": trade,
                        "risk_ctx": risk_ctx,
                        "strat_ctx": strat_ctx,
                        "decision": decision,
                        "ai_score": ai_score,
                    }
                )
                if isinstance(refined, dict):
                    decision = refined
            except Exception:  # noqa: BLE001
                log.debug("AI_SELL_SL_REFINE_FAIL", exc_info=True)

    if AI_SELL_DRYRUN:
        if AI_SELL_DEBUG:
            log.info(
                "AI_SELL_DRYRUN_DECISION %s",
                {
                    "mint": mint,
                    "decision": decision,
                    "pnl_pct": trade["pnl_pct"],
                    "hold_sec": trade["hold_sec"],
                },
            )
        # Still log state for training
        await _log_sell_signal(
            mint=mint,
            decision=decision,
            features=features,
            state=state,
        )
        state["last_eval_ts"] = now_ts
        await _save_state(mint, state)
        return

    action = str(decision.get("action"))
    pct = float(decision.get("pct", 0.0))
    pct = _clamp(pct, 0.0, 1.0)
    reason = str(decision.get("reason", "AI_SELL_DECISION"))

    if action not in {"sell", "partial"} or pct <= 0.0:
        # HOLD
        if AI_SELL_DEBUG:
            log.info(
                "AI_SELL_HOLD %s",
                {
                    "mint": mint,
                    "reason": reason,
                    "pnl_pct": trade["pnl_pct"],
                },
            )
        await _log_sell_signal(
            mint=mint,
            decision=decision,
            features=features,
            state=state,
        )
        state["last_eval_ts"] = now_ts
        await _save_state(mint, state)
        return

    # We are selling something
    position_raw = int(state.get("position_raw") or 0)
    if position_raw <= 0:
        if AI_SELL_DEBUG:
            log.info(
                "AI_SELL_NO_POSITION %s",
                {"mint": mint, "reason": reason},
            )
        return

    amount_raw = max(1, int(position_raw * pct))

    if AI_SELL_DEBUG:
        log.info(
            "AI_SELL_TRIGGER_EXIT %s",
            {
                "mint": mint,
                "action": action,
                "pct": pct,
                "amount_raw": amount_raw,
                "pnl_pct": trade["pnl_pct"],
                "reason": reason,
            },
        )

    # Enqueue exit via exit_engine (auto_exit_worker)
    try:
        await EXIT_ENGINE.enqueue_exit_request(  # type: ignore[call-arg]
            mint=mint,
            amount_raw=amount_raw,
            reason=reason,
            sell_pct=pct,
            entry_spend_sol=float(state.get("entry_spend_sol") or 0.0),
            follow_stats={
                "pnl_pct": trade["pnl_pct"],
                "max_up_pct": trade["max_up_pct"],
                "hold_sec": trade["hold_sec"],
                "sell_score": float(decision.get("sell_score", 0.0)),
            },
            candidate_id=state.get("candidate_id"),
        )
    except Exception:  # noqa: BLE001
        log.warning("AI_SELL_ENQUEUE_EXIT_FAIL", exc_info=True)

    # Update remaining position
    remaining = max(0, position_raw - amount_raw)
    state["position_raw"] = remaining
    state["last_eval_ts"] = now_ts

    await _log_sell_signal(
        mint=mint,
        decision=decision,
        features=features,
        state=state,
    )

    if remaining <= 0:
        # fully exited
        await _delete_state(mint)
    else:
        await _save_state(mint, state)


# ==============================================================================
# Forced / manual exit hooks
# ==============================================================================

async def schedule_force_exit(ev: Dict[str, Any]) -> None:
    """
    Called by exit_engine.follow-through (dead coin) as a *preferred* path.

    ev example:
        {
          "mint": ...,
          "symbol": ...,
          "pool": ...,
          "reason": "follow_through_fail",
          "age": ...,
          "trades_after_entry": ...,
          "up_pct": ...,
          "dd_pct": ...,
          "exit_label": "dead_follow",
          "candidate_id": ...,
          "source": "exit_engine.follow_through",
          "spend_sol": ...
        }
    """
    if EXIT_ENGINE is None:
        return

    mint = _extract_mint(ev)
    if not mint:
        return
    mint = mint.strip()

    sell_pct = ev.get("sell_pct", 1.0)
    try:
        sell_pct = float(sell_pct)
    except Exception:  # noqa: BLE001
        sell_pct = 1.0
    sell_pct = _clamp(sell_pct, 0.01, 1.0)

    # If we know the raw amount, we can use it; otherwise this is equivalent
    # to the generic exit_engine path that infers size.
    amount_raw = ev.get("position_raw") or ev.get("amount_raw")
    if isinstance(amount_raw, (int, float)) and amount_raw > 0:
        amt = int(amount_raw)
    else:
        # unknown -> let exit_engine infer from ev
        amt = -1

    reason = str(ev.get("reason", "force_exit"))
    entry_spend_sol = float(ev.get("spend_sol") or 0.0)

    try:
        if amt > 0:
            await EXIT_ENGINE.enqueue_exit_request(  # type: ignore[call-arg]
                mint=mint,
                amount_raw=amt,
                reason=reason,
                sell_pct=sell_pct,
                entry_spend_sol=entry_spend_sol,
                follow_stats=ev.get("follow_stats") or {
                    "up_pct": ev.get("up_pct"),
                    "dd_pct": ev.get("dd_pct"),
                    "age_sec": ev.get("age"),
                    "trades_after_entry": ev.get("trades_after_entry"),
                    "exit_label": ev.get("exit_label"),
                },
                candidate_id=ev.get("candidate_id"),
            )
        else:
            # fallback: call on_buy_filled-style path; exit_engine will infer size
            await EXIT_ENGINE.on_buy_filled(ev, tx_sig=ev.get("tx_sig"))  # type: ignore[arg-type]
    except Exception:  # noqa: BLE001
        log.warning("AI_SELL_FORCE_EXIT_FAIL", exc_info=True)


# Backwards-compatible aliases
async def force_exit(ev: Dict[str, Any]) -> None:
    await schedule_force_exit(ev)


async def request_exit(ev: Dict[str, Any]) -> None:
    await schedule_force_exit(ev)


async def exit_now(ev: Dict[str, Any]) -> None:
    await schedule_force_exit(ev)


__all__ = [
    "compute_sell_decision",
    "schedule_after_buy",
    "on_trade_tick",
    "schedule_force_exit",
    "force_exit",
    "request_exit",
    "exit_now",
]
