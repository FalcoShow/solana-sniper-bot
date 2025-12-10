from __future__ import annotations

"""
utils_pkg/ai_risk_model.py

AI risk overlay / capacity delta model (V3+++ / quant-grade)
============================================================

High-level role
---------------
This module is a PURE, side-effect-free "risk overlay" that takes:

    • risk snapshot    (what risk_manager already knows)
    • performance snap (from self_learning_ai_strategy.get_perf_snapshot)
    • regime snapshot  (from utils_pkg.regime / risk_manager, optional)

and returns a small set of AI-style overlays that risk_manager / strategy can
merge into risk:ctx and use to bias capacity, gates, and risk profile:

    • ai_capacity_delta        in [-0.6, +0.6]      (capacity tilt)
    • risk_profile             in {"ultra_defensive","defensive",
                                   "balanced","aggressive","degenerate"}
    • regime_entry_gate_factor in [0.2, 1.6]        (entry gate multiplier)
    • risk_mode                in {0, 1, 2}         (0=risk_off,1=normal,2=risk_on)
    • block_new_entries        bool                 (hard circuit-breaker)
    • recommend_max_pos_mult   in [0.2, 1.8]        (max positions / size tilt)
    • ai_risk_model_version    str                  (for debugging/backtests)

Design goals
------------
    • PURE / deterministic: takes dicts, returns dict; no Redis, IO, or logging.
    • Fully safe if some fields are missing (uses defaults).
    • Tuned for PnL / DD / regime-aware behavior & equity-curve gap.
    • Interpretable: no black-box LLM; just rules & smooth scaling.
    • Easy to plug into risk_manager without rewrites:
          - risk_manager can ignore any keys it doesn’t care about.
          - predict_capacity_adjustment() is a thin adapter for current_limits().

Public API
----------
    compute_ai_overlay(risk_snapshot, perf_snapshot=None, regime_snapshot=None) -> dict
    predict_capacity_adjustment(features) -> float  # compact adapter for risk_manager

Both are synchronous and side-effect-free.
"""

import math
from typing import Any, Dict, Optional

__all__ = [
    "compute_ai_overlay",
    "predict_capacity_adjustment",
]

AI_RISK_MODEL_VERSION = "v3+++"  # bump when behavior changes

# =============================================================================
# Config / bounds
# =============================================================================

# Hard numeric bounds for outputs
AI_CAPACITY_MIN = float(-0.6)   # remove up to 60% of capacity
AI_CAPACITY_MAX = float(0.6)    # add  up to 60% of capacity

REGIME_GATE_MIN = float(0.2)    # 20% of baseline entries
REGIME_GATE_MAX = float(1.6)    # 160% of baseline entries

MAX_POS_MULT_MIN = float(0.2)   # 20% of baseline max positions / size
MAX_POS_MULT_MAX = float(1.8)   # 180% of baseline max positions / size

# Heuristic thresholds (can be tuned later)
DD_HARD_CRASH = float(-22.0)    # ≤ -22% intraday DD → ultra defensive
DD_BAD = float(-12.0)
DD_OK = float(-5.0)

PNL_TERRIBLE = float(-15.0)     # ≤ -15% realized today
PNL_BAD = float(-6.0)
PNL_OK = float(0.0)
PNL_GOOD = float(6.0)
PNL_GREAT = float(15.0)

WINRATE_LOW = float(0.40)
WINRATE_OK = float(0.50)
WINRATE_GOOD = float(0.60)
WINRATE_GREAT = float(0.68)

SHARPE_LOW = float(-0.2)
SHARPE_OK = float(0.1)
SHARPE_GOOD = float(0.3)
SHARPE_GREAT = float(0.6)

REGIME_STRESS_HIGH = float(0.7)
REGIME_STRESS_MED = float(0.45)
REGIME_SIGNAL_GOOD = float(0.4)
REGIME_SIGNAL_STRONG = float(0.65)

PLAN_GAP_BEHIND_BAD = float(-12.0)   # behind equity curve by >12% → slight risk-on (if healthy)
PLAN_GAP_AHEAD_HIGH = float(12.0)    # ahead of plan by >12% → slight risk-off (lock gains)


# =============================================================================
# Helpers
# =============================================================================

def _clamp(x: float, lo: float, hi: float) -> float:
    return float(max(lo, min(hi, x)))


def _safe_float(d: Dict[str, Any], key: str, default: float = 0.0) -> float:
    try:
        return float(d.get(key, default))
    except Exception:
        return default


def _logistic(x: float, k: float = 1.0) -> float:
    """
    Smooth squashing to (0,1). k controls steepness.
    """
    return 1.0 / (1.0 + math.exp(-k * x))


def _map_linear(x: float, x0: float, x1: float) -> float:
    """
    Map [x0, x1] → [0, 1], clamp outside.
    If x0 > x1 we invert (useful for "lower is better" dimensions).
    """
    if x0 == x1:
        return 0.5
    if x0 < x1:
        t = (x - x0) / (x1 - x0)
    else:
        # inverted
        t = (x0 - x) / (x0 - x1)
    return _clamp(t, 0.0, 1.0)


# =============================================================================
# Core scoring pieces
# =============================================================================

def _score_pnl_and_dd(risk_snapshot: Dict[str, Any]) -> Dict[str, float]:
    """
    Score current PnL / drawdown into [0,1] scores.

    Inputs (percent space):
        daily_realized_pnl_pct: -∞ .. +∞
        daily_drawdown_pct:     usually negative (e.g. -8.0 == -8% DD)
                                but we also tolerate positive "magnitude" and
                                convert it to negative internally.
    """
    pnl = _safe_float(risk_snapshot, "daily_realized_pnl_pct", 0.0)
    dd = _safe_float(risk_snapshot, "daily_drawdown_pct", 0.0)

    # Tolerate both conventions:
    #   • negative DD: -8.0 == -8% DD  (preferred)
    #   • positive magnitude:  8.0 == 8% DD  (we convert to -8.0)
    if dd > 0:
        dd = -abs(dd)

    # PnL score: map [PNL_TERRIBLE, PNL_GREAT] → [0,1]
    pnl_score = _map_linear(pnl, PNL_TERRIBLE, PNL_GREAT)

    # DD score: "less negative is better"
    dd_score = _map_linear(dd, DD_HARD_CRASH, 0.0)

    return {
        "pnl_score": pnl_score,
        "dd_score": dd_score,
    }


def _score_perf(perf_snapshot: Optional[Dict[str, Any]]) -> Dict[str, float]:
    """
    Score winrate / Sharpe-like metrics into [0,1].

    perf_snapshot is expected to look like the output of
    self_learning_ai_strategy.get_perf_snapshot(), but we tolerate missing
    fields and fall back to neutral scores.
    """
    if not perf_snapshot:
        return {
            "winrate_score": 0.5,
            "sharpe_score": 0.5,
            "recent_winrate_score": 0.5,
            "recent_sharpe_score": 0.5,
            "kelly_confidence": 0.0,
        }

    wr = _safe_float(perf_snapshot, "win_rate", 0.0)
    rwr = _safe_float(perf_snapshot, "recent_win_rate", wr)
    sharpe = _safe_float(perf_snapshot, "sharpe_like", 0.0)
    rsharpe = _safe_float(perf_snapshot, "recent_sharpe_like", sharpe)
    k_conf = _safe_float(perf_snapshot, "kelly_confidence", 0.0)

    winrate_score = _map_linear(wr, WINRATE_LOW, WINRATE_GREAT)
    recent_winrate_score = _map_linear(rwr, WINRATE_LOW, WINRATE_GREAT)
    sharpe_score = _map_linear(sharpe, SHARPE_LOW, SHARPE_GREAT)
    recent_sharpe_score = _map_linear(rsharpe, SHARPE_LOW, SHARPE_GREAT)

    return {
        "winrate_score": winrate_score,
        "sharpe_score": sharpe_score,
        "recent_winrate_score": recent_winrate_score,
        "recent_sharpe_score": recent_sharpe_score,
        "kelly_confidence": _clamp(k_conf, 0.0, 1.0),
    }


def _score_regime(
    regime_snapshot: Optional[Dict[str, Any]],
    risk_snapshot: Dict[str, Any],
) -> Dict[str, float]:
    """
    Score regime into [0,1] signal & [0,1] stress.
    Prefers using explicit regime_snapshot first, then risk_snapshot fields.

    Expected fields (best-effort):
        regime_label_numeric   ~[-1..+1]  (neg = bad / risk_off)
        regime_stress_score    [0..1]
        regime_signal_strength [0..1]
    """
    src: Dict[str, Any] = {}
    if risk_snapshot:
        src.update(risk_snapshot)
    if regime_snapshot:
        # regime_snapshot wins for overlapping keys
        src.update(regime_snapshot)

    label = _safe_float(src, "regime_label_numeric", 0.0)      # ~[-1..+1]
    stress = _safe_float(src, "regime_stress_score", 0.0)      # 0..1
    signal = _safe_float(src, "regime_signal_strength", 0.0)   # 0..1

    # Good regime when label↑, signal↑, stress↓
    label_score = _map_linear(label, -1.0, +1.0)
    signal_score = _map_linear(signal, 0.0, 1.0)
    stress_score = _map_linear(stress, 1.0, 0.0)  # inverted: 1 → bad, 0 → good

    # Slight bias against very high stress beyond thresholds
    if stress > REGIME_STRESS_HIGH:
        stress_penalty = 0.4
    elif stress > REGIME_STRESS_MED:
        stress_penalty = 0.15
    else:
        stress_penalty = 0.0

    base_regime_score = 0.45 * label_score + 0.40 * signal_score + 0.15 * stress_score
    base_regime_score = max(0.0, base_regime_score - stress_penalty)

    return {
        "regime_score": _clamp(base_regime_score, 0.0, 1.0),
        "regime_stress": _clamp(stress, 0.0, 1.0),
        "regime_signal": _clamp(signal, 0.0, 1.0),
        "regime_label": label,
    }


def _score_capacity_base(risk_snapshot: Dict[str, Any]) -> float:
    """
    Take raw risk_capacity (0..1) and apply a soft non-linearity so that
    very low / very high values are somewhat de-emphasized.
    """
    cap = _safe_float(risk_snapshot, "risk_capacity", 1.0)
    cap = _clamp(cap, 0.0, 1.2)  # allow slight >1 pre-tilt

    # Convert [0,1] -> [0,1] with more sensitivity around 0.5
    # logistic centered at 0.5
    return _logistic((cap - 0.5) * 5.0)


# =============================================================================
# Risk profile + overlay
# =============================================================================

def _choose_risk_profile(
    *,
    pnl_score: float,
    dd_pct: float,
    perf_scores: Dict[str, float],
    regime_scores: Dict[str, float],
) -> str:
    """
    Map scores into a discrete risk_profile label.

    dd_pct is in percent space (negative numbers == drawdown).
    """
    wr_score = perf_scores["winrate_score"]
    rwr_score = perf_scores["recent_winrate_score"]
    sharpe_score = perf_scores["sharpe_score"]
    rsharpe_score = perf_scores["recent_sharpe_score"]
    regime_score = regime_scores["regime_score"]
    stress = regime_scores["regime_stress"]

    # Very bad DD or regime + poor winrate → ultra_defensive
    if dd_pct <= DD_HARD_CRASH or stress > 0.9:
        return "ultra_defensive"
    if dd_pct <= DD_BAD and (wr_score < 0.4 or rwr_score < 0.4):
        return "ultra_defensive"

    # Clearly negative edge / bad Sharpe / bad regime → defensive
    if (
        dd_pct <= DD_BAD
        or pnl_score < 0.35
        or wr_score < 0.45
        or rwr_score < 0.4
        or sharpe_score < 0.35
        or rsharpe_score < 0.35
        or regime_score < 0.35
    ):
        return "defensive"

    # Clearly strong stats + good regime, low stress → aggressive
    if (
        pnl_score > 0.65
        and wr_score > 0.7
        and rwr_score > 0.7
        and sharpe_score > 0.7
        and rsharpe_score > 0.7
        and regime_score > 0.7
        and stress < 0.4
    ):
        return "aggressive"

    # Very rare: everything is insane plus no DD: allow "degenerate"
    if (
        pnl_score > 0.85
        and wr_score > 0.8
        and rwr_score > 0.8
        and sharpe_score > 0.85
        and rsharpe_score > 0.85
        and regime_score > 0.85
        and dd_pct > DD_OK  # i.e. DD not very negative
    ):
        return "degenerate"

    # Otherwise middle bucket
    return "balanced"


def _compute_ai_capacity_delta(
    *,
    risk_snapshot: Dict[str, Any],
    pnl_dd_scores: Dict[str, float],
    perf_scores: Dict[str, float],
    regime_scores: Dict[str, float],
) -> float:
    """
    Turn scores into a signed capacity delta in [-0.6, +0.6].
    """
    pnl_score = pnl_dd_scores["pnl_score"]
    dd_score = pnl_dd_scores["dd_score"]
    wr_score = perf_scores["winrate_score"]
    rwr_score = perf_scores["recent_winrate_score"]
    sharpe_score = perf_scores["sharpe_score"]
    rsharpe_score = perf_scores["recent_sharpe_score"]
    k_conf = perf_scores["kelly_confidence"]
    regime_score = regime_scores["regime_score"]
    stress = regime_scores["regime_stress"]
    label = regime_scores["regime_label"]

    plan_gap = _safe_float(risk_snapshot, "plan_gap_pct", 0.0)

    # Blend everything into a "goodness" score 0..1
    goodness = (
        0.18 * pnl_score
        + 0.16 * dd_score
        + 0.16 * wr_score
        + 0.16 * rwr_score
        + 0.12 * sharpe_score
        + 0.12 * rsharpe_score
        + 0.10 * regime_score
    )
    goodness = _clamp(goodness, 0.0, 1.0)

    # Map goodness → base tilt: [-0.35, +0.35]
    base_tilt = (goodness - 0.5) * 0.7  # symmetric around 0

    # Stress / regime label safety adjustments
    if stress > REGIME_STRESS_HIGH or label <= -0.7:
        base_tilt -= 0.25
    elif stress > REGIME_STRESS_MED or label <= -0.4:
        base_tilt -= 0.10

    # Equity-curve "plan gap" nudges (small)
    # If we're behind plan but otherwise healthy → slight risk-on
    if (
        plan_gap <= PLAN_GAP_BEHIND_BAD
        and pnl_score > 0.55
        and dd_score > 0.55
        and regime_score > 0.55
    ):
        base_tilt += 0.06

    # If we're far ahead of plan or DD is heavy → slight risk-off
    if (
        plan_gap >= PLAN_GAP_AHEAD_HIGH
        and (dd_score < 0.55 or pnl_score < 0.55)
    ):
        base_tilt -= 0.06

    # If Kelly confidence is low, shrink absolute tilt (0.5..1.0)
    shrink = 0.5 + 0.5 * _clamp(k_conf, 0.0, 1.0)
    base_tilt *= shrink

    # If risk_capacity already very low, don't slam it further
    base_cap = _score_capacity_base(risk_snapshot)  # 0..1
    if base_cap < 0.25 and base_tilt < 0:
        base_tilt *= 0.4
    if base_cap > 0.85 and base_tilt > 0:
        base_tilt *= 0.7

    return _clamp(base_tilt, AI_CAPACITY_MIN, AI_CAPACITY_MAX)


def _compute_regime_gate_factor(
    *,
    risk_profile: str,
    regime_scores: Dict[str, float],
    pnl_dd_scores: Dict[str, float],
) -> float:
    """
    Compute regime_entry_gate_factor (0.2..1.6) used by entry logic
    and strategy brain to slightly tighten/loosen entry thresholds.
    """
    regime_score = regime_scores["regime_score"]
    stress = regime_scores["regime_stress"]
    pnl_score = pnl_dd_scores["pnl_score"]
    dd_score = pnl_dd_scores["dd_score"]

    # Start from regime score mapped [0,1] → [0.6, 1.4]
    base = 0.6 + 0.8 * _clamp(regime_score, 0.0, 1.0)

    # Apply stress penalty
    if stress > REGIME_STRESS_HIGH:
        base *= 0.5
    elif stress > REGIME_STRESS_MED:
        base *= 0.8

    # Bias by risk_profile
    if risk_profile == "ultra_defensive":
        base *= 0.5
    elif risk_profile == "defensive":
        base *= 0.8
    elif risk_profile == "aggressive":
        base *= 1.15
    elif risk_profile == "degenerate":
        base *= 1.30

    # If PnL/DD combo is very bad, clamp harshly
    if pnl_score < 0.3 or dd_score < 0.3:
        base = min(base, 0.85)

    return _clamp(base, REGIME_GATE_MIN, REGIME_GATE_MAX)


def _compute_risk_mode_and_block(
    *,
    risk_profile: str,
    pnl_dd_scores: Dict[str, float],
    regime_scores: Dict[str, float],
    risk_snapshot: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Produce a coarse risk_mode + hard circuit-breaker flag.

    risk_mode:
        0 = risk_off (entries heavily restricted)
        1 = normal
        2 = risk_on (allow more entries / size)

    block_new_entries:
        True if we recommend absolutely no *new* trades (but existing exits
        may still fire).
    """
    pnl_score = pnl_dd_scores["pnl_score"]
    dd_score = pnl_dd_scores["dd_score"]
    regime_score = regime_scores["regime_score"]
    stress = regime_scores["regime_stress"]

    dd_pct = _safe_float(risk_snapshot, "daily_drawdown_pct", 0.0)
    if dd_pct > 0:
        dd_pct = -abs(dd_pct)

    circuit_tripped = bool(risk_snapshot.get("circuit_breaker_tripped"))
    # Fallback: use "hard" DD and stress to trigger circuit
    hard_dd_hit = dd_pct <= DD_HARD_CRASH
    extreme_stress = stress >= 0.95

    block = circuit_tripped or hard_dd_hit or extreme_stress

    # Default risk_mode
    risk_mode = 1

    # Strong reasons for risk_off
    if (
        block
        or dd_pct <= DD_BAD
        or dd_score < 0.3
        or pnl_score < 0.3
        or regime_score < 0.3
        or risk_profile in ("ultra_defensive", "defensive")
    ):
        risk_mode = 0
    else:
        # Consider risk_on only in very healthy conditions
        if (
            risk_profile in ("aggressive", "degenerate")
            and pnl_score > 0.6
            and dd_score > 0.6
            and regime_score > 0.6
            and stress < 0.5
        ):
            risk_mode = 2
        else:
            risk_mode = 1

    return {
        "risk_mode": int(risk_mode),
        "block_new_entries": bool(block),
    }


def _compute_max_pos_mult(
    *,
    risk_profile: str,
    ai_capacity_delta: float,
    risk_mode: int,
) -> float:
    """
    Suggest a multiplier for max positions / max size.

    This is soft guidance; risk_manager can ignore it or clamp further.
    """
    # Start from neutral 1.0 and apply ai_capacity_delta (scaled)
    base = 1.0 + ai_capacity_delta * 0.8  # delta in [-0.6,0.6] → adj in [-0.48,0.48]

    # Profile bias
    if risk_profile == "ultra_defensive":
        base *= 0.5
    elif risk_profile == "defensive":
        base *= 0.8
    elif risk_profile == "aggressive":
        base *= 1.15
    elif risk_profile == "degenerate":
        base *= 1.3

    # Risk-mode override
    if risk_mode == 0:
        base *= 0.4
    elif risk_mode == 2:
        base *= 1.2

    return _clamp(base, MAX_POS_MULT_MIN, MAX_POS_MULT_MAX)


# =============================================================================
# Public API
# =============================================================================

def compute_ai_overlay(
    risk_snapshot: Dict[str, Any],
    perf_snapshot: Optional[Dict[str, Any]] = None,
    regime_snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Main entry point for full overlay.

    Args:
        risk_snapshot:
            The raw snapshot risk_manager already knows about the account /
            session. Expected keys (best-effort):

              - risk_capacity          (0..1)
              - daily_realized_pnl_pct (percent; +10.0 == +10%)
              - daily_drawdown_pct     (percent; usually negative; -8.0 == -8% DD
                                        but positive magnitudes are tolerated)
              - equity_usd             (float)
              - plan_gap_pct           (optional; % vs equity curve plan)
              - regime_label_numeric   (optional)
              - regime_stress_score    (optional)
              - regime_signal_strength (optional)
              - circuit_breaker_tripped (optional bool)

        perf_snapshot:
            Output of self_learning_ai_strategy.get_perf_snapshot(redis),
            usually containing:
              - win_rate, recent_win_rate
              - sharpe_like, recent_sharpe_like
              - kelly_confidence, etc.

        regime_snapshot:
            Optional extra regime struct. If provided, fields here override
            same-named fields from risk_snapshot.

    Returns:
        {
            "ai_capacity_delta": float in [-0.6, +0.6],
            "risk_profile": str in {"ultra_defensive","defensive",
                                    "balanced","aggressive","degenerate"},
            "regime_entry_gate_factor": float in [0.2, 1.6],
            "risk_mode": int in {0,1,2},
            "block_new_entries": bool,
            "recommend_max_pos_mult": float in [0.2, 1.8],
            "ai_risk_model_version": str,
        }

    risk_manager is free to ignore any of these keys; the old interface
    (reading only ai_capacity_delta / risk_profile / regime_entry_gate_factor)
    remains valid.
    """
    # 1) score pieces
    pnl_dd_scores = _score_pnl_and_dd(risk_snapshot)
    perf_scores = _score_perf(perf_snapshot)
    regime_scores = _score_regime(regime_snapshot, risk_snapshot)

    dd_pct = _safe_float(risk_snapshot, "daily_drawdown_pct", 0.0)
    if dd_pct > 0:
        dd_pct = -abs(dd_pct)

    # 2) risk_profile
    risk_profile = _choose_risk_profile(
        pnl_score=pnl_dd_scores["pnl_score"],
        dd_pct=dd_pct,
        perf_scores=perf_scores,
        regime_scores=regime_scores,
    )

    # 3) ai_capacity_delta
    ai_capacity_delta = _compute_ai_capacity_delta(
        risk_snapshot=risk_snapshot,
        pnl_dd_scores=pnl_dd_scores,
        perf_scores=perf_scores,
        regime_scores=regime_scores,
    )

    # 4) regime_entry_gate_factor
    regime_gate = _compute_regime_gate_factor(
        risk_profile=risk_profile,
        regime_scores=regime_scores,
        pnl_dd_scores=pnl_dd_scores,
    )

    # 5) risk_mode + circuit-breaker
    risk_mode_block = _compute_risk_mode_and_block(
        risk_profile=risk_profile,
        pnl_dd_scores=pnl_dd_scores,
        regime_scores=regime_scores,
        risk_snapshot=risk_snapshot,
    )
    risk_mode = int(risk_mode_block["risk_mode"])
    block_new_entries = bool(risk_mode_block["block_new_entries"])

    # 6) suggested max-position multiplier
    max_pos_mult = _compute_max_pos_mult(
        risk_profile=risk_profile,
        ai_capacity_delta=ai_capacity_delta,
        risk_mode=risk_mode,
    )

    return {
        "ai_capacity_delta": float(ai_capacity_delta),
        "risk_profile": risk_profile,
        "regime_entry_gate_factor": float(regime_gate),
        "risk_mode": risk_mode,
        "block_new_entries": block_new_entries,
        "recommend_max_pos_mult": float(max_pos_mult),
        "ai_risk_model_version": AI_RISK_MODEL_VERSION,
    }


# -----------------------------------------------------------------------------  
# Adapter used by utils_pkg.risk_manager.current_limits()
# -----------------------------------------------------------------------------  

def _build_risk_snapshot_from_features(features: Dict[str, Any]) -> Dict[str, Any]:
    """
    Translate the compact feature dict used by risk_manager into a
    risk_snapshot compatible with compute_ai_overlay().

    Expected (best-effort) keys in features:
        - equity_usd
        - winrate20             (fraction 0..1)
        - rolling_dd            (fraction 0..1)
        - realized_pnl_usd
        - regime_mult
        - plan_gap_pct
        - risk_capacity         (optional; 0..1)
        - circuit_breaker_tripped (optional)
    """
    eq = _safe_float(features, "equity_usd", 0.0)
    realized = _safe_float(features, "realized_pnl_usd", 0.0)
    dd_frac = _safe_float(features, "rolling_dd", 0.0)  # fraction 0..1
    regime_mult = _safe_float(features, "regime_mult", 1.0)
    plan_gap = _safe_float(features, "plan_gap_pct", 0.0)
    base_capacity = _safe_float(features, "risk_capacity", 0.5)
    circuit_tripped = bool(features.get("circuit_breaker_tripped", False))

    # Convert to percent space
    if eq > 0:
        daily_pnl_pct = (realized / eq) * 100.0
    else:
        daily_pnl_pct = 0.0

    # Store as negative percentage (our internal convention)
    daily_dd_pct = -abs(dd_frac) * 100.0

    # Derive a coarse regime label from regime_mult (1=neutral)
    if regime_mult <= 0.7:
        regime_label_numeric = -0.8
        regime_signal_strength = 0.3
        regime_stress_score = 0.7
    elif regime_mult <= 0.9:
        regime_label_numeric = -0.4
        regime_signal_strength = 0.4
        regime_stress_score = 0.55
    elif regime_mult >= 1.3:
        regime_label_numeric = 0.8
        regime_signal_strength = 0.8
        regime_stress_score = 0.3
    elif regime_mult >= 1.1:
        regime_label_numeric = 0.4
        regime_signal_strength = 0.6
        regime_stress_score = 0.4
    else:
        regime_label_numeric = 0.0
        regime_signal_strength = 0.5
        regime_stress_score = 0.5

    return {
        "risk_capacity": base_capacity,
        "daily_realized_pnl_pct": daily_pnl_pct,
        "daily_drawdown_pct": daily_dd_pct,
        "equity_usd": eq,
        "plan_gap_pct": plan_gap,
        "regime_label_numeric": regime_label_numeric,
        "regime_stress_score": regime_stress_score,
        "regime_signal_strength": regime_signal_strength,
        "circuit_breaker_tripped": circuit_tripped,
    }


def _build_perf_snapshot_from_features(features: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Build a minimal perf_snapshot-like dict from compact features.

    This lets predict_capacity_adjustment() be a little smarter without
    needing the full self_learning_ai_strategy.get_perf_snapshot() output.

    Expected (optional) keys:
        - winrate20           fraction 0..1
        - recent_winrate20    fraction 0..1 (fallback to winrate20)
        - sharpe20            scalar
        - recent_sharpe20     scalar (fallback to sharpe20)
        - kelly_confidence    0..1
    """
    if not features:
        return None

    wr = _safe_float(features, "winrate20", 0.0)
    rwr = _safe_float(features, "recent_winrate20", wr)
    sharpe = _safe_float(features, "sharpe20", 0.0)
    rsharpe = _safe_float(features, "recent_sharpe20", sharpe)
    k_conf = _safe_float(features, "kelly_confidence", 0.0)

    # If we have basically no signal, skip
    if wr == 0.0 and sharpe == 0.0 and k_conf == 0.0:
        return None

    return {
        "win_rate": wr,
        "recent_win_rate": rwr,
        "sharpe_like": sharpe,
        "recent_sharpe_like": rsharpe,
        "kelly_confidence": _clamp(k_conf, 0.0, 1.0),
    }


def predict_capacity_adjustment(features: Dict[str, Any]) -> float:
    """
    Compact adapter used by risk_manager.current_limits().

    Args:
        features:
            A compact dict with core metrics (see
            _build_risk_snapshot_from_features docstring).

    Returns:
        ai_capacity_delta in [-0.6, +0.6].
        Safe default is 0.0 if anything goes wrong.

    NOTE: This function intentionally exposes ONLY the ai_capacity_delta to
    risk_manager for now. If we want to use risk_mode / overlay there,
    risk_manager can call compute_ai_overlay() directly instead.
    """
    try:
        risk_snapshot = _build_risk_snapshot_from_features(features)
        perf_snapshot = _build_perf_snapshot_from_features(features)

        # We don't pass a separate regime_snapshot here: regime is already
        # encoded into risk_snapshot via regime_mult.
        overlay = compute_ai_overlay(
            risk_snapshot,
            perf_snapshot=perf_snapshot,
            regime_snapshot=None,
        )
        delta = float(overlay.get("ai_capacity_delta", 0.0))
        return _clamp(delta, AI_CAPACITY_MIN, AI_CAPACITY_MAX)
    except Exception:
        # Fail-safe: no adjustment
        return 0.0


# Simple CLI / debug usage
if __name__ == "__main__":
    # Tiny smoke test
    base_features = {
        "equity_usd": 1_000.0,
        "winrate20": 0.62,
        "recent_winrate20": 0.68,
        "sharpe20": 0.45,
        "recent_sharpe20": 0.55,
        "rolling_dd": 0.05,
        "realized_pnl_usd": 50.0,
        "regime_mult": 1.1,
        "plan_gap_pct": -3.0,
        "risk_capacity": 0.8,
        "kelly_confidence": 0.7,
    }
    delta = predict_capacity_adjustment(base_features)
    print("ai_capacity_delta:", delta)

    snap = _build_risk_snapshot_from_features(base_features)
    overlay = compute_ai_overlay(
        snap,
        perf_snapshot=_build_perf_snapshot_from_features(base_features),
    )
    print("overlay:", overlay)
