from __future__ import annotations

"""
utils_pkg/ai_risk_model.py

AI risk overlay / capacity + candidate risk model (V3+++ / quant-grade)
======================================================================

This module has TWO responsibilities:

(1) Account / regime overlay (slow, session-level)
--------------------------------------------------
    compute_ai_overlay(risk_snapshot, perf_snapshot=None, regime_snapshot=None) -> dict

    Returns a PURE dict (no IO / no globals) that RiskManager and the rest of
    the system can consume to modulate capacity, concurrency and entry gating.

    Core outputs (strict schema):
      • ai_capacity_delta        in [-0.6, +0.6]
      • risk_profile             str ("ultra_defensive"|"defensive"|"balanced"|"aggressive"|"degenerate")
      • regime_entry_gate_factor in [0.2, 1.6]
      • risk_mode                int {0,1,2}  (0=risk_off, 1=normal, 2=risk_on)
      • risk_mode_label          "risk_off" | "normal" | "risk_on"
      • block_new_entries        bool
      • recommend_max_pos_mult   in [0.2, 1.8]
      • ai_risk_model_version    str
      • debug                    dict (stable keys; numeric where possible)

    Inputs are "snapshots" – already-smoothed views of the account & regime:

      risk_snapshot: {
          daily_realized_pnl_pct,
          daily_drawdown_pct,
          risk_capacity,
          plan_gap_pct,
          circuit_breaker_tripped,
          risk_budget_util / risk_budget_utilization / daily_loss_used_frac,
          ...
      }

      perf_snapshot: {
          win_rate,
          recent_win_rate,
          sharpe_like,
          recent_sharpe_like,
          kelly_confidence,
          (optional) win_streak, loss_streak, avg_r_multiple, etc.
      }

      regime_snapshot: {
          regime_label_numeric,   # ~[-1..+1]
          regime_stress_score,    # 0..1
          regime_signal_strength, # 0..1
          manipulation_risk,      # 0..1
          ...
      }

(2) Candidate-level risk score (fast, per-trade)
------------------------------------------------
    score_candidate(features, config=None) -> dict

    Returns (strict schema):
      • score      in [0,1]   (0=safest, 1=highest risk)
      • conf       in [0,1]
      • label      "safe" | "ok" | "elevated" | "critical"
      • hard_block bool
      • components dict[str,float] (0..1 per component)
      • raw        dict (extracted values for debugging)
      • reasons    list[str]  (short human-readable strings)
      • version    str

Design goals
------------
- PURE / deterministic: dict in, dict out. No IO, Redis, logging, env.
- Safe for missing fields: best-effort interpretation with *neutral* defaults
  (NOT optimistic “low risk” defaults).
- Interpretable: exposes components + reasons + raw values.
- Backward-compatible: exposes score_risk / risk_score / score / evaluate aliases.
- Safe fallback: if anything internal errors, overlay returns risk_off+block,
  candidate score returns critical+hard_block. (No silent permissive failures.)

This is intended as the SINGLE SOURCE OF TRUTH for "AI risk semantics".
"""

import math
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple

__all__ = [
    "AI_RISK_MODEL_VERSION",
    "compute_ai_overlay",
    "predict_capacity_adjustment",
    "score_candidate",
    "score_entry",
    "score",
    "evaluate",
    "score_risk",
    "risk_score",
    "derive_capacity_multiplier",
    "is_risk_off",
    "validate_overlay",
    "validate_candidate_score",
]

# Bump whenever heuristics meaningfully change (keep stable for logging/audits)
AI_RISK_MODEL_VERSION = "v3+++-2025-12-22"

# =============================================================================
# Output bounds
# =============================================================================

AI_CAPACITY_MIN = float(-0.6)
AI_CAPACITY_MAX = float(+0.6)

REGIME_GATE_MIN = float(0.2)
REGIME_GATE_MAX = float(1.6)

MAX_POS_MULT_MIN = float(0.2)
MAX_POS_MULT_MAX = float(1.8)

# =============================================================================
# Overlay heuristic thresholds (percent-space or scalar)
# =============================================================================

DD_HARD_CRASH = float(-22.0)
DD_BAD = float(-12.0)
DD_OK = float(-5.0)

PNL_TERRIBLE = float(-15.0)
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

REGIME_STRESS_HIGH = float(0.70)
REGIME_STRESS_MED = float(0.45)

PLAN_GAP_BEHIND_BAD = float(-12.0)
PLAN_GAP_AHEAD_HIGH = float(12.0)

# Risk-budget utilization (0..1+)
BUDGET_UTIL_WARN = float(0.75)
BUDGET_UTIL_HIGH = float(0.90)
BUDGET_UTIL_CRIT = float(1.00)

# =============================================================================
# Strict output schemas
# =============================================================================

OVERLAY_REQUIRED_KEYS = (
    "ai_capacity_delta",
    "risk_profile",
    "regime_entry_gate_factor",
    "risk_mode",
    "risk_mode_label",
    "block_new_entries",
    "recommend_max_pos_mult",
    "ai_risk_model_version",
    "debug",
)

CANDIDATE_REQUIRED_KEYS = (
    "score",
    "conf",
    "label",
    "hard_block",
    "components",
    "raw",
    "reasons",
    "version",
)

# =============================================================================
# Safe fallbacks (used only if internal exception)
# =============================================================================

_SAFE_OVERLAY_FALLBACK: Dict[str, Any] = {
    "ai_capacity_delta": float(AI_CAPACITY_MIN),
    "risk_profile": "ultra_defensive",
    "regime_entry_gate_factor": float(REGIME_GATE_MIN),
    "risk_mode": 0,
    "risk_mode_label": "risk_off",
    "block_new_entries": True,
    "recommend_max_pos_mult": float(MAX_POS_MULT_MIN),
    "ai_risk_model_version": AI_RISK_MODEL_VERSION,
    "debug": {
        "error": "ai_risk_model_exception_fallback",
        "pnl_dd": {"pnl_score": 0.0, "dd_score": 0.0, "dd_pct": 0.0, "pnl_pct": 0.0},
        "perf": {
            "winrate_score": 0.0,
            "recent_winrate_score": 0.0,
            "sharpe_score": 0.0,
            "recent_sharpe_score": 0.0,
            "kelly_confidence": 0.0,
            "streak_bias": 0.0,
        },
        "regime": {
            "regime_score": 0.0,
            "regime_stress": 1.0,
            "regime_signal": 0.0,
            "regime_label": -1.0,
            "manipulation_risk": 1.0,
        },
        "budget_util": 9.99,
    },
}

_SAFE_CANDIDATE_FALLBACK: Dict[str, Any] = {
    "score": 1.0,
    "conf": 0.2,
    "label": "critical",
    "hard_block": True,
    "components": {},
    "raw": {"error": "ai_risk_model_exception_fallback"},
    "reasons": ["HARD_BLOCK=TRUE", "risk_model_exception_fallback"],
    "version": AI_RISK_MODEL_VERSION,
}

# =============================================================================
# Helpers (safe parsing + math)
# =============================================================================


def _clamp(x: float, lo: float, hi: float) -> float:
    try:
        xf = float(x)
    except Exception:
        return float(lo)
    if xf != xf:  # NaN
        return float(lo)
    return float(max(lo, min(hi, xf)))


def _safe_float(d: Mapping[str, Any], key: str, default: float = 0.0) -> float:
    try:
        v = d.get(key, default)
        if v is None or isinstance(v, bool):
            return float(default)
        return float(v)
    except Exception:
        return float(default)


def _safe_bool(d: Mapping[str, Any], key: str, default: bool = False) -> bool:
    v = d.get(key, default)
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return default


def _logistic(x: float, k: float = 1.0) -> float:
    """Numerically guarded logistic."""
    try:
        z = -k * float(x)
        if z > 50:
            return 0.0
        if z < -50:
            return 1.0
        return 1.0 / (1.0 + math.exp(z))
    except Exception:
        return 0.5


def _map_linear(x: float, x0: float, x1: float) -> float:
    """
    Map [x0,x1] -> [0,1] clamp; inverted if x0>x1.
    """
    if x0 == x1:
        return 0.5
    try:
        xf = float(x)
    except Exception:
        xf = float(x0)
    if x0 < x1:
        t = (xf - x0) / (x1 - x0)
    else:
        t = (x0 - xf) / (x0 - x1)
    return _clamp(t, 0.0, 1.0)


def _as_frac_01(v: float) -> float:
    """
    Accept either 0..1 or 0..100 and normalize to 0..1.
    """
    try:
        vf = float(v)
    except Exception:
        return 0.0
    if vf > 1.0 and vf <= 100.0:
        vf = vf / 100.0
    return _clamp(vf, 0.0, 1.0)


def _first_present_float(features: Mapping[str, Any], keys: Iterable[str], default: float = 0.0) -> float:
    for k in keys:
        if k in features and features[k] is not None:
            try:
                v = features[k]
                if isinstance(v, bool):
                    continue
                return float(v)
            except Exception:
                continue
    return float(default)


def _extract_budget_utilization(risk_snapshot: Mapping[str, Any]) -> float:
    """
    Extract risk budget utilization as 0..1+ (1.0 = hit limit) from several
    possible keys. Caller may treat >1 as "breach".
    """
    util = _first_present_float(
        risk_snapshot,
        (
            "risk_budget_util",
            "risk_budget_utilization",
            "riskBudgetUtilization",
            "daily_loss_used_frac",
            "dailyLossUsedFrac",
            "daily_loss_used_pct",
            "dailyLossUsedPct",
            "budget_utilization",
        ),
        0.0,
    )
    if util > 1.0 and util <= 100.0:
        util = util / 100.0
    return float(max(0.0, util))


# =============================================================================
# Overlay scoring primitives
# =============================================================================


def _score_pnl_and_dd(risk_snapshot: Dict[str, Any]) -> Dict[str, float]:
    pnl = _safe_float(risk_snapshot, "daily_realized_pnl_pct", 0.0)
    dd = _safe_float(risk_snapshot, "daily_drawdown_pct", 0.0)

    # tolerate dd magnitude convention: enforce negative = drawdown
    if dd > 0:
        dd = -abs(dd)

    pnl_score = _map_linear(pnl, PNL_TERRIBLE, PNL_GREAT)  # higher -> better
    dd_score = _map_linear(dd, DD_HARD_CRASH, 0.0)         # less negative -> better

    return {"pnl_score": pnl_score, "dd_score": dd_score, "dd_pct": dd, "pnl_pct": pnl}


def _score_perf(perf_snapshot: Optional[Dict[str, Any]]) -> Dict[str, float]:
    if not perf_snapshot:
        return {
            "winrate_score": 0.5,
            "recent_winrate_score": 0.5,
            "sharpe_score": 0.5,
            "recent_sharpe_score": 0.5,
            "kelly_confidence": 0.0,
            "streak_bias": 0.0,
        }

    wr = _safe_float(perf_snapshot, "win_rate", 0.0)
    rwr = _safe_float(perf_snapshot, "recent_win_rate", wr)
    sharpe = _safe_float(perf_snapshot, "sharpe_like", 0.0)
    rsharpe = _safe_float(perf_snapshot, "recent_sharpe_like", sharpe)
    k_conf = _safe_float(perf_snapshot, "kelly_confidence", 0.0)

    win_streak = _safe_float(perf_snapshot, "win_streak", 0.0)
    loss_streak = _safe_float(perf_snapshot, "loss_streak", 0.0)

    streak_bias = 0.0
    # penalize loss streak harder than we reward win streak
    if loss_streak >= 3:
        streak_bias -= min(0.15, 0.03 * loss_streak)
    if win_streak >= 3:
        streak_bias += min(0.10, 0.02 * win_streak)

    return {
        "winrate_score": _map_linear(wr, WINRATE_LOW, WINRATE_GREAT),
        "recent_winrate_score": _map_linear(rwr, WINRATE_LOW, WINRATE_GREAT),
        "sharpe_score": _map_linear(sharpe, SHARPE_LOW, SHARPE_GREAT),
        "recent_sharpe_score": _map_linear(rsharpe, SHARPE_LOW, SHARPE_GREAT),
        "kelly_confidence": _clamp(k_conf, 0.0, 1.0),
        "streak_bias": streak_bias,
    }


def _score_regime(regime_snapshot: Optional[Dict[str, Any]], risk_snapshot: Dict[str, Any]) -> Dict[str, float]:
    src: Dict[str, Any] = {}
    if risk_snapshot:
        src.update(risk_snapshot)
    if regime_snapshot:
        src.update(regime_snapshot)

    label = _safe_float(src, "regime_label_numeric", 0.0)      # ~[-1..+1]
    stress = _safe_float(src, "regime_stress_score", 0.0)      # 0..1
    signal = _safe_float(src, "regime_signal_strength", 0.0)   # 0..1
    manip = _safe_float(src, "manipulation_risk", _safe_float(src, "manipulationRisk", 0.0))  # 0..1

    label_score = _map_linear(label, -1.0, +1.0)
    signal_score = _map_linear(signal, 0.0, 1.0)
    stress_score = _map_linear(stress, 1.0, 0.0)  # inverted
    manip_pen = _map_linear(manip, 0.6, 1.0)      # 0..1 penalty

    base = 0.45 * label_score + 0.40 * signal_score + 0.15 * stress_score
    base = base * (1.0 - 0.25 * manip_pen)

    # stress penalty tiers
    if stress > REGIME_STRESS_HIGH:
        base = max(0.0, base - 0.35)
    elif stress > REGIME_STRESS_MED:
        base = max(0.0, base - 0.12)

    return {
        "regime_score": _clamp(base, 0.0, 1.0),
        "regime_stress": _clamp(stress, 0.0, 1.0),
        "regime_signal": _clamp(signal, 0.0, 1.0),
        "regime_label": float(label),
        "manipulation_risk": _clamp(manip, 0.0, 1.0),
    }


def _score_capacity_base(risk_snapshot: Dict[str, Any]) -> float:
    cap = _safe_float(risk_snapshot, "risk_capacity", 1.0)  # typical 0..1.2
    cap = _clamp(cap, 0.0, 1.2)
    # emphasize the middle region
    return _logistic((cap - 0.5) * 5.0)


def _choose_risk_profile(
    pnl_dd: Dict[str, float],
    perf: Dict[str, float],
    regime: Dict[str, float],
    budget_util: float,
) -> str:
    dd_pct = float(pnl_dd["dd_pct"])
    pnl_score = float(pnl_dd["pnl_score"])

    wr = perf["winrate_score"]
    rwr = perf["recent_winrate_score"]
    sh = perf["sharpe_score"]
    rsh = perf["recent_sharpe_score"]

    reg = regime["regime_score"]
    stress = regime["regime_stress"]
    manip = regime["manipulation_risk"]

    # Hard stops
    if dd_pct <= DD_HARD_CRASH or stress >= 0.95 or budget_util >= BUDGET_UTIL_CRIT:
        return "ultra_defensive"
    if manip >= 0.92 and reg < 0.45:
        return "ultra_defensive"

    # Defensive if edge looks poor, DD getting ugly, or budget nearly tapped
    if (
        dd_pct <= DD_BAD
        or pnl_score < 0.35
        or wr < 0.45
        or rwr < 0.42
        or sh < 0.35
        or rsh < 0.35
        or reg < 0.35
        or stress > REGIME_STRESS_HIGH
        or (manip > 0.8 and reg < 0.55)
        or budget_util >= BUDGET_UTIL_HIGH
    ):
        return "defensive"

    # Aggressive tiers only if everything aligns and budget is healthy
    if (
        pnl_score > 0.65
        and wr > 0.70
        and rwr > 0.70
        and sh > 0.70
        and rsh > 0.70
        and reg > 0.70
        and stress < 0.45
        and manip < 0.55
        and budget_util <= BUDGET_UTIL_WARN
    ):
        if pnl_score > 0.85 and sh > 0.85 and reg > 0.85 and dd_pct > DD_OK:
            return "degenerate"
        return "aggressive"

    return "balanced"


def _compute_ai_capacity_delta(
    risk_snapshot: Dict[str, Any],
    pnl_dd: Dict[str, float],
    perf: Dict[str, float],
    regime: Dict[str, float],
    risk_profile: str,
    budget_util: float,
) -> float:
    pnl_score = pnl_dd["pnl_score"]
    dd_score = pnl_dd["dd_score"]

    wr = perf["winrate_score"]
    rwr = perf["recent_winrate_score"]
    sh = perf["sharpe_score"]
    rsh = perf["recent_sharpe_score"]
    k_conf = perf["kelly_confidence"]
    streak_bias = perf.get("streak_bias", 0.0)

    reg = regime["regime_score"]
    stress = regime["regime_stress"]
    manip = regime["manipulation_risk"]
    label = regime["regime_label"]

    plan_gap = _safe_float(risk_snapshot, "plan_gap_pct", 0.0)

    # 1) Composite "goodness" 0..1
    goodness = (
        0.18 * pnl_score
        + 0.16 * dd_score
        + 0.16 * wr
        + 0.16 * rwr
        + 0.12 * sh
        + 0.12 * rsh
        + 0.10 * reg
    )
    goodness = _clamp(goodness, 0.0, 1.0)

    # 2) Base tilt in roughly [-0.35, +0.35]
    tilt = (goodness - 0.5) * 0.70

    # 3) Risk-profile shaping
    if risk_profile == "ultra_defensive":
        tilt -= 0.25
    elif risk_profile == "defensive":
        tilt -= 0.10
    elif risk_profile == "aggressive":
        tilt += 0.05
    elif risk_profile == "degenerate":
        tilt += 0.10

    # 4) Regime safety
    if stress > REGIME_STRESS_HIGH or label <= -0.7:
        tilt -= 0.18
    elif stress > REGIME_STRESS_MED or label <= -0.4:
        tilt -= 0.08

    # 5) Manipulation clamp
    if manip >= 0.90:
        tilt -= 0.25
    elif manip >= 0.75:
        tilt -= 0.12

    # 6) Budget utilization shaping
    if budget_util >= BUDGET_UTIL_CRIT:
        tilt -= 0.25
    elif budget_util >= BUDGET_UTIL_HIGH:
        tilt -= 0.12
    elif budget_util <= 0.30 and goodness > 0.55 and stress < 0.5 and manip < 0.5:
        tilt += 0.04

    # 7) Plan-gap nudges (small and conditional)
    if (
        plan_gap <= PLAN_GAP_BEHIND_BAD
        and pnl_score > 0.55
        and dd_score > 0.55
        and reg > 0.55
        and stress < 0.6
        and budget_util <= BUDGET_UTIL_WARN
    ):
        tilt += 0.06

    if (
        plan_gap >= PLAN_GAP_AHEAD_HIGH
        and (dd_score < 0.55 or pnl_score < 0.55 or stress > 0.6 or budget_util > BUDGET_UTIL_HIGH)
    ):
        tilt -= 0.06

    # 8) Streak bias
    tilt += float(streak_bias)

    # 9) Kelly confidence shrinks absolute tilt if low
    shrink = 0.5 + 0.5 * _clamp(k_conf, 0.0, 1.0)  # 0.5..1.0
    tilt *= shrink

    # 10) If baseline capacity is already tiny, don't over-shrink further
    base_cap = _score_capacity_base(risk_snapshot)
    if base_cap < 0.25 and tilt < 0:
        tilt *= 0.45
    if base_cap > 0.85 and tilt > 0:
        tilt *= 0.75

    # tiny sanity: if manip is extreme, never positive tilt
    if manip >= 0.92:
        tilt = min(tilt, 0.0)

    return _clamp(tilt, AI_CAPACITY_MIN, AI_CAPACITY_MAX)


def _compute_regime_entry_gate_factor(
    risk_profile: str,
    pnl_dd: Dict[str, float],
    regime: Dict[str, float],
    budget_util: float,
) -> float:
    reg = regime["regime_score"]
    stress = regime["regime_stress"]
    manip = regime["manipulation_risk"]

    pnl_score = pnl_dd["pnl_score"]
    dd_score = pnl_dd["dd_score"]

    gate = 0.6 + 0.8 * _clamp(reg, 0.0, 1.0)  # [0,1] -> [0.6, 1.4]

    if stress > REGIME_STRESS_HIGH:
        gate *= 0.50
    elif stress > REGIME_STRESS_MED:
        gate *= 0.80

    if manip >= 0.90:
        gate *= 0.55
    elif manip >= 0.75:
        gate *= 0.80

    if risk_profile == "ultra_defensive":
        gate *= 0.55
    elif risk_profile == "defensive":
        gate *= 0.80
    elif risk_profile == "aggressive":
        gate *= 1.12
    elif risk_profile == "degenerate":
        gate *= 1.28

    if budget_util >= BUDGET_UTIL_CRIT:
        gate *= 0.40
    elif budget_util >= BUDGET_UTIL_HIGH:
        gate *= 0.70

    # If pnl/dd are ugly, cap gate
    if pnl_score < 0.30 or dd_score < 0.30:
        gate = min(gate, 0.85)

    return _clamp(gate, REGIME_GATE_MIN, REGIME_GATE_MAX)


def _compute_risk_mode_and_block(
    risk_profile: str,
    pnl_dd: Dict[str, float],
    regime: Dict[str, float],
    risk_snapshot: Dict[str, Any],
    budget_util: float,
) -> Tuple[int, bool]:
    pnl_score = pnl_dd["pnl_score"]
    dd_pct = pnl_dd["dd_pct"]
    dd_score = pnl_dd["dd_score"]

    reg = regime["regime_score"]
    stress = regime["regime_stress"]
    manip = regime["manipulation_risk"]

    circuit_tripped = _safe_bool(risk_snapshot, "circuit_breaker_tripped", False)
    hard_dd = dd_pct <= DD_HARD_CRASH
    extreme_stress = stress >= 0.95
    extreme_manip = (manip >= 0.95 and reg < 0.5)
    over_budget = budget_util >= BUDGET_UTIL_CRIT

    block = bool(circuit_tripped or hard_dd or extreme_stress or extreme_manip or over_budget)

    # Default
    risk_mode = 1

    # Risk-off conditions
    if (
        block
        or dd_pct <= DD_BAD
        or dd_score < 0.30
        or pnl_score < 0.30
        or reg < 0.30
        or stress > REGIME_STRESS_HIGH
        or manip > 0.85
        or risk_profile in ("ultra_defensive", "defensive")
        or budget_util >= BUDGET_UTIL_HIGH
    ):
        risk_mode = 0
    else:
        # Risk-on only in genuinely healthy conditions
        if (
            risk_profile in ("aggressive", "degenerate")
            and pnl_score > 0.60
            and dd_score > 0.60
            and reg > 0.60
            and stress < 0.50
            and manip < 0.60
            and budget_util <= BUDGET_UTIL_WARN
        ):
            risk_mode = 2
        else:
            risk_mode = 1

    return int(risk_mode), bool(block)


def _compute_max_pos_mult(risk_profile: str, ai_capacity_delta: float, risk_mode: int) -> float:
    base = 1.0 + float(ai_capacity_delta) * 0.80  # [-0.6..0.6] -> [-0.48..+0.48]

    if risk_profile == "ultra_defensive":
        base *= 0.50
    elif risk_profile == "defensive":
        base *= 0.80
    elif risk_profile == "aggressive":
        base *= 1.15
    elif risk_profile == "degenerate":
        base *= 1.30

    if risk_mode == 0:
        base *= 0.40
    elif risk_mode == 2:
        base *= 1.20

    return _clamp(base, MAX_POS_MULT_MIN, MAX_POS_MULT_MAX)


# =============================================================================
# Public API – overlay
# =============================================================================


def compute_ai_overlay(
    risk_snapshot: Dict[str, Any],
    perf_snapshot: Optional[Dict[str, Any]] = None,
    regime_snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Core overlay used by RiskManager. PURE.

    Missing fields fall back to neutral defaults.
    If internal exception occurs: returns SAFE risk-off fallback (block entries).
    """
    try:
        risk_snapshot = dict(risk_snapshot or {})

        pnl_dd = _score_pnl_and_dd(risk_snapshot)
        perf = _score_perf(perf_snapshot)
        regime = _score_regime(regime_snapshot, risk_snapshot)
        budget_util = _extract_budget_utilization(risk_snapshot)

        risk_profile = _choose_risk_profile(pnl_dd, perf, regime, budget_util)

        ai_capacity_delta = _compute_ai_capacity_delta(
            risk_snapshot=risk_snapshot,
            pnl_dd=pnl_dd,
            perf=perf,
            regime=regime,
            risk_profile=risk_profile,
            budget_util=budget_util,
        )

        regime_gate = _compute_regime_entry_gate_factor(
            risk_profile=risk_profile,
            pnl_dd=pnl_dd,
            regime=regime,
            budget_util=budget_util,
        )

        risk_mode, block_new_entries = _compute_risk_mode_and_block(
            risk_profile=risk_profile,
            pnl_dd=pnl_dd,
            regime=regime,
            risk_snapshot=risk_snapshot,
            budget_util=budget_util,
        )

        max_pos_mult = _compute_max_pos_mult(
            risk_profile=risk_profile,
            ai_capacity_delta=ai_capacity_delta,
            risk_mode=risk_mode,
        )

        out = {
            "ai_capacity_delta": float(_clamp(ai_capacity_delta, AI_CAPACITY_MIN, AI_CAPACITY_MAX)),
            "risk_profile": str(risk_profile),
            "regime_entry_gate_factor": float(_clamp(regime_gate, REGIME_GATE_MIN, REGIME_GATE_MAX)),
            "risk_mode": int(risk_mode),
            "risk_mode_label": "risk_off" if risk_mode == 0 else ("risk_on" if risk_mode == 2 else "normal"),
            "block_new_entries": bool(block_new_entries),
            "recommend_max_pos_mult": float(_clamp(max_pos_mult, MAX_POS_MULT_MIN, MAX_POS_MULT_MAX)),
            "ai_risk_model_version": AI_RISK_MODEL_VERSION,
            "debug": {
                "pnl_dd": pnl_dd,
                "perf": perf,
                "regime": regime,
                "budget_util": budget_util,
            },
        }

        # enforce schema stability (never crash callers)
        ok, errs = validate_overlay(out)
        if not ok:
            dbg = dict(out.get("debug") or {})
            dbg["schema_errors"] = errs
            out["debug"] = dbg
        return out

    except Exception:
        # SAFE fallback: risk-off + block, deterministic
        return dict(_SAFE_OVERLAY_FALLBACK)


def derive_capacity_multiplier(overlay: Mapping[str, Any], base_capacity: float = 1.0) -> float:
    """
    Convenience helper: overlay + base_capacity -> capacity multiplier in [0.2, 1.8]. PURE.
    """
    try:
        delta = float(overlay.get("ai_capacity_delta", 0.0))
    except Exception:
        delta = 0.0

    base_capacity = _clamp(base_capacity, 0.0, 2.0)
    mult = base_capacity * (1.0 + 0.80 * _clamp(delta, AI_CAPACITY_MIN, AI_CAPACITY_MAX))
    return _clamp(mult, MAX_POS_MULT_MIN, MAX_POS_MULT_MAX)


def is_risk_off(overlay: Mapping[str, Any]) -> bool:
    """
    True if:
      • risk_mode == 0 OR
      • block_new_entries == True
    """
    try:
        mode = int(overlay.get("risk_mode", 1))
    except Exception:
        mode = 1
    block = bool(overlay.get("block_new_entries", False))
    return bool(block or mode == 0)


# =============================================================================
# Backward-compatible adapter used by risk_manager.current_limits()
# =============================================================================


def _build_risk_snapshot_from_features(features: Dict[str, Any]) -> Dict[str, Any]:
    """
    Accept compact features (often from risk_manager) OR a risk_snapshot already.
    """
    if not features:
        return {}

    # Already a risk_snapshot?
    if ("daily_realized_pnl_pct" in features) or ("daily_drawdown_pct" in features):
        snap = dict(features)
        dd = _safe_float(snap, "daily_drawdown_pct", 0.0)
        if dd > 0:
            snap["daily_drawdown_pct"] = -abs(dd)
        return snap

    # Equity / PnL synonyms
    eq = _first_present_float(features, ("equity_usd", "equityUSD", "equity", "eq_usd", "eqUSD", "eq"), 0.0)
    realized = _first_present_float(
        features,
        ("realized_pnl_usd", "realizedPnlUsd", "realized_pnl_today_usd", "realized_today_usd"),
        0.0,
    )
    dd_frac = _safe_float(features, "rolling_dd", 0.0)  # 0..1
    plan_gap = _safe_float(features, "plan_gap_pct", 0.0)
    base_capacity = _safe_float(features, "risk_capacity", 0.5)
    circuit_tripped = _safe_bool(features, "circuit_breaker_tripped", False)

    daily_pnl_pct = (realized / eq * 100.0) if eq > 0 else 0.0
    daily_dd_pct = -abs(dd_frac) * 100.0

    regime_mult = _safe_float(features, "regime_mult", 1.0)
    regime_label_numeric = _safe_float(features, "regime_label_numeric", 0.0)
    regime_stress_score = _safe_float(features, "regime_stress_score", 0.0)
    regime_signal_strength = _safe_float(features, "regime_signal_strength", 0.0)
    manip = _safe_float(features, "manipulation_risk", _safe_float(features, "manipulationRisk", 0.0))

    budget_util = _extract_budget_utilization(features)

    # If only regime_mult exists, derive coarse regime signals
    if (
        regime_label_numeric == 0.0
        and regime_stress_score == 0.0
        and regime_signal_strength == 0.0
        and ("regime_mult" in features)
    ):
        if regime_mult <= 0.7:
            regime_label_numeric, regime_signal_strength, regime_stress_score = -0.8, 0.30, 0.70
        elif regime_mult <= 0.9:
            regime_label_numeric, regime_signal_strength, regime_stress_score = -0.4, 0.40, 0.55
        elif regime_mult >= 1.3:
            regime_label_numeric, regime_signal_strength, regime_stress_score = 0.8, 0.80, 0.30
        elif regime_mult >= 1.1:
            regime_label_numeric, regime_signal_strength, regime_stress_score = 0.4, 0.60, 0.40
        else:
            regime_label_numeric, regime_signal_strength, regime_stress_score = 0.0, 0.50, 0.50

    return {
        "risk_capacity": base_capacity,
        "daily_realized_pnl_pct": daily_pnl_pct,
        "daily_drawdown_pct": daily_dd_pct,
        "equity_usd": eq,
        "plan_gap_pct": plan_gap,
        "regime_label_numeric": regime_label_numeric,
        "regime_stress_score": regime_stress_score,
        "regime_signal_strength": regime_signal_strength,
        "manipulation_risk": _clamp(manip, 0.0, 1.0),
        "circuit_breaker_tripped": circuit_tripped,
        "risk_budget_util": budget_util,
    }


def _build_perf_snapshot_from_features(features: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not features:
        return None

    wr = _safe_float(features, "winrate20", _safe_float(features, "win_rate", 0.0))
    rwr = _safe_float(features, "recent_winrate20", _safe_float(features, "recent_win_rate", wr))
    sharpe = _safe_float(features, "sharpe20", _safe_float(features, "sharpe_like", 0.0))
    rsharpe = _safe_float(features, "recent_sharpe20", _safe_float(features, "recent_sharpe_like", sharpe))
    k_conf = _safe_float(features, "kelly_confidence", 0.0)

    win_streak = _safe_float(features, "win_streak", 0.0)
    loss_streak = _safe_float(features, "loss_streak", 0.0)

    if wr == 0.0 and sharpe == 0.0 and k_conf == 0.0 and win_streak == 0.0 and loss_streak == 0.0:
        return None

    return {
        "win_rate": wr,
        "recent_win_rate": rwr,
        "sharpe_like": sharpe,
        "recent_sharpe_like": rsharpe,
        "kelly_confidence": _clamp(k_conf, 0.0, 1.0),
        "win_streak": win_streak,
        "loss_streak": loss_streak,
    }


def predict_capacity_adjustment(features: Dict[str, Any]) -> float:
    """
    Compatibility function.

    Returns only ai_capacity_delta in [-0.6, +0.6].
    Safe fallback: returns AI_CAPACITY_MIN on internal exception (risk-off bias).
    """
    try:
        risk_snapshot = _build_risk_snapshot_from_features(features or {})
        perf_snapshot = _build_perf_snapshot_from_features(features or {})
        overlay = compute_ai_overlay(risk_snapshot, perf_snapshot=perf_snapshot, regime_snapshot=None)
        delta = float(overlay.get("ai_capacity_delta", 0.0))
        return float(_clamp(delta, AI_CAPACITY_MIN, AI_CAPACITY_MAX))
    except Exception:
        return float(AI_CAPACITY_MIN)


# =============================================================================
# Candidate-level risk scoring (per-token)
# =============================================================================


def _candidate_components(
    features: Mapping[str, Any],
) -> Tuple[Dict[str, float], Dict[str, str], Dict[str, Any], bool]:
    """
    Build component risks in 0..1 plus short 'why' and raw extracted values.
    Returns (components, why_short, raw, hard_flag).
    """
    comp: Dict[str, float] = {}
    why: Dict[str, str] = {}
    raw: Dict[str, Any] = {}

    # Age (minutes) - missing => medium risk (not "safe")
    age_min = _first_present_float(features, ("age_min", "ageMinutes", "pool_age_min"), 45.0)
    raw["age_min"] = age_min
    age_risk = _map_linear(age_min, 180.0, 3.0)  # 3m => ~1, 180m => ~0
    comp["age_risk"] = _clamp(age_risk, 0.0, 1.0)
    why["age_risk"] = f"age={age_min:.1f}m"

    # Liquidity USD - missing => medium risk
    liq = _first_present_float(features, ("liq_usd", "liquidity_usd", "liquidity", "liq"), 25_000.0)
    raw["liq_usd"] = liq
    liq_risk = _map_linear(liq, 50_000.0, 800.0)
    comp["liq_risk"] = _clamp(liq_risk, 0.0, 1.0)
    why["liq_risk"] = f"liq=${liq:,.0f}"

    # 5m volume - missing => medium risk
    vol_5m = _first_present_float(features, ("vol_5m_usd", "volume_5m_usd", "vol5", "vol_5m"), 0.0)
    raw["vol_5m_usd"] = vol_5m
    if vol_5m <= 0.0:
        vol_risk = 0.55
    else:
        vol_risk = _map_linear(vol_5m, 15_000.0, 500.0)
    comp["volume_risk"] = _clamp(vol_risk, 0.0, 1.0)
    why["volume_risk"] = f"vol5m≈${vol_5m:,.0f}"

    # Holder concentration - missing => medium risk
    top1 = _first_present_float(features, ("top_holder_pct", "top1_holder_pct", "top1_pct"), 0.10)  # 10% default
    top10 = _first_present_float(features, ("top10_holder_pct", "top10_pct", "holder_concentration"), 0.45)  # 45%
    raw["top1_pct"] = top1
    raw["top10_pct"] = top10
    if top1 > 1.0:
        top1 /= 100.0
    if top10 > 1.0:
        top10 /= 100.0
    conc = _clamp(0.65 * _clamp(top1 * 2.0, 0.0, 1.0) + 0.35 * _clamp(top10, 0.0, 1.0), 0.0, 1.0)
    comp["conc_risk"] = conc
    why["conc_risk"] = f"top1≈{top1*100:.0f}% top10≈{top10*100:.0f}%"

    # Hard sellability/honeypot flags
    hard_flag = False
    for k in ("honeypot_flag", "is_honeypot", "rugpull_flag", "is_rug", "is_scam"):
        if _safe_bool(features, k, False):
            hard_flag = True
            raw["hard_flag_key"] = k
            break
    if ("can_sell" in features) and (features.get("can_sell") is False):
        hard_flag = True
        raw["hard_flag_key"] = "can_sell=False"

    if hard_flag:
        comp["honeypot_risk"] = 1.0
        why["honeypot_risk"] = "flagged"
    else:
        soft = _first_present_float(
            features,
            ("honeypot_score", "honeypot_prob", "rug_score", "fraud_score", "ai_risk_score"),
            0.35,  # missing => not safe
        )
        soft01 = _as_frac_01(soft)
        raw["honeypot_soft"] = soft
        comp["honeypot_risk"] = soft01
        why["honeypot_risk"] = f"soft={soft01:.2f}"

    # Mint/freeze authority risk - unknown => medium
    mint_auth_disabled = features.get("mint_authority_disabled")
    freeze_auth_disabled = features.get("freeze_authority_disabled")
    raw["mint_authority_disabled"] = mint_auth_disabled
    raw["freeze_authority_disabled"] = freeze_auth_disabled
    mint_r = 0.0 if (mint_auth_disabled is True) else (0.8 if mint_auth_disabled is False else 0.45)
    freeze_r = 0.0 if (freeze_auth_disabled is True) else (0.8 if freeze_auth_disabled is False else 0.45)
    comp["authority_risk"] = _clamp(0.5 * mint_r + 0.5 * freeze_r, 0.0, 1.0)
    why["authority_risk"] = f"mint={mint_auth_disabled} freeze={freeze_auth_disabled}"

    # Price impact / slippage proxy (bps) - missing => medium
    impact_bps = _first_present_float(features, ("impact_1k_bps", "priceImpact1kBps", "price_impact_bps"), 450.0)
    raw["impact_1k_bps"] = impact_bps
    impact_risk = _map_linear(impact_bps, 1200.0, 200.0)
    comp["impact_risk"] = _clamp(impact_risk, 0.0, 1.0)
    why["impact_risk"] = f"impact≈{impact_bps:.0f}bps"

    # Deployer behavior (optional) - missing => medium
    dev_sell = _first_present_float(features, ("dev_sell_5m_usd", "deployer_sell_5m_usd"), 0.0)
    dev_hold = _first_present_float(features, ("dev_hold_pct", "deployer_hold_pct", "creator_hold_pct"), 0.0)
    raw["dev_sell_5m_usd"] = dev_sell
    raw["dev_hold_pct"] = dev_hold
    if dev_hold > 1.0:
        dev_hold /= 100.0
    dev_sell_r = _clamp(math.log10(dev_sell + 1.0) / 3.0, 0.0, 1.0)
    dev_hold_r = _clamp(dev_hold * 1.6, 0.0, 1.0)
    comp["deployer_risk"] = _clamp(0.55 * dev_sell_r + 0.45 * dev_hold_r, 0.0, 1.0)
    why["deployer_risk"] = f"dev_sell5m=${dev_sell:.0f} dev_hold≈{dev_hold*100:.0f}%"

    # Micro-manip proxy from buys/sells
    buys = _first_present_float(features, ("buys_5m", "buys5", "buys_1m"), 0.0)
    sells = _first_present_float(features, ("sells_5m", "sells5", "sells_1m"), 0.0)
    raw["buys"] = buys
    raw["sells"] = sells
    total = buys + sells
    buy_ratio = (buys / total) if total > 0 else 0.5
    manip_r = 0.45 if total == 0 else 0.0  # no prints => not safe
    if total >= 6 and buy_ratio > 0.92 and sells <= 1:
        manip_r = 0.8
    elif total >= 10 and buy_ratio > 0.88 and sells <= 2:
        manip_r = 0.6
    elif total > 0 and buy_ratio < 0.25:
        manip_r = 0.5
    comp["manip_risk"] = _clamp(manip_r, 0.0, 1.0)
    why["manip_risk"] = f"buys/sells={int(buys)}/{int(sells)} br={buy_ratio:.2f}"

    # Whale / smart money concentration (optional)
    whale_hold = _first_present_float(features, ("whale_hold_pct", "whale_pct", "smart_money_hold_pct"), 0.0)
    raw["whale_hold_pct"] = whale_hold
    if whale_hold > 1.0:
        whale_hold /= 100.0
    whale_risk = _clamp(whale_hold * 1.2, 0.0, 1.0)
    comp["whale_risk"] = whale_risk
    why["whale_risk"] = (f"whale_hold≈{whale_hold*100:.0f}%" if whale_hold > 0.0 else "unknown")

    # Short-term trend / blow-off / dump risk (5m change)
    chg5 = _first_present_float(features, ("chg5", "change_5m_pct", "price_change_5m_pct"), 0.0)
    raw["chg5_pct"] = chg5
    if chg5 >= 0:
        trend_risk = _map_linear(chg5, 80.0, 10.0)
    else:
        trend_risk = _map_linear(chg5, -5.0, -35.0)
    comp["trend_risk"] = _clamp(trend_risk, 0.0, 1.0)
    why["trend_risk"] = f"chg5m={chg5:.1f}%"

    return comp, why, raw, hard_flag


def score_candidate(features: Dict[str, Any], config: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    """
    Candidate-level risk scoring.

    Safe fallback: on internal exception returns critical + hard_block.

    Optional config:
      - AI_RISK_W_* weights (AI_RISK_W_AGE, AI_RISK_W_LIQ, ...)
      - AI_RISK_HEUR_WEIGHT (blend with precomputed ai_risk_score/fraud_score if present)
      - AI_RISK_MIN_CLIP / AI_RISK_MAX_CLIP
      - AI_RISK_LABEL_SAFE_MAX, AI_RISK_LABEL_OK_MAX, AI_RISK_LABEL_ELEV_MAX
    """
    try:
        if features is None:
            features = {}

        comp, why, raw, hard_flag = _candidate_components(features)

        # Default weights (sum ~1; honeypot + conc + liq dominate)
        w = {
            "age_risk": 0.10,
            "liq_risk": 0.16,
            "volume_risk": 0.06,
            "conc_risk": 0.16,
            "honeypot_risk": 0.22,
            "authority_risk": 0.06,
            "impact_risk": 0.08,
            "deployer_risk": 0.06,
            "manip_risk": 0.04,
            "whale_risk": 0.04,
            "trend_risk": 0.02,
        }

        if config:
            for k in list(w.keys()):
                base = k.replace("_risk", "").upper()  # AGE, LIQ, ...
                ck = f"AI_RISK_W_{base}"
                if ck in config:
                    try:
                        w[k] = float(config[ck])
                    except Exception:
                        pass
            s = sum(max(0.0, float(v)) for v in w.values())
            if s > 0:
                for k in w:
                    w[k] = max(0.0, float(w[k])) / s

        heur = 0.0
        for k, wk in w.items():
            heur += float(wk) * float(_clamp(comp.get(k, 0.0), 0.0, 1.0))
        heur = _clamp(heur, 0.0, 1.0)

        # Optional blend with a precomputed score
        pre = None
        for key in ("ai_risk_score", "fraud_score", "rug_score", "security_risk_score"):
            if key in features and features[key] is not None:
                try:
                    pre = _as_frac_01(float(features[key]))
                    raw["precomputed_key"] = key
                    raw["precomputed_val"] = features[key]
                    break
                except Exception:
                    continue

        heur_weight = 0.75
        if config and "AI_RISK_HEUR_WEIGHT" in config:
            try:
                heur_weight = _clamp(float(config["AI_RISK_HEUR_WEIGHT"]), 0.0, 1.0)
            except Exception:
                pass

        score = heur if pre is None else _clamp(heur_weight * heur + (1.0 - heur_weight) * float(pre), 0.0, 1.0)

        # Confidence: number of meaningful signals present
        sig = 0
        for k in (
            "age_min",
            "liq_usd",
            "liquidity_usd",
            "top10_holder_pct",
            "top_holder_pct",
            "honeypot_flag",
            "honeypot_score",
            "mint_authority_disabled",
            "freeze_authority_disabled",
            "impact_1k_bps",
            "chg5",
        ):
            if k in features and features[k] is not None:
                sig += 1
        conf = _clamp(0.20 + 0.10 * sig, 0.20, 0.92)

        # Optional clip
        if config:
            try:
                mn = float(config.get("AI_RISK_MIN_CLIP", 0.0))
                mx = float(config.get("AI_RISK_MAX_CLIP", 1.0))
                if mx < mn:
                    mx = mn
                score = _clamp(score, mn, mx)
            except Exception:
                score = _clamp(score, 0.0, 1.0)

        # Label thresholds (configurable)
        safe_max = 0.25
        ok_max = 0.45
        elev_max = 0.70
        if config:
            try:
                safe_max = float(config.get("AI_RISK_LABEL_SAFE_MAX", safe_max))
                ok_max = float(config.get("AI_RISK_LABEL_OK_MAX", ok_max))
                elev_max = float(config.get("AI_RISK_LABEL_ELEV_MAX", elev_max))
            except Exception:
                pass
        safe_max = _clamp(safe_max, 0.05, 0.50)
        ok_max = _clamp(ok_max, safe_max + 0.01, 0.80)
        elev_max = _clamp(elev_max, ok_max + 0.01, 0.95)

        if score < safe_max:
            label = "safe"
        elif score < ok_max:
            label = "ok"
        elif score < elev_max:
            label = "elevated"
        else:
            label = "critical"

        # Hard block rules (never permissive if honeypot flagged)
        hard_block = bool(hard_flag or (score >= 0.90 and conf >= 0.60))

        reasons = [
            f"score={score:.2f} ({label}) conf={conf:.2f}",
            f"age: {why.get('age_risk','')}",
            f"liq: {why.get('liq_risk','')}",
            f"vol: {why.get('volume_risk','')}",
            f"conc: {why.get('conc_risk','')}",
            f"hp: {why.get('honeypot_risk','')}",
            f"auth: {why.get('authority_risk','')}",
            f"impact: {why.get('impact_risk','')}",
            f"dev: {why.get('deployer_risk','')}",
            f"whale: {why.get('whale_risk','')}",
            f"manip: {why.get('manip_risk','')}",
            f"trend: {why.get('trend_risk','')}",
        ]
        if hard_block:
            reasons.insert(1, "HARD_BLOCK=TRUE")

        out = {
            "score": float(score),
            "conf": float(conf),
            "label": str(label),
            "hard_block": bool(hard_block),
            "components": {k: float(_clamp(v, 0.0, 1.0)) for k, v in comp.items()},
            "raw": dict(raw),
            "reasons": list(reasons),
            "version": AI_RISK_MODEL_VERSION,
        }

        ok, errs = validate_candidate_score(out)
        if not ok:
            raw2 = dict(out.get("raw") or {})
            raw2["schema_errors"] = errs
            out["raw"] = raw2
        return out

    except Exception:
        return dict(_SAFE_CANDIDATE_FALLBACK)


# =============================================================================
# Aliases (candidate-level)
# =============================================================================


def score_entry(features: Dict[str, Any], config: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    return score_candidate(features, config)


def score(features: Dict[str, Any], config: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    return score_candidate(features, config)


def evaluate(features: Dict[str, Any], config: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    return score_candidate(features, config)


# =============================================================================
# Adapters used by scoring_service (expects risk_score or score key)
# =============================================================================


def score_risk(
    features: Dict[str, Any],
    meta: Optional[Dict[str, Any]] = None,
    config: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    out = score_candidate(features or {}, config=config)
    return {
        "risk_score": out["score"],  # 0..1 (caller may scale to 0..100)
        "score": out["score"],
        "conf": out.get("conf", 0.0),
        "label": out.get("label", "ok"),
        "reasons": out.get("reasons", []),
        "version": out.get("version", AI_RISK_MODEL_VERSION),
        "meta": meta or {},
        "hard_block": bool(out.get("hard_block", False)),
    }


def risk_score(
    features: Dict[str, Any],
    meta: Optional[Dict[str, Any]] = None,
    config: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    return score_risk(features, meta=meta, config=config)


# =============================================================================
# Strict schema validators (optional but used internally for stability)
# =============================================================================


def validate_overlay(overlay: Mapping[str, Any]) -> Tuple[bool, list[str]]:
    errs: list[str] = []
    if overlay is None:
        return False, ["overlay is None"]

    for k in OVERLAY_REQUIRED_KEYS:
        if k not in overlay:
            errs.append(f"missing:{k}")

    # numeric range checks (best-effort)
    try:
        d = float(overlay.get("ai_capacity_delta", 0.0))
        if not (AI_CAPACITY_MIN <= d <= AI_CAPACITY_MAX):
            errs.append("ai_capacity_delta out_of_range")
    except Exception:
        errs.append("ai_capacity_delta not_float")

    try:
        g = float(overlay.get("regime_entry_gate_factor", 1.0))
        if not (REGIME_GATE_MIN <= g <= REGIME_GATE_MAX):
            errs.append("regime_entry_gate_factor out_of_range")
    except Exception:
        errs.append("regime_entry_gate_factor not_float")

    try:
        m = int(overlay.get("risk_mode", 1))
        if m not in (0, 1, 2):
            errs.append("risk_mode invalid")
    except Exception:
        errs.append("risk_mode not_int")

    try:
        mp = float(overlay.get("recommend_max_pos_mult", 1.0))
        if not (MAX_POS_MULT_MIN <= mp <= MAX_POS_MULT_MAX):
            errs.append("recommend_max_pos_mult out_of_range")
    except Exception:
        errs.append("recommend_max_pos_mult not_float")

    # label check
    lbl = str(overlay.get("risk_mode_label", ""))
    if lbl not in ("risk_off", "normal", "risk_on"):
        errs.append("risk_mode_label invalid")

    rp = str(overlay.get("risk_profile", ""))
    if rp not in ("ultra_defensive", "defensive", "balanced", "aggressive", "degenerate"):
        errs.append("risk_profile invalid")

    return (len(errs) == 0), errs


def validate_candidate_score(out: Mapping[str, Any]) -> Tuple[bool, list[str]]:
    errs: list[str] = []
    if out is None:
        return False, ["candidate_out is None"]

    for k in CANDIDATE_REQUIRED_KEYS:
        if k not in out:
            errs.append(f"missing:{k}")

    try:
        s = float(out.get("score", 0.0))
        if not (0.0 <= s <= 1.0):
            errs.append("score out_of_range")
    except Exception:
        errs.append("score not_float")

    try:
        c = float(out.get("conf", 0.0))
        if not (0.0 <= c <= 1.0):
            errs.append("conf out_of_range")
    except Exception:
        errs.append("conf not_float")

    lbl = str(out.get("label", ""))
    if lbl not in ("safe", "ok", "elevated", "critical"):
        errs.append("label invalid")

    if not isinstance(out.get("reasons", []), list):
        errs.append("reasons not_list")

    comp = out.get("components", {})
    if not isinstance(comp, dict):
        errs.append("components not_dict")

    return (len(errs) == 0), errs
