# utils_pkg/ai_risk_model.py
from __future__ import annotations

"""
utils_pkg/ai_risk_model.py

AI risk overlay + candidate risk model (V3+++ / quant-grade)
============================================================

This module has TWO responsibilities:

(1) Account / regime overlay (slow, session-level)
--------------------------------------------------
    compute_ai_overlay(risk_snapshot, perf_snapshot=None, regime_snapshot=None, config=None) -> dict

    Returns a PURE dict (no IO / no env / no globals) that RiskManager and the rest of
    the system can consume to modulate capacity, concurrency and entry gating.

    Strict output schema:
      • ai_capacity_delta        in [-0.60, +0.60]
      • risk_profile             "ultra_defensive"|"defensive"|"balanced"|"aggressive"|"degenerate"
      • regime_entry_gate_factor in [0.20, 1.60]
      • risk_mode                int {0,1,2}  (0=risk_off, 1=normal, 2=risk_on)
      • risk_mode_label          "risk_off"|"normal"|"risk_on"
      • block_new_entries        bool
      • recommend_max_pos_mult   in [0.20, 1.80]
      • ai_risk_model_version    str
      • debug                    dict (stable keys; numeric where possible)

(2) Candidate-level risk score (fast, per-trade)
------------------------------------------------
    score_candidate(features, config=None) -> dict

    Strict output schema:
      • score      in [0,1]   (0=safest, 1=highest risk)
      • conf       in [0,1]
      • label      "safe"|"ok"|"elevated"|"critical"
      • hard_block bool
      • components dict[str,float] (0..1 per component)
      • raw        dict (extracted values for debugging)
      • reasons    list[str] (short human-readable strings)
      • version    str

Design goals
------------
- PURE / deterministic: dict in, dict out. No IO, no env, no imports that do IO.
- Safe for missing fields: defaults are neutral-to-conservative (NOT “safe”).
- Interpretable: exposes components + reasons + raw values.
- Backward-compatible aliases: score_risk / risk_score / score / evaluate / score_entry.
- Safe fallback: if anything internal errors,
    overlay -> risk_off + block entries
    candidate -> critical + hard_block
"""

import contextlib
import math
from collections.abc import Iterable, Mapping
from typing import Any

__all__ = [
    "AI_RISK_MODEL_VERSION",
    "candidate_gate",
    # overlay
    "compute_ai_overlay",
    "derive_capacity_multiplier",
    "evaluate",
    "explain_candidate",
    "explain_overlay",
    "is_risk_off",
    "overlay_from_features",
    "predict_capacity_adjustment",
    "risk_score",
    "score",
    # candidate
    "score_candidate",
    "score_entry",
    "score_risk",
    "validate_candidate_score",
    "validate_overlay",
]

# Bump whenever heuristics meaningfully change (stable for logging/audits)
AI_RISK_MODEL_VERSION = "v3+++-2026-02-03"

# =============================================================================
# Output bounds
# =============================================================================
AI_CAPACITY_MIN = -0.60
AI_CAPACITY_MAX = +0.60

REGIME_GATE_MIN = 0.20
REGIME_GATE_MAX = 1.60

MAX_POS_MULT_MIN = 0.20
MAX_POS_MULT_MAX = 1.80

# =============================================================================
# Overlay heuristic thresholds (percent-space or scalar)
# =============================================================================
DD_HARD_CRASH = -22.0
DD_BAD = -12.0
DD_OK = -5.0

PNL_TERRIBLE = -15.0
PNL_BAD = -6.0
PNL_OK = 0.0
PNL_GOOD = 6.0
PNL_GREAT = 15.0

WINRATE_LOW = 0.40
WINRATE_OK = 0.50
WINRATE_GOOD = 0.60
WINRATE_GREAT = 0.68

SHARPE_LOW = -0.2
SHARPE_OK = 0.1
SHARPE_GOOD = 0.3
SHARPE_GREAT = 0.6

REGIME_STRESS_HIGH = 0.70
REGIME_STRESS_MED = 0.45

PLAN_GAP_BEHIND_BAD = -12.0
PLAN_GAP_AHEAD_HIGH = 12.0

# Risk-budget utilization (0..1+)
BUDGET_UTIL_WARN = 0.75
BUDGET_UTIL_HIGH = 0.90
BUDGET_UTIL_CRIT = 1.00

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
_SAFE_OVERLAY_FALLBACK: dict[str, Any] = {
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
            "winrate_score": 0.50,
            "recent_winrate_score": 0.50,
            "sharpe_score": 0.50,
            "recent_sharpe_score": 0.50,
            "kelly_confidence": 0.00,
            "streak_bias": 0.00,
        },
        "regime": {
            "regime_score": 0.0,
            "regime_stress": 1.0,
            "regime_signal": 0.0,
            "regime_label": -1.0,
            "manipulation_risk": 1.0,
        },
        "budget_util": 9.99,
        "notes": ["SAFE_FALLBACK"],
    },
}

_SAFE_CANDIDATE_FALLBACK: dict[str, Any] = {
    "score": 1.0,
    "conf": 0.20,
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
    if xf in (float("inf"), float("-inf")):
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


def _truthy(v: Any, default: bool = False) -> bool:
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


def _map_linear(x: float, x0: float, x1: float) -> float:
    """
    Map [x0,x1] -> [0,1] with clamp; inverted if x0 > x1.
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


def _as_frac_01(v: Any) -> float:
    """
    Accept either 0..1 or 0..100 and normalize to 0..1.
    """
    try:
        vf = float(v)
    except Exception:
        return 0.0
    if 1.0 < vf <= 100.0:
        vf = vf / 100.0
    return _clamp(vf, 0.0, 1.0)


def _first_present_float(
    features: Mapping[str, Any], keys: Iterable[str], default: float = 0.0
) -> float:
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


def _cfg_float(cfg: Mapping[str, Any] | None, key: str, default: float) -> float:
    if not cfg:
        return float(default)
    try:
        if key in cfg and cfg[key] is not None:
            return float(cfg[key])
    except Exception:
        pass
    return float(default)


def _cfg_bool(cfg: Mapping[str, Any] | None, key: str, default: bool) -> bool:
    if not cfg:
        return bool(default)
    return _truthy(cfg.get(key, default), default=bool(default))


def _extract_budget_utilization(risk_snapshot: Mapping[str, Any]) -> float:
    """
    Extract risk budget utilization as 0..1+ (1.0 = hit limit) from several keys.
    Accepts percent or fraction.
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
            "daily_loss_util",
        ),
        0.0,
    )
    if util > 1.0 and util <= 100.0:
        util = util / 100.0
    return float(max(0.0, util))


def _pct_to_frac(v: Any) -> float:
    """Accept percent or fraction; return fraction 0..1+ (no clamp)."""
    try:
        x = float(v)
    except Exception:
        return 0.0
    if x > 1.0 and x <= 100.0:
        x /= 100.0
    return float(x)


# =============================================================================
# Overlay scoring primitives
# =============================================================================


def _score_pnl_and_dd(risk_snapshot: Mapping[str, Any]) -> dict[str, float]:
    pnl = _safe_float(risk_snapshot, "daily_realized_pnl_pct", 0.0)
    dd = _safe_float(risk_snapshot, "daily_drawdown_pct", 0.0)

    # tolerate dd convention: enforce negative = drawdown
    if dd > 0:
        dd = -abs(dd)

    pnl_score = _map_linear(pnl, PNL_TERRIBLE, PNL_GREAT)  # higher -> better
    dd_score = _map_linear(dd, DD_HARD_CRASH, 0.0)  # less negative -> better

    return {"pnl_score": pnl_score, "dd_score": dd_score, "dd_pct": dd, "pnl_pct": pnl}


def _score_perf(perf_snapshot: Mapping[str, Any] | None) -> dict[str, float]:
    if not perf_snapshot:
        # Neutral perf when missing; NOT optimistic.
        return {
            "winrate_score": 0.50,
            "recent_winrate_score": 0.50,
            "sharpe_score": 0.50,
            "recent_sharpe_score": 0.50,
            "kelly_confidence": 0.00,
            "streak_bias": 0.00,
        }

    wr = _safe_float(perf_snapshot, "win_rate", 0.0)
    rwr = _safe_float(perf_snapshot, "recent_win_rate", wr)
    sharpe = _safe_float(perf_snapshot, "sharpe_like", 0.0)
    rsharpe = _safe_float(perf_snapshot, "recent_sharpe_like", sharpe)
    k_conf = _safe_float(perf_snapshot, "kelly_confidence", 0.0)

    win_streak = _safe_float(perf_snapshot, "win_streak", 0.0)
    loss_streak = _safe_float(perf_snapshot, "loss_streak", 0.0)

    streak_bias = 0.0
    # penalize loss streak harder than reward win streak
    if loss_streak >= 3:
        streak_bias -= min(0.18, 0.03 * loss_streak)
    if win_streak >= 3:
        streak_bias += min(0.12, 0.02 * win_streak)

    return {
        "winrate_score": _map_linear(wr, WINRATE_LOW, WINRATE_GREAT),
        "recent_winrate_score": _map_linear(rwr, WINRATE_LOW, WINRATE_GREAT),
        "sharpe_score": _map_linear(sharpe, SHARPE_LOW, SHARPE_GREAT),
        "recent_sharpe_score": _map_linear(rsharpe, SHARPE_LOW, SHARPE_GREAT),
        "kelly_confidence": _clamp(k_conf, 0.0, 1.0),
        "streak_bias": float(streak_bias),
    }


def _score_regime(
    regime_snapshot: Mapping[str, Any] | None, risk_snapshot: Mapping[str, Any]
) -> dict[str, float]:
    # merge (regime_snapshot wins)
    src: dict[str, Any] = dict(risk_snapshot or {})
    if regime_snapshot:
        for k, v in regime_snapshot.items():
            src[k] = v

    label = _safe_float(src, "regime_label_numeric", 0.0)  # ~[-1..+1]
    stress = _safe_float(src, "regime_stress_score", 0.0)  # 0..1
    signal = _safe_float(src, "regime_signal_strength", 0.0)  # 0..1
    manip = _safe_float(src, "manipulation_risk", _safe_float(src, "manipulationRisk", 0.0))  # 0..1

    label_score = _map_linear(label, -1.0, +1.0)
    signal_score = _map_linear(signal, 0.0, 1.0)
    stress_score = _map_linear(stress, 1.0, 0.0)  # inverted (high stress worse)
    manip_pen = _map_linear(manip, 0.60, 1.00)  # 0..1 penalty

    base = 0.46 * label_score + 0.40 * signal_score + 0.14 * stress_score
    base *= 1.0 - 0.30 * manip_pen

    # stress tiers
    if stress > REGIME_STRESS_HIGH:
        base = max(0.0, base - 0.38)
    elif stress > REGIME_STRESS_MED:
        base = max(0.0, base - 0.14)

    return {
        "regime_score": _clamp(base, 0.0, 1.0),
        "regime_stress": _clamp(stress, 0.0, 1.0),
        "regime_signal": _clamp(signal, 0.0, 1.0),
        "regime_label": float(label),
        "manipulation_risk": _clamp(manip, 0.0, 1.0),
    }


def _choose_risk_profile(
    pnl_dd: Mapping[str, float],
    perf: Mapping[str, float],
    regime: Mapping[str, float],
    budget_util: float,
    cfg: Mapping[str, Any] | None,
) -> str:
    dd_pct = float(pnl_dd["dd_pct"])
    pnl_score = float(pnl_dd["pnl_score"])

    wr = float(perf["winrate_score"])
    rwr = float(perf["recent_winrate_score"])
    sh = float(perf["sharpe_score"])
    rsh = float(perf["recent_sharpe_score"])

    reg = float(regime["regime_score"])
    stress = float(regime["regime_stress"])
    manip = float(regime["manipulation_risk"])

    dd_hard = _cfg_float(cfg, "AI_OVERLAY_DD_HARD_CRASH", DD_HARD_CRASH)
    dd_bad = _cfg_float(cfg, "AI_OVERLAY_DD_BAD", DD_BAD)
    budget_crit = _cfg_float(cfg, "AI_OVERLAY_BUDGET_CRIT", BUDGET_UTIL_CRIT)
    budget_high = _cfg_float(cfg, "AI_OVERLAY_BUDGET_HIGH", BUDGET_UTIL_HIGH)

    # Hard stops
    if dd_pct <= dd_hard or stress >= 0.95 or budget_util >= budget_crit:
        return "ultra_defensive"
    if manip >= 0.92 and reg < 0.45:
        return "ultra_defensive"

    # Defensive if edge looks poor, DD ugly, or budget nearly tapped
    if (
        dd_pct <= dd_bad
        or pnl_score < 0.35
        or wr < 0.45
        or rwr < 0.42
        or sh < 0.35
        or rsh < 0.35
        or reg < 0.35
        or stress > REGIME_STRESS_HIGH
        or (manip > 0.80 and reg < 0.55)
        or budget_util >= budget_high
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
        and budget_util <= _cfg_float(cfg, "AI_OVERLAY_BUDGET_WARN", BUDGET_UTIL_WARN)
    ):
        if pnl_score > 0.85 and sh > 0.85 and reg > 0.85 and dd_pct > DD_OK:
            return "degenerate"
        return "aggressive"

    return "balanced"


def _compute_ai_capacity_delta(
    risk_snapshot: Mapping[str, Any],
    pnl_dd: Mapping[str, float],
    perf: Mapping[str, float],
    regime: Mapping[str, float],
    risk_profile: str,
    budget_util: float,
    cfg: Mapping[str, Any] | None,
) -> float:
    pnl_score = float(pnl_dd["pnl_score"])
    dd_score = float(pnl_dd["dd_score"])

    wr = float(perf["winrate_score"])
    rwr = float(perf["recent_winrate_score"])
    sh = float(perf["sharpe_score"])
    rsh = float(perf["recent_sharpe_score"])
    k_conf = float(perf["kelly_confidence"])
    streak_bias = float(perf.get("streak_bias", 0.0))

    reg = float(regime["regime_score"])
    stress = float(regime["regime_stress"])
    manip = float(regime["manipulation_risk"])
    label = float(regime["regime_label"])

    plan_gap = _safe_float(risk_snapshot, "plan_gap_pct", 0.0)

    # Composite "goodness" 0..1
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

    # Base tilt in roughly [-0.35, +0.35]
    tilt = (goodness - 0.5) * 0.70

    # Risk-profile shaping
    if risk_profile == "ultra_defensive":
        tilt -= 0.26
    elif risk_profile == "defensive":
        tilt -= 0.10
    elif risk_profile == "aggressive":
        tilt += 0.05
    elif risk_profile == "degenerate":
        tilt += 0.10

    # Regime safety
    if stress > REGIME_STRESS_HIGH or label <= -0.7:
        tilt -= 0.18
    elif stress > REGIME_STRESS_MED or label <= -0.4:
        tilt -= 0.08

    # Manipulation clamp
    if manip >= 0.90:
        tilt -= 0.26
    elif manip >= 0.75:
        tilt -= 0.12

    # Budget shaping
    if budget_util >= _cfg_float(cfg, "AI_OVERLAY_BUDGET_CRIT", BUDGET_UTIL_CRIT):
        tilt -= 0.25
    elif budget_util >= _cfg_float(cfg, "AI_OVERLAY_BUDGET_HIGH", BUDGET_UTIL_HIGH):
        tilt -= 0.12
    elif budget_util <= 0.30 and goodness > 0.55 and stress < 0.50 and manip < 0.50:
        tilt += 0.04

    # Plan-gap nudges (small and conditional)
    if (
        plan_gap <= PLAN_GAP_BEHIND_BAD
        and pnl_score > 0.55
        and dd_score > 0.55
        and reg > 0.55
        and stress < 0.60
        and budget_util <= _cfg_float(cfg, "AI_OVERLAY_BUDGET_WARN", BUDGET_UTIL_WARN)
    ):
        tilt += 0.06

    if plan_gap >= PLAN_GAP_AHEAD_HIGH and (
        dd_score < 0.55
        or pnl_score < 0.55
        or stress > 0.60
        or budget_util > _cfg_float(cfg, "AI_OVERLAY_BUDGET_HIGH", BUDGET_UTIL_HIGH)
    ):
        tilt -= 0.06

    # Streak bias
    tilt += streak_bias

    # Kelly confidence shrinks absolute tilt if low
    shrink = 0.50 + 0.50 * _clamp(k_conf, 0.0, 1.0)  # 0.5..1.0
    tilt *= shrink

    # optional anti-overconfidence knob
    overconf_pen = _cfg_float(cfg, "AI_OVERLAY_OVERCONF_PEN", 0.0)
    if overconf_pen > 0.0 and goodness > 0.75 and k_conf < 0.35:
        tilt -= _clamp(overconf_pen, 0.0, 0.12)

    # If manip is extreme, never positive tilt
    if manip >= 0.92:
        tilt = min(tilt, 0.0)

    return _clamp(tilt, AI_CAPACITY_MIN, AI_CAPACITY_MAX)


def _compute_regime_entry_gate_factor(
    risk_profile: str,
    pnl_dd: Mapping[str, float],
    regime: Mapping[str, float],
    budget_util: float,
    cfg: Mapping[str, Any] | None,
) -> float:
    reg = float(regime["regime_score"])
    stress = float(regime["regime_stress"])
    manip = float(regime["manipulation_risk"])

    pnl_score = float(pnl_dd["pnl_score"])
    dd_score = float(pnl_dd["dd_score"])

    gate = 0.60 + 0.80 * _clamp(reg, 0.0, 1.0)  # [0,1] -> [0.6, 1.4]

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

    if budget_util >= _cfg_float(cfg, "AI_OVERLAY_BUDGET_CRIT", BUDGET_UTIL_CRIT):
        gate *= 0.40
    elif budget_util >= _cfg_float(cfg, "AI_OVERLAY_BUDGET_HIGH", BUDGET_UTIL_HIGH):
        gate *= 0.70

    # If pnl/dd are ugly, cap gate
    if pnl_score < 0.30 or dd_score < 0.30:
        gate = min(gate, 0.85)

    return _clamp(gate, REGIME_GATE_MIN, REGIME_GATE_MAX)


def _compute_risk_mode_and_block(
    risk_profile: str,
    pnl_dd: Mapping[str, float],
    regime: Mapping[str, float],
    risk_snapshot: Mapping[str, Any],
    budget_util: float,
    cfg: Mapping[str, Any] | None,
) -> tuple[int, bool]:
    pnl_score = float(pnl_dd["pnl_score"])
    dd_pct = float(pnl_dd["dd_pct"])
    dd_score = float(pnl_dd["dd_score"])

    reg = float(regime["regime_score"])
    stress = float(regime["regime_stress"])
    manip = float(regime["manipulation_risk"])

    circuit_tripped = _truthy(risk_snapshot.get("circuit_breaker_tripped", False), default=False)
    hard_dd = dd_pct <= _cfg_float(cfg, "AI_OVERLAY_DD_HARD_CRASH", DD_HARD_CRASH)
    extreme_stress = stress >= _cfg_float(cfg, "AI_OVERLAY_STRESS_EXTREME", 0.95)
    extreme_manip = manip >= _cfg_float(cfg, "AI_OVERLAY_MANIP_EXTREME", 0.95) and reg < 0.50
    over_budget = budget_util >= _cfg_float(cfg, "AI_OVERLAY_BUDGET_CRIT", BUDGET_UTIL_CRIT)

    force_block = _truthy(risk_snapshot.get("force_block_new_entries", False), default=False)
    block = bool(
        force_block or circuit_tripped or hard_dd or extreme_stress or extreme_manip or over_budget
    )

    # Default
    risk_mode = 1

    # Risk-off conditions
    if (
        block
        or dd_pct <= _cfg_float(cfg, "AI_OVERLAY_DD_BAD", DD_BAD)
        or dd_score < 0.30
        or pnl_score < 0.30
        or reg < 0.30
        or stress > REGIME_STRESS_HIGH
        or manip > 0.85
        or risk_profile in ("ultra_defensive", "defensive")
        or budget_util >= _cfg_float(cfg, "AI_OVERLAY_BUDGET_HIGH", BUDGET_UTIL_HIGH)
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
            and budget_util <= _cfg_float(cfg, "AI_OVERLAY_BUDGET_WARN", BUDGET_UTIL_WARN)
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
    risk_snapshot: dict[str, Any],
    perf_snapshot: dict[str, Any] | None = None,
    regime_snapshot: dict[str, Any] | None = None,
    config: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Core overlay used by RiskManager. PURE.

    Missing fields fall back to neutral defaults.
    If internal exception occurs: returns SAFE risk-off fallback (block entries).
    """
    try:
        rs = dict(risk_snapshot or {})

        pnl_dd = _score_pnl_and_dd(rs)
        perf = _score_perf(perf_snapshot)
        regime = _score_regime(regime_snapshot, rs)
        budget_util = _extract_budget_utilization(rs)

        risk_profile = _choose_risk_profile(pnl_dd, perf, regime, budget_util, config)

        ai_capacity_delta = _compute_ai_capacity_delta(
            risk_snapshot=rs,
            pnl_dd=pnl_dd,
            perf=perf,
            regime=regime,
            risk_profile=risk_profile,
            budget_util=budget_util,
            cfg=config,
        )

        regime_gate = _compute_regime_entry_gate_factor(
            risk_profile=risk_profile,
            pnl_dd=pnl_dd,
            regime=regime,
            budget_util=budget_util,
            cfg=config,
        )

        risk_mode, block_new_entries = _compute_risk_mode_and_block(
            risk_profile=risk_profile,
            pnl_dd=pnl_dd,
            regime=regime,
            risk_snapshot=rs,
            budget_util=budget_util,
            cfg=config,
        )

        max_pos_mult = _compute_max_pos_mult(
            risk_profile=risk_profile,
            ai_capacity_delta=ai_capacity_delta,
            risk_mode=risk_mode,
        )

        out: dict[str, Any] = {
            "ai_capacity_delta": float(_clamp(ai_capacity_delta, AI_CAPACITY_MIN, AI_CAPACITY_MAX)),
            "risk_profile": str(risk_profile),
            "regime_entry_gate_factor": float(
                _clamp(regime_gate, REGIME_GATE_MIN, REGIME_GATE_MAX)
            ),
            "risk_mode": int(risk_mode),
            "risk_mode_label": "risk_off"
            if risk_mode == 0
            else ("risk_on" if risk_mode == 2 else "normal"),
            "block_new_entries": bool(block_new_entries),
            "recommend_max_pos_mult": float(
                _clamp(max_pos_mult, MAX_POS_MULT_MIN, MAX_POS_MULT_MAX)
            ),
            "ai_risk_model_version": AI_RISK_MODEL_VERSION,
            "debug": {
                "pnl_dd": pnl_dd,
                "perf": perf,
                "regime": regime,
                "budget_util": float(budget_util),
                "notes": [],
            },
        }

        ok, errs = validate_overlay(out)
        if not ok:
            dbg = dict(out.get("debug") or {})
            dbg["schema_errors"] = errs
            out["debug"] = dbg

        if _cfg_bool(config, "AI_OVERLAY_INCLUDE_EXPLAIN", True):
            out["debug"]["explain"] = explain_overlay(out)

        return out
    except Exception:
        return dict(_SAFE_OVERLAY_FALLBACK)


def explain_overlay(overlay: Mapping[str, Any]) -> str:
    """Human-readable summary (PURE). Safe even if overlay is partial."""
    try:
        profile = str(overlay.get("risk_profile", "balanced"))
        mode = int(overlay.get("risk_mode", 1))
        block = bool(overlay.get("block_new_entries", False))
        delta = float(overlay.get("ai_capacity_delta", 0.0))
        gate = float(overlay.get("regime_entry_gate_factor", 1.0))
        maxpos = float(overlay.get("recommend_max_pos_mult", 1.0))
        label = "risk_off" if mode == 0 else ("risk_on" if mode == 2 else "normal")
        return (
            f"profile={profile} mode={label} block={int(block)} "
            f"cap_delta={delta:+.2f} gate={gate:.2f} maxpos_mult={maxpos:.2f}"
        )
    except Exception:
        return "profile=unknown mode=unknown"


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


def validate_overlay(overlay: Mapping[str, Any] | None) -> tuple[bool, list[str]]:
    errs: list[str] = []
    if overlay is None:
        return False, ["overlay is None"]

    for k in OVERLAY_REQUIRED_KEYS:
        if k not in overlay:
            errs.append(f"missing:{k}")

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

    lbl = str(overlay.get("risk_mode_label", ""))
    if lbl not in ("risk_off", "normal", "risk_on"):
        errs.append("risk_mode_label invalid")

    rp = str(overlay.get("risk_profile", ""))
    if rp not in ("ultra_defensive", "defensive", "balanced", "aggressive", "degenerate"):
        errs.append("risk_profile invalid")

    dbg = overlay.get("debug", {})
    if not isinstance(dbg, dict):
        errs.append("debug not_dict")

    return (len(errs) == 0), errs


# =============================================================================
# Backward-compatible adapter used by risk_manager.current_limits()
# =============================================================================


def _build_risk_snapshot_from_features(features: dict[str, Any]) -> dict[str, Any]:
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
    eq = _first_present_float(
        features, ("equity_usd", "equityUSD", "equity", "eq_usd", "eqUSD", "eq"), 0.0
    )
    realized = _first_present_float(
        features,
        ("realized_pnl_usd", "realizedPnlUsd", "realized_pnl_today_usd", "realized_today_usd"),
        0.0,
    )
    dd_frac = _safe_float(
        features, "rolling_dd", _safe_float(features, "rolling_dd_frac", 0.0)
    )  # 0..1
    plan_gap = _safe_float(features, "plan_gap_pct", 0.0)
    base_capacity = _safe_float(features, "risk_capacity", 0.5)
    circuit_tripped = _truthy(features.get("circuit_breaker_tripped", False), default=False)

    daily_pnl_pct = (realized / eq * 100.0) if eq > 0 else 0.0
    daily_dd_pct = -abs(dd_frac) * 100.0

    regime_mult = _safe_float(features, "regime_mult", 1.0)
    regime_label_numeric = _safe_float(features, "regime_label_numeric", 0.0)
    regime_stress_score = _safe_float(features, "regime_stress_score", 0.0)
    regime_signal_strength = _safe_float(features, "regime_signal_strength", 0.0)
    manip = _safe_float(
        features, "manipulation_risk", _safe_float(features, "manipulationRisk", 0.0)
    )

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


def _build_perf_snapshot_from_features(features: dict[str, Any]) -> dict[str, Any] | None:
    if not features:
        return None

    wr = _safe_float(features, "winrate20", _safe_float(features, "win_rate", 0.0))
    rwr = _safe_float(features, "recent_winrate20", _safe_float(features, "recent_win_rate", wr))
    sharpe = _safe_float(features, "sharpe20", _safe_float(features, "sharpe_like", 0.0))
    rsharpe = _safe_float(
        features, "recent_sharpe20", _safe_float(features, "recent_sharpe_like", sharpe)
    )
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


def overlay_from_features(
    features: dict[str, Any], config: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    """Pure convenience wrapper used by some stacks."""
    rs = _build_risk_snapshot_from_features(features or {})
    ps = _build_perf_snapshot_from_features(features or {})
    return compute_ai_overlay(rs, perf_snapshot=ps, regime_snapshot=None, config=config)


def predict_capacity_adjustment(features: dict[str, Any]) -> float:
    """
    Compatibility function.

    Returns only ai_capacity_delta in [-0.6, +0.6].
    Safe fallback: returns AI_CAPACITY_MIN on internal exception (risk-off bias).
    """
    try:
        overlay = overlay_from_features(features or {}, config=None)
        delta = float(overlay.get("ai_capacity_delta", 0.0))
        return float(_clamp(delta, AI_CAPACITY_MIN, AI_CAPACITY_MAX))
    except Exception:
        return float(AI_CAPACITY_MIN)


# =============================================================================
# Candidate-level risk scoring (per-token)
# =============================================================================


def _candidate_components(
    features: Mapping[str, Any],
    cfg: Mapping[str, Any] | None,
) -> tuple[dict[str, float], dict[str, str], dict[str, Any], bool]:
    """
    Build component risks in 0..1 plus short 'why' and raw extracted values.
    Returns (components, why_short, raw, hard_flag).
    """
    comp: dict[str, float] = {}
    why: dict[str, str] = {}
    raw: dict[str, Any] = {}

    # ---- age (minutes): missing => medium risk (not "safe")
    age_min = _first_present_float(
        features, ("age_min", "ageMinutes", "pool_age_min", "token_age_min"), 45.0
    )
    raw["age_min"] = age_min
    age_safe = _cfg_float(cfg, "AI_RISK_AGE_SAFE_MIN", 180.0)
    age_risk_min = _cfg_float(cfg, "AI_RISK_AGE_RISK_MIN", 3.0)
    age_risk = _map_linear(age_min, age_safe, age_risk_min)
    comp["age_risk"] = _clamp(age_risk, 0.0, 1.0)
    why["age_risk"] = f"age={age_min:.1f}m"

    # ---- liquidity USD: missing => medium risk
    liq = _first_present_float(features, ("liq_usd", "liquidity_usd", "liquidity", "liq"), 25_000.0)
    raw["liq_usd"] = liq
    liq_safe = _cfg_float(cfg, "AI_RISK_LIQ_SAFE", 50_000.0)
    liq_risk_thr = _cfg_float(cfg, "AI_RISK_LIQ_RISK", 800.0)
    liq_risk = _map_linear(liq, liq_safe, liq_risk_thr)
    comp["liq_risk"] = _clamp(liq_risk, 0.0, 1.0)
    why["liq_risk"] = f"liq=${liq:,.0f}"

    # ---- 5m volume: missing/0 => conservative (not safe)
    vol_5m = _first_present_float(features, ("vol_5m_usd", "volume_5m_usd", "vol5", "vol_5m"), 0.0)
    raw["vol_5m_usd"] = vol_5m
    if vol_5m <= 0.0:
        vol_risk = 0.60
    else:
        vol_safe = _cfg_float(cfg, "AI_RISK_VOL5_SAFE", 15_000.0)
        vol_risk_thr = _cfg_float(cfg, "AI_RISK_VOL5_RISK", 500.0)
        vol_risk = _map_linear(vol_5m, vol_safe, vol_risk_thr)
    comp["volume_risk"] = _clamp(vol_risk, 0.0, 1.0)
    why["volume_risk"] = f"vol5m≈${vol_5m:,.0f}"

    # ---- holder concentration: missing => medium risk
    top1 = _first_present_float(features, ("top_holder_pct", "top1_holder_pct", "top1_pct"), 0.10)
    top10 = _first_present_float(
        features, ("top10_holder_pct", "top10_pct", "holder_concentration"), 0.45
    )
    if top1 > 1.0:
        top1 /= 100.0
    if top10 > 1.0:
        top10 /= 100.0
    raw["top1_pct"] = top1
    raw["top10_pct"] = top10
    conc = _clamp(0.65 * _clamp(top1 * 2.0, 0.0, 1.0) + 0.35 * _clamp(top10, 0.0, 1.0), 0.0, 1.0)
    comp["conc_risk"] = conc
    why["conc_risk"] = f"top1≈{top1 * 100:.0f}% top10≈{top10 * 100:.0f}%"

    # ---- LP lock/burn: missing => medium
    lp_locked = _first_present_float(
        features, ("lp_locked_pct", "lpLockPct", "lp_locked", "lp_lock"), 0.0
    )
    if lp_locked > 1.0:
        lp_locked /= 100.0
    raw["lp_locked_pct"] = lp_locked
    # higher lock => lower risk
    lp_r = _map_linear(lp_locked, 0.80, 0.05)
    comp["lp_lock_risk"] = _clamp(lp_r, 0.0, 1.0)
    why["lp_lock_risk"] = f"lp_lock≈{lp_locked * 100:.0f}%"

    # ---- sellability / honeypot / tax / transfer-hook flags
    hard_flag = False
    hard_keys = (
        "honeypot_flag",
        "is_honeypot",
        "rugpull_flag",
        "is_rug",
        "is_scam",
        "blacklist_flag",
        "transfer_hook_flag",
        "blocked_sells",
    )
    for k in hard_keys:
        if _truthy(features.get(k, False), default=False):
            hard_flag = True
            raw["hard_flag_key"] = k
            break

    if ("can_sell" in features) and (features.get("can_sell") is False):
        hard_flag = True
        raw["hard_flag_key"] = "can_sell=False"

    # soft score inputs (0..1 or 0..100)
    soft = _first_present_float(
        features,
        (
            "honeypot_score",
            "honeypot_prob",
            "rug_score",
            "fraud_score",
            "ai_risk_score",
            "security_risk_score",
        ),
        0.40,  # missing => NOT safe
    )
    soft01 = _as_frac_01(soft)
    raw["honeypot_soft"] = soft

    # tax/fee bps or pct
    buy_tax = _first_present_float(features, ("buy_tax_pct", "buyTaxPct", "buy_tax", "buyTax"), 0.0)
    sell_tax = _first_present_float(
        features, ("sell_tax_pct", "sellTaxPct", "sell_tax", "sellTax"), 0.0
    )
    buy_tax_f = _pct_to_frac(buy_tax)
    sell_tax_f = _pct_to_frac(sell_tax)
    raw["buy_tax_frac"] = buy_tax_f
    raw["sell_tax_frac"] = sell_tax_f
    tax_r = (
        _clamp((buy_tax_f + sell_tax_f) * 1.8, 0.0, 1.0) if (buy_tax_f + sell_tax_f) > 0 else 0.25
    )

    if hard_flag:
        comp["sellability_risk"] = 1.0
        why["sellability_risk"] = "flagged"
    else:
        comp["sellability_risk"] = _clamp(0.55 * soft01 + 0.45 * tax_r, 0.0, 1.0)
        why["sellability_risk"] = f"soft={soft01:.2f} tax≈{(buy_tax_f + sell_tax_f) * 100:.1f}%"

    # ---- mint/freeze authority: unknown => medium
    mint_auth_disabled = features.get("mint_authority_disabled")
    freeze_auth_disabled = features.get("freeze_authority_disabled")
    raw["mint_authority_disabled"] = mint_auth_disabled
    raw["freeze_authority_disabled"] = freeze_auth_disabled
    mint_r = 0.0 if mint_auth_disabled is True else (0.85 if mint_auth_disabled is False else 0.50)
    freeze_r = (
        0.0 if freeze_auth_disabled is True else (0.85 if freeze_auth_disabled is False else 0.50)
    )
    comp["authority_risk"] = _clamp(0.50 * mint_r + 0.50 * freeze_r, 0.0, 1.0)
    why["authority_risk"] = f"mint={mint_auth_disabled} freeze={freeze_auth_disabled}"

    # ---- price impact / slippage proxy (bps): missing => medium
    impact_bps = _first_present_float(
        features, ("impact_1k_bps", "priceImpact1kBps", "price_impact_bps"), 450.0
    )
    raw["impact_1k_bps"] = impact_bps
    impact_risk_bps = _cfg_float(cfg, "AI_RISK_IMPACT_RISK_BPS", 1200.0)
    impact_safe_bps = _cfg_float(cfg, "AI_RISK_IMPACT_SAFE_BPS", 200.0)
    impact_risk = _map_linear(impact_bps, impact_risk_bps, impact_safe_bps)
    comp["impact_risk"] = _clamp(impact_risk, 0.0, 1.0)
    why["impact_risk"] = f"impact≈{impact_bps:.0f}bps"

    # ---- deployer behavior (optional): missing => medium
    dev_sell = _first_present_float(
        features, ("dev_sell_5m_usd", "deployer_sell_5m_usd", "creator_sell_5m_usd"), 0.0
    )
    dev_hold = _first_present_float(
        features, ("dev_hold_pct", "deployer_hold_pct", "creator_hold_pct"), 0.0
    )
    if dev_hold > 1.0:
        dev_hold /= 100.0
    raw["dev_sell_5m_usd"] = dev_sell
    raw["dev_hold_frac"] = dev_hold
    dev_sell_r = _clamp(math.log10(dev_sell + 1.0) / 3.0, 0.0, 1.0)
    dev_hold_r = _clamp(dev_hold * 1.7, 0.0, 1.0)
    comp["deployer_risk"] = _clamp(0.55 * dev_sell_r + 0.45 * dev_hold_r, 0.0, 1.0)
    why["deployer_risk"] = f"dev_sell5m=${dev_sell:.0f} dev_hold≈{dev_hold * 100:.0f}%"

    # ---- micro-manip proxy from buys/sells
    buys = _first_present_float(features, ("buys_5m", "buys5", "buys_1m", "buys"), 0.0)
    sells = _first_present_float(features, ("sells_5m", "sells5", "sells_1m", "sells"), 0.0)
    raw["buys"] = buys
    raw["sells"] = sells
    total = buys + sells
    buy_ratio = (buys / total) if total > 0 else 0.5

    manip_r = 0.50 if total == 0 else 0.0  # no prints => NOT safe
    if total >= 6 and buy_ratio > 0.92 and sells <= 1:
        manip_r = 0.82
    elif total >= 10 and buy_ratio > 0.88 and sells <= 2:
        manip_r = 0.62
    elif total > 0 and buy_ratio < 0.25:
        manip_r = 0.55

    comp["manip_risk"] = _clamp(manip_r, 0.0, 1.0)
    why["manip_risk"] = f"buys/sells={int(buys)}/{int(sells)} br={buy_ratio:.2f}"

    # ---- whale / smart money concentration (optional): missing => medium
    whale_hold = _first_present_float(
        features, ("whale_hold_pct", "whale_pct", "smart_money_hold_pct"), 0.0
    )
    if whale_hold > 1.0:
        whale_hold /= 100.0
    raw["whale_hold_frac"] = whale_hold
    whale_risk = _clamp(0.35 + whale_hold * 0.90, 0.0, 1.0) if whale_hold > 0.0 else 0.45
    comp["whale_risk"] = whale_risk
    why["whale_risk"] = f"whale_hold≈{whale_hold * 100:.0f}%" if whale_hold > 0.0 else "unknown"

    # ---- short-term trend / blow-off / dump risk (5m change)
    chg5 = _first_present_float(
        features, ("chg5", "change_5m_pct", "price_change_5m_pct", "chg5_pct"), 0.0
    )
    raw["chg5_pct"] = chg5
    if chg5 >= 0:
        trend_risk = _map_linear(chg5, 80.0, 10.0)  # huge pumps are riskier
    else:
        trend_risk = _map_linear(chg5, -5.0, -35.0)  # dumps are riskier
    comp["trend_risk"] = _clamp(trend_risk, 0.0, 1.0)
    why["trend_risk"] = f"chg5m={chg5:.1f}%"

    # ---- volatility proxy (optional): missing => medium
    volat = _first_present_float(
        features, ("volatility_1m", "volatility", "stdev_1m", "ret_stdev_1m"), 0.0
    )
    raw["volatility_1m"] = volat
    vol_r = 0.50 if volat <= 0 else _clamp(math.log10(volat + 1e-9) / 2.0 + 0.50, 0.0, 1.0)
    comp["volatility_risk"] = _clamp(vol_r, 0.0, 1.0)
    why["volatility_risk"] = f"vol≈{volat:.4f}"

    # ---- route viability (optional): missing => mild penalty (unknown)
    route_ok = features.get("route_ok")
    route_conf = _first_present_float(features, ("route_conf", "route_confidence"), 0.0)
    raw["route_ok"] = route_ok
    raw["route_conf"] = route_conf

    if route_ok is False:
        comp["route_risk"] = 0.95
        why["route_risk"] = "route_ok=False"
    else:
        rc = _as_frac_01(route_conf) if route_conf else 0.0
        comp["route_risk"] = _clamp(0.45 - 0.35 * rc, 0.0, 1.0) if rc > 0 else 0.45
        why["route_risk"] = f"conf={rc:.2f}" if rc > 0 else "unknown"

    return comp, why, raw, hard_flag


def score_candidate(
    features: dict[str, Any], config: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    """
    Candidate-level risk scoring (PURE).

    Optional config supports:
      - AI_RISK_W_* weights (AI_RISK_W_AGE, AI_RISK_W_LIQ, ...)
      - AI_RISK_HEUR_WEIGHT (blend with precomputed ai_risk_score/fraud_score)
      - AI_RISK_MIN_CLIP / AI_RISK_MAX_CLIP
      - AI_RISK_LABEL_SAFE_MAX, AI_RISK_LABEL_OK_MAX, AI_RISK_LABEL_ELEV_MAX
      - AI_RISK_HARDBLOCK_SCORE (default 0.90)
      - AI_RISK_REQUIRE_SELLABLE (default True)
      - AI_RISK_REQUIRE_ROUTE_OK (default False)
    """
    try:
        feats = dict(features or {})
        comp, why, raw, hard_flag = _candidate_components(feats, config)

        # Default weights (sum ~1; sellability + conc + liq dominate)
        w = {
            "age_risk": 0.07,
            "liq_risk": 0.14,
            "volume_risk": 0.06,
            "conc_risk": 0.14,
            "lp_lock_risk": 0.06,
            "sellability_risk": 0.22,
            "authority_risk": 0.06,
            "impact_risk": 0.08,
            "deployer_risk": 0.06,
            "manip_risk": 0.05,
            "whale_risk": 0.04,
            "trend_risk": 0.03,
            "volatility_risk": 0.02,
            "route_risk": 0.07,
        }

        # Optional override weights via config
        if config:
            for k in list(w.keys()):
                base = k.replace("_risk", "").upper()
                ck = f"AI_RISK_W_{base}"
                if ck in config:
                    with contextlib.suppress(Exception):
                        w[k] = float(config[ck])

            s = sum(max(0.0, float(v)) for v in w.values())
            if s > 0:
                for k in w:
                    w[k] = max(0.0, float(w[k])) / s

        heur = 0.0
        for k, wk in w.items():
            heur += float(wk) * float(_clamp(comp.get(k, 0.0), 0.0, 1.0))
        heur = _clamp(heur, 0.0, 1.0)

        # Optional blend with a precomputed score (if present)
        pre: float | None = None
        for key in ("ai_risk_score", "fraud_score", "rug_score", "security_risk_score"):
            if key in feats and feats[key] is not None:
                try:
                    pre = _as_frac_01(float(feats[key]))
                    raw["precomputed_key"] = key
                    raw["precomputed_val"] = feats[key]
                    break
                except Exception:
                    continue

        heur_weight = _cfg_float(config, "AI_RISK_HEUR_WEIGHT", 0.80)
        heur_weight = _clamp(heur_weight, 0.0, 1.0)
        score = (
            heur
            if pre is None
            else _clamp(heur_weight * heur + (1.0 - heur_weight) * pre, 0.0, 1.0)
        )

        # Confidence: count of meaningful signals present
        sig = 0
        signal_keys = (
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
            "lp_locked_pct",
            "vol_5m_usd",
            "buy_tax_pct",
            "sell_tax_pct",
            "route_ok",
            "route_conf",
        )
        for k in signal_keys:
            if k in feats and feats[k] is not None:
                sig += 1
        conf = _clamp(0.22 + 0.08 * sig, 0.22, 0.92)

        # Optional clip
        mn = _cfg_float(config, "AI_RISK_MIN_CLIP", 0.0)
        mx = _cfg_float(config, "AI_RISK_MAX_CLIP", 1.0)
        if mx < mn:
            mx = mn
        score = _clamp(score, mn, mx)

        # Label thresholds (configurable)
        safe_max = _cfg_float(config, "AI_RISK_LABEL_SAFE_MAX", 0.25)
        ok_max = _cfg_float(config, "AI_RISK_LABEL_OK_MAX", 0.45)
        elev_max = _cfg_float(config, "AI_RISK_LABEL_ELEV_MAX", 0.70)

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

        hardblock_score = _cfg_float(config, "AI_RISK_HARDBLOCK_SCORE", 0.90)

        # Hard block rules (never permissive if flagged)
        hard_block = bool(hard_flag or (score >= hardblock_score and conf >= 0.60))

        # Optional: require sellability if provided
        if _cfg_bool(config, "AI_RISK_REQUIRE_SELLABLE", True):
            if ("can_sell" in feats) and (feats.get("can_sell") is False):
                hard_block = True

        # Optional: require route_ok if provided
        if _cfg_bool(config, "AI_RISK_REQUIRE_ROUTE_OK", False):
            if ("route_ok" in feats) and (feats.get("route_ok") is False):
                hard_block = True

        reasons = [
            f"score={score:.2f} ({label}) conf={conf:.2f}",
            f"age: {why.get('age_risk', '')}",
            f"liq: {why.get('liq_risk', '')}",
            f"vol: {why.get('volume_risk', '')}",
            f"conc: {why.get('conc_risk', '')}",
            f"lp: {why.get('lp_lock_risk', '')}",
            f"sell: {why.get('sellability_risk', '')}",
            f"auth: {why.get('authority_risk', '')}",
            f"impact: {why.get('impact_risk', '')}",
            f"dev: {why.get('deployer_risk', '')}",
            f"whale: {why.get('whale_risk', '')}",
            f"manip: {why.get('manip_risk', '')}",
            f"trend: {why.get('trend_risk', '')}",
            f"volat: {why.get('volatility_risk', '')}",
            f"route: {why.get('route_risk', '')}",
        ]
        if hard_block:
            reasons.insert(1, "HARD_BLOCK=TRUE")

        out: dict[str, Any] = {
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


def explain_candidate(candidate_out: Mapping[str, Any]) -> str:
    """Compact explain string for logs/alerts (PURE)."""
    try:
        s = float(candidate_out.get("score", 1.0))
        c = float(candidate_out.get("conf", 0.0))
        lbl = str(candidate_out.get("label", "critical"))
        hb = bool(candidate_out.get("hard_block", False))
        return f"risk={s:.2f} conf={c:.2f} label={lbl} hard_block={int(hb)}"
    except Exception:
        return "risk=unknown"


def candidate_gate(
    candidate_risk: Mapping[str, Any],
    overlay: Mapping[str, Any] | None = None,
    config: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """
    PURE helper that turns:
      - candidate risk dict (from score_candidate)
      - optional overlay dict (from compute_ai_overlay)
    into a single gating decision.

    Returns:
      { ok: bool, hard_block: bool, threshold: float, effective_score: float, notes: [...] }
    """
    try:
        score = float(candidate_risk.get("score", 1.0))
        hard = bool(candidate_risk.get("hard_block", False))

        # Base threshold: max acceptable risk score to allow entry
        base_thr = _cfg_float(config, "AI_GATE_MAX_RISK_OK", 0.55)
        base_thr = _clamp(base_thr, 0.10, 0.95)

        if overlay:
            mode = int(overlay.get("risk_mode", 1))
            prof = str(overlay.get("risk_profile", "balanced"))
            gate_factor = float(overlay.get("regime_entry_gate_factor", 1.0))

            # Tighten in risk_off / defensive
            if mode == 0 or prof in ("ultra_defensive", "defensive"):
                base_thr *= 0.78
            # Slightly loosen in risk_on / aggressive
            elif mode == 2 and prof in ("aggressive", "degenerate"):
                base_thr *= 1.05

            # gate_factor < 1 => tighten, >1 => loosen slightly (bounded)
            base_thr *= _clamp(gate_factor, 0.60, 1.20)

        base_thr = _clamp(base_thr, 0.08, 0.90)
        ok = (not hard) and (score <= base_thr)

        return {
            "ok": bool(ok),
            "hard_block": bool(hard),
            "threshold": float(base_thr),
            "effective_score": float(score),
            "notes": [],
        }
    except Exception:
        return {
            "ok": False,
            "hard_block": True,
            "threshold": 0.0,
            "effective_score": 1.0,
            "notes": ["exception_fallback"],
        }


# =============================================================================
# Aliases / adapters (candidate-level)
# =============================================================================


def score_entry(
    features: dict[str, Any], config: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    return score_candidate(features, config)


def score(features: dict[str, Any], config: Mapping[str, Any] | None = None) -> dict[str, Any]:
    return score_candidate(features, config)


def evaluate(features: dict[str, Any], config: Mapping[str, Any] | None = None) -> dict[str, Any]:
    return score_candidate(features, config)


def score_risk(
    features: dict[str, Any],
    meta: dict[str, Any] | None = None,
    config: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
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
    features: dict[str, Any],
    meta: dict[str, Any] | None = None,
    config: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return score_risk(features, meta=meta, config=config)


# =============================================================================
# Strict schema validators
# =============================================================================


def validate_candidate_score(out: Mapping[str, Any] | None) -> tuple[bool, list[str]]:
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

    hb = out.get("hard_block")
    if not isinstance(hb, bool):
        errs.append("hard_block not_bool")

    reasons = out.get("reasons", [])
    if not isinstance(reasons, list):
        errs.append("reasons not_list")
    else:
        # Keep this lightweight; just ensure string-ish
        for i, r in enumerate(reasons[:50]):
            if not isinstance(r, str):
                errs.append(f"reasons[{i}] not_str")
                break

    comp = out.get("components", {})
    if not isinstance(comp, dict):
        errs.append("components not_dict")
    else:
        for k, v in list(comp.items())[:50]:
            if not isinstance(k, str):
                errs.append("components key not_str")
                break
            try:
                fv = float(v)
                if not (0.0 <= fv <= 1.0):
                    errs.append(f"components[{k}] out_of_range")
            except Exception:
                errs.append(f"components[{k}] not_float")

    raw = out.get("raw", {})
    if not isinstance(raw, dict):
        errs.append("raw not_dict")

    ver = out.get("version")
    if not isinstance(ver, str) or not ver:
        errs.append("version invalid")

    return (len(errs) == 0), errs


# =============================================================================
# Local smoke
# =============================================================================

if __name__ == "__main__":  # pragma: no cover
    # quick sanity for manual runs (no IO)
    rs = {
        "equity_usd": 1000.0,
        "daily_realized_pnl_pct": 4.0,
        "daily_drawdown_pct": -3.5,
        "regime_label_numeric": 0.3,
        "regime_stress_score": 0.25,
        "regime_signal_strength": 0.55,
        "manipulation_risk": 0.15,
        "risk_budget_util": 0.20,
    }
    ov = compute_ai_overlay(rs)
    print("overlay:", ov)

    feats = {
        "age_min": 12.0,
        "liq_usd": 12_000.0,
        "vol_5m_usd": 4_000.0,
        "top_holder_pct": 0.12,
        "top10_holder_pct": 0.48,
        "lp_locked_pct": 0.35,
        "honeypot_score": 0.25,
        "buy_tax_pct": 1.0,
        "sell_tax_pct": 1.0,
        "impact_1k_bps": 650,
        "chg5": 22.0,
        "route_ok": True,
        "route_conf": 0.65,
    }
    cr = score_candidate(feats)
    print("candidate:", cr)
    print("gate:", candidate_gate(cr, ov))
