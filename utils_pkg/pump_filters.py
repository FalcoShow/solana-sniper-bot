# utils_pkg/pump_filters.py
from __future__ import annotations

"""
Pump Filters — V3+++ (quant-grade, Phase 0–7) — LOCK-IN
======================================================

Hard compatibility contract (DO NOT BREAK):
- PumpStats
- PumpDecision
- check_pump_candidate(stats: PumpStats) -> PumpDecision

Design rules
------------
- NO network calls in check_pump_candidate()
- Pure math + env tuning only
- Always deterministic, never crash callers (fails closed by default)

What this module does
---------------------
Given a PumpStats snapshot (typically 60s window), we:
1) Apply phase-aware sanity / hard guards (optional strictness)
2) Compute:
   - velocity_score (activity/flow minus wash/idle penalties)
   - smart_wallet_score (legacy good/bad + optional enriched smart features)
   - composite score 0..100 (quality - risk) when enabled
3) Decide ok / reason, with optional debug details

Notes
-----
- Composite gate is recommended from Phase>=4 (default).
- This module is intentionally "not too strict" to avoid the classic “no buys” problem:
  it uses risk penalties rather than hard rejects whenever safe to do so.

Env knobs (high-signal)
-----------------------
Phase:
  PUMP_FILTER_PHASE or MASTER_PHASE

Mode:
  PUMP_USE_COMPOSITE_GATE=1/0
  PUMP_STRICT_HARD_GUARDS=1/0
  PUMP_FILTER_FAIL_OPEN=1/0 (default 0)

Gate:
  PUMP_GATE_SCORE (0..100)
  PUMP_REQUIRE_MIN_VEL=1/0

Stage window:
  PUMP_MIN_STAGE, PUMP_MAX_STAGE, PUMP_STAGE_SWEET_SPOT, PUMP_STAGE_SWEET_WIDTH

Liquidity/FDV:
  PUMP_MIN_LIQ_USD, PUMP_MIN_FDV_USD, PUMP_MAX_FDV_USD (0 disables), PUMP_MIN_LIQ_RATIO

Activity:
  PUMP_MIN_UNIQUE_BUYERS, PUMP_MIN_BUYS_60S, PUMP_MIN_SOL_60S, PUMP_MAX_IDLE_SEC, PUMP_STALENESS_SEC

Wallet mix:
  PUMP_GOOD_WALLET_WEIGHT, PUMP_BAD_WALLET_WEIGHT, PUMP_MIN_SMART_WALLET_SCORE

Velocity:
  PUMP_MIN_VELOCITY_SCORE, PUMP_MIN_VELOCITY_FLOOR

Wash/Idle:
  PUMP_IDLE_FREE_SEC, PUMP_IDLE_RAMP_SEC
  PUMP_WASH_BUYS_PER_BUYER_START, PUMP_WASH_BUYS_PER_BUYER_RAMP
  PUMP_WASH_MIN_SOL_PER_BUY

Debug:
  PUMP_FILTER_RETURN_DETAILS=1/0
  PUMP_FILTER_DETAILS_MAXLEN (default 1400)
  PUMP_FILTER_CFG_TTL_SEC (default 2.0)

Async enrich (optional, in-process ONLY):
  PUMP_FILTER_ALLOW_ASYNC_ENRICH=1/0
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple
import math
import os
import time


# -----------------------------------------------------------------------------
# Backwards-compatible data models
# -----------------------------------------------------------------------------
@dataclass
class PumpStats:
    # Legacy positional args (keep order)
    stage: float
    liq_usd: float
    fdv_usd: float
    buyers_60s: int
    buys_60s: int
    sol_60s: float
    idle_sec: float
    good_wallets: int
    bad_wallets: int

    # Optional enrichers (safe defaults)
    mint: str = ""
    symbol: str = ""
    ts: float = 0.0  # unix time of snapshot

    # Optional smart-wallet / whale / regime hints (safe defaults)
    # If you attach these upstream (smart_wallets tracker, etc.), we blend them.
    smart_entry_boost_hint: float = 0.0      # -1..+1
    smart_signal_confidence: float = 0.0     # 0..1
    smart_signal_quality: float = 0.0        # 0..1
    smart_risk_flag: float = 0.0             # 0..1
    smart_exit_pressure_hint: float = 0.0    # 0..1

    smart_vs_whale_conflict: float = 0.0     # 0..1
    whale_domination_5m: float = 0.0         # 0..1
    whale_share_5m: float = 0.0              # 0..1

    smart_heat_score: float = 0.0            # 0..1
    whale_heat_score: float = 0.0            # 0..1
    smart_tradeable_flag: float = 0.0        # 0/1


@dataclass
class PumpDecision:
    # Legacy positional args (keep order)
    ok: bool
    reason: str
    velocity_score: float
    smart_wallet_score: float

    # Optional extras
    score: float = 0.0            # composite score (0..100)
    risk_penalty: float = 0.0     # 0..100-ish
    quality: float = 0.0          # 0..100-ish
    details: Dict[str, Any] = field(default_factory=dict)


# -----------------------------------------------------------------------------
# Safe helpers
# -----------------------------------------------------------------------------
_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}


def _clamp(x: float, lo: float, hi: float) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _safe_f(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else float(default)
    except Exception:
        return float(default)


def _safe_i(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return int(default)


def _sigmoid(x: float) -> float:
    x = _clamp(float(x), -30.0, 30.0)
    return 1.0 / (1.0 + math.exp(-x))


def _envs(name: str, default: str = "") -> str:
    try:
        v = os.getenv(name)
        return (v if v is not None else default).strip()
    except Exception:
        return str(default).strip()


def _env_bool(name: str, default: bool) -> bool:
    v = _envs(name, "")
    if v == "":
        return bool(default)
    s = v.lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return bool(default)


def _env_int(name: str, default: int) -> int:
    v = _envs(name, "")
    if v == "":
        return int(default)
    try:
        return int(float(v))
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    v = _envs(name, "")
    if v == "":
        return float(default)
    try:
        return float(v)
    except Exception:
        return float(default)


# -----------------------------------------------------------------------------
# Phase
# -----------------------------------------------------------------------------
def _phase() -> int:
    p = _env_int("PUMP_FILTER_PHASE", _env_int("MASTER_PHASE", 7))
    return max(0, min(7, int(p)))


# -----------------------------------------------------------------------------
# Config snapshot (fast path)
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class _Cfg:
    phase: int

    # safety
    fail_open: bool
    return_details: bool
    details_maxlen: int

    # gating mode
    use_composite_gate: bool
    strict_hard_guards: bool

    # stage window
    min_stage: float
    max_stage: float
    stage_sweet_spot: float
    stage_sweet_width: float

    # liq/fdv guards
    min_liq_usd: float
    min_fdv_usd: float
    max_fdv_usd: float
    min_liq_ratio: float

    # activity guards
    min_unique_buyers: int
    min_buys_60s: int
    min_sol_60s: float
    max_idle_sec: float
    staleness_sec: float

    # legacy wallets
    good_wallet_weight: float
    bad_wallet_weight: float
    min_smart_wallet_score: float

    # velocity floors
    min_velocity_score: float
    min_velocity_floor: float

    # composite gate
    gate_score: float
    require_min_vel: bool
    allow_smart_override: bool

    # composite scaling
    buyers_scale: float
    buys_scale: float
    sol_scale: float

    # wash/idle tuning
    idle_free_sec: float
    idle_ramp_sec: float
    wash_buys_per_buyer_start: float
    wash_buys_per_buyer_ramp: float
    wash_min_sol_per_buy: float

    # risk shaping
    risk_weight_early: float
    risk_weight_mid: float
    risk_weight_late: float

    # how much “risk” can eat into score
    risk_to_score_weight: float


_CFG_CACHE: Optional[_Cfg] = None
_CFG_CACHE_AT: float = 0.0


def _get_cfg() -> _Cfg:
    global _CFG_CACHE, _CFG_CACHE_AT
    ttl = max(0.0, _env_float("PUMP_FILTER_CFG_TTL_SEC", 2.0))
    now = time.time()
    if _CFG_CACHE is not None and (now - _CFG_CACHE_AT) <= ttl:
        return _CFG_CACHE

    ph = _phase()

    # defaults (phase-shaped but user-tunable)
    use_comp = _env_bool("PUMP_USE_COMPOSITE_GATE", ph >= 4)
    strict_hard = _env_bool("PUMP_STRICT_HARD_GUARDS", True)

    fail_open = _env_bool("PUMP_FILTER_FAIL_OPEN", False)
    return_details = _env_bool("PUMP_FILTER_RETURN_DETAILS", False)
    details_maxlen = max(200, _env_int("PUMP_FILTER_DETAILS_MAXLEN", 1400))

    # stage
    min_stage = _env_float("PUMP_MIN_STAGE", 0.15)
    max_stage = _env_float("PUMP_MAX_STAGE", 0.75)
    if ph <= 2:
        min_stage = max(min_stage, 0.18)
        max_stage = min(max_stage, 0.72)
    elif ph >= 6:
        min_stage = max(0.10, min_stage - 0.03)
        max_stage = min(0.82, max_stage + 0.03)

    sweet = _env_float("PUMP_STAGE_SWEET_SPOT", 0.42)
    width = _env_float("PUMP_STAGE_SWEET_WIDTH", 0.30)

    # liquidity/fdv
    min_liq = _env_float("PUMP_MIN_LIQ_USD", 1500.0)
    min_fdv = _env_float("PUMP_MIN_FDV_USD", 5000.0)
    max_fdv = _env_float("PUMP_MAX_FDV_USD", 0.0)  # 0 disables
    min_ratio = _env_float("PUMP_MIN_LIQ_RATIO", 0.04)

    # activity
    min_buyers = _env_int("PUMP_MIN_UNIQUE_BUYERS", 5)
    min_buys = _env_int("PUMP_MIN_BUYS_60S", 8)
    min_sol = _env_float("PUMP_MIN_SOL_60S", 0.8)
    if ph <= 2:
        min_buyers = max(min_buyers, 6)
        min_buys = max(min_buys, 9)
        min_sol = max(min_sol, 0.9)
    elif ph >= 6:
        min_buyers = max(3, min_buyers - 1)
        min_buys = max(6, min_buys - 1)
        min_sol = max(0.5, min_sol - 0.15)

    max_idle = _env_float("PUMP_MAX_IDLE_SEC", 90.0)
    if ph <= 2:
        max_idle = min(max_idle, 80.0)
    elif ph >= 6:
        max_idle = max(max_idle, 105.0)

    staleness = _env_float("PUMP_STALENESS_SEC", 12.0)

    # wallet mix
    gw_w = _env_float("PUMP_GOOD_WALLET_WEIGHT", 2.0)
    bw_w = _env_float("PUMP_BAD_WALLET_WEIGHT", -3.0)
    min_sw = _env_float("PUMP_MIN_SMART_WALLET_SCORE", -2.0)
    if ph <= 2:
        min_sw = max(min_sw, -1.0)
    elif ph >= 6:
        min_sw = min(min_sw, -3.0)

    # velocity
    min_vel = _env_float("PUMP_MIN_VELOCITY_SCORE", 6.0)
    if ph <= 2:
        min_vel = max(min_vel, 6.8)
    elif ph >= 6:
        min_vel = min(min_vel, 5.6)

    vel_floor = _env_float("PUMP_MIN_VELOCITY_FLOOR", 4.8)

    # composite gate
    gate = _env_float("PUMP_GATE_SCORE", 22.0)
    if ph <= 4:
        gate = max(gate, 24.0)
    elif ph >= 6:
        gate = min(gate, 20.0)

    req_min_vel = _env_bool("PUMP_REQUIRE_MIN_VEL", False)
    allow_override = _env_bool("PUMP_ALLOW_SMART_OVERRIDE", True)

    buyers_scale = max(6.0, _env_float("PUMP_BUYERS_SCALE", 18.0))
    buys_scale = max(8.0, _env_float("PUMP_BUYS_SCALE", 28.0))
    sol_scale = max(1.0, _env_float("PUMP_SOL_SCALE", 4.5))

    # wash/idle tuning
    idle_free = _env_float("PUMP_IDLE_FREE_SEC", 25.0)
    idle_ramp = _env_float("PUMP_IDLE_RAMP_SEC", 45.0)

    wash_bpb_start = _env_float("PUMP_WASH_BUYS_PER_BUYER_START", 2.5)
    wash_bpb_ramp = _env_float("PUMP_WASH_BUYS_PER_BUYER_RAMP", 6.0)
    wash_min_spb = _env_float("PUMP_WASH_MIN_SOL_PER_BUY", 0.025)

    # risk weights by phase
    risk_w_early = _env_float("PUMP_RISK_WEIGHT_EARLY", 1.15)
    risk_w_mid = _env_float("PUMP_RISK_WEIGHT_MID", 1.00)
    risk_w_late = _env_float("PUMP_RISK_WEIGHT_LATE", 0.92)

    # risk → score weight
    risk_to_score_w = _env_float("PUMP_RISK_TO_SCORE_WEIGHT", 0.85)

    cfg = _Cfg(
        phase=ph,
        fail_open=fail_open,
        return_details=return_details,
        details_maxlen=details_maxlen,

        use_composite_gate=use_comp,
        strict_hard_guards=strict_hard,

        min_stage=_clamp(min_stage, 0.0, 1.0),
        max_stage=_clamp(max_stage, 0.0, 1.0),
        stage_sweet_spot=_clamp(sweet, 0.0, 1.0),
        stage_sweet_width=max(0.05, min(1.0, width)),

        min_liq_usd=max(0.0, float(min_liq)),
        min_fdv_usd=max(0.0, float(min_fdv)),
        max_fdv_usd=max(0.0, float(max_fdv)),
        min_liq_ratio=_clamp(float(min_ratio), 0.0, 1.0),

        min_unique_buyers=max(0, int(min_buyers)),
        min_buys_60s=max(0, int(min_buys)),
        min_sol_60s=max(0.0, float(min_sol)),
        max_idle_sec=max(0.0, float(max_idle)),
        staleness_sec=max(0.0, float(staleness)),

        good_wallet_weight=_safe_f(gw_w, 2.0),
        bad_wallet_weight=_safe_f(bw_w, -3.0),
        min_smart_wallet_score=_safe_f(min_sw, -2.0),

        min_velocity_score=_safe_f(min_vel, 6.0),
        min_velocity_floor=_safe_f(vel_floor, 4.8),

        gate_score=_clamp(_safe_f(gate, 22.0), 0.0, 100.0),
        require_min_vel=bool(req_min_vel),
        allow_smart_override=bool(allow_override),

        buyers_scale=_safe_f(buyers_scale, 18.0),
        buys_scale=_safe_f(buys_scale, 28.0),
        sol_scale=_safe_f(sol_scale, 4.5),

        idle_free_sec=max(0.0, float(idle_free)),
        idle_ramp_sec=max(1e-6, float(idle_ramp)),
        wash_buys_per_buyer_start=max(0.0, float(wash_bpb_start)),
        wash_buys_per_buyer_ramp=max(1e-6, float(wash_bpb_ramp)),
        wash_min_sol_per_buy=max(1e-6, float(wash_min_spb)),

        risk_weight_early=_safe_f(risk_w_early, 1.15),
        risk_weight_mid=_safe_f(risk_w_mid, 1.00),
        risk_weight_late=_safe_f(risk_w_late, 0.92),

        risk_to_score_weight=_clamp(float(risk_to_score_w), 0.50, 1.25),
    )

    _CFG_CACHE = cfg
    _CFG_CACHE_AT = now
    return cfg


def _maybe_details(cfg: _Cfg, d: Dict[str, Any]) -> Dict[str, Any]:
    if not cfg.return_details:
        return {}
    # cap to avoid huge blobs in logs
    try:
        s = str(d)
        if len(s) > cfg.details_maxlen:
            return {"truncated": True, "len": len(s), "head": s[: cfg.details_maxlen]}
        return d
    except Exception:
        return {"details_err": True}


# -----------------------------------------------------------------------------
# Scoring components
# -----------------------------------------------------------------------------
def _liq_quality(liq_usd: float, fdv_usd: float) -> float:
    """
    Liquidity quality proxy (0..1):
      - liq/fdv ratio matters (thickness)
      - absolute liq matters (too thin = rugs; too thick might be late, but we only lightly penalize)
    """
    liq = max(0.0, _safe_f(liq_usd))
    fdv = max(0.0, _safe_f(fdv_usd))
    if fdv <= 0.0:
        return 0.0

    ratio = liq / fdv
    ratio_score = _clamp((ratio - 0.03) / 0.17, 0.0, 1.0)  # 0.03..0.20 maps to 0..1

    # absolute liq via log curve (0..1 around 0..10k)
    liq_score = _clamp(math.log1p(liq) / math.log1p(10_000.0), 0.0, 1.0)

    # prefer ratio slightly more
    return _clamp(0.58 * ratio_score + 0.42 * liq_score, 0.0, 1.0)


def _idle_penalty(idle_sec: float, cfg: _Cfg) -> float:
    idle = max(0.0, _safe_f(idle_sec))
    if idle <= cfg.idle_free_sec:
        return 0.0
    return _clamp((idle - cfg.idle_free_sec) / cfg.idle_ramp_sec, 0.0, 1.0)


def _wash_penalty(buys: int, buyers: int, sol_60s: float, cfg: _Cfg) -> float:
    """
    Penalize spam/wash patterns (0..1):
      - many buys but few unique buyers
      - tiny SOL per buy with high buy count
    """
    b = max(0, int(buys))
    u = max(0, int(buyers))
    s = max(0.0, _safe_f(sol_60s))

    if b <= 0:
        return 0.0

    buys_per_buyer = b / max(1, u)
    sol_per_buy = s / max(1.0, float(b))

    spam1 = _clamp((buys_per_buyer - cfg.wash_buys_per_buyer_start) / cfg.wash_buys_per_buyer_ramp, 0.0, 1.0)
    spam2 = _clamp((cfg.wash_min_sol_per_buy - sol_per_buy) / cfg.wash_min_sol_per_buy, 0.0, 1.0)

    # relief: lots of unique buyers reduces wash suspicion
    buyer_relief = _clamp(u / 18.0, 0.0, 1.0)
    pen = (0.65 * spam1 + 0.35 * spam2) * (1.0 - 0.55 * buyer_relief)
    return _clamp(pen, 0.0, 1.0)


def _velocity_score(buys_60s: int, buyers_60s: int, sol_60s: float, idle_sec: float, cfg: _Cfg) -> float:
    """
    Velocity score (roughly 0..12-ish; can be lower with penalties).
    Weighted blend of buys, buyers, sol; penalized by idle and wash.
    """
    buys = max(0, int(buys_60s))
    buyers = max(0, int(buyers_60s))
    sol = max(0.0, _safe_f(sol_60s))

    buys_norm = min(buys, 35)
    buyers_norm = min(buyers, 24)
    sol_norm = min(sol, 6.5)

    v = 0.24 * buys_norm + 0.34 * buyers_norm + 0.42 * sol_norm
    v -= 2.25 * _idle_penalty(idle_sec, cfg)
    v -= 2.00 * _wash_penalty(buys, buyers, sol, cfg)
    return float(v)


def _wallet_mix_score(gw: int, bw: int, cfg: _Cfg) -> float:
    """
    Legacy-compatible signed score:
      good * weight + bad * weight
    """
    good = max(0, int(gw))
    bad = max(0, int(bw))
    return float(good * cfg.good_wallet_weight + bad * cfg.bad_wallet_weight)


def _blend_smart_features(stats: PumpStats, legacy_sw_score: float) -> Tuple[float, float, float]:
    """
    Convert available smart/whale hints to (smart_quality01, smart_risk01, enriched_score_dbg).

    - If no smart features are attached, infer weakly from legacy score + wallet mix.
    """
    conf = _clamp(_safe_f(stats.smart_signal_confidence), 0.0, 1.0)
    qual = _clamp(_safe_f(stats.smart_signal_quality), 0.0, 1.0)
    risk = _clamp(_safe_f(stats.smart_risk_flag), 0.0, 1.0)

    boost = _safe_f(stats.smart_entry_boost_hint, 0.0)  # -1..+1
    boost01 = _clamp((boost + 1.0) / 2.0, 0.0, 1.0)

    # infer if missing
    if conf == 0.0 and qual == 0.0 and abs(boost) < 1e-9:
        legacy01 = _sigmoid(0.35 * legacy_sw_score)
        conf = _clamp(0.25 + 0.60 * legacy01, 0.0, 1.0)
        qual = _clamp(0.20 + 0.65 * legacy01, 0.0, 1.0)
        boost01 = _clamp(legacy01, 0.0, 1.0)

        denom = max(1.0, float(stats.good_wallets + stats.bad_wallets))
        bad_dom = stats.bad_wallets / denom
        risk = _clamp(0.15 + 0.75 * bad_dom, 0.0, 1.0)

    # whale conflict & domination add risk (but not hard reject)
    conflict = _clamp(_safe_f(stats.smart_vs_whale_conflict), 0.0, 1.0)
    domination = _clamp(_safe_f(stats.whale_domination_5m), 0.0, 1.0)
    panic = _clamp(_safe_f(stats.smart_exit_pressure_hint), 0.0, 1.0)
    risk = _clamp(risk + 0.18 * conflict + 0.12 * domination + 0.10 * panic, 0.0, 1.0)

    enriched_dbg = 6.0 * (0.45 * boost01 + 0.35 * conf + 0.20 * qual) - 6.0 * (0.60 * risk)
    return float(qual), float(risk), float(enriched_dbg)


def _stage_pref(stage: float, cfg: _Cfg) -> float:
    """
    Stage preference (0..1): closer to sweet spot is better.
    """
    st = _clamp(_safe_f(stage), 0.0, 1.0)
    mid = cfg.stage_sweet_spot
    w = max(cfg.stage_sweet_width, 1e-6)
    return 1.0 - _clamp(abs(st - mid) / w, 0.0, 1.0)


def _composite_score(
    stats: PumpStats,
    velocity: float,
    sw_score: float,
    liq_q: float,
    cfg: _Cfg,
) -> Tuple[float, float, float, Dict[str, Any]]:
    """
    Returns:
      (score0_100, risk_penalty0_100, quality0_100, details)
    """
    # normalize velocity to 0..1 (velocity ~6 is decent)
    vel01 = _clamp((float(velocity) - 2.0) / 10.0, 0.0, 1.0)

    buyers01 = _clamp(stats.buyers_60s / cfg.buyers_scale, 0.0, 1.0)
    buys01 = _clamp(stats.buys_60s / cfg.buys_scale, 0.0, 1.0)
    sol01 = _clamp(stats.sol_60s / cfg.sol_scale, 0.0, 1.0)

    wash = _wash_penalty(stats.buys_60s, stats.buyers_60s, stats.sol_60s, cfg)
    idle = _idle_penalty(stats.idle_sec, cfg)

    smart_q, smart_risk, enriched_sw = _blend_smart_features(stats, sw_score)
    stpref = _stage_pref(stats.stage, cfg)

    # quality (0..1)
    quality01 = (
        0.30 * vel01
        + 0.14 * buyers01
        + 0.12 * buys01
        + 0.10 * sol01
        + 0.20 * liq_q
        + 0.14 * smart_q
        + 0.10 * stpref
    )
    quality01 = _clamp(quality01, 0.0, 1.0)

    # risk (0..1)
    risk01 = 0.40 * smart_risk + 0.25 * wash + 0.20 * idle

    conflict = _clamp(_safe_f(stats.smart_vs_whale_conflict), 0.0, 1.0)
    domination = _clamp(_safe_f(stats.whale_domination_5m), 0.0, 1.0)
    panic = _clamp(_safe_f(stats.smart_exit_pressure_hint), 0.0, 1.0)
    risk01 += 0.15 * conflict + 0.10 * domination + 0.10 * panic
    risk01 = _clamp(risk01, 0.0, 1.0)

    # phase shaping
    if cfg.phase <= 2:
        risk_weight = cfg.risk_weight_early
        risk_floor = 0.15
    elif cfg.phase <= 4:
        risk_weight = cfg.risk_weight_mid
        risk_floor = 0.10
    else:
        risk_weight = cfg.risk_weight_late
        risk_floor = 0.08

    # off-sweet-spot adds a little risk
    risk01 = _clamp(risk_weight * risk01 + risk_floor * (1.0 - stpref), 0.0, 1.0)

    # score: quality - risk (scaled)
    score = 100.0 * (quality01 - cfg.risk_to_score_weight * risk01)
    score = _clamp(score, 0.0, 100.0)

    details = {
        "phase": cfg.phase,
        "vel01": vel01,
        "buyers01": buyers01,
        "buys01": buys01,
        "sol01": sol01,
        "liq_q": liq_q,
        "stage_pref": stpref,
        "wash": wash,
        "idle": idle,
        "smart_q": smart_q,
        "smart_risk": smart_risk,
        "conflict": conflict,
        "domination": domination,
        "panic": panic,
        "enriched_sw": enriched_sw,
        "quality01": quality01,
        "risk01": risk01,
    }
    return float(score), float(100.0 * risk01), float(100.0 * quality01), details


# -----------------------------------------------------------------------------
# Core gating (sync) — never network
# -----------------------------------------------------------------------------
def check_pump_candidate(stats: PumpStats) -> PumpDecision:
    cfg = _get_cfg()

    try:
        if cfg.phase <= 0:
            return PumpDecision(False, "phase0_off", 0.0, 0.0, score=0.0, details=_maybe_details(cfg, {"phase": cfg.phase}))

        # timestamp / staleness
        now = time.time()
        if _safe_f(stats.ts, 0.0) <= 0.0:
            stats.ts = now
        if cfg.staleness_sec > 0 and (now - float(stats.ts)) > cfg.staleness_sec:
            age = now - float(stats.ts)
            return PumpDecision(False, f"stale_stats:{age:.2f}s", 0.0, 0.0, details=_maybe_details(cfg, {"phase": cfg.phase, "age": age}))

        # sanitize
        st = _clamp(_safe_f(stats.stage), 0.0, 1.0)
        liq = max(0.0, _safe_f(stats.liq_usd))
        fdv = max(0.0, _safe_f(stats.fdv_usd))
        buyers = max(0, _safe_i(stats.buyers_60s))
        buys = max(0, _safe_i(stats.buys_60s))
        sol = max(0.0, _safe_f(stats.sol_60s))
        idle = max(0.0, _safe_f(stats.idle_sec))

        # -------- Hard guards (phase-aware)
        if not (cfg.min_stage <= st <= cfg.max_stage):
            return PumpDecision(False, f"stage_out_of_range:{st:.2f}", 0.0, 0.0, details=_maybe_details(cfg, {"phase": cfg.phase, "stage": st}))

        if liq < cfg.min_liq_usd:
            return PumpDecision(False, f"liq_too_low:{liq:.1f}", 0.0, 0.0, details=_maybe_details(cfg, {"phase": cfg.phase, "liq": liq}))

        if fdv < cfg.min_fdv_usd:
            return PumpDecision(False, f"fdv_too_low:{fdv:.1f}", 0.0, 0.0, details=_maybe_details(cfg, {"phase": cfg.phase, "fdv": fdv}))

        if cfg.max_fdv_usd > 0.0 and fdv > cfg.max_fdv_usd:
            return PumpDecision(False, f"fdv_too_high:{fdv:.1f}", 0.0, 0.0, details=_maybe_details(cfg, {"phase": cfg.phase, "fdv": fdv}))

        liq_ratio = 0.0 if fdv <= 0.0 else (liq / fdv)
        if liq_ratio < cfg.min_liq_ratio:
            return PumpDecision(False, f"liq_ratio_too_low:{liq_ratio:.3f}", 0.0, 0.0, details=_maybe_details(cfg, {"phase": cfg.phase, "liq_ratio": liq_ratio}))

        if buyers < cfg.min_unique_buyers:
            return PumpDecision(False, f"too_few_buyers:{buyers}", 0.0, 0.0, details=_maybe_details(cfg, {"phase": cfg.phase, "buyers": buyers}))

        if buys < cfg.min_buys_60s:
            return PumpDecision(False, f"too_few_buys_60s:{buys}", 0.0, 0.0, details=_maybe_details(cfg, {"phase": cfg.phase, "buys": buys}))

        if sol < cfg.min_sol_60s:
            return PumpDecision(False, f"too_little_sol_60s:{sol:.3f}", 0.0, 0.0, details=_maybe_details(cfg, {"phase": cfg.phase, "sol": sol}))

        if idle > cfg.max_idle_sec:
            return PumpDecision(False, f"idle_too_long:{idle:.1f}s", 0.0, 0.0, details=_maybe_details(cfg, {"phase": cfg.phase, "idle": idle}))

        # -------- Signals
        sw_score = _wallet_mix_score(stats.good_wallets, stats.bad_wallets, cfg)
        velocity = _velocity_score(buys, buyers, sol, idle, cfg)

        # strict velocity floor keeps out true dead coins even in composite mode
        if cfg.strict_hard_guards and velocity < cfg.min_velocity_floor:
            return PumpDecision(
                False,
                f"velocity_floor_fail:{velocity:.2f}",
                float(velocity),
                float(sw_score),
                details=_maybe_details(cfg, {"phase": cfg.phase, "velocity": velocity, "sw": sw_score}),
            )

        # -------- Classic (non-composite)
        if not cfg.use_composite_gate:
            if sw_score < cfg.min_smart_wallet_score:
                return PumpDecision(
                    False,
                    f"bad_wallet_mix:{sw_score:.1f}",
                    float(velocity),
                    float(sw_score),
                    details=_maybe_details(cfg, {"phase": cfg.phase, "velocity": velocity, "sw": sw_score}),
                )
            if velocity < cfg.min_velocity_score:
                return PumpDecision(
                    False,
                    f"velocity_too_low:{velocity:.2f}",
                    float(velocity),
                    float(sw_score),
                    details=_maybe_details(cfg, {"phase": cfg.phase, "velocity": velocity, "sw": sw_score}),
                )
            return PumpDecision(
                True,
                "ok",
                float(velocity),
                float(sw_score),
                details=_maybe_details(cfg, {"phase": cfg.phase, "velocity": velocity, "sw": sw_score}),
            )

        # -------- Composite mode
        liq_q = _liq_quality(liq, fdv)
        score, risk_pen, qual, details = _composite_score(stats, velocity, sw_score, liq_q, cfg)

        # always enforce absolute velocity floor in composite too
        if velocity < cfg.min_velocity_floor:
            return PumpDecision(
                False,
                f"velocity_floor_fail:{velocity:.2f}",
                float(velocity),
                float(sw_score),
                score=score,
                risk_penalty=risk_pen,
                quality=qual,
                details=_maybe_details(cfg, details),
            )

        # wallet floor only if wallets observed (don’t punish missing data)
        if (stats.good_wallets + stats.bad_wallets) > 0 and sw_score < cfg.min_smart_wallet_score:
            if not cfg.allow_smart_override:
                return PumpDecision(
                    False,
                    f"wallet_floor_fail:{sw_score:.1f}",
                    float(velocity),
                    float(sw_score),
                    score=score,
                    risk_penalty=risk_pen,
                    quality=qual,
                    details=_maybe_details(cfg, details),
                )
            # allow override: wallet floor becomes a soft penalty via score (already included by smart_risk inference)

        if score < cfg.gate_score:
            return PumpDecision(
                False,
                f"score_too_low:{score:.1f}",
                float(velocity),
                float(sw_score),
                score=score,
                risk_penalty=risk_pen,
                quality=qual,
                details=_maybe_details(cfg, details),
            )

        if cfg.require_min_vel and velocity < cfg.min_velocity_score:
            return PumpDecision(
                False,
                f"velocity_too_low:{velocity:.2f}",
                float(velocity),
                float(sw_score),
                score=score,
                risk_penalty=risk_pen,
                quality=qual,
                details=_maybe_details(cfg, details),
            )

        return PumpDecision(
            True,
            "ok",
            float(velocity),
            float(sw_score),
            score=score,
            risk_penalty=risk_pen,
            quality=qual,
            details=_maybe_details(cfg, details),
        )

    except Exception as e:
        if cfg.fail_open:
            return PumpDecision(True, f"fail_open:{type(e).__name__}", 0.0, 0.0, score=50.0, details=_maybe_details(cfg, {"err": str(e)[:200]}))
        return PumpDecision(False, f"exception:{type(e).__name__}", 0.0, 0.0, details=_maybe_details(cfg, {"err": str(e)[:200]}))


# -----------------------------------------------------------------------------
# Optional async enrichment (in-process ONLY)
# -----------------------------------------------------------------------------
async def check_pump_candidate_async(stats: PumpStats) -> PumpDecision:
    """
    Optional: if utils_pkg.smart_wallets.TRACKER is available, attach tracker-built
    features into stats before gating.

    MUST remain safe:
      - no external network required here
      - if anything fails, fall back to check_pump_candidate(stats)
    """
    m = (stats.mint or "").strip()
    if not m:
        return check_pump_candidate(stats)

    if not _env_bool("PUMP_FILTER_ALLOW_ASYNC_ENRICH", True):
        return check_pump_candidate(stats)

    try:
        # Intended to be in-memory / cached (NO network required)
        from utils_pkg.smart_wallets import TRACKER  # type: ignore

        feats = await TRACKER.build_features(m)  # dict-like

        # map only what we consume
        stats.smart_entry_boost_hint = float(feats.get("smart_entry_boost_hint", stats.smart_entry_boost_hint))
        stats.smart_signal_confidence = float(feats.get("smart_signal_confidence", stats.smart_signal_confidence))
        stats.smart_signal_quality = float(feats.get("smart_signal_quality", stats.smart_signal_quality))
        stats.smart_risk_flag = float(feats.get("smart_risk_flag", stats.smart_risk_flag))
        stats.smart_exit_pressure_hint = float(feats.get("smart_exit_pressure_hint", stats.smart_exit_pressure_hint))

        stats.smart_vs_whale_conflict = float(feats.get("smart_vs_whale_conflict", stats.smart_vs_whale_conflict))
        stats.whale_domination_5m = float(feats.get("whale_domination_5m", stats.whale_domination_5m))
        stats.whale_share_5m = float(feats.get("whale_share_5m", stats.whale_share_5m))
        stats.smart_heat_score = float(feats.get("smart_heat_score", stats.smart_heat_score))
        stats.whale_heat_score = float(feats.get("whale_heat_score", stats.whale_heat_score))
        stats.smart_tradeable_flag = float(feats.get("smart_tradeable_flag", stats.smart_tradeable_flag))
    except Exception:
        pass

    return check_pump_candidate(stats)


__all__ = [
    "PumpStats",
    "PumpDecision",
    "check_pump_candidate",
    "check_pump_candidate_async",
]
