# execution/params.py
from __future__ import annotations

"""
execution/params.py — Execution sizing + slippage plan (P0 CANONICAL)
====================================================================

Role
----
This module defines the **runtime knobs** and **deterministic order-planning**
logic used by the execution layer:

    Candidate + bankroll  ->  (order legs in USD, slippage_bps, diagnostics)

Goals
-----
- Deterministic & testable (pure math, no network)
- Safe by default (caps self-impact, caps slippage, avoids dust legs)
- Configurable via env (Render + local behave identically)
- Backwards compatible with your earlier constants:
    SLIPPAGE_BPS_MAX, BASE_SLIPPAGE_BPS, MAX_SELF_IMPACT_BPS, TRANCHES
    and compute_order_plan(cand, bankroll_usd)

Candidate fields used (best-effort; missing is fine)
---------------------------------------------------
- impact_1k_bps: expected impact in bps for a ~$1000 trade
- liq, liq_usd, liquidity_usd: liquidity in USD (optional cap)
- vol, vol5m, volume_usd_5m: volume hints (optional cap)
- symbol, mint: only for diagnostics

Env contract (prefix EXEC_)
--------------------------
Sizing:
- EXEC_MIN_ORDER_USD            default 10
- EXEC_MAX_ORDER_USD            default 2500
- EXEC_MAX_BANKROLL_PCT         default 0.25   (max 25% of bankroll per candidate)
- EXEC_MAX_LIQ_PCT              default 0.01   (max 1% of liquidity per entry if known)
- EXEC_MAX_VOL5M_PCT            default 0.05   (max 5% of 5m volume per entry if known)

Impact model:
- EXEC_MAX_SELF_IMPACT_BPS      default 300
- EXEC_IMPACT_EXPONENT          default 0.75   (impact grows sublinearly on thin books)
- EXEC_IMPACT_1K_BPS_FALLBACK   default 500

Slippage:
- EXEC_BASE_SLIPPAGE_BPS        default 400
- EXEC_SLIPPAGE_IMPACT_FACTOR   default 1.7    (base + factor*expectedImpact)
- EXEC_SLIPPAGE_BPS_MAX         default 1200
- EXEC_SLIPPAGE_BPS_MIN         default 50

Tranches:
- EXEC_TRANCHES                 default "0.6,0.4"
- EXEC_MIN_LEG_USD              default 6       (merge legs smaller than this)

Notes
-----
- This module outputs USD legs; later modules convert USD->SOL and route swaps.
- Fee modeling belongs in execution/fees.py (next file).
"""

import contextlib
import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

# ──────────────────────────────────────────────────────────────────────────────
# Logging (best-effort; never hard-fail)
# ──────────────────────────────────────────────────────────────────────────────


def _get_logger():
    try:
        from logging_setup import get_logger  # type: ignore

        return get_logger(__name__)
    except Exception:
        import logging

        return logging.getLogger(__name__)


log = _get_logger()


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────


def _truthy(x: Any) -> bool:
    return str(x or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _f(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return float(default)
        return float(x)
    except Exception:
        return float(default)


def _i(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return int(default)
        return int(float(x))
    except Exception:
        return int(default)


def _clamp(v: float, lo: float, hi: float) -> float:
    if v < lo:
        return lo
    if v > hi:
        return hi
    return v


def _cand_get(cand: Any, *keys: str) -> Any:
    """Get a field from dict-like candidate or attribute-like object."""
    if cand is None:
        return None
    if isinstance(cand, Mapping):
        for k in keys:
            if k in cand:
                return cand.get(k)
        return None
    for k in keys:
        if hasattr(cand, k):
            return getattr(cand, k)
    return None


def _parse_csv_floats(s: str) -> list[float]:
    out: list[float] = []
    for part in (s or "").replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(float(part))
        except Exception:
            continue
    return out


# ──────────────────────────────────────────────────────────────────────────────
# Data models
# ──────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ExecutionParams:
    # sizing
    min_order_usd: float = 10.0
    max_order_usd: float = 2500.0
    max_bankroll_pct: float = 0.25
    max_liq_pct: float = 0.01
    max_vol5m_pct: float = 0.05

    # impact model
    max_self_impact_bps: float = 300.0
    impact_exponent: float = 0.75
    impact_1k_bps_fallback: float = 500.0

    # slippage model
    base_slippage_bps: float = 400.0
    slippage_impact_factor: float = 1.7
    slippage_bps_min: int = 50
    slippage_bps_max: int = 1200

    # tranches
    tranches: tuple[float, ...] = (0.6, 0.4)
    min_leg_usd: float = 6.0

    @staticmethod
    def from_env(prefix: str = "EXEC_") -> ExecutionParams:
        def envf(name: str, default: float) -> float:
            return _f(os.getenv(prefix + name), default)

        def envi(name: str, default: int) -> int:
            return _i(os.getenv(prefix + name), default)

        tr_s = (os.getenv(prefix + "TRANCHES") or "0.6,0.4").strip()
        tr_list = _parse_csv_floats(tr_s)
        if not tr_list:
            tr_list = [0.6, 0.4]

        # sanitize tranches
        clean: list[float] = []
        for x in tr_list:
            if x <= 0:
                continue
            clean.append(float(x))
        if not clean:
            clean = [1.0]

        ssum = sum(clean)
        if ssum <= 0:
            clean = [1.0]
            ssum = 1.0
        # normalize to sum 1.0 (keeps relative proportions)
        clean = [x / ssum for x in clean]

        # clamp risky values
        impact_exp = _clamp(envf("IMPACT_EXPONENT", 0.75), 0.35, 1.25)
        max_bank_pct = _clamp(envf("MAX_BANKROLL_PCT", 0.25), 0.0, 1.0)
        max_liq_pct = _clamp(envf("MAX_LIQ_PCT", 0.01), 0.0, 0.25)
        max_vol_pct = _clamp(envf("MAX_VOL5M_PCT", 0.05), 0.0, 0.50)

        sl_min = max(0, envi("SLIPPAGE_BPS_MIN", 50))
        sl_max = max(sl_min, envi("SLIPPAGE_BPS_MAX", 1200))

        return ExecutionParams(
            min_order_usd=max(0.0, envf("MIN_ORDER_USD", 10.0)),
            max_order_usd=max(0.0, envf("MAX_ORDER_USD", 2500.0)),
            max_bankroll_pct=max_bank_pct,
            max_liq_pct=max_liq_pct,
            max_vol5m_pct=max_vol_pct,
            max_self_impact_bps=max(0.0, envf("MAX_SELF_IMPACT_BPS", 300.0)),
            impact_exponent=impact_exp,
            impact_1k_bps_fallback=max(1.0, envf("IMPACT_1K_BPS_FALLBACK", 500.0)),
            base_slippage_bps=max(0.0, envf("BASE_SLIPPAGE_BPS", 400.0)),
            slippage_impact_factor=max(0.0, envf("SLIPPAGE_IMPACT_FACTOR", 1.7)),
            slippage_bps_min=sl_min,
            slippage_bps_max=sl_max,
            tranches=tuple(clean),
            min_leg_usd=max(0.0, envf("MIN_LEG_USD", 6.0)),
        )


@dataclass(frozen=True)
class OrderPlan:
    order_usd: float
    legs_usd: tuple[float, ...]
    slippage_bps: int
    expected_impact_bps: float
    impact_1k_bps: float
    notes: tuple[str, ...] = ()


# ──────────────────────────────────────────────────────────────────────────────
# Cached params + backwards-compat constants
# ──────────────────────────────────────────────────────────────────────────────

_PARAMS: ExecutionParams = ExecutionParams.from_env()

# Backwards-compatible constants (do not edit directly; use env vars)
SLIPPAGE_BPS_MAX = int(_PARAMS.slippage_bps_max)
BASE_SLIPPAGE_BPS = int(_PARAMS.base_slippage_bps)
MAX_SELF_IMPACT_BPS = int(_PARAMS.max_self_impact_bps)
TRANCHES = list(_PARAMS.tranches)


def get_params() -> ExecutionParams:
    """Return the cached runtime params (read once from env on import)."""
    return _PARAMS


def reload_params() -> ExecutionParams:
    """
    Re-read env vars and update module cache.
    Useful in tests or local experimentation.
    """
    global _PARAMS, SLIPPAGE_BPS_MAX, BASE_SLIPPAGE_BPS, MAX_SELF_IMPACT_BPS, TRANCHES
    _PARAMS = ExecutionParams.from_env()
    SLIPPAGE_BPS_MAX = int(_PARAMS.slippage_bps_max)
    BASE_SLIPPAGE_BPS = int(_PARAMS.base_slippage_bps)
    MAX_SELF_IMPACT_BPS = int(_PARAMS.max_self_impact_bps)
    TRANCHES = list(_PARAMS.tranches)
    with contextlib.suppress(Exception):
        log.info(
            "EXEC_PARAMS_RELOADED",
            extra={
                "min_order_usd": _PARAMS.min_order_usd,
                "max_order_usd": _PARAMS.max_order_usd,
                "max_self_impact_bps": _PARAMS.max_self_impact_bps,
                "slippage_bps_max": _PARAMS.slippage_bps_max,
                "tranches": _PARAMS.tranches,
            },
        )
    return _PARAMS


# ──────────────────────────────────────────────────────────────────────────────
# Core math: impact + slippage + tranche splitting
# ──────────────────────────────────────────────────────────────────────────────


def _infer_liq_usd(cand: Any) -> float:
    return _f(
        _cand_get(cand, "liq_usd", "liquidity_usd", "liq", "liquidity"),
        0.0,
    )


def _infer_vol5m_usd(cand: Any) -> float:
    return _f(
        _cand_get(cand, "vol5m_usd", "volume_usd_5m", "vol5", "vol_5m", "vol"),
        0.0,
    )


def _infer_impact_1k_bps(cand: Any, p: ExecutionParams) -> float:
    imp = _f(_cand_get(cand, "impact_1k_bps", "impact_bps_1k"), 0.0)
    if imp <= 0:
        imp = p.impact_1k_bps_fallback
    # clamp absurd values
    return _clamp(imp, 10.0, 50_000.0)


def expected_impact_bps(*, impact_1k_bps: float, order_usd: float, exponent: float) -> float:
    """
    Expected impact for an order of size `order_usd`, given
    `impact_1k_bps` (impact at $1000) and scaling exponent.
    """
    order = max(0.0, float(order_usd))
    imp1k = max(1.0, float(impact_1k_bps))
    exp = _clamp(float(exponent), 0.35, 1.25)

    # model: impact(order) = impact_1k_bps * (order/1000)^exp
    x = max(0.0, order / 1000.0)
    return float(imp1k * (x**exp))


def solve_order_usd_for_impact_cap(
    *, impact_1k_bps: float, cap_bps: float, exponent: float
) -> float:
    """
    Invert expected_impact_bps to find the order_usd where impact ~= cap_bps.
    """
    imp1k = max(1.0, float(impact_1k_bps))
    cap = max(0.0, float(cap_bps))
    exp = _clamp(float(exponent), 0.35, 1.25)
    if cap <= 0:
        return 0.0
    # cap = imp1k * (order/1000)^exp  ->  order = 1000*(cap/imp1k)^(1/exp)
    ratio = max(0.0, cap / imp1k)
    return float(1000.0 * (ratio ** (1.0 / exp)))


def compute_slippage_bps(
    *,
    base_bps: float,
    expected_impact_bps: float,
    factor: float,
    bps_min: int,
    bps_max: int,
) -> int:
    """
    Slippage model:
        slippage = base + factor * expectedImpact
    then clamped to [bps_min, bps_max]
    """
    raw = float(base_bps) + float(factor) * max(0.0, float(expected_impact_bps))
    out = int(round(raw))
    if out < int(bps_min):
        out = int(bps_min)
    if out > int(bps_max):
        out = int(bps_max)
    return out


def split_tranches(
    order_usd: float, tranches: Iterable[float], *, min_leg_usd: float
) -> tuple[float, ...]:
    """
    Split an order into legs using tranche weights.
    - Normalizes weights (assumes input already sanitized but safe anyway)
    - Merges dust legs into the largest leg
    """
    order = max(0.0, float(order_usd))
    weights = [float(x) for x in tranches if float(x) > 0]
    if not weights:
        return (round(order, 2),)

    ssum = sum(weights)
    if ssum <= 0:
        return (round(order, 2),)

    w = [x / ssum for x in weights]
    legs = [round(order * wi, 2) for wi in w]

    # Fix rounding drift (ensure exact sum after rounding)
    drift = round(order - sum(legs), 2)
    if legs:
        legs[0] = round(legs[0] + drift, 2)

    # Merge dust
    if min_leg_usd > 0 and len(legs) > 1:
        big_i = max(range(len(legs)), key=lambda i: legs[i])
        dust_total = 0.0
        keep: list[float] = []
        for i, v in enumerate(legs):
            if i != big_i and v < float(min_leg_usd):
                dust_total += v
            else:
                keep.append(v)
        if dust_total > 0 and keep:
            # add dust into the largest remaining leg
            j = max(range(len(keep)), key=lambda i: keep[i])
            keep[j] = round(keep[j] + dust_total, 2)
        legs = keep

    # Remove zeros after dust merge
    legs = [v for v in legs if v > 0]
    if not legs:
        legs = [round(order, 2)]
    return tuple(legs)


# ──────────────────────────────────────────────────────────────────────────────
# Public API: order plan
# ──────────────────────────────────────────────────────────────────────────────


def compute_order_plan(
    cand: Any,
    bankroll_usd: float,
    params: ExecutionParams | None = None,
) -> tuple[list[float], int]:
    """
    Backwards-compatible wrapper returning:
        (legs_usd_list, slippage_bps)

    For richer diagnostics, call `compute_order_plan_v2`.
    """
    plan = compute_order_plan_v2(cand, bankroll_usd, params=params)
    return list(plan.legs_usd), int(plan.slippage_bps)


def compute_order_plan_v2(
    cand: Any,
    bankroll_usd: float,
    params: ExecutionParams | None = None,
) -> OrderPlan:
    """
    Canonical planner.

    Steps:
      1) Choose order_usd capped by bankroll, min/max, (optional) liquidity/volume caps,
         and self-impact cap via invertible impact model.
      2) Compute expected impact at that size.
      3) Compute slippage_bps from base + factor*impact, clamped.
      4) Split into tranche legs, merging dust.

    Returns an OrderPlan with diagnostics.
    """
    p = params or _PARAMS
    notes: list[str] = []

    br = max(0.0, float(bankroll_usd))
    if br <= 0:
        return OrderPlan(
            order_usd=0.0,
            legs_usd=(0.0,),
            slippage_bps=int(p.slippage_bps_min),
            expected_impact_bps=0.0,
            impact_1k_bps=p.impact_1k_bps_fallback,
            notes=("bankroll_usd<=0",),
        )

    impact1k = _infer_impact_1k_bps(cand, p)

    # 1) impact-limited size
    impact_cap_usd = solve_order_usd_for_impact_cap(
        impact_1k_bps=impact1k,
        cap_bps=p.max_self_impact_bps,
        exponent=p.impact_exponent,
    )
    if impact_cap_usd <= 0:
        impact_cap_usd = p.min_order_usd
        notes.append("impact_cap<=0->min_order")

    # 2) bankroll cap
    bankroll_cap_usd = br * _clamp(p.max_bankroll_pct, 0.0, 1.0)
    if bankroll_cap_usd <= 0:
        bankroll_cap_usd = br
        notes.append("bankroll_pct_cap<=0->full_bankroll")

    # 3) optional liquidity cap
    liq_usd = _infer_liq_usd(cand)
    liq_cap_usd = float("inf")
    if liq_usd > 0 and p.max_liq_pct > 0:
        liq_cap_usd = liq_usd * _clamp(p.max_liq_pct, 0.0, 0.25)
        notes.append(f"liq_cap({p.max_liq_pct:.4f})")

    # 4) optional 5m volume cap
    vol5m_usd = _infer_vol5m_usd(cand)
    vol_cap_usd = float("inf")
    if vol5m_usd > 0 and p.max_vol5m_pct > 0:
        vol_cap_usd = vol5m_usd * _clamp(p.max_vol5m_pct, 0.0, 0.50)
        notes.append(f"vol5m_cap({p.max_vol5m_pct:.4f})")

    # Combine caps
    order_usd = min(
        float(p.max_order_usd),
        impact_cap_usd,
        bankroll_cap_usd,
        liq_cap_usd,
        vol_cap_usd,
        br,  # never exceed bankroll itself
    )
    order_usd = _clamp(order_usd, float(p.min_order_usd), float(p.max_order_usd))

    # If min_order exceeds bankroll, allow spending bankroll (but still >=0)
    if order_usd > br:
        order_usd = max(0.0, br)
        notes.append("min_order>bankroll->use_bankroll")

    # 2) expected impact at chosen size
    exp_imp = expected_impact_bps(
        impact_1k_bps=impact1k,
        order_usd=order_usd,
        exponent=p.impact_exponent,
    )

    # 3) slippage
    slip = compute_slippage_bps(
        base_bps=p.base_slippage_bps,
        expected_impact_bps=exp_imp,
        factor=p.slippage_impact_factor,
        bps_min=p.slippage_bps_min,
        bps_max=p.slippage_bps_max,
    )

    # 4) split into legs
    legs = split_tranches(order_usd, p.tranches, min_leg_usd=p.min_leg_usd)

    # Safety: avoid a 0 plan
    if sum(legs) <= 0:
        legs = (round(order_usd, 2),)
        notes.append("legs_sum<=0->single_leg")

    return OrderPlan(
        order_usd=round(float(order_usd), 2),
        legs_usd=tuple(round(float(x), 2) for x in legs),
        slippage_bps=int(slip),
        expected_impact_bps=float(round(exp_imp, 2)),
        impact_1k_bps=float(round(impact1k, 2)),
        notes=tuple(notes),
    )


__all__ = [
    "BASE_SLIPPAGE_BPS",
    "MAX_SELF_IMPACT_BPS",
    # backwards-compat constants
    "SLIPPAGE_BPS_MAX",
    "TRANCHES",
    "ExecutionParams",
    "OrderPlan",
    "compute_order_plan",
    "compute_order_plan_v2",
    "compute_slippage_bps",
    "expected_impact_bps",
    "get_params",
    "reload_params",
    "split_tranches",
]
