# execution/fees.py
from __future__ import annotations

"""
execution/fees.py — Consistent fee + priority model (P0 CANONICAL)
=================================================================

Role
----
Centralized fee math used by execution + PnL:

- Jito tip recommendation (bounded, robust to outliers)
- Priority fee (ComputeBudget CU price + CU limit) recommendation
- Deterministic total tx fee estimation (lamports/SOL)
- Optional conversion helpers for PnL attribution

Design goals
------------
- Pure math / no RPC
- Env-configurable (Render + local identical)
- Safe by default (floors/caps; avoids runaway fees)
- Backwards compatible with your existing public helper:
    jito_tip_lamports(recent_tips_lamports: list[int]) -> int

Important notes
---------------
- Solana fee mechanics can vary across clusters/epochs. This module does NOT
  attempt to be "perfect"; it provides consistent, bounded estimates and knobs.
- Router/platform fees and AMM fees are not modeled here (those belong in quote
  parsing / ledger). This module focuses on *network/priority/Jito* costs.

Env contract (prefix EXEC_)
--------------------------
Jito tip strategy:
- EXEC_JITO_TIP_MODE              default "median"    ("median" | "p75" | "p90")
- EXEC_JITO_TIP_WINDOW            default 5           (use last N tips)
- EXEC_JITO_TIP_MULTIPLIER        default 1.30
- EXEC_JITO_TIP_MIN_LAMPORTS      default 20_000
- EXEC_JITO_TIP_MAX_LAMPORTS      default 200_000
- EXEC_JITO_TIP_ABS_MAX_LAMPORTS  default 500_000     (hard safety ceiling)

Priority fee (ComputeBudget):
- EXEC_CU_LIMIT_DEFAULT           default 1_000_000
- EXEC_CU_LIMIT_MIN               default 200_000
- EXEC_CU_LIMIT_MAX               default 1_400_000
- EXEC_CU_PRICE_MIN_MICROLAMPORTS default 50
- EXEC_CU_PRICE_MAX_MICROLAMPORTS default 2_000
- EXEC_CU_PRICE_DEFAULT           default 200
- EXEC_CU_PRICE_URGENCY_FAST      default 400
- EXEC_CU_PRICE_URGENCY_MAX       default 800

Signature fee (approximation; override if you want):
- EXEC_SIG_FEE_LAMPORTS           default 5_000
- EXEC_DEFAULT_NUM_SIGNATURES     default 2

Total fee caps:
- EXEC_TX_FEE_SOFT_CAP_LAMPORTS   default 300_000     (warn/clip recommendation)
- EXEC_TX_FEE_HARD_CAP_LAMPORTS   default 800_000     (never recommend above)

Public API
----------
- jito_tip_lamports(recent_tips_lamports) -> int
- recommend_priority_fees(urgency="normal") -> PriorityFees
- estimate_total_tx_fee_lamports(...)
- recommend_fee_package(recent_tips_lamports=None, urgency="normal") -> FeePackage
"""

import contextlib
import os
import statistics
from collections.abc import Sequence
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

LAMPORTS_PER_SOL = 1_000_000_000


def lamports_to_sol(lamports: int) -> float:
    try:
        return float(int(lamports)) / float(LAMPORTS_PER_SOL)
    except Exception:
        return 0.0


def sol_to_lamports(sol: float) -> int:
    try:
        return int(round(float(sol) * float(LAMPORTS_PER_SOL)))
    except Exception:
        return 0


def _f(x: Any, default: float) -> float:
    try:
        if x is None:
            return float(default)
        return float(x)
    except Exception:
        return float(default)


def _i(x: Any, default: int) -> int:
    try:
        if x is None:
            return int(default)
        return int(float(x))
    except Exception:
        return int(default)


def _clamp_int(v: int, lo: int, hi: int) -> int:
    if v < lo:
        return lo
    if v > hi:
        return hi
    return v


def _clamp_float(v: float, lo: float, hi: float) -> float:
    if v < lo:
        return lo
    if v > hi:
        return hi
    return v


def _percentile(values: Sequence[int], p: float) -> int:
    """
    Deterministic percentile for small lists, no numpy.
    p in [0, 100].
    """
    if not values:
        return 0
    xs = sorted(int(x) for x in values)
    if len(xs) == 1:
        return xs[0]
    p = _clamp_float(float(p), 0.0, 100.0)
    # linear interpolation on rank
    k = (len(xs) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(xs) - 1)
    if c == f:
        return xs[f]
    d0 = xs[f] * (c - k)
    d1 = xs[c] * (k - f)
    return int(round(d0 + d1))


# ──────────────────────────────────────────────────────────────────────────────
# Params / models
# ──────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class FeeParams:
    # Jito tip strategy
    jito_tip_mode: str = "median"  # median | p75 | p90
    jito_tip_window: int = 5
    jito_tip_multiplier: float = 1.30
    jito_tip_min_lamports: int = 20_000
    jito_tip_max_lamports: int = 200_000
    jito_tip_abs_max_lamports: int = 500_000

    # Priority fee (compute budget)
    cu_limit_default: int = 1_000_000
    cu_limit_min: int = 200_000
    cu_limit_max: int = 1_400_000

    cu_price_min_microlamports: int = 50
    cu_price_max_microlamports: int = 2_000
    cu_price_default: int = 200
    cu_price_urgency_fast: int = 400
    cu_price_urgency_max: int = 800

    # Signature fee approximation + defaults
    sig_fee_lamports: int = 5_000
    default_num_signatures: int = 2

    # Total tx fee caps for recommendations
    tx_fee_soft_cap_lamports: int = 300_000
    tx_fee_hard_cap_lamports: int = 800_000

    @staticmethod
    def from_env(prefix: str = "EXEC_") -> FeeParams:
        mode = (os.getenv(prefix + "JITO_TIP_MODE") or "median").strip().lower()
        if mode not in {"median", "p75", "p90"}:
            mode = "median"

        window = max(1, _i(os.getenv(prefix + "JITO_TIP_WINDOW"), 5))
        mult = _clamp_float(_f(os.getenv(prefix + "JITO_TIP_MULTIPLIER"), 1.30), 0.5, 5.0)

        tip_min = max(0, _i(os.getenv(prefix + "JITO_TIP_MIN_LAMPORTS"), 20_000))
        tip_max = max(tip_min, _i(os.getenv(prefix + "JITO_TIP_MAX_LAMPORTS"), 200_000))
        tip_abs = max(tip_max, _i(os.getenv(prefix + "JITO_TIP_ABS_MAX_LAMPORTS"), 500_000))

        cu_lim_def = _i(os.getenv(prefix + "CU_LIMIT_DEFAULT"), 1_000_000)
        cu_lim_min = _i(os.getenv(prefix + "CU_LIMIT_MIN"), 200_000)
        cu_lim_max = _i(os.getenv(prefix + "CU_LIMIT_MAX"), 1_400_000)
        cu_lim_min = max(10_000, cu_lim_min)
        cu_lim_max = max(cu_lim_min, cu_lim_max)
        cu_lim_def = _clamp_int(cu_lim_def, cu_lim_min, cu_lim_max)

        cu_p_min = max(0, _i(os.getenv(prefix + "CU_PRICE_MIN_MICROLAMPORTS"), 50))
        cu_p_max = max(cu_p_min, _i(os.getenv(prefix + "CU_PRICE_MAX_MICROLAMPORTS"), 2_000))
        cu_p_def = _clamp_int(_i(os.getenv(prefix + "CU_PRICE_DEFAULT"), 200), cu_p_min, cu_p_max)
        cu_p_fast = _clamp_int(
            _i(os.getenv(prefix + "CU_PRICE_URGENCY_FAST"), 400), cu_p_min, cu_p_max
        )
        cu_p_maxu = _clamp_int(
            _i(os.getenv(prefix + "CU_PRICE_URGENCY_MAX"), 800), cu_p_min, cu_p_max
        )

        sig_fee = max(0, _i(os.getenv(prefix + "SIG_FEE_LAMPORTS"), 5_000))
        nsig = max(1, _i(os.getenv(prefix + "DEFAULT_NUM_SIGNATURES"), 2))

        soft = max(0, _i(os.getenv(prefix + "TX_FEE_SOFT_CAP_LAMPORTS"), 300_000))
        hard = max(soft, _i(os.getenv(prefix + "TX_FEE_HARD_CAP_LAMPORTS"), 800_000))

        return FeeParams(
            jito_tip_mode=mode,
            jito_tip_window=window,
            jito_tip_multiplier=mult,
            jito_tip_min_lamports=tip_min,
            jito_tip_max_lamports=tip_max,
            jito_tip_abs_max_lamports=tip_abs,
            cu_limit_default=cu_lim_def,
            cu_limit_min=cu_lim_min,
            cu_limit_max=cu_lim_max,
            cu_price_min_microlamports=cu_p_min,
            cu_price_max_microlamports=cu_p_max,
            cu_price_default=cu_p_def,
            cu_price_urgency_fast=cu_p_fast,
            cu_price_urgency_max=cu_p_maxu,
            sig_fee_lamports=sig_fee,
            default_num_signatures=nsig,
            tx_fee_soft_cap_lamports=soft,
            tx_fee_hard_cap_lamports=hard,
        )


@dataclass(frozen=True)
class PriorityFees:
    cu_limit: int
    cu_price_microlamports: int  # micro-lamports per CU


@dataclass(frozen=True)
class FeePackage:
    tip_lamports: int
    priority: PriorityFees
    est_total_fee_lamports: int
    notes: tuple[str, ...] = ()


# cache env once on import (tests can call reload_fee_params)
_PARAMS: FeeParams = FeeParams.from_env()


def get_fee_params() -> FeeParams:
    return _PARAMS


def reload_fee_params() -> FeeParams:
    global _PARAMS
    _PARAMS = FeeParams.from_env()
    with contextlib.suppress(Exception):
        log.info(
            "EXEC_FEE_PARAMS_RELOADED",
            extra={
                "jito_tip_mode": _PARAMS.jito_tip_mode,
                "jito_tip_window": _PARAMS.jito_tip_window,
                "jito_tip_min": _PARAMS.jito_tip_min_lamports,
                "jito_tip_max": _PARAMS.jito_tip_max_lamports,
                "cu_limit_default": _PARAMS.cu_limit_default,
                "cu_price_default": _PARAMS.cu_price_default,
                "sig_fee_lamports": _PARAMS.sig_fee_lamports,
            },
        )
    return _PARAMS


# ──────────────────────────────────────────────────────────────────────────────
# Jito tip strategy (backwards compatible)
# ──────────────────────────────────────────────────────────────────────────────


def jito_tip_lamports(recent_tips_lamports: list[int]) -> int:
    """
    Backwards-compatible helper: recommend a Jito tip based on recent tips.
    Uses env-configurable bounds/strategy.

    If no tips provided -> min tip.
    """
    return recommend_jito_tip(recent_tips_lamports, params=None)


def recommend_jito_tip(
    recent_tips_lamports: Sequence[int] | None,
    *,
    params: FeeParams | None = None,
) -> int:
    p = params or _PARAMS
    tips = [int(x) for x in (recent_tips_lamports or []) if int(x) > 0]
    if not tips:
        return _clamp_int(p.jito_tip_min_lamports, 0, p.jito_tip_abs_max_lamports)

    window = max(1, int(p.jito_tip_window))
    sample = tips[-window:] if len(tips) >= window else tips[:]

    # robust center/percentile (avoid mean; outliers ruin it)
    if p.jito_tip_mode == "p90":
        base = _percentile(sample, 90.0)
    elif p.jito_tip_mode == "p75":
        base = _percentile(sample, 75.0)
    else:
        # median (statistics.median is robust and deterministic)
        base = int(statistics.median(sample))

    tip = int(round(float(base) * float(p.jito_tip_multiplier)))

    # clamp: min..max..abs_max
    tip = _clamp_int(tip, int(p.jito_tip_min_lamports), int(p.jito_tip_max_lamports))
    tip = _clamp_int(tip, 0, int(p.jito_tip_abs_max_lamports))
    return tip


# ──────────────────────────────────────────────────────────────────────────────
# Priority fees (ComputeBudget) + tx fee estimation
# ──────────────────────────────────────────────────────────────────────────────


def recommend_priority_fees(
    *,
    urgency: str = "normal",
    params: FeeParams | None = None,
    cu_limit: int | None = None,
) -> PriorityFees:
    """
    Recommend ComputeBudget priority fee knobs.
    urgency: "normal" | "fast" | "max"
    """
    p = params or _PARAMS
    urg = (urgency or "normal").strip().lower()
    if urg not in {"normal", "fast", "max"}:
        urg = "normal"

    if urg == "max":
        cu_price = int(p.cu_price_urgency_max)
    elif urg == "fast":
        cu_price = int(p.cu_price_urgency_fast)
    else:
        cu_price = int(p.cu_price_default)

    cu_price = _clamp_int(cu_price, p.cu_price_min_microlamports, p.cu_price_max_microlamports)

    lim = int(cu_limit if cu_limit is not None else p.cu_limit_default)
    lim = _clamp_int(lim, p.cu_limit_min, p.cu_limit_max)

    return PriorityFees(cu_limit=lim, cu_price_microlamports=cu_price)


def estimate_compute_budget_fee_lamports(*, cu_limit: int, cu_price_microlamports: int) -> int:
    """
    Estimate ComputeBudget fee:
      fee_lamports ≈ (cu_limit * cu_price_microLamports) / 1_000_000
    """
    lim = max(0, int(cu_limit))
    price = max(0, int(cu_price_microlamports))
    return int((lim * price) // 1_000_000)


def estimate_signature_fee_lamports(
    *, num_signatures: int, sig_fee_lamports: int | None = None
) -> int:
    p = _PARAMS
    ns = max(0, int(num_signatures))
    fee = int(sig_fee_lamports if sig_fee_lamports is not None else p.sig_fee_lamports)
    fee = max(0, fee)
    return ns * fee


def estimate_total_tx_fee_lamports(
    *,
    num_signatures: int | None = None,
    cu_limit: int | None = None,
    cu_price_microlamports: int | None = None,
    jito_tip_lamports: int = 0,
    extra_lamports: int = 0,
    params: FeeParams | None = None,
) -> int:
    """
    Deterministic estimate for total tx cost (network-ish):
      signatures + computeBudget + jitoTip + extra

    This is used for PnL attribution and safety caps.
    """
    p = params or _PARAMS

    ns = int(num_signatures if num_signatures is not None else p.default_num_signatures)
    ns = max(0, ns)

    lim = int(cu_limit if cu_limit is not None else p.cu_limit_default)
    lim = _clamp_int(lim, p.cu_limit_min, p.cu_limit_max)

    price = int(
        cu_price_microlamports if cu_price_microlamports is not None else p.cu_price_default
    )
    price = _clamp_int(price, p.cu_price_min_microlamports, p.cu_price_max_microlamports)

    tip = max(0, int(jito_tip_lamports))
    tip = _clamp_int(tip, 0, p.jito_tip_abs_max_lamports)

    sig_fee = estimate_signature_fee_lamports(
        num_signatures=ns, sig_fee_lamports=p.sig_fee_lamports
    )
    cu_fee = estimate_compute_budget_fee_lamports(cu_limit=lim, cu_price_microlamports=price)
    extra = max(0, int(extra_lamports))

    total = sig_fee + cu_fee + tip + extra
    return max(0, int(total))


def recommend_fee_package(
    *,
    recent_tips_lamports: Sequence[int] | None = None,
    urgency: str = "normal",
    num_signatures: int | None = None,
    cu_limit: int | None = None,
    extra_lamports: int = 0,
    params: FeeParams | None = None,
) -> FeePackage:
    """
    One-stop "give me safe defaults" function.
    Applies caps so we never recommend insane fees.
    """
    p = params or _PARAMS
    notes: list[str] = []

    tip = recommend_jito_tip(recent_tips_lamports, params=p)
    pri = recommend_priority_fees(urgency=urgency, params=p, cu_limit=cu_limit)

    est = estimate_total_tx_fee_lamports(
        num_signatures=num_signatures if num_signatures is not None else p.default_num_signatures,
        cu_limit=pri.cu_limit,
        cu_price_microlamports=pri.cu_price_microlamports,
        jito_tip_lamports=tip,
        extra_lamports=extra_lamports,
        params=p,
    )

    # soft/hard caps: adjust downward if needed (prefer reducing tip first, then cu_price)
    if est > p.tx_fee_soft_cap_lamports:
        notes.append("tx_fee>soft_cap")
    if est > p.tx_fee_hard_cap_lamports:
        notes.append("tx_fee>hard_cap:clipping")

        # Clip by reducing tip, then cu_price.
        # Keep deterministic behavior; do not loop infinitely.
        over = est - p.tx_fee_hard_cap_lamports

        # 1) reduce tip
        if tip > p.jito_tip_min_lamports and over > 0:
            reducible = max(0, tip - p.jito_tip_min_lamports)
            cut = min(reducible, over)
            tip -= cut
            over -= cut

        # 2) reduce cu_price (linear effect via cu_limit)
        if over > 0 and pri.cu_price_microlamports > p.cu_price_min_microlamports:
            # each micro-lamport per CU contributes cu_limit/1e6 lamports
            per_unit = max(1, pri.cu_limit // 1_000_000)  # coarse; still deterministic
            # attempt to lower price by enough "units"
            delta = int((over + per_unit - 1) // per_unit)
            new_price = max(p.cu_price_min_microlamports, pri.cu_price_microlamports - delta)
            pri = PriorityFees(cu_limit=pri.cu_limit, cu_price_microlamports=new_price)

        est = estimate_total_tx_fee_lamports(
            num_signatures=num_signatures
            if num_signatures is not None
            else p.default_num_signatures,
            cu_limit=pri.cu_limit,
            cu_price_microlamports=pri.cu_price_microlamports,
            jito_tip_lamports=tip,
            extra_lamports=extra_lamports,
            params=p,
        )

        # absolute clamp just in case (should already be <= hard cap)
        if est > p.tx_fee_hard_cap_lamports:
            est = p.tx_fee_hard_cap_lamports

    return FeePackage(
        tip_lamports=int(tip),
        priority=pri,
        est_total_fee_lamports=int(est),
        notes=tuple(notes),
    )


__all__ = [
    "FeePackage",
    "FeeParams",
    "PriorityFees",
    "estimate_compute_budget_fee_lamports",
    "estimate_signature_fee_lamports",
    "estimate_total_tx_fee_lamports",
    "get_fee_params",
    "jito_tip_lamports",
    "lamports_to_sol",
    "recommend_fee_package",
    "recommend_jito_tip",
    "recommend_priority_fees",
    "reload_fee_params",
    "sol_to_lamports",
]
