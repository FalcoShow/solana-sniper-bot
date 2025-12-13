# utils_pkg/pump_filters.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Tuple, List
import os
import math

@dataclass
class PumpStats:
    stage: float                # 0.0 - 1.0 bonding curve
    liq_usd: float
    fdv_usd: float
    buyers_60s: int
    buys_60s: int
    sol_60s: float
    idle_sec: float
    good_wallets: int
    bad_wallets: int

@dataclass
class PumpDecision:
    ok: bool
    reason: str
    velocity_score: float
    smart_wallet_score: float

def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    return float(v) if v is not None else default

def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    return int(v) if v is not None else default

def check_pump_candidate(stats: PumpStats) -> PumpDecision:
    # 1) Hard stage window
    min_stage = _env_float("PUMP_MIN_STAGE", 0.15)
    max_stage = _env_float("PUMP_MAX_STAGE", 0.75)
    if not (min_stage <= stats.stage <= max_stage):
        return PumpDecision(False, f"stage_out_of_range:{stats.stage:.2f}", 0.0, 0.0)

    # 2) Hard liquidity / FDV floor
    min_liq = _env_float("PUMP_MIN_LIQ_USD", 1500.0)
    min_fdv = _env_float("PUMP_MIN_FDV_USD", 5000.0)
    if stats.liq_usd < min_liq:
        return PumpDecision(False, f"liq_too_low:{stats.liq_usd:.1f}", 0.0, 0.0)
    if stats.fdv_usd < min_fdv:
        return PumpDecision(False, f"fdv_too_low:{stats.fdv_usd:.1f}", 0.0, 0.0)

    # 3) Basic buyer / volume guards
    min_buyers = _env_int("PUMP_MIN_UNIQUE_BUYERS", 5)
    min_buys_60s = _env_int("PUMP_MIN_BUYS_60S", 8)
    min_sol_60s = _env_float("PUMP_MIN_SOL_60S", 0.8)

    if stats.buyers_60s < min_buyers:
        return PumpDecision(False, f"too_few_buyers:{stats.buyers_60s}", 0.0, 0.0)
    if stats.buys_60s < min_buys_60s:
        return PumpDecision(False, f"too_few_buys_60s:{stats.buys_60s}", 0.0, 0.0)
    if stats.sol_60s < min_sol_60s:
        return PumpDecision(False, f"too_little_sol_60s:{stats.sol_60s:.3f}", 0.0, 0.0)

    # 4) Idle guard
    max_idle = _env_float("PUMP_MAX_IDLE_SEC", 90.0)
    if stats.idle_sec > max_idle:
        return PumpDecision(False, f"idle_too_long:{stats.idle_sec:.1f}s", 0.0, 0.0)

    # 5) Smart-wallet scoring
    gw = stats.good_wallets
    bw = stats.bad_wallets
    gw_w = _env_float("PUMP_GOOD_WALLET_WEIGHT", 2.0)
    bw_w = _env_float("PUMP_BAD_WALLET_WEIGHT", -3.0)
    smart_wallet_score = gw * gw_w + bw * bw_w
    min_sw = _env_float("PUMP_MIN_SMART_WALLET_SCORE", -2.0)
    if smart_wallet_score < min_sw:
        return PumpDecision(False, f"bad_wallet_mix:{smart_wallet_score:.1f}", 0.0, smart_wallet_score)

    # 6) Velocity score (simple but effective)
    # cap counts to avoid insane values dominating
    buys_norm = min(stats.buys_60s, 30)
    buyers_norm = min(stats.buyers_60s, 20)
    sol_norm = min(stats.sol_60s, 5.0)
    idle_penalty = max(stats.idle_sec - 30.0, 0.0) / 30.0  # each 30s adds 1 penalty

    velocity = (
        0.25 * buys_norm +
        0.35 * buyers_norm +
        0.4 * sol_norm -
        1.0 * idle_penalty
    )

    min_vel = _env_float("PUMP_MIN_VELOCITY_SCORE", 6.0)
    if velocity < min_vel:
        return PumpDecision(False, f"velocity_too_low:{velocity:.2f}", velocity, smart_wallet_score)

    return PumpDecision(True, "ok", velocity, smart_wallet_score)
