from __future__ import annotations

"""
utils_pkg/reinvestor.py

Quant-grade reinvestment engine (V3+++)
======================================

High-level role
---------------
Decides what to do with *realized proceeds in SOL* after a winning (or partial)
exit:

    • How much to send to savings
    • How much (if any) to recycle into new entries
    • When to skip reinvestment entirely and just bank profits

Design goals
------------
    • Fully *risk-aware*:
        - Pulls global risk context from utils_pkg.risk_manager:
            » risk_capacity / risk_mode / risk_mode_override
            » daily PnL / rolling DD / equity
            » regime label / stress / signal / entry_gate_factor
            » Kelly diagnostics, AI capacity delta, halt_reason
            » daily_trades caps, sandbox_mode, etc.

    • Fully *strategy-aware*:
        - Pulls knobs + performance from utils_pkg.self_learning_ai_strategy:
            » REINVEST_PERCENT, MAX_RISK_PER_TRADE, STRAT_MODE, HARD_HALT
            » Plan status/gap, APR target, perf snapshot, Sharpe-like stats
            » Kelly suggestion / confidence for position sizing

    • Infra-aware:
        - Reads infra health from utils_pkg.infra_guard:
            » infra_health_score / infra_rpc_latency_ms / infra_jup_latency_ms
            » infra_state flags (healthy/degraded/halted)
            » optional gating via REINVEST_INFRA_MIN_HEALTH / infra_block flags

    • Self-learning feedback loop:
        - note_reinvest_decision(...) BEFORE executing
        - note_reinvest_result(...) AFTER success/fail
        - note_reinvest_skipped(...) for gated / skipped cases

    • Smart target selection:
        0) meta["force_token_mint"]       → absolute override (same token / backtests)
        1) utils_pkg.smart_wallets.pick_reinvestment_target (whales / AI alpha)
        2) legacy find_reinvestment_token scanner (volume + honeypot filtering)

    • Execution:
        - Buys executed via Jupiter router (swap_token_jupiter)
        - Positions persisted via positions.save_position
        - Risk budget accounted via risk_manager.note_spend
        - Structured logs → utils_pkg.data_logger.log_event + Notion + Telegram

Public API
----------
    async def reinvest(
        proceeds_sol: float,
        meta: dict | None = None,
        redis=None,
    ) -> str | None

    async def queue_reinvest(
        proceeds_sol: float,
        meta: dict | None = None,
        redis=None,
    ) -> None

    async def run_reinvest_worker(
        redis=None,
        poll_interval: float = 2.0,
        stop_evt: asyncio.Event | None = None,
    ) -> None

    async def get_reinvest_health(redis=None) -> dict | None

    async def preview_reinvest_split(
        proceeds_sol: float,
        meta: dict | None = None,
        redis=None,
    ) -> dict | None

    async def explain_reinvest_decision(
        proceeds_sol: float,
        meta: dict | None = None,
        redis=None,
    ) -> dict | None
        → Human-friendly explanation for dashboards / CLI.

Meta fields
-----------
meta can optionally contain:
    • force_savings_only: bool          → route 100% to savings (no reinvest)
    • dry_run: bool                     → compute & log but no swap
    • sandbox: bool                     → force sandbox/dry-run behaviour
    • force_eff_pct: float              → override effective reinvest % (0–100)
    • min_eff_pct / max_eff_pct: float  → local clamps on eff %
    • force_token_mint: str             → reinvest into a specific token
    • ignore_health_gate: bool          → allow even if health score is low
    • ignore_time_gate: bool            → bypass "minutes between reinvests" gate
    • ignore_infra_gate: bool           → bypass infra health gates
    • force_reinvest_even_if_risk_blocked: bool
        - For *backtests / simulations only*:
          let reinvest proceed even if should_allow_new_entry() == False
          (still respects deep DD, HARD_HALT, etc.)

    • tag: str                          → source tag for position payload / logs
    • profile / bucket / reinvest_profile: str
        - "core", "growth", "explore", "defensive", "degen" etc.
        - used to nudge aggressiveness up/down
    • origin_trade_id / origin_position_id: str
    • cycle_id / strategy_version / debug tags: any extra labels for self-learning
"""

import asyncio
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

# ============================================================================ #
# Redis handling
# ============================================================================ #

try:  # optional dependency
    import redis.asyncio as aioredis  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    aioredis = None  # type: ignore[assignment]

# Shared Redis + core helpers
from utils_pkg.core_utils import (
    get_token_price,
    swap_token_jupiter,
    send_telegram,
    log_to_notion,
    transfer_to_savings,
    convert_sol_to_usd_amount,
    redis as shared_redis,
)

# Infra health (RPC / JUP) — may be missing in older setups
try:
    from utils_pkg.infra_guard import (
        infra_features,
        should_block_entries as infra_should_block_entries,
    )  # type: ignore
except Exception:  # pragma: no cover

    async def infra_features(redis=None) -> Dict[str, float]:
        # Conservative "OK-ish" default if infra_guard is not present
        return {
            "infra_rpc_latency_ms": 0.0,
            "infra_jup_latency_ms": 0.0,
            "infra_rpc_latency_norm": 0.0,
            "infra_jup_latency_norm": 0.0,
            "infra_error_streak": 0.0,
            "infra_state_is_healthy": 1.0,
            "infra_state_is_degraded": 0.0,
            "infra_state_is_halted": 0.0,
            "infra_health_score": 1.0,
            "infra_stability_score": 1.0,
        }

    async def infra_should_block_entries(redis=None) -> bool:
        return False


# Self-learning brain (strategy knobs + stats + journaling hooks)
try:
    from utils_pkg.self_learning_ai_strategy import (
        get_strategy_params,
        get_perf_snapshot,
        note_reinvest_decision,
        note_reinvest_result,
        note_reinvest_skipped,
    )
except Exception:  # pragma: no cover

    async def get_strategy_params(_redis=None) -> Dict[str, Any]:
        # Minimal safe defaults if the real module is missing
        return {
            "REINVEST_PERCENT": float(os.getenv("REINVEST_PERCENT", "75.0")),
            "STOP_LOSS_PERCENT": float(os.getenv("STOP_LOSS_PERCENT", "18.0")),
            "SLIPPAGE": float(os.getenv("SLIPPAGE", "0.005")),
            "MIN_SOL_BALANCE": float(os.getenv("MIN_SOL_BALANCE", "0.25")),
            "MAX_RISK_PER_TRADE": float(os.getenv("MAX_RISK_PER_TRADE", "0.05")),
            "STRAT_MODE": 1,
            "HARD_HALT": False,
            "PLAN_TARGET_APR_PCT": 180.0,
            "PLAN_GAP_PCT": 0.0,
            "PLAN_STATUS": "n/a",
        }

    async def get_perf_snapshot(_redis=None) -> Dict[str, Any]:
        return {}

    async def note_reinvest_decision(*_: Any, **__: Any) -> None:
        return None

    async def note_reinvest_result(*_: Any, **__: Any) -> None:
        return None

    async def note_reinvest_skipped(*_: Any, **__: Any) -> None:
        return None


# Risk context (global capacity / mode / gating / budgets)
try:
    from utils_pkg.risk_manager import (
        risk_capacity_snapshot,
        should_allow_new_entry,
        note_spend,
    )
except Exception:  # pragma: no cover

    async def risk_capacity_snapshot(_redis=None) -> Dict[str, Any]:
        return {
            "risk_capacity": 1.0,
            "risk_mode": 1,
            "winrate20": 0.0,
            "rolling_dd_pct": 0.0,
            "equity_usd": 0.0,
            "daily_realized_pnl_usd": 0.0,
            "daily_realized_pnl_pct": 0.0,
            "daily_drawdown_pct": 0.0,
            "regime_label": "neutral",
            "regime_risk_multiplier": 1.0,
            "regime_label_numeric": 0.0,
            "regime_stress_score": 0.0,
            "regime_signal_strength": 0.5,
            "regime_entry_gate_factor": 1.0,
            "consecutive_losses": 0,
            "cooldown_active": False,
            "max_risk_per_trade": 0.05,
            "min_sol_balance": 0.0,
            "strat_mode": 1,
            "plan_equity_today_usd": 0.0,
            "plan_gap_pct": 0.0,
            "plan_status": "n/a",
            "strat_daily_spend_cap_pct": 0.0,
            "strat_daily_spend_cap_usd": 0.0,
            "strat_max_concurrent_override": None,
            "ai_capacity_delta": 0.0,
            "halt_reason": None,
            "daily_trades_count": 0,
            "daily_trades_cap": 0,
            "sandbox_mode": False,
            "kelly_raw": 0.0,
            "kelly_pos_pct_cap": 0.0,
            "equity_override_usd": 0.0,
        }

    async def should_allow_new_entry(_redis=None) -> bool:
        return True

    async def note_spend(_redis, usd: float) -> None:
        return None


# Optional smart-wallet target picker (whales / alpha feeds / AI ranking)
try:
    from utils_pkg.smart_wallets import pick_reinvestment_target  # type: ignore
except Exception:  # pragma: no cover
    pick_reinvestment_target = None  # type: ignore[assignment]


# Positions persistence (canonical storage)
try:
    from positions import save_position  # type: ignore
except Exception:  # pragma: no cover

    def save_position(mint: str, data: Dict[str, Any]) -> None:
        """
        Simple JSON-file fallback; prod uses DB-backed positions.
        """
        os.makedirs("data", exist_ok=True)
        path = os.path.join("data", "positions.json")
        try:
            with open(path, "r", encoding="utf-8") as f:
                cur = json.load(f)
        except Exception:
            cur = {}
        cur[mint] = data
        with open(path, "w", encoding="utf-8") as f:
            json.dump(cur, f, indent=2)


# Legacy safety scanner (fallback)
try:
    from find_reinvestment_token import find_reinvestment_token  # type: ignore
except Exception:  # pragma: no cover

    async def find_reinvestment_token(
        min_volume: float,
        honeypot_filter: bool,
        **_: Any,
    ) -> Optional[str]:
        return None


# Optional structured logger (data lake / analytics)
try:
    from utils_pkg.data_logger import log_event  # type: ignore[attr-defined]
except Exception:  # pragma: no cover

    async def log_event(*_: Any, **__: Any) -> None:
        return None


logger = logging.getLogger("reinvestor")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

SOL_MINT = "So11111111111111111111111111111111111111112"

REINVEST_QUEUE = "reinvest:queue"            # Redis list for queued jobs
REINVEST_HEALTH_KEY = "reinvest:health"      # last health snapshot JSON
REINVEST_DAILY_COUNT_KEY = "reinvest:daily"  # daily reinvest count prefix
REINVEST_LAST_TS_KEY = "reinvest:last_ts"    # last successful reinvest ts

__all__ = [
    "reinvest",
    "queue_reinvest",
    "run_reinvest_worker",
    "get_reinvest_health",
    "preview_reinvest_split",
    "explain_reinvest_decision",
]

# ============================================================================ #
# Env knobs
# ============================================================================ #

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# Minimum realized profit to even consider splitting (in SOL)
REINVESTMENT_MIN_PROFIT_SOL = float(
    os.getenv("REINVESTMENT_MIN_PROFIT_SOL", "0.01")
)

# Optional min profit in USD (0 = disabled)
REINVESTMENT_MIN_PROFIT_USD = float(
    os.getenv("REINVESTMENT_MIN_PROFIT_USD", "0.0")
)

# Liquidity/volume floor for reinvestment targets (24h volume, USD)
REINVESTMENT_VOLUME_MIN_USD = float(
    os.getenv("REINVESTMENT_VOLUME_MIN_USD")
    or os.getenv("REINVESTMENT_VOLUME_MIN", "5000")
)

REINVESTMENT_HONEYPOT_CHECK = (
    os.getenv("REINVESTMENT_HONEYPOT_CHECK", "true").strip().lower()
    in ("1", "true", "yes", "on")
)

SAVINGS_WALLET = os.getenv("SAVINGS_WALLET") or os.getenv("SAVINGS_WALLET_ADDRESS")

# Extra knobs for risk-aware scaling
REINVEST_MIN_CAPACITY = float(os.getenv("REINVEST_MIN_CAPACITY", "0.15"))
REINVEST_DEFENSIVE_CAPACITY = float(os.getenv("REINVEST_DEF_CAPACITY", "0.40"))
REINVEST_MAX_EFF_PCT = float(os.getenv("REINVEST_MAX_EFF_PCT", "95.0"))

# Hard-cap the USD size of any single reinvestment (0 = off)
REINVEST_MAX_SINGLE_USD = float(os.getenv("REINVEST_MAX_SINGLE_USD", "0"))

# Daily drawdown soft / hard stops (magnitudes in %)
REINVEST_DD_HARD_STOP_PCT = float(
    os.getenv("REINVEST_DD_HARD_STOP_PCT", "15.0")
)
REINVEST_DD_SOFT_STOP_PCT = float(
    os.getenv("REINVEST_DD_SOFT_STOP_PCT", "8.0")
)

# Daily PnL lockout: when day PnL ≥ this, stop reinvesting (0 = disabled)
REINVEST_PNL_LOCK_PCT = float(
    os.getenv("REINVEST_PNL_LOCK_PCT", "20.0")
)

# Optional global kill switch for reinvestor (useful for debugging / manual mode)
REINVESTOR_ENABLED = (
    os.getenv("REINVESTOR_ENABLED", "1").strip().lower()
    in ("1", "true", "yes", "on")
)

# Optional global dry-run: if set, all reinvest() calls act as dry_run
REINVEST_GLOBAL_DRY_RUN = (
    os.getenv("REINVEST_GLOBAL_DRY_RUN", "0").strip().lower()
    in ("1", "true", "yes", "on")
)

# How many jobs the worker will process in one batch before a small pause
REINVEST_WORKER_BATCH = int(os.getenv("REINVEST_WORKER_BATCH", "10"))

# Optional weekend / off-hours guard for auto-reinvestment (0=off)
REINVEST_BLOCK_WEEKENDS = (
    os.getenv("REINVEST_BLOCK_WEEKENDS", "0").strip().lower()
    in ("1", "true", "yes", "on")
)

# Optional minimum equity threshold for reinvestment (SOL-equivalent)
REINVEST_MIN_EQUITY_SOL = float(os.getenv("REINVEST_MIN_EQUITY_SOL", "0.0"))

# Health / performance gating (per-metric)
REINVEST_HEALTH_MIN_WINRATE = float(
    os.getenv("REINVEST_HEALTH_MIN_WINRATE", "0.40")
)
REINVEST_HEALTH_MIN_RECENT_WINRATE = float(
    os.getenv("REINVEST_HEALTH_MIN_RECENT_WINRATE", "0.38")
)
REINVEST_HEALTH_MIN_SHARPE = float(
    os.getenv("REINVEST_HEALTH_MIN_SHARPE", "-0.2")
)
REINVEST_HEALTH_MAX_DD = float(
    os.getenv("REINVEST_HEALTH_MAX_DD", "25.0")
)

# Aggregate health score gate (0–100, 0 = disabled)
REINVEST_HEALTH_MIN_SCORE = float(
    os.getenv("REINVEST_HEALTH_MIN_SCORE", "0.0")
)

# Infra-based gating
REINVEST_INFRA_MIN_HEALTH = float(
    os.getenv("REINVEST_INFRA_MIN_HEALTH", "0.0")  # 0 = disabled
)
REINVEST_BLOCK_IF_INFRA_HALTED = (
    os.getenv("REINVEST_BLOCK_IF_INFRA_HALTED", "1").strip().lower()
    in ("1", "true", "yes", "on")
)
REINVEST_BLOCK_IF_INFRA_DEGRADED = (
    os.getenv("REINVEST_BLOCK_IF_INFRA_DEGRADED", "0").strip().lower()
    in ("1", "true", "yes", "on")
)

# Optional skim: always send this % of proceeds straight to savings (0–100)
REINVEST_ALWAYS_SAVINGS_PCT = float(
    os.getenv("REINVEST_ALWAYS_SAVINGS_PCT", "0.0")
)

# Optional anti-overtrading limits
REINVEST_MAX_REINVESTS_PER_DAY = int(
    os.getenv("REINVEST_MAX_REINVESTS_PER_DAY", "0")  # 0 = unlimited
)

REINVEST_MIN_MINUTES_BETWEEN_REINVESTS = float(
    os.getenv("REINVEST_MIN_MINUTES_BETWEEN_REINVESTS", "0.0")
)

# ============================================================================ #
# Dataclasses / context
# ============================================================================ #


@dataclass
class ReinvestContext:
    # Inputs
    proceeds_sol: float

    # Risk context
    risk_capacity: float          # [0..1]
    risk_mode: int                # 0=OFF,1=NORMAL,2=AGG,3=DEF
    risk_mode_override: Optional[int]
    halt_reason: Optional[str]
    daily_pnl_pct: float          # percent, +10 == +10%
    daily_dd_pct: float           # percent magnitude, 8 == 8% DD
    equity_usd: float
    equity_sol: float
    daily_trades_count: int
    daily_trades_cap: int
    sandbox_mode: bool

    # Regime signals (from risk_manager / regime module)
    regime_label: str
    regime_label_numeric: float      # ~[-1..+1]
    regime_stress_score: float       # [0..1]
    regime_signal_strength: float    # [0..1]
    regime_entry_gate_factor: float  # ~0.2..1.5
    regime_risk_multiplier: float

    # Strategy parameters
    base_reinvest_pct: float
    stop_loss_percent: float
    slippage: float
    min_sol_balance: float
    max_risk_per_trade: float
    strat_mode: int      # 0=DEF,1=NEU,2=AGG from self-learning brain
    hard_halt: bool

    # Equity-plan insight
    plan_target_apr_pct: float
    plan_gap_pct: float
    plan_status: str

    # Performance stats / Kelly
    kelly_suggestion: float
    kelly_confidence: float
    kelly_pos_cap_pct: float
    win_rate: float
    recent_win_rate: float
    pnl_mean: float
    pnl_std: float
    sharpe_like: float
    recent_sharpe_like: float

    # Infra / infra_guard
    infra_health_score: float
    infra_rpc_latency_ms: float
    infra_jup_latency_ms: float
    infra_state_is_healthy: float
    infra_state_is_degraded: float
    infra_state_is_halted: float
    infra_stability_score: float
    infra_raw: Dict[str, Any]

    # Raw snapshots for logging / analytics
    risk_ctx_raw: Dict[str, Any]
    perf_raw: Dict[str, Any]

    # Meta
    meta: Dict[str, Any]


@dataclass
class ReinvestScales:
    """
    Debug / introspection helper: how we got from base_reinvest_pct to eff_pct.
    """
    base_pct: float
    cap_scale: float
    dd_scale: float
    kelly_scale: float
    plan_scale: float
    regime_scale: float
    ai_scale: float
    perf_scale: float
    profile_scale: float
    infra_scale: float

    @property
    def eff_pct(self) -> float:
        return float(
            self.base_pct
            * self.cap_scale
            * self.dd_scale
            * self.kelly_scale
            * self.plan_scale
            * self.regime_scale
            * self.ai_scale
            * self.perf_scale
            * self.profile_scale
            * self.infra_scale
        )

    def as_dict(self) -> Dict[str, float]:
        return {
            "base_pct": self.base_pct,
            "cap_scale": self.cap_scale,
            "dd_scale": self.dd_scale,
            "kelly_scale": self.kelly_scale,
            "plan_scale": self.plan_scale,
            "regime_scale": self.regime_scale,
            "ai_scale": self.ai_scale,
            "perf_scale": self.perf_scale,
            "profile_scale": self.profile_scale,
            "infra_scale": self.infra_scale,
            "eff_pct": self.eff_pct,
        }


# ============================================================================ #
# Redis helper
# ============================================================================ #


async def _get_redis(url: Optional[str] = None):
    """
    Prefer shared Redis from core_utils. Fallback to local aioredis client.
    """
    if shared_redis is not None:
        return shared_redis
    if aioredis is None:
        logger.warning(
            "redis (async) not installed; reinvest queue will run in direct mode."
        )
        return None
    try:
        return aioredis.from_url(url or REDIS_URL, decode_responses=True)
    except Exception:
        return None


# ============================================================================ #
# Utility helpers
# ============================================================================ #


async def _sol_to_usd(amount_sol: float) -> float:
    try:
        usd = await convert_sol_to_usd_amount(float(amount_sol))
        return float(usd or 0.0)
    except Exception:
        return 0.0


async def _token_price_in_sol(mint: str) -> float:
    """
    Convert token USD price to SOL-based price:
        px_sol = px_usd / sol_usd
    so it's consistent with risk_manager / positions expectations.
    """
    try:
        px_usd = await get_token_price(mint)
        if px_usd is None or px_usd <= 0:
            return 0.0
        sol_usd = await convert_sol_to_usd_amount(1.0)
        if sol_usd is None or sol_usd <= 0:
            return 0.0
        return float(px_usd) / float(sol_usd)
    except Exception as e:
        logger.debug("[REINVEST] _token_price_in_sol failed for %s: %s", mint, e)
        return 0.0


async def _send_to_savings(amount_sol: float) -> None:
    """
    Best-effort savings transfer. If no savings wallet configured, no-op.
    """
    if amount_sol <= 0:
        return
    if not SAVINGS_WALLET:
        logger.info(
            "[REINVEST] SAVINGS_WALLET not set; keeping %.6f SOL in main wallet.",
            amount_sol,
        )
        return
    try:
        await transfer_to_savings(float(amount_sol))
        usd = await _sol_to_usd(amount_sol)
        logger.info(
            "[REINVEST] Moved %.6f SOL (~$%.2f) to savings wallet.", amount_sol, usd
        )
        await send_telegram(
            f"🏦 Moved {amount_sol:.4f} SOL (~${usd:.2f}) to savings wallet."
        )
        try:
            await log_event(
                "reinvest_savings_transfer",
                {"amount_sol": amount_sol, "amount_usd": usd},
            )
        except Exception:
            pass
    except Exception as e:
        logger.warning("[REINVEST] Savings transfer failed: %s", e)


def _is_weekend(dt: Optional[datetime] = None) -> bool:
    dt = dt or datetime.now(timezone.utc)
    # Monday=0 ... Sunday=6
    return dt.weekday() >= 5


def _today_key(prefix: str) -> str:
    now = datetime.now(timezone.utc)
    return f"{prefix}:{now.strftime('%Y%m%d')}"


async def _get_last_reinvest_ts(redis) -> Optional[float]:
    if not redis:
        return None
    try:
        raw = await redis.get(REINVEST_LAST_TS_KEY)
        if not raw:
            return None
        return float(raw)
    except Exception:
        return None


async def _set_last_reinvest_ts(redis, ts: Optional[float] = None) -> None:
    if not redis:
        return
    try:
        if ts is None:
            ts = datetime.now(timezone.utc).timestamp()
        await redis.set(REINVEST_LAST_TS_KEY, str(float(ts)))
    except Exception:
        return


# ============================================================================ #
# Target selection helpers
# ============================================================================ #


async def _pick_target_mint(
    reinvest_sol: float,
    meta: Dict[str, Any],
    redis=None,
) -> Optional[str]:
    """
    Choose a mint for reinvestment using the smartest available source:

        0) meta["force_token_mint"] override
        1) utils_pkg.smart_wallets.pick_reinvestment_target (if present)
        2) legacy find_reinvestment_token scanner

    Returns token mint on success, else None.

    pick_reinvestment_target can return:
      - string mint
      - dict with 'mint' key
      - list of candidates [{'mint': ..., 'score': ...}, ...]
    """
    # 0) Hard override from meta (used by backtests / experiments / same-token scale-in)
    forced = meta.get("force_token_mint") or meta.get("target_mint")
    if isinstance(forced, str) and len(forced) > 30:
        return forced

    # 1) Smart wallets module (if wired)
    if pick_reinvestment_target is not None:
        target = None
        try:
            target = await pick_reinvestment_target(
                budget_sol=reinvest_sol,
                min_24h_volume_usd=REINVESTMENT_VOLUME_MIN_USD,
                redis=redis,
                meta=meta,
            )
        except TypeError:
            # Fallback to older signatures
            try:
                target = await pick_reinvestment_target(reinvest_sol)
            except Exception:
                target = None
        except Exception:
            target = None

        if target:
            if isinstance(target, str):
                return target

            if isinstance(target, dict):
                mint = (
                    target.get("mint")
                    or target.get("token")
                    or target.get("address")
                )
                if isinstance(mint, str):
                    return mint

            if isinstance(target, list):
                best_mint = None
                best_score = float("-inf")
                for cand in target:
                    if not isinstance(cand, dict):
                        continue
                    mint = (
                        cand.get("mint")
                        or cand.get("token")
                        or cand.get("address")
                    )
                    score = cand.get("score", 0.0)
                    if isinstance(mint, str) and isinstance(score, (int, float)):
                        if float(score) > best_score:
                            best_score = float(score)
                            best_mint = mint
                if best_mint:
                    return best_mint

    # 2) Legacy scanner
    try:
        mint = await find_reinvestment_token(
            min_volume=REINVESTMENT_VOLUME_MIN_USD,
            honeypot_filter=REINVESTMENT_HONEYPOT_CHECK,
        )
        if isinstance(mint, str) and len(mint) > 30:
            return mint
    except Exception as e:
        logger.warning("[REINVEST] legacy find_reinvestment_token failed: %s", e)

    return None


# ============================================================================ #
# Context build (risk + strategy + perf + regime + infra)
# ============================================================================ #


async def _build_context(
    proceeds_sol: float,
    meta: Optional[Dict[str, Any]],
    redis,
) -> Optional[ReinvestContext]:
    meta = meta or {}

    # Risk context snapshot (risk_manager.risk_capacity_snapshot)
    risk_ctx = await risk_capacity_snapshot(redis)

    # Robust field extraction with sane fallbacks
    risk_capacity = float(risk_ctx.get("risk_capacity", 1.0))
    risk_mode = int(risk_ctx.get("risk_mode", 1))
    risk_mode_override = risk_ctx.get("risk_mode_override")
    halt_reason = risk_ctx.get("halt_reason")

    daily_pnl_pct = float(risk_ctx.get("daily_realized_pnl_pct", 0.0))
    daily_dd_pct = float(risk_ctx.get("daily_drawdown_pct", 0.0))
    equity_usd = float(risk_ctx.get("equity_usd", 0.0))

    sandbox_mode = bool(risk_ctx.get("sandbox_mode", False))

    # Daily trades
    daily_trades_count = int(risk_ctx.get("daily_trades_count", 0) or 0)
    daily_trades_cap = int(risk_ctx.get("daily_trades_cap", 0) or 0)

    # Regime fields
    regime_label = str(risk_ctx.get("regime_label", "neutral") or "neutral")
    regime_label_numeric = float(risk_ctx.get("regime_label_numeric", 0.0))
    regime_stress_score = float(risk_ctx.get("regime_stress_score", 0.0))
    regime_signal_strength = float(risk_ctx.get("regime_signal_strength", 0.5))
    regime_entry_gate_factor = float(
        risk_ctx.get("regime_entry_gate_factor", 1.0)
    )
    regime_risk_multiplier = float(
        risk_ctx.get("regime_risk_multiplier", 1.0)
    )

    # Derive equity_sol if possible (for max_risk_per_trade cap)
    try:
        sol_usd = await convert_sol_to_usd_amount(1.0)
        equity_sol = equity_usd / sol_usd if sol_usd and sol_usd > 0 else 0.0
    except Exception:
        equity_sol = 0.0

    # Strategy parameters + performance snapshot
    params = await get_strategy_params(redis)
    perf = await get_perf_snapshot(redis)

    base_reinvest_pct = float(params.get("REINVEST_PERCENT", 75.0))
    stop_loss_percent = float(params.get("STOP_LOSS_PERCENT", 18.0))
    slippage = float(params.get("SLIPPAGE", 0.005))
    min_sol_balance = float(
        params.get(
            "MIN_SOL_BALANCE",
            risk_ctx.get("min_sol_balance", 0.25),
        )
    )
    max_risk_per_trade = float(
        params.get(
            "MAX_RISK_PER_TRADE",
            risk_ctx.get("max_risk_per_trade", 0.05),
        )
    )
    strat_mode = int(params.get("STRAT_MODE", risk_ctx.get("strat_mode", 1) or 1))
    hard_halt = bool(params.get("HARD_HALT", False))

    plan_target_apr_pct = float(params.get("PLAN_TARGET_APR_PCT", 180.0))
    plan_gap_pct = float(
        params.get("PLAN_GAP_PCT", risk_ctx.get("plan_gap_pct", 0.0))
    )
    plan_status = str(
        params.get("PLAN_STATUS", risk_ctx.get("plan_status", "n/a")) or "n/a"
    )

    # Kelly & perf: prefer self-learning's snapshot, fallback to risk_ctx fields
    kelly_from_perf = float(perf.get("kelly_suggestion", 0.0))
    kelly_from_risk = float(risk_ctx.get("kelly_raw", 0.0))
    kelly_suggestion = kelly_from_perf if kelly_from_perf != 0.0 else kelly_from_risk
    if kelly_suggestion == 0.0:
        kelly_suggestion = 0.02  # gentle default

    kelly_pos_cap_pct = float(risk_ctx.get("kelly_pos_pct_cap", 0.0))
    kelly_confidence = float(perf.get("kelly_confidence", 0.0))

    win_rate = float(perf.get("win_rate", risk_ctx.get("winrate20", 0.0)))
    recent_win_rate = float(
        perf.get("recent_win_rate", win_rate)
    )

    pnl_mean = float(perf.get("pnl_mean", 0.0))
    pnl_std = float(perf.get("pnl_std", 0.0))
    sharpe_like = float(perf.get("sharpe_like", 0.0))
    recent_sharpe_like = float(perf.get("recent_sharpe_like", sharpe_like))

    # Infra snapshot
    infra = await infra_features(redis)
    infra_health_score = float(infra.get("infra_health_score", 1.0))
    infra_rpc_latency_ms = float(infra.get("infra_rpc_latency_ms", 0.0))
    infra_jup_latency_ms = float(infra.get("infra_jup_latency_ms", 0.0))
    infra_state_is_healthy = float(infra.get("infra_state_is_healthy", 1.0))
    infra_state_is_degraded = float(infra.get("infra_state_is_degraded", 0.0))
    infra_state_is_halted = float(infra.get("infra_state_is_halted", 0.0))
    infra_stability_score = float(infra.get("infra_stability_score", 1.0))

    return ReinvestContext(
        proceeds_sol=float(proceeds_sol),
        risk_capacity=risk_capacity,
        risk_mode=risk_mode,
        risk_mode_override=(
            int(risk_mode_override) if risk_mode_override is not None else None
        ),
        halt_reason=str(halt_reason) if halt_reason is not None else None,
        daily_pnl_pct=daily_pnl_pct,
        daily_dd_pct=daily_dd_pct,
        equity_usd=equity_usd,
        equity_sol=equity_sol,
        daily_trades_count=daily_trades_count,
        daily_trades_cap=daily_trades_cap,
        sandbox_mode=sandbox_mode,
        regime_label=regime_label,
        regime_label_numeric=regime_label_numeric,
        regime_stress_score=regime_stress_score,
        regime_signal_strength=regime_signal_strength,
        regime_entry_gate_factor=regime_entry_gate_factor,
        regime_risk_multiplier=regime_risk_multiplier,
        base_reinvest_pct=base_reinvest_pct,
        stop_loss_percent=stop_loss_percent,
        slippage=slippage,
        min_sol_balance=min_sol_balance,
        max_risk_per_trade=max_risk_per_trade,
        strat_mode=strat_mode,
        hard_halt=hard_halt,
        plan_target_apr_pct=plan_target_apr_pct,
        plan_gap_pct=plan_gap_pct,
        plan_status=plan_status,
        kelly_suggestion=kelly_suggestion,
        kelly_confidence=kelly_confidence,
        kelly_pos_cap_pct=kelly_pos_cap_pct,
        win_rate=win_rate,
        recent_win_rate=recent_win_rate,
        pnl_mean=pnl_mean,
        pnl_std=pnl_std,
        sharpe_like=sharpe_like,
        recent_sharpe_like=recent_sharpe_like,
        infra_health_score=infra_health_score,
        infra_rpc_latency_ms=infra_rpc_latency_ms,
        infra_jup_latency_ms=infra_jup_latency_ms,
        infra_state_is_healthy=infra_state_is_healthy,
        infra_state_is_degraded=infra_state_is_degraded,
        infra_state_is_halted=infra_state_is_halted,
        infra_stability_score=infra_stability_score,
        infra_raw=infra,
        risk_ctx_raw=risk_ctx,
        perf_raw=perf,
        meta=dict(meta),
    )


# ============================================================================ #
# Health scoring
# ============================================================================ #


def _compute_health_score(ctx: ReinvestContext) -> float:
    """
    Aggregate health score in [0, 100] combining:
        • win rate (quality of edge)
        • drawdown (capital pain)
        • risk capacity (how much room is left)
        • Sharpe-like risk-adjusted returns
        • infra health (RPC / JUP quality)

    This is used both for:
        • diagnostics / dashboards
        • optional gating via REINVEST_HEALTH_MIN_SCORE
    """
    # Winrate: clamp to [0, 1] then map [0.30..0.70] → [0..1]
    wr = max(0.0, min(1.0, ctx.win_rate))
    wr_score = max(0.0, min(1.0, (wr - 0.30) / (0.70 - 0.30))) * 100.0

    # Drawdown: 0% DD == 1, ≥ REINVEST_HEALTH_MAX_DD == 0
    dd = max(0.0, ctx.daily_dd_pct)
    dd_score = 100.0
    if REINVEST_HEALTH_MAX_DD > 0:
        dd_norm = max(
            0.0,
            1.0 - min(dd, REINVEST_HEALTH_MAX_DD) / REINVEST_HEALTH_MAX_DD,
        )
        dd_score = dd_norm * 100.0

    # Capacity: clamp [0,1] → [0,100]
    cap = max(0.0, min(1.0, ctx.risk_capacity))
    cap_score = cap * 100.0

    # Sharpe-like: treat -1.0 as "bad", +1.0 as "good", clamp further out.
    sharpe = ctx.recent_sharpe_like or ctx.sharpe_like or 0.0
    sharpe_clamped = max(-1.5, min(1.5, sharpe))
    # Map [-1.0, +1.0] → [0, 1], with smooth clipping outside.
    sharpe_score = (sharpe_clamped + 1.0) / 2.0
    sharpe_score = max(0.0, min(1.0, sharpe_score)) * 100.0

    # Infra health: already in [0,1]
    infra_score = max(0.0, min(1.0, ctx.infra_health_score)) * 100.0

    # Blend: slightly more weight on winrate, DD, and capacity; Sharpe + infra as tie-breakers.
    health_score = float(
        0.32 * wr_score
        + 0.22 * dd_score
        + 0.22 * cap_score
        + 0.12 * sharpe_score
        + 0.12 * infra_score
    )
    return health_score


# ============================================================================ #
# Health snapshot (for dashboards/debug)
# ============================================================================ #


async def _update_reinvest_health(
    redis,
    ctx: ReinvestContext,
    eff_pct: float,
    reinvest_sol: float,
    savings_sol: float,
) -> None:
    """
    Lightweight health snapshot for dashboards / monitoring.
    """
    if not redis:
        return

    try:
        usd_reinvest = await _sol_to_usd(reinvest_sol)
        usd_savings = await _sol_to_usd(savings_sol)

        health_score = _compute_health_score(ctx)

        health_doc: Dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "eff_pct": eff_pct,
            "reinvest_sol": reinvest_sol,
            "savings_sol": savings_sol,
            "reinvest_usd": usd_reinvest,
            "savings_usd": usd_savings,
            "risk_capacity": ctx.risk_capacity,
            "risk_mode": ctx.risk_mode,
            "risk_mode_override": ctx.risk_mode_override,
            "halt_reason": ctx.halt_reason,
            "daily_pnl_pct": ctx.daily_pnl_pct,
            "daily_dd_pct": ctx.daily_dd_pct,
            "win_rate": ctx.win_rate,
            "recent_win_rate": ctx.recent_win_rate,
            "sharpe_like": ctx.sharpe_like,
            "recent_sharpe_like": ctx.recent_sharpe_like,
            "ai_capacity_delta": ctx.risk_ctx_raw.get("ai_capacity_delta", 0.0),
            "plan_status": ctx.plan_status,
            "plan_gap_pct": ctx.plan_gap_pct,
            "regime_label": ctx.regime_label,
            "regime_label_numeric": ctx.regime_label_numeric,
            "regime_stress_score": ctx.regime_stress_score,
            "regime_signal_strength": ctx.regime_signal_strength,
            "regime_entry_gate_factor": ctx.regime_entry_gate_factor,
            "sandbox_mode": ctx.sandbox_mode,
            "daily_trades_count": ctx.daily_trades_count,
            "daily_trades_cap": ctx.daily_trades_cap,
            # infra
            "infra_health_score": ctx.infra_health_score,
            "infra_rpc_latency_ms": ctx.infra_rpc_latency_ms,
            "infra_jup_latency_ms": ctx.infra_jup_latency_ms,
            "infra_state_is_healthy": ctx.infra_state_is_healthy,
            "infra_state_is_degraded": ctx.infra_state_is_degraded,
            "infra_state_is_halted": ctx.infra_state_is_halted,
            "infra_stability_score": ctx.infra_stability_score,
            "health_score": health_score,
        }

        await redis.set(REINVEST_HEALTH_KEY, json.dumps(health_doc))

        try:
            await log_event("reinvest_health", health_doc)
        except Exception:
            pass

    except Exception:
        # Health snapshot is non-critical
        return


async def get_reinvest_health(redis=None) -> Optional[Dict[str, Any]]:
    """
    Convenience helper: read the last health snapshot (JSON) from Redis.

    Returns parsed dict or None if unavailable.
    """
    if redis is None:
        redis = await _get_redis()
    if not redis:
        return None
    try:
        raw = await redis.get(REINVEST_HEALTH_KEY)
        if not raw:
            return None
        return json.loads(raw)
    except Exception:
        return None


# ============================================================================ #
# Effective reinvest % computation
# ============================================================================ #


def _kelly_scale_for_reinvest(ctx: ReinvestContext) -> float:
    """
    Convert Kelly suggestion/confidence into a smooth multiplier for reinvest %.
    """
    kelly = max(0.0, ctx.kelly_suggestion)
    # Use Kelly cap as natural "full risk" reference if available
    kelly_target = ctx.kelly_pos_cap_pct if ctx.kelly_pos_cap_pct > 0 else 0.08
    kelly_target = max(0.01, min(kelly_target, 0.15))

    if kelly <= 0.005 or ctx.win_rate < 0.45 or ctx.pnl_mean < 0:
        base_scale = 0.5
    else:
        ratio = min(2.0, max(0.3, kelly / kelly_target))
        base_scale = 0.7 + 0.6 * ((ratio - 0.3) / (2.0 - 0.3))

    conf = ctx.kelly_confidence
    if 0.0 < conf < 1.0:
        return 1.0 + (base_scale - 1.0) * conf
    return base_scale


def _capacity_base_scale(ctx: ReinvestContext) -> float:
    """
    Start scaling around risk capacity, risk_mode and STRAT_MODE.
    """
    cap = max(0.0, min(1.0, ctx.risk_capacity))
    scale = max(0.25, min(1.0, cap))

    # risk_mode from risk_manager (0=off,1=neutral,2=aggressive,3=defensive)
    if ctx.risk_mode == 3:
        scale *= 0.5
    elif ctx.risk_mode == 2:
        scale *= 1.1

    # STRAT_MODE from self-learning brain (0=DEF,1=NEU,2=AGG)
    if ctx.strat_mode == 0:
        scale *= 0.7
    elif ctx.strat_mode == 2:
        scale *= 1.1

    return scale


def _drawdown_scale(ctx: ReinvestContext) -> float:
    """
    Soft shrink of reinvest % between DD soft/hard bands.

    daily_dd_pct is POSITIVE magnitude:
        - if < soft band: no change
        - between soft and hard: smoothly decay to 0.2
        - ≥ hard: handled earlier as savings-only
    """
    dd = max(0.0, ctx.daily_dd_pct)
    if (
        REINVEST_DD_SOFT_STOP_PCT <= 0
        or REINVEST_DD_HARD_STOP_PCT <= REINVEST_DD_SOFT_STOP_PCT
    ):
        return 1.0

    if dd < REINVEST_DD_SOFT_STOP_PCT:
        return 1.0

    span = REINVEST_DD_HARD_STOP_PCT - REINVEST_DD_SOFT_STOP_PCT
    span = max(span, 0.001)
    mag = min(REINVEST_DD_HARD_STOP_PCT, dd)
    frac = (mag - REINVEST_DD_SOFT_STOP_PCT) / span
    frac = max(0.0, min(1.0, frac))
    return max(0.2, 1.0 - 0.8 * frac)


def _plan_scale(ctx: ReinvestContext) -> float:
    """
    Equity-plan nudge:
      • "behind" and not in bad DD → mild boost
      • "ahead" with DD/vol issues → tilt more to savings
    """
    status = ctx.plan_status
    gap = ctx.plan_gap_pct
    dd = ctx.daily_dd_pct
    vol = ctx.pnl_std
    cap = ctx.risk_capacity

    scale = 1.0
    if status == "behind" and gap < -3.0:
        if cap > 0.6 and dd < REINVEST_DD_SOFT_STOP_PCT:
            scale *= 1.05
    elif status == "ahead" and gap > 3.0:
        if dd >= REINVEST_DD_SOFT_STOP_PCT or vol > 20.0:
            scale *= 0.9
    return scale


def _regime_scale(ctx: ReinvestContext) -> float:
    """
    Regime-aware nudge:
      • Bearish / high stress → shrink
      • Bullish / strong signal, low stress → mild boost
    """
    regime_label = ctx.regime_label_numeric
    regime_stress = ctx.regime_stress_score
    regime_signal = ctx.regime_signal_strength
    regime_gate = ctx.regime_entry_gate_factor

    scale = 1.0
    if regime_label < -0.3 or regime_stress > 0.7:
        scale *= 0.7
    elif regime_label > 0.3 and regime_signal > 0.4 and regime_stress < 0.4:
        scale *= 1.08

    # Regime gate factor (if <1 tighten, if >1 slightly loosen)
    if 0.1 <= regime_gate <= 2.0:
        if regime_gate < 0.95:
            scale *= (0.9 + 0.1 * regime_gate)
        elif regime_gate > 1.05:
            scale *= (1.0 + 0.05 * (regime_gate - 1.0))

    return scale


def _ai_capacity_scale(ctx: ReinvestContext) -> float:
    """
    AI-derived capacity delta: when AI says capacity is tight, lean more to savings.
    """
    delta = ctx.risk_ctx_raw.get("ai_capacity_delta", 0.0)
    if delta < -0.6:
        return 0.7
    if delta < -0.1:
        return 0.85
    if delta > 0.3:
        return 1.05
    if delta > 0.1:
        return 1.03
    return 1.0


def _perf_scale(ctx: ReinvestContext) -> float:
    """
    Performance-based nudging:
        • Poor recent WR + negative Sharpe → shrink
        • Strong recent WR + good Sharpe → mild boost
    """
    rec_wr = ctx.recent_win_rate or ctx.win_rate
    sharpe = ctx.recent_sharpe_like or ctx.sharpe_like

    scale = 1.0
    if rec_wr < 0.38 and sharpe < -0.2:
        scale *= 0.7
    elif rec_wr > 0.55 and sharpe > 0.3:
        scale *= 1.07
    return scale


def _profile_scale(meta: Dict[str, Any]) -> float:
    """
    Meta-driven profile scaling:
        profile / bucket / reinvest_profile ∈ {
          "core", "growth", "explore", "defensive", "degen", ...
        }
    """
    raw = meta.get("profile") or meta.get("bucket") or meta.get("reinvest_profile")
    if raw is None:
        return 1.0
    try:
        profile = str(raw).lower()
    except Exception:
        return 1.0

    scale = 1.0
    if profile in ("core", "base"):
        scale = 1.0
    elif profile in ("growth", "momentum", "trend"):
        scale = 1.05
    elif profile in ("explore", "experimental", "beta"):
        scale = 1.10
    elif profile in ("defensive", "income", "savings"):
        scale = 0.75
    elif profile in ("degen", "aggressive"):
        scale = 1.15

    # Keep profile influence within sane bounds
    return max(0.5, min(1.2, scale))


def _infra_scale(ctx: ReinvestContext) -> float:
    """
    Infra-based scaling:
        - Halted: heavily shrink (actual blocking is handled elsewhere).
        - Degraded: mild shrink unless risk/strategy already extreme.
        - Healthy + high stability: small boost.
    """
    halted = ctx.infra_state_is_halted > 0.5
    degraded = ctx.infra_state_is_degraded > 0.5
    healthy = ctx.infra_state_is_healthy > 0.5
    infra_h = max(0.0, min(1.0, ctx.infra_health_score))
    stability = max(0.0, min(1.0, ctx.infra_stability_score))

    if halted:
        return 0.3
    if degraded:
        return 0.8

    # Healthy path: if infra health + stability high, small boost
    if healthy and infra_h > 0.7 and stability > 0.7:
        return 1.03
    return 1.0


def _compute_scales(ctx: ReinvestContext) -> ReinvestScales:
    """
    Compute all the scaling factors and return a ReinvestScales object so we can
    reuse the same logic for eff_pct and expose a detailed breakdown.
    """
    cap_scale = _capacity_base_scale(ctx)
    dd_scale = _drawdown_scale(ctx)
    kelly_scale = _kelly_scale_for_reinvest(ctx)
    plan_scale = _plan_scale(ctx)
    regime_scale = _regime_scale(ctx)
    ai_scale = _ai_capacity_scale(ctx)
    perf_scale = _perf_scale(ctx)
    profile_scale = _profile_scale(ctx.meta)
    infra_scale = _infra_scale(ctx)

    return ReinvestScales(
        base_pct=ctx.base_reinvest_pct,
        cap_scale=cap_scale,
        dd_scale=dd_scale,
        kelly_scale=kelly_scale,
        plan_scale=plan_scale,
        regime_scale=regime_scale,
        ai_scale=ai_scale,
        perf_scale=perf_scale,
        profile_scale=profile_scale,
        infra_scale=infra_scale,
    )


def _apply_meta_eff_overrides(eff_pct: float, meta: Dict[str, Any]) -> float:
    """
    Apply meta-based clamps (min/max) and hard override (force_eff_pct).
    """
    # Hard override wins (for backtests / experiments)
    if "force_eff_pct" in meta:
        try:
            forced = float(meta["force_eff_pct"])
            return max(0.0, min(REINVEST_MAX_EFF_PCT, forced))
        except Exception:
            pass

    min_eff = float(meta.get("min_eff_pct", 0.0) or 0.0)
    max_eff_local = float(meta.get("max_eff_pct", REINVEST_MAX_EFF_PCT) or 0.0)

    eff_pct = max(min_eff, eff_pct)
    eff_pct = min(
        REINVEST_MAX_EFF_PCT,
        max_eff_local or REINVEST_MAX_EFF_PCT,
        eff_pct,
    )
    return max(0.0, eff_pct)


def _compute_eff_reinvest_pct(ctx: ReinvestContext) -> Tuple[float, ReinvestScales]:
    """
    Compose all the scaling factors into a single effective reinvest %.
    Returns:
        (eff_pct_raw, scales)
    """
    scales = _compute_scales(ctx)
    return float(scales.eff_pct), scales


# ============================================================================ #
# Skip helpers / journaling
# ============================================================================ #


async def _journal_skip(
    reason: str,
    proceeds_sol: float,
    ctx: Optional[ReinvestContext],
    meta: Dict[str, Any],
    redis=None,
) -> None:
    """
    Central place to announce & journal "skip reinvest" decisions.
    """
    ctx_payload: Dict[str, Any] = {}
    if ctx is not None:
        ctx_payload = {
            "risk_capacity": ctx.risk_capacity,
            "risk_mode": ctx.risk_mode,
            "risk_mode_override": ctx.risk_mode_override,
            "halt_reason": ctx.halt_reason,
            "daily_pnl_pct": ctx.daily_pnl_pct,
            "daily_dd_pct": ctx.daily_dd_pct,
            "equity_sol": ctx.equity_sol,
            "plan_status": ctx.plan_status,
            "plan_gap_pct": ctx.plan_gap_pct,
            "sandbox_mode": ctx.sandbox_mode,
            "daily_trades_count": ctx.daily_trades_count,
            "daily_trades_cap": ctx.daily_trades_cap,
            "infra_health_score": getattr(ctx, "infra_health_score", 1.0),
            "infra_state_is_halted": getattr(ctx, "infra_state_is_halted", 0.0),
            "infra_state_is_degraded": getattr(ctx, "infra_state_is_degraded", 0.0),
        }

    try:
        await note_reinvest_skipped(
            reason=reason,
            proceeds_sol=proceeds_sol,
            ctx=ctx_payload,
            meta=meta,
            redis=redis,
        )
    except Exception:
        pass

    try:
        await log_event(
            "reinvest_skipped",
            {
                "reason": reason,
                "proceeds_sol": proceeds_sol,
                "meta": meta,
                "ctx": getattr(ctx, "risk_ctx_raw", None) if ctx else None,
                "infra": getattr(ctx, "infra_raw", None) if ctx else None,
            },
        )
    except Exception:
        pass


async def _bump_daily_reinvest_counter(redis) -> None:
    if not redis:
        return
    try:
        key = _today_key(REINVEST_DAILY_COUNT_KEY)
        pipe = redis.pipeline()
        pipe.incr(key)
        pipe.expire(key, 60 * 60 * 36)  # ~1.5 days
        await pipe.execute()
    except Exception:
        return


async def _get_daily_reinvest_count(redis) -> int:
    if not redis:
        return 0
    try:
        key = _today_key(REINVEST_DAILY_COUNT_KEY)
        raw = await redis.get(key)
        return int(raw or "0")
    except Exception:
        return 0


# ============================================================================ #
# Core reinvest logic
# ============================================================================ #


async def reinvest(
    proceeds_sol: float,
    meta: Optional[Dict[str, Any]] = None,
    redis=None,
) -> Optional[str]:
    """
    Split proceeds into savings + reinvest, risk-gated & quant-aware.

    Returns:
        new token mint on success, else None.
    """
    meta = dict(meta or {})

    # Global dry-run override from env (useful for testing full stack)
    if REINVEST_GLOBAL_DRY_RUN:
        meta["dry_run"] = True

    # Meta sandbox flag (for backtests/tools) — makes sure we never trade live.
    if meta.get("sandbox"):
        meta["dry_run"] = True

    if proceeds_sol is None or proceeds_sol <= 0:
        logger.info("[REINVEST] No proceeds to reinvest.")
        return None

    # If realized leg itself is tiny in SOL, just bank it.
    if proceeds_sol < REINVESTMENT_MIN_PROFIT_SOL:
        logger.info(
            "[REINVEST] proceeds %.6f SOL below global threshold %.6f SOL; "
            "sending all to savings.",
            proceeds_sol,
            REINVESTMENT_MIN_PROFIT_SOL,
        )
        await _journal_skip(
            "below_min_profit_sol", proceeds_sol, None, meta, redis=redis
        )
        await _send_to_savings(proceeds_sol)
        return None

    # Optional USD threshold
    proceeds_usd = await _sol_to_usd(proceeds_sol)
    if REINVESTMENT_MIN_PROFIT_USD > 0 and proceeds_usd < REINVESTMENT_MIN_PROFIT_USD:
        logger.info(
            "[REINVEST] proceeds ~$%.2f below USD threshold %.2f; sending to savings.",
            proceeds_usd,
            REINVESTMENT_MIN_PROFIT_USD,
        )
        await _journal_skip(
            "below_min_profit_usd", proceeds_sol, None, meta, redis=redis
        )
        await _send_to_savings(proceeds_sol)
        return None

    # Global kill-switch: all proceeds → savings
    if not REINVESTOR_ENABLED or meta.get("force_savings_only"):
        logger.info(
            "[REINVEST] REINVESTOR disabled or force_savings_only "
            "-> sending %.6f SOL to savings only.",
            proceeds_sol,
        )
        await _journal_skip(
            "reinvestor_disabled_or_forced_savings",
            proceeds_sol,
            None,
            meta,
            redis=redis,
        )
        await _send_to_savings(proceeds_sol)
        return None

    if redis is None:
        redis = await _get_redis()

    # Weekend guard (optional)
    if REINVEST_BLOCK_WEEKENDS and _is_weekend() and not meta.get("ignore_time_gate"):
        logger.info(
            "[REINVEST] Weekend guard enabled; routing %.6f SOL to savings only.",
            proceeds_sol,
        )
        await _journal_skip(
            "weekend_guard", proceeds_sol, None, meta, redis=redis
        )
        await _send_to_savings(proceeds_sol)
        return None

    # Anti-overtrading: daily cap on reinvest count
    if REINVEST_MAX_REINVESTS_PER_DAY > 0 and redis is not None:
        daily_count = await _get_daily_reinvest_count(redis)
        if daily_count >= REINVEST_MAX_REINVESTS_PER_DAY:
            logger.info(
                "[REINVEST] Daily reinvest cap reached (%d >= %d); "
                "sending %.6f SOL to savings.",
                daily_count,
                REINVEST_MAX_REINVESTS_PER_DAY,
                proceeds_sol,
            )
            await _journal_skip(
                "daily_cap_reached", proceeds_sol, None, meta, redis=redis
            )
            await _send_to_savings(proceeds_sol)
            return None

    # ------------------------------------------------------------------ #
    # Build context (risk + strategy + perf + regime + infra)
    # ------------------------------------------------------------------ #
    ctx = await _build_context(proceeds_sol, meta, redis)
    if ctx is None:
        logger.info("[REINVEST] Failed to build reinvest context; sending to savings.")
        await _journal_skip(
            "context_build_failed", proceeds_sol, None, meta, redis=redis
        )
        await _send_to_savings(proceeds_sol)
        return None

    risk_capacity = ctx.risk_capacity
    risk_mode = ctx.risk_mode
    daily_pnl_pct = ctx.daily_pnl_pct
    daily_dd_pct = ctx.daily_dd_pct
    equity_sol = ctx.equity_sol
    strat_mode = ctx.strat_mode
    hard_halt = ctx.hard_halt

    regime_label = ctx.regime_label_numeric
    regime_stress = ctx.regime_stress_score
    regime_signal = ctx.regime_signal_strength
    regime_gate = ctx.regime_entry_gate_factor
    ai_capacity_delta = ctx.risk_ctx_raw.get("ai_capacity_delta", 0.0)

    # Daily trades cap from risk layer
    daily_trades_count = ctx.daily_trades_count
    daily_trades_cap = ctx.daily_trades_cap

    # Infra flags
    infra_halted = ctx.infra_state_is_halted > 0.5
    infra_degraded = ctx.infra_state_is_degraded > 0.5
    infra_health_score = ctx.infra_health_score

    # If risk layer says sandbox_mode, force dry-run at this layer as well.
    if ctx.sandbox_mode and not meta.get("dry_run"):
        logger.info("[REINVEST] sandbox_mode=1 from risk_ctx; forcing dry_run.")
        meta["dry_run"] = True

    # Optional equity floor: if account is very small, focus on rebuilding balance
    if REINVEST_MIN_EQUITY_SOL > 0 and equity_sol > 0 and equity_sol < REINVEST_MIN_EQUITY_SOL:
        logger.info(
            "[REINVEST] Equity %.4f SOL below REINVEST_MIN_EQUITY_SOL %.4f; "
            "routing %.6f SOL to savings only.",
            equity_sol,
            REINVEST_MIN_EQUITY_SOL,
            proceeds_sol,
        )
        await _journal_skip(
            "equity_below_min", proceeds_sol, ctx, meta, redis=redis
        )
        await _send_to_savings(proceeds_sol)
        return None

    # Time-based throttling: avoid rapidly chaining reinvests in choppy regimes
    if (
        REINVEST_MIN_MINUTES_BETWEEN_REINVESTS > 0
        and redis is not None
        and not meta.get("ignore_time_gate")
    ):
        last_ts = await _get_last_reinvest_ts(redis)
        if last_ts:
            now_ts = datetime.now(timezone.utc).timestamp()
            elapsed_min = (now_ts - last_ts) / 60.0
            if elapsed_min < REINVEST_MIN_MINUTES_BETWEEN_REINVESTS:
                logger.info(
                    "[REINVEST] Last reinvest %.2f min ago (< %.2f min); "
                    "sending %.6f SOL to savings.",
                    elapsed_min,
                    REINVEST_MIN_MINUTES_BETWEEN_REINVESTS,
                    proceeds_sol,
                )
                await _journal_skip(
                    "min_gap_between_reinvests",
                    proceeds_sol,
                    ctx,
                    meta,
                    redis=redis,
                )
                await _send_to_savings(proceeds_sol)
                return None

    # ------------------------------------------------------------------ #
    # Risk gating: HARD_HALT + capacity + risk_mode + trade caps
    # ------------------------------------------------------------------ #
    allow_entry = await should_allow_new_entry(redis)
    force_reinvest_even_if_risk_blocked = bool(
        meta.get("force_reinvest_even_if_risk_blocked", False)
    )

    # Strategy-level hard halt from self-learning brain
    if hard_halt:
        logger.info(
            "[REINVEST] HARD_HALT from strategy params; routing %.6f SOL to savings.",
            proceeds_sol,
        )
        await _journal_skip("hard_halt", proceeds_sol, ctx, meta, redis=redis)
        await _send_to_savings(proceeds_sol)
        return None

    # Global trades cap from risk layer
    if (
        daily_trades_cap > 0
        and daily_trades_count >= daily_trades_cap
        and not force_reinvest_even_if_risk_blocked
    ):
        logger.info(
            "[REINVEST] daily_trades_count cap reached (%d >= %d); "
            "sending %.6f SOL to savings.",
            daily_trades_count,
            daily_trades_cap,
            proceeds_sol,
        )
        await _journal_skip(
            "daily_trades_cap_reached", proceeds_sol, ctx, meta, redis=redis
        )
        await _send_to_savings(proceeds_sol)
        return None

    # Global hard blocks or very low capacity
    if (
        (not allow_entry) or risk_capacity < REINVEST_MIN_CAPACITY or risk_mode == 0
    ) and not force_reinvest_even_if_risk_blocked:
        logger.info(
            "[REINVEST] Risk gating blocked new entries "
            "(allow=%s, capacity=%.3f, mode=%s). Sending to savings only.",
            allow_entry,
            risk_capacity,
            risk_mode,
        )
        await _journal_skip(
            "risk_blocked", proceeds_sol, ctx, meta, redis=redis
        )
        await _send_to_savings(proceeds_sol)
        return None

    # Defensive mode with very low capacity: treat as soft halt
    if risk_mode == 3 and risk_capacity < REINVEST_DEFENSIVE_CAPACITY:
        logger.info(
            "[REINVEST] Defensive mode with low capacity (cap=%.3f < %.3f); "
            "sending %.6f SOL to savings.",
            risk_capacity,
            REINVEST_DEFENSIVE_CAPACITY,
            proceeds_sol,
        )
        await _journal_skip(
            "defensive_capacity_low", proceeds_sol, ctx, meta, redis=redis
        )
        await _send_to_savings(proceeds_sol)
        return None

    # Deep daily DD (magnitude ≥ hard stop) → pure capital protection
    if (
        REINVEST_DD_HARD_STOP_PCT > 0
        and daily_dd_pct >= REINVEST_DD_HARD_STOP_PCT
    ):
        logger.info(
            "[REINVEST] Deep daily drawdown (%.2f%% ≥ %.2f%%) — "
            "routing to savings only.",
            daily_dd_pct,
            REINVEST_DD_HARD_STOP_PCT,
            proceeds_sol,
        )
        await _journal_skip(
            "deep_drawdown", proceeds_sol, ctx, meta, redis=redis
        )
        await _send_to_savings(proceeds_sol)
        return None

    # Very high daily realized PnL: protect capital, avoid over-trading
    if REINVEST_PNL_LOCK_PCT > 0 and daily_pnl_pct >= REINVEST_PNL_LOCK_PCT:
        logger.info(
            "[REINVEST] Daily PnL already high (%.2f%% ≥ %.2f%%) — "
            "capital protection: sending %.6f SOL to savings.",
            daily_pnl_pct,
            REINVEST_PNL_LOCK_PCT,
            proceeds_sol,
        )
        await _journal_skip(
            "pnl_day_high", proceeds_sol, ctx, meta, redis=redis
        )
        await _send_to_savings(proceeds_sol)
        return None

    # Performance/health gating: if stats are too bad, don't spin up risk
    if (
        ctx.win_rate < REINVEST_HEALTH_MIN_WINRATE
        and ctx.recent_win_rate < REINVEST_HEALTH_MIN_RECENT_WINRATE
        and ctx.sharpe_like < REINVEST_HEALTH_MIN_SHARPE
    ):
        logger.info(
            "[REINVEST] Performance too weak (WR=%.2f, recent=%.2f, Sharpe=%.2f); "
            "sending %.6f SOL to savings.",
            ctx.win_rate,
            ctx.recent_win_rate,
            ctx.sharpe_like,
            proceeds_sol,
        )
        await _journal_skip(
            "perf_too_weak", proceeds_sol, ctx, meta, redis=redis
        )
        await _send_to_savings(proceeds_sol)
        return None

    # Aggregate health score gating (optional, can be bypassed per-meta)
    health_score = _compute_health_score(ctx)
    if (
        REINVEST_HEALTH_MIN_SCORE > 0.0
        and not meta.get("ignore_health_gate")
        and health_score < REINVEST_HEALTH_MIN_SCORE
    ):
        logger.info(
            "[REINVEST] Health score low (%.1f < %.1f); "
            "sending %.6f SOL to savings.",
            health_score,
            REINVEST_HEALTH_MIN_SCORE,
            proceeds_sol,
        )
        await _journal_skip(
            "health_score_low", proceeds_sol, ctx, meta, redis=redis
        )
        await _send_to_savings(proceeds_sol)
        return None

    # ------------------------------------------------------------------ #
    # Infra gating on top of risk_manager (via infra_guard)
    # ------------------------------------------------------------------ #
    if not meta.get("ignore_infra_gate"):
        # Hard block if infra says "block entries" and flag is set
        infra_block = await infra_should_block_entries(redis)
        if infra_block and REINVEST_BLOCK_IF_INFRA_HALTED:
            logger.info(
                "[REINVEST] Infra guard requested block; sending %.6f SOL to savings.",
                proceeds_sol,
            )
            await _journal_skip(
                "infra_guard_block", proceeds_sol, ctx, meta, redis=redis
            )
            await _send_to_savings(proceeds_sol)
            return None

        # Block if infra state is HALTED explicitly
        if infra_halted and REINVEST_BLOCK_IF_INFRA_HALTED:
            logger.info(
                "[REINVEST] Infra HALTED (health=%.2f); sending %.6f SOL to savings.",
                infra_health_score,
                proceeds_sol,
            )
            await _journal_skip(
                "infra_halted", proceeds_sol, ctx, meta, redis=redis
            )
            await _send_to_savings(proceeds_sol)
            return None

        # Optional block on DEGRADE
        if infra_degraded and REINVEST_BLOCK_IF_INFRA_DEGRADED:
            logger.info(
                "[REINVEST] Infra DEGRADED (health=%.2f); sending %.6f SOL to savings.",
                infra_health_score,
                proceeds_sol,
            )
            await _journal_skip(
                "infra_degraded", proceeds_sol, ctx, meta, redis=redis
            )
            await _send_to_savings(proceeds_sol)
            return None

        # Min infra health threshold (0..1)
        if (
            REINVEST_INFRA_MIN_HEALTH > 0.0
            and infra_health_score < REINVEST_INFRA_MIN_HEALTH
        ):
            logger.info(
                "[REINVEST] Infra health low (%.2f < %.2f); sending %.6f SOL to savings.",
                infra_health_score,
                REINVEST_INFRA_MIN_HEALTH,
                proceeds_sol,
            )
            await _journal_skip(
                "infra_health_low", proceeds_sol, ctx, meta, redis=redis
            )
            await _send_to_savings(proceeds_sol)
            return None

    # Regime gate factor extremely low → treat like soft halt
    if regime_gate < 0.2:
        logger.info(
            "[REINVEST] Regime gate factor very low (%.2f); sending %.6f SOL to savings.",
            regime_gate,
            proceeds_sol,
        )
        await _journal_skip(
            "regime_gate_low", proceeds_sol, ctx, meta, redis=redis
        )
        await _send_to_savings(proceeds_sol)
        return None

    # If AI capacity delta says "very constrained", treat like a soft halt
    if ai_capacity_delta < -0.8:
        logger.info(
            "[REINVEST] AI capacity strongly negative (%.3f); "
            "routing %.6f SOL to savings.",
            ai_capacity_delta,
            proceeds_sol,
        )
        await _journal_skip(
            "ai_capacity_tight", proceeds_sol, ctx, meta, redis=redis
        )
        await _send_to_savings(proceeds_sol)
        return None

    # ------------------------------------------------------------------ #
    # Effective reinvest % (core brain)
    # ------------------------------------------------------------------ #
    eff_pct_raw, scales = _compute_eff_reinvest_pct(ctx)

    # Global skim: always send part of proceeds straight to savings
    if REINVEST_ALWAYS_SAVINGS_PCT > 0:
        eff_pct_raw = max(0.0, eff_pct_raw - REINVEST_ALWAYS_SAVINGS_PCT)

    eff_pct = _apply_meta_eff_overrides(eff_pct_raw, ctx.meta)

    reinvest_sol = proceeds_sol * (eff_pct / 100.0)
    savings_sol = max(0.0, proceeds_sol - reinvest_sol)

    # Self-learning hook: log decision BEFORE caps
    try:
        await note_reinvest_decision(
            proceeds_sol=proceeds_sol,
            eff_pct_raw=eff_pct_raw,
            eff_pct_final=eff_pct,
            scales=scales.as_dict(),
            ctx={  # keep it JSON-safe where possible
                "risk_capacity": risk_capacity,
                "risk_mode": risk_mode,
                "risk_mode_override": ctx.risk_mode_override,
                "halt_reason": ctx.halt_reason,
                "daily_pnl_pct": daily_pnl_pct,
                "daily_dd_pct": daily_dd_pct,
                "equity_sol": equity_sol,
                "plan_status": ctx.plan_status,
                "plan_gap_pct": ctx.plan_gap_pct,
                "kelly_suggestion": ctx.kelly_suggestion,
                "kelly_confidence": ctx.kelly_confidence,
                "regime_label_numeric": regime_label,
                "regime_stress_score": regime_stress,
                "regime_signal_strength": regime_signal,
                "regime_entry_gate_factor": regime_gate,
                "ai_capacity_delta": ai_capacity_delta,
                "sandbox_mode": ctx.sandbox_mode,
                "health_score": health_score,
                "infra_health_score": infra_health_score,
                "infra_state_is_halted": infra_halted,
                "infra_state_is_degraded": infra_degraded,
                "daily_trades_count": daily_trades_count,
                "daily_trades_cap": daily_trades_cap,
            },
            meta=ctx.meta,
            redis=redis,
        )
    except Exception:
        pass

    # Optional: cap by equity * max_risk_per_trade if equity_sol available
    if equity_sol > 0 and ctx.max_risk_per_trade > 0:
        max_by_risk = ctx.max_risk_per_trade * equity_sol
        if reinvest_sol > max_by_risk:
            logger.info(
                "[REINVEST] Capping reinvest by max_risk_per_trade: "
                "requested=%.6f, cap=%.6f (equity=%.4f SOL, max_risk=%.3f)",
                reinvest_sol,
                max_by_risk,
                equity_sol,
                ctx.max_risk_per_trade,
            )
            reinvest_sol = max_by_risk
            savings_sol = max(0.0, proceeds_sol - reinvest_sol)

    # Optional: USD hard cap
    if REINVEST_MAX_SINGLE_USD > 0:
        usd_for_full = await _sol_to_usd(reinvest_sol)
        if usd_for_full > REINVEST_MAX_SINGLE_USD:
            if usd_for_full > 0:
                scale_down = REINVEST_MAX_SINGLE_USD / max(usd_for_full, 1e-9)
                reinvest_sol *= scale_down
                reinvest_sol = max(0.0, reinvest_sol)
                savings_sol = max(0.0, proceeds_sol - reinvest_sol)
                logger.info(
                    "[REINVEST] Capped by REINVEST_MAX_SINGLE_USD: "
                    "usd_full=%.2f, cap=%.2f, new_reinvest=%.6f SOL",
                    usd_for_full,
                    REINVEST_MAX_SINGLE_USD,
                    reinvest_sol,
                )

    logger.info(
        "[REINVEST] proceeds=%.6f SOL (~$%.2f) | base_pct=%.1f%% | eff_pct=%.1f%% "
        "| risk_capacity=%.3f | risk_mode=%d | strat_mode=%d | "
        "kelly=%.3f | reinvest=%.6f SOL | savings=%.6f SOL | "
        "dailyPnL=%.2f%% | dailyDD=%.2f%% | plan_status=%s (gap=%.2f%%) "
        "| regime=(%.2f, stress=%.2f, signal=%.2f, gate=%.2f) | ai_cap_delta=%.3f "
        "| infra_health=%.2f (halted=%s, degraded=%s) | sandbox_mode=%s "
        "| health_score=%.1f | dailyTrades=%d/%d",
        proceeds_sol,
        proceeds_usd,
        ctx.base_reinvest_pct,
        eff_pct,
        risk_capacity,
        risk_mode,
        strat_mode,
        ctx.kelly_suggestion,
        reinvest_sol,
        savings_sol,
        daily_pnl_pct,
        daily_dd_pct,
        ctx.plan_status,
        ctx.plan_gap_pct,
        regime_label,
        regime_stress,
        regime_signal,
        regime_gate,
        ai_capacity_delta,
        infra_health_score,
        infra_halted,
        infra_degraded,
        ctx.sandbox_mode,
        health_score,
        daily_trades_count,
        daily_trades_cap,
    )

    # If reinvest leg is too tiny, favour savings
    if reinvest_sol < REINVESTMENT_MIN_PROFIT_SOL:
        logger.info(
            "[REINVEST] Reinvest leg %.6f SOL below threshold %.6f SOL; "
            "sending all to savings.",
            reinvest_sol,
            REINVESTMENT_MIN_PROFIT_SOL,
        )
        await _journal_skip(
            "reinvest_leg_too_small", proceeds_sol, ctx, meta, redis=redis
        )
        await _send_to_savings(proceeds_sol)
        await _update_reinvest_health(
            redis, ctx, eff_pct=0.0, reinvest_sol=0.0, savings_sol=proceeds_sol
        )
        return None

    # Move savings portion first (best-effort; doesn't block trade if it fails)
    if savings_sol > 0:
        await _send_to_savings(savings_sol)

    # Dry-run mode: log + savings, but do not trade
    if meta.get("dry_run"):
        logger.info(
            "[REINVEST] dry_run=1 — skipping actual buy; would reinvest %.6f SOL.",
            reinvest_sol,
        )
        await _update_reinvest_health(redis, ctx, eff_pct, reinvest_sol, savings_sol)
        return None

    # ------------------------------------------------------------------ #
    # Select target mint
    # ------------------------------------------------------------------ #
    new_mint = await _pick_target_mint(reinvest_sol, meta=meta, redis=redis)
    if not new_mint:
        await send_telegram(
            "⚠️ Reinvestment skipped — no suitable target met filters right now."
        )
        logger.info("[REINVEST] No target candidate returned by pickers.")
        await _journal_skip(
            "no_target", proceeds_sol, ctx, meta, redis=redis
        )
        await _update_reinvest_health(
            redis, ctx, eff_pct=0.0, reinvest_sol=0.0, savings_sol=proceeds_sol
        )
        try:
            await note_reinvest_result(
                mint=None,
                proceeds_sol=proceeds_sol,
                reinvest_sol=0.0,
                savings_sol=proceeds_sol,
                eff_pct=0.0,
                success=False,
                tx_sig=None,
                ctx={"health_score": health_score},
                redis=redis,
            )
        except Exception:
            pass
        return None

    # ------------------------------------------------------------------ #
    # Execute buy via Jupiter
    # ------------------------------------------------------------------ #
    try:
        ok, tx_sig = await swap_token_jupiter(
            SOL_MINT,
            new_mint,
            amount_float=reinvest_sol,
            buy=True,
            slippage=ctx.slippage,
        )
    except TypeError:
        ok, tx_sig = await swap_token_jupiter(
            SOL_MINT,
            new_mint,
            reinvest_sol,
            True,
            ctx.slippage,
        )

    if not ok:
        await send_telegram(f"❌ Reinvestment buy failed for {new_mint}.")
        logger.warning("[REINVEST] Swap failed for %s", new_mint)
        await _journal_skip(
            "swap_failed", proceeds_sol, ctx, meta, redis=redis
        )
        await _update_reinvest_health(
            redis, ctx, eff_pct=0.0, reinvest_sol=0.0, savings_sol=proceeds_sol
        )
        try:
            await note_reinvest_result(
                mint=new_mint,
                proceeds_sol=proceeds_sol,
                reinvest_sol=0.0,
                savings_sol=proceeds_sol,
                eff_pct=0.0,
                success=False,
                tx_sig=None,
                ctx={"health_score": health_score},
                redis=redis,
            )
        except Exception:
            pass
        return None

    # Price in SOL per token (for consistent position schema)
    buy_price_sol = await _token_price_in_sol(new_mint)
    trailing_stop = (
        buy_price_sol * (1.0 - (ctx.stop_loss_percent / 100.0))
        if buy_price_sol > 0
        else 0.0
    )

    now_iso = datetime.now(timezone.utc).isoformat()

    position_payload: Dict[str, Any] = {
        "token_address": new_mint,
        "buy_price": float(buy_price_sol),
        "amount_invested_sol": float(reinvest_sol),
        "timestamp": now_iso,
        "trailing_stop": float(trailing_stop),
        "source": meta.get("tag") or "reinvestor",
        "origin_trade_id": meta.get("origin_trade_id"),
        "origin_position_id": meta.get("origin_position_id"),
        "meta": {
            **ctx.meta,
            "reinvest_pct": eff_pct,
            "risk_capacity": risk_capacity,
            "risk_mode": risk_mode,
            "risk_mode_override": ctx.risk_mode_override,
            "strat_mode": strat_mode,
            "kelly_suggestion": ctx.kelly_suggestion,
            "kelly_confidence": ctx.kelly_confidence,
            "daily_realized_pnl_pct": daily_pnl_pct,
            "daily_drawdown_pct": daily_dd_pct,
            "plan_status": ctx.plan_status,
            "plan_gap_pct": ctx.plan_gap_pct,
            "regime_label": ctx.regime_label,
            "regime_label_numeric": regime_label,
            "regime_stress_score": regime_stress,
            "regime_signal_strength": regime_signal,
            "regime_entry_gate_factor": regime_gate,
            "ai_capacity_delta": ai_capacity_delta,
            "sandbox_mode": ctx.sandbox_mode,
            "health_score": health_score,
            "daily_trades_count": daily_trades_count,
            "daily_trades_cap": daily_trades_cap,
            "infra_health_score": infra_health_score,
            "infra_state_is_halted": infra_halted,
            "infra_state_is_degraded": infra_degraded,
            "infra_rpc_latency_ms": ctx.infra_rpc_latency_ms,
            "infra_jup_latency_ms": ctx.infra_jup_latency_ms,
            "infra_stability_score": ctx.infra_stability_score,
            "scales": scales.as_dict(),
        },
    }
    save_position(new_mint, position_payload)

    # Count reinvest spend toward daily budget (using USD)
    try:
        usd_spent = await _sol_to_usd(reinvest_sol)
        if usd_spent > 0:
            await note_spend(redis, usd_spent)
    except Exception:
        pass

    # Bump daily reinvest counter (for cap)
    await _bump_daily_reinvest_counter(redis)
    # Update last reinvest timestamp (for time-based throttling)
    await _set_last_reinvest_ts(redis)

    # Alerts / journaling
    usd = await _sol_to_usd(reinvest_sol)

    await send_telegram(
        "🔁 *Reinvestment executed*\n"
        f"• Token: `{new_mint}`\n"
        f"• Size: {reinvest_sol:.4f} SOL (~${usd:.2f})\n"
        f"• Entry(px SOL): {buy_price_sol:.10f}\n"
        f"• SL%: {ctx.stop_loss_percent:.1f}  • Risk cap: {risk_capacity:.3f} "
        f"(risk_mode={risk_mode}, strat_mode={strat_mode})\n"
        f"• Kelly: {ctx.kelly_suggestion:.3f} (conf={ctx.kelly_confidence:.2f})  "
        f"WR={ctx.win_rate:.2%}  μ={ctx.pnl_mean:+.2f}% σ={ctx.pnl_std:.2f}%\n"
        f"• DailyPnL={daily_pnl_pct:+.2f}%  DD={daily_dd_pct:.2f}%  "
        f"Plan={ctx.plan_status} (gap={ctx.plan_gap_pct:+.2f}%)\n"
        f"• Regime: label={ctx.regime_label} num={regime_label:+.2f}, "
        f"stress={regime_stress:.2f}, signal={regime_signal:.2f}, gate={regime_gate:.2f}  "
        f"(ai_cap_delta={ai_capacity_delta:+.3f}, sandbox={ctx.sandbox_mode})\n"
        f"• Infra: health={infra_health_score:.2f}, "
        f"rpc={ctx.infra_rpc_latency_ms:.0f}ms, jup={ctx.infra_jup_latency_ms:.0f}ms, "
        f"halted={infra_halted}, degraded={infra_degraded}\n"
        f"• Health score: {health_score:.1f} | DailyTrades={daily_trades_count}/{daily_trades_cap}\n"
        f"• Tx: `{tx_sig}`"
    )

    try:
        await log_to_notion(
            token=new_mint,
            symbol=new_mint[:6],
            balance=reinvest_sol,
            entry=buy_price_sol,
            current=buy_price_sol,
            profit=0.0,
            reason=(
                "Reinvestment buy "
                f"(eff_pct={eff_pct:.1f}%, cap={risk_capacity:.3f}, "
                f"kelly={ctx.kelly_suggestion:.3f}, dd={daily_dd_pct:.2f}%, "
                f"plan={ctx.plan_status}, gap={ctx.plan_gap_pct:+.2f}%, "
                f"regime={ctx.regime_label}:{regime_label:+.2f}/"
                f"{regime_stress:.2f}/{regime_signal:.2f}, ai_cap_delta={ai_capacity_delta:+.3f}, "
                f"infra_health={infra_health_score:.2f}, rpc={ctx.infra_rpc_latency_ms:.0f}ms, "
                f"jup={ctx.infra_jup_latency_ms:.0f}ms, sandbox={ctx.sandbox_mode}, "
                f"health_score={health_score:.1f}, "
                f"dailyTrades={daily_trades_count}/{daily_trades_cap}, "
                f"scales={scales.as_dict()})"
            ),
        )
    except Exception:
        pass

    # Structured data log
    try:
        await log_event(
            "reinvest_executed",
            {
                "mint": new_mint,
                "proceeds_sol": proceeds_sol,
                "reinvest_sol": reinvest_sol,
                "savings_sol": savings_sol,
                "eff_pct": eff_pct,
                "health_score": health_score,
                "infra_health_score": infra_health_score,
                "infra": ctx.infra_raw,
                "scales": scales.as_dict(),
                "params": {
                    "STOP_LOSS_PERCENT": ctx.stop_loss_percent,
                    "SLIPPAGE": ctx.slippage,
                    "MIN_SOL_BALANCE": ctx.min_sol_balance,
                    "MAX_RISK_PER_TRADE": ctx.max_risk_per_trade,
                    "REINVEST_MIN_CAPACITY": REINVEST_MIN_CAPACITY,
                    "REINVEST_DD_SOFT_STOP_PCT": REINVEST_DD_SOFT_STOP_PCT,
                    "REINVEST_DD_HARD_STOP_PCT": REINVEST_DD_HARD_STOP_PCT,
                    "REINVEST_MAX_SINGLE_USD": REINVEST_MAX_SINGLE_USD,
                    "PLAN_TARGET_APR_PCT": ctx.plan_target_apr_pct,
                    "PLAN_GAP_PCT": ctx.plan_gap_pct,
                    "PLAN_STATUS": ctx.plan_status,
                    "STRAT_MODE": ctx.strat_mode,
                    "HARD_HALT": ctx.hard_halt,
                    "REINVEST_ALWAYS_SAVINGS_PCT": REINVEST_ALWAYS_SAVINGS_PCT,
                    "REINVEST_MIN_EQUITY_SOL": REINVEST_MIN_EQUITY_SOL,
                    "REINVEST_MIN_MINUTES_BETWEEN_REINVESTS": REINVEST_MIN_MINUTES_BETWEEN_REINVESTS,
                    "REINVEST_MAX_REINVESTS_PER_DAY": REINVEST_MAX_REINVESTS_PER_DAY,
                    "REINVEST_HEALTH_MIN_SCORE": REINVEST_HEALTH_MIN_SCORE,
                    "REINVEST_PNL_LOCK_PCT": REINVEST_PNL_LOCK_PCT,
                    "REINVEST_INFRA_MIN_HEALTH": REINVEST_INFRA_MIN_HEALTH,
                    "REINVEST_BLOCK_IF_INFRA_HALTED": REINVEST_BLOCK_IF_INFRA_HALTED,
                    "REINVEST_BLOCK_IF_INFRA_DEGRADED": REINVEST_BLOCK_IF_INFRA_DEGRADED,
                    "REINVESTMENT_MIN_PROFIT_USD": REINVESTMENT_MIN_PROFIT_USD,
                },
                "risk_ctx": ctx.risk_ctx_raw,
                "perf": ctx.perf_raw,
                "tx_sig": tx_sig,
                "meta": ctx.meta,
            },
        )
    except Exception:
        pass

    await _update_reinvest_health(redis, ctx, eff_pct, reinvest_sol, savings_sol)

    # Self-learning hook: record successful result
    try:
        await note_reinvest_result(
            mint=new_mint,
            proceeds_sol=proceeds_sol,
            reinvest_sol=reinvest_sol,
            savings_sol=savings_sol,
            eff_pct=eff_pct,
            success=True,
            tx_sig=tx_sig,
            ctx={
                "risk_capacity": risk_capacity,
                "risk_mode": risk_mode,
                "daily_pnl_pct": daily_pnl_pct,
                "daily_dd_pct": daily_dd_pct,
                "equity_sol": equity_sol,
                "sandbox_mode": ctx.sandbox_mode,
                "health_score": health_score,
                "infra_health_score": infra_health_score,
                "daily_trades_count": daily_trades_count,
                "daily_trades_cap": daily_trades_cap,
            },
            redis=redis,
        )
    except Exception:
        pass

    logger.info("[REINVEST] ✅ Completed into %s (tx=%s)", new_mint, tx_sig)
    return new_mint


# ============================================================================ #
# Queue & worker
# ============================================================================ #


async def queue_reinvest(
    proceeds_sol: float,
    meta: Optional[Dict[str, Any]] = None,
    redis=None,
) -> None:
    """
    Non-blocking: push a reinvest job onto a Redis queue.

    If Redis is unavailable, falls back to direct `reinvest(...)`.
    """
    if proceeds_sol is None or proceeds_sol <= 0:
        return

    if redis is None:
        redis = await _get_redis()
    if not redis:
        # No Redis → run synchronously (still risk-aware)
        await reinvest(proceeds_sol, meta=meta, redis=None)
        return

    job = {"proceeds_sol": float(proceeds_sol), "meta": meta or {}}
    await redis.rpush(REINVEST_QUEUE, json.dumps(job))
    logger.info("[REINVEST] Queued job: %s", job)


async def run_reinvest_worker(
    redis=None,
    poll_interval: float = 2.0,
    stop_evt: Optional[asyncio.Event] = None,
) -> None:
    """
    Background worker: continuously pops jobs from REINVEST_QUEUE and
    runs `reinvest(...)`.

    You can fire this from scripts/main.py as:

        from utils_pkg.reinvestor import run_reinvest_worker
        ...
        asyncio.create_task(
            run_reinvest_worker(redis, poll_interval=2.0, stop_evt=stop_evt),
            name="reinvest.worker",
        )
    """
    if redis is None:
        redis = await _get_redis()
    if not redis:
        logger.warning("Reinvest worker disabled: no Redis client.")
        return

    logger.info("🔁 Reinvest worker started (poll=%.1fs)", poll_interval)

    while True:
        if stop_evt is not None and stop_evt.is_set():
            logger.info("🔁 Reinvest worker stopping on stop_evt.")
            break

        try:
            processed = 0
            # Batch processing for throughput; BLPOP handles blocking
            while processed < max(1, REINVEST_WORKER_BATCH):
                raw = await redis.blpop(
                    REINVEST_QUEUE, timeout=int(max(1, poll_interval))
                )
                if not raw:
                    break

                _, payload = raw
                try:
                    job = json.loads(payload)
                except Exception:
                    logger.warning("[REINVEST] Invalid job payload: %s", payload)
                    continue

                amt = float(job.get("proceeds_sol", 0.0))
                meta = job.get("meta") or {}

                if amt > 0:
                    await reinvest(amt, meta=meta, redis=redis)
                processed += 1

            if processed == 0:
                await asyncio.sleep(poll_interval)

        except Exception as e:
            logger.error("[REINVEST] Worker error: %s", e, exc_info=True)
            await asyncio.sleep(1.0)


# ============================================================================ #
# Preview helper (for backtests / debugging)
# ============================================================================ #


async def preview_reinvest_split(
    proceeds_sol: float,
    meta: Optional[Dict[str, Any]] = None,
    redis=None,
) -> Optional[Dict[str, Any]]:
    """
    Dry "what would happen" helper.

    It runs the same context build + eff_pct logic (no swaps, no savings move).

    It returns a JSON-friendly dict describing:
        - outcome: "skip" | "savings_only" | "reinvest"
        - reason (for skip/savings_only)
        - eff_pct_raw / eff_pct_final
        - reinvest_sol / savings_sol
        - health_score
        - scales breakdown
        - compact context excerpt
    """
    meta = dict(meta or {})
    meta.setdefault("sandbox", True)  # ensure we never trade

    if redis is None:
        redis = await _get_redis()

    ctx = await _build_context(proceeds_sol, meta, redis)
    if ctx is None:
        return {
            "outcome": "skip",
            "reason": "context_build_failed",
        }

    health_score = _compute_health_score(ctx)
    eff_pct_raw, scales = _compute_eff_reinvest_pct(ctx)
    eff_pct = _apply_meta_eff_overrides(eff_pct_raw, ctx.meta)

    reinvest_sol = proceeds_sol * (eff_pct / 100.0)
    savings_sol = max(0.0, proceeds_sol - reinvest_sol)

    outcome = "reinvest"
    reason = None
    if proceeds_sol < REINVESTMENT_MIN_PROFIT_SOL:
        outcome = "savings_only"
        reason = "below_min_profit_sol"
    elif reinvest_sol < REINVESTMENT_MIN_PROFIT_SOL:
        outcome = "savings_only"
        reason = "reinvest_leg_too_small"

    return {
        "outcome": outcome,
        "reason": reason,
        "proceeds_sol": proceeds_sol,
        "eff_pct_raw": eff_pct_raw,
        "eff_pct_final": eff_pct,
        "reinvest_sol": reinvest_sol,
        "savings_sol": savings_sol,
        "health_score": health_score,
        "infra_health_score": ctx.infra_health_score,
        "scales": scales.as_dict(),
        "ctx": {
            "risk_capacity": ctx.risk_capacity,
            "risk_mode": ctx.risk_mode,
            "daily_pnl_pct": ctx.daily_pnl_pct,
            "daily_dd_pct": ctx.daily_dd_pct,
            "equity_sol": ctx.equity_sol,
            "plan_status": ctx.plan_status,
            "plan_gap_pct": ctx.plan_gap_pct,
            "daily_trades_count": ctx.daily_trades_count,
            "daily_trades_cap": ctx.daily_trades_cap,
            "infra_state_is_halted": ctx.infra_state_is_halted,
            "infra_state_is_degraded": ctx.infra_state_is_degraded,
        },
    }


# ============================================================================ #
# Human-friendly explanation helper
# ============================================================================ #


async def explain_reinvest_decision(
    proceeds_sol: float,
    meta: Optional[Dict[str, Any]] = None,
    redis=None,
) -> Optional[Dict[str, Any]]:
    """
    High-level "why" helper for dashboards / CLI / debugging.

    Uses preview_reinvest_split(...) under the hood and adds a compact
    explanation string plus top boosting/suppressing scales.
    """
    preview = await preview_reinvest_split(proceeds_sol, meta=meta, redis=redis)
    if not preview:
        return None

    outcome = preview.get("outcome")
    reason = preview.get("reason")
    scales = (preview.get("scales") or {}) if isinstance(preview, dict) else {}

    # Identify top boosters / suppressors among scales
    base_pct = scales.get("base_pct", 0.0)
    deltas: Dict[str, float] = {}
    for k in (
        "cap_scale",
        "dd_scale",
        "kelly_scale",
        "plan_scale",
        "regime_scale",
        "ai_scale",
        "perf_scale",
        "profile_scale",
        "infra_scale",
    ):
        v = float(scales.get(k, 1.0))
        deltas[k] = v - 1.0

    boost = sorted(
        [(k, v) for k, v in deltas.items() if v > 0],
        key=lambda kv: kv[1],
        reverse=True,
    )[:2]
    suppress = sorted(
        [(k, v) for k, v in deltas.items() if v < 0],
        key=lambda kv: kv[1],
    )[:2]

    def _fmt_scale(name: str) -> str:
        name = name.replace("_scale", "")
        return name

    boosts_text = ", ".join(
        f"{_fmt_scale(k)}({v:+.2f})" for k, v in boost
    ) or "none"
    suppress_text = ", ".join(
        f"{_fmt_scale(k)}({v:+.2f})" for k, v in suppress
    ) or "none"

    eff_pct = preview.get("eff_pct_final", 0.0)
    health = preview.get("health_score", 0.0)
    infra_health = preview.get("infra_health_score", 0.0)
    ctx = preview.get("ctx") or {}

    explanation = (
        f"Outcome: {outcome} "
        f"(reason={reason or 'n/a'}), "
        f"base={base_pct:.1f}%, eff={eff_pct:.1f}%, "
        f"health={health:.1f}, infra_health={infra_health:.1f}. "
        f"Top boosts: {boosts_text}. "
        f"Top suppressors: {suppress_text}. "
        f"RiskCap={ctx.get('risk_capacity', 0):.2f}, "
        f"DD={ctx.get('daily_dd_pct', 0):.2f}%, "
        f"plan_gap={ctx.get('plan_gap_pct', 0):+.2f}%, "
        f"infra_halted={bool(ctx.get('infra_state_is_halted', 0))}, "
        f"infra_degraded={bool(ctx.get('infra_state_is_degraded', 0))}."
    )

    preview["explanation"] = explanation
    preview["boosts"] = boost
    preview["suppressors"] = suppress
    return preview


# ============================================================================ #
# CLI / manual test
# ============================================================================ #

if __name__ == "__main__":  # simple local demo
    async def _demo():
        r = await _get_redis()
        # Queue a dry-run job
        await queue_reinvest(0.05, {"reason": "demo", "dry_run": True}, redis=r)
        # Also print a local explanation
        exp = await explain_reinvest_decision(
            0.05,
            {"reason": "demo", "sandbox": True},
            redis=r,
        )
        print("Explain:", json.dumps(exp, indent=2, default=str))

        stop = asyncio.Event()

        async def stop_later():
            await asyncio.sleep(5)
            stop.set()

        asyncio.create_task(stop_later())
        await run_reinvest_worker(r, poll_interval=1.0, stop_evt=stop)

    asyncio.run(_demo())
