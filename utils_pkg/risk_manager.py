"""
Risk manager — tiered sizing, governance & metrics (v3, wired for main)

Adds/updates:
- Equity-aware tier schedule (ticket size, pos% cap, max concurrency, daily spend cap)
- Adaptive throttle (cuts sizing when winrate-20 < 45% or rolling DD > 8%)
- Stop-after-profit + daily spend budget via Redis (resets at UTC midnight)
- Infra-halt gate (honors infra_guard's redis flag)
- Metrics: daily realized PnL, winrate-20, rolling drawdown vs equity HWM
- Exported risk_manager_loop(stop_evt) for scripts/main.py

Public helpers:
    await current_limits(redis) -> dict(ticket_usd, pos_pct_cap, max_concurrent, daily_spend_usd, equity_usd)
    await should_allow_new_entry(redis) -> bool
    await note_spend(redis, usd)
"""

from __future__ import annotations

import os
import asyncio
import logging
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timezone, timedelta

# Optional standalone redis client (fallback if core_utils.redis is None)
try:
    import redis.asyncio as aioredis
except Exception:  # pragma: no cover
    aioredis = None  # type: ignore[assignment]

# Local positions / strategy helpers (your existing modules)
from positions import load_positions, save_position, close_position as remove_position
from reinvestor import queue_reinvest
from self_learning_ai_strategy import get_strategy_params, process_trade_closure

# Core utils (shared Redis client + infra)
from utils_pkg.core_utils import (
    get_token_price,
    get_wallet_tokens,
    swap_token_jupiter,
    convert_sol_to_usd,
    send_telegram,
    log_to_notion,
    redis as shared_redis,
)

logger = logging.getLogger("risk_manager")
logging.basicConfig(level=logging.INFO)

# -------------------------
# Constants & ENV
# -------------------------
SOL_MINT = "So11111111111111111111111111111111111111112"

TRAILING_STOP_ENABLED = os.getenv("TRAILING_STOP_ENABLED", "true").lower() == "true"
DEFAULT_STOP_LOSS_PERCENT = float(os.getenv("STOP_LOSS_PERCENT", "12"))
DEFAULT_TARGET_PROFIT_PERCENT = float(os.getenv("TARGET_PROFIT_PERCENT", "0"))  # 0 = disabled
DEFAULT_MAX_HOLD_MINUTES = int(os.getenv("MAX_HOLD_TIME_MINUTES", "240"))
DEFAULT_SLIPPAGE = float(os.getenv("JUPITER_MAX_SLIPPAGE", "0.005"))  # fraction

POSITION_CHECK_INTERVAL = int(os.getenv("POSITION_MONITOR_INTERVAL", "20"))
ALERT_COOLDOWN_SECONDS = int(os.getenv("ALERT_COOLDOWN_SECONDS", "900"))
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
WALLET_PUBKEY = (
    os.getenv("WALLET_PUBKEY")
    or os.getenv("WALLET_PUBLIC_KEY")
    or os.getenv("WALLET_ADDRESS")
)

# Daily governance
STOP_AFTER_DAILY_PROFIT_PCT = float(os.getenv("STOP_AFTER_DAILY_PROFIT_PCT", "4"))   # % of equity
DAILY_SPEND_CAP_PCT = float(os.getenv("DAILY_SPEND_CAP_PCT", "0.15"))                # % of equity

# Baseline caps (tiering can be tighter)
RISK_MAX_PER_TRADE = float(os.getenv("RISK_MAX_PER_TRADE", "0.045"))
MAX_CONCURRENT_POS = int(os.getenv("MAX_CONCURRENT_POS", "4"))

# Adaptive throttle thresholds
THROTTLE_WINRATE_FLOOR = 0.45     # reduce sizing if 20-trade winrate < 45%
THROTTLE_DD_FLOOR = 0.08          # or rolling drawdown > 8%
THROTTLE_TICKET_CUT = 0.40        # cut ticket by 40%
THROTTLE_CONC_DEC = 1             # minus one slot

# Tier schedule (targets ~16.5–18%/mo with discipline)
TIER_SCHEDULE: List[Dict[str, Any]] = [
    {"equity_to": 2500,   "ticket": 50,   "pos_pct": 0.025, "max_conc": 3,  "spend_pct": 0.10},
    {"equity_to": 5000,   "ticket": 100,  "pos_pct": 0.030, "max_conc": 4,  "spend_pct": 0.12},
    {"equity_to": 10000,  "ticket": 175,  "pos_pct": 0.035, "max_conc": 5,  "spend_pct": 0.14},
    {"equity_to": 25000,  "ticket": 325,  "pos_pct": 0.040, "max_conc": 6,  "spend_pct": 0.15},
    {"equity_to": 50000,  "ticket": 600,  "pos_pct": 0.045, "max_conc": 7,  "spend_pct": 0.16},
    {"equity_to": 100000, "ticket": 1000, "pos_pct": 0.045, "max_conc": 8,  "spend_pct": 0.18},
    {"equity_to": 500000, "ticket": 2500, "pos_pct": 0.050, "max_conc": 10, "spend_pct": 0.20},
    {"equity_to": 10**12, "ticket": 0,    "pos_pct": 0.050, "max_conc": 10, "spend_pct": 0.20},  # depth-driven
]

# -------------------------
# Redis keys & helpers
# -------------------------
def _key_daily(prefix: str) -> str:
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    return f"{prefix}:{today}"

def _pnl_key_today() -> str:
    return _key_daily("pnl:day:realized_usd")

BUDGET_KEY = _key_daily("budget:sniper:spent_usd")

# Metrics keys
METRICS_LAST20_KEY = "metrics:last20"          # list of 1/0
METRICS_TRADES_KEY = "metrics:trades_total"
METRICS_WINS_KEY = "metrics:wins_total"
METRICS_WINRATE20 = "metrics:winrate20"
METRICS_ROLLING_DD = "metrics:rolling_dd_pct"
EQUITY_HWM_KEY = "metrics:equity_hwm_usd"

# Local cooldowns for sell alerts per-mint
_COOLDOWNS: Dict[str, float] = {}

async def _get_redis(url: Optional[str] = None):
    """
    Prefer the shared Redis client from core_utils; fallback to a local aioredis
    client if needed, or return None if Redis is unavailable.
    """
    if shared_redis is not None:
        return shared_redis
    if aioredis is None:
        logger.warning("redis (async) not installed; running without Redis-backed budgets/metrics.")
        return None
    return aioredis.from_url(url or REDIS_URL, decode_responses=True)

def _now_ts() -> float:
    return datetime.now(timezone.utc).timestamp()

def _midnight_utc() -> datetime:
    now = datetime.now(timezone.utc)
    return (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

async def _set_expire_at_midnight(redis, key: str):
    if not redis:
        return
    ttl = int((_midnight_utc() - datetime.now(timezone.utc)).total_seconds())
    await redis.expire(key, ttl)

# -------------------------
# Equity & metrics
# -------------------------
async def _wallet_equity_usd() -> float:
    """Estimate total equity in USD using wallet balances & token prices."""
    if not WALLET_PUBKEY:
        return 0.0
    try:
        tokens = await get_wallet_tokens(WALLET_PUBKEY)  # list of {mint, balance}
        total_sol = 0.0
        for t in tokens or []:
            mint = t.get("mint")
            bal = float(t.get("balance", 0.0) or 0.0)
            if bal <= 0:
                continue
            if mint == SOL_MINT:
                total_sol += bal
            else:
                px_in_sol = await get_token_price(mint)  # token price in SOL
                if px_in_sol and px_in_sol > 0:
                    total_sol += bal * px_in_sol
        usd = await convert_sol_to_usd(total_sol)
        return float(usd or 0.0)
    except Exception as e:
        logger.warning(f"equity calc failed: {e}")
        return 0.0

async def _metrics(redis) -> Tuple[float, float]:
    """Return (winrate20, rolling_dd_pct). Defaults if absent."""
    try:
        if redis is None:
            redis = await _get_redis()
        if not redis:
            return 0.50, 0.0
        wr = await redis.get(METRICS_WINRATE20)
        dd = await redis.get(METRICS_ROLLING_DD)
        winrate = float(wr) if wr is not None else 0.50
        drawdown = float(dd) if dd is not None else 0.0
        return winrate, drawdown
    except Exception:
        return 0.50, 0.0

def _select_tier(equity_usd: float) -> Dict[str, Any]:
    for tier in TIER_SCHEDULE:
        if equity_usd <= tier["equity_to"]:
            return dict(tier)
    return dict(TIER_SCHEDULE[-1])

async def current_limits(redis=None) -> Dict[str, Any]:
    """Compute risk limits from equity + tier + throttle."""
    if redis is None:
        redis = await _get_redis()
    equity = await _wallet_equity_usd()
    tier = _select_tier(equity)

    # Base tier values
    ticket = float(tier["ticket"]) if tier["ticket"] else max(50.0, equity * tier["pos_pct"] * 0.5)
    pos_pct_cap = float(tier["pos_pct"])
    max_conc = int(tier["max_conc"])
    daily_spend_usd = equity * float(tier["spend_pct"])

    # Adaptive throttle
    winrate, dd = await _metrics(redis)
    if (winrate < THROTTLE_WINRATE_FLOOR) or (dd > THROTTLE_DD_FLOOR):
        ticket *= (1.0 - THROTTLE_TICKET_CUT)
        max_conc = max(1, max_conc - THROTTLE_CONC_DEC)

    # Respect env caps
    pos_pct_cap = min(pos_pct_cap, RISK_MAX_PER_TRADE)
    max_conc = min(max_conc, MAX_CONCURRENT_POS)
    daily_spend_usd = min(daily_spend_usd, equity * DAILY_SPEND_CAP_PCT)

    # Round ticket nicely
    if ticket < 10:
        ticket = max(5.0, round(ticket, 2))
    elif ticket < 100:
        ticket = round(ticket / 5.0) * 5.0
    else:
        ticket = round(ticket / 10.0) * 10.0

    return {
        "equity_usd": float(equity),
        "ticket_usd": float(ticket),
        "pos_pct_cap": float(pos_pct_cap),
        "max_concurrent": int(max_conc),
        "daily_spend_usd": float(daily_spend_usd),
    }

async def _daily_realized_usd(redis) -> float:
    try:
        if redis is None:
            redis = await _get_redis()
        if not redis:
            return 0.0
        val = await redis.get(_pnl_key_today())
        return float(val) if val else 0.0
    except Exception:
        return 0.0

async def _push_trade_metrics(redis, profit_usd: float, equity_after: float):
    """Update totals, rolling winrate-20, rolling drawdown vs equity HWM."""
    if not redis:
        return
    try:
        await redis.incr(METRICS_TRADES_KEY)
        if profit_usd > 0:
            await redis.incr(METRICS_WINS_KEY)
        await redis.lpush(METRICS_LAST20_KEY, 1 if profit_usd > 0 else 0)
        await redis.ltrim(METRICS_LAST20_KEY, 0, 19)
        last20 = await redis.lrange(METRICS_LAST20_KEY, 0, 19)
        wins20 = sum(int(x) for x in last20)
        winrate20 = wins20 / max(1, len(last20))
        await redis.set(METRICS_WINRATE20, f"{winrate20:.4f}")

        # rolling DD vs equity HWM
        hwm_raw = await redis.get(EQUITY_HWM_KEY)
        hwm = float(hwm_raw) if hwm_raw else equity_after
        if equity_after > hwm:
            hwm = equity_after
        await redis.set(EQUITY_HWM_KEY, f"{hwm:.2f}")
        dd = 0.0 if hwm <= 0 else (hwm - equity_after) / hwm
        await redis.set(METRICS_ROLLING_DD, f"{dd:.4f}")
    except Exception:
        pass

async def should_allow_new_entry(redis=None) -> bool:
    """
    Gate new entries by:
      - infra halt (from infra_guard)
      - daily realized PnL ≥ STOP_AFTER_DAILY_PROFIT_PCT * equity
      - daily spend budget exhausted
    """
    if redis is None:
        redis = await _get_redis()
    limits = await current_limits(redis)
    equity = limits["equity_usd"]
    daily_spend = limits["daily_spend_usd"]

    # Infra-halt (set by infra_guard.py)
    try:
        if redis:
            halted = await redis.get("infra:halt")
            if halted == "1":
                return False
    except Exception:
        pass

    # Stop-after-profit
    realized = await _daily_realized_usd(redis)
    profit_stop_usd = equity * (STOP_AFTER_DAILY_PROFIT_PCT / 100.0)
    if realized >= profit_stop_usd:
        return False

    # Daily spend budget
    if redis:
        spent = await redis.get(BUDGET_KEY)
        spent_val = float(spent) if spent else 0.0
        if spent_val >= daily_spend:
            return False

    return True

async def note_spend(redis, usd: float):
    """Increment daily spend; create the key with TTL to midnight if missing."""
    if redis is None:
        redis = await _get_redis()
    if not redis:
        return
    try:
        await redis.incrbyfloat(BUDGET_KEY, float(usd))
        await _set_expire_at_midnight(redis, BUDGET_KEY)
    except Exception:
        pass

# -------------------------
# Sell-side logic (kept; adds metrics & realized PnL)
# -------------------------
def _cooldown_ok(mint: str) -> bool:
    last = _COOLDOWNS.get(mint, 0.0)
    if _now_ts() - last >= ALERT_COOLDOWN_SECONDS:
        _COOLDOWNS[mint] = _now_ts()
        return True
    return False

async def _find_wallet_token_balance(mint: str) -> float:
    if not WALLET_PUBKEY:
        logger.warning("WALLET_PUBKEY not set; cannot query wallet token balances.")
        return 0.0
    try:
        tokens = await get_wallet_tokens(WALLET_PUBKEY)
        for t in tokens or []:
            if t.get("mint") == mint:
                return float(t.get("balance", 0.0))
    except Exception as e:
        logger.warning(f"get_wallet_tokens failed: {e}")
    return 0.0

def _parse_buy_time(position: Dict[str, Any]) -> Optional[datetime]:
    ts = position.get("timestamp")
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None

async def _sell_all_token(mint: str, token_balance: float, slippage: float) -> tuple[bool, str]:
    if token_balance <= 0:
        return False, ""
    try:
        ok, tx_sig = await swap_token_jupiter(
            mint,
            SOL_MINT,
            amount_float=token_balance,
            buy=False,
            slippage=slippage,
        )
        return ok, tx_sig
    except TypeError:
        ok, tx_sig = await swap_token_jupiter(mint, SOL_MINT, token_balance, False, slippage)
        return ok, tx_sig

async def handle_position(token_address: str, position: Dict[str, Any], redis=None) -> None:
    """
    Core per-position risk logic:
      - trailing stop maintenance
      - stop-loss / target-profit / max-hold exits
      - sell via Jupiter
      - metrics + PnL logging + reinvest queue
    """
    try:
        if redis is None:
            redis = await _get_redis()

        params = await get_strategy_params(redis)
        stop_loss_pct = float(params.get("STOP_LOSS_PERCENT", DEFAULT_STOP_LOSS_PERCENT))
        target_profit_pct = float(params.get("TARGET_PROFIT_PERCENT", DEFAULT_TARGET_PROFIT_PERCENT))
        max_hold_minutes = int(params.get("MAX_HOLD_TIME_MINUTES", DEFAULT_MAX_HOLD_MINUTES))
        slippage = float(params.get("SLIPPAGE", DEFAULT_SLIPPAGE))
        trailing_on = bool(params.get("TRAILING_STOP_ENABLED", TRAILING_STOP_ENABLED))

        buy_price = float(position.get("buy_price", 0.0))  # price in SOL
        if buy_price <= 0:
            return

        current_price = await get_token_price(token_address)  # price in SOL
        if current_price is None or current_price <= 0:
            return

        # Maintain trailing stop on new highs
        trailing_stop = float(
            position.get("trailing_stop", buy_price * (1 - stop_loss_pct / 100))
        )
        if trailing_on and current_price > buy_price:
            new_trailing = current_price * (1 - stop_loss_pct / 100)
            if new_trailing > trailing_stop:
                trailing_stop = new_trailing
                position["trailing_stop"] = trailing_stop
                save_position(token_address, position)
                logger.info(f"[TSL] {token_address} raised trailing_stop -> {trailing_stop:.10f}")

        pct_change = ((current_price - buy_price) / buy_price) * 100.0
        buy_dt = _parse_buy_time(position)
        hold_minutes = 0.0
        if buy_dt:
            hold_minutes = (datetime.now(timezone.utc) - buy_dt).total_seconds() / 60.0

        # Exit checks
        do_sell = False
        reason = ""

        if current_price <= trailing_stop:
            do_sell = True
            reason = f"Trailing/Stop-Loss hit (≤ {stop_loss_pct:.1f}%)"

        if not do_sell and target_profit_pct > 0 and pct_change >= target_profit_pct:
            do_sell = True
            reason = f"Target profit reached (≥ {target_profit_pct:.1f}%)"

        if not do_sell and max_hold_minutes > 0 and hold_minutes >= max_hold_minutes:
            do_sell = True
            reason = f"Max hold time exceeded ({hold_minutes:.0f}m)"

        if not do_sell or not _cooldown_ok(token_address):
            return

        token_balance = await _find_wallet_token_balance(token_address)
        if token_balance <= 0:
            logger.info(f"[SELL] {token_address}: no wallet balance; assuming already sold.")
            remove_position(token_address)
            return

        ok, tx_sig = await _sell_all_token(token_address, token_balance, slippage)
        if not ok:
            logger.warning(f"[SELL] Jupiter sell failed for {token_address}")
            return

        proceeds_sol = token_balance * current_price  # approximate (price in SOL)
        usd_proceeds = await convert_sol_to_usd(proceeds_sol)
        sol_usd = await convert_sol_to_usd(1.0)
        profit_usd = (current_price - buy_price) * token_balance * float(sol_usd)

        await send_telegram(
            f"🔴 Sold {token_address[:6]}… — {reason}\n"
            f"• Δ {pct_change:+.2f}%  • Proceeds ≈ {proceeds_sol:.4f} SOL (${usd_proceeds:.2f})\n"
            f"• Entry {buy_price:.10f} → Exit {current_price:.10f}\n"
            f"• Tx: {tx_sig}"
        )
        try:
            await log_to_notion(
                token=token_address,
                symbol=token_address[:6],
                balance=token_balance,
                entry=buy_price,
                current=current_price,
                profit=(current_price - buy_price) * token_balance,
                reason=reason,
            )
        except Exception:
            pass

        # Self-learning strategy closure hook
        try:
            sell_time = datetime.now(timezone.utc)
            await process_trade_closure(
                token_address=token_address,
                buy_price=buy_price,
                sell_price=current_price,
                amount=token_balance,
                partial=False,
                timestamp=sell_time.isoformat(),
            )
        except Exception as e:
            logger.debug(f"process_trade_closure failed: {e}")

        # ---- Metrics & realized PnL (with TTL to midnight) ----
        try:
            if redis:
                await redis.incrbyfloat(_pnl_key_today(), float(profit_usd))
                await _set_expire_at_midnight(redis, _pnl_key_today())
                eq_after = await _wallet_equity_usd()
                await _push_trade_metrics(redis, float(profit_usd), float(eq_after))
        except Exception:
            pass

        remove_position(token_address)

        # Queue reinvestment
        try:
            await queue_reinvest(
                proceeds_sol,
                meta={"from": "risk_manager", "reason": reason},
                redis=redis,
            )
        except Exception as e:
            logger.warning(f"Queue reinvest failed: {e}")

    except Exception as e:
        logger.error(f"[RISK] Error handling {token_address}: {e}", exc_info=True)

# -------------------------
# Loops
# -------------------------
async def monitor_positions_loop(redis=None, interval: Optional[int] = None, stop_evt: Optional[asyncio.Event] = None):
    """
    Generic monitor loop (can be used standalone).
    If stop_evt is provided, exits when stop_evt.is_set().
    """
    if redis is None:
        redis = await _get_redis()
    sleep_s = int(interval or POSITION_CHECK_INTERVAL)
    logger.info(f"📊 Risk monitor started (interval={sleep_s}s)")
    while True:
        if stop_evt is not None and stop_evt.is_set():
            logger.info("📊 Risk monitor stopping on stop_evt.")
            break
        try:
            positions = load_positions()
            if positions:
                await asyncio.gather(
                    *(handle_position(mint, pos, redis=redis) for mint, pos in positions.items())
                )
        except Exception as e:
            logger.error(f"[RISK] monitor loop error: {e}", exc_info=True)
        await asyncio.sleep(sleep_s)

async def risk_manager_loop(stop_evt: asyncio.Event) -> None:
    """
    Entry point used by scripts/main.py:
      from utils_pkg.risk_manager import risk_manager_loop
      ...
      asyncio.create_task(risk_manager_loop(stop_evt), name="risk.manager")
    """
    # Use RISK_MANAGER_POLL_SEC if present, else POSITION_CHECK_INTERVAL
    poll = float(os.getenv("RISK_MANAGER_POLL_SEC", str(POSITION_CHECK_INTERVAL)))
    redis = await _get_redis()
    logger.info(f"📊 risk_manager_loop started (poll={poll}s)")
    while not stop_evt.is_set():
        try:
            positions = load_positions()
            if positions:
                await asyncio.gather(
                    *(handle_position(mint, pos, redis=redis) for mint, pos in positions.items())
                )
        except Exception as e:
            logger.error(f"[RISK] risk_manager_loop error: {e}", exc_info=True)
        # Sleep in small chunks so we respond promptly to stop_evt
        remaining = poll
        while remaining > 0 and not stop_evt.is_set():
            step = min(remaining, 1.0)
            await asyncio.sleep(step)
            remaining -= step
    logger.info("📊 risk_manager_loop exiting (stop_evt set).")

# -------------------------
# CLI / manual run
# -------------------------
if __name__ == "__main__":
    # Standalone: python utils_pkg/risk_manager.py
    asyncio.run(monitor_positions_loop())
