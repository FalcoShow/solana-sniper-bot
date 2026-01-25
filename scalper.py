# scalper.py
from __future__ import annotations

"""
scalper.py — P1/P2 (isolated, production-safe)
==============================================

Role
----
A small, isolated SOL/USDC "scalper/daytrader" module.
It is NOT part of the canonical sniper path and must not silently compete with it.

Design rules
------------
- Disabled by default.
- Explicit mode gating:
    * OFF   : do nothing
    * PAPER : simulate decisions, no swaps
    * TRADE : execute swaps (explicit)
- Redis-backed persistence (if available):
    * position state
    * daily pnl / drawdown
    * trade counters / cooldowns
- Strong risk/circuit breakers:
    * max daily loss %
    * max daily drawdown %
    * max trades per day
    * min time between trades (cooldown)
    * keep SOL fee reserve
- Observability:
    * clean logs
    * optional Telegram
- Backwards compatibility:
    * if DAYTRADER_ENABLED=true and SCALPER_MODE is not set -> TRADE (legacy)

Dependencies (best effort)
--------------------------
We prefer utils_pkg.core_utils hooks if present:
- init_resources / close_resources / get_redis_async
- send_telegram
- convert_sol_to_usd
- swap_token_jupiter
- get_wallet_tokens (optional; we can run without it but balances become unknown)

"""

import asyncio
import contextlib
import json
import logging
import os
import random
import signal
import time
from collections import deque
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any


# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
def _setup_logging() -> logging.Logger:
    # Prefer canonical logging_setup if present
    try:
        from logging_setup import get_logger  # type: ignore

        return get_logger("scalper")
    except Exception:
        pass

    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s: %(message)s",
    )
    return logging.getLogger("scalper")


log = _setup_logging()


# -----------------------------------------------------------------------------
# Optional core hooks (must not crash if absent)
# -----------------------------------------------------------------------------
try:
    from utils_pkg.core_utils import (  # type: ignore
        close_resources,
        convert_sol_to_usd,
        get_redis_async,
        get_wallet_tokens,
        init_resources,
        send_telegram,
        swap_token_jupiter,
    )
except Exception:  # pragma: no cover
    init_resources = None
    close_resources = None
    get_redis_async = None
    send_telegram = None
    convert_sol_to_usd = None
    swap_token_jupiter = None
    get_wallet_tokens = None


# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------
SOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"


def _env_bool(name: str, default: str = "0") -> bool:
    v = os.getenv(name, default).strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def _env_float(name: str, default: str) -> float:
    try:
        return float(os.getenv(name, default))
    except Exception:
        return float(default)


def _env_int(name: str, default: str) -> int:
    try:
        return int(os.getenv(name, default))
    except Exception:
        return int(default)


def _utc_day() -> str:
    return datetime.now(UTC).strftime("%Y%m%d")


# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class ScalperConfig:
    enabled: bool
    mode: str  # OFF | PAPER | TRADE

    timeframe_sec: int
    lookback: int

    # Strategy
    alpha_min: float
    rsi_bull: float
    stop_atr_mult: float
    tp_atr_mult: float
    use_trailing: bool
    trail_atr_mult: float
    max_hold_min: int

    # Risk
    risk_per_trade: float
    min_trade_usd: float
    max_trade_usd: float
    min_sol_fee_reserve: float
    max_trades_per_day: int
    cooldown_sec: int
    max_daily_loss_pct: float
    max_daily_dd_pct: float

    # Exec
    slippage: float

    # Telemetry
    telegram_on_start: bool
    telegram_on_trade: bool
    telegram_on_paper: bool

    # Redis keys
    state_key: str
    pnl_key: str
    dd_key: str
    trades_key: str
    cooldown_key: str

    @staticmethod
    def from_env() -> ScalperConfig:
        # New toggles
        scalper_enabled = _env_bool("SCALPER_ENABLED", "0")
        scalper_mode_env = os.getenv("SCALPER_MODE", "").strip()

        # Legacy toggle (your current file uses DAYTRADER_ENABLED)
        legacy_enabled = os.getenv("DAYTRADER_ENABLED", "false").strip().lower() == "true"

        enabled = bool(scalper_enabled or legacy_enabled)

        if scalper_mode_env:
            mode = scalper_mode_env.strip().upper()
        else:
            # Backcompat: legacy enabled implies TRADE unless user specifies otherwise
            mode = "TRADE" if legacy_enabled else ("PAPER" if enabled else "OFF")

        if mode not in ("OFF", "PAPER", "TRADE"):
            mode = "PAPER" if enabled else "OFF"

        timeframe_sec = _env_int("SCALPER_TIMEFRAME_SEC", os.getenv("TIMEFRAME_SEC", "60"))
        lookback = _env_int("SCALPER_LOOKBACK", os.getenv("DAYTRADER_LOOKBACK", "600"))

        alpha_min = _env_float("SCALPER_ALPHA_MIN", os.getenv("DAYTRADER_ALPHA_SCORE_MIN", "0.60"))
        rsi_bull = _env_float(
            "SCALPER_RSI_BULL", os.getenv("DAYTRADER_ALPHA_CONFIRM_RSIBULL", "55")
        )
        stop_atr_mult = _env_float(
            "SCALPER_STOP_ATR_MULT", os.getenv("DAYTRADER_STOP_ATR_MULT", "1.5")
        )
        tp_atr_mult = _env_float("SCALPER_TP_ATR_MULT", os.getenv("DAYTRADER_TP_ATR_MULT", "3.0"))

        use_trailing = _env_bool("SCALPER_USE_TRAIL", "1")
        trail_atr_mult = _env_float("SCALPER_TRAIL_ATR_MULT", "1.25")
        max_hold_min = _env_int("SCALPER_MAX_HOLD_MIN", "180")

        risk_per_trade = _env_float(
            "SCALPER_RISK_PER_TRADE", os.getenv("DAYTRADER_RISK_PER_TRADE", "0.005")
        )
        min_trade_usd = _env_float("SCALPER_MIN_TRADE_USD", "10")
        max_trade_usd = _env_float("SCALPER_MAX_TRADE_USD", "250")
        min_sol_fee_reserve = _env_float("SCALPER_MIN_SOL_FEE_RESERVE", "0.03")

        max_trades_per_day = _env_int("SCALPER_MAX_TRADES_PER_DAY", "8")
        cooldown_sec = _env_int("SCALPER_COOLDOWN_SEC", os.getenv("DAYTRADER_COOLDOWN_SEC", "60"))

        max_daily_loss_pct = _env_float(
            "SCALPER_MAX_DAILY_LOSS_PCT", os.getenv("DAYTRADER_MAX_DRAWDOWN_PCT", "0.07")
        )
        max_daily_dd_pct = _env_float("SCALPER_MAX_DAILY_DD_PCT", "0.10")

        slippage = _env_float("JUPITER_MAX_SLIPPAGE", os.getenv("JUPITER_SLIPPAGE", "0.005"))

        telegram_on_start = _env_bool("SCALPER_TELEGRAM_ON_START", "1")
        telegram_on_trade = _env_bool("SCALPER_TELEGRAM_ON_TRADE", "1")
        telegram_on_paper = _env_bool("SCALPER_TELEGRAM_ON_PAPER", "0")

        day = _utc_day()
        state_key = os.getenv("SCALPER_STATE_KEY", "scalper:state")
        pnl_key = os.getenv("SCALPER_PNL_KEY", f"scalper:pnl_usd:{day}")
        dd_key = os.getenv("SCALPER_DD_KEY", f"scalper:dd_pct:{day}")
        trades_key = os.getenv("SCALPER_TRADES_KEY", f"scalper:trades:{day}")
        cooldown_key = os.getenv("SCALPER_COOLDOWN_KEY", "scalper:cooldown_until")

        return ScalperConfig(
            enabled=enabled,
            mode=mode,
            timeframe_sec=max(5, timeframe_sec),
            lookback=max(60, lookback),
            alpha_min=float(alpha_min),
            rsi_bull=float(rsi_bull),
            stop_atr_mult=float(stop_atr_mult),
            tp_atr_mult=float(tp_atr_mult),
            use_trailing=bool(use_trailing),
            trail_atr_mult=float(trail_atr_mult),
            max_hold_min=max(5, int(max_hold_min)),
            risk_per_trade=max(0.0005, float(risk_per_trade)),
            min_trade_usd=max(1.0, float(min_trade_usd)),
            max_trade_usd=max(1.0, float(max_trade_usd)),
            min_sol_fee_reserve=max(0.0, float(min_sol_fee_reserve)),
            max_trades_per_day=max(1, int(max_trades_per_day)),
            cooldown_sec=max(0, int(cooldown_sec)),
            max_daily_loss_pct=max(0.0, float(max_daily_loss_pct)),
            max_daily_dd_pct=max(0.0, float(max_daily_dd_pct)),
            slippage=max(0.0001, float(slippage)),
            telegram_on_start=bool(telegram_on_start),
            telegram_on_trade=bool(telegram_on_trade),
            telegram_on_paper=bool(telegram_on_paper),
            state_key=state_key,
            pnl_key=pnl_key,
            dd_key=dd_key,
            trades_key=trades_key,
            cooldown_key=cooldown_key,
        )


# -----------------------------------------------------------------------------
# TA helpers (simple, deterministic)
# -----------------------------------------------------------------------------
def ema(prev: float, price: float, period: int) -> float:
    k = 2.0 / (period + 1.0)
    return price * k + prev * (1.0 - k) if prev else price


def compute_rsi(prices: deque[float], period: int = 14) -> float:
    if len(prices) < period + 1:
        return 50.0
    gains = 0.0
    losses = 0.0
    # iterate over last `period` diffs
    for i in range(len(prices) - period, len(prices)):
        diff = prices[i] - prices[i - 1]  # type: ignore[index]
        if diff > 0:
            gains += diff
        else:
            losses -= diff
    rs = (gains / period) / max(1e-9, (losses / period))
    return 100.0 - (100.0 / (1.0 + rs))


def compute_atr_close_to_close(prices: deque[float], period: int = 14) -> float:
    if len(prices) < period + 1:
        return 0.0
    trs = []
    for i in range(len(prices) - period + 1, len(prices)):
        trs.append(abs(prices[i] - prices[i - 1]))  # type: ignore[index]
    return float(sum(trs) / max(1, len(trs)))


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


# -----------------------------------------------------------------------------
# Redis + telemetry
# -----------------------------------------------------------------------------
async def _redis() -> Any:
    if get_redis_async is None:
        return None
    try:
        return await get_redis_async()
    except Exception:
        log.debug("get_redis_async failed", exc_info=True)
        return None


async def _tg(msg: str) -> None:
    if send_telegram is None:
        log.info(f"[telegram disabled] {msg}")
        return
    try:
        await send_telegram(msg)
    except Exception:
        log.debug("send_telegram failed", exc_info=True)


async def _get_float(r: Any, key: str, default: float = 0.0) -> float:
    if r is None:
        return default
    try:
        v = await r.get(key)
        return float(v) if v is not None else default
    except Exception:
        return default


async def _set_float(r: Any, key: str, value: float, ex_sec: int | None = None) -> None:
    if r is None:
        return
    try:
        if ex_sec is not None:
            await r.set(key, str(float(value)), ex=ex_sec)
        else:
            await r.set(key, str(float(value)))
    except Exception:
        pass


async def _incr_float(r: Any, key: str, delta: float) -> float:
    if r is None:
        return 0.0
    try:
        v = await r.incrbyfloat(key, float(delta))
        return float(v)
    except Exception:
        return 0.0


# -----------------------------------------------------------------------------
# Wallet / balances (SOL + USDC)
# -----------------------------------------------------------------------------
async def _sol_usd() -> float:
    if convert_sol_to_usd is None:
        return 0.0
    try:
        v = await convert_sol_to_usd(1.0)
        return float(v or 0.0)
    except Exception:
        return 0.0


async def _balances() -> tuple[float, float]:
    """
    Returns (sol_balance, usdc_balance).
    Uses get_wallet_tokens if available; otherwise returns (0,0).
    """
    if get_wallet_tokens is None:
        return 0.0, 0.0
    wallet = os.getenv("WALLET_PUBKEY", "").strip() or os.getenv("WALLET_ADDRESS", "").strip()
    if not wallet:
        return 0.0, 0.0
    try:
        toks = await get_wallet_tokens(wallet)
    except Exception:
        return 0.0, 0.0

    sol_bal = 0.0
    usdc_bal = 0.0
    for t in toks or []:
        mint = (t.get("mint") or "").strip()
        bal = float(t.get("balance", 0.0) or 0.0)
        if bal <= 0:
            continue
        if mint == SOL_MINT:
            sol_bal += bal
        elif mint == USDC_MINT:
            usdc_bal += bal
    return sol_bal, usdc_bal


async def _equity_usd(sol_usd: float) -> float:
    sol_bal, usdc_bal = await _balances()
    return float(sol_bal * sol_usd + usdc_bal)


# -----------------------------------------------------------------------------
# Trade execution (USDC <-> SOL)
# -----------------------------------------------------------------------------
async def _buy_sol(usdc_to_spend: float, cfg: ScalperConfig) -> tuple[bool, str]:
    if swap_token_jupiter is None:
        return False, ""
    if usdc_to_spend <= 0:
        return False, ""
    try:
        # Support multiple historical signatures
        try:
            ok, sig = await swap_token_jupiter(
                USDC_MINT,
                SOL_MINT,
                amount_float=float(usdc_to_spend),
                buy=True,
                slippage=float(cfg.slippage),
            )
            return bool(ok), str(sig or "")
        except TypeError:
            ok, sig = await swap_token_jupiter(
                USDC_MINT, SOL_MINT, float(usdc_to_spend), True, float(cfg.slippage)
            )
            return bool(ok), str(sig or "")
    except Exception:
        log.exception("swap buy failed")
        return False, ""


async def _sell_sol(sol_to_sell: float, cfg: ScalperConfig) -> tuple[bool, str]:
    if swap_token_jupiter is None:
        return False, ""
    if sol_to_sell <= 0:
        return False, ""
    try:
        try:
            ok, sig = await swap_token_jupiter(
                SOL_MINT,
                USDC_MINT,
                amount_float=float(sol_to_sell),
                buy=False,
                slippage=float(cfg.slippage),
            )
            return bool(ok), str(sig or "")
        except TypeError:
            ok, sig = await swap_token_jupiter(
                SOL_MINT, USDC_MINT, float(sol_to_sell), False, float(cfg.slippage)
            )
            return bool(ok), str(sig or "")
    except Exception:
        log.exception("swap sell failed")
        return False, ""


# -----------------------------------------------------------------------------
# State model
# -----------------------------------------------------------------------------
@dataclass
class PositionState:
    pos: str  # "FLAT" | "LONG"
    entry_ts: float
    entry_px: float
    size_sol: float
    entry_usd: float
    stop_px: float
    take_px: float
    trail_px: float  # trailing stop level if enabled (0 if none)

    @staticmethod
    def flat() -> PositionState:
        return PositionState("FLAT", 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)


async def _load_state(r: Any, key: str) -> PositionState:
    if r is None:
        return PositionState.flat()
    try:
        raw = await r.get(key)
        if not raw:
            return PositionState.flat()
        obj = json.loads(raw)
        return PositionState(
            pos=str(obj.get("pos", "FLAT")).upper(),
            entry_ts=float(obj.get("entry_ts", 0.0)),
            entry_px=float(obj.get("entry_px", 0.0)),
            size_sol=float(obj.get("size_sol", 0.0)),
            entry_usd=float(obj.get("entry_usd", 0.0)),
            stop_px=float(obj.get("stop_px", 0.0)),
            take_px=float(obj.get("take_px", 0.0)),
            trail_px=float(obj.get("trail_px", 0.0)),
        )
    except Exception:
        return PositionState.flat()


async def _save_state(r: Any, key: str, st: PositionState) -> None:
    if r is None:
        return
    with contextlib.suppress(Exception):
        await r.set(key, json.dumps(asdict(st), separators=(",", ":")))


# -----------------------------------------------------------------------------
# Risk / breakers
# -----------------------------------------------------------------------------
async def _cooldown_active(r: Any, cfg: ScalperConfig) -> bool:
    if r is None:
        return False
    try:
        until = await r.get(cfg.cooldown_key)
        if not until:
            return False
        return time.time() < float(until)
    except Exception:
        return False


async def _set_cooldown(r: Any, cfg: ScalperConfig) -> None:
    if r is None or cfg.cooldown_sec <= 0:
        return
    with contextlib.suppress(Exception):
        await r.set(
            cfg.cooldown_key, str(time.time() + cfg.cooldown_sec), ex=max(cfg.cooldown_sec + 5, 10)
        )


async def _trade_count(r: Any, cfg: ScalperConfig) -> int:
    if r is None:
        return 0
    try:
        v = await r.get(cfg.trades_key)
        return int(v) if v is not None else 0
    except Exception:
        return 0


async def _inc_trade_count(r: Any, cfg: ScalperConfig) -> int:
    if r is None:
        return 0
    try:
        v = await r.incr(cfg.trades_key)
        # expire daily keys after ~2 days
        await r.expire(cfg.trades_key, 172800)
        return int(v)
    except Exception:
        return 0


async def _breaker_ok(r: Any, cfg: ScalperConfig, equity_usd: float) -> bool:
    """
    Breakers use realized pnl approximations (USDC delta from trades is not directly tracked),
    so we track pnl from price * size using entry/exit prices.
    """
    pnl = await _get_float(r, cfg.pnl_key, 0.0)
    dd = await _get_float(r, cfg.dd_key, 0.0)

    # Daily loss breaker (pnl below -loss% of equity)
    if cfg.max_daily_loss_pct > 0 and equity_usd > 0:
        if pnl <= -abs(cfg.max_daily_loss_pct) * equity_usd:
            return False

    # DD breaker (max_dd_pct stored)
    if cfg.max_daily_dd_pct > 0 and dd >= abs(cfg.max_daily_dd_pct):
        return False

    # Trade count breaker
    trades = await _trade_count(r, cfg)
    return not trades >= cfg.max_trades_per_day


async def _update_dd(r: Any, cfg: ScalperConfig, equity_usd: float) -> None:
    if r is None or equity_usd <= 0:
        return
    pnl = await _get_float(r, cfg.pnl_key, 0.0)
    # DD approximation: if pnl negative, dd = |pnl|/equity (bounded)
    dd = abs(min(0.0, pnl)) / max(1e-9, equity_usd)
    dd = float(_clamp(dd, 0.0, 1.0))
    await _set_float(r, cfg.dd_key, dd, ex_sec=172800)


# -----------------------------------------------------------------------------
# Alpha / signals
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class Signals:
    alpha: float
    ema12: float
    ema26: float
    ema200: float
    rsi: float
    atr: float


def _compute_alpha(
    sol_usd: float, ema12_v: float, ema26_v: float, ema200_v: float, rsi_v: float
) -> float:
    """
    Bounded [0..1] alpha composite:
      - trend: ema12 > ema26
      - regime: price > ema200
      - confirmation: rsi >= threshold (bullish)
    """
    a = 0.0
    if ema26_v:
        a += 0.42 * (1.0 if ema12_v > ema26_v else 0.0)
    if ema200_v:
        a += 0.33 * (1.0 if sol_usd > ema200_v else 0.0)
    a += 0.25 * (1.0 if rsi_v >= 50.0 else 0.0)
    return float(_clamp(a, 0.0, 1.0))


# -----------------------------------------------------------------------------
# Main loop
# -----------------------------------------------------------------------------
async def run_scalper(stop_event: asyncio.Event) -> None:
    cfg = ScalperConfig.from_env()

    if not cfg.enabled or cfg.mode == "OFF":
        log.info("scalper disabled (SCALPER_ENABLED=0 or mode=OFF).")
        return

    if cfg.telegram_on_start:
        await _tg(
            "📈 Scalper started (SOL/USDC)\n"
            f"• mode={cfg.mode}\n"
            f"• timeframe={cfg.timeframe_sec}s lookback={cfg.lookback}\n"
            f"• alpha_min={cfg.alpha_min:.2f} rsi_bull={cfg.rsi_bull:.1f}\n"
            f"• risk={cfg.risk_per_trade:.4f} min/max_trade=${cfg.min_trade_usd:.0f}/${cfg.max_trade_usd:.0f}\n"
            f"• breakers: max_trades/day={cfg.max_trades_per_day} cooldown={cfg.cooldown_sec}s "
            f"loss_pct={cfg.max_daily_loss_pct:.3f} dd_pct={cfg.max_daily_dd_pct:.3f}"
        )

    r = await _redis()
    st = await _load_state(r, cfg.state_key)

    prices: deque[float] = deque(maxlen=cfg.lookback)
    ema12_v = 0.0
    ema26_v = 0.0
    ema200_v = 0.0

    # If we restart with an open position, we keep managing it.
    if st.pos == "LONG":
        log.warning(
            "Recovered LONG position from Redis: size_sol=%.6f entry_px=%.4f stop=%.4f take=%.4f",
            st.size_sol,
            st.entry_px,
            st.stop_px,
            st.take_px,
        )

    last_heartbeat = 0.0

    while not stop_event.is_set():
        time.time()
        try:
            sol_usd = await _sol_usd()
            if sol_usd <= 0:
                await asyncio.sleep(cfg.timeframe_sec)
                continue

            prices.append(sol_usd)
            ema12_v = ema(ema12_v, sol_usd, 12)
            ema26_v = ema(ema26_v, sol_usd, 26)
            if len(prices) >= 200:
                ema200_v = ema(ema200_v, sol_usd, 200)
            rsi_v = compute_rsi(prices, 14)
            atr_v = compute_atr_close_to_close(prices, 14)

            alpha = _compute_alpha(sol_usd, ema12_v, ema26_v, ema200_v, rsi_v)
            sigs = Signals(
                alpha=alpha, ema12=ema12_v, ema26=ema26_v, ema200=ema200_v, rsi=rsi_v, atr=atr_v
            )

            # Heartbeat every ~5 minutes
            if time.time() - last_heartbeat >= 300:
                eq = await _equity_usd(sol_usd)
                pnl = await _get_float(r, cfg.pnl_key, 0.0)
                dd = await _get_float(r, cfg.dd_key, 0.0)
                trades = await _trade_count(r, cfg)
                log.info(
                    "[hb] px=%.2f alpha=%.2f rsi=%.1f atr=%.4f pos=%s eq=$%.2f pnl=$%.2f dd=%.3f trades=%d mode=%s",
                    sol_usd,
                    sigs.alpha,
                    sigs.rsi,
                    sigs.atr,
                    st.pos,
                    eq,
                    pnl,
                    dd,
                    trades,
                    cfg.mode,
                )
                last_heartbeat = time.time()

            # Breakers check (only impacts NEW entries)
            equity = await _equity_usd(sol_usd)
            await _update_dd(r, cfg, equity)

            breakers_ok = await _breaker_ok(r, cfg, equity)
            cooldown_on = await _cooldown_active(r, cfg)

            # --------------------
            # Position management
            # --------------------
            if st.pos == "LONG":
                # Update trailing stop if enabled
                if cfg.use_trailing and sigs.atr > 0:
                    new_trail = sol_usd - cfg.trail_atr_mult * sigs.atr
                    if st.trail_px <= 0:
                        st = PositionState(
                            pos="LONG",
                            entry_ts=st.entry_ts,
                            entry_px=st.entry_px,
                            size_sol=st.size_sol,
                            entry_usd=st.entry_usd,
                            stop_px=st.stop_px,
                            take_px=st.take_px,
                            trail_px=new_trail,
                        )
                    else:
                        st = PositionState(
                            pos="LONG",
                            entry_ts=st.entry_ts,
                            entry_px=st.entry_px,
                            size_sol=st.size_sol,
                            entry_usd=st.entry_usd,
                            stop_px=st.stop_px,
                            take_px=st.take_px,
                            trail_px=max(st.trail_px, new_trail),
                        )

                # Effective stop is max(original_stop, trailing_stop)
                eff_stop = max(st.stop_px, st.trail_px) if st.trail_px > 0 else st.stop_px
                eff_take = st.take_px

                held_min = (time.time() - st.entry_ts) / 60.0 if st.entry_ts > 0 else 0.0

                exit_reason = ""
                if eff_stop > 0 and sol_usd <= eff_stop:
                    exit_reason = "STOP"
                elif eff_take > 0 and sol_usd >= eff_take:
                    exit_reason = "TAKE"
                elif cfg.max_hold_min > 0 and held_min >= cfg.max_hold_min:
                    exit_reason = "TIME"
                # Optional: alpha flips bearish while in position (soft exit)
                elif sigs.alpha < 0.25 and sigs.rsi < 45.0:
                    exit_reason = "ALPHA_FLIP"

                if exit_reason:
                    # Execute exit
                    if cfg.mode == "PAPER":
                        pnl_usd = (sol_usd - st.entry_px) * st.size_sol
                        await _incr_float(r, cfg.pnl_key, pnl_usd)
                        await _update_dd(r, cfg, equity)
                        if cfg.telegram_on_paper:
                            await _tg(
                                f"🧾 PAPER EXIT ({exit_reason})\n"
                                f"• px={sol_usd:.2f} entry={st.entry_px:.2f} size={st.size_sol:.6f} pnl=${pnl_usd:+.2f}"
                            )
                        log.info("[PAPER EXIT] %s pnl=$%.2f", exit_reason, pnl_usd)
                        st = PositionState.flat()
                        await _save_state(r, cfg.state_key, st)
                        await _set_cooldown(r, cfg)
                    else:
                        ok, sig = await _sell_sol(st.size_sol, cfg)
                        if ok:
                            pnl_usd = (sol_usd - st.entry_px) * st.size_sol
                            await _incr_float(r, cfg.pnl_key, pnl_usd)
                            await _update_dd(r, cfg, equity)
                            if cfg.telegram_on_trade:
                                await _tg(
                                    f"✅ EXIT ({exit_reason})\n"
                                    f"• px={sol_usd:.2f} entry={st.entry_px:.2f} size={st.size_sol:.6f}\n"
                                    f"• pnl=${pnl_usd:+.2f} • tx={sig}"
                                )
                            log.info("[EXIT] %s pnl=$%.2f tx=%s", exit_reason, pnl_usd, sig)
                            st = PositionState.flat()
                            await _save_state(r, cfg.state_key, st)
                            await _set_cooldown(r, cfg)
                        else:
                            # Don’t flip state; retry next bar.
                            log.warning(
                                "Exit attempt failed (%s). Will retry next bar.", exit_reason
                            )
                            if cfg.telegram_on_trade:
                                await _tg(f"⚠️ EXIT failed ({exit_reason}) — will retry next bar.")

            # --------------------
            # Entry logic
            # --------------------
            if st.pos == "FLAT":
                if not breakers_ok:
                    await asyncio.sleep(cfg.timeframe_sec)
                    continue
                if cooldown_on:
                    await asyncio.sleep(cfg.timeframe_sec)
                    continue
                if len(prices) < 200:
                    await asyncio.sleep(cfg.timeframe_sec)
                    continue

                # Strong trend filter
                trend_ok = (ema12_v > ema26_v) and (sol_usd > ema200_v)
                confirm_ok = sigs.rsi >= cfg.rsi_bull
                score_ok = sigs.alpha >= cfg.alpha_min

                if trend_ok and confirm_ok and score_ok:
                    # Risk sizing using ATR stop distance (USD)
                    if sigs.atr <= 0:
                        await asyncio.sleep(cfg.timeframe_sec)
                        continue

                    stop_dist = cfg.stop_atr_mult * sigs.atr
                    stop_px = sol_usd - stop_dist
                    take_px = sol_usd + cfg.tp_atr_mult * sigs.atr

                    # Determine allowed USD spend
                    sol_bal, usdc_bal = await _balances()
                    # keep SOL reserve for fees
                    # (if we hold SOL already, we still keep reserve)
                    if sol_bal < cfg.min_sol_fee_reserve:
                        # If you have insufficient SOL for fees, scalper trading is risky.
                        log.warning(
                            "SOL fee reserve low (sol=%.4f < %.4f). Skipping entry.",
                            sol_bal,
                            cfg.min_sol_fee_reserve,
                        )
                        await asyncio.sleep(cfg.timeframe_sec)
                        continue

                    if usdc_bal < cfg.min_trade_usd:
                        await asyncio.sleep(cfg.timeframe_sec)
                        continue

                    # USD risk budget
                    usd_risk = max(cfg.min_trade_usd, equity * cfg.risk_per_trade)
                    usd_to_use = _clamp(usd_risk, cfg.min_trade_usd, cfg.max_trade_usd)
                    usd_to_use = min(usd_to_use, usdc_bal)

                    if usd_to_use < cfg.min_trade_usd:
                        await asyncio.sleep(cfg.timeframe_sec)
                        continue

                    # Convert size from USD to SOL (approx)
                    size_sol = float(usd_to_use / max(1e-9, sol_usd))

                    # Book trade
                    if cfg.mode == "PAPER":
                        st = PositionState(
                            pos="LONG",
                            entry_ts=time.time(),
                            entry_px=float(sol_usd),
                            size_sol=float(size_sol),
                            entry_usd=float(usd_to_use),
                            stop_px=float(stop_px),
                            take_px=float(take_px),
                            trail_px=0.0,
                        )
                        await _save_state(r, cfg.state_key, st)
                        await _inc_trade_count(r, cfg)
                        if cfg.telegram_on_paper:
                            await _tg(
                                "🧾 PAPER ENTRY\n"
                                f"• px={sol_usd:.2f} size≈{size_sol:.6f} SOL spend≈${usd_to_use:.2f}\n"
                                f"• stop={stop_px:.2f} take={take_px:.2f} alpha={sigs.alpha:.2f} rsi={sigs.rsi:.1f}"
                            )
                        log.info(
                            "[PAPER ENTRY] px=%.2f size=%.6f stop=%.2f take=%.2f",
                            sol_usd,
                            size_sol,
                            stop_px,
                            take_px,
                        )
                        await _set_cooldown(r, cfg)

                    else:
                        ok, sig = await _buy_sol(usd_to_use, cfg)
                        if ok:
                            st = PositionState(
                                pos="LONG",
                                entry_ts=time.time(),
                                entry_px=float(sol_usd),
                                size_sol=float(size_sol),
                                entry_usd=float(usd_to_use),
                                stop_px=float(stop_px),
                                take_px=float(take_px),
                                trail_px=0.0,
                            )
                            await _save_state(r, cfg.state_key, st)
                            await _inc_trade_count(r, cfg)
                            if cfg.telegram_on_trade:
                                await _tg(
                                    "🟢 ENTRY (scalper)\n"
                                    f"• px={sol_usd:.2f} spend=${usd_to_use:.2f} size≈{size_sol:.6f} SOL\n"
                                    f"• stop={stop_px:.2f} take={take_px:.2f} alpha={sigs.alpha:.2f} rsi={sigs.rsi:.1f}\n"
                                    f"• tx={sig}"
                                )
                            log.info("[ENTRY] px=%.2f spend=$%.2f tx=%s", sol_usd, usd_to_use, sig)
                            await _set_cooldown(r, cfg)
                        else:
                            log.warning("Entry swap failed; will retry if setup persists.")
                            if cfg.telegram_on_trade:
                                await _tg("⚠️ ENTRY failed (swap) — will retry if setup persists.")

            # sleep with small jitter to avoid sync
            sleep_for = float(cfg.timeframe_sec) + random.uniform(0.0, 0.7)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop_event.wait(), timeout=sleep_for)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.exception("[scalper] loop error: %s", e)
            # small backoff
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=max(3.0, float(ScalperConfig.from_env().cooldown_sec)),
                )


async def main() -> None:
    if init_resources is not None:
        try:
            await init_resources()
        except Exception:
            log.debug("init_resources failed (continuing)", exc_info=True)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _stop(*_: Any) -> None:
        stop_event.set()

    for sig_name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, sig_name, None)
        if sig is None:
            continue
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _stop)

    try:
        await run_scalper(stop_event)
    finally:
        if close_resources is not None:
            try:
                await close_resources()
            except Exception:
                log.debug("close_resources failed", exc_info=True)


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
