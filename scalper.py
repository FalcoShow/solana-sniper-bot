from __future__ import annotations

"""
scalper.py — P1/P2/P3 hardened isolated scalper (production-safe)
=================================================================

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
- get_wallet_tokens (optional)
"""

import asyncio
import contextlib
import inspect
import json
import os
import random
import signal
import time
from collections import deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, cast

# -----------------------------------------------------------------------------
# Optional core hooks (best-effort; must never crash if absent)
# NOTE: Keep as Any so mypy doesn't lock us into one signature and then fail on
#       "optional-callable set to None" or "missing arg" patterns.
# -----------------------------------------------------------------------------
init_resources: Any = None
close_resources: Any = None
get_redis_async: Any = None
send_telegram: Any = None
convert_sol_to_usd: Any = None
swap_token_jupiter: Any = None
get_wallet_tokens: Any = None

try:
    from utils_pkg.core_utils import (  # type: ignore
        close_resources as _close_resources,
        convert_sol_to_usd as _convert_sol_to_usd,
        get_redis_async as _get_redis_async,
        get_wallet_tokens as _get_wallet_tokens,
        init_resources as _init_resources,
        send_telegram as _send_telegram,
        swap_token_jupiter as _swap_token_jupiter,
    )

    init_resources = _init_resources
    close_resources = _close_resources
    get_redis_async = _get_redis_async
    send_telegram = _send_telegram
    convert_sol_to_usd = _convert_sol_to_usd
    swap_token_jupiter = _swap_token_jupiter
    get_wallet_tokens = _get_wallet_tokens
except Exception:  # pragma: no cover
    # Keep Nones. This module must still import safely.
    pass


# -----------------------------------------------------------------------------
# Small async helpers (mypy-safe)
# -----------------------------------------------------------------------------
async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


async def _call_maybe_async(fn: Any, *args: Any, **kwargs: Any) -> Any:
    """
    Call fn(*args, **kwargs) and await it if it returns an awaitable.

    IMPORTANT: fn is Any on purpose (compat across multiple implementations).
    """
    out = fn(*args, **kwargs)
    return await _maybe_await(out)


def _short(x: Any, n: int = 320) -> str:
    try:
        s = str(x)
        return s if len(s) <= n else (s[:n] + "…")
    except Exception:
        return "<?>"


# -----------------------------------------------------------------------------
# Logging: no global side-effects at import (no setup_logging at import)
# -----------------------------------------------------------------------------
class _LogShim:
    """
    Adapter supporting .event() and .every() without mutating any logger object.
    If the underlying logger exposes .event, call it; else fall back to .info.
    """

    __slots__ = ("_supplier", "_every_store")

    def __init__(self, supplier: Any) -> None:
        self._supplier = supplier
        self._every_store: dict[str, float] = {}

    def _log(self) -> Any:
        with contextlib.suppress(Exception):
            return self._supplier()
        return None

    def event(self, name: str, **fields: Any) -> None:
        lg = self._log()
        if lg is None:
            return
        fn = getattr(lg, "event", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                fn(name, **fields)
            return
        info = getattr(lg, "info", None)
        if callable(info):
            with contextlib.suppress(Exception):
                info("%s %s", name, fields)

    def every(self, key: str, n: int = 30, msg: str | None = None, **fields: Any) -> None:
        try:
            now = time.monotonic()
            sec = float(max(1, int(n)))
            last = float(self._every_store.get(key, 0.0))
            if (now - last) >= sec:
                self._every_store[key] = now
                self.event(msg or key, **fields)
        except Exception:
            return

    def info(self, msg: str, *a: Any) -> None:
        lg = self._log()
        if lg is None:
            return
        fn = getattr(lg, "info", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                fn(msg, *a)

    def warning(self, msg: str, *a: Any) -> None:
        lg = self._log()
        if lg is None:
            return
        fn = getattr(lg, "warning", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                fn(msg, *a)

    def error(self, msg: str, *a: Any) -> None:
        lg = self._log()
        if lg is None:
            return
        fn = getattr(lg, "error", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                fn(msg, *a)

    def debug(self, msg: str, *a: Any) -> None:
        lg = self._log()
        if lg is None:
            return
        fn = getattr(lg, "debug", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                fn(msg, *a)


@lru_cache(maxsize=1)
def _resolve_logger() -> Any:
    # Prefer repo compat first
    with contextlib.suppress(Exception):
        from utils_pkg.log_compat import get_logger as gl  # type: ignore

        return gl(__name__)

    with contextlib.suppress(Exception):
        from utils_pkg.logging_setup import get_logger as gl2  # type: ignore

        try:
            return gl2(__name__)
        except TypeError:
            return gl2()

    # Root-level compatibility
    with contextlib.suppress(Exception):
        from logging_setup import get_logger as gl3  # type: ignore

        try:
            return gl3(__name__)
        except TypeError:
            return gl3("scalper")

    import logging

    lg = logging.getLogger("scalper")
    if not lg.handlers:
        logging.basicConfig(level=logging.INFO)
    return lg


log = _LogShim(_resolve_logger)

# -----------------------------------------------------------------------------
# Constants / truth tables
# -----------------------------------------------------------------------------
SOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


# -----------------------------------------------------------------------------
# Env helpers (stable + typed)
# -----------------------------------------------------------------------------
def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return bool(default)
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return bool(default)


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return int(default)
    with contextlib.suppress(Exception):
        return int(str(v).strip())
    with contextlib.suppress(Exception):
        return int(float(str(v).strip()))
    return int(default)


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return float(default)
    with contextlib.suppress(Exception):
        return float(str(v).strip())
    return float(default)


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _utc_day() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")


# -----------------------------------------------------------------------------
# Sleep helper (mypy-safe, avoids "unreachable" propagation)
# -----------------------------------------------------------------------------
async def _sleep_or_stop(stop_event: asyncio.Event, seconds: float) -> bool:
    timeout = float(max(0.0, seconds))
    if timeout <= 0.0:
        return bool(stop_event.is_set())

    if stop_event.is_set():
        return True

    try:
        await asyncio.wait_for(stop_event.wait(), timeout=timeout)
    except (asyncio.TimeoutError, TimeoutError):
        pass

    return bool(stop_event.is_set())


# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
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
    cooldown_key: str
    pnl_key_prefix: str
    dd_key_prefix: str
    trades_key_prefix: str

    @staticmethod
    def from_env() -> "ScalperConfig":
        scalper_enabled = _env_bool("SCALPER_ENABLED", False)
        scalper_mode_env = _env_str("SCALPER_MODE", "")

        legacy_enabled = _env_bool("DAYTRADER_ENABLED", False)
        enabled = bool(scalper_enabled or legacy_enabled)

        if scalper_mode_env:
            mode = scalper_mode_env.strip().upper()
        else:
            mode = "TRADE" if legacy_enabled else ("PAPER" if enabled else "OFF")

        if mode not in ("OFF", "PAPER", "TRADE"):
            mode = "PAPER" if enabled else "OFF"

        timeframe_sec = _env_int("SCALPER_TIMEFRAME_SEC", _env_int("TIMEFRAME_SEC", 60))
        lookback = _env_int("SCALPER_LOOKBACK", _env_int("DAYTRADER_LOOKBACK", 600))

        alpha_min = _env_float("SCALPER_ALPHA_MIN", _env_float("DAYTRADER_ALPHA_SCORE_MIN", 0.60))
        rsi_bull = _env_float("SCALPER_RSI_BULL", _env_float("DAYTRADER_ALPHA_CONFIRM_RSIBULL", 55))

        stop_atr_mult = _env_float(
            "SCALPER_STOP_ATR_MULT", _env_float("DAYTRADER_STOP_ATR_MULT", 1.5)
        )
        tp_atr_mult = _env_float("SCALPER_TP_ATR_MULT", _env_float("DAYTRADER_TP_ATR_MULT", 3.0))

        use_trailing = _env_bool("SCALPER_USE_TRAIL", True)
        trail_atr_mult = _env_float("SCALPER_TRAIL_ATR_MULT", 1.25)
        max_hold_min = _env_int("SCALPER_MAX_HOLD_MIN", 180)

        risk_per_trade = _env_float(
            "SCALPER_RISK_PER_TRADE", _env_float("DAYTRADER_RISK_PER_TRADE", 0.005)
        )
        min_trade_usd = _env_float("SCALPER_MIN_TRADE_USD", 10.0)
        max_trade_usd = _env_float("SCALPER_MAX_TRADE_USD", 250.0)
        min_sol_fee_reserve = _env_float("SCALPER_MIN_SOL_FEE_RESERVE", 0.03)

        max_trades_per_day = _env_int("SCALPER_MAX_TRADES_PER_DAY", 8)
        cooldown_sec = _env_int("SCALPER_COOLDOWN_SEC", _env_int("DAYTRADER_COOLDOWN_SEC", 60))

        max_daily_loss_pct = _env_float(
            "SCALPER_MAX_DAILY_LOSS_PCT", _env_float("DAYTRADER_MAX_DRAWDOWN_PCT", 0.07)
        )
        max_daily_dd_pct = _env_float("SCALPER_MAX_DAILY_DD_PCT", 0.10)

        slippage = _env_float("JUPITER_MAX_SLIPPAGE", _env_float("JUPITER_SLIPPAGE", 0.005))

        telegram_on_start = _env_bool("SCALPER_TELEGRAM_ON_START", True)
        telegram_on_trade = _env_bool("SCALPER_TELEGRAM_ON_TRADE", True)
        telegram_on_paper = _env_bool("SCALPER_TELEGRAM_ON_PAPER", False)

        state_key = _env_str("SCALPER_STATE_KEY", "scalper:state")
        cooldown_key = _env_str("SCALPER_COOLDOWN_KEY", "scalper:cooldown_until")

        pnl_key_prefix = _env_str("SCALPER_PNL_KEY_PREFIX", "scalper:pnl_usd")
        dd_key_prefix = _env_str("SCALPER_DD_KEY_PREFIX", "scalper:dd_pct")
        trades_key_prefix = _env_str("SCALPER_TRADES_KEY_PREFIX", "scalper:trades")

        return ScalperConfig(
            enabled=bool(enabled),
            mode=mode,
            timeframe_sec=max(5, int(timeframe_sec)),
            lookback=max(60, int(lookback)),
            alpha_min=float(_clamp(float(alpha_min), 0.0, 1.0)),
            rsi_bull=float(_clamp(float(rsi_bull), 1.0, 99.0)),
            stop_atr_mult=float(_clamp(float(stop_atr_mult), 0.1, 25.0)),
            tp_atr_mult=float(_clamp(float(tp_atr_mult), 0.1, 50.0)),
            use_trailing=bool(use_trailing),
            trail_atr_mult=float(_clamp(float(trail_atr_mult), 0.1, 25.0)),
            max_hold_min=max(5, int(max_hold_min)),
            risk_per_trade=float(_clamp(float(risk_per_trade), 0.0001, 0.25)),
            min_trade_usd=max(1.0, float(min_trade_usd)),
            max_trade_usd=max(1.0, float(max_trade_usd)),
            min_sol_fee_reserve=max(0.0, float(min_sol_fee_reserve)),
            max_trades_per_day=max(1, int(max_trades_per_day)),
            cooldown_sec=max(0, int(cooldown_sec)),
            max_daily_loss_pct=float(_clamp(float(max_daily_loss_pct), 0.0, 1.0)),
            max_daily_dd_pct=float(_clamp(float(max_daily_dd_pct), 0.0, 1.0)),
            slippage=float(_clamp(float(slippage), 0.0001, 0.50)),
            telegram_on_start=bool(telegram_on_start),
            telegram_on_trade=bool(telegram_on_trade),
            telegram_on_paper=bool(telegram_on_paper),
            state_key=state_key,
            cooldown_key=cooldown_key,
            pnl_key_prefix=pnl_key_prefix,
            dd_key_prefix=dd_key_prefix,
            trades_key_prefix=trades_key_prefix,
        )

    def daily_keys(self, day: str) -> tuple[str, str, str]:
        pnl_key = f"{self.pnl_key_prefix}:{day}"
        dd_key = f"{self.dd_key_prefix}:{day}"
        trades_key = f"{self.trades_key_prefix}:{day}"
        return pnl_key, dd_key, trades_key


# -----------------------------------------------------------------------------
# TA helpers (simple, deterministic)
# -----------------------------------------------------------------------------
def ema(prev: float, price: float, period: int) -> float:
    k = 2.0 / (period + 1.0)
    return (price * k + prev * (1.0 - k)) if prev else price


def compute_rsi(prices: deque[float], period: int = 14) -> float:
    if len(prices) < period + 1:
        return 50.0
    arr = list(prices)
    gains = 0.0
    losses = 0.0
    start = len(arr) - period
    for idx in range(start, len(arr)):
        diff = arr[idx] - arr[idx - 1]
        if diff > 0:
            gains += diff
        else:
            losses -= diff
    avg_gain = gains / period
    avg_loss = losses / period
    rs = avg_gain / max(1e-9, avg_loss)
    return 100.0 - (100.0 / (1.0 + rs))


def compute_atr_close_to_close(prices: deque[float], period: int = 14) -> float:
    if len(prices) < period + 1:
        return 0.0
    arr = list(prices)
    start = len(arr) - period
    tr_sum = 0.0
    count = 0
    for idx in range(start, len(arr)):
        tr_sum += abs(arr[idx] - arr[idx - 1])
        count += 1
    return float(tr_sum / max(1, count))


# -----------------------------------------------------------------------------
# Redis + telemetry wrappers (async/sync compatible)
# -----------------------------------------------------------------------------
class _RedisCompat:
    def __init__(self, client: Any):
        self._r = client

    async def call(self, method: str, *args: Any, **kwargs: Any) -> Any:
        if self._r is None:
            return None
        fn = getattr(self._r, method, None)
        if fn is None:
            return None
        try:
            return await _maybe_await(fn(*args, **kwargs))
        except Exception:
            return None

    async def get(self, key: str) -> Any:
        return await self.call("get", key)

    async def set(self, key: str, value: str, ex: int | None = None) -> Any:
        if ex is None:
            return await self.call("set", key, value)
        return await self.call("set", key, value, ex=int(ex))

    async def incr(self, key: str) -> int:
        out = await self.call("incr", key)
        with contextlib.suppress(Exception):
            return int(out)
        return 0

    async def incrbyfloat(self, key: str, delta: float) -> float:
        out = await self.call("incrbyfloat", key, float(delta))
        with contextlib.suppress(Exception):
            return float(out)
        return 0.0

    async def expire(self, key: str, seconds: int) -> None:
        await self.call("expire", key, int(seconds))


async def _redis() -> _RedisCompat | None:
    fn = get_redis_async
    if not callable(fn):
        return None
    try:
        r = await _call_maybe_async(fn)
        return _RedisCompat(r)
    except Exception as e:
        log.every("REDIS_GET_FAIL", n=30, msg="REDIS_GET_FAIL", err=_short(e))
        return None


def _decode_if_bytes(v: Any) -> str:
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", "ignore")
    return str(v)


async def _tg(msg: str) -> None:
    fn = send_telegram
    if not callable(fn):
        log.info("[telegram disabled] %s", msg)
        return
    with contextlib.suppress(Exception):
        await _call_maybe_async(fn, msg)


async def _get_float(r: _RedisCompat | None, key: str, default: float = 0.0) -> float:
    if r is None:
        return default
    v = await r.get(key)
    if v is None or v == "":
        return default
    s = _decode_if_bytes(v).strip()
    with contextlib.suppress(Exception):
        return float(s)
    return default


async def _set_float(
    r: _RedisCompat | None, key: str, value: float, ex_sec: int | None = None
) -> None:
    if r is None:
        return
    await r.set(key, str(float(value)), ex=ex_sec)


async def _incr_float(r: _RedisCompat | None, key: str, delta: float) -> float:
    if r is None:
        return 0.0
    out = await r.incrbyfloat(key, float(delta))
    return float(out)


# -----------------------------------------------------------------------------
# Wallet / balances (SOL + USDC)
# -----------------------------------------------------------------------------
def _wallet_pubkey() -> str:
    return _env_str("WALLET_PUBKEY", "") or _env_str("WALLET_ADDRESS", "")


async def _sol_usd() -> float:
    """
    Best-effort SOL->USD price for 1 SOL.

    We intentionally treat convert_sol_to_usd as a flex-signature function:
    - Some implementations want convert_sol_to_usd(1.0)
    - Others accept no args and return the spot price
    """
    fn = convert_sol_to_usd
    if not callable(fn):
        return 0.0

    # Try preferred (amount=1.0) first
    try:
        out = await _call_maybe_async(fn, 1.0)
        return float(out or 0.0)
    except TypeError:
        pass
    except Exception:
        return 0.0

    # Try no-arg variant
    try:
        out2 = await _call_maybe_async(fn)
        return float(out2 or 0.0)
    except Exception:
        return 0.0


async def _get_wallet_tokens_safe(wallet: str) -> list[dict[str, Any]]:
    fn = get_wallet_tokens
    if not callable(fn):
        return []

    def _only_dicts(raw: Any) -> list[dict[str, Any]]:
        if not isinstance(raw, list):
            return []
        out: list[dict[str, Any]] = []
        for item in raw:
            if isinstance(item, dict):
                out.append(cast(dict[str, Any], item))
        return out

    try:
        raw = await _call_maybe_async(fn, wallet)
        return _only_dicts(raw)
    except TypeError:
        pass
    except Exception:
        return []

    try:
        raw2 = await _call_maybe_async(fn)
        return _only_dicts(raw2)
    except Exception:
        return []


async def _balances() -> tuple[float, float]:
    """
    Returns (sol_balance, usdc_balance).
    Uses get_wallet_tokens if available; otherwise returns (0,0).
    """
    wallet = _wallet_pubkey()
    if not wallet:
        return 0.0, 0.0

    toks = await _get_wallet_tokens_safe(wallet)

    sol_bal = 0.0
    usdc_bal = 0.0
    for t in toks:
        mint = str(t.get("mint") or "").strip()
        try:
            bal = float(t.get("balance") or 0.0)
        except Exception:
            bal = 0.0
        if bal <= 0.0:
            continue
        if mint == SOL_MINT:
            sol_bal += bal
        elif mint == USDC_MINT:
            usdc_bal += bal

    return float(sol_bal), float(usdc_bal)


async def _equity_usd(sol_usd: float) -> float:
    sol_bal, usdc_bal = await _balances()
    return float(sol_bal * sol_usd + usdc_bal)


# -----------------------------------------------------------------------------
# Trade execution (USDC <-> SOL) (compat signatures)
# -----------------------------------------------------------------------------
async def _buy_sol(usdc_to_spend: float, cfg: ScalperConfig) -> tuple[bool, str]:
    fn = swap_token_jupiter
    if not callable(fn) or usdc_to_spend <= 0.0:
        return False, ""

    # Preferred keyword form first (Jupiter v6-like)
    try:
        out = await _call_maybe_async(
            fn,
            USDC_MINT,
            SOL_MINT,
            amount_float=float(usdc_to_spend),
            buy=True,
            slippage=float(cfg.slippage),
        )
        if isinstance(out, tuple) and len(out) >= 2:
            return bool(out[0]), str(out[1] or "")
        return bool(out), ""
    except TypeError:
        pass
    except Exception as e:
        log.every("SWAP_BUY_FAIL", n=10, msg="SWAP_BUY_FAIL", err=_short(e))
        return False, ""

    # Positional fallback
    try:
        out2 = await _call_maybe_async(
            fn, USDC_MINT, SOL_MINT, float(usdc_to_spend), True, float(cfg.slippage)
        )
        if isinstance(out2, tuple) and len(out2) >= 2:
            return bool(out2[0]), str(out2[1] or "")
        return bool(out2), ""
    except Exception as e:
        log.every("SWAP_BUY_FAIL_2", n=10, msg="SWAP_BUY_FAIL_2", err=_short(e))
        return False, ""


async def _sell_sol(sol_to_sell: float, cfg: ScalperConfig) -> tuple[bool, str]:
    fn = swap_token_jupiter
    if not callable(fn) or sol_to_sell <= 0.0:
        return False, ""

    try:
        out = await _call_maybe_async(
            fn,
            SOL_MINT,
            USDC_MINT,
            amount_float=float(sol_to_sell),
            buy=False,
            slippage=float(cfg.slippage),
        )
        if isinstance(out, tuple) and len(out) >= 2:
            return bool(out[0]), str(out[1] or "")
        return bool(out), ""
    except TypeError:
        pass
    except Exception as e:
        log.every("SWAP_SELL_FAIL", n=10, msg="SWAP_SELL_FAIL", err=_short(e))
        return False, ""

    try:
        out2 = await _call_maybe_async(
            fn, SOL_MINT, USDC_MINT, float(sol_to_sell), False, float(cfg.slippage)
        )
        if isinstance(out2, tuple) and len(out2) >= 2:
            return bool(out2[0]), str(out2[1] or "")
        return bool(out2), ""
    except Exception as e:
        log.every("SWAP_SELL_FAIL_2", n=10, msg="SWAP_SELL_FAIL_2", err=_short(e))
        return False, ""


# -----------------------------------------------------------------------------
# State model
# -----------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class PositionState:
    pos: str  # "FLAT" | "LONG"
    entry_ts: float
    entry_px: float
    size_sol: float
    entry_usd: float
    stop_px: float
    take_px: float
    trail_px: float

    @staticmethod
    def flat() -> "PositionState":
        return PositionState("FLAT", 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)


def _pos_norm(p: str) -> str:
    p2 = (p or "FLAT").strip().upper()
    return p2 if p2 in ("FLAT", "LONG") else "FLAT"


async def _load_state(r: _RedisCompat | None, key: str) -> PositionState:
    if r is None:
        return PositionState.flat()
    raw = await r.get(key)
    if not raw:
        return PositionState.flat()
    try:
        s = _decode_if_bytes(raw)
        obj = json.loads(s)
        if not isinstance(obj, dict):
            return PositionState.flat()
        return PositionState(
            pos=_pos_norm(str(obj.get("pos", "FLAT"))),
            entry_ts=float(obj.get("entry_ts", 0.0) or 0.0),
            entry_px=float(obj.get("entry_px", 0.0) or 0.0),
            size_sol=float(obj.get("size_sol", 0.0) or 0.0),
            entry_usd=float(obj.get("entry_usd", 0.0) or 0.0),
            stop_px=float(obj.get("stop_px", 0.0) or 0.0),
            take_px=float(obj.get("take_px", 0.0) or 0.0),
            trail_px=float(obj.get("trail_px", 0.0) or 0.0),
        )
    except Exception:
        return PositionState.flat()


async def _save_state(r: _RedisCompat | None, key: str, st: PositionState) -> None:
    if r is None:
        return
    payload = json.dumps(asdict(st), separators=(",", ":"), ensure_ascii=False)
    await r.set(key, payload)


# -----------------------------------------------------------------------------
# Risk / breakers
# -----------------------------------------------------------------------------
async def _cooldown_active(r: _RedisCompat | None, cooldown_key: str) -> bool:
    if r is None:
        return False
    until = await r.get(cooldown_key)
    if not until:
        return False
    try:
        return time.time() < float(_decode_if_bytes(until))
    except Exception:
        return False


async def _set_cooldown(r: _RedisCompat | None, cooldown_key: str, cooldown_sec: int) -> None:
    if r is None or cooldown_sec <= 0:
        return
    until = time.time() + float(cooldown_sec)
    await r.set(cooldown_key, str(until), ex=max(cooldown_sec + 10, 20))


async def _trade_count(r: _RedisCompat | None, trades_key: str) -> int:
    if r is None:
        return 0
    v = await r.get(trades_key)
    if v is None:
        return 0
    s = _decode_if_bytes(v).strip()
    with contextlib.suppress(Exception):
        return int(s)
    return 0


async def _inc_trade_count(r: _RedisCompat | None, trades_key: str) -> int:
    if r is None:
        return 0
    v = await r.incr(trades_key)
    await r.expire(trades_key, 172800)
    return int(v)


async def _update_dd(r: _RedisCompat | None, dd_key: str, pnl_key: str, equity_usd: float) -> None:
    if r is None or equity_usd <= 0.0:
        return
    pnl = await _get_float(r, pnl_key, 0.0)
    dd = abs(min(0.0, pnl)) / max(1e-9, equity_usd)
    dd = float(_clamp(dd, 0.0, 1.0))
    await _set_float(r, dd_key, dd, ex_sec=172800)


async def _breaker_ok(
    r: _RedisCompat | None,
    cfg: ScalperConfig,
    equity_usd: float,
    pnl_key: str,
    dd_key: str,
    trades_key: str,
) -> bool:
    pnl = await _get_float(r, pnl_key, 0.0)
    dd = await _get_float(r, dd_key, 0.0)

    if cfg.max_daily_loss_pct > 0.0 and equity_usd > 0.0:
        if pnl <= -abs(cfg.max_daily_loss_pct) * equity_usd:
            return False

    if cfg.max_daily_dd_pct > 0.0:
        if dd >= abs(cfg.max_daily_dd_pct):
            return False

    trades = await _trade_count(r, trades_key)
    if trades >= cfg.max_trades_per_day:
        return False

    return True


# -----------------------------------------------------------------------------
# Alpha / signals
# -----------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class Signals:
    alpha: float
    ema12: float
    ema26: float
    ema200: float
    rsi: float
    atr: float


def _compute_alpha(
    px: float, ema12_v: float, ema26_v: float, ema200_v: float, rsi_v: float
) -> float:
    a = 0.0
    a += 0.42 * (1.0 if ema12_v > ema26_v else 0.0)
    a += 0.33 * (1.0 if (ema200_v > 0.0 and px > ema200_v) else 0.0)
    a += 0.25 * (1.0 if rsi_v >= 50.0 else 0.0)
    return float(_clamp(a, 0.0, 1.0))


# -----------------------------------------------------------------------------
# Main loop
# -----------------------------------------------------------------------------
async def run_scalper(stop_event: asyncio.Event) -> None:
    cfg = ScalperConfig.from_env()
    if (not cfg.enabled) or cfg.mode == "OFF":
        log.info("scalper disabled (SCALPER_ENABLED=0 or mode=OFF).")
        return

    day = _utc_day()
    pnl_key, dd_key, trades_key = cfg.daily_keys(day)

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

    if st.pos == "LONG":
        log.warning(
            "Recovered LONG from state: size_sol=%.6f entry_px=%.4f stop=%.4f take=%.4f trail=%.4f",
            st.size_sol,
            st.entry_px,
            st.stop_px,
            st.take_px,
            st.trail_px,
        )

    last_heartbeat = 0.0

    while not stop_event.is_set():
        try:
            new_day = _utc_day()
            if new_day != day:
                day = new_day
                pnl_key, dd_key, trades_key = cfg.daily_keys(day)
                log.event("SCALPER_DAY_ROLLOVER", day=day, pnl_key=pnl_key, trades_key=trades_key)

            px = await _sol_usd()
            if px <= 0.0:
                if await _sleep_or_stop(stop_event, float(cfg.timeframe_sec)):
                    break
                continue

            prices.append(px)
            ema12_v = ema(ema12_v, px, 12)
            ema26_v = ema(ema26_v, px, 26)
            if len(prices) >= 200:
                ema200_v = ema(ema200_v, px, 200)

            rsi_v = compute_rsi(prices, 14)
            atr_v = compute_atr_close_to_close(prices, 14)
            alpha = _compute_alpha(px, ema12_v, ema26_v, ema200_v, rsi_v)

            sigs = Signals(
                alpha=alpha, ema12=ema12_v, ema26=ema26_v, ema200=ema200_v, rsi=rsi_v, atr=atr_v
            )

            equity = await _equity_usd(px)
            await _update_dd(r, dd_key, pnl_key, equity)

            breakers_ok = await _breaker_ok(r, cfg, equity, pnl_key, dd_key, trades_key)
            cooldown_on = await _cooldown_active(r, cfg.cooldown_key)

            if time.time() - last_heartbeat >= 300:
                pnl = await _get_float(r, pnl_key, 0.0)
                dd = await _get_float(r, dd_key, 0.0)
                trades = await _trade_count(r, trades_key)
                log.info(
                    "[hb] px=%.2f alpha=%.2f rsi=%.1f atr=%.4f pos=%s eq=$%.2f pnl=$%.2f dd=%.3f trades=%d mode=%s",
                    px,
                    sigs.alpha,
                    sigs.rsi,
                    sigs.atr,
                    st.pos,
                    equity,
                    pnl,
                    dd,
                    trades,
                    cfg.mode,
                )
                last_heartbeat = time.time()

            # --------------------
            # Position management
            # --------------------
            if st.pos == "LONG":
                trail_px = st.trail_px
                if cfg.use_trailing and sigs.atr > 0.0:
                    new_trail = px - (cfg.trail_atr_mult * sigs.atr)
                    trail_px = max(trail_px, new_trail) if trail_px > 0.0 else new_trail

                eff_stop = max(st.stop_px, trail_px) if trail_px > 0.0 else st.stop_px
                held_min = (time.time() - st.entry_ts) / 60.0 if st.entry_ts > 0.0 else 0.0

                exit_reason = ""
                if eff_stop > 0.0 and px <= eff_stop:
                    exit_reason = "STOP"
                elif st.take_px > 0.0 and px >= st.take_px:
                    exit_reason = "TAKE"
                elif cfg.max_hold_min > 0 and held_min >= float(cfg.max_hold_min):
                    exit_reason = "TIME"
                elif sigs.alpha < 0.25 and sigs.rsi < 45.0:
                    exit_reason = "ALPHA_FLIP"

                if exit_reason:
                    pnl_usd = (px - st.entry_px) * st.size_sol

                    if cfg.mode == "PAPER":
                        await _incr_float(r, pnl_key, pnl_usd)
                        await _update_dd(r, dd_key, pnl_key, equity)
                        if cfg.telegram_on_paper:
                            await _tg(
                                f"🧾 PAPER EXIT ({exit_reason})\n"
                                f"• px={px:.2f} entry={st.entry_px:.2f} size={st.size_sol:.6f} pnl=${pnl_usd:+.2f}"
                            )
                        log.info("[PAPER EXIT] %s pnl=$%.2f", exit_reason, pnl_usd)
                        st = PositionState.flat()
                        await _save_state(r, cfg.state_key, st)
                        await _set_cooldown(r, cfg.cooldown_key, cfg.cooldown_sec)
                    else:
                        ok, sig = await _sell_sol(st.size_sol, cfg)
                        if ok:
                            await _incr_float(r, pnl_key, pnl_usd)
                            await _update_dd(r, dd_key, pnl_key, equity)
                            if cfg.telegram_on_trade:
                                await _tg(
                                    f"✅ EXIT ({exit_reason})\n"
                                    f"• px={px:.2f} entry={st.entry_px:.2f} size={st.size_sol:.6f}\n"
                                    f"• pnl=${pnl_usd:+.2f} • tx={sig}"
                                )
                            log.info("[EXIT] %s pnl=$%.2f tx=%s", exit_reason, pnl_usd, sig)
                            st = PositionState.flat()
                            await _save_state(r, cfg.state_key, st)
                            await _set_cooldown(r, cfg.cooldown_key, cfg.cooldown_sec)
                        else:
                            log.warning(
                                "Exit attempt failed (%s). Will retry next bar.", exit_reason
                            )
                            if cfg.telegram_on_trade:
                                await _tg(f"⚠️ EXIT failed ({exit_reason}) — will retry next bar.")
                else:
                    if trail_px != st.trail_px:
                        st = PositionState(
                            pos="LONG",
                            entry_ts=st.entry_ts,
                            entry_px=st.entry_px,
                            size_sol=st.size_sol,
                            entry_usd=st.entry_usd,
                            stop_px=st.stop_px,
                            take_px=st.take_px,
                            trail_px=float(trail_px),
                        )
                        await _save_state(r, cfg.state_key, st)

            # --------------------
            # Entry logic
            # --------------------
            if st.pos == "FLAT":
                if (not breakers_ok) or cooldown_on or (len(prices) < 200) or (sigs.atr <= 0.0):
                    sleep_for = float(cfg.timeframe_sec) + random.uniform(0.0, 0.7)
                    if await _sleep_or_stop(stop_event, sleep_for):
                        break
                    continue

                trend_ok = (ema12_v > ema26_v) and (ema200_v > 0.0 and px > ema200_v)
                confirm_ok = sigs.rsi >= cfg.rsi_bull
                score_ok = sigs.alpha >= cfg.alpha_min

                if trend_ok and confirm_ok and score_ok:
                    stop_px = px - (cfg.stop_atr_mult * sigs.atr)
                    take_px = px + (cfg.tp_atr_mult * sigs.atr)

                    sol_bal, usdc_bal = await _balances()

                    if sol_bal < cfg.min_sol_fee_reserve:
                        log.warning(
                            "SOL fee reserve low (sol=%.4f < %.4f). Skipping entry.",
                            sol_bal,
                            cfg.min_sol_fee_reserve,
                        )
                    else:
                        if usdc_bal >= cfg.min_trade_usd:
                            usd_risk = max(cfg.min_trade_usd, equity * cfg.risk_per_trade)
                            usd_to_use = _clamp(usd_risk, cfg.min_trade_usd, cfg.max_trade_usd)
                            usd_to_use = min(usd_to_use, usdc_bal)

                            if usd_to_use >= cfg.min_trade_usd:
                                size_sol = float(usd_to_use / max(1e-9, px))

                                if cfg.mode == "PAPER":
                                    st = PositionState(
                                        pos="LONG",
                                        entry_ts=time.time(),
                                        entry_px=float(px),
                                        size_sol=float(size_sol),
                                        entry_usd=float(usd_to_use),
                                        stop_px=float(stop_px),
                                        take_px=float(take_px),
                                        trail_px=0.0,
                                    )
                                    await _save_state(r, cfg.state_key, st)
                                    await _inc_trade_count(r, trades_key)
                                    if cfg.telegram_on_paper:
                                        await _tg(
                                            "🧾 PAPER ENTRY\n"
                                            f"• px={px:.2f} size≈{size_sol:.6f} SOL spend≈${usd_to_use:.2f}\n"
                                            f"• stop={stop_px:.2f} take={take_px:.2f} alpha={sigs.alpha:.2f} rsi={sigs.rsi:.1f}"
                                        )
                                    log.info(
                                        "[PAPER ENTRY] px=%.2f size=%.6f stop=%.2f take=%.2f",
                                        px,
                                        size_sol,
                                        stop_px,
                                        take_px,
                                    )
                                    await _set_cooldown(r, cfg.cooldown_key, cfg.cooldown_sec)
                                else:
                                    ok, sig = await _buy_sol(usd_to_use, cfg)
                                    if ok:
                                        st = PositionState(
                                            pos="LONG",
                                            entry_ts=time.time(),
                                            entry_px=float(px),
                                            size_sol=float(size_sol),
                                            entry_usd=float(usd_to_use),
                                            stop_px=float(stop_px),
                                            take_px=float(take_px),
                                            trail_px=0.0,
                                        )
                                        await _save_state(r, cfg.state_key, st)
                                        await _inc_trade_count(r, trades_key)
                                        if cfg.telegram_on_trade:
                                            await _tg(
                                                "🟢 ENTRY (scalper)\n"
                                                f"• px={px:.2f} spend=${usd_to_use:.2f} size≈{size_sol:.6f} SOL\n"
                                                f"• stop={stop_px:.2f} take={take_px:.2f} alpha={sigs.alpha:.2f} rsi={sigs.rsi:.1f}\n"
                                                f"• tx={sig}"
                                            )
                                        log.info(
                                            "[ENTRY] px=%.2f spend=$%.2f tx=%s", px, usd_to_use, sig
                                        )
                                        await _set_cooldown(r, cfg.cooldown_key, cfg.cooldown_sec)
                                    else:
                                        log.warning(
                                            "Entry swap failed; will retry if setup persists."
                                        )
                                        if cfg.telegram_on_trade:
                                            await _tg(
                                                "⚠️ ENTRY failed (swap) — will retry if setup persists."
                                            )

            sleep_for2 = float(cfg.timeframe_sec) + random.uniform(0.0, 0.7)
            if await _sleep_or_stop(stop_event, sleep_for2):
                break

        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.every("SCALPER_LOOP_ERR", n=5, msg="SCALPER_LOOP_ERR", err=_short(e))
            if await _sleep_or_stop(stop_event, 3.0):
                break


async def main() -> None:
    # Configure logging for standalone runs only (no import-time side effects)
    with contextlib.suppress(Exception):
        from utils_pkg.logging_setup import setup_logging as _s1  # type: ignore

        await _call_maybe_async(_s1, service="scalper")

    with contextlib.suppress(Exception):
        from logging_setup import setup_logging as _s2  # type: ignore

        await _call_maybe_async(_s2, service="scalper")

    if callable(init_resources):
        with contextlib.suppress(Exception):
            await _call_maybe_async(init_resources)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _stop(*_: Any) -> None:
        stop_event.set()

    for sig_name in ("SIGINT", "SIGTERM"):
        sig_obj = getattr(signal, sig_name, None)
        if sig_obj is None:
            continue
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig_obj, _stop)

    try:
        await run_scalper(stop_event)
    finally:
        if callable(close_resources):
            with contextlib.suppress(Exception):
                await _call_maybe_async(close_resources)


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
