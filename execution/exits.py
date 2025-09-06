# execution/exits.py — v2.4 (exit engine + supervisor + registry)
from __future__ import annotations

import os
import time
import asyncio
import logging
from dataclasses import dataclass, field
from typing import Optional, Tuple, Dict, Any, Callable, Awaitable

log = logging.getLogger(__name__)

# ---- DB logging (compatible with your new logger) -----------------------------
try:
    from db.logger import log_trade_event, record_legacy_trade
except Exception:
    async def log_trade_event(*_a, **_k): ...
    async def record_legacy_trade(*_a, **_k): ...

# ==============================================================================
# Config helpers
# ==============================================================================

def _env_float(name: str, default: float) -> float:
    try:
        v = os.getenv(name)
        return float(v) if v not in (None, "") else float(default)
    except Exception:
        return float(default)

def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    if v in ("1", "true", "yes", "y", "on"): return True
    if v in ("0", "false", "no", "n", "off"): return False
    return default

def _normalize_stop_pct(v: float) -> float:
    # Accept either -30 (old style) or 30 (new style) for a 30% hard stop.
    return -abs(v) if v >= 0 else v

# ----------------------- Tunables (env-driven) ---------------------------------
HARD_STOP_PCT = _normalize_stop_pct(_env_float("EXIT_HARD_STOP_PCT", _env_float("HARD_STOP_PCT", -30.0)))

TP1_AT_X       = _env_float("TP1_AT_X", 2.0)
TP1_TAKE_PCT   = _env_float("TP1_TAKE_PCT", 40.0)

TP2_AT_X       = _env_float("TP2_AT_X", 3.0)
TP2_TAKE_PCT   = _env_float("TP2_TAKE_PCT", 30.0)   # set 0 to disable

TRAIL_FROM_PEAK_PCT = _env_float("TRAIL_FROM_PEAK_PCT", 20.0)  # sell-all if -20% off peak (armed)
TRAIL_ARM_AFTER_X   = _env_float("TRAIL_ARM_AFTER_X", 2.0)     # or once TP hit

BREAKEVEN_ARM_AFTER_X = _env_float("BREAKEVEN_ARM_AFTER_X", 1.4)
BREAKEVEN_STOP_PCT    = _normalize_stop_pct(_env_float("BREAKEVEN_STOP_PCT", -2.0))  # 0 = never let it go red after armed

MIN_HOLD_SEC   = int(_env_float("EXIT_MIN_HOLD_SEC", 30.0))
COOL_OFF_SEC   = int(_env_float("EXIT_COOL_OFF_SEC", 15.0))

EXIT_TICK_SEC  = _env_float("EXIT_TICK_SEC", 2.0)
EXITS_ENABLED  = _env_bool("EXITS_ENABLED", True)

# If you register positions using SOL sizes and want USD inferred:
USD_PER_SOL_ENV = _env_float("USD_PER_SOL", 0.0)  # optional hint

# ==============================================================================
# Position & decisions
# ==============================================================================

@dataclass
class PositionState:
    mint: str
    entry_px: float            # in same units as your price_fn returns (e.g., USD/token or SOL/token)
    size_usd: float            # remaining USD value to manage (we sell percentages of this)
    symbol: Optional[str] = None
    entry_ts: float = field(default_factory=lambda: time.time())
    peak_px: float = None
    tp1_done: bool = False
    tp2_done: bool = False
    last_action_ts: float = field(default_factory=float)

    def __post_init__(self):
        if self.peak_px is None:
            self.peak_px = float(self.entry_px)

@dataclass
class ExitDecision:
    action: str                    # 'HOLD' | 'SELL_PARTIAL' | 'SELL_ALL'
    sell_pct: float = 0.0
    reason: str = ""
    take_usd: float = 0.0
    pnl_usd: float = 0.0

# ==============================================================================
# Core math helpers
# ==============================================================================

def _pct_change(curr: float, ref: float) -> float:
    if ref <= 0: return 0.0
    return (curr / ref - 1.0) * 100.0

def _dd_from_peak_pct(curr: float, peak: float) -> float:
    if peak <= 0: return 0.0
    return (curr / peak - 1.0) * 100.0   # negative when below peak

# ==============================================================================
# Pure decision engine (no side effects)
# ==============================================================================

def decide_exit(pos: PositionState, last_px: float, now_ts: Optional[float] = None) -> ExitDecision:
    now = now_ts or time.time()
    last_px = float(last_px)

    # Time guards
    if now - pos.entry_ts < MIN_HOLD_SEC:
        return ExitDecision(action="HOLD", reason="min_hold")
    if pos.last_action_ts and (now - pos.last_action_ts) < COOL_OFF_SEC:
        return ExitDecision(action="HOLD", reason="cooloff")

    # Virtual peak update for decision
    peak_px = max(pos.peak_px, last_px)
    chg_pct = _pct_change(last_px, pos.entry_px)
    dd_pct  = _dd_from_peak_pct(last_px, peak_px)

    # 1) Hard stop
    if chg_pct <= HARD_STOP_PCT:
        return ExitDecision(action="SELL_ALL", reason="hard_stop")

    # 2) TP1
    if (not pos.tp1_done) and (last_px >= pos.entry_px * TP1_AT_X):
        sell_pct = max(0.0, min(100.0, TP1_TAKE_PCT))
        take_usd = pos.size_usd * (sell_pct / 100.0)
        pnl_usd  = (last_px / pos.entry_px - 1.0) * take_usd
        return ExitDecision("SELL_PARTIAL", sell_pct, "tp1", take_usd, pnl_usd)

    # 3) TP2
    if (TP2_TAKE_PCT > 0) and (not pos.tp2_done) and (last_px >= pos.entry_px * TP2_AT_X):
        sell_pct = max(0.0, min(100.0, TP2_TAKE_PCT))
        take_usd = pos.size_usd * (sell_pct / 100.0)
        pnl_usd  = (last_px / pos.entry_px - 1.0) * take_usd
        return ExitDecision("SELL_PARTIAL", sell_pct, "tp2", take_usd, pnl_usd)

    # Arming conditions
    armed_by_tp  = pos.tp1_done or pos.tp2_done
    armed_by_x   = (last_px >= pos.entry_px * TRAIL_ARM_AFTER_X)
    armed_by_be  = (last_px >= pos.entry_px * BREAKEVEN_ARM_AFTER_X)

    # 4) Trailing from peak (only when armed)
    if (armed_by_tp or armed_by_x) and dd_pct <= -abs(TRAIL_FROM_PEAK_PCT):
        return ExitDecision("SELL_ALL", 100.0, "trailing_peak", pos.size_usd, (last_px / pos.entry_px - 1.0) * pos.size_usd)

    # 5) Breakeven stop once armed
    if BREAKEVEN_STOP_PCT < 0 and armed_by_be and chg_pct <= BREAKEVEN_STOP_PCT:
        return ExitDecision("SELL_ALL", 100.0, "breakeven_stop", pos.size_usd, (last_px / pos.entry_px - 1.0) * pos.size_usd)

    return ExitDecision("HOLD", 0.0, "none", 0.0, 0.0)

# ==============================================================================
# Side-effectful executor
# ==============================================================================

SellFn  = Callable[[str, float, str], Awaitable[Tuple[bool, Optional[str], Dict[str, Any]]]]
PriceFn = Callable[[str], Awaitable[Optional[float]]]

async def apply_exit(
    pos: PositionState,
    last_px: float,
    sell_fn: SellFn,
    now_ts: Optional[float] = None,
) -> ExitDecision:
    now = now_ts or time.time()
    dec = decide_exit(pos, last_px, now)

    if dec.action == "HOLD":
        return dec

    # Determine USD chunk to sell
    usd_to_sell = dec.take_usd
    sell_pct = dec.sell_pct
    if dec.action == "SELL_ALL":
        usd_to_sell = pos.size_usd
        sell_pct = 100.0

    if usd_to_sell <= 0.0:
        return ExitDecision(action="HOLD", reason="empty_position")

    # Execute the sale
    try:
        ok, signature, meta = await sell_fn(pos.mint, float(usd_to_sell), dec.reason)
    except Exception as e:
        ok, signature, meta = False, None, {"exc": repr(e)}
        log.exception("sell_fn crashed for %s", pos.mint)

    pnl_usd = (last_px / pos.entry_px - 1.0) * usd_to_sell

    # Structured event log
    try:
        await log_trade_event(
            type="sell" if ok else "sell_fail",
            mint=pos.mint,
            symbol=pos.symbol,
            entry_sol_per_token=pos.entry_px,  # just "entry price" unit == last_px unit
            sol_spent=0.0,
            usd_spent=usd_to_sell * -1.0,
            signature=signature,
            cu_price=(meta or {}).get("cu_price"),
            jito_tip=(meta or {}).get("jito_tip"),
            data={"reason": dec.reason, "sell_pct": sell_pct, "last_px": last_px, "pnl_usd_est": pnl_usd},
        )
    except Exception:
        pass

    # Legacy row
    try:
        await record_legacy_trade(
            mint=pos.mint,
            entry_px=pos.entry_px,
            exit_px=last_px,
            size_usd=usd_to_sell,
            pnl_usd=pnl_usd,
            reason=dec.reason if ok else f"{dec.reason}_fail",
        )
    except Exception:
        pass

    if ok:
        pos.size_usd = max(0.0, pos.size_usd - usd_to_sell)
        pos.last_action_ts = now
        pos.peak_px = max(pos.peak_px, last_px)
        if dec.reason == "tp1": pos.tp1_done = True
        if dec.reason == "tp2": pos.tp2_done = True

    # If position is flat, supervisor will drop it after this tick.
    return ExitDecision(
        action="HOLD" if not ok else ("HOLD" if pos.size_usd > 0 else "SELL_ALL"),
        sell_pct=sell_pct,
        reason=dec.reason if ok else f"{dec.reason}_fail",
        take_usd=usd_to_sell,
        pnl_usd=pnl_usd,
    )

# Back-compat helper
def on_tick(pos: PositionState, last_px: float) -> str:
    d = decide_exit(pos, last_px)
    return "EXIT_ALL" if d.action == "SELL_ALL" else "HOLD"

def exits_config_summary() -> Dict[str, Any]:
    return {
        "HARD_STOP_PCT": HARD_STOP_PCT,
        "TP1_AT_X": TP1_AT_X,
        "TP1_TAKE_PCT": TP1_TAKE_PCT,
        "TP2_AT_X": TP2_AT_X,
        "TP2_TAKE_PCT": TP2_TAKE_PCT,
        "TRAIL_FROM_PEAK_PCT": TRAIL_FROM_PEAK_PCT,
        "TRAIL_ARM_AFTER_X": TRAIL_ARM_AFTER_X,
        "BREAKEVEN_ARM_AFTER_X": BREAKEVEN_ARM_AFTER_X,
        "BREAKEVEN_STOP_PCT": BREAKEVEN_STOP_PCT,
        "MIN_HOLD_SEC": MIN_HOLD_SEC,
        "COOL_OFF_SEC": COOL_OFF_SEC,
        "EXIT_TICK_SEC": EXIT_TICK_SEC,
        "EXITS_ENABLED": EXITS_ENABLED,
    }

# ==============================================================================
# Registry + Supervisor
# ==============================================================================

@dataclass
class PositionCtx:
    pos: PositionState
    sell_fn: SellFn
    price_fn: PriceFn
    buy_signature: Optional[str] = None

_positions: Dict[str, PositionCtx] = {}
_lock = asyncio.Lock()
_supervisor_task: Optional[asyncio.Task] = None
_running = False

async def _tick_one(ctx: PositionCtx) -> None:
    """Handle one position for a single tick; swallow all exceptions."""
    try:
        price = await ctx.price_fn(ctx.pos.mint)
    except Exception as e:
        log.warning("price_fn error for %s: %s", ctx.pos.mint, e)
        return

    if not price or price <= 0:
        # No fresh price; skip softly.
        return

    try:
        dec = await apply_exit(ctx.pos, float(price), ctx.sell_fn)
    except Exception:
        log.exception("apply_exit crashed for %s", ctx.pos.mint)
        return

    # If fully exited, drop from registry
    if ctx.pos.size_usd <= 0.0 or dec.action == "SELL_ALL":
        async with _lock:
            _positions.pop(ctx.pos.mint, None)
        log.info("Position closed & deregistered: %s", ctx.pos.mint)

async def _supervisor_loop():
    global _running
    log.info("Exit supervisor started (tick=%.2fs, enabled=%s)", EXIT_TICK_SEC, EXITS_ENABLED)
    try:
        while _running:
            if not EXITS_ENABLED:
                await asyncio.sleep(EXIT_TICK_SEC)
                continue

            # Snapshot to iterate without holding the lock across awaits
            async with _lock:
                items = list(_positions.values())

            # Tick all positions concurrently, but keep it bounded
            tasks = [asyncio.create_task(_tick_one(ctx)) for ctx in items]
            if tasks:
                # Avoid "Task exception was never retrieved"
                done, _ = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)

            await asyncio.sleep(EXIT_TICK_SEC)
    finally:
        log.info("Exit supervisor stopped")

# ---- exit supervisor + entry registry ----------------------------------------
import logging
from dataclasses import dataclass
from typing import Awaitable, Callable, Dict, Optional

logger = logging.getLogger(__name__)

PriceFn = Callable[[str], Awaitable[Optional[float]]]
SellFn  = Callable[[str, float, str], Awaitable[tuple]]  # (ok, sig, meta)

@dataclass
class ManagedPos:
    pos: PositionState
    price_fn: PriceFn
    sell_fn: SellFn

_positions: Dict[str, ManagedPos] = {}
_positions_lock = asyncio.Lock()
_supervisor_task: Optional[asyncio.Task] = None

def _units_note() -> str:
    return "⚠ entry_price units must match price_fn units (USD/token or SOL/token)."

def _resolve_size_usd(
    usd_spent: Optional[float],
    size_usd: Optional[float],
    sol_spent: Optional[float],
    usd_per_sol: Optional[float],
) -> Optional[float]:
    if usd_spent is not None:
        return float(usd_spent)
    if size_usd is not None:
        return float(size_usd)
    if sol_spent is not None and usd_per_sol:
        return float(sol_spent) * float(usd_per_sol)
    return None

def register_entry(
    signature: str,
    mint: str,
    qty: Optional[float],
    entry_price: float,
    *,
    symbol: Optional[str] = None,
    usd_spent: Optional[float] = None,
    size_usd: Optional[float] = None,
    sol_spent: Optional[float] = None,
    usd_per_sol: Optional[float] = None,
    price_fn: PriceFn,
    sell_fn: SellFn,
) -> None:
    """
    Register a freshly filled position so the supervisor can manage exits.
    - entry_price: must be in the same units returned by price_fn.  << VERY IMPORTANT >>
    - Prefer supplying usd_spent (or size_usd). If you only have sol_spent, also give usd_per_sol.
    """
    size = _resolve_size_usd(usd_spent, size_usd, sol_spent, usd_per_sol)
    if size is None or size <= 0:
        logger.warning("register_entry: cannot resolve size_usd (usd_spent/size_usd/sol_spent missing). %s", _units_note())
        return

    ps = PositionState(mint=mint, entry_px=float(entry_price), size_usd=float(size), symbol=symbol)

    async def _put():
        async with _positions_lock:
            _positions[mint] = ManagedPos(pos=ps, price_fn=price_fn, sell_fn=sell_fn)
        try:
            await log_trade_event(
                type="buy_registered",
                mint=mint,
                symbol=symbol,
                entry_sol_per_token=ps.entry_px,  # same unit as price_fn
                sol_spent=None,
                usd_spent=size,
                signature=signature,
                cu_price=None,
                jito_tip=None,
                data={"qty": qty, "note": "registered for exit mgmt"},
            )
        except Exception:
            pass
        logger.info("Registered position: mint=%s size_usd=%.2f entry_px=%.8f", mint, ps.size_usd, ps.entry_px)

    asyncio.get_event_loop().create_task(_put())

async def _exit_supervisor_loop(tick_sec: float = 2.0):
    logger.info("Exit supervisor started (tick=%.1fs, positions=%d)", tick_sec, len(_positions))
    while True:
        try:
            async with _positions_lock:
                items = list(_positions.items())
            for mint, mp in items:
                try:
                    px = await mp.price_fn(mint)
                except Exception as e:
                    logger.debug("price_fn error for %s: %r", mint, e)
                    px = None

                if px is None:
                    # no price → nothing to decide this tick
                    continue

                dec = await apply_exit(mp.pos, last_px=float(px), sell_fn=mp.sell_fn)
                # If position fully closed, remove it
                if mp.pos.size_usd <= 1e-9:
                    async with _positions_lock:
                        _positions.pop(mint, None)
                    logger.info("Position closed: %s (reason=%s)", mint, dec.reason)
        except Exception as e:
            logger.exception("Exit supervisor tick error: %r", e)

        await asyncio.sleep(tick_sec)

def start_exit_supervisor(tick_sec: float = 2.0) -> None:
    global _supervisor_task
    loop = asyncio.get_event_loop()
    if _supervisor_task and not _supervisor_task.done():
        logger.debug("Exit supervisor already running")
        return
    _supervisor_task = loop.create_task(_exit_supervisor_loop(tick_sec=tick_sec))
    logger.info("Exit supervisor task spawned")

# ------------------------------- Public API -----------------------------------

def start_exit_supervisor(loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
    """
    Starts the background exit supervisor once.
    Call this once at startup, after event loop exists.
    """
    global _supervisor_task, _running
    if _supervisor_task and not _supervisor_task.done():
        return
    _running = True
    if loop is None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.get_event_loop()
    _supervisor_task = loop.create_task(_supervisor_loop())

def stop_exit_supervisor() -> None:
    """Stops the supervisor loop; it will finish current tick and exit."""
    global _running
    _running = False

def has_position(mint: str) -> bool:
    return mint in _positions

def list_positions() -> Dict[str, Dict[str, Any]]:
    return {
        m: {
            "symbol": ctx.pos.symbol,
            "size_usd": ctx.pos.size_usd,
            "entry_px": ctx.pos.entry_px,
            "tp1_done": ctx.pos.tp1_done,
            "tp2_done": ctx.pos.tp2_done,
            "entry_ts": ctx.pos.entry_ts,
            "peak_px": ctx.pos.peak_px,
        }
        for m, ctx in _positions.items()
    }

def remove_position(mint: str) -> None:
    """Manual purge (e.g., after manual close)."""
    _positions.pop(mint, None)

def _infer_size_usd(
    *, size_usd: Optional[float], usd_spent: Optional[float], sol_spent: Optional[float]
) -> Optional[float]:
    if size_usd is not None: return float(size_usd)
    if usd_spent is not None: return float(usd_spent)
    if sol_spent is not None and USD_PER_SOL_ENV > 0:
        return float(sol_spent) * USD_PER_SOL_ENV
    return None

def register_entry(
    signature: str,
    mint: str,
    qty: Optional[float],
    entry_price: float,
    *,
    symbol: Optional[str] = None,
    size_usd: Optional[float] = None,      # preferred if you have it
    usd_spent: Optional[float] = None,     # alt: total USD committed
    sol_spent: Optional[float] = None,     # alt: if you spent SOL and set USD_PER_SOL
    sell_fn: Optional[SellFn] = None,
    price_fn: Optional[PriceFn] = None,
) -> None:
    """
    Registers/updates a position so the supervisor can manage exits.

    Required:
      - signature: buy tx sig (for tracking)
      - mint: token mint
      - entry_price: same units as price_fn returns (USD/token or SOL/token)
      - price_fn(mint) -> float | None
      - sell_fn(mint, usd_to_sell, reason) -> (ok, signature, meta)

    One of (size_usd | usd_spent | sol_spent) must be provided to size the position.
    qty is accepted for compatibility but not used directly here (exits use USD slices).
    """
    if price_fn is None:
        raise ValueError("register_entry requires a price_fn(mint) -> float|None")
    if sell_fn is None:
        raise ValueError("register_entry requires a sell_fn(mint, usd_to_sell, reason)")

    sized = _infer_size_usd(size_usd=size_usd, usd_spent=usd_spent, sol_spent=sol_spent)
    if sized is None or sized <= 0:
        log.warning("register_entry: could not determine size_usd for %s (mint=%s). Exits will be NO-OP.",
                    symbol or "", mint)
        sized = 0.0

    pos = PositionState(
        mint=mint,
        entry_px=float(entry_price),
        size_usd=float(sized),
        symbol=symbol,
    )
    ctx = PositionCtx(pos=pos, sell_fn=sell_fn, price_fn=price_fn, buy_signature=signature)

    # Upsert into registry
    async def _do():
        async with _lock:
            _positions[mint] = ctx
        log.info("Registered position: mint=%s symbol=%s size_usd=%.4f entry_px=%.8f",
                 mint, symbol, pos.size_usd, pos.entry_px)

    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_do())
    except RuntimeError:
        # If called before loop is running (e.g., sync context), do it synchronously
        # (safe because there are no awaits inside the lock usage at this moment).
        import contextlib
        with contextlib.suppress(Exception):
            # Initialize a temporary loop only to set the dict; fallback to direct
            _positions[mint] = ctx
        log.info("Registered position (sync): mint=%s symbol=%s size_usd=%.4f entry_px=%.8f",
                 mint, symbol, pos.size_usd, pos.entry_px)

__all__ = [
    "PositionState",
    "ExitDecision",
    "decide_exit",
    "apply_exit",
    "on_tick",
    "exits_config_summary",
    # Registry / supervisor exports
    "start_exit_supervisor",
    "stop_exit_supervisor",
    "register_entry",
    "has_position",
    "list_positions",
    "remove_position",
]

# --- passthrough exports so imports don't break ---
try:
    from .exits_wiring import register_entry, start_exit_supervisor
except Exception:
    def register_entry(*_a, **_k): ...
    def start_exit_supervisor(*_a, **_k): ...

