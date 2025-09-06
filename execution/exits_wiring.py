# execution/exits_wiring.py — v1.0 (supervisor + registry + public exports)
from __future__ import annotations

import os
import time
import asyncio
from dataclasses import dataclass
from typing import Optional, Dict, Any, Callable, Awaitable

# Engine (pure logic + executor)
from execution.exits import (
    PositionState,
    apply_exit,
    exits_config_summary,
    SellFn,  # type: Callable[[str, float, str], Awaitable[tuple[bool, Optional[str], dict]]]
)

# ---- optional logging (tolerant) --------------------------------------------
try:
    from db.logger import log_trade_event
except Exception:  # no logger is fine
    async def log_trade_event(*_a, **_k): ...

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
def _env_float(name: str, default: float) -> float:
    try:
        v = os.getenv(name)
        return float(v) if v not in (None, "") else float(default)
    except Exception:
        return float(default)

def _env_int(name: str, default: int) -> int:
    try:
        v = os.getenv(name)
        return int(v) if v not in (None, "") else int(default)
    except Exception:
        return int(default)

TICK_SEC         = _env_float("EXIT_TICK_SEC", 2.0)        # supervisor tick
CONCURRENCY      = _env_int("EXIT_CONCURRENCY", 6)         # concurrent apply_exit per tick
LOG_EVERY_TICKS  = _env_int("EXIT_LOG_EVERY", 15)          # heartbeat

# Optional helper if someone passes SOL notional instead of USD
USD_PER_SOL      = _env_float("USD_PER_SOL", 150.0)

# -----------------------------------------------------------------------------
# Types
# -----------------------------------------------------------------------------
PriceFn = Callable[[str], Awaitable[Optional[float]]]

@dataclass
class Managed:
    pos: PositionState
    sell_fn: SellFn
    price_fn: PriceFn
    key: str                 # unique key (we use buy signature)

# -----------------------------------------------------------------------------
# Globals (simple in-mem registry)
# -----------------------------------------------------------------------------
_POS: Dict[str, Managed] = {}
_SUP_TASK: Optional[asyncio.Task] = None
_SUP_LOCK = asyncio.Lock()
_TICK = 0

__all__ = [
    "start_exit_supervisor",
    "stop_exit_supervisor",
    "register_entry",
    "positions_count",
    "get_exit_status",
]

# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------
def register_entry(
    signature: str,
    mint: str,
    qty: Optional[int],
    entry_price: float,
    *,
    symbol: Optional[str] = None,
    usd_spent: Optional[float] = None,
    size_usd: Optional[float] = None,
    sol_spent: Optional[float] = None,
    sell_fn: SellFn,
    price_fn: PriceFn,
    entry_ts: Optional[float] = None,
) -> None:
    """
    Register a newly-filled position so the supervisor can manage exits.
    We slice by USD; token qty is not required.
    """
    # Resolve notional
    notional_usd: Optional[float] = None
    for cand in (usd_spent, size_usd):
        if cand is not None:
            notional_usd = float(cand)
            break
    if notional_usd is None and sol_spent is not None:
        notional_usd = float(sol_spent) * USD_PER_SOL
    if notional_usd is None:
        raise ValueError("register_entry: usd_spent/size_usd/sol_spent missing")

    pos = PositionState(
        mint=mint,
        entry_px=float(entry_price),
        size_usd=float(notional_usd),
        symbol=symbol,
        entry_ts=float(entry_ts or time.time()),
    )

    _POS[signature] = Managed(
        pos=pos,
        sell_fn=sell_fn,
        price_fn=price_fn,
        key=signature,
    )

    # fire-and-forget structured event
    try:
        asyncio.create_task(log_trade_event(
            type="entry_registered",
            mint=mint,
            symbol=symbol,
            entry_sol_per_token=entry_price,
            sol_spent=None,
            usd_spent=notional_usd,
            signature=signature,
            cu_price=None,
            jito_tip=None,
            data={"exits_cfg": exits_config_summary()},
        ))
    except Exception:
        pass


def positions_count() -> int:
    return len(_POS)


def get_exit_status() -> Dict[str, Any]:
    """
    Lightweight snapshot for debugging/metrics.
    """
    snap = {}
    for k, m in _POS.items():
        p = m.pos
        snap[k] = {
            "mint": p.mint,
            "symbol": p.symbol,
            "entry_px": p.entry_px,
            "peak_px": p.peak_px,
            "size_usd": p.size_usd,
            "tp1_done": p.tp1_done,
            "tp2_done": p.tp2_done,
            "entry_ts": p.entry_ts,
            "last_action_ts": p.last_action_ts,
        }
    return snap


async def stop_exit_supervisor():
    global _SUP_TASK
    if _SUP_TASK and not _SUP_TASK.done():
        _SUP_TASK.cancel()
        try:
            await _SUP_TASK
        except Exception:
            pass
    _SUP_TASK = None


def start_exit_supervisor(loop: Optional[asyncio.AbstractEventLoop] = None) -> asyncio.Task:
    """
    Idempotent starter. Returns the running Task.
    """
    global _SUP_TASK

    if _SUP_TASK and not _SUP_TASK.done():
        return _SUP_TASK

    # get a loop
    try:
        loop = loop or asyncio.get_running_loop()
    except RuntimeError:  # not in async context
        loop = asyncio.get_event_loop()

    async def _starter():
        async with _SUP_LOCK:
            # double-check inside lock
            if _SUP_TASK and not _SUP_TASK.done():
                return
            task = asyncio.create_task(_supervisor_loop(), name="exit_supervisor")
            # assign to global only after successful create
            globals()["_SUP_TASK"] = task

    # run the small starter in loop context (safe if already in loop)
    if loop.is_running():
        asyncio.create_task(_starter())
    else:
        loop.run_until_complete(_starter())

    return _SUP_TASK


# -----------------------------------------------------------------------------
# Internals
# -----------------------------------------------------------------------------
async def _supervisor_loop():
    global _TICK
    sem = asyncio.Semaphore(CONCURRENCY)
    hb_next = LOG_EVERY_TICKS

    # one-time: log config
    try:
        await log_trade_event(
            type="exit_supervisor_started",
            mint=None, symbol=None,
            entry_sol_per_token=None,
            sol_spent=None, usd_spent=None,
            signature=None,
            cu_price=None, jito_tip=None,
            data={"tick_sec": TICK_SEC, "concurrency": CONCURRENCY, "cfg": exits_config_summary()},
        )
    except Exception:
        pass

    while True:
        _TICK += 1

        # build a stable list of current managed positions
        items = list(_POS.items())

        async def _handle(key: str, m: Managed):
            async with sem:
                # fetch latest price
                px = None
                try:
                    px = await m.price_fn(m.pos.mint)
                except Exception:
                    return  # skip this round on price errors

                if px is None:
                    return

                # execute decision
                try:
                    dec = await apply_exit(m.pos, float(px), m.sell_fn)
                except Exception:
                    return

                # if fully exited, drop from registry
                if m.pos.size_usd <= 0.0 or dec.action == "SELL_ALL":
                    _POS.pop(key, None)

        # run concurrently with cap
        tasks = [asyncio.create_task(_handle(k, m)) for k, m in items]
        if tasks:
            try:
                await asyncio.gather(*tasks, return_exceptions=True)
            except Exception:
                pass

        # heartbeat
        hb_next -= 1
        if hb_next <= 0:
            hb_next = LOG_EVERY_TICKS
            try:
                await log_trade_event(
                    type="exit_supervisor_heartbeat",
                    mint=None, symbol=None,
                    entry_sol_per_token=None,
                    sol_spent=None, usd_spent=None,
                    signature=None, cu_price=None, jito_tip=None,
                    data={"tick": _TICK, "managed": positions_count()},
                )
            except Exception:
                pass

        await asyncio.sleep(TICK_SEC)
