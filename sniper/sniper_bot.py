# sniper/sniper_bot.py
"""
Sniper Orchestrator:
- Initializes shared resources (HTTP, Redis, Solana, OpenAI)
- Runs the Sniper scanner/buyer loop
- Optionally runs Risk Manager (trailing stop / stop-loss / reinvest)
- Optionally runs Auto-Sell sweeper (wallet sweep for unmanaged positions)
- Supervises tasks with backoff restarts
"""

from __future__ import annotations

import os
import sys
import asyncio
import logging
import signal
import random
from typing import Callable, Awaitable, Dict
from contextlib import suppress

# Ensure project root is importable even if run from subdir
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# --- Core shared resources ---
from utils_pkg.core_utils import init_resources, close_resources  # noqa

# --- Main Sniper loop ---
from .sniper import Sniper  # noqa

# --- Optional loops ---
# auto-sell lives in the same folder (sniper/auto_sell.py)
try:
    from .auto_sell import monitor_positions as auto_sell_monitor  # noqa
    HAS_AUTO_SELL = True
except Exception:
    HAS_AUTO_SELL = False

# risk_manager lives at repo root (risk_manager.py)
try:
    from risk_manager import monitor_positions_loop as risk_manager_loop  # noqa
    HAS_RISK_MANAGER = True
except Exception:
    HAS_RISK_MANAGER = False


logger = logging.getLogger("sniper_bot")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s: %(message)s",
)

# --------- config (env + light CLI flags) --------- #

ENABLE_RISK_MANAGER = os.getenv("ENABLE_RISK_MANAGER", "true").lower() == "true"
ENABLE_AUTO_SELL = os.getenv("ENABLE_AUTO_SELL", "true").lower() == "true"

# If you want to throttle buy concurrency from here:
MAX_CONCURRENT_BUYS = int(os.getenv("MAX_CONCURRENT_BUYS", "2"))

# Supervisor backoff
RESTART_BACKOFF_MIN = float(os.getenv("RESTART_BACKOFF_MIN", "2.0"))   # seconds
RESTART_BACKOFF_MAX = float(os.getenv("RESTART_BACKOFF_MAX", "20.0"))  # seconds


class Supervisor:
    """Runs named async tasks with auto-restart + jitter backoff."""

    def __init__(self) -> None:
        self.tasks: Dict[str, asyncio.Task] = {}
        self.stop_event = asyncio.Event()
        self._lock = asyncio.Lock()

    async def _runner(
        self,
        name: str,
        make_coro: Callable[[], Awaitable[None]],
    ) -> None:
        """Run a coroutine factory; restart on unexpected exit until stop_event is set."""
        while not self.stop_event.is_set():
            try:
                logger.info(f"[supervisor] starting task: {name}")
                await make_coro()  # returns only on normal completion or error
                if self.stop_event.is_set():
                    break
                # If it naturally returned, sleep briefly then restart
                delay = random.uniform(RESTART_BACKOFF_MIN, RESTART_BACKOFF_MAX)
                logger.info(f"[supervisor] task '{name}' ended; restarting in {delay:.1f}s")
                await asyncio.wait_for(self.stop_event.wait(), timeout=delay)
            except asyncio.CancelledError:
                logger.info(f"[supervisor] cancelled: {name}")
                break
            except Exception as e:
                delay = random.uniform(RESTART_BACKOFF_MIN, RESTART_BACKOFF_MAX)
                logger.error(
                    f"[supervisor] task '{name}' crashed: {e}; restarting in {delay:.1f}s",
                    exc_info=True,
                )
                try:
                    await asyncio.wait_for(self.stop_event.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass

    async def start_task(self, name: str, make_coro: Callable[[], Awaitable[None]]) -> None:
        async with self._lock:
            if name in self.tasks and not self.tasks[name].done():
                return
            self.tasks[name] = asyncio.create_task(self._runner(name, make_coro), name=name)

    async def stop(self) -> None:
        self.stop_event.set()
        async with self._lock:
            for t in list(self.tasks.values()):
                t.cancel()
        for t in list(self.tasks.values()):
            with suppress(asyncio.CancelledError):
                await t


async def run_sniper() -> None:
    """Main Sniper scanner/buyer loop (never returns unless cancelled)."""
    sniper = Sniper(max_concurrent_buys=MAX_CONCURRENT_BUYS)
    await sniper.main_loop()


async def run_risk_manager() -> None:
    """Trailing stop / stop-loss / reinvestment monitor (never returns unless cancelled)."""
    if not HAS_RISK_MANAGER:
        logger.warning("risk_manager module not found; skipping risk manager loop")
        # idle forever (so supervisor doesn't hot-loop)
        while True:
            await asyncio.sleep(3600)
    await risk_manager_loop()


async def run_auto_sell() -> None:
    """Wallet sweeper for unmanaged positions (never returns unless cancelled)."""
    if not HAS_AUTO_SELL:
        logger.warning("auto_sell module not found; skipping auto-sell loop")
        while True:
            await asyncio.sleep(3600)
    await auto_sell_monitor()


async def main() -> None:
    # 1) Init shared resources once
    await init_resources()

    # 2) Start supervised tasks
    sup = Supervisor()
    await sup.start_task("sniper", run_sniper)

    if ENABLE_RISK_MANAGER:
        await sup.start_task("risk_manager", run_risk_manager)
    else:
        logger.info("Risk manager disabled via env")

    if ENABLE_AUTO_SELL:
        await sup.start_task("auto_sell", run_auto_sell)
    else:
        logger.info("Auto-sell disabled via env")

    # 3) Handle OS signals for graceful shutdown
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()

    def _signal_handler(sig: int):
        logger.info(f"Received signal {sig}; stopping...")
        stop.set()

    # Windows: SIGTERM may not be available, but SIGINT works via Ctrl+C
    for sig in ("SIGINT", "SIGTERM"):
        if hasattr(signal, sig):
            try:
                loop.add_signal_handler(getattr(signal, sig), _signal_handler, getattr(signal, sig))
            except NotImplementedError:
                # On Windows event loop, add_signal_handler may not be implemented
                pass

    try:
        await stop.wait()
    finally:
        # 4) Stop supervised tasks + close resources
        await sup.stop()
        await close_resources()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Final safeguard for Windows Ctrl+C
        try:
            asyncio.run(close_resources())
        except Exception:
            pass
        logger.info("Exited via KeyboardInterrupt")
