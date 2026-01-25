# sniper/sniper_bot.py
from __future__ import annotations

"""
sniper/sniper_bot.py — V3+++ (God-Tier LOCK-IN)
==============================================

Role
----
This file is the *service wrapper* for the Sniper strategy loop.
It is NOT a second execution engine.

It:
  - sets up canonical logging
  - optionally runs preflight (best-effort)
  - initializes shared resources once (HTTP/Redis/Solana via utils_pkg.core_utils)
  - supervises long-running async tasks with backoff restarts
  - shuts down cleanly on SIGINT/SIGTERM
  - closes resources on exit

LOCK-IN constraints
-------------------
- No network at import time (best-effort; all heavy imports happen inside functions).
- sniper/sniper.py is canonical for scanning/entry/entry logging and starting exit supervisor.
- This file MUST NOT implement alternate buy/sell logic.
- Optional loops (risk manager, sweeper) are disabled by default and must not compete with utils_pkg.exit_engine.

Recommended entrypoint
----------------------
  python -m sniper.sniper_bot

Environment knobs (common)
-------------------------
SNIPER_RUN_PREFLIGHT=0/1          Run scripts.preflight (if present) before starting.
SNIPER_FAST_FAIL=0/1              If 1, crash on missing critical modules.
BOT_ENABLE_RISK_MANAGER=0/1       Start optional risk manager loop (default 0).
BOT_ENABLE_SWEEPER=0/1            Start optional unmanaged-position sweeper (default 0).
BOT_ENABLE_SNIPER=0/1             Start the sniper loop (default 1).

Supervisor
----------
BOT_RESTART_BACKOFF_MIN=2.0       Seconds (min backoff)
BOT_RESTART_BACKOFF_MAX=20.0      Seconds (max backoff)
BOT_RESTART_JITTER=0.35           Jitter fraction applied to backoff
BOT_SHUTDOWN_TIMEOUT_S=20         Graceful shutdown wait timeout

Notes
-----
- Missing optional modules do not crash unless SNIPER_FAST_FAIL=1 (or MAIN_FAST_FAIL=1).
"""

import asyncio
import contextlib
import logging
import os
import random
import signal
import sys
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

# -----------------------------------------------------------------------------
# Ensure repo root importable (works for `python sniper/sniper_bot.py` and module)
# -----------------------------------------------------------------------------
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

# -----------------------------------------------------------------------------
# Small env helpers
# -----------------------------------------------------------------------------
_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _b(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return bool(default)
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return bool(default)


def _f(name: str, default: float) -> float:
    v = os.getenv(name)
    try:
        return float(v) if v not in (None, "") else float(default)
    except Exception:
        return float(default)


def _i(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(float(v)) if v not in (None, "") else int(default)
    except Exception:
        return int(default)


def _fast_fail() -> bool:
    return _b("SNIPER_FAST_FAIL", _b("MAIN_FAST_FAIL", False))


def _short(x: object, n: int = 300) -> str:
    try:
        s = str(x)
        return s if len(s) <= n else (s[:n] + "…")
    except Exception:
        return "<?>"


# -----------------------------------------------------------------------------
# Logging (canonical)
# -----------------------------------------------------------------------------
def _get_logger() -> logging.Logger:
    try:
        from logging_setup import get_logger  # type: ignore

        lg = get_logger("sniper_bot")
        # ensure "event" shim exists
        if not hasattr(lg, "event"):

            def _event(name: str, **kw: object) -> None:
                with contextlib.suppress(Exception):
                    lg.info("%s %s", name, kw)

            lg.event = _event
        return lg
    except Exception:
        level = (os.getenv("LOG_LEVEL") or "INFO").upper()
        logging.basicConfig(
            level=getattr(logging, level, logging.INFO),
            format="%(asctime)s | %(levelname)s | %(name)s: %(message)s",
        )
        lg = logging.getLogger("sniper_bot")
        if not hasattr(lg, "event"):

            def _event(name: str, **kw: object) -> None:
                with contextlib.suppress(Exception):
                    lg.info("%s %s", name, kw)

            lg.event = _event
        return lg


log = _get_logger()


# -----------------------------------------------------------------------------
# Settings
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class Settings:
    enable_sniper: bool
    enable_risk_manager: bool
    enable_sweeper: bool
    run_preflight: bool

    backoff_min: float
    backoff_max: float
    jitter: float
    shutdown_timeout_s: int

    @staticmethod
    def load() -> Settings:
        return Settings(
            enable_sniper=_b("BOT_ENABLE_SNIPER", True),
            # OFF by default — risk_manager can easily compete with exit_engine if miswired.
            enable_risk_manager=_b("BOT_ENABLE_RISK_MANAGER", False),
            # OFF by default — sweeper must NOT sell; only register/observe unmanaged positions.
            enable_sweeper=_b("BOT_ENABLE_SWEEPER", False),
            run_preflight=_b("SNIPER_RUN_PREFLIGHT", False),
            backoff_min=max(0.1, _f("BOT_RESTART_BACKOFF_MIN", 2.0)),
            backoff_max=max(0.2, _f("BOT_RESTART_BACKOFF_MAX", 20.0)),
            jitter=max(0.0, min(0.95, _f("BOT_RESTART_JITTER", 0.35))),
            shutdown_timeout_s=max(5, _i("BOT_SHUTDOWN_TIMEOUT_S", 20)),
        )


# -----------------------------------------------------------------------------
# Best-effort wiring helpers (no hard deps)
# -----------------------------------------------------------------------------
async def _init_resources() -> None:
    try:
        from utils_pkg.core_utils import init_resources  # type: ignore

        out = init_resources()
        if asyncio.iscoroutine(out) or isinstance(out, Awaitable):
            await out  # type: ignore[misc]
        log.event("RESOURCES_INIT_OK")
    except Exception as e:
        log.event("RESOURCES_INIT_FAIL", err=_short(e))
        if _fast_fail():
            raise


async def _close_resources() -> None:
    try:
        from utils_pkg.core_utils import close_resources  # type: ignore

        out = close_resources()
        if asyncio.iscoroutine(out) or isinstance(out, Awaitable):
            await out  # type: ignore[misc]
        log.event("RESOURCES_CLOSE_OK")
    except Exception as e:
        log.event("RESOURCES_CLOSE_FAIL", err=_short(e))


async def _ensure_schema() -> None:
    # Prefer utils_pkg.ai_db.ensure_schema()
    try:
        import importlib

        m = importlib.import_module("utils_pkg.ai_db")
        fn = getattr(m, "ensure_schema", None)
        if callable(fn):
            out = fn()
            if asyncio.iscoroutine(out) or isinstance(out, Awaitable):
                await out  # type: ignore[misc]
            log.event("SCHEMA_OK", source="utils_pkg.ai_db")
            return
    except Exception as e:
        log.event("SCHEMA_FAIL", source="utils_pkg.ai_db", err=_short(e))
        if _fast_fail():
            raise

    # Fallback: db.schema.ensure_schema()
    try:
        import importlib

        m = importlib.import_module("db.schema")
        fn = getattr(m, "ensure_schema", None)
        if callable(fn):
            out = fn()
            if asyncio.iscoroutine(out) or isinstance(out, Awaitable):
                await out  # type: ignore[misc]
            log.event("SCHEMA_OK", source="db.schema")
    except Exception as e:
        log.event("SCHEMA_FAIL", source="db.schema", err=_short(e))
        if _fast_fail():
            raise


async def _maybe_run_preflight() -> None:
    """
    Best-effort: run scripts.preflight if present.
    We treat any exception as failure if fast-fail is enabled.
    """
    try:
        import importlib

        m = importlib.import_module("scripts.preflight")
        fn = getattr(m, "main", None) or getattr(m, "run", None)
        if callable(fn):
            out = fn()
            if asyncio.iscoroutine(out) or isinstance(out, Awaitable):
                await out  # type: ignore[misc]
            log.event("PREFLIGHT_OK")
            return
        # If module exists but no function, still OK.
        log.event("PREFLIGHT_SKIPPED", reason="no_main_or_run")
    except ModuleNotFoundError:
        log.event("PREFLIGHT_SKIPPED", reason="module_missing")
    except Exception as e:
        log.event("PREFLIGHT_FAIL", err=_short(e))
        if _fast_fail():
            raise


# -----------------------------------------------------------------------------
# Supervisor
# -----------------------------------------------------------------------------
class Supervisor:
    """
    Runs named coroutine factories under a restart loop with bounded backoff.
    """

    def __init__(self, *, backoff_min: float, backoff_max: float, jitter: float) -> None:
        self._tasks: dict[str, asyncio.Task] = {}
        self._stop = asyncio.Event()
        self._lock = asyncio.Lock()
        self._backoff_min = float(backoff_min)
        self._backoff_max = float(backoff_max)
        self._jitter = float(jitter)

    def stopping(self) -> bool:
        return self._stop.is_set()

    async def start(self, name: str, make_coro: Callable[[], Awaitable[None]]) -> None:
        async with self._lock:
            t = self._tasks.get(name)
            if t and not t.done():
                return
            self._tasks[name] = asyncio.create_task(self._runner(name, make_coro), name=name)

    async def stop(self) -> None:
        self._stop.set()
        async with self._lock:
            for t in list(self._tasks.values()):
                t.cancel()
        for t in list(self._tasks.values()):
            with contextlib.suppress(asyncio.CancelledError):
                await t

    async def _runner(self, name: str, make_coro: Callable[[], Awaitable[None]]) -> None:
        backoff = self._backoff_min
        while not self._stop.is_set():
            try:
                log.event("TASK_START", task=name)
                await make_coro()
                if self._stop.is_set():
                    break
                # If coroutine returns unexpectedly, treat it as restart-worthy.
                log.event("TASK_RETURNED", task=name)
                await self._sleep_backoff(backoff)
                backoff = min(self._backoff_max, max(self._backoff_min, backoff * 1.5))
            except asyncio.CancelledError:
                log.event("TASK_CANCELLED", task=name)
                break
            except Exception as e:
                log.event("TASK_CRASH", task=name, err=_short(e))
                await self._sleep_backoff(backoff)
                backoff = min(self._backoff_max, max(self._backoff_min, backoff * 1.5))

    async def _sleep_backoff(self, backoff: float) -> None:
        if self._stop.is_set():
            return
        j = self._jitter
        delay = float(backoff)
        if j > 0:
            delay = max(0.0, delay * (1.0 + random.uniform(-j, j)))
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=delay)
        except TimeoutError:
            return


# -----------------------------------------------------------------------------
# Task factories (lazy imports)
# -----------------------------------------------------------------------------
async def _run_sniper_task() -> None:
    """
    Run the canonical sniper loop (sniper/sniper.py).
    This MUST NOT initialize resources; resources are owned by this bot.
    """
    try:
        from sniper.sniper import run_sniper  # type: ignore

        await run_sniper()
    except asyncio.CancelledError:
        raise
    except Exception:
        # Let supervisor restart it
        raise


async def _run_risk_manager_task() -> None:
    """
    Optional. OFF by default.
    This must not compete with utils_pkg.exit_engine (no duplicate selling).
    """
    # Prefer utils_pkg.risk_manager if it exists; otherwise root risk_manager.py
    try:
        import importlib

        for mod_name in ("utils_pkg.risk_manager", "risk_manager"):
            with contextlib.suppress(ModuleNotFoundError):
                m = importlib.import_module(mod_name)
                fn = (
                    getattr(m, "monitor_positions_loop", None)
                    or getattr(m, "main_loop", None)
                    or getattr(m, "run", None)
                )
                if callable(fn):
                    out = fn()
                    if asyncio.iscoroutine(out) or isinstance(out, Awaitable):
                        await out  # type: ignore[misc]
                    else:
                        # If it's sync, idle so supervisor doesn't spin.
                        while True:
                            await asyncio.sleep(3600)
                    return

        log.event("RISK_MANAGER_MISSING")
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        raise


async def _run_sweeper_task() -> None:
    """
    Optional. OFF by default.
    Sweeper is allowed ONLY if it *does not sell* — it may register unmanaged
    positions with the canonical exit engine or provide alerts.
    """
    try:
        import importlib

        # Common naming patterns we’ve seen in repos:
        for mod_name in ("sniper.auto_sell", "sniper.sweeper", "utils_pkg.sweeper"):
            with contextlib.suppress(ModuleNotFoundError):
                m = importlib.import_module(mod_name)
                fn = (
                    getattr(m, "monitor_positions", None)
                    or getattr(m, "sweep_unmanaged_positions", None)
                    or getattr(m, "main_loop", None)
                    or getattr(m, "run", None)
                )
                if callable(fn):
                    out = fn()
                    if asyncio.iscoroutine(out) or isinstance(out, Awaitable):
                        await out  # type: ignore[misc]
                    else:
                        while True:
                            await asyncio.sleep(3600)
                    return

        log.event("SWEEPER_MISSING")
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        raise


async def _health_task(sup: Supervisor) -> None:
    """
    Lightweight health ticker: logs every N seconds so you always know the wrapper is alive.
    """
    every = max(20, _i("BOT_HEALTH_EVERY_SEC", 60))
    started = time.time()
    while not sup.stopping():
        uptime = int(time.time() - started)
        log.event("BOT_HEALTH", uptime_s=uptime)
        try:
            await asyncio.wait_for(asyncio.sleep(float(every)), timeout=float(every) + 1.0)
        except asyncio.CancelledError:
            raise
        except Exception:
            pass


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
async def main() -> None:
    s = Settings.load()
    log.event(
        "BOT_START",
        enable_sniper=int(s.enable_sniper),
        enable_risk_manager=int(s.enable_risk_manager),
        enable_sweeper=int(s.enable_sweeper),
        preflight=int(s.run_preflight),
    )

    # 1) Optional preflight (best-effort)
    if s.run_preflight:
        await _maybe_run_preflight()

    # 2) Init shared resources once
    await _init_resources()

    # 3) Ensure schema (best-effort)
    with contextlib.suppress(Exception):
        await _ensure_schema()

    # 4) Start supervised tasks
    sup = Supervisor(backoff_min=s.backoff_min, backoff_max=s.backoff_max, jitter=s.jitter)
    await sup.start("health", lambda: _health_task(sup))

    if s.enable_sniper:
        await sup.start("sniper", _run_sniper_task)
    else:
        log.event("SNIPER_DISABLED")

    if s.enable_risk_manager:
        await sup.start("risk_manager", _run_risk_manager_task)
    else:
        log.event("RISK_MANAGER_DISABLED")

    if s.enable_sweeper:
        await sup.start("sweeper", _run_sweeper_task)
    else:
        log.event("SWEEPER_DISABLED")

    # 5) Shutdown handling
    stop_evt = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _request_stop(sig: int | None = None) -> None:
        log.event("STOP_REQUESTED", sig=sig)
        stop_evt.set()

    # POSIX
    for sig_name in ("SIGINT", "SIGTERM"):
        if hasattr(signal, sig_name):
            sig = getattr(signal, sig_name)
            try:
                loop.add_signal_handler(sig, _request_stop, int(sig))
            except NotImplementedError:
                # Windows Proactor loop often doesn't support add_signal_handler
                pass

    try:
        await stop_evt.wait()
    finally:
        # stop tasks
        try:
            await asyncio.wait_for(sup.stop(), timeout=float(s.shutdown_timeout_s))
        except Exception as e:
            log.event("SUP_STOP_TIMEOUT", err=_short(e))

        # close resources
        await _close_resources()
        log.event("BOT_EXIT")


if __name__ == "__main__":  # pragma: no cover
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Ctrl+C fallback (especially on Windows)
        with contextlib.suppress(Exception):
            asyncio.run(_close_resources())
        log.event("BOT_EXIT_KEYBOARDINTERRUPT")
