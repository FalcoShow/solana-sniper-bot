# sniper/sniper_bot.py
from __future__ import annotations

"""
sniper/sniper_bot.py — V3+++ (God-Tier LOCK-IN)
==============================================

Role
----
Service wrapper for the *canonical* sniper strategy loop.

This file:
  - sets up canonical logging (with event() shim)
  - optionally runs preflight (best-effort)
  - initializes shared resources once (HTTP/Redis/Solana via utils_pkg.core_utils)
  - supervises long-running async tasks with restart backoff
  - shuts down cleanly on SIGINT/SIGTERM (Windows-safe)
  - closes resources on exit

LOCK-IN constraints
-------------------
- No network at import time (best-effort; heavy imports inside functions).
- sniper/sniper.py is canonical for scanning/entry/exit wiring.
- This file MUST NOT implement alternate buy/sell logic.

Recommended entrypoint
----------------------
  python -m sniper.sniper_bot

Environment knobs
-----------------
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
BOT_HEALTH_EVERY_SEC=60           Health ticker frequency
"""

import asyncio
import contextlib
import importlib
import inspect
import logging
import os
import random
import signal
import sys
import time
from dataclasses import dataclass
from typing import Any, Callable

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
    if v is None or v == "":
        return float(default)
    try:
        return float(v)
    except Exception:
        return float(default)


def _i(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v == "":
        return int(default)
    try:
        return int(float(v))
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


async def _maybe_await(obj: Any) -> Any:
    """Await obj if it is awaitable; otherwise return it."""
    if inspect.isawaitable(obj):
        return await obj
    return obj


# -----------------------------------------------------------------------------
# Logging (canonical) with safe event() support and strong typing
# -----------------------------------------------------------------------------
class EventLogger:
    """
    Wraps a logger-like object and guarantees .event(name, **kw) exists.
    If the underlying logger already has .event, it will be used.
    """

    def __init__(self, base: Any) -> None:
        self._base = base

    def event(self, name: str, **kw: object) -> None:
        fn = getattr(self._base, "event", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                fn(name, **kw)
                return
        with contextlib.suppress(Exception):
            self._base.info("%s %s", name, kw)

    def __getattr__(self, item: str) -> Any:
        return getattr(self._base, item)


def _get_logger() -> EventLogger:
    # Prefer your repo's logging_setup.get_logger if present
    try:
        from logging_setup import get_logger  # type: ignore[import-not-found]

        base = get_logger("sniper_bot")
        return EventLogger(base)
    except Exception:
        level = (os.getenv("LOG_LEVEL") or "INFO").upper()
        logging.basicConfig(
            level=getattr(logging, level, logging.INFO),
            format="%(asctime)s | %(levelname)s | %(name)s: %(message)s",
        )
        return EventLogger(logging.getLogger("sniper_bot"))


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
    def load() -> "Settings":
        return Settings(
            enable_sniper=_b("BOT_ENABLE_SNIPER", True),
            # OFF by default — can compete with exit engine if miswired.
            enable_risk_manager=_b("BOT_ENABLE_RISK_MANAGER", False),
            # OFF by default — must NOT sell, only observe/register/alert.
            enable_sweeper=_b("BOT_ENABLE_SWEEPER", False),
            run_preflight=_b("SNIPER_RUN_PREFLIGHT", False),
            backoff_min=max(0.1, _f("BOT_RESTART_BACKOFF_MIN", 2.0)),
            backoff_max=max(0.2, _f("BOT_RESTART_BACKOFF_MAX", 20.0)),
            jitter=max(0.0, min(0.95, _f("BOT_RESTART_JITTER", 0.35))),
            shutdown_timeout_s=max(5, _i("BOT_SHUTDOWN_TIMEOUT_S", 20)),
        )


# -----------------------------------------------------------------------------
# Best-effort wiring helpers (no hard deps; no import-time networking)
# -----------------------------------------------------------------------------
async def _init_resources() -> None:
    try:
        from utils_pkg.core_utils import init_resources  # type: ignore[import-not-found]

        await _maybe_await(init_resources())
        log.event("RESOURCES_INIT_OK")
    except Exception as e:
        log.event("RESOURCES_INIT_FAIL", err=_short(e))
        if _fast_fail():
            raise


async def _close_resources() -> None:
    try:
        from utils_pkg.core_utils import close_resources  # type: ignore[import-not-found]

        await _maybe_await(close_resources())
        log.event("RESOURCES_CLOSE_OK")
    except Exception as e:
        log.event("RESOURCES_CLOSE_FAIL", err=_short(e))


async def _ensure_schema() -> None:
    # Prefer utils_pkg.ai_db.ensure_schema()
    for mod_name in ("utils_pkg.ai_db", "db.schema"):
        try:
            m = importlib.import_module(mod_name)
            fn = getattr(m, "ensure_schema", None)
            if callable(fn):
                await _maybe_await(fn())
                log.event("SCHEMA_OK", source=mod_name)
                return
        except ModuleNotFoundError:
            continue
        except Exception as e:
            log.event("SCHEMA_FAIL", source=mod_name, err=_short(e))
            if _fast_fail():
                raise


async def _maybe_run_preflight() -> None:
    """
    Best-effort: run scripts.preflight if present.
    Treat exception as fatal only in fast-fail mode.
    """
    try:
        m = importlib.import_module("scripts.preflight")
        fn = getattr(m, "main", None) or getattr(m, "run", None)
        if callable(fn):
            await _maybe_await(fn())
            log.event("PREFLIGHT_OK")
            return
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
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._stop = asyncio.Event()
        self._lock = asyncio.Lock()
        self._backoff_min = float(backoff_min)
        self._backoff_max = float(backoff_max)
        self._jitter = float(jitter)

    def stopping(self) -> bool:
        return self._stop.is_set()

    async def start(
        self, name: str, make_coro: Callable[[], "asyncio.Future[None] | asyncio.Task[None] | Any"]
    ) -> None:
        # make_coro returns an awaitable; we keep Any to avoid import-time typing baggage
        async with self._lock:
            existing = self._tasks.get(name)
            if existing and not existing.done():
                return
            self._tasks[name] = asyncio.create_task(self._runner(name, make_coro), name=name)

    async def stop(self) -> None:
        self._stop.set()
        async with self._lock:
            tasks = list(self._tasks.values())
        for t in tasks:
            t.cancel()
        # Join all, suppress cancellations
        results = await asyncio.gather(*tasks, return_exceptions=True)
        _ = results  # keep for debugging if you ever want to log it

    async def _runner(self, name: str, make_coro: Callable[[], Any]) -> None:
        backoff = self._backoff_min
        while not self._stop.is_set():
            try:
                log.event("TASK_START", task=name)
                await _maybe_await(make_coro())
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
        delay = float(backoff)
        if self._jitter > 0.0:
            delay = max(0.0, delay * (1.0 + random.uniform(-self._jitter, self._jitter)))
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=delay)
        except asyncio.TimeoutError:
            return


# -----------------------------------------------------------------------------
# Task factories (lazy imports)
# -----------------------------------------------------------------------------
async def _run_sniper_task() -> None:
    """
    Run the canonical sniper loop (sniper/sniper.py).
    This MUST NOT initialize resources; resources are owned by this wrapper.
    """
    from sniper.sniper import run_sniper  # type: ignore[import-not-found]

    await run_sniper()


async def _run_risk_manager_task() -> None:
    """
    Optional. OFF by default.
    Must not compete with exit_engine (no duplicate selling).
    """
    for mod_name in ("utils_pkg.risk_manager", "risk_manager"):
        try:
            m = importlib.import_module(mod_name)
        except ModuleNotFoundError:
            continue

        fn = (
            getattr(m, "monitor_positions_loop", None)
            or getattr(m, "main_loop", None)
            or getattr(m, "run", None)
        )
        if callable(fn):
            out = fn()
            if inspect.isawaitable(out):
                await out
            else:
                # If it's sync, idle so supervisor doesn't spin.
                while True:
                    await asyncio.sleep(3600)
            return

    log.event("RISK_MANAGER_MISSING")
    while True:
        await asyncio.sleep(3600)


async def _run_sweeper_task() -> None:
    """
    Optional. OFF by default.
    Sweeper is allowed ONLY if it does NOT sell — it may register/observe unmanaged
    positions or emit alerts.
    """
    for mod_name in ("sniper.auto_sell", "sniper.sweeper", "utils_pkg.sweeper"):
        try:
            m = importlib.import_module(mod_name)
        except ModuleNotFoundError:
            continue

        fn = (
            getattr(m, "monitor_positions", None)
            or getattr(m, "sweep_unmanaged_positions", None)
            or getattr(m, "main_loop", None)
            or getattr(m, "run", None)
        )
        if callable(fn):
            out = fn()
            if inspect.isawaitable(out):
                await out
            else:
                while True:
                    await asyncio.sleep(3600)
            return

    log.event("SWEEPER_MISSING")
    while True:
        await asyncio.sleep(3600)


async def _health_task(sup: Supervisor) -> None:
    """Lightweight health ticker so you always know the wrapper is alive."""
    every = max(20, _i("BOT_HEALTH_EVERY_SEC", 60))
    started = time.time()
    while not sup.stopping():
        uptime = int(time.time() - started)
        log.event("BOT_HEALTH", uptime_s=uptime)
        try:
            await asyncio.wait_for(sup._stop.wait(), timeout=float(every))
        except asyncio.TimeoutError:
            continue


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

    # 1) Optional preflight
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

    # Prefer asyncio signal handlers (POSIX)
    for sig_name in ("SIGINT", "SIGTERM"):
        if hasattr(signal, sig_name):
            sig = getattr(signal, sig_name)
            try:
                loop.add_signal_handler(sig, _request_stop, int(sig))
            except NotImplementedError:
                # Windows Proactor loop often doesn't support add_signal_handler
                pass
            except Exception:
                pass

    # Windows/portable fallback: signal.signal (sync handler)
    def _sync_signal_handler(signum: int, _frame: Any) -> None:
        _request_stop(int(signum))

    with contextlib.suppress(Exception):
        if hasattr(signal, "SIGINT"):
            signal.signal(signal.SIGINT, _sync_signal_handler)
    with contextlib.suppress(Exception):
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, _sync_signal_handler)

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
