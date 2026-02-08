# -*- coding: utf-8 -*-
from __future__ import annotations

"""
utils_pkg/shutdown.py

God-Tier Shutdown Orchestrator (Render + local parity)
=====================================================

One canonical place to shut the system down cleanly and deterministically.

Guarantees
----------
- Crash-proof shutdown: public API never raises outward.
- Idempotent + concurrent-safe: multiple calls await the same in-flight shutdown.
- Cooperative first: sets a shared stop_event and waits a short grace period.
- Deterministic cancellation: cancels remaining tasks (except the caller) and drains results
  to avoid "Task exception was never retrieved".
- Resource closure: duck-typed close/aclose, plus redis pool disconnect, asyncpg pools,
  telemetry flush/shutdown, and an optional close_resources hook.
- Signal wiring: best-effort handlers for SIGINT/SIGTERM; compatible with Windows limitations.

Design constraints
------------------
- Safe to import (no heavy imports, no IO/network at import time).
- Logging is structured JSON (best-effort), with no intentional secret emission.

NOTE
----
Callers should avoid passing secrets into log fields. This module won't intentionally
log secret values, but it cannot magically redact user-provided fields.
"""

import asyncio
import contextlib
import inspect
import json
import logging
import signal
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

# =============================================================================
# Logger (canonical contract)
# =============================================================================

if TYPE_CHECKING:
    # mypy always sees the canonical signature/types from log_compat
    from utils_pkg.log_compat import EventLogger, get_logger
else:  # pragma: no cover
    try:
        from utils_pkg.log_compat import EventLogger, get_logger  # type: ignore
    except Exception:  # pragma: no cover
        EventLogger = Any  # type: ignore[misc,assignment]

        def get_logger(  # type: ignore[no-redef]
            name: str | None = None,
            *,
            service: str | None = None,
            version: str | None = None,
            env: str | None = None,
            level: str | int | None = None,
            json_logs: bool | None = None,
            use_utc: bool | None = None,
            include_src: bool | None = None,
            stream: str | None = None,
            force: bool = False,
        ) -> Any:
            lg = logging.getLogger(name or "shutdown")
            if force or not lg.handlers:
                logging.basicConfig(level=logging.INFO)
            if level is not None:
                with contextlib.suppress(Exception):
                    lg.setLevel(level)  # type: ignore[arg-type]
            return lg


log: "EventLogger" = get_logger(__name__)
_stdlog = logging.getLogger(__name__)  # always defined fallback


# =============================================================================
# small utils
# =============================================================================
def _now_s() -> float:
    return time.monotonic()


def _is_secret_key(k: str) -> bool:
    kk = (k or "").lower()
    return any(
        s in kk
        for s in (
            "secret",
            "token",
            "password",
            "passwd",
            "api_key",
            "apikey",
            "private",
            "seed",
            "mnemonic",
            "dsn",
            "uri",
            "key",
        )
    )


def _redact_fields(fields: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in fields.items():
        try:
            out[k] = "***" if _is_secret_key(k) else v
        except Exception:
            out[k] = "***"
    return out


def _emit(level: int, event: str, **fields: Any) -> None:
    """
    Emit one structured JSON-ish log line. Never raises.
    Prefers log.event(event, **fields) if available.
    """
    safe_fields = _redact_fields(fields)

    # Prefer structured logger if it supports `.event`
    try:
        ev = getattr(log, "event", None)
        if callable(ev):
            ev(event, **safe_fields)  # type: ignore[misc]
            return
    except Exception:
        pass

    payload = {"event": event, **safe_fields}
    try:
        line = json.dumps(payload, ensure_ascii=True, default=str)
    except Exception:
        line = f"{event} {safe_fields}"

    try:
        ll = getattr(log, "log", None)
        if callable(ll):
            ll(level, line)  # type: ignore[misc]
            return

        if level >= logging.ERROR:
            getattr(log, "error", _stdlog.error)(line)
        elif level >= logging.WARNING:
            getattr(log, "warning", _stdlog.warning)(line)
        else:
            getattr(log, "info", _stdlog.info)(line)
    except Exception:
        with contextlib.suppress(Exception):
            _stdlog.log(level, line)


async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


async def _call(obj: Any, name: str, *args: Any, **kwargs: Any) -> Any:
    """
    Duck-typed async/sync method caller. Never raises outward.
    """
    fn = getattr(obj, name, None)
    if not callable(fn):
        return None
    try:
        out = fn(*args, **kwargs)
    except Exception:
        return None
    with contextlib.suppress(Exception):
        return await _maybe_await(out)
    return None


async def _close_duck(obj: Any) -> None:
    """
    Best-effort close for common async resources. Never raises outward.
    - prefers aclose()
    - then close()
    - then (redis asyncio) connection_pool.disconnect()
    """
    if obj is None:
        return

    if hasattr(obj, "aclose"):
        await _call(obj, "aclose")
        return

    if hasattr(obj, "close"):
        await _call(obj, "close")

    pool = getattr(obj, "connection_pool", None)
    if pool is not None and hasattr(pool, "disconnect"):
        await _call(pool, "disconnect")


def _task_name(t: asyncio.Task[Any]) -> str:
    with contextlib.suppress(Exception):
        return t.get_name()
    return "task"


def _drain_task_result(t: asyncio.Task[Any]) -> None:
    if not t.done():
        return
    with contextlib.suppress(asyncio.CancelledError, Exception):
        _ = t.result()


# =============================================================================
# report models
# =============================================================================
@dataclass
class ShutdownStep:
    name: str
    ok: bool
    detail: str = ""
    took_ms: int = 0


@dataclass
class ShutdownReport:
    ok: bool
    reason: str
    took_ms: int
    steps: list[ShutdownStep] = field(default_factory=list)


# =============================================================================
# controller
# =============================================================================
class ShutdownController:
    """
    Shared shutdown signal for the whole app.

    - stop_event: set when shutdown is requested
    - reason: first reason wins
    - shutdown(): executes once (idempotent) and concurrent callers await same result
    """

    def __init__(self) -> None:
        self.stop_event: asyncio.Event = asyncio.Event()
        self._reason: str = ""
        self._lock: asyncio.Lock = asyncio.Lock()
        self._shutdown_future: asyncio.Future[ShutdownReport] | None = None

    @property
    def reason(self) -> str:
        return self._reason or "unspecified"

    def request(self, reason: str) -> None:
        """
        Sync-safe request (works in signal handlers). Never raises.
        """
        r = (reason or "").strip() or "unspecified"
        if not self._reason:
            self._reason = r
        with contextlib.suppress(Exception):
            self.stop_event.set()

    async def shutdown(
        self,
        *,
        reason: str | None = None,
        grace_s: float = 3.0,
        cancel_timeout_s: float = 10.0,
        resource_timeout_s: float = 10.0,
        total_timeout_s: float = 25.0,
        tasks: Iterable[asyncio.Task[Any]] | None = None,
        redis: Any = None,
        pg_pool: Any = None,
        telemetry: Any = None,
        close_resources: Callable[[], Any] | None = None,
    ) -> ShutdownReport:
        """
        Execute shutdown exactly once. Returns a report. Never raises outward.
        Concurrent callers await the same in-flight shutdown.
        """
        if reason:
            self.request(reason)

        run_shutdown = False
        fut: asyncio.Future[ShutdownReport] | None

        async with self._lock:
            fut = self._shutdown_future
            if fut is None:
                fut = asyncio.get_running_loop().create_future()
                self._shutdown_future = fut
                run_shutdown = True

        if not run_shutdown and fut is not None:
            with contextlib.suppress(Exception):
                return await asyncio.shield(fut)
            return ShutdownReport(ok=True, reason=self.reason, took_ms=0, steps=[])

        start = _now_s()
        steps: list[ShutdownStep] = []

        async def _step(
            name: str,
            fn: Callable[[], Any],
            timeout_s: float,
            *,
            warn_on_fail: bool = False,
        ) -> None:
            t0 = _now_s()
            try:
                out = fn()
                if inspect.isawaitable(out):
                    await asyncio.wait_for(out, timeout=max(0.1, float(timeout_s)))
                steps.append(ShutdownStep(name=name, ok=True, took_ms=int((_now_s() - t0) * 1000)))
            except asyncio.TimeoutError:
                steps.append(
                    ShutdownStep(
                        name=name,
                        ok=False,
                        detail=f"timeout_after_{float(timeout_s):.1f}s",
                        took_ms=int((_now_s() - t0) * 1000),
                    )
                )
                if warn_on_fail:
                    _emit(logging.WARNING, "shutdown_step_timeout", step=name, timeout_s=timeout_s)
            except Exception as e:
                steps.append(
                    ShutdownStep(
                        name=name,
                        ok=False,
                        detail=f"{type(e).__name__}: {e}",
                        took_ms=int((_now_s() - t0) * 1000),
                    )
                )
                if warn_on_fail:
                    _emit(logging.WARNING, "shutdown_step_error", step=name, err=type(e).__name__)

        async def _do_cancel_tasks() -> None:
            current = asyncio.current_task()
            all_tasks = list(tasks) if tasks is not None else list(asyncio.all_tasks())

            target: list[asyncio.Task[Any]] = []
            for t in all_tasks:
                if t is current or t.done():
                    continue
                target.append(t)

            for t in target:
                with contextlib.suppress(Exception):
                    t.cancel()

            if not target:
                return

            done, pending = await asyncio.wait(target, timeout=max(0.1, float(cancel_timeout_s)))

            for t in done:
                _drain_task_result(t)

            if pending:
                _emit(
                    logging.WARNING,
                    "shutdown_pending_tasks",
                    pending_count=len(pending),
                    pending_names=[_task_name(t) for t in list(pending)[:25]],
                )
                for t in pending:
                    with contextlib.suppress(Exception):
                        t.cancel()
                done2, _pending2 = await asyncio.wait(list(pending), timeout=0.5)
                for t in done2:
                    _drain_task_result(t)

        async def _do_telemetry() -> None:
            """
            Telemetry flush/shutdown. If telemetry not provided, best-effort import known modules.

            IMPORTANT: calls go through _call() to avoid mypy 'func-returns-value' and
            unknown return typing on module-level functions.
            """
            if telemetry is None:
                with contextlib.suppress(Exception):
                    import utils_pkg.telemetry as tmod  # type: ignore

                    await _call(tmod, "flush")
                    await _call(tmod, "shutdown")

                with contextlib.suppress(Exception):
                    import route_telemetry as rt  # type: ignore

                    await _call(rt, "flush")
                    await _call(rt, "shutdown")
                return

            await _call(telemetry, "flush")
            await _call(telemetry, "shutdown")
            await _call(telemetry, "close")

        async def _do_close_hook() -> None:
            """
            Preferred: provided close_resources(). Otherwise tries utils_pkg.core_utils.close_resources().
            """
            if close_resources is not None:
                await _maybe_await(close_resources())
                return

            with contextlib.suppress(Exception):
                import utils_pkg.core_utils as cu  # type: ignore

                await _call(cu, "close_resources")

        async def _amain_shutdown() -> None:
            _emit(logging.INFO, "shutdown_begin", reason=self.reason)

            await _step("set_stop_event", lambda: self.stop_event.set(), timeout_s=1.0)

            async def _grace_wait() -> None:
                await asyncio.sleep(max(0.0, float(grace_s)))

            await _step("grace_wait", _grace_wait, timeout_s=max(0.1, float(grace_s) + 0.5))

            await _step(
                "cancel_tasks",
                _do_cancel_tasks,
                timeout_s=max(0.1, float(cancel_timeout_s) + 1.0),
                warn_on_fail=True,
            )

            await _step("close_redis", lambda: _close_duck(redis), timeout_s=resource_timeout_s)
            await _step("close_pg_pool", lambda: _close_duck(pg_pool), timeout_s=resource_timeout_s)

            await _step("telemetry_flush", _do_telemetry, timeout_s=resource_timeout_s)
            await _step("close_resources", _do_close_hook, timeout_s=resource_timeout_s)

            _emit(logging.INFO, "shutdown_end", reason=self.reason)

        ok = True
        try:
            await asyncio.wait_for(_amain_shutdown(), timeout=max(1.0, float(total_timeout_s)))
        except asyncio.TimeoutError:
            ok = False
            steps.append(
                ShutdownStep(
                    name="shutdown_total_timeout",
                    ok=False,
                    detail=f"timeout_after_{float(total_timeout_s):.1f}s",
                )
            )
        except Exception as e:
            ok = False
            steps.append(
                ShutdownStep(name="shutdown_exception", ok=False, detail=f"{type(e).__name__}: {e}")
            )

        took_ms = int((_now_s() - start) * 1000)
        if any(not s.ok for s in steps):
            ok = False

        report = ShutdownReport(ok=ok, reason=self.reason, took_ms=took_ms, steps=steps)

        if fut is not None and not fut.done():
            with contextlib.suppress(Exception):
                fut.set_result(report)

        return report


# =============================================================================
# signal wiring
# =============================================================================
def install_signal_handlers(ctrl: ShutdownController) -> None:
    """
    Best-effort SIGINT/SIGTERM wiring.
    - Unix: prefer loop.add_signal_handler when available
    - Windows: falls back to signal.signal
    Never raises.

    B023-safe: binds loop vars at definition-time.
    """
    try:
        loop = asyncio.get_running_loop()
    except Exception:
        loop = None

    def _make_loop_handler(sig_name: str) -> Callable[[], None]:
        def _h() -> None:
            ctrl.request(f"signal:{sig_name}")

        return _h

    def _make_signal_handler(sig_name: str) -> Callable[[int, Any], None]:
        def _h(_signum: int, _frame: Any, *, _name: str = sig_name) -> None:
            ctrl.request(f"signal:{_name}")

        return _h

    for sig in (getattr(signal, "SIGINT", None), getattr(signal, "SIGTERM", None)):
        if sig is None:
            continue

        sig_name = getattr(sig, "name", str(int(sig)))

        if loop is not None:
            try:
                loop.add_signal_handler(sig, _make_loop_handler(sig_name))  # type: ignore[arg-type]
                continue
            except (NotImplementedError, RuntimeError):
                pass
            except Exception:
                pass

        with contextlib.suppress(Exception):
            signal.signal(sig, _make_signal_handler(sig_name))  # type: ignore[arg-type]


__all__ = [
    "ShutdownController",
    "ShutdownReport",
    "ShutdownStep",
    "install_signal_handlers",
]
