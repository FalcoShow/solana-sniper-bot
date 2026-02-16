# -*- coding: utf-8 -*-
from __future__ import annotations

"""
utils_pkg/task_supervisor.py

God-Tier Runtime Orchestration (P1)
===================================

Supervise asyncio tasks with:
- naming + tags
- structured JSON logs (single-line, stable keys)
- exception capture (incl. "weird" BaseException cases)
- restart policies (none / on_failure / always)
- restart budget window + exponential backoff + jitter
- deterministic shutdown (cancel + bounded waits; drains exceptions)
- essential-task semantics (unexpected exit/failure triggers global stop)
- cleanup hooks that ALWAYS run (startup/cleanup/finally hooks)

Hard requirements:
- NO side effects at import-time (safe for tests/contract import checks)
- Deterministic shutdown (Render-friendly)
- Windows + Unix compatible signal wiring (best-effort)
"""

import asyncio
import contextlib
import inspect
import json
import logging
import random
import signal
import time
from collections import deque
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

JSONDict = dict[str, Any]


# --------------------------------------------------------------------------------------
# JSON logging helpers
# --------------------------------------------------------------------------------------


def _utc_ts() -> str:
    """Stable UTC timestamp string without importing datetime."""
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _json_log(log: logging.Logger, level: int, event: str, **fields: Any) -> None:
    """
    Emit exactly one JSON log line. Never raises outward.

    Payload is stable:
      - ts, event, level always present
      - remaining fields are caller-supplied
    """
    payload: dict[str, Any] = {
        "ts": _utc_ts(),
        "event": event,
        "level": logging.getLevelName(level).lower(),
        **fields,
    }
    try:
        msg = json.dumps(payload, ensure_ascii=True, separators=(",", ":"), default=str)
    except Exception:
        msg = json.dumps(
            {"ts": _utc_ts(), "event": "json_encode_failed", "level": "error"},
            ensure_ascii=True,
            separators=(",", ":"),
        )
    with contextlib.suppress(Exception):
        log.log(level, msg)


def _drain_task_result(t: asyncio.Task[Any]) -> None:
    """Consume completed task exceptions to avoid 'Task exception was never retrieved'."""
    if not t.done():
        return
    with contextlib.suppress(asyncio.CancelledError, Exception):
        _ = t.result()


async def _maybe_await(obj: Any) -> Any:
    """Await if obj is awaitable; otherwise return as-is."""
    if inspect.isawaitable(obj):
        return await obj
    return obj


def _filter_kwargs_for_callable(fn: Callable[..., Any], kwargs: dict[str, Any]) -> dict[str, Any]:
    """
    Filter kwargs to match callable signature (unless it accepts **kwargs).
    Never raises.
    """
    try:
        sig = inspect.signature(fn)
        params = sig.parameters.values()
        if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params):
            return dict(kwargs)
        allowed = {p.name for p in params}
        return {k: v for k, v in kwargs.items() if k in allowed}
    except Exception:
        return dict(kwargs)


async def _run_hook(
    hook: Callable[..., Any] | None,
    *,
    logger: logging.Logger,
    supervisor: str,
    hook_name: str,
    timeout_s: float,
    **kwargs: Any,
) -> None:
    """
    Run a hook safely:
      - accepts sync or async hooks
      - bounded by timeout
      - logs errors but never raises outward (except CancelledError after best-effort run)

    Determinism: this function never leaves a background hook-task running.
    """
    if hook is None:
        return

    timeout = max(0.05, float(timeout_s))
    call_kwargs = _filter_kwargs_for_callable(hook, dict(kwargs))

    async def _call() -> None:
        _ = await _maybe_await(hook(**call_kwargs))

    ht = asyncio.create_task(_call(), name=f"hook:{supervisor}:{hook_name}")

    try:
        # Shield ensures *our* cancellation doesn't automatically cancel the hook.
        await asyncio.wait_for(asyncio.shield(ht), timeout=timeout)
        return

    except asyncio.TimeoutError:
        with contextlib.suppress(Exception):
            ht.cancel()
        with contextlib.suppress(Exception):
            await asyncio.gather(ht, return_exceptions=True)
        _json_log(
            logger,
            logging.WARNING,
            "hook_timeout",
            supervisor=supervisor,
            hook=hook_name,
            timeout_s=float(timeout),
        )
        return

    except asyncio.CancelledError:
        # Best-effort: try to let the hook complete quickly, then re-propagate cancellation.
        cur = asyncio.current_task()
        did_uncancel = False

        if cur is not None and hasattr(cur, "uncancel"):
            with contextlib.suppress(Exception):
                cur.uncancel()  # type: ignore[attr-defined]
                did_uncancel = True

        try:
            try:
                await asyncio.wait_for(asyncio.shield(ht), timeout=timeout)
            except asyncio.TimeoutError:
                with contextlib.suppress(Exception):
                    ht.cancel()
                with contextlib.suppress(Exception):
                    await asyncio.gather(ht, return_exceptions=True)
                _json_log(
                    logger,
                    logging.WARNING,
                    "hook_timeout_on_cancel",
                    supervisor=supervisor,
                    hook=hook_name,
                    timeout_s=float(timeout),
                )
        finally:
            # Ensure no dangling hook task.
            with contextlib.suppress(Exception):
                await asyncio.gather(ht, return_exceptions=True)

            # Restore cancellation only if we previously "uncancelled".
            if did_uncancel and cur is not None:
                with contextlib.suppress(Exception):
                    cur.cancel()
        raise

    except Exception as e:
        # If hook raised, ht is done; ensure drained.
        with contextlib.suppress(Exception):
            await asyncio.gather(ht, return_exceptions=True)
        _json_log(
            logger,
            logging.ERROR,
            "hook_error",
            supervisor=supervisor,
            hook=hook_name,
            exc_type=type(e).__name__,
            err=repr(e),
        )
        return


# --------------------------------------------------------------------------------------
# Restart policy
# --------------------------------------------------------------------------------------


class RestartPolicy:
    NONE = "none"
    ON_FAILURE = "on_failure"
    ALWAYS = "always"


@dataclass(frozen=True)
class RestartConfig:
    """
    Restart budget:
      - max_restarts: max restarts allowed within window_seconds (None = unlimited)
      - window_seconds: rolling window for counting restarts
    Backoff:
      - initial_delay_s: base delay for backoff
      - max_delay_s: cap delay
      - factor: exponential factor
      - jitter: proportion of delay to jitter (0..1)
    """

    max_restarts: int | None = 5
    window_seconds: float = 300.0

    initial_delay_s: float = 0.5
    max_delay_s: float = 30.0
    factor: float = 2.0
    jitter: float = 0.2


@dataclass(frozen=True)
class TaskSpec:
    """
    A supervised task spec.

    Hooks:
      - on_start: called before each run (including restarts)
      - on_stop: called after the run completes (success/failure/cancel)
      - on_exception: called when the task run throws
      - on_finally: always called (after on_stop), last chance for cleanup

    Hooks may be sync or async. All hooks are time-bounded.
    """

    name: str
    coro_factory: Callable[[], Awaitable[None]]

    restart_policy: str = RestartPolicy.NONE
    restart: RestartConfig = field(default_factory=RestartConfig)

    essential: bool = True
    shutdown_timeout_s: float = 10.0

    # Hook timeouts (bounded, deterministic)
    hook_timeout_s: float = 4.0

    # Optional lifecycle hooks (sync or async)
    on_start: Callable[..., Any] | None = None
    on_stop: Callable[..., Any] | None = None
    on_exception: Callable[..., Any] | None = None
    on_finally: Callable[..., Any] | None = None

    tags: dict[str, Any] = field(default_factory=dict)


@dataclass
class TaskRuntime:
    spec: TaskSpec

    # Child task: runs one lifecycle of TaskSpec and returns (failed, exc_type)
    task: asyncio.Task[tuple[bool, str | None]] | None = None
    # Supervisor loop task
    supervisor_task: asyncio.Task[None] | None = None

    started_at_s: float | None = None
    stopped_at_s: float | None = None

    restarts: int = 0
    last_error: str | None = None
    last_exc_type: str | None = None

    # rolling timestamps of restarts for budget enforcement (monotonic seconds)
    restart_times: deque[float] = field(default_factory=deque)


class TaskSupervisor:
    """
    Supervises multiple asyncio tasks with restart policies and deterministic shutdown.

    Typical usage:
        sup = TaskSupervisor(stop_event=shared_event)
        sup.add(TaskSpec(...))
        await sup.run()
    """

    def __init__(
        self,
        *,
        stop_event: asyncio.Event | None = None,
        logger: logging.Logger | None = None,
        name: str = "task_supervisor",
    ) -> None:
        self.name = name
        self.stop_event = stop_event or asyncio.Event()
        self._logger = logger or logging.getLogger(__name__)

        self._runtimes: dict[str, TaskRuntime] = {}
        self._stop_reason: str | None = None
        self._stop_requested_at_s: float | None = None

        self._running: bool = False
        self._shutdown_started: bool = False

        # Global cleanup hooks (run during shutdown, always best-effort)
        self._cleanup_hooks: list[Callable[[], Any]] = []

    @property
    def stop_reason(self) -> str | None:
        return self._stop_reason

    def add(self, spec: TaskSpec) -> None:
        if spec.name in self._runtimes:
            raise ValueError(f"Duplicate task name: {spec.name}")
        self._runtimes[spec.name] = TaskRuntime(spec=spec)

    def add_many(self, specs: Iterable[TaskSpec]) -> None:
        for s in specs:
            self.add(s)

    def add_cleanup_hook(self, hook: Callable[[], Any]) -> None:
        """Register a global cleanup hook (sync or async) that will run during shutdown."""
        self._cleanup_hooks.append(hook)

    def list_tasks(self) -> list[str]:
        return list(self._runtimes.keys())

    def snapshot(self) -> JSONDict:
        """Cheap introspection for health endpoints / debugging."""
        out: JSONDict = {
            "supervisor": self.name,
            "running": self._running,
            "shutdown_started": self._shutdown_started,
            "stop_set": self.stop_event.is_set(),
            "stop_reason": self._stop_reason,
            "tasks": {},
        }
        tasks: dict[str, Any] = {}
        for name, rt in self._runtimes.items():
            t = rt.task
            st = rt.supervisor_task
            tasks[name] = {
                "essential": rt.spec.essential,
                "restart_policy": rt.spec.restart_policy,
                "restarts": rt.restarts,
                "last_exc_type": rt.last_exc_type,
                "last_error": rt.last_error,
                "child": None
                if t is None
                else {
                    "done": t.done(),
                    "cancelled": t.cancelled(),
                    "name": t.get_name(),
                },
                "supervisor_task": None
                if st is None
                else {
                    "done": st.done(),
                    "cancelled": st.cancelled(),
                    "name": st.get_name(),
                },
            }
        out["tasks"] = tasks
        return out

    def request_stop(self, reason: str) -> None:
        """
        Idempotent stop request: first reason wins.
        Safe to call from signal handlers.
        """
        cleaned = (reason or "").strip() or "stop_requested"

        # Preserve the first reason, but if none exists yet, set it even if stop already set.
        if self._stop_reason is None:
            self._stop_reason = cleaned
            self._stop_requested_at_s = time.monotonic()

        if not self.stop_event.is_set():
            _json_log(
                self._logger,
                logging.WARNING,
                "supervisor_stop_requested",
                supervisor=self.name,
                reason=self._stop_reason,
            )
            with contextlib.suppress(Exception):
                self.stop_event.set()

    async def start(self) -> None:
        """Start supervising all tasks, but do not block waiting for stop_event."""
        if self._running:
            raise RuntimeError("TaskSupervisor is already running.")
        self._running = True

        _json_log(
            self._logger,
            logging.INFO,
            "supervisor_start",
            supervisor=self.name,
            tasks=self.list_tasks(),
        )

        for rt in self._runtimes.values():
            rt.supervisor_task = asyncio.create_task(
                self._supervise_one(rt),
                name=f"sup:{rt.spec.name}",
            )
            rt.supervisor_task.add_done_callback(_drain_task_result)

    async def wait(self) -> None:
        """Block until stop_event is set, then perform shutdown."""
        await self.stop_event.wait()
        await self.shutdown()
        _json_log(
            self._logger,
            logging.INFO,
            "supervisor_exit",
            supervisor=self.name,
            reason=self._stop_reason or "stop_event_set",
        )

    async def run(self) -> None:
        """Start supervising all tasks and block until stop_event is set."""
        try:
            await self.start()
            await self.wait()
        finally:
            self._running = False

    async def shutdown(self) -> None:
        """
        Cancel all tasks deterministically, bounded waits, drain exceptions.
        Never raises outward.
        """
        if self._shutdown_started:
            return
        self._shutdown_started = True

        with contextlib.suppress(Exception):
            self.stop_event.set()

        _json_log(self._logger, logging.INFO, "supervisor_shutdown_begin", supervisor=self.name)

        sup_tasks: list[asyncio.Task[Any]] = [
            rt.supervisor_task for rt in self._runtimes.values() if rt.supervisor_task
        ]
        child_tasks: list[asyncio.Task[Any]] = [
            rt.task for rt in self._runtimes.values() if rt.task
        ]

        for t in sup_tasks:
            with contextlib.suppress(Exception):
                t.cancel()
        for t in child_tasks:
            with contextlib.suppress(Exception):
                t.cancel()

        await self._gather_with_timeout(sup_tasks, timeout_s=15.0, label="supervisor_tasks")
        await self._await_children_bounded()
        await self._run_global_cleanup_hooks()

        _json_log(self._logger, logging.INFO, "supervisor_shutdown_done", supervisor=self.name)

    async def _await_children_bounded(self) -> None:
        coros: list[Awaitable[None]] = []
        for rt in self._runtimes.values():
            if rt.task is None:
                continue
            coros.append(
                self._await_task_with_timeout(
                    rt.task,
                    timeout_s=max(0.1, float(rt.spec.shutdown_timeout_s)),
                    name=rt.spec.name,
                )
            )
        if not coros:
            return
        with contextlib.suppress(Exception):
            await asyncio.gather(*coros, return_exceptions=True)

    async def _run_global_cleanup_hooks(self) -> None:
        if not self._cleanup_hooks:
            return

        async def _run_one(i: int, hook: Callable[[], Any]) -> None:
            try:
                res = hook()
                if inspect.isawaitable(res):
                    await asyncio.wait_for(asyncio.shield(res), timeout=5.0)
            except asyncio.TimeoutError:
                _json_log(
                    self._logger,
                    logging.WARNING,
                    "cleanup_hook_timeout",
                    supervisor=self.name,
                    index=i,
                )
            except Exception as e:
                _json_log(
                    self._logger,
                    logging.ERROR,
                    "cleanup_hook_error",
                    supervisor=self.name,
                    index=i,
                    exc_type=type(e).__name__,
                    err=repr(e),
                )

        await asyncio.gather(
            *[_run_one(i, h) for i, h in enumerate(self._cleanup_hooks)],
            return_exceptions=True,
        )

    async def _gather_with_timeout(
        self,
        tasks: list[asyncio.Task[Any]],
        *,
        timeout_s: float,
        label: str,
    ) -> None:
        if not tasks:
            return
        try:
            await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=max(0.1, float(timeout_s)),
            )
        except asyncio.TimeoutError:
            _json_log(
                self._logger,
                logging.WARNING,
                "shutdown_timeout",
                supervisor=self.name,
                label=label,
                timeout_s=float(timeout_s),
            )
        finally:
            for t in tasks:
                _drain_task_result(t)

    async def _await_task_with_timeout(
        self, task: asyncio.Task[Any], timeout_s: float, *, name: str
    ) -> None:
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=max(0.1, float(timeout_s)))
        except asyncio.TimeoutError:
            _json_log(
                self._logger,
                logging.WARNING,
                "task_shutdown_timeout",
                supervisor=self.name,
                task=name,
                timeout_s=float(timeout_s),
            )
        except asyncio.CancelledError:
            return
        except Exception as e:
            _json_log(
                self._logger,
                logging.ERROR,
                "task_shutdown_error",
                supervisor=self.name,
                task=name,
                exc_type=type(e).__name__,
                err=repr(e),
            )
        finally:
            _drain_task_result(task)

    # -----------------------------
    # Restart budget helpers
    # -----------------------------

    def _trim_restart_times(self, rt: TaskRuntime, *, now: float | None = None) -> None:
        cfg = rt.spec.restart
        nowv = time.monotonic() if now is None else float(now)
        window = max(0.0, float(cfg.window_seconds))
        while rt.restart_times and (nowv - rt.restart_times[0]) > window:
            rt.restart_times.popleft()

    def _restart_budget_ok(self, rt: TaskRuntime) -> bool:
        cfg = rt.spec.restart
        if cfg.max_restarts is None:
            return True
        self._trim_restart_times(rt)
        return len(rt.restart_times) < int(cfg.max_restarts)

    def _note_restart(self, rt: TaskRuntime) -> None:
        rt.restart_times.append(time.monotonic())
        self._trim_restart_times(rt)

    def _should_restart(self, spec: TaskSpec, failed: bool) -> bool:
        if spec.restart_policy == RestartPolicy.NONE:
            return False
        if spec.restart_policy == RestartPolicy.ON_FAILURE:
            return bool(failed)
        if spec.restart_policy == RestartPolicy.ALWAYS:
            return True
        return False

    def _compute_backoff_delay(self, cfg: RestartConfig, restart_attempt: int) -> float:
        """
        restart_attempt: 1 for first restart, 2 for second restart, ...
        """
        attempt = max(1, int(restart_attempt))
        n = attempt - 1
        base = float(cfg.initial_delay_s) * (float(cfg.factor) ** n)
        delay = min(float(cfg.max_delay_s), max(float(cfg.initial_delay_s), base))
        jitter = float(cfg.jitter)
        if jitter > 0.0:
            j = delay * jitter
            delay = max(0.0, delay + random.uniform(-j, j))
        return float(delay)

    async def _sleep_or_stop(self, seconds: float) -> None:
        if seconds <= 0:
            return
        try:
            await asyncio.wait_for(self.stop_event.wait(), timeout=float(seconds))
        except asyncio.TimeoutError:
            return

    # -----------------------------
    # Core supervision loop
    # -----------------------------

    async def _run_task_once(self, rt: TaskRuntime) -> tuple[bool, str | None]:
        """
        Run the spec coroutine once with lifecycle hooks.
        Returns (failed, exc_type).
        """
        spec = rt.spec
        rt.last_error = None
        rt.last_exc_type = None

        failed = False
        exc: BaseException | None = None
        cancelled_exc: asyncio.CancelledError | None = None

        try:
            await _run_hook(
                spec.on_start,
                logger=self._logger,
                supervisor=self.name,
                hook_name="on_start",
                timeout_s=spec.hook_timeout_s,
                task=spec.name,
                tags=spec.tags or None,
                runtime=rt,
                spec=spec,
                stop_event=self.stop_event,
            )

            await spec.coro_factory()
            return (False, None)

        except asyncio.CancelledError as e:
            cancelled_exc = e
            exc = e
            raise

        except BaseException as e:
            # Catch "weird" failures too (e.g., SystemExit inside a task) so we can shutdown deterministically.
            failed = True
            exc = e
            rt.last_exc_type = type(e).__name__
            rt.last_error = repr(e)

            _json_log(
                self._logger,
                logging.ERROR,
                "task_exception",
                supervisor=self.name,
                task=spec.name,
                exc_type=rt.last_exc_type,
                err=rt.last_error,
                tags=spec.tags or None,
            )

            await _run_hook(
                spec.on_exception,
                logger=self._logger,
                supervisor=self.name,
                hook_name="on_exception",
                timeout_s=spec.hook_timeout_s,
                task=spec.name,
                tags=spec.tags or None,
                runtime=rt,
                spec=spec,
                stop_event=self.stop_event,
                exc=e,
                exc_type=type(e).__name__,
                err=repr(e),
            )

            # If a task throws SystemExit / KeyboardInterrupt inside, treat it as a global stop request.
            if isinstance(e, (SystemExit, KeyboardInterrupt)):
                self.request_stop(f"task_baseexception:{spec.name}:{type(e).__name__}")

            return (True, rt.last_exc_type)

        finally:
            # Run stop/finally hooks best-effort even during cancellation.
            for hook, hook_name in ((spec.on_stop, "on_stop"), (spec.on_finally, "on_finally")):
                try:
                    await _run_hook(
                        hook,
                        logger=self._logger,
                        supervisor=self.name,
                        hook_name=hook_name,
                        timeout_s=spec.hook_timeout_s,
                        task=spec.name,
                        tags=spec.tags or None,
                        runtime=rt,
                        spec=spec,
                        stop_event=self.stop_event,
                        failed=failed,
                        exc=exc,
                    )
                except asyncio.CancelledError as e:
                    cancelled_exc = cancelled_exc or e
                    continue

            if cancelled_exc is not None:
                raise cancelled_exc

    async def _supervise_one(self, rt: TaskRuntime) -> None:
        """Supervisor loop for a single TaskSpec."""
        spec = rt.spec

        while not self.stop_event.is_set():
            if not self._restart_budget_ok(rt):
                msg = f"restart_budget_exhausted:{spec.name}"
                rt.last_error = msg
                _json_log(
                    self._logger,
                    logging.ERROR,
                    "task_restart_budget_exhausted",
                    supervisor=self.name,
                    task=spec.name,
                    restarts=rt.restarts,
                    max_restarts=spec.restart.max_restarts,
                    window_seconds=spec.restart.window_seconds,
                    essential=spec.essential,
                )
                if spec.essential:
                    self.request_stop(msg)
                return

            rt.started_at_s = time.monotonic()
            rt.stopped_at_s = None

            rt.task = asyncio.create_task(self._run_task_once(rt), name=spec.name)
            rt.task.add_done_callback(_drain_task_result)

            _json_log(
                self._logger,
                logging.INFO,
                "task_start",
                supervisor=self.name,
                task=spec.name,
                essential=spec.essential,
                restart_policy=spec.restart_policy,
                restarts=rt.restarts,
                tags=spec.tags or None,
            )

            try:
                failed, _exc_type = await rt.task
            except asyncio.CancelledError:
                return
            finally:
                rt.stopped_at_s = time.monotonic()

            if self.stop_event.is_set():
                return

            should_restart = self._should_restart(spec, failed)

            _json_log(
                self._logger,
                logging.WARNING if (failed or should_restart) else logging.INFO,
                "task_exit",
                supervisor=self.name,
                task=spec.name,
                failed=bool(failed),
                restart=bool(should_restart),
                restarts=rt.restarts,
                last_exc_type=rt.last_exc_type,
                last_error=rt.last_error,
            )

            if not should_restart:
                if spec.essential:
                    self.request_stop(f"essential_task_exited:{spec.name}")
                return

            self._note_restart(rt)
            rt.restarts += 1

            delay = self._compute_backoff_delay(spec.restart, restart_attempt=rt.restarts)
            _json_log(
                self._logger,
                logging.INFO,
                "task_restart_scheduled",
                supervisor=self.name,
                task=spec.name,
                in_s=float(delay),
                restarts=rt.restarts,
            )
            await self._sleep_or_stop(delay)

        return


# --------------------------------------------------------------------------------------
# Signal handling (optional)
# --------------------------------------------------------------------------------------


def install_signal_handlers(loop: asyncio.AbstractEventLoop | None, sup: TaskSupervisor) -> None:
    """
    Best-effort signal handlers for stopping the supervisor.
    Render typically sends SIGTERM; local Ctrl+C is SIGINT.

    - If loop supports add_signal_handler, use it (Unix).
    - Otherwise, fall back to signal.signal where possible (Windows).

    B023-safe: binds loop variables at definition-time.
    """
    if loop is None:
        with contextlib.suppress(Exception):
            loop = asyncio.get_running_loop()

    def _request(sig_name: str) -> None:
        sup.request_stop(f"signal:{sig_name}")

    sigs: list[signal.Signals] = []
    with contextlib.suppress(Exception):
        sigs.append(signal.SIGINT)
    with contextlib.suppress(Exception):
        sigs.append(signal.SIGTERM)

    for sig in sigs:
        sig_num = int(sig)
        sig_name = getattr(sig, "name", str(sig_num))

        if loop is not None:
            try:
                loop.add_signal_handler(sig_num, _request, sig_name)
                continue
            except (NotImplementedError, RuntimeError):
                pass
            except Exception:
                pass

        try:

            def _h(_signum: int, _frame: Any, *, _name: str = sig_name) -> None:
                _request(_name)

            signal.signal(sig_num, _h)
        except Exception:
            continue


__all__ = [
    "RestartPolicy",
    "RestartConfig",
    "TaskSpec",
    "TaskRuntime",
    "TaskSupervisor",
    "install_signal_handlers",
]
