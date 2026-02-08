# -*- coding: utf-8 -*-
from __future__ import annotations

"""
utils_pkg.auto_exit_worker (package)

God-Tier Auto Exit Worker package — V3+++ LOCK-IN
=================================================

This package provides a stable, import-safe public surface for the auto-exit worker.

WHY THIS FILE EXISTS
--------------------
This repo contains a directory: utils_pkg/auto_exit_worker/ (a Python package).
Therefore:

    import utils_pkg.auto_exit_worker

imports THIS package (__init__.py), not any utils_pkg/auto_exit_worker.py file.

So the canonical public surface MUST be exported here to keep imports stable for:
- tests
- CLI scripts
- Render worker entrypoints
- internal modules

CONTRACTS / FEATURES PRESERVED
------------------------------
1) Queue/executor surface (preserved via lazy re-exports from .exit_engine):
   - ExitRequest
   - enqueue_exit_request
   - process_one
   - reclaim_stale_inflight
   - prune_done
   - worker_loop

2) Production-grade worker loop (required by tests/P3 and production):
   - AutoExitWorkerConfig
   - auto_exit_worker

NO SIDE EFFECTS
---------------
Importing this module must be safe anywhere:
- no network calls
- no Redis connections
- no async loops created
"""

import asyncio
import contextlib
import logging
import time
from dataclasses import dataclass
from importlib import import_module
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Protocol

__version__ = "3.1.0"


# =============================================================================
# Logging (safe, no hard deps)
# =============================================================================

log = logging.getLogger("auto_exit_worker")


# =============================================================================
# Types
# =============================================================================


class _HealthFn(Protocol):
    """
    Signature-flexible health function.

    Supports:
      - async fn(...) -> dict
      - sync fn(...) -> dict
    """

    def __call__(self, *args: Any, **kwargs: Any) -> Any:  # noqa: D401
        ...


ShutdownCallback = Callable[[dict[str, Any]], Awaitable[None] | None]

if TYPE_CHECKING:  # pragma: no cover
    from .exit_engine import (  # noqa: F401
        ExitRequest,
        enqueue_exit_request,
        process_one,
        prune_done,
        reclaim_stale_inflight,
        worker_loop,
    )


# =============================================================================
# Small helpers (pure, deterministic)
# =============================================================================


def _is_null_redis(r: Any) -> bool:
    """
    Detect NullRedis-ish clients without importing anything heavy.
    """
    return r is None or r.__class__.__name__ in {"_NullRedis", "NullRedis"}


async def _maybe_await(v: Any) -> Any:
    if asyncio.iscoroutine(v) or isinstance(v, Awaitable):
        return await v
    # also handle objects that are awaitable but not coroutine instances
    if hasattr(v, "__await__"):
        return await v  # type: ignore[misc]
    return v


def _filter_kwargs_by_signature(fn: Any, kwargs: dict[str, Any]) -> dict[str, Any]:
    """
    Best-effort signature-tolerant kwarg filtering.

    Why:
      - health_snapshot() signatures can evolve across phases
      - tests may pass a stub with a different signature
      - we never want "unexpected keyword argument" to crash the worker
    """
    try:
        import inspect

        sig = inspect.signature(fn)
        params = sig.parameters.values()
        if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params):
            return kwargs
        allowed = {p.name for p in params}
        return {k: v for k, v in kwargs.items() if k in allowed}
    except Exception:
        return kwargs


async def _call_health_fn(
    health_fn: _HealthFn,
    *,
    deep: bool,
    timeout_s: float,
) -> dict[str, Any]:
    """
    Call health_fn in a maximally compatible way.

    Strategy:
      1) try common kw set
      2) if TypeError (unexpected kw), filter by signature and retry
      3) if still TypeError, try smaller kw sets
    """
    timeout_s = float(timeout_s)
    kw_full: dict[str, Any] = {
        "deep": bool(deep),
        "timeout_s": timeout_s,
        "timeout": timeout_s,
    }

    async def _run_with(kw: dict[str, Any]) -> dict[str, Any]:
        res = await _maybe_await(health_fn(**kw))
        if isinstance(res, dict):
            return res
        return {"ok": False, "err": "health_snapshot_returned_non_dict"}

    try:
        return await _run_with(kw_full)
    except TypeError:
        pass

    try:
        return await _run_with(_filter_kwargs_by_signature(health_fn, kw_full))
    except TypeError:
        pass

    # last-resort fallbacks (for very strict stubs)
    for kw in ({"deep": bool(deep)}, {"timeout_s": timeout_s}, {"timeout": timeout_s}, {}):
        try:
            return await _run_with(kw)
        except TypeError:
            continue

    return {"ok": False, "err": "health_snapshot_signature_incompatible"}


# =============================================================================
# Public config + worker loop (required by tests and production)
# =============================================================================


@dataclass(frozen=True)
class AutoExitWorkerConfig:
    """
    Production-grade control plane for the auto-exit worker.

    Requirements:
    - Periodic health polling (never blocks forever)
    - Grace period prevents early shutdown
    - Consecutive unhealthy threshold triggers shutdown ONCE
    - Null-redis mode is a clean no-op (still honors stop_event)
    """

    # Loop cadence
    poll_s: float = 1.0

    # Health polling
    health_every_s: float = 2.0
    health_timeout_s: float = 3.0
    health_deep: bool = False

    # Failure logic
    grace_s: float = 20.0
    max_consecutive_unhealthy: int = 5

    # Null-redis mode (avoid tight loop)
    noop_sleep_s: float = 30.0

    # Logging rate limit while unhealthy
    log_unhealthy_every_s: float = 5.0

    # Safety: do not kill process by default
    hard_kill: bool = False


async def auto_exit_worker(
    redis: Any,
    exit_engine: Any,
    poll_s: float = 1.0,
    *,
    config: AutoExitWorkerConfig | None = None,
    stop_event: asyncio.Event | None = None,
    shutdown_event: asyncio.Event | None = None,
    on_shutdown: ShutdownCallback | None = None,
    health_fn: _HealthFn | None = None,
) -> None:
    """
    Canonical production-grade auto-exit loop.

    Behavior:
    - Periodically calls `health_snapshot` (or provided `health_fn`)
    - After grace period, if unhealthy `max_consecutive_unhealthy` times -> shutdown ONCE
    - On each tick:
        - exit_engine.reclaim(redis)
        - exit_engine.claim(redis)
      (either may be sync or async; both are optional but logged if missing)
    - Exits cleanly when stop_event is set

    Notes:
    - No external I/O is initiated here beyond what exit_engine does.
    - Import-safe: health_fn default is lazy-imported.
    """
    stop_event = stop_event or asyncio.Event()
    cfg0 = config or AutoExitWorkerConfig()

    cfg = AutoExitWorkerConfig(
        poll_s=float(poll_s),
        health_every_s=float(cfg0.health_every_s),
        health_timeout_s=float(cfg0.health_timeout_s),
        health_deep=bool(cfg0.health_deep),
        grace_s=float(cfg0.grace_s),
        max_consecutive_unhealthy=int(cfg0.max_consecutive_unhealthy),
        noop_sleep_s=float(cfg0.noop_sleep_s),
        log_unhealthy_every_s=float(cfg0.log_unhealthy_every_s),
        hard_kill=bool(cfg0.hard_kill),
    )

    if cfg.poll_s <= 0.0:
        raise ValueError("poll_s must be > 0")
    if cfg.max_consecutive_unhealthy < 1:
        raise ValueError("AutoExitWorkerConfig.max_consecutive_unhealthy must be >= 1")

    # Default health function (lazy import to keep import-time side effects at zero)
    if health_fn is None:
        from utils_pkg.health import health_snapshot as _health_snapshot  # type: ignore

        health_fn = _health_snapshot

    start_m = time.monotonic()
    last_health_m = 0.0
    consecutive_unhealthy = 0
    shutdown_fired = False
    last_unhealthy_log_m = 0.0

    async def _sleep_or_stop(seconds: float) -> bool:
        """
        Sleep bounded by stop_event. Returns True if stop_event was set.
        """
        seconds = max(0.01, float(seconds))
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=seconds)
            return True
        except asyncio.TimeoutError:
            return stop_event.is_set()

    async def _health_check_if_due() -> tuple[bool, dict[str, Any] | None]:
        nonlocal last_health_m, consecutive_unhealthy, shutdown_fired, last_unhealthy_log_m

        now_m = time.monotonic()
        if (now_m - last_health_m) < max(0.05, cfg.health_every_s):
            return True, None

        last_health_m = now_m
        snap: dict[str, Any]
        try:
            snap = await asyncio.wait_for(
                _call_health_fn(
                    health_fn,  # type: ignore[arg-type]
                    deep=cfg.health_deep,
                    timeout_s=cfg.health_timeout_s,
                ),
                timeout=max(0.2, float(cfg.health_timeout_s)),
            )
        except Exception as e:
            snap = {"ok": False, "err": f"health_snapshot_exception: {type(e).__name__}: {e}"}

        ok = bool(snap.get("ok") is True)
        in_grace = (time.monotonic() - start_m) < max(0.0, cfg.grace_s)

        if ok or in_grace:
            consecutive_unhealthy = 0
            return True, snap

        consecutive_unhealthy += 1

        # rate-limited warning spam control
        if (now_m - last_unhealthy_log_m) >= max(0.25, cfg.log_unhealthy_every_s):
            last_unhealthy_log_m = now_m
            log.warning(
                "AUTO_EXIT_HEALTH_UNHEALTHY",
                extra={
                    "consecutive_unhealthy": consecutive_unhealthy,
                    "max_consecutive_unhealthy": cfg.max_consecutive_unhealthy,
                    "grace_s": cfg.grace_s,
                    "err": snap.get("err"),
                },
            )

        if (not shutdown_fired) and consecutive_unhealthy >= cfg.max_consecutive_unhealthy:
            shutdown_fired = True
            log.error(
                "AUTO_EXIT_WORKER_SHUTDOWN_TRIGGERED",
                extra={
                    "consecutive_unhealthy": consecutive_unhealthy,
                    "max_consecutive_unhealthy": cfg.max_consecutive_unhealthy,
                    "grace_s": cfg.grace_s,
                },
            )

            if shutdown_event is not None:
                shutdown_event.set()

            if on_shutdown is not None:
                with contextlib.suppress(Exception):
                    await _maybe_await(on_shutdown(snap))

            if cfg.hard_kill:
                raise SystemExit(1)

        return False, snap

    # Null redis => no-op mode (but still honors stop_event + health logic)
    if _is_null_redis(redis):
        log.warning("AUTO_EXIT_WORKER_DISABLED: redis_unavailable (null redis)")
        while not stop_event.is_set():
            with contextlib.suppress(Exception):
                await _health_check_if_due()
            if await _sleep_or_stop(cfg.noop_sleep_s):
                break
        return

    # Main loop
    while not stop_event.is_set():
        # health check (never blocks beyond health_timeout_s)
        with contextlib.suppress(Exception):
            await _health_check_if_due()

        # reclaim
        try:
            reclaim = getattr(exit_engine, "reclaim", None)
            if callable(reclaim):
                await _maybe_await(reclaim(redis))
            else:
                log.warning("EXIT_ENGINE_MISSING_RECLAIM")
        except Exception as e:
            log.info("EXIT_RECLAIM_ERR %r", {"err": f"{type(e).__name__}: {e}"})

        # claim
        try:
            claim = getattr(exit_engine, "claim", None)
            if callable(claim):
                await _maybe_await(claim(redis))
            else:
                log.warning("EXIT_ENGINE_MISSING_CLAIM")
        except Exception as e:
            log.info("EXIT_CLAIM_ERR %r", {"err": f"{type(e).__name__}: {e}"})

        if await _sleep_or_stop(cfg.poll_s):
            break


# =============================================================================
# Lazy re-exports (preserved surface)
# =============================================================================

__all__ = [
    # required by tests/production
    "AutoExitWorkerConfig",
    "auto_exit_worker",
    # preserved exports from .exit_engine
    "ExitRequest",
    "enqueue_exit_request",
    "process_one",
    "reclaim_stale_inflight",
    "prune_done",
    "worker_loop",
    # meta
    "__version__",
]

_LAZY_EXPORTS: set[str] = {
    "ExitRequest",
    "enqueue_exit_request",
    "process_one",
    "reclaim_stale_inflight",
    "prune_done",
    "worker_loop",
}


def __getattr__(name: str) -> Any:
    """
    Lazily resolve exports from .exit_engine to keep imports light and side-effect free.
    """
    if name in _LAZY_EXPORTS:
        mod = import_module(".exit_engine", __name__)
        return getattr(mod, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals().keys()) | _LAZY_EXPORTS)
