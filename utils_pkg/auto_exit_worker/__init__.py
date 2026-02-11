# -*- coding: utf-8 -*-
from __future__ import annotations

"""
utils_pkg.auto_exit_worker (package)

God-Tier Auto Exit Worker package — V3+++ LOCK-IN
=================================================

This package provides a stable, import-safe public surface for the auto-exit worker.

IMPORTANT NAME COLLISION NOTE
-----------------------------
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

3) NullRedis-ish detection MUST match core_utils contract:
   - Prefer utils_pkg.core_utils.is_null_redis()
   - Fallback: None OR class-name in {"_NullRedis", "NullRedis"}

NO SIDE EFFECTS
---------------
Importing this module must be safe anywhere:
- no network calls
- no Redis connections
- no async loops created
"""

import asyncio
import contextlib
import inspect
import logging
import time
from dataclasses import dataclass
from importlib import import_module
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Mapping, Protocol, cast

__version__ = "3.1.1"

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

    def __call__(self, *args: Any, **kwargs: Any) -> Any: ...


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
# Canonical NullRedis detection (aligned with core_utils)
# =============================================================================

_IS_NULL_REDIS_IMPL: Callable[[Any | None], bool] | None = None


def _resolve_is_null_redis() -> Callable[[Any | None], bool]:
    """
    Resolve canonical NullRedis detection.

    Prefer:
      - utils_pkg.core_utils.is_null_redis

    Fallback:
      - None => True
      - class-name in {"_NullRedis", "NullRedis"} => True

    Cached after first resolution; import-safe and avoids cycles.
    """
    global _IS_NULL_REDIS_IMPL
    if _IS_NULL_REDIS_IMPL is not None:
        return _IS_NULL_REDIS_IMPL

    with contextlib.suppress(Exception):
        from utils_pkg.core_utils import is_null_redis as impl  # type: ignore

        _IS_NULL_REDIS_IMPL = cast(Callable[[Any | None], bool], impl)
        return _IS_NULL_REDIS_IMPL

    def _fallback(r: Any | None) -> bool:
        if r is None:
            return True
        name = getattr(getattr(r, "__class__", None), "__name__", "")
        return name in {"_NullRedis", "NullRedis"}

    _IS_NULL_REDIS_IMPL = _fallback
    return _IS_NULL_REDIS_IMPL


def _is_null_redis(r: Any | None) -> bool:
    return _resolve_is_null_redis()(r)


# =============================================================================
# Small helpers (pure, deterministic)
# =============================================================================


async def _maybe_await(v: Any) -> Any:
    if inspect.isawaitable(v):
        return await v
    return v


def _filter_kwargs_by_signature(fn: Any, kwargs: dict[str, Any]) -> dict[str, Any]:
    """
    Best-effort signature-tolerant kwarg filtering.

    Why:
      - health_snapshot() signatures can evolve
      - tests may pass stubs with different kwargs
      - we never want "unexpected keyword argument" to crash the worker
    """
    with contextlib.suppress(Exception):
        sig = inspect.signature(fn)
        params = sig.parameters.values()
        if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params):
            return kwargs
        allowed = {p.name for p in params}
        return {k: v for k, v in kwargs.items() if k in allowed}
    return kwargs


def _sanitize_health_snapshot(snap: Mapping[str, Any]) -> dict[str, Any]:
    """
    Keep logs safe: do not dump full snapshot (may include env/config/secrets).
    """

    def _s(x: Any, lim: int = 260) -> str:
        with contextlib.suppress(Exception):
            t = str(x)
            return t if len(t) <= lim else (t[:lim] + "…")
        return "<?>"

    return {
        "ok": bool(snap.get("ok") is True),
        "detail": _s(snap.get("detail") or ""),
        "err": _s(snap.get("err") or ""),
        "step": _s(snap.get("step") or ""),
        "exit_code": snap.get("exit_code"),
        "sec": snap.get("sec"),
    }


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
      2) if TypeError, filter by signature and retry
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

    for kw in ({"deep": bool(deep)}, {"timeout_s": timeout_s}, {"timeout": timeout_s}, {}):
        with contextlib.suppress(TypeError):
            return await _run_with(kw)

    return {"ok": False, "err": "health_snapshot_signature_incompatible"}


async def _sleep_or_stop(stop_event: asyncio.Event, seconds: float) -> bool:
    """
    Sleep bounded by stop_event. Returns True if stop_event was set.
    """
    seconds = max(0.01, float(seconds))
    if stop_event.is_set():
        return True
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
        return True
    except asyncio.TimeoutError:
        return stop_event.is_set()


async def _call_engine(engine: Any, method: str, redis: Any) -> None:
    """
    Call engine.method(redis) safely (sync/async).
    """
    fn = getattr(engine, method, None)
    if not callable(fn):
        log.warning("EXIT_ENGINE_MISSING_%s", method.upper())
        return
    out = fn(redis)
    await _maybe_await(out)


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
    """
    stop_event = stop_event or asyncio.Event()
    cfg = config or AutoExitWorkerConfig()

    # Preserve legacy poll_s positional behavior (always respected)
    try:
        poll_s_f = float(poll_s)
    except Exception:
        poll_s_f = float(cfg.poll_s)
    if poll_s_f <= 0:
        poll_s_f = 0.01

    # Local effective config (no mutation)
    eff = AutoExitWorkerConfig(
        poll_s=poll_s_f,
        health_every_s=float(cfg.health_every_s),
        health_timeout_s=float(cfg.health_timeout_s),
        health_deep=bool(cfg.health_deep),
        grace_s=float(cfg.grace_s),
        max_consecutive_unhealthy=int(cfg.max_consecutive_unhealthy),
        noop_sleep_s=float(cfg.noop_sleep_s),
        log_unhealthy_every_s=float(cfg.log_unhealthy_every_s),
        hard_kill=bool(cfg.hard_kill),
    )

    if eff.max_consecutive_unhealthy < 1:
        raise ValueError("AutoExitWorkerConfig.max_consecutive_unhealthy must be >= 1")

    # Default health function (lazy import to keep import-time side effects at zero)
    if health_fn is None:
        from utils_pkg.health import health_snapshot as _health_snapshot  # type: ignore

        health_fn = cast(_HealthFn, _health_snapshot)

    start_m = time.monotonic()
    last_health_m = 0.0
    last_unhealthy_log_m = 0.0
    consecutive_unhealthy = 0
    shutdown_fired = False

    async def _health_check_if_due() -> None:
        nonlocal last_health_m, last_unhealthy_log_m, consecutive_unhealthy, shutdown_fired

        now_m = time.monotonic()
        if (now_m - last_health_m) < max(0.05, eff.health_every_s):
            return
        last_health_m = now_m

        snap: Any
        try:
            snap = await asyncio.wait_for(
                _call_health_fn(
                    health_fn,  # type: ignore[arg-type]
                    deep=eff.health_deep,
                    timeout_s=eff.health_timeout_s,
                ),
                timeout=max(0.2, float(eff.health_timeout_s)),
            )
        except Exception as e:
            snap = {"ok": False, "err": f"health_snapshot_exception: {type(e).__name__}: {e}"}

        if not isinstance(snap, Mapping):
            snap = {"ok": False, "err": f"health_snapshot_bad_type: {type(snap).__name__}"}

        ok = bool(snap.get("ok") is True)
        in_grace = (time.monotonic() - start_m) < max(0.0, eff.grace_s)

        if ok or in_grace:
            consecutive_unhealthy = 0
            return

        consecutive_unhealthy += 1

        if (now_m - last_unhealthy_log_m) >= max(0.25, eff.log_unhealthy_every_s):
            last_unhealthy_log_m = now_m
            safe = _sanitize_health_snapshot(snap)
            log.warning(
                "AUTO_EXIT_HEALTH_UNHEALTHY",
                extra={
                    "consecutive_unhealthy": consecutive_unhealthy,
                    "max_consecutive_unhealthy": eff.max_consecutive_unhealthy,
                    "grace_s": eff.grace_s,
                    "snap_err": safe.get("err"),
                },
            )

        if (not shutdown_fired) and consecutive_unhealthy >= eff.max_consecutive_unhealthy:
            shutdown_fired = True
            safe = _sanitize_health_snapshot(snap)
            log.error(
                "AUTO_EXIT_WORKER_SHUTDOWN_TRIGGERED",
                extra={
                    "consecutive_unhealthy": consecutive_unhealthy,
                    "max_consecutive_unhealthy": eff.max_consecutive_unhealthy,
                    "grace_s": eff.grace_s,
                    "snap_ok": safe.get("ok"),
                    "snap_detail": safe.get("detail"),
                    "snap_err": safe.get("err"),
                },
            )

            if shutdown_event is not None:
                shutdown_event.set()

            if on_shutdown is not None:
                with contextlib.suppress(Exception):
                    await _maybe_await(on_shutdown(dict(snap)))

            if eff.hard_kill:
                raise SystemExit(1)

    # Null redis => no-op mode (but still honors stop_event + health logic)
    if _is_null_redis(redis):
        log.warning("AUTO_EXIT_WORKER_DISABLED: redis_unavailable (null redis)")
        while not stop_event.is_set():
            with contextlib.suppress(asyncio.CancelledError):
                with contextlib.suppress(Exception):
                    await _health_check_if_due()
            if await _sleep_or_stop(stop_event, eff.noop_sleep_s):
                break
        return

    # Main loop
    while not stop_event.is_set():
        with contextlib.suppress(asyncio.CancelledError):
            with contextlib.suppress(Exception):
                await _health_check_if_due()

        # reclaim
        try:
            await _call_engine(exit_engine, "reclaim", redis)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.info("EXIT_RECLAIM_ERR %r", {"err": f"{type(e).__name__}: {e}"})

        # claim
        try:
            await _call_engine(exit_engine, "claim", redis)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.info("EXIT_CLAIM_ERR %r", {"err": f"{type(e).__name__}: {e}"})

        if await _sleep_or_stop(stop_event, eff.poll_s):
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
