# -*- coding: utf-8 -*-
from __future__ import annotations

"""
utils_pkg/auto_exit_worker.py — Production-grade Auto-Exit Worker (P3 LOCK-IN)
=============================================================================

Responsibilities
- Periodically run exit_engine.reclaim(redis) + exit_engine.claim(redis)
- Gate shutdown decisions using utils_pkg.health.health_snapshot()
- Use grace period + consecutive failure threshold
- Trigger shutdown via shared signal/callback (no hard-kill by default)
- Exit cleanly when stop_event is set

Design goals
- Never spam crashes: exceptions are contained and logged
- Deterministic behavior: health checks are bounded by timeouts and thresholds
- Backwards compatible: keeps the original auto_exit_worker(redis, exit_engine, poll_s=1.0) signature
"""

import asyncio
import contextlib
import inspect
import logging
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping

log = logging.getLogger("sniper.auto_exit_worker")

ShutdownCallback = Callable[[dict[str, Any]], Awaitable[None] | None]
HealthFn = Callable[..., Awaitable[dict[str, Any]]]


def _is_null_redis(r: Any) -> bool:
    if r is None:
        return True
    name = getattr(r.__class__, "__name__", "")
    return name in {"_NullRedis", "NullRedis"}


async def _maybe_await(v: Any) -> Any:
    if asyncio.iscoroutine(v) or isinstance(v, asyncio.Future):
        return await v
    return v


def _filter_kwargs(fn: Callable[..., Any], kwargs: dict[str, Any]) -> dict[str, Any]:
    """Only pass kwargs that the callable accepts (tolerates signature drift)."""
    with contextlib.suppress(Exception):
        sig = inspect.signature(fn)
        allowed = set(sig.parameters.keys())
        return {k: v for k, v in kwargs.items() if k in allowed}
    return kwargs


@dataclass(frozen=True)
class AutoExitWorkerConfig:
    # Worker cadence
    poll_s: float = 1.0

    # Health logic
    health_every_s: float = 2.0
    health_timeout_s: float = 3.0
    health_deep: bool = False

    # Grace & threshold
    grace_s: float = 20.0
    max_consecutive_unhealthy: int = 5

    # Redis missing behavior
    noop_sleep_s: float = 30.0  # when redis is null, loop sleeps but still exits on stop_event

    # Safety: never hard-kill by default
    hard_kill: bool = False  # if True, raises SystemExit AFTER firing shutdown (not recommended)


async def auto_exit_worker(
    redis: Any,
    exit_engine: Any,
    poll_s: float = 1.0,
    *,
    stop_event: asyncio.Event | None = None,
    shutdown_event: asyncio.Event | None = None,
    on_shutdown: ShutdownCallback | None = None,
    config: AutoExitWorkerConfig | None = None,
    health_fn: HealthFn | None = None,
) -> None:
    """
    Backwards compatible entrypoint.

    Required:
      - redis: redis client (or NullRedis/None to disable claim/reclaim)
      - exit_engine: object with reclaim(redis) and claim(redis) (sync or async)

    Optional:
      - stop_event: asyncio.Event that signals worker to stop
      - shutdown_event: set when shutdown condition is triggered
      - on_shutdown: async/sync callback invoked once when shutdown triggers
      - config: AutoExitWorkerConfig
      - health_fn: injectable health function for tests; defaults to utils_pkg.health.health_snapshot
    """
    stop_event = stop_event or asyncio.Event()
    cfg = config or AutoExitWorkerConfig(poll_s=float(poll_s))

    if cfg.max_consecutive_unhealthy < 1:
        raise ValueError("max_consecutive_unhealthy must be >= 1")

    # Default health function (must rely on health_snapshot)
    if health_fn is None:
        from utils_pkg.health import health_snapshot as _hs  # type: ignore

        health_fn = _hs

    # Cache filtered kwargs so we don't inspect every call
    base_health_kwargs = {
        "deep": bool(cfg.health_deep),
        "timeout_s": float(cfg.health_timeout_s),
    }
    health_kwargs = _filter_kwargs(health_fn, dict(base_health_kwargs))

    start_t = time.monotonic()
    last_health_t = 0.0
    consecutive_unhealthy = 0
    shutdown_fired = False

    async def _sleep_or_stop(seconds: float) -> bool:
        """Return True if stop_event was set during the wait."""
        if stop_event.is_set():
            return True
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=max(0.01, float(seconds)))
            return True
        except TimeoutError:
            return stop_event.is_set()

    async def _run_health_check() -> tuple[bool, dict[str, Any]]:
        """
        Returns (ok, snapshot). Snapshot is always a dict with at least {"ok": bool}.
        Bounded by cfg.health_timeout_s + a small floor.
        """
        timeout = max(0.2, float(cfg.health_timeout_s))
        try:
            snap = await asyncio.wait_for(health_fn(**health_kwargs), timeout=timeout)  # type: ignore[misc]
        except Exception as e:
            snap = {"ok": False, "err": f"health_snapshot_exception: {type(e).__name__}: {e}"}

        if not isinstance(snap, Mapping):
            snap = {"ok": False, "err": f"health_snapshot_bad_type: {type(snap).__name__}"}

        ok = bool(snap.get("ok") is True)
        return ok, dict(snap)

    async def _check_health_if_due() -> None:
        nonlocal last_health_t, consecutive_unhealthy, shutdown_fired

        now = time.monotonic()
        if (now - last_health_t) < max(0.05, float(cfg.health_every_s)):
            return
        last_health_t = now

        ok, snap = await _run_health_check()

        in_grace = (time.monotonic() - start_t) < max(0.0, float(cfg.grace_s))

        # Consecutive logic:
        # - During grace: never accumulate failures
        # - Healthy: resets counter
        # - Unhealthy and not in grace: increments
        if in_grace or ok:
            consecutive_unhealthy = 0
        else:
            consecutive_unhealthy += 1

        if (
            (not ok)
            and (not in_grace)
            and (consecutive_unhealthy >= cfg.max_consecutive_unhealthy)
            and (not shutdown_fired)
        ):
            shutdown_fired = True

            # DO NOT log the full snapshot (avoid leaking secrets); log safe metadata only.
            log.error(
                "AUTO_EXIT_WORKER_SHUTDOWN_TRIGGERED",
                extra={
                    "consecutive_unhealthy": consecutive_unhealthy,
                    "max_consecutive_unhealthy": cfg.max_consecutive_unhealthy,
                    "grace_s": float(cfg.grace_s),
                    "health_every_s": float(cfg.health_every_s),
                    "health_timeout_s": float(cfg.health_timeout_s),
                    "snap_ok": bool(snap.get("ok") is True),
                    "snap_detail": str(snap.get("detail") or ""),
                    "snap_err": str(snap.get("err") or ""),
                },
            )

            if shutdown_event is not None:
                shutdown_event.set()

            if on_shutdown is not None:
                with contextlib.suppress(Exception):
                    await _maybe_await(on_shutdown(snap))

            if cfg.hard_kill:
                raise SystemExit(1)

    # If redis is null -> worker is a no-op for claim/reclaim but STILL runs health logic.
    if _is_null_redis(redis):
        log.warning(
            "AUTO_EXIT_WORKER_DISABLED: redis_unavailable (claim/reclaim requires real redis)"
        )
        while not stop_event.is_set():
            try:
                await _check_health_if_due()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.info("AUTO_EXIT_WORKER_HEALTH_ERR %r", {"err": f"{type(e).__name__}: {e}"})

            if await _sleep_or_stop(cfg.noop_sleep_s):
                break
        return

    # Main loop
    while not stop_event.is_set():
        try:
            await _check_health_if_due()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.info("AUTO_EXIT_WORKER_HEALTH_ERR %r", {"err": f"{type(e).__name__}: {e}"})

        # reclaim
        try:
            fn = getattr(exit_engine, "reclaim", None)
            if callable(fn):
                await _maybe_await(fn(redis))
            else:
                log.warning("EXIT_ENGINE_MISSING_RECLAIM")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.info("EXIT_RECLAIM_ERR %r", {"err": f"{type(e).__name__}: {e}"})

        # claim
        try:
            fn = getattr(exit_engine, "claim", None)
            if callable(fn):
                await _maybe_await(fn(redis))
            else:
                log.warning("EXIT_ENGINE_MISSING_CLAIM")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.info("EXIT_CLAIM_ERR %r", {"err": f"{type(e).__name__}: {e}"})

        if await _sleep_or_stop(cfg.poll_s):
            break


__all__ = ["AutoExitWorkerConfig", "auto_exit_worker"]
