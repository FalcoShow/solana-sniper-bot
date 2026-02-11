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

Hard contracts
- Backwards compatible: auto_exit_worker(redis, exit_engine, poll_s=1.0) remains valid
- Accepts redis client OR None / NullRedis-ish (disables claim/reclaim but still runs health)
- Deterministic: health checks are bounded by timeout; loop never hangs on stop_event
- Canonical NullRedis detection: defer to utils_pkg.core_utils.is_null_redis() if available
"""

import asyncio
import contextlib
import inspect
import logging
import time
from dataclasses import dataclass, replace
from typing import Any, Awaitable, Callable, Mapping, Protocol, TypeVar, cast

log = logging.getLogger("sniper.auto_exit_worker")

T = TypeVar("T")

ShutdownCallback = Callable[[dict[str, Any]], Awaitable[None] | None]
HealthFn = Callable[..., Awaitable[dict[str, Any]]]


class _ExitEngineLike(Protocol):
    def reclaim(self, redis: Any) -> Any: ...
    def claim(self, redis: Any) -> Any: ...


# ──────────────────────────────────────────────────────────────────────────────
# Canonical NullRedis detection (aligned with core_utils)
# ──────────────────────────────────────────────────────────────────────────────

_IS_NULL_REDIS_IMPL: Callable[[Any | None], bool] | None = None


def _resolve_is_null_redis() -> Callable[[Any | None], bool]:
    """
    Resolve canonical NullRedis detection.

    Prefer:
      - utils_pkg.core_utils.is_null_redis

    Fallback:
      - class-name detection ("_NullRedis", "NullRedis") + None => True

    Cached after first resolution.
    """
    global _IS_NULL_REDIS_IMPL
    if _IS_NULL_REDIS_IMPL is not None:
        return _IS_NULL_REDIS_IMPL

    # Lazy import to avoid import cycles / eager load
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


# ──────────────────────────────────────────────────────────────────────────────
# Small helpers
# ──────────────────────────────────────────────────────────────────────────────


async def _maybe_await(v: Any) -> Any:
    if inspect.isawaitable(v):
        return await v
    return v


def _filter_kwargs(fn: Callable[..., Any], kwargs: dict[str, Any]) -> dict[str, Any]:
    """Only pass kwargs that the callable accepts (tolerates signature drift)."""
    with contextlib.suppress(Exception):
        sig = inspect.signature(fn)
        allowed = set(sig.parameters.keys())
        return {k: v for k, v in kwargs.items() if k in allowed}
    return kwargs


def _sanitize_health_snapshot(snap: Mapping[str, Any]) -> dict[str, Any]:
    """
    Keep logs safe: do not dump full snapshot (may include env/config/secrets).
    """

    def _s(x: Any, lim: int = 260) -> str:
        try:
            t = str(x)
            return t if len(t) <= lim else (t[:lim] + "…")
        except Exception:
            return "<?>"

    return {
        "ok": bool(snap.get("ok") is True),
        "detail": _s(snap.get("detail") or ""),
        "err": _s(snap.get("err") or ""),
        "step": _s(snap.get("step") or ""),
        "exit_code": snap.get("exit_code"),
        "sec": snap.get("sec"),
    }


async def _sleep_or_stop(stop_event: asyncio.Event, seconds: float) -> bool:
    """Return True if stop_event was set during the wait."""
    if stop_event.is_set():
        return True
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=max(0.01, float(seconds)))
        return True
    except TimeoutError:
        return stop_event.is_set()


async def _call_engine_method(engine: Any, method_name: str, redis: Any) -> None:
    """
    Call engine.method(redis) safely.

    Supports:
      - sync or async methods
      - signature drift: tries positional then keyword
    """
    fn = getattr(engine, method_name, None)
    if not callable(fn):
        log.warning("EXIT_ENGINE_MISSING_%s", method_name.upper())
        return

    # First try: positional call
    try:
        out = fn(redis)
    except TypeError:
        # Second try: keyword call
        out = fn(redis=redis)

    await _maybe_await(out)


# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────


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


# ──────────────────────────────────────────────────────────────────────────────
# Worker
# ──────────────────────────────────────────────────────────────────────────────


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

    cfg = config or AutoExitWorkerConfig()
    # Preserve legacy poll_s positional behavior even if config provided
    try:
        poll_f = float(poll_s)
    except Exception:
        poll_f = float(cfg.poll_s)
    if poll_f <= 0:
        poll_f = 0.01
    if poll_f != float(cfg.poll_s):
        cfg = replace(cfg, poll_s=poll_f)

    if int(cfg.max_consecutive_unhealthy) < 1:
        raise ValueError("max_consecutive_unhealthy must be >= 1")

    # Default health function (must rely on health_snapshot)
    if health_fn is None:
        from utils_pkg.health import health_snapshot as _hs  # type: ignore

        health_fn = cast(HealthFn, _hs)

    # Cache filtered kwargs so we don't inspect every call
    base_health_kwargs: dict[str, Any] = {
        "deep": bool(cfg.health_deep),
        "timeout_s": float(cfg.health_timeout_s),
    }
    health_kwargs = _filter_kwargs(health_fn, dict(base_health_kwargs))

    start_t = time.monotonic()
    last_health_t = 0.0
    consecutive_unhealthy = 0
    shutdown_fired = False

    async def _run_health_check() -> tuple[bool, dict[str, Any]]:
        """
        Returns (ok, snapshot). Snapshot is always a dict with at least {"ok": bool}.
        Bounded by cfg.health_timeout_s (with small floor).
        """
        timeout = max(0.2, float(cfg.health_timeout_s))

        snap: Any  # <-- IMPORTANT: widen to Any so isinstance guard is not "unreachable" to mypy
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
            and (consecutive_unhealthy >= int(cfg.max_consecutive_unhealthy))
            and (not shutdown_fired)
        ):
            shutdown_fired = True
            safe = _sanitize_health_snapshot(snap)

            log.error(
                "AUTO_EXIT_WORKER_SHUTDOWN_TRIGGERED",
                extra={
                    "consecutive_unhealthy": consecutive_unhealthy,
                    "max_consecutive_unhealthy": int(cfg.max_consecutive_unhealthy),
                    "grace_s": float(cfg.grace_s),
                    "health_every_s": float(cfg.health_every_s),
                    "health_timeout_s": float(cfg.health_timeout_s),
                    "snap_ok": bool(safe.get("ok") is True),
                    "snap_detail": str(safe.get("detail") or ""),
                    "snap_err": str(safe.get("err") or ""),
                },
            )

            if shutdown_event is not None:
                shutdown_event.set()

            if on_shutdown is not None:
                with contextlib.suppress(Exception):
                    await _maybe_await(on_shutdown(dict(snap)))

            if bool(cfg.hard_kill):
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
                log.info(
                    "AUTO_EXIT_WORKER_HEALTH_ERR %r",
                    {"err": f"{type(e).__name__}: {e}"},
                )

            if await _sleep_or_stop(stop_event, float(cfg.noop_sleep_s)):
                break
        return

    # Main loop
    while not stop_event.is_set():
        try:
            await _check_health_if_due()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.info(
                "AUTO_EXIT_WORKER_HEALTH_ERR %r",
                {"err": f"{type(e).__name__}: {e}"},
            )

        # reclaim
        try:
            await _call_engine_method(exit_engine, "reclaim", redis)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.info("EXIT_RECLAIM_ERR %r", {"err": f"{type(e).__name__}: {e}"})

        # claim
        try:
            await _call_engine_method(exit_engine, "claim", redis)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.info("EXIT_CLAIM_ERR %r", {"err": f"{type(e).__name__}: {e}"})

        if await _sleep_or_stop(stop_event, float(cfg.poll_s)):
            break


__all__ = ["AutoExitWorkerConfig", "auto_exit_worker"]
