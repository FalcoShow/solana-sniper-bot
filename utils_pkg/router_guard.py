from __future__ import annotations

"""
utils_pkg/router_guard.py — V3+++ (God-Tier Overhaul, Phase A) — CANONICAL / LOCK-IN
===================================================================================

Stable router-facing guard entrypoint for execution reliability.

Why this file exists
--------------------
- router.py should have ONE place to ask:
      "Should I attempt Jupiter for this mint/spend right now?"
- utils_pkg.routeguard implements heavy decisioning + caching.
- router_guard provides:
    1) stable import surface (no cycles, no network at import time)
    2) compatibility aliases (older call sites don’t break)
    3) execution reliability primitives (idempotency, singleflight, breakers)

Public API (LOCKED)
-------------------
  async def preflight(*, mint: str, lamports_in: int, **kwargs) -> tuple[bool, dict[str, Any] | None]
  def clear_cache(*, cancel_inflight: bool = False) -> None
  def stats() -> dict[str, Any]

Execution reliability helpers (opt-in)
--------------------------------------
  def idempotency_key(scope: str, payload: Any, *, salt: str | None = None) -> str
  async def singleflight(key: str, fn: Callable[[], Awaitable[T]]) -> T

Circuit breaker helpers (opt-in)
--------------------------------
  def cb_is_open(name: str) -> bool
  async def cb_is_open_async(name: str) -> bool
  def cb_record_success(name: str) -> None
  def cb_record_fail(name: str) -> None

Contract (router MUST follow)
-----------------------------
Return value from preflight():
  (True, quote_dict) -> Jupiter allowed. quote_dict may be {} meaning "allow Jupiter but no prequote".
  (True, None)       -> fallback-only allowed (router MUST NOT attempt Jupiter).
  (False, None)      -> blocked outright.

CRITICAL COMPAT NOTE
--------------------
When guard is DISABLED, we MUST NOT return None.
We return (True, {}) meaning "allow Jupiter, no enforced prequote".

Hard requirements
-----------------
- NO network at import time.
- No import cycles with router/routeguard (lazy import only).
- Never raise unexpectedly from preflight() — optimistic or pessimistic per env.
- Type/contract stable for mypy + tests.

E741 guarantee
--------------
No ambiguous variable names: no `l`, `I`, `O` identifiers anywhere.
"""

import asyncio
import contextlib
import hashlib
import inspect
import json
import os
import threading
import time
from collections import deque
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol, TypeVar, cast

T = TypeVar("T")

# =============================================================================
# Logging contract (Phase A): EventLogger Protocol (import-safe, no monkeypatch)
# =============================================================================


class EventLogger(Protocol):
    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def info(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def error(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def log(self, level: int, msg: str, *args: Any, **kwargs: Any) -> None: ...


class _EventCapable(EventLogger, Protocol):
    def event(self, name: str, **fields: Any) -> None: ...


class _EveryCapable(EventLogger, Protocol):
    def every(self, key: str, n: int, *, msg: str | None = None, **fields: Any) -> None: ...


def get_logger(name: str | None = None) -> EventLogger:
    """
    Single signature. Always returns an EventLogger-compatible object.

    Priority:
      1) utils_pkg.log_compat.get_event_logger / get_logger
      2) logging_setup.get_logger
      3) stdlib logging.getLogger (basicConfig if needed)
    """
    nm = (name or "utils_pkg.router_guard").strip() or "utils_pkg.router_guard"

    with contextlib.suppress(Exception):
        from utils_pkg.log_compat import get_event_logger  # type: ignore

        return cast(EventLogger, get_event_logger(nm))

    with contextlib.suppress(Exception):
        from utils_pkg.log_compat import get_logger as gl  # type: ignore

        return cast(EventLogger, gl(nm))

    with contextlib.suppress(Exception):
        from logging_setup import get_logger as gl2  # type: ignore

        return cast(EventLogger, gl2(nm))

    import logging

    lg = logging.getLogger(nm)
    if not lg.handlers:
        logging.basicConfig(level=logging.INFO)
    return cast(EventLogger, lg)


_LOG: EventLogger = get_logger(__name__)
_EVERY_STORE: dict[str, float] = {}

# Optional redactor (do not require)
_REDACTOR: Any | None = None
with contextlib.suppress(Exception):
    from utils_pkg.log_redaction import redact_text as _rt  # type: ignore

    _REDACTOR = _rt  # type: ignore[assignment]


def _redact_text(value: Any) -> str:
    try:
        if _REDACTOR is not None:
            return str(_REDACTOR(str(value)))
    except Exception:
        pass
    return str(value)


def _short(value: Any, limit: int = 280) -> str:
    try:
        text = _redact_text(value)
        return text if len(text) <= limit else (text[:limit] + "…")
    except Exception:
        return "<?>"


def _log_event(name: str, **fields: Any) -> None:
    """Never raises; uses .event if available else info."""
    try:
        evt = getattr(_LOG, "event", None)
        if callable(evt):
            safe = {str(k): _short(v, 520) for k, v in fields.items()}
            cast(_EventCapable, _LOG).event(str(name), **safe)
            return
    except Exception:
        pass

    with contextlib.suppress(Exception):
        safe2 = {str(k): _short(v, 520) for k, v in fields.items()}
        _LOG.info("%s %s", _short(name, 120), safe2)


def _log_every(key: str, seconds: int = 10, msg: str | None = None, **fields: Any) -> None:
    """Rate-limited log helper. Never raises."""
    try:
        every_fn = getattr(_LOG, "every", None)
        if callable(every_fn):
            safe = {str(k): _short(v, 520) for k, v in fields.items()}
            cast(_EveryCapable, _LOG).every(str(key), int(max(1, seconds)), msg=msg, **safe)
            return

        now_mono = time.monotonic()
        last_mono = float(_EVERY_STORE.get(key, 0.0))
        window_sec = float(max(1, int(seconds)))
        if (now_mono - last_mono) >= window_sec:
            _EVERY_STORE[key] = now_mono
            _log_event(msg or key, **fields)
    except Exception:
        return


# =============================================================================
# Env helpers
# =============================================================================

_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}


def _envs(name: str, default: str = "") -> str:
    raw = os.getenv(name)
    return (raw if raw is not None else default).strip()


def _envb(name: str, default: bool = False) -> bool:
    raw = _envs(name, "")
    if raw == "":
        return bool(default)
    low = raw.lower().strip()
    if low in _TRUE:
        return True
    if low in _FALSE:
        return False
    return bool(default)


def _envi(name: str, default: int) -> int:
    try:
        return int(float(_envs(name, str(default))))
    except Exception:
        return int(default)


def _enabled() -> bool:
    """Outer kill-switch (routeguard has its own enable too)."""
    return _envb("ROUTER_GUARD_ENABLED", True)


def _should_singleflight() -> bool:
    # Default ON: only dedupes identical concurrent preflight calls; output-stable.
    return _envb("ROUTER_GUARD_SINGLEFLIGHT", True)


def _optimistic_on_error() -> bool:
    return _envb("ROUTER_GUARD_OPTIMISTIC_ON_ERROR", True)


def _fallback_only_if_missing() -> bool:
    return _envb("ROUTER_GUARD_FALLBACK_ONLY_IF_MISSING", False)


# =============================================================================
# Stable JSON hashing (idempotency keys)
# =============================================================================


def _json_stable(obj: Any) -> str:
    """
    Serialize deterministically for hashing.
    - sort_keys ensures stable dict ordering
    - default=str prevents crashes on Pubkey/Decimal/etc
    """
    try:
        return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)
    except Exception:
        return json.dumps({"repr": str(obj)}, sort_keys=True, separators=(",", ":"))


def idempotency_key(scope: str, payload: Any, *, salt: str | None = None) -> str:
    """
    Deterministic idempotency key for dedupe / anti-double-spend.

    scope: short string like "swap", "buy", "sell"
    payload: any JSON-ish object (quote, mint, user, amount)
    salt: optional stable salt (do NOT put secrets here)
    """
    scope_s = (scope or "scope").strip().lower()
    salt_s = (salt or "").strip()
    raw_bytes = f"{scope_s}|{salt_s}|{_json_stable(payload)}".encode("utf-8", "ignore")
    return hashlib.sha256(raw_bytes).hexdigest()


# =============================================================================
# Singleflight (in-process dedupe)
# =============================================================================


@dataclass
class _SingleFlight:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    inflight: dict[str, asyncio.Future[Any]] = field(default_factory=dict)

    async def run(self, key: str, fn: Callable[[], Awaitable[T]]) -> T:
        dedupe_key = (key or "").strip()
        if not dedupe_key:
            return await fn()

        async with self.lock:
            fut_any = self.inflight.get(dedupe_key)
            if fut_any is None or fut_any.done():
                loop = asyncio.get_running_loop()
                fut_any = loop.create_future()
                self.inflight[dedupe_key] = fut_any
                leader = True
            else:
                leader = False

        fut = cast(asyncio.Future[T], fut_any)

        if not leader:
            return cast(T, await asyncio.shield(fut))

        try:
            out = await fn()
            if not fut.done():
                fut.set_result(out)
            return out
        except Exception as err:
            if not fut.done():
                fut.set_exception(err)
            raise
        finally:
            async with self.lock:
                cur = self.inflight.get(dedupe_key)
                if cur is fut_any:
                    self.inflight.pop(dedupe_key, None)


_SINGLEFLIGHT = _SingleFlight()


async def singleflight(key: str, fn: Callable[[], Awaitable[T]]) -> T:
    return await _SINGLEFLIGHT.run(key, fn)


# =============================================================================
# Circuit breaker (opt-in) — sync-safe state
# =============================================================================


@dataclass
class _Breaker:
    fail_times: deque[float] = field(default_factory=deque)
    open_until: float = 0.0


class _CircuitBreakers:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._breakers: dict[str, _Breaker] = {}

    def is_open(self, name: str) -> bool:
        breaker_name = (name or "").strip()
        if not breaker_name:
            return False
        now_mono = time.monotonic()
        with self._lock:
            bk = self._breakers.get(breaker_name)
            if not bk:
                return False
            return now_mono < float(bk.open_until)

    def record_success(self, name: str) -> None:
        breaker_name = (name or "").strip()
        if not breaker_name:
            return
        with self._lock:
            bk = self._breakers.get(breaker_name)
            if not bk:
                return
            bk.fail_times.clear()
            bk.open_until = 0.0

    def record_fail(self, name: str) -> None:
        breaker_name = (name or "").strip()
        if not breaker_name:
            return

        window_sec = max(1, _envi("ROUTER_GUARD_CB_WINDOW_SEC", 30))
        fail_max = max(0, _envi("ROUTER_GUARD_CB_FAIL_MAX", 6))  # 0 disables
        cooldown_sec = max(0, _envi("ROUTER_GUARD_CB_COOLDOWN_SEC", 20))  # 0 disables

        if fail_max <= 0 or cooldown_sec <= 0:
            return

        now_mono = time.monotonic()
        with self._lock:
            bk = self._breakers.get(breaker_name)
            if not bk:
                bk = _Breaker()
                self._breakers[breaker_name] = bk

            bk.fail_times.append(now_mono)
            while bk.fail_times and (now_mono - bk.fail_times[0]) > float(window_sec):
                bk.fail_times.popleft()

            if len(bk.fail_times) >= fail_max:
                bk.open_until = max(bk.open_until, now_mono + float(cooldown_sec))
                _log_every(
                    "ROUTER_GUARD_CB_OPEN",
                    6,
                    msg="ROUTER_GUARD_CB_OPEN",
                    name=breaker_name,
                    window_sec=window_sec,
                    fail_max=fail_max,
                    cooldown_sec=cooldown_sec,
                )

    def clear(self) -> None:
        with self._lock:
            self._breakers.clear()

    def count(self) -> int:
        with self._lock:
            return len(self._breakers)

    def open_names(self) -> list[str]:
        now_mono = time.monotonic()
        with self._lock:
            return [k for k, bk in self._breakers.items() if now_mono < float(bk.open_until)]


_CBS = _CircuitBreakers()


def cb_is_open(name: str) -> bool:
    with contextlib.suppress(Exception):
        return _CBS.is_open(name)
    return False


async def cb_is_open_async(name: str) -> bool:
    return cb_is_open(name)


def cb_record_success(name: str) -> None:
    with contextlib.suppress(Exception):
        _CBS.record_success(name)


def cb_record_fail(name: str) -> None:
    with contextlib.suppress(Exception):
        _CBS.record_fail(name)


# =============================================================================
# Delegation to routeguard (lazy import; avoids cycles)
# =============================================================================

_ROUTE_GUARD_MOD: Any | None = None
_ROUTE_GUARD_TRIED: bool = False


def _routeguard() -> Any | None:
    global _ROUTE_GUARD_MOD, _ROUTE_GUARD_TRIED
    if _ROUTE_GUARD_TRIED:
        return _ROUTE_GUARD_MOD
    _ROUTE_GUARD_TRIED = True
    try:
        from utils_pkg import routeguard as rg  # type: ignore

        _ROUTE_GUARD_MOD = rg
        return rg
    except Exception:
        _ROUTE_GUARD_MOD = None
        return None


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def _map_to_str_dict(value: Mapping[Any, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, val in value.items():
        out[str(key)] = val
    return out


def _normalize_preflight_result(value: Any) -> tuple[bool, dict[str, Any] | None]:
    """
    Accepts:
      - (bool, dict|None)
      - dict-like mapping -> treat as allow with quote dict
      - True/False -> allow/block with no quote
      - anything else -> optimistic allow {}
    """
    if isinstance(value, tuple) and len(value) == 2:
        ok_val = bool(value[0])
        q_val = value[1]
        if q_val is None:
            return ok_val, None
        if isinstance(q_val, Mapping):
            return ok_val, _map_to_str_dict(q_val)
        return ok_val, {}
    if isinstance(value, Mapping):
        return True, _map_to_str_dict(value)
    if isinstance(value, bool):
        return value, ({} if value else None)
    return True, {}


def _filter_kwargs_for_callable(fn: Any, raw_kwargs: dict[str, Any]) -> dict[str, Any]:
    """
    Best-effort: only pass kwargs in signature (if inspectable).
    If fn has **kwargs, pass everything.
    """
    try:
        sig = inspect.signature(fn)
        params = list(sig.parameters.values())
        has_var_kw = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params)
        if has_var_kw:
            return dict(raw_kwargs)
        allowed = {p.name for p in params}
        return {k: v for k, v in raw_kwargs.items() if k in allowed}
    except Exception:
        return dict(raw_kwargs)


async def preflight(
    *,
    mint: str,
    lamports_in: int,
    **kwargs: Any,
) -> tuple[bool, dict[str, Any] | None]:
    """
    Router-facing preflight decision.

    Delegates to utils_pkg.routeguard.preflight() if present.
    Drift-tolerant to signature changes via kwargs filtering + fallback call.

    IMPORTANT: If guard is disabled, we MUST NOT return None (return True, {}).
    """
    if not _enabled():
        return True, {}

    rg = _routeguard()
    fn = getattr(rg, "preflight", None) if rg else None
    if not callable(fn):
        return (True, None) if _fallback_only_if_missing() else (True, {})

    async def _call() -> tuple[bool, dict[str, Any] | None]:
        try:
            use_kwargs = _filter_kwargs_for_callable(fn, dict(kwargs))
            out = await _maybe_await(fn(mint=mint, lamports_in=int(lamports_in), **use_kwargs))  # type: ignore[misc]
            return _normalize_preflight_result(out)
        except TypeError:
            # Minimal signature fallback
            out2 = await _maybe_await(fn(mint=mint, lamports_in=int(lamports_in)))  # type: ignore[misc]
            return _normalize_preflight_result(out2)
        except Exception as err:
            optimistic = _optimistic_on_error()
            _log_every(
                "ROUTER_GUARD_PREFLIGHT_ERR_OPTIMISTIC"
                if optimistic
                else "ROUTER_GUARD_PREFLIGHT_ERR_BLOCK",
                10,
                msg="ROUTER_GUARD_PREFLIGHT_ERR_OPTIMISTIC"
                if optimistic
                else "ROUTER_GUARD_PREFLIGHT_ERR_BLOCK",
                mint=str(mint),
                lamports_in=int(lamports_in),
                err=_short(err, 220),
            )
            return (True, {}) if optimistic else (False, None)

    if _should_singleflight():
        key = idempotency_key(
            "preflight",
            {"mint": str(mint), "lamports_in": int(lamports_in)},
            salt="",
        )
        return await singleflight(key, _call)

    return await _call()


def clear_cache(*, cancel_inflight: bool = False) -> None:
    """
    Clear guard caches/breakers.

    Delegates to routeguard.clear_cache() when available.
    Also clears local singleflight + circuit breakers.
    """
    rg = _routeguard()
    fn = getattr(rg, "clear_cache", None) if rg else None
    if callable(fn):
        with contextlib.suppress(Exception):
            fn(cancel_inflight=cancel_inflight)

    async def _clear_singleflight_async() -> None:
        async with _SINGLEFLIGHT.lock:
            if cancel_inflight:
                for fut in list(_SINGLEFLIGHT.inflight.values()):
                    with contextlib.suppress(Exception):
                        if not fut.done():
                            fut.cancel()
            _SINGLEFLIGHT.inflight.clear()

    # Best-effort: if a loop is running, clear under lock; else clear directly.
    try:
        loop = asyncio.get_running_loop()
        if loop.is_running():
            with contextlib.suppress(Exception):
                loop.create_task(_clear_singleflight_async())
        else:
            with contextlib.suppress(Exception):
                _SINGLEFLIGHT.inflight.clear()
    except Exception:
        with contextlib.suppress(Exception):
            _SINGLEFLIGHT.inflight.clear()

    with contextlib.suppress(Exception):
        _CBS.clear()


def stats() -> dict[str, Any]:
    rg = _routeguard()
    fn = getattr(rg, "stats", None) if rg else None

    base: dict[str, Any] = {}
    if callable(fn):
        with contextlib.suppress(Exception):
            out = fn()
            if isinstance(out, dict):
                # ensure string keys (Render-friendly JSON)
                base = {str(k): v for k, v in out.items()}

    # Local wrapper stats
    base["router_guard_enabled"] = int(_enabled())
    base["router_guard_singleflight_enabled"] = int(_should_singleflight())
    base["router_guard_singleflight_inflight"] = len(_SINGLEFLIGHT.inflight)
    base["router_guard_breakers"] = _CBS.count()
    base["router_guard_breakers_open"] = _CBS.open_names()

    # Env snapshot (Render debugging)
    base["router_guard_cb_window_sec"] = _envi("ROUTER_GUARD_CB_WINDOW_SEC", 30)
    base["router_guard_cb_fail_max"] = _envi("ROUTER_GUARD_CB_FAIL_MAX", 6)
    base["router_guard_cb_cooldown_sec"] = _envi("ROUTER_GUARD_CB_COOLDOWN_SEC", 20)
    base["router_guard_optimistic_on_error"] = int(_optimistic_on_error())
    base["router_guard_fallback_only_if_missing"] = int(_fallback_only_if_missing())
    return base


# Compatibility aliases (older call sites)
rg_preflight = preflight
route_preflight = preflight
router_preflight = preflight

__all__ = [
    "EventLogger",
    "get_logger",
    "preflight",
    "clear_cache",
    "stats",
    "idempotency_key",
    "singleflight",
    "cb_is_open",
    "cb_is_open_async",
    "cb_record_success",
    "cb_record_fail",
    "rg_preflight",
    "route_preflight",
    "router_preflight",
]
