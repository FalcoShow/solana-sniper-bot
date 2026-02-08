from __future__ import annotations

"""
route_telemetry.py (ROOT) — V3+++ (God-Tier Overhaul)
=====================================================

Purpose
-------
Routing telemetry MUST be:
- Safe (never raises)
- Non-blocking (best-effort; bounded latency)
- No network at import time
- Single-source (uses shared Redis if available via utils_pkg.core_utils)
- Secret-safe logs (never print secrets/DSNs/tokens, even via exception strings)

This module is intentionally lightweight and dependency-tolerant.
It supports:
- DEX label histograms (route used)
- Watchdog deviation histograms
- Optional extra counters for route decisions / quote outcomes (future-proof)

Backwards compatibility
-----------------------
Keeps the legacy keys by default:
  route:dex_used:<LABEL>
  route:dex_watchdog:<LABEL>

You can change prefix via ROUTE_TELEM_PREFIX, but default is "route".
"""

import asyncio
import contextlib
import inspect
import os
import re
import time
from collections.abc import Awaitable, Iterable, Sequence
from dataclasses import dataclass
from typing import Any, Final, Protocol, cast

# =============================================================================
# Logging (Phase A: EventLogger contract; NO import-time config)
# =============================================================================


class EventLogger(Protocol):
    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def info(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def error(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def log(self, level: int, msg: str, *args: Any, **kwargs: Any) -> None: ...


class EventLoggerWithEvent(EventLogger, Protocol):
    def event(self, name: str, **fields: Any) -> None: ...


class EventLoggerWithEvery(EventLogger, Protocol):
    def every(self, key: str, n: int, *, msg: str | None = None, **fields: Any) -> None: ...


def get_logger(name: str | None = None) -> EventLogger:
    """
    Canonical logger getter.

    Phase A requirements:
      - single canonical signature
      - returns EventLogger-like object (not stdlib Logger typing)
    """
    nm = (name or "route_telemetry").strip() or "route_telemetry"

    # Prefer compat layer if present
    with contextlib.suppress(Exception):
        from utils_pkg.log_compat import get_event_logger  # type: ignore

        return cast(EventLogger, get_event_logger(nm))

    with contextlib.suppress(Exception):
        from utils_pkg.log_compat import get_logger as gl  # type: ignore

        return cast(EventLogger, gl(nm))

    # Preferred root helper
    with contextlib.suppress(Exception):
        from logging_setup import get_logger as gl2  # type: ignore

        return cast(EventLogger, gl2(nm))

    # Fallback: stdlib logger (no import-time network)
    import logging

    lg = logging.getLogger(nm)
    if not lg.handlers:
        logging.basicConfig(level=logging.INFO)
    return cast(EventLogger, lg)


_LOG: EventLogger = get_logger(__name__)
_EVERY_STORE: dict[str, float] = {}

# =============================================================================
# Redaction (never leak secrets / DSNs / tokens)
# =============================================================================

_REDACTOR: Any | None = None
with contextlib.suppress(Exception):
    from utils_pkg.log_redaction import redact_text as _rt  # type: ignore

    _REDACTOR = _rt  # type: ignore[assignment]


_RE_URL_CRED = re.compile(r"(?i)\b([a-z][a-z0-9+.-]*://)([^/\s:@]+):([^@\s/]+)@")
_RE_AUTH_BEARER = re.compile(r"(?i)\b(bearer)\s+([a-z0-9._\-]{12,})")
_RE_KV_SECRET = re.compile(
    r"(?i)\b(api[_-]?key|token|secret|password|passwd|pwd|private[_-]?key)\b\s*[:=]\s*([^\s'\";,]+)"
)
_RE_LONG_TOKEN = re.compile(r"\b([A-Za-z0-9_\-]{24,})\b")


def _fallback_redact_text(s: str) -> str:
    if not s:
        return s
    out = str(s)
    out = _RE_URL_CRED.sub(r"\1***:***@", out)
    out = _RE_AUTH_BEARER.sub(r"\1 ***", out)
    out = _RE_KV_SECRET.sub(r"\1=***", out)

    def _mask_long(m: re.Match[str]) -> str:
        tok = m.group(1)
        if len(tok) <= 28:
            return "***"
        return f"{tok[:4]}…{tok[-4:]}"

    out = _RE_LONG_TOKEN.sub(_mask_long, out)
    return out


def _redact_text(s: str) -> str:
    try:
        if _REDACTOR is not None:
            return str(_REDACTOR(str(s)))
    except Exception:
        pass
    return _fallback_redact_text(str(s))


def _redact_obj(x: Any, *, _depth: int = 0) -> Any:
    if _depth >= 4:
        return _redact_text(str(x))
    try:
        if x is None or isinstance(x, (int, float, bool)):
            return x
        if isinstance(x, str):
            return _redact_text(x)
        if isinstance(x, dict):
            out: dict[str, Any] = {}
            n = 0
            for k, v in x.items():
                out[_redact_text(str(k))] = _redact_obj(v, _depth=_depth + 1)
                n += 1
                if n >= 50:
                    out["__truncated__"] = True
                    break
            return out
        if isinstance(x, (list, tuple)):
            out_list: list[Any] = []
            for i, v in enumerate(x):
                if i >= 50:
                    out_list.append("__truncated__")
                    break
                out_list.append(_redact_obj(v, _depth=_depth + 1))
            return out_list
    except Exception:
        return "***"
    return _redact_text(str(x))


def _log_event(name: str, **fields: Any) -> None:
    safe_fields = _redact_obj(fields)
    try:
        if hasattr(_LOG, "event") and callable(getattr(_LOG, "event", None)):
            cast(EventLoggerWithEvent, _LOG).event(name, **cast(dict[str, Any], safe_fields))
            return
    except Exception:
        pass
    with contextlib.suppress(Exception):
        _LOG.info("%s %s", name, safe_fields)


def _log_every(key: str, n: int = 30, msg: str | None = None, **fields: Any) -> None:
    try:
        if hasattr(_LOG, "every") and callable(getattr(_LOG, "every", None)):
            safe_fields = _redact_obj(fields)
            cast(EventLoggerWithEvery, _LOG).every(key, int(max(1, n)), msg=msg, **safe_fields)
            return
    except Exception:
        pass

    try:
        sec = float(max(1, int(n)))
        now_mono = time.monotonic()
        last_mono = float(_EVERY_STORE.get(key, 0.0))
        if (now_mono - last_mono) >= sec:
            _EVERY_STORE[key] = now_mono
            _log_event(msg or key, **fields)
    except Exception:
        return


# =============================================================================
# Env + config
# =============================================================================

_TRUE: Final[set[str]] = {"1", "true", "yes", "y", "on", "t"}
_FALSE: Final[set[str]] = {"0", "false", "no", "n", "off", "f"}


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


ROUTE_TELEM_ENABLED: bool = _envb("ROUTE_TELEM_ENABLED", True)
ROUTE_TELEM_PREFIX: str = _envs("ROUTE_TELEM_PREFIX", "route")
ROUTE_TELEM_TIMEOUT_MS: int = _envi("ROUTE_TELEM_TIMEOUT_MS", 350)
ROUTE_TELEM_MAX_LABELS: int = _envi("ROUTE_TELEM_MAX_LABELS", 12)
ROUTE_TELEM_LABEL_MAXLEN: int = _envi("ROUTE_TELEM_LABEL_MAXLEN", 32)

ROUTE_TELEM_MIRROR_LEGACY_KEYS: bool = _envb(
    "ROUTE_TELEM_MIRROR_LEGACY_KEYS", ROUTE_TELEM_PREFIX != "route"
)
ROUTE_TELEM_DISABLE_DIRECT_REDIS: bool = _envb("ROUTE_TELEM_DISABLE_DIRECT_REDIS", False)

# =============================================================================
# Label normalization (cardinality-safe)
# =============================================================================


def _norm(label: str) -> str:
    text = (label or "OTHER").strip().upper().replace(" ", "_")
    buf: list[str] = []
    for ch in text:
        if ch.isalnum() or ch in {"_", "-", "."}:
            buf.append(ch)
        else:
            buf.append("_")
    out = "".join(buf).strip("_") or "OTHER"
    max_len = max(8, int(ROUTE_TELEM_LABEL_MAXLEN))
    if len(out) > max_len:
        out = out[:max_len]
    return out


def _uniq_norm(labels: Iterable[Any]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    max_labels = max(1, int(ROUTE_TELEM_MAX_LABELS))
    for raw in labels:
        if raw is None:
            continue
        lbl = _norm(str(raw))
        if lbl in seen:
            continue
        seen.add(lbl)
        out.append(lbl)
        if len(out) >= max_labels:
            break
    return out or ["OTHER"]


def _keys_for(kind: str, labels: Sequence[str]) -> tuple[list[str], list[str]]:
    primary = [f"{ROUTE_TELEM_PREFIX}:{kind}:{lbl}" for lbl in labels]
    legacy: list[str] = []
    if ROUTE_TELEM_MIRROR_LEGACY_KEYS and ROUTE_TELEM_PREFIX != "route":
        legacy = [f"route:{kind}:{lbl}" for lbl in labels]
    return primary, legacy


# =============================================================================
# Redis backend (shared-first, safe, cached; no import-time network)
# =============================================================================


@dataclass(frozen=True, slots=True)
class _Cfg:
    timeout_ms: int = ROUTE_TELEM_TIMEOUT_MS


class _NoopPipe:
    def incrby(self, *_args: Any, **_kwargs: Any) -> "_NoopPipe":
        return self

    def incr(self, *_args: Any, **_kwargs: Any) -> "_NoopPipe":
        return self

    def execute(self) -> None:
        return None


class _NoopRedis:
    def incrby(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    def incr(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    def pipeline(self, *_args: Any, **_kwargs: Any) -> _NoopPipe:
        return _NoopPipe()


class _RedisManager:
    def __init__(self, cfg: _Cfg) -> None:
        self._cfg = cfg
        self._lock = asyncio.Lock()
        self._client: Any = None
        self._last_err_mono = 0.0
        self._mode: str = "unset"  # shared | direct | noop

    async def _maybe_await(self, v: Any) -> Any:
        try:
            if asyncio.iscoroutine(v) or isinstance(v, Awaitable):
                return await v
            return v
        except Exception:
            return None

    async def _try_shared(self) -> Any | None:
        try:
            from utils_pkg import core_utils as cu  # type: ignore

            fn = getattr(cu, "get_redis_async", None)
            if callable(fn):
                return await self._maybe_await(fn())

            fn2 = getattr(cu, "get_redis", None)
            if callable(fn2):
                return await self._maybe_await(fn2())

            r3 = getattr(cu, "redis", None)
            if r3 is not None:
                return r3
        except Exception:
            return None
        return None

    async def _try_direct(self) -> Any:
        if ROUTE_TELEM_DISABLE_DIRECT_REDIS:
            self._mode = "noop"
            return _NoopRedis()
        try:
            import redis.asyncio as aioredis  # type: ignore

            redis_url = _envs("REDIS_URL", "")
            if not redis_url:
                self._mode = "noop"
                return _NoopRedis()
            self._mode = "direct"
            return aioredis.from_url(redis_url, decode_responses=True)
        except Exception:
            self._mode = "noop"
            return _NoopRedis()

    async def get(self) -> Any:
        if self._client is None:
            async with self._lock:
                if self._client is None:
                    shared = await self._try_shared()
                    if shared is not None:
                        self._client = shared
                        self._mode = "shared"
                    else:
                        self._client = await self._try_direct()

        if self._client is None:
            self._client = _NoopRedis()
            self._mode = "noop"

        return self._client

    def mark_err(self) -> None:
        self._last_err_mono = time.monotonic()

    def err_recent(self, sec: float = 10.0) -> bool:
        return (time.monotonic() - float(self._last_err_mono)) < float(sec)

    def mode(self) -> str:
        return self._mode


_RM = _RedisManager(_Cfg())

_MEM: dict[str, int] = {}
_MEM_LOCK = asyncio.Lock()


async def _with_timeout(awaitable: Awaitable[Any], timeout_ms: int) -> Any:
    try:
        return await asyncio.wait_for(awaitable, timeout=float(timeout_ms) / 1000.0)
    except asyncio.CancelledError:
        raise
    except Exception:
        return None


async def _call(fn: Any, *args: Any, timeout_ms: int) -> Any:
    """
    Call a possibly-async callable safely and without blocking the loop:
      - if coroutinefunction: call + await with timeout
      - else: run in thread + timeout; if result is awaitable, await it too

    Never raises (except CancelledError).
    """
    try:
        if inspect.iscoroutinefunction(fn):
            return await _with_timeout(fn(*args), timeout_ms)
        res = await _with_timeout(asyncio.to_thread(fn, *args), timeout_ms)
        if asyncio.iscoroutine(res) or isinstance(res, Awaitable):
            return await _with_timeout(res, timeout_ms)
        return res
    except asyncio.CancelledError:
        raise
    except Exception:
        return None


async def _mem_incr(keys: Sequence[str], delta: int = 1) -> None:
    async with _MEM_LOCK:
        inc = int(delta)
        for key in keys:
            _MEM[key] = int(_MEM.get(key, 0)) + inc


async def _redis_incr(keys: Sequence[str], delta: int = 1) -> None:
    if _RM.err_recent(5.0):
        await _mem_incr(keys, delta=delta)
        return

    client = await _RM.get()
    if isinstance(client, _NoopRedis):
        await _mem_incr(keys, delta=delta)
        return

    try:
        pipe = None
        with contextlib.suppress(Exception):
            pipe = client.pipeline(transaction=False)

        if pipe is not None:
            for key in keys:
                with contextlib.suppress(Exception):
                    if hasattr(pipe, "incrby"):
                        pipe.incrby(key, int(delta))
                    else:
                        pipe.incr(key)

            exec_fn = getattr(pipe, "execute", None)
            if callable(exec_fn):
                await _call(exec_fn, timeout_ms=ROUTE_TELEM_TIMEOUT_MS)
            return

        incrby_fn = getattr(client, "incrby", None)
        incr_fn = getattr(client, "incr", None)
        for key in keys:
            with contextlib.suppress(Exception):
                if callable(incrby_fn):
                    await _call(incrby_fn, key, int(delta), timeout_ms=ROUTE_TELEM_TIMEOUT_MS)
                elif callable(incr_fn):
                    await _call(incr_fn, key, timeout_ms=ROUTE_TELEM_TIMEOUT_MS)

    except asyncio.CancelledError:
        raise
    except Exception as err:
        _RM.mark_err()
        await _mem_incr(keys, delta=delta)
        _log_every(
            "route_telem_redis_err",
            n=30,
            msg="ROUTE_TELEM_REDIS_ERR",
            err=_redact_text(str(err))[:240],
            mode=_RM.mode(),
        )


# =============================================================================
# Public API (never raises)
# =============================================================================


async def count_route_used(labels: Iterable[Any]) -> None:
    """
    Increment histogram counters for which DEX labels were used in the chosen route.
    Keys:
      <PREFIX>:dex_used:<LABEL>
      (optionally) route:dex_used:<LABEL>  [legacy mirror]
    """
    if not ROUTE_TELEM_ENABLED:
        return
    try:
        norm_labels = _uniq_norm(labels)
        primary, legacy = _keys_for("dex_used", norm_labels)
        await _redis_incr(primary + legacy, delta=1)
    except asyncio.CancelledError:
        raise
    except Exception:
        return


async def count_watchdog_hit(labels: Iterable[Any]) -> None:
    """
    Increment histogram counters for watchdog deviation alerts by DEX label.
    Keys:
      <PREFIX>:dex_watchdog:<LABEL>
      (optionally) route:dex_watchdog:<LABEL>  [legacy mirror]
    """
    if not ROUTE_TELEM_ENABLED:
        return
    try:
        norm_labels = _uniq_norm(labels)
        primary, legacy = _keys_for("dex_watchdog", norm_labels)
        await _redis_incr(primary + legacy, delta=1)
    except asyncio.CancelledError:
        raise
    except Exception:
        return


# =============================================================================
# Extra “God-tier” telemetry helpers (optional for callers)
# =============================================================================


async def count_route_decision(
    action: str,
    reason: str = "",
    labels: Iterable[Any] | None = None,
) -> None:
    """
    Optional: increment allow/block counters with reasons (cardinality-safe).

    Keys:
      <PREFIX>:decision:<ACTION>
      <PREFIX>:decision_reason:<ACTION>:<REASON>
      <PREFIX>:decision_by_dex:<ACTION>:<DEX> (if labels provided)

    NOTE: We do NOT mirror legacy keys for expanded counters; only for legacy histograms.
    """
    if not ROUTE_TELEM_ENABLED:
        return
    try:
        act = _norm(action or "UNKNOWN")
        rsn = _norm(reason or "UNKNOWN")

        keys: list[str] = [
            f"{ROUTE_TELEM_PREFIX}:decision:{act}",
            f"{ROUTE_TELEM_PREFIX}:decision_reason:{act}:{rsn}",
        ]

        if labels is not None:
            norm_labels = _uniq_norm(labels)
            keys.extend(
                [f"{ROUTE_TELEM_PREFIX}:decision_by_dex:{act}:{lbl}" for lbl in norm_labels]
            )

        await _redis_incr(keys, delta=1)
    except asyncio.CancelledError:
        raise
    except Exception:
        return


async def count_quote_outcome(kind: str, ok: bool, status: int | None = None) -> None:
    """
    Optional: quote outcome counters (helps diagnose "why no routes").

    Keys:
      <PREFIX>:quote:<KIND>:OK / FAIL
      <PREFIX>:quote:<KIND>:STATUS:<CODE> (if status provided)
    """
    if not ROUTE_TELEM_ENABLED:
        return
    try:
        kind_norm = _norm(kind or "QUOTE")
        keys = [f"{ROUTE_TELEM_PREFIX}:quote:{kind_norm}:{'OK' if ok else 'FAIL'}"]
        if status is not None:
            keys.append(f"{ROUTE_TELEM_PREFIX}:quote:{kind_norm}:STATUS:{int(status)}")
        await _redis_incr(keys, delta=1)
    except asyncio.CancelledError:
        raise
    except Exception:
        return


async def snapshot_memory() -> dict[str, int]:
    """
    Debug helper: returns the in-memory counters (only used when Redis missing/errors).
    """
    async with _MEM_LOCK:
        return dict(_MEM)


async def stats() -> dict[str, Any]:
    """
    Debug helper: exposes config + backend mode (safe, no secrets).
    Never raises.
    """
    try:
        return {
            "enabled": int(ROUTE_TELEM_ENABLED),
            "prefix": ROUTE_TELEM_PREFIX,
            "timeout_ms": int(ROUTE_TELEM_TIMEOUT_MS),
            "max_labels": int(ROUTE_TELEM_MAX_LABELS),
            "label_maxlen": int(ROUTE_TELEM_LABEL_MAXLEN),
            "mirror_legacy": int(ROUTE_TELEM_MIRROR_LEGACY_KEYS),
            "disable_direct_redis": int(ROUTE_TELEM_DISABLE_DIRECT_REDIS),
            "backend_mode": _RM.mode(),
            "mem_keys": len(_MEM),
        }
    except Exception:
        return {}


__all__ = [
    "count_quote_outcome",
    "count_route_decision",
    "count_route_used",
    "count_watchdog_hit",
    "get_logger",
    "snapshot_memory",
    "stats",
]
