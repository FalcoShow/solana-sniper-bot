# -*- coding: utf-8 -*-
from __future__ import annotations

"""
utils_pkg/event_logger.py

God-Tier structured event logger adapter (typed + safe)
======================================================

Purpose
- Provide a tiny, mypy-friendly interface used across the codebase:
    - .event(name, **fields)     -> emit a structured event
    - .every(key, ...)           -> throttle + (optionally) emit; returns bool when it emits
    - .once(key, ...)            -> convenience throttle for "log once per TTL"

Design goals
- Works with plain stdlib logging.Logger (fallback adapter)
- If the provided logger already implements event/every/once, we defer to it
- Safe forwarding: stdlib logging forbids arbitrary kwargs, so extra structured fields
  are attached under extra={"fields": {...}}.
- Exception-safe: logging must never crash the trading loop.
- Bounded memory for throttling keys (prevents unbounded growth on long uptime).
"""

import logging
import time
from dataclasses import dataclass, field
from threading import Lock
from typing import Any, Mapping, Protocol, runtime_checkable


@runtime_checkable
class EventLogger(Protocol):
    """
    Minimal typed interface used across the codebase.

    Notes
    - event(): structured event emission
    - every(): time-based throttle, returns True iff it emitted
    - once(): time-based throttle for "log once per TTL", returns True iff it emitted
    """

    def event(
        self, name: str, /, msg: str | None = None, level: str | int = "INFO", **fields: Any
    ) -> None: ...

    def every(
        self,
        key: str,
        *args: Any,
        every_sec: float | None = None,
        level: str | int = "INFO",
        msg: str | None = None,
        **fields: Any,
    ) -> bool: ...

    def once(
        self,
        key: str,
        /,
        msg: str | None = None,
        level: str | int = "INFO",
        **fields: Any,
    ) -> bool: ...

    # Common logging.Logger API used across the repo.
    def debug(self, msg: object, *args: object, **kwargs: Any) -> None: ...
    def info(self, msg: object, *args: object, **kwargs: Any) -> None: ...
    def warning(self, msg: object, *args: object, **kwargs: Any) -> None: ...
    def error(self, msg: object, *args: object, **kwargs: Any) -> None: ...
    def exception(self, msg: object, *args: object, **kwargs: Any) -> None: ...
    def critical(self, msg: object, *args: object, **kwargs: Any) -> None: ...

    # Some modules check handlers to avoid double-wiring.
    @property
    def handlers(self) -> list[logging.Handler]: ...  # pragma: no cover

    def addHandler(self, hdlr: logging.Handler) -> None: ...  # pragma: no cover
    def removeHandler(self, hdlr: logging.Handler) -> None: ...  # pragma: no cover
    def setLevel(self, level: int) -> None: ...  # pragma: no cover


def _coerce_int(value: object, default: int) -> int:
    """Best-effort object->int conversion (never raises)."""
    try:
        if value is None:
            return default
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str):
            s = value.strip()
            if not s:
                return default
            try:
                return int(s, 0)
            except Exception:
                try:
                    return int(float(s))
                except Exception:
                    return default
        return int(value)  # type: ignore[call-overload]
    except Exception:
        return default


def _coerce_float(value: object, default: float) -> float:
    """Best-effort object->float conversion (never raises)."""
    try:
        if value is None:
            return default
        if isinstance(value, bool):
            return float(int(value))
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            s = value.strip()
            if not s:
                return default
            try:
                return float(s)
            except Exception:
                return default
        return float(value)  # type: ignore[arg-type]
    except Exception:
        return default


def _split_log_kwargs(
    kwargs: dict[str, Any],
) -> tuple[Any, bool, int, Mapping[str, object] | None, dict[str, object]]:
    """
    Stdlib logging accepts only certain keyword args.
    Anything else must be routed into structured fields.
    """
    exc_info = kwargs.pop("exc_info", None)
    stack_info_raw = kwargs.pop("stack_info", False)
    stacklevel_raw = kwargs.pop("stacklevel", 1)
    extra_raw = kwargs.pop("extra", None)

    stack_info = bool(stack_info_raw) if stack_info_raw is not None else False
    stacklevel = _coerce_int(stacklevel_raw, 1)

    extra: Mapping[str, object] | None = extra_raw if isinstance(extra_raw, Mapping) else None
    fields: dict[str, object] = dict(kwargs)
    return exc_info, stack_info, stacklevel, extra, fields


def _merge_extra(
    extra: Mapping[str, object] | None,
    fields: Mapping[str, object] | None,
    *,
    event_name: str | None = None,
) -> dict[str, object] | None:
    """
    Merge caller-provided extra with our structured fields under `fields`.
    Adds optional `event` key (if caller didn't already provide one).
    """
    if not extra and not fields and event_name is None:
        return None

    merged: dict[str, object] = dict(extra) if extra else {}

    if event_name is not None and "event" not in merged:
        merged["event"] = event_name

    if fields:
        existing = merged.get("fields")
        if isinstance(existing, dict):
            tmp = dict(existing)
            tmp.update(dict(fields))
            merged["fields"] = tmp
        else:
            merged["fields"] = dict(fields)

    return merged


@dataclass(slots=True)
class _ThrottleState:
    lock: Lock = field(default_factory=Lock)
    last_emit: dict[str, float] = field(default_factory=dict)
    calls_since_prune: int = 0


@dataclass(slots=True)
class EventLoggerAdapter:
    """
    Wrap a stdlib logging.Logger and provide `.event()` / `.every()` / `.once()`.

    If the wrapped logger already supports those methods, we defer to it.
    Otherwise we provide consistent fallback behavior.

    IMPORTANT:
    We accept arbitrary **kwargs on debug/info/etc, but we do NOT forward them
    directly into stdlib logging. We route them into extra={"fields": ...}.
    """

    _base: logging.Logger
    _throttle: _ThrottleState = field(default_factory=_ThrottleState)

    # Contextual tags bound to every call:
    _bound: dict[str, object] = field(default_factory=dict)

    # Throttle memory bounds:
    _max_throttle_keys: int = 2048
    _prune_every_n_calls: int = 256

    @property
    def handlers(self) -> list[logging.Handler]:  # pragma: no cover
        return self._base.handlers

    def addHandler(self, hdlr: logging.Handler) -> None:  # pragma: no cover
        self._base.addHandler(hdlr)

    def removeHandler(self, hdlr: logging.Handler) -> None:  # pragma: no cover
        self._base.removeHandler(hdlr)

    def setLevel(self, level: int) -> None:  # pragma: no cover
        self._base.setLevel(level)

    @property
    def base(self) -> logging.Logger:
        return self._base

    def bind(self, **fields: object) -> "EventLoggerAdapter":
        """Return a new adapter with persistent bound fields (shares throttle state)."""
        try:
            merged = dict(self._bound)
            merged.update(fields)
            return EventLoggerAdapter(
                self._base,
                _throttle=self._throttle,
                _bound=merged,
                _max_throttle_keys=self._max_throttle_keys,
                _prune_every_n_calls=self._prune_every_n_calls,
            )
        except Exception:
            return self

    def _merge_bound(self, fields: Mapping[str, Any] | None) -> dict[str, Any]:
        if not self._bound:
            return dict(fields) if fields else {}
        out: dict[str, Any] = dict(self._bound)
        if fields:
            out.update(dict(fields))
        return out

    def _maybe_prune_emit_cache(self, *, now: float) -> None:
        """Prevent unbounded growth of throttle keys on long uptime (call under lock)."""
        st = self._throttle
        st.calls_since_prune += 1
        if st.calls_since_prune < self._prune_every_n_calls:
            return
        st.calls_since_prune = 0

        if len(st.last_emit) <= self._max_throttle_keys:
            return

        try:
            items = sorted(st.last_emit.items(), key=lambda kv: kv[1])
            drop_n = max(1, int(len(items) * 0.10))
            for k, _ts in items[:drop_n]:
                st.last_emit.pop(k, None)
        except Exception:
            return

    # ------------------------------------------------------------------
    # Structured methods
    # ------------------------------------------------------------------

    def event(
        self, name: str, /, msg: str | None = None, level: str | int = "INFO", **fields: Any
    ) -> None:
        merged_fields: dict[str, Any] = self._merge_bound(fields)

        # Prefer native structured event if present.
        try:
            fn = getattr(self._base, "event", None)
            if callable(fn):
                fn(name, msg=msg, level=level, **merged_fields)  # type: ignore[misc]
                return
        except Exception:
            pass

        # Fallback: emit via stdlib logging with structured extras.
        try:
            extra = _merge_extra(None, merged_fields, event_name=name)
            text = msg if msg is not None else name

            if isinstance(level, str):
                lvlno = logging.getLevelName(level.upper())
                if isinstance(lvlno, int):
                    self._base.log(lvlno, "%s %s", text, merged_fields, extra=extra)
                else:
                    self._base.info("%s %s", text, merged_fields, extra=extra)
            else:
                self._base.log(int(level), "%s %s", text, merged_fields, extra=extra)
        except Exception:
            return

    def every(
        self,
        key: str,
        *args: Any,
        every_sec: float | None = None,
        level: str | int = "INFO",
        msg: str | None = None,
        **fields: Any,
    ) -> bool:
        """
        Throttle events by time interval (seconds).
        Returns True iff an emit occurred.

        Compatibility:
        - accepts legacy positional first arg as interval
        - accepts legacy n=30 via kwargs
        """
        merged_fields: dict[str, Any] = self._merge_bound(fields)

        # Prefer native throttled logger if present.
        try:
            fn = getattr(self._base, "every", None)
            if callable(fn):
                out = fn(  # type: ignore[misc]
                    key, *args, every_sec=every_sec, level=level, msg=msg, **merged_fields
                )
                return bool(out) if out is not None else True
        except Exception:
            pass

        try:
            if every_sec is not None:
                interval = max(1.0, float(every_sec))
            elif args:
                interval = max(1.0, _coerce_float(args[0], 30.0))
            else:
                interval = max(1.0, _coerce_float(merged_fields.pop("n", 30), 30.0))

            now = time.monotonic()
            st = self._throttle

            with st.lock:
                last = float(st.last_emit.get(key, 0.0))
                if now - last < interval:
                    self._maybe_prune_emit_cache(now=now)
                    return False
                st.last_emit[key] = now
                self._maybe_prune_emit_cache(now=now)

            # Emit
            if msg is not None:
                self.log(level, msg, **merged_fields)
            else:
                self.event(key, level=level, **merged_fields)
            return True
        except Exception:
            return False

    def once(
        self, key: str, /, msg: str | None = None, level: str | int = "INFO", **fields: Any
    ) -> bool:
        """
        Convenience: "log once per TTL".

        Compatibility:
        - supports ttl / ttl_sec / every_sec / n in kwargs
        - default TTL is 3600s if nothing provided
        """
        merged_fields: dict[str, Any] = self._merge_bound(fields)

        # Prefer native once if present.
        try:
            fn = getattr(self._base, "once", None)
            if callable(fn):
                out = fn(key, msg=msg, level=level, **merged_fields)  # type: ignore[misc]
                return bool(out) if out is not None else True
        except Exception:
            pass

        ttl = merged_fields.pop("ttl_sec", None)
        if ttl is None:
            ttl = merged_fields.pop("ttl", None)
        if ttl is None:
            ttl = merged_fields.pop("every_sec", None)
        if ttl is None:
            ttl = merged_fields.pop("n", None)

        ttl_sec = max(1.0, _coerce_float(ttl, 3600.0))
        return self.every(key, every_sec=ttl_sec, level=level, msg=msg, **merged_fields)

    # ------------------------------------------------------------------
    # stdlib-compatible logger methods (accept arbitrary structured kwargs safely)
    # ------------------------------------------------------------------

    def log(self, level: str | int, msg: object, *args: object, **kwargs: Any) -> None:
        if isinstance(level, str):
            lvlno = logging.getLevelName(level.upper())
            if isinstance(lvlno, int):
                self._log(self._base.log, msg, *args, level=lvlno, **kwargs)
            else:
                self._log(self._base.info, msg, *args, **kwargs)
        else:
            self._log(self._base.log, msg, *args, level=int(level), **kwargs)

    def debug(self, msg: object, *args: object, **kwargs: Any) -> None:
        self._log(self._base.debug, msg, *args, **kwargs)

    def info(self, msg: object, *args: object, **kwargs: Any) -> None:
        self._log(self._base.info, msg, *args, **kwargs)

    def warning(self, msg: object, *args: object, **kwargs: Any) -> None:
        self._log(self._base.warning, msg, *args, **kwargs)

    def error(self, msg: object, *args: object, **kwargs: Any) -> None:
        self._log(self._base.error, msg, *args, **kwargs)

    def critical(self, msg: object, *args: object, **kwargs: Any) -> None:
        self._log(self._base.critical, msg, *args, **kwargs)

    def exception(self, msg: object, *args: object, **kwargs: Any) -> None:
        try:
            kw = dict(kwargs)
            if "exc_info" not in kw:
                kw["exc_info"] = True
            self._log(self._base.exception, msg, *args, **kw)
        except Exception:
            return

    def _log(self, fn: Any, msg: object, *args: object, **kwargs: Any) -> None:
        """Route unknown kwargs into LogRecord.extra safely. Never raises."""
        try:
            kw = dict(kwargs)
            lvl = kw.pop("level", None)

            exc_info, stack_info, stacklevel, extra, fields = _split_log_kwargs(kw)
            merged_fields = self._merge_bound(fields)
            merged_extra = _merge_extra(extra, merged_fields)

            if lvl is not None:
                fn(
                    int(lvl),
                    msg,
                    *args,
                    exc_info=exc_info,
                    stack_info=stack_info,
                    stacklevel=stacklevel,
                    extra=merged_extra,
                )
                return

            if merged_fields and not args:
                fn(
                    "%s %s",
                    msg,
                    merged_fields,
                    exc_info=exc_info,
                    stack_info=stack_info,
                    stacklevel=stacklevel,
                    extra=merged_extra,
                )
            else:
                fn(
                    msg,
                    *args,
                    exc_info=exc_info,
                    stack_info=stack_info,
                    stacklevel=stacklevel,
                    extra=merged_extra,
                )
        except Exception:
            return


def get_event_logger(base: logging.Logger) -> EventLogger:
    """
    Return something that satisfies EventLogger.

    - If `base` already provides event/every/once, return it (no wrapping).
    - Otherwise wrap it with EventLoggerAdapter.
    """
    try:
        has_event = callable(getattr(base, "event", None))
        has_every = callable(getattr(base, "every", None))
        has_once = callable(getattr(base, "once", None))
        if has_event and has_every and has_once:
            return base  # type: ignore[return-value]
    except Exception:
        pass
    return EventLoggerAdapter(base)


def ensure_event_logger(obj: object) -> EventLogger:
    """
    Best-effort adapter: accepts EventLogger, logging.Logger, or anything.
    Never raises outward; falls back to a module logger.
    """
    try:
        if isinstance(obj, EventLogger):
            return obj
    except Exception:
        pass

    if isinstance(obj, logging.Logger):
        return get_event_logger(obj)

    return get_event_logger(logging.getLogger("event_logger"))


__all__ = [
    "EventLogger",
    "EventLoggerAdapter",
    "ensure_event_logger",
    "get_event_logger",
]
