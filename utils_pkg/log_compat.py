from __future__ import annotations

"""
utils_pkg/log_compat.py

Logging compatibility helpers (production-safe)
===============================================

Purpose
-------
This module provides a *repo-wide* logger contract and safe adapters so that:

- Code can depend on an "EventLogger-like" object with:
  - debug/info/warning/error/exception
  - handlers (always present)
  - event(name, *, msg=None, level="INFO", **fields)
  - every(name, *args, every_sec=None, level="INFO", msg=None, **fields) -> bool

- Runtime NEVER crashes due to logging:
  - best-effort emission
  - swallow exceptions
  - sanitize stdlib `extra=` keys to avoid LogRecord collisions

- The canonical get_logger(...) signature is stable across the repo.
  This is a *high-multiplier* fix for mypy (conditional signature explosions).

Design notes
------------
- `event()` / `every()` are Protocol *methods* (not assignable attributes).
  This eliminates "Protocol expected settable variable, got read-only attribute" class of issues.

- We do NOT assume utils_pkg.logging_setup.get_logger accepts every knob (e.g., stream).
  We call it via a safe Any-cast + TypeError fallbacks to avoid:
    * mypy call-arg errors
    * runtime signature mismatches

- No heavy side effects at import time:
  stdlib patching happens lazily in ensure_event()/setup_logging().
"""

import contextlib
import logging
import time
from collections.abc import Mapping, MutableMapping
from functools import lru_cache
from typing import Any, Protocol, cast

# -----------------------------------------------------------------------------
# Typing contracts
# -----------------------------------------------------------------------------


class EventLogger(Protocol):
    """Minimal logger surface relied upon repo-wide."""

    # common log methods (signatures intentionally permissive)
    def debug(self, *args: Any, **kwargs: Any) -> Any: ...
    def info(self, *args: Any, **kwargs: Any) -> Any: ...
    def warning(self, *args: Any, **kwargs: Any) -> Any: ...
    def error(self, *args: Any, **kwargs: Any) -> Any: ...
    def exception(self, *args: Any, **kwargs: Any) -> Any: ...

    # stdlib-like: must exist (read-only is enough; attribute also satisfies this)
    @property
    def handlers(self) -> list[logging.Handler]: ...

    # stable compatibility APIs
    def event(
        self,
        name: str,
        *,
        msg: str | None = None,
        level: str | int = "INFO",
        **fields: Any,
    ) -> None: ...

    def every(
        self,
        name: str,
        *args: Any,
        every_sec: float | None = None,
        level: str | int = "INFO",
        msg: str | None = None,
        **fields: Any,
    ) -> bool: ...


class GetLogger(Protocol):
    """
    Canonical get_logger signature used across the repo.

    IMPORTANT: this signature is allowed to be a *superset* of what logging_setup supports.
    The wrapper here will safely degrade if the underlying factory doesn't accept some kwargs.
    """

    def __call__(
        self,
        name: str | None = ...,
        *,
        service: str | None = ...,
        version: str | None = ...,
        env: str | None = ...,
        level: str | int | None = ...,
        json_logs: bool | None = ...,
        use_utc: bool | None = ...,
        include_src: bool | None = ...,
        stream: str | None = ...,
        force: bool = ...,
    ) -> EventLogger: ...


# -----------------------------------------------------------------------------
# Internals: reserved keys + sanitization
# -----------------------------------------------------------------------------


@lru_cache(maxsize=1)
def _reserved_logrecord_keys() -> frozenset[str]:
    """
    Keys that cannot be written via stdlib logging's `extra=...` because they
    collide with LogRecord attributes and raise KeyError.
    """
    rec = logging.LogRecord(
        name="x",
        level=logging.INFO,
        pathname="x",
        lineno=1,
        msg="x",
        args=(),
        exc_info=None,
    )
    keys = set(rec.__dict__.keys())
    # common formatter-derived keys
    keys.update({"message", "asctime"})
    return frozenset(keys)


def _safe_extra(fields: Mapping[str, Any]) -> dict[str, Any]:
    """
    Return a dict safe to pass as `extra=...` to stdlib logging.
    Any reserved keys are renamed to `field_<name>`.
    """
    reserved = _reserved_logrecord_keys()
    out: dict[str, Any] = {}
    for k, v in fields.items():
        key = str(k)
        if key in reserved:
            out[f"field_{key}"] = v
        else:
            out[key] = v
    return out


def _level_no(level: str | int) -> int:
    """
    Convert common string levels ("INFO", "WARNING", "ERROR") to numeric.
    Keeps behavior stable across Python versions without relying on logging internals.
    """
    if isinstance(level, int):
        return int(level)

    name = str(level).strip().upper()
    if not name:
        return int(logging.INFO)

    # Prefer public API mapping first
    try:
        # logging.getLevelName expects an int; if passed str, returns a string.
        # So we use _nameToLevel if present, else fall back to INFO.
        table = getattr(logging, "_nameToLevel", None)
        if isinstance(table, dict):
            v = table.get(name)
            if isinstance(v, int):
                return int(v)
    except Exception:
        pass

    # Conservative fallback
    return int(logging.INFO)


def _first_int_arg(args: tuple[Any, ...], default: int = 1) -> int:
    """Extract an 'every N' integer from args[0] when present, avoiding bool."""
    if not args:
        return default
    a0 = args[0]
    if isinstance(a0, bool):
        return default
    if isinstance(a0, int):
        return max(int(a0), 1)
    return default


def _emit_event_best_effort(
    logger: Any,
    *,
    event: str,
    msg: str | None = None,
    level: str | int = "INFO",
    fields: Mapping[str, Any] | None = None,
) -> None:
    """
    Best-effort event emission. Never raises.

    Supports:
      1) structlog-style: logger.info(event, **fields)
      2) stdlib logging:  logger.log(levelno, msg_or_event, extra={...})
      3) fallback:        logger.info("event | {fields}")
    """
    ev = str(event) if event else "event"
    payload: dict[str, Any] = {}
    try:
        for k, v in (fields or {}).items():
            payload[str(k)] = v
        if msg is not None:
            payload.setdefault("msg", str(msg))
    except Exception:
        payload = {"msg": str(msg)} if msg is not None else {}

    lvl = _level_no(level)

    # choose method preference by level
    if lvl >= logging.ERROR:
        methods: tuple[str, ...] = ("error", "exception", "warning", "info", "debug")
    elif lvl >= logging.WARNING:
        methods = ("warning", "info", "debug")
    elif lvl <= logging.DEBUG:
        methods = ("debug", "info")
    else:
        methods = ("info", "debug")

    # 1) structlog-ish kwargs (event, **payload)
    if payload:
        for m in methods:
            fn = getattr(logger, m, None)
            if not callable(fn):
                continue
            try:
                fn(ev, **payload)
                return
            except TypeError:
                # likely stdlib signature; stop trying kwargs route
                break
            except Exception:
                continue

    # 1b) positional-only (msg_or_event)
    for m in methods:
        fn = getattr(logger, m, None)
        if not callable(fn):
            continue
        try:
            fn(msg or ev)
            return
        except Exception:
            continue

    # 2) stdlib via .log(..., extra=...)
    fn2 = getattr(logger, "log", None)
    if callable(fn2):
        try:
            extra = _safe_extra({"event": ev, **payload})
            fn2(lvl, msg or ev, extra=extra)
            return
        except Exception:
            pass

    # 3) fallback string
    fn3 = getattr(logger, "info", None)
    if callable(fn3):
        try:
            if payload:
                fn3(f"{ev} | {payload}")
            else:
                fn3(msg or ev)
        except Exception:
            pass


# -----------------------------------------------------------------------------
# stdlib monkeypatch (safe / idempotent)
# -----------------------------------------------------------------------------


def install_stdlib_event() -> None:
    """Install `.event(...)` onto `logging.Logger` if missing. Safe/idempotent."""
    if hasattr(logging.Logger, "event"):
        return

    def _event(
        self: logging.Logger,
        name: str,
        *,
        msg: str | None = None,
        level: str | int = "INFO",
        **fields: Any,
    ) -> None:
        _emit_event_best_effort(self, event=name, msg=msg, level=level, fields=fields)

    # mypy-safe: avoid direct attribute assignment on Logger's type
    with contextlib.suppress(Exception):
        cast(Any, logging.Logger).event = _event


def install_stdlib_every() -> None:
    """Install `.every(...) -> bool` onto `logging.Logger` if missing. Safe/idempotent."""
    if hasattr(logging.Logger, "every"):
        return

    def _every(
        self: logging.Logger,
        name: str,
        *args: Any,
        every_sec: float | None = None,
        level: str | int = "INFO",
        msg: str | None = None,
        **fields: Any,
    ) -> bool:
        n = _first_int_arg(args)

        st: MutableMapping[str, Any] = self.__dict__.setdefault("_log_compat_every_state", {})
        key = str(name)
        now = time.monotonic()

        last_t = float(st.get(f"{key}:t", 0.0))
        count = int(st.get(f"{key}:c", 0)) + 1
        st[f"{key}:c"] = count

        should = False
        if every_sec is not None:
            sec = float(every_sec)
            if sec <= 0.0 or (now - last_t) >= sec:
                should = True
        else:
            if (count % n) == 0:
                should = True

        if should:
            st[f"{key}:t"] = now
            _emit_event_best_effort(self, event=name, msg=msg, level=level, fields=fields)
            return True
        return False

    # mypy-safe: avoid direct attribute assignment on Logger's type
    with contextlib.suppress(Exception):
        cast(Any, logging.Logger).every = _every


# -----------------------------------------------------------------------------
# Proxy / Null logger (always safe)
# -----------------------------------------------------------------------------


class _EventProxy:
    """
    Wrapper that provides `.event()` / `.every()` / `.handlers` even if the underlying
    logger cannot be modified or lacks stdlib attributes.
    """

    __slots__ = ("_base", "_local_handlers", "_state")

    def __init__(self, base: Any) -> None:
        self._base = base
        self._local_handlers: list[logging.Handler] = []
        self._state: dict[str, Any] = {}

    def __getattr__(self, item: str) -> Any:
        return getattr(self._base, item)

    @property
    def handlers(self) -> list[logging.Handler]:
        try:
            h = getattr(self._base, "handlers", None)
            if isinstance(h, list):
                return h
        except Exception:
            pass
        return self._local_handlers

    def debug(self, *args: Any, **kwargs: Any) -> None:
        with contextlib.suppress(Exception):
            fn = getattr(self._base, "debug", None)
            if callable(fn):
                fn(*args, **kwargs)

    def info(self, *args: Any, **kwargs: Any) -> None:
        with contextlib.suppress(Exception):
            fn = getattr(self._base, "info", None)
            if callable(fn):
                fn(*args, **kwargs)

    def warning(self, *args: Any, **kwargs: Any) -> None:
        with contextlib.suppress(Exception):
            fn = getattr(self._base, "warning", None)
            if callable(fn):
                fn(*args, **kwargs)

    def error(self, *args: Any, **kwargs: Any) -> None:
        with contextlib.suppress(Exception):
            fn = getattr(self._base, "error", None)
            if callable(fn):
                fn(*args, **kwargs)

    def exception(self, *args: Any, **kwargs: Any) -> None:
        # prefer underlying exception(), else fall back to error()
        with contextlib.suppress(Exception):
            fn = getattr(self._base, "exception", None)
            if callable(fn):
                fn(*args, **kwargs)
                return
        self.error(*args, **kwargs)

    def event(
        self,
        name: str,
        *,
        msg: str | None = None,
        level: str | int = "INFO",
        **fields: Any,
    ) -> None:
        _emit_event_best_effort(self._base, event=name, msg=msg, level=level, fields=fields)

    def every(
        self,
        name: str,
        *args: Any,
        every_sec: float | None = None,
        level: str | int = "INFO",
        msg: str | None = None,
        **fields: Any,
    ) -> bool:
        n = _first_int_arg(args)

        key = str(name)
        now = time.monotonic()
        last_t = float(self._state.get(f"{key}:t", 0.0))
        count = int(self._state.get(f"{key}:c", 0)) + 1
        self._state[f"{key}:c"] = count

        should = False
        if every_sec is not None:
            sec = float(every_sec)
            if sec <= 0.0 or (now - last_t) >= sec:
                should = True
        else:
            if (count % n) == 0:
                should = True

        if should:
            self._state[f"{key}:t"] = now
            _emit_event_best_effort(self._base, event=name, msg=msg, level=level, fields=fields)
            return True
        return False


class _NullLogger:
    """Safe no-op logger that still satisfies EventLogger."""

    __slots__ = ("_handlers",)

    def __init__(self) -> None:
        self._handlers: list[logging.Handler] = []

    @property
    def handlers(self) -> list[logging.Handler]:
        return self._handlers

    def debug(self, *args: Any, **kwargs: Any) -> None:
        return None

    def info(self, *args: Any, **kwargs: Any) -> None:
        return None

    def warning(self, *args: Any, **kwargs: Any) -> None:
        return None

    def error(self, *args: Any, **kwargs: Any) -> None:
        return None

    def exception(self, *args: Any, **kwargs: Any) -> None:
        return None

    def event(
        self,
        name: str,
        *,
        msg: str | None = None,
        level: str | int = "INFO",
        **fields: Any,
    ) -> None:
        return None

    def every(
        self,
        name: str,
        *args: Any,
        every_sec: float | None = None,
        level: str | int = "INFO",
        msg: str | None = None,
        **fields: Any,
    ) -> bool:
        return False


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------


def ensure_event(logger: Any) -> EventLogger:
    """
    Ensure the returned object satisfies EventLogger and never crashes runtime.

    - Installs stdlib `.event` / `.every` globally (idempotent).
    - If logger already has `.event` + `.every` + `.handlers`, returns it.
    - Otherwise returns a proxy that provides missing pieces safely.
    """
    if logger is None:
        return cast(EventLogger, _NullLogger())

    install_stdlib_event()
    install_stdlib_every()

    try:
        has_event = callable(getattr(logger, "event", None))
        has_every = callable(getattr(logger, "every", None))
        # handlers can be attribute or property; we just need it readable as a list
        h = getattr(logger, "handlers", None)
        has_handlers = isinstance(h, list)
        if has_event and has_every and has_handlers:
            return cast(EventLogger, logger)
    except Exception:
        pass

    return cast(EventLogger, _EventProxy(logger))


def _call_project_get_logger_best_effort(
    factory: Any,
    name: str,
    *,
    service: str | None,
    version: str | None,
    env: str | None,
    level: str | int | None,
    json_logs: bool | None,
    use_utc: bool | None,
    include_src: bool | None,
    stream: str | None,
    force: bool,
) -> Any:
    """
    Call a project logger factory with progressively smaller kwarg sets.
    Never raises TypeError to callers; other exceptions are allowed to bubble to
    the outer suppress() in get_logger().
    """
    # include only non-None knobs, except force (explicit)
    base_kwargs: dict[str, Any] = {"force": force}
    if service is not None:
        base_kwargs["service"] = service
    if version is not None:
        base_kwargs["version"] = version
    if env is not None:
        base_kwargs["env"] = env
    if level is not None:
        base_kwargs["level"] = level
    if json_logs is not None:
        base_kwargs["json_logs"] = json_logs
    if use_utc is not None:
        base_kwargs["use_utc"] = use_utc
    if include_src is not None:
        base_kwargs["include_src"] = include_src
    if stream is not None:
        base_kwargs["stream"] = stream

    # Try a few sensible degradations (common mismatch: stream/include_src/use_utc/json_logs)
    candidates: list[dict[str, Any]] = []
    candidates.append(dict(base_kwargs))

    if "stream" in base_kwargs:
        k = dict(base_kwargs)
        k.pop("stream", None)
        candidates.append(k)

    # keep only core knobs
    core: dict[str, Any] = {"force": force}
    if level is not None:
        core["level"] = level
    candidates.append(core)

    # bare minimum
    candidates.append({})

    last_type_error: TypeError | None = None
    for kw in candidates:
        try:
            return factory(name, **kw)
        except TypeError as e:
            last_type_error = e
            continue

    # If we got here, everything TypeError'd; re-raise the last one so caller can suppress.
    raise last_type_error if last_type_error is not None else TypeError("get_logger call failed")


def bump(
    name: str,
    *,
    logger: Any | None = None,
    level: str | int = "INFO",
    msg: str | None = None,
    every_sec: float | None = None,
    **fields: Any,
) -> None:
    """
    Lightweight repo-wide helper used by some modules (e.g. pump_trader) to emit
    a one-shot "counter-style" event without ever risking a runtime crash.

    - If `logger` is provided, it is wrapped via ensure_event(...).
    - Otherwise we use this module's get_logger(...) fallback.
    - If `every_sec` is provided, uses logger.every(..., every_sec=...).
      Otherwise emits logger.event(...).

    This keeps the call-site contract stable and makes mypy happy.
    """
    try:
        log = ensure_event(logger) if logger is not None else get_logger(__name__)
        # common convention: count bump
        fields.setdefault("count", 1)

        if every_sec is not None:
            # gate emission by time
            log.every(name, every_sec=float(every_sec), level=level, msg=msg, **fields)
        else:
            log.event(name, level=level, msg=msg, **fields)
    except Exception:
        return


def get_logger(
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
) -> EventLogger:
    """
    Canonical repo-wide logger constructor.

    Priority:
    1) utils_pkg.logging_setup.get_logger (if present) — project formatting/handlers
    2) stdlib logging.getLogger

    Always returns an EventLogger.

    IMPORTANT:
    - We do not assume logging_setup.get_logger accepts every knob (stream/include_src/etc).
      We attempt it safely and fall back with progressively smaller kwargs.
    """
    resolved_name = name if name is not None else __name__

    # Prefer project logger factory if available (lazy import; keep import side-effect light).
    with contextlib.suppress(Exception):
        from utils_pkg.logging_setup import get_logger as _gl  # type: ignore[import-not-found]

        _gl_any = cast(Any, _gl)
        base = _call_project_get_logger_best_effort(
            _gl_any,
            resolved_name,
            service=service,
            version=version,
            env=env,
            level=level,
            json_logs=json_logs,
            use_utc=use_utc,
            include_src=include_src,
            stream=stream,
            force=force,
        )
        return ensure_event(base)

    # stdlib fallback (no global setup here; keep get_logger side-effect light)
    base2 = logging.getLogger(resolved_name)
    return ensure_event(base2)


def get_event_logger(
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
) -> EventLogger:
    """Alias for get_logger(...) to make intent explicit at call sites."""
    return get_logger(
        name,
        service=service,
        version=version,
        env=env,
        level=level,
        json_logs=json_logs,
        use_utc=use_utc,
        include_src=include_src,
        stream=stream,
        force=force,
    )


def setup_logging(*args: Any, **kwargs: Any) -> None:
    """
    Repo-wide stable logging setup entrypoint.

    If utils_pkg.logging_setup.setup_logging exists, delegate to it.
    Otherwise do a safe stdlib basicConfig fallback.

    Signature intentionally loose for backward compatibility.
    """
    with contextlib.suppress(Exception):
        from utils_pkg.logging_setup import (
            setup_logging as _setup,  # type: ignore[import-not-found]
        )

        _setup(*args, **kwargs)
        install_stdlib_event()
        install_stdlib_every()
        return

    level = kwargs.get("level", logging.INFO)
    force = bool(kwargs.get("force", False))

    try:
        logging.basicConfig(level=level, force=force)  # type: ignore[call-arg]
    except TypeError:
        # Python <3.8 doesn't support force; keep backward-safe behavior anyway
        if force:
            root = logging.getLogger()
            for h in list(getattr(root, "handlers", [])):
                with contextlib.suppress(Exception):
                    root.removeHandler(h)
        logging.basicConfig(level=level)

    install_stdlib_event()
    install_stdlib_every()


__all__ = [
    "EventLogger",
    "GetLogger",
    "bump",
    "ensure_event",
    "get_event_logger",
    "get_logger",
    "install_stdlib_event",
    "install_stdlib_every",
    "setup_logging",
]
