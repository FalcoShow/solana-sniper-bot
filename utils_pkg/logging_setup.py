# -*- coding: utf-8 -*-
from __future__ import annotations

"""
utils_pkg/logging_setup.py (COMPAT WRAPPER — NO DRIFT)
======================================================

LOCK-IN
-------
- Root `logging_setup.py` (repo root) is canonical logging configuration (handlers/formatters).
- `utils_pkg.log_compat` is the canonical *logger contract* (EventLogger + get_logger signature).
- This module exists ONLY to preserve backwards compatibility for legacy imports:
    from utils_pkg.logging_setup import get_logger, Heartbeat, hook_stdio, ...

Design goals
------------
1) Side-effect free import: no logging configuration at import time.
2) No competing logger types: EventLogger is re-exported from utils_pkg.log_compat.
3) Delegate handler setup to root logging_setup (drift-safe signature forwarding).
4) Keep hardening features:
   - secret/DSN redaction filter
   - burst limiting filter
   - dedupe filter
   - heartbeat counter + stdio hooks + exception hooks
   - context passthrough to root if present
"""

import asyncio
import collections
import contextlib
import importlib
import inspect
import logging
import os
import re
import sys
import threading
import time
from io import TextIOBase
from types import TracebackType
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    # Canonical contract lives here (single surface across repo).
    from utils_pkg.log_compat import EventLogger as EventLogger  # noqa: F401
else:
    # Runtime import is safe in-repo; if something truly exotic happens, fall back to Any.
    try:
        from utils_pkg.log_compat import EventLogger as EventLogger  # type: ignore
    except Exception:  # pragma: no cover
        EventLogger = Any  # type: ignore[assignment,misc]


# =============================================================================
# Lazy root module access (avoids import-time side effects)
# =============================================================================


def _root_module() -> Any:
    """Import root logging_setup lazily to avoid import-time config side effects."""
    mod = sys.modules.get("logging_setup")
    if mod is not None:
        return mod
    return importlib.import_module("logging_setup")


def _call_root(fn_name: str, **kwargs: Any) -> Any:
    """
    Call a function from root logging_setup, safely.

    - If the target doesn't exist, returns None.
    - Filters kwargs to match the root function signature (drift-safe).
    """
    root = _root_module()
    fn = getattr(root, fn_name, None)
    if not callable(fn):
        return None

    try:
        params = set(inspect.signature(fn).parameters.keys())
        safe = {k: v for k, v in kwargs.items() if k in params}
    except Exception:
        safe = dict(kwargs)

    try:
        return fn(**safe)
    except TypeError:
        # Best-effort fallback for older "get_logger(name)" style signatures.
        if fn_name == "get_logger" and "name" in kwargs:
            try:
                return fn(kwargs["name"])
            except Exception:
                return None
        return None
    except Exception:
        return None


# =============================================================================
# Env helpers (support both LOG_* and legacy PF_* knobs)
# =============================================================================

_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}

_LEVEL_MAP: dict[str, int] = {
    "CRITICAL": logging.CRITICAL,
    "FATAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARN": logging.WARNING,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
    "TRACE": logging.DEBUG,  # stdlib has no TRACE
}


def _env_str(*names: str, default: str = "") -> str:
    for n in names:
        v = os.getenv(n)
        if v is not None and str(v).strip():
            return str(v).strip()
    return default


def _env_bool(*names: str, default: bool = False) -> bool:
    v = _env_str(*names, default="")
    if not v:
        return default
    s = v.strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return default


def _env_int(*names: str, default: int = 0) -> int:
    v = _env_str(*names, default="")
    if not v:
        return default
    try:
        return int(float(v.replace("_", "")))
    except Exception:
        return default


def _env_float(*names: str, default: float = 0.0) -> float:
    v = _env_str(*names, default="")
    if not v:
        return default
    try:
        return float(v.replace("_", ""))
    except Exception:
        return default


def _env_level() -> str | int:
    s = _env_str("PF_LOG_LEVEL", "LOG_LEVEL", default="INFO").upper()
    return s if s in _LEVEL_MAP else "INFO"


def _env_service() -> str:
    return _env_str("PF_SERVICE", "LOG_SERVICE", "APP_SERVICE", "APP_NAME", default="sniper")


def _env_version() -> str:
    return _env_str("PF_VERSION", "LOG_VERSION", "APP_VERSION", "VERSION", default="v1")


def _env_json_logs() -> bool:
    return _env_bool("PF_LOG_JSON", "LOG_JSON", default=True)


def _env_use_utc() -> bool:
    return _env_bool("PF_LOG_TIME_UTC", "LOG_TIME_UTC", default=True)


def _env_include_src() -> bool:
    return _env_bool("PF_LOG_INCLUDE_SRC", "LOG_INCLUDE_SRC", default=False)


def _env_stream() -> str | None:
    s = _env_str("PF_LOG_STREAM", "LOG_STREAM", default="")
    return s or None


# =============================================================================
# Extra sanitization (prevents LogRecord collisions on extra=)
# =============================================================================

_RESERVED_LOGRECORD_KEYS: set[str] = set(logging.makeLogRecord({}).__dict__.keys()) | {
    "message",
    "asctime",
}


def _sanitize_extra(extra: dict[str, Any] | None) -> dict[str, Any]:
    if not extra:
        return {}
    out: dict[str, Any] = {}
    for k, v in extra.items():
        kk = str(k)
        nk = f"x_{kk}" if kk in _RESERVED_LOGRECORD_KEYS else kk
        if nk in out:
            i = 2
            base = nk
            while f"{base}_{i}" in out:
                i += 1
            nk = f"{base}_{i}"
        out[nk] = v
    return out


# =============================================================================
# Global handler filters (applied ONCE to root handler(s))
# =============================================================================

_SENSITIVE_KEY_RE = re.compile(
    r"(token|secret|api[_-]?key|private|mnemonic|seed|pass(word)?|bearer|authorization|cookie|dsn|keypair|wallet)",
    re.IGNORECASE,
)
_DSN_RE = re.compile(r"(\w+://)([^:@/]+):([^@/]+)@")


def _redact_dsn(s: str) -> str:
    try:
        return _DSN_RE.sub(r"\1\2:***@", s)
    except Exception:
        return s


def _looks_secretish(val: str) -> bool:
    v = val.strip()
    return len(v) >= 40 and any(c.isdigit() for c in v) and any(c.isalpha() for c in v)


def _redact_value(key: str, value: Any) -> Any:
    k = (key or "").strip()

    if isinstance(value, str):
        if "://" in value and "@" in value and ":" in value:
            return _redact_dsn(value)
        if _SENSITIVE_KEY_RE.search(k) and _looks_secretish(value):
            return value[:4] + "…" + "*" * 6
        low = value.lower()
        if "bearer " in low and len(value) > 20:
            parts = value.split()
            if len(parts) >= 2:
                parts[1] = parts[1][:4] + "…" + "*" * 6
                return " ".join(parts)
        return _redact_dsn(value)

    if isinstance(value, dict):
        return {str(kk): _redact_value(str(kk), vv) for kk, vv in value.items()}
    if isinstance(value, (list, tuple)):
        return [_redact_value(k, x) for x in value]
    return value


class SecretRedactionFilter(logging.Filter):
    """Best-effort redaction for extras + DSN-like patterns."""

    def __init__(self, enabled: bool = True) -> None:
        super().__init__()
        self.enabled = bool(enabled)

    def filter(self, record: logging.LogRecord) -> bool:
        if not self.enabled:
            return True

        # Avoid breaking %-formatting: never mutate record.args shape unsafely.
        try:
            args = getattr(record, "args", None)
            if isinstance(args, dict):
                record.args = {k: _redact_value(str(k), v) for k, v in args.items()}
            elif isinstance(args, tuple):
                record.args = tuple(_redact_value("", v) for v in args)
        except Exception:
            pass

        # Redact extra fields attached to the record.
        for k, v in list(record.__dict__.items()):
            if k in ("exc_info", "stack_info", "args", "msg"):
                continue
            if k.startswith("_"):
                continue
            with contextlib.suppress(Exception):
                record.__dict__[k] = _redact_value(k, v)

        # Redact DSN-like patterns in message template itself.
        if isinstance(record.msg, str):
            record.msg = _redact_dsn(record.msg)

        return True


class DedupFilter(logging.Filter):
    """
    Drops identical bursts within window_sec.
    First log passes; subsequent suppressed until window expires.
    When next passes, it adds record.suppressed=<count> if any were dropped.
    """

    def __init__(self, window_sec: float = 20.0, max_keys: int = 20_000) -> None:
        super().__init__()
        self.window = float(max(0.0, window_sec))
        self.max_keys = int(max(1000, max_keys))
        self._lock = threading.Lock()
        self._state: dict[tuple[Any, ...], tuple[float, int]] = {}

    def filter(self, record: logging.LogRecord) -> bool:
        if self.window <= 0:
            return True

        event = getattr(record, "event", None)
        try:
            msg = record.getMessage()
        except Exception:
            msg = str(getattr(record, "msg", ""))

        key = (record.name, record.levelno, event, msg)
        now = time.monotonic()

        with self._lock:
            next_ok, suppressed = self._state.get(key, (0.0, 0))
            if now >= next_ok:
                if suppressed:
                    record.suppressed = suppressed
                self._state[key] = (now + self.window, 0)

                if len(self._state) > self.max_keys:
                    with contextlib.suppress(Exception):
                        self._state.pop(next(iter(self._state)))

                return True

            self._state[key] = (next_ok, suppressed + 1)
            return False


class BurstLimitFilter(logging.Filter):
    """Caps logs per second per key bucket."""

    def __init__(self, max_per_sec: int = 200, key_mode: str = "logger+level") -> None:
        super().__init__()
        self.max_per_sec = int(max(0, max_per_sec))
        self.key_mode = (key_mode or "logger+level").strip().lower()
        self._lock = threading.Lock()
        self._sec = 0
        self._counts: dict[str, int] = {}

    def _k(self, record: logging.LogRecord) -> str:
        if self.key_mode == "global":
            return "global"
        if self.key_mode == "logger":
            return record.name
        if self.key_mode == "level":
            return str(record.levelno)
        if self.key_mode == "event":
            return str(getattr(record, "event", record.name))
        return f"{record.name}:{record.levelno}"

    def filter(self, record: logging.LogRecord) -> bool:
        if self.max_per_sec <= 0:
            return True
        now_sec = int(time.time())
        k = self._k(record)
        with self._lock:
            if now_sec != self._sec:
                self._sec = now_sec
                self._counts.clear()
            c = self._counts.get(k, 0) + 1
            self._counts[k] = c
            return c <= self.max_per_sec


_WRAPPER_SETUP_LOCK = threading.Lock()
_WRAPPER_SETUP_DONE = False


def _iter_root_handlers() -> list[logging.Handler]:
    return list(logging.getLogger().handlers)


def _install_root_filters_once(force: bool = False) -> None:
    """Adds redaction/dedup/burst filters to root handlers once (if handlers exist)."""
    global _WRAPPER_SETUP_DONE
    handlers = _iter_root_handlers()
    if not handlers:
        return

    with _WRAPPER_SETUP_LOCK:
        if _WRAPPER_SETUP_DONE and not force:
            return

        redact_enabled = _env_bool("PF_LOG_REDACT", "LOG_REDACT", default=True)
        burst_max = _env_int("PF_LOG_BURST_MAX_PER_SEC", "LOG_BURST_MAX_PER_SEC", default=200)
        burst_key = _env_str("PF_LOG_BURST_KEY", "LOG_BURST_KEY", default="logger+level")
        dedup_window = _env_float("PF_LOG_DEDUP_WINDOW_SEC", "LOG_DEDUP_WINDOW_SEC", default=20.0)
        dedup_max_keys = _env_int("PF_LOG_DEDUP_MAX_KEYS", "LOG_DEDUP_MAX_KEYS", default=20_000)

        for h in handlers:
            existing = list(getattr(h, "filters", []) or [])

            red = next((f for f in existing if isinstance(f, SecretRedactionFilter)), None)
            if red is None:
                h.addFilter(SecretRedactionFilter(enabled=redact_enabled))
            else:
                red.enabled = bool(redact_enabled)

            burst = next((f for f in existing if isinstance(f, BurstLimitFilter)), None)
            if burst_max > 0:
                if burst is None:
                    h.addFilter(BurstLimitFilter(max_per_sec=burst_max, key_mode=burst_key))
                else:
                    burst.max_per_sec = int(burst_max)
                    burst.key_mode = str(burst_key or "logger+level").strip().lower()

            dedup = next((f for f in existing if isinstance(f, DedupFilter)), None)
            if dedup_window > 0:
                if dedup is None:
                    h.addFilter(DedupFilter(window_sec=dedup_window, max_keys=dedup_max_keys))
                else:
                    dedup.window = float(max(0.0, dedup_window))
                    dedup.max_keys = int(max(1000, dedup_max_keys))

        _WRAPPER_SETUP_DONE = True


# =============================================================================
# Public logging configuration (delegates to root)
# =============================================================================


def configure_logging(
    *,
    level: str | int | None = None,
    json_logs: bool | None = None,
    use_utc: bool | None = None,
    include_src: bool | None = None,
    service: str | None = None,
    version: str | None = None,
    env: str | None = None,
    stream: str | None = None,
    force: bool = False,
) -> None:
    """Compatibility configure (no import-time side effects)."""
    lvl = level if level is not None else _env_level()
    jl = _env_json_logs() if json_logs is None else bool(json_logs)
    utc = _env_use_utc() if use_utc is None else bool(use_utc)
    src = _env_include_src() if include_src is None else bool(include_src)
    svc = service if service is not None else _env_service()
    ver = version if version is not None else _env_version()
    stm = stream if stream is not None else _env_stream()

    out = _call_root(
        "configure_logging",
        level=lvl,
        json_logs=jl,
        use_utc=utc,
        include_src=src,
        service=svc,
        version=ver,
        env=env,
        stream=stm,
        force=force,
    )
    if out is None:
        _call_root(
            "setup_logging",
            level=lvl,
            json_logs=jl,
            use_utc=utc,
            include_src=src,
            service=svc,
            version=ver,
            env=env,
            stream=stm,
            force=force,
        )

    _install_root_filters_once(force=force)


def setup_logging(**kwargs: Any) -> None:
    """Legacy alias used across older modules."""
    configure_logging(**kwargs)


# =============================================================================
# Canonical get_logger (EventLogger contract is utils_pkg.log_compat)
# =============================================================================


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
    """Returns canonical EventLogger from utils_pkg.log_compat (single repo-wide surface)."""
    configure_logging(
        level=level,
        json_logs=json_logs,
        use_utc=use_utc,
        include_src=include_src,
        service=service,
        version=version,
        env=env,
        stream=stream,
        force=force,
    )

    from utils_pkg import log_compat as _lc  # type: ignore

    lg = _lc.get_logger(
        name=name,
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

    _install_root_filters_once(force=False)
    return lg


def set_level(logger: logging.Logger, level_str: str) -> None:
    """Legacy helper: set global level quickly (best-effort)."""
    s = (level_str or "INFO").upper()
    os.environ["PF_LOG_LEVEL"] = s
    os.environ["LOG_LEVEL"] = s
    lvl = _LEVEL_MAP.get(s, logging.INFO)
    logging.getLogger().setLevel(lvl)
    with contextlib.suppress(Exception):
        logger.setLevel(lvl)


# =============================================================================
# Heartbeat (hot-path safe)
# =============================================================================


class Heartbeat:
    def __init__(
        self, logger: EventLogger, interval: float = 30.0, name: str = "HEARTBEAT"
    ) -> None:
        self.log = logger
        self.interval = float(interval)
        self.name = str(name)
        self._ctr: collections.Counter[str] = collections.Counter()
        self._lock = threading.Lock()
        self._t = threading.Thread(target=self._run, daemon=True)
        self._running = False

    def start(self) -> "Heartbeat":
        if not self._running:
            self._running = True
            self._t.start()
        return self

    def stop(self) -> None:
        self._running = False

    def bump(self, key: str, n: int = 1) -> None:
        with self._lock:
            self._ctr[str(key)] += int(n)

    def _run(self) -> None:
        while self._running:
            time.sleep(self.interval)
            snap: dict[str, int] = {}
            with self._lock:
                if self._ctr:
                    snap = dict(self._ctr)
                    self._ctr.clear()
            if snap:
                self.log.event(self.name, msg=self.name, counters=snap)


_global_hb: Heartbeat | None = None


def start_global_heartbeat(
    logger: EventLogger, interval: float = 30.0, name: str = "HEARTBEAT"
) -> Heartbeat:
    global _global_hb
    _global_hb = Heartbeat(logger, interval=float(interval), name=str(name)).start()
    return _global_hb


def bump(key: str, n: int = 1) -> None:
    if _global_hb:
        _global_hb.bump(str(key), int(n))


# =============================================================================
# Stdout/stderr capture
# =============================================================================

_STDIO_LOCK = threading.Lock()
_STDIO_STACK: list[tuple[Any, Any]] = []


class _PrintToLogger(TextIOBase):
    def __init__(self, logger: EventLogger, level: int) -> None:
        super().__init__()
        self.logger = logger
        self.level = int(level)
        self._buf = ""

    def writable(self) -> bool:  # pragma: no cover
        return True

    def write(self, s: Any) -> int:
        if isinstance(s, (bytes, bytearray, memoryview)):
            with contextlib.suppress(Exception):
                s = bytes(s).decode("utf-8", "replace")
            if not isinstance(s, str):
                s = str(s)
        elif not isinstance(s, str):
            with contextlib.suppress(Exception):
                s = str(s)
            if not isinstance(s, str):
                s = "<unprintable>"

        self._buf += s
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            line = line.strip()
            if line:
                self.logger.event("STDIO", msg="STDIO", level=self.level, line=line)
        return len(s)

    def flush(self) -> None:
        b = self._buf.strip()
        if b:
            self.logger.event("STDIO", msg="STDIO", level=self.level, line=b)
            self._buf = ""


def hook_stdio(
    logger: EventLogger, stdout_level: int = logging.INFO, stderr_level: int = logging.ERROR
) -> None:
    with _STDIO_LOCK:
        _STDIO_STACK.append((sys.stdout, sys.stderr))
        sys.stdout = _PrintToLogger(logger, stdout_level)  # type: ignore[assignment]
        sys.stderr = _PrintToLogger(logger, stderr_level)  # type: ignore[assignment]


def restore_stdio() -> None:
    with _STDIO_LOCK:
        if not _STDIO_STACK:
            return
        out, err = _STDIO_STACK.pop()
        sys.stdout = out  # type: ignore[assignment]
        sys.stderr = err  # type: ignore[assignment]


unhook_stdio = restore_stdio


# =============================================================================
# Exception hooks
# =============================================================================

_ExcTuple = tuple[type[BaseException], BaseException, TracebackType | None]
_ExcInfo = _ExcTuple | tuple[None, None, None]
ExcInfoArg = _ExcInfo | BaseException | bool | None


def _log_exc(logger: EventLogger, title: str, exc_info: ExcInfoArg, **fields: Any) -> None:
    """Best-effort exception emission that NEVER raises."""
    merged_extra = _sanitize_extra(dict(fields or {}))

    raw = getattr(logger, "raw", None)
    if isinstance(raw, logging.Logger):
        with contextlib.suppress(Exception):
            raw.error(title, exc_info=exc_info, extra=merged_extra, stacklevel=2)
            return

    with contextlib.suppress(Exception):
        logger.error(title, exc_info=exc_info, **merged_extra)  # type: ignore[misc]
        return

    with contextlib.suppress(Exception):
        logger.event("EXCEPTION", msg=title, level="ERROR", **merged_extra)


def install_excepthook(logger: EventLogger) -> None:
    def _hook(exc_type: type[BaseException], exc: BaseException, tb: Any) -> None:
        try:
            _log_exc(
                logger, "UNCAUGHT", (exc_type, exc, tb), event="UNCAUGHT", exc_type=str(exc_type)
            )
        finally:
            sys.__excepthook__(exc_type, exc, tb)

    sys.excepthook = _hook


def install_asyncio_exception_hook(logger: EventLogger, loop: Any = None) -> None:
    lp: Any = loop
    if lp is None:
        with contextlib.suppress(Exception):
            lp = asyncio.get_running_loop()
    if lp is None:
        with contextlib.suppress(Exception):
            lp = asyncio.get_event_loop()
    if lp is None:
        return

    def _handler(_lp: Any, context: dict[str, Any]) -> None:
        fields2 = dict(context or {})
        fields2.setdefault("event", "ASYNCIO_EXCEPTION")
        if "message" in fields2:
            fields2["asyncio_message"] = fields2.pop("message")

        exc = fields2.get("exception")
        if isinstance(exc, BaseException):
            exc_info2: _ExcTuple = (type(exc), exc, getattr(exc, "__traceback__", None))
            _log_exc(logger, "ASYNCIO_EXCEPTION", exc_info2, **fields2)
            return

        with contextlib.suppress(Exception):
            logger.error("ASYNCIO_EXCEPTION", **fields2)
            return

        with contextlib.suppress(Exception):
            logger.event("ASYNCIO_EXCEPTION", msg="ASYNCIO_EXCEPTION", level="ERROR", **fields2)

    with contextlib.suppress(Exception):
        lp.set_exception_handler(_handler)


# =============================================================================
# Context passthrough to canonical root (lazy)
# =============================================================================


def set_log_context(**kwargs: Any) -> None:
    fn = getattr(_root_module(), "set_log_context", None)
    if callable(fn):
        with contextlib.suppress(Exception):
            fn(**kwargs)


def clear_log_context(*keys: str) -> None:
    fn = getattr(_root_module(), "clear_log_context", None)
    if callable(fn):
        with contextlib.suppress(Exception):
            fn(*keys)


def get_log_context() -> dict[str, Any]:
    fn = getattr(_root_module(), "get_log_context", None)
    if callable(fn):
        with contextlib.suppress(Exception):
            out = fn()
            return dict(out) if isinstance(out, dict) else {}
    return {}


__all__ = [
    "EventLogger",
    "configure_logging",
    "setup_logging",
    "get_logger",
    "set_level",
    "SecretRedactionFilter",
    "DedupFilter",
    "BurstLimitFilter",
    "Heartbeat",
    "start_global_heartbeat",
    "bump",
    "hook_stdio",
    "restore_stdio",
    "unhook_stdio",
    "install_excepthook",
    "install_asyncio_exception_hook",
    "set_log_context",
    "clear_log_context",
    "get_log_context",
]
