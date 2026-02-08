# -*- coding: utf-8 -*-
from __future__ import annotations

"""
logging_setup.py (ROOT — CANONICAL)
==================================

God-Tier Structured Logging for the entire repo.

Key guarantees
--------------
- Canonical root config (other logging shims should re-export/import from here).
- JSON by default (Render-friendly), text optional.
- Idempotent configuration (safe to call multiple times).
- Context propagation via contextvars (run_id, trade_id, request_id, etc).
- Safe extras + redaction (keys + secret values + URLs/userinfo).
- Spam guards: logger.every(...) and logger.once(...).
- Bind helpers: logger.bind(...) -> BoundLogger with merged extras.
- Import-time safe (no networking; env read only during configure/get_logger).
- Pytest-safe: does not break capture handlers; only mutates handlers we own.
- Collision-safe extra= handling (prevents LogRecord reserved overwrites).
"""

import atexit
import io
import json
import logging
import os
import re
import socket
import sys
import threading
import time
import traceback
import uuid
import weakref
from collections.abc import Iterator, Mapping
from contextlib import contextmanager, suppress
from contextvars import ContextVar, Token
from dataclasses import dataclass
from datetime import datetime, timezone
from types import TracebackType
from typing import Any, Protocol, cast

from utils_pkg.log_redaction import MASK, RedactionFilter, redact_obj, redact_text, safe_repr

UTC = timezone.utc

_BASE_STDOUT = getattr(sys, "__stdout__", None) or sys.stdout
_BASE_STDERR = getattr(sys, "__stderr__", None) or sys.stderr

# -----------------------------------------------------------------------------
# Typing: EventLogger protocol (canonical surface repo-wide)
# -----------------------------------------------------------------------------


class EventLogger(Protocol):
    handlers: list[logging.Handler]

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def info(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def error(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def critical(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def log(self, level: int, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def isEnabledFor(self, level: int) -> bool: ...  # noqa: N802

    def event(
        self, name: str, /, msg: str | None = None, level: str | int = logging.INFO, **fields: Any
    ) -> None: ...

    def every(
        self,
        name: str,
        *args: Any,
        every_sec: float | None = None,
        level: str | int = logging.INFO,
        msg: str | None = None,
        **fields: Any,
    ) -> bool: ...

    def once(
        self, name: str, /, msg: str | None = None, level: str | int = logging.INFO, **fields: Any
    ) -> bool: ...

    def bind(self, /, **fields: Any) -> "BoundLogger": ...


__all__ = [
    "EventLogger",
    "BoundLogger",
    "bind",
    "bump",
    "clear_log_context",
    "configure_logging",
    "event",
    "get_event_logger",
    "get_log_context",
    "get_logger",
    "hook_stdio",
    "install_excepthook",
    "mask_url",
    "metrics_snapshot",
    "new_run_id",
    "redact_value",
    "reset_metrics",
    "reset_logging_state",
    "set_log_context",
    "setup_logging",
    "start_global_heartbeat",
    "stop_global_heartbeat",
    "unhook_stdio",
    "uninstall_excepthook",
    "with_log_context",
]

# -----------------------------------------------------------------------------
# Contextvars (per-task/per-request metadata)
# -----------------------------------------------------------------------------

_LOG_CONTEXT: ContextVar[dict[str, Any] | None] = ContextVar("_LOG_CONTEXT", default=None)

# -----------------------------------------------------------------------------
# Metrics
# -----------------------------------------------------------------------------

_METRICS_LOCK = threading.Lock()
_METRICS: dict[str, int] = {}

# -----------------------------------------------------------------------------
# Defaults (env is read at CONFIGURE time, not import time)
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class _LogDefaults:
    service: str
    version: str
    env: str
    level: str
    json_logs: bool
    use_utc: bool
    include_src: bool
    include_process: bool
    include_host: bool
    stream: str  # stderr|stdout
    exc_max_chars: int
    stack_max_chars: int
    value_max_chars: int
    noisy_min_level: str
    every_default_sec: float
    redact_enabled: bool
    redact_key_fragments: set[str]
    dedupe_window_sec: float


_DEFAULT_REDACT_KEY_FRAGMENTS: set[str] = {
    "password",
    "pass",
    "passwd",
    "secret",
    "token",
    "access_token",
    "refresh_token",
    "api_key",
    "apikey",
    "private_key",
    "auth",
    "authorization",
    "cookie",
    "set-cookie",
    "session",
    "mnemonic",
    "seed",
    "signature",
    "dsn",
    "database_url",
    "postgres",
    "redis",
    "rpc",
    "jupiter",
    "openai",
    "telegram",
    "discord",
}

_DEFAULTS: _LogDefaults = _LogDefaults(
    service="sniper",
    version="v1",
    env="dev",
    level="INFO",
    json_logs=True,
    use_utc=True,
    include_src=False,
    include_process=True,
    include_host=True,
    stream="stderr",
    exc_max_chars=8000,
    stack_max_chars=8000,
    value_max_chars=4096,
    noisy_min_level="INFO",
    every_default_sec=30.0,
    redact_enabled=True,
    redact_key_fragments=set(_DEFAULT_REDACT_KEY_FRAGMENTS),
    dedupe_window_sec=0.0,
)

_LEVEL_ALIASES: dict[str, str] = {
    "WARN": "WARNING",
    "FATAL": "CRITICAL",
    "ERR": "ERROR",
    "TRACE": "DEBUG",
}

_RESERVED_LOGRECORD_KEYS: set[str] = set(logging.makeLogRecord({}).__dict__.keys()) | {
    "message",
    "asctime",
}

# -----------------------------------------------------------------------------
# Env parsing helpers (ONLY keys already in env contract)
# -----------------------------------------------------------------------------


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name, "")
    if raw == "":
        return default
    return raw.strip().lower() not in ("0", "false", "no", "off", "n")


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "")
    if raw == "":
        return default
    try:
        return int(raw.strip())
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name, "")
    if raw == "":
        return default
    try:
        return float(raw.strip())
    except Exception:
        return default


def _infer_env_name() -> str:
    v = (os.getenv("LOG_ENV", "") or "").strip()
    if v:
        return v

    for k in ("APP_ENV", "ENV"):
        vv = (os.getenv(k, "") or "").strip()
        if vv:
            return vv

    if (os.getenv("RENDER", "") or "").strip() not in ("", "0", "false", "False"):
        return "prod"
    if (os.getenv("RENDER_SERVICE_ID", "") or "").strip():
        return "prod"
    if (os.getenv("RENDER_INSTANCE_ID", "") or "").strip():
        return "prod"

    return "dev"


def _read_env_defaults() -> _LogDefaults:
    service = (os.getenv("LOG_SERVICE", _DEFAULTS.service) or _DEFAULTS.service).strip()
    version = (os.getenv("LOG_VERSION", _DEFAULTS.version) or _DEFAULTS.version).strip()
    env = _infer_env_name()

    level = (os.getenv("LOG_LEVEL", _DEFAULTS.level) or _DEFAULTS.level).strip().upper()
    json_logs = _env_bool("LOG_JSON", _DEFAULTS.json_logs)
    use_utc = _env_bool("LOG_TIME_UTC", _DEFAULTS.use_utc)
    include_src = _env_bool("LOG_INCLUDE_SRC", _DEFAULTS.include_src)
    include_process = _env_bool("LOG_INCLUDE_PROCESS", _DEFAULTS.include_process)
    include_host = _env_bool("LOG_INCLUDE_HOST", _DEFAULTS.include_host)

    stream = (os.getenv("LOG_STREAM", _DEFAULTS.stream) or _DEFAULTS.stream).strip().lower()
    if stream not in ("stderr", "stdout"):
        stream = "stderr"

    exc_max_chars = int(_DEFAULTS.exc_max_chars)
    stack_max_chars = int(_DEFAULTS.stack_max_chars)
    value_max_chars = int(_DEFAULTS.value_max_chars)

    noisy_min_level = (
        (os.getenv("LOG_NOISY_LEVEL", _DEFAULTS.noisy_min_level) or _DEFAULTS.noisy_min_level)
        .strip()
        .upper()
    )
    every_default_sec = float(_DEFAULTS.every_default_sec)

    redact_enabled = _env_bool("LOG_REDACT", _DEFAULTS.redact_enabled)

    extra = os.getenv("LOG_REDACT_KEYS", "") or ""
    fragments = set(_DEFAULT_REDACT_KEY_FRAGMENTS)
    for part in extra.replace(";", ",").split(","):
        p = part.strip().lower()
        if p:
            fragments.add(p)

    dedupe_window_sec = _env_float("LOG_DEDUP_WINDOW_SEC", _DEFAULTS.dedupe_window_sec)
    dedupe_window_sec = max(0.0, min(float(dedupe_window_sec), 3600.0))

    service = (service or "sniper")[:64]
    version = (version or "v1")[:64]
    env = (env or "dev")[:32]

    return _LogDefaults(
        service=service,
        version=version,
        env=env,
        level=level,
        json_logs=bool(json_logs),
        use_utc=bool(use_utc),
        include_src=bool(include_src),
        include_process=bool(include_process),
        include_host=bool(include_host),
        stream=str(stream),
        exc_max_chars=exc_max_chars,
        stack_max_chars=stack_max_chars,
        value_max_chars=value_max_chars,
        noisy_min_level=noisy_min_level,
        every_default_sec=every_default_sec,
        redact_enabled=bool(redact_enabled),
        redact_key_fragments={k.lower() for k in fragments},
        dedupe_window_sec=float(dedupe_window_sec),
    )


def _parse_level(level: str | int | None) -> int:
    if level is None:
        return int(getattr(logging, _DEFAULTS.level, logging.INFO))
    if isinstance(level, int):
        return int(level)

    s = str(level).strip().upper()
    if not s:
        return int(getattr(logging, _DEFAULTS.level, logging.INFO))
    if s.isdigit():
        try:
            return int(s)
        except Exception:
            return logging.INFO

    s = _LEVEL_ALIASES.get(s, s)
    return int(getattr(logging, s, logging.INFO))


def new_run_id() -> str:
    return uuid.uuid4().hex


# -----------------------------------------------------------------------------
# Redaction helpers
# -----------------------------------------------------------------------------

_URL_USERINFO_RE = re.compile(r"(?i)\b([a-z][a-z0-9+.-]*://)([^/@\s]+)@")


def mask_url(u: str) -> str:
    if not u:
        return u
    s = _URL_USERINFO_RE.sub(r"\1***@", str(u))
    if "?" in s:
        s = s.split("?", 1)[0] + "?..."
    return s


_SECRET_VALUES_LOCK = threading.Lock()
_SECRET_VALUES: list[str] = []


def _is_sensitive_env_key(k: str) -> bool:
    kk = (k or "").strip().lower()
    if not kk:
        return False
    needles = (
        "password",
        "passwd",
        "pwd",
        "secret",
        "token",
        "apikey",
        "api_key",
        "auth",
        "private",
        "mnemonic",
        "seed",
        "dsn",
        "database_url",
        "redis_url",
        "rpc_url",
        "openai",
        "telegram",
        "discord",
    )
    return any(n in kk for n in needles)


def _collect_secret_values(redaction_enabled: bool) -> list[str]:
    if not redaction_enabled:
        return []
    values: set[str] = set()
    try:
        for k, v in os.environ.items():
            if not v:
                continue
            if _is_sensitive_env_key(k):
                vv = str(v).strip().strip('"').strip("'")
                if 8 <= len(vv) <= 8192:
                    values.add(vv)
    except Exception:
        pass
    out = sorted(values, key=lambda s: (-len(s), s))
    return out[:300]


def _refresh_secret_values(redaction_enabled: bool) -> None:
    global _SECRET_VALUES
    with _SECRET_VALUES_LOCK:
        _SECRET_VALUES = _collect_secret_values(bool(redaction_enabled))


def _sanitize_text(text: Any) -> str:
    try:
        s = "" if text is None else str(text)
        if _DEFAULTS.redact_enabled:
            with _SECRET_VALUES_LOCK:
                secrets = list(_SECRET_VALUES)
            for sec in secrets:
                if sec and sec in s:
                    s = s.replace(sec, "***")
        return redact_text(s)
    except Exception:
        return MASK


def _truncate_str(s: str) -> str:
    vmax = int(_DEFAULTS.value_max_chars)
    if vmax == 0:
        return s
    if len(s) <= vmax:
        return s
    head = max(0, (vmax // 2) - 32)
    tail = max(0, vmax - head - 64)
    if tail > 0:
        return f"{s[:head]}…<{len(s)} chars>…{s[-tail:]}"
    return f"{s[:vmax]}…<{len(s)} chars>"


def _cap_text(s: str, max_chars: int) -> str:
    if max_chars <= 0:
        return s
    if len(s) <= max_chars:
        return s
    return s[-max_chars:]


def _is_sensitive_key_name(key: str) -> bool:
    k = (key or "").strip().lower().replace("-", "_")
    if not k:
        return False
    if any(frag in k for frag in _DEFAULTS.redact_key_fragments):
        return True
    if k.endswith("_token") or k.endswith("_secret") or k.endswith("_key"):
        return True
    return False


def redact_value(key: str, value: Any) -> Any:
    try:
        k = str(key or "")
        if _DEFAULTS.redact_enabled and _is_sensitive_key_name(k):
            return "***"

        if isinstance(value, str):
            if "://" in value and "@" in value:
                return _truncate_str(_sanitize_text(mask_url(value)))
            return _truncate_str(_sanitize_text(value))

        return redact_obj(value)
    except Exception:
        return "***"


def _format_exc(exc_info: Any, max_chars: int) -> str:
    try:
        s = "".join(traceback.format_exception(*exc_info))
        s = _sanitize_text(s)
        return _cap_text(s, max_chars)
    except Exception:
        return "<exc_format_failed>"


def _sanitize_extra(extra: Mapping[str, Any] | None) -> dict[str, Any]:
    if not extra:
        return {}
    out: dict[str, Any] = {}
    for k, v in extra.items():
        ks = str(k)
        nk = f"x_{ks}" if ks in _RESERVED_LOGRECORD_KEYS else ks
        if nk in out:
            i = 2
            base = nk
            while f"{base}_{i}" in out:
                i += 1
            nk = f"{base}_{i}"
        out[nk] = redact_value(nk, v)
    return out


# -----------------------------------------------------------------------------
# Context management
# -----------------------------------------------------------------------------


def get_log_context() -> dict[str, Any]:
    ctx = _LOG_CONTEXT.get()
    return dict(ctx) if isinstance(ctx, dict) else {}


def set_log_context(**kwargs: Any) -> None:
    base = get_log_context()
    for k, v in kwargs.items():
        if v is None:
            base.pop(k, None)
        else:
            base[k] = v
    _LOG_CONTEXT.set(base if base else None)


def clear_log_context(*keys: str) -> None:
    if not keys:
        _LOG_CONTEXT.set(None)
        return
    base = get_log_context()
    for k in keys:
        base.pop(k, None)
    _LOG_CONTEXT.set(base if base else None)


@contextmanager
def with_log_context(**kwargs: Any) -> Iterator[None]:
    token: Token[dict[str, Any] | None] | None = None
    try:
        merged = get_log_context()
        for k, v in kwargs.items():
            if v is None:
                merged.pop(k, None)
            else:
                merged[k] = v
        token = _LOG_CONTEXT.set(merged if merged else None)
        yield
    finally:
        if token is not None:
            with suppress(Exception):
                _LOG_CONTEXT.reset(token)


# -----------------------------------------------------------------------------
# Runtime meta & filter
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class _RuntimeMeta:
    service: str
    version: str
    env: str
    host: str
    pid: int


_RUNTIME = _RuntimeMeta(
    service=_DEFAULTS.service,
    version=_DEFAULTS.version,
    env=_DEFAULTS.env,
    host=socket.gethostname(),
    pid=os.getpid(),
)


def _current_task_name() -> str | None:
    try:
        import asyncio  # noqa: PLC0415

        t = asyncio.current_task()
        if t is None:
            return None
        return t.get_name()
    except Exception:
        return None


class ContextFilter(logging.Filter):
    def __init__(self, *, include_src: bool, include_process: bool, include_host: bool) -> None:
        super().__init__()
        self.include_src = bool(include_src)
        self.include_process = bool(include_process)
        self.include_host = bool(include_host)

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        record.service = _RUNTIME.service
        record.version = _RUNTIME.version
        record.env = _RUNTIME.env

        if self.include_host:
            record.host = _RUNTIME.host
        if self.include_process:
            record.pid = _RUNTIME.pid
            record.thread_name = threading.current_thread().name
            record.taskName = _current_task_name()

        record._include_src = bool(self.include_src)

        ctx = get_log_context()
        for k, v in ctx.items():
            ks = str(k)
            if ks.startswith("_"):
                continue
            try:
                setattr(record, ks, redact_value(ks, v))
            except Exception:
                continue
        return True


# -----------------------------------------------------------------------------
# Formatters
# -----------------------------------------------------------------------------


class JsonFormatter(logging.Formatter):
    def __init__(self, *, use_utc: bool) -> None:
        super().__init__()
        self.use_utc = bool(use_utc)

    def format(self, record: logging.LogRecord) -> str:  # noqa: A003
        created = float(getattr(record, "created", time.time()))
        ts_sec = int(created)
        ms = int((created - ts_sec) * 1000)
        if ms >= 1000:
            ts_sec += 1
            ms -= 1000
        if ms < 0:
            ms = 0
        ts_ms = (ts_sec * 1000) + ms

        if self.use_utc:
            dt = datetime.fromtimestamp(created, tz=UTC)
            iso = dt.strftime("%Y-%m-%dT%H:%M:%S") + f".{ms:03d}Z"
        else:
            dt = datetime.fromtimestamp(created).astimezone()
            iso = dt.isoformat(timespec="milliseconds")

        try:
            raw_msg = record.getMessage()
        except Exception:
            raw_msg = "<log_message_format_failed>"
        msg = _truncate_str(_sanitize_text(raw_msg))

        payload: dict[str, Any] = {
            "ts": ts_sec,
            "ts_ms": ts_ms,
            "time": iso,
            "level": record.levelname,
            "logger": record.name,
            "msg": msg,
            "service": getattr(record, "service", _DEFAULTS.service),
            "version": getattr(record, "version", _DEFAULTS.version),
            "env": getattr(record, "env", _DEFAULTS.env),
        }

        if hasattr(record, "host"):
            payload["host"] = getattr(record, "host", "")
        if hasattr(record, "pid"):
            payload["pid"] = getattr(record, "pid", os.getpid())
        if hasattr(record, "thread_name"):
            payload["thread"] = getattr(record, "thread_name", "")
        if hasattr(record, "taskName"):
            tn = getattr(record, "taskName", None)
            if tn:
                payload["taskName"] = tn

        ev = getattr(record, "event", None)
        if ev not in (None, "", False):
            payload["event"] = ev

        if bool(getattr(record, "_include_src", False)):
            payload.update(
                {
                    "file": getattr(record, "filename", ""),
                    "line": getattr(record, "lineno", 0),
                    "func": getattr(record, "funcName", ""),
                    "module": getattr(record, "module", ""),
                }
            )

        fields_obj = getattr(record, "fields", None)
        if isinstance(fields_obj, dict):
            for k, v in fields_obj.items():
                ks = str(k)
                if ks in payload or ks.startswith("_"):
                    continue
                payload[ks] = redact_value(ks, v)

        for k, v in record.__dict__.items():
            if k in _RESERVED_LOGRECORD_KEYS or k.startswith("_") or k in payload:
                continue
            payload[str(k)] = redact_value(str(k), v)

        if record.exc_info:
            payload["exc_type"] = getattr(record.exc_info[0], "__name__", "Exception")
            payload["exc"] = _format_exc(record.exc_info, _DEFAULTS.exc_max_chars)
        elif record.stack_info:
            payload["stack"] = _cap_text(
                _sanitize_text(record.stack_info), _DEFAULTS.stack_max_chars
            )

        try:
            return json.dumps(payload, separators=(",", ":"), ensure_ascii=False, default=str)
        except Exception:
            fallback = {
                "ts": ts_sec,
                "ts_ms": ts_ms,
                "time": iso,
                "level": record.levelname,
                "logger": record.name,
                "msg": msg,
                "event": "LOG_JSON_DUMP_FAILED",
            }
            return json.dumps(fallback, separators=(",", ":"), ensure_ascii=False)


class TextFormatter(logging.Formatter):
    def __init__(self, *, use_utc: bool) -> None:
        super().__init__(fmt="%(asctime)s %(levelname)s %(name)s: %(message)s")
        self.use_utc = bool(use_utc)

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:  # noqa: N802
        _ = datefmt
        if self.use_utc:
            dt = datetime.fromtimestamp(record.created, tz=UTC)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        dt = datetime.fromtimestamp(record.created).astimezone()
        return dt.isoformat(timespec="seconds")

    def format(self, record: logging.LogRecord) -> str:  # noqa: A003
        try:
            raw_msg = record.getMessage()
        except Exception:
            raw_msg = "<log_message_format_failed>"

        record.message = _truncate_str(_sanitize_text(raw_msg))
        if self.usesTime():
            record.asctime = self.formatTime(record, self.datefmt)

        s = self.formatMessage(record)

        if record.exc_info:
            exc_txt = _format_exc(record.exc_info, _DEFAULTS.exc_max_chars)
            if s and not s.endswith("\n"):
                s += "\n"
            s += exc_txt

        if record.stack_info:
            stack = _cap_text(_sanitize_text(record.stack_info), _DEFAULTS.stack_max_chars)
            if s and not s.endswith("\n"):
                s += "\n"
            s += stack

        return s


# -----------------------------------------------------------------------------
# Spam guards + extensions: logger.every / logger.event / logger.once / logger.bind
# -----------------------------------------------------------------------------


@dataclass
class _EveryState:
    last_ts: float
    suppressed: int


_EVERY_LOCK = threading.Lock()
_EVERY: dict[tuple[str, str], _EveryState] = {}
_EVERY_MAX_KEYS = 50_000

_ONCE_LOCK = threading.Lock()
_ONCE: dict[tuple[str, str], float] = {}


def _every_key(logger: logging.Logger, name: str) -> tuple[str, str]:
    return (logger.name, name)


def _maybe_prune_every(now: float) -> None:
    if len(_EVERY) <= _EVERY_MAX_KEYS:
        return
    items = list(_EVERY.items())
    items.sort(key=lambda kv: kv[1].last_ts)
    drop_n = max(1, int(len(items) * 0.2))
    for i in range(drop_n):
        _EVERY.pop(items[i][0], None)


def _log_every(
    logger: logging.Logger,
    name: str,
    *,
    every_sec: float,
    level: int,
    msg: str | None,
    fields: Mapping[str, Any],
) -> bool:
    now = time.time()
    key = _every_key(logger, name)
    emit = False
    suppressed_count = 0

    with _EVERY_LOCK:
        _maybe_prune_every(now)
        st = _EVERY.get(key)
        if st is None:
            _EVERY[key] = _EveryState(last_ts=now, suppressed=0)
            emit = True
        else:
            if now - st.last_ts >= every_sec:
                suppressed_count = st.suppressed
                st.last_ts = now
                st.suppressed = 0
                emit = True
            else:
                st.suppressed += 1
                emit = False

    if not emit:
        return False

    extra: dict[str, Any] = {"event": name}
    extra.update(fields)
    if suppressed_count:
        extra["suppressed"] = suppressed_count

    text = msg if msg is not None else name
    logger.log(level, text, extra=_sanitize_extra(extra), stacklevel=3)
    return True


def _logger_every(
    self: logging.Logger,  # noqa: ANN001
    name: str,
    *args: Any,
    every_sec: float | None = None,
    level: str | int = logging.INFO,
    msg: str | None = None,
    **fields: Any,
) -> bool:
    if args:
        if len(args) >= 1 and every_sec is None:
            every_sec = args[0]
        if len(args) >= 2 and level == logging.INFO:
            level = args[1]
        if len(args) > 2:
            raise TypeError("every() accepts at most 2 positional args: (name, every_sec[, level])")

    sec = float(every_sec) if every_sec is not None else float(_DEFAULTS.every_default_sec)
    if sec <= 0:
        sec = 0.001

    return _log_every(
        self,
        name,
        every_sec=sec,
        level=_parse_level(level),
        msg=msg,
        fields=fields,
    )


def _logger_event(
    self: logging.Logger,  # noqa: ANN001
    name: str,
    /,
    msg: str | None = None,
    level: str | int = logging.INFO,
    **fields: Any,
) -> None:
    extra: dict[str, Any] = {"event": name}
    extra.update(fields)
    text = msg if msg is not None else name
    self.log(_parse_level(level), text, extra=_sanitize_extra(extra), stacklevel=3)


def _logger_once(
    self: logging.Logger,  # noqa: ANN001
    name: str,
    /,
    msg: str | None = None,
    level: str | int = logging.INFO,
    **fields: Any,
) -> bool:
    now = time.time()
    key = (self.name, name)

    with _ONCE_LOCK:
        last = _ONCE.get(key)
        if last is not None:
            if _DEFAULTS.dedupe_window_sec <= 0:
                return False
            if now - last < _DEFAULTS.dedupe_window_sec:
                return False
        _ONCE[key] = now

    _logger_event(self, name, msg=msg, level=level, **fields)
    return True


def _logger_bind(self: logging.Logger, /, **fields: Any) -> "BoundLogger":  # noqa: ANN001
    return BoundLogger(cast(EventLogger, self), fields)


def _install_logger_extras() -> None:
    lg_cls = cast(Any, logging.Logger)
    if not callable(getattr(logging.Logger, "every", None)):
        lg_cls.every = _logger_every
    if not callable(getattr(logging.Logger, "event", None)):
        lg_cls.event = _logger_event
    if not callable(getattr(logging.Logger, "once", None)):
        lg_cls.once = _logger_once
    if not callable(getattr(logging.Logger, "bind", None)):
        lg_cls.bind = _logger_bind


# -----------------------------------------------------------------------------
# Bound logger (structured extras)
# -----------------------------------------------------------------------------


class BoundLogger:
    def __init__(self, logger: EventLogger, bound: Mapping[str, Any]) -> None:
        self._logger = logger
        self._bound = dict(bound)

    @property
    def logger(self) -> EventLogger:
        return self._logger

    def _merge_fields(self, fields: Mapping[str, Any] | None) -> dict[str, Any]:
        out = dict(self._bound)
        if fields:
            out.update(fields)
        return out

    def _merge_extra(self, extra: Mapping[str, Any] | None) -> dict[str, Any]:
        return _sanitize_extra(self._merge_fields(extra))

    def log(
        self,
        level: int,
        msg: str,
        *args: Any,
        extra: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self._logger.log(level, msg, *args, extra=self._merge_extra(extra), **kwargs)

    def isEnabledFor(self, level: int) -> bool:  # noqa: N802
        return self._logger.isEnabledFor(level)

    def debug(
        self, msg: str, *args: Any, extra: Mapping[str, Any] | None = None, **kwargs: Any
    ) -> None:
        self._logger.debug(msg, *args, extra=self._merge_extra(extra), **kwargs)

    def info(
        self, msg: str, *args: Any, extra: Mapping[str, Any] | None = None, **kwargs: Any
    ) -> None:
        self._logger.info(msg, *args, extra=self._merge_extra(extra), **kwargs)

    def warning(
        self, msg: str, *args: Any, extra: Mapping[str, Any] | None = None, **kwargs: Any
    ) -> None:
        self._logger.warning(msg, *args, extra=self._merge_extra(extra), **kwargs)

    def error(
        self, msg: str, *args: Any, extra: Mapping[str, Any] | None = None, **kwargs: Any
    ) -> None:
        self._logger.error(msg, *args, extra=self._merge_extra(extra), **kwargs)

    def exception(
        self, msg: str, *args: Any, extra: Mapping[str, Any] | None = None, **kwargs: Any
    ) -> None:
        self._logger.exception(msg, *args, extra=self._merge_extra(extra), **kwargs)

    def critical(
        self, msg: str, *args: Any, extra: Mapping[str, Any] | None = None, **kwargs: Any
    ) -> None:
        self._logger.critical(msg, *args, extra=self._merge_extra(extra), **kwargs)

    def event(
        self, name: str, /, msg: str | None = None, level: str | int = logging.INFO, **fields: Any
    ) -> None:
        merged = self._merge_fields(fields)
        self._logger.event(name, msg=msg, level=level, **merged)

    def every(
        self,
        name: str,
        *args: Any,
        every_sec: float | None = None,
        level: str | int = logging.INFO,
        msg: str | None = None,
        **fields: Any,
    ) -> bool:
        merged = self._merge_fields(fields)
        return bool(
            self._logger.every(name, *args, every_sec=every_sec, level=level, msg=msg, **merged)
        )

    def once(
        self, name: str, /, msg: str | None = None, level: str | int = logging.INFO, **fields: Any
    ) -> bool:
        merged = self._merge_fields(fields)
        return bool(self._logger.once(name, msg=msg, level=level, **merged))

    def bind(self, /, **fields: Any) -> "BoundLogger":
        merged = dict(self._bound)
        merged.update(fields)
        return BoundLogger(self._logger, merged)

    def __getattr__(self, item: str) -> Any:
        return getattr(self._logger, item)


def bind(logger: str | EventLogger | None = None, **kwargs: Any) -> BoundLogger:
    if logger is None:
        lg = get_logger()
    elif isinstance(logger, str):
        lg = get_logger(logger)
    else:
        lg = logger
    return BoundLogger(lg, kwargs)


def event(logger: EventLogger | BoundLogger | None, name: str, **fields: Any) -> None:
    lg = logger or get_logger()
    if isinstance(lg, BoundLogger):
        lg.event(name, **fields)
    else:
        lg.event(name, **fields)


# -----------------------------------------------------------------------------
# bump() / metrics
# -----------------------------------------------------------------------------


def bump(name: str, by: int = 1, **tags: Any) -> None:
    if not name:
        return
    inc = int(by) if by else 1
    with _METRICS_LOCK:
        _METRICS[name] = _METRICS.get(name, 0) + inc

    with suppress(Exception):
        if tags:
            get_logger("metrics").debug("bump %s +%s tags=%s", name, inc, safe_repr(tags))


def metrics_snapshot() -> dict[str, int]:
    with _METRICS_LOCK:
        return dict(_METRICS)


def reset_metrics() -> None:
    with _METRICS_LOCK:
        _METRICS.clear()


# -----------------------------------------------------------------------------
# Configuration (idempotent + thread-safe)
# -----------------------------------------------------------------------------

_CONFIG_LOCK = threading.Lock()
_CONFIGURED = False

_OWNED_HANDLERS: "weakref.WeakSet[logging.Handler]" = weakref.WeakSet()


def _running_under_pytest() -> bool:
    return bool(os.getenv("PYTEST_CURRENT_TEST"))


def _choose_stream(stream_name: str) -> Any:
    if _running_under_pytest():
        return sys.stdout if stream_name == "stdout" else sys.stderr
    return _BASE_STDOUT if stream_name == "stdout" else _BASE_STDERR


def _handler_set_formatter(handler: logging.Handler, *, json_logs: bool, use_utc: bool) -> None:
    handler.setFormatter(
        JsonFormatter(use_utc=use_utc) if json_logs else TextFormatter(use_utc=use_utc)
    )


def _ensure_filters(
    handler: logging.Handler, *, include_src: bool, include_process: bool, include_host: bool
) -> None:
    has_ctx = any(isinstance(f, ContextFilter) for f in getattr(handler, "filters", []))
    if not has_ctx:
        with suppress(Exception):
            handler.addFilter(
                ContextFilter(
                    include_src=bool(include_src),
                    include_process=bool(include_process),
                    include_host=bool(include_host),
                )
            )
    else:
        for f in list(getattr(handler, "filters", [])):
            if isinstance(f, ContextFilter):
                f.include_src = bool(include_src)
                f.include_process = bool(include_process)
                f.include_host = bool(include_host)

    has_redact = any(isinstance(f, RedactionFilter) for f in getattr(handler, "filters", []))
    if not has_redact:
        with suppress(Exception):
            handler.addFilter(RedactionFilter())


def _is_owned_handler(h: logging.Handler) -> bool:
    try:
        return h in _OWNED_HANDLERS
    except Exception:
        return False


def _mark_owned_handler(h: logging.Handler) -> None:
    with suppress(Exception):
        _OWNED_HANDLERS.add(h)


def _set_noisy_lib_levels(noisy_level: int) -> None:
    for name in (
        "asyncio",
        "urllib3",
        "httpx",
        "httpcore",
        "websockets",
        "aiohttp",
        "solana",
        "solders",
        "asyncpg",
    ):
        logging.getLogger(name).setLevel(noisy_level)


def configure_logging(
    *,
    level: str | int | None = None,
    json_logs: bool | None = None,
    use_utc: bool | None = None,
    include_src: bool | None = None,
    include_process: bool | None = None,
    include_host: bool | None = None,
    stream: str | None = None,
    service: str | None = None,
    version: str | None = None,
    env: str | None = None,
    force: bool = False,
    capture_warnings: bool = True,
) -> None:
    global _CONFIGURED, _DEFAULTS, _RUNTIME

    _install_logger_extras()

    with _CONFIG_LOCK:
        _DEFAULTS = _read_env_defaults()
        _refresh_secret_values(_DEFAULTS.redact_enabled)

        if stream is not None:
            st = str(stream).strip().lower()
            if st in ("stderr", "stdout"):
                _DEFAULTS = _LogDefaults(**{**_DEFAULTS.__dict__, "stream": st})

        if json_logs is None:
            json_logs = _DEFAULTS.json_logs
        if use_utc is None:
            use_utc = _DEFAULTS.use_utc
        if include_src is None:
            include_src = _DEFAULTS.include_src
        if include_process is None:
            include_process = _DEFAULTS.include_process
        if include_host is None:
            include_host = _DEFAULTS.include_host

        lvl = _parse_level(level if level is not None else _DEFAULTS.level)

        _RUNTIME = _RuntimeMeta(
            service=(service or _DEFAULTS.service or _RUNTIME.service).strip() or "sniper",
            version=(version or _DEFAULTS.version or _RUNTIME.version).strip() or "v1",
            env=(env or _DEFAULTS.env or _RUNTIME.env).strip() or "dev",
            host=socket.gethostname(),
            pid=os.getpid(),
        )

        root = logging.getLogger()
        root.setLevel(lvl)

        # Pytest-safe: only remove handlers we own (even if force=True)
        if force:
            for h in list(root.handlers):
                if _is_owned_handler(h):
                    with suppress(Exception):
                        root.removeHandler(h)

        owned = [h for h in list(root.handlers) if _is_owned_handler(h)]

        if not owned:
            handler = logging.StreamHandler(stream=_choose_stream(_DEFAULTS.stream))
            _mark_owned_handler(handler)
            handler.setLevel(lvl)
            _handler_set_formatter(handler, json_logs=bool(json_logs), use_utc=bool(use_utc))
            _ensure_filters(
                handler,
                include_src=bool(include_src),
                include_process=bool(include_process),
                include_host=bool(include_host),
            )
            root.addHandler(handler)
            owned = [handler]

        for h in owned:
            with suppress(Exception):
                h.setLevel(lvl)
            with suppress(Exception):
                if isinstance(h, logging.StreamHandler):
                    h.stream = _choose_stream(_DEFAULTS.stream)
            with suppress(Exception):
                _handler_set_formatter(h, json_logs=bool(json_logs), use_utc=bool(use_utc))
            _ensure_filters(
                h,
                include_src=bool(include_src),
                include_process=bool(include_process),
                include_host=bool(include_host),
            )

        logging.captureWarnings(bool(capture_warnings))

        noisy_level = _parse_level(_DEFAULTS.noisy_min_level)
        _set_noisy_lib_levels(noisy_level)

        _CONFIGURED = True


def setup_logging(**kwargs: Any) -> None:
    configure_logging(**kwargs)


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
    include_process: bool | None = None,
    include_host: bool | None = None,
    stream: str | None = None,
    force: bool = False,
) -> EventLogger:
    _install_logger_extras()

    needs_config = (not _CONFIGURED) or any(
        x is not None
        for x in (
            service,
            version,
            env,
            level,
            json_logs,
            use_utc,
            include_src,
            include_process,
            include_host,
            stream,
        )
    )

    if needs_config:
        with suppress(Exception):
            configure_logging(
                level=level,
                json_logs=json_logs,
                use_utc=use_utc,
                include_src=include_src,
                include_process=include_process,
                include_host=include_host,
                stream=stream,
                service=service,
                version=version,
                env=env,
                force=force,
            )

    target = name if name else (service or _DEFAULTS.service or "sniper")
    return cast(EventLogger, logging.getLogger(target))


def get_event_logger(name: str | None = None, **kwargs: Any) -> EventLogger:
    return get_logger(name, **kwargs)


# -----------------------------------------------------------------------------
# StdIO hook (hook_stdio / unhook_stdio)
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class _StdIoHookState:
    orig_stdout: Any
    orig_stderr: Any


_STDIO_LOCK = threading.Lock()
_STDIO_STATE: _StdIoHookState | None = None


class _StreamToLogger(io.TextIOBase):
    encoding: str

    def __init__(self, logger: EventLogger, level: int, tee: Any) -> None:
        super().__init__()
        self._logger = logger
        self._level = int(level)
        self._tee = tee
        self._local = threading.local()
        enc = getattr(self._tee, "encoding", None)
        self.encoding = str(enc) if enc else "utf-8"

    def writable(self) -> bool:  # pragma: no cover
        return True

    def _buf(self) -> str:
        return cast(str, getattr(self._local, "buf", ""))

    def _set_buf(self, s: str) -> None:
        self._local.buf = s

    def _emit_line(self, line: str) -> None:
        line = line.rstrip("\r\n")
        if not line:
            return
        self._logger.log(
            self._level, _sanitize_text(line), extra=_sanitize_extra({"event": "STDIO"})
        )

    def write(self, s: str) -> int:
        if not s:
            return 0

        # Tee to original stream (best local UX)
        with suppress(Exception):
            self._tee.write(s)

        if getattr(self._local, "in_write", False):
            return len(str(s))

        self._local.in_write = True
        try:
            buf = self._buf() + str(s)
            while "\n" in buf:
                line, buf = buf.split("\n", 1)
                if line:
                    with suppress(Exception):
                        self._emit_line(line)
            self._set_buf(buf)
            return len(str(s))
        finally:
            self._local.in_write = False

    def flush(self) -> None:
        buf = self._buf()
        if buf:
            self._set_buf("")
            with suppress(Exception):
                self._emit_line(buf)
        with suppress(Exception):
            if hasattr(self._tee, "flush"):
                self._tee.flush()

    def isatty(self) -> bool:  # pragma: no cover
        with suppress(Exception):
            f = getattr(self._tee, "isatty", None)
            return bool(f() if callable(f) else False)
        return False


def hook_stdio(
    *,
    stdout_level: int | str = logging.INFO,
    stderr_level: int | str = logging.ERROR,
    stdout_logger: str = "stdio.stdout",
    stderr_logger: str = "stdio.stderr",
) -> None:
    global _STDIO_STATE
    with _STDIO_LOCK:
        if _STDIO_STATE is not None:
            return

        with suppress(Exception):
            configure_logging()

        orig_out = sys.stdout if sys.stdout is not None else _BASE_STDOUT
        orig_err = sys.stderr if sys.stderr is not None else _BASE_STDERR

        sys.stdout = _StreamToLogger(
            get_logger(stdout_logger), _parse_level(stdout_level), tee=orig_out
        )
        sys.stderr = _StreamToLogger(
            get_logger(stderr_logger), _parse_level(stderr_level), tee=orig_err
        )

        _STDIO_STATE = _StdIoHookState(orig_stdout=orig_out, orig_stderr=orig_err)
        with suppress(Exception):
            atexit.register(unhook_stdio)


def unhook_stdio() -> None:
    global _STDIO_STATE
    with _STDIO_LOCK:
        if _STDIO_STATE is None:
            return
        sys.stdout = _STDIO_STATE.orig_stdout
        sys.stderr = _STDIO_STATE.orig_stderr
        _STDIO_STATE = None


# -----------------------------------------------------------------------------
# Excepthook (process + threading)
# -----------------------------------------------------------------------------

_EXC_LOCK = threading.Lock()
_OLD_EXCEPTHOOK: Any | None = None
_OLD_THREAD_EXCEPTHOOK: Any | None = None
_EXC_INSTALLED = False


def install_excepthook(logger: EventLogger | None = None) -> None:
    global _OLD_EXCEPTHOOK, _OLD_THREAD_EXCEPTHOOK, _EXC_INSTALLED

    with _EXC_LOCK:
        if _EXC_INSTALLED:
            return

        lg = logger or get_logger("excepthook")
        _OLD_EXCEPTHOOK = sys.excepthook

        def _hook(
            exc_type: type[BaseException], exc: BaseException, tb: TracebackType | None
        ) -> None:
            if exc_type in (SystemExit, KeyboardInterrupt):
                if _OLD_EXCEPTHOOK is not None:
                    _OLD_EXCEPTHOOK(exc_type, exc, tb)
                return
            try:
                lg.critical(
                    "UNCAUGHT_EXCEPTION",
                    exc_info=(exc_type, exc, tb),
                    extra=_sanitize_extra({"event": "UNCAUGHT_EXCEPTION"}),
                )
            except Exception:
                with suppress(Exception):
                    _BASE_STDERR.write("UNCAUGHT_EXCEPTION (logging failed)\n")
                    _BASE_STDERR.write("".join(traceback.format_exception(exc_type, exc, tb)))
                    _BASE_STDERR.flush()
            finally:
                if _OLD_EXCEPTHOOK is not None:
                    with suppress(Exception):
                        _OLD_EXCEPTHOOK(exc_type, exc, tb)

        sys.excepthook = _hook

        if hasattr(threading, "excepthook"):
            try:
                _OLD_THREAD_EXCEPTHOOK = threading.excepthook

                def _thread_hook(args: Any) -> None:
                    try:
                        et_obj = getattr(args, "exc_type", Exception)
                        ev_obj = getattr(args, "exc_value", None)
                        tb_obj = getattr(args, "exc_traceback", None)

                        et: type[BaseException] = et_obj if isinstance(et_obj, type) else Exception
                        ev: BaseException = (
                            ev_obj
                            if isinstance(ev_obj, BaseException)
                            else Exception(
                                str(ev_obj) if ev_obj is not None else "thread exception"
                            )
                        )
                        tb2: TracebackType | None = (
                            tb_obj if isinstance(tb_obj, TracebackType) else None
                        )

                        thr = getattr(args, "thread", None)
                        tname = getattr(thr, "name", None)

                        lg.critical(
                            "THREAD_UNCAUGHT_EXCEPTION",
                            exc_info=(et, ev, tb2),
                            extra=_sanitize_extra(
                                {"event": "THREAD_UNCAUGHT_EXCEPTION", "thread": tname}
                            ),
                        )
                    except Exception:
                        pass
                    finally:
                        if _OLD_THREAD_EXCEPTHOOK is not None:
                            with suppress(Exception):
                                _OLD_THREAD_EXCEPTHOOK(args)

                threading.excepthook = _thread_hook  # type: ignore[attr-defined]
            except Exception:
                _OLD_THREAD_EXCEPTHOOK = None

        _EXC_INSTALLED = True
        with suppress(Exception):
            atexit.register(uninstall_excepthook)


def uninstall_excepthook() -> None:
    global _OLD_EXCEPTHOOK, _OLD_THREAD_EXCEPTHOOK, _EXC_INSTALLED

    with _EXC_LOCK:
        if not _EXC_INSTALLED:
            return

        if _OLD_EXCEPTHOOK is not None:
            sys.excepthook = _OLD_EXCEPTHOOK
        _OLD_EXCEPTHOOK = None

        if _OLD_THREAD_EXCEPTHOOK is not None and hasattr(threading, "excepthook"):
            with suppress(Exception):
                threading.excepthook = _OLD_THREAD_EXCEPTHOOK  # type: ignore[attr-defined]
        _OLD_THREAD_EXCEPTHOOK = None

        _EXC_INSTALLED = False


# -----------------------------------------------------------------------------
# Global heartbeat
# -----------------------------------------------------------------------------

_HEARTBEAT_LOCK = threading.Lock()
_HEARTBEAT_STOP: threading.Event | None = None
_HEARTBEAT_THREAD: threading.Thread | None = None


def start_global_heartbeat(
    *,
    interval_sec: float = 30.0,
    logger_name: str = "heartbeat",
    event_name: str = "HEARTBEAT",
    enabled: bool = True,
    include_metrics: bool = True,
    include_runtime: bool = True,
) -> bool:
    global _HEARTBEAT_STOP, _HEARTBEAT_THREAD

    try:
        interval = float(interval_sec)
    except Exception:
        interval = 30.0

    if (not enabled) or interval <= 0:
        return False

    with _HEARTBEAT_LOCK:
        if _HEARTBEAT_THREAD is not None and _HEARTBEAT_THREAD.is_alive():
            return False

        stop_ev = threading.Event()
        _HEARTBEAT_STOP = stop_ev
        lg = get_logger(logger_name)

        def _loop() -> None:
            with suppress(Exception):
                configure_logging()

            while not stop_ev.is_set():
                extra: dict[str, Any] = {"event": event_name}

                if include_runtime:
                    extra.update(
                        {
                            "runtime_service": _RUNTIME.service,
                            "runtime_version": _RUNTIME.version,
                            "runtime_env": _RUNTIME.env,
                        }
                    )
                    if _DEFAULTS.include_host:
                        extra["runtime_host"] = _RUNTIME.host
                    if _DEFAULTS.include_process:
                        extra["runtime_pid"] = _RUNTIME.pid

                if include_metrics:
                    snap = metrics_snapshot()
                    extra["metrics_n"] = len(snap)
                    if snap:
                        keys = sorted(snap.keys())
                        top_keys = keys[:25]
                        extra["metrics_top"] = {k: snap.get(k, 0) for k in top_keys}

                with suppress(Exception):
                    lg.info(event_name, extra=_sanitize_extra(extra))

                stop_ev.wait(interval)

        t = threading.Thread(target=_loop, name="global-heartbeat", daemon=True)
        _HEARTBEAT_THREAD = t
        t.start()

        with suppress(Exception):
            atexit.register(stop_global_heartbeat)

        return True


def stop_global_heartbeat(*, wait: float = 1.0) -> None:
    global _HEARTBEAT_STOP, _HEARTBEAT_THREAD

    with _HEARTBEAT_LOCK:
        stop_ev = _HEARTBEAT_STOP
        thr = _HEARTBEAT_THREAD
        _HEARTBEAT_STOP = None
        _HEARTBEAT_THREAD = None

    if stop_ev is not None:
        with suppress(Exception):
            stop_ev.set()

    if thr is not None:
        with suppress(Exception):
            thr.join(timeout=float(wait))


# -----------------------------------------------------------------------------
# Test reset helper
# -----------------------------------------------------------------------------


def reset_logging_state() -> None:
    """Hard reset for tests (pytest isolation / contract tests)."""
    global _CONFIGURED, _DEFAULTS, _RUNTIME

    with suppress(Exception):
        stop_global_heartbeat()
    with suppress(Exception):
        unhook_stdio()
    with suppress(Exception):
        uninstall_excepthook()

    with suppress(Exception):
        root = logging.getLogger()
        for h in list(root.handlers):
            if _is_owned_handler(h):
                with suppress(Exception):
                    root.removeHandler(h)

    with suppress(Exception):
        logging.captureWarnings(False)

    with suppress(Exception):
        with _EVERY_LOCK:
            _EVERY.clear()

    with suppress(Exception):
        with _ONCE_LOCK:
            _ONCE.clear()

    with suppress(Exception):
        reset_metrics()

    _DEFAULTS = _LogDefaults(
        service="sniper",
        version="v1",
        env="dev",
        level="INFO",
        json_logs=True,
        use_utc=True,
        include_src=False,
        include_process=True,
        include_host=True,
        stream="stderr",
        exc_max_chars=8000,
        stack_max_chars=8000,
        value_max_chars=4096,
        noisy_min_level="INFO",
        every_default_sec=30.0,
        redact_enabled=True,
        redact_key_fragments=set(_DEFAULT_REDACT_KEY_FRAGMENTS),
        dedupe_window_sec=0.0,
    )
    _RUNTIME = _RuntimeMeta(
        service=_DEFAULTS.service,
        version=_DEFAULTS.version,
        env=_DEFAULTS.env,
        host=socket.gethostname(),
        pid=os.getpid(),
    )
    with suppress(Exception):
        _refresh_secret_values(_DEFAULTS.redact_enabled)
    _CONFIGURED = False
