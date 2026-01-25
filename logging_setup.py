# -*- coding: utf-8 -*-
from __future__ import annotations

"""
logging_setup.py (ROOT — CANONICAL)
==================================

God-Tier Structured Logging for the entire repo.

Design goals
------------
- One truth: ROOT logging_setup.py is canonical.
- JSON by default (Render-friendly), human text optional.
- Idempotent configuration (safe to call multiple times).
- Context propagation via contextvars (trade_id, run_id, session_id, request_id, etc).
- Safe extras: include useful fields, avoid noisy/unsafe dumps.
- Works for asyncio + threads, and plays nice with uvicorn/httpx/aiohttp/etc.
- Spam guards: log.every(...) helpers to avoid log floods.
- Secret redaction end-to-end (msg + extras + nested structures + URLs with creds).
- Import-time safe (no env reads locked in, no network).

Usage
-----
from logging_setup import (
    configure_logging, get_logger, set_log_context, event, bind,
    hook_stdio, unhook_stdio, install_excepthook,
)

configure_logging(service="sniper", version="v1")
install_excepthook()
hook_stdio()

log = get_logger(__name__)
set_log_context(run_id="...", trade_id="...")
log.info("boot ok", extra={"event": "BOOT_OK"})
event(log, "WHY_NOT_TRADING", reason="momo_gate", score=0.12)

Spam guards
-----------
log = get_logger(__name__)
log.every("CANDIDATE_SKIPPED", every_sec=30, reason="gate", score=0.11)  # at most once per 30s
log.every("HTTPX_RETRY", every_sec=10, url="...", status=429)

Back-compat
-----------
Some modules call: if log.every("evt", 1_000_000): ...
So .every(...) accepts positional every_sec and RETURNS bool (True when it emitted).

Env knobs
---------
LOG_LEVEL=INFO|DEBUG|WARNING|ERROR|CRITICAL (WARN/FATAL supported)
LOG_JSON=1|0
LOG_TIME_UTC=1|0
LOG_SERVICE=sniper
LOG_VERSION=v1
LOG_ENV=prod|dev
LOG_INCLUDE_SRC=1|0        (include filename/line/function in json)
LOG_REDACT_KEYS=a,b,c      (extra keys to redact; case-insensitive)
LOG_EXC_MAX_CHARS=8000
LOG_STACK_MAX_CHARS=8000
LOG_VALUE_MAX_CHARS=4096   (max chars for any single string value in extras; 0 disables)
LOG_NOISY_LEVEL=INFO       (level assigned to chatty libs)
LOG_BUMP_DEBUG=1|0         (emit bump debug logs; default 0)
LOG_EVERY_DEFAULT_SEC=30   (default for log.every if not provided)
LOG_HEARTBEAT=1|0
LOG_MASK_LONG_TOKENS=1|0   (default 1; keeps Solana mints visible)
LOG_SECRET_VALUE_REDACTION=1|0 (default 1; masks exact env secret values when they appear)
LOG_FORCE_RESET=1|0        (default 0; hard-resets root handlers at configure time)
"""

import asyncio
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
from collections.abc import Iterator, Mapping
from contextlib import contextmanager, suppress
from contextvars import ContextVar, Token
from dataclasses import dataclass
from datetime import datetime, timezone
from types import TracebackType
from typing import Any

UTC = timezone.utc

# =============================================================================
# Baseline stdio (avoid recursion when sys.stdout is hooked)
# =============================================================================

_BASE_STDOUT = getattr(sys, "__stdout__", None) or sys.stdout
_BASE_STDERR = getattr(sys, "__stderr__", None) or sys.stderr

# =============================================================================
# Contextvars (per-task/per-request metadata)
# =============================================================================

_LOG_CONTEXT: ContextVar[dict[str, Any] | None] = ContextVar("_LOG_CONTEXT", default=None)

# =============================================================================
# Simple in-process metrics (for bump())
# =============================================================================

_METRICS_LOCK = threading.Lock()
_METRICS: dict[str, int] = {}

# =============================================================================
# Defaults (IMPORTANT: read env at CONFIGURE time, not import time)
# =============================================================================


@dataclass(frozen=True)
class _LogDefaults:
    service: str
    version: str
    env: str
    level: str
    json_logs: bool
    use_utc: bool
    include_src: bool
    exc_max_chars: int
    stack_max_chars: int
    value_max_chars: int
    noisy_min_level: str
    every_default_sec: float
    redact_keys: set[str]
    mask_long_tokens: bool
    redact_secret_values: bool
    force_reset: bool


_RESERVED_RECORD_KEYS = {
    # Standard LogRecord fields
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
    "message",
    # formatter fields
    "asctime",
    "thread_name",
    "_include_src",
    # common payload keys
    "service",
    "version",
    "env",
    "host",
    "pid",
    "taskName",
    "event",
    "logger",
    "level",
    "time",
    "ts",
    "ts_ms",
}

_DEFAULT_REDACT_KEYS = {
    "password",
    "pass",
    "passwd",
    "secret",
    "token",
    "api_key",
    "apikey",
    "private_key",
    "auth",
    "authorization",
    "cookie",
    "set-cookie",
    "session",
    "wallet",
    "mnemonic",
    "seed",
    "signature",
    "dsn",
    "database_url",
    "postgres_dsn",
    "pg_dsn",
    "redis_url",
    "rpc_url",
    "rpc",
    "jupiter_api_key",
    "openai_api_key",
    "telegram_token",
    "discord_token",
}

_DEFAULTS: _LogDefaults = _LogDefaults(
    service="sniper",
    version="v1",
    env="dev",
    level="INFO",
    json_logs=True,
    use_utc=True,
    include_src=False,
    exc_max_chars=8000,
    stack_max_chars=8000,
    value_max_chars=4096,
    noisy_min_level="INFO",
    every_default_sec=30.0,
    redact_keys=set(_DEFAULT_REDACT_KEYS),
    mask_long_tokens=True,
    redact_secret_values=True,
    force_reset=False,
)

__all__ = [
    "bind",
    "bump",
    "clear_log_context",
    "configure_logging",
    "event",
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

# =============================================================================
# Env parsing helpers (SAFE: defaults if malformed)
# =============================================================================


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
    env = (os.getenv("LOG_ENV", "") or "").strip()
    if env:
        return env

    for k in ("ENV", "APP_ENV", "ENVIRONMENT", "RUNTIME_ENV"):
        v = (os.getenv(k, "") or "").strip()
        if v:
            return v

    if (os.getenv("RENDER", "") or "").strip():
        return "prod"
    if (os.getenv("RENDER_SERVICE_ID", "") or "").strip():
        return "prod"
    if (os.getenv("RENDER_INSTANCE_ID", "") or "").strip():
        return "prod"

    return "dev"


def _read_env_defaults() -> _LogDefaults:
    service = (os.getenv("LOG_SERVICE", "sniper") or "sniper").strip()
    version = (os.getenv("LOG_VERSION", "v1") or "v1").strip()
    env = _infer_env_name()

    level = (os.getenv("LOG_LEVEL", "INFO") or "INFO").strip().upper()
    json_logs = _env_bool("LOG_JSON", True)
    use_utc = _env_bool("LOG_TIME_UTC", True)
    include_src = _env_bool("LOG_INCLUDE_SRC", False)

    exc_max_chars = _env_int("LOG_EXC_MAX_CHARS", 8000)
    stack_max_chars = _env_int("LOG_STACK_MAX_CHARS", 8000)
    value_max_chars = _env_int("LOG_VALUE_MAX_CHARS", 4096)

    noisy_min_level = (os.getenv("LOG_NOISY_LEVEL", "INFO") or "INFO").strip().upper()
    every_default_sec = _env_float("LOG_EVERY_DEFAULT_SEC", 30.0)

    mask_long_tokens = _env_bool("LOG_MASK_LONG_TOKENS", True)
    redact_secret_values = _env_bool("LOG_SECRET_VALUE_REDACTION", True)
    force_reset = _env_bool("LOG_FORCE_RESET", False)

    extra = os.getenv("LOG_REDACT_KEYS", "") or ""
    redact = set(_DEFAULT_REDACT_KEYS)
    for part in extra.replace(";", ",").split(","):
        p = part.strip().lower()
        if p:
            redact.add(p)

    exc_max_chars = max(512, min(int(exc_max_chars), 200_000))
    stack_max_chars = max(512, min(int(stack_max_chars), 200_000))
    if int(value_max_chars) != 0:
        value_max_chars = max(256, min(int(value_max_chars), 200_000))
    every_default_sec = max(0.001, min(float(every_default_sec), 3600.0))

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
        exc_max_chars=int(exc_max_chars),
        stack_max_chars=int(stack_max_chars),
        value_max_chars=int(value_max_chars),
        noisy_min_level=noisy_min_level,
        every_default_sec=float(every_default_sec),
        redact_keys={k.lower() for k in redact},
        mask_long_tokens=bool(mask_long_tokens),
        redact_secret_values=bool(redact_secret_values),
        force_reset=bool(force_reset),
    )


# =============================================================================
# Utilities
# =============================================================================

_LEVEL_ALIASES: dict[str, str] = {
    "WARN": "WARNING",
    "FATAL": "CRITICAL",
    "ERR": "ERROR",
    "TRACE": "DEBUG",
}


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


# =============================================================================
# Redaction
# =============================================================================

_BASE58_ALPH = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")
_MINT_LEN_MIN = 32
_MINT_LEN_MAX = 44

_URL_CRED_RE = re.compile(r"([a-zA-Z][a-zA-Z0-9+.-]*://)([^@\s/]+@)")
_APIKEY_HDR_RE = re.compile(r"(?i)(x-api-key:\s*)([^\s]+)")
_BEARER_HDR_RE = re.compile(r"(?i)(authorization:\s*bearer\s+)([^\s]+)")
_BEARER_ANY_RE = re.compile(r"(?i)\b(bearer)\s+([A-Za-z0-9\-._~+/]+=*)")
_KV_SECRET_RE = re.compile(
    r"(?i)\b(api[_-]?key|token|secret|password|passwd|pass|authorization)\b(\s*[:=]\s*)([^\s,;]+)"
)
_QP_SECRET_RE = re.compile(r"(?i)(password|pass|token|api_key|apikey|secret)=([^&\s]+)")
_LONG_TOKENISH_RE = re.compile(r"\b[A-Za-z0-9_\-]{28,}\b")


def _looks_like_solana_mint(s: str) -> bool:
    ss = (s or "").strip()
    if not (_MINT_LEN_MIN <= len(ss) <= _MINT_LEN_MAX):
        return False
    return all(ch in _BASE58_ALPH for ch in ss)


def mask_url(u: str) -> str:
    if not u:
        return u
    s = _URL_CRED_RE.sub(r"\1***@", str(u))
    if "?" in s:
        s = s.split("?", 1)[0] + "?..."
    return s


def _is_sensitive_key_name(k: str) -> bool:
    kk = (k or "").upper()
    return any(
        needle in kk
        for needle in (
            "PASSWORD",
            "PASS",
            "SECRET",
            "TOKEN",
            "API_KEY",
            "APIKEY",
            "AUTH",
            "PRIVATE",
            "MNEMONIC",
            "SEED",
            "SIGNATURE",
            "KEYPAIR",
            "WALLET",
            "DATABASE_URL",
            "DSN",
            "REDIS_URL",
            "RPC_URL",
        )
    )


def _collect_secret_values() -> list[str]:
    if not _DEFAULTS.redact_secret_values:
        return []
    values: set[str] = set()
    try:
        for k, v in os.environ.items():
            if not v:
                continue
            if _is_sensitive_key_name(k):
                vv = str(v).strip().strip('"').strip("'")
                if 6 <= len(vv) <= 8192:
                    values.add(vv)
    except Exception:
        pass
    out = sorted(values, key=lambda s: (-len(s), s))
    return out[:300]


_SECRET_VALUES_LOCK = threading.Lock()
_SECRET_VALUES: list[str] = []


def _refresh_secret_values() -> None:
    global _SECRET_VALUES
    with _SECRET_VALUES_LOCK:
        _SECRET_VALUES = _collect_secret_values()


def _mask_tokenish_match(m: re.Match) -> str:
    tok = m.group(0)
    if _looks_like_solana_mint(tok):
        return tok
    return "***"


def _sanitize_text(text: Any) -> str:
    if text is None:
        return ""
    s = str(text)

    s = _URL_CRED_RE.sub(r"\1***@", s)
    s = _APIKEY_HDR_RE.sub(r"\1***", s)
    s = _BEARER_HDR_RE.sub(r"\1***", s)
    s = _BEARER_ANY_RE.sub(r"\1 ***", s)
    s = _KV_SECRET_RE.sub(r"\1\2***", s)
    s = _QP_SECRET_RE.sub(r"\1=***", s)

    if _DEFAULTS.redact_secret_values:
        with _SECRET_VALUES_LOCK:
            sv = list(_SECRET_VALUES)
        for sec in sv:
            try:
                if sec and sec in s:
                    s = s.replace(sec, "***")
            except Exception:
                continue

    if _DEFAULTS.mask_long_tokens:
        s = _LONG_TOKENISH_RE.sub(_mask_tokenish_match, s)

    return s


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


def redact_value(key: str, value: Any) -> Any:
    k = (key or "").lower()
    rk = _DEFAULTS.redact_keys

    if (
        k in rk
        or "token" in k
        or "secret" in k
        or "password" in k
        or "private" in k
        or "mnemonic" in k
        or "seed" in k
        or "apikey" in k
        or "api_key" in k
        or k.endswith("_key")
        or k.endswith("_secret")
        or k.endswith("_token")
    ):
        return "***"

    if k.endswith("_dsn") or k in ("dsn", "database_url", "postgres_dsn", "pg_dsn"):
        if isinstance(value, str):
            out = mask_url(value) if ("@" in value and "://" in value) else _sanitize_text(value)
            return _truncate_str(out)
        return "***"

    if k.endswith("_url") or k in ("redis_url", "rpc_url"):
        if isinstance(value, str):
            out = mask_url(value) if ("@" in value and "://" in value) else _sanitize_text(value)
            return _truncate_str(out)
        return "***"

    if isinstance(value, str):
        out = mask_url(value) if ("@" in value and "://" in value) else _sanitize_text(value)
        return _truncate_str(out)

    return value


def _jsonable(x: Any, *, depth: int = 0, max_depth: int = 6) -> Any:
    if x is None:
        return None
    if isinstance(x, (str, int, float, bool)):
        return _truncate_str(x) if isinstance(x, str) else x
    if isinstance(x, bytes):
        return f"<bytes:{len(x)}>"
    if depth >= max_depth:
        try:
            return _truncate_str(_sanitize_text(str(x)))
        except Exception:
            return "<unprintable>"

    if isinstance(x, (list, tuple)):
        return [_jsonable(i, depth=depth + 1, max_depth=max_depth) for i in x[:200]]

    if isinstance(x, dict):
        out: dict[str, Any] = {}
        for k, v in list(x.items())[:200]:
            ks = str(k)
            out[ks] = _jsonable(redact_value(ks, v), depth=depth + 1, max_depth=max_depth)
        return out

    if isinstance(x, Mapping):
        out2: dict[str, Any] = {}
        for k, v in list(x.items())[:200]:
            ks = str(k)
            out2[ks] = _jsonable(redact_value(ks, v), depth=depth + 1, max_depth=max_depth)
        return out2

    try:
        return _truncate_str(_sanitize_text(str(x)))
    except Exception:
        return "<unprintable>"


def _format_exc(exc_info: Any, max_chars: int) -> str:
    try:
        s = "".join(traceback.format_exception(*exc_info))
        s = _sanitize_text(s)
        return _cap_text(s, max_chars)
    except Exception:
        return "<exc_format_failed>"


# =============================================================================
# Context management
# =============================================================================


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


# =============================================================================
# Runtime meta & filter
# =============================================================================


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


class ContextFilter(logging.Filter):
    def __init__(self, include_src: bool = False) -> None:
        super().__init__()
        self.include_src = bool(include_src)

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        record.service = _RUNTIME.service
        record.version = _RUNTIME.version
        record.env = _RUNTIME.env
        record.host = _RUNTIME.host
        record.pid = _RUNTIME.pid

        task_name: str | None = None
        try:
            t = asyncio.current_task()
            if t is not None:
                task_name = t.get_name()
        except Exception:
            task_name = None
        record.taskName = task_name

        record.thread_name = threading.current_thread().name

        ctx = get_log_context()
        for k, v in ctx.items():
            ks = str(k)
            if ks in _RESERVED_RECORD_KEYS:
                continue
            if ks.startswith("_"):
                continue
            try:
                setattr(record, ks, redact_value(ks, v))
            except Exception:
                continue

        record._include_src = bool(self.include_src)
        return True


# =============================================================================
# Formatters
# =============================================================================


class JsonFormatter(logging.Formatter):
    def __init__(self, use_utc: bool = True) -> None:
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
            msg_raw = record.getMessage()
        except Exception:
            msg_raw = "<log_message_format_failed>"
        msg = _truncate_str(_sanitize_text(str(msg_raw)))

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
            "host": getattr(record, "host", ""),
            "pid": getattr(record, "pid", os.getpid()),
            "thread": getattr(record, "thread_name", ""),
            "taskName": getattr(record, "taskName", None),
        }

        event_name = getattr(record, "event", None)
        if event_name not in (None, "", False):
            payload["event"] = event_name

        if payload.get("taskName") in (None, "", False):
            payload.pop("taskName", None)

        if bool(getattr(record, "_include_src", False)):
            payload.update(
                {
                    "file": getattr(record, "filename", ""),
                    "line": getattr(record, "lineno", 0),
                    "func": getattr(record, "funcName", ""),
                    "module": getattr(record, "module", ""),
                }
            )

        for k, v in record.__dict__.items():
            if k in _RESERVED_RECORD_KEYS:
                continue
            if k.startswith("_"):
                continue
            if k in payload:
                continue
            payload[str(k)] = _jsonable(redact_value(str(k), v))

        if record.exc_info:
            payload["exc_type"] = getattr(record.exc_info[0], "__name__", "Exception")
            payload["exc"] = _format_exc(record.exc_info, _DEFAULTS.exc_max_chars)
        elif record.stack_info:
            s = _sanitize_text(str(record.stack_info))
            payload["stack"] = _cap_text(s, _DEFAULTS.stack_max_chars)

        try:
            return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
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
    def __init__(self, use_utc: bool = True) -> None:
        super().__init__(fmt="%(asctime)s %(levelname)s %(name)s: %(message)s")
        self.use_utc = bool(use_utc)

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:  # noqa: N802
        if self.use_utc:
            dt = datetime.fromtimestamp(record.created, tz=UTC)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        dt = datetime.fromtimestamp(record.created).astimezone()
        return dt.isoformat(timespec="seconds")

    def format(self, record: logging.LogRecord) -> str:  # noqa: A003
        try:
            msg_raw = record.getMessage()
        except Exception:
            msg_raw = "<log_message_format_failed>"

        record.message = _truncate_str(_sanitize_text(str(msg_raw)))

        if self.usesTime():
            record.asctime = self.formatTime(record, self.datefmt)

        s = self.formatMessage(record)

        if record.exc_info:
            if not getattr(record, "exc_text", None):
                record.exc_text = self.formatException(record.exc_info)
        exc_text = getattr(record, "exc_text", None)
        if exc_text:
            exc_txt = _cap_text(_sanitize_text(str(exc_text)), _DEFAULTS.exc_max_chars)
            if s and not s.endswith("\n"):
                s += "\n"
            s += exc_txt

        if record.stack_info:
            stack = _cap_text(
                _sanitize_text(self.formatStack(record.stack_info)), _DEFAULTS.stack_max_chars
            )
            if s and not s.endswith("\n"):
                s += "\n"
            s += stack

        return s


# =============================================================================
# Spam guards (log.every-style)
# =============================================================================


@dataclass
class _EveryState:
    last_ts: float
    suppressed: int


_EVERY_LOCK = threading.Lock()
_EVERY: dict[tuple[str, str], _EveryState] = {}
_EVERY_MAX_KEYS = 50_000


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
    logger.log(level, text, extra=extra)
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


def _install_every_on_logger_class() -> None:
    if callable(getattr(logging.Logger, "every", None)):
        return
    with suppress(Exception):
        logging.Logger.every = _logger_every


# =============================================================================
# Bound logger
# =============================================================================


class BoundLogger:
    def __init__(self, logger: logging.Logger, bound: Mapping[str, Any]) -> None:
        self._logger = logger
        self._bound = dict(bound)

    @property
    def logger(self) -> logging.Logger:
        return self._logger

    def _merge_extra(self, extra: Mapping[str, Any] | None) -> dict[str, Any]:
        out = dict(self._bound)
        if extra:
            out.update(extra)
        return out

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

    def every(
        self,
        name: str,
        *args: Any,
        every_sec: float | None = None,
        level: str | int = logging.INFO,
        msg: str | None = None,
        **fields: Any,
    ) -> bool:
        return bool(
            self._logger.every(name, *args, every_sec=every_sec, level=level, msg=msg, **fields)
        )

    def __getattr__(self, item: str) -> Any:
        return getattr(self._logger, item)


def bind(logger: logging.Logger | str | None = None, **kwargs: Any) -> BoundLogger:
    if logger is None:
        lg = get_logger()
    elif isinstance(logger, str):
        lg = get_logger(logger)
    else:
        lg = logger
    return BoundLogger(lg, kwargs)


def event(logger: logging.Logger | BoundLogger | None, name: str, **fields: Any) -> None:
    lg = logger or get_logger()
    extra = {"event": name}
    extra.update(fields)
    if isinstance(lg, BoundLogger):
        lg.info(name, extra=extra)
    else:
        lg.info(name, extra=extra)


# =============================================================================
# bump() / metrics
# =============================================================================


def bump(name: str, by: int = 1, **tags: Any) -> None:
    if not name:
        return
    inc = int(by) if by else 1
    with _METRICS_LOCK:
        _METRICS[name] = _METRICS.get(name, 0) + inc

    if _env_bool("LOG_BUMP_DEBUG", False):
        get_logger("metrics").debug("bump %s +%s tags=%s", name, inc, _jsonable(tags))


def metrics_snapshot() -> dict[str, int]:
    with _METRICS_LOCK:
        return dict(_METRICS)


def reset_metrics() -> None:
    with _METRICS_LOCK:
        _METRICS.clear()


# =============================================================================
# Configuration (idempotent + thread-safe)
# =============================================================================

_CONFIG_LOCK = threading.Lock()
_CONFIGURED = False


def _set_noisy_lib_levels() -> None:
    noisy_level = _parse_level(_DEFAULTS.noisy_min_level)
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


def _normalize_third_party_loggers() -> None:
    for lname in (
        "uvicorn",
        "uvicorn.error",
        "uvicorn.access",
        "gunicorn",
        "gunicorn.error",
        "gunicorn.access",
    ):
        lg = logging.getLogger(lname)
        lg.propagate = True
        with suppress(Exception):
            if lg.handlers:
                lg.handlers.clear()


def _handler_set_formatter(handler: logging.Handler, *, json_logs: bool, use_utc: bool) -> None:
    handler.setFormatter(
        JsonFormatter(use_utc=use_utc) if json_logs else TextFormatter(use_utc=use_utc)
    )


def _ensure_context_filter(handler: logging.Handler, *, include_src: bool) -> None:
    try:
        filters = list(getattr(handler, "filters", []))
    except Exception:
        filters = []

    for f in filters:
        if isinstance(f, ContextFilter):
            f.include_src = bool(include_src)
            return

    with suppress(Exception):
        handler.addFilter(ContextFilter(include_src=bool(include_src)))


@dataclass
class _StdIoHookState:
    orig_stdout: Any
    orig_stderr: Any
    patched_handlers: list[tuple[logging.Handler, Any]]


_STDIO_LOCK = threading.Lock()
_STDIO_STATE: _StdIoHookState | None = None


def _preferred_root_stream() -> Any:
    st = _STDIO_STATE
    if st is not None and st.orig_stdout is not None:
        return st.orig_stdout
    return sys.stdout if sys.stdout is not None else _BASE_STDOUT


def configure_logging(
    *,
    level: str | int | None = None,
    json_logs: bool | None = None,
    use_utc: bool | None = None,
    include_src: bool | None = None,
    service: str | None = None,
    version: str | None = None,
    env: str | None = None,
    force: bool = False,
    capture_warnings: bool = True,
) -> None:
    global _CONFIGURED, _RUNTIME, _DEFAULTS

    _install_every_on_logger_class()
    _DEFAULTS = _read_env_defaults()
    _refresh_secret_values()

    if _DEFAULTS.force_reset:
        force = True

    lvl = _parse_level(level if level is not None else _DEFAULTS.level)

    if json_logs is None:
        json_logs = _DEFAULTS.json_logs
    if use_utc is None:
        use_utc = _DEFAULTS.use_utc
    if include_src is None:
        include_src = _DEFAULTS.include_src

    with _CONFIG_LOCK:
        _RUNTIME = _RuntimeMeta(
            service=(service or _DEFAULTS.service or _RUNTIME.service).strip() or "sniper",
            version=(version or _DEFAULTS.version or _RUNTIME.version).strip() or "v1",
            env=(env or _DEFAULTS.env or _RUNTIME.env).strip() or "dev",
            host=socket.gethostname(),
            pid=os.getpid(),
        )

        root = logging.getLogger()

        if _CONFIGURED and not force:
            root.setLevel(lvl)
            for h in list(root.handlers):
                with suppress(Exception):
                    h.setLevel(lvl)
                with suppress(Exception):
                    if isinstance(h, logging.StreamHandler):
                        desired = _preferred_root_stream()
                        if getattr(h, "stream", None) is sys.stdout:
                            h.stream = desired
                with suppress(Exception):
                    _handler_set_formatter(h, json_logs=bool(json_logs), use_utc=bool(use_utc))
                _ensure_context_filter(h, include_src=bool(include_src))
            logging.captureWarnings(bool(capture_warnings))
            _set_noisy_lib_levels()
            _normalize_third_party_loggers()
            return

        if force:
            for h in list(root.handlers):
                with suppress(Exception):
                    root.removeHandler(h)

        root.setLevel(lvl)

        if not root.handlers:
            handler = logging.StreamHandler(stream=_preferred_root_stream())
            handler.setLevel(lvl)
            _handler_set_formatter(handler, json_logs=bool(json_logs), use_utc=bool(use_utc))
            _ensure_context_filter(handler, include_src=bool(include_src))
            root.addHandler(handler)
        else:
            for h in list(root.handlers):
                with suppress(Exception):
                    h.setLevel(lvl)
                with suppress(Exception):
                    if (
                        isinstance(h, logging.StreamHandler)
                        and getattr(h, "stream", None) is sys.stdout
                    ):
                        h.stream = _preferred_root_stream()
                with suppress(Exception):
                    _handler_set_formatter(h, json_logs=bool(json_logs), use_utc=bool(use_utc))
                _ensure_context_filter(h, include_src=bool(include_src))

        logging.captureWarnings(bool(capture_warnings))
        _set_noisy_lib_levels()
        _normalize_third_party_loggers()
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
    force: bool = False,
) -> logging.Logger:
    needs_config = (not _CONFIGURED) or any(
        x is not None for x in (service, version, env, level, json_logs, use_utc, include_src)
    )
    if needs_config:
        try:
            configure_logging(
                level=level,
                json_logs=json_logs,
                use_utc=use_utc,
                include_src=include_src,
                service=service,
                version=version,
                env=env,
                force=force,
            )
        except Exception:
            logging.basicConfig(level=_parse_level(level))

    target = name if name else (service or _DEFAULTS.service or "sniper")
    return logging.getLogger(target)


# =============================================================================
# StdIO hook (hook_stdio / unhook_stdio) — recursion-safe
# =============================================================================


class _StreamToLogger(io.TextIOBase):
    def __init__(self, logger: logging.Logger, level: int, tee: Any) -> None:
        super().__init__()
        self._logger = logger
        self._level = int(level)
        self._tee = tee
        self._local = threading.local()

    def writable(self) -> bool:  # pragma: no cover
        return True

    def _buf(self) -> str:
        return getattr(self._local, "buf", "")

    def _set_buf(self, s: str) -> None:
        self._local.buf = s

    def _emit_line(self, line: str) -> None:
        line = line.rstrip("\r\n")
        if not line:
            return
        self._logger.log(self._level, _sanitize_text(line), extra={"event": "STDIO"})

    def write(self, s: str) -> int:
        if not s:
            return 0

        if getattr(self._local, "in_write", False):
            try:
                return int(self._tee.write(s))
            except Exception:
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

    @property
    def encoding(self) -> str:  # pragma: no cover
        return getattr(self._tee, "encoding", "utf-8")


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

        out_lvl = _parse_level(stdout_level)
        err_lvl = _parse_level(stderr_level)

        orig_out = sys.stdout if sys.stdout is not None else _BASE_STDOUT
        orig_err = sys.stderr if sys.stderr is not None else _BASE_STDERR

        with suppress(Exception):
            configure_logging()

        patched: list[tuple[logging.Handler, Any]] = []
        root = logging.getLogger()
        for h in list(root.handlers):
            if isinstance(h, logging.StreamHandler):
                try:
                    stream = getattr(h, "stream", None)
                    if stream is sys.stdout:
                        patched.append((h, stream))
                        h.stream = orig_out
                    elif stream is sys.stderr:
                        patched.append((h, stream))
                        h.stream = orig_err
                except Exception:
                    continue

        sys.stdout = _StreamToLogger(get_logger(stdout_logger), out_lvl, tee=orig_out)
        sys.stderr = _StreamToLogger(get_logger(stderr_logger), err_lvl, tee=orig_err)

        _STDIO_STATE = _StdIoHookState(
            orig_stdout=orig_out,
            orig_stderr=orig_err,
            patched_handlers=patched,
        )

        with suppress(Exception):
            atexit.register(unhook_stdio)


def unhook_stdio() -> None:
    global _STDIO_STATE
    with _STDIO_LOCK:
        if _STDIO_STATE is None:
            return

        sys.stdout = _STDIO_STATE.orig_stdout
        sys.stderr = _STDIO_STATE.orig_stderr

        for h, old_stream in _STDIO_STATE.patched_handlers:
            with suppress(Exception):
                if isinstance(h, logging.StreamHandler):
                    h.stream = old_stream

        _STDIO_STATE = None


# =============================================================================
# Excepthook
# =============================================================================

_EXC_LOCK = threading.Lock()
_OLD_EXCEPTHOOK: Any | None = None
_OLD_THREAD_EXCEPTHOOK: Any | None = None
_EXC_INSTALLED = False


def install_excepthook(logger: logging.Logger | None = None) -> None:
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
                if _OLD_EXCEPTHOOK:
                    _OLD_EXCEPTHOOK(exc_type, exc, tb)
                return
            try:
                lg.critical(
                    "UNCAUGHT_EXCEPTION",
                    exc_info=(exc_type, exc, tb),
                    extra={"event": "UNCAUGHT_EXCEPTION"},
                )
            except Exception:
                with suppress(Exception):
                    _BASE_STDERR.write("UNCAUGHT_EXCEPTION (logging failed)\n")
                    _BASE_STDERR.write("".join(traceback.format_exception(exc_type, exc, tb)))
                    _BASE_STDERR.flush()
            finally:
                if _OLD_EXCEPTHOOK:
                    with suppress(Exception):
                        _OLD_EXCEPTHOOK(exc_type, exc, tb)

        sys.excepthook = _hook

        if hasattr(threading, "excepthook"):
            try:
                _OLD_THREAD_EXCEPTHOOK = threading.excepthook

                def _thread_hook(args: Any) -> None:
                    try:
                        et = getattr(args, "exc_type", Exception)
                        ev = getattr(args, "exc_value", None)
                        tb2 = getattr(args, "exc_traceback", None)
                        thr = getattr(args, "thread", None)
                        tname = getattr(thr, "name", None)
                        lg.critical(
                            "THREAD_UNCAUGHT_EXCEPTION",
                            exc_info=(et, ev, tb2),
                            extra={"event": "THREAD_UNCAUGHT_EXCEPTION", "thread_uncaught": tname},
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


# =============================================================================
# Global heartbeat
# =============================================================================

_HEARTBEAT_LOCK = threading.Lock()
_HEARTBEAT_STOP: threading.Event | None = None
_HEARTBEAT_THREAD: threading.Thread | None = None


def _heartbeat_enabled_default() -> bool:
    return _env_bool("LOG_HEARTBEAT", True)


def start_global_heartbeat(
    *,
    interval_sec: float = 30.0,
    logger_name: str = "heartbeat",
    event_name: str = "HEARTBEAT",
    enabled: bool | None = None,
    include_metrics: bool = True,
    include_runtime: bool = True,
) -> bool:
    global _HEARTBEAT_STOP, _HEARTBEAT_THREAD

    if enabled is None:
        enabled = _heartbeat_enabled_default()

    try:
        interval = float(interval_sec)
    except Exception:
        interval = 30.0

    if (not enabled) or interval <= 0:
        return False

    with _HEARTBEAT_LOCK:
        if _HEARTBEAT_THREAD is not None and _HEARTBEAT_THREAD.is_alive():
            return False

        _HEARTBEAT_STOP = threading.Event()
        lg = get_logger(logger_name)

        def _loop() -> None:
            with suppress(Exception):
                configure_logging()
            with suppress(Exception):
                time.sleep(min(1.0, interval / 10))

            while True:
                stop_ev = _HEARTBEAT_STOP
                if stop_ev is None or stop_ev.is_set():
                    break

                extra: dict[str, Any] = {"event": event_name}

                if include_runtime:
                    extra.update(
                        {
                            "runtime_service": _RUNTIME.service,
                            "runtime_version": _RUNTIME.version,
                            "runtime_env": _RUNTIME.env,
                            "runtime_host": _RUNTIME.host,
                            "runtime_pid": _RUNTIME.pid,
                        }
                    )

                if include_metrics:
                    snap = metrics_snapshot()
                    extra["metrics_n"] = len(snap)
                    if snap:
                        keys = sorted(snap.keys())
                        top_keys = keys[:25]
                        extra["metrics_top"] = {k: snap.get(k, 0) for k in top_keys}

                with suppress(Exception):
                    lg.info(event_name, extra=extra)

                stop_ev2 = _HEARTBEAT_STOP
                if stop_ev2 is None:
                    break
                stop_ev2.wait(interval)

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


# =============================================================================
# Test reset helper (critical for pytest isolation)
# =============================================================================


def reset_logging_state() -> None:
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
            with suppress(Exception):
                root.removeHandler(h)

    with suppress(Exception):
        logging.captureWarnings(False)

    with suppress(Exception):
        with _EVERY_LOCK:
            _EVERY.clear()

    with suppress(Exception):
        reset_metrics()

    with suppress(Exception):
        _refresh_secret_values()

    _CONFIGURED = False
    _DEFAULTS = _LogDefaults(
        service="sniper",
        version="v1",
        env="dev",
        level="INFO",
        json_logs=True,
        use_utc=True,
        include_src=False,
        exc_max_chars=8000,
        stack_max_chars=8000,
        value_max_chars=4096,
        noisy_min_level="INFO",
        every_default_sec=30.0,
        redact_keys=set(_DEFAULT_REDACT_KEYS),
        mask_long_tokens=True,
        redact_secret_values=True,
        force_reset=False,
    )
    _RUNTIME = _RuntimeMeta(
        service=_DEFAULTS.service,
        version=_DEFAULTS.version,
        env=_DEFAULTS.env,
        host=socket.gethostname(),
        pid=os.getpid(),
    )
