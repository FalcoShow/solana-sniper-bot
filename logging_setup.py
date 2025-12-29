# logging_setup.py
# stdlib-only structured JSON logging for high-throughput bots/services (V3+++ LOCK-IN)
#
# Guarantees:
# - JSON logs with consistent fields (ts/time/level/logger/service/version/host/pid + extras)
# - NO crashes from "extra" collisions (e.g., msg=..., levelname=..., etc.)
# - Rate limiting / dedup to stop log spam from killing CPU + storage
# - Secret-safe redaction (extra fields + common credential shapes + DSNs + key arrays)
# - Convenience helpers: log.event / log.every / log.once / log.sample / log.bind / heartbeat
# - Optional stdio capture + uncaught exception hook + asyncio loop exception hook

from __future__ import annotations

import collections
import hashlib
import json
import logging
import os
import re
import socket
import sys
import threading
import time
from io import TextIOBase
from typing import Any, Dict, Iterable, List, Optional, Tuple


# ──────────────────────────────────────────────────────────────────────────────
# Reserved LogRecord keys (prevents KeyError: "Attempt to overwrite 'msg'")
# ──────────────────────────────────────────────────────────────────────────────

# Build once: the keys logging creates on a standard record.
_RESERVED_LOGRECORD_KEYS = set(logging.makeLogRecord({}).__dict__.keys())

# Also treat these as "reserved-ish" to avoid confusion.
_RESERVED_LOGRECORD_KEYS |= {"message", "asctime"}  # logging formatters sometimes inject these later


def _sanitize_extra(extra: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ensure 'extra' dict will never collide with LogRecord attributes.
    Colliding keys are renamed deterministically.

    Example: {"msg": "..."} -> {"x_msg": "..."}  (so logging won't raise)
    """
    if not extra:
        return {}

    out: Dict[str, Any] = {}
    for k, v in extra.items():
        kk = str(k)

        if kk in _RESERVED_LOGRECORD_KEYS:
            # Deterministic rename
            nk = f"x_{kk}"
        else:
            nk = kk

        # Ensure uniqueness if user already had x_msg etc.
        if nk in out:
            i = 2
            base = nk
            while f"{base}_{i}" in out:
                i += 1
            nk = f"{base}_{i}"

        out[nk] = v

    return out


# ──────────────────────────────────────────────────────────────────────────────
# Constants / helpers
# ──────────────────────────────────────────────────────────────────────────────

_DEFAULT_EXCLUDE = {
    "name", "msg", "args", "levelname", "levelno", "pathname", "filename", "module",
    "exc_info", "exc_text", "stack_info", "lineno", "funcName", "created", "msecs",
    "relativeCreated", "thread", "threadName", "processName", "process", "asctime",
}

_LEVEL_MAP = {
    "CRITICAL": logging.CRITICAL, "FATAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARN": logging.WARNING, "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
    "TRACE": logging.DEBUG,  # alias
}

_SENSITIVE_KEY_RE = re.compile(
    r"(token|secret|api[_-]?key|private|mnemonic|seed|pass(word)?|bearer|authorization|cookie|dsn|keypair)",
    re.IGNORECASE,
)

_DSN_RE_1 = re.compile(r"(\w+://)([^:@/]+):([^@/]+)@")
_DSN_RE_2 = re.compile(r"(postgres(?:ql)?\+?\w*://)([^:@/]+):([^@/]+)@")

_JSON_INT_ARRAY_RE = re.compile(r"^\[\s*\d+(?:\s*,\s*\d+){10,}\s*\]\s*$")
_BASE58ISH_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{28,}$")
_HEXISH_RE = re.compile(r"^[0-9a-fA-F]{32,}$")


def _env_level() -> int:
    lvl = os.getenv("PF_LOG_LEVEL") or os.getenv("LOG_LEVEL") or "INFO"
    return _LEVEL_MAP.get(lvl.upper(), logging.INFO)


def _env_service() -> str:
    return os.getenv("PF_SERVICE") or os.getenv("APP_SERVICE") or os.getenv("APP_NAME") or "sniper"


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    s = str(v).strip().lower()
    return s in {"1", "true", "yes", "y", "on", "t"}


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return float(str(v).strip().replace("_", ""))
    except Exception:
        return default


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return int(float(str(v).strip().replace("_", "")))
    except Exception:
        return default


def _coerce(obj: Any) -> Any:
    """Coerce unknown types to JSON-safe values."""
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    if isinstance(obj, (list, tuple)):
        return [_coerce(x) for x in obj]
    if isinstance(obj, dict):
        return {str(k): _coerce(v) for k, v in obj.items()}
    if isinstance(obj, bytes):
        try:
            return obj.decode("utf-8", "replace")
        except Exception:
            return repr(obj)
    return str(obj)


def _mask_value(s: str, keep_head: int = 4, keep_tail: int = 0) -> str:
    if not s:
        return ""
    s = str(s)
    if len(s) <= keep_head + keep_tail:
        return "*" * len(s)
    head = s[:keep_head] if keep_head > 0 else ""
    tail = s[-keep_tail:] if keep_tail > 0 else ""
    return f"{head}…{'*' * 6}{tail}"


def _redact_dsn(s: str) -> str:
    if not s:
        return ""
    out = str(s)
    out = _DSN_RE_2.sub(r"\1\2:***@", out)
    out = _DSN_RE_1.sub(r"\1\2:***@", out)
    return out


def _looks_like_secret_string(val: str) -> bool:
    if not val:
        return False
    v = val.strip()
    if _JSON_INT_ARRAY_RE.match(v):
        return True
    if _BASE58ISH_RE.match(v):
        return True
    if _HEXISH_RE.match(v):
        return True
    if len(v) >= 40 and any(c.isdigit() for c in v) and any(c.isalpha() for c in v):
        return True
    return False


def _redact_value(key: Optional[str], value: Any) -> Any:
    k = (key or "").strip()
    is_sensitive_key = bool(k and _SENSITIVE_KEY_RE.search(k))

    if isinstance(value, str) and "://" in value and "@" in value and ":" in value:
        return _redact_dsn(value)

    if isinstance(value, str) and _JSON_INT_ARRAY_RE.match(value.strip()):
        return "[REDACTED_KEY_ARRAY]"

    if isinstance(value, (list, tuple)):
        if is_sensitive_key and len(value) >= 32 and all(isinstance(x, int) for x in value):
            return f"[REDACTED_INT_ARRAY len={len(value)}]"
        return [_redact_value(k, x) for x in value]

    if isinstance(value, dict):
        return {str(kk): _redact_value(str(kk), vv) for kk, vv in value.items()}

    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8", "replace")
        except Exception:
            value = repr(value)

    if isinstance(value, str):
        v = value.strip()
        if is_sensitive_key and _looks_like_secret_string(v):
            return _mask_value(v, 4, 0)
        if "bearer " in v.lower() and len(v) > 20:
            parts = v.split()
            if len(parts) >= 2:
                parts[1] = _mask_value(parts[1], 4, 0)
                return " ".join(parts)
        return value

    return value


# ──────────────────────────────────────────────────────────────────────────────
# Filters: Secret redaction + dedup + burst limiter
# ──────────────────────────────────────────────────────────────────────────────

class SecretRedactionFilter(logging.Filter):
    def __init__(self, enabled: bool = True):
        super().__init__()
        self.enabled = bool(enabled)

    def filter(self, record: logging.LogRecord) -> bool:
        if not self.enabled:
            return True

        # redact extras in-place
        for k, v in list(record.__dict__.items()):
            if k in _DEFAULT_EXCLUDE:
                continue
            record.__dict__[k] = _redact_value(k, v)

        # best-effort DSN redaction inside message too
        try:
            if isinstance(record.msg, str):
                record.msg = _redact_dsn(record.msg)
        except Exception:
            pass

        return True


class DedupFilter(logging.Filter):
    def __init__(self, window_sec: float = 20.0, max_keys: int = 20_000):
        super().__init__()
        self.window = float(max(0.0, window_sec))
        self.max_keys = int(max(1000, max_keys))
        self._lock = threading.Lock()
        self._state: Dict[Tuple[Any, ...], Tuple[float, int, float]] = {}
        self._lru: collections.deque[Tuple[Any, ...]] = collections.deque()

    def _touch(self, key: Tuple[Any, ...]) -> None:
        self._lru.append(key)
        if len(self._lru) > self.max_keys * 3:
            seen = set()
            newq: collections.deque[Tuple[Any, ...]] = collections.deque()
            for k in reversed(self._lru):
                if k in seen:
                    continue
                if k in self._state:
                    seen.add(k)
                    newq.appendleft(k)
                if len(newq) >= self.max_keys:
                    break
            self._lru = newq

    def filter(self, record: logging.LogRecord) -> bool:
        if self.window <= 0:
            return True

        event = getattr(record, "event", None)
        line = getattr(record, "line", None)
        key = (record.name, record.levelno, event, line if line is not None else record.getMessage())

        now = time.monotonic()
        with self._lock:
            next_ok_at, suppressed, _last = self._state.get(key, (0.0, 0, 0.0))
            if now >= next_ok_at:
                if suppressed:
                    setattr(record, "suppressed", suppressed)
                self._state[key] = (now + self.window, 0, now)
                self._touch(key)
            else:
                self._state[key] = (next_ok_at, suppressed + 1, now)
                self._touch(key)
                return False

            while len(self._state) > self.max_keys and self._lru:
                k = self._lru.popleft()
                if k in self._state:
                    del self._state[k]

        return True


class BurstLimitFilter(logging.Filter):
    def __init__(self, max_per_sec: int = 200, key_mode: str = "logger+level"):
        super().__init__()
        self.max_per_sec = int(max(0, max_per_sec))
        self.key_mode = str(key_mode or "logger+level").strip().lower()
        self._lock = threading.Lock()
        self._sec = 0
        self._counts: Dict[str, int] = {}
        self._suppressed: int = 0

    def _k(self, record: logging.LogRecord) -> str:
        if self.key_mode == "global":
            return "global"
        if self.key_mode == "logger":
            return record.name
        if self.key_mode == "level":
            return str(record.levelno)
        if self.key_mode == "event":
            return str(getattr(record, "event", record.getMessage()[:64]))
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
                self._suppressed = 0

            c = self._counts.get(k, 0) + 1
            self._counts[k] = c
            if c > self.max_per_sec:
                self._suppressed += 1
                return False

        return True


# ──────────────────────────────────────────────────────────────────────────────
# JSON formatter / handler (fail-safe emit)
# ──────────────────────────────────────────────────────────────────────────────

class JsonFormatter(logging.Formatter):
    def __init__(self, service: str, version: Optional[str] = None):
        super().__init__()
        self.service = service
        self.version = version or os.getenv("APP_VERSION") or os.getenv("VERSION") or "v1"
        self.host = os.getenv("HOSTNAME") or socket.gethostname()
        self.pid = os.getpid()

    def format(self, record: logging.LogRecord) -> str:
        base: Dict[str, Any] = {
            "ts": int(record.created),
            "time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(record.created)),
            "level": record.levelname,
            "logger": record.name,
            "service": self.service,
            "version": self.version,
            "host": self.host,
            "pid": self.pid,
        }

        # Extras (already sanitized in EventLogger._log)
        for k, v in record.__dict__.items():
            if k in _DEFAULT_EXCLUDE:
                continue
            base[k] = _coerce(v)

        msg = record.getMessage()
        if msg and "event" not in base:
            base["msg"] = msg
        elif msg and msg != base.get("event"):
            base["msg"] = msg

        base.setdefault("source", f"{record.filename}:{record.lineno}")

        if hasattr(record, "suppressed"):
            base["suppressed"] = getattr(record, "suppressed")

        if record.exc_info:
            etype = record.exc_info[0].__name__ if record.exc_info[0] else None
            base["exc_type"] = etype
            base["exc"] = self.formatException(record.exc_info)

        return json.dumps(base, separators=(",", ":"), ensure_ascii=False)


class JsonStreamHandler(logging.StreamHandler):
    """
    Never let logging kill the process. If emit fails, fallback to stderr.
    """
    def __init__(self, service: str, version: Optional[str] = None, stream=None):
        super().__init__(stream or sys.stdout)
        self.setFormatter(JsonFormatter(service=service, version=version))

    def emit(self, record: logging.LogRecord) -> None:
        try:
            super().emit(record)
        except Exception:
            try:
                fallback = {
                    "ts": int(time.time()),
                    "time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "level": record.levelname,
                    "logger": record.name,
                    "service": "logger",
                    "msg": "LOG_EMIT_FAILED",
                    "orig": str(getattr(record, "msg", "")),
                }
                sys.stderr.write(json.dumps(fallback) + "\n")
            except Exception:
                pass


# ──────────────────────────────────────────────────────────────────────────────
# Logger with helpers (sanitizes extras at the core)
# ──────────────────────────────────────────────────────────────────────────────

class EventLogger(logging.Logger):
    _once_seen: Dict[str, float] = {}
    _once_lock = threading.Lock()

    _every_lock = threading.Lock()
    _every_counters: collections.Counter[str] = collections.Counter()

    _sample_lock = threading.Lock()
    _sample_counters: collections.Counter[str] = collections.Counter()

    def _log(  # type: ignore[override]
        self,
        level: int,
        msg: Any,
        args: Any,
        exc_info=None,
        extra: Optional[Dict[str, Any]] = None,
        stack_info: bool = False,
        stacklevel: int = 1,
    ) -> None:
        # Sanitize extras *once* here so no call site can ever crash logging.
        safe_extra = _sanitize_extra(extra or {})
        super()._log(level, msg, args, exc_info=exc_info, extra=safe_extra, stack_info=stack_info, stacklevel=stacklevel)

    def event(self, name: str, level: int = logging.INFO, **fields: Any) -> None:
        fields.setdefault("event", name)
        self.log(level, name, extra=fields, stacklevel=2)

    def bind(self, **ctx: Any) -> "BoundLogger":
        return BoundLogger(self, ctx)

    def once(
        self,
        key: str,
        ttl: float = 30.0,
        level: int = logging.INFO,
        msg: Optional[str] = None,
        **fields: Any,
    ) -> bool:
        now = time.monotonic()
        with self._once_lock:
            last = self._once_seen.get(key, 0.0)
            if now - last < float(ttl):
                return False
            self._once_seen[key] = now
        self.log(level, msg or fields.get("event") or key, extra=fields, stacklevel=2)
        return True

    def every(
        self,
        key: str,
        n: int = 10,
        level: int = logging.DEBUG,
        msg: Optional[str] = None,
        **fields: Any,
    ) -> None:
        n = max(1, int(n))
        with self._every_lock:
            self._every_counters[key] += 1
            i = self._every_counters[key]
        if i % n == 0:
            self.log(level, msg or fields.get("event") or key, extra=fields, stacklevel=2)

    def sample(
        self,
        key: str,
        rate: float = 0.1,
        level: int = logging.DEBUG,
        msg: Optional[str] = None,
        **fields: Any,
    ) -> None:
        r = max(0.0, min(1.0, float(rate)))
        with self._sample_lock:
            self._sample_counters[key] += 1
            i = self._sample_counters[key]

        if i == 1:
            self.log(level, msg or fields.get("event") or key, extra=fields, stacklevel=2)
            return

        h = hashlib.md5(f"{key}:{i}".encode("utf-8")).digest()
        bucket = int.from_bytes(h[:2], "big") / 65535.0
        if bucket < r:
            self.log(level, msg or fields.get("event") or key, extra=fields, stacklevel=2)


class BoundLogger:
    __slots__ = ("_base", "_ctx")

    def __init__(self, base: EventLogger, ctx: Dict[str, Any]):
        self._base = base
        self._ctx = dict(ctx or {})

    def bind(self, **more: Any) -> "BoundLogger":
        nxt = dict(self._ctx)
        nxt.update(more)
        return BoundLogger(self._base, nxt)

    def event(self, name: str, level: int = logging.INFO, **fields: Any) -> None:
        merged = dict(self._ctx)
        merged.update(fields)
        self._base.event(name, level=level, **merged)

    def once(self, key: str, ttl: float = 30.0, level: int = logging.INFO, msg: Optional[str] = None, **fields: Any) -> bool:
        merged = dict(self._ctx)
        merged.update(fields)
        return self._base.once(key, ttl=ttl, level=level, msg=msg, **merged)

    def every(self, key: str, n: int = 10, level: int = logging.DEBUG, msg: Optional[str] = None, **fields: Any) -> None:
        merged = dict(self._ctx)
        merged.update(fields)
        self._base.every(key, n=n, level=level, msg=msg, **merged)

    def sample(self, key: str, rate: float = 0.1, level: int = logging.DEBUG, msg: Optional[str] = None, **fields: Any) -> None:
        merged = dict(self._ctx)
        merged.update(fields)
        self._base.sample(key, rate=rate, level=level, msg=msg, **merged)

    def log(self, level: int, msg: str, **fields: Any) -> None:
        merged = dict(self._ctx)
        merged.update(fields)
        self._base.log(level, msg, extra=merged, stacklevel=2)

    def debug(self, msg: str, **fields: Any) -> None: self.log(logging.DEBUG, msg, **fields)
    def info(self, msg: str, **fields: Any) -> None: self.log(logging.INFO, msg, **fields)
    def warning(self, msg: str, **fields: Any) -> None: self.log(logging.WARNING, msg, **fields)
    def error(self, msg: str, **fields: Any) -> None: self.log(logging.ERROR, msg, **fields)
    def exception(self, msg: str, **fields: Any) -> None:
        merged = dict(self._ctx)
        merged.update(fields)
        self._base.error(msg, extra=merged, exc_info=True, stacklevel=2)  # type: ignore[arg-type]


# ──────────────────────────────────────────────────────────────────────────────
# Heartbeat counter (hot-path safe)
# ──────────────────────────────────────────────────────────────────────────────

class Heartbeat:
    def __init__(self, logger: EventLogger, interval: float = 30.0, name: str = "HEARTBEAT"):
        self.log = logger
        self.interval = float(interval)
        self.name = str(name)
        self._ctr = collections.Counter()
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
            self._ctr[key] += int(n)

    def _run(self) -> None:
        while self._running:
            time.sleep(self.interval)
            snapshot: Dict[str, int] = {}
            with self._lock:
                if self._ctr:
                    snapshot = dict(self._ctr)
                    self._ctr.clear()
            if snapshot:
                self.log.event(self.name, **snapshot)


# ──────────────────────────────────────────────────────────────────────────────
# Factory & setup (idempotent)
# ──────────────────────────────────────────────────────────────────────────────

_LOGGER_CLASS_SET = False
_global_hb: Optional[Heartbeat] = None

_stdout_original = sys.stdout
_stderr_original = sys.stderr


def get_logger(
    name: str = "sniper",
    level: Optional[int] = None,
    service: Optional[str] = None,
    version: Optional[str] = None,
    dedup_window_sec: Optional[float] = None,
) -> EventLogger:
    global _LOGGER_CLASS_SET, _global_hb

    if not _LOGGER_CLASS_SET:
        logging.setLoggerClass(EventLogger)
        _LOGGER_CLASS_SET = True

    if level is None:
        level = _env_level()
    if service is None:
        service = _env_service()

    if dedup_window_sec is None:
        dedup_window_sec = _env_float("PF_LOG_DEDUP_WINDOW_SEC", 20.0)

    dedup_max_keys = _env_int("PF_LOG_DEDUP_MAX_KEYS", 20_000)
    redact_enabled = _env_bool("PF_LOG_REDACT", True)

    burst_max = _env_int("PF_LOG_BURST_MAX_PER_SEC", 200)
    burst_key = os.getenv("PF_LOG_BURST_KEY", "logger+level")

    log: EventLogger = logging.getLogger(name)  # type: ignore[assignment]
    log.setLevel(level)
    log.propagate = False

    handler: Optional[JsonStreamHandler] = None
    for h in log.handlers:
        if isinstance(h, JsonStreamHandler):
            handler = h
            break

    if handler is None:
        handler = JsonStreamHandler(service=service, version=version)
        handler.setLevel(level)

        handler.addFilter(SecretRedactionFilter(enabled=redact_enabled))
        if burst_max > 0:
            handler.addFilter(BurstLimitFilter(max_per_sec=burst_max, key_mode=burst_key))
        if dedup_window_sec and dedup_window_sec > 0:
            handler.addFilter(DedupFilter(window_sec=float(dedup_window_sec), max_keys=dedup_max_keys))

        log.addHandler(handler)
    else:
        handler.setLevel(level)

    hb_sec = os.getenv("PF_HEARTBEAT_SEC")
    if hb_sec and _global_hb is None:
        try:
            iv = float(str(hb_sec).strip().replace("_", ""))
            if iv > 0:
                name_override = os.getenv("PF_HEARTBEAT_NAME", "HEARTBEAT")
                _global_hb = Heartbeat(log, interval=iv, name=name_override).start()
        except Exception:
            pass

    return log


def set_level(logger: logging.Logger, level_str: str) -> None:
    level = _LEVEL_MAP.get(level_str.upper(), logging.INFO)
    os.environ["PF_LOG_LEVEL"] = level_str.upper()
    logger.setLevel(level)
    for h in getattr(logger, "handlers", []):
        try:
            h.setLevel(level)
        except Exception:
            pass
    if isinstance(logger, EventLogger):
        logger.event("LOG_LEVEL_CHANGED", level_str=level_str.upper())


# ──────────────────────────────────────────────────────────────────────────────
# Stdout/stderr capture (route print() to JSON logs)
# ──────────────────────────────────────────────────────────────────────────────

class _PrintToLogger(TextIOBase):
    def __init__(self, logger: EventLogger, level: int):
        self.logger = logger
        self.level = level
        self._buf = ""

    def writable(self) -> bool:
        return True

    def write(self, s: str) -> int:
        if not isinstance(s, str):
            try:
                s = s.decode("utf-8", "replace")  # type: ignore[attr-defined]
            except Exception:
                s = str(s)
        self._buf += s
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            line = line.strip()
            if line:
                self.logger.log(self.level, "STDIO", extra={"event": "STDIO", "line": line}, stacklevel=3)
        return len(s)

    def flush(self) -> None:
        if self._buf.strip():
            self.logger.log(self.level, "STDIO", extra={"event": "STDIO", "line": self._buf.strip()}, stacklevel=3)
            self._buf = ""


def hook_stdio(logger: EventLogger, stdout_level: int = logging.INFO, stderr_level: int = logging.ERROR) -> None:
    global _stdout_original, _stderr_original
    _stdout_original = sys.stdout
    _stderr_original = sys.stderr
    sys.stdout = _PrintToLogger(logger, stdout_level)  # type: ignore[assignment]
    sys.stderr = _PrintToLogger(logger, stderr_level)  # type: ignore[assignment]


def restore_stdio() -> None:
    global _stdout_original, _stderr_original
    sys.stdout = _stdout_original  # type: ignore[assignment]
    sys.stderr = _stderr_original  # type: ignore[assignment]


# ──────────────────────────────────────────────────────────────────────────────
# Uncaught exception hook + asyncio loop exception hook
# ──────────────────────────────────────────────────────────────────────────────

def install_excepthook(logger: EventLogger) -> None:
    def _hook(exc_type, exc, tb):
        logger.error("UNCAUGHT", exc_info=(exc_type, exc, tb))
        sys.__excepthook__(exc_type, exc, tb)
    sys.excepthook = _hook


def install_asyncio_exception_hook(logger: EventLogger, loop=None) -> None:
    """
    Safe loop exception handler that will never crash on reserved keys.
    """
    try:
        import asyncio  # stdlib
        lp = loop or asyncio.get_event_loop()
    except Exception:
        return

    def _handler(lp, context: Dict[str, Any]) -> None:
        # context often contains: message, exception, future/task, handle, etc.
        fields = dict(context or {})
        # normalize key naming (leave 'message' as-is; avoid using 'msg')
        if "message" in fields and "event" not in fields:
            fields["event"] = "ASYNCIO_EXCEPTION"
        logger.error("ASYNCIO_EXCEPTION", extra=fields, exc_info=fields.get("exception"), stacklevel=2)

    try:
        lp.set_exception_handler(_handler)
    except Exception:
        pass


# ──────────────────────────────────────────────────────────────────────────────
# Global heartbeat convenience
# ──────────────────────────────────────────────────────────────────────────────

def start_global_heartbeat(logger: EventLogger, interval: float = 30.0, name: str = "HEARTBEAT") -> Heartbeat:
    global _global_hb
    _global_hb = Heartbeat(logger, interval=float(interval), name=str(name)).start()
    return _global_hb


def bump(key: str, n: int = 1) -> None:
    if _global_hb:
        _global_hb.bump(str(key), int(n))


__all__ = [
    "JsonFormatter",
    "JsonStreamHandler",
    "EventLogger",
    "BoundLogger",
    "get_logger",
    "set_level",
    "Heartbeat",
    "start_global_heartbeat",
    "bump",
    "hook_stdio",
    "restore_stdio",
    "install_excepthook",
    "install_asyncio_exception_hook",
    "DedupFilter",
    "BurstLimitFilter",
    "SecretRedactionFilter",
]
