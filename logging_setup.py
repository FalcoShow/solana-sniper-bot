# logging_setup.py
# stdlib-only structured logging helpers for high-throughput bots/services

from __future__ import annotations

import collections
import hashlib
import json
import logging
import os
import socket
import sys
import threading
import time
from io import TextIOBase
from typing import Any, Dict, Optional

# ──────────────────────────────────────────────────────────────────────────────
# JSON formatter
# ──────────────────────────────────────────────────────────────────────────────

_DEFAULT_EXCLUDE = {
    "name", "msg", "args", "levelname", "levelno", "pathname", "filename", "module",
    "exc_info", "exc_text", "stack_info", "lineno", "funcName", "created", "msecs",
    "relativeCreated", "thread", "threadName", "processName", "process", "asctime",
}

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

        # extras (from extra=...)
        extras = {k: v for k, v in record.__dict__.items() if k not in _DEFAULT_EXCLUDE}
        if extras:
            for k, v in extras.items():
                base[k] = _coerce(v)

        # message/event
        msg = record.getMessage()
        if "event" not in base and msg:
            base["msg"] = msg
        elif msg and msg != base.get("event"):
            base["msg"] = msg

        # quick source hint
        base.setdefault("source", f"{record.filename}:{record.lineno}")

        # exceptions
        if record.exc_info:
            etype = record.exc_info[0].__name__ if record.exc_info[0] else None
            base["exc_type"] = etype
            base["exc"] = self.formatException(record.exc_info)

        return json.dumps(base, separators=(",", ":"), ensure_ascii=False)

class JsonStreamHandler(logging.StreamHandler):
    def __init__(self, service: str, version: Optional[str] = None, stream=None):
        super().__init__(stream or sys.stdout)
        self.setFormatter(JsonFormatter(service=service, version=version))

# ──────────────────────────────────────────────────────────────────────────────
# Dedup / rate-limit filter
# ──────────────────────────────────────────────────────────────────────────────

class DedupFilter(logging.Filter):
    """
    Collapse identical messages for a window. When the message is allowed again,
    the emitted record gets `suppressed: N` if any were skipped.
    Uses monotonic time to be resilient to clock changes.
    """
    def __init__(self, window_sec: float = 20.0):
        super().__init__()
        self.window = float(window_sec)
        self._state: dict[tuple, tuple[float, int]] = {}  # key -> (next_ok_at, suppressed)

    def filter(self, record: logging.LogRecord) -> bool:
        event = getattr(record, "event", None)
        line = getattr(record, "line", None)
        key = (
            record.name,
            record.levelno,
            event,
            line if line is not None else record.getMessage(),
        )
        now = time.monotonic()
        next_ok_at, suppressed = self._state.get(key, (0.0, 0))
        if now >= next_ok_at:
            if suppressed:
                setattr(record, "suppressed", suppressed)
            self._state[key] = (now + self.window, 0)
            return True
        else:
            self._state[key] = (next_ok_at, suppressed + 1)
            return False

# ──────────────────────────────────────────────────────────────────────────────
# Logger with helpers
# ──────────────────────────────────────────────────────────────────────────────

class EventLogger(logging.Logger):
    """
    Convenience methods for high-volume bots:
      - .event(name, **fields)
      - .once(key, ttl, level, msg?, **fields)
      - .every(key, n, level, msg?, **fields)
      - .sample(key, rate, level, msg?, **fields)
    """
    _once_seen: dict[str, float] = {}
    _once_lock = threading.Lock()
    _every_counters: collections.Counter[str] = collections.Counter()
    _sample_counters: collections.Counter[str] = collections.Counter()

    def event(self, name: str, level: int = logging.INFO, **fields):
        fields.setdefault("event", name)
        self.log(level, name, extra=fields, stacklevel=2)

    def once(self, key: str, ttl: float = 30.0, level: int = logging.INFO, msg: str | None = None, **fields):
        now = time.monotonic()
        with self._once_lock:
            last = self._once_seen.get(key, 0.0)
            if now - last < ttl:
                return False
            self._once_seen[key] = now
        self.log(level, msg or fields.get("event") or key, extra=fields, stacklevel=2)
        return True

    def every(self, key: str, n: int = 10, level: int = logging.DEBUG, msg: str | None = None, **fields):
        self._every_counters[key] += 1
        if self._every_counters[key] % max(1, n) == 0:
            self.log(level, msg or fields.get("event") or key, extra=fields, stacklevel=2)

    def sample(self, key: str, rate: float = 0.1, level: int = logging.DEBUG, msg: str | None = None, **fields):
        """
        First occurrence always logs; subsequent ones log with probability=rate (deterministic-ish).
        """
        self._sample_counters[key] += 1
        i = self._sample_counters[key]
        if i == 1:
            self.log(level, msg or fields.get("event") or key, extra=fields, stacklevel=2)
            return
        h = hashlib.md5(f"{key}:{i}".encode()).digest()
        bucket = int.from_bytes(h[:2], "big") / 65535.0
        if bucket < max(0.0, min(1.0, rate)):
            self.log(level, msg or fields.get("event") or key, extra=fields, stacklevel=2)

# ──────────────────────────────────────────────────────────────────────────────
# Heartbeat counter
# ──────────────────────────────────────────────────────────────────────────────

class Heartbeat:
    """
    Lightweight counter you can bump from hot paths; emits a summary line
    every `interval` seconds with event name `name` (default: HEARTBEAT).
    """
    def __init__(self, logger: EventLogger, interval: float = 30.0, name: str = "HEARTBEAT"):
        self.log = logger
        self.interval = float(interval)
        self.name = name
        self._ctr = collections.Counter()
        self._lock = threading.Lock()
        self._t = threading.Thread(target=self._run, daemon=True)
        self._running = False

    def start(self):
        if not self._running:
            self._running = True
            self._t.start()
        return self

    def stop(self):
        self._running = False

    def bump(self, key: str, n: int = 1):
        with self._lock:
            self._ctr[key] += n

    def _run(self):
        while self._running:
            time.sleep(self.interval)
            snapshot = {}
            with self._lock:
                if self._ctr:
                    snapshot = dict(self._ctr)
                    self._ctr.clear()
            if snapshot:
                self.log.event(self.name, **snapshot)

# ──────────────────────────────────────────────────────────────────────────────
# Factory & setup
# ──────────────────────────────────────────────────────────────────────────────

_LOGGER_CLASS_SET = False
_global_hb: Optional[Heartbeat] = None

def _env_level() -> int:
    lvl = os.getenv("PF_LOG_LEVEL") or os.getenv("LOG_LEVEL") or "INFO"
    mapping = {
        "CRITICAL": logging.CRITICAL, "FATAL": logging.CRITICAL,
        "ERROR": logging.ERROR,
        "WARN": logging.WARNING, "WARNING": logging.WARNING,
        "INFO": logging.INFO,
        "DEBUG": logging.DEBUG,
        "TRACE": logging.DEBUG,  # alias
    }
    return mapping.get(lvl.upper(), logging.INFO)

def _env_service() -> str:
    return os.getenv("PF_SERVICE") or os.getenv("APP_SERVICE") or "sniper"

def get_logger(
    name: str = "sniper",
    level: Optional[int] = None,
    service: Optional[str] = None,
    version: Optional[str] = None,
    dedup_window_sec: Optional[float] = None,
) -> EventLogger:
    """
    Create or retrieve a JSON logger configured for stdout.
    Honors PF_LOG_LEVEL or LOG_LEVEL if `level` not provided.
    Set PF_LOG_DEDUP_WINDOW_SEC to tune the default dedup window (seconds).
    To auto-start heartbeat, set PF_HEARTBEAT_SEC (e.g., 30). Name via PF_HEARTBEAT_NAME.
    """
    global _LOGGER_CLASS_SET, _global_hb
    if not _LOGGER_CLASS_SET:
        logging.setLoggerClass(EventLogger)
        _LOGGER_CLASS_SET = True

    if level is None:
        level = _env_level()

    if service is None:
        service = _env_service()

    if dedup_window_sec is None:
        try:
            dedup_window_sec = float(os.getenv("PF_LOG_DEDUP_WINDOW_SEC", "20.0"))
        except Exception:
            dedup_window_sec = 20.0

    log: EventLogger = logging.getLogger(name)  # type: ignore[assignment]
    log.setLevel(level)
    log.propagate = False

    # Attach one JsonStreamHandler (idempotent)
    if not any(isinstance(h, JsonStreamHandler) for h in log.handlers):
        handler = JsonStreamHandler(service=service, version=version)
        handler.setLevel(level)
        if dedup_window_sec and dedup_window_sec > 0:
            handler.addFilter(DedupFilter(window_sec=dedup_window_sec))
        log.addHandler(handler)

    # Optional: auto-start global heartbeat if PF_HEARTBEAT_SEC is set (>0)
    hb_sec = os.getenv("PF_HEARTBEAT_SEC")
    if hb_sec and _global_hb is None:
        try:
            iv = float(hb_sec)
            if iv > 0:
                name_override = os.getenv("PF_HEARTBEAT_NAME", "HEARTBEAT")
                _global_hb = Heartbeat(log, interval=iv, name=name_override).start()
        except Exception:
            pass

    return log

def set_level(logger: logging.Logger, level_str: str):
    """Hot-switch log level (e.g., from a Telegram command)."""
    mapping = {
        "CRITICAL": logging.CRITICAL, "FATAL": logging.CRITICAL,
        "ERROR": logging.ERROR,
        "WARN": logging.WARNING, "WARNING": logging.WARNING,
        "INFO": logging.INFO,
        "DEBUG": logging.DEBUG,
        "TRACE": logging.DEBUG,
    }
    level = mapping.get(level_str.upper(), logging.INFO)
    os.environ["PF_LOG_LEVEL"] = level_str.upper()
    logger.setLevel(level)
    for h in logger.handlers:
        h.setLevel(level)
    if isinstance(logger, EventLogger):
        logger.event("LOG_LEVEL_CHANGED", level=level_str.upper())

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

    def flush(self):
        if self._buf.strip():
            self.logger.log(self.level, "STDIO", extra={"event": "STDIO", "line": self._buf.strip()}, stacklevel=3)
            self._buf = ""

_stdout_original = sys.stdout
_stderr_original = sys.stderr

def hook_stdio(logger: EventLogger, stdout_level: int = logging.INFO, stderr_level: int = logging.ERROR):
    """Route print()/stdout/stderr to structured logs. Call early in your process."""
    global _stdout_original, _stderr_original
    sys.stdout = _PrintToLogger(logger, stdout_level)  # type: ignore[assignment]
    sys.stderr = _PrintToLogger(logger, stderr_level)  # type: ignore[assignment]

def restore_stdio():
    """Undo hook_stdio (mainly useful for tests)."""
    global _stdout_original, _stderr_original
    sys.stdout = _stdout_original  # type: ignore[assignment]
    sys.stderr = _stderr_original  # type: ignore[assignment]

# ──────────────────────────────────────────────────────────────────────────────
# Uncaught exception hook
# ──────────────────────────────────────────────────────────────────────────────

def install_excepthook(logger: EventLogger):
    """Log uncaught exceptions as JSON (does not swallow them)."""
    def _hook(exc_type, exc, tb):
        logger.error("UNCAUGHT", exc_info=(exc_type, exc, tb))
        sys.__excepthook__(exc_type, exc, tb)
    sys.excepthook = _hook

# ──────────────────────────────────────────────────────────────────────────────
# Global heartbeat convenience
# ──────────────────────────────────────────────────────────────────────────────

def start_global_heartbeat(logger: EventLogger, interval: float = 30.0, name: str = "HEARTBEAT") -> Heartbeat:
    global _global_hb
    _global_hb = Heartbeat(logger, interval=interval, name=name).start()
    return _global_hb

def bump(key: str, n: int = 1):
    if _global_hb:
        _global_hb.bump(key, n)

__all__ = [
    "JsonFormatter",
    "JsonStreamHandler",
    "EventLogger",
    "get_logger",
    "set_level",
    "Heartbeat",
    "start_global_heartbeat",
    "bump",
    "hook_stdio",
    "restore_stdio",
    "install_excepthook",
    "DedupFilter",
]
