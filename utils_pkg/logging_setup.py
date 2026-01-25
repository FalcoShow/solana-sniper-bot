from __future__ import annotations

"""
utils_pkg/logging_setup.py (COMPAT WRAPPER — NO DRIFT)
======================================================

LOCK-IN:
- Root logging_setup.py is the canonical logging system.
- This module exists ONLY to preserve backwards compatibility for legacy imports:
    from utils_pkg.logging_setup import get_logger, Heartbeat, hook_stdio, ...

What this wrapper provides
--------------------------
1) Calls root.configure_logging(...) as the single source of truth.
2) Installs optional global filters on the ROOT handler(s):
   - SecretRedactionFilter (best-effort DSN/token masking)
   - DedupFilter (drops repeated spam bursts, emits suppressed count)
   - BurstLimitFilter (caps logs/sec)
3) Exposes legacy helper ergonomics:
   - logger.event(name, **fields)
   - logger.bind(**ctx) -> bound logger
   - logger.once(key, ttl=...)
   - logger.every(key, n=...)
   - logger.sample(key, rate=...)
   - Heartbeat (threaded) + bump()
   - hook_stdio/restore_stdio (routes print() into structured logs)
   - install_excepthook / install_asyncio_exception_hook

IMPORTANT:
- This module does NOT add competing handlers.
- All output flows through root logging_setup.py.
"""

import collections
import contextlib
import hashlib
import logging
import os
import re
import sys
import threading
import time
from io import TextIOBase
from typing import Any

# Canonical root logging system (single source of truth)
import logging_setup as _root

# =============================================================================
# Env helpers (support both LOG_* and legacy PF_* knobs)
# =============================================================================

_LEVEL_MAP = {
    "CRITICAL": logging.CRITICAL,
    "FATAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARN": logging.WARNING,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
    "TRACE": logging.DEBUG,  # no TRACE in stdlib; map to DEBUG
}


def _env_str(*names: str, default: str = "") -> str:
    for n in names:
        v = os.getenv(n)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return default


def _env_bool(*names: str, default: bool = False) -> bool:
    v = _env_str(*names, default="")
    if v == "":
        return default
    s = v.strip().lower()
    return s in {"1", "true", "yes", "y", "on", "t"}


def _env_int(*names: str, default: int = 0) -> int:
    v = _env_str(*names, default="")
    if v == "":
        return default
    try:
        return int(float(v.replace("_", "")))
    except Exception:
        return default


def _env_float(*names: str, default: float = 0.0) -> float:
    v = _env_str(*names, default="")
    if v == "":
        return default
    try:
        return float(v.replace("_", ""))
    except Exception:
        return default


def _env_level() -> str | int:
    # Keep as str for root parser compatibility, but allow int too
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


# =============================================================================
# Extra sanitization (prevents LogRecord collisions on extra=)
# =============================================================================

_RESERVED_LOGRECORD_KEYS = set(logging.makeLogRecord({}).__dict__.keys())
_RESERVED_LOGRECORD_KEYS |= {"message", "asctime"}


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
# Global handler filters (applied ONCE to the root handler(s))
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
    if not val:
        return False
    v = val.strip()
    # Heuristic: long-ish mixed alpha+digit strings (base58/hex tokens)
    return len(v) >= 40 and any(c.isdigit() for c in v) and any(c.isalpha() for c in v)


def _redact_value(key: str, value: Any) -> Any:
    k = (key or "").strip()
    if isinstance(value, str):
        # URLs with creds
        if "://" in value and "@" in value and ":" in value:
            return _redact_dsn(value)
        # key indicates secret
        if _SENSITIVE_KEY_RE.search(k) and _looks_secretish(value):
            return value[:4] + "…" + "*" * 6
        # bearer inside strings
        low = value.lower()
        if "bearer " in low and len(value) > 20:
            parts = value.split()
            if len(parts) >= 2:
                parts[1] = parts[1][:4] + "…" + "*" * 6
                return " ".join(parts)
        return _redact_dsn(value)

    if isinstance(value, dict):
        return {str(kk): _redact_value(str(kk), vv) for kk, vv in value.items()}
    if isinstance(value, list | tuple):
        return [_redact_value(k, x) for x in value]
    return value


class SecretRedactionFilter(logging.Filter):
    """
    Best-effort redaction. Root logging_setup.py already redacts by key,
    but this also scrubs DSN-like patterns and obvious secret blobs.
    """

    def __init__(self, enabled: bool = True) -> None:
        super().__init__()
        self.enabled = bool(enabled)

    def filter(self, record: logging.LogRecord) -> bool:
        if not self.enabled:
            return True

        # Redact extras best-effort. Avoid touching exc_info tuple.
        for k, v in list(record.__dict__.items()):
            if k in ("exc_info", "stack_info"):
                continue
            if k.startswith("_"):
                continue
            try:
                record.__dict__[k] = _redact_value(k, v)
            except Exception:
                continue

        # Redact DSN-like patterns inside the message template too
        try:
            if isinstance(record.msg, str):
                record.msg = _redact_dsn(record.msg)
        except Exception:
            pass

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
        self._lru: collections.deque[tuple[Any, ...]] = collections.deque()

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
                self._lru.append(key)
            else:
                self._state[key] = (next_ok, suppressed + 1)
                self._lru.append(key)
                return False

            # Trim memory
            while len(self._state) > self.max_keys and self._lru:
                k = self._lru.popleft()
                # only delete if still present (LRU may contain duplicates)
                if k in self._state:
                    del self._state[k]
                    break

        return True


class BurstLimitFilter(logging.Filter):
    """
    Caps logs per second per key bucket (global/logger/level/event/logger+level).
    """

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


# =============================================================================
# One-time installation on root handler(s)
# =============================================================================

_WRAPPER_SETUP_LOCK = threading.Lock()
_WRAPPER_SETUP_DONE = False


def _iter_root_handlers() -> list[logging.Handler]:
    """
    Root logging may have 1+ handlers. We attach filters to all StreamHandlers.
    """
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        return []
    return list(root_logger.handlers)


def _install_root_filters_once() -> None:
    """
    Attach filters to the root handler(s) created by root.configure_logging.
    No parallel handlers, no competing pipelines.
    """
    handlers = _iter_root_handlers()
    if not handlers:
        return

    redact_enabled = _env_bool("PF_LOG_REDACT", "LOG_REDACT", default=True)

    burst_max = _env_int("PF_LOG_BURST_MAX_PER_SEC", "LOG_BURST_MAX_PER_SEC", default=200)
    burst_key = _env_str("PF_LOG_BURST_KEY", "LOG_BURST_KEY", default="logger+level")

    dedup_window = _env_float("PF_LOG_DEDUP_WINDOW_SEC", "LOG_DEDUP_WINDOW_SEC", default=20.0)
    dedup_max_keys = _env_int("PF_LOG_DEDUP_MAX_KEYS", "LOG_DEDUP_MAX_KEYS", default=20_000)

    for h in handlers:
        existing = {type(f) for f in getattr(h, "filters", [])}

        if SecretRedactionFilter not in existing:
            h.addFilter(SecretRedactionFilter(enabled=redact_enabled))

        if burst_max > 0 and BurstLimitFilter not in existing:
            h.addFilter(BurstLimitFilter(max_per_sec=burst_max, key_mode=burst_key))

        if dedup_window > 0 and DedupFilter not in existing:
            h.addFilter(DedupFilter(window_sec=dedup_window, max_keys=dedup_max_keys))


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
) -> None:
    """
    Compatibility configure. Delegates to root, then installs global filters.
    """
    lvl = level if level is not None else _env_level()
    jl = json_logs if json_logs is not None else _env_json_logs()
    utc = use_utc if use_utc is not None else _env_use_utc()
    src = include_src if include_src is not None else _env_include_src()
    svc = service if service is not None else _env_service()
    ver = version if version is not None else _env_version()

    _root.configure_logging(
        level=lvl,
        json_logs=jl,
        use_utc=utc,
        include_src=src,
        service=svc,
        version=ver,
        env=env,
        force=force,
    )

    global _WRAPPER_SETUP_DONE
    with _WRAPPER_SETUP_LOCK:
        if not _WRAPPER_SETUP_DONE or force:
            _install_root_filters_once()
            _WRAPPER_SETUP_DONE = True


# =============================================================================
# Logger adapters (event/once/every/sample/bind ergonomics)
# =============================================================================

_ONCE_LOCK = threading.Lock()
_ONCE_SEEN: dict[str, float] = {}

_EVERY_LOCK = threading.Lock()
_EVERY_CTR: collections.Counter[str] = collections.Counter()

_SAMPLE_LOCK = threading.Lock()
_SAMPLE_CTR: collections.Counter[str] = collections.Counter()


class EventLogger:
    """
    Thin wrapper around stdlib logging.Logger that:
    - Sanitizes extra keys to avoid LogRecord collisions
    - Provides convenience helpers used across the repo
    """

    def __init__(self, logger: logging.Logger):
        self._log = logger

    @property
    def raw(self) -> logging.Logger:
        return self._log

    def __getattr__(self, name: str) -> Any:
        return getattr(self._log, name)

    def _safe_log(
        self,
        level: int,
        msg: str,
        *,
        extra: dict[str, Any] | None = None,
        exc_info: Any = None,
    ) -> None:
        ex = _sanitize_extra(extra)
        # stacklevel makes caller location correct (py3.8+; best on 3.11+)
        try:
            self._log.log(level, msg, extra=ex, exc_info=exc_info, stacklevel=3)
        except TypeError:
            # older python might not support stacklevel here
            self._log.log(level, msg, extra=ex, exc_info=exc_info)

    def bind(self, **ctx: Any) -> BoundLogger:
        return BoundLogger(self, ctx)

    def event(self, name: str, level: int = logging.INFO, **fields: Any) -> None:
        fields.setdefault("event", name)
        self._safe_log(level, name, extra=fields)

    def once(
        self,
        key: str,
        ttl: float = 30.0,
        level: int = logging.INFO,
        msg: str | None = None,
        **fields: Any,
    ) -> bool:
        now = time.monotonic()
        with _ONCE_LOCK:
            last = _ONCE_SEEN.get(key, 0.0)
            if now - last < float(ttl):
                return False
            _ONCE_SEEN[key] = now
        self._safe_log(level, msg or key, extra=fields)
        return True

    def every(
        self,
        key: str,
        n: int = 10,
        level: int = logging.DEBUG,
        msg: str | None = None,
        **fields: Any,
    ) -> None:
        n = max(1, int(n))
        with _EVERY_LOCK:
            _EVERY_CTR[key] += 1
            i = _EVERY_CTR[key]
        if i % n == 0:
            self._safe_log(level, msg or key, extra=fields)

    def sample(
        self,
        key: str,
        rate: float = 0.1,
        level: int = logging.DEBUG,
        msg: str | None = None,
        **fields: Any,
    ) -> None:
        r = max(0.0, min(1.0, float(rate)))
        with _SAMPLE_LOCK:
            _SAMPLE_CTR[key] += 1
            i = _SAMPLE_CTR[key]

        if i == 1:
            self._safe_log(level, msg or key, extra=fields)
            return

        h = hashlib.md5(f"{key}:{i}".encode()).digest()
        bucket = int.from_bytes(h[:2], "big") / 65535.0
        if bucket < r:
            self._safe_log(level, msg or key, extra=fields)

    # Convenience: legacy style logger.info("msg", foo=1)
    def debug(self, msg: str, **fields: Any) -> None:
        self._safe_log(logging.DEBUG, msg, extra=fields)

    def info(self, msg: str, **fields: Any) -> None:
        self._safe_log(logging.INFO, msg, extra=fields)

    def warning(self, msg: str, **fields: Any) -> None:
        self._safe_log(logging.WARNING, msg, extra=fields)

    def error(self, msg: str, **fields: Any) -> None:
        self._safe_log(logging.ERROR, msg, extra=fields)

    def exception(self, msg: str, **fields: Any) -> None:
        self._safe_log(logging.ERROR, msg, extra=fields, exc_info=True)


class BoundLogger:
    __slots__ = ("_base", "_ctx")

    def __init__(self, base: EventLogger, ctx: dict[str, Any]):
        self._base = base
        self._ctx = dict(ctx or {})

    def bind(self, **more: Any) -> BoundLogger:
        nxt = dict(self._ctx)
        nxt.update(more)
        return BoundLogger(self._base, nxt)

    def _merged(self, fields: dict[str, Any]) -> dict[str, Any]:
        out = dict(self._ctx)
        out.update(fields)
        return out

    def event(self, name: str, level: int = logging.INFO, **fields: Any) -> None:
        self._base.event(name, level=level, **self._merged(fields))

    def once(
        self,
        key: str,
        ttl: float = 30.0,
        level: int = logging.INFO,
        msg: str | None = None,
        **fields: Any,
    ) -> bool:
        return self._base.once(key, ttl=ttl, level=level, msg=msg, **self._merged(fields))

    def every(
        self,
        key: str,
        n: int = 10,
        level: int = logging.DEBUG,
        msg: str | None = None,
        **fields: Any,
    ) -> None:
        self._base.every(key, n=n, level=level, msg=msg, **self._merged(fields))

    def sample(
        self,
        key: str,
        rate: float = 0.1,
        level: int = logging.DEBUG,
        msg: str | None = None,
        **fields: Any,
    ) -> None:
        self._base.sample(key, rate=rate, level=level, msg=msg, **self._merged(fields))

    def debug(self, msg: str, **fields: Any) -> None:
        self._base.debug(msg, **self._merged(fields))

    def info(self, msg: str, **fields: Any) -> None:
        self._base.info(msg, **self._merged(fields))

    def warning(self, msg: str, **fields: Any) -> None:
        self._base.warning(msg, **self._merged(fields))

    def error(self, msg: str, **fields: Any) -> None:
        self._base.error(msg, **self._merged(fields))

    def exception(self, msg: str, **fields: Any) -> None:
        self._base.exception(msg, **self._merged(fields))


# =============================================================================
# Heartbeat (hot-path safe)
# =============================================================================


class Heartbeat:
    def __init__(self, logger: EventLogger, interval: float = 30.0, name: str = "HEARTBEAT"):
        self.log = logger
        self.interval = float(interval)
        self.name = str(name)
        self._ctr = collections.Counter()
        self._lock = threading.Lock()
        self._t = threading.Thread(target=self._run, daemon=True)
        self._running = False

    def start(self) -> Heartbeat:
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
                self.log.event(self.name, **snap)


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
# Stdout/stderr capture (route print() to JSON logs)
# =============================================================================

_stdout_original = sys.stdout
_stderr_original = sys.stderr


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
                self.logger._safe_log(self.level, "STDIO", extra={"event": "STDIO", "line": line})
        return len(s)

    def flush(self) -> None:
        if self._buf.strip():
            self.logger._safe_log(
                self.level, "STDIO", extra={"event": "STDIO", "line": self._buf.strip()}
            )
            self._buf = ""


def hook_stdio(
    logger: EventLogger, stdout_level: int = logging.INFO, stderr_level: int = logging.ERROR
) -> None:
    global _stdout_original, _stderr_original
    _stdout_original = sys.stdout
    _stderr_original = sys.stderr
    sys.stdout = _PrintToLogger(logger, stdout_level)  # type: ignore[assignment]
    sys.stderr = _PrintToLogger(logger, stderr_level)  # type: ignore[assignment]


def restore_stdio() -> None:
    global _stdout_original, _stderr_original
    sys.stdout = _stdout_original  # type: ignore[assignment]
    sys.stderr = _stderr_original  # type: ignore[assignment]


# =============================================================================
# Exception hooks
# =============================================================================


def install_excepthook(logger: EventLogger) -> None:
    def _hook(exc_type, exc, tb):
        logger._safe_log(
            logging.ERROR, "UNCAUGHT", extra={"event": "UNCAUGHT"}, exc_info=(exc_type, exc, tb)
        )
        sys.__excepthook__(exc_type, exc, tb)

    sys.excepthook = _hook


def install_asyncio_exception_hook(logger: EventLogger, loop=None) -> None:
    """
    Logs asyncio loop exceptions as structured events.
    """
    try:
        import asyncio

        lp = loop or asyncio.get_event_loop()
    except Exception:
        return

    def _handler(_lp, context: dict[str, Any]) -> None:
        fields = dict(context or {})
        fields.setdefault("event", "ASYNCIO_EXCEPTION")

        # asyncio gives "exception": Exception and "message": str
        exc = fields.get("exception")
        # avoid reserved collisions: move common keys
        if "message" in fields:
            fields["asyncio_message"] = fields.pop("message")

        if isinstance(exc, BaseException):
            logger._safe_log(logging.ERROR, "ASYNCIO_EXCEPTION", extra=fields, exc_info=True)
        else:
            logger._safe_log(logging.ERROR, "ASYNCIO_EXCEPTION", extra=fields)

    with contextlib.suppress(Exception):
        lp.set_exception_handler(_handler)


# =============================================================================
# Public: get_logger + set_level + context passthrough
# =============================================================================


def get_logger(
    name: str = "sniper",
    level: str | int | None = None,
    service: str | None = None,
    version: str | None = None,
    dedup_window_sec: float | None = None,  # legacy signature: optional override
) -> EventLogger:
    """
    Legacy entrypoint. Ensures root logging is configured and returns an EventLogger adapter.
    """
    configure_logging(
        level=level if level is not None else _env_level(),
        service=service if service is not None else _env_service(),
        version=version if version is not None else _env_version(),
        json_logs=_env_json_logs(),
        use_utc=_env_use_utc(),
        include_src=_env_include_src(),
        force=False,
    )

    # Optional per-process override
    if dedup_window_sec is not None:
        os.environ["LOG_DEDUP_WINDOW_SEC"] = str(float(dedup_window_sec))
        os.environ["PF_LOG_DEDUP_WINDOW_SEC"] = str(float(dedup_window_sec))
        _install_root_filters_once()

    lg = _root.get_logger(name)  # stdlib logger configured by root
    return EventLogger(lg)


def set_level(logger: logging.Logger, level_str: str) -> None:
    s = (level_str or "INFO").upper()
    os.environ["PF_LOG_LEVEL"] = s
    os.environ["LOG_LEVEL"] = s
    lvl = _LEVEL_MAP.get(s, logging.INFO)
    logging.getLogger().setLevel(lvl)
    with contextlib.suppress(Exception):
        logger.setLevel(lvl)


# contextvars passthrough to root canonical
set_log_context = _root.set_log_context
clear_log_context = _root.clear_log_context
get_log_context = _root.get_log_context


__all__ = [
    "BoundLogger",
    "BurstLimitFilter",
    "DedupFilter",
    # compat surface
    "EventLogger",
    "Heartbeat",
    # filters
    "SecretRedactionFilter",
    "bump",
    "clear_log_context",
    # canonical passthrough (root)
    "configure_logging",
    "get_log_context",
    "get_logger",
    "hook_stdio",
    "install_asyncio_exception_hook",
    "install_excepthook",
    "restore_stdio",
    "set_level",
    "set_log_context",
    "start_global_heartbeat",
]
