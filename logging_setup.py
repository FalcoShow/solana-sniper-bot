# logging_setup.py
# stdlib-only structured logging helpers for high-throughput bots/services
import os, sys, time, json, socket, hashlib, threading, collections, logging
from typing import Any, Dict

# ---------- JSON formatter ----------

DEFAULT_EXCLUDE = {
    "name","msg","args","levelname","levelno","pathname","filename","module",
    "exc_info","exc_text","stack_info","lineno","funcName","created","msecs",
    "relativeCreated","thread","threadName","processName","process","asctime"
}

def _coerce(obj: Any) -> Any:
    # Make extras JSON-safe without exploding on weird types
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
    def __init__(self, service: str, version: str | None = None):
        super().__init__()
        self.service = service
        self.version = version
        self.host = socket.gethostname()
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

        # Pull extras (anything added via extra=... or EventLogger helpers)
        extras = {k: v for k, v in record.__dict__.items() if k not in DEFAULT_EXCLUDE}
        if extras:
            for k, v in extras.items():
                base[k] = _coerce(v)

        # Message & event
        msg = record.getMessage()
        if "event" not in base and msg:
            # If caller used plain log.info("text"), keep it in msg
            base["msg"] = msg
        elif msg and msg != base.get("event"):
            # If caller used event(...), let "event" be primary and keep msg if different
            base["msg"] = msg

        # Source hints for quick grep (cheap)
        base.setdefault("source", f"{record.filename}:{record.lineno}")

        # Exceptions
        if record.exc_info:
            base["exc_type"] = record.exc_info[0].__name__ if record.exc_info[0] else None
            base["exc"] = self.formatException(record.exc_info)

        return json.dumps(base, separators=(",", ":"), ensure_ascii=False)

# ---------- Handler & Logger class with helpers ----------

class JsonStreamHandler(logging.StreamHandler):
    def __init__(self, service: str, version: str | None = None, stream=None):
        super().__init__(stream or sys.stdout)
        self.setFormatter(JsonFormatter(service=service, version=version))

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
        now = time.time()
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
        # Simple stable hash sampling
        h = hashlib.md5(f"{key}:{i}".encode()).digest()
        bucket = int.from_bytes(h[:2], "big") / 65535.0
        if bucket < max(0.0, min(1.0, rate)):
            self.log(level, msg or fields.get("event") or key, extra=fields, stacklevel=2)

# ---------- Heartbeat counter (optional) ----------

class Heartbeat:
    """
    Lightweight counter you can bump from hot paths; emits a summary line every `interval` seconds.
    """
    def __init__(self, logger: EventLogger, interval: float = 30.0, name: str = "HEARTBEAT"):
        self.log = logger
        self.interval = interval
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

# ---------- Factory & setup ----------

_LOGGER_CLASS_SET = False

def get_logger(
    name: str = "sniper",
    level: int | None = None,
    service: str | None = None,
    version: str | None = None,
) -> EventLogger:
    """
    Create or retrieve a JSON logger configured for stdout.
    Honors env PF_LOG_LEVEL or LOG_LEVEL if `level` not provided.
    """
    global _LOGGER_CLASS_SET
    if not _LOGGER_CLASS_SET:
        logging.setLoggerClass(EventLogger)
        _LOGGER_CLASS_SET = True

    if level is None:
        level = _env_level()

    if service is None:
        service = os.getenv("PF_SERVICE", "sniper")

    log: EventLogger = logging.getLogger(name)  # type: ignore[assignment]
    log.setLevel(level)

    # Attach one JsonStreamHandler (idempotent)
    if not any(isinstance(h, JsonStreamHandler) for h in log.handlers):
        handler = JsonStreamHandler(service=service, version=version)
        handler.setLevel(level)
        log.addHandler(handler)

    return log

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

def set_level(logger: logging.Logger, level_str: str):
    """Hot-switch log level (e.g., from a Telegram command)."""
    level = _env_to_level(level_str)
    logger.setLevel(level)
    for h in logger.handlers:
        h.setLevel(level)
    if isinstance(logger, EventLogger):
        logger.event("LOG_LEVEL_CHANGED", level=level_str.upper())

def _env_to_level(level_str: str) -> int:
    os.environ["PF_LOG_LEVEL"] = level_str
    return _env_level()

# ---------- Convenience: global heartbeat (optional) ----------

_global_hb: Heartbeat | None = None

def start_global_heartbeat(logger: EventLogger, interval: float = 30.0) -> Heartbeat:
    global _global_hb
    _global_hb = Heartbeat(logger, interval=interval).start()
    return _global_hb

def bump(key: str, n: int = 1):
    if _global_hb:
        _global_hb.bump(key, n)

# ---------- Exception hook (optional) ----------

def install_excepthook(logger: EventLogger):
    """Log uncaught exceptions as JSON (does not swallow them)."""
    def _hook(exc_type, exc, tb):
        logger.error("UNCAUGHT", exc_info=(exc_type, exc, tb))
        # Delegate to default hook so Render still marks exits properly
        sys.__excepthook__(exc_type, exc, tb)
    sys.excepthook = _hook
