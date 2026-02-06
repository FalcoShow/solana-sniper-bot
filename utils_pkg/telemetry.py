from __future__ import annotations

"""
utils_pkg/telemetry.py — V3+++ (God-Tier Overhaul)
==================================================

Purpose
-------
Unified, secret-safe telemetry + audit logging helpers.

Requirements (God-Tier contract)
--------------------------------
- Safe: never raises *new* errors to callers (never crashes trading loop)
- Non-blocking: no network at import time; logging is best-effort
- Secret-safe: redacts tokens/DSNs/secrets in BOTH message + fields
- Predictable: no randomness; IDs derive from process seed + monotonic counter, hashed
- Lightweight: stdlib-only by default; integrates with your logging_setup/log_compat if present

Env (do not add new env vars here — contract stability)
-------------------------------------------------------
TELEM_ENABLED             = 0|1 (default 1)
TELEM_MAX_FIELD_STR       = int (default 4096)
TELEM_MAX_DEPTH           = int (default 6)
TELEM_COMPONENT           = component name (default "telemetry")
"""

import asyncio
import contextlib
import hashlib
import logging
import os
import threading
import time
from collections.abc import AsyncIterator, Iterator, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final, Protocol

# -----------------------------------------------------------------------------
# Canonical logger contract (Phase A)
# -----------------------------------------------------------------------------
# When running mypy on ".", we want EventLogger to be EXACTLY the canonical type.
# But at runtime we still want to avoid hard dependencies and import-time side effects.
if TYPE_CHECKING:
    from utils_pkg.log_compat import EventLogger as EventLogger  # noqa: F401
else:

    class EventLogger(Protocol):
        def debug(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
        def info(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
        def warning(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
        def error(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
        def exception(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
        def log(self, level: int, msg: str, *args: Any, **kwargs: Any) -> None: ...

        # Canonical extras used across your codebase
        def event(
            self,
            name: str,
            *,
            msg: str | None = ...,
            level: str | int = ...,
            **fields: Any,
        ) -> None: ...

        def every(self, key: str, *args: Any, **kwargs: Any) -> bool: ...


class _EventLoggerAdapter:
    """
    Adapter that makes ANY logger (logging_setup.EventLogger, stdlib Logger, etc.)
    conform to the canonical utils_pkg.log_compat.EventLogger structural contract.

    Key fixes:
    - event(): provide canonical signature (name: str, *, msg=..., level=..., **fields)
      and delegate safely even if underlying has positional-only `name`.
    - every(): provide throttling gate when underlying lacks it or signature mismatches.
    """

    __slots__ = ("_base", "_every_last")

    def __init__(self, base: Any) -> None:
        self._base = base
        self._every_last: dict[str, float] = {}

    def __getattr__(self, name: str) -> Any:
        # Preserve extra capabilities (bind/child/etc) on richer loggers.
        return getattr(self._base, name)

    # ---- basic logging passthrough ----
    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        fn = getattr(self._base, "debug", None)
        if callable(fn):
            fn(msg, *args, **kwargs)

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        fn = getattr(self._base, "info", None)
        if callable(fn):
            fn(msg, *args, **kwargs)

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        fn = getattr(self._base, "warning", None)
        if callable(fn):
            fn(msg, *args, **kwargs)

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        fn = getattr(self._base, "error", None)
        if callable(fn):
            fn(msg, *args, **kwargs)

    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None:
        fn = getattr(self._base, "exception", None)
        if callable(fn):
            fn(msg, *args, **kwargs)

    def log(self, level: int, msg: str, *args: Any, **kwargs: Any) -> None:
        fn = getattr(self._base, "log", None)
        if callable(fn):
            fn(level, msg, *args, **kwargs)
            return
        # fallback if base doesn't implement log()
        if level >= logging.ERROR:
            self.error(msg, *args, **kwargs)
        elif level >= logging.WARNING:
            self.warning(msg, *args, **kwargs)
        else:
            self.info(msg, *args, **kwargs)

    # ---- canonical structured methods ----
    def event(
        self,
        name: str,
        *,
        msg: str | None = None,
        level: str | int = "INFO",
        **fields: Any,
    ) -> None:
        """
        Canonical signature (matches utils_pkg.log_compat.EventLogger).

        If the underlying logger implements event(), we delegate.
        If not, we fallback to .log/.info with a structured line.
        """
        fn = getattr(self._base, "event", None)
        if callable(fn):
            try:
                # Works for BOTH:
                # - canonical event(name: str, *, msg=..., level=..., **fields)
                # - legacy event(self, str, /, msg=..., level=..., **fields)  (positional-only name)
                fn(name, msg=msg, level=level, **fields)
                return
            except TypeError:
                # Try minimal positional call in case keyword-only differs
                with contextlib.suppress(Exception):
                    fn(name, **fields)
                    return
            except Exception:
                # Do not raise new errors
                return

        # Fallback: degrade to human-readable structured log line.
        try:
            payload = {"event": name, **fields}
            line = f"{name} {payload}" if not msg else f"{name} {msg} {payload}"
            self._log_by_level(level, line)
        except Exception:
            return

    def every(self, key: str, *args: Any, **kwargs: Any) -> bool:
        """
        Throttle gate. If underlying has a compatible every(), delegate.
        Otherwise implement deterministic monotonic throttling.

        Supports common patterns via kwargs:
          - seconds=..., sec=..., interval_s=..., every_s=...
        Default: seconds=60.0
        """
        fn = getattr(self._base, "every", None)
        if callable(fn):
            try:
                out = fn(key, *args, **kwargs)
                return bool(out)
            except Exception:
                # fall back to local gate
                pass

        seconds = 60.0
        for k in ("seconds", "sec", "interval_s", "every_s"):
            v = kwargs.get(k)
            if isinstance(v, (int, float)) and float(v) > 0:
                seconds = float(v)
                break

        now = time.monotonic()
        last = self._every_last.get(str(key))
        if last is None or (now - last) >= seconds:
            self._every_last[str(key)] = now
            return True
        return False

    def _log_by_level(self, level: str | int, msg: str) -> None:
        try:
            if isinstance(level, int):
                self.log(level, msg)
                return
            lv = str(level).upper().strip()
            if lv in {"CRITICAL", "FATAL"}:
                self.error(msg)
            elif lv == "ERROR":
                self.error(msg)
            elif lv in {"WARN", "WARNING"}:
                self.warning(msg)
            elif lv == "DEBUG":
                self.debug(msg)
            else:
                self.info(msg)
        except Exception:
            return


def _as_event_logger(obj: Any) -> EventLogger:
    # Always return an adapter to satisfy the canonical structural type in mypy (.)
    return _EventLoggerAdapter(obj)  # type: ignore[return-value]


# -----------------------------------------------------------------------------
# Redaction (prefer utils_pkg.log_redaction; fallback is ultra-safe)
# -----------------------------------------------------------------------------
try:
    from utils_pkg.log_redaction import (  # type: ignore
        redact_obj as _redact_obj,
        redact_text as _redact_text,
    )
except Exception:  # pragma: no cover
    _redact_obj = None  # type: ignore[assignment]
    _redact_text = None  # type: ignore[assignment]

# -----------------------------------------------------------------------------
# Env parsing (contract-stable)
# -----------------------------------------------------------------------------
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


TELEM_ENABLED: bool = _envb("TELEM_ENABLED", True)
TELEM_MAX_FIELD_STR: int = _envi("TELEM_MAX_FIELD_STR", 4096)
TELEM_MAX_DEPTH: int = _envi("TELEM_MAX_DEPTH", 6)
TELEM_COMPONENT: str = _envs("TELEM_COMPONENT", "telemetry")


# -----------------------------------------------------------------------------
# get_logger (returns canonical EventLogger ALWAYS, via adapter on fallbacks)
# -----------------------------------------------------------------------------
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
    Canonical logger getter wrapper (single stable signature).

    Order:
      1) utils_pkg.log_compat.get_logger (canonical) -> returned directly
      2) logging_setup.get_logger (project root) -> wrapped to canonical
      3) utils_pkg.logging_setup.get_logger (legacy) -> wrapped to canonical
      4) stdlib logging.Logger -> wrapped to canonical

    Must never raise.
    """
    nm = (name or "telemetry").strip() or "telemetry"

    # 1) Canonical compat layer
    with contextlib.suppress(Exception):
        from utils_pkg.log_compat import get_logger as _gl  # type: ignore[attr-defined]

        try:
            return _gl(
                nm,
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
        except TypeError:
            return _gl(nm)

    # 2) Project helper (wrap)
    with contextlib.suppress(Exception):
        from logging_setup import get_logger as _gl2  # type: ignore[import-not-found]

        try:
            return _as_event_logger(_gl2(nm))
        except TypeError:
            return _as_event_logger(_gl2())  # type: ignore[call-arg]

    # 3) Legacy compat location (wrap)
    with contextlib.suppress(Exception):
        from utils_pkg.logging_setup import get_logger as _gl3  # type: ignore[import-not-found]

        try:
            return _as_event_logger(_gl3(nm))
        except TypeError:
            return _as_event_logger(_gl3())  # type: ignore[call-arg]

    # 4) Last resort: stdlib logging (wrap)
    lg = logging.getLogger(nm)
    if not lg.handlers:
        logging.basicConfig(level=logging.INFO)
    return _as_event_logger(lg)


_LOG: EventLogger = get_logger(__name__, service=TELEM_COMPONENT)


# -----------------------------------------------------------------------------
# Deterministic-ish ID factory (no randomness)
# -----------------------------------------------------------------------------
class _IdFactory:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._ctr = 0
        self._seed = f"{os.getpid()}:{time.time_ns()}".encode("utf-8", "ignore")

    def new(self, prefix: str = "") -> str:
        try:
            with self._lock:
                self._ctr += 1
                ctr = self._ctr
            raw = (
                self._seed
                + b"|"
                + prefix.encode("utf-8", "ignore")
                + b"|"
                + str(ctr).encode("ascii", "ignore")
            )
            return hashlib.sha256(raw).hexdigest()[:16]
        except Exception:
            return f"{int(time.time() * 1000):x}"[:16]


_IDS = _IdFactory()


def _new_id(prefix: str = "") -> str:
    return _IDS.new(prefix)


# -----------------------------------------------------------------------------
# Context (prefer contextvars; fallback to thread-local)
# -----------------------------------------------------------------------------
try:
    import contextvars

    _trace_id_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
        "telemetry_trace_id", default=None
    )
    _span_id_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
        "telemetry_span_id", default=None
    )
    _ctx_fields_var: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar(
        "telemetry_ctx_fields", default=None
    )
    _HAS_CTXVARS = True
except Exception:  # pragma: no cover
    _trace_id_var = None  # type: ignore[assignment]
    _span_id_var = None  # type: ignore[assignment]
    _ctx_fields_var = None  # type: ignore[assignment]
    _HAS_CTXVARS = False

_thread_local = threading.local()


def _tl_get(name: str) -> Any:
    try:
        return getattr(_thread_local, name, None)
    except Exception:
        return None


def _tl_set(name: str, value: Any) -> None:
    try:
        setattr(_thread_local, name, value)
    except Exception:
        return


def get_trace_id() -> str:
    try:
        if _HAS_CTXVARS and _trace_id_var is not None:
            return str(_trace_id_var.get() or "")
        return str(_tl_get("trace_id") or "")
    except Exception:
        return ""


def get_span_id() -> str:
    try:
        if _HAS_CTXVARS and _span_id_var is not None:
            return str(_span_id_var.get() or "")
        return str(_tl_get("span_id") or "")
    except Exception:
        return ""


def _get_ctx_fields() -> dict[str, Any]:
    try:
        if _HAS_CTXVARS and _ctx_fields_var is not None:
            cur = _ctx_fields_var.get()
            return dict(cur) if isinstance(cur, dict) else {}
        cur2 = _tl_get("ctx_fields")
        return dict(cur2) if isinstance(cur2, dict) else {}
    except Exception:
        return {}


@contextlib.contextmanager
def bind_context(**fields: Any) -> Iterator[None]:
    token: Any = None
    prev: Any = None
    try:
        merged = _get_ctx_fields()
        merged.update(fields)

        if _HAS_CTXVARS and _ctx_fields_var is not None:
            token = _ctx_fields_var.set(merged)
            yield
            return

        prev = _tl_get("ctx_fields")
        _tl_set("ctx_fields", merged)
        yield
    except Exception:
        yield
    finally:
        if _HAS_CTXVARS and token is not None and _ctx_fields_var is not None:
            with contextlib.suppress(Exception):
                _ctx_fields_var.reset(token)
        else:
            _tl_set("ctx_fields", prev)


@contextlib.contextmanager
def with_trace(trace_id: str | None = None) -> Iterator[str]:
    token: Any = None
    prev: Any = None
    tid = ""
    try:
        tid = str(trace_id).strip() if trace_id else ""
        if not tid:
            tid = get_trace_id() or _new_id("trace")

        if _HAS_CTXVARS and _trace_id_var is not None:
            token = _trace_id_var.set(tid)
            yield tid
            return

        prev = _tl_get("trace_id")
        _tl_set("trace_id", tid)
        yield tid
    except Exception:
        yield tid
    finally:
        if _HAS_CTXVARS and token is not None and _trace_id_var is not None:
            with contextlib.suppress(Exception):
                _trace_id_var.reset(token)
        else:
            _tl_set("trace_id", prev)


# -----------------------------------------------------------------------------
# Redaction helpers
# -----------------------------------------------------------------------------
def redact_text(text: str) -> str:
    try:
        if callable(_redact_text):
            out = _redact_text(text)  # type: ignore[misc]
            return out if isinstance(out, str) else str(out)
    except Exception:
        pass

    try:
        s = "" if text is None else str(text)
        cap = max(256, int(TELEM_MAX_FIELD_STR))
        if len(s) > cap:
            s = s[:cap] + "…"
        return s
    except Exception:
        return "***REDACTED***"


def redact_obj(obj: Any, *, max_depth: int = 6, max_str_len: int = 4096) -> Any:
    try:
        if callable(_redact_obj):
            return _redact_obj(obj, max_depth=max_depth, max_str_len=max_str_len)  # type: ignore[misc]
    except Exception:
        pass

    try:
        return redact_text(str(obj))[: max(1, int(max_str_len))]
    except Exception:
        return "***REDACTED***"


# -----------------------------------------------------------------------------
# Core event emission
# -----------------------------------------------------------------------------
def _now_iso() -> str:
    try:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    except Exception:
        return ""


def _base_fields() -> dict[str, Any]:
    out: dict[str, Any] = {"ts": _now_iso(), "component": TELEM_COMPONENT}

    tid = get_trace_id()
    sid = get_span_id()
    if tid:
        out["trace_id"] = tid
    if sid:
        out["span_id"] = sid

    ctx = _get_ctx_fields()
    if ctx:
        out.update(ctx)
    return out


def _emit(ev_name: str, fields: Mapping[str, Any]) -> None:
    # Best-effort; never raises.
    try:
        _LOG.event(ev_name, **dict(fields))
        return
    except Exception:
        pass

    with contextlib.suppress(Exception):
        _LOG.info("%s %s", redact_text(ev_name), dict(fields))


def event(name: str, **fields: Any) -> None:
    if not TELEM_ENABLED:
        return
    try:
        ev_name = (name or "EVENT").strip()
        merged = _base_fields()
        if fields:
            merged.update(fields)

        safe = redact_obj(
            merged,
            max_depth=max(1, int(TELEM_MAX_DEPTH)),
            max_str_len=max(256, int(TELEM_MAX_FIELD_STR)),
        )
        safe_fields: dict[str, Any] = safe if isinstance(safe, dict) else {"value": safe}
        _emit(ev_name, safe_fields)
    except Exception:
        return


def exception(name: str, err: BaseException, **fields: Any) -> None:
    try:
        event(
            name or "EXCEPTION",
            ok=False,
            err_type=type(err).__name__,
            err=redact_text(str(err)),
            **fields,
        )
    except Exception:
        return


# -----------------------------------------------------------------------------
# Spans (timing)
# -----------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class Span:
    name: str
    start_mono: float
    span_id: str
    parent_span_id: str
    trace_id: str

    def finish(self, ok: bool = True, **fields: Any) -> None:
        try:
            dur_ms = (time.monotonic() - float(self.start_mono)) * 1000.0
            event(
                "SPAN",
                span_name=self.name,
                ok=bool(ok),
                dur_ms=round(dur_ms, 3),
                span_id=self.span_id,
                parent_span_id=self.parent_span_id or "",
                trace_id=self.trace_id or "",
                **fields,
            )
        except Exception:
            return


@contextlib.contextmanager
def span(name: str, **fields: Any) -> Iterator[Span]:
    span_token: Any = None
    trace_token: Any = None
    prev_span: Any = None
    prev_trace: Any = None

    sp: Span | None = None
    nm = (name or "SPAN").strip()

    try:
        tid = get_trace_id()
        if not tid:
            tid = _new_id("trace")
            if _HAS_CTXVARS and _trace_id_var is not None:
                trace_token = _trace_id_var.set(tid)
            else:
                prev_trace = _tl_get("trace_id")
                _tl_set("trace_id", tid)

        parent = get_span_id()
        sid = _new_id("span")

        if _HAS_CTXVARS and _span_id_var is not None:
            span_token = _span_id_var.set(sid)
        else:
            prev_span = _tl_get("span_id")
            _tl_set("span_id", sid)

        sp = Span(
            name=nm,
            start_mono=time.monotonic(),
            span_id=sid,
            parent_span_id=parent,
            trace_id=tid,
        )

        event(
            "SPAN_START",
            span_name=sp.name,
            span_id=sp.span_id,
            parent_span_id=parent or "",
            trace_id=tid or "",
            **fields,
        )

        yield sp
        sp.finish(ok=True)
    except BaseException as err:  # noqa: BLE001
        if isinstance(err, asyncio.CancelledError):
            with contextlib.suppress(Exception):
                exception("SPAN_CANCEL", err, span_name=nm, **fields)
            raise

        with contextlib.suppress(Exception):
            exception("SPAN_ERR", err, span_name=nm, **fields)
        with contextlib.suppress(Exception):
            if sp is not None:
                sp.finish(ok=False)
        raise
    finally:
        if _HAS_CTXVARS and span_token is not None and _span_id_var is not None:
            with contextlib.suppress(Exception):
                _span_id_var.reset(span_token)
        else:
            _tl_set("span_id", prev_span)

        if _HAS_CTXVARS and trace_token is not None and _trace_id_var is not None:
            with contextlib.suppress(Exception):
                _trace_id_var.reset(trace_token)
        else:
            _tl_set("trace_id", prev_trace)


@contextlib.asynccontextmanager
async def aspan(name: str, **fields: Any) -> AsyncIterator[Span]:
    with span(name, **fields) as sp:
        yield sp


# -----------------------------------------------------------------------------
# Shutdown hooks (called by utils_pkg/shutdown.py)
# -----------------------------------------------------------------------------
def flush() -> None:
    try:
        fn = getattr(_LOG, "flush", None)
        if callable(fn):
            fn()
    except Exception:
        return


def shutdown() -> None:
    try:
        fn = getattr(_LOG, "shutdown", None)
        if callable(fn):
            fn()
    except Exception:
        return


__all__ = [
    "TELEM_COMPONENT",
    "TELEM_ENABLED",
    "TELEM_MAX_DEPTH",
    "TELEM_MAX_FIELD_STR",
    "Span",
    "aspan",
    "bind_context",
    "event",
    "exception",
    "flush",
    "get_logger",
    "get_span_id",
    "get_trace_id",
    "redact_obj",
    "redact_text",
    "shutdown",
    "span",
    "with_trace",
]
