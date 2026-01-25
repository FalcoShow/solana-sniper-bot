# utils_pkg/log_compat.py
from __future__ import annotations

import logging
from functools import lru_cache
from typing import Any, Mapping


@lru_cache(maxsize=1)
def _reserved_logrecord_keys() -> set[str]:
    """
    Keys that cannot be written via logging's `extra=` because they collide with
    LogRecord attributes and raise KeyError (e.g. "module").
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
    # common computed/formatter keys
    keys.update({"message", "asctime"})
    return keys


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


def _emit_event(logger: Any, name: str, fields: Mapping[str, Any]) -> None:
    """
    Best-effort event emission. Never raises.
    Supports:
      - structlog-style: logger.info(msg, **fields)
      - stdlib logging:  logger.info(msg, extra={...})
      - fallback:        logger.info("msg | {fields}")
    """
    msg = str(name) if name else "event"

    # No fields: simplest path
    if not fields:
        try:
            logger.info(msg)
        except Exception:
            return
        return

    # 1) Try structlog-style (kwargs)
    try:
        logger.info(msg, **dict(fields))
        return
    except TypeError:
        # stdlib logging doesn't accept arbitrary kwargs
        pass
    except Exception:
        # never crash runtime due to logging
        pass

    # 2) Try stdlib logging-style `extra=...` with sanitization
    try:
        extra = _safe_extra({"event": msg, **dict(fields)})
        logger.info(msg, extra=extra)
        return
    except TypeError:
        # logger.info doesn't accept extra
        pass
    except Exception:
        pass

    # 3) Plain text fallback
    try:
        logger.info(f"{msg} | {dict(fields)}")
    except Exception:
        return


def install_stdlib_event() -> None:
    """
    Install `.event(name, **fields)` onto `logging.Logger` if missing.
    Safe to call multiple times.
    """
    if hasattr(logging.Logger, "event"):
        return

    def _event(self: logging.Logger, name: str, **fields: Any) -> None:
        _emit_event(self, name, fields)

    # Assign directly (ruff-safe) rather than setattr("event", ...)
    logging.Logger.event = _event  # type: ignore[attr-defined]


class _EventProxy:
    """
    Wrapper that provides `.event()` even if the underlying logger object
    doesn't allow attribute assignment.
    """

    def __init__(self, base: Any):
        self._base = base

    def __getattr__(self, item: str) -> Any:
        return getattr(self._base, item)

    def event(self, name: str, **fields: Any) -> None:
        _emit_event(self._base, name, fields)


def ensure_event(logger: Any) -> Any:
    """
    Ensure `logger.event(name, **fields)` exists.

    - If missing, we attach `.event()` when possible, otherwise return a proxy.
    - Also installs stdlib `logging.Logger.event` globally (safe no-op if already).
    - Never raises; logging must never crash runtime.
    """
    if logger is None:
        return logger

    # Make stdlib `logging.Logger` safe everywhere
    install_stdlib_event()

    # If it already has `.event`, done.
    try:
        existing = logger.event  # type: ignore[attr-defined]
    except Exception:
        existing = None

    if callable(existing):
        return logger

    def _event(name: str, **fields: Any) -> None:
        _emit_event(logger, name, fields)

    # Try to attach directly
    try:
        logger.event = _event  # type: ignore[attr-defined]
        return logger
    except Exception:
        # Fall back to proxy wrapper
        return _EventProxy(logger)
