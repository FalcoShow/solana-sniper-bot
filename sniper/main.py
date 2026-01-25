# -*- coding: utf-8 -*-
from __future__ import annotations

"""
sniper/main.py — God-Tier Lifecycle Wrapper + aiohttp Leak Guard (V3+++)
======================================================================

Why this file exists
--------------------
Your project runs via `py -m scripts.main`, and you saw:
  ASYNCIO_EXCEPTION {'msg': 'Unclosed client session', ...}

That means at least one aiohttp.ClientSession created during runtime is not being
closed on shutdown. This wrapper provides a *zero-regression* fix:

1) Installs a tracker by monkeypatching aiohttp.ClientSession.__init__ to record
   every created session (covers `aiohttp.ClientSession()` AND `ClientSession()`
   imported from aiohttp).
2) Runs the existing entrypoint (`scripts.main`) without modifying it.
3) On exit (normal, SystemExit, KeyboardInterrupt, exceptions), closes:
   - any leaked aiohttp sessions (best-effort)
   - core resources via utils_pkg.core_utils.close_resources() (if present)

How to use
----------
Local:
  py -m sniper.main

Render:
  Change start command from `python -m scripts.main` to `python -m sniper.main`

Feature flags
-------------
LEAK_GUARD=1                Enable tracking + close-on-exit (default: 1)
LEAK_GUARD_FORCE_CLOSE=1    Actually close sessions (default: 1)
LEAK_GUARD_VERBOSE=1        Log extra shutdown details (default: 0)

Non-negotiables
---------------
- No network I/O at import time.
- No changes to your existing modules.
- No feature amputations.
"""

import asyncio
import contextlib
import os
import runpy
import time
import weakref
from typing import Any

_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return default


# Logging (safe fallback)
try:  # pragma: no cover
    from logging_setup import get_logger  # type: ignore
except Exception:  # pragma: no cover
    import logging

    def get_logger(name: str | None = None):  # type: ignore
        return logging.getLogger(name or "sniper.main")


log = get_logger("sniper.main.wrapper")


class AiohttpSessionTracker:
    """
    Tracks aiohttp.ClientSession objects created anywhere in-process by patching
    ClientSession.__init__ (covers `aiohttp.ClientSession` and imported aliases).
    """

    def __init__(self) -> None:
        self._sessions: "weakref.WeakSet[Any]" = weakref.WeakSet()
        self._installed = False
        self._orig_init: Any | None = None

    def install(self) -> bool:
        if self._installed:
            return True
        try:
            import aiohttp  # lazy import

            cls = aiohttp.ClientSession
            # idempotency guard
            if getattr(cls, "_LEAK_GUARD_PATCHED", False):
                self._installed = True
                return True

            orig_init = cls.__init__
            self._orig_init = orig_init

            tracker = self

            def patched_init(self_sess: Any, *args: Any, **kwargs: Any) -> None:
                orig_init(self_sess, *args, **kwargs)
                with contextlib.suppress(Exception):
                    tracker._sessions.add(self_sess)

            cls.__init__ = patched_init  # type: ignore[assignment]
            cls._LEAK_GUARD_PATCHED = True
            self._installed = True
            return True
        except Exception:
            return False

    def stats(self) -> dict[str, int]:
        total = 0
        open_n = 0
        for s in list(self._sessions):
            total += 1
            with contextlib.suppress(Exception):
                if not getattr(s, "closed", False):
                    open_n += 1
        return {"tracked": total, "open": open_n}

    async def close_all(self, *, force_close: bool = True, verbose: bool = False) -> None:
        # Snapshot to avoid WeakSet mutation while iterating
        sessions = list(self._sessions)
        open_sessions: list[Any] = []
        for s in sessions:
            with contextlib.suppress(Exception):
                if not getattr(s, "closed", False):
                    open_sessions.append(s)

        if verbose:
            st = self.stats()
            log.info("LEAK_GUARD_STATS %s", st)

        if not force_close:
            return

        # Close in parallel (best-effort)
        async def _close_one(sess: Any) -> None:
            try:
                c = getattr(sess, "close", None)
                if callable(c):
                    out = c()
                    if asyncio.iscoroutine(out):
                        await out
            except Exception:
                return

        if open_sessions:
            await asyncio.gather(*[_close_one(s) for s in open_sessions], return_exceptions=True)


_TRACKER = AiohttpSessionTracker()


async def _shutdown_all() -> None:
    """
    Best-effort shutdown cleanup:
      - close leaked aiohttp sessions
      - close core resources (solana client / core aiohttp / httpx / redis) if available
    """
    verbose = _env_bool("LEAK_GUARD_VERBOSE", False)
    force_close = _env_bool("LEAK_GUARD_FORCE_CLOSE", True)

    if _env_bool("LEAK_GUARD", True):
        await _TRACKER.close_all(force_close=force_close, verbose=verbose)

    # Close project resources (best-effort, no hard dependency)
    with contextlib.suppress(Exception):
        from utils_pkg.core_utils import close_resources  # type: ignore

        out = close_resources()
        if asyncio.iscoroutine(out):
            await out

    # Give the loop one tick to flush any pending close callbacks
    with contextlib.suppress(Exception):
        await asyncio.sleep(0)


def _run_shutdown_sync() -> None:
    try:
        asyncio.run(_shutdown_all())
    except RuntimeError:
        # If we're already in an event loop (rare here), schedule and return.
        with contextlib.suppress(Exception):
            loop = asyncio.get_running_loop()
            loop.create_task(_shutdown_all())
    except Exception:
        return


def main() -> None:
    """
    Executes scripts.main as if `python -m scripts.main`, but with leak guard
    and guaranteed cleanup on exit.
    """
    if _env_bool("LEAK_GUARD", True):
        ok = _TRACKER.install()
        if _env_bool("LEAK_GUARD_VERBOSE", False):
            log.info("LEAK_GUARD_INSTALL ok=%s", int(ok))

    t0 = time.monotonic()
    err: BaseException | None = None
    exit_code: int | None = None

    try:
        # Run the existing entrypoint exactly as -m scripts.main would.
        runpy.run_module("scripts.main", run_name="__main__")
    except SystemExit as e:
        # Preserve exit behavior, but still run cleanup.
        err = e
        code = e.code
        if code is None:
            exit_code = 0
        elif isinstance(code, int):
            exit_code = int(code)
        else:
            exit_code = 1
    except KeyboardInterrupt as e:
        err = e
        exit_code = 130
    except BaseException as e:
        err = e
    finally:
        _run_shutdown_sync()
        if _env_bool("LEAK_GUARD_VERBOSE", False):
            dt = time.monotonic() - t0
            log.info("WRAPPER_EXIT dt_sec=%.3f", float(dt))

    if err is None:
        return

    # Re-raise with preserved semantics
    if isinstance(err, SystemExit):
        raise SystemExit(exit_code if exit_code is not None else err.code)
    if isinstance(err, KeyboardInterrupt):
        raise SystemExit(exit_code if exit_code is not None else 130)
    raise err


if __name__ == "__main__":
    main()
