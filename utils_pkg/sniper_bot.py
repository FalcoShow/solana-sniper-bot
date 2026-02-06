from __future__ import annotations

"""
utils_pkg/sniper_bot.py

God-Tier Compatibility Shim (P3 — repo hygiene / correctness)
=============================================================

Why this exists
---------------
Some tooling/configs may reference `utils_pkg.sniper_bot`, but the canonical
service wrapper lives at `sniper/sniper_bot.py`.

Hard contract (God-Tier)
------------------------
- NO heavy imports at module import time (lazy import only).
- NO network I/O at import time.
- Stable surface:
    - async main(...)
    - sync run(...)
- Signature-tolerant: supports drift in the target `sniper.sniper_bot.main`.

Running
-------
python -m utils_pkg.sniper_bot
"""

import asyncio
import contextlib
import importlib
import inspect
import sys
from typing import Any, Callable

# Cache the resolved callable to avoid repeated imports
_MAIN_CALLABLE: Callable[..., Any] | None = None
_MAIN_TRIED: bool = False

_TARGET_MODULES: tuple[str, ...] = ("sniper.sniper_bot",)
_TARGET_NAMES: tuple[str, ...] = ("main",)


def _load_main_callable() -> Callable[..., Any] | None:
    """
    Lazy-load the canonical async main callable.

    Safe: never raises.
    """
    global _MAIN_CALLABLE, _MAIN_TRIED
    if _MAIN_TRIED:
        return _MAIN_CALLABLE
    _MAIN_TRIED = True

    for mod_name in _TARGET_MODULES:
        try:
            mod = importlib.import_module(mod_name)
        except Exception:
            continue

        for fn_name in _TARGET_NAMES:
            fn = getattr(mod, fn_name, None)
            if callable(fn):
                _MAIN_CALLABLE = fn
                return fn

    _MAIN_CALLABLE = None
    return None


def _filter_kwargs(fn: Callable[..., Any], kwargs: dict[str, Any]) -> dict[str, Any]:
    """
    Best-effort kwarg filtering to tolerate signature drift.
    """
    try:
        sig = inspect.signature(fn)
        allowed = set(sig.parameters.keys())
        return {k: v for k, v in kwargs.items() if k in allowed}
    except Exception:
        return kwargs


async def main(*args: Any, **kwargs: Any) -> int:
    """
    Compatibility async entrypoint. Delegates to sniper.sniper_bot.main.

    Returns an int exit code (0 default).
    """
    fn = _load_main_callable()
    if fn is None:
        # Fail loudly but safely (no exception spam)
        _stderr(
            "utils_pkg.sniper_bot: missing target 'sniper.sniper_bot.main'. "
            "Ensure sniper/sniper_bot.py exists and is importable."
        )
        return 2

    # Delegate with drift tolerance:
    try:
        return int(await _maybe_await(fn(*args, **kwargs)) or 0)
    except TypeError:
        # Retry with filtered kwargs if signature drifted
        try:
            kw2 = _filter_kwargs(fn, dict(kwargs))
            return int(await _maybe_await(fn(*args, **kw2)) or 0)
        except Exception as e:
            _stderr(f"utils_pkg.sniper_bot: target main() failed: {type(e).__name__}: {e}")
            return 1
    except Exception as e:
        _stderr(f"utils_pkg.sniper_bot: target main() failed: {type(e).__name__}: {e}")
        return 1


def run(*args: Any, **kwargs: Any) -> int:
    """
    Sync runner for CLI usage.
    - If no loop is running: uses asyncio.run(main()).
    - If a loop is already running: schedules main() and returns 0.
      (In that case, callers should await `main()` directly for proper completion.)
    """
    try:
        loop = asyncio.get_running_loop()
        if loop.is_running():
            # We cannot block here; schedule and return.
            loop.create_task(main(*args, **kwargs))
            return 0
    except Exception:
        pass

    try:
        return int(asyncio.run(main(*args, **kwargs)))
    except KeyboardInterrupt:
        return 130
    except Exception as e:
        _stderr(f"utils_pkg.sniper_bot: run() failed: {type(e).__name__}: {e}")
        return 1


async def _maybe_await(x: Any) -> Any:
    return await x if asyncio.iscoroutine(x) else x


def _stderr(msg: str) -> None:
    with contextlib.suppress(Exception):
        sys.stderr.write(str(msg).rstrip() + "\n")


__all__ = ["main", "run"]


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(run())
