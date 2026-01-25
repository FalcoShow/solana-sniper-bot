# execution/exits.py
from __future__ import annotations

"""
execution/exits.py — THIN WRAPPER (P0 LOCK-IN)
=============================================

LOCK-IN:
- utils_pkg/exit_engine.py is the ONE canonical exit brain.
- This module must NOT contain a second decision engine, registry, or supervisor.
- This module exists to:
    (1) provide stable imports for legacy call-sites (execution.exits.*),
    (2) adapt sell_fn signatures (3-arg vs 4-arg),
    (3) re-export the canonical types + helpers,
    (4) lazily re-export wiring helpers without creating circular imports.

If something seems missing, fix it in utils_pkg/exit_engine.py (the brain),
NOT here.
"""

import inspect
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar, cast

from logging_setup import get_logger

log = get_logger(__name__)

T = TypeVar("T")


# ──────────────────────────────────────────────────────────────────────────────
# Canonical engine import (MUST)
# ──────────────────────────────────────────────────────────────────────────────
try:
    # Canonical brain
    from utils_pkg.exit_engine import (  # type: ignore
        ExitDecision,
        PositionState,
        apply_exit as _engine_apply_exit,
        decide_exit as _engine_decide_exit,
        exits_config_summary as _engine_exits_config_summary,
    )
except Exception as e:  # pragma: no cover
    # Fail fast with a clear, actionable error.
    raise RuntimeError(
        "FATAL: execution.exits could not import utils_pkg.exit_engine. "
        "Per P0 lock-in, utils_pkg/exit_engine.py is required. "
        f"Import error: {e!r}"
    ) from e


# ──────────────────────────────────────────────────────────────────────────────
# Types (stable contracts for call-sites)
# ──────────────────────────────────────────────────────────────────────────────
# price_fn(mint) -> latest px (units must match entry_px)
PriceFn = Callable[[str], Awaitable[float | None]]

# sell_fn supports either:
#   (mint, usd_to_sell, reason) -> (ok, sig, meta)
# OR
#   (mint, usd_to_sell, reason, last_px) -> (ok, sig, meta)
#
# We accept both and adapt.
SellFn = Callable[..., Awaitable[tuple[bool, str | None, dict[str, Any]]]]


# ──────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ──────────────────────────────────────────────────────────────────────────────
async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


def _accepts_n_args(fn: Callable[..., Any], n: int) -> bool:
    """
    Best-effort signature probing:
      - Returns True if fn clearly accepts >= n positional args, or has *args.
      - If signature cannot be inspected, we assume it can take n args (permissive).
    """
    try:
        sig = inspect.signature(fn)
    except Exception:
        return True

    params = list(sig.parameters.values())
    # *args => yes
    if any(p.kind == inspect.Parameter.VAR_POSITIONAL for p in params):
        return True

    # Count positional-capable params (pos-only + pos-or-kw) that don't have defaults
    pos_capable = [
        p
        for p in params
        if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    # We treat it as "accepts n" if it has at least n positional-capable params
    return len(pos_capable) >= n


async def _call_sell_fn_compat(
    sell_fn: SellFn,
    mint: str,
    usd_to_sell: float,
    reason: str,
    last_px: float,
) -> tuple[bool, str | None, dict[str, Any]]:
    """
    Call sell_fn in a backwards-compatible way.

    Preferred newer form:
      sell_fn(mint, usd_to_sell, reason, last_px)

    Legacy form:
      sell_fn(mint, usd_to_sell, reason)
    """
    try:
        if _accepts_n_args(sell_fn, 4):
            out = sell_fn(mint, float(usd_to_sell), str(reason), float(last_px))
        else:
            out = sell_fn(mint, float(usd_to_sell), str(reason))
        ok, sig, meta = await _maybe_await(out)
        return bool(ok), cast(str | None, sig), cast(dict[str, Any], meta or {})
    except Exception as e:
        log.exception(
            "sell_fn failed mint=%s usd=%.4f reason=%s err=%s", mint, usd_to_sell, reason, e
        )
        return False, None, {"exc": repr(e)}


# ──────────────────────────────────────────────────────────────────────────────
# Pure decision wrapper (no side effects)
# ──────────────────────────────────────────────────────────────────────────────
def decide_exit(pos: PositionState, last_px: float, now_ts: float | None = None) -> ExitDecision:
    """
    Proxy to utils_pkg.exit_engine.decide_exit (canonical).
    """
    return _engine_decide_exit(pos, float(last_px), now_ts)  # type: ignore[arg-type]


# ──────────────────────────────────────────────────────────────────────────────
# Side-effectful executor wrapper (executes sell via sell_fn)
# ──────────────────────────────────────────────────────────────────────────────
async def apply_exit(
    pos: PositionState,
    last_px: float,
    sell_fn: SellFn,
    now_ts: float | None = None,
) -> ExitDecision:
    """
    Proxy to utils_pkg.exit_engine.apply_exit (canonical),
    adapting sell_fn signature if needed.
    """
    last_px_f = float(last_px)

    async def _adapted_sell_fn(mint: str, usd_to_sell: float, reason: str, lp: float | None = None):
        return await _call_sell_fn_compat(
            sell_fn=sell_fn,
            mint=mint,
            usd_to_sell=float(usd_to_sell),
            reason=str(reason),
            last_px=float(lp if lp is not None else last_px_f),
        )

    # Some canonical engines define apply_exit(pos, last_px, sell_fn, now_ts=?)
    # We call positionally for maximum compatibility, but keep now_ts if supported.
    try:
        return await _maybe_await(_engine_apply_exit(pos, last_px_f, _adapted_sell_fn, now_ts))  # type: ignore[misc]
    except TypeError:
        # Engine doesn't accept now_ts (older signature)
        return await _maybe_await(_engine_apply_exit(pos, last_px_f, _adapted_sell_fn))  # type: ignore[misc]


# ──────────────────────────────────────────────────────────────────────────────
# Back-compat helper
# ──────────────────────────────────────────────────────────────────────────────
def on_tick(pos: PositionState, last_px: float) -> str:
    """
    Legacy helper used by older call-sites.
    """
    d = decide_exit(pos, float(last_px))
    action = getattr(d, "action", "")
    return "EXIT_ALL" if action == "SELL_ALL" else "HOLD"


def exits_config_summary() -> dict[str, Any]:
    """
    Proxy to canonical engine config summary.
    """
    try:
        out = _engine_exits_config_summary()
        return dict(out or {})
    except Exception as e:
        return {"error": f"exits_config_summary_failed: {e!r}"}


# ──────────────────────────────────────────────────────────────────────────────
# Lazily re-export wiring helpers WITHOUT circular imports
# (execution.exits_wiring imports execution.exits at import time)
# ──────────────────────────────────────────────────────────────────────────────
def register_entry(*args: Any, **kwargs: Any) -> None:
    """
    Legacy import path:
      from execution.exits import register_entry

    Actual implementation lives in execution.exits_wiring.
    """
    from execution.exits_wiring import register_entry as _impl  # local import avoids cycle

    return _impl(*args, **kwargs)


def start_exit_supervisor(*args: Any, **kwargs: Any):
    """
    Legacy import path:
      from execution.exits import start_exit_supervisor
    """
    from execution.exits_wiring import start_exit_supervisor as _impl  # local import avoids cycle

    return _impl(*args, **kwargs)


async def stop_exit_supervisor(*args: Any, **kwargs: Any):
    """
    Legacy import path:
      from execution.exits import stop_exit_supervisor
    """
    from execution.exits_wiring import stop_exit_supervisor as _impl  # local import avoids cycle

    return await _impl(*args, **kwargs)


__all__ = [
    "ExitDecision",
    # canonical types
    "PositionState",
    "PriceFn",
    # stable type aliases
    "SellFn",
    "apply_exit",
    # canonical engine wrappers
    "decide_exit",
    "exits_config_summary",
    "on_tick",
    # wiring exports (lazy)
    "register_entry",
    "start_exit_supervisor",
    "stop_exit_supervisor",
]
