# execution/__init__.py
from __future__ import annotations

"""
execution/ — Execution package (P0 canonical exports)
====================================================

Contract (P0):
- execution/ is the *thin wiring layer* around the canonical brains:
    • Exits:   utils_pkg/exit_engine.py (logic) + execution/exits_wiring.py (supervisor wiring)
    • PnL:     execution/pnl_watcher.py (observability only; no trading side effects)
    • Sells:   execution/sell_fn.py (swap/tx execution helpers)

Rules:
- NO side effects at import time (no tasks spawned, no network calls).
- Re-export stable public APIs so the rest of the repo has a single import surface:
    from execution import start_execution, stop_execution, register_entry, start_exit_supervisor, start_pnl_watcher
"""

from typing import Any

__all__ = [
    "PnlSnapshot",
    "PnlWatcher",
    "get_execution_status",
    "get_exit_status",
    "positions_count",
    "register_entry",
    # Sell helper (canonical)
    "sell_token_usd",
    # Bundle helpers
    "start_execution",
    # Exits wiring (canonical)
    "start_exit_supervisor",
    # PnL watcher (canonical)
    "start_pnl_watcher",
    "stop_execution",
    "stop_exit_supervisor",
    "stop_pnl_watcher",
]

__version__ = "p0.1.0"


# ──────────────────────────────────────────────────────────────────────────────
# Safe re-exports (best-effort). We intentionally avoid hard failures here so
# imports don't explode during partial refactors/tests.
# ──────────────────────────────────────────────────────────────────────────────

# --- exits wiring
try:
    from .exits_wiring import (  # type: ignore
        get_exit_status,
        positions_count,
        register_entry,
        start_exit_supervisor,
        stop_exit_supervisor,
    )
except Exception:

    def start_exit_supervisor(*_a, **_k):  # type: ignore
        raise RuntimeError("execution.exits_wiring not available")

    async def stop_exit_supervisor(*_a, **_k):  # type: ignore
        return None

    def register_entry(*_a, **_k):  # type: ignore
        raise RuntimeError("execution.exits_wiring not available")

    def positions_count() -> int:  # type: ignore
        return 0

    def get_exit_status() -> dict[str, Any]:  # type: ignore
        return {}


# --- pnl watcher
try:
    from .pnl_watcher import (  # type: ignore
        PnlSnapshot,
        PnlWatcher,
        start_pnl_watcher,
        stop_pnl_watcher,
    )
except Exception:
    PnlWatcher = None  # type: ignore
    PnlSnapshot = None  # type: ignore

    def start_pnl_watcher(*_a, **_k):  # type: ignore
        raise RuntimeError("execution.pnl_watcher not available")

    async def stop_pnl_watcher(*_a, **_k):  # type: ignore
        return None


# --- sell fn
try:
    from .sell_fn import sell_token_usd  # type: ignore
except Exception:

    async def sell_token_usd(*_a, **_k):  # type: ignore
        raise RuntimeError("execution.sell_fn not available")


# ──────────────────────────────────────────────────────────────────────────────
# Bundle helpers (start/stop all execution services)
# ──────────────────────────────────────────────────────────────────────────────
def start_execution(loop: Any | None = None) -> dict[str, Any]:
    """
    Idempotently starts all background execution services that should be running
    in production.

    Returns a lightweight dict describing what started (or was already running).
    """
    status: dict[str, Any] = {}

    # Start exits supervisor
    try:
        t = start_exit_supervisor(loop=loop)  # type: ignore
        status["exit_supervisor"] = {
            "ok": True,
            "task": str(getattr(t, "get_name", lambda: t)()) if t else "spawned",
        }
    except Exception as e:
        status["exit_supervisor"] = {"ok": False, "err": repr(e)}

    # Start pnl watcher
    try:
        t = start_pnl_watcher(loop=loop)  # type: ignore
        status["pnl_watcher"] = {
            "ok": True,
            "task": str(getattr(t, "get_name", lambda: t)()) if t else "spawned",
        }
    except Exception as e:
        status["pnl_watcher"] = {"ok": False, "err": repr(e)}

    return status


async def stop_execution() -> dict[str, Any]:
    """
    Stops all background execution services. Safe to call multiple times.
    """
    status: dict[str, Any] = {}

    try:
        await stop_exit_supervisor()  # type: ignore
        status["exit_supervisor"] = {"ok": True}
    except Exception as e:
        status["exit_supervisor"] = {"ok": False, "err": repr(e)}

    try:
        await stop_pnl_watcher()  # type: ignore
        status["pnl_watcher"] = {"ok": True}
    except Exception as e:
        status["pnl_watcher"] = {"ok": False, "err": repr(e)}

    return status


def get_execution_status() -> dict[str, Any]:
    """
    Consolidated debug snapshot (safe, best-effort).
    """
    out: dict[str, Any] = {}
    try:
        out["exits"] = {
            "managed": int(positions_count()),
            "positions": get_exit_status(),
        }
    except Exception as e:
        out["exits"] = {"err": repr(e)}

    # PnL watcher exposes its own Redis snapshot key; the watcher module is already
    # responsible for persisting it. Here we only report that the module exists.
    out["pnl"] = {"module": "execution.pnl_watcher"}
    return out
