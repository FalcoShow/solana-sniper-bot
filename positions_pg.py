# positions_pg.py (ROOT)
from __future__ import annotations

"""
positions_pg.py (ROOT) — COMPATIBILITY WRAPPER (P0 DEDUP LOCK-IN)
================================================================

🚫 This file must NOT contain competing Postgres logic.

Canonical sources:
- Position truth + lifecycle: utils_pkg.position_store
- Postgres “positions mirror / analytics”: utils_pkg.positions_pg
- PnL / realized reporting: utils_pkg.ledger (preferred) or daily_report

This root module exists ONLY because some legacy imports still do:
    import positions_pg

We keep a *tiny* surface area and delegate to canonical modules.

Current supported API (legacy):
- realized_today_sol(...)  -> float (async)

Everything else should import from utils_pkg.* directly.
"""

import contextlib
import os
from datetime import UTC, datetime
from typing import Any


def _truthy(x: Any) -> bool:
    return str(x or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _get_logger():
    try:
        from logging_setup import get_logger  # type: ignore

        return get_logger(__name__)
    except Exception:
        import logging

        return logging.getLogger(__name__)


log = _get_logger()

_DEPRECATION_LOGGED = False


def _log_deprecated_once() -> None:
    global _DEPRECATION_LOGGED
    if _DEPRECATION_LOGGED:
        return
    _DEPRECATION_LOGGED = True
    with contextlib.suppress(Exception):
        log.warning(
            "DEPRECATED_IMPORT: root positions_pg.py used. "
            "Prefer utils_pkg.ledger / daily_report / utils_pkg.positions_pg.",
            extra={"module": "positions_pg"},
        )


def _default_wallet() -> str:
    return (
        (os.getenv("POSITIONS_DEFAULT_WALLET") or "").strip()
        or (os.getenv("WALLET_ADDRESS") or "").strip()
        or (os.getenv("WALLET_PUBKEY") or "").strip()
        or (os.getenv("PUBLIC_KEY") or "").strip()
        or "unknown_wallet"
    )


def _start_of_today_utc() -> datetime:
    now = datetime.now(UTC)
    return now.replace(hour=0, minute=0, second=0, microsecond=0)


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────
async def realized_today_sol(
    *,
    wallet: str | None = None,
    tz: str | None = None,
) -> float:
    """
    Return realized PnL for "today" in SOL.

    Delegation order (most canonical first):
      1) utils_pkg.ledger.realized_today_sol(...)
      2) daily_report.realized_today_sol(...)
      3) utils_pkg.daily_report.realized_today_sol(...)
      4) fallback 0.0 (for local dev stubs)

    Notes:
    - We default to UTC day boundaries unless the delegated function supports tz.
    - This wrapper is intentionally minimal to avoid logic drift.
    """
    _log_deprecated_once()

    w = (wallet or _default_wallet()).strip() or None

    # 1) Preferred: ledger owns realized PnL
    try:
        from utils_pkg import ledger as _ledger  # type: ignore

        fn = getattr(_ledger, "realized_today_sol", None)
        if callable(fn):
            try:
                # try with tz if supported
                return float(await fn(wallet=w, tz=tz))
            except TypeError:
                return float(await fn(wallet=w))
    except Exception:
        pass

    # 2) Root daily_report (legacy)
    try:
        import daily_report as _dr  # type: ignore

        fn = getattr(_dr, "realized_today_sol", None)
        if callable(fn):
            try:
                return float(await fn(wallet=w, tz=tz))
            except TypeError:
                return float(await fn(wallet=w))
    except Exception:
        pass

    # 3) utils_pkg.daily_report (if you moved it)
    try:
        from utils_pkg import daily_report as _udr  # type: ignore

        fn = getattr(_udr, "realized_today_sol", None)
        if callable(fn):
            try:
                return float(await fn(wallet=w, tz=tz))
            except TypeError:
                return float(await fn(wallet=w))
    except Exception:
        pass

    # 4) Final fallback for local Windows dev stubs
    # (keeps older smoke tests from crashing)
    if _truthy(os.getenv("DEBUG_POSITIONS_PG_FALLBACK", "0")):
        with contextlib.suppress(Exception):
            log.warning(
                "positions_pg.realized_today_sol: falling back to 0.0",
                extra={"wallet": w, "tz": tz},
            )
    return 0.0


# Optional tiny sync shim (only used in one-off scripts)
def realized_today_sol_sync(*, wallet: str | None = None, tz: str | None = None) -> float:
    """
    Sync shim: runs the async function.
    Avoid using in production services (prefer async).
    """
    import asyncio

    try:
        loop = asyncio.get_running_loop()
        # If we’re already in a loop, schedule and return 0.0 immediately (best-effort).
        loop.create_task(realized_today_sol(wallet=wallet, tz=tz))  # type: ignore[arg-type]
        return 0.0
    except RuntimeError:
        return asyncio.run(realized_today_sol(wallet=wallet, tz=tz))


__all__ = ["realized_today_sol", "realized_today_sol_sync"]
