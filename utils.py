# utils.py
# Compatibility shim so old imports like `from utils import ...` still work.
# We re-export the public API from utils_pkg.core_utils and provide a
# back-compat wrapper for `track_profit_notion`.

from __future__ import annotations
from typing import Any, Optional

# Re-export everything people expect from core_utils
from utils_pkg.core_utils import *  # noqa: F401,F403

# Import the real Notion logger
from utils_pkg.core_utils import log_to_notion as _log_to_notion

async def track_profit_notion(
    token: str,
    symbol: str = "",
    balance: Optional[float] = None,
    entry: Optional[float] = None,
    current: Optional[float] = None,
    profit: Optional[float] = None,
    reason: str = "",
    **kwargs: Any,
) -> None:
    """
    Back-compat wrapper used by older scanners.
    Delegates to utils_pkg.core_utils.log_to_notion(). If Notion is disabled,
    this is effectively a no-op.

    Accepts flexible arg names via **kwargs** (amount/size, pnl/profit_sol, note/reason).
    """
    # tolerate alternative names
    if balance is None:
        try:
            balance = float(kwargs.get("amount") or kwargs.get("size") or 0.0)
        except Exception:
            balance = 0.0
    if profit is None:
        try:
            profit = float(kwargs.get("pnl") or kwargs.get("profit_sol") or 0.0)
        except Exception:
            profit = 0.0
    if not reason:
        reason = str(kwargs.get("note") or kwargs.get("reason") or "")

    try:
        await _log_to_notion(
            token=token,
            symbol=symbol,
            balance=float(balance or 0.0),
            entry=entry if entry is not None else 0.0,
            current=current if current is not None else 0.0,
            profit=profit if profit is not None else 0.0,
            reason=reason,
        )
    except Exception:
        # Never crash the scanner over logging
        return
