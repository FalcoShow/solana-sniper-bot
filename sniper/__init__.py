# sniper/__init__.py
from __future__ import annotations

"""
sniper package (strategy wiring layer)
======================================

LOCK-IN contract:
- This package wires together the top-level trading strategy (scanner → entry → execution),
  but MUST NOT become a second execution engine.
- Importing this package MUST NOT:
    • open network connections
    • initialize wallets/RPC
    • create aiohttp sessions
    • connect Redis/Postgres
    • start loops / background tasks

Exports are lazy to avoid heavy imports or side effects at import time.
"""

from typing import TYPE_CHECKING, Any

__all__ = [
    "Sniper",
    "__version__",
    "arbitrage_hint_for_candidate",
    "check_arbitrage_opportunity",
]

# Keep version simple and non-breaking.
# If you later add a central version module, you can swap this to import from there.
__version__ = "0.1.0"


if TYPE_CHECKING:
    # These are only for type checkers / IDEs (no runtime side effects).
    from .arbitrage import (
        arbitrage_hint_for_candidate as arbitrage_hint_for_candidate,
        check_arbitrage_opportunity as check_arbitrage_opportunity,
    )
    from .sniper import Sniper as Sniper


def __getattr__(name: str) -> Any:
    """
    Lazy attribute access (PEP 562).

    This allows:
        from sniper import Sniper
    without importing sniper.sniper at package import time.
    """
    if name == "Sniper":
        from .sniper import Sniper  # local import = lazy

        return Sniper

    if name in ("check_arbitrage_opportunity", "arbitrage_hint_for_candidate"):
        from .arbitrage import arbitrage_hint_for_candidate, check_arbitrage_opportunity

        return {
            "check_arbitrage_opportunity": check_arbitrage_opportunity,
            "arbitrage_hint_for_candidate": arbitrage_hint_for_candidate,
        }[name]

    raise AttributeError(f"module 'sniper' has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(list(globals().keys()) + __all__)
