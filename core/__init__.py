# core/__init__.py
from __future__ import annotations

"""
core package — canonical "core primitives" layer.

Rules (LOCK-IN)
---------------
- core/ must stay *pure* primitives: deterministic, no network, no DB, no RPC.
- Import-time must never perform heavy work or fail due to optional deps.
- core/ must not compete with utils_pkg/ execution brains; it only provides
  small scoring/utility primitives that other layers may use.

Exposed symbols
---------------
- alpha: AlphaConfig, AlphaResult, score_candidate, passes_alpha, select_top, score_only
"""

from typing import TYPE_CHECKING

__all__ = [
    "CORE_FEATURES",
    # alpha exports
    "AlphaConfig",
    "AlphaResult",
    "__version__",
    "passes_alpha",
    "score_candidate",
    "score_only",
    "select_top",
]

# Keep version simple and internal (no packaging requirement)
__version__ = "0.1.0"

# Feature manifest: used by scripts/preflight.py + smoke tests to assert wiring.
# (Do not add anything here that requires imports to be executed.)
CORE_FEATURES = {
    "alpha": True,
    # Reserved for future core primitives (keep stable keys):
    "regime": False,
    "sizing": False,
    "risk_math": False,
}

# Re-export alpha symbols. This should be safe (alpha is pure stdlib).
from .alpha import (
    AlphaConfig,
    AlphaResult,
    passes_alpha,
    score_candidate,
    score_only,
    select_top,
)

if TYPE_CHECKING:
    # type-only imports go here (no runtime impact)
    pass
