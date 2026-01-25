# perps/__init__.py
"""
perps package
=============

This package is an *optional* PERPS layer that must NOT silently compete with the
main sniper/spot path. It provides:

- alpha helpers (tiny rule gates)
- a Drift connector (feature-flagged, dry-run capable)
- perps risk sizing helpers (pure functions)

No side effects on import.
"""

from __future__ import annotations

# Alpha gates
from .alpha import should_long, should_short

# Connector (Drift)
from .connector_drift import DriftPerps, HealthInfo, OrderResult, PerpsConfig, PositionInfo

# Risk helpers (canonical in perps/risk.py)
from .risk import (
    PerpsRiskConfig,
    clamp_leverage,
    max_notional,
    params,
    risk_summary,
    ticket_notional,
)

# Backward-compat aliases in case anything imported weird names historically
max_notional_usd = max_notional
ticket_notional_usd = ticket_notional

__all__ = [
    # connector
    "DriftPerps",
    "HealthInfo",
    "OrderResult",
    "PerpsConfig",
    # risk
    "PerpsRiskConfig",
    "PositionInfo",
    "clamp_leverage",
    "max_notional",
    # aliases
    "max_notional_usd",
    "params",
    "risk_summary",
    # alpha
    "should_long",
    "should_short",
    "ticket_notional",
    "ticket_notional_usd",
]
