# perps/risk.py
from __future__ import annotations

"""
perps.risk
==========

Perps-only risk sizing + guardrails.

Contract:
- Pure functions + small config loader (no I/O, no network, no side effects).
- Units are explicit:
    * equity_usd: USD
    * atr_pct: fraction of price (e.g. 0.02 == 2%)
    * notionals: USD
- Fail-closed: invalid inputs return conservative outputs (often 0).

Env (defaults are conservative):
  PERPS_LEVERAGE_CAP        float  (default 2.0)
  PERPS_SLEEVE_PCT          float  (default 0.30)  # max fraction of equity allocated to perps
  PERPS_VAR_LIMIT_PCT       float  (default 0.06)  # daily VaR budget (informational guard)
  PERPS_RISK_PER_TRADE      float  (default 0.005) # 0.5% equity risk per trade
  PERPS_ATR_PCT_FLOOR       float  (default 0.0005) # prevents infinite sizing on tiny ATR%
  PERPS_MIN_TICKET_USD      float  (default 5.0)
  PERPS_MAX_TICKET_USD      float  (default 0.0)    # 0 => no extra cap beyond sleeve
"""

import os
from dataclasses import dataclass

# ----------------------------- env parsing -----------------------------------


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or v == "" or v.lower() == "none":
        return float(default)
    try:
        return float(v)
    except Exception:
        return float(default)


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


# ----------------------------- config ----------------------------------------


@dataclass(frozen=True)
class PerpsRiskConfig:
    leverage_cap: float = 2.0
    sleeve_pct: float = 0.30
    var_daily_pct: float = 0.06

    risk_per_trade: float = 0.005
    atr_pct_floor: float = 0.0005

    min_ticket_usd: float = 5.0
    max_ticket_usd: float = 0.0  # 0 => disabled

    @staticmethod
    def from_env() -> PerpsRiskConfig:
        return PerpsRiskConfig(
            leverage_cap=_clamp(_env_float("PERPS_LEVERAGE_CAP", 2.0), 1.0, 100.0),
            sleeve_pct=_clamp(_env_float("PERPS_SLEEVE_PCT", 0.30), 0.0, 1.0),
            var_daily_pct=_clamp(_env_float("PERPS_VAR_LIMIT_PCT", 0.06), 0.0, 1.0),
            risk_per_trade=_clamp(_env_float("PERPS_RISK_PER_TRADE", 0.005), 0.0, 0.10),
            atr_pct_floor=_clamp(_env_float("PERPS_ATR_PCT_FLOOR", 0.0005), 1e-6, 0.25),
            min_ticket_usd=max(0.0, _env_float("PERPS_MIN_TICKET_USD", 5.0)),
            max_ticket_usd=max(0.0, _env_float("PERPS_MAX_TICKET_USD", 0.0)),
        )


# ----------------------------- API -------------------------------------------


def params(cfg: PerpsRiskConfig | None = None) -> tuple[float, float, float]:
    """
    Backward-compatible: returns (leverage_cap, sleeve_pct, var_daily_pct).
    """
    c = cfg or PerpsRiskConfig.from_env()
    return c.leverage_cap, c.sleeve_pct, c.var_daily_pct


def clamp_leverage(req_lev: float, cfg: PerpsRiskConfig | None = None) -> float:
    """
    Clamp requested leverage to config leverage_cap.
    """
    c = cfg or PerpsRiskConfig.from_env()
    try:
        req = float(req_lev)
    except Exception:
        req = 1.0
    return _clamp(req, 1.0, c.leverage_cap)


def max_notional(equity_usd: float, cfg: PerpsRiskConfig | None = None) -> float:
    """
    Max gross notional allowed for perps sleeve.
    """
    c = cfg or PerpsRiskConfig.from_env()
    try:
        eq = float(equity_usd)
    except Exception:
        return 0.0
    if eq <= 0 or c.sleeve_pct <= 0:
        return 0.0
    return max(0.0, c.sleeve_pct * eq)


def ticket_notional(
    equity_usd: float,
    atr_pct: float,
    risk_per_trade: float | None = None,
    cfg: PerpsRiskConfig | None = None,
) -> float:
    """
    Simple per-trade ticket sizing using ATR%.

    Approx:
      risk (USD) ~ atr_pct * notional
      target risk = r * equity
      => notional ≈ (r/atr_pct) * equity

    Then:
      - capped by perps sleeve max_notional
      - optional hard max_ticket_usd
      - floored by min_ticket_usd
    """
    c = cfg or PerpsRiskConfig.from_env()

    try:
        eq = float(equity_usd)
        a = float(atr_pct)
    except Exception:
        return 0.0
    if eq <= 0:
        return 0.0

    r = float(risk_per_trade) if (risk_per_trade is not None) else c.risk_per_trade
    r = _clamp(r, 0.0, 0.10)

    a = max(c.atr_pct_floor, abs(a))
    # base notional by risk
    notional = (r / a) * eq

    # cap by sleeve
    cap = max_notional(eq, c)
    if cap > 0:
        notional = min(notional, cap)

    # optional extra cap
    if c.max_ticket_usd > 0:
        notional = min(notional, c.max_ticket_usd)

    # floor
    if notional < c.min_ticket_usd:
        return 0.0

    return max(0.0, notional)


def risk_summary(cfg: PerpsRiskConfig | None = None) -> str:
    c = cfg or PerpsRiskConfig.from_env()
    return (
        f"[perps.risk] lev_cap={c.leverage_cap:.2f} sleeve={c.sleeve_pct:.0%} "
        f"VaR_day={c.var_daily_pct:.0%} r/trade={c.risk_per_trade:.3%} "
        f"atr_floor={c.atr_pct_floor:.4f} min_ticket=${c.min_ticket_usd:.2f} "
        f"max_ticket=${c.max_ticket_usd:.2f if c.max_ticket_usd>0 else 0.0}"
    )


__all__ = [
    "PerpsRiskConfig",
    "clamp_leverage",
    "max_notional",
    "params",
    "risk_summary",
    "ticket_notional",
]
