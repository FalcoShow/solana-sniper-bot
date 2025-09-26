# perps/risk.py
import os, math

def params():
    lev_cap = float(os.getenv("PERPS_LEVERAGE_CAP", "2.0"))
    sleeve_pct = float(os.getenv("PERPS_SLEEVE_PCT", "0.30"))  # portion of equity max on perps
    var_daily_pct = float(os.getenv("PERPS_VAR_LIMIT_PCT", "0.06"))  # 6% daily VaR budget
    return lev_cap, sleeve_pct, var_daily_pct

def max_notional(equity_usd: float):
    _, sleeve_pct, _ = params()
    return sleeve_pct * equity_usd

def ticket_notional(equity_usd: float, atr_pct: float, risk_per_trade: float = None):
    r = risk_per_trade or float(os.getenv("PERPS_RISK_PER_TRADE", "0.005"))
    # approximate position sizing: risk = ATR% * notional ≈ r * equity => notional ≈ (r/ATR%) * equity
    if atr_pct <= 0.0005: atr_pct = 0.0005
    return min((r / atr_pct) * equity_usd, max_notional(equity_usd))

def clamp_leverage(req_lev: float):
    lev_cap, _, _ = params()
    return min(req_lev, lev_cap)
