# utils_pkg/sizer.py
from __future__ import annotations
import os

def _b(k, d=False): return str(os.getenv(k, "1" if d else "0")).strip().lower() in {"1","true","yes","y","on"}
def _f(k, d): 
    try: return float(os.getenv(k, str(d)))
    except: return float(d)

def suggest_trade_sol() -> float:
    # Hard min/max rails
    mn = _f("PF_TRADE_MIN_SOL", 0.005)
    mx = _f("PF_TRADE_MAX_SOL", 0.10)

    # If autosize off → PF_TRADE_SOL wins
    if not _b("PF_TRADE_AUTOSIZE", True):
        base = _f("PF_TRADE_SOL", mn)
        return max(mn, min(mx, base))

    # Equity-based sizing
    equity_usd  = _f("EQUITY_USD_FALLBACK", 2000.0)
    sol_usd     = _f("SOL_PRICE_USD_FALLBACK", 150.0)
    equity_sol  = max(0.0, equity_usd / max(1e-9, sol_usd))

    pct  = _f("POSITION_PCT_OF_EQUITY", _f("TICKET_PCT_EQUITY", 0.05))
    step = _f("PF_TRADE_STEP_SOL", 0.005)

    raw = equity_sol * max(0.0, min(1.0, pct))
    sized = max(mn, min(mx, raw))

    # snap to nice step
    if step > 0:
        sized = round(sized / step) * step
    return float(sized)
