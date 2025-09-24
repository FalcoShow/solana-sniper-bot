# utils_pkg/sizer.py
from __future__ import annotations
import os
from typing import Optional

def _b(k: str, d: bool = False) -> bool:
    v = (os.getenv(k, "") or "").strip().lower()
    return d if not v else v in {"1", "true", "yes", "y", "on"}

def _f(k: str, d: float) -> float:
    try:
        return float(os.getenv(k, str(d)))
    except Exception:
        return float(d)

def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def _round_step(x: float, step: float) -> float:
    if step <= 0:
        return float(x)
    # round to nearest multiple of step
    return round(x / step) * step

def _mode() -> str:
    # "equity" or "wallet" — equity is default to keep this function sync-only
    m = (os.getenv("PF_TRADE_AUTOSIZE_MODE") or "equity").strip().lower()
    return "wallet" if m in {"wallet", "bal", "balance"} else "equity"

def _rails() -> tuple[float, float, float]:
    mn = _f("PF_TRADE_MIN_SOL", 0.005)
    mx = _f("PF_TRADE_MAX_SOL", 0.10)
    step = _f("PF_TRADE_STEP_SOL", 0.005)
    if mn > mx:  # guard bad env
        mn, mx = mx, mn
    return float(mn), float(mx), float(step)

def _equity_ticket_sol() -> float:
    """
    Equity-based sizing: equity (USD) / SOL (USD) * pct.
    Env:
      - EQUITY_USD_FALLBACK (default 2000)
      - SOL_PRICE_USD_FALLBACK (default 150)
      - POSITION_PCT_OF_EQUITY or TICKET_PCT_EQUITY (default 0.05 = 5%)
    """
    equity_usd = _f("EQUITY_USD_FALLBACK", 2000.0)
    sol_usd = max(1e-9, _f("SOL_PRICE_USD_FALLBACK", 150.0))
    pct = _f("POSITION_PCT_OF_EQUITY", _f("TICKET_PCT_EQUITY", 0.05))
    pct = _clamp(pct, 0.0, 1.0)
    return (equity_usd / sol_usd) * pct

def _wallet_ticket_sol() -> float:
    """
    Wallet-based sizing (no RPC): use WALLET_SOL_FALLBACK and PF_TRADE_RISK_PCT.
    Env:
      - WALLET_SOL_FALLBACK or PF_WALLET_SOL_FALLBACK (default 0)
      - PF_TRADE_RISK_PCT or POSITION_PCT_OF_WALLET (default 0.05)
    """
    wal = os.getenv("WALLET_SOL_FALLBACK", os.getenv("PF_WALLET_SOL_FALLBACK", "0")).strip()
    try:
        wallet_sol = max(0.0, float(wal))
    except Exception:
        wallet_sol = 0.0
    pct = _f("PF_TRADE_RISK_PCT", _f("POSITION_PCT_OF_WALLET", 0.05))
    pct = _clamp(pct, 0.0, 1.0)
    return wallet_sol * pct

def suggest_trade_sol() -> float:
    """
    Return the recommended trade size in SOL (sync, no RPC).
    Modes:
      - Fixed: if PF_TRADE_AUTOSIZE is false → PF_TRADE_SOL (clamped to rails).
      - Autosize 'equity' (default): EQUITY_USD_FALLBACK / SOL_PRICE_USD_FALLBACK * pct.
      - Autosize 'wallet': WALLET_SOL_FALLBACK * pct (still clamped by rails).
    Hard rails + step rounding are always applied.
    """
    mn, mx, step = _rails()

    # Fixed-size mode
    if not _b("PF_TRADE_AUTOSIZE", True):
        fixed = _f("PF_TRADE_SOL", mn)
        return float(_round_step(_clamp(fixed, mn, mx), step))

    # Autosize mode
    base = _wallet_ticket_sol() if _mode() == "wallet" else _equity_ticket_sol()
    sized = _clamp(base, mn, mx)
    return float(_round_step(sized, step))


__all__ = ["suggest_trade_sol"]
