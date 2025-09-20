# utils_pkg/guardrails.py
from __future__ import annotations
import os, time, math, json
from typing import Optional
try:
    import redis  # type: ignore
except Exception:
    redis = None

def _b(v, d=False): return str((v if v is not None else d)).strip().lower() in {"1","true","yes","y","on"}
def _f(k, d): 
    try: return float(os.getenv(k, str(d)))
    except: return float(d)
def _i(k, d):
    try: return int(os.getenv(k, str(d)))
    except: return int(d)

# Limits (we check both PF_* and legacy names)
MAX_LOSSES     = _i("PF_DAILY_MAX_LOSSES", _i("DAILY_MAX_LOSSES", 0))
MAX_DD_PCT     = _f("PF_DAILY_MAX_DRAWDOWN_PCT", _f("DAILY_MAX_DRAWDOWN_PCT", 0.0)) / 100.0
PAUSE_SWITCH   = "PAUSE_TRADING"

# Equity reference (for DD calc)
EQUITY_USD     = _f("EQUITY_USD_FALLBACK", 1500.0)
SOL_USD        = _f("SOL_PRICE_USD_FALLBACK", 150.0)

# Redis (recommended) or in-proc memory
_r: Optional["redis.Redis"] = None
if redis and os.getenv("REDIS_URL"):
    try:
        _r = redis.from_url(os.getenv("REDIS_URL"))  # type: ignore
    except Exception:
        _r = None

_mem = {"day": None, "losses": 0, "realized_sol": 0.0}

def _day_key(): return time.strftime("%Y-%m-%d")
def _get_state():
    day = _day_key()
    if _r:
        raw = _r.get("gr.state")
        if raw:
            try:
                st = json.loads(raw)
                if st.get("day") == day: return st
            except Exception: pass
        st = {"day": day, "losses": 0, "realized_sol": 0.0}
        _r.set("gr.state", json.dumps(st), ex=3*24*3600)
        return st
    # memory fallback
    if _mem["day"] != day:
        _mem.update({"day": day, "losses": 0, "realized_sol": 0.0})
    return dict(_mem)

def _save_state(st):
    if _r:
        _r.set("gr.state", json.dumps(st), ex=3*24*3600)
    else:
        _mem.update(st)

def buys_allowed() -> bool:
    # operator pause switch
    if _b(os.getenv(PAUSE_SWITCH, "0"), False): 
        return False
    if MAX_LOSSES <= 0 and MAX_DD_PCT <= 0:
        return True
    st = _get_state()
    # losses gate
    if MAX_LOSSES > 0 and st["losses"] >= MAX_LOSSES:
        return False
    # drawdown gate (vs equity fallback)
    if MAX_DD_PCT > 0:
        dd_usd = -max(0.0, st["realized_sol"])*SOL_USD
        if dd_usd >= (EQUITY_USD * MAX_DD_PCT): 
            return False
    return True

def note_close(realized_pnl_sol: float) -> None:
    """Call after a sell fills; realized_pnl_sol can be negative."""
    st = _get_state()
    st["realized_sol"] = float(st.get("realized_sol", 0.0)) + float(realized_pnl_sol or 0.0)
    if (realized_pnl_sol or 0.0) < 0:
        st["losses"] = int(st.get("losses", 0)) + 1
    _save_state(st)

def record_buy_result(ok: bool, reason: str, sig: Optional[str] = None) -> None:
    """Optional audit hook used by main; kept lightweight."""
    if not _r: return
    try:
        _r.lpush("gr.buylog", json.dumps({"ts": time.time(), "ok": ok, "reason": reason, "sig": sig}))
        _r.ltrim("gr.buylog", 0, 500)
    except Exception:
        pass
