# utils_pkg/circuit_breakers.py
import os, time, json, datetime as dt
from typing import Optional, Tuple

try:
    import redis
except Exception:
    redis = None

DAY_FMT = "%Y-%m-%d"

def _r():
    url = os.getenv("REDIS_URL")
    if not url or not redis:
        return None
    return redis.from_url(url, decode_responses=True, ssl=url.startswith("rediss://"))

def _today():
    return dt.datetime.utcnow().strftime(DAY_FMT)

def _key(prefix): return f"cb:{prefix}"

def get_limits():
    daily_stop_pct = float(os.getenv("DAILY_STOP_PCT", "0.08"))     # 8%
    max_loss_streak = int(os.getenv("MAX_CONSECUTIVE_LOSSES", "6")) # 6 losers
    err_rate_halt = float(os.getenv("ERROR_RATE_HALT", "0.25"))     # 25% routing/error
    return daily_stop_pct, max_loss_streak, err_rate_halt

def record_error(ok: bool):
    r = _r()
    if not r: return
    today = _today()
    r.hincrby(_key(f"errs:{today}"), "n", 1)
    if not ok:
        r.hincrby(_key(f"errs:{today}"), "bad", 1)

def error_rate_breached() -> bool:
    r = _r()
    if not r: return False
    today = _today()
    m = r.hgetall(_key(f"errs:{today}"))
    if not m: return False
    n, bad = int(m.get("n", 0)), int(m.get("bad", 0))
    if n < 20:  # need some sample size
        return False
    _, _, err_rate_halt = get_limits()
    return (bad / max(1, n)) >= err_rate_halt

def record_fill(pnl_usd: float, equity_usd: float, ts: Optional[float] = None):
    """
    Call after every trade close/fill (or at least on realized pnl events).
    """
    r = _r()
    ts = ts or time.time()
    today = _today()

    if r:
        # Daily PnL
        r.hincrbyfloat(_key(f"pnl:{today}"), "pnl", pnl_usd)
        r.hset(_key(f"pnl:{today}"), mapping={"eq": equity_usd, "t": ts})
        # Streaks
        if pnl_usd < 0:
            r.hincrby(_key(f"streak:{today}"), "losers", 1)
            r.hset(_key(f"streak:{today}"), "winners", 0)
        elif pnl_usd > 0:
            r.hincrby(_key(f"streak:{today}"), "winners", 1)
            r.hset(_key(f"streak:{today}"), "losers", 0)
        else:
            pass

def status() -> dict:
    r = _r()
    today = _today()
    d = {"today": today}
    if r:
        pnlm = r.hgetall(_key(f"pnl:{today}")) or {}
        strk = r.hgetall(_key(f"streak:{today}")) or {}
        errs = r.hgetall(_key(f"errs:{today}")) or {}
        d["pnl"] = {"pnl": float(pnlm.get("pnl", 0.0)), "eq": float(pnlm.get("eq", 0.0))}
        d["streak"] = {"losers": int(strk.get("losers", 0)), "winners": int(strk.get("winners", 0))}
        n, bad = int(errs.get("n", 0)), int(errs.get("bad", 0))
        d["errors"] = {"n": n, "bad": bad, "rate": (bad / n) if n else 0.0}
    return d

def should_halt() -> Tuple[bool, str]:
    daily_stop_pct, max_loss_streak, _ = get_limits()
    s = status()
    pnl = s.get("pnl", {})
    eq = pnl.get("eq", 0.0) or 0.0
    today_pnl = pnl.get("pnl", 0.0) or 0.0
    if eq > 0 and today_pnl <= -abs(daily_stop_pct) * eq:
        return True, f"daily_stop hit: pnl={today_pnl:.2f} eq={eq:.2f} (limit {daily_stop_pct:.0%})"
    if s.get("streak", {}).get("losers", 0) >= max_loss_streak:
        return True, f"loss_streak >= {max_loss_streak}"
    if error_rate_breached():
        return True, "error_rate_halt"
    return False, ""
