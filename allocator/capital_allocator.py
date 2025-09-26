# allocator/capital_allocator.py
import os, time, math, json, numpy as np
from typing import Dict, List
try:
    import redis
except Exception:
    redis = None

SLEEVES = ["daytrader", "sniper", "perps"]

def _r():
    url = os.getenv("REDIS_URL")
    if not url or not redis: return None
    return redis.from_url(url, decode_responses=True, ssl=url.startswith("rediss://"))

def _sharpe(x: np.ndarray):
    if len(x) < 20: return 0.0
    mu, sd = np.mean(x), np.std(x) + 1e-9
    return (mu / sd) * math.sqrt(365)

def _sortino(x: np.ndarray):
    if len(x) < 20: return 0.0
    mu = np.mean(x)
    downside = np.std(np.minimum(0, x)) + 1e-9
    return (mu / downside) * math.sqrt(365)

def _normalize(weights: Dict[str, float]) -> Dict[str, float]:
    s = sum(max(0.0, v) for v in weights.values())
    if s <= 0: return {k: 1.0/len(weights) for k in weights}
    return {k: max(0.0, v) / s for k, v in weights.items()}

def allocate(trailing: Dict[str, List[float]], regime: str) -> Dict[str, float]:
    # base by sharpe + sortino
    base = {}
    for k in SLEEVES:
        arr = np.array(trailing.get(k, []), dtype=float)
        base[k] = 0.5 * _sharpe(arr) + 0.5 * _sortino(arr)
    w = _normalize(base)

    # regime bias: push more to trend strategies in trend regimes
    if regime in ("low_vol_trend", "high_vol_trend"):
        w["daytrader"] *= 1.15
        w["perps"] *= 1.15
    elif regime in ("high_vol_chop",):
        w["daytrader"] *= 0.9
        w["sniper"] *= 1.1
    w = _normalize(w)

    # enforce caps
    cap = float(os.getenv("SLEEVE_WEIGHT_CAP", "0.6"))
    for k in w:
        w[k] = min(w[k], cap)
    w = _normalize(w)
    return w

def publish(weights: Dict[str, float]):
    r = _r()
    if not r: return
    r.set("alloc:weights", json.dumps(weights))

def run_allocator(get_trailing_returns, get_regime_returns, interval_sec=600):
    """
    - get_trailing_returns(): -> dict sleeve -> list of daily returns (float, e.g. 0.003)
    - get_regime_returns(): -> np.ndarray of recent equity returns for regime classification
    Writes JSON to Redis 'alloc:weights'.
    """
    from allocator.regime import classify_regime
    while True:
        try:
            trailing = get_trailing_returns()
            regime_arr = get_regime_returns()
            regime = classify_regime(regime_arr)
            w = allocate(trailing, regime)
            publish(w)
        except Exception as e:
            print(f"[allocator] error: {e}")
        time.sleep(interval_sec)
