# allocator/regime.py
import numpy as np

def classify_regime(returns: np.ndarray, vol_window: int = 64):
    if len(returns) < max(32, vol_window):
        return "neutral"
    r = returns[-64:]
    vol = np.std(r)
    trend = np.sign(np.mean(np.diff(np.cumsum(r))))
    if vol < 0.5 * np.std(returns[-256:]):
        return "low_vol_trend" if trend > 0 else "low_vol_sideways"
    if vol > 1.5 * np.std(returns[-256:]):
        return "high_vol_trend" if trend > 0 else "high_vol_chop"
    return "neutral"
