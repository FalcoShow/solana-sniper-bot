# core/alpha.py
def _z(a, lo, hi):
    # 0..1 in sweet spot, smooth outside
    if a is None: return 0.0
    if a < lo: return max(0.0, (a / lo) * 0.25)
    if a > hi: return max(0.0, (hi / a) * 0.25)
    # inside band -> 0.75..1.0, peak-ish in middle
    mid = (lo + hi) / 2.0
    span = (hi - lo) / 2.0
    return 0.75 + max(0.0, 0.25 - abs(a - mid) / (4 * span))

def score_candidate(c: dict) -> float:
    age = c.get("pool_age_min")     # want <= 15
    liq = c.get("liq_usd")          # sweet spot 5k..60k
    imp = c.get("impact_1k_bps")    # sweet spot 250..900

    s = 0.0
    if age is not None:
        s += max(0.0, (15.0 - age) / 15.0) * 0.35
    s += _z(liq, 5_000, 60_000) * 0.35
    s += _z(imp, 250, 900) * 0.25
    if c.get("source") == "pumpfun":
        s += 0.05
    # optional boost if you annotate smart wallets elsewhere: c["smart_wallet_score"] in [0..1]
    s += 0.10 * float(c.get("smart_wallet_score", 0))
    return max(0.0, min(1.0, s))

THRESHOLD = 0.62
MAX_TRADEABLE = 8
