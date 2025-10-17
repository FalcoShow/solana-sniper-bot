# execution/params.py
SLIPPAGE_BPS_MAX = 1200      # 12%
BASE_SLIPPAGE_BPS = 400      # 4% floor
MAX_SELF_IMPACT_BPS = 300    # target your own impact <= 300 bps
TRANCHES = [0.6, 0.4]        # two bites

def compute_order_plan(cand, bankroll_usd: float):
    imp_bps = float(cand.get("impact_1k_bps") or 500.0)
    # size so our impact stays contained
    # empirical exponent < 1.0 to be conservative on thin books
    k = max(1.0, (MAX_SELF_IMPACT_BPS / max(10.0, imp_bps)) ** (1/0.75)) * 1000.0
    # k is the $ at which we expect ~MAX_SELF_IMPACT_BPS
    order_usd = min(bankroll_usd, k)

    # slippage = base + factor * expected impact, capped
    expected_bps = min(1500.0, imp_bps * (order_usd / 1000.0) ** 0.75)
    slippage_bps = int(min(SLIPPAGE_BPS_MAX, BASE_SLIPPAGE_BPS + 1.7 * expected_bps))

    # split into tranches
    legs = [round(order_usd * p, 2) for p in TRANCHES]
    return legs, slippage_bps
