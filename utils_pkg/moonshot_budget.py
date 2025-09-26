# utils_pkg/moonshot_budget.py
import os

def moonshot_limits():
    sleeve_max = float(os.getenv("MOONSHOT_SLEEVE_MAX_PCT", "0.10"))  # 10% equity
    per_trade = float(os.getenv("MOONSHOT_PER_TRADE_PCT", "0.03"))    # 3% equity
    return sleeve_max, per_trade

def can_allocate(equity_usd: float, current_moonshot_alloc_usd: float, desired_usd: float) -> bool:
    sleeve_max, per_trade = moonshot_limits()
    if desired_usd > per_trade * equity_usd:
        return False
    if current_moonshot_alloc_usd + desired_usd > sleeve_max * equity_usd:
        return False
    return True
