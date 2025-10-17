# execution/fees.py
import statistics

def jito_tip_lamports(recent_tips_lamports: list[int]) -> int:
    if not recent_tips_lamports:
        return 20_000  # ~0.00002 SOL
    med = int(statistics.median(recent_tips_lamports[-5:]))
    tip = int(med * 1.3)            # outbid the median
    return min(tip, 200_000)        # cap
