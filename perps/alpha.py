# perps/alpha.py
from typing import Dict

def should_long(features: Dict[str, float], funding8h_sum: float) -> bool:
    # example: EMA12>EMA26>EMA50, RSI>55, and funding not too positive
    return (
        features.get("ema12",0)>features.get("ema26",0)>features.get("ema50",0) and
        features.get("rsi14",50)>55 and
        funding8h_sum < 0.03
    )

def should_short(features: Dict[str, float], funding8h_sum: float) -> bool:
    return (
        features.get("ema12",0)<features.get("ema26",0)<features.get("ema50",0) and
        features.get("rsi14",50)<45 and
        funding8h_sum > -0.03
    )
