# perps/connector_drift.py
import os, random
from typing import Tuple

class DriftPerps:
    def __init__(self):
        self.enabled = os.getenv("PERPS_ENABLED","false").lower()=="true"
        self.dry = os.getenv("DRY_RUN","false").lower()=="true"

    async def place_order(self, symbol: str, side: str, notional: float, leverage: float, stop_price: float, take_profit: float) -> Tuple[bool,str]:
        if not self.enabled:
            return False, "perps_disabled"
        if self.dry:
            return True, f"DRY-PERPS-{symbol}-{side}-{random.randint(1000,9999)}"
        # TODO: implement Drift client call here
        return False, "not_implemented"

    async def close_position(self, symbol: str, reduce_only: bool=True) -> Tuple[bool,str]:
        if self.dry:
            return True, f"DRY-CLOSE-{symbol}-{random.randint(1000,9999)}"
        return False, "not_implemented"
