# scripts/main.py
from __future__ import annotations

import asyncio
import os
import traceback
from typing import Any, Dict

from utils_pkg.pumpportal_client import PumpPortalClient
from utils_pkg.pump_trader import maybe_buy  # contains DRY_RUN + autosizing usage

# --------- lightweight, overridable filters ----------
MIN_INITIAL_BUY_SOL = float(os.getenv("ENTRY_MIN_BUY_SOL", "0.0"))  # optional floor
MAX_NAME_LEN       = int(os.getenv("ENTRY_MAX_NAME_LEN", "40"))
SYMBOL_BLACKLIST   = {
    s.strip().upper()
    for s in os.getenv("ENTRY_SYMBOL_BLACKLIST", "RUG,SCAM").split(",")
    if s.strip()
}

def _passes_filters(msg: Dict[str, Any]) -> bool:
    # Only act on brand-new token creations seen by PumpPortal
    if msg.get("txType") != "create":
        return False

    # Initial buy (PumpPortal sends a float SOL in many payloads)
    try:
        init_buy = float(msg.get("initialBuy", 0))
        if init_buy < MIN_INITIAL_BUY_SOL:
            return False
    except Exception:
        # if field missing/malformed, allow through (or return False to be stricter)
        pass

    # Basic symbol/name hygiene
    sym = (msg.get("symbol") or "").upper()
    if any(bad in sym for bad in SYMBOL_BLACKLIST):
        return False

    name = msg.get("name") or ""
    if len(name) > MAX_NAME_LEN:
        return False

    return True
# -----------------------------------------------------


class Strategy:
    """Streams PumpPortal events and triggers buys for tokens that pass filters."""
    def __init__(self) -> None:
        self._seen_mints: set[str] = set()
        # Use your SCANNER_PERIOD_SEC as a tiny cooldown after a hit
        self._cooldown = float(os.getenv("SCANNER_PERIOD_SEC", "15"))

    async def on_new_token(self, m: Dict[str, Any]) -> None:
        mint = m.get("mint")
        if not mint or mint in self._seen_mints:
            return
        self._seen_mints.add(mint)

        if not _passes_filters(m):
            return

        try:
            await maybe_buy(m)  # this handles DRY_RUN, sizing, slippage, etc.
        except Exception:
            print("[strategy] maybe_buy exception:", flush=True)
            traceback.print_exc()

        # light cooldown to avoid spamming back-to-back tokens
        await asyncio.sleep(self._cooldown)

    async def on_trade(self, m: Dict[str, Any]) -> None:
        # Hook for future: observe whales/liquidity, feed sell AI, etc.
        return


async def stream_loop() -> None:
    client = PumpPortalClient()  # uses PF_PP_KEY / PF_PP_ACCOUNT / PF_PP_MINTS
    strat = Strategy()

    print("[main] Connecting:", client.masked_uri(), flush=True)
    async for ev in client.events():
        k, raw = ev["kind"], ev["raw"]
        if k == "new_token":
            await strat.on_new_token(raw)
        elif k == "trade":
            await strat.on_trade(raw)


async def main() -> None:
    """Never returns: reconnects on socket end/crash."""
    backoff = 3
    while True:
        try:
            await stream_loop()
            print("[main] stream ended (remote closed). Reconnecting...", flush=True)
            backoff = 3  # reset after a clean end
        except asyncio.CancelledError:
            raise
        except Exception:
            print("[main] stream_loop crashed:\n", flush=True)
            traceback.print_exc()
            backoff = min(backoff * 2, 30)
        await asyncio.sleep(backoff)
