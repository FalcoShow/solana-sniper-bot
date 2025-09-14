# scripts/main.py
from __future__ import annotations

import asyncio
import os
import traceback
from typing import Any, Dict

from utils_pkg.pumpportal_client import PumpPortalClient
from utils_pkg.pump_trader import maybe_buy  # expects mint=..., symbol=..., ui=...

# --------- lightweight, overridable filters ----------
MIN_INITIAL_BUY_SOL = float(os.getenv("ENTRY_MIN_BUY_SOL", "0.0"))  # optional floor
MAX_NAME_LEN       = int(os.getenv("ENTRY_MAX_NAME_LEN", "40"))
SYMBOL_BLACKLIST   = {
    s.strip().upper()
    for s in os.getenv("ENTRY_SYMBOL_BLACKLIST", "RUG,SCAM").split(",")
    if s.strip()
}

def _as_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default

def _extract_mint(msg: Dict[str, Any]) -> str:
    return (
        msg.get("mint")
        or msg.get("mintAddress")
        or msg.get("mint_address")
        or ""
    ).strip()

def _extract_symbol(msg: Dict[str, Any]) -> str:
    return (msg.get("symbol") or msg.get("ticker") or "").strip()

def _extract_name(msg: Dict[str, Any]) -> str:
    return (msg.get("name") or "").strip()

def _passes_filters(msg: Dict[str, Any]) -> bool:
    # Only act on brand-new token creations seen by PumpPortal
    if (msg.get("txType") or msg.get("type")) != "create":
        return False

    init_buy = _as_float(msg.get("initialBuy"), 0.0)
    if init_buy < MIN_INITIAL_BUY_SOL:
        return False

    sym = _extract_symbol(msg).upper()
    if any(bad in sym for bad in SYMBOL_BLACKLIST):
        return False

    name = _extract_name(msg)
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
        mint = _extract_mint(m)
        if not mint:
            return
        if mint in self._seen_mints:
            return
        self._seen_mints.add(mint)

        if not _passes_filters(m):
            return

        try:
            # ✅ match pump_trader.maybe_buy signature
            await maybe_buy(mint=mint, symbol=_extract_symbol(m), ui=m)
        except Exception:
            print("[strategy] maybe_buy exception:", flush=True)
            traceback.print_exc()

        # light cooldown to avoid spamming back-to-back tokens
        await asyncio.sleep(self._cooldown)

    async def on_trade(self, m: Dict[str, Any]) -> None:
        # Hook: observe whales/liquidity, feed sell AI, etc.
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
