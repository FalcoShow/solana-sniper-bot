# scripts/main.py
from __future__ import annotations

import os
import asyncio
from typing import Any, Dict

from utils_pkg.pumpportal_client import PumpPortalClient
from utils_pkg.solana_wallet import load_keypair
from utils_pkg.pump_trader import maybe_buy  # <-- your async trader hook

# ----------------------------
# Config (overridable via ENV)
# ----------------------------
DRY_RUN = int(os.getenv("DRY_RUN", "1"))

# Buy routing / fees
SLIPPAGE_BPS = int(os.getenv("PF_SLIPPAGE_BPS", "1000"))       # 10% default
PRIORITY_SOL = float(os.getenv("PF_PRIORITY_SOL", "0.0005"))

# Basic quality guards
MIN_INITIAL_BUY_SOL = float(os.getenv("MIN_INITIAL_BUY_SOL", "0.15"))
MAX_NAME_LEN = int(os.getenv("MAX_NAME_LEN", "40"))
SYMBOL_BLACKLIST = {
    s.strip().upper()
    for s in os.getenv("SYMBOL_BLACKLIST", "RUG,SCAM").split(",")
    if s.strip()
}

# Behavior
ENTRY_COOLDOWN_SEC = float(os.getenv("ENTRY_COOLDOWN_SEC", "3.0"))


def current_trade_sol() -> float:
    """Read PF_TRADE_SOL at use-time so autosizer updates apply instantly."""
    try:
        return float(os.getenv("PF_TRADE_SOL", "0.01"))
    except Exception:
        return 0.01


def passes_filters(msg: Dict[str, Any]) -> bool:
    """Very light filters; expand/replace with your full filter stack as desired."""
    if not isinstance(msg, dict):
        return False

    # only react to new token creates
    if msg.get("txType") != "create":
        return False

    # initial buy gate (PumpPortal sends this as a float in SOL)
    try:
        init_buy = float(msg.get("initialBuy", 0.0))
    except Exception:
        init_buy = 0.0
    if init_buy < MIN_INITIAL_BUY_SOL:
        return False

    # symbol blacklist
    sym = (msg.get("symbol") or "").upper()
    if any(bad in sym for bad in SYMBOL_BLACKLIST):
        return False

    # keep names short-ish to avoid obvious spam
    name = msg.get("name") or ""
    if len(name) > MAX_NAME_LEN:
        return False

    # optionally: require pool == "pump"
    pool = (msg.get("pool") or "").lower()
    if pool and pool != "pump":
        return False

    return True


class Strategy:
    """Minimal strategy that buys once per mint if it passes filters."""
    def __init__(self):
        self._seen_mints: set[str] = set()
        self._kp = load_keypair()  # solders.Keypair
        self._pub = str(self._kp.pubkey())

    async def on_new_token(self, msg: Dict[str, Any]) -> None:
        mint = msg.get("mint")
        symbol = msg.get("symbol")
        if not mint or mint in self._seen_mints:
            return
        self._seen_mints.add(mint)

        if not passes_filters(msg):
            return

        size_sol = current_trade_sol()

        if DRY_RUN:
            print(f"[DRYRUN] would buy {symbol} ({mint}) for {size_sol} SOL")
        else:
            try:
                # route = "portal" (entries via PumpPortal)
                await maybe_buy(
                    keypair=self._kp,
                    mint=mint,
                    amount_sol=size_sol,
                    venue="portal",
                    slippage_bps=SLIPPAGE_BPS,
                    priority_sol=PRIORITY_SOL,
                )
            except Exception as e:
                print(f"[BUY ERR] {symbol} ({mint}) -> {e}")

        # small spacing so we don't spam builders
        await asyncio.sleep(ENTRY_COOLDOWN_SEC)

    async def on_trade(self, msg: Dict[str, Any]) -> None:
        # Optional: track live trades per mint, liquidity growth, whale prints, etc.
        # You can wire PnL/AI logic here or in a separate process.
        pass


async def main() -> None:
    client = PumpPortalClient()  # uses PF_PP_KEY / PF_PP_ACCOUNT / PF_PP_MINTS
    strat = Strategy()

    print("Connecting to:", client.masked_uri())
    async for ev in client.events():
        k, m = ev["kind"], ev["raw"]
        if k == "new_token":
            await strat.on_new_token(m)
        elif k == "trade":
            await strat.on_trade(m)
        # ignore info/warning/unknown to keep logs clean


if __name__ == "__main__":
    asyncio.run(main())
