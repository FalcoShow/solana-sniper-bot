from __future__ import annotations

import asyncio
import os
import traceback
from typing import Any, Dict

from utils_pkg.pumpportal_client import PumpPortalClient
from utils_pkg.pump_trader import maybe_buy            # expects: maybe_buy(mint=..., ui=...)
from utils_pkg.trader import sell_on_jupiter, exit_engine, IS_DRYRUN

EXIT_ENGINE_ENABLED = str(os.getenv("EXIT_ENGINE_ENABLED", "1")).strip().lower() in {"1", "true", "yes", "y", "on"}

# ---------- lightweight, overridable filters ----------
MIN_INITIAL_BUY_SOL = float(os.getenv("ENTRY_MIN_BUY_SOL", "0.0"))  # optional floor
MAX_NAME_LEN        = int(os.getenv("ENTRY_MAX_NAME_LEN", "40"))
SYMBOL_BLACKLIST    = {
    s.strip().upper()
    for s in os.getenv("ENTRY_SYMBOL_BLACKLIST", "RUG,SCAM").split(",")
    if s.strip()
}

def _as_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v) if v is not None else default
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

    init_buy = _as_float(msg.get("initialBuy"), 0.0)  # PumpPortal sends a float
    if init_buy < MIN_INITIAL_BUY_SOL:
        return False

    sym = _extract_symbol(msg).upper()
    if any(bad and bad in sym for bad in SYMBOL_BLACKLIST):
        return False

    name = _extract_name(msg)
    if len(name) > MAX_NAME_LEN:
        return False

    return True
# ------------------------------------------------------

async def _post_buy_exit_test(mint: str) -> None:
    """
    Optional smoke-test: if POSTBUY_MODE=immediate_sell_test, wait 5s then
    sell 100% via Jupiter. Otherwise no-op.
    """
    mode = os.getenv("POSTBUY_MODE", "").strip().lower()
    if mode == "immediate_sell_test":
        await asyncio.sleep(5)
        try:
            await sell_on_jupiter(mint, pct_of_wallet=1.0)
            print(f"[post-buy test] sold 100% via Jupiter for {mint}", flush=True)
        except Exception as e:
            print(f"[post-buy test] sell failed for {mint}: {e}", flush=True)

class Strategy:
    """Streams PumpPortal events and triggers buys for tokens that pass filters."""
    def __init__(self) -> None:
        self._seen_mints: set[str] = set()
        self._cooldown = float(os.getenv("SCANNER_PERIOD_SEC", "15"))  # micro-cooldown
        self._max_conc = int(os.getenv("PF_MAX_CONCURRENT", "2"))
        self._sema = asyncio.Semaphore(max(1, self._max_conc))

    async def on_new_token(self, m: Dict[str, Any]) -> None:
        mint = _extract_mint(m)
        if not mint or mint in self._seen_mints:
            return
        self._seen_mints.add(mint)

        if not _passes_filters(m):
            return

        async with self._sema:
            try:
                # ✅ buy path (maybe real, maybe DRYRUN) — pump_trader decides size/slippage/prio
                await maybe_buy(mint=mint, ui=m)

                # 🔔 after a *real* fill:
                # either run the immediate-sell test OR register the position into the exit engine.
                if not IS_DRYRUN and EXIT_ENGINE_ENABLED:
                    if os.getenv("POSTBUY_MODE", "").strip().lower() == "immediate_sell_test":
                        asyncio.create_task(_post_buy_exit_test(mint))
                    else:
                        asyncio.create_task(exit_engine.on_buy_filled(mint))
            except Exception:
                print("[strategy] maybe_buy exception:", flush=True)
                traceback.print_exc()

        # avoid spamming back-to-back tokens
        await asyncio.sleep(self._cooldown)

    async def on_trade(self, m: Dict[str, Any]) -> None:
        # optional analytics/hooks for your own dashboards
        return

async def stream_loop() -> None:
    client = PumpPortalClient()  # uses PF_PP_KEY / PF_PP_ACCOUNT / PF_PP_MINTS
    strat = Strategy()

    print("[main] Connecting:", client.masked_uri(), flush=True)
    async for ev in client.events():
        k, raw = ev["kind"], ev["raw"]
        if k == "new_token":
            await strat.on_new_token(raw)
        elif k == "trade" and EXIT_ENGINE_ENABLED:
            # feed live ticks into exit engine for TP/TP2/TSL/SL
            await exit_engine.on_trade_tick(raw)
            await strat.on_trade(raw)

async def main() -> None:
    """Never returns: reconnects on socket end/crash with backoff."""
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

if __name__ == "__main__":
    asyncio.run(main())
