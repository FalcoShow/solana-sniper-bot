# scripts/main.py
from __future__ import annotations
import asyncio, os, traceback
from typing import Any, Dict
from utils_pkg.pumpportal_client import PumpPortalClient
from utils_pkg.pump_trader import maybe_buy
from utils_pkg.trader import sell_on_jupiter, exit_engine, IS_DRYRUN

EXIT_ENGINE_ENABLED = str(os.getenv("EXIT_ENGINE_ENABLED", "1")).strip().lower() in {"1","true","yes","y","on"}

# --- NEW: normalize mints coming from PumpPortal --------------------------------
def _norm_mint_str(m: str) -> str:
    m = (m or "").strip()
    return m[:-4] if m.lower().endswith("pump") else m
# -------------------------------------------------------------------------------

# ---------- lightweight, overridable filters ----------
MIN_INITIAL_BUY_SOL = float(os.getenv("ENTRY_MIN_BUY_SOL", "0.0"))
MAX_NAME_LEN        = int(os.getenv("ENTRY_MAX_NAME_LEN", "40"))
SYMBOL_BLACKLIST    = { s.strip().upper() for s in os.getenv("ENTRY_SYMBOL_BLACKLIST", "RUG,SCAM").split(",") if s.strip() }

def _as_float(v: Any, default: float = 0.0) -> float:
    try: return float(v) if v is not None else default
    except Exception: return default

def _extract_mint(msg: Dict[str, Any]) -> str:
    raw = (msg.get("mint") or msg.get("mintAddress") or msg.get("mint_address") or "")
    m = _norm_mint_str(raw)
    # optional debug:
    if raw and raw != m and os.getenv("PF_DEBUG_TX") in {"1","true","yes"}:
        print(f"[main] normalized mint {raw} -> {m}", flush=True)
    return m

def _extract_symbol(msg: Dict[str, Any]) -> str: return (msg.get("symbol") or msg.get("ticker") or "").strip()
def _extract_name(msg: Dict[str, Any]) -> str:   return (msg.get("name") or "").strip()

def _passes_filters(msg: Dict[str, Any]) -> bool:
    if (msg.get("txType") or msg.get("type")) != "create": return False
    if _as_float(msg.get("initialBuy"), 0.0) < MIN_INITIAL_BUY_SOL: return False
    sym = _extract_symbol(msg).upper()
    if any(bad and bad in sym for bad in SYMBOL_BLACKLIST): return False
    if len(_extract_name(msg)) > MAX_NAME_LEN: return False
    return True
# ------------------------------------------------------

async def _post_buy_exit_test(mint: str) -> None:
    if os.getenv("POSTBUY_MODE", "").strip().lower() == "immediate_sell_test":
        await asyncio.sleep(5)
        try:
            await sell_on_jupiter(mint, pct_of_wallet=1.0)
            print(f"[post-buy test] sold 100% via Jupiter for {mint}", flush=True)
        except Exception as e:
            print(f"[post-buy test] sell failed for {mint}: {e}", flush=True)

class Strategy:
    def __init__(self) -> None:
        self._seen_mints: set[str] = set()
        self._cooldown = float(os.getenv("SCANNER_PERIOD_SEC", "15"))
        self._sema = asyncio.Semaphore(max(1, int(os.getenv("PF_MAX_CONCURRENT", "2"))))

    async def on_new_token(self, m: Dict[str, Any]) -> None:
        mint = _extract_mint(m)
        if not mint or mint in self._seen_mints: return
        self._seen_mints.add(mint)
        if not _passes_filters(m): return

        async with self._sema:
            try:
                sig = await maybe_buy(mint=mint, ui=m)  # we now pass a clean mint
                if not IS_DRYRUN and EXIT_ENGINE_ENABLED and sig:
                    if os.getenv("POSTBUY_MODE", "").strip().lower() == "immediate_sell_test":
                        asyncio.create_task(_post_buy_exit_test(mint))
                    else:
                        asyncio.create_task(exit_engine.on_buy_filled(mint, tx_sig=sig))
            except Exception:
                print("[strategy] maybe_buy exception:", flush=True)
                traceback.print_exc()

        await asyncio.sleep(self._cooldown)

    async def on_trade(self, m: Dict[str, Any]) -> None:
        return

async def stream_loop() -> None:
    client = PumpPortalClient()
    strat = Strategy()
    print("[main] Connecting:", client.masked_uri(), flush=True)
    async for ev in client.events():
        k, raw = ev["kind"], ev["raw"]
        if k == "new_token": await strat.on_new_token(raw)
        elif k == "trade" and EXIT_ENGINE_ENABLED:
            await exit_engine.on_trade_tick(raw)
            await strat.on_trade(raw)

async def main() -> None:
    backoff = 3
    while True:
        try:
            await stream_loop()
            print("[main] stream ended (remote closed). Reconnecting...", flush=True)
            backoff = 3
        except asyncio.CancelledError:
            raise
        except Exception:
            print("[main] stream_loop crashed:\n", flush=True)
            traceback.print_exc()
            backoff = min(backoff * 2, 30)
        await asyncio.sleep(backoff)

if __name__ == "__main__":
    asyncio.run(main())
