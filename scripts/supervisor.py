# scripts/supervisor.py
import os, asyncio, json, numpy as np, signal
from utils_pkg.circuit_breakers import should_halt
from allocator.capital_allocator import run_allocator
from perps.connector_drift import DriftPerps
from perps.risk import ticket_notional, clamp_leverage
from perps.alpha import should_long, should_short

# ---- provide trailing returns hooks for allocator ----
def get_trailing_returns():
    # TODO: replace with PG queries filtered by 'reason' or 'strategy'
    # For now, read from JSON cache if present
    path = os.path.join(os.getenv("DATA_DIR","data"), "trailing_returns.json")
    try:
        with open(path,"r") as f: return json.load(f)
    except Exception:
        return {"daytrader":[], "sniper":[], "perps":[]}

def get_regime_returns():
    # TODO: replace with equity curve computation
    path = os.path.join(os.getenv("DATA_DIR","data"), "equity_returns.npy")
    try: return np.load(path)
    except Exception: return np.array([], dtype=float)

async def perps_manager():
    conn = DriftPerps()
    if not conn.enabled:
        print("[perps] disabled")
        return
    symbol = os.getenv("PERPS_SYMBOL","SOL-PERP")
    while True:
        halt, why = should_halt()
        if halt:
            print(f"[perps] halted: {why}")
            await asyncio.sleep(30)
            continue
        # TODO: fetch features + funding for symbol
        feats = {"ema12":1,"ema26":0.9,"ema50":0.8,"rsi14":56}  # placeholder
        funding8h_sum = 0.0
        equity = float(os.getenv("SIM_EQUITY_USD","2000"))  # replace with wallet equity function
        atr_pct = 0.02  # replace with real ATR%

        notional = ticket_notional(equity, atr_pct)
        lev = clamp_leverage(1.5)
        if should_long(feats, funding8h_sum):
            ok, sig = await conn.place_order(symbol, "long", notional, lev, stop_price=0, take_profit=0)
            print("[perps] long", ok, sig)
        elif should_short(feats, funding8h_sum):
            ok, sig = await conn.place_order(symbol, "short", notional, lev, stop_price=0, take_profit=0)
            print("[perps] short", ok, sig)
        await asyncio.sleep(int(os.getenv("PERPS_POLL_SEC","15")))

async def main():
    # allocator in background
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(getattr(signal, "SIGINT", signal.SIGTERM), lambda: asyncio.create_task(asyncio.sleep(0)))
    import threading
    t = threading.Thread(target=run_allocator, args=(get_trailing_returns, get_regime_returns, int(os.getenv("ALLOCATOR_INTERVAL_SEC","600"))), daemon=True)
    t.start()

    # daytrader runner (your existing module)
    if os.getenv("DAYTRADER_ENABLED","false").lower()=="true":
        from daytrader.runner import run_live
        asyncio.create_task(run_live())

    # perps sleeve
    asyncio.create_task(perps_manager())

    while True:
        await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(main())
