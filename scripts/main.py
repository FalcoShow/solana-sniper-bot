# scripts/main.py
import os, asyncio, datetime as dt
from utils_pkg.pumpportal_client import PumpPortalClient
from utils_pkg.solana_wallet import load_keypair, get_balance_sol
from utils_pkg.pump_trader import maybe_buy  # your buy hook

import asyncio, os, traceback
from utils_pkg.pumpportal_client import PumpPortalClient
from utils_pkg.pump_trader import maybe_buy  # your buy path (DRY_RUN respected inside)

# === Runtime guardrails (from ENV) ===
DAILY_MAX_DD_PCT = float(os.getenv("PF_DAILY_MAX_DRAWDOWN_PCT", "999"))   # e.g. 8
DAILY_MAX_LOSSES = int(os.getenv("PF_DAILY_MAX_LOSSES", "999"))           # e.g. 4
MAX_CONCURRENT   = int(os.getenv("PF_MAX_CONCURRENT", "4"))               # e.g. 2
COOLDOWN_SEC     = float(os.getenv("PF_COOLDOWN_SEC", "0"))               # e.g. 15

MIN_INITIAL_BUY_SOL = float(os.getenv("ENTRY_MIN_BUY_SOL", "0.0"))  # optional
MAX_NAME_LEN = 40
SYMBOL_BLACKLIST = {"RUG", "SCAM"}

def _passes_filters(msg: dict) -> bool:
    # Only consider brand new token events from PumpPortal
    if msg.get("txType") != "create":
        return False
    try:
        init_buy = float(msg.get("initialBuy", 0))
        if init_buy < MIN_INITIAL_BUY_SOL:
            return False
    except Exception:
        pass
    sym = (msg.get("symbol") or "").upper()
    if any(bad in sym for bad in SYMBOL_BLACKLIST):
        return False
    name = (msg.get("name") or "")
    if len(name) > MAX_NAME_LEN:
        return False
    return True

class Strategy:
    def __init__(self):
        self._seen_mints: set[str] = set()
        self._cooldown = float(os.getenv("SCANNER_PERIOD_SEC", "15"))

    async def on_new_token(self, m: dict):
        mint = m.get("mint")
        if not mint or mint in self._seen_mints:
            return
        self._seen_mints.add(mint)

        if not _passes_filters(m):
            return

        try:
            await maybe_buy(m)
        except Exception:
            print("[strategy] maybe_buy exception:", flush=True)
            traceback.print_exc()
        await asyncio.sleep(self._cooldown)

    async def on_trade(self, m: dict):
        # hook for later (track whales/liquidity, trigger AI sell, etc.)
        pass

async def stream_loop():
    client = PumpPortalClient()  # reads PF_PP_KEY / PF_PP_ACCOUNT / PF_PP_MINTS
    strat = Strategy()
    print("[main] Connecting:", client.masked_uri(), flush=True)

    async for ev in client.events():
        k, raw = ev["kind"], ev["raw"]
        if k == "new_token":
            await strat.on_new_token(raw)
        elif k == "trade":
            await strat.on_trade(raw)

async def main():
    # run forever, reconnecting if the websocket ends
    while True:
        try:
            await stream_loop()
            print("[main] stream ended (remote closed). Reconnecting...", flush=True)
        except asyncio.CancelledError:
            raise
        except Exception:
            print("[main] stream_loop crashed:\n", flush=True)
            traceback.print_exc()
        await asyncio.sleep(3)  # small backoff before reconnect
# You can keep your other filter/env knobs in here as needed

class Guard:
   

 """Tracks daily drawdown, loss count, and enforces concurrency/cooldown."""
    def __init__(self, pubkey_str: str):
        self.pubkey_str = pubkey_str
        self.day = dt.date.today()
        self.start_bal_sol: float | None = None
        self.sema = asyncio.Semaphore(MAX_CONCURRENT)
        self.losses_today = 0
        self.paused_reason: str | None = None
        self.cooldown_sec = COOLDOWN_SEC
        self._cooling = asyncio.Lock()  # used only to sequence cooldown sleeps

    async def _maybe_roll_day(self):
        today = dt.date.today()
        if today != self.day:
            self.day = today
            self.start_bal_sol = None
            self.losses_today = 0
            self.paused_reason = None
            print(f"[guard] New day -> counters reset.")

    async def refresh_start_balance(self):
        if self.start_bal_sol is None:
            bal = await get_balance_sol(self.pubkey_str)
            self.start_bal_sol = max(bal, 1e-9)
            print(f"[guard] start_of_day_balance={self.start_bal_sol:.6f} SOL")

    async def should_trade(self) -> bool:
        if self.paused_reason:
            print(f"[guard] trading paused: {self.paused_reason}")
            return False

        await self._maybe_roll_day()
        await self.refresh_start_balance()

        # drawdown check
        try:
            cur = await get_balance_sol(self.pubkey_str)
        except Exception as e:
            print(f"[guard] balance fetch failed: {e} (allowing trade)")
            return True

        dd_pct = 0.0
        if self.start_bal_sol and self.start_bal_sol > 0:
            dd_pct = max(0.0, (self.start_bal_sol - cur) / self.start_bal_sol * 100.0)

        if dd_pct >= DAILY_MAX_DD_PCT:
            self.paused_reason = f"daily drawdown {dd_pct:.2f}% ≥ {DAILY_MAX_DD_PCT}%"
            print(f"[guard] {self.paused_reason}")
            return False

        if self.losses_today >= DAILY_MAX_LOSSES:
            self.paused_reason = f"daily losses {self.losses_today} ≥ {DAILY_MAX_LOSSES}"
            print(f"[guard] {self.paused_reason}")
            return False

        return True

    async def after_trade(self):
        # Global cooldown between fresh entries
        if self.cooldown_sec > 0:
            async with self._cooling:
                await asyncio.sleep(self.cooldown_sec)

    # Optional helper you can call when a hard stop triggers to count a loss
    def register_loss(self):
        self.losses_today += 1
        print(f"[guard] loss counted -> losses_today={self.losses_today}")


if __name__ == "__main__":
    asyncio.run(main())
