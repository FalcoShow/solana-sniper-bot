# scripts/main.py
import os, asyncio, datetime as dt
from utils_pkg.pumpportal_client import PumpPortalClient
from utils_pkg.solana_wallet import load_keypair, get_balance_sol
from utils_pkg.pump_trader import maybe_buy  # your buy hook

# === Runtime guardrails (from ENV) ===
DAILY_MAX_DD_PCT = float(os.getenv("PF_DAILY_MAX_DRAWDOWN_PCT", "999"))   # e.g. 8
DAILY_MAX_LOSSES = int(os.getenv("PF_DAILY_MAX_LOSSES", "999"))           # e.g. 4
MAX_CONCURRENT   = int(os.getenv("PF_MAX_CONCURRENT", "4"))               # e.g. 2
COOLDOWN_SEC     = float(os.getenv("PF_COOLDOWN_SEC", "0"))               # e.g. 15

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

class Strategy:
    def __init__(self, guard: Guard):
        self.guard = guard
        self._seen = set()

    async def on_new_token(self, msg: dict):
        mint = msg.get("mint")
        if not mint or mint in self._seen:
            return
        self._seen.add(mint)

        if not await self.guard.should_trade():
            return

        # concurrency guard
        async with self.guard.sema:
            try:
                await maybe_buy(msg)       # your existing buy logic (respects PF_TRADE_SOL autosize)
            finally:
                await self.guard.after_trade()

    async def on_trade(self, msg: dict):
        # (optional) You can use trades to detect rug/stop and call guard.register_loss()
        pass

async def main():
    kp = load_keypair()
    pubkey_str = str(kp.pubkey())
    guard = Guard(pubkey_str)
    client = PumpPortalClient()  # uses PF_PP_KEY / PF_PP_ACCOUNT / PF_PP_MINTS

    print("Connecting to:", client.masked_uri())
    strat = Strategy(guard)

    async for ev in client.events():
        k, m = ev["kind"], ev["raw"]
        if k == "new_token":
            await strat.on_new_token(m)
        elif k == "trade":
            await strat.on_trade(m)

if __name__ == "__main__":
    asyncio.run(main())
