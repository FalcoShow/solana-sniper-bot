# scripts/main.py
import asyncio, time
from utils_pkg.pumpportal_client import PumpPortalClient
import os
from solders.pubkey import Pubkey
from utils_pkg.solana_wallet import load_keypair, sign_and_send_b64_tx
from utils_pkg.pump_trader import build_buy_tx_via_portal

# --- simple heuristics you can tweak ---
MIN_INITIAL_BUY_SOL = 0.15       # ignore tiny prank mints
MAX_NAME_LEN = 30
SYMBOL_BLACKLIST = {"RUG", "SCAM"}  # example

TRADE_SOL = float(os.getenv("PF_TRADE_SOL", "0.01"))
SLIPPAGE_BPS = int(os.getenv("PF_SLIPPAGE_BPS", "1000"))
PRIORITY_SOL = float(os.getenv("PF_PRIORITY_SOL", "0.0005"))

kp_cache = {"kp": None, "pub": None}

def _kp():
    if kp_cache["kp"] is None:
        kp = load_keypair()
        kp_cache["kp"] = kp
        kp_cache["pub"] = str(kp.public_key)
    return kp_cache["kp"], kp_cache["pub"]

async def maybe_buy(msg: dict):
    mint = msg.get("mint")
    symbol = msg.get("symbol")
    kp, pub = _kp()
    print(f"[BUY] {symbol} {mint}  amount={TRADE_SOL} SOL  slip={SLIPPAGE_BPS/100:.2f}%")
    # 1) build unsigned tx (base64)
    tx_b64 = build_buy_tx_via_portal(
        mint=mint,
        payer_pubkey=pub,
        amount_sol=TRADE_SOL,
        slippage_bps=SLIPPAGE_BPS,
        priority_sol=PRIORITY_SOL,
    )
    # 2) sign+send
    sig = await sign_and_send_b64_tx(tx_b64, kp, skip_preflight=True)
    print(f"[OK] sent: https://solscan.io/tx/{sig}")

def passes_filters(msg: dict) -> bool:
    if msg.get("txType") != "create":
        return False
    if msg.get("initialBuy", 0) < MIN_INITIAL_BUY_SOL * 1e8:  # if value is raw? WS sends UI value -> treat as float
        try:
            # portal sends float SOL in 'solAmount' and float in 'initialBuy' (ui)
            if float(msg.get("initialBuy", 0)) < MIN_INITIAL_BUY_SOL:
                return False
        except Exception:
            return False
    sym = (msg.get("symbol") or "").upper()[:64]
    if any(bad in sym for bad in SYMBOL_BLACKLIST):
        return False
    name = (msg.get("name") or "")
    if len(name) > MAX_NAME_LEN:
        return False
    return True

# --- your trade hook (implement later) ---
async def maybe_buy(msg: dict):
    mint = msg.get("mint")
    symbol = msg.get("symbol")
    # TODO: call your swap code here.
    # e.g. await utils_pkg.trader.buy_on_pump(mint, amount_sol=0.01)
    print(f"[DRYRUN] would buy {symbol} ({mint}) for 0.01 SOL")

class Strategy:
    def __init__(self):
        self._seen = set()
        self._cooldown_after_hit = 3.0  # seconds

    async def on_new_token(self, msg: dict):
        mint = msg.get("mint")
        if not mint or mint in self._seen:
            return
        self._seen.add(mint)
        if not passes_filters(msg):
            return
        await maybe_buy(msg)
        await asyncio.sleep(self._cooldown_after_hit)

    async def on_trade(self, msg: dict):
        # optional: track liquidity/volume/rugs here
        pass

async def main():
    client = PumpPortalClient()  # reads PF_PP_KEY / PF_PP_ACCOUNT / PF_PP_MINTS
    strat = Strategy()
    print("Connecting to:", client.masked_uri())
    async for ev in client.events():
        k, m = ev["kind"], ev["raw"]
        if k == "new_token":
            await strat.on_new_token(m)
        elif k == "trade":
            await strat.on_trade(m)

if __name__ == "__main__":
    asyncio.run(main())
