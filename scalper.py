# scalper.py
import os
import asyncio
import logging
from collections import deque
from datetime import datetime, timezone

try:
    import redis.asyncio as aioredis
except Exception:
    aioredis = None

from utils_pkg.core_utils import (
    get_token_price,        # price in SOL (for tokens) or SOL itself
    convert_sol_to_usd,
    swap_token_jupiter,
    get_wallet_tokens,
    send_telegram,
)

# ---- Config from ENV
DAYTRADER_ENABLED = os.getenv("DAYTRADER_ENABLED", "false").lower() == "true"
TIMEFRAME_SEC = 60
LOOKBACK = int(os.getenv("DAYTRADER_LOOKBACK", "600"))  # 10h @ 1m
MAX_CONCURRENT = int(os.getenv("DAYTRADER_MAX_CONCURRENT", "1"))
RISK_PER_TRADE = float(os.getenv("DAYTRADER_RISK_PER_TRADE", "0.005"))  # 0.5%
DD_BREAKER = float(os.getenv("DAYTRADER_MAX_DRAWDOWN_PCT", "0.07"))     # 7%

ALPHA_MIN = float(os.getenv("DAYTRADER_ALPHA_SCORE_MIN", "0.60"))
RSI_BULL = float(os.getenv("DAYTRADER_ALPHA_CONFIRM_RSIBULL", "55"))
STOP_ATR_MULT = float(os.getenv("DAYTRADER_STOP_ATR_MULT", "1.5"))
TP_ATR_MULT = float(os.getenv("DAYTRADER_TP_ATR_MULT", "3.0"))
COOLDOWN_SEC = int(os.getenv("DAYTRADER_COOLDOWN_SEC", "60"))
SLIPPAGE = float(os.getenv("JUPITER_MAX_SLIPPAGE", "0.005"))

WALLET_PUBKEY = os.getenv("WALLET_PUBKEY")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

SOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

logger = logging.getLogger("scalper")
logging.basicConfig(level=logging.INFO)

# ---- Simple TA helpers
def ema(prev: float, price: float, period: int) -> float:
    k = 2.0 / (period + 1.0)
    return price * k + prev * (1.0 - k) if prev else price

def compute_rsi(prices: deque, period: int = 14) -> float:
    if len(prices) < period + 1:
        return 50.0
    gains = 0.0
    losses = 0.0
    for i in range(-period, 0):
        diff = prices[i] - prices[i - 1]
        if diff > 0:
            gains += diff
        else:
            losses -= diff
    rs = (gains / period) / max(1e-9, (losses / period))
    return 100.0 - (100.0 / (1.0 + rs))

def compute_atr(closes: deque, period: int = 14) -> float:
    if len(closes) < period + 1:
        return 0.0
    # Close-to-close proxy ATR (no OHLC)
    trs = [abs(closes[i] - closes[i-1]) for i in range(-period+1, 0)]
    return sum(trs) / max(1, len(trs))

# ---- Redis helpers
async def _redis():
    if aioredis is None:
        return None
    return aioredis.from_url(REDIS_URL, decode_responses=True)

def _key(prefix: str) -> str:
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    return f"{prefix}:{today}"

PNL_KEY = _key("scalper:pnl_usd")
DD_KEY = _key("scalper:max_dd_pct")
STATE_KEY = "scalper:state"  # {"pos":"flat|long", "entry_price_usd":..., "size_sol":...}

# ---- Equity helper
async def _equity_usd() -> float:
    # Use wallet SOL value + token->SOL value -> USD
    tokens = await get_wallet_tokens(WALLET_PUBKEY)
    total_sol = 0.0
    for t in tokens or []:
        mint = t.get("mint")
        bal = float(t.get("balance", 0.0) or 0.0)
        if bal <= 0: continue
        if mint == SOL_MINT:
            total_sol += bal
        else:
            px_in_sol = await get_token_price(mint)
            if px_in_sol and px_in_sol > 0:
                total_sol += bal * px_in_sol
    usd = await convert_sol_to_usd(total_sol)
    return float(usd or 0.0)

async def _price_sol_usd() -> float:
    # Approx via 1 SOL → USD
    return float(await convert_sol_to_usd(1.0) or 0.0)

async def _usdc_balance() -> float:
    tokens = await get_wallet_tokens(WALLET_PUBKEY)
    for t in tokens or []:
        if t.get("mint") == USDC_MINT:
            # For stables, balance "amount" is shown in token units; treat 1 token ≈ $1
            return float(t.get("balance", 0.0) or 0.0)
    return 0.0

async def _sol_balance() -> float:
    tokens = await get_wallet_tokens(WALLET_PUBKEY)
    for t in tokens or []:
        if t.get("mint") == SOL_MINT:
            return float(t.get("balance", 0.0) or 0.0)
    return 0.0

# ---- Trade helpers (USDC <-> SOL)
async def _buy_sol(usdc_to_spend: float) -> tuple[bool, str, float]:
    if usdc_to_spend <= 0:
        return False, "", 0.0
    # For Jupiter, we need amounts in token units; amount_float takes token units
    ok, sig = await swap_token_jupiter(USDC_MINT, SOL_MINT, amount_float=usdc_to_spend, buy=True, slippage=SLIPPAGE)
    # Get resulting SOL by re-querying balance delta (approx)
    sol = await _sol_balance()
    return ok, sig, sol

async def _sell_sol(sol_to_sell: float) -> tuple[bool, str, float]:
    if sol_to_sell <= 0:
        return False, "", 0.0
    ok, sig = await swap_token_jupiter(SOL_MINT, USDC_MINT, amount_float=sol_to_sell, buy=False, slippage=SLIPPAGE)
    usdc = await _usdc_balance()
    return ok, sig, usdc

# ---- Main loop
async def run_scalper():
    if not DAYTRADER_ENABLED:
        logger.info("Daytrader disabled; exiting.")
        return

    r = await _redis()
    prices_usd = deque(maxlen=LOOKBACK)
    ema12 = ema26 = ema200 = 0.0
    in_position = False
    entry_price = 0.0
    size_sol = 0.0

    await send_telegram("📈 Scalper started (SOL/USDC, 1m).")

    while True:
        try:
            # Sample close (USD)
            sol_usd = await _price_sol_usd()
            if sol_usd <= 0:
                await asyncio.sleep(TIMEFRAME_SEC)
                continue

            prices_usd.append(sol_usd)
            ema12 = ema(ema12, sol_usd, 12)
            ema26 = ema(ema26, sol_usd, 26)
            ema200 = ema(ema200, sol_usd, 200) if len(prices_usd) >= 200 else ema200
            rsi = compute_rsi(prices_usd, 14)
            atr = compute_atr(prices_usd, 14)

            alpha = 0.0
            if ema26 and ema200:
                alpha += 0.4 * (1.0 if ema12 > ema26 else 0.0)
                alpha += 0.3 * (1.0 if sol_usd > ema200 else 0.0)
                alpha += 0.3 * (1.0 if rsi >= RSI_BULL else 0.0)

            # Check breaker
            if r:
                dd = await r.get(DD_KEY)
                if dd and float(dd) >= DD_BREAKER:
                    # Stop taking new entries on high drawdown day
                    await asyncio.sleep(TIMEFRAME_SEC)
                    continue

            # Position management
            if in_position:
                # Stops/TP by ATR
                stop = entry_price - STOP_ATR_MULT * atr
                take = entry_price + TP_ATR_MULT * atr
                if sol_usd <= stop or sol_usd >= take:
                    ok, sig, usdc = await _sell_sol(size_sol)
                    if ok:
                        pnl_usd = (sol_usd - entry_price) * size_sol
                        if r:
                            await r.incrbyfloat(PNL_KEY, pnl_usd)
                            # Update day DD
                            realized = float(await r.get(PNL_KEY) or 0.0)
                            eq = await _equity_usd()
                            max_dd = min(0.0, realized) / max(1e-9, eq)
                            await r.set(DD_KEY, abs(max_dd))
                        await send_telegram(f"✅ Scalper exit: {pnl_usd:+.2f} USD  • price {sol_usd:.3f}  • tx {sig}")
                    else:
                        await send_telegram("⚠️ Scalper exit failed; will retry next bar.")
                    in_position = False
                    size_sol = 0.0
                    entry_price = 0.0

            else:
                # Entry
                if len(prices_usd) >= 200 and alpha >= ALPHA_MIN and rsi >= RSI_BULL and ema12 > ema26 and sol_usd > ema200:
                    # Risk sizing
                    eq = await _equity_usd()
                    usd_risk = max(10.0, eq * RISK_PER_TRADE)
                    usdc_bal = await _usdc_balance()
                    usd_to_use = min(usdc_bal, usd_risk)
                    if usd_to_use >= 10.0:
                        ok, sig, size_sol = await _buy_sol(usd_to_use)
                        if ok:
                            in_position = True
                            entry_price = sol_usd
                            await send_telegram(f"🟢 Scalper entry: ${usd_to_use:.2f} USDC → SOL  • px {sol_usd:.3f}  • tx {sig}")
                        else:
                            await send_telegram("⚠️ Scalper entry failed; will retry when conditions persist.")

            await asyncio.sleep(TIMEFRAME_SEC)

        except Exception as e:
            logger.error(f"[SCALPER] loop error: {e}", exc_info=True)
            await asyncio.sleep(COOLDOWN_SEC)

if __name__ == "__main__":
    asyncio.run(run_scalper())
