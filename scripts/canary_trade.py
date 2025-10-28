# scripts/canary_trade.py
import asyncio
from utils_pkg import router

USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

async def main():
    await router.start()
    ok_buy, sig_buy = await router.buy_via_lightning(mint=USDC, amount_sol=0.01, slippage_bps=60)
    print("buy:", ok_buy, sig_buy)

    # If buy succeeded, immediately sell ~all (use decimals to size)
    if ok_buy:
        dec = await router.mint_decimals(USDC) or 6
        # assume ~10 USDC per 0.01 SOL at SOL≈1000$? (varies) — better to fetch balance,
        # but for canary use the quote we got earlier; here we ask price then sell a small fixed amount:
        q = await router.get_buy_quote(mint=USDC, amount_sol=0.01)
        amount_raw = int(q.get("out_amount_raw") or 0)
        amount_raw = max(1, int(amount_raw * 0.9))  # leave dust
        ok_sell, sig_sell = await router.sell_via_lightning(mint=USDC, token_program=None, amount_raw=amount_raw, slippage_bps=60)
        print("sell:", ok_sell, sig_sell)

    await router.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
