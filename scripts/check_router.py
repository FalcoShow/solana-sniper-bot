# scripts/check_router.py
import asyncio
from utils_pkg import router

async def main():
    await router.start()

    usdc = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    sol  = router.SOL_MINT

    d_usdc = await router.get_decimals(usdc)
    d_sol  = await router.get_decimals(sol)
    print("decimals:", {"USDC": d_usdc, "SOL": d_sol})

    # Buy 0.01 SOL worth of USDC (quote only)
    q_buy  = await router.get_buy_quote(mint=usdc, amount_sol=0.01)
    # Sell those tokens back (quote only) if out_amount_raw exists
    out_raw = int(q_buy.get("out_amount_raw") or 0)
    q_sell = await router.get_sell_quote(mint=usdc, amount_raw=max(out_raw, 10))
    # Spot price probe
    px = await router.get_price_ui(usdc)

    print("buy_quote_out_ui:", q_buy.get("out_amount_ui"))
    print("sell_quote_out_sol:", q_sell.get("out_amount_sol"))
    print("price_ui_sol:", px)

    await router.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
