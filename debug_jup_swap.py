# debug_jup_swap.py
import asyncio, os, json
from utils_pkg.router import _jup_quote, _jup_swap, SOL_MINT

USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
async def main():
    # request small quote (same as you did)
    q = await _jup_quote(SOL_MINT, USDC, 4_000_000, slippage_bps=30)
    print("QUOTE KEYS:", list(q.keys()))
    print("QUOTE sample:", json.dumps({k: q.get(k) for k in ("inAmount","outAmount","routePlan","routes","platformFee","swapMode")}, default=str, indent=2)[:2000])
    # attempt the swap POST but catch and print full response
    try:
        # use a fake user pubkey (you can pass your keypair pubkey)
        tx_b64 = await _jup_swap(os.getenv("PUBKEY") or "11111111111111111111111111111111", q, priority_lamports=None, compute_units=None)
        print("tx_b64:", bool(tx_b64))
    except Exception as e:
        # print full exception & str(e)
        print("SWAP EXCEPTION:", repr(e))
asyncio.run(main())
