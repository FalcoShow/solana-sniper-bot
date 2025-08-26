# scripts/quote_debug.py
import os, sys, json, asyncio, argparse
from typing import Any, Dict, List

# Ensure repo root is importable whether you run `python scripts/quote_debug.py`
# or `python -m scripts.quote_debug`
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

# --- Imports from your utils package ---
from utils_pkg.core_utils import init_resources, close_resources, get_jupiter_quote

# Common mints
SOL_MINT  = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"


def _fmt_amount(x: Any, decimals: int) -> str:
    try:
        lamports = int(x)
        return f"{lamports / (10 ** decimals):,.6f}"
    except Exception:
        return str(x)


def _print_route_plan(route_plan: List[Dict[str, Any]], max_hops: int = 6) -> None:
    print(f"routePlan hops: {len(route_plan)}")
    for i, hop in enumerate(route_plan[:max_hops], 1):
        # Typical v6 hop shape: {"swapInfo": {...}, "percent": <int bps?>}
        swap = hop.get("swapInfo", {})
        label = swap.get("label") or swap.get("ammKey") or "?"
        in_mint  = swap.get("inputMint")
        out_mint = swap.get("outputMint")
        out_amt  = swap.get("outAmount")
        pct      = hop.get("percent")
        print(f"  hop {i}: {label}  {in_mint} -> {out_mint}  outAmount={out_amt}  percent={pct}")


def describe_quote(q: Dict[str, Any], in_decimals: int, out_decimals_guess: int) -> None:
    keys = list(q.keys())
    print("Quote keys:", keys)

    # Common v6 fields
    if "aggregatorVersion" in q:
        print("aggregatorVersion:", q["aggregatorVersion"])
    if "timeTaken" in q:
        print("timeTaken:", q["timeTaken"])
    if "contextSlot" in q:
        print("contextSlot:", q["contextSlot"])

    for k in ("swapMode", "slippageBps", "priceImpactPct", "swapUsdValue", "simplerRouteUsed"):
        if k in q:
            print(f"{k}:", q[k])

    # Amounts
    in_amt  = q.get("inAmount")
    out_amt = q.get("outAmount")
    if in_amt is not None:
        print(f"inAmount: {in_amt} ({_fmt_amount(in_amt, in_decimals)})")
    if out_amt is not None:
        print(f"outAmount: {out_amt} ({_fmt_amount(out_amt, out_decimals_guess)})")

    # Route presence (v6)
    route_plan = q.get("routePlan")
    if isinstance(route_plan, list) and len(route_plan) > 0:
        print("Route present (v6).")
        _print_route_plan(route_plan)
        return

    # Legacy shapes that might still show up in some paths
    if "route" in q and q["route"]:
        print("Route present (legacy 'route').")
        return

    data = q.get("data")
    if isinstance(data, list) and len(data) > 0:
        print("Route(s) present (legacy 'data' list).")
        return

    print("NO ROUTE in quote payload.")


async def run(args: argparse.Namespace) -> None:
    amt_lamports = int(round(args.amount * (10 ** args.decimals)))
    print(
        f"Requesting quote: {args.mint_in} -> {args.mint_out} "
        f"amount={args.amount} (lamports={amt_lamports}) slip={args.slip}"
    )

    await init_resources()
    try:
        q = await get_jupiter_quote(args.mint_in, args.mint_out, amt_lamports, args.slip)
        if not q:
            print("Quote: None / empty")
            return

        if args.raw:
            # Raw JSON dump for debugging
            print(json.dumps(q, indent=2 if args.pretty else None))
            return

        # Guess output decimals: if out mint is SOL, assume 9; else try same as input as a fallback
        out_decimals_guess = 9 if args.mint_out == SOL_MINT else args.decimals
        describe_quote(q, args.decimals, out_decimals_guess)

    finally:
        await close_resources()


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Jupiter quote debugger (v6-aware)")
    ap.add_argument("--in", dest="mint_in", default=USDC_MINT, help="input mint (default: USDC)")
    ap.add_argument("--out", dest="mint_out", default=SOL_MINT, help="output mint (default: SOL)")
    ap.add_argument("--amount", type=float, default=0.20, help="amount in input token units")
    ap.add_argument("--decimals", type=int, default=6, help="input token decimals (default: 6)")
    ap.add_argument("--slip", type=float, default=0.01, help="slippage fraction, e.g. 0.01=1%")
    ap.add_argument("--raw", action="store_true", help="print raw JSON response and exit")
    ap.add_argument("--pretty", action="store_true", help="pretty print JSON when --raw is used")
    return ap


def main() -> None:
    ap = build_arg_parser()
    args = ap.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
