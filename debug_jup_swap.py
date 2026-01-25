#!/usr/bin/env python3
from __future__ import annotations

"""
debug_jup_swap.py — Jupiter quote/swap debug harness (P2 / dev-only)
===================================================================

What this is
------------
A *safe* diagnostic script that validates:
- your utils_pkg.router Jupiter connectivity
- quote response shape (keys, route plan)
- (optional) swap tx build endpoint behavior (NO signing/broadcast here)

This is intentionally dev-only. Keep it in:
  tools/debug_jup_swap.py   (recommended)
or archive under scripts/ if you prefer, but DO NOT import this in production.

Usage
-----
# Quote SOL -> USDC (default)
python debug_jup_swap.py --mode quote --amount-sol 0.004 --slippage-bps 30

# Attempt to build a swap tx payload (requires pubkey)
python debug_jup_swap.py --mode swap --amount-sol 0.004 --slippage-bps 30 --pubkey <YOUR_PUBKEY>

Env
---
Optional:
  PUBKEY=<YOUR_PUBKEY>    (if --pubkey not provided)

Notes
-----
- This script tries hard to be compatible across router refactors by discovering
  quote/swap callables dynamically (prefers public functions; falls back to _jup_*).
- If your router exposes different function names, add them to the resolver lists below.
"""

import argparse
import asyncio
import json
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────
try:
    from logging_setup import get_logger  # canonical

    LOG = get_logger("debug_jup_swap")
except Exception:  # pragma: no cover
    import logging

    logging.basicConfig(
        level=logging.INFO, format="[debug_jup_swap] %(asctime)s %(levelname)s %(message)s"
    )
    LOG = logging.getLogger("debug_jup_swap")


# ──────────────────────────────────────────────────────────────────────────────
# Constants / defaults
# ──────────────────────────────────────────────────────────────────────────────
DEFAULT_SOL_MINT = "So11111111111111111111111111111111111111112"
DEFAULT_USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
LAMPORTS_PER_SOL = 1_000_000_000


def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    return (v if v is not None else d).strip()


def _to_lamports(amount_sol: float) -> int:
    if amount_sol <= 0:
        return 0
    return max(1, int(amount_sol * LAMPORTS_PER_SOL))


def _pretty(obj: Any, max_chars: int = 4000) -> str:
    try:
        s = json.dumps(obj, indent=2, default=str, sort_keys=True)
    except Exception:
        s = str(obj)
    if len(s) > max_chars:
        return s[:max_chars] + "\n...<truncated>..."
    return s


def _pick(d: dict[str, Any], keys: tuple[str, ...]) -> dict[str, Any]:
    return {k: d.get(k) for k in keys if k in d}


# ──────────────────────────────────────────────────────────────────────────────
# Router callable discovery (forward-compatible)
# ──────────────────────────────────────────────────────────────────────────────
QuoteFn = Callable[..., Awaitable[dict[str, Any]]]
SwapFn = Callable[..., Awaitable[Any]]


@dataclass(frozen=True)
class RouterAPI:
    sol_mint: str
    quote_fn: QuoteFn
    swap_fn: SwapFn | None


def _get_attr(module: Any, name: str) -> Any | None:
    try:
        return getattr(module, name)
    except Exception:
        return None


def _resolve_router() -> RouterAPI:
    """
    Tries to discover a stable quote/swap interface from utils_pkg.router.

    Preferred (public) names for quote:
      - get_quote
      - quote
      - get_buy_quote
      - get_sell_quote
    Fallback (private/legacy):
      - _jup_quote

    Preferred (public) names for swap build:
      - get_swap_tx
      - build_swap_tx
      - swap
      - execute_swap   (if it *builds* and does not broadcast; depends on your impl)
    Fallback (private/legacy):
      - _jup_swap
    """
    try:
        from utils_pkg import router as R  # canonical
    except Exception as e:
        raise RuntimeError(f"Failed to import utils_pkg.router: {e}") from e

    sol_mint = str(_get_attr(R, "SOL_MINT") or DEFAULT_SOL_MINT)

    # quote
    quote_candidates = ["get_quote", "quote", "get_buy_quote", "get_sell_quote", "_jup_quote"]
    quote_fn: QuoteFn | None = None
    quote_name: str | None = None
    for n in quote_candidates:
        fn = _get_attr(R, n)
        if callable(fn):
            quote_fn = fn
            quote_name = n
            break
    if quote_fn is None:
        raise RuntimeError(
            "No quote function found in utils_pkg.router (expected one of: "
            + ", ".join(quote_candidates)
            + ")"
        )

    # swap (optional)
    swap_candidates = ["get_swap_tx", "build_swap_tx", "swap", "execute_swap", "_jup_swap"]
    swap_fn: SwapFn | None = None
    swap_name: str | None = None
    for n in swap_candidates:
        fn = _get_attr(R, n)
        if callable(fn):
            swap_fn = fn
            swap_name = n
            break

    LOG.info(
        "Router discovered: SOL_MINT=%s quote_fn=%s swap_fn=%s",
        sol_mint,
        quote_name,
        swap_name or "None",
    )
    return RouterAPI(sol_mint=sol_mint, quote_fn=quote_fn, swap_fn=swap_fn)


async def _call_quote(
    api: RouterAPI, in_mint: str, out_mint: str, amount_raw: int, slippage_bps: int
) -> dict[str, Any]:
    """
    Calls router quote using best-effort signature compatibility.
    """
    fn = api.quote_fn

    # Try most common signatures first
    # (inputMint, outputMint, amount, slippage_bps=..)
    try:
        return await fn(in_mint, out_mint, amount_raw, slippage_bps=slippage_bps)  # type: ignore[misc]
    except TypeError:
        pass

    # (in_mint=, out_mint=, amount_raw=, slippage_bps=)
    try:
        return await fn(
            in_mint=in_mint, out_mint=out_mint, amount_raw=amount_raw, slippage_bps=slippage_bps
        )  # type: ignore[misc]
    except TypeError:
        pass

    # (inputMint=, outputMint=, amount=, slippageBps=)
    try:
        return await fn(
            inputMint=in_mint, outputMint=out_mint, amount=amount_raw, slippageBps=slippage_bps
        )  # type: ignore[misc]
    except TypeError as e:
        raise RuntimeError(f"Quote call failed due to incompatible signature: {e}") from e


async def _call_swap_build(
    api: RouterAPI,
    user_pubkey: str,
    quote: dict[str, Any],
    priority_lamports: int | None,
    compute_units: int | None,
) -> Any:
    """
    Calls router swap builder using best-effort signature compatibility.

    Important: this is intended to ONLY build tx(s). If your router's 'swap'
    function broadcasts, do NOT use this script in swap mode until you expose
    a build-only API.
    """
    if api.swap_fn is None:
        raise RuntimeError("No swap function found in utils_pkg.router (swap mode unavailable).")

    fn = api.swap_fn

    # Try common signatures:
    # (user_pubkey, quote, priority_lamports=None, compute_units=None)
    try:
        return await fn(
            user_pubkey, quote, priority_lamports=priority_lamports, compute_units=compute_units
        )  # type: ignore[misc]
    except TypeError:
        pass

    # keyword-style
    try:
        return await fn(
            user_pubkey=user_pubkey,
            quote=quote,
            priority_lamports=priority_lamports,
            compute_units=compute_units,
        )  # type: ignore[misc]
    except TypeError:
        pass

    # Some routers use: (user_pubkey, quote, priority_lamports, compute_units)
    try:
        return await fn(user_pubkey, quote, priority_lamports, compute_units)  # type: ignore[misc]
    except TypeError as e:
        raise RuntimeError(f"Swap call failed due to incompatible signature: {e}") from e


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
def _build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Debug Jupiter quote/swap via utils_pkg.router")
    p.add_argument(
        "--mode",
        choices=["quote", "swap"],
        default="quote",
        help="quote only, or attempt swap-tx build",
    )
    p.add_argument("--out-mint", default=DEFAULT_USDC_MINT, help="Output mint (default USDC)")
    p.add_argument("--in-mint", default="", help="Input mint (default SOL mint from router)")
    p.add_argument(
        "--amount-sol", type=float, default=0.004, help="Amount in SOL (only used if input is SOL)"
    )
    p.add_argument(
        "--amount-raw",
        type=int,
        default=0,
        help="Override amount raw base units (takes precedence)",
    )
    p.add_argument("--slippage-bps", type=int, default=30, help="Slippage in bps")
    p.add_argument("--pubkey", default="", help="User pubkey for swap build (or set env PUBKEY)")
    p.add_argument(
        "--priority-lamports", type=int, default=0, help="Optional priority fee lamports (0 = none)"
    )
    p.add_argument(
        "--compute-units",
        type=int,
        default=0,
        help="Optional compute unit limit (0 = router default)",
    )
    p.add_argument("--print-full", action="store_true", help="Print full quote JSON (may be large)")
    return p


async def _amain() -> int:
    args = _build_argparser().parse_args()

    api = _resolve_router()
    in_mint = args.in_mint.strip() or api.sol_mint
    out_mint = args.out_mint.strip()

    if not out_mint:
        LOG.error("out_mint required")
        return 2

    # amount
    amount_raw = int(args.amount_raw or 0)
    if amount_raw <= 0:
        if in_mint == api.sol_mint:
            amount_raw = _to_lamports(float(args.amount_sol))
        else:
            LOG.error("Non-SOL input requires --amount-raw (base units).")
            return 2

    slippage_bps = max(1, int(args.slippage_bps))

    LOG.info(
        "Requesting quote in=%s out=%s amount_raw=%s slippage_bps=%s",
        in_mint,
        out_mint,
        amount_raw,
        slippage_bps,
    )
    quote = await _call_quote(api, in_mint, out_mint, amount_raw, slippage_bps)

    if not isinstance(quote, dict) or not quote:
        LOG.error("Quote failed or empty response.")
        return 1

    # Print a compact summary
    summary_keys = (
        "inAmount",
        "outAmount",
        "swapMode",
        "priceImpactPct",
        "otherAmountThreshold",
        "contextSlot",
        "timeTaken",
    )
    route_keys = ("routePlan", "routes", "platformFee")
    summary = _pick(quote, summary_keys)
    _pick(quote, route_keys)

    print("\n=== QUOTE SUMMARY ===")
    print(_pretty(summary))

    # routePlan can be huge; show first step summary
    rp = quote.get("routePlan") or quote.get("routes")
    if isinstance(rp, list) and rp:
        print("\n=== ROUTE (first hop preview) ===")
        print(_pretty(rp[0]))

    if args.print_full:
        print("\n=== FULL QUOTE JSON ===")
        print(_pretty(quote, max_chars=200000))

    if args.mode == "quote":
        return 0

    # swap build mode
    pubkey = args.pubkey.strip() or _envs("PUBKEY", "")
    if not pubkey:
        LOG.error("Swap mode requires --pubkey or env PUBKEY.")
        return 2

    priority_lamports = int(args.priority_lamports or 0) or None
    compute_units = int(args.compute_units or 0) or None

    LOG.info(
        "Attempting swap-tx build for pubkey=%s (priority=%s cu=%s)",
        pubkey,
        priority_lamports,
        compute_units,
    )

    try:
        swap_result = await _call_swap_build(api, pubkey, quote, priority_lamports, compute_units)
    except Exception as e:
        print("\n=== SWAP BUILD ERROR ===")
        print(repr(e))
        return 1

    print("\n=== SWAP BUILD RESULT (type) ===")
    print(type(swap_result))

    # Many Jupiter responses include {"swapTransaction": "..."} or a base64 string
    if isinstance(swap_result, str | bytes):
        print("\n=== SWAP TX (base64 present?) ===")
        print(bool(swap_result))
    elif isinstance(swap_result, dict):
        # print likely fields
        likely = {}
        for k in (
            "swapTransaction",
            "tx",
            "transaction",
            "setupTransaction",
            "cleanupTransaction",
            "addressLookupTableAddresses",
        ):
            if k in swap_result:
                likely[k] = swap_result.get(k)
        print("\n=== SWAP RESULT (selected fields) ===")
        print(_pretty(likely if likely else swap_result))
    else:
        print("\n=== SWAP RESULT (repr) ===")
        print(repr(swap_result)[:2000])

    return 0


def main() -> None:
    try:
        rc = asyncio.run(_amain())
    except KeyboardInterrupt:
        rc = 130
    except Exception as e:
        LOG.exception("fatal: %s", e)
        rc = 1
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
