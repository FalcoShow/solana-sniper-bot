#!/usr/bin/env python3
from __future__ import annotations

"""
debug_jup_swap.py — Jupiter quote/swap debug harness (DEV-ONLY)
===============================================================

What this is
------------
A *safe* diagnostic script that validates:
- your utils_pkg.router Jupiter connectivity (quote call path)
- quote response shape (keys, route plan)
- (optional) swap tx build endpoint behavior (NO signing/broadcast here)

This is intentionally dev-only. Keep it in:
  tools/debug_jup_swap.py   (recommended)

DO NOT import this from production code.

Notes
-----
- This script tries hard to be compatible across router refactors by discovering
  quote/swap callables dynamically (prefers public functions; falls back to _jup_*).
- Swap mode assumes the discovered "swap" callable is BUILD-ONLY.
  If your router's swap broadcasts, DO NOT run swap mode until a build-only API is exposed.

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
"""

import argparse
import asyncio
import contextlib
import json
import logging
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Final, Protocol, TypeAlias, cast

# ──────────────────────────────────────────────────────────────────────────────
# Phase A — Logging contract (EventLogger + safe get_logger)
# ──────────────────────────────────────────────────────────────────────────────


class EventLogger(Protocol):
    handlers: list[Any]

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def info(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def error(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def log(self, level: int, msg: str, *args: Any, **kwargs: Any) -> None: ...


class _EventCapable(EventLogger, Protocol):
    def event(self, name: str, **fields: Any) -> None: ...


class _LoggerProxy:
    """
    Adapter that guarantees `.handlers` exists for typing compatibility,
    while forwarding standard logging methods to the underlying logger.
    """

    def __init__(self, base: Any):
        self._base = base
        self.handlers: list[Any] = []
        with contextlib.suppress(Exception):
            h = getattr(base, "handlers", None)
            if isinstance(h, list):
                self.handlers = h

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        cast(Any, self._base).debug(msg, *args, **kwargs)

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        cast(Any, self._base).info(msg, *args, **kwargs)

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        fn = getattr(self._base, "warning", None) or getattr(self._base, "warn", None)
        if callable(fn):
            fn(msg, *args, **kwargs)
            return
        self.info(msg, *args, **kwargs)

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        cast(Any, self._base).error(msg, *args, **kwargs)

    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None:
        fn = getattr(self._base, "exception", None)
        if callable(fn):
            fn(msg, *args, **kwargs)
            return
        self.error(msg, *args, **kwargs)

    def log(self, level: int, msg: str, *args: Any, **kwargs: Any) -> None:
        cast(Any, self._base).log(level, msg, *args, **kwargs)

    def event(self, name: str, **fields: Any) -> None:
        fn = getattr(self._base, "event", None)
        if callable(fn):
            fn(name, **fields)
            return
        self.info("%s %s", name, fields)


def get_logger(name: str | None = None) -> EventLogger:
    """
    Always returns an EventLogger-compatible object.

    Priority:
      1) utils_pkg.log_compat.get_event_logger / get_logger
      2) logging_setup.get_logger
      3) stdlib logging.getLogger (basicConfig if needed)
    """
    nm = (name or "debug_jup_swap").strip() or "debug_jup_swap"

    with contextlib.suppress(Exception):
        from utils_pkg.log_compat import get_event_logger  # type: ignore

        return cast(EventLogger, _LoggerProxy(get_event_logger(nm)))

    with contextlib.suppress(Exception):
        from utils_pkg.log_compat import get_logger as gl  # type: ignore

        return cast(EventLogger, _LoggerProxy(gl(nm)))

    with contextlib.suppress(Exception):
        from logging_setup import get_logger as gl2  # type: ignore

        return cast(EventLogger, _LoggerProxy(gl2(nm)))

    lg = logging.getLogger(nm)
    if not lg.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="[debug_jup_swap] %(asctime)s %(levelname)s %(name)s %(message)s",
        )
    return cast(EventLogger, _LoggerProxy(lg))


LOG: EventLogger = get_logger("debug_jup_swap")


def _event(name: str, **fields: Any) -> None:
    """Best-effort structured event. Never raises."""
    try:
        fn = getattr(LOG, "event", None)
        if callable(fn):
            cast(_EventCapable, LOG).event(str(name), **fields)
        else:
            LOG.info("%s %s", str(name), fields)
    except Exception:
        return


# ──────────────────────────────────────────────────────────────────────────────
# Constants / defaults
# ──────────────────────────────────────────────────────────────────────────────
DEFAULT_SOL_MINT: Final[str] = "So11111111111111111111111111111111111111112"
DEFAULT_USDC_MINT: Final[str] = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
LAMPORTS_PER_SOL: Final[int] = 1_000_000_000

JsonDict: TypeAlias = dict[str, Any]

# Router callables are intentionally "Any-return" because signatures vary across refactors.
QuoteFn: TypeAlias = Callable[..., Awaitable[Any]]
SwapFn: TypeAlias = Callable[..., Awaitable[Any]]


def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    return (v if v is not None else d).strip()


def _to_lamports(amount_sol: float) -> int:
    if float(amount_sol) <= 0.0:
        return 0
    return max(1, int(float(amount_sol) * float(LAMPORTS_PER_SOL)))


def _pretty(obj: Any, max_chars: int = 4000) -> str:
    try:
        s = json.dumps(obj, indent=2, default=str, sort_keys=True)
    except Exception:
        s = str(obj)
    if len(s) > max_chars:
        return s[:max_chars] + "\n...<truncated>..."
    return s


def _pick(d: JsonDict, keys: tuple[str, ...]) -> JsonDict:
    out: JsonDict = {}
    for k in keys:
        if k in d:
            out[k] = d.get(k)
    return out


# ──────────────────────────────────────────────────────────────────────────────
# Router callable discovery (forward-compatible)
# ──────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class RouterAPI:
    sol_mint: str
    quote_fn: QuoteFn
    quote_name: str
    swap_fn: SwapFn | None
    swap_name: str | None


def _get_attr(module: Any, name: str) -> Any | None:
    try:
        return getattr(module, name)
    except Exception:
        return None


def _resolve_router() -> RouterAPI:
    """
    Discover a stable quote/swap build interface from utils_pkg.router.

    Quote candidates (prefer public):
      - get_quote_with_age_io
      - get_quote
      - quote
      - get_buy_quote
      - get_sell_quote
      - _jup_quote   (legacy/private)

    Swap build candidates (optional; prefer build-only names):
      - get_swap_tx
      - build_swap_tx
      - build_swap
      - get_swap
      - swap          (ONLY if it builds, not broadcasts)
      - _jup_swap     (legacy/private)
    """
    try:
        from utils_pkg import router as R  # type: ignore[import-not-found]
    except Exception as e:
        raise RuntimeError(f"Failed to import utils_pkg.router: {e}") from e

    sol_mint = str(_get_attr(R, "SOL_MINT") or DEFAULT_SOL_MINT)

    quote_candidates = [
        "get_quote_with_age_io",
        "get_quote",
        "quote",
        "get_buy_quote",
        "get_sell_quote",
        "_jup_quote",
    ]
    quote_fn: QuoteFn | None = None
    quote_name: str | None = None
    for n in quote_candidates:
        fn = _get_attr(R, n)
        if callable(fn):
            quote_fn = cast(QuoteFn, fn)
            quote_name = n
            break
    if quote_fn is None or quote_name is None:
        raise RuntimeError(
            "No quote function found in utils_pkg.router (expected one of: "
            + ", ".join(quote_candidates)
            + ")"
        )

    swap_candidates = [
        "get_swap_tx",
        "build_swap_tx",
        "build_swap",
        "get_swap",
        "swap",
        "_jup_swap",
    ]
    swap_fn: SwapFn | None = None
    swap_name: str | None = None
    for n in swap_candidates:
        fn = _get_attr(R, n)
        if callable(fn):
            swap_fn = cast(SwapFn, fn)
            swap_name = n
            break

    LOG.info(
        "Router discovered: SOL_MINT=%s quote_fn=%s swap_fn=%s",
        sol_mint,
        quote_name,
        swap_name or "None",
    )
    _event("DEBUG_JUP_SWAP_ROUTER", sol_mint=sol_mint, quote_fn=quote_name, swap_fn=swap_name)

    return RouterAPI(
        sol_mint=sol_mint,
        quote_fn=quote_fn,
        quote_name=quote_name,
        swap_fn=swap_fn,
        swap_name=swap_name,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Best-effort call adapters (signature-flex)
# ──────────────────────────────────────────────────────────────────────────────
def _ensure_dict(obj: Any, *, where: str) -> JsonDict:
    if isinstance(obj, dict):
        return cast(JsonDict, obj)
    raise RuntimeError(f"Expected dict result from {where}, got {type(obj)!r}")


async def _call_quote(
    api: RouterAPI,
    in_mint: str,
    out_mint: str,
    amount_raw: int,
    slippage_bps: int,
) -> JsonDict:
    """
    Calls the router quote function using best-effort signature compatibility.
    Returns a dict or raises RuntimeError.
    """
    fn = api.quote_fn

    # 1) common positional + keyword slippage
    try:
        res = await fn(in_mint, out_mint, amount_raw, slippage_bps=slippage_bps)  # type: ignore[misc]
        return _ensure_dict(res, where=f"{api.quote_name}(positional)")
    except TypeError:
        pass

    # 2) common keyword style (snake)
    try:
        res = await fn(
            in_mint=in_mint,
            out_mint=out_mint,
            amount_raw=amount_raw,
            slippage_bps=slippage_bps,
        )  # type: ignore[misc]
        return _ensure_dict(res, where=f"{api.quote_name}(snake_kwargs)")
    except TypeError:
        pass

    # 3) Jupiter-style keys (camel)
    try:
        res = await fn(
            inputMint=in_mint,
            outputMint=out_mint,
            amount=amount_raw,
            slippageBps=slippage_bps,
        )  # type: ignore[misc]
        return _ensure_dict(res, where=f"{api.quote_name}(camel_kwargs)")
    except TypeError as e:
        raise RuntimeError(f"Quote call failed due to incompatible signature: {e}") from e


async def _call_swap_build(
    api: RouterAPI,
    user_pubkey: str,
    quote: JsonDict,
    priority_lamports: int | None,
    compute_units: int | None,
) -> Any:
    """
    Calls router swap builder using best-effort signature compatibility.

    IMPORTANT:
      This is intended to ONLY build tx(s).
      If your router's swap broadcasts, do NOT run swap mode.
    """
    if api.swap_fn is None or api.swap_name is None:
        raise RuntimeError("No swap function found in utils_pkg.router (swap mode unavailable).")

    fn = api.swap_fn

    # 1) positional (pubkey, quote) + keywords
    try:
        return await fn(
            user_pubkey,
            quote,
            priority_lamports=priority_lamports,
            compute_units=compute_units,
        )  # type: ignore[misc]
    except TypeError:
        pass

    # 2) keyword style
    try:
        return await fn(
            user_pubkey=user_pubkey,
            quote=quote,
            priority_lamports=priority_lamports,
            compute_units=compute_units,
        )  # type: ignore[misc]
    except TypeError:
        pass

    # 3) pure positional fallback
    try:
        return await fn(user_pubkey, quote, priority_lamports, compute_units)  # type: ignore[misc]
    except TypeError as e:
        raise RuntimeError(f"Swap call failed due to incompatible signature: {e}") from e


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────
def _build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Debug Jupiter quote/swap via utils_pkg.router")
    p.add_argument(
        "--mode",
        choices=["quote", "swap"],
        default="quote",
        help="quote only, or attempt swap-tx build (build-only!)",
    )
    p.add_argument("--out-mint", default=DEFAULT_USDC_MINT, help="Output mint (default USDC)")
    p.add_argument("--in-mint", default="", help="Input mint (default SOL mint from router)")
    p.add_argument(
        "--amount-sol",
        type=float,
        default=0.004,
        help="Amount in SOL (only used if input is SOL)",
    )
    p.add_argument(
        "--amount-raw",
        type=int,
        default=0,
        help="Override amount raw base units (takes precedence)",
    )
    p.add_argument("--slippage-bps", type=int, default=30, help="Slippage in bps")
    p.add_argument("--pubkey", default="", help="User pubkey for swap build (or env PUBKEY)")
    p.add_argument(
        "--priority-lamports",
        type=int,
        default=0,
        help="Optional priority fee lamports (0 = none)",
    )
    p.add_argument(
        "--compute-units",
        type=int,
        default=0,
        help="Optional compute unit limit (0 = router default)",
    )
    p.add_argument("--print-full", action="store_true", help="Print full quote JSON (may be large)")
    return p


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
async def _amain() -> int:
    args = _build_argparser().parse_args()

    api = _resolve_router()

    in_mint = args.in_mint.strip() or api.sol_mint
    out_mint = args.out_mint.strip()
    if not out_mint:
        LOG.error("out_mint required")
        return 2

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
    _event(
        "DEBUG_JUP_SWAP_QUOTE_REQ",
        in_mint=in_mint,
        out_mint=out_mint,
        amount_raw=amount_raw,
        slippage_bps=slippage_bps,
    )

    try:
        quote = await _call_quote(api, in_mint, out_mint, amount_raw, slippage_bps)
    except Exception as e:
        LOG.error("Quote failed: %s", e)
        _event("DEBUG_JUP_SWAP_QUOTE_FAIL", err=str(e))
        return 1

    # Compact quote summary
    summary_keys = (
        "inAmount",
        "outAmount",
        "swapMode",
        "priceImpactPct",
        "otherAmountThreshold",
        "contextSlot",
        "timeTaken",
    )
    summary = _pick(quote, summary_keys)

    print("\n=== QUOTE SUMMARY ===")
    print(_pretty(summary))

    rp = quote.get("routePlan") or quote.get("routes")
    if isinstance(rp, list) and rp:
        print("\n=== ROUTE (first hop preview) ===")
        print(_pretty(rp[0], max_chars=6000))

    if args.print_full:
        print("\n=== FULL QUOTE JSON ===")
        print(_pretty(quote, max_chars=200000))

    if args.mode == "quote":
        _event("DEBUG_JUP_SWAP_DONE", mode="quote")
        return 0

    # Swap build mode
    pubkey = args.pubkey.strip() or _envs("PUBKEY", "")
    if not pubkey:
        LOG.error("Swap mode requires --pubkey or env PUBKEY.")
        return 2

    priority_lamports = int(args.priority_lamports or 0) or None
    compute_units = int(args.compute_units or 0) or None

    LOG.warning(
        "SWAP MODE: this assumes the router swap callable is BUILD-ONLY (no broadcast). "
        "If your router broadcasts, stop now and expose a build-only API."
    )
    _event(
        "DEBUG_JUP_SWAP_BUILD_REQ",
        swap_fn=api.swap_name,
        priority_lamports=priority_lamports,
        compute_units=compute_units,
    )

    try:
        swap_result = await _call_swap_build(api, pubkey, quote, priority_lamports, compute_units)
    except Exception as e:
        print("\n=== SWAP BUILD ERROR ===")
        print(repr(e))
        _event("DEBUG_JUP_SWAP_BUILD_FAIL", err=str(e))
        return 1

    print("\n=== SWAP BUILD RESULT (type) ===")
    print(type(swap_result))

    # Common shapes: dict (Jupiter response), bytes/str (tx b64), or object wrapper
    if isinstance(swap_result, (str, bytes)):
        print("\n=== SWAP TX (present?) ===")
        print(bool(swap_result))
        _event("DEBUG_JUP_SWAP_DONE", mode="swap", result_type="str_or_bytes")
        return 0

    if isinstance(swap_result, dict):
        likely: JsonDict = {}
        for k in (
            "swapTransaction",
            "tx",
            "transaction",
            "setupTransaction",
            "cleanupTransaction",
            "addressLookupTableAddresses",
            "lastValidBlockHeight",
            "prioritizationFeeLamports",
            "computeUnitLimit",
        ):
            if k in swap_result:
                likely[k] = swap_result.get(k)

        print("\n=== SWAP RESULT (selected fields) ===")
        print(_pretty(likely if likely else cast(JsonDict, swap_result), max_chars=12000))
        _event("DEBUG_JUP_SWAP_DONE", mode="swap", result_type="dict")
        return 0

    print("\n=== SWAP RESULT (repr) ===")
    print(repr(swap_result)[:4000])
    _event("DEBUG_JUP_SWAP_DONE", mode="swap", result_type=str(type(swap_result)))
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


__all__ = ["EventLogger", "get_logger", "main"]
