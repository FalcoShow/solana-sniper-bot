# scripts/quote_debug.py
from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# --------------------------------------------------------------------------------------
# Repo import path + dotenv
# --------------------------------------------------------------------------------------

THIS = Path(__file__).resolve()
ROOT = THIS.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None  # type: ignore


def _load_env() -> None:
    """Load .env.extra (preferred) then .env if python-dotenv is installed."""
    if load_dotenv is None:
        return
    # Prefer .env.extra (matches Render secret-file pattern), then .env
    for name in (".env.extra", ".env"):
        p = ROOT / name
        if p.exists():
            load_dotenv(str(p), override=False)
    # Also allow python-dotenv default discovery
    with contextlib.suppress(Exception):
        load_dotenv(override=False)


# --------------------------------------------------------------------------------------
# Canonical mints (MAINNET)
# --------------------------------------------------------------------------------------
WSOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"  # canonical USDC
USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"


# --------------------------------------------------------------------------------------
# Optional: core_utils Jupiter wrapper (preferred)
# --------------------------------------------------------------------------------------
try:
    from utils_pkg.core_utils import (  # type: ignore
        close_resources,
        get_jupiter_quote,
        init_resources,
    )
except Exception:
    init_resources = None  # type: ignore
    close_resources = None  # type: ignore
    get_jupiter_quote = None  # type: ignore


async def _maybe_init_resources() -> tuple[bool, str]:
    """Try to init shared resources; never hard-fail quote_debug."""
    if init_resources is None:
        return False, "core_utils.init_resources not available"
    try:
        await init_resources()
        return True, "ok"
    except Exception as e:
        return False, f"init_resources failed: {e!r}"


async def _force_close_sessions_best_effort() -> None:
    """
    Prevent "Unclosed client session/connector" noise even if core_utils is buggy.
    We try:
      - close_resources()
      - core_utils.get_session() if present
    """
    # 1) ask core_utils to close
    if close_resources is not None:
        with contextlib.suppress(Exception):
            await close_resources()

    # 2) try closing shared aiohttp session if core_utils exposes it
    try:
        from utils_pkg import core_utils as cu  # type: ignore

        get_sess = getattr(cu, "get_session", None)
        if callable(get_sess):
            sess = get_sess()
            if sess is not None and getattr(sess, "closed", True) is False:
                with contextlib.suppress(Exception):
                    await sess.close()
    except Exception:
        pass


# --------------------------------------------------------------------------------------
# Direct Jupiter v6 HTTP fallback (when core_utils is unavailable or broken)
# --------------------------------------------------------------------------------------
async def _direct_jupiter_quote(
    input_mint: str,
    output_mint: str,
    amount: int,
    slippage_bps: int,
    base_url: str,
    timeout_s: float = 12.0,
) -> dict[str, Any]:
    import aiohttp

    url = base_url.rstrip("/") + "/v6/quote"
    params = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": str(int(amount)),
        "slippageBps": str(int(slippage_bps)),
    }
    t0 = time.time()
    async with aiohttp.ClientSession() as s, s.get(url, params=params, timeout=timeout_s) as r:
        txt = await r.text()
        if r.status >= 400:
            raise RuntimeError(f"Jupiter HTTP {r.status}: {txt[:400]}")
        data = json.loads(txt)
    data["_debug"] = {"direct": True, "ms": int((time.time() - t0) * 1000)}
    return data


async def _coreutils_jupiter_quote(
    input_mint: str,
    output_mint: str,
    amount: int,
    slippage_frac: float,
) -> dict[str, Any]:
    if get_jupiter_quote is None:
        raise RuntimeError("core_utils.get_jupiter_quote not available")

    # core_utils signature has varied across your repo. Try common forms.
    last_err: Exception | None = None
    for call in (
        lambda: get_jupiter_quote(input_mint, output_mint, amount, slippage_frac),
        lambda: get_jupiter_quote(input_mint, output_mint, amount),
        lambda: get_jupiter_quote(
            input_mint, output_mint, amount, int(round(slippage_frac * 10_000))
        ),  # bps
    ):
        try:
            q = await call()
            if not isinstance(q, dict):
                raise RuntimeError(f"unexpected quote type: {type(q)}")
            return q
        except TypeError as e:
            last_err = e
        except Exception as e:
            last_err = e
            break
    raise RuntimeError(f"core_utils.get_jupiter_quote failed: {last_err!r}")


# --------------------------------------------------------------------------------------
# Pretty printing helpers
# --------------------------------------------------------------------------------------
def _fmt_units(lamports: int, decimals: int) -> str:
    try:
        return f"{lamports / (10**decimals):,.8f}".rstrip("0").rstrip(".")
    except Exception:
        return str(lamports)


def _print_route_plan(route_plan: list[dict[str, Any]], max_hops: int = 8) -> None:
    print(f"routePlan hops: {len(route_plan)}")
    for i, hop in enumerate(route_plan[:max_hops], 1):
        swap = hop.get("swapInfo", {}) if isinstance(hop, dict) else {}
        label = swap.get("label") or swap.get("ammKey") or "?"
        in_m = swap.get("inputMint")
        out_m = swap.get("outputMint")
        out_amt = swap.get("outAmount")
        pct = hop.get("percent")
        print(f"  hop {i}: {label}  {in_m} -> {out_m}  outAmount={out_amt}  percent={pct}")
    if len(route_plan) > max_hops:
        print(f"  ... ({len(route_plan) - max_hops} more hops)")


def describe_quote(q: dict[str, Any], in_decimals: int, out_decimals_guess: int) -> None:
    # Common v6 fields:
    for k in (
        "swapMode",
        "slippageBps",
        "priceImpactPct",
        "swapUsdValue",
        "contextSlot",
        "timeTaken",
    ):
        if k in q:
            print(f"{k}: {q.get(k)}")

    in_amt = q.get("inAmount")
    out_amt = q.get("outAmount")
    if in_amt is not None:
        try:
            in_amt_i = int(in_amt)
            print(f"inAmount:  {in_amt_i} ({_fmt_units(in_amt_i, in_decimals)})")
        except Exception:
            print(f"inAmount:  {in_amt}")
    if out_amt is not None:
        try:
            out_amt_i = int(out_amt)
            print(f"outAmount: {out_amt_i} ({_fmt_units(out_amt_i, out_decimals_guess)})")
        except Exception:
            print(f"outAmount: {out_amt}")

    rp = q.get("routePlan")
    if isinstance(rp, list) and rp:
        print("Route present (v6 routePlan).")
        _print_route_plan(rp)
        return

    data = q.get("data")
    if isinstance(data, list) and data:
        print("Route present (legacy data list).")
        print(f"routes: {len(data)}")
        return

    route = q.get("route")
    if route:
        print("Route present (legacy route).")
        return

    print("NO ROUTE found in quote payload.")


# --------------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------------
@dataclass
class Args:
    mint_in: str
    mint_out: str
    amount: float
    decimals: int
    slip: float
    raw: bool
    pretty: bool
    direct: bool
    jup_base: str
    json_out: bool


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Jupiter quote debugger (v6-aware, core_utils-first)")
    ap.add_argument("--in", dest="mint_in", default=USDC_MINT, help="input mint (default: USDC)")
    ap.add_argument("--out", dest="mint_out", default=WSOL_MINT, help="output mint (default: WSOL)")
    ap.add_argument("--amount", type=float, default=1.0, help="amount in input token units")
    ap.add_argument("--decimals", type=int, default=6, help="input token decimals (default: 6)")
    ap.add_argument("--slip", type=float, default=0.01, help="slippage fraction (0.01=1%)")
    ap.add_argument("--raw", action="store_true", help="print raw JSON response and exit")
    ap.add_argument(
        "--pretty", action="store_true", help="pretty-print JSON when --raw/--json is used"
    )
    ap.add_argument(
        "--direct", action="store_true", help="force direct Jupiter HTTP (bypass core_utils)"
    )
    ap.add_argument(
        "--jup-base",
        default=os.getenv("JUP_QUOTE_BASE", "https://quote-api.jup.ag"),
        help="Jupiter base URL",
    )
    ap.add_argument(
        "--json", dest="json_out", action="store_true", help="emit machine-readable JSON summary"
    )
    return ap


async def run(a: Args) -> int:
    amt_lamports = int(round(a.amount * (10**a.decimals)))
    slippage_bps = int(round(max(0.0, a.slip) * 10_000))

    started = time.time()
    init_ok, init_detail = await _maybe_init_resources()

    q: dict[str, Any] | None = None
    err: str | None = None
    try:
        if a.direct or get_jupiter_quote is None:
            q = await _direct_jupiter_quote(
                a.mint_in, a.mint_out, amt_lamports, slippage_bps, a.jup_base
            )
        else:
            q = await _coreutils_jupiter_quote(a.mint_in, a.mint_out, amt_lamports, a.slip)
    except Exception as e:
        err = repr(e)

    elapsed_ms = int((time.time() - started) * 1000)

    if a.json_out:
        out = {
            "ok": bool(q) and err is None,
            "elapsed_ms": elapsed_ms,
            "init_resources": {"ok": init_ok, "detail": init_detail},
            "request": {
                "mint_in": a.mint_in,
                "mint_out": a.mint_out,
                "amount_units": a.amount,
                "amount_lamports": amt_lamports,
                "in_decimals": a.decimals,
                "slip_frac": a.slip,
                "slip_bps": slippage_bps,
                "direct": bool(a.direct or get_jupiter_quote is None),
                "jup_base": a.jup_base,
            },
            "error": err,
        }
        if q is not None:
            out["quote_keys"] = list(q.keys())
            out["has_routeplan"] = (
                isinstance(q.get("routePlan"), list) and len(q.get("routePlan") or []) > 0
            )
            out["inAmount"] = q.get("inAmount")
            out["outAmount"] = q.get("outAmount")
            out["priceImpactPct"] = q.get("priceImpactPct")
            if a.raw:
                out["quote"] = q
        print(json.dumps(out, indent=2 if a.pretty else None))
        await _force_close_sessions_best_effort()
        return 0 if out["ok"] else 2

    # Human output
    print(
        f"Requesting quote: {a.mint_in} -> {a.mint_out} "
        f"amount={a.amount} (lamports={amt_lamports}) slip={a.slip} ({slippage_bps} bps) "
        f"{'[DIRECT]' if (a.direct or get_jupiter_quote is None) else '[core_utils]'}"
    )
    if not init_ok:
        print(f"note: init_resources not OK (non-fatal here): {init_detail}")

    if err is not None:
        print(f"ERROR: {err}")
        await _force_close_sessions_best_effort()
        return 2

    assert q is not None
    if a.raw:
        print(json.dumps(q, indent=2 if a.pretty else None))
        await _force_close_sessions_best_effort()
        return 0

    out_dec_guess = 9 if a.mint_out == WSOL_MINT else a.decimals
    describe_quote(q, a.decimals, out_dec_guess)

    await _force_close_sessions_best_effort()
    return 0


def main() -> None:
    _load_env()
    ap = build_arg_parser()
    ns = ap.parse_args()
    a = Args(
        mint_in=ns.mint_in,
        mint_out=ns.mint_out,
        amount=ns.amount,
        decimals=ns.decimals,
        slip=ns.slip,
        raw=bool(ns.raw),
        pretty=bool(ns.pretty),
        direct=bool(ns.direct),
        jup_base=str(ns.jup_base),
        json_out=bool(ns.json_out),
    )
    raise SystemExit(asyncio.run(run(a)))


if __name__ == "__main__":
    main()
