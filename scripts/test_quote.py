# scripts/test_quote.py
from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

# --------------------------------------------------------------------------------------
# Repo path + dotenv
# --------------------------------------------------------------------------------------
THIS = Path(__file__).resolve()
ROOT = THIS.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None  # type: ignore


def _load_env() -> None:
    if load_dotenv is None:
        return
    # Prefer .env.extra then .env (matches Render secret-file habits)
    for name in (".env.extra", ".env"):
        p = ROOT / name
        if p.exists():
            load_dotenv(str(p), override=False)
    with contextlib.suppress(Exception):
        load_dotenv(override=False)


# --------------------------------------------------------------------------------------
# Canonical mainnet mints
# --------------------------------------------------------------------------------------
SOL_MINT = os.getenv("SOL_MINT", "So11111111111111111111111111111111111111112")
USDC_MINT = os.getenv("USDC_MINT", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")

# --------------------------------------------------------------------------------------
# Optional core_utils (preferred path in your stack)
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


# --------------------------------------------------------------------------------------
# Safety latch (scripts folder is dangerous)
# --------------------------------------------------------------------------------------
def _require_allow(yes: bool) -> None:
    if yes:
        return
    if os.getenv("ALLOW_TEST_QUOTE_SCRIPT") == "1":
        return
    if os.getenv("ALLOW_DRY_RUN_SCRIPT") == "1":
        return
    raise SystemExit(
        "Safety latch: set ALLOW_TEST_QUOTE_SCRIPT=1 (or ALLOW_DRY_RUN_SCRIPT=1) "
        "or pass --yes-i-am-sure"
    )


# --------------------------------------------------------------------------------------
# Direct Jupiter HTTP fallback (v1 + v6)
# --------------------------------------------------------------------------------------
def _bool_str(x: bool) -> str:
    return "true" if x else "false"


def _http_quote_v1(
    base: str,
    input_mint: str,
    output_mint: str,
    amount: int,
    slippage_bps: int,
    only_direct: bool,
    as_legacy: bool,
    restrict_intermediate: bool,
    timeout_s: float,
    pro_key: str | None,
) -> dict[str, Any]:
    import httpx

    url = base.rstrip("/") + "/swap/v1/quote"
    params = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": str(int(amount)),
        "slippageBps": str(int(slippage_bps)),
        "swapMode": "ExactIn",
        "onlyDirectRoutes": _bool_str(bool(only_direct)),
        "asLegacyTransaction": _bool_str(bool(as_legacy)),
        "restrictIntermediateTokens": _bool_str(bool(restrict_intermediate)),
    }
    headers = {"Accept": "application/json"}
    if pro_key and "api.jup.ag" in base:
        headers["x-api-key"] = pro_key

    with httpx.Client(timeout=timeout_s) as c:
        r = c.get(url, params=params, headers=headers)
        r.raise_for_status()
        return r.json()


def _http_quote_v6(
    base: str,
    input_mint: str,
    output_mint: str,
    amount: int,
    slippage_bps: int,
    only_direct: bool,
    as_legacy: bool,
    timeout_s: float,
) -> dict[str, Any]:
    import httpx

    url = base.rstrip("/") + "/v6/quote"
    params = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": str(int(amount)),
        "slippageBps": str(int(slippage_bps)),
        "onlyDirectRoutes": _bool_str(bool(only_direct)),
        "asLegacyTransaction": _bool_str(bool(as_legacy)),
    }
    with httpx.Client(timeout=timeout_s) as c:
        r = c.get(url, params=params)
        r.raise_for_status()
        return r.json()


# --------------------------------------------------------------------------------------
# Close any leaked aiohttp sessions inside core_utils to avoid shutdown spam
# --------------------------------------------------------------------------------------
async def _close_everything_best_effort() -> None:
    # 1) core close
    if close_resources is not None:
        with contextlib.suppress(Exception):
            await close_resources()

    # 2) brute force: close any aiohttp.ClientSession objects that exist in core_utils globals
    try:
        import aiohttp  # type: ignore

        from utils_pkg import core_utils as cu  # type: ignore

        sessions: list[aiohttp.ClientSession] = []
        for name in dir(cu):
            try:
                v = getattr(cu, name)
            except Exception:
                continue
            try:
                if isinstance(v, aiohttp.ClientSession):
                    sessions.append(v)
            except Exception:
                continue

        # Also common private names used in your repo over time
        for name in ("_session", "_session_ref", "_aiohttp_session", "session", "http_session"):
            try:
                v = getattr(cu, name, None)
                if isinstance(v, aiohttp.ClientSession):
                    sessions.append(v)
            except Exception:
                pass

        # De-dupe by id
        uniq = {id(s): s for s in sessions}.values()
        for s in uniq:
            try:
                if not s.closed:
                    await s.close()
            except Exception:
                pass
    except Exception:
        pass


# --------------------------------------------------------------------------------------
# Quote parsing helpers (v1 + v6)
# --------------------------------------------------------------------------------------
def _has_route(payload: dict[str, Any]) -> bool:
    rp = payload.get("routePlan")
    if isinstance(rp, list) and len(rp) > 0:
        return True
    data = payload.get("data")
    if isinstance(data, list) and len(data) > 0:
        return True
    return bool(payload.get("outAmount"))


def _route_hops(payload: dict[str, Any]) -> int:
    rp = payload.get("routePlan")
    if isinstance(rp, list):
        return len(rp)
    data = payload.get("data")
    if isinstance(data, list) and len(data) > 0:
        # legacy v4/v5 often
        return len(data[0].get("marketInfos", [])) if isinstance(data[0], dict) else len(data)
    return 0


# --------------------------------------------------------------------------------------
# Runner
# --------------------------------------------------------------------------------------
async def _try_coreutils_quote(
    input_mint: str,
    output_mint: str,
    amount: int,
    slip_frac: float,
) -> tuple[dict[str, Any] | None, str | None]:
    if init_resources is None or get_jupiter_quote is None:
        return None, "core_utils not available"

    try:
        await init_resources()
    except Exception as e:
        # still try quote even if init partially fails
        init_err = f"init_resources failed: {e!r}"
    else:
        init_err = None

    try:
        # signature drift tolerant
        try:
            q = await get_jupiter_quote(input_mint, output_mint, amount, slip_frac)
        except TypeError:
            q = await get_jupiter_quote(input_mint, output_mint, amount)

        if q is None:
            return None, (init_err or "") + " core_utils returned None"

        if not isinstance(q, dict):
            return None, (init_err or "") + f" unexpected core_utils type: {type(q)}"

        return q, init_err
    except Exception as e:
        return None, (init_err or "") + f" core_utils quote error: {e!r}"


async def main() -> int:
    _load_env()

    ap = argparse.ArgumentParser(
        description="Jupiter quote smoke test (core_utils first, HTTP fallback)"
    )
    ap.add_argument("--yes-i-am-sure", action="store_true", help="bypass safety latch")
    ap.add_argument("--in", dest="mint_in", default=SOL_MINT, help="input mint (default SOL)")
    ap.add_argument("--out", dest="mint_out", default=USDC_MINT, help="output mint (default USDC)")
    ap.add_argument(
        "--amount",
        type=float,
        default=float(os.getenv("TEST_QUOTE_SOL", "0.05")),
        help="amount in input units",
    )
    ap.add_argument("--decimals", type=int, default=9, help="input decimals (SOL=9, USDC=6)")
    ap.add_argument("--slip", type=float, default=0.012, help="slippage fraction (0.012=120bps)")
    ap.add_argument("--require-route", action="store_true", help="exit nonzero if no route found")
    ap.add_argument("--only-direct", action="store_true", help="onlyDirectRoutes=true")
    ap.add_argument("--legacy-tx", action="store_true", help="asLegacyTransaction=true")
    ap.add_argument(
        "--no-restrict-intermediate", action="store_true", help="restrictIntermediateTokens=false"
    )
    ap.add_argument(
        "--timeout",
        type=float,
        default=float(os.getenv("JUP_TEST_TIMEOUT_SEC", "12")),
        help="HTTP timeout seconds",
    )
    ap.add_argument("--json", action="store_true", help="print JSON output")

    ns = ap.parse_args()
    _require_allow(bool(ns.yes_i_am_sure))

    amount_lamports = int(round(float(ns.amount) * (10 ** int(ns.decimals))))
    slippage_bps = int(round(max(0.0, float(ns.slip)) * 10_000))
    restrict_intermediate = not bool(ns.no_restrict_intermediate)

    # Endpoints
    PRO = (os.getenv("JUP_UPSTREAM") or "https://api.jup.ag").rstrip("/")
    LITE = (os.getenv("JUP_LITE") or "https://lite-api.jup.ag").rstrip("/")
    V6 = (os.getenv("JUP_LEGACY") or "https://quote-api.jup.ag").rstrip("/")
    PRO_KEY = os.getenv("JUP_PRO_KEY")

    started = time.time()
    result: dict[str, Any] = {
        "ok": False,
        "request": {
            "mint_in": ns.mint_in,
            "mint_out": ns.mint_out,
            "amount_units": float(ns.amount),
            "amount_lamports": amount_lamports,
            "decimals": int(ns.decimals),
            "slip_frac": float(ns.slip),
            "slip_bps": slippage_bps,
            "only_direct": bool(ns.only_direct),
            "as_legacy": bool(ns.legacy_tx),
            "restrict_intermediate": bool(restrict_intermediate),
            "timeout_s": float(ns.timeout),
        },
        "attempts": [],
        "elapsed_ms": None,
    }

    try:
        # 1) Try core_utils (authoritative)
        q, note = await _try_coreutils_quote(
            ns.mint_in, ns.mint_out, amount_lamports, float(ns.slip)
        )
        if q is not None:
            ok = _has_route(q)
            result["attempts"].append(
                {
                    "source": "core_utils",
                    "ok": ok,
                    "note": note,
                    "route_hops": _route_hops(q),
                    "outAmount": q.get("outAmount"),
                    "priceImpactPct": q.get("priceImpactPct"),
                    "keys": list(q.keys()),
                }
            )
            if ok or (not ns.require_route):
                result["ok"] = bool(ok) if ns.require_route else True
                if ns.json:
                    print(json.dumps(result, indent=2))
                else:
                    print("QUOTE OK (core_utils):", json.dumps(result["attempts"][-1], indent=2))
                return 0 if result["ok"] else 2

        else:
            result["attempts"].append(
                {"source": "core_utils", "ok": False, "note": note or "no payload"}
            )

        # 2) HTTP fallback order: PRO v1 -> LITE v1 -> LEGACY v6
        last_err: str | None = None
        for label, fn, base in (
            ("PRO_v1", _http_quote_v1, PRO),
            ("LITE_v1", _http_quote_v1, LITE),
        ):
            try:
                payload = fn(
                    base=base,
                    input_mint=ns.mint_in,
                    output_mint=ns.mint_out,
                    amount=amount_lamports,
                    slippage_bps=slippage_bps,
                    only_direct=bool(ns.only_direct),
                    as_legacy=bool(ns.legacy_tx),
                    restrict_intermediate=restrict_intermediate,
                    timeout_s=float(ns.timeout),
                    pro_key=PRO_KEY,
                )
                ok = _has_route(payload)
                result["attempts"].append(
                    {
                        "source": label,
                        "base": base,
                        "ok": ok,
                        "route_hops": _route_hops(payload),
                        "outAmount": payload.get("outAmount"),
                        "priceImpactPct": payload.get("priceImpactPct"),
                        "keys": list(payload.keys()),
                    }
                )
                if ok or (not ns.require_route):
                    result["ok"] = bool(ok) if ns.require_route else True
                    if ns.json:
                        print(json.dumps(result, indent=2))
                    else:
                        print("QUOTE OK:", json.dumps(result["attempts"][-1], indent=2))
                    return 0 if result["ok"] else 2
            except Exception as e:
                last_err = repr(e)
                result["attempts"].append(
                    {"source": label, "base": base, "ok": False, "error": last_err}
                )

        # legacy v6 last
        try:
            payload = _http_quote_v6(
                base=V6,
                input_mint=ns.mint_in,
                output_mint=ns.mint_out,
                amount=amount_lamports,
                slippage_bps=slippage_bps,
                only_direct=bool(ns.only_direct),
                as_legacy=bool(ns.legacy_tx),
                timeout_s=float(ns.timeout),
            )
            ok = _has_route(payload)
            result["attempts"].append(
                {
                    "source": "LEGACY_v6",
                    "base": V6,
                    "ok": ok,
                    "route_hops": _route_hops(payload),
                    "outAmount": payload.get("outAmount"),
                    "priceImpactPct": payload.get("priceImpactPct"),
                    "keys": list(payload.keys()),
                }
            )
            result["ok"] = bool(ok) if ns.require_route else True
        except Exception as e:
            result["attempts"].append(
                {"source": "LEGACY_v6", "base": V6, "ok": False, "error": repr(e)}
            )
            result["ok"] = False

        if ns.json:
            print(json.dumps(result, indent=2))
        else:
            print("QUOTE RESULT:", json.dumps(result, indent=2))
        return 0 if result["ok"] else 2

    finally:
        result["elapsed_ms"] = int((time.time() - started) * 1000)
        await _close_everything_best_effort()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
