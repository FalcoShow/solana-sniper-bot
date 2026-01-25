#!/usr/bin/env python3
from __future__ import annotations

"""
debug_jup_v6.py — Jupiter v6 / swap-v1 endpoint debugger (P2 / dev-only)
========================================================================

Purpose
-------
A *diagnostic* harness that tests Jupiter endpoints directly (HTTP) to isolate:
- base URL / relay correctness
- header/auth correctness (x-api-key, bearer)
- quote path differences (/quote vs /swap/v1/quote)
- swap build behavior (/swap vs /swap/v1/swap)

This script is DEV-ONLY. Do not import it from production code.
Recommended location:
  tools/debug_jup_v6.py

What it does NOT do
-------------------
- It does NOT sign.
- It does NOT send transactions.
- It only requests a swap-transaction *build* payload from Jupiter.

Environment (optional)
----------------------
JUP_UPSTREAM / JUP_BASE_URL      Base URL (examples: https://api.jup.ag, http://127.0.0.1:8081)
JUP_AS_LEGACY                   "1"/"true" to prefer /swap/v1/* paths
JUP_ONLY_DIRECT                 "1"/"true" to request direct routes only
JUP_API_KEY / JUP_PRO_KEY       API key (sent as x-api-key)
JUP_BEARER                      Sent as x-jup-auth: Bearer <token>
PUBKEY                          User pubkey fallback (if keypair loader unavailable)

Optional test defaults:
TEST_MINT                       Output mint (default USDC)
TEST_SOL                        Amount SOL float (default 0.004)
TEST_SLIPPAGE_BPS               Slippage bps (default 30)

CLI Examples
------------
# Quote only (recommended first):
python debug_jup_v6.py --mode quote --base https://api.jup.ag

# Quote + swap build (requires pubkey or loadable keypair):
python debug_jup_v6.py --mode both --base https://api.jup.ag --pubkey <YOUR_PUBKEY>

# Test via local relay:
python debug_jup_v6.py --mode both --base http://127.0.0.1:8081 --as-legacy 1
"""

import argparse
import asyncio
import json
import os
from dataclasses import dataclass
from typing import Any

import aiohttp

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────
try:
    from logging_setup import get_logger  # canonical

    LOG = get_logger("debug_jup_v6")
except Exception:  # pragma: no cover
    import logging

    logging.basicConfig(
        level=logging.INFO, format="[debug_jup_v6] %(asctime)s %(levelname)s %(message)s"
    )
    LOG = logging.getLogger("debug_jup_v6")


# ──────────────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────────────
SOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
LAMPORTS_PER_SOL = 1_000_000_000


def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    return (v if v is not None else d).strip()


def _envb(k: str, default: bool = False) -> bool:
    v = _envs(k, "")
    if not v:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def _to_lamports(sol: float) -> int:
    if sol <= 0:
        return 0
    return max(1, int(sol * LAMPORTS_PER_SOL))


def _pretty(obj: Any, max_chars: int = 3500) -> str:
    try:
        s = json.dumps(obj, indent=2, default=str, sort_keys=True)
    except Exception:
        s = str(obj)
    if len(s) > max_chars:
        return s[:max_chars] + "\n...<truncated>..."
    return s


def _safe_bool_str(x: bool) -> str:
    return "true" if x else "false"


def _pick(d: dict[str, Any], keys: tuple[str, ...]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k in keys:
        if k in d:
            out[k] = d.get(k)
    return out


# ──────────────────────────────────────────────────────────────────────────────
# Keypair/pubkey helpers (best-effort, no hard dependency)
# ──────────────────────────────────────────────────────────────────────────────
def _try_load_pubkey() -> str | None:
    """
    Best-effort:
    - If utils_pkg.solana_wallet exists and can load a keypair, use it.
    - Else fall back to env PUBKEY.
    """
    # env override
    env_pk = _envs("PUBKEY", "")
    if env_pk:
        return env_pk

    try:
        from utils_pkg.solana_wallet import get_pubkey_str, load_keypair  # type: ignore
    except Exception:
        return None

    try:
        kp = load_keypair()
        return str(get_pubkey_str(kp))
    except Exception:
        return None


# ──────────────────────────────────────────────────────────────────────────────
# HTTP helpers
# ──────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class HttpResult:
    status: int
    is_json: bool
    json: dict[str, Any] | None
    text: str
    url: str


def _headers() -> dict[str, str]:
    """
    Construct safe headers for Jupiter endpoints.
    Never prints secrets; only indicates presence in logs.
    """
    h: dict[str, str] = {
        "accept": "application/json",
        "content-type": "application/json",
        "origin": "https://jup.ag",
        "referer": "https://jup.ag/",
        "user-agent": "pnlsniper-debug/1.0",
    }
    api_key = (_envs("JUP_API_KEY", "") or _envs("JUP_PRO_KEY", "")).strip()
    if api_key:
        h["x-api-key"] = api_key
    bearer = _envs("JUP_BEARER", "")
    if bearer:
        h["x-jup-auth"] = f"Bearer {bearer}"
    return h


async def _http_get_json(
    session: aiohttp.ClientSession,
    url: str,
    params: dict[str, Any],
    headers: dict[str, str],
    timeout_s: int,
) -> HttpResult:
    try:
        async with session.get(
            url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout_s)
        ) as r:
            raw = await r.read()
            txt = raw.decode("utf-8", errors="replace")
            try:
                js = json.loads(txt)
                if isinstance(js, dict):
                    return HttpResult(
                        status=r.status, is_json=True, json=js, text=txt, url=str(r.url)
                    )
            except Exception:
                pass
            return HttpResult(status=r.status, is_json=False, json=None, text=txt, url=str(r.url))
    except TimeoutError:
        return HttpResult(status=0, is_json=False, json=None, text="timeout", url=url)
    except Exception as e:
        return HttpResult(status=0, is_json=False, json=None, text=f"error: {e}", url=url)


async def _http_post_json(
    session: aiohttp.ClientSession,
    url: str,
    body: dict[str, Any],
    headers: dict[str, str],
    timeout_s: int,
) -> HttpResult:
    try:
        async with session.post(
            url, json=body, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout_s)
        ) as r:
            raw = await r.read()
            txt = raw.decode("utf-8", errors="replace")
            try:
                js = json.loads(txt)
                if isinstance(js, dict):
                    return HttpResult(
                        status=r.status, is_json=True, json=js, text=txt, url=str(r.url)
                    )
            except Exception:
                pass
            return HttpResult(status=r.status, is_json=False, json=None, text=txt, url=str(r.url))
    except TimeoutError:
        return HttpResult(status=0, is_json=False, json=None, text="timeout", url=url)
    except Exception as e:
        return HttpResult(status=0, is_json=False, json=None, text=f"error: {e}", url=url)


def _join(base: str, path: str) -> str:
    base = (base or "").rstrip("/")
    path = (path or "").lstrip("/")
    return f"{base}/{path}"


def _quote_paths(as_legacy: bool) -> tuple[str, str]:
    """
    Returns (preferred, fallback).
    - Non-legacy: prefer /quote, fallback /swap/v1/quote
    - Legacy: prefer /swap/v1/quote, fallback /quote
    """
    return ("/swap/v1/quote", "/quote") if as_legacy else ("/quote", "/swap/v1/quote")


def _swap_paths(as_legacy: bool) -> tuple[str, str]:
    """
    Returns (preferred, fallback).
    - Non-legacy: prefer /swap, fallback /swap/v1/swap
    - Legacy: prefer /swap/v1/swap, fallback /swap
    """
    return ("/swap/v1/swap", "/swap") if as_legacy else ("/swap", "/swap/v1/swap")


# ──────────────────────────────────────────────────────────────────────────────
# Core flow
# ──────────────────────────────────────────────────────────────────────────────
async def run(
    base: str,
    as_legacy: bool,
    only_direct: bool,
    out_mint: str,
    amount_lamports: int,
    slippage_bps: int,
    mode: str,
    pubkey: str | None,
    timeout_s: int,
    print_full: bool,
) -> int:
    base = (base or "").rstrip("/")
    if not base:
        LOG.error("Missing --base (or env JUP_UPSTREAM/JUP_BASE_URL)")
        return 2

    hdr = _headers()
    LOG.info(
        "BASE=%s as_legacy=%s only_direct=%s api_key_present=%s bearer_present=%s amount_lamports=%s slippage_bps=%s out_mint=%s",
        base,
        as_legacy,
        only_direct,
        bool(_envs("JUP_API_KEY", "") or _envs("JUP_PRO_KEY", "")),
        bool(_envs("JUP_BEARER", "")),
        amount_lamports,
        slippage_bps,
        out_mint,
    )

    qpref, qfb = _quote_paths(as_legacy)
    quote_urls = [_join(base, qpref), _join(base, qfb)]

    qparams: dict[str, Any] = {
        "inputMint": SOL_MINT,
        "outputMint": out_mint,
        "amount": str(amount_lamports),
        "slippageBps": int(slippage_bps),
        "onlyDirectRoutes": _safe_bool_str(bool(only_direct)),
        "swapMode": "ExactIn",
        # This param is accepted on some endpoints; harmless if ignored.
        "asLegacyTransaction": _safe_bool_str(bool(as_legacy)),
    }

    async with aiohttp.ClientSession() as session:
        quote_res: HttpResult | None = None
        for u in quote_urls:
            LOG.info("QUOTE GET -> %s", u)
            r = await _http_get_json(session, u, qparams, hdr, timeout_s=timeout_s)
            quote_res = r
            if r.status == 200 and r.is_json and isinstance(r.json, dict):
                break
            # common: 404 on wrong path
            if r.status in (0, 404, 405):
                LOG.warning("QUOTE path failed (%s) status=%s; trying fallback", u, r.status)
                continue
            # other errors: stop early (still print)
            break

        if not quote_res:
            LOG.error("QUOTE: no response (unexpected).")
            return 1

        print("\n=== QUOTE RESULT ===")
        print("URL:", quote_res.url)
        print("STATUS:", quote_res.status)
        if quote_res.is_json and quote_res.json:
            q = quote_res.json
            print("KEYS:", list(q.keys())[:25])
            summary = _pick(
                q,
                (
                    "inAmount",
                    "outAmount",
                    "swapMode",
                    "priceImpactPct",
                    "otherAmountThreshold",
                    "contextSlot",
                    "timeTaken",
                ),
            )
            print("\nQUOTE SUMMARY:")
            print(_pretty(summary))

            rp = q.get("routePlan") or q.get("routes")
            if isinstance(rp, list) and rp:
                print("\nROUTE PREVIEW (first step):")
                print(_pretty(rp[0]))

            if quote_res.status != 200:
                print("\nQUOTE ERROR JSON (truncated):")
                print(_pretty(q, max_chars=5000))
                return 1

            if print_full:
                print("\nFULL QUOTE JSON:")
                print(_pretty(q, max_chars=200000))
        else:
            print("\nQUOTE NON-JSON BODY (truncated):")
            print((quote_res.text or "")[:5000])
            return 1

        if mode == "quote":
            return 0

        # swap build mode
        qjson = quote_res.json or {}
        spref, sfb = _swap_paths(as_legacy)
        swap_urls = [_join(base, spref), _join(base, sfb)]

        user_pubkey = (pubkey or "").strip() or _try_load_pubkey()
        if not user_pubkey:
            LOG.error("SWAP mode requires a pubkey (--pubkey or env PUBKEY, or loadable keypair).")
            return 2

        # Keep body minimal to isolate errors.
        body: dict[str, Any] = {
            "userPublicKey": user_pubkey,
            "wrapAndUnwrapSol": True,
            "dynamicComputeUnitLimit": True,
            "quoteResponse": qjson,
            "asLegacyTransaction": bool(as_legacy),
        }

        swap_res: HttpResult | None = None
        for u in swap_urls:
            LOG.info("SWAP POST -> %s", u)
            r2 = await _http_post_json(session, u, body, hdr, timeout_s=timeout_s)
            swap_res = r2
            if r2.status == 200 and r2.is_json and isinstance(r2.json, dict):
                break
            if r2.status in (0, 404, 405):
                LOG.warning("SWAP path failed (%s) status=%s; trying fallback", u, r2.status)
                continue
            break

        if not swap_res:
            LOG.error("SWAP: no response (unexpected).")
            return 1

        print("\n=== SWAP BUILD RESULT ===")
        print("URL:", swap_res.url)
        print("STATUS:", swap_res.status)

        if swap_res.is_json and swap_res.json:
            sj = swap_res.json
            print("KEYS:", list(sj.keys())[:25])

            # Common Jupiter field:
            has_tx = "swapTransaction" in sj or "transaction" in sj or "tx" in sj
            print("HAS_TX_FIELD?:", bool(has_tx))

            # Print compact info
            compact: dict[str, Any] = {}
            for k in (
                "swapTransaction",
                "transaction",
                "tx",
                "lastValidBlockHeight",
                "prioritizationFeeLamports",
                "computeUnitLimit",
                "addressLookupTableAddresses",
            ):
                if k in sj:
                    compact[k] = sj.get(k)
            if compact:
                print("\nSWAP COMPACT:")
                print(_pretty(compact, max_chars=6000))

            if swap_res.status != 200:
                print("\nSWAP ERROR JSON (truncated):")
                print(_pretty(sj, max_chars=8000))
                return 1

            if print_full:
                print("\nFULL SWAP JSON:")
                print(_pretty(sj, max_chars=200000))
        else:
            print("\nSWAP NON-JSON BODY (truncated):")
            print((swap_res.text or "")[:8000])
            return 1

        return 0


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────
def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Debug Jupiter /quote and /swap endpoints directly (v6 + swap/v1)."
    )
    p.add_argument(
        "--base",
        default="",
        help="Base URL (or env JUP_UPSTREAM/JUP_BASE_URL). Example: https://api.jup.ag or http://127.0.0.1:8081",
    )
    p.add_argument(
        "--mode",
        choices=["quote", "swap", "both"],
        default="quote",
        help="Run quote only, swap only (requires pubkey), or both.",
    )
    p.add_argument(
        "--as-legacy",
        type=int,
        default=-1,
        help="Prefer /swap/v1/* paths. -1 uses env JUP_AS_LEGACY.",
    )
    p.add_argument(
        "--only-direct",
        type=int,
        default=-1,
        help="Request direct routes only. -1 uses env JUP_ONLY_DIRECT.",
    )
    p.add_argument(
        "--out-mint", default=_envs("TEST_MINT", USDC_MINT), help="Output mint (default USDC)."
    )
    p.add_argument(
        "--amount-sol",
        type=float,
        default=float(_envs("TEST_SOL", "0.004") or "0.004"),
        help="SOL amount (ExactIn).",
    )
    p.add_argument(
        "--amount-lamports",
        type=int,
        default=0,
        help="Override amount in lamports (takes precedence).",
    )
    p.add_argument(
        "--slippage-bps",
        type=int,
        default=int(_envs("TEST_SLIPPAGE_BPS", "30") or "30"),
        help="Slippage bps.",
    )
    p.add_argument("--pubkey", default="", help="User pubkey (or env PUBKEY, or loadable keypair).")
    p.add_argument("--timeout", type=int, default=25, help="Total timeout per request (seconds).")
    p.add_argument("--print-full", action="store_true", help="Print full JSON payloads (large).")
    return p


def main() -> None:
    args = _parser().parse_args()

    base = (
        args.base.strip()
        or _envs("JUP_UPSTREAM", "")
        or _envs("JUP_BASE_URL", "")
        or _envs("JUP_BASE", "")
    )
    as_legacy = _envb("JUP_AS_LEGACY", False) if args.as_legacy < 0 else bool(args.as_legacy)
    only_direct = (
        _envb("JUP_ONLY_DIRECT", False) if args.only_direct < 0 else bool(args.only_direct)
    )

    out_mint = (args.out_mint or "").strip()
    if not out_mint:
        raise SystemExit(2)

    slippage_bps = max(1, int(args.slippage_bps))
    amount_lamports = int(args.amount_lamports or 0)
    if amount_lamports <= 0:
        amount_lamports = _to_lamports(float(args.amount_sol))

    mode = args.mode
    if mode == "swap":
        # swap-only still needs quote in most Jupiter flows; we treat as "both"
        mode = "both"

    rc = asyncio.run(
        run(
            base=base,
            as_legacy=as_legacy,
            only_direct=only_direct,
            out_mint=out_mint,
            amount_lamports=amount_lamports,
            slippage_bps=slippage_bps,
            mode=mode,
            pubkey=args.pubkey.strip() or None,
            timeout_s=int(args.timeout),
            print_full=bool(args.print_full),
        )
    )
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
