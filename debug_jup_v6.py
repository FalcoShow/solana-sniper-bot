#!/usr/bin/env python3
from __future__ import annotations

"""
debug_jup_v6.py — Jupiter v6 / swap-v1 endpoint debugger (DEV-ONLY)
===================================================================

Purpose
-------
A diagnostic harness that tests Jupiter endpoints directly (HTTP) to isolate:
- base URL / relay correctness
- header/auth correctness (x-api-key, bearer)
- quote path differences (/quote vs /swap/v1/quote)
- swap build behavior (/swap vs /swap/v1/swap)

This script is DEV-ONLY. Do not import it from production code.
Recommended location: tools/debug_jup_v6.py

What it does NOT do
-------------------
- It does NOT sign.
- It does NOT send transactions.
- It only requests a swap-transaction build payload from Jupiter.

Environment (optional) — do NOT add new env keys here unless you also update .env.example.
----------------------------------------------------------------------------------------
JUP_UPSTREAM / JUP_BASE_URL      Base URL (examples: https://api.jup.ag, http://127.0.0.1:8081)
JUP_BASE                        Extra fallback name used by some older setups
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
import contextlib
import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Final, Literal, Protocol, cast

import aiohttp

# =============================================================================
# Phase A — Logging contract (EventLogger + safe get_logger)
# =============================================================================


class EventLogger(Protocol):
    # keep .handlers in the contract (some code checks it)
    handlers: list[Any]

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def info(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def error(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def log(self, level: int, msg: str, *args: Any, **kwargs: Any) -> None: ...


class _EventCapable(EventLogger, Protocol):
    def event(self, name: str, **fields: Any) -> None: ...


def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    return (v if v is not None else d).strip()


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
    nm = (name or "debug_jup_v6").strip() or "debug_jup_v6"

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
            format="[debug_jup_v6] %(asctime)s %(levelname)s %(name)s %(message)s",
        )
    return cast(EventLogger, _LoggerProxy(lg))


LOG: EventLogger = get_logger("debug_jup_v6")


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


# =============================================================================
# Constants
# =============================================================================

SOL_MINT: Final[str] = "So11111111111111111111111111111111111111112"
USDC_MINT: Final[str] = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
LAMPORTS_PER_SOL: Final[int] = 1_000_000_000

JsonDict = dict[str, Any]
BaseKind = Literal["root", "v6", "swap_v1"]


# =============================================================================
# Env helpers (NO new env keys)
# =============================================================================
def _envb(k: str, default: bool = False) -> bool:
    v = _envs(k, "")
    if not v:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on", "t"}


def _to_lamports(sol: float) -> int:
    if sol <= 0.0:
        return 0
    return max(1, int(float(sol) * float(LAMPORTS_PER_SOL)))


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


def _pick(d: JsonDict, keys: tuple[str, ...]) -> JsonDict:
    out: JsonDict = {}
    for k in keys:
        if k in d:
            out[k] = d.get(k)
    return out


# =============================================================================
# Pubkey helpers (best-effort, no hard dependency)
# =============================================================================
def _try_load_pubkey() -> str | None:
    """
    Best-effort:
    - env PUBKEY first
    - try utils_pkg.solana_wallet if present
    """
    env_pk = _envs("PUBKEY", "")
    if env_pk:
        return env_pk

    try:
        from utils_pkg.solana_wallet import (  # type: ignore[import-not-found]
            get_pubkey_str,
            load_keypair,
        )
    except Exception:
        return None

    try:
        kp = load_keypair()
        return str(get_pubkey_str(kp))
    except Exception:
        return None


# =============================================================================
# HTTP models + helpers
# =============================================================================
@dataclass(frozen=True)
class HttpResult:
    status: int
    url: str
    is_json: bool
    json: JsonDict | None
    text: str


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
    bearer = _envs("JUP_BEARER", "").strip()
    if bearer:
        h["x-jup-auth"] = f"Bearer {bearer}"
    return h


def _base_kind(base: str) -> BaseKind:
    b = (base or "").rstrip("/")
    if b.endswith("/v6"):
        return "v6"
    if b.endswith("/swap/v1"):
        return "swap_v1"
    return "root"


def _join(base: str, path: str) -> str:
    b = (base or "").rstrip("/")
    p = (path or "").lstrip("/")
    return f"{b}/{p}"


def _normalize_path_for_base(base: str, path: str) -> str:
    """
    Normalizes a path depending on base style:
      - base endswith /swap/v1: "/swap/v1/quote" -> "/quote"
      - base endswith /v6: keep path as-is (expects "/quote", "/swap", etc)
      - root: keep path as-is
    """
    kind = _base_kind(base)
    if kind == "swap_v1" and path.startswith("/swap/v1/"):
        return path[len("/swap/v1") :]  # keep leading '/'
    return path


def _try_parse_json_dict(txt: str) -> JsonDict | None:
    try:
        obj = json.loads(txt)
        if isinstance(obj, dict):
            return cast(JsonDict, obj)
        return None
    except Exception:
        return None


async def _http_get(
    session: aiohttp.ClientSession,
    url: str,
    params: dict[str, Any],
    headers: dict[str, str],
    timeout_s: int,
) -> HttpResult:
    timeout = aiohttp.ClientTimeout(total=float(max(1, timeout_s)))
    try:
        async with session.get(url, params=params, headers=headers, timeout=timeout) as r:
            raw = await r.read()
            txt = raw.decode("utf-8", errors="replace")
            js = _try_parse_json_dict(txt)
            return HttpResult(
                status=int(r.status),
                url=str(r.url),
                is_json=js is not None,
                json=js,
                text=txt,
            )
    except asyncio.CancelledError:
        raise
    except asyncio.TimeoutError:
        return HttpResult(status=0, url=url, is_json=False, json=None, text="timeout")
    except Exception as e:
        return HttpResult(status=0, url=url, is_json=False, json=None, text=f"error: {e}")


async def _http_post(
    session: aiohttp.ClientSession,
    url: str,
    body: JsonDict,
    headers: dict[str, str],
    timeout_s: int,
) -> HttpResult:
    timeout = aiohttp.ClientTimeout(total=float(max(1, timeout_s)))
    try:
        async with session.post(url, json=body, headers=headers, timeout=timeout) as r:
            raw = await r.read()
            txt = raw.decode("utf-8", errors="replace")
            js = _try_parse_json_dict(txt)
            return HttpResult(
                status=int(r.status),
                url=str(r.url),
                is_json=js is not None,
                json=js,
                text=txt,
            )
    except asyncio.CancelledError:
        raise
    except asyncio.TimeoutError:
        return HttpResult(status=0, url=url, is_json=False, json=None, text="timeout")
    except Exception as e:
        return HttpResult(status=0, url=url, is_json=False, json=None, text=f"error: {e}")


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


def _print_http_result(title: str, r: HttpResult, *, max_body: int = 8000) -> None:
    print(f"\n=== {title} ===")
    print("URL:", r.url)
    print("STATUS:", r.status)
    if r.is_json and r.json is not None:
        print("KEYS:", list(r.json.keys())[:30])
        return
    body = r.text or ""
    if len(body) > max_body:
        body = body[:max_body] + "\n...<truncated>..."
    print("BODY (non-json):")
    print(body)


# =============================================================================
# Core flow
# =============================================================================
async def run(
    *,
    base: str,
    mode: Literal["quote", "both"],
    as_legacy: bool,
    only_direct: bool,
    out_mint: str,
    amount_lamports: int,
    slippage_bps: int,
    pubkey: str | None,
    timeout_s: int,
    print_full: bool,
) -> int:
    base = (base or "").rstrip("/")
    if not base:
        LOG.error("Missing base URL (--base or env JUP_UPSTREAM/JUP_BASE_URL/JUP_BASE).")
        return 2

    out_mint = (out_mint or "").strip()
    if not out_mint:
        LOG.error("Missing out mint.")
        return 2

    amount_lamports = int(amount_lamports)
    if amount_lamports <= 0:
        LOG.error("Amount must be > 0.")
        return 2

    slippage_bps = int(max(1, int(slippage_bps)))

    hdr = _headers()
    LOG.info(
        "BASE=%s kind=%s mode=%s as_legacy=%s only_direct=%s api_key_present=%s bearer_present=%s lamports=%s slippage_bps=%s out_mint=%s",
        base,
        _base_kind(base),
        mode,
        as_legacy,
        only_direct,
        bool(_envs("JUP_API_KEY", "") or _envs("JUP_PRO_KEY", "")),
        bool(_envs("JUP_BEARER", "")),
        amount_lamports,
        slippage_bps,
        out_mint,
    )

    qpref, qfb = _quote_paths(as_legacy)
    qpref = _normalize_path_for_base(base, qpref)
    qfb = _normalize_path_for_base(base, qfb)
    quote_urls = [_join(base, qpref), _join(base, qfb)]

    qparams: dict[str, Any] = {
        "inputMint": SOL_MINT,
        "outputMint": out_mint,
        "amount": str(int(amount_lamports)),
        "slippageBps": int(slippage_bps),
        "onlyDirectRoutes": _safe_bool_str(bool(only_direct)),
        "swapMode": "ExactIn",
        "asLegacyTransaction": _safe_bool_str(bool(as_legacy)),
    }

    _event(
        "DEBUG_JUP_START",
        base=base,
        kind=_base_kind(base),
        as_legacy=as_legacy,
        only_direct=only_direct,
        mode=mode,
    )

    async with aiohttp.ClientSession() as session:
        quote_res: HttpResult | None = None

        for u in quote_urls:
            LOG.info("QUOTE GET -> %s", u)
            r = await _http_get(session, u, qparams, hdr, timeout_s=timeout_s)
            quote_res = r

            if r.status == 200 and r.is_json and isinstance(r.json, dict):
                break

            # common "wrong path" failures: try fallback
            if r.status in (0, 404, 405):
                LOG.warning("QUOTE failed status=%s; trying fallback", r.status)
                continue

            break

        if quote_res is None:
            LOG.error("QUOTE: no response (unexpected).")
            return 1

        _print_http_result("QUOTE RESULT", quote_res)

        if not (quote_res.status == 200 and quote_res.is_json and quote_res.json is not None):
            if quote_res.is_json and quote_res.json is not None:
                print("\nQUOTE ERROR JSON (truncated):")
                print(_pretty(quote_res.json, max_chars=8000))
            return 1

        qjson = quote_res.json
        print("\nQUOTE SUMMARY:")
        print(
            _pretty(
                _pick(
                    qjson,
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
            )
        )

        rp = qjson.get("routePlan") or qjson.get("routes")
        if isinstance(rp, list) and rp:
            print("\nROUTE PREVIEW (first step):")
            print(_pretty(rp[0], max_chars=6000))

        if print_full:
            print("\nFULL QUOTE JSON:")
            print(_pretty(qjson, max_chars=200000))

        if mode == "quote":
            return 0

        # swap build
        user_pubkey = (pubkey or "").strip() or (_try_load_pubkey() or "")
        if not user_pubkey:
            LOG.error("SWAP build requires a pubkey (--pubkey, env PUBKEY, or loadable keypair).")
            return 2

        spref, sfb = _swap_paths(as_legacy)
        spref = _normalize_path_for_base(base, spref)
        sfb = _normalize_path_for_base(base, sfb)
        swap_urls = [_join(base, spref), _join(base, sfb)]

        body: JsonDict = {
            "userPublicKey": user_pubkey,
            "wrapAndUnwrapSol": True,
            "dynamicComputeUnitLimit": True,
            "quoteResponse": qjson,
            "asLegacyTransaction": bool(as_legacy),
        }

        swap_res: HttpResult | None = None
        for u in swap_urls:
            LOG.info("SWAP POST -> %s", u)
            r2 = await _http_post(session, u, body, hdr, timeout_s=timeout_s)
            swap_res = r2

            if r2.status == 200 and r2.is_json and isinstance(r2.json, dict):
                break

            if r2.status in (0, 404, 405):
                LOG.warning("SWAP failed status=%s; trying fallback", r2.status)
                continue

            break

        if swap_res is None:
            LOG.error("SWAP: no response (unexpected).")
            return 1

        _print_http_result("SWAP BUILD RESULT", swap_res)

        if not (swap_res.status == 200 and swap_res.is_json and swap_res.json is not None):
            if swap_res.is_json and swap_res.json is not None:
                print("\nSWAP ERROR JSON (truncated):")
                print(_pretty(swap_res.json, max_chars=12000))
            return 1

        sj = swap_res.json
        has_tx = any(k in sj for k in ("swapTransaction", "transaction", "tx"))
        print("\nHAS_TX_FIELD?:", bool(has_tx))

        compact: JsonDict = {}
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
            print(_pretty(compact, max_chars=7000))

        if print_full:
            print("\nFULL SWAP JSON:")
            print(_pretty(sj, max_chars=200000))

        return 0


# =============================================================================
# CLI
# =============================================================================
def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Debug Jupiter /quote and /swap endpoints directly (v6 + swap/v1)."
    )
    p.add_argument(
        "--base",
        default="",
        help="Base URL (or env JUP_UPSTREAM/JUP_BASE_URL/JUP_BASE). Example: https://api.jup.ag or http://127.0.0.1:8081",
    )
    p.add_argument(
        "--mode",
        choices=["quote", "swap", "both"],
        default="quote",
        help="Run quote only, swap build (requires pubkey), or both.",
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
        "--out-mint",
        default=_envs("TEST_MINT", USDC_MINT),
        help="Output mint (default USDC).",
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
    p.add_argument(
        "--pubkey",
        default="",
        help="User pubkey (or env PUBKEY, or loadable keypair).",
    )
    p.add_argument(
        "--timeout",
        type=int,
        default=25,
        help="Total timeout per request (seconds).",
    )
    p.add_argument(
        "--print-full",
        action="store_true",
        help="Print full JSON payloads (large).",
    )
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

    slippage_bps = int(max(1, int(args.slippage_bps)))

    amount_lamports = int(args.amount_lamports or 0)
    if amount_lamports <= 0:
        amount_lamports = _to_lamports(float(args.amount_sol))

    # swap-only still needs quote in most Jupiter flows; treat as both
    mode: Literal["quote", "both"] = "quote"
    if args.mode in {"both", "swap"}:
        mode = "both"

    rc = asyncio.run(
        run(
            base=base,
            mode=mode,
            as_legacy=as_legacy,
            only_direct=only_direct,
            out_mint=out_mint,
            amount_lamports=amount_lamports,
            slippage_bps=slippage_bps,
            pubkey=args.pubkey.strip() or None,
            timeout_s=int(args.timeout),
            print_full=bool(args.print_full),
        )
    )
    raise SystemExit(rc)


if __name__ == "__main__":
    main()


__all__ = ["EventLogger", "get_logger", "run", "main"]
