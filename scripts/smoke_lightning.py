# -*- coding: utf-8 -*-
from __future__ import annotations

"""
scripts/smoke_lightning.py — V3+++ God-Tier Lightning Smoke Test (LOCK-IN)
==========================================================================

Goal
----
Prove end-to-end QUOTE works through the Lightning layer (preferred) and that
mint normalization prevents garbage mints from ever hitting Jupiter.

Hard rules
----------
- QUOTE ONLY: no swaps, no tx building, no signing, no sending.
- Never prints secrets (api keys, wallets, DSNs).
- JSON (when enabled) goes to STDOUT; human logs go to STDERR.
- Emits explicit phrase:
    "NOTE: falling back to direct http"
  whenever Lightning isn't used, so scripts/preflight can enforce policy.

Exit codes
----------
0 = success
2 = failure
130 = ctrl-c

CLI examples
------------
py -m scripts.smoke_lightning --amount-sol 0.005 --out USDC --slippage-bps 50 --only-direct 0 --roundtrip 1
py -m scripts.smoke_lightning --require-lightning 1 --json
py -m scripts.smoke_lightning --require-roundtrip 1 --total-timeout 25 --debug
py -m scripts.smoke_lightning --no-fallback 1 --json

Env knobs
---------
SMOKE_TIMEOUT_S=15
SMOKE_TOTAL_TIMEOUT_S=0                (0 disables total cap)
SMOKE_JSON=0|1
SMOKE_JSON_ONLY=0|1
SMOKE_DEBUG=0|1

SMOKE_REQUIRE_LIGHTNING=0|1            (if 1 -> fail if Lightning isn't used)
SMOKE_REQUIRE_ROUNDTRIP=0|1            (if 1 -> fail if roundtrip quote fails)
SMOKE_ALLOW_HTTP_FALLBACK=1|0          (default 1; if 0 -> fail when lightning fails)

SMOKE_BASE_URL=<override>              (else uses JUP_BASE_URL)
SMOKE_USE_ALT=0|1                      (if 1 -> random choice from JUP_ALT_BASE_URLS)
SMOKE_AMOUNT_SOL=0.005
SMOKE_OUT=USDC
SMOKE_SLIPPAGE_BPS=50
SMOKE_ONLY_DIRECT=0|1
SMOKE_ROUNDTRIP=0|1

SMOKE_SELL_CLAMP_RAW=50000000
SMOKE_SELL_MIN_RAW=10
SMOKE_PROBE_MINTS=0|1                  (local-only probe: invalid strings never become mints)

Optional cross-check (still QUOTE ONLY)
---------------------------------------
SMOKE_USE_ROUTER=0|1                   (if 1: also ask utils_pkg.router for a quote and compare)

Jupiter env
-----------
JUP_BASE_URL=https://api.jup.ag
JUP_ALT_BASE_URLS=...
JUP_AS_LEGACY=0|1
JUP_ONLY_DIRECT=0|1
JUP_PRO_KEY=...
JUP_API_KEY=... (or JUP_KEY)
JUP_QUOTE_PATH=/v6/quote (optional override; if set must start with /)
"""

import argparse
import asyncio
import contextlib
import inspect
import json
import os
import platform
import random
import re
import sys
import time
import traceback
from typing import Any, Dict, List, Optional, Sequence, Tuple


# =============================================================================
# env helpers
# =============================================================================
_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}


def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    return (v if v is not None else d).strip()


def _envb(k: str, d: bool = False) -> bool:
    v = _envs(k, "")
    if v == "":
        return bool(d)
    s = v.lower().strip()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return bool(d)


def _envi(k: str, d: int) -> int:
    try:
        return int(float(_envs(k, str(d))))
    except Exception:
        return int(d)


def _envf(k: str, d: float) -> float:
    try:
        return float(_envs(k, str(d)))
    except Exception:
        return float(d)


def _ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _now_ms() -> int:
    return int(time.time() * 1000)


def _split_csv(s: str) -> List[str]:
    out: List[str] = []
    for part in (s or "").replace(";", ",").split(","):
        p = part.strip()
        if p:
            out.append(p)
    return out


# =============================================================================
# safe printing (Windows cp1252 safe)
# =============================================================================
def _safe_print(s: str, *, file) -> None:
    try:
        file.write(s + ("" if s.endswith("\n") else "\n"))
        file.flush()
    except UnicodeEncodeError:
        enc = getattr(file, "encoding", None) or "utf-8"
        b = s.encode(enc, errors="replace")
        with contextlib.suppress(Exception):
            file.buffer.write(b + b"\n")
            file.buffer.flush()
    except Exception:
        pass


def _hlog(msg: str, *, json_only: bool) -> None:
    if json_only:
        return
    _safe_print(f"[smoke_lightning] {_ts()} {msg}", file=sys.stderr)


# =============================================================================
# redaction / safety
# =============================================================================
_URL_CRED_RE = re.compile(r"(://)([^/@\s]+)@")
_APIKEY_RE = re.compile(r"(?i)(x-api-key:\s*)([^\s]+)")
_BEARER_RE = re.compile(r"(?i)(authorization:\s*bearer\s+)([^\s]+)")
_LONG_TOKENISH_RE = re.compile(r"\b[A-Za-z0-9_\-]{28,}\b")

_SENSITIVE_ENV_KEYS = [
    "JUP_PRO_KEY",
    "JUP_API_KEY",
    "JUP_KEY",
    "OPENAI_API_KEY",
    "TELEGRAM_BOT_TOKEN",
    "DISCORD_TOKEN",
    "WALLET_JSON",
    "WALLET_JSON_B64",
    "WALLET_PRIVATE_KEY",
    "KEYPAIR_B64",
    "KEYPAIR_PATH",
    "DATABASE_URL",
    "PG_DSN",
    "PG_DSN_INTERNAL",
    "PG_DSN_EXTERNAL",
    "POSTGRES_DSN",
    "PNL_DSN",
    "REDIS_URL",
    "REDIS_TLS_URL",
    "RPC_URL",
    "SOL_RPC_URL",
]


def _sanitize(text: Any) -> str:
    if text is None:
        return ""
    s = str(text)
    s = _URL_CRED_RE.sub(r"\1***@", s)
    s = _APIKEY_RE.sub(r"\1***", s)
    s = _BEARER_RE.sub(r"\1***", s)

    for k in _SENSITIVE_ENV_KEYS:
        v = os.getenv(k, "")
        if v and len(v) >= 6:
            s = s.replace(v, "***")

    if _envb("PREFLIGHT_MASK_LONG_TOKENS", True):
        s = _LONG_TOKENISH_RE.sub("***", s)

    return s


def _short_exc(e: BaseException, n: int = 360) -> str:
    s = _sanitize(f"{type(e).__name__}: {e}")
    return (s[:n] + "…") if len(s) > n else s


def _mask(s: str, keep: int = 4) -> str:
    if not s:
        return ""
    s = str(s)
    if len(s) <= keep:
        return "*" * len(s)
    return s[:keep] + ("*" * (len(s) - keep))


# =============================================================================
# mint normalization (canonical if available; safe fallback)
# =============================================================================
_BASE58_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")

DEFAULT_SOL_MINT = "So11111111111111111111111111111111111111112"
DEFAULT_USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"


def _canon_mints() -> Tuple[str, str]:
    sol = DEFAULT_SOL_MINT
    usdc = DEFAULT_USDC_MINT
    with contextlib.suppress(Exception):
        from utils_pkg.mints import SOL_MINT, USDC_MINT  # type: ignore

        sol = str(SOL_MINT)
        usdc = str(USDC_MINT)
    return sol, usdc


def _try_normalize_mint_best_effort(s: str, *, sol_mint: str, usdc_mint: str) -> Optional[str]:
    raw = (s or "").strip()
    if not raw:
        return None

    # project canonical normalizer if present
    with contextlib.suppress(Exception):
        from utils_pkg.mints import normalize_mint  # type: ignore

        out = normalize_mint(raw)
        if isinstance(out, str) and out.strip():
            return out.strip()

    with contextlib.suppress(Exception):
        from utils_pkg.mints import try_normalize_mint  # type: ignore

        out2 = try_normalize_mint(raw)
        if isinstance(out2, str) and out2.strip():
            return out2.strip()

    # fallback symbols
    up = raw.upper()
    if up in {"SOL"}:
        return sol_mint
    if up in {"USDC", "USDCOIN", "USD COIN"}:
        return usdc_mint

    # fallback: accept base58-ish mint-looking strings only
    if _BASE58_RE.fullmatch(raw):
        return raw

    return None


def _probe_mint_normalization(sol_mint: str, usdc_mint: str) -> Dict[str, Any]:
    """
    Local-only sanity: confirm a few garbage strings are rejected and known symbols resolve.
    """
    tests = {
        "": None,
        "   ": None,
        "not-a-mint": None,
        "0" * 44: None,  # not base58
        "USDC": usdc_mint,
        "SOL": sol_mint,
        sol_mint: sol_mint,
        usdc_mint: usdc_mint,
    }
    results: Dict[str, Any] = {}
    for k, expected in tests.items():
        got = _try_normalize_mint_best_effort(k, sol_mint=sol_mint, usdc_mint=usdc_mint)
        results[_sanitize(k) if k else "(empty)"] = {"ok": int(got == expected), "got": got, "expected": expected}
    return results


# =============================================================================
# misc helpers
# =============================================================================
def _sol_to_lamports(amount_sol: float) -> int:
    return int(round(float(amount_sol) * 1_000_000_000))


def _to_int(x: Any, default: int = 0) -> int:
    try:
        if x is None or isinstance(x, bool):
            return int(default)
        if isinstance(x, int):
            return int(x)
        if isinstance(x, float):
            return int(x)
        s = str(x).strip()
        if not s:
            return int(default)
        return int(float(s))
    except Exception:
        return int(default)


def _extract_amounts(q: Dict[str, Any]) -> Tuple[Optional[int], Optional[int]]:
    in_amt = q.get("inAmount") or q.get("inputAmount") or q.get("in_amount") or q.get("inAmountRaw")
    out_amt = q.get("outAmount") or q.get("outputAmount") or q.get("out_amount") or q.get("outAmountRaw")
    i = _to_int(in_amt, default=0) if in_amt is not None else 0
    o = _to_int(out_amt, default=0) if out_amt is not None else 0
    return (i if i > 0 else None), (o if o > 0 else None)


def _route_count(q: Dict[str, Any]) -> Optional[int]:
    rp = q.get("routePlan") or q.get("route_plan")
    if isinstance(rp, list):
        return len(rp)
    data = q.get("data")
    if isinstance(data, list):
        return len(data)
    return None


def _brief_quote(q: Dict[str, Any]) -> Dict[str, Any]:
    i, o = _extract_amounts(q)
    meta = q.get("_meta") or {}
    return {
        "inAmountRaw": i,
        "outAmountRaw": o,
        "priceImpactPct": q.get("priceImpactPct"),
        "routes": _route_count(q),
        "endpoint": meta.get("url"),
        "status": meta.get("status"),
        "ms": meta.get("ms"),
        "used": meta.get("used"),
        "via": meta.get("via"),
    }


# =============================================================================
# Jupiter endpoint resolution
# =============================================================================
def _resolve_quote_urls(base_url: str, as_legacy: bool, quote_path_override: str) -> List[str]:
    """
    Candidate quote URLs, tried in order.
      - legacy: /swap/v1/quote
      - v6    : /v6/quote
      - alt   : /quote (some gateways/proxies)
    """
    b = (base_url or "").strip().rstrip("/")
    if not b:
        b = "https://api.jup.ag"

    if quote_path_override and quote_path_override.strip().startswith("/"):
        return [b + quote_path_override.strip()]

    if as_legacy:
        return [b + "/swap/v1/quote", b + "/quote", b + "/v6/quote"]
    return [b + "/v6/quote", b + "/quote", b + "/swap/v1/quote"]


def _jup_headers() -> Dict[str, str]:
    pro = _envs("JUP_PRO_KEY", "")
    api = _envs("JUP_API_KEY", "") or _envs("JUP_KEY", "")
    h: Dict[str, str] = {"user-agent": "pnlsniper-smoke"}
    key = pro or api
    if key:
        h["x-api-key"] = key
    return h


def _diagnose_error(payload: Any, raw_text: str = "") -> str:
    if isinstance(payload, dict):
        e = payload.get("error") or payload.get("message") or payload.get("code")
        if isinstance(e, str) and e.strip():
            return e.strip()
        if isinstance(payload.get("error"), dict):
            e2 = payload["error"]
            msg = e2.get("message") or e2.get("code") or e2.get("error")
            if isinstance(msg, str) and msg.strip():
                return msg.strip()
    t = (raw_text or "").strip()
    return t[:260] if t else "unknown_error"


def _classify_http_error(msg: str) -> str:
    m = (msg or "").lower()
    if "429" in m or "rate" in m:
        return "rate_limit"
    if "401" in m or "403" in m or "unauthorized" in m or "forbidden" in m:
        return "auth"
    if "timeout" in m or "timed out" in m:
        return "timeout"
    if "dns" in m or "name or service not known" in m or "could not translate host name" in m:
        return "dns"
    return "other"


# =============================================================================
# HTTP quote (async) with httpx, fallback to requests in a thread
# =============================================================================
async def _http_get_json(url: str, params: Dict[str, Any], headers: Dict[str, str], timeout_s: float) -> Tuple[int, Any, str]:
    try:
        import httpx  # type: ignore

        async with httpx.AsyncClient(timeout=timeout_s) as c:
            r = await c.get(url, params=params, headers=headers)
            text = r.text
            try:
                return int(r.status_code), r.json(), text
            except Exception:
                return int(r.status_code), {"_non_json": True, "text": text[:2000]}, text
    except Exception:
        import requests  # type: ignore

        def _do() -> Tuple[int, Any, str]:
            rr = requests.get(url, params=params, headers=headers, timeout=timeout_s)
            t = rr.text
            try:
                return int(rr.status_code), rr.json(), t
            except Exception:
                return int(rr.status_code), {"_non_json": True, "text": t[:2000]}, t

        return await asyncio.to_thread(_do)


async def _direct_http_quote(
    *,
    base_url: str,
    as_legacy: bool,
    quote_path_override: str,
    input_mint: str,
    output_mint: str,
    amount_raw: int,
    slippage_bps: int,
    only_direct: bool,
    timeout_s: float,
    debug: bool,
    json_only: bool,
) -> Dict[str, Any]:
    urls = _resolve_quote_urls(base_url, as_legacy, quote_path_override)
    headers = _jup_headers()

    params_base: Dict[str, Any] = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": int(amount_raw),
        "slippageBps": int(slippage_bps),
    }
    if only_direct:
        params_base["onlyDirectRoutes"] = "true"

    # small backoff for rate limits / transient errors
    backoff_s = 0.35
    last_err = "no_attempts"
    tried: List[str] = []

    for u in urls:
        tried.append(u)
        for attempt in range(1, 4):
            t0 = _now_ms()
            try:
                if debug:
                    _hlog(f"HTTP GET {u} attempt={attempt} keys={sorted(list(params_base.keys()))}", json_only=json_only)

                status, js, raw = await _http_get_json(u, dict(params_base), headers, timeout_s=timeout_s)
                dt = _now_ms() - t0

                if status >= 400:
                    # common fallback: some gateways reject onlyDirectRoutes param
                    if status in (400, 422) and ("onlyDirectRoutes" in params_base):
                        params_retry = dict(params_base)
                        params_retry.pop("onlyDirectRoutes", None)
                        status2, js2, raw2 = await _http_get_json(u, params_retry, headers, timeout_s=timeout_s)
                        dt2 = _now_ms() - t0
                        if status2 < 400 and isinstance(js2, dict):
                            js2["_meta"] = {"url": u, "ms": dt2, "status": status2, "used": "direct_http_retry_no_onlyDirectRoutes"}
                            js2["_meta"]["via"] = "direct_http"
                            return js2
                        last_err = f"HTTP {status2} @ {u}: {_diagnose_error(js2, raw2)}"
                    else:
                        last_err = f"HTTP {status} @ {u}: {_diagnose_error(js, raw)}"

                    # backoff only for rate-limit / transient
                    kind = _classify_http_error(last_err)
                    if kind in {"rate_limit", "timeout"} or status >= 500:
                        await asyncio.sleep(min(1.8, backoff_s))
                        backoff_s *= 1.7
                        continue

                    break  # non-retryable -> next URL

                if isinstance(js, dict):
                    js["_meta"] = {"url": u, "ms": dt, "status": status, "used": "direct_http", "via": "direct_http"}
                    return js

                last_err = f"non_dict_payload @ {u}"
                break

            except Exception as e:
                last_err = f"{type(e).__name__} @ {u}: {_sanitize(e)}"
                kind = _classify_http_error(last_err)
                if kind in {"timeout", "dns"}:
                    await asyncio.sleep(min(1.8, backoff_s))
                    backoff_s *= 1.7
                    continue
                break

    raise RuntimeError(f"{last_err} | tried={len(tried)}")


# =============================================================================
# Lightning quote (signature-drift tolerant)
# =============================================================================
async def _maybe_await(x: Any) -> Any:
    return await x if asyncio.iscoroutine(x) or asyncio.isfuture(x) else x


def _filter_kwargs(fn: Any, kwargs: Dict[str, Any]) -> Dict[str, Any]:
    try:
        sig = inspect.signature(fn)
        params = sig.parameters.values()
        if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params):
            return kwargs
        allowed = {p.name for p in params}
        return {k: v for k, v in kwargs.items() if k in allowed}
    except Exception:
        return kwargs


def _normalize_quote_return(res: Any, *, used: str, via: str) -> Optional[Dict[str, Any]]:
    """
    Normalize various possible return shapes into a dict quote payload.
    """
    if isinstance(res, dict):
        out = dict(res)
        meta = dict(out.get("_meta") or {})
        meta.setdefault("used", used)
        meta.setdefault("via", via)
        out["_meta"] = meta
        return out

    # tuple/list shapes
    if isinstance(res, (tuple, list)) and res:
        # common: (ok, payload) or (payload, meta)
        for item in res:
            if isinstance(item, dict):
                out = dict(item)
                meta = dict(out.get("_meta") or {})
                meta.setdefault("used", used)
                meta.setdefault("via", via)
                out["_meta"] = meta
                return out

    return None


async def _try_lightning_quote(
    *,
    input_mint: str,
    output_mint: str,
    amount_raw: int,
    slippage_bps: int,
    only_direct: bool,
    timeout_s: float,
    debug: bool,
    json_only: bool,
) -> Tuple[bool, Optional[Dict[str, Any]], str]:
    """
    Attempts to call into utils_pkg.lightning_impl with signature drift tolerance.
    Returns: (ok, quote_dict_or_none, via_string)
    """
    try:
        from utils_pkg import lightning_impl as L  # type: ignore
    except Exception as e:
        return False, None, f"import_failed:{type(e).__name__}:{_short_exc(e)}"

    fn_names = (
        "quote",
        "get_quote",
        "jup_quote",
        "get_jup_quote",
        "quote_once",
        "quote_raw",
        "quote_v6",
        "quote_legacy",
    )

    fn = None
    fn_name = ""
    for nm in fn_names:
        f = getattr(L, nm, None)
        if callable(f):
            fn = f
            fn_name = nm
            break

    if not callable(fn):
        return False, None, "no_quote_fn"

    # superset kwargs (we'll filter by signature)
    superset: Dict[str, Any] = {
        # Jupiter-style
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": int(amount_raw),
        "slippageBps": int(slippage_bps),
        "onlyDirectRoutes": bool(only_direct),
        # internal-style
        "input_mint": input_mint,
        "output_mint": output_mint,
        "amount_raw": int(amount_raw),
        "slippage_bps": int(slippage_bps),
        "only_direct": bool(only_direct),
        # timeouts (if supported)
        "timeout_s": float(timeout_s),
        "timeout": float(timeout_s),
    }

    variants: List[Tuple[Tuple[Any, ...], Dict[str, Any]]] = [
        ((), dict(superset)),
        ((input_mint, output_mint, int(amount_raw)), {"slippageBps": int(slippage_bps), "onlyDirectRoutes": bool(only_direct)}),
        ((input_mint, output_mint, int(amount_raw), int(slippage_bps)), {"onlyDirectRoutes": bool(only_direct)}),
    ]

    last_err = "unknown"
    for args, kwargs in variants:
        try:
            kw = _filter_kwargs(fn, kwargs)
            if debug:
                _hlog(f"LIGHTNING CALL {fn_name} args={len(args)} kwargs={sorted(list(kw.keys()))}", json_only=json_only)
            res = await asyncio.wait_for(_maybe_await(fn(*args, **kw)), timeout=max(2.0, float(timeout_s)))
            out = _normalize_quote_return(res, used="lightning", via=f"lightning_impl.{fn_name}")
            if out is not None:
                return True, out, fn_name
            last_err = f"unexpected_return:{type(res).__name__}"
        except asyncio.TimeoutError:
            last_err = "timeout"
            continue
        except Exception as e:
            last_err = _short_exc(e)
            continue

    return False, None, f"{fn_name}:{last_err}"


# =============================================================================
# Optional router cross-check (quote only)
# =============================================================================
async def _try_router_quote(
    *,
    input_mint: str,
    output_mint: str,
    amount_raw: int,
    slippage_bps: int,
    only_direct: bool,
    timeout_s: float,
) -> Optional[Dict[str, Any]]:
    """
    Best-effort quote via utils_pkg.router for "matches prod stack" validation.
    Never required; never blocks success.
    """
    with contextlib.suppress(Exception):
        from utils_pkg import router  # type: ignore

        # try common quote functions
        fns = []
        for nm in ("get_buy_quote", "get_quote", "quote", "get_jup_quote", "get_sell_quote"):
            f = getattr(router, nm, None)
            if callable(f):
                fns.append((nm, f))

        if not fns:
            return None

        nm, fn = fns[0]
        # try kw arg names both styles
        sup = {
            "input_mint": input_mint,
            "output_mint": output_mint,
            "mint": output_mint,
            "amount_raw": int(amount_raw),
            "amount": int(amount_raw),
            "slippage_bps": int(slippage_bps),
            "slippageBps": int(slippage_bps),
            "only_direct": bool(only_direct),
            "onlyDirectRoutes": bool(only_direct),
        }
        kw = _filter_kwargs(fn, sup)
        res = await asyncio.wait_for(_maybe_await(fn(**kw)), timeout=max(2.0, float(timeout_s)))
        out = _normalize_quote_return(res, used="router", via=f"router.{nm}")
        return out
    return None


# =============================================================================
# main
# =============================================================================
async def _amain() -> int:
    ap = argparse.ArgumentParser(description="Lightning/Jupiter quote smoke test (LOCK-IN)")
    ap.add_argument("--amount-sol", type=float, default=float(_envf("SMOKE_AMOUNT_SOL", 0.005)), help="SOL input amount")
    ap.add_argument("--out", type=str, default=_envs("SMOKE_OUT", "USDC"), help="Output mint/symbol (normalized)")
    ap.add_argument("--slippage-bps", type=int, default=int(_envi("SMOKE_SLIPPAGE_BPS", 50)), help="Slippage bps")
    ap.add_argument("--only-direct", type=int, default=1 if _envb("SMOKE_ONLY_DIRECT", False) else 0, help="1 to force direct routes")
    ap.add_argument("--roundtrip", type=int, default=1 if _envb("SMOKE_ROUNDTRIP", False) else 0, help="1 to quote OUT->SOL too")
    ap.add_argument("--require-roundtrip", type=int, default=1 if _envb("SMOKE_REQUIRE_ROUNDTRIP", False) else 0, help="1 to fail if roundtrip fails")
    ap.add_argument("--require-lightning", type=int, default=1 if _envb("SMOKE_REQUIRE_LIGHTNING", False) else 0, help="1 to fail if Lightning isn't used")
    ap.add_argument("--allow-http-fallback", type=int, default=1 if _envb("SMOKE_ALLOW_HTTP_FALLBACK", True) else 0, help="1 to allow fallback to direct HTTP")
    ap.add_argument("--no-fallback", type=int, default=0, help="Alias: if 1, forces allow-http-fallback=0")
    ap.add_argument("--base", type=str, default=_envs("SMOKE_BASE_URL", ""), help="Override base URL for direct HTTP fallback")
    ap.add_argument("--use-alt", type=int, default=1 if _envb("SMOKE_USE_ALT", False) else 0, help="1 to random pick from JUP_ALT_BASE_URLS")
    ap.add_argument("--timeout", type=float, default=float(_envf("SMOKE_TIMEOUT_S", 15.0)), help="Per-call timeout seconds")
    ap.add_argument("--total-timeout", type=float, default=float(_envf("SMOKE_TOTAL_TIMEOUT_S", 0.0)), help="Total cap seconds (0 disables)")
    ap.add_argument("--json", action="store_true", default=_envb("SMOKE_JSON", False), help="Emit JSON summary to STDOUT")
    ap.add_argument("--json-only", action="store_true", default=_envb("SMOKE_JSON_ONLY", False), help="Only JSON output (no human logs)")
    ap.add_argument("--debug", action="store_true", default=_envb("SMOKE_DEBUG", False), help="Verbose trace (sanitized)")
    ap.add_argument("--probe-mints", type=int, default=1 if _envb("SMOKE_PROBE_MINTS", False) else 0, help="1 to run local mint normalization probes")
    ap.add_argument("--use-router", type=int, default=1 if _envb("SMOKE_USE_ROUTER", False) else 0, help="1 to also quote via utils_pkg.router and compare (best-effort)")
    args = ap.parse_args()

    json_only = bool(args.json_only)
    emit_json = bool(args.json or args.json_only)
    debug = bool(args.debug)

    timeout_s = max(5.0, float(args.timeout))
    total_timeout_s = max(0.0, float(args.total_timeout))

    # Jupiter env
    base_env = _envs("JUP_BASE_URL", "https://api.jup.ag")
    alt_env = _split_csv(_envs("JUP_ALT_BASE_URLS", ""))
    as_legacy = _envb("JUP_AS_LEGACY", False)
    only_direct = bool(int(args.only_direct)) or _envb("JUP_ONLY_DIRECT", False)
    quote_path_override = _envs("JUP_QUOTE_PATH", "").strip()

    pro_key = _envs("JUP_PRO_KEY", "")
    api_key = _envs("JUP_API_KEY", "") or _envs("JUP_KEY", "")

    chosen_base = (args.base or base_env).strip() or base_env
    if int(args.use_alt) == 1 and alt_env:
        # make deterministic optional by honoring SMOKE_SEED if set
        seed = _envs("SMOKE_SEED", "")
        if seed:
            with contextlib.suppress(Exception):
                random.seed(int(seed))
        chosen_base = random.choice(alt_env)

    sol_mint, usdc_mint = _canon_mints()

    # normalize out mint (MUST be valid before any network call)
    out_norm = _try_normalize_mint_best_effort(args.out, sol_mint=sol_mint, usdc_mint=usdc_mint)
    if not out_norm:
        summary = {"ts": _ts(), "ok": False, "error": f"normalize_failed:{_sanitize(args.out)}"}
        if emit_json:
            _safe_print(json.dumps(summary, ensure_ascii=False), file=sys.stdout)
        _hlog(f"FAIL: could not normalize --out={args.out!r}", json_only=json_only)
        return 2

    amount_sol = max(0.0, float(args.amount_sol))
    lamports = _sol_to_lamports(amount_sol)
    if lamports <= 0:
        summary = {"ts": _ts(), "ok": False, "error": "amount_sol_must_be_positive"}
        if emit_json:
            _safe_print(json.dumps(summary, ensure_ascii=False), file=sys.stdout)
        _hlog("FAIL: amount-sol must be > 0", json_only=json_only)
        return 2

    slippage_bps = int(max(1, min(5000, int(args.slippage_bps))))
    require_lightning = bool(int(args.require_lightning))
    require_roundtrip = bool(int(args.require_roundtrip))
    allow_http_fallback = bool(int(args.allow_http_fallback))
    if int(args.no_fallback) == 1:
        allow_http_fallback = False
    do_roundtrip = bool(int(args.roundtrip) or require_roundtrip)

    # summary skeleton
    t0 = _now_ms()
    summary: Dict[str, Any] = {
        "ts": _ts(),
        "ok": False,
        "used_lightning": False,
        "fell_back_to_http": False,
        "require_lightning": int(require_lightning),
        "allow_http_fallback": int(allow_http_fallback),
        "require_roundtrip": int(require_roundtrip),
        "as_legacy": int(bool(as_legacy)),
        "only_direct": int(bool(only_direct)),
        "base_url": _sanitize(chosen_base),
        "quote_path_override": (quote_path_override or None),
        "in_mint": sol_mint,
        "out_mint": out_norm,
        "amount_sol": float(amount_sol),
        "amount_raw": int(lamports),
        "slippage_bps": int(slippage_bps),
        "env": {
            "JUP_BASE_URL": _sanitize(base_env),
            "JUP_ALT_BASE_URLS": [_sanitize(x) for x in alt_env][:20],
            "has_JUP_PRO_KEY": int(bool(pro_key)),
            "has_JUP_API_KEY": int(bool(api_key)),
            "JUP_PRO_KEY_mask": _mask(pro_key),
            "JUP_API_KEY_mask": _mask(api_key),
        },
        "mint_probe": None,
        "buy": None,
        "roundtrip": None,
        "router_check": None,
        "errors": [],
        "warnings": [],
        "elapsed_ms": 0,
    }

    if int(args.probe_mints) == 1:
        summary["mint_probe"] = _probe_mint_normalization(sol_mint, usdc_mint)

    if not json_only:
        _hlog("────────────────────────────────────────────────────────────", json_only=False)
        _hlog(" V3+++ smoke_lightning — Lightning/Jupiter QUOTE-ONLY sanity  ", json_only=False)
        _hlog("────────────────────────────────────────────────────────────", json_only=False)
        _hlog(f"python={sys.version.split()[0]} platform={platform.platform()}", json_only=False)
        _hlog(
            _sanitize(
                "env "
                + json.dumps(
                    {
                        "JUP_BASE_URL": base_env,
                        "JUP_ALT_BASE_URLS": alt_env,
                        "JUP_AS_LEGACY": as_legacy,
                        "JUP_ONLY_DIRECT": _envb("JUP_ONLY_DIRECT", False),
                        "JUP_QUOTE_PATH": quote_path_override or "(default)",
                        "has_JUP_PRO_KEY": bool(pro_key),
                        "has_JUP_API_KEY": bool(api_key),
                        "JUP_PRO_KEY": _mask(pro_key),
                        "JUP_API_KEY": _mask(api_key),
                    },
                    ensure_ascii=False,
                )
            ),
            json_only=False,
        )
        _hlog(f"mints in=SOL({sol_mint}) out={out_norm} (from {args.out!r})", json_only=False)
        _hlog(
            f"params amount_sol={amount_sol:.9f} lamports={lamports} slippage_bps={slippage_bps} "
            f"only_direct={int(only_direct)} roundtrip={int(do_roundtrip)} "
            f"require_lightning={int(require_lightning)} allow_http_fallback={int(allow_http_fallback)}",
            json_only=False,
        )
        _hlog(f"direct_http_base={_sanitize(chosen_base)}", json_only=False)

    async def _run() -> int:
        # 1) Lightning-first BUY
        ok_l, q1, via = await _try_lightning_quote(
            input_mint=sol_mint,
            output_mint=out_norm,
            amount_raw=lamports,
            slippage_bps=slippage_bps,
            only_direct=only_direct,
            timeout_s=timeout_s,
            debug=debug,
            json_only=json_only,
        )

        if ok_l and isinstance(q1, dict):
            summary["used_lightning"] = True
            summary["buy"] = _brief_quote(q1)
            _hlog(f"OK: lightning buy via={via} :: {summary['buy']}", json_only=json_only)
        else:
            # IMPORTANT: preflight can enforce policy by grep'ing this exact phrase.
            _hlog("NOTE: falling back to direct http (lightning unavailable or failed)", json_only=json_only)
            summary["fell_back_to_http"] = True

            if not allow_http_fallback:
                summary["errors"].append(f"lightning_failed:{_sanitize(via)}")
                summary["errors"].append("http_fallback_disabled")
                return 2

            try:
                q1 = await _direct_http_quote(
                    base_url=chosen_base,
                    as_legacy=as_legacy,
                    quote_path_override=quote_path_override,
                    input_mint=sol_mint,
                    output_mint=out_norm,
                    amount_raw=lamports,
                    slippage_bps=slippage_bps,
                    only_direct=only_direct,
                    timeout_s=timeout_s,
                    debug=debug,
                    json_only=json_only,
                )
                summary["buy"] = _brief_quote(q1)
                _hlog(f"OK: direct http buy :: {summary['buy']}", json_only=json_only)
            except Exception as e:
                summary["errors"].append(_sanitize(f"buy_quote_failed:{type(e).__name__}:{e}"))
                return 2

        # enforce lightning policy
        if require_lightning and (not summary["used_lightning"]):
            summary["errors"].append("require_lightning=1 but lightning path was not used")
            return 2

        # ensure outAmount exists
        if not isinstance(q1, dict):
            summary["errors"].append("internal_error:no_quote_payload")
            return 2

        _, out_amt = _extract_amounts(q1)
        if not out_amt or out_amt <= 0:
            summary["errors"].append("quote_missing_outAmount")
            return 2

        # optional router cross-check (does not affect success)
        if int(args.use_router) == 1:
            q_router = await _try_router_quote(
                input_mint=sol_mint,
                output_mint=out_norm,
                amount_raw=lamports,
                slippage_bps=slippage_bps,
                only_direct=only_direct,
                timeout_s=timeout_s,
            )
            if isinstance(q_router, dict):
                summary["router_check"] = _brief_quote(q_router)
                # warn if wildly off (very loose; just sanity)
                _, o_r = _extract_amounts(q_router)
                if o_r and out_amt and out_amt > 0:
                    delta = abs(int(o_r) - int(out_amt)) / float(out_amt)
                    if delta > 0.25:
                        summary["warnings"].append(f"router_quote_deviation_gt_25pct delta={delta:.2f}")

        # 2) optional roundtrip OUT -> SOL
        if do_roundtrip:
            clamp_max = int(_envi("SMOKE_SELL_CLAMP_RAW", 50_000_000))
            clamp_min = int(_envi("SMOKE_SELL_MIN_RAW", 10))
            probe_raw = int(max(clamp_min, min(int(out_amt), clamp_max)))

            ok_l2, q2, via2 = await _try_lightning_quote(
                input_mint=out_norm,
                output_mint=sol_mint,
                amount_raw=probe_raw,
                slippage_bps=slippage_bps,
                only_direct=only_direct,
                timeout_s=timeout_s,
                debug=debug,
                json_only=json_only,
            )

            if ok_l2 and isinstance(q2, dict):
                summary["roundtrip"] = _brief_quote(q2)
                _hlog(f"OK: lightning roundtrip via={via2} :: {summary['roundtrip']}", json_only=json_only)
            else:
                _hlog("NOTE: falling back to direct http for roundtrip (lightning failed)", json_only=json_only)
                summary["fell_back_to_http"] = True

                if allow_http_fallback:
                    try:
                        q2 = await _direct_http_quote(
                            base_url=chosen_base,
                            as_legacy=as_legacy,
                            quote_path_override=quote_path_override,
                            input_mint=out_norm,
                            output_mint=sol_mint,
                            amount_raw=probe_raw,
                            slippage_bps=slippage_bps,
                            only_direct=only_direct,
                            timeout_s=timeout_s,
                            debug=debug,
                            json_only=json_only,
                        )
                        summary["roundtrip"] = _brief_quote(q2)
                        _hlog(f"OK: direct http roundtrip :: {summary['roundtrip']}", json_only=json_only)
                    except Exception as e:
                        msg = _sanitize(f"roundtrip_failed:{type(e).__name__}:{e}")
                        if require_roundtrip:
                            summary["errors"].append(msg)
                            return 2
                        summary["warnings"].append(msg)
                else:
                    msg = f"roundtrip_lightning_failed:{_sanitize(via2)}"
                    if require_roundtrip:
                        summary["errors"].append(msg)
                        return 2
                    summary["warnings"].append(msg)

        summary["ok"] = True
        return 0

    rc = 2
    try:
        if total_timeout_s > 0:
            rc = await asyncio.wait_for(_run(), timeout=float(total_timeout_s))
        else:
            rc = await _run()
    except asyncio.TimeoutError:
        summary["errors"].append("total_timeout")
        rc = 2
    finally:
        summary["elapsed_ms"] = _now_ms() - t0

        # add minimal hints (no secrets)
        msg_all = " ".join([str(x) for x in summary["errors"]] + [str(x) for x in summary["warnings"]]).lower()
        hints: List[str] = []
        kind = _classify_http_error(msg_all)
        if kind == "rate_limit":
            hints.append("Rate limiting detected. Add/verify Jupiter API key, reduce quote frequency, or rotate base URLs.")
        if kind == "auth":
            hints.append("Auth failure detected. Check JUP_PRO_KEY/JUP_API_KEY and whether your endpoint requires a key.")
        if kind == "timeout":
            hints.append("Timeout detected. Increase SMOKE_TIMEOUT_S / SMOKE_TOTAL_TIMEOUT_S or use a faster endpoint.")
        if kind == "dns":
            hints.append("DNS/network issue detected. Verify outbound DNS from runtime or switch endpoints.")
        if "require_lightning" in msg_all:
            hints.append("Lightning was required but not used. Ensure utils_pkg/lightning_impl.py is present and healthy.")
        if "normalize_failed" in msg_all:
            hints.append("Mint normalization failed. Pass USDC/SOL or a base58 mint address.")
        # de-dupe
        dedup: List[str] = []
        seen = set()
        for h in hints:
            if h not in seen:
                seen.add(h)
                dedup.append(h)
        summary["fix_hints"] = dedup[:10]

        if emit_json:
            _safe_print(json.dumps(summary, ensure_ascii=False, default=str), file=sys.stdout)

        if not emit_json:
            _hlog(
                ("OK" if summary.get("ok") else "FAIL")
                + f" used_lightning={summary.get('used_lightning')}"
                + f" fell_back_to_http={summary.get('fell_back_to_http')}"
                + f" elapsed_ms={summary.get('elapsed_ms')}",
                json_only=json_only,
            )

    return 0 if bool(summary.get("ok")) else 2


def main() -> None:
    try:
        rc = asyncio.run(_amain())
    except KeyboardInterrupt:
        rc = 130
    except Exception:
        err = _sanitize(traceback.format_exc())
        if _envb("SMOKE_JSON", False) or _envb("SMOKE_JSON_ONLY", False):
            _safe_print(json.dumps({"ts": _ts(), "ok": False, "error": err[:700]}, ensure_ascii=False), file=sys.stdout)
        else:
            _safe_print(f"[smoke_lightning] {_ts()} FAIL unhandled\n{err[:1600]}", file=sys.stderr)
        rc = 2
    raise SystemExit(rc)


if __name__ == "__main__":
    with contextlib.suppress(Exception):
        if os.name == "nt" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]
    main()
