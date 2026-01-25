# scripts/smoke_lightning.py
from __future__ import annotations

"""
scripts/smoke_lightning.py — V3+++ Lightning Quote Smoke (LOCK-IN)
==================================================================

Goal
----
Prove end-to-end QUOTE works through the Lightning layer (preferred), and that
mint normalization blocks garbage mints BEFORE any Jupiter request.

Hard rules
----------
- QUOTE ONLY: no swaps, no tx building, no signing, no sending.
- Never prints secrets (api keys, wallets, DSNs).
- JSON (when enabled) goes to STDOUT; human logs go to STDERR.
- Emits explicit phrase:
    "NOTE: falling back to direct http"
  whenever Lightning isn't used and we attempt fallback. Preflight can enforce policy.

Exit codes
----------
0 = success
2 = failure
130 = ctrl-c

CLI examples
------------
py -m scripts.smoke_lightning --amount-sol 0.005 --out USDC --slippage-bps 50
py -m scripts.smoke_lightning --require-lightning 1 --json
py -m scripts.smoke_lightning --roundtrip 1 --require-roundtrip 1 --total-timeout 25 --debug
py -m scripts.smoke_lightning --no-fallback 1 --json

Env knobs (compatible with your current convention)
---------------------------------------------------
SMOKE_TIMEOUT_S=15
SMOKE_TOTAL_TIMEOUT_S=0                (0 disables total cap)
SMOKE_JSON=0|1
SMOKE_JSON_ONLY=0|1
SMOKE_DEBUG=0|1

SMOKE_REQUIRE_LIGHTNING=0|1
SMOKE_REQUIRE_ROUNDTRIP=0|1
SMOKE_ALLOW_HTTP_FALLBACK=1|0          (default 1)

SMOKE_BASE_URL=<override>              (else uses JUP_BASE_URL)
SMOKE_USE_ALT=0|1                      (if 1 -> random choice from JUP_ALT_BASE_URLS; SMOKE_SEED makes deterministic)
SMOKE_AMOUNT_SOL=0.005
SMOKE_OUT=USDC
SMOKE_SLIPPAGE_BPS=50
SMOKE_ONLY_DIRECT=0|1
SMOKE_ROUNDTRIP=0|1

SMOKE_SELL_CLAMP_RAW=50000000
SMOKE_SELL_MIN_RAW=10
SMOKE_PROBE_MINTS=0|1

Optional cross-check (still QUOTE ONLY)
---------------------------------------
SMOKE_USE_ROUTER=0|1                   (best-effort quote via utils_pkg.router and compare)

Jupiter env
-----------
JUP_BASE_URL=https://api.jup.ag
JUP_ALT_BASE_URLS=...
JUP_AS_LEGACY=0|1
JUP_ONLY_DIRECT=0|1
JUP_PRO_KEY=...
JUP_API_KEY=... (or JUP_KEY)
JUP_QUOTE_PATH=/v6/quote (optional override; if set must start with /)

Secret redaction knobs
----------------------
PREFLIGHT_REDACT_KEYS=K1,K2,...        (extra env keys to redact by exact value)
PREFLIGHT_MASK_LONG_TOKENS=0|1         (default 1; BUT we do not mask base58 pubkeys/mints)
"""

import argparse
import asyncio
import contextlib
import importlib
import inspect
import json
import os
import platform
import random
import re
import sys
import time
import traceback
from pathlib import Path
from typing import Any

RC_OK = 0
RC_FAIL = 2
RC_CTRL_C = 130

_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}

DEFAULT_SOL_MINT = "So11111111111111111111111111111111111111112"
DEFAULT_USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

# basic redaction patterns
_URL_CRED_RE = re.compile(r"(://)([^/@\s]+)@")
_APIKEY_HDR_RE = re.compile(r"(?i)(x-api-key:\s*)([^\s]+)")
_BEARER_HDR_RE = re.compile(r"(?i)(authorization:\s*bearer\s+)([^\s]+)")
_KV_SECRET_RE = re.compile(
    r"(?i)\b(api[_-]?key|token|secret|password|pass)\b(\s*[:=]\s*)([^\s,;]+)"
)

# token-ish (we will NOT mask base58 pubkeys/mints)
_LONG_TOKENISH_RE = re.compile(r"\b[A-Za-z0-9_\-]{28,}\b")
_BASE58_PUBKEY_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")


# =============================================================================
# repo-root bootstrap (direct-run friendly)
# =============================================================================
def _bootstrap_repo_root() -> str:
    try:
        here = Path(__file__).resolve()
    except Exception:
        return ""

    markers = ("pyproject.toml", "requirements.txt", "setup.cfg", ".git")
    candidates = [here.parent, *list(here.parents)]

    root: Path | None = None
    for p in candidates:
        with contextlib.suppress(Exception):
            if any((p / m).exists() for m in markers) or (p / "src").is_dir():
                root = p
                break

    if root is None:
        root = here.parents[1] if len(here.parents) > 1 else here.parent

    root_str = str(root)
    if root_str and root_str not in sys.path:
        sys.path.insert(0, root_str)

    os.environ.setdefault("REPO_ROOT", root_str)
    return root_str


# =============================================================================
# env helpers
# =============================================================================
def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    s = v if v is not None else d
    s = str(s).strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    return s


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


def _split_csv(s: str) -> list[str]:
    out: list[str] = []
    for part in (s or "").replace(";", ",").split(","):
        p = part.strip()
        if p:
            out.append(p)
    return out


# =============================================================================
# safe printing (cp1252-safe)
# =============================================================================
def _safe_write(file, s: str) -> None:
    try:
        file.write(s)
        file.flush()
        return
    except UnicodeEncodeError:
        enc = getattr(file, "encoding", None) or "utf-8"
        b = s.encode(enc, errors="replace")
        with contextlib.suppress(Exception):
            if hasattr(file, "buffer"):
                file.buffer.write(b)
                file.buffer.flush()
                return
        with contextlib.suppress(Exception):
            file.write(b.decode(enc, errors="replace"))
            file.flush()
            return
    except Exception:
        return


def _hlog(msg: str, *, json_only: bool) -> None:
    if json_only:
        return
    _safe_write(sys.stderr, f"[smoke_lightning] {_ts()} {msg}\n")


# =============================================================================
# redaction / safety
# =============================================================================
def _redact_keys() -> list[str]:
    base = [
        "JUP_PRO_KEY",
        "JUP_API_KEY",
        "JUP_KEY",
        "OPENAI_API_KEY",
        "TELEGRAM_BOT_TOKEN",
        "DISCORD_TOKEN",
        "DISCORD_WEBHOOK_URL",
        "NOTION_TOKEN",
        "NOTION_DATABASE_ID",
        "WALLET_JSON",
        "WALLET_JSON_B64",
        "WALLET_PRIVATE_KEY",
        "KEYPAIR_B64",
        "KEYPAIR_PATH",
        "DATABASE_URL",
        "POSTGRES_DSN",
        "PG_DSN",
        "PG_DSN_INTERNAL",
        "PG_DSN_EXTERNAL",
        "PNL_DSN",
        "REDIS_URL",
        "REDIS_TLS_URL",
        "RPC_URL",
        "SOL_RPC_URL",
        "SOL_RPC_URLS",
    ]
    extra = [
        p.strip()
        for p in _envs("PREFLIGHT_REDACT_KEYS", "").replace(";", ",").split(",")
        if p.strip()
    ]
    out: list[str] = []
    seen = set()
    for k in base + extra:
        if k and k not in seen:
            out.append(k)
            seen.add(k)
    return out


def _mask(s: str, keep: int = 4) -> str:
    if not s:
        return ""
    s = str(s)
    if len(s) <= keep:
        return "*" * len(s)
    return s[:keep] + ("*" * (len(s) - keep))


def _sanitize(x: Any) -> str:
    """
    Secret-safe sanitizer:
      - removes URL creds and token-ish headers
      - replaces exact env secret values
      - optionally masks long token-ish blobs BUT does NOT mask base58 pubkeys/mints
    """
    if x is None:
        return ""
    s = str(x)

    s = _URL_CRED_RE.sub(r"\1***@", s)
    s = _APIKEY_HDR_RE.sub(r"\1***", s)
    s = _BEARER_HDR_RE.sub(r"\1***", s)
    s = _KV_SECRET_RE.sub(r"\1\2***", s)

    for k in _redact_keys():
        v = os.getenv(k, "")
        if v and len(v) >= 6:
            s = s.replace(v, "***")

    if _envb("PREFLIGHT_MASK_LONG_TOKENS", True):

        def _repl(m: re.Match) -> str:
            t = m.group(0) or ""
            # keep base58 pubkeys/mints visible (not secrets)
            if _BASE58_PUBKEY_RE.fullmatch(t):
                return t
            return "***"

        s = _LONG_TOKENISH_RE.sub(_repl, s)

    return s


def _short_exc(e: BaseException, n: int = 420) -> str:
    s = _sanitize(f"{type(e).__name__}: {e}")
    return (s[:n] + "…") if len(s) > n else s


def _maybe_load_dotenv() -> None:
    if _envb("SMOKE_NO_DOTENV", False):
        return
    with contextlib.suppress(Exception):
        from dotenv import load_dotenv  # type: ignore

        load_dotenv(override=False)


# =============================================================================
# mint normalization (canonical if available; safe fallback)
# =============================================================================
def _canon_mints() -> tuple[str, str]:
    sol = DEFAULT_SOL_MINT
    usdc = DEFAULT_USDC_MINT
    with contextlib.suppress(Exception):
        m = importlib.import_module("utils_pkg.mints")
        sol = str(m.SOL_MINT)
        usdc = str(m.USDC_MINT)
    return sol, usdc


def _normalize_mint(raw: str, *, sol_mint: str, usdc_mint: str) -> str | None:
    s = (raw or "").strip()
    if not s:
        return None

    # Prefer project canonical normalizers if present
    with contextlib.suppress(Exception):
        m = importlib.import_module("utils_pkg.mints")
        fn = getattr(m, "normalize_mint", None)
        if callable(fn):
            out = fn(s)
            if isinstance(out, str) and out.strip():
                return out.strip()
    with contextlib.suppress(Exception):
        m = importlib.import_module("utils_pkg.mints")
        fn = getattr(m, "try_normalize_mint", None)
        if callable(fn):
            out = fn(s)
            if isinstance(out, str) and out.strip():
                return out.strip()

    up = s.upper()
    if up == "SOL":
        return sol_mint
    if up in {"USDC", "USDCOIN", "USD COIN"}:
        return usdc_mint

    if _BASE58_PUBKEY_RE.fullmatch(s):
        return s

    return None


def _probe_mint_normalization(sol_mint: str, usdc_mint: str) -> dict[str, Any]:
    tests = {
        "": None,
        "   ": None,
        "not-a-mint": None,
        "0" * 44: None,
        "USDC": usdc_mint,
        "SOL": sol_mint,
        sol_mint: sol_mint,
        usdc_mint: usdc_mint,
    }
    results: dict[str, Any] = {}
    for k, expected in tests.items():
        got = _normalize_mint(k, sol_mint=sol_mint, usdc_mint=usdc_mint)
        results[_sanitize(k) if k else "(empty)"] = {
            "ok": int(got == expected),
            "got": got,
            "expected": expected,
        }
    return results


# =============================================================================
# quote helpers
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


def _extract_amounts(q: dict[str, Any]) -> tuple[int | None, int | None]:
    in_amt = q.get("inAmount") or q.get("inputAmount") or q.get("in_amount") or q.get("inAmountRaw")
    out_amt = (
        q.get("outAmount") or q.get("outputAmount") or q.get("out_amount") or q.get("outAmountRaw")
    )
    i = _to_int(in_amt, default=0) if in_amt is not None else 0
    o = _to_int(out_amt, default=0) if out_amt is not None else 0
    return (i if i > 0 else None), (o if o > 0 else None)


def _route_count(q: dict[str, Any]) -> int | None:
    rp = q.get("routePlan") or q.get("route_plan")
    if isinstance(rp, list):
        return len(rp)
    data = q.get("data")
    if isinstance(data, list):
        return len(data)
    return None


def _brief_quote(q: dict[str, Any]) -> dict[str, Any]:
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


def _resolve_quote_urls(base_url: str, as_legacy: bool, quote_path_override: str) -> list[str]:
    b = (base_url or "").strip().rstrip("/")
    if not b:
        b = "https://api.jup.ag"

    if quote_path_override and quote_path_override.strip().startswith("/"):
        return [b + quote_path_override.strip()]

    if as_legacy:
        return [b + "/swap/v1/quote", b + "/quote", b + "/v6/quote"]
    return [b + "/v6/quote", b + "/quote", b + "/swap/v1/quote"]


def _jup_headers() -> dict[str, str]:
    pro = _envs("JUP_PRO_KEY", "")
    api = _envs("JUP_API_KEY", "") or _envs("JUP_KEY", "")
    h: dict[str, str] = {"user-agent": "pnlsniper-smoke"}
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
    if "getaddrinfo" in m or "could not resolve" in m or "name or service not known" in m:
        return "dns"
    return "other"


async def _http_get_json(
    url: str, params: dict[str, Any], headers: dict[str, str], timeout_s: float
) -> tuple[int, Any, str]:
    """
    Prefer httpx; fallback to requests in a thread.
    """
    with contextlib.suppress(Exception):
        import httpx  # type: ignore

        async with httpx.AsyncClient(timeout=timeout_s) as c:
            r = await c.get(url, params=params, headers=headers)
            text = r.text
            try:
                return int(r.status_code), r.json(), text
            except Exception:
                return int(r.status_code), {"_non_json": True, "text": text[:2000]}, text

    import requests  # type: ignore

    def _do() -> tuple[int, Any, str]:
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
) -> dict[str, Any]:
    urls = _resolve_quote_urls(base_url, as_legacy, quote_path_override)
    headers = _jup_headers()

    params: dict[str, Any] = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": int(amount_raw),
        "slippageBps": int(slippage_bps),
    }
    if only_direct:
        params["onlyDirectRoutes"] = "true"

    backoff_s = 0.35
    last_err = "no_attempts"
    tried: list[str] = []

    for u in urls:
        tried.append(u)
        for attempt in range(1, 4):
            t0 = _now_ms()
            try:
                if debug:
                    _hlog(
                        f"HTTP GET {u} attempt={attempt} params={sorted(params.keys())}",
                        json_only=json_only,
                    )

                status, js, raw = await _http_get_json(
                    u, dict(params), headers, timeout_s=timeout_s
                )
                dt = _now_ms() - t0

                if status >= 400:
                    # some gateways reject onlyDirectRoutes; retry without it once
                    if status in (400, 422) and ("onlyDirectRoutes" in params):
                        params2 = dict(params)
                        params2.pop("onlyDirectRoutes", None)
                        status2, js2, raw2 = await _http_get_json(
                            u, params2, headers, timeout_s=timeout_s
                        )
                        dt2 = _now_ms() - t0
                        if status2 < 400 and isinstance(js2, dict):
                            js2["_meta"] = {
                                "url": u,
                                "ms": dt2,
                                "status": status2,
                                "used": "direct_http",
                                "via": "direct_http",
                            }
                            return js2
                        last_err = f"HTTP {status2} @ {u}: {_diagnose_error(js2, raw2)}"
                    else:
                        last_err = f"HTTP {status} @ {u}: {_diagnose_error(js, raw)}"

                    kind = _classify_http_error(last_err)
                    if kind in {"rate_limit", "timeout"} or status >= 500:
                        await asyncio.sleep(min(1.8, backoff_s))
                        backoff_s *= 1.7
                        continue
                    break

                if isinstance(js, dict):
                    js["_meta"] = {
                        "url": u,
                        "ms": dt,
                        "status": status,
                        "used": "direct_http",
                        "via": "direct_http",
                    }
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


async def _maybe_await(x: Any) -> Any:
    return await x if asyncio.iscoroutine(x) or asyncio.isfuture(x) else x


def _filter_kwargs(fn: Any, kwargs: dict[str, Any]) -> dict[str, Any]:
    try:
        sig = inspect.signature(fn)
        params = sig.parameters.values()
        if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params):
            return kwargs
        allowed = {p.name for p in params}
        return {k: v for k, v in kwargs.items() if k in allowed}
    except Exception:
        return kwargs


def _normalize_quote_return(res: Any, *, used: str, via: str) -> dict[str, Any] | None:
    if isinstance(res, dict):
        out = dict(res)
        meta = dict(out.get("_meta") or {})
        meta.setdefault("used", used)
        meta.setdefault("via", via)
        out["_meta"] = meta
        return out
    if isinstance(res, tuple | list) and res:
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
) -> tuple[bool, dict[str, Any] | None, str]:
    try:
        L = importlib.import_module("utils_pkg.lightning_impl")
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

    superset: dict[str, Any] = {
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
        # timeouts
        "timeout_s": float(timeout_s),
        "timeout": float(timeout_s),
    }

    variants: list[tuple[tuple[Any, ...], dict[str, Any]]] = [
        ((), dict(superset)),
        (
            (input_mint, output_mint, int(amount_raw)),
            {"slippageBps": int(slippage_bps), "onlyDirectRoutes": bool(only_direct)},
        ),
        (
            (input_mint, output_mint, int(amount_raw), int(slippage_bps)),
            {"onlyDirectRoutes": bool(only_direct)},
        ),
    ]

    last_err = "unknown"
    for args, kwargs in variants:
        try:
            kw = _filter_kwargs(fn, kwargs)
            if debug:
                _hlog(
                    f"LIGHTNING CALL {fn_name} args={len(args)} kwargs={sorted(list(kw.keys()))}",
                    json_only=json_only,
                )
            res = await asyncio.wait_for(
                _maybe_await(fn(*args, **kw)), timeout=max(2.0, float(timeout_s))
            )
            out = _normalize_quote_return(res, used="lightning", via=f"lightning_impl.{fn_name}")
            if out is not None:
                return True, out, fn_name
            last_err = f"unexpected_return:{type(res).__name__}"
        except TimeoutError:
            last_err = "timeout"
            continue
        except Exception as e:
            last_err = _short_exc(e)
            continue

    return False, None, f"{fn_name}:{last_err}"


async def _try_router_quote(
    *,
    input_mint: str,
    output_mint: str,
    amount_raw: int,
    slippage_bps: int,
    only_direct: bool,
    timeout_s: float,
) -> dict[str, Any] | None:
    with contextlib.suppress(Exception):
        router = importlib.import_module("utils_pkg.router")

        fns = []
        for nm in ("get_quote", "quote", "get_buy_quote", "get_sell_quote", "get_jup_quote"):
            f = getattr(router, nm, None)
            if callable(f):
                fns.append((nm, f))
        if not fns:
            return None

        nm, fn = fns[0]
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
async def amain(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Lightning/Jupiter quote smoke test (QUOTE-ONLY)")

    ap.add_argument(
        "--amount-sol",
        type=float,
        default=float(_envf("SMOKE_AMOUNT_SOL", 0.005)),
        help="SOL input amount",
    )
    ap.add_argument(
        "--out",
        type=str,
        default=_envs("SMOKE_OUT", "USDC"),
        help="Output mint/symbol (normalized)",
    )
    ap.add_argument(
        "--slippage-bps",
        type=int,
        default=int(_envi("SMOKE_SLIPPAGE_BPS", 50)),
        help="Slippage bps",
    )

    ap.add_argument(
        "--only-direct",
        type=int,
        default=1 if _envb("SMOKE_ONLY_DIRECT", False) else 0,
        help="1 to force direct routes",
    )
    ap.add_argument(
        "--roundtrip",
        type=int,
        default=1 if _envb("SMOKE_ROUNDTRIP", False) else 0,
        help="1 to quote OUT->SOL too",
    )
    ap.add_argument(
        "--require-roundtrip",
        type=int,
        default=1 if _envb("SMOKE_REQUIRE_ROUNDTRIP", False) else 0,
        help="1 to fail if roundtrip fails",
    )

    ap.add_argument(
        "--require-lightning",
        type=int,
        default=1 if _envb("SMOKE_REQUIRE_LIGHTNING", False) else 0,
        help="1 to fail if Lightning isn't used",
    )
    ap.add_argument(
        "--allow-http-fallback",
        type=int,
        default=1 if _envb("SMOKE_ALLOW_HTTP_FALLBACK", True) else 0,
        help="1 to allow fallback to direct HTTP",
    )
    ap.add_argument(
        "--no-fallback", type=int, default=0, help="Alias: if 1, forces allow-http-fallback=0"
    )

    ap.add_argument(
        "--base",
        type=str,
        default=_envs("SMOKE_BASE_URL", ""),
        help="Override base URL for direct HTTP fallback",
    )
    ap.add_argument(
        "--use-alt",
        type=int,
        default=1 if _envb("SMOKE_USE_ALT", False) else 0,
        help="1 to pick from JUP_ALT_BASE_URLS (SMOKE_SEED makes deterministic)",
    )

    ap.add_argument(
        "--timeout",
        type=float,
        default=float(_envf("SMOKE_TIMEOUT_S", 15.0)),
        help="Per-call timeout seconds",
    )
    ap.add_argument(
        "--total-timeout",
        type=float,
        default=float(_envf("SMOKE_TOTAL_TIMEOUT_S", 0.0)),
        help="Total cap seconds (0 disables)",
    )

    ap.add_argument(
        "--json",
        action="store_true",
        default=_envb("SMOKE_JSON", False),
        help="Emit JSON summary to STDOUT",
    )
    ap.add_argument(
        "--json-only",
        action="store_true",
        default=_envb("SMOKE_JSON_ONLY", False),
        help="Only JSON output (no human logs)",
    )
    ap.add_argument(
        "--debug",
        action="store_true",
        default=_envb("SMOKE_DEBUG", False),
        help="Verbose trace (sanitized)",
    )

    ap.add_argument(
        "--probe-mints",
        type=int,
        default=1 if _envb("SMOKE_PROBE_MINTS", False) else 0,
        help="1 to run local mint normalization probes",
    )
    ap.add_argument(
        "--use-router",
        type=int,
        default=1 if _envb("SMOKE_USE_ROUTER", False) else 0,
        help="1 to also quote via utils_pkg.router and compare (best-effort)",
    )

    args = ap.parse_args(argv if argv is not None else None)

    json_only = bool(args.json_only)
    emit_json = bool(args.json or args.json_only)
    debug = bool(args.debug)

    # Safety: discourage ANY trade path even if downstream code is sloppy.
    os.environ.setdefault("SMOKE", "1")
    os.environ.setdefault("SMOKE_LIGHTNING", "1")
    os.environ.setdefault("DRYRUN", os.getenv("DRYRUN", "1"))

    _bootstrap_repo_root()
    _maybe_load_dotenv()

    timeout_s = max(5.0, float(args.timeout))
    total_timeout_s = max(0.0, float(args.total_timeout))

    base_env = _envs("JUP_BASE_URL", "https://api.jup.ag")
    alt_env = _split_csv(_envs("JUP_ALT_BASE_URLS", ""))
    as_legacy = _envb("JUP_AS_LEGACY", False)
    only_direct = bool(int(args.only_direct)) or _envb("JUP_ONLY_DIRECT", False)
    quote_path_override = _envs("JUP_QUOTE_PATH", "").strip()

    chosen_base = (args.base or base_env).strip() or base_env
    if int(args.use_alt) == 1 and alt_env:
        seed = _envs("SMOKE_SEED", "")
        if seed:
            with contextlib.suppress(Exception):
                random.seed(int(seed))
        chosen_base = random.choice(alt_env)

    sol_mint, usdc_mint = _canon_mints()
    out_norm = _normalize_mint(args.out, sol_mint=sol_mint, usdc_mint=usdc_mint)

    if not out_norm:
        summary = {
            "ts": _ts(),
            "ok": False,
            "step": "smoke_lightning",
            "error": f"normalize_failed:{_sanitize(args.out)}",
        }
        if emit_json:
            print(json.dumps(summary, ensure_ascii=False, default=str))
        _hlog(f"FAIL: could not normalize --out={args.out!r}", json_only=json_only)
        return RC_FAIL

    amount_sol = max(0.0, float(args.amount_sol))
    lamports = _sol_to_lamports(amount_sol)
    if lamports <= 0:
        summary = {
            "ts": _ts(),
            "ok": False,
            "step": "smoke_lightning",
            "error": "amount_sol_must_be_positive",
        }
        if emit_json:
            print(json.dumps(summary, ensure_ascii=False, default=str))
        _hlog("FAIL: amount-sol must be > 0", json_only=json_only)
        return RC_FAIL

    slippage_bps = int(max(1, min(5000, int(args.slippage_bps))))
    require_lightning = bool(int(args.require_lightning))
    require_roundtrip = bool(int(args.require_roundtrip))
    allow_http_fallback = bool(int(args.allow_http_fallback))
    if int(args.no_fallback) == 1:
        allow_http_fallback = False
    do_roundtrip = bool(int(args.roundtrip) or require_roundtrip)

    t0 = _now_ms()
    pro_key = _envs("JUP_PRO_KEY", "")
    api_key = _envs("JUP_API_KEY", "") or _envs("JUP_KEY", "")

    summary: dict[str, Any] = {
        "ts": _ts(),
        "ok": False,
        "step": "smoke_lightning",
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
            "python": _sanitize(sys.version.split()[0]),
            "platform": _sanitize(platform.platform()),
        },
        "mint_probe": None,
        "buy": None,
        "roundtrip": None,
        "router_check": None,
        "errors": [],
        "warnings": [],
        "fix_hints": [],
        "elapsed_ms": 0,
    }

    if int(args.probe_mints) == 1:
        summary["mint_probe"] = _probe_mint_normalization(sol_mint, usdc_mint)

    if not json_only:
        _hlog("────────────────────────────────────────────────────────────", json_only=False)
        _hlog(" V3+++ smoke_lightning — Lightning/Jupiter QUOTE-ONLY sanity  ", json_only=False)
        _hlog("────────────────────────────────────────────────────────────", json_only=False)
        _hlog(
            _sanitize(
                json.dumps(
                    {
                        "only_direct": bool(only_direct),
                        "roundtrip": bool(do_roundtrip),
                        "require_lightning": bool(require_lightning),
                        "allow_http_fallback": bool(allow_http_fallback),
                        "as_legacy": bool(as_legacy),
                        "JUP_QUOTE_PATH": quote_path_override or "(default)",
                        "has_JUP_PRO_KEY": bool(pro_key),
                        "has_JUP_API_KEY": bool(api_key),
                        "JUP_PRO_KEY": _mask(pro_key),
                        "JUP_API_KEY": _mask(api_key),
                        "base_url": chosen_base,
                    },
                    ensure_ascii=False,
                )
            ),
            json_only=False,
        )
        _hlog(f"mints in=SOL({sol_mint}) out={out_norm} (from {args.out!r})", json_only=False)
        _hlog(
            f"amount_sol={amount_sol:.9f} lamports={lamports} slippage_bps={slippage_bps}",
            json_only=False,
        )

    async def _run_once() -> int:
        # 1) Lightning-first BUY quote
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
            # REQUIRED phrase for preflight policy enforcement
            _hlog(
                "NOTE: falling back to direct http (lightning unavailable or failed)",
                json_only=json_only,
            )
            summary["fell_back_to_http"] = True

            if not allow_http_fallback:
                summary["errors"].append(f"lightning_failed:{_sanitize(via)}")
                summary["errors"].append("http_fallback_disabled")
                return RC_FAIL

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
                return RC_FAIL

        # Enforce Lightning policy
        if require_lightning and not summary["used_lightning"]:
            summary["errors"].append("require_lightning=1 but lightning path was not used")
            return RC_FAIL

        # Validate buy quote amounts
        if not isinstance(q1, dict):
            summary["errors"].append("internal_error:no_quote_payload")
            return RC_FAIL

        in_amt, out_amt = _extract_amounts(q1)
        if not out_amt or out_amt <= 0:
            summary["errors"].append("quote_missing_outAmount")
            return RC_FAIL
        if in_amt and int(in_amt) != int(lamports):
            summary["warnings"].append(f"inAmount_mismatch got={in_amt} expected={lamports}")

        # Optional router cross-check (never blocks success)
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
                _, o_r = _extract_amounts(q_router)
                if o_r and out_amt and out_amt > 0:
                    delta = abs(int(o_r) - int(out_amt)) / float(out_amt)
                    if delta > 0.25:
                        summary["warnings"].append(
                            f"router_quote_deviation_gt_25pct delta={delta:.2f}"
                        )

        # 2) Optional roundtrip OUT -> SOL
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
                _hlog(
                    f"OK: lightning roundtrip via={via2} :: {summary['roundtrip']}",
                    json_only=json_only,
                )
            else:
                _hlog(
                    "NOTE: falling back to direct http for roundtrip (lightning unavailable or failed)",
                    json_only=json_only,
                )
                summary["fell_back_to_http"] = True

                if not allow_http_fallback:
                    msg = f"roundtrip_lightning_failed:{_sanitize(via2)}"
                    if require_roundtrip:
                        summary["errors"].append(msg)
                        return RC_FAIL
                    summary["warnings"].append(msg)
                else:
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
                        _hlog(
                            f"OK: direct http roundtrip :: {summary['roundtrip']}",
                            json_only=json_only,
                        )
                    except Exception as e:
                        msg = _sanitize(f"roundtrip_failed:{type(e).__name__}:{e}")
                        if require_roundtrip:
                            summary["errors"].append(msg)
                            return RC_FAIL
                        summary["warnings"].append(msg)

        summary["ok"] = True
        return RC_OK

    try:
        if total_timeout_s > 0:
            await asyncio.wait_for(_run_once(), timeout=float(total_timeout_s))
        else:
            await _run_once()
    except TimeoutError:
        summary["errors"].append("total_timeout")
    finally:
        summary["elapsed_ms"] = _now_ms() - t0

        # fix hints (lightweight)
        msg_all = " ".join(
            [str(x) for x in summary["errors"]] + [str(x) for x in summary["warnings"]]
        ).lower()
        hints: list[str] = []
        kind = _classify_http_error(msg_all)
        if kind == "rate_limit":
            hints.append(
                "Rate limiting detected. Add/verify Jupiter API key, reduce quote frequency, or rotate base URLs."
            )
        if kind == "auth":
            hints.append(
                "Auth failure detected. Check JUP_PRO_KEY/JUP_API_KEY and whether your endpoint requires a key."
            )
        if kind == "timeout":
            hints.append(
                "Timeout detected. Increase SMOKE_TIMEOUT_S / SMOKE_TOTAL_TIMEOUT_S or use a faster endpoint."
            )
        if kind == "dns":
            hints.append("DNS/network issue detected. Verify outbound DNS or switch endpoints.")
        if "require_lightning" in msg_all:
            hints.append(
                "Lightning was required but not used. Ensure utils_pkg/lightning_impl.py is present and healthy."
            )
        if "normalize_failed" in msg_all:
            hints.append("Mint normalization failed. Use USDC/SOL or a base58 mint address.")

        # de-dupe
        dedup: list[str] = []
        seen = set()
        for h in hints:
            if h and h not in seen:
                dedup.append(h)
                seen.add(h)
        summary["fix_hints"] = dedup[:10]

        if emit_json:
            print(json.dumps(summary, ensure_ascii=False, default=str))

        if not emit_json:
            _hlog(
                ("OK" if summary.get("ok") else "FAIL")
                + f" used_lightning={summary.get('used_lightning')}"
                + f" fell_back_to_http={summary.get('fell_back_to_http')}"
                + f" elapsed_ms={summary.get('elapsed_ms')}",
                json_only=json_only,
            )

    return RC_OK if bool(summary.get("ok")) else RC_FAIL


def main(argv: list[str] | None = None) -> None:
    try:
        rc = asyncio.run(amain(argv))
    except KeyboardInterrupt:
        rc = RC_CTRL_C
    except Exception:
        err = _sanitize(traceback.format_exc())
        if _envb("SMOKE_JSON", False) or _envb("SMOKE_JSON_ONLY", False):
            print(
                json.dumps(
                    {"ts": _ts(), "ok": False, "step": "smoke_lightning", "error": err[:900]},
                    ensure_ascii=False,
                )
            )
        else:
            _safe_write(sys.stderr, f"[smoke_lightning] {_ts()} FAIL unhandled\n{err[:1800]}\n")
        rc = RC_FAIL
    raise SystemExit(int(rc))


if __name__ == "__main__":
    main()
