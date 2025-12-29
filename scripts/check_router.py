# -*- coding: utf-8 -*-
from __future__ import annotations

"""
scripts/check_router.py — God-Tier Router Precheck (LOCK-IN FINAL)
==================================================================

Mission
-------
Validate router wiring for QUOTE-ONLY viability:
  1) Import + repo bootstrap
  2) Router target resolution (get_router / Router() / module)
  3) Decimals (SOL + token) via:
        target -> module -> utils_pkg.solana_token fallback
  4) BUY quote  (SOL -> TOKEN) [REQUIRED]
  5) SELL quote (TOKEN -> SOL) [STRICT optional]
  6) Spot price (best-effort; require with --require-price)

Critical fixes (based on your logs)
-----------------------------------
- If JUP key is missing, lightning_impl may throw 401. For precheck we
  auto-set ROUTER_STRICT_AUTH=0 (ONLY when no key is present) so router
  can fall back to public quote endpoints.
- Decimals lookup no longer depends on target object having get_decimals().
  We try target, then router module, then solana_token.

Safety
------
- No swaps / tx building / signing / sending.
- Sync callables run in a thread so timeouts work.
- Secrets never printed (env values redacted, long tokens masked).
- JSON summary to STDOUT; human logs to STDERR.

Exit codes
----------
0 success
2 failure (missing required BUY quote / strict unmet / require-price unmet / timeout)
130 ctrl-c
"""

import argparse
import asyncio
import contextlib
import inspect
import json
import os
import re
import sys
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple


# =============================================================================
# repo-root bootstrap (TOP OF FILE)
# =============================================================================
def _bootstrap_repo_root() -> str:
    try:
        here = Path(__file__).resolve()
    except Exception:
        return ""

    candidates = [here.parent] + list(here.parents)
    markers = ("pyproject.toml", "requirements.txt", "setup.cfg", ".git")
    root: Optional[Path] = None

    for p in candidates:
        try:
            if any((p / m).exists() for m in markers) or (p / "src").is_dir():
                root = p
                break
        except Exception:
            continue

    if root is None:
        root = here.parents[1] if len(here.parents) > 1 else here.parent

    root_str = str(root)
    if root_str and root_str not in sys.path:
        sys.path.insert(0, root_str)

    os.environ.setdefault("REPO_ROOT", root_str)
    return root_str


_REPO_ROOT = _bootstrap_repo_root()


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


def _envf(k: str, d: float) -> float:
    try:
        return float(_envs(k, str(d)))
    except Exception:
        return float(d)


def _envi(k: str, d: int) -> int:
    try:
        return int(float(_envs(k, str(d))))
    except Exception:
        return int(d)


def _ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _now_ms() -> int:
    return int(time.time() * 1000)


# =============================================================================
# IO safety
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
    _safe_print(f"[check_router] {_ts()} {msg}", file=sys.stderr)


# =============================================================================
# Redaction / safety
# =============================================================================
_URL_CRED_RE = re.compile(r"(://)([^/@\s]+)@")
_APIKEY_HDR_RE = re.compile(r"(?i)(x-api-key:\s*)([^\s]+)")
_BEARER_HDR_RE = re.compile(r"(?i)(authorization:\s*bearer\s+)([^\s]+)")
_LONG_TOKENISH_RE = re.compile(r"\b[A-Za-z0-9_\-]{28,}\b")

_SENSITIVE_ENV_KEYS = [
    "JUP_PRO_KEY",
    "JUP_API_KEY",
    "OPENAI_API_KEY",
    "TELEGRAM_BOT_TOKEN",
    "DISCORD_TOKEN",
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
    "REDIS_URL",
    "RPC_URL",
    "SOL_RPC_URL",
]


def _mask_long_tokens_enabled() -> bool:
    return _envb("PREFLIGHT_MASK_LONG_TOKENS", True)


def _sanitize_text(x: Any) -> str:
    if x is None:
        return ""
    s = str(x)
    s = _URL_CRED_RE.sub(r"\1***@", s)
    s = _APIKEY_HDR_RE.sub(r"\1***", s)
    s = _BEARER_HDR_RE.sub(r"\1***", s)

    for k in _SENSITIVE_ENV_KEYS:
        v = os.getenv(k, "")
        if v and len(v) >= 6:
            s = s.replace(v, "***")

    if _mask_long_tokens_enabled():
        s = _LONG_TOKENISH_RE.sub("***", s)
    return s


def _short_exc(e: BaseException, n: int = 360) -> str:
    s = _sanitize_text(f"{type(e).__name__}: {e}")
    return (s[:n] + "…") if len(s) > n else s


def _safe_jsonable(x: Any) -> Any:
    try:
        json.dumps(x, default=str)
        return x
    except Exception:
        return str(x)


# =============================================================================
# Strict-auth relaxation for quote-only precheck
# =============================================================================
def _looks_like_placeholder_key(v: str) -> bool:
    s = (v or "").strip()
    if not s:
        return True
    low = s.lower()
    if low in {"$jup_api_key", "$jup_pro_key", "your-api-key", "your_key", "my key", "changeme"}:
        return True
    if "$" in s and "key" in low:
        return True
    return False


def _maybe_relax_router_auth_for_precheck(*, json_only: bool) -> None:
    """
    If no Jupiter key exists, lightning_impl quoting may 401.
    For *precheck only*, disable strict auth so router can fall back to public quote.
    """
    k = os.getenv("JUP_API_KEY") or os.getenv("JUP_PRO_KEY") or ""
    if _looks_like_placeholder_key(k):
        if os.getenv("ROUTER_STRICT_AUTH") is None:
            os.environ["ROUTER_STRICT_AUTH"] = "0"
            _hlog("Auto-set ROUTER_STRICT_AUTH=0 (no JUP key detected) for precheck fallback", json_only=json_only)


# =============================================================================
# small utils
# =============================================================================
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


def _to_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None or isinstance(x, bool):
            return float(default)
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if not s:
            return float(default)
        return float(s)
    except Exception:
        return float(default)


def _raw_to_ui(raw: int, decimals: Optional[int]) -> Optional[float]:
    try:
        if raw <= 0:
            return None
        d = int(decimals or 0)
        if d <= 0 or d > 18:
            return None
        return float(raw) / float(10**d)
    except Exception:
        return None


def _sol_to_lamports(amount_sol: float, sol_decimals: int = 9) -> int:
    d = int(sol_decimals if sol_decimals > 0 else 9)
    return int(round(float(amount_sol) * float(10**d)))


def _pick(d: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    for k in keys:
        if k in d and d.get(k) is not None:
            return d.get(k)
    return default


# =============================================================================
# Signature drift tolerant calling (SYNC safe)
# =============================================================================
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


async def _invoke(fn: Any, *args: Any, **kwargs: Any) -> Any:
    if inspect.iscoroutinefunction(fn):
        return await fn(*args, **kwargs)
    res = await asyncio.to_thread(fn, *args, **kwargs)
    if inspect.isawaitable(res):
        return await res
    return res


@dataclass
class CallHit:
    ok: bool
    used: str
    err: str
    val: Any


async def _try_call(
    target: Any,
    fn_names: Tuple[str, ...],
    variants: Sequence[Tuple[Tuple[Any, ...], Dict[str, Any]]],
    timeout_s: float,
    *,
    debug: bool,
    json_only: bool,
) -> CallHit:
    last_err = "no_callable_found"
    for nm in fn_names:
        fn = getattr(target, nm, None)
        if not callable(fn):
            continue
        for args, kwargs in variants:
            try:
                kw = _filter_kwargs(fn, dict(kwargs))
                if debug:
                    _hlog(f"CALL {nm} args={len(args)} kwargs={sorted(list(kw.keys()))}", json_only=json_only)
                val = await asyncio.wait_for(_invoke(fn, *args, **kw), timeout=max(1.0, float(timeout_s)))
                return CallHit(True, nm, "", val)
            except TypeError as e:
                last_err = _sanitize_text(f"{nm}: {e}")
                continue
            except asyncio.TimeoutError:
                last_err = f"{nm}: timeout"
                continue
            except Exception as e:
                last_err = _sanitize_text(f"{nm}: {_short_exc(e)}")
                continue
    return CallHit(False, "", last_err, None)


# =============================================================================
# Quote normalization / parsing
# =============================================================================
def _find_dict_quote(obj: Any) -> Optional[Dict[str, Any]]:
    if obj is None:
        return None
    if isinstance(obj, dict):
        data = obj.get("data")
        if isinstance(data, list) and data and isinstance(data[0], dict):
            return dict(data[0])
        return dict(obj)
    if isinstance(obj, list) and obj:
        if isinstance(obj[0], dict):
            return dict(obj[0])
        return _find_dict_quote(obj[0])
    if isinstance(obj, tuple) and obj:
        # (ok, quote, why) or (sig, meta)
        for item in obj:
            q = _find_dict_quote(item)
            if q:
                return q
    return None


def _parse_out_amount_raw(q: Dict[str, Any]) -> int:
    return _to_int(_pick(q, "out_amount_raw", "outAmountRaw", "outAmount", "out_amount", "outputAmount", default=0), default=0)


def _parse_in_amount_raw(q: Dict[str, Any]) -> int:
    return _to_int(_pick(q, "in_amount_raw", "inAmountRaw", "inAmount", "in_amount", "inputAmount", default=0), default=0)


def _quote_summary(q: Dict[str, Any]) -> Dict[str, Any]:
    keys = sorted(list(q.keys()))
    routeish = any(k in q for k in ("routePlan", "route_plan", "routes", "data"))
    return {
        "keys": keys[:80],
        "has_route": int(routeish),
        "in_amount_raw": (_parse_in_amount_raw(q) or None),
        "out_amount_raw": (_parse_out_amount_raw(q) or None),
        "priceImpactPct": q.get("priceImpactPct", None),
    }


# =============================================================================
# Mint resolution
# =============================================================================
def _normalize_mint_best_effort(m: str) -> str:
    s = (m or "").strip()
    if not s:
        return s
    with contextlib.suppress(Exception):
        from utils_pkg.mints import try_normalize_mint  # type: ignore
        out = try_normalize_mint(s)
        if isinstance(out, str) and out.strip():
            return out.strip()
    with contextlib.suppress(Exception):
        from utils_pkg.mints import normalize_mint  # type: ignore
        out2 = normalize_mint(s)
        if isinstance(out2, str) and out2.strip():
            return out2.strip()
    return s


def _canonical_mints() -> Tuple[str, str]:
    sol = "So11111111111111111111111111111111111111112"
    usdc = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    with contextlib.suppress(Exception):
        from utils_pkg.mints import SOL_MINT, USDC_MINT  # type: ignore
        sol = str(SOL_MINT)
        usdc = str(USDC_MINT)
    return _normalize_mint_best_effort(sol), _normalize_mint_best_effort(usdc)


# =============================================================================
# Router target resolution + lifecycle
# =============================================================================
async def _resolve_router_target(router_mod: Any, timeout_s: float, *, debug: bool, json_only: bool) -> Tuple[Any, str]:
    hit = await _try_call(
        router_mod,
        ("get_router", "router", "get", "get_instance"),
        variants=[((), {}), ((), {"timeout_s": timeout_s}), ((), {"timeout": timeout_s})],
        timeout_s=timeout_s,
        debug=debug,
        json_only=json_only,
    )
    if hit.ok and hit.val is not None:
        return hit.val, f"{hit.used}()"

    RouterCls = getattr(router_mod, "Router", None)
    if callable(RouterCls):
        try:
            obj = await asyncio.wait_for(asyncio.to_thread(RouterCls), timeout=max(1.0, float(timeout_s)))
            return obj, "Router()"
        except Exception as e:
            if debug:
                _hlog(f"Router() init failed: {_short_exc(e)}", json_only=json_only)

    return router_mod, "module"


async def _bootstrap_router(target: Any, timeout_s: float, *, debug: bool, json_only: bool) -> str:
    hit = await _try_call(
        target,
        ("start", "bootstrap", "init", "setup", "warmup"),
        variants=[((), {}), ((), {"timeout_s": timeout_s}), ((), {"timeout": timeout_s})],
        timeout_s=timeout_s,
        debug=debug,
        json_only=json_only,
    )
    return hit.used if hit.ok else f"router_bootstrap_skipped({hit.err})"


async def _shutdown_router(target: Any, timeout_s: float, *, debug: bool, json_only: bool) -> str:
    hit = await _try_call(
        target,
        ("shutdown", "close", "stop", "teardown"),
        variants=[((), {}), ((), {"timeout_s": timeout_s}), ((), {"timeout": timeout_s})],
        timeout_s=timeout_s,
        debug=debug,
        json_only=json_only,
    )
    return hit.used if hit.ok else f"router_shutdown_skipped({hit.err})"


# =============================================================================
# Decimals: target -> module -> solana_token fallback
# =============================================================================
async def _get_decimals_any(
    target: Any,
    router_mod: Any,
    mint: str,
    timeout_s: float,
    *,
    debug: bool,
    json_only: bool,
) -> Tuple[Optional[int], str]:
    # Try target methods first
    hit = await _try_call(
        target,
        ("get_decimals", "mint_decimals", "decimals", "fetch_decimals"),
        variants=[((mint,), {}), ((), {"mint": mint})],
        timeout_s=timeout_s,
        debug=debug,
        json_only=json_only,
    )
    if hit.ok:
        d = _to_int(hit.val, default=0)
        if 0 < d <= 18:
            return d, f"target.{hit.used}"

    # Try module-level next
    hit2 = await _try_call(
        router_mod,
        ("get_decimals", "mint_decimals", "decimals", "fetch_decimals"),
        variants=[((mint,), {}), ((), {"mint": mint})],
        timeout_s=timeout_s,
        debug=debug,
        json_only=json_only,
    )
    if hit2.ok:
        d = _to_int(hit2.val, default=0)
        if 0 < d <= 18:
            return d, f"module.{hit2.used}"

    # Final fallback: solana_token (canonical)
    with contextlib.suppress(Exception):
        from utils_pkg import solana_token as st  # type: ignore
        d3 = await asyncio.wait_for(st.get_token_decimals(mint, default=None), timeout=max(1.0, float(timeout_s)))
        if d3 is not None:
            d3i = int(d3)
            if 0 < d3i <= 18:
                return d3i, "solana_token.get_token_decimals"

    return None, hit.err if hit.err != "no_callable_found" else hit2.err


# =============================================================================
# Quote getters (drift tolerant, amount variants)
# =============================================================================
async def _get_buy_quote_any(
    target: Any,
    router_mod: Any,
    *,
    out_mint: str,
    amount_sol: float,
    slippage_bps: int,
    sol_mint: str,
    sol_decimals: int,
    timeout_s: float,
    debug: bool,
    json_only: bool,
) -> Tuple[Optional[Dict[str, Any]], str]:
    lamports = _sol_to_lamports(amount_sol, sol_decimals=sol_decimals)
    sb = int(slippage_bps)

    # Dedicated buy quote API (target then module)
    variants = [
        ((), {"mint": out_mint, "amount_sol": float(amount_sol), "slippage_bps": sb}),
        ((), {"mint": out_mint, "amount_sol": float(amount_sol), "slippageBps": sb}),
        ((), {"mint": out_mint, "lamports_in": int(lamports), "slippage_bps": sb}),
        ((), {"mint": out_mint, "lamports_in": int(lamports), "slippageBps": sb}),
        ((), {"mint": out_mint, "amount": int(lamports), "slippageBps": sb}),
        ((out_mint, float(amount_sol), sb), {}),
        ((out_mint, int(lamports), sb), {}),
        ((out_mint,), {"amount_sol": float(amount_sol), "slippage_bps": sb}),
        ((out_mint,), {"lamports_in": int(lamports), "slippage_bps": sb}),
    ]

    for scope, obj in (("target", target), ("module", router_mod)):
        hit = await _try_call(
            obj,
            ("get_buy_quote", "buy_quote", "quote_buy"),
            variants=variants,
            timeout_s=timeout_s,
            debug=debug,
            json_only=json_only,
        )
        q = _find_dict_quote(hit.val) if hit.ok else None
        if q:
            return q, f"{scope}.{hit.used}"

    # Generic quote API SOL->token (target then module)
    variants2 = [
        ((), {"inputMint": sol_mint, "outputMint": out_mint, "amount": int(lamports), "slippageBps": sb}),
        ((), {"input_mint": sol_mint, "output_mint": out_mint, "amount": int(lamports), "slippage_bps": sb}),
        ((sol_mint, out_mint, int(lamports)), {"slippageBps": sb}),
        ((sol_mint, out_mint, int(lamports), sb), {}),
    ]
    for scope, obj in (("target", target), ("module", router_mod)):
        hit2 = await _try_call(
            obj,
            ("get_quote", "quote", "get_swap_quote", "swap_quote", "jup_quote", "jupiter_quote"),
            variants=variants2,
            timeout_s=timeout_s,
            debug=debug,
            json_only=json_only,
        )
        q2 = _find_dict_quote(hit2.val) if hit2.ok else None
        if q2:
            return q2, f"{scope}.{hit2.used}"

    return None, "buy_quote_failed_all_variants"


async def _get_sell_quote_any(
    target: Any,
    router_mod: Any,
    *,
    in_mint: str,
    amount_raw: int,
    slippage_bps: int,
    sol_mint: str,
    timeout_s: float,
    debug: bool,
    json_only: bool,
) -> Tuple[Optional[Dict[str, Any]], str]:
    amt = int(max(1, int(amount_raw)))
    sb = int(slippage_bps)

    variants = [
        ((), {"mint": in_mint, "amount_raw": amt, "slippage_bps": sb}),
        ((), {"mint": in_mint, "amount_raw": amt, "slippageBps": sb}),
        ((), {"mint": in_mint, "amount": amt, "slippageBps": sb}),
        ((in_mint, amt, sb), {}),
        ((in_mint,), {"amount_raw": amt, "slippage_bps": sb}),
        ((in_mint,), {"amount": amt, "slippageBps": sb}),
    ]

    for scope, obj in (("target", target), ("module", router_mod)):
        hit = await _try_call(
            obj,
            ("get_sell_quote", "sell_quote", "quote_sell"),
            variants=variants,
            timeout_s=timeout_s,
            debug=debug,
            json_only=json_only,
        )
        q = _find_dict_quote(hit.val) if hit.ok else None
        if q:
            return q, f"{scope}.{hit.used}"

    variants2 = [
        ((), {"inputMint": in_mint, "outputMint": sol_mint, "amount": amt, "slippageBps": sb}),
        ((), {"input_mint": in_mint, "output_mint": sol_mint, "amount": amt, "slippage_bps": sb}),
        ((in_mint, sol_mint, amt), {"slippageBps": sb}),
        ((in_mint, sol_mint, amt, sb), {}),
    ]
    for scope, obj in (("target", target), ("module", router_mod)):
        hit2 = await _try_call(
            obj,
            ("get_quote", "quote", "get_swap_quote", "swap_quote", "jup_quote", "jupiter_quote"),
            variants=variants2,
            timeout_s=timeout_s,
            debug=debug,
            json_only=json_only,
        )
        q2 = _find_dict_quote(hit2.val) if hit2.ok else None
        if q2:
            return q2, f"{scope}.{hit2.used}"

    return None, "sell_quote_failed_all_variants"


async def _get_price_ui_any(
    target: Any,
    router_mod: Any,
    mint: str,
    timeout_s: float,
    *,
    debug: bool,
    json_only: bool,
) -> Tuple[Optional[float], str]:
    for scope, obj in (("target", target), ("module", router_mod)):
        hit = await _try_call(
            obj,
            ("get_price_ui", "price_ui", "get_price", "price", "spot_price", "price_of_ui", "price_of"),
            variants=[((mint,), {}), ((), {"mint": mint})],
            timeout_s=timeout_s,
            debug=debug,
            json_only=json_only,
        )
        if hit.ok:
            f = _to_float(hit.val, default=0.0)
            if f > 0:
                return f, f"{scope}.{hit.used}"
    return None, "price_unavailable"


# =============================================================================
# Hints
# =============================================================================
def _hintify(errors: List[str], warnings: List[str], *, strict: bool, require_price: bool) -> List[str]:
    if not _envb("CHECK_ROUTER_HINTS", True):
        return []
    txt = " ".join([e.lower() for e in errors] + [w.lower() for w in warnings])

    hints: List[str] = []
    if "401" in txt or "403" in txt or "unauthorized" in txt or "forbidden" in txt:
        hints.append("Auth failure detected. Set a valid JUP_PRO_KEY/JUP_API_KEY OR allow public quoting (ROUTER_STRICT_AUTH=0).")
    if "timeout" in txt:
        hints.append("Timeout detected. Increase CHECK_ROUTER_TIMEOUT_S or switch endpoints.")
    if strict and any("sell" in e.lower() for e in errors):
        hints.append("Strict mode requires SELL quote. Disable --strict or ensure router exposes get_sell_quote().")
    if require_price and any("price" in e.lower() for e in errors):
        hints.append("Price required but unavailable. Wire router.get_price_ui() or disable --require-price.")
    if not hints and errors:
        hints.append("Router precheck failed. Run with --debug to see which method names/signatures are mismatched.")
    out: List[str] = []
    seen = set()
    for h in hints:
        if h not in seen:
            seen.add(h)
            out.append(h)
    return out[:10]


# =============================================================================
# CLI
# =============================================================================
def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="God-Tier Router quote-only precheck (LOCK-IN FINAL)")
    ap.add_argument("--mint", default=_envs("CHECK_ROUTER_MINT", "USDC"), help="Token mint/symbol/url/blob (default USDC)")
    ap.add_argument("--amount-sol", type=float, default=float(_envf("CHECK_ROUTER_AMOUNT_SOL", 0.01)), help="SOL amount for buy quote")
    ap.add_argument("--slippage-bps", type=int, default=int(_envi("CHECK_ROUTER_SLIPPAGE_BPS", 200)), help="Slippage bps")
    ap.add_argument("--timeout", type=float, default=float(_envf("CHECK_ROUTER_TIMEOUT_S", 12.0)), help="Per-call timeout seconds")
    ap.add_argument("--total-timeout", type=float, default=float(_envf("CHECK_ROUTER_TOTAL_TIMEOUT_S", 0.0)), help="Total cap seconds (0 disables)")

    ap.add_argument("--strict", action="store_true", help="Require BOTH buy and sell quote")
    ap.add_argument("--require-price", action="store_true", help="Require spot price")
    ap.add_argument("--json", action="store_true", help="Emit JSON summary to STDOUT")
    ap.add_argument("--json-only", action="store_true", help="Only emit JSON (no human logs)")
    ap.add_argument("--debug", action="store_true", default=_envb("CHECK_ROUTER_DEBUG", False), help="Verbose call tracing (sanitized)")
    ap.add_argument("--force", action="store_true", help="Run even if DISABLE_SCANNERS=1.")
    ap.add_argument("--list", action="store_true", help="List discovered router callables and exit")
    ap.add_argument("--print-config", action="store_true", help="Print resolved config JSON and exit")
    return ap.parse_args()


def _list_callables(obj: Any) -> List[str]:
    out: List[str] = []
    for name in dir(obj):
        if name.startswith("_"):
            continue
        try:
            if callable(getattr(obj, name)):
                out.append(name)
        except Exception:
            continue
    return sorted(out)


async def _amain() -> int:
    args = _parse_args()
    env_json = _envb("CHECK_ROUTER_JSON", True)
    env_json_only = _envb("CHECK_ROUTER_JSON_ONLY", False)

    json_only = bool(args.json_only or env_json_only)
    json_mode = bool(args.json or args.json_only or env_json or env_json_only)

    debug = bool(args.debug)
    strict = bool(args.strict or _envb("CHECK_ROUTER_STRICT", False))
    require_price = bool(args.require_price or _envb("CHECK_ROUTER_REQUIRE_PRICE", False))

    timeout_s = max(3.0, float(args.timeout))
    total_timeout_s = max(0.0, float(args.total_timeout))

    amount_sol = max(0.000001, float(args.amount_sol))
    slippage_bps = int(max(1, min(5000, int(args.slippage_bps))))

    disable_scanners = _envb("DISABLE_SCANNERS", False)
    forced = bool(args.force or _envb("CHECK_ROUTER_FORCE", False))
    explicit_intent = forced or strict or require_price or debug or args.json or args.json_only

    if args.print_config:
        cfg = {
            "ts": _ts(),
            "repo_root": _sanitize_text(_REPO_ROOT),
            "CHECK_ROUTER_DISABLED": bool(_envb("CHECK_ROUTER_DISABLED", False)),
            "DISABLE_SCANNERS": bool(disable_scanners),
            "explicit_intent": bool(explicit_intent),
            "json_mode": bool(json_mode),
            "json_only": bool(json_only),
            "debug": bool(debug),
            "strict": bool(strict),
            "require_price": bool(require_price),
            "timeout_s": float(timeout_s),
            "total_timeout_s": float(total_timeout_s),
            "mint": _sanitize_text(args.mint),
            "amount_sol": float(amount_sol),
            "slippage_bps": int(slippage_bps),
        }
        _safe_print(json.dumps(_safe_jsonable(cfg), ensure_ascii=False, default=str), file=sys.stdout)
        return 0

    if _envb("CHECK_ROUTER_DISABLED", False):
        summary = {"ts": _ts(), "ok": True, "skipped": True, "reason": "CHECK_ROUTER_DISABLED=1"}
        if json_mode:
            _safe_print(json.dumps(summary, ensure_ascii=False, default=str), file=sys.stdout)
        _hlog("SKIP (CHECK_ROUTER_DISABLED=1)", json_only=json_only)
        return 0

    if disable_scanners and (not explicit_intent):
        summary = {"ts": _ts(), "ok": True, "skipped": True, "reason": "DISABLE_SCANNERS=1 and no explicit intent"}
        if json_mode:
            _safe_print(json.dumps(summary, ensure_ascii=False, default=str), file=sys.stdout)
        _hlog("SKIP (DISABLE_SCANNERS=1 and no explicit intent)", json_only=json_only)
        return 0

    t0_ms = _now_ms()

    sol_mint, usdc_mint = _canonical_mints()
    mint_input = str(args.mint).strip()
    test_mint = _normalize_mint_best_effort(mint_input)
    if mint_input.upper() == "USDC":
        test_mint = usdc_mint
    if not test_mint:
        test_mint = usdc_mint

    summary: Dict[str, Any] = {
        "ts": _ts(),
        "ok": False,
        "repo_root": _sanitize_text(_REPO_ROOT),
        "strict": int(strict),
        "require_price": int(require_price),
        "mint_input": _sanitize_text(mint_input),
        "mint": test_mint,
        "sol_mint": sol_mint,
        "amount_sol": float(amount_sol),
        "slippage_bps": int(slippage_bps),
        "timeout_s": float(timeout_s),
        "total_timeout_s": float(total_timeout_s),
        "router_target": "",
        "router_bootstrap": "",
        "router_shutdown": "",
        "timings_ms": {},
        "capabilities": {},
        "decimals": {},
        "buy_quote": {},
        "sell_quote": {},
        "price": {},
        "errors": [],
        "warnings": [],
        "fix_hints": [],
        "elapsed_ms": 0,
    }

    # Make precheck resilient to missing JUP key
    _maybe_relax_router_auth_for_precheck(json_only=json_only)

    # Import router module
    try:
        from utils_pkg import router as router_mod  # type: ignore
    except Exception as e:
        summary["errors"].append(f"import_router_failed:{_short_exc(e)}")
        summary["elapsed_ms"] = _now_ms() - t0_ms
        summary["fix_hints"] = _hintify(summary["errors"], summary["warnings"], strict=strict, require_price=require_price)
        if json_mode:
            _safe_print(json.dumps(_safe_jsonable(summary), ensure_ascii=False, default=str), file=sys.stdout)
        else:
            _hlog(f"FAIL import utils_pkg.router err={_short_exc(e)}", json_only=json_only)
        return 2

    # Resolve target
    t_resolve = _now_ms()
    target, target_used = await _resolve_router_target(router_mod, timeout_s=timeout_s, debug=debug, json_only=json_only)
    summary["router_target"] = target_used
    summary["timings_ms"]["resolve_target"] = _now_ms() - t_resolve

    if args.list:
        summary["capabilities"] = {
            "target_used": target_used,
            "callables_target": _list_callables(target),
            "callables_module": _list_callables(router_mod),
        }
        if json_mode:
            _safe_print(json.dumps(_safe_jsonable(summary), ensure_ascii=False, default=str), file=sys.stdout)
        else:
            _hlog("LIST done", json_only=json_only)
        return 0

    # Router bootstrap (best-effort)
    t_boot = _now_ms()
    summary["router_bootstrap"] = await _bootstrap_router(target, timeout_s=timeout_s, debug=debug, json_only=json_only)
    summary["timings_ms"]["router_bootstrap"] = _now_ms() - t_boot

    sol_decimals = 9

    async def _run_body() -> int:
        nonlocal sol_decimals

        # Decimals (SOL + token)
        t_dec = _now_ms()
        d_sol, via_sol = await _get_decimals_any(target, router_mod, sol_mint, timeout_s, debug=debug, json_only=json_only)
        d_tok, via_tok = await _get_decimals_any(target, router_mod, test_mint, timeout_s, debug=debug, json_only=json_only)
        if d_sol:
            sol_decimals = int(d_sol)
        else:
            summary["warnings"].append("sol_decimals_unavailable_assuming_9")
            sol_decimals = 9

        summary["decimals"] = {
            "sol": {"val": d_sol, "via": via_sol},
            "token": {"val": d_tok, "via": via_tok},
        }
        summary["timings_ms"]["decimals"] = _now_ms() - t_dec

        # BUY quote (required)
        t_buy = _now_ms()
        buy_q, buy_via = await _get_buy_quote_any(
            target,
            router_mod,
            out_mint=test_mint,
            amount_sol=amount_sol,
            slippage_bps=slippage_bps,
            sol_mint=sol_mint,
            sol_decimals=sol_decimals,
            timeout_s=timeout_s,
            debug=debug,
            json_only=json_only,
        )
        summary["timings_ms"]["buy_quote"] = _now_ms() - t_buy

        if not buy_q:
            summary["buy_quote"] = {"ok": 0, "via": buy_via}
            summary["errors"].append(f"buy_quote_failed:{buy_via}")
            return 2

        out_raw = _parse_out_amount_raw(buy_q)
        out_ui = _raw_to_ui(out_raw, summary.get("decimals", {}).get("token", {}).get("val"))
        summary["buy_quote"] = {
            "ok": 1,
            "via": buy_via,
            "out_amount_raw": (out_raw if out_raw > 0 else None),
            "out_amount_ui": out_ui,
            **_quote_summary(buy_q),
        }
        if out_raw <= 0:
            summary["warnings"].append("buy_quote_out_amount_missing_or_zero")

        # SELL quote (optional; required if strict)
        t_sell = _now_ms()
        sell_ok = False
        sell_q: Optional[Dict[str, Any]] = None
        sell_via = "skipped"

        override_raw = int(_envi("CHECK_ROUTER_SELL_AMOUNT_RAW", 0))
        sell_amt = 0
        if override_raw > 0:
            sell_amt = override_raw
        elif out_raw and out_raw > 0:
            clamp_max = int(_envi("CHECK_ROUTER_SELL_CLAMP_RAW", 50_000_000))
            clamp_min = int(_envi("CHECK_ROUTER_SELL_MIN_RAW", 10))
            sell_amt = int(max(clamp_min, min(int(out_raw), clamp_max)))

        if sell_amt > 0:
            sell_q, sell_via = await _get_sell_quote_any(
                target,
                router_mod,
                in_mint=test_mint,
                amount_raw=int(sell_amt),
                slippage_bps=slippage_bps,
                sol_mint=sol_mint,
                timeout_s=timeout_s,
                debug=debug,
                json_only=json_only,
            )
            sell_ok = bool(sell_q)

        summary["timings_ms"]["sell_quote"] = _now_ms() - t_sell

        if sell_q:
            sell_out_raw = _parse_out_amount_raw(sell_q)
            sell_out_sol_ui = _raw_to_ui(sell_out_raw, summary.get("decimals", {}).get("sol", {}).get("val") or 9)
            summary["sell_quote"] = {
                "ok": 1,
                "via": sell_via,
                "sell_amount_raw_used": int(sell_amt),
                "out_amount_raw": (sell_out_raw if sell_out_raw > 0 else None),
                "out_amount_sol_ui": sell_out_sol_ui,
                **_quote_summary(sell_q),
            }
        else:
            summary["sell_quote"] = {"ok": 0, "via": sell_via, "sell_amount_raw_used": int(sell_amt), "note": "no_sell_quote"}

        # Price (best-effort)
        t_px = _now_ms()
        px, px_via = await _get_price_ui_any(target, router_mod, test_mint, timeout_s, debug=debug, json_only=json_only)
        summary["timings_ms"]["price"] = _now_ms() - t_px
        summary["price"] = {"via": px_via, "val": px}

        ok = True
        if strict and not sell_ok:
            ok = False
            summary["errors"].append("strict_requires_sell_quote")
        if require_price and (px is None or px <= 0):
            ok = False
            summary["errors"].append("require_price_failed")

        summary["ok"] = bool(ok)
        return 0 if ok else 2

    rc = 2
    try:
        if total_timeout_s > 0:
            rc = await asyncio.wait_for(_run_body(), timeout=float(total_timeout_s))
        else:
            rc = await _run_body()
    except asyncio.TimeoutError:
        summary["ok"] = False
        summary["errors"].append("total_timeout")
        rc = 2
    finally:
        t_shut = _now_ms()
        summary["router_shutdown"] = await _shutdown_router(target, timeout_s=timeout_s, debug=debug, json_only=json_only)
        summary["timings_ms"]["router_shutdown"] = _now_ms() - t_shut

        summary["elapsed_ms"] = _now_ms() - t0_ms
        summary["fix_hints"] = _hintify(summary["errors"], summary["warnings"], strict=strict, require_price=require_price)

        if json_mode:
            _safe_print(json.dumps(_safe_jsonable(summary), ensure_ascii=False, default=str), file=sys.stdout)
        else:
            _hlog(
                ("OK" if summary.get("ok") else "FAIL")
                + f" target={summary.get('router_target')}"
                + f" buy_via={summary.get('buy_quote', {}).get('via')}"
                + f" sell_via={summary.get('sell_quote', {}).get('via')}"
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
        err = _sanitize_text(traceback.format_exc())
        _safe_print(f"[check_router] {_ts()} FAIL unhandled\n{err[:1600]}", file=sys.stderr)
        rc = 2
    raise SystemExit(rc)


if __name__ == "__main__":
    with contextlib.suppress(Exception):
        if os.name == "nt" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]
    main()
