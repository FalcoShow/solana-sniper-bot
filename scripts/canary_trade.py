# -*- coding: utf-8 -*-
from __future__ import annotations

"""
scripts/canary_trade.py — God-Tier Canary Trade (Phase 0–7 LOCK-IN)
===================================================================

Purpose
-------
SAFE, deterministic pipeline canary that validates end-to-end plumbing:

  1) Quote path (always): SOL -> TOKEN and TOKEN -> SOL (quote-only)
  2) Optional build path: quote -> swap tx (base64)  (NO signing / NO sending)
  3) Optional simulate path: swap tx -> simulateTransaction (sigVerify=false)
  4) Optional entry path (DRYRUN by default): invoke maybe_buy() safely
  5) LIVE mode exists but is HARD-LOCKED behind env + CLI confirms

Hard safety rules
-----------------
- DEFAULT MODE IS DRYRUN (no broadcast expected)
- QUOTE/BUILD/SIMULATE NEVER sign or send
- DRYRUN forces DRYRUN=1 in-process BEFORE importing entry modules
- Any entry invocation (even DRYRUN) requires CANARY_TRADE_ENABLED=1
- LIVE requires explicit multi-lock:
      CANARY_TRADE_ENABLED=1
      CANARY_MODE=live
      CANARY_LIVE=1
      CANARY_CONFIRM=YES
      --live-confirm YES

Optional sell-back (LIVE) requires extra locks:
      CANARY_SELL_BACK=1
      CANARY_SELL_CONFIRM=YES
      --sell-back

Exit codes
----------
0   pass
2   fail (broken path / blocked config / required component missing)
130 ctrl-c

Run
---
python -m scripts.canary_trade
python -m scripts.canary_trade --stage quote --json
python -m scripts.canary_trade --stage simulate --mint USDC --amount-sol 0.01 --slippage-bps 60

LIVE (real trades) — requires env locks + explicit CLI confirm
python -m scripts.canary_trade --stage live --live-confirm YES
python -m scripts.canary_trade --stage live --live-confirm YES --sell-back
"""

import argparse
import asyncio
import contextlib
import importlib
import inspect
import json
import os
import platform
import re
import sys
import time
import traceback
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence, Tuple

# Optional local dev .env (never required)
with contextlib.suppress(Exception):
    from dotenv import find_dotenv, load_dotenv  # type: ignore

    load_dotenv(find_dotenv())

# -----------------------------------------------------------------------------
# Human logs -> STDERR, JSON -> STDOUT
# -----------------------------------------------------------------------------
def _ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _hlog(msg: str, *, quiet: bool) -> None:
    if quiet:
        return
    try:
        sys.stderr.write(f"[canary] {_ts()} {msg}\n")
        sys.stderr.flush()
    except Exception:
        pass


# -----------------------------------------------------------------------------
# Env helpers
# -----------------------------------------------------------------------------
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


# -----------------------------------------------------------------------------
# Redaction / safety
# -----------------------------------------------------------------------------
_URL_CRED_RE = re.compile(r"(://)([^/@\s]+)@")
_APIKEY_RE = re.compile(r"(?i)(x-api-key:\s*)([^\s]+)")
_BEARER_RE = re.compile(r"(?i)(authorization:\s*bearer\s+)([^\s]+)")
_LONG_TOKENISH_RE = re.compile(r"\b[A-Za-z0-9_\-]{28,}\b")


def _sanitize_text(text: Any) -> str:
    if text is None:
        return ""
    s = str(text)
    s = _URL_CRED_RE.sub(r"\1***@", s)
    s = _APIKEY_RE.sub(r"\1***", s)
    s = _BEARER_RE.sub(r"\1***", s)

    sensitive_keys = [
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
    for k in sensitive_keys:
        v = os.getenv(k, "")
        if v and len(v) >= 6:
            s = s.replace(v, "***")

    if _envb("CANARY_MASK_LONG_TOKENS", True):
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


def _mask_url(u: str) -> str:
    if not u:
        return ""
    try:
        out = _URL_CRED_RE.sub(r"\1***@", str(u))
        if "?" in out:
            out = out.split("?", 1)[0] + "?..."
        return out
    except Exception:
        return str(u)


# -----------------------------------------------------------------------------
# Generic helpers
# -----------------------------------------------------------------------------
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


def _sol_to_lamports(sol: float) -> int:
    return int(round(float(sol) * 1_000_000_000))


def _pick(d: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    for k in keys:
        if k in d and d.get(k) is not None:
            return d.get(k)
    return default


# -----------------------------------------------------------------------------
# Signature drift tolerant calling
# -----------------------------------------------------------------------------
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


async def _maybe_await(x: Any) -> Any:
    return await x if inspect.isawaitable(x) else x


async def _call(fn: Any, *args: Any, **kwargs: Any) -> Any:
    return await _maybe_await(fn(*args, **kwargs))


async def _try_call_named(
    target: Any,
    names: Tuple[str, ...],
    variants: List[Tuple[Tuple[Any, ...], Dict[str, Any]]],
    timeout_s: float,
) -> Tuple[bool, Any, str]:
    last_err = "no_callable_found"
    for nm in names:
        fn = getattr(target, nm, None)
        if not callable(fn):
            continue
        for args, kwargs in variants:
            try:
                kw = _filter_kwargs(fn, dict(kwargs))
                val = await asyncio.wait_for(_call(fn, *args, **kw), timeout=max(1.0, float(timeout_s)))
                return True, val, nm
            except TypeError as e:
                last_err = _sanitize_text(f"{nm}: {e}")
                continue
            except asyncio.TimeoutError:
                last_err = f"{nm}: timeout"
                continue
            except Exception as e:
                return False, None, _sanitize_text(f"{nm}: {_short_exc(e)}")
    return False, None, last_err


def _try_import(name: str) -> Any:
    with contextlib.suppress(Exception):
        return importlib.import_module(name)
    return None


# -----------------------------------------------------------------------------
# RPC selection + simulateTransaction (for simulate stage)
# -----------------------------------------------------------------------------
async def _rpc_post(url: str, payload: Dict[str, Any], timeout_s: float) -> Dict[str, Any]:
    url = str(url).strip()
    if not url:
        raise RuntimeError("empty RPC url")

    with contextlib.suppress(Exception):
        import httpx  # type: ignore

        async with httpx.AsyncClient(timeout=timeout_s) as c:
            r = await c.post(url, json=payload, headers={"content-type": "application/json"})
            if int(r.status_code) != 200:
                raise RuntimeError(f"rpc status={r.status_code} body={r.text[:180]!r}")
            return r.json()

    import urllib.request

    def _do() -> Dict[str, Any]:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers={"content-type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:  # noqa: S310
            body = resp.read().decode("utf-8", "ignore")
            return json.loads(body)

    return await asyncio.to_thread(_do)


async def _rpc_get_latest_blockhash(url: str, timeout_s: float) -> str:
    payload = {"jsonrpc": "2.0", "id": 1, "method": "getLatestBlockhash", "params": [{"commitment": "processed"}]}
    data = await _rpc_post(url, payload, timeout_s=timeout_s)
    bh = None
    with contextlib.suppress(Exception):
        bh = data["result"]["value"]["blockhash"]
    if not bh:
        raise RuntimeError("could not parse getLatestBlockhash response")
    return str(bh)


def _candidate_rpcs() -> List[str]:
    out: List[str] = []
    for k in ("RPC_URL", "SOL_RPC_URL"):
        v = _envs(k, "")
        if v:
            out.append(v)
    for k in ("RPC_URLS", "SOL_RPC_URLS", "SOLANA_RPC_URLS"):
        out.extend([p.strip() for p in _envs(k, "").replace(";", ",").split(",") if p.strip()])

    seen: Set[str] = set()
    uniq: List[str] = []
    for x in out:
        if x and x not in seen:
            uniq.append(x)
            seen.add(x)
    return uniq


async def _select_rpc(timeout_s: float) -> Tuple[str, Dict[str, Any]]:
    """
    Prefer utils_pkg.rpc_failover.apply_failover_to_env() if present, else internal probe.
    """
    rf = _try_import("utils_pkg.rpc_failover")
    if rf is not None and callable(getattr(rf, "apply_failover_to_env", None)):
        ok, val, used = await _try_call_named(
            rf,
            ("apply_failover_to_env",),
            variants=[((), {"timeout_s": timeout_s}), ((), {"timeout": timeout_s}), ((), {})],
            timeout_s=timeout_s,
        )
        if ok:
            chosen = os.getenv("RPC_URL") or os.getenv("SOL_RPC_URL") or ""
            notes = None
            if isinstance(val, (tuple, list)) and len(val) >= 1:
                chosen = str(val[0] or chosen or "")
                notes = val[1] if len(val) >= 2 else None
            if chosen:
                os.environ["RPC_URL"] = chosen
                os.environ["SOL_RPC_URL"] = chosen
                return chosen, {"via": f"rpc_failover:{used}", "notes": _safe_jsonable(notes)}

    cands = _candidate_rpcs()
    if not cands:
        raise RuntimeError("no RPC candidates (set RPC_URL or SOL_RPC_URL or *_URLS)")
    best: Optional[Tuple[str, int, str]] = None
    last_err = None
    for u in cands[:12]:
        t0 = time.time()
        try:
            bh = await asyncio.wait_for(_rpc_get_latest_blockhash(u, timeout_s=timeout_s), timeout=max(1.0, timeout_s + 1.0))
            ms = int((time.time() - t0) * 1000)
            if best is None or ms < best[1]:
                best = (u, ms, bh)
        except Exception as e:
            last_err = _short_exc(e)
            continue
    if not best:
        raise RuntimeError(f"no reachable RPC from {len(cands)} candidates; last_err={last_err}")
    u, ms, bh = best
    os.environ["RPC_URL"] = u
    os.environ["SOL_RPC_URL"] = u
    return u, {"via": "internal_probe_best_latency", "latency_ms": ms, "blockhash": bh}


async def _simulate_b64_tx(rpc_url: str, tx_b64: str, timeout_s: float) -> Dict[str, Any]:
    """
    Simulate without signature verification.
    """
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "simulateTransaction",
        "params": [
            str(tx_b64),
            {
                "encoding": "base64",
                "sigVerify": False,
                "commitment": "processed",
                "replaceRecentBlockhash": True,
            },
        ],
    }
    data = await _rpc_post(rpc_url, payload, timeout_s=timeout_s)
    with contextlib.suppress(Exception):
        if "error" in data and data["error"]:
            return {"ok": False, "error": _safe_jsonable(data["error"])}
    res = data.get("result") or {}
    value = res.get("value") or {}
    err = value.get("err")
    logs = value.get("logs")
    return {
        "ok": bool(err is None),
        "err": _safe_jsonable(err),
        "unitsConsumed": _safe_jsonable(value.get("unitsConsumed")),
        "logs_head": (list(logs)[:25] if isinstance(logs, list) else None),
    }


# -----------------------------------------------------------------------------
# Mints normalization (LOCK-IN)
# -----------------------------------------------------------------------------
def _normalize_mint_or_symbol(mint_input: str) -> str:
    mint_input = str(mint_input or "").strip()
    if not mint_input:
        return ""

    mints = _try_import("utils_pkg.mints")
    if mints is None:
        return mint_input

    # try_normalize_mint preferred
    fn = getattr(mints, "try_normalize_mint", None)
    if callable(fn):
        with contextlib.suppress(Exception):
            out = fn(mint_input)
            if out:
                return str(out)

    # normalize_mint fallback
    fn2 = getattr(mints, "normalize_mint", None)
    if callable(fn2):
        with contextlib.suppress(Exception):
            out = fn2(mint_input)
            if out:
                return str(out)

    # common consts
    for const in ("USDC_MINT", "SOL_MINT"):
        if mint_input.upper() == const.split("_")[0]:
            v = getattr(mints, const, None)
            if v:
                return str(v)

    return mint_input


def _get_mint_constants() -> Tuple[str, str]:
    mints = _try_import("utils_pkg.mints")
    sol = _envs("SOL_MINT", "So11111111111111111111111111111111111111112").strip()
    usdc = _envs("USDC_MINT", "").strip()
    if mints is not None:
        sol = str(getattr(mints, "SOL_MINT", sol) or sol)
        usdc = str(getattr(mints, "USDC_MINT", usdc) or usdc)
    if not usdc:
        # keep it empty if project doesn't define; script can still run with explicit mint
        usdc = ""
    return sol, usdc


# -----------------------------------------------------------------------------
# Router helpers: quote / build swap tx
# -----------------------------------------------------------------------------
async def _resolve_router_target(router_mod: Any, timeout_s: float) -> Tuple[Any, str]:
    if callable(getattr(router_mod, "get_router", None)):
        ok, obj, used = await _try_call_named(
            router_mod,
            ("get_router",),
            variants=[((), {}), ((), {"timeout_s": timeout_s}), ((), {"timeout": timeout_s})],
            timeout_s=timeout_s,
        )
        if ok and obj is not None:
            return obj, f"get_router:{used}"

    RouterCls = getattr(router_mod, "Router", None)
    if callable(RouterCls):
        with contextlib.suppress(Exception):
            return RouterCls(), "Router()"

    return router_mod, "module"


async def _router_bootstrap(target: Any, timeout_s: float) -> str:
    ok, _, used = await _try_call_named(
        target,
        ("start", "bootstrap", "init", "setup", "warmup"),
        variants=[((), {}), ((), {"timeout_s": timeout_s}), ((), {"timeout": timeout_s})],
        timeout_s=timeout_s,
    )
    return used if ok else f"bootstrap_skipped({used})"


async def _router_shutdown(target: Any, timeout_s: float) -> str:
    ok, _, used = await _try_call_named(
        target,
        ("shutdown", "close", "stop", "teardown"),
        variants=[((), {}), ((), {"timeout_s": timeout_s}), ((), {"timeout": timeout_s})],
        timeout_s=timeout_s,
    )
    return used if ok else f"shutdown_skipped({used})"


async def _get_buy_quote(
    target: Any,
    *,
    out_mint: str,
    amount_sol: float,
    slippage_bps: int,
    sol_mint: str,
    timeout_s: float,
) -> Tuple[Optional[Dict[str, Any]], str]:
    # Dedicated buy-quote
    ok, q, used = await _try_call_named(
        target,
        ("get_buy_quote", "buy_quote", "quote_buy"),
        variants=[
            ((), {"mint": out_mint, "amount_sol": float(amount_sol), "slippage_bps": int(slippage_bps)}),
            ((), {"mint": out_mint, "amount_sol": float(amount_sol), "slippageBps": int(slippage_bps)}),
            ((out_mint, float(amount_sol), int(slippage_bps)), {}),
        ],
        timeout_s=timeout_s,
    )
    if ok and isinstance(q, dict):
        return dict(q), used

    lamports = _sol_to_lamports(amount_sol)
    ok2, q2, used2 = await _try_call_named(
        target,
        ("get_quote", "quote", "get_swap_quote", "swap_quote", "jup_quote", "jupiter_quote"),
        variants=[
            ((), {"inputMint": sol_mint, "outputMint": out_mint, "amount": int(lamports), "slippageBps": int(slippage_bps)}),
            ((), {"input_mint": sol_mint, "output_mint": out_mint, "amount": int(lamports), "slippage_bps": int(slippage_bps)}),
            ((sol_mint, out_mint, int(lamports)), {"slippageBps": int(slippage_bps)}),
        ],
        timeout_s=timeout_s,
    )
    if ok2 and isinstance(q2, dict):
        return dict(q2), used2

    return None, _sanitize_text(f"{used} | {used2}")


async def _get_sell_quote(
    target: Any,
    *,
    in_mint: str,
    amount_raw: int,
    slippage_bps: int,
    sol_mint: str,
    timeout_s: float,
) -> Tuple[Optional[Dict[str, Any]], str]:
    amount_raw = int(max(1, int(amount_raw)))
    ok, q, used = await _try_call_named(
        target,
        ("get_sell_quote", "sell_quote", "quote_sell"),
        variants=[
            ((), {"mint": in_mint, "amount_raw": int(amount_raw), "slippage_bps": int(slippage_bps)}),
            ((), {"mint": in_mint, "amount_raw": int(amount_raw), "slippageBps": int(slippage_bps)}),
            ((in_mint, int(amount_raw), int(slippage_bps)), {}),
        ],
        timeout_s=timeout_s,
    )
    if ok and isinstance(q, dict):
        return dict(q), used

    ok2, q2, used2 = await _try_call_named(
        target,
        ("get_quote", "quote", "get_swap_quote", "swap_quote", "jup_quote", "jupiter_quote"),
        variants=[
            ((), {"inputMint": in_mint, "outputMint": sol_mint, "amount": int(amount_raw), "slippageBps": int(slippage_bps)}),
            ((), {"input_mint": in_mint, "output_mint": sol_mint, "amount": int(amount_raw), "slippage_bps": int(slippage_bps)}),
            ((in_mint, sol_mint, int(amount_raw)), {"slippageBps": int(slippage_bps)}),
        ],
        timeout_s=timeout_s,
    )
    if ok2 and isinstance(q2, dict):
        return dict(q2), used2

    return None, _sanitize_text(f"{used} | {used2}")


def _get_user_pubkey() -> Tuple[str, str]:
    """
    Prefer CANARY_USER_PUBKEY. If absent, optionally derive from keypair IF CANARY_ALLOW_KEYLOAD=1.
    Never prints secrets.
    """
    pk = _envs("CANARY_USER_PUBKEY", "").strip()
    if pk:
        return pk, "CANARY_USER_PUBKEY"

    if not _envb("CANARY_ALLOW_KEYLOAD", False):
        return "", "missing (set CANARY_USER_PUBKEY or CANARY_ALLOW_KEYLOAD=1)"

    sw = _try_import("utils_pkg.solana_wallet")
    if sw is None:
        return "", "solana_wallet_missing"

    # Try get_pubkey_str()
    fn = getattr(sw, "get_pubkey_str", None)
    if callable(fn):
        with contextlib.suppress(Exception):
            v = fn()
            if v:
                return str(v), "solana_wallet.get_pubkey_str"

    # Try load_keypair + pubkey
    fn_load = getattr(sw, "load_keypair", None)
    if callable(fn_load):
        with contextlib.suppress(Exception):
            kp = fn_load()
            # common patterns: kp.pubkey() / kp.public_key
            if hasattr(kp, "pubkey") and callable(getattr(kp, "pubkey", None)):
                return str(kp.pubkey()), "solana_wallet.load_keypair.pubkey()"
            if hasattr(kp, "public_key"):
                return str(getattr(kp, "public_key")), "solana_wallet.load_keypair.public_key"
    return "", "could_not_derive_pubkey"


async def _build_swap_tx_b64(
    *,
    router_target: Any,
    quote: Dict[str, Any],
    user_pubkey: str,
    wrap_unwrap_sol: bool,
    timeout_s: float,
) -> Tuple[Optional[str], str, Dict[str, Any]]:
    """
    Build tx via router if supported; else fall back to lightning_impl.
    Returns (tx_b64, via, extra)
    """
    # Router path first
    ok, res, used = await _try_call_named(
        router_target,
        (
            "build_swap_tx",
            "build_swap",
            "build_tx",
            "get_swap_tx",
            "swap_tx",
            "jup_swap_tx",
            "swap_transaction",
        ),
        variants=[
            ((), {"quote": quote, "user_pubkey": user_pubkey, "wrap_unwrap_sol": wrap_unwrap_sol, "timeout_s": timeout_s}),
            ((), {"quote_response": quote, "userPublicKey": user_pubkey, "wrapAndUnwrapSol": wrap_unwrap_sol, "timeout_s": timeout_s}),
            ((), {"quote": quote, "userPublicKey": user_pubkey, "wrapAndUnwrapSol": wrap_unwrap_sol}),
            ((quote, user_pubkey), {"wrapAndUnwrapSol": wrap_unwrap_sol}),
        ],
        timeout_s=timeout_s,
    )
    if ok:
        if isinstance(res, dict):
            tx = res.get("swapTransaction") or res.get("swap_transaction") or res.get("tx_b64") or res.get("transaction")
            if isinstance(tx, str) and tx.strip():
                return tx.strip(), f"router.{used}", {"keys": list(res.keys())[:18]}
        if isinstance(res, str) and res.strip():
            return res.strip(), f"router.{used}", {}

    # Lightning fallback
    L = _try_import("utils_pkg.lightning_impl")
    if L is None:
        return None, f"router_failed({used})_and_lightning_missing", {}

    ok2, res2, used2 = await _try_call_named(
        L,
        ("swap", "build_swap_tx", "get_swap_tx", "swap_tx", "swap_transaction"),
        variants=[
            ((), {"quoteResponse": quote, "userPublicKey": user_pubkey, "wrapAndUnwrapSol": wrap_unwrap_sol, "timeout_s": timeout_s}),
            ((), {"quote_response": quote, "user_pubkey": user_pubkey, "wrap_unwrap_sol": wrap_unwrap_sol, "timeout_s": timeout_s}),
            ((), {"quote": quote, "user_pubkey": user_pubkey, "wrap_unwrap_sol": wrap_unwrap_sol}),
        ],
        timeout_s=timeout_s,
    )
    if ok2:
        if isinstance(res2, dict):
            tx = res2.get("swapTransaction") or res2.get("swap_transaction") or res2.get("tx_b64") or res2.get("transaction")
            if isinstance(tx, str) and tx.strip():
                return tx.strip(), f"lightning_impl.{used2}", {"keys": list(res2.keys())[:18]}
        if isinstance(res2, str) and res2.strip():
            return res2.strip(), f"lightning_impl.{used2}", {}
    return None, f"build_failed(router={used}, lightning={used2})", {}


# -----------------------------------------------------------------------------
# Entry / exit wiring
# -----------------------------------------------------------------------------
def _resolve_entry_executor() -> Optional[Callable[..., Any]]:
    for mod_name in (
        "sniper.entry_executor",
        "sniper.sniper",
        "utils_pkg.pump_trader",
        "utils_pkg.trader",
        "utils_pkg.entry_engine",
        "utils_pkg.trade_engine",
        "utils_pkg.sniper",
    ):
        with contextlib.suppress(Exception):
            m = importlib.import_module(mod_name)
            fn = getattr(m, "maybe_buy", None)
            if callable(fn):
                return fn
    return None


def _resolve_sell_executor() -> Optional[Callable[..., Any]]:
    for mod_name in (
        "sniper.entry_executor",
        "utils_pkg.pump_trader",
        "utils_pkg.trader",
        "utils_pkg.exit_engine",
        "utils_pkg.auto_sell",
    ):
        with contextlib.suppress(Exception):
            m = importlib.import_module(mod_name)
            for nm in ("maybe_sell", "sell_token", "sell_position", "force_sell"):
                fn = getattr(m, nm, None)
                if callable(fn):
                    return fn
    return None


def _resolve_exit_forwarders() -> List[Tuple[str, Callable[[Dict[str, Any]], Any]]]:
    out: List[Tuple[str, Callable[[Dict[str, Any]], Any]]] = []
    with contextlib.suppress(Exception):
        m = importlib.import_module("utils_pkg.exit_engine")
        fn = getattr(m, "on_buy_filled", None) or getattr(m, "on_buy_filled_event", None)
        if callable(fn):
            out.append(("utils_pkg.exit_engine", fn))
    with contextlib.suppress(Exception):
        m2 = importlib.import_module("utils_pkg.auto_exit_worker.exit_engine")
        fn2 = getattr(m2, "on_buy_filled", None) or getattr(m2, "on_buy_filled_event", None)
        if callable(fn2):
            out.append(("utils_pkg.auto_exit_worker.exit_engine", fn2))
    return out


async def _forward_buy_filled(forwarders: List[Tuple[str, Callable[[Dict[str, Any]], Any]]], payload: Dict[str, Any]) -> Dict[str, Any]:
    called: Dict[str, Any] = {}
    for name, fn in forwarders:
        try:
            await _call(fn, payload)
            called[name] = True
        except Exception:
            called[name] = False
    return called


def _live_allowed(cli_confirm: str) -> Tuple[bool, str]:
    if not _envb("CANARY_TRADE_ENABLED", False):
        return False, "CANARY_TRADE_ENABLED!=1"
    if _envs("CANARY_MODE", "").strip().lower() != "live":
        return False, "CANARY_MODE!=live"
    if not _envb("CANARY_LIVE", False):
        return False, "CANARY_LIVE!=1"
    if _envs("CANARY_CONFIRM", "").strip().upper() != "YES":
        return False, "CANARY_CONFIRM!=YES"
    if str(cli_confirm).strip().upper() != "YES":
        return False, "--live-confirm must be YES"
    return True, "ok"


def _sell_back_allowed() -> Tuple[bool, str]:
    if not _envb("CANARY_SELL_BACK", False):
        return False, "CANARY_SELL_BACK!=1"
    if _envs("CANARY_SELL_CONFIRM", "").strip().upper() != "YES":
        return False, "CANARY_SELL_CONFIRM!=YES"
    return True, "ok"


# -----------------------------------------------------------------------------
# Core resources (best-effort; stabilizes entry path)
# -----------------------------------------------------------------------------
async def _core_init(timeout_s: float) -> str:
    if _envb("CANARY_SKIP_CORE_INIT", False):
        return "skipped(env CANARY_SKIP_CORE_INIT=1)"
    core = _try_import("utils_pkg.core_utils")
    if core is None:
        return "core_utils_missing"
    ok, _, used = await _try_call_named(
        core,
        ("init_resources", "init", "bootstrap", "setup", "warmup"),
        variants=[((), {}), ((), {"timeout_s": timeout_s}), ((), {"timeout": timeout_s})],
        timeout_s=timeout_s,
    )
    return used if ok else f"init_skipped({used})"


async def _core_close(timeout_s: float) -> str:
    if _envb("CANARY_SKIP_CORE_INIT", False):
        return "skipped(env CANARY_SKIP_CORE_INIT=1)"
    core = _try_import("utils_pkg.core_utils")
    if core is None:
        return "core_utils_missing"
    ok, _, used = await _try_call_named(
        core,
        ("close_resources", "close", "shutdown", "teardown"),
        variants=[((), {}), ((), {"timeout_s": timeout_s}), ((), {"timeout": timeout_s})],
        timeout_s=timeout_s,
    )
    return used if ok else f"close_skipped({used})"


# -----------------------------------------------------------------------------
# Callback latch
# -----------------------------------------------------------------------------
@dataclass
class CanaryEvents:
    submitted: int = 0
    filled: int = 0
    failed: int = 0
    last_payload: Dict[str, Any] = field(default_factory=dict)
    filled_qty_raw: int = 0
    forwarded: Dict[str, Any] = field(default_factory=dict)


def _mk_cb(async_fn: Callable[[Dict[str, Any]], Awaitable[None]]) -> Callable[[Dict[str, Any]], None]:
    def _cb(payload: Dict[str, Any]) -> None:
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(async_fn(payload or {}))
        except Exception:
            pass

    return _cb


async def _invoke_maybe_buy(
    maybe_buy: Callable[..., Any],
    *,
    mint: str,
    ui: Dict[str, Any],
    amount_sol: float,
    slippage_bps: int,
    dryrun: bool,
    callbacks: Dict[str, Callable[[Dict[str, Any]], None]],
    timeout_s: float,
) -> Tuple[bool, Any, str]:
    sig_txt = ""
    with contextlib.suppress(Exception):
        sig_txt = str(inspect.signature(maybe_buy))

    base_kwargs = {
        "mint": mint,
        "ui": ui,
        "source": "scripts.canary_trade",
        "worker_id": -1,
        "amount_sol": float(amount_sol),
        "slippage_bps": int(slippage_bps),
        "slippageBps": int(slippage_bps),
        "dryrun": bool(dryrun),
        "DRYRUN": bool(dryrun),
        **callbacks,
        # hints (only passed if accepted)
        "is_canary": True,
        "canary": True,
        "force": True,
        "force_enter": True,
        "bypass_gate": True,
        "ignore_gate": True,
    }

    # A) kwargs-only
    try:
        kw = _filter_kwargs(maybe_buy, dict(base_kwargs))
        res = await asyncio.wait_for(_call(maybe_buy, **kw), timeout=max(1.0, timeout_s))
        return True, res, f"kwargs {sig_txt}".strip()
    except TypeError:
        pass
    except Exception as e:
        return False, None, _sanitize_text(f"kwargs: {_short_exc(e)}")

    # B) candidate positional + kwargs
    candidate = {"mint": mint, "ui": ui, "source": "scripts.canary_trade", "canary": True}
    try:
        kw = _filter_kwargs(maybe_buy, dict(base_kwargs))
        res = await asyncio.wait_for(_call(maybe_buy, candidate, **kw), timeout=max(1.0, timeout_s))
        return True, res, f"(candidate, **kw) {sig_txt}".strip()
    except TypeError:
        pass
    except Exception as e:
        return False, None, _sanitize_text(f"(candidate, **kw): {_short_exc(e)}")

    # C) (mint, amount_sol, slippage) + kwargs
    try:
        kw = _filter_kwargs(maybe_buy, dict(base_kwargs))
        res = await asyncio.wait_for(_call(maybe_buy, mint, float(amount_sol), int(slippage_bps), **kw), timeout=max(1.0, timeout_s))
        return True, res, f"(mint,amount,slip,**kw) {sig_txt}".strip()
    except TypeError as e:
        return False, None, _sanitize_text(f"positional: {e}")
    except Exception as e:
        return False, None, _sanitize_text(f"positional: {_short_exc(e)}")


# -----------------------------------------------------------------------------
# Optional canary logging (best-effort)
# -----------------------------------------------------------------------------
async def _best_effort_log_canary(payload: Dict[str, Any]) -> None:
    if _envb("CANARY_SKIP_LOGGER", False):
        return
    dl = _try_import("utils_pkg.data_logger")
    if dl is None:
        return
    for nm in ("log_canary", "log_event", "log_json", "log_misc"):
        fn = getattr(dl, nm, None)
        if callable(fn):
            with contextlib.suppress(Exception):
                await _maybe_await(fn(payload))
            return


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
async def _amain() -> int:
    ap = argparse.ArgumentParser(description="God-Tier Canary (SAFE by default)")
    ap.add_argument(
        "--stage",
        default=_envs("CANARY_STAGE", "dryrun"),
        choices=["quote", "build", "simulate", "dryrun", "live"],
        help="quote=quotes only; build=quote+build tx; simulate=build+simulate; dryrun=invoke entry in dryrun; live=real trade (HARD LOCKED)",
    )
    ap.add_argument("--mint", default=_envs("CANARY_MINT", "USDC"), help="symbol/mint/url/blob (normalized via utils_pkg.mints)")
    ap.add_argument("--amount-sol", type=float, default=float(_envf("CANARY_AMOUNT_SOL", 0.01)))
    ap.add_argument("--slippage-bps", type=int, default=int(_envi("CANARY_SLIPPAGE_BPS", 60)))
    ap.add_argument("--timeout", type=float, default=float(_envf("CANARY_TIMEOUT_S", 18.0)))
    ap.add_argument("--post-wait", type=float, default=float(_envf("CANARY_POST_WAIT_S", 2.0)), help="seconds to wait for callbacks")
    ap.add_argument("--json", action="store_true", default=_envb("CANARY_JSON", False))
    ap.add_argument("--json-only", action="store_true", default=_envb("CANARY_JSON_ONLY", False))
    ap.add_argument("--strict", action="store_true", default=_envb("CANARY_STRICT", False), help="fail if sell quote/build/sim missing when stage requires it")
    ap.add_argument("--sell-back", action="store_true", default=False, help="LIVE ONLY; requires extra env locks")
    ap.add_argument("--live-confirm", type=str, default="", help="LIVE ONLY: must be YES")
    args = ap.parse_args()

    json_only = bool(args.json_only)
    json_mode = bool(args.json or json_only)
    quiet = bool(json_only)

    t0 = time.time()
    stage = str(args.stage).lower().strip()

    # Clamp for safety
    max_live_sol = float(_envf("CANARY_MAX_LIVE_SOL", 0.02))
    amount_sol = max(0.000001, float(args.amount_sol))
    if stage == "live":
        amount_sol = min(amount_sol, max_live_sol)

    slippage_bps = int(max(1, min(5000, int(args.slippage_bps))))
    timeout_s = max(4.0, float(args.timeout))
    post_wait_s = float(max(0.0, min(12.0, float(args.post_wait))))

    # Ensure repo root importability
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)

    sol_mint, usdc_mint = _get_mint_constants()

    mint_input = str(args.mint).strip()
    out_mint = _normalize_mint_or_symbol(mint_input)
    if not out_mint:
        out_mint = mint_input
    out_mint = out_mint.strip()
    if not out_mint:
        _hlog("FAIL empty mint after normalization", quiet=quiet)
        return 2

    # Enforce DRYRUN env BEFORE importing entry modules
    if stage in ("dryrun", "quote", "build", "simulate"):
        os.environ["DRYRUN"] = "1"
        os.environ["PF_DRYRUN"] = "1"
        os.environ["BUY_DRYRUN"] = "1"
    elif stage == "live":
        os.environ["DRYRUN"] = "0"
        os.environ["PF_DRYRUN"] = "0"
        os.environ["BUY_DRYRUN"] = "0"

    out: Dict[str, Any] = {
        "ok": False,
        "ts": _ts(),
        "stage": stage,
        "host": _sanitize_text(platform.node()),
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "mint_input": mint_input,
        "mint": out_mint,
        "sol_mint": sol_mint,
        "usdc_mint": usdc_mint or None,
        "amount_sol": float(amount_sol),
        "amount_lamports": int(_sol_to_lamports(amount_sol)),
        "slippage_bps": int(slippage_bps),
        "timeout_s": float(timeout_s),
        "router": {},
        "rpc": {},
        "quotes": {},
        "build": {},
        "simulate": {},
        "core": {},
        "entry": {},
        "exit_wiring": {},
        "elapsed_ms": 0,
    }

    # Import router
    try:
        from utils_pkg import router as router_mod  # type: ignore
    except Exception as e:
        out["router"] = {"ok": False, "err": _short_exc(e)}
        out["elapsed_ms"] = int((time.time() - t0) * 1000)
        if json_mode:
            print(json.dumps(_safe_jsonable(out), ensure_ascii=False, default=str))
        else:
            _hlog(f"FAIL import_router err={out['router']['err']}", quiet=quiet)
        return 2

    router_target, router_used = await _resolve_router_target(router_mod, timeout_s=timeout_s)
    out["router"] = {"ok": True, "target": router_used}
    out["router"]["bootstrap"] = await _router_bootstrap(router_target, timeout_s=timeout_s)

    # 1) Quote (required for ALL stages)
    buy_q, buy_via = await _get_buy_quote(
        router_target,
        out_mint=out_mint,
        amount_sol=amount_sol,
        slippage_bps=slippage_bps,
        sol_mint=sol_mint,
        timeout_s=timeout_s,
    )
    if not buy_q:
        out["quotes"] = {"ok": False, "buy_via": buy_via}
        out["router"]["shutdown"] = await _router_shutdown(router_target, timeout_s=timeout_s)
        out["elapsed_ms"] = int((time.time() - t0) * 1000)
        await _best_effort_log_canary(out)
        if json_mode:
            print(json.dumps(_safe_jsonable(out), ensure_ascii=False, default=str))
        else:
            _hlog(f"FAIL buy_quote via={buy_via}", quiet=quiet)
        return 2

    out_raw = _to_int(_pick(buy_q, "outAmount", "out_amount_raw", "outAmountRaw", "outputAmount", default=0), default=0)
    rp = buy_q.get("routePlan")
    out["quotes"]["buy"] = {
        "ok": True,
        "via": buy_via,
        "out_amount_raw": (out_raw if out_raw > 0 else None),
        "priceImpactPct": _safe_jsonable(buy_q.get("priceImpactPct")),
        "routes": (len(rp) if isinstance(rp, list) else None),
        "keys": list(buy_q.keys())[:18],
    }

    sell_ok = False
    sell_via = "skipped"
    if out_raw > 0:
        probe_raw = int(max(10, min(out_raw, int(_envi("CANARY_SELL_QUOTE_MAX_RAW", 50_000_000)))))
        sell_q, sell_via = await _get_sell_quote(
            router_target,
            in_mint=out_mint,
            amount_raw=probe_raw,
            slippage_bps=slippage_bps,
            sol_mint=sol_mint,
            timeout_s=timeout_s,
        )
        sell_ok = bool(sell_q)
        out["quotes"]["sell"] = {"ok": sell_ok, "via": sell_via}
    else:
        out["quotes"]["sell"] = {"ok": False, "via": "skipped(out_raw<=0)"}

    if stage == "quote":
        out["ok"] = True
        out["router"]["shutdown"] = await _router_shutdown(router_target, timeout_s=timeout_s)
        out["elapsed_ms"] = int((time.time() - t0) * 1000)
        await _best_effort_log_canary(out)
        if json_mode:
            print(json.dumps(_safe_jsonable(out), ensure_ascii=False, default=str))
        else:
            _hlog(f"OK stage=quote buy_via={buy_via} sell_via={sell_via} elapsed_ms={out['elapsed_ms']}", quiet=quiet)
        return 0

    # For build/simulate: require user pubkey
    wrap_unwrap_sol = _envb("CANARY_WRAP_UNWRAP_SOL", True)

    tx_b64: Optional[str] = None
    build_via = "skipped"
    build_extra: Dict[str, Any] = {}

    if stage in ("build", "simulate"):
        user_pk, pk_via = _get_user_pubkey()
        out["build"]["user_pubkey_via"] = pk_via
        out["build"]["user_pubkey_set"] = bool(user_pk)
        if not user_pk:
            out["build"]["ok"] = False
            out["build"]["err"] = "missing user pubkey (set CANARY_USER_PUBKEY or CANARY_ALLOW_KEYLOAD=1)"
            if args.strict:
                out["router"]["shutdown"] = await _router_shutdown(router_target, timeout_s=timeout_s)
                out["elapsed_ms"] = int((time.time() - t0) * 1000)
                await _best_effort_log_canary(out)
                if json_mode:
                    print(json.dumps(_safe_jsonable(out), ensure_ascii=False, default=str))
                else:
                    _hlog("FAIL build requires user pubkey", quiet=quiet)
                return 2
        else:
            tx_b64, build_via, build_extra = await _build_swap_tx_b64(
                router_target=router_target,
                quote=buy_q,
                user_pubkey=user_pk,
                wrap_unwrap_sol=wrap_unwrap_sol,
                timeout_s=timeout_s,
            )
            out["build"]["via"] = build_via
            out["build"]["ok"] = bool(tx_b64)
            out["build"]["extra"] = _safe_jsonable(build_extra)
            if tx_b64:
                out["build"]["tx_b64_len"] = len(tx_b64)
            if (not tx_b64) and args.strict:
                out["router"]["shutdown"] = await _router_shutdown(router_target, timeout_s=timeout_s)
                out["elapsed_ms"] = int((time.time() - t0) * 1000)
                await _best_effort_log_canary(out)
                if json_mode:
                    print(json.dumps(_safe_jsonable(out), ensure_ascii=False, default=str))
                else:
                    _hlog(f"FAIL build_tx via={build_via}", quiet=quiet)
                return 2

        if stage == "build":
            out["ok"] = bool(tx_b64) or (not args.strict)
            out["router"]["shutdown"] = await _router_shutdown(router_target, timeout_s=timeout_s)
            out["elapsed_ms"] = int((time.time() - t0) * 1000)
            await _best_effort_log_canary(out)
            if json_mode:
                print(json.dumps(_safe_jsonable(out), ensure_ascii=False, default=str))
            else:
                status = "OK" if out["ok"] else "FAIL"
                _hlog(f"{status} stage=build build_via={build_via} elapsed_ms={out['elapsed_ms']}", quiet=quiet)
            return 0 if out["ok"] else 2

    # Simulate stage: requires tx and rpc
    if stage == "simulate":
        if not tx_b64:
            out["simulate"] = {"ok": False, "err": "no tx_b64 to simulate"}
            if args.strict:
                out["router"]["shutdown"] = await _router_shutdown(router_target, timeout_s=timeout_s)
                out["elapsed_ms"] = int((time.time() - t0) * 1000)
                await _best_effort_log_canary(out)
                if json_mode:
                    print(json.dumps(_safe_jsonable(out), ensure_ascii=False, default=str))
                else:
                    _hlog("FAIL simulate requires built tx", quiet=quiet)
                return 2
        try:
            rpc_url, rpc_meta = await _select_rpc(timeout_s=max(2.0, min(10.0, timeout_s)))
            out["rpc"] = {"ok": True, "url": _mask_url(rpc_url)[:180], "meta": _safe_jsonable(rpc_meta)}
            sim = await _simulate_b64_tx(rpc_url, tx_b64 or "", timeout_s=max(3.0, min(20.0, timeout_s)))
            out["simulate"] = sim
            out["ok"] = bool(sim.get("ok")) or (not args.strict)
        except Exception as e:
            out["rpc"] = {"ok": False, "err": _short_exc(e)}
            out["simulate"] = {"ok": False}
            out["ok"] = False

        out["router"]["shutdown"] = await _router_shutdown(router_target, timeout_s=timeout_s)
        out["elapsed_ms"] = int((time.time() - t0) * 1000)
        await _best_effort_log_canary(out)
        if json_mode:
            print(json.dumps(_safe_jsonable(out), ensure_ascii=False, default=str))
        else:
            status = "OK" if out["ok"] else "FAIL"
            _hlog(f"{status} stage=simulate build_via={build_via} rpc_ok={int(bool(out['rpc'].get('ok')))} elapsed_ms={out['elapsed_ms']}", quiet=quiet)
        return 0 if out["ok"] else 2

    # Entry stages: dryrun/live
    # Guard: to touch trader (even DRYRUN), require CANARY_TRADE_ENABLED=1
    if not _envb("CANARY_TRADE_ENABLED", False):
        out["entry"] = {"ok": False, "note": "CANARY_TRADE_ENABLED!=1 (refusing to call trader)"}
        out["router"]["shutdown"] = await _router_shutdown(router_target, timeout_s=timeout_s)
        out["elapsed_ms"] = int((time.time() - t0) * 1000)
        await _best_effort_log_canary(out)
        if json_mode:
            print(json.dumps(_safe_jsonable(out), ensure_ascii=False, default=str))
        else:
            _hlog("FAIL CANARY_TRADE_ENABLED!=1", quiet=quiet)
        return 2

    dryrun = (stage == "dryrun")
    if stage == "live":
        ok_live, why = _live_allowed(args.live_confirm)
        if not ok_live:
            out["entry"] = {"ok": False, "note": f"live_blocked ({why})"}
            out["router"]["shutdown"] = await _router_shutdown(router_target, timeout_s=timeout_s)
            out["elapsed_ms"] = int((time.time() - t0) * 1000)
            await _best_effort_log_canary(out)
            if json_mode:
                print(json.dumps(_safe_jsonable(out), ensure_ascii=False, default=str))
            else:
                _hlog(f"FAIL live_blocked {why}", quiet=quiet)
            return 2

    # Init core resources (best-effort)
    out["core"]["init"] = await _core_init(timeout_s=min(timeout_s, 12.0))

    maybe_buy = _resolve_entry_executor()
    forwarders = _resolve_exit_forwarders()
    events = CanaryEvents()

    if maybe_buy is None:
        out["entry"] = {"ok": False, "note": "maybe_buy_not_found (quotes OK)"}
        out["core"]["close"] = await _core_close(timeout_s=min(timeout_s, 10.0))
        out["router"]["shutdown"] = await _router_shutdown(router_target, timeout_s=timeout_s)
        out["elapsed_ms"] = int((time.time() - t0) * 1000)
        await _best_effort_log_canary(out)
        if json_mode:
            print(json.dumps(_safe_jsonable(out), ensure_ascii=False, default=str))
        else:
            _hlog("FAIL maybe_buy_not_found", quiet=quiet)
        return 2

    # Canary UI payload (no secrets)
    ui: Dict[str, Any] = {
        "mint": out_mint,
        "pool": "canary",
        "source": "scripts.canary_trade",
        "ts": time.time(),
        "canary": True,
        "slippage_bps": int(slippage_bps),
        "amount_sol": float(amount_sol),
        "stage": stage,
    }

    async def _on_submitted_async(p: Dict[str, Any]) -> None:
        events.submitted += 1
        events.last_payload = dict(p or {})

    async def _on_filled_async(p: Dict[str, Any]) -> None:
        events.filled += 1
        events.last_payload = dict(p or {})
        events.filled_qty_raw = _to_int(_pick(p or {}, "qty_raw", "amount_raw", "out_amount_raw", "filled_amount_raw", default=0), default=0)
        if not _envb("CANARY_SKIP_EXIT_FORWARD", False) and forwarders:
            events.forwarded = await _forward_buy_filled(forwarders, dict(p or {}))

    async def _on_failed_async(p: Dict[str, Any]) -> None:
        events.failed += 1
        events.last_payload = dict(p or {})

    callbacks = {
        "on_buy_submitted": _mk_cb(_on_submitted_async),
        "on_submitted": _mk_cb(_on_submitted_async),
        "on_buy_filled": _mk_cb(_on_filled_async),
        "on_filled": _mk_cb(_on_filled_async),
        "on_buy_failed": _mk_cb(_on_failed_async),
        "on_failed": _mk_cb(_on_failed_async),
    }

    invoked_ok = False
    invoke_used = ""
    res_obj: Any = None

    try:
        invoked_ok, res_obj, invoke_used = await _invoke_maybe_buy(
            maybe_buy,
            mint=out_mint,
            ui=ui,
            amount_sol=amount_sol,
            slippage_bps=slippage_bps,
            dryrun=dryrun,
            callbacks=callbacks,
            timeout_s=timeout_s,
        )
        if post_wait_s > 0:
            await asyncio.sleep(post_wait_s)

        out["entry"] = {
            "invoked": bool(invoked_ok),
            "invoke_used": invoke_used,
            "dryrun": int(dryrun),
            "events": {"submitted": events.submitted, "filled": events.filled, "failed": events.failed},
            "filled_qty_raw": int(events.filled_qty_raw),
            "forwarded": _safe_jsonable(events.forwarded),
            "result_type": type(res_obj).__name__ if res_obj is not None else None,
            "result": _safe_jsonable(res_obj) if isinstance(res_obj, (dict, str, int, float, bool, type(None))) else (type(res_obj).__name__ if res_obj is not None else None),
        }

        # PASS RULES:
        # - DRYRUN: invoked successfully without crash
        # - LIVE: invoked successfully AND (truthy result OR fill callback)
        if dryrun:
            out["ok"] = bool(invoked_ok)
        else:
            out["ok"] = bool(invoked_ok) and (bool(res_obj) or events.filled > 0)

    except Exception as e:
        out["entry"] = {"invoked": False, "err": _short_exc(e), "trace": _sanitize_text(traceback.format_exc()[:1400])}
        out["ok"] = False

    # Optional LIVE sell-back (extra locks)
    if stage == "live" and out["ok"] and bool(args.sell_back):
        ok_sb, why_sb = _sell_back_allowed()
        if not ok_sb:
            out["entry"]["sell_back"] = {"attempted": False, "note": why_sb}
        else:
            sell_fn = _resolve_sell_executor()
            if sell_fn is None:
                out["entry"]["sell_back"] = {"attempted": False, "note": "no_sell_executor_found"}
            elif events.filled_qty_raw <= 0:
                out["entry"]["sell_back"] = {"attempted": False, "note": "no_filled_qty_raw_detected"}
            else:
                frac = float(_envf("CANARY_SELL_BACK_FRACTION", 0.90))
                frac = max(0.01, min(1.0, frac))
                amt = int(max(1, int(events.filled_qty_raw * frac)))
                sell_kw = _filter_kwargs(
                    sell_fn,
                    {
                        "mint": out_mint,
                        "amount_raw": amt,
                        "slippage_bps": int(slippage_bps),
                        "slippageBps": int(slippage_bps),
                        "source": "scripts.canary_trade",
                        "ui": ui,
                    },
                )
                try:
                    ok_s = await asyncio.wait_for(_call(sell_fn, **sell_kw), timeout=max(timeout_s, 25.0))
                    out["entry"]["sell_back"] = {"attempted": True, "ok": bool(ok_s), "amount_raw": amt}
                    if not ok_s:
                        out["ok"] = False
                except Exception as e:
                    out["entry"]["sell_back"] = {"attempted": True, "ok": False, "err": _short_exc(e)}
                    out["ok"] = False

    out["exit_wiring"] = {
        "forwarders_found": [name for name, _ in forwarders],
        "buy_filled_forwarded": _safe_jsonable(events.forwarded),
    }

    # Close core + router
    out["core"]["close"] = await _core_close(timeout_s=min(timeout_s, 10.0))
    out["router"]["shutdown"] = await _router_shutdown(router_target, timeout_s=timeout_s)

    out["elapsed_ms"] = int((time.time() - t0) * 1000)

    await _best_effort_log_canary(out)

    if json_mode:
        print(json.dumps(_safe_jsonable(out), ensure_ascii=False, default=str))
    else:
        status = "OK" if out["ok"] else "FAIL"
        _hlog(
            f"{status} stage={stage} mint={out_mint[:10]}… buy_via={buy_via} sell_ok={int(bool(sell_ok))} "
            f"events={out.get('entry', {}).get('events', {})} elapsed_ms={out['elapsed_ms']}",
            quiet=quiet,
        )

    return 0 if out["ok"] else 2


def main() -> None:
    try:
        rc = asyncio.run(_amain())
    except KeyboardInterrupt:
        rc = 130
    except Exception:
        err = _sanitize_text(traceback.format_exc())
        sys.stderr.write(f"[canary] {_ts()} FAIL unhandled:\n{err}\n")
        rc = 2
    raise SystemExit(rc)


if __name__ == "__main__":
    # Windows-friendly loop policy
    with contextlib.suppress(Exception):
        if os.name == "nt" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]
    main()
