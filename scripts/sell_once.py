#!/usr/bin/env python3
from __future__ import annotations

"""
scripts/sell_once.py — God-Tier One-Off Sell (SAFE + deterministic)
===================================================================

Purpose
-------
A one-off SELL driver that routes through the canonical sell adapter:
    scripts.sell_adapter.execute_sell(...)

This is a *tooling script* (P2 quarantine / dev-only) but kept production-safe:
- Defaults to DRYRUN unless you explicitly unlock LIVE mode
- Strong input validation
- Redacted output (never prints secrets)
- Retries with bounded backoff
- Signature-drift tolerant call into execute_sell()

Hard safety rules
-----------------
- Default is DRYRUN (no broadcast expected).
- LIVE requires explicit locks (env + CLI confirm):

    SELL_ONCE_ENABLED=1
    SELL_ONCE_MODE=live
    SELL_ONCE_LIVE=1
    SELL_ONCE_CONFIRM=YES
    --confirm YES
    --live

Exit codes
----------
0   success (sell executed or dryrun succeeded)
2   failure / blocked
130 ctrl-c

Examples
--------
# DRYRUN (default)
python -m scripts.sell_once --mint EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v --symbol USDC --amount 0.001

# Explicit DRYRUN flag
python -m scripts.sell_once --mint ... --symbol ... --amount 0.001 --dry-run

# LIVE (real sell) — HARD LOCKED
SELL_ONCE_ENABLED=1 SELL_ONCE_MODE=live SELL_ONCE_LIVE=1 SELL_ONCE_CONFIRM=YES \
  python -m scripts.sell_once --live --confirm YES --mint ... --symbol ... --amount 0.001

# Env-driven
SELL_MINT=... SELL_SYMBOL=USDC SELL_AMOUNT=0.001 python -m scripts.sell_once
"""

import argparse
import asyncio
import contextlib
import importlib
import inspect
import json
import os
import re
import sys
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# -----------------------------------------------------------------------------
# Optional local dev .env (never required in Render)
# -----------------------------------------------------------------------------
with contextlib.suppress(Exception):
    from dotenv import find_dotenv, load_dotenv  # type: ignore

    load_dotenv(find_dotenv(), override=False)


# -----------------------------------------------------------------------------
# Repo root importability (Windows friendly / python -m)
# -----------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


# -----------------------------------------------------------------------------
# Logging helpers (human logs -> STDERR, JSON -> STDOUT)
# -----------------------------------------------------------------------------
def _ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _hlog(msg: str, *, quiet: bool = False) -> None:
    if quiet:
        return
    try:
        sys.stderr.write(f"[sell_once] {_ts()} {msg}\n")
        sys.stderr.flush()
    except Exception:
        pass


# -----------------------------------------------------------------------------
# Env parsing helpers
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
_LONG_TOKENISH_RE = re.compile(r"\b[A-Za-z0-9_\-]{28,}\b")


def _sanitize_text(text: Any) -> str:
    if text is None:
        return ""
    s = str(text)
    s = _URL_CRED_RE.sub(r"\1***@", s)

    # redact exact env values if present in string
    sensitive_keys = [
        "OPENAI_API_KEY",
        "TELEGRAM_BOT_TOKEN",
        "DISCORD_TOKEN",
        "WALLET_PRIVATE_KEY",
        "KEYPAIR_B64",
        "KEYPAIR_PATH",
        "DATABASE_URL",
        "PG_DSN",
        "PG_DSN_INTERNAL",
        "PG_DSN_EXTERNAL",
        "POSTGRES_DSN",
        "REDIS_URL",
        "REDIS_TLS_URL",
        "RPC_URL",
        "SOL_RPC_URL",
        "JUP_API_KEY",
        "JUP_PRO_KEY",
    ]
    for k in sensitive_keys:
        v = os.getenv(k, "")
        if v and len(v) >= 6:
            s = s.replace(v, "***")

    if _envb("SELL_ONCE_MASK_LONG_TOKENS", True):
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


# -----------------------------------------------------------------------------
# Base58-ish validation (lightweight)
# -----------------------------------------------------------------------------
_B58_ALPH = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")


def _looks_like_base58(s: str, min_len: int = 32, max_len: int = 64) -> bool:
    if not isinstance(s, str):
        return False
    t = s.strip()
    if len(t) < min_len or len(t) > max_len:
        return False
    return all(c in _B58_ALPH for c in t)


def _normalize_mint_or_symbol(mint_input: str) -> str:
    """
    Prefer utils_pkg.mints normalization if available.
    """
    mint_input = str(mint_input or "").strip()
    if not mint_input:
        return ""
    with contextlib.suppress(Exception):
        mints = importlib.import_module("utils_pkg.mints")
        fn = getattr(mints, "try_normalize_mint", None) or getattr(mints, "normalize_mint", None)
        if callable(fn):
            out = fn(mint_input)
            if out:
                return str(out).strip()
        # common constants
        if mint_input.upper() == "SOL":
            v = getattr(mints, "SOL_MINT", None)
            if v:
                return str(v).strip()
        if mint_input.upper() == "USDC":
            v = getattr(mints, "USDC_MINT", None)
            if v:
                return str(v).strip()
    return mint_input


# -----------------------------------------------------------------------------
# Async helpers
# -----------------------------------------------------------------------------
async def _maybe_await(x: Any) -> Any:
    return await x if inspect.isawaitable(x) else x


def _filter_kwargs(fn: Any, kwargs: dict[str, Any]) -> dict[str, Any]:
    try:
        sig = inspect.signature(fn)
        params = list(sig.parameters.values())
        if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params):
            return kwargs
        allowed = {p.name for p in params}
        return {k: v for k, v in kwargs.items() if k in allowed}
    except Exception:
        return kwargs


async def _call(fn: Any, *args: Any, **kwargs: Any) -> Any:
    return await _maybe_await(fn(*args, **kwargs))


# -----------------------------------------------------------------------------
# LIVE locks
# -----------------------------------------------------------------------------
def _live_allowed(confirm: str) -> tuple[bool, str]:
    if not _envb("SELL_ONCE_ENABLED", False):
        return False, "SELL_ONCE_ENABLED!=1"
    if _envs("SELL_ONCE_MODE", "").strip().lower() != "live":
        return False, "SELL_ONCE_MODE!=live"
    if not _envb("SELL_ONCE_LIVE", False):
        return False, "SELL_ONCE_LIVE!=1"
    if _envs("SELL_ONCE_CONFIRM", "").strip().upper() != "YES":
        return False, "SELL_ONCE_CONFIRM!=YES"
    if str(confirm).strip().upper() != "YES":
        return False, "--confirm must be YES"
    return True, "ok"


# -----------------------------------------------------------------------------
# Args / config
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class SellConfig:
    mint: str
    symbol: str
    amount: float
    slippage: float  # fraction, e.g. 0.01 = 1%
    reason: str
    dry_run: bool
    live: bool
    confirm: str
    retries: int
    json: bool
    json_only: bool


def _parse_args() -> SellConfig:
    """
    Backwards-compatible env keys:
      SELL_MINT, SELL_SYMBOL, SELL_AMOUNT, SELL_SLIPPAGE, SELL_REASON, SELL_DRY_RUN, SELL_RETRIES
    New safer keys:
      SELL_ONCE_* locks for LIVE execution
    """
    p = argparse.ArgumentParser(
        prog="sell_once",
        description="Execute a single sell via scripts.sell_adapter.execute_sell (SAFE by default).",
    )
    p.add_argument(
        "--mint",
        "-m",
        type=str,
        default=_envs("SELL_MINT", ""),
        help="Token mint to SELL (or symbol).",
    )
    p.add_argument(
        "--symbol",
        "-s",
        type=str,
        default=_envs("SELL_SYMBOL", ""),
        help="Readable symbol for logs (e.g., USDC).",
    )
    p.add_argument(
        "--amount",
        "-a",
        type=float,
        default=float(_envf("SELL_AMOUNT", 0.0)),
        help="Token amount (UI float) to sell.",
    )
    p.add_argument(
        "--slip",
        "--slippage",
        type=float,
        default=float(_envf("SELL_SLIPPAGE", 0.01)),
        help="Slippage fraction (0.01=1%). Default: 0.01",
    )
    p.add_argument(
        "--reason",
        type=str,
        default=_envs("SELL_REASON", "sell_once"),
        help="Reason string for logs/DB.",
    )
    p.add_argument(
        "--retries",
        type=int,
        default=int(_envi("SELL_RETRIES", 0)),
        help="Retry attempts on failure (default 0).",
    )

    # Safety / mode
    p.add_argument("--dry-run", action="store_true", help="Force dry-run (no broadcast expected).")
    p.add_argument(
        "--live",
        action="store_true",
        help="Attempt LIVE execution (HARD LOCKED by env + --confirm).",
    )
    p.add_argument(
        "--confirm", type=str, default="", help="LIVE ONLY: must be YES (plus env locks)."
    )

    # Output
    p.add_argument(
        "--json",
        action="store_true",
        default=_envb("SELL_JSON", False),
        help="Emit JSON summary to stdout.",
    )
    p.add_argument(
        "--json-only",
        action="store_true",
        default=_envb("SELL_JSON_ONLY", False),
        help="Only JSON to stdout.",
    )

    a = p.parse_args()

    mint_in = _normalize_mint_or_symbol(str(a.mint or "").strip())
    sym = str(a.symbol or "").strip() or _envs("SELL_SYMBOL", "")

    # If user passed "USDC" as mint and left symbol blank, mirror for logs.
    if not sym and mint_in and (mint_in.upper() in {"USDC", "SOL"}):
        sym = mint_in.upper()

    amount = float(a.amount) if a.amount is not None else 0.0
    slip = float(a.slip) if a.slip is not None else 0.0

    # Safer default: DRYRUN unless explicit --live AND locks pass
    env_default_dry = _envb("SELL_DRY_RUN", True)
    dry_run = bool(a.dry_run) or (not bool(a.live)) or bool(env_default_dry and not bool(a.live))
    live = bool(a.live)

    return SellConfig(
        mint=mint_in,
        symbol=sym,
        amount=amount,
        slippage=slip,
        reason=str(a.reason or "sell_once"),
        dry_run=dry_run,
        live=live,
        confirm=str(a.confirm or ""),
        retries=int(max(0, int(a.retries))),
        json=bool(a.json or a.json_only),
        json_only=bool(a.json_only),
    )


def _validate(cfg: SellConfig) -> None:
    if not cfg.mint:
        raise ValueError("Missing --mint (or SELL_MINT).")
    if not _looks_like_base58(cfg.mint, min_len=32, max_len=64):
        # allow symbol-like input only if normalization produced base58; otherwise reject
        raise ValueError("Invalid --mint (expected base58 mint address; try a full token mint).")

    if not cfg.symbol or len(cfg.symbol) < 2:
        raise ValueError("Invalid --symbol (or SELL_SYMBOL).")

    if cfg.amount <= 0:
        raise ValueError("Invalid --amount (or SELL_AMOUNT). Must be > 0.")

    if cfg.slippage <= 0 or cfg.slippage > 0.5:
        raise ValueError(
            "Invalid --slip/--slippage. Require 0 < slip ≤ 0.5 (e.g., 0.005 for 0.5%)."
        )

    if cfg.live and cfg.dry_run:
        # If user explicitly asked --live but also forced dry-run, we honor dry-run.
        pass


# -----------------------------------------------------------------------------
# Adapter + core resources
# -----------------------------------------------------------------------------
async def _init_resources(timeout_s: float) -> str:
    """
    Best-effort init_resources wrapper (sync/async tolerant).
    """
    try:
        cu = importlib.import_module("utils_pkg.core_utils")
    except Exception as e:
        return f"core_utils_import_failed:{_short_exc(e)}"

    fn = (
        getattr(cu, "init_resources", None)
        or getattr(cu, "init", None)
        or getattr(cu, "bootstrap", None)
    )
    if not callable(fn):
        return "init_resources_missing"

    try:
        await asyncio.wait_for(_maybe_await(fn()), timeout=max(1.0, timeout_s))
        return "ok"
    except Exception as e:
        return f"init_failed:{_short_exc(e)}"


async def _close_resources(timeout_s: float) -> str:
    """
    Best-effort close_resources wrapper (sync/async tolerant).
    """
    try:
        cu = importlib.import_module("utils_pkg.core_utils")
    except Exception as e:
        return f"core_utils_import_failed:{_short_exc(e)}"

    fn = (
        getattr(cu, "close_resources", None)
        or getattr(cu, "close", None)
        or getattr(cu, "shutdown", None)
    )
    if not callable(fn):
        return "close_resources_missing"

    try:
        await asyncio.wait_for(_maybe_await(fn()), timeout=max(1.0, timeout_s))
        return "ok"
    except Exception as e:
        return f"close_failed:{_short_exc(e)}"


async def _execute_sell(cfg: SellConfig, timeout_s: float) -> tuple[bool, dict[str, Any]]:
    """
    Calls scripts.sell_adapter.execute_sell with signature-drift tolerance.
    """
    try:
        adapter = importlib.import_module("scripts.sell_adapter")
    except Exception as e:
        return False, {"err": f"sell_adapter_import_failed:{_short_exc(e)}"}

    fn = getattr(adapter, "execute_sell", None)
    if not callable(fn):
        return False, {"err": "sell_adapter.execute_sell_missing"}

    # canonical kwargs
    kw = {
        "mint": cfg.mint,
        "symbol": cfg.symbol,
        "amount": float(cfg.amount),
        "slippage": float(cfg.slippage),
        "reason": cfg.reason,
        "dry_run": bool(cfg.dry_run),
        # extra hints if supported
        "source": "scripts.sell_once",
        "is_tool": True,
        "tool": "sell_once",
    }

    # Try kwargs-only first
    try:
        fkw = _filter_kwargs(fn, dict(kw))
        res = await asyncio.wait_for(_call(fn, **fkw), timeout=max(2.0, timeout_s))
        return bool(res), {"result": _safe_jsonable(res), "used": "kwargs"}
    except TypeError:
        pass
    except Exception as e:
        return False, {"err": f"execute_sell_failed:{_short_exc(e)}", "used": "kwargs"}

    # Fallback positional (mint, symbol, amount) + kwargs
    try:
        fkw = _filter_kwargs(fn, dict(kw))
        res = await asyncio.wait_for(
            _call(fn, cfg.mint, cfg.symbol, float(cfg.amount), **fkw), timeout=max(2.0, timeout_s)
        )
        return bool(res), {
            "result": _safe_jsonable(res),
            "used": "positional(mint,symbol,amount)+kw",
        }
    except Exception as e:
        return False, {"err": f"execute_sell_failed:{_short_exc(e)}", "used": "positional"}


async def _run_with_retries(cfg: SellConfig, timeout_s: float) -> tuple[bool, dict[str, Any]]:
    attempt = 0
    last_meta: dict[str, Any] = {}
    while True:
        attempt += 1
        ok, meta = await _execute_sell(cfg, timeout_s=timeout_s)
        meta = dict(meta or {})
        meta["attempt"] = attempt
        last_meta = meta

        if ok:
            return True, last_meta

        if attempt > cfg.retries:
            return False, last_meta

        # bounded exponential backoff
        delay = min(2.0 * attempt, 10.0)
        await asyncio.sleep(delay)


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
async def _amain() -> int:
    t0 = time.time()
    cfg = _parse_args()
    quiet = bool(cfg.json_only)

    out: dict[str, Any] = {
        "ok": False,
        "ts": _ts(),
        "mint": cfg.mint,
        "symbol": cfg.symbol,
        "amount": float(cfg.amount),
        "slippage": float(cfg.slippage),
        "reason": cfg.reason,
        "dry_run": bool(cfg.dry_run),
        "live_requested": bool(cfg.live),
        "retries": int(cfg.retries),
        "locks": {},
        "core": {},
        "sell": {},
        "elapsed_ms": 0,
    }

    # Validate inputs early
    try:
        _validate(cfg)
    except Exception as e:
        out["sell"] = {"ok": False, "err": _short_exc(e)}
        out["elapsed_ms"] = int((time.time() - t0) * 1000)
        if cfg.json:
            print(json.dumps(_safe_jsonable(out), ensure_ascii=False, default=str))
        else:
            _hlog(f"FAIL invalid_inputs err={_short_exc(e)}", quiet=quiet)
        return 2

    # Decide final mode
    if cfg.live and not cfg.dry_run:
        ok_live, why = _live_allowed(cfg.confirm)
        out["locks"] = {"live_allowed": bool(ok_live), "why": why}
        if not ok_live:
            out["sell"] = {"ok": False, "err": f"live_blocked:{why}"}
            out["elapsed_ms"] = int((time.time() - t0) * 1000)
            if cfg.json:
                print(json.dumps(_safe_jsonable(out), ensure_ascii=False, default=str))
            else:
                _hlog(f"FAIL live_blocked {why}", quiet=quiet)
            return 2
    else:
        out["locks"] = {"live_allowed": False, "why": "dryrun_default_or_requested"}

    # Human-readable config
    if not cfg.json_only:
        mode = "LIVE" if (cfg.live and not cfg.dry_run) else "DRYRUN"
        _hlog(
            f"config mode={mode} mint={cfg.mint[:8]}… symbol={cfg.symbol} amount={cfg.amount} slip={cfg.slippage} retries={cfg.retries}",
            quiet=quiet,
        )

    # Init core resources (best-effort)
    core_timeout = float(_envf("SELL_ONCE_CORE_TIMEOUT_S", 20.0))
    out["core"]["init"] = await _init_resources(timeout_s=min(core_timeout, 60.0))

    # Execute (with retries)
    exec_timeout = float(_envf("SELL_ONCE_EXEC_TIMEOUT_S", 90.0))
    try:
        ok, meta = await _run_with_retries(cfg, timeout_s=min(exec_timeout, 300.0))
        out["sell"] = {"ok": bool(ok), **_safe_jsonable(meta)}
        out["ok"] = bool(ok)
    except Exception as e:
        out["sell"] = {
            "ok": False,
            "err": _short_exc(e),
            "trace": _sanitize_text(traceback.format_exc()[:1400]),
        }
        out["ok"] = False
    finally:
        out["core"]["close"] = await _close_resources(timeout_s=min(core_timeout, 60.0))

    out["elapsed_ms"] = int((time.time() - t0) * 1000)

    if cfg.json:
        print(json.dumps(_safe_jsonable(out), ensure_ascii=False, default=str))
    else:
        status = "OK" if out["ok"] else "FAIL"
        _hlog(
            f"{status} sell_ok={int(bool(out['ok']))} elapsed_ms={out['elapsed_ms']}", quiet=quiet
        )

    return 0 if out["ok"] else 2


def main() -> None:
    try:
        rc = asyncio.run(_amain())
    except KeyboardInterrupt:
        rc = 130
    except Exception:
        err = _sanitize_text(traceback.format_exc())
        sys.stderr.write(f"[sell_once] {_ts()} FAIL unhandled:\n{err}\n")
        rc = 2
    raise SystemExit(rc)


if __name__ == "__main__":
    # Windows-friendly loop policy
    with contextlib.suppress(Exception):
        if os.name == "nt" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]
    main()
