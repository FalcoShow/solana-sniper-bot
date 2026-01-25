# utils_pkg/sizer.py
from __future__ import annotations

"""
utils_pkg/sizer.py

God-Tier Sizer (P0) — deterministic trade sizing in SOL
=======================================================

Role
----
Compute the *recommended* per-trade size in SOL, with hard rails, rounding,
and optional integration with `utils_pkg.risk_manager` outputs.

Design goals
------------
- Sync + deterministic (safe to call anywhere; no RPC)
- Backwards compatible: `suggest_trade_sol()` still works
- Production-safe: clamps, rounding modes, environment contract, and guardrails
- Risk-aware: if risk_manager exposes an interface, we use it; otherwise we fall back

Primary env knobs
-----------------
Sizing mode:
- PF_TRADE_AUTOSIZE (default true)
- PF_TRADE_AUTOSIZE_MODE: "risk" | "equity" | "wallet" | "fixed" (default "risk")

Fixed:
- PF_TRADE_SOL (fallback: BUY_SOL_PER_TRADE)

Rails / rounding:
- PF_TRADE_MIN_SOL (default 0.005)
- PF_TRADE_MAX_SOL (default 0.10)
- PF_TRADE_STEP_SOL (default 0.005)
- PF_TRADE_ROUNDING: "nearest" | "floor" | "ceil" (default "nearest")

Equity-based sizing:
- EQUITY_USD_FALLBACK (default 2000)
- SOL_PRICE_USD_FALLBACK (default 150)
- POSITION_PCT_OF_EQUITY or TICKET_PCT_EQUITY (default 0.05)

Wallet-based sizing (no RPC):
- WALLET_SOL_FALLBACK or PF_WALLET_SOL_FALLBACK (default 0)
- PF_TRADE_RISK_PCT or POSITION_PCT_OF_WALLET (default 0.05)

Optional global multipliers:
- PF_TRADE_MULT (default 1.0)            -> multiplies size after base sizing
- PF_TRADE_MAX_AFTER_MULT_SOL (optional) -> extra cap after multipliers (still inside rails)
"""

import contextlib
import inspect
import logging
import math
import os
from collections.abc import Callable
from typing import Any

log = logging.getLogger(__name__)


# -------------------------
# env helpers (strict-safe)
# -------------------------
def _env_str(k: str, default: str = "") -> str:
    v = os.getenv(k)
    if v is None:
        return default
    v = v.strip()
    return default if v == "" else v


def _env_bool(k: str, default: bool = False) -> bool:
    v = _env_str(k, "")
    if v == "":
        return default
    return v.lower() in {"1", "true", "yes", "y", "on"}


def _env_float(k: str, default: float) -> float:
    v = _env_str(k, "")
    if v == "":
        return float(default)
    try:
        return float(v)
    except Exception:
        return float(default)


def _clamp(x: float, lo: float, hi: float) -> float:
    return float(max(lo, min(hi, x)))


def _round_step(x: float, step: float, mode: str) -> float:
    """
    Rounds x to step. Modes:
      - nearest: round to nearest step (ties are handled by Python's round)
      - floor: down to step
      - ceil: up to step
    """
    if not math.isfinite(x):
        return 0.0
    if step <= 0:
        return float(x)

    q = x / step
    m = (mode or "nearest").strip().lower()
    if m in {"floor", "down"}:
        return float(math.floor(q) * step)
    if m in {"ceil", "up"}:
        return float(math.ceil(q) * step)
    # nearest
    return float(round(q) * step)


def _rails() -> tuple[float, float, float, str]:
    mn = _env_float("PF_TRADE_MIN_SOL", 0.005)
    mx = _env_float("PF_TRADE_MAX_SOL", 0.10)
    step = _env_float("PF_TRADE_STEP_SOL", 0.005)
    rounding = _env_str("PF_TRADE_ROUNDING", "nearest").lower()

    # sanitize
    if not math.isfinite(mn) or mn < 0:
        mn = 0.0
    if not math.isfinite(mx) or mx < 0:
        mx = 0.0
    if mn > mx:
        mn, mx = mx, mn
    if not math.isfinite(step) or step < 0:
        step = 0.0
    if rounding not in {"nearest", "floor", "ceil", "down", "up"}:
        rounding = "nearest"
    return float(mn), float(mx), float(step), rounding


def _mode() -> str:
    """
    "risk" (preferred) | "equity" | "wallet" | "fixed"
    Default: "risk"
    """
    m = _env_str("PF_TRADE_AUTOSIZE_MODE", "risk").strip().lower()
    if m in {"fixed", "manual"}:
        return "fixed"
    if m in {"wallet", "bal", "balance"}:
        return "wallet"
    if m in {"equity", "usd"}:
        return "equity"
    return "risk"


# -------------------------
# base sizing strategies
# -------------------------
def _fixed_ticket_sol(mn: float) -> float:
    """
    Fixed-size sizing.
    Primary: PF_TRADE_SOL
    Fallback: BUY_SOL_PER_TRADE
    """
    v = _env_str("PF_TRADE_SOL", "")
    if v == "":
        v = _env_str("BUY_SOL_PER_TRADE", "")
    if v == "":
        return float(mn)
    try:
        return float(v)
    except Exception:
        return float(mn)


def _equity_ticket_sol() -> float:
    """
    Equity-based sizing: equity (USD) / SOL (USD) * pct.
    """
    equity_usd = _env_float("EQUITY_USD_FALLBACK", 2000.0)
    sol_usd = max(1e-9, _env_float("SOL_PRICE_USD_FALLBACK", 150.0))
    pct = _env_float("POSITION_PCT_OF_EQUITY", _env_float("TICKET_PCT_EQUITY", 0.05))
    pct = _clamp(pct, 0.0, 1.0)
    return float((equity_usd / sol_usd) * pct)


def _wallet_ticket_sol() -> float:
    """
    Wallet-based sizing (no RPC): use WALLET_SOL_FALLBACK and PF_TRADE_RISK_PCT.
    """
    wal = _env_str("WALLET_SOL_FALLBACK", _env_str("PF_WALLET_SOL_FALLBACK", "0"))
    try:
        wallet_sol = max(0.0, float(wal))
    except Exception:
        wallet_sol = 0.0

    pct = _env_float("PF_TRADE_RISK_PCT", _env_float("POSITION_PCT_OF_WALLET", 0.05))
    pct = _clamp(pct, 0.0, 1.0)
    return float(wallet_sol * pct)


# -------------------------
# risk_manager integration
# -------------------------
def _try_call(fn: Callable[..., Any], **kwargs: Any) -> float | None:
    """
    Call fn with the subset of kwargs it supports, return float if possible.
    """
    try:
        sig = inspect.signature(fn)
        allowed = {k: v for k, v in kwargs.items() if k in sig.parameters}
        out = fn(**allowed)
        if out is None:
            return None
        val = float(out)
        if not math.isfinite(val):
            return None
        return val
    except Exception:
        return None


def _risk_manager_size_override(
    base_sol: float,
    *,
    candidate: Any = None,
    features: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
) -> float | None:
    """
    Best-effort integration with utils_pkg.risk_manager.

    We do NOT assume a single API; we try a small set of common hooks.
    If nothing matches, return None (caller falls back).
    """
    if not _env_bool("PF_TRADE_USE_RISK_MANAGER", True):
        return None

    try:
        from utils_pkg import risk_manager as rm  # type: ignore
    except Exception:
        return None

    # 1) If trading is halted, some risk_managers expose `is_halted` or `allow_new_trades`
    for guard_name in ("is_trading_halted", "is_halted"):
        guard = getattr(rm, guard_name, None)
        if callable(guard):
            g = _try_call(guard)
            if g is not None and bool(g) is True:
                return 0.0
    allow = getattr(rm, "allow_new_trades", None)
    if callable(allow):
        a = _try_call(allow)
        if a is not None and bool(a) is False:
            return 0.0

    # 2) Common "sizing" entrypoints we might have defined in risk_manager
    candidates = (
        "suggest_trade_sol",
        "size_trade_sol",
        "compute_trade_size_sol",
        "recommend_trade_sol",
        "trade_size_sol",
    )
    for name in candidates:
        fn = getattr(rm, name, None)
        if not callable(fn):
            continue
        out = _try_call(
            fn,
            base_sol=float(base_sol),
            candidate=candidate,
            features=features,
            context=context,
        )
        if out is not None:
            return float(out)

    # 3) Multiplier-style APIs (risk adjusts base size)
    for name in ("trade_size_multiplier", "size_multiplier", "get_size_multiplier"):
        fn = getattr(rm, name, None)
        if not callable(fn):
            continue
        mult = _try_call(
            fn,
            candidate=candidate,
            features=features,
            context=context,
        )
        if mult is None:
            continue
        mult = float(mult)
        if not math.isfinite(mult):
            continue
        return float(max(0.0, base_sol * mult))

    # 4) State object path (best-effort):
    #    If risk_manager exposes a `get_risk_state()` with attrs like:
    #      - halted / allow_trades
    #      - max_trade_sol
    #      - size_mult
    get_state = getattr(rm, "get_risk_state", None)
    if callable(get_state):
        try:
            st = get_state()  # we won't pass args; state is usually global
            if st is not None:
                halted = bool(getattr(st, "halted", False) or getattr(st, "trading_halted", False))
                allow_trades = getattr(st, "allow_trades", None)
                if halted or (allow_trades is False):
                    return 0.0

                size_mult = getattr(st, "size_mult", None)
                max_trade_sol = getattr(st, "max_trade_sol", None)
                out = float(base_sol)

                if size_mult is not None:
                    with contextlib.suppress(Exception):
                        out = max(0.0, out * float(size_mult))
                if max_trade_sol is not None:
                    with contextlib.suppress(Exception):
                        out = min(out, float(max_trade_sol))
                if math.isfinite(out):
                    return float(out)
        except Exception:
            pass

    return None


# -------------------------
# public API
# -------------------------
def compute_trade_sol(
    *,
    candidate: Any = None,
    features: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
) -> tuple[float, dict[str, Any]]:
    """
    Compute the recommended SOL size plus an explanation dict
    (useful for tests / logs / debug).

    This is the "new" rich API; `suggest_trade_sol()` calls this and returns only the float.
    """
    mn, mx, step, rounding = _rails()
    autosize = _env_bool("PF_TRADE_AUTOSIZE", True)
    mode = _mode()

    detail: dict[str, Any] = {
        "autosize": autosize,
        "mode": mode,
        "mn": mn,
        "mx": mx,
        "step": step,
        "rounding": rounding,
        "source": None,
        "base_sol": None,
        "risk_override_sol": None,
        "mult": None,
        "post_mult_cap": None,
        "final_sol": None,
    }

    # Base sizing (pre-risk)
    if not autosize or mode == "fixed":
        base = _fixed_ticket_sol(mn)
        detail["source"] = "fixed"
    else:
        if mode == "wallet":
            base = _wallet_ticket_sol()
            detail["source"] = "wallet"
        elif mode == "equity":
            base = _equity_ticket_sol()
            detail["source"] = "equity"
        else:
            # "risk" starts from equity by default; risk_manager can override/adjust
            base = _equity_ticket_sol()
            detail["source"] = "risk(equity_base)"

    base = float(max(0.0, base))
    detail["base_sol"] = base

    # Risk manager override/adjustment (optional)
    if autosize and mode in {"risk", "equity", "wallet"}:
        rm_out = _risk_manager_size_override(
            base,
            candidate=candidate,
            features=features,
            context=context,
        )
        if rm_out is not None:
            base = float(max(0.0, rm_out))
            detail["risk_override_sol"] = base
            detail["source"] = f"{detail['source']}+risk_manager"

    # Optional multiplier stage (post-risk)
    mult = _env_float("PF_TRADE_MULT", 1.0)
    if not math.isfinite(mult) or mult < 0:
        mult = 1.0
    detail["mult"] = mult

    sized = float(base * mult)

    # Optional post-mult cap (still subject to rails)
    cap2 = _env_str("PF_TRADE_MAX_AFTER_MULT_SOL", "")
    if cap2 != "":
        try:
            c = float(cap2)
            if math.isfinite(c) and c >= 0:
                sized = min(sized, c)
                detail["post_mult_cap"] = c
        except Exception:
            pass

    # Rails + rounding (always)
    sized = _clamp(sized, mn, mx)
    sized = _round_step(sized, step, rounding)

    # If rounding pushed below mn but >0, snap up to mn (unless mn=0)
    if mn > 0 and 0 < sized < mn:
        sized = mn

    # Final sanitize
    if not math.isfinite(sized) or sized < 0:
        sized = 0.0

    detail["final_sol"] = float(sized)
    return float(sized), detail


def suggest_trade_sol() -> float:
    """
    Backwards-compatible API: returns recommended trade size in SOL (sync, deterministic).
    """
    sol, _detail = compute_trade_sol()
    return float(sol)


__all__ = ["compute_trade_sol", "suggest_trade_sol"]
