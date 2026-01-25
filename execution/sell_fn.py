# execution/sell_fn.py
from __future__ import annotations

"""
execution/sell_fn.py — CANONICAL SELL ADAPTER (P0)
=================================================

Contract (LOCK-IN)
------------------
✅ This module MUST route all swaps through: `utils_pkg.router` (canonical money pipe).
❌ No direct Jupiter calls from here (no core_utils.swap_token_jupiter).

Role
----
Sell helpers used by exit engine / wiring:
- Sell a USD chunk (or token amount) of a mint into SOL using the canonical router.
- Compute safe, bounded slippage + priority fees/tips (via execution.fees).
- Emit structured metadata for ledger + debugging.
- Best-effort alerts (optional) without becoming a second alerting system.

Backwards compatibility
-----------------------
Keeps the legacy signature:
  sell_token_usd(...) -> (ok: bool, sig: str|None, meta: dict)

Notes
-----
- `last_px_sol_per_token` is used ONLY to convert USD->token units.
  (Execution correctness is enforced by router quote/route-guard.)
- If you can provide `token_decimals`, we will compute a safer `amount_raw`.
"""

import asyncio
import contextlib
import inspect
import os
import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from execution.fees import recommend_fee_package
from logging_setup import get_logger

# Canonical router (MONEY PIPE)
from utils_pkg import router as jrouter  # MUST be the only swap path

# Optional helpers (NOT swap)
try:
    from utils_pkg.core_utils import convert_sol_to_usd  # pricing helper only
except Exception:
    convert_sol_to_usd = None  # type: ignore

try:
    from utils_pkg.core_utils import send_telegram  # best-effort notify
except Exception:
    send_telegram = None  # type: ignore


log = get_logger(__name__)

SOL_MINT = "So11111111111111111111111111111111111111112"


# ──────────────────────────────────────────────────────────────────────────────
# Env knobs (bounded, deterministic)
# ──────────────────────────────────────────────────────────────────────────────


def _f(env_key: str, default: float) -> float:
    try:
        v = (os.getenv(env_key) or "").strip()
        return float(v) if v else float(default)
    except Exception:
        return float(default)


def _i(env_key: str, default: int) -> int:
    try:
        v = (os.getenv(env_key) or "").strip()
        return int(float(v)) if v else int(default)
    except Exception:
        return int(default)


# Support your old env too (fraction), but canonicalize to bps
# - JUPITER_MAX_SLIPPAGE = 0.005  (0.5%)  -> 50 bps
_JUP_SLIPPAGE_FRACTION = _f("JUPITER_MAX_SLIPPAGE", 0.0)
SELL_BASE_SLIPPAGE_BPS = _i("EXEC_SELL_BASE_SLIPPAGE_BPS", 120)  # 1.20% default for sells
SELL_MAX_SLIPPAGE_BPS = _i("EXEC_SELL_MAX_SLIPPAGE_BPS", 1200)  # 12% hard cap
SELL_MIN_USD = _f("EXEC_SELL_MIN_USD", 5.0)  # ignore tiny dust
SELL_MIN_TOKEN_UNITS = _f("EXEC_SELL_MIN_TOKEN_UNITS", 0.0)  # optional min token units
SELL_PREFLIGHT_QUOTE = _i("EXEC_SELL_PREFLIGHT_QUOTE", 0) == 1  # optional router quote
SELL_QUOTE_TIMEOUT_S = _f("EXEC_SELL_QUOTE_TIMEOUT_S", 3.0)

# “Urgency” used by execution.fees priority fee selection
SELL_URGENCY_SMALL = (os.getenv("EXEC_SELL_URGENCY_SMALL") or "normal").strip().lower()
SELL_URGENCY_MED = (os.getenv("EXEC_SELL_URGENCY_MED") or "fast").strip().lower()
SELL_URGENCY_LARGE = (os.getenv("EXEC_SELL_URGENCY_LARGE") or "max").strip().lower()

# If router supports passing priority params
PASS_PRIORITY_TO_ROUTER = _i("EXEC_PASS_PRIORITY_TO_ROUTER", 1) == 1

# Router call hard timeout (avoid hanging exit engine)
SELL_ROUTER_TIMEOUT_S = _f("EXEC_SELL_ROUTER_TIMEOUT_S", 20.0)


# ──────────────────────────────────────────────────────────────────────────────
# Models
# ──────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SellRequest:
    mint: str
    usd_to_sell: float
    reason: str
    last_px_sol_per_token: float
    token_decimals: int | None = None
    urgency: str | None = None
    slippage_bps: int | None = None
    recent_jito_tips_lamports: Sequence[int] | None = None


# ──────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ──────────────────────────────────────────────────────────────────────────────


def _slippage_bps(usd_to_sell: float, override_bps: int | None = None) -> int:
    """
    Deterministic bounded slippage:
    - Prefer explicit override
    - Else if JUPITER_MAX_SLIPPAGE provided (fraction), convert it
    - Else use base + mild size ramp, clipped
    """
    if override_bps is not None:
        bps = int(override_bps)
        return max(0, min(int(SELL_MAX_SLIPPAGE_BPS), bps))

    if _JUP_SLIPPAGE_FRACTION and _JUP_SLIPPAGE_FRACTION > 0:
        # fraction -> bps
        bps = int(round(float(_JUP_SLIPPAGE_FRACTION) * 10_000.0))
        return max(0, min(int(SELL_MAX_SLIPPAGE_BPS), bps))

    base = int(SELL_BASE_SLIPPAGE_BPS)
    cap = int(SELL_MAX_SLIPPAGE_BPS)

    # gentle ramp: +0..+400 bps over $0..$2k, capped
    usd = float(max(0.0, usd_to_sell))
    extra = int(min(400, (usd / 2000.0) * 400.0))  # linear ramp
    bps = base + extra
    return max(0, min(cap, bps))


def _pick_urgency(usd_to_sell: float, override: str | None) -> str:
    if override:
        u = override.strip().lower()
        return u if u in {"normal", "fast", "max"} else "fast"
    if usd_to_sell < 150:
        return SELL_URGENCY_SMALL if SELL_URGENCY_SMALL in {"normal", "fast", "max"} else "normal"
    if usd_to_sell < 800:
        return SELL_URGENCY_MED if SELL_URGENCY_MED in {"normal", "fast", "max"} else "fast"
    return SELL_URGENCY_LARGE if SELL_URGENCY_LARGE in {"normal", "fast", "max"} else "max"


def _amount_raw_from_units(amount_units: float, decimals: int | None) -> int | None:
    if decimals is None:
        return None
    try:
        d = int(decimals)
        d = max(0, min(18, d))
        raw = int(amount_units * (10**d))
        return max(0, raw)
    except Exception:
        return None


def _parse_router_result(res: Any) -> tuple[bool, str | None, dict[str, Any]]:
    """
    Normalize router return shapes:
      - (ok, sig) or (ok, sig, meta)
      - {"ok": bool, "sig": str, ...}
      - {"success": bool, "signature": str, ...}
    """
    meta: dict[str, Any] = {}

    if isinstance(res, tuple) and len(res) >= 2:
        ok = bool(res[0])
        sig = res[1] if isinstance(res[1], str) or res[1] is None else str(res[1])
        if len(res) >= 3 and isinstance(res[2], dict):
            meta.update(res[2])
        return ok, sig, meta

    if isinstance(res, dict):
        ok = bool(res.get("ok", res.get("success", False)))
        sig = res.get("sig", res.get("signature"))
        if sig is not None and not isinstance(sig, str):
            sig = str(sig)
        meta.update(
            {k: v for k, v in res.items() if k not in {"ok", "success", "sig", "signature"}}
        )
        return ok, sig, meta

    # Fallback: truthy means "ok" but signature unknown
    return bool(res), None, meta


async def _await_maybe(x: Any) -> Any:
    if asyncio.iscoroutine(x):
        return await x
    return x


async def _router_quote_best_effort(
    *,
    input_mint: str,
    output_mint: str,
    amount_raw: int | None,
    amount_float: float | None,
    slippage_bps: int,
) -> dict[str, Any]:
    """
    Optional preflight quote to improve debugging and sanity checks.
    Never blocks sell if quote fails.
    """
    if not SELL_PREFLIGHT_QUOTE:
        return {}

    # Try common quote function names
    fn = None
    for name in ("get_sell_quote", "get_quote", "quote", "get_swap_quote"):
        if hasattr(jrouter, name):
            fn = getattr(jrouter, name)
            break
    if fn is None:
        return {}

    kwargs = {
        "mint": input_mint,
        "input_mint": input_mint,
        "output_mint": output_mint,
        "amount_raw": amount_raw,
        "amount": amount_raw,
        "amount_float": amount_float,
        "amount_in": amount_raw,
        "slippage_bps": slippage_bps,
        "slippage": slippage_bps,
    }

    try:
        sig = inspect.signature(fn)
        call_kwargs = {k: v for k, v in kwargs.items() if k in sig.parameters and v is not None}
        # timeout so we never hang exits
        res = await asyncio.wait_for(
            _await_maybe(fn(**call_kwargs)), timeout=float(SELL_QUOTE_TIMEOUT_S)
        )
        return {"quote": res}
    except Exception as e:
        return {"quote_error": str(e)}


async def _router_swap(
    *,
    input_mint: str,
    output_mint: str,
    amount_raw: int | None,
    amount_float: float | None,
    slippage_bps: int,
    jito_tip_lamports: int,
    cu_limit: int | None,
    cu_price_microlamports: int | None,
) -> tuple[bool, str | None, dict[str, Any]]:
    """
    Call the canonical router swap function, adapting to signature drift.
    """
    # Find the best swap function name
    fn = None
    for name in (
        "swap",
        "swap_token",
        "swap_exact_in",
        "swap_exact_input",
        "execute_swap",
        "swap_token_jupiter",
    ):
        if hasattr(jrouter, name):
            fn = getattr(jrouter, name)
            break
    if fn is None:
        return False, None, {"error": "router_missing_swap_fn"}

    # Common arg names across router implementations
    kwargs = {
        "input_mint": input_mint,
        "in_mint": input_mint,
        "mint_in": input_mint,
        "output_mint": output_mint,
        "out_mint": output_mint,
        "mint_out": output_mint,
        # amount forms
        "amount_raw": amount_raw,
        "amount": amount_raw,
        "amount_in": amount_raw,
        "amount_in_raw": amount_raw,
        "amount_float": amount_float,
        "amount_ui": amount_float,
        "amount_in_ui": amount_float,
        # slippage forms
        "slippage_bps": slippage_bps,
        "slippage": slippage_bps,
        "max_slippage_bps": slippage_bps,
        # priority/tip forms
        "jito_tip_lamports": jito_tip_lamports,
        "tip_lamports": jito_tip_lamports,
        "cu_limit": cu_limit,
        "compute_unit_limit": cu_limit,
        "cu_price_microlamports": cu_price_microlamports,
        "compute_unit_price_microlamports": cu_price_microlamports,
    }

    try:
        sig = inspect.signature(fn)
        call_kwargs = {}
        for k, v in kwargs.items():
            if k in sig.parameters and v is not None:
                # allow disabling priority passthrough
                if not PASS_PRIORITY_TO_ROUTER and k in {
                    "jito_tip_lamports",
                    "tip_lamports",
                    "cu_limit",
                    "compute_unit_limit",
                    "cu_price_microlamports",
                    "compute_unit_price_microlamports",
                }:
                    continue
                call_kwargs[k] = v

        # Ensure we provide *some* amount
        if all(
            k not in call_kwargs
            for k in (
                "amount_raw",
                "amount",
                "amount_in",
                "amount_in_raw",
                "amount_float",
                "amount_ui",
                "amount_in_ui",
            )
        ):
            return False, None, {"error": "router_swap_missing_amount"}

        res = await asyncio.wait_for(
            _await_maybe(fn(**call_kwargs)), timeout=float(SELL_ROUTER_TIMEOUT_S)
        )
        ok, sig_out, meta = _parse_router_result(res)
        meta.setdefault("router_fn", getattr(fn, "__name__", "swap"))
        meta.setdefault(
            "router_args",
            {
                k: call_kwargs.get(k)
                for k in sorted(call_kwargs.keys())
                if k not in {"wallet", "keypair", "private_key"}
            },
        )
        return ok, sig_out, meta
    except Exception as e:
        return False, None, {"error": f"router_swap_exception:{e}"}


async def _notify_best_effort(msg: str) -> None:
    try:
        if send_telegram is not None:
            await _await_maybe(send_telegram(msg))
    except Exception:
        pass


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────


async def sell_token_usd(
    mint: str,
    usd_to_sell: float,
    reason: str,
    last_px_sol_per_token: float,
    *,
    token_decimals: int | None = None,
    urgency: str | None = None,
    slippage_bps: int | None = None,
    recent_jito_tips_lamports: Sequence[int] | None = None,
) -> tuple[bool, str | None, dict[str, Any]]:
    """
    Convert USD chunk -> token amount using last_px (SOL/token),
    then sell token -> SOL via utils_pkg.router (ONLY).

    Returns: (ok, signature, meta)
    """
    t0 = time.time()
    meta: dict[str, Any] = {
        "mint": mint,
        "usd_to_sell": float(usd_to_sell),
        "reason": reason,
        "last_px_sol_per_token": float(last_px_sol_per_token or 0.0),
        "ts": t0,
    }

    usd_to_sell = float(usd_to_sell)
    if usd_to_sell <= 0:
        meta["error"] = "usd_to_sell<=0"
        return False, None, meta
    if usd_to_sell < float(SELL_MIN_USD):
        meta["error"] = "usd_to_sell<min"
        meta["min_usd"] = float(SELL_MIN_USD)
        return False, None, meta
    if last_px_sol_per_token is None or float(last_px_sol_per_token) <= 0:
        meta["error"] = "last_px_invalid"
        return False, None, meta

    if convert_sol_to_usd is None:
        meta["error"] = "convert_sol_to_usd_unavailable"
        return False, None, meta

    usd_per_sol = await _await_maybe(convert_sol_to_usd(1.0))
    try:
        usd_per_sol = float(usd_per_sol or 0.0)
    except Exception:
        usd_per_sol = 0.0

    meta["usd_per_sol"] = usd_per_sol
    if usd_per_sol <= 0:
        meta["error"] = "usd_per_sol_unknown"
        return False, None, meta

    # USD -> SOL -> token units (since last_px is SOL/token)
    sol_to_sell = usd_to_sell / usd_per_sol
    token_units = sol_to_sell / float(last_px_sol_per_token)

    meta["sol_to_sell_est"] = sol_to_sell
    meta["token_units_est"] = token_units

    if SELL_MIN_TOKEN_UNITS and token_units < float(SELL_MIN_TOKEN_UNITS):
        meta["error"] = "token_units<min"
        meta["min_token_units"] = float(SELL_MIN_TOKEN_UNITS)
        return False, None, meta

    # If decimals given, compute raw for safer rounding
    amount_raw = _amount_raw_from_units(token_units, token_decimals)
    if amount_raw is not None:
        meta["token_decimals"] = int(token_decimals) if token_decimals is not None else None
        meta["amount_raw"] = int(amount_raw)

        # Avoid "0 raw" sells due to rounding; treat as dust
        if amount_raw <= 0:
            meta["error"] = "amount_raw<=0_dust"
            return False, None, meta

    # Slippage & urgency
    slip_bps = _slippage_bps(usd_to_sell, override_bps=slippage_bps)
    urg = _pick_urgency(usd_to_sell, urgency)

    meta["slippage_bps"] = int(slip_bps)
    meta["urgency"] = urg

    # Fee package (priority fees + bounded jito tip)
    fee_pkg = recommend_fee_package(
        recent_tips_lamports=recent_jito_tips_lamports,
        urgency=urg,
    )
    meta["fee"] = {
        "jito_tip_lamports": int(fee_pkg.tip_lamports),
        "cu_limit": int(fee_pkg.priority.cu_limit),
        "cu_price_microlamports": int(fee_pkg.priority.cu_price_microlamports),
        "est_total_fee_lamports": int(fee_pkg.est_total_fee_lamports),
        "notes": list(fee_pkg.notes),
    }

    # Optional quote preflight (best-effort)
    meta.update(
        await _router_quote_best_effort(
            input_mint=mint,
            output_mint=SOL_MINT,
            amount_raw=amount_raw,
            amount_float=None if amount_raw is not None else float(token_units),
            slippage_bps=slip_bps,
        )
    )

    # Execute swap through router ONLY
    ok, sig, rmeta = await _router_swap(
        input_mint=mint,
        output_mint=SOL_MINT,
        amount_raw=amount_raw,
        amount_float=None if amount_raw is not None else float(token_units),
        slippage_bps=slip_bps,
        jito_tip_lamports=int(fee_pkg.tip_lamports),
        cu_limit=int(fee_pkg.priority.cu_limit),
        cu_price_microlamports=int(fee_pkg.priority.cu_price_microlamports),
    )

    meta["router"] = rmeta
    meta["ok"] = bool(ok)
    meta["sig"] = sig
    meta["latency_s"] = round(time.time() - t0, 3)

    # Structured log
    try:
        log.event("SELL_TOKEN_USD", **{k: v for k, v in meta.items() if k not in {"quote"}})
    except Exception:
        with contextlib.suppress(Exception):
            log.info(
                "SELL_TOKEN_USD %s",
                {"ok": ok, "mint": mint, "usd": usd_to_sell, "sig": sig, "reason": reason},
            )

    # Best-effort notify (success only)
    if ok:
        await _notify_best_effort(
            f"🏁 Sold {mint[:6]}… • ${usd_to_sell:,.2f} • reason={reason} • slip={slip_bps}bps • tip={fee_pkg.tip_lamports}\nTx: {sig}"
        )

    return ok, sig, meta


__all__ = ["sell_token_usd"]
