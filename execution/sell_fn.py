from __future__ import annotations

"""
execution/sell_fn.py — CANONICAL SELL ADAPTER (P0)
=================================================

Contract (LOCK-IN)
------------------
✅ This module MUST route all swaps through: `utils_pkg.router` (canonical money pipe).
❌ No direct Jupiter calls from here.

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
- Execution correctness is enforced by router quote/route-guard.
"""

import asyncio
import contextlib
import inspect
import os
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Callable, Optional

from execution.fees import recommend_fee_package

# Canonical router (MONEY PIPE)
from utils_pkg import router as jrouter  # MUST be the only swap path
from utils_pkg.log_compat import EventLogger, get_logger  # ✅ canonical logging contract

log: EventLogger = get_logger(__name__)

SOL_MINT = "So11111111111111111111111111111111111111112"


# -----------------------------------------------------------------------------
# Optional helpers (NOT swap): price + notify (best-effort)
# -----------------------------------------------------------------------------
_convert_sol_to_usd: Optional[Callable[[float], Any]]
_send_telegram: Optional[Callable[..., Any]]

try:
    from utils_pkg.core_utils import convert_sol_to_usd as _convert_sol_to_usd  # type: ignore
except Exception:  # pragma: no cover
    _convert_sol_to_usd = None

try:
    from utils_pkg.core_utils import send_telegram as _send_telegram  # type: ignore
except Exception:  # pragma: no cover
    _send_telegram = None

convert_sol_to_usd: Optional[Callable[[float], Any]] = _convert_sol_to_usd
send_telegram: Optional[Callable[..., Any]] = _send_telegram


# -----------------------------------------------------------------------------
# Env knobs (bounded, deterministic)
# -----------------------------------------------------------------------------
_TRUE = {"1", "true", "t", "yes", "y", "on"}


def _env_str(key: str, default: str) -> str:
    v = (os.getenv(key) or "").strip()
    return v if v else default


def _env_bool(key: str, default: bool) -> bool:
    v = (os.getenv(key) or "").strip().lower()
    if not v:
        return bool(default)
    return v in _TRUE


def _env_float(key: str, default: float) -> float:
    v = (os.getenv(key) or "").strip()
    try:
        return float(v) if v else float(default)
    except Exception:
        return float(default)


def _env_int(key: str, default: int) -> int:
    v = (os.getenv(key) or "").strip()
    try:
        return int(float(v)) if v else int(default)
    except Exception:
        return int(default)


def _clamp_int(v: int, lo: int, hi: int) -> int:
    return max(int(lo), min(int(hi), int(v)))


def _clamp_float(v: float, lo: float, hi: float) -> float:
    return max(float(lo), min(float(hi), float(v)))


# Support old env too (fraction), but canonicalize to bps:
# - JUPITER_MAX_SLIPPAGE = 0.005 (0.5%) -> 50 bps
_JUP_SLIPPAGE_FRACTION = _env_float("JUPITER_MAX_SLIPPAGE", 0.0)

SELL_BASE_SLIPPAGE_BPS = _clamp_int(_env_int("EXEC_SELL_BASE_SLIPPAGE_BPS", 120), 0, 25_000)
SELL_MAX_SLIPPAGE_BPS = _clamp_int(_env_int("EXEC_SELL_MAX_SLIPPAGE_BPS", 1200), 0, 25_000)

SELL_MIN_USD = _clamp_float(_env_float("EXEC_SELL_MIN_USD", 5.0), 0.0, 1e12)
SELL_MIN_TOKEN_UNITS = _clamp_float(_env_float("EXEC_SELL_MIN_TOKEN_UNITS", 0.0), 0.0, 1e30)

SELL_PREFLIGHT_QUOTE = _env_bool("EXEC_SELL_PREFLIGHT_QUOTE", False)
SELL_QUOTE_TIMEOUT_S = _clamp_float(_env_float("EXEC_SELL_QUOTE_TIMEOUT_S", 3.0), 0.1, 60.0)

PASS_PRIORITY_TO_ROUTER = _env_bool("EXEC_PASS_PRIORITY_TO_ROUTER", True)
SELL_ROUTER_TIMEOUT_S = _clamp_float(_env_float("EXEC_SELL_ROUTER_TIMEOUT_S", 20.0), 0.5, 120.0)

SELL_DISABLE_NOTIFY = _env_bool("EXEC_SELL_DISABLE_NOTIFY", False)

# “Urgency” used by execution.fees priority fee selection
SELL_URGENCY_SMALL = _env_str("EXEC_SELL_URGENCY_SMALL", "normal").lower()
SELL_URGENCY_MED = _env_str("EXEC_SELL_URGENCY_MED", "fast").lower()
SELL_URGENCY_LARGE = _env_str("EXEC_SELL_URGENCY_LARGE", "max").lower()
_ALLOWED_URGENCY = {"normal", "fast", "max"}


# -----------------------------------------------------------------------------
# Models
# -----------------------------------------------------------------------------
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


# -----------------------------------------------------------------------------
# Async helpers
# -----------------------------------------------------------------------------
async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


# -----------------------------------------------------------------------------
# Logging helpers (compatible with EventLogger or stdlib Logger)
# -----------------------------------------------------------------------------
def _log_event(name: str, fields: Mapping[str, Any]) -> None:
    try:
        ev = getattr(log, "event", None)
        if callable(ev):
            ev(name, **dict(fields))
            return
    except Exception:
        pass

    with contextlib.suppress(Exception):
        log.info("%s %s", name, dict(fields))


# -----------------------------------------------------------------------------
# Router signature / drift tolerance helpers
# -----------------------------------------------------------------------------
_PARAM_CACHE: dict[int, set[str]] = {}


def _get_params(fn: Any) -> set[str]:
    fid = id(fn)
    cached = _PARAM_CACHE.get(fid)
    if cached is not None:
        return cached
    try:
        params = set(inspect.signature(fn).parameters.keys())
    except Exception:
        params = set()
    _PARAM_CACHE[fid] = params
    return params


def _pick_router_fn(names: Sequence[str]) -> Any | None:
    for n in names:
        with contextlib.suppress(Exception):
            if hasattr(jrouter, n):
                return getattr(jrouter, n)
    return None


def _filtered_kwargs(fn: Any, candidate_kwargs: Mapping[str, Any]) -> dict[str, Any]:
    params = _get_params(fn)
    if not params:
        return {k: v for k, v in candidate_kwargs.items() if v is not None}
    return {k: v for k, v in candidate_kwargs.items() if (k in params and v is not None)}


# -----------------------------------------------------------------------------
# Conversions / sizing
# -----------------------------------------------------------------------------
def _slippage_bps(usd_to_sell: float, override_bps: int | None = None) -> int:
    cap = int(SELL_MAX_SLIPPAGE_BPS)

    if override_bps is not None:
        return _clamp_int(int(override_bps), 0, cap)

    if _JUP_SLIPPAGE_FRACTION and _JUP_SLIPPAGE_FRACTION > 0:
        bps = int(round(float(_JUP_SLIPPAGE_FRACTION) * 10_000.0))
        return _clamp_int(bps, 0, cap)

    base = int(SELL_BASE_SLIPPAGE_BPS)
    usd = float(max(0.0, usd_to_sell))
    extra = int(min(400, (usd / 2000.0) * 400.0))
    return _clamp_int(base + extra, 0, cap)


def _pick_urgency(usd_to_sell: float, override: str | None) -> str:
    if override:
        u = override.strip().lower()
        return u if u in _ALLOWED_URGENCY else "fast"

    if usd_to_sell < 150:
        return SELL_URGENCY_SMALL if SELL_URGENCY_SMALL in _ALLOWED_URGENCY else "normal"
    if usd_to_sell < 800:
        return SELL_URGENCY_MED if SELL_URGENCY_MED in _ALLOWED_URGENCY else "fast"
    return SELL_URGENCY_LARGE if SELL_URGENCY_LARGE in _ALLOWED_URGENCY else "max"


def _amount_raw_from_units(amount_units: float, decimals: int | None) -> int | None:
    if decimals is None:
        return None
    try:
        d = _clamp_int(int(decimals), 0, 18)
        if amount_units <= 0:
            return 0
        raw = int(amount_units * (10**d))
        return max(0, raw)
    except Exception:
        return None


def _parse_router_result(res: Any) -> tuple[bool, str | None, dict[str, Any]]:
    meta: dict[str, Any] = {}

    if isinstance(res, tuple) and len(res) >= 2:
        ok = bool(res[0])
        sig = res[1] if (isinstance(res[1], str) or res[1] is None) else str(res[1])
        if len(res) >= 3 and isinstance(res[2], dict):
            meta.update(res[2])
        return ok, sig, meta

    if isinstance(res, dict):
        ok = bool(res.get("ok", res.get("success", False)))
        sig_val = res.get("sig", res.get("signature"))
        sig = sig_val if (isinstance(sig_val, str) or sig_val is None) else str(sig_val)
        meta.update(
            {k: v for k, v in res.items() if k not in {"ok", "success", "sig", "signature"}}
        )
        return ok, sig, meta

    return bool(res), None, meta


# -----------------------------------------------------------------------------
# Optional preflight quote (best-effort, never blocks)
# -----------------------------------------------------------------------------
async def _router_quote_best_effort(
    *,
    input_mint: str,
    output_mint: str,
    amount_raw: int | None,
    amount_float: float | None,
    slippage_bps: int,
) -> dict[str, Any]:
    if not SELL_PREFLIGHT_QUOTE:
        return {}

    fn = _pick_router_fn(("get_sell_quote", "get_quote", "quote", "get_swap_quote"))
    if fn is None:
        return {}

    candidates: dict[str, Any] = {
        "mint": input_mint,
        "input_mint": input_mint,
        "in_mint": input_mint,
        "mint_in": input_mint,
        "output_mint": output_mint,
        "out_mint": output_mint,
        "mint_out": output_mint,
        "amount_raw": amount_raw,
        "amount": amount_raw,
        "amount_in": amount_raw,
        "amount_in_raw": amount_raw,
        "amount_float": amount_float,
        "amount_ui": amount_float,
        "amount_in_ui": amount_float,
        "slippage_bps": slippage_bps,
        "slippage": slippage_bps,
        "max_slippage_bps": slippage_bps,
    }

    try:
        call_kwargs = _filtered_kwargs(fn, candidates)
        if not call_kwargs:
            return {}
        res = await asyncio.wait_for(
            _maybe_await(fn(**call_kwargs)), timeout=float(SELL_QUOTE_TIMEOUT_S)
        )
        return {"quote": res}
    except Exception as e:
        return {"quote_error": str(e)}


# -----------------------------------------------------------------------------
# Router swap (the only money path)
# -----------------------------------------------------------------------------
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
    fn = _pick_router_fn(
        (
            "swap",
            "swap_token",
            "swap_exact_in",
            "swap_exact_input",
            "execute_swap",
            "swap_token_jupiter",
        )
    )
    if fn is None:
        return False, None, {"error": "router_missing_swap_fn"}

    candidates: dict[str, Any] = {
        "input_mint": input_mint,
        "in_mint": input_mint,
        "mint_in": input_mint,
        "mint": input_mint,
        "output_mint": output_mint,
        "out_mint": output_mint,
        "mint_out": output_mint,
        "amount_raw": amount_raw,
        "amount": amount_raw,
        "amount_in": amount_raw,
        "amount_in_raw": amount_raw,
        "amount_float": amount_float,
        "amount_ui": amount_float,
        "amount_in_ui": amount_float,
        "slippage_bps": slippage_bps,
        "slippage": slippage_bps,
        "max_slippage_bps": slippage_bps,
        "jito_tip_lamports": jito_tip_lamports,
        "tip_lamports": jito_tip_lamports,
        "cu_limit": cu_limit,
        "compute_unit_limit": cu_limit,
        "cu_price_microlamports": cu_price_microlamports,
        "compute_unit_price_microlamports": cu_price_microlamports,
    }

    try:
        call_kwargs = _filtered_kwargs(fn, candidates)

        if not PASS_PRIORITY_TO_ROUTER:
            for k in (
                "jito_tip_lamports",
                "tip_lamports",
                "cu_limit",
                "compute_unit_limit",
                "cu_price_microlamports",
                "compute_unit_price_microlamports",
            ):
                call_kwargs.pop(k, None)

        has_amount = any(
            k in call_kwargs
            for k in (
                "amount_raw",
                "amount",
                "amount_in",
                "amount_in_raw",
                "amount_float",
                "amount_ui",
                "amount_in_ui",
            )
        )
        if not has_amount:
            return False, None, {"error": "router_swap_missing_amount"}

        res = await asyncio.wait_for(
            _maybe_await(fn(**call_kwargs)), timeout=float(SELL_ROUTER_TIMEOUT_S)
        )
        ok, sig_out, meta = _parse_router_result(res)

        meta.setdefault("router_fn", getattr(fn, "__name__", "swap"))
        meta.setdefault(
            "router_args",
            {
                k: call_kwargs.get(k)
                for k in sorted(call_kwargs.keys())
                if k not in {"wallet", "keypair", "private_key", "secret", "seed"}
            },
        )
        return ok, sig_out, meta
    except Exception as e:
        return False, None, {"error": f"router_swap_exception:{e}"}


# -----------------------------------------------------------------------------
# Notifications (best-effort)
# -----------------------------------------------------------------------------
async def _notify_best_effort(msg: str) -> None:
    if SELL_DISABLE_NOTIFY:
        return
    if send_telegram is None:
        return
    with contextlib.suppress(Exception):
        await _maybe_await(send_telegram(msg))


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------
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
    t0 = time.time()
    meta: dict[str, Any] = {
        "mint": mint,
        "usd_to_sell": float(usd_to_sell),
        "reason": reason,
        "last_px_sol_per_token": float(last_px_sol_per_token or 0.0),
        "ts": t0,
    }

    usd = float(usd_to_sell)
    if usd <= 0:
        meta["error"] = "usd_to_sell<=0"
        return False, None, meta
    if usd < float(SELL_MIN_USD):
        meta["error"] = "usd_to_sell<min"
        meta["min_usd"] = float(SELL_MIN_USD)
        return False, None, meta

    px_sol = float(last_px_sol_per_token or 0.0)
    if px_sol <= 0:
        meta["error"] = "last_px_invalid"
        return False, None, meta

    if convert_sol_to_usd is None:
        meta["error"] = "convert_sol_to_usd_unavailable"
        return False, None, meta

    usd_per_sol_raw = await _maybe_await(convert_sol_to_usd(1.0))
    try:
        usd_per_sol = float(usd_per_sol_raw or 0.0)
    except Exception:
        usd_per_sol = 0.0

    meta["usd_per_sol"] = usd_per_sol
    if usd_per_sol <= 0:
        meta["error"] = "usd_per_sol_unknown"
        return False, None, meta

    sol_to_sell = usd / usd_per_sol
    token_units = sol_to_sell / px_sol

    meta["sol_to_sell_est"] = sol_to_sell
    meta["token_units_est"] = token_units

    if SELL_MIN_TOKEN_UNITS and token_units < float(SELL_MIN_TOKEN_UNITS):
        meta["error"] = "token_units<min"
        meta["min_token_units"] = float(SELL_MIN_TOKEN_UNITS)
        return False, None, meta

    amount_raw = _amount_raw_from_units(token_units, token_decimals)
    if amount_raw is not None:
        meta["token_decimals"] = int(token_decimals) if token_decimals is not None else None
        meta["amount_raw"] = int(amount_raw)
        if amount_raw <= 0:
            meta["error"] = "amount_raw<=0_dust"
            return False, None, meta

    slip_bps = _slippage_bps(usd, override_bps=slippage_bps)
    urg = _pick_urgency(usd, urgency)
    meta["slippage_bps"] = int(slip_bps)
    meta["urgency"] = urg

    fee_pkg = recommend_fee_package(recent_tips_lamports=recent_jito_tips_lamports, urgency=urg)
    meta["fee"] = {
        "jito_tip_lamports": int(fee_pkg.tip_lamports),
        "cu_limit": int(fee_pkg.priority.cu_limit),
        "cu_price_microlamports": int(fee_pkg.priority.cu_price_microlamports),
        "est_total_fee_lamports": int(fee_pkg.est_total_fee_lamports),
        "notes": list(fee_pkg.notes),
    }

    meta.update(
        await _router_quote_best_effort(
            input_mint=mint,
            output_mint=SOL_MINT,
            amount_raw=amount_raw,
            amount_float=None if amount_raw is not None else float(token_units),
            slippage_bps=slip_bps,
        )
    )

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

    try:
        scrub = {k: v for k, v in meta.items() if k != "quote"}
        _log_event("SELL_TOKEN_USD", scrub)
    except Exception:
        with contextlib.suppress(Exception):
            log.info(
                "SELL_TOKEN_USD %s",
                {"ok": ok, "mint": mint, "usd": usd, "sig": sig, "reason": reason},
            )

    if ok:
        await _notify_best_effort(
            f"🏁 Sold {mint[:6]}… • ${usd:,.2f} • reason={reason} • slip={slip_bps}bps • tip={int(fee_pkg.tip_lamports)}\nTx: {sig}"
        )

    return ok, sig, meta


__all__ = ["SellRequest", "sell_token_usd"]
