# scripts/sell_adapter.py
from __future__ import annotations

"""
scripts/sell_adapter.py — God-Tier Sell Adapter (SAFE, canonical-router driven)
===============================================================================

Role
----
A *tooling adapter* used by scripts (and optionally by internal callers) to
execute a single SELL through the canonical router path.

Hard guarantees
---------------
- Router is the single source of truth for mint normalization, quotes, swap building/sending.
- Sells are allowed even when buy-side trading is halted (risk reduction), unless an
  explicit "full halt" policy exists in your circuit breaker layer.
- Never prints secrets; redacts env-backed sensitive values from errors when possible.
- Signature-drift tolerant calls into router / core utils.

Default safety
--------------
- Respects DRYRUN env (DRYRUN/PF_DRYRUN/BUY_DRYRUN) and explicit dry_run param.
- If dry-run, will quote (best-effort) and emit telemetry/log, but will not broadcast.

Public API (backwards compatible)
---------------------------------
async def execute_sell(
    mint: str,
    symbol: str,
    token_amount: float,
    slippage: float = 0.005,
    *,
    reason: str = "sell_adapter",
    dry_run: bool = False,
    **kwargs,
) -> bool
"""

import contextlib
import importlib
import inspect
import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Any


# -----------------------------------------------------------------------------
# Logger (canonical root logging_setup.py if present)
# -----------------------------------------------------------------------------
def _get_logger() -> logging.Logger:
    with contextlib.suppress(Exception):
        from logging_setup import get_logger  # type: ignore

        lg = get_logger()
        if lg:
            return lg
    return logging.getLogger("sell_adapter")


logger = _get_logger()


def _log_event(level: str, msg: str, **fields: Any) -> None:
    """
    Use structured logger if available; fall back to std logging.
    """
    try:
        if hasattr(logger, "event") and callable(logger.event):
            # project shim
            logger.event(msg, **fields)
            return
    except Exception:
        pass

    # fallback
    line = msg
    if fields:
        # minimal stable formatting
        line += " " + " ".join(f"{k}={_sanitize_text(v)}" for k, v in fields.items())
    lvl = (level or "info").lower().strip()
    fn = getattr(logger, lvl, None) if hasattr(logger, lvl) else None
    if callable(fn):
        fn(line)
    else:
        logger.info(line)


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


def _effective_dryrun(explicit: bool) -> bool:
    if explicit:
        return True
    # Common project flags
    for k in ("DRYRUN", "PF_DRYRUN", "BUY_DRYRUN", "SELL_DRY_RUN", "SELL_DRYRUN"):
        if _envb(k, False):
            return True
    return False


# -----------------------------------------------------------------------------
# Redaction / safety
# -----------------------------------------------------------------------------
_URL_CRED_RE = re.compile(r"(://)([^/@\s]+)@")
_LONG_TOKENISH_RE = re.compile(r"\b[A-Za-z0-9_\-]{28,}\b")


def _sanitize_text(x: Any) -> str:
    if x is None:
        return ""
    s = str(x)
    s = _URL_CRED_RE.sub(r"\1***@", s)

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

    if _envb("SELL_ADAPTER_MASK_LONG_TOKENS", True):
        s = _LONG_TOKENISH_RE.sub("***", s)

    return s


def _ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


# -----------------------------------------------------------------------------
# Circuit breakers (optional)
# -----------------------------------------------------------------------------
async def _cb_trading_halted(*_a: Any, **_k: Any) -> bool:
    return False


async def _cb_record_error(*_a: Any, **_k: Any) -> None:
    return None


async def _cb_record_pnl(*_a: Any, **_k: Any) -> None:
    return None


with contextlib.suppress(Exception):
    from utils_pkg.circuit_breakers import (  # type: ignore
        record_error_and_maybe_halt as _cb_record_error,  # type: ignore
        record_pnl_and_maybe_halt as _cb_record_pnl,  # type: ignore
        trading_halted as _cb_trading_halted,  # type: ignore
    )


# -----------------------------------------------------------------------------
# Core utils (optional, best-effort)
# -----------------------------------------------------------------------------
CU: Any = None
with contextlib.suppress(Exception):
    CU = importlib.import_module("utils_pkg.core_utils")


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
# Router access (canonical)
# -----------------------------------------------------------------------------
def _router_mod() -> Any:
    return importlib.import_module("utils_pkg.router")


def _mints_mod() -> Any:
    return importlib.import_module("utils_pkg.mints")


def _get_mint_consts() -> tuple[str, str, str]:
    sol = "So11111111111111111111111111111111111111112"
    usdc = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    usdt = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
    with contextlib.suppress(Exception):
        m = _mints_mod()
        sol = str(getattr(m, "SOL_MINT", sol) or sol)
        usdc = str(getattr(m, "USDC_MINT", usdc) or usdc)
        usdt = str(getattr(m, "USDT_MINT", usdt) or usdt)
    return sol, usdc, usdt


SOL_MINT, USDC_MINT, USDT_MINT = _get_mint_consts()


def _slippage_to_bps(slippage: float) -> int:
    """
    Accepts fraction (0.005 => 50 bps). Clamps to sane range.
    """
    try:
        bps = int(round(float(slippage) * 10_000))
    except Exception:
        bps = 50
    return int(max(1, min(5000, bps)))


def _clamp_decimals(d: Any, fallback: int) -> int:
    try:
        di = int(d)
    except Exception:
        di = int(fallback)
    return int(max(0, min(18, di)))


async def _normalize_mint(mint_in: str) -> str:
    mint_in = str(mint_in or "").strip()
    if not mint_in:
        return ""

    # Prefer router normalization for pump ids
    with contextlib.suppress(Exception):
        r = _router_mod()
        for nm in ("normalize_pump_id_to_mint", "normalize_mint", "try_normalize_mint"):
            fn = getattr(r, nm, None)
            if callable(fn):
                out = await _maybe_await(fn(mint_in))
                if out:
                    return str(out).strip()

    # Fall back to mints module
    with contextlib.suppress(Exception):
        m = _mints_mod()
        for nm in ("try_normalize_mint", "normalize_mint"):
            fn = getattr(m, nm, None)
            if callable(fn):
                out = fn(mint_in)
                if out:
                    return str(out).strip()

    # Common shorthands
    if mint_in.upper() == "SOL":
        return SOL_MINT
    if mint_in.upper() == "USDC":
        return USDC_MINT
    if mint_in.upper() == "USDT":
        return USDT_MINT

    return mint_in


async def _get_decimals(mint: str) -> int:
    # Router first
    with contextlib.suppress(Exception):
        r = _router_mod()
        fn = getattr(r, "get_decimals", None)
        if callable(fn):
            v = await _maybe_await(fn(mint))
            if v is not None:
                return _clamp_decimals(v, fallback=9)

    # Mints module
    with contextlib.suppress(Exception):
        m = _mints_mod()
        fn2 = getattr(m, "get_decimals", None)
        if callable(fn2):
            v = await _maybe_await(fn2(mint))
            if v is not None:
                return _clamp_decimals(v, fallback=9)

    # Heuristic
    if mint in (USDC_MINT, USDT_MINT):
        return 6
    return 9


async def _get_sell_quote(mint: str, amount_raw: int, slippage_bps: int) -> dict[str, Any] | None:
    """
    Best-effort quote; shape depends on router implementation.
    """
    with contextlib.suppress(Exception):
        r = _router_mod()
        fn = getattr(r, "get_sell_quote", None)
        if callable(fn):
            q = await _call(
                fn,
                **_filter_kwargs(
                    fn,
                    {
                        "mint": mint,
                        "amount_raw": amount_raw,
                        "slippage_bps": slippage_bps,
                        "slippageBps": slippage_bps,
                    },
                ),
            )
            if isinstance(q, dict):
                return q

    # Fallback: try generic quote names if present
    with contextlib.suppress(Exception):
        r = _router_mod()
        for nm in ("get_quote", "quote", "swap_quote", "jup_quote", "jupiter_quote"):
            fn2 = getattr(r, nm, None)
            if not callable(fn2):
                continue
            # inputMint=mint outputMint=SOL amount=amount_raw
            q2 = await _call(
                fn2,
                **_filter_kwargs(
                    fn2,
                    {
                        "inputMint": mint,
                        "outputMint": SOL_MINT,
                        "amount": int(amount_raw),
                        "slippageBps": int(slippage_bps),
                        "slippage_bps": int(slippage_bps),
                    },
                ),
            )
            if isinstance(q2, dict):
                return q2
    return None


async def _get_price_ui(mint: str) -> float | None:
    with contextlib.suppress(Exception):
        r = _router_mod()
        for nm in ("get_price_ui", "get_price", "price_ui", "price"):
            fn = getattr(r, nm, None)
            if callable(fn):
                v = await _maybe_await(fn(mint))
                if v is None:
                    continue
                return float(v)
    return None


@dataclass
class SellExecResult:
    ok: bool
    tx_sig: str = ""
    meta: dict[str, Any] = None  # type: ignore


async def _sell_via_router(
    *,
    mint: str,
    amount_raw: int,
    slippage_bps: int,
    priority_sol: float,
) -> SellExecResult:
    """
    Calls router sell implementation; handles return-shape drift.
    Expected shapes:
      - (ok: bool, sig: str)
      - {"ok": bool, "sig"/"signature": str, ...}
      - sig: str (truthy => ok)
    """
    r = _router_mod()

    # Prefer sell_via_lightning (project standard)
    fn = getattr(r, "sell_via_lightning", None)
    cand_names = ("sell_via_lightning", "sell", "swap_sell", "sell_swap", "execute_sell")
    if not callable(fn):
        fn = None
        for nm in cand_names:
            f = getattr(r, nm, None)
            if callable(f):
                fn = f
                break
    if not callable(fn):
        return SellExecResult(ok=False, tx_sig="", meta={"err": "router_sell_fn_missing"})

    kw = {
        "mint": mint,
        "amount_raw": int(amount_raw),
        "slippage_bps": int(slippage_bps),
        "slippageBps": int(slippage_bps),
        "priority_sol": float(priority_sol),
        # legacy keys some impls use:
        "prioritizationFeeLamports": None,
        "computeUnitPriceMicroLamports": None,
    }
    fkw = _filter_kwargs(fn, dict(kw))

    res = await _call(fn, **fkw)

    if isinstance(res, tuple) and len(res) >= 2:
        return SellExecResult(
            ok=bool(res[0]),
            tx_sig=str(res[1] or ""),
            meta={"via": getattr(fn, "__name__", "fn"), "shape": "tuple"},
        )
    if isinstance(res, dict):
        sig = res.get("sig") or res.get("signature") or res.get("tx_sig") or res.get("tx") or ""
        ok = bool(res.get("ok", bool(sig)))
        meta = dict(res)
        return SellExecResult(
            ok=ok,
            tx_sig=str(sig or ""),
            meta={
                "via": getattr(fn, "__name__", "fn"),
                "shape": "dict",
                "keys": list(meta.keys())[:20],
            },
        )
    if isinstance(res, str):
        return SellExecResult(
            ok=bool(res.strip()),
            tx_sig=res.strip(),
            meta={"via": getattr(fn, "__name__", "fn"), "shape": "str"},
        )
    return SellExecResult(
        ok=bool(res),
        tx_sig="",
        meta={"via": getattr(fn, "__name__", "fn"), "shape": type(res).__name__},
    )


# -----------------------------------------------------------------------------
# Telemetry + trade logging (best-effort)
# -----------------------------------------------------------------------------
async def _maybe_tele(text: str) -> None:
    """
    Best-effort Telegram/alerts:
      - core_utils.send_telegram(msg)
      - core_utils.alerts.send_telegram(msg)
      - core_utils.alert_manager.send(...)
    """
    msg = _sanitize_text(text)
    if not msg:
        return

    if CU is None:
        return

    # send_telegram
    with contextlib.suppress(Exception):
        fn = getattr(CU, "send_telegram", None)
        if callable(fn):
            await _maybe_await(fn(msg))
            return

    # alerts object
    with contextlib.suppress(Exception):
        alerts = getattr(CU, "alerts", None)
        fn2 = getattr(alerts, "send_telegram", None) if alerts is not None else None
        if callable(fn2):
            await _maybe_await(fn2(msg))
            return

    # alert_manager
    with contextlib.suppress(Exception):
        am = getattr(CU, "alert_manager", None)
        fn3 = getattr(am, "send", None) if am is not None else None
        if callable(fn3):
            await _maybe_await(fn3(msg))
            return


async def _log_trade_event(payload: dict[str, Any]) -> None:
    """
    Best-effort event log:
      - utils_pkg.data_logger.log_trade_event / log_trade / log_json
      - core_utils.log_trade_event (legacy)
    """
    p = dict(payload or {})
    p["ts"] = p.get("ts") or time.time()

    # data_logger preferred
    with contextlib.suppress(Exception):
        dl = importlib.import_module("utils_pkg.data_logger")
        for nm in ("log_trade_event", "log_trade", "log_json", "log_misc"):
            fn = getattr(dl, nm, None)
            if callable(fn):
                await _maybe_await(fn(p))
                return

    # legacy core_utils log_trade_event
    if CU is not None:
        with contextlib.suppress(Exception):
            fn2 = getattr(CU, "log_trade_event", None)
            if callable(fn2):
                await _maybe_await(fn2(**_filter_kwargs(fn2, p)))
                return


# -----------------------------------------------------------------------------
# Public entrypoint
# -----------------------------------------------------------------------------
async def execute_sell(
    mint: str,
    symbol: str,
    token_amount: float,
    slippage: float = 0.005,
    *,
    reason: str = "sell_adapter",
    dry_run: bool = False,
    **kwargs: Any,
) -> bool:
    """
    Sell `token_amount` (UI float) of `mint` into SOL using the canonical router.

    Notes:
    - `slippage` is a fraction: 0.005 = 0.5% (50 bps).
    - Supports overrides via kwargs:
        - amount_raw: int
        - decimals: int
        - slippage_bps: int
        - priority_sol: float
        - ui: dict
        - source: str
    """
    start = time.time()
    src = str(kwargs.get("source") or "scripts.sell_adapter")
    ui = kwargs.get("ui") if isinstance(kwargs.get("ui"), dict) else {}
    is_tool = bool(kwargs.get("is_tool", True))

    # Sells reduce risk: we generally DO NOT block sells if trading is halted.
    # If your circuit breaker supports a "full halt", you can wire it there.
    with contextlib.suppress(Exception):
        halted = await _cb_trading_halted(side="sell", source=src)  # type: ignore[arg-type]
        if bool(halted) and not _envb("ALLOW_SELLS_WHEN_HALTED", True):
            await _cb_record_error(reason="sell_blocked_halted", source=src)  # type: ignore[arg-type]
            _log_event(
                "warning", "sell blocked by full-halt policy", mint=mint, symbol=symbol, source=src
            )
            return False

    # Normalize mint
    try:
        norm_mint = await _normalize_mint(mint)
    except Exception as e:
        await _cb_record_error(reason="sell_normalize_fail", source=src)  # type: ignore[arg-type]
        _log_event(
            "warning", "sell normalize failed", mint=_sanitize_text(mint), err=_sanitize_text(e)
        )
        return False

    if not norm_mint:
        await _cb_record_error(reason="sell_empty_mint", source=src)  # type: ignore[arg-type]
        _log_event("warning", "sell empty mint after normalization", mint=_sanitize_text(mint))
        return False

    # Decide decimals and amount_raw
    decimals = kwargs.get("decimals")
    if decimals is None:
        with contextlib.suppress(Exception):
            decimals = await _get_decimals(norm_mint)
    dec = _clamp_decimals(decimals, fallback=(6 if norm_mint in (USDC_MINT, USDT_MINT) else 9))

    amount_raw = kwargs.get("amount_raw")
    if amount_raw is None:
        try:
            amount_raw = int(round(float(token_amount) * (10**dec)))
        except Exception:
            amount_raw = 0
    try:
        amount_raw_i = int(amount_raw)
    except Exception:
        amount_raw_i = 0

    if amount_raw_i <= 0:
        await _cb_record_error(reason="sell_bad_amount", source=src)  # type: ignore[arg-type]
        _log_event(
            "warning",
            "sell invalid amount",
            mint=norm_mint,
            symbol=symbol,
            token_amount=token_amount,
            decimals=dec,
        )
        return False

    # Slippage bps
    slippage_bps = kwargs.get("slippage_bps")
    if slippage_bps is None:
        # allow env override (router-level)
        if _envs("PF_SLIPPAGE_BPS", ""):
            slippage_bps = _envi("PF_SLIPPAGE_BPS", _slippage_to_bps(slippage))
        else:
            slippage_bps = _slippage_to_bps(slippage)
    try:
        slippage_bps_i = int(max(1, min(5000, int(slippage_bps))))
    except Exception:
        slippage_bps_i = 50

    # Priority fee
    priority_sol = kwargs.get("priority_sol")
    if priority_sol is None:
        priority_sol = _envf("PF_PRIORITY_SOL", _envf("JUP_PRIORITY_SOL", 0.002))
    try:
        priority_sol_f = float(max(0.0, float(priority_sol)))
    except Exception:
        priority_sol_f = 0.002

    # Quote (best-effort)
    quote: dict[str, Any] | None = None
    sol_out_est: float | None = None
    price_sol: float | None = None
    with contextlib.suppress(Exception):
        quote = await _get_sell_quote(norm_mint, amount_raw_i, slippage_bps_i)
        if isinstance(quote, dict):
            # common fields
            lamports_out = int(
                quote.get("lamports_out") or quote.get("outAmount") or quote.get("out_amount") or 0
            )
            if lamports_out > 0:
                sol_out_est = lamports_out / 1_000_000_000

    # Price estimate
    if token_amount > 0:
        if sol_out_est is not None:
            with contextlib.suppress(Exception):
                price_sol = float(sol_out_est) / float(token_amount)
        else:
            with contextlib.suppress(Exception):
                px = await _get_price_ui(norm_mint)
                price_sol = float(px) if px is not None else None

    eff_dry = _effective_dryrun(bool(dry_run))

    # DRYRUN path
    if eff_dry:
        sig = f"DRYRUN-{os.urandom(4).hex()}"
        _log_event(
            "info",
            "sell dryrun",
            mint=norm_mint,
            symbol=symbol,
            token_amount=float(token_amount),
            amount_raw=amount_raw_i,
            decimals=dec,
            slippage_bps=slippage_bps_i,
            sol_out_est=sol_out_est,
            reason=reason,
            source=src,
            sig=sig,
        )
        await _maybe_tele(
            f"🧪 [SELL DRYRUN] {symbol}\nMint: {norm_mint}\nAmount: {float(token_amount)}\n"
            + (f"Est SOL out: {sol_out_est:.9f}\n" if sol_out_est is not None else "")
            + f"Reason: {reason}\nTx: {sig}"
        )
        # optional logging
        await _log_trade_event(
            {
                "side": "sell",
                "mode": "dryrun",
                "symbol": symbol,
                "mint": norm_mint,
                "token_amount": float(token_amount),
                "amount_raw": int(amount_raw_i),
                "decimals": int(dec),
                "slippage_bps": int(slippage_bps_i),
                "priority_sol": float(priority_sol_f),
                "sol_out_est": sol_out_est,
                "price_sol": price_sol,
                "reason": reason,
                "source": src,
                "is_tool": bool(is_tool),
                "ui": ui,
                "sig": sig,
            }
        )
        return True

    # LIVE sell
    try:
        res = await _sell_via_router(
            mint=norm_mint,
            amount_raw=amount_raw_i,
            slippage_bps=slippage_bps_i,
            priority_sol=priority_sol_f,
        )
    except Exception as e:
        await _cb_record_error(reason="sell_swap_exception", source=src)  # type: ignore[arg-type]
        _log_event(
            "warning", "sell swap exception", mint=norm_mint, symbol=symbol, err=_sanitize_text(e)
        )
        return False

    if not res.ok:
        await _cb_record_error(reason="sell_swap_failed", source=src)  # type: ignore[arg-type]
        _log_event(
            "warning",
            "sell failed",
            mint=norm_mint,
            symbol=symbol,
            token_amount=float(token_amount),
            amount_raw=amount_raw_i,
            slippage_bps=slippage_bps_i,
            priority_sol=priority_sol_f,
            reason=reason,
            source=src,
            meta=_sanitize_text(res.meta),
        )
        return False

    tx_sig = str(res.tx_sig or "")
    elapsed_ms = int((time.time() - start) * 1000)

    # Best-effort PnL circuit breaker (estimate-based unless you have realized fills elsewhere)
    with contextlib.suppress(Exception):
        # If your project keeps entry price in redis/positions, compute est pnl for risk guard.
        if CU is not None and hasattr(CU, "get_entry_price"):
            entry_px = await _maybe_await(CU.get_entry_price(getattr(CU, "redis", None), norm_mint))
            if entry_px is not None and price_sol is not None:
                profit_sol = (float(price_sol) - float(entry_px)) * float(token_amount)
                # Optional USD conversion
                profit_usd = None
                if hasattr(CU, "convert_sol_to_usd"):
                    with contextlib.suppress(Exception):
                        profit_usd = await _maybe_await(CU.convert_sol_to_usd(float(profit_sol)))
                if profit_usd is not None:
                    try:
                        await _cb_record_pnl(pnl_usd=float(profit_usd))  # type: ignore[arg-type]
                    except TypeError:
                        # older sig might require redis first
                        await _cb_record_pnl(getattr(CU, "redis", None), pnl_usd=float(profit_usd))  # type: ignore[misc]

    # Log trade
    await _log_trade_event(
        {
            "side": "sell",
            "mode": "live",
            "symbol": symbol,
            "mint": norm_mint,
            "token_amount": float(token_amount),
            "amount_raw": int(amount_raw_i),
            "decimals": int(dec),
            "slippage_bps": int(slippage_bps_i),
            "priority_sol": float(priority_sol_f),
            "sol_out_est": sol_out_est,
            "price_sol": price_sol,
            "reason": reason,
            "source": src,
            "is_tool": bool(is_tool),
            "ui": ui,
            "sig": tx_sig,
            "elapsed_ms": elapsed_ms,
            "router_meta": _sanitize_text(res.meta),
        }
    )

    # Telegram / alerts
    with contextlib.suppress(Exception):
        out_line = f"\nEst SOL out: {sol_out_est:.9f}" if sol_out_est is not None else ""
        await _maybe_tele(
            f"✅ [SELL] {symbol}\nMint: {norm_mint}\nAmount: {float(token_amount)}{out_line}\nReason: {reason}\nTx: {tx_sig}"
        )

    _log_event(
        "info",
        "sell complete",
        mint=norm_mint,
        symbol=symbol,
        token_amount=float(token_amount),
        amount_raw=amount_raw_i,
        slippage_bps=slippage_bps_i,
        priority_sol=priority_sol_f,
        sol_out_est=sol_out_est,
        reason=reason,
        source=src,
        sig=tx_sig,
        elapsed_ms=elapsed_ms,
    )

    return True
