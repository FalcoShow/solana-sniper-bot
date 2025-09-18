# scripts/entrypoint.py
from __future__ import annotations

import asyncio
import inspect
import logging
import math
import os
import signal
import sys
from contextlib import suppress
from datetime import datetime, timezone
from typing import Optional

from logging_setup import (
    get_logger,
    start_global_heartbeat,
    bump,
    install_excepthook,
)

BANNER = "sniper-entrypoint v1"


# ──────────────────────────────────────────────────────────────────────────────
# Small helpers
# ──────────────────────────────────────────────────────────────────────────────
def _envf(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return float(default)


def _envi(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return int(default)


def _envb(name: str, default: bool = False) -> bool:
    v = (os.getenv(name, "") or "").strip().lower()
    if not v:
        return default
    return v in {"1", "true", "yes", "y", "on"}


def _round_down_step(x: float, step: float) -> float:
    if step <= 0:
        return x
    # floor to the nearest step
    return math.floor(x / step) * step


def _mask(s: str, head: int = 4) -> str:
    return (s[:head] + "...") if s else ""


def _fmt_amt(x: float) -> str:
    # Nice formatting without trailing zeros
    return f"{x:.9f}".rstrip("0").rstrip(".")


def _clamp(x: float, lo: float, hi: float) -> float:
    if lo > hi:
        lo, hi = hi, lo
    return max(lo, min(hi, x))


# ──────────────────────────────────────────────────────────────────────────────
# Logger (global)
# ──────────────────────────────────────────────────────────────────────────────
LOG_SERVICE = os.getenv("PF_SERVICE", "sniper")
LOG_VERSION = os.getenv("PF_VERSION", "v1")
log = get_logger(service=LOG_SERVICE, version=LOG_VERSION)
install_excepthook(log)  # log uncaught exceptions as JSON


# ──────────────────────────────────────────────────────────────────────────────
# Autosizer
# ──────────────────────────────────────────────────────────────────────────────
async def autosize_once() -> float:
    """
    Compute PF_TRADE_SOL from wallet balance + guardrails, then set in env.
    Sizing = balance(SOL) * PF_TRADE_RISK_PCT, clamped & stepped.
    Emits a compact event only when value changes; otherwise rate-limited debug.
    """
    enable = _envb("PF_TRADE_AUTOSIZE", False)
    # Guardrails / tuning (with sanity)
    risk_pct = _envf("PF_TRADE_RISK_PCT", 0.03)  # 3% of balance
    risk_pct = _clamp(risk_pct, 0.0, 1.0)

    min_sol = _envf("PF_TRADE_MIN_SOL", 0.005)
    max_sol = _envf("PF_TRADE_MAX_SOL", 0.25)
    if max_sol < min_sol:
        # Fix inverted bounds silently
        min_sol, max_sol = max_sol, min_sol

    step_sol = max(0.0, _envf("PF_TRADE_STEP_SOL", 0.001))

    def _finalize(val: float, why: str, **extra):
        os.environ["PF_TRADE_SOL"] = _fmt_amt(val)
        # Only emit an update event if PF_TRADE_SOL actually changed
        prev = extra.pop("prev", None)
        if prev is None or _fmt_amt(prev) != os.environ["PF_TRADE_SOL"]:
            log.event(
                "AUTOSIZE_UPDATE",
                bal_sol=extra.get("bal_sol"),
                risk_pct=risk_pct,
                min_sol=min_sol,
                max_sol=max_sol,
                step_sol=step_sol,
                trade_sol=val,
                why=why,
            )
            bump("autosize_updates")
        else:
            # Rate-limit 'same' lines
            log.every(
                "autosize.same",
                n=max(5, _envi("PF_TRADE_AUTOSIZE_EVERY_N", 10)),
                level=logging.DEBUG,
                msg="AUTOSIZE_SAME",
                bal_sol=extra.get("bal_sol"),
                risk_pct=risk_pct,
                trade_sol=val,
            )
            bump("autosize_ticks")

    if not enable:
        # Respect an explicitly set PF_TRADE_SOL if autosize is disabled.
        val = _envf("PF_TRADE_SOL", max(min_sol, 0.01))
        val = _clamp(val, min_sol, max_sol)
        if step_sol > 0:
            val = max(min_sol, _round_down_step(val, step_sol))
        prev = _envf("PF_TRADE_SOL", val)
        _finalize(val, why="disabled", prev=prev, bal_sol=None)
        return val

    # Lazy import to avoid loading Solana libs before we need them
    try:
        from utils_pkg.solana_wallet import load_keypair, get_balance_sol
    except Exception as e:
        # If wallet lib is missing during early boot, fall back
        fallback = _envf("PF_TRADE_SOL", min_sol)
        fallback = _clamp(fallback, min_sol, max_sol)
        if step_sol > 0:
            fallback = max(min_sol, _round_down_step(fallback, step_sol))
        prev = _envf("PF_TRADE_SOL", fallback)
        log.once("autosize.wallet_import_fail", ttl=60, level=logging.WARNING, msg="AUTOSIZE_WALLET_IMPORT_FAIL", error=str(e))
        _finalize(fallback, why="wallet-import-fail", prev=prev, bal_sol=None)
        return fallback

    # Get balance
    kp = load_keypair()
    try:
        bal_sol = await get_balance_sol(str(kp.pubkey()))
    except Exception as e:
        # On failure, keep previous PF_TRADE_SOL if present, else min_sol
        fallback = _envf("PF_TRADE_SOL", min_sol)
        fallback = _clamp(fallback, min_sol, max_sol)
        if step_sol > 0:
            fallback = max(min_sol, _round_down_step(fallback, step_sol))
        prev = _envf("PF_TRADE_SOL", fallback)
        log.once("autosize.balance_fetch_fail", ttl=30, level=logging.WARNING, msg="AUTOSIZE_BALANCE_FAIL", error=str(e))
        _finalize(fallback, why="balance-fetch-fail", prev=prev, bal_sol=None)
        return fallback

    # Raw sizing (SOL terms), clamp, then step
    raw = max(0.0, bal_sol * risk_pct)
    sized = _clamp(raw, min_sol, max_sol)
    sized = max(min_sol, _round_down_step(sized, step_sol)) if step_sol > 0 else sized
    if sized <= 0 and min_sol > 0:
        sized = min_sol

    prev = _envf("PF_TRADE_SOL", sized)
    _finalize(sized, why="ok", prev=prev, bal_sol=bal_sol)
    return sized


async def _autosize_loop(refresh_sec: int, stop_evt: asyncio.Event):
    while not stop_evt.is_set():
        try:
            await asyncio.wait_for(stop_evt.wait(), timeout=max(1, refresh_sec))
        except asyncio.TimeoutError:
            with suppress(Exception):
                await autosize_once()


# ──────────────────────────────────────────────────────────────────────────────
# Boot banner (structured)
# ──────────────────────────────────────────────────────────────────────────────
def _boot_event() -> None:
    now = datetime.now(timezone.utc).isoformat()
    guardrails_keys = (
        "PF_TRADE_RISK_PCT",
        "PF_TRADE_MIN_SOL",
        "PF_TRADE_MAX_SOL",
        "PF_TRADE_STEP_SOL",
        "PF_DAILY_MAX_DRAWDOWN_PCT",
        "PF_DAILY_MAX_LOSSES",
        "PF_MAX_CONCURRENT",
        "PF_COOLDOWN_SEC",
        "PF_TRADE_AUTOSIZE",
        "PF_TRADE_AUTOSIZE_REFRESH_SEC",
    )
    knobs = {k: os.getenv(k) for k in ("PF_SLIPPAGE_BPS", "PF_PRIORITY_SOL", "PF_POOLS") if k in os.environ}

    log.event(
        "BOOT",
        banner=BANNER,
        time=now,
        py_unbuffered=os.getenv("PYTHONUNBUFFERED", ""),
        dry_run=os.getenv("DRY_RUN", ""),
        pf_service=LOG_SERVICE,
        pf_version=LOG_VERSION,
        pf_pp_key=_mask(os.getenv("PF_PP_KEY", "")),
        pf_pp_account=os.getenv("PF_PP_ACCOUNT", ""),
        pf_pp_mints=os.getenv("PF_PP_MINTS"),
        sol_rpc_url=_mask(os.getenv("SOL_RPC_URL", "")),
        helius_api_key=_mask(os.getenv("HELIUS_API_KEY", "")),
        telegram_chat_id=_mask(os.getenv("TELEGRAM_CHAT_ID", ""), 6),
        guardrails={k: os.getenv(k, "") for k in guardrails_keys},
        knobs=knobs,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
async def _run_handoff(stop_evt: asyncio.Event) -> int:
    """
    Hand off to scripts.main.main; works for async or sync variants, with or without a stop_evt arg.
    """
    try:
        from scripts.main import main as run_main
    except Exception as e:  # pragma: no cover
        log.event("HANDOFF_IMPORT_FAIL", error=str(e))
        return 1

    log.event("HANDOFF_START")
    try:
        # Try with stop_evt kwarg
        try:
            result = run_main(stop_evt=stop_evt)
        except TypeError:
            # Try without args
            result = run_main()

        # If coroutine/awaitable, await it; otherwise assume sync completion
        if inspect.isawaitable(result):
            await result

        log.event("HANDOFF_DONE")
        return 0
    except asyncio.CancelledError:
        log.event("HANDOFF_CANCELLED")
        return 130
    except Exception as e:
        log.event("HANDOFF_ERROR", error=str(e))
        return 1


async def _run_main_with_shutdown() -> int:
    """
    Runs autosizer (once + optional loop) and then hands off to scripts.main.main().
    Handles SIGINT/SIGTERM and cleans up.
    """
    stop_evt = asyncio.Event()
    autosize_task: Optional[asyncio.Task] = None

    # Wire signals for graceful shutdown
    def _sig_handler(signame: str):
        log.event("SIGNAL", signal=signame, action="shutdown")
        stop_evt.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, _sig_handler, sig.name)

    # One shot autosize before we start
    await autosize_once()

    # Optional refresh loop
    refresh = _envi("PF_TRADE_AUTOSIZE_REFRESH_SEC", 0)
    if refresh > 0:
        autosize_task = asyncio.create_task(_autosize_loop(refresh, stop_evt))

    # Hand off to trading loop
    rc = await _run_handoff(stop_evt)

    # Cleanup
    stop_evt.set()
    if autosize_task:
        autosize_task.cancel()
        with suppress(Exception):
            await autosize_task

    return rc


def _maybe_set_windows_policy():
    # On Windows, prefer ProactorEventLoop for subprocess/socket compat (Py3.8+ default),
    # but don't explode if policy is already set.
    if sys.platform.startswith("win"):
        with suppress(Exception):
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())  # type: ignore[attr-defined]


def _maybe_enable_uvloop():
    # uvloop is great on Linux; optional dependency.
    if sys.platform.startswith("linux"):
        with suppress(Exception):
            import uvloop  # type: ignore
            uvloop.install()


async def main():
    _boot_event()

    # Heartbeat every PF_HEARTBEAT_SEC (default 30s)
    hb_sec = _envi("PF_HEARTBEAT_SEC", 30)
    start_global_heartbeat(log, interval=max(5, hb_sec))

    rc = await _run_main_with_shutdown()

    # Exit code is informative when running under supervisors
    if rc:
        # small delay so logs flush
        await asyncio.sleep(0.05)
        log.event("EXIT", code=rc)
        sys.exit(rc)
    else:
        log.event("EXIT", code=0)


if __name__ == "__main__":
    _maybe_set_windows_policy()
    _maybe_enable_uvloop()
    asyncio.run(main())
