# scripts/entrypoint.py
from __future__ import annotations

import asyncio
import math
import os
import signal
import sys
from contextlib import suppress
from datetime import datetime, timezone
from typing import Optional

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
# Autosizer
# ──────────────────────────────────────────────────────────────────────────────
async def autosize_once() -> float:
    """
    Compute PF_TRADE_SOL from wallet balance + guardrails, then set in env.
    Sizing = balance(SOL) * PF_TRADE_RISK_PCT, clamped & stepped.
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

    if not enable:
        # Respect an explicitly set PF_TRADE_SOL if autosize is disabled.
        val = _envf("PF_TRADE_SOL", max(min_sol, 0.01))
        val = _clamp(val, min_sol, max_sol)
        if step_sol > 0:
            val = max(min_sol, _round_down_step(val, step_sol))
        os.environ["PF_TRADE_SOL"] = _fmt_amt(val)
        print(f"[autosize] disabled -> PF_TRADE_SOL={os.environ['PF_TRADE_SOL']}", flush=True)
        return val

    # Lazy import to avoid loading Solana libs before we need them
    from utils_pkg.solana_wallet import load_keypair, get_balance_sol

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
        os.environ["PF_TRADE_SOL"] = _fmt_amt(fallback)
        print(f"[autosize] balance fetch failed: {e!r} -> keeping PF_TRADE_SOL={os.environ['PF_TRADE_SOL']}", flush=True)
        return fallback

    # Raw sizing (SOL terms), clamp, then step
    raw = max(0.0, bal_sol * risk_pct)
    sized = _clamp(raw, min_sol, max_sol)
    sized = max(min_sol, _round_down_step(sized, step_sol)) if step_sol > 0 else sized
    if sized <= 0 and min_sol > 0:
        sized = min_sol

    os.environ["PF_TRADE_SOL"] = _fmt_amt(sized)
    print(
        f"[autosize] bal_sol={bal_sol:.4f}, risk={risk_pct*100:.2f}% -> PF_TRADE_SOL={os.environ['PF_TRADE_SOL']}",
        flush=True,
    )
    return sized


async def _autosize_loop(refresh_sec: int, stop_evt: asyncio.Event):
    while not stop_evt.is_set():
        try:
            await asyncio.wait_for(stop_evt.wait(), timeout=max(1, refresh_sec))
        except asyncio.TimeoutError:
            # Timer fired
            with suppress(Exception):
                await autosize_once()


# ──────────────────────────────────────────────────────────────────────────────
# Boot banner
# ──────────────────────────────────────────────────────────────────────────────
def _print_banner() -> None:
    now = datetime.now(timezone.utc).isoformat()
    print(f"\n=== {BANNER} boot @ {now} ===", flush=True)
    print("PYTHONUNBUFFERED =", os.getenv("PYTHONUNBUFFERED", ""))
    print("DRY_RUN          =", os.getenv("DRY_RUN", ""))
    print("PF_PP_KEY        =", _mask(os.getenv("PF_PP_KEY", "")))
    print("PF_PP_ACCOUNT    =", os.getenv("PF_PP_ACCOUNT", ""))
    print("PF_PP_MINTS      =", os.getenv("PF_PP_MINTS"))
    print("SOL_RPC_URL      =", _mask(os.getenv("SOL_RPC_URL", "")))
    print("HELIUS_API_KEY   =", _mask(os.getenv("HELIUS_API_KEY", "")))
    print("TELEGRAM_CHAT_ID =", _mask(os.getenv("TELEGRAM_CHAT_ID", ""), 6))

    # Nice-to-have trading knobs (if present)
    for k in ("PF_SLIPPAGE_BPS", "PF_PRIORITY_SOL", "PF_POOLS"):
        if k in os.environ:
            print(f"{k:16s} =", os.getenv(k, ""))

    print("— guardrails —")
    for k in (
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
    ):
        print(f"{k:27s}= {os.getenv(k, '')}")
    print("====================================================", flush=True)


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
async def _run_main_with_shutdown() -> int:
    """
    Runs autosizer (once + optional loop) and then hands off to scripts.main.main().
    Handles SIGINT/SIGTERM and cleans up.
    """
    stop_evt = asyncio.Event()
    autosize_task: Optional[asyncio.Task] = None

    # Wire signals for graceful shutdown
    def _sig_handler(signame: str):
        print(f"[entrypoint] received {signame} → shutting down…", flush=True)
        stop_evt.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, _sig_handler, sig.name)

    # One shot before we start
    await autosize_once()

    # Optional refresh loop
    refresh = _envi("PF_TRADE_AUTOSIZE_REFRESH_SEC", 0)
    if refresh > 0:
        autosize_task = asyncio.create_task(_autosize_loop(refresh, stop_evt))

    # Hand off to trading loop
    rc = 0
    try:
        from scripts.main import main as run_main
        await run_main(stop_evt=stop_evt)  # pass the stop signal if your main supports it
    except TypeError:
        # Back-compat: if scripts.main.main() doesn't accept stop_evt
        from scripts.main import main as run_main_noarg
        await run_main_noarg()
    except asyncio.CancelledError:
        rc = 130
    except Exception as e:
        print(f"[entrypoint] fatal error in main: {e!r}", flush=True)
        rc = 1
    finally:
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
    _print_banner()
    rc = await _run_main_with_shutdown()
    # Exit code is informative when running under supervisors
    if rc:
        # Make sure logs flush before supervisor kills us
        await asyncio.sleep(0.05)
        sys.exit(rc)


if __name__ == "__main__":
    _maybe_set_windows_policy()
    _maybe_enable_uvloop()
    asyncio.run(main())
