# scripts/entrypoint.py
import asyncio
import math
import os
from datetime import datetime, timezone

BANNER = "sniper-entrypoint v1"

# ---------- small helpers ----------
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

def _round_down_step(x: float, step: float) -> float:
    if step <= 0:
        return x
    # floor to the nearest step
    return math.floor(x / step) * step

def _mask(s: str, head: int = 4) -> str:
    return (s[:head] + "...") if s else ""

# ---------- autosizer ----------
async def autosize_once() -> float:
    """
    Compute PF_TRADE_SOL from wallet balance + guardrails, then set in env.
    Sizing = balance(SOL) * PF_TRADE_RISK_PCT, clamped & stepped.
    """
    enable = os.getenv("PF_TRADE_AUTOSIZE", "0") == "1"
    if not enable:
        # Respect an explicitly set PF_TRADE_SOL if autosize is disabled.
        val = _envf("PF_TRADE_SOL", 0.01)
        os.environ["PF_TRADE_SOL"] = f"{val:.9f}".rstrip("0").rstrip(".")
        print(f"[autosize] disabled -> PF_TRADE_SOL={os.environ['PF_TRADE_SOL']}", flush=True)
        return val

    from utils_pkg.solana_wallet import load_keypair, get_balance_sol

    # Guardrails / tuning
    risk_pct = _envf("PF_TRADE_RISK_PCT", 0.03)          # 3% of balance
    min_sol  = _envf("PF_TRADE_MIN_SOL", 0.005)
    max_sol  = _envf("PF_TRADE_MAX_SOL", 0.25)
    step_sol = _envf("PF_TRADE_STEP_SOL", 0.001)

    # Get balance
    kp = load_keypair()
    try:
        bal_sol = await get_balance_sol(str(kp.pubkey()))
    except Exception as e:
        # On failure, keep previous PF_TRADE_SOL if present, else min_sol
        fallback = _envf("PF_TRADE_SOL", min_sol)
        os.environ["PF_TRADE_SOL"] = f"{fallback:.9f}".rstrip("0").rstrip(".")
        print(f"[autosize] balance fetch failed: {e!r} -> keeping PF_TRADE_SOL={os.environ['PF_TRADE_SOL']}", flush=True)
        return fallback

    # Raw sizing (SOL terms), clamp, then step
    raw   = max(0.0, bal_sol * max(0.0, min(1.0, risk_pct)))
    sized = max(min_sol, min(max_sol, raw))
    sized = _round_down_step(sized, step_sol)
    if sized <= 0 and min_sol > 0:
        sized = min_sol

    os.environ["PF_TRADE_SOL"] = f"{sized:.9f}".rstrip("0").rstrip(".")
    print(
        f"[autosize] bal_sol={bal_sol:.4f}, risk={risk_pct*100:.2f}% "
        f"-> PF_TRADE_SOL={os.environ['PF_TRADE_SOL']}",
        flush=True,
    )
    return sized

async def _autosize_loop(refresh_sec: int):
    while True:
        await asyncio.sleep(refresh_sec)
        try:
            await autosize_once()
        except Exception as e:
            print(f"[autosize] loop error: {e!r}", flush=True)

# ---------- boot + handoff ----------
async def main():
    now = datetime.now(timezone.utc).isoformat()
    print(f"\n=== {BANNER} boot @ {now} ===", flush=True)
    print("PYTHONUNBUFFERED =", os.getenv("PYTHONUNBUFFERED", ""))
    print("DRY_RUN          =", os.getenv("DRY_RUN", ""))
    print("PF_PP_KEY        =", _mask(os.getenv("PF_PP_KEY", "")))
    print("PF_PP_ACCOUNT    =", os.getenv("PF_PP_ACCOUNT", ""))
    print("PF_PP_MINTS      =", os.getenv("PF_PP_MINTS"))
    print("SOL_RPC_URL      =", _mask(os.getenv("SOL_RPC_URL", "")))
    print("TELEGRAM_CHAT_ID =", _mask(os.getenv("TELEGRAM_CHAT_ID", ""), 6))
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

    # One shot before we start
    await autosize_once()

    # Optional refresh loop
    refresh = _envi("PF_TRADE_AUTOSIZE_REFRESH_SEC", 0)
    if refresh > 0:
        asyncio.create_task(_autosize_loop(refresh))

    # Hand off to trading loop
    from scripts.main import main as run_main
    await run_main()

if __name__ == "__main__":
    asyncio.run(main())
