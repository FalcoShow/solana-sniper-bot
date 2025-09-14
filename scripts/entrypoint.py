# scripts/entrypoint.py
import os, asyncio
from datetime import datetime, UTC

BANNER = "sniper-entrypoint v1"

def _envf(name: str, default: float) -> float:
    try: return float(os.getenv(name, str(default)))
    except Exception: return default

def _envi(name: str, default: int) -> int:
    try: return int(os.getenv(name, str(default)))
    except Exception: return default

def _round_down_step(x: float, step: float) -> float:
    if step <= 0: return x
    return max(step, (int(x / step)) * step)

async def autosize_once() -> float:
    """Compute PF_TRADE_SOL from wallet balance & guardrails; write env var."""
    from utils_pkg.solana_wallet import load_keypair, get_balance_sol

    enable = os.getenv("PF_TRADE_AUTOSIZE", "0") == "1"
    if not enable:
        return float(os.getenv("PF_TRADE_SOL", "0.01"))

    risk_pct   = _envf("PF_TRADE_RISK_PCT", 0.03)        # 3% of balance by default
    min_sol    = _envf("PF_TRADE_MIN_SOL", 0.005)
    max_sol    = _envf("PF_TRADE_MAX_SOL", 0.25)
    step_sol   = _envf("PF_TRADE_STEP_SOL", 0.001)

    kp = load_keypair()
    bal_sol = await get_balance_sol(str(kp.pubkey()))

    # equity in SOL terms (simple & robust; no price feed required)
    equity_sol = bal_sol
    raw_size = equity_sol * max(0.0, min(1.0, risk_pct))

    # clamp & step
    sized = max(min_sol, min(max_sol, raw_size))
    sized = _round_down_step(sized, step_sol)

    # write for the trading loop to read
    os.environ["PF_TRADE_SOL"] = f"{sized:.9f}"

    print(
        f"[autosize] bal_sol={bal_sol:.4f}, risk={risk_pct:.2%} "
        f"-> PF_TRADE_SOL={sized:.9f}",
        flush=True,
    )
    return sized

async def main():
    print(f"\n=== {BANNER} boot @ {datetime.now(UTC).isoformat()} ===", flush=True)
    print("PYTHONUNBUFFERED =", os.getenv("PYTHONUNBUFFERED", ""))
    print("DRY_RUN          =", os.getenv("DRY_RUN", ""))
    print("PF_PP_KEY        =", (os.getenv("PF_PP_KEY","")[:4] + "..." if os.getenv("PF_PP_KEY") else ""))
    print("PF_PP_ACCOUNT    =", os.getenv("PF_PP_ACCOUNT",""))
    print("PF_PP_MINTS      =", os.getenv("PF_PP_MINTS"))
    print("SOL_RPC_URL      =", (os.getenv("SOL_RPC_URL","")[:4] + "..."))
    print("TELEGRAM_CHAT_ID =", (os.getenv("TELEGRAM_CHAT_ID","")[:6] + "..."))
    print("— guardrails —")
    for k in ("PF_TRADE_RISK_PCT","PF_DAILY_MAX_DRAWDOWN_PCT","PF_DAILY_MAX_LOSSES","PF_MAX_CONCURRENT","PF_COOLDOWN_SEC"):
        print(f"{k:25s}= {os.getenv(k,'')}")
    print("====================================================", flush=True)

    # set PF_TRADE_SOL once before we start
    await autosize_once()

    # hand off to the main loop
    from scripts.main import main as run_main
    await run_main()

if __name__ == "__main__":
    asyncio.run(main())
