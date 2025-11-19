from __future__ import annotations
import asyncio
import os
import sys
from typing import Any, Dict

# --- Ensure repo root is on PYTHONPATH ---------------------------------------
# Allows scripts/ to import utils_pkg/, scanner/, etc.
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# Now safe to import
try:
    from utils_pkg import risk_manager as rm
except Exception as e:
    print(f"[print_limits] Failed to import utils_pkg.risk_manager: {e}", flush=True)
    raise

async def main() -> None:
    print("\n=== 📊 Risk / Limits Probe ===\n")

    # Show a couple of key envs that affect behaviour
    env_snippets = {
        "REDIS_URL": os.getenv("REDIS_URL", "<none>"),
        "WALLET_PUBKEY": os.getenv("WALLET_PUBKEY")
        or os.getenv("WALLET_PUBLIC_KEY")
        or os.getenv("WALLET_ADDRESS")
        or "<none>",
        "TRAILING_STOP_ENABLED": os.getenv("TRAILING_STOP_ENABLED", "true"),
        "STOP_AFTER_DAILY_PROFIT_PCT": os.getenv("STOP_AFTER_DAILY_PROFIT_PCT", "4"),
        "DAILY_SPEND_CAP_PCT": os.getenv("DAILY_SPEND_CAP_PCT", "0.15"),
        "RISK_MAX_PER_TRADE": os.getenv("RISK_MAX_PER_TRADE", "0.045"),
        "MAX_CONCURRENT_POS": os.getenv("MAX_CONCURRENT_POS", "4"),
    }

    print("Key env vars:")
    for k, v in env_snippets.items():
        print(f"  {k} = {v}")
    print()

    # Core limits from risk_manager
    limits: Dict[str, Any] = await rm.current_limits()
    allowed = await rm.should_allow_new_entry()
    daily_realized = await rm._daily_realized_usd(None)  # type: ignore[attr-defined]

    print("Current limits (derived from wallet equity + tier + throttle):")
    print(f"  equity_usd        = {limits.get('equity_usd', 0.0):.2f}")
    print(f"  ticket_usd        = {limits.get('ticket_usd', 0.0):.2f}")
    print(f"  pos_pct_cap       = {limits.get('pos_pct_cap', 0.0) * 100:.2f}%")
    print(f"  max_concurrent    = {limits.get('max_concurrent', 0)}")
    print(f"  daily_spend_usd   = {limits.get('daily_spend_usd', 0.0):.2f}")
    print()

    print("Governance state:")
    print(f"  daily_realized_usd      = {daily_realized:.2f}")
    print(f"  should_allow_new_entry  = {bool(allowed)}")
    print()

    # Extra: show winrate20 and rolling DD if available
    try:
        winrate20, dd = await rm._metrics(None)  # type: ignore[attr-defined]
        print("Metrics:")
        print(f"  winrate_last_20_trades  = {winrate20 * 100:.2f}%")
        print(f"  rolling_drawdown_pct    = {dd * 100:.2f}%")
        print()
    except Exception as e:
        print(f"[print_limits] metrics unavailable: {e}")
        print()

    print("=== done ===\n")


if __name__ == "__main__":
    asyncio.run(main())
