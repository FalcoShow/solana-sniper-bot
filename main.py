# main.py
import asyncio
import logging
from pprint import pprint

from config import summarize
from utils_pkg import (
    init_resources,
    close_resources,
    get_session,
    get_redis,
    get_wallet_pubkey,
    get_sol_balance,       # <-- missing before
    get_sol_usd,           # small extra check
    send_telegram,
    send_discord_alert,
)

log = logging.getLogger("main")
logging.basicConfig(level=logging.INFO)

async def self_check() -> None:
    """
    Minimal health check:
      - HTTP session usable
      - Redis reachable
      - Wallet present + SOL balance
      - Quick SOL/USD fetch
    """
    sess = get_session()
    r    = get_redis()
    w    = get_wallet_pubkey()

    # HTTP ping (no-op request to a super cheap endpoint)
    http_ok = False
    try:
        async with sess.get("https://example.com", timeout=5) as resp:
            http_ok = (200 <= resp.status < 400)
    except Exception:
        http_ok = False

    # Redis ping
    redis_ok = False
    if r:
        try:
            pong = await r.ping()
            redis_ok = bool(pong)
        except Exception:
            redis_ok = False

    # Wallet + balance
    balance_sol = 0.0
    if w:
        try:
            balance_sol = await get_sol_balance(w)
        except Exception:
            balance_sol = 0.0

    # SOL/USD quick price
    try:
        sol_usd = await get_sol_usd()
    except Exception:
        sol_usd = 0.0

    log.info("HTTP OK: %s | Redis OK: %s | Wallet: %s | SOL: %.6f | SOL/USD: %.2f",
             http_ok, redis_ok, (w[:6] + "..." + w[-6:]) if w else "(none)",
             balance_sol, sol_usd)

async def main() -> None:
    await init_resources()

    # Pretty-print the config summary (already printed by your startup code, but handy)
    print("== CONFIG SUMMARY ==")
    pprint(summarize())

    # Run a short health-check
    await self_check()

    # Optional: send a quick “bot online” alert (comment out if noisy)
    try:
        await send_discord_alert(
            title="✅ Bot online",
            fields=[{"name": "Wallet", "value": get_wallet_pubkey() or "(none)", "inline": False}]
        )
    except Exception:
        pass

    await close_resources()

if __name__ == "__main__":
    asyncio.run(main())
