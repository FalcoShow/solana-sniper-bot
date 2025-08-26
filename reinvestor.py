# reinvestor.py
"""
Reinvestment engine (root-level)

- Reads dynamic caps/knobs from the self-learning strategy (Redis).
- Splits proceeds into savings + reinvest.
- Picks a fresh token via find_reinvestment_token() with safety filters.
- Buys via Jupiter and records a new position (atomic positions.json).
- Non-blocking: optional background worker consumes a Redis queue.

How to use:
  - (Immediate)   await reinvest(proceeds_sol, meta={...}, redis=redis)
  - (Asynchronous) await queue_reinvest(proceeds_sol, meta={...}, redis=redis)
                    # and run: asyncio.create_task(run_reinvest_worker(redis))
"""
import os
import json
import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any

try:
    import redis.asyncio as aioredis
except Exception:  # pragma: no cover
    aioredis = None

# positions persistence
try:
    from positions import save_position  # pragma: no cover
except Exception:  # pragma: no cover
    # fallback shim
    def save_position(mint: str, data: Dict[str, Any]) -> None:
        os.makedirs("data", exist_ok=True)
        path = os.path.join("data", "positions.json")
        try:
            cur = json.load(open(path, "r"))
        except Exception:
            cur = {}
        cur[mint] = data
        with open(path, "w") as f:
            json.dump(cur, f, indent=2)

# safe token scanner
try:
    from find_reinvestment_token import find_reinvestment_token  # pragma: no cover
except Exception:  # pragma: no cover
    async def find_reinvestment_token(min_volume: float, honeypot_filter: bool):
        return None

# strategy params
try:
    from self_learning_ai_strategy import get_strategy_params
except Exception:
    async def get_strategy_params(_redis):
        return {
            "REINVESTMENT_PERCENT_CAP": float(os.getenv("REINVESTMENT_PERCENT_CAP", "50.0")),
            "STOP_LOSS_PERCENT": float(os.getenv("STOP_LOSS_PERCENT", "20.0")),
            "SLIPPAGE": float(os.getenv("SLIPPAGE", "0.005")),
        }

from utils_pkg.core_utils import (
    get_token_price,
    swap_token_jupiter,
    convert_sol_to_usd,
    send_telegram,
    log_to_notion,
    transfer_to_savings,
)

logger = logging.getLogger("reinvestor")
logging.basicConfig(level=logging.INFO)

SOL_MINT = "So11111111111111111111111111111111111111112"

# Redis queue key (optional async worker)
REINVEST_QUEUE = "reinvest:queue"

# Extra env knobs (fallbacks; AI store overrides % caps)
REINVESTMENT_MIN_PROFIT_SOL = float(os.getenv("REINVESTMENT_MIN_PROFIT_SOL", "0.01"))
REINVESTMENT_VOLUME_MIN = float(os.getenv("REINVESTMENT_VOLUME_MIN", "5000"))  # 24h USD volume
REINVESTMENT_HONEYPOT_CHECK = os.getenv("REINVESTMENT_HONEYPOT_CHECK", "true").lower() == "true"
SAVINGS_WALLET = os.getenv("SAVINGS_WALLET") or os.getenv("SAVINGS_WALLET_ADDRESS")

# ---------------------------------------------------------------------------

async def _get_redis(url: Optional[str] = None):
    if aioredis is None:
        logger.warning("redis (async) not installed; reinvest worker queue will be disabled.")
        return None
    return aioredis.from_url(url or os.getenv("REDIS_URL", "redis://localhost:6379"), decode_responses=True)

# ---------------------------------------------------------------------------

async def reinvest(proceeds_sol: float, meta: Optional[Dict[str, Any]] = None, redis=None) -> Optional[str]:
    """
    Split proceeds, optionally move savings, find new token, buy, save position.
    Returns the new token mint on success, else None.
    """
    meta = meta or {}
    if proceeds_sol is None or proceeds_sol <= 0:
        logger.info("[REINVEST] No proceeds to reinvest.")
        return None

    if proceeds_sol < REINVESTMENT_MIN_PROFIT_SOL:
        logger.info(f"[REINVEST] Proceeds {proceeds_sol:.6f} SOL below min threshold "
                    f"{REINVESTMENT_MIN_PROFIT_SOL:.6f} SOL; skipping.")
        return None

    # Pull dynamic knobs from the AI strategy brain
    params = await get_strategy_params(redis)
    reinvest_cap_pct = float(params.get("REINVESTMENT_PERCENT_CAP", 50.0))
    stop_loss_percent = float(params.get("STOP_LOSS_PERCENT", 20.0))
    slippage = float(params.get("SLIPPAGE", 0.005))  # already fraction

    reinvest_sol = proceeds_sol * (reinvest_cap_pct / 100.0)
    savings_sol = max(0.0, proceeds_sol - reinvest_sol)

    logger.info(f"[REINVEST] proceeds={proceeds_sol:.6f} SOL | reinvest={reinvest_sol:.6f} SOL | savings={savings_sol:.6f} SOL")

    # Move savings first (best-effort)
    if savings_sol > 0 and SAVINGS_WALLET:
        try:
            await transfer_to_savings(savings_sol)
            logger.info(f"[REINVEST] Moved {savings_sol:.6f} SOL to savings wallet.")
        except Exception as e:
            logger.warning(f"[REINVEST] Savings transfer failed: {e}")

    # Pick a new token (safe scanner)
    new_token = await find_reinvestment_token(
        min_volume=REINVESTMENT_VOLUME_MIN,
        honeypot_filter=REINVESTMENT_HONEYPOT_CHECK,
    )
    if not new_token:
        await send_telegram("⚠️ Reinvestment skipped — no safe token met criteria right now.")
        logger.info("[REINVEST] No token candidate returned by scanner.")
        return None

    # Execute buy (SOL -> token)
    try:
        ok, tx_sig = await swap_token_jupiter(
            SOL_MINT, new_token, amount_float=reinvest_sol, buy=True, slippage=slippage
        )
    except TypeError:
        # Handle older signature: swap_token_jupiter(input_mint, output_mint, amount_float, buy=True, slippage=..)
        ok, tx_sig = await swap_token_jupiter(
            SOL_MINT, new_token, reinvest_sol, True, slippage
        )

    if not ok:
        await send_telegram(f"❌ Reinvestment buy failed for {new_token}.")
        logger.warning(f"[REINVEST] Swap failed for {new_token}")
        return None

    # Fetch price & persist position
    new_price = await get_token_price(new_token)
    if not new_price or new_price <= 0:
        new_price = 0.0

    trailing_stop = new_price * (1 - (stop_loss_percent / 100.0)) if new_price > 0 else 0.0
    now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

    save_position(new_token, {
        "token_address": new_token,
        "buy_price": new_price,
        "amount_invested": reinvest_sol,
        "timestamp": now_iso,
        "trailing_stop": trailing_stop,
        "source": "reinvestor",
        "meta": meta,
    })

    # Alerts & journaling
    usd = await convert_sol_to_usd(reinvest_sol)
    await send_telegram(
        f"🔁 Reinvested {reinvest_sol:.4f} SOL (${usd:.2f}) into {new_token}\n"
        f"• Entry: {new_price:.8f} • SL%: {stop_loss_percent:.1f} • Tx: {tx_sig}"
    )
    try:
        await log_to_notion(
            token=new_token,
            symbol=new_token[:6],
            balance=reinvest_sol,
            entry=new_price,
            current=new_price,
            profit=0.0,
            reason="Reinvestment buy",
        )
    except Exception:
        pass

    logger.info(f"[REINVEST] ✅ Completed into {new_token} (tx={tx_sig})")
    return new_token

# ---------------------------------------------------------------------------

async def queue_reinvest(proceeds_sol: float, meta: Optional[Dict[str, Any]] = None, redis=None) -> None:
    """
    Non-blocking: push a reinvest job onto a Redis queue for the worker to process.
    """
    if redis is None:
        redis = await _get_redis()
    if not redis:
        # Fall back to direct reinvest if Redis not available
        await reinvest(proceeds_sol, meta=meta, redis=None)
        return
    job = {"proceeds_sol": float(proceeds_sol), "meta": meta or {}}
    await redis.rpush(REINVEST_QUEUE, json.dumps(job))
    logger.info(f"[REINVEST] Queued job: {job}")

async def run_reinvest_worker(redis=None, poll_interval: float = 2.0):
    """
    Background loop: pops queue and processes reinvest jobs.
    Launch with: asyncio.create_task(run_reinvest_worker(redis))
    """
    if redis is None:
        redis = await _get_redis()
    if not redis:
        logger.warning("Reinvest worker disabled: no Redis.")
        return

    logger.info("🔁 Reinvest worker started.")
    while True:
        try:
            raw = await redis.blpop(REINVEST_QUEUE, timeout=int(max(1, poll_interval)))
            if not raw:
                continue
            _, payload = raw
            job = json.loads(payload)
            amt = float(job.get("proceeds_sol", 0.0))
            meta = job.get("meta") or {}
            await reinvest(amt, meta=meta, redis=redis)
        except Exception as e:
            logger.error(f"[REINVEST] Worker error: {e}", exc_info=True)
            await asyncio.sleep(1.0)

# ---------------------------------------------------------------------------

if __name__ == "__main__":
    async def _demo():
        r = await _get_redis()
        await queue_reinvest(0.05, {"reason": "demo"}, redis=r)
        await run_reinvest_worker(r)

    asyncio.run(_demo())
