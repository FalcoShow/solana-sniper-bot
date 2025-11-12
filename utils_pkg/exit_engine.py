# utils_pkg/exit_engine.py
from __future__ import annotations
import os, time, asyncio, logging
from typing import Dict, Any, Optional

log = logging.getLogger("sniper")

# Optional: use your existing sell helpers if present
try:
    # modern path
    from utils_pkg import ai_sell_logic as SELL
except Exception:
    SELL = None

# Fallback direct Jupiter V6 seller (tiny helper)
async def _direct_sell_via_jup(mint: str, amount_raw: int, slippage_bps: int = 200) -> Optional[str]:
    try:
        from utils_pkg.lightning_impl import sell_via_lightning
        sig = await sell_via_lightning(mint=mint, token_program=None, amount_raw=amount_raw, slippage_bps=slippage_bps)
        return sig
    except Exception as e:
        log.warning("DIRECT_SELL_FAIL", extra={"err": str(e)[:200]})
        return None

def _env_bool(k: str, d=False) -> bool:
    v = os.getenv(k)
    return d if v is None else str(v).lower() in ("1","true","t","yes","y","on")

DRYRUN = _env_bool("DRYRUN", False) or _env_bool("DRY_RUN", False) or _env_bool("IS_DRYRUN", False)
MIN_SELL_SOL = float(os.getenv("PF_MIN_SELL_SOL", "0.001"))
LAMPORTS_PER_SOL = 1_000_000_000
DEFAULT_SLIPPAGE_BPS = int(os.getenv("PF_SLIPPAGE_BPS", "200"))

async def on_buy_filled(ev: Dict[str, Any]) -> None:
    """
    Standard entry hook. Called with:
      ev = {"mint": str, "signature": str, "symbol": str, "spend_sol": float, "pool": str, "ts": float}
    This shim guarantees the function exists and triggers your sell engine.
    """
    mint = ev.get("mint")
    spend_sol = float(ev.get("spend_sol", 0.0))
    sym = ev.get("symbol") or mint[:4]

    if not mint or spend_sol <= 0:
        log.warning("EXIT_HOOK_BAD_EVENT", extra={"ev": {k: ev.get(k) for k in ("mint","spend_sol","signature","pool")}})
        return

    log.info("EXIT_HOOK_START", extra={"mint": mint, "symbol": sym, "spend_sol": spend_sol, "dryrun": DRYRUN})

    # Prefer your real sell logic if present (TP/SL/DCA loop, etc.)
    if SELL and hasattr(SELL, "schedule_after_buy"):
        try:
            await SELL.schedule_after_buy(ev)  # your engine should take over from here
            log.info("EXIT_SCHED_OK", extra={"mint": mint, "engine": "ai_sell_logic"})
            return
        except Exception as e:
            log.warning("EXIT_SCHED_FAIL_AI", extra={"err": str(e)[:200]})

    # Fallback: place a tiny “can-sell” probe (never blocks, DRYRUN safe)
    probe_sol = max(MIN_SELL_SOL, spend_sol * 0.10)  # 10% of position or min
    probe_raw = int(probe_sol * LAMPORTS_PER_SOL)

    if DRYRUN:
        log.info("EXIT_FALLBACK_DRYRUN", extra={"mint": mint, "probe_sol": probe_sol})
        return

    sig = await _direct_sell_via_jup(mint, amount_raw=probe_raw, slippage_bps=DEFAULT_SLIPPAGE_BPS)
    log.info("EXIT_FALLBACK_RESULT", extra={"mint": mint, "probe_sol": probe_sol, "sig": sig})

# Legacy signature compatibility (mint, tx_sig)
async def on_buy_filled_legacy(raw_id_or_mint: str, tx_sig: str) -> None:
    await on_buy_filled({"mint": raw_id_or_mint, "signature": tx_sig, "symbol": "", "spend_sol": MIN_SELL_SOL, "pool": "unknown", "ts": time.time()})
