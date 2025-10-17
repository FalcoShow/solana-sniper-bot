# execution/sell_fn.py
from __future__ import annotations
import os
from typing import Tuple, Optional, Dict, Any

# your helpers
from utils_pkg.core_utils import convert_sol_to_usd, swap_token_jupiter, send_telegram

# local config (kept here to avoid circular import back to sniper.py)
SOL_MINT = "So11111111111111111111111111111111111111112"
JUPITER_SLIPPAGE = float(os.getenv("JUPITER_MAX_SLIPPAGE", "0.005"))

# optional: priority fees
DEFAULT_JITO_TIP_LAMPORTS = int(os.getenv("JITO_TIP_LAMPORTS", "0"))  # set in .env if you want a fixed tip

def _heuristic_jito_tip(usd_to_sell: float) -> int:
    """
    Simple, safe default:
      - no tip for tiny chunks
      - small fixed tip for normal chunks
      - modest ramp if you’re selling bigger USD chunks
    Tune freely. Returns lamports.
    """
    if usd_to_sell < 50:
        return 0
    if usd_to_sell < 250:
        return max(DEFAULT_JITO_TIP_LAMPORTS, 20_000)   # ~0.00002 SOL
    if usd_to_sell < 1_000:
        return max(DEFAULT_JITO_TIP_LAMPORTS, 60_000)   # ~0.00006 SOL
    return max(DEFAULT_JITO_TIP_LAMPORTS, 120_000)      # ~0.00012 SOL

async def sell_token_usd(
    mint: str,
    usd_to_sell: float,
    reason: str,
    last_px_sol_per_token: float,
) -> Tuple[bool, Optional[str], Dict[str, Any]]:
    """
    Convert USD chunk -> token amount using last_px (SOL/token), then sell token->SOL via Jupiter.
    Return: (ok, signature, meta)
    """
    usd_to_sell = float(usd_to_sell)
    if usd_to_sell <= 0:
        return False, None, {"error": "usd_to_sell<=0"}

    usd_per_sol = await convert_sol_to_usd(1.0)
    if not usd_per_sol or usd_per_sol <= 0:
        return False, None, {"error": "usd_per_sol_unknown"}

    # USD -> SOL -> token amount (because last_px is SOL/token)
    sol_to_sell = usd_to_sell / float(usd_per_sol)
    if last_px_sol_per_token <= 0:
        return False, None, {"error": "last_px_invalid"}

    token_amount_in = sol_to_sell / float(last_px_sol_per_token)

    # pick a Jito tip
    jito_tip = _heuristic_jito_tip(usd_to_sell)

    ok, sig = await swap_token_jupiter(
        input_mint=mint,               # we are SELLING the token
        output_mint=SOL_MINT,          # and receiving SOL
        amount_float=token_amount_in,  # amount in *input* token units
        slippage=JUPITER_SLIPPAGE,
        jito_tip_lamports=jito_tip,    # your swap func can ignore if unsupported
    )

    if ok:
        try:
            await send_telegram(
                f"🏁 Sold {mint[:6]}… • ${usd_to_sell:,.2f} • reason={reason} • tip={jito_tip} lamports\nTx: {sig}"
            )
        except Exception:
            pass

    meta = {"jito_tip": jito_tip}
    return ok, sig, meta
