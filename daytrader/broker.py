# daytrader/broker.py
"""
daytrader.broker
----------------
Thin bridge from strategy orders -> Jupiter swap via your existing core utils.

Key points
- Uses utils_pkg.core_utils.swap_token_jupiter (your known-good path).
- Clear unit model: amount_input is specified in *input token units*.
  * BUY  (BASE/QUOTE): input token is QUOTE  -> amount_input = quote amount to spend (e.g., USDC)
  * SELL (BASE/QUOTE): input token is BASE   -> amount_input = base amount to dump (e.g., SOL)
- Minimal validation + logging; leaves persistence/telemetry to core_utils.

If you need more symbols, extend TOKENS and (optionally) PAIRS below.
"""

from typing import Optional, Tuple
import os

from utils_pkg.core_utils import swap_token_jupiter, logger  # reuse your logger

# --- Token registry (extend as needed) ---
TOKENS = {
    "SOL": {
        "mint": "So11111111111111111111111111111111111111112",
        "decimals": 9,
    },
    "USDC": {
        "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "decimals": 6,
    },
    "JUP": {
        "mint": "JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB",
        "decimals": 6,
    },
    "RAY": {
        "mint": "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R",
        "decimals": 6,
    },
}

# Common pairs your strategies may trade (optional, purely informational)
PAIRS = ["SOL/USDC", "JUP/SOL", "RAY/SOL", "JUP/USDC", "RAY/USDC"]

DEFAULT_SLIPPAGE = float(os.getenv("SLIPPAGE", "0.005"))  # 0.5% default


# --- Helpers -----------------------------------------------------------------

def _mints_for_pair(pair: str) -> Tuple[str, str, str, str]:
    """
    Returns: (base_mint, quote_mint, base_sym, quote_sym)
    Raises ValueError if unknown symbol.
    """
    if "/" not in pair:
        raise ValueError(f"[broker] Pair must be like BASE/QUOTE, got '{pair}'")
    base_sym, quote_sym = pair.split("/", 1)
    base_sym, quote_sym = base_sym.strip().upper(), quote_sym.strip().upper()

    if base_sym not in TOKENS:
        raise ValueError(f"[broker] Unknown base symbol '{base_sym}' in pair '{pair}'")
    if quote_sym not in TOKENS:
        raise ValueError(f"[broker] Unknown quote symbol '{quote_sym}' in pair '{pair}'")

    base = TOKENS[base_sym]["mint"]
    quote = TOKENS[quote_sym]["mint"]
    return base, quote, base_sym, quote_sym


# --- Public API --------------------------------------------------------------

async def place_order(
    pair: str,
    side: str,               # "buy" or "sell"
    amount_input: float,     # in INPUT token units (see docstring below)
    slippage: float = DEFAULT_SLIPPAGE,
    reason: str = "daytrader",
) -> Tuple[bool, Optional[str]]:
    """
    Place a market-style swap via Jupiter.

    UNIT MODEL (important):
      For pair "BASE/QUOTE":
        - BUY:  input token = QUOTE. amount_input is the QUOTE amount to SPEND (e.g., USDC for SOL/USDC).
        - SELL: input token = BASE.  amount_input is the BASE amount to DUMP (e.g., SOL  for SOL/USDC).

    Args:
      pair         : e.g., "SOL/USDC"
      side         : "buy" or "sell" (case-insensitive)
      amount_input : float in input-token native units (respecting its decimals)
      slippage     : 0.005 = 0.5% by default
      reason       : passed through to your logging/pipelines (if core handles it)

    Returns:
      (ok: bool, signature: Optional[str])
    """
    side_l = side.lower().strip()
    if side_l not in ("buy", "sell"):
        logger.warning(f"[broker] Unknown side '{side}', expected 'buy' or 'sell'")
        return False, None
    if amount_input is None or amount_input <= 0:
        logger.warning(f"[broker] Rejecting non-positive amount_input={amount_input}")
        return False, None

    try:
        base_mint, quote_mint, base_sym, quote_sym = _mints_for_pair(pair)
    except Exception as e:
        logger.warning(str(e))
        return False, None

    # Route to your known-good Jupiter bridge
    try:
        if side_l == "buy":
            # Spend QUOTE -> Receive BASE
            ok, sig = await swap_token_jupiter(
                input_mint=quote_mint,
                output_mint=base_mint,
                amount_float=amount_input,  # QUOTE units (e.g., USDC)
                buy=True,
                slippage=slippage,
            )
        else:
            # SELL: Spend BASE -> Receive QUOTE
            ok, sig = await swap_token_jupiter(
                input_mint=base_mint,
                output_mint=quote_mint,
                amount_float=amount_input,  # BASE units (e.g., SOL)
                buy=False,
                slippage=slippage,
            )
    except Exception as e:
        logger.warning(f"[broker] place_order exception: {e}")
        return False, None

    if ok:
        logger.info(
            f"[broker] ✅ {side_l.upper()} {pair} input_amt={amount_input} "
            f"slip={slippage} sig={sig} reason={reason}"
        )
    else:
        logger.warning(f"[broker] ❌ {side_l.upper()} {pair} failed (amt={amount_input}, slip={slippage})")
    return ok, sig


async def buy_base_with_quote(
    pair: str,
    quote_amount: float,
    slippage: float = DEFAULT_SLIPPAGE,
    reason: str = "daytrader",
) -> Tuple[bool, Optional[str]]:
    """
    Convenience wrapper for BUY (BASE/QUOTE) spending 'quote_amount' units of QUOTE.
    """
    return await place_order(pair, "buy", quote_amount, slippage=slippage, reason=reason)


async def sell_base_for_quote(
    pair: str,
    base_amount: float,
    slippage: float = DEFAULT_SLIPPAGE,
    reason: str = "daytrader",
) -> Tuple[bool, Optional[str]]:
    """
    Convenience wrapper for SELL (BASE/QUOTE) dumping 'base_amount' units of BASE.
    """
    return await place_order(pair, "sell", base_amount, slippage=slippage, reason=reason)
