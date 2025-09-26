# scripts/sell_adapter.py
from __future__ import annotations

import os
import logging
import inspect
from typing import Optional, Tuple, Dict, Any

# Prefer project logger (has .event/.every shims)
try:
    from logging_setup import get_logger
    logger = get_logger()
except Exception:  # pragma: no cover
    logger = logging.getLogger("sell_adapter")

# Optional circuit-breakers (no-ops if missing)
try:
    from utils_pkg.circuit_breakers import (
        trading_halted,
        record_error_and_maybe_halt,
        record_pnl_and_maybe_halt,
    )
except Exception:  # pragma: no cover
    async def trading_halted(*_a, **_k) -> bool:  # type: ignore
        return False
    async def record_error_and_maybe_halt(*_a, **_k):  # type: ignore
        return None
    async def record_pnl_and_maybe_halt(*_a, **_k):  # type: ignore
        return None

# Best-effort core utils (for telemetry / db / telegram)
try:
    from utils_pkg import core_utils as CU  # type: ignore
except Exception:  # pragma: no cover
    CU = None  # type: ignore

# Router = single source of truth for mint handling, quotes & swaps
from utils_pkg.router import (
    normalize_pump_id_to_mint,
    get_decimals,
    get_sell_quote,
    get_price_ui,
    sell_via_lightning,
)

SOL_MINT  = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"

# ————————————————————————————————————————————————————————————————————————
# Helpers
# ————————————————————————————————————————————————————————————————————————
def _slippage_to_bps(slippage: float) -> int:
    """0.005 → 50 bps."""
    try:
        return max(1, int(round(float(slippage) * 10_000)))
    except Exception:
        return 50

async def _maybe_tele(msg: str) -> None:
    if CU and hasattr(CU, "send_telegram"):
        try:
            await CU.send_telegram(msg)  # type: ignore
        except Exception:
            pass

async def _log_trade_event(payload: Dict[str, Any]) -> None:
    """Adapt to your CU.log_trade_event signature if present."""
    if not CU or not hasattr(CU, "log_trade_event"):
        return
    try:
        fn = CU.log_trade_event  # type: ignore
        sig = inspect.signature(fn)
        params = sig.parameters

        # Massage field names to whatever your impl expects.
        rename_map = {
            "sol_out": ("sol_amount", "amount_sol", "sol", "balance"),
            "price_sol": ("price_sol", "price", "current"),
            "token_amount": ("token_amount", "amount"),
        }
        out = dict(payload)
        for src, alts in rename_map.items():
            if src in out:
                for alt in alts:
                    if alt in params and alt not in out:
                        out[alt] = out[src]

        # Fill any required-but-missing args with safe defaults.
        required = [
            name for name, p in params.items()
            if p.default is inspect._empty and p.kind in (p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY)
        ]
        for r in required:
            if r not in out:
                out[r] = 0 if "amount" in r or "sol" in r or "price" in r or r in {"balance", "profit"} else payload.get(r)

        if inspect.iscoroutinefunction(fn):
            await fn(**out)
        else:
            fn(**out)
    except Exception as e:
        logger.warning(f"trade log skipped: {e}")

# ————————————————————————————————————————————————————————————————————————
# Main entrypoint
# ————————————————————————————————————————————————————————————————————————
async def execute_sell(
    mint: str,
    symbol: str,
    token_amount: float,
    slippage: float = 0.005,
    *,
    reason: str = "sell_adapter",
    dry_run: bool = False,
) -> bool:
    """
    Sell <token_amount> of `mint` into SOL using router (Jupiter v6 under the hood).

    Steps:
      1) Normalize mint (handles '...pump'/'...bonk' IDs).
      2) Fetch SPL decimals.
      3) Pre-sell quote (for expected SOL and basic sanity).
      4) Execute via sell_via_lightning (respects PF_PRIORITY_SOL / PF_SLIPPAGE_BPS).
      5) Best-effort PnL log & Telegram.

    Returns True on success, False on failure.
    """
    try:
        # Sells should always be allowed to reduce risk. We skip the trading_halted gate on purpose.

        # 1) Normalize (robust to Pump Portal ids)
        try:
            norm = normalize_pump_id_to_mint(mint)
        except Exception as e:
            await record_error_and_maybe_halt(reason="sell_normalize_fail")
            logger.warning(f"[{symbol}] normalize failed for {mint}: {e}")
            return False

        # 2) Decimals (router cached RPC)
        try:
            dec = await get_decimals(norm)
        except Exception:
            dec = None
        if dec is None:
            # Heuristic fallback
            dec = 6 if norm in (USDC_MINT, USDT_MINT) else 9
        dec = max(0, min(18, int(dec)))

        # Amount in raw atoms
        try:
            amount_raw = int(round(float(token_amount) * (10 ** dec)))
        except Exception:
            await record_error_and_maybe_halt(reason="sell_bad_amount")
            logger.warning(f"[{symbol}] invalid token_amount={token_amount}")
            return False
        if amount_raw <= 0:
            await record_error_and_maybe_halt(reason="sell_zero_amount")
            logger.warning(f"[{symbol}] zero amount after scaling (dec={dec})")
            return False

        # 3) Pre-sell quote for telemetry & sanity (doesn't submit)
        sol_out_exp: Optional[float] = None
        try:
            q = await get_sell_quote(mint=norm, amount_raw=amount_raw, slippage_bps=_slippage_to_bps(slippage))
            lamports_out = int(q.get("lamports_out") or q.get("outAmount") or 0)
            sol_out_exp = lamports_out / 1_000_000_000 if lamports_out > 0 else None
        except Exception:
            q = None
            sol_out_exp = None

        # Also estimate a price per token (SOL / UI token)
        price_sol: Optional[float] = None
        try:
            if token_amount > 0:
                if sol_out_exp is None:
                    px = await get_price_ui(norm)
                    price_sol = float(px) if px else None
                else:
                    price_sol = float(sol_out_exp) / float(token_amount)
        except Exception:
            price_sol = None

        # 4) Dry-run: report & exit
        if dry_run:
            tx_sig = f"DRYRUN-{os.urandom(4).hex()}"
            await _maybe_tele(
                f"🧪 [SELL DRY RUN] {symbol}\nMint: {norm}\nAmount: {token_amount}\n"
                + (f"Est SOL out: {sol_out_exp:.9f}\n" if sol_out_exp is not None else "")
                + f"Tx: {tx_sig}"
            )
            logger.info(f"[{symbol}] DRYRUN sell OK: {tx_sig}")
            return True

        # 5) Live sell via router
        priority_sol = float(os.getenv("PF_PRIORITY_SOL", "0.002") or 0.0)
        slippage_bps = int(os.getenv("PF_SLIPPAGE_BPS", str(_slippage_to_bps(slippage))) or _slippage_to_bps(slippage))

        ok, tx_sig = await sell_via_lightning(
            mint=norm,
            token_program=None,         # router/Jupiter doesn't need it for the swap request
            amount_raw=amount_raw,
            slippage_bps=slippage_bps,
            priority_sol=priority_sol,
        )
        if not ok:
            await record_error_and_maybe_halt(reason="sell_swap_failed")
            logger.warning(f"[{symbol}] Jupiter sell failed")
            return False

        # 6) PnL bookkeeping (best-effort, powers daily circuit breaker)
        try:
            if CU and hasattr(CU, "get_entry_price") and hasattr(CU, "convert_sol_to_usd"):
                entry_sol = await CU.get_entry_price(CU.redis, norm)  # type: ignore
                if entry_sol is not None:
                    # pick latest price if we have it
                    p_now = price_sol
                    if p_now is None:
                        px = await get_price_ui(norm)
                        p_now = float(px) if px else None
                    if p_now is not None:
                        profit_sol = (float(p_now) - float(entry_sol)) * float(token_amount)
                        profit_usd = await CU.convert_sol_to_usd(float(profit_sol))  # type: ignore
                        try:
                            await record_pnl_and_maybe_halt(pnl_usd=float(profit_usd))
                        except TypeError:
                            await record_pnl_and_maybe_halt(CU.redis, pnl_usd=float(profit_usd))  # type: ignore
        except Exception:
            pass

        # 7) Adaptive trade log → Postgres (if your impl exists)
        try:
            payload = {
                "side": "sell",
                "token": norm,
                "symbol": symbol,
                "signature": tx_sig,
                "reason": reason,
                "token_amount": float(token_amount),
            }
            if sol_out_exp is not None:
                payload["sol_out"] = float(sol_out_exp)
            if price_sol is not None:
                payload["price_sol"] = float(price_sol)
            await _log_trade_event(payload)
        except Exception as e:
            logger.warning(f"trade log skipped: {e}")

        # 8) Telegram (best-effort)
        try:
            out_line = f"\nSOL out (est): {sol_out_exp:.9f}" if sol_out_exp is not None else ""
            await _maybe_tele(f"✅ [SELL] {symbol}\nMint: {norm}\nAmount: {token_amount}{out_line}\nTx: {tx_sig}")
        except Exception:
            pass

        logger.info(f"[{symbol}] ✅ Sell complete: {tx_sig}")
        return True

    except Exception as e:
        await record_error_and_maybe_halt(reason="sell_exception")
        logger.warning(f"[{symbol}] sell exception: {e}")
        return False
