import os
import base64
import logging
from typing import Optional, Tuple, Dict, Any

from solana.rpc.types import TxOpts

# Import the module (not symbols) so globals are fresh after init_resources()
from utils_pkg import core_utils as CU

# Optional circuit-breakers (best-effort imports)
try:
    from utils_pkg.circuit_breakers import (
        trading_halted,
        record_error_and_maybe_halt,
        record_pnl_and_maybe_halt,
    )
except Exception:  # graceful fallbacks
    async def trading_halted(*args, **kwargs) -> bool:  # type: ignore
        return False
    async def record_error_and_maybe_halt(*args, **kwargs):  # type: ignore
        return None
    async def record_pnl_and_maybe_halt(*args, **kwargs):  # type: ignore
        return None

# Fixed mints (mainnet)
SOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"

logger = logging.getLogger("sell_adapter")


def _maybe_call(x):
    try:
        return x() if callable(x) else x
    except TypeError:
        return x


def _extract_signer_from_wallet(wallet):
    """
    Return (solders.Keypair signer, secret_bytes_or_None).
    Tries wallet directly, then wallet._keypair. Handles 32-byte seed and 64-byte secret.
    """
    from solders.keypair import Keypair as SKeypair

    if hasattr(wallet, "pubkey") and not callable(getattr(wallet, "pubkey")):
        # If someone passed a raw Keypair, just try to parse below.
        pass

    if wallet.__class__.__name__ == "Keypair":  # solders Keypair
        return wallet, None

    sk = None
    for attr in ("secret_key", "secret", "to_bytes", "__bytes__", "seed"):
        if hasattr(wallet, attr):
            val = _maybe_call(getattr(wallet, attr))
            if isinstance(val, (bytes, bytearray)):
                sk = bytes(val)
                break

    if sk is None and hasattr(wallet, "_keypair"):
        inner = wallet._keypair
        for attr in ("secret_key", "secret", "to_bytes", "__bytes__", "seed"):
            if hasattr(inner, attr):
                val = _maybe_call(getattr(inner, attr))
                if isinstance(val, (bytes, bytearray)):
                    sk = bytes(val)
                    break

    if sk is None:
        raise RuntimeError("wallet keypair bytes not accessible for signing")

    from solders.keypair import Keypair as SKeypair
    if len(sk) == 64:
        signer = SKeypair.from_bytes(sk)
    elif len(sk) == 32:
        signer = SKeypair.from_seed(sk)
    else:
        raise RuntimeError(f"unexpected secret length {len(sk)}; expected 32 or 64")
    return signer, sk


async def _perform_jupiter_swap_v6(quote_json: dict) -> Tuple[bool, Optional[str]]:
    """
    Pure v6 swap using core_utils globals. Posts {quoteResponse, userPublicKey, wrapAndUnwrapSol}
    to /swap, then signs and submits the versioned tx with **solders only**.
    """
    # pull fresh references AFTER init_resources()
    session = CU.session
    solana_client = CU.solana_client
    wallet = CU.wallet
    JUPITER_SWAP_API = CU.JUPITER_SWAP_API
    aiohttp = CU.aiohttp
    Confirmed = CU.Confirmed

    try:
        if session is None or solana_client is None or wallet is None:
            await record_error_and_maybe_halt(reason="swap_missing_infra")
            logger.warning("swap v6: session/solana_client/wallet not initialized")
            return False, None
        if not isinstance(quote_json, dict):
            await record_error_and_maybe_halt(reason="swap_invalid_quote")
            logger.warning("swap v6: invalid quote")
            return False, None

        user_pubkey = os.getenv("WALLET_PUBKEY") or str(wallet.pubkey())
        payload: Dict[str, Any] = {
            "quoteResponse": quote_json,
            "userPublicKey": user_pubkey,
            "wrapAndUnwrapSol": True,
        }

        # Optional priority fee knobs (best-effort)
        try:
            jito_enabled = str(os.getenv("JITO_ENABLED", "false")).lower() in ("1", "true", "yes", "y")
            fee_micro = int(float(os.getenv("JITO_FEE_MICROLAMPORTS", "0")))
            tip_lamports = int(float(os.getenv("JITO_TIP_LAMPORTS", "0")))
            if jito_enabled:
                if fee_micro > 0:
                    payload["computeUnitPriceMicroLamports"] = fee_micro
                elif tip_lamports > 0:
                    payload["jitoTipLamports"] = tip_lamports
        except Exception:
            pass

        # Call Jupiter v6 /swap
        url = f"{JUPITER_SWAP_API.rstrip('/')}/swap"
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            body = await resp.text()
            if resp.status != 200:
                await record_error_and_maybe_halt(reason=f"swap_http_{resp.status}")
                logger.warning(f"swap v6 HTTP {resp.status}: {body}")
                return False, None
            try:
                data = await resp.json()
            except Exception:
                await record_error_and_maybe_halt(reason="swap_non_json")
                logger.warning(f"swap v6: non-JSON response: {body[:200]}")
                return False, None

        tx_b64 = data.get("swapTransaction") or data.get("swapTransactionBase64")
        if not tx_b64:
            await record_error_and_maybe_halt(reason="swap_missing_tx")
            logger.warning(f"swap v6: missing swapTransaction in response: keys={list(data.keys())}")
            return False, None

        # Decode, sign, and send with **solders** only (versioned tx)
        from solders.transaction import VersionedTransaction as SVTx

        raw = base64.b64decode(tx_b64)
        signer, _sk = _extract_signer_from_wallet(wallet)

        vtx = SVTx.from_bytes(raw)
        msg = vtx.message
        new_vtx = SVTx(msg, [signer])

        try:
            raw_signed = new_vtx.to_bytes()
        except Exception:
            raw_signed = bytes(new_vtx)

        resp = await solana_client.send_raw_transaction(raw_signed, opts=TxOpts(skip_preflight=True))

        # Normalize signature from various return shapes
        tx_sig = None
        for k in ("value", "result", "signature"):
            if hasattr(resp, k):
                tx_sig = getattr(resp, k)
                break
        if tx_sig is None and isinstance(resp, dict):
            tx_sig = resp.get("result") or resp.get("value")
        if tx_sig is None:
            tx_sig = str(resp)

        await solana_client.confirm_transaction(tx_sig, commitment=Confirmed)
        return True, tx_sig

    except Exception as e:
        await record_error_and_maybe_halt(reason="swap_exception")
        logger.warning(f"swap v6 exception: {e}")
        return False, None


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
    Sell <token_amount> of `mint` into SOL using Jupiter v6:
      1) CU.get_jupiter_quote (v6)
      2) _perform_jupiter_swap_v6 (posts quoteResponse, signs v0 tx) unless dry_run=True
      3) Log to Postgres/Telegram
      4) Record realized PnL in USD (circuit-breaker daily stops)
    NOTE: even if trading is halted for the day, sells are allowed to flatten risk.
    """
    try:
        # USDC/USDT = 6 decimals; most other tokens = 9
        decimals = 6 if mint in (USDC_MINT, USDT_MINT) else 9
        amount_atoms = int(round(float(token_amount) * (10 ** decimals)))

        q = await CU.get_jupiter_quote(mint, SOL_MINT, amount_atoms, slippage)
        if not (isinstance(q, dict) and "routePlan" in q and "outAmount" in q):
            await record_error_and_maybe_halt(reason="sell_no_route")
            logger.warning(f"[{symbol}] Jupiter sell: no v6 route (quote missing routePlan/outAmount)")
            return False

        # Expected SOL out (lamports → SOL)
        try:
            sol_out = int(q.get("outAmount", 0)) / 1_000_000_000
        except Exception:
            sol_out = None

        # approx price per token in SOL if we can estimate it
        price_sol = None
        try:
            if token_amount and sol_out is not None:
                price_sol = float(sol_out) / float(token_amount)
        except Exception:
            price_sol = None

        if dry_run:
            tx_sig = f"DRYRUN-{os.urandom(4).hex()}"
            ok = True
        else:
            ok, tx_sig = await _perform_jupiter_swap_v6(q)

        if not ok:
            await record_error_and_maybe_halt(reason="sell_swap_failed")
            logger.warning(f"[{symbol}] Jupiter sell failed")
            return False

        # --- best-effort realized PnL tracking (power daily stops) ---
        try:
            entry_sol = None
            if hasattr(CU, "get_entry_price"):
                entry_sol = await CU.get_entry_price(CU.redis, mint)  # type: ignore
            if entry_sol is not None and price_sol is not None:
                profit_sol = (float(price_sol) - float(entry_sol)) * float(token_amount)
                profit_usd = await CU.convert_sol_to_usd(profit_sol)
                try:
                    await record_pnl_and_maybe_halt(pnl_usd=float(profit_usd))
                except TypeError:
                    # Some implementations accept (redis, pnl_usd=...)
                    await record_pnl_and_maybe_halt(CU.redis, pnl_usd=float(profit_usd))  # type: ignore
        except Exception:
            pass

        # --- Write sell event to Postgres (best-effort, auto-adapts to your function signature) ---
        try:
            import inspect
            sig_obj = inspect.signature(CU.log_trade_event)
            params = sig_obj.parameters

            payload = {
                "side": "sell",
                "token": mint,
                "symbol": symbol,
                "signature": tx_sig,
                "reason": reason,
            }

            if sol_out is not None:
                if "sol_amount" in params:
                    payload["sol_amount"] = sol_out
                elif "amount_sol" in params:
                    payload["amount_sol"] = sol_out
                elif "sol" in params:
                    payload["sol"] = sol_out
                elif "balance" in params:
                    payload["balance"] = sol_out
            if price_sol is not None:
                if "price_sol" in params:
                    payload["price_sol"] = price_sol
                elif "price" in params:
                    payload["price"] = price_sol
                if "current" in params:
                    payload["current"] = price_sol

            if "token_amount" in params:
                payload["token_amount"] = float(token_amount)
            elif "amount" in params:
                payload["amount"] = float(token_amount)

            required = [
                name
                for name, p in params.items()
                if p.default is inspect._empty and p.kind in (p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY)
            ]
            if "balance" in required and "balance" not in payload:
                payload["balance"] = 0.0 if sol_out is None else float(sol_out)
            if "entry" in required and "entry" not in payload:
                payload["entry"] = 0.0
            if "current" in required and "current" not in payload:
                payload["current"] = 0.0 if price_sol is None else float(price_sol)
            if "profit" in required and "profit" not in payload:
                payload["profit"] = 0.0
            if "reason" in required and "reason" not in payload:
                payload["reason"] = reason

            await CU.log_trade_event(**payload)
        except Exception as e:
            logger.warning(f"trade log skipped: {e}")

        # Telegram notify (best-effort)
        try:
            note = "DRY RUN" if dry_run else "Sell complete"
            out_line = f"\nSOL out: {sol_out:.9f}" if sol_out is not None else ""
            await CU.send_telegram(f"✅ [{symbol}] {note}\nAmount (token): {token_amount}{out_line}\nTx: {tx_sig}")
        except Exception:
            pass

        logger.info(f"[{symbol}] ✅ Sell complete: {tx_sig}")
        return True

    except Exception as e:
        await record_error_and_maybe_halt(reason="sell_exception")
        logger.warning(f"[{symbol}] sell exception: {e}")
        return False
