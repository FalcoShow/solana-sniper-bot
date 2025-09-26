# utils_pkg/trader.py
from __future__ import annotations

import asyncio
import os
import time
from typing import Any, Dict, Optional

import requests

from logging_setup import get_logger
from utils_pkg.solana_wallet import load_keypair, get_pubkey_str
from utils_pkg.router import sell_via_lightning

log = get_logger()

# -----------------------------------------------------------------------------
# Env / flags
# -----------------------------------------------------------------------------
IS_DRYRUN = (os.getenv("DRY_RUN", "0").strip().lower() in {"1", "true", "yes", "y", "on"})

# Pull-through RPC knobs (kept optional to avoid hard dependency on txlearn)
TX_RPC_URL = (os.getenv("SOL_RPC_URL") or os.getenv("TX_RPC_URL") or "").strip()
TX_COMMITMENT = (os.getenv("SOL_COMMITMENT") or os.getenv("TX_COMMITMENT") or "confirmed").strip()

# Basic HTTP config
_UA = {"user-agent": "pnlsniper/1.0 (+trader)"}
_RPC_TIMEOUT = float(os.getenv("ROUTER_RPC_TIMEOUT_SEC", "10") or "10")

# Optional budget hooks (fail open)
try:
    from utils_pkg import budget  # type: ignore
except Exception:
    class _BudgetNoop:
        def before_rpc(self, method: str, *, weight: Optional[int] = None) -> bool: return True
        def after_rpc(self, method: str, *, weight: Optional[int] = None) -> None:  return None
    budget = _BudgetNoop()  # type: ignore


# -----------------------------------------------------------------------------
# Lightweight RPC helpers (requests in worker thread)
# -----------------------------------------------------------------------------
def _rpc_post_blocking(method: str, params: list) -> dict:
    """
    Blocking JSON-RPC call; run via asyncio.to_thread from async contexts.
    """
    if not TX_RPC_URL:
        raise RuntimeError("SOL_RPC_URL / TX_RPC_URL not configured")
    if not budget.before_rpc(method):
        # behave as if the call failed pre-flight
        return {}
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    try:
        r = requests.post(TX_RPC_URL, json=payload, timeout=_RPC_TIMEOUT, headers=_UA)
        r.raise_for_status()
        return r.json()
    finally:
        # always release budget unit
        try:
            budget.after_rpc(method)
        except Exception:
            pass


async def _rpc_post(method: str, params: list) -> dict:
    return await asyncio.to_thread(_rpc_post_blocking, method, params)


# -----------------------------------------------------------------------------
# Token balance helpers
# -----------------------------------------------------------------------------
def _sum_token_balance_raw_from_resp(js: dict) -> int:
    """
    Sum raw token balance across all token accounts for the mint.
    """
    vals = (js or {}).get("result", {}).get("value", [])
    if not isinstance(vals, list) or not vals:
        return 0

    total = 0
    for it in vals:
        try:
            # parsed layout
            amt = it["account"]["data"]["parsed"]["info"]["tokenAmount"]["amount"]
            total += int(amt)
        except Exception:
            # best-effort fallback to UI fields if present
            try:
                ui = float(it["account"]["data"]["parsed"]["info"]["tokenAmount"]["uiAmount"] or 0.0)
                dec = int(it["account"]["data"]["parsed"]["info"]["tokenAmount"]["decimals"] or 0)
                total += int(round(ui * (10 ** dec)))
            except Exception:
                continue
    return int(total)


async def get_token_balance_raw(owner: str, mint: str) -> int:
    """
    Returns the *sum* raw token amount across all accounts owned by `owner` for `mint`.
    """
    try:
        js = await _rpc_post(
            "getTokenAccountsByOwner",
            [
                owner,
                {"mint": mint},
                {"encoding": "jsonParsed", "commitment": TX_COMMITMENT or "confirmed"},
            ],
        )
        return _sum_token_balance_raw_from_resp(js)
    except Exception as e:
        log.event("RPC_BALANCE_ERR", err=str(e))
        return 0


# -----------------------------------------------------------------------------
# Public SELL API (used by scripts/main.py)
# -----------------------------------------------------------------------------
async def sell_on_jupiter(mint: str, pct_of_wallet: float = 1.0) -> Optional[str]:
    """
    Sells `pct_of_wallet` (0..1) of the wallet's current balance for `mint`
    using the Jupiter router (utils_pkg.router.sell_via_lightning).

    Returns:
        str tx signature on success, or None (including DRY_RUN).
    """
    pct = max(0.0, min(1.0, float(pct_of_wallet)))
    if pct <= 0:
        log.event("SELL_SKIP_ZERO_PCT", mint=mint)
        return None

    # Determine owner & balance
    kp = load_keypair()
    owner = get_pubkey_str(kp)

    balance_raw = await get_token_balance_raw(owner, mint)
    if balance_raw <= 0:
        log.event("SELL_SKIP_NO_BAL", mint=mint)
        return None

    amount_raw = max(1, int(round(balance_raw * pct)))

    # Delegate to router; it will honor DRY_RUN itself too
    ok, sig = await sell_via_lightning(
        mint=mint,
        token_program=None,   # Jupiter doesn't need it
        amount_raw=amount_raw,
        slippage_bps=None,    # let router/env decide
        priority_sol=None,    # let dynamic priority decide
    )

    if ok and sig:
        log.event("SELL_OK", mint=mint, pct=pct, amount_raw=amount_raw, sig=sig)
        return sig

    log.event("SELL_FAIL", mint=mint, pct=pct, amount_raw=amount_raw)
    return None


# -----------------------------------------------------------------------------
# Exit engine hooks (minimal, non-blocking)
# -----------------------------------------------------------------------------
class _ExitEngine:
    """
    Minimal exit engine that satisfies scripts/main.py:
      - on_trade_tick(raw_event)
      - on_buy_filled(raw_id_or_mint, tx_sig)
    You can expand this later to implement actual exit logic.
    """
    def __init__(self) -> None:
        self._last_tick_note = 0.0

    async def on_trade_tick(self, raw_event: Dict[str, Any]) -> None:
        # Keep this ultra-light; just periodic logging to prove liveness
        now = time.time()
        if now - self._last_tick_note > 10:
            self._last_tick_note = now
            sym = raw_event.get("symbol") or raw_event.get("ticker") or "?"
            log.every("exit.tick", n=10, msg="EXIT_TICK", symbol=sym)

    async def on_buy_filled(self, raw_id_or_mint: str, tx_sig: str) -> None:
        # Hook for post-buy actions (learning/trailing/etc.). For now, log.
        log.event("EXIT_ON_BUY_FILLED", mint=raw_id_or_mint, sig=tx_sig)
        # If you later want to kick off a dedicated sell loop, you can do it here.


exit_engine = _ExitEngine()

__all__ = ["IS_DRYRUN", "sell_on_jupiter", "exit_engine", "get_token_balance_raw"]
