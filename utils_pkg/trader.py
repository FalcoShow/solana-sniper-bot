# utils_pkg/trader.py
from __future__ import annotations

"""
utils_pkg/trader.py — lightweight sell helpers + exit-engine stub

Highlights
- Robust DRYRUN detection (DRYRUN / DRY_RUN / IS_DRYRUN), shared with other modules.
- Async-safe JSON-RPC helpers using requests in a worker thread (no hard httpx dep).
- Token balance fetch via getTokenAccountsByOwner (jsonParsed).
- Jupiter-v6 sell via router.sell_via_lightning() → returns tx signature or None.
- Minimal exit_engine stub with hooks consumed by main.py.

This module avoids tight coupling so the rest of the app can import it early.
"""

import asyncio
import os
import time
from typing import Any, Dict, Optional

import requests

from logging_setup import get_logger
from utils_pkg.solana_wallet import load_keypair, get_pubkey_str

# Router sell entrypoint (should forward to lightning impl internally)
try:
    from utils_pkg.router import sell_via_lightning  # async (mint, token_program, amount_raw, slippage_bps?) -> Optional[str]
except Exception as e:  # pragma: no cover
    sell_via_lightning = None  # type: ignore

log = get_logger()

# ──────────────────────────────────────────────────────────────────────────────
# Env helpers
# ──────────────────────────────────────────────────────────────────────────────

def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else v.strip()

def _env_int(name: str, default: int) -> int:
    try:
        return int((os.getenv(name, str(default)) or str(default)).strip())
    except Exception:
        return default

def _env_float(name: str, default: float) -> float:
    try:
        return float((os.getenv(name, str(default)) or str(default)).strip())
    except Exception:
        return default

def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "t", "yes", "y", "on")

# DRYRUN detection (any of these turns it on)
IS_DRYRUN: bool = (
    _env_bool("DRYRUN", False) or
    _env_bool("DRY_RUN", False) or
    _env_bool("IS_DRYRUN", False)
)

# RPC knobs (with broad fallbacks)
TX_RPC_URL: str = (
    _env_str("SOL_RPC_URL")
    or _env_str("TX_RPC_URL")
    or _env_str("SOLANA_RPC_URL")
)
TX_COMMITMENT: str = (
    _env_str("PF_TXLEARN_COMMITMENT", "")  # app-wide default seen in your env
    or _env_str("SOL_COMMITMENT", "")
    or _env_str("TX_COMMITMENT", "")
    or "confirmed"
)

_UA = {"user-agent": "pnlsniper/trader"}
_RPC_TIMEOUT = _env_float("ROUTER_RPC_TIMEOUT_SEC", 10.0)

# Optional budget hooks (fail-open)
try:
    from utils_pkg import budget  # type: ignore
except Exception:  # pragma: no cover
    class _BudgetNoop:
        def before_rpc(self, method: str, *, weight: Optional[int] = None) -> bool: return True
        def after_rpc(self, method: str, *, weight: Optional[int] = None) -> None:  return None
    budget = _BudgetNoop()  # type: ignore

# ──────────────────────────────────────────────────────────────────────────────
# Lightweight RPC helpers (requests in worker thread)
# ──────────────────────────────────────────────────────────────────────────────

def _rpc_post_blocking(method: str, params: list) -> dict:
    """
    Blocking JSON-RPC call; execute via asyncio.to_thread from async contexts.
    """
    if not TX_RPC_URL:
        raise RuntimeError("SOL_RPC_URL / TX_RPC_URL / SOLANA_RPC_URL not configured")
    if not budget.before_rpc(method):
        # behave as if the call failed pre-flight (budget exceeded)
        return {}
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    try:
        r = requests.post(TX_RPC_URL, json=payload, timeout=_RPC_TIMEOUT, headers=_UA)
        r.raise_for_status()
        return r.json()
    finally:
        try:
            budget.after_rpc(method)
        except Exception:
            pass

async def _rpc_post(method: str, params: list) -> dict:
    return await asyncio.to_thread(_rpc_post_blocking, method, params)

# ──────────────────────────────────────────────────────────────────────────────
# Token balance helpers
# ──────────────────────────────────────────────────────────────────────────────

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
            # parsed path (preferred)
            amt = it["account"]["data"]["parsed"]["info"]["tokenAmount"]["amount"]
            total += int(amt)
            continue
        except Exception:
            pass
        # fallback using uiAmount if present
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
                {"encoding": "jsonParsed", "commitment": TX_COMMITMENT},
            ],
        )
        return _sum_token_balance_raw_from_resp(js)
    except Exception as e:
        log.event("RPC_BALANCE_ERR", err=str(e))
        return 0

# ──────────────────────────────────────────────────────────────────────────────
# Public SELL API (used by scripts/main.py)
# ──────────────────────────────────────────────────────────────────────────────

async def sell_on_jupiter(mint: str, pct_of_wallet: float = 1.0) -> Optional[str]:
    """
    Sell `pct_of_wallet` (0..1) of wallet's balance for `mint` via router.sell_via_lightning.

    Returns:
        str tx signature on success, else None (including DRYRUN).
    """
    pct = max(0.0, min(1.0, float(pct_of_wallet)))
    if pct <= 0:
        log.event("SELL_SKIP_ZERO_PCT", mint=mint)
        return None

    # DRYRUN short-circuit (still compute balance to allow logging if needed)
    if IS_DRYRUN:
        log.event("SELL_DRYRUN", mint=mint, pct=pct)
        return None

    # Determine owner & balance
    kp = load_keypair()
    owner = get_pubkey_str(kp)

    balance_raw = await get_token_balance_raw(owner, mint)
    if balance_raw <= 0:
        log.event("SELL_SKIP_NO_BAL", mint=mint)
        return None

    amount_raw = max(1, int(round(balance_raw * pct)))

    if sell_via_lightning is None:
        log.event("SELL_ROUTER_UNAVAILABLE", mint=mint, amount_raw=amount_raw)
        return None

    # Optional slippage from env; let router defaults apply if not set
    slippage_bps_env = os.getenv("PF_SLIPPAGE_BPS")
    slippage_bps = int(slippage_bps_env) if (slippage_bps_env or "").strip().isdigit() else None

    try:
        sig = await sell_via_lightning(  # type: ignore[misc]
            mint=mint,
            token_program=None,        # Jupiter v6 doesn’t require choosing program
            amount_raw=amount_raw,
            **({"slippage_bps": slippage_bps} if slippage_bps is not None else {}),
        )
    except TypeError:
        # Support older signature without named kwargs
        sig = await sell_via_lightning(mint, None, amount_raw)  # type: ignore[call-arg]
    except Exception as e:
        log.event("SELL_CALL_ERR", mint=mint, err=str(e))
        return None

    if isinstance(sig, str) and sig:
        log.event("SELL_OK", mint=mint, pct=pct, amount_raw=amount_raw, sig=sig)
        return sig

    log.event("SELL_FAIL", mint=mint, pct=pct, amount_raw=amount_raw)
    return None

# ──────────────────────────────────────────────────────────────────────────────
# Exit engine hooks (minimal, non-blocking)
# ──────────────────────────────────────────────────────────────────────────────

class _ExitEngine:
    """
    Minimal exit engine used by main.py:
      - on_trade_tick(raw_event)
      - on_buy_filled(raw_id_or_mint, tx_sig)  # legacy-compatible name/shape
    Expand here if you later add stop-loss / TP / trailing logic.
    """
    def __init__(self) -> None:
        self._last_tick_note = 0.0

    async def on_trade_tick(self, raw_event: Dict[str, Any]) -> None:
        now = time.time()
        if now - self._last_tick_note > 10:
            self._last_tick_note = now
            sym = raw_event.get("symbol") or raw_event.get("ticker") or "?"
            log.every("exit.tick", n=10, msg="EXIT_TICK", symbol=sym)

    async def on_buy_filled(self, raw_id_or_mint: str, tx_sig: str) -> None:
        log.event("EXIT_ON_BUY_FILLED", mint=raw_id_or_mint, sig=tx_sig)
        # Place for post-buy actions (trailers, journaling, etc.)

exit_engine = _ExitEngine()

__all__ = ["IS_DRYRUN", "sell_on_jupiter", "exit_engine", "get_token_balance_raw"]
