from __future__ import annotations

"""
utils_pkg/trader.py — wallet token helpers + sell wrapper + exit-engine shim

Roles:
- DRYRUN detection (DRYRUN / DRY_RUN / IS_DRYRUN) shared with the rest of the app.
- Async-safe JSON-RPC helpers (requests in a worker thread).
- Token balance fetch via getTokenAccountsByOwner (jsonParsed).
- Sell helper: uses router.sell_via_lightning() → returns tx signature or None.
- Exit-engine shim: re-exports the real exit_engine from auto_exit_worker, or
  falls back to a minimal stub if that import is unavailable.

This module is intentionally lightweight so it can be imported early.
"""

import asyncio
import os
import time
from typing import Any, Dict, Optional

import requests

from logging_setup import get_logger
from utils_pkg.solana_wallet import load_keypair, get_pubkey_str

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
    _env_bool("DRYRUN", False)
    or _env_bool("DRY_RUN", False)
    or _env_bool("IS_DRYRUN", False)
)

# RPC knobs (shared with router/txlearn)
TX_RPC_URL: str = (
    _env_str("SOL_RPC_URL")
    or _env_str("TX_RPC_URL")
    or _env_str("SOLANA_RPC_URL")
)
TX_COMMITMENT: str = (
    _env_str("PF_TXLEARN_COMMITMENT", "")
    or _env_str("SOL_COMMITMENT", "")
    or _env_str("TX_COMMITMENT", "")
    or "confirmed"
)

_UA = {"user-agent": "pnlsniper/trader", "accept": "application/json"}
_RPC_TIMEOUT = _env_float("ROUTER_RPC_TIMEOUT_SEC", 10.0)

LAMPORTS_PER_SOL = 1_000_000_000

# Optional budget hooks (fail-open)
try:
    from utils_pkg import budget  # type: ignore
except Exception:  # pragma: no cover

    class _BudgetNoop:
        def before_rpc(self, method: str, *, weight: Optional[int] = None) -> bool:
            return True

        def after_rpc(self, method: str, *, weight: Optional[int] = None) -> None:
            return None

    budget = _BudgetNoop()  # type: ignore


# Router sell entrypoint (delegates to lightning_impl internally)
try:
    from utils_pkg.router import (
        sell_via_lightning as _router_sell_via_lightning,
    )  # async
except Exception:  # pragma: no cover
    _router_sell_via_lightning = None  # type: ignore


# ──────────────────────────────────────────────────────────────────────────────
# Lightweight RPC helpers (requests in worker thread)
# ──────────────────────────────────────────────────────────────────────────────


def _rpc_post_blocking(method: str, params: list) -> dict:
    """
    Blocking JSON-RPC call; execute via asyncio.to_thread from async contexts.
    """
    if not TX_RPC_URL:
        raise RuntimeError(
            "RPC URL not configured (SOL_RPC_URL / TX_RPC_URL / SOLANA_RPC_URL)"
        )

    if not budget.before_rpc(method):
        # behave as if the call failed pre-flight (budget exceeded)
        return {}

    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    try:
        r = requests.post(
            TX_RPC_URL,
            json=payload,
            timeout=_RPC_TIMEOUT,
            headers=_UA,
        )
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
            tok = it["account"]["data"]["parsed"]["info"]["tokenAmount"]
            ui = float(tok.get("uiAmount") or 0.0)
            dec = int(tok.get("decimals") or 0)
            total += int(round(ui * (10**dec)))
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
        raw = _sum_token_balance_raw_from_resp(js)
        log.every(
            "bal.token",
            n=50,
            msg="TOKEN_BALANCE",
            owner=owner,
            mint=mint,
            raw=raw,
        )
        return raw
    except Exception as e:
        log.event("RPC_BALANCE_ERR", err=str(e)[:200], mint=mint)
        return 0


async def get_wallet_token_balance_raw(mint: str) -> int:
    """
    Convenience: load keypair and fetch wallet's total raw balance for `mint`.
    """
    kp = load_keypair()
    owner = get_pubkey_str(kp)
    return await get_token_balance_raw(owner, mint)


# ──────────────────────────────────────────────────────────────────────────────
# Public SELL API (used by scripts/main.py / manual tools)
# ──────────────────────────────────────────────────────────────────────────────


async def sell_on_jupiter(
    mint: str,
    pct_of_wallet: float = 1.0,
    slippage_bps: Optional[int] = None,
) -> Optional[str]:
    """
    Sell `pct_of_wallet` (0..1) of wallet's balance for `mint` via router.sell_via_lightning.

    Returns:
        str tx signature on success, else None (including DRYRUN).

    Notes:
        - If IS_DRYRUN, we skip sending the tx and just log.
        - If router sell entrypoint is missing, we log + return None.
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

    # DRYRUN short-circuit
    if IS_DRYRUN:
        log.event(
            "SELL_DRYRUN",
            mint=mint,
            pct=pct,
            amount_raw=amount_raw,
            raw_balance=balance_raw,
        )
        return None

    if _router_sell_via_lightning is None:
        log.event(
            "SELL_ROUTER_UNAVAILABLE",
            mint=mint,
            amount_raw=amount_raw,
        )
        return None

    # Optional slippage from env if not explicitly provided
    if slippage_bps is None:
        sbps_env = os.getenv("PF_SLIPPAGE_BPS", "").strip()
        if sbps_env.isdigit():
            try:
                slippage_bps = int(sbps_env)
            except Exception:
                slippage_bps = None

    try:
        if slippage_bps is not None:
            sig = await _router_sell_via_lightning(  # type: ignore[misc]
                mint=mint,
                token_program=None,  # Jupiter v6 doesn’t require choosing program
                amount_raw=amount_raw,
                slippage_bps=slippage_bps,
            )
        else:
            sig = await _router_sell_via_lightning(  # type: ignore[misc]
                mint=mint,
                token_program=None,
                amount_raw=amount_raw,
            )

    except TypeError:
        # Support older signature without named kwargs
        sig = await _router_sell_via_lightning(  # type: ignore[call-arg]
            mint,
            None,
            amount_raw,
        )
    except Exception as e:
        log.event("SELL_CALL_ERR", mint=mint, err=str(e)[:200])
        return None

    if isinstance(sig, str) and sig:
        log.event(
            "SELL_OK",
            mint=mint,
            pct=pct,
            amount_raw=amount_raw,
            sig=sig,
        )
        return sig

    log.event(
        "SELL_FAIL",
        mint=mint,
        pct=pct,
        amount_raw=amount_raw,
    )
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Exit engine shim — re-export the real one from auto_exit_worker
# ──────────────────────────────────────────────────────────────────────────────


try:
    from utils_pkg.auto_exit_worker import exit_engine as _exit_engine  # type: ignore
except Exception:  # pragma: no cover

    class _FallbackExitEngine:
        """
        Minimal stub used only if auto_exit_worker is unavailable.
        Real logic now lives in utils_pkg.auto_exit_worker.exit_engine.
        """

        def __init__(self) -> None:
            self._last_tick_note = 0.0

        async def on_trade_tick(self, raw_event: Dict[str, Any]) -> None:
            now = time.time()
            if now - self._last_tick_note > 10:
                self._last_tick_note = now
                sym = raw_event.get("symbol") or raw_event.get("ticker") or "?"
                log.every("exit.tick", n=10, msg="EXIT_TICK_STUB", symbol=sym)

        async def on_buy_filled(self, ev: Dict[str, Any], tx_sig: str | None = None) -> None:
            mint = ev.get("mint") or ev.get("id") or "?"
            log.event("EXIT_ON_BUY_FILLED_STUB", mint=mint, sig=(tx_sig or "?"))

    _exit_engine = _FallbackExitEngine()  # type: ignore

# Public alias
exit_engine = _exit_engine


# ──────────────────────────────────────────────────────────────────────────────
# Exports
# ──────────────────────────────────────────────────────────────────────────────

__all__ = [
    # DRYRUN / config
    "IS_DRYRUN",
    "TX_RPC_URL",
    "TX_COMMITMENT",
    "LAMPORTS_PER_SOL",
    # token helpers
    "get_token_balance_raw",
    "get_wallet_token_balance_raw",
    # sells
    "sell_on_jupiter",
    # exit engine shim
    "exit_engine",
]
