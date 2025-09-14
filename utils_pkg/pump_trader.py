# utils_pkg/pump_trader.py
# ----------------------------------------------------------------------
# PumpPortal trading helpers: build buy tx, sign & send via your wallet.
#
# Reads dynamic sizing from PF_TRADE_SOL (set by scripts/entrypoint.py).
# Honors PF_SLIPPAGE_BPS, PF_PRIORITY_SOL, DRY_RUN.
#
# ENV used (all optional unless noted):
#   PF_PP_KEY                (required for PumpPortal)
#   PF_PP_URL                default https://pumpportal.fun/api/trade
#   PF_PP_ACCOUNT            optional hint/account tag for PP
#   PF_SLIPPAGE_BPS          default 1000  (10%)
#   PF_PRIORITY_SOL          default 0.0005 SOL
#   PF_TRADE_SOL             set/updated by autosizer
#   DRY_RUN                  "1"/"true" to simulate
#   SOL_RPC_URL              (required) RPC endpoint for submit
#
# Dependencies:
#   - utils_pkg.solana_wallet.load_keypair()
# ----------------------------------------------------------------------

from __future__ import annotations

import os
import json
import logging
from typing import Optional, Dict, Any
from base64 import b64decode

import requests
from solana.rpc.async_api import AsyncClient
from solana.rpc.types import TxOpts
from solana.transaction import Transaction

from utils_pkg.solana_wallet import load_keypair

LOG = logging.getLogger("pump_trader")


# ----------------------------- env helpers -----------------------------

def _env_float(name: str, default: float) -> float:
    try:
        return float(str(os.getenv(name, default)).strip())
    except Exception:
        return float(default)


def _env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name, default)).strip())
    except Exception:
        return int(default)


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "on")


def _current_trade_sol() -> float:
    """
    Always read PF_TRADE_SOL at order time so autosizer updates apply.
    """
    try:
        return float(os.getenv("PF_TRADE_SOL", "0.01"))
    except Exception:
        return 0.01


def _rpc_url() -> str:
    url = os.getenv("SOL_RPC_URL")
    if not url:
        raise RuntimeError("SOL_RPC_URL is required (Helius or other mainnet RPC).")
    return url


# --------------------------- portal builders ---------------------------

def _pp_url() -> str:
    return os.getenv("PF_PP_URL", "https://pumpportal.fun/api/trade")


def _pp_headers() -> Dict[str, str]:
    headers = {"content-type": "application/json"}
    api_key = os.getenv("PF_PP_KEY")
    if api_key:
        headers["x-api-key"] = api_key  # some setups require header
    return headers


def _build_trade_payload(
    *,
    action: str,
    mint: str,
    payer_pubkey: str,
    amount_sol: float,
    slippage_bps: int,
    priority_sol: float,
) -> Dict[str, Any]:
    """
    Build the PumpPortal trade payload (buy path). Some PP deployments accept
    apiKey in both header and body; we include both to be safe.
    """
    payload: Dict[str, Any] = {
        "action": action,                 # "buy" | "sell"
        "mint": mint,
        "publicKey": payer_pubkey,
        "amount": float(amount_sol),      # SOL to spend (for buy)
        "slippage": int(slippage_bps),    # in bps
        "priorityFee": float(priority_sol),
        "pool": "pump",
    }
    api_key = os.getenv("PF_PP_KEY")
    if api_key:
        payload["apiKey"] = api_key
    # Optional tagging/analytics
    pp_acct = os.getenv("PF_PP_ACCOUNT")
    if pp_acct:
        payload["account"] = pp_acct
    return payload


def _request_trade_tx(payload: Dict[str, Any]) -> str:
    """
    POST to PumpPortal, return base64-encoded transaction string.
    """
    url = _pp_url()
    headers = _pp_headers()
    try:
        r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=25)
    except Exception as e:
        raise RuntimeError(f"PumpPortal request error: {e}") from e

    if not r.ok:
        # include server message for debugging
        snippet = r.text[:800]
        raise RuntimeError(f"PumpPortal error {r.status_code}: {snippet}")

    try:
        js = r.json()
    except Exception as e:
        raise RuntimeError(f"PumpPortal bad JSON: {r.text[:800]}") from e

    tx_b64 = js.get("transaction") or js.get("tx") or js.get("data", {}).get("transaction")
    if not tx_b64:
        raise RuntimeError(f"PumpPortal response missing tx: {js}")
    return tx_b64


# ---------------------------- tx submission ----------------------------

async def _sign_and_send_b64_tx(tx_b64: str) -> str:
    """
    Sign the PP-built transaction with your wallet and submit to SOL_RPC_URL.
    Returns the transaction signature (base58).
    """
    kp = load_keypair()
    try:
        tx = Transaction.deserialize(b64decode(tx_b64))
    except Exception as e:
        raise RuntimeError(f"Failed to decode/deserialize tx from portal: {e}") from e

    # Sign and serialize
    tx.sign(kp)
    raw = tx.serialize()  # bytes

    rpc = _rpc_url()
    async with AsyncClient(rpc, timeout=30) as client:
        try:
            resp = await client.send_raw_transaction(
                raw,
                opts=TxOpts(skip_preflight=True, max_retries=3),
            )
        except Exception as e:
            raise RuntimeError(f"send_raw_transaction failed: {e}") from e

        # solana-py returns RPCResponse with .value or dict-like
        sig = None
        if isinstance(resp, dict):
            sig = resp.get("result") or resp.get("value")
        else:
            sig = getattr(resp, "value", None) or getattr(resp, "result", None)

        if not sig:
            raise RuntimeError(f"Unexpected RPC response: {resp}")

        # (Optional) you can confirm here if your flow needs it:
        # await client.confirm_transaction(sig, commitment="confirmed")

    return str(sig)


# ------------------------------- public API ----------------------------

async def buy_via_portal(mint: str) -> Optional[str]:
    """
    Buy token (mint) via PumpPortal using auto-sized PF_TRADE_SOL.
    Returns the tx signature, or None in DRY_RUN.
    """
    kp = load_keypair()
    payer = str(kp.pubkey())

    amount_sol = _current_trade_sol()
    slippage_bps = _env_int("PF_SLIPPAGE_BPS", 1000)
    priority_sol = _env_float("PF_PRIORITY_SOL", 0.0005)

    if _env_bool("DRY_RUN", False):
        print(f"[DRYRUN] would buy {mint} for {amount_sol} SOL "
              f"(slippage {slippage_bps} bps, priority {priority_sol} SOL)")
        return None

    payload = _build_trade_payload(
        action="buy",
        mint=mint,
        payer_pubkey=payer,
        amount_sol=amount_sol,
        slippage_bps=slippage_bps,
        priority_sol=priority_sol,
    )

    tx_b64 = _request_trade_tx(payload)
    sig = await _sign_and_send_b64_tx(tx_b64)
    print(f"[BUY] {mint}  amount={amount_sol} SOL  slippage={slippage_bps}bps  "
          f"priority={priority_sol} SOL  -> sig={sig}")
    return sig


# ---------------------------- (optional) sell --------------------------
# If/when you want to route sells via PumpPortal as well, add a function like
# `sell_via_portal(...)` mirroring the buy path above, but set action="sell"
# and adjust payload fields per your PP deployment (amount in tokens or pct).
# Your current repo’s AI/exit logic may already be using Jupiter/routeguard.
