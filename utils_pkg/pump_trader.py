# utils_pkg/pump_trader.py
from __future__ import annotations

import os
import json
from typing import Optional

import requests
import base58

from utils_pkg.solana_wallet import (
    load_keypair,
    get_pubkey_str,
    sign_and_send_b64_tx,
)

# -----------------------------------------------------------------------------
# env / config
# -----------------------------------------------------------------------------
PP_BASE = "https://pumpportal.fun"
PP_KEY  = (os.getenv("PF_PP_KEY") or "").strip()

def _is_true(v: Optional[str]) -> bool:
    return str(v).lower() in {"1", "true", "yes", "y", "on"}

DRY_RUN = _is_true(os.getenv("DRY_RUN", "1"))

# Trade sizing (entrypoint may update PF_TRADE_SOL dynamically)
def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def current_trade_sol() -> float:
    try:
        sz = float(os.getenv("PF_TRADE_SOL", "0.01"))
    except Exception:
        sz = 0.01
    try:
        mn = float(os.getenv("PF_TRADE_MIN_SOL", "0.005"))
        mx = float(os.getenv("PF_TRADE_MAX_SOL", "1000"))
    except Exception:
        mn, mx = 0.005, 1000.0
    return _clamp(sz, mn, mx)

# Swap params
def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default

PF_SLIPPAGE_BPS = _env_int("PF_SLIPPAGE_BPS", 1000)       # 10.00% default safety for new pairs
PF_PRIORITY_SOL = _env_float("PF_PRIORITY_SOL", 0.0005)   # 5e-4 SOL

# -----------------------------------------------------------------------------
# PumpPortal HTTP helpers
# -----------------------------------------------------------------------------
def _pp_headers() -> dict:
    if not PP_KEY:
        raise RuntimeError("PF_PP_KEY is not set; PumpPortal trades need an API key")
    # send in both common places; some deployments check one or the other
    return {
        "accept": "application/json",
        "content-type": "application/json",
        "api-key": PP_KEY,
        "x-api-key": PP_KEY,
        "authorization": f"Bearer {PP_KEY}",
    }

def _pp_post_json(path: str, payload: dict, timeout: int = 20) -> dict:
    r = requests.post(f"{PP_BASE}{path}", headers=_pp_headers(), json=payload, timeout=timeout)
    if r.status_code == 403:
        raise PermissionError(
            "PumpPortal returned 403 Forbidden. Your PF_PP_KEY likely lacks trading permissions "
            "(WS read-only keys can’t POST). Request trading access for your key."
        )
    r.raise_for_status()
    return r.json()

def build_buy_tx_via_portal(
    mint: str,
    payer_pubkey: str,
    *,
    amount_sol: float,
    slippage_bps: int,
    priority_sol: float,
) -> str:
    js = _pp_post_json("/api/trade", {
        "apiKey": PP_KEY,           # some builders also check this in the JSON
        "publicKey": payer_pubkey,
        "action": "buy",
        "mint": mint,
        "amount": amount_sol,       # SOL amount
        "slippage": slippage_bps,   # integer BPS
        "priorityFee": priority_sol,
        "pool": "pump",
    })
    tx_b64 = js.get("transaction") or js.get("tx")
    if not tx_b64:
        raise RuntimeError(f"Builder response missing transaction: {js}")
    return tx_b64
# -----------------------------------------------------------------------------
# Validation
# -----------------------------------------------------------------------------
def _assert_mint(mint: str) -> None:
    try:
        if len(base58.b58decode(mint)) != 32:
            raise ValueError("decoded length != 32")
    except Exception as e:
        raise ValueError(f"Invalid mint '{mint}': {e}")

# -----------------------------------------------------------------------------
# Builder: BUY
# -----------------------------------------------------------------------------
def build_buy_tx_via_portal(
    mint: str,
    payer_pubkey: str,
    *,
    amount_sol: float,
    slippage_bps: int,
    priority_sol: float,
) -> str:
    """
    Build a BUY tx via PumpPortal Lightning Transaction API.

    Auth is ONLY via the query param `api-key`, not headers.
    """
    api_key = (os.getenv("PF_PP_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("PF_PP_KEY is not set")

    # per docs, slippage is a percent (e.g., 1.0 == 1%) not bps
    slippage_percent = max(0.01, float(slippage_bps) / 100.0)

    url = f"https://pumpportal.fun/api/trade?api-key={api_key}"
    body = {
        "action": "buy",
        "mint": mint,
        "amount": float(amount_sol),     # SOL amount
        "denominatedInSol": True,        # amount is in SOL, not tokens
        "slippage": slippage_percent,    # percent
        "priorityFee": float(priority_sol),
        "pool": "pump",                  # or 'auto' / other pools if you want
        # "skipPreflight": True,         # optional; defaults true per docs
        # "jitoOnly": False,             # optional
        "publicKey": payer_pubkey,       # they also accept this field
    }

    r = requests.post(url, json=body, timeout=20)
    if r.status_code == 403:
        raise PermissionError(
            "PumpPortal returned 403. The key in the URL is missing/invalid, or not enabled for trading."
        )
    r.raise_for_status()
    js = r.json()
    tx_b64 = js.get("transaction") or js.get("tx")
    if not tx_b64:
        raise RuntimeError(f"Builder response missing tx: {js}")
    return tx_b64

# -----------------------------------------------------------------------------
# Public: maybe_buy
# -----------------------------------------------------------------------------
async def maybe_buy(mint: str, **kwargs) -> Optional[str]:
    """
    Execute a buy via PumpPortal.
    Returns the signature string when broadcasted; None on dry-run or failure.
    Accepts **kwargs (e.g., ui, symbol) for logging context.
    """
    symbol = (kwargs.get("symbol") or (kwargs.get("ui") or {}).get("symbol") or "").strip()
    label  = f"{symbol} ({mint})" if symbol else mint

    amt_sol = current_trade_sol()

    if DRY_RUN:
        print(f"[DRYRUN] would buy {label} for {amt_sol} SOL")
        return None

    kp = load_keypair()
    owner = get_pubkey_str(kp)

    try:
        tx_b64 = build_buy_tx_via_portal(
            mint=mint,
            payer_pubkey=owner,
            amount_sol=amt_sol,
            slippage_bps=PF_SLIPPAGE_BPS,
            priority_sol=PF_PRIORITY_SOL,
        )
    except Exception as e:
        print(f"[BUY ERR] builder failed for {mint}: {e}")
        return None

    try:
        sig = await sign_and_send_b64_tx(tx_b64, kp, skip_preflight=True)
        print(f"[BUY OK] {label} -> https://solscan.io/tx/{sig}")
        return sig
    except Exception as e:
        print(f"[BUY ERR] send failed for {mint}: {e}")
        return None
