# utils_pkg/pump_trader.py
from __future__ import annotations

import os
import json
import requests

from utils_pkg.solana_wallet import load_keypair, sign_and_send_b64_tx
from utils_pkg.solana_wallet import load_keypair, get_pubkey_str

PP_API_KEY       = os.getenv("PF_PP_KEY") or ""
PF_SLIPPAGE_BPS  = int(os.getenv("PF_SLIPPAGE_BPS", "1000"))
PF_PRIORITY_SOL  = float(os.getenv("PF_PRIORITY_SOL", "0.0005"))

def _is_true(v: str | None) -> bool:
    return str(v).lower() in {"1", "true", "yes", "y", "on"}

DRY_RUN = _is_true(os.getenv("DRY_RUN", "1"))

def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def current_trade_sol() -> float:
    """
    Read PF_TRADE_SOL (autosized by entrypoint) and clamp to min/max.
    Falls back to 0.01 if unset.
    """
    try:
        size = float(os.getenv("PF_TRADE_SOL", "0.01"))
    except Exception:
        size = 0.01
    try:
        mn  = float(os.getenv("PF_TRADE_MIN_SOL", "0.005"))
        mx  = float(os.getenv("PF_TRADE_MAX_SOL", "1000"))
    except Exception:
        mn, mx = 0.005, 1000.0
    return _clamp(size, mn, mx)

def build_buy_tx_via_portal(
    mint: str,
    payer_pubkey: str,
    *,
    amount_sol: float,
    slippage_bps: int,
    priority_sol: float,
) -> str:
    """
    Requests a prebuilt BUY transaction (base64) from PumpPortal.
    """
    url = "https://pumpportal.fun/api/trade"
    headers = {"content-type": "application/json", "x-api-key": PP_API_KEY}
    body = {
        "apiKey": PP_API_KEY,      # some builders expect it here too
        "publicKey": payer_pubkey,
        "action": "buy",
        "mint": mint,
        "amount": amount_sol,      # SOL amount
        "slippage": slippage_bps,  # integer BPS
        "priorityFee": priority_sol,
        "pool": "pump",
    }

    r = requests.post(url, headers=headers, data=json.dumps(body), timeout=20)
    r.raise_for_status()
    js = r.json()
    tx_b64 = js.get("transaction") or js.get("tx")
    if not tx_b64:
        raise RuntimeError(f"Builder response missing tx: {js}")
    return tx_b64

async def maybe_buy(mint: str, **kwargs) -> None:
    """
    Execute a buy on PumpPortal. Accepts **kwargs (ui, symbol, etc.)
    so callers can pass extra context without breaking this API.
    Respects DRY_RUN.
    """
    symbol = (
        kwargs.get("symbol")
        or (kwargs.get("ui") or {}).get("symbol")
        or ""
    )

    amt = current_trade_sol()

    if DRY_RUN:
        label = f"{symbol} ({mint})" if symbol else mint
        print(f"[DRYRUN] would buy {label} for {amt} SOL")
        return

    kp = load_keypair()
    pub = get_pubkey_str(kp)

    try:
        tx_b64 = build_buy_tx_via_portal(
            mint=mint,
            payer_pubkey=pub,
            amount_sol=amt,
            slippage_bps=PF_SLIPPAGE_BPS,
            priority_sol=PF_PRIORITY_SOL,
        )
    except Exception as e:
        print(f"[BUY ERR] builder failed for {mint}: {e}")
        return

    try:
        sig = await sign_and_send_b64_tx(tx_b64, kp, skip_preflight=True)
        print(f"[BUY OK] {symbol or mint} -> https://solscan.io/tx/{sig}")
    except Exception as e:
        print(f"[BUY ERR] send failed for {mint}: {e}")
