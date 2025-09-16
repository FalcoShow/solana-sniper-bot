# utils_pkg/pump_trader.py
from __future__ import annotations

import os
import json
from typing import Optional, Tuple, Dict, Any

import requests

from utils_pkg.solana_wallet import (
    load_keypair,
    get_pubkey_str,
    sign_and_send_b64_tx,
)

# -----------------------------------------------------------------------------
# Env / config
# -----------------------------------------------------------------------------
def _is_true(v: str | None) -> bool:
    return str(v).lower() in {"1", "true", "yes", "y", "on"}

PP_API_KEY      = (os.getenv("PF_PP_KEY") or "").strip()
DRY_RUN         = _is_true(os.getenv("DRY_RUN", "1"))
PF_SLIPPAGE_BPS = int(os.getenv("PF_SLIPPAGE_BPS", "1000"))    # 1000 bps = 10.0%
PF_PRIORITY_SOL = float(os.getenv("PF_PRIORITY_SOL", "0.0005"))

def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def current_trade_sol() -> float:
    """Read PF_TRADE_SOL (autosized externally) and clamp to min/max."""
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

# -----------------------------------------------------------------------------
# PumpPortal Lightning (/api/trade) — server-side execution
# -----------------------------------------------------------------------------
def call_lightning_trade(
    *,
    action: str,
    mint: str,
    payer_pubkey: str,
    amount_sol: float,
    slippage_bps: int,
    priority_sol: float,
    pool: str = "pump",
    timeout: int = 20,
) -> Dict[str, Any]:
    """
    POST https://pumpportal.fun/api/trade?api-key=...
    Lightning API executes and typically returns {"signature": "...", "errors": []}.
    Some flows may also return {"transaction": "..."} (rare). We bubble the json.
    """
    if not PP_API_KEY:
        raise RuntimeError("PF_PP_KEY is not set (trading requires a PumpPortal API key).")

    url = f"https://pumpportal.fun/api/trade?api-key={PP_API_KEY}"

    # Lightning expects slippage as PERCENT, not bps.
    body = {
        "action": action,                 # "buy" or "sell"
        "mint": mint,
        "amount": float(amount_sol),
        "denominatedInSol": True,         # amount is in SOL
        "slippage": float(slippage_bps) / 100.0,   # convert bps -> %
        "priorityFee": float(priority_sol),
        "pool": pool,
        "publicKey": payer_pubkey,
    }

    r = requests.post(url, json=body, timeout=timeout)
    # Lightning returns 200 even for some errors; always parse the body.
    try:
        js = r.json()
    except Exception:
        r.raise_for_status()
        raise

    # Normalize keys so callers can branch once.
    if "tx" in js and "transaction" not in js:
        js["transaction"] = js["tx"]

    if not r.ok:
        raise RuntimeError(f"Lightning {r.status_code}: {js}")

    return js

# -----------------------------------------------------------------------------
# Maybe buy helper
# -----------------------------------------------------------------------------
async def maybe_buy(mint: str, **kwargs) -> None:
    """
    Execute a buy through PumpPortal Lightning. If Lightning returns a signature,
    we treat it as success. If it (unusually) returns a base64 transaction, we
    sign & send with our wallet.
    """
    symbol = (kwargs.get("symbol") or (kwargs.get("ui") or {}).get("symbol") or "").strip()
    label = f"{symbol} ({mint})" if symbol else mint

    amt_sol = current_trade_sol()

    if DRY_RUN:
        print(f"[DRYRUN] would buy {label} for {amt_sol} SOL")
        return

    kp = load_keypair()
    payer_pub = get_pubkey_str(kp)

    try:
        resp = call_lightning_trade(
            action="buy",
            mint=mint,
            payer_pubkey=payer_pub,
            amount_sol=amt_sol,
            slippage_bps=PF_SLIPPAGE_BPS,
            priority_sol=PF_PRIORITY_SOL,
            pool="pump",
        )
    except Exception as e:
        print(f"[BUY ERR] lightning call failed for {mint}: {e}")
        return

    # PumpPortal may include an 'errors' array even with HTTP 200
    errs = resp.get("errors") or []
    if isinstance(errs, list) and len(errs) > 0:
        print(f"[BUY ERR] lightning returned errors for {mint}: {errs}")
        return

    # Most common: server executed and we get a signature back.
    sig = resp.get("signature")
    if sig:
        print(f"[BUY OK] {label} — executed via Lightning. https://solscan.io/tx/{sig}")
        return

    # Rare builder-style response: server gave us a base64 tx to sign.
    tx_b64 = resp.get("transaction")
    if tx_b64:
        try:
            sig2 = await sign_and_send_b64_tx(tx_b64, kp, skip_preflight=True)
            print(f"[BUY OK] {label} — signed & sent. https://solscan.io/tx/{sig2}")
        except Exception as e:
            print(f"[BUY ERR] send failed for {mint}: {e}")
        return

    # If we get here, the response had neither signature nor transaction.
    print(f"[BUY ERR] unexpected Lightning response for {mint}: {json.dumps(resp)[:400]}")
