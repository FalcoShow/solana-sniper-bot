# utils_pkg/pump_trader.py
from __future__ import annotations

import os
import json
from typing import Dict, Any, Optional

import requests

from utils_pkg.solana_wallet import (
    load_keypair,
    get_pubkey_str,
    sign_and_send_b64_tx,
)

# -----------------------------------------------------------------------------
# Env / config
# -----------------------------------------------------------------------------
def _is_true(v: Optional[str]) -> bool:
    return str(v).lower() in {"1", "true", "yes", "y", "on"}

PP_API_KEY       = (os.getenv("PF_PP_KEY") or "").strip()
DRY_RUN          = _is_true(os.getenv("DRY_RUN", "1"))
PF_SLIPPAGE_BPS  = int(os.getenv("PF_SLIPPAGE_BPS", "1000"))   # bps (e.g., 1000 = 10.00%)
PF_PRIORITY_SOL  = float(os.getenv("PF_PRIORITY_SOL", "0.0005"))
PP_MODE          = (os.getenv("PF_PP_MODE") or "local").strip().lower()  # "local" (default) or "lightning"

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
# PumpPortal: Local Transaction API (preferred)
# -----------------------------------------------------------------------------
def call_local_trade(
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
    POST https://pumpportal.fun/api/trade-local?api-key=...
    Returns: {"transaction":"<b64>", "errors":[...]} or {"errors":[...]}.
    - slippage is PERCENT for PumpPortal (not bps), so we convert bps->%.
    """
    if not PP_API_KEY:
        raise RuntimeError("PF_PP_KEY is not set (trading requires a PumpPortal API key).")

    url = f"https://pumpportal.fun/api/trade-local?api-key={PP_API_KEY}"
    body = {
        "action": action,                     # "buy" / "sell"
        "mint": mint,
        "amount": float(amount_sol),          # in SOL because we set denominatedInSol=True
        "denominatedInSol": True,
        "slippage": float(slippage_bps) / 100.0,  # bps -> %
        "priorityFee": float(priority_sol),
        "pool": pool,
        "publicKey": payer_pubkey,            # VERY IMPORTANT: builds tx for OUR wallet
        # you could add "skipPreflight": False, "jitoOnly": False if desired
    }
    r = requests.post(url, json=body, timeout=timeout)

    # /trade-local usually returns 200 even with errors; parse body either way
    try:
        js = r.json()
    except Exception:
        r.raise_for_status()
        raise

    # normalize optional alt key
    if "tx" in js and "transaction" not in js:
        js["transaction"] = js["tx"]

    return js

# -----------------------------------------------------------------------------
# PumpPortal: Lightning API (optional fallback)
# -----------------------------------------------------------------------------
def call_lightning_trade(
    *,
    action: str,
    mint: str,
    amount_sol: float,
    slippage_bps: int,
    priority_sol: float,
    pool: str = "pump",
    timeout: int = 20,
) -> Dict[str, Any]:
    """
    POST https://pumpportal.fun/api/trade?api-key=...
    Executes server-side using the wallet LINKED to the API key. Do NOT use this
    if you want fills to hit your local id.json wallet.
    """
    if not PP_API_KEY:
        raise RuntimeError("PF_PP_KEY is not set (trading requires a PumpPortal API key).")

    url = f"https://pumpportal.fun/api/trade?api-key={PP_API_KEY}"
    body = {
        "action": action,
        "mint": mint,
        "amount": float(amount_sol),
        "denominatedInSol": True,
        "slippage": float(slippage_bps) / 100.0,  # bps -> %
        "priorityFee": float(priority_sol),
        "pool": pool,
    }
    r = requests.post(url, json=body, timeout=timeout)
    try:
        js = r.json()
    except Exception:
        r.raise_for_status()
        raise
    if "tx" in js and "transaction" not in js:
        js["transaction"] = js["tx"]
    return js

# -----------------------------------------------------------------------------
# Buy helper
# -----------------------------------------------------------------------------
async def maybe_buy(mint: str, **kwargs) -> None:
    """
    Execute a buy. Default path is Local Transaction API so fills land in the
    same wallet we control (id.json). Lightning is available as an opt-in mode.
    """
    symbol = (kwargs.get("symbol") or (kwargs.get("ui") or {}).get("symbol") or "").strip()
    label = f"{symbol} ({mint})" if symbol else mint
    amount_sol = current_trade_sol()

    if DRY_RUN:
        print(f"[DRYRUN] would buy {label} for {amount_sol} SOL")
        return

    if not PP_API_KEY:
        print("[BUY ERR] PF_PP_KEY is not set")
        return

    # use our local keypair & pubkey
    kp = load_keypair()
    payer_pub = get_pubkey_str(kp)

    try:
        if PP_MODE == "lightning":
            resp = call_lightning_trade(
                action="buy",
                mint=mint,
                amount_sol=amount_sol,
                slippage_bps=PF_SLIPPAGE_BPS,
                priority_sol=PF_PRIORITY_SOL,
                pool="pump",
            )
        else:
            # default / recommended path
            resp = call_local_trade(
                action="buy",
                mint=mint,
                payer_pubkey=payer_pub,
                amount_sol=amount_sol,
                slippage_bps=PF_SLIPPAGE_BPS,
                priority_sol=PF_PRIORITY_SOL,
                pool="pump",
            )
    except Exception as e:
        print(f"[BUY ERR] request failed for {mint}: {e}")
        return

    # PumpPortal often includes 'errors' even with HTTP 200
    errs = resp.get("errors") or []
    if isinstance(errs, list) and len(errs) > 0:
        print(f"[BUY ERR] API returned errors for {mint}: {errs}")
        return

    # Lightning success returns a server-side 'signature'
    sig = resp.get("signature")
    if sig:
        print(f"[BUY OK] {label} — executed via Lightning. https://solscan.io/tx/{sig}")
        return

    # Local success returns a base64 'transaction' to sign and send
    tx_b64 = resp.get("transaction")
    if tx_b64:
        try:
            sig2 = await sign_and_send_b64_tx(tx_b64, kp, skip_preflight=True)
            print(f"[BUY OK] {label} — signed & sent. https://solscan.io/tx/{sig2}")
        except Exception as e:
            print(f"[BUY ERR] send failed for {mint}: {e}")
        return

    print(f"[BUY ERR] unexpected response for {mint}: {json.dumps(resp)[:400]}")
