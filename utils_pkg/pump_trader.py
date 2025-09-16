# utils_pkg/pump_trader.py
from __future__ import annotations

import os
from typing import Dict, Any, Optional

import requests

from utils_pkg.solana_wallet import (
    load_keypair,
    get_pubkey_str,
    sign_and_send_b64_tx,
)

# ──────────────────────────────────────────────────────────────────────────────
# Env / config
# ──────────────────────────────────────────────────────────────────────────────
def _is_true(v: Optional[str]) -> bool:
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}

PP_API_KEY       = (os.getenv("PF_PP_KEY") or "").strip()
DRY_RUN          = _is_true(os.getenv("DRY_RUN", "0"))
PF_SLIPPAGE_BPS  = int(os.getenv("PF_SLIPPAGE_BPS", "1000"))     # 1000 bps = 10%
PF_PRIORITY_SOL  = float(os.getenv("PF_PRIORITY_SOL", "0.0005"))
PF_POOLS         = {p.strip().lower() for p in (os.getenv("PF_POOLS", "pump").split(","))}
USE_LOCAL_FALLBK = _is_true(os.getenv("PF_USE_LOCAL_FALLBACK", "0"))

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

# ──────────────────────────────────────────────────────────────────────────────
# PumpPortal helpers
# ──────────────────────────────────────────────────────────────────────────────
class TemporaryPumpPortal(Exception):
    """Non-fatal / transient response (retry later)."""

def _slippage_percent_from_bps(bps: int) -> float:
    # PumpPortal wants a *percent number*, not a fraction. e.g. 800 -> 8.0
    return float(bps) / 100.0

def _lightning_url() -> str:
    if not PP_API_KEY:
        raise RuntimeError("PF_PP_KEY is not set (trading requires a PumpPortal API key).")
    return f"https://pumpportal.fun/api/trade?api-key={PP_API_KEY}"

def _local_url() -> str:
    if not PP_API_KEY:
        raise RuntimeError("PF_PP_KEY is not set (trading requires a PumpPortal API key).")
    return f"https://pumpportal.fun/api/trade-local?api-key={PP_API_KEY}"

def _build_body(
    *,
    action: str,
    mint: str,
    payer_pubkey: str,
    amount_sol: float,
    pool: str,
    slippage_bps: int,
    priority_sol: float,
) -> Dict[str, Any]:
    # Lightning/local both accept this shape. For slippage: they want percent, not bps.
    return {
        "action": action,                 # "buy" or "sell"
        "mint": mint,
        "amount": float(amount_sol),
        "denominatedInSol": True,         # amount is in SOL
        "slippage": _slippage_percent_from_bps(slippage_bps),  # percent figure
        "priorityFee": float(priority_sol),
        "pool": pool,
        "publicKey": payer_pubkey,
    }

def _post_expect_json(url: str, body: Dict[str, Any], timeout: int = 20) -> Dict[str, Any]:
    """
    POST and return JSON. If response is 200 but not JSON (e.g., application/octet-stream),
    raise TemporaryPumpPortal so the caller can treat it as a transient and back off.
    """
    r = requests.post(url, json=body, timeout=timeout, allow_redirects=False)
    ctype = (r.headers.get("content-type") or "").split(";")[0].strip().lower()

    # Some endpoints reply 200 with binary (octet-stream) or plain text (e.g., "Bad Request").
    if r.status_code == 200 and ctype != "application/json":
        head = (r.text or "")[:180].replace("\n", " ")
        if ctype == "text/plain":
            raise RuntimeError(f"{url} returned text/plain 200: {head or 'Bad Request'}")
        raise TemporaryPumpPortal(f"non-JSON 200 ({ctype})")

    if r.status_code != 200:
        try:
            js = r.json()
        except Exception:
            js = {"error": (r.text or "").strip()[:240] or r.reason}
        raise RuntimeError(f"{url} {r.status_code}: {js}")

    try:
        return r.json()
    except Exception as e:
        raise TemporaryPumpPortal(f"json parse error: {e}")

def _call_lightning(body: Dict[str, Any]) -> Dict[str, Any]:
    return _post_expect_json(_lightning_url(), body)

def _call_local(body: Dict[str, Any]) -> Dict[str, Any]:
    return _post_expect_json(_local_url(), body)

# ──────────────────────────────────────────────────────────────────────────────
# Public: BUY helper (returns tx signature or None)
# ──────────────────────────────────────────────────────────────────────────────
# utils_pkg/pump_trader.py (replace the whole maybe_buy with this)
async def maybe_buy(mint: str, **kwargs) -> Optional[str]:
    """
    Execute a buy via PumpPortal.
    Returns the transaction signature on success, else None.
    """
    ui = kwargs.get("ui") or {}
    symbol = (kwargs.get("symbol") or ui.get("symbol") or "").strip()
    pool   = (kwargs.get("pool")   or ui.get("pool")   or "pump").strip().lower()

    if pool not in PF_POOLS:
        print(f"[skip] unsupported pool '{pool}' for {mint}; allowed={sorted(PF_POOLS)}; skipping.")
        return None

    label   = f"{symbol} ({mint})" if symbol else mint
    amt_sol = current_trade_sol()

    if DRY_RUN:
        print(f"[DRYRUN] would buy {label} on {pool} for {amt_sol} SOL")
        return None

    kp = load_keypair()
    payer_pub = get_pubkey_str(kp)

    body = _build_body(
        action="buy",
        mint=mint,
        payer_pubkey=payer_pub,
        amount_sol=amt_sol,
        pool=pool,
        slippage_bps=PF_SLIPPAGE_BPS,
        priority_sol=PF_PRIORITY_SOL,
    )

    # 1) Lightning
    try:
        resp = _call_lightning(body)
    except TemporaryPumpPortal as te:
        print(f"[BUY TEMP] lightning quirk for {mint}: {te}")
        return None
    except Exception as e:
        if USE_LOCAL_FALLBK:
            try:
                resp = _call_local(body)
            except TemporaryPumpPortal as te2:
                print(f"[BUY TEMP] local quirk for {mint}: {te2}")
                return None
            except Exception as e2:
                print(f"[BUY ERR] local builder failed for {mint}: {e2}")
                return None
        else:
            print(f"[BUY ERR] lightning call failed for {mint}: {e}")
            return None

    # 2) Handle response
    errs = resp.get("errors") or []
    if isinstance(errs, list) and errs:
        print(f"[BUY ERR] pumpportal errors for {mint}: {errs}")
        return None

    sig = resp.get("signature")
    if sig:
        print(f"[BUY OK] {label} — executed via Lightning. https://solscan.io/tx/{sig}")
        return sig

    # Local path: sign & send base64 tx if provided
    tx_b64 = resp.get("transaction") or resp.get("tx")
    if tx_b64:
        try:
            sig2 = await sign_and_send_b64_tx(tx_b64, kp, skip_preflight=True)
            print(f"[BUY OK] {label} — local sign. https://solscan.io/tx/{sig2}")
            return sig2
        except Exception as e:
            print(f"[BUY ERR] signing local tx failed for {mint}: {e}")
            return None

    print(f"[BUY ERR] unexpected PumpPortal response for {mint}: {json.dumps(resp)[:300]}")
    return None