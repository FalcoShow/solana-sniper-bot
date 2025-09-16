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
    return str(v or "").strip().lower() in {"1", "true", "yes", "y", "on"}

PP_API_KEY       = (os.getenv("PF_PP_KEY") or "").strip()
DRY_RUN          = _is_true(os.getenv("DRY_RUN", "0"))
PF_SLIPPAGE_BPS  = int(os.getenv("PF_SLIPPAGE_BPS", "800"))       # default 8.00%
PF_PRIORITY_SOL  = float(os.getenv("PF_PRIORITY_SOL", "0.002"))
PF_POOLS         = {p.strip().lower() for p in (os.getenv("PF_POOLS", "pump").split(","))}
USE_LOCAL_FALLBK = _is_true(os.getenv("PF_USE_LOCAL_FALLBACK", "0"))
PF_PP_ACCOUNT = (os.getenv("PF_PP_ACCOUNT") or "").strip() or None

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
    # Lightning/local both accept this shape. Slippage is % (not bps).
    return {
        "action": action,                 # "buy" or "sell"
        "mint": mint,
        "amount": float(amount_sol),
        "denominatedInSol": True,         # amount is in SOL
        "slippage": _slippage_percent_from_bps(slippage_bps),  # percent
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

    # 200 but not JSON: Lightning sometimes does this under load → treat as transient.
    if r.status_code == 200 and ctype != "application/json":
        raise TemporaryPumpPortal(f"non-JSON 200 ({ctype})")

    # Non-200: surface JSON if available.
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
# Public: BUY helper
# ──────────────────────────────────────────────────────────────────────────────
async def maybe_buy(mint: str, **kwargs) -> Optional[str]:
    """
    Execute a buy via PumpPortal Lightning and return the transaction signature
    (or None on skip/error). Only trades tokens whose `pool` is allowed by PF_POOLS.
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
    payer_pub = PF_PP_ACCOUNT or get_pubkey_str(kp) 

    body = _build_body(
        action="buy",
        mint=mint,
        payer_pubkey=payer_pub,
        amount_sol=amt_sol,
        pool=pool,
        slippage_bps=PF_SLIPPAGE_BPS,
        priority_sol=PF_PRIORITY_SOL,
    )

    # 1) Lightning (primary path)
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

    # 2) Handle JSON result → return signature if we have one
    return await _handle_trade_response(label, resp, kp, mint)

async def _handle_trade_response(label: str, resp: Dict[str, Any], kp, mint: str) -> Optional[str]:
    """
    Normalize PumpPortal responses:
      - If 'errors' → transient/skip.
      - If 'signature' → success (executed server-side).
      - If 'transaction' (base64) → sign & send locally.
    Returns the broadcast signature or None.
    """
    errs = resp.get("errors") or []
    if isinstance(errs, list) and errs:
        msg = " | ".join(str(e) for e in errs)
        if "Failed to get bonding curve" in msg:
            print(f"[BUY TEMP] bonding-curve not ready for {mint}; skipping for now.")
            return None
        print(f"[BUY ERR] pumpportal errors for {mint}: {errs}")
        return None

    sig = resp.get("signature")
    if sig:
        print(f"[BUY OK] {label} — executed via Lightning. https://solscan.io/tx/{sig}")
        return sig

    tx_b64 = resp.get("transaction") or resp.get("tx")
    if tx_b64:
        try:
            sig2 = await sign_and_send_b64_tx(tx_b64, kp, skip_preflight=True)
            print(f"[BUY OK] {label} — signed & sent. https://solscan.io/tx/{sig2}")
            return sig2
        except Exception as e:
            print(f"[BUY ERR] send failed for {mint}: {e}")
            return None

    print(f"[BUY ERR] unexpected PumpPortal response for {mint}: {str(resp)[:300]}")
    return None
