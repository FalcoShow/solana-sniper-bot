# utils_pkg/pump_trader.py
from __future__ import annotations

import os, json, re
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

PP_API_KEY      = (os.getenv("PF_PP_KEY") or "").strip()
DRY_RUN         = _is_true(os.getenv("DRY_RUN", "1"))
PF_SLIPPAGE_BPS = int(os.getenv("PF_SLIPPAGE_BPS", "1000"))   # 1000 = 10.00%
PF_PRIORITY_SOL = float(os.getenv("PF_PRIORITY_SOL", "0.0005"))
PP_MODE         = (os.getenv("PF_PP_MODE") or "local").strip().lower()  # "local" | "lightning"

def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def current_trade_sol() -> float:
    """Read PF_TRADE_SOL (autosized elsewhere) and clamp to min/max."""
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
# Helpers
# -----------------------------------------------------------------------------
_B64_RE = re.compile(r"^[A-Za-z0-9+/=\s]+$")

def _looks_like_b64(s: str) -> bool:
    s = s.strip()
    return len(s) > 100 and _B64_RE.match(s) is not None and "<html" not in s.lower()

_HEADERS = lambda: {
    "accept": "application/json",
    "content-type": "application/json",
    "x-api-key": PP_API_KEY,
}

def _post_json_with_fallback(url: str, body: Dict[str, Any], timeout: int = 20) -> Dict[str, Any]:
    """
    Post JSON, try JSON parse; if that fails:
      - if body is base64 => treat as {"transaction": "<b64>"}
      - else raise with rich context
    """
    if not PP_API_KEY:
        raise RuntimeError("PF_PP_KEY is not set (trading requires a PumpPortal API key).")

    r = requests.post(url, headers=_HEADERS(), data=json.dumps(body), timeout=timeout, allow_redirects=False)
    ct = r.headers.get("content-type", "")
    txt = None
    try:
        return r.json()
    except Exception:
        txt = (r.text or "")
        if _looks_like_b64(txt):
            return {"transaction": txt.strip(), "errors": []}
        head = txt.strip().replace("\n", " ")[:180]
        if not r.ok:
            raise RuntimeError(f"PumpPortal {r.status_code} ({ct}): {head}")
        # 200 but not JSON/base64 -> still error (often HTML/cloudflare)
        raise RuntimeError(f"Non-JSON 200 from PumpPortal ({ct}): {head}")

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
    Returns {"transaction":"<b64>", "errors":[...]} (or {"errors":[...]}).
    NOTE: slippage is PERCENT here, so bps -> %.
    """
    url = f"https://pumpportal.fun/api/trade-local?api-key={PP_API_KEY}"
    body = {
        "action": action,
        "mint": mint,
        "amount": float(amount_sol),
        "denominatedInSol": True,
        "slippage": float(slippage_bps) / 100.0,
        "priorityFee": float(priority_sol),
        "pool": pool,
        "publicKey": payer_pubkey,  # build tx for OUR wallet
        # "skipPreflight": False, "jitoOnly": False,  # optional toggles
    }
    return _post_json_with_fallback(url, body, timeout=timeout)

# -----------------------------------------------------------------------------
# PumpPortal: Lightning API (optional)
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
    Executes server-side using wallet linked to the API key.
    """
    url = f"https://pumpportal.fun/api/trade?api-key={PP_API_KEY}"
    body = {
        "action": action,
        "mint": mint,
        "amount": float(amount_sol),
        "denominatedInSol": True,
        "slippage": float(slippage_bps) / 100.0,
        "priorityFee": float(priority_sol),
        "pool": pool,
    }
    return _post_json_with_fallback(url, body, timeout=timeout)

# -----------------------------------------------------------------------------
# Buy helper
# -----------------------------------------------------------------------------
async def maybe_buy(mint: str, **kwargs) -> None:
    """
    Default: Local API -> tx for our wallet -> we sign & send.
    Optional: set PF_PP_MODE=lightning to let server execute.
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

    errs = resp.get("errors") or []
    if isinstance(errs, list) and errs:
        print(f"[BUY ERR] API returned errors for {mint}: {errs}")
        return

    sig = resp.get("signature")
    if sig:
        print(f"[BUY OK] {label} — executed via Lightning. https://solscan.io/tx/{sig}")
        return

    tx_b64 = resp.get("transaction")
    if tx_b64:
        try:
            sig2 = await sign_and_send_b64_tx(tx_b64, kp, skip_preflight=True)
            print(f"[BUY OK] {label} — signed & sent. https://solscan.io/tx/{sig2}")
        except Exception as e:
            print(f"[BUY ERR] send failed for {mint}: {e}")
        return

    print(f"[BUY ERR] unexpected response for {mint}: {json.dumps(resp)[:400]}")
