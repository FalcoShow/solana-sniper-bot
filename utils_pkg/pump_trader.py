# utils_pkg/pump_trader.py
from __future__ import annotations

import os
import json
from typing import Dict, Any

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

# Common headers
def _json_headers() -> Dict[str, str]:
    if not PP_API_KEY:
        raise RuntimeError("PF_PP_KEY is not set (trading requires a PumpPortal API key).")
    return {
        "accept": "application/json",
        "content-type": "application/json",
        "x-api-key": PP_API_KEY,  # header is accepted even when api-key is also in the query
    }

# -----------------------------------------------------------------------------
# PumpPortal — Lightning first, Local fallback
# -----------------------------------------------------------------------------
def _slippage_percent_from_bps(bps: int) -> float:
    return float(bps) / 100.0

def call_lightning_trade(
    *,
    action: str,
    mint: str,
    payer_pubkey: str,
    amount_sol: float,
    slippage_bps: int,
    priority_sol: float,
    pool: str = "pump",
    timeout: int = 15,
) -> Dict[str, Any]:
    """
    POST https://pumpportal.fun/api/trade?api-key=...
    Usually returns JSON: {"signature": "...", "errors": []}
    Some edge paths mislabel or stream non-JSON — we detect that at the caller and fall back.
    """
    url = f"https://pumpportal.fun/api/trade?api-key={PP_API_KEY}"
    body = {
        "action": action,                 # "buy" or "sell"
        "mint": mint,
        "amount": float(amount_sol),
        "denominatedInSol": True,
        "slippage": _slippage_percent_from_bps(slippage_bps),   # percent, not bps
        "priorityFee": float(priority_sol),
        "pool": pool,
        "publicKey": payer_pubkey,
    }

    r = requests.post(url, headers=_json_headers(), json=body, timeout=timeout, allow_redirects=False)

    # Some servers return 200 with binary or wrong content-type. We try to parse anyway.
    ctype = (r.headers.get("content-type") or "").lower()
    text = None
    try:
        return r.json()
    except Exception:
        # Not JSON. Capture a small head for logs and let caller decide to fallback.
        try:
            text = r.text  # safe if it's actually text
        except Exception:
            text = ""
        return {
            "_non_json": True,
            "status": r.status_code,
            "content_type": ctype,
            "head": (text or "")[:160],
        }

def call_local_trade_build(
    *,
    action: str,
    mint: str,
    payer_pubkey: str,
    amount_sol: float,
    slippage_bps: int,
    priority_sol: float,
    pool: str = "pump",
    timeout: int = 15,
) -> Dict[str, Any]:
    """
    POST https://pumpportal.fun/api/trade-local?api-key=...
    Returns JSON with {"transaction": "<base64>"} (or "tx").
    We sign & send that transaction ourselves.
    """
    url = f"https://pumpportal.fun/api/trade-local?api-key={PP_API_KEY}"
    body = {
        "action": action,
        "mint": mint,
        "amount": float(amount_sol),
        "denominatedInSol": True,
        "slippage": _slippage_percent_from_bps(slippage_bps),
        "priorityFee": float(priority_sol),
        "pool": pool,
        "publicKey": payer_pubkey,
    }
    r = requests.post(url, headers=_json_headers(), json=body, timeout=timeout, allow_redirects=False)
    # trade-local tends to be strict about bad inputs and may return 400 text/plain with "Bad Request".
    # Try best-effort parse, but always surface status & snippet.
    try:
        js = r.json()
    except Exception:
        js = {
            "_non_json": True,
            "status": r.status_code,
            "content_type": (r.headers.get("content-type") or "").lower(),
            "head": (r.text or "")[:160],
        }
    # Normalize
    if isinstance(js, dict) and ("tx" in js) and ("transaction" not in js):
        js["transaction"] = js["tx"]
    js.setdefault("_http_ok", r.ok)
    return js

# -----------------------------------------------------------------------------
# Public: BUY (Lightning with Local fallback)
# -----------------------------------------------------------------------------
async def maybe_buy(mint: str, **kwargs) -> None:
    """
    Execute a buy. Flow:
      1) Try Lightning (/api/trade) which executes on server and should return a signature.
      2) If Lightning yields errors or a non-JSON body, fall back to Local (/api/trade-local),
         sign & send the returned base64 transaction with our wallet.
    """
    symbol = (kwargs.get("symbol") or (kwargs.get("ui") or {}).get("symbol") or "").strip()
    label = f"{symbol} ({mint})" if symbol else mint

    # Optional guard if caller passes a pool we don't want to handle here
    pool = (kwargs.get("pool") or "pump").strip().lower()
    if pool not in {"pump", "raydium", "pump-amm", "launchlab", "raydium-cpmm", "bonk", "auto"}:
        print(f"[skip] unsupported pool '{pool}' for {mint}; skipping.")
        return

    amt_sol = current_trade_sol()
    if DRY_RUN:
        print(f"[DRYRUN] would buy {label} for {amt_sol} SOL (pool={pool})")
        return

    kp = load_keypair()
    payer_pub = get_pubkey_str(kp)

    # ---- 1) Lightning
    try:
        resp = call_lightning_trade(
            action="buy",
            mint=mint,
            payer_pubkey=payer_pub,
            amount_sol=amt_sol,
            slippage_bps=PF_SLIPPAGE_BPS,
            priority_sol=PF_PRIORITY_SOL,
            pool=pool,
        )
    except Exception as e:
        print(f"[BUY ERR] request failed for {mint}: {e}")
        resp = {"_exception": str(e)}

    # If it’s clean JSON, check for errors/sig
    if isinstance(resp, dict) and not resp.get("_non_json"):
        errs = resp.get("errors") or []
        if errs:
            # Lightning ran but rejected the trade (e.g., no bonding curve yet).
            print(f"[BUY ERR] lightning returned errors for {mint}: {errs}")
        else:
            sig = resp.get("signature")
            if sig:
                print(f"[BUY OK] {label} — executed via Lightning. https://solscan.io/tx/{sig}")
                return
            # Some paths return a b64 tx; sign/send it.
            tx_b64 = resp.get("transaction") or resp.get("tx")
            if tx_b64:
                try:
                    sig2 = await sign_and_send_b64_tx(tx_b64, kp, skip_preflight=True)
                    print(f"[BUY OK] {label} — signed & sent. https://solscan.io/tx/{sig2}")
                    return
                except Exception as e:
                    print(f"[BUY ERR] send failed for {mint}: {e}")
            else:
                # Unexpected clean JSON shape
                print(f"[BUY ERR] unexpected Lightning response for {mint}: {json.dumps(resp)[:400]}")
        # If we reached here, we’ll attempt Local fallback
    else:
        # Non-JSON 200 or exception
        if isinstance(resp, dict) and resp.get("_non_json"):
            ct = resp.get("content_type", "?")
            head = (resp.get("head") or "").replace("\n", " ")
            print(f"[BUY WARN] Lightning returned non-JSON ({ct}) for {mint}; head: {head[:120]}")
        elif isinstance(resp, dict) and resp.get("_exception"):
            print(f"[BUY WARN] Lightning exception for {mint}: {resp['_exception']}")

    # ---- 2) Local fallback (build + sign/send)
    try:
        jr = call_local_trade_build(
            action="buy",
            mint=mint,
            payer_pubkey=payer_pub,
            amount_sol=amt_sol,
            slippage_bps=PF_SLIPPAGE_BPS,
            priority_sol=PF_PRIORITY_SOL,
            pool=pool,
        )
    except Exception as e:
        print(f"[BUY ERR] local builder failed for {mint}: {e}")
        return

    if jr.get("_non_json"):
        print(f"[BUY ERR] local builder non-JSON ({jr.get('content_type','?')}) for {mint}; head: {(jr.get('head') or '')[:120]}")
        return

    if not jr.get("_http_ok", True):
        print(f"[BUY ERR] local builder HTTP {jr.get('status')} for {mint}: {(jr.get('head') or '')[:120]}")
        return

    errs2 = jr.get("errors") or []
    if errs2:
        print(f"[BUY ERR] local builder errors for {mint}: {errs2}")
        return

    tx_b64 = jr.get("transaction") or jr.get("tx")
    if not tx_b64:
        # Some rare responses jam a 'signature' even from local; treat that as success.
        sig_local = jr.get("signature")
        if sig_local:
            print(f"[BUY OK] {label} — (local) already executed. https://solscan.io/tx/{sig_local}")
            return
        print(f"[BUY ERR] local builder missing transaction for {mint}: {json.dumps(jr)[:400]}")
        return

    try:
        sig3 = await sign_and_send_b64_tx(tx_b64, kp, skip_preflight=True)
        print(f"[BUY OK] {label} — (local) signed & sent. https://solscan.io/tx/{sig3}")
    except Exception as e:
        print(f"[BUY ERR] local send failed for {mint}: {e}")
