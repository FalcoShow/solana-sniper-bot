# utils_pkg/pump_trader.py
from __future__ import annotations

import os
import json
from typing import Dict, Any, Optional, Tuple

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
PF_SLIPPAGE_BPS  = int(os.getenv("PF_SLIPPAGE_BPS", "1000"))     # 10%
PF_PRIORITY_SOL  = float(os.getenv("PF_PRIORITY_SOL", "0.0005"))
PF_POOLS         = {p.strip().lower() for p in (os.getenv("PF_POOLS", "pump").split(","))}
PF_DEBUG_TX      = _is_true(os.getenv("PF_DEBUG_TX", "0"))
# default OFF since you said you removed it from env
USE_LOCAL_FALLBK = _is_true(os.getenv("PF_USE_LOCAL_FALLBACK", "0"))

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

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
_BASE58 = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")

def _looks_base58(s: str) -> bool:
    return bool(s) and isinstance(s, str) and all(ch in _BASE58 for ch in s)

def _strip_pump_suffix(s: str) -> str:
    s = (s or "").strip()
    return s[:-4] if s.endswith("pump") else s

def _mint_for_pump(raw_id: str) -> Optional[str]:
    """
    Normalize to the exact value we will put in the `mint` field for Lightning:

    - If the ID ends with 'pump', strip it and use the remainder as mint.
    - Else, if it already looks base58, use it as-is.
    - Else, return None (we don't know what this is).
    """
    raw = (raw_id or "").strip()
    if not raw:
        return None
    if raw.endswith("pump"):
        base = _strip_pump_suffix(raw)
        return base if _looks_base58(base) else None
    return raw if _looks_base58(raw) else None

# ──────────────────────────────────────────────────────────────────────────────
# PumpPortal HTTP
# ──────────────────────────────────────────────────────────────────────────────
class TemporaryPumpPortal(Exception):
    pass

def _slippage_percent_from_bps(bps: int) -> float:
    return float(bps) / 100.0

def _url_lightning() -> str:
    if not PP_API_KEY:
        raise RuntimeError("PF_PP_KEY missing.")
    return f"https://pumpportal.fun/api/trade?api-key={PP_API_KEY}"

def _url_local() -> str:
    if not PP_API_KEY:
        raise RuntimeError("PF_PP_KEY missing.")
    return f"https://pumpportal.fun/api/trade-local?api-key={PP_API_KEY}"

def _build_body(
    *,
    action: str,
    pool: str,
    payer_pubkey: str,
    amount_sol: float,
    slippage_bps: int,
    priority_sol: float,
    mint: str,
) -> Dict[str, Any]:
    return {
        "action": action,
        "amount": float(amount_sol),
        "denominatedInSol": True,
        "slippage": _slippage_percent_from_bps(slippage_bps),
        "priorityFee": float(priority_sol),
        "pool": pool,
        "publicKey": payer_pubkey,
        "mint": mint,  # ← always present and always a base58 string
    }

def _post_json(url: str, body: Dict[str, Any], timeout: int = 20) -> Dict[str, Any]:
    r = requests.post(url, json=body, timeout=timeout, allow_redirects=False)
    content_type = (r.headers.get("content-type") or "").split(";")[0].strip().lower()
    if r.status_code == 200 and content_type != "application/json":
        head = (r.text or "")[:200].replace("\n", " ")
        raise TemporaryPumpPortal(f"non-JSON 200 ({content_type}): {head or 'Bad Request'}")
    try:
        js = r.json()
    except Exception as e:
        raise TemporaryPumpPortal(f"json parse error: {e}")
    if r.status_code != 200:
        raise RuntimeError(f"{url} {r.status_code}: {js}")
    return js

def _resp_errors(resp: Dict[str, Any]) -> list[str]:
    errs = resp.get("errors") or []
    if isinstance(errs, dict):
        errs = [str(errs)]
    return [str(e) for e in errs] if isinstance(errs, list) else ([str(errs)] if errs else [])

def _is_schema_err(errs: list[str]) -> bool:
    text = " | ".join(e.lower() for e in errs)
    return any(k in text for k in [
        "invalid public key input",
        "mint must be a string",
        "wrongsize",
        "invalid param",
    ])

# ──────────────────────────────────────────────────────────────────────────────
# BUY
# ──────────────────────────────────────────────────────────────────────────────
async def maybe_buy(mint: str, **kwargs) -> Optional[str]:
    ui = kwargs.get("ui") or {}
    symbol = (kwargs.get("symbol") or ui.get("symbol") or "").strip()
    pool   = (kwargs.get("pool")   or ui.get("pool")   or "pump").strip().lower()

    if pool not in PF_POOLS:
        print(f"[skip] unsupported pool '{pool}' for {mint}; allowed={sorted(PF_POOLS)}; skipping.")
        return None

    # Prefer the raw id from upstream if present (so we can strip 'pump' correctly).
    raw_hint = ui.get("rawMint") or ui.get("raw") or ui.get("raw_mint") or mint
    id_for_mint = _mint_for_pump(str(raw_hint))

    label   = f"{(symbol or '').strip()} ({raw_hint})".strip() or str(raw_hint)
    amt_sol = current_trade_sol()

    if not id_for_mint:
        print(f"[BUY ERR] could not derive base58 mint from '{raw_hint}' for {label}")
        return None

    if PF_DEBUG_TX:
        # show both raw id and normalized mint we will send
        print(f"[trade] pool={pool} symbol={(symbol or '?')} mint={id_for_mint} (from raw='{raw_hint}') for {label}")

    if DRY_RUN:
        print(f"[DRYRUN] would buy {label} on {pool} for {amt_sol} SOL")
        return None

    kp = load_keypair()
    payer_pub = get_pubkey_str(kp)

    def _try(url: str, body: Dict[str, Any], tag: str) -> Tuple[Optional[str], Optional[list[str]], Optional[Dict[str, Any]]]:
        try:
            resp = _post_json(url, body)
        except TemporaryPumpPortal as te:
            print(f"[BUY TEMP] {tag} for {label}: {te}")
            return None, None, None
        except Exception as e:
            print(f"[BUY ERR] {tag} HTTP for {label}: {e}")
            return None, None, None
        errs = _resp_errors(resp)
        if errs:
            return None, errs, resp
        # success: signature or unsigned tx
        sig = resp.get("signature")
        if sig:
            print(f"[BUY OK] {label} — executed via Lightning. https://solscan.io/tx/{sig} [payer={payer_pub}]")
            return sig, None, None
        tx_b64 = resp.get("transaction") or resp.get("tx")
        if tx_b64:
            try:
                sig2 = sign_and_send_b64_tx(tx_b64, kp, skip_preflight=True)
                print(f"[BUY OK] {label} — local sign. https://solscan.io/tx/{sig2} [payer={payer_pub}]")
                return sig2, None, None
            except Exception as e:
                print(f"[BUY ERR] local sign failed for {label}: {e}")
        else:
            print(f"[BUY ERR] unexpected response for {label}: {json.dumps(resp)[:300]}")
        return None, None, None

    body = _build_body(
        action="buy",
        pool=pool,
        payer_pubkey=payer_pub,
        amount_sol=amt_sol,
        slippage_bps=PF_SLIPPAGE_BPS,
        priority_sol=PF_PRIORITY_SOL,
        mint=id_for_mint,              # ← ALWAYS set
    )

    # 1) Lightning
    sig, errs, _ = _try(_url_lightning(), body, "lightning")
    if sig:
        return sig

    # 2) Optional local fallback on schema-ish failures
    if errs and _is_schema_err(errs) and USE_LOCAL_FALLBK:
        sig2, errs2, _ = _try(_url_local(), body, "local")
        if sig2:
            return sig2
        if errs2:
            print(f"[BUY ERR] pumpportal errors (local): {errs2}")
    elif errs:
        print(f"[BUY ERR] pumpportal errors: {errs}")

    return None
