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
PF_PP_DOUBLETRY  = _is_true(os.getenv("PF_PP_DOUBLETRY", "1"))    # try one flip if needed
USE_LOCAL_FALLBK = _is_true(os.getenv("PF_USE_LOCAL_FALLBACK", "1"))  # default ON

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

def _split_id(raw: str) -> Tuple[Optional[str], Optional[str], bool]:
    """
    Given a raw string, return (mint, bonding_curve, is_pump_id).
    - If raw endswith 'pump' and the base is base58 => it's a Pump bonding curve id.
    - If raw is base58 and not ending with 'pump' => it's a mint.
    """
    s = (raw or "").strip()
    if not s:
        return None, None, False

    if s.endswith("pump"):
        base = s[:-4].strip()
        return (None, base if _looks_base58(base) else None, True)

    return (s if _looks_base58(s) else None, None, False)

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
    mint: Optional[str] = None,
    bonding_curve: Optional[str] = None,
) -> Dict[str, Any]:
    body: Dict[str, Any] = {
        "action": action,
        "amount": float(amount_sol),
        "denominatedInSol": True,
        "slippage": _slippage_percent_from_bps(slippage_bps),
        "priorityFee": float(priority_sol),
        "pool": pool,
        "publicKey": payer_pubkey,
    }
    # STRICTLY send only one of these
    if bonding_curve and not mint:
        body["bondingCurve"] = bonding_curve
    elif mint and not bonding_curve:
        body["mint"] = mint
    return body

class TemporaryPumpPortal(Exception):
    pass

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

def _is_schemaish(errs: list[str]) -> bool:
    t = " | ".join(e.lower() for e in errs)
    return any(k in t for k in ["invalid public key input", "mint must be a string", "invalid param", "wrongsize"])

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

    label   = f"{symbol} ({mint})" if symbol else mint
    amt_sol = current_trade_sol()

    if DRY_RUN:
        print(f"[DRYRUN] would buy {label} on {pool} for {amt_sol} SOL")
        return None

    # Decide strictly which field to send
    raw_hint: str = (ui.get("rawMint") or ui.get("raw") or mint or "").strip()
    tok_mint, curve_id, is_pump_id = _split_id(raw_hint)

    # If caller passed a plain base58 mint, respect that
    if not is_pump_id and not tok_mint and _looks_base58(mint):
        tok_mint = mint

    # For pump ids: favor bondingCurve only
    if is_pump_id and curve_id:
        tok_mint = None

    if PF_DEBUG_TX:
        if is_pump_id:
            print(f"[trade] pool={pool} symbol={symbol} "
                  f"mint=- curve={curve_id} (from raw='{raw_hint}') for {label}")
        else:
            print(f"[trade] pool={pool} symbol={symbol} "
                  f"mint={tok_mint or '-'} curve=- (from raw='{raw_hint}') for {label}")

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

    async def _attempt(one_mint: Optional[str], one_curve: Optional[str], note: str) -> Optional[str]:
        body = _build_body(
            action="buy",
            pool=pool,
            payer_pubkey=payer_pub,
            amount_sol=amt_sol,
            slippage_bps=PF_SLIPPAGE_BPS,
            priority_sol=PF_PRIORITY_SOL,
            mint=one_mint,
            bonding_curve=one_curve,
        )
        # Lightning
        sig, errs, _ = _try(_url_lightning(), body, f"lightning/{note}")
        if sig:
            return sig
        # Local fallback when schema-ish issues pop (common when using bondingCurve)
        if errs and _is_schemaish(errs) and USE_LOCAL_FALLBK:
            sig2, errs2, _ = _try(_url_local(), body, f"local/{note}")
            if sig2:
                return sig2
            if errs2:
                print(f"[BUY ERR] pumpportal errors ({note}) for {one_mint or one_curve or mint}: {errs2}")
        elif errs:
            print(f"[BUY ERR] pumpportal errors ({note}) for {one_mint or one_curve or mint}: {errs}")
        return None

    # Strict order:
    # 1) If the raw looked like a pump id, use bondingCurve ONLY
    if pool == "pump" and curve_id and not tok_mint:
        sig0 = await _attempt(None, curve_id, "curve")
        if sig0:
            return sig0

    # 2) Otherwise, try with mint ONLY
    if tok_mint:
        sig1 = await _attempt(tok_mint, None, "mint")
        if sig1:
            return sig1

    # 3) Optionally flip once (mint <-> curve) if enabled
    if PF_PP_DOUBLETRY and pool == "pump":
        if tok_mint:
            sig2 = await _attempt(None, tok_mint, "flip-mint→curve")
            if sig2:
                return sig2
        elif curve_id:
            sig3 = await _attempt(curve_id, None, "flip-curve→mint")
            if sig3:
                return sig3

    return None
