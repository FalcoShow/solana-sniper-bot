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
PF_SLIPPAGE_BPS  = int(os.getenv("PF_SLIPPAGE_BPS", "1000"))     # 1000 bps = 10%
PF_PRIORITY_SOL  = float(os.getenv("PF_PRIORITY_SOL", "0.0005"))
PF_POOLS         = {p.strip().lower() for p in (os.getenv("PF_POOLS", "pump").split(","))}
USE_LOCAL_FALLBK = _is_true(os.getenv("PF_USE_LOCAL_FALLBACK", "0"))
PF_DEBUG_TX      = _is_true(os.getenv("PF_DEBUG_TX", "0"))

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
# Base58 helpers & mint/bondingCurve splitter
# ──────────────────────────────────────────────────────────────────────────────
_BASE58 = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")

def _looks_base58(s: str) -> bool:
    return bool(s) and isinstance(s, str) and all(ch in _BASE58 for ch in s)

def _split_mint_or_curve(raw: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (mint, bonding_curve). Exactly one will be set when possible.

    PumpPortal trade feed often surfaces bonding-curve ids as '<base58>pump'.
    For those, you must call the trade API with {"bondingCurve": "<base58>"} and
    OMIT the 'mint' field. Sending the curve as 'mint' triggers errors like
    'mint must be a string' / 'invalid public key input'.
    """
    s = (raw or "").strip()
    if not s:
        return None, None

    # bonding curve path: "<curve>pump"
    if s.endswith("pump"):
        curve = s[:-4].strip()
        if _looks_base58(curve):
            return None, curve

    # plain token mint path
    if _looks_base58(s):
        return s, None

    # unknown / malformed
    return None, None

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
    pool: str,
    payer_pubkey: str,
    amount_sol: float,
    slippage_bps: int,
    priority_sol: float,
    mint: Optional[str] = None,
    bonding_curve: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build request body for Lightning (and local) routes.
    Only include 'mint' if we really have a token mint.
    Only include 'bondingCurve' if we're trading a pump bonding curve id.
    """
    body: Dict[str, Any] = {
        "action": action,                   # "buy" or "sell"
        "amount": float(amount_sol),
        "denominatedInSol": True,          # amount is in SOL
        "slippage": _slippage_percent_from_bps(slippage_bps),
        "priorityFee": float(priority_sol),
        "pool": pool,
        "publicKey": payer_pubkey,
    }
    if bonding_curve:
        body["bondingCurve"] = bonding_curve
    if mint:
        body["mint"] = mint
    return body

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

    # Decide between {mint} vs {bondingCurve}
    token_mint, bonding_curve = _split_mint_or_curve(mint)
    if PF_DEBUG_TX:
        if bonding_curve and not token_mint:
            print(f"[trade] using bondingCurve={bonding_curve} (pump) for {label}")
        elif token_mint:
            print(f"[trade] using mint={token_mint} for {label}")
        else:
            print(f"[trade] cannot parse mint '{mint}' as token or bonding curve; aborting.")
            return None

    kp = load_keypair()
    payer_pub = get_pubkey_str(kp)

    body = _build_body(
        action="buy",
        pool=pool,
        payer_pubkey=payer_pub,
        amount_sol=amt_sol,
        slippage_bps=PF_SLIPPAGE_BPS,
        priority_sol=PF_PRIORITY_SOL,
        mint=token_mint,
        bonding_curve=bonding_curve,
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
        print(f"[BUY ERR] pumpportal errors for {token_mint or bonding_curve or mint}: {errs}")
        return None

    sig = resp.get("signature")
    if sig:
        print(f"[BUY OK] {label} — executed via Lightning. https://solscan.io/tx/{sig} [payer={payer_pub}]")
        return sig

    # Local path: sign & send base64 tx if provided
    tx_b64 = resp.get("transaction") or resp.get("tx")
    if tx_b64:
        try:
            sig2 = await sign_and_send_b64_tx(tx_b64, kp, skip_preflight=True)
            print(f"[BUY OK] {label} — local sign. https://solscan.io/tx/{sig2} [payer={payer_pub}]")
            return sig2
        except Exception as e:
            print(f"[BUY ERR] signing local tx failed for {mint}: {e}")
            return None

    print(f"[BUY ERR] unexpected PumpPortal response for {mint}: {json.dumps(resp)[:300]}")
    return None
