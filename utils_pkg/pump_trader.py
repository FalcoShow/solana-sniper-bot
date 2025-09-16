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
PF_PP_DOUBLETRY  = _is_true(os.getenv("PF_PP_DOUBLETRY", "1"))    # try other param once on schema error

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
    If the feed gave '<base58>pump', that's a bonding-curve id; otherwise try mint.
    """
    s = (raw or "").strip()
    if not s:
        return None, None
    if s.endswith("pump"):
        curve = s[:-4].strip()
        if _looks_base58(curve):
            return None, curve
    if _looks_base58(s):
        return s, None
    return None, None

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
    if bonding_curve:
        body["bondingCurve"] = bonding_curve
    if mint:
        body["mint"] = mint
    return body

def _post_expect_json(url: str, body: Dict[str, Any], timeout: int = 20) -> Dict[str, Any]:
    r = requests.post(url, json=body, timeout=timeout, allow_redirects=False)
    ctype = (r.headers.get("content-type") or "").split(";")[0].strip().lower()

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

def _resp_errors(resp: Dict[str, Any]) -> list[str]:
    errs = resp.get("errors") or []
    if isinstance(errs, dict):
        errs = [str(errs)]
    return [str(e) for e in errs] if isinstance(errs, list) else [str(errs)]

def _is_schema_err(errs: list[str]) -> bool:
    text = " | ".join(e.lower() for e in errs)
    return any(k in text for k in [
        "invalid public key input",
        "mint must be a string",
        "wrongsize",
        "invalid param",
    ])

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
            print(f"[trade] cannot parse mint '{mint}' as token or bonding curve; will try both if enabled.")

    kp = load_keypair()
    payer_pub = get_pubkey_str(kp)

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
        try:
            resp = _call_lightning(body)
        except TemporaryPumpPortal as te:
            print(f"[BUY TEMP] lightning quirk for {mint} ({note}): {te}")
            return None
        except Exception as e:
            if USE_LOCAL_FALLBK:
                try:
                    resp = _call_local(body)
                except TemporaryPumpPortal as te2:
                    print(f"[BUY TEMP] local quirk for {mint} ({note}): {te2}")
                    return None
                except Exception as e2:
                    print(f"[BUY ERR] local builder failed for {mint} ({note}): {e2}")
                    return None
            else:
                print(f"[BUY ERR] lightning call failed for {mint} ({note}): {e}")
                return None

        errs = _resp_errors(resp)
        if errs:
            print(f"[BUY ERR] pumpportal errors for {one_mint or one_curve or mint} ({note}): {errs}")
            return None

        sig = resp.get("signature")
        if sig:
            print(f"[BUY OK] {label} — executed via Lightning. https://solscan.io/tx/{sig} [payer={payer_pub}]")
            return sig

        tx_b64 = resp.get("transaction") or resp.get("tx")
        if tx_b64:
            try:
                sig2 = await sign_and_send_b64_tx(tx_b64, kp, skip_preflight=True)
                print(f"[BUY OK] {label} — local sign. https://solscan.io/tx/{sig2} [payer={payer_pub}]")
                return sig2
            except Exception as e:
                print(f"[BUY ERR] signing local tx failed for {mint} ({note}): {e}")
                return None

        print(f"[BUY ERR] unexpected PumpPortal response for {mint} ({note}): {json.dumps(resp)[:300]}")
        return None

    # First attempt: what we believe is correct
    first_sig = await _attempt(token_mint, bonding_curve, "first")
    if first_sig:
        return first_sig

    # If the first attempt failed with schema-ish errors, flip and retry once
    if PF_PP_DOUBLETRY:
        # flip params only if pool is pump (other pools don't use bondingCurve)
        if pool == "pump":
            # If we started with mint, try bondingCurve=raw (or stripped); else try mint=raw.
            flip_mint, flip_curve = (None, mint) if token_mint else (mint, None)
            if PF_DEBUG_TX:
                print(f"[BUY RETRY] flipping to {'bondingCurve' if flip_curve else 'mint'}={mint}")
            second_sig = await _attempt(flip_mint, flip_curve, "retry-flip")
            if second_sig:
                return second_sig

    return None
