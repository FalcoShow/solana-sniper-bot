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

# optional knob: force using bondingCurve when present in tick
FORCE_BONDING    = _is_true(os.getenv("PF_FORCE_BONDING_CURVE", "1"))

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

def _post_expect_json(url: str, body: Dict[str, Any], timeout: int = 20) -> Dict[str, Any]:
    """
    POST and return JSON. If response is 200 but not JSON (e.g., application/octet-stream),
    raise TemporaryPumpPortal so the caller can treat it as a transient and back off.
    """
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

# ──────────────────────────────────────────────────────────────────────────────
# Address helpers
# ──────────────────────────────────────────────────────────────────────────────
_BASE58 = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")

def _looks_like_mint(addr: str) -> bool:
    """Heuristic: 43–44 base58 chars → likely an SPL mint/pubkey."""
    s = addr.strip()
    if not s or any(c not in _BASE58 for c in s):
        return False
    return 43 <= len(s) <= 44

def _pick_trade_fields(mint_param: str, ui: Dict[str, Any]) -> Dict[str, Any]:
    """
    Decide whether to send PumpPortal {mint: ...} or {bondingCurve: ...}.
    Priority:
      1) If tick provides an explicit bonding curve, use it.
      2) Else, if the 'ui.mint' ends with 'pump' or clearly isn't a pubkey → bondingCurve.
      3) Else, use given mint_param as {mint: ...}.
    """
    bc = (
        ui.get("bondingCurve")
        or ui.get("bonding_curve")
        or ui.get("bondingCurveAddress")
        or ui.get("bondingCurvePubkey")
        or ui.get("bonding_address")
        or ui.get("bonding")
        or ""
    )
    raw_ui_mint = (ui.get("mint") or ui.get("mintAddress") or ui.get("mint_address") or "").strip()

    if bc:
        return {"bondingCurve": str(bc).strip()}

    # Some feeds stick the bonding curve into "mint" like "...pump"
    if FORCE_BONDING and raw_ui_mint.endswith("pump"):
        return {"bondingCurve": raw_ui_mint}

    # If the provided mint doesn’t look like a real pubkey, treat as bonding curve
    if not _looks_like_mint(mint_param):
        if raw_ui_mint.endswith("pump"):
            return {"bondingCurve": raw_ui_mint}
        # last-ditch: if *itself* ends with pump, use it directly
        if str(mint_param).endswith("pump"):
            return {"bondingCurve": mint_param}

    # Default: trade by mint
    return {"mint": mint_param.strip()}

def _build_body(
    *,
    action: str,
    payer_pubkey: str,
    amount_sol: float,
    pool: str,
    slippage_bps: int,
    priority_sol: float,
    trade_fields: Dict[str, str],
) -> Dict[str, Any]:
    # Lightning/local both accept this shape.
    body: Dict[str, Any] = {
        "action": action,                 # "buy" or "sell"
        "amount": float(amount_sol),
        "denominatedInSol": True,         # amount is in SOL
        "slippage": _slippage_percent_from_bps(slippage_bps),  # percent
        "priorityFee": float(priority_sol),
        "pool": pool,
        "publicKey": payer_pubkey,
    }
    body.update(trade_fields)  # either {mint: ...} or {bondingCurve: ...}
    return body

def _has_invalid_pubkey_error(resp: Dict[str, Any]) -> bool:
    errs = resp.get("errors") or []
    if not isinstance(errs, list):
        return False
    text = " ".join(str(e) for e in errs).lower()
    return "invalid public key" in text or "invalid pubkey" in text

# ──────────────────────────────────────────────────────────────────────────────
# Public: BUY helper (returns tx signature or None)
# ──────────────────────────────────────────────────────────────────────────────
async def maybe_buy(mint: str, **kwargs) -> Optional[str]:
    """
    Execute a buy via PumpPortal.
    Returns the transaction signature on success, else None.
    """
    ui = kwargs.get("ui") or {}
    symbol = (kwargs.get("symbol") or ui.get("symbol") or ui.get("name") or "").strip()
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

    trade_fields = _pick_trade_fields(mint, ui)

    def _do_trade(body: Dict[str, Any]) -> Dict[str, Any]:
        try:
            return _call_lightning(body)
        except TemporaryPumpPortal as te:
            print(f"[BUY TEMP] lightning quirk for {mint}: {te}")
            return {"errors": [str(te)]}
        except Exception as e:
            if USE_LOCAL_FALLBK:
                try:
                    return _call_local(body)
                except TemporaryPumpPortal as te2:
                    print(f"[BUY TEMP] local quirk for {mint}: {te2}")
                    return {"errors": [str(te2)]}
                except Exception as e2:
                    return {"errors": [f"local failed: {e2}"]}
            else:
                return {"errors": [f"lightning failed: {e}"]}

    body = _build_body(
        action="buy",
        payer_pubkey=payer_pub,
        amount_sol=amt_sol,
        pool=pool,
        slippage_bps=PF_SLIPPAGE_BPS,
        priority_sol=PF_PRIORITY_SOL,
        trade_fields=trade_fields,
    )
    resp = _do_trade(body)

    # If we tried {mint: ...} and got "Invalid public key", retry once with bondingCurve if we can.
    if _has_invalid_pubkey_error(resp) and "mint" in trade_fields:
        # pull possible bonding curve from the tick
        bc_fields = _pick_trade_fields("INVALID", ui)
        if "bondingCurve" in bc_fields:
            print(f"[buy-retry] switching to bondingCurve={bc_fields['bondingCurve']} for {label}")
            body = _build_body(
                action="buy",
                payer_pubkey=payer_pub,
                amount_sol=amt_sol,
                pool=pool,
                slippage_bps=PF_SLIPPAGE_BPS,
                priority_sol=PF_PRIORITY_SOL,
                trade_fields=bc_fields,
            )
            resp = _do_trade(body)

    errs = resp.get("errors") or []
    if isinstance(errs, list) and errs:
        print(f"[BUY ERR] pumpportal errors for {mint}: {errs}")
        return None

    sig = resp.get("signature")
    if sig:
        payer_show = payer_pub[:6] + "..." + payer_pub[-6:]
        which = "bondingCurve" if "bondingCurve" in trade_fields else "mint"
        val   = trade_fields.get(which)
        print(f"[BUY OK] {label} — executed via Lightning. https://solscan.io/tx/{sig} [payer={payer_pub}]")
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
