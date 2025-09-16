# utils_pkg/pump_trader.py
from __future__ import annotations

import os
from typing import Dict, Any, Optional

import requests

from utils_pkg.solana_wallet import (
    load_keypair,
    get_pubkey_str,
    sign_and_send_b64_tx,  # async
)

# ──────────────────────────────────────────────────────────────────────────────
# Env / config
# ──────────────────────────────────────────────────────────────────────────────

def _is_true(v: Optional[str]) -> bool:
    return str(v or "").strip().lower() in {"1", "true", "yes", "y", "on"}

PP_API_KEY       = (os.getenv("PF_PP_KEY") or "").strip()
DRY_RUN          = _is_true(os.getenv("DRY_RUN", "0"))
PF_SLIPPAGE_BPS  = int(os.getenv("PF_SLIPPAGE_BPS", "1000"))       # e.g., 800 = 8.00%
PF_PRIORITY_SOL  = float(os.getenv("PF_PRIORITY_SOL", "0.0005"))
PF_POOLS         = {p.strip().lower() for p in (os.getenv("PF_POOLS", "pump").split(","))}
USE_LOCAL_FALLBK = _is_true(os.getenv("PF_USE_LOCAL_FALLBACK", "0"))  # off by default

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

class TemporaryPumpPortal(RuntimeError):
    """Non-fatal / transient response (retry later)."""

def _slippage_percent_from_bps(bps: int) -> float:
    return float(bps) / 100.0

def _ensure_api_key() -> None:
    if not PP_API_KEY:
        raise RuntimeError("PF_PP_KEY is not set (trading requires a PumpPortal API key).")

def _lightning_url() -> str:
    _ensure_api_key()
    return f"https://pumpportal.fun/api/trade?api-key={PP_API_KEY}"

def _local_url() -> str:
    _ensure_api_key()
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
        "action": action,                                # "buy" or "sell"
        "mint": mint,
        "amount": float(amount_sol),
        "denominatedInSol": True,                        # amount is in SOL
        "slippage": _slippage_percent_from_bps(slippage_bps),  # PERCENT value
        "priorityFee": float(priority_sol),
        "pool": pool,
        "publicKey": payer_pubkey,
    }

def _post_expect_json(url: str, body: Dict[str, Any], timeout: int = 20) -> Dict[str, Any]:
    """
    POST and return JSON. If response is 200 but not JSON (e.g., application/octet-stream),
    raise TemporaryPumpPortal so the caller can treat it as a transient and back off.
    """
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "x-api-key": PP_API_KEY,  # redundant with query param; harmless and sometimes required by gateways
    }
    r = requests.post(url, headers=headers, json=body, timeout=timeout, allow_redirects=False)

    ctype = (r.headers.get("content-type") or "").split(";")[0].strip().lower()

    # Some endpoints reply 200 with binary (octet-stream) or plain text; treat as transient/hard error.
    if r.status_code == 200 and ctype != "application/json":
        head = (r.text or "").splitlines()[0][:200]
        if ctype == "text/plain":
            # Common from local builder when it can't parse: "Bad Request"
            raise RuntimeError(f"{url} returned text/plain 200: {head or 'Bad Request'}")
        # Lightning may rarely respond non-JSON during load; back off this token quietly.
        raise TemporaryPumpPortal(f"non-JSON 200 ({ctype})")

    if r.status_code != 200:
        # Surface JSON error if any, else status text/body
        try:
            js = r.json()
        except Exception:
            js = {"error": (r.text or "").strip()[:240] or r.reason}
        raise RuntimeError(f"{url} {r.status_code}: {js}")

    try:
        return r.json()
    except Exception as e:
        # Unexpected parse failure despite JSON content-type — don't crash the loop
        raise TemporaryPumpPortal(f"json parse error: {e}")

def _call_lightning(body: Dict[str, Any]) -> Dict[str, Any]:
    return _post_expect_json(_lightning_url(), body)

def _call_local(body: Dict[str, Any]) -> Dict[str, Any]:
    # Local builder often replies 400 text/plain "Bad Request" when it can't build.
    return _post_expect_json(_local_url(), body)

# ──────────────────────────────────────────────────────────────────────────────
# Public: BUY helper
# ──────────────────────────────────────────────────────────────────────────────

async def maybe_buy(mint: str, **kwargs) -> None:
    """
    Execute a buy via PumpPortal Lightning. Only trades tokens whose `pool` is allowed
    by PF_POOLS (default: only 'pump'). We treat odd non-JSON Lightning replies as
    transient and quietly skip the attempt to avoid log spam.
    """
    ui = kwargs.get("ui") or {}
    symbol = (kwargs.get("symbol") or ui.get("symbol") or "").strip()
    pool   = (kwargs.get("pool")   or ui.get("pool")   or "pump").strip().lower()

    if PF_POOLS and pool not in PF_POOLS:
        print(f"[skip] unsupported pool '{pool}' for {mint}; allowed={sorted(PF_POOLS)}; skipping.")
        return

    label   = f"{symbol} ({mint})" if symbol else mint
    amt_sol = current_trade_sol()

    if DRY_RUN:
        print(f"[DRYRUN] would buy {label} on {pool} for {amt_sol} SOL")
        return

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

    # 1) Lightning (primary path)
    try:
        resp = _call_lightning(body)
    except TemporaryPumpPortal as te:
        # Known transient (e.g., 200 with non-JSON octet-stream). Just back off this token.
        print(f"[BUY TEMP] lightning quirk for {mint}: {te}")
        return
    except Exception as e:
        # Hard failure from Lightning. Optionally try local, otherwise bail.
        if USE_LOCAL_FALLBK:
            try:
                lresp = _call_local(body)
            except TemporaryPumpPortal as te2:
                print(f"[BUY TEMP] local quirk for {mint}: {te2}")
                return
            except Exception as e2:
                print(f"[BUY ERR] local builder failed for {mint}: {e2}")
                return
            else:
                await _handle_trade_response(label, lresp, kp, mint)
                return
        else:
            print(f"[BUY ERR] lightning call failed for {mint}: {e}")
            return

    # 2) Handle Lightning JSON result
    await _handle_trade_response(label, resp, kp, mint)

# ──────────────────────────────────────────────────────────────────────────────
# Response handling
# ──────────────────────────────────────────────────────────────────────────────

async def _handle_trade_response(label: str, resp: Dict[str, Any], kp, mint: str) -> None:
    """
    Normalize PumpPortal responses:
      - If 'errors' array has items -> print and exit.
      - If 'signature' -> success (executed server-side).
      - If 'transaction' (base64) -> sign & send locally.
    """
    errs = resp.get("errors") or []
    if isinstance(errs, list) and errs:
        # Common message: "Failed to get bonding curve ..." (non-pump pools or very new tokens)
        print(f"[BUY ERR] pumpportal errors for {mint}: {errs}")
        return

    sig = resp.get("signature")
    if sig:
        print(f"[BUY OK] {label} — executed via Lightning. https://solscan.io/tx/{sig}")
        return

    tx_b64 = resp.get("transaction") or resp.get("tx")
    if tx_b64:
        # Rare path: sign & send locally (legacy tx expected; our wallet code handles it)
        try:
            sig2 = await sign_and_send_b64_tx(tx_b64, kp, skip_preflight=True)
            print(f"[BUY OK] {label} — signed & sent. https://solscan.io/tx/{sig2}")
        except Exception as e:
            print(f"[BUY ERR] local send failed for {mint}: {e}")
        return

    # No signature nor transaction -> strange but not fatal
    # Keep the body concise in logs.
    try:
        short = str({k: resp[k] for k in list(resp)[:5]})[:300]
    except Exception:
        short = str(resp)[:300]
    print(f"[BUY ERR] unexpected PumpPortal response for {mint}: {short}")

# ──────────────────────────────────────────────────────────────────────────────
# Optional: SELL via Lightning (useful if Lightning wallet != bot keypair)
# ──────────────────────────────────────────────────────────────────────────────

async def sell_via_lightning(mint: str, pct_of_wallet: float = 1.0, *, pool: str = "pump") -> Optional[str]:
    """
    Sell a percentage of your position via Lightning (tokens must be in the wallet
    linked to PF_PP_KEY). Returns signature if executed.
    """
    if PF_POOLS and pool not in PF_POOLS:
        print(f"[skip] unsupported pool '{pool}' for {mint}; allowed={sorted(PF_POOLS)}; skipping.")
        return None

    if DRY_RUN:
        print(f"[DRYRUN] would SELL {mint} {pct_of_wallet:.0%} via Lightning on {pool}")
        return None

    pct = max(0.0, min(1.0, pct_of_wallet))
    pct_str = f"{pct*100:.0f}%"

    kp = load_keypair()
    payer_pub = get_pubkey_str(kp)

    body = {
        "action": "sell",
        "mint": mint,
        "amount": pct_str,            # percent-of-position string
        "denominatedInSol": False,    # selling tokens (not specifying in SOL)
        "slippage": _slippage_percent_from_bps(PF_SLIPPAGE_BPS),
        "priorityFee": PF_PRIORITY_SOL,
        "pool": pool,
        "publicKey": payer_pub,
    }

    try:
        resp = _call_lightning(body)
    except TemporaryPumpPortal as te:
        print(f"[SELL TEMP] lightning quirk for {mint}: {te}")
        return None
    except Exception as e:
        print(f"[SELL ERR] lightning call failed for {mint}: {e}")
        return None

    errs = resp.get("errors") or []
    if isinstance(errs, list) and errs:
        print(f"[SELL ERR] pumpportal errors for {mint}: {errs}")
        return None

    sig = resp.get("signature")
    if sig:
        print(f"[SELL OK] {mint} — via Lightning. https://solscan.io/tx/{sig}")
        return sig

    print(f"[SELL WARN] unexpected Lightning response for {mint}: {str(resp)[:300]}")
    return None
