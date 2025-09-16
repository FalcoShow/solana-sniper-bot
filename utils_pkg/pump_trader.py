# utils_pkg/pump_trader.py
from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

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

# PumpPortal API key (required for trading)
PP_API_KEY       = (os.getenv("PF_PP_KEY") or "").strip()

# Trading toggles & knobs
DRY_RUN          = _is_true(os.getenv("DRY_RUN", "0"))
PF_SLIPPAGE_BPS  = int(os.getenv("PF_SLIPPAGE_BPS", "1000"))      # 1000 bps = 10%
PF_PRIORITY_SOL  = float(os.getenv("PF_PRIORITY_SOL", "0.0005"))  # priority fee in SOL
PF_POOLS         = {p.strip().lower() for p in (os.getenv("PF_POOLS", "pump").split(","))}
USE_LOCAL_FALLBK = _is_true(os.getenv("PF_USE_LOCAL_FALLBACK", "0"))

def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def current_trade_sol() -> float:
    """Read PF_TRADE_SOL (typically autosized elsewhere) and clamp to min/max."""
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
    # PumpPortal expects a *percent number*, not a fraction. e.g. 800 -> 8.0
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
    POST JSON and parse the reply.
    If 200 but not JSON (e.g., octet-stream) -> raise TemporaryPumpPortal so caller can retry or fallback.
    """
    r = requests.post(url, json=body, timeout=timeout, allow_redirects=False)
    ctype = (r.headers.get("content-type") or "").split(";")[0].strip().lower()

    if r.status_code == 200 and ctype != "application/json":
        head = (r.text or "")[:200].replace("\n", " ")
        if ctype == "text/plain":
            # PumpPortal sometimes returns text/plain 200: "Bad Request"
            raise RuntimeError(f"{url} returned text/plain 200: {head or 'Bad Request'}")
        raise TemporaryPumpPortal(f"non-JSON 200 from {url} ({ctype})")

    if r.status_code != 200:
        try:
            js = r.json()
        except Exception:
            js = {"error": (r.text or "").strip()[:240] or r.reason}
        raise RuntimeError(f"{url} {r.status_code}: {js}")

    try:
        return r.json()
    except Exception as e:
        raise TemporaryPumpPortal(f"json parse error from {url}: {e}")

def _call_lightning(body: Dict[str, Any]) -> Dict[str, Any]:
    return _post_expect_json(_lightning_url(), body)

def _call_local(body: Dict[str, Any]) -> Dict[str, Any]:
    return _post_expect_json(_local_url(), body)

def _build_body(
    *,
    action: str,               # "buy" or "sell"
    mint: str,                 # IMPORTANT: keep raw '...pump' id for PumpPortal
    payer_pubkey: str,
    amount_sol: float,
    pool: str,
    slippage_bps: int,
    priority_sol: float,
) -> Dict[str, Any]:
    return {
        "action": action,
        "mint": mint,
        "amount": float(amount_sol),
        "denominatedInSol": True,                            # amount is in SOL
        "slippage": _slippage_percent_from_bps(slippage_bps),# percent figure (e.g., 8.0)
        "priorityFee": float(priority_sol),                  # in SOL
        "pool": pool,
        "publicKey": payer_pubkey,
    }

# ──────────────────────────────────────────────────────────────────────────────
# Mint helpers
# ──────────────────────────────────────────────────────────────────────────────

def _raw_mint_for_trade(mint_param: str, ui: Dict[str, Any], pool: str) -> str:
    """
    PumpPortal trade endpoints expect the exact pool mint id.
    For 'pump' pool this usually ends with the literal suffix 'pump'.
    Strategy:
      - Prefer the UI's mint fields as-is.
      - Fall back to the provided `mint_param`.
      - If pool == 'pump' and we somehow have a bare base58 (no suffix), append 'pump'.
    """
    raw = (ui.get("mint") or ui.get("mintAddress") or ui.get("mint_address") or mint_param or "").strip()
    if pool == "pump" and raw and not raw.lower().endswith("pump"):
        # Some upstream paths may pass us the normalized base58. Restore the portal id.
        raw = f"{raw}pump"
    return raw

def _label_from_ui(mint: str, ui: Dict[str, Any]) -> str:
    name   = (ui.get("name")   or "").strip()
    symbol = (ui.get("symbol") or "").strip()
    if name and symbol:
        return f"{name} ({symbol})"
    return name or symbol or mint

# ──────────────────────────────────────────────────────────────────────────────
# Public: BUY helper (returns tx signature or None)
# ──────────────────────────────────────────────────────────────────────────────

async def maybe_buy(mint: str, **kwargs) -> Optional[str]:
    """
    Execute a buy via PumpPortal.
    Returns the transaction signature on success, else None.

    IMPORTANT:
      - Use the RAW mint with '...pump' for PumpPortal endpoints.
      - Do NOT strip the suffix here. (Exit/quotes may normalize separately.)
    """
    ui: Dict[str, Any] = kwargs.get("ui") or {}
    pool = (kwargs.get("pool") or ui.get("pool") or ui.get("poolType") or ui.get("amm") or "pump").strip().lower()

    if pool not in PF_POOLS:
        print(f"[skip] unsupported pool '{pool}' for {mint}; allowed={sorted(PF_POOLS)}; skipping.", flush=True)
        return None

    trade_mint = _raw_mint_for_trade(mint, ui, pool)
    label      = _label_from_ui(trade_mint, ui)
    amount_sol = current_trade_sol()

    if not trade_mint:
        print("[BUY ERR] no mint provided", flush=True)
        return None

    if DRY_RUN:
        print(f"[DRYRUN] would BUY {label} on {pool} for {amount_sol} SOL (mint={trade_mint})", flush=True)
        return None

    # Load wallet and build body
    kp = load_keypair()
    payer_pub = get_pubkey_str(kp)

    body = _build_body(
        action="buy",
        mint=trade_mint,          # keep ...pump suffix for PumpPortal
        payer_pubkey=payer_pub,
        amount_sol=amount_sol,
        pool=pool,
        slippage_bps=PF_SLIPPAGE_BPS,
        priority_sol=PF_PRIORITY_SOL,
    )

    # 1) Try Lightning route
    try:
        resp = _call_lightning(body)
    except TemporaryPumpPortal as te:
        print(f"[BUY TEMP] lightning quirk for {trade_mint}: {te}", flush=True)
        return None
    except Exception as e:
        if USE_LOCAL_FALLBK:
            # 2) Optional local builder fallback
            try:
                resp = _call_local(body)
            except TemporaryPumpPortal as te2:
                print(f"[BUY TEMP] local quirk for {trade_mint}: {te2}", flush=True)
                return None
            except Exception as e2:
                print(f"[BUY ERR] local builder failed for {trade_mint}: {e2}", flush=True)
                return None
        else:
            print(f"[BUY ERR] lightning call failed for {trade_mint}: {e}", flush=True)
            return None

    # 3) Handle response consistently
    errs = resp.get("errors")
    if isinstance(errs, list) and errs:
        print(f"[BUY ERR] pumpportal errors for {trade_mint}: {errs}", flush=True)
        return None
    if isinstance(errs, str) and errs.strip():
        print(f"[BUY ERR] pumpportal error for {trade_mint}: {errs.strip()}", flush=True)
        return None

    # Lightning path usually returns a signature directly
    sig = resp.get("signature") or resp.get("txSig") or resp.get("tx")
    if isinstance(sig, str) and len(sig) > 20:  # crude sanity check
        print(f"[BUY OK] {label} — executed via Lightning. https://solscan.io/tx/{sig}", flush=True)
        return sig

    # Local path generally returns a base64 transaction to sign
    tx_b64 = resp.get("transaction") or resp.get("txBase64") or resp.get("tx")
    if isinstance(tx_b64, str) and tx_b64:
        try:
            sig2 = await sign_and_send_b64_tx(tx_b64, kp, skip_preflight=True)
            print(f"[BUY OK] {label} — local sign. https://solscan.io/tx/{sig2}", flush=True)
            return sig2
        except Exception as e:
            print(f"[BUY ERR] signing local tx failed for {trade_mint}: {e}", flush=True)
            return None

    # Unexpected shape
    try:
        preview = json.dumps(resp)[:300]
    except Exception:
        preview = str(resp)[:300]
    print(f"[BUY ERR] unexpected PumpPortal response for {trade_mint}: {preview}", flush=True)
    return None
