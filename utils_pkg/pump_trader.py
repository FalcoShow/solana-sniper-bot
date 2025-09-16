# utils_pkg/pump_trader.py
from __future__ import annotations

import os
import json
import asyncio
from typing import Dict, Any, Optional

import requests

from utils_pkg.solana_wallet import (
    load_keypair,
    get_pubkey_str,
    sign_and_send_b64_tx,
)

# Optional: notify the exit engine after a buy fills.
# This import does NOT create a circular import (trader.py doesn't import this file).
try:
    from utils_pkg.trader import exit_engine, _wait_tx_confirmed  # type: ignore
    _HAS_EXIT_ENGINE = True
except Exception:
    _HAS_EXIT_ENGINE = False


# -----------------------------------------------------------------------------
# Env / config
# -----------------------------------------------------------------------------
def _is_true(v: Optional[str]) -> bool:
    return str(v).lower() in {"1", "true", "yes", "y", "on"}

PP_API_KEY       = (os.getenv("PF_PP_KEY") or "").strip()
DRY_RUN          = _is_true(os.getenv("DRY_RUN", "1"))
PF_SLIPPAGE_BPS  = int(os.getenv("PF_SLIPPAGE_BPS", "1000"))     # 1000 bps = 10.00%
PF_PRIORITY_SOL  = float(os.getenv("PF_PRIORITY_SOL", "0.0005")) # in SOL

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


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _is_probably_pump_mint(mint: str) -> bool:
    """
    Lightning 'pump' pool only works for pump.fun bonding-curve tokens.
    Heuristic: pump mints end with 'pump'.
    """
    return mint.endswith("pump")


def _derive_pool(ui: Dict[str, Any]) -> str:
    """Prefer any pool/platform hint from upstream UI payloads; default 'pump'."""
    pool = (ui.get("pool") or ui.get("platform") or "").strip().lower()
    return pool if pool else "pump"


# -----------------------------------------------------------------------------
# PumpPortal Lightning (/api/trade) — server-side execution
# -----------------------------------------------------------------------------
def call_lightning_trade(
    *,
    action: str,            # "buy" | "sell"
    mint: str,
    payer_pubkey: str,
    amount_sol: float,
    slippage_bps: int,
    priority_sol: float,
    pool: str = "pump",
    timeout: int = 20,
) -> Dict[str, Any]:
    """
    POST https://pumpportal.fun/api/trade?api-key=...
    Lightning executes on their side and returns JSON like:
      { "signature": "...", "errors": [] }
    or more rarely:
      { "transaction": "<base64 tx>", "errors": [] }
    """
    if not PP_API_KEY:
        raise RuntimeError("PF_PP_KEY is not set (trading requires a PumpPortal API key).")

    url = f"https://pumpportal.fun/api/trade?api-key={PP_API_KEY}"

    body = {
        "action": action,                   # "buy" or "sell"
        "mint": mint,
        "amount": float(amount_sol),
        "denominatedInSol": True,           # 'amount' is in SOL
        "slippage": float(slippage_bps) / 100.0,  # bps -> percent for Lightning
        "priorityFee": float(priority_sol),
        "pool": pool,                       # usually "pump"
        "publicKey": payer_pubkey,
    }

    r = requests.post(url, json=body, timeout=timeout)

    # Lightning frequently returns HTTP 200 even on logical errors — always parse.
    try:
        js = r.json()
    except Exception:
        r.raise_for_status()
        raise

    # Normalize rare alt key
    if "tx" in js and "transaction" not in js:
        js["transaction"] = js["tx"]

    # If non-200, include parsed JSON context
    if not r.ok:
        raise RuntimeError(f"Lightning {r.status_code}: {js}")

    return js


# -----------------------------------------------------------------------------
# Maybe buy
# -----------------------------------------------------------------------------
async def maybe_buy(mint: str, **kwargs) -> None:
    """
    Attempt a buy via PumpPortal Lightning.
    - Skips non-pump pools/tokens (prevents 'Failed to get bonding curve' spam).
    - Respects DRY_RUN.
    - If Lightning returns a signature, we log success.
    - If Lightning returns a transaction, we sign & send it.
    - Optionally triggers the exit engine after confirmation (if available).
    """
    ui = (kwargs.get("ui") or {})
    symbol = (kwargs.get("symbol") or ui.get("symbol") or "").strip()
    pool = _derive_pool(ui)
    label = f"{symbol} ({mint})" if symbol else mint

    # Only send Lightning orders to the 'pump' pool / pump.fun mints
    if pool == "pump" and not _is_probably_pump_mint(mint):
        print(f"[skip] non-pump mint pattern for {label} (pool='pump'); skipping.")
        return
    if pool != "pump":
        # We currently only support Lightning 'pump' pool here.
        print(f"[skip] unsupported pool '{pool}' for {label}; skipping.")
        return

    amount_sol = current_trade_sol()

    if DRY_RUN:
        print(f"[DRYRUN] would buy {label} for {amount_sol} SOL")
        return

    kp = load_keypair()
    payer_pub = get_pubkey_str(kp)

    try:
        resp = call_lightning_trade(
            action="buy",
            mint=mint,
            payer_pubkey=payer_pub,
            amount_sol=amount_sol,
            slippage_bps=PF_SLIPPAGE_BPS,
            priority_sol=PF_PRIORITY_SOL,
            pool="pump",
        )
    except Exception as e:
        print(f"[BUY ERR] lightning call failed for {mint}: {e}")
        return

    # Common error payloads still return 200 with {"errors": [...]}
    errs = resp.get("errors") or []
    if isinstance(errs, list) and errs:
        # Special-case a frequent error for clarity
        joined = " | ".join(map(str, errs))
        if "Failed to get bonding curve" in joined:
            print(f"[skip] {label}: Lightning says token is not on pump bonding curve.")
            return
        print(f"[BUY ERR] lightning returned errors for {mint}: {errs}")
        return

    # The happy path: server executed already and returns a signature.
    sig = resp.get("signature")
    if sig:
        print(f"[BUY OK] {label} — executed via Lightning. https://solscan.io/tx/{sig}")

        # Optionally ping the exit engine after confirmation, if available.
        if _HAS_EXIT_ENGINE:
            try:
                # Wait a moment to reduce “not indexed yet” races on some RPCs
                ok = await _wait_tx_confirmed(sig, max_secs=30)  # type: ignore
                if not ok:
                    # Not fatal; the exit engine has internal polling/fallbacks
                    print(f"[BUY WARN] {mint} signature not confirmed within timeout.")
                # Hand off to the exit engine (fire-and-forget)
                asyncio.create_task(exit_engine.on_buy_filled(mint))  # type: ignore
            except Exception:
                # Non-fatal: the trade already landed
                pass
        return

    # Less common: server returned a base64 transaction to sign locally.
    tx_b64 = resp.get("transaction")
    if tx_b64:
        try:
            sig2 = await sign_and_send_b64_tx(tx_b64, kp, skip_preflight=True)
            print(f"[BUY OK] {label} — signed & sent. https://solscan.io/tx/{sig2}")
            if _HAS_EXIT_ENGINE:
                try:
                    ok = await _wait_tx_confirmed(sig2, max_secs=30)  # type: ignore
                    if not ok:
                        print(f"[BUY WARN] {mint} signature not confirmed within timeout.")
                    asyncio.create_task(exit_engine.on_buy_filled(mint))  # type: ignore
                except Exception:
                    pass
        except Exception as e:
            print(f"[BUY ERR] send failed for {mint}: {e}")
        return

    # If neither signature nor transaction was present, log the payload for debugging.
    print(f"[BUY ERR] unexpected Lightning response for {mint}: {json.dumps(resp)[:400]}")
