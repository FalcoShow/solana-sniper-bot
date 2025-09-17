# utils_pkg/pump_trader.py
from __future__ import annotations

import os
import json
import time
import urllib.parse
from typing import Dict, Any, Optional, Tuple, List

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

# Local builder often returns HTML/non-JSON; keep OFF unless you’ve verified it.
USE_LOCAL_FALLBK = _is_true(os.getenv("PF_USE_LOCAL_FALLBACK", "0"))

SOL_RPC_URL      = (os.getenv("SOL_RPC_URL") or "").strip()
HELIUS_API_KEY   = (os.getenv("HELIUS_API_KEY") or "").strip()

# Optional: tiny post-buy resolve (sig→mint) poll
PF_RESOLVE_MINT_AFTER_BUY = _is_true(os.getenv("PF_RESOLVE_MINT_AFTER_BUY", "1"))
PF_RESOLVE_TRIES          = int(os.getenv("PF_RESOLVE_TRIES", "6"))     # ~6s
PF_RESOLVE_SLEEP_SEC      = float(os.getenv("PF_RESOLVE_SLEEP_SEC", "1"))

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

def _strip_pump_suffix(raw: str) -> Tuple[str, bool]:
    s = (raw or "").strip()
    if s.endswith("pump"):
        return s[:-4].strip(), True
    return s, False

def _slippage_percent_from_bps(bps: int) -> float:
    return float(bps) / 100.0

def _extract_qp(url: str, key: str) -> Optional[str]:
    try:
        q = urllib.parse.urlparse(url).query
        return urllib.parse.parse_qs(q).get(key, [None])[0]
    except Exception:
        return None

def _helius_key() -> Optional[str]:
    if HELIUS_API_KEY:
        return HELIUS_API_KEY
    k = _extract_qp(SOL_RPC_URL, "api-key")
    return (k or "").strip() or None

# ──────────────────────────────────────────────────────────────────────────────
# PumpPortal HTTP
# ──────────────────────────────────────────────────────────────────────────────
class TemporaryPumpPortal(Exception):
    pass

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
    mint: str,   # NOTE: PumpPortal expects a *mint string only*. For Pump, this may be a real SPL mint *or* a pump id.
) -> Dict[str, Any]:
    return {
        "action": action,
        "amount": float(amount_sol),
        "denominatedInSol": True,
        "slippage": _slippage_percent_from_bps(slippage_bps),
        "priorityFee": float(priority_sol),
        "pool": pool,
        "publicKey": payer_pubkey,
        "mint": str(mint),  # <- only field supported now
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

def _resp_errors(resp: Dict[str, Any]) -> List[str]:
    errs = resp.get("errors") or []
    if isinstance(errs, dict):
        errs = [str(errs)]
    return [str(e) for e in errs] if isinstance(errs, list) else ([str(errs)] if errs else [])

# ──────────────────────────────────────────────────────────────────────────────
# Mint resolvers (for your positions/txlearn, not required for trades)
# ──────────────────────────────────────────────────────────────────────────────
def resolve_true_mint_from_pump_id(pump_id: str) -> Optional[str]:
    """
    Best-effort: Given a PUMP ID (usually '<base>pump'), try to infer the real SPL mint.
    Uses Helius Enhanced Tx History on the *base* address.
    """
    key = _helius_key()
    base, had = _strip_pump_suffix(pump_id)
    if not (key and had and _looks_base58(base)):
        return None
    try:
        url = f"https://api.helius.xyz/v0/addresses/{base}/transactions?api-key={key}&limit=25"
        r = requests.get(url, timeout=8)
        data = r.json() if r.status_code == 200 else []
    except Exception:
        return None
    for tx in data or []:
        for t in (tx.get("tokenTransfers") or []):
            m = (t.get("mint") or "").strip()
            if _looks_base58(m):
                return m
        for ev in (tx.get("events") or {}).values():
            if isinstance(ev, dict):
                for k in ("mint", "mintAddress", "mint_address", "tokenMint", "token_mint"):
                    m = (ev.get(k) or "").strip()
                    if _looks_base58(m):
                        return m
    return None

def resolve_true_mint_from_signature(sig: str) -> Optional[str]:
    """
    Best-effort: Pull the transaction via Helius Parse API and extract a token mint we interacted with.
    """
    key = _helius_key()
    if not (key and sig):
        return None
    try:
        url = f"https://api.helius.xyz/v0/transactions?api-key={key}"
        r = requests.post(url, json={"transactions": [sig]}, timeout=8)
        arr = r.json() if r.status_code == 200 else []
    except Exception:
        return None
    for tx in arr or []:
        for t in (tx.get("tokenTransfers") or []):
            m = (t.get("mint") or "").strip()
            if _looks_base58(m):
                return m
        for ev in (tx.get("events") or {}).values():
            if isinstance(ev, dict):
                for k in ("mint", "mintAddress", "mint_address", "tokenMint", "token_mint"):
                    m = (ev.get(k) or "").strip()
                    if _looks_base58(m):
                        return m
    return None

def resolve_true_mint(pump_or_mint: str, *, signature: Optional[str] = None) -> Optional[str]:
    """
    Convenience: try signature first (most precise), then pump id base address.
    Returns a base58 SPL mint or None.
    """
    m = resolve_true_mint_from_signature(signature) if signature else None
    if m:
        return m
    m2 = resolve_true_mint_from_pump_id(pump_or_mint)
    if m2:
        return m2
    # If it's already a plausible base58 and not a pump id, return as-is
    s, had = _strip_pump_suffix(pump_or_mint)
    if not had and _looks_base58(s):
        return s
    return None

# ──────────────────────────────────────────────────────────────────────────────
# Candidate mints to send to PumpPortal (no bondingCurve anymore)
# ──────────────────────────────────────────────────────────────────────────────
def _candidate_mints(raw_hint: str) -> List[Tuple[str, str]]:
    """
    Returns a list of (mint_to_send, note).
    If the input ends with 'pump', try the *full* id first (this is what PumpPortal expects),
    then optionally try the base as a fallback.
    For plain base58, just try it as-is.
    """
    out: List[Tuple[str, str]] = []
    base, had = _strip_pump_suffix((raw_hint or "").strip())
    if had:
        out.append((raw_hint.strip(), "as-pump-id"))
        if _looks_base58(base):
            out.append((base, "as-base58-from-id"))
    else:
        if _looks_base58(base):
            out.append((base, "as-base58"))
        else:
            # last resort: pass-through; server may still accept non-standard ids
            out.append((raw_hint.strip(), "as-raw"))
    return out

# ──────────────────────────────────────────────────────────────────────────────
# BUY
# ──────────────────────────────────────────────────────────────────────────────
async def maybe_buy(mint: str, **kwargs) -> Optional[str]:
    """
    Buy on PumpPortal. `mint` may be:
      - a real SPL mint (base58), or
      - a Pump id (like '<base>pump'), which the API accepts in the 'mint' field.
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

    raw_hint: str = (ui.get("rawMint") or ui.get("raw") or mint or "").strip()

    kp = load_keypair()
    payer_pub = get_pubkey_str(kp)

    if PF_DEBUG_TX:
        print(f"[trade] pool={pool} symbol={symbol or '-'} "
              f"raw={raw_hint} label={label}")

    def _try_once(send_mint: str, note: str) -> Tuple[Optional[str], Optional[List[str]], Optional[Dict[str, Any]]]:
        body = _build_body(
            action="buy",
            pool=pool,
            payer_pubkey=payer_pub,
            amount_sol=amt_sol,
            slippage_bps=PF_SLIPPAGE_BPS,
            priority_sol=PF_PRIORITY_SOL,
            mint=send_mint,
        )
        try:
            resp = _post_json(_url_lightning(), body)
        except TemporaryPumpPortal as te:
            print(f"[BUY TEMP] lightning/{note} for {label}: {te}")
            return None, None, None
        except Exception as e:
            print(f"[BUY ERR] lightning/{note} HTTP for {label}: {e}")
            return None, None, None

        errs = _resp_errors(resp)
        if errs:
            print(f"[BUY ERR] lightning/{note} errors for {label}: {errs}")
            return None, errs, resp

        sig = resp.get("signature")
        if sig:
            print(f"[BUY OK] {label} — executed via Lightning. https://solscan.io/tx/{sig} [payer={payer_pub}]")
            return sig, None, resp

        tx_b64 = resp.get("transaction") or resp.get("tx")
        if tx_b64:
            try:
                sig2 = sign_and_send_b64_tx(tx_b64, kp, skip_preflight=True)
                print(f"[BUY OK] {label} — local sign. https://solscan.io/tx/{sig2} [payer={payer_pub}]")
                return sig2, None, resp
            except Exception as e:
                print(f"[BUY ERR] local sign failed for {label}: {e}")
        else:
            print(f"[BUY ERR] unexpected response for {label}: {json.dumps(resp)[:300]}")
        return None, None, resp

    # Attempt in smart order
    attempts = _candidate_mints(raw_hint)
    for send_mint, note in attempts:
        if PF_DEBUG_TX:
            print(f"[route] {label} try={note} mint={send_mint}")
        sig, errs, _resp = _try_once(send_mint, note)
        if sig:
            # Optional: try to resolve a real SPL mint for downstream consumers
            if PF_RESOLVE_MINT_AFTER_BUY:
                try:
                    resolved = resolve_true_mint(raw_hint, signature=sig)
                    if resolved:
                        print(f"[buy-hint] resolved_mint={resolved} for {label}")
                except Exception:
                    pass
            return sig
        # Local builder rarely helps; keep the hook for completeness
        if errs and USE_LOCAL_FALLBK:
            try:
                body = _build_body(
                    action="buy",
                    pool=pool,
                    payer_pubkey=payer_pub,
                    amount_sol=amt_sol,
                    slippage_bps=PF_SLIPPAGE_BPS,
                    priority_sol=PF_PRIORITY_SOL,
                    mint=send_mint,
                )
                resp2 = _post_json(_url_local(), body)
                e2 = _resp_errors(resp2)
                if not e2:
                    sig2 = resp2.get("signature")
                    if sig2:
                        print(f"[BUY OK] {label} — executed via Local. https://solscan.io/tx/{sig2} [payer={payer_pub}]")
                        return sig2
            except Exception as e:
                print(f"[BUY ERR] local/{note} HTTP for {label}: {e}")

    return None
