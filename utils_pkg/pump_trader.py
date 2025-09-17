# utils_pkg/pump_trader.py
from __future__ import annotations

"""
Robust PumpPortal buy helper.

Key behavior:
- If an incoming id ends with 'pump', treat it as a Pump.fun bonding-curve id.
- Try Lightning with bondingCurve first; if schema-ish errors, resolve curve → mint
  using Pump.fun public endpoints, then retry with mint. Finally, Helius fallback.
- Never send a curve id in the 'mint' field.
- Local builder is optional (PF_USE_LOCAL_FALLBACK=1) and OFF by default.

Env (most relevant):
  PF_PP_KEY                PumpPortal API key (required)
  DRY_RUN                  "1" to simulate
  PF_SLIPPAGE_BPS          default 1000 (10%)
  PF_PRIORITY_SOL          default 0.0005
  PF_POOLS                 comma list, default "pump"
  PF_DEBUG_TX              default 0
  PF_PP_DOUBLETRY          kept for compatibility; no harmful flip attempts
  PF_USE_LOCAL_FALLBACK    default 0 (HTML/non-JSON risk)
  SOL_RPC_URL              your RPC (Helius URL ok)
  HELIUS_API_KEY           optional; auto-extracted from SOL_RPC_URL if absent
  PF_TRADE_SOL / PF_TRADE_MIN_SOL / PF_TRADE_MAX_SOL (sizing)
"""

import json
import os
import time
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple

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
PF_SLIPPAGE_BPS  = int(os.getenv("PF_SLIPPAGE_BPS", "1000"))   # 10%
PF_PRIORITY_SOL  = float(os.getenv("PF_PRIORITY_SOL", "0.0005"))
PF_POOLS         = {p.strip().lower() for p in (os.getenv("PF_POOLS", "pump").split(","))}
PF_DEBUG_TX      = _is_true(os.getenv("PF_DEBUG_TX", "0"))
PF_PP_DOUBLETRY  = _is_true(os.getenv("PF_PP_DOUBLETRY", "1"))  # kept; used only for harmless retries
USE_LOCAL_FALLBK = _is_true(os.getenv("PF_USE_LOCAL_FALLBACK", "0"))
HTTP_TIMEOUT     = int(os.getenv("PF_HTTP_TIMEOUT", "20"))

SOL_RPC_URL      = (os.getenv("SOL_RPC_URL") or "").strip()
HELIUS_API_KEY   = (os.getenv("HELIUS_API_KEY") or "").strip()

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

_BASE58 = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")

def _looks_base58(s: str) -> bool:
    return bool(s) and isinstance(s, str) and all(ch in _BASE58 for ch in s)

def _split_id(raw: str) -> Tuple[Optional[str], Optional[str], bool]:
    """
    Returns (mint, bonding_curve, is_pump_id).
    - "....pump" → bonding curve id (strip 'pump' suffix)
    - plain base58 → mint
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

def _get_json(url: str, timeout: int = 8) -> Optional[Dict[str, Any]]:
    try:
        r = requests.get(url, timeout=timeout)
    except Exception:
        return None
    if r.status_code != 200:
        return None
    ctype = (r.headers.get("content-type") or "").lower()
    if "application/json" not in ctype:
        return None
    try:
        return r.json()
    except Exception:
        return None

# ──────────────────────────────────────────────────────────────────────────────
# PumpPortal HTTP
# ──────────────────────────────────────────────────────────────────────────────

class TemporaryPumpPortal(Exception):
    """Used when the endpoint returned 200 but not JSON or similar transient issue."""

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
    # IMPORTANT: exactly one of these.
    if bonding_curve and not mint:
        body["bondingCurve"] = bonding_curve
    elif mint and not bonding_curve:
        body["mint"] = mint
    return body

def _post_json(url: str, body: Dict[str, Any], timeout: int = HTTP_TIMEOUT) -> Dict[str, Any]:
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

def _is_schemaish(errs: List[str]) -> bool:
    # errors that indicate the request schema/fields weren't accepted
    t = " | ".join(e.lower() for e in errs)
    return any(k in t for k in [
        "invalid public key input",
        "mint must be a string",
        "invalid param",
        "wrongsize",
        "missing required",
    ])

# ──────────────────────────────────────────────────────────────────────────────
# Resolve bondingCurve → token mint
# ──────────────────────────────────────────────────────────────────────────────

def _resolve_curve_to_mint(curve: str) -> Optional[str]:
    """
    Resolve a Pump bonding-curve id to its token mint.

    Strategy:
      1) Pump.fun public endpoints (fastest, most reliable for new coins)
      2) PumpPortal public endpoints (extra chance)
      3) Helius Enhanced history fallback (RPC-key derived if not given)

    Returns base58 mint or None.
    """
    curve = (curve or "").strip()
    if not _looks_base58(curve):
        return None

    # Pump.fun public API variants
    pump_urls = [
        f"https://frontend-api.pump.fun/coins/{curve}",
        f"https://frontend-api.pump.fun/coins/curve/{curve}",
        f"https://frontend-api.pump.fun/bonding-curves/{curve}",
    ]
    # PumpPortal public variants (sometimes proxied JSON)
    portal_urls = [
        f"https://pumpportal.fun/api/coins/{curve}",
        f"https://pumpportal.fun/api/v1/coins/{curve}",
    ]

    def _extract_mint(js: Any) -> Optional[str]:
        if isinstance(js, dict):
            # Common top-level fields
            for k in ("mint", "mintAddress", "mint_address", "tokenMint", "token_mint"):
                v = (js.get(k) or "").strip()
                if _looks_base58(v):
                    return v
            # Wrapped payloads
            data = js.get("data") if isinstance(js, dict) else None
            if isinstance(data, dict):
                for k in ("mint", "mintAddress", "tokenMint", "token_mint"):
                    v = (data.get(k) or "").strip()
                    if _looks_base58(v):
                        return v
        return None

    # 1) Pump.fun
    for u in pump_urls:
        js = _get_json(u, timeout=6)
        if js:
            m = _extract_mint(js)
            if m:
                return m

    # 2) PumpPortal public
    for u in portal_urls:
        js = _get_json(u, timeout=6)
        if js:
            m = _extract_mint(js)
            if m:
                return m

    # 3) Helius Enhanced history
    key = _helius_key()
    if not key:
        return None
    try:
        url = f"https://api.helius.xyz/v0/addresses/{curve}/transactions?api-key={key}&limit=25"
        r = requests.get(url, timeout=8)
        data = r.json() if r.status_code == 200 else []
    except Exception:
        data = []

    for tx in data or []:
        # tokenTransfers
        for t in tx.get("tokenTransfers", []) or []:
            m = (t.get("mint") or "").strip()
            if _looks_base58(m):
                return m
        # generic events bundle
        ev = tx.get("events") or {}
        for k in ("mint", "token", "swap", "amm"):
            maybe = ev.get(k) or {}
            for key_name in ("mint", "mintAddress", "mint_address", "tokenMint", "token_mint"):
                m = (maybe.get(key_name) or "").strip()
                if _looks_base58(m):
                    return m

    return None

# ──────────────────────────────────────────────────────────────────────────────
# BUY
# ──────────────────────────────────────────────────────────────────────────────

async def maybe_buy(mint: str, **kwargs) -> Optional[str]:
    """
    Attempt to buy via PumpPortal Lightning, with safe retries:
      - If given a '...pump' id, try bondingCurve first.
      - If schema-ish error, resolve curve → mint and retry with mint.
      - If given a base58 mint, send mint directly.
      - Local builder can be enabled via PF_USE_LOCAL_FALLBACK=1.
    Returns transaction signature if successful.
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

    # figure out what we actually got from the feed
    raw_hint: str = (ui.get("rawMint") or ui.get("raw") or mint or "").strip()
    tok_mint, curve_id, is_pump_id = _split_id(raw_hint)

    # If caller passed a plain base58 mint in the function argument, prefer it.
    if not is_pump_id and not tok_mint and _looks_base58(mint):
        tok_mint = mint

    if PF_DEBUG_TX:
        if is_pump_id:
            print(f"[trade] pool={pool} symbol={symbol} mint=- curve={curve_id} "
                  f"(from raw='{raw_hint}') for {label}")
        else:
            print(f"[trade] pool={pool} symbol={symbol} mint={tok_mint or '-'} curve=- "
                  f"(from raw='{raw_hint}') for {label}")

    kp = load_keypair()
    payer_pub = get_pubkey_str(kp)

    def _try(url: str, body: Dict[str, Any], tag: str) -> Tuple[Optional[str], Optional[List[str]], Optional[Dict[str, Any]]]:
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
            print(f"[BUY ERR] {tag} errors for {label}: {errs}")
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

    async def _attempt(one_mint: Optional[str], one_curve: Optional[str], note: str) -> Tuple[Optional[str], Optional[List[str]]]:
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
        # 1) Lightning
        sig, errs, _ = _try(_url_lightning(), body, f"lightning/{note}")
        if sig:
            return sig, None

        # 2) Local builder (optional)
        if errs and _is_schemaish(errs) and USE_LOCAL_FALLBK:
            sig2, errs2, _ = _try(_url_local(), body, f"local/{note}")
            if sig2:
                return sig2, None
            if errs2:
                return None, errs2

        return None, errs

    # ── Path A: Pump curve id (no mint yet)
    if pool == "pump" and curve_id and not tok_mint:
        # First try bondingCurve (Lightning sometimes accepts this)
        sig0, errs0 = await _attempt(None, curve_id, "curve")
        if sig0:
            return sig0

        # If schema-ish, resolve curve → mint then retry with mint
        should_resolve = (errs0 and _is_schemaish(errs0)) or True  # resolve aggressively
        if should_resolve:
            resolved = _resolve_curve_to_mint(curve_id)
            if resolved and _looks_base58(resolved):
                if PF_DEBUG_TX:
                    print(f"[resolver] curve {curve_id} → mint {resolved}")
                # small backoff so indexers catch up
                try:
                    time.sleep(0.25)
                except Exception:
                    pass
                sigR, _ = await _attempt(resolved, None, "mint-from-curve")
                if sigR:
                    return sigR

    # ── Path B: We have a mint
    if tok_mint:
        sig1, _ = await _attempt(tok_mint, None, "mint")
        if sig1:
            return sig1

        # harmless retry: tiny backoff then retry once (no flips)
        if PF_PP_DOUBLETRY:
            time.sleep(0.15)
            sig1b, _ = await _attempt(tok_mint, None, "mint-retry")
            if sig1b:
                return sig1b

    # No success
    return None
