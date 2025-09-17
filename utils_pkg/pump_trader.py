# utils_pkg/pump_trader.py
from __future__ import annotations

import os
import json
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
PF_PP_DOUBLETRY  = _is_true(os.getenv("PF_PP_DOUBLETRY", "1"))
# Local fallback often returns HTML / non-JSON; keep OFF unless you know it works.
USE_LOCAL_FALLBK = _is_true(os.getenv("PF_USE_LOCAL_FALLBACK", "0"))

SOL_RPC_URL      = (os.getenv("SOL_RPC_URL") or "").strip()
HELIUS_API_KEY   = (os.getenv("HELIUS_API_KEY") or "").strip()

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
        base = s[:-4].strip()
        return base, True
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
    # Exactly one of these:
    if bonding_curve and not mint:
        body["bondingCurve"] = str(bonding_curve)
    elif mint and not bonding_curve:
        body["mint"] = str(mint)
    return body

def _post_json(url: str, body: Dict[str, Any], timeout: int = 20) -> Dict[str, Any]:
    r = requests.post(url, json=body, timeout=timeout, allow_redirects=False)
    content_type = (r.headers.get("content-type") or "").split(";")[0].strip().lower()
    # A lot of non-JSON 200s happen on trade-local
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
    t = " | ".join(e.lower() for e in errs)
    return any(k in t for k in [
        "invalid public key input",
        "mint must be a string",
        "invalid param",
        "wrongsize",
    ])

# ──────────────────────────────────────────────────────────────────────────────
# Helius fallback: resolve bondingCurve → token mint from recent txs
# ──────────────────────────────────────────────────────────────────────────────
def _resolve_curve_to_mint(curve: str) -> Optional[str]:
    """
    Heuristic resolver using Helius Enhanced Transaction History.
    Looks for any token transfer mentioning a mint address.
    """
    key = _helius_key()
    if not key or not _looks_base58(curve):
        return None
    try:
        url = f"https://api.helius.xyz/v0/addresses/{curve}/transactions?api-key={key}&limit=25"
        r = requests.get(url, timeout=8)
        data = r.json() if r.status_code == 200 else []
    except Exception:
        return None

    for tx in data or []:
        for t in (tx.get("tokenTransfers") or []):
            m = (t.get("mint") or "").strip()
            if _looks_base58(m):
                return m
        ev = tx.get("events") or {}
        for k in ("mint", "token", "swap", "amm"):
            maybe = ev.get(k) or {}
            for key_name in ("mint", "mintAddress", "mint_address", "tokenMint", "token_mint"):
                m = (maybe.get(key_name) or "").strip()
                if _looks_base58(m):
                    return m
    return None

# ──────────────────────────────────────────────────────────────────────────────
# Candidates builder: prefer treating “…pump” base as MINT first, then CURVE
# ──────────────────────────────────────────────────────────────────────────────
def _candidates_from_raw(raw_hint: str, default_mint: Optional[str]) -> List[Tuple[Optional[str], Optional[str], str]]:
    """
    Returns a list of (mint, curve, note) attempts, in order.
    We favor interpreting `<base>pump` as a *mint* first. If that fails, try as *curve*.
    Finally, try whatever explicit mint the caller passed.
    """
    cands: List[Tuple[Optional[str], Optional[str], str]] = []
    base, had_suffix = _strip_pump_suffix(raw_hint)
    base_ok = _looks_base58(base)

    if had_suffix and base_ok:
        # 1) Try base as mint (most feeds actually append 'pump' to a mint)
        cands.append((base, None, "as-mint-from-id"))
        # 2) Try base as curve
        cands.append((None, base, "as-curve-from-id"))

    elif base_ok:
        # Plain base58 string → treat as mint
        cands.append((base, None, "as-mint-plain"))

    # If the caller gave an explicit plain mint that differs, try it too
    if default_mint and _looks_base58(default_mint) and default_mint != base:
        cands.append((default_mint, None, "as-mint-explicit"))

    return cands

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

    raw_hint: str = (ui.get("rawMint") or ui.get("raw") or mint or "").strip()
    kp = load_keypair()
    payer_pub = get_pubkey_str(kp)

    # Build attempts
    attempts: List[Tuple[Optional[str], Optional[str], str]] = _candidates_from_raw(raw_hint, mint)

    # If we have a curve candidate, we'll optionally try to resolve it → mint via Helius at the end
    curve_bases = [c for m,c,_ in attempts if c]

    if PF_DEBUG_TX:
        for m,c,n in attempts:
            print(f"[route] {label} try={n} mint={m or '-'} curve={c or '-'}")

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

    # Try candidates in order (prefer interpreting “…pump” base as mint)
    for cand_mint, cand_curve, note in attempts:
        sig, _ = await _attempt(cand_mint, cand_curve, note)
        if sig:
            return sig

    # If none succeeded and we had a curve base, try resolve curve → mint via Helius then buy with mint
    if pool == "pump":
        for curve in curve_bases:
            resolved = _resolve_curve_to_mint(curve)
            if resolved and _looks_base58(resolved):
                if PF_DEBUG_TX:
                    print(f"[resolver] curve {curve} → mint {resolved}")
                sigR, _ = await _attempt(resolved, None, "mint-from-curve")
                if sigR:
                    return sigR

    # Optional flip once, just in case
    if PF_PP_DOUBLETRY and attempts:
        m0, c0, _ = attempts[0]
        # flip whichever we tried first
        if m0:
            sig2, _ = await _attempt(None, m0, "flip-mint→curve")
            if sig2:
                return sig2
        if c0:
            sig3, _ = await _attempt(c0, None, "flip-curve→mint")
            if sig3:
                return sig3

    return None
