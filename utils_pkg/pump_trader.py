# utils_pkg/pump_trader.py
from __future__ import annotations

import asyncio
import json
import os
from typing import Any, Dict, Optional, Tuple

import requests

from utils_pkg.solana_wallet import (
    load_keypair,
    get_pubkey_str,
    sign_and_send_b64_tx,
)
from utils_pkg.txlearn import TxLearner


# ──────────────────────────────────────────────────────────────────────────────
# Env / config
# ──────────────────────────────────────────────────────────────────────────────
def _is_true(v: Optional[str]) -> bool:
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}

PP_API_KEY         = (os.getenv("PF_PP_KEY") or "").strip()
DRY_RUN            = _is_true(os.getenv("DRY_RUN", "0"))
PF_SLIPPAGE_BPS    = int(os.getenv("PF_SLIPPAGE_BPS", "1000"))        # 10%
PF_PRIORITY_SOL    = float(os.getenv("PF_PRIORITY_SOL", "0.0005"))
PF_POOLS           = {p.strip().lower() for p in (os.getenv("PF_POOLS", "pump").split(","))}
PF_DEBUG_TX        = _is_true(os.getenv("PF_DEBUG_TX", "0"))
PF_PP_DOUBLETRY    = _is_true(os.getenv("PF_PP_DOUBLETRY", "1"))      # flip once between mint/curve on schema errs
USE_LOCAL_FALLBK   = _is_true(os.getenv("PF_USE_LOCAL_FALLBACK", "0")) # default OFF per your latest setup

# Post-buy learning controller (outer loop)
POS_RETRY_TRIES        = int(os.getenv("PF_POS_RETRY_TRIES", "10"))
POS_RETRY_SLEEP_SEC    = float(os.getenv("PF_POS_RETRY_SLEEP_SEC", "0"))  # extra pause between outer attempts
TXLEARN_AUTORUN        = _is_true(os.getenv("PF_TXLEARN_AUTORUN", "1"))   # run learner after buy by default

# txlearn inner loop knobs (forwarded to TxLearner)
TX_COMMITMENT          = os.getenv("PF_TXLEARN_COMMITMENT", "confirmed")
TX_TRIES               = int(os.getenv("PF_TXLEARN_TRIES", "20"))
TX_DELAY_SEC           = float(os.getenv("PF_TXLEARN_DELAY_SEC", "1.2"))
TX_WARMUP_SEC          = float(os.getenv("PF_TXLEARN_WARMUP_SEC", "2.0"))

OWNER_PUBKEY           = (os.getenv("PF_PP_ACCOUNT") or "").strip()
SOL_RPC_URL            = (os.getenv("SOL_RPC_URL") or "").strip()


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
# Base58 helpers & pump splitter
# ──────────────────────────────────────────────────────────────────────────────
_BASE58 = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")

def _looks_base58(s: str) -> bool:
    return bool(s) and isinstance(s, str) and all(ch in _BASE58 for ch in s)

def _split_mint_or_curve(raw: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (mint, bonding_curve).
    If input endswith 'pump' => it's a Pump bonding-curve id (strip suffix).
    Else treat as a token mint if it looks base58.
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
# PumpPortal HTTP helpers
# ──────────────────────────────────────────────────────────────────────────────
class TemporaryPumpPortal(Exception):
    """Non-fatal, retryable issues like non-JSON 200s or parse errors."""
    pass

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
    if bonding_curve:
        body["bondingCurve"] = bonding_curve
    if mint:
        body["mint"] = mint
    return body

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

def _is_schema_err(errs: list[str]) -> bool:
    text = " | ".join(e.lower() for e in errs)
    return any(k in text for k in [
        "invalid public key input",
        "mint must be a string",
        "wrongsize",
        "invalid param",
    ])


# ──────────────────────────────────────────────────────────────────────────────
# Post-buy learner (background)
# ──────────────────────────────────────────────────────────────────────────────
async def _learn_position_bg(mint: str, sig: str) -> None:
    """
    Outer retry loop (to match your prior logs like:
      [pos] retry learn <mint> attempt X/N... last_bal=0
    Each outer attempt runs an inner TxLearner loop that probes signature status,
    getTransaction, and owned token balance.
    """
    if not TXLEARN_AUTORUN:
        return

    tries = max(1, POS_RETRY_TRIES)
    last_bal = 0.0

    for i in range(1, tries + 1):
        # run one inner learn cycle
        learner = TxLearner(
            rpc_url=SOL_RPC_URL or None,
            owner=OWNER_PUBKEY or None,
            commitment=TX_COMMITMENT,
            tries=TX_TRIES,
            delay_sec=TX_DELAY_SEC,
            warmup_sec=TX_WARMUP_SEC,
        )
        try:
            res = await learner.learn(mint, sig)
        finally:
            await learner.close()

        last_bal = float(res.get("owner_balance_ui") or 0.0)
        if last_bal > 0:
            break

        if i < tries:
            print(f"[pos] retry learn {mint} attempt {i}/{tries}... last_bal={last_bal}", flush=True)
            if POS_RETRY_SLEEP_SEC > 0:
                await asyncio.sleep(POS_RETRY_SLEEP_SEC)

    # Final state log (optional)
    if last_bal <= 0:
        print(f"[pos] learn finished for {mint} with balance {last_bal} (no tokens detected)", flush=True)


# ──────────────────────────────────────────────────────────────────────────────
# BUY entry point
# ──────────────────────────────────────────────────────────────────────────────
async def maybe_buy(mint: str, **kwargs) -> Optional[str]:
    """
    Attempts to buy using PumpPortal Lightning.
    Accepts either a token mint (base58) or a Pump bonding-curve id ending with 'pump'.
    Will flip once between mint/curve if schema errors are returned.
    Spawns a background balance learner after success.
    Returns the signature on success, otherwise None.
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

    # Hints from upstream (IMPORTANT: pass raw ...pump here if you keep normalization upstream)
    curve_hint = ui.get("bondingCurve") or kwargs.get("bondingCurve")
    raw_hint   = ui.get("rawMint") or ui.get("raw") or ui.get("raw_mint") or kwargs.get("raw_mint")
    if isinstance(raw_hint, str) and raw_hint.endswith("pump"):
        raw_base = raw_hint[:-4].strip()
        if _looks_base58(raw_base):
            curve_hint = curve_hint or raw_base

    token_mint, bonding_curve = _split_mint_or_curve(mint)
    if curve_hint:
        bonding_curve = curve_hint
        # if engine already normalized to curve, avoid sending as mint
        if token_mint == bonding_curve:
            token_mint = None

    if PF_DEBUG_TX:
        print(f"[trade] pool={pool} symbol={symbol} "
              f"mint={token_mint or '-'} curve={bonding_curve or '-'} for {label}", flush=True)

    kp = load_keypair()
    payer_pub = get_pubkey_str(kp)

    def _try(url: str, body: Dict[str, Any], tag: str) -> Tuple[Optional[str], Optional[list[str]], Optional[Dict[str, Any]]]:
        try:
            resp = _post_json(url, body)
        except TemporaryPumpPortal as te:
            print(f"[BUY TEMP] {tag} for {label}: {te}", flush=True)
            return None, None, None
        except Exception as e:
            print(f"[BUY ERR] {tag} HTTP for {label}: {e}", flush=True)
            return None, None, None

        errs = _resp_errors(resp)
        if errs:
            return None, errs, resp

        # success: signature or unsigned tx
        sig = resp.get("signature")
        if sig:
            print(f"[BUY OK] {label} — executed via Lightning. https://solscan.io/tx/{sig} [payer={payer_pub}]", flush=True)
            return sig, None, None

        tx_b64 = resp.get("transaction") or resp.get("tx")
        if tx_b64:
            try:
                sig2 = sign_and_send_b64_tx(tx_b64, kp, skip_preflight=True)
                print(f"[BUY OK] {label} — local sign. https://solscan.io/tx/{sig2} [payer={payer_pub}]", flush=True)
                return sig2, None, None
            except Exception as e:
                print(f"[BUY ERR] local sign failed for {label}: {e}", flush=True)
        else:
            print(f"[BUY ERR] unexpected response for {label}: {json.dumps(resp)[:300]}", flush=True)

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

        # 1) Lightning (primary)
        sig, errs, _ = _try(_url_lightning(), body, f"lightning/{note}")
        if sig:
            return sig

        # 2) Optional local fallback on schema-ish errors (off by default)
        if errs and _is_schema_err(errs) and USE_LOCAL_FALLBK:
            sig2, errs2, _ = _try(_url_local(), body, f"local/{note}")
            if sig2:
                return sig2
            if errs2:
                print(f"[BUY ERR] pumpportal errors for {one_mint or one_curve or mint} ({note}): {errs2}", flush=True)
        elif errs:
            print(f"[BUY ERR] pumpportal errors for {one_mint or one_curve or mint} ({note}): {errs}", flush=True)

        return None

    # Prefer curve on Pump when we have a curve hint or a raw ...pump
    if pool == "pump" and bonding_curve and not token_mint:
        sig0 = await _attempt(None, bonding_curve, "curve-first")
        if sig0:
            if TXLEARN_AUTORUN:
                asyncio.create_task(_learn_position_bg(bonding_curve, sig0 if sig0 else ""))  # pass curve as the mint-id source if we bought via curve
            return sig0

    # Otherwise try mint first
    first_sig = await _attempt(token_mint, bonding_curve if token_mint is None else None, "first")
    if first_sig:
        if TXLEARN_AUTORUN:
            # if we sent mint, use the true token mint for learning; else fall back to whatever we have
            learn_key = token_mint or bonding_curve or mint
            asyncio.create_task(_learn_position_bg(learn_key, first_sig))
        return first_sig

    # Flip & retry once
    if PF_PP_DOUBLETRY and pool == "pump":
        flip_mint, flip_curve = (None, token_mint) if token_mint else (bonding_curve or mint, None)
        if PF_DEBUG_TX:
            print(f"[BUY RETRY] flipping to {'bondingCurve' if flip_curve else 'mint'}={flip_curve or flip_mint}", flush=True)
        second_sig = await _attempt(flip_mint, flip_curve, "retry-flip")
        if second_sig:
            if TXLEARN_AUTORUN:
                learn_key = flip_mint or flip_curve or mint
                asyncio.create_task(_learn_position_bg(learn_key, second_sig))
            return second_sig

    return None
