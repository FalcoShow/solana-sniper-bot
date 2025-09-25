# utils_pkg/pump_trader.py
from __future__ import annotations

import asyncio
import math
import os
import time
from typing import Any, Dict, Optional, Tuple, List

import requests

from logging_setup import get_logger, bump

# Router (Jupiter) for quotes, prices & execution
from utils_pkg.router import (
    get_buy_quote,
    get_sell_quote,
    buy_via_lightning,
    get_price_ui,
)

# Wallet & helpers (import defensively)
try:
    from utils_pkg.solana_wallet import (
        load_keypair,
        get_pubkey_str,
        sign_and_send_b64_tx,
    )
except Exception as e:  # pragma: no cover
    raise RuntimeError(f"solana_wallet core helpers missing: {e}")

try:
    # Prefer async helper if available
    from utils_pkg.solana_wallet import get_balance_sol as _get_balance_sol  # type: ignore
except Exception:  # pragma: no cover
    async def _get_balance_sol(_owner: str) -> float:  # type: ignore
        return 0.0

# Optional sizer (preferred)
try:
    from utils_pkg.sizer import suggest_trade_sol as _auto_size_sol  # type: ignore
except Exception:  # pragma: no cover
    _auto_size_sol = None  # type: ignore

# Optional safety filters (best-effort)
try:
    from utils_pkg.liquidity import looks_like_mint, screen_token  # type: ignore
except Exception:  # pragma: no cover
    def looks_like_mint(_: str) -> bool:  # type: ignore
        return False
    async def screen_token(_: str):  # type: ignore
        return True, {"ok": True}

# Optional global guardrails (Strategy checks first; we re-check here too)
try:
    from utils_pkg.guardrails import buys_allowed  # type: ignore
except Exception:  # pragma: no cover
    def buys_allowed() -> bool:  # type: ignore
        return True

# Optional external routeguard (preferred); internal fallback if missing
try:
    from utils_pkg.routeguard import preflight as rg_preflight  # type: ignore
except Exception:  # pragma: no cover
    rg_preflight = None  # type: ignore

log = get_logger()

# =============================================================================
# ENV helpers
# =============================================================================
def _envb(k: str, d: bool = False) -> bool:
    v = (os.getenv(k, "") or "").strip().lower()
    if not v:
        return d
    return v in {"1", "true", "yes", "y", "on"}

def _envf(k: str, d: float) -> float:
    try:
        return float(os.getenv(k, str(d)))
    except Exception:
        return float(d)

def _envi(k: str, d: int) -> int:
    try:
        return int(os.getenv(k, str(d)))
    except Exception:
        return int(d)

def _envi_alias(keys: List[str], d: int) -> int:
    for k in keys:
        v = os.getenv(k)
        if v is not None and v != "":
            try:
                return int(v)
            except Exception:
                pass
    return d

# =============================================================================
# ENV & knobs
# =============================================================================
IS_DRYRUN                = _envb("DRY_RUN", False)
OWNER_FROM_ENV           = (os.getenv("PF_PP_ACCOUNT") or "").strip()
MIN_SOL_BALANCE          = _envf("MIN_SOL_BALANCE", 0.20)

# Autosizing knobs (fallback if sizer module missing)
AUTOSIZE_REFRESH_SEC     = _envf("PF_TRADE_AUTOSIZE_REFRESH_SEC", 10.0)
RISK_PCT_OF_WALLET       = _envf("PF_TRADE_RISK_PCT", 0.05)    # 5% of live SOL bal
MIN_SOL_PER_TRADE        = _envf("PF_TRADE_MIN_SOL", 0.005)
MAX_SOL_PER_TRADE        = _envf("PF_TRADE_MAX_SOL", 0.08)
STEP_SOL                 = _envf("PF_TRADE_STEP_SOL", 0.005)

# Clamp safety if misconfigured
if MIN_SOL_PER_TRADE > MAX_SOL_PER_TRADE:
    MIN_SOL_PER_TRADE, MAX_SOL_PER_TRADE = MAX_SOL_PER_TRADE, MIN_SOL_PER_TRADE

# Buy pacing (with aliases used by your entrypoint logs)
BUY_COOLDOWN_SEC         = _envi_alias(["BUY_COOLDOWN_SEC", "PF_COOLDOWN_SEC"], 120)
MAX_CONCURRENT_BUYS      = _envi_alias(["MAX_CONCURRENT_BUYS", "PF_MAX_CONCURRENT"], 2)
NOROUTE_COOLDOWN_SEC     = _envi("NOROUTE_COOLDOWN_SEC", 600)

# Route guard policies (only used by internal fallback)
ROUTE_ALLOW_NO_ROUTE     = _envb("ROUTEGUARD_ALLOW_NO_ROUTE", True)
ROUTE_REQUIRE_SELL       = _envb("ROUTEGUARD_REQUIRE_SELL", True)

# Momentum gate (optional) — cache-aware to avoid false rejects
MOMO_ENABLED             = _envb("ENTRY_MOMENTUM_ENABLED", True)
MOMO_MIN_UP_PCT          = _envf("ENTRY_MOMO_MIN_UP_PCT", 5.0)     # +5% in window
MOMO_WINDOW_SEC          = _envf("ENTRY_MOMO_WINDOW_SEC", 10.0)
MOMO_PRICE_TRIES         = _envi("ENTRY_MOMO_PRICE_TRIES", 4)
MOMO_PRICE_DELAY_S       = _envf("ENTRY_MOMO_PRICE_DELAY_S", 0.7)
PRICE_TTL_SEC_ENV        = _envf("PF_PRICE_TTL_SEC", 5.0)          # router cache TTL

# Liquidity screen (Strategy may run it; we reuse for sizing)
LIQ_FILTER_ENABLED       = _envb("ENTRY_LIQ_FILTER", True)
ENTRY_MIN_LIQ_USD        = _envf("ENTRY_MIN_LIQ_USD", 10_000)
ENTRY_MAX_LIQ_USD        = _envf("ENTRY_MAX_LIQ_USD", 200_000)
ENTRY_MIN_IMPACT_BPS     = _envi("ENTRY_MIN_IMPACT_BPS", 250)
ENTRY_MAX_IMPACT_BPS     = _envi("ENTRY_MAX_IMPACT_BPS", 800)

# PumpPortal fallback (Lightning)
PP_API_KEY               = (os.getenv("PF_PP_KEY") or "").strip()
JUP_SLIPPAGE_BPS         = _envi("PF_SLIPPAGE_BPS", 600)
PP_PRIORITY_SOL          = _envf("PF_PRIORITY_SOL", 0.002)
PF_POOLS                 = {p.strip().lower() for p in (os.getenv("PF_POOLS", "pump").split(","))}
PF_DEBUG_TX              = _envb("PF_DEBUG_TX", False)
USE_LOCAL_FALLBK         = _envb("PF_USE_LOCAL_FALLBACK", False)

# Internal pacing state
_last_buy_at_global: float = 0.0
_last_no_route: Dict[str, float] = {}
_inflight = asyncio.Semaphore(MAX_CONCURRENT_BUYS)
_last_autosize_at: float = 0.0
_cached_wallet_sol: float = 0.0

# =============================================================================
# Small utils
# =============================================================================
_BASE58 = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")

def _looks_base58(s: str) -> bool:
    return bool(s) and isinstance(s, str) and all(ch in _BASE58 for ch in s)

def _strip_pump_suffix(raw: str) -> Tuple[str, bool]:
    s = (raw or "").strip()
    if s.endswith("pump"):
        return s[:-4].strip(), True
    return s, False

async def _get_sol_balance(owner: str) -> float:
    """Best-effort SOL balance using wallet helper."""
    try:
        return await _get_balance_sol(owner)
    except Exception:
        return 0.0

def _round_step(val: float, step: float) -> float:
    if step <= 0:
        return val
    return math.floor(val / step) * step

def _cooldown_ok(key: str, ttl: int) -> bool:
    now = time.time()
    last = _last_no_route.get(key, 0.0)
    return (now - last) >= ttl

def _note_no_route(key: str) -> None:
    _last_no_route[key] = time.time()

# =============================================================================
# Momentum gate (credit-efficient & cache-aware)
# =============================================================================
async def _momentum_ok(mint: str) -> bool:
    """
    Lightweight momentum confirmation. To avoid false negatives when the router
    price cache (PF_PRICE_TTL_SEC) ≈ momentum window, we skip internal momentum
    if the TTL would mask price movement; Strategy's gate already covers it.
    """
    if not MOMO_ENABLED:
        return True
    if not looks_like_mint(mint):
        return True  # can't price pre-mint reliably; defer to external gate
    if PRICE_TTL_SEC_ENV >= MOMO_WINDOW_SEC * 0.9:
        return True

    tries = max(2, MOMO_PRICE_TRIES)
    delay = max(MOMO_PRICE_DELAY_S, PRICE_TTL_SEC_ENV + 0.1)

    px0 = await get_price_ui(mint)
    if px0 <= 0:
        return False

    last_px = px0
    for _ in range(tries - 1):
        await asyncio.sleep(delay)
        last_px = await get_price_ui(mint)
        if last_px <= 0:
            return False

    pct_up = (last_px / px0 - 1.0) * 100.0
    ok = pct_up >= MOMO_MIN_UP_PCT
    # Use .event (widely available) instead of .every to avoid attribute issues
    log.event(
        "ENTRY_MOMO",
        mint=mint,
        px0=px0,
        px1=last_px,
        pct_up=round(pct_up, 2),
        ok=int(ok),
    )
    return ok

# =============================================================================
# Liquidity-aware size boost
# =============================================================================
async def _liquidity_size_boost(mint: str, base_sol: float) -> float:
    if not LIQ_FILTER_ENABLED or not looks_like_mint(mint):
        return base_sol
    ok, rep = await screen_token(mint)
    if not ok:
        return min(base_sol, 0.75 * base_sol)

    liq_usd = float(rep.get("liqUsd") or 0.0)
    imp_bps = int(rep.get("priceImpactBps") or rep.get("impactBps") or 0)

    boost = 1.0
    if ENTRY_MIN_LIQ_USD <= liq_usd <= ENTRY_MAX_LIQ_USD:
        if ENTRY_MIN_IMPACT_BPS <= imp_bps <= ENTRY_MAX_IMPACT_BPS:
            boost = 1.0
        elif imp_bps < ENTRY_MIN_IMPACT_BPS:
            boost = 1.20
        else:
            boost = 0.70

    sized = base_sol * boost
    return min(MAX_SOL_PER_TRADE, max(MIN_SOL_PER_TRADE, sized))

# =============================================================================
# Fallback autosizer (only used when sizer module is unavailable)
# =============================================================================
async def _autosize_by_wallet(owner: str) -> float:
    global _last_autosize_at, _cached_wallet_sol
    now = time.time()
    if (now - _last_autosize_at) >= AUTOSIZE_REFRESH_SEC or _cached_wallet_sol <= 0:
        _cached_wallet_sol = await _get_sol_balance(owner)
        _last_autosize_at = now

    base = _cached_wallet_sol * max(0.0, min(1.0, RISK_PCT_OF_WALLET))
    base = min(MAX_SOL_PER_TRADE, max(MIN_SOL_PER_TRADE, base))
    return _round_step(base, STEP_SOL)

# =============================================================================
# Internal Route Guard (used only if utils_pkg.routeguard is not present)
# =============================================================================
async def _internal_preflight(mint: str, spend_lamports: int) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """
    Returns (allow_buy, jupiter_buy_quote_or_None).
    - If ROUTE_REQUIRE_SELL is True → require a valid tiny sell route.
    - If buy quote fails and ROUTE_ALLOW_NO_ROUTE is False → block.
    """
    # 1) Try buy quote
    q_buy: Optional[Dict[str, Any]] = None
    try:
        q_buy = await get_buy_quote(mint=mint, lamports_in=spend_lamports)
        out_raw = int(q_buy.get("outAmount", 0))
        if out_raw <= 0:
            if not ROUTE_ALLOW_NO_ROUTE:
                return False, None
            log.event("ROUTE_NO_BUY_PATH", mint=mint, lamports_in=spend_lamports)
            q_buy = None
    except Exception as e:
        if not ROUTE_ALLOW_NO_ROUTE:
            log.event("ROUTE_BUY_QUOTE_FAIL", mint=mint, err=str(e))
            return False, None
        log.event("ROUTE_BUY_QUOTE_FAIL_ALLOWED", mint=mint, err=str(e))
        q_buy = None

    # 2) Require sell route?
    if ROUTE_REQUIRE_SELL:
        tiny_raw = 1_000
        try:
            q_sell = await get_sell_quote(mint=mint, amount_raw=tiny_raw)
            lam = int(q_sell.get("outAmount", 0) or q_sell.get("lamports_out", 0))
            if lam <= 0:
                return False, q_buy
        except Exception as e:
            log.event("ROUTE_SELL_QUOTE_FAIL", mint=mint, err=str(e))
            return False, q_buy

    return True, q_buy

# =============================================================================
# PumpPortal (Lightning) fallback helpers
# =============================================================================
class TemporaryPumpPortal(Exception):
    pass

def _pp_url_lightning() -> str:
    if not PP_API_KEY:
        raise RuntimeError("PF_PP_KEY missing.")
    return f"https://pumpportal.fun/api/trade?api-key={PP_API_KEY}"

def _pp_url_local() -> str:
    if not PP_API_KEY:
        raise RuntimeError("PF_PP_KEY missing.")
    return f"https://pumpportal.fun/api/trade-local?api-key={PP_API_KEY}"

def _pp_body(*, action: str, pool: str, payer: str, amount_sol: float, slippage_bps: int, priority_sol: float, mint: str) -> Dict[str, Any]:
    return {
        "action": action,
        "amount": float(amount_sol),
        "denominatedInSol": True,
        "slippage": float(slippage_bps) / 100.0,  # bps→%
        "priorityFee": float(priority_sol),
        "pool": pool,
        "publicKey": payer,
        "mint": str(mint),
    }

async def _pp_post_json(url: str, body: Dict[str, Any], timeout: int = 20) -> Dict[str, Any]:
    def _req():
        r = requests.post(url, json=body, timeout=timeout, allow_redirects=False)
        ctype = (r.headers.get("content-type") or "").split(";")[0].strip().lower()
        if r.status_code == 200 and ctype != "application/json":
            head = (r.text or "")[:200].replace("\n", " ")
            raise TemporaryPumpPortal(f"non-JSON 200 ({ctype}): {head or 'Bad Request'}")
        js = r.json()
        if r.status_code != 200:
            raise RuntimeError(f"{url} {r.status_code}: {js}")
        return js
    return await asyncio.to_thread(_req)

def _pp_errors(resp: Dict[str, Any]) -> List[str]:
    errs = resp.get("errors") or []
    if isinstance(errs, dict):
        errs = [str(errs)]
    return [str(e) for e in errs] if isinstance(errs, list) else ([str(errs)] if errs else [])

def _pp_candidates(raw_hint: str) -> List[Tuple[str, str]]:
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
            out.append((raw_hint.strip(), "as-raw"))
    return out

# =============================================================================
# Public: maybe_buy
# =============================================================================
async def maybe_buy(*, mint: str, ui: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """
    Main buy entry called by Strategy.
    Tries Jupiter (router) first; if buy route is missing but allowed, falls back to PumpPortal Lightning.
    Returns signature on success, or None if skipped/failed.
    """
    ui = ui or {}
    pool = (ui.get("pool") or "pump").strip().lower()
    symbol = (ui.get("symbol") or "").strip()
    raw_hint = (ui.get("rawMint") or ui.get("raw") or mint or "").strip()

    if pool not in PF_POOLS:
        log.event("BUY_SKIP_UNSUPPORTED_POOL", pool=pool, mint=mint)
        return None

    # Guardrails (global halt)
    if not buys_allowed():
        log.event("BUY_BLOCKED_GUARDRAILS")
        return None

    # Cooldowns
    global _last_buy_at_global
    now = time.time()
    if (now - _last_buy_at_global) < BUY_COOLDOWN_SEC:
        log.event("BUY_COOLDOWN_GLOBAL", secs_left=int(BUY_COOLDOWN_SEC - (now - _last_buy_at_global)))
        return None
    if mint in _last_no_route and not _cooldown_ok(mint, NOROUTE_COOLDOWN_SEC):
        log.event("BUY_SKIP_NOROUTE_COOLDOWN", mint=mint)
        return None

    # Identity & balance
    kp = load_keypair()
    owner = OWNER_FROM_ENV or get_pubkey_str(kp)
    bal = await _get_sol_balance(owner)
    if bal < MIN_SOL_BALANCE:
        log.event("BUY_BLOCK_LOW_BAL", owner=owner, bal_sol=bal, min_sol=MIN_SOL_BALANCE)
        return None

    # Momentum gate (cheap & safe; cache-aware)
    if not await _momentum_ok(mint):
        log.event("BUY_BLOCK_MOMENTUM", mint=mint)
        return None

    # Size ticket — prefer sizer.suggest_trade_sol(); fall back to wallet %
    if _auto_size_sol:
        try:
            spend_sol = float(_auto_size_sol())
        except Exception:
            spend_sol = MIN_SOL_PER_TRADE
    else:
        spend_sol = await _autosize_by_wallet(owner)

    # Liquidity-aware tweak
    spend_sol = await _liquidity_size_boost(mint, spend_sol)
    # Clamp hard to env rails
    spend_sol = min(MAX_SOL_PER_TRADE, max(MIN_SOL_PER_TRADE, spend_sol))
    lamports_in = int(spend_sol * 1_000_000_000)

    # >>> EARLY DRY-RUN EXIT (no quotes burned) <<<
    if IS_DRYRUN:
        log.event("DRYRUN_BUY_EXEC", mint=mint, pool=pool, spend_sol=spend_sol, out_ui=0.0)
        bump("buys_ok")
        _last_buy_at_global = time.time()
        return None

    # Route preflight — prefer external routeguard if present, else internal
    if rg_preflight:
        try:
            allow_buy, q_buy = await rg_preflight(mint=mint, lamports_in=lamports_in)  # type: ignore
        except Exception:
            allow_buy, q_buy = True, None
    else:
        allow_buy, q_buy = await _internal_preflight(mint, lamports_in)

    jupiter_path_allowed = allow_buy and q_buy is not None

    async with _inflight:
        # Preferred: Jupiter router
        if jupiter_path_allowed:
            try:
                ok2, sig = await buy_via_lightning(
                    mint=mint,
                    amount_sol=spend_sol,
                    slippage_bps=None,   # router/env decide
                    priority_sol=None,   # dynamic priority inside router
                )
                if ok2 and sig:
                    _last_buy_at_global = time.time()
                    bump("buys_ok")
                    log.event("BUY_EXEC_OK", mint=mint, pool=pool, spend_sol=spend_sol, sig=sig, symbol=symbol or "-")
                    return sig
                log.event("BUY_SWAP_FAIL", mint=mint)
            except Exception as e:
                log.event("BUY_EXEC_ERROR", mint=mint, error=str(e))

        # Fallback: PumpPortal Lightning (when allowed to buy without a Jupiter route)
        if not jupiter_path_allowed and PP_API_KEY:
            payer = owner
            attempts = _pp_candidates(raw_hint)
            for send_mint, note in attempts:
                try:
                    body = _pp_body(
                        action="buy",
                        pool=pool,
                        payer=payer,
                        amount_sol=spend_sol,
                        slippage_bps=JUP_SLIPPAGE_BPS,
                        priority_sol=PP_PRIORITY_SOL,
                        mint=send_mint,
                    )
                    resp = await _pp_post_json(_pp_url_lightning(), body)
                except TemporaryPumpPortal as te:
                    log.event("PP_TEMP", note=note, mint=send_mint, err=str(te))
                    continue
                except Exception as e:
                    log.event("PP_HTTP_ERR", note=note, mint=send_mint, err=str(e))
                    continue

                errs = _pp_errors(resp)
                if errs:
                    log.event("PP_ERRORS", note=note, mint=send_mint, errors=";".join(errs))
                    # Optional local fallback
                    if USE_LOCAL_FALLBK:
                        try:
                            resp2 = await _pp_post_json(_pp_url_local(), body)
                            errs2 = _pp_errors(resp2)
                            if not errs2:
                                sig2 = resp2.get("signature")
                                if sig2:
                                    _last_buy_at_global = time.time()
                                    bump("buys_ok")
                                    log.event("BUY_EXEC_OK_PP_LOCAL", mint=mint, pool=pool, spend_sol=spend_sol, sig=sig2)
                                    return sig2
                        except Exception as e2:
                            log.event("PP_LOCAL_ERR", note=note, mint=send_mint, err=str(e2))
                    continue

                sig = resp.get("signature")
                if sig:
                    _last_buy_at_global = time.time()
                    bump("buys_ok")
                    log.event("BUY_EXEC_OK_PP", mint=mint, pool=pool, spend_sol=spend_sol, sig=sig)
                    return sig

                # Rare: server returns unsigned transaction
                tx_b64 = resp.get("transaction") or resp.get("tx")
                if tx_b64:
                    try:
                        sig2 = await sign_and_send_b64_tx(tx_b64, kp, skip_preflight=True)
                        _last_buy_at_global = time.time()
                        bump("buys_ok")
                        log.event("BUY_EXEC_OK_PP_SIGN", mint=mint, pool=pool, spend_sol=spend_sol, sig=sig2)
                        return sig2
                    except Exception as e:
                        log.event("PP_SIGN_ERR", mint=mint, err=str(e))

            # Mark this mint as no-route cooled down
            _note_no_route(mint)

        bump("buys_fail")
        return None

# =============================================================================
# Mint resolvers (best-effort; used by exit/trader)
# =============================================================================
def _helius_key() -> Optional[str]:
    # try HELIUS_API_KEY or extract from SOL_RPC_URL ?api-key=
    key = (os.getenv("HELIUS_API_KEY") or "").strip()
    if key:
        return key
    sol_rpc = (os.getenv("SOL_RPC_URL") or "").strip()
    try:
        import urllib.parse as _u
        q = _u.urlparse(sol_rpc).query
        return _u.parse_qs(q).get("api-key", [None])[0]
    except Exception:
        return None

def resolve_true_mint_from_pump_id(pump_id: str) -> Optional[str]:
    """
    Given a PUMP ID (usually '<base>pump'), try to infer the real SPL mint.
    Uses Helius Enhanced Tx History on the base address (if available).
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
    """Pull the transaction via Helius Parse API and extract a token mint we interacted with."""
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
    """Try signature first (most precise), then pump-id base address; else accept plausible base58."""
    m = resolve_true_mint_from_signature(signature) if signature else None
    if m:
        return m
    m2 = resolve_true_mint_from_pump_id(pump_or_mint)
    if m2:
        return m2
    s, had = _strip_pump_suffix(pump_or_mint)
    if not had and _looks_base58(s):
        return s
    return None

__all__ = [
    "maybe_buy",
    "resolve_true_mint",
    "resolve_true_mint_from_pump_id",
    "resolve_true_mint_from_signature",
]
