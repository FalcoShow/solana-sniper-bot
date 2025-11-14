from __future__ import annotations

"""
utils_pkg/pump_trader.py — vNext (full rewrite)

Key features
------------
- Primary path: Jupiter lightning buy; fallbacks: PumpPortal remote → local, with retries.
- Buy hooks: on_buy_submitted / on_buy_filled / on_buy_failed (fully wired, resilient).
- Momentum gate + liquidity-aware sizing + wallet-risk autosizer + per-mint cooldowns.
- Global guardrails integration (buys_allowed) + optional external routeguard preflight.
- Dedupe support (optional): prevents duplicate buys per mint for a TTL.
- Jito / priority-fee knobs, slippage bps override, compute unit limit knob (optional).
- Optional confirmation wait for tx signature before reporting FILLED hook.
- Noroute cooldown: if a mint can’t route, back off per-mint to avoid thrashing.
- Extensive telemetry via logging_setup.get_logger().event + bump() counters.
- All optional deps handled via soft imports; safe in DRYRUN.
- NEW: Direct Pump.fun helpers:
    * buy_pump_token(mint, sol_amount, slippage_bps)
    * buy_direct_lamports(mint, in_amount_lamports, slippage_bps)
  for router’s Jupiter→Pump fallback.

Env toggles (selected)
----------------------
DRYRUN / DRY_RUN
PF_TRADE_RISK_PCT, PF_TRADE_MIN_SOL, PF_TRADE_MAX_SOL, PF_TRADE_STEP_SOL
PF_TRADE_AUTOSIZE_REFRESH_SEC
MIN_SOL_BALANCE
PF_SLIPPAGE_BPS
PF_PRIORITY_SOL, PF_JITO_TIP_SOL (alias), PF_COMPUTE_UNITS
PF_POOLS
PF_DEBUG_TX
MAX_CONCURRENT_BUYS, BUY_COOLDOWN_SEC (alias PF_COOLDOWN_SEC)
NOROUTE_COOLDOWN_SEC
ENTRY_* (momentum & liquidity windows)
TELEGRAM_ENABLED, TELEGRAM_NOTIFY_FILLED
DEDUPE_ENABLED, DEDUPE_TTL_SEC
CONFIRM_TIMEOUT_SEC, CONFIRM_COMMITMENT (processed/confirmed/finalized)
ROUTEGUARD_ALLOW_NO_ROUTE, ROUTEGUARD_REQUIRE_SELL
PF_USE_LOCAL_FALLBACK
"""

import asyncio
import math
import os
import time
from typing import Any, Dict, Optional, Tuple, List, Callable, Awaitable

import requests

from logging_setup import get_logger, bump

# ---------------------------
# Optional/primary router & helpers
# ---------------------------
try:
    from utils_pkg.router import (
        get_buy_quote,
        get_sell_quote,
        buy_via_lightning,
        get_price_ui,
    )
except Exception as e:  # pragma: no cover
    # Keep module importable; buys will fail gracefully.
    async def get_buy_quote(*_a, **_k):
        raise RuntimeError("router.get_buy_quote unavailable")

    async def get_sell_quote(*_a, **_k):
        raise RuntimeError("router.get_sell_quote unavailable")

    async def buy_via_lightning(*_a, **_k):
        return False, None

    async def get_price_ui(*_a, **_k):
        return 0.0

# Telegram notifier (optional)
try:
    from utils_pkg.telegram_utils import notify_trade_fill
except Exception:  # pragma: no cover

    async def notify_trade_fill(*_a, **_k):  # type: ignore
        return None

# Wallet helpers
try:
    from utils_pkg.solana_wallet import (
        load_keypair,
        get_pubkey_str,
        sign_and_send_b64_tx,
        get_balance_sol as _get_balance_sol,
        confirm_signature as _confirm_signature,  # optional
    )
except Exception as e:  # pragma: no cover
    raise RuntimeError(f"solana_wallet core helpers missing: {e}")

# Optional sizer
try:
    from utils_pkg.sizer import suggest_trade_sol as _auto_size_sol  # type: ignore
except Exception:  # pragma: no cover
    _auto_size_sol = None  # type: ignore

# Optional safety filters
try:
    from utils_pkg.liquidity import looks_like_mint, screen_token  # type: ignore
except Exception:  # pragma: no cover

    def looks_like_mint(_: str) -> bool:  # type: ignore
        return False

    async def screen_token(_: str):  # type: ignore
        return True, {"ok": True}

# Optional global guardrails & dedupe
try:
    from utils_pkg.guardrails import buys_allowed, record_buy_result  # type: ignore
except Exception:  # pragma: no cover

    def buys_allowed() -> bool:  # type: ignore
        return True

    def record_buy_result(*_a, **_k):  # type: ignore
        return None

try:
    from utils_pkg.dedupe import claim_once  # type: ignore
except Exception:  # pragma: no cover

    def claim_once(_key: str, _ttl: int = 60) -> bool:  # type: ignore
        return True

# Optional external routeguard
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
    if v == "":
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
IS_DRYRUN = _envb("DRYRUN", _envb("DRY_RUN", False))
OWNER_FROM_ENV = (os.getenv("PF_PP_ACCOUNT") or "").strip()
MIN_SOL_BALANCE = _envf("MIN_SOL_BALANCE", 0.20)

# Telegram
TELEGRAM_ENABLED = _envb("TELEGRAM_ENABLED", True)
TELEGRAM_NOTIFY_FILLED = _envb("TELEGRAM_NOTIFY_FILLED", True)

# Autosizing
AUTOSIZE_REFRESH_SEC = _envf("PF_TRADE_AUTOSIZE_REFRESH_SEC", 10.0)
RISK_PCT_OF_WALLET = _envf("PF_TRADE_RISK_PCT", 0.05)
MIN_SOL_PER_TRADE = _envf("PF_TRADE_MIN_SOL", 0.005)
MAX_SOL_PER_TRADE = _envf("PF_TRADE_MAX_SOL", 0.08)
STEP_SOL = _envf("PF_TRADE_STEP_SOL", 0.005)
if MIN_SOL_PER_TRADE > MAX_SOL_PER_TRADE:
    MIN_SOL_PER_TRADE, MAX_SOL_PER_TRADE = MAX_SOL_PER_TRADE, MIN_SOL_PER_TRADE

# Pacing/concurrency
BUY_COOLDOWN_SEC = _envi_alias(["BUY_COOLDOWN_SEC", "PF_COOLDOWN_SEC"], 120)
MAX_CONCURRENT_BUYS = max(1, _envi_alias(["MAX_CONCURRENT_BUYS", "PF_MAX_CONCURRENT"], 2))
NOROUTE_COOLDOWN_SEC = _envi("NOROUTE_COOLDOWN_SEC", 600)

# Route guard policies (internal fallback)
ROUTEGUARD_ALLOW_NO_ROUTE = _envb("ROUTEGUARD_ALLOW_NO_ROUTE", True)
ROUTEGUARD_REQUIRE_SELL = _envb("ROUTEGUARD_REQUIRE_SELL", True)

# Momentum
MOMO_ENABLED = _envb("ENTRY_MOMENTUM_ENABLED", True)
MOMO_MIN_UP_PCT = _envf("ENTRY_MOMO_MIN_UP_PCT", 5.0)
MOMO_WINDOW_SEC = _envf("ENTRY_MOMO_WINDOW_SEC", 10.0)
MOMO_PRICE_TRIES = _envi("ENTRY_MOMO_PRICE_TRIES", 4)
MOMO_PRICE_DELAY_S = _envf("ENTRY_MOMO_PRICE_DELAY_S", 0.7)
PRICE_TTL_SEC_ENV = _envf("PF_PRICE_TTL_SEC", 5.0)

# Liquidity screen
LIQ_FILTER_ENABLED = _envb("ENTRY_LIQ_FILTER", True)
ENTRY_MIN_LIQ_USD = _envf("ENTRY_MIN_LIQ_USD", 10_000)
ENTRY_MAX_LIQ_USD = _envf("ENTRY_MAX_LIQ_USD", 200_000)
ENTRY_MIN_IMPACT_BPS = _envi("ENTRY_MIN_IMPACT_BPS", 250)
ENTRY_MAX_IMPACT_BPS = _envi("ENTRY_MAX_IMPACT_BPS", 800)

# Pools / router
PP_API_KEY = (os.getenv("PF_PP_KEY") or "").strip()
JUP_SLIPPAGE_BPS = _envi("PF_SLIPPAGE_BPS", 600)
PF_PRIORITY_SOL = _envf("PF_PRIORITY_SOL", 0.002)  # also used for PP
PF_JITO_TIP_SOL = _envf("PF_JITO_TIP_SOL", PF_PRIORITY_SOL)  # alias for clarity
PF_COMPUTE_UNITS = _envi("PF_COMPUTE_UNITS", 0)  # 0 = router default
PF_POOLS = {p.strip().lower() for p in (os.getenv("PF_POOLS", "pump").split(","))}
PF_DEBUG_TX = _envb("PF_DEBUG_TX", False)
USE_LOCAL_FALLBK = _envb("PF_USE_LOCAL_FALLBACK", False)

# Dedupe & confirm config
DEDUPE_ENABLED = _envb("DEDUPE_ENABLED", True)
DEDUPE_TTL_SEC = _envi("DEDUPE_TTL_SEC", 600)
CONFIRM_TIMEOUT_SEC = _envi("CONFIRM_TIMEOUT_SEC", 25)
CONFIRM_COMMITMENT = (
    os.getenv("CONFIRM_COMMITMENT") or "confirmed"
).strip().lower()  # processed/confirmed/finalized

LAMPORTS_PER_SOL = 1_000_000_000
LAMPORTS_PER_SOL_F = float(LAMPORTS_PER_SOL)

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


def _strip_pseudo_suffix(raw: str) -> Tuple[str, Optional[str]]:
    s = (raw or "").strip()
    for suf in ("pump", "bonk"):
        if s.endswith(suf):
            return s[: -len(suf)].strip(), suf
    return s, None


def _is_pseudo_id(s: str) -> bool:
    return (s or "").strip().endswith(("pump", "bonk"))


async def _get_sol_balance(owner: str) -> float:
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
# Momentum gate
# =============================================================================
async def _momentum_ok(mint: str) -> bool:
    if not MOMO_ENABLED:
        return True
    if not looks_like_mint(mint):
        return True
    if PRICE_TTL_SEC_ENV >= MOMO_WINDOW_SEC * 0.9:
        # Cached prices are too stale relative to the decision window; skip gate.
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

    liq_usd = float(rep.get("liqUsd") or rep.get("liq_usd") or 0.0)
    imp_bps = int(
        rep.get("priceImpactBps")
        or rep.get("impactBps")
        or rep.get("impact_bps")
        or 0
    )

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
# Fallback autosizer
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
# Internal Route Guard (fallback)
# =============================================================================
async def _internal_preflight(
    mint: str, spend_lamports: int
) -> Tuple[bool, Optional[Dict[str, Any]]]:
    q_buy: Optional[Dict[str, Any]] = None
    try:
        q_buy = await get_buy_quote(mint=mint, lamports_in=spend_lamports)
        out_raw = int(q_buy.get("outAmount", 0))
        if out_raw <= 0:
            if not ROUTEGUARD_ALLOW_NO_ROUTE:
                return False, None
            log.event("ROUTE_NO_BUY_PATH", mint=mint, lamports_in=spend_lamports)
            q_buy = None
    except Exception as e:
        if not ROUTEGUARD_ALLOW_NO_ROUTE:
            log.event("ROUTE_BUY_QUOTE_FAIL", mint=mint, err=str(e))
            return False, None
        log.event("ROUTE_BUY_QUOTE_FAIL_ALLOWED", mint=mint, err=str(e))
        q_buy = None

    if ROUTEGUARD_REQUIRE_SELL:
        try:
            q_sell = await get_sell_quote(mint=mint, amount_raw=1_000)
            lam = int(q_sell.get("outAmount", 0) or q_sell.get("lamports_out", 0))
            if lam <= 0:
                return False, q_buy
        except Exception as e:
            log.event("ROUTE_SELL_QUOTE_FAIL", mint=mint, err=str(e))
            return False, q_buy

    return True, q_buy

# =============================================================================
# PumpPortal (Lightning) helpers
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


def _pp_body(
    *,
    action: str,
    pool: str,
    payer: str,
    amount_sol: float,
    slippage_bps: int,
    priority_sol: float,
    mint: str,
) -> Dict[str, Any]:
    return {
        "action": action,
        "amount": float(amount_sol),
        "denominatedInSol": True,
        "slippage": float(slippage_bps) / 100.0,  # PP expects % (bps/100)
        "priorityFee": float(priority_sol),
        "pool": pool,
        "publicKey": payer,
        "mint": str(mint),
    }


async def _pp_post_json(
    url: str, body: Dict[str, Any], timeout: int = 20
) -> Dict[str, Any]:
    def _req():
        r = requests.post(url, json=body, timeout=timeout, allow_redirects=False)
        ctype = (r.headers.get("content-type") or "").split(";")[
            0
        ].strip().lower()
        if r.status_code == 200 and ctype != "application/json":
            head = (r.text or "")[:200].replace("\n", " ")
            raise TemporaryPumpPortal(
                f"non-JSON 200 ({ctype}): {head or 'Bad Request'}"
            )
        js = r.json()
        if r.status_code != 200:
            raise RuntimeError(f"{url} {r.status_code}: {js}")
        return js

    return await asyncio.to_thread(_req)


def _pp_errors(resp: Dict[str, Any]) -> List[str]:
    errs = resp.get("errors") or []
    if isinstance(errs, dict):
        errs = [str(errs)]
    return [str(e) for e in errs] if isinstance(errs, list) else (
        [str(errs)] if errs else []
    )


def _pp_candidates(raw_hint: str) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    s = (raw_hint or "").strip()
    base, suf = _strip_pseudo_suffix(s)
    if suf in ("pump", "bonk"):
        out.append((s, f"as-{suf}-id"))
        if _looks_base58(base):
            out.append((base, f"as-base58-from-{suf}"))
    else:
        if _looks_base58(base):
            out.append((base, "as-base58"))
        else:
            out.append((s, "as-raw"))
    return out

# =============================================================================
# Hooks types & helpers
# =============================================================================
SubmittedHook = Optional[Callable[[Dict[str, Any]], Awaitable[None]]]
FilledHook = Optional[Callable[[Dict[str, Any]], Awaitable[None]]]
FailedHook = Optional[Callable[[Dict[str, Any]], Awaitable[None]]]


async def _fire_submitted(h: SubmittedHook, payload: Dict[str, Any]) -> None:
    if not h:
        return
    try:
        await h(payload)
    except Exception as e:
        log.warning("on_buy_submitted hook failed: %s", e)


async def _fire_filled(h: FilledHook, payload: Dict[str, Any]) -> None:
    if not h:
        return
    try:
        await h(payload)
    except Exception as e:
        log.warning("on_buy_filled hook failed: %s", e)


async def _fire_failed(h: FailedHook, payload: Dict[str, Any]) -> None:
    if not h:
        return
    try:
        await h(payload)
    except Exception as e:
        log.warning("on_buy_failed hook failed: %s", e)

# =============================================================================
# Buy execution helpers (Jupiter + optional confirm)
# =============================================================================
async def _jupiter_buy(
    *,
    mint: str,
    spend_sol: float,
    slippage_bps: Optional[int],
    jito_tip_sol: Optional[float],
    compute_units: Optional[int],
) -> Tuple[bool, Optional[str]]:
    """
    Calls router.buy_via_lightning; returns (ok, signature or None).
    """
    ok, sig = await buy_via_lightning(
        mint=mint,
        amount_sol=spend_sol,
        slippage_bps=slippage_bps,
        priority_sol=jito_tip_sol,
        compute_units=compute_units if (compute_units and compute_units > 0) else None,
    )
    return ok, sig


async def _maybe_confirm(sig: Optional[str]) -> Optional[str]:
    """
    Wait for confirmation if wallet helper is present and timeout > 0.
    """
    if not sig:
        return None
    if CONFIRM_TIMEOUT_SEC <= 0:
        return sig
    try:
        if _confirm_signature:  # type: ignore
            ok = await _confirm_signature(
                sig, timeout=CONFIRM_TIMEOUT_SEC, commitment=CONFIRM_COMMITMENT
            )
            if ok:
                return sig
            # If confirm fails within timeout, still return sig (it may settle).
            log.event("CONFIRM_TIMEOUT", sig=sig, commitment=CONFIRM_COMMITMENT)
            return sig
    except Exception as e:
        log.event("CONFIRM_ERROR", sig=sig, err=str(e))
    return sig

# =============================================================================
# Public: maybe_buy (strategy entry)
# =============================================================================
async def maybe_buy(
    *,
    mint: str,
    ui: Optional[Dict[str, Any]] = None,
    on_buy_submitted: SubmittedHook = None,
    on_buy_filled: FilledHook = None,
    on_buy_failed: FailedHook = None,
) -> Optional[str]:
    """
    Main buy entry called by Strategy.
    Tries Jupiter first for real SPL mints; if no route/pseudo id, falls back to PumpPortal.
    Returns signature on success, or None if skipped/failed.

    Hook payloads:

    on_buy_submitted: {
        mint, pool, symbol, spend_sol, route: "jupiter"|"pp_remote"|"pp_local",
        hint: str|None
    }

    on_buy_filled: {
        mint, signature, symbol, spend_sol, pool
    }

    on_buy_failed: {
        mint, pool, symbol, spend_sol, route, hint, reason, error
    }
    """
    ui = ui or {}
    pool = (ui.get("pool") or "pump").strip().lower()
    symbol = (ui.get("symbol") or "").strip()
    raw_hint = (ui.get("rawMint") or ui.get("raw") or mint or "").strip()

    if pool not in PF_POOLS:
        log.event("BUY_SKIP_UNSUPPORTED_POOL", pool=pool, mint=mint)
        return None

    if not buys_allowed():
        log.event("BUY_BLOCKED_GUARDRAILS")
        return None

    # Global cooldown / per-mint noroute cooldown
    global _last_buy_at_global
    now = time.time()
    if (now - _last_buy_at_global) < BUY_COOLDOWN_SEC:
        log.event(
            "BUY_COOLDOWN_GLOBAL",
            secs_left=int(BUY_COOLDOWN_SEC - (now - _last_buy_at_global)),
        )
        return None
    if mint in _last_no_route and not _cooldown_ok(mint, NOROUTE_COOLDOWN_SEC):
        log.event("BUY_SKIP_NOROUTE_COOLDOWN", mint=mint)
        return None

    # Dedupe (optional): prevent duplicate buys for this mint
    if DEDUPE_ENABLED:
        if not claim_once(f"buy:{mint}", ttl=DEDUPE_TTL_SEC):
            if PF_DEBUG_TX:
                print(
                    f".[dedupe] buy already claimed for {mint} — skip", flush=True
                )
            record_buy_result(ok=False, reason="buy_claimed_elsewhere")
            return None

    # Identity & balance
    kp = load_keypair()
    owner = OWNER_FROM_ENV or get_pubkey_str(kp)
    bal = await _get_sol_balance(owner)
    if bal < MIN_SOL_BALANCE:
        log.event(
            "BUY_BLOCK_LOW_BAL",
            owner=owner,
            bal_sol=bal,
            min_sol=MIN_SOL_BALANCE,
        )
        record_buy_result(ok=False, reason="low_balance")
        return None

    # Momentum gate
    if not await _momentum_ok(mint):
        log.event("BUY_BLOCK_MOMENTUM", mint=mint)
        record_buy_result(ok=False, reason="momo_reject")
        return None

    # Size ticket
    if _auto_size_sol:
        try:
            spend_sol = float(_auto_size_sol())
        except Exception:
            spend_sol = MIN_SOL_PER_TRADE
    else:
        spend_sol = await _autosize_by_wallet(owner)

    spend_sol = await _liquidity_size_boost(mint, spend_sol)
    spend_sol = min(MAX_SOL_PER_TRADE, max(MIN_SOL_PER_TRADE, spend_sol))
    lamports_in = int(spend_sol * LAMPORTS_PER_SOL)

    # DRYRUN: pretend success path for accounting; do not actually buy or fire hooks
    if IS_DRYRUN:
        log.event(
            "DRYRUN_BUY_EXEC",
            mint=mint,
            pool=pool,
            spend_sol=spend_sol,
            out_ui=0.0,
        )
        bump("buys_ok")
        record_buy_result(ok=False, reason="dryrun_skip")
        _last_buy_at_global = time.time()
        return None

    # Decide whether to skip Jupiter (pseudo ids etc.)
    skip_jupiter = _is_pseudo_id(mint) or not looks_like_mint(mint)

    allow_buy: bool = True
    q_buy: Optional[Dict[str, Any]] = None
    if not skip_jupiter:
        if rg_preflight:
            try:
                allow_buy, q_buy = await rg_preflight(  # type: ignore
                    mint=mint, lamports_in=lamports_in
                )
            except Exception:
                allow_buy, q_buy = True, None
        else:
            allow_buy, q_buy = await _internal_preflight(mint, lamports_in)

    jupiter_path_allowed = (not skip_jupiter) and allow_buy and (q_buy is not None)

    # Concurrency guard
    async with _inflight:
        # ------------------------------
        # Preferred path: Jupiter
        # ------------------------------
        if jupiter_path_allowed:
            await _fire_submitted(
                on_buy_submitted,
                {
                    "mint": mint,
                    "pool": pool,
                    "symbol": symbol,
                    "spend_sol": spend_sol,
                    "route": "jupiter",
                    "hint": None,
                },
            )
            try:
                # Slippage & priority knobs
                slippage_bps = JUP_SLIPPAGE_BPS if JUP_SLIPPAGE_BPS > 0 else None
                jito_tip = PF_JITO_TIP_SOL if PF_JITO_TIP_SOL > 0 else None
                compute_units = PF_COMPUTE_UNITS if PF_COMPUTE_UNITS > 0 else None

                # Small retry loop to ride transient router hiccups
                attempts = 2
                last_err = None
                sig: Optional[str] = None
                for i in range(attempts):
                    ok2, sig = await _jupiter_buy(
                        mint=mint,
                        spend_sol=spend_sol,
                        slippage_bps=slippage_bps,
                        jito_tip_sol=jito_tip,
                        compute_units=compute_units,
                    )
                    if ok2 and sig:
                        break
                    last_err = "router_fail"
                    await asyncio.sleep(0.6 * (i + 1))

                if sig:
                    sig = await _maybe_confirm(sig)
                    _last_buy_at_global = time.time()
                    bump("buys_ok")
                    record_buy_result(ok=True, reason="executed", sig=sig)
                    log.event(
                        "BUY_EXEC_OK",
                        mint=mint,
                        pool=pool,
                        spend_sol=spend_sol,
                        sig=sig,
                        symbol=symbol or "-",
                    )

                    # 🔔 Telegram fill (BUY)
                    if TELEGRAM_ENABLED and TELEGRAM_NOTIFY_FILLED:
                        try:
                            await notify_trade_fill(
                                symbol=symbol or "-",
                                side="BUY",
                                size_sol=spend_sol,
                                price=0.0,
                                tx=sig,
                            )
                        except Exception as e:
                            log.warning("telegram notify fill failed: %s", e)

                    await _fire_filled(
                        on_buy_filled,
                        {
                            "mint": mint,
                            "signature": sig,
                            "symbol": symbol,
                            "spend_sol": spend_sol,
                            "pool": pool,
                        },
                    )
                    return sig

                # Jupiter failed → fall through to PP
                log.event(
                    "BUY_SWAP_FAIL",
                    mint=mint,
                    err=last_err or "router_fail",
                )
                await _fire_failed(
                    on_buy_failed,
                    {
                        "mint": mint,
                        "pool": pool,
                        "symbol": symbol,
                        "spend_sol": spend_sol,
                        "route": "jupiter",
                        "hint": None,
                        "reason": "router_fail",
                        "error": last_err,
                    },
                )
            except Exception as e:
                log.event("BUY_EXEC_ERROR", mint=mint, error=str(e))
                await _fire_failed(
                    on_buy_failed,
                    {
                        "mint": mint,
                        "pool": pool,
                        "symbol": symbol,
                        "spend_sol": spend_sol,
                        "route": "jupiter",
                        "hint": None,
                        "reason": "exception",
                        "error": str(e),
                    },
                )

        # ------------------------------
        # Fallback path(s): PumpPortal
        # ------------------------------
        if (not jupiter_path_allowed) and PP_API_KEY:
            payer = owner
            attempts = _pp_candidates(raw_hint)
            last_err: Optional[str] = None

            for send_mint, note in attempts:
                await _fire_submitted(
                    on_buy_submitted,
                    {
                        "mint": mint,
                        "pool": pool,
                        "symbol": symbol,
                        "spend_sol": spend_sol,
                        "route": "pp_remote",
                        "hint": note,
                    },
                )
                try:
                    body = _pp_body(
                        action="buy",
                        pool=pool,
                        payer=payer,
                        amount_sol=spend_sol,
                        slippage_bps=JUP_SLIPPAGE_BPS,
                        priority_sol=PF_PRIORITY_SOL,
                        mint=send_mint,
                    )
                    resp = await _pp_post_json(_pp_url_lightning(), body)
                except TemporaryPumpPortal as te:
                    last_err = f"temp:{te}"
                    log.event(
                        "PP_TEMP", note=note, mint=send_mint, err=str(te)
                    )
                    continue
                except Exception as e:
                    last_err = str(e)
                    log.event(
                        "PP_HTTP_ERR", note=note, mint=send_mint, err=str(e)
                    )
                    continue

                errs = _pp_errors(resp)
                if errs:
                    last_err = ";".join(errs)
                    log.event(
                        "PP_ERRORS", note=note, mint=send_mint, errors=last_err
                    )

                    # Optional local fallback
                    if USE_LOCAL_FALLBK:
                        await _fire_submitted(
                            on_buy_submitted,
                            {
                                "mint": mint,
                                "pool": pool,
                                "symbol": symbol,
                                "spend_sol": spend_sol,
                                "route": "pp_local",
                                "hint": note,
                            },
                        )
                        try:
                            resp2 = await _pp_post_json(_pp_url_local(), body)
                            errs2 = _pp_errors(resp2)
                            if not errs2:
                                sig2 = resp2.get("signature")
                                if sig2:
                                    sig2 = await _maybe_confirm(sig2)
                                    _last_buy_at_global = time.time()
                                    bump("buys_ok")
                                    record_buy_result(
                                        ok=True,
                                        reason="executed_pp_local",
                                        sig=sig2,
                                    )
                                    log.event(
                                        "BUY_EXEC_OK_PP_LOCAL",
                                        mint=mint,
                                        pool=pool,
                                        spend_sol=spend_sol,
                                        sig=sig2,
                                    )

                                    if (
                                        TELEGRAM_ENABLED
                                        and TELEGRAM_NOTIFY_FILLED
                                    ):
                                        try:
                                            await notify_trade_fill(
                                                symbol=symbol or "-",
                                                side="BUY",
                                                size_sol=spend_sol,
                                                price=0.0,
                                                tx=sig2,
                                            )
                                        except Exception as e:
                                            log.warning(
                                                "telegram notify fill failed: %s",
                                                e,
                                            )

                                    await _fire_filled(
                                        on_buy_filled,
                                        {
                                            "mint": mint,
                                            "signature": sig2,
                                            "symbol": symbol,
                                            "spend_sol": spend_sol,
                                            "pool": pool,
                                        },
                                    )
                                    return sig2
                            else:
                                last_err = ";".join(errs2)
                                log.event(
                                    "PP_LOCAL_ERRORS",
                                    note=note,
                                    mint=send_mint,
                                    errors=last_err,
                                )
                        except Exception as e2:
                            last_err = str(e2)
                            log.event(
                                "PP_LOCAL_ERR",
                                note=note,
                                mint=send_mint,
                                err=str(e2),
                            )
                    continue

                # Remote PP success
                sig = resp.get("signature")
                if sig:
                    sig = await _maybe_confirm(sig)
                    _last_buy_at_global = time.time()
                    bump("buys_ok")
                    record_buy_result(
                        ok=True, reason="executed_pp", sig=sig
                    )
                    log.event(
                        "BUY_EXEC_OK_PP",
                        mint=mint,
                        pool=pool,
                        spend_sol=spend_sol,
                        sig=sig,
                    )

                    if TELEGRAM_ENABLED and TELEGRAM_NOTIFY_FILLED:
                        try:
                            await notify_trade_fill(
                                symbol=symbol or "-",
                                side="BUY",
                                size_sol=spend_sol,
                                price=0.0,
                                tx=sig,
                            )
                        except Exception as e:
                            log.warning(
                                "telegram notify fill failed: %s", e
                            )

                    await _fire_filled(
                        on_buy_filled,
                        {
                            "mint": mint,
                            "signature": sig,
                            "symbol": symbol,
                            "spend_sol": spend_sol,
                            "pool": pool,
                        },
                    )
                    return sig

                # Rare: server returns unsigned transaction → sign locally
                tx_b64 = resp.get("transaction") or resp.get("tx")
                if tx_b64:
                    try:
                        sig2 = await sign_and_send_b64_tx(
                            tx_b64, kp, skip_preflight=True
                        )
                        sig2 = await _maybe_confirm(sig2)
                        _last_buy_at_global = time.time()
                        bump("buys_ok")
                        record_buy_result(
                            ok=True,
                            reason="executed_pp_sign",
                            sig=sig2,
                        )
                        log.event(
                            "BUY_EXEC_OK_PP_SIGN",
                            mint=mint,
                            pool=pool,
                            spend_sol=spend_sol,
                            sig=sig2,
                        )

                        if TELEGRAM_ENABLED and TELEGRAM_NOTIFY_FILLED:
                            try:
                                await notify_trade_fill(
                                    symbol=symbol or "-",
                                    side="BUY",
                                    size_sol=spend_sol,
                                    price=0.0,
                                    tx=sig2,
                                )
                            except Exception as e:
                                log.warning(
                                    "telegram notify fill failed: %s", e
                                )

                        await _fire_filled(
                            on_buy_filled,
                            {
                                "mint": mint,
                                "signature": sig2,
                                "symbol": symbol,
                                "spend_sol": spend_sol,
                                "pool": pool,
                            },
                        )
                        return sig2
                    except Exception as e:
                        last_err = str(e)
                        log.event("PP_SIGN_ERR", mint=mint, err=str(e))

            # No PP route worked → set per-mint cooldown and emit failed
            _note_no_route(mint)
            record_buy_result(ok=False, reason="no_route", error=last_err)
            await _fire_failed(
                on_buy_failed,
                {
                    "mint": mint,
                    "pool": pool,
                    "symbol": symbol,
                    "spend_sol": spend_sol,
                    "route": "pp_remote",
                    "hint": raw_hint,
                    "reason": "no_route",
                    "error": last_err,
                },
            )

        bump("buys_fail")
        record_buy_result(ok=False, reason="end_of_paths")
        return None

# =============================================================================
# NEW: Direct Pump.fun helpers for router fallback
# =============================================================================
async def buy_pump_token(
    *,
    mint: str,
    sol_amount: float,
    slippage_bps: int,
    pool: str = "pump",
) -> Dict[str, Any]:
    """
    Direct Pump.fun bonding-curve buy used by router’s Pump fallback.

    - No momentum/liquidity/guardrail logic here (that’s in strategy/maybe_buy).
    - Talks to PumpPortal remote, optionally local.
    - Returns a dict: { ok, txid, signature, mint, route, error? }
    """
    if sol_amount <= 0:
        return {
            "ok": False,
            "error": "sol_amount must be > 0",
            "mint": mint,
        }
    if not PP_API_KEY:
        return {
            "ok": False,
            "error": "PF_PP_KEY missing",
            "mint": mint,
        }

    kp = load_keypair()
    payer = OWNER_FROM_ENV or get_pubkey_str(kp)
    body = _pp_body(
        action="buy",
        pool=pool,
        payer=payer,
        amount_sol=sol_amount,
        slippage_bps=slippage_bps,
        priority_sol=PF_PRIORITY_SOL,
        mint=mint,
    )

    last_err: Optional[str] = None

    # 1) Remote PumpPortal
    try:
        resp = await _pp_post_json(_pp_url_lightning(), body)
    except TemporaryPumpPortal as te:
        last_err = f"temp:{te}"
        log.event("PUMP_DIRECT_TEMP", mint=mint, err=str(te))
        resp = {}
    except Exception as e:
        last_err = str(e)
        log.event("PUMP_DIRECT_HTTP_ERR", mint=mint, err=str(e))
        resp = {}

    errs = _pp_errors(resp) if resp else []
    if errs:
        last_err = ";".join(errs)
        log.event("PUMP_DIRECT_ERRORS", mint=mint, errors=last_err)

    # 1a) Local fallback if configured and remote errored
    if (not resp or errs) and USE_LOCAL_FALLBK:
        try:
            resp2 = await _pp_post_json(_pp_url_local(), body)
            errs2 = _pp_errors(resp2)
            if not errs2:
                sig2 = resp2.get("signature")
                if sig2:
                    return {
                        "ok": True,
                        "txid": str(sig2),
                        "signature": str(sig2),
                        "mint": mint,
                        "route": "pp_local",
                    }
                tx_b64 = resp2.get("transaction") or resp2.get("tx")
                if tx_b64:
                    sig3 = await sign_and_send_b64_tx(
                        tx_b64, kp, skip_preflight=True
                    )
                    return {
                        "ok": True,
                        "txid": str(sig3),
                        "signature": str(sig3),
                        "mint": mint,
                        "route": "pp_local_sign",
                    }
            else:
                last_err = ";".join(errs2)
                log.event("PUMP_DIRECT_LOCAL_ERRORS", mint=mint, errors=last_err)
        except Exception as e2:
            last_err = str(e2)
            log.event("PUMP_DIRECT_LOCAL_ERR", mint=mint, err=str(e2))

    # 2) Remote success cases
    if resp and not errs:
        sig = resp.get("signature")
        if sig:
            return {
                "ok": True,
                "txid": str(sig),
                "signature": str(sig),
                "mint": mint,
                "route": "pp_remote",
            }
        tx_b64 = resp.get("transaction") or resp.get("tx")
        if tx_b64:
            try:
                sig2 = await sign_and_send_b64_tx(
                    tx_b64, kp, skip_preflight=True
                )
                return {
                    "ok": True,
                    "txid": str(sig2),
                    "signature": str(sig2),
                    "mint": mint,
                    "route": "pp_remote_sign",
                }
            except Exception as e:
                last_err = str(e)
                log.event("PUMP_DIRECT_SIGN_ERR", mint=mint, err=str(e))

    return {
        "ok": False,
        "error": last_err or "pump_direct_failed",
        "mint": mint,
    }


async def buy_direct_lamports(
    *,
    mint: str,
    in_amount_lamports: int,
    slippage_bps: int,
    pool: str = "pump",
) -> Dict[str, Any]:
    """
    Thin lamports-based wrapper for router.

    - Converts lamports → SOL and calls buy_pump_token.
    - Returns the same dict shape as buy_pump_token.
    """
    if in_amount_lamports <= 0:
        return {
            "ok": False,
            "error": "in_amount_lamports must be > 0",
            "mint": mint,
        }
    sol_amount = in_amount_lamports / LAMPORTS_PER_SOL_F
    return await buy_pump_token(
        mint=mint,
        sol_amount=sol_amount,
        slippage_bps=slippage_bps,
        pool=pool,
    )

# =============================================================================
# Mint resolvers (unchanged behavior, hardened)
# =============================================================================
def _helius_key() -> Optional[str]:
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


def resolve_true_mint_from_pseudo_id(pid: str) -> Optional[str]:
    key = _helius_key()
    base, suf = _strip_pseudo_suffix(pid)
    if not (key and suf and _looks_base58(base)):
        return None
    try:
        url = (
            f"https://api.helius.xyz/v0/addresses/{base}/transactions"
            f"?api-key={key}&limit=25"
        )
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
                for k in (
                    "mint",
                    "mintAddress",
                    "mint_address",
                    "tokenMint",
                    "token_mint",
                ):
                    m = (ev.get(k) or "").strip()
                    if _looks_base58(m):
                        return m
    return None


def resolve_true_mint_from_signature(sig: str) -> Optional[str]:
    key = _helius_key()
    if not (key and sig):
        return None
    try:
        url = f"https://api.helius.xyz/v0/transactions?api-key={key}"
        r = requests.post(
            url,
            json={"transactions": [sig]},
            timeout=8,
        )
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
                for k in (
                    "mint",
                    "mintAddress",
                    "mint_address",
                    "tokenMint",
                    "token_mint",
                ):
                    m = (ev.get(k) or "").strip()
                    if _looks_base58(m):
                        return m
    return None


def resolve_true_mint(
    pump_or_mint: str, *, signature: Optional[str] = None
) -> Optional[str]:
    m = resolve_true_mint_from_signature(signature) if signature else None
    if m:
        return m
    m2 = resolve_true_mint_from_pseudo_id(pump_or_mint)
    if m2:
        return m2
    base, suf = _strip_pseudo_suffix(pump_or_mint)
    if (suf is None) and _looks_base58(base):
        return base
    return None

__all__ = [
    "maybe_buy",
    "buy_pump_token",
    "buy_direct_lamports",
    "resolve_true_mint",
    "resolve_true_mint_from_pseudo_id",
    "resolve_true_mint_from_signature",
]
