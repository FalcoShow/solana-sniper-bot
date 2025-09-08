# sniper/sniper.py — v8.0
# Max-PnL preset · rank+select · priority fees · exits wiring · DB logs · CF refresher

from __future__ import annotations

import os
import asyncio
import logging
import random
import inspect
import importlib
from typing import Dict, Any, List, Callable, Optional, Tuple
from datetime import datetime, timezone

# ==============================================================================
# Logging
# ==============================================================================
_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, _LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
)
log = logging.getLogger("sniper")

# ==============================================================================
# DB schema + DB logger (safe wrappers)
# ==============================================================================
try:
    from db.schema import ensure_schema
except Exception as e:
    log.warning("db.schema import failed: %s", e)

    async def ensure_schema():
        ...

try:
    from db.logger import (
        log_scan_batch,
        log_scan_row,
        log_trade_event,
        log_error_event,
        log_pnl_snapshot,
    )
except Exception as e:
    log.warning("db.logger import failed: %s", e)

    async def log_scan_batch(*_a, **_k): ...
    async def log_scan_row(*_a, **_k): ...
    async def log_trade_event(*_a, **_k): ...
    async def log_error_event(*_a, **_k): ...
    async def log_pnl_snapshot(*_a, **_k): ...

# ==============================================================================
# Core utils (session/redis + wallet + jupiter + telegram + notion)
# ==============================================================================
_session_ref = None
_redis_ref = None

def get_session():
    return _session_ref

def get_redis():
    return _redis_ref

try:
    # Preferred: live objects exposed as names
    from utils_pkg.core_utils import (
        init_resources, close_resources,
        session as _session_ref, redis as _redis_ref,
        get_sol_balance, get_token_price, swap_token_jupiter,
        save_entry_price, convert_sol_to_usd, send_telegram,
        log_to_notion,
    )
except Exception as e_a:
    try:
        # Accessor shape
        from utils_pkg.core_utils import (
            init_resources, close_resources,
            get_session as _get_session, get_redis as _get_redis,
            get_sol_balance, get_token_price, swap_token_jupiter,
            save_entry_price, convert_sol_to_usd, send_telegram,
            log_to_notion,
        )
        _session_ref = _get_session()
        _redis_ref = _get_redis()
    except Exception as e_b:
        log.warning("core_utils import failed: %s | %s", e_a, e_b)

        async def init_resources(): ...
        async def close_resources(): ...
        async def get_sol_balance(_): return 0.0
        async def get_token_price(_): return 0.0  # returns SOL/token (our convention here)
        async def swap_token_jupiter(**_): return (False, "")
        async def save_entry_price(*_a, **_k): ...
        async def convert_sol_to_usd(_x: float): return 0.0
        async def send_telegram(_): ...
        async def log_to_notion(*_a, **_k): ...

# ==============================================================================
# Cloudflare / Playwright cookie refresher (safe)
# ==============================================================================
ensure_cookie_refresher = None
cf_refresh_loop = None
try:
    import utils_pkg.pumpfun_cf as _pump_cf
    ensure_cookie_refresher = getattr(_pump_cf, "ensure_cookie_refresher", None)
    cf_refresh_loop = getattr(_pump_cf, "refresh_loop", None)
    log.info("pumpfun_cf loaded: ensure=%s loop=%s",
             bool(ensure_cookie_refresher), bool(cf_refresh_loop))
except Exception as e:
    log.warning("pumpfun_cf import failed: %s", e)

# ==============================================================================
# Risk manager / entry gate / quote cache / route guard (safe)
# ==============================================================================
try:
    from risk_manager import current_limits, should_allow_new_entry, note_spend
except Exception as e:
    log.warning("risk_manager import fallback: %s", e)

    async def current_limits(_):
        pct = float(os.getenv("TICKET_PCT_EQUITY", "0.03"))  # 3% equity default
        cap = float(os.getenv("TICKET_USD_CAP", "250"))
        return {"ticket_usd_pct": pct, "ticket_cap_usd": cap, "ticket_usd": 50.0}

    async def should_allow_new_entry(_):
        return True

    async def note_spend(*_a, **_k): ...

try:
    from entry_gate import try_acquire, release
except Exception:
    async def try_acquire(_k): return True
    async def release(_k): ...

try:
    from jup_quote_cache import prewarm_quotes
except Exception:
    async def prewarm_quotes(*_a, **_k): ...

try:
    from route_guard import precheck_buy, posttrade_watchdog
except Exception:
    async def precheck_buy(_mint, _sol_to_spend, _slip_bps):
        return True, "ok", {"hops": 1, "impact_bps": 0, "out_amount": 0}

    async def posttrade_watchdog(_mint, _sol_to_spend, _slip_bps, pre_meta):
        return True, "ok", {"hops": pre_meta.get("hops", 1), "out_amount": pre_meta.get("out_amount", 0)}

# ==============================================================================
# Alpha scoring / params / fees
# ==============================================================================
alpha_score_fn: Optional[Callable[[Dict[str, Any]], float]] = None
try:
    from core.alpha import score as alpha_score_fn  # type: ignore
except Exception:
    def alpha_score_fn(c: Dict[str, Any]) -> float:
        age = float(c.get("pool_age_min") or c.get("ageMin") or 9999)
        liq = float(c.get("liq_usd") or c.get("liquidity_usd") or c.get("liquidityUSD") or 0)
        imp = float(c.get("impact_1k_bps") or c.get("priceImpact1kBps") or 0)
        vol5 = float(c.get("vol5m_pct") or 0)
        score = 0.0
        if age < 60: score += (60 - age) * 0.5
        score += min(liq, 80_000) / 8_000.0
        score += max(0, 200 - abs(imp)) / 50.0
        score += vol5 * 0.25
        return float(score)

try:
    from execution.params import get_ticket_size_usd  # type: ignore
except Exception:
    def get_ticket_size_usd(equity_usd: float) -> float:
        pct = float(os.getenv("TICKET_PCT_EQUITY", "0.04"))  # slightly aggressive
        cap = float(os.getenv("TICKET_USD_CAP", "300"))
        base = equity_usd * pct
        return float(min(cap, max(10.0, base)))

try:
    from execution.fees import compute_priority_tip  # type: ignore
except Exception:
    def compute_priority_tip(sol_to_spend: float) -> Tuple[int, Optional[int]]:
        if sol_to_spend >= 1.0:
            return (10_000, 50_000)    # cu price (µ-lamports), jito tip (lamports)
        if sol_to_spend >= 0.25:
            return (6_000, 25_000)
        return (3_000, 10_000)

# ==============================================================================
# Exit supervisor API (wire via exits_wiring)
# ==============================================================================
try:
    # start_exit_supervisor(loop: asyncio.AbstractEventLoop)
    # register_entry(signature, mint, qty, entry_price, symbol, usd_spent, sell_fn, price_fn)
    from execution.exits_wiring import start_exit_supervisor, register_entry  # type: ignore
except Exception as e:
    log.warning("exits_wiring import fallback: %s", e)

    def start_exit_supervisor(_loop): ...
    def register_entry(**_): ...

# Default sell/price helpers (optional)
try:
    # optional helpers you created in execution/sell_fn.py
    from execution.sell_fn import default_sell_fn, default_price_fn  # type: ignore
except Exception:
    default_sell_fn = None
    default_price_fn = None

# ==============================================================================
# AI filters / fraud / honeypot (safe)
# ==============================================================================
try:
    from utils_pkg.ai_buy_scoring import Candidate, pass_gate
except Exception:
    Candidate = None
    def pass_gate(*_a, **_k): return True, 1.0, "no_ai"

try:
    from utils_pkg.ai_sell_logic import ai_fraud_score
except Exception:
    async def ai_fraud_score(_m): return 0.0

try:
    from utils_pkg.honeypot import honeypot_check
except Exception:
    async def honeypot_check(_m): return (True, {})

# ==============================================================================
# Scanners (flat or package)
# ==============================================================================
scan_dexscreener = scan_pumpfun = scan_raydium = scan_axiom = None

try:
    from dexscreener_scanner import scan_dexscreener  # type: ignore
except Exception as e:
    log.debug("dexscreener_scanner flat import failed: %s", e)
try:
    from pumpfun_scanner import scan_pumpfun  # type: ignore
except Exception as e:
    log.debug("pumpfun_scanner flat import failed: %s", e)
try:
    from raydium_scanner import scan_raydium  # type: ignore
except Exception as e:
    log.debug("raydium_scanner flat import failed: %s", e)
try:
    from axiom_scanner import scan_axiom  # type: ignore
except Exception as e:
    log.debug("axiom_scanner flat import failed: %s", e)

if scan_dexscreener is None or scan_pumpfun is None or scan_raydium is None or scan_axiom is None:
    try:
        if scan_dexscreener is None:
            from scanner.dexscreener_scanner import scan_dexscreener  # type: ignore
        if scan_pumpfun is None:
            from scanner.pumpfun_scanner import scan_pumpfun  # type: ignore
        if scan_raydium is None:
            from scanner.raydium_scanner import scan_raydium  # type: ignore
        if scan_axiom is None:
            from scanner.axiom_scanner import scan_axiom  # type: ignore
    except Exception as e:
        log.warning("scanner package import failed: %s", e)

# ==============================================================================
# Env/config & presets
# ==============================================================================
SOL_MINT = "So11111111111111111111111111111111111111112"
WALLET_PUBKEY = os.getenv("WALLET_PUBKEY") or os.getenv("WALLET_PUBLIC_KEY")

PAUSE_TRADING = os.getenv("PAUSE_TRADING", "false").lower() == "true"
DRY_RUN       = os.getenv("DRY_RUN", "false").lower() == "true"
AXIOM_SCAN_ENABLED = os.getenv("AXIOM_SCAN_ENABLED", "true").lower() == "true"

SCANNER_PERIOD_SEC   = int(os.getenv("SCANNER_PERIOD_SEC", "15"))
BUY_COOLDOWN_SEC     = int(os.getenv("BUY_COOLDOWN_SEC", "120"))
MAX_CONCURRENT_BUYS  = int(os.getenv("MAX_CONCURRENT_BUYS", "2"))
MIN_SOL_BALANCE      = float(os.getenv("MIN_SOL_BALANCE", "0.2"))
JUPITER_SLIPPAGE     = float(os.getenv("JUPITER_MAX_SLIPPAGE", "0.005"))
SLIPPAGE_BPS         = max(1, int(JUPITER_SLIPPAGE * 10000))
PREWARM_TOP_N        = int(os.getenv("PREWARM_TOP_N", "8"))
TELEGRAM_NOTIFY_ROUTE_BLOCKS = os.getenv("TELEGRAM_NOTIFY_ROUTE_BLOCKS", "false").lower() == "true"
NOROUTE_COOLDOWN_SEC  = int(os.getenv("NOROUTE_COOLDOWN_SEC", "600"))  # 10 minutes default

PRESET = os.getenv("STRATEGY_PRESET", "max-pnl").lower()
if PRESET not in ("max-pnl", "balanced", "conservative"):
    PRESET = "max-pnl"

BLOCKLIST = {x.strip() for x in os.getenv("BLOCKLIST_MINTS", "").split(",") if x.strip()}
ALLOWLIST = {x.strip() for x in os.getenv("ALLOWLIST_MINTS", "").split(",") if x.strip()}

HALT_KEYS = ("halt:manual", "halt:infra", "infra:halt", "halt:daily_loss")

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

# ==============================================================================
# Halt & cooldown
# ==============================================================================
async def _is_halted() -> bool:
    if PAUSE_TRADING:
        return True
    r = get_redis()
    if not r:
        return False
    try:
        vals = await asyncio.gather(*[r.get(k) for k in HALT_KEYS])
        halted = any(v and str(v) not in ("0", "", "None") for v in vals)
        if halted:
            log.info("halt: active flags -> %s", {k: v for k, v in zip(HALT_KEYS, vals)})
        return halted
    except Exception as e:
        log.debug("halt check error: %s", e)
        return False

async def _cooldown_key(key: str, ttl: int) -> bool:
    r = get_redis()
    if not r:
        return False
    try:
        v = await r.get(key)
        if v:
            return True
        await r.set(key, "1", ex=ttl)
        return False
    except Exception as e:
        log.debug("cooldown set failed: %s", e)
        return False

async def _get_flag(key: str) -> bool:
    r = get_redis()
    if not r:
        return False
    try:
        return bool(await r.get(key))
    except Exception:
        return False

async def _set_flag(key: str, ttl: int):
    r = get_redis()
    if not r:
        return
    try:
        await r.set(key, "1", ex=ttl)
    except Exception:
        pass
# ==============================================================================
# Moonshot budget (optional)
# ==============================================================================
MOONSHOT_ENABLED           = os.getenv("MOONSHOT_ENABLED", "true").lower() == "true"
MOONSHOT_DAILY_USD         = float(os.getenv("MOONSHOT_DAILY_USD", "100"))
MOONSHOT_MAX_TRADE_USD     = float(os.getenv("MOONSHOT_MAX_TRADE_USD", "25"))
MOONSHOT_AGE_CUTOFF_MIN    = int(os.getenv("MOONSHOT_AGE_CUTOFF_MIN", "30"))
MOONSHOT_LIQ_CUTOFF_USD    = float(os.getenv("MOONSHOT_LIQ_CUTOFF_USD", "20000"))
MOONSHOT_IMPACT_CUTOFF_BPS = float(os.getenv("MOONSHOT_IMPACT_CUTOFF_BPS", "150"))

def _is_moonshot_like(c: Dict[str, Any]) -> bool:
    try:
        age = c.get("pool_age_min") or c.get("poolAgeMin") or c.get("ageMin")
        if age is not None and float(age) < MOONSHOT_AGE_CUTOFF_MIN:
            return True
    except Exception:
        pass
    try:
        liq = c.get("liq_usd") or c.get("liquidity_usd") or c.get("liquidityUSD")
        if liq is not None and float(liq) < MOONSHOT_LIQ_CUTOFF_USD:
            return True
    except Exception:
        pass
    try:
        imp = c.get("impact_1k_bps") or c.get("priceImpact1kBps")
        if imp is not None and float(imp) > MOONSHOT_IMPACT_CUTOFF_BPS:
            return True
    except Exception:
        pass
    return False

def _today_tag() -> str:
    t = _now_utc()
    return f"{t.year:04d}{t.month:02d}{t.day:02d}"

async def _moonshot_spent_today() -> float:
    r = get_redis()
    if not r:
        return 0.0
    try:
        v = await r.get(f"moonshot:spent:{_today_tag()}")
        return float(v) if v else 0.0
    except Exception:
        return 0.0

async def _moonshot_add_spent(usd: float) -> None:
    r = get_redis()
    if not r:
        return
    try:
        k = f"moonshot:spent:{_today_tag()}"
        await r.incrbyfloat(k, float(max(0.0, usd)))
        await r.expire(k, 26 * 3600)
    except Exception:
        pass

async def _apply_moonshot_budget(c: Dict[str, Any], ticket_usd: float):
    if not MOONSHOT_ENABLED or not _is_moonshot_like(c):
        return True, ticket_usd, ""
    note = "moonshot"
    if ticket_usd > MOONSHOT_MAX_TRADE_USD:
        ticket_usd = MOONSHOT_MAX_TRADE_USD
        note += f" | trade capped ${MOONSHOT_MAX_TRADE_USD:.2f}"
    spent = await _moonshot_spent_today()
    if spent + ticket_usd > MOONSHOT_DAILY_USD:
        return False, ticket_usd, note + f" | daily cap ${MOONSHOT_DAILY_USD:.2f}"
    await _moonshot_add_spent(ticket_usd)
    return True, ticket_usd, note

# ==============================================================================
# Scan plumbing
# ==============================================================================
async def _collect_scan(result) -> List[Dict[str, Any]]:
    if hasattr(result, "__aiter__"):
        out: List[Dict[str, Any]] = []
        async for item in result:
            if isinstance(item, dict):
                out.append(item)
        return out
    if asyncio.iscoroutine(result):
        try:
            return await _collect_scan(await result)
        except Exception:
            return []
    if isinstance(result, list):
        return [x for x in result if isinstance(x, dict)]
    return []

async def _maybe_call_scanner(func: Optional[Callable], name: str) -> List[Dict[str, Any]]:
    if func is None:
        log.debug("scanner.%s not available", name)
        return []
    for arity in (2, 1, 0):
        try:
            if arity == 2:
                items = await _collect_scan(func(get_redis(), get_session()))  # type: ignore
            elif arity == 1:
                items = await _collect_scan(func(get_redis()))  # type: ignore
            else:
                items = await _collect_scan(func())  # type: ignore
            log.info("scanner.%s: %d candidates", name, len(items))
            return items
        except TypeError:
            continue
        except Exception as e:
            log.warning("scanner.%s error: %s", name, e)
            return []
    log.warning("scanner.%s: no compatible signature", name)
    return []

async def _scan_all_sources() -> List[Dict[str, Any]]:
    tasks = [
    _maybe_call_scanner(scan_dexscreener, "dexscreener"),
    _maybe_call_scanner(scan_pumpfun, "pumpfun"),
    _maybe_call_scanner(scan_raydium, "raydium"),
]
if AXIOM_SCAN_ENABLED:
    tasks.append(_maybe_call_scanner(scan_axiom, "axiom"))
batches = await asyncio.gather(*tasks, return_exceptions=False)
    merged: Dict[str, Dict[str, Any]] = {}
    for batch in batches:
        for c in batch:
            mint = c.get("mint")
            if not mint:
                continue
            if mint not in merged:
                merged[mint] = dict(c)
            else:
                v0 = float(merged[mint].get("volumeUsd24h", 0.0) or 0.0)
                v1 = float(c.get("volumeUsd24h", 0.0) or 0.0)
                if v1 > v0:
                    merged[mint] = {**merged[mint], **c}
                else:
                    for k, v in c.items():
                        if merged[mint].get(k) in (None, "", 0):
                            merged[mint][k] = v
    out = list(merged.values())
    log.info("scan: merged %d unique candidates", len(out))
    return out

# ==============================================================================
# Rank & select (preset aware)
# ==============================================================================
def _rank_key(c: Dict[str, Any]) -> float:
    try:
        base = float(alpha_score_fn(c))
    except Exception:
        base = 0.0
    age = float(c.get("pool_age_min") or c.get("ageMin") or 9999)
    liq = float(c.get("liq_usd") or c.get("liquidity_usd") or c.get("liquidityUSD") or 0)
    impact = float(c.get("impact_1k_bps") or c.get("priceImpact1kBps") or 0)

    if PRESET == "max-pnl":
        base += max(0, 60 - age) * 0.7
        base += max(0, 200 - abs(impact)) / 40.0
        base += min(liq, 100_000) / 6_000.0
    elif PRESET == "balanced":
        base += max(0, 90 - age) * 0.4
        base += max(0, 180 - abs(impact)) / 60.0
        base += min(liq, 150_000) / 10_000.0
    else:
        base += max(0, 180 - age) * 0.2
        base += max(0, 150 - abs(impact)) / 80.0
        base += min(liq, 300,000) / 20_000.0
    return float(base)

def _select_after_rank(cands: List[Dict[str, Any]], k: int) -> List[Dict[str, Any]]:
    if not cands:
        return []
    ranked = sorted(cands, key=_rank_key, reverse=True)
    return ranked[:k]

# ==============================================================================
# Filters & buy path
# ==============================================================================
async def _basic_filters(c: Dict[str, Any]) -> bool:
    mint = c.get("mint")
    if not mint:
        log.debug("skip: no mint")
        return False
    if BLOCKLIST and mint in BLOCKLIST:
        log.debug("skip: %s in blocklist", mint)
        return False
    if ALLOWLIST and len(ALLOWLIST) > 0 and mint not in ALLOWLIST:
        log.debug("skip: %s not in allowlist", mint)
        return False
    try:
        ok_hp, _ = await honeypot_check(mint)
        if not ok_hp:
            log.debug("skip: honeypot %s", mint)
            return False
        if await ai_fraud_score(mint) > 0.85:
            log.debug("skip: fraud score high %s", mint)
            return False
    except Exception as e:
        log.debug("filter error %s: %s", mint, e)
        return False
    return True

def _coalesce(src: Dict[str, Any], *keys, default=None):
    for k in keys:
        if k in src and src[k] is not None:
            return src[k]
    return default

async def _buy_token(
    mint: str,
    symbol: str,
    ticket_usd: float,
    entry_price_sol_per_token: float,  # SOL/token; exits price_fn will return same units
) -> bool:
    # Guards
    if await _is_halted():
        return False
    if await _cooldown_key(f"buycd:{mint}", BUY_COOLDOWN_SEC):
        return False
    if not await try_acquire(mint):
        return False

    # --- local helpers for exits wiring ---
    async def price_fn(_mint: str) -> float | None:
        # MUST match units with entry_price -> we use SOL/token
        try:
            px = await get_token_price(_mint)
            return float(px) if px else None
        except Exception:
            return None

    async def sell_fn(_mint: str, usd_to_sell: float, reason: str):
        """
        Fallback sell by notional USD using Jupiter:
          - convert USD -> SOL value
          - use current SOL/token to compute token amount
          - swap token -> SOL
        If you created execution/sell_fn.default_sell_fn, we'll prefer that instead.
        """
        if default_sell_fn:
            try:
                return await default_sell_fn(_mint, float(usd_to_sell), reason)
            except Exception:
                pass
        try:
            usd_per_sol = await convert_sol_to_usd(1.0) or 0.0
            if usd_per_sol <= 0:
                return False, None, {"why": "no_usd_per_sol"}
            est_sol_value = float(usd_to_sell) / float(usd_per_sol)
            px_sol_per_token = await price_fn(_mint) or 0.0
            if px_sol_per_token <= 0:
                return False, None, {"why": "no_token_px"}

            token_amount_in = est_sol_value / px_sol_per_token
            cu_price, jito_tip = compute_priority_tip(est_sol_value)

            swap_kwargs = dict(
                input_mint=_mint,
                output_mint=SOL_MINT,
                amount_float=float(token_amount_in),
                slippage=JUPITER_SLIPPAGE,
            )
            if cu_price is not None:
                swap_kwargs["compute_unit_price_micro_lamports"] = cu_price
                swap_kwargs["priority_fee_micro_lamports"] = cu_price
            if jito_tip is not None:
                swap_kwargs["jito_tip_lamports"] = jito_tip

            ok, sig = await swap_token_jupiter(**swap_kwargs)
            meta = {"reason": reason, "cu_price": cu_price, "jito_tip": jito_tip}
            return ok, sig if ok else None, meta
        except Exception as e:
            return False, None, {"why": f"sell_fn_error {e}"}

    try:
        # DRY-RUN?
        if DRY_RUN:
            try:
                await send_telegram(f"🧪 DRY-RUN — would buy {symbol} ({mint}) for ${ticket_usd:,.2f}")
            except Exception:
                pass
            log.info("dry-run: would-buy %s %s @ $%.2f", symbol, mint, ticket_usd)
            return False

        # Sizing in SOL
        usd_per_sol = await convert_sol_to_usd(1.0)
        if not usd_per_sol or usd_per_sol <= 0:
            return False
        sol_to_spend = ticket_usd / usd_per_sol
        if sol_to_spend <= 0:
            return False

        # Fees + route guard
        cu_price, jito_tip = compute_priority_tip(sol_to_spend)
        ok_rg, why, pre_meta = await precheck_buy(mint, sol_to_spend, SLIPPAGE_BPS)
        # Back-off this mint so we don't hammer Jupiter or spam logs
            try:
                await _set_flag(f"noroute:{mint}", NOROUTE_COOLDOWN_SEC)
            except Exception:
                pass
            msg = (f"⛔ RouteGuard block {symbol} ({mint}) — {why} • "
                   f"hops={pre_meta.get('hops')} • impact_bps={pre_meta.get('impact_bps')}")
            if TELEGRAM_NOTIFY_ROUTE_BLOCKS:
                try:
                    await send_telegram(msg)
                except Exception:
                    pass
            log.info(msg)
             return False

        # Swap (include priority fees when supported)
        swap_kwargs = dict(
            input_mint=SOL_MINT,
            output_mint=mint,
            amount_float=sol_to_spend,
            slippage=JUPITER_SLIPPAGE,
        )
        if cu_price is not None:
            swap_kwargs["compute_unit_price_micro_lamports"] = cu_price
            swap_kwargs["priority_fee_micro_lamports"] = cu_price
        if jito_tip is not None:
            swap_kwargs["jito_tip_lamports"] = jito_tip

        ok, sig = await swap_token_jupiter(**swap_kwargs)
        if not ok:
            log.info("buy: swap failed %s %s", symbol, mint)
            await log_trade_event({"type": "buy_fail", "mint": mint, "symbol": symbol, "why": "swap_failed"})
            return False

        # Persist entry meta (optional)
        try:
            await save_entry_price(get_redis(), mint, entry_price_sol_per_token)
        except Exception as e:
            log.debug("save_entry_price failed: %s", e)

        # Estimated USD spent (based on pre-trade USD/SOL)
        usd_est = float(sol_to_spend * usd_per_sol)

        # ---- register with exit supervisor (key piece) ----
        try:
            # Prefer external helpers if you created them
            pf = default_price_fn if default_price_fn else price_fn
            sf = default_sell_fn if default_sell_fn else sell_fn

            register_entry(
                signature=sig,
                mint=mint,
                qty=None,                               # exits slice by USD; qty not required
                entry_price=float(entry_price_sol_per_token),   # SOL/token (matches price_fn)
                symbol=symbol,
                usd_spent=float(usd_est),
                sell_fn=sf,
                price_fn=pf,
            )
            log.info("exits: entry registered for %s (%s), usd_spent=%.2f", symbol, mint, usd_est)
        except Exception as e:
            log.debug("register_entry failed: %s", e)

        await note_spend(None, float(ticket_usd))

        msg = (
            f"✅ SNIPED {symbol}\n"
            f"Spent: {sol_to_spend:.4f} SOL (~${usd_est:.2f})\n"
            f"Entry (SOL/token): {entry_price_sol_per_token:.10f}\n"
            f"Mint: {mint}\nTx: {sig}"
        )
        try:
            await send_telegram(msg)
        except Exception:
            pass
        log.info("buy: success %s %s sig=%s", symbol, mint, sig)

        await log_trade_event({
            "type": "buy",
            "symbol": symbol,
            "mint": mint,
            "sol_spent": sol_to_spend,
            "usd_spent": usd_est,
            "entry_sol_per_token": entry_price_sol_per_token,
            "signature": sig,
            "cu_price": cu_price,
            "jito_tip": jito_tip,
        })

        # Post-trade watchdog
        try:
            ok_wd, msg2, now_meta = await posttrade_watchdog(mint, sol_to_spend, SLIPPAGE_BPS, pre_meta)
            if not ok_wd:
                await send_telegram(
                    f"⚠️ Route watchdog: {symbol} ({mint}) {msg2} • "
                    f"pre_out={pre_meta.get('out_amount'):.0f} now_out={now_meta.get('out_amount',0):.0f} "
                    f"hops(pre/now)={pre_meta.get('hops')}/{now_meta.get('hops',0)}"
                )
        except Exception as e:
            log.debug("posttrade_watchdog failed: %s", e)

        return True

    finally:
        try:
            await release(mint)
        except Exception:
            pass

# ==============================================================================
# Sniper class
# ==============================================================================
class Sniper:
    def __init__(self):
        self.sem = asyncio.Semaphore(MAX_CONCURRENT_BUYS)
        self.stop = asyncio.Event()

    async def _prewarm(self, cands: List[Dict[str, Any]], ticket_usd: float):
        if not cands or ticket_usd <= 0:
            return
        usd_per_sol = await convert_sol_to_usd(1.0) or 0.0
        if usd_per_sol <= 0:
            return
        amount_sol = ticket_usd / usd_per_sol
        top = sorted(
            [c for c in cands if c.get("mint")],
            key=lambda x: float(x.get("liquidity_usd", 0) or 0),
            reverse=True,
        )[:PREWARM_TOP_N]
        mints = [c["mint"] for c in top]
        try:
            await prewarm_quotes(mints, amount_sol, SLIPPAGE_BPS)
            log.debug("prewarm: %d mints @ ~%.4f SOL each", len(mints), amount_sol)
        except Exception as e:
            log.debug("prewarm failed: %s", e)

    async def _consider(self, c: Dict[str, Any], equity_usd: float):
        if await _is_halted():
            return
        if not await should_allow_new_entry(None):
            log.debug("consider: budget denies new entry")
            return
        if not await _basic_filters(c):
            return

        mint = c.get("mint")
        symbol = c.get("symbol") or (mint[:6] if mint else "???")

        # AI pre-gate (if available)
        try:
            pool_age_min = _coalesce(c, "pool_age_min", "poolAgeMin", "ageMin", default=None)
            try:
            # Harden None → numeric defaults to avoid AI gate type errors
            pool_age_min = float(_coalesce(c, "pool_age_min", "poolAgeMin", "ageMin", default=9999) or 9999)
            liq_usd      = float(_coalesce(c, "liq_usd", "liquidity_usd", "liquidityUSD", default=0.0) or 0.0)
            impact1k_bps = float(_coalesce(c, "impact_1k_bps", "priceImpact1kBps", default=0.0) or 0.0)
            vol5m_pct    = float(_coalesce(c, "vol5m_pct", default=0.0) or 0.0)
            top10_pct    = float(_coalesce(c, "top10_pct", default=0.0) or 0.0)
            whale_hits1h = float(_coalesce(c, "whale_touches_1h", default=0.0) or 0.0)
            if Candidate:
                ok_ai, score_ai, why_ai = pass_gate(
                    Candidate(
                        mint=mint, symbol=symbol, pool_age_min=pool_age_min,
                        liq_usd=liq_usd, impact_1k_bps=impact1k_bps,
                        vol5m_pct=vol5m_pct, top10_pct=top10_pct,
                        honeypot_flag=False, whale_touches_1h=whale_hits1h,
                    )
                )
                if not ok_ai:
                    log.debug("consider: ai-gate reject %s score=%.2f why=%s", mint, score_ai, why_ai)
                    return
        except Exception as e:
            log.debug("ai-gate error %s: %s", mint, e)

        if WALLET_PUBKEY:
            try:
                bal = await get_sol_balance(WALLET_PUBKEY)
                if bal <= MIN_SOL_BALANCE:
                    log.debug("consider: low balance %.4f <= %.4f", bal, MIN_SOL_BALANCE)
                    return
            except Exception as e:
                log.debug("balance check error: %s", e)
                return

        # Ticket sizing
        ticket = float(get_ticket_size_usd(float(equity_usd)))
        try:
            lims = await current_limits(None)
            if isinstance(lims, dict) and "ticket_usd" in lims:
                ticket = min(ticket, float(lims["ticket_usd"]))
        except Exception:
            pass

        ok_mshot, ticket, mnote = await _apply_moonshot_budget(c, ticket)
        if not ok_mshot or ticket <= 0:
            if mnote:
                log.info("skip %s: %s", mint, mnote)
            return

        # Route back-off: if we recently saw no route, skip for a while
        if await _get_flag(f"noroute:{mint}"):
            log.debug("consider: noroute cooldown active for %s", mint)
            return

        entry = await get_token_price(mint)
        if not entry or entry <= 0:
            # Fallback: try a micro-quote to derive SOL/token (best effort)
            try:
                test_sol = 0.02  # tiny probe
                ok_rg, _why, pre = await precheck_buy(mint, test_sol, SLIPPAGE_BPS)
                if ok_rg:
                    out_amt = float(pre.get("out_amount") or 0.0)
                    if out_amt > 0:
                        entry = float(test_sol) / out_amt  # ≈ SOL per token
            except Exception as e:
                log.debug("entry price fallback failed %s: %s", mint, e)
        if not entry or entry <= 0:
            log.debug("consider: no entry price for %s", mint)
            return

        if self.sem.locked():
            log.debug("consider: semaphore locked")
            return

        async with self.sem:
            log.info("buy?: %s (%s) ticket=$%.2f entry=%.10f SOL/token", symbol, mint, ticket, entry)
            ok = await _buy_token(mint, symbol, float(ticket), float(entry))
            if ok:
                await log_scan_row({"mint": mint, "symbol": symbol, "action": "buy_ok", "ticket_usd": ticket})

    async def main_loop(self):
        await send_telegram(f"🎯 Sniper started (preset={PRESET}).")

        while True:
            try:
                if await _is_halted():
                    log.info("loop: halted, sleeping %ds", SCANNER_PERIOD_SEC)
                    await asyncio.sleep(SCANNER_PERIOD_SEC)
                    continue

                # Equity snapshot (USD)
                try:
                    sol_bal = await get_sol_balance(WALLET_PUBKEY) if WALLET_PUBKEY else 0.0
                    usd_per_sol = await convert_sol_to_usd(1.0) or 0.0
                    equity_usd = max(0.0, sol_bal * usd_per_sol)
                except Exception:
                    equity_usd = float(os.getenv("EQUITY_USD_FALLBACK", "2000"))
                await log_pnl_snapshot(_now_utc().isoformat(), equity_usd, 0.0)

                log.info("loop: scanning...")
                cands = await _scan_all_sources()
                if not cands:
                    log.info("loop: no candidates")
                    await asyncio.sleep(SCANNER_PERIOD_SEC)
                    continue

                # Rank & select
                pool_size = max(MAX_CONCURRENT_BUYS * 4, 12) if PRESET == "max-pnl" else max(MAX_CONCURRENT_BUYS * 3, 9)
                top = _select_after_rank(cands, pool_size)

                await log_scan_batch(_now_utc().isoformat(), len(cands), len(top), f"preset={PRESET}")

                # Prewarm quotes with current ticket sizing
                try:
                    ticket = get_ticket_size_usd(equity_usd)
                    await self._prewarm(top, float(ticket))
                except Exception as e:
                    log.debug("prewarm fail: %s", e)

                # Walk the ranked list
                for c in top:
                    await self._consider(c, equity_usd)

            except Exception as e:
                log.error("loop error: %s", e, exc_info=True)
                await log_error_event("main_loop", str(e))

            await asyncio.sleep(SCANNER_PERIOD_SEC)

# ==============================================================================
# Scanner entry shim (optional, used only if SCANNERS_ENTRY is set)
# ==============================================================================
async def _maybe_await(x):
    if inspect.iscoroutine(x):
        return await x
    return x

def _resolve_callable(path: str):
    """
    Resolve 'module.sub:func' or 'func' (from current module) to a callable.
    """
    if not path:
        return None
    if ":" in path:
        mod_name, fn_name = path.split(":", 1)
        mod = importlib.import_module(mod_name)
        return getattr(mod, fn_name, None)
    return globals().get(path)

async def start_scanners():
    """
    Back-compat delegator that finds your scanner loop and runs it.
    If SCANNERS_ENTRY is present, we run that target; otherwise this
    shim is unused (we run Sniper().main_loop()).
    """
    env_target = os.getenv("SCANNERS_ENTRY") or os.getenv("START_SCANNERS_TARGET")
    if env_target:
        try:
            fn = _resolve_callable(env_target)
            if callable(fn):
                log.info(f"start_scanners: env target -> {env_target}")
                return await _maybe_await(fn())
            else:
                log.error(f"start_scanners: env target not callable: {env_target}")
        except Exception as e:
            log.exception(f"start_scanners: failed to import env target {env_target}: {e}")

    # fallback idle so the container stays up if someone calls this directly
    beat = int(os.getenv("SCAN_HEARTBEAT_SEC", "10"))
    log.error("start_scanners: no env target set; idling.")
    while True:
        log.info("scanner: idle heartbeat")
        await asyncio.sleep(beat)

# ==============================================================================
# Entrypoint
# ==============================================================================
# --- explicit entry for Render (env SCANNERS_ENTRY points here) ---
async def run_sniper():
    # start the full scanner/buyer loop (resources already inited by main())
    return await Sniper().main_loop()

async def main():
    # Init infra
    await init_resources()

    # Ensure DB schema exists once
    try:
        await ensure_schema()
    except Exception as e:
        log.warning("ensure_schema failed: %s", e)

    # Refresh session/redis refs via accessors if needed
    try:
        global _session_ref, _redis_ref
        if _session_ref is None or _redis_ref is None:
            try:
                from utils_pkg.core_utils import get_session as _get_session, get_redis as _get_redis
                _session_ref = _get_session()
                _redis_ref = _get_redis()
            except Exception:
                pass
    except Exception:
        pass

    # Start Pump.fun CF cookie refresher
    try:
        if ensure_cookie_refresher:
            ensure_cookie_refresher()
        elif cf_refresh_loop:
            asyncio.create_task(cf_refresh_loop())
        else:
            log.info("pumpfun CF refresher not available")
    except Exception as e:
        log.warning("pumpfun cookie refresher not started: %s", e)

    # Start exits supervisor (once, global)
    try:
        loop = asyncio.get_running_loop()
        start_exit_supervisor(loop)
        log.info("exits supervisor started")
    except Exception as e:
        log.exception(f"failed to start exit supervisor: {e}")

    # Optional: start PnL watcher
    try:
        from execution.pnl_watcher import start_pnl_watcher
        asyncio.create_task(start_pnl_watcher())
        log.info("pnl_watcher task started")
    except Exception:
        pass

    # --- DEMO: register a fake entry on boot (optional) -----------------------
    if os.getenv("DEMO_REGISTER_ON_BOOT") == "1":
        try:
            async def demo_price_fn(_mint: str) -> float | None:
                # keep units consistent with entry_price below (SOL/token here)
                return float(os.getenv("DEMO_ENTRY_PRICE_SOL", "0.01"))

            async def demo_sell_fn(_mint: str, usd_to_sell: float, reason: str):
                log.info("[DEMO SELL] usd=%.2f reason=%s", usd_to_sell, reason)
                return True, "demo_sig", {}

            register_entry(
                signature="demo_entry",
                mint=os.getenv("DEMO_MINT", "DemoMint1111111111111111111111111111111"),
                qty=None,
                entry_price=float(os.getenv("DEMO_ENTRY_PRICE_SOL", "0.01")),  # SOL/token
                symbol="DEMO",
                usd_spent=float(os.getenv("DEMO_USD", "50")),
                sell_fn=demo_sell_fn,
                price_fn=demo_price_fn,
            )
            log.info("DEMO: entry registered")
        except Exception as e:
            log.warning("DEMO register failed: %s", e)
    # -------------------------------------------------------------------------

    # If SCANNERS_ENTRY is set, use the shim; else run our Sniper loop
    if os.getenv("SCANNERS_ENTRY") or os.getenv("START_SCANNERS_TARGET"):
        await start_scanners()
    else:
        await Sniper().main_loop()

    # Cleanup
    await close_resources()

if __name__ == "__main__":
    asyncio.run(main())
