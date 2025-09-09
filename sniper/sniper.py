# sniper/sniper.py — v9.0
# Max-PnL preset · rank+select · priority fees · exits wiring · DB logs · CF refresher
from __future__ import annotations

import os
import re
import asyncio
import logging
import inspect
import importlib
from typing import Dict, Any, List, Callable, Optional, Tuple
from datetime import datetime, timezone

# ==============================================================================
# Logging
# ==============================================================================
def _env_true(x: Optional[str], *, default=False) -> bool:
    if x is None:
        return bool(default)
    return str(x).strip().lower() in ("1", "true", "yes", "on")

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
    async def ensure_schema(): ...

try:
    from db.logger import (
        log_scan_batch, log_scan_row, log_trade_event, log_error_event, log_pnl_snapshot
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
        log.warning("core_utils import failed, using stubs: %s | %s", e_a, e_b)
        async def init_resources(): ...
        async def close_resources(): ...
        async def get_sol_balance(_): return 0.0
        async def get_token_price(_): return 0.0
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
        pct = float(os.getenv("TICKET_PCT_EQUITY", "0.03"))
        cap = float(os.getenv("TICKET_USD_CAP", "250"))
        return {"ticket_usd_pct": pct, "ticket_cap_usd": cap, "ticket_usd": 50.0}
    async def should_allow_new_entry(_): return True
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
# Alpha scoring / params / fees / exits supervisor (safe)
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
        pct = float(os.getenv("TICKET_PCT_EQUITY", "0.04"))
        cap = float(os.getenv("TICKET_USD_CAP", "300"))
        base = equity_usd * pct
        return float(min(cap, max(10.0, base)))

try:
    from fees import compute_priority_tip  # type: ignore
except Exception:
    def compute_priority_tip(sol_to_spend: float) -> Tuple[int, Optional[int]]:
        if sol_to_spend >= 1.0:
            return (10_000, 50_000)
        if sol_to_spend >= 0.25:
            return (6_000, 25_000)
        return (3_000, 10_000)

try:
    from execution.exits import start_exit_supervisor as _exits_start, register_entry
    import asyncio

    async def start_exit_supervisor(_redis):
        loop = asyncio.get_running_loop()
        try:
            # newer one-arg form
            return await _exits_start(_redis)  # type: ignore
        except Exception:
            # older two-arg form
            return await _exits_start(loop, _redis)  # type: ignore

except Exception as e:
    LOG.warning("execution.exits import fallback: %s", e)
    async def start_exit_supervisor(_redis): ...
    async def register_entry(**_): ...

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

PAUSE_TRADING = _env_true(os.getenv("PAUSE_TRADING", "false"))
DRY_RUN       = _env_true(os.getenv("DRY_RUN", "false"))

SCANNER_PERIOD_SEC   = int(os.getenv("SCANNER_PERIOD_SEC", "15"))
BUY_COOLDOWN_SEC     = int(os.getenv("BUY_COOLDOWN_SEC", "120"))
MAX_CONCURRENT_BUYS  = int(os.getenv("MAX_CONCURRENT_BUYS", "2"))
MIN_SOL_BALANCE      = float(os.getenv("MIN_SOL_BALANCE", "0.2"))
JUPITER_SLIPPAGE     = float(os.getenv("JUPITER_MAX_SLIPPAGE", "0.005"))
SLIPPAGE_BPS         = max(1, int(JUPITER_SLIPPAGE * 10000))
PREWARM_TOP_N        = int(os.getenv("PREWARM_TOP_N", "8"))

# New envs
TELEGRAM_NOTIFY_ROUTE_BLOCKS = _env_true(os.getenv("TELEGRAM_NOTIFY_ROUTE_BLOCKS", "true"))
NOROUTE_COOLDOWN_SEC = int(os.getenv("NOROUTE_COOLDOWN_SEC", "600"))
MIN_POOL_AGE_MIN     = int(os.getenv("MIN_POOL_AGE_MIN", "0"))
MIN_LIQ_USD          = float(os.getenv("MIN_LIQ_USD", "0"))

PRESET = os.getenv("STRATEGY_PRESET", "max-pnl").lower()
if PRESET not in ("max-pnl", "balanced", "conservative"):
    PRESET = "max-pnl"

BLOCKLIST = {x.strip() for x in os.getenv("BLOCKLIST_MINTS", "").split(",") if x.strip()}
ALLOWLIST = {x.strip() for x in os.getenv("ALLOWLIST_MINTS", "").split(",") if x.strip()}

HALT_KEYS = ("halt:manual", "halt:infra", "infra:halt", "halt:daily_loss")

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

# ==============================================================================
# Helpers: mint normalization + numeric safety
# ==============================================================================
BASE58_RE = re.compile(r"[1-9A-HJ-NP-Za-km-z]{32,44}")

def normalize_mint(m) -> str:
    if not isinstance(m, str):
        return ""
    m = m.strip().replace(" ", "")
    hit = BASE58_RE.search(m)
    return hit.group(0) if hit else ""

def _nz(x, d=0.0) -> float:
    try:
        fx = float(x)
        if fx != fx:  # NaN guard
            return float(d)
        return fx
    except Exception:
        return float(d)

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

# ==============================================================================
# Moonshot budget (optional)
# ==============================================================================
MOONSHOT_ENABLED           = _env_true(os.getenv("MOONSHOT_ENABLED", "true"))
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
    batches = await asyncio.gather(
        _maybe_call_scanner(scan_dexscreener, "dexscreener"),
        _maybe_call_scanner(scan_pumpfun, "pumpfun"),
        _maybe_call_scanner(scan_raydium, "raydium"),
        _maybe_call_scanner(scan_axiom, "axiom"),
        return_exceptions=False,
    )
    merged: Dict[str, Dict[str, Any]] = {}
    for batch in batches:
        for c in batch:
            mint = normalize_mint(c.get("mint"))
            if not mint:
                continue
            c["mint"] = mint
            if mint not in merged:
                merged[mint] = dict(c)
            else:
                v0 = _nz(merged[mint].get("volumeUsd24h"), 0.0)
                v1 = _nz(c.get("volumeUsd24h"), 0.0)
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
    age = _nz(c.get("pool_age_min") or c.get("ageMin"), 9999)
    liq = _nz(c.get("liq_usd") or c.get("liquidity_usd") or c.get("liquidityUSD"), 0)
    impact = _nz(c.get("impact_1k_bps") or c.get("priceImpact1kBps"), 0)

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
        base += min(liq, 300_000) / 20_000.0
    return float(base)

def _select_after_rank(cands: List[Dict[str, Any]], k: int) -> List[Dict[str, Any]]:
    if not cands:
        return []
    ranked = sorted(cands, key=_rank_key, reverse=True)
    return ranked[:k]

# ==============================================================================
# Filters & buy path
# ==============================================================================
def _coalesce(src: Dict[str, Any], *keys, default=None):
    for k in keys:
        if k in src and src[k] is not None:
            return src[k]
    return default

async def _basic_filters(c: Dict[str, Any]) -> bool:
    mint = normalize_mint(c.get("mint"))
    if mint != c.get("mint"):
        c["mint"] = mint
    if not mint:
        log.debug("skip: bad mint string")
        return False

    if BLOCKLIST and mint in BLOCKLIST:
        log.debug("skip: %s in blocklist", mint)
        return False
    if ALLOWLIST and len(ALLOWLIST) > 0 and mint not in ALLOWLIST:
        log.debug("skip: %s not in allowlist", mint)
        return False

    # Minimums
    try:
        age = _nz(_coalesce(c, "pool_age_min", "poolAgeMin", "ageMin"), 9999)
        if age < MIN_POOL_AGE_MIN:
            log.debug("skip: age %.2f < MIN_POOL_AGE_MIN=%s", age, MIN_POOL_AGE_MIN)
            return False
    except Exception:
        pass
    try:
        liq = _nz(_coalesce(c, "liq_usd", "liquidity_usd", "liquidityUSD"), 0.0)
        if liq < MIN_LIQ_USD:
            log.debug("skip: liq %.0f < MIN_LIQ_USD=%s", liq, MIN_LIQ_USD)
            return False
    except Exception:
        pass

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

async def _buy_token(
    mint: str,
    symbol: str,
    ticket_usd: float,
    entry_price_sol_per_token: float,
) -> bool:
    # Guards
    if await _is_halted():
        return False
    if await _cooldown_key(f"buycd:{mint}", BUY_COOLDOWN_SEC):
        return False
    if not await try_acquire(mint):
        return False

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
        if not ok_rg:
            if TELEGRAM_NOTIFY_ROUTE_BLOCKS:
                msg = (f"⛔ RouteGuard block {symbol} ({mint}) — {why} • "
                       f"hops={pre_meta.get('hops')} • impact_bps={pre_meta.get('impact_bps')}")
                try:
                    await send_telegram(msg)
                except Exception:
                    pass
            # Back-off on mint with no routes
            if why and "no_route" in str(why).lower() and NOROUTE_COOLDOWN_SEC > 0:
                await _cooldown_key(f"noroutecd:{mint}", NOROUTE_COOLDOWN_SEC)
            await log_trade_event({"type": "buy_block", "mint": mint, "symbol": symbol, "why": str(why)})
            log.info("routeguard: block %s %s: %s", symbol, mint, why)
            return False

        # Swap (include priority fees when supported)
        swap_kwargs = dict(
            input_mint=SOL_MINT,
            output_mint=mint,
            amount_float=sol_to_spend,
            slippage=JUPITER_SLIPPAGE,
        )
        for k, v in [
            ("compute_unit_price_micro_lamports", cu_price),
            ("priority_fee_micro_lamports", cu_price),
            ("jito_tip_lamports", jito_tip),
        ]:
            if v is not None:
                swap_kwargs[k] = v

        ok, sig = await swap_token_jupiter(**swap_kwargs)
        if not ok:
            log.info("buy: swap failed %s %s", symbol, mint)
            await log_trade_event({"type": "buy_fail", "mint": mint, "symbol": symbol, "why": "swap_failed"})
            return False

        # Persist entry meta + register with exit supervisor
        try:
            await save_entry_price(get_redis(), mint, entry_price_sol_per_token)
        except Exception as e:
            log.debug("save_entry_price failed: %s", e)

        try:
            trailing_pct = float(os.getenv("STOP_LOSS_PERCENT", "20"))
            take1_pct    = float(os.getenv("TAKE_PROFIT1_PCT", "30"))
            take2_pct    = float(os.getenv("TAKE_PROFIT2_PCT", "60"))
            take3_pct    = float(os.getenv("TAKE_PROFIT3_PCT", "120"))
            await register_entry(
                mint=mint,
                symbol=symbol,
                entry_price=entry_price_sol_per_token,
                amount_sol=sol_to_spend,
                trailing_stop_pct=trailing_pct,
                take_profits=[take1_pct, take2_pct, take3_pct],
                created_at=_now_utc().isoformat(),
            )
        except Exception as e:
            log.debug("register_entry failed: %s", e)

        await note_spend(None, float(ticket_usd))

        usd_est = await convert_sol_to_usd(sol_to_spend) or 0.0
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
            if not ok_wd and TELEGRAM_NOTIFY_ROUTE_BLOCKS:
                await send_telegram(
                    f"⚠️ Route watchdog: {symbol} ({mint}) {msg2} • "
                    f"pre_out={_nz(pre_meta.get('out_amount'),0):.0f} now_out={_nz(now_meta.get('out_amount'),0):.0f} "
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
        def _liq(x):
            return _nz(x.get("liquidity_usd") or x.get("liq_usd") or x.get("liquidityUSD"), 0)
        top = sorted(
            [c for c in cands if c.get("mint")],
            key=_liq,
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
            liq_usd      = _nz(_coalesce(c, "liq_usd", "liquidity_usd", "liquidityUSD"), 0.0)
            impact1k_bps = _nz(_coalesce(c, "impact_1k_bps", "priceImpact1kBps"), 0.0)
            vol5m_pct    = _nz(_coalesce(c, "vol5m_pct"), 0.0)
            top10_pct    = _nz(_coalesce(c, "top10_pct"), 0.0)
            whale_hits1h = _nz(_coalesce(c, "whale_touches_1h"), 0.0)
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

        entry = _nz(await get_token_price(mint), 0.0)
        if entry <= 0:
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
        try:
            await send_telegram(f"🎯 Sniper started (preset={PRESET}).")
        except Exception:
            pass

        # Start exits supervisor (non-blocking)
        try:
            asyncio.create_task(start_exit_supervisor(get_redis()))
            log.info("exits supervisor started")
        except Exception as e:
            log.debug("exit supervisor not started: %s", e)

        # Optional: self-learning loop
        try:
            from self_learning_ai_strategy import run_self_learning_loop
            asyncio.create_task(run_self_learning_loop(get_redis()))
        except Exception as e:
            log.debug("self_learning loop not started: %s", e)

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
# Entrypoint (clean, no stray indentation)
# ==============================================================================
async def _maybe_await(x):
    return await x if inspect.iscoroutine(x) else x

def _resolve_callable(path: str):
    """Resolve 'module.sub:func' or 'func' (from current module) to a callable."""
    if not path:
        return None
    if ":" in path:
        mod_name, fn_name = path.split(":", 1)
        mod = importlib.import_module(mod_name)
        return getattr(mod, fn_name, None)
    return globals().get(path)

async def start_scanners():
    """
    Delegates to your scanner loop.
    Priority:
      1) SCANNERS_ENTRY / START_SCANNERS_TARGET -> 'module.path:func'
      2) Common function names in this module or 'scanners' modules
      3) Idle keepalive
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

    for name in ("run_sniper", "scan_loop", "scan_forever", "scanner_loop", "run_scanners"):
        fn = globals().get(name)
        if callable(fn):
            log.info(f"start_scanners: delegating to local {name}()")
            return await _maybe_await(fn())

    for mod_name, fn_name in (
        ("scanners", "scan_loop"),
        ("scanners", "scan_forever"),
        ("sniper.scanners", "scan_loop"),
        ("sniper.scanners", "scan_forever"),
    ):
        try:
            mod = importlib.import_module(mod_name)
            fn = getattr(mod, fn_name, None)
            if callable(fn):
                log.info(f"start_scanners: delegating to {mod_name}.{fn_name}()")
                return await _maybe_await(fn())
        except Exception:
            pass

    log.error(
        "start_scanners: no scan loop found. "
        "Set SCANNERS_ENTRY='module.path:func' or provide run_sniper()/scan_loop(). "
        "Idling to keep the container alive."
    )
    while True:
        log.info("scanner: idle heartbeat")
        await asyncio.sleep(int(os.getenv("SCAN_HEARTBEAT_SEC", "10")))

async def scan_loop():
    """Minimal runner: start exits + optional PnL watcher, then idle."""
    try:
        asyncio.create_task(start_exit_supervisor(get_redis()))
        log.info("scanner: exit supervisor started")
    except Exception as e:
        log.debug("scanner: failed to start exit supervisor: %s", e)

    try:
        from execution.pnl_watcher import start_pnl_watcher
        asyncio.create_task(start_pnl_watcher())
        log.info("scanner: pnl_watcher task started")
    except Exception:
        pass

    while True:
        log.info("scanner: idle heartbeat")
        await asyncio.sleep(int(os.getenv("SCAN_HEARTBEAT_SEC", "10")))

async def run_sniper():
    """Default scanner entry: exits + watcher + main loop."""
    try:
        asyncio.create_task(start_exit_supervisor(get_redis()))
        log.info("exits supervisor started")
    except Exception as e:
        log.debug("exit supervisor not started: %s", e)

    try:
        from execution.pnl_watcher import watch_positions_loop
        asyncio.create_task(watch_positions_loop())
    except Exception as e:
        log.debug("pnl watcher not started: %s", e)

    await Sniper().main_loop()

async def main():
    await init_resources()

    try:
        await ensure_schema()
    except Exception as e:
        log.warning("ensure_schema failed: %s", e)

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

    # Refresh session/redis refs via accessors if needed
    try:
        global _session_ref, _redis_ref
        if _session_ref is None or _redis_ref is None:
            from utils_pkg.core_utils import get_session as _gs, get_redis as _gr
            _session_ref, _redis_ref = _gs(), _gr()
    except Exception:
        pass

    if os.getenv("SCANNERS_ENTRY") or os.getenv("START_SCANNERS_TARGET"):
        await start_scanners()
    else:
        await run_sniper()

    await close_resources()

if __name__ == "__main__":
    asyncio.run(main())
