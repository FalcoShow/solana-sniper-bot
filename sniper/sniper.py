# sniper/sniper.py — v6.2 (CF refresher-safe, resilient scanners)
from __future__ import annotations

import os
import asyncio
import logging
import random
from typing import Dict, Any, List, Callable, Optional
from datetime import datetime, timezone

# ----------------------------- logging -----------------------------------------
_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, _LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
)
log = logging.getLogger("sniper")

# ----------------------------- core utils (tolerant) ---------------------------
_session_ref = None
_redis_ref = None

def get_session():
    return _session_ref

def get_redis():
    return _redis_ref

try:
    # Preferred shape: exposes live objects as names
    from utils_pkg.core_utils import (
        init_resources, close_resources,
        session as _session_ref, redis as _redis_ref,
        get_sol_balance, get_token_price, swap_token_jupiter,
        save_entry_price, convert_sol_to_usd, send_telegram,
        log_to_notion, log_trade_event,
    )
except Exception as e_a:
    try:
        # Alternate shape: exposes accessors
        from utils_pkg.core_utils import (
            init_resources, close_resources,
            get_session as _get_session, get_redis as _get_redis,
            get_sol_balance, get_token_price, swap_token_jupiter,
            save_entry_price, convert_sol_to_usd, send_telegram,
            log_to_notion, log_trade_event,
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
        async def log_trade_event(*_a, **_k): ...

# ----------------------------- CF cookie refresher ------------------------------
# IMPORTANT: call with NO arguments.
try:
    from utils_pkg.pumpfun_cf import ensure_cookie_refresher, refresh_loop as cf_refresh_loop
except Exception:
    ensure_cookie_refresher = None
    cf_refresh_loop = None

# ----------------------------- risk/locks/ai/route (soft deps) -----------------
try:
    from risk_manager import current_limits, should_allow_new_entry, note_spend
except Exception as e:
    log.warning("risk_manager import fallback: %s", e)

    async def current_limits(_):
        return {"ticket_usd": float(os.getenv("DEFAULT_TICKET_USD", "25"))}

    async def should_allow_new_entry(_):
        return True

    async def note_spend(*_a, **_k): ...

try:
    from shadow_utils import shadow_take, shadow_log
except Exception:
    def shadow_take(_tag: str) -> bool: return False
    async def shadow_log(*_a, **_k): ...

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

# ----------------------------- scanners (flat or package) ----------------------
scan_dexscreener = scan_pumpfun = scan_raydium = None

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

if scan_dexscreener is None or scan_pumpfun is None or scan_raydium is None:
    try:
        if scan_dexscreener is None:
            from scanner.dexscreener_scanner import scan_dexscreener  # type: ignore
        if scan_pumpfun is None:
            from scanner.pumpfun_scanner import scan_pumpfun  # type: ignore
        if scan_raydium is None:
            from scanner.raydium_scanner import scan_raydium  # type: ignore
    except Exception as e:
        log.warning("scanner package import failed: %s", e)

# ----------------------------- env/config --------------------------------------
SOL_MINT = "So11111111111111111111111111111111111111112"
WALLET_PUBKEY = os.getenv("WALLET_PUBKEY") or os.getenv("WALLET_PUBLIC_KEY")

PAUSE_TRADING = os.getenv("PAUSE_TRADING", "false").lower() == "true"
DRY_RUN       = os.getenv("DRY_RUN", "false").lower() == "true"

SCANNER_PERIOD_SEC   = int(os.getenv("SCANNER_PERIOD_SEC", "15"))
BUY_COOLDOWN_SEC     = int(os.getenv("BUY_COOLDOWN_SEC", "120"))
MAX_CONCURRENT_BUYS  = int(os.getenv("MAX_CONCURRENT_BUYS", "2"))
MIN_SOL_BALANCE      = float(os.getenv("MIN_SOL_BALANCE", "0.2"))
JUPITER_SLIPPAGE     = float(os.getenv("JUPITER_MAX_SLIPPAGE", "0.005"))
SLIPPAGE_BPS         = max(1, int(JUPITER_SLIPPAGE * 10000))
PREWARM_TOP_N        = int(os.getenv("PREWARM_TOP_N", "8"))

BLOCKLIST = {x.strip() for x in os.getenv("BLOCKLIST_MINTS", "").split(",") if x.strip()}
ALLOWLIST = {x.strip() for x in os.getenv("ALLOWLIST_MINTS", "").split(",") if x.strip()}

HALT_KEYS = ("halt:manual", "halt:infra", "infra:halt", "halt:daily_loss")

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

# ----------------------------- halt + cooldown ---------------------------------
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

# ----------------------------- moonshot budget ---------------------------------
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

# ----------------------------- scan plumbing -----------------------------------
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
            # Try next arity
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
        return_exceptions=False,
    )
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
    random.shuffle(out)
    log.info("scan: merged %d unique candidates", len(out))
    return out

# ----------------------------- filters & buy path ------------------------------
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

async def _buy_token(mint: str, symbol: str, ticket_usd: float, entry_price_sol_per_token: float) -> bool:
    if await _is_halted():
        return False
    if await _cooldown_key(f"buycd:{mint}", BUY_COOLDOWN_SEC):
        return False
    if not await try_acquire(mint):
        return False

    try:
        if shadow_take("SNIPER"):
            await shadow_log("sniper", "candidate", {
                "mint": mint, "symbol": symbol, "ticket": f"${ticket_usd:,.2f}",
                "entry(SOL/token)": f"{entry_price_sol_per_token:.10f}",
            })
            log.info("shadow: would-buy %s %s @ $%.2f", symbol, mint, ticket_usd)
            return False

        if DRY_RUN:
            await send_telegram(f"🧪 DRY-RUN — would buy {symbol} ({mint}) for ${ticket_usd:,.2f}")
            log.info("dry-run: would-buy %s %s @ $%.2f", symbol, mint, ticket_usd)
            return False

        usd_per_sol = await convert_sol_to_usd(1.0)
        if not usd_per_sol or usd_per_sol <= 0:
            return False
        sol_to_spend = ticket_usd / usd_per_sol
        if sol_to_spend <= 0:
            return False

        ok_rg, why, pre_meta = await precheck_buy(mint, sol_to_spend, SLIPPAGE_BPS)
        if not ok_rg:
            msg = (f"⛔ RouteGuard block {symbol} ({mint}) — {why} • "
                   f"hops={pre_meta.get('hops')} • impact_bps={pre_meta.get('impact_bps')}")
            await send_telegram(msg)
            log.info(msg)
            return False

        ok, sig = await swap_token_jupiter(
            input_mint=SOL_MINT,
            output_mint=mint,
            amount_float=sol_to_spend,
            slippage=JUPITER_SLIPPAGE,
        )
        if not ok:
            log.info("buy: swap failed %s %s", symbol, mint)
            return False

        trailing = entry_price_sol_per_token * (1 - float(os.getenv("STOP_LOSS_PERCENT", "20")) / 100.0)
        pos = {
            "token_address": mint, "symbol": symbol,
            "buy_price": entry_price_sol_per_token,
            "amount_invested": sol_to_spend,
            "timestamp": _now_utc().isoformat(),
            "trailing_stop": trailing, "chunks_sold": 0,
        }
        try:
            from positions import save_position
            await save_position(mint, pos)
        except Exception as e:
            log.debug("save_position failed: %s", e)

        try:
            await save_entry_price(get_redis(), mint, entry_price_sol_per_token)
        except Exception as e:
            log.debug("save_entry_price failed: %s", e)

        await note_spend(None, float(ticket_usd))

        usd_est = await convert_sol_to_usd(sol_to_spend) or 0.0
        msg = (
            f"✅ SNIPED {symbol}\n"
            f"Spent: {sol_to_spend:.4f} SOL (~${usd_est:.2f})\n"
            f"Entry (SOL/token): {entry_price_sol_per_token:.10f}\n"
            f"Mint: {mint}\nTx: {sig}"
        )
        await send_telegram(msg)
        log.info("buy: success %s %s sig=%s", symbol, mint, sig)
        try:
            await log_trade_event(
                token=mint, symbol=symbol, balance=sol_to_spend,
                entry=entry_price_sol_per_token, current=entry_price_sol_per_token,
                profit=0.0, reason="Entry", side="buy", signature=sig,
            )
            await log_to_notion(
                token=mint, symbol=symbol, balance=sol_to_spend,
                entry=entry_price_sol_per_token, current=entry_price_sol_per_token,
                profit=0.0, reason="Entry",
            )
        except Exception as e:
            log.debug("post-log failed: %s", e)

        ok_wd, msg2, now_meta = await posttrade_watchdog(mint, sol_to_spend, SLIPPAGE_BPS, pre_meta)
        if not ok_wd:
            await send_telegram(
                f"⚠️ Route watchdog: {symbol} ({mint}) {msg2} • "
                f"pre_out={pre_meta.get('out_amount'):.0f} now_out={now_meta.get('out_amount',0):.0f} "
                f"hops(pre/now)={pre_meta.get('hops')}/{now_meta.get('hops',0)}"
            )
        return True
    finally:
        try:
            await release(mint)
        except Exception:
            pass

# ----------------------------- main loop ---------------------------------------
class Sniper:
    def __init__(self):
        self.sem = asyncio.Semaphore(MAX_CONCURRENT_BUYS)
        self.stop = asyncio.Event()
        self.iter = 0

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

    async def _consider(self, c: Dict[str, Any], sl_params: Dict[str, Any]):
        if await _is_halted():
            return
        if not await should_allow_new_entry(None):
            log.debug("consider: budget denies new entry")
            return
        if not await _basic_filters(c):
            return

        mint = c.get("mint")
        symbol = c.get("symbol") or (mint[:6] if mint else "???")
        try:
            pool_age_min = _coalesce(c, "pool_age_min", "poolAgeMin", "ageMin", default=None)
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

        limits = await current_limits(None)
        ticket = float(limits["ticket_usd"])

        ok_mshot, ticket, mnote = await _apply_moonshot_budget(c, ticket)
        if not ok_mshot or ticket <= 0:
            if mnote:
                log.info("skip %s: %s", mint, mnote)
            return

        entry = await get_token_price(mint)
        if not entry or entry <= 0:
            log.debug("consider: no entry price for %s", mint)
            return

        if self.sem.locked():
            log.debug("consider: semaphore locked")
            return

        async with self.sem:
            log.info("buy?: %s (%s) ticket=$%.2f entry=%.10f", symbol, mint, ticket, entry)
            await _buy_token(mint, symbol, float(ticket), float(entry))

    async def main_loop(self):
        await send_telegram("🎯 Sniper started (v6.2: CF refresher-safe).")
        try:
            from self_learning_ai_strategy import run_self_learning_loop
            asyncio.create_task(run_self_learning_loop(get_redis()))
        except Exception as e:
            log.debug("self_learning loop not started: %s", e)

        while not self.stop.is_set():
            try:
                if await _is_halted():
                    log.info("loop: halted, sleeping %ds", SCANNER_PERIOD_SEC)
                    await asyncio.sleep(SCANNER_PERIOD_SEC)
                    continue

                try:
                    from self_learning_ai_strategy import get_strategy_params
                    sl_params = await get_strategy_params(get_redis())
                except Exception:
                    sl_params = {"SLIPPAGE": JUPITER_SLIPPAGE}

                log.info("loop: scanning...")
                cands = await _scan_all_sources()
                if not cands:
                    log.info("loop: no candidates")
                    await asyncio.sleep(SCANNER_PERIOD_SEC)
                    continue

                limits = await current_limits(None)
                await self._prewarm(cands, float(limits["ticket_usd"]))

                random.shuffle(cands)
                for c in cands:
                    await self._consider(c, sl_params)

            except Exception as e:
                log.error("loop error: %s", e, exc_info=True)

            await asyncio.sleep(SCANNER_PERIOD_SEC)

# ----------------------------- entrypoint --------------------------------------
async def main():
    await init_resources()

    # Start Pump.fun CF cookie refresher — NO ARGUMENTS
    try:
        if ensure_cookie_refresher:
            ensure_cookie_refresher()
        elif cf_refresh_loop:
            asyncio.create_task(cf_refresh_loop())
        else:
            log.info("pumpfun CF refresher not available")
    except Exception as e:
        log.warning("pumpfun cookie refresher not started: %s", e)

    try:
        # Refresh session/redis refs if accessors are available
        global _session_ref, _redis_ref
        if _session_ref is None and _redis_ref is None:
            try:
                from utils_pkg.core_utils import get_session as _get_session, get_redis as _get_redis
                _session_ref = _get_session()
                _redis_ref = _get_redis()
            except Exception:
                pass

        await Sniper().main_loop()
    finally:
        await close_resources()

if __name__ == "__main__":
    asyncio.run(main())
