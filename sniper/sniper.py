# -*- coding: utf-8 -*-
"""
sniper/sniper.py — v6 (compat-safe)

- Keeps your original behavior (prewarm quotes + route guard + watchdog + self-learning loop)
- Tolerates different scanner function signatures: (redis, session) OR (redis) OR ()
- Handles missing optional modules gracefully
- No writes to /mnt/data (Render-friendly)
"""

from __future__ import annotations

import os
import asyncio
import logging
import random
from typing import Dict, Any, List, Callable, Optional
from datetime import datetime, timezone

# ──────────────────────────────────────────────────────────────
# Core utils (soft fallbacks)
# ──────────────────────────────────────────────────────────────
try:
    from utils_pkg.core_utils import (
        init_resources, close_resources, session, redis,
        get_sol_balance, get_token_price, swap_token_jupiter,
        save_entry_price, convert_sol_to_usd, send_telegram,
        log_to_notion, log_trade_event,
    )
except Exception:
    async def init_resources(): ...
    async def close_resources(): ...
    session = None  # type: ignore
    redis = None    # type: ignore
    async def get_sol_balance(_: str) -> float: return 0.0
    async def get_token_price(_: str) -> float: return 0.0
    async def swap_token_jupiter(**_: Any): return (False, "")
    async def save_entry_price(*_: Any, **__: Any): ...
    async def convert_sol_to_usd(x: float) -> float: return 0.0
    async def send_telegram(_: str): ...
    async def log_to_notion(*_: Any, **__: Any): ...
    async def log_trade_event(*_: Any, **__: Any): ...

# Risk / budget
try:
    from risk_manager import current_limits, should_allow_new_entry, note_spend
except Exception:
    async def current_limits(_: Any) -> Dict[str, Any]:
        return {"ticket_usd": float(os.getenv("DEFAULT_TICKET_USD", "25"))}
    async def should_allow_new_entry(_: Any) -> bool: return True
    async def note_spend(*_: Any, **__: Any): ...

# Optional shadow mode
try:
    from shadow_utils import shadow_take, shadow_log
except Exception:
    def shadow_take(_: str) -> bool: return False
    async def shadow_log(*_: Any, **__: Any): ...

# Entry dedupe gate
try:
    from entry_gate import try_acquire, release
except Exception:
    async def try_acquire(_: str) -> bool: return True
    async def release(_: str): ...

# Quote prewarm + Route Guard
try:
    from jup_quote_cache import prewarm_quotes
except Exception:
    async def prewarm_quotes(*_: Any, **__: Any): ...

try:
    from route_guard import precheck_buy, posttrade_watchdog
except Exception:
    async def precheck_buy(_mint: str, _sol_to_spend: float, _slip_bps: int):
        return True, "ok", {"hops": 1, "impact_bps": 0, "out_amount": 0}
    async def posttrade_watchdog(_mint: str, _sol_to_spend: float, _slip_bps: int, pre_meta: Dict[str, Any]):
        return True, "ok", {"hops": pre_meta.get("hops", 1), "out_amount": pre_meta.get("out_amount", 0)}

# Optional AI filters
try:
    from utils_pkg.ai_buy_scoring import Candidate, pass_gate
except Exception:
    Candidate = None
    def pass_gate(*_: Any, **__: Any): return True, 1.0, "no_ai"

try:
    from utils_pkg.ai_sell_logic import ai_fraud_score
except Exception:
    async def ai_fraud_score(_: str) -> float: return 0.0

try:
    from utils_pkg.honeypot import honeypot_check
except Exception:
    async def honeypot_check(_: str): return True, {}

# Scanners: import flexible
SCAN_IMPORT_ERRORS: List[str] = []
scan_dexscreener = None
scan_pumpfun = None
scan_raydium = None

try:
    from dexscreener_scanner import scan_dexscreener  # type: ignore
except Exception as e:
    SCAN_IMPORT_ERRORS.append(f"dexscreener_scanner flat import failed: {e}")
try:
    from pumpfun_scanner import scan_pumpfun  # type: ignore
except Exception as e:
    SCAN_IMPORT_ERRORS.append(f"pumpfun_scanner flat import failed: {e}")
try:
    from raydium_scanner import scan_raydium  # type: ignore
except Exception as e:
    SCAN_IMPORT_ERRORS.append(f"raydium_scanner flat import failed: {e}")

if (scan_dexscreener is None) or (scan_pumpfun is None) or (scan_raydium is None):
    try:
        if scan_dexscreener is None:
            from scanner.dexscreener_scanner import scan_dexscreener  # type: ignore
        if scan_pumpfun is None:
            from scanner.pumpfun_scanner import scan_pumpfun  # type: ignore
        if scan_raydium is None:
            from scanner.raydium_scanner import scan_raydium  # type: ignore
    except Exception as e:
        SCAN_IMPORT_ERRORS.append(f"scanner.pkg import failed: {e}")

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
logger = logging.getLogger("sniper")

# Config
SOL_MINT = "So11111111111111111111111111111111111111112"
WALLET_PUBKEY = os.getenv("WALLET_PUBKEY") or os.getenv("WALLET_PUBLIC_KEY")

PAUSE_TRADING = os.getenv("PAUSE_TRADING", "false").strip().lower() in ("1","true","yes","on")
DRY_RUN       = os.getenv("DRY_RUN", "false").strip().lower() in ("1","true","yes","on")

SCANNER_PERIOD_SEC  = int(os.getenv("SCANNER_PERIOD_SEC", "15"))
BUY_COOLDOWN_SEC    = int(os.getenv("BUY_COOLDOWN_SEC", "120"))
MAX_CONCURRENT_BUYS = int(os.getenv("MAX_CONCURRENT_BUYS", "2"))
MIN_SOL_BALANCE     = float(os.getenv("MIN_SOL_BALANCE", "0.2"))
JUPITER_SLIPPAGE    = float(os.getenv("JUPITER_MAX_SLIPPAGE", "0.005"))
SLIPPAGE_BPS        = max(1, int(JUPITER_SLIPPAGE * 10000))

BLOCKLIST = set(x.strip() for x in os.getenv("BLOCKLIST_MINTS", "").split(",") if x.strip())
ALLOWLIST = set(x.strip() for x in os.getenv("ALLOWLIST_MINTS", "").split(",") if x.strip())

HALT_KEYS = ("halt:manual", "halt:infra", "infra:halt", "halt:daily_loss")

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

async def _is_halted() -> bool:
    if PAUSE_TRADING:
        return True
    if not redis:
        return False
    try:
        vals = await asyncio.gather(*[redis.get(k) for k in HALT_KEYS])
        return any(v and str(v) not in ("0", "") for v in vals)
    except Exception:
        return False

async def _cooldown_key(key: str, ttl: int) -> bool:
    """Return True if on cooldown (key exists). Otherwise set TTL and return False."""
    if not redis:
        return False
    try:
        v = await redis.get(key)
        if v: 
            return True
        await redis.set(key, "1", ex=ttl)
        return False
    except Exception:
    and considered in the "analysis" are actually included in the file.  
        return False

# Moonshot sleeve config
MOONSHOT_ENABLED           = os.getenv("MOONSHOT_ENABLED", "true").strip().lower() in ("1","true","yes","on")
MOONSHOT_DAILY_USD         = float(os.getenv("MOONSHOT_DAILY_USD", "100"))
MOONSHOT_MAX_TRADE_USD     = float(os.getenv("MOONSHOT_MAX_TRADE_USD", "25"))
MOONSHOT_AGE_CUTOFF_MIN    = int(os.getenv("MOONSHOT_AGE_CUTOFF_MIN", "30"))
MOONSHOT_LIQ_CUTOFF_USD    = float(os.getenv("MOONSHOT_LIQ_CUTOFF_USD", "20000"))
MOONSHOT_IMPACT_CUTOFF_BPS = float(os.getenv("MOONSHOT_IMPACT_CUTOFF_BPS", "150"))

try:
    from utils_pkg.moonshot_budget import MoonshotBudget
    _moonshot_mgr = MoonshotBudget(redis=redis)
except Exception:
    _moonshot_mgr = None

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
    if not redis:
        return 0.0
    try:
        v = await redis.get(f"moonshot:spent:{_today_tag()}")
        return float(v) if v else 0.0
    except Exception:
        return 0.0

async def _moonshot_add_spent(usd: float) -> None:
    if not redis:
        return
    try:
        k = f"moonshot:spent:{_today_tag()}"
        await redis.incrbyfloat(k, float(max(0.0, usd)))
        await redis.expire(k, 26 * 3600)
    except Exception:
        pass

async def _apply_moonshot_budget(c: Dict[str, Any], ticket_usd: float):
    if not MOONSHOT_ENABLED or not _is_moonshot_like(c):
        return True, ticket_usd, ""
    note = "moonshot"
    if ticket_usd > MOONSHOT_MAX_TRADE_USD:
        ticket_usd = MOONSHOT_MAX_TRADE_USD
        note += f" | trade capped ${MOONSHOT_MAX_TRADE_USD:.2f}"
    if _moonshot_mgr is not None:
        try:
            if not await _moonshot_mgr.allow_spend(ticket_usd):
                return False, ticket_usd, note + " | over daily cap"
        except Exception:
            pass
    else:
        spent = await _moonshot_spent_today()
        if spent + ticket_usd > MOONSHOT_DAILY_USD:
            return False, ticket_usd, note + f" | daily cap ${MOONSHOT_DAILY_USD:.2f}"
    await _moonshot_add_spent(ticket_usd)
    return True, ticket_usd, note

# ──────────────────────────────────────────────────────────────
# Scanner compatibility helpers
# ──────────────────────────────────────────────────────────────
async def _collect_scan(result) -> List[Dict[str, Any]]:
    """Normalize scanner outputs to List[Dict]."""
    # async iterator
    if hasattr(result, "__aiter__"):
        out: List[Dict[str, Any]] = []
        async for item in result:
            if isinstance(item, dict):
                out.append(item)
        return out
    # awaitable
    if asyncio.iscoroutine(result):
        try:
            resolved = await result
            return await _collect_scan(resolved)
        except Exception:
            return []
    # list
    if isinstance(result, list):
        return [x for x in result if isinstance(x, dict)]
    return []

async def _maybe_call_scanner(func: Optional[Callable], *args) -> List[Dict[str, Any]]:
    if func is None:
        return []
    tried = []
    for arity in (2, 1, 0):
        try:
            if arity == 2:
                tried.append("(redis, session)")
                return await _collect_scan(func(redis, session))  # type: ignore
            elif arity == 1:
                tried.append("(redis)")
                return await _collect_scan(func(redis))  # type: ignore
            else:
                tried.append("()")
                return await _collect_scan(func())  # type: ignore
        except TypeError:
            continue
        except Exception as e:
            logger.warning(f"[scanner:{getattr(func, '__name__', 'unknown')}] error: {e}")
            return []
    logger.warning(f"[scanner:{getattr(func, '__name__', 'unknown')}] no compatible signature among {tried}")
    return []

async def _scan_all_sources() -> List[Dict[str, Any]]:
    batches = await asyncio.gather(
        _maybe_call_scanner(scan_dexscreener),
        _maybe_call_scanner(scan_pumpfun),
        _maybe_call_scanner(scan_raydium),
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
    return out

# ──────────────────────────────────────────────────────────────
# Gates & helpers
# ──────────────────────────────────────────────────────────────
async def _basic_filters(c: Dict[str, Any]) -> bool:
    mint = c.get("mint")
    if not mint:
        return False
    if BLOCKLIST and mint in BLOCKLIST:
        return False
    if ALLOWLIST and len(ALLOWLIST) > 0 and mint not in ALLOWLIST:
        return False
    try:
        ok_hp, _ = await honeypot_check(mint)
        if not ok_hp:
            return False
        if await ai_fraud_score(mint) > 0.85:
            return False
    except Exception:
        return False
    return True

def _coalesce(src: Dict[str, Any], *keys, default=None):
    for k in keys:
        if k in src and src[k] is not None:
            return src[k]
    return default

# ──────────────────────────────────────────────────────────────
# Buy path
# ──────────────────────────────────────────────────────────────
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
                "entry(SOL/token)": f"{entry_price_sol_per_token:.10f}"
            })
            return False
        if DRY_RUN:
            await send_telegram(f"🧪 DRY-RUN — would buy {symbol} ({mint}) for ${ticket_usd:,.2f}")
            return False

        usd_per_sol = await convert_sol_to_usd(1.0)
        if not usd_per_sol or usd_per_sol <= 0:
            return False
        sol_to_spend = ticket_usd / usd_per_sol
        if sol_to_spend <= 0:
            return False

        # Route Guard precheck
        ok_rg, why, pre_meta = await precheck_buy(mint, sol_to_spend, SLIPPAGE_BPS)
        if not ok_rg:
            await send_telegram(
                f"⛔ RouteGuard block {symbol} ({mint}) — {why} • "
                f"hops={pre_meta.get('hops')} • impact_bps={pre_meta.get('impact_bps'):.0f}"
            )
            return False

        ok, sig = await swap_token_jupiter(
            input_mint=SOL_MINT,
            output_mint=mint,
            amount_float=sol_to_spend,
            slippage=JUPITER_SLIPPAGE,
        )
        if not ok:
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
        except Exception:
            pass
        await save_entry_price(redis, mint, entry_price_sol_per_token)
        await note_spend(None, float(ticket_usd))

        usd_est = await convert_sol_to_usd(sol_to_spend)
        await send_telegram(
            f"✅ SNIPED {symbol}\n"
            f"Spent: {sol_to_spend:.4f} SOL (~${usd_est or 0.0:.2f})\n"
            f"Entry (SOL/token): {entry_price_sol_per_token:.10f}\nMint: {mint}\nTx: {sig}"
        )
        try:
            await log_trade_event(token=mint, symbol=symbol, balance=sol_to_spend, entry=entry_price_sol_per_token,
                                  current=entry_price_sol_per_token, profit=0.0, reason="Entry", side="buy",
                                  signature=sig)
            await log_to_notion(token=mint, symbol=symbol, balance=sol_to_spend, entry=entry_price_sol_per_token,
                                current=entry_price_sol_per_token, profit=0.0, reason="Entry")
        except Exception:
            pass

        ok_wd, msg, now_meta = await posttrade_watchdog(mint, sol_to_spend, SLIPPAGE_BPS, pre_meta)
        if not ok_wd:
            await send_telegram(
                f"⚠️ Route watchdog: {symbol} ({mint}) {msg} • "
                f"pre_out={pre_meta.get('out_amount'):.0f} now_out={now_meta.get('out_amount', 0):.0f} "
                f"hops(pre/now)={pre_meta.get('hops')}/{now_meta.get('hops', 0)}"
            )
        return True
    finally:
        try:
            await release(mint)
        except Exception:
            pass

# ──────────────────────────────────────────────────────────────
# Sniper class
# ──────────────────────────────────────────────────────────────
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
        top = sorted([c for c in cands if c.get("mint")],
                     key=lambda x: float(x.get("liquidity_usd", 0) or 0),
                     reverse=True)[:8]
        mints = [c["mint"] for c in top]
        try:
            await prewarm_quotes(mints, amount_sol, SLIPPAGE_BPS)
        except Exception:
            pass

    async def _consider(self, c: Dict[str, Any], sl_params: Dict[str, Any]):
        if await _is_halted():
            return
        if not await should_allow_new_entry(None):
            return
        if not await _basic_filters(c):
            return

        mint = c.get("mint")
        symbol = c.get("symbol") or (mint[:6] if mint else "TKN")

        # AI buy gate (best-effort, tolerant to missing fields)
        try:
            pool_age_min = _coalesce(c, "pool_age_min", "poolAgeMin", "ageMin", default=None)
            liq_usd      = float(_coalesce(c, "liq_usd", "liquidity_usd", "liquidityUSD", default=0.0) or 0.0)
            impact1k_bps = float(_coalesce(c, "impact_1k_bps", "priceImpact1kBps", default=0.0) or 0.0)
            vol5m_pct    = float(_coalesce(c, "vol5m_pct", default=0.0) or 0.0)
            top10_pct    = float(_coalesce(c, "top10_pct", default=0.0) or 0.0)
            whale_hits1h = float(_coalesce(c, "whale_touches_1h", default=0.0) or 0.0)
            if Candidate:
                ok_ai, score_ai, why_ai = pass_gate(Candidate(
                    mint=mint, symbol=symbol,
                    pool_age_min=pool_age_min,
                    liq_usd=liq_usd, impact_1k_bps=impact1k_bps,
                    vol5m_pct=vol5m_pct, top10_pct=top10_pct,
                    honeypot_flag=False, whale_touches_1h=whale_hits1h
                ))
                if not ok_ai:
                    return
        except Exception:
            pass

        if WALLET_PUBKEY:
            try:
                if await get_sol_balance(WALLET_PUBKEY) <= MIN_SOL_BALANCE:
                    return
            except Exception:
                return

        limits = await current_limits(None)
        ticket = float(limits["ticket_usd"])

        ok_mshot, ticket, mnote = await _apply_moonshot_budget(c, ticket)
        if not ok_mshot or ticket <= 0:
            if mnote:
                await send_telegram(f"⏸️ Skipping {symbol} ({mint}) — {mnote}")
            return

        entry = await get_token_price(mint)
        if not entry or entry <= 0:
            return

        if self.sem.locked():
            return
        async with self.sem:
            await _buy_token(mint, symbol, float(ticket), float(entry))

    async def main_loop(self):
        await send_telegram("🎯 Sniper started (v6: prewarm + route_guard + watchdog).")
        # Start self-learning loop (non-blocking)
        try:
            from self_learning_ai_strategy import run_self_learning_loop
            asyncio.create_task(run_self_learning_loop(redis))
        except Exception:
            pass

        while not self.stop.is_set():
            try:
                if await _is_halted():
                    await asyncio.sleep(SCANNER_PERIOD_SEC)
                    continue

                try:
                    from self_learning_ai_strategy import get_strategy_params
                    sl_params = await get_strategy_params(redis)
                except Exception:
                    sl_params = {"SLIPPAGE": JUPITER_SLIPPAGE}

                cands = await _scan_all_sources()
                if not cands:
                    await asyncio.sleep(SCANNER_PERIOD_SEC)
                    continue

                # Prewarm quotes for top-liquidity mints
                limits = await current_limits(None)
                await self._prewarm(cands, float(limits["ticket_usd"]))

                random.shuffle(cands)
                for c in cands:
                    await self._consider(c, sl_params)

            except Exception as e:
                logger.error(f"[sniper] loop error: {e}", exc_info=True)
            await asyncio.sleep(SCANNER_PERIOD_SEC)

# ──────────────────────────────────────────────────────────────
# Entrypoint
# ──────────────────────────────────────────────────────────────
async def main():
    await init_resources()
    try:
        await Sniper().main_loop()
    finally:
        await close_resources()

if __name__ == "__main__":
    asyncio.run(main())
