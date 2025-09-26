# execution/pnl_watcher.py — v8 (glitch-guarded equity + light price cache)
from __future__ import annotations

import os
import asyncio
import logging
import json
import math
import time
from typing import Dict, Any, Optional, Tuple
from datetime import datetime, timezone

# ---- prices, wallet, redis, telegram
try:
    from utils_pkg.core_utils import (
        get_token_price,
        get_sol_balance,
        convert_sol_to_usd,
        redis as _redis,
        send_telegram,
    )
except Exception:
    _redis = None
    async def get_token_price(_mint: str) -> float: return 0.0
    async def get_sol_balance(_pub: str) -> float: return 0.0
    async def convert_sol_to_usd(x: float) -> float: return 0.0
    async def send_telegram(_msg: str): pass

# ---- exits logic
try:
    from execution.exits import PositionState, decide_exit, apply_exit
except Exception:
    # Minimal safe stubs so watcher can still heartbeat
    class PositionState:  # type: ignore
        def __init__(self, mint: str, entry_px: float, size_usd: float):
            self.mint, self.entry_px, self.size_usd = mint, float(entry_px), float(size_usd)
            self.peak_px, self.tp1_done, self.symbol = self.entry_px, False, None
    def decide_exit(_pos, _last_px):  # type: ignore
        class R: action = "HOLD"
        return R()
    async def apply_exit(pos, last_px, sell_fn):  # type: ignore
        class Res: ok=True; reason="hold"; signature=""; action="HOLD"
        return Res()

# ---- sell function (USD -> swap)
try:
    from execution.sell_fn import sell_token_usd
except Exception:
    async def sell_token_usd(mint: str, usd_to_sell: float, reason: str, last_px: Optional[float] = None):
        return False, "", {"stub": True}

# ---- DB logs (tiny PnL line per exit + heartbeat)
_log_trade_row = None
_log_trade_event = None
_log_pnl_snapshot = None
try:
    from db.logger import log_trade as _log_trade_row  # (mint, entry_px, exit_px, size_usd, pnl_usd, reason)
except Exception:
    pass
try:
    from db.logger import log_trade_event as _log_trade_event  # (dict)
except Exception:
    pass
try:
    from db.logger import log_pnl_snapshot as _log_pnl_snapshot  # (when_iso, equity_usd, open_risk_usd)
except Exception:
    pass

# ---- optionally import canonical keys from positions.py (if present)
try:
    from positions import REDIS_INDEX_KEY as _IDX, REDIS_POS_PREFIX as _PFX
except Exception:
    _IDX, _PFX = "positions:index", "pos:"

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
log = logging.getLogger("pnl_watcher")
log.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

SCAN_SEC = int(os.getenv("PNL_SCAN_SEC", "5"))
PNL_HEARTBEAT_N = int(os.getenv("PNL_HEARTBEAT_N", "6"))  # ~30s if SCAN_SEC=5

# Guards / smoothing
PNL_PRICE_CACHE_TTL_SEC   = int(os.getenv("PNL_PRICE_CACHE_TTL_SEC", "20"))
PNL_ZERO_CONFIRM_STREAK   = int(os.getenv("PNL_ZERO_CONFIRM_STREAK", "3"))
PNL_WALLET_MIN_VALID_USD  = float(os.getenv("PNL_WALLET_MIN_VALID_USD", "5"))

# Redis-only watcher (same behavior as your v7)
PNL_REDIS_ONLY = os.getenv("PNL_REDIS_ONLY", "true").strip().lower() in ("1","true","yes","on")

# Telegram heartbeat controls
PNL_TELEGRAM_HEARTBEAT      = os.getenv("PNL_TELEGRAM_HEARTBEAT", "false").lower() == "true"
PNL_TELEGRAM_ONLY_ON_CHANGE = os.getenv("PNL_TELEGRAM_ONLY_ON_CHANGE", "false").lower() == "true"
PNL_TELEGRAM_DELTA_USD      = float(os.getenv("PNL_TELEGRAM_DELTA_USD", "100"))
PNL_TELEGRAM_DELTA_PCT      = float(os.getenv("PNL_TELEGRAM_DELTA_PCT", "2"))
PNL_TELEGRAM_DELTA_UPNL_USD = float(os.getenv("PNL_TELEGRAM_DELTA_UPNL_USD", "0"))
PNL_TELEGRAM_FORCE_EVERY_N  = int(os.getenv("PNL_TELEGRAM_FORCE_EVERY_N", "0"))

PNL_BLIP_STATE_TTL_HOURS    = int(os.getenv("PNL_BLIP_STATE_TTL_HOURS", "48"))

EPS_USD = 1e-6

WALLET_PUBKEY = os.getenv("WALLET_PUBKEY") or os.getenv("WALLET_PUBLIC_KEY")
EQUITY_USD_FALLBACK = float(os.getenv("EQUITY_USD_FALLBACK", "2000"))

# ---------- Redis helpers ----------

def _redis_ok() -> bool:
    return _redis is not None

async def _r_get(key: str) -> Optional[str]:
    try: return await _redis.get(key)  # type: ignore
    except Exception: return None

async def _r_set(key: str, val: str) -> None:
    try: await _redis.set(key, val)  # type: ignore
    except Exception: pass

async def _r_expire(key: str, seconds: int) -> None:
    try: await _redis.expire(key, seconds)  # type: ignore
    except Exception: pass

async def _r_set_with_ttl(key: str, val: str, seconds: int) -> None:
    await _r_set(key, val)
    if seconds > 0:
        await _r_expire(key, seconds)

async def _r_smembers(key: str):
    try: return await _redis.smembers(key)  # type: ignore
    except Exception: return set()

async def _r_sadd(key: str, member: str) -> None:
    try: await _redis.sadd(key, member)  # type: ignore
    except Exception: pass

async def _r_srem(key: str, member: str) -> None:
    try: await _redis.srem(key, member)  # type: ignore
    except Exception: pass

async def _r_del(key: str) -> None:
    try: await _redis.delete(key)  # type: ignore
    except Exception: pass

# ---------- state adapters ----------

def _dict_to_state(token: str, d: Dict[str, Any]) -> PositionState:
    entry_px = float(d.get("entry_px") or d.get("buy_price") or 0.0)  # SOL/token
    size_usd = float(d.get("size_usd") or d.get("amount_invested") or 0.0)
    pos = PositionState(mint=token, entry_px=entry_px, size_usd=size_usd)
    pos.peak_px = float(d.get("peak_px", pos.entry_px))
    pos.tp1_done = bool(d.get("tp1_done", False))
    sym = d.get("symbol")
    if sym: setattr(pos, "symbol", sym)
    return pos

def _state_to_patch(pos: PositionState) -> Dict[str, Any]:
    patch = {
        "token_address": pos.mint,
        "buy_price": float(pos.entry_px),
        "amount_invested": float(pos.size_usd),
        "peak_px": float(getattr(pos, "peak_px", pos.entry_px)),
        "tp1_done": bool(getattr(pos, "tp1_done", False)),
    }
    sym = getattr(pos, "symbol", None)
    if sym: patch["symbol"] = sym
    return patch

# ---------- load/save/close (Redis only) ----------

async def _load_positions_redis() -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    members = await _r_smembers(_IDX)
    for m in members or []:
        token = str(m)
        raw = await _r_get(f"{_PFX}{token}")
        if not raw: continue
        try:
            out[token] = json.loads(raw)
        except Exception:
            continue
    return out

async def _save_position_redis(token: str, patch: Dict[str, Any]) -> None:
    cur_raw = await _r_get(f"{_PFX}{token}")
    cur = {}
    if cur_raw:
        try: cur = json.loads(cur_raw)
        except Exception: cur = {}
    cur.update(patch)
    cur.setdefault("token_address", token)
    await _r_set(f"{_PFX}{token}", json.dumps(cur, separators=(",", ":")))
    await _r_sadd(_IDX, token)

async def _remove_position_redis(token: str) -> None:
    await _r_del(f"{_PFX}{token}")
    await _r_srem(_IDX, token)

# ---------- tiny DB PnL log for each exit ----------

async def _tiny_db_log_exit(
    *,
    mint: str,
    symbol: Optional[str],
    entry_px: float,
    exit_px: float,
    usd_sold: float,
    reason: str,
    ok: bool,
    sig: str = "",
) -> None:
    pnl_usd = ((exit_px / entry_px) - 1.0) * float(usd_sold) if entry_px > 0 else 0.0

    if _log_trade_row:
        try:
            await _log_trade_row(mint, entry_px, exit_px, float(usd_sold), float(pnl_usd), reason)
            return
        except Exception:
            pass

    if _log_trade_event:
        try:
            await _log_trade_event({
                "type": "sell",
                "mint": mint,
                "symbol": symbol,
                "entry_px": float(entry_px),
                "exit_px": float(exit_px),
                "usd_sold": float(usd_sold),
                "pnl_usd": float(pnl_usd),
                "reason": reason,
                "ok": bool(ok),
                "signature": sig,
            })
        except Exception:
            pass

# ---------- heartbeat baseline persistence ----------

BLIP_EQ_KEY   = "pnl:last_blip:equity"
BLIP_UPNL_KEY = "pnl:last_blip:upnl"
BLIP_SINCE_KEY= "pnl:last_blip:since_n"
_BLIP_TTL_SEC = max(0, PNL_BLIP_STATE_TTL_HOURS) * 3600

_last_blip_equity: Optional[float] = None
_last_blip_upnl: Optional[float] = None
_since_last_forced: int = 0

async def _load_blip_state() -> None:
    """Load last-blip baseline from Redis (if present)."""
    global _last_blip_equity, _last_blip_upnl, _since_last_forced
    try:
        raw_eq = await _r_get(BLIP_EQ_KEY)
        raw_up = await _r_get(BLIP_UPNL_KEY)
        raw_sn = await _r_get(BLIP_SINCE_KEY)
        if raw_eq is not None:
            _last_blip_equity = float(raw_eq)
        if raw_up is not None:
            _last_blip_upnl = float(raw_up)
        if raw_sn is not None:
            _since_last_forced = int(raw_sn)
    except Exception:
        pass

async def _save_blip_state() -> None:
    """Persist current baseline to Redis with TTL."""
    try:
        if _last_blip_equity is not None:
            await _r_set_with_ttl(BLIP_EQ_KEY, str(float(_last_blip_equity)), _BLIP_TTL_SEC)
        if _last_blip_upnl is not None:
            await _r_set_with_ttl(BLIP_UPNL_KEY, str(float(_last_blip_upnl)), _BLIP_TTL_SEC)
        await _r_set_with_ttl(BLIP_SINCE_KEY, str(int(_since_last_forced)), _BLIP_TTL_SEC)
    except Exception:
        pass

# ---------- light price cache (avoid 0 on transient hiccups) ----------

_PRICE_CACHE: Dict[str, Tuple[float, float]] = {}  # mint -> (price, ts)

def _now() -> float:
    return time.time()

def _is_num(x: Any) -> bool:
    try:
        fx = float(x)
        return math.isfinite(fx)
    except Exception:
        return False

async def _get_price_safe(mint: str) -> Optional[float]:
    """Fetch price with small cache/TTL; ignore zeros/None by falling back to cache."""
    px = None
    try:
        px = await get_token_price(mint)
    except Exception:
        px = None

    if _is_num(px) and px and px > 0:
        _PRICE_CACHE[mint] = (float(px), _now())
        return float(px)

    # fallback to cache if fresh
    cached = _PRICE_CACHE.get(mint)
    if cached and (_now() - cached[1]) <= PNL_PRICE_CACHE_TTL_SEC:
        return float(cached[0])
    return None

# ---------- wallet smoothing (guard against 0/None spikes) ----------

_last_good_wallet_usd: float = 0.0
_wallet_zero_streak: int = 0

async def _wallet_usd_smoothed() -> float:
    global _last_good_wallet_usd, _wallet_zero_streak
    try:
        usd_per_sol = await convert_sol_to_usd(1.0) or 0.0
        sol_bal = await get_sol_balance(WALLET_PUBKEY) if WALLET_PUBKEY else 0.0
        candidate = float(usd_per_sol) * float(sol_bal)
    except Exception:
        candidate = 0.0

    if not _is_num(candidate) or candidate < PNL_WALLET_MIN_VALID_USD:
        # treat as glitch if we recently had a valid positive wallet
        if _last_good_wallet_usd > 0 and _wallet_zero_streak < PNL_ZERO_CONFIRM_STREAK:
            _wallet_zero_streak += 1
            return _last_good_wallet_usd
        _wallet_zero_streak = min(_wallet_zero_streak + 1, PNL_ZERO_CONFIRM_STREAK)
        _last_good_wallet_usd = 0.0 if _wallet_zero_streak >= PNL_ZERO_CONFIRM_STREAK else _last_good_wallet_usd
        return _last_good_wallet_usd
    else:
        _wallet_zero_streak = 0
        _last_good_wallet_usd = float(candidate)
        return _last_good_wallet_usd

# ---------- heartbeat helpers ----------

async def _should_blip(equity_usd: float, unrealized_pnl_usd: float) -> bool:
    """
    Decide and update baseline; persist to Redis.
    """
    global _last_blip_equity, _last_blip_upnl, _since_last_forced

    # first heartbeat -> set baseline and send
    if _last_blip_equity is None:
        _last_blip_equity = float(equity_usd)
        _last_blip_upnl = float(unrealized_pnl_usd)
        _since_last_forced = 0
        await _save_blip_state()
        return True

    delta_usd = float(equity_usd) - float(_last_blip_equity)
    pct = (delta_usd / abs(_last_blip_equity)) * 100.0 if abs(_last_blip_equity) > 1e-9 else 0.0
    upnl_delta = float(unrealized_pnl_usd) - float(_last_blip_upnl or 0.0)

    hit = False
    if abs(delta_usd) >= PNL_TELEGRAM_DELTA_USD:
        hit = True
    if abs(pct) >= PNL_TELEGRAM_DELTA_PCT:
        hit = True
    if PNL_TELEGRAM_DELTA_UPNL_USD > 0 and abs(upnl_delta) >= PNL_TELEGRAM_DELTA_UPNL_USD:
        hit = True

    if not hit and PNL_TELEGRAM_FORCE_EVERY_N > 0 and _since_last_forced >= PNL_TELEGRAM_FORCE_EVERY_N:
        hit = True  # force a keep-alive blip

    if not PNL_TELEGRAM_ONLY_ON_CHANGE:
        hit = True  # always send on heartbeat cadence

    if hit:
        _last_blip_equity = float(equity_usd)
        _last_blip_upnl = float(unrealized_pnl_usd)
        _since_last_forced = 0
        await _save_blip_state()
    else:
        _since_last_forced += 1
        await _save_blip_state()

    return hit

async def _emit_heartbeat(now_iso: str, equity_usd: float, open_cost_usd: float,
                          open_mv_usd: float, unrealized_pnl_usd: float,
                          wallet_usd: float, open_count: int) -> None:
    # Always persist structured heartbeat to DB logs if available
    if _log_pnl_snapshot:
        try:
            await _log_pnl_snapshot(now_iso, float(equity_usd), float(open_cost_usd))
        except Exception:
            pass
    if _log_trade_event:
        try:
            await _log_trade_event({
                "type": "pnl_heartbeat",
                "ts": now_iso,
                "equity_usd": float(equity_usd),
                "wallet_usd": float(wallet_usd),
                "open_cost_usd": float(open_cost_usd),
                "open_mv_usd": float(open_mv_usd),
                "unrealized_pnl_usd": float(unrealized_pnl_usd),
                "open_positions": int(open_count),
            })
        except Exception:
            pass

    # Telegram blip with thresholds
    if PNL_TELEGRAM_HEARTBEAT:
        try:
            prev_eq = _last_blip_equity if _last_blip_equity is not None else equity_usd
            d_usd = equity_usd - prev_eq
            d_pct = (d_usd / abs(prev_eq)) * 100.0 if abs(prev_eq) > 1e-9 else 0.0
            if await _should_blip(equity_usd, unrealized_pnl_usd):
                msg = (
                    f"📈 Heartbeat | equity ${equity_usd:,.0f} "
                    f"(Δ {d_usd:+,.0f}, {d_pct:+.2f}%) | "
                    f"open MV ${open_mv_usd:,.0f} | uPnL {unrealized_pnl_usd:+,.0f} | "
                    f"open {open_count}"
                )
                await send_telegram(msg)
        except Exception:
            pass

# ---------- main watcher loop ----------

async def watch_positions_loop():
    if PNL_REDIS_ONLY and not _redis_ok():
        log.warning("PnL watcher: Redis not available; cannot run Redis-only watcher.")
        return

    log.info(
        "PnL watcher started — scan=%ss, heartbeat=%dx, price_cache_ttl=%ss, zero_confirm=%dx, "
        "wallet_min_valid_usd=%.2f, telegram=%s/%s",
        SCAN_SEC, PNL_HEARTBEAT_N, PNL_PRICE_CACHE_TTL_SEC, PNL_ZERO_CONFIRM_STREAK,
        PNL_WALLET_MIN_VALID_USD, PNL_TELEGRAM_HEARTBEAT, ("on_change" if PNL_TELEGRAM_ONLY_ON_CHANGE else "every")
    )

    # load baseline so restarts keep the delta reference
    await _load_blip_state()

    tick = 0
    while True:
        try:
            store = await _load_positions_redis() if _redis_ok() else {}
            open_count = len(store or {})
            # position pass (also gather MV/pnl for heartbeat)
            open_cost_usd = 0.0
            open_mv_usd = 0.0
            unrealized_pnl_usd = 0.0

            if store:
                for mint, pdata in list(store.items()):
                    try:
                        pos = _dict_to_state(mint, pdata)
                        if pos.entry_px <= 0 or pos.size_usd <= EPS_USD:
                            continue

                        last_px = await _get_price_safe(mint)   # SOL/token
                        if not last_px or last_px <= 0:
                            # no price this tick → skip PnL contrib, keep position as-is
                            continue

                        # accumulate PnL/MV for heartbeat
                        open_cost_usd += float(pos.size_usd)
                        mv = float(pos.size_usd) * float(last_px / pos.entry_px)
                        open_mv_usd += mv
                        unrealized_pnl_usd += (mv - float(pos.size_usd))

                        decision = decide_exit(pos, last_px)
                        if getattr(decision, "action", "HOLD") == "HOLD":
                            continue

                        before_usd = float(pos.size_usd)

                        async def sell_fn(m: str, usd: float, reason: str):
                            return await sell_token_usd(m, usd, reason, last_px)

                        result = await apply_exit(pos, last_px, sell_fn)

                        sold_usd = max(0.0, before_usd - float(pos.size_usd))
                        await _tiny_db_log_exit(
                            mint=mint,
                            symbol=getattr(pos, "symbol", None),
                            entry_px=float(pos.entry_px),
                            exit_px=float(last_px),
                            usd_sold=float(sold_usd),
                            reason=str(getattr(result, "reason", "exit")),
                            ok=bool(getattr(result, "ok", True)),
                            sig=str(getattr(result, "signature", "")),
                        )

                        if getattr(result, "action", "") == "SELL_ALL" or pos.size_usd <= EPS_USD:
                            await _remove_position_redis(mint)
                            log.info("closed %s (%s) via %s", getattr(pos, "symbol", ""), mint, getattr(result, "reason", "exit"))
                            open_count = max(0, open_count - 1)
                        else:
                            await _save_position_redis(mint, _state_to_patch(pos))

                    except Exception as e:
                        log.debug("watch error on %s: %s", mint, e)

            # heartbeat
            tick += 1
            if tick % PNL_HEARTBEAT_N == 0:
                wallet_usd = await _wallet_usd_smoothed()

                # Equity smoothing: if everything vanished but we recently had a value,
                # keep the last good wallet estimate (handled in _wallet_usd_smoothed).
                equity_usd = float(wallet_usd or 0.0) + float(open_mv_usd or 0.0)
                if equity_usd <= 0 and EQUITY_USD_FALLBACK > 0 and _last_good_wallet_usd <= 0:
                    # absolute fallback for fresh cold start without wallet key
                    equity_usd = EQUITY_USD_FALLBACK

                now_iso = datetime.now(timezone.utc).isoformat()

                await _emit_heartbeat(
                    now_iso=now_iso,
                    equity_usd=float(equity_usd),
                    open_cost_usd=float(open_cost_usd),
                    open_mv_usd=float(open_mv_usd),
                    unrealized_pnl_usd=float(unrealized_pnl_usd),
                    wallet_usd=float(wallet_usd or 0.0),
                    open_count=int(open_count),
                )

        except Exception as e:
            log.warning("pnl_watcher loop error: %s", e)

        await asyncio.sleep(SCAN_SEC)

# Convenience wrapper used by some entrypoints
async def start_pnl_watcher():
    await watch_positions_loop()

__all__ = ["watch_positions_loop", "start_pnl_watcher"]
