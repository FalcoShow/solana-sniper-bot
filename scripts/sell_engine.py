# scripts/sell_engine.py
from __future__ import annotations

import asyncio
import importlib
import math
import os
import random
import time
from typing import Any, Callable, Dict, Optional, Tuple

from logging_setup import get_logger, bump

# Positions API (required: all(), remove(mint))
from utils_pkg.positions import all as all_positions  # noqa: N812
from utils_pkg.positions import remove as remove_position  # noqa: N812

# Router sell executor (Jupiter via Lightning-style router)
from utils_pkg.router import sell_via_lightning  # returns (ok: bool, sig: Optional[str])

# Guardrails: realized PnL record (buys can be halted upstream; sells always allowed)
try:
    from utils_pkg.guardrails import note_close  # type: ignore
except Exception:  # pragma: no cover
    def note_close(*_args, **_kwargs):  # type: ignore
        return None

log = get_logger()

# ──────────────────────────────────────────────────────────────────────────────
# Env helpers
# ──────────────────────────────────────────────────────────────────────────────
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

def _envb(k: str, d: bool = False) -> bool:
    v = (os.getenv(k, "") or "").strip().lower()
    if not v:
        return d
    return v in {"1", "true", "yes", "y", "on"}

# ──────────────────────────────────────────────────────────────────────────────
# Tunables
# ──────────────────────────────────────────────────────────────────────────────
TP1 = _envf("PF_TP_PCT", 0.0) / 100.0
TP2 = _envf("PF_TP2_PCT", _envf("EXIT_TP2_PCT", 0.0)) / 100.0
SL  = _envf("PF_SL_PCT", 0.0) / 100.0
TSL = _envf("PF_TSL_PCT", 0.0) / 100.0
TSL_ARM = _envf("PF_TSL_ARM_PCT", max(0.0, TSL * 100)) / 100.0
HARD_TIMEOUT = _envi("PF_HARD_TIMEOUT_SEC", 0)

# Break-even ratchet (after TP1)
BE_ENABLED    = _envb("PF_BE_ENABLE", True)
BE_AFTER_TP1  = _envf("PF_BE_AFTER_TP1_PCT", 0.0) / 100.0
BE_TRAIL      = _envf("PF_BE_TRAIL_PCT", 0.0) / 100.0

# Fractions per trigger (fallback to PF_SELL_FRAC)
SELL_FRAC_FALLBACK = max(0.0, min(1.0, _envf("PF_SELL_FRAC", 1.0)))
SELL_FRAC_TP   = max(0.0, min(1.0, _envf("PF_SELL_FRAC_TP", 0.35)))
SELL_FRAC_TP2  = max(0.0, min(1.0, _envf("PF_SELL_FRAC_TP2", 0.30)))
SELL_FRAC_TSL  = max(0.0, min(1.0, _envf("PF_SELL_FRAC_TSL", 1.00)))
SELL_FRAC_SL   = max(0.0, min(1.0, _envf("PF_SELL_FRAC_SL", 1.00)))
SELL_FRAC_BE   = max(0.0, min(1.0, _envf("PF_SELL_FRAC_BE", SELL_FRAC_FALLBACK)))

def _frac_for(reason: str) -> float:
    if reason == "TP1":
        return SELL_FRAC_TP if "PF_SELL_FRAC_TP" in os.environ else SELL_FRAC_FALLBACK
    if reason == "TP2":
        return SELL_FRAC_TP2 if "PF_SELL_FRAC_TP2" in os.environ else SELL_FRAC_FALLBACK
    if reason == "TSL":
        return SELL_FRAC_TSL if "PF_SELL_FRAC_TSL" in os.environ else SELL_FRAC_FALLBACK
    if reason == "SL":
        return SELL_FRAC_SL if "PF_SELL_FRAC_SL" in os.environ else SELL_FRAC_FALLBACK
    if reason == "BE":
        return SELL_FRAC_BE if "PF_SELL_FRAC_BE" in os.environ else SELL_FRAC_FALLBACK
    if reason == "TIMEOUT":
        return SELL_FRAC_SL if "PF_SELL_FRAC_SL" in os.environ else SELL_FRAC_FALLBACK
    return SELL_FRAC_FALLBACK

MIN_NOTIONAL_SOL = max(0.0, _envf("PF_MIN_SELL_SOL", 0.0))
SLIPPAGE_BPS     = _envi("PF_SLIPPAGE_BPS", 600)
PRIORITY_SOL     = _envf("PF_PRIORITY_SOL", 0.002)

LOOP_SEC              = max(2, _envi("PF_SELL_LOOP_SEC", 5))
RETRY_COOLDOWN_SEC    = max(5, _envi("PF_SELL_RETRY_SEC", 20))
PER_MINT_COOLDOWN_SEC = max(5, _envi("PF_SELL_MINT_COOLDOWN_SEC", 10))
PRICE_TTL_SEC         = max(1, _envi("PF_PRICE_TTL_SEC", 5))  # credit-friendly

MAX_CONC = _envi("PF_SELL_MAX_CONCURRENT", _envi("PF_MAX_CONCURRENT", 2))
DRY_RUN  = _envb("DRY_RUN", False)

# ──────────────────────────────────────────────────────────────────────────────
# Internal state
# ──────────────────────────────────────────────────────────────────────────────
_peaks: Dict[str, float] = {}            # % change since entry (for TSL/BE)
_in_flight: Dict[str, float] = {}        # mint -> started_at_ts
_last_attempt_at: Dict[str, float] = {}  # mint -> last_attempt_ts
_tp1_done: Dict[str, bool] = {}
_tp2_done: Dict[str, bool] = {}
_be_floor: Dict[str, float] = {}         # SOL per token floor after TP1

# Per-mint price cache to shrink quote/RPC load
_price_cache: Dict[str, Tuple[float, float]] = {}  # mint -> (ts, priceSOL)

# Optional hooks from utils_pkg.positions (works with positions_pg too)
_positions_mod = importlib.import_module("utils_pkg.positions")
_positions_reduce_qty: Optional[Callable[[str, int], None]] = getattr(_positions_mod, "decrement_qty", None)  # type: ignore[attr-defined]
_positions_update_after_fill: Optional[Callable[..., None]] = getattr(_positions_mod, "update_after_sell", None)  # type: ignore[attr-defined]

# Provide log.every if missing
if not hasattr(log, "every"):
    def _every(_key: str, n: int = 10, msg: Optional[str] = None, **kw):
        return log.event(msg or _key, **kw)
    setattr(log, "every", _every)  # type: ignore[attr-defined]

# ──────────────────────────────────────────────────────────────────────────────
# Price discovery (SOL per token) with cache
# ──────────────────────────────────────────────────────────────────────────────
async def _fetch_price_ui(mint: str, *, decimals_hint: Optional[int]) -> float:
    """
    Try router.get_price_ui / price_of_ui / price_of.
    Fallback: tiny sell-quote to derive px. Returns 0.0 on failure.
    """
    try:
        router = importlib.import_module("utils_pkg.router")
    except Exception:
        return 0.0

    # 1) direct getters on router
    for fname in ("get_price_ui", "price_of_ui", "price_of"):
        fn = getattr(router, fname, None)
        if callable(fn):
            try:
                if asyncio.iscoroutinefunction(fn):
                    return float(await fn(mint) or 0.0)  # type: ignore
                return float(fn(mint) or 0.0)           # type: ignore
            except Exception:
                pass

    # 2) tiny sell via router quote fallback
    q_fn = None
    for fname in ("get_sell_quote", "quote_sell", "sell_quote"):
        fn = getattr(router, fname, None)
        if callable(fn):
            q_fn = fn
            break
    if not q_fn:
        return 0.0

    decimals = int(decimals_hint or 6)
    decimals = max(0, min(18, decimals))
    tiny_raw = max(1, 10 ** max(0, decimals - 3))  # ≈0.001 token
    try:
        q = await q_fn(mint=mint, amount_raw=int(tiny_raw)) if asyncio.iscoroutinefunction(q_fn) else q_fn(mint=mint, amount_raw=int(tiny_raw))  # type: ignore
        # Prefer direct SOL field; else compute from lamports_out
        out_sol = (
            q.get("out_amount_sol")
            or q.get("out_sol")
            or q.get("outSOL")
            or (q.get("lamports_out", 0) / 1e9 if q.get("lamports_out") else 0.0)
            or 0.0
        )
        if not out_sol:
            return 0.0
        return float(out_sol) / (tiny_raw / (10 ** decimals))
    except Exception:
        return 0.0

async def _get_price_ui(mint: str, *, decimals_hint: Optional[int]) -> float:
    now = time.time()
    rec = _price_cache.get(mint)
    if rec and (now - rec[0]) <= PRICE_TTL_SEC:
        return rec[1]
    price = await _fetch_price_ui(mint, decimals_hint=decimals_hint)
    _price_cache[mint] = (now, price)
    return price

# ──────────────────────────────────────────────────────────────────────────────
# Decision logic
# ──────────────────────────────────────────────────────────────────────────────
def _decide_sell(
    *,
    mint: str,
    entry_price_sol: float,
    now_price_sol: float,
    opened_at: float,
) -> Tuple[bool, str, float]:
    if entry_price_sol <= 0 or now_price_sol <= 0:
        return False, "", 0.0

    change = (now_price_sol / entry_price_sol) - 1.0

    # Track peak % change for TSL & BE
    peak = _peaks.get(mint, change)
    if change > peak:
        peak = change
    _peaks[mint] = peak

    # TP2 → once
    if TP2 > 0 and (not _tp2_done.get(mint)) and change >= TP2:
        return True, "TP2", change

    # TP1 → once
    if TP1 > 0 and (not _tp1_done.get(mint)) and change >= TP1:
        return True, "TP1", change

    # Break-even ratchet (active after TP1)
    if BE_ENABLED and _tp1_done.get(mint):
        target_floor = entry_price_sol * (1.0 + max(0.0, BE_AFTER_TP1))
        if BE_TRAIL > 0.0:
            peak_px = entry_price_sol * (1.0 + max(0.0, peak))
            target_floor = max(target_floor, peak_px * (1.0 - BE_TRAIL))
        prev = _be_floor.get(mint, 0.0)
        floor_px = _be_floor[mint] = max(prev, target_floor)
        if now_price_sol <= floor_px:
            return True, "BE", change

    # TSL
    if TSL > 0 and peak >= TSL_ARM and (peak - change) >= TSL:
        return True, "TSL", change

    # SL
    if SL > 0 and change <= -SL:
        return True, "SL", change

    # Timeout
    if HARD_TIMEOUT > 0 and (time.time() - opened_at) >= HARD_TIMEOUT:
        return True, "TIMEOUT", change

    return False, "", change

# ──────────────────────────────────────────────────────────────────────────────
# Sell executor
# ──────────────────────────────────────────────────────────────────────────────
async def _execute_sell(p: Dict[str, Any], *, reason: str, price_sol: float) -> None:
    mint = p["mint"]
    token_program = p.get("token_program")
    qty_raw = int(p.get("qty_raw", 0))
    decimals = max(0, min(18, int(p.get("decimals", 0) or 0)))
    entry = float(p.get("avg_cost_sol", 0.0))
    qty_ui = float(p.get("qty_ui", 0.0)) or (qty_raw / (10 ** decimals) if decimals else 0.0)

    if qty_raw <= 0 or qty_ui <= 0.0:
        log.event("SELL_SKIP_EMPTY", mint=mint)
        return

    sell_frac = _frac_for(reason)
    if sell_frac <= 0.0:
        log.event("SELL_SKIP_ZERO_FRAC", mint=mint, reason=reason)
        return

    sell_raw = min(qty_raw, int(max(1, math.floor(qty_raw * sell_frac))))

    # min notional on selected slice
    sold_ui = (sell_raw / (10 ** decimals)) if decimals else qty_ui * (sell_raw / max(1, qty_raw))
    notional_sol = price_sol * sold_ui
    if notional_sol < MIN_NOTIONAL_SOL:
        log.event(
            "SELL_SKIP_SMALL",
            mint=mint,
            reason=reason,
            notional_sol=notional_sol,
            min_sol=MIN_NOTIONAL_SOL,
            sell_frac=sell_frac,
        )
        return

    # per-mint cooldown
    now = time.time()
    last = _last_attempt_at.get(mint, 0.0)
    if (now - last) < PER_MINT_COOLDOWN_SEC:
        log.event("SELL_COOLDOWN", mint=mint, secs_left=int(PER_MINT_COOLDOWN_SEC - (now - last)))
        return
    _last_attempt_at[mint] = now

    # est realized pnl for this slice
    est_realized_pnl = (price_sol - entry) * sold_ui

    if DRY_RUN:
        log.event(
            "SELL_DRY_RUN",
            mint=mint,
            reason=reason,
            sell_raw=sell_raw,
            sell_frac=sell_frac,
            slippage_bps=SLIPPAGE_BPS,
            priority_sol=PRIORITY_SOL,
            est_pnl_sol=est_realized_pnl,
        )
        return

    ok = False
    sig: Optional[str] = None
    try:
        ok, sig = await sell_via_lightning(
            mint=mint,
            token_program=token_program,
            amount_raw=sell_raw,
            slippage_bps=SLIPPAGE_BPS,
            priority_sol=PRIORITY_SOL,
        )
    except Exception as e:
        log.event("SELL_SUBMIT_ERROR", mint=mint, reason=reason, error=str(e))
        bump("sells_fail")
        return

    log.event(
        "SELL_SUBMIT",
        mint=mint,
        reason=reason,
        ok=ok,
        sig=sig,
        sell_raw=sell_raw,
        sell_frac=sell_frac,
    )

    if ok:
        try:
            note_close(est_realized_pnl)
        except Exception:
            pass

        remaining_raw = qty_raw - sell_raw
        closed = (remaining_raw <= 0) or (sell_frac >= 0.999)

        if reason == "TP1":
            _tp1_done[mint] = True
            if BE_ENABLED:
                _be_floor[mint] = max(_be_floor.get(mint, 0.0), entry * (1.0 + max(0.0, BE_AFTER_TP1)))
        elif reason == "TP2":
            _tp2_done[mint] = True

        try:
            if _positions_update_after_fill:
                _positions_update_after_fill(mint=mint, sold_raw=sell_raw, tx_sig=sig, closed=closed)  # type: ignore
            elif closed:
                remove_position(mint)
            elif _positions_reduce_qty:
                _positions_reduce_qty(mint, sell_raw)  # type: ignore
        except Exception:
            try:
                if closed:
                    remove_position(mint)
            except Exception:
                pass

        if closed:
            _tp1_done.pop(mint, None)
            _tp2_done.pop(mint, None)
            _peaks.pop(mint, None)
            _be_floor.pop(mint, None)
            _price_cache.pop(mint, None)

        bump("sells_ok")
    else:
        bump("sells_fail")

# ──────────────────────────────────────────────────────────────────────────────
# Worker
# ──────────────────────────────────────────────────────────────────────────────
async def _process_one(p: Dict[str, Any], sem: asyncio.Semaphore):
    mint = p.get("mint")
    if not mint or mint in _in_flight:
        return

    async with sem:
        _in_flight[mint] = time.time()
        try:
            entry = float(p.get("avg_cost_sol", 0.0))
            opened_at = float(p.get("opened_at", 0.0))
            decimals = max(0, min(18, int(p.get("decimals", 0) or 0)))

            price = await _get_price_ui(mint, decimals_hint=decimals)
            if price <= 0.0 or entry <= 0.0:
                return

            should, reason, change = _decide_sell(
                mint=mint, entry_price_sol=entry, now_price_sol=price, opened_at=opened_at
            )

            log.every(
                f"sell.check.{mint}",
                n=12,
                msg="SELL_CHECK",
                mint=mint,
                price_sol=price,
                entry_sol=entry,
                change=change,
                peak=_peaks.get(mint),
                be_floor=_be_floor.get(mint),
                tp1_done=int(_tp1_done.get(mint, False)),
                tp2_done=int(_tp2_done.get(mint, False)),
            )

            if not should:
                return

            qty_raw = int(p.get("qty_raw", 0))
            qty_ui = float(p.get("qty_ui", 0.0)) or (qty_raw / (10 ** decimals) if decimals else 0.0)
            sell_frac = _frac_for(reason)
            est_pnl = (price - entry) * qty_ui * sell_frac
            log.event(
                "SELL_DECISION",
                mint=mint,
                reason=reason,
                price_sol=price,
                entry_sol=entry,
                change=change,
                sell_frac=sell_frac,
                est_pnl_sol=est_pnl,
            )

            await _execute_sell(p, reason=reason, price_sol=price)

        finally:
            _in_flight.pop(mint, None)

# ──────────────────────────────────────────────────────────────────────────────
# Public loop
# ──────────────────────────────────────────────────────────────────────────────
async def run_sell_loop(stop_evt: asyncio.Event):
    sem = asyncio.Semaphore(max(1, MAX_CONC))
    cooldown_until = 0.0

    log.event(
        "SELL_LOOP_START",
        tp1_pct=TP1 * 100,
        tp2_pct=TP2 * 100,
        sl_pct=SL * 100,
        tsl_pct=TSL * 100,
        tsl_arm_pct=TSL_ARM * 100,
        be_enable=int(BE_ENABLED),
        be_after_tp1_pct=BE_AFTER_TP1 * 100,
        be_trail_pct=BE_TRAIL * 100,
        timeout_sec=HARD_TIMEOUT,
        sell_frac_default=SELL_FRAC_FALLBACK,
        sell_frac_tp=SELL_FRAC_TP,
        sell_frac_tp2=SELL_FRAC_TP2,
        sell_frac_tsl=SELL_FRAC_TSL,
        sell_frac_sl=SELL_FRAC_SL,
        sell_frac_be=SELL_FRAC_BE,
        min_notional_sol=MIN_NOTIONAL_SOL,
        loop_sec=LOOP_SEC,
        price_ttl_sec=PRICE_TTL_SEC,
        max_concurrent=MAX_CONC,
        slippage_bps=SLIPPAGE_BPS,
        priority_sol=PRIORITY_SOL,
        dry_run=int(DRY_RUN),
    )

    while not stop_evt.is_set():
        now = time.time()
        if now < cooldown_until:
            await asyncio.sleep(LOOP_SEC)
            continue

        try:
            positions = list(all_positions())
        except Exception as e:
            log.event("SELL_POSITIONS_ERROR", error=str(e))
            cooldown_until = time.time() + RETRY_COOLDOWN_SEC
            await asyncio.sleep(LOOP_SEC)
            continue

        tasks = []
        for p in positions:
            mint = p.get("mint")
            if not mint:
                continue
            last = _last_attempt_at.get(mint, 0.0)
            if (now - last) < PER_MINT_COOLDOWN_SEC:
                continue
            tasks.append(asyncio.create_task(_process_one(p, sem)))

        if tasks:
            try:
                await asyncio.gather(*tasks, return_exceptions=True)
            except Exception as e:
                log.event("SELL_GATHER_ERROR", error=str(e))
                cooldown_until = time.time() + RETRY_COOLDOWN_SEC

        # jitter the loop slightly so many dynos don’t sync up
        await asyncio.sleep(LOOP_SEC + random.uniform(0, 0.75))

    log.event("SELL_LOOP_STOP")
