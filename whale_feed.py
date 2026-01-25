# whale_feed.py
from __future__ import annotations

"""
whale_feed.py (P0 — production)
==============================

Purpose
-------
"Whale feed" watches high-liquidity / high-activity pairs and detects
*clustered buy bursts* + short-window momentum (a proxy for whale-style flow).

SAFE DEFAULT:
  - QUEUE mode: publish candidates to Redis for the canonical sniper/entry pipeline.

OPTIONAL:
  - TRADE mode: execute buys directly via Jupiter (only if WHALE_FEED_MODE=TRADE).

P0 Lock-in Principles
---------------------
- Safe-by-default (no trading unless explicitly enabled)
- No competing execution engines by default (QUEUE feeds canonical path)
- Guardrails:
    * allow-entry gate (risk_manager if available)
    * liquidity + estimated impact caps
    * buy-cluster gate (buys over a window)
    * optional momentum gates (vol/price change)
    * cooldown + distributed lock (Redis)
    * optional route safety checks (route_guard)
- Observability: logs + optional Telegram

This worker intentionally DOES NOT attempt to own positions logic.
"""

import asyncio
import contextlib
import inspect
import json
import logging
import os
import random
import signal
import time
from dataclasses import asdict, dataclass
from typing import Any

import aiohttp


# =============================================================================
# Logging
# =============================================================================
def _setup_logging() -> None:
    # Prefer canonical project logger if available; do not crash if missing.
    with contextlib.suppress(Exception):
        from logging_setup import setup_logging  # type: ignore

        setup_logging(service="whale_feed")
        return

    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s | %(levelname)s | %(name)s: %(message)s",
    )


_setup_logging()
logger = logging.getLogger("whale")


# =============================================================================
# Optional integrations (must not crash if missing)
# =============================================================================
try:
    from utils_pkg.core_utils import (  # type: ignore
        close_resources,
        convert_sol_to_usd,
        get_redis_async,
        init_resources,
        send_telegram,
        swap_token_jupiter,
    )
except Exception:  # pragma: no cover
    init_resources = None
    close_resources = None
    get_redis_async = None
    send_telegram = None
    convert_sol_to_usd = None
    swap_token_jupiter = None

try:
    from risk_manager import (  # type: ignore
        current_limits,
        note_spend,
        should_allow_new_entry,
    )
except Exception:  # pragma: no cover
    current_limits = None
    should_allow_new_entry = None
    note_spend = None

try:
    from shadow_utils import shadow_log, shadow_take  # type: ignore
except Exception:  # pragma: no cover
    shadow_take = None
    shadow_log = None

try:
    from route_guard import posttrade_watchdog, precheck_buy  # type: ignore
except Exception:  # pragma: no cover
    precheck_buy = None
    posttrade_watchdog = None

try:
    from jup_quote_cache import prewarm_quotes  # type: ignore
except Exception:  # pragma: no cover
    prewarm_quotes = None


# =============================================================================
# Env / constants
# =============================================================================
SOL_MINT = "So11111111111111111111111111111111111111112"

DEX_SEARCH_API = os.getenv(
    "WHALE_DEX_SEARCH_API",
    "https://api.dexscreener.com/latest/dex/search?q=sol",
)


def _env_bool(name: str, default: str = "0") -> bool:
    v = os.getenv(name, default)
    if v is None:
        return default.strip().lower() in ("1", "true", "yes", "y", "on")
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "on")


def _env_int(name: str, default: int, *, lo: int | None = None, hi: int | None = None) -> int:
    raw = os.getenv(name, "")
    try:
        val = int(raw.strip()) if raw.strip() else int(default)
    except Exception:
        val = int(default)
    if lo is not None:
        val = max(lo, val)
    if hi is not None:
        val = min(hi, val)
    return val


def _env_float(
    name: str, default: float, *, lo: float | None = None, hi: float | None = None
) -> float:
    raw = os.getenv(name, "")
    try:
        val = float(raw.strip()) if raw.strip() else float(default)
    except Exception:
        val = float(default)
    if lo is not None:
        val = max(lo, val)
    if hi is not None:
        val = min(hi, val)
    return val


WHALE_FEED_ENABLED = _env_bool("WHALE_FEED_ENABLED", "1")

# SAFE DEFAULT: publish to queue (no direct execution)
WHALE_FEED_MODE = os.getenv("WHALE_FEED_MODE", "QUEUE").strip().upper()  # QUEUE | TRADE
if WHALE_FEED_MODE not in ("QUEUE", "TRADE"):
    WHALE_FEED_MODE = "QUEUE"

POLL_SEC = _env_int("WHALE_POLL_SEC", 20, lo=3, hi=300)
JITTER_SEC = _env_float("WHALE_JITTER_SEC", 1.0, lo=0.0, hi=10.0)

COOLDOWN_SEC = _env_int("WHALE_ENTRY_COOLDOWN_SEC", 300, lo=0, hi=3600)
LOCK_TTL_SEC = _env_int("WHALE_LOCK_TTL_SEC", 120, lo=10, hi=600)

MAX_SCAN_PAIRS = _env_int("WHALE_MAX_SCAN_PAIRS", 60, lo=10, hi=500)
MAX_EMIT_PER_TICK = _env_int("WHALE_MAX_EMIT_PER_TICK", 20, lo=1, hi=250)  # queue spam control
MAX_TRADES_PER_TICK = _env_int("WHALE_MAX_TRADES_PER_TICK", 1, lo=0, hi=10)  # trade mode only

# Gates
MIN_LIQ_USD = _env_float("AI_MIN_LIQ_USD", 20_000.0, lo=0.0)
MAX_IMPACT_BPS = _env_float(
    "WHALE_MAX_IMPACT_BPS", _env_float("AI_MAX_IMPACT_BPS", 150.0, lo=1.0), lo=1.0
)

# "Honeypot-ish" sell tax gate (Dexscreener info.sellTax best-effort)
HONEYPOT_MAX_SELL_TAX_BPS = _env_float("HONEYPOT_MAX_SELL_TAX_BPS", 800.0, lo=0.0)

# Whale cluster gate (buy burst)
CLUSTER_MIN_BUYS = _env_int("WHALE_CLUSTER_MIN_BUYS", 2, lo=1, hi=50)
CLUSTER_WINDOW_MIN = _env_int("WHALE_CLUSTER_WINDOW_MIN", 5, lo=1, hi=1440)

# Optional momentum filters (helps avoid dead coins)
WHALE_MIN_VOL_M5_USD = _env_float("WHALE_MIN_VOL_M5_USD", 1500.0, lo=0.0)
WHALE_MIN_PRICECHANGE_M5 = _env_float("WHALE_MIN_PRICECHANGE_M5", 0.5, lo=-100.0, hi=10_000.0)

# Scoring (bounded 0..1)
WHALE_SCORE_MIN = _env_float("WHALE_SCORE_MIN", 0.60, lo=0.0, hi=1.0)

# Slippage (trade mode)
JUPITER_MAX_SLIPPAGE = _env_float(
    "JUPITER_MAX_SLIPPAGE",
    _env_float("JUPITER_SLIPPAGE", 0.005, lo=0.0001, hi=0.50),
    lo=0.0001,
    hi=0.50,
)
SLIPPAGE_BPS = max(1, int(JUPITER_MAX_SLIPPAGE * 10_000))

# Publishing (queue mode)
REDIS_QUEUE_KEY = os.getenv("WHALE_QUEUE_KEY", "entry_queue:whale")
REDIS_COOLDOWN_KEY_PREFIX = os.getenv("WHALE_COOLDOWN_KEY_PREFIX", "cd:whale:")
REDIS_LOCK_KEY_PREFIX = os.getenv("WHALE_LOCK_KEY_PREFIX", "lock:whale:")

# Quote prewarm
PREWARM_ENABLED = _env_bool("WHALE_PREWARM_ENABLED", "1")
PREWARM_TOP_N = _env_int("WHALE_PREWARM_TOP_N", 8, lo=0, hi=50)

# Notifications
TELEGRAM_ON_START = _env_bool("WHALE_TELEGRAM_ON_START", "1")
TELEGRAM_ON_CANDIDATE = _env_bool("WHALE_TELEGRAM_ON_CANDIDATE", "0")
TELEGRAM_ON_TRADE = _env_bool("WHALE_TELEGRAM_ON_TRADE", "1")

# Shadow
WHALE_SHADOW = _env_bool("WHALE_SHADOW", "0")


# =============================================================================
# Data model
# =============================================================================
@dataclass(frozen=True, slots=True)
class WhaleCandidate:
    ts: float
    source: str
    mint: str
    symbol: str
    pair_url: str

    liquidity_usd: float
    vol_m5_usd: float
    price_change_m5: float

    buys_win: int
    sells_win: int
    window_min: int

    sell_tax_bps: float
    est_impact_bps: float
    score: float

    raw: dict[str, Any]


# =============================================================================
# Generic helpers
# =============================================================================
def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


async def _call_maybe_async(fn: Any, *args: Any, **kwargs: Any) -> Any:
    """
    Call `fn` that might be sync or async. Never raises; callers decide.
    """
    res = fn(*args, **kwargs)
    if inspect.isawaitable(res):
        return await res
    return res


async def _tg(msg: str) -> None:
    if send_telegram is None:
        logger.info("[telegram disabled] %s", msg)
        return
    with contextlib.suppress(Exception):
        await _call_maybe_async(send_telegram, msg)


async def _redis() -> Any:
    if get_redis_async is None:
        return None
    try:
        return await _call_maybe_async(get_redis_async)
    except Exception:
        logger.debug("get_redis_async failed", exc_info=True)
        return None


async def _allow_new_entry() -> bool:
    if should_allow_new_entry is None:
        return True
    try:
        ok = await _call_maybe_async(should_allow_new_entry, None)
        return bool(ok)
    except Exception:
        return True


async def _ticket_usd() -> float:
    fallback = _env_float(
        "WHALE_TICKET_USD", _env_float("BUY_TICKET_USD", _env_float("PF_TICKET_USD", 20.0))
    )
    fallback = max(0.5, min(fallback, 500.0))

    if current_limits is None:
        return fallback

    try:
        lim = await _call_maybe_async(current_limits, None)
        if not isinstance(lim, dict):
            return fallback
        t = _safe_float(lim.get("ticket_usd", fallback), fallback)
        return max(0.5, min(t, 500.0))
    except Exception:
        return fallback


def _impact_bps(liq_usd: float, ticket_usd: float) -> float:
    if liq_usd <= 0.0:
        return 9e9
    return (ticket_usd / liq_usd) * 10_000.0


def _sell_tax_to_bps(v: Any) -> float:
    """
    Dexscreener's info.sellTax can vary:
      - 0.08  (fraction)
      - 8     (percent)
      - 800   (bps-like)
    We normalize to bps conservatively.
    """
    x = _safe_float(v, 0.0)
    if x <= 0.0:
        return 0.0
    if 0.0 < x <= 1.0:
        return x * 10_000.0
    if 1.0 < x <= 100.0:
        return x * 100.0
    return x


def _pick_window_txns(pair: dict[str, Any], window_min: int) -> tuple[int, int, int]:
    """
    Best-effort mapping to Dexscreener txns windows:
      - m5
      - m15 (sometimes present)
      - h1
      - h24
    Choose the smallest available window >= requested; else the largest available.
    Returns (buys, sells, actual_window_min_used).
    """
    txns = pair.get("txns") or {}
    candidates: list[tuple[int, str]] = [(5, "m5"), (15, "m15"), (60, "h1"), (1440, "h24")]

    available: list[tuple[int, str]] = [
        (mins, key) for mins, key in candidates if isinstance(txns.get(key), dict)
    ]
    if not available:
        return 0, 0, window_min

    bigger_or_equal = [t for t in available if t[0] >= window_min]
    mins, key = (
        min(bigger_or_equal, key=lambda t: t[0])
        if bigger_or_equal
        else max(available, key=lambda t: t[0])
    )

    obj = txns.get(key) or {}
    buys = _safe_int(obj.get("buys", 0))
    sells = _safe_int(obj.get("sells", 0))
    return buys, sells, mins


def _compute_score(
    liq_usd: float,
    vol_m5: float,
    pct_m5: float,
    buys_win: int,
    sell_tax_bps: float,
) -> float:
    """
    Bounded composite score in [0..1], tuned for "whale-ish" bursts:
      - more buys in window
      - decent vol_m5
      - positive price change
      - lower sell tax
      - higher liquidity (soft)

    Deterministic (not a model).
    """
    # buys: map [CLUSTER_MIN_BUYS .. CLUSTER_MIN_BUYS+20] -> [0..1]
    buys_norm = (buys_win - CLUSTER_MIN_BUYS) / 20.0
    buys_norm = max(0.0, min(1.0, buys_norm))

    # vol: map [WHALE_MIN_VOL_M5_USD .. +50k] -> [0..1]
    vol_norm = (vol_m5 - WHALE_MIN_VOL_M5_USD) / 50_000.0
    vol_norm = max(0.0, min(1.0, vol_norm))

    # price: map [WHALE_MIN_PRICECHANGE_M5 .. +12] -> [0..1]
    px_norm = (pct_m5 - WHALE_MIN_PRICECHANGE_M5) / 12.0
    px_norm = max(0.0, min(1.0, px_norm))

    # tax penalty: 0..HONEYPOT_MAX_SELL_TAX_BPS (cap)
    tax_norm = sell_tax_bps / max(1.0, HONEYPOT_MAX_SELL_TAX_BPS)
    tax_norm = max(0.0, min(1.0, tax_norm))
    tax_bonus = 1.0 - tax_norm

    # liquidity bonus (soft): map [MIN_LIQ_USD .. 250k] -> [0..1]
    liq_norm = (liq_usd - MIN_LIQ_USD) / max(1.0, 250_000.0)
    liq_norm = max(0.0, min(1.0, liq_norm))

    score = 0.34 * buys_norm + 0.24 * vol_norm + 0.20 * px_norm + 0.14 * tax_bonus + 0.08 * liq_norm
    return max(0.0, min(1.0, score))


_LOCAL_COOLDOWN: dict[str, int] = {}


async def _cooldown_ok(r: Any, mint: str) -> bool:
    now = int(time.time())
    if r is None:
        last = _LOCAL_COOLDOWN.get(mint, 0)
        if now - last >= COOLDOWN_SEC:
            _LOCAL_COOLDOWN[mint] = now
            return True
        return False

    key = f"{REDIS_COOLDOWN_KEY_PREFIX}{mint}"
    try:
        ok = await r.set(key, str(now), nx=True, ex=COOLDOWN_SEC)
        return bool(ok)
    except Exception:
        logger.debug("cooldown redis op failed", exc_info=True)
        return True


async def _try_lock(r: Any, mint: str) -> bool:
    if r is None:
        return True
    key = f"{REDIS_LOCK_KEY_PREFIX}{mint}"
    try:
        ok = await r.set(key, "1", nx=True, ex=LOCK_TTL_SEC)
        return bool(ok)
    except Exception:
        logger.debug("lock redis op failed", exc_info=True)
        return True


async def _unlock(r: Any, mint: str) -> None:
    if r is None:
        return
    key = f"{REDIS_LOCK_KEY_PREFIX}{mint}"
    with contextlib.suppress(Exception):
        await r.delete(key)


async def _publish(r: Any, c: WhaleCandidate) -> None:
    payload = json.dumps(asdict(c), separators=(",", ":"), ensure_ascii=False)
    if r is None:
        logger.info("[queue-fallback] %s", payload)
        return
    try:
        await r.rpush(REDIS_QUEUE_KEY, payload)
    except Exception:
        logger.exception("Failed to publish whale candidate to Redis queue")


async def _usd_per_sol() -> float:
    if convert_sol_to_usd is None:
        return 0.0
    try:
        v = await _call_maybe_async(convert_sol_to_usd, 1.0)
        return float(v or 0.0)
    except Exception:
        return 0.0


async def _maybe_prewarm(pairs: list[dict[str, Any]], ticket_usd: float) -> None:
    if not PREWARM_ENABLED or prewarm_quotes is None or PREWARM_TOP_N <= 0:
        return

    usd_per_sol = await _usd_per_sol()
    if usd_per_sol <= 0.0:
        return

    amount_sol = ticket_usd / usd_per_sol

    top = sorted(
        pairs,
        key=lambda x: _safe_float((x.get("liquidity") or {}).get("usd", 0.0)),
        reverse=True,
    )[:PREWARM_TOP_N]

    mints: list[str] = []
    for pair in top:
        addr = (pair.get("baseToken") or {}).get("address")
        if addr:
            mints.append(str(addr))

    if not mints:
        return

    try:
        await _call_maybe_async(prewarm_quotes, mints, float(amount_sol), int(SLIPPAGE_BPS))
        logger.debug("Prewarmed %d quotes (amount_sol=%.6f)", len(mints), amount_sol)
    except Exception:
        logger.debug("prewarm_quotes failed", exc_info=True)


async def _fetch_pairs(http: aiohttp.ClientSession) -> list[dict[str, Any]]:
    try:
        async with http.get(DEX_SEARCH_API, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status != 200:
                txt = await r.text()
                logger.warning(
                    "Dexscreener fetch [%s] %s: %s", r.status, DEX_SEARCH_API, (txt or "")[:250]
                )
                return []
            js = await r.json()
            pairs = js.get("pairs", []) or []
            return pairs if isinstance(pairs, list) else []
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.warning("Dexscreener fetch error: %s", e)
        return []


def _build_candidates(pairs: list[dict[str, Any]], ticket_usd: float) -> list[WhaleCandidate]:
    out: list[WhaleCandidate] = []
    scan = pairs[: min(len(pairs), max(10, MAX_SCAN_PAIRS))]

    for pair in scan:
        base = pair.get("baseToken") or {}
        mint = str(base.get("address") or "").strip()
        if not mint:
            continue

        symbol = str(base.get("symbol") or base.get("name") or "UNK")
        pair_url = str(pair.get("url") or "")

        liq = _safe_float((pair.get("liquidity") or {}).get("usd", 0.0))
        if liq < MIN_LIQ_USD:
            continue

        vol_m5 = _safe_float((pair.get("volume") or {}).get("m5", 0.0))
        if vol_m5 < WHALE_MIN_VOL_M5_USD:
            continue

        pct_m5 = _safe_float((pair.get("priceChange") or {}).get("m5", 0.0))
        if pct_m5 < WHALE_MIN_PRICECHANGE_M5:
            continue

        buys_win, sells_win, win_used = _pick_window_txns(pair, CLUSTER_WINDOW_MIN)
        if buys_win < CLUSTER_MIN_BUYS:
            continue

        sell_tax_raw = (pair.get("info") or {}).get("sellTax", 0.0)
        sell_tax_bps = _sell_tax_to_bps(sell_tax_raw)
        if sell_tax_bps > HONEYPOT_MAX_SELL_TAX_BPS:
            continue

        imp = _impact_bps(liq, ticket_usd)
        if imp > MAX_IMPACT_BPS:
            continue

        score = _compute_score(liq, vol_m5, pct_m5, buys_win, sell_tax_bps)
        if score < WHALE_SCORE_MIN:
            continue

        out.append(
            WhaleCandidate(
                ts=time.time(),
                source="dexscreener_whale",
                mint=mint,
                symbol=symbol,
                pair_url=pair_url,
                liquidity_usd=float(liq),
                vol_m5_usd=float(vol_m5),
                price_change_m5=float(pct_m5),
                buys_win=int(buys_win),
                sells_win=int(sells_win),
                window_min=int(win_used),
                sell_tax_bps=float(sell_tax_bps),
                est_impact_bps=float(imp),
                score=float(score),
                raw={"pair": pair},
            )
        )

    out.sort(key=lambda c: (c.score, c.buys_win, c.liquidity_usd, c.vol_m5_usd), reverse=True)
    return out


# =============================================================================
# TRADE mode helpers
# =============================================================================
async def _route_precheck(mint: str, amount_sol: float) -> tuple[bool, str, dict[str, Any]]:
    if precheck_buy is None:
        return True, "no_route_guard", {}
    try:
        out = await _call_maybe_async(precheck_buy, mint, amount_sol, SLIPPAGE_BPS)
        # expected: (ok, why, meta)
        if isinstance(out, tuple) and len(out) >= 2:
            ok = bool(out[0])
            why = str(out[1])
            meta = out[2] if len(out) >= 3 and isinstance(out[2], dict) else {}
            return ok, why, meta
        return True, "route_guard_unexpected_return", {}
    except Exception:
        logger.debug("route_guard precheck failed", exc_info=True)
        return True, "route_guard_error_ignored", {}


async def _posttrade_watchdog(
    mint: str, amount_sol: float, pre_meta: dict[str, Any], tx_sig: str
) -> None:
    if posttrade_watchdog is None:
        return
    try:
        # Support multiple historical signatures (best-effort)
        with contextlib.suppress(TypeError):
            await _call_maybe_async(posttrade_watchdog, mint, amount_sol, SLIPPAGE_BPS, pre_meta)  # type: ignore
            return
        with contextlib.suppress(TypeError):
            await _call_maybe_async(
                posttrade_watchdog, mint=mint, tx_sig=tx_sig, reason="whale_feed"
            )  # type: ignore
            return
        await _call_maybe_async(posttrade_watchdog, mint)  # type: ignore
    except Exception:
        logger.debug("posttrade_watchdog failed", exc_info=True)


async def _swap_buy(mint: str, amount_sol: float) -> tuple[bool, str]:
    if swap_token_jupiter is None:
        return False, ""
    try:
        # Support both signatures you've used historically
        with contextlib.suppress(TypeError):
            out = await _call_maybe_async(
                swap_token_jupiter,
                SOL_MINT,
                mint,
                amount_float=float(amount_sol),
                buy=True,
                slippage=float(JUPITER_MAX_SLIPPAGE),
            )
            if isinstance(out, tuple) and len(out) >= 2:
                return bool(out[0]), str(out[1] or "")
            return bool(out), ""

        out2 = await _call_maybe_async(
            swap_token_jupiter, SOL_MINT, mint, float(amount_sol), True, float(JUPITER_MAX_SLIPPAGE)
        )
        if isinstance(out2, tuple) and len(out2) >= 2:
            return bool(out2[0]), str(out2[1] or "")
        return bool(out2), ""
    except Exception:
        logger.exception("swap_token_jupiter failed")
        return False, ""


# =============================================================================
# Shadow helpers
# =============================================================================
def _is_shadow() -> bool:
    if WHALE_SHADOW:
        return True
    if shadow_take is not None:
        with contextlib.suppress(Exception):
            return bool(shadow_take("WHALE"))
    return _env_bool("SHADOW_MODE", "0")


async def _shadow_log(evt: str, payload: dict[str, Any]) -> None:
    if shadow_log is None:
        logger.info("[shadow:%s] %s", evt, payload)
        return
    with contextlib.suppress(Exception):
        await _call_maybe_async(shadow_log, "whale", evt, payload)


# =============================================================================
# Main loop
# =============================================================================
async def run_whale_feed(stop_event: asyncio.Event) -> None:
    if not WHALE_FEED_ENABLED:
        logger.warning("WHALE_FEED_ENABLED=0; worker will idle.")
        while not stop_event.is_set():
            await asyncio.sleep(5)
        return

    if TELEGRAM_ON_START:
        await _tg(
            "🐳 Whale feed started\n"
            f"• mode={WHALE_FEED_MODE}\n"
            f"• poll={POLL_SEC}s cooldown={COOLDOWN_SEC}s lock_ttl={LOCK_TTL_SEC}s\n"
            f"• gates: liq>={MIN_LIQ_USD:,.0f} vol5>={WHALE_MIN_VOL_M5_USD:,.0f} "
            f"+5m>={WHALE_MIN_PRICECHANGE_M5:.2f}% "
            f"cluster>= {CLUSTER_MIN_BUYS} buys in ~{CLUSTER_WINDOW_MIN}m "
            f"tax<={HONEYPOT_MAX_SELL_TAX_BPS:.0f}bps impact<={MAX_IMPACT_BPS:.0f}bps score>={WHALE_SCORE_MIN:.2f}"
        )

    r = await _redis()
    timeout = aiohttp.ClientTimeout(total=20)

    async with aiohttp.ClientSession(timeout=timeout) as http:
        while not stop_event.is_set():
            t0 = time.time()
            emitted = 0
            traded = 0

            try:
                if not await _allow_new_entry():
                    await asyncio.sleep(POLL_SEC)
                    continue

                ticket_usd = await _ticket_usd()
                pairs = await _fetch_pairs(http)
                if not pairs:
                    await asyncio.sleep(POLL_SEC)
                    continue

                await _maybe_prewarm(pairs, ticket_usd)

                cands = _build_candidates(pairs, ticket_usd)
                if not cands:
                    await asyncio.sleep(POLL_SEC)
                    continue

                # apply cooldown + lock only after candidate passes gates
                ready: list[WhaleCandidate] = []
                for c in cands:
                    if not await _cooldown_ok(r, c.mint):
                        continue
                    if not await _try_lock(r, c.mint):
                        continue
                    ready.append(c)

                if not ready:
                    await asyncio.sleep(POLL_SEC)
                    continue

                if _is_shadow():
                    topn = ready[: min(10, len(ready))]
                    await _shadow_log(
                        "candidates",
                        {
                            "mode": WHALE_FEED_MODE,
                            "ticket_usd": ticket_usd,
                            "top": [
                                {
                                    "sym": c.symbol,
                                    "mint": c.mint,
                                    "score": round(c.score, 3),
                                    "liq_usd": round(c.liquidity_usd, 2),
                                    "vol_m5": round(c.vol_m5_usd, 2),
                                    "pct_m5": round(c.price_change_m5, 2),
                                    "buys_win": c.buys_win,
                                    "win_min": c.window_min,
                                    "tax_bps": round(c.sell_tax_bps, 1),
                                    "impact_bps": round(c.est_impact_bps, 1),
                                }
                                for c in topn
                            ],
                        },
                    )
                    for c in ready:
                        await _unlock(r, c.mint)
                    await asyncio.sleep(POLL_SEC)
                    continue

                if WHALE_FEED_MODE == "QUEUE":
                    for c in ready[:MAX_EMIT_PER_TICK]:
                        try:
                            await _publish(r, c)
                            emitted += 1
                            logger.info(
                                "[QUEUE] %s %s score=%.2f buys%sm=%s vol5=$%s +5m=%.2f%% liq=$%s tax=%.0fbps impact=%.0fbps",
                                c.symbol,
                                c.mint,
                                c.score,
                                c.window_min,
                                c.buys_win,
                                f"{c.vol_m5_usd:,.0f}",
                                c.price_change_m5,
                                f"{c.liquidity_usd:,.0f}",
                                c.sell_tax_bps,
                                c.est_impact_bps,
                            )
                            if TELEGRAM_ON_CANDIDATE:
                                await _tg(
                                    "🐳 Whale candidate queued\n"
                                    f"• {c.symbol} ({c.mint}) score={c.score:.2f}\n"
                                    f"• buys≈{c.buys_win} in {c.window_min}m • vol5=${c.vol_m5_usd:,.0f} • +5m={c.price_change_m5:.2f}%\n"
                                    f"• liq=${c.liquidity_usd:,.0f} • tax≈{c.sell_tax_bps:.0f}bps • impact≈{c.est_impact_bps:.0f}bps\n"
                                    f"• {c.pair_url}"
                                )
                        finally:
                            await _unlock(r, c.mint)

                    dt = time.time() - t0
                    logger.info(
                        "[tick] pairs=%s cands=%s emitted=%s dt=%.2fs mode=%s",
                        len(pairs),
                        len(cands),
                        emitted,
                        dt,
                        WHALE_FEED_MODE,
                    )

                else:
                    # TRADE mode
                    usd_per_sol = await _usd_per_sol()
                    if usd_per_sol <= 0.0:
                        logger.warning("Cannot resolve SOL/USD; skipping TRADE tick.")
                        for c in ready:
                            await _unlock(r, c.mint)
                        await asyncio.sleep(POLL_SEC)
                        continue

                    amount_sol = ticket_usd / usd_per_sol

                    for c in ready:
                        if traded >= max(1, MAX_TRADES_PER_TICK):
                            await _unlock(r, c.mint)
                            continue

                        try:
                            ok_rg, why, pre_meta = await _route_precheck(c.mint, amount_sol)
                            if not ok_rg:
                                await _tg(
                                    "⛔ Whale RouteGuard blocked\n"
                                    f"• {c.symbol} ({c.mint})\n"
                                    f"• why={why}\n"
                                    f"• impact≈{c.est_impact_bps:.0f}bps slippage_bps={SLIPPAGE_BPS}"
                                )
                                continue

                            ok, sig = await _swap_buy(c.mint, amount_sol)
                            if not ok:
                                continue

                            if note_spend is not None:
                                with contextlib.suppress(Exception):
                                    await _call_maybe_async(note_spend, None, float(ticket_usd))

                            traded += 1

                            if TELEGRAM_ON_TRADE:
                                await _tg(
                                    "🐳 BUY (whale_feed)\n"
                                    f"• {c.symbol} ({c.mint})\n"
                                    f"• ticket=${ticket_usd:,.2f} amount≈{amount_sol:.6f} SOL\n"
                                    f"• score={c.score:.2f} buys≈{c.buys_win}/{c.window_min}m vol5=${c.vol_m5_usd:,.0f} +5m={c.price_change_m5:.2f}%\n"
                                    f"• liq=${c.liquidity_usd:,.0f} tax≈{c.sell_tax_bps:.0f}bps impact≈{c.est_impact_bps:.0f}bps\n"
                                    f"• tx={sig}"
                                )

                            await _posttrade_watchdog(c.mint, amount_sol, pre_meta, sig)

                        finally:
                            await _unlock(r, c.mint)

                    dt = time.time() - t0
                    logger.info(
                        "[tick] pairs=%s cands=%s traded=%s dt=%.2fs mode=%s",
                        len(pairs),
                        len(cands),
                        traded,
                        dt,
                        WHALE_FEED_MODE,
                    )

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("[whale_feed] loop error: %s", e, exc_info=True)

            # sleep with jitter to avoid herd sync
            sleep_for = float(POLL_SEC) + random.uniform(0.0, float(JITTER_SEC))
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop_event.wait(), timeout=max(1.0, sleep_for))


async def main() -> None:
    if init_resources is not None:
        with contextlib.suppress(Exception):
            await _call_maybe_async(init_resources)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _stop(*_: Any) -> None:
        stop_event.set()

    for sig_name in ("SIGINT", "SIGTERM"):
        sig_obj = getattr(signal, sig_name, None)
        if sig_obj is None:
            continue
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig_obj, _stop)

    try:
        await run_whale_feed(stop_event)
    finally:
        if close_resources is not None:
            with contextlib.suppress(Exception):
                await _call_maybe_async(close_resources)


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
