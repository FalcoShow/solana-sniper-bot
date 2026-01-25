# hotlist_worker.py
from __future__ import annotations

"""
hotlist_worker.py (P0 — production)
==================================

Purpose
-------
Continuously pulls "hot" token candidates (Dexscreener search / trending-style feed),
applies strong risk + quality gates, then:

DEFAULT (recommended):
  - QUEUE mode: publishes candidates to Redis for the canonical sniper/entry pipeline
    to decide & execute (prevents "competing execution engines").

OPTIONAL:
  - TRADE mode: executes buys directly via Jupiter (only if HOTLIST_MODE=TRADE)

Hard Requirements (P0 lock-in)
------------------------------
- Safe-by-default (no trading unless explicitly enabled)
- No duplicate entries: entry lock + cooldown (persisted in Redis)
- Risk gates: liquidity / impact / basic activity / allow-entry + ticket sizing
- Observability: logs + Telegram pings on key state transitions
- Graceful shutdown (SIGINT/SIGTERM)
"""

import asyncio
import contextlib
import json
import logging
import os
import random
import signal
import time
from dataclasses import asdict, dataclass
from typing import Any

import aiohttp


# ---------------------------
# Logging setup (canonical if available)
# ---------------------------
def _setup_logging() -> None:
    # Prefer root logging_setup.py if present (per your P0 decision).
    try:
        from logging_setup import setup_logging  # type: ignore

        setup_logging(service="hotlist_worker")
        return
    except Exception:
        pass

    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s | %(levelname)s | %(name)s: %(message)s",
    )


_setup_logging()
logger = logging.getLogger("hotlist")


# ---------------------------
# Optional integrations (kept compatible but not required)
# ---------------------------
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

# Risk manager is optional; if missing we fallback to env sizing.
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

# Route guard (optional): precheck & posttrade watchdog
try:
    from route_guard import posttrade_watchdog, precheck_buy  # type: ignore
except Exception:  # pragma: no cover
    precheck_buy = None
    posttrade_watchdog = None

# Quote cache prewarm (optional)
try:
    from jup_quote_cache import prewarm_quotes  # type: ignore
except Exception:  # pragma: no cover
    prewarm_quotes = None


# ---------------------------
# Constants / env
# ---------------------------
SOL_MINT = "So11111111111111111111111111111111111111112"

DEX_SEARCH_API = os.getenv(
    "HOTLIST_DEX_SEARCH_API",
    "https://api.dexscreener.com/latest/dex/search?q=sol",
)

HOTLIST_ENABLED = os.getenv("HOTLIST_ENABLED", "1").strip() not in ("0", "false", "False")

# SAFE DEFAULT: queue candidates for canonical sniper pipeline
HOTLIST_MODE = os.getenv("HOTLIST_MODE", "QUEUE").strip().upper()  # QUEUE | TRADE
if HOTLIST_MODE not in ("QUEUE", "TRADE"):
    HOTLIST_MODE = "QUEUE"

POLL_SEC = int(os.getenv("HOTLIST_POLL_SEC", "30"))
JITTER_SEC = float(os.getenv("HOTLIST_JITTER_SEC", "1.25"))

COOLDOWN_SEC = int(os.getenv("HOTLIST_COOLDOWN_SEC", "300"))
LOCK_TTL_SEC = int(os.getenv("HOTLIST_LOCK_TTL_SEC", "120"))

# Candidate gates (tunable)
MIN_SCORE = float(os.getenv("HOTLIST_SCORE_MIN", "0.68"))
MIN_LIQ_USD = float(os.getenv("HOTLIST_MIN_LIQ_USD", os.getenv("AI_MIN_LIQ_USD", "20000")))
MIN_VOL24_USD = float(os.getenv("HOTLIST_MIN_VOL24_USD", "0"))
MIN_BUYS_5M = int(os.getenv("HOTLIST_MIN_BUYS_5M", "0"))
MAX_IMPACT_BPS = float(os.getenv("HOTLIST_MAX_IMPACT_BPS", os.getenv("AI_MAX_IMPACT_BPS", "150")))

# Trade settings (only used in TRADE mode)
JUPITER_MAX_SLIPPAGE = float(os.getenv("JUPITER_MAX_SLIPPAGE", "0.005"))
SLIPPAGE_BPS = max(1, int(JUPITER_MAX_SLIPPAGE * 10_000))

# Ticket sizing fallback (USD) if risk_manager is absent
DEFAULT_TICKET_USD = float(os.getenv("HOTLIST_TICKET_USD", "15"))

# Where we publish candidates (QUEUE mode)
REDIS_QUEUE_KEY = os.getenv("HOTLIST_QUEUE_KEY", "entry_queue:hotlist")  # list/rpush
REDIS_COOLDOWN_KEY_PREFIX = os.getenv("HOTLIST_COOLDOWN_KEY_PREFIX", "cd:hotlist:")
REDIS_LOCK_KEY_PREFIX = os.getenv("HOTLIST_LOCK_KEY_PREFIX", "lock:hotlist:")

# Prewarm behavior
PREWARM_ENABLED = os.getenv("HOTLIST_PREWARM_ENABLED", "1").strip() not in ("0", "false", "False")
PREWARM_TOP_N = int(os.getenv("HOTLIST_PREWARM_TOP_N", "8"))

# Telegram announcements
TELEGRAM_ON_START = os.getenv("HOTLIST_TELEGRAM_ON_START", "1").strip() not in (
    "0",
    "false",
    "False",
)
TELEGRAM_ON_CANDIDATE = os.getenv("HOTLIST_TELEGRAM_ON_CANDIDATE", "0").strip() not in (
    "0",
    "false",
    "False",
)
TELEGRAM_ON_BUY = os.getenv("HOTLIST_TELEGRAM_ON_BUY", "1").strip() not in ("0", "false", "False")


# ---------------------------
# Data model
# ---------------------------
@dataclass(frozen=True)
class HotCandidate:
    ts: float
    source: str
    mint: str
    symbol: str
    pair_url: str

    # raw metrics
    liquidity_usd: float
    volume_h24_usd: float
    price_change_m5: float
    buys_m5: int
    sells_m5: int

    # scoring / risk
    score: float
    est_impact_bps: float

    # passthrough fields for your downstream feature builder / logs
    raw: dict[str, Any]


# ---------------------------
# Helpers
# ---------------------------
async def _tg(msg: str) -> None:
    if send_telegram is None:
        logger.info(f"[telegram disabled] {msg}")
        return
    try:
        await send_telegram(msg)
    except Exception:
        logger.debug("send_telegram failed", exc_info=True)


async def _usd_per_sol() -> float:
    if convert_sol_to_usd is None:
        return 0.0
    try:
        v = await convert_sol_to_usd(1.0)
        return float(v or 0.0)
    except Exception:
        return 0.0


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


def _impact_bps(liq_usd: float, ticket_usd: float) -> float:
    if liq_usd <= 0:
        return 9e9
    return (ticket_usd / liq_usd) * 10_000.0


def _compute_score(p: dict[str, Any]) -> float:
    """
    Score in [0, 1].
    Prefer Dexscreener's hotScore if present; else compute a robust composite.
    """
    hs = p.get("hotScore")
    if isinstance(hs, int | float):
        # hotScore can exceed 1; clamp to 1 to behave like a gate score
        return max(0.0, min(1.0, float(hs)))

    liq = _safe_float((p.get("liquidity") or {}).get("usd", 0.0))
    vol24 = _safe_float((p.get("volume") or {}).get("h24", 0.0))
    pc5 = _safe_float((p.get("priceChange") or {}).get("m5", 0.0))
    tx5 = (p.get("txns") or {}).get("m5") or {}
    buys5 = _safe_int(tx5.get("buys", 0))
    sells5 = _safe_int(tx5.get("sells", 0))

    # Normalize pieces
    liq_term = min(1.0, liq / 250_000.0)  # 0..1 around 250k liq
    vol_term = min(1.0, vol24 / 1_000_000.0)  # 0..1 around 1m vol
    act_term = min(1.0, (buys5 + sells5) / 40.0)  # 0..1 around 40 tx in 5m

    # Momentum prefers positive but doesn't punish mildly negative too hard (avoid dead coins)
    momo_term = 0.0
    if pc5 >= 0:
        momo_term = min(1.0, pc5 / 10.0)  # 10% in 5m = 1.0
    else:
        momo_term = max(0.0, 1.0 + (pc5 / 20.0))  # -20% = 0

    # Slight bias toward buy imbalance
    imb = 0.0
    denom = max(1, buys5 + sells5)
    imb = (buys5 - sells5) / denom  # -1..+1
    imb_term = (imb + 1.0) / 2.0  # 0..1

    # Weighted composite
    score = 0.28 * liq_term + 0.25 * vol_term + 0.20 * act_term + 0.20 * momo_term + 0.07 * imb_term
    return max(0.0, min(1.0, float(score)))


async def _fetch_pairs(http: aiohttp.ClientSession) -> list[dict[str, Any]]:
    try:
        async with http.get(DEX_SEARCH_API, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status != 200:
                txt = await r.text()
                logger.warning(f"Dexscreener fetch [{r.status}] {DEX_SEARCH_API}: {txt[:250]}")
                return []
            data = await r.json()
            pairs = data.get("pairs", []) or []
            if not isinstance(pairs, list):
                return []
            return pairs
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.warning(f"Dexscreener fetch error: {e}")
        return []


async def _redis() -> Any:
    if get_redis_async is None:
        return None
    try:
        return await get_redis_async()
    except Exception:
        logger.debug("get_redis_async failed", exc_info=True)
        return None


async def _cooldown_ok(r: Any, mint: str) -> bool:
    """
    Cooldown persisted in Redis (survives restarts). If Redis unavailable,
    fallback to in-memory last-seen.
    """
    now = int(time.time())

    if r is None:
        # in-memory fallback
        if not hasattr(_cooldown_ok, "_mem"):
            _cooldown_ok._mem = {}  # type: ignore
        mem: dict[str, int] = _cooldown_ok._mem  # type: ignore
        last = mem.get(mint, 0)
        if now - last >= COOLDOWN_SEC:
            mem[mint] = now
            return True
        return False

    key = f"{REDIS_COOLDOWN_KEY_PREFIX}{mint}"
    try:
        # set if not exists, with ttl
        ok = await r.set(key, str(now), nx=True, ex=COOLDOWN_SEC)
        return bool(ok)
    except Exception:
        logger.debug("cooldown redis op failed", exc_info=True)
        return True


async def _try_lock(r: Any, mint: str) -> bool:
    """
    Entry lock to prevent parallel workers (or multiple loops) buying/queuing the same mint.
    """
    if r is None:
        return True  # best-effort only
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
    try:
        await r.delete(key)
    except Exception:
        logger.debug("unlock redis op failed", exc_info=True)


async def _get_ticket_usd() -> float:
    """
    Ticket sizing is owned by risk_manager when present; otherwise fallback to env.
    """
    if current_limits is None:
        return float(DEFAULT_TICKET_USD)

    try:
        lim = await current_limits(None)
        t = float(lim.get("ticket_usd", DEFAULT_TICKET_USD))
        # Safety clamp
        return max(0.5, min(t, 500.0))
    except Exception:
        return float(DEFAULT_TICKET_USD)


async def _allow_new_entry() -> bool:
    if should_allow_new_entry is None:
        return True
    try:
        return bool(await should_allow_new_entry(None))
    except Exception:
        return True


async def _publish_candidate(r: Any, c: HotCandidate) -> None:
    payload = json.dumps(asdict(c), separators=(",", ":"), ensure_ascii=False)
    if r is None:
        # If Redis is down, we still log for visibility.
        logger.info(f"[queue-fallback] {payload}")
        return
    try:
        await r.rpush(REDIS_QUEUE_KEY, payload)
    except Exception:
        logger.exception("Failed to publish candidate to Redis queue")


async def _maybe_prewarm(r: Any, pairs: list[dict[str, Any]], ticket_usd: float) -> None:
    if not PREWARM_ENABLED or prewarm_quotes is None:
        return
    usd_per_sol = await _usd_per_sol()
    if usd_per_sol <= 0:
        return
    amount_sol = ticket_usd / usd_per_sol

    # Pick top N by (hotScore if present else liquidity)
    def key(p: dict[str, Any]) -> float:
        hs = p.get("hotScore")
        if isinstance(hs, int | float):
            return float(hs)
        return _safe_float((p.get("liquidity") or {}).get("usd", 0.0))

    top = sorted(pairs, key=key, reverse=True)[: max(1, PREWARM_TOP_N)]
    mints: list[str] = []
    for p in top:
        mint = (p.get("baseToken") or {}).get("address")
        if mint:
            mints.append(str(mint))

    if not mints:
        return

    try:
        await prewarm_quotes(mints, amount_sol, SLIPPAGE_BPS)
        logger.debug(f"Prewarmed {len(mints)} quotes (amount_sol={amount_sol:.6f})")
    except Exception:
        logger.debug("prewarm_quotes failed", exc_info=True)


# ---------------------------
# Main loop
# ---------------------------
async def run_hotlist(stop_event: asyncio.Event) -> None:
    if not HOTLIST_ENABLED:
        logger.warning("HOTLIST_ENABLED=0; worker will idle.")
        while not stop_event.is_set():
            await asyncio.sleep(5)
        return

    if TELEGRAM_ON_START:
        await _tg(
            "🔥 Hotlist worker started\n"
            f"• mode={HOTLIST_MODE}\n"
            f"• poll={POLL_SEC}s cooldown={COOLDOWN_SEC}s lock_ttl={LOCK_TTL_SEC}s\n"
            f"• gates: score>={MIN_SCORE:.2f} liq>={MIN_LIQ_USD:,.0f} impact<={MAX_IMPACT_BPS:.0f}bps"
        )

    r = await _redis()

    timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(timeout=timeout) as http:
        while not stop_event.is_set():
            start = time.time()
            try:
                # Global allow-entry gate
                if not await _allow_new_entry():
                    await asyncio.sleep(POLL_SEC)
                    continue

                ticket_usd = await _get_ticket_usd()
                pairs = await _fetch_pairs(http)

                # Prewarm quotes (best-effort)
                await _maybe_prewarm(r, pairs, ticket_usd)

                emitted = 0
                for p in pairs:
                    base = p.get("baseToken") or {}
                    mint = base.get("address")
                    symbol = base.get("symbol") or base.get("name") or "UNK"
                    if not mint:
                        continue
                    mint = str(mint)

                    # Persisted cooldown first
                    if not await _cooldown_ok(r, mint):
                        continue

                    # Lock per mint to prevent concurrency
                    if not await _try_lock(r, mint):
                        continue

                    try:
                        liq = _safe_float((p.get("liquidity") or {}).get("usd", 0.0))
                        if liq < MIN_LIQ_USD:
                            continue

                        vol24 = _safe_float((p.get("volume") or {}).get("h24", 0.0))
                        if vol24 < MIN_VOL24_USD:
                            continue

                        tx5 = (p.get("txns") or {}).get("m5") or {}
                        buys5 = _safe_int(tx5.get("buys", 0))
                        sells5 = _safe_int(tx5.get("sells", 0))
                        if buys5 < MIN_BUYS_5M:
                            continue

                        score = _compute_score(p)
                        if score < MIN_SCORE:
                            continue

                        impact = _impact_bps(liq, ticket_usd)
                        if impact > MAX_IMPACT_BPS:
                            continue

                        pair_url = str(p.get("url") or "")

                        cand = HotCandidate(
                            ts=time.time(),
                            source="dexscreener_search",
                            mint=mint,
                            symbol=str(symbol),
                            pair_url=pair_url,
                            liquidity_usd=float(liq),
                            volume_h24_usd=float(vol24),
                            price_change_m5=_safe_float(
                                (p.get("priceChange") or {}).get("m5", 0.0)
                            ),
                            buys_m5=int(buys5),
                            sells_m5=int(sells5),
                            score=float(score),
                            est_impact_bps=float(impact),
                            raw={"pair": p},
                        )

                        # QUEUE mode (default): publish and let canonical sniper handle entry decision/execution
                        if HOTLIST_MODE == "QUEUE":
                            await _publish_candidate(r, cand)
                            emitted += 1
                            logger.info(
                                f"[QUEUE] {cand.symbol} {cand.mint} "
                                f"score={cand.score:.2f} liq=${cand.liquidity_usd:,.0f} "
                                f"vol24=${cand.volume_h24_usd:,.0f} buys5={cand.buys_m5} "
                                f"impact={cand.est_impact_bps:.0f}bps"
                            )
                            if TELEGRAM_ON_CANDIDATE:
                                await _tg(
                                    "🧲 Hotlist candidate queued\n"
                                    f"• {cand.symbol} ({cand.mint})\n"
                                    f"• score={cand.score:.2f} liq=${cand.liquidity_usd:,.0f} "
                                    f"vol24=${cand.volume_h24_usd:,.0f} buys5={cand.buys_m5}\n"
                                    f"• impact≈{cand.est_impact_bps:.0f} bps\n"
                                    f"• {cand.pair_url}"
                                )
                            continue

                        # TRADE mode (explicit): execute buy here (only if enabled)
                        # NOTE: This is intentionally stricter + uses route guard when available.
                        if HOTLIST_MODE == "TRADE":
                            if swap_token_jupiter is None:
                                logger.error(
                                    "TRADE mode requested but swap_token_jupiter is unavailable"
                                )
                                continue

                            usd_per_sol = await _usd_per_sol()
                            if usd_per_sol <= 0:
                                logger.warning("Cannot resolve SOL/USD; skipping TRADE")
                                continue

                            amount_sol = ticket_usd / usd_per_sol

                            # Optional route guard precheck
                            pre_meta: dict[str, Any] = {}
                            if precheck_buy is not None:
                                try:
                                    ok_rg, why, meta = await precheck_buy(
                                        mint, amount_sol, SLIPPAGE_BPS
                                    )
                                    pre_meta = meta or {}
                                    if not ok_rg:
                                        logger.info(f"[BLOCK] route_guard mint={mint} why={why}")
                                        await _tg(
                                            f"⛔ Hotlist RouteGuard blocked\n"
                                            f"• {symbol} ({mint})\n"
                                            f"• why={why}\n"
                                            f"• impact≈{impact:.0f}bps slippage_bps={SLIPPAGE_BPS}"
                                        )
                                        continue
                                except Exception:
                                    logger.debug("route_guard precheck failed", exc_info=True)

                            # Execute buy
                            ok = False
                            sig = None
                            try:
                                # Support both historical signatures
                                try:
                                    ok, sig = await swap_token_jupiter(
                                        SOL_MINT,
                                        mint,
                                        amount_float=float(amount_sol),
                                        buy=True,
                                        slippage=float(JUPITER_MAX_SLIPPAGE),
                                    )
                                except TypeError:
                                    ok, sig = await swap_token_jupiter(
                                        SOL_MINT,
                                        mint,
                                        float(amount_sol),
                                        True,
                                        float(JUPITER_MAX_SLIPPAGE),
                                    )
                            except Exception:
                                logger.exception("swap_token_jupiter failed")
                                ok = False

                            if not ok:
                                continue

                            # Note spend to risk manager (best effort)
                            if note_spend is not None:
                                try:
                                    await note_spend(None, float(ticket_usd))
                                except Exception:
                                    logger.debug("note_spend failed", exc_info=True)

                            if TELEGRAM_ON_BUY:
                                await _tg(
                                    "🔥 BUY (hotlist)\n"
                                    f"• {symbol} ({mint})\n"
                                    f"• ticket=${ticket_usd:,.2f} amount≈{amount_sol:.6f} SOL\n"
                                    f"• liq=${liq:,.0f} score={score:.2f} impact≈{impact:.0f}bps\n"
                                    f"• tx={sig}"
                                )

                            # Optional post-trade watchdog
                            if posttrade_watchdog is not None:
                                try:
                                    ok_wd, msg, now_meta = await posttrade_watchdog(
                                        mint, amount_sol, SLIPPAGE_BPS, pre_meta
                                    )
                                    if not ok_wd:
                                        await _tg(
                                            f"⚠️ Route watchdog\n"
                                            f"• {symbol} ({mint})\n"
                                            f"• {msg}\n"
                                            f"• pre={pre_meta.get('out_amount')} now={now_meta.get('out_amount')}"
                                        )
                                except Exception:
                                    logger.debug("posttrade_watchdog failed", exc_info=True)

                            emitted += 1
                            continue

                    finally:
                        await _unlock(r, mint)

                # loop pacing
                dur = time.time() - start
                logger.info(
                    f"[tick] pairs={len(pairs)} emitted={emitted} dt={dur:.2f}s mode={HOTLIST_MODE}"
                )

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"[hotlist] loop error: {e}", exc_info=True)

            # Sleep with small jitter to avoid synchronized thundering herd
            sleep_for = max(1.0, float(POLL_SEC)) + random.uniform(0.0, JITTER_SEC)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop_event.wait(), timeout=sleep_for)


async def main() -> None:
    if init_resources is not None:
        try:
            await init_resources()
        except Exception:
            logger.debug("init_resources failed (continuing)", exc_info=True)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _set_stop(*_: Any) -> None:
        stop_event.set()

    # Signals (best-effort; Windows may not support add_signal_handler)
    for sig_name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, sig_name, None)
        if sig is None:
            continue
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _set_stop)

    try:
        await run_hotlist(stop_event)
    finally:
        if close_resources is not None:
            try:
                await close_resources()
            except Exception:
                logger.debug("close_resources failed", exc_info=True)


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
