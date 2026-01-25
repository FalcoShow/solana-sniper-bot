# velocity_worker.py
from __future__ import annotations

"""
velocity_worker.py (P0 — production)
===================================

Purpose
-------
"Velocity" detects short-window acceleration candidates (volume + price + activity),
then:

DEFAULT (recommended):
  - QUEUE mode: publish candidates to Redis for the canonical sniper/entry pipeline.

OPTIONAL:
  - TRADE mode: execute buys directly via Jupiter (only if VELOCITY_MODE=TRADE).

P0 Lock-in Principles
---------------------
- Safe-by-default (no trading unless explicitly enabled)
- No competing execution engines by default (QUEUE feeds the canonical path)
- Strong guardrails:
    * allow-entry gate (risk_manager if available)
    * liquidity + impact caps
    * velocity acceleration gate
    * cooldown + distributed lock (Redis)
- Optional route safety checks (route_guard) and quote prewarming (jup_quote_cache)
- Observability: logs + Telegram pings
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
# Logging (canonical if available)
# ---------------------------
def _setup_logging() -> None:
    try:
        from logging_setup import setup_logging  # type: ignore

        setup_logging(service="velocity_worker")
        return
    except Exception:
        pass

    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s | %(levelname)s | %(name)s: %(message)s",
    )


_setup_logging()
logger = logging.getLogger("velocity")


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
    from route_guard import posttrade_watchdog, precheck_buy  # type: ignore
except Exception:  # pragma: no cover
    precheck_buy = None
    posttrade_watchdog = None

try:
    from jup_quote_cache import prewarm_quotes  # type: ignore
except Exception:  # pragma: no cover
    prewarm_quotes = None


# ---------------------------
# Env / constants
# ---------------------------
SOL_MINT = "So11111111111111111111111111111111111111112"

DEX_SEARCH_API = os.getenv(
    "VELOCITY_DEX_SEARCH_API",
    "https://api.dexscreener.com/latest/dex/search?q=sol",
)


def _env_bool(name: str, default: str = "0") -> bool:
    v = os.getenv(name, default).strip().lower()
    return v in ("1", "true", "yes", "y", "on")


VELOCITY_ENABLED = _env_bool("VELOCITY_ENABLED", "1")

# SAFE DEFAULT: publish to queue (no direct execution)
VELOCITY_MODE = os.getenv("VELOCITY_MODE", "QUEUE").strip().upper()  # QUEUE | TRADE
if VELOCITY_MODE not in ("QUEUE", "TRADE"):
    VELOCITY_MODE = "QUEUE"

POLL_SEC = int(os.getenv("VELOCITY_POLL_SEC", "20"))
JITTER_SEC = float(os.getenv("VELOCITY_JITTER_SEC", "1.0"))

COOLDOWN_SEC = int(os.getenv("VELOCITY_COOLDOWN_SEC", "300"))
LOCK_TTL_SEC = int(os.getenv("VELOCITY_LOCK_TTL_SEC", "120"))

MAX_SCAN_PAIRS = int(os.getenv("VELOCITY_MAX_CANDIDATES", "120"))
MAX_EMIT_PER_TICK = int(os.getenv("VELOCITY_MAX_EMIT_PER_TICK", "25"))  # queue spam control
MAX_TRADES_PER_TICK = int(os.getenv("VELOCITY_MAX_TRADES_PER_POLL", "1"))  # trade mode only

# Gates (velocity-specific)
# Use acceleration ratio = vol_m5 / (vol_m30/6). (vol_m30 is a 30m window, 6x 5m bins)
ACCEL_MIN = float(os.getenv("VELOCITY_ACCEL_MIN", os.getenv("VELOCITY_VOL_RATIO_MIN", "2.0")))
PRICE_CHANGE_M5_MIN = float(os.getenv("VELOCITY_PRICE_CHANGE_MIN", "2.0"))
BUYS_M5_MIN = int(os.getenv("VELOCITY_BUYS5M_MIN", "6"))
SCORE_MIN = float(os.getenv("VELOCITY_SCORE_MIN", "0.65"))

# Shared risk gates
MIN_LIQ_USD = float(os.getenv("VELOCITY_MIN_LIQ_USD", os.getenv("AI_MIN_LIQ_USD", "20000")))
MAX_IMPACT_BPS = float(os.getenv("VELOCITY_MAX_IMPACT_BPS", os.getenv("AI_MAX_IMPACT_BPS", "150")))

# Slippage (trade mode)
JUPITER_MAX_SLIPPAGE = float(
    os.getenv("JUPITER_MAX_SLIPPAGE", os.getenv("JUPITER_SLIPPAGE", "0.005"))
)
SLIPPAGE_BPS = max(1, int(JUPITER_MAX_SLIPPAGE * 10_000))

# Publishing (queue mode)
REDIS_QUEUE_KEY = os.getenv("VELOCITY_QUEUE_KEY", "entry_queue:velocity")
REDIS_COOLDOWN_KEY_PREFIX = os.getenv("VELOCITY_COOLDOWN_KEY_PREFIX", "cd:velocity:")
REDIS_LOCK_KEY_PREFIX = os.getenv("VELOCITY_LOCK_KEY_PREFIX", "lock:velocity:")

# Quote prewarm (best-effort)
PREWARM_ENABLED = _env_bool("VELOCITY_PREWARM_ENABLED", "1")
PREWARM_TOP_N = int(os.getenv("VELOCITY_PREWARM_TOP_N", "8"))

# Notifications
TELEGRAM_ON_START = _env_bool("VELOCITY_TELEGRAM_ON_START", "1")
TELEGRAM_ON_CANDIDATE = _env_bool("VELOCITY_TELEGRAM_ON_CANDIDATE", "0")
TELEGRAM_ON_TRADE = _env_bool("VELOCITY_TELEGRAM_ON_TRADE", "1")


# ---------------------------
# Data model
# ---------------------------
@dataclass(frozen=True)
class VelocityCandidate:
    ts: float
    source: str
    mint: str
    symbol: str
    pair_url: str

    liquidity_usd: float
    vol_m5_usd: float
    vol_m30_usd: float
    accel: float  # vol_m5 / (vol_m30/6)
    price_change_m5: float
    buys_m5: int
    sells_m5: int

    score: float
    est_impact_bps: float

    raw: dict[str, Any]


# ---------------------------
# Helpers
# ---------------------------
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
    Cooldown persisted in Redis. If Redis unavailable, fallback to in-memory cooldown.
    """
    now = int(time.time())

    if r is None:
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
    try:
        await r.delete(key)
    except Exception:
        logger.debug("unlock redis op failed", exc_info=True)


async def _allow_new_entry() -> bool:
    if should_allow_new_entry is None:
        return True
    try:
        return bool(await should_allow_new_entry(None))
    except Exception:
        return True


async def _ticket_usd() -> float:
    """
    Ticket sizing owned by risk_manager when present; env fallback if not.
    """
    fallback = float(
        os.getenv(
            "VELOCITY_TICKET_USD", os.getenv("BUY_TICKET_USD", os.getenv("PF_TICKET_USD", "20"))
        )
    )
    if current_limits is None:
        return max(0.5, min(fallback, 500.0))
    try:
        lim = await current_limits(None)
        t = float(lim.get("ticket_usd", fallback))
        return max(0.5, min(t, 500.0))
    except Exception:
        return max(0.5, min(fallback, 500.0))


def _compute_score(accel: float, pct_m5: float, buys_m5: int) -> float:
    """
    Bounded composite score in [0, 1].
    - accel contributes up to 0.45
    - pct_m5 contributes up to 0.35
    - buys contributes up to 0.20
    """
    # accel: map [ACCEL_MIN .. 3*ACCEL_MIN] -> [0..1]
    a = max(0.0, min(1.0, (accel - ACCEL_MIN) / max(1e-9, (ACCEL_MIN * 2.0))))
    # pct_m5: map [PRICE_CHANGE_M5_MIN .. +22] -> [0..1]
    p = max(0.0, min(1.0, (pct_m5 - PRICE_CHANGE_M5_MIN) / 20.0))
    # buys_m5: map [BUYS_M5_MIN .. BUYS_M5_MIN+30] -> [0..1]
    b = max(0.0, min(1.0, (buys_m5 - BUYS_M5_MIN) / 30.0))
    s = 0.45 * a + 0.35 * p + 0.20 * b
    return max(0.0, min(1.0, s))


async def _fetch_pairs(http: aiohttp.ClientSession) -> list[dict[str, Any]]:
    try:
        async with http.get(DEX_SEARCH_API, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status != 200:
                txt = await r.text()
                logger.warning(f"Dexscreener fetch [{r.status}] {DEX_SEARCH_API}: {txt[:250]}")
                return []
            js = await r.json()
            pairs = js.get("pairs", []) or []
            return pairs if isinstance(pairs, list) else []
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.warning(f"Dexscreener fetch error: {e}")
        return []


def _build_candidates(pairs: list[dict[str, Any]], ticket_usd: float) -> list[VelocityCandidate]:
    out: list[VelocityCandidate] = []

    for p in pairs[: max(10, MAX_SCAN_PAIRS)]:
        base = p.get("baseToken") or {}
        mint = str(base.get("address") or "").strip()
        if not mint:
            continue

        symbol = str(base.get("symbol") or base.get("name") or "UNK")
        pair_url = str(p.get("url") or "")

        liq = _safe_float((p.get("liquidity") or {}).get("usd", 0.0))
        if liq < MIN_LIQ_USD:
            continue

        vol_m5 = _safe_float((p.get("volume") or {}).get("m5", 0.0))
        vol_m30 = _safe_float((p.get("volume") or {}).get("m30", 0.0))
        # accel = m5 vs average 5m in last 30m
        denom = max(1e-9, (vol_m30 / 6.0))
        accel = vol_m5 / denom
        if accel < ACCEL_MIN:
            continue

        pct5 = _safe_float((p.get("priceChange") or {}).get("m5", 0.0))
        if pct5 < PRICE_CHANGE_M5_MIN:
            continue

        tx5 = (p.get("txns") or {}).get("m5") or {}
        buys5 = _safe_int(tx5.get("buys", 0))
        sells5 = _safe_int(tx5.get("sells", 0))
        if buys5 < BUYS_M5_MIN:
            continue

        imp = _impact_bps(liq, ticket_usd)
        if imp > MAX_IMPACT_BPS:
            continue

        score = _compute_score(accel, pct5, buys5)
        if score < SCORE_MIN:
            continue

        out.append(
            VelocityCandidate(
                ts=time.time(),
                source="dexscreener_velocity",
                mint=mint,
                symbol=symbol,
                pair_url=pair_url,
                liquidity_usd=float(liq),
                vol_m5_usd=float(vol_m5),
                vol_m30_usd=float(vol_m30),
                accel=float(accel),
                price_change_m5=float(pct5),
                buys_m5=int(buys5),
                sells_m5=int(sells5),
                score=float(score),
                est_impact_bps=float(imp),
                raw={"pair": p},
            )
        )

    out.sort(key=lambda c: (c.score, c.liquidity_usd, c.buys_m5), reverse=True)
    return out


async def _publish(r: Any, c: VelocityCandidate) -> None:
    payload = json.dumps(asdict(c), separators=(",", ":"), ensure_ascii=False)
    if r is None:
        logger.info(f"[queue-fallback] {payload}")
        return
    try:
        await r.rpush(REDIS_QUEUE_KEY, payload)
    except Exception:
        logger.exception("Failed to publish candidate to Redis queue")


async def _maybe_prewarm(pairs: list[dict[str, Any]], ticket_usd: float) -> None:
    if not PREWARM_ENABLED or prewarm_quotes is None:
        return
    usd_per_sol = await _usd_per_sol()
    if usd_per_sol <= 0:
        return
    amount_sol = ticket_usd / usd_per_sol

    # prewarm top by liquidity
    top = sorted(
        pairs,
        key=lambda x: _safe_float((x.get("liquidity") or {}).get("usd", 0.0)),
        reverse=True,
    )[: max(1, PREWARM_TOP_N)]

    mints: list[str] = []
    for p in top:
        addr = (p.get("baseToken") or {}).get("address")
        if addr:
            mints.append(str(addr))

    if not mints:
        return

    try:
        await prewarm_quotes(mints, amount_sol, SLIPPAGE_BPS)
        logger.debug(f"Prewarmed {len(mints)} quotes (amount_sol={amount_sol:.6f})")
    except Exception:
        logger.debug("prewarm_quotes failed", exc_info=True)


# ---------------------------
# TRADE mode execution helpers
# ---------------------------
async def _route_precheck(mint: str, amount_sol: float) -> tuple[bool, str, dict[str, Any]]:
    if precheck_buy is None:
        return True, "no_route_guard", {}
    try:
        ok, why, meta = await precheck_buy(mint, amount_sol, SLIPPAGE_BPS)
        return bool(ok), str(why), (meta or {})
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
        try:
            await posttrade_watchdog(mint, amount_sol, SLIPPAGE_BPS, pre_meta)  # type: ignore
            return
        except TypeError:
            pass
        try:
            await posttrade_watchdog(mint=mint, tx_sig=tx_sig, reason="velocity")  # type: ignore
            return
        except TypeError:
            pass
        await posttrade_watchdog(mint)  # type: ignore
    except Exception:
        logger.debug("posttrade_watchdog failed", exc_info=True)


async def _swap_buy(mint: str, amount_sol: float) -> tuple[bool, str]:
    if swap_token_jupiter is None:
        return False, ""
    try:
        # Support both signatures you’ve used historically
        try:
            ok, sig = await swap_token_jupiter(
                SOL_MINT,
                mint,
                amount_float=float(amount_sol),
                buy=True,
                slippage=float(JUPITER_MAX_SLIPPAGE),
            )
            return bool(ok), str(sig or "")
        except TypeError:
            ok, sig = await swap_token_jupiter(
                SOL_MINT, mint, float(amount_sol), True, float(JUPITER_MAX_SLIPPAGE)
            )
            return bool(ok), str(sig or "")
    except Exception:
        logger.exception("swap_token_jupiter failed")
        return False, ""


# ---------------------------
# Main loop
# ---------------------------
async def run_velocity(stop_event: asyncio.Event) -> None:
    if not VELOCITY_ENABLED:
        logger.warning("VELOCITY_ENABLED=0; worker will idle.")
        while not stop_event.is_set():
            await asyncio.sleep(5)
        return

    if TELEGRAM_ON_START:
        await _tg(
            "⚡ Velocity worker started\n"
            f"• mode={VELOCITY_MODE}\n"
            f"• poll={POLL_SEC}s cooldown={COOLDOWN_SEC}s lock_ttl={LOCK_TTL_SEC}s\n"
            f"• gates: accel>={ACCEL_MIN:.2f} +5m>={PRICE_CHANGE_M5_MIN:.2f}% buys5>={BUYS_M5_MIN} "
            f"liq>={MIN_LIQ_USD:,.0f} impact<={MAX_IMPACT_BPS:.0f}bps score>={SCORE_MIN:.2f}"
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

                # best-effort prewarm
                await _maybe_prewarm(pairs, ticket_usd)

                cands = _build_candidates(pairs, ticket_usd)
                if not cands:
                    await asyncio.sleep(POLL_SEC)
                    continue

                # Apply cooldown + lock per mint only after we know it is a good candidate
                cooled: list[VelocityCandidate] = []
                for c in cands:
                    if not await _cooldown_ok(r, c.mint):
                        continue
                    if not await _try_lock(r, c.mint):
                        continue
                    cooled.append(c)

                if not cooled:
                    await asyncio.sleep(POLL_SEC)
                    continue

                # QUEUE mode: publish best candidates and unlock immediately
                if VELOCITY_MODE == "QUEUE":
                    for c in cooled[: max(1, MAX_EMIT_PER_TICK)]:
                        try:
                            await _publish(r, c)
                            emitted += 1
                            logger.info(
                                f"[QUEUE] {c.symbol} {c.mint} "
                                f"score={c.score:.2f} accel={c.accel:.2f} "
                                f"liq=${c.liquidity_usd:,.0f} vol5=${c.vol_m5_usd:,.0f} "
                                f"+5m={c.price_change_m5:.2f}% buys5={c.buys_m5} "
                                f"impact={c.est_impact_bps:.0f}bps"
                            )
                            if TELEGRAM_ON_CANDIDATE:
                                await _tg(
                                    "⚡ Velocity candidate queued\n"
                                    f"• {c.symbol} ({c.mint})\n"
                                    f"• score={c.score:.2f} accel={c.accel:.2f}\n"
                                    f"• liq=${c.liquidity_usd:,.0f} vol5=${c.vol_m5_usd:,.0f} "
                                    f"+5m={c.price_change_m5:.2f}% buys5={c.buys_m5}\n"
                                    f"• impact≈{c.est_impact_bps:.0f} bps\n"
                                    f"• {c.pair_url}"
                                )
                        finally:
                            await _unlock(r, c.mint)

                    dt = time.time() - t0
                    logger.info(
                        f"[tick] pairs={len(pairs)} cands={len(cands)} emitted={emitted} dt={dt:.2f}s mode={VELOCITY_MODE}"
                    )
                else:
                    # TRADE mode: attempt up to MAX_TRADES_PER_TICK
                    usd_per_sol = await _usd_per_sol()
                    if usd_per_sol <= 0:
                        logger.warning("Cannot resolve SOL/USD; skipping TRADE tick.")
                        for c in cooled:
                            await _unlock(r, c.mint)
                        await asyncio.sleep(POLL_SEC)
                        continue

                    amount_sol = ticket_usd / usd_per_sol

                    for c in cooled:
                        if traded >= max(1, MAX_TRADES_PER_TICK):
                            await _unlock(r, c.mint)
                            continue

                        try:
                            ok_rg, why, pre_meta = await _route_precheck(c.mint, amount_sol)
                            if not ok_rg:
                                await _tg(
                                    f"⛔ Velocity RouteGuard blocked\n"
                                    f"• {c.symbol} ({c.mint})\n"
                                    f"• why={why}\n"
                                    f"• impact≈{c.est_impact_bps:.0f}bps slippage_bps={SLIPPAGE_BPS}"
                                )
                                continue

                            ok, sig = await _swap_buy(c.mint, amount_sol)
                            if not ok:
                                continue

                            # note spend (best-effort)
                            if note_spend is not None:
                                try:
                                    await note_spend(None, float(ticket_usd))
                                except Exception:
                                    logger.debug("note_spend failed", exc_info=True)

                            traded += 1

                            if TELEGRAM_ON_TRADE:
                                await _tg(
                                    "⚡ BUY (velocity)\n"
                                    f"• {c.symbol} ({c.mint})\n"
                                    f"• ticket=${ticket_usd:,.2f} amount≈{amount_sol:.6f} SOL\n"
                                    f"• score={c.score:.2f} accel={c.accel:.2f}\n"
                                    f"• liq=${c.liquidity_usd:,.0f} impact≈{c.est_impact_bps:.0f}bps\n"
                                    f"• tx={sig}"
                                )

                            # posttrade watchdog (best-effort)
                            await _posttrade_watchdog(c.mint, amount_sol, pre_meta, sig)

                        finally:
                            await _unlock(r, c.mint)

                    dt = time.time() - t0
                    logger.info(
                        f"[tick] pairs={len(pairs)} cands={len(cands)} traded={traded} dt={dt:.2f}s mode={VELOCITY_MODE}"
                    )

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"[velocity] loop error: {e}", exc_info=True)

            # sleep with jitter (avoid herd sync)
            sleep_for = max(1.0, float(POLL_SEC)) + random.uniform(0.0, float(JITTER_SEC))
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

    def _stop(*_: Any) -> None:
        stop_event.set()

    for sig_name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, sig_name, None)
        if sig is None:
            continue
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _stop)

    try:
        await run_velocity(stop_event)
    finally:
        if close_resources is not None:
            try:
                await close_resources()
            except Exception:
                logger.debug("close_resources failed", exc_info=True)


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
