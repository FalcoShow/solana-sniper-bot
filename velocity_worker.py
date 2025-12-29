# velocity_worker.py — V4+++ (entry-lock + risk gates + shadow + prewarm + route_guard)
# -----------------------------------------------------------------------------
# Purpose:
#   "Velocity" is a separate worker that looks for short-window volume/price
#   acceleration candidates and attempts a buy via Jupiter (or your router hook),
#   but only after passing risk gates and route safety checks.
#
# Design goals (God-tier stability):
#   - Never fail import-time because some optional modules aren't present yet
#   - Never break compileall
#   - Strong guardrails (cooldown, lock, min liq, impact, min buys, score gate)
#   - Shadow mode support (observe without trading)
#   - RouteGuard precheck + posttrade watchdog (if available)
#   - Quote prewarming for top candidates (if available)
#
# Run:
#   py -m py_compile .\velocity_worker.py
#   py -m compileall -q .
#   py -c "import asyncio; from velocity_worker import main; asyncio.run(main())"
# -----------------------------------------------------------------------------

from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

# ---- Prefer your logging_setup if present ----
try:
    from logging_setup import get_logger  # type: ignore
    log = get_logger("velocity")
except Exception:
    log = logging.getLogger("velocity")
    logging.basicConfig(level=logging.INFO)

# ---- Core utils hooks (these should exist in your repo) ----
# convert_sol_to_usd, swap_token_jupiter, send_telegram
try:
    from utils_pkg.core_utils import convert_sol_to_usd, swap_token_jupiter, send_telegram  # type: ignore
except Exception as e:
    raise RuntimeError(f"velocity_worker requires utils_pkg.core_utils. Import failed: {e}")

# ---- Optional modules (worker must not die if they are not ready yet) ----
# risk_manager
try:
    from risk_manager import current_limits, should_allow_new_entry, note_spend  # type: ignore
except Exception:
    async def should_allow_new_entry(_ctx: Any) -> bool:
        return True

    async def current_limits(_ctx: Any) -> Dict[str, Any]:
        # Fallback: use a generic ticket if risk_manager not wired yet
        return {"ticket_usd": float(os.getenv("BUY_TICKET_USD", os.getenv("PF_TICKET_USD", "20")))}

    async def note_spend(_ctx: Any, _usd: float) -> None:
        return None

# shadow utils
try:
    from shadow_utils import shadow_take, shadow_log  # type: ignore
except Exception:
    def shadow_take(_name: str) -> bool:
        # Allow turning on shadow globally via env even without module
        return os.getenv("SHADOW_MODE", "0").strip() in ("1", "true", "True", "yes", "YES")

    async def shadow_log(_chan: str, _evt: str, _payload: Dict[str, Any]) -> None:
        # no-op fallback
        return None

# entry gate / lock
try:
    from entry_gate import try_acquire, release  # type: ignore
except Exception:
    _LOCAL_LOCKS: set[str] = set()

    async def try_acquire(key: str) -> bool:
        if key in _LOCAL_LOCKS:
            return False
        _LOCAL_LOCKS.add(key)
        return True

    async def release(key: str) -> None:
        _LOCAL_LOCKS.discard(key)

# quote prewarm
try:
    from jup_quote_cache import prewarm_quotes  # type: ignore
except Exception:
    async def prewarm_quotes(_mints: List[str], _amount_sol: float, _slippage_bps: int) -> None:
        return None

# route guard
try:
    from route_guard import precheck_buy, posttrade_watchdog  # type: ignore
except Exception:
    async def precheck_buy(_mint: str, _sol: float, _slippage_bps: int) -> Tuple[bool, str, Dict[str, Any]]:
        return True, "no_route_guard", {"hops": None, "impact_bps": 0.0}

    async def posttrade_watchdog(*_args: Any, **_kwargs: Any) -> None:
        return None


# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------

SOL_MINT = "So11111111111111111111111111111111111111112"
DEX_SEARCH_API = os.getenv("VELOCITY_DEX_SEARCH_API", "https://api.dexscreener.com/latest/dex/search?q=sol")

def _env_bool(name: str, default: str = "0") -> bool:
    v = os.getenv(name, default).strip().lower()
    return v in ("1", "true", "yes", "y", "on")

# Worker enable
VELOCITY_ENABLED = _env_bool("VELOCITY_ENABLED", "1")

# pacing
VELOCITY_POLL_SEC = int(os.getenv("VELOCITY_POLL_SEC", "20"))
VELOCITY_COOLDOWN_SEC = int(os.getenv("VELOCITY_COOLDOWN_SEC", "300"))
VELOCITY_MAX_CANDIDATES = int(os.getenv("VELOCITY_MAX_CANDIDATES", "80"))
VELOCITY_MAX_TRADES_PER_POLL = int(os.getenv("VELOCITY_MAX_TRADES_PER_POLL", "1"))

# gates
VELOCITY_VOL_RATIO_MIN = float(os.getenv("VELOCITY_VOL_RATIO_MIN", "2.0"))
VELOCITY_PRICE_CHANGE_MIN = float(os.getenv("VELOCITY_PRICE_CHANGE_MIN", "2.0"))
VELOCITY_BUYS5M_MIN = int(os.getenv("VELOCITY_BUYS5M_MIN", "6"))
VELOCITY_SCORE_MIN = float(os.getenv("VELOCITY_SCORE_MIN", "0.65"))

AI_MIN_LIQ_USD = float(os.getenv("AI_MIN_LIQ_USD", "20000"))
AI_MAX_IMPACT_BPS = float(os.getenv("AI_MAX_IMPACT_BPS", "150"))

# Jupiter slippage
JUPITER_MAX_SLIPPAGE = float(os.getenv("JUPITER_MAX_SLIPPAGE", os.getenv("JUPITER_SLIPPAGE", "0.005")))
SLIPPAGE_BPS = max(1, int(JUPITER_MAX_SLIPPAGE * 10000))

# behavior toggles
VELOCITY_SHADOW = _env_bool("VELOCITY_SHADOW", "0")
VELOCITY_NOTIFY_CANDIDATES = _env_bool("VELOCITY_NOTIFY_CANDIDATES", "0")

# If your router is disabled by env, don’t spam attempts
JUP_ENABLED = _env_bool("JUP_ENABLED", "0")


# -----------------------------------------------------------------------------
# State
# -----------------------------------------------------------------------------

_last_seen: Dict[str, float] = {}

def _cooldown_ok(mint: str, sec: int) -> bool:
    now = time.time()
    last = _last_seen.get(mint, 0.0)
    if now - last >= sec:
        _last_seen[mint] = now
        return True
    return False

async def _usd_per_sol() -> float:
    try:
        v = await convert_sol_to_usd(1.0)
        return float(v or 0.0)
    except Exception:
        return 0.0

def _impact_bps(liq_usd: float, ticket_usd: float) -> float:
    if liq_usd <= 0:
        return 9e9
    return (ticket_usd / liq_usd) * 10000.0

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


@dataclass
class VelocityCandidate:
    mint: str
    symbol: str
    liq_usd: float
    vol5: float
    vol30: float
    ratio: float
    pct5: float
    buys5: int
    impact_bps: float
    score: float
    raw: Dict[str, Any]


def _compute_score(ratio: float, pct5: float, buys5: int) -> float:
    """
    Simple bounded composite in [0,1].
    - ratio contributes up to ~0.45
    - pct5 contributes up to ~0.35
    - buys5 contributes up to ~0.20
    """
    r = max(0.0, min(1.0, (ratio - VELOCITY_VOL_RATIO_MIN) / max(1e-9, VELOCITY_VOL_RATIO_MIN * 2.0)))
    p = max(0.0, min(1.0, (pct5 - VELOCITY_PRICE_CHANGE_MIN) / 20.0))
    b = max(0.0, min(1.0, (buys5 - VELOCITY_BUYS5M_MIN) / 30.0))
    score = 0.45 * r + 0.35 * p + 0.20 * b
    return max(0.0, min(1.0, score))


async def _fetch_pairs(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    try:
        async with session.get(DEX_SEARCH_API, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status != 200:
                return []
            js = await r.json()
            pairs = js.get("pairs", []) or []
            if not isinstance(pairs, list):
                return []
            return pairs
    except Exception as e:
        log.warning("DEX fetch error: %s", e)
        return []


def _build_candidates(pairs: List[Dict[str, Any]], ticket_usd: float) -> List[VelocityCandidate]:
    out: List[VelocityCandidate] = []
    for p in pairs[: max(10, VELOCITY_MAX_CANDIDATES)]:
        base = p.get("baseToken") or {}
        mint = base.get("address") or ""
        sym = base.get("symbol") or base.get("name") or "?"
        if not mint:
            continue

        liq = _safe_float((p.get("liquidity") or {}).get("usd", 0.0), 0.0)
        if liq < AI_MIN_LIQ_USD:
            continue

        vol5 = _safe_float((p.get("volume") or {}).get("m5", 0.0), 0.0)
        vol30 = _safe_float((p.get("volume") or {}).get("m30", 0.0), 0.0)
        ratio = (vol5 / vol30) if vol30 > 0 else 0.0
        if ratio < VELOCITY_VOL_RATIO_MIN:
            continue

        pct5 = _safe_float((p.get("priceChange") or {}).get("m5", 0.0), 0.0)
        if pct5 < VELOCITY_PRICE_CHANGE_MIN:
            continue

        buys5 = _safe_int(((p.get("txns") or {}).get("m5") or {}).get("buys", 0), 0)
        if buys5 < VELOCITY_BUYS5M_MIN:
            continue

        imp = _impact_bps(liq, ticket_usd)
        if imp > AI_MAX_IMPACT_BPS:
            continue

        score = _compute_score(ratio, pct5, buys5)
        if score < VELOCITY_SCORE_MIN:
            continue

        out.append(
            VelocityCandidate(
                mint=mint,
                symbol=str(sym),
                liq_usd=liq,
                vol5=vol5,
                vol30=vol30,
                ratio=ratio,
                pct5=pct5,
                buys5=buys5,
                impact_bps=imp,
                score=score,
                raw=p,
            )
        )
    # Best first
    out.sort(key=lambda c: (c.score, c.liq_usd, c.buys5), reverse=True)
    return out


async def _notify(msg: str) -> None:
    try:
        await send_telegram(msg)
    except Exception:
        # telegram may be missing locally, do not crash worker
        log.info("[telegram skipped] %s", msg)


async def _attempt_buy(c: VelocityCandidate, ticket_usd: float) -> bool:
    """
    Execute a guarded buy attempt.
    Returns True if buy succeeded, else False.
    """
    mint = c.mint

    if not await try_acquire(mint):
        return False

    try:
        usd_per_sol = await _usd_per_sol()
        if usd_per_sol <= 0:
            log.warning("USD/SOL unavailable; skipping buy.")
            return False

        sol_amt = ticket_usd / usd_per_sol

        # Route guard precheck
        ok_rg, why, meta = await precheck_buy(mint, sol_amt, SLIPPAGE_BPS)
        if not ok_rg:
            hops = meta.get("hops")
            ibps = meta.get("impact_bps", 0.0)
            await _notify(f"⛔ RouteGuard BLOCK {c.symbol} {mint}\n• why: {why}\n• hops: {hops}\n• impact_bps: {float(ibps):.0f}")
            return False

        if not JUP_ENABLED:
            # Don’t spam failed swaps when you intentionally disabled Jupiter by env
            log.info("JUP disabled by env; skipping live buy (candidate %s %s).", c.symbol, mint)
            return False

        # Execute swap (support both signatures you’ve had in the past)
        ok = False
        sig = ""
        try:
            ok, sig = await swap_token_jupiter(
                SOL_MINT,
                mint,
                amount_float=sol_amt,
                buy=True,
                slippage=JUPITER_MAX_SLIPPAGE,
            )
        except TypeError:
            ok, sig = await swap_token_jupiter(SOL_MINT, mint, sol_amt, True, JUPITER_MAX_SLIPPAGE)

        if not ok:
            return False

        # Record risk spend
        await note_spend(None, float(ticket_usd))

        # Notify
        await _notify(
            "⚡ BUY (velocity)\n"
            f"• Symbol: {c.symbol}\n"
            f"• Mint: {mint}\n"
            f"• Ticket: ${ticket_usd:,.2f}  • Liq: ${c.liq_usd:,.0f}\n"
            f"• Ratio: {c.ratio:.2f}  • +5m: {c.pct5:.2f}%  • Buys5m: {c.buys5}\n"
            f"• Impact: {c.impact_bps:.0f} bps  • Score: {c.score:.2f}\n"
            f"• Tx: {sig}"
        )

        # Post-trade watchdog (don’t block worker)
        try:
            asyncio.create_task(
                posttrade_watchdog(mint=mint, tx_sig=sig, reason="velocity"),
                name=f"route_guard.watchdog:{mint[:6]}",
            )
        except Exception:
            pass

        return True

    finally:
        try:
            await release(mint)
        except Exception:
            pass


async def run_velocity(stop_evt: Optional[asyncio.Event] = None) -> None:
    if not VELOCITY_ENABLED:
        log.info("VELOCITY_ENABLED=0; velocity worker exiting.")
        return

    await _notify("⚡ Velocity worker started (V4+++ prewarm + route_guard + risk gates).")

    cd = int(VELOCITY_COOLDOWN_SEC)
    poll = int(VELOCITY_POLL_SEC)

    async with aiohttp.ClientSession() as session:
        while True:
            if stop_evt and stop_evt.is_set():
                log.info("stop_evt set; exiting velocity worker.")
                return

            try:
                # risk gate: allow new entry?
                allowed = await should_allow_new_entry(None)
                if not allowed:
                    await asyncio.sleep(poll)
                    continue

                limits = await current_limits(None)
                ticket_usd = float(limits.get("ticket_usd") or limits.get("ticket") or 0.0)
                if ticket_usd <= 0:
                    ticket_usd = float(os.getenv("BUY_TICKET_USD", os.getenv("PF_TICKET_USD", "20")))

                pairs = await _fetch_pairs(session)
                if not pairs:
                    await asyncio.sleep(poll)
                    continue

                # Prewarm quotes for top-liquidity candidates (best-effort)
                try:
                    usd_per_sol = await _usd_per_sol()
                    if usd_per_sol > 0:
                        amount_sol = ticket_usd / usd_per_sol
                        top = sorted(
                            pairs,
                            key=lambda x: _safe_float((x.get("liquidity") or {}).get("usd", 0.0), 0.0),
                            reverse=True,
                        )[:8]
                        mints = []
                        for p in top:
                            addr = (p.get("baseToken") or {}).get("address")
                            if addr:
                                mints.append(addr)
                        if mints:
                            await prewarm_quotes(mints, amount_sol, SLIPPAGE_BPS)
                except Exception:
                    pass

                cands = _build_candidates(pairs, ticket_usd)

                # Apply cooldown last, so we don’t penalize before knowing it’s a good candidate
                cooled: List[VelocityCandidate] = []
                for c in cands:
                    if _cooldown_ok(c.mint, cd):
                        cooled.append(c)

                if not cooled:
                    await asyncio.sleep(poll)
                    continue

                # Optional: notify candidates even in shadow
                if VELOCITY_NOTIFY_CANDIDATES:
                    top = cooled[0]
                    await _notify(
                        "⚡ Velocity candidate\n"
                        f"• {top.symbol} • score={top.score:.2f}\n"
                        f"• liq=${top.liq_usd:,.0f} • ratio={top.ratio:.2f} • +5m={top.pct5:.2f}% • buys5m={top.buys5}\n"
                        f"• mint={top.mint}"
                    )

                # Shadow mode: log + skip trading
                if VELOCITY_SHADOW or shadow_take("VELOCITY"):
                    topn = cooled[: min(5, len(cooled))]
                    payload = [
                        {
                            "sym": c.symbol,
                            "mint": c.mint,
                            "ticket_usd": ticket_usd,
                            "liq_usd": c.liq_usd,
                            "ratio": c.ratio,
                            "pct5": c.pct5,
                            "buys5": c.buys5,
                            "impact_bps": c.impact_bps,
                            "score": c.score,
                        }
                        for c in topn
                    ]
                    await shadow_log("velocity", "candidates", {"top": payload})
                    await asyncio.sleep(poll)
                    continue

                # Execute up to N trades per poll (default 1)
                trades = 0
                for c in cooled:
                    if trades >= VELOCITY_MAX_TRADES_PER_POLL:
                        break

                    ok = await _attempt_buy(c, ticket_usd)
                    if ok:
                        trades += 1

                await asyncio.sleep(poll)

            except Exception as e:
                log.exception("velocity loop error: %s", e)
                await asyncio.sleep(max(3, poll))


# Standardized entrypoint
async def main(stop_evt: Optional[asyncio.Event] = None) -> None:
    await run_velocity(stop_evt=stop_evt)


if __name__ == "__main__":
    asyncio.run(main())
