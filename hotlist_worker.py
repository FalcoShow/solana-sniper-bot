# hotlist_worker.py — v4 (entry-lock + risk gates + shadow + prewarm + route_guard)
import os, asyncio, logging, aiohttp
from datetime import datetime, timezone

from utils_pkg.core_utils import convert_sol_to_usd, swap_token_jupiter, send_telegram
from risk_manager import current_limits, should_allow_new_entry, note_spend
from shadow_utils import shadow_take, shadow_log
from entry_gate import try_acquire, release
from jup_quote_cache import prewarm_quotes
from route_guard import precheck_buy, posttrade_watchdog

logger = logging.getLogger("hotlist")
logging.basicConfig(level=logging.INFO)

SOL_MINT = "So11111111111111111111111111111111111111112"
DEX_SEARCH_API = "https://api.dexscreener.com/latest/dex/search?q=sol"

HOTLIST_SCORE_MIN = float(os.getenv("HOTLIST_SCORE_MIN", "0.68"))
AI_MIN_LIQ_USD    = float(os.getenv("AI_MIN_LIQ_USD", "20000"))
AI_MAX_IMPACT_BPS = float(os.getenv("AI_MAX_IMPACT_BPS", "150"))
JUPITER_SLIPPAGE  = float(os.getenv("JUPITER_MAX_SLIPPAGE", "0.005"))
SLIPPAGE_BPS      = max(1, int(JUPITER_SLIPPAGE * 10000))

_last_ts: dict[str, float] = {}
def _cooldown_ok(m: str, sec: int) -> bool:
    now = datetime.now(timezone.utc).timestamp()
    if now - _last_ts.get(m, 0.0) >= sec:
        _last_ts[m] = now
        return True
    return False

async def _usd_per_sol() -> float:
    return float(await convert_sol_to_usd(1.0) or 0.0)

async def _impact_bps(liq: float, usd: float) -> float:
    return (usd / liq) * 10000.0 if liq > 0 else 9e9

async def _fetch(session):
    async with session.get(DEX_SEARCH_API, timeout=15) as r:
        if r.status != 200:
            return []
        return (await r.json()).get("pairs", []) or []

async def run_hotlist():
    await send_telegram("🔥 Hotlist started (v4: prewarm+route_guard).")
    poll = int(os.getenv("HOTLIST_POLL_SEC", "30"))
    cd   = int(os.getenv("HOTLIST_COOLDOWN_SEC", "300"))

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                if not await should_allow_new_entry(None):
                    await asyncio.sleep(poll); continue
                limits = await current_limits(None)
                ticket = float(limits["ticket_usd"])

                pairs = await _fetch(session)

                # Prewarm by top-liquidity or top-score
                try:
                    usd_per_sol = await convert_sol_to_usd(1.0) or 0.0
                    if usd_per_sol > 0 and pairs:
                        amount_sol = ticket / usd_per_sol
                        # Prefer native hotScore if available, else liquidity
                        def score_key(p):
                            hs = p.get("hotScore")
                            if isinstance(hs, (int, float)): return float(hs)
                            return float((p.get("liquidity") or {}).get("usd", 0) or 0)
                        top = sorted(pairs, key=score_key, reverse=True)[:8]
                        mints = [(p.get("baseToken") or {}).get("address") for p in top if (p.get("baseToken") or {}).get("address")]
                        await prewarm_quotes(mints, amount_sol, SLIPPAGE_BPS)
                except Exception:
                    pass

                for p in pairs:
                    mint = (p.get("baseToken") or {}).get("address")
                    if not mint or not _cooldown_ok(mint, cd): continue
                    liq = float((p.get("liquidity") or {}).get("usd", 0.0) or 0.0)
                    if liq < AI_MIN_LIQ_USD: continue

                    # native 'hotScore' if present; else simple composite
                    hs = p.get("hotScore")
                    score = float(hs) if isinstance(hs, (int, float)) else 0.0
                    if not score:
                        vol24 = float((p.get("volume") or {}).get("h24", 0.0) or 0.0)
                        pct5  = float((p.get("priceChange") or {}).get("m5", 0.0) or 0.0)
                        buys5 = int((((p.get("txns") or {}).get("m5") or {}).get("buys", 0)) or 0)
                        score = min(1.0, (vol24/1_000_000.0)*0.35 + max(0.0, pct5)/10.0*0.30 + min(1.0, buys5/20.0)*0.20 + 0.15)

                    if score < HOTLIST_SCORE_MIN: continue
                    imp = await _impact_bps(liq, ticket)
                    if imp > AI_MAX_IMPACT_BPS: continue

                    if shadow_take("HOTLIST"):
                        await shadow_log("hotlist", "candidate", {
                            "mint": mint, "ticket": f"${ticket:,.2f}", "liq": f"${liq:,.0f}",
                            "score": f"{score:.2f}", "impact_bps": f"{imp:.0f}"
                        })
                        continue

                    if not await try_acquire(mint):
                        continue

                    try:
                        sol = ticket / max(1e-9, await _usd_per_sol())

                        # Route guard precheck
                        ok_rg, why, pre_meta = await precheck_buy(mint, sol, SLIPPAGE_BPS)
                        if not ok_rg:
                            await send_telegram(f"⛔ RouteGuard block {mint} — {why} • hops={pre_meta.get('hops')} • impact_bps={pre_meta.get('impact_bps'):.0f}")
                            await release(mint); continue

                        try:
                            ok, sig = await swap_token_jupiter(SOL_MINT, mint, amount_float=sol, buy=True, slippage=JUPITER_SLIPPAGE)
                        except TypeError:
                            ok, sig = await swap_token_jupiter(SOL_MINT, mint, sol, True, JUPITER_SLIPPAGE)
                        if not ok:
                            await release(mint); continue

                        await note_spend(None, ticket)
                        await send_telegram(
                            f"🔥 BUY (hotlist) mint={mint}\n"
                            f"• ticket=${ticket:,.2f} • liq=${liq:,.0f} • score={score:.2f} • impact={imp:.0f} bps\n"
                            f"• tx={sig}"
                        )

                        # Post-trade watchdog
                        ok_wd, msg, now_meta = await posttrade_watchdog(mint, sol, SLIPPAGE_BPS, pre_meta)
                        if not ok_wd:
                            await send_telegram(
                                f"⚠️ Route watchdog: {mint} {msg} • "
                                f"pre_out={pre_meta.get('out_amount'):.0f} now_out={now_meta.get('out_amount',0):.0f} "
                                f"hops(pre/now)={pre_meta.get('hops')}/{now_meta.get('hops',0)}"
                            )
                    except Exception:
                        await release(mint)
                        raise

                await asyncio.sleep(poll)
            except Exception as e:
                logger.error(f"[hotlist] loop error: {e}", exc_info=True)
                await asyncio.sleep(poll)

if __name__ == "__main__":
    asyncio.run(run_hotlist())
