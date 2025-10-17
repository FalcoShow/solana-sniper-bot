# whale_feed.py — v4 (entry-lock + risk gates + shadow + prewarm + route_guard)
import os, asyncio, logging, aiohttp
from datetime import datetime, timezone

from utils_pkg.core_utils import convert_sol_to_usd, swap_token_jupiter, send_telegram
from risk_manager import current_limits, should_allow_new_entry, note_spend
from shadow_utils import shadow_take, shadow_log
from entry_gate import try_acquire, release
from jup_quote_cache import prewarm_quotes
from route_guard import precheck_buy, posttrade_watchdog

logger = logging.getLogger("whale")
logging.basicConfig(level=logging.INFO)

SOL_MINT = "So11111111111111111111111111111111111111112"
DEX_SEARCH_API = "https://api.dexscreener.com/latest/dex/search?q=sol"

WHALE_FEED_ENABLED        = os.getenv("WHALE_FEED_ENABLED", "true").lower() == "true"
JUPITER_SLIPPAGE          = float(os.getenv("JUPITER_MAX_SLIPPAGE", "0.005"))
SLIPPAGE_BPS              = max(1, int(JUPITER_SLIPPAGE * 10000))
AI_MIN_LIQ_USD            = float(os.getenv("AI_MIN_LIQ_USD", "20000"))
HONEYPOT_MAX_SELL_TAX_BPS = float(os.getenv("HONEYPOT_MAX_SELL_TAX_BPS", "800"))
MAX_IMPACT_BPS            = float(os.getenv("WHALE_MAX_IMPACT_BPS", os.getenv("AI_MAX_IMPACT_BPS", "150")))
CLUSTER_MIN_BUYS          = int(os.getenv("WHALE_CLUSTER_MIN_BUYS", "2"))
CLUSTER_WINDOW_MIN        = int(os.getenv("WHALE_CLUSTER_WINDOW_MIN", "5"))
ENTRY_COOLDOWN_SEC        = int(os.getenv("WHALE_ENTRY_COOLDOWN_SEC", "300"))

_last: dict[str, float] = {}
def _cooldown_ok(mint: str) -> bool:
    now = datetime.now(timezone.utc).timestamp()
    if now - _last.get(mint, 0.0) >= ENTRY_COOLDOWN_SEC:
        _last[mint] = now
        return True
    return False

async def _usd_per_sol() -> float:
    return float(await convert_sol_to_usd(1.0) or 0.0)

async def _impact_bps(liq_usd: float, buy_usd: float) -> float:
    return (buy_usd / liq_usd) * 10000.0 if liq_usd > 0 else 9e9

async def _fetch_pairs(session: aiohttp.ClientSession) -> list[dict]:
    async with session.get(DEX_SEARCH_API, timeout=15) as r:
        if r.status != 200:
            return []
        return (await r.json()).get("pairs", []) or []

def _passes_gates(pair: dict) -> tuple[bool, str]:
    liq = float((pair.get("liquidity") or {}).get("usd", 0.0) or 0.0)
    if liq < AI_MIN_LIQ_USD: return False, "liq"
    sell_tax = float(((pair.get("info") or {}) or {}).get("sellTax", 0.0) or 0.0)
    if sell_tax * 10000.0 > HONEYPOT_MAX_SELL_TAX_BPS: return False, "tax"
    return True, ""

async def run_whale_feed():
    if not WHALE_FEED_ENABLED:
        logger.info("whale feed disabled")
        return
    await send_telegram("🐳 Whale feed started (v4: prewarm+route_guard).")

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                if not await should_allow_new_entry(None):
                    await asyncio.sleep(20); continue
                limits = await current_limits(None)
                ticket = float(limits["ticket_usd"])

                pairs = (await _fetch_pairs(session))[:20]

                # Prewarm quotes for top-liquidity mints
                try:
                    usd_per_sol = await convert_sol_to_usd(1.0) or 0.0
                    if usd_per_sol > 0 and pairs:
                        amount_sol = ticket / usd_per_sol
                        top = sorted(pairs, key=lambda x: float((x.get("liquidity") or {}).get("usd", 0) or 0), reverse=True)[:8]
                        mints = [(p.get("baseToken") or {}).get("address") for p in top if (p.get("baseToken") or {}).get("address")]
                        await prewarm_quotes(mints, amount_sol, SLIPPAGE_BPS)
                except Exception:
                    pass

                for p in pairs:
                    mint = (p.get("baseToken") or {}).get("address")
                    if not mint or not _cooldown_ok(mint): continue
                    ok, _ = _passes_gates(p)
                    if not ok: continue

                    liq = float((p.get("liquidity") or {}).get("usd", 0.0) or 0.0)
                    imp = await _impact_bps(liq, ticket)
                    if imp > MAX_IMPACT_BPS: continue

                    if shadow_take("WHALE"):
                        await shadow_log("whale", "candidate", {
                            "mint": mint, "ticket": f"${ticket:,.2f}", "liq": f"${liq:,.0f}",
                            "impact_bps": f"{imp:.0f}"
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
                            f"🐳 BUY mint={mint}\n"
                            f"• ticket=${ticket:,.2f} • liq=${liq:,.0f} • impact={imp:.0f} bps\n"
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

                await asyncio.sleep(20)
            except Exception as e:
                logger.error(f"[whale] loop error: {e}", exc_info=True)
                await asyncio.sleep(30)

if __name__ == "__main__":
    asyncio.run(run_whale_feed())
