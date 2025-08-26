# velocity_worker.py — v4 (entry-lock + risk gates + shadow + prewarm + route_guard)
import os, asyncio, logging, aiohttp
from datetime import datetime, timezone

from utils_pkg.core_utils import convert_sol_to_usd, swap_token_jupiter, send_telegram
from risk_manager import current_limits, should_allow_new_entry, note_spend
from shadow_utils import shadow_take, shadow_log
from entry_gate import try_acquire, release
from jup_quote_cache import prewarm_quotes
from route_guard import precheck_buy, posttrade_watchdog

logger = logging.getLogger("velocity")
logging.basicConfig(level=logging.INFO)

SOL_MINT = "So11111111111111111111111111111111111111112"
DEX_SEARCH_API = "https://api.dexscreener.com/latest/dex/search?q=sol"

VELOCITY_VOL_RATIO_MIN   = float(os.getenv("VELOCITY_VOL_RATIO_MIN", "2.0"))
VELOCITY_PRICE_CHANGE_MIN= float(os.getenv("VELOCITY_PRICE_CHANGE_MIN", "2.0"))
VELOCITY_BUYS5M_MIN      = int(os.getenv("VELOCITY_BUYS5M_MIN", "6"))
VELOCITY_SCORE_MIN       = float(os.getenv("VELOCITY_SCORE_MIN", "0.65"))

AI_MIN_LIQ_USD    = float(os.getenv("AI_MIN_LIQ_USD", "20000"))
AI_MAX_IMPACT_BPS = float(os.getenv("AI_MAX_IMPACT_BPS", "150"))
JUPITER_SLIPPAGE  = float(os.getenv("JUPITER_MAX_SLIPPAGE", "0.005"))
SLIPPAGE_BPS      = max(1, int(JUPITER_SLIPPAGE * 10000))

_last: dict[str, float] = {}
def _cooldown_ok(mint: str, sec: int) -> bool:
    now = datetime.now(timezone.utc).timestamp()
    if now - _last.get(mint, 0.0) >= sec:
        _last[mint] = now
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

async def run_velocity():
    await send_telegram("⚡ Velocity started (v4: prewarm+route_guard).")
    poll = int(os.getenv("VELOCITY_POLL_SEC", "20"))
    cd   = int(os.getenv("VELOCITY_COOLDOWN_SEC", "300"))

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                if not await should_allow_new_entry(None):
                    await asyncio.sleep(poll); continue
                limits = await current_limits(None)
                ticket = float(limits["ticket_usd"])

                pairs = await _fetch(session)

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
                    if not mint or not _cooldown_ok(mint, cd): continue
                    liq = float((p.get("liquidity") or {}).get("usd", 0.0) or 0.0)
                    if liq < AI_MIN_LIQ_USD: continue

                    vol5  = float((p.get("volume") or {}).get("m5", 0.0) or 0.0)
                    vol30 = float((p.get("volume") or {}).get("m30", 0.0) or 0.0)
                    ratio = (vol5/vol30) if vol30>0 else 0.0
                    if ratio < VELOCITY_VOL_RATIO_MIN: continue

                    pct5  = float((p.get("priceChange") or {}).get("m5", 0.0) or 0.0)
                    if pct5 < VELOCITY_PRICE_CHANGE_MIN: continue

                    buys5 = int(((p.get("txns") or {}).get("m5") or {}).get("buys", 0) or 0)
                    if buys5 < VELOCITY_BUYS5M_MIN: continue

                    imp = await _impact_bps(liq, ticket)
                    if imp > AI_MAX_IMPACT_BPS: continue

                    if shadow_take("VELOCITY"):
                        await shadow_log("velocity", "candidate", {
                            "mint": mint, "ticket": f"${ticket:,.2f}", "liq": f"${liq:,.0f}",
                            "ratio": f"{ratio:.2f}", "pct5": f"{pct5:.2f}", "impact_bps": f"{imp:.0f}"
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
                            "⚡ BUY (velocity)\n"
                            f"• Mint: {mint}\n"
                            f"• Ticket: ${ticket:,.2f}  • Liq: ${liq:,.0f}\n"
                            f"• Ratio: {ratio:.2f}  • +5m: {pct5:.2f}%  • Impact: {imp:.0f} bps\n"
                            f"• Tx: {sig}"
