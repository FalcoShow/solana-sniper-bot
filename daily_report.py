# daily_report.py
import os, asyncio, logging
from datetime import datetime, timezone
try:
    import redis.asyncio as aioredis
except Exception:
    aioredis = None

from risk_manager import current_limits
from utils_pkg.core_utils import send_telegram

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("daily_report")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

REPORT_ENABLED = os.getenv("REPORT_ENABLED", "true").lower() == "true"
REPORT_HOUR_UTC = int(os.getenv("REPORT_HOUR_UTC", "23"))
REPORT_MINUTE_UTC = int(os.getenv("REPORT_MINUTE_UTC", "59"))

# keys that risk_manager writes
METRICS_WINRATE20 = "metrics:winrate20"
METRICS_ROLLING_DD = "metrics:rolling_dd_pct"
EQUITY_HWM_KEY = "metrics:equity_hwm_usd"

def _today_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")

def _pnl_key_today() -> str:
    return f"pnl:day:realized_usd:{_today_tag()}"

def _sent_key() -> str:
    return f"report:sent:{_today_tag()}"

def _worker_status(name: str, enabled_var: str, shadow_var: str) -> str:
    en = os.getenv(enabled_var, "true").lower() == "true"
    sh = os.getenv(shadow_var, "false").lower() == "true"
    if not en:
        return f"{name}: off"
    return f"{name}: {'shadow' if sh else 'live'}"

async def _get_redis():
    if aioredis is None:
        return None
    return aioredis.from_url(REDIS_URL, decode_responses=True)

async def _fetch_float(r, key, default=0.0):
    try:
        v = await r.get(key)
        return float(v) if v is not None else default
    except Exception:
        return default

async def run_report():
    if not REPORT_ENABLED:
        logger.info("Report disabled")
        return
    r = await _get_redis()
    if not r:
        logger.warning("No Redis available; exiting.")
        return

    await send_telegram("📝 Daily report worker started.")

    while True:
        try:
            now = datetime.now(timezone.utc)
            if now.hour == REPORT_HOUR_UTC and now.minute == REPORT_MINUTE_UTC:
                if await r.get(_sent_key()) == "1":
                    await asyncio.sleep(30)
                    continue

                limits = await current_limits(r)
                realized = await _fetch_float(r, _pnl_key_today(), 0.0)
                winrate20 = await _fetch_float(r, METRICS_WINRATE20, 0.0) * 100.0
                rolling_dd = await _fetch_float(r, METRICS_ROLLING_DD, 0.0) * 100.0
                hwm = await _fetch_float(r, EQUITY_HWM_KEY, 0.0)

                # worker statuses
                workers = [
                    _worker_status("scalper", "DAYTRADER_ENABLED", "SCALPER_SHADOW_MODE"),
                    _worker_status("whales", "WHALE_FEED_ENABLED", "WHALE_SHADOW_MODE"),
                    _worker_status("velocity", "VELOCITY_ENABLED", "VELOCITY_SHADOW_MODE"),
                    _worker_status("hotlist", "HOTLIST_ENABLED", "HOTLIST_SHADOW_MODE"),
                ]
                text = (
                    f"📊 *Daily Summary* (UTC {now:%Y-%m-%d})\n"
                    f"• Equity: ${limits['equity_usd']:,.2f}  (HWM ${hwm:,.2f})\n"
                    f"• Realized PnL (today): ${realized:,.2f}\n"
                    f"• Winrate-20: {winrate20:.1f}%   • Rolling DD: {rolling_dd:.1f}%\n"
                    f"• Tier: ticket ${limits['ticket_usd']:,.2f}  • max_conc {limits['max_concurrent']}  • daily_budget ${limits['daily_spend_usd']:,.0f}\n"
                    f"• Workers: {', '.join(workers)}\n"
                    f"• Stop-after-profit: {os.getenv('STOP_AFTER_DAILY_PROFIT_PCT','4')}%  • Spend cap: {int(float(os.getenv('DAILY_SPEND_CAP_PCT','0.15'))*100)}%"
                )

                await send_telegram(text)
                await r.set(_sent_key(), "1", ex=26*3600)  # don't resend today

            await asyncio.sleep(30)
        except Exception as e:
            logger.error(f"[report] loop error: {e}", exc_info=True)
            await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(run_report())
