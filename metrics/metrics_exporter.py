# metrics_exporter.py — v2 (route histogram + playbook levers)
import os, asyncio, logging
from datetime import datetime, timezone

try:
    import redis.asyncio as aioredis
except Exception:
    aioredis = None

from prometheus_client import Gauge, start_http_server

REDIS_URL     = os.getenv("REDIS_URL", "redis://localhost:6379")
EXPORTER_PORT = int(os.getenv("METRICS_PORT", "9102"))
POLL_SEC      = int(os.getenv("EXPORTER_POLL_SEC", "10"))

METRICS_WINRATE20 = "metrics:winrate20"
METRICS_ROLLING_DD= "metrics:rolling_dd_pct"
EQUITY_HWM_KEY    = "metrics:equity_hwm_usd"
METRICS_TRADES_KEY= "metrics:trades_total"
METRICS_WINS_KEY  = "metrics:wins_total"
BUDGET_KEY_PREFIX = "budget:sniper:spent_usd"
PNL_PREFIX        = "pnl:day:realized_usd"

g_equity_hwm = Gauge("bot_equity_hwm_usd", "High water mark equity in USD")
g_winrate20  = Gauge("bot_winrate20", "Rolling win-rate over last 20 trades (0..1)")
g_rolling_dd = Gauge("bot_rolling_drawdown", "Rolling drawdown vs HWM (0..1)")
g_trades     = Gauge("bot_trades_total", "Total trades observed (counter-like)")
g_wins       = Gauge("bot_wins_total", "Total wins observed (counter-like)")
g_realized   = Gauge("bot_realized_pnl_today_usd", "Realized PnL for today (USD)")
g_budget     = Gauge("bot_spend_today_usd", "Spend counted toward daily budget (USD)")
g_infrahalt  = Gauge("bot_infra_halt", "Infra halt flag (1=halt)")

g_worker_state = Gauge("bot_worker_state", "Worker state: 0=off,1=shadow,2=live", ["worker"])

# Route histogram
g_route_used     = Gauge("bot_route_used_total", "Route usage count by DEX (cumulative)", ["dex"])
g_route_watchdog = Gauge("bot_route_watchdog_hits_total", "Watchdog hits by DEX (cumulative)", ["dex"])

# Playbook levers
g_lever_value       = Gauge("bot_lever_value", "Current lever value (labels carry string value)", ["lever", "value"])
g_lever_last_change = Gauge("bot_lever_last_change", "Unix timestamp of last change", ["lever"])
g_lever_changes     = Gauge("bot_lever_change_total", "Total lever change events logged", ["lever"])

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("metrics_exporter")

def _today_tag() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.year:04d}{now.month:02d}{now.day:02d}"

def _daily_key(prefix: str) -> str:
    return f"{prefix}:{_today_tag()}"

def _worker_status(name: str, enabled_env: str, shadow_env: str) -> int:
    en = os.getenv(enabled_env, "true").lower() == "true"
    sh = os.getenv(shadow_env, "false").lower() == "true"
    if not en: return 0
    return 1 if sh else 2

async def _redis():
    if aioredis is None:
        class Dummy:
            async def get(self, *_a, **_k): return None
            async def scan_iter(self, *_a, **_k):
                if False: yield ""  # pragma: no cover
        return Dummy()
    return aioredis.from_url(REDIS_URL, decode_responses=True)

async def _read_float(r, key, default=0.0):
    try:
        v = await r.get(key); return float(v) if v is not None else default
    except Exception:
        return default

async def collect_loop():
    r = await _redis()
    start_http_server(EXPORTER_PORT)
    logger.info(f"Prometheus exporter listening on :{EXPORTER_PORT}")

    while True:
        try:
            g_equity_hwm.set(await _read_float(r, EQUITY_HWM_KEY, 0.0))
            g_winrate20.set(await _read_float(r, METRICS_WINRATE20, 0.0))
            g_rolling_dd.set(await _read_float(r, METRICS_ROLLING_DD, 0.0))
            g_trades.set(await _read_float(r, METRICS_TRADES_KEY, 0.0))
            g_wins.set(await _read_float(r, METRICS_WINS_KEY, 0.0))
            g_realized.set(await _read_float(r, _daily_key(PNL_PREFIX), 0.0))
            g_budget.set(await _read_float(r, _daily_key(BUDGET_KEY_PREFIX), 0.0))

            halted_vals = []
            for k in ("halt:manual", "halt:infra", "infra:halt", "halt:daily_loss"):
                halted_vals.append(await _read_float(r, k, 0.0))
            g_infrahalt.set(1.0 if any(v and v != 0.0 for v in halted_vals) else 0.0)

            g_worker_state.labels("scalper").set(_worker_status("scalper", "DAYTRADER_ENABLED", "SCALPER_SHADOW_MODE"))
            g_worker_state.labels("whales").set(_worker_status("whales", "WHALE_FEED_ENABLED", "WHALE_SHADOW_MODE"))
            g_worker_state.labels("velocity").set(_worker_status("velocity", "VELOCITY_ENABLED", "VELOCITY_SHADOW_MODE"))
            g_worker_state.labels("hotlist").set(_worker_status("hotlist", "HOTLIST_ENABLED", "HOTLIST_SHADOW_MODE"))
            g_worker_state.labels("sniper").set(_worker_status("sniper", "SNIPER_ENABLED", "SNIPER_SHADOW_MODE"))

            async for key in r.scan_iter(match="route:dex_used:*"):
                dex = key.split(":")[-1]
                g_route_used.labels(dex).set(await _read_float(r, key, 0.0))
            async for key in r.scan_iter(match="route:dex_watchdog:*"):
                dex = key.split(":")[-1]
                g_route_watchdog.labels(dex).set(await _read_float(r, key, 0.0))

            async for key in r.scan_iter(match="playbook:lever:*:value"):
                parts = key.split(":")
                lever = parts[2]
                val = await r.get(key)
                g_lever_value.labels(lever, val if val is not None else "").set(1.0)
                cnt = await _read_float(r, f"playbook:lever:{lever}:changes", 0.0)
                g_lever_changes.labels(lever).set(cnt)
                ts = await _read_float(r, f"playbook:lever:{lever}:last_change_ts", 0.0)
                g_lever_last_change.labels(lever).set(ts)

        except Exception as e:
            logger.error(f"[exporter] collect error: {e}", exc_info=True)

        await asyncio.sleep(POLL_SEC)

if __name__ == "__main__":
    asyncio.run(collect_loop())
