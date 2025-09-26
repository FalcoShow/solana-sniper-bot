# playbook_logger.py — v1 (CLI to record lever changes; logs to Redis + Notion)
import os, argparse, time, json, logging, asyncio
from datetime import datetime, timezone

try:
    import redis.asyncio as aioredis
except Exception:
    aioredis = None

import aiohttp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("playbook")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
NOTION_TOKEN = os.getenv("NOTION_TOKEN") or os.getenv("NOTION_API_KEY")
NOTION_DB_PLAYBOOK = os.getenv("NOTION_DB_PLAYBOOK")

async def _redis():
    if aioredis is None:
        class Dummy:
            async def set(self, *_a, **_k): pass
            async def incrby(self, *_a, **_k): pass
        return Dummy()
    return aioredis.from_url(REDIS_URL, decode_responses=True)

async def _notion_create(lever: str, old: str, new: str, note: str, kpi_before: str, kpi_after: str, ts: float):
    if not (NOTION_TOKEN and NOTION_DB_PLAYBOOK):
        return
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json"
    }
    body = {
      "parent": {"database_id": NOTION_DB_PLAYBOOK},
      "properties": {
        "Lever": {"title": [{"type": "text", "text": {"content": lever}}]},
        "Old": {"rich_text": [{"type": "text", "text": {"content": str(old)}}]},
        "New": {"rich_text": [{"type": "text", "text": {"content": str(new)}}]},
        "Reason": {"rich_text": [{"type": "text", "text": {"content": note or ""}}]},
        "KPIs Before": {"rich_text": [{"type": "text", "text": {"content": kpi_before or ""}}]},
        "KPIs After": {"rich_text": [{"type": "text", "text": {"content": kpi_after or ""}}]},
        "Timestamp": {"date": {"start": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()}}
      }
    }
    async with aiohttp.ClientSession() as s:
        async with s.post("https://api.notion.com/v1/pages", headers=headers, data=json.dumps(body)) as r:
            if r.status not in (200, 201):
                msg = await r.text()
                logger.warning(f"[notion] create failed: {r.status} {msg[:200]}")

async def log_lever_change(lever: str, old: str, new: str, reason: str = "", kpi_before: str = "", kpi_after: str = ""):
    r = await _redis()
    ts = time.time()
    try:
        await r.set(f"playbook:lever:{lever}:value", str(new))
        await r.set(f"playbook:lever:{lever}:old_value", str(old))
        await r.set(f"playbook:lever:{lever}:note", reason or "")
        await r.set(f"playbook:lever:{lever}:kpi_before", kpi_before or "")
        await r.set(f"playbook:lever:{lever}:kpi_after", kpi_after or "")
        await r.set(f"playbook:lever:{lever}:last_change_ts", ts)
        await r.incrby(f"playbook:lever:{lever}:changes", 1)
    except Exception:
        pass
    await _notion_create(lever, old, new, reason, kpi_before, kpi_after, ts)

def _parse():
    p = argparse.ArgumentParser(description="Record a lever change (Redis + Notion)")
    p.add_argument("--lever", required=True, help="e.g. VELOCITY_VOL_RATIO_MIN")
    p.add_argument("--old", required=True, help="old value")
    p.add_argument("--new", required=True, help="new value")
    p.add_argument("--reason", default="", help="why you changed it")
    p.add_argument("--kpi_before", default="", help="short KPI snapshot before")
    p.add_argument("--kpi_after", default="", help="short KPI snapshot after (fill later if unknown)")
    return p.parse_args()

if __name__ == "__main__":
    args = _parse()
    asyncio.run(log_lever_change(args.lever, args.old, args.new, args.reason, args.kpi_before, args.kpi_after))
