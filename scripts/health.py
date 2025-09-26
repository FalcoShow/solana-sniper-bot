from __future__ import annotations
import asyncio, json, os
from aiohttp import web
from logging_setup import get_logger

log = get_logger()

async def handle_health(request):
    return web.Response(text="ok", status=200)

async def handle_stats(request):
    # Minimal surface; extend as needed
    try:
        import utils_pkg.guardrails as gr  # type: ignore
        s = gr.snapshot()
    except Exception:
        s = {}
    return web.json_response({
        "uptime_sec": int(asyncio.get_event_loop().time()),
        "guardrails": s,
        "env": {
            "DRY_RUN": os.getenv("DRY_RUN","0"),
            "PF_PRIORITY_TARGET_SEC": os.getenv("PF_PRIORITY_TARGET_SEC","2.0"),
            "PF_PRICE_TTL_SEC": os.getenv("PF_PRICE_TTL_SEC","5"),
        }
    })

async def create_app():
    app = web.Application()
    app.router.add_get("/healthz", handle_health)
    app.router.add_get("/stats", handle_stats)
    return app

def run():
    web.run_app(asyncio.run(create_app()), port=int(os.getenv("PORT","8080")))

if __name__ == "__main__":
    run()
