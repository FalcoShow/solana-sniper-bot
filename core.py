# core.py — unified runtime entrypoint

from __future__ import annotations

import asyncio
import logging
import signal
import sys
from typing import Awaitable, Callable

from dotenv import load_dotenv, find_dotenv

# Centralized config
import config as C

# Shared resources & helpers
from utils_pkg.core_utils import (
    init_resources,
    close_resources,
    get_http_session,
    get_redis,
    send_telegram,
)
from utils_pkg.alert_manager import AlertManager

# Scanners (async generators or async funcs returning candidates)
from scanner.dexscreener_scanner import scan_dexscreener
from scanner.pumpfun_scanner import scan_pumpfun
from scanner.raydium_scanner import scan_raydium

# Trading runtime
from sniper.sniper import SniperBot

# Risk & AI loops
from risk_manager import monitor_positions_loop            # root module (kept)
from self_learning_ai_strategy import run_self_learning_loop  # root module (kept)


# --- Jito dynamic tip-accounts bootstrap ------------------------------------
# Ensures JITO_TIP_ACCOUNTS is populated at runtime when left empty in .env.
# Priority: env -> live fetch (getTipAccounts) -> static fallback
import os as _os
import time as _time
import logging as _logging

_jito_log = _logging.getLogger("jito.bootstrap")

_JITO_TIP_STATIC_LIST = [
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
    "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
    "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
    "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
    "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
    "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
]

def _env_bool(name: str, default: bool=False) -> bool:
    v = _os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1","true","yes","y","on")

def _parse_env_tip_accounts(raw: str):
    tips = [t.strip() for t in (raw or "").split(",") if t.strip()]
    # basic sanity check – Solana pubkeys are base58, typically 32 bytes -> ~43-44 chars
    tips = [t for t in tips if 30 <= len(t) <= 50]
    return tips

async def ensure_jito_tip_accounts_env() -> None:
    if not _env_bool("JITO_ENABLED", False):
        return

    raw = _os.getenv("JITO_TIP_ACCOUNTS", "")
    tips = _parse_env_tip_accounts(raw)
    if tips:
        return  # already provided by user

    # Try live fetch
    endpoint = _os.getenv("JITO_BLOCK_ENGINE_URL", "https://mainnet.block-engine.jito.wtf/api/v1/bundles")
    payload = {"jsonrpc": "2.0", "id": 1, "method": "getTipAccounts", "params": []}

    try:
        import aiohttp  # lazy import so modules not using it won't fail on import
        timeout = aiohttp.ClientTimeout(total=5)
        async with aiohttp.ClientSession(timeout=timeout) as _session:
            async with _session.post(endpoint, json=payload) as r:
                if r.status == 200:
                    data = await r.json()
                    live = data.get("result") or []
                    live = _parse_env_tip_accounts(",".join(live))
                    if live:
                        _os.environ["JITO_TIP_ACCOUNTS"] = ",".join(live)
                        _jito_log.info("Jito tip accounts loaded from endpoint (%d).", len(live))
                        return
                _jito_log.info("Jito endpoint response status=%s; falling back to static list.", r.status)
    except Exception as e:
        _jito_log.info("Jito getTipAccounts failed: %s; using static fallback.", e)

    # Static fallback
    _os.environ["JITO_TIP_ACCOUNTS"] = ",".join(_JITO_TIP_STATIC_LIST)
    _jito_log.info("Jito tip accounts set from static list (%d).", len(_JITO_TIP_STATIC_LIST))


async def refresh_jito_tips_daily():
    """
    Refresh Jito tip accounts once per day so we follow any upstream rotations.
    Safe no-op if Jito is disabled.
    """
    while True:
        try:
            await ensure_jito_tip_accounts_env()
        except Exception as _e:
            _jito_log.debug("daily tip refresh error: %s", _e)
        # sleep ~24h
        await asyncio.sleep(24 * 3600)



log = logging.getLogger("core")
logging.basicConfig(
    level=getattr(logging, str(C.LOG_LEVEL).upper(), logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)


async def handle_scanner(
    name: str,
    scanner: Callable[..., Awaitable],
    bot: SniperBot,
    alerts: AlertManager,
):
    """
    Wrap a scanner to:
      - iterate yielded token_data safely
      - submit to SniperBot with rate-limits already inside bot
      - surface errors via AlertManager (batched)
    """
    try:
        async for token in scanner(get_redis(), get_http_session()):
            if not token:
                continue
            # Normalize minimum expected keys (defensive)
            token_mint = token.get("mint") or token.get("token") or token.get("address")
            sym = token.get("symbol") or (token_mint[:6] if token_mint else "UNKNOWN")

            try:
                await bot.execute_sniper(token)  # bot handles buy size, fraud checks, slippage, etc.
                log.info("✅ [%s] submitted %s (%s)", name, sym, token_mint)
            except Exception as e:
                msg = f"❌ [{name}] error submitting {sym} ({token_mint}): {e}"
                await alerts.queue(msg)
                log.exception(msg)

    except asyncio.CancelledError:
        raise
    except Exception as e:
        msg = f"⚠️ Scanner {name} crashed: {e}"
        await alerts.queue(msg)
        log.exception(msg)


async def _graceful_cancel(tasks: list[asyncio.Task]):
    for t in tasks:
        t.cancel()
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for r in results:
        if isinstance(r, Exception) and not isinstance(r, asyncio.CancelledError):
            log.warning("task ended with error: %s", r)


async def main():
    # 1) env + config summary
    # Ensure Jito tip accounts env is populated when left blank
    await ensure_jito_tip_accounts_env()
    # 2) global resources (Redis + HTTP + SQLite)
    await init_resources()
    redis = get_redis()
    session = get_http_session()

    # 3) alert batching
    alerts = AlertManager(batch_size=5, flush_interval=10)
    alert_task = asyncio.create_task(alerts.run(), name="alerts.run")

    # 4) sniper bot instance (holds live positions, AI params refresh, arb loop, etc.)
    bot = SniperBot(redis=redis, session=session, alert_manager=alerts)
    await bot.start()  # starts: AI loop, monitor loop, arb loop (internally)

    # 5) external loops not owned by bot
    loops: list[asyncio.Task] = []

    # Risk manager loop (trailing SL, emergency exits)
    loops.append(asyncio.create_task(monitor_positions_loop(), name="risk.loop"))

    # Optional self-learning param tuner (kept; bot already refreshes params)
    loops.append(asyncio.create_task(run_self_learning_loop(redis), name="ai.tuner"))

    # Scanners: run forever with SCAN_INTERVAL pacing
    async def scanner_loop(name, fn):
        while True:
            try:
                await handle_scanner(name, fn, bot, alerts)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                await alerts.queue(f"⚠️ {name} loop error: {e}")
                log.exception("[%s] scan loop error", name)
            await asyncio.sleep(C.SCAN_INTERVAL)

    if C.ENABLE_DEXSCREENER_SCANNER:
        loops.append(asyncio.create_task(scanner_loop("Dexscreener", scan_dexscreener), name="scan.dexscreener"))
    if C.ENABLE_PUMPFUN_SCANNER:
        loops.append(asyncio.create_task(scanner_loop("Pump.fun", scan_pumpfun), name="scan.pumpfun"))
    if C.ENABLE_RAYDIUM_SCANNER:
        loops.append(asyncio.create_task(scanner_loop("Raydium", scan_raydium), name="scan.raydium"))

    # 6) graceful shutdown handlers
    stop = asyncio.Event()

    def _signal(*_):
        stop.set()

    try:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _signal)
            except NotImplementedError:
                # Windows terminal may not support add_signal_handler for SIGTERM
                pass
    except RuntimeError:
        pass

    log.info("🚀 Runtime started (scan interval=%ss)", C.SCAN_INTERVAL)

    try:
        await stop.wait()
    finally:
        log.info("🛑 Shutting down…")
        await _graceful_cancel(loops)
        alert_task.cancel()
        try:
            await alert_task
        except Exception:
            pass
        await bot.close()
        await close_resources()
        log.info("✅ Clean exit")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception as e:
        log.exception("Fatal: %s", e)
        # best-effort alert if Telegram is configured
        try:
            asyncio.run(send_telegram(f"🔥 Fatal in core: {e}"))
        except Exception:
            pass
        sys.exit(1)