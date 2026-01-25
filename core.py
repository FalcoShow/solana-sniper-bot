# core.py
from __future__ import annotations

"""
core.py — Lifecycle + task wiring + shutdown (God-Tier Overhaul / V3+++ LOCK-IN)
===============================================================================

Canonical runtime orchestrator. It wires:
- env bootstrap (optional dotenv for local)
- logging bootstrap (root logging_setup.py is canonical)
- proxy install (if enabled)
- shared resources init/close (utils_pkg.core_utils)
- AlertManager background loop
- SniperBot lifecycle
- scanner loops (Raydium / Pumpfun / Dexscreener / others as added)
- optional “support loops” (risk monitor, self-learning tuner, jito tips refresh, app server)
- graceful shutdown on SIGINT/SIGTERM or external stop_evt

Entry contract (used by main.py)
--------------------------------
- async amain(argv: list[str] | None = None, stop_evt: asyncio.Event | None = None) -> int
- async run(argv: list[str] | None = None, stop_evt: asyncio.Event | None = None) -> int   (alias)
- def main(argv: list[str] | None = None) -> None
"""

import asyncio
import contextlib
import inspect
import logging
import os
import random
import signal
import sys
import time
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import Any

log = logging.getLogger("core")


# =============================================================================
# Logging bootstrap (root logging_setup.py is canonical)
# =============================================================================


def _configure_logging() -> None:
    # 1) canonical
    try:
        import logging_setup  # type: ignore

        fn = getattr(logging_setup, "setup_logging", None)
        if callable(fn):
            fn()
            return
    except Exception:
        pass

    # 2) wrapper (until dedup is complete)
    try:
        from utils_pkg.logging_setup import setup_logging  # type: ignore

        setup_logging()
        return
    except Exception:
        pass

    # 3) fallback
    level = os.environ.get("LOG_LEVEL", "INFO").strip().upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


# =============================================================================
# Optional dotenv for local (Render start.sh typically injects env already)
# =============================================================================


def _maybe_load_dotenv() -> None:
    """
    Local convenience only. Never required in Render.
    Controlled by env CORE_LOAD_DOTENV=1 (default: auto when a .env exists).
    """
    try:
        want = os.environ.get("CORE_LOAD_DOTENV")
        if want is not None and want.strip().lower() in {"0", "false", "no", "off"}:
            return

        is_render = bool(os.environ.get("RENDER")) or bool(os.environ.get("RENDER_SERVICE_ID"))
        if is_render and want is None:
            return

        from dotenv import find_dotenv, load_dotenv  # type: ignore

        path = find_dotenv(usecwd=True)
        if path:
            load_dotenv(path, override=False)
            log.info("dotenv loaded (local): %s", os.path.basename(path))
    except Exception:
        return


# =============================================================================
# Tiny helpers
# =============================================================================


def _truthy(v: str | None) -> bool:
    if v is None:
        return False
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


def _mask(s: str, keep_head: int = 4, keep_tail: int = 4) -> str:
    if not s:
        return ""
    s = str(s)
    if len(s) <= keep_head + keep_tail + 3:
        return s[: max(1, keep_head)] + "…"
    return f"{s[:keep_head]}…{s[-keep_tail:]}"


# =============================================================================
# Playbook logger (runbook-grade “why not trading” events)
# =============================================================================


class Playbook:
    def __init__(self) -> None:
        try:
            import playbook_logger as pl  # type: ignore

            self._pl = pl
        except Exception:
            self._pl = None

    def event(self, kind: str, msg: str, **fields: Any) -> None:
        """
        Best-effort structured event:
          - playbook_logger.log_event(kind=..., msg=..., **fields)
          - playbook_logger.event(...)
          - fallback to log.info
        """
        if self._pl is not None:
            for name in ("log_event", "event", "emit", "write"):
                fn = getattr(self._pl, name, None)
                if callable(fn):
                    try:
                        fn(kind=kind, msg=msg, **fields)
                        return
                    except Exception:
                        break

        extra = " ".join(f"{k}={fields[k]}" for k in sorted(fields.keys()))
        log.info("[playbook] %s: %s %s", kind, msg, extra)


PB = Playbook()


# =============================================================================
# Backoff for restartable loops
# =============================================================================


@dataclass
class Backoff:
    base_s: float = 2.0
    max_s: float = 60.0
    factor: float = 1.8
    jitter: float = 0.25
    _cur: float = 0.0

    def reset(self) -> None:
        self._cur = 0.0

    def next_sleep(self) -> float:
        if self._cur <= 0:
            self._cur = self.base_s
        else:
            self._cur = min(self.max_s, self._cur * self.factor)
        j = self._cur * self.jitter
        return max(0.0, self._cur + random.uniform(-j, j))


# =============================================================================
# Shared resources (utils_pkg.core_utils is canonical)
# =============================================================================


@dataclass
class RuntimeDeps:
    config: Any
    core_utils: Any
    redis: Any
    session: Any
    alerts: Any
    bot: Any


async def _load_core_utils() -> Any:
    try:
        from utils_pkg import core_utils as cu  # type: ignore

        return cu
    except Exception as e:
        raise RuntimeError(f"Cannot import utils_pkg.core_utils: {e}") from e


async def _init_resources(cu: Any) -> None:
    fn = (
        getattr(cu, "init_resources", None)
        or getattr(cu, "init_all", None)
        or getattr(cu, "init", None)
    )
    if callable(fn):
        await _maybe_await(fn())


async def _close_resources(cu: Any) -> None:
    fn = (
        getattr(cu, "close_resources", None)
        or getattr(cu, "close_all", None)
        or getattr(cu, "close", None)
    )
    if callable(fn):
        await _maybe_await(fn())


async def _get_redis(cu: Any) -> Any:
    fn = (
        getattr(cu, "get_redis", None)
        or getattr(cu, "get_redis_async", None)
        or getattr(cu, "redis", None)
    )
    if callable(fn):
        return await _maybe_await(fn())
    return fn


async def _get_session(cu: Any) -> Any:
    fn = (
        getattr(cu, "get_http_session", None)
        or getattr(cu, "get_session", None)
        or getattr(cu, "session", None)
    )
    if callable(fn):
        return await _maybe_await(fn())
    return fn


async def _send_telegram_best_effort(cu: Any, text: str) -> None:
    fn = getattr(cu, "send_telegram", None) or getattr(cu, "telegram_send", None)
    if callable(fn):
        with contextlib.suppress(Exception):
            await _maybe_await(fn(text))


# =============================================================================
# Proxy bootstrap (global aiohttp proxy defaults)
# =============================================================================


def _install_proxy_globals() -> None:
    try:
        from utils_pkg.proxy_client import install_global_aiohttp_proxy  # type: ignore

        install_global_aiohttp_proxy()
        PB.event("infra", "proxy globals installed")
    except Exception as e:
        PB.event("infra", "proxy globals not installed (non-fatal)", err=str(e))


async def _proxy_selfcheck(config: Any) -> None:
    want = _truthy(os.environ.get("PROXY_HEALTHCHECK")) or bool(
        getattr(config, "PROXY_HEALTHCHECK", False)
    )
    if not want:
        return

    strict = _truthy(os.environ.get("PROXY_STRICT")) or bool(getattr(config, "PROXY_STRICT", False))
    url = os.environ.get("PROXY_HEALTHCHECK_URL", "https://api.ipify.org?format=json")
    timeout_s = float(os.environ.get("PROXY_HEALTHCHECK_TIMEOUT_S", "6"))

    try:
        from utils_pkg.proxy_client import http_get, make_session  # type: ignore

        async with make_session() as s:
            status, _body = await http_get(s, url, timeout=timeout_s)  # type: ignore
            if int(status) == 200:
                PB.event("infra", "proxy healthcheck ok", url=url)
                return
            msg = f"proxy healthcheck bad status={status}"
            PB.event("infra", msg, url=url)
            if strict:
                raise RuntimeError(msg)
    except Exception as e:
        PB.event("infra", "proxy healthcheck failed", err=str(e), url=url)
        if strict:
            raise


# =============================================================================
# Jito tip accounts bootstrap (safe best-effort)
# =============================================================================

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


def _parse_tip_accounts(raw: str) -> list[str]:
    tips = [t.strip() for t in (raw or "").split(",") if t.strip()]
    tips = [t for t in tips if 30 <= len(t) <= 60]
    seen: set[str] = set()
    out: list[str] = []
    for t in tips:
        if t not in seen:
            out.append(t)
            seen.add(t)
    return out


async def ensure_jito_tip_accounts_env() -> None:
    if not _truthy(os.environ.get("JITO_ENABLED")):
        return

    tips = _parse_tip_accounts(os.environ.get("JITO_TIP_ACCOUNTS", "") or "")
    if tips:
        return

    rpc_url = os.environ.get("JITO_TIP_ACCOUNTS_RPC") or os.environ.get("JITO_RPC_URL") or ""
    if rpc_url:
        try:
            import aiohttp

            payload = {"jsonrpc": "2.0", "id": 1, "method": "getTipAccounts", "params": []}
            timeout = aiohttp.ClientTimeout(total=6)

            # ✅ SIM117 fix: single async-with statement
            async with (
                aiohttp.ClientSession(timeout=timeout) as s,
                s.post(rpc_url, json=payload) as r,
            ):
                if int(r.status) == 200:
                    data = await r.json(content_type=None)
                    live = data.get("result") or []
                    tips = _parse_tip_accounts(",".join(map(str, live)))
                    if tips:
                        os.environ["JITO_TIP_ACCOUNTS"] = ",".join(tips)
                        PB.event("infra", "jito tip accounts loaded (live)", n=len(tips))
                        return
        except Exception as e:
            PB.event("infra", "jito tip accounts fetch failed", err=str(e))

    os.environ["JITO_TIP_ACCOUNTS"] = ",".join(_JITO_TIP_STATIC_LIST)
    PB.event("infra", "jito tip accounts set (static)", n=len(_JITO_TIP_STATIC_LIST))


async def refresh_jito_tips_daily(stop_evt: asyncio.Event) -> None:
    while not stop_evt.is_set():
        with contextlib.suppress(Exception):
            await ensure_jito_tip_accounts_env()
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop_evt.wait(), timeout=24 * 3600)


# =============================================================================
# Scanner plumbing (accepts many shapes)
# =============================================================================


async def _call_scanner(scanner_fn: Any, redis: Any, session: Any) -> Any:
    if not callable(scanner_fn):
        return scanner_fn

    try:
        sig = inspect.signature(scanner_fn)
        params = sig.parameters
        kwargs: dict[str, Any] = {}

        if "redis" in params:
            kwargs["redis"] = redis
        if "session" in params:
            kwargs["session"] = session

        if kwargs:
            return await _maybe_await(scanner_fn(**kwargs))

        if len(params) >= 2:
            return await _maybe_await(scanner_fn(redis, session))
        if len(params) == 1:
            try:
                return await _maybe_await(scanner_fn(redis))
            except Exception:
                return await _maybe_await(scanner_fn(session))

        return await _maybe_await(scanner_fn())
    except Exception:
        return await _maybe_await(scanner_fn())


async def _iter_candidates(
    scanner_fn: Any, redis: Any, session: Any
) -> AsyncIterator[dict[str, Any]]:
    out = await _call_scanner(scanner_fn, redis, session)

    if hasattr(out, "__aiter__"):
        async for item in out:  # type: ignore
            if isinstance(item, dict):
                yield item
            elif item is not None:
                with contextlib.suppress(Exception):
                    yield dict(item)  # type: ignore
        return

    if hasattr(out, "__iter__") and not isinstance(out, (dict, str, bytes)):
        for item in out:  # type: ignore
            if isinstance(item, dict):
                yield item
            elif item is not None:
                with contextlib.suppress(Exception):
                    yield dict(item)  # type: ignore
        return

    if isinstance(out, dict):
        yield out


async def _bot_submit(bot: Any, candidate: dict[str, Any]) -> None:
    for name in (
        "execute_sniper",
        "submit_candidate",
        "on_candidate",
        "handle_candidate",
        "process_candidate",
    ):
        fn = getattr(bot, name, None)
        if callable(fn):
            await _maybe_await(fn(candidate))
            return
    raise RuntimeError("SniperBot has no supported submit method")


# =============================================================================
# Runtime loops (restartable)
# =============================================================================


async def scanner_loop(
    stop_evt: asyncio.Event,
    name: str,
    scanner_fn: Any,
    bot: Any,
    alerts: Any,
    redis: Any,
    session: Any,
    scan_interval_s: float,
) -> None:
    backoff = Backoff(base_s=2.0, max_s=60.0)
    last_emit = 0.0
    seen = 0

    PB.event("scanner", "scanner loop starting", name=name, interval_s=scan_interval_s)

    while not stop_evt.is_set():
        try:
            async for cand in _iter_candidates(scanner_fn, redis, session):
                if stop_evt.is_set():
                    break

                mint = cand.get("mint") or cand.get("token") or cand.get("address") or ""
                sym = (
                    cand.get("symbol")
                    or cand.get("sym")
                    or (mint[:6] if isinstance(mint, str) and mint else "UNKNOWN")
                )

                try:
                    await _bot_submit(bot, cand)
                    seen += 1
                    now = time.time()
                    if now - last_emit > 2.0:
                        log.info("✅ [%s] submitted %s (%s)", name, sym, _mask(str(mint)))
                        last_emit = now
                except Exception as e:
                    msg = f"❌ [{name}] submit error {sym} ({_mask(str(mint))}): {e}"
                    q = getattr(alerts, "queue", None)
                    if callable(q):
                        with contextlib.suppress(Exception):
                            await _maybe_await(q(msg))
                    log.exception(msg)

            backoff.reset()

            if scan_interval_s > 0:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop_evt.wait(), timeout=scan_interval_s)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            msg = f"⚠️ Scanner {name} crashed: {e}"
            q = getattr(alerts, "queue", None)
            if callable(q):
                with contextlib.suppress(Exception):
                    await _maybe_await(q(msg))
            log.exception("[%s] crashed", name)
            PB.event("scanner", "scanner crashed", name=name, err=str(e), seen=seen)

            sleep_s = backoff.next_sleep()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop_evt.wait(), timeout=sleep_s)

    PB.event("scanner", "scanner loop stopped", name=name, seen=seen)


async def _run_alerts_loop(stop_evt: asyncio.Event, alerts: Any) -> None:
    run = getattr(alerts, "run", None)
    start = getattr(alerts, "start", None)
    close = getattr(alerts, "close", None)

    if callable(run):
        task = asyncio.create_task(run(), name="alerts.run")
        try:
            await stop_evt.wait()
        finally:
            task.cancel()
            with contextlib.suppress(Exception):
                await task
            if callable(close):
                with contextlib.suppress(Exception):
                    await _maybe_await(close())
        return

    if callable(start):
        await _maybe_await(start())
        await stop_evt.wait()
        if callable(close):
            await _maybe_await(close())


async def _run_support_loop(
    stop_evt: asyncio.Event, name: str, fn: Callable[..., Any], *args: Any
) -> None:
    backoff = Backoff(base_s=3.0, max_s=120.0)
    PB.event("support", "support loop starting", name=name)

    while not stop_evt.is_set():
        try:
            await _maybe_await(fn(*args))
            backoff.reset()
            sleep_s = backoff.next_sleep()
            PB.event("support", "support loop returned; restarting", name=name, sleep_s=sleep_s)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop_evt.wait(), timeout=sleep_s)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            PB.event("support", "support loop crashed", name=name, err=str(e))
            log.exception("[%s] support loop crashed", name)
            sleep_s = backoff.next_sleep()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop_evt.wait(), timeout=sleep_s)

    PB.event("support", "support loop stopped", name=name)


async def _heartbeat(stop_evt: asyncio.Event, config: Any, bot: Any) -> None:
    sec = float(
        os.environ.get("PLAYBOOK_HEARTBEAT_SEC", "")
        or getattr(config, "PLAYBOOK_HEARTBEAT_SEC", 60.0)
        or 60.0
    )
    sec = max(10.0, sec)

    while not stop_evt.is_set():
        try:
            state: dict[str, Any] = {}
            for attr in ("enabled", "paused", "mode", "positions", "open_positions"):
                if hasattr(bot, attr):
                    v = getattr(bot, attr)
                    if isinstance(v, (list, tuple, set, dict)):
                        state[attr] = len(v)
                    else:
                        state[attr] = v

            toggles = {
                "pause_trading": bool(getattr(config, "PAUSE_TRADING", False)),
                "kill_switch": bool(getattr(config, "KILL_SWITCH", False)),
                "disable_buys": bool(getattr(config, "DISABLE_BUYS", False)),
                "disable_sells": bool(getattr(config, "DISABLE_SELLS", False)),
            }

            PB.event("heartbeat", "runtime ok", **toggles, **{f"bot_{k}": state[k] for k in state})
        except Exception:
            pass

        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop_evt.wait(), timeout=sec)


# =============================================================================
# Core orchestrator
# =============================================================================


async def _build_runtime() -> RuntimeDeps:
    _maybe_load_dotenv()
    _configure_logging()

    import config as C  # type: ignore

    cu = await _load_core_utils()

    _install_proxy_globals()
    await _proxy_selfcheck(C)
    await ensure_jito_tip_accounts_env()

    await _init_resources(cu)
    redis = await _get_redis(cu)
    session = await _get_session(cu)

    from utils_pkg.alert_manager import AlertManager  # type: ignore

    alerts = AlertManager(
        batch_size=int(getattr(C, "ALERT_BATCH_SIZE", 5) or 5),
        flush_interval=int(getattr(C, "ALERT_FLUSH_SEC", 10) or 10),
    )

    from sniper.sniper import SniperBot  # type: ignore

    try:
        bot = SniperBot(redis=redis, session=session, alert_manager=alerts)  # type: ignore
    except Exception:
        try:
            bot = SniperBot(redis, session, alerts)  # type: ignore
        except Exception:
            bot = SniperBot()  # type: ignore

    for name in ("start", "init", "amain"):
        fn = getattr(bot, name, None)
        if callable(fn):
            await _maybe_await(fn())
            break

    PB.event(
        "core",
        "runtime built",
        env=str(getattr(C, "ENV", "")),
        scan_interval=float(getattr(C, "SCAN_INTERVAL", 5)),
    )

    return RuntimeDeps(
        config=C, core_utils=cu, redis=redis, session=session, alerts=alerts, bot=bot
    )


async def _shutdown_runtime(rt: RuntimeDeps) -> None:
    for name in ("close", "stop", "shutdown"):
        fn = getattr(rt.bot, name, None)
        if callable(fn):
            with contextlib.suppress(Exception):
                await _maybe_await(fn())
            break

    with contextlib.suppress(Exception):
        await _close_resources(rt.core_utils)


def _install_signals(loop: asyncio.AbstractEventLoop, stop_evt: asyncio.Event) -> None:
    def _stop() -> None:
        if not stop_evt.is_set():
            stop_evt.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:
            with contextlib.suppress(Exception):
                signal.signal(sig, lambda *_: _stop())


async def amain(argv: list[str] | None = None, stop_evt: asyncio.Event | None = None) -> int:
    argv = argv or []
    external_stop = stop_evt is not None
    stop_evt = stop_evt or asyncio.Event()

    loop = asyncio.get_running_loop()
    _install_signals(loop, stop_evt)

    rt: RuntimeDeps | None = None
    try:
        rt = await _build_runtime()
        C = rt.config

        support_tasks: list[asyncio.Task] = []
        scanner_tasks: list[asyncio.Task] = []

        support_tasks.append(
            asyncio.create_task(_run_alerts_loop(stop_evt, rt.alerts), name="alerts.loop")
        )
        support_tasks.append(asyncio.create_task(_heartbeat(stop_evt, C, rt.bot), name="heartbeat"))

        if _truthy(os.environ.get("JITO_ENABLED")):
            support_tasks.append(
                asyncio.create_task(refresh_jito_tips_daily(stop_evt), name="jito.refresh")
            )

        # Risk monitor loop (defensive import)
        if bool(getattr(C, "ENABLE_RISK_LOOP", True)):
            rm_fn = None
            try:
                from utils_pkg.risk_manager import monitor_positions_loop as rm_fn  # type: ignore
            except Exception:
                try:
                    from risk_manager import monitor_positions_loop as rm_fn  # type: ignore
                except Exception:
                    rm_fn = None
            if callable(rm_fn):
                support_tasks.append(
                    asyncio.create_task(
                        _run_support_loop(stop_evt, "risk.loop", rm_fn), name="risk.loop"
                    )
                )

        # Self-learning loop (defensive import)
        if bool(getattr(C, "ENABLE_SELF_LEARNING_LOOP", False)):
            sl_fn = None
            try:
                from utils_pkg.self_learning_ai_strategy import (
                    run_self_learning_loop as sl_fn,  # type: ignore
                )
            except Exception:
                try:
                    from self_learning_ai_strategy import (
                        run_self_learning_loop as sl_fn,  # type: ignore
                    )
                except Exception:
                    sl_fn = None
            if callable(sl_fn):
                support_tasks.append(
                    asyncio.create_task(
                        _run_support_loop(stop_evt, "ai.tuner", sl_fn, rt.redis),
                        name="ai.tuner",
                    )
                )

        # Optional app server (health/metrics) — only if ENABLE_APP_SERVER=true
        if bool(getattr(C, "ENABLE_APP_SERVER", False)):
            try:
                import app as app_mod  # type: ignore

                fn = (
                    getattr(app_mod, "amain", None)
                    or getattr(app_mod, "run", None)
                    or getattr(app_mod, "main", None)
                )
                if callable(fn):
                    support_tasks.append(
                        asyncio.create_task(
                            _run_support_loop(stop_evt, "app.server", fn, stop_evt),
                            name="app.server",
                        )
                    )
            except Exception as e:
                PB.event("app", "app server not started", err=str(e))

        scan_interval = float(getattr(C, "SCAN_INTERVAL", 5.0) or 5.0)

        # Scanners (enable flags from config; import only if enabled)
        if bool(getattr(C, "ENABLE_DEXSCREENER_SCANNER", False)):
            from scanner.dexscreener_scanner import scan_dexscreener  # type: ignore

            scanner_tasks.append(
                asyncio.create_task(
                    scanner_loop(
                        stop_evt,
                        "Dexscreener",
                        scan_dexscreener,
                        rt.bot,
                        rt.alerts,
                        rt.redis,
                        rt.session,
                        scan_interval,
                    ),
                    name="scan.dexscreener",
                )
            )

        if bool(getattr(C, "ENABLE_PUMPFUN_SCANNER", False)):
            from scanner.pumpfun_scanner import scan_pumpfun  # type: ignore

            scanner_tasks.append(
                asyncio.create_task(
                    scanner_loop(
                        stop_evt,
                        "Pump.fun",
                        scan_pumpfun,
                        rt.bot,
                        rt.alerts,
                        rt.redis,
                        rt.session,
                        scan_interval,
                    ),
                    name="scan.pumpfun",
                )
            )

        if bool(getattr(C, "ENABLE_RAYDIUM_SCANNER", False)):
            from scanner.raydium_scanner import scan_raydium  # type: ignore

            scanner_tasks.append(
                asyncio.create_task(
                    scanner_loop(
                        stop_evt,
                        "Raydium",
                        scan_raydium,
                        rt.bot,
                        rt.alerts,
                        rt.redis,
                        rt.session,
                        scan_interval,
                    ),
                    name="scan.raydium",
                )
            )

        if not scanner_tasks:
            PB.event("core", "no scanners enabled (bot will not discover entries)")
            log.warning("No scanners enabled. Enable at least one scanner in config/env.")

        PB.event(
            "core",
            "runtime started",
            scanners=len(scanner_tasks),
            support=len(support_tasks),
            argv=" ".join(argv),
        )
        log.info(
            "🚀 Runtime started (scan_interval=%ss) scanners=%d", scan_interval, len(scanner_tasks)
        )

        await stop_evt.wait()
        PB.event("core", "stop requested", external=external_stop)

        for t in scanner_tasks + support_tasks:
            t.cancel()
        await asyncio.gather(*scanner_tasks, return_exceptions=True)
        await asyncio.gather(*support_tasks, return_exceptions=True)

        return 0

    except asyncio.CancelledError:
        raise
    except Exception as e:
        PB.event("core", "fatal error", err=str(e))
        log.exception("Fatal in core: %s", e)
        if rt is not None:
            await _send_telegram_best_effort(rt.core_utils, f"🔥 Fatal in core: {e}")
        return 2
    finally:
        if rt is not None:
            with contextlib.suppress(Exception):
                PB.event("core", "shutting down")
            await _shutdown_runtime(rt)
            PB.event("core", "clean exit")


async def run(argv: list[str] | None = None, stop_evt: asyncio.Event | None = None) -> int:
    return await amain(argv=argv, stop_evt=stop_evt)


def main(argv: list[str] | None = None) -> None:
    argv = argv or sys.argv[1:]
    try:
        rc = asyncio.run(amain(argv=argv))
    except KeyboardInterrupt:
        rc = 130
    raise SystemExit(int(rc))


if __name__ == "__main__":
    main()
