# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Callable

from utils_pkg.auto_exit_worker import AutoExitWorkerConfig, auto_exit_worker


@dataclass
class EngineStub:
    reclaim_calls: int = 0
    claim_calls: int = 0

    async def reclaim(self, redis: Any) -> None:
        _ = redis
        self.reclaim_calls += 1

    async def claim(self, redis: Any) -> None:
        _ = redis
        self.claim_calls += 1


class RedisStub:
    pass


def _run(coro):
    """
    Windows-friendly runner.

    Pytest on Windows can hit event-loop-policy edge cases depending on environment.
    We force Selector policy when available to keep behavior consistent.
    """
    if hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
        try:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]
        except Exception:
            pass
    return asyncio.run(coro)


async def _wait_until(
    predicate: Callable[[], bool], timeout_s: float = 1.0, step_s: float = 0.01
) -> bool:
    """Poll until predicate() is True or timeout."""
    end = asyncio.get_running_loop().time() + float(timeout_s)
    while asyncio.get_running_loop().time() < end:
        if predicate():
            return True
        await asyncio.sleep(float(step_s))
    return predicate()


def test_auto_exit_worker_healthy_no_shutdown():
    async def _t():
        stop = asyncio.Event()
        shutdown = asyncio.Event()
        engine = EngineStub()
        redis = RedisStub()

        calls = {"shutdown": 0}

        async def health_fn(**_kw):
            return {"ok": True, "detail": "healthy"}

        async def on_shutdown(_snap):
            calls["shutdown"] += 1

        cfg = AutoExitWorkerConfig(
            poll_s=0.01,
            health_every_s=0.01,
            health_timeout_s=0.2,
            grace_s=0.0,
            max_consecutive_unhealthy=2,
        )

        task = asyncio.create_task(
            auto_exit_worker(
                redis,
                engine,
                poll_s=cfg.poll_s,
                stop_event=stop,
                shutdown_event=shutdown,
                on_shutdown=on_shutdown,
                config=cfg,
                health_fn=health_fn,
            )
        )

        await asyncio.sleep(0.10)
        stop.set()
        await asyncio.wait_for(task, timeout=1.0)

        assert calls["shutdown"] == 0
        assert shutdown.is_set() is False
        assert engine.reclaim_calls > 0
        assert engine.claim_calls > 0

    _run(_t())


def test_auto_exit_worker_unhealthy_triggers_shutdown_once():
    async def _t():
        stop = asyncio.Event()
        shutdown = asyncio.Event()
        engine = EngineStub()
        redis = RedisStub()

        calls = {"shutdown": 0}

        async def health_fn(**_kw):
            return {"ok": False, "err": "forced_unhealthy"}

        async def on_shutdown(_snap):
            calls["shutdown"] += 1

        cfg = AutoExitWorkerConfig(
            poll_s=0.01,
            health_every_s=0.01,
            health_timeout_s=0.2,
            grace_s=0.0,
            max_consecutive_unhealthy=3,
        )

        task = asyncio.create_task(
            auto_exit_worker(
                redis,
                engine,
                poll_s=cfg.poll_s,
                stop_event=stop,
                shutdown_event=shutdown,
                on_shutdown=on_shutdown,
                config=cfg,
                health_fn=health_fn,
            )
        )

        # Wait until shutdown triggers
        ok = await _wait_until(lambda: shutdown.is_set(), timeout_s=1.5, step_s=0.02)
        assert ok is True
        assert calls["shutdown"] == 1

        # Let it run a bit longer: MUST NOT fire again
        await asyncio.sleep(0.10)
        assert calls["shutdown"] == 1

        stop.set()
        await asyncio.wait_for(task, timeout=1.0)

    _run(_t())


def test_auto_exit_worker_grace_period_prevents_early_shutdown():
    async def _t():
        stop = asyncio.Event()
        shutdown = asyncio.Event()
        engine = EngineStub()
        redis = RedisStub()

        async def health_fn(**_kw):
            return {"ok": False, "err": "forced_unhealthy"}

        cfg = AutoExitWorkerConfig(
            poll_s=0.01,
            health_every_s=0.01,
            health_timeout_s=0.2,
            grace_s=0.20,
            max_consecutive_unhealthy=2,
        )

        task = asyncio.create_task(
            auto_exit_worker(
                redis,
                engine,
                poll_s=cfg.poll_s,
                stop_event=stop,
                shutdown_event=shutdown,
                config=cfg,
                health_fn=health_fn,
            )
        )

        # During grace, should NOT trip
        await asyncio.sleep(0.12)
        assert shutdown.is_set() is False

        # After grace, it should trip
        ok = await _wait_until(lambda: shutdown.is_set(), timeout_s=1.5, step_s=0.02)
        assert ok is True

        stop.set()
        await asyncio.wait_for(task, timeout=1.0)

    _run(_t())


def test_auto_exit_worker_null_redis_exits_cleanly():
    async def _t():
        stop = asyncio.Event()
        shutdown = asyncio.Event()
        engine = EngineStub()

        async def health_fn(**_kw):
            return {"ok": True, "detail": "healthy"}

        cfg = AutoExitWorkerConfig(
            poll_s=0.01,
            noop_sleep_s=0.05,
            health_every_s=0.01,
            health_timeout_s=0.2,
            grace_s=0.0,
            max_consecutive_unhealthy=2,
        )

        task = asyncio.create_task(
            auto_exit_worker(
                None,
                engine,
                poll_s=cfg.poll_s,
                stop_event=stop,
                shutdown_event=shutdown,
                config=cfg,
                health_fn=health_fn,
            )
        )

        await asyncio.sleep(0.08)
        stop.set()
        await asyncio.wait_for(task, timeout=1.0)

        # no redis -> should not claim/reclaim
        assert engine.reclaim_calls == 0
        assert engine.claim_calls == 0
        assert shutdown.is_set() is False

    _run(_t())
