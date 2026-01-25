from __future__ import annotations

import asyncio
import inspect
import os


def test_strategy_class_exists_and_has_run_methods():
    os.environ.setdefault("MASTER_PHASE", "0")
    os.environ.setdefault("MAIN_DISABLE_RANDOM_JITTER", "1")

    from scripts.main import Strategy

    assert inspect.isclass(Strategy)
    s = Strategy()

    assert hasattr(s, "run")
    assert hasattr(s, "run_forever")


async def _run_strategy_quick_exit():
    # Ensure Strategy.run() can be awaited and exits quickly when stop_evt is already set
    os.environ["MASTER_PHASE"] = "0"
    os.environ["MAIN_DISABLE_RANDOM_JITTER"] = "1"
    os.environ.setdefault("MAIN_FAST_FAIL", "0")

    from scripts.main import Strategy

    stop_evt = asyncio.Event()
    stop_evt.set()

    s = Strategy()
    await asyncio.wait_for(s.run(stop_evt), timeout=5.0)


def test_strategy_run_quick_exit_event_loop():
    asyncio.run(_run_strategy_quick_exit())


def test_builder_aliases_exist():
    os.environ.setdefault("MASTER_PHASE", "0")
    os.environ.setdefault("MAIN_DISABLE_RANDOM_JITTER", "1")

    from scripts.main import build_runtime, build_services, build_system

    assert callable(build_runtime)
    assert callable(build_system)
    assert callable(build_services)
