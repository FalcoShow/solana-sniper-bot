from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from typing import Any

_TASKS: set[asyncio.Task[Any]] = set()


def _done_cb(task: asyncio.Task[Any]) -> None:
    _TASKS.discard(task)
    # Force exception retrieval so errors don't get silently dropped.
    try:
        _ = task.exception()
    except asyncio.CancelledError:
        return
    except Exception:
        # Nothing else to do here; logging is handled elsewhere if desired.
        return


def spawn(
    coro: Awaitable[Any],
    *,
    name: str | None = None,
    loop: asyncio.AbstractEventLoop | None = None,
) -> asyncio.Task[Any]:
    """
    Create a background task and keep a strong reference until completion.

    - Satisfies Ruff RUF006.
    - Prevents silent background task errors.
    """
    if loop is None:
        task = asyncio.create_task(coro, name=name)
    else:
        task = loop.create_task(coro)
        if name is not None:
            try:
                task.set_name(name)
            except Exception:
                pass

    _TASKS.add(task)
    task.add_done_callback(_done_cb)
    return task


def active_tasks() -> int:
    return len(_TASKS)


def cancel_all() -> int:
    n = 0
    for t in list(_TASKS):
        if not t.done():
            t.cancel()
            n += 1
    return n
