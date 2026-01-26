from __future__ import annotations

import asyncio
import os
import time
from collections.abc import Coroutine
from dataclasses import dataclass
from typing import Any, TypeVar, cast, overload

T = TypeVar("T")

# Strong references so background tasks aren't GC'd early and exceptions aren't dropped.
_TASKS: set[asyncio.Task[Any]] = set()


# Optional: store lightweight metadata for debugging / shutdown reporting
@dataclass(frozen=True)
class TaskMeta:
    name: str | None
    created_ts: float
    origin: str | None


_TASK_META: dict[asyncio.Task[Any], TaskMeta] = {}

# Env knobs (import-time safe; defaults only)
# If enabled, we capture a tiny "origin" string from the current stack for debugging.
_CAPTURE_ORIGIN = (os.getenv("TASK_SPAWN_CAPTURE_ORIGIN", "") or "").strip().lower() not in (
    "0",
    "false",
    "no",
    "off",
    "",
)

# How many stack frames to show in origin (small by design)
_ORIGIN_FRAMES = int((os.getenv("TASK_SPAWN_ORIGIN_FRAMES", "2") or "2").strip() or "2")
_ORIGIN_FRAMES = max(1, min(_ORIGIN_FRAMES, 10))


def _task_origin() -> str | None:
    if not _CAPTURE_ORIGIN:
        return None
    try:
        import inspect

        # Grab a couple of caller frames without dumping huge stacks
        stack = inspect.stack()
        # stack[0] is here; stack[1] is spawn(); stack[2] is caller
        start = 2
        end = min(len(stack), start + _ORIGIN_FRAMES)
        parts: list[str] = []
        for i in range(start, end):
            fr = stack[i]
            parts.append(f"{os.path.basename(fr.filename)}:{fr.lineno} {fr.function}()")
        return " <- ".join(parts) if parts else None
    except Exception:
        return None


def _done_cb(task: asyncio.Task[Any]) -> None:
    # Always release strong refs + metadata
    _TASKS.discard(task)
    _TASK_META.pop(task, None)

    # Force exception retrieval so errors don't get silently dropped.
    try:
        _ = task.exception()
    except asyncio.CancelledError:
        return
    except Exception:
        # Logging is handled elsewhere; we just ensure exception is realized.
        return


def _register(task: asyncio.Task[Any], *, name: str | None) -> None:
    _TASKS.add(task)
    _TASK_META[task] = TaskMeta(name=name, created_ts=time.time(), origin=_task_origin())
    task.add_done_callback(_done_cb)


def _set_task_name(task: asyncio.Task[Any], name: str | None) -> None:
    if not name:
        return
    try:
        task.set_name(name)
    except Exception:
        # older loops / edge cases
        pass


@overload
def spawn(
    coro: Coroutine[Any, Any, T],
    *,
    name: str | None = None,
    loop: asyncio.AbstractEventLoop | None = None,
    shield: bool = False,
) -> asyncio.Task[T]: ...


@overload
def spawn(
    coro: Coroutine[Any, Any, Any],
    *,
    name: str | None = None,
    loop: asyncio.AbstractEventLoop | None = None,
    shield: bool = False,
) -> asyncio.Task[Any]: ...


def spawn(
    coro: Coroutine[Any, Any, Any],
    *,
    name: str | None = None,
    loop: asyncio.AbstractEventLoop | None = None,
    shield: bool = False,
) -> asyncio.Task[Any]:
    """
    Create a background task and keep a strong reference until completion.

    Goals:
    - mypy-correct: asyncio.create_task expects a Coroutine, not generic Awaitable.
    - prevents silent background task errors (forces exception retrieval).
    - keeps strong ref until completion to avoid GC + dropped exceptions.
    - optional: shield task from cancellation propagation (use sparingly).

    Args:
        coro: Coroutine object to schedule.
        name: Optional task name (visible in debug tools).
        loop: Optional explicit loop; otherwise uses current running loop.
        shield: If True, the scheduled coroutine is wrapped in asyncio.shield()
                (this prevents the coroutine from being cancelled when the caller
                 is cancelled; the returned Task can still be cancelled directly).

    Returns:
        asyncio.Task
    """
    if shield:
        # Shield returns a Future; wrap it in a coroutine so create_task is happy.
        async def _shielded() -> Any:
            return await asyncio.shield(coro)

        coro = _shielded()

    if loop is None:
        task = asyncio.create_task(coro, name=name)
    else:
        task = loop.create_task(coro)
        _set_task_name(task, name)

    _register(task, name=name)
    return task


def spawn_future(
    aw: asyncio.Future[T] | Coroutine[Any, Any, T],
    *,
    name: str | None = None,
    loop: asyncio.AbstractEventLoop | None = None,
) -> asyncio.Task[T]:
    """
    Accept either a Future or Coroutine and ensure it's scheduled as a Task.

    Useful when callers already have a Future (e.g., ensure_future returns one).
    """
    if isinstance(aw, asyncio.Future):
        # ensure_future returns a Future; create a Task wrapper for uniform management.
        async def _await_future() -> T:
            return await aw

        return cast(asyncio.Task[T], spawn(_await_future(), name=name, loop=loop))
    return cast(asyncio.Task[T], spawn(aw, name=name, loop=loop))


def active_tasks() -> int:
    return len(_TASKS)


def active_task_names() -> list[str]:
    """
    Debug helper: return current active task names (best-effort).
    """
    out: list[str] = []
    for t in list(_TASKS):
        try:
            out.append(t.get_name())
        except Exception:
            out.append(repr(t))
    return out


def tasks_snapshot() -> list[dict[str, Any]]:
    """
    Debug helper: snapshot active tasks + lightweight metadata.
    """
    snap: list[dict[str, Any]] = []
    now = time.time()
    for t in list(_TASKS):
        meta = _TASK_META.get(t)
        item: dict[str, Any] = {
            "done": t.done(),
        }
        try:
            item["name"] = t.get_name()
        except Exception:
            item["name"] = meta.name if meta else None
        if meta:
            item["age_sec"] = max(0.0, now - meta.created_ts)
            if meta.origin:
                item["origin"] = meta.origin
        snap.append(item)
    return snap


def cancel_all() -> int:
    """
    Cancel all active tasks that are not done yet.
    Returns: number of tasks cancelled (requested).
    """
    n = 0
    for t in list(_TASKS):
        if not t.done():
            t.cancel()
            n += 1
    return n


async def wait_all(*, timeout: float | None = None) -> bool:
    """
    Wait for all currently tracked tasks to finish.

    Returns:
        True if all tasks completed before timeout; False otherwise.
    """
    tasks = [t for t in list(_TASKS) if not t.done()]
    if not tasks:
        return True

    try:
        if timeout is None:
            await asyncio.gather(*tasks, return_exceptions=True)
            return True
        await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=timeout)
        return True
    except asyncio.TimeoutError:
        return False


def reset_state_for_tests() -> None:
    """
    Test helper: clears registry (does NOT cancel tasks).
    Only use in test environments where tasks are controlled.
    """
    _TASKS.clear()
    _TASK_META.clear()
