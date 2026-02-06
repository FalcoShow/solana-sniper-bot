from __future__ import annotations

"""
execution/exits_wiring.py — EXIT PIPELINE WIRING (P0)
=====================================================

LOCK-IN (per your plan)
-----------------------
This module is *wiring*, not “a second exit engine”.

✅ Responsibilities:
  1) Receive BUY fills (register_entry / on_buy_fill).
  2) Persist/refresh position state in the *single* position store (best-effort).
  3) Enqueue an exit job into Redis `exit_queue` (canonical handoff to exit worker).
  4) Provide lightweight status/metrics introspection.
  5) Optional in-proc supervisor (dev/local only) that still calls the canonical exit engine.

❌ Non-responsibilities:
  - No alternative exit logic.
  - No duplicate sell execution in queue mode.
  - No forked worker brain.

Modes
-----
EXIT_WIRING_MODE:
  - "queue"  (default): enqueue jobs only (recommended for Render).
  - "inproc" : optional local dev supervisor loop (still uses execution.exits.apply_exit).
  - "off"    : disable wiring (no enqueue; useful for tests).

Queue contract (canonical)
--------------------------
We use Redis primitives:
  - ZSET  exit_queue                member=job_id, score=next_run_ts
  - KEY   exit:job:{job_id}         JSON payload (string), TTL optional
  - KEY   exit:enq:{job_id}         idempotency guard (SET NX), TTL

Job id:
  - default = buy fill signature (string)

Payload:
  - mint, symbol, entry_px, size_usd, entry_ts, qty_raw (optional), buy_sig, tags, exits_cfg, ...

Design goals
------------
- Deterministic: handoff is persisted (no fragile in-memory only state).
- Idempotent: repeated fills / retries don’t spam the exit queue.
- Observable: queue size + job payloads are inspectable.
- Drift-tolerant: adapts to signature changes in exit engine / position store / redis client.

Public API
----------
- register_entry(...)  : call this after a BUY fill (sync; best-effort background work)
- register_entry_async(...): async variant that actually awaits enqueue/persist/log
- on_buy_fill(...)     : alias for compatibility
- start_exit_supervisor() / stop_exit_supervisor() : only when EXIT_WIRING_MODE=inproc
- positions_count(), get_exit_status()
"""

import asyncio
import contextlib
import inspect
import json
import os
import time
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass
from typing import Any, Protocol

from logging_setup import get_logger

log = get_logger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Optional structured trade event logger (best-effort, lazy)
# ──────────────────────────────────────────────────────────────────────────────
_trade_logger_cached: Any = None


async def _log_trade_event_best_effort(**kwargs: Any) -> None:
    """
    Best-effort wrapper around db.logger.log_trade_event (if present).
    Must never raise.
    """
    global _trade_logger_cached
    try:
        if _trade_logger_cached is None:
            try:
                from db.logger import log_trade_event as _lte  # type: ignore

                _trade_logger_cached = _lte
            except Exception:
                _trade_logger_cached = False

        if not _trade_logger_cached:
            return

        fn = _trade_logger_cached
        res = fn(**kwargs)
        if asyncio.iscoroutine(res):
            await res
    except Exception:
        return


# ──────────────────────────────────────────────────────────────────────────────
# Types
# ──────────────────────────────────────────────────────────────────────────────
class _SellFn(Protocol):
    def __call__(
        self, mint: str, usd_to_sell: float, reason: str, last_px_sol_per_token: float
    ) -> Awaitable[tuple[bool, str | None, dict[str, Any]]]: ...


class _PriceFn(Protocol):
    def __call__(self, mint: str) -> Awaitable[float | None]: ...


SellFn = Callable[[str, float, str, float], Awaitable[tuple[bool, str | None, dict[str, Any]]]]
PriceFn = Callable[[str], Awaitable[float | None]]


# ──────────────────────────────────────────────────────────────────────────────
# Config (env)
# ──────────────────────────────────────────────────────────────────────────────
def _env_str(name: str, default: str) -> str:
    v = (os.getenv(name) or "").strip()
    return v if v else default


def _env_float(name: str, default: float) -> float:
    try:
        v = (os.getenv(name) or "").strip()
        return float(v) if v else float(default)
    except Exception:
        return float(default)


def _env_int(name: str, default: int) -> int:
    try:
        v = (os.getenv(name) or "").strip()
        return int(float(v)) if v else int(default)
    except Exception:
        return int(default)


EXIT_WIRING_MODE = _env_str("EXIT_WIRING_MODE", "queue").lower()  # queue | inproc | off

# Queue keys
EXIT_QUEUE_KEY = _env_str("EXIT_QUEUE_KEY", "exit_queue")
EXIT_JOB_KEY_PREFIX = _env_str("EXIT_JOB_KEY_PREFIX", "exit:job:")
EXIT_ENQ_KEY_PREFIX = _env_str("EXIT_ENQ_KEY_PREFIX", "exit:enq:")

# Idempotency guard TTL (seconds) for enqueue keys + job payload keys
EXIT_ENQ_TTL_S = _env_int("EXIT_ENQ_TTL_S", 24 * 3600)  # 24h
EXIT_JOB_TTL_S = _env_int("EXIT_JOB_TTL_S", 72 * 3600)  # 72h

# Initial schedule: enqueue for immediate run (score=now)
EXIT_INITIAL_DELAY_S = _env_float("EXIT_INITIAL_DELAY_S", 0.0)

# In-proc supervisor loop knobs
TICK_SEC = _env_float("EXIT_TICK_SEC", 2.0)
CONCURRENCY = max(1, _env_int("EXIT_CONCURRENCY", 6))
LOG_EVERY_TICKS = max(1, _env_int("EXIT_LOG_EVERY", 15))

# Optional helper if someone passes SOL notional instead of USD
USD_PER_SOL_FALLBACK = _env_float("USD_PER_SOL", 150.0)

# If true, inproc mode also persists queue payloads (handy for debugging)
INPROC_MIRROR_QUEUE = _env_int("EXIT_INPROC_MIRROR_QUEUE", 1) == 1


# ──────────────────────────────────────────────────────────────────────────────
# Small helpers
# ──────────────────────────────────────────────────────────────────────────────
def _now() -> float:
    return time.time()


def _json_dumps(obj: Any) -> str:
    # default=str prevents serialization bombs (tags sometimes carry Decimals/objects)
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str)


def _job_key(job_id: str) -> str:
    return f"{EXIT_JOB_KEY_PREFIX}{job_id}"


def _enq_key(job_id: str) -> str:
    return f"{EXIT_ENQ_KEY_PREFIX}{job_id}"


def _in_running_loop() -> bool:
    try:
        asyncio.get_running_loop()
        return True
    except RuntimeError:
        return False


def _spawn_best_effort(coro: Coroutine[Any, Any, Any]) -> bool:
    """
    Fire-and-forget without ever raising if there is no running loop.
    Returns True if scheduled, False otherwise.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return False
    try:
        # Important: keep a typed local so mypy doesn't infer Never.
        task: asyncio.Task[Any] = loop.create_task(coro)
        _ = task
        return True
    except Exception:
        return False


async def _await_maybe(x: Any) -> Any:
    if asyncio.iscoroutine(x):
        return await x
    return x


def _safe_log_event(name: str, **fields: Any) -> None:
    """
    Use log.event if available; otherwise log.info. Must never raise.
    """
    try:
        evt = getattr(log, "event", None)
        if callable(evt):
            evt(name, **fields)
        else:
            log.info("%s %s", name, fields)
    except Exception:
        return


# ──────────────────────────────────────────────────────────────────────────────
# Lazy access to canonical exit engine (avoids import-time fragility)
# ──────────────────────────────────────────────────────────────────────────────
_exits_cached: Any = None


def _exits_mod() -> Any:
    global _exits_cached
    if _exits_cached is not None:
        return _exits_cached
    # Local import: keeps module import light and reduces circular risk
    from execution import exits as _exits  # type: ignore

    _exits_cached = _exits
    return _exits_cached


def _exits_config_summary_best_effort() -> Any:
    try:
        em = _exits_mod()
        fn = getattr(em, "exits_config_summary", None)
        if callable(fn):
            return fn()
    except Exception:
        pass
    return {}


async def _apply_exit_drift_tolerant(pos: Any, px: float, sell_fn: SellFn) -> Any:
    """
    Call execution.exits.apply_exit while tolerating signature drift.
    Expected modern shape: await apply_exit(pos, px, sell_fn)
    """
    em = _exits_mod()
    fn = getattr(em, "apply_exit", None)
    if not callable(fn):
        raise RuntimeError("execution.exits.apply_exit missing")

    # Candidate arg names seen across rewrites
    candidates: dict[str, Any] = {
        "pos": pos,
        "position": pos,
        "state": pos,
        "p": pos,
        "price": float(px),
        "px": float(px),
        "last_px": float(px),
        "sell_fn": sell_fn,
        "sell": sell_fn,
        "sell_token_usd": sell_fn,
    }

    sig = None
    with contextlib.suppress(Exception):
        sig = inspect.signature(fn)

    if sig is None:
        return await _await_maybe(fn(pos, float(px), sell_fn))

    call_kwargs: dict[str, Any] = {}
    for k, v in candidates.items():
        if k in sig.parameters:
            call_kwargs[k] = v

    # Fallback positional if signature doesn’t include expected names
    if not call_kwargs:
        return await _await_maybe(fn(pos, float(px), sell_fn))

    return await _await_maybe(fn(**call_kwargs))


def _make_position_state_drift_tolerant(
    *, mint: str, entry_px: float, size_usd: float, symbol: str | None, entry_ts: float
) -> Any:
    """
    Build PositionState even if its __init__ signature drifts.
    """
    em = _exits_mod()
    PS = getattr(em, "PositionState", None)
    if PS is None:
        raise RuntimeError("execution.exits.PositionState missing")

    # Try a rich constructor first
    try:
        return PS(  # type: ignore
            mint=mint,
            entry_px=float(entry_px),
            size_usd=float(size_usd),
            symbol=symbol,
            entry_ts=float(entry_ts),
        )
    except Exception:
        pass

    # Minimal constructor
    pos = PS(mint=mint, entry_px=float(entry_px), size_usd=float(size_usd))  # type: ignore

    # Best-effort set optional attrs
    for k, v in (
        ("symbol", symbol),
        ("entry_ts", float(entry_ts)),
        ("peak_px", float(entry_px)),
        ("tp1_done", False),
        ("tp2_done", False),
        ("last_action_ts", float(entry_ts)),
    ):
        with contextlib.suppress(Exception):
            if hasattr(pos, k):
                setattr(pos, k, v)

    return pos


# ──────────────────────────────────────────────────────────────────────────────
# Redis access (best-effort, supports sync OR async clients)
# ──────────────────────────────────────────────────────────────────────────────
_redis_client: Any = None
_redis_lock = asyncio.Lock()


async def _get_redis_best_effort() -> Any:
    """
    Try common shapes in your stack:
      - utils_pkg.core_utils.get_redis_async()
      - utils_pkg.core_utils.get_redis() (may return sync or async client)
      - utils_pkg.core_utils.redis_async / redis

    Returned object may be sync or async; callers must use _await_maybe around ops.
    """
    global _redis_client

    client = _redis_client
    if client is not None:
        return client

    async with _redis_lock:
        client = _redis_client
        if client is not None:
            return client

        # Prefer async getter
        try:
            from utils_pkg.core_utils import get_redis_async  # type: ignore

            client = await _await_maybe(get_redis_async())
            _redis_client = client
            return client
        except Exception:
            pass

        # Fallback getter
        try:
            from utils_pkg.core_utils import get_redis  # type: ignore

            client = get_redis()
            _redis_client = client
            return client
        except Exception:
            pass

        # Direct handles
        try:
            from utils_pkg.core_utils import redis_async as r  # type: ignore

            client = r
            _redis_client = client
            return client
        except Exception:
            pass

        try:
            from utils_pkg.core_utils import redis as r  # type: ignore

            client = r
            _redis_client = client
            return client
        except Exception:
            pass

        _redis_client = None
        return None


async def _enqueue_job(job_id: str, payload: dict[str, Any], run_at_ts: float) -> bool:
    """
    Idempotent enqueue:
      1) SET exit:enq:{id} NX EX ttl   (guard)
      2) SET exit:job:{id} payload EX ttl (payload)
      3) ZADD exit_queue {id: score}  (schedule)
    Returns True if newly enqueued, False if it was already enqueued or redis missing.
    """
    r = await _get_redis_best_effort()
    if not r:
        return False

    enqk = _enq_key(job_id)
    jobk = _job_key(job_id)

    try:
        pipe = getattr(r, "pipeline", None)
        if callable(pipe):
            p = r.pipeline(transaction=True)
            p.set(enqk, "1", nx=True, ex=int(EXIT_ENQ_TTL_S))
            p.set(jobk, _json_dumps(payload), ex=int(EXIT_JOB_TTL_S))
            p.zadd(EXIT_QUEUE_KEY, {job_id: float(run_at_ts)})

            res = await _await_maybe(p.execute())
            inserted = bool(res[0]) if isinstance(res, (list, tuple)) and res else bool(res)
            return inserted

        inserted = await _await_maybe(r.set(enqk, "1", nx=True, ex=int(EXIT_ENQ_TTL_S)))
        if not inserted:
            return False

        await _await_maybe(r.set(jobk, _json_dumps(payload), ex=int(EXIT_JOB_TTL_S)))
        await _await_maybe(r.zadd(EXIT_QUEUE_KEY, {job_id: float(run_at_ts)}))
        return True
    except Exception as e:
        log.warning("EXIT_ENQUEUE_FAIL job=%s err=%s", job_id, e)
        return False


async def _queue_size() -> int | None:
    r = await _get_redis_best_effort()
    if not r:
        return None
    try:
        v = await _await_maybe(r.zcard(EXIT_QUEUE_KEY))
        return int(v)
    except Exception:
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Position store (best-effort hook)
# ──────────────────────────────────────────────────────────────────────────────
async def _persist_position_best_effort(payload: dict[str, Any]) -> None:
    """
    Best-effort store of initial position snapshot, so exits/pnl have a source of truth.
    We try:
      - root positions.py (redis-first/file fallback) : save_position(...)
      - utils_pkg.positions (PG wrapper)              : record_buy(...)
    This module MUST NOT become the source of truth; it only calls canonical stores.
    """
    # root positions store
    with contextlib.suppress(Exception):
        import positions as root_positions  # type: ignore

        fn = getattr(root_positions, "save_position", None)
        if callable(fn):
            mint = str(payload.get("mint") or "")
            if mint:
                await _await_maybe(fn(mint, payload))
                return

    # utils_pkg positions wrapper
    with contextlib.suppress(Exception):
        from utils_pkg import positions as upos  # type: ignore

        fn = getattr(upos, "record_buy", None)
        if callable(fn):
            await _await_maybe(
                fn(
                    mint=str(payload.get("mint") or ""),
                    qty_raw=int(payload.get("qty_raw") or 0),
                    price=float(payload.get("entry_px") or 0.0),
                    sig=str(payload.get("buy_sig") or ""),
                    symbol=payload.get("symbol"),
                    usd_spent=float(payload.get("size_usd") or 0.0),
                    ts=float(payload.get("entry_ts") or _now()),
                )
            )
            return


# ──────────────────────────────────────────────────────────────────────────────
# In-proc supervisor (optional, local/dev)
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class _Managed:
    job_id: str
    pos: Any
    sell_fn: SellFn
    price_fn: PriceFn


_REG: dict[str, _Managed] = {}
_SUP_TASK: asyncio.Task | None = None
_TICK: int = 0


async def stop_exit_supervisor() -> None:
    """
    Stop in-proc supervisor if running. Safe to call even if not running.
    """
    global _SUP_TASK
    if _SUP_TASK and not _SUP_TASK.done():
        _SUP_TASK.cancel()
        with contextlib.suppress(Exception):
            await _SUP_TASK
    _SUP_TASK = None


def start_exit_supervisor() -> asyncio.Task:
    """
    Start the in-proc exit supervisor loop.
    IMPORTANT: Only used when EXIT_WIRING_MODE=inproc.
    Must be called from within an active event loop.
    """
    global _SUP_TASK

    if EXIT_WIRING_MODE != "inproc":
        raise RuntimeError(
            f"start_exit_supervisor called but EXIT_WIRING_MODE={EXIT_WIRING_MODE!r}"
        )

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError as e:
        raise RuntimeError(
            "start_exit_supervisor must be called from an async context (running loop)."
        ) from e

    if _SUP_TASK and not _SUP_TASK.done():
        return _SUP_TASK

    _SUP_TASK = loop.create_task(_supervisor_loop(), name="exit_supervisor")
    return _SUP_TASK


async def _supervisor_loop() -> None:
    """
    In-proc loop: periodically price positions and invoke canonical apply_exit.
    """
    global _TICK
    sem = asyncio.Semaphore(CONCURRENCY)
    hb_next = LOG_EVERY_TICKS

    # one-time config event
    with contextlib.suppress(Exception):
        await _log_trade_event_best_effort(
            type="exit_supervisor_started",
            mint=None,
            symbol=None,
            entry_sol_per_token=None,
            sol_spent=None,
            usd_spent=None,
            signature=None,
            cu_price=None,
            jito_tip=None,
            data={
                "tick_sec": float(TICK_SEC),
                "concurrency": int(CONCURRENCY),
                "cfg": _exits_config_summary_best_effort(),
            },
        )

    while True:
        _TICK += 1
        items = list(_REG.items())

        async def _handle(job_id: str, m: _Managed) -> None:
            async with sem:
                try:
                    px = await m.price_fn(str(getattr(m.pos, "mint", "")))
                except Exception:
                    return
                if px is None:
                    return

                try:
                    await _apply_exit_drift_tolerant(m.pos, float(px), m.sell_fn)
                except Exception:
                    return

                # If position closed, drop from registry (best-effort heuristics)
                with contextlib.suppress(Exception):
                    if float(getattr(m.pos, "size_usd", 0.0) or 0.0) <= 0.0:
                        _REG.pop(job_id, None)

        tasks = [asyncio.create_task(_handle(j, m)) for j, m in items]
        if tasks:
            with contextlib.suppress(Exception):
                await asyncio.gather(*tasks, return_exceptions=True)

        hb_next -= 1
        if hb_next <= 0:
            hb_next = LOG_EVERY_TICKS
            with contextlib.suppress(Exception):
                await _log_trade_event_best_effort(
                    type="exit_supervisor_heartbeat",
                    mint=None,
                    symbol=None,
                    entry_sol_per_token=None,
                    sol_spent=None,
                    usd_spent=None,
                    signature=None,
                    cu_price=None,
                    jito_tip=None,
                    data={"tick": int(_TICK), "managed": int(len(_REG))},
                )

        await asyncio.sleep(float(TICK_SEC))


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────
def register_entry(
    signature: str,
    mint: str,
    qty: int | None,
    entry_price: float,
    *,
    symbol: str | None = None,
    usd_spent: float | None = None,
    size_usd: float | None = None,
    sol_spent: float | None = None,
    sell_fn: SellFn | None = None,
    price_fn: PriceFn | None = None,
    entry_ts: float | None = None,
    tags: dict[str, Any] | None = None,
) -> None:
    """
    Register a newly-filled BUY so exits can manage it.

    Queue mode:
      - Persist position snapshot (best-effort; fire-and-forget if in loop)
      - Enqueue job into Redis exit_queue (fire-and-forget if in loop)
      - sell_fn/price_fn are ignored (handled by exit worker)

    Inproc mode:
      - Store in-memory registry and supervisor manages it (requires sell_fn + price_fn)
      - Optionally mirrors into queue (INPROC_MIRROR_QUEUE=1)

    Off mode:
      - No-op

    Raises:
      - ValueError only when notional cannot be resolved (same as your current behavior).
    """
    if EXIT_WIRING_MODE == "off":
        return

    # Resolve notional USD
    notional_usd: float | None = None
    for cand in (usd_spent, size_usd):
        if cand is not None:
            notional_usd = float(cand)
            break
    if notional_usd is None and sol_spent is not None:
        notional_usd = float(sol_spent) * float(USD_PER_SOL_FALLBACK)

    if notional_usd is None:
        raise ValueError("register_entry: usd_spent/size_usd/sol_spent missing")

    ts = float(entry_ts or _now())

    payload: dict[str, Any] = {
        "job_id": str(signature),
        "buy_sig": str(signature),
        "mint": str(mint),
        "symbol": symbol,
        "qty_raw": int(qty or 0),
        "entry_px": float(entry_price),
        "size_usd": float(notional_usd),
        "entry_ts": ts,
        "tags": dict(tags or {}),
        "exits_cfg": _exits_config_summary_best_effort(),
        "created_at": _now(),
    }

    # Best-effort persist snapshot
    if _in_running_loop():
        _spawn_best_effort(_persist_position_best_effort(payload))

    async def _do_enqueue_and_log() -> None:
        run_at = _now() + float(EXIT_INITIAL_DELAY_S)
        inserted = await _enqueue_job(str(signature), payload, run_at_ts=run_at)
        if inserted:
            _safe_log_event(
                "EXIT_ENQUEUED",
                job_id=str(signature),
                mint=str(mint),
                size_usd=float(notional_usd),
                run_at=float(run_at),
            )
        else:
            _safe_log_event("EXIT_ENQUEUE_DUP", job_id=str(signature), mint=str(mint))

    async def _do_trade_event() -> None:
        await _log_trade_event_best_effort(
            type="entry_registered",
            mint=str(mint),
            symbol=symbol,
            entry_sol_per_token=float(entry_price),
            sol_spent=float(sol_spent) if sol_spent is not None else None,
            usd_spent=float(notional_usd),
            signature=str(signature),
            cu_price=None,
            jito_tip=None,
            data={
                "mode": EXIT_WIRING_MODE,
                "queue": EXIT_QUEUE_KEY,
                "cfg": payload.get("exits_cfg"),
            },
        )

    # Queue mode: enqueue in background if possible
    if EXIT_WIRING_MODE == "queue":
        if _in_running_loop():
            _spawn_best_effort(_do_enqueue_and_log())
            _spawn_best_effort(_do_trade_event())
        return

    # Inproc mode: must have functions
    if EXIT_WIRING_MODE == "inproc":
        if sell_fn is None or price_fn is None:
            raise ValueError("register_entry: inproc mode requires sell_fn and price_fn")

        pos = _make_position_state_drift_tolerant(
            mint=str(mint),
            entry_px=float(entry_price),
            size_usd=float(notional_usd),
            symbol=symbol,
            entry_ts=ts,
        )
        _REG[str(signature)] = _Managed(
            job_id=str(signature), pos=pos, sell_fn=sell_fn, price_fn=price_fn
        )

        if INPROC_MIRROR_QUEUE and _in_running_loop():
            _spawn_best_effort(_do_enqueue_and_log())

        if _in_running_loop():
            _spawn_best_effort(_do_trade_event())


async def register_entry_async(
    signature: str,
    mint: str,
    qty: int | None,
    entry_price: float,
    *,
    symbol: str | None = None,
    usd_spent: float | None = None,
    size_usd: float | None = None,
    sol_spent: float | None = None,
    sell_fn: SellFn | None = None,
    price_fn: PriceFn | None = None,
    entry_ts: float | None = None,
    tags: dict[str, Any] | None = None,
) -> None:
    """
    Async variant: actually awaits persist/enqueue/log when in a loop.
    Keeps the same semantics as register_entry().
    """
    if EXIT_WIRING_MODE == "off":
        return

    # Resolve notional USD
    notional_usd: float | None = None
    for cand in (usd_spent, size_usd):
        if cand is not None:
            notional_usd = float(cand)
            break
    if notional_usd is None and sol_spent is not None:
        notional_usd = float(sol_spent) * float(USD_PER_SOL_FALLBACK)

    if notional_usd is None:
        raise ValueError("register_entry_async: usd_spent/size_usd/sol_spent missing")

    ts = float(entry_ts or _now())

    payload: dict[str, Any] = {
        "job_id": str(signature),
        "buy_sig": str(signature),
        "mint": str(mint),
        "symbol": symbol,
        "qty_raw": int(qty or 0),
        "entry_px": float(entry_price),
        "size_usd": float(notional_usd),
        "entry_ts": ts,
        "tags": dict(tags or {}),
        "exits_cfg": _exits_config_summary_best_effort(),
        "created_at": _now(),
    }

    await _persist_position_best_effort(payload)

    run_at = _now() + float(EXIT_INITIAL_DELAY_S)

    if EXIT_WIRING_MODE == "queue":
        inserted = await _enqueue_job(str(signature), payload, run_at_ts=run_at)
        if inserted:
            _safe_log_event(
                "EXIT_ENQUEUED",
                job_id=str(signature),
                mint=str(mint),
                size_usd=float(notional_usd),
                run_at=float(run_at),
            )
        else:
            _safe_log_event("EXIT_ENQUEUE_DUP", job_id=str(signature), mint=str(mint))

        await _log_trade_event_best_effort(
            type="entry_registered",
            mint=str(mint),
            symbol=symbol,
            entry_sol_per_token=float(entry_price),
            sol_spent=float(sol_spent) if sol_spent is not None else None,
            usd_spent=float(notional_usd),
            signature=str(signature),
            cu_price=None,
            jito_tip=None,
            data={
                "mode": EXIT_WIRING_MODE,
                "queue": EXIT_QUEUE_KEY,
                "cfg": payload.get("exits_cfg"),
            },
        )
        return

    if EXIT_WIRING_MODE == "inproc":
        if sell_fn is None or price_fn is None:
            raise ValueError("register_entry_async: inproc mode requires sell_fn and price_fn")

        pos = _make_position_state_drift_tolerant(
            mint=str(mint),
            entry_px=float(entry_price),
            size_usd=float(notional_usd),
            symbol=symbol,
            entry_ts=ts,
        )
        _REG[str(signature)] = _Managed(
            job_id=str(signature), pos=pos, sell_fn=sell_fn, price_fn=price_fn
        )

        if INPROC_MIRROR_QUEUE:
            await _enqueue_job(str(signature), payload, run_at_ts=run_at)

        await _log_trade_event_best_effort(
            type="entry_registered",
            mint=str(mint),
            symbol=symbol,
            entry_sol_per_token=float(entry_price),
            sol_spent=float(sol_spent) if sol_spent is not None else None,
            usd_spent=float(notional_usd),
            signature=str(signature),
            cu_price=None,
            jito_tip=None,
            data={
                "mode": EXIT_WIRING_MODE,
                "queue": EXIT_QUEUE_KEY,
                "cfg": payload.get("exits_cfg"),
            },
        )


def on_buy_fill(*args: Any, **kwargs: Any) -> None:
    """
    Compatibility alias for older call-sites.
    Forwards to register_entry.
    """
    return register_entry(*args, **kwargs)


def positions_count() -> int:
    """
    Count managed positions (inproc) OR return -1 in queue mode (non-blocking).
    """
    if EXIT_WIRING_MODE == "inproc":
        return len(_REG)
    return -1


async def get_exit_status() -> dict[str, Any]:
    """
    Lightweight snapshot for debugging/metrics.

    Includes:
      - wiring mode
      - inproc managed (if applicable)
      - redis queue size (if available)
      - supervisor running (if applicable)
    """
    out: dict[str, Any] = {
        "mode": EXIT_WIRING_MODE,
        "tick_sec": float(TICK_SEC),
        "concurrency": int(CONCURRENCY),
        "queue_key": EXIT_QUEUE_KEY,
        "managed_inproc": len(_REG) if EXIT_WIRING_MODE == "inproc" else 0,
        "supervisor_running": bool(_SUP_TASK and not _SUP_TASK.done())
        if EXIT_WIRING_MODE == "inproc"
        else False,
        "cfg": _exits_config_summary_best_effort(),
    }

    qs = await _queue_size()
    if qs is not None:
        out["queue_size"] = int(qs)

    if EXIT_WIRING_MODE == "inproc":
        snap: dict[str, Any] = {}
        for job_id, m in _REG.items():
            p = m.pos
            snap[job_id] = {
                "mint": getattr(p, "mint", None),
                "symbol": getattr(p, "symbol", None),
                "entry_px": getattr(p, "entry_px", None),
                "peak_px": getattr(p, "peak_px", None),
                "size_usd": getattr(p, "size_usd", None),
                "tp1_done": getattr(p, "tp1_done", None),
                "tp2_done": getattr(p, "tp2_done", None),
                "entry_ts": getattr(p, "entry_ts", None),
                "last_action_ts": getattr(p, "last_action_ts", None),
            }
        out["managed_detail"] = snap

    return out


__all__ = [
    "get_exit_status",
    "on_buy_fill",
    "positions_count",
    "register_entry",
    "register_entry_async",
    "start_exit_supervisor",
    "stop_exit_supervisor",
]
