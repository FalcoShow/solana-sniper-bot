# execution/exits_wiring.py
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

❌ Non-responsibilities:
  - No “alternative” exit logic.
  - No duplicate sell execution.
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
  - mint, symbol, entry_px, size_usd, entry_ts, qty_raw (optional), buy_sig, tags, etc.

Why this design?
----------------
- Deterministic: the “handoff” is persisted (no fragile in-memory only state).
- Idempotent: repeated fills / retries won’t spam the exit queue.
- Observable: queue size + job payloads are inspectable.

Public API
----------
- register_entry(...)  : call this after a BUY fill
- start_exit_supervisor() / stop_exit_supervisor() : only used when EXIT_WIRING_MODE=inproc
- positions_count(), get_exit_status()

Compatibility
-------------
Existing call-sites that pass (sell_fn, price_fn) will still work.
In queue mode they’re ignored (worker handles pricing + selling).
"""

import asyncio
import json
import os
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from logging_setup import get_logger

log = get_logger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Optional structured event logger (best-effort)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from db.logger import log_trade_event  # type: ignore
except Exception:  # pragma: no cover

    async def log_trade_event(*_a: Any, **_k: Any) -> None:
        return


# ──────────────────────────────────────────────────────────────────────────────
# Canonical exit engine (logic only)
# ──────────────────────────────────────────────────────────────────────────────
import contextlib

from execution.exits import PositionState, apply_exit, exits_config_summary  # type: ignore

# ──────────────────────────────────────────────────────────────────────────────
# Types
# ──────────────────────────────────────────────────────────────────────────────
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
CONCURRENCY = _env_int("EXIT_CONCURRENCY", 6)
LOG_EVERY_TICKS = _env_int("EXIT_LOG_EVERY", 15)

# Optional helper if someone passes SOL notional instead of USD
USD_PER_SOL_FALLBACK = _env_float("USD_PER_SOL", 150.0)

# If true, inproc mode also persists queue payloads (handy for debugging)
INPROC_MIRROR_QUEUE = _env_int("EXIT_INPROC_MIRROR_QUEUE", 1) == 1


# ──────────────────────────────────────────────────────────────────────────────
# Redis access (best-effort)
# ──────────────────────────────────────────────────────────────────────────────
_redis_client: Any = None
_redis_lock = asyncio.Lock()


async def _get_redis_async() -> Any:
    """
    Try common shapes in your stack:
      - utils_pkg.core_utils.get_redis_async()
      - utils_pkg.core_utils.get_redis() (sync) but returns async client sometimes
      - utils_pkg.core_utils.redis_async / redis
    """
    global _redis_client
    if _redis_client is not None:
        return _redis_client

    async with _redis_lock:
        if _redis_client is not None:
            return _redis_client

        try:
            from utils_pkg.core_utils import get_redis_async  # type: ignore

            _redis_client = await get_redis_async()
            return _redis_client
        except Exception:
            pass

        try:
            from utils_pkg.core_utils import get_redis  # type: ignore

            r = get_redis()
            _redis_client = r
            return _redis_client
        except Exception:
            pass

        try:
            from utils_pkg.core_utils import redis_async as r  # type: ignore

            _redis_client = r
            return _redis_client
        except Exception:
            pass

        try:
            from utils_pkg.core_utils import redis as r  # type: ignore

            _redis_client = r
            return _redis_client
        except Exception:
            _redis_client = None
            return None


# ──────────────────────────────────────────────────────────────────────────────
# Position store (best-effort hook)
# ──────────────────────────────────────────────────────────────────────────────
async def _persist_position_best_effort(payload: dict[str, Any]) -> None:
    """
    Best-effort store of initial position snapshot, so exits/pnl have a source of truth.
    We try:
      - root positions.py (redis-first, file fallback) : save_position(...)
      - utils_pkg.positions (PG wrapper)               : record_buy(...)
    This module MUST NOT become the source of truth; it only calls the canonical stores.
    """
    # root positions store (your current execution/exits state often uses this)
    try:
        import positions as root_positions  # type: ignore

        if hasattr(root_positions, "save_position"):
            mint = str(payload.get("mint") or "")
            if mint:
                await root_positions.save_position(mint, payload)  # type: ignore
                return
    except Exception:
        pass

    # utils_pkg positions (PG-backed wrapper)
    try:
        from utils_pkg import positions as upos  # type: ignore

        if hasattr(upos, "record_buy"):
            await upos.record_buy(  # type: ignore
                mint=str(payload.get("mint") or ""),
                qty_raw=int(payload.get("qty_raw") or 0),
                price=float(payload.get("entry_px") or 0.0),
                sig=str(payload.get("buy_sig") or ""),
                symbol=payload.get("symbol"),
                usd_spent=float(payload.get("size_usd") or 0.0),
                ts=float(payload.get("entry_ts") or time.time()),
            )
            return
    except Exception:
        pass


# ──────────────────────────────────────────────────────────────────────────────
# Queue primitives
# ──────────────────────────────────────────────────────────────────────────────
def _job_key(job_id: str) -> str:
    return f"{EXIT_JOB_KEY_PREFIX}{job_id}"


def _enq_key(job_id: str) -> str:
    return f"{EXIT_ENQ_KEY_PREFIX}{job_id}"


def _now() -> float:
    return time.time()


def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


async def _enqueue_job(job_id: str, payload: dict[str, Any], run_at_ts: float) -> bool:
    """
    Idempotent enqueue:
      1) SET exit:enq:{id} NX EX ttl   (guard)
      2) SET exit:job:{id} payload EX ttl (payload)
      3) ZADD exit_queue {id: score}  (schedule)
    Returns True if newly enqueued, False if it was already enqueued.
    """
    r = await _get_redis_async()
    if not r:
        return False

    enqk = _enq_key(job_id)
    jobk = _job_key(job_id)

    try:
        # Use a pipeline if available
        pipe = getattr(r, "pipeline", None)
        if callable(pipe):
            p = r.pipeline(transaction=True)
            p.set(enqk, "1", nx=True, ex=int(EXIT_ENQ_TTL_S))
            p.set(jobk, _json_dumps(payload), ex=int(EXIT_JOB_TTL_S))
            # zadd signature differs across clients; redis-py supports mapping
            p.zadd(EXIT_QUEUE_KEY, {job_id: float(run_at_ts)})
            res = await p.execute()
            # res[0] is SET NX result: True/OK if inserted, None if existed
            inserted = bool(res[0])
            return inserted

        # Fallback without pipeline
        inserted = await r.set(enqk, "1", nx=True, ex=int(EXIT_ENQ_TTL_S))
        if not inserted:
            return False
        await r.set(jobk, _json_dumps(payload), ex=int(EXIT_JOB_TTL_S))
        await r.zadd(EXIT_QUEUE_KEY, {job_id: float(run_at_ts)})
        return True
    except Exception as e:
        log.warning("EXIT_ENQUEUE_FAIL job=%s err=%s", job_id, e)
        return False


async def _queue_size() -> int | None:
    r = await _get_redis_async()
    if not r:
        return None
    try:
        return int(await r.zcard(EXIT_QUEUE_KEY))
    except Exception:
        return None


# ──────────────────────────────────────────────────────────────────────────────
# In-proc supervisor (optional, local/dev)
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class _Managed:
    job_id: str
    pos: PositionState
    sell_fn: SellFn
    price_fn: PriceFn


_REG: dict[str, _Managed] = {}
_SUP_TASK: asyncio.Task | None = None
_SUP_LOCK = asyncio.Lock()
_TICK = 0


async def stop_exit_supervisor() -> None:
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

    async def _start_once() -> None:
        global _SUP_TASK
        async with _SUP_LOCK:
            if _SUP_TASK and not _SUP_TASK.done():
                return
            _SUP_TASK = loop.create_task(_supervisor_loop(), name="exit_supervisor")

    loop.create_task(_start_once())
    # _SUP_TASK is set shortly; return a placeholder task if needed
    return _SUP_TASK or loop.create_task(asyncio.sleep(0), name="exit_supervisor_starting")


async def _supervisor_loop() -> None:
    global _TICK
    sem = asyncio.Semaphore(CONCURRENCY)
    hb_next = LOG_EVERY_TICKS

    # one-time config log
    with contextlib.suppress(Exception):
        await log_trade_event(
            type="exit_supervisor_started",
            mint=None,
            symbol=None,
            entry_sol_per_token=None,
            sol_spent=None,
            usd_spent=None,
            signature=None,
            cu_price=None,
            jito_tip=None,
            data={"tick_sec": TICK_SEC, "concurrency": CONCURRENCY, "cfg": exits_config_summary()},
        )

    while True:
        _TICK += 1
        items = list(_REG.items())

        async def _handle(job_id: str, m: _Managed) -> None:
            async with sem:
                try:
                    px = await m.price_fn(m.pos.mint)
                except Exception:
                    return
                if px is None:
                    return

                try:
                    # apply_exit mutates PositionState; sells are executed via provided sell_fn
                    dec = await apply_exit(m.pos, float(px), m.sell_fn)
                except Exception:
                    return

                # If position closed, drop from registry
                try:
                    if getattr(m.pos, "size_usd", 0.0) <= 0.0:
                        _REG.pop(job_id, None)
                    # Some engines may signal hard exit via decision object
                    if getattr(dec, "action", "") == "SELL_ALL":
                        _REG.pop(job_id, None)
                except Exception:
                    pass

        tasks = [asyncio.create_task(_handle(j, m)) for j, m in items]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        hb_next -= 1
        if hb_next <= 0:
            hb_next = LOG_EVERY_TICKS
            with contextlib.suppress(Exception):
                await log_trade_event(
                    type="exit_supervisor_heartbeat",
                    mint=None,
                    symbol=None,
                    entry_sol_per_token=None,
                    sol_spent=None,
                    usd_spent=None,
                    signature=None,
                    cu_price=None,
                    jito_tip=None,
                    data={"tick": _TICK, "managed": len(_REG)},
                )

        await asyncio.sleep(float(TICK_SEC))


# ──────────────────────────────────────────────────────────────────────────────
# PositionState creation (tolerant to drift)
# ──────────────────────────────────────────────────────────────────────────────
def _make_position_state(
    *,
    mint: str,
    entry_px: float,
    size_usd: float,
    symbol: str | None,
    entry_ts: float,
) -> PositionState:
    """
    PositionState signature may drift across rewrites; build it defensively.
    """
    # Try rich signature first
    try:
        return PositionState(  # type: ignore
            mint=mint,
            entry_px=float(entry_px),
            size_usd=float(size_usd),
            symbol=symbol,
            entry_ts=float(entry_ts),
        )
    except Exception:
        pass

    # Minimal signature fallback
    pos = PositionState(mint=mint, entry_px=float(entry_px), size_usd=float(size_usd))  # type: ignore

    # Set optional attrs if present
    for k, v in (
        ("symbol", symbol),
        ("entry_ts", float(entry_ts)),
        ("peak_px", float(entry_px)),
        ("tp1_done", False),
        ("tp2_done", False),
        ("last_action_ts", float(entry_ts)),
    ):
        try:
            if hasattr(pos, k):
                setattr(pos, k, v)
        except Exception:
            pass

    return pos


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

    In queue mode:
      - Persist position snapshot (best-effort)
      - Enqueue job into Redis exit_queue
      - sell_fn/price_fn are ignored (handled by exit worker)

    In inproc mode:
      - Store in in-memory registry and supervisor manages it
      - Optionally mirrors into queue (INPROC_MIRROR_QUEUE=1)

    Raises:
      - ValueError only when notional cannot be resolved.
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
        "exits_cfg": exits_config_summary(),
        "created_at": _now(),
    }

    # Persist initial position snapshot (best-effort)
    with contextlib.suppress(Exception):
        asyncio.create_task(_persist_position_best_effort(payload))

    # Enqueue job (queue mode OR inproc mirror)
    async def _do_enqueue() -> None:
        run_at = _now() + float(EXIT_INITIAL_DELAY_S)
        inserted = await _enqueue_job(str(signature), payload, run_at_ts=run_at)
        if inserted:
            log.event(
                "EXIT_ENQUEUED",
                job_id=str(signature),
                mint=mint,
                size_usd=float(notional_usd),
                run_at=run_at,
            )
        else:
            log.event("EXIT_ENQUEUE_DUP", job_id=str(signature), mint=mint)

    if EXIT_WIRING_MODE == "queue":
        with contextlib.suppress(Exception):
            asyncio.create_task(_do_enqueue())

    elif EXIT_WIRING_MODE == "inproc":
        # inproc requires sell_fn + price_fn
        if sell_fn is None or price_fn is None:
            raise ValueError("register_entry: inproc mode requires sell_fn and price_fn")

        pos = _make_position_state(
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
            with contextlib.suppress(Exception):
                asyncio.create_task(_do_enqueue())

    # Fire-and-forget structured event
    async def _log_evt() -> None:
        with contextlib.suppress(Exception):
            await log_trade_event(
                type="entry_registered",
                mint=mint,
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
                    "cfg": exits_config_summary(),
                },
            )

    with contextlib.suppress(Exception):
        asyncio.create_task(_log_evt())


def positions_count() -> int:
    """
    Count managed positions (inproc) OR best-effort queue size (queue).
    """
    if EXIT_WIRING_MODE == "inproc":
        return len(_REG)
    # queue mode: approximate (no blocking sync)
    return -1


async def get_exit_status() -> dict[str, Any]:
    """
    Lightweight snapshot for debugging/metrics.
    Includes:
      - wiring mode
      - inproc managed (if applicable)
      - redis queue size (if available)
    """
    out: dict[str, Any] = {
        "mode": EXIT_WIRING_MODE,
        "tick_sec": float(TICK_SEC),
        "concurrency": int(CONCURRENCY),
        "queue_key": EXIT_QUEUE_KEY,
        "managed_inproc": len(_REG) if EXIT_WIRING_MODE == "inproc" else 0,
        "cfg": exits_config_summary(),
    }

    qs = await _queue_size()
    if qs is not None:
        out["queue_size"] = qs

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
    "positions_count",
    "register_entry",
    "start_exit_supervisor",
    "stop_exit_supervisor",
]
