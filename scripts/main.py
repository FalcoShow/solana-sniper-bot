# -*- coding: utf-8 -*-
from __future__ import annotations

"""
scripts/main.py

God-Tier Orchestrator (Master Roadmap Phase 0–7) — FINAL LOCK-IN (V3+++)
======================================================================

CONTROL PLANE ONLY
------------------
This file supervises the whole system:
  • Starts scanners -> emits candidates (+ optional ticks)
  • Runs entry workers -> calls maybe_buy()
  • (Optional) Pre-buy unified scoring -> utils_pkg.scoring_service.score_candidate()
  • Routes ticks -> exit_engine.on_trade_tick()
  • Wires buy-filled events -> exit_engine.on_buy_filled()
  • Runs queue-based auto-exit worker (Redis zset consumer) if enabled
  • Optional reinvestor / smart_wallets loops
  • Watchdogs, bounded queues, dedupe, restart resilience
  • Optional: stdlib-only HTTP debug server (main web health lives in health.py)

HARD RULES
----------
- NO swap/build/sign tx code here.
- SAFE IMPORT: no network at import time. Only stdlib at module scope (besides logging_setup fallback).
- Optional modules may be missing; main must not crash unless MAIN_FAST_FAIL=1.
- Respect: KILL_SWITCH / DISABLE_BUYS / DISABLE_SELLS / DISABLE_SCANNERS (+ file flags).
- Never print secrets. (This file only logs high-level flags & counts.)
"""

# stdlib only at module scope
import asyncio
import contextlib
import hashlib
import importlib
import importlib.util
import inspect
import json
import os
import random
import signal
import socket
import sys
import time
import traceback
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence, Tuple

# =============================================================================
# Identity (RUN_ID is generated per run inside main(); build/version are stable)
# =============================================================================
MAIN_VERSION = "v3+++_lockin_orchestrator"
MAIN_BUILD = (os.getenv("GIT_COMMIT", "")[:12] or os.getenv("RENDER_GIT_COMMIT", "")[:12] or "").strip()
RUN_ID = ""  # set per main() invocation

_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}
_JITTER_DISABLED = os.getenv("MAIN_DISABLE_RANDOM_JITTER", "").strip().lower() in _TRUE

# =============================================================================
# Logging (soft import + shims)
# =============================================================================
try:
    from logging_setup import (  # type: ignore
        get_logger as _get_logger,
        bump as _bump,
        start_global_heartbeat as _start_global_heartbeat,
    )
except Exception:  # pragma: no cover
    import logging

    def _get_logger(name: str = "sniper.main") -> Any:  # type: ignore
        lg = logging.getLogger(name)
        if not lg.handlers:
            logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(name)s:%(message)s")
        return lg

    def _bump(*_a: Any, **_k: Any) -> None:  # type: ignore
        return None

    def _start_global_heartbeat(*_a: Any, **_k: Any) -> None:  # type: ignore
        return None


def _logger() -> Any:
    try:
        return _get_logger("sniper.main")  # type: ignore[misc]
    except TypeError:
        return _get_logger()  # type: ignore[misc]


log = _logger()


def _ensure_logger_shims() -> None:
    # log.event(name, **fields)
    if not hasattr(log, "event"):

        def _event(name: str, **fields: Any) -> None:
            with contextlib.suppress(Exception):
                log.info("%s %s", name, fields)

        setattr(log, "event", _event)

    # log.every(key, sec, msg=None, **fields): time-based throttle
    if not hasattr(log, "every"):

        def _every(key: str, sec: int = 60, msg: Optional[str] = None, **fields: Any) -> None:
            try:
                now_m = time.monotonic()
                store = getattr(_every, "_store", {})  # type: ignore[attr-defined]
                last = float(store.get(key, 0.0))
                if now_m - last >= float(sec):
                    store[key] = now_m
                    setattr(_every, "_store", store)  # type: ignore[attr-defined]
                    with contextlib.suppress(Exception):
                        log.event(msg or key, **fields)  # type: ignore[attr-defined]
            except Exception:
                return None

        setattr(log, "every", _every)


_ensure_logger_shims()


def bump(name: str) -> None:
    with contextlib.suppress(Exception):
        _bump(name)


# =============================================================================
# Tiny helpers
# =============================================================================
def _now() -> float:
    return time.time()


def _mono() -> float:
    return time.monotonic()


def _short_exc(e: BaseException, n: int = 420) -> str:
    s = f"{type(e).__name__}: {e}"
    return (s[:n] + "…") if len(s) > n else s


def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    return (v if v is not None else d).strip()


def _envb(k: str, d: bool = False) -> bool:
    v = _envs(k, "")
    if v == "":
        return bool(d)
    s = v.lower().strip()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return bool(d)


def _envi(k: str, d: int) -> int:
    try:
        return int(float(_envs(k, str(d))))
    except Exception:
        return int(d)


def _envf(k: str, d: float) -> float:
    try:
        return float(_envs(k, str(d)))
    except Exception:
        return float(d)


def _fast_fail() -> bool:
    return _envb("MAIN_FAST_FAIL", False)


def _json_dumps(x: Any, max_len: int = 250_000) -> str:
    try:
        s = json.dumps(x, ensure_ascii=False, separators=(",", ":"), default=str)
        if len(s) > max_len:
            return s[:max_len] + "…"
        return s
    except Exception:
        return "{}"


def _ema(prev: float, x: float, alpha: float = 0.12) -> float:
    if prev <= 0:
        return x
    return (1.0 - alpha) * prev + alpha * x


def _safe_float(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(d)


# =============================================================================
# Config (soft) — MUST be safe / no network
# =============================================================================
def _load_config() -> Any:
    try:
        return importlib.import_module("config")
    except Exception:

        class _C:
            ENABLE_RAYDIUM_SCANNER = True
            ENABLE_PUMP_SCANNER = True
            ENABLE_DEXSCREENER_SCANNER = True

            ENTRY_WORKERS = 2
            CANDIDATE_QUEUE_MAX = 2500
            TICK_QUEUE_MAX = 5000

            SUPERVISOR_SEC = 5
            RESTART_BACKOFF_SEC = 2
            RESTART_MAX_BACKOFF_SEC = 60
            RESTART_MAX_PER_HOUR = 60

            HEALTH_LOG_SEC = 30
            STARVATION_WARN_SEC = 180
            NO_BUY_WARN_SEC = 300

            MAIN_IDLE_SLEEP_SEC = 0.5
            SHUTDOWN_GRACE_SEC = 15.0

            MASTER_PHASE = int(os.getenv("MASTER_PHASE", "7"))

        return _C()


C = _load_config()


def _phase() -> int:
    p = _envi("MASTER_PHASE", int(getattr(C, "MASTER_PHASE", 7) or 7))
    if os.getenv("MASTER_PHASE") is None:
        p = _envi("STRATEGY_PHASE", p)
    return max(0, min(7, int(p)))


# =============================================================================
# Kill-switch + file flags
# =============================================================================
_KILL_FILE = _envs("KILL_SWITCH_FILE", "")
_DISABLE_BUYS_FILE = _envs("DISABLE_BUYS_FILE", "")
_DISABLE_SELLS_FILE = _envs("DISABLE_SELLS_FILE", "")
_DISABLE_SCANNERS_FILE = _envs("DISABLE_SCANNERS_FILE", "")

_KILL_WATCHDOG_SEC = max(0.5, float(_envf("KILL_WATCHDOG_SEC", 2.0)))
_KILL_SHUTDOWN_ON_TRIGGER = _envb("KILL_SHUTDOWN_ON_TRIGGER", False)

_PANIC_STOP = _envb("PANIC_STOP", False)  # immediate stop if set at boot (safety)
_MAIN_MAX_UPTIME_SEC = max(0.0, float(_envf("MAIN_MAX_UPTIME_SEC", 0.0)))  # 0 disables


def _file_flag(path: str) -> bool:
    if not path:
        return False
    with contextlib.suppress(Exception):
        return os.path.exists(path)
    return False


def _kill_active() -> bool:
    return _envb("KILL_SWITCH", False) or _file_flag(_KILL_FILE)


def _buys_disabled() -> bool:
    return _envb("DISABLE_BUYS", False) or _kill_active() or _file_flag(_DISABLE_BUYS_FILE)


def _sells_disabled() -> bool:
    return _envb("DISABLE_SELLS", False) or _kill_active() or _file_flag(_DISABLE_SELLS_FILE)


def _scanners_disabled() -> bool:
    return _envb("DISABLE_SCANNERS", False) or _kill_active() or _file_flag(_DISABLE_SCANNERS_FILE)


def _apply_disable_sells_to_env() -> None:
    """
    If sells are disabled, force-disable downstream sell execution behavior via env
    BEFORE we import exit_engine / auto_exit modules.
    """
    if not _sells_disabled():
        return
    os.environ["EXIT_ALLOW_WORKER_ENQUEUE"] = "0"
    os.environ["EXIT_ALLOW_DIRECT_FALLBACK"] = "0"
    os.environ["EXIT_FOLLOW_ALLOW_DIRECT_FALLBACK"] = "0"
    os.environ["AUTO_EXIT_WORKER_ENABLED"] = "0"


# =============================================================================
# Runtime soft import helper
# =============================================================================
def _soft_import(mod_name: str, *, aliases: Optional[List[str]] = None, critical: bool = False) -> Optional[Any]:
    tried: List[str] = []
    for name in [mod_name] + (aliases or []):
        tried.append(name)
        try:
            return importlib.import_module(name)
        except Exception as e:
            log.every("SOFT_IMPORT_FAIL", 30, module=name, err=_short_exc(e))
    log.every("SOFT_IMPORT_ALL_FAIL", 60, module=mod_name, tried=",".join(tried)[:320])
    if critical and _fast_fail():
        raise RuntimeError(f"Critical import failed: {mod_name} tried={tried!r}")
    return None


def _module_exists(name: str) -> bool:
    with contextlib.suppress(Exception):
        return importlib.util.find_spec(name) is not None
    return False


# =============================================================================
# Signature-safe helpers
# =============================================================================
def _filter_kwargs(fn: Any, kwargs: Dict[str, Any]) -> Dict[str, Any]:
    try:
        sig = inspect.signature(fn)
        params = sig.parameters.values()
        if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params):
            return kwargs
        allowed = {p.name for p in params}
        return {k: v for k, v in kwargs.items() if k in allowed}
    except Exception:
        return kwargs


def _filter_kwargs_ctor(cls_or_fn: Any, kwargs: Dict[str, Any]) -> Dict[str, Any]:
    try:
        if inspect.isclass(cls_or_fn):
            init = getattr(cls_or_fn, "__init__", None)
            if callable(init):
                return _filter_kwargs(init, kwargs)
        return _filter_kwargs(cls_or_fn, kwargs)
    except Exception:
        return kwargs


async def _call_maybe_async(fn: Any, *args: Any, **kwargs: Any) -> Any:
    res = fn(*args, **kwargs)
    if inspect.isawaitable(res):
        return await res
    return res


def _threadify_sync_modules_enabled() -> bool:
    return _envb("MAIN_THREADIFY_SYNC_MODULES", False)


async def _call_possibly_blocking(fn: Any, *args: Any, **kwargs: Any) -> Any:
    try:
        is_coro_fn = inspect.iscoroutinefunction(fn)
    except Exception:
        is_coro_fn = False

    if is_coro_fn:
        return await _call_maybe_async(fn, *args, **kwargs)

    if _threadify_sync_modules_enabled():
        return await asyncio.to_thread(fn, *args, **kwargs)

    return await _call_maybe_async(fn, *args, **kwargs)


def _install_loop_exception_handler() -> None:
    with contextlib.suppress(Exception):
        loop = asyncio.get_running_loop()

        def _handler(_loop: asyncio.AbstractEventLoop, context: Dict[str, Any]) -> None:
            msg = str(context.get("message") or "asyncio_exception")
            exc = context.get("exception")
            if exc:
                log.event("ASYNCIO_EXCEPTION", msg=msg, err=_short_exc(exc))
            else:
                log.event("ASYNCIO_EXCEPTION", msg=msg, context=str(context)[:1600])

        loop.set_exception_handler(_handler)


async def _cancel_and_gather(task_map: Dict[str, asyncio.Task], timeout: float) -> None:
    me = asyncio.current_task()
    to_wait: List[asyncio.Task] = []
    for _, t in list(task_map.items()):
        if t is me:
            continue
        if not t.done():
            t.cancel()
        to_wait.append(t)

    if not to_wait:
        return

    with contextlib.suppress(Exception):
        await asyncio.wait_for(asyncio.gather(*to_wait, return_exceptions=True), timeout=timeout)


# =============================================================================
# Optional: preflight runner (subprocess, no side-effects in-process)
# =============================================================================
async def _run_preflight_if_enabled(run_id: str) -> None:
    """
    If START_PRECHECKS=1 and RUN_PREFLIGHT_IN_MAIN=1, run the canonical preflight.
    (In Render, start.sh can run preflight; this is a belt-and-suspenders option.)
    """
    if not _envb("START_PRECHECKS", False):
        return
    if not _envb("RUN_PREFLIGHT_IN_MAIN", False):
        return
    if not _module_exists("scripts.preflight"):
        log.event("PREFLIGHT_MISSING", run_id=run_id)
        if _fast_fail():
            raise RuntimeError("scripts.preflight missing")
        return

    exe = sys.executable or "python"
    args = [exe, "-m", "scripts.preflight"]
    if _envb("START_PRECHECKS_STRICT", False) or _envb("PREFLIGHT_STRICT", False):
        args.append("--strict")
    if _envb("PREFLIGHT_JSON", False):
        args.append("--json")

    log.event("PREFLIGHT_START", run_id=run_id)
    try:
        proc = await asyncio.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        try:
            out, err = await asyncio.wait_for(
                proc.communicate(), timeout=max(20.0, _envf("PREFLIGHT_MAIN_TIMEOUT_S", 120.0))
            )
        except asyncio.TimeoutError:
            with contextlib.suppress(Exception):
                proc.kill()
            raise RuntimeError("preflight timeout")

        rc = int(proc.returncode or 0)
        if err:
            log.every("PREFLIGHT_STDERR", 10, run_id=run_id, stderr=err.decode("utf-8", "ignore")[:2000])
        if out:
            log.every("PREFLIGHT_STDOUT", 10, run_id=run_id, stdout=out.decode("utf-8", "ignore")[:2000])

        if rc != 0:
            raise RuntimeError(f"preflight failed rc={rc}")
        log.event("PREFLIGHT_OK", run_id=run_id)
    except Exception as e:
        log.event("PREFLIGHT_FAIL", run_id=run_id, err=_short_exc(e))
        if _envb("PREFLIGHT_FAIL_HARD", True) or _fast_fail():
            raise


# =============================================================================
# Mint normalizer resolver (supports older repos)
# =============================================================================
def _resolve_normalize_mint() -> Callable[[str], str]:
    with contextlib.suppress(Exception):
        m = importlib.import_module("utils_pkg.mints")
        fn = getattr(m, "normalize_mint", None)
        if callable(fn):
            return fn  # type: ignore[return-value]
    with contextlib.suppress(Exception):
        m = importlib.import_module("scripts.pp_utils")
        fn = getattr(m, "normalize_mint", None)
        if callable(fn):
            return fn  # type: ignore[return-value]
    return lambda x: (x or "").strip()


# =============================================================================
# Optional bootstraps (best effort)
# =============================================================================
async def _core_bootstrap_best_effort(run_id: str) -> None:
    cu = _soft_import("utils_pkg.core_utils", aliases=["utils_pkg.core", "utils_pkg.bootstrap"])
    if not cu:
        return
    for nm in ("bootstrap", "init", "setup", "warmup", "init_resources"):
        fn = getattr(cu, nm, None)
        if callable(fn):
            try:
                out = fn()
                if inspect.isawaitable(out):
                    await out
                log.event("CORE_BOOTSTRAP_OK", run_id=run_id, fn=nm)
            except Exception as e:
                log.event("CORE_BOOTSTRAP_FAIL", run_id=run_id, fn=nm, err=_short_exc(e))
                if _fast_fail():
                    raise
            return


async def _ai_db_bootstrap_best_effort(run_id: str) -> None:
    adb = _soft_import("utils_pkg.ai_db", critical=False)
    if not adb:
        return
    for nm in ("bootstrap", "migrate", "ensure_schema", "init_db", "setup"):
        fn = getattr(adb, nm, None)
        if callable(fn):
            try:
                out = fn()
                if inspect.isawaitable(out):
                    await out
                log.event("AI_DB_BOOTSTRAP_OK", run_id=run_id, fn=nm)
            except Exception as e:
                log.event("AI_DB_BOOTSTRAP_FAIL", run_id=run_id, fn=nm, err=_short_exc(e))
                if _fast_fail():
                    raise
            return


# =============================================================================
# Modules resolved lazily inside main()
# =============================================================================
@dataclass
class Modules:
    normalize_mint: Callable[[str], str]

    maybe_buy: Optional[Callable[..., Any]] = None
    buys_allowed: Optional[Callable[[], bool]] = None

    # Optional AI gate (legacy)
    ai_pass_gate: Optional[Callable[..., Any]] = None
    ai_candidate_cls: Optional[Any] = None

    # Preferred unified scorer
    scoring_mod: Optional[Any] = None
    scoring_fn: Optional[Callable[..., Any]] = None  # async score_candidate
    scoring_enabled: bool = True

    routeguard_mod: Optional[Any] = None
    risk_mod: Optional[Any] = None

    exit_mod: Optional[Any] = None

    # Redis queue-based auto-exit worker
    auto_exit_worker_mod: Optional[Any] = None
    auto_exit_worker_loop: Optional[Callable[..., Any]] = None

    reinvest_mod: Optional[Any] = None
    smart_mod: Optional[Any] = None

    data_logger_mod: Optional[Any] = None


def _resolve_modules() -> Modules:
    nm = _resolve_normalize_mint()

    # BUY executor: prefer pump_trader.maybe_buy
    maybe_buy = None
    with contextlib.suppress(Exception):
        m = importlib.import_module("utils_pkg.pump_trader")
        maybe_buy = getattr(m, "maybe_buy", None)

    if maybe_buy is None:
        pt = _soft_import(
            "utils_pkg.trader",
            aliases=["utils_pkg.trade_engine", "utils_pkg.entry_engine", "utils_pkg.pump_trader"],
        )
        maybe_buy = getattr(pt, "maybe_buy", None) if pt else None

    # guardrails.buys_allowed optional
    buys_allowed = None
    with contextlib.suppress(Exception):
        gr = importlib.import_module("utils_pkg.guardrails")
        buys_allowed = getattr(gr, "buys_allowed", None)

    # AI gate (optional)
    ai_pass_gate = None
    ai_candidate_cls = None
    with contextlib.suppress(Exception):
        ai = importlib.import_module("utils_pkg.ai_buy_scoring")
        ai_pass_gate = getattr(ai, "pass_gate", None)
        ai_candidate_cls = getattr(ai, "Candidate", None)

    # Unified scoring service (optional but recommended)
    scoring_mod = _soft_import("utils_pkg.scoring_service", critical=False)
    scoring_fn = None
    if scoring_mod is not None:
        fn = getattr(scoring_mod, "score_candidate", None)
        if callable(fn):
            scoring_fn = fn  # type: ignore[assignment]

    scoring_enabled = _envb("SCORING_SERVICE_ENABLED", True)

    # Routeguard + risk manager (optional)
    routeguard_mod = _soft_import("utils_pkg.routeguard", critical=False)
    risk_mod = _soft_import("utils_pkg.risk_manager", critical=False)

    # Exit engine (optional)
    exit_mod = _soft_import("utils_pkg.exit_engine", critical=False)

    # Auto-exit queue worker (Redis consumer)
    auto_exit_worker_mod = _soft_import("utils_pkg.auto_exit_worker.exit_engine", critical=False)
    auto_exit_worker_loop = None
    if auto_exit_worker_mod is not None:
        fn = getattr(auto_exit_worker_mod, "worker_loop", None)
        if callable(fn):
            auto_exit_worker_loop = fn  # type: ignore[assignment]

    # Optional brains
    reinvest_mod = _soft_import("utils_pkg.reinvestor", critical=False)
    smart_mod = _soft_import("utils_pkg.smart_wallets", critical=False)

    # Optional data logger
    data_logger_mod = _soft_import(
        "utils_pkg.data_logger",
        aliases=["utils_pkg.logger", "utils_pkg.trade_logger"],
        critical=False,
    )

    return Modules(
        normalize_mint=nm,
        maybe_buy=maybe_buy if callable(maybe_buy) else None,
        buys_allowed=buys_allowed if callable(buys_allowed) else None,
        ai_pass_gate=ai_pass_gate if callable(ai_pass_gate) else None,
        ai_candidate_cls=ai_candidate_cls,
        scoring_mod=scoring_mod,
        scoring_fn=scoring_fn,
        scoring_enabled=scoring_enabled,
        routeguard_mod=routeguard_mod,
        risk_mod=risk_mod,
        exit_mod=exit_mod,
        auto_exit_worker_mod=auto_exit_worker_mod,
        auto_exit_worker_loop=auto_exit_worker_loop,
        reinvest_mod=reinvest_mod,
        smart_mod=smart_mod,
        data_logger_mod=data_logger_mod,
    )


# =============================================================================
# Data models
# =============================================================================
@dataclass
class CandidateMsg:
    mint: str
    ui: Dict[str, Any] = field(default_factory=dict)
    source: str = "unknown"
    ts: float = field(default_factory=_now)
    kind: str = "candidate"  # candidate | tick
    trace_id: str = ""


@dataclass
class RuntimeState:
    run_id: str = ""
    started_at: float = field(default_factory=_now)
    started_mono: float = field(default_factory=_mono)

    last_candidate_at: float = 0.0
    last_tick_at: float = 0.0
    last_buy_action_at: float = 0.0
    last_buy_fill_at: float = 0.0

    buy_ok: int = 0
    buy_fail: int = 0
    buy_exc: int = 0
    buy_skip: int = 0

    cand_seen: int = 0
    cand_dropped: int = 0
    cand_sunk: int = 0

    tick_seen: int = 0
    tick_dropped: int = 0
    tick_sunk: int = 0

    per_source_seen: Dict[str, int] = field(default_factory=dict)
    per_source_dropped: Dict[str, int] = field(default_factory=dict)
    per_source_last: Dict[str, float] = field(default_factory=dict)

    loop_lag_ms: float = 0.0
    last_health_at: float = 0.0

    cand_q_hwm: int = 0
    tick_q_hwm: int = 0
    cand_lat_ms_ema: float = 0.0
    tick_lat_ms_ema: float = 0.0

    last_kill_seen: float = 0.0


# =============================================================================
# Snapshot (debug)
# =============================================================================
def _snapshot_state(
    M: Modules,
    st: RuntimeState,
    *,
    cand_q: asyncio.Queue,
    tick_q: asyncio.Queue,
    tasks: Optional[Dict[str, asyncio.Task]] = None,
) -> Dict[str, Any]:
    task_view: Dict[str, Any] = {}
    if isinstance(tasks, dict):
        with contextlib.suppress(Exception):
            task_view = {
                k: {"done": int(t.done()), "cancelled": int(t.cancelled())}
                for k, t in list(tasks.items())[:800]
            }

    return {
        "version": MAIN_VERSION,
        "build": MAIN_BUILD,
        "run_id": st.run_id,
        "up_sec": int(_now() - st.started_at),
        "phase": _phase(),
        "queues": {
            "cand_q": int(cand_q.qsize()),
            "tick_q": int(tick_q.qsize()),
            "cand_q_hwm": int(st.cand_q_hwm),
            "tick_q_hwm": int(st.tick_q_hwm),
            "cand_lat_ms_ema": int(st.cand_lat_ms_ema),
            "tick_lat_ms_ema": int(st.tick_lat_ms_ema),
        },
        "cand": {"seen": st.cand_seen, "dropped": st.cand_dropped, "sunk": st.cand_sunk},
        "tick": {"seen": st.tick_seen, "dropped": st.tick_dropped, "sunk": st.tick_sunk},
        "buy": {"ok": st.buy_ok, "fail": st.buy_fail, "exc": st.buy_exc, "skip": st.buy_skip},
        "flags": {
            "kill": int(_kill_active()),
            "disable_buys": int(_buys_disabled()),
            "disable_sells": int(_sells_disabled()),
            "disable_scanners": int(_scanners_disabled()),
            "dryrun": int(_envb("DRYRUN", _envb("DRY_RUN", False))),
            "fast_fail": int(_fast_fail()),
        },
        "modules": {
            "buy_executor": int(M.maybe_buy is not None),
            "ai_gate": int(M.ai_pass_gate is not None),
            "scoring_service": int(M.scoring_fn is not None and M.scoring_enabled),
            "exit_engine": int(M.exit_mod is not None),
            "auto_exit_worker": int(M.auto_exit_worker_loop is not None),
            "reinvestor": int(M.reinvest_mod is not None),
            "smart_wallets": int(M.smart_mod is not None),
            "routeguard": int(M.routeguard_mod is not None),
            "risk_manager": int(M.risk_mod is not None),
        },
        "last": {
            "cand_sec_ago": int(_now() - st.last_candidate_at) if st.last_candidate_at else -1,
            "tick_sec_ago": int(_now() - st.last_tick_at) if st.last_tick_at else -1,
            "buy_sec_ago": int(_now() - st.last_buy_action_at) if st.last_buy_action_at else -1,
        },
        "tasks": task_view,
    }


# =============================================================================
# Deduper (cheap spam shield)
# =============================================================================
class _Deduper:
    def __init__(self, ttl_sec: float, max_items: int) -> None:
        self.ttl = float(ttl_sec)
        self.max_items = int(max_items)
        self._m: "OrderedDict[str, float]" = OrderedDict()

    def _evict(self, now_m: float) -> None:
        cutoff = now_m - self.ttl
        while self._m:
            k0, t0 = next(iter(self._m.items()))
            if t0 >= cutoff:
                break
            self._m.popitem(last=False)
        while len(self._m) > self.max_items:
            self._m.popitem(last=False)

    def seen_recently(self, key: str) -> bool:
        now_m = _mono()
        t = self._m.get(key)
        if t is not None and (now_m - t) < self.ttl:
            self._m.move_to_end(key, last=True)
            return True
        self._m[key] = now_m
        self._m.move_to_end(key, last=True)
        self._evict(now_m)
        return False


_CAND_DEDUP = _Deduper(ttl_sec=_envf("CAND_DEDUP_TTL_SEC", 10.0), max_items=_envi("CAND_DEDUP_MAX", 75_000))
_TICK_DEDUP = _Deduper(ttl_sec=_envf("TICK_DEDUP_TTL_SEC", 2.0), max_items=_envi("TICK_DEDUP_MAX", 200_000))

_DROP_POLICY = (_envs("QUEUE_DROP_POLICY", "drop_newest") or "drop_newest").strip().lower()
_DROP_OLDEST = _DROP_POLICY in {"drop_oldest", "oldest", "pop_oldest", "drop-oldest"}


def _bump_source(st: RuntimeState, source: str, *, seen: bool = False, dropped: bool = False) -> None:
    s = source or "unknown"
    if seen:
        st.per_source_seen[s] = int(st.per_source_seen.get(s, 0)) + 1
        st.per_source_last[s] = _now()
    if dropped:
        st.per_source_dropped[s] = int(st.per_source_dropped.get(s, 0)) + 1


def _trace_id_for(mint: str, source: str, ui: Dict[str, Any], ts: float, run_id: str) -> str:
    seed = f"{mint}|{source}|{ui.get('pair') or ui.get('pairAddress') or ''}|{int(ts)}|{run_id}"
    return hashlib.sha1(seed.encode("utf-8", "ignore")).hexdigest()[:12]


# =============================================================================
# Candidate/Tick helpers
# =============================================================================
def _looks_like_tick(d: Dict[str, Any]) -> bool:
    if not isinstance(d, dict):
        return False
    kind = str(d.get("kind") or d.get("type") or d.get("event") or "").lower()
    if kind in {"tick", "trade", "swap", "fill", "price"}:
        return True
    has_price = any(
        k in d
        for k in (
            "price",
            "priceUi",
            "price_ui",
            "priceInSol",
            "price_in_sol",
            "last_price",
            "lastPrice",
        )
    )
    has_amt = any(k in d for k in ("amount", "amount_raw", "amount_in", "amount_out", "qty", "size"))
    return bool(has_price and (has_amt or kind != ""))


def _queue_hwm_update(st: RuntimeState, cand_q: asyncio.Queue, tick_q: asyncio.Queue) -> None:
    with contextlib.suppress(Exception):
        st.cand_q_hwm = max(st.cand_q_hwm, int(cand_q.qsize()))
    with contextlib.suppress(Exception):
        st.tick_q_hwm = max(st.tick_q_hwm, int(tick_q.qsize()))


def _source_to_pool_hint(source: str) -> str:
    s = (source or "").lower()
    if "raydium" in s:
        return "raydium"
    if "dexscreener" in s:
        return "dexscreener"
    if "pump" in s:
        return "pump"
    if "axiom" in s:
        return "axiom"
    return "unknown"


def _enqueue_candidate_nowait(
    M: Modules,
    out_q: "asyncio.Queue[CandidateMsg]",
    msg: CandidateMsg,
    st: RuntimeState,
) -> None:
    mint = M.normalize_mint(msg.mint)
    if not mint:
        return

    msg.mint = mint
    msg.ts = float(msg.ts or _now())
    msg.source = msg.source or "unknown"
    msg.trace_id = msg.trace_id or _trace_id_for(mint, msg.source, msg.ui or {}, msg.ts, st.run_id)

    _bump_source(st, msg.source, seen=True)

    if _envb("CAND_DEDUP_ENABLED", True):
        rid = str((msg.ui or {}).get("pair") or (msg.ui or {}).get("pairAddress") or (msg.ui or {}).get("id") or "")
        key = f"{msg.source}:{msg.mint}:{rid}" if rid else f"{msg.source}:{msg.mint}"
        if _CAND_DEDUP.seen_recently(key):
            bump("cand_dedup_drop")
            st.cand_dropped += 1
            _bump_source(st, msg.source, dropped=True)
            return

    try:
        out_q.put_nowait(msg)
    except asyncio.QueueFull:
        if _DROP_OLDEST:
            with contextlib.suppress(Exception):
                _ = out_q.get_nowait()
                out_q.task_done()
            with contextlib.suppress(Exception):
                out_q.put_nowait(msg)
            bump("cand_drop_oldest")
            st.cand_dropped += 1
            _bump_source(st, msg.source, dropped=True)
            return
        bump("cand_queue_full")
        st.cand_dropped += 1
        _bump_source(st, msg.source, dropped=True)


def _enqueue_tick_nowait(
    M: Modules,
    tick_q: "asyncio.Queue[Dict[str, Any]]",
    d: Dict[str, Any],
    st: RuntimeState,
) -> None:
    mint = M.normalize_mint(str(d.get("mint") or ""))
    if not mint:
        return

    if _envb("TICK_DEDUP_ENABLED", True):
        price = str(d.get("price") or d.get("priceUi") or d.get("price_ui") or d.get("last_price") or "")
        key = f"{mint}:{price}"
        if _TICK_DEDUP.seen_recently(key):
            bump("tick_dedup_drop")
            st.tick_dropped += 1
            return

    try:
        tick_q.put_nowait(d)
    except asyncio.QueueFull:
        if _DROP_OLDEST:
            with contextlib.suppress(Exception):
                _ = tick_q.get_nowait()
                tick_q.task_done()
            with contextlib.suppress(Exception):
                tick_q.put_nowait(d)
            bump("tick_drop_oldest")
            st.tick_dropped += 1
            return
        bump("tick_queue_full")
        st.tick_dropped += 1


def _push_payload_nowait(
    M: Modules,
    out_q: "asyncio.Queue[CandidateMsg]",
    tick_q: "asyncio.Queue[Dict[str, Any]]",
    source: str,
    payload: Any,
    stop_evt: asyncio.Event,
    st: RuntimeState,
) -> None:
    if stop_evt.is_set() or payload is None:
        return

    forward_ticks = _envb("FORWARD_TICKS_FROM_SCANNERS", False)

    if isinstance(payload, CandidateMsg):
        payload.source = payload.source or source
        _enqueue_candidate_nowait(M, out_q, payload, st)
        if forward_ticks and isinstance(payload.ui, dict) and _looks_like_tick(payload.ui):
            _enqueue_tick_nowait(M, tick_q, dict(payload.ui), st)
        return

    if not isinstance(payload, dict):
        return

    mint = M.normalize_mint(
        str(payload.get("mint") or payload.get("mintAddress") or payload.get("address") or payload.get("token") or "")
    )
    if not mint:
        return

    ui = dict(payload)
    ui.setdefault("mint", mint)
    ui.setdefault("source", source)
    ui.setdefault("pool", ui.get("pool") or ui.get("venue") or ui.get("dex") or _source_to_pool_hint(source))
    ui.setdefault("ts", float(ui.get("ts") or ui.get("timestamp") or _now()))

    if forward_ticks and _looks_like_tick(ui):
        _enqueue_tick_nowait(M, tick_q, ui, st)

    msg = CandidateMsg(mint=mint, ui=ui, source=source, kind="candidate", ts=float(ui["ts"]))
    msg.trace_id = _trace_id_for(mint, source, ui, msg.ts, st.run_id)
    _enqueue_candidate_nowait(M, out_q, msg, st)


async def _push_payload(
    M: Modules,
    out_q: "asyncio.Queue[CandidateMsg]",
    tick_q: "asyncio.Queue[Dict[str, Any]]",
    source: str,
    payload: Any,
    stop_evt: asyncio.Event,
    st: RuntimeState,
) -> None:
    _push_payload_nowait(M, out_q, tick_q, source, payload, stop_evt, st)


class _ScannerQueueProxy:
    """Lets legacy scanners do await q.put(dict)/q.put_nowait(dict) safely."""

    def __init__(
        self,
        *,
        M: Modules,
        out_q: "asyncio.Queue[CandidateMsg]",
        tick_q: "asyncio.Queue[Dict[str, Any]]",
        source: str,
        stop_evt: asyncio.Event,
        st: RuntimeState,
    ) -> None:
        self._M = M
        self._out_q = out_q
        self._tick_q = tick_q
        self._source = source
        self._stop = stop_evt
        self._st = st

    async def put(self, item: Any) -> None:
        await _push_payload(self._M, self._out_q, self._tick_q, self._source, item, self._stop, self._st)

    def put_nowait(self, item: Any) -> None:
        _push_payload_nowait(self._M, self._out_q, self._tick_q, self._source, item, self._stop, self._st)

    def qsize(self) -> int:
        return self._out_q.qsize()

    def empty(self) -> bool:
        return self._out_q.empty()

    @property
    def maxsize(self) -> int:
        return int(getattr(self._out_q, "maxsize", 0) or 0)


# =============================================================================
# Scanner runner
# =============================================================================
def _scanner_entrypoints(mod: Any) -> List[Callable[..., Any]]:
    out: List[Callable[..., Any]] = []
    for nm in ("run_scanner", "run", "main", "scan_loop", "scanner_loop"):
        fn = getattr(mod, nm, None)
        if callable(fn):
            out.append(fn)
    for nm in ("scan_once", "scan"):
        fn = getattr(mod, nm, None)
        if callable(fn):
            out.append(fn)
    for nm in ("scan_raydium", "scan_pumpfun", "scan_dexscreener", "scan_axiom"):
        fn = getattr(mod, nm, None)
        if callable(fn):
            out.append(fn)
    return out


async def _consume_result_as_stream(
    result: Any,
    emit: Callable[[Any], Awaitable[None]],
    stop_evt: asyncio.Event,
    *,
    module_name: str,
) -> None:
    if result is None or stop_evt.is_set():
        return

    if inspect.isasyncgen(result) or hasattr(result, "__aiter__"):
        try:
            async for item in result:
                if stop_evt.is_set():
                    break
                await emit(item)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.event("SCANNER_STREAM_FAIL", module=module_name, err=_short_exc(e))
        return

    if not isinstance(result, (dict, str, bytes, bytearray)) and hasattr(result, "__iter__"):
        try:
            for item in result:
                if stop_evt.is_set():
                    break
                await emit(item)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.event("SCANNER_ITER_FAIL", module=module_name, err=_short_exc(e))
        return

    if isinstance(result, dict):
        await emit(result)


async def _scanner_run_module(
    M: Modules,
    module_name: str,
    out_q: "asyncio.Queue[CandidateMsg]",
    tick_q: "asyncio.Queue[Dict[str, Any]]",
    stop_evt: asyncio.Event,
    st: RuntimeState,
) -> None:
    try:
        mod = importlib.import_module(module_name)
    except Exception as e:
        log.event("SCANNER_IMPORT_FAIL", module=module_name, err=_short_exc(e))
        if _fast_fail():
            raise
        return

    async def _emit(payload: Any) -> None:
        await _push_payload(M, out_q, tick_q, module_name, payload, stop_evt, st)

    proxy_q = _ScannerQueueProxy(M=M, out_q=out_q, tick_q=tick_q, source=module_name, stop_evt=stop_evt, st=st)

    fns = _scanner_entrypoints(mod)
    if not fns:
        log.event("SCANNER_NO_ENTRYPOINT", module=module_name)
        if _fast_fail():
            raise RuntimeError(f"Scanner has no entrypoints: {module_name}")
        return

    def _score_fn(fn: Callable[..., Any]) -> int:
        nm = getattr(fn, "__name__", "")
        if nm in {"run_scanner"}:
            return 110
        if nm in {"run", "main", "scan_loop", "scanner_loop"}:
            return 100
        if nm in {"scan_once", "scan"}:
            return 60
        return 10

    fns.sort(key=_score_fn, reverse=True)

    async def _try_call(fn: Callable[..., Any]) -> bool:
        if stop_evt.is_set():
            return True

        # kwargs-based probes
        for kwargs in (
            {"q": proxy_q, "queue": proxy_q, "out_q": proxy_q, "emit": _emit, "stop_evt": stop_evt},
            {"q": proxy_q, "queue": proxy_q, "stop_evt": stop_evt},
            {"emit": _emit, "stop_evt": stop_evt},
            {"stop_evt": stop_evt},
        ):
            try:
                kw = _filter_kwargs(fn, dict(kwargs))
                res = await _call_possibly_blocking(fn, **kw)  # type: ignore[misc]
                await _consume_result_as_stream(res, _emit, stop_evt, module_name=module_name)
                return True
            except TypeError:
                continue
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.event("SCANNER_RUN_FAIL", module=module_name, fn=getattr(fn, "__name__", "?"), err=_short_exc(e))
                if _fast_fail():
                    raise
                return True

        # args-based probes
        for args in ((proxy_q, stop_evt), (_emit, stop_evt), (proxy_q,), (stop_evt,), tuple()):
            try:
                res = await _call_possibly_blocking(fn, *args)  # type: ignore[misc]
                await _consume_result_as_stream(res, _emit, stop_evt, module_name=module_name)
                return True
            except TypeError:
                continue
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.event("SCANNER_RUN_FAIL", module=module_name, fn=getattr(fn, "__name__", "?"), err=_short_exc(e))
                if _fast_fail():
                    raise
                return True

        return False

    base_interval = max(0.2, float(_envf("SCAN_ONCE_INTERVAL_SEC", 2.0)))
    jitter = max(0.0, float(_envf("SCAN_ONCE_JITTER_SEC", 0.35)))
    if _JITTER_DISABLED:
        jitter = 0.0

    for fn in fns:
        nm = getattr(fn, "__name__", "")

        if nm in {"scan_once", "scan"}:
            while not stop_evt.is_set():
                if _scanners_disabled():
                    log.every("SCANNERS_DISABLED_RUNTIME", 15, module=module_name)
                    with contextlib.suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(stop_evt.wait(), timeout=1.0)
                    continue
                _ = await _try_call(fn)
                j = 0.0 if jitter <= 0 else random.uniform(-jitter, jitter)
                await asyncio.sleep(max(0.05, base_interval + j))
            return

        if _scanners_disabled():
            log.every("SCANNERS_DISABLED_RUNTIME", 15, module=module_name)
            await stop_evt.wait()
            return

        ok = await _try_call(fn)
        if ok:
            return

    log.event("SCANNER_NO_COMPATIBLE_CALL", module=module_name)
    if _fast_fail():
        raise RuntimeError(f"No compatible call signature for scanner: {module_name}")


# =============================================================================
# Pre-buy gating: scoring_service + AI gate + routeguard + risk_manager
# =============================================================================
async def _scoring_gate(M: Modules, ui: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Preferred unified scorer gate.
    Attaches ui['score'] = dict when available.
    """
    if (not M.scoring_enabled) or (M.scoring_fn is None):
        return True, "scoring_off"

    fail_closed = _envb("SCORING_SERVICE_FAIL_CLOSED", False)
    attach_features = _envb("SCORING_SERVICE_ATTACH_FEATURES", False)  # can be big; default off
    gate_mode = (_envs("SCORING_SERVICE_GATE_MODE", "trade_viable") or "trade_viable").lower()
    min_combined = float(_envf("SCORING_SERVICE_MIN_COMBINED", 0.0))  # optional extra clamp

    raw_pool = dict(ui)
    raw_pool.setdefault("mint", ui.get("mint"))
    raw_pool.setdefault("symbol", ui.get("symbol") or ui.get("sym") or "UNKNOWN")
    raw_pool.setdefault("chain", ui.get("chain") or "SOL")
    raw_pool.setdefault("liq_usd", ui.get("liq_usd") or ui.get("liquidity_usd") or ui.get("liq") or 0.0)
    raw_pool.setdefault("age_min", ui.get("age_min") or ui.get("token_age_min") or 0.0)

    context: Dict[str, Any] = {}
    if isinstance(ui.get("regime"), dict):
        context["regime"] = ui.get("regime")

    try:
        st = await M.scoring_fn(raw_pool, context=context)  # type: ignore[misc]
        if hasattr(st, "to_dict") and callable(getattr(st, "to_dict")):
            st_dict = st.to_dict()  # type: ignore[misc]
        elif isinstance(st, dict):
            st_dict = dict(st)
        else:
            st_dict = {"score": str(st)}

        if not attach_features and isinstance(st_dict.get("raw_features"), dict):
            st_dict.pop("raw_features", None)

        ui["score"] = st_dict

        if gate_mode in {"trade_viable", "viable", "default"}:
            ok = bool(st_dict.get("is_trade_viable", True))
            reason = str(st_dict.get("trade_gate_reason") or "trade_viable_gate")
        elif gate_mode in {"label"}:
            ok = str(st_dict.get("label") or "").upper() in {"GREEN", "YELLOW"}
            reason = f"label={st_dict.get('label')}"
        elif gate_mode in {"combined"}:
            ok = float(st_dict.get("combined_score") or 0.0) >= max(0.0, min_combined)
            reason = f"combined={st_dict.get('combined_score')}"
        else:
            ok = bool(st_dict.get("is_trade_viable", True))
            reason = str(st_dict.get("trade_gate_reason") or "trade_viable_gate")

        if ok and min_combined > 0:
            with contextlib.suppress(Exception):
                if float(st_dict.get("combined_score") or 0.0) < min_combined:
                    return False, f"combined<{min_combined:.1f}"

        return ok, reason
    except Exception as e:
        if fail_closed:
            return False, f"scoring_err:{_short_exc(e)}"
        return True, f"scoring_err_allowed:{_short_exc(e)}"


async def _ai_gate(M: Modules, ui: Dict[str, Any]) -> Tuple[bool, float, str]:
    if (not M.ai_pass_gate) or _envb("AI_GATE_DISABLED", False):
        return True, 0.0, "ai_gate_off"
    try:
        if M.ai_candidate_cls:
            cand_kwargs = _filter_kwargs_ctor(M.ai_candidate_cls, dict(ui))
            c = M.ai_candidate_cls(**cand_kwargs)  # type: ignore[misc]
            out = M.ai_pass_gate(c)  # type: ignore[misc]
        else:
            out = M.ai_pass_gate(ui)  # type: ignore[misc]

        if inspect.isawaitable(out):
            out = await out

        ok, score, why = out  # type: ignore[misc]
        return bool(ok), float(score), str(why)
    except Exception as e:
        fail_closed = _envb("AI_GATE_FAIL_CLOSED", False)
        if fail_closed:
            return False, 0.0, f"ai_gate_error:{_short_exc(e)}"
        return True, 0.0, f"ai_gate_error_allowed:{_short_exc(e)}"


async def _routeguard_gate(M: Modules, ui: Dict[str, Any]) -> Tuple[bool, str]:
    if not M.routeguard_mod or _envb("ROUTEGUARD_DISABLED", False):
        return True, "routeguard_off"

    rg = M.routeguard_mod
    fail_closed = _envb("ROUTEGUARD_FAIL_CLOSED", False)

    async def _maybe_await(x: Any) -> Any:
        if inspect.isawaitable(x):
            return await x
        return x

    for nm in ("guard_candidate", "guard", "check_candidate"):
        fn = getattr(rg, nm, None)
        if callable(fn):
            try:
                out = await _maybe_await(fn(ui))
                if isinstance(out, tuple) and len(out) >= 1:
                    ok = bool(out[0])
                    reason = str(out[1]) if len(out) > 1 else nm
                    return ok, reason
                if isinstance(out, dict):
                    ok = bool(out.get("ok", True))
                    reason = str(out.get("reason") or nm)
                    return ok, reason
            except Exception as e:
                return (not fail_closed), f"routeguard_err:{_short_exc(e)}"

    for nm in ("allow_candidate", "should_allow", "allow"):
        fn = getattr(rg, nm, None)
        if callable(fn):
            try:
                out = await _maybe_await(fn(ui))
                return bool(out), nm
            except Exception as e:
                return (not fail_closed), f"routeguard_err:{_short_exc(e)}"

    return True, "routeguard_no_api"


async def _risk_gate(M: Modules, ui: Dict[str, Any]) -> Tuple[bool, str]:
    if not M.risk_mod or _envb("RISK_MANAGER_DISABLED", False):
        return True, "risk_off"

    rm = M.risk_mod
    fail_closed = _envb("RISK_FAIL_CLOSED", False)

    async def _maybe_await(x: Any) -> Any:
        if inspect.isawaitable(x):
            return await x
        return x

    for nm in ("check_candidate", "check_buy", "check_entry", "guard_candidate", "guard_buy", "allow_buy", "allow_entry"):
        fn = getattr(rm, nm, None)
        if callable(fn):
            try:
                kw = _filter_kwargs(fn, {"ui": ui, "candidate": ui, **ui})
                out = await _maybe_await(fn(**kw) if kw else fn(ui))
                if isinstance(out, tuple) and len(out) >= 1:
                    ok = bool(out[0])
                    reason = str(out[1]) if len(out) > 1 else nm
                    return ok, reason
                if isinstance(out, dict):
                    ok = bool(out.get("ok", True))
                    reason = str(out.get("reason") or nm)
                    return ok, reason
                if isinstance(out, bool):
                    return out, nm
            except Exception as e:
                return (not fail_closed), f"risk_err:{_short_exc(e)}"

    for nm in ("can_trade", "can_buy", "should_trade", "should_buy"):
        fn = getattr(rm, nm, None)
        if callable(fn):
            try:
                out = await _maybe_await(fn())
                return bool(out), nm
            except Exception as e:
                return (not fail_closed), f"risk_err:{_short_exc(e)}"

    return True, "risk_no_api"


# =============================================================================
# Exit engine wiring
# =============================================================================
def _exit_allow_events_when_sells_disabled() -> bool:
    return _envb("EXIT_ENGINE_ALLOW_EVENTS_WHEN_SELLS_DISABLED", True)


def _reload_exit_engine_config_best_effort(M: Modules) -> None:
    if not M.exit_mod:
        return
    fn = getattr(M.exit_mod, "reload_exit_engine_config", None)
    if callable(fn):
        with contextlib.suppress(Exception):
            fn()


async def _exit_on_buy_filled(M: Modules, payload: Dict[str, Any]) -> None:
    if not M.exit_mod:
        return
    if _sells_disabled() and not _exit_allow_events_when_sells_disabled():
        return
    fn = getattr(M.exit_mod, "on_buy_filled", None) or getattr(M.exit_mod, "on_buy_filled_event", None)
    if not callable(fn):
        return
    with contextlib.suppress(Exception):
        await _call_maybe_async(fn, payload)


async def _exit_on_tick(M: Modules, payload: Dict[str, Any]) -> None:
    if not M.exit_mod:
        return
    if _sells_disabled() and not _exit_allow_events_when_sells_disabled():
        return
    fn = getattr(M.exit_mod, "on_trade_tick", None)
    if not callable(fn):
        return
    try:
        await _call_maybe_async(fn, payload)
    except Exception as e:
        log.every("EXIT_ON_TICK_FAIL", 10, err=_short_exc(e))


# =============================================================================
# Long-running worker runner (best-effort)
# =============================================================================
async def _run_module_worker(mod: Any, stop_evt: asyncio.Event, label: str) -> None:
    candidates = (
        "run_worker",
        "exit_engine_loop",
        "reinvestor_loop",
        "smart_wallets_loop",
        "worker_loop",
        "loop",
        "run",
        "main",
    )
    for name in candidates:
        fn = getattr(mod, name, None)
        if not callable(fn):
            continue
        try:
            kwargs = _filter_kwargs(fn, {"stop_evt": stop_evt, "stop_event": stop_evt, "stop": stop_evt})
            if kwargs:
                return await _call_possibly_blocking(fn, **kwargs)
            with contextlib.suppress(TypeError):
                return await _call_possibly_blocking(fn, stop_evt)
            return await _call_possibly_blocking(fn)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.event(f"{label}_RUNNER_FAIL", fn=name, err=_short_exc(e))
            raise

    log.event(f"{label}_NO_RUNNER")
    if _fast_fail():
        raise RuntimeError(f"{label} worker has no compatible runner")


async def _run_loop_until_stop(loop_fn: Callable[..., Any], stop_evt: asyncio.Event, *, name: str) -> None:
    async def _start_inner() -> Any:
        try:
            kw = _filter_kwargs(loop_fn, {"stop_evt": stop_evt, "stop_event": stop_evt, "stop": stop_evt})
            if kw:
                return await _call_maybe_async(loop_fn, **kw)
        except TypeError:
            pass

        with contextlib.suppress(TypeError):
            return await _call_maybe_async(loop_fn, stop_evt)

        return await _call_maybe_async(loop_fn)

    inner = asyncio.create_task(_start_inner(), name=f"{name}::inner")
    try:
        await stop_evt.wait()
    finally:
        inner.cancel()
        with contextlib.suppress(Exception):
            await asyncio.gather(inner, return_exceptions=True)


# =============================================================================
# Tick router + sinks
# =============================================================================
async def _tick_router(M: Modules, tick_q: "asyncio.Queue[Dict[str, Any]]", stop_evt: asyncio.Event, st: RuntimeState) -> None:
    while not stop_evt.is_set():
        try:
            d = await asyncio.wait_for(tick_q.get(), timeout=0.5)
        except asyncio.TimeoutError:
            continue
        except asyncio.CancelledError:
            raise
        except Exception:
            continue

        try:
            st.tick_seen += 1
            st.last_tick_at = _now()
            with contextlib.suppress(Exception):
                ts = _safe_float(d.get("ts") or d.get("timestamp") or 0.0, 0.0)
                if ts > 0:
                    st.tick_lat_ms_ema = _ema(st.tick_lat_ms_ema, max(0.0, (_now() - ts) * 1000.0))
            await _exit_on_tick(M, d)
        finally:
            with contextlib.suppress(Exception):
                tick_q.task_done()


async def _tick_sink(tick_q: "asyncio.Queue[Dict[str, Any]]", stop_evt: asyncio.Event, st: RuntimeState) -> None:
    while not stop_evt.is_set():
        try:
            _ = await asyncio.wait_for(tick_q.get(), timeout=0.5)
        except asyncio.TimeoutError:
            continue
        except asyncio.CancelledError:
            raise
        except Exception:
            continue
        try:
            st.tick_sunk += 1
            st.last_tick_at = _now()
        finally:
            with contextlib.suppress(Exception):
                tick_q.task_done()


async def _candidate_sink(in_q: "asyncio.Queue[CandidateMsg]", stop_evt: asyncio.Event, st: RuntimeState) -> None:
    while not stop_evt.is_set():
        try:
            msg = await asyncio.wait_for(in_q.get(), timeout=0.5)
        except asyncio.TimeoutError:
            continue
        except asyncio.CancelledError:
            raise
        except Exception:
            continue
        try:
            st.cand_sunk += 1
            st.last_candidate_at = _now()
            with contextlib.suppress(Exception):
                st.cand_lat_ms_ema = _ema(st.cand_lat_ms_ema, max(0.0, (_now() - float(msg.ts or _now())) * 1000.0))
        finally:
            with contextlib.suppress(Exception):
                in_q.task_done()


# =============================================================================
# Entry worker
# =============================================================================
_BUY_INFLIGHT: Dict[str, float] = {}
_BUY_INFLIGHT_TTL = _envf("BUY_INFLIGHT_TTL_SEC", 25.0)

_BUY_COOLDOWN_SEC = _envf("BUY_MINT_COOLDOWN_SEC", 0.0)
_LAST_BUY_ATTEMPT: Dict[str, float] = {}

_BUY_EXEC_TIMEOUT_S = float(_envf("BUY_EXEC_TIMEOUT_S", 35.0))
_BUY_WARMUP_SEC = float(_envf("BUY_WARMUP_SEC", 0.0))


def _buy_inflight_ok(mint: str) -> bool:
    now = _now()
    cutoff = now - float(_BUY_INFLIGHT_TTL)
    for k, ts in list(_BUY_INFLIGHT.items()):
        if ts < cutoff:
            _BUY_INFLIGHT.pop(k, None)
    if mint in _BUY_INFLIGHT:
        return False
    _BUY_INFLIGHT[mint] = now
    return True


def _buy_cooldown_ok(mint: str) -> bool:
    if _BUY_COOLDOWN_SEC <= 0:
        return True
    now = _now()
    last = float(_LAST_BUY_ATTEMPT.get(mint, 0.0))
    if last and (now - last) < float(_BUY_COOLDOWN_SEC):
        return False
    _LAST_BUY_ATTEMPT[mint] = now
    return True


def _optional_log_candidate(M: Modules, ui: Dict[str, Any]) -> None:
    if not M.data_logger_mod:
        return
    if not _envb("LOG_CANDIDATES_ENABLED", False):
        return
    fn = getattr(M.data_logger_mod, "log_candidate_event", None)
    if not callable(fn):
        return
    try:
        out = fn(ui)
        if inspect.isawaitable(out):
            asyncio.create_task(out)
    except Exception:
        return


async def _entry_worker(
    M: Modules,
    wid: int,
    in_q: "asyncio.Queue[CandidateMsg]",
    stop_evt: asyncio.Event,
    st: RuntimeState,
    buy_sema: asyncio.Semaphore,
) -> None:
    if M.maybe_buy is None:
        log.event("ENTRY_DISABLED_NO_BUY_EXECUTOR", run_id=st.run_id)
        if _fast_fail():
            raise RuntimeError("No maybe_buy executor available")
        await stop_evt.wait()
        return

    dryrun = _envb("DRYRUN", _envb("DRY_RUN", False))
    started_at = _now()

    async def _on_submitted(p: Dict[str, Any]) -> None:
        bump("buy_submitted")
        st.last_buy_action_at = _now()
        log.event("BUY_SUBMITTED", run_id=st.run_id, worker=wid, **(p or {}))

    async def _on_filled(p: Dict[str, Any]) -> None:
        bump("buy_filled")
        st.last_buy_action_at = _now()
        st.last_buy_fill_at = _now()
        log.event("BUY_FILLED", run_id=st.run_id, worker=wid, **(p or {}))
        await _exit_on_buy_filled(M, p or {})

    async def _on_failed(p: Dict[str, Any]) -> None:
        bump("buy_failed")
        st.last_buy_action_at = _now()
        log.event("BUY_FAILED", run_id=st.run_id, worker=wid, **(p or {}))

    while not stop_evt.is_set():
        try:
            msg = await asyncio.wait_for(in_q.get(), timeout=0.5)
        except asyncio.TimeoutError:
            continue
        except asyncio.CancelledError:
            raise
        except Exception:
            continue

        mint = ""
        try:
            st.cand_seen += 1
            st.last_candidate_at = _now()

            mint = M.normalize_mint(msg.mint)
            ui = dict(msg.ui or {})
            ui.setdefault("pool", ui.get("pool") or _source_to_pool_hint(msg.source))
            ui.setdefault("source", msg.source)
            ui.setdefault("mint", mint)
            ui.setdefault("ts", float(msg.ts or _now()))
            ui.setdefault("trace_id", msg.trace_id or _trace_id_for(mint, msg.source, ui, float(ui["ts"]), st.run_id))
            ui.setdefault("run_id", st.run_id)

            with contextlib.suppress(Exception):
                st.cand_lat_ms_ema = _ema(st.cand_lat_ms_ema, max(0.0, (_now() - float(msg.ts or _now())) * 1000.0))

            if not mint:
                st.buy_skip += 1
                continue

            _optional_log_candidate(M, ui)

            if _BUY_WARMUP_SEC > 0 and (_now() - started_at) < _BUY_WARMUP_SEC:
                bump("buy_skip_warmup")
                st.buy_skip += 1
                log.every("BUY_SKIP_WARMUP", 10, mint=mint, warmup_sec=_BUY_WARMUP_SEC)
                continue

            if _buys_disabled():
                bump("buy_skip_disabled")
                st.buy_skip += 1
                log.every("BUY_SKIP_DISABLED", 15, mint=mint, pool=ui.get("pool"), source=msg.source)
                continue

            # Optional global guardrails hook (if present)
            if M.buys_allowed is not None:
                with contextlib.suppress(Exception):
                    ok_guard = bool(M.buys_allowed())
                    if not ok_guard:
                        bump("buy_skip_guardrails")
                        st.buy_skip += 1
                        log.every("BUY_SKIP_GUARDRAILS", 10, mint=mint)
                        continue

            if not _buy_inflight_ok(mint):
                bump("buy_skip_inflight")
                st.buy_skip += 1
                log.every("BUY_SKIP_INFLIGHT", 10, mint=mint, pool=ui.get("pool"), source=msg.source)
                continue

            if not _buy_cooldown_ok(mint):
                bump("buy_skip_cooldown")
                st.buy_skip += 1
                log.every("BUY_SKIP_COOLDOWN", 10, mint=mint, cooldown_sec=_BUY_COOLDOWN_SEC)
                with contextlib.suppress(Exception):
                    _BUY_INFLIGHT.pop(mint, None)
                continue

            ok_sc, why_sc = await _scoring_gate(M, ui)
            if not ok_sc:
                bump("buy_skip_scoring")
                st.buy_skip += 1
                log.event(
                    "BUY_SKIP_SCORING",
                    run_id=st.run_id,
                    mint=mint,
                    pool=ui.get("pool"),
                    why=why_sc,
                    score=ui.get("score"),
                )
                with contextlib.suppress(Exception):
                    _BUY_INFLIGHT.pop(mint, None)
                continue

            ok_risk, why_risk = await _risk_gate(M, ui)
            if not ok_risk:
                bump("buy_skip_risk")
                st.buy_skip += 1
                log.event(
                    "BUY_SKIP_RISK",
                    run_id=st.run_id,
                    mint=mint,
                    pool=ui.get("pool"),
                    why=why_risk,
                    score=ui.get("score"),
                )
                with contextlib.suppress(Exception):
                    _BUY_INFLIGHT.pop(mint, None)
                continue

            ok_rg, why_rg = await _routeguard_gate(M, ui)
            if not ok_rg:
                bump("buy_skip_routeguard")
                st.buy_skip += 1
                log.event(
                    "BUY_SKIP_ROUTEGUARD",
                    run_id=st.run_id,
                    mint=mint,
                    pool=ui.get("pool"),
                    why=why_rg,
                    score=ui.get("score"),
                )
                with contextlib.suppress(Exception):
                    _BUY_INFLIGHT.pop(mint, None)
                continue

            ok_ai, score_ai, why_ai = await _ai_gate(M, ui)
            if not ok_ai:
                bump("buy_skip_ai_gate")
                st.buy_skip += 1
                log.event(
                    "BUY_SKIP_AI_GATE",
                    run_id=st.run_id,
                    mint=mint,
                    pool=ui.get("pool"),
                    score=score_ai,
                    why=why_ai,
                    score_blob=ui.get("score"),
                )
                with contextlib.suppress(Exception):
                    _BUY_INFLIGHT.pop(mint, None)
                continue

            try:
                async with buy_sema:
                    fn = M.maybe_buy
                    kwargs = _filter_kwargs(
                        fn,
                        {
                            "mint": mint,
                            "ui": ui,
                            "on_buy_submitted": _on_submitted,
                            "on_buy_filled": _on_filled,
                            "on_buy_failed": _on_failed,
                            "worker_id": wid,
                            "source": msg.source,
                            "dryrun": dryrun,
                            "DRYRUN": dryrun,
                        },
                    )

                    if dryrun and ("dryrun" not in kwargs) and ("DRYRUN" not in kwargs):
                        st.buy_skip += 1
                        bump("buy_dryrun_skip_noarg")
                        log.every("DRYRUN_SKIP_NOARG", 10, mint=mint)
                        continue

                    ok = await asyncio.wait_for(_call_maybe_async(fn, **kwargs), timeout=_BUY_EXEC_TIMEOUT_S)

                    ok_bool = bool(ok)
                    if isinstance(ok, dict):
                        ok_bool = bool(ok.get("ok", ok.get("success", False)))

                    if ok_bool:
                        st.buy_ok += 1
                    else:
                        st.buy_fail += 1

            except asyncio.TimeoutError:
                st.buy_exc += 1
                st.buy_fail += 1
                bump("buy_exec_timeout")
                log.event("BUY_EXEC_TIMEOUT", run_id=st.run_id, mint=mint, timeout_s=_BUY_EXEC_TIMEOUT_S)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                st.buy_exc += 1
                st.buy_fail += 1
                bump("buy_exec_error")
                log.event(
                    "BUY_EXEC_EXCEPTION",
                    run_id=st.run_id,
                    mint=mint,
                    err=_short_exc(e),
                    tb=traceback.format_exc(limit=2),
                )

        finally:
            with contextlib.suppress(Exception):
                if mint:
                    _BUY_INFLIGHT.pop(mint, None)
            with contextlib.suppress(Exception):
                in_q.task_done()


# =============================================================================
# Supervisor + kill watchdog
# =============================================================================
@dataclass
class TaskSpec:
    name: str
    factory: Callable[[], Awaitable[None]]
    critical: bool = True
    restarts: int = 0
    last_start: float = 0.0
    backoff: float = 0.0
    restarts_window: List[float] = field(default_factory=list)


async def _supervisor_loop(
    M: Modules,
    specs: Dict[str, TaskSpec],
    tasks: Dict[str, asyncio.Task],
    stop_evt: asyncio.Event,
    st: RuntimeState,
    *,
    cand_q: asyncio.Queue,
    tick_q: asyncio.Queue,
) -> None:
    interval = max(1, int(getattr(C, "SUPERVISOR_SEC", 5) or 5))
    base_backoff = float(getattr(C, "RESTART_BACKOFF_SEC", 2) or 2)
    max_backoff = float(getattr(C, "RESTART_MAX_BACKOFF_SEC", 60) or 60)
    max_per_hour = int(getattr(C, "RESTART_MAX_PER_HOUR", 60) or 60)

    health_sec = int(getattr(C, "HEALTH_LOG_SEC", 30) or 30)
    starvation_warn = float(getattr(C, "STARVATION_WARN_SEC", 180) or 180)
    no_buy_warn = float(getattr(C, "NO_BUY_WARN_SEC", 300) or 300)

    hang_restart_ms = float(_envf("MAIN_HANG_RESTART_LAG_MS", 0.0))  # 0 disables
    last_loop_mono = _mono()

    while not stop_evt.is_set():
        await asyncio.sleep(interval)

        now_m = _mono()
        st.loop_lag_ms = max(0.0, (now_m - last_loop_mono - float(interval)) * 1000.0)
        last_loop_mono = now_m

        _queue_hwm_update(st, cand_q, tick_q)

        if hang_restart_ms and st.loop_lag_ms >= hang_restart_ms:
            log.event(
                "MAIN_LOOP_LAG_TOO_HIGH",
                run_id=st.run_id,
                lag_ms=int(st.loop_lag_ms),
                threshold_ms=int(hang_restart_ms),
            )
            stop_evt.set()

        now = _now()
        cutoff = now - 3600.0
        for spec in specs.values():
            spec.restarts_window = [t for t in spec.restarts_window if t >= cutoff]

        for name, t in list(tasks.items()):
            if not t.done():
                continue

            err: Optional[BaseException] = None
            with contextlib.suppress(Exception):
                err = t.exception()

            tasks.pop(name, None)
            spec = specs.get(name)
            log.event("TASK_DIED", run_id=st.run_id, task=name, err=("done" if err is None else _short_exc(err)))

            if stop_evt.is_set() or not spec or not spec.critical:
                continue

            if len(spec.restarts_window) >= max_per_hour:
                log.event("TASK_RESTART_RATE_LIMIT", run_id=st.run_id, task=name, per_hour=max_per_hour)
                continue

            spec.backoff = min(max_backoff, max(base_backoff, spec.backoff * 1.7 if spec.backoff else base_backoff))
            jitter = 0.0 if _JITTER_DISABLED else random.uniform(0.0, min(1.0, spec.backoff * 0.15))
            wait = spec.backoff + jitter
            log.event("TASK_RESTART_SCHEDULED", run_id=st.run_id, task=name, backoff_sec=round(wait, 2))

            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(stop_evt.wait(), timeout=wait)
                continue

            tasks[name] = asyncio.create_task(spec.factory(), name=name)
            spec.restarts += 1
            spec.last_start = _now()
            spec.restarts_window.append(spec.last_start)
            log.event("TASK_RESTARTED", run_id=st.run_id, task=name, restarts=spec.restarts)

        now = _now()
        if st.last_candidate_at > 0 and (now - st.last_candidate_at) > starvation_warn:
            log.every(
                "WATCHDOG_NO_CANDIDATES",
                30,
                run_id=st.run_id,
                sec_since_last=int(now - st.last_candidate_at),
                cand_q=cand_q.qsize(),
            )

        if st.last_buy_action_at > 0 and (now - st.last_buy_action_at) > no_buy_warn:
            log.every(
                "WATCHDOG_NO_BUY_ACTION",
                30,
                run_id=st.run_id,
                sec_since_last=int(now - st.last_buy_action_at),
                cand_q=cand_q.qsize(),
            )

        if (now - st.last_health_at) >= float(health_sec):
            st.last_health_at = now
            snap = _snapshot_state(M, st, cand_q=cand_q, tick_q=tick_q, tasks=tasks)
            log.event("HEALTH", **snap, queue_policy=_DROP_POLICY, loop_lag_ms=int(st.loop_lag_ms))


async def _kill_watchdog(stop_evt: asyncio.Event, st: RuntimeState) -> None:
    while not stop_evt.is_set():
        if _kill_active():
            st.last_kill_seen = _now()
            log.every("KILL_SWITCH_ACTIVE", 2, run_id=st.run_id, shutdown_on_trigger=int(_KILL_SHUTDOWN_ON_TRIGGER))
            if _KILL_SHUTDOWN_ON_TRIGGER:
                stop_evt.set()
                return
        await asyncio.sleep(_KILL_WATCHDOG_SEC)


def _install_signal_handlers(stop_evt: asyncio.Event, run_id: str) -> None:
    def _ask_stop(*_a: Any) -> None:
        if not stop_evt.is_set():
            log.event("STOP_REQUESTED", run_id=run_id)
            stop_evt.set()

    with contextlib.suppress(Exception):
        signal.signal(signal.SIGINT, lambda *_: _ask_stop())
    with contextlib.suppress(Exception):
        signal.signal(signal.SIGTERM, lambda *_: _ask_stop())

    with contextlib.suppress(Exception):
        loop = asyncio.get_running_loop()
        with contextlib.suppress(Exception):
            loop.add_signal_handler(signal.SIGINT, _ask_stop)  # type: ignore[arg-type]
        with contextlib.suppress(Exception):
            loop.add_signal_handler(signal.SIGTERM, _ask_stop)  # type: ignore[arg-type]


# =============================================================================
# Optional stdlib HTTP debug server (health.py is the real Render web service)
# =============================================================================
async def _http_server(
    M: Modules,
    st: RuntimeState,
    cand_q: asyncio.Queue,
    tick_q: asyncio.Queue,
    tasks: Dict[str, asyncio.Task],
    stop_evt: asyncio.Event,
) -> None:
    host = _envs("MAIN_HTTP_HOST", "0.0.0.0")
    port = _envi("MAIN_HTTP_PORT", 0)
    if port <= 0:
        return

    async def _handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            data = await asyncio.wait_for(reader.read(4096), timeout=2.0)
        except Exception:
            with contextlib.suppress(Exception):
                writer.close()
            return

        path = "/"
        try:
            line0 = (data.splitlines()[0] if data else b"").decode("utf-8", "ignore")
            parts = line0.split()
            if len(parts) >= 2:
                path = parts[1]
        except Exception:
            path = "/"

        if path in {"/health", "/healthz"}:
            body = _json_dumps({"ok": True, "run_id": st.run_id, "version": MAIN_VERSION})
            resp = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s" % (
                len(body),
                body,
            )
        elif path in {"/state", "/debug/state"}:
            body = _json_dumps(_snapshot_state(M, st, cand_q=cand_q, tick_q=tick_q, tasks=tasks))
            resp = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s" % (
                len(body),
                body,
            )
        elif path in {"/metrics"}:
            snap = _snapshot_state(M, st, cand_q=cand_q, tick_q=tick_q, tasks=None)
            lines = [
                "sniper_up 1",
                f"sniper_uptime_sec {snap.get('up_sec', 0)}",
                f"sniper_cand_seen {snap['cand']['seen']}",
                f"sniper_cand_dropped {snap['cand']['dropped']}",
                f"sniper_buy_ok {snap['buy']['ok']}",
                f"sniper_buy_fail {snap['buy']['fail']}",
                f"sniper_buy_skip {snap['buy']['skip']}",
                f"sniper_tick_seen {snap['tick']['seen']}",
            ]
            body = "\n".join(lines) + "\n"
            resp = "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: %d\r\n\r\n%s" % (
                len(body),
                body,
            )
        else:
            body = "ok\n"
            resp = "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: %d\r\n\r\n%s" % (
                len(body),
                body,
            )

        try:
            writer.write(resp.encode("utf-8", "ignore"))
            await writer.drain()
        finally:
            with contextlib.suppress(Exception):
                writer.close()

    server = await asyncio.start_server(_handle, host=host, port=port)
    log.event("HTTP_SERVER_STARTED", run_id=st.run_id, host=host, port=port)

    try:
        await stop_evt.wait()
    finally:
        server.close()
        with contextlib.suppress(Exception):
            await server.wait_closed()
        log.event("HTTP_SERVER_STOPPED", run_id=st.run_id)


# =============================================================================
# Scanner module selection
# =============================================================================
def _scanner_modules_from_env() -> Optional[List[str]]:
    raw = _envs("SCANNER_MODULES", "").strip()
    if not raw:
        return None
    parts = [p.strip() for p in raw.replace(";", ",").split(",") if p.strip()]
    return parts or None


def _pick_first_existing(cands: List[str]) -> Optional[str]:
    for m in cands:
        if _module_exists(m):
            return m
    return None


def _default_scanner_modules() -> List[str]:
    mods: List[str] = []

    if getattr(C, "ENABLE_RAYDIUM_SCANNER", True) and _envb("ENABLE_RAYDIUM_SCANNER", True):
        ray = _pick_first_existing(["scanner.raydium_scanner", "scanner.raydium", "scanner.raydium_scanner_v2"])
        mods.append(ray or "scanner.raydium_scanner")

    if getattr(C, "ENABLE_PUMP_SCANNER", True) and _envb("ENABLE_PUMP_SCANNER", True):
        pump = _pick_first_existing(["scanner.pumpfun_scanner", "scanner.pumpfun", "scanner.pump_scanner"])
        mods.append(pump or "scanner.pumpfun_scanner")

    if getattr(C, "ENABLE_DEXSCREENER_SCANNER", True) and _envb("ENABLE_DEXSCREENER_SCANNER", True):
        dex = _pick_first_existing(["scanner.dexscreener_scanner", "scanner.dexscreener"])
        mods.append(dex or "scanner.dexscreener_scanner")

    return mods


# =============================================================================
# Main
# =============================================================================
async def main(stop_evt: Optional[asyncio.Event] = None) -> None:
    global RUN_ID

    # Fresh per-run identity (so run_forever restarts don't reuse the same RUN_ID)
    if not RUN_ID or _envb("MAIN_FORCE_NEW_RUN_ID", True):
        base = f"{time.time()}|{os.getpid()}|{socket.gethostname()}|{random.random()}"
        RUN_ID = (os.getenv("RUN_ID", "") or hashlib.sha1(base.encode("utf-8", "ignore")).hexdigest()[:10]).strip()

    # Deterministic-ish seed per run (unless jitter disabled)
    if not _JITTER_DISABLED:
        with contextlib.suppress(Exception):
            seed = int(hashlib.sha1(RUN_ID.encode("utf-8", "ignore")).hexdigest()[:8], 16)
            random.seed(seed)

    stop_evt = stop_evt or asyncio.Event()
    _install_signal_handlers(stop_evt, RUN_ID)
    with contextlib.suppress(Exception):
        _install_loop_exception_handler()

    # IMPORTANT: apply sells-disable env BEFORE importing exit modules
    _apply_disable_sells_to_env()

    if _PANIC_STOP:
        log.event("PANIC_STOP_SET", run_id=RUN_ID)
        stop_evt.set()

    # Optional belt-and-suspenders preflight (subprocess)
    await _run_preflight_if_enabled(RUN_ID)

    # Bootstraps (best effort)
    with contextlib.suppress(Exception):
        await _core_bootstrap_best_effort(RUN_ID)
    with contextlib.suppress(Exception):
        await _ai_db_bootstrap_best_effort(RUN_ID)

    # Resolve modules lazily (after env overrides + bootstrap)
    M = _resolve_modules()

    if _sells_disabled():
        _reload_exit_engine_config_best_effort(M)

    phase = _phase()
    disable_sells = _sells_disabled()

    max_concurrent_buys = max(1, _envi("MAX_CONCURRENT_BUYS", _envi("MAX_CONCURRENCY", 3)))
    buy_sema = asyncio.Semaphore(max_concurrent_buys)

    log.event(
        "MAIN_BOOT",
        run_id=RUN_ID,
        version=MAIN_VERSION,
        build=MAIN_BUILD,
        phase=phase,
        py=sys.version.split()[0],
        pid=os.getpid(),
        buy_executor=int(M.maybe_buy is not None),
        ai_gate=int(M.ai_pass_gate is not None),
        scoring_service=int(M.scoring_fn is not None and M.scoring_enabled),
        exit_engine=int(M.exit_mod is not None),
        auto_exit_worker=int(M.auto_exit_worker_loop is not None),
        reinvestor=int(M.reinvest_mod is not None),
        smart_wallets=int(M.smart_mod is not None),
        routeguard=int(M.routeguard_mod is not None),
        risk_manager=int(M.risk_mod is not None),
        queue_policy=_DROP_POLICY,
        max_concurrent_buys=max_concurrent_buys,
        buy_exec_timeout_s=_BUY_EXEC_TIMEOUT_S,
        kill_switch=int(_kill_active()),
        disable_buys=int(_buys_disabled()),
        disable_sells=int(disable_sells),
        disable_scanners=int(_scanners_disabled()),
        dryrun=int(_envb("DRYRUN", _envb("DRY_RUN", False))),
        threadify_sync_modules=int(_threadify_sync_modules_enabled()),
        fast_fail=int(_fast_fail()),
        jitter_disabled=int(_JITTER_DISABLED),
        max_uptime_sec=int(_MAIN_MAX_UPTIME_SEC),
    )

    with contextlib.suppress(Exception):
        _start_global_heartbeat()  # type: ignore[misc]

    if phase <= 0:
        log.event("MAIN_PHASE0_IDLE", run_id=RUN_ID)
        await stop_evt.wait()
        return

    qmax = _envi("CANDIDATE_QUEUE_MAX", int(getattr(C, "CANDIDATE_QUEUE_MAX", 2500) or 2500))
    tqmax = _envi("TICK_QUEUE_MAX", int(getattr(C, "TICK_QUEUE_MAX", 5000) or 5000))
    qmax = max(50, min(250_000, qmax))
    tqmax = max(50, min(500_000, tqmax))

    cand_q: "asyncio.Queue[CandidateMsg]" = asyncio.Queue(maxsize=qmax)
    tick_q: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue(maxsize=tqmax)
    st = RuntimeState(run_id=RUN_ID)

    tasks: Dict[str, asyncio.Task] = {}
    specs: Dict[str, TaskSpec] = {}

    def _spawn(name: str, coro_factory: Callable[[], Awaitable[None]], critical: bool = True) -> None:
        if name in tasks and not tasks[name].done():
            return
        specs[name] = TaskSpec(name=name, factory=coro_factory, critical=critical, last_start=_now())
        tasks[name] = asyncio.create_task(coro_factory(), name=name)

    # Optional debug HTTP server (NOT the Render web service; see health.py)
    _spawn("http_server", lambda: _http_server(M, st, cand_q, tick_q, tasks, stop_evt), critical=False)

    # Phase 1: scanners
    if phase >= 1:
        mods = _scanner_modules_from_env() or _default_scanner_modules()
        log.event("SCANNERS_SELECTED", run_id=RUN_ID, modules=",".join(mods) if mods else "")
        for mod_name in (mods or []):
            crit = _envb(f"SCANNER_CRITICAL__{mod_name.replace('.', '_').upper()}", True)
            _spawn(
                f"scanner::{mod_name}",
                lambda m=mod_name: _scanner_run_module(M, m, cand_q, tick_q, stop_evt, st),
                critical=crit,
            )

    # Phase 2: entry workers
    entry_workers_started = 0
    if phase >= 2:
        nworkers = int(getattr(C, "ENTRY_WORKERS", 2) or 2)
        nworkers = max(1, min(32, _envi("ENTRY_WORKERS", nworkers)))
        if M.maybe_buy is not None:
            for wid in range(nworkers):
                _spawn(
                    f"entry_worker_{wid}",
                    lambda wid=wid: _entry_worker(M, wid, cand_q, stop_evt, st, buy_sema),
                    critical=True,
                )
            entry_workers_started = nworkers
            log.event("ENTRY_WORKERS_STARTED", run_id=RUN_ID, n=nworkers)
        else:
            log.event("ENTRY_WORKERS_SKIPPED_NO_BUY_EXECUTOR", run_id=RUN_ID, requested=nworkers)
            if _fast_fail():
                raise RuntimeError("No buy executor; cannot run entry workers")

    # If scanners running but no entry workers, drain to sink (prevents memory growth)
    if phase >= 1 and entry_workers_started <= 0:
        _spawn("candidate_sink", lambda: _candidate_sink(cand_q, stop_evt, st), critical=False)

    # Phase 3: tick routing + exit modules + AUTO EXIT QUEUE WORKER
    if phase >= 3:
        _spawn("tick_router", lambda: _tick_router(M, tick_q, stop_evt, st), critical=True)

        if M.exit_mod is not None and _envb("EXIT_ENGINE_WORKER_ENABLED", True):
            _spawn("exit_engine", lambda: _run_module_worker(M.exit_mod, stop_evt, "EXIT"), critical=False)

        if (not disable_sells) and _envb("AUTO_EXIT_WORKER_ENABLED", True) and M.auto_exit_worker_loop is not None:
            _spawn(
                "auto_exit_worker",
                lambda: _run_loop_until_stop(M.auto_exit_worker_loop, stop_evt, name="auto_exit_worker"),
                critical=False,
            )
        elif disable_sells:
            log.event("AUTO_EXIT_WORKER_SKIPPED_DISABLE_SELLS", run_id=RUN_ID)
        else:
            log.event(
                "AUTO_EXIT_WORKER_NOT_AVAILABLE",
                run_id=RUN_ID,
                enabled=int(_envb("AUTO_EXIT_WORKER_ENABLED", True)),
            )

    else:
        if _envb("FORWARD_TICKS_FROM_SCANNERS", False):
            _spawn("tick_sink", lambda: _tick_sink(tick_q, stop_evt, st), critical=False)

    # Phase 4: reinvestor
    if phase >= 4 and M.reinvest_mod is not None and _envb("REINVESTOR_ENABLED", True):
        _spawn("reinvestor", lambda: _run_module_worker(M.reinvest_mod, stop_evt, "REINVEST"), critical=False)

    # Phase 5: smart wallets
    if phase >= 5 and M.smart_mod is not None and _envb("SMART_WALLETS_ENABLED", True):
        _spawn("smart_wallets", lambda: _run_module_worker(M.smart_mod, stop_evt, "SMARTWALLETS"), critical=False)

    # Phase 6+: supervisor + kill watchdog
    supervisor_needed = (phase >= 6) or _envb("SUPERVISOR_ALWAYS", False)
    if supervisor_needed:
        _spawn(
            "supervisor",
            lambda: _supervisor_loop(M, specs, tasks, stop_evt, st, cand_q=cand_q, tick_q=tick_q),
            critical=True,
        )
        _spawn("kill_watchdog", lambda: _kill_watchdog(stop_evt, st), critical=True)

    log.event("MAIN_STARTED", run_id=RUN_ID, phase=phase, tasks=len(tasks), cand_q_max=qmax, tick_q_max=tqmax)

    idle_sleep = float(getattr(C, "MAIN_IDLE_SLEEP_SEC", 0.5) or 0.5)
    shutdown_grace = float(_envf("SHUTDOWN_GRACE_SEC", float(getattr(C, "SHUTDOWN_GRACE_SEC", 15.0) or 15.0)))

    try:
        while not stop_evt.is_set():
            if _MAIN_MAX_UPTIME_SEC and (_now() - st.started_at) >= _MAIN_MAX_UPTIME_SEC:
                log.event("MAIN_MAX_UPTIME_REACHED", run_id=RUN_ID, max_uptime_sec=int(_MAIN_MAX_UPTIME_SEC))
                stop_evt.set()
                break
            await asyncio.sleep(idle_sleep)
    finally:
        log.event("MAIN_SHUTDOWN_BEGIN", run_id=RUN_ID)
        stop_evt.set()
        await _cancel_and_gather(tasks, timeout=shutdown_grace)
        log.event("MAIN_SHUTDOWN_DONE", run_id=RUN_ID)


# =============================================================================
# Compatibility aliases
# =============================================================================
async def run(stop_evt: Optional[asyncio.Event] = None) -> None:
    await main(stop_evt)


async def loop(stop_evt: Optional[asyncio.Event] = None) -> None:
    await main(stop_evt)


# =============================================================================
# Crash-resilient runner (Phase 7)
# =============================================================================
async def run_forever() -> None:
    restart_default = _phase() >= 7
    restart = _envb("MAIN_RESTART_ON_CRASH", restart_default)
    if not restart:
        await main()
        return

    backoff = float(_envf("MAIN_CRASH_BACKOFF_SEC", 2.0))
    backoff_max = float(_envf("MAIN_CRASH_BACKOFF_MAX_SEC", 60.0))

    while True:
        try:
            await main()
            return
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.event("MAIN_CRASH", run_id=RUN_ID or "unknown", err=_short_exc(e, 450))
            await asyncio.sleep(backoff)
            backoff = min(backoff_max, max(1.0, backoff * 1.7))


if __name__ == "__main__":  # pragma: no cover
    # Windows-friendly: Proactor is REQUIRED for asyncio subprocess APIs (preflight runner).
    with contextlib.suppress(Exception):
        if os.name == "nt" and hasattr(asyncio, "WindowsProactorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())  # type: ignore[attr-defined]
    asyncio.run(run_forever())
