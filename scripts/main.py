# scripts/main.py
from __future__ import annotations

"""
scripts/main.py

God-Tier Orchestrator (Phase 0–7 LOCK-IN) — PRODUCTION (V3+++)
=============================================================

CONTROL PLANE ONLY.
- Starts scanners -> normalizes payloads into CandidateMsg
- Entry workers -> run gating chain -> call maybe_buy()
- Tick router -> forwards ticks to exit_engine.on_trade_tick()
- Buy fill wiring -> forwards fill to exit_engine.on_buy_filled()
- Optional: auto-exit worker, reinvestor, smart_wallets
- Supervisor + watchdogs + health snapshots

Hard rules:
- NO swap/build/sign tx code here.
- SAFE IMPORTS: module scope is stdlib only. (critical for unit tests)
- Optional modules may be missing; orchestrator must not crash unless MAIN_FAST_FAIL=1.
- Respect kill/disable flags (env + file flags). Never log secrets.

Entry-point:
    python -m scripts.main

Back-compat:
- Exports `Strategy` (some harnesses expect scripts.main.Strategy)
- Exports handler aliases on Strategy
- Exports module-level helper aliases (tests expect these to exist)

Important runtime detail:
- If MAIN_HTTP_EARLY_START=1, the HTTP server is started *before* preflight/bootstrap
  to satisfy Render health checks. This file guarantees the HTTP server references the
  SAME queues/state used by the supervisor runtime (no double-init).

Run mode (optional, canonical):
- RUN_MODE=safe   -> SAFE_MODE=1, DRYRUN=1, DISABLE_BUYS=1, DISABLE_SELLS=1 (scanners optional)
- RUN_MODE=paper  -> DRYRUN=1 (buys allowed if executor supports dryrun)
- RUN_MODE=live   -> DRYRUN=0 (normal behavior)

Defaults are unchanged unless RUN_MODE is set.
"""

__all__ = [
    "Strategy",
    "StrategyHandler",
    "Handler",
    "main",
    "run",
    "loop",
    "run_forever",
    "build_app",
    "build_runtime",
    "build_system",
    "build_services",
    "contracts_surface",
    "get_contracts_surface",
    "ensure_event",
    "install_stdlib_event",
    "get_run_id",
    "set_run_id",
    "get_version",
    "get_build",
    "phase",
    "is_render",
    "envs",
    "envb",
    "envi",
    "envf",
    "now",
    "mono",
    "json_dumps",
    "short_exc",
    "module_exists",
    "soft_import",
    "ensure_windows_event_loop_policy",
    "run_main",
    "run_loop",
    "start",
    "start_main",
    "entrypoint",
    "refresh_runtime_constants",
]

# =============================================================================
# stdlib only at module scope
# =============================================================================
import asyncio
import contextlib
import hashlib
import importlib
import importlib.util
import inspect
import json
import logging
import os
import random
import signal
import socket
import sys
import time
import traceback
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

JSONDict = dict[str, Any]

# =============================================================================
# Identity / constants
# =============================================================================
MAIN_VERSION = "v3+++_orchestrator_lockin"
MAIN_BUILD = (
    os.getenv("GIT_COMMIT", "")[:12] or os.getenv("RENDER_GIT_COMMIT", "")[:12] or ""
).strip()
RUN_ID = ""

_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}


def _is_true(v: str) -> bool:
    return (v or "").strip().lower() in _TRUE


def _is_false(v: str) -> bool:
    return (v or "").strip().lower() in _FALSE


def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    return (v if v is not None else d).strip()


def _envb(k: str, d: bool = False) -> bool:
    v = _envs(k, "")
    if v == "":
        return bool(d)
    if _is_true(v):
        return True
    if _is_false(v):
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


def _now() -> float:
    return time.time()


def _mono() -> float:
    return time.monotonic()


def _safe_float(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(d)


def _short_exc(e: BaseException, n: int = 420) -> str:
    s = f"{type(e).__name__}: {e}"
    return (s[:n] + "…") if len(s) > n else s


def _json_dumps(x: Any, max_len: int = 250_000) -> str:
    try:
        s = json.dumps(x, ensure_ascii=False, separators=(",", ":"), default=str)
        return (s[:max_len] + "…") if len(s) > max_len else s
    except Exception:
        return "{}"


def _hash_prefix(seed: str, n: int = 12) -> str:
    nn = max(4, int(n))
    digest_size = max(1, min(32, (nn + 1) // 2))
    h = hashlib.blake2s(seed.encode("utf-8", "ignore"), digest_size=digest_size)
    return h.hexdigest()[:nn]


def _ema(prev: float, x: float, alpha: float = 0.12) -> float:
    if prev <= 0:
        return x
    return (1.0 - alpha) * prev + alpha * x


def _is_render() -> bool:
    return bool(
        os.getenv("RENDER_SERVICE_ID")
        or os.getenv("RENDER_INSTANCE_ID")
        or os.getenv("RENDER_EXTERNAL_URL")
        or _is_true(_envs("RENDER", ""))
    )


def _in_tests() -> bool:
    # pytest sets this env var for each test
    return bool(os.getenv("PYTEST_CURRENT_TEST"))


def _fast_fail() -> bool:
    return _envb("MAIN_FAST_FAIL", False)


def _threadify_sync_modules_enabled() -> bool:
    return _envb("MAIN_THREADIFY_SYNC_MODULES", False)


def _jitter_disabled() -> bool:
    return _envb("MAIN_DISABLE_RANDOM_JITTER", False)


# =============================================================================
# Canonical run-mode (optional; defaults unchanged unless RUN_MODE set)
# =============================================================================
def _run_mode() -> str:
    raw = (_envs("RUN_MODE", "") or _envs("TRADE_MODE", "") or _envs("MODE", "")).strip().lower()
    return raw if raw in {"safe", "paper", "live"} else ""


def _apply_run_mode_defaults() -> str:
    """
    If RUN_MODE is set, enforce minimal, deterministic env defaults.
    If RUN_MODE is not set, DO NOT change behavior.
    """
    mode = _run_mode()
    if not mode:
        return ""

    if mode == "safe":
        os.environ.setdefault("SAFE_MODE", "1")
        os.environ.setdefault("DRYRUN", "1")
        os.environ.setdefault("DRY_RUN", "1")
        os.environ.setdefault("DISABLE_BUYS", "1")
        os.environ.setdefault("DISABLE_SELLS", "1")
        # scanners optional in safe mode; default OFF unless explicitly enabled
        if os.getenv("MAIN_SAFE_MODE_ENABLE_SCANNERS") is None:
            os.environ.setdefault("DISABLE_SCANNERS", "1")
        return "safe"

    if mode == "paper":
        os.environ.setdefault("DRYRUN", "1")
        os.environ.setdefault("DRY_RUN", "1")
        return "paper"

    if mode == "live":
        if os.getenv("DRYRUN") is None and os.getenv("DRY_RUN") is None:
            os.environ.setdefault("DRYRUN", "0")
        return "live"

    return ""


# =============================================================================
# Logging shims (stdlib-only)
# =============================================================================
_REDACT_KEYS = (
    "token",
    "secret",
    "api_key",
    "apikey",
    "private",
    "seed",
    "mnemonic",
    "pass",
    "password",
    "auth",
    "bearer",
    "signature",
)


def _redact_fields(fields: dict[str, Any]) -> dict[str, Any]:
    if not fields:
        return fields
    out: dict[str, Any] = {}
    for k, v in fields.items():
        lk = str(k).lower()
        if any(x in lk for x in _REDACT_KEYS):
            out[k] = "***REDACTED***"
        else:
            out[k] = v
    return out


def install_stdlib_event(force: bool | None = None) -> None:
    """
    Install safe Logger.event / Logger.every / Logger.once shims.

    We only skip installing if we detect our own shim already present.
    """
    force = _envb("MAIN_FORCE_LOG_SHIMS", False) if force is None else bool(force)

    def _is_ours(fn: Any) -> bool:
        return bool(getattr(fn, "_pf_shim", False))

    ev0 = getattr(logging.Logger, "event", None)
    ev1 = getattr(logging.Logger, "every", None)
    ev2 = getattr(logging.Logger, "once", None)

    if not force and all(callable(x) and _is_ours(x) for x in (ev0, ev1, ev2)):
        return

    def _event(self: logging.Logger, name: str, **fields: Any) -> None:
        with contextlib.suppress(Exception):
            self.info("%s %s", name, _redact_fields(fields))

    def _every(
        self: logging.Logger,
        key: str,
        sec: int = 60,
        msg: str | None = None,
        level: int | None = None,
        **fields: Any,
    ) -> None:
        try:
            now_m = time.monotonic()
            store = getattr(self, "_pf_every_store", None)
            if not isinstance(store, dict):
                store = {}
            last = float(store.get(key, 0.0))
            if now_m - last >= float(sec):
                store[key] = now_m
                self._pf_every_store = store
                if level is not None:
                    self.log(int(level), "%s %s", (msg or key), _redact_fields(fields))
                else:
                    self.event(msg or key, **fields)  # type: ignore[attr-defined]
        except Exception:
            return

    def _once(
        self: logging.Logger,
        key: str,
        ttl: int = 60,
        msg: str | None = None,
        level: int | None = None,
        **fields: Any,
    ) -> None:
        return _every(self, key, sec=ttl, msg=msg, level=level, **fields)

    _event._pf_shim = True  # type: ignore[attr-defined]
    _every._pf_shim = True  # type: ignore[attr-defined]
    _once._pf_shim = True  # type: ignore[attr-defined]

    logging.Logger.event = _event  # type: ignore[assignment]
    logging.Logger.every = _every  # type: ignore[assignment]
    logging.Logger.once = _once  # type: ignore[assignment]


def ensure_event(logger: Any) -> Any:
    install_stdlib_event()
    if isinstance(logger, logging.Logger):
        return logger

    if not hasattr(logger, "event"):

        def _event(name: str, **fields: Any) -> None:
            with contextlib.suppress(Exception):
                logger.info("%s %s", name, _redact_fields(fields))

        logger.event = _event  # type: ignore[attr-defined]

    if not hasattr(logger, "every"):

        def _every(
            key: str,
            sec: int = 60,
            msg: str | None = None,
            level: int | None = None,
            **fields: Any,
        ) -> None:
            try:
                now_m = time.monotonic()
                store = getattr(_every, "_store", {})  # type: ignore[attr-defined]
                last = float(store.get(key, 0.0))
                if now_m - last >= float(sec):
                    store[key] = now_m
                    _every._store = store  # type: ignore[attr-defined]
                    with contextlib.suppress(Exception):
                        if level is not None:
                            logger.log(int(level), "%s %s", (msg or key), _redact_fields(fields))
                        else:
                            logger.event(msg or key, **fields)  # type: ignore[attr-defined]
            except Exception:
                return

        logger.every = _every  # type: ignore[attr-defined]

    if not hasattr(logger, "once"):

        def _once(
            key: str,
            ttl: int = 60,
            msg: str | None = None,
            level: int | None = None,
            **fields: Any,
        ) -> None:
            return logger.every(key, sec=ttl, msg=msg, level=level, **fields)  # type: ignore[attr-defined]

        logger.once = _once  # type: ignore[attr-defined]

    return logger


def _base_logger(name: str) -> logging.Logger:
    lg = logging.getLogger(name)
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s %(levelname)s:%(name)s:%(message)s"
        )
    return lg


log: Any = ensure_event(_base_logger("sniper.main"))


# =============================================================================
# Config (lazy)
# =============================================================================
class _CDefaults:
    ENABLE_RAYDIUM_SCANNER = True
    ENABLE_PUMP_SCANNER = True
    ENABLE_DEXSCREENER_SCANNER = True

    ENTRY_WORKERS = 2
    CANDIDATE_QUEUE_MAX = 2500
    TICK_QUEUE_MAX = 5000

    HEALTH_LOG_SEC = 30
    STARVATION_WARN_SEC = 180
    NO_BUY_WARN_SEC = 300

    MAIN_IDLE_SLEEP_SEC = 0.5
    SHUTDOWN_GRACE_SEC = 15.0

    MASTER_PHASE = int(os.getenv("MASTER_PHASE", "7"))


C: Any = _CDefaults()


def _load_config() -> Any:
    try:
        m = importlib.import_module("config")
        return getattr(m, "C", getattr(m, "SETTINGS", m))
    except Exception:
        return _CDefaults()


def _refresh_config() -> None:
    global C
    C = _load_config()


def _phase() -> int:
    p = _envi("MASTER_PHASE", int(getattr(C, "MASTER_PHASE", 7) or 7))
    if os.getenv("MASTER_PHASE") is None:
        p = _envi("STRATEGY_PHASE", p)
    return max(0, min(7, int(p)))


# =============================================================================
# Kill-switch + file flags
# =============================================================================
def _file_flag(path: str) -> bool:
    if not path:
        return False
    with contextlib.suppress(Exception):
        return os.path.exists(path)
    return False


def _kill_file() -> str:
    return _envs("KILL_SWITCH_FILE", "")


def _disable_buys_file() -> str:
    return _envs("DISABLE_BUYS_FILE", "")


def _disable_sells_file() -> str:
    return _envs("DISABLE_SELLS_FILE", "")


def _disable_scanners_file() -> str:
    return _envs("DISABLE_SCANNERS_FILE", "")


def _kill_active() -> bool:
    return _envb("KILL_SWITCH", False) or _file_flag(_kill_file())


def _buys_disabled() -> bool:
    return _envb("DISABLE_BUYS", False) or _kill_active() or _file_flag(_disable_buys_file())


def _sells_disabled() -> bool:
    return _envb("DISABLE_SELLS", False) or _kill_active() or _file_flag(_disable_sells_file())


def _scanners_disabled() -> bool:
    return (
        _envb("DISABLE_SCANNERS", False) or _kill_active() or _file_flag(_disable_scanners_file())
    )


def _auto_exit_enabled() -> bool:
    if os.getenv("AUTO_EXIT_ENABLED") is not None:
        return _envb("AUTO_EXIT_ENABLED", True)
    return _envb("AUTO_EXIT_WORKER_ENABLED", True)


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
    os.environ["EXIT_ENGINE_WORKER_ENABLED"] = "0"
    os.environ["AUTO_EXIT_ENABLED"] = "0"
    os.environ["AUTO_EXIT_WORKER_ENABLED"] = "0"


# =============================================================================
# Runtime import helpers
# =============================================================================
def _module_exists(name: str) -> bool:
    with contextlib.suppress(Exception):
        return importlib.util.find_spec(name) is not None
    return False


def _soft_import(
    mod_name: str, *, aliases: list[str] | None = None, critical: bool = False
) -> Any | None:
    tried: list[str] = []
    for name in [mod_name] + (aliases or []):
        tried.append(name)
        try:
            return importlib.import_module(name)
        except Exception as e:
            log.every("SOFT_IMPORT_FAIL", 30, mod=name, err=_short_exc(e))
    log.every("SOFT_IMPORT_ALL_FAIL", 60, module=mod_name, tried=",".join(tried)[:320])
    if critical and _fast_fail():
        raise RuntimeError(f"Critical import failed: {mod_name} tried={tried!r}")
    return None


def _filter_kwargs(fn: Any, kwargs: dict[str, Any]) -> dict[str, Any]:
    try:
        sig = inspect.signature(fn)
        params = sig.parameters.values()
        if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params):
            return kwargs
        allowed = {p.name for p in params}
        return {k: v for k, v in kwargs.items() if k in allowed}
    except Exception:
        return kwargs


async def _call_maybe_async(fn: Any, *args: Any, **kwargs: Any) -> Any:
    res = fn(*args, **kwargs)
    if inspect.isawaitable(res):
        return await res
    return res


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


async def _await_or_value_with_timeout(val_or_awaitable: Any, timeout: float) -> Any:
    if inspect.isawaitable(val_or_awaitable):
        return await asyncio.wait_for(val_or_awaitable, timeout=timeout)
    return val_or_awaitable


def refresh_runtime_constants() -> None:
    """
    Re-read env-driven tunables and refresh globals.
    Useful for tests that set env after import.
    """
    global _SCORING_TIMEOUT_S, _AI_RISK_TIMEOUT_S
    global _BUY_INFLIGHT_TTL, _BUY_COOLDOWN_SEC, _BUY_EXEC_TIMEOUT_S, _BUY_WARMUP_SEC

    _SCORING_TIMEOUT_S = float(_envf("SCORING_SERVICE_TIMEOUT_S", float(_SCORING_TIMEOUT_S)))
    _AI_RISK_TIMEOUT_S = float(_envf("AI_RISK_MODEL_TIMEOUT_S", float(_AI_RISK_TIMEOUT_S)))

    _BUY_INFLIGHT_TTL = float(_envf("BUY_INFLIGHT_TTL_SEC", float(_BUY_INFLIGHT_TTL)))
    _BUY_COOLDOWN_SEC = float(_envf("BUY_MINT_COOLDOWN_SEC", float(_BUY_COOLDOWN_SEC)))
    _BUY_EXEC_TIMEOUT_S = float(_envf("BUY_EXEC_TIMEOUT_S", float(_BUY_EXEC_TIMEOUT_S)))
    _BUY_WARMUP_SEC = float(_envf("BUY_WARMUP_SEC", float(_BUY_WARMUP_SEC)))


# =============================================================================
# Router proxy (avoid importing utils_pkg.router at import time)
# =============================================================================
_ROUTER_CACHE: Any | None = None


def _get_router_module() -> Any | None:
    global _ROUTER_CACHE
    if _ROUTER_CACHE is not None:
        return _ROUTER_CACHE
    try:
        _ROUTER_CACHE = importlib.import_module("utils_pkg.router")
    except Exception:
        _ROUTER_CACHE = None
    return _ROUTER_CACHE


class _RouterProxy:
    def __getattr__(self, name: str) -> Any:
        m = _get_router_module()
        if m is None:
            raise AttributeError(name)
        return getattr(m, name)

    def __setattr__(self, name: str, value: Any) -> None:
        m = _get_router_module()
        if m is None:
            raise AttributeError(name)
        setattr(m, name, value)


router = _RouterProxy()


# =============================================================================
# Data models
# =============================================================================
@dataclass
class CandidateMsg:
    mint: str
    ui: JSONDict = field(default_factory=dict)
    source: str = "unknown"
    ts: float = field(default_factory=_now)
    kind: str = "candidate"
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

    per_source_seen: dict[str, int] = field(default_factory=dict)
    per_source_dropped: dict[str, int] = field(default_factory=dict)
    per_source_last: dict[str, float] = field(default_factory=dict)

    skip_reasons: dict[str, int] = field(default_factory=dict)
    gate_rejects: dict[str, int] = field(default_factory=dict)
    last_reject: JSONDict = field(default_factory=dict)

    loop_lag_ms: float = 0.0
    last_health_at: float = 0.0

    cand_q_hwm: int = 0
    tick_q_hwm: int = 0
    cand_lat_ms_ema: float = 0.0
    tick_lat_ms_ema: float = 0.0

    last_kill_seen: float = 0.0


def _bump_counter(m: dict[str, int], key: str, n: int = 1) -> None:
    m[key] = int(m.get(key, 0)) + int(n)


def _topn(d: dict[str, int], n: int = 10) -> list[tuple[str, int]]:
    items = sorted(d.items(), key=lambda kv: (-kv[1], kv[0]))
    return items[: max(1, int(n))]


def _snapshot_state(
    M: "Modules",
    st: RuntimeState,
    *,
    cand_q: asyncio.Queue,
    tick_q: asyncio.Queue,
    task_view: JSONDict | None = None,
) -> JSONDict:
    dryrun_flag = _envb("DRYRUN", _envb("DRY_RUN", False))
    mode = _run_mode() or ""
    return {
        "version": MAIN_VERSION,
        "build": MAIN_BUILD,
        "run_id": st.run_id,
        "up_sec": int(_now() - st.started_at),
        "phase": _phase(),
        "run_mode": mode,
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
        "why_not_trading": {
            "top_skips": _topn(st.skip_reasons, n=_envi("MAIN_RUNBOOK_TOPN", 10)),
            "top_gate_rejects": _topn(st.gate_rejects, n=_envi("MAIN_RUNBOOK_TOPN", 10)),
            "last_reject": st.last_reject,
        },
        "flags": {
            "kill": int(_kill_active()),
            "disable_buys": int(_buys_disabled()),
            "disable_sells": int(_sells_disabled()),
            "disable_scanners": int(_scanners_disabled()),
            "dryrun": int(dryrun_flag),
            "fast_fail": int(_fast_fail()),
            "threadify_sync_modules": int(_threadify_sync_modules_enabled()),
            "jitter_disabled": int(_jitter_disabled()),
            "auto_exit_enabled": int(_auto_exit_enabled()),
        },
        "modules": {
            "buy_executor": int(M.maybe_buy is not None),
            "scoring_service": int(M.scoring_fn is not None and M.scoring_enabled),
            "ai_risk_model": int(
                M.ai_risk_enabled and M.ai_risk_gate_fn is not None and M.ai_overlay_fn is not None
            ),
            "infra_guard": int(M.infra_guard_mod is not None),
            "circuit_breakers": int(M.circuit_breakers_mod is not None),
            "ai_gate_legacy": int(M.ai_pass_gate is not None),
            "exit_engine": int(M.exit_mod is not None),
            "auto_exit_worker": int(M.auto_exit_worker_loop is not None),
            "reinvestor": int(M.reinvest_mod is not None),
            "smart_wallets": int(M.smart_mod is not None),
            "routeguard": int(M.routeguard_mod is not None),
            "risk_manager": int(M.risk_mod is not None),
        },
        "tasks": task_view or {},
    }


# =============================================================================
# Deduper
# =============================================================================
class _Deduper:
    def __init__(self, ttl_sec: float, max_items: int) -> None:
        self.ttl = float(ttl_sec)
        self.max_items = int(max_items)
        self._m: OrderedDict[str, float] = OrderedDict()

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


_CAND_DEDUP = _Deduper(
    ttl_sec=_envf("CAND_DEDUP_TTL_SEC", 10.0), max_items=_envi("CAND_DEDUP_MAX", 75_000)
)
_TICK_DEDUP = _Deduper(
    ttl_sec=_envf("TICK_DEDUP_TTL_SEC", 2.0), max_items=_envi("TICK_DEDUP_MAX", 200_000)
)

_DROP_POLICY = (_envs("QUEUE_DROP_POLICY", "drop_newest") or "drop_newest").strip().lower()
_DROP_OLDEST = _DROP_POLICY in {"drop_oldest", "oldest", "pop_oldest", "drop-oldest"}


def _queue_hwm_update(st: RuntimeState, cand_q: asyncio.Queue, tick_q: asyncio.Queue) -> None:
    with contextlib.suppress(Exception):
        st.cand_q_hwm = max(st.cand_q_hwm, int(cand_q.qsize()))
    with contextlib.suppress(Exception):
        st.tick_q_hwm = max(st.tick_q_hwm, int(tick_q.qsize()))


# =============================================================================
# Mint normalizer
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
# Bootstraps (best effort)
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
# Modules bundle
# =============================================================================
@dataclass
class Modules:
    normalize_mint: Callable[[str], str]

    maybe_buy: Callable[..., Any] | None = None
    buys_allowed: Callable[[], bool] | None = None

    scoring_mod: Any | None = None
    scoring_fn: Callable[..., Any] | None = None
    scoring_enabled: bool = True

    ai_risk_mod: Any | None = None
    ai_overlay_fn: Callable[..., Any] | None = None
    ai_risk_score_fn: Callable[..., Any] | None = None
    ai_risk_gate_fn: Callable[..., Any] | None = None
    ai_risk_enabled: bool = True

    infra_guard_mod: Any | None = None
    circuit_breakers_mod: Any | None = None

    ai_pass_gate: Callable[..., Any] | None = None
    ai_candidate_cls: Any | None = None

    routeguard_mod: Any | None = None
    risk_mod: Any | None = None

    exit_mod: Any | None = None

    auto_exit_worker_mod: Any | None = None
    auto_exit_worker_loop: Callable[..., Any] | None = None

    reinvest_mod: Any | None = None
    smart_mod: Any | None = None

    data_logger_mod: Any | None = None


def _resolve_modules() -> Modules:
    nm = _resolve_normalize_mint()

    maybe_buy = None
    with contextlib.suppress(Exception):
        m = importlib.import_module("utils_pkg.pump_trader")
        maybe_buy = getattr(m, "maybe_buy", None)
    if not callable(maybe_buy):
        pt = _soft_import(
            "utils_pkg.trader",
            aliases=["utils_pkg.trade_engine", "utils_pkg.entry_engine", "utils_pkg.pump_trader"],
        )
        maybe_buy = getattr(pt, "maybe_buy", None) if pt else None

    buys_allowed = None
    with contextlib.suppress(Exception):
        gr = importlib.import_module("utils_pkg.guardrails")
        buys_allowed = getattr(gr, "buys_allowed", None)

    scoring_mod = _soft_import("utils_pkg.scoring_service", critical=False)
    scoring_fn = getattr(scoring_mod, "score_candidate", None) if scoring_mod else None
    scoring_enabled = _envb("SCORING_SERVICE_ENABLED", True)

    ai_risk_mod = _soft_import("utils_pkg.ai_risk_model", critical=False)
    ai_overlay_fn = getattr(ai_risk_mod, "compute_ai_overlay", None) if ai_risk_mod else None
    ai_risk_score_fn = getattr(ai_risk_mod, "score_candidate", None) if ai_risk_mod else None
    ai_risk_gate_fn = getattr(ai_risk_mod, "candidate_gate", None) if ai_risk_mod else None
    ai_risk_enabled = _envb("AI_RISK_MODEL_ENABLED", True)

    infra_guard_mod = _soft_import("utils_pkg.infra_guard", critical=False)
    circuit_breakers_mod = _soft_import("utils_pkg.circuit_breakers", critical=False)

    ai_pass_gate = None
    ai_candidate_cls = None
    with contextlib.suppress(Exception):
        ai = importlib.import_module("utils_pkg.ai_buy_scoring")
        ai_pass_gate = getattr(ai, "pass_gate", None)
        ai_candidate_cls = getattr(ai, "Candidate", None)

    routeguard_mod = _soft_import("utils_pkg.routeguard", critical=False)
    risk_mod = _soft_import("utils_pkg.risk_manager", critical=False)

    exit_mod = _soft_import("utils_pkg.exit_engine", critical=False)

    auto_exit_worker_mod = _soft_import(
        "utils_pkg.auto_exit_worker",
        aliases=[
            "utils_pkg.auto_exit",
            "utils_pkg.workers.auto_exit_worker",
            "utils_pkg.worker.auto_exit_worker",
        ],
        critical=False,
    )
    auto_exit_worker_loop = (
        getattr(auto_exit_worker_mod, "worker_loop", None) if auto_exit_worker_mod else None
    )
    if not callable(auto_exit_worker_loop) and auto_exit_worker_mod is not None:
        for nm2 in ("loop", "run", "main", "run_worker", "auto_exit_loop"):
            fn = getattr(auto_exit_worker_mod, nm2, None)
            if callable(fn):
                auto_exit_worker_loop = fn
                break

    reinvest_mod = _soft_import("utils_pkg.reinvestor", critical=False)
    smart_mod = _soft_import("utils_pkg.smart_wallets", critical=False)

    data_logger_mod = _soft_import(
        "utils_pkg.data_logger",
        aliases=["utils_pkg.logger", "utils_pkg.trade_logger"],
        critical=False,
    )

    return Modules(
        normalize_mint=nm,
        maybe_buy=maybe_buy if callable(maybe_buy) else None,
        buys_allowed=buys_allowed if callable(buys_allowed) else None,
        scoring_mod=scoring_mod,
        scoring_fn=scoring_fn if callable(scoring_fn) else None,
        scoring_enabled=scoring_enabled,
        ai_risk_mod=ai_risk_mod,
        ai_overlay_fn=ai_overlay_fn if callable(ai_overlay_fn) else None,
        ai_risk_score_fn=ai_risk_score_fn if callable(ai_risk_score_fn) else None,
        ai_risk_gate_fn=ai_risk_gate_fn if callable(ai_risk_gate_fn) else None,
        ai_risk_enabled=ai_risk_enabled,
        infra_guard_mod=infra_guard_mod,
        circuit_breakers_mod=circuit_breakers_mod,
        ai_pass_gate=ai_pass_gate if callable(ai_pass_gate) else None,
        ai_candidate_cls=ai_candidate_cls,
        routeguard_mod=routeguard_mod,
        risk_mod=risk_mod,
        exit_mod=exit_mod,
        auto_exit_worker_mod=auto_exit_worker_mod,
        auto_exit_worker_loop=auto_exit_worker_loop if callable(auto_exit_worker_loop) else None,
        reinvest_mod=reinvest_mod,
        smart_mod=smart_mod,
        data_logger_mod=data_logger_mod,
    )


def _copy_modules(dst: Modules, src: Modules) -> None:
    # mutate in-place (critical for early-http snapshot to see live modules once loaded)
    for attr in (
        "normalize_mint",
        "maybe_buy",
        "buys_allowed",
        "scoring_mod",
        "scoring_fn",
        "scoring_enabled",
        "ai_risk_mod",
        "ai_overlay_fn",
        "ai_risk_score_fn",
        "ai_risk_gate_fn",
        "ai_risk_enabled",
        "infra_guard_mod",
        "circuit_breakers_mod",
        "ai_pass_gate",
        "ai_candidate_cls",
        "routeguard_mod",
        "risk_mod",
        "exit_mod",
        "auto_exit_worker_mod",
        "auto_exit_worker_loop",
        "reinvest_mod",
        "smart_mod",
        "data_logger_mod",
    ):
        with contextlib.suppress(Exception):
            setattr(dst, attr, getattr(src, attr))


# =============================================================================
# Scanner + queue plumbing (compatible)
# =============================================================================
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


def _trace_id_for(mint: str, source: str, ui: JSONDict, ts: float, run_id: str) -> str:
    seed = f"{mint}|{source}|{ui.get('pair') or ui.get('pairAddress') or ''}|{int(ts)}|{run_id}"
    return _hash_prefix(seed, 12)


def _bump_source(
    st: RuntimeState, source: str, *, seen: bool = False, dropped: bool = False
) -> None:
    s = source or "unknown"
    if seen:
        st.per_source_seen[s] = int(st.per_source_seen.get(s, 0)) + 1
        st.per_source_last[s] = _now()
    if dropped:
        st.per_source_dropped[s] = int(st.per_source_dropped.get(s, 0)) + 1


def _enqueue_candidate_nowait(
    M: Modules, out_q: asyncio.Queue[CandidateMsg], msg: CandidateMsg, st: RuntimeState
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
        rid = str(
            (msg.ui or {}).get("pair")
            or (msg.ui or {}).get("pairAddress")
            or (msg.ui or {}).get("id")
            or ""
        )
        key = f"{msg.source}:{msg.mint}:{rid}" if rid else f"{msg.source}:{msg.mint}"
        if _CAND_DEDUP.seen_recently(key):
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
            st.cand_dropped += 1
            _bump_source(st, msg.source, dropped=True)
            return
        st.cand_dropped += 1
        _bump_source(st, msg.source, dropped=True)


def _enqueue_tick_nowait(
    M: Modules, tick_q: asyncio.Queue[JSONDict], d: JSONDict, st: RuntimeState
) -> None:
    mint = M.normalize_mint(str(d.get("mint") or ""))
    if not mint:
        return
    if _envb("TICK_DEDUP_ENABLED", True):
        price = str(
            d.get("price") or d.get("priceUi") or d.get("price_ui") or d.get("last_price") or ""
        )
        key = f"{mint}:{price}"
        if _TICK_DEDUP.seen_recently(key):
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
            st.tick_dropped += 1
            return
        st.tick_dropped += 1


class _ScannerQueueProxy:
    def __init__(
        self,
        *,
        M: Modules,
        out_q: asyncio.Queue[CandidateMsg],
        tick_q: asyncio.Queue[JSONDict],
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
        self.put_nowait(item)

    def put_nowait(self, payload: Any) -> None:
        if self._stop.is_set() or payload is None:
            return

        if isinstance(payload, CandidateMsg):
            payload.source = payload.source or self._source
            _enqueue_candidate_nowait(self._M, self._out_q, payload, self._st)
            return

        if not isinstance(payload, dict):
            return

        mint = self._M.normalize_mint(
            str(
                payload.get("mint")
                or payload.get("mintAddress")
                or payload.get("address")
                or payload.get("token")
                or ""
            )
        )
        if not mint:
            return

        ui = dict(payload)
        ui.setdefault("mint", mint)
        ui.setdefault("source", self._source)
        ui.setdefault(
            "pool",
            ui.get("pool")
            or ui.get("venue")
            or ui.get("dex")
            or _source_to_pool_hint(self._source),
        )
        ui.setdefault("ts", float(ui.get("ts") or ui.get("timestamp") or _now()))

        msg = CandidateMsg(
            mint=mint, ui=ui, source=self._source, kind="candidate", ts=float(ui["ts"])
        )
        msg.trace_id = _trace_id_for(mint, self._source, ui, msg.ts, self._st.run_id)
        _enqueue_candidate_nowait(self._M, self._out_q, msg, self._st)

    def qsize(self) -> int:
        return self._out_q.qsize()

    def empty(self) -> bool:
        return self._out_q.empty()

    @property
    def maxsize(self) -> int:
        return int(getattr(self._out_q, "maxsize", 0) or 0)


def _scanner_entrypoints(mod: Any) -> list[Callable[..., Any]]:
    out: list[Callable[..., Any]] = []
    for nm in ("run_scanner", "run", "main", "scan_loop", "scanner_loop", "scan_once", "scan"):
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
    out_q: asyncio.Queue[CandidateMsg],
    tick_q: asyncio.Queue[JSONDict],
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

    proxy_q = _ScannerQueueProxy(
        M=M, out_q=out_q, tick_q=tick_q, source=module_name, stop_evt=stop_evt, st=st
    )

    async def _emit(payload: Any) -> None:
        proxy_q.put_nowait(payload)

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

    base_interval = max(0.2, float(_envf("SCAN_ONCE_INTERVAL_SEC", 2.0)))
    jitter = 0.0 if _jitter_disabled() else max(0.0, float(_envf("SCAN_ONCE_JITTER_SEC", 0.35)))

    async def _try_call(fn: Callable[..., Any]) -> bool:
        if stop_evt.is_set():
            return True

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
                log.event(
                    "SCANNER_RUN_FAIL",
                    module=module_name,
                    fn=getattr(fn, "__name__", "?"),
                    err=_short_exc(e),
                )
                if _fast_fail():
                    raise
                return True

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
                log.event(
                    "SCANNER_RUN_FAIL",
                    module=module_name,
                    fn=getattr(fn, "__name__", "?"),
                    err=_short_exc(e),
                )
                if _fast_fail():
                    raise
                return True

        return False

    for fn in fns:
        nm = getattr(fn, "__name__", "")
        if nm in {"scan_once", "scan"}:
            while not stop_evt.is_set():
                if _scanners_disabled():
                    log.every("SCANNERS_DISABLED_RUNTIME", 15, module=module_name)
                    with contextlib.suppress(asyncio.TimeoutError, TimeoutError):
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
# Gates
# =============================================================================
_SCORING_TIMEOUT_S = float(_envf("SCORING_SERVICE_TIMEOUT_S", 3.0))
_AI_RISK_TIMEOUT_S = float(_envf("AI_RISK_MODEL_TIMEOUT_S", 2.5))


async def _scoring_gate(M: Modules, ui: JSONDict) -> tuple[bool, str]:
    if (not M.scoring_enabled) or (M.scoring_fn is None):
        return True, "scoring_off"

    fail_closed = _envb("SCORING_SERVICE_FAIL_CLOSED", False)
    attach_features = _envb("SCORING_SERVICE_ATTACH_FEATURES", False)
    gate_mode = (_envs("SCORING_SERVICE_GATE_MODE", "trade_viable") or "trade_viable").lower()
    min_combined = float(_envf("SCORING_SERVICE_MIN_COMBINED", 0.0))

    raw_pool = dict(ui)
    raw_pool.setdefault("mint", ui.get("mint"))
    raw_pool.setdefault("symbol", ui.get("symbol") or ui.get("sym") or "UNKNOWN")
    raw_pool.setdefault("chain", ui.get("chain") or "SOL")
    raw_pool.setdefault(
        "liq_usd", ui.get("liq_usd") or ui.get("liquidity_usd") or ui.get("liq") or 0.0
    )
    raw_pool.setdefault("age_min", ui.get("age_min") or ui.get("token_age_min") or 0.0)

    context: JSONDict = {}
    if isinstance(ui.get("regime"), dict):
        context["regime"] = ui.get("regime")

    try:
        fn = M.scoring_fn
        kw = _filter_kwargs(fn, {"pool": raw_pool, "raw_pool": raw_pool, "context": context})

        def call() -> Any:
            if kw:
                return fn(**kw)  # type: ignore[misc]
            return fn(raw_pool, context=context)  # type: ignore[misc]

        if inspect.iscoroutinefunction(fn):
            out = await asyncio.wait_for(call(), timeout=_SCORING_TIMEOUT_S)
        else:
            out = (
                await asyncio.wait_for(asyncio.to_thread(call), timeout=_SCORING_TIMEOUT_S)
                if _threadify_sync_modules_enabled()
                else call()
            )

        if hasattr(out, "to_dict") and callable(out.to_dict):
            st_dict = out.to_dict()  # type: ignore[misc]
        elif isinstance(out, dict):
            st_dict = dict(out)
        else:
            st_dict = {"score": str(out)}

        if not attach_features and isinstance(st_dict.get("raw_features"), dict):
            st_dict.pop("raw_features", None)

        ui["score"] = st_dict

        if gate_mode in {"trade_viable", "viable", "default"}:
            ok = bool(st_dict.get("is_trade_viable", True))
            reason = str(st_dict.get("trade_gate_reason") or "trade_viable_gate")
        elif gate_mode == "label":
            ok = str(st_dict.get("label") or "").upper() in {"GREEN", "YELLOW"}
            reason = f"label={st_dict.get('label')}"
        elif gate_mode == "combined":
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
    except (asyncio.TimeoutError, TimeoutError):
        return (not fail_closed), (
            "scoring_timeout_allowed" if not fail_closed else "scoring_timeout"
        )
    except Exception as e:
        return (not fail_closed), (
            f"scoring_err_allowed:{_short_exc(e)}"
            if not fail_closed
            else f"scoring_err:{_short_exc(e)}"
        )


async def _infra_guard_gate(M: Modules, ui: JSONDict) -> tuple[bool, str]:
    if not M.infra_guard_mod or _envb("INFRA_GUARD_DISABLED", False):
        return True, "infra_guard_off"
    fail_closed = _envb("INFRA_GUARD_FAIL_CLOSED", False)

    mod = M.infra_guard_mod
    for nm in (
        "candidate_gate",
        "guard_candidate",
        "allow_candidate",
        "check_candidate",
        "ok_to_trade",
    ):
        fn = getattr(mod, nm, None)
        if not callable(fn):
            continue
        try:
            kw = _filter_kwargs(fn, {"ui": ui, "candidate": ui, **ui})
            out = fn(**kw) if kw else fn(ui)
            out = await _await_or_value_with_timeout(
                out, timeout=float(_envf("INFRA_GUARD_TIMEOUT_S", 2.0))
            )
            if isinstance(out, tuple) and len(out) >= 1:
                return bool(out[0]), str(out[1]) if len(out) > 1 else nm
            if isinstance(out, dict):
                return bool(out.get("ok", True)), str(out.get("reason") or nm)
            if isinstance(out, bool):
                return out, nm
        except (asyncio.TimeoutError, TimeoutError):
            return (not fail_closed), (
                "infra_guard_timeout_allowed" if not fail_closed else "infra_guard_timeout"
            )
        except Exception as e:
            return (not fail_closed), (
                f"infra_guard_err_allowed:{_short_exc(e)}"
                if not fail_closed
                else f"infra_guard_err:{_short_exc(e)}"
            )
    return True, "infra_guard_no_api"


async def _circuit_breakers_gate(M: Modules, ui: JSONDict) -> tuple[bool, str]:
    if not M.circuit_breakers_mod or _envb("CIRCUIT_BREAKERS_DISABLED", False):
        return True, "cb_off"
    fail_closed = _envb("CIRCUIT_BREAKERS_FAIL_CLOSED", False)

    mod = M.circuit_breakers_mod
    for nm in ("allow_entry", "allow_buy", "check_entry", "check_candidate", "gate"):
        fn = getattr(mod, nm, None)
        if not callable(fn):
            continue
        try:
            kw = _filter_kwargs(fn, {"ui": ui, "candidate": ui, **ui})
            out = fn(**kw) if kw else fn(ui)
            out = await _await_or_value_with_timeout(
                out, timeout=float(_envf("CIRCUIT_BREAKERS_TIMEOUT_S", 2.0))
            )
            if isinstance(out, tuple) and len(out) >= 1:
                return bool(out[0]), str(out[1]) if len(out) > 1 else nm
            if isinstance(out, dict):
                return bool(out.get("ok", True)), str(out.get("reason") or nm)
            if isinstance(out, bool):
                return out, nm
        except (asyncio.TimeoutError, TimeoutError):
            return (not fail_closed), ("cb_timeout_allowed" if not fail_closed else "cb_timeout")
        except Exception as e:
            return (not fail_closed), (
                f"cb_err_allowed:{_short_exc(e)}" if not fail_closed else f"cb_err:{_short_exc(e)}"
            )
    return True, "cb_no_api"


async def _ai_risk_gate(M: Modules, ui: JSONDict) -> tuple[bool, str]:
    if (
        (not M.ai_risk_enabled)
        or (M.ai_overlay_fn is None)
        or (M.ai_risk_gate_fn is None)
        or (M.ai_risk_score_fn is None)
    ):
        return True, "ai_risk_off"

    fail_closed = _envb("AI_RISK_MODEL_FAIL_CLOSED", False)

    async def _compute_overlay() -> Any:
        fn = M.ai_overlay_fn
        assert fn is not None
        for attempt in (
            lambda: fn(ui),
            lambda: fn(candidate=ui),
            lambda: fn(live=ui),
            lambda: fn(),
        ):
            try:
                out = attempt()
                if inspect.isawaitable(out):
                    out = await out
                return out
            except TypeError:
                continue
        out = fn()  # type: ignore[misc]
        if inspect.isawaitable(out):
            out = await out
        return out

    async def _score_candidate() -> Any:
        fn = M.ai_risk_score_fn
        assert fn is not None
        kw = _filter_kwargs(fn, {"candidate": ui, "ui": ui, **ui})
        out = fn(**kw) if kw else fn(ui)
        if inspect.isawaitable(out):
            out = await out
        return out

    async def _candidate_gate(cand: Any, overlay: Any) -> Any:
        fn = M.ai_risk_gate_fn
        assert fn is not None
        for attempt in (
            lambda: fn(cand, overlay),
            lambda: fn(candidate=cand, overlay=overlay),
            lambda: fn(ui, overlay),
            lambda: fn(cand),
        ):
            try:
                out = attempt()
                if inspect.isawaitable(out):
                    out = await out
                return out
            except TypeError:
                continue
        out = fn(cand, overlay)  # type: ignore[misc]
        if inspect.isawaitable(out):
            out = await out
        return out

    try:
        overlay = await asyncio.wait_for(_compute_overlay(), timeout=_AI_RISK_TIMEOUT_S)
        ui["ai_overlay"] = overlay if isinstance(overlay, dict) else {"overlay": str(overlay)}

        cand = await asyncio.wait_for(_score_candidate(), timeout=_AI_RISK_TIMEOUT_S)
        ui["ai_candidate"] = cand if isinstance(cand, dict) else {"cand": str(cand)}

        gate = await asyncio.wait_for(_candidate_gate(cand, overlay), timeout=_AI_RISK_TIMEOUT_S)

        if isinstance(gate, dict):
            ui["ai_gate"] = gate
            ok = bool(gate.get("ok", True))
            why = str(gate.get("reason") or gate.get("label") or "ai_risk_gate")
        elif isinstance(gate, tuple) and len(gate) >= 1:
            ok = bool(gate[0])
            why = str(gate[1]) if len(gate) > 1 else "ai_risk_gate"
            ui["ai_gate"] = {"ok": ok, "reason": why}
        else:
            ok = bool(gate)
            why = "ai_risk_gate"
            ui["ai_gate"] = {"ok": ok, "reason": why}

        return ok, why
    except (asyncio.TimeoutError, TimeoutError):
        return (not fail_closed), (
            "ai_risk_timeout_allowed" if not fail_closed else "ai_risk_timeout"
        )
    except Exception as e:
        return (not fail_closed), (
            f"ai_risk_err_allowed:{_short_exc(e)}"
            if not fail_closed
            else f"ai_risk_err:{_short_exc(e)}"
        )


async def _ai_gate_legacy(M: Modules, ui: JSONDict) -> tuple[bool, float, str]:
    if (not M.ai_pass_gate) or _envb("AI_GATE_DISABLED", False):
        return True, 0.0, "ai_gate_off"
    try:
        if M.ai_candidate_cls:
            cand_kwargs = _filter_kwargs(
                getattr(M.ai_candidate_cls, "__init__", lambda **_: None), dict(ui)
            )
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


async def _routeguard_gate(M: Modules, ui: JSONDict) -> tuple[bool, str]:
    if not M.routeguard_mod or _envb("ROUTEGUARD_DISABLED", False):
        return True, "routeguard_off"
    rg = M.routeguard_mod
    fail_closed = _envb("ROUTEGUARD_FAIL_CLOSED", False)

    async def _maybe_await(x: Any) -> Any:
        return await x if inspect.isawaitable(x) else x

    for nm in ("guard_candidate", "guard", "check_candidate"):
        fn = getattr(rg, nm, None)
        if callable(fn):
            try:
                out = await _maybe_await(fn(ui))
                if isinstance(out, tuple) and len(out) >= 1:
                    return bool(out[0]), str(out[1]) if len(out) > 1 else nm
                if isinstance(out, dict):
                    return bool(out.get("ok", True)), str(out.get("reason") or nm)
            except Exception as e:
                return (not fail_closed), f"routeguard_err:{_short_exc(e)}"
    return True, "routeguard_no_api"


async def _risk_gate(M: Modules, ui: JSONDict) -> tuple[bool, str]:
    if not M.risk_mod or _envb("RISK_MANAGER_DISABLED", False):
        return True, "risk_off"
    rm = M.risk_mod
    fail_closed = _envb("RISK_FAIL_CLOSED", False)

    async def _maybe_await(x: Any) -> Any:
        return await x if inspect.isawaitable(x) else x

    for nm in (
        "check_candidate",
        "check_buy",
        "check_entry",
        "guard_candidate",
        "guard_buy",
        "allow_buy",
        "allow_entry",
    ):
        fn = getattr(rm, nm, None)
        if callable(fn):
            try:
                kw = _filter_kwargs(fn, {"ui": ui, "candidate": ui, **ui})
                out = await _maybe_await(fn(**kw) if kw else fn(ui))
                if isinstance(out, tuple) and len(out) >= 1:
                    return bool(out[0]), str(out[1]) if len(out) > 1 else nm
                if isinstance(out, dict):
                    return bool(out.get("ok", True)), str(out.get("reason") or nm)
                if isinstance(out, bool):
                    return out, nm
            except Exception as e:
                return (not fail_closed), f"risk_err:{_short_exc(e)}"
    return True, "risk_no_api"


# =============================================================================
# Exit engine wiring
# =============================================================================
def _exit_allow_events_when_sells_disabled() -> bool:
    return _envb("EXIT_ENGINE_ALLOW_EVENTS_WHEN_SELLS_DISABLED", True)


async def _exit_on_buy_filled(M: Modules, payload: JSONDict) -> None:
    if not M.exit_mod:
        return
    if _sells_disabled() and not _exit_allow_events_when_sells_disabled():
        return
    fn = getattr(M.exit_mod, "on_buy_filled", None) or getattr(
        M.exit_mod, "on_buy_filled_event", None
    )
    if not callable(fn):
        return
    with contextlib.suppress(Exception):
        await _call_maybe_async(fn, payload)


async def _exit_on_tick(M: Modules, payload: JSONDict) -> None:
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
# Tick router + sinks
# =============================================================================
async def _tick_router(
    M: Modules, tick_q: asyncio.Queue[JSONDict], stop_evt: asyncio.Event, st: RuntimeState
) -> None:
    while not stop_evt.is_set():
        try:
            d = await asyncio.wait_for(tick_q.get(), timeout=0.5)
        except (asyncio.TimeoutError, TimeoutError):
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


async def _tick_sink(
    tick_q: asyncio.Queue[JSONDict], stop_evt: asyncio.Event, st: RuntimeState
) -> None:
    while not stop_evt.is_set():
        try:
            _ = await asyncio.wait_for(tick_q.get(), timeout=0.5)
        except (asyncio.TimeoutError, TimeoutError):
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


async def _candidate_sink(
    in_q: asyncio.Queue[CandidateMsg], stop_evt: asyncio.Event, st: RuntimeState
) -> None:
    while not stop_evt.is_set():
        try:
            msg = await asyncio.wait_for(in_q.get(), timeout=0.5)
        except (asyncio.TimeoutError, TimeoutError):
            continue
        except asyncio.CancelledError:
            raise
        except Exception:
            continue
        try:
            st.cand_sunk += 1
            st.last_candidate_at = _now()
            with contextlib.suppress(Exception):
                st.cand_lat_ms_ema = _ema(
                    st.cand_lat_ms_ema, max(0.0, (_now() - float(msg.ts or _now())) * 1000.0)
                )
        finally:
            with contextlib.suppress(Exception):
                in_q.task_done()


# =============================================================================
# Entry worker
# =============================================================================
_BUY_INFLIGHT: dict[str, float] = {}
_BUY_INFLIGHT_TTL = _envf("BUY_INFLIGHT_TTL_SEC", 25.0)

_BUY_COOLDOWN_SEC = _envf("BUY_MINT_COOLDOWN_SEC", 0.0)
_LAST_BUY_ATTEMPT: dict[str, float] = {}

_BUY_EXEC_TIMEOUT_S = float(_envf("BUY_EXEC_TIMEOUT_S", 35.0))
_BUY_WARMUP_SEC = float(_envf("BUY_WARMUP_SEC", 0.0))


def _buy_inflight_ok(mint: str) -> bool:
    now_ts = _now()
    cutoff = now_ts - float(_BUY_INFLIGHT_TTL)
    for k, ts in list(_BUY_INFLIGHT.items()):
        if ts < cutoff:
            _BUY_INFLIGHT.pop(k, None)
    if mint in _BUY_INFLIGHT:
        return False
    _BUY_INFLIGHT[mint] = now_ts
    return True


def _buy_cooldown_ok(mint: str) -> bool:
    if _BUY_COOLDOWN_SEC <= 0:
        return True
    now_ts = _now()
    last = float(_LAST_BUY_ATTEMPT.get(mint, 0.0))
    if last and (now_ts - last) < float(_BUY_COOLDOWN_SEC):
        return False
    _LAST_BUY_ATTEMPT[mint] = now_ts
    return True


def _reject(
    st: RuntimeState,
    *,
    skip_key: str,
    gate_key: str | None = None,
    last_blob: JSONDict | None = None,
) -> None:
    st.buy_skip += 1
    _bump_counter(st.skip_reasons, skip_key, 1)
    if gate_key:
        _bump_counter(st.gate_rejects, gate_key, 1)
    if last_blob:
        st.last_reject = {k: v for k, v in list(last_blob.items())[:50]}


async def _entry_worker(
    M: Modules,
    wid: int,
    in_q: asyncio.Queue[CandidateMsg],
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

    async def _on_submitted(p: JSONDict) -> None:
        st.last_buy_action_at = _now()
        log.event("BUY_SUBMITTED", run_id=st.run_id, worker=wid, **(p or {}))

    async def _on_filled(p: JSONDict) -> None:
        st.last_buy_action_at = _now()
        st.last_buy_fill_at = _now()
        log.event("BUY_FILLED", run_id=st.run_id, worker=wid, **(p or {}))
        await _exit_on_buy_filled(M, p or {})

    async def _on_failed(p: JSONDict) -> None:
        st.last_buy_action_at = _now()
        log.event("BUY_FAILED", run_id=st.run_id, worker=wid, **(p or {}))

    while not stop_evt.is_set():
        try:
            msg = await asyncio.wait_for(in_q.get(), timeout=0.5)
        except (asyncio.TimeoutError, TimeoutError):
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
            ui.setdefault(
                "trace_id",
                msg.trace_id or _trace_id_for(mint, msg.source, ui, float(ui["ts"]), st.run_id),
            )
            ui.setdefault("run_id", st.run_id)

            with contextlib.suppress(Exception):
                st.cand_lat_ms_ema = _ema(
                    st.cand_lat_ms_ema, max(0.0, (_now() - float(msg.ts or _now())) * 1000.0)
                )

            if not mint:
                _reject(st, skip_key="buy_skip_no_mint")
                continue

            if _BUY_WARMUP_SEC > 0 and (_now() - started_at) < _BUY_WARMUP_SEC:
                _reject(st, skip_key="buy_skip_warmup")
                log.every("BUY_SKIP_WARMUP", 10, mint=mint, warmup_sec=_BUY_WARMUP_SEC)
                continue

            if _buys_disabled():
                _reject(st, skip_key="buy_skip_disabled")
                log.every(
                    "BUY_SKIP_DISABLED", 15, mint=mint, pool=ui.get("pool"), source=msg.source
                )
                continue

            if M.buys_allowed is not None:
                with contextlib.suppress(Exception):
                    if not bool(M.buys_allowed()):
                        _reject(st, skip_key="buy_skip_guardrails")
                        log.every("BUY_SKIP_GUARDRAILS", 10, mint=mint)
                        continue

            if not _buy_inflight_ok(mint):
                _reject(st, skip_key="buy_skip_inflight")
                log.every(
                    "BUY_SKIP_INFLIGHT", 10, mint=mint, pool=ui.get("pool"), source=msg.source
                )
                continue

            if not _buy_cooldown_ok(mint):
                _reject(st, skip_key="buy_skip_cooldown")
                log.every("BUY_SKIP_COOLDOWN", 10, mint=mint, cooldown_sec=_BUY_COOLDOWN_SEC)
                with contextlib.suppress(Exception):
                    _BUY_INFLIGHT.pop(mint, None)
                continue

            ok_sc, why_sc = await _scoring_gate(M, ui)
            if not ok_sc:
                _reject(
                    st,
                    skip_key="buy_skip_scoring",
                    gate_key="scoring_gate",
                    last_blob={
                        "mint": mint,
                        "why": why_sc,
                        "pool": ui.get("pool"),
                        "score": ui.get("score"),
                    },
                )
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

            ok_ig, why_ig = await _infra_guard_gate(M, ui)
            if not ok_ig:
                _reject(
                    st,
                    skip_key="buy_skip_infra_guard",
                    gate_key="infra_guard",
                    last_blob={"mint": mint, "why": why_ig},
                )
                log.event("BUY_SKIP_INFRA_GUARD", run_id=st.run_id, mint=mint, why=why_ig)
                with contextlib.suppress(Exception):
                    _BUY_INFLIGHT.pop(mint, None)
                continue

            ok_cb, why_cb = await _circuit_breakers_gate(M, ui)
            if not ok_cb:
                _reject(
                    st,
                    skip_key="buy_skip_circuit_breakers",
                    gate_key="circuit_breakers",
                    last_blob={"mint": mint, "why": why_cb},
                )
                log.event("BUY_SKIP_CIRCUIT_BREAKERS", run_id=st.run_id, mint=mint, why=why_cb)
                with contextlib.suppress(Exception):
                    _BUY_INFLIGHT.pop(mint, None)
                continue

            ok_arm, why_arm = await _ai_risk_gate(M, ui)
            if not ok_arm:
                _reject(
                    st,
                    skip_key="buy_skip_ai_risk",
                    gate_key="ai_risk_gate",
                    last_blob={"mint": mint, "why": why_arm, "ai_gate": ui.get("ai_gate")},
                )
                log.event(
                    "BUY_SKIP_AI_RISK",
                    run_id=st.run_id,
                    mint=mint,
                    pool=ui.get("pool"),
                    why=why_arm,
                    ai_gate=ui.get("ai_gate"),
                )
                with contextlib.suppress(Exception):
                    _BUY_INFLIGHT.pop(mint, None)
                continue

            ok_risk, why_risk = await _risk_gate(M, ui)
            if not ok_risk:
                _reject(
                    st,
                    skip_key="buy_skip_risk",
                    gate_key="risk_gate",
                    last_blob={"mint": mint, "why": why_risk},
                )
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
                _reject(
                    st,
                    skip_key="buy_skip_routeguard",
                    gate_key="routeguard_gate",
                    last_blob={"mint": mint, "why": why_rg},
                )
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

            ok_ai, score_ai, why_ai = await _ai_gate_legacy(M, ui)
            if not ok_ai:
                _reject(
                    st,
                    skip_key="buy_skip_ai_gate",
                    gate_key="legacy_ai_gate",
                    last_blob={"mint": mint, "why": why_ai, "score": score_ai},
                )
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
                        _reject(st, skip_key="buy_dryrun_skip_noarg")
                        log.every("DRYRUN_SKIP_NOARG", 10, mint=mint)
                        continue

                    ok = await asyncio.wait_for(
                        _call_maybe_async(fn, **kwargs), timeout=_BUY_EXEC_TIMEOUT_S
                    )

                    ok_bool = bool(ok)
                    if isinstance(ok, dict):
                        ok_bool = bool(ok.get("ok", ok.get("success", False)))

                    if ok_bool:
                        st.buy_ok += 1
                    else:
                        st.buy_fail += 1

            except (asyncio.TimeoutError, TimeoutError):
                st.buy_exc += 1
                st.buy_fail += 1
                log.event(
                    "BUY_EXEC_TIMEOUT", run_id=st.run_id, mint=mint, timeout_s=_BUY_EXEC_TIMEOUT_S
                )

            except asyncio.CancelledError:
                raise
            except Exception as e:
                st.buy_exc += 1
                st.buy_fail += 1
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
# Signal handling (keep back-compat)
# =============================================================================
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
        loop_ = asyncio.get_running_loop()
        with contextlib.suppress(Exception):
            loop_.add_signal_handler(signal.SIGINT, _ask_stop)  # type: ignore[arg-type]
        with contextlib.suppress(Exception):
            loop_.add_signal_handler(signal.SIGTERM, _ask_stop)  # type: ignore[arg-type]


# =============================================================================
# Optional stdlib HTTP server (+ P2 hook for utils_pkg.health)
# =============================================================================
def _resolve_health_snapshot_fn() -> Callable[[], Any] | None:
    with contextlib.suppress(Exception):
        m = importlib.import_module("utils_pkg.health")
        fn = getattr(m, "health_snapshot", None)
        if callable(fn):
            return fn  # type: ignore[return-value]
    return None


def _health_sidecar_owns_port() -> bool:
    """
    Render bootstrap may start a dedicated health_http sidecar that binds PORT.
    If HEALTH_PORT/HEALTH_HTTP_PORT matches PORT, assume the sidecar owns the bind.

    Override with MAIN_HTTP_ALLOW_BIND_PORT=1 if you *really* want main to bind PORT.
    """
    if not _is_render():
        return False

    if _envb("MAIN_HTTP_ALLOW_BIND_PORT", False):
        return False

    port = int(_envi("PORT", 0))
    hp = 0

    if os.getenv("HEALTH_HTTP_PORT") is not None:
        hp = max(hp, int(_envi("HEALTH_HTTP_PORT", 0)))
    if os.getenv("HEALTH_PORT") is not None:
        hp = max(hp, int(_envi("HEALTH_PORT", 0)))

    if port <= 0 or hp <= 0:
        return False

    return hp == port


def _http_port() -> int:
    """
    Deterministic + Render-safe:

    - Disabled automatically during pytest unless MAIN_HTTP_ALLOW_IN_TESTS=1.
    - MAIN_HTTP_PORT wins ONLY if > 0.
    - Otherwise fall back to Render's PORT if present.
    - Otherwise optional HTTP_PORT.
    - Else 0 (disabled).
    """
    if _in_tests() and not _envb("MAIN_HTTP_ALLOW_IN_TESTS", False):
        return 0

    # If Render health sidecar owns PORT, do NOT bind it from main unless explicitly configured.
    if _health_sidecar_owns_port():
        if os.getenv("MAIN_HTTP_PORT") is not None:
            p = int(_envi("MAIN_HTTP_PORT", 0))
            return p if p > 0 else 0
        if os.getenv("HTTP_PORT") is not None:
            p = int(_envi("HTTP_PORT", 0))
            return p if p > 0 else 0
        return 0

    if os.getenv("MAIN_HTTP_PORT") is not None:
        p = int(_envi("MAIN_HTTP_PORT", 0))
        if p > 0:
            return p

    if os.getenv("PORT") is not None:
        p = int(_envi("PORT", 0))
        if p > 0:
            return p

    if os.getenv("HTTP_PORT") is not None:
        p = int(_envi("HTTP_PORT", 0))
        if p > 0:
            return p

    return 0


async def _http_server(
    M: Modules,
    st: RuntimeState,
    cand_q: asyncio.Queue,
    tick_q: asyncio.Queue,
    stop_evt: asyncio.Event,
    task_view_fn: Callable[[], JSONDict],
) -> None:
    host = _envs("MAIN_HTTP_HOST", "0.0.0.0")
    port = int(_http_port())
    if port <= 0:
        return

    health_fn = _resolve_health_snapshot_fn()

    async def _write_resp(writer: asyncio.StreamWriter, code: int, body: str, ctype: str) -> None:
        b = body.encode("utf-8", "ignore")
        reason = "OK" if code == 200 else "ERROR"
        resp = (
            f"HTTP/1.1 {code} {reason}\r\n"
            f"Content-Type: {ctype}\r\n"
            f"Content-Length: {len(b)}\r\n"
            f"Connection: close\r\n\r\n"
        ).encode("utf-8", "ignore") + b
        writer.write(resp)
        with contextlib.suppress(Exception):
            await writer.drain()
        with contextlib.suppress(Exception):
            writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()

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

        ctype = "application/json"

        if path in {"/live", "/livez"}:
            body = _json_dumps(
                {"ok": True, "run_id": st.run_id, "version": MAIN_VERSION, "build": MAIN_BUILD}
            )
            await _write_resp(writer, 200, body, ctype)
            return

        if path in {"/ready", "/readyz"}:
            if stop_evt.is_set() or _kill_active():
                body = _json_dumps({"ok": False, "reason": "stopping_or_kill", "run_id": st.run_id})
                await _write_resp(writer, 503, body, ctype)
                return

            use_snapshot = _envb("MAIN_READYZ_USES_HEALTH_SNAPSHOT", True)
            if health_fn is not None and use_snapshot:
                try:
                    out = health_fn()
                    if inspect.isawaitable(out):
                        out = await asyncio.wait_for(
                            out, timeout=float(_envf("MAIN_HTTP_READY_TIMEOUT_S", 1.5))
                        )
                    if isinstance(out, dict):
                        ok = bool(out.get("ok", True))
                        body = _json_dumps(out)
                        await _write_resp(writer, 200 if ok else 503, body, ctype)
                        return
                except Exception as e:
                    body = _json_dumps(
                        {
                            "ok": False,
                            "reason": "ready_snapshot_failed",
                            "err": _short_exc(e),
                            "run_id": st.run_id,
                        }
                    )
                    await _write_resp(writer, 503, body, ctype)
                    return

            body = _json_dumps({"ok": True, "run_id": st.run_id, "phase": _phase()})
            await _write_resp(writer, 200, body, ctype)
            return

        if path in {"/health", "/healthz"}:
            if health_fn is not None:
                try:
                    out = health_fn()
                    if inspect.isawaitable(out):
                        out = await asyncio.wait_for(
                            out, timeout=float(_envf("MAIN_HTTP_HEALTH_TIMEOUT_S", 2.0))
                        )
                    if isinstance(out, dict):
                        body = _json_dumps(out)
                        code = 200 if bool(out.get("ok", True)) else 503
                        await _write_resp(writer, code, body, ctype)
                        return
                    body = _json_dumps({"ok": True, "detail": str(out), "run_id": st.run_id})
                    await _write_resp(writer, 200, body, ctype)
                    return
                except Exception as e:
                    body = _json_dumps(
                        {
                            "ok": False,
                            "reason": "health_snapshot_failed",
                            "err": _short_exc(e),
                            "run_id": st.run_id,
                        }
                    )
                    await _write_resp(writer, 503, body, ctype)
                    return

            body = _json_dumps(
                {"ok": True, "run_id": st.run_id, "version": MAIN_VERSION, "build": MAIN_BUILD}
            )
            await _write_resp(writer, 200, body, ctype)
            return

        if path in {"/state", "/debug/state"}:
            body = _json_dumps(
                _snapshot_state(M, st, cand_q=cand_q, tick_q=tick_q, task_view=task_view_fn())
            )
            await _write_resp(writer, 200, body, ctype)
            return

        await _write_resp(writer, 200, "ok\n", "text/plain")

    try:
        server = await asyncio.start_server(_handle, host=host, port=port)
    except OSError as e:
        log.event(
            "HTTP_SERVER_BIND_FAIL", run_id=st.run_id, host=host, port=port, err=_short_exc(e)
        )
        if _fast_fail():
            raise
        return

    log.event("HTTP_SERVER_STARTED", run_id=st.run_id, host=host, port=port)

    try:
        await stop_evt.wait()
    finally:
        server.close()
        with contextlib.suppress(Exception):
            await server.wait_closed()
        log.event("HTTP_SERVER_STOPPED", run_id=st.run_id)


def _early_http_enabled(port: int) -> bool:
    if port <= 0:
        return False
    if _envb("MAIN_HTTP_DISABLED", False):
        return False
    if _health_sidecar_owns_port():
        return False
    # Default early-http ON in Render, OFF elsewhere, unless explicitly set.
    return _envb("MAIN_HTTP_EARLY_START", _is_render())


# =============================================================================
# Watchdogs
# =============================================================================
async def _kill_watchdog(stop_evt: asyncio.Event, st: RuntimeState) -> None:
    watchdog_sec = max(0.5, float(_envf("KILL_WATCHDOG_SEC", 2.0)))
    shutdown_on_trigger = _envb("KILL_SHUTDOWN_ON_TRIGGER", False)
    while not stop_evt.is_set():
        if _kill_active():
            st.last_kill_seen = _now()
            log.every(
                "KILL_SWITCH_ACTIVE",
                2,
                run_id=st.run_id,
                shutdown_on_trigger=int(shutdown_on_trigger),
            )
            if shutdown_on_trigger:
                stop_evt.set()
                return
        await asyncio.sleep(watchdog_sec)


async def _runtime_watchdog(
    M: Modules,
    st: RuntimeState,
    *,
    cand_q: asyncio.Queue,
    tick_q: asyncio.Queue,
    stop_evt: asyncio.Event,
    task_view_fn: Callable[[], JSONDict],
) -> None:
    interval = max(1.0, float(_envf("MAIN_WATCHDOG_SEC", 5.0)))
    health_sec = int(getattr(C, "HEALTH_LOG_SEC", 30) or 30)
    starvation_warn = float(getattr(C, "STARVATION_WARN_SEC", 180) or 180)
    no_buy_warn = float(getattr(C, "NO_BUY_WARN_SEC", 300) or 300)
    hang_restart_ms = float(_envf("MAIN_HANG_RESTART_LAG_MS", 0.0))
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
            return

        now_ts = _now()
        if st.last_candidate_at > 0 and (now_ts - st.last_candidate_at) > starvation_warn:
            log.every(
                "WATCHDOG_NO_CANDIDATES",
                30,
                run_id=st.run_id,
                sec_since_last=int(now_ts - st.last_candidate_at),
                cand_q=cand_q.qsize(),
            )

        if st.last_buy_action_at > 0 and (now_ts - st.last_buy_action_at) > no_buy_warn:
            log.every(
                "WATCHDOG_NO_BUY_ACTION",
                30,
                run_id=st.run_id,
                sec_since_last=int(now_ts - st.last_buy_action_at),
                cand_q=cand_q.qsize(),
            )

        if (now_ts - st.last_health_at) >= float(health_sec):
            st.last_health_at = now_ts
            snap = _snapshot_state(M, st, cand_q=cand_q, tick_q=tick_q, task_view=task_view_fn())
            log.event("HEALTH", **snap, queue_policy=_DROP_POLICY, loop_lag_ms=int(st.loop_lag_ms))


async def _uptime_monitor(stop_evt: asyncio.Event, st: RuntimeState, max_uptime_sec: float) -> None:
    if max_uptime_sec <= 0:
        return
    while not stop_evt.is_set():
        if (_now() - st.started_at) >= max_uptime_sec:
            log.event(
                "MAIN_MAX_UPTIME_REACHED", run_id=st.run_id, max_uptime_sec=int(max_uptime_sec)
            )
            stop_evt.set()
            return
        await asyncio.sleep(1.0)


# =============================================================================
# Helpers
# =============================================================================
async def _run_preflight_if_enabled(run_id: str) -> None:
    def _should_run() -> bool:
        if _envb("SAFE_MODE", False) or _in_tests():
            return False
        return _envb("MAIN_RUN_PREFLIGHT_DEFAULT", _is_render())

    if not _should_run():
        return

    if not _module_exists("scripts.preflight"):
        log.event("PREFLIGHT_MISSING", run_id=run_id)
        if _fast_fail():
            raise RuntimeError("scripts.preflight missing")
        return

    exe = sys.executable or "python"
    profile = (
        _envs("PREFLIGHT_PROFILE", "") or _envs("PREFLIGHT_MAIN_PROFILE", "deploy") or "deploy"
    ).strip()
    args = [exe, "-m", "scripts.preflight", "--profile", profile, "--json-only"]
    if _envb("PREFLIGHT_STRICT", _is_render()):
        args.append("--strict")

    log.event("PREFLIGHT_START", run_id=run_id)
    proc = await asyncio.create_subprocess_exec(
        *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    try:
        timeout_s = float(_envf("PREFLIGHT_MAIN_TIMEOUT_S", 120.0))
        out, err = await asyncio.wait_for(proc.communicate(), timeout=max(20.0, timeout_s))
    except (asyncio.TimeoutError, TimeoutError) as err2:
        with contextlib.suppress(Exception):
            proc.kill()
        raise RuntimeError("preflight timeout") from err2

    rc = int(proc.returncode or 0)
    if err:
        log.every(
            "PREFLIGHT_STDERR", 10, run_id=run_id, stderr=err.decode("utf-8", "ignore")[:2000]
        )
    if out:
        log.every(
            "PREFLIGHT_STDOUT", 10, run_id=run_id, stdout=out.decode("utf-8", "ignore")[:2000]
        )

    if rc != 0:
        raise RuntimeError(f"preflight failed rc={rc}")
    log.event("PREFLIGHT_OK", run_id=run_id)


async def _shutdown_best_effort(stop_evt: asyncio.Event, *, reason: str, run_id: str) -> None:
    with contextlib.suppress(Exception):
        m = importlib.import_module("utils_pkg.shutdown")
        for nm in ("shutdown", "shutdown_all", "close_all", "graceful_shutdown", "cleanup"):
            fn = getattr(m, nm, None)
            if callable(fn):
                kw = _filter_kwargs(
                    fn,
                    {
                        "stop_evt": stop_evt,
                        "stop_event": stop_evt,
                        "reason": reason,
                        "run_id": run_id,
                    },
                )
                out = fn(**kw) if kw else fn()  # type: ignore[misc]
                if inspect.isawaitable(out):
                    await out
                return


async def _close_asyncpg_pools_best_effort(*, run_id: str, timeout_s: float = 2.0) -> None:
    """
    Best-effort close of any asyncpg pools used by the app.
    Stdlib-only at import time; uses lazy imports; time-bounded.
    """
    targets: list[tuple[str, tuple[str, ...]]] = [
        ("utils_pkg.ai_db", ("close_asyncpg_pool", "close_pool", "shutdown", "close")),
        ("utils_pkg.positions_pg", ("close_asyncpg_pool", "close_pool", "shutdown", "close")),
        (
            "utils_pkg.shutdown",
            ("shutdown", "shutdown_all", "close_all", "graceful_shutdown", "cleanup"),
        ),
    ]

    async def _call(fn: Any) -> None:
        try:
            out = fn()
        except TypeError:
            out = fn(run_id=run_id)
        if inspect.isawaitable(out):
            await out

    for mod_name, fn_names in targets:
        try:
            mod = importlib.import_module(mod_name)
        except Exception:
            continue

        for fn_name in fn_names:
            fn = getattr(mod, fn_name, None)
            if not callable(fn):
                continue
            try:
                await asyncio.wait_for(_call(fn), timeout=timeout_s)
            except Exception:
                pass
            break


def _scanner_modules_from_env() -> list[str] | None:
    raw = _envs("SCANNER_MODULES", "").strip()
    if not raw:
        return None
    parts = [p.strip() for p in raw.replace(";", ",").split(",") if p.strip()]
    return parts or None


def _pick_first_existing(cands: list[str]) -> str | None:
    for m in cands:
        if _module_exists(m):
            return m
    return None


def _default_scanner_modules() -> list[str]:
    mods: list[str] = []
    if getattr(C, "ENABLE_RAYDIUM_SCANNER", True) and _envb("ENABLE_RAYDIUM_SCANNER", True):
        mods.append(
            _pick_first_existing(
                ["scanner.raydium_scanner", "scanner.raydium", "scanner.raydium_scanner_v2"]
            )
            or "scanner.raydium_scanner"
        )
    if getattr(C, "ENABLE_PUMP_SCANNER", True) and _envb("ENABLE_PUMP_SCANNER", True):
        mods.append(
            _pick_first_existing(
                ["scanner.pumpfun_scanner", "scanner.pumpfun", "scanner.pump_scanner"]
            )
            or "scanner.pumpfun_scanner"
        )
    if getattr(C, "ENABLE_DEXSCREENER_SCANNER", True) and _envb("ENABLE_DEXSCREENER_SCANNER", True):
        mods.append(
            _pick_first_existing(["scanner.dexscreener_scanner", "scanner.dexscreener"])
            or "scanner.dexscreener_scanner"
        )
    return mods


async def _run_loop_until_stop(
    loop_fn: Callable[..., Any], stop_evt: asyncio.Event, *, name: str
) -> None:
    async def _start_inner() -> Any:
        kw = _filter_kwargs(
            loop_fn, {"stop_evt": stop_evt, "stop_event": stop_evt, "stop": stop_evt}
        )
        if kw:
            return await _call_maybe_async(loop_fn, **kw)
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


def _task_view_from_supervisor(sup: Any) -> JSONDict:
    view: JSONDict = {}
    try:
        rts = getattr(sup, "_runtimes", {}) or {}
        if isinstance(rts, dict):
            for name, rt in rts.items():
                t = getattr(rt, "task", None)
                view[str(name)] = {
                    "done": int(t.done()) if t else None,
                    "cancelled": int(t.cancelled()) if t else None,
                    "restarts": int(getattr(rt, "restarts", 0) or 0),
                    "last_exc_type": getattr(rt, "last_exc_type", None),
                    "last_error": getattr(rt, "last_error", None),
                }
    except Exception:
        return {}
    return view


# =============================================================================
# Main (orchestrator)
# =============================================================================
async def main(stop_evt: asyncio.Event | None = None) -> None:
    global RUN_ID

    _refresh_config()
    refresh_runtime_constants()

    applied_mode = _apply_run_mode_defaults()

    if not RUN_ID or _envb("MAIN_FORCE_NEW_RUN_ID", True):
        base = f"{time.time()}|{os.getpid()}|{socket.gethostname()}|{random.random()}"
        RUN_ID = (os.getenv("RUN_ID", "") or _hash_prefix(base, 10)).strip()

    if not _jitter_disabled():
        with contextlib.suppress(Exception):
            seed = int(_hash_prefix(RUN_ID, 8), 16)
            random.seed(seed)

    stop_evt = stop_evt or asyncio.Event()
    _install_signal_handlers(stop_evt, RUN_ID)
    _apply_disable_sells_to_env()

    # Create runtime queues/state ONCE (shared by early HTTP + supervisor runtime).
    qmax = _envi("CANDIDATE_QUEUE_MAX", int(getattr(C, "CANDIDATE_QUEUE_MAX", 2500) or 2500))
    tqmax = _envi("TICK_QUEUE_MAX", int(getattr(C, "TICK_QUEUE_MAX", 5000) or 5000))
    qmax = max(50, min(250_000, qmax))
    tqmax = max(50, min(500_000, tqmax))

    cand_q: asyncio.Queue[CandidateMsg] = asyncio.Queue(maxsize=qmax)
    tick_q: asyncio.Queue[JSONDict] = asyncio.Queue(maxsize=tqmax)
    st = RuntimeState(run_id=RUN_ID)

    # Placeholder Modules object (mutated in-place once full imports complete).
    M = Modules(normalize_mint=_resolve_normalize_mint())

    # Task view function can be updated after supervisor exists.
    task_view_holder: dict[str, Callable[[], JSONDict]] = {"fn": (lambda: {})}

    http_task: asyncio.Task[None] | None = None
    port = int(_http_port())
    early_http = _early_http_enabled(port)
    if early_http:
        http_task = asyncio.create_task(
            _http_server(M, st, cand_q, tick_q, stop_evt, lambda: task_view_holder["fn"]()),
            name="main::http_server",
        )

    try:
        # ===== Critical correctness fix =====
        # PANIC_STOP must be clean and NOT contain unreachable statements (mypy/ruff).
        if _envb("PANIC_STOP", False):
            log.event("PANIC_STOP_SET", run_id=RUN_ID)
            stop_evt.set()
            # always attempt graceful shutdown steps, but never explode
            with contextlib.suppress(Exception):
                await _shutdown_best_effort(stop_evt, reason="panic_stop", run_id=RUN_ID)
            with contextlib.suppress(Exception):
                await _close_asyncpg_pools_best_effort(
                    run_id=RUN_ID, timeout_s=float(_envf("MAIN_CLOSE_POOLS_TIMEOUT_S", 2.0))
                )
            return

        await _run_preflight_if_enabled(RUN_ID)
        with contextlib.suppress(Exception):
            await _core_bootstrap_best_effort(RUN_ID)
        with contextlib.suppress(Exception):
            await _ai_db_bootstrap_best_effort(RUN_ID)

        # Hydrate modules (in-place) AFTER preflight/bootstrap.
        resolved = _resolve_modules()
        _copy_modules(M, resolved)

        phase_ = _phase()
        disable_sells = _sells_disabled()

        max_concurrent_buys = max(1, _envi("MAX_CONCURRENT_BUYS", _envi("MAX_CONCURRENCY", 3)))
        buy_sema = asyncio.Semaphore(max_concurrent_buys)

        max_uptime_sec = max(0.0, float(_envf("MAIN_MAX_UPTIME_SEC", 0.0)))

        log.event(
            "MAIN_BOOT",
            run_id=RUN_ID,
            version=MAIN_VERSION,
            build=MAIN_BUILD,
            phase=phase_,
            run_mode=applied_mode or (_run_mode() or ""),
            py=sys.version.split()[0],
            pid=os.getpid(),
            buy_executor=int(M.maybe_buy is not None),
            scoring_service=int(M.scoring_fn is not None and M.scoring_enabled),
            ai_risk_model=int(
                M.ai_risk_enabled and M.ai_risk_gate_fn is not None and M.ai_overlay_fn is not None
            ),
            infra_guard=int(M.infra_guard_mod is not None),
            circuit_breakers=int(M.circuit_breakers_mod is not None),
            ai_gate_legacy=int(M.ai_pass_gate is not None),
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
            jitter_disabled=int(_jitter_disabled()),
            max_uptime_sec=int(max_uptime_sec),
            auto_exit_enabled=int(_auto_exit_enabled()),
            http_port=int(port),
            early_http=int(early_http),
        )

        if phase_ <= 0:
            log.event("MAIN_PHASE0_IDLE", run_id=RUN_ID)
            await stop_evt.wait()
            return

        sup_mod = _soft_import("utils_pkg.task_supervisor", critical=False)
        if not sup_mod:
            if _fast_fail():
                raise RuntimeError("utils_pkg.task_supervisor missing")
            log.event("TASK_SUPERVISOR_MISSING_FALLBACK", run_id=RUN_ID)
            await stop_evt.wait()
            return

        TaskSupervisor = getattr(sup_mod, "TaskSupervisor", None)
        TS_TaskSpec = getattr(sup_mod, "TaskSpec", None)
        RestartPolicy = getattr(sup_mod, "RestartPolicy", None)
        RestartConfig = getattr(sup_mod, "RestartConfig", None)
        install_sig = getattr(sup_mod, "install_signal_handlers", None)

        if (
            (not callable(TaskSupervisor))
            or (not callable(TS_TaskSpec))
            or RestartPolicy is None
            or RestartConfig is None
        ):
            if _fast_fail():
                raise RuntimeError("task_supervisor API mismatch")
            log.event("TASK_SUPERVISOR_API_MISMATCH", run_id=RUN_ID)
            await stop_evt.wait()
            return

        sup = TaskSupervisor(
            stop_event=stop_evt, logger=_base_logger("sniper.supervisor"), name="main_supervisor"
        )

        with contextlib.suppress(Exception):
            if callable(install_sig):
                loop_ = asyncio.get_running_loop()
                install_sig(loop_, sup)

        restart_cfg_unlimited = RestartConfig(max_restarts=None)

        def _wrap_supervised_task(
            task_name: str, coro_factory: Callable[[], Awaitable[None]]
        ) -> Callable[[], Awaitable[None]]:
            """
            Ensures a supervised task never "silently completes" while the system is still running.
            If a task returns while stop_evt is not set -> treat as failure so supervisor restarts it.
            """

            async def _runner() -> None:
                try:
                    await coro_factory()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    raise

                # Normal completion is only allowed during shutdown.
                if stop_evt.is_set():
                    return

                raise RuntimeError(f"task_returned_unexpectedly:{task_name}")

            return _runner

        def add_task(
            name: str,
            coro_factory: Callable[[], Awaitable[None]],
            *,
            essential: bool,
            restart_policy: str,
            tags: JSONDict | None = None,
            shutdown_timeout_s: float = 15.0,
        ) -> None:
            wrapped = _wrap_supervised_task(name, coro_factory)
            spec = TS_TaskSpec(
                name=name,
                coro_factory=wrapped,
                restart_policy=restart_policy,
                restart=restart_cfg_unlimited,
                essential=essential,
                shutdown_timeout_s=float(shutdown_timeout_s),
                tags=tags or {},
            )
            sup.add(spec)

        def task_view_fn() -> JSONDict:
            return _task_view_from_supervisor(sup)

        task_view_holder["fn"] = task_view_fn

        # HTTP server: if early-started it, do NOT bind a 2nd copy.
        # Also: do not create a supervised task if HTTP is effectively disabled (port=0).
        if (not early_http) and int(_http_port()) > 0 and (not _envb("MAIN_HTTP_DISABLED", False)):
            add_task(
                "http_server",
                lambda: _http_server(M, st, cand_q, tick_q, stop_evt, task_view_fn),
                essential=False,
                restart_policy=RestartPolicy.ON_FAILURE,
                tags={"svc": "http"},
                shutdown_timeout_s=5.0,
            )

        # scanners
        if phase_ >= 1:
            mods = _scanner_modules_from_env() or _default_scanner_modules()
            log.event("SCANNERS_SELECTED", run_id=RUN_ID, modules=",".join(mods) if mods else "")
            for mod_name in mods or []:
                # Default: scanners are NOT essential (they must never take the worker down).
                crit_default = _envb("SCANNER_TASKS_ESSENTIAL_DEFAULT", False)
                crit = _envb(
                    f"SCANNER_CRITICAL__{mod_name.replace('.', '_').upper()}", crit_default
                )
                add_task(
                    f"scanner::{mod_name}",
                    lambda m=mod_name: _scanner_run_module(M, m, cand_q, tick_q, stop_evt, st),
                    essential=bool(crit),
                    restart_policy=RestartPolicy.ON_FAILURE,
                    tags={"svc": "scanner", "module": mod_name},
                    shutdown_timeout_s=10.0,
                )

        # entry workers OR candidate sink
        entry_workers_started = 0
        if phase_ >= 2:
            nworkers = int(getattr(C, "ENTRY_WORKERS", 2) or 2)
            nworkers = max(1, min(32, _envi("ENTRY_WORKERS", nworkers)))
            if M.maybe_buy is not None:
                for wid in range(nworkers):
                    add_task(
                        f"entry_worker_{wid}",
                        lambda wid=wid: _entry_worker(M, wid, cand_q, stop_evt, st, buy_sema),
                        essential=True,
                        restart_policy=RestartPolicy.ON_FAILURE,
                        tags={"svc": "entry", "wid": wid},
                        shutdown_timeout_s=15.0,
                    )
                entry_workers_started = nworkers
                log.event("ENTRY_WORKERS_STARTED", run_id=RUN_ID, n=nworkers)
            else:
                log.event(
                    "ENTRY_WORKERS_SKIPPED_NO_BUY_EXECUTOR", run_id=RUN_ID, requested=nworkers
                )
                if _fast_fail():
                    raise RuntimeError("No buy executor; cannot run entry workers")

        if phase_ >= 1 and entry_workers_started <= 0:
            add_task(
                "candidate_sink",
                lambda: _candidate_sink(cand_q, stop_evt, st),
                essential=False,
                restart_policy=RestartPolicy.ON_FAILURE,
                tags={"svc": "sink", "kind": "candidate"},
                shutdown_timeout_s=10.0,
            )

        # tick + exit wiring
        if phase_ >= 3:
            add_task(
                "tick_router",
                lambda: _tick_router(M, tick_q, stop_evt, st),
                essential=True,
                restart_policy=RestartPolicy.ON_FAILURE,
                tags={"svc": "tick"},
                shutdown_timeout_s=10.0,
            )

            if M.exit_mod is not None and _envb("EXIT_ENGINE_WORKER_ENABLED", True):

                async def _run_exit_engine() -> None:
                    mod = M.exit_mod
                    for nm in ("run_worker", "worker_loop", "loop", "run", "main"):
                        fn = getattr(mod, nm, None)
                        if callable(fn):
                            kw = _filter_kwargs(
                                fn, {"stop_evt": stop_evt, "stop_event": stop_evt, "stop": stop_evt}
                            )
                            if kw:
                                return await _call_possibly_blocking(fn, **kw)
                            with contextlib.suppress(TypeError):
                                return await _call_possibly_blocking(fn, stop_evt)
                            return await _call_possibly_blocking(fn)
                    await stop_evt.wait()

                add_task(
                    "exit_engine",
                    _run_exit_engine,
                    essential=False,
                    restart_policy=RestartPolicy.ON_FAILURE,
                    tags={"svc": "exit"},
                    shutdown_timeout_s=15.0,
                )

            if (not disable_sells) and _auto_exit_enabled() and M.auto_exit_worker_loop is not None:
                add_task(
                    "auto_exit_worker",
                    lambda: _run_loop_until_stop(
                        M.auto_exit_worker_loop, stop_evt, name="auto_exit_worker"
                    ),
                    essential=False,
                    restart_policy=RestartPolicy.ON_FAILURE,
                    tags={"svc": "auto_exit"},
                    shutdown_timeout_s=15.0,
                )
            elif disable_sells:
                log.event("AUTO_EXIT_WORKER_SKIPPED_DISABLE_SELLS", run_id=RUN_ID)
            else:
                log.event(
                    "AUTO_EXIT_WORKER_NOT_AVAILABLE",
                    run_id=RUN_ID,
                    enabled=int(_auto_exit_enabled()),
                )
        else:
            if _envb("FORWARD_TICKS_FROM_SCANNERS", False):
                add_task(
                    "tick_sink",
                    lambda: _tick_sink(tick_q, stop_evt, st),
                    essential=False,
                    restart_policy=RestartPolicy.ON_FAILURE,
                    tags={"svc": "sink", "kind": "tick"},
                    shutdown_timeout_s=10.0,
                )

        # optional services
        if phase_ >= 4 and M.reinvest_mod is not None and _envb("REINVESTOR_ENABLED", True):

            async def _run_reinvestor() -> None:
                mod = M.reinvest_mod
                for nm in ("run_worker", "worker_loop", "loop", "run", "main"):
                    fn = getattr(mod, nm, None)
                    if callable(fn):
                        kw = _filter_kwargs(
                            fn, {"stop_evt": stop_evt, "stop_event": stop_evt, "stop": stop_evt}
                        )
                        if kw:
                            return await _call_possibly_blocking(fn, **kw)
                        with contextlib.suppress(TypeError):
                            return await _call_possibly_blocking(fn, stop_evt)
                        return await _call_possibly_blocking(fn)
                await stop_evt.wait()

            add_task(
                "reinvestor",
                _run_reinvestor,
                essential=False,
                restart_policy=RestartPolicy.ON_FAILURE,
                tags={"svc": "reinvestor"},
                shutdown_timeout_s=15.0,
            )

        if phase_ >= 5 and M.smart_mod is not None and _envb("SMART_WALLETS_ENABLED", True):

            async def _run_smart_wallets() -> None:
                mod = M.smart_mod
                for nm in ("run_worker", "worker_loop", "loop", "run", "main"):
                    fn = getattr(mod, nm, None)
                    if callable(fn):
                        kw = _filter_kwargs(
                            fn, {"stop_evt": stop_evt, "stop_event": stop_evt, "stop": stop_evt}
                        )
                        if kw:
                            return await _call_possibly_blocking(fn, **kw)
                        with contextlib.suppress(TypeError):
                            return await _call_possibly_blocking(fn, stop_evt)
                        return await _call_possibly_blocking(fn)
                await stop_evt.wait()

            add_task(
                "smart_wallets",
                _run_smart_wallets,
                essential=False,
                restart_policy=RestartPolicy.ON_FAILURE,
                tags={"svc": "smart_wallets"},
                shutdown_timeout_s=15.0,
            )

        # watchdogs
        if _envb("MAIN_WATCHDOG_ENABLED", True):
            add_task(
                "runtime_watchdog",
                lambda: _runtime_watchdog(
                    M,
                    st,
                    cand_q=cand_q,
                    tick_q=tick_q,
                    stop_evt=stop_evt,
                    task_view_fn=task_view_fn,
                ),
                essential=True,
                restart_policy=RestartPolicy.ON_FAILURE,
                tags={"svc": "watchdog"},
                shutdown_timeout_s=10.0,
            )

        add_task(
            "kill_watchdog",
            lambda: _kill_watchdog(stop_evt, st),
            essential=True,
            restart_policy=RestartPolicy.ON_FAILURE,
            tags={"svc": "kill"},
            shutdown_timeout_s=5.0,
        )

        if max_uptime_sec > 0:
            add_task(
                "uptime_monitor",
                lambda: _uptime_monitor(stop_evt, st, max_uptime_sec),
                essential=False,
                restart_policy=RestartPolicy.ON_FAILURE,
                tags={"svc": "uptime"},
                shutdown_timeout_s=3.0,
            )

        log.event(
            "MAIN_STARTED",
            run_id=RUN_ID,
            phase=phase_,
            tasks=len(getattr(sup, "_runtimes", {}) or {}),
            cand_q_max=qmax,
            tick_q_max=tqmax,
        )

        try:
            await sup.run()
            # If the supervisor returns while we are not stopping, treat as a crash.
            if not stop_evt.is_set():
                log.event("SUPERVISOR_RETURNED_WITHOUT_STOP", run_id=RUN_ID)
                raise RuntimeError("supervisor_returned_without_stop")
        finally:
            log.event("MAIN_SHUTDOWN_BEGIN", run_id=RUN_ID)
            stop_evt.set()
            await _shutdown_best_effort(stop_evt, reason="main_shutdown", run_id=RUN_ID)
            await _close_asyncpg_pools_best_effort(
                run_id=RUN_ID, timeout_s=float(_envf("MAIN_CLOSE_POOLS_TIMEOUT_S", 2.0))
            )
            log.event("MAIN_SHUTDOWN_DONE", run_id=RUN_ID)

    finally:
        if http_task is not None:
            http_task.cancel()
            with contextlib.suppress(Exception):
                await asyncio.gather(http_task, return_exceptions=True)


# =============================================================================
# Compatibility aliases
# =============================================================================
async def run(stop_evt: asyncio.Event | None = None) -> None:
    await main(stop_evt)


async def loop(stop_evt: asyncio.Event | None = None) -> None:
    await main(stop_evt)


# =============================================================================
# Strategy back-compat
# =============================================================================
def _resolve_external_strategy_class() -> type | None:
    spec = _envs("STRATEGY_CLASS", "").strip()
    candidates: list[str] = []
    if spec:
        candidates.append(spec)
    candidates += [
        "sniper.sniper:Strategy",
        "sniper.strategy:Strategy",
        "sniper.sniper_bot:Strategy",
        "sniper.sniper_bot:SniperBot",
        "sniper.core:Strategy",
    ]

    for c in candidates:
        if ":" in c:
            mod_name, cls_name = c.split(":", 1)
        else:
            mod_name, cls_name = c, "Strategy"
        try:
            if not _module_exists(mod_name):
                continue
            m = importlib.import_module(mod_name)
            cls = getattr(m, cls_name, None)
            if inspect.isclass(cls):
                return cls
        except Exception:
            continue
    return None


def _fire_and_forget_or_run(res: Any) -> Any:
    if not inspect.isawaitable(res):
        return res
    try:
        loop_ = asyncio.get_running_loop()
        return loop_.create_task(res)  # type: ignore[return-value]
    except RuntimeError:
        return asyncio.run(res)


class Strategy:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._impl: Any = None
        cls = _resolve_external_strategy_class()
        if cls is not None:
            try:
                self._impl = cls(*args, **kwargs)
            except Exception as e:
                raise RuntimeError(
                    f"Failed to init external Strategy {cls}: {_short_exc(e)}"
                ) from e

    def __getattr__(self, name: str) -> Any:
        if self._impl is not None:
            return getattr(self._impl, name)
        raise AttributeError(name)

    async def run(self, stop_evt: asyncio.Event | None = None) -> None:
        if self._impl is not None and hasattr(self._impl, "run"):
            out = self._impl.run(stop_evt)  # type: ignore[misc]
            if inspect.isawaitable(out):
                await out
            return
        await main(stop_evt)

    async def run_forever(self) -> None:
        await run_forever()

    def _call_first(self, names: list[str], *args: Any, **kwargs: Any) -> Any:
        if self._impl is None:
            return None
        for nm in names:
            fn = getattr(self._impl, nm, None)
            if callable(fn):
                try:
                    res = fn(*args, **kwargs)
                    return _fire_and_forget_or_run(res)
                except TypeError:
                    try:
                        res = fn(*args)
                        return _fire_and_forget_or_run(res)
                    except Exception:
                        continue
                except Exception:
                    continue
        return None

    def handle(self, *args: Any, **kwargs: Any) -> Any:
        return self._call_first(
            ["handle", "on_event", "handle_event", "process", "dispatch"], *args, **kwargs
        )

    def handle_event(self, *args: Any, **kwargs: Any) -> Any:
        return self._call_first(["handle_event", "on_event", "handle", "dispatch"], *args, **kwargs)

    def on_event(self, *args: Any, **kwargs: Any) -> Any:
        return self._call_first(["on_event", "handle_event", "handle"], *args, **kwargs)

    def on_candidate(self, *args: Any, **kwargs: Any) -> Any:
        return self._call_first(
            ["on_candidate", "handle_candidate", "candidate", "on_pool", "on_signal"],
            *args,
            **kwargs,
        )

    def handle_candidate(self, *args: Any, **kwargs: Any) -> Any:
        return self._call_first(
            ["handle_candidate", "on_candidate", "candidate", "on_pool", "on_signal"],
            *args,
            **kwargs,
        )

    def on_tick(self, *args: Any, **kwargs: Any) -> Any:
        return self._call_first(
            ["on_tick", "handle_tick", "tick", "on_trade_tick"], *args, **kwargs
        )

    def handle_tick(self, *args: Any, **kwargs: Any) -> Any:
        return self._call_first(
            ["handle_tick", "on_tick", "tick", "on_trade_tick"], *args, **kwargs
        )

    def shutdown(self, *args: Any, **kwargs: Any) -> Any:
        return self._call_first(["shutdown", "close", "stop"], *args, **kwargs)

    async def start(self, *args: Any, **kwargs: Any) -> None:
        if self._impl is not None:
            for nm in ("start", "start_async", "run"):
                fn = getattr(self._impl, nm, None)
                if callable(fn):
                    out = fn(*args, **kwargs)
                    if inspect.isawaitable(out):
                        await out
                    return
        await main(kwargs.get("stop_evt") if "stop_evt" in kwargs else None)


StrategyHandler = Strategy
Handler = Strategy


# =============================================================================
# Build contract surface (safe-mode; tests)
# =============================================================================
def build_app(*, safe: bool = True, **kwargs: Any) -> dict[str, Any]:
    contracts: Any = {}
    if safe:
        os.environ.setdefault("SAFE_MODE", "1")
        os.environ.setdefault("DRYRUN", "1")

    try:
        mod = importlib.import_module("utils_pkg")
        fn = getattr(mod, "build_contracts_surface", None)
        if callable(fn):
            contracts = fn()
        else:
            contracts = {"ok": False, "reason": "utils_pkg.build_contracts_surface missing"}
    except Exception as e:
        contracts = {"ok": False, "reason": f"build_contracts_surface import fail: {_short_exc(e)}"}

    return {"safe": bool(safe), "contracts": contracts, "kwargs": dict(kwargs)}


def build_runtime(*, safe: bool = True, **kwargs: Any) -> dict[str, Any]:
    return build_app(safe=safe, **kwargs)


def build_system(*, safe: bool = True, **kwargs: Any) -> dict[str, Any]:
    return build_app(safe=safe, **kwargs)


def build_services(*, safe: bool = True, **kwargs: Any) -> dict[str, Any]:
    return build_app(safe=safe, **kwargs)


def contracts_surface(*, safe: bool = True) -> Any:
    return build_app(safe=safe).get("contracts")


def get_contracts_surface(*, safe: bool = True) -> Any:
    return contracts_surface(safe=safe)


# =============================================================================
# Crash-resilient runner (LOCAL / non-start.sh contexts)
# =============================================================================
async def run_forever() -> None:
    is_render_ = _is_render()
    restart_default = (not is_render_) and (_phase() >= 7)
    # On Render, default to self-heal on crash unless explicitly disabled.
    if is_render_ and os.getenv("MAIN_INNER_RESTART_ON_CRASH") is None:
        restart_default = True
    restart = _envb("MAIN_INNER_RESTART_ON_CRASH", restart_default)

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


# =============================================================================
# Module-level helper aliases (tests)
# =============================================================================
def get_run_id() -> str:
    return RUN_ID


def set_run_id(run_id: str) -> None:
    global RUN_ID
    RUN_ID = (run_id or "").strip()


def get_version() -> str:
    return MAIN_VERSION


def get_build() -> str:
    return MAIN_BUILD


def phase() -> int:
    return _phase()


def is_render() -> bool:
    return _is_render()


def envs(k: str, d: str = "") -> str:
    return _envs(k, d)


def envb(k: str, d: bool = False) -> bool:
    return _envb(k, d)


def envi(k: str, d: int) -> int:
    return _envi(k, d)


def envf(k: str, d: float) -> float:
    return _envf(k, d)


def now() -> float:
    return _now()


def mono() -> float:
    return _mono()


def json_dumps(x: Any, max_len: int = 250_000) -> str:
    return _json_dumps(x, max_len=max_len)


def short_exc(e: BaseException, n: int = 420) -> str:
    return _short_exc(e, n=n)


def module_exists(name: str) -> bool:
    return _module_exists(name)


def soft_import(
    mod_name: str, *, aliases: list[str] | None = None, critical: bool = False
) -> Any | None:
    return _soft_import(mod_name, aliases=aliases, critical=critical)


def ensure_windows_event_loop_policy() -> None:
    with contextlib.suppress(Exception):
        if os.name != "nt":
            return
        pref = (_envs("WINDOWS_EVENT_LOOP_POLICY", "") or "").strip().lower()
        if pref in {"selector", "windowsselector", "select"} and hasattr(
            asyncio, "WindowsSelectorEventLoopPolicy"
        ):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]
            return
        if hasattr(asyncio, "WindowsProactorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())  # type: ignore[attr-defined]


run_main = run
run_loop = loop
start = run_forever
start_main = run_forever
entrypoint = run_forever


if __name__ == "__main__":  # pragma: no cover
    ensure_windows_event_loop_policy()
    asyncio.run(run_forever())
