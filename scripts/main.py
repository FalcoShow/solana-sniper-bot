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
- Optional: auto-exit Redis worker, reinvestor, smart_wallets
- Supervisor + watchdogs + "why not trading" runbook-grade health snapshots

Hard rules:
- NO swap/build/sign tx code here.
- SAFE IMPORTS: module scope is stdlib only. (critical for unit_tests)
- Optional modules may be missing; orchestrator must not crash unless MAIN_FAST_FAIL=1.
- Respect kill/disable flags (env + file flags). Never log secrets.

Entry-point:
    python -m scripts.main

Back-compat:
- Exports `Strategy` (integration_smoke expects scripts.main.Strategy).
- Exports handler aliases on Strategy (integration_smoke expects some handler names).
- Exports module-level helper aliases (unit_tests expects some helpers to exist).
"""

__all__ = [
    # primary entrypoints
    "Strategy",
    "StrategyHandler",
    "Handler",
    "main",
    "run",
    "loop",
    "run_forever",
    # builders (unit_tests)
    "build_app",
    "build_runtime",
    "build_system",
    "build_services",
    "contracts_surface",
    "get_contracts_surface",
    # module-level helpers (unit_tests)
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
    # legacy aliases
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
# Lazy / safe optional imports (NO non-stdlib at module scope)
# =============================================================================
_ROUTER_CACHE: Any | None = None


def _get_router_module() -> Any | None:
    """Lazy import utils_pkg.router (never at module import time)."""
    global _ROUTER_CACHE
    if _ROUTER_CACHE is not None:
        return _ROUTER_CACHE
    try:
        _ROUTER_CACHE = importlib.import_module("utils_pkg.router")
    except Exception:
        _ROUTER_CACHE = None
    return _ROUTER_CACHE


class _RouterProxy:
    """
    Back-compat convenience:
    - Some code may expect scripts.main.router to exist.
    - We keep this without importing utils_pkg at module scope.
    """

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
# Identity / constants
# =============================================================================
MAIN_VERSION = "v3+++_orchestrator_lockin"
MAIN_BUILD = (
    os.getenv("GIT_COMMIT", "")[:12] or os.getenv("RENDER_GIT_COMMIT", "")[:12] or ""
).strip()

RUN_ID = ""  # set per main() invocation

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
    """
    Deterministic non-crypto hash for IDs (run_id/trace_id).
    Uses blake2s to avoid any sha1-related security lint noise.
    """
    nn = max(4, int(n))
    digest_size = max(1, min(32, (nn + 1) // 2))
    h = hashlib.blake2s(seed.encode("utf-8", "ignore"), digest_size=digest_size)
    return h.hexdigest()[:nn]


def _ema(prev: float, x: float, alpha: float = 0.12) -> float:
    if prev <= 0:
        return x
    return (1.0 - alpha) * prev + alpha * x


def _is_render() -> bool:
    if (
        os.getenv("RENDER_SERVICE_ID")
        or os.getenv("RENDER_INSTANCE_ID")
        or os.getenv("RENDER_EXTERNAL_URL")
    ):
        return True
    return _is_true(_envs("RENDER", ""))


def _fast_fail() -> bool:
    return _envb("MAIN_FAST_FAIL", False)


def _threadify_sync_modules_enabled() -> bool:
    return _envb("MAIN_THREADIFY_SYNC_MODULES", False)


def _jitter_disabled() -> bool:
    return _envb("MAIN_DISABLE_RANDOM_JITTER", False)


# =============================================================================
# Logging (stdlib-only at import time)
# =============================================================================
def install_stdlib_event() -> None:
    """
    Pure-stdlib compatibility shim:
    Adds Logger.event / Logger.every / Logger.once so older code & tests can rely on them
    without importing utils_pkg at module scope.
    """
    # Patch only once
    if (
        hasattr(logging.Logger, "event")
        and hasattr(logging.Logger, "every")
        and hasattr(logging.Logger, "once")
    ):
        return

    def _event(self: logging.Logger, name: str, **fields: Any) -> None:
        # Keep it simple + safe (no secrets handling here; higher layers can redact)
        with contextlib.suppress(Exception):
            self.info("%s %s", name, fields)

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
                    self.log(int(level), "%s %s", (msg or key), fields)
                else:
                    # type: ignore[attr-defined] - patched below
                    self.event(msg or key, **fields)  # noqa: B018
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
        # semantic sugar over every()
        return _every(self, key, sec=ttl, msg=msg, level=level, **fields)

    # Monkeypatch Logger class (stdlib-only)
    logging.Logger.event = _event  # type: ignore[assignment]
    logging.Logger.every = _every  # type: ignore[assignment]
    logging.Logger.once = _once  # type: ignore[assignment]


def ensure_event(logger: Any) -> Any:
    """
    Ensure a logger object has .event/.every/.once.
    Works for stdlib Logger and also for custom logger objects.
    """
    install_stdlib_event()

    # If it's a stdlib Logger, class patch already covers it
    if isinstance(logger, logging.Logger):
        return logger

    # Otherwise, attach per-instance fallbacks
    if not hasattr(logger, "event"):

        def _event(name: str, **fields: Any) -> None:
            with contextlib.suppress(Exception):
                logger.info("%s %s", name, fields)

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
                            logger.log(int(level), "%s %s", (msg or key), fields)
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


_LOG_SHIMS_READY = False
_LS_CACHE: dict[str, Any] = {}


def _base_logger(name: str) -> logging.Logger:
    lg = logging.getLogger(name)
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
        )
    return lg


log: Any = _base_logger("sniper.main")


def _ensure_logger_shims() -> None:
    """
    Ensure log.event(...), log.every(...), log.once(...) exist even without logging_setup.
    """
    global _LOG_SHIMS_READY
    if _LOG_SHIMS_READY:
        return

    ensure_event(log)

    if not hasattr(log, "event"):

        def _event(name: str, **fields: Any) -> None:
            with contextlib.suppress(Exception):
                log.info("%s %s", name, fields)

        log.event = _event  # type: ignore[attr-defined]

    if not hasattr(log, "every"):

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
                            log.log(int(level), "%s %s", (msg or key), fields)
                        else:
                            log.event(msg or key, **fields)  # type: ignore[attr-defined]
            except Exception:
                return

        log.every = _every  # type: ignore[attr-defined]

    if not hasattr(log, "once"):

        def _once(
            key: str,
            ttl: int = 60,
            msg: str | None = None,
            level: int | None = None,
            **fields: Any,
        ) -> None:
            # identical to every() but semantic sugar
            return log.every(key, sec=ttl, msg=msg, level=level, **fields)  # type: ignore[attr-defined]

        log.once = _once  # type: ignore[attr-defined]

    _LOG_SHIMS_READY = True


_ensure_logger_shims()


def _load_logging_setup() -> dict[str, Any]:
    """Lazy-import logging_setup (optional; non-stdlib). Never raises."""
    global _LS_CACHE
    if _LS_CACHE:
        return _LS_CACHE
    try:
        m = importlib.import_module("logging_setup")
        _LS_CACHE = {
            "get_logger": getattr(m, "get_logger", None),
            "bump": getattr(m, "bump", None),
            "start_global_heartbeat": getattr(m, "start_global_heartbeat", None),
        }
    except Exception:
        _LS_CACHE = {"get_logger": None, "bump": None, "start_global_heartbeat": None}
    return _LS_CACHE


def _maybe_upgrade_logger() -> None:
    """At runtime (inside main), upgrade to logging_setup.get_logger if present."""
    global log
    ls = _load_logging_setup()
    fn = ls.get("get_logger")
    if callable(fn):
        try:
            # try richer signatures first
            log2 = fn(
                service=_envs("PF_SERVICE", "sniper"), version=_envs("PF_VERSION", MAIN_VERSION)
            )
        except TypeError:
            try:
                log2 = fn("sniper.main")
            except TypeError:
                try:
                    log2 = fn()
                except Exception:
                    log2 = None
        except Exception:
            log2 = None
        if log2 is not None:
            log = log2
    _ensure_logger_shims()


def bump(name: str) -> None:
    ls = _load_logging_setup()
    fn = ls.get("bump")
    if callable(fn):
        with contextlib.suppress(Exception):
            fn(name)  # type: ignore[misc]


def _start_global_heartbeat() -> None:
    ls = _load_logging_setup()
    fn = ls.get("start_global_heartbeat")
    if not callable(fn):
        return

    interval = int(_envi("MAIN_HEARTBEAT_SEC", _envi("PF_HEARTBEAT_SEC", 30)))
    # flexible calling: (log, interval) is the preferred modern signature
    for attempt in (
        lambda: fn(log, interval=interval),  # type: ignore[misc]
        lambda: fn(log, interval),  # type: ignore[misc]
        lambda: fn(interval=interval),  # type: ignore[misc]
        lambda: fn(interval),  # type: ignore[misc]
        lambda: fn(),  # type: ignore[misc]
    ):
        try:
            with contextlib.suppress(Exception):
                attempt()
            return
        except TypeError:
            continue
        except Exception:
            return


# =============================================================================
# Config (lazy import; defaults safe)
# =============================================================================
class _CDefaults:
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
    os.environ["EXIT_ENGINE_WORKER_ENABLED"] = "0"


# =============================================================================
# Runtime import helpers (best effort)
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
            log.every("SOFT_IMPORT_FAIL", 30, module=name, err=_short_exc(e))
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


def _filter_kwargs_ctor(cls_or_fn: Any, kwargs: dict[str, Any]) -> dict[str, Any]:
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


async def _call_possibly_blocking(fn: Any, *args: Any, **kwargs: Any) -> Any:
    """
    - If coroutine fn: await directly
    - If sync fn and MAIN_THREADIFY_SYNC_MODULES=1: run in a thread
    - Else: call inline
    """
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


def _install_loop_exception_handler() -> None:
    with contextlib.suppress(Exception):
        loop = asyncio.get_running_loop()

        def _handler(_loop: asyncio.AbstractEventLoop, context: dict[str, Any]) -> None:
            msg = str(context.get("message") or "asyncio_exception")
            exc = context.get("exception")
            if exc:
                log.event("ASYNCIO_EXCEPTION", msg=msg, err=_short_exc(exc))
            else:
                log.event("ASYNCIO_EXCEPTION", msg=msg, context=str(context)[:1600])

        loop.set_exception_handler(_handler)


async def _cancel_and_gather(task_map: dict[str, asyncio.Task], timeout: float) -> None:
    me = asyncio.current_task()
    to_wait: list[asyncio.Task] = []
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
# Router wiring (runtime-only; import-safe)
# =============================================================================
async def _bind_router_runtime(wallet: Any, solana_client: Any, *, dryrun: bool) -> None:
    """
    Bind runtime objects to utils_pkg.router ONLY when actually running.
    Must NOT be called at import time.
    """
    rm = _get_router_module()
    if rm is None:
        return

    with contextlib.suppress(Exception):
        rm.wallet = wallet

    with contextlib.suppress(Exception):
        rm.solana_client = solana_client

    try:
        if getattr(rm, "client", None) is None:
            rm.client = solana_client
    except Exception:
        pass

    if dryrun:
        return

    if getattr(rm, "session", None) is None:
        try:
            aiohttp = importlib.import_module("aiohttp")
            rm.session = aiohttp.ClientSession()
        except Exception as e:
            log.every("AIOHTTP_MISSING_OR_FAIL", 60, err=_short_exc(e))


async def _shutdown_router_runtime() -> None:
    rm = _get_router_module()
    if rm is None:
        return
    sess = getattr(rm, "session", None)
    if sess is not None:
        with contextlib.suppress(Exception):
            await sess.close()
        with contextlib.suppress(Exception):
            rm.session = None


async def _resolve_wallet_and_client_best_effort() -> tuple[Any | None, Any | None]:
    """
    Best-effort runtime resolver for (wallet, solana_client) without hard-coupling.
    Never raises. Works across older/newer repos.
    """
    wallet = None
    client = None

    cu = _soft_import("utils_pkg.core_utils", aliases=["utils_pkg.core", "utils_pkg.bootstrap"])
    if cu:
        for attr in ("wallet", "WALLET", "keypair", "KEYPAIR"):
            with contextlib.suppress(Exception):
                v = getattr(cu, attr, None)
                if v is not None:
                    wallet = v
                    break

        for attr in ("solana_client", "SOLANA_CLIENT", "client", "CLIENT"):
            with contextlib.suppress(Exception):
                v = getattr(cu, attr, None)
                if v is not None:
                    client = v
                    break

        for fn_name in (
            "get_wallet",
            "load_wallet",
            "get_keypair",
            "load_keypair",
            "wallet_from_env",
        ):
            fn = getattr(cu, fn_name, None)
            if callable(fn) and wallet is None:
                with contextlib.suppress(Exception):
                    out = fn()
                    if inspect.isawaitable(out):
                        out = await out
                    if out is not None:
                        wallet = out
                        break

        for fn_name in (
            "get_solana_client",
            "get_client",
            "get_async_client",
            "get_client_async",
            "make_client",
        ):
            fn = getattr(cu, fn_name, None)
            if callable(fn) and client is None:
                with contextlib.suppress(Exception):
                    out = fn()
                    if inspect.isawaitable(out):
                        out = await out
                    if out is not None:
                        client = out
                        break

    return wallet, client


# =============================================================================
# Optional preflight (subprocess)
# =============================================================================
async def _run_preflight_if_enabled(run_id: str) -> None:
    def _should_run_main_preflight() -> bool:
        # Never run implicitly during tests / safe-mode
        if _envb("SAFE_MODE", False) or os.getenv("PYTEST_CURRENT_TEST"):
            return False
        # Default-on in Render; locally override via MAIN_RUN_PREFLIGHT_DEFAULT=1/0
        return _envb("MAIN_RUN_PREFLIGHT_DEFAULT", _is_render())

    if not _should_run_main_preflight():
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

    # In Render/prod, strict-by-default unless explicitly disabled.
    if _envb("PREFLIGHT_STRICT", _is_render()):
        args.append("--strict")

    # Optional extra args (comma/space/semicolon separated), e.g. "--force"
    extra = (_envs("PREFLIGHT_MAIN_ARGS", "") or "").strip()
    if extra:
        # keep it simple + robust across shells
        parts = [p.strip() for p in extra.replace(";", " ").replace(",", " ").split() if p.strip()]
        args.extend(parts)

    log.event("PREFLIGHT_START", run_id=run_id)
    try:
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
                "PREFLIGHT_STDERR",
                10,
                run_id=run_id,
                stderr=err.decode("utf-8", "ignore")[:2000],
            )
        if out:
            log.every(
                "PREFLIGHT_STDOUT",
                10,
                run_id=run_id,
                stdout=out.decode("utf-8", "ignore")[:2000],
            )

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
# Module bundle (resolved lazily in main())
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

    # BUY executor
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

    auto_exit_worker_mod = _soft_import("utils_pkg.auto_exit_worker.exit_engine", critical=False)
    auto_exit_worker_loop = (
        getattr(auto_exit_worker_mod, "worker_loop", None) if auto_exit_worker_mod else None
    )
    if not callable(auto_exit_worker_loop):
        auto_exit_worker_loop = None

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
    ui: JSONDict = field(default_factory=dict)
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
    M: Modules,
    st: RuntimeState,
    *,
    cand_q: asyncio.Queue,
    tick_q: asyncio.Queue,
    tasks: dict[str, asyncio.Task] | None = None,
) -> JSONDict:
    task_view: JSONDict = {}
    if isinstance(tasks, dict):
        with contextlib.suppress(Exception):
            task_view = {
                k: {"done": int(t.done()), "cancelled": int(t.cancelled())}
                for k, t in list(tasks.items())
            }

    dryrun_flag = _envb("DRYRUN", _envb("DRY_RUN", False))

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
        "last": {
            "cand_sec_ago": int(_now() - st.last_candidate_at) if st.last_candidate_at else -1,
            "tick_sec_ago": int(_now() - st.last_tick_at) if st.last_tick_at else -1,
            "buy_sec_ago": int(_now() - st.last_buy_action_at) if st.last_buy_action_at else -1,
            "buy_fill_sec_ago": int(_now() - st.last_buy_fill_at) if st.last_buy_fill_at else -1,
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
        self._m: OrderedDict[str, float] = OrderedDict()

    def set_params(self, *, ttl_sec: float | None = None, max_items: int | None = None) -> None:
        if ttl_sec is not None:
            self.ttl = float(ttl_sec)
        if max_items is not None:
            self.max_items = int(max_items)
        # trim immediately so changes take effect deterministically
        self._evict(_mono())

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


def refresh_runtime_constants() -> None:
    """
    Re-read env-driven tunables and refresh globals.
    Useful for:
      - tests that set env after import
      - long-lived processes where you hot-tune via env (rare but supported)
    """
    global _DROP_POLICY, _DROP_OLDEST
    global _SCORING_TIMEOUT_S, _AI_RISK_TIMEOUT_S
    global _BUY_INFLIGHT_TTL, _BUY_COOLDOWN_SEC, _BUY_EXEC_TIMEOUT_S, _BUY_WARMUP_SEC

    # Queue drop policy
    _DROP_POLICY = (
        (_envs("QUEUE_DROP_POLICY", _DROP_POLICY or "drop_newest") or "drop_newest").strip().lower()
    )
    _DROP_OLDEST = _DROP_POLICY in {"drop_oldest", "oldest", "pop_oldest", "drop-oldest"}

    # Deduper params
    try:
        _CAND_DEDUP.set_params(
            ttl_sec=_envf("CAND_DEDUP_TTL_SEC", float(getattr(_CAND_DEDUP, "ttl", 10.0))),
            max_items=_envi("CAND_DEDUP_MAX", int(getattr(_CAND_DEDUP, "max_items", 75_000))),
        )
    except Exception:
        pass

    try:
        _TICK_DEDUP.set_params(
            ttl_sec=_envf("TICK_DEDUP_TTL_SEC", float(getattr(_TICK_DEDUP, "ttl", 2.0))),
            max_items=_envi("TICK_DEDUP_MAX", int(getattr(_TICK_DEDUP, "max_items", 200_000))),
        )
    except Exception:
        pass

    # Gate timeouts
    _SCORING_TIMEOUT_S = float(_envf("SCORING_SERVICE_TIMEOUT_S", float(_SCORING_TIMEOUT_S)))
    _AI_RISK_TIMEOUT_S = float(_envf("AI_RISK_MODEL_TIMEOUT_S", float(_AI_RISK_TIMEOUT_S)))

    # Buy/execution tunables
    _BUY_INFLIGHT_TTL = float(_envf("BUY_INFLIGHT_TTL_SEC", float(_BUY_INFLIGHT_TTL)))
    _BUY_COOLDOWN_SEC = float(_envf("BUY_MINT_COOLDOWN_SEC", float(_BUY_COOLDOWN_SEC)))
    _BUY_EXEC_TIMEOUT_S = float(_envf("BUY_EXEC_TIMEOUT_S", float(_BUY_EXEC_TIMEOUT_S)))
    _BUY_WARMUP_SEC = float(_envf("BUY_WARMUP_SEC", float(_BUY_WARMUP_SEC)))


def _bump_source(
    st: RuntimeState, source: str, *, seen: bool = False, dropped: bool = False
) -> None:
    s = source or "unknown"
    if seen:
        st.per_source_seen[s] = int(st.per_source_seen.get(s, 0)) + 1
        st.per_source_last[s] = _now()
    if dropped:
        st.per_source_dropped[s] = int(st.per_source_dropped.get(s, 0)) + 1


def _trace_id_for(mint: str, source: str, ui: JSONDict, ts: float, run_id: str) -> str:
    seed = f"{mint}|{source}|{ui.get('pair') or ui.get('pairAddress') or ''}|{int(ts)}|{run_id}"
    return _hash_prefix(seed, 12)


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


def _looks_like_tick(d: JSONDict) -> bool:
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
    has_amt = any(
        k in d for k in ("amount", "amount_raw", "amount_in", "amount_out", "qty", "size")
    )
    return bool(has_price and (has_amt or kind != ""))


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
    out_q: asyncio.Queue[CandidateMsg],
    tick_q: asyncio.Queue[JSONDict],
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
    ui.setdefault("source", source)
    ui.setdefault(
        "pool", ui.get("pool") or ui.get("venue") or ui.get("dex") or _source_to_pool_hint(source)
    )
    ui.setdefault("ts", float(ui.get("ts") or ui.get("timestamp") or _now()))

    if forward_ticks and _looks_like_tick(ui):
        _enqueue_tick_nowait(M, tick_q, ui, st)

    msg = CandidateMsg(mint=mint, ui=ui, source=source, kind="candidate", ts=float(ui["ts"]))
    msg.trace_id = _trace_id_for(mint, source, ui, msg.ts, st.run_id)
    _enqueue_candidate_nowait(M, out_q, msg, st)


async def _push_payload(
    M: Modules,
    out_q: asyncio.Queue[CandidateMsg],
    tick_q: asyncio.Queue[JSONDict],
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
        await _push_payload(
            self._M, self._out_q, self._tick_q, self._source, item, self._stop, self._st
        )

    def put_nowait(self, item: Any) -> None:
        _push_payload_nowait(
            self._M, self._out_q, self._tick_q, self._source, item, self._stop, self._st
        )

    def qsize(self) -> int:
        return self._out_q.qsize()

    def empty(self) -> bool:
        return self._out_q.empty()

    @property
    def maxsize(self) -> int:
        return int(getattr(self._out_q, "maxsize", 0) or 0)


# =============================================================================
# Scanners
# =============================================================================
def _scanner_entrypoints(mod: Any) -> list[Callable[..., Any]]:
    out: list[Callable[..., Any]] = []
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

    async def _emit(payload: Any) -> None:
        await _push_payload(M, out_q, tick_q, module_name, payload, stop_evt, st)

    proxy_q = _ScannerQueueProxy(
        M=M, out_q=out_q, tick_q=tick_q, source=module_name, stop_evt=stop_evt, st=st
    )

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

    base_interval = max(0.2, float(_envf("SCAN_ONCE_INTERVAL_SEC", 2.0)))
    jitter = 0.0 if _jitter_disabled() else max(0.0, float(_envf("SCAN_ONCE_JITTER_SEC", 0.35)))

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
# Gates (pre-buy)
# =============================================================================
_SCORING_TIMEOUT_S = float(_envf("SCORING_SERVICE_TIMEOUT_S", 3.0))
_AI_RISK_TIMEOUT_S = float(_envf("AI_RISK_MODEL_TIMEOUT_S", 2.5))

# --- BEGIN gates block (functional parity; keep stable) ---


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
        if "pool" in kw or "raw_pool" in kw:

            def call():
                return fn(**kw)  # type: ignore[misc]
        else:

            def call():
                return fn(raw_pool, context=context)  # type: ignore[misc]

        if inspect.iscoroutinefunction(fn):
            out = await asyncio.wait_for(call(), timeout=_SCORING_TIMEOUT_S)
        else:
            if _threadify_sync_modules_enabled():
                out = await asyncio.wait_for(asyncio.to_thread(call), timeout=_SCORING_TIMEOUT_S)
            else:
                out = call()

        if hasattr(out, "to_dict") and callable(out.to_dict):
            st_dict = out.to_dict()  # type: ignore[misc]
        elif isinstance(out, dict):
            st_dict = dict(out)
        else:
            st_dict = {"score": str(out)}

        if not attach_features and isinstance(st_dict.get("raw_features"), dict):
            st_dict.pop("raw_features", None)

        ui["score"] = st_dict

        with contextlib.suppress(Exception):
            if "maxpos_mult" in st_dict:
                ui["maxpos_mult"] = st_dict.get("maxpos_mult")
            if "cap_delta" in st_dict:
                ui["cap_delta"] = st_dict.get("cap_delta")

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

    perf_snapshot = ui.get("perf_snapshot") if isinstance(ui.get("perf_snapshot"), dict) else {}
    regime_snapshot = (
        ui.get("regime_snapshot") if isinstance(ui.get("regime_snapshot"), dict) else {}
    )

    rm = M.risk_mod
    if (not perf_snapshot) and rm is not None:
        for nm in ("get_perf_snapshot", "perf_snapshot", "snapshot_perf"):
            fn = getattr(rm, nm, None)
            if callable(fn):
                with contextlib.suppress(Exception):
                    out = fn()
                    if inspect.isawaitable(out):
                        out = await out
                    if isinstance(out, dict):
                        perf_snapshot = out
                        break

    if (not regime_snapshot) and rm is not None:
        for nm in ("get_regime_snapshot", "regime_snapshot", "snapshot_regime"):
            fn = getattr(rm, nm, None)
            if callable(fn):
                with contextlib.suppress(Exception):
                    out = fn()
                    if inspect.isawaitable(out):
                        out = await out
                    if isinstance(out, dict):
                        regime_snapshot = out
                        break

    live: JSONDict = {}
    for k in (
        "daily_realized_pnl_pct",
        "daily_drawdown_pct",
        "risk_capacity",
        "risk_budget_util",
        "open_positions",
        "equity_usd",
    ):
        if k in ui:
            live[k] = ui.get(k)

    live_json = _envs("AI_RISK_LIVE_JSON", "").strip()
    if live_json:
        with contextlib.suppress(Exception):
            x = json.loads(live_json)
            if isinstance(x, dict):
                live.update(x)

    async def _compute_overlay() -> Any:
        fn = M.ai_overlay_fn
        assert fn is not None
        for attempt in (
            lambda: fn(live, perf_snapshot=perf_snapshot, regime_snapshot=regime_snapshot),
            lambda: fn(live, perf_snapshot, regime_snapshot),
            lambda: fn(perf_snapshot=perf_snapshot, regime_snapshot=regime_snapshot, live=live),
            lambda: fn(perf_snapshot=perf_snapshot, regime_snapshot=regime_snapshot),
            lambda: fn(live),
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
            lambda: fn(cand),
            lambda: fn(ui, overlay),
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

        with contextlib.suppress(Exception):
            ov = ui.get("ai_overlay") or {}
            if isinstance(ov, dict) and "maxpos_mult" in ov:
                ui.setdefault("maxpos_mult", ov.get("maxpos_mult"))

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


async def _routeguard_gate(M: Modules, ui: JSONDict) -> tuple[bool, str]:
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


async def _risk_gate(M: Modules, ui: JSONDict) -> tuple[bool, str]:
    if not M.risk_mod or _envb("RISK_MANAGER_DISABLED", False):
        return True, "risk_off"

    rm = M.risk_mod
    fail_closed = _envb("RISK_FAIL_CLOSED", False)

    async def _maybe_await(x: Any) -> Any:
        if inspect.isawaitable(x):
            return await x
        return x

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


# --- END gates block ---


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
# Worker runner (best-effort)
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
            kwargs = _filter_kwargs(
                fn, {"stop_evt": stop_evt, "stop_event": stop_evt, "stop": stop_evt}
            )
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


async def _run_loop_until_stop(
    loop_fn: Callable[..., Any], stop_evt: asyncio.Event, *, name: str
) -> None:
    async def _start_inner() -> Any:
        try:
            kw = _filter_kwargs(
                loop_fn, {"stop_evt": stop_evt, "stop_event": stop_evt, "stop": stop_evt}
            )
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


def _optional_log_candidate(M: Modules, ui: JSONDict) -> None:
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
        bump("buy_submitted")
        st.last_buy_action_at = _now()
        log.event("BUY_SUBMITTED", run_id=st.run_id, worker=wid, **(p or {}))

    async def _on_filled(p: JSONDict) -> None:
        bump("buy_filled")
        st.last_buy_action_at = _now()
        st.last_buy_fill_at = _now()
        log.event("BUY_FILLED", run_id=st.run_id, worker=wid, **(p or {}))
        await _exit_on_buy_filled(M, p or {})

    async def _on_failed(p: JSONDict) -> None:
        bump("buy_failed")
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

            _optional_log_candidate(M, ui)

            if _BUY_WARMUP_SEC > 0 and (_now() - started_at) < _BUY_WARMUP_SEC:
                bump("buy_skip_warmup")
                _reject(st, skip_key="buy_skip_warmup")
                log.every("BUY_SKIP_WARMUP", 10, mint=mint, warmup_sec=_BUY_WARMUP_SEC)
                continue

            if _buys_disabled():
                bump("buy_skip_disabled")
                _reject(st, skip_key="buy_skip_disabled")
                log.every(
                    "BUY_SKIP_DISABLED", 15, mint=mint, pool=ui.get("pool"), source=msg.source
                )
                continue

            if M.buys_allowed is not None:
                with contextlib.suppress(Exception):
                    if not bool(M.buys_allowed()):
                        bump("buy_skip_guardrails")
                        _reject(st, skip_key="buy_skip_guardrails")
                        log.every("BUY_SKIP_GUARDRAILS", 10, mint=mint)
                        continue

            if not _buy_inflight_ok(mint):
                bump("buy_skip_inflight")
                _reject(st, skip_key="buy_skip_inflight")
                log.every(
                    "BUY_SKIP_INFLIGHT", 10, mint=mint, pool=ui.get("pool"), source=msg.source
                )
                continue

            if not _buy_cooldown_ok(mint):
                bump("buy_skip_cooldown")
                _reject(st, skip_key="buy_skip_cooldown")
                log.every("BUY_SKIP_COOLDOWN", 10, mint=mint, cooldown_sec=_BUY_COOLDOWN_SEC)
                with contextlib.suppress(Exception):
                    _BUY_INFLIGHT.pop(mint, None)
                continue

            ok_sc, why_sc = await _scoring_gate(M, ui)
            if not ok_sc:
                bump("buy_skip_scoring")
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
                bump("buy_skip_infra_guard")
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
                bump("buy_skip_circuit_breakers")
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
                bump("buy_skip_ai_risk")
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
                bump("buy_skip_risk")
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
                bump("buy_skip_routeguard")
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
                bump("buy_skip_ai_gate")
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
                        bump("buy_dryrun_skip_noarg")
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
                bump("buy_exec_timeout")
                log.event(
                    "BUY_EXEC_TIMEOUT", run_id=st.run_id, mint=mint, timeout_s=_BUY_EXEC_TIMEOUT_S
                )

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
    restarts_window: list[float] = field(default_factory=list)


async def _supervisor_loop(
    M: Modules,
    specs: dict[str, TaskSpec],
    tasks: dict[str, asyncio.Task],
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

    state_dump_file = _envs("MAIN_STATE_DUMP_FILE", "").strip()

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

            err: BaseException | None = None
            with contextlib.suppress(Exception):
                err = t.exception()

            tasks.pop(name, None)
            spec = specs.get(name)
            log.event(
                "TASK_DIED",
                run_id=st.run_id,
                task=name,
                err=("done" if err is None else _short_exc(err)),
            )

            if stop_evt.is_set() or not spec or not spec.critical:
                continue

            if len(spec.restarts_window) >= max_per_hour:
                log.event(
                    "TASK_RESTART_RATE_LIMIT", run_id=st.run_id, task=name, per_hour=max_per_hour
                )
                continue

            spec.backoff = min(
                max_backoff,
                max(base_backoff, spec.backoff * 1.7 if spec.backoff else base_backoff),
            )
            jitter = (
                0.0 if _jitter_disabled() else random.uniform(0.0, min(1.0, spec.backoff * 0.15))
            )
            wait = spec.backoff + jitter
            log.event(
                "TASK_RESTART_SCHEDULED", run_id=st.run_id, task=name, backoff_sec=round(wait, 2)
            )

            with contextlib.suppress(asyncio.TimeoutError, TimeoutError):
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

            if state_dump_file:
                with contextlib.suppress(Exception):
                    tmp = state_dump_file + ".tmp"
                    with open(tmp, "w", encoding="utf-8") as f:
                        f.write(_json_dumps(snap, max_len=1_000_000))
                    os.replace(tmp, state_dump_file)


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
# Optional stdlib HTTP debug server
# =============================================================================
async def _http_server(
    M: Modules,
    st: RuntimeState,
    cand_q: asyncio.Queue,
    tick_q: asyncio.Queue,
    tasks: dict[str, asyncio.Task],
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
            resp = (
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s"
                % (len(body), body)
            )
        elif path in {"/state", "/debug/state"}:
            body = _json_dumps(_snapshot_state(M, st, cand_q=cand_q, tick_q=tick_q, tasks=tasks))
            resp = (
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s"
                % (len(body), body)
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
# Scanner module selection (+ optional registry wiring)
# =============================================================================
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
    # Optional: defer to scanner.registry when explicitly enabled
    if _envb("SCANNER_USE_REGISTRY", False) and _module_exists("scanner.registry"):
        with contextlib.suppress(Exception):
            reg = importlib.import_module("scanner.registry")
            fn = getattr(reg, "default_scanner_modules", None)
            if callable(fn):
                out = fn()  # type: ignore[misc]
                if isinstance(out, list) and all(isinstance(x, str) for x in out):
                    return out

    mods: list[str] = []
    if getattr(C, "ENABLE_RAYDIUM_SCANNER", True) and _envb("ENABLE_RAYDIUM_SCANNER", True):
        ray = _pick_first_existing(
            ["scanner.raydium_scanner", "scanner.raydium", "scanner.raydium_scanner_v2"]
        )
        mods.append(ray or "scanner.raydium_scanner")
    if getattr(C, "ENABLE_PUMP_SCANNER", True) and _envb("ENABLE_PUMP_SCANNER", True):
        pump = _pick_first_existing(
            ["scanner.pumpfun_scanner", "scanner.pumpfun", "scanner.pump_scanner"]
        )
        mods.append(pump or "scanner.pumpfun_scanner")
    if getattr(C, "ENABLE_DEXSCREENER_SCANNER", True) and _envb("ENABLE_DEXSCREENER_SCANNER", True):
        dex = _pick_first_existing(["scanner.dexscreener_scanner", "scanner.dexscreener"])
        mods.append(dex or "scanner.dexscreener_scanner")
    return mods


# =============================================================================
# Main
# =============================================================================
async def main(stop_evt: asyncio.Event | None = None) -> None:
    global RUN_ID, log

    _refresh_config()
    _maybe_upgrade_logger()

    # ---- log_compat patch (runtime-only; after logger creation) ----
    with contextlib.suppress(Exception):
        lc = importlib.import_module("utils_pkg.log_compat")
        for nm in (
            "patch_logger",
            "patch",
            "install",
            "apply",
            "setup",
            "ensure_compat",
            "ensure",
        ):
            fn = getattr(lc, nm, None)
            if not callable(fn):
                continue

            # support a few common signatures: (log=...), (logger=...), (name=...), (log), or ()
            kw = _filter_kwargs(fn, {"log": log, "logger": log, "name": "sniper.main"})
            try:
                out = fn(**kw) if kw else fn(log)  # type: ignore[misc]
            except TypeError:
                out = fn()  # type: ignore[misc]

            # if log_compat returns a wrapped logger, adopt it
            if out is not None:
                log = out
            break

    _ensure_logger_shims()
    refresh_runtime_constants()

    if not RUN_ID or _envb("MAIN_FORCE_NEW_RUN_ID", True):
        base = f"{time.time()}|{os.getpid()}|{socket.gethostname()}|{random.random()}"
        RUN_ID = (os.getenv("RUN_ID", "") or _hash_prefix(base, 10)).strip()

    if not _jitter_disabled():
        with contextlib.suppress(Exception):
            seed = int(_hash_prefix(RUN_ID, 8), 16)
            random.seed(seed)

    stop_evt = stop_evt or asyncio.Event()
    _install_signal_handlers(stop_evt, RUN_ID)
    with contextlib.suppress(Exception):
        _install_loop_exception_handler()

    _apply_disable_sells_to_env()

    if _envb("PANIC_STOP", False):
        log.event("PANIC_STOP_SET", run_id=RUN_ID)
        stop_evt.set()

    await _run_preflight_if_enabled(RUN_ID)

    with contextlib.suppress(Exception):
        await _core_bootstrap_best_effort(RUN_ID)
    with contextlib.suppress(Exception):
        await _ai_db_bootstrap_best_effort(RUN_ID)

    M = _resolve_modules()

    if _sells_disabled():
        _reload_exit_engine_config_best_effort(M)

    phase_ = _phase()
    disable_sells = _sells_disabled()

    max_concurrent_buys = max(1, _envi("MAX_CONCURRENT_BUYS", _envi("MAX_CONCURRENCY", 3)))
    buy_sema = asyncio.Semaphore(max_concurrent_buys)

    max_uptime_sec = max(0.0, float(_envf("MAIN_MAX_UPTIME_SEC", 0.0)))  # 0 disables

    log.event(
        "MAIN_BOOT",
        run_id=RUN_ID,
        version=MAIN_VERSION,
        build=MAIN_BUILD,
        phase=phase_,
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
    )

    _start_global_heartbeat()

    if phase_ <= 0:
        log.event("MAIN_PHASE0_IDLE", run_id=RUN_ID)
        await stop_evt.wait()
        return

    dryrun_flag = _envb("DRYRUN", _envb("DRY_RUN", False))

    wallet, solana_client = await _resolve_wallet_and_client_best_effort()
    if wallet is not None and solana_client is not None:
        await _bind_router_runtime(wallet, solana_client, dryrun=dryrun_flag)
        log.event("ROUTER_BOUND", run_id=RUN_ID, dryrun=int(dryrun_flag))
    else:
        log.every(
            "ROUTER_BIND_SKIPPED",
            30,
            run_id=RUN_ID,
            have_wallet=int(wallet is not None),
            have_client=int(solana_client is not None),
            dryrun=int(dryrun_flag),
        )

    qmax = _envi("CANDIDATE_QUEUE_MAX", int(getattr(C, "CANDIDATE_QUEUE_MAX", 2500) or 2500))
    tqmax = _envi("TICK_QUEUE_MAX", int(getattr(C, "TICK_QUEUE_MAX", 5000) or 5000))
    qmax = max(50, min(250_000, qmax))
    tqmax = max(50, min(500_000, tqmax))

    cand_q: asyncio.Queue[CandidateMsg] = asyncio.Queue(maxsize=qmax)
    tick_q: asyncio.Queue[JSONDict] = asyncio.Queue(maxsize=tqmax)
    st = RuntimeState(run_id=RUN_ID)

    tasks: dict[str, asyncio.Task] = {}
    specs: dict[str, TaskSpec] = {}

    def _spawn(
        name: str, coro_factory: Callable[[], Awaitable[None]], critical: bool = True
    ) -> None:
        if name in tasks and not tasks[name].done():
            return
        specs[name] = TaskSpec(
            name=name, factory=coro_factory, critical=critical, last_start=_now()
        )
        tasks[name] = asyncio.create_task(coro_factory(), name=name)

    _spawn(
        "http_server", lambda: _http_server(M, st, cand_q, tick_q, tasks, stop_evt), critical=False
    )

    if phase_ >= 1:
        mods = _scanner_modules_from_env() or _default_scanner_modules()
        log.event("SCANNERS_SELECTED", run_id=RUN_ID, modules=",".join(mods) if mods else "")
        for mod_name in mods or []:
            crit = _envb(f"SCANNER_CRITICAL__{mod_name.replace('.', '_').upper()}", True)
            _spawn(
                f"scanner::{mod_name}",
                lambda m=mod_name: _scanner_run_module(M, m, cand_q, tick_q, stop_evt, st),
                critical=crit,
            )

    entry_workers_started = 0
    if phase_ >= 2:
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

    if phase_ >= 1 and entry_workers_started <= 0:
        _spawn("candidate_sink", lambda: _candidate_sink(cand_q, stop_evt, st), critical=False)

    if phase_ >= 3:
        _spawn("tick_router", lambda: _tick_router(M, tick_q, stop_evt, st), critical=True)

        if M.exit_mod is not None and _envb("EXIT_ENGINE_WORKER_ENABLED", True):
            _spawn(
                "exit_engine",
                lambda: _run_module_worker(M.exit_mod, stop_evt, "EXIT"),
                critical=False,
            )

        if (
            (not disable_sells)
            and _envb("AUTO_EXIT_WORKER_ENABLED", True)
            and M.auto_exit_worker_loop is not None
        ):
            _spawn(
                "auto_exit_worker",
                lambda: _run_loop_until_stop(
                    M.auto_exit_worker_loop, stop_evt, name="auto_exit_worker"
                ),
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

    if phase_ >= 4 and M.reinvest_mod is not None and _envb("REINVESTOR_ENABLED", True):
        _spawn(
            "reinvestor",
            lambda: _run_module_worker(M.reinvest_mod, stop_evt, "REINVEST"),
            critical=False,
        )

    if phase_ >= 5 and M.smart_mod is not None and _envb("SMART_WALLETS_ENABLED", True):
        _spawn(
            "smart_wallets",
            lambda: _run_module_worker(M.smart_mod, stop_evt, "SMARTWALLETS"),
            critical=False,
        )

    supervisor_needed = (phase_ >= 6) or _envb("SUPERVISOR_ALWAYS", False)
    if supervisor_needed:
        _spawn(
            "supervisor",
            lambda: _supervisor_loop(M, specs, tasks, stop_evt, st, cand_q=cand_q, tick_q=tick_q),
            critical=True,
        )
        _spawn("kill_watchdog", lambda: _kill_watchdog(stop_evt, st), critical=True)

    log.event(
        "MAIN_STARTED",
        run_id=RUN_ID,
        phase=phase_,
        tasks=len(tasks),
        cand_q_max=qmax,
        tick_q_max=tqmax,
    )

    idle_sleep = float(getattr(C, "MAIN_IDLE_SLEEP_SEC", 0.5) or 0.5)
    shutdown_grace = float(
        _envf("SHUTDOWN_GRACE_SEC", float(getattr(C, "SHUTDOWN_GRACE_SEC", 15.0) or 15.0))
    )

    try:
        while not stop_evt.is_set():
            if max_uptime_sec and (_now() - st.started_at) >= max_uptime_sec:
                log.event(
                    "MAIN_MAX_UPTIME_REACHED", run_id=RUN_ID, max_uptime_sec=int(max_uptime_sec)
                )
                stop_evt.set()
                break
            await asyncio.sleep(idle_sleep)
    finally:
        log.event("MAIN_SHUTDOWN_BEGIN", run_id=RUN_ID)
        stop_evt.set()
        await _cancel_and_gather(tasks, timeout=shutdown_grace)
        await _shutdown_router_runtime()
        log.event("MAIN_SHUTDOWN_DONE", run_id=RUN_ID)


# =============================================================================
# Compatibility aliases (callable by legacy entrypoints)
# =============================================================================
async def run(stop_evt: asyncio.Event | None = None) -> None:
    await main(stop_evt)


async def loop(stop_evt: asyncio.Event | None = None) -> None:
    await main(stop_evt)


# =============================================================================
# Back-compat export: scripts.main.Strategy (+ integration_smoke handler aliases)
# =============================================================================
def _resolve_external_strategy_class() -> type | None:
    """
    Resolve a real Strategy class from the repo if one exists.
    This runs ONLY when Strategy() is instantiated (not at module import).
    Env override:
        STRATEGY_CLASS="module.path:ClassName"
    """
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
    """
    Used by Strategy handler aliases:
    - If res is awaitable and we're in an event loop -> create_task(res).
    - If res is awaitable and no loop -> asyncio.run(res).
    - Else -> return res.
    """
    if not inspect.isawaitable(res):
        return res
    try:
        loop_ = asyncio.get_running_loop()
        return loop_.create_task(res)  # type: ignore[return-value]
    except RuntimeError:
        return asyncio.run(res)


class Strategy:
    """
    integration_smoke expects scripts.main.Strategy.
    Also expects some handler-ish method names (aliases below).

    Behavior:
    - If your repo defines a real Strategy class (see _resolve_external_strategy_class),
      this proxy will instantiate and forward to it.
    - Otherwise it acts as a thin wrapper around the orchestrator `main()`.
    """

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
# Build contract surface (safe-mode; unit_tests)
# =============================================================================
def build_app(*, safe: bool = True, **kwargs: Any) -> dict[str, Any]:
    """
    Contract startup builder: must be safe-mode friendly and must not touch network.

    IMPORTANT: This should not crash unit tests even if utils_pkg surface is absent.
    """
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
    """
    Default:
      - Render: assume start.sh supervises -> inner restart OFF by default
      - Local: if phase>=7 -> inner restart ON by default
    Override:
      - MAIN_INNER_RESTART_ON_CRASH=0|1
    """
    is_render_ = _is_render()
    restart_default = (not is_render_) and (_phase() >= 7)
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
# Module-level helper aliases (unit_tests expects these to exist)
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
    """
    Unit-test friendly helper. Does nothing on non-Windows.

    Defaults to Proactor on Windows (prior behavior), but allows override:
        WINDOWS_EVENT_LOOP_POLICY=selector|proactor
    """
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


# legacy aliases some harnesses use
run_main = run
run_loop = loop
start = run_forever
start_main = run_forever
entrypoint = run_forever


if __name__ == "__main__":  # pragma: no cover
    ensure_windows_event_loop_policy()
    asyncio.run(run_forever())
