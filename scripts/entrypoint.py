# scripts/entrypoint.py
from __future__ import annotations

import asyncio
import inspect
import logging
import math
import os
import signal
import sys
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime

try:
    # Python 3.11+
    from datetime import UTC as _UTC  # type: ignore
except Exception:  # pragma: no cover
    from datetime import timezone as _timezone

    _UTC = _timezone.utc  # type: ignore


BANNER = "sniper-entrypoint v3"
DEFAULT_SERVICE = "sniper"
DEFAULT_VERSION = "v1"


# ──────────────────────────────────────────────────────────────────────────────
# logging_setup integration (soft dependency)
# ──────────────────────────────────────────────────────────────────────────────
def _noop(*_a: object, **_k: object) -> None:
    return None


def _install_stdio_hook(_log: object) -> None:
    # hook_stdio(log) is optional; tolerate missing / signature drift
    try:
        from logging_setup import hook_stdio  # type: ignore
    except Exception:
        return
    try:
        hook_stdio(_log)  # type: ignore[misc]
    except TypeError:
        with suppress(Exception):
            hook_stdio()  # type: ignore[misc]
    except Exception:
        return


def _install_excepthook(_log: object) -> None:
    try:
        from logging_setup import install_excepthook  # type: ignore
    except Exception:
        return
    try:
        install_excepthook(_log)  # type: ignore[misc]
    except TypeError:
        with suppress(Exception):
            install_excepthook()  # type: ignore[misc]
    except Exception:
        return


def _start_heartbeat(_log: object, interval: int) -> None:
    try:
        from logging_setup import start_global_heartbeat  # type: ignore
    except Exception:
        return
    try:
        # prefer named arg "interval" (your current code)
        start_global_heartbeat(_log, interval=interval)  # type: ignore[misc]
        return
    except TypeError:
        pass
    except Exception:
        return
    # signature drift fallback
    with suppress(Exception):
        start_global_heartbeat(_log, interval_sec=interval)  # type: ignore[misc]


def _bump(metric: str, value: int = 1) -> None:
    # bump("metric") is optional
    try:
        from logging_setup import bump  # type: ignore
    except Exception:
        return
    try:
        bump(metric, value=value)  # type: ignore[misc]
    except TypeError:
        with suppress(Exception):
            bump(metric)  # type: ignore[misc]
    except Exception:
        return


def _get_logger(service: str, version: str) -> logging.Logger:
    """
    Try to use your structured logger; fall back to stdlib logger if anything goes wrong.
    """
    try:
        from logging_setup import get_logger  # type: ignore

        lg = get_logger(service=service, version=version)  # type: ignore[misc]
        if lg is None:
            raise RuntimeError("get_logger returned None")
        return lg  # type: ignore[return-value]
    except Exception:
        lg = logging.getLogger(service or "sniper")
        if not lg.handlers:
            h = logging.StreamHandler()
            fmt = logging.Formatter("[entrypoint] %(asctime)s %(levelname)s %(message)s")
            h.setFormatter(fmt)
            lg.addHandler(h)
            lg.setLevel(logging.INFO)
        return lg


class _LogCompat:
    """
    Compatibility wrapper: ensures log.event / log.once / log.every exist,
    even if get_logger() returns stdlib logger or a slightly different interface.
    """

    def __init__(self, base: logging.Logger):
        self._base = base
        self._once_cache: dict[str, float] = {}
        self._every_cache: dict[str, int] = {}

    def _emit(self, level: int, msg: str, **fields: object) -> None:
        # Keep it safe: no secrets printing beyond what caller provides.
        if fields:
            self._base.log(level, "%s %s", msg, fields)
        else:
            self._base.log(level, "%s", msg)

    def event(self, name: str, **fields: object) -> None:
        # If base already supports .event, use it
        ev = getattr(self._base, "event", None)
        if callable(ev):
            try:
                ev(name, **fields)  # type: ignore[misc]
                return
            except Exception:
                pass
        self._emit(logging.INFO, name, **fields)

    def once(
        self,
        key: str,
        ttl: float = 60.0,
        level: int = logging.INFO,
        msg: str = "",
        **fields: object,
    ) -> None:
        now = datetime.now(_UTC).timestamp()
        last = self._once_cache.get(key, 0.0)
        if (now - last) < float(ttl):
            return
        self._once_cache[key] = now
        text = msg or key
        self._emit(level, text, **fields)

    def every(
        self, key: str, n: int = 10, level: int = logging.DEBUG, msg: str = "", **fields: object
    ) -> None:
        cnt = self._every_cache.get(key, 0) + 1
        self._every_cache[key] = cnt
        if n <= 1 or (cnt % int(n) == 0):
            text = msg or key
            self._emit(level, text, **fields)

    def exception(self, msg: str, **fields: object) -> None:
        # stdlib exception stack
        self._base.exception(msg, extra={"fields": fields} if fields else None)

    # allow direct attr pass-through
    def __getattr__(self, name: str) -> object:
        return getattr(self._base, name)


# ──────────────────────────────────────────────────────────────────────────────
# Env helpers (strict, boring, reliable)
# ──────────────────────────────────────────────────────────────────────────────
def _envs(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v)


def _envb(name: str, default: bool = False) -> bool:
    v = (_envs(name, "") or "").strip().lower()
    if not v:
        return default
    return v in {"1", "true", "yes", "y", "on", "t"}


def _envi(name: str, default: int) -> int:
    try:
        return int((_envs(name, str(default))).strip())
    except Exception:
        return int(default)


def _envf(name: str, default: float) -> float:
    try:
        return float((_envs(name, str(default))).strip())
    except Exception:
        return float(default)


def _mask(s: str, head: int = 4) -> str:
    s = s or ""
    if not s:
        return ""
    if head <= 0:
        return "..."
    return (s[:head] + "...") if len(s) > head else (s + "...")


def _clamp(x: float, lo: float, hi: float) -> float:
    if lo > hi:
        lo, hi = hi, lo
    return max(lo, min(hi, x))


def _round_down_step(x: float, step: float) -> float:
    if step <= 0:
        return x
    return math.floor(x / step) * step


def _fmt_amt(x: float) -> str:
    # stable, compact float string for env/logging
    return f"{x:.9f}".rstrip("0").rstrip(".")


# ──────────────────────────────────────────────────────────────────────────────
# Runtime config (entrypoint-only)
# ──────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class EntrypointCfg:
    service: str
    version: str
    stdio_hook: bool
    heartbeat_sec: int
    shutdown_grace_sec: float

    router_start_timeout_sec: int
    preflight_enabled: bool

    autosize_enabled: bool
    autosize_refresh_sec: int
    trade_risk_pct: float
    trade_min_sol: float
    trade_max_sol: float
    trade_step_sol: float
    autosize_every_n: int


def _load_cfg() -> EntrypointCfg:
    service = _envs("PF_SERVICE", DEFAULT_SERVICE)
    version = _envs("PF_VERSION", DEFAULT_VERSION)

    stdio_hook = _envb("PF_STDIO_HOOK", True)
    heartbeat_sec = max(5, _envi("PF_HEARTBEAT_SEC", 30))
    shutdown_grace_sec = float(_envf("PF_SHUTDOWN_GRACE_SEC", 2.0))

    router_start_timeout_sec = max(1, _envi("ROUTER_START_TIMEOUT_SEC", 3))
    preflight_enabled = _envb("PF_PREFLIGHT", False)

    autosize_enabled = _envb("PF_TRADE_AUTOSIZE", False)
    autosize_refresh_sec = max(0, _envi("PF_TRADE_AUTOSIZE_REFRESH_SEC", 0))

    trade_risk_pct = _clamp(_envf("PF_TRADE_RISK_PCT", 0.03), 0.0, 1.0)
    trade_min_sol = max(0.0, _envf("PF_TRADE_MIN_SOL", 0.005))
    trade_max_sol = max(0.0, _envf("PF_TRADE_MAX_SOL", 0.25))
    trade_step_sol = max(0.0, _envf("PF_TRADE_STEP_SOL", 0.001))
    autosize_every_n = max(5, _envi("PF_TRADE_AUTOSIZE_EVERY_N", 10))

    if trade_max_sol < trade_min_sol:
        trade_min_sol, trade_max_sol = trade_max_sol, trade_min_sol

    return EntrypointCfg(
        service=service,
        version=version,
        stdio_hook=stdio_hook,
        heartbeat_sec=heartbeat_sec,
        shutdown_grace_sec=shutdown_grace_sec,
        router_start_timeout_sec=router_start_timeout_sec,
        preflight_enabled=preflight_enabled,
        autosize_enabled=autosize_enabled,
        autosize_refresh_sec=autosize_refresh_sec,
        trade_risk_pct=trade_risk_pct,
        trade_min_sol=trade_min_sol,
        trade_max_sol=trade_max_sol,
        trade_step_sol=trade_step_sol,
        autosize_every_n=autosize_every_n,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Logger (global, test-safe)
# ──────────────────────────────────────────────────────────────────────────────
CFG = _load_cfg()
_log_base = _get_logger(service=CFG.service, version=CFG.version)
log = _LogCompat(_log_base)
_install_excepthook(log)


# ──────────────────────────────────────────────────────────────────────────────
# Boot event (structured, stable schema)
# ──────────────────────────────────────────────────────────────────────────────
def _boot_event() -> None:
    now = datetime.now(_UTC).isoformat()
    knobs = {
        k: os.getenv(k)
        for k in ("PF_SLIPPAGE_BPS", "PF_PRIORITY_SOL", "PF_POOLS")
        if k in os.environ
    }

    guardrails_keys = (
        "PF_DAILY_MAX_DRAWDOWN_PCT",
        "PF_DAILY_MAX_LOSSES",
        "PF_MAX_CONCURRENT",
        "PF_COOLDOWN_SEC",
        "PF_TRADE_AUTOSIZE",
        "PF_TRADE_AUTOSIZE_REFRESH_SEC",
        "PF_TRADE_RISK_PCT",
        "PF_TRADE_MIN_SOL",
        "PF_TRADE_MAX_SOL",
        "PF_TRADE_STEP_SOL",
        "PF_TRADE_SOL",
    )

    router_keys = (
        "JUP_BASE_URL",
        "JUP_ALT_BASE_URLS",
        "JUP_PRO_KEY",
        "JUP_MAX_QPS",
        "JUP_AS_LEGACY",
        "JUP_ONLY_DIRECT",
        "ROUTER_QUOTE_RETRIES",
        "ROUTER_SWAP_RETRIES",
        "ROUTER_RETRY_BACKOFF_SEC",
        "ROUTER_QUOTE_TTL_SEC",
        "PF_PRICE_TTL_SEC",
        "JUPITER_MAX_SLIPPAGE",
    )

    # start.sh uses DRYRUN; some code uses DRY_RUN; log both
    dry_run = _envs("DRYRUN", "") or _envs("DRY_RUN", "") or _envs("SAFE_MODE", "")

    log.event(
        "BOOT",
        banner=BANNER,
        ts=now,
        python=sys.version.split()[0],
        platform=sys.platform,
        py_unbuffered=_envs("PYTHONUNBUFFERED", ""),
        dry_run=dry_run,
        pf_service=CFG.service,
        pf_version=CFG.version,
        # secrets masked
        pf_pp_key=_mask(_envs("PF_PP_KEY", "")),
        sol_rpc_url=_mask(_envs("SOL_RPC_URL", "")),
        helius_api_key=_mask(_envs("HELIUS_API_KEY", "")),
        telegram_chat_id=_mask(_envs("TELEGRAM_CHAT_ID", ""), 6),
        # visible config (safe)
        guardrails={k: _envs(k, "") for k in guardrails_keys},
        knobs=knobs,
        router={
            k: (_mask(_envs(k, "")) if ("KEY" in k or "TOKEN" in k) else _envs(k, ""))
            for k in router_keys
        },
    )


# ──────────────────────────────────────────────────────────────────────────────
# Optional runtime tweaks
# ──────────────────────────────────────────────────────────────────────────────
def _maybe_set_windows_policy() -> None:
    if sys.platform.startswith("win"):
        with suppress(Exception):
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())  # type: ignore[attr-defined]


def _maybe_enable_uvloop() -> None:
    # uvloop on linux for lower latency / less overhead
    if sys.platform.startswith("linux"):
        with suppress(Exception):
            import uvloop  # type: ignore

            uvloop.install()


def _maybe_hook_stdio() -> None:
    if CFG.stdio_hook:
        _install_stdio_hook(log)
        log.event("STDIO_HOOKED", stdout_level="INFO", stderr_level="ERROR")


def _install_loop_exception_handler(loop: asyncio.AbstractEventLoop) -> None:
    """
    Catch unhandled task exceptions (very common source of “silent death”).
    """

    def _handler(_loop: asyncio.AbstractEventLoop, context: dict) -> None:
        msg = context.get("message") or "ASYNCIO_EXCEPTION"
        exc = context.get("exception")
        if exc:
            log.once("asyncio.loop.exception", ttl=5, level=logging.ERROR, msg=msg, error=str(exc))
        else:
            log.once(
                "asyncio.loop.exception.noexc", ttl=5, level=logging.ERROR, msg=msg, context=context
            )

    with suppress(Exception):
        loop.set_exception_handler(_handler)


# ──────────────────────────────────────────────────────────────────────────────
# Autosize (entrypoint-level compatibility; may be moved to sizer later)
# ──────────────────────────────────────────────────────────────────────────────
async def autosize_once() -> float:
    """
    Compute PF_TRADE_SOL from wallet balance + env guardrails, then set in env.

    Behavior:
      - If PF_TRADE_AUTOSIZE=0: clamps existing PF_TRADE_SOL (or default) into bounds.
      - If PF_TRADE_AUTOSIZE=1: uses balance * PF_TRADE_RISK_PCT (clamped & stepped).
      - Never raises; always returns a usable float.
    """
    min_sol = float(CFG.trade_min_sol)
    max_sol = float(CFG.trade_max_sol)
    step_sol = float(CFG.trade_step_sol)
    risk_pct = float(CFG.trade_risk_pct)

    def _finalize(val: float, why: str, *, bal_sol: float | None, prev: float | None) -> float:
        val = float(_clamp(float(val), min_sol, max_sol))
        if step_sol > 0:
            val = max(min_sol, _round_down_step(val, step_sol))

        os.environ["PF_TRADE_SOL"] = _fmt_amt(val)
        new_s = os.environ["PF_TRADE_SOL"]
        prev_s = _fmt_amt(prev) if prev is not None else None

        if prev_s != new_s:
            log.event(
                "AUTOSIZE_UPDATE",
                why=why,
                bal_sol=bal_sol,
                risk_pct=risk_pct,
                min_sol=min_sol,
                max_sol=max_sol,
                step_sol=step_sol,
                trade_sol=val,
            )
            _bump("autosize_updates")
        else:
            log.every(
                "autosize.same",
                n=int(CFG.autosize_every_n),
                level=logging.DEBUG,
                msg="AUTOSIZE_SAME",
                bal_sol=bal_sol,
                risk_pct=risk_pct,
                trade_sol=val,
            )
            _bump("autosize_ticks")

        return val

    # Disabled path: clamp whatever is set
    if not CFG.autosize_enabled:
        current = float(_envf("PF_TRADE_SOL", max(min_sol, 0.01)))
        return _finalize(current, "disabled", bal_sol=None, prev=current)

    # Enabled path: try balance fetch
    try:
        # keep your existing module contract; if it changes, we degrade gracefully
        from utils_pkg.solana_wallet import get_balance_sol, load_keypair  # type: ignore
    except Exception as e:
        fallback = float(_envf("PF_TRADE_SOL", min_sol))
        log.once(
            "autosize.wallet_import_fail",
            ttl=60,
            level=logging.WARNING,
            msg="AUTOSIZE_WALLET_IMPORT_FAIL",
            error=str(e),
        )
        return _finalize(fallback, "wallet-import-fail", bal_sol=None, prev=fallback)

    try:
        kp = load_keypair()
        pub = getattr(kp, "pubkey", None)
        pubkey = str(pub() if callable(pub) else pub)  # tolerate object vs callable
        bal_sol = await get_balance_sol(pubkey)
    except Exception as e:
        fallback = float(_envf("PF_TRADE_SOL", min_sol))
        log.once(
            "autosize.balance_fetch_fail",
            ttl=30,
            level=logging.WARNING,
            msg="AUTOSIZE_BALANCE_FAIL",
            error=str(e),
        )
        return _finalize(fallback, "balance-fetch-fail", bal_sol=None, prev=fallback)

    raw = max(0.0, float(bal_sol) * risk_pct)
    sized = _clamp(raw, min_sol, max_sol)
    if step_sol > 0:
        sized = max(min_sol, _round_down_step(sized, step_sol))
    if sized <= 0 and min_sol > 0:
        sized = min_sol

    prev = float(_envf("PF_TRADE_SOL", sized))
    return _finalize(float(sized), "ok", bal_sol=float(bal_sol), prev=prev)


async def _autosize_loop(stop_evt: asyncio.Event) -> None:
    refresh_sec = max(1, int(CFG.autosize_refresh_sec))
    while not stop_evt.is_set():
        try:
            await asyncio.wait_for(stop_evt.wait(), timeout=refresh_sec)
        except TimeoutError:
            with suppress(Exception):
                await autosize_once()


# ──────────────────────────────────────────────────────────────────────────────
# Optional subsystem bootstrap (idempotent)
# ──────────────────────────────────────────────────────────────────────────────
async def _start_router_subsystem() -> None:
    """
    Boot router subsystem if present.
    Safe to call even if router module doesn't exist or start() isn't implemented.
    """
    try:
        from utils_pkg import router  # type: ignore
    except Exception as e:
        log.once(
            "router.import_fail",
            ttl=120,
            level=logging.ERROR,
            msg="ROUTER_IMPORT_FAIL",
            error=str(e),
        )
        return

    start_fn = getattr(router, "start", None)
    if start_fn is None:
        log.event("ROUTER_START_SKIPPED", why="no_start_fn")
        return

    try:
        res = start_fn()
        if inspect.isawaitable(res):
            await asyncio.wait_for(res, timeout=int(CFG.router_start_timeout_sec))
        log.event("ROUTER_STARTED")
    except TimeoutError:
        log.once("router.start.timeout", ttl=60, level=logging.WARNING, msg="ROUTER_START_TIMEOUT")
    except Exception as e:
        log.once(
            "router.start.error",
            ttl=60,
            level=logging.WARNING,
            msg="ROUTER_START_ERROR",
            error=str(e),
        )


async def _maybe_run_preflight() -> None:
    """
    Optional preflight hook. Keeps entrypoint canonical while letting you enforce
    wiring before going live: PF_PREFLIGHT=1
    """
    if not CFG.preflight_enabled:
        return
    try:
        from scripts.preflight import main as preflight_main  # type: ignore
    except Exception as e:
        log.once(
            "preflight.import_fail",
            ttl=120,
            level=logging.ERROR,
            msg="PREFLIGHT_IMPORT_FAIL",
            error=str(e),
        )
        return

    try:
        log.event("PREFLIGHT_START")
        res = preflight_main()
        if inspect.isawaitable(res):
            await res
        log.event("PREFLIGHT_OK")
    except Exception as e:
        # Preflight is allowed to fail-fast when enabled
        log.event("PREFLIGHT_FAIL", error=str(e))
        raise


# ──────────────────────────────────────────────────────────────────────────────
# Handoff: scripts.main.main
# ──────────────────────────────────────────────────────────────────────────────
async def _run_handoff(stop_evt: asyncio.Event) -> int:
    """
    Call scripts.main.main (async or sync, with or without stop_evt).
    Returns process-style rc integer.
    """
    try:
        from scripts.main import main as run_main  # type: ignore
    except Exception as e:  # pragma: no cover
        log.event("HANDOFF_IMPORT_FAIL", error=str(e))
        return 1

    log.event("HANDOFF_START")
    try:
        try:
            out = run_main(stop_evt=stop_evt)  # preferred
        except TypeError:
            out = run_main()  # legacy signature

        if inspect.isawaitable(out):
            out = await out  # type: ignore[assignment]

        rc = int(out) if out is not None else 0
        log.event("HANDOFF_DONE", rc=rc)
        return rc
    except asyncio.CancelledError:
        log.event("HANDOFF_CANCELLED")
        return 130
    except Exception as e:
        log.event("HANDOFF_ERROR", error=str(e))
        return 1


# ──────────────────────────────────────────────────────────────────────────────
# Main orchestration / shutdown wiring
# ──────────────────────────────────────────────────────────────────────────────
async def _run_main_with_shutdown() -> int:
    stop_evt = asyncio.Event()
    autosize_task: asyncio.Task[None] | None = None

    # record last signal (to translate to conventional exit codes)
    last_signal: str | None = None

    def _request_stop(signame: str) -> None:
        nonlocal last_signal
        last_signal = signame
        if not stop_evt.is_set():
            log.event("SIGNAL", signal=signame, action="shutdown")
            stop_evt.set()

    loop = asyncio.get_running_loop()
    _install_loop_exception_handler(loop)

    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, _request_stop, sig.name)

    # Optional safety preflight (fail-fast when enabled)
    await _maybe_run_preflight()

    # Router warm start (non-fatal)
    await _start_router_subsystem()

    # Autosize once before trading starts
    with suppress(Exception):
        await autosize_once()

    # Autosize refresh loop (optional)
    if CFG.autosize_enabled and CFG.autosize_refresh_sec > 0:
        autosize_task = asyncio.create_task(_autosize_loop(stop_evt))

    # Run main trading loop
    rc = await _run_handoff(stop_evt)

    # Begin shutdown
    stop_evt.set()

    # Grace window for background tasks
    grace = max(0.0, float(CFG.shutdown_grace_sec))
    if autosize_task:
        autosize_task.cancel()
        with suppress(Exception):
            if grace > 0:
                await asyncio.wait_for(autosize_task, timeout=grace)
            else:
                await autosize_task

    # Conventional signal exit codes if main returns 0 but we were signaled
    if rc == 0 and last_signal:
        if last_signal == "SIGINT":
            return 130
        if last_signal == "SIGTERM":
            return 143
    return rc


async def main() -> None:
    _maybe_hook_stdio()
    _boot_event()

    # Heartbeat for supervisors/visibility
    _start_heartbeat(log, interval=int(CFG.heartbeat_sec))

    rc = await _run_main_with_shutdown()

    log.event("EXIT", code=rc)
    if rc:
        # give structured logs a moment to flush
        await asyncio.sleep(0.05)
        raise SystemExit(rc)


if __name__ == "__main__":
    _maybe_set_windows_policy()
    _maybe_enable_uvloop()
    asyncio.run(main())
