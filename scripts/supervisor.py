#!/usr/bin/env python3
# scripts/supervisor.py
from __future__ import annotations

"""
scripts/supervisor.py  (QUARANTINED / EXTRA SYSTEMS ORCHESTRATOR)
=================================================================

This file exists to *avoid silent competition* with the canonical sniper path.

Role (P0/P2 contract)
---------------------
- This is NOT a production entrypoint for the sniper bot.
- It ONLY runs "extra systems" (allocator/daytrader/perps) when explicitly enabled.
- Safe-by-default: everything OFF unless you opt-in via CLI flags or env vars.
- On Render: default is to refuse running unless SUPERVISOR_ALLOW_ON_RENDER=1.

Why?
----
Your P0 plan explicitly calls out "extra systems must not silently compete with
sniper path". This supervisor is therefore:
  - gated
  - explicit
  - observable
  - resilient (imports are lazy, failures are reported without crashing other sleeves)

Usage
-----
  # one-shot help
  python -m scripts.supervisor --help

  # run allocator + perps sleeve locally
  SUPERVISOR_ALLOW_ON_RENDER=0 python -m scripts.supervisor --allocator --perps

  # run daytrader only
  python -m scripts.supervisor --daytrader

Environment flags (defaults OFF)
--------------------------------
  SUPERVISOR_ENABLED=1                  # master enable (else only CLI opt-in)
  SUPERVISOR_ALLOW_ON_RENDER=1          # allow on Render (default 0)
  ALLOCATOR_ENABLED=1                   # enable allocator sleeve
  DAYTRADER_ENABLED=1                   # enable daytrader sleeve
  PERPS_ENABLED=1                       # enable perps sleeve

Perps safety
------------
This script will not place live perps orders unless:
  PERPS_LIVE_TRADING=1

and a features provider is configured (recommended):
  PERPS_FEATURES_PROVIDER="pkg.mod:func"
Where func returns a dict like:
  {"feats": {...}, "funding8h_sum": float, "atr_pct": float, "equity_usd": float}

If no provider is configured, perps sleeve will run in "observe only" mode.
"""

import argparse
import asyncio
import contextlib
import importlib
import json
import os
import signal
import sys
import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

# ─────────────────────────────────────────────────────────────────────────────
# Ensure project root on sys.path (so `python -m scripts.supervisor` works anywhere)
# ─────────────────────────────────────────────────────────────────────────────
_HERE = Path(__file__).resolve().parent
for _p in (_HERE, *_HERE.parents):
    if (_p / "utils_pkg").is_dir():
        if str(_p) not in sys.path:
            sys.path.insert(0, str(_p))
        break


# ─────────────────────────────────────────────────────────────────────────────
# Logging (canonical)
# ─────────────────────────────────────────────────────────────────────────────
try:
    from logging_setup import get_logger  # canonical root logging
except Exception:  # pragma: no cover
    import logging

    def get_logger(name: str = "supervisor") -> logging.Logger:
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(name)


log = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def _utc_now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _is_render() -> bool:
    # Render sets multiple env vars; be defensive.
    return any(
        os.getenv(k)
        for k in (
            "RENDER",
            "RENDER_SERVICE_ID",
            "RENDER_INSTANCE_ID",
            "RENDER_EXTERNAL_URL",
        )
    )


def _env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(key: str, default: int) -> int:
    v = os.getenv(key)
    if v is None or str(v).strip() == "":
        return default
    try:
        return int(float(v))
    except Exception:
        return default


def _env_float(key: str, default: float) -> float:
    v = os.getenv(key)
    if v is None or str(v).strip() == "":
        return default
    try:
        return float(v)
    except Exception:
        return default


def _import_string(spec: str) -> Callable[..., Any]:
    """
    Import "pkg.mod:callable" and return it.
    """
    if ":" not in spec:
        raise ValueError("Import spec must be in the form 'pkg.mod:callable'")
    mod_name, fn_name = spec.split(":", 1)
    mod = importlib.import_module(mod_name)
    fn = getattr(mod, fn_name, None)
    if not callable(fn):
        raise TypeError(f"{spec} is not callable")
    return fn


def _safe_read_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _safe_write_json(path: Path, obj: Any) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(obj, indent=2, sort_keys=True, default=str), encoding="utf-8")
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Settings
# ─────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class SupervisorSettings:
    allow_on_render: bool
    allocator_enabled: bool
    daytrader_enabled: bool
    perps_enabled: bool

    allocator_interval_sec: int
    perps_poll_sec: int

    data_dir: Path

    # Perps
    perps_symbol: str
    perps_live_trading: bool
    perps_features_provider: str  # "pkg.mod:func" or ""

    # Observability
    heartbeat_sec: int


def load_settings_from_env(
    *,
    allocator_enabled_cli: bool | None,
    daytrader_enabled_cli: bool | None,
    perps_enabled_cli: bool | None,
) -> SupervisorSettings:
    enabled_master = _env_bool("SUPERVISOR_ENABLED", default=False)

    allow_on_render = _env_bool("SUPERVISOR_ALLOW_ON_RENDER", default=False)

    allocator_enabled = (
        allocator_enabled_cli
        if allocator_enabled_cli is not None
        else _env_bool("ALLOCATOR_ENABLED", default=False)
    )
    daytrader_enabled = (
        daytrader_enabled_cli
        if daytrader_enabled_cli is not None
        else _env_bool("DAYTRADER_ENABLED", default=False)
    )
    perps_enabled = (
        perps_enabled_cli
        if perps_enabled_cli is not None
        else _env_bool("PERPS_ENABLED", default=False)
    )

    # Master enable: if SUPERVISOR_ENABLED=0, only explicit CLI flags can enable sleeves.
    if not enabled_master:
        allocator_enabled = bool(allocator_enabled_cli)
        daytrader_enabled = bool(daytrader_enabled_cli)
        perps_enabled = bool(perps_enabled_cli)

    data_dir = Path(os.getenv("DATA_DIR", "data")).resolve()

    return SupervisorSettings(
        allow_on_render=allow_on_render,
        allocator_enabled=allocator_enabled,
        daytrader_enabled=daytrader_enabled,
        perps_enabled=perps_enabled,
        allocator_interval_sec=_env_int("ALLOCATOR_INTERVAL_SEC", 600),
        perps_poll_sec=_env_int("PERPS_POLL_SEC", 15),
        data_dir=data_dir,
        perps_symbol=os.getenv("PERPS_SYMBOL", "SOL-PERP"),
        perps_live_trading=_env_bool("PERPS_LIVE_TRADING", default=False),
        perps_features_provider=os.getenv("PERPS_FEATURES_PROVIDER", "").strip(),
        heartbeat_sec=_env_int("SUPERVISOR_HEARTBEAT_SEC", 30),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Allocator hooks (file-based fallback)
# ─────────────────────────────────────────────────────────────────────────────
def get_trailing_returns(data_dir: Path) -> dict:
    """
    Allocator hook: return dict of trailing returns per sleeve.

    Default: read from DATA_DIR/trailing_returns.json.
    If missing, return empty series.
    """
    path = data_dir / "trailing_returns.json"
    out = _safe_read_json(path)
    if not isinstance(out, dict):
        return {"daytrader": [], "sniper": [], "perps": []}
    out.setdefault("daytrader", [])
    out.setdefault("sniper", [])
    out.setdefault("perps", [])
    return out


def get_regime_returns(data_dir: Path) -> list[float]:
    """
    Allocator hook: return a 1D float array (as python list) for equity returns.

    Default: read DATA_DIR/equity_returns.npy if numpy is available.
    """
    npy = data_dir / "equity_returns.npy"
    if not npy.exists():
        return []
    try:
        import numpy as np  # lazy import

        arr = np.load(str(npy))
        try:
            return [float(x) for x in arr.tolist()]
        except Exception:
            return []
    except Exception:
        return []


def start_allocator_thread(
    settings: SupervisorSettings, stop_flag: threading.Event
) -> threading.Thread | None:
    """
    Start allocator in a daemon thread (allocator is typically sync).
    We *do not* hard-require allocator module presence.
    """
    try:
        from allocator.capital_allocator import run_allocator  # type: ignore
    except Exception as e:
        log.warning("allocator: not importable (%s). Skipping.", e)
        return None

    def _runner() -> None:
        log.info(
            "allocator: starting (interval=%ss, data_dir=%s)",
            settings.allocator_interval_sec,
            settings.data_dir,
        )
        # Note: run_allocator doesn't accept a stop signal in your current design.
        # We run it as daemon; process shutdown will terminate it.
        try:
            run_allocator(
                lambda: get_trailing_returns(settings.data_dir),
                lambda: get_regime_returns(settings.data_dir),
                int(settings.allocator_interval_sec),
            )
        except Exception as e:
            log.exception("allocator: crashed: %s", e)
        finally:
            stop_flag.set()

    t = threading.Thread(target=_runner, name="allocator", daemon=True)
    t.start()
    return t


# ─────────────────────────────────────────────────────────────────────────────
# Perps sleeve
# ─────────────────────────────────────────────────────────────────────────────
async def _perps_fetch_inputs(settings: SupervisorSettings) -> tuple[dict, float, float, float]:
    """
    Returns: (feats, funding8h_sum, atr_pct, equity_usd)

    Preferred: PERPS_FEATURES_PROVIDER="pkg.mod:func"
      func may be sync or async, and may accept (symbol=str) kwarg.
    """
    if settings.perps_features_provider:
        fn = _import_string(settings.perps_features_provider)
        try:
            if asyncio.iscoroutinefunction(fn):
                payload = await fn(symbol=settings.perps_symbol)  # type: ignore[arg-type]
            else:
                payload = fn(symbol=settings.perps_symbol)  # type: ignore[arg-type]
        except TypeError:
            # provider doesn't accept symbol kwarg
            payload = await fn() if asyncio.iscoroutinefunction(fn) else fn()

        if not isinstance(payload, dict):
            raise TypeError("PERPS_FEATURES_PROVIDER must return a dict")

        feats = payload.get("feats") or {}
        funding8h_sum = float(payload.get("funding8h_sum", 0.0) or 0.0)
        atr_pct = float(payload.get("atr_pct", 0.0) or 0.0)
        equity_usd = float(payload.get("equity_usd", 0.0) or 0.0)

        return dict(feats), funding8h_sum, atr_pct, equity_usd

    # Fallback: observe-only placeholder (NO TRADING)
    feats = {"placeholder": 1}
    funding8h_sum = 0.0
    atr_pct = 0.0
    equity_usd = _env_float("SIM_EQUITY_USD", 0.0)
    return feats, funding8h_sum, atr_pct, equity_usd


async def perps_manager(settings: SupervisorSettings, stop_event: asyncio.Event) -> None:
    """
    Perps sleeve loop. Safe-by-default:
      - If perps connector missing -> exits
      - If disabled -> exits
      - If live trading enabled but no features provider -> observe-only (no orders)
    """
    try:
        from utils_pkg.circuit_breakers import should_halt  # type: ignore
    except Exception:
        # if circuit_breakers missing, treat as no halt
        def should_halt() -> tuple[bool, str]:
            return False, ""

    try:
        from perps.alpha import should_long, should_short  # type: ignore
        from perps.connector_drift import DriftPerps  # type: ignore
        from perps.risk import clamp_leverage, ticket_notional  # type: ignore
    except Exception as e:
        log.warning("perps: modules not importable (%s). Skipping perps sleeve.", e)
        return

    conn = DriftPerps()
    if not getattr(conn, "enabled", False):
        log.info("perps: disabled by connector/config.")
        return

    log.info(
        "perps: starting (symbol=%s poll=%ss live_trading=%s provider=%s)",
        settings.perps_symbol,
        settings.perps_poll_sec,
        settings.perps_live_trading,
        bool(settings.perps_features_provider),
    )

    observe_only = settings.perps_live_trading and not settings.perps_features_provider
    if settings.perps_live_trading and observe_only:
        log.warning(
            "perps: PERPS_LIVE_TRADING=1 but PERPS_FEATURES_PROVIDER not set; running OBSERVE-ONLY (no orders)."
        )

    while not stop_event.is_set():
        try:
            halt, why = should_halt()
            if halt:
                log.warning("perps: halted by circuit breaker (%s)", why)
                await asyncio.sleep(30)
                continue

            feats, funding8h_sum, atr_pct, equity_usd = await _perps_fetch_inputs(settings)

            # sizing (defensive defaults)
            if equity_usd <= 0:
                equity_usd = _env_float("SIM_EQUITY_USD", 2000.0)
            if atr_pct <= 0:
                atr_pct = _env_float("PERPS_ATR_PCT_FALLBACK", 0.02)

            notional = float(ticket_notional(float(equity_usd), float(atr_pct)))
            lev = float(clamp_leverage(_env_float("PERPS_LEVERAGE", 1.5)))

            go_long = bool(should_long(feats, float(funding8h_sum)))
            go_short = bool(should_short(feats, float(funding8h_sum)))

            if observe_only or not settings.perps_live_trading:
                log.info(
                    "perps: observe (long=%s short=%s notional=%.2f lev=%.2f funding8h=%.6f atr_pct=%.4f)",
                    go_long,
                    go_short,
                    notional,
                    lev,
                    funding8h_sum,
                    atr_pct,
                )
            else:
                if go_long and not go_short:
                    ok, sig = await conn.place_order(
                        settings.perps_symbol,
                        "long",
                        notional,
                        lev,
                        stop_price=0,
                        take_profit=0,
                    )
                    log.info("perps: long order ok=%s sig=%s", ok, sig)
                elif go_short and not go_long:
                    ok, sig = await conn.place_order(
                        settings.perps_symbol,
                        "short",
                        notional,
                        lev,
                        stop_price=0,
                        take_profit=0,
                    )
                    log.info("perps: short order ok=%s sig=%s", ok, sig)
                else:
                    log.info("perps: no-op (signals: long=%s short=%s)", go_long, go_short)

        except asyncio.CancelledError:
            break
        except Exception as e:
            log.exception("perps: loop error: %s", e)

        await asyncio.sleep(max(1, int(settings.perps_poll_sec)))

    log.info("perps: stopped")


# ─────────────────────────────────────────────────────────────────────────────
# Daytrader sleeve
# ─────────────────────────────────────────────────────────────────────────────
async def daytrader_manager(stop_event: asyncio.Event) -> None:
    """
    Daytrader runner wrapper. Lazy import so it cannot break supervisor import.
    """
    try:
        from daytrader.runner import run_live  # type: ignore
    except Exception as e:
        log.warning("daytrader: not importable (%s). Skipping.", e)
        return

    log.info("daytrader: starting")

    try:
        if asyncio.iscoroutinefunction(run_live):
            task = asyncio.create_task(run_live())
        else:
            # sync runner: run in a thread to avoid blocking event loop
            task = asyncio.create_task(asyncio.to_thread(run_live))
        # Wait until stop_event triggers, then cancel runner task (best-effort)
        await stop_event.wait()
        task.cancel()
        with contextlib.suppress(Exception):
            await task
    except asyncio.CancelledError:
        pass
    except Exception as e:
        log.exception("daytrader: crashed: %s", e)
    finally:
        log.info("daytrader: stopped")


# ─────────────────────────────────────────────────────────────────────────────
# Supervisor core
# ─────────────────────────────────────────────────────────────────────────────
def _install_signal_handlers(stop_event: asyncio.Event) -> None:
    """
    Cross-platform signal handling:
    - On Unix: add_signal_handler if available.
    - On Windows: fallback to signal.signal.
    """

    def _set_stop(*_a: Any, **_k: Any) -> None:
        if not stop_event.is_set():
            stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop = asyncio.get_running_loop()
            loop.add_signal_handler(sig, _set_stop)
        except (NotImplementedError, RuntimeError):
            with contextlib.suppress(Exception):
                signal.signal(sig, lambda *_: _set_stop())


async def heartbeat(settings: SupervisorSettings, stop_event: asyncio.Event) -> None:
    """
    Periodic heartbeat so you can tell this supervisor is alive.
    Writes a small JSON status file into DATA_DIR for dashboards.
    """
    status_path = settings.data_dir / "supervisor_status.json"
    settings.data_dir.mkdir(parents=True, exist_ok=True)

    while not stop_event.is_set():
        payload = {
            "ts": _utc_now(),
            "on_render": _is_render(),
            "allocator_enabled": settings.allocator_enabled,
            "daytrader_enabled": settings.daytrader_enabled,
            "perps_enabled": settings.perps_enabled,
        }
        _safe_write_json(status_path, payload)
        log.info(
            "heartbeat: allocator=%s daytrader=%s perps=%s",
            settings.allocator_enabled,
            settings.daytrader_enabled,
            settings.perps_enabled,
        )
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=max(5, settings.heartbeat_sec))
        except TimeoutError:
            continue


async def run_supervisor(settings: SupervisorSettings) -> int:
    """
    Returns process exit code.
    """
    # Hard guard: do not let this silently become a Render entrypoint.
    if _is_render() and not settings.allow_on_render:
        log.error(
            "Refusing to run supervisor on Render (set SUPERVISOR_ALLOW_ON_RENDER=1 to override)."
        )
        return 2

    # Also guard "competition" explicitly: supervisor never launches sniper.
    if _env_bool("SUPERVISOR_START_SNIPER", default=False):
        log.error(
            "SUPERVISOR_START_SNIPER is not supported. Use the canonical entrypoint (start.sh / scripts/main.py)."
        )
        return 2

    stop_event = asyncio.Event()
    _install_signal_handlers(stop_event)

    thread_stop = threading.Event()
    allocator_thread: threading.Thread | None = None

    # Start allocator thread first (so it can warm caches)
    if settings.allocator_enabled:
        allocator_thread = start_allocator_thread(settings, thread_stop)

    tasks: list[asyncio.Task[Any]] = []
    tasks.append(asyncio.create_task(heartbeat(settings, stop_event), name="heartbeat"))

    if settings.daytrader_enabled:
        # Lazy import; isolate failures
        try:
            tasks.append(asyncio.create_task(daytrader_manager(stop_event), name="daytrader"))
        except Exception as e:
            log.exception("daytrader: failed to start: %s", e)

    if settings.perps_enabled:
        tasks.append(asyncio.create_task(perps_manager(settings, stop_event), name="perps"))

    # If nothing enabled, exit quickly (explicit)
    if (
        (not settings.allocator_enabled)
        and (not settings.daytrader_enabled)
        and (not settings.perps_enabled)
    ):
        log.warning(
            "No sleeves enabled. Nothing to do. (Use --allocator/--daytrader/--perps or env flags.)"
        )
        stop_event.set()

    # Main wait: stop on signal, or if allocator thread signals stop_flag (crash)
    try:
        while not stop_event.is_set():
            if thread_stop.is_set():
                log.warning("allocator thread signaled stop_flag; stopping supervisor")
                stop_event.set()
                break
            await asyncio.sleep(0.5)
    finally:
        stop_event.set()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        # allocator thread is daemon; we don't join indefinitely
        if allocator_thread and allocator_thread.is_alive():
            with contextlib.suppress(Exception):
                allocator_thread.join(timeout=1.0)

    log.info("supervisor: clean shutdown")
    return 0


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Quarantined orchestrator for extra systems (allocator/daytrader/perps). NOT the sniper entrypoint.",
    )
    p.add_argument(
        "--allocator", action="store_true", help="Enable allocator sleeve (capital allocator)."
    )
    p.add_argument("--daytrader", action="store_true", help="Enable daytrader sleeve.")
    p.add_argument("--perps", action="store_true", help="Enable perps sleeve.")
    p.add_argument(
        "--allow-on-render",
        action="store_true",
        help="Allow running on Render for this invocation (also set SUPERVISOR_ALLOW_ON_RENDER=1).",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    # CLI opt-in overrides env gating (still respects Render guard unless explicitly allowed)
    allocator_cli = True if args.allocator else None
    daytrader_cli = True if args.daytrader else None
    perps_cli = True if args.perps else None

    settings = load_settings_from_env(
        allocator_enabled_cli=allocator_cli,
        daytrader_enabled_cli=daytrader_cli,
        perps_enabled_cli=perps_cli,
    )

    if args.allow_on_render:
        settings = SupervisorSettings(
            allow_on_render=True,
            allocator_enabled=settings.allocator_enabled,
            daytrader_enabled=settings.daytrader_enabled,
            perps_enabled=settings.perps_enabled,
            allocator_interval_sec=settings.allocator_interval_sec,
            perps_poll_sec=settings.perps_poll_sec,
            data_dir=settings.data_dir,
            perps_symbol=settings.perps_symbol,
            perps_live_trading=settings.perps_live_trading,
            perps_features_provider=settings.perps_features_provider,
            heartbeat_sec=settings.heartbeat_sec,
        )

    try:
        return asyncio.run(run_supervisor(settings))
    except KeyboardInterrupt:
        return 130
    except Exception as e:
        log.exception("supervisor: fatal: %s", e)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
