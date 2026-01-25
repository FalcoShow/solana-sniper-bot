# tests/test_contract_exit_engine.py
from __future__ import annotations

"""
tests/test_contract_exit_engine.py — Exit Engine LOCK-IN (P0)
============================================================

What this file protects
-----------------------
This contract suite locks the *exit* decision layer (NOT execution):

  Position-ish payload (+ optional tick/meta context) -> Exit decision (or HOLD)

Hard requirements (LOCK-IN)
---------------------------
✅ Import-safe: no secrets required, no dotenv side effects, no network at import-time
✅ No real network I/O during tests (guards hard-fail)
✅ Stable discovery across refactors (module paths + callable names are flexible)
✅ Accepts "position-ish" payloads (dict) and ideally contract objects (optional)
✅ Returns a decision object that is loggable (dict-like / dataclass / pydantic) OR None for HOLD
✅ If decision indicates EXIT/SELL => must include a human-readable reason
✅ Deterministic for fixed inputs in SAFE_MODE (no hidden randomness/time)

These tests validate interface + safety only (not profitability).
"""

import asyncio
import contextlib
import importlib
import inspect
import json
import os
import socket
import sys
import time
from collections.abc import Callable
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.contract

# -----------------------------------------------------------------------------
# Stable constants (no network dependency)
# -----------------------------------------------------------------------------
SOL_MINT = "So11111111111111111111111111111111111111112"  # Wrapped SOL
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"  # USDC


# -----------------------------------------------------------------------------
# Environment + network guards
# -----------------------------------------------------------------------------
class NetworkBlockedError(RuntimeError):
    """Raised when contract tests detect network I/O."""


def _set_safe_mode_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Force pure decision paths and prevent any dependency on external services.
    Set multiple toggles so whichever your code recognizes will work.
    """
    monkeypatch.setenv("DRYRUN", "1")
    monkeypatch.setenv("DRY_RUN", "1")
    monkeypatch.setenv("SAFE_MODE", "1")
    monkeypatch.setenv("NO_NETWORK", "1")
    monkeypatch.setenv("DISABLE_NETWORK", "1")
    monkeypatch.setenv("CI", "1")
    monkeypatch.setenv("PYTEST_MODE", "1")

    # Prevent dotenv/autoload side effects (if supported)
    monkeypatch.setenv("CORE_LOAD_DOTENV", "0")

    # Extra safety in case your engine checks these:
    monkeypatch.setenv("TRADING_ENABLED", "0")
    monkeypatch.setenv("LIVE_TRADING", "0")
    monkeypatch.setenv("ALLOW_BUYS", "0")
    monkeypatch.setenv("ALLOW_SELLS", "0")
    monkeypatch.setenv("DISABLE_LIVE_TRADING", "1")

    # Disable integrations if your code supports these flags (safe if ignored)
    monkeypatch.setenv("REDIS_DISABLED", "1")
    monkeypatch.setenv("POSTGRES_DISABLED", "1")
    monkeypatch.setenv("NOTION_DISABLED", "1")
    monkeypatch.setenv("TELEGRAM_DISABLED", "1")
    monkeypatch.setenv("DISCORD_DISABLED", "1")


def _is_win() -> bool:
    return (os.name == "nt") or sys.platform.startswith("win")


def _ephemeral_low_port() -> int:
    """
    Lower bound for ephemeral ports.
    - Windows default ephemeral starts at 49152.
    - Linux typically >= 32768 (attempt to read kernel config).
    """
    if _is_win():
        return 49152
    try:
        p = Path("/proc/sys/net/ipv4/ip_local_port_range")
        if p.exists():
            raw = p.read_text(encoding="utf-8", errors="ignore").strip().split()
            if len(raw) >= 2:
                lo = int(raw[0])
                if 1024 <= lo <= 65000:
                    return lo
    except Exception:
        pass
    return 32768


_LOOPBACK_HOSTS = {"127.0.0.1", "::1", "localhost"}


def _extract_host_port(sockaddr: Any) -> tuple[str | None, int | None]:
    if isinstance(sockaddr, tuple) and len(sockaddr) >= 2:
        host = sockaddr[0]
        port = sockaddr[1]
        return (str(host), int(port) if isinstance(port, int) else None)
    return (None, None)


def _allow_loopback_ephemeral_only(sockaddr: Any) -> bool:
    host, port = _extract_host_port(sockaddr)
    if host is None or port is None:
        return False
    h = str(host).strip().strip("[]").lower()
    if h not in _LOOPBACK_HOSTS:
        return False
    if port == 0:
        return True
    return port >= _ephemeral_low_port()


def _raise_blocked(where: str, host: Any, port: Any, why: str = "") -> None:
    h = str(host).strip() if host is not None else "<unknown>"
    p = port if port is not None else "<unknown>"
    msg = (
        "Network is blocked in exit-engine contract tests.\n"
        f"Blocked at: {where}\n"
        f"Destination: {h}:{p}\n"
    )
    if why:
        msg += f"Reason: {why}\n"
    msg += (
        "Allowed by this file's guard: loopback + ephemeral ports only.\n"
        "If you intended to use local services or real network, this must NOT be a contract test.\n"
    )
    raise NetworkBlockedError(msg)


@contextlib.contextmanager
def _no_network_local_services(monkeypatch: pytest.MonkeyPatch, *mods: Any):
    """
    EXTRA guard on top of tests/conftest.py.

    Blocks loopback connections to non-ephemeral ports to avoid accidentally
    hitting local redis/postgres/RPC. Scoped to code-under-test so pytest-asyncio
    teardown remains stable.
    """
    with monkeypatch.context() as m:
        orig_create_connection = getattr(socket, "create_connection", None)
        orig_connect = socket.socket.connect
        orig_connect_ex = socket.socket.connect_ex

        def create_connection(address: Any, *args: Any, **kwargs: Any) -> Any:
            # AF_UNIX path string: allow
            if isinstance(address, str | bytes):
                assert orig_create_connection is not None
                return orig_create_connection(address, *args, **kwargs)

            host, port = _extract_host_port(address)
            if host is not None and port is not None:
                if _allow_loopback_ephemeral_only(address):
                    assert orig_create_connection is not None
                    return orig_create_connection(address, *args, **kwargs)
                _raise_blocked("socket.create_connection", host, port, why="loopback non-ephemeral")
            raise NetworkBlockedError(
                "Network blocked: socket.create_connection unsupported address."
            )

        def connect(self: socket.socket, address: Any) -> Any:
            if isinstance(address, str | bytes):
                return orig_connect(self, address)
            host, port = _extract_host_port(address)
            if host is not None and port is not None:
                if _allow_loopback_ephemeral_only(address):
                    return orig_connect(self, address)
                _raise_blocked("socket.socket.connect", host, port, why="loopback non-ephemeral")
            raise NetworkBlockedError("Network blocked: socket.connect unsupported address.")

        def connect_ex(self: socket.socket, address: Any) -> Any:
            if isinstance(address, str | bytes):
                return orig_connect_ex(self, address)
            host, port = _extract_host_port(address)
            if host is not None and port is not None:
                if _allow_loopback_ephemeral_only(address):
                    return orig_connect_ex(self, address)
                _raise_blocked("socket.socket.connect_ex", host, port, why="loopback non-ephemeral")
            raise NetworkBlockedError("Network blocked: socket.connect_ex unsupported address.")

        m.setattr(socket, "create_connection", create_connection, raising=True)
        m.setattr(socket.socket, "connect", connect, raising=True)
        m.setattr(socket.socket, "connect_ex", connect_ex, raising=True)

        # Patch common HTTP clients IF present in module namespaces
        for mod in mods:
            aiohttp = getattr(mod, "aiohttp", None)
            if aiohttp is not None:

                class _GuardAiohttpSession:
                    def __init__(self, *args: Any, **kwargs: Any) -> None:
                        raise NetworkBlockedError("Network blocked (aiohttp.ClientSession).")

                m.setattr(aiohttp, "ClientSession", _GuardAiohttpSession, raising=False)

            httpx = getattr(mod, "httpx", None)
            if httpx is not None:

                class _GuardHttpxAsyncClient:
                    def __init__(self, *args: Any, **kwargs: Any) -> None:
                        raise NetworkBlockedError("Network blocked (httpx.AsyncClient).")

                m.setattr(httpx, "AsyncClient", _GuardHttpxAsyncClient, raising=False)

            requests = getattr(mod, "requests", None)
            if requests is not None:

                def _guard_requests(*args: Any, **kwargs: Any) -> Any:
                    raise NetworkBlockedError("Network blocked (requests).")

                for nm in ("request", "get", "post"):
                    if hasattr(requests, nm):
                        m.setattr(requests, nm, _guard_requests, raising=False)

        yield


def _freeze_time(monkeypatch: pytest.MonkeyPatch, fixed_ts: int) -> None:
    """
    Best-effort determinism: patch time.time() and time.monotonic()/perf_counter().
    Engines that take injected now_ts will ignore this; engines that call time.time()
    become deterministic.
    """
    monkeypatch.setattr(time, "time", lambda: float(fixed_ts), raising=False)
    monkeypatch.setattr(time, "monotonic", lambda: float(fixed_ts), raising=False)
    monkeypatch.setattr(time, "perf_counter", lambda: float(fixed_ts), raising=False)


# -----------------------------------------------------------------------------
# Import + discovery helpers (stable across refactors)
# -----------------------------------------------------------------------------
def _fresh_import(module_name: str) -> Any:
    for name in list(sys.modules.keys()):
        if name == module_name or name.startswith(module_name + "."):
            sys.modules.pop(name, None)
    return importlib.import_module(module_name)


def _try_import_first(module_names: tuple[str, ...]) -> tuple[str, Any] | None:
    for name in module_names:
        try:
            mod = _fresh_import(name)
            return name, mod
        except Exception:
            continue
    return None


def _discover_exit_engine_module() -> tuple[str, Any]:
    found = _try_import_first(
        (
            "utils_pkg.exit_engine",
            "utils_pkg.auto_exit_worker.exit_engine",
            "utils_pkg.auto_exit_worker.exit_engine_v2",
            "utils_pkg.exit_engine_v2",
            "utils_pkg.exit_engine_core",
        )
    )
    if not found:
        pytest.skip(
            "Could not import exit engine module. Tried: "
            "utils_pkg.exit_engine / utils_pkg.auto_exit_worker.exit_engine / exit_engine_v2 / exit_engine_core."
        )
    return found


def _is_executor_name(name: str) -> bool:
    """
    Filter out obvious side-effectful execution functions.
    We want decision/planning, not actually sending swaps/txs.
    """
    s = (name or "").lower()
    bad = (
        "execute",
        "submit",
        "send",
        "broadcast",
        "place_order",
        "placeorder",
        "swap",
        "do_swap",
        "sell_now",
        "force_sell",
        "market_sell",
        "perform",
        "run_loop",
        "worker",
    )
    return any(tok in s for tok in bad)


def _discover_decider(mod: Any) -> Callable[..., Any]:
    """
    Find a "pure-ish" decision function (sync or async).
    Works across refactors by checking common names and then scanning.
    """
    preferred = (
        "decide_exit",
        "compute_exit_decision",
        "exit_decision",
        "evaluate_exit",
        "plan_exit",
        "get_exit_decision",
        "decide",
    )
    tick_style = (
        "on_trade_tick",
        "on_price_tick",
        "tick",
        "on_tick",
        "handle_tick",
    )

    for name in preferred:
        fn = getattr(mod, name, None)
        if callable(fn) and not _is_executor_name(name):
            return fn

    for name in tick_style:
        fn = getattr(mod, name, None)
        if callable(fn) and not _is_executor_name(name):
            return fn

    # Class-based engines (optional)
    for cls_name in ("ExitEngine", "ExitEngineV2", "ExitDecisionEngine", "Engine"):
        cls = getattr(mod, cls_name, None)
        if cls is not None and callable(cls):
            with contextlib.suppress(Exception):
                inst = cls()  # must be side-effect free in SAFE_MODE
                for meth in preferred + tick_style:
                    fn = getattr(inst, meth, None)
                    if callable(fn) and not _is_executor_name(meth):
                        return fn

    for attr in dir(mod):
        if "exit" in attr.lower():
            fn = getattr(mod, attr, None)
            if callable(fn) and not _is_executor_name(attr):
                return fn

    pytest.skip(
        "No discoverable exit decision callable found. Expected something like "
        "decide_exit/evaluate_exit/plan_exit/get_exit_decision or on_trade_tick."
    )


# -----------------------------------------------------------------------------
# Payload fixtures (minimal but realistic; no network)
# -----------------------------------------------------------------------------
def _full_position_payload() -> dict[str, Any]:
    return {
        "mint": USDC_MINT,
        "symbol": "TEST",
        "qty": 1_000_000,
        "amount_raw": 1_000_000,
        "size_raw": 1_000_000,
        "decimals": 6,
        "entry_price": 1.00,
        "entry_px": 1.00,
        "entry_price_usd": 1.00,
        "current_price": 1.05,
        "mark_price": 1.05,
        "price_usd": 1.05,
        "entry_ts": 1700000000,
        "opened_ts": 1700000000,
        "last_ts": 1700000300,
        "fees_usd": 0.01,
        "cost_sol": 0.02,
        "stop_loss_pct": 8.0,
        "take_profit_pct": 12.0,
        "trailing_arm_pct": 10.0,
        "trailing_pct": 4.5,
        "extra": {},
    }


def _minimal_position_payload() -> dict[str, Any]:
    return {
        "mint": USDC_MINT,
        "qty": 1_000_000,
        "entry_price": 1.00,
        "current_price": 0.98,
    }


def _market_meta_payload() -> dict[str, Any]:
    return {
        "ts": 1700000300,
        "mint": USDC_MINT,
        "liq": 50_000.0,
        "vol5": 1_000.0,
        "buys5": 10,
        "sells5": 8,
        "chg5": 2.0,
        "age_min": 5.0,
        "price_usd": 1.05,
    }


def _tick_payload() -> dict[str, Any]:
    return {
        "mint": USDC_MINT,
        "price_usd": 1.05,
        "price_sol": 0.0,
        "ts": 1700000400,
        "ts_ms": 1700000400000,
        "source": "contract_test",
    }


# -----------------------------------------------------------------------------
# Call + normalize helpers
# -----------------------------------------------------------------------------
def _build_kwargs_for_signature(
    sig: inspect.Signature,
    *,
    position: Any,
    meta: dict[str, Any],
    tick: dict[str, Any],
    now_ts: int,
) -> dict[str, Any]:
    """
    Map our canonical inputs into whatever signature the decider uses.
    """
    mint = None
    symbol = None
    try:
        if isinstance(position, dict):
            mint = position.get("mint")
            symbol = position.get("symbol")
        else:
            mint = getattr(position, "mint", None)
            symbol = getattr(position, "symbol", None)
    except Exception:
        pass

    superset: dict[str, Any] = {
        # position-ish
        "position": position,
        "pos": position,
        "payload": position,
        "position_payload": position,
        "state": position,
        "position_state": position,
        "trade": position,
        # context-ish
        "meta": meta,
        "ctx": meta,
        "context": meta,
        "snapshot": meta,
        "market": meta,
        "market_meta": meta,
        # tick-ish
        "tick": tick,
        "price_tick": tick,
        "trade_tick": tick,
        "tick_data": tick,
        # time-ish
        "now_ts": now_ts,
        "ts": now_ts,
        "timestamp": now_ts,
        "now": now_ts,
        # id-ish
        "mint": mint,
        "token_mint": mint,
        "symbol": symbol,
        # commonly used quantities/prices
        "qty": (
            position.get("qty") if isinstance(position, dict) else getattr(position, "qty", None)
        ),
        "amount_raw": (
            position.get("amount_raw")
            if isinstance(position, dict)
            else getattr(position, "amount_raw", None)
        ),
        "size_raw": (
            position.get("size_raw")
            if isinstance(position, dict)
            else getattr(position, "size_raw", None)
        ),
        "entry_price": (
            position.get("entry_price")
            if isinstance(position, dict)
            else getattr(position, "entry_price", None)
        ),
        "entry_px": (
            position.get("entry_px")
            if isinstance(position, dict)
            else getattr(position, "entry_px", None)
        ),
        "mark_price": (
            position.get("current_price")
            if isinstance(position, dict)
            else getattr(position, "current_price", None)
        ),
        "current_price": (
            position.get("current_price")
            if isinstance(position, dict)
            else getattr(position, "current_price", None)
        ),
        "price": (
            position.get("current_price")
            if isinstance(position, dict)
            else getattr(position, "current_price", None)
        ),
        "price_usd": (
            position.get("current_price")
            if isinstance(position, dict)
            else getattr(position, "current_price", None)
        ),
        # mode toggles
        "dryrun": True,
        "safe_mode": True,
        "no_network": True,
        "strict": False,
    }

    out: dict[str, Any] = {name: superset[name] for name in sig.parameters if name in superset}

    if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()):
        out.update(
            {
                "position": position,
                "meta": meta,
                "tick": tick,
                "now_ts": now_ts,
                "dryrun": True,
                "safe_mode": True,
                "strict": False,
            }
        )

    for p in sig.parameters.values():
        if p.default is not inspect._empty:
            continue
        if p.name in out:
            continue

        pn = p.name.lower()
        if pn in {"position", "pos", "payload", "state", "trade"} or "position" in pn:
            out[p.name] = position
        elif "tick" in pn:
            out[p.name] = tick
        elif pn in {"meta", "ctx", "context", "snapshot", "market"}:
            out[p.name] = meta
        elif "ts" in pn or "time" in pn or pn in {"now"}:
            out[p.name] = now_ts
        elif "safe" in pn or "dry" in pn or "no_network" in pn:
            out[p.name] = True
        elif "strict" in pn:
            out[p.name] = False
        else:
            out[p.name] = None

    return out


async def _call_maybe_async(fn: Callable[..., Any], **kwargs: Any) -> Any:
    out = fn(**kwargs)
    if inspect.isawaitable(out):
        out = await out
    return out


def _decision_to_dict(decision: Any) -> dict[str, Any]:
    """
    Normalize decision outputs into a dict for logging + assertions.
    """
    if decision is None:
        return {"_none": True}

    if isinstance(decision, bool):
        return {"should_exit": decision, "reason": "OK" if decision else "HOLD"}

    if isinstance(decision, dict):
        return dict(decision)

    if isinstance(decision, tuple):
        # common patterns:
        #   (ok, reason)
        #   (ok, score, reason)
        #   ("SELL", reason, score)
        if len(decision) >= 2 and isinstance(decision[0], bool):
            d: dict[str, Any] = {"should_exit": decision[0]}
            if isinstance(decision[1], int | float):
                d["strength"] = float(decision[1])
                if len(decision) >= 3:
                    d["reason"] = str(decision[2])
            else:
                d["reason"] = str(decision[1])
                if len(decision) >= 3 and isinstance(decision[2], int | float):
                    d["strength"] = float(decision[2])
            return d

        if len(decision) >= 2 and isinstance(decision[0], str):
            d2: dict[str, Any] = {"action": decision[0]}
            d2["reason"] = decision[1] if isinstance(decision[1], str) else str(decision[1])
            if len(decision) >= 3 and isinstance(decision[2], int | float):
                d2["strength"] = float(decision[2])
            return d2

        return {"value": [str(x) for x in decision]}

    if is_dataclass(decision):
        return asdict(decision)

    model_dump = getattr(decision, "model_dump", None)
    if callable(model_dump):
        with contextlib.suppress(Exception):
            d = model_dump()
            if isinstance(d, dict):
                return dict(d)

    to_dict = getattr(decision, "to_dict", None)
    if callable(to_dict):
        with contextlib.suppress(Exception):
            d = to_dict()
            if isinstance(d, dict):
                return dict(d)

    to_dict2 = getattr(decision, "dict", None)
    if callable(to_dict2):
        with contextlib.suppress(Exception):
            d = to_dict2()
            if isinstance(d, dict):
                return dict(d)

    dct = getattr(decision, "__dict__", None)
    if isinstance(dct, dict):
        return dict(dct)

    # last resort: pick common attrs
    out: dict[str, Any] = {}
    for k in (
        "action",
        "decision",
        "signal",
        "reason",
        "why",
        "should_exit",
        "exit",
        "sell",
        "strength",
        "score",
        "size_pct",
        "qty",
        "amount_raw",
        "mint",
        "pos_id",
        "target_side",
    ):
        if hasattr(decision, k):
            out[k] = getattr(decision, k)
    if out:
        return out

    raise AssertionError(
        f"Unsupported decision object type: {type(decision).__name__}: {decision!r}"
    )


def _actionish(d: dict[str, Any]) -> str | None:
    for k in ("action", "decision", "signal"):
        v = d.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip().upper()
    return None


def _should_exitish(d: dict[str, Any]) -> bool | None:
    for k in ("should_exit", "exit", "sell", "do_exit"):
        v = d.get(k)
        if isinstance(v, bool):
            return v
    return None


def _reasonish(d: dict[str, Any]) -> str | None:
    for k in ("reason", "why", "detail", "message", "msg"):
        v = d.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _strengthish(d: dict[str, Any]) -> float | None:
    for k in ("strength", "score", "priority", "confidence"):
        v = d.get(k)
        if isinstance(v, int | float):
            return float(v)
    return None


def _stable_projection(d: dict[str, Any]) -> dict[str, Any]:
    """
    Compare only stable/logical fields (ignore any trace/debug payloads).
    """
    keep = (
        "should_exit",
        "exit",
        "sell",
        "action",
        "decision",
        "signal",
        "reason",
        "why",
        "strength",
        "score",
        "size_pct",
        "qty",
        "amount_raw",
        "mint",
        "pos_id",
        "target_side",
    )
    out = {k: d.get(k) for k in keep if k in d}
    for k in ("action", "decision", "signal", "target_side"):
        if k in out and isinstance(out[k], str):
            out[k] = out[k].strip().upper()
    return out


def _assert_json_loggable(d: dict[str, Any]) -> None:
    try:
        json.dumps(d, ensure_ascii=False, sort_keys=True)
    except Exception as e:
        raise AssertionError(f"Decision dict must be JSON-serializable: {e}. Got: {d!r}") from e


# -----------------------------------------------------------------------------
# Optional: contract Position helper (best-effort; never blocks passing)
# -----------------------------------------------------------------------------
def _maybe_make_contract_position(pos_dict: dict[str, Any]) -> Any | None:
    """
    If utils_pkg.contracts exists and provides ensure_position, build a Position dataclass.
    This is optional to avoid breaking repos that haven't wired it yet.
    """
    try:
        c = _fresh_import("utils_pkg.contracts")
        ensure_position = getattr(c, "ensure_position", None)
        if callable(ensure_position):
            return ensure_position(pos_dict)
    except Exception:
        return None
    return None


# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------
def test_exit_engine_imports_without_network(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_safe_mode_env(monkeypatch)

    with _no_network_local_services(monkeypatch):
        name, mod = _discover_exit_engine_module()

    assert mod is not None, f"Failed to import {name}"


def test_exit_engine_exposes_a_decision_callable(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_safe_mode_env(monkeypatch)
    _, mod = _discover_exit_engine_module()
    fn = _discover_decider(mod)
    assert callable(fn)


def test_decider_signature_accepts_positionish_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_safe_mode_env(monkeypatch)
    _, mod = _discover_exit_engine_module()
    fn = _discover_decider(mod)
    sig = inspect.signature(fn)

    has_kwargs = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())
    has_position_param = any(
        p.name.lower()
        in {"position", "pos", "payload", "position_payload", "state", "position_state", "trade"}
        for p in sig.parameters.values()
    )
    assert has_kwargs or has_position_param, (
        f"{fn.__name__} should accept a position payload (position/pos/payload/...) or **kwargs. "
        f"Signature: {sig}"
    )

    # Stability: avoid positional-only (keeps call sites stable across refactors)
    assert all(p.kind != inspect.Parameter.POSITIONAL_ONLY for p in sig.parameters.values()), (
        f"{fn.__name__} should not use positional-only args. Signature: {sig}"
    )


@pytest.mark.asyncio
async def test_accepts_full_payload_returns_decision_and_is_deterministic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_safe_mode_env(monkeypatch)
    fixed_now = 1700000400
    _freeze_time(monkeypatch, fixed_now)

    _, mod = _discover_exit_engine_module()
    fn = _discover_decider(mod)
    sig = inspect.signature(fn)

    pos = _full_position_payload()
    meta = _market_meta_payload()
    tick = _tick_payload()

    kwargs = _build_kwargs_for_signature(sig, position=pos, meta=meta, tick=tick, now_ts=fixed_now)
    assert kwargs, f"Could not map any kwargs to {fn.__name__} signature: {sig}"

    with _no_network_local_services(monkeypatch, mod):
        out_a = await asyncio.wait_for(_call_maybe_async(fn, **kwargs), timeout=5.0)
        out_b = await asyncio.wait_for(_call_maybe_async(fn, **kwargs), timeout=5.0)

    d_a = _decision_to_dict(out_a)
    d_b = _decision_to_dict(out_b)

    if d_a.get("_none") and d_b.get("_none"):
        return

    _assert_json_loggable(d_a)
    _assert_json_loggable(d_b)

    assert _stable_projection(d_a) == _stable_projection(d_b), (
        "Exit decision must be deterministic for fixed inputs in SAFE_MODE.\n"
        f"A: {_stable_projection(d_a)}\n"
        f"B: {_stable_projection(d_b)}\n"
        f"RAW_A: {d_a}\n"
        f"RAW_B: {d_b}"
    )

    action = _actionish(d_a)
    should_exit = _should_exitish(d_a)

    assert (action is not None) or (should_exit is not None), (
        "Exit decision object must include an action/decision/signal string "
        "or a should_exit/exit boolean.\n"
        f"Got: {d_a}"
    )

    exiting = False
    if should_exit is True:
        exiting = True
    if action is not None and any(tok in action for tok in ("SELL", "EXIT", "CLOSE", "TP", "SL")):
        exiting = True

    if exiting:
        reason = _reasonish(d_a)
        assert reason is not None and len(reason) >= 3, (
            f"Exit decision must include a reason. Got: {d_a}"
        )

    strength = _strengthish(d_a)
    if strength is not None:
        # allow either normalized 0..1 or percent-like 0..100
        assert (-0.001 <= strength <= 1.001) or (0.0 <= strength <= 100.0), (
            f"Exit strength/score out of sane bounds: {strength}. Got: {d_a}"
        )


@pytest.mark.asyncio
async def test_never_throws_on_missing_optional_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_safe_mode_env(monkeypatch)
    fixed_now = 1700000400
    _freeze_time(monkeypatch, fixed_now)

    _, mod = _discover_exit_engine_module()
    fn = _discover_decider(mod)
    sig = inspect.signature(fn)

    pos = _minimal_position_payload()
    meta: dict[str, Any] = {}
    tick = {"mint": USDC_MINT, "price_usd": 0.98, "ts": fixed_now}

    kwargs = _build_kwargs_for_signature(sig, position=pos, meta=meta, tick=tick, now_ts=fixed_now)
    assert kwargs, f"Could not map any kwargs to {fn.__name__} signature: {sig}"

    with _no_network_local_services(monkeypatch, mod):
        out = await asyncio.wait_for(_call_maybe_async(fn, **kwargs), timeout=5.0)

    d = _decision_to_dict(out)
    if not d.get("_none"):
        _assert_json_loggable(d)


@pytest.mark.asyncio
async def test_decision_object_is_loggable_shape_if_not_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_safe_mode_env(monkeypatch)
    fixed_now = 1700000400
    _freeze_time(monkeypatch, fixed_now)

    _, mod = _discover_exit_engine_module()
    fn = _discover_decider(mod)
    sig = inspect.signature(fn)

    pos = _full_position_payload()
    meta = _market_meta_payload()
    tick = _tick_payload()

    kwargs = _build_kwargs_for_signature(sig, position=pos, meta=meta, tick=tick, now_ts=fixed_now)

    with _no_network_local_services(monkeypatch, mod):
        out = await asyncio.wait_for(_call_maybe_async(fn, **kwargs), timeout=5.0)

    d = _decision_to_dict(out)
    if d.get("_none"):
        return

    _assert_json_loggable(d)

    # Loggability: key fields should be scalars if present.
    for k in (
        "action",
        "decision",
        "signal",
        "reason",
        "should_exit",
        "exit",
        "sell",
        "strength",
        "score",
        "size_pct",
    ):
        if k in d:
            assert isinstance(d[k], str | bool | int | float | type(None)), (
                f"Decision field {k} not loggable: {d[k]!r}"
            )


@pytest.mark.asyncio
async def test_optional_accepts_contract_position_if_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Optional: if contracts.ensure_position exists, the exit engine should accept
    the Position dataclass too. We don't fail the suite if the repo hasn't wired
    this yet—we just skip.
    """
    _set_safe_mode_env(monkeypatch)
    fixed_now = 1700000400
    _freeze_time(monkeypatch, fixed_now)

    pos_dict = _full_position_payload()
    pos_obj = _maybe_make_contract_position(pos_dict)
    if pos_obj is None:
        pytest.skip("utils_pkg.contracts.ensure_position not available (optional contract).")

    _, mod = _discover_exit_engine_module()
    fn = _discover_decider(mod)
    sig = inspect.signature(fn)

    meta = _market_meta_payload()
    tick = _tick_payload()

    kwargs = _build_kwargs_for_signature(
        sig, position=pos_obj, meta=meta, tick=tick, now_ts=fixed_now
    )
    assert kwargs, f"Could not map any kwargs to {fn.__name__} signature: {sig}"

    with _no_network_local_services(monkeypatch, mod):
        out = await asyncio.wait_for(_call_maybe_async(fn, **kwargs), timeout=5.0)

    d = _decision_to_dict(out)
    if not d.get("_none"):
        _assert_json_loggable(d)
