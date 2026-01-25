# tests/test_contract_startup.py
from __future__ import annotations

"""
tests/test_contract_startup.py — Startup/Boot Contract (P0 LOCK-IN)
==================================================================

Goal
----
Guarantee the *app entrypoint* is import-safe and lazy in SAFE/DRYRUN mode:

✅ Import succeeds with NO secrets and NO network.
✅ Import does NOT create aiohttp sessions, Redis/DB connections, or background tasks.
✅ Entrypoint module exposes at least one canonical callable (build_* / create_* / main_async / run).
✅ If a pure "builder" exists, calling it in SAFE mode must not crash and must not touch network.

Why this matters
----------------
This is the final guard against "works locally / breaks on Render" regressions:
- no side-effects at import time
- explicit startup only
- safe-mode boot path stays stable forever

Notes
-----
- tests/conftest.py already blocks outbound network; THIS test further blocks *all* TCP connects
  during the critical import/call window to prevent accidental localhost Redis/Postgres/RPC.
- We are intentionally tolerant on exact entrypoint name (scripts.main vs sniper.main, etc.),
  but strict that at least one canonical callable exists.

If your repo uses a non-standard entrypoint module, you may set:
  APP_ENTRYPOINT_MODULE="your.module.path"
(or ENTRYPOINT_MODULE)

Run
---
pytest -q
"""

import asyncio
import contextlib
import importlib
import inspect
import os
import socket
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.contract


# -----------------------------------------------------------------------------
# Repo root + import path helpers
# -----------------------------------------------------------------------------
def _find_repo_root(start: Path) -> Path:
    start = start.resolve()
    markers = (
        ".env.example",
        "pyproject.toml",
        "pytest.ini",
        ".git",
        "requirements.txt",
        "setup.cfg",
        "render.yaml",
        ".renderignore",
    )
    best: Path | None = None

    for p in (start, *start.parents):
        if (p / ".env.example").exists():
            return p
        if best is None and any((p / m).exists() for m in markers):
            best = p
        if (p / "utils_pkg").is_dir():
            best = best or p
    return best or (start.parents[0] if start.parents else start)


def _repo_root() -> Path:
    rr = (os.environ.get("REPO_ROOT") or "").strip()
    if rr:
        try:
            p = Path(rr).resolve()
            if p.exists():
                return p
        except Exception:
            pass
    return _find_repo_root(Path(__file__).resolve().parent)


def _ensure_import_paths(root: Path) -> None:
    root = root.resolve()
    candidates = [root]
    src = root / "src"
    if src.is_dir():
        candidates.append(src)
    for p in candidates:
        ps = str(p)
        if ps and ps not in sys.path:
            sys.path.insert(0, ps)


# -----------------------------------------------------------------------------
# Safe env fixture: force offline, non-strict, no side effects
# -----------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _safe_env(monkeypatch: pytest.MonkeyPatch) -> None:
    # Non-strict env: tests must not require secrets
    monkeypatch.setenv("ENV_STRICT", "0")
    monkeypatch.setenv("STRICT_ENV", "0")
    monkeypatch.setenv("CORE_UTILS_ENV_STRICT", "0")
    monkeypatch.setenv("LOG_LEVEL", os.getenv("LOG_LEVEL", "WARNING"))

    # Universal "no network / no rpc / safe mode" toggles
    monkeypatch.setenv("SAFE_MODE", "1")
    monkeypatch.setenv("DRYRUN", "1")
    monkeypatch.setenv("DRY_RUN", "1")
    monkeypatch.setenv("NO_NETWORK", "1")
    monkeypatch.setenv("DISABLE_NETWORK", "1")
    monkeypatch.setenv("DISABLE_RPC", "1")
    monkeypatch.setenv("DISABLE_HTTP", "1")
    monkeypatch.setenv("SANDBOX_MODE", "1")

    # Prevent dotenv/autoload side effects if supported
    monkeypatch.setenv("CORE_LOAD_DOTENV", "0")

    # Strong "do not trade" toggles (harmless if unused)
    monkeypatch.setenv("LIVE_TRADING", "0")
    monkeypatch.setenv("TRADING_ENABLED", "0")
    monkeypatch.setenv("ALLOW_BUYS", "0")
    monkeypatch.setenv("ALLOW_SELLS", "0")
    monkeypatch.setenv("DISABLE_LIVE_TRADING", "1")

    # CI hints
    monkeypatch.setenv("CI", "1")
    monkeypatch.setenv("PYTEST_MODE", "1")
    monkeypatch.setenv("TEST", "1")
    monkeypatch.setenv("BACKTEST_MODE", "1")


# -----------------------------------------------------------------------------
# Network guard: block ALL TCP connects during import/call windows
# -----------------------------------------------------------------------------
class NetworkBlockedError(RuntimeError):
    """Raised when startup contract tests detect a TCP connect attempt."""


def _extract_host_port(sockaddr: Any) -> tuple[str | None, int | None]:
    if isinstance(sockaddr, tuple) and len(sockaddr) >= 2:
        host = sockaddr[0]
        port = sockaddr[1]
        return (str(host), int(port) if isinstance(port, int) else None)
    return (None, None)


@contextlib.contextmanager
def _no_tcp_connect(monkeypatch: pytest.MonkeyPatch):
    """
    Prevent *any* TCP connects inside this scope (including localhost).
    Allows AF_UNIX socket path strings/bytes to avoid breaking local IPC used by Python internals.
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
            h = host or "<unknown>"
            p = port if port is not None else "<unknown>"
            raise NetworkBlockedError(
                "Network is blocked in startup contract tests.\n"
                f"Blocked at: socket.create_connection\nDestination: {h}:{p}\n"
                "Fix: do not connect at import-time; only connect after explicit start()."
            )

        def connect(self: socket.socket, address: Any) -> Any:
            # AF_UNIX path string: allow
            if isinstance(address, str | bytes):
                return orig_connect(self, address)

            host, port = _extract_host_port(address)
            h = host or "<unknown>"
            p = port if port is not None else "<unknown>"
            raise NetworkBlockedError(
                "Network is blocked in startup contract tests.\n"
                f"Blocked at: socket.socket.connect\nDestination: {h}:{p}\n"
                "Fix: do not connect at import-time; only connect after explicit start()."
            )

        def connect_ex(self: socket.socket, address: Any) -> Any:
            # AF_UNIX path string: allow
            if isinstance(address, str | bytes):
                return orig_connect_ex(self, address)

            host, port = _extract_host_port(address)
            h = host or "<unknown>"
            p = port if port is not None else "<unknown>"
            raise NetworkBlockedError(
                "Network is blocked in startup contract tests.\n"
                f"Blocked at: socket.socket.connect_ex\nDestination: {h}:{p}\n"
                "Fix: do not connect at import-time; only connect after explicit start()."
            )

        m.setattr(socket, "create_connection", create_connection, raising=True)
        m.setattr(socket.socket, "connect", connect, raising=True)
        m.setattr(socket.socket, "connect_ex", connect_ex, raising=True)
        yield


# -----------------------------------------------------------------------------
# Entrypoint discovery + call helpers
# -----------------------------------------------------------------------------
def _fresh_import(module_name: str) -> Any:
    for name in list(sys.modules.keys()):
        if name == module_name or name.startswith(module_name + "."):
            sys.modules.pop(name, None)
    return importlib.import_module(module_name)


def _candidate_entry_modules(root: Path) -> list[str]:
    """
    Prefer explicit env override, else try common repo entrypoints.
    Also consider file existence to reduce false attempts.
    """
    override = (os.getenv("APP_ENTRYPOINT_MODULE") or os.getenv("ENTRYPOINT_MODULE") or "").strip()
    if override:
        return [override]

    # file-based hints (prefer scripts/main.py)
    candidates: list[tuple[str, str]] = [
        ("scripts/main.py", "scripts.main"),
        ("scripts/__main__.py", "scripts.__main__"),
        ("main.py", "main"),
        ("sniper/main.py", "sniper.main"),
        ("sniper/__main__.py", "sniper.__main__"),
        ("app/main.py", "app.main"),
        ("service/main.py", "service.main"),
        ("scripts/entrypoint.py", "scripts.entrypoint"),
        ("scripts/run.py", "scripts.run"),
        ("scripts/start.py", "scripts.start"),
    ]

    # also src/ layout
    candidates += [(f"src/{rel}", mod) for (rel, mod) in candidates]

    out: list[str] = []
    for rel, mod in candidates:
        if (root / rel).exists() and mod not in out:
            out.append(mod)

    # fallback tries (if file layout is unusual)
    fallback = [
        "scripts.main",
        "sniper.main",
        "main",
        "app.main",
        "service.main",
        "scripts.entrypoint",
        "scripts.run",
        "scripts.start",
    ]
    for mod in fallback:
        if mod not in out:
            out.append(mod)

    return out


def _import_entry_module(root: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    _ensure_import_paths(root)
    tried: list[tuple[str, str]] = []
    for mod_name in _candidate_entry_modules(root):
        try:
            with _no_tcp_connect(monkeypatch):
                return _fresh_import(mod_name)
        except Exception as e:
            tried.append((mod_name, f"{type(e).__name__}: {e}"))
            continue

    msg = [
        "Could not import any entrypoint module in SAFE/DRYRUN mode.",
        "Tried modules (first error per module):",
        *[f" - {m}: {err}" for m, err in tried[:20]],
        "",
        "Fix options:",
        "  1) Ensure your entrypoint exists (recommended: scripts/main.py -> module scripts.main).",
        "  2) Or set APP_ENTRYPOINT_MODULE to the correct module path.",
        "     Example: APP_ENTRYPOINT_MODULE=sniper.main",
    ]
    raise AssertionError("\n".join(msg))


def _discover_callables(mod: Any) -> dict[str, Callable[..., Any]]:
    names = (
        # builder-first (preferred: side-effect free)
        "build_app",
        "create_app",
        "make_app",
        "build_services",
        "wire_services",
        "build_runtime",
        "build_system",
        # runners (may start loops; tests avoid calling unless safe)
        "main_async",
        "run_async",
        "start_async",
        "main",
        "run",
        "start",
    )
    found: dict[str, Callable[..., Any]] = {}
    for n in names:
        fn = getattr(mod, n, None)
        if callable(fn):
            found[n] = fn
    return found


def _fill_kwargs_best_effort(sig: inspect.Signature) -> dict[str, Any]:
    """
    Build a minimal kwargs map that satisfies required params while keeping things safe.
    Unknown required params are passed as None (contract: should not crash in SAFE mode).
    """
    safe_pool: dict[str, Any] = {
        "dryrun": True,
        "dry_run": True,
        "safe_mode": True,
        "no_network": True,
        "disable_network": True,
        "sandbox": True,
        "sandbox_mode": True,
        "test_mode": True,
        "ci": True,
        "once": True,
        "single_tick": True,
        "max_ticks": 1,
        "ticks": 1,
        "iterations": 1,
        "loop_once": True,
        "timeout_s": 1.0,
        "timeout": 1.0,
        "argv": [],
        "args": [],
        "config": None,
        "ctx": {"source": "tests", "mode": "contract"},
        "logger": None,
    }

    kwargs: dict[str, Any] = {}
    for p in sig.parameters.values():
        if p.kind == inspect.Parameter.VAR_POSITIONAL:
            continue
        if p.kind == inspect.Parameter.VAR_KEYWORD:
            continue
        if p.default is not inspect._empty:
            continue

        pn = p.name
        key = pn.lower()
        if pn in safe_pool:
            kwargs[pn] = safe_pool[pn]
        elif key in safe_pool:
            kwargs[pn] = safe_pool[key]
        else:
            # last resort: safe None (should be tolerated in SAFE mode)
            kwargs[pn] = None

    # include common optional safety flags if accepted
    for k in ("dryrun", "safe_mode", "no_network", "max_ticks", "once", "timeout_s"):
        if k in sig.parameters and k not in kwargs:
            kwargs[k] = safe_pool[k]

    return kwargs


async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------
def test_entrypoint_import_is_side_effect_free(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Contract: importing the entrypoint in SAFE/DRYRUN must not open any TCP sockets.
    """
    root = _repo_root()
    mod = _import_entry_module(root, monkeypatch)
    assert mod is not None


def test_import_does_not_create_core_sessions_or_background_tasks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Contract: entrypoint import must not eagerly create aiohttp sessions or background tasks.
    We validate via utils_pkg.core_utils invariants.
    """
    root = _repo_root()
    _ = _import_entry_module(root, monkeypatch)

    _ensure_import_paths(root)
    cu = importlib.import_module("utils_pkg.core_utils")

    assert hasattr(cu, "_aiohttp_session"), (
        "LOCK-IN: utils_pkg.core_utils must define _aiohttp_session (lazy session handle)."
    )
    assert getattr(cu, "_aiohttp_session", None) is None, (
        "LOCK-IN: core_utils created aiohttp session at import-time. Must be lazy."
    )

    bg = getattr(cu, "_BACKGROUND_TASKS", None)
    assert isinstance(bg, set), "LOCK-IN: core_utils must define _BACKGROUND_TASKS as a set."
    assert len(bg) == 0, "LOCK-IN: background tasks started at import-time (must be explicit)."


def test_entrypoint_exposes_canonical_callable(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Contract: entrypoint module must expose at least one canonical callable
    (preferably a pure builder like build_app/create_app).
    """
    root = _repo_root()
    mod = _import_entry_module(root, monkeypatch)

    found = _discover_callables(mod)
    assert found, (
        "LOCK-IN: entrypoint must expose at least one callable among:\n"
        "  build_app/create_app/make_app/build_services/wire_services/build_runtime/build_system\n"
        "  main_async/run_async/start_async/main/run/start\n"
        "If your project uses a different name, add a thin wrapper with one of these names."
    )

    # If *_async exists, it must truly be async
    for nm in ("main_async", "run_async", "start_async"):
        fn = found.get(nm)
        if fn is not None:
            assert asyncio.iscoroutinefunction(fn), f"{nm} must be async def"


@pytest.mark.asyncio
async def test_builder_smoke_safe_mode_no_tcp_connect(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    If a pure builder exists, calling it in SAFE mode must not crash and must not connect.
    We DO NOT run infinite loops here.
    """
    root = _repo_root()
    mod = _import_entry_module(root, monkeypatch)

    found = _discover_callables(mod)

    # Prefer builder-style callables only (do not call run/start)
    builder_names = (
        "build_app",
        "create_app",
        "make_app",
        "build_services",
        "wire_services",
        "build_runtime",
        "build_system",
    )
    builder = None
    for n in builder_names:
        if n in found:
            builder = found[n]
            break

    if builder is None:
        pytest.skip("No builder-style callable found; import+surface contracts already enforced.")

    sig = inspect.signature(builder)
    kwargs = _fill_kwargs_best_effort(sig)

    # Guard ONLY around the call-under-test (avoid messing with pytest teardown)
    with _no_tcp_connect(monkeypatch):
        out = builder(**kwargs)
        out = await asyncio.wait_for(_maybe_await(out), timeout=3.0)

    # We don't enforce a specific return type; just that it doesn't explode in SAFE mode.
    _ = out
