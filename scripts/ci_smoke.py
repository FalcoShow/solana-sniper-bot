# scripts/ci_smoke.py
from __future__ import annotations

import asyncio
import importlib
import os
import socket
import sys
import traceback
from contextlib import contextmanager
from pathlib import Path
from typing import Any


# -----------------------------------------------------------------------------
# Path bootstrap (CRITICAL for pre-commit)
#
# If this file is executed as:
#   python scripts/ci_smoke.py
# then sys.path[0] becomes "scripts/" and imports like "utils_pkg" fail.
# We force repo root (+ optional src/) onto sys.path.
# -----------------------------------------------------------------------------
def _bootstrap_sys_path() -> Path:
    here = Path(__file__).resolve()
    repo_root = here.parents[1]  # <repo>/scripts/ci_smoke.py -> <repo>

    # Put repo root first
    root_str = str(repo_root)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)

    # If repo uses src layout, add it too
    src = repo_root / "src"
    if src.is_dir():
        src_str = str(src)
        if src_str not in sys.path:
            sys.path.insert(0, src_str)

    return repo_root


REPO_ROOT = _bootstrap_sys_path()


# -----------------------------------------------------------------------------
# Environment: force safe/no-network execution
# -----------------------------------------------------------------------------
def force_safe_env() -> None:
    """
    Force a deterministic, no-network, CI-friendly environment.
    These toggles are intentionally redundant so whichever your code recognizes will apply.
    """
    os.environ.setdefault("SAFE_MODE", "1")
    os.environ.setdefault("DRYRUN", "1")
    os.environ.setdefault("NO_NETWORK", "1")
    os.environ.setdefault("DISABLE_NETWORK", "1")
    os.environ.setdefault("CI", "1")
    os.environ.setdefault("PYTHONUNBUFFERED", "1")

    # Some codepaths gate on these names:
    os.environ.setdefault("CHECK_ENV_JSON", "1")
    os.environ.setdefault("CHECK_ENV_JSON_ONLY", "1")


# -----------------------------------------------------------------------------
# Hard network guard
# -----------------------------------------------------------------------------
class NetworkBlockedError(RuntimeError):
    pass


@contextmanager
def block_network() -> Any:
    """
    Block common outbound network pathways. If anything tries to open a socket / HTTP client,
    it should fail fast with a clear error.
    """
    # --- socket guards ---
    orig_socket = socket.socket
    orig_create_conn = getattr(socket, "create_connection", None)
    orig_getaddrinfo = getattr(socket, "getaddrinfo", None)

    def guarded_socket(*args: Any, **kwargs: Any) -> Any:
        raise NetworkBlockedError("Network is blocked in ci_smoke.py (socket.socket).")

    def guarded_create_connection(*args: Any, **kwargs: Any) -> Any:
        raise NetworkBlockedError("Network is blocked in ci_smoke.py (socket.create_connection).")

    def guarded_getaddrinfo(*args: Any, **kwargs: Any) -> Any:
        raise NetworkBlockedError("Network is blocked in ci_smoke.py (socket.getaddrinfo).")

    socket.socket = guarded_socket  # type: ignore[assignment]
    if callable(orig_create_conn):
        socket.create_connection = guarded_create_connection  # type: ignore[assignment]
    if callable(orig_getaddrinfo):
        socket.getaddrinfo = guarded_getaddrinfo  # type: ignore[assignment]

    # --- requests guard (if installed) ---
    requests_mod = None
    orig_requests_request = None
    orig_requests_get = None
    orig_requests_post = None
    try:
        import requests as _requests  # type: ignore

        requests_mod = _requests
        orig_requests_request = getattr(_requests, "request", None)
        orig_requests_get = getattr(_requests, "get", None)
        orig_requests_post = getattr(_requests, "post", None)

        def _blocked(*args: Any, **kwargs: Any) -> Any:
            raise NetworkBlockedError("Network is blocked in ci_smoke.py (requests).")

        if callable(orig_requests_request):
            _requests.request = _blocked  # type: ignore[assignment]
        if callable(orig_requests_get):
            _requests.get = _blocked  # type: ignore[assignment]
        if callable(orig_requests_post):
            _requests.post = _blocked  # type: ignore[assignment]
    except Exception:
        pass

    # --- aiohttp guard (if installed) ---
    aiohttp_mod = None
    orig_aiohttp_session = None
    try:
        import aiohttp as _aiohttp  # type: ignore

        aiohttp_mod = _aiohttp
        orig_aiohttp_session = getattr(_aiohttp, "ClientSession", None)

        class _BlockedSession:
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                raise NetworkBlockedError(
                    "Network is blocked in ci_smoke.py (aiohttp.ClientSession)."
                )

        if orig_aiohttp_session is not None:
            _aiohttp.ClientSession = _BlockedSession  # type: ignore[assignment]
    except Exception:
        pass

    # --- httpx guard (if installed) ---
    httpx_mod = None
    orig_httpx_client = None
    orig_httpx_async_client = None
    try:
        import httpx as _httpx  # type: ignore

        httpx_mod = _httpx
        orig_httpx_client = getattr(_httpx, "Client", None)
        orig_httpx_async_client = getattr(_httpx, "AsyncClient", None)

        class _BlockedClient:
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                raise NetworkBlockedError("Network is blocked in ci_smoke.py (httpx.Client).")

        class _BlockedAsyncClient:
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                raise NetworkBlockedError("Network is blocked in ci_smoke.py (httpx.AsyncClient).")

        if orig_httpx_client is not None:
            _httpx.Client = _BlockedClient  # type: ignore[assignment]
        if orig_httpx_async_client is not None:
            _httpx.AsyncClient = _BlockedAsyncClient  # type: ignore[assignment]
    except Exception:
        pass

    try:
        yield
    finally:
        # Restore everything we changed
        socket.socket = orig_socket  # type: ignore[assignment]
        if callable(orig_create_conn):
            socket.create_connection = orig_create_conn  # type: ignore[assignment]
        if callable(orig_getaddrinfo):
            socket.getaddrinfo = orig_getaddrinfo  # type: ignore[assignment]

        if requests_mod is not None:
            if callable(orig_requests_request):
                requests_mod.request = orig_requests_request  # type: ignore[assignment]
            if callable(orig_requests_get):
                requests_mod.get = orig_requests_get  # type: ignore[assignment]
            if callable(orig_requests_post):
                requests_mod.post = orig_requests_post  # type: ignore[assignment]

        if aiohttp_mod is not None and orig_aiohttp_session is not None:
            aiohttp_mod.ClientSession = orig_aiohttp_session  # type: ignore[assignment]

        if httpx_mod is not None:
            if orig_httpx_client is not None:
                httpx_mod.Client = orig_httpx_client  # type: ignore[assignment]
            if orig_httpx_async_client is not None:
                httpx_mod.AsyncClient = orig_httpx_async_client  # type: ignore[assignment]


# -----------------------------------------------------------------------------
# Import + env contract probing
# -----------------------------------------------------------------------------
def try_import(name: str) -> tuple[bool, Any | None, str | None, str | None]:
    try:
        mod = importlib.import_module(name)
        return True, mod, None, None
    except Exception as e:
        return False, None, f"{type(e).__name__}: {e}", traceback.format_exc()


def call_env_validator(mod: Any) -> tuple[bool, str]:
    """
    Best-effort env validation call (NON-STRICT).
    Supports common function names used in your overhaul.

    This must never require network; block_network() guarantees that.
    """
    candidates = (
        "validate_env_contract",
        "validate_env",
        "check_env",
        "require_env",
        "ensure_env",
        "assert_env",
    )
    for fn_name in candidates:
        fn = getattr(mod, fn_name, None)
        if not callable(fn):
            continue

        try:
            kwargs: dict[str, Any] = {}
            try:
                import inspect

                sig = inspect.signature(fn)
                if "strict" in sig.parameters:
                    kwargs["strict"] = False
                if "safe_mode" in sig.parameters:
                    kwargs["safe_mode"] = True
                if "dryrun" in sig.parameters:
                    kwargs["dryrun"] = True
            except Exception:
                pass

            out = fn(**kwargs)
            if hasattr(out, "__await__"):
                asyncio.run(out)  # type: ignore[arg-type]

            return True, f"env validator OK via {mod.__name__}.{fn_name}"
        except Exception as e:
            return (
                False,
                f"env validator FAILED via {mod.__name__}.{fn_name}: {type(e).__name__}: {e}",
            )

    return True, f"no env validator found in {mod.__name__} (skipped)"


def main() -> int:
    force_safe_env()

    # Minimal, high-signal import targets (the spine)
    targets: list[str] = [
        "utils_pkg.core_utils",
        "utils_pkg.router",
        "utils_pkg.feature_builder",
        "utils_pkg.ai_entry_logic",
        "utils_pkg.exit_engine",
        "utils_pkg.auto_exit_worker.exit_engine",  # optional path
    ]

    print("[ci_smoke] SAFE_MODE/DRYRUN/NO_NETWORK enabled")
    print(f"[ci_smoke] repo_root={REPO_ROOT}")
    print(f"[ci_smoke] cwd={Path.cwd()}")
    print(f"[ci_smoke] sys.path[0]={sys.path[0]}")

    failures: list[str] = []
    last_tb: str | None = None

    print("[ci_smoke] Blocking outbound network...")

    with block_network():
        for name in targets:
            ok, mod, err, tb = try_import(name)
            if ok and mod is not None:
                print(f"[ci_smoke] import OK: {name}")
                if name == "utils_pkg.core_utils":
                    v_ok, msg = call_env_validator(mod)
                    print(f"[ci_smoke] {msg}")
                    if not v_ok:
                        failures.append(msg)
            else:
                # tolerate missing optional exit_engine path
                if name == "utils_pkg.auto_exit_worker.exit_engine":
                    print(f"[ci_smoke] import SKIP (optional path): {name} ({err})")
                    continue

                failures.append(f"import FAILED: {name} ({err})")
                print(f"[ci_smoke] import FAILED: {name} ({err})")
                last_tb = tb or last_tb

    if failures:
        print("\n[ci_smoke] FAILURES:")
        for f in failures:
            print("  -", f)

        if last_tb:
            print("\n[ci_smoke] Traceback (last failure):")
            print(last_tb)
        return 1

    print("\n[ci_smoke] SUCCESS: spine imports OK, env contract sane, network remained blocked.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
