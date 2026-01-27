# scripts/ci_smoke.py
from __future__ import annotations

import asyncio
import importlib
import os
import socket
import sys
import traceback
import urllib.request
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType
from typing import Any, Callable


# -----------------------------------------------------------------------------
# Path bootstrap (CRITICAL for pre-commit)
#
# If executed as:
#   python scripts/ci_smoke.py
# then sys.path[0] becomes "scripts/" and imports like "utils_pkg" can fail.
# We force repo root (+ optional src/) onto sys.path.
# -----------------------------------------------------------------------------
def _bootstrap_sys_path() -> Path:
    here = Path(__file__).resolve()
    repo_root = here.parents[1]  # <repo>/scripts/ci_smoke.py -> <repo>

    root_str = str(repo_root)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)

    src = repo_root / "src"
    if src.is_dir():
        src_str = str(src)
        if src_str not in sys.path:
            sys.path.insert(0, src_str)

    return repo_root


REPO_ROOT = _bootstrap_sys_path()


# -----------------------------------------------------------------------------
# Environment: force safe/no-network execution
#
# NOTE: Do NOT add new env keys here unless they are already in the contract.
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


def _setattr_if_present(obj: Any, name: str, value: Any) -> None:
    """
    setattr wrapper used to avoid mypy 'Cannot assign to a type' / attr-defined errors
    while keeping behavior identical.
    """
    try:
        setattr(obj, name, value)
    except Exception:
        # If an attribute is readonly or missing on some platforms, ignore;
        # the smoke still blocks the primary channels.
        pass


def _wrap_blocked(msg: str) -> Callable[..., Any]:
    def _blocked(*_args: Any, **_kwargs: Any) -> Any:
        raise NetworkBlockedError(msg)

    return _blocked


@contextmanager
def block_network() -> Iterator[None]:
    """
    Block common outbound network pathways. If anything tries to open a socket / HTTP client,
    it should fail fast with a clear error.

    This is intentionally strict: CI smoke must not touch the network.
    """
    # --- socket guards ---
    orig_socket = getattr(socket, "socket", None)
    orig_create_conn = getattr(socket, "create_connection", None)
    orig_getaddrinfo = getattr(socket, "getaddrinfo", None)

    _setattr_if_present(
        socket, "socket", _wrap_blocked("Network is blocked in ci_smoke.py (socket.socket).")
    )
    if callable(orig_create_conn):
        _setattr_if_present(
            socket,
            "create_connection",
            _wrap_blocked("Network is blocked in ci_smoke.py (socket.create_connection)."),
        )
    if callable(orig_getaddrinfo):
        _setattr_if_present(
            socket,
            "getaddrinfo",
            _wrap_blocked("Network is blocked in ci_smoke.py (socket.getaddrinfo)."),
        )

    # --- urllib guard (stdlib) ---
    orig_urlopen = getattr(urllib.request, "urlopen", None)
    _setattr_if_present(
        urllib.request,
        "urlopen",
        _wrap_blocked("Network is blocked in ci_smoke.py (urllib.request.urlopen)."),
    )

    # --- urllib guard (stdlib) ---
    orig_urlopen = urllib.request.urlopen

    def guarded_urlopen(*args: Any, **kwargs: Any) -> Any:
        raise NetworkBlockedError("Network is blocked in ci_smoke.py (urllib.request.urlopen).")

    urllib.request.urlopen = guarded_urlopen  # type: ignore[assignment]

    # --- requests guard (if installed) ---
    requests_mod: Any | None = None
    orig_requests_request: Any = None
    orig_requests_get: Any = None
    orig_requests_post: Any = None
    try:
        import requests as _requests  # type: ignore[import-not-found]

        requests_mod = _requests
        orig_requests_request = getattr(_requests, "request", None)
        orig_requests_get = getattr(_requests, "get", None)
        orig_requests_post = getattr(_requests, "post", None)

        if callable(orig_requests_request):
            _setattr_if_present(
                _requests, "request", _wrap_blocked("Network is blocked (requests.request).")
            )
        if callable(orig_requests_get):
            _setattr_if_present(
                _requests, "get", _wrap_blocked("Network is blocked (requests.get).")
            )
        if callable(orig_requests_post):
            _setattr_if_present(
                _requests, "post", _wrap_blocked("Network is blocked (requests.post).")
            )
    except Exception:
        pass

    # --- aiohttp guard (if installed) ---
    aiohttp_mod: Any | None = None
    orig_aiohttp_session: Any = None
    try:
        import aiohttp as _aiohttp  # type: ignore[import-not-found]

        aiohttp_mod = _aiohttp
        orig_aiohttp_session = getattr(_aiohttp, "ClientSession", None)

        class _BlockedClientSession:
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                raise NetworkBlockedError(
                    "Network is blocked in ci_smoke.py (aiohttp.ClientSession)."
                )

        if orig_aiohttp_session is not None:
            _setattr_if_present(_aiohttp, "ClientSession", _BlockedClientSession)
    except Exception:
        pass

    # --- httpx guard (if installed) ---
    httpx_mod: Any | None = None
    orig_httpx_client: Any = None
    orig_httpx_async_client: Any = None
    try:
        import httpx as _httpx  # type: ignore[import-not-found]

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
            _setattr_if_present(_httpx, "Client", _BlockedClient)
        if orig_httpx_async_client is not None:
            _setattr_if_present(_httpx, "AsyncClient", _BlockedAsyncClient)
    except Exception:
        pass

    try:
        yield
    finally:
        # Restore everything we changed (best-effort; do not fail on restore)
        if orig_socket is not None:
            _setattr_if_present(socket, "socket", orig_socket)
        if callable(orig_create_conn):
            _setattr_if_present(socket, "create_connection", orig_create_conn)
        if callable(orig_getaddrinfo):
            _setattr_if_present(socket, "getaddrinfo", orig_getaddrinfo)

        if orig_urlopen is not None:
            _setattr_if_present(urllib.request, "urlopen", orig_urlopen)

        urllib.request.urlopen = orig_urlopen  # type: ignore[assignment]

        if requests_mod is not None:
            if callable(orig_requests_request):
                _setattr_if_present(requests_mod, "request", orig_requests_request)
            if callable(orig_requests_get):
                _setattr_if_present(requests_mod, "get", orig_requests_get)
            if callable(orig_requests_post):
                _setattr_if_present(requests_mod, "post", orig_requests_post)

        if aiohttp_mod is not None and orig_aiohttp_session is not None:
            _setattr_if_present(aiohttp_mod, "ClientSession", orig_aiohttp_session)

        if httpx_mod is not None:
            if orig_httpx_client is not None:
                _setattr_if_present(httpx_mod, "Client", orig_httpx_client)
            if orig_httpx_async_client is not None:
                _setattr_if_present(httpx_mod, "AsyncClient", orig_httpx_async_client)


# -----------------------------------------------------------------------------
# Import + env contract probing
# -----------------------------------------------------------------------------
def try_import(name: str) -> tuple[bool, ModuleType | None, str | None, str | None]:
    try:
        mod = importlib.import_module(name)
        return True, mod, None, None
    except Exception as e:
        return False, None, f"{type(e).__name__}: {e}", traceback.format_exc()


def _run_maybe_async(result: Any) -> None:
    """
    Run coroutine-like return values safely.
    Supports coroutines and generic awaitables by wrapping them.
    """
    import inspect

    if inspect.iscoroutine(result):
        asyncio.run(result)
        return

    if inspect.isawaitable(result):
        aw = result

        async def _wrap() -> Any:
            return await aw

        asyncio.run(_wrap())


def call_env_validator(mod: ModuleType) -> tuple[bool, str]:
    """
    if hasattr(result, "__await__"):
        asyncio.run(result)  # type: ignore[arg-type]


def call_env_validator(mod: Any) -> tuple[bool, str]:
    """
    Best-effort env validation call (NON-STRICT).
    Supports common function names used in your overhaul.
    Must never require network; block_network() guarantees that.
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
            _run_maybe_async(out)
            return True, f"env validator OK via {mod.__name__}.{fn_name}"
        except Exception as e:
            return (
                False,
                f"env validator FAILED via {mod.__name__}.{fn_name}: {type(e).__name__}: {e}",
            )

    return True, f"no env validator found in {mod.__name__} (skipped)"


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main() -> int:
    force_safe_env()

    # Minimal, high-signal import targets (the spine)
    targets: list[str] = [
        "utils_pkg.core_utils",
        "utils_pkg.router",
        "utils_pkg.feature_builder",
        "utils_pkg.ai_entry_logic",
        "utils_pkg.exit_engine",
        # Optional: only skip if it is truly missing. If it exists but errors, we want to know.
        "utils_pkg.auto_exit_worker.exit_engine",
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

                # Only validate env contract from the canonical spine module
                if name == "utils_pkg.core_utils":
                    v_ok, msg = call_env_validator(mod)
                    print(f"[ci_smoke] {msg}")
                    if not v_ok:
                        failures.append(msg)
                continue

            # Optional module handling: skip ONLY if module is missing.
            if name == "utils_pkg.auto_exit_worker.exit_engine" and err:
                if err.startswith("ModuleNotFoundError") or err.startswith("ImportError"):
                    print(f"[ci_smoke] import SKIP (optional missing): {name} ({err})")
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
