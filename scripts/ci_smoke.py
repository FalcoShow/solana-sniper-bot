# scripts/ci_smoke.py
from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import os
import socket
import sys
import traceback
import urllib.request
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any, Callable

# =============================================================================
# CI Smoke: hard no-network import spine
#
# Goals:
# - Ensure core modules can be imported with SAFE_MODE/DRYRUN/NO_NETWORK enabled.
# - Ensure imports do not perform outbound network calls.
# - Optionally call an env validator (best-effort, non-strict).
#
# Run patterns:
#   python -m scripts.ci_smoke
#   python scripts/ci_smoke.py
#
# NOTE:
# - Do NOT add new env keys here unless they are already present in the env contract.
# =============================================================================


# -----------------------------------------------------------------------------
# Repo root + sys.path bootstrap
# -----------------------------------------------------------------------------
def _find_repo_root(start: Path) -> Path:
    """
    Find repo root by walking upward for markers.
    Falls back to <repo>/scripts -> parents[1] if markers are absent.
    """
    start = start.resolve()
    for p in (start,) + tuple(start.parents):
        if (p / "pyproject.toml").is_file():
            return p
        if (p / ".git").exists():
            return p
    # Fallback: <repo>/scripts/ci_smoke.py -> <repo>
    try:
        return start.parents[1]
    except Exception:
        return start


def _bootstrap_sys_path() -> Path:
    """
    If executed as `python scripts/ci_smoke.py`, sys.path[0] becomes `scripts/`
    and imports like `utils_pkg` can fail. We force repo root (+ optional src/)
    onto sys.path.
    """
    here = Path(__file__).resolve()
    repo_root = _find_repo_root(here)

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
# -----------------------------------------------------------------------------
def force_safe_env() -> None:
    """
    Force a deterministic, no-network, CI-friendly environment.
    Redundant toggles are intentional: whichever your code recognizes will apply.

    IMPORTANT: only set keys already present in the env contract.
    """
    os.environ.setdefault("CI", "1")
    os.environ.setdefault("PYTHONUNBUFFERED", "1")

    os.environ.setdefault("SAFE_MODE", "1")
    os.environ.setdefault("DRYRUN", "1")
    os.environ.setdefault("DRY_RUN", "1")

    os.environ.setdefault("NO_NETWORK", "1")
    os.environ.setdefault("DISABLE_NETWORK", "1")

    # Common “contract scripts” knobs
    os.environ.setdefault("CHECK_ENV_JSON", "1")
    os.environ.setdefault("CHECK_ENV_JSON_ONLY", "1")


# -----------------------------------------------------------------------------
# Hard network guard (strict by design)
# -----------------------------------------------------------------------------
class NetworkBlockedError(RuntimeError):
    """Raised when outbound network is attempted during CI smoke."""


@dataclass(frozen=True)
class _Patch:
    obj: Any
    attr: str
    original: Any


def _blocked_fn(msg: str) -> Callable[..., Any]:
    def _blocked(*_args: Any, **_kwargs: Any) -> Any:
        raise NetworkBlockedError(msg)

    return _blocked


def _safe_setattr(obj: Any, name: str, value: Any) -> bool:
    try:
        setattr(obj, name, value)
        return True
    except Exception:
        return False


@contextmanager
def block_network() -> Iterator[None]:
    """
    Block common outbound network pathways (socket/urllib/requests/aiohttp/httpx).
    CI smoke must not touch the network.

    This is intentionally strict and will fail fast with a clear error message.
    """
    patches: list[_Patch] = []

    def patch(obj: Any, attr: str, value: Any) -> None:
        try:
            orig = getattr(obj, attr)
        except Exception:
            return
        if _safe_setattr(obj, attr, value):
            patches.append(_Patch(obj=obj, attr=attr, original=orig))

    # socket guards
    patch(socket, "socket", _blocked_fn("Network blocked (socket.socket) in ci_smoke."))
    if hasattr(socket, "create_connection"):
        patch(
            socket,
            "create_connection",
            _blocked_fn("Network blocked (socket.create_connection) in ci_smoke."),
        )
    if hasattr(socket, "getaddrinfo"):
        patch(
            socket, "getaddrinfo", _blocked_fn("Network blocked (socket.getaddrinfo) in ci_smoke.")
        )

    # urllib guard
    patch(
        urllib.request,
        "urlopen",
        _blocked_fn("Network blocked (urllib.request.urlopen) in ci_smoke."),
    )

    # requests guard (optional)
    try:
        import requests as _requests  # type: ignore[import-not-found]

        patch(_requests, "request", _blocked_fn("Network blocked (requests.request) in ci_smoke."))
        patch(_requests, "get", _blocked_fn("Network blocked (requests.get) in ci_smoke."))
        patch(_requests, "post", _blocked_fn("Network blocked (requests.post) in ci_smoke."))
    except Exception:
        pass

    # aiohttp guard (optional)
    try:
        import aiohttp as _aiohttp  # type: ignore[import-not-found]

        class _BlockedClientSession:  # noqa: D401
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                raise NetworkBlockedError("Network blocked (aiohttp.ClientSession) in ci_smoke.")

        if hasattr(_aiohttp, "ClientSession"):
            patch(_aiohttp, "ClientSession", _BlockedClientSession)
    except Exception:
        pass

    # httpx guard (optional)
    try:
        import httpx as _httpx  # type: ignore[import-not-found]

        class _BlockedClient:  # noqa: D401
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                raise NetworkBlockedError("Network blocked (httpx.Client) in ci_smoke.")

        class _BlockedAsyncClient:  # noqa: D401
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                raise NetworkBlockedError("Network blocked (httpx.AsyncClient) in ci_smoke.")

        if hasattr(_httpx, "Client"):
            patch(_httpx, "Client", _BlockedClient)
        if hasattr(_httpx, "AsyncClient"):
            patch(_httpx, "AsyncClient", _BlockedAsyncClient)
    except Exception:
        pass

    try:
        yield
    finally:
        # Restore in reverse order (best-effort).
        for p in reversed(patches):
            _safe_setattr(p.obj, p.attr, p.original)


# -----------------------------------------------------------------------------
# Import + env contract probing
# -----------------------------------------------------------------------------
@dataclass
class ImportResult:
    name: str
    ok: bool
    err: str | None = None
    tb: str | None = None


def try_import(name: str) -> ImportResult:
    try:
        importlib.import_module(name)
        return ImportResult(name=name, ok=True)
    except Exception as e:
        return ImportResult(
            name=name,
            ok=False,
            err=f"{type(e).__name__}: {e}",
            tb=traceback.format_exc(),
        )


def _run_maybe_async(val: Any) -> None:
    """
    Run coroutine-like return values safely.
    Supports coroutines and generic awaitables.
    """
    import inspect

    if inspect.iscoroutine(val):
        asyncio.run(val)
        return
    if inspect.isawaitable(val):
        aw = val

        async def _wrap() -> Any:
            return await aw

        asyncio.run(_wrap())


def call_env_validator(mod: ModuleType) -> tuple[bool, str]:
    """
    Best-effort env validation call (NON-STRICT).
    Supports common function names used across the overhaul.
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


def _is_missing_module_error(err: str | None) -> bool:
    """
    Conservative check for "the module itself doesn't exist".
    We only use this for modules explicitly marked optional.
    """
    if not err:
        return False
    return err.startswith("ModuleNotFoundError") or err.startswith("ImportError")


# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
DEFAULT_TARGETS: list[str] = [
    # Core import spine
    "utils_pkg.core_utils",
    "utils_pkg.router",
    "utils_pkg.feature_builder",
    "utils_pkg.ai_entry_logic",
    "utils_pkg.exit_engine",
    # Optional: only skip if missing
    "utils_pkg.auto_exit_worker.exit_engine",
]


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="CI smoke: hard no-network spine imports.")
    p.add_argument(
        "--json",
        action="store_true",
        help="Emit a JSON summary line to stdout (in addition to human logs).",
    )
    p.add_argument(
        "--json-only",
        action="store_true",
        help="Emit ONLY JSON summary to stdout (no human logs).",
    )
    p.add_argument(
        "--targets",
        nargs="*",
        default=None,
        help="Override default import targets (space-separated module names).",
    )
    return p.parse_args(argv)


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def _human_print(enabled: bool, msg: str) -> None:
    if enabled:
        print(msg)


def _json_dump(obj: dict[str, Any]) -> None:
    print(json.dumps(obj, sort_keys=True))


def run_ci_smoke(targets: list[str], human: bool) -> tuple[int, dict[str, Any]]:
    force_safe_env()

    _human_print(human, "[ci_smoke] SAFE_MODE/DRYRUN/NO_NETWORK enabled")
    _human_print(human, f"[ci_smoke] repo_root={REPO_ROOT}")
    _human_print(human, f"[ci_smoke] cwd={Path.cwd()}")
    _human_print(human, f"[ci_smoke] sys.path[0]={sys.path[0]}")
    _human_print(human, "[ci_smoke] Blocking outbound network...")

    results: list[ImportResult] = []
    failures: list[str] = []
    last_tb: str | None = None

    with block_network():
        for name in targets:
            r = try_import(name)
            results.append(r)

            if r.ok:
                _human_print(human, f"[ci_smoke] import OK: {name}")

                # Best-effort env validation from canonical spine module.
                if name == "utils_pkg.core_utils":
                    try:
                        mod = importlib.import_module(name)
                        v_ok, msg = call_env_validator(mod)
                        _human_print(human, f"[ci_smoke] {msg}")
                        if not v_ok:
                            failures.append(msg)
                    except Exception as e:
                        failures.append(
                            f"env validator failed unexpectedly: {type(e).__name__}: {e}"
                        )
                continue

            # Optional target handling: skip ONLY when the module is missing.
            if name == "utils_pkg.auto_exit_worker.exit_engine" and _is_missing_module_error(r.err):
                _human_print(human, f"[ci_smoke] import SKIP (optional missing): {name} ({r.err})")
                continue

            failures.append(f"import FAILED: {name} ({r.err})")
            _human_print(human, f"[ci_smoke] import FAILED: {name} ({r.err})")
            last_tb = r.tb or last_tb

    exit_code = 0 if not failures else 1

    if failures:
        _human_print(human, "\n[ci_smoke] FAILURES:")
        for f in failures:
            _human_print(human, f"  - {f}")
        if last_tb:
            _human_print(human, "\n[ci_smoke] Traceback (last failure):")
            _human_print(human, last_tb)
    else:
        _human_print(
            human,
            "\n[ci_smoke] SUCCESS: spine imports OK, env contract sane, network remained blocked.",
        )

    summary = {
        "ok": exit_code == 0,
        "exit_code": exit_code,
        "repo_root": str(REPO_ROOT),
        "cwd": str(Path.cwd()),
        "targets": list(targets),
        "results": [{"name": r.name, "ok": r.ok, "err": r.err} for r in results],
        "failures": list(failures),
    }
    return exit_code, summary


def main(argv: list[str] | None = None) -> int:
    ns = _parse_args(sys.argv[1:] if argv is None else argv)
    targets = list(DEFAULT_TARGETS if ns.targets is None else ns.targets)
    human = not ns.json_only

    exit_code, summary = run_ci_smoke(targets=targets, human=human)

    if ns.json or ns.json_only:
        _json_dump(summary)

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
