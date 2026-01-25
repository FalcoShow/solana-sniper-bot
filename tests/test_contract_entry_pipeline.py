# tests/test_contract_entry_pipeline.py
from __future__ import annotations

"""
tests/test_contract_entry_pipeline.py — Entry Pipeline LOCK-IN (P0)
===================================================================

What this file protects
-----------------------
This contract suite locks the *entry* path (feature builder + entry IQ logic):

  Candidate snapshot -> FeatureBuilder -> score_entry -> (optional) should_enter/gate

Hard requirements (LOCK-IN)
---------------------------
✅ Import-safe: no secrets required, no dotenv side effects, no network at import-time
✅ Deterministic in SAFE_MODE: same inputs -> same features/score/gate output
✅ Feature dict is loggable: JSON-serializable and scalar values only
✅ Score is a float in [0,1]
✅ If gate exists: returns (ok, score?, reason) with a meaningful reason string
✅ Never HANGS: contract harness must fail fast with actionable diagnostics

Notes
-----
- Network blocking is already enforced globally via tests/conftest.py.
  This file adds an extra scoped guard to prevent accidental localhost service connects.
- We intentionally tolerate refactors by *discovering* callables by common names.
"""

import asyncio
import contextlib
import importlib
import inspect
import json
import os
import random
import socket
import sys
import traceback
from collections.abc import Callable, Mapping
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.contract

# ---------------------------------------------------------------------
# Tunables (fast failure, not pytest-timeout global)
# ---------------------------------------------------------------------
_CALL_TIMEOUT_S = float(os.getenv("CONTRACT_CALL_TIMEOUT_S", "3.0"))
_TOTAL_TEST_TIMEOUT_S = float(os.getenv("CONTRACT_TOTAL_TIMEOUT_S", "12.0"))

# ---------------------------------------------------------------------
# Stable constants (no network dependency)
# ---------------------------------------------------------------------
SOL_MINT = "So11111111111111111111111111111111111111112"  # Wrapped SOL
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"  # USDC


# ---------------------------------------------------------------------
# Environment + network guards
# ---------------------------------------------------------------------
class NetworkBlockedError(RuntimeError):
    """Raised when contract tests detect network I/O."""


def _seed_everything(seed: int = 1337) -> None:
    random.seed(seed)
    # Optional seeds (if present). Never require these deps in contract tests.
    try:
        import numpy as np  # type: ignore

        np.random.seed(seed)
    except Exception:
        pass


def _set_safe_mode_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Force entry pipeline into SAFE/DRYRUN/CONTRACT mode.

    We set a *superset* of common flags so whichever your code recognizes will work.
    (This is intentionally redundant and harmless.)
    """
    # Primary safety toggles
    monkeypatch.setenv("DRYRUN", "1")
    monkeypatch.setenv("DRY_RUN", "1")
    monkeypatch.setenv("SAFE_MODE", "1")
    monkeypatch.setenv("NO_NETWORK", "1")
    monkeypatch.setenv("DISABLE_NETWORK", "1")
    monkeypatch.setenv("PYTEST_MODE", "1")
    monkeypatch.setenv("CI", "1")
    monkeypatch.setenv("BACKTEST_MODE", "1")

    # Contract harness toggles (match conftest-style output)
    monkeypatch.setenv("SNIPER_TEST_CONTRACT", "1")
    monkeypatch.setenv("CONTRACT_TEST", "1")
    monkeypatch.setenv("NETWORK", "OFF")
    monkeypatch.setenv("INTEGRATION", "OFF")
    monkeypatch.setenv("SNIPER_TEST_NETWORK", "OFF")
    monkeypatch.setenv("SNIPER_TEST_INTEGRATION", "OFF")
    monkeypatch.setenv("SNIPER_TEST_SLOW", "OFF")
    monkeypatch.setenv("ALLOW_LIVE_ENV", "OFF")
    monkeypatch.setenv("ALLOW_LIVE_ENV_VARS", "0")

    # Extra safety: ensure trading side effects are off if your app checks these.
    monkeypatch.setenv("TRADING_ENABLED", "0")
    monkeypatch.setenv("LIVE_TRADING", "0")
    monkeypatch.setenv("ALLOW_BUYS", "0")
    monkeypatch.setenv("ALLOW_SELLS", "0")
    monkeypatch.setenv("DISABLE_LIVE_TRADING", "1")

    # Disable optional subsystems if code supports them (no-op if unused)
    monkeypatch.setenv("TELEGRAM_ENABLED", "0")
    monkeypatch.setenv("DISCORD_ENABLED", "0")
    monkeypatch.setenv("NOTION_ENABLED", "0")
    monkeypatch.setenv("DATA_LOGGER_ENABLED", "0")
    monkeypatch.setenv("SCORING_SERVICE_ENABLED", "0")
    monkeypatch.setenv("PRICE_FEED_ENABLED", "0")
    monkeypatch.setenv("REDIS_ENABLED", "0")
    monkeypatch.setenv("POSTGRES_ENABLED", "0")
    monkeypatch.setenv("OPENAI_ENABLED", "0")

    # Prevent dotenv/autoload side effects in imports (if supported)
    monkeypatch.setenv("CORE_LOAD_DOTENV", "0")
    monkeypatch.setenv("LOAD_DOTENV", "0")


def _is_win() -> bool:
    return (os.name == "nt") or sys.platform.startswith("win")


def _ephemeral_low_port() -> int:
    """
    Lower bound for ephemeral ports.
    - Windows default ephemeral starts at 49152.
    - Linux typically >= 32768 (we attempt to read kernel config).
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


def _norm_host(host: Any) -> str:
    if host is None:
        return ""
    if isinstance(host, bytes):
        try:
            host = host.decode("utf-8", "ignore")
        except Exception:
            host = str(host)
    return str(host).strip().strip("[]").lower()


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
    h = _norm_host(host)
    if h not in _LOOPBACK_HOSTS:
        return False
    if port == 0:
        return True
    return port >= _ephemeral_low_port()


def _raise_blocked(where: str, host: Any, port: Any, why: str = "") -> None:
    h = _norm_host(host) or "<unknown>"
    p = port if port is not None else "<unknown>"
    msg = (
        "Network is blocked in entry-pipeline contract tests.\n"
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

        # Patch common HTTP clients if they exist inside module namespaces
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


# ---------------------------------------------------------------------
# Fail-fast await wrapper (prevents “hang until pytest-timeout”)
# ---------------------------------------------------------------------
def _format_task(task: asyncio.Task[Any]) -> str:
    name = getattr(task, "get_name", lambda: "<task>")()
    state = "done" if task.done() else "pending"
    lines = [f"- {name} [{state}] coro={getattr(task, 'get_coro', lambda: None)()}"]
    try:
        frames = task.get_stack(limit=8)
        if frames:
            lines.append("  stack:")
            for f in frames[-8:]:
                lines.append("    " + "".join(traceback.format_stack(f, limit=1)).rstrip())
    except Exception:
        pass
    return "\n".join(lines)


async def _await_maybe(obj: Any, *, timeout_s: float, label: str) -> Any:
    if not inspect.isawaitable(obj):
        return obj

    try:
        return await asyncio.wait_for(obj, timeout=timeout_s)
    except TimeoutError as e:
        # Snapshot current tasks for actionable debugging
        try:
            tasks = [t for t in asyncio.all_tasks() if not t.done()]
        except Exception:
            tasks = []
        diag = "\n".join(_format_task(t) for t in tasks[:20]) or "<no pending tasks>"
        raise AssertionError(
            f"Contract call timed out ({timeout_s:.2f}s) during: {label}\n"
            f"This indicates a WAIT/QUEUE/READY latch is blocking in SAFE/CONTRACT mode.\n\n"
            f"Pending tasks (up to 20):\n{diag}\n"
        ) from e


# ---------------------------------------------------------------------
# “Common hang neutralizer” (only within contract harness)
# ---------------------------------------------------------------------
class _NoWaitLimiter:
    async def wait(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover
        return None


def _neutralize_common_blockers(monkeypatch: pytest.MonkeyPatch, *mods: Any) -> None:
    """
    In contract mode, entry scoring must not block on runtime workers.
    Some subsystems use limiters/gates/conditions. If they leak into scoring,
    they can hang (e.g., limiter configured to 0 QPS).

    This neutralizes the most common *module-level* blockers without touching
    production code paths outside the test scope.
    """
    for mod in mods:
        if mod is None:
            continue
        # Replace module-level limiters if present
        for nm in ("limiter", "_limiter", "rate_limiter", "_rate_limiter"):
            if hasattr(mod, nm):
                v = getattr(mod, nm)
                if v is not None and hasattr(v, "wait"):
                    monkeypatch.setattr(mod, nm, _NoWaitLimiter(), raising=False)

        # Set module-level gates if present
        for _k, v in list(getattr(mod, "__dict__", {}).items()):
            try:
                if isinstance(v, asyncio.Event):
                    # Release anything waiting on this gate
                    v.set()
            except Exception:
                pass


# ---------------------------------------------------------------------
# Repo helpers + fixture loading
# ---------------------------------------------------------------------
def _repo_root() -> Path:
    here = Path(__file__).resolve()
    for p in (here.parent, *here.parents):
        if (p / ".env.example").exists() and (p / "tests").exists():
            return p
    for p in (here.parent, *here.parents):
        if p.name == "tests":
            return p.parent
    return here.parent.parent


def _load_candidate_fixture() -> dict[str, Any] | None:
    root = _repo_root()
    path = root / "tests" / "fixtures" / "candidate_sample.json"
    if not path.exists():
        return None
    try:
        d = json.loads(path.read_text(encoding="utf-8", errors="replace"))
        return d if isinstance(d, dict) else None
    except Exception:
        return None


def _default_candidate() -> dict[str, Any]:
    """
    Minimal-but-realistic candidate snapshot.
    Keep stable; no network. Includes common key aliases.
    """
    return {
        "mint": USDC_MINT,
        "symbol": "TEST",
        "sym": "TEST",
        "source": "contract_test",
        "dex": "raydium",
        "ts": 1700000000,
        "liq": 150_000.0,
        "liq_usd": 150_000.0,
        "vol5": 4_000.0,
        "buys5": 18,
        "sells5": 12,
        "chg5": 3.0,
        "vol15": 8_000.0,
        "buys15": 40,
        "sells15": 33,
        "chg15": 4.5,
        "vol60": 20_000.0,
        "buys60": 120,
        "sells60": 105,
        "chg60": 6.0,
        "age_min": 7.5,
        "age_s": 450.0,
        "price": 1.00,
        "price_usd": 1.00,
        "base_mint": SOL_MINT,
        "quote_mint": USDC_MINT,
    }


# ---------------------------------------------------------------------
# Import + discovery helpers (stable across refactors)
# ---------------------------------------------------------------------
def _fresh_import(module_name: str) -> Any:
    for name in list(sys.modules.keys()):
        if name == module_name or name.startswith(module_name + "."):
            sys.modules.pop(name, None)
    return importlib.import_module(module_name)


def _import(name: str, *, fresh: bool = True) -> Any:
    return _fresh_import(name) if fresh else importlib.import_module(name)


def _find_callable(mod: Any, names: tuple[str, ...]) -> Callable[..., Any] | None:
    for n in names:
        fn = getattr(mod, n, None)
        if callable(fn):
            return fn
    return None


def _discover_feature_builder(feature_builder_mod: Any) -> Callable[..., Any]:
    fn = _find_callable(
        feature_builder_mod,
        (
            "build_features",
            "build_feature_dict",
            "build_feature_map",
            "make_features",
            "features_from_candidate",
            "build",
        ),
    )
    if callable(fn):
        return fn

    cls = getattr(feature_builder_mod, "FeatureBuilder", None)
    if cls is not None and callable(cls):
        try:
            inst = cls()
        except Exception as e:
            pytest.skip(f"FeatureBuilder exists but could not be instantiated without args: {e}")

        if callable(inst):
            return inst
        build = getattr(inst, "build", None)
        if callable(build):
            return build

    pytest.skip(
        "No discoverable feature builder entrypoint in utils_pkg.feature_builder. "
        "Expected build_features/build_feature_dict/make_features/build or FeatureBuilder.build()."
    )


def _discover_entry_scorer(ai_entry_logic_mod: Any) -> Callable[..., Any]:
    fn = _find_callable(
        ai_entry_logic_mod,
        (
            "score_entry",
            "compute_entry_score",
            "entry_score",
            "score",
            "score_only",
        ),
    )
    if fn is None:
        pytest.skip(
            "No discoverable scoring callable in utils_pkg.ai_entry_logic. "
            "Expected score_entry/compute_entry_score/entry_score/score/score_only."
        )
    return fn


def _discover_entry_gate(ai_entry_logic_mod: Any) -> Callable[..., Any] | None:
    return _find_callable(
        ai_entry_logic_mod,
        (
            "should_enter",
            "gate_entry",
            "decide_entry",
            "should_buy",
            "gate",
            "pass_gate",
        ),
    )


# ---------------------------------------------------------------------
# Normalizers (tolerant to refactors)
# ---------------------------------------------------------------------
def _normalize_score(out: Any) -> float:
    if isinstance(out, int | float):
        return float(out)

    if isinstance(out, tuple):
        # common: (score, why) or (ok, score, why)
        if len(out) >= 1 and isinstance(out[0], int | float):
            return float(out[0])
        if len(out) >= 2 and isinstance(out[1], int | float):
            return float(out[1])

    if isinstance(out, dict):
        for k in ("score", "entry_score", "s", "value"):
            v = out.get(k)
            if isinstance(v, int | float):
                return float(v)

    if hasattr(out, "score"):
        v = out.score
        if isinstance(v, int | float):
            return float(v)

    raise AssertionError(f"Could not normalize score from {type(out).__name__}: {out!r}")


def _normalize_gate(out: Any) -> tuple[bool, float | None, str]:
    if isinstance(out, bool):
        return out, None, ("OK" if out else "REJECT")

    if isinstance(out, tuple):
        if len(out) == 2 and isinstance(out[0], bool):
            ok = out[0]
            reason = out[1] if isinstance(out[1], str) else str(out[1])
            return ok, None, reason
        if len(out) >= 3 and isinstance(out[0], bool):
            ok = out[0]
            score = out[1] if isinstance(out[1], int | float) else None
            reason = out[2] if isinstance(out[2], str) else str(out[2])
            return ok, (float(score) if score is not None else None), reason

    if isinstance(out, dict):
        ok: Any = out.get("ok")
        if not isinstance(ok, bool):
            ok = out.get("pass")
        if not isinstance(ok, bool):
            ok = out.get("should_enter")
        if not isinstance(ok, bool):
            ok = out.get("allowed")
        if not isinstance(ok, bool):
            raise AssertionError(f"Gate dict missing ok/pass/should_enter/allowed: {out!r}")

        score = None
        for k in ("score", "entry_score", "s", "value"):
            v = out.get(k)
            if isinstance(v, int | float):
                score = float(v)
                break

        reason = None
        for k in ("reason", "why", "msg", "message", "detail"):
            v = out.get(k)
            if isinstance(v, str) and v.strip():
                reason = v.strip()
                break
        if reason is None:
            reason = "OK" if ok else "REJECT"

        return bool(ok), score, reason

    if is_dataclass(out):
        return _normalize_gate(asdict(out))

    dump = getattr(out, "model_dump", None)
    if callable(dump):
        try:
            d = dump()
            if isinstance(d, dict):
                return _normalize_gate(d)
        except Exception:
            pass

    ok = getattr(out, "ok", None)
    if not isinstance(ok, bool):
        ok = getattr(out, "pass_", None)
    if not isinstance(ok, bool):
        ok = getattr(out, "should_enter", None)
    if not isinstance(ok, bool):
        ok = getattr(out, "allowed", None)

    if isinstance(ok, bool):
        score = getattr(out, "score", None)
        score_f = float(score) if isinstance(score, int | float) else None
        reason = (
            getattr(out, "reason", None)
            or getattr(out, "why", None)
            or getattr(out, "message", None)
        )
        reason_s = str(reason).strip() if reason is not None else ("OK" if ok else "REJECT")
        return ok, score_f, reason_s

    raise AssertionError(f"Could not normalize gate output {type(out).__name__}: {out!r}")


# ---------------------------------------------------------------------
# Call helpers
# ---------------------------------------------------------------------
def _kw_pool(candidate: dict[str, Any], features: dict[str, Any] | None = None) -> dict[str, Any]:
    feats = features or {}
    return {
        "candidate": candidate,
        "cand": candidate,
        "snapshot": candidate,
        "raw": candidate,
        "meta": candidate,
        "features": feats,
        "feat": feats,
        "x": feats,
        "ctx": {},
        "context": {},
        "strict": False,
        "safe_mode": True,
        "dryrun": True,
        "no_network": True,
        "timeout_s": _CALL_TIMEOUT_S,
        "timeout": _CALL_TIMEOUT_S,
        "seed": 1337,
    }


def _call_sig_filtered(fn: Callable[..., Any], pool: dict[str, Any]) -> Any:
    sig = inspect.signature(fn)
    kwargs = {k: v for k, v in pool.items() if k in sig.parameters}

    # If function accepts **kwargs and we matched nothing, pass canonical names.
    if not kwargs and any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()):
        kwargs = {
            k: pool[k]
            for k in ("candidate", "meta", "features", "strict", "safe_mode", "dryrun")
            if k in pool
        }

    # Fill remaining required params conservatively.
    for p in sig.parameters.values():
        if p.default is not inspect._empty:
            continue
        if p.name in kwargs:
            continue

        pn = p.name.lower()

        # Common “timeout” params: force short to avoid hangs
        if "timeout" in pn or "deadline" in pn or "max_wait" in pn:
            kwargs[p.name] = _CALL_TIMEOUT_S
            continue

        # Feature / candidate / ctx
        if "feature" in pn:
            kwargs[p.name] = pool.get("features", {})
        elif "cand" in pn or "snapshot" in pn or "meta" in pn:
            kwargs[p.name] = pool.get("candidate", {})
        elif "context" in pn or pn == "ctx":
            kwargs[p.name] = {}
        elif "strict" in pn:
            kwargs[p.name] = False
        elif "safe" in pn or "dry" in pn or "no_network" in pn:
            kwargs[p.name] = True

        # Common optional integrations: keep None so code can short-circuit
        elif (
            "redis" in pn
            or "db" in pn
            or "conn" in pn
            or "client" in pn
            or "session" in pn
            or "logger" in pn
        ):
            kwargs[p.name] = None
        elif "seed" in pn:
            kwargs[p.name] = 1337
        else:
            kwargs[p.name] = None

    return fn(**kwargs)


def _assert_jsonable(d: Mapping[str, Any]) -> None:
    try:
        json.dumps(d, ensure_ascii=False, sort_keys=True)
    except Exception as e:
        raise AssertionError(
            f"Expected dict to be JSON-serializable, but json.dumps failed: {e}"
        ) from e


def _scalar_only_features(d: Mapping[str, Any]) -> None:
    allowed = (int, float, bool, type(None), str)
    for k, v in d.items():
        assert isinstance(k, str) and k.strip(), "Feature keys must be non-empty strings"
        assert isinstance(v, allowed), (
            f"Feature value for {k!r} must be scalar/loggable, got {type(v).__name__}"
        )


def _has_any_key(keys_lower: set[str], *names: str) -> bool:
    return any(n.lower() in keys_lower for n in names)


# ---------------------------------------------------------------------
# Autouse fixture: always safe env + deterministic seed
# ---------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _contract_env(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_safe_mode_env(monkeypatch)
    _seed_everything(1337)


# ---------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------
def test_entry_modules_import_are_side_effect_free(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Contract: feature_builder + ai_entry_logic must be importable in SAFE_MODE
    with no secrets and no real network sockets.
    """
    with _no_network_local_services(monkeypatch):
        fb = _import("utils_pkg.feature_builder", fresh=True)
        el = _import("utils_pkg.ai_entry_logic", fresh=True)

    assert fb is not None, "Failed to import utils_pkg.feature_builder"
    assert el is not None, "Failed to import utils_pkg.ai_entry_logic"


@pytest.mark.asyncio
async def test_feature_builder_produces_deterministic_jsonable_feature_dict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fb_mod = _import("utils_pkg.feature_builder", fresh=True)
    _neutralize_common_blockers(monkeypatch, fb_mod)

    builder = _discover_feature_builder(fb_mod)

    cand = _load_candidate_fixture() or _default_candidate()
    pool = _kw_pool(cand)

    async def _run() -> dict[str, Any]:
        with _no_network_local_services(monkeypatch, fb_mod):
            out1 = _call_sig_filtered(builder, pool)
            out1 = await _await_maybe(
                out1, timeout_s=_CALL_TIMEOUT_S, label="feature_builder(out1)"
            )

            out2 = _call_sig_filtered(builder, pool)
            out2 = await _await_maybe(
                out2, timeout_s=_CALL_TIMEOUT_S, label="feature_builder(out2)"
            )

        assert isinstance(out1, dict), (
            f"Feature builder must return dict, got {type(out1).__name__}"
        )
        assert out1 == out2, "Feature builder must be deterministic for same inputs"

        assert len(out1) >= 8, f"Feature dict too small ({len(out1)} keys); expected a real schema"
        _scalar_only_features(out1)
        _assert_jsonable(out1)

        keys = {k.lower() for k in out1}
        assert _has_any_key(keys, "liq", "liquidity", "liq_usd", "liquidity_usd"), (
            "Missing liquidity-ish feature"
        )
        assert _has_any_key(keys, "vol5", "volume5", "vol_5m", "vol_5", "vol5_usd"), (
            "Missing vol5-ish feature"
        )
        assert _has_any_key(keys, "buys5", "buy5", "buys_5m", "buys_5"), "Missing buys5-ish feature"
        assert _has_any_key(keys, "sells5", "sell5", "sells_5m", "sells_5"), (
            "Missing sells5-ish feature"
        )

        return out1

    await asyncio.wait_for(_run(), timeout=_TOTAL_TEST_TIMEOUT_S)


@pytest.mark.asyncio
async def test_entry_scoring_returns_float_in_0_1_and_is_deterministic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fb_mod = _import("utils_pkg.feature_builder", fresh=True)
    el_mod = _import("utils_pkg.ai_entry_logic", fresh=True)

    # Neutralize typical module-level blockers if they leak into SAFE_MODE paths
    _neutralize_common_blockers(monkeypatch, fb_mod, el_mod)

    builder = _discover_feature_builder(fb_mod)
    scorer = _discover_entry_scorer(el_mod)

    cand = _load_candidate_fixture() or _default_candidate()

    async def _run() -> None:
        pool_fb = _kw_pool(cand)
        with _no_network_local_services(monkeypatch, fb_mod, el_mod):
            feats = _call_sig_filtered(builder, pool_fb)
            feats = await _await_maybe(
                feats, timeout_s=_CALL_TIMEOUT_S, label="feature_builder(feats)"
            )

        assert isinstance(feats, dict) and feats, "Feature builder must produce a non-empty dict"
        _scalar_only_features(feats)
        _assert_jsonable(feats)

        pool_sc = _kw_pool(cand, feats)
        with _no_network_local_services(monkeypatch, el_mod):
            out_a = _call_sig_filtered(scorer, pool_sc)
            out_a = await _await_maybe(
                out_a, timeout_s=_CALL_TIMEOUT_S, label="entry_scorer(out_a)"
            )
            score_a = _normalize_score(out_a)

            out_b = _call_sig_filtered(scorer, pool_sc)
            out_b = await _await_maybe(
                out_b, timeout_s=_CALL_TIMEOUT_S, label="entry_scorer(out_b)"
            )
            score_b = _normalize_score(out_b)

        assert isinstance(score_a, float)
        assert 0.0 <= score_a <= 1.0, f"Entry score out of range [0,1]: {score_a}"
        assert score_a == score_b, (
            "Entry scoring must be deterministic for same features in SAFE_MODE"
        )

    await asyncio.wait_for(_run(), timeout=_TOTAL_TEST_TIMEOUT_S)


@pytest.mark.asyncio
async def test_entry_gating_reason_is_non_empty_if_gate_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fb_mod = _import("utils_pkg.feature_builder", fresh=True)
    el_mod = _import("utils_pkg.ai_entry_logic", fresh=True)

    _neutralize_common_blockers(monkeypatch, fb_mod, el_mod)

    builder = _discover_feature_builder(fb_mod)
    gate = _discover_entry_gate(el_mod)
    if gate is None:
        pytest.skip("No discoverable entry gate (should_enter/gate_entry/decide_entry/...).")

    cand = _load_candidate_fixture() or _default_candidate()

    async def _run() -> None:
        pool_fb = _kw_pool(cand)
        with _no_network_local_services(monkeypatch, fb_mod, el_mod):
            feats = _call_sig_filtered(builder, pool_fb)
            feats = await _await_maybe(
                feats, timeout_s=_CALL_TIMEOUT_S, label="feature_builder(feats_for_gate)"
            )

        assert isinstance(feats, dict) and feats

        pool_gate = _kw_pool(cand, feats)
        with _no_network_local_services(monkeypatch, el_mod):
            out = _call_sig_filtered(gate, pool_gate)
            out = await _await_maybe(out, timeout_s=_CALL_TIMEOUT_S, label="entry_gate(out)")

        ok, score, reason = _normalize_gate(out)

        assert isinstance(ok, bool)
        assert isinstance(reason, str) and len(reason.strip()) >= 3, (
            f"Gating reason must be non-empty. Got: {reason!r}"
        )
        if score is not None:
            assert 0.0 <= score <= 1.0, f"Gating score out of range [0,1]: {score}"

    await asyncio.wait_for(_run(), timeout=_TOTAL_TEST_TIMEOUT_S)
