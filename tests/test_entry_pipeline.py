# tests/test_entry_pipeline.py
from __future__ import annotations

"""
Entry pipeline contract tests (P0 / MUST)
========================================

Purpose
-------
Lock in the *canonical* entry pipeline wiring (no drift / no silent forks):

    normalized candidate snapshot
        -> utils_pkg.feature_builder.build_features(...)
        -> utils_pkg.ai_entry_logic.score_entry(...)
        -> utils_pkg.ai_entry_logic.should_enter(...)
        -> utils_pkg.ai_entry_logic.decide_entry(...)

LOCKED requirements
-------------------
utils_pkg.feature_builder:
  - build_features(snapshot: dict, ctx: dict | None = None) -> dict

utils_pkg.ai_entry_logic:
  - score_entry(features: dict, ctx: dict | None = None) -> float | tuple | dict
  - should_enter(features: dict, ctx: dict | None = None) -> bool | tuple | dict
  - decide_entry(features: dict, ctx: dict | None = None) -> bool | tuple | dict

What these tests enforce
------------------------
- Imports succeed in SAFE_MODE / DRYRUN with NO secrets and NO network I/O.
- Required symbols exist and are callable.
- build_features returns a JSON-serializable dict (PG/JSONB safe).
- score_entry resolves to a float in [0, 1].
- should_enter and decide_entry resolve to a boolean.
- Deterministic output for identical inputs in SAFE_MODE (time frozen).
- Sparse snapshots don't crash the pipeline (robustness).

Notes
-----
These tests are tolerant to return shapes (tuple/dict/float/bool), but strict about:
- existence of canonical functions
- bounded score
- boolean decision
- JSON-safe feature map
"""

import contextlib
import importlib
import inspect
import json
import math
import random
import socket
import sys
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import pytest

# ---------------------------------------------------------------------------
# Stable constants (no network dependency)
# ---------------------------------------------------------------------------
SOL_MINT = "So11111111111111111111111111111111111111112"  # Wrapped SOL
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"  # USDC


# ---------------------------------------------------------------------------
# Ensure repo root is on sys.path (so pytest works anywhere)
# ---------------------------------------------------------------------------
_HERE = Path(__file__).resolve().parent
for _p in (_HERE, *_HERE.parents):
    if (_p / "utils_pkg").is_dir():
        if str(_p) not in sys.path:
            sys.path.insert(0, str(_p))
        break


# ---------------------------------------------------------------------------
# Environment + network guards (Windows-friendly)
# ---------------------------------------------------------------------------
class _NetworkGuardError(RuntimeError):
    pass


@pytest.fixture(autouse=True)
def _safe_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Force offline, non-strict, deterministic mode.
    We set multiple toggles so whichever your code recognizes will work.
    """
    monkeypatch.setenv("CI", "1")
    monkeypatch.setenv("SAFE_MODE", "1")
    monkeypatch.setenv("DRYRUN", "1")
    monkeypatch.setenv("DRY_RUN", "1")

    monkeypatch.setenv("NO_NETWORK", "1")
    monkeypatch.setenv("DISABLE_NETWORK", "1")
    monkeypatch.setenv("DISABLE_RPC", "1")
    monkeypatch.setenv("DISABLE_HTTP", "1")

    monkeypatch.setenv("ENV_STRICT", "0")
    monkeypatch.setenv("STRICT_ENV", "0")
    monkeypatch.setenv("CORE_UTILS_ENV_STRICT", "0")
    monkeypatch.setenv("CORE_LOAD_DOTENV", "0")

    monkeypatch.setenv("TRADING_ENABLED", "0")
    monkeypatch.setenv("ALLOW_BUYS", "0")
    monkeypatch.setenv("ALLOW_SELLS", "0")

    monkeypatch.setenv("LOG_LEVEL", "WARNING")


def _is_loopback_ephemeral(addr: Any) -> bool:
    """
    Windows asyncio internals may create loopback connects to ephemeral ports.
    Allow ONLY:
      - host in {127.0.0.1, localhost, ::1}
      - port >= 49152
    """
    try:
        if not isinstance(addr, tuple) or len(addr) < 2:
            return False
        host, port = addr[0], addr[1]
        return host in ("127.0.0.1", "localhost", "::1") and isinstance(port, int) and port >= 49152
    except Exception:
        return False


@contextlib.contextmanager
def _no_network(monkeypatch: pytest.MonkeyPatch, *mods: Any):
    """
    Strictly block real network, but allow loopback ephemeral connects required by
    Windows asyncio internals. Patches are scoped to this context manager so
    pytest-asyncio teardown doesn't explode.
    """
    with monkeypatch.context() as m:
        orig_create_connection = getattr(socket, "create_connection", None)

        def _guard_create_connection(*args: Any, **kwargs: Any) -> Any:
            addr = args[0] if args else kwargs.get("address")
            if _is_loopback_ephemeral(addr) and callable(orig_create_connection):
                return orig_create_connection(*args, **kwargs)
            raise _NetworkGuardError(
                "Network blocked in entry pipeline tests (socket.create_connection)."
            )

        m.setattr(socket, "create_connection", _guard_create_connection, raising=False)

        orig_socket_cls = socket.socket

        class _GuardSocket(orig_socket_cls):  # type: ignore[misc]
            def connect(self, *args: Any, **kwargs: Any) -> Any:
                addr = args[0] if args else None
                if _is_loopback_ephemeral(addr):
                    return super().connect(*args, **kwargs)
                raise _NetworkGuardError(
                    "Network blocked in entry pipeline tests (socket.connect)."
                )

            def connect_ex(self, *args: Any, **kwargs: Any) -> Any:
                addr = args[0] if args else None
                if _is_loopback_ephemeral(addr):
                    return super().connect_ex(*args, **kwargs)
                raise _NetworkGuardError(
                    "Network blocked in entry pipeline tests (socket.connect_ex)."
                )

        m.setattr(socket, "socket", _GuardSocket, raising=False)

        # Patch common HTTP clients IF present in module namespaces
        for mod in mods:
            aiohttp = getattr(mod, "aiohttp", None)
            if aiohttp is not None:

                class _GuardAiohttpSession:
                    def __init__(self, *args: Any, **kwargs: Any) -> None:
                        raise _NetworkGuardError("Network blocked (aiohttp.ClientSession).")

                m.setattr(aiohttp, "ClientSession", _GuardAiohttpSession, raising=False)

            httpx = getattr(mod, "httpx", None)
            if httpx is not None:

                class _GuardHttpxAsyncClient:
                    def __init__(self, *args: Any, **kwargs: Any) -> None:
                        raise _NetworkGuardError("Network blocked (httpx.AsyncClient).")

                m.setattr(httpx, "AsyncClient", _GuardHttpxAsyncClient, raising=False)

            requests = getattr(mod, "requests", None)
            if requests is not None:

                def _guard_requests(*args: Any, **kwargs: Any) -> Any:
                    raise _NetworkGuardError("Network blocked (requests).")

                for nm in ("request", "get", "post"):
                    if hasattr(requests, nm):
                        m.setattr(requests, nm, _guard_requests, raising=False)

                if hasattr(requests, "Session"):

                    class _GuardSession:
                        def __init__(self, *args: Any, **kwargs: Any) -> None:
                            raise _NetworkGuardError("Network blocked (requests.Session).")

                    m.setattr(requests, "Session", _GuardSession, raising=False)

        yield


def _freeze_determinism(monkeypatch: pytest.MonkeyPatch, fixed_ts: int) -> None:
    """
    Best-effort determinism across runs: freeze time + random.
    """
    monkeypatch.setattr(time, "time", lambda: float(fixed_ts), raising=False)
    monkeypatch.setattr(time, "monotonic", lambda: float(fixed_ts), raising=False)
    monkeypatch.setattr(random, "random", lambda: 0.123456789, raising=False)
    monkeypatch.setattr(random, "randint", lambda a, b: a, raising=False)


# ---------------------------------------------------------------------------
# Repo helpers + fixture loading
# ---------------------------------------------------------------------------
def _repo_root() -> Path:
    here = Path(__file__).resolve()
    for p in [here.parent, *here.parents]:
        if (p / ".env.example").exists() and (p / "tests").exists():
            return p
    for p in [here.parent, *here.parents]:
        if p.name == "tests":
            return p.parent
    return here.parent.parent


def _load_candidate_fixture() -> dict[str, Any] | None:
    root = _repo_root()
    path = root / "tests" / "fixtures" / "candidate_sample.json"
    if not path.exists():
        return None
    try:
        d = json.loads(path.read_text(encoding="utf-8"))
        return d if isinstance(d, dict) else None
    except Exception:
        return None


def _minimal_candidate_snapshot() -> dict[str, Any]:
    """
    Small but meaningful normalized snapshot (stable, offline).
    """
    return {
        "sym": "TEST",
        "symbol": "TEST",
        "mint": SOL_MINT,
        "base_mint": SOL_MINT,
        "quote_mint": USDC_MINT,
        "liq": 25_000.0,
        "liq_usd": 25_000.0,
        "vol5": 3_000.0,
        "buys5": 6,
        "sells5": 2,
        "chg5": 2.1,
        "vol60": 12_500.0,
        "buys60": 40,
        "sells60": 28,
        "chg60": 6.5,
        "age_min": 12.0,
        "dex": "raydium",
        "ts": 1_700_000_000,
    }


def _sparse_candidate_snapshot() -> dict[str, Any]:
    return {
        "sym": "TEST",
        "mint": SOL_MINT,
        "liq": 10_000.0,
        "age_min": 6.0,
        "buys5": 1,
        "sells5": 0,
    }


def _ctx() -> dict[str, Any]:
    return {
        "now_ts": 1_700_000_000,
        "source": "tests",
        "mode": "unit",
        "safe_mode": True,
        "dryrun": True,
    }


# ---------------------------------------------------------------------------
# Import + signature helpers
# ---------------------------------------------------------------------------
def _import(name: str, *, reload: bool = True) -> Any:
    if reload and name in sys.modules:
        return importlib.reload(sys.modules[name])
    return importlib.import_module(name)


def _resolve_build_features(fb_mod: Any) -> Callable[..., Any]:
    assert hasattr(fb_mod, "build_features"), (
        "LOCK-IN: utils_pkg.feature_builder must export build_features(...)"
    )
    fn = fb_mod.build_features
    assert callable(fn), "feature_builder.build_features must be callable"
    return fn


def _resolve_entry_fns(
    entry_mod: Any,
) -> tuple[Callable[..., Any], Callable[..., Any], Callable[..., Any]]:
    for name in ("score_entry", "should_enter", "decide_entry"):
        assert hasattr(entry_mod, name), f"LOCK-IN: utils_pkg.ai_entry_logic missing {name}(...)"
        assert callable(getattr(entry_mod, name)), f"ai_entry_logic.{name} must be callable"
    return (entry_mod.score_entry, entry_mod.should_enter, entry_mod.decide_entry)  # type: ignore[attr-defined]


def _kw_pool(
    snapshot: dict[str, Any], ctx: dict[str, Any], features: dict[str, Any] | None = None
) -> dict[str, Any]:
    feats = features or {}
    return {
        # common snapshot names
        "snapshot": snapshot,
        "snap": snapshot,
        "candidate": snapshot,
        "cand": snapshot,
        "meta": snapshot,
        "raw": snapshot,
        # context names
        "ctx": ctx,
        "context": ctx,
        # features names
        "features": feats,
        "feat": feats,
        "x": feats,
        # common flags
        "strict": False,
        "safe_mode": True,
        "dryrun": True,
        "no_network": True,
        "now_ts": ctx.get("now_ts"),
    }


def _call_sig_filtered(fn: Callable[..., Any], pool: dict[str, Any]) -> Any:
    """
    Pass only kwargs the function declares; if it supports **kwargs, pass a compact set.
    This keeps tests stable across small refactors while preserving the locked function names.
    """
    sig = inspect.signature(fn)
    kwargs = {k: v for k, v in pool.items() if k in sig.parameters}

    if not kwargs and any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()):
        for k in (
            "snapshot",
            "candidate",
            "meta",
            "features",
            "ctx",
            "context",
            "strict",
            "safe_mode",
            "dryrun",
            "now_ts",
        ):
            if k in pool:
                kwargs[k] = pool[k]

    # Fill required params best-effort (so we don't depend on exact param names)
    for p in sig.parameters.values():
        if p.default is not inspect._empty:
            continue
        if p.name in kwargs:
            continue

        pn = p.name.lower()
        if "feature" in pn:
            kwargs[p.name] = pool.get("features", {})
        elif "ctx" in pn or "context" in pn:
            kwargs[p.name] = pool.get("ctx", {})
        elif "snap" in pn or "cand" in pn or "meta" in pn:
            kwargs[p.name] = pool.get("snapshot", {})
        elif "strict" in pn:
            kwargs[p.name] = False
        elif "safe" in pn or "dry" in pn:
            kwargs[p.name] = True
        else:
            kwargs[p.name] = None

    return fn(**kwargs)


async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


# ---------------------------------------------------------------------------
# Normalization helpers (tolerant to return shapes)
# ---------------------------------------------------------------------------
def _extract_float_score(out: Any) -> float:
    if isinstance(out, int | float):
        return float(out)

    if isinstance(out, dict):
        for k in ("score", "entry_score", "s", "value", "blended_score"):
            v = out.get(k)
            if isinstance(v, int | float):
                return float(v)

    if isinstance(out, tuple) and out:
        # common: (ok, score, reason) or (score, reason)
        for x in out:
            if isinstance(x, int | float):
                return float(x)

    if hasattr(out, "score") and isinstance(out.score, int | float):
        return float(out.score)

    raise AssertionError(f"Could not extract numeric score from {type(out).__name__}: {out!r}")


def _extract_bool_decision(out: Any) -> bool:
    if isinstance(out, bool):
        return bool(out)

    if isinstance(out, dict):
        for k in ("enter", "ok", "should_enter", "decision", "allow", "allowed", "pass"):
            v = out.get(k)
            if isinstance(v, bool):
                return bool(v)

    if isinstance(out, tuple) and out:
        if isinstance(out[0], bool):
            return bool(out[0])
        for x in out:
            if isinstance(x, bool):
                return bool(x)

    if hasattr(out, "ok") and isinstance(out.ok, bool):
        return bool(out.ok)

    raise AssertionError(f"Could not extract bool decision from {type(out).__name__}: {out!r}")


def _is_json_safe_value(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, bool):
        return True
    if isinstance(v, int):
        return True
    if isinstance(v, float):
        return math.isfinite(v)
    if isinstance(v, str):
        return True
    if isinstance(v, list):
        return all(_is_json_safe_value(x) for x in v)
    if isinstance(v, dict):
        return all(isinstance(k, str) and _is_json_safe_value(x) for k, x in v.items())
    return False


def _assert_json_safe_map(feats: Mapping[str, Any]) -> None:
    assert isinstance(feats, dict), f"features must be dict; got {type(feats).__name__}"
    for k, v in feats.items():
        assert isinstance(k, str) and k.strip(), f"feature keys must be non-empty str; got {k!r}"
        assert _is_json_safe_value(v), f"feature {k!r} not JSON-safe: {type(v).__name__}={v!r}"
    # strict sanity
    json.dumps(feats, sort_keys=True, ensure_ascii=False)


def _stable_score_projection(out: Any) -> dict[str, Any]:
    """
    Create a stable projection for determinism assertions (avoid giant dicts).
    """
    if isinstance(out, int | float):
        return {"score": float(out)}
    if isinstance(out, dict):
        proj: dict[str, Any] = {}
        for k in (
            "score",
            "entry_score",
            "s",
            "value",
            "blended_score",
            "ok",
            "should_enter",
            "reason",
            "why",
        ):
            if k in out:
                proj[k] = out.get(k)
        return proj or dict(out)
    if isinstance(out, tuple):
        return {
            "tuple": [
                ("B" if isinstance(x, bool) else ("N" if isinstance(x, int | float) else "X"))
                for x in out
            ],
            "raw": out,
        }
    if hasattr(out, "__dict__") and isinstance(out.__dict__, dict):
        return dict(out.__dict__)
    return {"type": type(out).__name__, "repr": repr(out)[:200]}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
def test_entry_modules_import_side_effect_free(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Contract: modules import in SAFE_MODE with no network at import time.
    """
    _freeze_determinism(monkeypatch, fixed_ts=1_700_000_000)
    with _no_network(monkeypatch):
        fb = _import("utils_pkg.feature_builder", reload=True)
        el = _import("utils_pkg.ai_entry_logic", reload=True)
    assert fb is not None
    assert el is not None


def test_entry_contract_symbols_exist() -> None:
    fb = _import("utils_pkg.feature_builder", reload=True)
    entry = _import("utils_pkg.ai_entry_logic", reload=True)
    _resolve_build_features(fb)
    _resolve_entry_fns(entry)


@pytest.mark.asyncio
async def test_entry_pipeline_smoke_minimal(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    End-to-end: snapshot -> features -> score -> should_enter -> decide_entry.
    Deterministic + bounded + JSON-safe.
    """
    fixed_now = 1_700_000_000
    _freeze_determinism(monkeypatch, fixed_now)

    fb_mod = _import("utils_pkg.feature_builder", reload=True)
    el_mod = _import("utils_pkg.ai_entry_logic", reload=True)

    build_features = _resolve_build_features(fb_mod)
    score_entry, should_enter, decide_entry = _resolve_entry_fns(el_mod)

    snap = _load_candidate_fixture() or _minimal_candidate_snapshot()
    ctx = _ctx()
    pool = _kw_pool(snap, ctx)

    with _no_network(monkeypatch, fb_mod, el_mod):
        feats_a = await _maybe_await(_call_sig_filtered(build_features, pool))
        feats_b = await _maybe_await(_call_sig_filtered(build_features, pool))

        assert isinstance(feats_a, dict), (
            f"build_features must return dict; got {type(feats_a).__name__}"
        )
        assert feats_a, "build_features returned empty dict; pipeline would be blind"
        _assert_json_safe_map(feats_a)

        # Deterministic features for same inputs in SAFE_MODE
        assert feats_a == feats_b, (
            "build_features must be deterministic for same inputs in SAFE_MODE"
        )

        pool_sc = _kw_pool(snap, ctx, feats_a)
        score_out_1 = await _maybe_await(_call_sig_filtered(score_entry, pool_sc))
        score_out_2 = await _maybe_await(_call_sig_filtered(score_entry, pool_sc))

        s1 = _extract_float_score(score_out_1)
        s2 = _extract_float_score(score_out_2)

        assert 0.0 <= s1 <= 1.0, f"score_entry must be bounded [0,1]; got {s1}"
        assert 0.0 <= s2 <= 1.0, f"score_entry must be bounded [0,1]; got {s2}"

        # Deterministic score for same inputs in SAFE_MODE
        assert _stable_score_projection(score_out_1) == _stable_score_projection(score_out_2), (
            "score_entry must be deterministic for identical features in SAFE_MODE.\n"
            f"A: {_stable_score_projection(score_out_1)}\n"
            f"B: {_stable_score_projection(score_out_2)}"
        )

        enter_out_1 = await _maybe_await(_call_sig_filtered(should_enter, pool_sc))
        enter_out_2 = await _maybe_await(_call_sig_filtered(should_enter, pool_sc))
        dec_out_1 = await _maybe_await(_call_sig_filtered(decide_entry, pool_sc))
        dec_out_2 = await _maybe_await(_call_sig_filtered(decide_entry, pool_sc))

    e1 = _extract_bool_decision(enter_out_1)
    e2 = _extract_bool_decision(enter_out_2)
    d1 = _extract_bool_decision(dec_out_1)
    d2 = _extract_bool_decision(dec_out_2)

    assert isinstance(e1, bool) and isinstance(e2, bool)
    assert isinstance(d1, bool) and isinstance(d2, bool)

    assert e1 == e2, "should_enter must be deterministic for identical inputs in SAFE_MODE"
    assert d1 == d2, "decide_entry must be deterministic for identical inputs in SAFE_MODE"


@pytest.mark.asyncio
async def test_entry_pipeline_handles_sparse_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Builder must not crash if optional fields are missing.
    It may produce fewer features, but must return JSON-safe dict and allow scoring/decision.
    """
    fixed_now = 1_700_000_123
    _freeze_determinism(monkeypatch, fixed_now)

    fb_mod = _import("utils_pkg.feature_builder", reload=True)
    el_mod = _import("utils_pkg.ai_entry_logic", reload=True)

    build_features = _resolve_build_features(fb_mod)
    score_entry, should_enter, decide_entry = _resolve_entry_fns(el_mod)

    snap = _sparse_candidate_snapshot()
    ctx = dict(_ctx())
    ctx["now_ts"] = fixed_now

    pool = _kw_pool(snap, ctx)

    with _no_network(monkeypatch, fb_mod, el_mod):
        feats = await _maybe_await(_call_sig_filtered(build_features, pool))
        assert isinstance(feats, dict)
        _assert_json_safe_map(feats)

        pool_sc = _kw_pool(snap, ctx, feats)
        score_out = await _maybe_await(_call_sig_filtered(score_entry, pool_sc))
        s = _extract_float_score(score_out)
        assert 0.0 <= s <= 1.0

        enter_out = await _maybe_await(_call_sig_filtered(should_enter, pool_sc))
        decide_out = await _maybe_await(_call_sig_filtered(decide_entry, pool_sc))

    assert isinstance(_extract_bool_decision(enter_out), bool)
    assert isinstance(_extract_bool_decision(decide_out), bool)


@pytest.mark.asyncio
async def test_entry_decisions_do_not_flip_within_same_inputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Not required that should_enter == decide_entry (some implementations add guards),
    but *each* should be stable for the same exact feature map within a single run.
    """
    fixed_now = 1_700_000_777
    _freeze_determinism(monkeypatch, fixed_now)

    fb_mod = _import("utils_pkg.feature_builder", reload=True)
    el_mod = _import("utils_pkg.ai_entry_logic", reload=True)

    build_features = _resolve_build_features(fb_mod)
    _, should_enter, decide_entry = _resolve_entry_fns(el_mod)

    snap = _load_candidate_fixture() or _minimal_candidate_snapshot()
    ctx = dict(_ctx())
    ctx["now_ts"] = fixed_now

    with _no_network(monkeypatch, fb_mod, el_mod):
        feats = await _maybe_await(_call_sig_filtered(build_features, _kw_pool(snap, ctx)))
        pool_sc = _kw_pool(snap, ctx, feats)

        a = await _maybe_await(_call_sig_filtered(should_enter, pool_sc))
        b = await _maybe_await(_call_sig_filtered(should_enter, pool_sc))
        c = await _maybe_await(_call_sig_filtered(decide_entry, pool_sc))
        d = await _maybe_await(_call_sig_filtered(decide_entry, pool_sc))

    assert _extract_bool_decision(a) == _extract_bool_decision(b), (
        "should_enter flipped for identical inputs"
    )
    assert _extract_bool_decision(c) == _extract_bool_decision(d), (
        "decide_entry flipped for identical inputs"
    )
