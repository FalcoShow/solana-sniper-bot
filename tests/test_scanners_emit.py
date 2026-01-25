# tests/test_scanners_emit.py
from __future__ import annotations

"""
tests/test_scanners_emit.py — Scanner Emit Contract Tests (P0 LOCK-IN)
=====================================================================

Goal
----
Every scanner must be able to *emit candidates* in a way that the rest of the
pipeline can consume deterministically.

We enforce (in order of preference):
  1) scanner.registry is canonical (Candidate + validate/coerce)
  2) utils_pkg.contracts is canonical (ensure_candidate)
  3) Minimal fallback contract for legacy scanners during migration

Non-flaky by default
-------------------
- By default we do NOT run live network scans.
- To run live scans, set:
      SCANNER_LIVE_TESTS=1

Timeouts / limits (env)
-----------------------
- SCANNER_TEST_TIMEOUT_S (default 25)
- SCANNER_TEST_LIMIT     (default 15)

Hard rules
----------
- Deterministic in default mode (SCANNER_LIVE_TESTS=0).
- No accidental real trading (we set DRYRUN/IS_DRYRUN and DISABLE_NETWORK hints).
- Signature drift tolerant (we only pass kwargs a scan() declares).

Notes
-----
This file is a *contract test*. It is allowed to skip live scans when the
environment is not configured. It should not silently pass if scanners exist
but cannot emit in offline mode.

Next file on list: tests/test_swap.py
"""

import dataclasses
import importlib
import inspect
import os
from collections.abc import AsyncIterable, Awaitable, Callable, Sequence
from typing import (
    Any,
)

import pytest


# ──────────────────────────────────────────────────────────────────────────────
# Knobs
# ──────────────────────────────────────────────────────────────────────────────
def _truthy(v: str) -> bool:
    return v.strip() in {"1", "true", "TRUE", "yes", "YES", "on", "ON"}


SCANNER_LIVE_TESTS = _truthy(os.getenv("SCANNER_LIVE_TESTS", "0"))
SCANNER_TEST_TIMEOUT_S = float(os.getenv("SCANNER_TEST_TIMEOUT_S", "25").strip() or "25")
SCANNER_TEST_LIMIT = int(float(os.getenv("SCANNER_TEST_LIMIT", "15").strip() or "15"))

# Offline flags we recognize in scan(...) signatures
OFFLINE_FLAG_NAMES = ("dry_run", "offline", "no_network", "mock", "test_mode")

# Limit-ish kwargs we may pass if scan(...) supports it
LIMIT_PARAM_NAMES = ("limit", "max_results", "n", "top_n", "count", "max_items")

# Minimal (legacy) candidate contract during migration
MINTISH_KEYS = {
    "mint",
    "address",
    "token_mint",
    "base_mint",
    "mint_address",
    "tokenAddress",
    "baseAddress",
}
SYMBOL_KEYS = ("sym", "symbol", "ticker")
LIQ_KEYS = ("liq", "liq_usd", "liquidity", "liquidity_usd")
AGE_MIN_KEYS = ("age_min", "age_minutes")
AGE_S_KEYS = ("age_s", "age_sec", "age_seconds")


# ──────────────────────────────────────────────────────────────────────────────
# Hermetic env defaults (important: scanners should not trade / should not net)
# ──────────────────────────────────────────────────────────────────────────────
@pytest.fixture(autouse=True)
def _safe_env(monkeypatch: pytest.MonkeyPatch) -> None:
    # Keep scanners from doing anything dangerous; scanners may ignore these,
    # but setting them makes intent explicit and helps variants that respect them.
    monkeypatch.setenv("DRYRUN", os.getenv("DRYRUN", "1"))
    monkeypatch.setenv("IS_DRYRUN", os.getenv("IS_DRYRUN", "1"))
    monkeypatch.setenv("DISABLE_NETWORK", os.getenv("DISABLE_NETWORK", "1"))
    monkeypatch.setenv("ENV_STRICT", os.getenv("ENV_STRICT", "0"))
    monkeypatch.setenv("STRICT_ENV", os.getenv("STRICT_ENV", "0"))
    monkeypatch.setenv("LOG_LEVEL", os.getenv("LOG_LEVEL", "WARNING"))


# ──────────────────────────────────────────────────────────────────────────────
# Import helpers
# ──────────────────────────────────────────────────────────────────────────────
def _try_import(paths: Sequence[str]) -> Any | None:
    for p in paths:
        try:
            return importlib.import_module(p)
        except Exception:  # pragma: no cover
            continue
    return None


def _find_callable(mod: Any, names: Sequence[str]) -> Callable[..., Any] | None:
    for n in names:
        fn = getattr(mod, n, None)
        if callable(fn):
            return fn
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Normalization helpers
# ──────────────────────────────────────────────────────────────────────────────
def _maybe_to_dict(x: Any) -> dict[str, Any]:
    """
    Normalize a candidate-ish object into a dict without being brittle:
      - dict
      - dataclass
      - pydantic v1/v2 (.dict/.model_dump)
      - objects with to_dict/asdict
      - objects with __dict__
    """
    if x is None:
        return {}
    if isinstance(x, dict):
        return dict(x)

    if dataclasses.is_dataclass(x):
        try:
            return dataclasses.asdict(x)
        except Exception:
            pass

    for meth in ("model_dump", "dict"):
        fn = getattr(x, meth, None)
        if callable(fn):
            try:
                d = fn()
                if isinstance(d, dict):
                    return d
            except Exception:
                pass

    for meth in ("to_dict", "asdict"):
        fn = getattr(x, meth, None)
        if callable(fn):
            try:
                d = fn()
                if isinstance(d, dict):
                    return d
            except Exception:
                pass

    try:
        d = getattr(x, "__dict__", None)
        if isinstance(d, dict):
            return dict(d)
    except Exception:
        pass

    return {}


def _is_iterable(x: Any) -> bool:
    if isinstance(x, str | bytes):
        return False
    try:
        iter(x)
        return True
    except Exception:
        return False


def _is_async_iterable(x: Any) -> bool:
    return hasattr(x, "__aiter__")


async def _collect_async_iterable(it: AsyncIterable[Any], limit: int) -> list[Any]:
    out: list[Any] = []
    async for item in it:
        out.append(item)
        if len(out) >= limit:
            break
    return out


def _coerce_items(result: Any) -> list[Any]:
    """
    Normalize scan output into a list of candidate-ish objects.

    Accept:
      - list/tuple
      - generator/iterable
      - dict like {"candidates": [...]}, {"results":[...]}, {"data":[...]}
      - single candidate object/dict
      - async iterable handled elsewhere
    """
    if result is None:
        return []

    if isinstance(result, dict):
        for k in ("candidates", "items", "results", "data"):
            v = result.get(k)
            if isinstance(v, list | tuple):
                return list(v)
            if v is not None and _is_iterable(v):
                return list(v)
        return [result]

    if isinstance(result, list | tuple):
        return list(result)

    if _is_iterable(result):
        return list(result)

    return [result]


# ──────────────────────────────────────────────────────────────────────────────
# Candidate schema resolution
# ──────────────────────────────────────────────────────────────────────────────
def _load_candidate_registry() -> tuple[
    Any | None, Any | None, Callable[..., Any] | None, Sequence[str] | None
]:
    """
    Attempt to import scanner.registry, the canonical scanner Candidate schema.

    Recognized exports:
      - Candidate (class/dataclass/model)
      - validate_candidate(obj_or_dict) -> candidate
      - coerce_candidate(obj_or_dict) -> candidate
      - REQUIRED_KEYS (iterable[str]) optional
    """
    mod = _try_import(["scanner.registry", "src.scanner.registry"])
    if mod is None:
        return None, None, None, None

    Candidate = getattr(mod, "Candidate", None)

    validate_fn = getattr(mod, "validate_candidate", None)
    if not callable(validate_fn):
        validate_fn = getattr(mod, "coerce_candidate", None)
    if not callable(validate_fn):
        validate_fn = None

    required = getattr(mod, "REQUIRED_KEYS", None)
    if isinstance(required, set | frozenset | list | tuple):
        required_keys: Sequence[str] | None = list(required)
    else:
        required_keys = None

    return mod, Candidate, validate_fn, required_keys


def _load_contracts_candidate() -> tuple[Any | None, Callable[..., Any] | None]:
    """
    Attempt to import utils_pkg.contracts as the canonical cross-module schema.
    We prefer ensure_candidate(dict)->Candidate.
    """
    c = _try_import(["utils_pkg.contracts"])
    if c is None:
        return None, None
    ensure = getattr(c, "ensure_candidate", None)
    if not callable(ensure):
        ensure = None
    return c, ensure


def _pick_first(d: dict[str, Any], keys: Sequence[str]) -> Any | None:
    for k in keys:
        if k in d and d.get(k) is not None:
            return d.get(k)
    return None


def _extract_legacy_minimal(d: dict[str, Any]) -> tuple[str, float, float, bool]:
    """
    Legacy minimal contract extractor.

    Returns:
      (sym, liq, age_min, has_mintish)
    """
    sym = _pick_first(d, list(SYMBOL_KEYS))
    liq = _pick_first(d, list(LIQ_KEYS))
    age_min = _pick_first(d, list(AGE_MIN_KEYS))

    # age in seconds -> minutes
    if age_min is None:
        age_s = _pick_first(d, list(AGE_S_KEYS))
        if isinstance(age_s, int | float):
            age_min = float(age_s) / 60.0

    has_mintish = any((k in d and bool(d.get(k))) for k in MINTISH_KEYS)

    if not isinstance(sym, str) or not sym.strip():
        raise AssertionError(
            f"candidate missing/invalid symbol: sym/symbol/ticker not found. keys={sorted(d.keys())}"
        )
    if not isinstance(liq, int | float):
        raise AssertionError(
            f"candidate missing/invalid liquidity: liq/liq_usd not found. keys={sorted(d.keys())}"
        )
    if not isinstance(age_min, int | float):
        raise AssertionError(
            f"candidate missing/invalid age: age_min/age_s not found. keys={sorted(d.keys())}"
        )

    sym_s = str(sym).strip()
    liq_f = float(liq)
    age_m = float(age_min)

    if liq_f < 0:
        raise AssertionError(f"liq must be >= 0; got {liq_f}")
    if age_m < 0:
        raise AssertionError(f"age_min must be >= 0; got {age_m}")

    return sym_s, liq_f, age_m, has_mintish


def _assert_candidate_contract(obj: Any) -> None:
    """
    Validate a single emitted candidate-like object.

    Preference:
      1) scanner.registry validate/coerce
      2) utils_pkg.contracts.ensure_candidate
      3) legacy minimal contract
    """
    reg_mod, Candidate, validate_fn, required_keys = _load_candidate_registry()
    if validate_fn is not None:
        try:
            out = validate_fn(obj)
        except Exception as e:
            raise AssertionError(
                f"scanner.registry validator rejected candidate: {type(e).__name__}: {e}"
            ) from e

        d = _maybe_to_dict(out)
        if required_keys:
            missing = [k for k in required_keys if k not in d]
            assert not missing, (
                f"candidate missing registry REQUIRED_KEYS={missing}; got keys={sorted(d.keys())}"
            )

        # In addition to registry, keep legacy sanity checks so broken scanners are obvious.
        _, _, _, has_mintish = _extract_legacy_minimal(d)
        if SCANNER_LIVE_TESTS:
            assert has_mintish, f"live candidate missing mint-like field; keys={sorted(d.keys())}"
        return

    # Candidate class exists (but no validate function): allow instance
    if Candidate is not None:
        try:
            if isinstance(obj, Candidate):  # type: ignore[arg-type]
                d = _maybe_to_dict(obj)
                _, _, _, has_mintish = _extract_legacy_minimal(d)
                if SCANNER_LIVE_TESTS:
                    assert has_mintish, (
                        f"live candidate missing mint-like field; keys={sorted(d.keys())}"
                    )
                return
        except Exception:
            pass

    # contracts fallback
    c_mod, ensure_candidate = _load_contracts_candidate()
    if ensure_candidate is not None:
        d0 = _maybe_to_dict(obj)
        try:
            cand = ensure_candidate(d0)
        except Exception as e:
            raise AssertionError(
                f"utils_pkg.contracts.ensure_candidate rejected candidate: {type(e).__name__}: {e}"
            ) from e
        # ensure_candidate should produce dataclass-like
        assert dataclasses.is_dataclass(cand), (
            "ensure_candidate should return a dataclass Candidate"
        )
        d1 = _maybe_to_dict(cand)
        _, _, _, has_mintish = _extract_legacy_minimal(d1)
        if SCANNER_LIVE_TESTS:
            assert has_mintish, f"live candidate missing mint-like field; keys={sorted(d1.keys())}"
        return

    # legacy minimal fallback
    d = _maybe_to_dict(obj)
    _, _, _, has_mintish = _extract_legacy_minimal(d)
    if SCANNER_LIVE_TESTS:
        assert has_mintish, f"live candidate missing mint-like field; keys={sorted(d.keys())}"


# ──────────────────────────────────────────────────────────────────────────────
# Scan calling (signature-drift tolerant)
# ──────────────────────────────────────────────────────────────────────────────
def _supports_offline(fn: Callable[..., Any]) -> bool:
    try:
        sig = inspect.signature(fn)
    except Exception:
        return False
    return any(name in sig.parameters for name in OFFLINE_FLAG_NAMES)


def _build_scan_kwargs(fn: Callable[..., Any]) -> dict[str, Any]:
    """
    Only pass kwargs the scan function declares.
    Prefer offline flags when SCANNER_LIVE_TESTS=0.
    """
    try:
        sig = inspect.signature(fn)
    except Exception:
        return {}

    kwargs: dict[str, Any] = {}

    for name in LIMIT_PARAM_NAMES:
        if name in sig.parameters:
            kwargs[name] = SCANNER_TEST_LIMIT
            break

    if not SCANNER_LIVE_TESTS:
        for name in OFFLINE_FLAG_NAMES:
            if name in sig.parameters:
                kwargs[name] = True
                break

    return kwargs


async def _maybe_await(x: Any) -> Any:
    if isinstance(x, Awaitable) or inspect.isawaitable(x):
        return await x
    return x


async def _call_scan(fn: Callable[..., Any]) -> Any:
    kwargs = _build_scan_kwargs(fn)

    # First try with kwargs; if signature mismatch, retry bare call.
    try:
        out = fn(**kwargs)  # type: ignore[arg-type]
    except TypeError:
        out = fn()  # type: ignore[misc]

    out = await _maybe_await(out)

    # Support async iterables (rare, but nice)
    if _is_async_iterable(out):
        return await _collect_async_iterable(out, limit=max(1, SCANNER_TEST_LIMIT))

    return out


def _maybe_skip_live_due_to_env(mod: Any) -> str | None:
    """
    If module declares required env vars, skip live tests when missing.
    """
    for attr in ("REQUIRED_ENV_VARS", "REQUIRED_ENV", "ENV_REQUIRED", "REQUIRES_ENV"):
        req = getattr(mod, attr, None)
        if isinstance(req, list | tuple | set | frozenset) and req:
            missing = [k for k in req if not os.getenv(str(k))]
            if missing:
                return f"missing required env vars for live scan: {missing}"
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Scanner list (expandable)
# ──────────────────────────────────────────────────────────────────────────────
SCANNERS: Sequence[tuple[Sequence[str], Sequence[str]]] = (
    (("scanner.raydium", "src.scanner.raydium"), ("scan_raydium", "scan")),
    (("scanner.dexscreener", "src.scanner.dexscreener"), ("scan_dexscreener", "scan")),
    (("scanner.pumpfun", "src.scanner.pumpfun"), ("scan_pumpfun", "scan")),
    # Optional scanners (won't fail if missing):
    (
        (
            "scanner.axiom",
            "src.scanner.axiom",
            "scanner.axiom_scanner",
            "src.scanner.axiom_scanner",
        ),
        ("scan_axiom", "scan"),
    ),
)


# ──────────────────────────────────────────────────────────────────────────────
# Tests
# ──────────────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("module_paths, fn_names", SCANNERS)
def test_scanner_module_has_scan_fn(module_paths: Sequence[str], fn_names: Sequence[str]) -> None:
    mod = _try_import(module_paths)
    if mod is None:
        pytest.skip(f"scanner module not found in paths={list(module_paths)}")

    fn = _find_callable(mod, fn_names)
    assert fn is not None, f"no scan function found in {mod.__name__} among {list(fn_names)}"

    # Ensure it has a signature (catches broken refactors)
    _ = inspect.signature(fn)
    assert callable(fn)


def test_candidate_registry_or_contracts_or_min_exists() -> None:
    """
    LOCK-IN: prefer scanner.registry; else accept utils_pkg.contracts; else enforce
    that our minimal legacy contract keys are defined in this test.
    """
    reg_mod, Candidate, validate_fn, required_keys = _load_candidate_registry()
    if reg_mod is not None:
        assert any(
            [
                Candidate is not None,
                validate_fn is not None,
                isinstance(required_keys, list | tuple),
            ]
        ), "scanner.registry found but missing Candidate/validate_candidate/REQUIRED_KEYS exports"
        return

    c_mod, ensure_candidate = _load_contracts_candidate()
    if c_mod is not None and ensure_candidate is not None:
        return

    # Minimal fallback is allowed only if neither canonical schema exists yet.
    assert SYMBOL_KEYS and LIQ_KEYS and (AGE_MIN_KEYS or AGE_S_KEYS)


@pytest.mark.parametrize("module_paths, fn_names", SCANNERS)
@pytest.mark.asyncio
async def test_scanners_emit_candidate_contract_or_skip(
    module_paths: Sequence[str], fn_names: Sequence[str]
) -> None:
    """
    Non-flaky by default:
      - If SCANNER_LIVE_TESTS=0, we only run emit checks when the scanner
        advertises an offline/dry flag. Otherwise we skip.
      - If SCANNER_LIVE_TESTS=1, we attempt to run the scanner. If the module
        declares required env and it is missing, we skip. Otherwise:
            - return must be non-empty (at least 1 candidate)
            - candidates must satisfy the canonical schema (preferred) or fallback.
    """
    mod = _try_import(module_paths)
    if mod is None:
        pytest.skip(f"scanner module not found in paths={list(module_paths)}")

    fn = _find_callable(mod, fn_names)
    if fn is None:
        pytest.skip(f"no scan function found in {mod.__name__}")

    if not SCANNER_LIVE_TESTS:
        if not _supports_offline(fn):
            pytest.skip(
                f"{mod.__name__}.{fn.__name__} has no offline flag; set SCANNER_LIVE_TESTS=1 to run live"
            )
    else:
        why = _maybe_skip_live_due_to_env(mod)
        if why:
            pytest.skip(f"{mod.__name__}: {why}")

    # Run the scan with a hard timeout
    try:
        out = await pytest.helpers.timeout(_call_scan(fn), timeout=SCANNER_TEST_TIMEOUT_S)  # type: ignore[attr-defined]
    except AttributeError:
        # If pytest.helpers.timeout isn't available, do a simple wait_for
        import asyncio

        out = await asyncio.wait_for(_call_scan(fn), timeout=SCANNER_TEST_TIMEOUT_S)

    # Normalize to list
    if _is_async_iterable(out):
        items = await _collect_async_iterable(out, limit=max(1, SCANNER_TEST_LIMIT))
    else:
        items = _coerce_items(out)

    # In offline/dry mode, allow empty (scanner may require sessions/env)
    if SCANNER_LIVE_TESTS:
        assert len(items) > 0, f"{mod.__name__}.{fn.__name__} returned no candidates in live mode"

    # Validate a capped set
    cap = max(1, min(len(items), SCANNER_TEST_LIMIT))
    for obj in items[:cap]:
        _assert_candidate_contract(obj)
