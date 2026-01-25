# tests/unit_tests.py
from __future__ import annotations

"""
tests/unit_tests.py

Strict, non-flaky unit tests for *pure helpers* (no RPC, no network, no wallets).

Goals
-----
- Keep these tests FAST and deterministic.
- Avoid importing modules that trigger live trading side-effects.
- Validate small helper contracts used across the pipeline.

Notes
-----
- We intentionally test "private" helpers in scripts.main because they are part of
  the operational contract (parsing, normalization, mint heuristics, basic filters).
"""

import datetime as dt
import sys
from pathlib import Path
from typing import Any

import pytest

# ─────────────────────────────────────────────────────────────────────────────
# Import helpers: ensure repo root is on sys.path (so `pytest` works anywhere)
# ─────────────────────────────────────────────────────────────────────────────
_HERE = Path(__file__).resolve().parent
for _p in (_HERE, *_HERE.parents):
    if (_p / "utils_pkg").is_dir() and (_p / "scripts").is_dir():
        if str(_p) not in sys.path:
            sys.path.insert(0, str(_p))
        break


@pytest.fixture(scope="session")
def main_mod():
    """
    Import scripts.main once for the whole test session.
    If this fails, it's a hard signal your entrypoint module is not import-safe.
    """
    try:
        from scripts import main as M  # type: ignore
    except Exception as e:
        # Keep failure loud + actionable.
        raise RuntimeError(f"Failed to import scripts.main for unit tests: {e}") from e
    return M


# ─────────────────────────────────────────────────────────────────────────────
# _approx_minutes_since
# ─────────────────────────────────────────────────────────────────────────────


def _utc_ts_now() -> float:
    return dt.datetime.now(dt.UTC).timestamp()


def test_approx_minutes_since_seconds(main_mod):
    """
    Passing a seconds timestamp should return a small non-negative float near 0.
    """
    now = _utc_ts_now()
    rv = main_mod._approx_minutes_since(now)
    assert rv is not None
    assert isinstance(rv, int | float)
    # Allow tiny clock/rounding jitter; must not be wildly off.
    assert -0.05 <= float(rv) <= 2.0


def test_approx_minutes_since_millis(main_mod):
    """
    Passing a milliseconds timestamp should be handled correctly.
    """
    now_ms = int(_utc_ts_now() * 1000)
    rv = main_mod._approx_minutes_since(now_ms)
    assert rv is not None
    assert isinstance(rv, int | float)
    assert -0.05 <= float(rv) <= 2.0


def test_approx_minutes_since_none(main_mod):
    """
    None/empty-ish inputs should not crash. Contract: return None.
    """
    rv = main_mod._approx_minutes_since(None)
    assert rv is None


# ─────────────────────────────────────────────────────────────────────────────
# _normalize_from_source
# ─────────────────────────────────────────────────────────────────────────────


def test_normalize_from_source_strips(main_mod):
    rv = main_mod._normalize_from_source("  abc  ")
    assert isinstance(rv, str)
    assert rv == rv.strip()
    assert rv.lower() == "abc"


def test_normalize_from_source_empty(main_mod):
    rv = main_mod._normalize_from_source("   ")
    assert isinstance(rv, str)
    # Should normalize to something safe (often "" or "unknown"); must not be whitespace.
    assert rv == rv.strip()


# ─────────────────────────────────────────────────────────────────────────────
# _is_probable_mint
# ─────────────────────────────────────────────────────────────────────────────


def test_is_probable_mint_empty_false(main_mod):
    assert main_mod._is_probable_mint("") is False


def test_is_probable_mint_garbage_false(main_mod):
    # contains illegal base58 chars like 0/O/I/l, or just too short
    assert main_mod._is_probable_mint("0OIl") is False
    assert main_mod._is_probable_mint("abc") is False


def test_is_probable_mint_known_true(main_mod):
    # Wrapped SOL mint (commonly used in Solana tooling)
    wsol = "So11111111111111111111111111111111111111112"
    assert main_mod._is_probable_mint(wsol) is True


# ─────────────────────────────────────────────────────────────────────────────
# _passes_basic_filters
# ─────────────────────────────────────────────────────────────────────────────


def test_passes_basic_filters_rejects_too_old(main_mod, monkeypatch: pytest.MonkeyPatch):
    """
    If created_at is extremely old, basic filters should reject when ENTRY_MAX_AGE_MIN is low.
    This test is stable because the date is fixed (year 2000).
    """
    ev: dict[str, Any] = {
        "pool": "pump",
        "created_at": "2000-01-01T00:00:00Z",
        "txType": "create",
        "initialBuy": 1000,
    }

    # Ensure we trigger the age gate without affecting other tests.
    if not hasattr(main_mod, "ENTRY_MAX_AGE_MIN"):
        pytest.skip("scripts.main has no ENTRY_MAX_AGE_MIN; skipping age-gate unit test.")

    monkeypatch.setattr(main_mod, "ENTRY_MAX_AGE_MIN", 1, raising=False)
    assert main_mod._passes_basic_filters(ev) is False


def test_passes_basic_filters_handles_missing_fields(main_mod):
    """
    Missing/partial events should not crash the filter function.
    Contract: return False for clearly insufficient events.
    """
    assert main_mod._passes_basic_filters({}) is False
    assert main_mod._passes_basic_filters({"created_at": "not-a-date"}) in (
        False,
        True,
    )  # must not raise


# Optional: run a minimal “likely pass” smoke on the basic filter,
# but keep it tolerant since the filter may include multiple gates.
def test_passes_basic_filters_reasonable_event_does_not_throw(main_mod):
    ev: dict[str, Any] = {
        "pool": "pump",
        "created_at": dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "txType": "create",
        "initialBuy": 1000,
        "from": "pumpfun",
    }
    _ = main_mod._passes_basic_filters(ev)  # must not raise
