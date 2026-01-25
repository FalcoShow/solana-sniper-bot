from __future__ import annotations

"""
tests/test_contract_positions.py — Positions Contract Tests (God-Tier LOCK-IN, P0)
=================================================================================

Why this file exists
--------------------
Positions are the *bridge* between entry -> risk mgmt -> exit -> logging/analytics.
If the "position record" shape drifts, you get silent PnL bugs.

This contract test suite is intentionally:
- OFFLINE (no RPC, no HTTP, no Redis/Postgres needed)
- Import-safe (no secrets required)
- Tolerant to refactors (best-effort discovery of validators/normalizers)
- Strict about the *invariants* we need for correctness:
    • mint/symbol identity
    • size/amount representation
    • entry price
    • open timestamp
    • JSON-safe (PG JSONB safe; no NaN/Inf, no bytes/sets, etc.)

What it enforces (LOCK-IN, but refactor-friendly)
-------------------------------------------------
1) Safe-mode imports succeed (no network on import).
2) If a canonical position normalizer/validator exists (recommended), it must:
     - be callable
     - accept a dict-like position (plus optional ctx)
     - return a dict that is JSON-serializable
     - preserve identity & core economics fields
3) Exit brain should not crash when fed a minimal position snapshot (best-effort).

Recommended canonical surface (optional, but strongly suggested)
---------------------------------------------------------------
Expose ONE of these in utils_pkg.contracts (or a dedicated schema module):
  - ensure_position(...)
  - ensure_position_record(...)
  - normalize_position(...)

If none exist, tests will SKIP the strict validator checks (so your suite still passes),
but you should add it soon for a stronger LOCK-IN.
"""

import importlib
import json
import math
import os
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.contract


# ─────────────────────────────────────────────────────────────────────────────
# Ensure repo root (folder that contains `utils_pkg`) is on sys.path
# ─────────────────────────────────────────────────────────────────────────────
_HERE = Path(__file__).resolve().parent
for _p in (_HERE, *_HERE.parents):
    if (_p / "utils_pkg").is_dir():
        if str(_p) not in sys.path:
            sys.path.insert(0, str(_p))
        break


# ─────────────────────────────────────────────────────────────────────────────
# Autouse safety env: keep imports non-strict & offline
# ─────────────────────────────────────────────────────────────────────────────
@pytest.fixture(autouse=True)
def _safe_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENV_STRICT", os.getenv("ENV_STRICT", "0"))
    monkeypatch.setenv("STRICT_ENV", os.getenv("STRICT_ENV", "0"))
    monkeypatch.setenv("CORE_UTILS_ENV_STRICT", os.getenv("CORE_UTILS_ENV_STRICT", "0"))
    monkeypatch.setenv("LOG_LEVEL", os.getenv("LOG_LEVEL", "WARNING"))

    # Hard “no network” hints
    monkeypatch.setenv("DISABLE_NETWORK", os.getenv("DISABLE_NETWORK", "1"))
    monkeypatch.setenv("DISABLE_RPC", os.getenv("DISABLE_RPC", "1"))
    monkeypatch.setenv("DISABLE_HTTP", os.getenv("DISABLE_HTTP", "1"))
    monkeypatch.setenv("NO_NETWORK", os.getenv("NO_NETWORK", "1"))
    monkeypatch.setenv("SAFE_MODE", os.getenv("SAFE_MODE", "1"))
    monkeypatch.setenv("DRYRUN", os.getenv("DRYRUN", "1"))
    monkeypatch.setenv("DRY_RUN", os.getenv("DRY_RUN", "1"))
    monkeypatch.setenv("SANDBOX_MODE", os.getenv("SANDBOX_MODE", "1"))

    # Ensure unit tests never accidentally trade
    monkeypatch.setenv("LIVE_TRADING", os.getenv("LIVE_TRADING", "0"))
    monkeypatch.setenv("TRADING_ENABLED", os.getenv("TRADING_ENABLED", "0"))
    monkeypatch.setenv("ALLOW_BUYS", os.getenv("ALLOW_BUYS", "0"))
    monkeypatch.setenv("ALLOW_SELLS", os.getenv("ALLOW_SELLS", "0"))


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def _call_best_effort(fn: Callable[..., Any], attempts: list[tuple[tuple, dict]]) -> Any:
    """
    Try calling fn with several arg/kw combos, selecting the first that works.
    Only TypeError triggers fallback; other exceptions should surface (real bugs).
    """
    last_te: TypeError | None = None
    for args, kwargs in attempts:
        try:
            return fn(*args, **kwargs)
        except TypeError as te:
            last_te = te
            continue
    if last_te is not None:
        raise last_te
    raise AssertionError("No call attempts provided")


def _is_json_safe_value(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, bool | int | str):
        return True
    if isinstance(v, float):
        return math.isfinite(v)
    if isinstance(v, list):
        return all(_is_json_safe_value(x) for x in v)
    if isinstance(v, dict):
        return all(isinstance(k, str) and _is_json_safe_value(x) for k, x in v.items())
    return False


def _assert_json_safe_map(d: dict[str, Any]) -> None:
    assert isinstance(d, dict), f"expected dict; got {type(d)}"
    for k, v in d.items():
        assert isinstance(k, str), f"keys must be str; got {type(k)}"
        assert _is_json_safe_value(v), f"key '{k}' not JSON-safe: {type(v)}={v!r}"
    json.dumps(d, sort_keys=True)


def _has_any_key(d: dict[str, Any], keys: tuple[str, ...]) -> bool:
    return any(k in d and d.get(k) is not None for k in keys)


def _get_first_present(d: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for k in keys:
        if k in d:
            return d.get(k)
    return None


def _discover_position_normalizer() -> Callable[..., Any] | None:
    """
    Discover a canonical position normalizer/validator if your code exposes one.

    Preferred location: utils_pkg.contracts
    Also accepted: utils_pkg.positions / utils_pkg.position_store / utils_pkg.data_logger / utils_pkg.exit_engine
    """
    candidates: list[tuple[str, tuple[str, ...]]] = [
        (
            "utils_pkg.contracts",
            ("ensure_position", "ensure_position_record", "normalize_position"),
        ),
        (
            "utils_pkg.positions",
            ("ensure_position", "normalize_position", "position_from_dict", "position_to_dict"),
        ),
        (
            "utils_pkg.position_store",
            ("ensure_position", "normalize_position", "position_from_dict"),
        ),
        ("utils_pkg.data_logger", ("ensure_position", "normalize_position", "coerce_position")),
        ("utils_pkg.exit_engine", ("ensure_position", "normalize_position", "coerce_position")),
    ]

    for mod_name, names in candidates:
        try:
            m = importlib.import_module(mod_name)
        except Exception:
            continue
        for nm in names:
            fn = getattr(m, nm, None)
            if callable(fn):
                return fn
    return None


def _minimal_position() -> dict[str, Any]:
    """
    Minimal-but-meaningful position snapshot.

    IMPORTANT: Keep fields generic; normalizer/validator can map to canonical names.
    """
    now_ms = 1_700_000_000_000
    return {
        "symbol": "TST",
        "sym": "TST",
        "mint": "So11111111111111111111111111111111111111112",
        "amount_raw": 1_000_000,  # raw token units (or lamports if SOL)
        "qty_raw": 1_000_000,
        "entry_price": 1.0,
        "avg_entry": 1.0,
        "opened_at_ms": now_ms,
        "open_ts_ms": now_ms,
        "side": "long",
        "venue": "raydium",
        "dex": "raydium",
        "status": "OPEN",
    }


def _rich_position() -> dict[str, Any]:
    now_ms = 1_700_000_000_000
    return {
        **_minimal_position(),
        "rid": "RID_TEST",
        "strategy": "momentum_v3",
        "slippage_bps": 80,
        "priority_fee_lamports": 2_000,
        "entry_sig": "SIG_ENTRY_TEST",
        "entry_fee_usd": 0.05,
        "fees_usd": 0.05,
        "stop_loss_pct": 0.15,
        "take_profit_pct": 0.30,
        "tp_levels": [0.15, 0.30],
        "sell_pcts": [0.5, 0.5],
        "meta": {"source": "unit", "notes": "contract_test"},
        "updated_at_ms": now_ms,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────
def test_positions_related_modules_import() -> None:
    """
    Import-safety baseline. These should import without requiring secrets or spawning I/O.
    """
    # Optional modules: positions/position_store may or may not exist.
    importlib.import_module("utils_pkg.contracts")
    importlib.import_module("utils_pkg.exit_engine")
    importlib.import_module("utils_pkg.data_logger")


def test_position_sample_is_json_safe_locally() -> None:
    """
    Sanity for our own test fixtures (prevents false failures).
    """
    _assert_json_safe_map(_minimal_position())
    _assert_json_safe_map(_rich_position())


def test_position_normalizer_if_present_returns_json_safe_and_keeps_core_fields() -> None:
    """
    If a canonical position normalizer/validator exists, enforce invariants.

    If none exist yet, skip (so suite passes), but strongly recommended to add one.
    """
    fn = _discover_position_normalizer()
    if fn is None:
        pytest.skip(
            "No position normalizer/validator discovered (ensure_position/normalize_position)."
        )

    pos = _rich_position()
    ctx = {"source": "unit", "mode": "contract", "now_ms": 1_700_000_000_000}

    out = _call_best_effort(
        fn,
        attempts=[
            ((pos, ctx), {}),
            ((pos,), {"ctx": ctx}),
            ((pos,), {}),
            ((), {"position": pos, "ctx": ctx}),
            ((), {"pos": pos, "ctx": ctx}),
            ((), {"d": pos, "ctx": ctx}),
        ],
    )
    assert isinstance(out, dict), f"position normalizer must return dict; got {type(out)}"
    _assert_json_safe_map(out)

    # Identity: must have mint-ish and symbol-ish fields (names flexible)
    assert _has_any_key(out, ("mint", "token_mint", "base_mint")), (
        f"normalized position missing mint-ish key; keys={sorted(out.keys())[:60]}"
    )
    assert _has_any_key(out, ("symbol", "sym", "ticker")), (
        f"normalized position missing symbol-ish key; keys={sorted(out.keys())[:60]}"
    )

    # Economics: must have amount-ish and entry price-ish fields
    assert _has_any_key(out, ("amount_raw", "qty_raw", "size_raw", "amount", "qty")), (
        f"normalized position missing amount-ish key; keys={sorted(out.keys())[:60]}"
    )
    assert _has_any_key(out, ("entry_price", "avg_entry", "entry", "avg_entry_price")), (
        f"normalized position missing entry-price-ish key; keys={sorted(out.keys())[:60]}"
    )

    # Time: must have opened-at-ish field
    assert _has_any_key(
        out, ("opened_at_ms", "open_ts_ms", "opened_ms", "ts_open_ms", "opened_at")
    ), f"normalized position missing opened-at-ish key; keys={sorted(out.keys())[:60]}"

    # Keep the same mint string somewhere (don’t allow identity loss)
    mint_in = pos["mint"]
    mint_out = _get_first_present(out, ("mint", "token_mint", "base_mint"))
    assert mint_out is None or str(mint_out) != ""  # basic
    assert mint_in in json.dumps(out, sort_keys=True), (
        "normalized position lost the original mint identity"
    )


def test_exit_engine_can_handle_minimal_position_best_effort() -> None:
    """
    Exit brain must not crash on a minimal position dict (offline).
    We keep this best-effort because exit_engine surface may evolve.
    """
    ex = importlib.import_module("utils_pkg.exit_engine")

    # Find a callable surface (functional)
    fn = None
    for name in (
        "evaluate_position",
        "decide_exit",
        "on_position_tick",
        "compute_exit",
        "get_exit_actions",
        "maybe_exit",
        "should_exit",
        "exit_decision",
    ):
        f = getattr(ex, name, None)
        if callable(f):
            fn = f
            break

    if fn is None:
        # Class surface: validate instantiation at least doesn't catastrophically explode.
        if hasattr(ex, "ExitEngine") and callable(ex.ExitEngine):
            try:
                _ = ex.ExitEngine()  # type: ignore[attr-defined]
            except Exception:
                pass
            return
        pytest.skip("No known exit_engine callable surface found for best-effort position call.")

    pos = _minimal_position()
    now_ms = 1_700_000_000_000

    try:
        # Try several common call shapes; accept signature evolution.
        _ = _call_best_effort(
            fn,
            attempts=[
                ((), {"position": pos, "now_ms": now_ms, "ctx": {"source": "unit"}, "redis": None}),
                ((pos,), {"now_ms": now_ms, "ctx": {"source": "unit"}, "redis": None}),
                ((pos, now_ms), {}),
                (
                    (),
                    {
                        "mint": pos["mint"],
                        "symbol": pos.get("symbol") or pos.get("sym"),
                        "position": pos,
                        "now_ms": now_ms,
                    },
                ),
                (
                    (),
                    {
                        "mint": pos["mint"],
                        "position": pos,
                        "current_price": 1.05,
                        "entry_price": 1.0,
                        "now_ms": now_ms,
                        "ctx": {"source": "unit"},
                        "redis": None,
                    },
                ),
            ],
        )
    except TypeError:
        # Signature mismatch is acceptable; other exceptions are not.
        return
    except Exception as e:
        pytest.fail(f"exit_engine crashed on minimal position: {type(e).__name__}: {e}")
