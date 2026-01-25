# tests/test_contracts.py
from __future__ import annotations

"""
tests/test_contracts.py — V3+++ Contracts LOCK-IN (P0)
======================================================

Purpose
-------
Lock the *canonical* schema contracts used across the system:

  scanners -> Candidate
  price feeds -> Tick
  entry IQ -> EntrySignal
  router -> RouteQuote
  execution -> OrderIntent / Fill
  ledger -> Position
  exit IQ -> ExitSignal
  risk -> RiskSnapshot
  telemetry -> EventEnvelope

This suite is intentionally:
  - stdlib-only (except pytest)
  - deterministic (no network, no clock assumptions)
  - strict on contract shape (prevents schema drift)
  - tolerant to extra/unknown fields (must go to .extra)

Hard LOCK-IN
------------
utils_pkg.contracts MUST provide:
  - schema_names() -> list[str]          (deterministic order)
  - json_dumps(obj) -> str              (stable JSON, deterministic)
  - stable_id(*parts, prefix="") -> str (deterministic ID helper)
  - redact_any(obj) -> obj              (recursive redaction)

Core dataclasses MUST provide:
  - from_dict(cls, d: Mapping) -> instance
  - to_dict(self) -> dict              (includes schema_version, schema_rev, ts, ts_ms, extra)
  - to_json(self) -> str               (valid JSON)

And ensure_* helpers for each core schema:
  - ensure_candidate(x), ensure_tick(x), ... (accept dict or dataclass)

Notes
-----
- contracts module must remain lightweight and MUST NOT import heavy deps.
- Router dependency checks belong elsewhere; we only assert contracts are clean.
"""

import importlib
import json
import sys
from collections.abc import Callable
from dataclasses import is_dataclass
from typing import Any

import pytest

pytestmark = pytest.mark.contract

# ──────────────────────────────────────────────────────────────────────────────
# Lightweight / forbidden import guard
# ──────────────────────────────────────────────────────────────────────────────

_FORBIDDEN_PREFIXES: tuple[str, ...] = (
    "numpy",
    "pandas",
    "httpx",
    "aiohttp",
    "requests",
    "websockets",
    "solana",
    "solders",
)

_REQUIRED_SCHEMA_NAMES: tuple[str, ...] = (
    "Candidate",
    "Tick",
    "EntrySignal",
    "RouteQuote",
    "OrderIntent",
    "Fill",
    "Position",
    "ExitSignal",
    "RiskSnapshot",
    "EventEnvelope",
)


def _purge_modules_by_prefix(prefixes: tuple[str, ...]) -> None:
    """
    Remove heavy modules from sys.modules so we can accurately detect if contracts imports them.
    Safe because contract tests are explicitly meant to be stdlib-only.
    """
    for name in list(sys.modules.keys()):
        for p in prefixes:
            if name == p or name.startswith(p + "."):
                sys.modules.pop(name, None)
                break


# ──────────────────────────────────────────────────────────────────────────────
# Import helpers (fresh import to test true import-time behavior)
# ──────────────────────────────────────────────────────────────────────────────


def _fresh_import(module_name: str) -> Any:
    """
    Remove module + submodules so we test true import-time behavior.
    Also purge forbidden modules so a contracts import can't hide behind prior imports.
    """
    _purge_modules_by_prefix(_FORBIDDEN_PREFIXES)

    for name in list(sys.modules.keys()):
        if name == module_name or name.startswith(module_name + "."):
            sys.modules.pop(name, None)

    return importlib.import_module(module_name)


def _contracts(*, fresh: bool = True) -> Any:
    return (
        _fresh_import("utils_pkg.contracts")
        if fresh
        else importlib.import_module("utils_pkg.contracts")
    )


def _require_attr(mod: Any, name: str) -> Any:
    assert hasattr(mod, name), f"Missing attribute: utils_pkg.contracts.{name}"
    return getattr(mod, name)


def _require_callable(mod: Any, name: str) -> Callable[..., Any]:
    fn = _require_attr(mod, name)
    assert callable(fn), f"Expected callable: utils_pkg.contracts.{name}"
    return fn


def _as_dict(x: Any) -> dict[str, Any]:
    assert isinstance(x, dict), f"Expected dict, got {type(x)}"
    return x


def _json_loads(s: str) -> dict[str, Any]:
    assert isinstance(s, str) and s, "Expected non-empty JSON string"
    obj = json.loads(s)
    assert isinstance(obj, dict), "Expected JSON object"
    return obj


def _assert_has_meta_fields(d: dict[str, Any]) -> None:
    """
    Meta fields are required for ALL contracts.
    Keep tolerant typing because meta normalization may evolve.
    """
    for k in ("schema_version", "schema_rev", "ts", "ts_ms", "extra"):
        assert k in d, f"to_dict missing required field: {k}"
    assert isinstance(d.get("extra", {}), dict), "extra must be a dict"
    assert isinstance(d["schema_version"], str), "schema_version must be a string"
    assert isinstance(d["schema_rev"], int | str), "schema_rev must be int/str"
    assert isinstance(d["ts"], int | float | str), "ts must be int/float/str"
    assert isinstance(d["ts_ms"], int | float | str), "ts_ms must be int/float/str"


def _assert_unknowns_roundtrip_to_extra(d0: dict[str, Any], d1: dict[str, Any]) -> None:
    """
    Unknown keys MUST be preserved under extra (forward compatibility).
    Only enforce for our deliberate unknown_* keys.
    """
    unknowns = {k: v for k, v in d0.items() if k.startswith("unknown_")}
    if not unknowns:
        return
    extra = d1.get("extra", {})
    assert isinstance(extra, dict), "extra must be dict"
    for k, v in unknowns.items():
        assert extra.get(k) == v, f"Unknown key {k} must land in extra"


# ──────────────────────────────────────────────────────────────────────────────
# Sample payload builders (deliberately include unknown fields)
# ──────────────────────────────────────────────────────────────────────────────


def _mk_candidate_dict() -> dict[str, Any]:
    return {
        "source": "raydium",
        "chain": "solana",
        "mint": "So11111111111111111111111111111111111111112",
        "symbol": "SOL",
        "pool_id": "SomePoolId1111111111111111111111111111111111",
        "quote_mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "dex": "raydium",
        "url": "https://example.invalid",
        "price_usd": 100.0,
        "liq_usd": 12345.0,
        "age_s": 60.0,
        "vol_5m": 555.0,
        "buys_5m": 10,
        "sells_5m": 7,
        "chg_5m_pct": 3.5,
        "confidence": 0.75,
        "unknown_new_field": {"ok": True},
    }


def _mk_tick_dict() -> dict[str, Any]:
    return {
        "chain": "solana",
        "source": "dexscreener",
        "mint": "So11111111111111111111111111111111111111112",
        "price_usd": 100.0,
        "price_sol": 1.0,
        "unknown_tick_field": 123,
    }


def _mk_entry_signal_dict() -> dict[str, Any]:
    return {
        "ok": True,
        "score": 0.88,
        "reasons": ["momo_ok", "liq_ok"],
        "hard_rejects": [],
        "candidate_key": "cand_abc",
        "unknown_entry_field": "x",
    }


def _mk_route_quote_dict() -> dict[str, Any]:
    return {
        "provider": "jupiter",
        "in_mint": "So11111111111111111111111111111111111111112",
        "out_mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "in_amount_raw": 123,
        "out_amount_raw": 456,
        "price_impact_pct": 0.1,
        "fee_amount_raw": 2,
        "route": "route_1",
        "ok": True,
        "reason": "",
        "unknown_quote_field": {"a": 1},
    }


def _mk_order_intent_dict() -> dict[str, Any]:
    return {
        "side": "buy",
        "order_type": "market",
        "mint": "So11111111111111111111111111111111111111112",
        "amount_sol": 0.01,
        "amount_raw": 0,
        "slippage_bps": 50,
        "reason": "test",
        "candidate_key": "cand_abc",
        "unknown_intent_field": True,
    }


def _mk_fill_dict() -> dict[str, Any]:
    return {
        "side": "buy",
        "mint": "So11111111111111111111111111111111111111112",
        "tx_sig": "5hQm9n7nXx1n2n3n4n5n6n7n8n9nAaBbCcDdEeFfGgHh",
        "in_mint": "So11111111111111111111111111111111111111112",
        "out_mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "in_amount_raw": 123,
        "out_amount_raw": 456,
        "avg_price_usd": 100.0,
        "ok": True,
        "reason": "",
        "unknown_fill_field": 999,
    }


def _mk_position_dict() -> dict[str, Any]:
    return {
        "mint": "So11111111111111111111111111111111111111112",
        "symbol": "SOL",
        "size_raw": 1_000_000,
        "decimals": 9,
        "entry_price_usd": 100.0,
        "entry_price_sol": 1.0,
        "cost_sol": 0.01,
        "opened_ts": 1_700_000_000,
        "last_ts": 1_700_000_060,
        "stop_loss_pct": 8.0,
        "take_profit_pct": 25.0,
        "trailing_arm_pct": 10.0,
        "trailing_pct": 4.5,
        "entry_tx": "sig_entry",
        "last_tx": "sig_last",
        "unknown_pos_field": {"x": "y"},
    }


def _mk_exit_signal_dict() -> dict[str, Any]:
    return {
        "should_exit": True,
        "reason": "tp_hit",
        "strength": 0.9,
        "mint": "So11111111111111111111111111111111111111112",
        "pos_id": "pos_abc",
        "target_side": "sell",
        "unknown_exit_field": "ok",
    }


def _mk_risk_snapshot_dict() -> dict[str, Any]:
    return {
        "ok_to_trade": True,
        "reason": "",
        "daily_pnl_sol": 0.0,
        "drawdown_pct": 0.0,
        "open_positions": 1,
        "inflight_orders": 0,
        "max_concurrency": 5,
        "unknown_risk_field": 0,
    }


def _mk_event_envelope_dict() -> dict[str, Any]:
    return {
        "type": "scan_candidate",
        "data": {
            "token": "THIS_SHOULD_REDACT",
            "nested": {"api_key": "ALSO_REDACT"},
            "pubkey": "So11111111111111111111111111111111111111112",
        },
        "meta": {"authorization": "Bearer SECRETSECRETSECRET"},
        "unknown_event_field": 1,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Core contract export checks
# ──────────────────────────────────────────────────────────────────────────────


def test_contracts_imports_and_exports() -> None:
    c = _contracts(fresh=True)

    schema_names = _require_callable(c, "schema_names")
    names = schema_names()
    assert isinstance(names, list)
    assert all(isinstance(x, str) for x in names)

    # Deterministic list (order should not drift)
    assert schema_names() == names

    for req in _REQUIRED_SCHEMA_NAMES:
        assert req in names, f"{req} missing from schema_names()"
        assert hasattr(c, req), f"contracts missing symbol: {req}"


def test_json_dumps_is_stable_and_valid_json() -> None:
    c = _contracts()
    json_dumps = _require_callable(c, "json_dumps")

    a = {"b": 1, "a": 2, "z": {"y": 1, "x": 2}}
    b = {"z": {"x": 2, "y": 1}, "a": 2, "b": 1}

    sa = json_dumps(a)
    sb = json_dumps(b)
    assert sa == sb, "json_dumps must be deterministic (key order stable)"

    parsed = json.loads(sa)
    assert parsed["a"] == 2
    assert parsed["z"]["x"] == 2


def test_stable_id_is_deterministic_and_input_sensitive() -> None:
    c = _contracts()
    stable_id = _require_callable(c, "stable_id")

    x1 = stable_id("raydium", "mint", "pool", 123)
    x2 = stable_id("raydium", "mint", "pool", 123)
    x3 = stable_id("raydium", "mint", "pool", 124)

    assert isinstance(x1, str) and x1
    assert x1 == x2
    assert x1 != x3

    # Should be "id-like" (avoid returning huge JSON blobs)
    assert len(x1) <= 128

    # Prefix support must work (used elsewhere for readability)
    xp = stable_id("a", "b", prefix="fp_")
    assert xp.startswith("fp_")
    assert len(xp) <= 140


def test_redact_any_is_recursive_and_does_not_leak_obvious_secrets() -> None:
    c = _contracts()
    redact_any = _require_callable(c, "redact_any")

    payload = {
        "api_key": "SUPERSECRETKEYSUPERSECRETKEYSUPERSECRET",
        "token": "ALSOSECRETALSOSECRETALSOSECRET",
        "authorization": "Bearer abcdefghijklmnopqrstuvwxyz0123456789",
        "nested": {"secret": "TOPSECRET", "auth": "Bearer ZZZ"},
        "pubkey": "So11111111111111111111111111111111111111112",
    }
    red = redact_any(payload)

    assert red["api_key"] == "***"
    assert red["token"] == "***"
    assert red["authorization"] == "***"
    assert red["nested"]["secret"] == "***"
    assert red["nested"]["auth"] == "***"
    # Pubkeys should generally remain visible
    assert red["pubkey"] == payload["pubkey"]


# ──────────────────────────────────────────────────────────────────────────────
# Dataclass roundtrip contract tests
# ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "cls_name, sample_dict_fn",
    [
        ("Candidate", _mk_candidate_dict),
        ("Tick", _mk_tick_dict),
        ("EntrySignal", _mk_entry_signal_dict),
        ("RouteQuote", _mk_route_quote_dict),
        ("OrderIntent", _mk_order_intent_dict),
        ("Fill", _mk_fill_dict),
        ("Position", _mk_position_dict),
        ("ExitSignal", _mk_exit_signal_dict),
        ("RiskSnapshot", _mk_risk_snapshot_dict),
        ("EventEnvelope", _mk_event_envelope_dict),
    ],
)
def test_contract_roundtrip_to_dict_from_dict(
    cls_name: str, sample_dict_fn: Callable[[], dict[str, Any]]
) -> None:
    c = _contracts()
    cls = _require_attr(c, cls_name)

    d0 = sample_dict_fn()
    assert hasattr(cls, "from_dict"), f"{cls_name}.from_dict missing"

    obj = cls.from_dict(d0)
    assert is_dataclass(obj), f"{cls_name} should be a dataclass"

    assert hasattr(obj, "to_dict"), f"{cls_name}.to_dict missing"
    d1 = _as_dict(obj.to_dict())
    _assert_has_meta_fields(d1)

    # Unknown keys must be preserved under extra (LOCK-IN).
    _assert_unknowns_roundtrip_to_extra(d0, d1)

    # to_json must be valid JSON and contain the same schema_version
    assert hasattr(obj, "to_json"), f"{cls_name}.to_json missing"
    js = obj.to_json()
    parsed = _json_loads(js)
    assert parsed.get("schema_version") == d1.get("schema_version")

    # from_dict(to_dict(obj)) should not crash and should preserve schema_version
    obj2 = cls.from_dict(d1)
    d2 = _as_dict(obj2.to_dict())
    assert d2.get("schema_version") == d1.get("schema_version")


def test_candidate_unknown_keys_go_to_extra() -> None:
    c = _contracts()
    Candidate = _require_attr(c, "Candidate")

    d = _mk_candidate_dict()
    obj = Candidate.from_dict(d)

    assert isinstance(getattr(obj, "extra", None), dict)
    assert obj.extra.get("unknown_new_field") == {"ok": True}


def test_extra_input_merge_precedence_extra_wins_on_conflict() -> None:
    """
    If input mapping includes an explicit 'extra' dict, and the payload also contains
    unknown_* keys, the explicit extra mapping must win for conflicting keys.
    (This is forward-compat safety: producers can control extra canonicalization.)
    """
    c = _contracts()
    Candidate = _require_attr(c, "Candidate")

    d = _mk_candidate_dict()
    d["extra"] = {"unknown_new_field": {"wins": True}}
    d["unknown_new_field"] = {"wins": False}

    obj = Candidate.from_dict(d)
    assert isinstance(obj.extra, dict)
    assert obj.extra.get("unknown_new_field") == {"wins": True}


def test_candidate_key_is_stable_enough_for_dedupe() -> None:
    c = _contracts()
    Candidate = _require_attr(c, "Candidate")

    a = Candidate.from_dict(_mk_candidate_dict())
    b = Candidate.from_dict(_mk_candidate_dict())
    assert hasattr(a, "key") and callable(a.key)
    assert a.key() == b.key()

    d = _mk_candidate_dict()
    d["pool_id"] = "DifferentPool111111111111111111111111111111111"
    c2 = Candidate.from_dict(d)
    assert a.key() != c2.key()


def test_candidate_validate_raises_when_mint_missing() -> None:
    c = _contracts()
    Candidate = _require_attr(c, "Candidate")
    ValidationError = _require_attr(c, "ValidationError")

    obj = Candidate.from_dict({"source": "raydium", "chain": "solana", "mint": ""})
    assert hasattr(obj, "validate") and callable(obj.validate)

    with pytest.raises(ValidationError):
        obj.validate()


def test_position_pnl_pct_math_is_reasonable() -> None:
    c = _contracts()
    Position = _require_attr(c, "Position")

    p = Position.from_dict(_mk_position_dict())
    assert hasattr(p, "pnl_pct") and callable(p.pnl_pct)

    # +10% if price USD 100 -> 110
    assert p.pnl_pct(mark_price_usd=110.0) == pytest.approx(10.0, abs=1e-6)
    # -50% if price SOL 1.0 -> 0.5
    assert p.pnl_pct(mark_price_sol=0.5) == pytest.approx(-50.0, abs=1e-6)
    # if no mark provided -> 0
    assert p.pnl_pct() == 0.0


def test_event_envelope_redacted_redacts_sensitive_fields() -> None:
    c = _contracts()
    EventEnvelope = _require_attr(c, "EventEnvelope")

    evt = EventEnvelope.from_dict(_mk_event_envelope_dict())
    assert hasattr(evt, "redacted") and callable(evt.redacted)

    red = evt.redacted()

    # ensure structure preserved
    assert red["type"] == "scan_candidate"
    assert isinstance(red.get("data"), dict)
    assert isinstance(red.get("meta"), dict)

    # redaction expected
    assert red["data"].get("token") == "***"
    assert red["data"]["nested"].get("api_key") == "***"
    assert red["meta"].get("authorization") == "***"


def test_ensure_helpers_accept_dicts_and_dataclasses() -> None:
    c = _contracts()

    ensure_specs: list[tuple[str, str, Callable[[], dict[str, Any]]]] = [
        ("ensure_candidate", "Candidate", _mk_candidate_dict),
        ("ensure_tick", "Tick", _mk_tick_dict),
        ("ensure_entry_signal", "EntrySignal", _mk_entry_signal_dict),
        ("ensure_route_quote", "RouteQuote", _mk_route_quote_dict),
        ("ensure_order_intent", "OrderIntent", _mk_order_intent_dict),
        ("ensure_fill", "Fill", _mk_fill_dict),
        ("ensure_position", "Position", _mk_position_dict),
        ("ensure_exit_signal", "ExitSignal", _mk_exit_signal_dict),
        ("ensure_risk_snapshot", "RiskSnapshot", _mk_risk_snapshot_dict),
        ("ensure_event_envelope", "EventEnvelope", _mk_event_envelope_dict),
    ]

    for fn_name, cls_name, mk in ensure_specs:
        fn = _require_callable(c, fn_name)
        cls = _require_attr(c, cls_name)

        d = mk()
        obj = fn(d)
        assert is_dataclass(obj), f"{fn_name} should return a dataclass"
        assert isinstance(obj, cls), f"{fn_name} should return {cls_name}"

        # Passing the dataclass should return a dataclass (ideally same instance, but not required)
        obj2 = fn(obj)
        assert is_dataclass(obj2), f"{fn_name} should accept dataclass input too"
        assert isinstance(obj2, cls), f"{fn_name} must preserve type on dataclass input"


# ──────────────────────────────────────────────────────────────────────────────
# Guardrails: contracts must remain lightweight
# ──────────────────────────────────────────────────────────────────────────────


def test_contracts_module_is_lightweight_and_no_heavy_imports() -> None:
    """
    Contracts should not import heavy optional deps (numpy/pandas/http clients/solana SDK, etc).
    This catches accidental dependency creep.

    We purge forbidden modules first so we can detect if contracts pulls them in.
    """
    _purge_modules_by_prefix(_FORBIDDEN_PREFIXES)

    before = set(sys.modules.keys())
    _ = _contracts(fresh=True)
    after = set(sys.modules.keys())

    newly_loaded = sorted(after - before)

    bad = sorted(
        m for m in newly_loaded if any(m == p or m.startswith(p + ".") for p in _FORBIDDEN_PREFIXES)
    )
    assert not bad, f"contracts.py must stay lightweight; heavy deps imported at import-time: {bad}"
