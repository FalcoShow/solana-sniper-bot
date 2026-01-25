#!/usr/bin/env python3
from __future__ import annotations

"""
scripts/test_stack.py

Quant-grade end-to-end stack smoke test (V3+++ LOCK-IN).
=======================================================

Goal
----
Answer one question fast:

    "Is my whole sniper stack healthy enough to run live?"

This is an end-to-end *stack* test – not unit tests. It lights up the major
subsystems and reports OK/FAIL/SKIP with a clean summary (and optional JSON).

Design principles
-----------------
- Robust to missing optional modules (risk/regime/ai layers/pumpportal).
- Robust to signature drift (introspects function signatures).
- Never prints secrets (masked env snapshot).
- Network tests are optional and time-bounded.
- Default exit code is 0 even if failures (CI-friendly). Use --strict-exit to
  exit non-zero when anything FAILS.
"""

import argparse
import asyncio
import inspect
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Repo root / .env bootstrap
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:  # pragma: no cover
    from dotenv import find_dotenv, load_dotenv  # type: ignore
except Exception:  # pragma: no cover
    load_dotenv = None  # type: ignore[assignment]
    find_dotenv = None  # type: ignore[assignment]

if load_dotenv is not None and find_dotenv is not None:
    # Do not override already-set env (Render etc.), but pick up local .env if present.
    load_dotenv(find_dotenv(), override=False)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
try:
    from logging_setup import get_logger  # type: ignore
except Exception:  # pragma: no cover
    import logging

    def get_logger(name: str):  # type: ignore
        return logging.getLogger(name)


log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
WSOL_MINT = "So11111111111111111111111111111111111111112"
DEFAULT_USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"


# ---------------------------------------------------------------------------
# Result model
# ---------------------------------------------------------------------------
@dataclass
class TestResult:
    name: str
    status: str  # "ok" | "fail" | "skip"
    duration_sec: float
    error: str | None = None
    details: dict[str, Any] | None = None

    @property
    def ok(self) -> bool:
        return self.status == "ok"


class SkipTest(RuntimeError):
    pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
_TRUE = {"1", "true", "t", "yes", "y", "on"}


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in _TRUE


def _mask_value(val: str | None) -> str:
    """Mask secrets while still showing something useful."""
    if not val:
        return "<unset>"
    s = str(val)
    if len(s) <= 8:
        return "***"
    return f"{s[:3]}***{s[-2:]}"


def _mask_dsn(dsn: str | None) -> str:
    """
    Mask DSNs but keep host/db hints.
    Examples:
      postgres://user:pass@host:5432/db  -> postgres://***@host:5432/db
      postgresql://...                  -> postgresql://***@host/db
    """
    if not dsn:
        return "<unset>"
    s = str(dsn).strip()
    # remove creds between scheme:// and @
    s = re.sub(r"^(?P<sch>[a-zA-Z][a-zA-Z0-9+.-]*://)(?P<creds>[^@/]+)@", r"\g<sch>***@", s)
    # if still contains password-like "password=" (rare in URLs), mask it
    s = re.sub(r"(password=)([^&]+)", r"\1***", s, flags=re.IGNORECASE)
    return s


def _short(x: Any, n: int = 340) -> str:
    try:
        s = str(x)
        return s if len(s) <= n else (s[:n] + "…")
    except Exception:
        return "<?>"


def _filtered_kwargs(fn: Any, kw: dict[str, Any]) -> dict[str, Any]:
    """Return kw filtered to what fn accepts (best-effort)."""
    if not callable(fn):
        return {}
    try:
        sig = inspect.signature(fn)
        params = sig.parameters
        if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()):
            return dict(kw)
        allowed = set(params.keys())
        return {k: v for k, v in kw.items() if k in allowed}
    except Exception:
        return dict(kw)


async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


async def _call(fn: Any, **kw: Any) -> Any:
    if not callable(fn):
        raise RuntimeError("fn_not_callable")
    fkw = _filtered_kwargs(fn, kw)
    out = fn(**fkw)
    return await _maybe_await(out)


def _has_any_env(names: list[str]) -> bool:
    return any(os.getenv(n) for n in names)


def _pg_dsn_present() -> bool:
    # cover your various naming schemes across the repo
    return _has_any_env(
        [
            "POSTGRES_DSN",
            "PG_DSN",
            "PG_DSN_INTERNAL",
            "PG_DSN_EXTERNAL",
            "SNIPER_DB_DSN",
            "PNL_DB_DSN",
            "PNL_PG_DSN",
        ]
    )


def _redis_present() -> bool:
    return _has_any_env(["REDIS_URL", "UPSTASH_REDIS_REST_URL"])


async def _run_test(name: str, coro, results: list[TestResult]) -> None:
    print(f"\n=== [{name}] ===")
    t0 = time.perf_counter()
    try:
        details = await coro()
        dt = time.perf_counter() - t0
        print(f"[{name}] OK in {dt:.2f}s ✅")
        results.append(TestResult(name=name, status="ok", duration_sec=dt, details=details or {}))
    except SkipTest as e:
        dt = time.perf_counter() - t0
        msg = str(e) or "skipped"
        print(f"[{name}] SKIP → {msg}")
        results.append(TestResult(name=name, status="skip", duration_sec=dt, error=msg, details={}))
    except Exception as e:
        dt = time.perf_counter() - t0
        msg = f"{type(e).__name__}: {e}"
        print(f"[{name}] FAIL → {msg}")
        try:
            log.exception("[%s] test failed", name)
        except Exception:
            pass
        results.append(TestResult(name=name, status="fail", duration_sec=dt, error=msg, details={}))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
async def test_env_summary() -> dict[str, Any]:
    """
    Non-secret env snapshot + common misconfig hints.
    """
    # Jupiter / PumpPortal / Wallet / DB / Redis / AI
    interesting: dict[str, Any] = {
        # Mode
        "DRYRUN": os.getenv("DRYRUN", os.getenv("DRY_RUN", "0")),
        "ALLOW_TEST_QUOTE_SCRIPT": os.getenv("ALLOW_TEST_QUOTE_SCRIPT", "<unset>"),
        # RPC hints
        "SOLANA_RPC_HTTP": _mask_value(os.getenv("SOLANA_RPC_HTTP") or os.getenv("RPC_HTTP")),
        "SOLANA_RPC_WS": _mask_value(os.getenv("SOLANA_RPC_WS") or os.getenv("RPC_WS")),
        "RPC_FAILOVER_ENABLED": os.getenv("RPC_FAILOVER_ENABLED", "<env>"),
        # Redis / DB
        "REDIS_URL": _mask_value(os.getenv("REDIS_URL")),
        "POSTGRES_DSN": _mask_dsn(
            os.getenv("POSTGRES_DSN") or os.getenv("PG_DSN") or os.getenv("SNIPER_DB_DSN")
        ),
        # Jupiter
        "JUP_BASE_URL": os.getenv("JUP_BASE_URL", "<default>"),
        "JUP_ALT_BASE_URLS": _mask_value(os.getenv("JUP_ALT_BASE_URLS")),
        "JUP_PRO_KEY": _mask_value(
            os.getenv("JUP_PRO_KEY") or os.getenv("JUP_API_KEY") or os.getenv("JUP_KEY")
        ),
        "JUP_HTTP2": os.getenv("JUP_HTTP2", "0"),
        "PF_SLIPPAGE_BPS": os.getenv("PF_SLIPPAGE_BPS", "<env>"),
        # PumpPortal
        "PUMPPORTAL_WS_URL": os.getenv("PUMPPORTAL_WS_URL", "<default>"),
        "PF_PP_KEY": _mask_value(os.getenv("PF_PP_KEY")),
        # AI
        "AI_ENABLED": os.getenv("AI_ENABLED", os.getenv("AI_ENTRY_ENABLED", "<env>")),
        "OPENAI_API_KEY": _mask_value(os.getenv("OPENAI_API_KEY")),
        # Wallet
        "WALLET_PUBKEY": _mask_value(
            os.getenv("WALLET_PUBKEY")
            or os.getenv("WALLET_PUBLIC_KEY")
            or os.getenv("WALLET_ADDRESS")
        ),
        "KEYPAIR_PATH": _mask_value(os.getenv("KEYPAIR_PATH")),
        "WALLET_PRIVATE_KEY": "<set>"
        if bool(os.getenv("WALLET_PRIVATE_KEY"))
        else "<unset>",  # never print it
    }

    print("Key environment snapshot (masked):")
    for k, v in interesting.items():
        print(f"  {k:24s} = {v}")

    # Helpful hints (non-fatal)
    hints: list[str] = []
    if os.getenv("AI_ENABLED") in _TRUE and not os.getenv("OPENAI_API_KEY"):
        hints.append("AI_ENABLED=1 but OPENAI_API_KEY missing (AI should fall back safely).")
    if not _redis_present():
        hints.append("REDIS_URL not set (core_utils will likely skip or fail Redis init).")
    if not _pg_dsn_present():
        hints.append("POSTGRES_DSN / PG_DSN not set (DB tests will be skipped).")
    if not (os.getenv("WALLET_PRIVATE_KEY") or os.getenv("KEYPAIR_PATH")):
        hints.append("Wallet creds missing (sign/send tests should be skipped; quotes still work).")

    if hints:
        print("\nHints:")
        for h in hints:
            print(" -", h)

    return {"env": interesting, "hints": hints}


async def test_rpc_failover() -> dict[str, Any]:
    """
    Exercise utils_pkg.rpc_failover to verify at least one working RPC.
    """
    try:
        from utils_pkg.rpc_failover import apply_failover_to_env  # type: ignore
    except Exception as e:
        raise SkipTest(f"rpc_failover not importable: {e}") from e

    chosen, notes = await apply_failover_to_env(timeout=4.0)  # type: ignore[misc]
    print(f"Chosen RPC: {chosen}")
    print("Candidates:")
    for url, ok, why in notes:
        status = "OK" if ok else "FAIL"
        print(f"  - [{status}] {url} ({why})")

    return {
        "chosen_rpc": chosen,
        "candidates": [{"url": url, "ok": bool(ok), "why": str(why)} for (url, ok, why) in notes],
    }


async def test_core_utils_and_redis() -> dict[str, Any]:
    """
    Initializes core_utils resources and performs a tiny Redis round-trip.
    """
    try:
        from utils_pkg import core_utils as cu  # type: ignore
    except Exception as e:
        raise SkipTest(f"core_utils not importable: {e}") from e

    print("Initializing core_utils resources...")
    await cu.init_resources()  # type: ignore[attr-defined]

    try:
        r = getattr(cu, "redis", None)
        if r is None:
            raise SkipTest("core_utils.redis is None after init_resources() (REDIS_URL unset?)")

        key = "test_stack:ping"
        val = "ok"
        await r.set(key, val, ex=30)
        got = await r.get(key)
        if isinstance(got, bytes):
            got = got.decode(errors="ignore")

        print(f"Redis ping key={key!r} value={got!r}")
        if got != val:
            raise RuntimeError(f"Redis echo mismatch: expected {val!r}, got {got!r}")
        return {"key": key, "echo": got}
    finally:
        with contextlib.suppress(Exception):  # type: ignore[name-defined]
            await cu.close_resources()  # type: ignore[attr-defined]


async def test_ai_db() -> dict[str, Any]:
    """
    Basic DB health: describe_backend + ping. Skips cleanly when DSN missing.
    """
    if not _pg_dsn_present():
        raise SkipTest("No POSTGRES_DSN/PG_DSN present")

    try:
        from utils_pkg import ai_db  # type: ignore
    except Exception as e:
        raise SkipTest(f"ai_db not importable: {e}") from e

    out: dict[str, Any] = {}

    # describe_backend (optional)
    desc_fn = getattr(ai_db, "describe_backend", None)
    if callable(desc_fn):
        try:
            desc = desc_fn()
            out["backend"] = desc
            print(f"describe_backend(): {desc}")
        except Exception as e:
            out["backend_error"] = str(e)
            print("describe_backend() FAILED:", e)

    # ping (required-ish)
    ping_fn = getattr(ai_db, "ping", None)
    if not callable(ping_fn):
        raise SkipTest("ai_db.ping() not available")

    ok = await _maybe_await(ping_fn())
    out["ping"] = bool(ok)
    print(f"ai_db.ping(): {ok}")
    if not ok:
        raise RuntimeError("ai_db.ping() returned False")

    return out


async def test_risk_and_regime() -> dict[str, Any]:
    """
    Fetch risk/regime snapshots (optional higher-level brains).
    """
    try:
        from utils_pkg import risk_manager  # type: ignore
        from utils_pkg.regime import regime_state  # type: ignore
    except Exception as e:
        raise SkipTest(f"risk/regime not importable: {e}") from e

    limits = await _call(getattr(risk_manager, "current_limits", None))
    snap = await _call(getattr(risk_manager, "risk_snapshot", None))
    cap_snap = await _call(getattr(risk_manager, "risk_capacity_snapshot", None))
    cap_score = await _call(getattr(risk_manager, "risk_capacity_score", None))
    allow_entry = await _call(getattr(risk_manager, "should_allow_new_entry", None))
    feats = await _call(getattr(regime_state, "features", None))

    print(
        "limits: equity_usd={:.2f} ticket_usd={:.2f} max_conc={} pos_pct_cap={:.3f} risk_mode={}".format(
            float((limits or {}).get("equity_usd") or 0.0),
            float((limits or {}).get("ticket_usd") or 0.0),
            int((limits or {}).get("max_concurrent") or 0),
            float((limits or {}).get("pos_pct_cap") or 0.0),
            int((limits or {}).get("risk_mode") or 1),
        )
    )
    print(
        "cap: winrate20={:.3f} rolling_dd_pct={:.3f} daily_pnl_pct={:.3f} risk_capacity={:.3f} allow_entry={}".format(
            float((cap_snap or {}).get("winrate20") or 0.0),
            float((cap_snap or {}).get("rolling_dd_pct") or 0.0),
            float((cap_snap or {}).get("daily_realized_pnl_pct") or 0.0),
            float(cap_score or 0.0),
            bool(allow_entry),
        )
    )
    print(
        "regime: label={label} mult={mult:.3f} trend={trend:+.3f} vol={vol:.3f} liq={liq:.3f} manip={manip:.3f}".format(
            label=(feats or {}).get("regime_label", "normal"),
            mult=float((feats or {}).get("regime_risk_multiplier") or 1.0),
            trend=float(
                (feats or {}).get("regime_trend") or (feats or {}).get("trend_score") or 0.0
            ),
            vol=float(
                (feats or {}).get("regime_volatility")
                or (feats or {}).get("volatility_score")
                or 0.0
            ),
            liq=float(
                (feats or {}).get("regime_liquidity") or (feats or {}).get("liquidity_score") or 0.5
            ),
            manip=float(
                (feats or {}).get("regime_manipulation_risk")
                or (feats or {}).get("manipulation_score")
                or 0.0
            ),
        )
    )

    return {
        "limits": limits or {},
        "risk_snapshot": snap or {},
        "risk_capacity_snapshot": cap_snap or {},
        "risk_capacity_score": cap_score,
        "allow_new_entry": bool(allow_entry),
        "regime_features": feats or {},
    }


async def test_feature_builder_smoke() -> dict[str, Any]:
    """
    Smoke-test FeatureBuilder on a synthetic PF-ish snapshot. Signature-safe.
    """
    try:
        from utils_pkg import feature_builder  # type: ignore
    except Exception as e:
        raise SkipTest(f"feature_builder not importable: {e}") from e

    FB = getattr(feature_builder, "FeatureBuilder", None)
    if FB is None:
        raise SkipTest("feature_builder.FeatureBuilder not found")

    fb = FB()  # type: ignore[call-arg]

    snapshot: dict[str, Any] = {
        "liq_usd": 25_000.0,
        "mcap_usd": 40_000.0,
        "fdv_usd": 60_000.0,
        "token_age_min": 12.0,
        "vol_5m_usd": 5_000.0,
        "buys_5m": 8,
        "sells_5m": 3,
        "txns_5m": 11,
        "price_usd": 0.0042,
        "price_change_5m_pct": 7.5,
        "holders": 120,
        "whales_5m": 2,
        "whale_vol_5m_usd": 2_000.0,
        "smart_wallet_score": 0.7,
        "ai_rug_score_01": 0.08,
        "pf_final_score": 0.62,
        "pf_momentum_score": 0.58,
        "regime_score": 0.2,
        "regime_trend": 0.25,
        "regime_liquidity": 0.6,
        "regime_volatility": 0.4,
        "regime_manipulation_risk": 0.1,
        "scanner_momentum_ok": 1.0,
        "scanner_source_id": 999.0,
    }

    build_fn = getattr(fb, "build", None)
    if not callable(build_fn):
        raise SkipTest("FeatureBuilder.build not callable")

    feats = await _call(
        build_fn,
        symbol="TEST/USDC",
        mint=WSOL_MINT,
        snapshot=snapshot,
        history=None,
        regime=None,
        wallet_signals=None,
        ai_signals=None,
        scanner_source="test_stack",
        risk_context=None,
    )

    if not isinstance(feats, dict) or not feats:
        raise RuntimeError(f"FeatureBuilder.build returned {type(feats)} empty={not bool(feats)}")

    keys_preview = sorted(list(feats.keys()))[:15]
    print(f"FeatureBuilder produced {len(feats)} features. Example keys: {keys_preview}")
    return {"num_features": len(feats), "keys_preview": keys_preview}


async def test_ai_buy_scoring_api() -> dict[str, Any]:
    """
    API sanity for utils_pkg.ai_buy_scoring: Candidate + pass_gate + score_only.
    Signature-safe and robust to field drift.
    """
    try:
        from utils_pkg import ai_buy_scoring  # type: ignore
    except Exception as e:
        raise SkipTest(f"ai_buy_scoring not importable: {e}") from e

    Candidate = getattr(ai_buy_scoring, "Candidate", None)
    pass_gate = getattr(ai_buy_scoring, "pass_gate", None)
    score_only = getattr(ai_buy_scoring, "score_only", None)
    if Candidate is None or pass_gate is None or score_only is None:
        raise SkipTest("ai_buy_scoring missing Candidate/pass_gate/score_only")

    # Candidate accepted fields
    field_names: set[str] | None = None
    if hasattr(Candidate, "__dataclass_fields__"):
        field_names = set(Candidate.__dataclass_fields__.keys())  # type: ignore[attr-defined]
    else:
        try:
            sig = inspect.signature(Candidate)
            field_names = {n for n, p in sig.parameters.items() if n != "self"}
        except Exception:
            field_names = None

    base_kwargs: dict[str, Any] = {
        "src": "test_stack",
        "source": "test_stack",
        "scanner_source": "test_stack",
        "mint": WSOL_MINT,
        "symbol": "TEST",
        "liq_usd": 20_000.0,
        "pool_age_min": 15.0,
        "impact_1k_bps": 10.0,
        "pf_final_score": 0.6,
        "pf_momentum_score": 0.55,
        "ai_rug_score_01": 0.05,
        "ai_fraud_score": 0.05,
        "risk_capacity_score": 0.9,
        "features": {"liq_usd": 20_000.0, "pf_final_score": 0.6},
        "extra": {"from": "test_stack"},
        "ts": time.time(),
    }

    cand_kwargs = {
        k: v for k, v in base_kwargs.items() if (field_names is None or k in field_names)
    }
    # Fill any required-without-default params if present
    try:
        sig = inspect.signature(Candidate)
        for name, param in sig.parameters.items():
            if name == "self" or name in cand_kwargs:
                continue
            if param.default is not inspect._empty:
                continue
            cand_kwargs[name] = None
    except Exception:
        pass

    cand = Candidate(**cand_kwargs)  # type: ignore[call-arg]

    # Defensive: set common numeric-ish Nones to 0.0 to avoid None comparisons
    for attr in dir(cand):
        if attr.startswith("_"):
            continue
        try:
            val = getattr(cand, attr)
        except Exception:
            continue
        if val is not None:
            continue
        if any(
            attr.endswith(suf)
            for suf in ("_pct", "_bps", "_score", "_usd", "_min", "_sec", "_count")
        ):
            with contextlib.suppress(Exception):  # type: ignore[name-defined]
                setattr(cand, attr, 0.0)

    res = pass_gate(cand)  # type: ignore[misc]
    res = await _maybe_await(res)

    ok = True
    score = 0.0
    why = "ok"
    if isinstance(res, tuple | list) and len(res) >= 2:
        ok = bool(res[0])
        score = float(res[1])
        if len(res) >= 3:
            why = str(res[2])
    else:
        ok = bool(getattr(res, "ok", True))
        score = float(getattr(res, "score", 0.0))
        why = str(getattr(res, "reason", "ok"))

    s2_raw = score_only(cand)  # type: ignore[misc]
    s2_raw = await _maybe_await(s2_raw)
    s2 = float(s2_raw[0]) if isinstance(s2_raw, tuple | list) and s2_raw else float(s2_raw or 0.0)

    print(f"ai_buy_scoring.pass_gate → ok={ok}, score={score:.3f}, why={why}")
    print(f"ai_buy_scoring.score_only → score={s2:.3f}")

    return {"ok": bool(ok), "score": float(score), "score_only": float(s2), "why": why}


async def test_data_logger_roundtrip() -> dict[str, Any]:
    """
    Smoke test for data_logger logging + DB write. Skips cleanly if PG missing.
    """
    if not _pg_dsn_present():
        raise SkipTest("No POSTGRES_DSN/PG_DSN present")

    try:
        from utils_pkg import data_logger  # type: ignore
    except Exception as e:
        raise SkipTest(f"data_logger not importable: {e}") from e

    # If data_logger advertises PG_ENABLED=0, skip
    if hasattr(data_logger, "PG_ENABLED") and not bool(data_logger.PG_ENABLED):
        raise SkipTest("data_logger.PG_ENABLED=0")

    # Call signature-safe
    meta = {
        "liq_usd": 30_000.0,
        "vol_5m_usd": 6_000.0,
        "buys_5m": 10,
        "sells_5m": 4,
        "age_min": 9.0,
        "pf_final_score": 0.65,
        "pf_momentum_score": 0.6,
        "ai_rug_score_01": 0.06,
        "scanner_momentum_ok": 1.0,
        "scanner_source_id": 777,
    }
    position_context = {
        "open_positions": 0,
        "max_positions": 5,
        "daily_realized_pnl_pct": 0.0,
        "daily_drawdown_pct": 0.0,
        "risk_mode": 1,
        "risk_capacity_score": 0.9,
        "session_id": "test_stack_session",
    }

    log_cand = getattr(data_logger, "log_raydium_candidate", None)
    if not callable(log_cand):
        raise SkipTest("data_logger.log_raydium_candidate not found")

    print("Logging synthetic candidate…")
    candidate_id = await _call(
        log_cand,
        mint=WSOL_MINT,
        symbol="TEST",
        meta=meta,
        position_context=position_context,
        wallet_tag="test_stack",
        regime_state=None,
        ai_signals={"entry_score": 0.7, "tags": ["test_stack"]},
        candidate_key="test_stack:raydium:WSOL",
        ts=time.time(),
    )

    print(f"candidate_id={candidate_id}")
    if candidate_id is None:
        raise RuntimeError("candidate_id is None")
    cid = int(candidate_id)
    if cid <= 0:
        raise RuntimeError(f"candidate_id non-positive: {cid}")

    log_out = getattr(data_logger, "log_entry_outcome", None)
    if not callable(log_out):
        raise SkipTest("data_logger.log_entry_outcome not found")

    print("Logging synthetic outcome…")
    outcome_id = await _call(
        log_out,
        candidate_id=cid,
        pnl_pct=12.3,
        hold_seconds=3600.0,
        got_stopped=False,
        extra={
            "max_runup_pct": 25.0,
            "max_drawdown_pct": -5.0,
            "exit_policy": "test_stack_exit",
            "exit_trigger": "artificial",
        },
    )

    return {"candidate_id": cid, "outcome_row_id": int(outcome_id or 0)}


async def test_ai_entry_layer() -> dict[str, Any]:
    """
    AI entry diagnostics driven by the real entry logic.
    """
    try:
        from utils_pkg import ai_entry_logic  # type: ignore
    except Exception as e:
        raise SkipTest(f"ai_entry_logic not importable: {e}") from e

    should_enter = getattr(ai_entry_logic, "should_enter", None)
    if not callable(should_enter):
        raise SkipTest("ai_entry_logic.should_enter not found")

    min_liq = float(getattr(ai_entry_logic, "ENTRY_MIN_LIQ_USD", 10_000.0))

    feats: dict[str, Any] = {
        "symbol": "TEST/USDC",
        "mint": WSOL_MINT,
        "liq_usd": min_liq * 1.2,
        "vol_5m_usd": min_liq * 0.15,
        "buys_5m": 10,
        "sells_5m": 4,
        "age_min": 8.0,
        "pf_final_score": 0.65,
        "pf_risk_adjusted_final": 0.62,
        "pf_momentum_score": 0.6,
        "scanner_source": "test_stack",
        "scanner_momentum_ok": 1.0,
        "scanner_source_id": 999.0,
        "regime_trend": 0.3,
        "regime_liquidity": 0.6,
        "regime_manipulation_risk": 0.1,
        "regime_volatility": 0.4,
        "risk_capacity_score": 0.9,
        "ctx_open_positions": 0.0,
        "ctx_max_positions": 5.0,
        "ctx_daily_drawdown_pct": 0.0,
        "ctx_daily_realized_pnl_pct": 0.0,
        "ctx_risk_mode": 1.0,
        "ai_fraud_score": 0.05,
    }

    res = should_enter(dict(feats))
    res = await _maybe_await(res)

    ok, score, reason = False, 0.0, "unknown"
    if isinstance(res, tuple | list) and len(res) >= 3:
        ok, score, reason = bool(res[0]), float(res[1]), str(res[2])
    else:
        ok = bool(getattr(res, "ok", False))
        score = float(getattr(res, "score", 0.0))
        reason = str(getattr(res, "reason", "unknown"))

    print(f"AI entry: ok={ok}, score={score:.3f}, reason={reason}")
    return {"ok": ok, "score": score, "reason": reason}


async def test_jupiter_quote() -> dict[str, Any]:
    """
    Quote SOL→USDC using your real lightning_impl (Metis) layer.
    No wallet required (quote only).
    """
    if not _env_bool("ALLOW_TEST_QUOTE_SCRIPT", False) and not _env_bool(
        "ALLOW_STACK_NETWORK_TESTS", False
    ):
        raise SkipTest(
            "Set ALLOW_TEST_QUOTE_SCRIPT=1 (or ALLOW_STACK_NETWORK_TESTS=1) to run network quote tests"
        )

    try:
        from utils_pkg import lightning_impl as L  # type: ignore
    except Exception as e:
        raise SkipTest(f"lightning_impl not importable: {e}") from e

    usdc = os.getenv("USDC_MINT", DEFAULT_USDC_MINT)
    amount = 1_000_000  # 0.001 SOL in lamports

    print(f"Quoting: SOL({WSOL_MINT}) -> USDC({usdc}) amount={amount} lamports")
    q = await L.get_quote(WSOL_MINT, usdc, amount, slippage_bps=50)  # type: ignore[arg-type]
    if not isinstance(q, dict):
        raise RuntimeError(f"quote returned non-dict: {type(q)}")

    out_amt = q.get("outAmount")
    rp = q.get("routePlan")
    hops = len(rp) if isinstance(rp, list) else None

    print(f"Quote OK. outAmount={out_amt} hops={hops}")
    return {"outAmount": out_amt, "hops": hops, "priceImpactPct": q.get("priceImpactPct")}


async def test_pumpportal_once(timeout: float) -> dict[str, Any]:
    """
    Connect to PumpPortal WS and consume a single event. Skips if client/key missing.
    """
    if not _env_bool("ALLOW_STACK_NETWORK_TESTS", False):
        raise SkipTest("Set ALLOW_STACK_NETWORK_TESTS=1 to run PumpPortal test")

    try:
        from utils_pkg.pumpportal_client import PumpPortalClient  # type: ignore
    except Exception as e:
        raise SkipTest(f"PumpPortalClient not importable: {e}") from e

    # If your PumpPortal requires key, skip when missing
    if not os.getenv("PF_PP_KEY") and _env_bool("PUMPPORTAL_REQUIRE_KEY", True):
        raise SkipTest("PF_PP_KEY missing (PumpPortal test requires key)")

    print(f"Connecting to PumpPortal WS (timeout={timeout}s)...")
    out: dict[str, Any] = {}

    async with PumpPortalClient() as client:  # type: ignore[call-arg]
        agen = client.events()
        msg = await asyncio.wait_for(agen.__anext__(), timeout=timeout)
        kind = msg.get("kind")
        raw = msg.get("raw") or {}
        print(f"kind={kind!r} raw_snippet={json.dumps(raw, default=str)[:220]}…")
        out["kind"] = kind
        out["raw_preview"] = {k: raw.get(k) for k in list(raw.keys())[:6]}
    return out


# ---------------------------------------------------------------------------
# CLI / main
# ---------------------------------------------------------------------------
def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "End-to-end stack smoke test: env, RPC failover, Redis, DB, risk/regime, "
            "FeatureBuilder, AI buy scoring, data logger, AI entry, Jupiter, PumpPortal."
        )
    )
    p.add_argument(
        "--timeout",
        type=float,
        default=float(os.getenv("TEST_STACK_TIMEOUT", "15")),
        help="Timeout seconds for network tests (default: 15 or TEST_STACK_TIMEOUT).",
    )
    p.add_argument("--skip-env", action="store_true", help="Skip env snapshot.")
    p.add_argument("--skip-rpc", action="store_true", help="Skip RPC failover test.")
    p.add_argument("--skip-redis", action="store_true", help="Skip Redis/core_utils test.")
    p.add_argument("--skip-db", action="store_true", help="Skip Postgres/ai_db test.")
    p.add_argument("--skip-risk", action="store_true", help="Skip risk/regime test.")
    p.add_argument("--skip-feature-builder", action="store_true", help="Skip FeatureBuilder test.")
    p.add_argument("--skip-ai-buy", action="store_true", help="Skip ai_buy_scoring API test.")
    p.add_argument(
        "--skip-data-logger", action="store_true", help="Skip data_logger DB roundtrip test."
    )
    p.add_argument("--skip-ai-entry", action="store_true", help="Skip ai_entry_logic test.")
    p.add_argument("--skip-jupiter", action="store_true", help="Skip Jupiter quote test.")
    p.add_argument("--skip-pumpportal", action="store_true", help="Skip PumpPortal WS test.")
    p.add_argument(
        "--skip-network", action="store_true", help="Skip all network tests (Jupiter + PumpPortal)."
    )
    p.add_argument("--json", action="store_true", help="Emit JSON summary at end.")
    p.add_argument(
        "--strict-exit",
        action="store_true",
        help="Exit non-zero if any FAIL (default is always 0).",
    )
    return p


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    results: list[TestResult] = []

    if not args.skip_env:
        await _run_test("ENV", test_env_summary, results)

    if not args.skip_rpc:
        await _run_test("RPC_FAILOVER", test_rpc_failover, results)

    if not args.skip_redis:
        await _run_test("REDIS / CORE_UTILS", test_core_utils_and_redis, results)

    if not args.skip_db:
        await _run_test("AI_DB / POSTGRES", test_ai_db, results)

    if not args.skip_risk:
        await _run_test("RISK / REGIME", test_risk_and_regime, results)

    if not args.skip_feature_builder:
        await _run_test("FEATURE_BUILDER", test_feature_builder_smoke, results)

    if not args.skip_ai_buy:
        await _run_test("AI_BUY_SCORING", test_ai_buy_scoring_api, results)

    if not args.skip_data_logger:
        await _run_test("DATA_LOGGER_ROUNDTRIP", test_data_logger_roundtrip, results)

    if not args.skip_ai_entry:
        await _run_test("AI_ENTRY_LAYER", test_ai_entry_layer, results)

    if not args.skip_network and not args.skip_jupiter:
        await _run_test("JUPITER_QUOTE", test_jupiter_quote, results)

    if not args.skip_network and not args.skip_pumpportal:
        await _run_test("PUMPPORTAL_EVENTS", lambda: test_pumpportal_once(args.timeout), results)

    # Summary
    print("\n==================== SUMMARY ====================")
    ok_count = sum(1 for r in results if r.status == "ok")
    skip_count = sum(1 for r in results if r.status == "skip")
    fail_count = sum(1 for r in results if r.status == "fail")

    for r in results:
        tag = r.status.upper()
        if r.error:
            print(f"[{tag}] {r.name}: {r.error}")
        else:
            print(f"[{tag}] {r.name}")

    print(f"Total: {ok_count} OK, {skip_count} skipped, {fail_count} failed")
    print("================================================\n")

    return {
        "ok": fail_count == 0,
        "ok_count": ok_count,
        "skip_count": skip_count,
        "fail_count": fail_count,
        "results": [
            {
                "name": r.name,
                "status": r.status,
                "duration_sec": r.duration_sec,
                "error": r.error,
                "details": r.details or {},
            }
            for r in results
        ],
    }


def main() -> None:
    args = build_arg_parser().parse_args()
    summary = asyncio.run(_run(args))

    if args.json:
        print(json.dumps(summary, indent=2, default=str))

    if args.strict_exit and not summary.get("ok", False):
        raise SystemExit(1)


if __name__ == "__main__":
    # Small import to avoid failing on platforms where contextlib is trimmed (rare)
    import contextlib

    main()
