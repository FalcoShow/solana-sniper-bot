#!/usr/bin/env python3
from __future__ import annotations

"""
scripts/test_stack.py

Quant-grade end-to-end stack smoke test (V3+++).
===============================================

High-level goal
---------------
Answer one question fast:

    "Is my whole sniper stack healthy enough to run live?"

This is an end-to-end *stack* test – not just unit tests. It tries to
light up every major subsystem you rely on:

  1) Env snapshot (non-secret knobs / feature flags / DSNs)
  2) RPC failover wiring (utils_pkg.rpc_failover)
  3) Core utils & Redis:
       - utils_pkg.core_utils.init_resources()
       - Redis connectivity + simple key round-trip
  4) Postgres / AI DB:
       - ai_db.describe_backend()
       - ai_db.ping()
  5) Risk / regime / limits:
       - risk_manager.current_limits()
       - risk_manager.risk_snapshot()
       - risk_manager.risk_capacity_snapshot()
       - risk_manager.risk_capacity_score()
       - risk_manager.should_allow_new_entry()
       - regime_state.features()
  6) FeatureBuilder V3+++:
       - utils_pkg.feature_builder.FeatureBuilder.build(...) smoke-build
         from a PF-style snapshot similar to scanners
  7) AI buy scoring layer:
       - utils_pkg.ai_buy_scoring API availability (Candidate / pass_gate / score_only)
         (signature-safe – adapts to whatever Candidate fields you have)
  8) Data logger + DB roundtrip:
       - data_logger.log_raydium_candidate(...)
       - data_logger.log_entry_outcome(...)
  9) AI entry layer:
       - ai_entry_logic.should_enter() on synthetic feature vector
 10) Jupiter / router:
       - Direct HTTP quote via utils_pkg.lightning_impl (BASE, headers, variants)
         using SOL → USDC (no more SOL → SOL 400 error)
 11) PumpPortal WS:
       - Connect and read a single event via PumpPortalClient

Each test is independent and non-fatal: failures are captured and shown
in a summary at the end. The process exits with code 0 even if tests
fail, so you can run this in CI / dashboards without killing the job
just because Jupiter or PumpPortal flaked once.
"""

import argparse
import asyncio
import inspect
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

# ---------------------------------------------------------------------------
# Repo root / .env bootstrap
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:  # pragma: no cover
    from dotenv import load_dotenv, find_dotenv  # type: ignore[import]
except Exception:  # pragma: no cover
    load_dotenv = None  # type: ignore[assignment]

if load_dotenv is not None:
    # Do not override already-set env (Render etc.), but pick up local .env if present.
    load_dotenv(find_dotenv(), override=False)

# Core stack modules
from logging_setup import get_logger  # type: ignore[import]
from utils_pkg import core_utils as cu  # Redis / Notion / etc.
from utils_pkg import ai_db  # DB helpers (describe_backend, ping)
from utils_pkg import lightning_impl as L
from utils_pkg.pumpportal_client import PumpPortalClient
from utils_pkg.rpc_failover import apply_failover_to_env  # RPC health

# Optional higher-level brains (best-effort)
try:  # pragma: no cover
    from utils_pkg import risk_manager  # type: ignore[import]
    from utils_pkg.regime import regime_state  # type: ignore[import]
except Exception:  # pragma: no cover
    risk_manager = None  # type: ignore[assignment]
    regime_state = None  # type: ignore[assignment]

# AI entry logic (required for V3+++, but we handle missing gracefully)
try:  # pragma: no cover
    from utils_pkg import ai_entry_logic  # type: ignore[import]
except Exception:  # pragma: no cover
    ai_entry_logic = None  # type: ignore[assignment]

# Feature builder (V3+++ FeatureVector)
try:  # pragma: no cover
    from utils_pkg import feature_builder  # type: ignore[import]
except Exception:  # pragma: no cover
    feature_builder = None  # type: ignore[assignment]

# AI buy scoring (pre-gate on candidates)
try:  # pragma: no cover
    from utils_pkg import ai_buy_scoring  # type: ignore[import]
except Exception:  # pragma: no cover
    ai_buy_scoring = None  # type: ignore[assignment]

# Data logger (candidates / outcomes / executions / regimes)
try:  # pragma: no cover
    from utils_pkg import data_logger  # type: ignore[import]
except Exception:  # pragma: no cover
    data_logger = None  # type: ignore[assignment]

log = get_logger(__name__)

WSOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = os.getenv(
    "USDC_MINT",
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # mainnet USDC
)


# ════════════════════════════════════════════════════════
# Result struct
# ════════════════════════════════════════════════════════


@dataclass
class TestResult:
    name: str
    ok: bool
    duration_sec: float
    error: Optional[str] = None
    details: Dict[str, Any] | None = None


# ════════════════════════════════════════════════════════
# Helpers
# ════════════════════════════════════════════════════════


def _mask_value(val: Optional[str]) -> str:
    """
    Mask secrets (tokens, DSNs) while still showing something useful.

    Example:
      "abcdef1234567890" → "abc***90"
    """
    if not val:
        return "<unset>"
    s = str(val)
    if len(s) <= 6:
        return "***"
    return f"{s[:3]}***{s[-2:]}"


async def _safe_run(
    *,
    name: str,
    coro_func,
    results: List[TestResult],
) -> None:
    """
    Run a test coroutine, catch/log any exception, and push a TestResult.

    coro_func must be an async function with no positional args:
        async def func() -> Dict[str, Any]:
            ...
    """
    print(f"\n=== [{name}] ===")
    t0 = time.perf_counter()
    try:
        details = await coro_func()
        dt = time.perf_counter() - t0
        print(f"[{name}] OK in {dt:.2f}s ✅")
        results.append(
            TestResult(
                name=name,
                ok=True,
                duration_sec=dt,
                details=details or {},
            )
        )
    except Exception as e:  # noqa: BLE001
        dt = time.perf_counter() - t0
        msg = f"{type(e).__name__}: {e}"
        print(f"[{name}] FAILED → {msg}")
        log.exception("[%s] test failed", name)
        results.append(
            TestResult(
                name=name,
                ok=False,
                duration_sec=dt,
                error=msg,
                details={},
            )
        )


# ════════════════════════════════════════════════════════
# Tests
# ════════════════════════════════════════════════════════


async def test_env_summary() -> Dict[str, Any]:
    """
    Non-secret env snapshot – so you can eyeball misconfig quickly.
    """
    interesting: Dict[str, Any] = {
        # Basic mode
        "DRYRUN": os.getenv("DRYRUN", "0"),
        "NETWORK": os.getenv("NETWORK") or os.getenv("SOLANA_CLUSTER") or "<auto>",
        "PUMP_TRADES": os.getenv("PUMP_SUBSCRIBE_TRADES", "1"),

        # Redis / DB
        "REDIS_URL": _mask_value(os.getenv("REDIS_URL")),
        "SNIPER_DB_DSN": _mask_value(os.getenv("SNIPER_DB_DSN")),
        "PNL_DB_DSN": _mask_value(os.getenv("PNL_DB_DSN") or os.getenv("PNL_PG_DSN")),

        # Jupiter
        "JUP_BASE_URL": os.getenv("JUP_BASE_URL") or getattr(L, "BASE", "<default>"),
        "JUP_AS_LEGACY": os.getenv("JUP_AS_LEGACY", str(getattr(L, "AS_LEGACY", ""))),
        "JUP_SHARED": os.getenv("JUP_USE_SHARED_ACCOUNTS", "1"),
        "JUP_PRO_KEY": _mask_value(os.getenv("JUP_PRO_KEY")),

        # PumpPortal
        "PUMPPORTAL_WS_URL": os.getenv("PUMPPORTAL_WS_URL", "<default>"),
        "PF_PP_KEY": _mask_value(os.getenv("PF_PP_KEY")),

        # Risk / regime flags
        "RISK_MANAGER_ENABLED": os.getenv("RISK_MANAGER_ENABLED", "1"),
        "REGIME_ENABLED": os.getenv("REGIME_ENABLED", "1"),
        "RISK_MODE_OVERRIDE": os.getenv("RISK_MODE_OVERRIDE", "<none>"),

        # Entry / AI flags
        "ENTRY_MOMENTUM_ENABLED": os.getenv("ENTRY_MOMENTUM_ENABLED", "1"),
        "AI_ENTRY_ENABLED": os.getenv(
            "AI_ENTRY_ENABLED", os.getenv("ENTRY_AI_ENABLED", "1")
        ),
        "ENTRY_MODEL": os.getenv("ENTRY_MODEL_NAME")
        or os.getenv("ENTRY_MODEL")
        or "<none>",
        "ENTRY_MIN_INITIAL_BUY_SOL": os.getenv("ENTRY_MIN_INITIAL_BUY_SOL", "<env>"),
        "ENTRY_MIN_SCORE": os.getenv("ENTRY_MIN_SCORE", "<env>"),
        "ENTRY_DYNAMIC_MIN_ENABLED": os.getenv(
            "ENTRY_DYNAMIC_MIN_ENABLED", "<env>"
        ),

        # AI DB / models
        "AI_DB_BACKEND": os.getenv("AI_DB_BACKEND", "<default>"),
        "OPENAI_API_KEY": _mask_value(os.getenv("OPENAI_API_KEY")),

        # Notification hints
        "TELEGRAM_BOT_TOKEN": _mask_value(os.getenv("TELEGRAM_BOT_TOKEN")),
        "TELEGRAM_CHAT_ID": os.getenv("DAILY_PNL_CHAT_ID")
        or os.getenv("TELEGRAM_CHAT_ID")
        or "<none>",

        # Wallet
        "WALLET_PUBKEY": _mask_value(
            os.getenv("WALLET_PUBKEY")
            or os.getenv("WALLET_PUBLIC_KEY")
            or os.getenv("WALLET_ADDRESS")
        ),
    }

    print("Key environment snapshot (masked where appropriate):")
    for k, v in interesting.items():
        print(f"  {k:24s} = {v}")
    return interesting


async def test_rpc_failover() -> Dict[str, Any]:
    """
    Exercise utils_pkg.rpc_failover to verify at least one working RPC
    and that env override works.
    """
    chosen, notes = await apply_failover_to_env(timeout=4.0)
    print(f"Chosen RPC: {chosen}")
    print("Candidates:")
    for url, ok, why in notes:
        status = "OK" if ok else "FAIL"
        print(f"  - [{status}] {url} ({why})")
    return {
        "chosen_rpc": chosen,
        "candidates": [
            {"url": url, "ok": ok, "why": why} for (url, ok, why) in notes
        ],
    }


async def test_core_utils_and_redis() -> Dict[str, Any]:
    print("Initializing core_utils resources (Redis, etc.)...")
    await cu.init_resources()
    try:
        r = cu.redis
        if r is None:
            raise RuntimeError("core_utils.redis is None after init_resources()")

        key = "test_stack:ping"
        val = "ok"
        await r.set(key, val, ex=30)
        got = await r.get(key)
        if isinstance(got, bytes):
            got = got.decode()
        print(f"Redis ping key={key!r} value={got!r}")
        if got != val:
            raise RuntimeError(f"Redis echo mismatch: expected {val!r}, got {got!r}")
        return {"key": key, "echo": got}
    finally:
        await cu.close_resources()


async def test_ai_db() -> Dict[str, Any]:
    """
    Basic DB health: describe_backend + ping.
    """
    out: Dict[str, Any] = {}
    print("Describing backend + pinging DB...")

    try:
        if hasattr(ai_db, "describe_backend"):
            desc = ai_db.describe_backend()
            out["backend"] = desc
            print(f"  describe_backend(): {desc}")
        else:
            print("  describe_backend(): <not available>")
            out["backend"] = "<not_available>"
    except Exception as e:  # noqa: BLE001
        print(f"  describe_backend FAILED: {e}")
        out["backend_error"] = str(e)

    if not hasattr(ai_db, "ping"):
        raise RuntimeError("ai_db.ping() not available")

    ok = ai_db.ping()
    out["ping"] = bool(ok)
    print(f"  ai_db.ping(): {ok}")
    if not ok:
        raise RuntimeError("ai_db.ping() returned False")
    return out


async def test_risk_and_regime() -> Dict[str, Any]:
    """
    Full risk / regime sanity.
    """
    if risk_manager is None or regime_state is None:
        raise RuntimeError("risk_manager / regime_state not importable")

    print("Fetching risk limits + snapshots...")
    limits_task = asyncio.create_task(risk_manager.current_limits())
    snap_task = asyncio.create_task(risk_manager.risk_snapshot())
    cap_snap_task = asyncio.create_task(risk_manager.risk_capacity_snapshot())
    cap_score_task = asyncio.create_task(risk_manager.risk_capacity_score())
    allow_entry_task = asyncio.create_task(risk_manager.should_allow_new_entry())
    regime_feats_task = asyncio.create_task(regime_state.features())

    limits = await limits_task
    snap = await snap_task
    cap_snap = await cap_snap_task
    cap_score = await cap_score_task
    allow_entry = await allow_entry_task
    feats = await regime_feats_task

    print(
        "  limits: equity_usd={:.2f}, ticket_usd={:.2f}, "
        "max_concurrent={}, pos_pct_cap={:.3f}, risk_mode={}".format(
            float(limits.get("equity_usd") or 0.0),
            float(limits.get("ticket_usd") or 0.0),
            int(limits.get("max_concurrent") or 0),
            float(limits.get("pos_pct_cap") or 0.0),
            int(limits.get("risk_mode") or 1),
        )
    )
    print(
        "  cap: winrate20={:.3f}, rolling_dd_pct={:.3f}, "
        "daily_pnl_pct={:.3f}, risk_capacity={:.3f}, allow_entry={}".format(
            float(cap_snap.get("winrate20") or 0.0),
            float(cap_snap.get("rolling_dd_pct") or 0.0),
            float(cap_snap.get("daily_realized_pnl_pct") or 0.0),
            float(cap_score or 0.0),
            bool(allow_entry),
        )
    )

    print(
        "  regime: label={label}, mult={mult:.3f}, trend={trend:+.3f}, "
        "vol={vol:.3f}, liq={liq:.3f}, manip={manip:.3f}".format(
            label=feats.get("regime_label", "normal"),
            mult=float(feats.get("regime_risk_multiplier") or 1.0),
            trend=float(
                feats.get("regime_trend")
                or feats.get("trend_score")
                or 0.0
            ),
            vol=float(
                feats.get("regime_volatility")
                or feats.get("volatility_score")
                or 0.0
            ),
            liq=float(
                feats.get("regime_liquidity")
                or feats.get("liquidity_score")
                or 0.5
            ),
            manip=float(
                feats.get("regime_manipulation_risk")
                or feats.get("manipulation_score")
                or 0.0
            ),
        )
    )

    return {
        "limits": limits,
        "risk_snapshot": snap,
        "risk_capacity_snapshot": cap_snap,
        "risk_capacity_score": cap_score,
        "allow_new_entry": bool(allow_entry),
        "regime_features": feats,
    }


async def test_feature_builder_smoke() -> Dict[str, Any]:
    """
    Smoke-test FeatureBuilder V3+++ on a synthetic PF-style snapshot.
    """
    if feature_builder is None:
        raise RuntimeError("utils_pkg.feature_builder not importable")

    FB = getattr(feature_builder, "FeatureBuilder", None)
    if FB is None:
        raise RuntimeError("feature_builder.FeatureBuilder not found")

    fb = FB()  # type: ignore[call-arg]

    snapshot: Dict[str, Any] = {
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

    feats = fb.build(
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

    if not isinstance(feats, dict):
        raise RuntimeError(f"FeatureBuilder.build returned non-dict type={type(feats)}")
    if not feats:
        raise RuntimeError("FeatureBuilder.build returned empty feature dict")

    keys_preview = sorted(list(feats.keys()))[:15]
    print(
        f"FeatureBuilder produced {len(feats)} features. "
        f"Example keys: {keys_preview}"
    )
    return {"num_features": len(feats), "keys_preview": keys_preview}


async def test_ai_buy_scoring_api() -> Dict[str, Any]:
    """
    API-level sanity for utils_pkg.ai_buy_scoring.

    We don't try to fully simulate a live candidate here (that belongs in
    backtests). We just ensure the key surfaces exist and are callable:

      - Candidate dataclass / type
      - pass_gate()
      - score_only()

    The Candidate dataclass can change over time, so we introspect its
    fields and only pass the ones it actually supports.
    """
    if ai_buy_scoring is None:
        raise RuntimeError("utils_pkg.ai_buy_scoring not importable")

    Candidate = getattr(ai_buy_scoring, "Candidate", None)
    pass_gate = getattr(ai_buy_scoring, "pass_gate", None)
    score_only = getattr(ai_buy_scoring, "score_only", None)

    missing = []
    if Candidate is None:
        missing.append("Candidate")
    if pass_gate is None:
        missing.append("pass_gate")
    if score_only is None:
        missing.append("score_only")

    if missing:
        raise RuntimeError(
            f"ai_buy_scoring missing required symbols: {', '.join(missing)}"
        )

    # Discover what fields Candidate actually accepts.
    field_names: Optional[set[str]] = None
    if hasattr(Candidate, "__dataclass_fields__"):
        field_names = set(Candidate.__dataclass_fields__.keys())  # type: ignore[attr-defined]
    else:
        try:
            sig = inspect.signature(Candidate)
            field_names = {
                name for name, p in sig.parameters.items() if name != "self"
            }
        except Exception:  # noqa: BLE001
            field_names = None

    # Generic field pool – rich but optional. We will filter it.
    base_kwargs: Dict[str, Any] = {
        "src": "test_stack",
        "source": "test_stack",
        "scanner_source": "test_stack",
        "mint": WSOL_MINT,
        "symbol": "TEST",
        "liq_usd": 20_000.0,
        "pf_final_score": 0.6,
        "pf_momentum_score": 0.55,
        "pf_entry_gate_score": 0.6,
        "ai_rug_score": 0.05,
        "ai_rug_score_01": 0.05,
        "ai_fraud_score": 0.05,
        "regime_label": "normal",
        "regime_trend": 0.2,
        "regime_liquidity": 0.6,
        "regime_volatility": 0.4,
        "regime_manipulation_risk": 0.1,
        "risk_capacity_score": 0.9,
        "pool_age_min": 15.0,        # avoid None vs float in hard filters
        "impact_1k_bps": 10.0,       # avoid NoneType > float in _hard_filters
        "features": {"liq_usd": 20_000.0, "pf_final_score": 0.6},
        "extra": {"from": "test_stack"},
        "ts": time.time(),
    }

    if field_names:
        # Only keep keys Candidate actually supports.
        cand_kwargs = {k: v for k, v in base_kwargs.items() if k in field_names}
    else:
        # Fallback minimal subset
        cand_kwargs = {
            "mint": WSOL_MINT,
            "symbol": "TEST",
            "liq_usd": 20_000.0,
            "pool_age_min": 15.0,
            "impact_1k_bps": 10.0,
        }

    # Final safety: ensure any required, non-default params are filled with
    # harmless placeholders.
    try:
        sig = inspect.signature(Candidate)
        for name, param in sig.parameters.items():
            if name == "self":
                continue
            if name in cand_kwargs:
                continue
            if param.default is not inspect._empty:
                continue  # has default
            # required without default → plug in a dumb-safe placeholder
            ann = param.annotation
            value: Any
            if ann is int:
                value = 0
            elif ann is float:
                value = 0.0
            elif ann is bool:
                value = False
            elif ann is str:
                value = ""
            elif ann in (dict, Dict, Dict[str, Any]):
                value = {}
            elif ann in (list, List, List[Any]):
                value = []
            else:
                value = None
            cand_kwargs[name] = value
    except Exception:  # noqa: BLE001
        pass

    cand = Candidate(**cand_kwargs)  # type: ignore[call-arg]

    # EXTRA DEFENSIVE PATCH:
    # Patch numeric-ish None fields → 0.0 so score_only() and hard_filters
    # never hit None * float or None > float.
    numeric_suffixes = (
        "_pct",
        "_bps",
        "_score",
        "_sec",
        "_usd",
        "_age",
        "_age_min",
        "_1h",
        "_5m",
        "_1m",
        "_blocks",
        "_touches",
        "_count",
    )
    numeric_explicit = {
        "impact_1k_bps",
        "pool_age_min",
        "top10_pct",
        "liq_usd",
        "whale_touches_1h",  # explicitly handle whale_touches_1h here
    }

    for attr in dir(cand):
        if attr.startswith("_"):
            continue
        try:
            val = getattr(cand, attr)
        except Exception:
            continue
        if val is not None:
            continue
        if attr in numeric_explicit or any(attr.endswith(suf) for suf in numeric_suffixes):
            try:
                setattr(cand, attr, 0.0)
            except Exception:
                # If setattr fails for some weird reason, ignore gracefully.
                pass

    # pass_gate may be sync or async, and may return:
    #   (ok, score, why) or some struct.
    res = pass_gate(cand)  # type: ignore[call-arg]
    if asyncio.iscoroutine(res):
        res = await res  # type: ignore[assignment]

    ok = True
    score = 0.0
    why = "ok"

    if isinstance(res, (tuple, list)) and len(res) >= 2:
        ok = bool(res[0])
        score = float(res[1])
        if len(res) >= 3:
            why = str(res[2])
    else:
        # Try to be robust if return type changes.
        if hasattr(res, "ok"):
            ok = bool(getattr(res, "ok"))
        if hasattr(res, "score"):
            score = float(getattr(res, "score"))
        if hasattr(res, "reason"):
            why = str(getattr(res, "reason"))

    # score_only may also be sync/async, and might return score or (score, meta)
    s2_raw = score_only(cand)  # type: ignore[call-arg]
    if asyncio.iscoroutine(s2_raw):
        s2_raw = await s2_raw  # type: ignore[assignment]

    if isinstance(s2_raw, (tuple, list)) and s2_raw:
        s2 = float(s2_raw[0])
    else:
        try:
            s2 = float(s2_raw)  # type: ignore[arg-type]
        except Exception:
            s2 = 0.0

    print(f"ai_buy_scoring.pass_gate → ok={ok}, score={score:.3f}, why={why}")
    print(f"ai_buy_scoring.score_only → score={s2:.3f}")

    return {
        "has_Candidate": True,
        "has_pass_gate": True,
        "has_score_only": True,
        "score": float(score),
        "score_only": float(s2),
        "ok": bool(ok),
    }


async def test_data_logger_roundtrip() -> Dict[str, Any]:
    """
    Basic smoke test for utils_pkg.data_logger + ai_db.

    This expects the DB schema to already include entry_outcomes:
      - pnl_pct
      - hold_sec
      - notes_json

    (That's what scripts/migrate_entry_outcomes.py is for.)
    """
    if data_logger is None:
        raise RuntimeError("utils_pkg.data_logger not importable")

    pg_enabled = bool(getattr(data_logger, "PG_ENABLED", True))
    if not pg_enabled:
        print("PG_ENABLED=0 – skipping DB roundtrip, treating as success.")
        return {"skipped": True, "reason": "PG_ENABLED=0"}

    # Synthetic Raydium-style meta
    meta = {
        "liq_usd": 30_000.0,
        "vol_5m_usd": 6_000.0,
        "buys_5m": 10,
        "sells_5m": 4,
        "age_min": 9.0,
        "pf_final_score": 0.65,
        "pf_momentum_score": 0.6,
        "ai_rug_score_01": 0.06,
        "regime_score": 0.25,
        "regime_trend": 0.3,
        "regime_liquidity": 0.6,
        "regime_volatility": 0.4,
        "regime_manipulation_risk": 0.1,
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

    print("Logging synthetic Raydium candidate via data_logger.log_raydium_candidate...")
    candidate_id = data_logger.log_raydium_candidate(
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

    print(f"  got candidate_id={candidate_id}")
    if candidate_id is None or int(candidate_id) <= 0:
        raise RuntimeError(
            f"data_logger.log_raydium_candidate returned non-positive id={candidate_id}"
        )

    print("Logging synthetic outcome via data_logger.log_entry_outcome...")
    outcome_id = data_logger.log_entry_outcome(
        candidate_id=int(candidate_id),
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
    print(f"  outcome row id={outcome_id}")

    return {
        "candidate_id": int(candidate_id),
        "outcome_row_id": int(outcome_id or 0),
    }


async def test_ai_entry_layer() -> Dict[str, Any]:
    """
    AI entry diagnostics driven by the *real* entry logic.
    """
    if ai_entry_logic is None:
        raise RuntimeError("utils_pkg.ai_entry_logic not importable")

    min_liq = float(getattr(ai_entry_logic, "ENTRY_MIN_LIQ_USD", 10_000.0))

    feats: Dict[str, Any] = {
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
        # Regime: mildly positive
        "regime_trend": 0.3,
        "regime_liquidity": 0.6,
        "regime_manipulation_risk": 0.1,
        "regime_volatility": 0.4,
        # Risk context (minimal but sane)
        "risk_capacity_score": 0.9,
        "ctx_open_positions": 0.0,
        "ctx_max_positions": 5.0,
        "ctx_daily_drawdown_pct": 0.0,
        "ctx_daily_realized_pnl_pct": 0.0,
        "ctx_risk_mode": 1.0,
        # Fraud/rug low
        "ai_fraud_score": 0.05,
    }

    ok, score, reason = await ai_entry_logic.should_enter(dict(feats))  # type: ignore[arg-type]

    print(
        f"AI entry layer result: ok={ok}, score={score:.3f}, reason={reason}"
    )

    meta = feats.get("entry_meta") if isinstance(feats, dict) else None
    if isinstance(meta, dict) and meta:
        print("entry_meta keys:", sorted(list(meta.keys()))[:15])
    else:
        print("entry_meta: <none or not attached>")

    return {
        "ok": bool(ok),
        "score": float(score),
        "reason": str(reason),
        "has_entry_meta": bool(isinstance(meta, dict) and meta),
    }


async def test_jupiter_quote(timeout: float) -> Dict[str, Any]:
    """
    Low-notional quote SOL→USDC just to test Jupiter connectivity.
    """
    base = getattr(L, "BASE", os.getenv("JUP_BASE_URL", "https://api.jup.ag"))
    print(f"Using JUP BASE={base!r}, AS_LEGACY={getattr(L, 'AS_LEGACY', None)!r}")
    url = base.rstrip("/") + "/swap/v1/quote"
    params = {
        "inputMint": WSOL_MINT,
        "outputMint": USDC_MINT,
        "amount": 1_000_000,  # 0.001 SOL
        "slippageBps": 50,
        "swapMode": "ExactIn",
        "onlyDirectRoutes": "false",
        "asLegacyTransaction": "false",
    }
    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.get(
            url,
            params=params,
            headers=L._jup_headers(),  # type: ignore[attr-defined]
        )
        print("  status:", r.status_code)
        if r.status_code != 200:
            print("  body:", r.text[:500])
            raise RuntimeError(f"Jupiter quote failed with status {r.status_code}")
        data = r.json()
        print("  top-level keys:", list(data.keys())[:6])
        return {
            "base": base,
            "status": r.status_code,
            "keys": list(data.keys())[:6],
        }


async def test_pumpportal_once(timeout: float) -> Dict[str, Any]:
    """
    Connect to PumpPortal WS and consume a single event.
    """
    print(f"Connecting to PumpPortal WS (timeout={timeout}s)...")
    out: Dict[str, Any] = {}
    async with PumpPortalClient() as client:
        agen = client.events()
        msg = await asyncio.wait_for(agen.__anext__(), timeout=timeout)
        kind = msg.get("kind")
        raw = msg.get("raw") or {}
        print(f"  kind={kind!r}")
        print("  raw snippet:", json.dumps(raw, default=str)[:280], "...")
        out["kind"] = kind
        out["raw_preview"] = {k: raw.get(k) for k in list(raw.keys())[:6]}
    return out


# ════════════════════════════════════════════════════════
# CLI / main
# ════════════════════════════════════════════════════════


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "End-to-end stack smoke test: env, RPC failover, Redis, DB, "
            "risk/regime, FeatureBuilder, AI buy scoring, data logger, "
            "AI entry, Jupiter, PumpPortal."
        )
    )
    p.add_argument(
        "--timeout",
        type=float,
        default=float(os.getenv("TEST_STACK_TIMEOUT", "15")),
        help="Network timeout in seconds for Jupiter / PumpPortal (default: 15 or TEST_STACK_TIMEOUT).",
    )
    p.add_argument(
        "--skip-env",
        action="store_true",
        help="Skip env snapshot.",
    )
    p.add_argument(
        "--skip-rpc",
        action="store_true",
        help="Skip RPC failover test.",
    )
    p.add_argument(
        "--skip-redis",
        action="store_true",
        help="Skip core_utils / Redis smoke test.",
    )
    p.add_argument(
        "--skip-db",
        action="store_true",
        help="Skip Postgres / ai_db smoke test.",
    )
    p.add_argument(
        "--skip-risk",
        action="store_true",
        help="Skip risk_manager / regime snapshot.",
    )
    p.add_argument(
        "--skip-feature-builder",
        action="store_true",
        help="Skip FeatureBuilder smoke test.",
    )
    p.add_argument(
        "--skip-ai-buy",
        action="store_true",
        help="Skip ai_buy_scoring API test.",
    )
    p.add_argument(
        "--skip-data-logger",
        action="store_true",
        help="Skip data_logger roundtrip test.",
    )
    p.add_argument(
        "--skip-ai",
        action="store_true",
        help="Skip AI entry diagnostics test.",
    )
    p.add_argument(
        "--skip-jupiter",
        action="store_true",
        help="Skip Jupiter quote test.",
    )
    p.add_argument(
        "--skip-pumpportal",
        action="store_true",
        help="Skip PumpPortal WS event test.",
    )
    p.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON summary of all tests at the end (in addition to human logs).",
    )
    return p


async def _run(args: argparse.Namespace) -> Dict[str, Any]:
    results: List[TestResult] = []

    if not args.skip_env:
        await _safe_run(name="ENV", coro_func=test_env_summary, results=results)

    if not args.skip_rpc:
        await _safe_run(
            name="RPC_FAILOVER",
            coro_func=test_rpc_failover,
            results=results,
        )

    if not args.skip_redis:
        await _safe_run(
            name="REDIS / CORE_UTILS",
            coro_func=test_core_utils_and_redis,
            results=results,
        )

    if not args.skip_db:
        await _safe_run(
            name="AI_DB / POSTGRES",
            coro_func=test_ai_db,
            results=results,
        )

    if not args.skip_risk:
        await _safe_run(
            name="RISK / REGIME",
            coro_func=test_risk_and_regime,
            results=results,
        )

    if not args.skip_feature_builder:
        await _safe_run(
            name="FEATURE_BUILDER",
            coro_func=test_feature_builder_smoke,
            results=results,
        )

    if not args.skip_ai_buy:
        await _safe_run(
            name="AI_BUY_SCORING",
            coro_func=test_ai_buy_scoring_api,
            results=results,
        )

    if not args.skip_data_logger:
        await _safe_run(
            name="DATA_LOGGER_ROUNDTRIP",
            coro_func=test_data_logger_roundtrip,
            results=results,
        )

    if not args.skip_ai:
        await _safe_run(
            name="AI_ENTRY_LAYER",
            coro_func=test_ai_entry_layer,
            results=results,
        )

    if not args.skip_jupiter:
        await _safe_run(
            name="JUPITER_QUOTE",
            coro_func=lambda: test_jupiter_quote(args.timeout),
            results=results,
        )

    if not args.skip_pumpportal:
        await _safe_run(
            name="PUMPPORTAL_EVENTS",
            coro_func=lambda: test_pumpportal_once(args.timeout),
            results=results,
        )

    print("\n==================== SUMMARY ====================")
    ok_count = sum(1 for r in results if r.ok)
    fail_count = sum(1 for r in results if not r.ok)
    for r in results:
        status = "OK" if r.ok else "FAIL"
        if r.error:
            print(f"[{status}] {r.name}: {r.error}")
        else:
            print(f"[{status}] {r.name}")
    print(f"Total: {ok_count} OK, {fail_count} failed")
    print("================================================\n")

    summary = {
        "ok": fail_count == 0,
        "ok_count": ok_count,
        "fail_count": fail_count,
        "results": [
            {
                "name": r.name,
                "ok": r.ok,
                "duration_sec": r.duration_sec,
                "error": r.error,
                "details": r.details or {},
            }
            for r in results
        ],
    }
    return summary


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    summary = asyncio.run(_run(args))

    if args.json:
        print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
