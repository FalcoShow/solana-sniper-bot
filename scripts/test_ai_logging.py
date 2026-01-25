#!/usr/bin/env python3
from __future__ import annotations

"""
scripts/test_ai_logging.py

God-Tier AI Logging Canary (SAFE-by-default; opt-in writes)
==========================================================

Mission
-------
Prove the AI logging + training data pipeline is wired end-to-end:

  • Postgres DSN resolution works (via utils_pkg.db_dsn if available)
  • DB is reachable
  • Required tables exist (read-only check)
  • (Opt-in) Insert demo entry_candidates rows via utils_pkg.data_logger
  • (Opt-in) Patch decision metadata (JSONB merge/update)
  • (Opt-in) Insert demo outcomes (entry_outcomes) via utils_pkg.ai_db

Safety / P0 rules
-----------------
- Default mode is READ-ONLY (no inserts, no updates).
- Never prints secrets (DSNs/tokens are masked).
- Demo writes are tagged with `_demo_source="scripts.test_ai_logging"` so training exports can exclude them.

Typical usage
-------------
# 1) Safe check (default): connectivity + required tables
python -m scripts.test_ai_logging

# 2) Write a single BUY demo candidate + outcome + posthoc decision patch
python -m scripts.test_ai_logging --write --decision BUY --with-outcome --update-decision

# 3) Insert 5 SKIP candidates (no outcomes), still tagged as demo
python -m scripts.test_ai_logging --write --count 5 --decision SKIP

Exit codes
----------
0 OK
1 Fail (check or write failed)
2 Misconfigured (missing deps / missing DSN)
3 Network/DB unreachable (connection failure)
4 Write test failed (only when --write)

Env (high-signal)
-----------------
PG_ENABLED=1|0                (default 1)

Schema/table checks:
AI_TEST_SCHEMA="public"
AI_TEST_REQUIRED_TABLES="entry_candidates,entry_outcomes"
AI_TEST_TIMEOUT_S=15
AI_TEST_CONNECT_TIMEOUT_S=8

Output:
AI_TEST_JSON=1|0              (default 1)
AI_TEST_JSON_ONLY=1|0
AI_TEST_VERBOSE=1|0

Write gating:
AI_TEST_FORCE_WRITE=1|0       (if PG_ENABLED=0, must set this or pass --force)
"""

import argparse
import asyncio
import contextlib
import inspect
import json
import os
import random
import re
import sys
import time
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

# Optional dotenv (local dev only)
try:
    from dotenv import load_dotenv  # type: ignore

    _HAVE_DOTENV = True
except Exception:
    load_dotenv = None  # type: ignore
    _HAVE_DOTENV = False


# =============================================================================
# bootstrap repo root / dotenv
# =============================================================================
def _bootstrap_repo_root() -> Path:
    here = Path(__file__).resolve()
    # scripts/ -> repo root = parent.parent
    root = here.parent.parent
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    os.environ.setdefault("REPO_ROOT", str(root))
    return root


def _maybe_load_dotenv(root: Path) -> None:
    if os.getenv("AI_TEST_NO_DOTENV", "").strip().lower() in {"1", "true", "yes", "y", "on"}:
        return
    if _HAVE_DOTENV and load_dotenv is not None:
        env_path = root / ".env"
        if env_path.exists():
            with contextlib.suppress(Exception):
                load_dotenv(env_path, override=False)


ROOT = _bootstrap_repo_root()
_maybe_load_dotenv(ROOT)


# =============================================================================
# env helpers
# =============================================================================
_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    s = v if v is not None else d
    s = str(s).strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    return s


def _envb(k: str, d: bool = False) -> bool:
    v = os.getenv(k)
    if v is None:
        return d
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return d


def _envi(k: str, d: int) -> int:
    try:
        return int(float(_envs(k, str(d)) or str(d)))
    except Exception:
        return d


def _envf(k: str, d: float) -> float:
    try:
        return float(_envs(k, str(d)) or str(d))
    except Exception:
        return d


def _split_csv(s: str) -> list[str]:
    return [p.strip() for p in (s or "").replace(";", ",").split(",") if p.strip()]


# =============================================================================
# safe output / sanitization
# =============================================================================
def _ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _safe_print(line: str, *, file) -> None:
    try:
        file.write(line + ("" if line.endswith("\n") else "\n"))
        file.flush()
    except UnicodeEncodeError:
        enc = getattr(file, "encoding", None) or "utf-8"
        b = line.encode(enc, errors="replace")
        with contextlib.suppress(Exception):
            file.buffer.write(b + b"\n")  # type: ignore[attr-defined]
            file.buffer.flush()  # type: ignore[attr-defined]
    except Exception:
        pass


def _hlog(msg: str, *, json_only: bool) -> None:
    if json_only:
        return
    _safe_print(f"[test_ai_logging] {_ts()} {msg}", file=sys.stderr)


_URL_CRED_RE = re.compile(r"(://)([^/@\s]+)@")
_LONG_TOKENISH_RE = re.compile(r"\b[A-Za-z0-9_\-]{28,}\b")


def _sanitize(s: Any) -> str:
    if s is None:
        return ""
    x = str(s)
    x = _URL_CRED_RE.sub(r"\1***@", x)
    # strip any long token-looking substrings
    if _envb("AI_TEST_MASK_LONG_TOKENS", True):
        x = _LONG_TOKENISH_RE.sub("***", x)
    return x


def _mask_dsn(dsn: str) -> str:
    dsn = (dsn or "").strip()
    if not dsn:
        return ""
    try:
        parts = dsn.split("://", 1)
        if len(parts) != 2:
            return _sanitize(dsn)
        scheme, rest = parts
        if "@" not in rest:
            return _sanitize(dsn)
        creds, tail = rest.split("@", 1)
        if ":" in creds:
            user, _ = creds.split(":", 1)
            creds = f"{user}:***"
        else:
            creds = "***"
        return _sanitize(f"{scheme}://{creds}@{tail}")
    except Exception:
        return "***"


# =============================================================================
# optional imports: db_dsn + sqlalchemy
# =============================================================================
try:
    from sqlalchemy import text  # type: ignore
    from sqlalchemy.ext.asyncio import create_async_engine  # type: ignore

    _HAVE_SQLA = True
except Exception:
    text = None  # type: ignore
    create_async_engine = None  # type: ignore
    _HAVE_SQLA = False

try:
    from utils_pkg import db_dsn as dsnmod  # type: ignore

    _HAVE_DSNMOD = True
except Exception:
    dsnmod = None  # type: ignore
    _HAVE_DSNMOD = False

# Core modules under test
try:
    from utils_pkg import ai_db, data_logger  # type: ignore

    _HAVE_CORE = True
except Exception as e:
    ai_db = None  # type: ignore
    data_logger = None  # type: ignore
    _CORE_IMPORT_ERR = _sanitize(f"{type(e).__name__}: {e}")
    _HAVE_CORE = False

# risk_manager is optional; we never fail the canary because it isn't available
try:
    from utils_pkg import risk_manager  # type: ignore

    _HAVE_RISK = True
except Exception:
    risk_manager = None  # type: ignore
    _HAVE_RISK = False


# =============================================================================
# async helpers for "maybe async" funcs
# =============================================================================
async def _maybe_await(x: Any) -> Any:
    if asyncio.iscoroutine(x):
        return await x
    return x


async def _call(fn: Any, *args: Any, **kwargs: Any) -> Any:
    if fn is None:
        raise AttributeError("missing_function")
    if asyncio.iscoroutinefunction(fn):
        return await fn(*args, **kwargs)
    res = fn(*args, **kwargs)
    return await _maybe_await(res)


def _filter_kwargs(fn: Any, kwargs: dict[str, Any]) -> dict[str, Any]:
    """
    Filter kwargs to only those accepted by fn's signature.
    Prevents minor API drift from breaking the canary.
    """
    try:
        sig = inspect.signature(fn)
        allowed = set(sig.parameters.keys())
        # if fn accepts **kwargs, keep everything
        for p in sig.parameters.values():
            if p.kind == inspect.Parameter.VAR_KEYWORD:
                return dict(kwargs)
        return {k: v for k, v in kwargs.items() if k in allowed}
    except Exception:
        return dict(kwargs)


# =============================================================================
# DB plan / read-only schema check
# =============================================================================
@dataclass
class DsnPlan:
    dsn_sa: str
    connect_args: dict[str, Any]
    host: str = ""
    port: int = 5432
    db_hint: str = ""
    ssl_enabled: int = 0
    sslmode: str = ""


def _build_dsn_plan(*, timeout_s: float, app_name: str, connect_timeout_s: float) -> DsnPlan | None:
    """
    Prefer utils_pkg.db_dsn as single source of truth.
    """
    if not _HAVE_DSNMOD or dsnmod is None:
        return None

    pr = dsnmod.pick_pg_dsn_debug(timeout_s=timeout_s, app_name=app_name)
    if not getattr(pr, "dsn_sa", ""):
        return None

    dsn_sa = pr.dsn_sa
    ca = dict(getattr(pr, "connect_args", {}) or {})
    if connect_timeout_s and connect_timeout_s > 0:
        ca.setdefault("timeout", float(connect_timeout_s))

    u = urlparse(dsn_sa)
    host = getattr(pr, "host", "") or (u.hostname or "")
    port = int(u.port or 5432)
    db = (u.path or "").lstrip("/").split("/", 1)[0] if u.path else ""
    sslmode = getattr(pr, "sslmode", "") or ""
    ssl_enabled = 1 if bool(getattr(pr, "ssl_enabled", False)) else 0

    return DsnPlan(
        dsn_sa=dsn_sa,
        connect_args=ca,
        host=host,
        port=port,
        db_hint=db,
        ssl_enabled=ssl_enabled,
        sslmode=sslmode,
    )


async def _check_tables_read_only(
    plan: DsnPlan,
    *,
    schema: str,
    required_tables: Sequence[str],
    timeout_s: float,
) -> tuple[bool, list[str], str]:
    """
    Read-only check: confirm required tables exist in schema.

    Returns: (ok, missing_tables, error)
    """
    if not _HAVE_SQLA or create_async_engine is None or text is None:
        return False, list(required_tables), "missing_deps:sqlalchemy"

    eng = create_async_engine(
        plan.dsn_sa,
        connect_args=dict(plan.connect_args or {}),
        pool_pre_ping=True,
        pool_size=1,
        max_overflow=0,
        pool_timeout=max(3.0, float(timeout_s)),
    )

    try:
        async with eng.connect() as conn:
            # enforce read-only transaction
            await conn.exec_driver_sql("BEGIN READ ONLY")
            try:
                # ping
                row = (await conn.execute(text("SELECT 1"))).first()
                if not row or row[0] != 1:
                    return False, list(required_tables), "select1_failed"

                if not required_tables:
                    return True, [], ""

                q = text(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = :schema
                      AND table_name = ANY(:names)
                    """
                )
                rows = (
                    await conn.execute(q, {"schema": schema, "names": list(required_tables)})
                ).fetchall()
                present = {str(r[0]) for r in rows}
                missing = [t for t in required_tables if t not in present]
                if missing:
                    return False, missing, ""
                return True, [], ""
            finally:
                with contextlib.suppress(Exception):
                    await conn.exec_driver_sql("ROLLBACK")
    except Exception as e:
        return False, list(required_tables), _sanitize(f"{type(e).__name__}: {e}")
    finally:
        with contextlib.suppress(Exception):
            await eng.dispose()


# =============================================================================
# demo payload builders (PF / AI / risk context)
# =============================================================================
def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def build_demo_snapshot(idx: int) -> dict[str, Any]:
    base_liq = 320_000.0 + 16_000.0 * idx
    base_vol5 = 7_100.0 + 150.0 * idx
    base_chg5 = 4.0 + 0.25 * idx
    base_mcap = 900_000.0 + 25_000.0 * idx

    pf_momentum = 0.55 + 0.03 * idx
    pf_final = 0.60 + 0.04 * idx
    pf_risk_adj = pf_final * (0.95 + 0.01 * idx)
    ai_fraud_score = max(0.0, min(0.9, 0.05 * idx))

    return {
        "ts": _now_iso(),
        "liq": base_liq,
        "vol5": base_vol5,
        "buys5": 24 + idx,
        "sells5": 9 + idx // 2,
        "chg5": base_chg5,
        "age_min": 12.3 + 0.5 * idx,
        "mcap": base_mcap,
        "vol1": 1_500.0 + 25.0 * idx,
        "buys1": 10 + idx,
        "sells1": 3 + idx // 2,
        "pf_momentum_score": pf_momentum,
        "pf_final_score": pf_final,
        "pf_risk_adjusted_final": pf_risk_adj,
        "ai_fraud_score": ai_fraud_score,
        "_filter_debug": {
            "momentum_score": pf_momentum,
            "final_score": pf_final,
            "risk_adjusted_final": pf_risk_adj,
            "ai_fraud_score": ai_fraud_score,
            "debug_note": f"dbg idx={idx}",
        },
        "example_flag": True,
        "note": f"demo insert from scripts/test_ai_logging.py idx={idx}",
    }


def build_demo_wallet_signals(idx: int) -> dict[str, Any]:
    return {
        "smart_wallet_score": 0.70 + 0.02 * idx,
        "n_smart_buys_5m": 1 + (idx % 3),
        "smart_flow_tag": "demo_cluster",
        "whale_buy_flag": bool(idx % 2 == 0),
        "whale_buy_score": 0.5 + 0.1 * (idx % 3),
    }


def build_demo_ai_signals(
    idx: int,
    *,
    decision: str | None,
    entry_model_name: str | None,
    policy_version: str | None,
    experiment_tag: str | None,
) -> dict[str, Any]:
    base_score = 0.60 + 0.03 * idx
    if decision and decision.upper() == "SKIP":
        base_score *= 0.7
    fraud_score = 0.10 + 0.02 * idx

    return {
        "entry_score": float(base_score),
        "fraud_score": float(fraud_score),
        "value_estimate": 1.5 + 0.1 * idx,
        "tags": ["demo", "smoke_test", f"idx_{idx}"],
        "model": entry_model_name or _envs("ENTRY_MODEL_NAME", "demo_entry_model"),
        "policy_version": policy_version or _envs("BT_POLICY_VERSION", "v3_demo"),
        "experiment_tag": experiment_tag or _envs("BT_EXPERIMENT_TAG", "local_smoke"),
        "task": "entry_gate_v3",
    }


async def _try_risk_capacity_snapshot() -> dict[str, Any] | None:
    if not _HAVE_RISK or risk_manager is None:
        return None
    fn = getattr(risk_manager, "risk_capacity_snapshot", None)
    if fn is None:
        return None
    try:
        res = await _call(fn)
        return res if isinstance(res, dict) else None
    except Exception:
        return None


async def build_demo_position_context(idx: int, *, risk_profile: str | None) -> dict[str, Any]:
    ctx: dict[str, Any] = {
        "open_positions": 1 + (idx % 3),
        "max_positions": 5,
        "daily_realized_pnl_pct": 1.2,
        "daily_drawdown_pct": -0.4,
        "risk_mode": 1,
        "risk_capacity_score": 0.85,
        "session_id": "local_demo_session",
        "risk_profile": risk_profile or _envs("RISK_PROFILE", "balanced"),
    }
    snap = await _try_risk_capacity_snapshot()
    if snap:
        ctx["_risk_capacity_snapshot"] = snap
    return ctx


# =============================================================================
# steps / summary
# =============================================================================
@dataclass
class Step:
    key: str
    ok: bool
    status: int = 0
    error: str = ""
    details: dict[str, Any] = field(default_factory=dict)
    ms: int = 0


@dataclass
class Summary:
    ts: str
    ok: bool
    mode: str
    schema: str
    required_tables: list[str]
    dsn_host: str = ""
    dsn_port: int = 0
    db_hint: str = ""
    ssl_enabled: int = 0
    sslmode: str = ""
    steps: list[Step] = field(default_factory=list)
    inserted_ids: list[int] = field(default_factory=list)
    last_row_id: int | None = None
    last_row_brief: dict[str, Any] | None = None


# =============================================================================
# core operations
# =============================================================================
def _pg_enabled() -> bool:
    return _envb("PG_ENABLED", True)


def _describe_backend_safe() -> str:
    if not _HAVE_CORE or ai_db is None:
        return "unavailable"
    fn = getattr(ai_db, "describe_backend", None)
    if fn is None:
        return "unknown"
    try:
        x = fn()
        return _sanitize(x)
    except Exception as e:
        return _sanitize(f"err:{type(e).__name__}:{e}")


async def _ping_db() -> tuple[bool, str]:
    if not _HAVE_CORE or ai_db is None:
        return False, "missing_core_imports"
    fn = getattr(ai_db, "ping", None)
    if fn is None:
        # best effort: if no ping, we assume DB might still work; treat as ok-ish
        return True, "no_ping_helper"
    try:
        ok = await _maybe_await(fn())
        return bool(ok), "" if bool(ok) else "ping_returned_false"
    except Exception as e:
        return False, _sanitize(f"{type(e).__name__}: {e}")


async def _log_candidate(
    *,
    src: str,
    mint: str,
    symbol: str,
    regime: str,
    wallet_tag: str,
    idx: int,
    decision: str | None,
    update_decision: bool,
    with_outcome: bool,
    entry_model_name: str | None,
    policy_version: str | None,
    experiment_tag: str | None,
    risk_profile: str | None,
    exit_policy_name: str | None,
    verbose: bool,
) -> tuple[int | None, str | None]:
    """
    Returns: (candidate_id, error)
    """
    if not _HAVE_CORE or data_logger is None:
        return None, "missing_core_imports"

    snap = build_demo_snapshot(idx)
    wallet_signals = build_demo_wallet_signals(idx)
    ai_signals = build_demo_ai_signals(
        idx,
        decision=decision,
        entry_model_name=entry_model_name,
        policy_version=policy_version,
        experiment_tag=experiment_tag,
    )
    position_context = await build_demo_position_context(idx, risk_profile=risk_profile)

    decision_upper = decision.upper() if decision else None

    extra_meta: dict[str, Any] = {
        "RISK_PROFILE": risk_profile,
        "EXIT_POLICY_NAME": exit_policy_name,
        "ENTRY_MODEL_NAME": entry_model_name,
        "ai_policy_version": policy_version,
        "ai_experiment_tag": experiment_tag,
        "_demo_source": "scripts.test_ai_logging",
        "_demo_ts": _now_iso(),
    }
    candidate_key = f"{src}:{regime}:{mint}:idx{idx}"

    try:
        if (
            decision_upper == "SKIP"
            and getattr(data_logger, "log_skip_candidate", None) is not None
        ):
            fn = data_logger.log_skip_candidate
            kwargs = dict(
                src=src,
                mint=mint,
                symbol=symbol,
                snapshot=snap,
                regime=regime,
                wallet_tag=wallet_tag,
                wallet_signals=wallet_signals,
                ai_signals=ai_signals,
                position_context=position_context,
                candidate_key=candidate_key,
                extra_meta=extra_meta,
            )
            row_id = await _maybe_await(fn(**_filter_kwargs(fn, kwargs)))
        else:
            fn = data_logger.log_entry_candidate
            kwargs = dict(
                src=src,
                mint=mint,
                symbol=symbol,
                snapshot=snap,
                regime=regime,
                wallet_tag=wallet_tag,
                wallet_signals=wallet_signals,
                ai_signals=ai_signals,
                position_context=position_context,
                candidate_key=candidate_key,
                extra_meta=extra_meta,
            )
            if decision_upper == "BUY":
                kwargs.update(
                    decision="BUY",
                    decision_score=float(ai_signals.get("entry_score", 0.0)),
                    decision_reason="demo_buy_from_test_ai_logging",
                )
            row_id = await _maybe_await(fn(**_filter_kwargs(fn, kwargs)))

        if row_id is None or int(row_id) <= 0:
            return None, "row_id_not_positive (logging disabled/sampled out/soft-fail)"

        row_id_i = int(row_id)

        if verbose:
            _hlog(
                f"inserted candidate_id={row_id_i} decision={decision_upper or 'NONE'}",
                json_only=False,
            )

        if update_decision:
            fn2 = getattr(data_logger, "log_entry_decision_update", None)
            if fn2 is not None:
                kwargs2 = dict(
                    candidate_id=row_id_i,
                    decision=decision_upper or "BUY",
                    decision_score=float(ai_signals.get("entry_score", 0.0)),
                    decision_reason="posthoc_update_from_test_ai_logging",
                    extra={
                        "update_source": "scripts.test_ai_logging",
                        "policy_version": policy_version,
                        "experiment_tag": experiment_tag,
                        "_demo_source": "scripts.test_ai_logging",
                    },
                )
                with contextlib.suppress(Exception):
                    await _call(fn2, **_filter_kwargs(fn2, kwargs2))

        if with_outcome:
            fn3 = getattr(ai_db, "mark_outcome_for_candidate", None) if ai_db is not None else None
            if fn3 is not None:
                base_pnl_pct = 8.0 + 3.0 * idx
                if decision_upper == "SKIP":
                    base_pnl_pct *= -0.3
                hold_sec = 300 + 60 * idx
                label = "win" if base_pnl_pct >= 0 else "loss"
                kwargs3 = dict(
                    candidate_id=row_id_i,
                    label=label,
                    pnl_pct=float(base_pnl_pct),
                    hold_sec=int(hold_sec),
                    notes="demo outcome from scripts.test_ai_logging",
                    extra={
                        "demo_idx": idx,
                        "from_test_script": True,
                        "decision": decision_upper,
                        "risk_profile": risk_profile,
                        "exit_policy_name": exit_policy_name,
                        "policy_version": policy_version,
                        "experiment_tag": experiment_tag,
                        "_demo_source": "scripts.test_ai_logging",
                    },
                )
                with contextlib.suppress(Exception):
                    await _call(fn3, **_filter_kwargs(fn3, kwargs3))

        return row_id_i, None
    except Exception as e:
        return None, _sanitize(f"{type(e).__name__}: {e}")


async def _fetch_candidate_brief(candidate_id: int) -> dict[str, Any] | None:
    if ai_db is None:
        return None
    fn = getattr(ai_db, "fetch_candidate_by_id", None)
    if fn is None:
        return None
    try:
        row = await _call(fn, candidate_id)
        if not isinstance(row, dict):
            return None

        feats = row.get("features") or {}
        raw = row.get("raw") or {}
        brief = {
            "id": row.get("id"),
            "ts": row.get("ts"),
            "src": row.get("src"),
            "mint": row.get("mint"),
            "symbol": row.get("symbol"),
            "regime": row.get("regime"),
            "wallet_tag": row.get("wallet_tag"),
            "feature_version": row.get("feature_version"),
            "risk_profile": row.get("risk_profile"),
            "exit_policy_name": row.get("exit_policy_name"),
            "features_subset": {
                k: feats.get(k)
                for k in [
                    "pf_momentum_score",
                    "pf_final_score",
                    "pf_risk_adjusted_final",
                    "ai_entry_score",
                    "ai_fraud_score",
                    "ctx_open_positions",
                    "ctx_daily_drawdown_pct",
                    "ctx_daily_realized_pnl_pct",
                    "risk_capacity_score",
                    "smart_wallet_score",
                    "whale_buy_score",
                ]
                if k in feats
            },
            "raw_has": sorted(
                [
                    k
                    for k in ["_decision_meta", "_ai_signals", "_position_context", "meta"]
                    if k in raw
                ]
            ),
            "raw_meta_demo_source": (raw.get("meta") or {}).get("_demo_source")
            if isinstance(raw.get("meta"), dict)
            else None,
        }
        return brief
    except Exception:
        return None


# =============================================================================
# main
# =============================================================================
async def _amain() -> int:
    RC_OK = 0
    RC_FAIL = 1
    RC_MISCONFIG = 2
    RC_NET = 3
    RC_WRITE_FAIL = 4

    ap = argparse.ArgumentParser(description="God-Tier AI logging canary (safe by default).")
    ap.add_argument("--write", action="store_true", help="Opt-in: perform demo inserts/updates.")
    ap.add_argument("--force", action="store_true", help="Allow --write even if PG_ENABLED=0.")
    ap.add_argument(
        "--count", type=int, default=1, help="Number of demo candidates to insert (default 1)."
    )
    ap.add_argument(
        "--decision",
        type=str,
        default="BUY",
        choices=["BUY", "SKIP", "NONE", "buy", "skip", "none"],
    )
    ap.add_argument(
        "--update-decision", action="store_true", help="Patch decision JSONB after insert (opt-in)."
    )
    ap.add_argument(
        "--with-outcome", action="store_true", help="Insert synthetic outcomes (opt-in)."
    )

    ap.add_argument("--src", type=str, default="test_script", help="src tag for entry_candidates.")
    ap.add_argument(
        "--regime", type=str, default="test_regime", help="regime tag for entry_candidates."
    )
    ap.add_argument(
        "--wallet-tag", type=str, default=_envs("RAYDIUM_WALLET_TAG", "test_wallet_cluster")
    )
    ap.add_argument("--mint", type=str, default="TEST_MINT_123")
    ap.add_argument("--symbol", type=str, default="TEST")

    ap.add_argument("--entry-model", type=str, default=_envs("ENTRY_MODEL_NAME", ""))
    ap.add_argument("--policy-version", type=str, default=_envs("BT_POLICY_VERSION", ""))
    ap.add_argument("--experiment-tag", type=str, default=_envs("BT_EXPERIMENT_TAG", ""))

    ap.add_argument("--risk-profile", type=str, default=_envs("RISK_PROFILE", "balanced"))
    ap.add_argument("--exit-policy", type=str, default=_envs("EXIT_POLICY_NAME", "hybrid"))

    ap.add_argument("--schema", type=str, default=_envs("AI_TEST_SCHEMA", "public") or "public")
    ap.add_argument(
        "--required-tables",
        type=str,
        default=_envs("AI_TEST_REQUIRED_TABLES", "entry_candidates,entry_outcomes"),
    )
    ap.add_argument(
        "--skip-table-check", action="store_true", help="Skip read-only schema/table check."
    )

    ap.add_argument("--timeout", type=float, default=float(_envf("AI_TEST_TIMEOUT_S", 15.0)))
    ap.add_argument(
        "--connect-timeout", type=float, default=float(_envf("AI_TEST_CONNECT_TIMEOUT_S", 8.0))
    )

    ap.add_argument("--json", action="store_true", help="Emit JSON summary.")
    ap.add_argument("--json-only", action="store_true", help="Only JSON output (no human logs).")
    ap.add_argument("--verbose", action="store_true", help="Verbose logs.")

    args = ap.parse_args()

    json_only = bool(args.json_only or _envb("AI_TEST_JSON_ONLY", False))
    emit_json = bool(
        args.json
        or args.json_only
        or _envb("AI_TEST_JSON", True)
        or _envb("AI_TEST_JSON_ONLY", False)
    )
    verbose = bool(args.verbose or _envb("AI_TEST_VERBOSE", False))

    if not _HAVE_CORE:
        out = Summary(
            ts=_ts(),
            ok=False,
            mode="read_only",
            schema=args.schema,
            required_tables=_split_csv(args.required_tables),
            steps=[Step(key="imports", ok=False, status=0, error=_CORE_IMPORT_ERR)],
        )
        if emit_json:
            print(json.dumps(asdict(out), ensure_ascii=False, default=str))
        _hlog(f"FAIL imports: {_CORE_IMPORT_ERR}", json_only=json_only)
        return RC_MISCONFIG

    mode = "write" if bool(args.write) else "read_only"
    required_tables = _split_csv(args.required_tables)
    timeout_s = max(4.0, float(args.timeout))
    connect_timeout_s = max(2.0, float(args.connect_timeout))
    app_name = _envs("PG_APP_NAME", "pnlsniper_test_ai_logging") or "pnlsniper_test_ai_logging"

    summ = Summary(
        ts=_ts(),
        ok=False,
        mode=mode,
        schema=(args.schema or "public").strip() or "public",
        required_tables=list(required_tables),
    )

    # Step: backend description
    t0 = time.monotonic()
    backend = _describe_backend_safe()
    summ.steps.append(
        Step(
            key="backend",
            ok=True,
            status=200,
            details={"backend": backend},
            ms=int((time.monotonic() - t0) * 1000),
        )
    )

    # Step: ai_db ping
    t1 = time.monotonic()
    ok_ping, ping_err = await _ping_db()
    summ.steps.append(
        Step(
            key="ping",
            ok=bool(ok_ping),
            status=200 if ok_ping else 500,
            error=ping_err,
            ms=int((time.monotonic() - t1) * 1000),
        )
    )
    if not ok_ping and ping_err != "no_ping_helper":
        summ.ok = False
        if emit_json:
            print(json.dumps(asdict(summ), ensure_ascii=False, default=str))
        _hlog(f"FAIL ping: {ping_err}", json_only=json_only)
        return RC_NET

    # Step: DSN plan + table check (read-only)
    plan = _build_dsn_plan(
        timeout_s=timeout_s, app_name=app_name, connect_timeout_s=connect_timeout_s
    )
    if plan:
        summ.dsn_host = plan.host
        summ.dsn_port = plan.port
        summ.db_hint = plan.db_hint
        summ.ssl_enabled = plan.ssl_enabled
        summ.sslmode = plan.sslmode

    if not args.skip_table_check:
        t2 = time.monotonic()
        if not plan:
            st = Step(
                key="tables",
                ok=False,
                status=0,
                error="missing_dsn_plan (utils_pkg.db_dsn unavailable or no DSN configured)",
                ms=int((time.monotonic() - t2) * 1000),
                details={"have_db_dsn": int(_HAVE_DSNMOD), "have_sqlalchemy": int(_HAVE_SQLA)},
            )
            summ.steps.append(st)
            if emit_json:
                print(json.dumps(asdict(summ), ensure_ascii=False, default=str))
            _hlog("FAIL tables: missing DSN plan", json_only=json_only)
            return RC_MISCONFIG

        ok_tables, missing, terr = await _check_tables_read_only(
            plan, schema=summ.schema, required_tables=required_tables, timeout_s=timeout_s
        )
        st = Step(
            key="tables",
            ok=bool(ok_tables),
            status=200 if ok_tables else 422,
            error=terr,
            details={"missing": missing},
            ms=int((time.monotonic() - t2) * 1000),
        )
        summ.steps.append(st)
        if not ok_tables:
            summ.ok = False
            if emit_json:
                print(json.dumps(asdict(summ), ensure_ascii=False, default=str))
            _hlog(f"FAIL tables missing={missing} err={terr or '-'}", json_only=json_only)
            return RC_FAIL

    # If read-only mode, success
    if not args.write:
        summ.ok = True
        if emit_json:
            print(json.dumps(asdict(summ), ensure_ascii=False, default=str))
        _hlog("OK (read-only)", json_only=json_only)
        return RC_OK

    # Write gating
    if (not _pg_enabled()) and (not args.force) and (not _envb("AI_TEST_FORCE_WRITE", False)):
        msg = "PG_ENABLED=0 (refusing writes). Pass --force or set AI_TEST_FORCE_WRITE=1."
        summ.steps.append(Step(key="write_gate", ok=False, status=403, error=msg))
        if emit_json:
            print(json.dumps(asdict(summ), ensure_ascii=False, default=str))
        _hlog(f"FAIL {msg}", json_only=json_only)
        return RC_MISCONFIG

    # Write steps: insert candidates
    decision = args.decision.upper()
    if decision == "NONE":
        decision_opt: str | None = None
    else:
        decision_opt = decision

    entry_model = args.entry_model.strip() or None
    policy_version = args.policy_version.strip() or None
    experiment_tag = args.experiment_tag.strip() or None
    risk_profile = args.risk_profile.strip() or None
    exit_policy = args.exit_policy.strip() or None

    inserted: list[int] = []
    last_id: int | None = None
    write_errors: list[str] = []

    # jitter seed (deterministic-ish but not identical across runs)
    random.seed(int(time.time()) // 10)

    for i in range(max(1, int(args.count))):
        tW = time.monotonic()
        cid, err = await _log_candidate(
            src=args.src,
            mint=args.mint,
            symbol=args.symbol,
            regime=args.regime,
            wallet_tag=args.wallet_tag,
            idx=i,
            decision=decision_opt,
            update_decision=bool(args.update_decision),
            with_outcome=bool(args.with_outcome),
            entry_model_name=entry_model,
            policy_version=policy_version,
            experiment_tag=experiment_tag,
            risk_profile=risk_profile,
            exit_policy_name=exit_policy,
            verbose=verbose,
        )
        ok = cid is not None
        if cid is not None:
            inserted.append(int(cid))
            last_id = int(cid)
        if err:
            write_errors.append(err)

        summ.steps.append(
            Step(
                key=f"write_row_{i}",
                ok=ok,
                status=200 if ok else 500,
                error=err or "",
                ms=int((time.monotonic() - tW) * 1000),
                details={"candidate_id": cid},
            )
        )

    summ.inserted_ids = inserted
    summ.last_row_id = last_id

    if last_id is not None:
        brief = await _fetch_candidate_brief(last_id)
        summ.last_row_brief = brief

    # Determine overall success
    if write_errors or not inserted:
        summ.ok = False
        if emit_json:
            print(json.dumps(asdict(summ), ensure_ascii=False, default=str))
        _hlog(
            f"FAIL write inserted={len(inserted)} errors={len(write_errors)}", json_only=json_only
        )
        return RC_WRITE_FAIL

    summ.ok = True
    if emit_json:
        print(json.dumps(asdict(summ), ensure_ascii=False, default=str))

    # human summary
    if not json_only:
        dsn_hint = ""
        if plan:
            dsn_hint = f" host={plan.host or '-'} port={plan.port} db={plan.db_hint or '-'} ssl={plan.ssl_enabled} sslmode={plan.sslmode or '-'}"
        _hlog(f"OK (write) inserted={len(inserted)} last_id={last_id}{dsn_hint}", json_only=False)
    return RC_OK


def main() -> None:
    try:
        # Windows asyncio stability
        with contextlib.suppress(Exception):
            if os.name == "nt" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
                asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]
        rc = asyncio.run(_amain())
    except KeyboardInterrupt:
        rc = 130
    except Exception as e:
        _safe_print(
            f"[test_ai_logging] {_ts()} FAIL unhandled {type(e).__name__}: {_sanitize(e)}",
            file=sys.stderr,
        )
        rc = 1
    raise SystemExit(int(rc))


if __name__ == "__main__":
    main()
