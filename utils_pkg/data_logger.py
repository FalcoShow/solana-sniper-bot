from __future__ import annotations

"""
utils_pkg/data_logger.py

God-Tier V3+++ Data Logger (Phase 0–7 LOCK-IN)
=============================================

Single logging API for the whole stack.

Schema targets (aligned to your check_schema / ai_db schema):
  - candidate_events   (all candidates + features in ui JSONB)
  - trades             (executions/fills; best-effort idempotent via ON CONFLICT)
  - positions          (upsert)
  - exit_state         (upsert)
  - risk_events        (AI/risk/decision/outcome events)
  - health_heartbeats  (snapshots: regime/equity/risk/reinvestor/heartbeat)

Hard rules
----------
- Safe by default (sampling, payload caps, redaction).
- Non-blocking in async loops (fire-and-forget) by default.
- Deterministic sampling (stable hash).
- DB failures are swallowed if AI_LOG_STRICT=0.
- Prefers utils_pkg.ai_db for DB access if available; otherwise uses asyncpg pool.

Recommended usage
-----------------
- In hot loops: use sync wrappers (fire-and-forget).
- When you NEED an id: call the async variants (a_log_*).
"""

import asyncio
import inspect
import json
import os
import socket
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, Mapping, Optional, Sequence, Tuple

from logging_setup import get_logger

# Optional imports (soft hooks)
try:
    from utils_pkg import feature_builder  # type: ignore
except Exception:  # pragma: no cover
    feature_builder = None  # type: ignore

try:
    from utils_pkg import ai_db as _ai_db  # type: ignore
except Exception:  # pragma: no cover
    _ai_db = None  # type: ignore

log = get_logger(__name__)

# logging safety shim (so .every always exists)
if not hasattr(log, "every"):

    def _every(_key: str, n: int = 60) -> bool:
        return True

    setattr(log, "every", _every)

__all__ = [
    # candidates
    "log_entry_candidate",
    "log_buy_candidate",
    "log_skip_candidate",
    "log_entry_decision_update",
    "log_entry_outcome",
    # async variants (when you need ids)
    "a_log_entry_candidate",
    "a_log_trade_execution",
    "a_log_entry_decision_update",
    "a_log_entry_outcome",
    # per-source wrappers
    "log_raydium_candidate",
    "log_pumpfun_candidate",
    "log_dexscreener_candidate",
    "log_momo_candidate",
    # executions + state
    "log_trade_execution",
    "log_position_upsert",
    "log_exit_state_upsert",
    # events / snapshots
    "log_ai_event",
    "log_regime_snapshot",
    "log_equity_curve_point",
    "log_risk_manager_snapshot",
    "log_reinvestor_snapshot",
    # lifecycle
    "close_logger",
]

# ──────────────────────────────────────────────────────────────────────────────
# Env helpers / flags
# ──────────────────────────────────────────────────────────────────────────────
_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}


def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    return (v if v is not None else d).strip()


def _envb(k: str, d: bool = False) -> bool:
    v = _envs(k, "")
    if v == "":
        return bool(d)
    s = v.lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return bool(d)


def _envi(k: str, d: int) -> int:
    try:
        return int(float(_envs(k, str(d))))
    except Exception:
        return int(d)


def _envf(k: str, d: float) -> float:
    try:
        return float(_envs(k, str(d)))
    except Exception:
        return float(d)


# Master toggles
PG_ENABLED = _envb("PG_ENABLED", True)
AI_LOG_STRICT = _envb("AI_LOG_STRICT", True)

AI_LOG_CANDIDATES = _envb("AI_LOG_CANDIDATES", True)
AI_LOG_OUTCOMES = _envb("AI_LOG_OUTCOMES", True)
AI_LOG_SKIPS = _envb("AI_LOG_SKIPS", True)

AI_LOG_EXECUTIONS = _envb("AI_LOG_EXECUTIONS", True)
AI_LOG_EVENTS = _envb("AI_LOG_EVENTS", True)
AI_LOG_SNAPSHOTS = _envb("AI_LOG_SNAPSHOTS", True)

# Sampling
AI_LOG_SAMPLE_RATE = _envf("AI_LOG_SAMPLE_RATE", 1.0)
AI_LOG_SAMPLE_RATE_SKIPS = _envf("AI_LOG_SAMPLE_RATE_SKIPS", AI_LOG_SAMPLE_RATE)
AI_LOG_EXEC_SAMPLE_RATE = _envf("AI_LOG_EXEC_SAMPLE_RATE", 1.0)
AI_LOG_OUTCOME_SAMPLE_RATE = _envf("AI_LOG_OUTCOME_SAMPLE_RATE", 1.0)
AI_LOG_EVENT_SAMPLE_RATE = _envf("AI_LOG_EVENT_SAMPLE_RATE", 1.0)
AI_LOG_SNAPSHOT_SAMPLE_RATE = _envf("AI_LOG_SNAPSHOT_SAMPLE_RATE", 1.0)

# Filters
AI_LOG_SRC_FILTER = _envs("AI_LOG_SRC_FILTER", "")
AI_LOG_REGIME_FILTER = _envs("AI_LOG_REGIME_FILTER", "")

_SRC_ALLOW: Optional[set[str]] = None
_REGIME_ALLOW: Optional[set[str]] = None
if AI_LOG_SRC_FILTER:
    _SRC_ALLOW = {s.strip() for s in AI_LOG_SRC_FILTER.split(",") if s.strip()}
if AI_LOG_REGIME_FILTER:
    _REGIME_ALLOW = {s.strip() for s in AI_LOG_REGIME_FILTER.split(",") if s.strip()}

# Payload caps (JSONB size control)
AI_LOG_MAX_UI_BYTES = _envi("AI_LOG_MAX_UI_BYTES", 60_000)
AI_LOG_MAX_FEATURE_KEYS = _envi("AI_LOG_MAX_FEATURE_KEYS", 450)
AI_LOG_HISTORY_IN_UI = _envb("AI_LOG_HISTORY_IN_UI", False)
AI_LOG_HISTORY_MAX_LEN = _envi("AI_LOG_HISTORY_MAX_LEN", 80)

# Basic liquidity/volume hard filters (logger-level)
AI_LOG_MIN_LIQ_USD = _envf("AI_LOG_MIN_LIQ_USD", 0.0)
AI_LOG_MIN_VOL5_USD = _envf("AI_LOG_MIN_VOL5_USD", 0.0)

# Metadata tags (helpful for offline analytics)
AI_POLICY_VERSION = _envs("AI_POLICY_VERSION", "")
AI_EXPERIMENT_TAG = _envs("AI_EXPERIMENT_TAG", "")
AI_CODE_VERSION = _envs("AI_CODE_VERSION", "")
AI_STACK_PROFILE = _envs("AI_STACK_PROFILE", "")

SERVICE_NAME = _envs("SERVICE_NAME", "sniper")
SERVICE_VERSION = _envs("SERVICE_VERSION", _envs("VERSION", "v1"))

# DB tuning (fallback pool path only)
AI_PG_POOL_MIN = _envi("AI_PG_POOL_MIN", 1)
AI_PG_POOL_MAX = _envi("AI_PG_POOL_MAX", 3)
AI_PG_STATEMENT_TIMEOUT_MS = _envi("AI_PG_STATEMENT_TIMEOUT_MS", 15_000)

# Circuit breaker
AI_LOG_DISABLE_AFTER_FAILS = _envi("AI_LOG_DISABLE_AFTER_FAILS", 25)

# ──────────────────────────────────────────────────────────────────────────────
# FeatureBuilder (soft hook)
# ──────────────────────────────────────────────────────────────────────────────
_FB = None
if feature_builder is not None:
    try:
        _FB = feature_builder.FeatureBuilder()
    except Exception:  # pragma: no cover
        log.exception("data_logger: FeatureBuilder init failed; continuing without it")
        _FB = None

# ──────────────────────────────────────────────────────────────────────────────
# DB backend (prefer ai_db; fallback asyncpg pool)
# ──────────────────────────────────────────────────────────────────────────────
try:
    import asyncpg  # type: ignore
except Exception:  # pragma: no cover
    asyncpg = None  # type: ignore

_POOL = None
_POOL_LOCK = asyncio.Lock()
_DB_FAILS = 0
_DB_DISABLED = False


def _ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _stable_hash01(s: str) -> float:
    h = 0
    for ch in s:
        h = (h * 31 + ord(ch)) & 0xFFFFFFFF
    return (h % 10_000) / 10_000.0


def _should_sample(key: str, rate: float) -> bool:
    if rate >= 0.999:
        return True
    if rate <= 0.0:
        return False
    return _stable_hash01(key) < rate


def _src_regime_allowed(src: str, regime: Optional[str]) -> bool:
    if _SRC_ALLOW is not None and src not in _SRC_ALLOW:
        return False
    if _REGIME_ALLOW is not None and regime is not None and regime not in _REGIME_ALLOW:
        return False
    return True


def _redact_url(dsn: str) -> str:
    if not dsn:
        return ""
    try:
        import re as _re

        return _re.sub(r"(://)[^/@]+@", r"\1***@", dsn.strip())
    except Exception:
        return dsn


def _host_from_dsn(dsn: str) -> str:
    after_at = dsn.split("@", 1)[-1]
    hostport = after_at.split("/", 1)[0]
    return hostport.split(":", 1)[0].strip()


def _resolves(host: str) -> bool:
    try:
        socket.getaddrinfo(host, None)
        return True
    except OSError:
        return False


def _has_param(dsn: str, key: str) -> bool:
    if "?" not in dsn:
        return False
    q = dsn.split("?", 1)[1]
    for part in q.split("&"):
        if not part.strip():
            continue
        k = part.split("=", 1)[0].strip().lower()
        if k == key.lower():
            return True
    return False


def _ensure_sslmode_require(dsn: str) -> str:
    dsn = dsn.replace("ssl=true", "sslmode=require")
    if _has_param(dsn, "sslmode"):
        return dsn
    return dsn + ("&sslmode=require" if "?" in dsn else "?sslmode=require")


def _normalize_asyncpg_dsn(raw: str) -> str:
    v = (raw or "").strip()
    if v.startswith("postgresql+asyncpg://"):
        v = "postgresql://" + v[len("postgresql+asyncpg://") :]
    if v.startswith("postgres://"):
        v = "postgresql://" + v[len("postgres://") :]
    host = _host_from_dsn(v)
    if host.endswith(".render.com") or "postgres.render.com" in host:
        v = _ensure_sslmode_require(v)
    return v


def _candidate_dsns() -> list[Tuple[str, str]]:
    keys = ["PNL_DSN", "PG_DSN", "DATABASE_URL", "PG_DSN_INTERNAL", "PG_DSN_EXTERNAL"]
    out: list[Tuple[str, str]] = []
    for k in keys:
        v = _envs(k, "")
        if v:
            out.append((k, v))
    return out


def _pick_best_dsn() -> Tuple[str, str]:
    cands = _candidate_dsns()
    if not cands:
        return "", ""
    resolved: list[Tuple[str, str]] = []
    for label, raw in cands:
        dsn = _normalize_asyncpg_dsn(raw)
        host = _host_from_dsn(dsn)
        if host and _resolves(host):
            resolved.append((label, dsn))
    if resolved:
        return resolved[0][0], resolved[0][1]
    label, raw = cands[0]
    return label, _normalize_asyncpg_dsn(raw)


def _call_with_accepted_kwargs(fn: Any, **kwargs: Any) -> Any:
    sig = inspect.signature(fn)
    params = sig.parameters
    if any(p.kind == p.VAR_KEYWORD for p in params.values()):
        return fn(**kwargs)
    filtered = {k: v for k, v in kwargs.items() if k in params}
    return fn(**filtered)


async def _ai_db_conn() -> Any:
    """
    Best-effort adapter to ai_db.
    We support common helper shapes without assuming an exact signature.
    """
    if _ai_db is None:
        raise RuntimeError("ai_db not available")

    # pool getter
    for name in ("get_pool_async", "get_pool", "pool", "get_asyncpg_pool"):
        if hasattr(_ai_db, name):
            obj = getattr(_ai_db, name)
            pool = await obj() if asyncio.iscoroutinefunction(obj) else obj()
            if pool is not None:
                return pool.acquire()

    # connection getter
    for name in ("connect", "get_conn", "acquire", "conn"):
        if hasattr(_ai_db, name):
            obj = getattr(_ai_db, name)
            cm = await obj() if asyncio.iscoroutinefunction(obj) else obj()
            # if it's already a context manager, return it
            return cm

    raise RuntimeError("ai_db present but no usable connection/pool helper found")


async def _fallback_get_pool() -> Any:
    global _POOL
    if _POOL is not None:
        return _POOL
    if asyncpg is None:
        raise RuntimeError("asyncpg not installed (required for data_logger DB writes)")
    async with _POOL_LOCK:
        if _POOL is not None:
            return _POOL

        label, dsn = _pick_best_dsn()
        if not dsn:
            raise RuntimeError("No DSN found (set PNL_DSN or PG_DSN or DATABASE_URL)")

        log.debug("data_logger: creating asyncpg pool dsn_source=%s dsn=%s", label, _redact_url(dsn))

        async def _init(conn: Any) -> None:
            try:
                await conn.execute(f"SET statement_timeout = {int(AI_PG_STATEMENT_TIMEOUT_MS)};")
            except Exception:
                pass

        _POOL = await asyncpg.create_pool(
            dsn=dsn,
            min_size=max(1, int(AI_PG_POOL_MIN)),
            max_size=max(1, int(AI_PG_POOL_MAX)),
            init=_init,
        )
        return _POOL


@dataclass(frozen=True)
class _DB:
    """
    Internal db executor facade.
    """

    use_ai_db: bool

    async def execute(self, sql: str, *args: Any) -> str:
        if self.use_ai_db:
            # Try ai_db helper execute first
            if hasattr(_ai_db, "execute"):
                fn = getattr(_ai_db, "execute")
                return await _call_with_accepted_kwargs(fn, sql=sql, args=args)
            # Otherwise acquire connection context manager and execute
            async with (await _ai_db_conn()) as conn:
                return await conn.execute(sql, *args)

        p = await _fallback_get_pool()
        async with p.acquire() as conn:
            return await conn.execute(sql, *args)

    async def fetchrow(self, sql: str, *args: Any) -> Any:
        if self.use_ai_db:
            if hasattr(_ai_db, "fetchrow"):
                fn = getattr(_ai_db, "fetchrow")
                return await _call_with_accepted_kwargs(fn, sql=sql, args=args)
            async with (await _ai_db_conn()) as conn:
                return await conn.fetchrow(sql, *args)

        p = await _fallback_get_pool()
        async with p.acquire() as conn:
            return await conn.fetchrow(sql, *args)


_DBX = _DB(use_ai_db=(_ai_db is not None))


def _db_disabled() -> bool:
    return (not PG_ENABLED) or _DB_DISABLED


def _bump_db_fail(e: Exception) -> None:
    global _DB_FAILS, _DB_DISABLED
    _DB_FAILS += 1
    if log.every("data_logger_db_fail", 30):
        log.warning("data_logger: db error (fails=%s): %s", _DB_FAILS, repr(e))
    if _DB_FAILS >= max(1, AI_LOG_DISABLE_AFTER_FAILS):
        _DB_DISABLED = True
        log.error("data_logger: disabling DB logging after too many failures (%s)", _DB_FAILS)


async def close_logger() -> None:
    """
    Closes fallback asyncpg pool. (ai_db lifecycle is managed by ai_db.)
    """
    global _POOL
    p = _POOL
    _POOL = None
    if p is not None:
        try:
            await p.close()
        except Exception:
            pass


# ──────────────────────────────────────────────────────────────────────────────
# Payload safety: redaction + size caps
# ──────────────────────────────────────────────────────────────────────────────
_SENSITIVE_KEYS = {
    "WALLET_PRIVATE_KEY",
    "WALLET_JSON",
    "PRIVATE_KEY",
    "SECRET",
    "SEED",
    "MNEMONIC",
    "KEYPAIR",
    "API_KEY",
    "JUP_API_KEY",
    "JUP_PRO_KEY",
    "OPENAI_API_KEY",
    "DISCORD_TOKEN",
    "TELEGRAM_BOT_TOKEN",
    "NOTION_TOKEN",
}


def _redact_obj(o: Any) -> Any:
    if isinstance(o, dict):
        out: Dict[str, Any] = {}
        for k, v in o.items():
            ks = str(k)
            if any(tok in ks.upper() for tok in _SENSITIVE_KEYS):
                out[ks] = "***REDACTED***"
            else:
                out[ks] = _redact_obj(v)
        return out
    if isinstance(o, (list, tuple)):
        return [_redact_obj(x) for x in o[:500]]
    return o


def _json_bytes(o: Any) -> int:
    try:
        return len(json.dumps(o, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
    except Exception:
        return 10**9


def _cap_dict_keys(d: Dict[str, Any], max_keys: int) -> Dict[str, Any]:
    if len(d) <= max_keys:
        return d
    keys = sorted(d.keys())
    keep = keys[:max_keys]
    return {k: d[k] for k in keep}


def _cap_ui(ui: Dict[str, Any]) -> Dict[str, Any]:
    ui = _redact_obj(ui)

    feats = ui.get("features")
    if isinstance(feats, dict):
        ui["features"] = _cap_dict_keys(feats, AI_LOG_MAX_FEATURE_KEYS)

    if _json_bytes(ui) <= AI_LOG_MAX_UI_BYTES:
        return ui

    # Drop heavy optional blocks in a deterministic order
    for heavy in ("history", "raw", "wallet_signals", "ai_signals", "position_context", "gate_detail", "extra_raw"):
        if heavy in ui:
            ui.pop(heavy, None)
            if _json_bytes(ui) <= AI_LOG_MAX_UI_BYTES:
                return ui

    # Final minimal payload
    if _json_bytes(ui) > AI_LOG_MAX_UI_BYTES:
        ui = {
            "ts": ui.get("ts"),
            "event_uuid": ui.get("event_uuid"),
            "src": ui.get("src"),
            "mint": ui.get("mint"),
            "symbol": ui.get("symbol"),
            "regime": ui.get("regime"),
            "decision": ui.get("decision"),
            "score": ui.get("score"),
            "reason": ui.get("reason"),
        }
    return ui


# ──────────────────────────────────────────────────────────────────────────────
# Snapshot normalization (meta -> snapshot)
# ──────────────────────────────────────────────────────────────────────────────
def _normalize_rug(raw: Any) -> float:
    try:
        v = float(raw)
    except Exception:
        return 0.0
    if v > 1.5:
        v = v / 100.0
    return max(0.0, min(1.0, v))


def _build_pf_snapshot_from_meta(meta: Mapping[str, Any]) -> Dict[str, Any]:
    m = meta

    def f_any(keys: Sequence[str], default: float = 0.0) -> float:
        for k in keys:
            if k in m:
                try:
                    return float(m.get(k, default))
                except Exception:
                    return float(default)
        return float(default)

    def i_any(keys: Sequence[str], default: int = 0) -> int:
        for k in keys:
            if k in m:
                try:
                    return int(m.get(k, default))
                except Exception:
                    return int(default)
        return int(default)

    liq_usd = f_any(["liq_usd", "liq", "liqUsd", "liquidity_usd"])
    vol5_usd = f_any(["vol_5m_usd", "vol5", "volume_5m_usd"])
    buys5 = i_any(["buys_5m", "buys5", "buys_5min"])
    sells5 = i_any(["sells_5m", "sells5", "sells_5min"])
    chg5 = f_any(["price_change_5m_pct", "chg5", "change_5m_pct"])
    age_min = f_any(["token_age_min", "age_min", "age_minutes"])
    price_usd = f_any(["price_usd", "priceUsd", "price"])
    ai_rug = f_any(["ai_rug_score", "fraud_score", "ai_rug_score_01"], 0.0)

    snap: Dict[str, Any] = {
        "liq_usd": liq_usd,
        "vol_5m_usd": vol5_usd,
        "buys_5m": buys5,
        "sells_5m": sells5,
        "txns_5m": int(buys5 + sells5),
        "price_change_5m_pct": chg5,
        "token_age_min": age_min,
        "price_usd": price_usd,
        "ai_rug_score": ai_rug,
        # keep raw for offline debugging (may be dropped by capper)
        "raw_meta": dict(meta),
    }
    if "_filter_debug" in meta:
        snap["_filter_debug"] = meta.get("_filter_debug")
    return snap


# ──────────────────────────────────────────────────────────────────────────────
# FeatureBuilder safe call
# ──────────────────────────────────────────────────────────────────────────────
def _call_accepted(fn: Any, **kwargs: Any) -> Any:
    try:
        return _call_with_accepted_kwargs(fn, **kwargs)
    except Exception:
        return fn(**kwargs)


def _build_features_safe(
    *,
    mint: str,
    symbol: Optional[str],
    snapshot: Mapping[str, Any],
    history: Optional[Sequence[Mapping[str, Any]]],
    regime: Optional[str],
    regime_state: Optional[Any],
    wallet_signals: Optional[Mapping[str, Any]],
    ai_signals: Optional[Mapping[str, Any]],
    scanner_source: Optional[str],
    position_context: Optional[Mapping[str, Any]],
) -> Dict[str, Any]:
    feats: Dict[str, Any] = {}

    def f(key: str, default: float = 0.0) -> float:
        try:
            return float(snapshot.get(key, default) or default)
        except Exception:
            return float(default)

    feats["liq_usd"] = f("liq_usd", f("liq", 0.0))
    feats["vol_5m_usd"] = f("vol_5m_usd", f("vol5", 0.0))
    feats["buys_5m"] = f("buys_5m", f("buys5", 0.0))
    feats["sells_5m"] = f("sells_5m", f("sells5", 0.0))
    feats["chg5_pct"] = f("price_change_5m_pct", f("chg5", 0.0))
    feats["age_min"] = f("token_age_min", f("age_min", 0.0))
    feats["price_usd"] = f("price_usd", 0.0)
    feats["ai_fraud_score"] = _normalize_rug(snapshot.get("ai_rug_score", 0.0))

    if _FB is None:
        feats["features_mode"] = "minimal"
        return feats

    try:
        built = _call_accepted(
            _FB.build,
            symbol=symbol,
            mint=mint,
            snapshot=dict(snapshot),
            history=list(history) if history else None,
            regime=regime_state,
            wallet_signals=dict(wallet_signals) if wallet_signals else None,
            ai_signals=dict(ai_signals) if ai_signals else None,
            scanner_source=scanner_source,
            risk_context=dict(position_context) if position_context else None,
            regime_label=regime,
        )
        if isinstance(built, dict):
            feats.update(built)
            feats["features_mode"] = "feature_builder"
        else:
            feats["features_mode"] = "minimal_fb_non_dict"
    except Exception:
        if log.every("feature_builder_fail", 60):
            log.debug("data_logger: FeatureBuilder.build failed (using minimal)", exc_info=True)
        feats["features_mode"] = "minimal_fb_failed"

    return feats


# ──────────────────────────────────────────────────────────────────────────────
# Non-blocking submit (sync callers -> async writes)
# ──────────────────────────────────────────────────────────────────────────────
def _submit(coro: Any) -> None:
    """
    Fire-and-forget if running inside an event loop; otherwise run once.
    Ensures task exceptions are consumed (no 'Task exception was never retrieved').
    """

    async def _runner() -> None:
        if _db_disabled():
            return
        try:
            await coro
        except Exception as e:
            _bump_db_fail(e)
            if AI_LOG_STRICT:
                # strict mode: we still can't raise into the caller in async fire-and-forget
                # but we *do* log loudly.
                log.exception("data_logger: strict async write failed")

    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_runner())
        return
    except RuntimeError:
        pass

    # No running loop -> best-effort run
    try:
        asyncio.run(_runner())
    except Exception as e:
        _bump_db_fail(e)
        if AI_LOG_STRICT:
            raise


# ──────────────────────────────────────────────────────────────────────────────
# Outcome labeling
# ──────────────────────────────────────────────────────────────────────────────
def _safe_label_from_pnl(pnl_pct: float, got_stopped: bool) -> str:
    try:
        p = float(pnl_pct)
    except Exception:
        p = 0.0
    if got_stopped:
        return "stopped_loss" if p <= 0 else "stopped_win"
    if p >= 40:
        return "mega_win"
    if p >= 20:
        return "monster_win"
    if p >= 10:
        return "big_win"
    if p >= 0:
        return "win"
    if p <= -35:
        return "disaster"
    if p <= -20:
        return "big_loss"
    return "loss"


def _base_ctx() -> Dict[str, Any]:
    return {
        "service": SERVICE_NAME,
        "version": SERVICE_VERSION,
        "host": socket.gethostname(),
        "pid": os.getpid(),
        "policy": AI_POLICY_VERSION or None,
        "experiment": AI_EXPERIMENT_TAG or None,
        "code_version": AI_CODE_VERSION or None,
        "stack_profile": AI_STACK_PROFILE or None,
    }


# ──────────────────────────────────────────────────────────────────────────────
# SQL helpers (schema-aligned)
# ──────────────────────────────────────────────────────────────────────────────
async def _a_insert_candidate_event(
    *,
    mint: str,
    source: str,
    pool: str,
    ai_ok: Optional[bool],
    ai_score: Optional[float],
    ui: Dict[str, Any],
) -> int:
    if _db_disabled():
        return 0
    ui = _cap_ui(ui)
    ui_s = json.dumps(ui, ensure_ascii=False)
    row = await _DBX.fetchrow(
        """
        INSERT INTO candidate_events (mint, source, pool, ai_ok, ai_score, ui)
        VALUES ($1, $2, $3, $4, $5, $6::jsonb)
        RETURNING id
        """,
        mint,
        source,
        pool,
        ai_ok,
        ai_score,
        ui_s,
    )
    return int(row["id"]) if row and "id" in row else 0


async def _a_patch_candidate_ui(candidate_id: int, patch: Dict[str, Any]) -> int:
    if _db_disabled():
        return 0
    patch = _cap_ui(patch)
    patch_s = json.dumps(patch, ensure_ascii=False)
    await _DBX.execute(
        """
        UPDATE candidate_events
        SET ui = COALESCE(ui, '{}'::jsonb) || $1::jsonb
        WHERE id = $2
        """,
        patch_s,
        int(candidate_id),
    )
    return int(candidate_id)


async def _a_insert_trade(
    *,
    mint: str,
    side: str,
    sig: str,
    venue: str,
    status: str,
    amount_sol: Optional[float],
    amount_raw: Optional[int],
    out_amount_sol: Optional[float],
    out_amount_raw: Optional[int],
    price_sol: Optional[float],
    fee_sol: Optional[float],
    reason: str,
    meta: Dict[str, Any],
) -> int:
    if _db_disabled():
        return 0
    meta = _cap_ui(meta)
    meta_s = json.dumps(meta, ensure_ascii=False)

    # Idempotent if schema defines any unique constraint (commonly sig unique).
    row = await _DBX.fetchrow(
        """
        INSERT INTO trades
          (mint, side, sig, venue, status, amount_sol, amount_raw, out_amount_sol, out_amount_raw,
           price_sol, fee_sol, reason, meta)
        VALUES
          ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb)
        ON CONFLICT DO NOTHING
        RETURNING id
        """,
        mint,
        side,
        sig,
        venue,
        status,
        amount_sol,
        amount_raw,
        out_amount_sol,
        out_amount_raw,
        price_sol,
        fee_sol,
        reason,
        meta_s,
    )
    return int(row["id"]) if row and "id" in row else 0


async def _a_upsert_position(
    *,
    mint: str,
    status: str,
    qty_raw: Optional[int],
    avg_price_sol: Optional[float],
    cost_sol: Optional[float],
    realized_pnl_sol: Optional[float],
    last_price_sol: Optional[float],
    meta: Dict[str, Any],
) -> int:
    if _db_disabled():
        return 0
    meta = _cap_ui(meta)
    meta_s = json.dumps(meta, ensure_ascii=False)
    await _DBX.execute(
        """
        INSERT INTO positions
          (mint, status, qty_raw, avg_price_sol, cost_sol, realized_pnl_sol, last_price_sol, meta, updated_at)
        VALUES
          ($1,$2,$3,$4,$5,$6,$7,$8::jsonb, now())
        ON CONFLICT (mint) DO UPDATE SET
          status = EXCLUDED.status,
          qty_raw = COALESCE(EXCLUDED.qty_raw, positions.qty_raw),
          avg_price_sol = COALESCE(EXCLUDED.avg_price_sol, positions.avg_price_sol),
          cost_sol = COALESCE(EXCLUDED.cost_sol, positions.cost_sol),
          realized_pnl_sol = COALESCE(EXCLUDED.realized_pnl_sol, positions.realized_pnl_sol),
          last_price_sol = COALESCE(EXCLUDED.last_price_sol, positions.last_price_sol),
          meta = COALESCE(positions.meta, '{}'::jsonb) || EXCLUDED.meta,
          updated_at = now()
        """,
        mint,
        status,
        qty_raw,
        avg_price_sol,
        cost_sol,
        realized_pnl_sol,
        last_price_sol,
        meta_s,
    )
    return 1


async def _a_upsert_exit_state(mint: str, state: Dict[str, Any]) -> int:
    if _db_disabled():
        return 0
    state = _cap_ui(state)
    state_s = json.dumps(state, ensure_ascii=False)
    await _DBX.execute(
        """
        INSERT INTO exit_state (mint, state, updated_at)
        VALUES ($1, $2::jsonb, now())
        ON CONFLICT (mint) DO UPDATE SET
          state = COALESCE(exit_state.state, '{}'::jsonb) || EXCLUDED.state,
          updated_at = now()
        """,
        mint,
        state_s,
    )
    return 1


async def _a_insert_risk_event(level: str, code: str, msg: str, meta: Dict[str, Any]) -> int:
    if _db_disabled():
        return 0
    meta = _cap_ui(meta)
    meta_s = json.dumps(meta, ensure_ascii=False)
    row = await _DBX.fetchrow(
        """
        INSERT INTO risk_events (level, code, msg, meta)
        VALUES ($1,$2,$3,$4::jsonb)
        RETURNING id
        """,
        level,
        code,
        msg,
        meta_s,
    )
    return int(row["id"]) if row and "id" in row else 0


async def _a_insert_heartbeat(component: str, payload: Dict[str, Any]) -> int:
    if _db_disabled():
        return 0
    payload = _cap_ui(payload)
    payload_s = json.dumps(payload, ensure_ascii=False)
    row = await _DBX.fetchrow(
        """
        INSERT INTO health_heartbeats (component, payload)
        VALUES ($1,$2::jsonb)
        RETURNING id
        """,
        component,
        payload_s,
    )
    return int(row["id"]) if row and "id" in row else 0


# ──────────────────────────────────────────────────────────────────────────────
# Public API: candidates (sync + async)
# ──────────────────────────────────────────────────────────────────────────────
async def a_log_entry_candidate(
    *,
    src: str,
    mint: str,
    symbol: Optional[str],
    snapshot: Mapping[str, Any],
    regime: Optional[str] = None,
    wallet_tag: Optional[str] = None,
    regime_state: Optional[Any] = None,
    wallet_signals: Optional[Mapping[str, Any]] = None,
    ai_signals: Optional[Mapping[str, Any]] = None,
    scanner_source: Optional[str] = None,
    history: Optional[Sequence[Mapping[str, Any]]] = None,
    position_context: Optional[Mapping[str, Any]] = None,
    decision: Optional[str] = None,  # BUY / SKIP / TEST_BUY / etc
    decision_score: Optional[float] = None,
    decision_reason: Optional[str] = None,
    episode_id: Optional[str] = None,
    step_idx: Optional[int] = None,
    strategy_profile: Optional[str] = None,
    trade_id: Optional[str] = None,
    session_id: Optional[str] = None,
    candidate_key: Optional[str] = None,
    gate_detail: Optional[Mapping[str, Any]] = None,
    ts: Optional[Any] = None,
    extra_raw: Optional[Mapping[str, Any]] = None,
    pool: str = "",
    event_uuid: Optional[str] = None,
) -> int:
    """
    Async candidate logger (returns the DB id).
    Use this when you need ids for tight linkage.
    """
    if _db_disabled() or (not AI_LOG_CANDIDATES):
        return 0
    if not _src_regime_allowed(src, regime):
        return 0

    ck = candidate_key or f"{src}:{regime or ''}:{mint}"
    d_upper = (decision or "").upper()

    if d_upper == "SKIP":
        if not AI_LOG_SKIPS:
            return 0
        if not _should_sample(ck, AI_LOG_SAMPLE_RATE_SKIPS):
            return 0
    else:
        if not _should_sample(ck, AI_LOG_SAMPLE_RATE):
            return 0

    # logger-level hard filters
    try:
        liq = float(snapshot.get("liq_usd") or snapshot.get("liq") or 0.0)
    except Exception:
        liq = 0.0
    try:
        vol5 = float(snapshot.get("vol_5m_usd") or snapshot.get("vol5") or 0.0)
    except Exception:
        vol5 = 0.0
    if AI_LOG_MIN_LIQ_USD > 0.0 and liq < AI_LOG_MIN_LIQ_USD:
        return 0
    if AI_LOG_MIN_VOL5_USD > 0.0 and vol5 < AI_LOG_MIN_VOL5_USD:
        return 0

    feats = _build_features_safe(
        mint=mint,
        symbol=symbol,
        snapshot=snapshot,
        history=history if AI_LOG_HISTORY_IN_UI else None,
        regime=regime,
        regime_state=regime_state,
        wallet_signals=wallet_signals,
        ai_signals=ai_signals,
        scanner_source=scanner_source or src,
        position_context=position_context,
    )

    score: Optional[float] = None
    if decision_score is not None:
        score = float(decision_score)
    elif "ai_entry_score" in feats:
        try:
            score = float(feats["ai_entry_score"])
        except Exception:
            score = None

    ai_ok: Optional[bool] = None
    if decision is not None:
        ai_ok = (d_upper != "SKIP")

    e_uuid = event_uuid or str(uuid.uuid4())
    ui: Dict[str, Any] = {
        "ts": ts if ts is not None else _ts(),
        "event_uuid": e_uuid,
        "src": src,
        "mint": mint,
        "symbol": symbol,
        "regime": regime,
        "wallet_tag": wallet_tag,
        "decision": decision,
        "score": score,
        "reason": decision_reason,
        "trade_id": trade_id,
        "session_id": session_id,
        "candidate_key": candidate_key,
        "episode_id": episode_id,
        "step_idx": step_idx,
        "strategy_profile": strategy_profile,
        "features": feats,
        "raw": dict(snapshot),
        "ctx": _base_ctx(),
    }

    if extra_raw:
        ui["extra_raw"] = dict(extra_raw)
    if AI_LOG_HISTORY_IN_UI and history:
        ui["history"] = list(history[-max(0, AI_LOG_HISTORY_MAX_LEN) :])
    if wallet_signals:
        ui["wallet_signals"] = dict(wallet_signals)
    if ai_signals:
        ui["ai_signals"] = dict(ai_signals)
    if position_context:
        ui["position_context"] = dict(position_context)
    if gate_detail:
        ui["gate_detail"] = dict(gate_detail)

    try:
        return await _a_insert_candidate_event(
            mint=mint,
            source=src,
            pool=pool or "",
            ai_ok=ai_ok,
            ai_score=score,
            ui=ui,
        )
    except Exception as e:
        _bump_db_fail(e)
        if AI_LOG_STRICT:
            raise
        return 0


def log_entry_candidate(**kwargs: Any) -> int:
    """
    Sync wrapper (fire-and-forget in a running loop).
    Returns:
      - 0 in hot async contexts (scheduled),
      - best-effort id if executed synchronously (no running loop).
    """
    if _db_disabled() or (not AI_LOG_CANDIDATES):
        return 0

    async def _write() -> int:
        return await a_log_entry_candidate(**kwargs)

    try:
        asyncio.get_running_loop()
        _submit(_write())
        return 0
    except RuntimeError:
        try:
            return asyncio.run(_write())
        except Exception:
            if AI_LOG_STRICT:
                raise
            return 0


def log_buy_candidate(**kwargs: Any) -> int:
    kwargs.setdefault("decision", "BUY")
    return log_entry_candidate(**kwargs)


def log_skip_candidate(**kwargs: Any) -> int:
    if not AI_LOG_SKIPS:
        return 0
    kwargs.setdefault("decision", "SKIP")
    return log_entry_candidate(**kwargs)


async def a_log_entry_decision_update(
    *,
    candidate_id: int,
    decision: Optional[str] = None,
    decision_score: Optional[float] = None,
    decision_reason: Optional[str] = None,
    extra: Optional[Mapping[str, Any]] = None,
) -> int:
    if _db_disabled() or candidate_id <= 0:
        return 0

    patch: Dict[str, Any] = {
        "decision": decision,
        "score": float(decision_score) if decision_score is not None else None,
        "reason": decision_reason,
        "decision_updated_at": _ts(),
    }
    if extra:
        patch["decision_extra"] = dict(extra)

    try:
        return await _a_patch_candidate_ui(int(candidate_id), patch)
    except Exception as e:
        _bump_db_fail(e)
        if AI_LOG_STRICT:
            raise
        return int(candidate_id)


def log_entry_decision_update(
    *,
    candidate_id: int,
    decision: Optional[str] = None,
    decision_score: Optional[float] = None,
    decision_reason: Optional[str] = None,
    extra: Optional[Mapping[str, Any]] = None,
) -> int:
    if _db_disabled() or candidate_id <= 0:
        return 0
    _submit(
        a_log_entry_decision_update(
            candidate_id=candidate_id,
            decision=decision,
            decision_score=decision_score,
            decision_reason=decision_reason,
            extra=extra,
        )
    )
    return int(candidate_id)


async def a_log_entry_outcome(
    *,
    candidate_id: int,
    pnl_pct: float,
    hold_seconds: float,
    got_stopped: bool,
    label: Optional[str] = None,
    notes: Optional[str] = None,
    extra: Optional[Mapping[str, Any]] = None,
) -> int:
    if _db_disabled() or (not AI_LOG_OUTCOMES) or candidate_id <= 0:
        return 0
    if not _should_sample(f"outcome:{candidate_id}", AI_LOG_OUTCOME_SAMPLE_RATE):
        return 0

    outcome = {
        "label": label or _safe_label_from_pnl(pnl_pct, got_stopped),
        "pnl_pct": float(pnl_pct),
        "hold_sec": float(hold_seconds),
        "got_stopped": bool(got_stopped),
        "notes": notes,
        "ts": _ts(),
        "extra": dict(extra) if extra else {},
    }

    try:
        await _a_patch_candidate_ui(int(candidate_id), {"outcome": outcome})
        await _a_insert_risk_event(
            level="info",
            code="outcome",
            msg=f"candidate_outcome label={outcome['label']} pnl_pct={outcome['pnl_pct']:.2f}",
            meta={"candidate_id": int(candidate_id), "outcome": outcome, "ctx": _base_ctx()},
        )
        return int(candidate_id)
    except Exception as e:
        _bump_db_fail(e)
        if AI_LOG_STRICT:
            raise
        return int(candidate_id)


def log_entry_outcome(
    *,
    candidate_id: int,
    pnl_pct: float,
    hold_seconds: float,
    got_stopped: bool,
    label: Optional[str] = None,
    notes: Optional[str] = None,
    extra: Optional[Mapping[str, Any]] = None,
) -> int:
    if _db_disabled() or (not AI_LOG_OUTCOMES) or candidate_id <= 0:
        return 0
    _submit(
        a_log_entry_outcome(
            candidate_id=candidate_id,
            pnl_pct=pnl_pct,
            hold_seconds=hold_seconds,
            got_stopped=got_stopped,
            label=label,
            notes=notes,
            extra=extra,
        )
    )
    return int(candidate_id)


# ──────────────────────────────────────────────────────────────────────────────
# Public API: executions / positions / exit_state (sync + async)
# ──────────────────────────────────────────────────────────────────────────────
async def a_log_trade_execution(
    *,
    mint: str,
    side: str,
    sig: str = "",
    venue: str = "",
    status: str = "ok",
    amount_sol: Optional[float] = None,
    amount_raw: Optional[int] = None,
    out_amount_sol: Optional[float] = None,
    out_amount_raw: Optional[int] = None,
    price_sol: Optional[float] = None,
    fee_sol: Optional[float] = None,
    reason: str = "",
    meta: Optional[Mapping[str, Any]] = None,
    candidate_id: Optional[int] = None,
    session_id: Optional[str] = None,
) -> int:
    if _db_disabled() or (not AI_LOG_EXECUTIONS):
        return 0
    if not _should_sample(f"exec:{mint}:{side}:{sig}", AI_LOG_EXEC_SAMPLE_RATE):
        return 0

    s = str(side).lower().strip()
    if s in ("buy", "sell"):
        pass
    elif s == "b":
        s = "buy"
    elif s == "s":
        s = "sell"
    else:
        s = (s[:12] or "unknown")

    meta_d: Dict[str, Any] = dict(meta) if meta else {}
    if candidate_id is not None:
        meta_d["candidate_id"] = int(candidate_id)
    if session_id is not None:
        meta_d["session_id"] = session_id
    meta_d.setdefault("ctx", _base_ctx())

    try:
        return await _a_insert_trade(
            mint=mint,
            side=s,
            sig=sig or "",
            venue=venue or "",
            status=status or "unknown",
            amount_sol=float(amount_sol) if amount_sol is not None else None,
            amount_raw=int(amount_raw) if amount_raw is not None else None,
            out_amount_sol=float(out_amount_sol) if out_amount_sol is not None else None,
            out_amount_raw=int(out_amount_raw) if out_amount_raw is not None else None,
            price_sol=float(price_sol) if price_sol is not None else None,
            fee_sol=float(fee_sol) if fee_sol is not None else None,
            reason=reason or "",
            meta=meta_d,
        )
    except Exception as e:
        _bump_db_fail(e)
        if AI_LOG_STRICT:
            raise
        return 0


def log_trade_execution(**kwargs: Any) -> int:
    if _db_disabled() or (not AI_LOG_EXECUTIONS):
        return 0

    async def _write() -> int:
        return await a_log_trade_execution(**kwargs)

    try:
        asyncio.get_running_loop()
        _submit(_write())
        return 0
    except RuntimeError:
        try:
            return asyncio.run(_write())
        except Exception:
            if AI_LOG_STRICT:
                raise
            return 0


def log_position_upsert(
    *,
    mint: str,
    status: str = "open",
    qty_raw: Optional[int] = None,
    avg_price_sol: Optional[float] = None,
    cost_sol: Optional[float] = None,
    realized_pnl_sol: Optional[float] = None,
    last_price_sol: Optional[float] = None,
    meta: Optional[Mapping[str, Any]] = None,
) -> int:
    if _db_disabled():
        return 0

    meta_d = dict(meta) if meta else {}
    meta_d.setdefault("ctx", _base_ctx())

    async def _write() -> int:
        try:
            return await _a_upsert_position(
                mint=mint,
                status=status or "open",
                qty_raw=int(qty_raw) if qty_raw is not None else None,
                avg_price_sol=float(avg_price_sol) if avg_price_sol is not None else None,
                cost_sol=float(cost_sol) if cost_sol is not None else None,
                realized_pnl_sol=float(realized_pnl_sol) if realized_pnl_sol is not None else None,
                last_price_sol=float(last_price_sol) if last_price_sol is not None else None,
                meta=meta_d,
            )
        except Exception as e:
            _bump_db_fail(e)
            if AI_LOG_STRICT:
                raise
            return 0

    _submit(_write())
    return 1


def log_exit_state_upsert(*, mint: str, state: Mapping[str, Any]) -> int:
    if _db_disabled():
        return 0

    async def _write() -> int:
        try:
            st = dict(state)
            st.setdefault("ctx", _base_ctx())
            return await _a_upsert_exit_state(mint, st)
        except Exception as e:
            _bump_db_fail(e)
            if AI_LOG_STRICT:
                raise
            return 0

    _submit(_write())
    return 1


# ──────────────────────────────────────────────────────────────────────────────
# Public API: events / snapshots
# ──────────────────────────────────────────────────────────────────────────────
def log_ai_event(
    *,
    event_type: str,
    level: str = "info",
    msg: str = "",
    src: Optional[str] = None,
    regime: Optional[str] = None,
    payload: Optional[Mapping[str, Any]] = None,
) -> int:
    if _db_disabled() or (not AI_LOG_EVENTS):
        return 0
    if not _should_sample(f"evt:{event_type}:{src or ''}:{regime or ''}", AI_LOG_EVENT_SAMPLE_RATE):
        return 0

    meta: Dict[str, Any] = {
        "event_type": event_type,
        "src": src,
        "regime": regime,
        "ts": _ts(),
        "payload": dict(payload) if payload else {},
        "ctx": _base_ctx(),
    }

    _submit(_a_insert_risk_event(level=level or "info", code=event_type, msg=msg or "", meta=meta))
    return 1


def log_regime_snapshot(*, regime: str, state: Mapping[str, Any], extra: Optional[Mapping[str, Any]] = None) -> int:
    if _db_disabled() or (not AI_LOG_SNAPSHOTS):
        return 0
    if not _should_sample(f"snap:regime:{regime}", AI_LOG_SNAPSHOT_SAMPLE_RATE):
        return 0
    payload: Dict[str, Any] = {"ts": _ts(), "regime": regime, "state": dict(state), "ctx": _base_ctx()}
    if extra:
        payload["extra"] = dict(extra)
    _submit(_a_insert_heartbeat("regime", payload))
    return 1


def log_equity_curve_point(
    *,
    equity_usd: Optional[float] = None,
    balance_sol: Optional[float] = None,
    pnl_day_pct: Optional[float] = None,
    pnl_total_pct: Optional[float] = None,
    extra: Optional[Mapping[str, Any]] = None,
) -> int:
    if _db_disabled() or (not AI_LOG_SNAPSHOTS):
        return 0
    if not _should_sample("snap:equity", AI_LOG_SNAPSHOT_SAMPLE_RATE):
        return 0
    payload: Dict[str, Any] = {
        "ts": _ts(),
        "equity_usd": float(equity_usd) if equity_usd is not None else None,
        "balance_sol": float(balance_sol) if balance_sol is not None else None,
        "pnl_day_pct": float(pnl_day_pct) if pnl_day_pct is not None else None,
        "pnl_total_pct": float(pnl_total_pct) if pnl_total_pct is not None else None,
        "ctx": _base_ctx(),
    }
    if extra:
        payload["extra"] = dict(extra)
    _submit(_a_insert_heartbeat("equity", payload))
    return 1


def log_risk_manager_snapshot(*, snapshot: Mapping[str, Any]) -> int:
    if _db_disabled() or (not AI_LOG_SNAPSHOTS):
        return 0
    if not _should_sample("snap:risk", AI_LOG_SNAPSHOT_SAMPLE_RATE):
        return 0
    payload = {"ts": _ts(), "snapshot": dict(snapshot), "ctx": _base_ctx()}
    _submit(_a_insert_heartbeat("risk_manager", payload))
    return 1


def log_reinvestor_snapshot(*, snapshot: Mapping[str, Any]) -> int:
    if _db_disabled() or (not AI_LOG_SNAPSHOTS):
        return 0
    if not _should_sample("snap:reinvestor", AI_LOG_SNAPSHOT_SAMPLE_RATE):
        return 0
    payload = {"ts": _ts(), "snapshot": dict(snapshot), "ctx": _base_ctx()}
    _submit(_a_insert_heartbeat("reinvestor", payload))
    return 1


# ──────────────────────────────────────────────────────────────────────────────
# Source-specific convenience wrappers
# ──────────────────────────────────────────────────────────────────────────────
def log_raydium_candidate(
    *,
    mint: str,
    symbol: Optional[str],
    meta: Mapping[str, Any],
    regime: str = "raydium_live",
    **kwargs: Any,
) -> int:
    snapshot = _build_pf_snapshot_from_meta(meta)
    return log_entry_candidate(src="raydium", mint=mint, symbol=symbol, snapshot=snapshot, regime=regime, **kwargs)


def log_pumpfun_candidate(
    *,
    mint: str,
    symbol: Optional[str],
    meta: Mapping[str, Any],
    regime: str = "pumpfun_live",
    **kwargs: Any,
) -> int:
    snapshot = _build_pf_snapshot_from_meta(meta)
    return log_entry_candidate(src="pumpfun", mint=mint, symbol=symbol, snapshot=snapshot, regime=regime, **kwargs)


def log_dexscreener_candidate(
    *,
    mint: str,
    symbol: Optional[str],
    meta: Mapping[str, Any],
    regime: str = "dexscreener_live",
    **kwargs: Any,
) -> int:
    snapshot = _build_pf_snapshot_from_meta(meta)
    return log_entry_candidate(src="dexscreener", mint=mint, symbol=symbol, snapshot=snapshot, regime=regime, **kwargs)


def log_momo_candidate(
    *,
    mint: str,
    symbol: Optional[str],
    meta: Mapping[str, Any],
    regime: str = "momo_live",
    **kwargs: Any,
) -> int:
    snapshot = _build_pf_snapshot_from_meta(meta)
    return log_entry_candidate(src="momo", mint=mint, symbol=symbol, snapshot=snapshot, regime=regime, **kwargs)
