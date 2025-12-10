from __future__ import annotations

"""
utils_pkg/ai_db.py

Postgres helpers for AI logging & quant analytics (V3+++ / quant-grade).
=======================================================================

High-level role
---------------
Central DB utility for all AI / quant / analytics layers:

• DSN resolution / normalization (Render-friendly, SQLAlchemy-friendly).
• Safe, minimal connection helpers (no fancy pooling; not a hot QPS path).
• Table creation & migrations for:

    - entry_candidates      (raw decision context / features)
    - entry_outcomes        (labels / PnL / drawdowns / hold time)
    - trade_executions      (per-fill executions, slippage / fees)
    - regime_snapshots      (regime state time series)
    - ai_events             (high-level AI / risk / strategy events)

• Read / write helpers for:
    - data_logger
    - self_learning_ai_strategy
    - risk_manager / reinvestor
    - backtest_engine
    - offline training scripts

• Quant-grade helpers:
    - Training row fetchers / iterators
    - Backtest row iterator with flexible filters
    - Performance breakdowns by regime / src / policy_version / experiment_tag
    - Equity curve approximations
    - Drawdown stats (dd tail risk)
    - Label distribution / label “quality”
    - Outcome coverage (label health)
    - Kelly-style stats for risk sizing (V3+++)
    - Simple Sharpe-like time-bucketed summary (V3+++)

Design philosophy
-----------------
• Keep this module *boring* and stable.
• Provide narrow, well-documented functions called by more complex layers.
• Fail fast on DSN issues; be forgiving on analytic helpers.
"""

import json
import os
from contextlib import contextmanager
from datetime import datetime
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

import psycopg2
from psycopg2.extensions import connection as _PGConn, cursor as _PGCursor

from logging_setup import get_logger

log = get_logger(__name__)

# =============================================================================
# DSN env configuration
# =============================================================================

# Primary env var used everywhere (Render secret + local .env)
_DSN_ENV_PRIMARY = "SNIPER_DB_DSN"

# Additional env vars we accept as fallbacks (in priority order)
_DSN_ENV_CANDIDATES: List[str] = [
    _DSN_ENV_PRIMARY,
    "SNIPER_DB_URL",
    "DATABASE_URL",
    "PG_DSN",
    "PG_DSN_INTERNAL",
    "PG_DSN_EXTERNAL",
    # Common Render env for Postgres if user wired it that way
    "RENDER_INTERNAL_DATABASE_URL",
]

# Fallback for local testing if nothing is set in the environment.
DB_URL_FALLBACK = os.getenv("SNIPER_DB_DSN_FALLBACK", "")

# Optional DSN tweaks / diagnostics
AI_DB_APP_NAME = os.getenv("AI_DB_APP_NAME", "sniper_ai_db")
AI_DB_STATEMENT_TIMEOUT_MS = int(os.getenv("AI_DB_STATEMENT_TIMEOUT_MS", "0") or "0")
AI_DB_IDLE_IN_TX_TIMEOUT_MS = int(
    os.getenv("AI_DB_IDLE_IN_TX_TIMEOUT_MS", "0") or "0"
)

# Small module-level cache so we only normalize DSN once
_NORMALIZED_DSN: Optional[str] = None


# =============================================================================
# DSN helpers
# =============================================================================


def _mask_dsn(raw: str) -> str:
    """
    Mask DSN for logs, hiding username/password.

    Examples:
        postgres://user:pass@host:5432/db  -> host:5432/db
        postgresql://user@host/db?ssl...   -> host/db?ssl...
        postgresql+asyncpg://user@h/db     -> h/db
    """
    if not raw:
        return "<empty>"
    try:
        if "://" in raw:
            _, rest = raw.split("://", 1)
        else:
            rest = raw

        if "@" in rest:
            _, hostpart = rest.split("@", 1)
        else:
            hostpart = rest
        return hostpart
    except Exception:
        return "<dsn>"


def _normalize_dsn(raw: str) -> str:
    """
    Normalize DSN for psycopg2:

    - Accept both postgres:// and postgresql://
    - Accept SQLAlchemy-style postgresql+asyncpg:// and strip the driver suffix
    - If talking to Render (render.com / *-postgres.render.com) and sslmode
      isn't set, add ?sslmode=require

    If the string looks like key=value pairs (no "://"), it is returned as-is.
    """
    import urllib.parse as urlparse

    if not raw:
        raise RuntimeError("Empty DB DSN")

    # Already key=value style → return as-is
    if "://" not in raw:
        return raw

    parsed = urlparse.urlparse(raw)

    # Normalize scheme: strip driver suffix (e.g. postgresql+asyncpg)
    base_scheme = parsed.scheme
    if "+" in base_scheme:
        base_scheme = base_scheme.split("+", 1)[0]

    if base_scheme.startswith("postgres"):
        scheme = "postgresql"
    else:
        scheme = base_scheme or "postgresql"

    netloc = parsed.netloc
    path = parsed.path
    query = parsed.query or ""

    # Add sslmode=require for typical Render hosts if not present
    if ("render.com" in netloc or "postgres.render.com" in netloc) and "sslmode=" not in query:
        query = (query + "&" if query else "") + "sslmode=require"

    normalized = urlparse.urlunparse((scheme, netloc, path, "", query, ""))
    return normalized


def _resolve_raw_dsn() -> str:
    """
    Look through known env vars, then DB_URL_FALLBACK.
    """
    for key in _DSN_ENV_CANDIDATES:
        v = os.environ.get(key)
        if v and v.strip():
            log.info("ai_db: using DSN from %s", key)
            return v.strip()

    if DB_URL_FALLBACK:
        log.info("ai_db: using DSN from DB_URL_FALLBACK")
        return DB_URL_FALLBACK.strip()

    raise RuntimeError(
        "No Postgres DSN found. Set SNIPER_DB_DSN (recommended) or one of "
        f"{_DSN_ENV_CANDIDATES[1:]} or SNIPER_DB_DSN_FALLBACK."
    )


def _get_dsn() -> str:
    """
    Resolve + normalize DSN from env / fallback once per process.
    """
    global _NORMALIZED_DSN
    if _NORMALIZED_DSN:
        return _NORMALIZED_DSN

    raw = _resolve_raw_dsn()
    _NORMALIZED_DSN = _normalize_dsn(raw)
    log.info("ai_db: using DSN (normalized): %s", _mask_dsn(_NORMALIZED_DSN))
    return _NORMALIZED_DSN


def get_normalized_dsn_for_debug() -> str:
    """
    Small helper for scripts/test_pg.py style diagnostics.
    Returns the normalized DSN (masked) for logging/printing.
    """
    return _mask_dsn(_get_dsn())


# =============================================================================
# Core connection helpers
# =============================================================================


def _connect() -> _PGConn:
    """
    Low-level connection creator. One connection per context usage.

    We keep it simple and rely on Postgres' connection handling; this module
    is not a high-QPS hot path.
    """
    dsn = _get_dsn()
    conn = psycopg2.connect(dsn)
    # Optional diagnostics / safety knobs
    try:
        with conn.cursor() as cur:
            cur.execute("SET application_name = %s", (AI_DB_APP_NAME,))
            if AI_DB_STATEMENT_TIMEOUT_MS > 0:
                cur.execute(
                    "SET statement_timeout = %s",
                    (AI_DB_STATEMENT_TIMEOUT_MS,),
                )
            if AI_DB_IDLE_IN_TX_TIMEOUT_MS > 0:
                cur.execute(
                    "SET idle_in_transaction_session_timeout = %s",
                    (AI_DB_IDLE_IN_TX_TIMEOUT_MS,),
                )
    except Exception:
        # Not fatal – just log and keep going
        log.exception("ai_db: failed to apply connection-level settings")
    return conn


@contextmanager
def conn_cursor() -> Iterable[Tuple[_PGConn, _PGCursor]]:
    """
    Context manager that yields (conn, cur) and handles commit/rollback.
    Use this for multi-statement operations.
    """
    conn = _connect()
    try:
        with conn.cursor() as cur:
            yield conn, cur
            conn.commit()
    except Exception:
        conn.rollback()
        log.exception("DB error while executing ai_db operation")
        raise
    finally:
        conn.close()


@contextmanager
def cursor_only() -> Iterable[_PGCursor]:
    """
    Convenience wrapper when you only need the cursor and don't care about conn.
    """
    with conn_cursor() as (_c, cur):
        yield cur


# =============================================================================
# Health check / simple utilities
# =============================================================================


def ping() -> bool:
    """
    Cheap health check: returns True if we can run `SELECT 1`.
    """
    try:
        with cursor_only() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        return True
    except Exception:
        log.exception("ai_db.ping() failed")
        return False


def describe_backend() -> str:
    """
    Human-readable description of the backend.

    Used by scripts/test_ai_logging.py to print a quick summary like:
      dsn=host:5432/db?sslmode=require, server=16.2, ping=True
    """
    dsn = _get_dsn()
    dsn_mask = _mask_dsn(dsn)
    ok = False
    version = "unknown"
    try:
        with cursor_only() as cur:
            cur.execute("SHOW server_version")
            row = cur.fetchone()
            if row and row[0]:
                version = str(row[0])
        ok = True
    except Exception as e:
        log.warning("describe_backend: failed to query server_version: %s", e)

    return f"dsn={dsn_mask}, server={version}, ping={ok}"


def _jsonb(obj: Mapping[str, Any] | Sequence[Any] | None) -> str:
    """
    JSON-encode in a psycopg2-friendly way.
    """
    if obj is None:
        return "{}"
    return json.dumps(obj, separators=(",", ":"))


# =============================================================================
# Table creation / migrations helper
# =============================================================================


def ensure_ai_tables() -> None:
    """
    Create core AI tables if they don't exist and ensure basic indexes exist.

    Tables
    ------
    • entry_candidates
    • entry_outcomes
    • trade_executions        (V3+++; per-fill execution stats)
    • regime_snapshots        (V3+++; regime timeseries)
    • ai_events               (V3+++; AI / risk / strategy events)

    This is safe to call repeatedly and is meant for scripts/init_ai_tables.py.
    """
    with conn_cursor() as (_, cur):
        # Core tables
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS entry_candidates (
                id              BIGSERIAL PRIMARY KEY,
                ts              TIMESTAMPTZ NOT NULL DEFAULT now(),
                src             TEXT NOT NULL,
                mint            TEXT NOT NULL,
                symbol          TEXT,
                regime          TEXT,
                wallet_tag      TEXT,
                feature_version INT NOT NULL,
                features        JSONB NOT NULL,
                raw             JSONB NOT NULL
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS entry_outcomes (
                id           BIGSERIAL PRIMARY KEY,
                candidate_id BIGINT UNIQUE REFERENCES entry_candidates(id) ON DELETE CASCADE,
                label        TEXT NOT NULL,
                pnl_pct      DOUBLE PRECISION,
                hold_sec     INTEGER,
                notes        TEXT,
                extra        JSONB NOT NULL DEFAULT '{}'::jsonb,
                ts           TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        )

        # Per-fill executions (for slippage / fees / router stats)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_executions (
                id           BIGSERIAL PRIMARY KEY,
                ts           TIMESTAMPTZ NOT NULL DEFAULT now(),
                trade_id     TEXT NOT NULL,
                mint         TEXT NOT NULL,
                side         TEXT NOT NULL,
                qty_raw      NUMERIC,
                price_usd    DOUBLE PRECISION,
                slippage_bps DOUBLE PRECISION,
                fees_usd     DOUBLE PRECISION,
                candidate_id BIGINT REFERENCES entry_candidates(id) ON DELETE SET NULL,
                session_id   TEXT,
                extra        JSONB NOT NULL DEFAULT '{}'::jsonb
            );
            """
        )

        # Regime snapshots (for regime-aware AI & dashboards)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS regime_snapshots (
                id         BIGSERIAL PRIMARY KEY,
                ts         TIMESTAMPTZ NOT NULL DEFAULT now(),
                regime     TEXT NOT NULL,
                state      JSONB NOT NULL,
                session_id TEXT,
                extra      JSONB NOT NULL DEFAULT '{}'::jsonb
            );
            """
        )

        # Optional: lightweight ai_events table for self-learning / monitoring
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ai_events (
                id             BIGSERIAL PRIMARY KEY,
                ts             TIMESTAMPTZ NOT NULL DEFAULT now(),
                event_type     TEXT NOT NULL,
                policy_version TEXT,
                experiment_tag TEXT,
                src            TEXT,
                regime         TEXT,
                payload        JSONB NOT NULL DEFAULT '{}'::jsonb
            );
            """
        )

        # Helpful indexes for quant queries (IF NOT EXISTS is idempotent)
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_entry_candidates_ts
            ON entry_candidates (ts DESC);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_entry_candidates_src_regime_ts
            ON entry_candidates (src, regime, ts DESC);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_entry_candidates_feature_version
            ON entry_candidates (feature_version);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_entry_candidates_mint_ts
            ON entry_candidates (mint, ts DESC);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_entry_candidates_wallet_tag_ts
            ON entry_candidates (wallet_tag, ts DESC);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_entry_outcomes_candidate_id
            ON entry_outcomes (candidate_id);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_entry_outcomes_label_ts
            ON entry_outcomes (label, ts DESC);
            """
        )
        # Extra: many quant queries filter by o.ts, so index it explicitly
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_entry_outcomes_ts
            ON entry_outcomes (ts DESC);
            """
        )

        # Quant-grade: expression indexes on policy_version / experiment_tag in raw
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_entry_candidates_policy_version
            ON entry_candidates ((raw->'_decision_meta'->>'ai_policy_version'));
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_entry_candidates_experiment_tag
            ON entry_candidates ((raw->'_decision_meta'->>'ai_experiment_tag'));
            """
        )

        # V3+++: index on extra->'max_dd_pct' to accelerate drawdown stats
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_entry_outcomes_max_dd_pct
            ON entry_outcomes ((extra->>'max_dd_pct'));
            """
        )

        # trade_executions indexes
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_trade_executions_ts
            ON trade_executions (ts DESC);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_trade_executions_trade_id
            ON trade_executions (trade_id);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_trade_executions_candidate_id
            ON trade_executions (candidate_id);
            """
        )

        # regime_snapshots indexes
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_regime_snapshots_ts
            ON regime_snapshots (ts DESC);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_regime_snapshots_regime_ts
            ON regime_snapshots (regime, ts DESC);
            """
        )

        # ai_events indexes
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_ai_events_ts
            ON ai_events (ts DESC);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_ai_events_type_ts
            ON ai_events (event_type, ts DESC);
            """
        )

    log.info(
        "ensure_ai_tables: ensured entry_candidates, entry_outcomes, "
        "trade_executions, regime_snapshots, ai_events + indexes exist"
    )


# =============================================================================
# entry_candidates helpers (training input)
# =============================================================================


def insert_entry_candidate(
    *,
    src: str,
    mint: str,
    symbol: Optional[str],
    regime: Optional[str],
    wallet_tag: Optional[str],
    feature_version: int,
    features: Mapping[str, Any],
    raw: Mapping[str, Any],
) -> int:
    """
    Insert a row into entry_candidates and return the new id.

    Stable API used by data_logger.log_entry_candidate().
    """
    with conn_cursor() as (_, cur):
        cur.execute(
            """
            INSERT INTO entry_candidates
                (src, mint, symbol, regime, wallet_tag,
                 feature_version, features, raw)
            VALUES
                (%s,  %s,   %s,     %s,     %s,
                 %s,             %s::jsonb, %s::jsonb)
            RETURNING id
            """,
            (
                src,
                mint,
                symbol,
                regime,
                wallet_tag,
                int(feature_version),
                _jsonb(features),
                _jsonb(raw),
            ),
        )
        (row_id,) = cur.fetchone()
        return int(row_id)


def fetch_latest_candidates(
    *,
    limit: int = 50,
    src: Optional[str] = None,
    regime: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Convenience helper used by scripts/show_latest_candidates.py etc.
    """
    limit = max(1, min(int(limit), 500))

    clauses: List[str] = []
    params: List[Any] = []

    if src:
        clauses.append("src = %s")
        params.append(src)
    if regime:
        clauses.append("regime = %s")
        params.append(regime)

    where_sql = ""
    if clauses:
        where_sql = "WHERE " + " AND ".join(clauses)

    sql = f"""
        SELECT id, ts, src, mint, symbol, regime, wallet_tag,
               feature_version, features, raw
        FROM entry_candidates
        {where_sql}
        ORDER BY ts DESC, id DESC
        LIMIT %s
    """
    params.append(limit)

    out: List[Dict[str, Any]] = []
    with cursor_only() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    for r in rows:
        (
            row_id,
            ts,
            src_val,
            mint_val,
            symbol_val,
            regime_val,
            wallet_tag_val,
            feat_ver,
            features_json,
            raw_json,
        ) = r

        if isinstance(features_json, str):
            try:
                features_obj = json.loads(features_json)
            except Exception:
                features_obj = {}
        else:
            features_obj = features_json or {}

        if isinstance(raw_json, str):
            try:
                raw_obj = json.loads(raw_json)
            except Exception:
                raw_obj = {}
        else:
            raw_obj = raw_json or {}

        out.append(
            {
                "id": int(row_id),
                "ts": ts,
                "src": src_val,
                "mint": mint_val,
                "symbol": symbol_val,
                "regime": regime_val,
                "wallet_tag": wallet_tag_val,
                "feature_version": int(feat_ver),
                "features": features_obj,
                "raw": raw_obj,
            }
        )

    return out


def fetch_candidate_by_id(candidate_id: int) -> Optional[Dict[str, Any]]:
    """
    Small helper for backtests / debugging: fetch a single entry_candidates row.
    """
    with cursor_only() as cur:
        cur.execute(
            """
            SELECT id, ts, src, mint, symbol, regime, wallet_tag,
                   feature_version, features, raw
            FROM entry_candidates
            WHERE id = %s
            """,
            (int(candidate_id),),
        )
        row = cur.fetchone()

    if not row:
        return None

    (
        row_id,
        ts,
        src_val,
        mint_val,
        symbol_val,
        regime_val,
        wallet_tag_val,
        feat_ver,
        features_json,
        raw_json,
    ) = row

    if isinstance(features_json, str):
        try:
            features_obj = json.loads(features_json)
        except Exception:
            features_obj = {}
    else:
        features_obj = features_json or {}

    if isinstance(raw_json, str):
        try:
            raw_obj = json.loads(raw_json)
        except Exception:
            raw_obj = {}
    else:
        raw_obj = raw_json or {}

    return {
        "id": int(row_id),
        "ts": ts,
        "src": src_val,
        "mint": mint_val,
        "symbol": symbol_val,
        "regime": regime_val,
        "wallet_tag": wallet_tag_val,
        "feature_version": int(feat_ver),
        "features": features_obj,
        "raw": raw_obj,
    }


def update_entry_decision(
    *,
    candidate_id: int,
    decision: Optional[str] = None,
    decision_score: Optional[float] = None,
    decision_reason: Optional[str] = None,
    **extra_fields: Any,
) -> int:
    """
    Optional helper used by data_logger.log_entry_decision_update.

    We patch the `raw` JSONB with an updated `_decision_meta` blob.
    """
    patch_meta: Dict[str, Any] = {
        "decision": decision,
        "decision_score": decision_score,
        "decision_reason": decision_reason,
    }
    if extra_fields:
        patch_meta.update(extra_fields)

    patch_json = _jsonb({"_decision_meta": patch_meta})

    with conn_cursor() as (_, cur):
        cur.execute(
            """
            UPDATE entry_candidates
            SET raw = COALESCE(raw, '{}'::jsonb) || %s::jsonb
            WHERE id = %s
            RETURNING id
            """,
            (patch_json, int(candidate_id)),
        )
        row = cur.fetchone()
        if not row:
            return 0
        (row_id,) = row
        log.debug(
            "ai_db.update_entry_decision: candidate_id=%s decision=%s score=%s",
            candidate_id,
            decision,
            decision_score,
        )
        return int(row_id)


# =============================================================================
# entry_outcomes helpers (training labels)
# =============================================================================


def insert_entry_outcome(
    *,
    candidate_id: int,
    label: str,
    pnl_pct: Optional[float] = None,
    hold_sec: Optional[int] = None,
    notes: Optional[str] = None,
    extra: Optional[Mapping[str, Any]] = None,
) -> int:
    """
    Insert a row into entry_outcomes.

    Typical usage from backtests or manual labeling tools.
    """
    with conn_cursor() as (_, cur):
        cur.execute(
            """
            INSERT INTO entry_outcomes
                (candidate_id, label, pnl_pct, hold_sec, notes, extra)
            VALUES
                (%s,           %s,    %s,       %s,       %s,    %s::jsonb)
            RETURNING id
            """,
            (
                int(candidate_id),
                label,
                pnl_pct,
                hold_sec,
                notes,
                _jsonb(extra or {}),
            ),
        )
        (row_id,) = cur.fetchone()
        return int(row_id)


def mark_outcome_for_candidate(
    candidate_id: int,
    *,
    label: str,
    pnl_pct: Optional[float] = None,
    hold_sec: Optional[int] = None,
    notes: Optional[str] = None,
    extra: Optional[Mapping[str, Any]] = None,
) -> int:
    """
    Idempotent helper: if an outcome already exists for candidate_id,
    we update it; otherwise we insert a new one.
    """
    extra_json = _jsonb(extra or {})

    with conn_cursor() as (_, cur):
        cur.execute(
            """
            INSERT INTO entry_outcomes
                (candidate_id, label, pnl_pct, hold_sec, notes, extra)
            VALUES
                (%s,           %s,    %s,       %s,       %s,    %s::jsonb)
            ON CONFLICT (candidate_id)
            DO UPDATE SET
                label    = EXCLUDED.label,
                pnl_pct  = EXCLUDED.pnl_pct,
                hold_sec = EXCLUDED.hold_sec,
                notes    = EXCLUDED.notes,
                extra    = EXCLUDED.extra
            RETURNING id
            """,
            (
                int(candidate_id),
                label,
                pnl_pct,
                hold_sec,
                notes,
                extra_json,
            ),
        )
        (row_id,) = cur.fetchone()
        return int(row_id)


def fetch_recent_outcomes(
    *,
    limit: int = 100,
    since_candidate_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch recent outcomes, most recent first.
    """
    limit = max(1, min(int(limit), 1000))

    clauses: List[str] = []
    params: List[Any] = []

    if since_candidate_id is not None:
        clauses.append("candidate_id > %s")
        params.append(int(since_candidate_id))

    where_sql = ""
    if clauses:
        where_sql = "WHERE " + " AND ".join(clauses)

    sql = f"""
        SELECT id, candidate_id, label, pnl_pct, hold_sec, notes, extra, ts
        FROM entry_outcomes
        {where_sql}
        ORDER BY ts DESC, id DESC
        LIMIT %s
    """
    params.append(limit)

    out: List[Dict[str, Any]] = []
    with cursor_only() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    for r in rows:
        oid, cid, label, pnl, hold, notes, extra_json, ts = r
        if isinstance(extra_json, str):
            try:
                extra_obj = json.loads(extra_json)
            except Exception:
                extra_obj = {}
        else:
            extra_obj = extra_json or {}

        out.append(
            {
                "id": int(oid),
                "candidate_id": int(cid),
                "label": label,
                "pnl_pct": pnl,
                "hold_sec": hold,
                "notes": notes,
                "extra": extra_obj,
                "ts": ts,
            }
        )

    return out


# =============================================================================
# Executions & regime snapshots (for data_logger optional hooks)
# =============================================================================


def insert_trade_execution(
    *,
    trade_id: str,
    mint: str,
    side: str,  # "BUY" / "SELL"
    qty_raw: int,
    price_usd: float,
    slippage_bps: Optional[float] = None,
    fees_usd: Optional[float] = None,
    candidate_id: Optional[int] = None,
    session_id: Optional[str] = None,
    ts: Optional[Any] = None,
    extra: Optional[Mapping[str, Any]] = None,
) -> int:
    """
    Insert a row into trade_executions.

    Called by data_logger.log_trade_execution (via getattr) when enabled.
    """
    payload_extra = _jsonb(extra or {})
    with conn_cursor() as (_, cur):
        cur.execute(
            """
            INSERT INTO trade_executions
                (ts, trade_id, mint, side, qty_raw, price_usd,
                 slippage_bps, fees_usd, candidate_id, session_id, extra)
            VALUES
                (COALESCE(%s::timestamptz, now()), %s, %s, %s, %s, %s,
                 %s, %s, %s, %s, %s::jsonb)
            RETURNING id
            """,
            (
                ts,
                trade_id,
                mint,
                side,
                qty_raw,
                price_usd,
                slippage_bps,
                fees_usd,
                candidate_id,
                session_id,
                payload_extra,
            ),
        )
        (row_id,) = cur.fetchone()
        return int(row_id)


def fetch_recent_trade_executions(
    *,
    limit: int = 200,
    trade_id: Optional[str] = None,
    candidate_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch recent trade_executions rows for debugging / slippage stats.
    """
    limit = max(1, min(int(limit), 2000))

    clauses: List[str] = []
    params: List[Any] = []

    if trade_id:
        clauses.append("trade_id = %s")
        params.append(trade_id)
    if candidate_id is not None:
        clauses.append("candidate_id = %s")
        params.append(int(candidate_id))

    where_sql = ""
    if clauses:
        where_sql = "WHERE " + " AND ".join(clauses)

    sql = f"""
        SELECT id, ts, trade_id, mint, side, qty_raw, price_usd,
               slippage_bps, fees_usd, candidate_id, session_id, extra
        FROM trade_executions
        {where_sql}
        ORDER BY ts DESC, id DESC
        LIMIT %s
    """
    params.append(limit)

    out: List[Dict[str, Any]] = []
    with cursor_only() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    for r in rows:
        (
            eid,
            ts,
            t_id,
            mint_val,
            side_val,
            qty_raw,
            price_usd,
            slippage_bps,
            fees_usd,
            cid,
            session_id_val,
            extra_json,
        ) = r
        if isinstance(extra_json, str):
            try:
                extra_obj = json.loads(extra_json)
            except Exception:
                extra_obj = {}
        else:
            extra_obj = extra_json or {}

        out.append(
            {
                "id": int(eid),
                "ts": ts,
                "trade_id": t_id,
                "mint": mint_val,
                "side": side_val,
                "qty_raw": qty_raw,
                "price_usd": price_usd,
                "slippage_bps": slippage_bps,
                "fees_usd": fees_usd,
                "candidate_id": cid,
                "session_id": session_id_val,
                "extra": extra_obj,
            }
        )

    return out


def insert_regime_snapshot(
    *,
    regime: str,
    state: Mapping[str, Any],
    ts: Optional[Any] = None,
    session_id: Optional[str] = None,
    extra: Optional[Mapping[str, Any]] = None,
) -> int:
    """
    Insert a row into regime_snapshots.

    Called by data_logger.log_regime_snapshot (via getattr) when enabled.
    """
    with conn_cursor() as (_, cur):
        cur.execute(
            """
            INSERT INTO regime_snapshots
                (ts, regime, state, session_id, extra)
            VALUES
                (COALESCE(%s::timestamptz, now()), %s, %s::jsonb, %s, %s::jsonb)
            RETURNING id
            """,
            (
                ts,
                regime,
                _jsonb(state),
                session_id,
                _jsonb(extra or {}),
            ),
        )
        (row_id,) = cur.fetchone()
        return int(row_id)


def fetch_recent_regime_snapshots(
    *,
    limit: int = 200,
    regime: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch recent regime snapshots for dashboards / self-learning AI.
    """
    limit = max(1, min(int(limit), 2000))

    clauses: List[str] = []
    params: List[Any] = []

    if regime:
        clauses.append("regime = %s")
        params.append(regime)

    where_sql = ""
    if clauses:
        where_sql = "WHERE " + " AND ".join(clauses)

    sql = f"""
        SELECT id, ts, regime, state, session_id, extra
        FROM regime_snapshots
        {where_sql}
        ORDER BY ts DESC, id DESC
        LIMIT %s
    """
    params.append(limit)

    out: List[Dict[str, Any]] = []
    with cursor_only() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    for r in rows:
        eid, ts, regime_val, state_json, session_id_val, extra_json = r

        if isinstance(state_json, str):
            try:
                state_obj = json.loads(state_json)
            except Exception:
                state_obj = {}
        else:
            state_obj = state_json or {}

        if isinstance(extra_json, str):
            try:
                extra_obj = json.loads(extra_json)
            except Exception:
                extra_obj = {}
        else:
            extra_obj = extra_json or {}

        out.append(
            {
                "id": int(eid),
                "ts": ts,
                "regime": regime_val,
                "state": state_obj,
                "session_id": session_id_val,
                "extra": extra_obj,
            }
        )

    return out


# =============================================================================
# Quant / training helpers (joined candidates + outcomes)
# =============================================================================


def fetch_training_rows(
    *,
    limit: int = 2000,
    require_outcome: bool = True,
    min_feature_version: Optional[int] = None,
    src: Optional[str] = None,
    regime: Optional[str] = None,
    policy_version: Optional[str] = None,
    experiment_tag: Optional[str] = None,
    label_in: Optional[Sequence[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch joined candidate+outcome rows for training / backtests.

    Returns rows ordered newest→oldest with fields:
      {
        "id": candidate_id,
        "ts": ts,
        "src": ...,
        "regime": ...,
        "feature_version": ...,
        "features": {...},
        "raw": {...},
        "label": "win"/"loss"/... or None,
        "pnl_pct": float | None,
        "hold_sec": int | None,
        "has_outcome": bool,
      }
    """
    limit = max(1, min(int(limit), 10_000))

    clauses: List[str] = []
    params: List[Any] = []

    join_type = "JOIN" if require_outcome else "LEFT JOIN"

    if src:
        clauses.append("c.src = %s")
        params.append(src)
    if regime:
        clauses.append("c.regime = %s")
        params.append(regime)
    if min_feature_version is not None:
        clauses.append("c.feature_version >= %s")
        params.append(int(min_feature_version))
    if policy_version:
        clauses.append("c.raw->'_decision_meta'->>'ai_policy_version' = %s")
        params.append(policy_version)
    if experiment_tag:
        clauses.append("c.raw->'_decision_meta'->>'ai_experiment_tag' = %s")
        params.append(experiment_tag)
    if label_in:
        clauses.append("o.label = ANY(%s)")
        params.append(list(label_in))

    where_sql = ""
    if clauses:
        where_sql = "WHERE " + " AND ".join(clauses)

    sql = f"""
        SELECT
            c.id,
            c.ts,
            c.src,
            c.mint,
            c.symbol,
            c.regime,
            c.wallet_tag,
            c.feature_version,
            c.features,
            c.raw,
            o.label,
            o.pnl_pct,
            o.hold_sec
        FROM entry_candidates c
        {join_type} entry_outcomes o
            ON o.candidate_id = c.id
        {where_sql}
        ORDER BY c.ts DESC, c.id DESC
        LIMIT %s
    """
    params.append(limit)

    out: List[Dict[str, Any]] = []
    with cursor_only() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    for r in rows:
        (
            cid,
            ts,
            src_val,
            mint_val,
            symbol_val,
            regime_val,
            wallet_tag_val,
            feat_ver,
            features_json,
            raw_json,
            label,
            pnl_pct,
            hold_sec,
        ) = r

        if isinstance(features_json, str):
            try:
                features_obj = json.loads(features_json)
            except Exception:
                features_obj = {}
        else:
            features_obj = features_json or {}

        if isinstance(raw_json, str):
            try:
                raw_obj = json.loads(raw_json)
            except Exception:
                raw_obj = {}
        else:
            raw_obj = raw_json or {}

        has_outcome = label is not None

        out.append(
            {
                "id": int(cid),
                "ts": ts,
                "src": src_val,
                "mint": mint_val,
                "symbol": symbol_val,
                "regime": regime_val,
                "wallet_tag": wallet_tag_val,
                "feature_version": int(feat_ver),
                "features": features_obj,
                "raw": raw_obj,
                "label": label,
                "pnl_pct": pnl_pct,
                "hold_sec": hold_sec,
                "has_outcome": has_outcome,
            }
        )

    return out


def fetch_unlabeled_candidates(
    *,
    limit: int = 500,
    min_feature_version: Optional[int] = None,
    src: Optional[str] = None,
    regime: Optional[str] = None,
    min_age_seconds: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch candidates that do not yet have an outcome row.

    Useful for:
      - background workers that label "dead coins" / rugs
      - periodic training-label sweeps

    min_age_seconds:
      If provided, only return candidates with c.ts <= now() - min_age_seconds.
      This avoids labeling trades too early when PnL is still forming.
    """
    limit = max(1, min(int(limit), 5_000))

    clauses: List[str] = []
    params: List[Any] = []

    if src:
        clauses.append("c.src = %s")
        params.append(src)
    if regime:
        clauses.append("c.regime = %s")
        params.append(regime)
    if min_feature_version is not None:
        clauses.append("c.feature_version >= %s")
        params.append(int(min_feature_version))

    if min_age_seconds is not None and min_age_seconds > 0:
        clauses.append("c.ts <= now() - (%s::interval)")
        params.append(f"{int(min_age_seconds)} seconds")

    where_sql = ""
    if clauses:
        where_sql = "WHERE " + " AND ".join(clauses)

    # Ensure unlabeled filter is applied in WHERE
    if where_sql:
        where_sql += " AND o.candidate_id IS NULL"
    else:
        where_sql = "WHERE o.candidate_id IS NULL"

    sql = f"""
        SELECT
            c.id,
            c.ts,
            c.src,
            c.mint,
            c.symbol,
            c.regime,
            c.wallet_tag,
            c.feature_version,
            c.features,
            c.raw
        FROM entry_candidates c
        LEFT JOIN entry_outcomes o
            ON o.candidate_id = c.id
        {where_sql}
        ORDER BY c.ts DESC, c.id DESC
        LIMIT %s
    """
    params.append(limit)

    out: List[Dict[str, Any]] = []
    with cursor_only() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    for r in rows:
        (
            cid,
            ts,
            src_val,
            mint_val,
            symbol_val,
            regime_val,
            wallet_tag_val,
            feat_ver,
            features_json,
            raw_json,
        ) = r

        if isinstance(features_json, str):
            try:
                features_obj = json.loads(features_json)
            except Exception:
                features_obj = {}
        else:
            features_obj = features_json or {}

        if isinstance(raw_json, str):
            try:
                raw_obj = json.loads(raw_json)
            except Exception:
                raw_obj = {}
        else:
            raw_obj = raw_json or {}

        out.append(
            {
                "id": int(cid),
                "ts": ts,
                "src": src_val,
                "mint": mint_val,
                "symbol": symbol_val,
                "regime": regime_val,
                "wallet_tag": wallet_tag_val,
                "feature_version": int(feat_ver),
                "features": features_obj,
                "raw": raw_obj,
            }
        )

    return out


# =============================================================================
# Backtest iterators (used by backtest_engine.run_backtest)
# =============================================================================


def _parse_iso_dt(val: Any) -> Optional[datetime]:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    s = str(val)
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def iter_backtest_rows(config: Mapping[str, Any]) -> Iterator[Mapping[str, Any]]:
    """
    Yield normalized candidate+outcome rows for backtesting.

    This is designed to match backtest_engine._normalize_row expectations:

        - "features"        : dict
        - "pnl_pct"         : float
        - "label"           : str (optional)
        - "max_dd_pct"      : float (optional, from outcome.extra->'max_dd_pct')
        - "hold_sec"        : float/int (optional)
        - "created_at"      : datetime (candidate ts)
        - "mint"            : str (token mint)

    Supported config keys (optional):
        - limit / BT_MAX_ROWS
        - days_back
        - start_ts / end_ts (ISO strings)
        - min_feature_version / BT_MIN_FEATURE_VERSION
        - src, regime, wallet_tag
        - policy_version, experiment_tag
        - label_in, label_not_in
        - min_pnl_pct, max_pnl_pct
        - require_outcome / BT_REQUIRE_OUTCOME
    """
    # Limit rows for safety
    limit = int(config.get("BT_MAX_ROWS") or config.get("limit") or 50_000)
    limit = max(1, min(limit, 200_000))

    days_back = config.get("days_back")
    start_ts_raw = config.get("start_ts")
    end_ts_raw = config.get("end_ts")

    min_feature_version = (
        config.get("BT_MIN_FEATURE_VERSION")
        if config.get("BT_MIN_FEATURE_VERSION") is not None
        else config.get("min_feature_version")
    )

    src = config.get("src")
    regime = config.get("regime")
    wallet_tag = config.get("wallet_tag")
    policy_version = config.get("policy_version")
    experiment_tag = config.get("experiment_tag")
    label_in = config.get("label_in")
    label_not_in = config.get("label_not_in")

    min_pnl_pct = config.get("min_pnl_pct")
    max_pnl_pct = config.get("max_pnl_pct")

    # Respect explicit overrides correctly:
    #   - BT_REQUIRE_OUTCOME (if present) wins
    #   - then require_outcome
    #   - default: True
    if "BT_REQUIRE_OUTCOME" in config:
        require_outcome = bool(config.get("BT_REQUIRE_OUTCOME"))
    elif "require_outcome" in config:
        require_outcome = bool(config.get("require_outcome"))
    else:
        require_outcome = True

    join_type = "JOIN" if require_outcome else "LEFT JOIN"

    clauses: List[str] = []
    params: List[Any] = []

    if days_back is not None:
        try:
            db_int = int(days_back)
            db_int = max(1, min(db_int, 365))
            clauses.append("c.ts >= now() - (%s::interval)")
            params.append(f"{db_int} days")
        except Exception:
            pass

    if start_ts_raw:
        start_dt = _parse_iso_dt(start_ts_raw)
        if start_dt is not None:
            clauses.append("c.ts >= %s")
            params.append(start_dt)
    if end_ts_raw:
        end_dt = _parse_iso_dt(end_ts_raw)
        if end_dt is not None:
            clauses.append("c.ts <= %s")
            params.append(end_dt)

    if src:
        clauses.append("c.src = %s")
        params.append(src)
    if regime:
        clauses.append("c.regime = %s")
        params.append(regime)
    if wallet_tag:
        clauses.append("c.wallet_tag = %s")
        params.append(wallet_tag)
    if min_feature_version is not None:
        try:
            fv = int(min_feature_version)
            clauses.append("c.feature_version >= %s")
            params.append(fv)
        except Exception:
            pass
    if policy_version:
        clauses.append("c.raw->'_decision_meta'->>'ai_policy_version' = %s")
        params.append(policy_version)
    if experiment_tag:
        clauses.append("c.raw->'_decision_meta'->>'ai_experiment_tag' = %s")
        params.append(experiment_tag)

    if label_in:
        clauses.append("o.label = ANY(%s)")
        params.append(list(label_in))
    if label_not_in:
        clauses.append("NOT (o.label = ANY(%s))")
        params.append(list(label_not_in))

    if min_pnl_pct is not None:
        try:
            clauses.append("o.pnl_pct >= %s")
            params.append(float(min_pnl_pct))
        except Exception:
            pass
    if max_pnl_pct is not None:
        try:
            clauses.append("o.pnl_pct <= %s")
            params.append(float(max_pnl_pct))
        except Exception:
            pass

    where_sql = ""
    if clauses:
        where_sql = "WHERE " + " AND ".join(clauses)

    sql = f"""
        SELECT
            c.id,
            c.ts,
            c.src,
            c.mint,
            c.symbol,
            c.regime,
            c.wallet_tag,
            c.feature_version,
            c.features,
            c.raw,
            o.label,
            o.pnl_pct,
            o.hold_sec,
            (o.extra->>'max_dd_pct')::double precision AS max_dd_pct
        FROM entry_candidates c
        {join_type} entry_outcomes o
          ON o.candidate_id = c.id
        {where_sql}
        ORDER BY c.ts ASC, c.id ASC
        LIMIT %s
    """
    params.append(limit)

    with cursor_only() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    for r in rows:
        (
            cid,
            ts,
            src_val,
            mint_val,
            symbol_val,
            regime_val,
            wallet_tag_val,
            feat_ver,
            features_json,
            raw_json,
            label,
            pnl_pct,
            hold_sec,
            max_dd_pct,
        ) = r

        if isinstance(features_json, str):
            try:
                features_obj = json.loads(features_json)
            except Exception:
                features_obj = {}
        else:
            features_obj = features_json or {}

        if isinstance(raw_json, str):
            try:
                raw_obj = json.loads(raw_json)
            except Exception:
                raw_obj = {}
        else:
            raw_obj = raw_json or {}

        yield {
            "id": int(cid),
            "created_at": ts,
            "ts": ts,  # alias
            "src": src_val,
            "mint": mint_val,
            "symbol": symbol_val,
            "regime": regime_val,
            "wallet_tag": wallet_tag_val,
            "feature_version": int(feat_ver),
            "features": features_obj,
            "raw": raw_obj,
            "label": label,
            "pnl_pct": float(pnl_pct) if pnl_pct is not None else None,
            "hold_sec": float(hold_sec) if hold_sec is not None else None,
            "max_dd_pct": float(max_dd_pct) if max_dd_pct is not None else None,
        }


def iter_training_rows(config: Mapping[str, Any]) -> Iterator[Mapping[str, Any]]:
    """
    Thin wrapper exposing fetch_training_rows as an iterator.

    Supported config keys (subset):
        - limit
        - require_outcome
        - min_feature_version / BT_MIN_FEATURE_VERSION
        - src, regime
        - policy_version, experiment_tag
        - label_in
    """
    limit = int(config.get("limit") or 2000)
    limit = max(1, min(limit, 10_000))

    min_feature_version = (
        config.get("BT_MIN_FEATURE_VERSION")
        if config.get("BT_MIN_FEATURE_VERSION") is not None
        else config.get("min_feature_version")
    )

    rows = fetch_training_rows(
        limit=limit,
        require_outcome=bool(config.get("require_outcome", True)),
        min_feature_version=min_feature_version,
        src=config.get("src"),
        regime=config.get("regime"),
        policy_version=config.get("policy_version"),
        experiment_tag=config.get("experiment_tag"),
        label_in=config.get("label_in"),
    )

    for row in rows:
        yield row


# =============================================================================
# Quant-grade stats helpers
# =============================================================================

# Env-configurable mapping of "win labels"
# Example: AI_DB_WIN_LABELS="good,win,green"
_WIN_LABELS_DEFAULT = {"good", "win", "green", "profit"}
_AI_DB_WIN_LABELS_ENV = os.getenv("AI_DB_WIN_LABELS", "")
if _AI_DB_WIN_LABELS_ENV.strip():
    WIN_LABELS: set[str] = {
        s.strip().lower()
        for s in _AI_DB_WIN_LABELS_ENV.split(",")
        if s.strip()
    }
else:
    WIN_LABELS = _WIN_LABELS_DEFAULT


def _is_win_label(label: Optional[str]) -> Optional[bool]:
    if label is None:
        return None
    lbl = label.strip().lower()
    if not lbl:
        return None
    return lbl in WIN_LABELS


def fetch_basic_stats(
    *,
    days_back: int = 7,
    src: Optional[str] = None,
    regime: Optional[str] = None,
    min_feature_version: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Quant-grade helper: returns basic aggregated stats for a time window.

    Output example:
      {
        "n_trades": 123,
        "n_labeled": 120,
        "win_labels": ["good","win","green","profit"],
        "n_wins": 70,
        "win_rate": 0.5833,
        "avg_pnl_pct": 5.2,
        "median_pnl_pct": 4.1,
        "avg_hold_sec": 620,
      }
    """
    days_back = max(1, min(int(days_back), 365))

    clauses: List[str] = ["o.ts >= now() - (%s::interval)"]
    params: List[Any] = [f"{days_back} days"]

    if src:
        clauses.append("c.src = %s")
        params.append(src)
    if regime:
        clauses.append("c.regime = %s")
        params.append(regime)
    if min_feature_version is not None:
        clauses.append("c.feature_version >= %s")
        params.append(int(min_feature_version))

    where_sql = "WHERE " + " AND ".join(clauses)

    sql = f"""
        SELECT
            count(*) AS n_total,
            count(o.pnl_pct) AS n_with_pnl,
            avg(o.pnl_pct) AS avg_pnl_pct,
            percentile_disc(0.5)
                WITHIN GROUP (ORDER BY o.pnl_pct) AS median_pnl_pct,
            avg(o.hold_sec) AS avg_hold_sec,
            array_agg(o.label) AS labels
        FROM entry_outcomes o
        JOIN entry_candidates c
          ON c.id = o.candidate_id
        {where_sql}
    """

    with cursor_only() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()

    if not row:
        return {
            "n_trades": 0,
            "n_labeled": 0,
            "win_labels": sorted(WIN_LABELS),
            "n_wins": 0,
            "win_rate": 0.0,
            "avg_pnl_pct": None,
            "median_pnl_pct": None,
            "avg_hold_sec": None,
        }

    n_total, _n_with_pnl, avg_pnl_pct, median_pnl_pct, avg_hold_sec, labels_arr = row

    labels_list = labels_arr or []
    n_labeled = len([l for l in labels_list if l is not None])

    n_wins = 0
    for l in labels_list:
        if _is_win_label(l):
            n_wins += 1

    win_rate = float(n_wins) / float(n_labeled) if n_labeled else 0.0

    return {
        "n_trades": int(n_total or 0),
        "n_labeled": int(n_labeled or 0),
        "win_labels": sorted(WIN_LABELS),
        "n_wins": int(n_wins),
        "win_rate": win_rate,
        "avg_pnl_pct": float(avg_pnl_pct) if avg_pnl_pct is not None else None,
        "median_pnl_pct": float(median_pnl_pct)
        if median_pnl_pct is not None
        else None,
        "avg_hold_sec": float(avg_hold_sec) if avg_hold_sec is not None else None,
    }


def fetch_label_distribution(
    *,
    days_back: int = 30,
    src: Optional[str] = None,
    regime: Optional[str] = None,
    min_feature_version: Optional[int] = None,
) -> Dict[str, int]:
    """
    Return a label -> count mapping over a given window.

    This is handy for quickly seeing how many "mega_win"/"disaster"/etc
    examples you have for training.
    """
    days_back = max(1, min(int(days_back), 365))

    clauses: List[str] = ["o.ts >= now() - (%s::interval)"]
    params: List[Any] = [f"{days_back} days"]]

    if src:
        clauses.append("c.src = %s")
        params.append(src)
    if regime:
        clauses.append("c.regime = %s")
        params.append(regime)
    if min_feature_version is not None:
        clauses.append("c.feature_version >= %s")
        params.append(int(min_feature_version))

    where_sql = "WHERE " + " AND ".join(clauses)

    sql = f"""
        SELECT o.label, count(*) AS n
        FROM entry_outcomes o
        JOIN entry_candidates c
          ON c.id = o.candidate_id
        {where_sql}
        GROUP BY o.label
    """

    out: Dict[str, int] = {}
    with cursor_only() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    for label, n in rows:
        out[str(label)] = int(n)

    return out


def fetch_perf_by_regime(
    *,
    days_back: int = 30,
    min_feature_version: Optional[int] = None,
    src: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Optional quant helper: PnL & win-rate per regime.
    """
    days_back = max(1, min(int(days_back), 365))

    clauses: List[str] = ["o.ts >= now() - (%s::interval)"]
    params: List[Any] = [f"{days_back} days"]

    if src:
        clauses.append("c.src = %s")
        params.append(src)
    if min_feature_version is not None:
        clauses.append("c.feature_version >= %s")
        params.append(int(min_feature_version))

    where_sql = "WHERE " + " AND ".join(clauses)

    sql = f"""
        SELECT
            c.regime,
            count(*) AS n_trades,
            avg(o.pnl_pct) AS avg_pnl_pct,
            percentile_disc(0.5)
                WITHIN GROUP (ORDER BY o.pnl_pct) AS median_pnl_pct,
            array_agg(o.label) AS labels
        FROM entry_outcomes o
        JOIN entry_candidates c
          ON c.id = o.candidate_id
        {where_sql}
        GROUP BY c.regime
        ORDER BY c.regime
    """

    out: List[Dict[str, Any]] = []
    with cursor_only() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    for regime_val, n_trades, avg_pnl_pct, median_pnl_pct, labels_arr in rows:
        labels_list = labels_arr or []
        n_labeled = len([l for l in labels_list if l is not None])
        n_wins = 0
        for l in labels_list:
            if _is_win_label(l):
                n_wins += 1
        win_rate = float(n_wins) / float(n_labeled) if n_labeled else 0.0

        out.append(
            {
                "regime": regime_val,
                "n_trades": int(n_trades or 0),
                "avg_pnl_pct": float(avg_pnl_pct) if avg_pnl_pct is not None else None,
                "median_pnl_pct": float(median_pnl_pct)
                if median_pnl_pct is not None
                else None,
                "n_wins": int(n_wins),
                "win_rate": win_rate,
            }
        )

    return out


def fetch_perf_by_src(
    *,
    days_back: int = 30,
    min_feature_version: Optional[int] = None,
    regime: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Optional quant helper: PnL & win-rate per src (scanner).
    """
    days_back = max(1, min(int(days_back), 365))

    clauses: List[str] = ["o.ts >= now() - (%s::interval)"]
    params: List[Any] = [f"{days_back} days"]

    if regime:
        clauses.append("c.regime = %s")
        params.append(regime)
    if min_feature_version is not None:
        clauses.append("c.feature_version >= %s")
        params.append(int(min_feature_version))

    where_sql = "WHERE " + " AND ".join(clauses)

    sql = f"""
        SELECT
            c.src,
            count(*) AS n_trades,
            avg(o.pnl_pct) AS avg_pnl_pct,
            percentile_disc(0.5)
                WITHIN GROUP (ORDER BY o.pnl_pct) AS median_pnl_pct,
            array_agg(o.label) AS labels
        FROM entry_outcomes o
        JOIN entry_candidates c
          ON c.id = o.candidate_id
        {where_sql}
        GROUP BY c.src
        ORDER BY c.src
    """

    out: List[Dict[str, Any]] = []
    with cursor_only() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    for src_val, n_trades, avg_pnl_pct, median_pnl_pct, labels_arr in rows:
        labels_list = labels_arr or []
        n_labeled = len([l for l in labels_list if l is not None])
        n_wins = 0
        for l in labels_list:
            if _is_win_label(l):
                n_wins += 1
        win_rate = float(n_wins) / float(n_labeled) if n_labeled else 0.0

        out.append(
            {
                "src": src_val,
                "n_trades": int(n_trades or 0),
                "avg_pnl_pct": float(avg_pnl_pct) if avg_pnl_pct is not None else None,
                "median_pnl_pct": float(median_pnl_pct)
                if median_pnl_pct is not None
                else None,
                "n_wins": int(n_wins),
                "win_rate": win_rate,
            }
        )

    return out


def fetch_outcome_coverage(
    *,
    days_back: int = 7,
    min_feature_version: Optional[int] = None,
    src: Optional[str] = None,
    regime: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Quant helper: what fraction of candidates have outcomes in a window?

    Used by self-learning / monitoring to see if labeling pipelines are healthy.

    Returns:
      {
        "days_back": int,
        "n_candidates": int,
        "n_labeled": int,
        "coverage": float,   # [0,1]
      }
    """
    days_back = max(1, min(int(days_back), 365))

    clauses: List[str] = ["c.ts >= now() - (%s::interval)"]
    params: List[Any] = [f"{days_back} days"]

    if src:
        clauses.append("c.src = %s")
        params.append(src)
    if regime:
        clauses.append("c.regime = %s")
        params.append(regime)
    if min_feature_version is not None:
        clauses.append("c.feature_version >= %s")
        params.append(int(min_feature_version))

    where_sql = "WHERE " + " AND ".join(clauses)

    sql = f"""
        SELECT
            count(*) AS n_candidates,
            count(o.id) AS n_labeled
        FROM entry_candidates c
        LEFT JOIN entry_outcomes o
          ON o.candidate_id = c.id
        {where_sql}
    """

    with cursor_only() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()

    if not row:
        return {
            "days_back": days_back,
            "n_candidates": 0,
            "n_labeled": 0,
            "coverage": 0.0,
        }

    n_candidates, n_labeled = row
    n_candidates = int(n_candidates or 0)
    n_labeled = int(n_labeled or 0)
    coverage = float(n_labeled) / float(n_candidates) if n_candidates else 0.0

    return {
        "days_back": days_back,
        "n_candidates": n_candidates,
        "n_labeled": n_labeled,
        "coverage": coverage,
    }


def fetch_perf_by_policy_version(
    *,
    days_back: int = 30,
    min_feature_version: Optional[int] = None,
    src: Optional[str] = None,
    regime: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Quant helper: PnL & win-rate per ai_policy_version.

    Used by self_learning_ai_strategy to compare policy variants.
    """
    days_back = max(1, min(int(days_back), 365))

    clauses: List[str] = ["o.ts >= now() - (%s::interval)"]
    params: List[Any] = [f"{days_back} days"]

    if src:
        clauses.append("c.src = %s")
        params.append(src)
    if regime:
        clauses.append("c.regime = %s")
        params.append(regime)
    if min_feature_version is not None:
        clauses.append("c.feature_version >= %s")
        params.append(int(min_feature_version))

    where_sql = "WHERE " + " AND ".join(clauses)

    sql = f"""
        SELECT
            c.raw->'_decision_meta'->>'ai_policy_version' AS policy_version,
            count(*) AS n_trades,
            avg(o.pnl_pct) AS avg_pnl_pct,
            percentile_disc(0.5)
                WITHIN GROUP (ORDER BY o.pnl_pct) AS median_pnl_pct,
            array_agg(o.label) AS labels
        FROM entry_outcomes o
        JOIN entry_candidates c
          ON c.id = o.candidate_id
        {where_sql}
        GROUP BY c.raw->'_decision_meta'->>'ai_policy_version'
        ORDER BY policy_version
    """

    out: List[Dict[str, Any]] = []
    with cursor_only() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    for policy_version, n_trades, avg_pnl_pct, median_pnl_pct, labels_arr in rows:
        labels_list = labels_arr or []
        n_labeled = len([l for l in labels_list if l is not None])
        n_wins = 0
        for l in labels_list:
            if _is_win_label(l):
                n_wins += 1
        win_rate = float(n_wins) / float(n_labeled) if n_labeled else 0.0

        out.append(
            {
                "policy_version": policy_version,
                "n_trades": int(n_trades or 0),
                "avg_pnl_pct": float(avg_pnl_pct) if avg_pnl_pct is not None else None,
                "median_pnl_pct": float(median_pnl_pct)
                if median_pnl_pct is not None
                else None,
                "n_wins": int(n_wins),
                "win_rate": win_rate,
            }
        )

    return out


def fetch_perf_by_experiment_tag(
    *,
    days_back: int = 30,
    min_feature_version: Optional[int] = None,
    src: Optional[str] = None,
    regime: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Quant helper: PnL & win-rate per ai_experiment_tag.

    Lets you A/B compare experiment cohorts ("A", "B", "golden", etc).
    """
    days_back = max(1, min(int(days_back), 365))

    clauses: List[str] = ["o.ts >= now() - (%s::interval)"]
    params: List[Any] = [f"{days_back} days"]

    if src:
        clauses.append("c.src = %s")
        params.append(src)
    if regime:
        clauses.append("c.regime = %s")
        params.append(regime)
    if min_feature_version is not None:
        clauses.append("c.feature_version >= %s")
        params.append(int(min_feature_version))

    where_sql = "WHERE " + " AND ".join(clauses)

    sql = f"""
        SELECT
            c.raw->'_decision_meta'->>'ai_experiment_tag' AS experiment_tag,
            count(*) AS n_trades,
            avg(o.pnl_pct) AS avg_pnl_pct,
            percentile_disc(0.5)
                WITHIN GROUP (ORDER BY o.pnl_pct) AS median_pnl_pct,
            array_agg(o.label) AS labels
        FROM entry_outcomes o
        JOIN entry_candidates c
          ON c.id = o.candidate_id
        {where_sql}
        GROUP BY c.raw->'_decision_meta'->>'ai_experiment_tag'
        ORDER BY experiment_tag
    """

    out: List[Dict[str, Any]] = []
    with cursor_only() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    for exp_tag, n_trades, avg_pnl_pct, median_pnl_pct, labels_arr in rows:
        labels_list = labels_arr or []
        n_labeled = len([l for l in labels_list if l is not None])
        n_wins = 0
        for l in labels_list:
            if _is_win_label(l):
                n_wins += 1
        win_rate = float(n_wins) / float(n_labeled) if n_labeled else 0.0

        out.append(
            {
                "experiment_tag": exp_tag,
                "n_trades": int(n_trades or 0),
                "avg_pnl_pct": float(avg_pnl_pct) if avg_pnl_pct is not None else None,
                "median_pnl_pct": float(median_pnl_pct)
                if median_pnl_pct is not None
                else None,
                "n_wins": int(n_wins),
                "win_rate": win_rate,
            }
        )

    return out


# =============================================================================
# V3+++ extras: equity curve / drawdown / label quality / AI events
# =============================================================================


def fetch_daily_equity_curve(
    *,
    days_back: int = 90,
    src: Optional[str] = None,
    regime: Optional[str] = None,
    min_feature_version: Optional[int] = None,
    base_equity: float = 1.0,
) -> List[Dict[str, Any]]:
    """
    Approximate daily equity curve from realized PnL%.

    For each day we compute:
        - n_trades
        - avg_pnl_pct
        - sum_pnl_pct
    Then we compound equity as:
        equity_t = equity_{t-1} * (1 + avg_pnl_pct / 100)

    This is *approximate* (it assumes "1x" normalized exposure per day),
    but it's super useful for dashboards / regime health.

    Returns ascending by date:
      [
        {
          "date": "2025-01-01",
          "n_trades": 12,
          "avg_pnl_pct": 3.1,
          "sum_pnl_pct": 37.2,
          "equity": 1.031,
        },
        ...
      ]
    """
    days_back = max(1, min(int(days_back), 730))

    clauses: List[str] = ["o.ts >= now() - (%s::interval)"]
    params: List[Any] = [f"{days_back} days"]

    if src:
        clauses.append("c.src = %s")
        params.append(src)
    if regime:
        clauses.append("c.regime = %s")
        params.append(regime)
    if min_feature_version is not None:
        clauses.append("c.feature_version >= %s")
        params.append(int(min_feature_version))

    where_sql = "WHERE " + " AND ".join(clauses)

    sql = f"""
        SELECT
            date(o.ts) AS d,
            count(*) AS n_trades,
            avg(o.pnl_pct) AS avg_pnl_pct,
            sum(o.pnl_pct) AS sum_pnl_pct
        FROM entry_outcomes o
        JOIN entry_candidates c
          ON c.id = o.candidate_id
        {where_sql}
        GROUP BY date(o.ts)
        ORDER BY date(o.ts) ASC
    """

    rows_out: List[Dict[str, Any]] = []
    with cursor_only() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    equity = float(base_equity)
    for d, n_trades, avg_pnl_pct, sum_pnl_pct in rows:
        avg_pnl = float(avg_pnl_pct or 0.0)
        # approximate compounding
        equity *= (1.0 + avg_pnl / 100.0)
        rows_out.append(
            {
                "date": d.isoformat() if hasattr(d, "isoformat") else str(d),
                "n_trades": int(n_trades or 0),
                "avg_pnl_pct": avg_pnl,
                "sum_pnl_pct": float(sum_pnl_pct or 0.0),
                "equity": equity,
            }
        )

    return rows_out


def fetch_drawdown_stats(
    *,
    days_back: int = 30,
    src: Optional[str] = None,
    regime: Optional[str] = None,
    min_feature_version: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Quant helper: summarize realized per-trade drawdowns from outcome.extra.

    Expects extra->'max_dd_pct' per trade (as logged by exit_engine /
    risk_manager / data_logger).

    Returns something like:
      {
        "days_back": 30,
        "n_trades": 120,
        "avg_dd_pct": 8.3,
        "p95_dd_pct": 19.7,
        "p99_dd_pct": 28.4,
      }
    """
    days_back = max(1, min(int(days_back), 365))

    clauses: List[str] = ["o.ts >= now() - (%s::interval)"]
    params: List[Any] = [f"{days_back} days"]

    if src:
        clauses.append("c.src = %s")
        params.append(src)
    if regime:
        clauses.append("c.regime = %s")
        params.append(regime)
    if min_feature_version is not None:
        clauses.append("c.feature_version >= %s")
        params.append(int(min_feature_version))

    # Only include rows that actually have a numeric max_dd_pct
    clauses.append("(o.extra->>'max_dd_pct') IS NOT NULL")

    where_sql = "WHERE " + " AND ".join(clauses)

    sql = f"""
        SELECT
            count(*) AS n_trades,
            avg((o.extra->>'max_dd_pct')::double precision) AS avg_dd_pct,
            percentile_disc(0.95)
                WITHIN GROUP (ORDER BY (o.extra->>'max_dd_pct')::double precision)
                AS p95_dd_pct,
            percentile_disc(0.99)
                WITHIN GROUP (ORDER BY (o.extra->>'max_dd_pct')::double precision)
                AS p99_dd_pct
        FROM entry_outcomes o
        JOIN entry_candidates c
          ON c.id = o.candidate_id
        {where_sql}
    """

    with cursor_only() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()

    if not row:
        return {
            "days_back": days_back,
            "n_trades": 0,
            "avg_dd_pct": None,
            "p95_dd_pct": None,
            "p99_dd_pct": None,
        }

    n_trades, avg_dd_pct, p95_dd_pct, p99_dd_pct = row
    return {
        "days_back": days_back,
        "n_trades": int(n_trades or 0),
        "avg_dd_pct": float(avg_dd_pct) if avg_dd_pct is not None else None,
        "p95_dd_pct": float(p95_dd_pct) if p95_dd_pct is not None else None,
        "p99_dd_pct": float(p99_dd_pct) if p99_dd_pct is not None else None,
    }


def fetch_label_quality_stats(
    *,
    days_back: int = 60,
    src: Optional[str] = None,
    regime: Optional[str] = None,
    min_feature_version: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Diagnostic helper: how "good" is each label in terms of PnL and frequency?

    This is super useful when designing label schemes like:
        - mega_win, win, scratch, loss, disaster

    Returns rows like:
      {
        "label": "mega_win",
        "n": 42,
        "share": 0.18,
        "avg_pnl_pct": 63.2,
        "median_pnl_pct": 57.1,
        "avg_hold_sec": 320,
        "is_win_label": true,
      }
    """
    days_back = max(1, min(int(days_back), 365))

    clauses: List[str] = ["o.ts >= now() - (%s::interval)"]
    params: List[Any] = [f"{days_back} days"]

    if src:
        clauses.append("c.src = %s")
        params.append(src)
    if regime:
        clauses.append("c.regime = %s")
        params.append(regime)
    if min_feature_version is not None:
        clauses.append("c.feature_version >= %s")
        params.append(int(min_feature_version))

    where_sql = "WHERE " + " AND ".join(clauses)

    sql_total = f"""
        SELECT count(*) AS n_total
        FROM entry_outcomes o
        JOIN entry_candidates c
          ON c.id = o.candidate_id
        {where_sql}
    """

    sql = f"""
        SELECT
            o.label,
            count(*) AS n,
            avg(o.pnl_pct) AS avg_pnl_pct,
            percentile_disc(0.5)
                WITHIN GROUP (ORDER BY o.pnl_pct) AS median_pnl_pct,
            avg(o.hold_sec) AS avg_hold_sec
        FROM entry_outcomes o
        JOIN entry_candidates c
          ON c.id = o.candidate_id
        {where_sql}
        GROUP BY o.label
        ORDER BY o.label
    """

    with cursor_only() as cur:
        cur.execute(sql_total, params)
        total_row = cur.fetchone()
        n_total = int(total_row[0] or 0) if total_row else 0

    out: List[Dict[str, Any]] = []
    if n_total == 0:
        return out

    with cursor_only() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    for label, n, avg_pnl_pct, median_pnl_pct, avg_hold_sec in rows:
        lbl = str(label)
        is_win = bool(_is_win_label(lbl))
        out.append(
            {
                "label": lbl,
                "n": int(n or 0),
                "share": float(n or 0) / float(n_total) if n_total else 0.0,
                "avg_pnl_pct": float(avg_pnl_pct) if avg_pnl_pct is not None else None,
                "median_pnl_pct": float(median_pnl_pct)
                if median_pnl_pct is not None
                else None,
                "avg_hold_sec": float(avg_hold_sec) if avg_hold_sec is not None else None,
                "is_win_label": is_win,
            }
        )

    return out


# =============================================================================
# AI events logging (hook for self_learning_ai_strategy / monitoring)
# =============================================================================


def log_ai_event(
    *,
    event_type: str,
    policy_version: Optional[str] = None,
    experiment_tag: Optional[str] = None,
    src: Optional[str] = None,
    regime: Optional[str] = None,
    payload: Optional[Mapping[str, Any]] = None,
) -> int:
    """
    Lightweight log of AI-related events:

      - parameter_tune
      - risk_halt_triggered
      - risk_halt_cleared
      - reinvestor_soft_stop
      - reinvestor_hard_stop
      - strategy_health_snapshot
      - backtest_best_config_selected
      - etc.

    The self_learning_ai_strategy, risk_manager, reinvestor, or backtest drivers
    can call this to create a durable audit trail of high-level events.

    Returns the newly inserted ai_events.id.
    """
    payload_json = _jsonb(payload or {})

    with conn_cursor() as (_, cur):
        cur.execute(
            """
            INSERT INTO ai_events
                (event_type, policy_version, experiment_tag, src, regime, payload)
            VALUES
                (%s,         %s,             %s,             %s,  %s,     %s::jsonb)
            RETURNING id
            """,
            (
                event_type,
                policy_version,
                experiment_tag,
                src,
                regime,
                payload_json,
            ),
        )
        (row_id,) = cur.fetchone()
        return int(row_id)


def fetch_recent_ai_events(
    *,
    limit: int = 200,
    event_type: Optional[str] = None,
    policy_version: Optional[str] = None,
    experiment_tag: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch recent AI events for dashboards / debugging.

    Returns newest→oldest events.
    """
    limit = max(1, min(int(limit), 2000))

    clauses: List[str] = []
    params: List[Any] = []

    if event_type:
        clauses.append("event_type = %s")
        params.append(event_type)
    if policy_version:
        clauses.append("policy_version = %s")
        params.append(policy_version)
    if experiment_tag:
        clauses.append("experiment_tag = %s")
        params.append(experiment_tag)

    where_sql = ""
    if clauses:
        where_sql = "WHERE " + " AND ".join(clauses)

    sql = f"""
        SELECT id, ts, event_type, policy_version, experiment_tag,
               src, regime, payload
        FROM ai_events
        {where_sql}
        ORDER BY ts DESC, id DESC
        LIMIT %s
    """
    params.append(limit)

    out: List[Dict[str, Any]] = []
    with cursor_only() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    for r in rows:
        (
            eid,
            ts,
            ev_type,
            policy_ver,
            exp_tag,
            src_val,
            regime_val,
            payload_json,
        ) = r
        if isinstance(payload_json, str):
            try:
                payload_obj = json.loads(payload_json)
            except Exception:
                payload_obj = {}
        else:
            payload_obj = payload_json or {}

        out.append(
            {
                "id": int(eid),
                "ts": ts,
                "event_type": ev_type,
                "policy_version": policy_ver,
                "experiment_tag": exp_tag,
                "src": src_val,
                "regime": regime_val,
                "payload": payload_obj,
            }
        )

    return out


# =============================================================================
# V3+++ small extras: housekeeping & combined health snapshot
# =============================================================================


def prune_old_ai_data(*, max_age_days: int = 365) -> int:
    """
    Optional housekeeping: prune very old AI data to keep the DB lean.

    This deletes *candidates* older than `max_age_days`. Thanks to
    ON DELETE CASCADE on entry_outcomes.candidate_id, their outcomes are
    also removed. trade_executions keep their rows but candidate_id is
    already ON DELETE SET NULL, so you retain execution history.

    Returns: number of entry_candidates rows deleted (best-effort).
    """
    max_age_days = max(1, int(max_age_days))

    sql = """
        WITH deleted AS (
            DELETE FROM entry_candidates
            WHERE ts < now() - (%s::interval)
            RETURNING 1
        )
        SELECT count(*) FROM deleted
    """
    with cursor_only() as cur:
        cur.execute(sql, (f"{max_age_days} days",))
        row = cur.fetchone()
    return int(row[0] or 0) if row else 0


def fetch_overall_health_snapshot(
    *,
    days_back_core: int = 7,
    days_back_dd: int = 30,
    days_back_coverage: int = 7,
    src: Optional[str] = None,
    regime: Optional[str] = None,
    min_feature_version: Optional[int] = None,
) -> Dict[str, Any]:
    """
    High-level "one shot" health snapshot used by self_learning_ai_strategy
    and dashboards.

    It composes:
      - basic_stats        (PnL, win-rate, avg hold)
      - outcome_coverage   (how many candidates have labels)
      - drawdown_stats     (avg / p95 / p99 per-trade DD)

    So your AI brain can quickly answer:
      • Are we making money?
      • Are labels healthy and dense?
      • Are tails (DD) under control?
    """
    basic = fetch_basic_stats(
        days_back=days_back_core,
        src=src,
        regime=regime,
        min_feature_version=min_feature_version,
    )
    coverage = fetch_outcome_coverage(
        days_back=days_back_coverage,
        src=src,
        regime=regime,
        min_feature_version=min_feature_version,
    )
    dd_stats = fetch_drawdown_stats(
        days_back=days_back_dd,
        src=src,
        regime=regime,
        min_feature_version=min_feature_version,
    )

    return {
        "basic_stats": basic,
        "coverage": coverage,
        "drawdown": dd_stats,
    }


# =============================================================================
# V3+++: Kelly / Sharpe-style helpers for risk_manager & reinvestor
# =============================================================================


def fetch_kelly_stats(
    *,
    days_back: int = 60,
    src: Optional[str] = None,
    regime: Optional[str] = None,
    min_feature_version: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Estimate Kelly-style sizing parameters from realized per-trade PnL%.

    We treat pnl_pct / 100 as the per-trade return R.
    Then:
        mu   = mean(R)
        sig2 = var(R)
        kelly_fraction = mu / sig2   (if sig2 > 0)
    This is a *very* rough estimate but useful for:
        - risk_manager guardrails
        - reinvestor sizing caps
    """
    days_back = max(1, min(int(days_back), 365))

    clauses: List[str] = [
        "o.ts >= now() - (%s::interval)",
        "o.pnl_pct IS NOT NULL",
    ]
    params: List[Any] = [f"{days_back} days"]

    if src:
        clauses.append("c.src = %s")
        params.append(src)
    if regime:
        clauses.append("c.regime = %s")
        params.append(regime)
    if min_feature_version is not None:
        clauses.append("c.feature_version >= %s")
        params.append(int(min_feature_version))

    where_sql = "WHERE " + " AND ".join(clauses)

    sql = f"""
        SELECT o.pnl_pct
        FROM entry_outcomes o
        JOIN entry_candidates c
          ON c.id = o.candidate_id
        {where_sql}
    """

    returns: List[float] = []
    with cursor_only() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    for (pnl_pct,) in rows:
        try:
            r = float(pnl_pct) / 100.0
            returns.append(r)
        except Exception:
            continue

    n = len(returns)
    if n == 0:
        return {
            "days_back": days_back,
            "n": 0,
            "mu": None,
            "sigma": None,
            "kelly_fraction": None,
        }

    mu = sum(returns) / float(n)
    # sample variance (unbiased) if n > 1 else 0
    if n > 1:
        var = sum((r - mu) ** 2 for r in returns) / float(n - 1)
    else:
        var = 0.0

    sigma = var ** 0.5 if var > 0 else 0.0
    if var > 0:
        kelly_fraction = mu / var
    else:
        kelly_fraction = None

    return {
        "days_back": days_back,
        "n": n,
        "mu": mu,
        "sigma": sigma,
        "kelly_fraction": kelly_fraction,
    }


def fetch_daily_sharpe_like(
    *,
    days_back: int = 90,
    src: Optional[str] = None,
    regime: Optional[str] = None,
    min_feature_version: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Very rough Sharpe-style metric over daily avg pnl_pct.

    We:
      • aggregate per-day avg pnl_pct
      • treat that as a daily return series r_t = avg_pnl_pct / 100
      • compute mean(r_t), std(r_t), and sharpe_like = mean / std

    Good for a single scalar “strategy health” metric next to equity curve.
    """
    days_back = max(1, min(int(days_back), 730))

    eq_rows = fetch_daily_equity_curve(
        days_back=days_back,
        src=src,
        regime=regime,
        min_feature_version=min_feature_version,
        base_equity=1.0,
    )

    daily_rs: List[float] = []
    for row in eq_rows:
        try:
            r = float(row.get("avg_pnl_pct") or 0.0) / 100.0
            daily_rs.append(r)
        except Exception:
            continue

    n = len(daily_rs)
    if n == 0:
        return {
            "days_back": days_back,
            "n_days": 0,
            "mu_daily": None,
            "sigma_daily": None,
            "sharpe_like": None,
        }

    mu = sum(daily_rs) / float(n)
    if n > 1:
        var = sum((r - mu) ** 2 for r in daily_rs) / float(n - 1)
    else:
        var = 0.0
    sigma = var ** 0.5 if var > 0 else 0.0
    sharpe_like = mu / sigma if sigma > 0 else None

    return {
        "days_back": days_back,
        "n_days": n,
        "mu_daily": mu,
        "sigma_daily": sigma,
        "sharpe_like": sharpe_like,
    }
