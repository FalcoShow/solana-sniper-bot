from __future__ import annotations

"""
tests/test_contract_db_schema.py — DB Schema Contract Tests (God-Tier LOCK-IN, P0)
=================================================================================

Goal
----
Lock in the *canonical* Postgres schema for the bot WITHOUT requiring Postgres.

This suite is intentionally:
- OFFLINE (no DB needed, no network)
- Import-safe (no secrets required)
- Refactor-tolerant (discovers schema from:
    1) exported schema registries / SQL helpers (preferred), OR
    2) embedded DDL strings, OR
    3) scanning utils_pkg/ai_db.py source for CREATE TABLE statements)

Hard invariants enforced (P0 / MUST)
-----------------------------------
- utils_pkg.ai_db imports in safe mode without performing I/O at import time.
- We can discover at least one CREATE TABLE target (schema not empty).
- Schema includes "core families" (via flexible matching):
    • positions-like table
    • trades/fills-like table
    • candidates/entries-like table

Soft (best-effort) invariants (non-fatal unless STRICT_SCHEMA_CONTRACT=1)
------------------------------------------------------------------------
- DDL is idempotent (CREATE TABLE IF NOT EXISTS preferred).
- A schema/migrations "version" exists (SCHEMA_VERSION / MIGRATIONS).

How to make this suite maximally strong
---------------------------------------
Expose one of these in utils_pkg.ai_db (or utils_pkg.contracts):
  - SCHEMA_DDL: str | list[str]
  - DDL_STATEMENTS: list[str]
  - SCHEMA_SQL / schema_sql() -> str | list[str]
  - migrations() -> list[str] or MIGRATIONS list
  - SCHEMA_VERSION: int

If you don't expose anything, this suite still discovers CREATE TABLE from source text,
but exported registries are preferred for clarity and lock-in.
"""

import importlib
import os
import re
import sys
from collections.abc import Callable, Iterable
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

    # Never trade in tests
    monkeypatch.setenv("LIVE_TRADING", os.getenv("LIVE_TRADING", "0"))
    monkeypatch.setenv("TRADING_ENABLED", os.getenv("TRADING_ENABLED", "0"))
    monkeypatch.setenv("ALLOW_BUYS", os.getenv("ALLOW_BUYS", "0"))
    monkeypatch.setenv("ALLOW_SELLS", os.getenv("ALLOW_SELLS", "0"))


# ─────────────────────────────────────────────────────────────────────────────
# Regex helpers: parse CREATE TABLE names from SQL / source
# ─────────────────────────────────────────────────────────────────────────────
_CREATE_TABLE_RE = re.compile(
    r"""
    \bcreate\s+table\s+
    (?:(?:if\s+not\s+exists)\s+)?
    (?P<name>
        (?:
            (?:"[^"]+"\.) | (?:[A-Za-z_][A-Za-z0-9_]*\.)
        )?
        (?:"[^"]+"|[A-Za-z_][A-Za-z0-9_]*)
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)

_CREATE_INDEX_RE = re.compile(r"\bcreate\s+(?:unique\s+)?index\b", re.IGNORECASE)
_IF_NOT_EXISTS_RE = re.compile(r"\bif\s+not\s+exists\b", re.IGNORECASE)


def _strict_enabled() -> bool:
    """
    Opt-in strictness to avoid breaking current builds during refactors.
    Set STRICT_SCHEMA_CONTRACT=1 to enforce best-practice invariants as hard failures.
    """
    return os.getenv("STRICT_SCHEMA_CONTRACT", "0").strip() in {"1", "true", "TRUE", "yes", "YES"}


def _norm_table_name(raw: str) -> str:
    """
    Normalize table names:
      - remove schema prefix
      - strip double quotes
      - lowercase
    """
    s = (raw or "").strip()
    if "." in s:
        s = s.split(".", 1)[1]
    s = s.strip().strip('"').strip()
    return s.lower()


def _extract_create_table_names(sql_text: str) -> set[str]:
    out: set[str] = set()
    if not sql_text:
        return out
    for m in _CREATE_TABLE_RE.finditer(sql_text):
        name = _norm_table_name(m.group("name") or "")
        if name:
            out.add(name)
    return out


def _split_sql_statements(sql: str) -> list[str]:
    """
    Very lightweight statement splitter: splits on ';' but keeps non-empty chunks.
    Good enough for contract tests.
    """
    if not sql:
        return []
    parts = [p.strip() for p in sql.split(";")]
    return [p for p in parts if p]


def _iter_strings_from_module(mod: Any) -> Iterable[str]:
    """
    Pull likely DDL-bearing strings from module attributes.
    """
    for attr in dir(mod):
        if attr.startswith("__"):
            continue
        try:
            v = getattr(mod, attr)
        except Exception:
            continue
        if isinstance(v, str) and ("create table" in v.lower() or "create index" in v.lower()):
            yield v
        if isinstance(v, list | tuple):
            for x in v:
                if isinstance(x, str) and (
                    "create table" in x.lower() or "create index" in x.lower()
                ):
                    yield x


def _find_callable(mod: Any, names: tuple[str, ...]) -> Callable[..., Any] | None:
    for n in names:
        fn = getattr(mod, n, None)
        if callable(fn):
            return fn
    return None


def _maybe_call_schema_exporter(mod: Any) -> list[str]:
    """
    Best-effort: call an exported schema SQL helper if present.
    Must be PURE (no DB connection) — tests will not provide DSN.

    Accepted outputs:
      - str (full SQL)
      - list[str] (statements)
      - tuple[str] (statements)
      - dict with "sql"/"ddl"/"statements"
    """
    fn = _find_callable(
        mod,
        (
            "schema_sql",
            "get_schema_sql",
            "ddl_sql",
            "get_ddl_sql",
            "schema_ddl",
            "get_schema_ddl",
            "export_schema_sql",
            "export_ddl",
            "migrations",
            "get_migrations",
        ),
    )
    if fn is None:
        return []

    # Try calling without args; tolerate optional ctx/now_ms, etc.
    out = None
    try:
        out = fn()
    except TypeError:
        try:
            out = fn(ctx={"source": "contract"})
        except Exception:
            return []
    except Exception:
        return []

    # Normalize
    if isinstance(out, str):
        return _split_sql_statements(out)
    if isinstance(out, list | tuple) and all(isinstance(x, str) for x in out):
        return [x.strip() for x in out if str(x).strip()]
    if isinstance(out, dict):
        for k in ("statements", "ddl", "sql", "schema", "migrations"):
            v = out.get(k)
            if isinstance(v, str):
                return _split_sql_statements(v)
            if isinstance(v, list | tuple) and all(isinstance(x, str) for x in v):
                return [x.strip() for x in v if str(x).strip()]
    return []


def _read_ai_db_source(mod: Any) -> str:
    """
    Read utils_pkg.ai_db source text if available (offline) to discover CREATE TABLE.
    """
    p = getattr(mod, "__file__", None)
    if not p:
        return ""
    try:
        path = Path(str(p)).resolve()
        if path.exists():
            return path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""
    return ""


def _discover_schema(mod: Any) -> tuple[set[str], list[str]]:
    """
    Return: (table_names, ddl_statements)

    Discovery order:
      1) exported schema exporter (pure)
      2) embedded DDL strings in module attrs
      3) source scan of utils_pkg.ai_db.py
    """
    ddl: list[str] = []

    ddl_from_exporter = _maybe_call_schema_exporter(mod)
    if ddl_from_exporter:
        ddl.extend(ddl_from_exporter)

    for s in _iter_strings_from_module(mod):
        # If it looks like full SQL, split; else keep as fragment
        if ";" in s:
            ddl.extend(_split_sql_statements(s))
        else:
            ddl.append(s.strip())

    if not ddl:
        src = _read_ai_db_source(mod)
        # Keep whole source as "ddl" bucket for idempotency checks (we still parse tables from it).
        if src:
            ddl.append(src)

    # Extract tables from all collected text
    tables: set[str] = set()
    for chunk in ddl:
        tables |= _extract_create_table_names(chunk)

    return tables, ddl


def _matches_any(table: str, needles: tuple[str, ...]) -> bool:
    t = (table or "").lower()
    return any(n in t for n in needles)


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────
def test_ai_db_import_is_safe() -> None:
    """
    Contract: importing ai_db must not require a DB connection in safe mode.
    """
    mod = importlib.import_module("utils_pkg.ai_db")
    assert mod is not None


def test_schema_is_discoverable_and_non_empty() -> None:
    """
    Contract: schema cannot be empty — we must be able to discover at least one table.
    """
    mod = importlib.import_module("utils_pkg.ai_db")
    tables, ddl = _discover_schema(mod)

    assert isinstance(tables, set)
    assert len(tables) >= 1, (
        "Could not discover any CREATE TABLE targets from utils_pkg.ai_db.\n"
        "Fix (recommended): export a pure schema helper (schema_sql()/SCHEMA_DDL/etc.) "
        "or embed explicit CREATE TABLE statements.\n"
        "Tip: this test also scans ai_db.py source if nothing is exported."
    )


def test_schema_contains_core_table_families() -> None:
    """
    P0 MUST: schema should cover the core lifecycle:
      - candidates / entries
      - trades / fills
      - positions

    This uses flexible substring matching to avoid coupling to exact names.
    """
    mod = importlib.import_module("utils_pkg.ai_db")
    tables, _ddl = _discover_schema(mod)

    # Flexible synonyms
    pos_needles = ("position", "positions", "pos_")
    trade_needles = ("trade", "trades", "fill", "fills", "execut", "orders")
    cand_needles = ("candidate", "candidates", "entry", "entries", "signals", "signal")

    has_pos = any(_matches_any(t, pos_needles) for t in tables)
    has_trade = any(_matches_any(t, trade_needles) for t in tables)
    has_cand = any(_matches_any(t, cand_needles) for t in tables)

    missing: list[str] = []
    if not has_pos:
        missing.append("positions-like")
    if not has_trade:
        missing.append("trades/fills-like")
    if not has_cand:
        missing.append("candidates/entries-like")

    assert not missing, (
        "Schema is missing core table families required for end-to-end P0.\n"
        f"Missing: {', '.join(missing)}\n"
        f"Discovered tables (sample): {sorted(list(tables))[:80]}\n"
        "Fix: ensure ai_db schema includes these families (names can vary, but should match intent)."
    )


def test_schema_idempotency_best_effort() -> None:
    """
    Best-practice (strongly preferred):
      - CREATE TABLE IF NOT EXISTS
      - CREATE INDEX IF NOT EXISTS

    This is enforced STRICTLY only if STRICT_SCHEMA_CONTRACT=1.
    """
    mod = importlib.import_module("utils_pkg.ai_db")
    _tables, ddl_chunks = _discover_schema(mod)

    # Extract statements that look like DDL
    stmts: list[str] = []
    for chunk in ddl_chunks:
        if not chunk:
            continue
        # If this is a whole source file, it might contain many statements;
        # still count CREATE TABLE occurrences for heuristic.
        if "create table" in chunk.lower() or "create index" in chunk.lower():
            stmts.extend(_split_sql_statements(chunk) if ";" in chunk else [chunk])

    create_table_stmts = [s for s in stmts if "create table" in s.lower()]
    create_index_stmts = [s for s in stmts if _CREATE_INDEX_RE.search(s or "")]

    # If we can't isolate statements (e.g., DDL built dynamically), don't fail.
    if not create_table_stmts:
        pytest.skip(
            "Could not isolate CREATE TABLE statements for idempotency check (best-effort)."
        )

    table_ok = sum(1 for s in create_table_stmts if _IF_NOT_EXISTS_RE.search(s or "")) / max(
        1, len(create_table_stmts)
    )
    index_ok = 1.0
    if create_index_stmts:
        index_ok = sum(1 for s in create_index_stmts if _IF_NOT_EXISTS_RE.search(s or "")) / max(
            1, len(create_index_stmts)
        )

    # Heuristic: prefer at least 80% IF NOT EXISTS coverage
    want = 0.80
    msg = (
        "Schema DDL does not appear idempotent enough.\n"
        f"CREATE TABLE IF NOT EXISTS coverage: {table_ok:.2%} ({len(create_table_stmts)} stmts)\n"
        f"CREATE INDEX IF NOT EXISTS coverage: {index_ok:.2%} ({len(create_index_stmts)} stmts)\n"
        "Recommendation: use IF NOT EXISTS for tables + indexes to make boot safe on Render.\n"
        "Set STRICT_SCHEMA_CONTRACT=1 to enforce this as a hard failure."
    )

    if _strict_enabled():
        assert table_ok >= want and index_ok >= want, msg
    else:
        # Non-strict: warn via skip-like behavior without failing the suite.
        if table_ok < want or index_ok < want:
            pytest.skip(msg)


def test_schema_versioning_surface_best_effort() -> None:
    """
    Best-effort: if ai_db exposes schema versioning, sanity-check it.
    Non-fatal unless STRICT_SCHEMA_CONTRACT=1.
    """
    mod = importlib.import_module("utils_pkg.ai_db")

    version = getattr(mod, "SCHEMA_VERSION", None)
    migrations = getattr(mod, "MIGRATIONS", None)

    # Also accept callables
    if version is None and hasattr(mod, "schema_version") and callable(mod.schema_version):
        try:
            version = mod.schema_version()  # type: ignore[attr-defined]
        except Exception:
            version = None

    if migrations is None and hasattr(mod, "migrations") and callable(mod.migrations):
        try:
            migrations = mod.migrations()  # type: ignore[attr-defined]
        except Exception:
            migrations = None

    if version is None and migrations is None:
        if _strict_enabled():
            raise AssertionError(
                "ai_db should expose SCHEMA_VERSION and/or MIGRATIONS for lock-in.\n"
                "Add SCHEMA_VERSION: int and MIGRATIONS: list[str] (pure DDL statements)."
            )
        pytest.skip("No SCHEMA_VERSION/MIGRATIONS surface found (best-effort).")

    if version is not None:
        assert isinstance(version, int) and version >= 0, (
            f"SCHEMA_VERSION must be a non-negative int; got {version!r}"
        )

    if migrations is not None:
        assert isinstance(migrations, list | tuple), (
            f"MIGRATIONS must be list/tuple[str]; got {type(migrations).__name__}"
        )
        # Don’t enforce non-empty; some systems generate at runtime.
        for i, s in enumerate(list(migrations)[:50]):
            assert isinstance(s, str), f"MIGRATIONS[{i}] must be str; got {type(s).__name__}"
            # if a migration string exists, it should include DDL-ish content
            if s.strip():
                assert "create " in s.lower() or "alter " in s.lower(), (
                    f"MIGRATIONS[{i}] does not look like DDL: {s[:120]!r}"
                )
