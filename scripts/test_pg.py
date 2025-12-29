# -*- coding: utf-8 -*-
from __future__ import annotations

"""
scripts/test_pg.py

God-Tier PG / DSN Canary (READ ONLY, PROD-LOCKED)
================================================

Mission
-------
A zero-risk Postgres canary that proves connectivity + schema sanity
WITHOUT modifying data.

Hard guarantees
---------------
- No writes: runs inside explicit READ ONLY transaction
- Never prints secrets: DSNs are always masked
- Uses utils_pkg/db_dsn.py as the SINGLE SOURCE OF TRUTH for:
    * DSN picking / normalization
    * query stripping (no sslmode forwarded to asyncpg)
    * SSLContext via connect_args["ssl"]

Exit codes
----------
0   OK (connected + read-only + required tables satisfied, or skipped)
1   No candidate passed
2   Connected but REQUIRED_TABLES missing (no candidate passes)
3   Bad DSN format / missing deps / internal runner error
124 All candidates timed out

Env (high-signal)
-----------------
SKIP_DSN_REQUIRED=0|1

REQUIRED_TABLES="trades,positions,ai_features"
PG_TEST_SCHEMA="public"

PG_TEST_TIMEOUT_S=15
PG_TEST_CONNECT_TIMEOUT_S=8
PG_TEST_TRY_ALL=0|1

PG_TEST_JSON=1|0          (default 1)
PG_TEST_JSON_ONLY=0|1

PG_TEST_RETRIES=2
PG_TEST_RETRY_BASE_S=0.35

PG_TEST_TCP_PROBE=1|0     (default 1)
PG_TEST_DNS_DEBUG=1|0     (default 1)

CLI
---
python -m scripts.test_pg
python -m scripts.test_pg --try-all
python -m scripts.test_pg --schema public --required-tables trades,positions
python -m scripts.test_pg --dsn "postgresql://..." --json-only
"""

import argparse
import asyncio
import contextlib
import json
import os
import random
import socket
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

# Optional dotenv (local dev only)
try:
    from dotenv import load_dotenv  # type: ignore

    load_dotenv()
except Exception:
    pass

# Ensure repo root on sys.path
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from sqlalchemy import text  # noqa: E402
from sqlalchemy.ext.asyncio import create_async_engine  # noqa: E402

from utils_pkg import db_dsn as dsnmod  # noqa: E402


_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


# =============================================================================
# env helpers
# =============================================================================
def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    s = (v if v is not None else d)
    s = str(s).strip()
    # Strip one layer of quotes (Render/UI often stores quoted strings)
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


def _split_csv(s: str) -> List[str]:
    return [p.strip() for p in (s or "").replace(";", ",").split(",") if p.strip()]


# =============================================================================
# time/log helpers
# =============================================================================
def _ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _safe_print(line: str, *, file) -> None:
    """Never crash on cp1252 consoles."""
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
    _safe_print(f"[test_pg] {_ts()} {msg}", file=sys.stderr)


# =============================================================================
# diagnostics
# =============================================================================
def _dns_debug(host: str, port: int) -> List[str]:
    try:
        infos = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
        ips: List[str] = []
        for _, _, _, _, sockaddr in infos:
            ip = sockaddr[0]
            if ip not in ips:
                ips.append(ip)
        return ips
    except Exception:
        return []


def _tcp_probe(host: str, port: int, timeout_s: float) -> Tuple[bool, str]:
    try:
        with socket.create_connection((host, port), timeout=timeout_s):
            return True, "ok"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


# =============================================================================
# error classification
# =============================================================================
def _classify_error(msg: str) -> str:
    m = (msg or "").lower()
    if not m:
        return "other"
    if "timed out" in m or "timeout" in m:
        return "timeout"
    if "getaddrinfo failed" in m or "could not translate host name" in m or "name or service not known" in m:
        return "dns"
    if "password authentication failed" in m or "authentication failed" in m or "no pg_hba.conf entry" in m:
        return "auth"
    if "ssl" in m and ("handshake" in m or "certificate" in m or "required" in m or "alert" in m):
        return "ssl"
    if "connection refused" in m:
        return "refused"
    if "too many connections" in m:
        return "over_capacity"
    if "read_only" in m:
        return "not_read_only"
    if "connection was closed in the middle of operation" in m:
        return "server_closed_mid_op"
    if "bad_format" in m:
        return "bad_format"
    if "missing_deps" in m:
        return "missing_deps"
    return "other"


def _fix_hints(results: List[Dict[str, Any]], *, schema: str, required_tables: List[str]) -> List[str]:
    if not results:
        return ["No attempts were made."]

    kinds = [_classify_error(str(r.get("error", ""))) for r in results if not r.get("ok")]
    hints: List[str] = []

    if "dns" in kinds:
        hints.append("DNS failed for the DB host. Check outbound DNS/network, VPN, or try another network.")
    if "refused" in kinds:
        hints.append("Port refused. DB may not allow external connections from your IP/runtime.")
    if "timeout" in kinds:
        hints.append("Timed out. Increase PG_TEST_TIMEOUT_S and/or PG_TEST_CONNECT_TIMEOUT_S.")
    if "ssl" in kinds:
        hints.append("SSL/TLS problem. Ensure sslmode=require/verify-* and that db_dsn provides a real SSLContext.")
    if "auth" in kinds:
        hints.append("Auth failed. Verify username/password and the exact DSN from Render dashboard.")
    if "server_closed_mid_op" in kinds:
        hints.append(
            "Server closed mid-operation. Common causes: SSL mismatch, transient ISP/VPN resets, or DB policy. "
            "Use EXTERNAL DSN locally; on Render use INTERNAL DSN."
        )
    if any(r.get("missing_tables") for r in results) and required_tables:
        hints.append(f"Connected but required tables missing in schema '{schema}'. Run schema creation/migrations.")
    if not hints:
        hints.append("No candidate connected. Verify DSN(s), network egress, DB policy, and that Postgres is reachable.")

    out: List[str] = []
    seen = set()
    for h in hints:
        if h not in seen:
            seen.add(h)
            out.append(h)
    return out[:10]


# =============================================================================
# retry policy
# =============================================================================
def _retry_cfg() -> Tuple[int, float]:
    retries = max(0, _envi("PG_TEST_RETRIES", 2))
    base = max(0.05, float(_envf("PG_TEST_RETRY_BASE_S", 0.35)))
    return retries, base


async def _sleep_backoff(attempt: int, base: float) -> None:
    cap = 2.5
    t = min(cap, base * (1.6 ** max(0, attempt)))
    t = t * (0.85 + random.random() * 0.35)
    await asyncio.sleep(t)


# =============================================================================
# candidates
# =============================================================================
_DSN_KEYS = (
    "POSITIONS_PG_DSN",
    "SNIPER_DB_DSN",
    "DATABASE_URL",
    "POSTGRES_DSN",
    "PG_DSN",
    "PG_DSN_INTERNAL",
    "PG_DSN_EXTERNAL",
    "PNL_DSN",
)


def _candidate_dsns(cli_dsns: Sequence[str]) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    for i, d in enumerate(cli_dsns):
        if d and str(d).strip():
            out.append((f"CLI_{i+1}", str(d).strip()))

    # Deterministic order (high-signal first for your setup)
    for k in ("PNL_DSN", "POSTGRES_DSN", "PG_DSN", "DATABASE_URL", "PG_DSN_INTERNAL", "PG_DSN_EXTERNAL", "SNIPER_DB_DSN", "POSITIONS_PG_DSN"):
        v = _envs(k, "")
        if v:
            out.append((k, v))

    # Deduplicate by raw string
    seen = set()
    uniq: List[Tuple[str, str]] = []
    for lbl, raw in out:
        key = raw.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        uniq.append((lbl, key))
    return uniq


@contextlib.contextmanager
def _temp_env(overrides: Dict[str, Optional[str]]) -> Any:
    """
    Temporarily set/unset environment variables.
    overrides: {key: value_or_None}. None => unset.
    """
    old: Dict[str, Optional[str]] = {}
    for k, v in overrides.items():
        old[k] = os.getenv(k)
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v
    try:
        yield
    finally:
        for k, prev in old.items():
            if prev is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = prev


@dataclass
class AttemptPlan:
    label: str
    raw: str
    dsn_sa: str
    connect_args: Dict[str, Any]
    host: str
    port: int
    db: str
    sslmode: str
    ssl_enabled: bool


def _build_plan(label: str, raw: str, *, timeout_s: Optional[float], app_name: str, connect_timeout_s: float) -> AttemptPlan:
    """
    Build a plan for an arbitrary raw DSN WITHOUT calling db_dsn private functions.
    We do this by temporarily forcing db_dsn's env selection to pick this raw DSN.
    """
    # Clear all DSN keys so the DSN brain is forced to pick our injected one.
    overrides = {k: None for k in _DSN_KEYS}
    overrides["POSTGRES_DSN"] = raw

    with _temp_env(overrides):
        pr = dsnmod.pick_pg_dsn_debug(timeout_s=timeout_s, app_name=app_name)

    if not pr.dsn_sa:
        raise RuntimeError("bad_format:empty_sanitized_dsn")

    u = urlparse(pr.dsn_sa)
    port = int(u.port or 5432)
    db = (u.path or "").lstrip("/").split("/", 1)[0] if u.path else ""

    ca = dict(pr.connect_args or {})
    # Add connect timeout for the canary (asyncpg supports `timeout=` for connect)
    if connect_timeout_s and connect_timeout_s > 0:
        ca.setdefault("timeout", float(connect_timeout_s))

    return AttemptPlan(
        label=label,
        raw=raw,
        dsn_sa=pr.dsn_sa,
        connect_args=ca,
        host=pr.host or (u.hostname or ""),
        port=port,
        db=db,
        sslmode=pr.sslmode,
        ssl_enabled=bool(pr.ssl_enabled),
    )


# =============================================================================
# DB canary (SQLAlchemy asyncpg) — READ ONLY
# =============================================================================
async def _sqlalchemy_canary(
    plan: AttemptPlan,
    *,
    schema: str,
    required_tables: List[str],
    timeout_s: float,
    statement_timeout_ms: int,
    lock_timeout_ms: int,
    idle_tx_timeout_ms: int,
    tcp_probe: bool,
    dns_debug: bool,
) -> Dict[str, Any]:
    t0 = time.monotonic()
    out: Dict[str, Any] = {
        "label": plan.label,
        "ok": False,
        "dsn": dsnmod.mask_dsn(plan.raw),
        "dsn_sa": dsnmod.mask_dsn(plan.dsn_sa),
        "host": plan.host,
        "port": plan.port,
        "db_hint": plan.db,
        "sslmode": plan.sslmode,
        "ssl_enabled": int(bool(plan.ssl_enabled)),
        "ips": [],
        "tcp_probe": None,
        "tcp_probe_ok": None,
        "sec": None,
        "attempts": 0,
        "missing_tables": [],
        "details": {},
        "error": "",
        "error_kind": "",
    }

    if dns_debug and plan.host:
        out["ips"] = _dns_debug(plan.host, plan.port)

    if tcp_probe and plan.host:
        ok, msg = _tcp_probe(plan.host, plan.port, timeout_s=min(6.0, max(2.0, timeout_s * 0.4)))
        out["tcp_probe_ok"] = int(bool(ok))
        out["tcp_probe"] = msg

    eng = create_async_engine(
        plan.dsn_sa,
        connect_args=dict(plan.connect_args or {}),
        pool_pre_ping=True,
        pool_size=1,
        max_overflow=0,
        pool_timeout=max(3.0, float(timeout_s)),
    )

    retries, base = _retry_cfg()
    last_err: Optional[BaseException] = None

    try:
        for attempt in range(retries + 1):
            out["attempts"] = attempt + 1
            try:
                async with eng.connect() as conn:
                    # Session timeouts (safe: no writes)
                    with contextlib.suppress(Exception):
                        await conn.exec_driver_sql(f"SET statement_timeout = {max(1000, int(statement_timeout_ms))}")
                    with contextlib.suppress(Exception):
                        await conn.exec_driver_sql(f"SET lock_timeout = {max(500, int(lock_timeout_ms))}")
                    with contextlib.suppress(Exception):
                        await conn.exec_driver_sql(
                            f"SET idle_in_transaction_session_timeout = {max(1000, int(idle_tx_timeout_ms))}"
                        )

                    # Fast ping
                    row = (await conn.execute(text("SELECT 1"))).first()
                    if not row or row[0] != 1:
                        raise RuntimeError("select1_failed")

                    # Explicit read-only transaction
                    await conn.exec_driver_sql("BEGIN READ ONLY")
                    try:
                        ro = (await conn.execute(text("SHOW transaction_read_only"))).scalar()
                        ro_s = (str(ro).strip().lower() if ro is not None else "")

                        row1 = (await conn.execute(text("SELECT now(), version()"))).first()
                        row2 = (await conn.execute(text("SELECT current_database(), current_user"))).first()

                        out["details"] = {
                            "now": str(row1[0]) if row1 else "",
                            "version": (str(row1[1]).splitlines()[0] if row1 and row1[1] else ""),
                            "db": str(row2[0]) if row2 else "",
                            "user": str(row2[1]) if row2 else "",
                            "transaction_read_only": ro_s,
                            "schema": schema,
                            "required_tables": list(required_tables),
                        }

                        if ro_s not in ("on", "true", "1"):
                            raise RuntimeError("read_only_enforcement_failed")

                        if required_tables:
                            q = text(
                                """
                                SELECT table_name
                                FROM information_schema.tables
                                WHERE table_schema = :schema
                                  AND table_name = ANY(:names)
                                """
                            )
                            rows = (await conn.execute(q, {"schema": schema, "names": list(required_tables)})).fetchall()
                            present = {str(r[0]) for r in rows}
                            missing = [t for t in required_tables if t not in present]
                            out["missing_tables"] = missing
                            if missing:
                                out["ok"] = False
                                return out

                        out["ok"] = True
                        return out
                    finally:
                        with contextlib.suppress(Exception):
                            await conn.exec_driver_sql("ROLLBACK")

            except Exception as e:
                last_err = e
                msg = f"{type(e).__name__}: {e}"
                out["error"] = msg
                out["error_kind"] = _classify_error(msg)

                if attempt < retries and out["error_kind"] in ("timeout", "server_closed_mid_op", "ssl", "other"):
                    await _sleep_backoff(attempt, base)
                    continue
                return out

        if last_err is not None:
            msg = f"{type(last_err).__name__}: {last_err}"
            out["error"] = msg
            out["error_kind"] = _classify_error(msg)
        return out
    finally:
        out["sec"] = round(time.monotonic() - t0, 3)
        with contextlib.suppress(Exception):
            await eng.dispose()


# =============================================================================
# main runner
# =============================================================================
async def _amain() -> int:
    RC_OK = 0
    RC_FAIL = 1
    RC_MISSING_TABLES = 2
    RC_BAD = 3
    RC_TIMEOUT = 124

    ap = argparse.ArgumentParser(description="God-Tier Postgres canary (READ ONLY).")
    ap.add_argument("--dsn", action="append", default=[], help="Override DSN (repeatable). If omitted uses env candidates.")
    ap.add_argument("--schema", default=_envs("PG_TEST_SCHEMA", "public") or "public", help="Schema for REQUIRED_TABLES.")
    ap.add_argument("--required-tables", default=_envs("REQUIRED_TABLES", ""), help="CSV list of required tables.")
    ap.add_argument("--timeout", type=float, default=float(_envf("PG_TEST_TIMEOUT_S", 15.0)), help="Per-candidate timeout.")
    ap.add_argument("--connect-timeout", type=float, default=float(_envf("PG_TEST_CONNECT_TIMEOUT_S", 8.0)), help="Connect timeout.")
    ap.add_argument("--try-all", action="store_true", help="Try all candidates (instead of stopping at first pass).")
    ap.add_argument("--json", action="store_true", help="Emit JSON summary.")
    ap.add_argument("--json-only", action="store_true", help="Only JSON output (no human logs).")
    args = ap.parse_args()

    json_only = bool(args.json_only or _envb("PG_TEST_JSON_ONLY", False))
    emit_json = bool(args.json or args.json_only or _envb("PG_TEST_JSON", True) or _envb("PG_TEST_JSON_ONLY", False))

    if _envb("SKIP_DSN_REQUIRED", False):
        summary = {"ts": _ts(), "ok": True, "skipped": True, "reason": "SKIP_DSN_REQUIRED=1"}
        if emit_json:
            print(json.dumps(summary, ensure_ascii=False, default=str))
        _hlog("SKIP (SKIP_DSN_REQUIRED=1)", json_only=json_only)
        return RC_OK

    schema = (args.schema or "public").strip() or "public"
    required_tables = _split_csv(args.required_tables or "")

    timeout_s = max(4.0, float(args.timeout))
    connect_timeout_s = max(2.0, float(args.connect_timeout))
    try_all = bool(args.try_all or _envb("PG_TEST_TRY_ALL", False))

    tcp_probe = _envb("PG_TEST_TCP_PROBE", True)
    dns_debug = _envb("PG_TEST_DNS_DEBUG", True)

    statement_timeout_ms = _envi("PG_TEST_STATEMENT_TIMEOUT_MS", 8000)
    lock_timeout_ms = _envi("PG_TEST_LOCK_TIMEOUT_MS", 5000)
    idle_tx_timeout_ms = _envi("PG_TEST_IDLE_TX_TIMEOUT_MS", 10000)

    cands = _candidate_dsns([d for d in args.dsn if d])
    if not cands:
        summary = {
            "ts": _ts(),
            "ok": False,
            "skipped": False,
            "error": "no_dsn_candidates",
            "detail": "Set one of PNL_DSN/POSTGRES_DSN/PG_DSN/DATABASE_URL/PG_DSN_INTERNAL/PG_DSN_EXTERNAL or pass --dsn",
        }
        if emit_json:
            print(json.dumps(summary, ensure_ascii=False, default=str))
        _hlog("FAIL no DSN candidates", json_only=json_only)
        return RC_FAIL

    if not json_only:
        _hlog("────────────────────────────────────", json_only=False)
        _hlog("God-Tier test_pg (READ ONLY, asyncpg)", json_only=False)
        _hlog("────────────────────────────────────", json_only=False)
        _hlog(
            f"timeout_s={timeout_s:.1f} connect_timeout_s={connect_timeout_s:.1f} schema={schema} "
            f"required_tables={','.join(required_tables) if required_tables else '-'} "
            f"try_all={int(try_all)} retries={_envi('PG_TEST_RETRIES',2)} tcp_probe={int(tcp_probe)} dns_debug={int(dns_debug)}",
            json_only=False,
        )
        _hlog("candidates=" + ",".join([k for k, _ in cands]), json_only=False)

    results: List[Dict[str, Any]] = []
    picked: Optional[Dict[str, Any]] = None
    any_missing_tables = False

    app_name = _envs("PG_APP_NAME", "pnlsniper_test_pg") or "pnlsniper_test_pg"

    for label, raw in cands:
        try:
            plan = _build_plan(label, raw, timeout_s=timeout_s, app_name=app_name, connect_timeout_s=connect_timeout_s)
        except Exception as e:
            r = {
                "label": label,
                "ok": False,
                "dsn": dsnmod.mask_dsn(raw),
                "error": f"bad_format:{type(e).__name__}: {e}",
                "error_kind": "bad_format",
                "sec": 0.0,
            }
            results.append(r)
            if not json_only:
                _hlog(f"RES {label} ok=0 error={r['error']}", json_only=False)
            continue

        if not json_only:
            _hlog(
                f"TRY {label} host={plan.host or '-'} port={plan.port} db={plan.db or '-'} "
                f"ssl_enabled={int(plan.ssl_enabled)} sslmode={plan.sslmode or '-'} dsn={dsnmod.mask_dsn(raw)}",
                json_only=False,
            )

        try:
            r = await asyncio.wait_for(
                _sqlalchemy_canary(
                    plan,
                    schema=schema,
                    required_tables=required_tables,
                    timeout_s=timeout_s,
                    statement_timeout_ms=statement_timeout_ms,
                    lock_timeout_ms=lock_timeout_ms,
                    idle_tx_timeout_ms=idle_tx_timeout_ms,
                    tcp_probe=tcp_probe,
                    dns_debug=dns_debug,
                ),
                timeout=timeout_s + 2.0,
            )
        except asyncio.TimeoutError:
            r = {
                "label": label,
                "ok": False,
                "dsn": dsnmod.mask_dsn(raw),
                "dsn_sa": dsnmod.mask_dsn(plan.dsn_sa),
                "host": plan.host,
                "port": plan.port,
                "db_hint": plan.db,
                "sslmode": plan.sslmode,
                "ssl_enabled": int(plan.ssl_enabled),
                "ips": _dns_debug(plan.host, plan.port) if (dns_debug and plan.host) else [],
                "tcp_probe_ok": None,
                "tcp_probe": None,
                "sec": round(timeout_s, 3),
                "attempts": 0,
                "missing_tables": [],
                "details": {},
                "error": "attempt_timeout",
                "error_kind": "timeout",
            }

        results.append(r)

        if (not r.get("ok")) and bool(r.get("missing_tables")):
            any_missing_tables = True

        if not json_only:
            _hlog(
                f"RES {label} ok={int(bool(r.get('ok')))} attempts={r.get('attempts',0)} sec={r.get('sec')} "
                f"kind={r.get('error_kind') or '-'}",
                json_only=False,
            )
            if r.get("ips"):
                _hlog(f"  ips={r['ips']}", json_only=False)
            if r.get("tcp_probe_ok") is not None:
                _hlog(f"  tcp_probe_ok={r['tcp_probe_ok']} tcp_probe={r.get('tcp_probe')}", json_only=False)
            if r.get("error"):
                _hlog(f"  error={r['error']}", json_only=False)
            if r.get("missing_tables"):
                _hlog(f"  missing_tables={','.join(r['missing_tables'])}", json_only=False)

        if r.get("ok") and picked is None:
            picked = r
            if not try_all:
                break

        if (not r.get("ok")) and r.get("missing_tables") and (not try_all):
            break

    summary: Dict[str, Any] = {
        "ts": _ts(),
        "ok": bool(picked is not None),
        "skipped": False,
        "schema": schema,
        "required_tables": required_tables,
        "picked": (picked.get("label") if picked else None),
        "results": results,
        "fix_hints": _fix_hints(results, schema=schema, required_tables=required_tables),
    }

    if emit_json:
        print(json.dumps(summary, ensure_ascii=False, default=str))

    if picked is not None:
        d = picked.get("details", {}) or {}
        _hlog(
            f"SUMMARY ok=1 db={d.get('db')} user={d.get('user')} read_only={d.get('transaction_read_only')} picked={picked.get('label')}",
            json_only=json_only,
        )
        return RC_OK

    if any_missing_tables:
        _hlog("SUMMARY ok=0 (connected but REQUIRED_TABLES missing; no candidate passed)", json_only=json_only)
        return RC_MISSING_TABLES

    if results and all((_classify_error(str(r.get("error", ""))) == "timeout") for r in results):
        _hlog("SUMMARY ok=0 (all candidates timed out)", json_only=json_only)
        return RC_TIMEOUT

    if results and all((_classify_error(str(r.get("error", ""))) in ("bad_format", "missing_deps")) for r in results):
        _hlog("SUMMARY ok=0 (all candidates bad format or missing deps)", json_only=json_only)
        return RC_BAD

    _hlog("SUMMARY ok=0 (no candidate connected)", json_only=json_only)
    return RC_FAIL


def main() -> None:
    try:
        rc = asyncio.run(_amain())
    except KeyboardInterrupt:
        rc = 130
    except Exception as e:
        _safe_print(f"[test_pg] {_ts()} FAIL unhandled {type(e).__name__}: {e}", file=sys.stderr)
        rc = 1
    raise SystemExit(rc)


if __name__ == "__main__":
    # Windows asyncio stability
    with contextlib.suppress(Exception):
        if os.name == "nt" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]
    main()
