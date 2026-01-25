from __future__ import annotations

"""
scripts/pg_check.py

God-Tier Postgres Connectivity + Health Guard (P0 LOCK-IN)
=========================================================

Purpose
-------
Fast, production-safe DB health check for:
- DSN correctness (Render/local)
- DNS + TCP reachability
- Auth/SSL correctness
- Basic query latency + server identity
- Read-only / recovery signals

Design rules
------------
- NEVER prints secrets (DSNs always masked).
- Tries multiple DSN candidates (CLI overrides + env keys) until one works (unless try-all).
- Uses utils_pkg/db_dsn.py as the *single source of truth* for DSN normalization.
- Human logs -> STDERR (unless JSON_ONLY)
- JSON summary -> STDOUT when:
    PG_JSON=1 or PG_JSON_ONLY=1 or --json/--json-only

Exit codes
----------
0   OK (connected)
2   Not OK (couldn't connect)
1   Fatal (missing deps / misconfigured)
124 Timeout
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
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any
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

# Canonical DSN brain
try:
    from utils_pkg import db_dsn as dsnmod  # type: ignore
except Exception as e:  # pragma: no cover
    dsnmod = None  # type: ignore[assignment]
    _DB_DSN_IMPORT_ERR = e
else:
    _DB_DSN_IMPORT_ERR = None

# Primary engine path: SQLAlchemy async
try:
    from sqlalchemy import text  # type: ignore
    from sqlalchemy.ext.asyncio import create_async_engine  # type: ignore
except Exception as e:  # pragma: no cover
    text = None  # type: ignore
    create_async_engine = None  # type: ignore
    _SA_IMPORT_ERR = e
else:
    _SA_IMPORT_ERR = None

_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}

_DSN_KEYS = (
    "PNL_DSN",
    "POSTGRES_DSN",
    "PG_DSN",
    "PG_DSN_INTERNAL",
    "PG_DSN_EXTERNAL",
    "DATABASE_URL",
    "SNIPER_DB_DSN",
    "POSITIONS_PG_DSN",
)

# ---------------------------------------------------------------------
# env + safe io
# ---------------------------------------------------------------------


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
    _safe_print(f"[pg_check] {_ts()} {msg}", file=sys.stderr)


def _mask_dsn(raw: str) -> str:
    raw = str(raw or "")
    if dsnmod is not None and hasattr(dsnmod, "mask_dsn"):
        try:
            return str(dsnmod.mask_dsn(raw))
        except Exception:
            pass
    # fallback: remove creds + query
    try:
        u = urlparse(raw)
        scheme = u.scheme or "postgres"
        host = u.hostname or ""
        port = u.port or 5432
        db = (u.path or "").lstrip("/").split("/", 1)[0]
        return f"{scheme}://***@{host}:{port}/{db}?..."
    except Exception:
        return "***"


# ---------------------------------------------------------------------
# diagnostics
# ---------------------------------------------------------------------


def _resolves(host: str) -> bool:
    if not host:
        return False
    try:
        socket.getaddrinfo(host, None)
        return True
    except Exception:
        return False


def _dns_debug(host: str, port: int) -> list[str]:
    try:
        infos = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
        ips: list[str] = []
        for _, _, _, _, sockaddr in infos:
            ip = sockaddr[0]
            if ip not in ips:
                ips.append(ip)
        return ips
    except Exception:
        return []


def _tcp_probe(host: str, port: int, timeout_s: float) -> tuple[bool, str]:
    try:
        with socket.create_connection((host, port), timeout=timeout_s):
            return True, "ok"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


# ---------------------------------------------------------------------
# retry + error classification
# ---------------------------------------------------------------------


def _classify_error(msg: str) -> str:
    m = (msg or "").lower()
    if not m:
        return "other"
    if "missing_deps" in m:
        return "missing_deps"
    if "timed out" in m or "timeout" in m:
        return "timeout"
    if (
        "getaddrinfo failed" in m
        or "could not translate host name" in m
        or "name or service not known" in m
    ):
        return "dns"
    if (
        "password authentication failed" in m
        or "authentication failed" in m
        or "no pg_hba.conf entry" in m
    ):
        return "auth"
    if "ssl" in m and ("handshake" in m or "certificate" in m or "required" in m or "alert" in m):
        return "ssl"
    if "connection refused" in m:
        return "refused"
    if "too many connections" in m:
        return "over_capacity"
    if "read_only" in m or "read-only" in m:
        return "read_only"
    if "connection was closed in the middle of operation" in m:
        return "server_closed_mid_op"
    return "other"


def _retry_cfg() -> tuple[int, float]:
    retries = max(0, _envi("PG_RETRIES", 1))
    base = max(0.05, float(_envf("PG_RETRY_BASE_S", 0.30)))
    return retries, base


async def _sleep_backoff(attempt: int, base: float) -> None:
    cap = 2.25
    t = min(cap, base * (1.7 ** max(0, attempt)))
    t = t * (0.85 + random.random() * 0.35)
    await asyncio.sleep(t)


def _fix_hints(results: list[dict[str, Any]]) -> list[str]:
    kinds = [_classify_error(str(r.get("error", ""))) for r in results if not r.get("ok")]
    hints: list[str] = []

    if "missing_deps" in kinds:
        hints.append(
            "Missing deps. Ensure utils_pkg/db_dsn.py imports and install sqlalchemy[asyncio] + asyncpg."
        )
    if "dns" in kinds:
        hints.append(
            "DNS failed. If local: use EXTERNAL DSN. If Render: use INTERNAL DSN or ensure host resolves."
        )
    if "ssl" in kinds:
        hints.append(
            "SSL issue. Local typically needs sslmode=require on Render-hosted DB; internal DSN is Render-only."
        )
    if "auth" in kinds:
        hints.append("Auth failed. Verify username/password and that your IP/runtime is allowed.")
    if "timeout" in kinds:
        hints.append(
            "Timed out. Increase PG_CONNECT_TIMEOUT_S / PG_TIMEOUT_S or check network egress."
        )
    if "over_capacity" in kinds:
        hints.append(
            "DB overloaded (too many connections). Reduce pool usage and check DB plan limits."
        )
    if not hints:
        hints.append("No candidate connected. Verify DSN values and Postgres reachability.")
    # de-dupe
    out: list[str] = []
    seen = set()
    for h in hints:
        if h not in seen:
            seen.add(h)
            out.append(h)
    return out[:10]


# ---------------------------------------------------------------------
# candidate DSNs + plan building via db_dsn
# ---------------------------------------------------------------------


def _candidate_dsns(cli_dsns: Sequence[str]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for i, d in enumerate(cli_dsns):
        if d and str(d).strip():
            out.append((f"CLI_{i + 1}", str(d).strip()))

    for k in _DSN_KEYS:
        v = _envs(k, "")
        if v:
            out.append((k, v))

    seen = set()
    uniq: list[tuple[str, str]] = []
    for lbl, raw in out:
        key = raw.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        uniq.append((lbl, key))
    return uniq


@contextlib.contextmanager
def _temp_env(overrides: dict[str, str | None]) -> Any:
    old: dict[str, str | None] = {}
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
    connect_args: dict[str, Any]
    host: str
    port: int
    db: str
    sslmode: str
    ssl_enabled: bool
    host_resolves: int


def _build_plan(
    label: str, raw: str, *, timeout_s: float, connect_timeout_s: float, app_name: str
) -> AttemptPlan:
    if dsnmod is None:
        raise RuntimeError(f"missing_deps:utils_pkg.db_dsn import failed: {_DB_DSN_IMPORT_ERR}")

    # Force db_dsn selection to pick this DSN (same technique as check_schema)
    overrides = {k: None for k in _DSN_KEYS}
    overrides["POSTGRES_DSN"] = raw

    with _temp_env(overrides):
        pr = dsnmod.pick_pg_dsn_debug(timeout_s=timeout_s, app_name=app_name)

    dsn_sa = str(getattr(pr, "dsn_sa", "") or "")
    if not dsn_sa:
        raise RuntimeError("bad_format:empty_sanitized_dsn")

    u = urlparse(dsn_sa)
    host = str(getattr(pr, "host", "") or (u.hostname or ""))
    port = int(u.port or 5432)
    db = (u.path or "").lstrip("/").split("/", 1)[0] if u.path else ""

    ca = dict(getattr(pr, "connect_args", {}) or {})
    if connect_timeout_s and connect_timeout_s > 0:
        ca.setdefault("timeout", float(connect_timeout_s))

    return AttemptPlan(
        label=label,
        raw=raw,
        dsn_sa=dsn_sa,
        connect_args=ca,
        host=host,
        port=port,
        db=db,
        sslmode=str(getattr(pr, "sslmode", "") or ""),
        ssl_enabled=bool(getattr(pr, "ssl_enabled", False)),
        host_resolves=int(bool(host and _resolves(host))),
    )


# ---------------------------------------------------------------------
# core check (async, SQLAlchemy+asyncpg)
# ---------------------------------------------------------------------


async def _run_one(
    plan: AttemptPlan,
    *,
    json_only: bool,
    attempt_timeout_s: float,
) -> dict[str, Any]:
    t0 = time.monotonic()
    out: dict[str, Any] = {
        "label": plan.label,
        "ok": False,
        "dsn": _mask_dsn(plan.raw),
        "dsn_sa": _mask_dsn(plan.dsn_sa),
        "host": plan.host,
        "host_resolves": plan.host_resolves,
        "port": plan.port,
        "db_hint": plan.db,
        "sslmode": plan.sslmode,
        "ssl_enabled": int(bool(plan.ssl_enabled)),
        "driver": "sqlalchemy+asyncpg",
        "ips": [],
        "tcp_probe_ok": None,
        "tcp_probe": None,
        "ping_ms": None,
        "sec": None,
        "details": {},
        "error": "",
        "error_kind": "",
    }

    # deps
    if create_async_engine is None or text is None:
        out["error"] = f"missing_deps:sqlalchemy:{type(_SA_IMPORT_ERR).__name__}:{_SA_IMPORT_ERR}"
        out["error_kind"] = "missing_deps"
        out["sec"] = round(time.monotonic() - t0, 3)
        return out

    dns_debug = _envb("PG_DNS_DEBUG", True)
    tcp_probe = _envb("PG_TCP_PROBE", True)

    if dns_debug and plan.host:
        out["ips"] = _dns_debug(plan.host, plan.port)
    if tcp_probe and plan.host:
        okp, msgp = _tcp_probe(
            plan.host, plan.port, timeout_s=min(6.0, max(2.0, attempt_timeout_s * 0.35))
        )
        out["tcp_probe_ok"] = int(bool(okp))
        out["tcp_probe"] = msgp

    eng = create_async_engine(
        plan.dsn_sa,
        connect_args=dict(plan.connect_args or {}),
        pool_pre_ping=True,
        pool_size=1,
        max_overflow=0,
        pool_timeout=max(3.0, float(attempt_timeout_s)),
    )

    retries, base = _retry_cfg()
    last_err: BaseException | None = None

    try:
        for attempt in range(retries + 1):
            try:
                async with eng.connect() as conn:
                    # session timeouts (best-effort)
                    stmt_ms = int(_envi("PG_STATEMENT_TIMEOUT_MS", 15000))
                    lock_ms = int(_envi("PG_LOCK_TIMEOUT_MS", 4000))
                    idle_ms = int(_envi("PG_IDLE_TX_TIMEOUT_MS", 8000))
                    with contextlib.suppress(Exception):
                        await conn.exec_driver_sql(f"SET statement_timeout = {max(1500, stmt_ms)}")
                    with contextlib.suppress(Exception):
                        await conn.exec_driver_sql(f"SET lock_timeout = {max(500, lock_ms)}")
                    with contextlib.suppress(Exception):
                        await conn.exec_driver_sql(
                            f"SET idle_in_transaction_session_timeout = {max(1500, idle_ms)}"
                        )

                    # ping latency
                    tping = time.perf_counter()
                    row = (await conn.execute(text("SELECT 1"))).first()
                    out["ping_ms"] = round((time.perf_counter() - tping) * 1000.0, 2)
                    if not row or row[0] != 1:
                        raise RuntimeError("select1_failed")

                    # read-only signal + recovery + identity
                    await conn.exec_driver_sql("BEGIN READ ONLY")
                    try:
                        ver = (await conn.execute(text("SELECT version()"))).scalar()
                        out["details"]["version"] = str(ver) if ver is not None else ""

                        r = (
                            await conn.execute(text("SELECT current_database(), current_user"))
                        ).first()
                        if r:
                            out["details"]["db"] = str(r[0])
                            out["details"]["user"] = str(r[1])

                        ro = (await conn.execute(text("SHOW transaction_read_only"))).scalar()
                        out["details"]["transaction_read_only"] = (
                            str(ro).strip().lower() if ro is not None else ""
                        )

                        with contextlib.suppress(Exception):
                            rec = (await conn.execute(text("SELECT pg_is_in_recovery()"))).scalar()
                            out["details"]["in_recovery"] = int(bool(rec))

                        with contextlib.suppress(Exception):
                            addr = (await conn.execute(text("SELECT inet_server_addr()"))).scalar()
                            out["details"]["server_addr"] = str(addr) if addr is not None else ""

                        with contextlib.suppress(Exception):
                            caddr = (await conn.execute(text("SELECT inet_client_addr()"))).scalar()
                            out["details"]["client_addr"] = str(caddr) if caddr is not None else ""

                        # best-effort: was SSL used for this backend?
                        with contextlib.suppress(Exception):
                            ssl_used = (
                                await conn.execute(
                                    text("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()")
                                )
                            ).scalar()
                            if ssl_used is not None:
                                out["details"]["ssl_used"] = int(bool(ssl_used))

                    finally:
                        with contextlib.suppress(Exception):
                            await conn.exec_driver_sql("ROLLBACK")

                out["ok"] = True
                return out

            except Exception as e:
                last_err = e
                msg = f"{type(e).__name__}: {e}"
                out["error"] = msg
                out["error_kind"] = _classify_error(msg)
                if attempt < retries and out["error_kind"] in (
                    "timeout",
                    "server_closed_mid_op",
                    "ssl",
                    "other",
                ):
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


# ---------------------------------------------------------------------
# main
# ---------------------------------------------------------------------


async def _amain() -> int:
    RC_OK = 0
    RC_FATAL = 1
    RC_NOT_OK = 2
    RC_TIMEOUT = 124

    ap = argparse.ArgumentParser(description="God-Tier Postgres health check (P0 LOCK-IN).")
    ap.add_argument(
        "--dsn",
        action="append",
        default=[],
        help="DSN override (repeatable). If omitted, uses env candidates.",
    )
    ap.add_argument("--try-all", action="store_true", help="Try all candidates even if one passes.")
    ap.add_argument("--json", action="store_true", help="Emit JSON summary to STDOUT.")
    ap.add_argument("--json-only", action="store_true", help="Only emit JSON (no STDERR logs).")
    args = ap.parse_args()

    json_only = bool(args.json_only or _envb("PG_JSON_ONLY", False))
    emit_json = bool(
        args.json or args.json_only or _envb("PG_JSON", False) or _envb("PG_JSON_ONLY", False)
    )

    if _envb("SKIP_DSN_REQUIRED", False):
        payload = {"ts": _ts(), "ok": True, "skipped": True, "reason": "SKIP_DSN_REQUIRED=1"}
        if emit_json:
            print(json.dumps(payload, ensure_ascii=False, default=str))
        _hlog("SKIP (SKIP_DSN_REQUIRED=1)", json_only=json_only)
        return RC_OK

    if dsnmod is None:
        payload = {
            "ts": _ts(),
            "ok": False,
            "skipped": False,
            "error": f"missing_deps:utils_pkg.db_dsn:{_DB_DSN_IMPORT_ERR}",
        }
        if emit_json:
            print(json.dumps(payload, ensure_ascii=False, default=str))
        _hlog(f"FATAL: missing db_dsn ({_DB_DSN_IMPORT_ERR})", json_only=json_only)
        return RC_FATAL

    # timeouts
    connect_timeout_s = max(2.0, float(_envf("PG_CONNECT_TIMEOUT_S", 8.0)))
    timeout_s = max(6.0, float(_envf("PG_TIMEOUT_S", 15.0)))
    attempt_timeout_s = max(timeout_s, connect_timeout_s + 6.0)
    total_timeout_s = max(0.0, float(_envf("PG_TOTAL_TIMEOUT_S", 0.0)))
    started = time.monotonic()

    cands = _candidate_dsns([d for d in args.dsn if d])
    if not cands:
        payload = {
            "ts": _ts(),
            "ok": False,
            "skipped": False,
            "error": "no_dsn_candidates",
            "detail": "Set one of PNL_DSN/POSTGRES_DSN/PG_DSN/DATABASE_URL/PG_DSN_INTERNAL/PG_DSN_EXTERNAL or pass --dsn",
        }
        if emit_json:
            print(json.dumps(payload, ensure_ascii=False, default=str))
        _hlog("FATAL: no DSN candidates", json_only=json_only)
        return RC_FATAL

    if not json_only:
        _hlog("──────────────────────────────", json_only=False)
        _hlog("God-Tier pg_check (P0 LOCK-IN)", json_only=False)
        _hlog("──────────────────────────────", json_only=False)
        _hlog(
            f"connect_timeout_s={connect_timeout_s:.1f} timeout_s={timeout_s:.1f} attempt_timeout_s={attempt_timeout_s:.1f} total_timeout_s={total_timeout_s:.1f}",
            json_only=False,
        )
        _hlog(
            f"try_all={int(bool(args.try_all or _envb('PG_TRY_ALL', False)))} candidates={','.join([k for k, _ in cands])}",
            json_only=False,
        )

    try_all = bool(args.try_all or _envb("PG_TRY_ALL", False))

    results: list[dict[str, Any]] = []
    picked: dict[str, Any] | None = None
    saw_timeout = False
    saw_fatal = False

    app_name = _envs("PG_APP_NAME", "pnlsniper_pg_check") or "pnlsniper_pg_check"

    for label, raw in cands:
        if total_timeout_s > 0 and (time.monotonic() - started) > total_timeout_s:
            saw_timeout = True
            results.append(
                {
                    "label": label,
                    "ok": False,
                    "dsn": _mask_dsn(raw),
                    "error": "total_timeout_exceeded",
                    "error_kind": "timeout",
                }
            )
            break

        # plan via db_dsn normalization
        try:
            plan = _build_plan(
                label,
                raw,
                timeout_s=timeout_s,
                connect_timeout_s=connect_timeout_s,
                app_name=app_name,
            )
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            kind = _classify_error(msg)
            r = {
                "label": label,
                "ok": False,
                "dsn": _mask_dsn(raw),
                "error": msg,
                "error_kind": (kind if kind == "missing_deps" else "bad_format"),
                "sec": 0.0,
            }
            results.append(r)
            saw_fatal = True
            if not json_only:
                _hlog(
                    f"RES {label} ok=0 kind={r['error_kind']} error={r['error']}", json_only=False
                )
            continue

        if not json_only:
            _hlog(
                f"TRY {label} host={plan.host or '-'} port={plan.port} db={plan.db or '-'} ssl_enabled={int(plan.ssl_enabled)} sslmode={plan.sslmode or '-'} dsn={_mask_dsn(raw)}",
                json_only=False,
            )

        try:
            r = await asyncio.wait_for(
                _run_one(plan, json_only=json_only, attempt_timeout_s=attempt_timeout_s),
                timeout=attempt_timeout_s + 3.0,
            )
        except TimeoutError:
            saw_timeout = True
            r = {
                "label": label,
                "ok": False,
                "dsn": _mask_dsn(raw),
                "dsn_sa": _mask_dsn(plan.dsn_sa),
                "host": plan.host,
                "host_resolves": plan.host_resolves,
                "port": plan.port,
                "db_hint": plan.db,
                "sslmode": plan.sslmode,
                "ssl_enabled": int(plan.ssl_enabled),
                "driver": "watchdog",
                "ips": [],
                "tcp_probe_ok": None,
                "tcp_probe": None,
                "ping_ms": None,
                "sec": round(attempt_timeout_s, 3),
                "details": {},
                "error": "attempt_timeout",
                "error_kind": "timeout",
            }

        results.append(r)

        if not json_only:
            _hlog(
                f"RES {label} ok={int(bool(r.get('ok')))} sec={r.get('sec')} ping_ms={r.get('ping_ms')} kind={r.get('error_kind') or '-'}",
                json_only=False,
            )
            if r.get("error"):
                _hlog(f"  error={r['error']}", json_only=False)

        if r.get("ok") and picked is None:
            picked = r
            if not try_all:
                break

        if str(r.get("error_kind") or "") == "timeout":
            saw_timeout = True
        if str(r.get("error_kind") or "") in {"missing_deps", "bad_format"}:
            saw_fatal = True

    summary: dict[str, Any] = {
        "ts": _ts(),
        "ok": bool(picked is not None),
        "skipped": False,
        "picked": (picked.get("label") if picked else None),
        "results": results,
        "fix_hints": _fix_hints(results),
    }

    if emit_json:
        print(json.dumps(summary, ensure_ascii=False, default=str))

    if picked is not None:
        _hlog("SUMMARY ok=1 (db reachable)", json_only=json_only)
        return RC_OK

    if saw_timeout and not saw_fatal:
        _hlog("SUMMARY ok=0 (timeout)", json_only=json_only)
        return RC_TIMEOUT

    if saw_fatal:
        _hlog("SUMMARY ok=0 (fatal)", json_only=json_only)
        return RC_FATAL

    _hlog("SUMMARY ok=0 (db not reachable)", json_only=json_only)
    return RC_NOT_OK


def main() -> None:
    try:
        rc = asyncio.run(_amain())
    except KeyboardInterrupt:
        rc = 130
    except Exception as e:
        _safe_print(f"[pg_check] {_ts()} FAIL unhandled {type(e).__name__}: {e}", file=sys.stderr)
        rc = 1
    raise SystemExit(rc)


if __name__ == "__main__":
    # Windows asyncio stability
    with contextlib.suppress(Exception):
        if os.name == "nt" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]
    main()
