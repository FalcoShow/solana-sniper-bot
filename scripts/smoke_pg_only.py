from __future__ import annotations

import argparse
import asyncio
import os
import re
import socket
import sys
import time
import ssl
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple
from urllib.parse import parse_qsl, urlparse, urlunparse

import certifi
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

# Optional dotenv (local dev)
try:
    from dotenv import load_dotenv  # type: ignore

    load_dotenv()
except Exception:
    pass

# Ensure repo root on sys.path
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from utils_pkg.db_dsn import pick_pg_dsn  # noqa: E402


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _mask_dsn(dsn: str) -> str:
    """Hide password but keep host/db visible."""
    try:
        u = urlparse(dsn)
        if not u.username:
            return dsn
        netloc = u.netloc
        # Replace user:pass@ with user:***@
        netloc = re.sub(rf"{re.escape(u.username)}:([^@]+)@", f"{u.username}:***@", netloc)
        return urlunparse((u.scheme, netloc, u.path, u.params, u.query, u.fragment))
    except Exception:
        return "<dsn>"


def _ensure_sqlalchemy_asyncpg_url(dsn: str) -> str:
    """
    SQLAlchemy AsyncEngine must use postgresql+asyncpg://
    Accepts postgres://, postgresql://, postgresql+asyncpg://
    """
    dsn = (dsn or "").strip()
    if dsn.startswith("postgresql+asyncpg://"):
        return dsn
    if dsn.startswith("postgres://"):
        return "postgresql+asyncpg://" + dsn[len("postgres://") :]
    if dsn.startswith("postgresql://"):
        return "postgresql+asyncpg://" + dsn[len("postgresql://") :]
    return dsn


def _split_and_strip_query(url: str) -> Tuple[str, Dict[str, str]]:
    """
    Return (url_without_query, query_dict).
    IMPORTANT: SQLAlchemy asyncpg dialect forwards *all* URL query params
    to asyncpg.connect(**kwargs). Unknown keys will crash.
    So we strip the query entirely after extracting what we need.
    """
    u = urlparse(url)
    q = dict(parse_qsl(u.query, keep_blank_values=True))
    stripped = urlunparse((u.scheme, u.netloc, u.path, u.params, "", u.fragment))
    return stripped, q


def _dns_debug(host: str, port: int) -> list[str]:
    try:
        infos = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
        ips: list[str] = []
        for _fam, _typ, _pro, _cn, sockaddr in infos:
            ip = sockaddr[0]
            if ip not in ips:
                ips.append(ip)
        return ips
    except Exception:
        return []


def _tcp_probe(host: str, port: int, timeout_s: float = 3.0) -> Tuple[bool, str]:
    try:
        with socket.create_connection((host, port), timeout=timeout_s):
            return True, "ok"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def _ssl_ctx_for_mode(sslmode: str, insecure: bool) -> Optional[ssl.SSLContext]:
    """
    Translate libpq sslmode into asyncpg's `ssl=` argument (SSLContext).
    """
    mode = (sslmode or "").strip().lower()
    if mode in ("", "disable", "0", "false", "off"):
        return None

    # require / verify-ca / verify-full all become "use SSL".
    ctx = ssl.create_default_context(cafile=certifi.where())

    if insecure:
        # Debug-only / last resort. Not recommended for production.
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

    return ctx


@dataclass
class PgSanitized:
    raw_dsn: str
    sa_url: str
    host: str
    port: int
    db: str
    sslmode: str
    ssl_enabled: bool
    connect_args: Dict[str, Any]


def _sanitize_for_asyncpg(
    dsn: str,
    connect_args: Optional[Dict[str, Any]],
    *,
    timeout_s: float,
    app_name: str,
    sslmode_override: Optional[str] = None,
    insecure_ssl: bool = False,
) -> PgSanitized:
    """
    Build a SQLAlchemy URL safe for asyncpg AND asyncpg-friendly connect_args.
    Removes URL query params entirely to prevent asyncpg unexpected-kw crashes.
    """
    dsn = (dsn or "").strip()
    if not dsn:
        raise ValueError("empty DSN")

    sa_url = _ensure_sqlalchemy_asyncpg_url(dsn)

    # Strip ALL query params (asyncpg will crash on unknown ones), but capture sslmode
    sa_url_noq, q = _split_and_strip_query(sa_url)
    sslmode = (sslmode_override or q.get("sslmode") or os.getenv("PG_SSLMODE") or "").strip() or ""
    # Render external DSNs often use sslmode=require; if it's there, we must convert to SSLContext

    # Build asyncpg connect_args
    ca: Dict[str, Any] = dict(connect_args or {})

    # Hard remove anything that can crash asyncpg
    ca.pop("sslmode", None)  # asyncpg does not accept this
    ca.pop("ssl", None)      # we will set properly
    # safe defaults for smoke tests
    ca["statement_cache_size"] = int(os.getenv("PG_STATEMENT_CACHE_SIZE", "0") or "0")
    ca["command_timeout"] = float(timeout_s)

    ss = ca.get("server_settings")
    if not isinstance(ss, dict):
        ss = {}
    ss.setdefault("application_name", app_name)
    ca["server_settings"] = ss

    # SSL
    ssl_required = sslmode.strip().lower() in {"require", "verify-ca", "verify-full"}
    ssl_insecure_env = (os.getenv("PG_SSL_INSECURE", "") or "").strip().lower() in {"1", "true", "yes", "y", "on"}
    insecure = bool(insecure_ssl or ssl_insecure_env)
    ssl_ctx = _ssl_ctx_for_mode(sslmode, insecure=insecure)
    if ssl_ctx is not None:
        ca["ssl"] = ssl_ctx
        ssl_enabled = True
    else:
        ssl_enabled = False

    u = urlparse(sa_url_noq)
    host = u.hostname or ""
    port = int(u.port or 5432)
    db = (u.path or "").lstrip("/")

    return PgSanitized(
        raw_dsn=dsn,
        sa_url=sa_url_noq,
        host=host,
        port=port,
        db=db,
        sslmode=sslmode,
        ssl_enabled=ssl_enabled,
        connect_args=ca,
    )


async def _try_select1(sa_url: str, connect_args: Dict[str, Any], timeout_s: float) -> None:
    # Tiny pool: smoke test only
    eng = create_async_engine(
        sa_url,
        connect_args=connect_args,
        pool_pre_ping=True,
        pool_size=1,
        max_overflow=0,
        pool_timeout=timeout_s,
    )
    try:
        async with eng.connect() as conn:
            row = (await conn.execute(text("SELECT 1"))).first()
            if not row:
                raise RuntimeError("SELECT 1 returned no row")
            print("✅ PG SELECT 1 =>", row[0])
    finally:
        await eng.dispose()


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
async def main() -> int:
    ap = argparse.ArgumentParser(description="Smoke test Postgres connectivity via SQLAlchemy asyncpg.")
    ap.add_argument("--timeout", type=float, default=12.0, help="Per-attempt timeout seconds.")
    ap.add_argument("--tries", type=int, default=3, help="Number of attempts.")
    ap.add_argument(
        "--sslmode",
        type=str,
        default="",
        help="Override sslmode (e.g. require/disable). If empty, extracted from DSN/env.",
    )
    ap.add_argument(
        "--insecure",
        action="store_true",
        help="Disable SSL verification (debug only). Also honored via PG_SSL_INSECURE=1.",
    )
    ap.add_argument(
        "--app-name",
        type=str,
        default="pnlsniper_smoke_pg_only",
        help="application_name sent to Postgres.",
    )
    args = ap.parse_args()

    dsn, connect_args = pick_pg_dsn()
    if not dsn:
        print("❌ No DSN available. Set POSTGRES_DSN / PG_DSN_INTERNAL / DATABASE_URL etc.")
        return 2

    sslmode_override = args.sslmode.strip() or None

    try:
        S = _sanitize_for_asyncpg(
            dsn,
            connect_args,
            timeout_s=float(args.timeout),
            app_name=str(args.app_name),
            sslmode_override=sslmode_override,
            insecure_ssl=bool(args.insecure),
        )
    except Exception as e:
        print("❌ DSN sanitize failed:", type(e).__name__, str(e))
        return 2

    # Debug info (masked)
    print("[smoke_pg_only] DSN:", _mask_dsn(S.raw_dsn))
    print("[smoke_pg_only] SQLAlchemy URL (sanitized):", _mask_dsn(S.sa_url))
    if S.host:
        ips = _dns_debug(S.host, S.port)
        ok, why = _tcp_probe(S.host, S.port, timeout_s=min(3.0, float(args.timeout)))
        print(f"[smoke_pg_only] host={S.host} port={S.port} db={S.db or '(none)'} ips={ips if ips else '(none)'}")
        print(f"[smoke_pg_only] tcp_probe={ok} ({why})")
    print(f"[smoke_pg_only] sslmode={S.sslmode or '(none)'} ssl_enabled={S.ssl_enabled}")
    print(f"[smoke_pg_only] connect_args keys={sorted([k for k in S.connect_args.keys() if k != 'ssl'])} ssl={'yes' if 'ssl' in S.connect_args else 'no'}")

    last_err: Exception | None = None
    for i in range(1, int(args.tries) + 1):
        t0 = time.time()
        try:
            print(f"[smoke_pg_only] TRY {i}/{args.tries} timeout={args.timeout}s")
            await asyncio.wait_for(
                _try_select1(S.sa_url, S.connect_args, float(args.timeout)),
                timeout=float(args.timeout) + 2.0,
            )
            print(f"[smoke_pg_only] OK in {int((time.time() - t0) * 1000)}ms")
            return 0
        except Exception as e:
            last_err = e
            print(f"[smoke_pg_only] FAIL {i}/{args.tries}: {type(e).__name__}: {e}")
            await asyncio.sleep(0.6)

    print("[smoke_pg_only] ❌ All attempts failed.")
    if last_err:
        print("Last error:", type(last_err).__name__, str(last_err))
    return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
