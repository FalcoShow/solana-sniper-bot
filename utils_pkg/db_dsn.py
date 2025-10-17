# utils_pkg/db_dsn.py
"""
DSN selector for Postgres
- Prefer INTERNAL host (no domain) only if it resolves (i.e., on Render)
- Otherwise use EXTERNAL host with TLS (asyncpg)
Returns: (dsn: str, connect_args: dict)
Env vars:
  PG_DSN_INTERNAL=postgresql+asyncpg://user:pass@dpg-<id>-a:5432/dbname
  PG_DSN_EXTERNAL=postgresql+asyncpg://user:pass@dpg-<id>-a.ohio-postgres.render.com:5432/dbname
"""
from __future__ import annotations
import os, socket

def _host_from_dsn(dsn: str) -> str:
    after_at = dsn.split("@", 1)[-1]
    hostport = after_at.split("/", 1)[0]
    return hostport.split(":", 1)[0]

def _resolves(host: str) -> bool:
    try:
        socket.getaddrinfo(host, None)
        return True
    except OSError:
        return False

def pick_pg_dsn() -> tuple[str, dict]:
    dsn_int = os.getenv("PG_DSN_INTERNAL", "").strip()
    dsn_ext = os.getenv("PG_DSN_EXTERNAL", "").strip()

    if dsn_int:
        host = _host_from_dsn(dsn_int)
        if _resolves(host):
            # Internal: no TLS for private VPC host
            return dsn_int, {}

    if dsn_ext:
        # External: require TLS for asyncpg
        return dsn_ext, {"ssl": True}

    # Last-ditch: return whatever we have (may still fail at connect time)
    return dsn_int, {}
