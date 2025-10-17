# scripts/test_pg.py
import asyncio, os, socket
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

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

def _redact(dsn: str) -> str:
    parts = dsn.split("://", 1)
    if len(parts) != 2:
        return dsn
    scheme, rest = parts
    if "@" not in rest:
        return dsn
    creds, tail = rest.split("@", 1)
    if ":" in creds:
        user, _pass = creds.split(":", 1)
        creds = f"{user}:********"
    return f"{scheme}://{creds}@{tail}"

def _needs_ssl(dsn: str) -> bool:
    host = _host_from_dsn(dsn)
    return host.endswith(".render.com")

def pick_pg_dsn() -> tuple[str, dict]:
    dsn_int = os.getenv("PG_DSN_INTERNAL", "").strip()
    dsn_ext = os.getenv("PG_DSN_EXTERNAL", "").strip()
    if dsn_int:
        host = _host_from_dsn(dsn_int)
        if _resolves(host):
            return dsn_int, {}
    if dsn_ext:
        return dsn_ext, {"ssl": True}
    return dsn_int or dsn_ext, {}

async def main():
    dsn, connect_args = pick_pg_dsn()
    if not dsn:
        print("❌ No DSN set (PG_DSN_INTERNAL/PG_DSN_EXTERNAL)."); raise SystemExit(1)
    print("Using DSN:", _redact(dsn), "| TLS:", bool(connect_args))

    eng = create_async_engine(
        dsn,
        echo=False,
        pool_pre_ping=True,
        pool_recycle=1800,
        connect_args=connect_args,
    )
    try:
        async with eng.begin() as conn:
            res = await conn.execute(text("SELECT now(), version()"))
            row = res.first()
            print("✅ PG OK:", row[0], "|", row[1].splitlines()[0])
    finally:
        await eng.dispose()

if __name__ == "__main__":
    asyncio.run(main())
