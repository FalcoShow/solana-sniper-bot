import os, sys, asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
try:
    from dotenv import load_dotenv
    load_dotenv()  # load .env from project root if present
except Exception:
    pass

# Ensure repo root on sys.path if run as a script (not -m)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    from utils_pkg.db_dsn import pick_pg_dsn
except Exception as e:
    print("❌ Unable to import pick_pg_dsn from utils_pkg.db_dsn:", e)
    raise

def _redact(dsn: str) -> str:
    if not dsn:
        return ""
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

async def main():
    dsn, connect_args = pick_pg_dsn()
    if not dsn:
        print("⚠️  No DSN found. Check PG_DSN_EXTERNAL / PG_DSN_INTERNAL in your environment or .env file.")
        print("    Example external: postgresql+asyncpg://user:pass@dpg-<id>-a.ohio-postgres.render.com:5432/sniper1")
        print("    Example internal: postgresql+asyncpg://user:pass@dpg-<id>-a:5432/sniper1")
        raise SystemExit(2)
    host = dsn.split("@",1)[-1].split("/",1)[0]
    print("Chosen host:", host, "| TLS:", bool(connect_args))
    print("Using DSN:", _redact(dsn))
    eng = create_async_engine(dsn, connect_args=connect_args, pool_pre_ping=True)
    async with eng.begin() as conn:
        row = (await conn.execute(text("select now(), current_database(), version()"))).first()
        print("now() =", row[0], "| db =", row[1])
        print(row[2].splitlines()[0])
    await eng.dispose()

if __name__ == "__main__":
    asyncio.run(main())
