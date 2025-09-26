import os, sys, asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils_pkg.db_dsn import pick_pg_dsn

async def main():
    dsn, connect_args = pick_pg_dsn()
    if not dsn:
        print("❌ No DSN available. Ensure PG_DSN_EXTERNAL/PG_DSN_INTERNAL are set.")
        raise SystemExit(2)
    eng = create_async_engine(dsn, connect_args=connect_args, pool_pre_ping=True)
    async with eng.begin() as conn:
        row = (await conn.execute(text("SELECT 1"))).first()
        print("PG SELECT 1 =>", row[0])
    await eng.dispose()

if __name__ == "__main__":
    asyncio.run(main())
