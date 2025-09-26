import os, sys, asyncio
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils_pkg.core_utils import init_resources, close_resources

async def main():
    try:
        await init_resources()
    except Exception as e:
        print("⚠️ init_resources failed:", e)
        print("Hint: For a quick Postgres-only check, run scripts/smoke_pg_only.py")
        raise
    finally:
        try:
            await close_resources()
        except Exception:
            pass

if __name__ == "__main__":
    asyncio.run(main())
