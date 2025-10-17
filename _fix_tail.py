# _fix_tail.py
from pathlib import Path

p = Path("sniper/sniper.py")
src = p.read_text(encoding="utf-8")

# Prefer a clear anchor; fall back to the last "Entrypoint" or last "async def main("
anchors = [
    "# ==============================================================================\n# Entrypoint",
    "# Entrypoint",
]
pos = -1
for a in anchors:
    i = src.rfind(a)
    if i != -1:
        pos = i
        break
if pos == -1:
    i = src.rfind("async def main(")
    if i == -1:
        raise SystemExit("Entrypoint anchor not found. Open sniper/sniper.py and paste the clean tail manually.")
    # go a bit above the 'async def main(' line
    pos = max(0, src.rfind("\n", 0, i))

clean_tail = """
# ==============================================================================
# Entrypoint (clean, no stray indentation)
# ==============================================================================
import inspect, importlib, os, asyncio
log = logging.getLogger("sniper")

async def _maybe_await(x):
    return await x if inspect.iscoroutine(x) else x

def _resolve_callable(path: str):
    \"\"\"Resolve 'module.sub:func' or 'func' (from current module) to a callable.\"\"\"
    if not path:
        return None
    if ":" in path:
        mod_name, fn_name = path.split(":", 1)
        mod = importlib.import_module(mod_name)
        return getattr(mod, fn_name, None)
    return globals().get(path)

async def start_scanners():
    \"\"\"
    Delegates to your scanner loop.
    Priority:
      1) SCANNERS_ENTRY / START_SCANNERS_TARGET -> 'module.path:func'
      2) Common function names in this module or 'scanners' modules
      3) Idle keepalive
    \"\"\"
    env_target = os.getenv("SCANNERS_ENTRY") or os.getenv("START_SCANNERS_TARGET")
    if env_target:
        try:
            fn = _resolve_callable(env_target)
            if callable(fn):
                log.info(f"start_scanners: env target -> {env_target}")
                return await _maybe_await(fn())
            else:
                log.error(f"start_scanners: env target not callable: {env_target}")
        except Exception as e:
            log.exception(f"start_scanners: failed to import env target {env_target}: {e}")

    for name in ("run_sniper", "scan_loop", "scan_forever", "scanner_loop", "run_scanners"):
        fn = globals().get(name)
        if callable(fn):
            log.info(f"start_scanners: delegating to local {name}()")
            return await _maybe_await(fn())

    for mod_name, fn_name in (
        ("scanners", "scan_loop"),
        ("scanners", "scan_forever"),
        ("sniper.scanners", "scan_loop"),
        ("sniper.scanners", "scan_forever"),
    ):
        try:
            mod = importlib.import_module(mod_name)
            fn = getattr(mod, fn_name, None)
            if callable(fn):
                log.info(f"start_scanners: delegating to {mod_name}.{fn_name}()")
                return await _maybe_await(fn())
        except Exception:
            pass

    log.error(
        "start_scanners: no scan loop found. "
        "Set SCANNERS_ENTRY='module.path:func' or provide run_sniper()/scan_loop(). "
        "Idling to keep the container alive."
    )
    while True:
        log.info("scanner: idle heartbeat")
        await asyncio.sleep(int(os.getenv("SCAN_HEARTBEAT_SEC", "10")))

async def scan_loop():
    \"\"\"Minimal runner: start exits + optional PnL watcher, then idle.\"\"\"
    try:
        asyncio.create_task(start_exit_supervisor(get_redis()))
        log.info("scanner: exit supervisor started")
    except Exception as e:
        log.debug("scanner: failed to start exit supervisor: %s", e)

    try:
        from execution.pnl_watcher import start_pnl_watcher
        asyncio.create_task(start_pnl_watcher())
        log.info("scanner: pnl_watcher task started")
    except Exception:
        pass

    while True:
        log.info("scanner: idle heartbeat")
        await asyncio.sleep(int(os.getenv("SCAN_HEARTBEAT_SEC", "10")))

async def run_sniper():
    \"\"\"Default scanner entry: exits + watcher + main loop.\"\"\"
    try:
        asyncio.create_task(start_exit_supervisor(get_redis()))
        log.info("exits supervisor started")
    except Exception as e:
        log.debug("exit supervisor not started: %s", e)

    try:
        from execution.pnl_watcher import watch_positions_loop
        asyncio.create_task(watch_positions_loop())
    except Exception as e:
        log.debug("pnl watcher not started: %s", e)

    await Sniper().main_loop()

async def main():
    await init_resources()

    try:
        await ensure_schema()
    except Exception as e:
        log.warning("ensure_schema failed: %s", e)

    try:
        if ensure_cookie_refresher:
            ensure_cookie_refresher()
        elif cf_refresh_loop:
            asyncio.create_task(cf_refresh_loop())
        else:
            log.info("pumpfun CF refresher not available")
    except Exception as e:
        log.warning("pumpfun cookie refresher not started: %s", e)

    # refresh session/redis refs via accessors if needed
    try:
        global _session_ref, _redis_ref
        if _session_ref is None or _redis_ref is None:
            from utils_pkg.core_utils import get_session as _gs, get_redis as _gr
            _session_ref, _redis_ref = _gs(), _gr()
    except Exception:
        pass

    if os.getenv("SCANNERS_ENTRY") or os.getenv("START_SCANNERS_TARGET"):
        await start_scanners()
    else:
        await run_sniper()

    await close_resources()

if __name__ == "__main__":
    asyncio.run(main())
""".lstrip("\n")

# Normalize tabs -> 4 spaces to avoid weird top-level indents
head = src[:pos].replace("\\t", "    ")

p.write_text(head + clean_tail, encoding="utf-8")
print("Patched sniper/sniper.py from Entrypoint marker → EOF.")
