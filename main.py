# main.py
from __future__ import annotations

"""
main.py — Canonical entrypoint (God-Tier Overhaul / V3+++ LOCK-IN)
==================================================================

Non-negotiables
---------------
- This is the ONE canonical entrypoint for local + CI + dev ops.
- Render should ultimately call this (directly or via thin wrapper).
- No secrets printed. Ever.
- Defensive imports while the repo is mid-overhaul.

Commands
--------
- run        : start the orchestrator (prefers core.py; falls back to scripts.main)
- selfcheck  : fast sanity checks (http/redis/rpc/wallet/balance/optional router)
- preflight  : run scripts.preflight (if present)
- check-env  : run scripts.check_env (if present)
- config     : print redacted config summary (config.summarize()) if present
"""

import argparse
import asyncio
import inspect
import json
import logging
import os
import signal
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

log = logging.getLogger("main")


# =============================================================================
# Small helpers
# =============================================================================

_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}


def _truthy(v: str | None) -> bool:
    if v is None:
        return False
    return v.strip().lower() in _TRUE


def _safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, default=str, sort_keys=True)
    except Exception:
        try:
            return str(obj)
        except Exception:
            return "<unserializable>"


def _mask(s: str, keep_head: int = 4, keep_tail: int = 4) -> str:
    if not s:
        return ""
    s = str(s)
    if len(s) <= keep_head + keep_tail + 3:
        return s[: max(1, keep_head)] + "…"
    return f"{s[:keep_head]}…{s[-keep_tail:]}"


async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


def _find_callable(mod: Any, names: tuple[str, ...]) -> Callable[..., Any] | None:
    for n in names:
        fn = getattr(mod, n, None)
        if callable(fn):
            return fn
    return None


async def _call_entry(fn: Callable[..., Any], **kwargs: Any) -> Any:
    """
    Calls fn with best-effort kwarg matching:
    - only passes kwargs supported by signature
    - awaits if needed
    """
    try:
        sig = inspect.signature(fn)
        supported = {k: v for k, v in kwargs.items() if k in sig.parameters}
        out = fn(**supported)
    except Exception:
        # if signature introspection fails, try calling raw
        out = fn()
    return await _maybe_await(out)


# =============================================================================
# Logging bootstrap (dedup: prefer root logging_setup.py)
# =============================================================================


def configure_logging() -> None:
    """
    Prefer root logging_setup.setup_logging (canonical).
    Fall back to utils_pkg.logging_setup.setup_logging (wrapper).
    Final fallback: logging.basicConfig.
    """
    # 1) canonical
    try:
        import logging_setup  # type: ignore

        fn = getattr(logging_setup, "setup_logging", None)
        if callable(fn):
            fn()
            return
    except Exception:
        pass

    # 2) wrapper (until dedup is complete)
    try:
        from utils_pkg.logging_setup import setup_logging  # type: ignore

        setup_logging()
        return
    except Exception:
        pass

    # 3) basic fallback
    level = os.environ.get("LOG_LEVEL", "INFO").strip().upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


# =============================================================================
# Config helpers (safe-only)
# =============================================================================


def _load_config_summary() -> dict[str, Any] | None:
    try:
        import config  # type: ignore

        summarize = getattr(config, "summarize", None)
        if callable(summarize):
            return summarize()
        return None
    except Exception:
        return None


def _print_config_summary() -> None:
    s = _load_config_summary()
    if not s:
        print("== CONFIG SUMMARY ==\n<config.summarize() unavailable>")
        return
    print("== CONFIG SUMMARY ==")
    print(_safe_json(s))


def _config_health() -> dict[str, Any]:
    """
    Best-effort: surfaces the strict/non-strict errors/warnings from config.py,
    if present, without crashing if config is mid-rewrite.
    """
    try:
        import config  # type: ignore

        strict = bool(getattr(config, "STRICT_CONFIG", False))
        errs = list(getattr(config, "_CONFIG_ERRORS", []))
        warns = list(getattr(config, "_CONFIG_WARNS", []))
        env = str(getattr(config, "ENV", "") or "")
        phase = int(getattr(config, "MASTER_PHASE", -1))
        return {"strict": strict, "env": env, "phase": phase, "errors": errs, "warnings": warns}
    except Exception as e:
        return {
            "strict": False,
            "env": "",
            "phase": -1,
            "errors": [f"config import failed: {e}"],
            "warnings": [],
        }


# =============================================================================
# Signal handling
# =============================================================================


def _install_signals(loop: asyncio.AbstractEventLoop, stop_evt: asyncio.Event) -> None:
    def _stop() -> None:
        if not stop_evt.is_set():
            stop_evt.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:
            # Windows / limited envs
            signal.signal(sig, lambda *_: _stop())


# =============================================================================
# Resource layer (defensive; canonical is utils_pkg.core_utils)
# =============================================================================


@dataclass
class Resources:
    core_utils: Any = None
    router: Any = None


def _load_resources() -> Resources:
    cu = None
    try:
        from utils_pkg import core_utils as cu  # type: ignore
    except Exception:
        try:
            import utils_pkg as cu  # type: ignore
        except Exception as e:
            raise RuntimeError(f"Cannot import utils_pkg/core_utils: {e}") from e

    router = None
    try:
        from utils_pkg import router as router  # type: ignore
    except Exception:
        router = None

    return Resources(core_utils=cu, router=router)


async def _init_resources(cu: Any) -> None:
    fn = None
    for name in ("init_resources", "init_all", "init"):
        fn = getattr(cu, name, None)
        if callable(fn):
            break
    if callable(fn):
        await _maybe_await(fn())


async def _close_resources(cu: Any) -> None:
    fn = None
    for name in ("close_resources", "close_all", "close"):
        fn = getattr(cu, name, None)
        if callable(fn):
            break
    if callable(fn):
        await _maybe_await(fn())


# =============================================================================
# Selfcheck (fast, useful, no secrets)
# =============================================================================


async def selfcheck(json_out: bool = False, timeout_s: float = 6.0, do_quote: bool = False) -> int:
    """
    Returns:
      0 ok (or mostly ok)
      2 hard failure
    """
    started = time.time()
    report: dict[str, Any] = {
        "ok": True,
        "ts": int(time.time()),
        "elapsed_ms": None,
        "config": _config_health(),
        "checks": {},
    }

    # Surface obvious “why not trading” toggles (from config summarize if available)
    cfg_sum = _load_config_summary() or {}
    kill = (cfg_sum.get("kill") or {}) if isinstance(cfg_sum, dict) else {}
    report["checks"]["trading_toggles"] = {
        "kill_switch": bool(kill.get("kill_switch", False)),
        "disable_buys": bool(kill.get("disable_buys", False)),
        "disable_sells": bool(kill.get("disable_sells", False)),
        "disable_scanners": bool(kill.get("disable_scanners", False)),
        "pause_trading": bool(kill.get("pause_trading", False)),
    }

    rsrc = _load_resources()
    cu = rsrc.core_utils
    await _init_resources(cu)

    # ---- HTTP ----
    http_ok, http_err = False, None
    try:
        import aiohttp

        async with aiohttp.ClientSession() as s:
            async with s.get("https://example.com", timeout=timeout_s) as resp:
                http_ok = 200 <= int(resp.status) < 500
    except Exception as e:
        http_ok, http_err = False, str(e)
    report["checks"]["http"] = {"ok": http_ok, "err": http_err}

    # ---- Redis ----
    redis_ok, redis_err = None, None
    try:
        getr = None
        for name in ("get_redis_async", "get_redis", "redis"):
            fn = getattr(cu, name, None)
            if callable(fn):
                getr = fn
                break
        if callable(getr):
            r = await _maybe_await(getr())
            if r is not None:
                pong = await _maybe_await(r.ping())
                redis_ok = bool(pong)
            else:
                redis_ok = None
        else:
            redis_ok = None
    except Exception as e:
        redis_ok, redis_err = False, str(e)
    report["checks"]["redis"] = {"ok": redis_ok, "err": redis_err}

    # ---- RPC health (Solana JSON-RPC getHealth) ----
    rpc_ok, rpc_err = None, None
    try:
        import aiohttp

        rpc_url = None
        try:
            import config  # type: ignore

            rpc_url = str(getattr(config, "RPC_URL", "") or "") or None
        except Exception:
            rpc_url = os.environ.get("RPC_URL") or os.environ.get("SOLANA_RPC_URL")

        if rpc_url:
            payload = {"jsonrpc": "2.0", "id": 1, "method": "getHealth"}
            async with aiohttp.ClientSession() as s:
                async with s.post(rpc_url, json=payload, timeout=timeout_s) as resp:
                    j = await resp.json(content_type=None)
                    # result is usually "ok"
                    rpc_ok = resp.status < 500 and ("result" in j or "error" in j)
        else:
            rpc_ok = None
    except Exception as e:
        rpc_ok, rpc_err = False, str(e)
    report["checks"]["rpc_health"] = {"ok": rpc_ok, "err": rpc_err}

    # ---- Wallet pubkey (best effort) ----
    wallet_pub = None
    try:
        fn = None
        for name in ("get_wallet_pubkey", "wallet_pubkey", "pubkey"):
            cand = getattr(cu, name, None)
            if callable(cand):
                fn = cand
                break
        if callable(fn):
            wallet_pub = fn()
        elif isinstance(getattr(cu, "WALLET_PUBKEY", None), str):
            wallet_pub = cu.WALLET_PUBKEY
        if isinstance(wallet_pub, str):
            wallet_pub = wallet_pub.strip() or None
    except Exception:
        wallet_pub = None
    report["checks"]["wallet"] = {
        "present": bool(wallet_pub),
        "pubkey_masked": _mask(wallet_pub or ""),
    }

    # ---- SOL balance (if helper exists) ----
    bal_ok, bal_err, bal_sol = None, None, None
    try:
        if wallet_pub:
            fn = None
            for name in ("get_sol_balance", "sol_balance", "get_balance_sol"):
                cand = getattr(cu, name, None)
                if callable(cand):
                    fn = cand
                    break
            if callable(fn):
                bal_sol = float(await _maybe_await(fn(wallet_pub)))
                bal_ok = True
            else:
                bal_ok = None
        else:
            bal_ok = None
    except Exception as e:
        bal_ok, bal_err = False, str(e)
    report["checks"]["sol_balance"] = {"ok": bal_ok, "sol": bal_sol, "err": bal_err}

    # ---- Optional quote sanity (router must be stable; keep off by default) ----
    quote_ok, quote_err = None, None
    if do_quote and rsrc.router is not None:
        try:
            router = rsrc.router
            # Prefer a very small SOL -> SOL "quote" if helpers exist; otherwise skip
            # We intentionally avoid using secrets here.
            fn = getattr(router, "get_sell_quote", None) or getattr(router, "get_quote", None)
            if callable(fn):
                # If the router expects (mint, amount_raw) we pass SOL mint and small lamports.
                sol_mint = os.environ.get("SOL_MINT", "So11111111111111111111111111111111111111112")
                out = fn(mint=sol_mint, amount_raw=1_000_000)  # 0.001 SOL lamports
                _ = await _maybe_await(out)
                quote_ok = True
            else:
                quote_ok = None
        except Exception as e:
            quote_ok, quote_err = False, str(e)
    report["checks"]["router_quote"] = {"ok": quote_ok, "err": quote_err, "enabled": bool(do_quote)}

    # Decide overall ok
    critical_fail = False
    if report["checks"]["http"]["ok"] is False:
        critical_fail = True
    if report["checks"]["redis"]["ok"] is False:
        critical_fail = True
    if report["checks"]["rpc_health"]["ok"] is False:
        critical_fail = True
    if report["config"]["strict"] and report["config"]["errors"]:
        critical_fail = True

    report["ok"] = not critical_fail
    report["elapsed_ms"] = int((time.time() - started) * 1000)

    if json_out:
        print(_safe_json(report))
    else:
        log.info("SELFHECK ok=%s elapsed_ms=%s", report["ok"], report["elapsed_ms"])
        for k, v in report["checks"].items():
            log.info(" - %s: %s", k, _safe_json(v))
        if report["config"].get("errors"):
            log.warning("config errors: %s", " | ".join(map(str, report["config"]["errors"])))
        if report["config"].get("warnings"):
            log.warning("config warnings: %s", " | ".join(map(str, report["config"]["warnings"])))

    await _close_resources(cu)
    return 0 if report["ok"] else 2


# =============================================================================
# Run orchestration (prefers core.py; falls back to scripts.main)
# =============================================================================


async def run_orchestrator(passthrough_args: list[str], stop_evt: asyncio.Event) -> int:
    """
    Prefers:
      1) core.py (post-overhaul canonical)
      2) scripts.main (legacy orchestrator module)
    """
    # Pass-through args for wrappers that still parse argv
    os.environ["MAIN_PASSTHROUGH_ARGS"] = json.dumps(passthrough_args)

    # 1) core.py
    try:
        import core as core_mod  # type: ignore

        entry = _find_callable(core_mod, ("amain", "async_main", "run", "main"))
        if callable(entry):
            log.info("Starting orchestrator via core.%s", entry.__name__)
            await _call_entry(entry, argv=passthrough_args, stop_evt=stop_evt)
            return 0
    except Exception as e:
        log.warning("core.py entry unavailable (falling back): %s", e)

    # 2) scripts.main
    try:
        import scripts.main as sm  # type: ignore

        entry = _find_callable(sm, ("amain", "async_main", "run", "main"))
        if not callable(entry):
            log.error("scripts.main has no callable main/async_main/amain/run")
            return 2

        log.info("Starting orchestrator via scripts.main.%s", entry.__name__)
        await _call_entry(entry, argv=passthrough_args, stop_evt=stop_evt)
        return 0
    except SystemExit as e:
        return int(getattr(e, "code", 0) or 0)
    except Exception as e:
        log.exception("Orchestrator crashed: %s", e)
        return 2


# =============================================================================
# Delegated scripts
# =============================================================================


async def run_script(module_name: str) -> int:
    """
    Runs scripts.<name> with best-effort discovery of entry callable.
    """
    try:
        mod = __import__(module_name, fromlist=["*"])
    except Exception as e:
        log.error("Cannot import %s: %s", module_name, e)
        return 2

    fn = _find_callable(mod, ("amain", "async_main", "run", "main"))
    if not callable(fn):
        log.error("%s has no callable main/async_main/amain/run", module_name)
        return 2

    try:
        await _call_entry(fn)
        return 0
    except SystemExit as e:
        return int(getattr(e, "code", 0) or 0)
    except Exception as e:
        log.exception("%s crashed: %s", module_name, e)
        return 2


# =============================================================================
# CLI
# =============================================================================


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="main.py", add_help=True)
    p.add_argument(
        "--no-config-summary", action="store_true", help="Do not print config summary at boot"
    )
    p.add_argument("--json", action="store_true", help="JSON output (where applicable)")

    sub = p.add_subparsers(dest="cmd", required=False)

    sc = sub.add_parser("selfcheck", help="Fast sanity checks (http/redis/rpc/wallet/balance)")
    sc.add_argument("--timeout", type=float, default=6.0, help="Per-check timeout seconds")
    sc.add_argument(
        "--quote", action="store_true", help="Also attempt a tiny router quote (optional)"
    )

    sub.add_parser("preflight", help="Run scripts.preflight (if present)")
    sub.add_parser("check-env", help="Run scripts.check_env (if present)")
    sub.add_parser("config", help="Print redacted config summary")

    runp = sub.add_parser("run", help="Run orchestrator (core.py preferred; fallback scripts.main)")
    runp.add_argument("args", nargs=argparse.REMAINDER, help="Pass-through args")

    return p


async def _main_async(argv: list[str]) -> int:
    configure_logging()

    parser = _build_parser()
    args = parser.parse_args(argv)

    # Default command if none provided
    cmd = args.cmd or "run"

    if not args.no_config_summary and cmd not in ("config",):
        _print_config_summary()

    if cmd == "config":
        _print_config_summary()
        return 0

    if cmd == "selfcheck":
        return await selfcheck(
            json_out=bool(args.json),
            timeout_s=float(getattr(args, "timeout", 6.0)),
            do_quote=bool(getattr(args, "quote", False)),
        )

    if cmd == "preflight":
        return await run_script("scripts.preflight")

    if cmd == "check-env":
        return await run_script("scripts.check_env")

    if cmd == "run":
        stop_evt = asyncio.Event()
        loop = asyncio.get_running_loop()
        _install_signals(loop, stop_evt)

        passthrough = list(getattr(args, "args", []) or [])
        # normalize: argparse includes leading "--" if user does: main.py run -- --flag
        passthrough = [a for a in passthrough if a != "--"]

        rc = await run_orchestrator(passthrough, stop_evt=stop_evt)
        return int(rc)

    parser.print_help()
    return 2


def main() -> None:
    try:
        rc = asyncio.run(_main_async(sys.argv[1:]))
    except KeyboardInterrupt:
        rc = 130
    raise SystemExit(int(rc))


if __name__ == "__main__":
    main()
