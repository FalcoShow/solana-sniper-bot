# scripts/dry_run.py
from __future__ import annotations

"""
scripts/dry_run.py  (DEV-ONLY / QUARANTINED)
============================================

Goal
----
A SAFE, quote-only diagnostic harness to validate that your runtime wiring is healthy:
  - Environment loading
  - Resource init/close lifecycle
  - Token price fetching
  - Jupiter quote fetching (NO SWAPS)
  - Optional DB/Redis sanity (best-effort if available)

This file is intentionally "dev-only" and should not be used as the live trading entrypoint.

Safety Contract (hard fences)
-----------------------------
Refuses to run unless BOTH:
  - --yes-i-am-sure
  - ALLOW_DRY_RUN_SCRIPT=1

Additionally:
  - Forces DRY_RUN=1 and disables swaps by default
  - Never calls swap functions
  - Uses timeouts so it cannot hang deploys

Exit Codes
----------
0  = all required checks passed
2  = refused by safety fence
3  = required check failed
4  = unexpected error
"""

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

# --------------------------- pathing / env loading ---------------------------

THIS = Path(__file__).resolve()
ROOT = THIS.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _env_true(v: str | None) -> bool:
    if v is None:
        return False
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def _looks_like_hosted_env() -> bool:
    hosted_keys = (
        "RENDER",
        "RENDER_SERVICE_ID",
        "RENDER_SERVICE_NAME",
        "DYNO",
        "K_SERVICE",
        "AWS_EXECUTION_ENV",
    )
    if any(os.getenv(k) for k in hosted_keys):
        return True
    return os.getenv("ENV", "").strip().lower() in ("prod", "production")


def _load_env_files(env_file: str | None = None) -> None:
    """
    Loads .env (repo root) and Render secret file if present.
    Order:
      1) explicit --env-file
      2) repo .env (if exists)
      3) /etc/secrets/.env.extra (if exists)
    """
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        return

    if env_file:
        p = Path(env_file).expanduser().resolve()
        if p.exists():
            load_dotenv(str(p), override=False)

    # repo .env
    repo_env = ROOT / ".env"
    if repo_env.exists():
        load_dotenv(str(repo_env), override=False)

    # Render-style secrets file
    render_env = Path("/etc/secrets/.env.extra")
    if render_env.exists():
        load_dotenv(str(render_env), override=False)


# --------------------------- logging -----------------------------------------


def _setup_logging(level: str = "INFO") -> logging.Logger:
    """
    Prefer canonical logging_setup.py if present, else fallback.
    """
    lvl = getattr(logging, level.upper(), logging.INFO)
    try:
        # Your P0 contract: root logging_setup.py is canonical
        from logging_setup import setup_logging  # type: ignore

        setup_logging(service="dry_run", level=level)
    except Exception:
        logging.basicConfig(
            level=lvl,
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        )
    return logging.getLogger("scripts.dry_run")


# --------------------------- async helpers -----------------------------------


async def _maybe_await(x: Any) -> Any:
    return await x if asyncio.iscoroutine(x) else x


async def _with_timeout(coro, timeout_s: float, name: str, log: logging.Logger):
    try:
        return await asyncio.wait_for(coro, timeout=timeout_s)
    except asyncio.TimeoutError:
        log.error("timeout: %s after %.1fs", name, timeout_s)
        raise


# --------------------------- adapters (quote-only) ---------------------------


def _resolve_quote_fns(log: logging.Logger) -> tuple[Callable[..., Any], Callable[..., Any]]:
    """
    Returns: (get_token_price, get_jupiter_quote)
    Tries multiple known locations for backward compatibility.
    """
    # core_utils is the primary place in your repo
    try:
        from utils_pkg.core_utils import get_token_price as _gtp  # type: ignore
    except Exception as err:
        # B904: chain the original import error
        raise RuntimeError(f"missing get_token_price in utils_pkg.core_utils: {err}") from err

    # Jupiter quote has moved around in many repos; try a few
    _gjq = None
    try:
        from utils_pkg.core_utils import get_jupiter_quote as _gjq  # type: ignore
    except Exception:
        pass

    if _gjq is None:
        # router-style quote fallback
        try:
            from utils_pkg.router import quote_jupiter as _gjq  # type: ignore
        except Exception:
            _gjq = None

    if _gjq is None:
        raise RuntimeError(
            "missing Jupiter quote function (tried core_utils.get_jupiter_quote and router.quote_jupiter)"
        )

    return _gtp, _gjq


# --------------------------- results model -----------------------------------


@dataclass
class CheckResult:
    name: str
    ok: bool
    ms: int
    detail: str = ""
    data: dict[str, Any] | None = None


@dataclass
class RunSummary:
    ok: bool
    started_at: str
    finished_at: str
    elapsed_ms: int
    hosted_env: bool
    dry_run: str
    checks: list[CheckResult]
    notes: list[str]


# --------------------------- constants ---------------------------------------

WSOL = "So11111111111111111111111111111111111111112"
USDC = "EPjFWdd5Lap8wGweDbosA4Y4J1WKBhG9cYFxaPkmD8n"

# --------------------------- main run ----------------------------------------


async def run_checks(args: argparse.Namespace, log: logging.Logger) -> RunSummary:
    t0 = time.time()
    notes: list[str] = []
    checks: list[CheckResult] = []

    # Hard safety: refuse unless explicitly allowed
    if not args.yes_i_am_sure or not _env_true(os.getenv("ALLOW_DRY_RUN_SCRIPT")):
        raise SystemExit(2)

    if _looks_like_hosted_env() and not args.force_on_hosted:
        log.error("Refusing to run in hosted/prod-like environment without --force-on-hosted.")
        raise SystemExit(2)

    # Force safety defaults
    if not args.allow_non_dry:
        os.environ["DRY_RUN"] = "1"
    os.environ.setdefault("SWAPS_ENABLED", "0")
    os.environ.setdefault("JUPITER_ENABLED", "1")  # quotes allowed
    os.environ.setdefault("LIVE_TRADING", "0")

    # Resolve functions
    get_token_price, get_jupiter_quote = _resolve_quote_fns(log)

    # Init resources
    try:
        from utils_pkg.core_utils import close_resources, init_resources  # type: ignore
    except Exception as err:
        # B904: chain the original import error
        raise RuntimeError(
            f"missing init_resources/close_resources in utils_pkg.core_utils: {err}"
        ) from err

    # Optional helpers
    get_redis_async = None
    ensure_schema = None
    try:
        from utils_pkg.core_utils import get_redis_async as get_redis_async  # type: ignore
    except Exception:
        pass
    try:
        from utils_pkg.ai_db import ensure_schema as ensure_schema  # type: ignore
    except Exception:
        pass

    # --- check: init_resources
    start = time.time()
    try:
        await _with_timeout(init_resources(), args.timeout, "init_resources", log)
        checks.append(CheckResult("init_resources", True, int((time.time() - start) * 1000), "ok"))
    except Exception as e:
        checks.append(
            CheckResult("init_resources", False, int((time.time() - start) * 1000), f"{e}")
        )
        return _final_summary(checks, notes, t0)

    try:
        # --- check: token prices
        start = time.time()
        try:
            wsol_px = await _with_timeout(
                _maybe_await(get_token_price(WSOL)), args.timeout, "get_token_price(WSOL)", log
            )
            usdc_px = await _with_timeout(
                _maybe_await(get_token_price(USDC)), args.timeout, "get_token_price(USDC)", log
            )

            ok = (
                isinstance(wsol_px, int | float)
                and float(wsol_px) > 0
                and isinstance(usdc_px, int | float)
                and float(usdc_px) > 0
            )
            detail = (
                f"WSOL={float(wsol_px):.6f} USDC={float(usdc_px):.6f}" if ok else "invalid price(s)"
            )
            checks.append(
                CheckResult(
                    "token_prices",
                    ok,
                    int((time.time() - start) * 1000),
                    detail,
                    data={"wsol_price": wsol_px, "usdc_price": usdc_px},
                )
            )
            if not ok:
                return _final_summary(checks, notes, t0)
        except Exception as e:
            checks.append(
                CheckResult("token_prices", False, int((time.time() - start) * 1000), f"{e}")
            )
            return _final_summary(checks, notes, t0)

        # --- check: Jupiter quote (WSOL -> USDC)
        start = time.time()
        try:
            lamports = int(float(args.quote_sol) * 1e9)
            quote = await _with_timeout(
                _maybe_await(get_jupiter_quote(WSOL, USDC, lamports)),
                args.timeout,
                "get_jupiter_quote(WSOL->USDC)",
                log,
            )
            ok, detail, payload = _interpret_quote(quote)
            checks.append(
                CheckResult(
                    "jupiter_quote_wsol_usdc",
                    ok,
                    int((time.time() - start) * 1000),
                    detail,
                    payload,
                )
            )
            if not ok and args.require_quote:
                return _final_summary(checks, notes, t0)
        except Exception as e:
            checks.append(
                CheckResult(
                    "jupiter_quote_wsol_usdc", False, int((time.time() - start) * 1000), f"{e}"
                )
            )
            if args.require_quote:
                return _final_summary(checks, notes, t0)

        # --- optional: ensure_schema
        if args.check_db and ensure_schema is not None:
            start = time.time()
            try:
                await _with_timeout(
                    _maybe_await(ensure_schema()), args.timeout, "ensure_schema", log
                )
                checks.append(
                    CheckResult("ensure_schema", True, int((time.time() - start) * 1000), "ok")
                )
            except Exception as e:
                checks.append(
                    CheckResult("ensure_schema", False, int((time.time() - start) * 1000), f"{e}")
                )
                if args.require_db:
                    return _final_summary(checks, notes, t0)
        elif args.check_db:
            notes.append(
                "DB check requested but ensure_schema not available (utils_pkg.ai_db.ensure_schema missing)."
            )

        # --- optional: redis ping
        if args.check_redis and get_redis_async is not None:
            start = time.time()
            try:
                r = await _with_timeout(
                    _maybe_await(get_redis_async()), args.timeout, "get_redis_async", log
                )
                pong = None
                if r:
                    pong = await _with_timeout(r.ping(), args.timeout, "redis.ping", log)
                ok = bool(pong) is True
                checks.append(
                    CheckResult(
                        "redis_ping",
                        ok,
                        int((time.time() - start) * 1000),
                        "ok" if ok else "no pong",
                    )
                )
                if not ok and args.require_redis:
                    return _final_summary(checks, notes, t0)
            except Exception as e:
                checks.append(
                    CheckResult("redis_ping", False, int((time.time() - start) * 1000), f"{e}")
                )
                if args.require_redis:
                    return _final_summary(checks, notes, t0)
        elif args.check_redis:
            notes.append(
                "Redis check requested but get_redis_async not available (utils_pkg.core_utils.get_redis_async missing)."
            )

        return _final_summary(checks, notes, t0)

    finally:
        # Always close resources
        try:
            await _with_timeout(close_resources(), args.timeout, "close_resources", log)
        except Exception as e:
            log.warning("close_resources failed: %s", e)


def _interpret_quote(quote: Any) -> tuple[bool, str, dict[str, Any]]:
    """
    Accepts multiple possible shapes.
    Returns (ok, detail, payload).
    """
    payload: dict[str, Any] = {"raw_type": str(type(quote))}
    if quote is None:
        return False, "no quote (None)", payload

    if isinstance(quote, dict):
        payload["keys"] = list(quote.keys())[:30]
        # Jupiter APIs often return dict with "data" list
        if "data" in quote and isinstance(quote.get("data"), list) and quote["data"]:
            route0 = quote["data"][0]
            out_amt = None
            if isinstance(route0, dict):
                out_amt = route0.get("outAmount") or route0.get("out_amount")
            payload["outAmount"] = out_amt
            return True, f"ok outAmount={out_amt}", payload

        # Some wrappers return route directly
        out_amt = quote.get("outAmount") or quote.get("out_amount")
        if out_amt is not None:
            payload["outAmount"] = out_amt
            return True, f"ok outAmount={out_amt}", payload

        # Or {ok:bool, data:...}
        if "ok" in quote:
            ok = bool(quote.get("ok"))
            return ok, "ok" if ok else "not ok", payload

    # Unknown shape: treat as failure but include repr
    payload["repr"] = repr(quote)[:500]
    return False, "unrecognized quote shape", payload


def _final_summary(checks: list[CheckResult], notes: list[str], t0: float) -> RunSummary:
    elapsed_ms = int((time.time() - t0) * 1000)
    ok = all(c.ok for c in checks) if checks else False
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    start = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(t0)))
    return RunSummary(
        ok=ok,
        started_at=start,
        finished_at=now,
        elapsed_ms=elapsed_ms,
        hosted_env=_looks_like_hosted_env(),
        dry_run=os.getenv("DRY_RUN", ""),
        checks=checks,
        notes=notes,
    )


# --------------------------- CLI ---------------------------------------------


def _parse_args(argv: list[str]) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="DEV-ONLY quote-only dry run checks (NO SWAPS).")
    ap.add_argument("--env-file", default=None, help="explicit .env file path (optional)")
    ap.add_argument(
        "--timeout",
        type=float,
        default=float(os.getenv("DRYRUN_TIMEOUT_S", "20")),
        help="per-check timeout seconds",
    )
    ap.add_argument(
        "--quote-sol",
        type=float,
        default=float(os.getenv("DRYRUN_QUOTE_SOL", "0.01")),
        help="SOL size for Jupiter quote",
    )
    ap.add_argument(
        "--require-quote", action="store_true", help="fail run if quote fails (default: no)"
    )
    ap.add_argument("--check-db", action="store_true", help="attempt ensure_schema (best-effort)")
    ap.add_argument("--require-db", action="store_true", help="fail run if DB check fails")
    ap.add_argument("--check-redis", action="store_true", help="attempt redis.ping (best-effort)")
    ap.add_argument("--require-redis", action="store_true", help="fail run if redis check fails")

    ap.add_argument("--json", action="store_true", help="print JSON summary")
    ap.add_argument("--quiet", action="store_true", help="less logging")
    ap.add_argument(
        "--allow-non-dry", action="store_true", help="do not force DRY_RUN=1 (still no swaps)"
    )

    # hard fences
    ap.add_argument("--yes-i-am-sure", action="store_true", help="required opt-in")
    ap.add_argument(
        "--force-on-hosted",
        action="store_true",
        help="override hosted/prod fence (NOT recommended)",
    )
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    _load_env_files(args.env_file)

    log = _setup_logging("WARNING" if args.quiet else "INFO")

    # Safety fences
    if not args.yes_i_am_sure or not _env_true(os.getenv("ALLOW_DRY_RUN_SCRIPT")):
        log.error("Refusing to run. Need: --yes-i-am-sure and ALLOW_DRY_RUN_SCRIPT=1")
        return 2

    if _looks_like_hosted_env() and not args.force_on_hosted:
        log.error("Refusing to run in hosted/prod-like env without --force-on-hosted.")
        return 2

    try:
        summary = asyncio.run(run_checks(args, log))
        if args.json:
            print(json.dumps(asdict(summary), indent=2, sort_keys=False))
        else:
            print(
                f"[dry_run] ok={summary.ok} elapsed_ms={summary.elapsed_ms} dry_run={summary.dry_run} hosted={summary.hosted_env}"
            )
            for c in summary.checks:
                status = "OK" if c.ok else "FAIL"
                print(f"  - {status:<4} {c.name:<24} {c.ms:>6}ms  {c.detail}")
            for n in summary.notes:
                print(f"  * note: {n}")

        # Exit code based on success
        return 0 if summary.ok else 3

    except SystemExit as se:
        code = int(se.code) if se.code is not None else 4
        return code
    except Exception as e:
        log.exception("unexpected error: %s", e)
        return 4


if __name__ == "__main__":
    raise SystemExit(main())
