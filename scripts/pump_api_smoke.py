#!/usr/bin/env python3
from __future__ import annotations

"""
scripts/pump_api_smoke.py

Pump.fun HTTP smoke test (GET + POST TRPC) — production-grade dev tool
=====================================================================

Purpose
-------
Quickly validate that your Pump.fun HTTP client layer works end-to-end:
  • DNS / TLS / routing to pump.fun endpoints
  • cookies / CF handling (as implemented inside utils_pkg.pump_client)
  • GET + POST calls to the same TRPC endpoint
  • basic latency + response sanity (bytes + optional preview)

This is a **tool** (P0 hygiene). It must not affect live trading paths.

Depends on
----------
utils_pkg.pump_client:
  - pump_get(path: str, timeout: int|float=...) -> str
  - pump_post(path: str, json_body: dict, timeout: int|float=...) -> str

Env (informational)
-------------------
- PUMPFUN_API_BASES   (comma-separated bases; your pump_client likely rotates/chooses)
- PF_* / cookies      (whatever your pump_client uses internally)

Usage
-----
python -m scripts.pump_api_smoke
python -m scripts.pump_api_smoke --retries 5 --timeout 20
python -m scripts.pump_api_smoke --loop --interval 10 --max-seconds 120
python -m scripts.pump_api_smoke --json --preview 180
python -m scripts.pump_api_smoke --endpoint board.latest
"""

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Repo root + dotenv
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from dotenv import find_dotenv, load_dotenv  # type: ignore

    load_dotenv(find_dotenv(), override=False)
except Exception:
    pass


# ---------------------------------------------------------------------------
# Imports from your codebase (canonical client)
# ---------------------------------------------------------------------------

from utils_pkg.pump_client import pump_get, pump_post


def _ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _split_csv(s: str) -> list[str]:
    return [p.strip() for p in (s or "").replace(";", ",").split(",") if p.strip()]


def _safe_preview(s: str, n: int) -> str:
    if n <= 0:
        return ""
    s = s.replace("\n", " ").replace("\r", " ")
    return s[:n]


async def _call_with_retries(
    label: str,
    fn,
    *,
    retries: int,
    timeout_s: float,
    backoff_s: float,
    preview: int,
    quiet: bool,
) -> tuple[bool, dict[str, Any]]:
    """
    Generic retry wrapper for an awaitable call that returns a str payload.
    Returns (ok, info).
    """
    attempt = 0
    last_err: str | None = None
    while True:
        attempt += 1
        t0 = time.perf_counter()
        try:
            payload: str = await fn(timeout_s)
            dt_ms = (time.perf_counter() - t0) * 1000.0
            info: dict[str, Any] = {
                "label": label,
                "ok": True,
                "attempt": attempt,
                "latency_ms": round(dt_ms, 2),
                "bytes": len(payload.encode("utf-8", errors="ignore")),
            }
            if preview > 0:
                info["preview"] = _safe_preview(payload, preview)
            if not quiet:
                print(
                    f"[pump_api_smoke] {_ts()} {label} OK "
                    f"(attempt={attempt} latency_ms={info['latency_ms']} bytes={info['bytes']})"
                )
            return True, info
        except Exception as e:
            dt_ms = (time.perf_counter() - t0) * 1000.0
            last_err = f"{type(e).__name__}: {e}"
            if not quiet:
                print(
                    f"[pump_api_smoke] {_ts()} {label} FAIL "
                    f"(attempt={attempt} latency_ms={round(dt_ms, 2)} err={last_err})"
                )
            if attempt > retries:
                return False, {
                    "label": label,
                    "ok": False,
                    "attempt": attempt,
                    "latency_ms": round(dt_ms, 2),
                    "error": last_err,
                }
            # backoff with a small cap to avoid huge sleeps
            sleep_s = min(30.0, backoff_s * (2 ** (attempt - 1)))
            await asyncio.sleep(sleep_s)


def _build_paths(endpoint: str) -> tuple[str, str]:
    """
    Pump TRPC: GET uses encoded input; POST uses JSON body.
    We default to { } input which works for board.latest.
    """
    ep = endpoint.strip().lstrip("/")
    if ep.startswith("api/"):
        # user passed a raw path (rare) — trust it and derive POST path
        get_path = "/" + ep
        post_path = get_path.split("?", 1)[0] + "?batch=1" if "?batch=" in get_path else get_path
        return get_path, post_path

    get_path = f"/api/trpc/{ep}?batch=1&input=%7B%7D"  # input={}
    post_path = f"/api/trpc/{ep}?batch=1"
    return get_path, post_path


async def smoke_once(
    *,
    endpoint: str,
    timeout_s: float,
    retries: int,
    backoff_s: float,
    do_get: bool,
    do_post: bool,
    preview: int,
    quiet: bool,
) -> dict[str, Any]:
    get_path, post_path = _build_paths(endpoint)

    results: dict[str, Any] = {
        "ts": _ts(),
        "endpoint": endpoint,
        "get_path": get_path if do_get else None,
        "post_path": post_path if do_post else None,
        "bases_env": _split_csv(os.getenv("PUMPFUN_API_BASES", "")),
        "ok": True,
        "calls": [],
    }

    if do_get:

        async def _do_get(t: float) -> str:
            return await pump_get(get_path, timeout=t)  # type: ignore[arg-type]

        ok, info = await _call_with_retries(
            "GET",
            _do_get,
            retries=retries,
            timeout_s=timeout_s,
            backoff_s=backoff_s,
            preview=preview,
            quiet=quiet,
        )
        results["calls"].append(info)
        results["ok"] = results["ok"] and ok

    if do_post:

        async def _do_post(t: float) -> str:
            body = {"0": {"json": {}}}
            return await pump_post(post_path, json_body=body, timeout=t)  # type: ignore[arg-type]

        ok, info = await _call_with_retries(
            "POST",
            _do_post,
            retries=retries,
            timeout_s=timeout_s,
            backoff_s=backoff_s,
            preview=preview,
            quiet=quiet,
        )
        results["calls"].append(info)
        results["ok"] = results["ok"] and ok

    return results


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Pump.fun API smoke test (GET + POST).")
    p.add_argument(
        "--endpoint", default="board.latest", help="TRPC endpoint, default: board.latest"
    )
    p.add_argument(
        "--timeout", type=float, default=20.0, help="Per-call timeout seconds (default: 20)."
    )
    p.add_argument("--retries", type=int, default=2, help="Retries per call (default: 2).")
    p.add_argument(
        "--backoff", type=float, default=1.5, help="Base backoff seconds (default: 1.5)."
    )

    p.add_argument("--no-get", action="store_true", help="Skip GET call.")
    p.add_argument("--no-post", action="store_true", help="Skip POST call.")

    p.add_argument(
        "--preview",
        type=int,
        default=0,
        help="Include first N chars of response in output (default: 0).",
    )
    p.add_argument("--json", action="store_true", help="Print JSON summary (machine-readable).")
    p.add_argument(
        "--quiet", action="store_true", help="Less chatter (still prints JSON if requested)."
    )

    p.add_argument(
        "--loop", action="store_true", help="Repeat smoke check until max-seconds (or Ctrl+C)."
    )
    p.add_argument(
        "--interval", type=float, default=10.0, help="Loop interval seconds (default: 10)."
    )
    p.add_argument(
        "--max-seconds", type=float, default=0.0, help="Stop after N seconds (0 = infinite)."
    )
    return p.parse_args()


async def run() -> int:
    # Windows stability
    try:
        if os.name == "nt" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]
    except Exception:
        pass

    args = parse_args()

    do_get = not bool(args.no_get)
    do_post = not bool(args.no_post)
    if not do_get and not do_post:
        print(
            "[pump_api_smoke] ERROR: both --no-get and --no-post set; nothing to do.",
            file=sys.stderr,
        )
        return 2

    start = time.monotonic()
    last: dict[str, Any] | None = None

    try:
        while True:
            last = await smoke_once(
                endpoint=str(args.endpoint),
                timeout_s=float(args.timeout),
                retries=max(0, int(args.retries)),
                backoff_s=max(0.2, float(args.backoff)),
                do_get=do_get,
                do_post=do_post,
                preview=max(0, int(args.preview)),
                quiet=bool(args.quiet),
            )

            if args.json:
                print(json.dumps(last, indent=2, default=str))
            else:
                if not args.quiet:
                    print(f"[pump_api_smoke] {_ts()} overall ok={last.get('ok')}")

            if not args.loop:
                break

            if float(args.max_seconds) > 0 and (time.monotonic() - start) >= float(
                args.max_seconds
            ):
                break

            await asyncio.sleep(max(0.2, float(args.interval)))

    except KeyboardInterrupt:
        pass

    if last is None:
        return 2
    return 0 if bool(last.get("ok")) else 3


def main() -> None:
    rc = 3
    try:
        rc = asyncio.run(run())
    except KeyboardInterrupt:
        rc = 0
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
