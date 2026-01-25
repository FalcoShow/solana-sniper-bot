# scripts/onchain_scan.py
from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import signal
import sys
import time
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_dotenv(dotenv_path: str | None) -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        return
    if dotenv_path:
        load_dotenv(dotenv_path, override=False)
    else:
        load_dotenv(override=False)


def _coerce_list(value: str | None) -> list[str]:
    if not value:
        return []
    parts = [p.strip() for p in value.replace(";", ",").split(",")]
    return [p for p in parts if p]


def _json_default(o: Any) -> Any:
    if is_dataclass(o):
        return asdict(o)
    if hasattr(o, "__dict__"):
        return dict(o.__dict__)
    return str(o)


def _event_to_dict(ev: Any) -> dict[str, Any]:
    # Supports: dataclass objects, plain objects with attrs, dict-like events
    if isinstance(ev, dict):
        return dict(ev)
    if is_dataclass(ev):
        return asdict(ev)
    d: dict[str, Any] = {}
    for k in (
        "slot",
        "signature",
        "sig",
        "mint",
        "program_id",
        "keywords_hit",
        "logs",
        "ts",
        "ts_ms",
    ):
        if hasattr(ev, k):
            d[k] = getattr(ev, k)
    if not d and hasattr(ev, "__dict__"):
        d = dict(ev.__dict__)
    return d


async def _run_stream_events(
    rpc_http: str,
    rpc_ws: str,
    program_ids: list[str],
    commitment: str,
    max_events: int,
    pretty: bool,
) -> int:
    import inspect

    from utils_pkg import onchain_scanner as oc  # your canonical module

    if not hasattr(oc, "stream_events"):
        return 0

    fn = oc.stream_events
    sig = inspect.signature(fn)

    kwargs: dict[str, Any] = {}
    if "rpc_http" in sig.parameters:
        kwargs["rpc_http"] = rpc_http
    if "rpc_ws" in sig.parameters:
        kwargs["rpc_ws"] = rpc_ws
    if "program_ids" in sig.parameters:
        kwargs["program_ids"] = program_ids
    if "commitment" in sig.parameters:
        kwargs["commitment"] = commitment

    n = 0
    async for ev in fn(**kwargs):
        n += 1
        payload = _event_to_dict(ev)
        # Normalize a few fields for consistency in output
        if "sig" in payload and "signature" not in payload:
            payload["signature"] = payload["sig"]
        payload.setdefault("ts_ms", int(time.time() * 1000))

        if pretty:
            print(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default))
        else:
            print(json.dumps(payload, ensure_ascii=False, default=_json_default))

        if n >= max_events:
            break

    return n


async def _run_legacy_onchain_scan(
    max_events: int,
    pretty: bool,
) -> int:
    """
    Legacy fallback: utils_pkg.onchain_scanner.onchain_scan(handle_cb)
    where handle_cb receives a dict like:
      {"slot":..., "signature":..., "mint":..., "logs":[...], "ts":...}
    """
    from utils_pkg.onchain_scanner import onchain_scan  # type: ignore

    n = 0

    async def handle(ev: Any) -> None:
        nonlocal n
        n += 1
        payload = _event_to_dict(ev)
        payload.setdefault("ts_ms", int(time.time() * 1000))

        if pretty:
            print(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default))
        else:
            print(json.dumps(payload, ensure_ascii=False, default=_json_default))

        # Stop condition: raise CancelledError by cancelling the task
        if n >= max_events:
            raise asyncio.CancelledError()

    with contextlib.suppress(asyncio.CancelledError):
        await onchain_scan(handle)

    return n


async def main() -> int:
    p = argparse.ArgumentParser(description="Onchain logs scanner (Pump/program log watcher).")
    p.add_argument("--dotenv", default=None, help="Path to .env file (optional).")
    p.add_argument("--rpc", default=None, help="SOL RPC HTTP URL (or env SOL_RPC_URL).")
    p.add_argument("--ws", default=None, help="SOL RPC WS URL (or env SOL_WS_URL).")
    p.add_argument(
        "--program",
        action="append",
        default=[],
        help="Program ID to watch (repeatable). Falls back to env PUMP_PROGRAM_ID / PROGRAM_IDS.",
    )
    p.add_argument(
        "--commitment", default="confirmed", help="Commitment (confirmed/finalized/processed)."
    )
    p.add_argument("--max-events", type=int, default=0, help="Stop after N events (0 = infinite).")
    p.add_argument("--one", action="store_true", help="Stop after 1 event.")
    p.add_argument("--pretty", action="store_true", help="Pretty-print JSON.")
    args = p.parse_args()

    _load_dotenv(args.dotenv)

    rpc_http = (args.rpc or os.environ.get("SOL_RPC_URL", "")).strip()
    if not rpc_http:
        print("ERROR: SOL_RPC_URL is not set (and --rpc was not provided).", file=sys.stderr)
        print("Set it in your .env or PowerShell, e.g.:", file=sys.stderr)
        print('  $env:SOL_RPC_URL="https://YOUR_RPC_HTTP"', file=sys.stderr)
        return 2

    # Use your module’s derive_ws_url when available
    ws_env = (args.ws or os.environ.get("SOL_WS_URL", "")).strip()
    rpc_ws = ws_env
    if not rpc_ws:
        try:
            from utils_pkg.onchain_scanner import derive_ws_url  # type: ignore

            rpc_ws = derive_ws_url(rpc_http)
        except Exception:
            # very safe fallback
            u = rpc_http.replace("https://", "").replace("http://", "")
            rpc_ws = "wss://" + u

    programs = list(args.program or [])
    if not programs:
        programs = _coerce_list(os.environ.get("PROGRAM_IDS", "")) or _coerce_list(
            os.environ.get("PUMP_PROGRAM_ID", "")
        )

    if not programs:
        print("ERROR: No program IDs provided.", file=sys.stderr)
        print(
            "Provide --program <ID> or set PUMP_PROGRAM_ID / PROGRAM_IDS in env.", file=sys.stderr
        )
        return 2

    max_events = 1 if args.one else (args.max_events if args.max_events > 0 else 10**18)

    # Handle Ctrl+C cleanly
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()

    def _sigint(*_a: Any) -> None:
        stop.set()

    try:
        loop.add_signal_handler(signal.SIGINT, _sigint)
        loop.add_signal_handler(signal.SIGTERM, _sigint)
    except Exception:
        # Windows can be limited here; fine.
        pass

    # Prefer new API; fallback to legacy
    printed = 0
    try:
        printed = await _run_stream_events(
            rpc_http=rpc_http,
            rpc_ws=rpc_ws,
            program_ids=programs,
            commitment=args.commitment,
            max_events=max_events,
            pretty=args.pretty,
        )
        if printed == 0:
            printed = await _run_legacy_onchain_scan(max_events=max_events, pretty=args.pretty)
    finally:
        # If your price_feed or other modules keep global sessions, you can close here too
        try:
            from utils_pkg import price_feed  # type: ignore

            if hasattr(price_feed, "close"):
                await price_feed.close()
        except Exception:
            pass

    return 0 if printed > 0 else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
