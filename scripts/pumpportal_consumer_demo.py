#!/usr/bin/env python3
from __future__ import annotations

"""
scripts/pumpportal_consumer_demo.py

PumpPortal event consumer (dev tool, production-grade safety)
=============================================================

Purpose
-------
A clean, deterministic consumer for PumpPortal websocket events via:

    utils_pkg.pumpportal_client.PumpPortalClient

This is a **dev / ops tool** (quarantine-friendly) used to:
- verify connectivity + auth,
- inspect event shapes (new_token / trade / warning / info),
- optionally write JSONL for later analysis.

Design goals (P0-style)
-----------------------
- No secrets in logs (masked URI only).
- Graceful shutdown (Ctrl+C).
- Robust event parsing (tolerant to schema drift).
- Optional filters + JSON output + file sink.
- Deterministic exit codes.

Env
---
PumpPortalClient reads env vars itself. Common ones (depending on your implementation):
- PUMPPORTAL_WS_URL / PUMPPORTAL_URI / PUMPPORTAL_KEY / etc.

CLI
---
python -m scripts.pumpportal_consumer_demo
python -m scripts.pumpportal_consumer_demo --kinds new_token,trade
python -m scripts.pumpportal_consumer_demo --mint So111... --kinds trade --min-amt 0.5
python -m scripts.pumpportal_consumer_demo --json --max-events 200
python -m scripts.pumpportal_consumer_demo --out ./pumpportal_events.jsonl --max-seconds 120

Exit codes
----------
0 clean exit
2 config/client import error
3 runtime/stream error (after retries)
"""

import argparse
import asyncio
import contextlib
import json
import os
import signal
import sys
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Repo root + dotenv
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

with contextlib.suppress(Exception):
    from dotenv import find_dotenv, load_dotenv  # type: ignore

    load_dotenv(find_dotenv(), override=False)

# ---------------------------------------------------------------------------
# Client import (canonical)
# ---------------------------------------------------------------------------

try:
    from utils_pkg.pumpportal_client import PumpPortalClient  # type: ignore
except Exception as e:
    print(
        "[pumpportal_consumer_demo] ERROR: cannot import PumpPortalClient: "
        f"{type(e).__name__}: {e}",
        file=sys.stderr,
    )
    raise SystemExit(2) from e


_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _now() -> float:
    return time.monotonic()


def _split_csv(s: str) -> list[str]:
    return [p.strip() for p in (s or "").replace(";", ",").split(",") if p.strip()]


def _safe_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str)


def _get(d: dict[str, Any], key: str, default: Any = None) -> Any:
    try:
        return d.get(key, default)
    except Exception:
        return default


def _normalize_kind(k: str) -> str:
    return str(k or "").strip().lower()


@dataclass(frozen=True)
class Filters:
    kinds: set[str] | None
    mint: str | None
    symbol: str | None
    tx_type: str | None
    min_amt: float | None
    max_amt: float | None


def _passes_filters(kind: str, raw: dict[str, Any], flt: Filters) -> bool:
    k = _normalize_kind(kind)

    if flt.kinds is not None and k not in flt.kinds:
        return False

    if flt.mint:
        mint = str(_get(raw, "mint", "") or "").strip()
        if mint != flt.mint:
            return False

    if flt.symbol:
        sym = str(_get(raw, "symbol", "") or "").strip()
        if sym.lower() != flt.symbol.lower():
            return False

    if flt.tx_type:
        tx = str(_get(raw, "txType", "") or _get(raw, "tx_type", "") or "").strip()
        if tx.lower() != flt.tx_type.lower():
            return False

    # Amount filters (best effort)
    if flt.min_amt is not None or flt.max_amt is not None:
        amt = _get(raw, "uiAmount", None)
        if amt is None:
            amt = _get(raw, "amount", None)
        try:
            amt_f = float(amt)
        except Exception:
            return False

        if flt.min_amt is not None and amt_f < flt.min_amt:
            return False
        if flt.max_amt is not None and amt_f > flt.max_amt:
            return False

    return True


def _format_event(kind: str, raw: dict[str, Any]) -> str:
    k = _normalize_kind(kind)
    if k == "new_token":
        return (
            f"NEW  symbol={_get(raw, 'symbol', '?')} "
            f"name={_get(raw, 'name', '?')} mint={_get(raw, 'mint', '?')}"
        )
    if k == "trade":
        return (
            f"TRD  mint={_get(raw, 'mint', '?')} "
            f"txType={_get(raw, 'txType', _get(raw, 'tx_type', '?'))} "
            f"uiAmount={_get(raw, 'uiAmount', _get(raw, 'amount', '?'))}"
        )
    if k == "warning":
        return f"WARN errors={_get(raw, 'errors', _get(raw, 'error', '?'))}"
    if k == "info":
        return f"INFO {_get(raw, 'msg', _get(raw, 'message', ''))}"
    return f"{k.upper():4} {_safe_json(raw)[:400]}"


class JsonlWriter:
    """
    Simple JSONL writer (sync context manager).
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self._fh: Any = None

    def __enter__(self) -> "JsonlWriter":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self.path, "a", encoding="utf-8", newline="\n")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._fh:
            with contextlib.suppress(Exception):
                self._fh.flush()
            with contextlib.suppress(Exception):
                self._fh.close()
        self._fh = None

    def write(self, obj: dict[str, Any]) -> None:
        if not self._fh:
            return
        self._fh.write(json.dumps(obj, ensure_ascii=False, default=str) + "\n")
        self._fh.flush()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Consume PumpPortal events (dev tool).")

    p.add_argument(
        "--kinds",
        default="",
        help="CSV kinds to include (new_token,trade,warning,info). Empty = all.",
    )
    p.add_argument("--mint", default="", help="Filter by exact mint.")
    p.add_argument("--symbol", default="", help="Filter by symbol (case-insensitive).")
    p.add_argument("--tx-type", default="", help="Filter trades by txType (buy/sell/etc).")
    p.add_argument("--min-amt", type=float, default=None, help="Min uiAmount filter (trade only).")
    p.add_argument("--max-amt", type=float, default=None, help="Max uiAmount filter (trade only).")

    p.add_argument("--json", action="store_true", help="Print JSON per event (normalized wrapper).")
    p.add_argument("--raw-json", action="store_true", help="Print raw JSON (raw payload only).")
    p.add_argument("--out", default="", help="Write events to JSONL file (appends).")
    p.add_argument("--quiet", action="store_true", help="Less chatter (still prints events).")

    p.add_argument(
        "--max-events", type=int, default=0, help="Stop after N matched events (0=unlimited)."
    )
    p.add_argument("--max-seconds", type=int, default=0, help="Stop after N seconds (0=unlimited).")

    p.add_argument("--retries", type=int, default=5, help="Reconnect retries on stream failure.")
    p.add_argument(
        "--retry-backoff", type=float, default=2.0, help="Base backoff seconds for retries."
    )

    return p.parse_args()


async def _iter_events(client: PumpPortalClient) -> AsyncIterator[dict[str, Any]]:
    async for ev in client.events():
        # Expect ev shape: {"kind": "...", "raw": {...}, ...}
        if isinstance(ev, dict):
            yield ev
        else:
            yield {"kind": "unknown", "raw": {"value": ev}}


async def _consume(
    *,
    flt: Filters,
    json_mode: bool,
    raw_json: bool,
    out_path: Path | None,
    quiet: bool,
    max_events: int,
    max_seconds: int,
    retries: int,
    retry_backoff: float,
) -> int:
    client = PumpPortalClient()

    if not quiet:
        masked = None
        with contextlib.suppress(Exception):
            masked = client.masked_uri()
        print(
            f"[pumpportal_consumer_demo] {_ts()} connecting to: {masked or '(masked uri unavailable)'}"
        )

    # Graceful stop flag
    stop = asyncio.Event()

    def _request_stop() -> None:
        stop.set()

    # Signal handlers (best-effort)
    loop = asyncio.get_running_loop()
    for sig_name in ("SIGINT", "SIGTERM"):
        if hasattr(signal, sig_name):
            sig = getattr(signal, sig_name)
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, _request_stop)

    writer_ctx: Any = JsonlWriter(out_path) if out_path else contextlib.nullcontext()

    matched = 0
    started = _now()
    attempt = 0

    with writer_ctx as writer:
        while True:
            if stop.is_set():
                return 0

            if max_seconds > 0 and (_now() - started) >= max_seconds:
                if not quiet:
                    print(f"[pumpportal_consumer_demo] {_ts()} max-seconds reached; exiting.")
                return 0

            if max_events > 0 and matched >= max_events:
                if not quiet:
                    print(f"[pumpportal_consumer_demo] {_ts()} max-events reached; exiting.")
                return 0

            try:
                async for ev in _iter_events(client):
                    if stop.is_set():
                        return 0

                    kind = str(ev.get("kind") or "unknown")
                    raw = ev.get("raw") or {}
                    if not isinstance(raw, dict):
                        raw = {"value": raw}

                    if not _passes_filters(kind, raw, flt):
                        continue

                    matched += 1

                    wrapper: dict[str, Any] = {
                        "ts": _ts(),
                        "kind": _normalize_kind(kind),
                        "raw": raw,
                    }

                    if out_path and hasattr(writer, "write"):
                        writer.write(wrapper)

                    if raw_json:
                        print(_safe_json(raw))
                    elif json_mode:
                        print(_safe_json(wrapper))
                    else:
                        print(_format_event(kind, raw))

                    if max_events > 0 and matched >= max_events:
                        if not quiet:
                            print(
                                f"[pumpportal_consumer_demo] {_ts()} max-events reached; exiting."
                            )
                        return 0

                    if max_seconds > 0 and (_now() - started) >= max_seconds:
                        if not quiet:
                            print(
                                f"[pumpportal_consumer_demo] {_ts()} max-seconds reached; exiting."
                            )
                        return 0

                # Stream ended naturally -> treat as disconnect
                raise RuntimeError("stream_ended")

            except asyncio.CancelledError:
                return 0
            except KeyboardInterrupt:
                return 0
            except Exception as e:
                attempt += 1
                if attempt > max(0, retries):
                    print(
                        f"[pumpportal_consumer_demo] {_ts()} ERROR: stream failed after retries: "
                        f"{type(e).__name__}: {e}",
                        file=sys.stderr,
                    )
                    return 3

                base = max(0.2, float(retry_backoff))
                sleep_s = base * min(30.0, 2.0 ** (attempt - 1))
                print(
                    f"[pumpportal_consumer_demo] {_ts()} WARN: stream error "
                    f"({type(e).__name__}: {e}) — retry {attempt}/{retries} in {sleep_s:.1f}s",
                    file=sys.stderr,
                )
                await asyncio.sleep(sleep_s)


def _make_filters(args: argparse.Namespace) -> Filters:
    kinds_raw = _split_csv(str(args.kinds or ""))
    kinds = {k.strip().lower() for k in kinds_raw if k.strip()} if kinds_raw else None

    mint = str(args.mint or "").strip() or None
    symbol = str(args.symbol or "").strip() or None
    tx_type = str(args.tx_type or "").strip() or None

    return Filters(
        kinds=kinds,
        mint=mint,
        symbol=symbol,
        tx_type=tx_type,
        min_amt=args.min_amt,
        max_amt=args.max_amt,
    )


def main() -> None:
    args = parse_args()

    # Windows asyncio stability (important for websockets)
    with contextlib.suppress(Exception):
        if os.name == "nt" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]

    flt = _make_filters(args)
    out_path = Path(args.out).expanduser().resolve() if str(args.out or "").strip() else None

    try:
        rc = asyncio.run(
            _consume(
                flt=flt,
                json_mode=bool(args.json),
                raw_json=bool(args.raw_json),
                out_path=out_path,
                quiet=bool(args.quiet),
                max_events=max(0, int(args.max_events or 0)),
                max_seconds=max(0, int(args.max_seconds or 0)),
                retries=max(0, int(args.retries or 0)),
                retry_backoff=float(args.retry_backoff or 2.0),
            )
        )
    except KeyboardInterrupt:
        rc = 0
    except Exception as e:
        print(
            f"[pumpportal_consumer_demo] {_ts()} ERROR: {type(e).__name__}: {e}",
            file=sys.stderr,
        )
        rc = 3

    raise SystemExit(rc)


if __name__ == "__main__":
    main()
