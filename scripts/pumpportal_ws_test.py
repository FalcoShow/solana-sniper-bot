#!/usr/bin/env python3
from __future__ import annotations

"""
scripts/pumpportal_ws_test.py

PumpPortal WS connectivity + subscription smoke test (dev tool)
==============================================================

What it does
------------
- Opens a websocket to PumpPortal (pumpportal.fun)
- Subscribes to:
    • FREE:  subscribeNewToken
    • PAID:  subscribeTokenTrade (optional mint list)
    • PAID:  subscribeAccountTrade (optional account)
- Prints events (pretty) and optionally:
    • emits JSON lines
    • writes JSONL to a file
    • filters by mint / event type
- Handles reconnects + graceful shutdown

Why this exists (P0 hygiene)
----------------------------
This is a **tool** for validating your stream credentials and event shapes
without touching the live trading path.

Env (supports new + legacy)
--------------------------
Required:
- PF_PP_KEY            (legacy)
  or PUMPPORTAL_API_KEY / PUMPPORTAL_KEY

Optional:
- PF_PP_MINTS          CSV mints for subscribeTokenTrade
  or PUMPPORTAL_MINTS
- PF_PP_ACCOUNT        account for subscribeAccountTrade
  or PUMPPORTAL_ACCOUNT
- PF_PP_URI            override ws base URI (rare)
  or PUMPPORTAL_WS_URL / PUMPPORTAL_URI

Usage
-----
python -m scripts.pumpportal_ws_test
python -m scripts.pumpportal_ws_test --tokens So111...,EPjF... --max-events 50
python -m scripts.pumpportal_ws_test --account <pubkey> --kinds trade --json
python -m scripts.pumpportal_ws_test --mint So111... --out ./pp_events.jsonl --max-seconds 60

Exit codes
----------
0 clean exit
2 config error (missing key / deps)
3 stream error (after retries)
"""

import argparse
import asyncio
import contextlib
import json
import os
import re
import signal
import sys
import time
from collections.abc import Iterable
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# dotenv + repo root
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

with contextlib.suppress(Exception):
    from dotenv import find_dotenv, load_dotenv  # type: ignore

    load_dotenv(find_dotenv(), override=False)

# ---------------------------------------------------------------------------
# deps
# ---------------------------------------------------------------------------

try:
    import websockets  # type: ignore
except Exception as e:
    print(
        f"[pumpportal_ws_test] ERROR: missing dependency websockets: {e}",
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


def _env_first(keys: Iterable[str], default: str = "") -> str:
    for k in keys:
        v = os.getenv(k)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return default


def _env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return default


def _mask_secret(s: str) -> str:
    s = (s or "").strip()
    if len(s) <= 10:
        return "<short>"
    return s[:4] + "..." + s[-4:]


def _mask_uri(uri: str) -> str:
    """
    Mask api-key query param if present:
      ...?api-key=REALKEY -> ...?api-key=ABCD...WXYZ
    """
    return re.sub(
        r"(api-key=)([^&]+)",
        lambda m: m.group(1) + _mask_secret(m.group(2)),
        uri,
    )


def _json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str)


def _pretty(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2, default=str)


def _env_key() -> str:
    return _env_first(["PF_PP_KEY", "PUMPPORTAL_API_KEY", "PUMPPORTAL_KEY"], "")


def _env_tokens() -> list[str]:
    blob = _env_first(["PF_PP_MINTS", "PUMPPORTAL_MINTS"], "")
    return _split_csv(blob)


def _env_account() -> str:
    return _env_first(["PF_PP_ACCOUNT", "PUMPPORTAL_ACCOUNT"], "")


def _base_uri() -> str:
    # Default PumpPortal endpoint
    # (Override only if you know what you're doing.)
    return _env_first(
        ["PF_PP_URI", "PUMPPORTAL_WS_URL", "PUMPPORTAL_URI"],
        "wss://pumpportal.fun/api/data",
    )


def _build_uri(api_key: str) -> str:
    base = _base_uri().rstrip("/")
    # If user already included query params, preserve them
    if "?" in base:
        # Ensure api-key exists exactly once
        if "api-key=" in base:
            return base
        return base + f"&api-key={api_key}"
    return base + f"?api-key={api_key}"


def _kind(msg: dict[str, Any]) -> str:
    # PumpPortal payloads vary; infer a coarse kind
    if not isinstance(msg, dict):
        return "unknown"
    if "errors" in msg:
        return "warning"
    # Common keys seen:
    # - "type" or "event" or "txType"
    if "txType" in msg or "tx_type" in msg:
        return "trade"
    if msg.get("method") or msg.get("result"):
        return "info"
    # New token payloads often contain mint/symbol/name without txType
    if "mint" in msg and ("symbol" in msg or "name" in msg) and "txType" not in msg:
        return "new_token"
    return "unknown"


def _mint(msg: dict[str, Any]) -> str:
    if not isinstance(msg, dict):
        return ""
    v = msg.get("mint") or msg.get("tokenMint") or msg.get("token_mint") or ""
    return str(v) if v is not None else ""


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="PumpPortal WS smoke test.")
    p.add_argument(
        "--tokens",
        default="",
        help="CSV token mints for subscribeTokenTrade (overrides env).",
    )
    p.add_argument(
        "--account",
        default="",
        help="Account for subscribeAccountTrade (overrides env).",
    )
    p.add_argument(
        "--free-only",
        action="store_true",
        help="Only subscribeNewToken (ignore paid feeds).",
    )

    p.add_argument(
        "--kinds",
        default="",
        help="CSV kinds filter: new_token,trade,warning,info,unknown.",
    )
    p.add_argument("--mint", default="", help="Filter by exact mint (post-receive).")

    p.add_argument(
        "--json",
        action="store_true",
        help="Print compact JSON per event (default is single-line text).",
    )
    p.add_argument("--pretty", action="store_true", help="Pretty-print JSON per event.")
    p.add_argument("--out", default="", help="Write matching events to JSONL file (append).")

    p.add_argument(
        "--max-events",
        type=int,
        default=0,
        help="Stop after N matching events (0=unlimited).",
    )
    p.add_argument(
        "--max-seconds",
        type=int,
        default=0,
        help="Stop after N seconds (0=unlimited).",
    )

    p.add_argument(
        "--ping-interval",
        type=int,
        default=20,
        help="Websocket ping interval seconds.",
    )
    p.add_argument(
        "--ping-timeout",
        type=int,
        default=30,
        help="Websocket ping timeout seconds.",
    )
    p.add_argument(
        "--close-timeout",
        type=int,
        default=5,
        help="Websocket close timeout seconds.",
    )
    p.add_argument("--max-size-mb", type=int, default=32, help="Max message size in MB.")
    p.add_argument("--retries", type=int, default=5, help="Reconnect retries on failure.")
    p.add_argument(
        "--retry-backoff",
        type=float,
        default=2.0,
        help="Base backoff seconds for reconnect.",
    )
    p.add_argument("--quiet", action="store_true", help="Less chatter.")
    return p.parse_args()


class JsonlWriter:
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
        self._fh.write(_json(obj) + "\n")
        self._fh.flush()


async def _send(ws, obj: dict[str, Any]) -> None:
    await ws.send(_json(obj))


def _format_line(msg: dict[str, Any]) -> str:
    k = _kind(msg)
    if k == "new_token":
        return (
            f"NEW  symbol={msg.get('symbol', '?')} name={msg.get('name', '?')} "
            f"mint={msg.get('mint', '?')}"
        )
    if k == "trade":
        return (
            f"TRD  mint={_mint(msg) or '?'} "
            f"txType={msg.get('txType', msg.get('tx_type', '?'))} "
            f"uiAmount={msg.get('uiAmount', msg.get('amount', '?'))}"
        )
    if k == "warning":
        return f"WARN {msg.get('errors', msg.get('error', '?'))}"
    if k == "info":
        return f"INFO {msg.get('method', msg.get('result', ''))}"
    return f"EVT  {_json(msg)[:300]}"


def _passes(msg: dict[str, Any], kinds: set[str] | None, mint: str | None) -> bool:
    k = _kind(msg)
    if kinds is not None and k not in kinds:
        return False
    if mint:
        return _mint(msg) == mint
    return True


async def consume_once(
    *,
    uri: str,
    tokens: list[str],
    account: str,
    free_only: bool,
    kinds: set[str] | None,
    mint_filter: str | None,
    json_mode: bool,
    pretty: bool,
    writer: JsonlWriter | None,
    max_events: int,
    max_seconds: int,
    ping_interval: int,
    ping_timeout: int,
    close_timeout: int,
    max_size: int,
    quiet: bool,
    stop_event: asyncio.Event,
) -> int:
    started = _now()
    matched = 0

    if not quiet:
        print(f"[pumpportal_ws_test] {_ts()} connecting: {_mask_uri(uri)}")
        if tokens:
            print(f"[pumpportal_ws_test] tokens: {tokens}")
        if account:
            print(f"[pumpportal_ws_test] account: {account}")
        if free_only:
            print("[pumpportal_ws_test] free-only mode ON")

    async with websockets.connect(
        uri,
        ping_interval=ping_interval,
        ping_timeout=ping_timeout,
        close_timeout=close_timeout,
        max_size=max_size,
    ) as ws:
        # Always subscribe to free new token feed
        await _send(ws, {"method": "subscribeNewToken"})
        if not quiet:
            print("[pumpportal_ws_test] subscribed: subscribeNewToken")

        # Paid feeds
        if not free_only:
            if tokens:
                await _send(ws, {"method": "subscribeTokenTrade", "keys": tokens})
                if not quiet:
                    print("[pumpportal_ws_test] subscribed: subscribeTokenTrade")
            if account:
                await _send(ws, {"method": "subscribeAccountTrade", "keys": [account]})
                if not quiet:
                    print("[pumpportal_ws_test] subscribed: subscribeAccountTrade")

        # Read loop
        while True:
            if stop_event.is_set():
                return 0

            if max_seconds > 0 and (_now() - started) >= max_seconds:
                if not quiet:
                    print(f"[pumpportal_ws_test] {_ts()} max-seconds reached; exiting.")
                return 0

            raw = await ws.recv()
            try:
                msg = json.loads(raw)
            except Exception:
                msg = {"kind": "unknown", "raw": str(raw)[:2000]}

            # Filter the known banner (don’t spam logs)
            if (
                isinstance(msg, dict)
                and msg.get("errors") == "Invalid API key. PumpSwap data will not be streamed."
            ):
                continue

            if not isinstance(msg, dict):
                msg = {"kind": "unknown", "raw": msg}

            if not _passes(msg, kinds, mint_filter):
                continue

            matched += 1
            wrapper = {"ts": _ts(), "kind": _kind(msg), "mint": _mint(msg) or None, "msg": msg}

            if writer is not None:
                writer.write(wrapper)

            if pretty:
                print(_pretty(wrapper if json_mode else msg))
            elif json_mode:
                print(_json(wrapper))
            else:
                print(_format_line(msg))

            if max_events > 0 and matched >= max_events:
                if not quiet:
                    print(f"[pumpportal_ws_test] {_ts()} max-events reached; exiting.")
                return 0


async def run() -> int:
    args = parse_args()

    # Windows websocket stability
    with contextlib.suppress(Exception):
        if os.name == "nt" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]

    api_key = _env_key()
    if not api_key:
        print(
            "[pumpportal_ws_test] ERROR: missing API key. Set PF_PP_KEY or PUMPPORTAL_API_KEY.",
            file=sys.stderr,
        )
        return 2

    uri = _build_uri(api_key)

    tokens = _split_csv(args.tokens) if str(args.tokens).strip() else _env_tokens()
    account = str(args.account).strip() if str(args.account).strip() else _env_account()
    free_only = bool(args.free_only)

    kinds_raw = _split_csv(args.kinds)
    kinds: set[str] | None = {k.strip().lower() for k in kinds_raw} if kinds_raw else None
    mint_filter = str(args.mint).strip() or None

    json_mode = bool(args.json) or bool(args.pretty)
    pretty = bool(args.pretty)

    out_path = Path(args.out).expanduser().resolve() if str(args.out).strip() else None

    stop_event = asyncio.Event()

    def _stop() -> None:
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig_name in ("SIGINT", "SIGTERM"):
        if hasattr(signal, sig_name):
            sig = getattr(signal, sig_name)
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, _stop)

    writer_ctx = JsonlWriter(out_path) if out_path else contextlib.nullcontext()

    max_size = max(1, int(args.max_size_mb)) * 1024 * 1024

    with writer_ctx as writer:  # type: ignore
        attempt = 0
        while True:
            try:
                return await consume_once(
                    uri=uri,
                    tokens=tokens,
                    account=account,
                    free_only=free_only,
                    kinds=kinds,
                    mint_filter=mint_filter,
                    json_mode=json_mode,
                    pretty=pretty,
                    writer=writer if out_path else None,
                    max_events=max(0, int(args.max_events or 0)),
                    max_seconds=max(0, int(args.max_seconds or 0)),
                    ping_interval=max(0, int(args.ping_interval)),
                    ping_timeout=max(1, int(args.ping_timeout)),
                    close_timeout=max(1, int(args.close_timeout)),
                    max_size=max_size,
                    quiet=bool(args.quiet),
                    stop_event=stop_event,
                )
            except asyncio.CancelledError:
                return 0
            except KeyboardInterrupt:
                return 0
            except Exception as e:
                attempt += 1
                if attempt > max(0, int(args.retries)):
                    print(
                        f"[pumpportal_ws_test] {_ts()} ERROR: stream failed after retries: "
                        f"{type(e).__name__}: {e}",
                        file=sys.stderr,
                    )
                    return 3

                backoff = max(0.2, float(args.retry_backoff)) * min(30.0, 2.0 ** (attempt - 1))
                print(
                    f"[pumpportal_ws_test] {_ts()} WARN: stream error "
                    f"({type(e).__name__}: {e}) — retry {attempt}/{args.retries} in {backoff:.1f}s",
                    file=sys.stderr,
                )
                await asyncio.sleep(backoff)


def main() -> None:
    try:
        rc = asyncio.run(run())
    except KeyboardInterrupt:
        rc = 0
    except Exception as e:
        print(f"[pumpportal_ws_test] {_ts()} ERROR: {type(e).__name__}: {e}", file=sys.stderr)
        rc = 3
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
