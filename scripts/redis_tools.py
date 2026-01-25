#!/usr/bin/env python3
from __future__ import annotations

"""
scripts/redis_tools.py
======================

Quant-grade Redis toolbox (P0/P1 support) — safe, explicit, and Render+local friendly.

Why this exists
---------------
- You need a **single** reliable way to inspect / debug / clear Redis flags and queues
  without relying on ad-hoc scripts.
- Must work whether utils_pkg.core_utils exposes a bound `redis` client or not.
- Must support BOTH legacy and current key layouts (we probe and show both).

What it can do (subcommands)
----------------------------
Connectivity / meta:
  - ping                     : verify connectivity + show server meta

PnL / halts (risk pipeline):
  - show-pnl                 : show realized pnl for a day (supports both key layouts)
  - show-halts               : show halt flags (and TTL)
  - clear-halts              : clear halt flags (safe, targeted)

Queues / ops:
  - queues                   : inspect exit_queue + other common queues
  - metrics                  : show metrics:* keys (winrate, dd, etc.)

Generic utilities (safe):
  - get KEY                  : read a key (auto-decode; json pretty if possible)
  - set KEY VALUE            : set a key (string or JSON), optional TTL
  - scan PATTERN             : list keys matching pattern (type + ttl + short preview)
  - del KEY [KEY ...]        : delete explicit keys (no wildcard delete unless --pattern + --yes)
  - del-pattern PATTERN      : delete keys by pattern (requires --yes)

Usage examples
--------------
  python -m scripts.redis_tools ping
  python -m scripts.redis_tools show-halts
  python -m scripts.redis_tools clear-halts
  python -m scripts.redis_tools show-pnl --date 2026-01-04
  python -m scripts.redis_tools queues
  python -m scripts.redis_tools metrics --limit 50
  python -m scripts.redis_tools scan "pnl:*" --limit 100
  python -m scripts.redis_tools get "halt:manual"
  python -m scripts.redis_tools set "halt:manual" "1" --ttl 3600
  python -m scripts.redis_tools del "halt:manual"
  python -m scripts.redis_tools del-pattern "tmp:*" --yes

Notes
-----
- Always initializes shared infra via utils_pkg.core_utils.init_resources()
  so behavior matches the running bot environment.
"""

import argparse
import asyncio
import contextlib
import json
import os
import sys
from datetime import date as _date, datetime
from pathlib import Path
from typing import Any

# ──────────────────────────────────────────────────────────────
# Repo root + dotenv
# ──────────────────────────────────────────────────────────────

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

with contextlib.suppress(Exception):
    from dotenv import find_dotenv, load_dotenv  # type: ignore

    load_dotenv(find_dotenv(), override=False)

# Shared infra (Redis usually comes from here)
from utils_pkg.core_utils import close_resources, init_resources  # type: ignore

# Optional canonical logger
try:
    from logging_setup import get_logger  # type: ignore

    log = get_logger()
except Exception:  # pragma: no cover
    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("redis_tools")

# ──────────────────────────────────────────────────────────────
# Globals (fallback redis client if core_utils doesn't provide one)
# ──────────────────────────────────────────────────────────────

_FALLBACK_REDIS: Any = None

HALT_KEYS_DEFAULT = [
    "halt:infra",
    "halt:manual",
    "halt:daily_loss",
]

# Common queue keys you likely care about (best-effort probe)
COMMON_QUEUE_KEYS = [
    ("exit_queue", "zset"),
    ("entry_queue", "zset"),
    ("buy_queue", "list"),
    ("sell_queue", "list"),
]


# PnL key layouts (support both)
# Newer (used by pnl_peek.py): pnl:day:realized_usd:YYYYMMDD
# Legacy: pnl:day:YYYYMMDD:realized_usd
def pnl_keys_for_day(ds: str) -> list[str]:
    return [
        f"pnl:day:realized_usd:{ds}",
        f"pnl:day:{ds}:realized_usd",
    ]


# ──────────────────────────────────────────────────────────────
# Small helpers
# ──────────────────────────────────────────────────────────────


def _datestr(s: str | None) -> str:
    """Accept YYYY-MM-DD or YYYYMMDD; default today (local)."""
    if not s:
        return _date.today().strftime("%Y%m%d")
    s = s.strip()
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y%m%d")
        except ValueError:
            continue
    raise SystemExit("Bad --date format. Use YYYY-MM-DD or YYYYMMDD")


def _decode(v: Any) -> Any:
    if v is None:
        return None
    # IMPORTANT: isinstance does NOT accept PEP604 unions (bytes | bytearray)
    if isinstance(v, (bytes, bytearray)):
        try:
            return v.decode("utf-8", errors="replace")
        except Exception:
            return str(v)
    return v


def _to_str(v: Any) -> str | None:
    v = _decode(v)
    if v is None:
        return None
    return str(v)


def _maybe_json(v: Any) -> tuple[Any, bool]:
    s = _to_str(v)
    if not s:
        return v, False
    s2 = s.strip()
    if not (s2.startswith("{") or s2.startswith("[")):
        return s, False
    try:
        return json.loads(s2), True
    except Exception:
        return s, False


def _env(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name)
    return v if v not in (None, "") else default


async def _close_fallback_redis() -> None:
    global _FALLBACK_REDIS
    if _FALLBACK_REDIS is None:
        return
    try:
        # redis.asyncio client supports aclose/close depending on version
        if hasattr(_FALLBACK_REDIS, "aclose"):
            await _FALLBACK_REDIS.aclose()
        else:
            await _FALLBACK_REDIS.close()
    except Exception:
        pass
    _FALLBACK_REDIS = None


async def _maybe_await(x: Any) -> Any:
    return await x if asyncio.iscoroutine(x) else x


async def _get_redis():
    """
    Return a usable redis client:
      1) Prefer utils_pkg.core_utils.redis if present/bound
      2) Else try core_utils getter helpers (get_redis_async/get_redis)
      3) Else create a fallback client from REDIS_URL
    """
    # 1) core_utils.redis (common)
    try:
        import utils_pkg.core_utils as cu  # type: ignore

        r = getattr(cu, "redis", None)
        if r is not None:
            return r
    except Exception:
        pass

    # 2) getter helpers (best-effort)
    try:
        import utils_pkg.core_utils as cu  # type: ignore

        for nm in ("get_redis_async", "get_redis"):
            fn = getattr(cu, nm, None)
            if callable(fn):
                with contextlib.suppress(Exception):
                    r = await _maybe_await(fn())
                    if r is not None:
                        return r
    except Exception:
        pass

    # 3) fallback client
    global _FALLBACK_REDIS
    if _FALLBACK_REDIS is None:
        url = _env("REDIS_URL")
        if not url:
            raise SystemExit("REDIS_URL not set; cannot connect to Redis.")
        try:
            import redis.asyncio as redis_async  # type: ignore
        except Exception as e:
            raise SystemExit(
                f"redis.asyncio not available. pip install redis>=4.2.0. Error: {e}"
            ) from e

        # decode_responses=True => strings in/out (matches most tool expectations)
        _FALLBACK_REDIS = redis_async.from_url(url, decode_responses=True)
        try:
            await _FALLBACK_REDIS.ping()
        except Exception as e:
            await _close_fallback_redis()
            raise SystemExit(f"Failed to connect to Redis at {url}: {e}") from e

    return _FALLBACK_REDIS


# ──────────────────────────────────────────────────────────────
# Commands
# ──────────────────────────────────────────────────────────────


async def cmd_ping(_: argparse.Namespace) -> None:
    r = await _get_redis()
    ok = await r.ping()
    print(f"ping: {ok}")

    # best-effort meta
    try:
        info = await r.info()
        ver = info.get("redis_version")
        mode = info.get("redis_mode") or info.get("role")
        mem = info.get("used_memory_human")
        print(f"redis_version: {ver} | mode/role: {mode} | used_memory: {mem}")
    except Exception as e:
        print(f"(info unavailable: {e})")


async def cmd_show_pnl(args: argparse.Namespace) -> None:
    r = await _get_redis()
    ds = _datestr(args.date)
    keys = pnl_keys_for_day(ds)

    found_any = False
    for k in keys:
        v = await r.get(k)
        v = _decode(v)
        if v is not None:
            print(f"{k} = {v}")
            found_any = True

    if not found_any:
        print(f"(no pnl key found for {ds}) tried: {', '.join(keys)}")


async def cmd_show_halts(args: argparse.Namespace) -> None:
    r = await _get_redis()
    keys = args.keys or HALT_KEYS_DEFAULT

    out: dict[str, Any] = {}
    for k in keys:
        v = await r.get(k)
        if v is None:
            continue
        ttl = None
        if not args.no_ttl:
            try:
                ttl = await r.ttl(k)
            except Exception:
                ttl = None
        out[k] = {"value": _decode(v), "ttl": ttl}

    if args.json:
        print(json.dumps(out, indent=2, default=str))
    else:
        if not out:
            print("Halts: (none)")
            return
        print("Halts:")
        for k, d in out.items():
            ttl_s = "" if args.no_ttl else f" (ttl={d.get('ttl')})"
            print(f"  - {k} = {d.get('value')}{ttl_s}")


async def cmd_clear_halts(args: argparse.Namespace) -> None:
    r = await _get_redis()
    keys = args.keys or HALT_KEYS_DEFAULT

    deleted = 0
    for k in keys:
        with contextlib.suppress(Exception):
            deleted += int(await r.delete(k))
    print(f"Cleared {deleted} halt key(s): {', '.join(keys)}")


async def cmd_get(args: argparse.Namespace) -> None:
    r = await _get_redis()
    v = await r.get(args.key)
    v = _decode(v)
    if v is None:
        print("(nil)")
        return

    parsed, is_json = _maybe_json(v)
    if args.json and is_json:
        print(json.dumps(parsed, indent=2, default=str))
    else:
        print(v)


async def cmd_set(args: argparse.Namespace) -> None:
    r = await _get_redis()
    val: str = args.value

    # If user passed --as-json, validate json
    if args.as_json:
        try:
            obj = json.loads(val)
        except Exception as e:
            raise SystemExit(f"--as-json expects valid JSON. error: {e}") from e
        val = json.dumps(obj, separators=(",", ":"), ensure_ascii=False)

    if args.ttl and args.ttl > 0:
        ok = await r.set(args.key, val, ex=int(args.ttl))
    else:
        ok = await r.set(args.key, val)
    print(f"set: {ok}")


async def cmd_scan(args: argparse.Namespace) -> None:
    r = await _get_redis()
    pattern = args.pattern
    limit = max(1, min(50_000, int(args.limit)))

    keys: list[str] = []
    cursor = 0
    while True:
        cursor, batch = await r.scan(cursor=cursor, match=pattern, count=min(1000, limit))
        if batch:
            keys.extend([_to_str(k) or "" for k in batch])
            keys = [k for k in keys if k]
        if len(keys) >= limit or cursor == 0:
            break

    keys = keys[:limit]

    if not keys:
        print("(no keys)")
        return

    rows: list[dict[str, Any]] = []
    for k in keys:
        row: dict[str, Any] = {"key": k}
        if not args.no_type:
            try:
                row["type"] = await r.type(k)
            except Exception:
                row["type"] = None
        if not args.no_ttl:
            try:
                row["ttl"] = await r.ttl(k)
            except Exception:
                row["ttl"] = None
        if args.preview:
            try:
                if (row.get("type") or "") in {"string", None}:
                    v = await r.get(k)
                    v = _to_str(v)
                    if v is not None and len(v) > 140:
                        v = v[:140] + "…"
                    row["preview"] = v
                else:
                    row["preview"] = None
            except Exception:
                row["preview"] = None
        rows.append(row)

    if args.json:
        print(json.dumps(rows, indent=2, default=str))
    else:
        for row in rows:
            parts = [row["key"]]
            if "type" in row:
                parts.append(f"type={row.get('type')}")
            if "ttl" in row:
                parts.append(f"ttl={row.get('ttl')}")
            if args.preview:
                parts.append(f"preview={row.get('preview')}")
            print(" | ".join(parts))


async def cmd_del(args: argparse.Namespace) -> None:
    r = await _get_redis()
    keys = list(args.keys or [])
    if not keys:
        raise SystemExit("del requires at least one KEY.")
    deleted = 0
    for k in keys:
        with contextlib.suppress(Exception):
            deleted += int(await r.delete(k))
    print(f"deleted={deleted}")


async def cmd_del_pattern(args: argparse.Namespace) -> None:
    if not args.yes:
        raise SystemExit("Refusing to delete by pattern without --yes (safety).")
    r = await _get_redis()

    pattern = args.pattern
    limit = max(1, min(200_000, int(args.limit)))

    keys: list[str] = []
    cursor = 0
    while True:
        cursor, batch = await r.scan(cursor=cursor, match=pattern, count=min(2000, limit))
        if batch:
            keys.extend([_to_str(k) or "" for k in batch])
            keys = [k for k in keys if k]
        if len(keys) >= limit or cursor == 0:
            break

    keys = keys[:limit]
    if not keys:
        print("(no keys matched)")
        return

    deleted = 0
    chunk = 500
    for i in range(0, len(keys), chunk):
        part = keys[i : i + chunk]
        try:
            deleted += int(await r.delete(*part))
        except Exception:
            for k in part:
                with contextlib.suppress(Exception):
                    deleted += int(await r.delete(k))

    print(f"matched={len(keys)} deleted={deleted} pattern={pattern} (limit={limit})")


async def cmd_queues(args: argparse.Namespace) -> None:
    r = await _get_redis()

    names = args.names or [n for n, _t in COMMON_QUEUE_KEYS]
    limit = max(1, min(200, int(args.limit)))

    out: dict[str, Any] = {}
    for name in names:
        try:
            t = await r.type(name)
        except Exception:
            t = None

        entry: dict[str, Any] = {"type": t}
        try:
            if t == "zset":
                entry["zcard"] = await r.zcard(name)
                entry["head"] = await r.zrange(name, 0, min(limit - 1, 20), withscores=True)
            elif t == "list":
                entry["llen"] = await r.llen(name)
                entry["head"] = await r.lrange(name, 0, min(limit - 1, 20))
            elif t == "set":
                entry["scard"] = await r.scard(name)
                cursor, items = await r.sscan(name, cursor=0, count=min(50, limit))
                entry["head"] = items
                entry["cursor"] = cursor
            elif t == "hash":
                entry["hlen"] = await r.hlen(name)
                cursor, items = await r.hscan(name, cursor=0, count=min(50, limit))
                entry["head"] = items
                entry["cursor"] = cursor
            else:
                entry["note"] = "unsupported type for detailed peek"
        except Exception as e:
            entry["error"] = str(e)

        out[name] = entry

    if args.json:
        print(json.dumps(out, indent=2, default=str))
    else:
        for k, v in out.items():
            print(f"== {k} ==")
            for kk, vv in v.items():
                print(f"  {kk}: {vv}")
            print("")


async def cmd_metrics(args: argparse.Namespace) -> None:
    r = await _get_redis()
    limit = max(1, min(5000, int(args.limit)))
    pattern = args.pattern or "metrics:*"

    keys: list[str] = []
    cursor = 0
    while True:
        cursor, batch = await r.scan(cursor=cursor, match=pattern, count=min(1000, limit))
        if batch:
            keys.extend([_to_str(k) or "" for k in batch])
            keys = [k for k in keys if k]
        if len(keys) >= limit or cursor == 0:
            break
    keys = sorted(keys[:limit])

    if not keys:
        print("(no metrics keys)")
        return

    try:
        vals = await r.mget(*keys)
    except Exception:
        vals = [await r.get(k) for k in keys]

    out: dict[str, Any] = {}
    for k, v in zip(keys, vals, strict=False):
        out[k] = _decode(v)

    if args.json:
        print(json.dumps(out, indent=2, default=str))
    else:
        for k in keys:
            print(f"{k} = {out.get(k)}")


# ──────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Redis helper toolbox (PnL, halts, queues, metrics, and safe utilities)."
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    # ping
    sub.add_parser("ping", help="Ping Redis and show basic server info")

    # pnl/halts
    sp = sub.add_parser(
        "show-pnl", help="Show realized PnL for a day (supports multiple key layouts)"
    )
    sp.add_argument("--date", help="YYYY-MM-DD or YYYYMMDD (default: today)")

    sp = sub.add_parser("show-halts", help="Show active halt flags (and ttl)")
    sp.add_argument(
        "--keys", nargs="*", default=None, help="Override halt keys (default: standard halt:* keys)"
    )
    sp.add_argument("--no-ttl", action="store_true", help="Skip ttl")
    sp.add_argument("--json", action="store_true", help="JSON output")

    sp = sub.add_parser("clear-halts", help="Clear halt flags (safe, targeted)")
    sp.add_argument(
        "--keys", nargs="*", default=None, help="Override halt keys (default: standard halt:* keys)"
    )

    # queues/metrics
    sp = sub.add_parser("queues", help="Inspect common queues (exit_queue, etc.)")
    sp.add_argument(
        "--names", nargs="*", default=None, help="Queue keys to inspect (default: common list)"
    )
    sp.add_argument("--limit", type=int, default=100, help="Limit for peeks (default 100)")
    sp.add_argument("--json", action="store_true", help="JSON output")

    sp = sub.add_parser("metrics", help="Show metrics:* keys")
    sp.add_argument("--pattern", default=None, help="Pattern (default: metrics:*)")
    sp.add_argument("--limit", type=int, default=200, help="Max keys (default 200)")
    sp.add_argument("--json", action="store_true", help="JSON output")

    # generic
    sp = sub.add_parser("get", help="Get a key value")
    sp.add_argument("key", help="Redis key")
    sp.add_argument("--json", action="store_true", help="Pretty-print if value is JSON")

    sp = sub.add_parser("set", help="Set a key value")
    sp.add_argument("key", help="Redis key")
    sp.add_argument("value", help="Value (string). Use --as-json to validate/compact JSON.")
    sp.add_argument("--ttl", type=int, default=0, help="TTL seconds (0 = no TTL)")
    sp.add_argument("--as-json", action="store_true", help="Validate/compact JSON before setting")

    sp = sub.add_parser("scan", help="SCAN keys by pattern (non-blocking)")
    sp.add_argument("pattern", help='Pattern, e.g. "pnl:*" or "halt:*"')
    sp.add_argument("--limit", type=int, default=200, help="Max keys (default 200)")
    sp.add_argument("--preview", action="store_true", help="Show short value preview for strings")
    sp.add_argument("--no-ttl", action="store_true", help="Skip TTL lookup")
    sp.add_argument("--no-type", action="store_true", help="Skip TYPE lookup")
    sp.add_argument("--json", action="store_true", help="JSON output")

    sp = sub.add_parser("del", help="Delete explicit keys (safe)")
    sp.add_argument("keys", nargs="+", help="Keys to delete")

    sp = sub.add_parser("del-pattern", help="Delete keys matching pattern (requires --yes)")
    sp.add_argument("pattern", help='Pattern, e.g. "tmp:*"')
    sp.add_argument("--limit", type=int, default=50000, help="Max keys to delete (default 50k)")
    sp.add_argument("--yes", action="store_true", help="Required safety flag")

    return p


async def _dispatch(args: argparse.Namespace) -> None:
    if args.cmd == "ping":
        await cmd_ping(args)
    elif args.cmd == "show-pnl":
        await cmd_show_pnl(args)
    elif args.cmd == "show-halts":
        await cmd_show_halts(args)
    elif args.cmd == "clear-halts":
        await cmd_clear_halts(args)
    elif args.cmd == "queues":
        await cmd_queues(args)
    elif args.cmd == "metrics":
        await cmd_metrics(args)
    elif args.cmd == "get":
        await cmd_get(args)
    elif args.cmd == "set":
        await cmd_set(args)
    elif args.cmd == "scan":
        await cmd_scan(args)
    elif args.cmd == "del":
        await cmd_del(args)
    elif args.cmd == "del-pattern":
        await cmd_del_pattern(args)
    else:
        raise SystemExit(f"Unknown cmd: {args.cmd}")


async def main_async() -> None:
    args = build_parser().parse_args()

    await init_resources()
    try:
        await _dispatch(args)
    finally:
        await close_resources()
        await _close_fallback_redis()


def main() -> None:
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        print("\n⏹ stopped")


if __name__ == "__main__":
    main()
