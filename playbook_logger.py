# playbook_logger.py
from __future__ import annotations

"""
playbook_logger.py — Runbook-grade events for “why not trading”
===============================================================

This module is the canonical place to record *operational truth*:
- What levers changed (who/why/old/new)
- Why we are NOT trading right now (gates, infra, budget, scanner drought)
- Key lifecycle events (startup/shutdown, health transitions)

Storage targets (best-effort, never crash the bot):
1) Redis stream     : playbook:events  (primary, fast, queryable)
2) Redis status keys: playbook:status:<name> (for dashboards/health)
3) Notion database  : NOTION_DB_PLAYBOOK (optional, human timeline)

Design goals
------------
- Safe: never throws in production paths
- Redacts secrets from fields/logs
- Works with either utils_pkg.core_utils (preferred) or direct REDIS_URL
- Supports both async usage (inside bot) and CLI usage (ops)
"""

import argparse
import asyncio
import contextlib
import json
import logging
import os
import socket
import time
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

log = logging.getLogger("playbook")


# -----------------------------------------------------------------------------
# Env / constants
# -----------------------------------------------------------------------------

REDIS_URL = (
    os.getenv("REDIS_URL") or os.getenv("REDIS_TLS_URL") or "redis://localhost:6379"
).strip()

NOTION_TOKEN = (os.getenv("NOTION_TOKEN") or os.getenv("NOTION_API_KEY") or "").strip()
NOTION_DB_PLAYBOOK = (
    os.getenv("NOTION_DB_PLAYBOOK")
    or os.getenv("NOTION_DATABASE_PLAYBOOK")
    or os.getenv("NOTION_PLAYBOOK_DB")
    or ""
).strip()

# Stream config
PLAYBOOK_STREAM_KEY = os.getenv("PLAYBOOK_STREAM_KEY", "playbook:events").strip()
PLAYBOOK_STREAM_MAXLEN = int(os.getenv("PLAYBOOK_STREAM_MAXLEN", "5000"))

# Status config
STATUS_KEY_PREFIX = os.getenv("PLAYBOOK_STATUS_PREFIX", "playbook:status:").strip()
STATUS_DEFAULT_TTL_SEC = int(os.getenv("PLAYBOOK_STATUS_TTL_SEC", "900"))

# Notion config
NOTION_ENABLED = bool(
    NOTION_TOKEN and NOTION_DB_PLAYBOOK and os.getenv("PLAYBOOK_NOTION_ENABLED", "1").strip() != "0"
)
NOTION_VERSION = os.getenv("NOTION_VERSION", "2022-06-28").strip()
NOTION_TIMEOUT_SEC = float(os.getenv("PLAYBOOK_NOTION_TIMEOUT_SEC", "10"))
NOTION_DEDUPE_TTL_SEC = int(os.getenv("PLAYBOOK_NOTION_DEDUPE_TTL_SEC", "86400"))

# Safety
MAX_FIELD_STR = int(os.getenv("PLAYBOOK_MAX_FIELD_STR", "400"))
MAX_MSG_STR = int(os.getenv("PLAYBOOK_MAX_MSG_STR", "600"))


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


def _now_ts() -> float:
    return time.time()


def _utc_iso(ts: float | None = None) -> str:
    ts = float(ts if ts is not None else _now_ts())
    return datetime.fromtimestamp(ts, tz=UTC).isoformat()


def _truthy(v: str | None) -> bool:
    if v is None:
        return False
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


def _safe_str(x: Any, max_len: int = MAX_FIELD_STR) -> str:
    try:
        s = str(x)
    except Exception:
        s = repr(x)
    s = s.replace("\n", " ").replace("\r", " ").strip()
    if len(s) > max_len:
        s = s[: max_len - 1] + "…"
    return s


_SECRET_KEY_FRAGS = ("key", "token", "secret", "private", "pass", "pwd", "authorization", "bearer")


def _redact_value(v: Any) -> str:
    s = _safe_str(v, max_len=MAX_FIELD_STR)
    if not s:
        return s
    # Keep only ends for debugging
    if len(s) <= 10:
        return "***"
    return f"{s[:3]}…{s[-3:]}"


def _sanitize_fields(fields: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in (fields or {}).items():
        ks = _safe_str(k, 80)
        low = ks.lower()
        if any(frag in low for frag in _SECRET_KEY_FRAGS):
            out[ks] = _redact_value(v)
            continue
        # JSON-safe coercions
        if isinstance(v, str | int | float | bool) or v is None:
            out[ks] = v if not isinstance(v, str) else _safe_str(v, MAX_FIELD_STR)
        else:
            # serialize complex values safely
            try:
                out[ks] = json.loads(json.dumps(v, default=str))
            except Exception:
                out[ks] = _safe_str(v, MAX_FIELD_STR)
    return out


def _host_meta() -> dict[str, Any]:
    return {
        "host": socket.gethostname(),
        "pid": os.getpid(),
        "service": os.getenv("SERVICE", "sniper"),
        "env": os.getenv("ENV", ""),
        "version": os.getenv("VERSION", "") or os.getenv("GIT_SHA", "") or "",
    }


def _make_event_id() -> str:
    return uuid.uuid4().hex


def _level_norm(level: str) -> str:
    s = (level or "").strip().upper()
    if s in {"DEBUG", "INFO", "WARN", "WARNING", "ERROR", "CRITICAL"}:
        return "WARNING" if s == "WARN" else s
    return "INFO"


# -----------------------------------------------------------------------------
# Redis access (prefer core_utils; fallback to aioredis)
# -----------------------------------------------------------------------------

_aioredis = None
try:
    import redis.asyncio as _aioredis  # type: ignore
except Exception:
    _aioredis = None


@dataclass
class _RedisHandle:
    client: Any
    owned: bool  # if we created it, we should close it


_REDIS_SINGLETON: _RedisHandle | None = None
_REDIS_LOCK = asyncio.Lock()


async def _get_redis() -> _RedisHandle:
    """
    Returns a redis handle (client + owned flag). Never raises.
    """
    global _REDIS_SINGLETON

    async with _REDIS_LOCK:
        if _REDIS_SINGLETON is not None:
            return _REDIS_SINGLETON

        # Prefer utils_pkg.core_utils
        try:
            from utils_pkg import core_utils as cu  # type: ignore

            fn = getattr(cu, "get_redis_async", None) or getattr(cu, "get_redis", None)
            if callable(fn):
                r = fn()
                if asyncio.iscoroutine(r):
                    r = await r
                if r is not None:
                    _REDIS_SINGLETON = _RedisHandle(client=r, owned=False)
                    return _REDIS_SINGLETON
        except Exception:
            pass

        # Fallback to aioredis
        if _aioredis is None:
            # Dummy no-op client
            class _Dummy:
                async def xadd(self, *_a, **_k):
                    return None

                async def set(self, *_a, **_k):
                    return None

                async def setex(self, *_a, **_k):
                    return None

                async def expire(self, *_a, **_k):
                    return None

                async def ping(self):
                    return False

                async def close(self):
                    return None

            _REDIS_SINGLETON = _RedisHandle(client=_Dummy(), owned=True)
            return _REDIS_SINGLETON

        try:
            r = _aioredis.from_url(REDIS_URL, decode_responses=True)
            _REDIS_SINGLETON = _RedisHandle(client=r, owned=True)
            return _REDIS_SINGLETON
        except Exception:
            # Dummy if creation fails
            class _Dummy:
                async def xadd(self, *_a, **_k):
                    return None

                async def set(self, *_a, **_k):
                    return None

                async def setex(self, *_a, **_k):
                    return None

                async def expire(self, *_a, **_k):
                    return None

                async def ping(self):
                    return False

                async def close(self):
                    return None

            _REDIS_SINGLETON = _RedisHandle(client=_Dummy(), owned=True)
            return _REDIS_SINGLETON


async def close() -> None:
    """
    Close redis if we own the connection.
    """
    global _REDIS_SINGLETON
    async with _REDIS_LOCK:
        h = _REDIS_SINGLETON
        _REDIS_SINGLETON = None
    if h and h.owned:
        with contextlib.suppress(Exception):
            await h.client.close()


# -----------------------------------------------------------------------------
# Notion (best-effort)
# -----------------------------------------------------------------------------


async def _notion_create_page(title: str, props: dict[str, Any]) -> None:
    """
    Create a page in Notion database. Best-effort.
    """
    if not NOTION_ENABLED:
        return

    # Prefer utils_pkg.notion_client if present
    try:
        from utils_pkg import notion_client as nc  # type: ignore

        fn = getattr(nc, "create_page", None) or getattr(nc, "create_playbook_entry", None)
        if callable(fn):
            out = fn(database_id=NOTION_DB_PLAYBOOK, title=title, properties=props)  # type: ignore
            if asyncio.iscoroutine(out):
                await out
            return
    except Exception:
        pass

    # Fallback raw HTTP
    try:
        import aiohttp

        headers = {
            "Authorization": f"Bearer {NOTION_TOKEN}",
            "Notion-Version": NOTION_VERSION,
            "Content-Type": "application/json",
        }

        body = {"parent": {"database_id": NOTION_DB_PLAYBOOK}, "properties": props}

        timeout = aiohttp.ClientTimeout(total=NOTION_TIMEOUT_SEC)

        # Prefer utils_pkg.proxy_client session if available (proxy/timeouts unified)
        try:
            from utils_pkg.proxy_client import make_session  # type: ignore

            async with make_session(timeout=NOTION_TIMEOUT_SEC) as s:  # type: ignore
                async with s.post(
                    "https://api.notion.com/v1/pages", headers=headers, json=body
                ) as r:
                    if r.status not in (200, 201):
                        txt = await r.text()
                        log.warning("[notion] create failed: %s %s", r.status, (txt or "")[:200])
            return
        except Exception:
            pass

        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.post("https://api.notion.com/v1/pages", headers=headers, json=body) as r:
                if r.status not in (200, 201):
                    txt = await r.text()
                    log.warning("[notion] create failed: %s %s", r.status, (txt or "")[:200])
    except Exception as e:
        log.debug("[notion] error: %s", e)


def _notion_rich_text(s: str) -> dict[str, Any]:
    return {"rich_text": [{"type": "text", "text": {"content": _safe_str(s, 1800)}}]}


def _notion_title(s: str) -> dict[str, Any]:
    return {"title": [{"type": "text", "text": {"content": _safe_str(s, 200)}}]}


def _notion_date_iso(ts: float) -> dict[str, Any]:
    return {"date": {"start": datetime.fromtimestamp(ts, tz=UTC).isoformat()}}


# -----------------------------------------------------------------------------
# Public API: log_event / set_status / lever changes / why-not-trading
# -----------------------------------------------------------------------------


async def log_event(
    *,
    kind: str,
    msg: str,
    level: str = "INFO",
    name: str = "",
    code: str = "",
    fields: Mapping[str, Any] | None = None,
    to_notion: bool | None = None,
    to_redis: bool = True,
    event_id: str | None = None,
) -> dict[str, Any]:
    """
    Canonical runbook-grade event.

    kind  : subsystem (infra/scanner/entry/router/risk/exit/db/app/etc)
    msg   : human message
    name  : optional component name (e.g. "raydium", "dexscreener", "rpc_failover")
    code  : optional reason code (e.g. "NO_CANDIDATES", "RPC_UNHEALTHY")
    fields: structured details (auto-redacted)
    """
    ts = _now_ts()
    eid = event_id or _make_event_id()
    lvl = _level_norm(level)

    msg_s = _safe_str(msg, MAX_MSG_STR)
    kind_s = _safe_str(kind or "event", 64)
    name_s = _safe_str(name, 96)
    code_s = _safe_str(code, 96)

    meta = _host_meta()
    f = _sanitize_fields(fields or {})

    event = {
        "id": eid,
        "ts": ts,
        "time": _utc_iso(ts),
        "level": lvl,
        "kind": kind_s,
        "name": name_s,
        "code": code_s,
        "msg": msg_s,
        "fields": f,
        **meta,
    }

    # Redis stream write (best-effort)
    if to_redis:
        try:
            h = await _get_redis()
            r = h.client
            # Streams only accept flat string fields; store compact JSON in one field
            payload = json.dumps(event, ensure_ascii=False, separators=(",", ":"), default=str)
            await r.xadd(
                PLAYBOOK_STREAM_KEY,
                {"json": payload, "kind": kind_s, "level": lvl, "code": code_s, "name": name_s},
                maxlen=PLAYBOOK_STREAM_MAXLEN,
                approximate=True,
            )
            # Also keep "last event" pointer for dashboards
            try:
                await r.set(f"playbook:last:{kind_s}", payload)
                await r.set(f"playbook:last:{kind_s}:{name_s or 'default'}", payload)
            except Exception:
                pass
        except Exception:
            # never fail callers
            pass

    # Notion write (best-effort, deduped)
    if to_notion is None:
        # Default: only send warnings/errors or explicit why-not-trading codes
        to_notion = (lvl in {"WARNING", "ERROR", "CRITICAL"}) or (kind_s == "why_not_trading")

    if to_notion and NOTION_ENABLED:
        try:
            # Dedupe to avoid Notion spam (per event id)
            try:
                h = await _get_redis()
                r = h.client
                dedupe_key = f"playbook:notion:dedupe:{eid}"
                ok = True
                try:
                    # setnx style
                    set_ok = await r.set(dedupe_key, "1", nx=True, ex=NOTION_DEDUPE_TTL_SEC)  # type: ignore
                    ok = bool(set_ok)
                except Exception:
                    ok = True
                if not ok:
                    return event
            except Exception:
                pass

            title = f"[{lvl}] {kind_s}{('/' + name_s) if name_s else ''}"
            props = {
                "Title": _notion_title(title),
                "Kind": _notion_rich_text(kind_s),
                "Name": _notion_rich_text(name_s or ""),
                "Code": _notion_rich_text(code_s or ""),
                "Level": _notion_rich_text(lvl),
                "Message": _notion_rich_text(msg_s),
                "Timestamp": _notion_date_iso(ts),
                "EventId": _notion_rich_text(eid),
                "Host": _notion_rich_text(_safe_str(meta.get("host", ""))),
                "Service": _notion_rich_text(_safe_str(meta.get("service", ""))),
                "Version": _notion_rich_text(_safe_str(meta.get("version", ""))),
                "Fields": _notion_rich_text(
                    _safe_str(json.dumps(f, ensure_ascii=False, default=str), 1800)
                ),
            }
            await _notion_create_page(title=title, props=props)
        except Exception:
            pass

    return event


async def set_status(
    name: str,
    *,
    ok: bool | None = None,
    state: str = "",
    msg: str = "",
    fields: Mapping[str, Any] | None = None,
    ttl_sec: int = STATUS_DEFAULT_TTL_SEC,
) -> dict[str, Any]:
    """
    Store a structured status snapshot used by dashboards / health tooling.

    Example names:
      - "infra"
      - "router"
      - "scanner:raydium"
      - "entry_gate"
      - "budget"
    """
    ts = _now_ts()
    payload = {
        "ts": ts,
        "time": _utc_iso(ts),
        "name": _safe_str(name, 120),
        "ok": ok,
        "state": _safe_str(state, 120),
        "msg": _safe_str(msg, MAX_MSG_STR),
        "fields": _sanitize_fields(fields or {}),
        **_host_meta(),
    }
    try:
        h = await _get_redis()
        r = h.client
        key = f"{STATUS_KEY_PREFIX}{payload['name']}"
        blob = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)
        try:
            await r.setex(key, int(ttl_sec), blob)
        except Exception:
            await r.set(key, blob)
            with contextlib.suppress(Exception):
                await r.expire(key, int(ttl_sec))
    except Exception:
        pass
    return payload


# --- Lever changes ------------------------------------------------------------


async def log_lever_change(
    lever: str,
    old: Any,
    new: Any,
    reason: str = "",
    kpi_before: str = "",
    kpi_after: str = "",
    *,
    to_notion: bool | None = True,
) -> dict[str, Any]:
    """
    Records a lever change to Redis (keys + stream) and optionally Notion.
    """
    lever_s = _safe_str(lever, 180)
    ts = _now_ts()

    # Store canonical lever keys (best-effort)
    try:
        h = await _get_redis()
        r = h.client
        base = f"playbook:lever:{lever_s}"
        await r.set(f"{base}:value", _safe_str(new))
        await r.set(f"{base}:old_value", _safe_str(old))
        await r.set(f"{base}:reason", _safe_str(reason))
        await r.set(f"{base}:kpi_before", _safe_str(kpi_before))
        await r.set(f"{base}:kpi_after", _safe_str(kpi_after))
        await r.set(f"{base}:last_change_ts", str(ts))
        with contextlib.suppress(Exception):
            await r.incrby(f"{base}:changes", 1)
    except Exception:
        pass

    # Stream + Notion entry
    return await log_event(
        kind="lever",
        name=lever_s,
        level="INFO",
        code="LEVER_CHANGE",
        msg=f"Lever changed: {lever_s} = {old} → {new}",
        fields={
            "lever": lever_s,
            "old": _safe_str(old),
            "new": _safe_str(new),
            "reason": reason,
            "kpi_before": kpi_before,
            "kpi_after": kpi_after,
        },
        to_notion=to_notion,
    )


# --- Why-not-trading ----------------------------------------------------------

WHY_CODES = {
    # infra
    "INFRA_DOWN",
    "PROXY_DOWN",
    "REDIS_DOWN",
    "DB_DOWN",
    "RPC_UNHEALTHY",
    "ROUTER_UNHEALTHY",
    "UPSTREAM_TIMEOUT",
    # discovery
    "NO_CANDIDATES",
    "SCANNER_DRY",
    "SCANNER_ERROR",
    # gates
    "GATE_REJECT",
    "RISK_REJECT",
    "HONEYPOT_REJECT",
    "GUARDRAIL_REJECT",
    "CIRCUIT_BREAKER",
    "BUDGET_LOCK",
    "COOLDOWN",
    "MINT_DENY",
    # execution
    "QUOTE_FAIL",
    "SWAP_FAIL",
    "CONFIRM_FAIL",
    "SLIPPAGE_TOO_HIGH",
    # exits/positions
    "POSITION_LIMIT",
    "DAILY_LOSS_LIMIT",
    "WALLET_LOW_BAL",
}


async def why_not_trading(
    code: str,
    msg: str,
    *,
    name: str = "",
    fields: Mapping[str, Any] | None = None,
    to_notion: bool | None = None,
) -> dict[str, Any]:
    """
    Runbook-grade “why not trading” event + status snapshot.

    code should be one of WHY_CODES (best-effort; not enforced).
    """
    code_s = _safe_str(code, 120)
    if code_s and code_s not in WHY_CODES:
        # allow custom codes but keep it obvious in logs
        code_s = f"CUSTOM_{code_s}"

    ev = await log_event(
        kind="why_not_trading",
        name=name,
        level="WARNING",
        code=code_s,
        msg=msg,
        fields=fields,
        to_notion=to_notion,
    )

    # Also update status key for quick dashboard glance
    await set_status(
        f"why_not_trading:{name or 'global'}",
        ok=False,
        state=code_s,
        msg=msg,
        fields=fields,
        ttl_sec=STATUS_DEFAULT_TTL_SEC,
    )

    return ev


# -----------------------------------------------------------------------------
# Sync convenience wrappers (safe in scripts)
# -----------------------------------------------------------------------------


def log_event_sync(**kwargs: Any) -> dict[str, Any]:
    return asyncio.run(log_event(**kwargs))


def log_lever_change_sync(
    lever: str, old: Any, new: Any, reason: str = "", kpi_before: str = "", kpi_after: str = ""
) -> dict[str, Any]:
    return asyncio.run(log_lever_change(lever, old, new, reason, kpi_before, kpi_after))


def why_not_trading_sync(
    code: str, msg: str, name: str = "", fields: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    return asyncio.run(why_not_trading(code, msg, name=name, fields=fields))


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------


def _parse_json_kv(items: Iterable[str]) -> dict[str, Any]:
    """
    Accepts:
      --field k=v
      --field '{"a":1}'
    """
    out: dict[str, Any] = {}
    for it in items:
        it = (it or "").strip()
        if not it:
            continue
        if it.startswith("{") and it.endswith("}"):
            try:
                out.update(json.loads(it))
                continue
            except Exception:
                pass
        if "=" in it:
            k, v = it.split("=", 1)
            out[_safe_str(k, 120)] = v
        else:
            out[f"arg_{len(out) + 1}"] = it
    return out


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Playbook logger (Redis + optional Notion)")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_lev = sub.add_parser("lever", help="Record a lever change")
    p_lev.add_argument("--lever", required=True, help="e.g. VELOCITY_VOL_RATIO_MIN")
    p_lev.add_argument("--old", required=True, help="old value")
    p_lev.add_argument("--new", required=True, help="new value")
    p_lev.add_argument("--reason", default="", help="why you changed it")
    p_lev.add_argument("--kpi_before", default="", help="short KPI snapshot before")
    p_lev.add_argument("--kpi_after", default="", help="short KPI snapshot after")

    p_evt = sub.add_parser("event", help="Record a generic event")
    p_evt.add_argument("--kind", required=True, help="subsystem (infra/scanner/entry/router/etc)")
    p_evt.add_argument("--msg", required=True, help="message")
    p_evt.add_argument("--level", default="INFO", help="DEBUG/INFO/WARNING/ERROR/CRITICAL")
    p_evt.add_argument("--name", default="", help="component name")
    p_evt.add_argument("--code", default="", help="optional code")
    p_evt.add_argument("--field", action="append", default=[], help="extra fields: k=v or JSON")

    p_wnt = sub.add_parser("why", help="Record a why-not-trading event")
    p_wnt.add_argument(
        "--code", required=True, help="reason code (e.g. NO_CANDIDATES, RPC_UNHEALTHY)"
    )
    p_wnt.add_argument("--msg", required=True, help="human message")
    p_wnt.add_argument("--name", default="", help="scope name (e.g. raydium, router)")
    p_wnt.add_argument("--field", action="append", default=[], help="extra fields: k=v or JSON")

    p_stat = sub.add_parser("status", help="Set a status snapshot key")
    p_stat.add_argument("--name", required=True, help="status name (e.g. router, scanner:raydium)")
    p_stat.add_argument("--ok", default="", help="true/false/blank")
    p_stat.add_argument("--state", default="", help="state string")
    p_stat.add_argument("--msg", default="", help="message")
    p_stat.add_argument("--ttl", type=int, default=STATUS_DEFAULT_TTL_SEC, help="ttl seconds")
    p_stat.add_argument("--field", action="append", default=[], help="extra fields: k=v or JSON")

    return p


async def _amain(argv: list[str]) -> int:
    # minimal logging if caller didn’t configure it
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )

    args = _build_parser().parse_args(argv)

    if args.cmd == "lever":
        await log_lever_change(
            args.lever,
            args.old,
            args.new,
            args.reason,
            args.kpi_before,
            args.kpi_after,
            to_notion=True,
        )
        return 0

    if args.cmd == "event":
        fields = _parse_json_kv(args.field)
        await log_event(
            kind=args.kind,
            msg=args.msg,
            level=args.level,
            name=args.name,
            code=args.code,
            fields=fields,
            to_notion=None,
        )
        return 0

    if args.cmd == "why":
        fields = _parse_json_kv(args.field)
        await why_not_trading(args.code, args.msg, name=args.name, fields=fields, to_notion=True)
        return 0

    if args.cmd == "status":
        ok_val: bool | None
        ok_val = None if not str(args.ok).strip() else _truthy(str(args.ok))
        fields = _parse_json_kv(args.field)
        await set_status(
            args.name,
            ok=ok_val,
            state=args.state,
            msg=args.msg,
            fields=fields,
            ttl_sec=int(args.ttl),
        )
        return 0

    return 2


def main() -> None:
    try:
        rc = asyncio.run(_amain(list(os.sys.argv[1:])))
    except KeyboardInterrupt:
        rc = 130
    raise SystemExit(int(rc))


if __name__ == "__main__":
    main()
