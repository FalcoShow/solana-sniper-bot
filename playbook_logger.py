# -*- coding: utf-8 -*-
from __future__ import annotations

"""
playbook_logger.py — Runbook-grade events for “why not trading”
===============================================================

Canonical place to record operational truth:
- What levers changed (who/why/old/new)
- Why we are NOT trading right now (gates, infra, budget, scanner drought)
- Key lifecycle events (startup/shutdown, health transitions)

Storage targets (best-effort, never crash the bot):
1) Redis stream      : playbook:events (primary, fast, queryable)
2) Redis status keys : playbook:status:<name> (for dashboards/health)
3) Notion database   : NOTION_DB_PLAYBOOK (optional, human timeline)

Design goals
------------
- Safe: never throws in production paths
- Redacts secrets from fields/logs
- Works with either utils_pkg.core_utils (preferred) or direct REDIS_URL
- Supports async usage (inside bot) and CLI usage (ops)
- Import is side-effect free (no network calls, no background tasks)

IMPORTANT:
- Do not add new env keys here without updating env contract/tests.
"""

import argparse
import asyncio
import contextlib
import json
import logging
import os
import socket
import sys
import time
import uuid
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import lru_cache
from typing import Any

LOG = logging.getLogger("playbook")

# -----------------------------------------------------------------------------
# Optional third-party imports (kept lightweight + mypy-safe)
# -----------------------------------------------------------------------------
# Optional third-party imports (kept lightweight + mypy-safe)
redis_async: Any | None
try:
    import redis.asyncio as redis_async  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    redis_async = None


# -----------------------------------------------------------------------------
# Config (env-driven, cached) — DO NOT introduce new env keys here.
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class PlaybookConfig:
    # Redis
    redis_url: str
    stream_key: str
    stream_maxlen: int
    status_prefix: str
    status_default_ttl_sec: int

    # Notion
    notion_token: str
    notion_db_playbook: str
    notion_enabled: bool
    notion_version: str
    notion_timeout_sec: float
    notion_dedupe_ttl_sec: int

    # Safety / sizing
    max_field_str: int
    max_msg_str: int


def _getenv_str(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _getenv_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return int(default)
    try:
        return int(float(raw))
    except Exception:
        return int(default)


def _getenv_float(name: str, default: float) -> float:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


@lru_cache(maxsize=1)
def cfg() -> PlaybookConfig:
    """
    Cached config. If you need to re-read env at runtime, call refresh_config().
    """
    # Keep existing behavior: allow REDIS_TLS_URL fallback and local default.
    redis_url = _getenv_str("REDIS_URL") or _getenv_str("REDIS_TLS_URL") or "redis://localhost:6379"

    notion_token = _getenv_str("NOTION_TOKEN") or _getenv_str("NOTION_API_KEY")
    notion_db_playbook = (
        _getenv_str("NOTION_DB_PLAYBOOK")
        or _getenv_str("NOTION_DATABASE_PLAYBOOK")
        or _getenv_str("NOTION_PLAYBOOK_DB")
    )

    # Existing enable switch: PLAYBOOK_NOTION_ENABLED != "0"
    enabled_flag = _getenv_str("PLAYBOOK_NOTION_ENABLED", "1")
    notion_enabled = bool(notion_token and notion_db_playbook and enabled_flag != "0")

    return PlaybookConfig(
        redis_url=redis_url,
        stream_key=_getenv_str("PLAYBOOK_STREAM_KEY", "playbook:events"),
        stream_maxlen=_getenv_int("PLAYBOOK_STREAM_MAXLEN", 5000),
        status_prefix=_getenv_str("PLAYBOOK_STATUS_PREFIX", "playbook:status:"),
        status_default_ttl_sec=_getenv_int("PLAYBOOK_STATUS_TTL_SEC", 900),
        notion_token=notion_token,
        notion_db_playbook=notion_db_playbook,
        notion_enabled=notion_enabled,
        notion_version=_getenv_str("NOTION_VERSION", "2022-06-28"),
        notion_timeout_sec=_getenv_float("PLAYBOOK_NOTION_TIMEOUT_SEC", 10.0),
        notion_dedupe_ttl_sec=_getenv_int("PLAYBOOK_NOTION_DEDUPE_TTL_SEC", 86400),
        max_field_str=_getenv_int("PLAYBOOK_MAX_FIELD_STR", 400),
        max_msg_str=_getenv_int("PLAYBOOK_MAX_MSG_STR", 600),
    )


def refresh_config() -> None:
    cfg.cache_clear()


# -----------------------------------------------------------------------------
# Helpers (time, ids, strings, redaction)
# -----------------------------------------------------------------------------
_TRUE = {"1", "true", "yes", "y", "on"}


def _now_ts() -> float:
    return time.time()


def _utc_iso(ts: float | None = None) -> str:
    t = float(ts if ts is not None else _now_ts())
    return datetime.fromtimestamp(t, tz=UTC).isoformat()


def _truthy(v: str | None) -> bool:
    if v is None:
        return False
    return v.strip().lower() in _TRUE


def _safe_str(x: Any, max_len: int) -> str:
    try:
        s = str(x)
    except Exception:
        s = repr(x)
    s = s.replace("\n", " ").replace("\r", " ").strip()
    if len(s) > max_len:
        s = s[: max_len - 1] + "…"
    return s


_SECRET_KEY_FRAGS = (
    "key",
    "token",
    "secret",
    "private",
    "pass",
    "pwd",
    "authorization",
    "bearer",
    "dsn",
)


def _redact_value(v: Any, max_len: int) -> str:
    s = _safe_str(v, max_len=max_len)
    if not s:
        return s
    if len(s) <= 10:
        return "***"
    return f"{s[:3]}…{s[-3:]}"


def _sanitize_fields(fields: Mapping[str, Any], *, max_field_str: int) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in (fields or {}).items():
        ks = _safe_str(k, 80)
        low = ks.lower()

        # Redact by key name
        if any(frag in low for frag in _SECRET_KEY_FRAGS):
            out[ks] = _redact_value(v, max_len=max_field_str)
            continue

        # JSON-safe primitives
        if isinstance(v, (str, int, float, bool)) or v is None:
            out[ks] = _safe_str(v, max_field_str) if isinstance(v, str) else v
            continue

        # Serialize complex values safely
        try:
            out[ks] = json.loads(json.dumps(v, default=str))
        except Exception:
            out[ks] = _safe_str(v, max_field_str)

    return out


def _host_meta() -> dict[str, Any]:
    return {
        "host": socket.gethostname(),
        "pid": os.getpid(),
        "service": _getenv_str("SERVICE", "sniper"),
        "env": _getenv_str("ENV", ""),
        "version": _getenv_str("VERSION", "") or _getenv_str("GIT_SHA", "") or "",
    }


def _make_event_id() -> str:
    return uuid.uuid4().hex


def _level_norm(level: str) -> str:
    s = (level or "").strip().upper()
    if s in {"DEBUG", "INFO", "WARN", "WARNING", "ERROR", "CRITICAL"}:
        return "WARNING" if s == "WARN" else s
    return "INFO"


def _json_compact(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str)


async def _maybe_await(x: Any) -> Any:
    return await x if asyncio.iscoroutine(x) else x


# -----------------------------------------------------------------------------
# Redis access (prefer core_utils; fallback to redis_async; never raises)
# -----------------------------------------------------------------------------
class _NoopRedis:
    async def xadd(self, *_a: Any, **_k: Any) -> None:
        return None

    async def set(self, *_a: Any, **_k: Any) -> None:
        return None

    async def setex(self, *_a: Any, **_k: Any) -> None:
        return None

    async def expire(self, *_a: Any, **_k: Any) -> None:
        return None

    async def incrby(self, *_a: Any, **_k: Any) -> None:
        return None

    async def ping(self) -> bool:
        return False

    async def close(self) -> None:
        return None


@dataclass(frozen=True)
class _RedisHandle:
    client: Any
    owned: bool  # if we created it, we should close it


_REDIS_SINGLETON: _RedisHandle | None = None
_REDIS_LOCK: asyncio.Lock | None = None


def _get_lock() -> asyncio.Lock:
    global _REDIS_LOCK
    if _REDIS_LOCK is None:
        _REDIS_LOCK = asyncio.Lock()
    return _REDIS_LOCK


async def _get_redis() -> _RedisHandle:
    """
    Returns a redis handle (client + owned flag). Best-effort; never raises.
    """
    global _REDIS_SINGLETON
    lock = _get_lock()

    async with lock:
        if _REDIS_SINGLETON is not None:
            return _REDIS_SINGLETON

        # Prefer utils_pkg.core_utils if available
        try:
            from utils_pkg import core_utils as cu  # type: ignore

            fn = getattr(cu, "get_redis_async", None) or getattr(cu, "get_redis", None)
            if callable(fn):
                try:
                    r = fn()
                except TypeError:
                    r = None
                r = await _maybe_await(r)
                if r is not None:
                    _REDIS_SINGLETON = _RedisHandle(client=r, owned=False)
                    return _REDIS_SINGLETON
        except Exception:
            pass

        # Fallback to redis_async if installed
        if redis_async is None:
            _REDIS_SINGLETON = _RedisHandle(client=_NoopRedis(), owned=True)
            return _REDIS_SINGLETON

        # Create our own client (avoid hanging connects)
        try:
            c = cfg()
            r = redis_async.from_url(  # type: ignore[union-attr]
                c.redis_url,
                decode_responses=True,
                socket_connect_timeout=2,
                socket_timeout=2,
            )
            _REDIS_SINGLETON = _RedisHandle(client=r, owned=True)
            return _REDIS_SINGLETON
        except Exception:
            _REDIS_SINGLETON = _RedisHandle(client=_NoopRedis(), owned=True)
            return _REDIS_SINGLETON


async def close() -> None:
    """
    Close redis if we own the connection.
    """
    global _REDIS_SINGLETON
    lock = _get_lock()

    async with lock:
        h = _REDIS_SINGLETON
        _REDIS_SINGLETON = None

    if not h or not h.owned:
        return

    with contextlib.suppress(Exception):
        # redis-py 5+ deprecates close() in favor of aclose()
        aclose = getattr(h.client, "aclose", None)
        if callable(aclose):
            await _maybe_await(aclose())
        else:
            await _maybe_await(h.client.close())


# -----------------------------------------------------------------------------
# Notion (best-effort; never raises)
# -----------------------------------------------------------------------------
def _notion_rich_text(s: str) -> dict[str, Any]:
    return {"rich_text": [{"type": "text", "text": {"content": _safe_str(s, 1800)}}]}


def _notion_title(s: str) -> dict[str, Any]:
    return {"title": [{"type": "text", "text": {"content": _safe_str(s, 200)}}]}


def _notion_date_iso(ts: float) -> dict[str, Any]:
    return {"date": {"start": datetime.fromtimestamp(ts, tz=UTC).isoformat()}}


async def _notion_create_page(*, props: dict[str, Any]) -> None:
    c = cfg()
    if not c.notion_enabled:
        return

    # Prefer utils_pkg.notion_client if present
    try:
        from utils_pkg import notion_client as nc  # type: ignore

        fn = getattr(nc, "create_page", None) or getattr(nc, "create_playbook_entry", None)
        if callable(fn):
            out = fn(database_id=c.notion_db_playbook, properties=props)  # type: ignore
            await _maybe_await(out)
            return
    except Exception:
        pass

    # Fallback raw HTTP (import aiohttp only when needed)
    try:
        import aiohttp  # type: ignore[import-not-found]

        headers = {
            "Authorization": f"Bearer {c.notion_token}",
            "Notion-Version": c.notion_version,
            "Content-Type": "application/json",
        }
        body = {"parent": {"database_id": c.notion_db_playbook}, "properties": props}
        timeout = aiohttp.ClientTimeout(total=float(c.notion_timeout_sec))

        # Prefer utils_pkg.proxy_client session if available
        try:
            from utils_pkg.proxy_client import make_session  # type: ignore

            async with make_session(timeout=c.notion_timeout_sec) as s:  # type: ignore
                async with s.post(
                    "https://api.notion.com/v1/pages", headers=headers, json=body
                ) as r:
                    if r.status not in (200, 201):
                        txt = await r.text()
                        LOG.warning("[notion] create failed: %s %s", r.status, (txt or "")[:200])
            return
        except Exception:
            pass

        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.post("https://api.notion.com/v1/pages", headers=headers, json=body) as r:
                if r.status not in (200, 201):
                    txt = await r.text()
                    LOG.warning("[notion] create failed: %s %s", r.status, (txt or "")[:200])
    except Exception as e:
        LOG.debug("[notion] error: %s", e)


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
    c = cfg()
    ts = _now_ts()
    eid = event_id or _make_event_id()
    lvl = _level_norm(level)

    msg_s = _safe_str(msg, c.max_msg_str)
    kind_s = _safe_str(kind or "event", 64)
    name_s = _safe_str(name, 96)
    code_s = _safe_str(code, 96)

    meta = _host_meta()
    f = _sanitize_fields(fields or {}, max_field_str=c.max_field_str)

    event: dict[str, Any] = {
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

            payload = _json_compact(event)
            stream_fields = {
                "json": payload,
                "kind": kind_s,
                "level": lvl,
                "code": code_s,
                "name": name_s,
            }

            # xadd signatures vary across versions -> tolerate TypeError
            try:
                await r.xadd(
                    c.stream_key,
                    stream_fields,
                    maxlen=int(c.stream_maxlen),
                    approximate=True,
                )
            except TypeError:
                await r.xadd(
                    c.stream_key,
                    stream_fields,
                    maxlen=int(c.stream_maxlen),
                )

            # Last-event pointers for dashboards
            with contextlib.suppress(Exception):
                await r.set(f"playbook:last:{kind_s}", payload)
                await r.set(f"playbook:last:{kind_s}:{name_s or 'default'}", payload)
        except Exception:
            pass

    # Notion write (best-effort, deduped)
    if to_notion is None:
        # Default: only send warnings/errors or explicit why-not-trading events
        to_notion = (lvl in {"WARNING", "ERROR", "CRITICAL"}) or (kind_s == "why_not_trading")

    if to_notion and c.notion_enabled:
        try:
            # Dedupe to avoid Notion spam (per event id)
            allow = True
            try:
                h = await _get_redis()
                r = h.client
                dedupe_key = f"playbook:notion:dedupe:{eid}"
                try:
                    set_ok = await r.set(  # type: ignore[call-arg]
                        dedupe_key, "1", nx=True, ex=int(c.notion_dedupe_ttl_sec)
                    )
                    # redis may return True/False, "OK", or None
                    allow = bool(set_ok)
                except Exception:
                    allow = True
            except Exception:
                allow = True

            if allow:
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
                    "Host": _notion_rich_text(_safe_str(meta.get("host", ""), 200)),
                    "Service": _notion_rich_text(_safe_str(meta.get("service", ""), 200)),
                    "Version": _notion_rich_text(_safe_str(meta.get("version", ""), 200)),
                    "Fields": _notion_rich_text(_safe_str(_json_compact(f), 1800)),
                }
                await _notion_create_page(props=props)
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
    ttl_sec: int | None = None,
) -> dict[str, Any]:
    """
    Store a structured status snapshot used by dashboards / health tooling.
    """
    c = cfg()
    ts = _now_ts()
    ttl = int(ttl_sec) if ttl_sec is not None else int(c.status_default_ttl_sec)

    payload: dict[str, Any] = {
        "ts": ts,
        "time": _utc_iso(ts),
        "name": _safe_str(name, 120),
        "ok": ok,
        "state": _safe_str(state, 120),
        "msg": _safe_str(msg, c.max_msg_str),
        "fields": _sanitize_fields(fields or {}, max_field_str=c.max_field_str),
        **_host_meta(),
    }

    try:
        h = await _get_redis()
        r = h.client
        key = f"{c.status_prefix}{payload['name']}"
        blob = _json_compact(payload)

        try:
            await r.setex(key, ttl, blob)
        except Exception:
            await r.set(key, blob)
            with contextlib.suppress(Exception):
                await r.expire(key, ttl)
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
    Record a lever change to Redis (keys + stream) and optionally Notion.
    """
    c = cfg()
    lever_s = _safe_str(lever, 180)
    ts = _now_ts()

    # Store canonical lever keys (best-effort)
    try:
        h = await _get_redis()
        r = h.client
        base = f"playbook:lever:{lever_s}"
        await r.set(f"{base}:value", _safe_str(new, c.max_field_str))
        await r.set(f"{base}:old_value", _safe_str(old, c.max_field_str))
        await r.set(f"{base}:reason", _safe_str(reason, c.max_msg_str))
        await r.set(f"{base}:kpi_before", _safe_str(kpi_before, c.max_msg_str))
        await r.set(f"{base}:kpi_after", _safe_str(kpi_after, c.max_msg_str))
        await r.set(f"{base}:last_change_ts", str(ts))
        with contextlib.suppress(Exception):
            await r.incrby(f"{base}:changes", 1)
    except Exception:
        pass

    return await log_event(
        kind="lever",
        name=lever_s,
        level="INFO",
        code="LEVER_CHANGE",
        msg=f"Lever changed: {lever_s} = {old} → {new}",
        fields={
            "lever": lever_s,
            "old": _safe_str(old, c.max_field_str),
            "new": _safe_str(new, c.max_field_str),
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
    """
    code_s = _safe_str(code, 120)
    if code_s and code_s not in WHY_CODES:
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

    await set_status(
        f"why_not_trading:{name or 'global'}",
        ok=False,
        state=code_s,
        msg=msg,
        fields=fields,
    )

    return ev


# -----------------------------------------------------------------------------
# Sync convenience wrappers (safe in scripts; not for use inside running loops)
# -----------------------------------------------------------------------------
def _run(coro: Any) -> Any:
    try:
        loop = asyncio.get_running_loop()
    except Exception:
        loop = None

    if loop and loop.is_running():
        raise RuntimeError(
            "Use async functions inside a running event loop (log_event/why_not_trading/etc)."
        )

    return asyncio.run(coro)


def log_event_sync(**kwargs: Any) -> dict[str, Any]:
    return _run(log_event(**kwargs))


def log_lever_change_sync(
    lever: str, old: Any, new: Any, reason: str = "", kpi_before: str = "", kpi_after: str = ""
) -> dict[str, Any]:
    return _run(log_lever_change(lever, old, new, reason, kpi_before, kpi_after))


def why_not_trading_sync(
    code: str, msg: str, name: str = "", fields: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    return _run(why_not_trading(code, msg, name=name, fields=fields))


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def _parse_json_kv(items: Iterable[str], *, max_key_len: int = 120) -> dict[str, Any]:
    """
    Accepts:
      --field k=v
      --field '{"a":1}'
    """
    out: dict[str, Any] = {}
    for it in items:
        s = (it or "").strip()
        if not s:
            continue

        if s.startswith("{") and s.endswith("}"):
            try:
                obj = json.loads(s)
                if isinstance(obj, dict):
                    out.update(obj)
                    continue
            except Exception:
                pass

        if "=" in s:
            k, v = s.split("=", 1)
            out[_safe_str(k, max_key_len)] = v
        else:
            out[f"arg_{len(out) + 1}"] = s

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
    p_stat.add_argument("--ttl", type=int, default=0, help="ttl seconds (0 = default)")
    p_stat.add_argument("--field", action="append", default=[], help="extra fields: k=v or JSON")

    return p


async def _amain(argv: Sequence[str]) -> int:
    # Minimal logging if caller didn’t configure it
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        )

    args = _build_parser().parse_args(list(argv))
    cmd = str(getattr(args, "cmd", "")).strip()

    if cmd == "lever":
        await log_lever_change(
            args.lever,
            args.old,
            args.new,
            getattr(args, "reason", ""),
            getattr(args, "kpi_before", ""),
            getattr(args, "kpi_after", ""),
            to_notion=True,
        )
        return 0

    if cmd == "event":
        fields = _parse_json_kv(getattr(args, "field", []))
        await log_event(
            kind=args.kind,
            msg=args.msg,
            level=getattr(args, "level", "INFO"),
            name=getattr(args, "name", ""),
            code=getattr(args, "code", ""),
            fields=fields,
            to_notion=None,
        )
        return 0

    if cmd == "why":
        fields = _parse_json_kv(getattr(args, "field", []))
        await why_not_trading(
            args.code,
            args.msg,
            name=getattr(args, "name", ""),
            fields=fields,
            to_notion=True,
        )
        return 0

    if cmd == "status":
        ok_raw = str(getattr(args, "ok", "")).strip()
        ok_val: bool | None = None if not ok_raw else _truthy(ok_raw)
        fields = _parse_json_kv(getattr(args, "field", []))
        ttl = int(getattr(args, "ttl", 0) or 0)
        await set_status(
            args.name,
            ok=ok_val,
            state=getattr(args, "state", ""),
            msg=getattr(args, "msg", ""),
            fields=fields,
            ttl_sec=None if ttl <= 0 else ttl,
        )
        return 0

    return 2


def main(argv: Sequence[str] | None = None) -> None:
    try:
        rc = asyncio.run(_amain(list(sys.argv[1:]) if argv is None else argv))
    except KeyboardInterrupt:
        rc = 130
    raise SystemExit(int(rc))


if __name__ == "__main__":
    main()
