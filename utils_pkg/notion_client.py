# utils_pkg/notion_client.py
from __future__ import annotations

"""
Notion client (God-Tier / production-grade)
===========================================

Goals
-----
- ONE canonical Notion API client for the whole repo.
- Safe, retrying, rate-aware, and *never* crashes the trading loop unless you choose strict behavior.
- Supports both async (preferred) and sync (for scripts).
- Schema discovery helpers (title/date props), generic query pagination,
  and safe upsert patterns (skip formula/rollup, coerce types, ignore unknowns).

Env contract (common)
---------------------
NOTION_TOKEN / NOTION_API_KEY
NOTION_VERSION                      (default 2022-06-28)
NOTION_TIMEOUT_SEC                  (default 20)
NOTION_RPS                          (default 2.0)  # light throttle
NOTION_MAX_RETRIES                  (default 5)
NOTION_BACKOFF_BASE_SEC             (default 0.6)
NOTION_BACKOFF_MAX_SEC              (default 6.0)
NOTION_ALLOW_DRYRUN                 (default true)
NOTION_TITLE_PROP                   (optional override)
NOTION_DATE_PROP                    (optional override; default "Date")

DRYRUN / DRY_RUN / IS_DRYRUN are respected (writes can be disabled via NOTION_ALLOW_DRYRUN=0).

Exports
-------
- NotionAsync: async client for bot runtime
- Notion: sync client for scripts (backwards compatible name)
- DbSchema, NotionError
"""

import asyncio
import contextlib
import json
import os
import random
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, Callable

import httpx

# logger fallback
try:  # pragma: no cover
    from logging_setup import get_logger  # type: ignore
except Exception:  # pragma: no cover
    import logging

    def get_logger(name: str | None = None):  # type: ignore
        return logging.getLogger(name or "notion_client")


log = get_logger(__name__)

# Optional metrics hook (no-op if not present)
try:  # pragma: no cover
    from logging_setup import bump  # type: ignore
except Exception:  # pragma: no cover

    def bump(*_a: Any, **_k: Any) -> None:  # type: ignore
        return None


NOTION_API = "https://api.notion.com/v1"
_DEFAULT_VER = "2022-06-28"

# Notion transient-ish statuses; 409 happens for conflict/rate, 429 rate limit
TRANSIENT_STATUS: set[int] = {408, 409, 425, 429, 500, 502, 503, 504}


# ──────────────────────────────────────────────────────────────────────────────
# Env helpers
# ──────────────────────────────────────────────────────────────────────────────
_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _b(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return default


def _s(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()


def _i(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    try:
        return int(float(str(v).strip()))
    except Exception:
        return default


def _f(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    try:
        return float(str(v).strip())
    except Exception:
        return default


def _dryrun_on() -> bool:
    return _b("DRYRUN", False) or _b("DRY_RUN", False) or _b("IS_DRYRUN", False)


def _clip(s: str, n: int) -> str:
    t = (s or "").strip()
    return t if len(t) <= n else t[: max(0, n - 1)] + "…"


def _json(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return "{}"


def normalize_notion_id(x: str) -> str:
    """
    Accepts with/without dashes. Returns canonical dashed 32-hex form when possible.
    If not 32 hex, returns stripped input.
    """
    t = (x or "").strip()
    if not t:
        return ""
    raw = t.replace("-", "")
    if len(raw) != 32:
        return t
    # best-effort hex check
    try:
        int(raw, 16)
    except Exception:
        return t
    return f"{raw[0:8]}-{raw[8:12]}-{raw[12:16]}-{raw[16:20]}-{raw[20:32]}"


def _hdrs(token: str, version: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": version,
        "Content-Type": "application/json",
    }


def _utc_iso(ts: datetime | None = None) -> str:
    dt = ts or datetime.now(UTC)
    return dt.isoformat(timespec="seconds")


# ──────────────────────────────────────────────────────────────────────────────
# Types / Errors
# ──────────────────────────────────────────────────────────────────────────────
class NotionError(RuntimeError):
    def __init__(
        self,
        msg: str,
        *,
        status_code: int | None = None,
        body: str | None = None,
        retryable: bool = False,
        request_id: str | None = None,
    ) -> None:
        super().__init__(msg)
        self.status_code = status_code
        self.body = body
        self.retryable = retryable
        self.request_id = request_id

    def short(self) -> str:
        bits = [str(self)]
        if self.status_code is not None:
            bits.append(f"status={self.status_code}")
        if self.request_id:
            bits.append(f"rid={self.request_id}")
        return " ".join(bits)


@dataclass(frozen=True)
class DbSchema:
    id: str
    title_prop: str
    date_prop: str
    prop_types: dict[str, str]  # name -> type (number|date|title|formula|select|...)
    has_prop: dict[str, bool]  # name -> True
    raw_properties: dict[str, Any]  # full Notion property metadata


# ──────────────────────────────────────────────────────────────────────────────
# Property builders (safe)
# ──────────────────────────────────────────────────────────────────────────────
def prop_title(text: str) -> dict[str, Any]:
    return {"title": [{"type": "text", "text": {"content": str(text or "")}}]}


def prop_rich_text(text: str) -> dict[str, Any]:
    return {"rich_text": [{"type": "text", "text": {"content": str(text or "")}}]}


def prop_number(x: Any) -> dict[str, Any] | None:
    if x is None:
        return None
    try:
        return {"number": float(x)}
    except Exception:
        return None


def prop_select(name: Any) -> dict[str, Any] | None:
    if name is None:
        return None
    s = str(name).strip()
    if not s:
        return None
    return {"select": {"name": s}}


def prop_multi_select(values: Any) -> dict[str, Any] | None:
    if values is None:
        return None
    if isinstance(values, str):
        vals = [v.strip() for v in values.split(",") if v.strip()]
    elif isinstance(values, (list, tuple, set)):
        vals = [str(v).strip() for v in values if str(v).strip()]
    else:
        vals = [str(values).strip()] if str(values).strip() else []
    if not vals:
        return None
    return {"multi_select": [{"name": v} for v in vals]}


def prop_checkbox(x: Any) -> dict[str, Any]:
    return {"checkbox": bool(x)}


def prop_url(x: Any) -> dict[str, Any] | None:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    return {"url": s}


def prop_date_start(x: Any) -> dict[str, Any] | None:
    if x is None:
        return None
    if isinstance(x, (datetime, date)):
        if isinstance(x, date) and not isinstance(x, datetime):
            s = x.isoformat()
        else:
            s = x.astimezone(UTC).isoformat(timespec="seconds")
    else:
        s = str(x).strip()
    if not s:
        return None
    # Notion accepts "YYYY-MM-DD" or ISO datetime
    return {"date": {"start": s}}


_TYPE_BUILDERS: dict[str, Callable[[Any], dict[str, Any] | None]] = {
    "title": lambda v: prop_title(str(v or "")),
    "rich_text": lambda v: prop_rich_text(str(v or "")),
    "number": lambda v: prop_number(v),
    "select": lambda v: prop_select(v),
    "multi_select": lambda v: prop_multi_select(v),
    "checkbox": lambda v: prop_checkbox(v),
    "url": lambda v: prop_url(v),
    "date": lambda v: prop_date_start(v),
}


def _looks_like_raw_prop_payload(v: Any) -> bool:
    # allow passing direct Notion property object like {"number": 1.0}
    if not isinstance(v, dict):
        return False
    if len(v.keys()) != 1:
        return False
    k = next(iter(v.keys()))
    return str(k) in _TYPE_BUILDERS or str(k) in (
        "people",
        "email",
        "phone_number",
        "status",
        "relation",
    )


_SKIP_WRITE_TYPES = {"formula", "rollup", "created_time", "last_edited_time", "unique_id"}


def build_properties_payload(
    schema: DbSchema,
    *,
    values: Mapping[str, Any],
    iso_date: str | None = None,
    title_text: str | None = None,
    allow_raw: bool = True,
    strict: bool = False,
) -> dict[str, Any]:
    """
    Pure builder: create a Notion "properties" payload from python values, respecting schema types.
    - Always sets schema.date_prop if iso_date provided
    - Always sets schema.title_prop if title_text provided (can be empty string)
    - Skips formula/rollup/created_time/last_edited_time/unique_id (Notion rejects writes)
    - Unknown properties are skipped (unless strict=True)
    """
    props_out: dict[str, Any] = {}

    if iso_date:
        props_out[schema.date_prop] = {"date": {"start": str(iso_date)}}

    if title_text is not None:
        props_out[schema.title_prop] = prop_title(str(title_text))

    for name, val in values.items():
        if name in (schema.date_prop, schema.title_prop):
            continue

        if not schema.has_prop.get(name, False):
            if strict:
                raise NotionError(f"Property not found in DB schema: {name}")
            continue

        ptype = schema.prop_types.get(name) or ""
        if ptype in _SKIP_WRITE_TYPES:
            continue

        if allow_raw and _looks_like_raw_prop_payload(val):
            props_out[name] = val
            continue

        builder = _TYPE_BUILDERS.get(ptype)
        if not builder:
            if strict:
                raise NotionError(f"Unsupported property type '{ptype}' for '{name}'")
            continue

        built = builder(val)
        if built is None:
            continue
        props_out[name] = built

    return props_out


# ──────────────────────────────────────────────────────────────────────────────
# Rate gates
# ──────────────────────────────────────────────────────────────────────────────
class _AsyncRateGate:
    def __init__(self, rps: float) -> None:
        self._lock = asyncio.Lock()
        self._next = 0.0
        self._rps = max(0.0, float(rps or 0.0))

    async def wait(self) -> None:
        if self._rps <= 0:
            return
        min_interval = 1.0 / self._rps
        async with self._lock:
            now = time.monotonic()
            if now < self._next:
                await asyncio.sleep(self._next - now)
            self._next = time.monotonic() + min_interval


class _SyncRateGate:
    def __init__(self, rps: float) -> None:
        self._next = 0.0
        self._rps = max(0.0, float(rps or 0.0))

    def wait(self) -> None:
        if self._rps <= 0:
            return
        min_interval = 1.0 / self._rps
        now = time.monotonic()
        if now < self._next:
            time.sleep(self._next - now)
        self._next = time.monotonic() + min_interval


def _parse_retry_after(headers: Mapping[str, str]) -> float | None:
    ra = headers.get("retry-after") or headers.get("Retry-After") or ""
    if not ra:
        return None
    try:
        return float(str(ra).strip())
    except Exception:
        return None


def _request_id(headers: Mapping[str, str]) -> str | None:
    return headers.get("x-request-id") or headers.get("request-id") or None


# ──────────────────────────────────────────────────────────────────────────────
# Async client (preferred)
# ──────────────────────────────────────────────────────────────────────────────
class NotionAsync:
    """
    Async Notion client for runtime usage in the bot.

    Best-effort:
      - read methods raise NotionError (scripts/tests can fail-fast)
      - writes respect dryrun gating (NOTION_ALLOW_DRYRUN=0 disables writes under DRYRUN)
    """

    def __init__(
        self,
        token: str | None = None,
        *,
        version: str | None = None,
        timeout_sec: float | None = None,
        max_retries: int | None = None,
        backoff_base_sec: float | None = None,
        backoff_max_sec: float | None = None,
        rps: float | None = None,
        allow_dryrun: bool | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.token = (token or _s("NOTION_TOKEN") or _s("NOTION_API_KEY")).strip()
        self.version = (version or _s("NOTION_VERSION", _DEFAULT_VER)).strip() or _DEFAULT_VER
        self.timeout_sec = float(
            timeout_sec if timeout_sec is not None else _f("NOTION_TIMEOUT_SEC", 20.0)
        )
        self.max_retries = int(
            max_retries if max_retries is not None else max(1, _i("NOTION_MAX_RETRIES", 5))
        )
        self.backoff_base_sec = float(
            backoff_base_sec if backoff_base_sec is not None else _f("NOTION_BACKOFF_BASE_SEC", 0.6)
        )
        self.backoff_max_sec = float(
            backoff_max_sec if backoff_max_sec is not None else _f("NOTION_BACKOFF_MAX_SEC", 6.0)
        )
        self.allow_dryrun = bool(
            allow_dryrun if allow_dryrun is not None else _b("NOTION_ALLOW_DRYRUN", True)
        )

        rps_val = float(rps if rps is not None else _f("NOTION_RPS", 2.0))
        self._gate = _AsyncRateGate(rps_val)

        self._external_client = client is not None
        self._client: httpx.AsyncClient | None = client
        self._client_lock = asyncio.Lock()
        self._schema_cache: dict[str, DbSchema] = {}

        if self.token:
            log.info(
                "NOTION_CLIENT_READY",
                extra={
                    "version": self.version,
                    "timeout": self.timeout_sec,
                    "max_retries": self.max_retries,
                    "rps": rps_val,
                    "dryrun": _dryrun_on(),
                    "allow_dryrun": self.allow_dryrun,
                },
            )
        else:
            log.warning("NOTION_CLIENT_NO_TOKEN")

    def ok(self) -> bool:
        return bool(self.token)

    async def __aenter__(self) -> NotionAsync:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()

    async def _ensure_client(self) -> httpx.AsyncClient:
        client = self._client
        if client is not None:
            return client

        async with self._client_lock:
            client = self._client
            if client is not None:
                return client

            client = httpx.AsyncClient(
                headers=_hdrs(self.token, self.version),
                timeout=httpx.Timeout(self.timeout_sec),
            )
            self._client = client
            return client

    async def aclose(self) -> None:
        if not self._external_client and self._client is not None:
            with contextlib.suppress(Exception):
                await self._client.aclose()
        self._client = None

    def _writes_allowed(self) -> bool:
        # If dryrun is ON and allow_dryrun is False -> block writes
        if _dryrun_on() and (not self.allow_dryrun):
            return False
        return True

    async def _sleep_backoff(self, attempt: int, *, retry_after: float | None = None) -> None:
        if retry_after is not None:
            await asyncio.sleep(min(max(0.5, retry_after), self.backoff_max_sec))
            return
        base = self.backoff_base_sec * float(attempt)
        jitter = random.random() * 0.25
        await asyncio.sleep(min(self.backoff_max_sec, base + jitter))

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not self.token:
            raise NotionError("NOTION token missing", retryable=False)

        c = await self._ensure_client()
        url = f"{NOTION_API}{path}"

        last_err: NotionError | None = None

        for attempt in range(1, self.max_retries + 1):
            await self._gate.wait()
            try:
                resp = await c.request(method, url, json=json_body, params=params)
                rid = _request_id(resp.headers)

                if resp.status_code in (200, 201):
                    bump("notion_ok")
                    data = resp.json()
                    return data if isinstance(data, dict) else {}

                if resp.status_code == 429:
                    bump("notion_429")
                    retry_after = _parse_retry_after(resp.headers)
                    last_err = NotionError(
                        f"Notion rate limited ({resp.status_code})",
                        status_code=resp.status_code,
                        body=_clip(resp.text, 800),
                        retryable=True,
                        request_id=rid,
                    )
                    await self._sleep_backoff(attempt, retry_after=retry_after)
                    continue

                if resp.status_code in TRANSIENT_STATUS:
                    bump("notion_transient")
                    last_err = NotionError(
                        f"Notion transient error ({resp.status_code})",
                        status_code=resp.status_code,
                        body=_clip(resp.text, 800),
                        retryable=True,
                        request_id=rid,
                    )
                    await self._sleep_backoff(attempt)
                    continue

                bump("notion_fail")
                raise NotionError(
                    f"Notion request failed ({resp.status_code})",
                    status_code=resp.status_code,
                    body=_clip(resp.text, 1200),
                    retryable=False,
                    request_id=rid,
                )

            except httpx.TimeoutException as e:
                bump("notion_timeout")
                last_err = NotionError(f"Notion timeout: {e}", retryable=True)
                await self._sleep_backoff(attempt)
            except httpx.HTTPError as e:
                bump("notion_http_err")
                last_err = NotionError(f"Notion http error: {e}", retryable=True)
                await self._sleep_backoff(attempt)
            except NotionError:
                raise
            except Exception as e:
                bump("notion_exc")
                last_err = NotionError(f"Notion unexpected error: {e}", retryable=True)
                await self._sleep_backoff(attempt)

        raise last_err or NotionError("Notion request failed after retries", retryable=True)

    # ────────────────────────────────────────────────────────────────────
    # Schema
    # ────────────────────────────────────────────────────────────────────
    async def get_database(self, database_id: str) -> dict[str, Any]:
        dbid = normalize_notion_id(database_id)
        return await self._request("GET", f"/databases/{dbid}")

    async def discover_schema(self, database_id: str, *, use_cache: bool = True) -> DbSchema:
        dbid = normalize_notion_id(database_id)
        if use_cache and dbid in self._schema_cache:
            return self._schema_cache[dbid]

        data = await self.get_database(dbid)
        props = (data.get("properties") or {}) if isinstance(data, dict) else {}
        if not isinstance(props, dict):
            props = {}

        # title prop: env override or first property with type "title"
        title_prop = _s("NOTION_TITLE_PROP", "")
        if not title_prop:
            title_prop = next((n for n, m in props.items() if (m or {}).get("type") == "title"), "")
        if (
            not title_prop
            or title_prop not in props
            or (props[title_prop] or {}).get("type") != "title"
        ):
            raise NotionError(
                "Could not locate Notion title property. Set NOTION_TITLE_PROP to your title column name."
            )

        # date prop: env override or prefer "Date" then first "date"
        date_prop = _s("NOTION_DATE_PROP", "Date")
        if date_prop not in props or (props[date_prop] or {}).get("type") != "date":
            fallback = next((n for n, m in props.items() if (m or {}).get("type") == "date"), "")
            if not fallback:
                raise NotionError(
                    "No Notion date property found. Set NOTION_DATE_PROP or add a date column."
                )
            date_prop = fallback

        prop_types: dict[str, str] = {
            name: str((meta or {}).get("type") or "") for name, meta in props.items()
        }
        schema = DbSchema(
            id=str(data.get("id") or dbid),
            title_prop=title_prop,
            date_prop=date_prop,
            prop_types=prop_types,
            has_prop={name: True for name in props},
            raw_properties=props,
        )
        self._schema_cache[dbid] = schema
        return schema

    # ────────────────────────────────────────────────────────────────────
    # Query pagination
    # ────────────────────────────────────────────────────────────────────
    async def query_database(
        self,
        database_id: str,
        *,
        filter_obj: dict[str, Any] | None = None,
        sorts: list[dict[str, Any]] | None = None,
        page_size: int = 100,
        start_cursor: str | None = None,
        max_pages: int = 50,
    ) -> list[dict[str, Any]]:
        """
        Returns all results up to max_pages * page_size.
        """
        dbid = normalize_notion_id(database_id)
        out: list[dict[str, Any]] = []
        cursor = start_cursor

        for _ in range(max_pages):
            payload: dict[str, Any] = {"page_size": max(1, min(int(page_size), 100))}
            if filter_obj:
                payload["filter"] = filter_obj
            if sorts:
                payload["sorts"] = sorts
            if cursor:
                payload["start_cursor"] = cursor

            data = await self._request("POST", f"/databases/{dbid}/query", json_body=payload)
            results = data.get("results") or []
            if isinstance(results, list):
                out.extend([r for r in results if isinstance(r, dict)])

            if not data.get("has_more"):
                break
            cursor = data.get("next_cursor")
            if not cursor:
                break

        return out

    async def find_page_by_date(
        self, database_id: str, date_prop: str, iso_date: str
    ) -> str | None:
        """
        Return first page id whose date_prop equals iso_date (YYYY-MM-DD) or full ISO.
        """
        dbid = normalize_notion_id(database_id)
        flt = {"property": date_prop, "date": {"equals": iso_date}}
        rows = await self.query_database(dbid, filter_obj=flt, page_size=1, max_pages=1)
        if not rows:
            return None
        pid = rows[0].get("id")
        return normalize_notion_id(str(pid)) if pid else None

    # ────────────────────────────────────────────────────────────────────
    # Property builder (pure)
    # ────────────────────────────────────────────────────────────────────
    def build_properties(
        self,
        schema: DbSchema,
        *,
        values: Mapping[str, Any],
        iso_date: str | None = None,
        title_text: str | None = None,
        allow_raw: bool = True,
        strict: bool = False,
    ) -> dict[str, Any]:
        return build_properties_payload(
            schema,
            values=values,
            iso_date=iso_date,
            title_text=title_text,
            allow_raw=allow_raw,
            strict=strict,
        )

    # ────────────────────────────────────────────────────────────────────
    # Writes (create/update/upsert)
    # ────────────────────────────────────────────────────────────────────
    async def create_page(self, database_id: str, properties: dict[str, Any]) -> str:
        if not self._writes_allowed():
            bump("notion_dryrun_skip")
            return "DRYRUN_PAGE"
        dbid = normalize_notion_id(database_id)
        data = await self._request(
            "POST",
            "/pages",
            json_body={"parent": {"database_id": dbid}, "properties": properties},
        )
        pid = data.get("id")
        return normalize_notion_id(str(pid)) if pid else ""

    async def update_page(self, page_id: str, properties: dict[str, Any]) -> str:
        if not self._writes_allowed():
            bump("notion_dryrun_skip")
            return normalize_notion_id(page_id) or "DRYRUN_PAGE"
        pid = normalize_notion_id(page_id)
        _ = await self._request("PATCH", f"/pages/{pid}", json_body={"properties": properties})
        return pid

    async def upsert_page_by_date(
        self,
        database_id: str,
        schema: DbSchema,
        iso_date: str,
        values: Mapping[str, Any],
        *,
        title_text: str | None = None,
        strict: bool = False,
    ) -> str:
        """
        Upsert pattern:
        - Find by schema.date_prop == iso_date
        - If exists: update properties
        - Else: create page
        """
        t = title_text
        if t is None:
            t = str(values.get(schema.title_prop) or values.get("Notes") or "")

        props = self.build_properties(
            schema, values=values, iso_date=iso_date, title_text=t, strict=strict
        )
        pid = await self.find_page_by_date(database_id, schema.date_prop, iso_date)
        if pid:
            return await self.update_page(pid, props)
        return await self.create_page(database_id, props)


# ──────────────────────────────────────────────────────────────────────────────
# Sync client (for scripts) — keeps old "Notion" name compatible
# ──────────────────────────────────────────────────────────────────────────────
class Notion:
    """
    Sync Notion client (scripts/tools). Backwards compatible with your existing usage.
    NOTE: In the live asyncio bot, prefer NotionAsync to avoid blocking the event loop.
    """

    def __init__(
        self,
        token: str | None = None,
        timeout: float = 20.0,
        *,
        version: str | None = None,
        max_retries: int | None = None,
        backoff_base_sec: float | None = None,
        backoff_max_sec: float | None = None,
        rps: float | None = None,
        allow_dryrun: bool | None = None,
        client: httpx.Client | None = None,
    ) -> None:
        self.token = (token or _s("NOTION_TOKEN") or _s("NOTION_API_KEY")).strip()
        self.version = (version or _s("NOTION_VERSION", _DEFAULT_VER)).strip() or _DEFAULT_VER
        self.timeout = float(timeout)
        self.max_retries = int(
            max_retries if max_retries is not None else max(1, _i("NOTION_MAX_RETRIES", 5))
        )
        self.backoff_base_sec = float(
            backoff_base_sec if backoff_base_sec is not None else _f("NOTION_BACKOFF_BASE_SEC", 0.6)
        )
        self.backoff_max_sec = float(
            backoff_max_sec if backoff_max_sec is not None else _f("NOTION_BACKOFF_MAX_SEC", 6.0)
        )
        self.allow_dryrun = bool(
            allow_dryrun if allow_dryrun is not None else _b("NOTION_ALLOW_DRYRUN", True)
        )

        rps_val = float(rps if rps is not None else _f("NOTION_RPS", 2.0))
        self._gate = _SyncRateGate(rps_val)

        self._external_client = client is not None
        self._client_obj: httpx.Client | None = client
        self._schema_cache: dict[str, DbSchema] = {}

    def ok(self) -> bool:
        return bool(self.token)

    def __enter__(self) -> Notion:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()

    def close(self) -> None:
        if not self._external_client and self._client_obj is not None:
            with contextlib.suppress(Exception):
                self._client_obj.close()
        self._client_obj = None

    def _ensure_client(self) -> httpx.Client:
        if self._client_obj is not None:
            return self._client_obj
        self._client_obj = httpx.Client(
            headers=_hdrs(self.token, self.version),
            timeout=httpx.Timeout(self.timeout),
        )
        return self._client_obj

    def _writes_allowed(self) -> bool:
        if _dryrun_on() and (not self.allow_dryrun):
            return False
        return True

    def _sleep_backoff(self, attempt: int, *, retry_after: float | None = None) -> None:
        if retry_after is not None:
            time.sleep(min(max(0.5, retry_after), self.backoff_max_sec))
            return
        base = self.backoff_base_sec * float(attempt)
        jitter = random.random() * 0.25
        time.sleep(min(self.backoff_max_sec, base + jitter))

    def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not self.token:
            raise NotionError("NOTION token missing", retryable=False)

        url = f"{NOTION_API}{path}"
        last_err: NotionError | None = None
        c = self._ensure_client()

        for attempt in range(1, self.max_retries + 1):
            self._gate.wait()
            try:
                resp = c.request(method, url, json=json_body, params=params)
                rid = _request_id(resp.headers)

                if resp.status_code in (200, 201):
                    bump("notion_ok")
                    data = resp.json()
                    return data if isinstance(data, dict) else {}

                if resp.status_code == 429:
                    bump("notion_429")
                    retry_after = _parse_retry_after(resp.headers)
                    last_err = NotionError(
                        f"Notion rate limited ({resp.status_code})",
                        status_code=resp.status_code,
                        body=_clip(resp.text, 800),
                        retryable=True,
                        request_id=rid,
                    )
                    self._sleep_backoff(attempt, retry_after=retry_after)
                    continue

                if resp.status_code in TRANSIENT_STATUS:
                    bump("notion_transient")
                    last_err = NotionError(
                        f"Notion transient error ({resp.status_code})",
                        status_code=resp.status_code,
                        body=_clip(resp.text, 800),
                        retryable=True,
                        request_id=rid,
                    )
                    self._sleep_backoff(attempt)
                    continue

                bump("notion_fail")
                raise NotionError(
                    f"Notion request failed ({resp.status_code})",
                    status_code=resp.status_code,
                    body=_clip(resp.text, 1200),
                    retryable=False,
                    request_id=rid,
                )

            except httpx.TimeoutException as e:
                bump("notion_timeout")
                last_err = NotionError(f"Notion timeout: {e}", retryable=True)
                self._sleep_backoff(attempt)
            except httpx.HTTPError as e:
                bump("notion_http_err")
                last_err = NotionError(f"Notion http error: {e}", retryable=True)
                self._sleep_backoff(attempt)
            except NotionError:
                raise
            except Exception as e:
                bump("notion_exc")
                last_err = NotionError(f"Notion unexpected error: {e}", retryable=True)
                self._sleep_backoff(attempt)

        raise last_err or NotionError("Notion request failed after retries", retryable=True)

    # Backwards compatible methods
    def get_database(self, database_id: str) -> dict[str, Any]:
        dbid = normalize_notion_id(database_id)
        return self._request("GET", f"/databases/{dbid}")

    def discover_schema(self, database_id: str, *, use_cache: bool = True) -> DbSchema:
        dbid = normalize_notion_id(database_id)
        if use_cache and dbid in self._schema_cache:
            return self._schema_cache[dbid]

        data = self.get_database(dbid)
        props = (data.get("properties") or {}) if isinstance(data, dict) else {}
        if not isinstance(props, dict):
            props = {}

        title_prop = _s("NOTION_TITLE_PROP", "")
        if not title_prop:
            title_prop = next((n for n, m in props.items() if (m or {}).get("type") == "title"), "")
        if (
            not title_prop
            or title_prop not in props
            or (props[title_prop] or {}).get("type") != "title"
        ):
            raise NotionError("Could not locate Notion title property. Set NOTION_TITLE_PROP.")

        date_prop = _s("NOTION_DATE_PROP", "Date")
        if date_prop not in props or (props[date_prop] or {}).get("type") != "date":
            fallback = next((n for n, m in props.items() if (m or {}).get("type") == "date"), "")
            if not fallback:
                raise NotionError(
                    "No Notion date property found. Set NOTION_DATE_PROP or add a date column."
                )
            date_prop = fallback

        prop_types: dict[str, str] = {
            name: str((meta or {}).get("type") or "") for name, meta in props.items()
        }
        schema = DbSchema(
            id=str(data.get("id") or dbid),
            title_prop=title_prop,
            date_prop=date_prop,
            prop_types=prop_types,
            has_prop={name: True for name in props},
            raw_properties=props,
        )
        self._schema_cache[dbid] = schema
        return schema

    def query_database(
        self,
        database_id: str,
        *,
        filter_obj: dict[str, Any] | None = None,
        sorts: list[dict[str, Any]] | None = None,
        page_size: int = 100,
        start_cursor: str | None = None,
        max_pages: int = 50,
    ) -> list[dict[str, Any]]:
        dbid = normalize_notion_id(database_id)
        out: list[dict[str, Any]] = []
        cursor = start_cursor

        for _ in range(max_pages):
            payload: dict[str, Any] = {"page_size": max(1, min(int(page_size), 100))}
            if filter_obj:
                payload["filter"] = filter_obj
            if sorts:
                payload["sorts"] = sorts
            if cursor:
                payload["start_cursor"] = cursor

            data = self._request("POST", f"/databases/{dbid}/query", json_body=payload)
            results = data.get("results") or []
            if isinstance(results, list):
                out.extend([r for r in results if isinstance(r, dict)])

            if not data.get("has_more"):
                break
            cursor = data.get("next_cursor")
            if not cursor:
                break

        return out

    def find_page_by_date(self, database_id: str, date_prop: str, iso_date: str) -> str | None:
        rows = self.query_database(
            database_id,
            filter_obj={"property": date_prop, "date": {"equals": iso_date}},
            page_size=1,
            max_pages=1,
        )
        if not rows:
            return None
        pid = (rows[0] or {}).get("id")
        return normalize_notion_id(str(pid)) if pid else None

    def build_properties(
        self,
        schema: DbSchema,
        *,
        values: Mapping[str, Any],
        iso_date: str | None = None,
        title_text: str | None = None,
        allow_raw: bool = True,
        strict: bool = False,
    ) -> dict[str, Any]:
        return build_properties_payload(
            schema,
            values=values,
            iso_date=iso_date,
            title_text=title_text,
            allow_raw=allow_raw,
            strict=strict,
        )

    def upsert_page(
        self, database_id: str, schema: DbSchema, iso_date: str, values: dict[str, Any]
    ) -> str:
        """
        Backwards compatible upsert: uses date match and writes properties.
        NOTE: blocking (sync) call.
        """
        if not self._writes_allowed():
            bump("notion_dryrun_skip")
            return "DRYRUN_PAGE"

        title_text = str(values.get(schema.title_prop) or values.get("Notes") or "")
        props = self.build_properties(
            schema, values=values, iso_date=iso_date, title_text=title_text
        )

        page_id = self.find_page_by_date(database_id, schema.date_prop, iso_date)
        if page_id:
            pid = normalize_notion_id(page_id)
            _ = self._request("PATCH", f"/pages/{pid}", json_body={"properties": props})
            return pid

        dbid = normalize_notion_id(database_id)
        data = self._request(
            "POST",
            "/pages",
            json_body={"parent": {"database_id": dbid}, "properties": props},
        )
        raw_id = data.get("id")
        return normalize_notion_id(str(raw_id)) if raw_id else ""


__all__ = [
    "NOTION_API",
    "DbSchema",
    "Notion",
    "NotionAsync",
    "NotionError",
    "build_properties_payload",
    "normalize_notion_id",
    "prop_checkbox",
    "prop_date_start",
    "prop_multi_select",
    "prop_number",
    "prop_rich_text",
    "prop_select",
    "prop_title",
    "prop_url",
]
