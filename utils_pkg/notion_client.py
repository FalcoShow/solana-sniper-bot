# utils_pkg/notion_client.py
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import httpx

NOTION_API = "https://api.notion.com/v1"
NOTION_VER = os.getenv("NOTION_VERSION", "2022-06-28")

TRANSIENT = {408, 409, 425, 429, 500, 502, 503, 504}


class NotionError(RuntimeError):
    pass


def _hdrs(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VER,
        "Content-Type": "application/json",
    }


def _retries():
    # gentle backoff for rate limits/flakiness
    for s in (0.6, 1.0, 1.6, 2.5, 4.0):
        yield s


@dataclass
class DbSchema:
    id: str
    title_prop: str           # the name of the title property (e.g., "Notes")
    date_prop: str            # the name of the date property (e.g., "Date")
    prop_types: Dict[str, str]  # name -> type (number|date|title|formula|select|...)
    # convenience flags
    has_prop: Dict[str, bool]


class Notion:
    """
    Small helper around Notion API for this use-case:
      - discover database schema
      - find page by exact date
      - upsert a page with numeric/text props
    """

    def __init__(self, token: str, timeout: float = 20.0):
        self.token = token
        self.timeout = timeout

    def _client(self) -> httpx.Client:
        return httpx.Client(headers=_hdrs(self.token), timeout=self.timeout)

    # ---------------- schema ----------------

    def get_database(self, database_id: str) -> Dict[str, Any]:
        for delay in (0.0, *_retries()):
            if delay:
                time.sleep(delay)
            with self._client() as c:
                r = c.get(f"{NOTION_API}/databases/{database_id}")
                if r.status_code == 200:
                    return r.json()
                if r.status_code in TRANSIENT:
                    continue
                raise NotionError(f"database fetch failed {r.status_code}: {r.text}")
        raise NotionError("database fetch gave up after retries")

    def discover_schema(self, database_id: str) -> DbSchema:
        data = self.get_database(database_id)
        props = data.get("properties", {}) or {}

        # Title property: find the one with type == "title"
        title_prop = os.getenv("NOTION_TITLE_PROP")
        if not title_prop:
            title_prop = next(
                (name for name, meta in props.items() if meta.get("type") == "title"),
                None,
            )
        if not title_prop or title_prop not in props or props[title_prop]["type"] != "title":
            raise NotionError(
                "Could not locate a title property. "
                "Make sure your DB has a title column (e.g., 'Notes')."
            )

        # Date property: env override or guess a 'date' typed prop (prefer 'Date')
        date_prop = os.getenv("NOTION_DATE_PROP", "Date")
        if date_prop not in props or props[date_prop].get("type") != "date":
            # fall back to any date prop
            fallback = next(
                (name for name, meta in props.items() if meta.get("type") == "date"),
                None,
            )
            if not fallback:
                raise NotionError("No date property found in database.")
            date_prop = fallback

        prop_types = {name: meta.get("type") for name, meta in props.items()}
        return DbSchema(
            id=data["id"],
            title_prop=title_prop,
            date_prop=date_prop,
            prop_types=prop_types,
            has_prop={name: True for name in props.keys()},
        )

    # ---------------- queries ----------------

    def find_page_by_date(self, database_id: str, date_prop: str, iso_date: str) -> Optional[str]:
        """Return page id whose {date_prop} equals iso_date (YYYY-MM-DD), or None."""
        payload = {"filter": {"property": date_prop, "date": {"equals": iso_date}}, "page_size": 1}
        for delay in (0.0, *_retries()):
            if delay:
                time.sleep(delay)
            with self._client() as c:
                r = c.post(f"{NOTION_API}/databases/{database_id}/query", json=payload)
                if r.status_code == 200:
                    results = r.json().get("results", [])
                    return results[0]["id"] if results else None
                if r.status_code in TRANSIENT:
                    continue
                raise NotionError(f"query failed {r.status_code}: {r.text}")
        raise NotionError("query gave up after retries")

    # ---------------- upsert ----------------

    def upsert_page(
        self,
        database_id: str,
        schema: DbSchema,
        iso_date: str,
        values: Dict[str, Any],
    ) -> str:
        """
        values: NOTION property-name → native Python value
        Rules:
          - We always set the date property and the title property.
          - We only set properties that exist.
          - We never try to write to 'formula' properties (Notion rejects that).
          - For numbers, we coerce to float; for select (if used), we write by name.
        """
        # Build Notion property payload safely
        props_out: Dict[str, Any] = {}

        # Date
        props_out[schema.date_prop] = {"date": {"start": iso_date}}

        # Title
        title_text = str(values.get(schema.title_prop) or values.get("Notes") or "")
        props_out[schema.title_prop] = {"title": [{"type": "text", "text": {"content": title_text}}]}

        # Other fields: only if present in DB and not formula
        for name, val in values.items():
            if name in (schema.date_prop, schema.title_prop):
                continue
            if not schema.has_prop.get(name):
                continue
            ptype = schema.prop_types.get(name)

            if ptype == "formula":
                # computed in Notion; skip
                continue
            elif ptype == "number":
                if val is None:
                    continue
                try:
                    props_out[name] = {"number": float(val)}
                except Exception:
                    continue
            elif ptype == "select":
                if val:
                    props_out[name] = {"select": {"name": str(val)}}
            elif ptype == "rich_text":
                if val:
                    props_out[name] = {"rich_text": [{"type": "text", "text": {"content": str(val)}}]}
            elif ptype == "title":
                # already handled via schema.title_prop
                pass
            elif ptype == "date":
                # if someone passes a full date for another date prop
                if val:
                    props_out[name] = {"date": {"start": str(val)}}
            else:
                # Ignore unsupported types for this workflow
                pass

        # Upsert
        page_id = self.find_page_by_date(database_id, schema.date_prop, iso_date)

        for delay in (0.0, *_retries()):
            if delay:
                time.sleep(delay)
            with self._client() as c:
                if page_id:
                    r = c.patch(f"{NOTION_API}/pages/{page_id}", json={"properties": props_out})
                else:
                    r = c.post(
                        f"{NOTION_API}/pages",
                        json={"parent": {"database_id": database_id}, "properties": props_out},
                    )
                if r.status_code in (200, 201):
                    return page_id or r.json()["id"]
                if r.status_code in TRANSIENT:
                    continue
                raise NotionError(f"upsert failed {r.status_code}: {r.text}")
        raise NotionError("upsert gave up after retries")
