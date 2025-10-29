# utils_pkg/notion_client.py
from __future__ import annotations
import os, time, math
from typing import Any, Dict, Optional, Tuple, List
import httpx

NOTION_API = "https://api.notion.com/v1"
NOTION_VER = "2022-06-28"

class NotionError(RuntimeError): ...
def _hdrs(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VER,
        "Content-Type": "application/json",
    }

def _retry_delays():
    # gentle backoff for rate limits or flakiness
    for s in (0.6, 1.0, 1.6, 2.5):
        yield s

class Notion:
    def __init__(self, token: str, timeout: float = 15.0):
        self.token = token
        self.timeout = timeout

    def _client(self) -> httpx.Client:
        return httpx.Client(headers=_hdrs(self.token), timeout=self.timeout)

    def find_page_by_date(self, database_id: str, iso_date: str) -> Optional[str]:
        """Query database for a page with Date == iso_date (YYYY-MM-DD)."""
        q = {
            "filter": {
                "property": "Date",
                "date": {"equals": iso_date}
            },
            "page_size": 1,
        }
        for delay in [0.0, *list(_retry_delays())]:
            if delay: time.sleep(delay)
            with self._client() as c:
                r = c.post(f"{NOTION_API}/databases/{database_id}/query", json=q)
                if r.status_code == 200:
                    results = r.json().get("results", [])
                    if results:
                        return results[0]["id"]
                    return None
                if r.status_code in (409, 429, 500, 502, 503):
                    continue
                raise NotionError(f"query failed {r.status_code}: {r.text}")
        raise NotionError("query gave up after retries")

    def upsert_daily_pnl(
        self,
        database_id: str,
        iso_date: str,
        props: Dict[str, Any],
    ) -> str:
        """
        props keys may include: Realized SOL, Unrealized SOL, Open Positions,
        Wallet SOL, Trades, Realized USD, Unrealized USD, Net SOL, Notes, Day Outcome
        """
        page_id = self.find_page_by_date(database_id, iso_date)

        notion_props = {
            "Date": {"date": {"start": iso_date}},
        }
        # Map numeric props
        num_fields = [
            "Realized SOL","Unrealized SOL","Open Positions","Wallet SOL",
            "Trades","Realized USD","Unrealized USD","Net SOL"
        ]
        for k in num_fields:
            if k in props and props[k] is not None:
                notion_props[k] = {"number": float(props[k])}

        # Select + Rich text
        if "Day Outcome" in props and props["Day Outcome"]:
            notion_props["Day Outcome"] = {"select": {"name": str(props["Day Outcome"])}}
        if "Notes" in props and props["Notes"]:
            notion_props["Notes"] = {"rich_text": [{"type":"text","text":{"content": str(props["Notes"])}}]}

        payload = {"parent": {"database_id": database_id}, "properties": notion_props}

        for delay in [0.0, *list(_retry_delays())]:
            if delay: time.sleep(delay)
            with self._client() as c:
                if page_id:
                    r = c.patch(f"{NOTION_API}/pages/{page_id}", json={"properties": notion_props})
                else:
                    r = c.post(f"{NOTION_API}/pages", json=payload)
                if r.status_code in (200, 201):
                    return (page_id or r.json()["id"])
                if r.status_code in (409, 429, 500, 502, 503):
                    continue
                raise NotionError(f"upsert failed {r.status_code}: {r.text}")
        raise NotionError("upsert gave up after retries")
