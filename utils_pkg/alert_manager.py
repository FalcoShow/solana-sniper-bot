# utils_pkg/alert_manager.py
from __future__ import annotations
import os, json, time, httpx
from typing import Optional, Dict, Any
from logging_setup import get_logger

log = get_logger()

def _b(name: str, default: bool=False) -> bool:
    v = os.getenv(name)
    return default if v is None else str(v).strip().lower() in ("1","true","t","yes","y","on")

def _s(name: str, default: str="") -> str:
    v = os.getenv(name)
    return default if v is None else v.strip()

class AlertManager:
    def __init__(self) -> None:
        # master switches
        self.enabled = _b("ALERTS_ENABLED", True)
        self.tg_enabled = _b("ALERTS_TELEGRAM_ENABLED", True)
        self.notion_enabled = _b("ALERTS_NOTION_ENABLED", False)
        self.debug = _b("PF_DEBUG_ALERTS", True)

        # Telegram
        self.tg_token = _s("TELEGRAM_BOT_TOKEN")
        self.tg_chat  = _s("TELEGRAM_CHAT_ID")

        # Notion
        self.notion_token = _s("NOTION_API_KEY")
        self.notion_db    = _s("NOTION_DATABASE_ID")
        self.notion_version = _s("NOTION_VERSION", "2022-06-28")

        if self.debug:
            log.info("ALERTS_READY", extra={
                "enabled": self.enabled,
                "tg": self.tg_enabled and bool(self.tg_token and self.tg_chat),
                "notion": self.notion_enabled and bool(self.notion_token and self.notion_db),
            })

    async def _send_tg(self, text: str) -> None:
        if not (self.enabled and self.tg_enabled and self.tg_token and self.tg_chat):
            return
        url = f"https://api.telegram.org/bot{self.tg_token}/sendMessage"
        data = {"chat_id": self.tg_chat, "text": text, "disable_web_page_preview": True}
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.post(url, data=data)
            if r.status_code != 200:
                log.warning("ALERT_TG_SEND_FAIL", extra={"status": r.status_code, "body": r.text[:300]})
            elif self.debug:
                log.info("ALERT_TG_SEND_OK")

    async def _send_notion_trade(self, props: Dict[str, Any]) -> None:
        if not (self.enabled and self.notion_enabled and self.notion_token and self.notion_db):
            return
        url = "https://api.notion.com/v1/pages"
        body = {
            "parent": {"database_id": self.notion_db},
            "properties": props,
        }
        headers = {
            "Authorization": f"Bearer {self.notion_token}",
            "Notion-Version": self.notion_version,
            "Content-Type": "application/json",
        }
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.post(url, headers=headers, content=json.dumps(body))
            if r.status_code >= 300:
                log.warning("ALERT_NOTION_FAIL", extra={"status": r.status_code, "body": r.text[:300]})
            elif self.debug:
                log.info("ALERT_NOTION_OK")

    # public helpers
    async def notify_text(self, text: str) -> None:
        await self._send_tg(text)

    async def notify_trade_fill(self, *, side: str, mint: str, symbol: Optional[str], pool: str,
                                sig: str, spend_sol: Optional[float], out_amount: Optional[int]) -> None:
        # Telegram
        human = (
            f"✅ {side} FILLED\n"
            f"pool={pool}  symbol={symbol or '-'}\n"
            f"mint={mint}\n"
            f"spendSOL={spend_sol if spend_sol is not None else '-'}  outRaw={out_amount if out_amount is not None else '-'}\n"
            f"tx={sig}"
        )
        await self._send_tg(human)

        # Notion
        props = {
            "Name": {"title": [{"text": {"content": f"{side} {symbol or mint[:4]}…"}}]},
            "Side": {"select": {"name": side.upper()}},
            "Pool": {"select": {"name": pool}},
            "Mint": {"rich_text": [{"text": {"content": mint}}]},
            "Tx":   {"url": f"https://solscan.io/tx/{sig}"},
        }
        if spend_sol is not None:
            props["Spend SOL"] = {"number": float(spend_sol)}
        if out_amount is not None:
            props["Out Raw"] = {"number": float(out_amount)}
        await self._send_notion_trade(props)
