# utils_pkg/alert_manager.py
from __future__ import annotations

import os
import json
import httpx
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from logging_setup import get_logger

log = get_logger()

# ───────────────────────────────────────────────────────────────────────────────
# Env helpers
# ───────────────────────────────────────────────────────────────────────────────

def _b(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "t", "yes", "y", "on")

def _s(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else v.strip()

def _dryrun_env_on() -> bool:
    return _b("DRYRUN", False)

# ───────────────────────────────────────────────────────────────────────────────
# AlertManager
# ───────────────────────────────────────────────────────────────────────────────

class AlertManager:
    """
    Single place to fan out alerts to Telegram and Notion.

    Notion "trade fill" rows target a DB with the following properties:
      - Name       (title)
      - Side       (select: "Buy" or "Sell")
      - Symbol     (rich_text)
      - Pool       (rich_text)
      - Mint       (rich_text)
      - SpendSOL   (number)
      - OutRaw     (number)
      - TxSig      (url)
      - Time       (date)
    """

    # Notion API version (stable)
    _NOTION_VERSION = _s("NOTION_VERSION", "2022-06-28")

    def __init__(self) -> None:
        # Master switches
        self.enabled        = _b("ALERTS_ENABLED", True)
        self.tg_enabled     = _b("ALERTS_TELEGRAM_ENABLED", True)
        self.notion_enabled = _b("ALERTS_NOTION_ENABLED", False)
        self.debug          = _b("PF_DEBUG_ALERTS", True)

        # Allow Notion inserts even when DRYRUN=1 (recommended default ON)
        self.allow_dryrun_notion = _b("ALERTS_NOTION_ALLOW_DRYRUN", True)

        # Telegram config
        self.tg_token = _s("TELEGRAM_BOT_TOKEN")
        self.tg_chat  = _s("TELEGRAM_CHAT_ID")

        # Notion config (separate DBs for PnL and Trades; we use TRADES here)
        self.notion_token   = _s("NOTION_API_KEY")
        self.notion_trades_db = _s("NOTION_TRADES_DB_ID")  # <- your Sniper Trades DB

        if self.debug:
            log.info(
                "ALERTS_READY",
                extra={
                    "enabled": self.enabled,
                    "tg": self.tg_enabled and bool(self.tg_token and self.tg_chat),
                    "notion": self.notion_enabled and bool(self.notion_token and self.notion_trades_db),
                },
            )

    # ────────────────────────────────────────────────────────────────────
    # Internal capability checks
    # ────────────────────────────────────────────────────────────────────

    def _tg_ok(self) -> bool:
        return self.enabled and self.tg_enabled and bool(self.tg_token and self.tg_chat)

    def _notion_ok(self) -> bool:
        return self.enabled and self.notion_enabled and bool(self.notion_token and self.notion_trades_db)

    # ────────────────────────────────────────────────────────────────────
    # Telegram
    # ────────────────────────────────────────────────────────────────────

    async def _send_tg(self, text: str) -> None:
        if not self._tg_ok():
            return
        url = f"https://api.telegram.org/bot{self.tg_token}/sendMessage"
        data = {"chat_id": self.tg_chat, "text": text, "disable_web_page_preview": True}
        try:
            async with httpx.AsyncClient(timeout=10) as c:
                r = await c.post(url, data=data)
            if r.status_code != 200:
                log.warning(
                    "ALERT_TG_SEND_FAIL",
                    extra={"status": r.status_code, "body": r.text[:400]},
                )
            elif self.debug:
                log.info("ALERT_TG_SEND_OK")
        except Exception as e:
            log.warning("ALERT_TG_SEND_EXC", extra={"err": str(e)})

    # ────────────────────────────────────────────────────────────────────
    # Notion (Trades DB — per-fill rows)
    # ────────────────────────────────────────────────────────────────────

    async def _notion_insert_trade(self, props: Dict[str, Any]) -> None:
        """
        Insert a row into NOTION_TRADES_DB_ID. Respects ALERTS_NOTION_ALLOW_DRYRUN:
        if True, writes even when DRYRUN=1.
        """
        if not self._notion_ok():
            return

        # DRYRUN gate (allowed if flag is True)
        if _dryrun_env_on() and not self.allow_dryrun_notion:
            if self.debug:
                log.info("ALERT_NOTION_SKIP_DRYRUN")
            return

        headers = {
            "Authorization": f"Bearer {self.notion_token}",
            "Notion-Version": self._NOTION_VERSION,
            "Content-Type": "application/json",
        }
        body = {"parent": {"database_id": self.notion_trades_db}, "properties": props}

        try:
            async with httpx.AsyncClient(timeout=15) as c:
                r = await c.post("https://api.notion.com/v1/pages", headers=headers, content=json.dumps(body))
            if r.status_code >= 300:
                log.warning(
                    "ALERT_NOTION_FAIL",
                    extra={"status": r.status_code, "body": r.text[:600]},
                )
            elif self.debug:
                log.info("ALERT_NOTION_OK")
        except Exception as e:
            log.warning("ALERT_NOTION_EXC", extra={"err": str(e)})

    # ────────────────────────────────────────────────────────────────────
    # Public helpers
    # ────────────────────────────────────────────────────────────────────

    async def notify_text(self, text: str) -> None:
        """Plain text ping (Telegram only)."""
        await self._send_tg(text)

    async def notify_trade_fill(
        self,
        *,
        side: str,                         # "BUY" or "SELL" (case-insensitive)
        mint: str,
        symbol: Optional[str],
        pool: str,
        sig: Optional[str],                # tx hash or full URL (we'll normalize)
        spend_sol: Optional[float],
        out_amount: Optional[int],
    ) -> None:
        """
        Fan out a trade fill to Telegram and Notion (Trades DB).
        """

        # ── Telegram message
        # Normalize tx URL
        tx_url = None
        if sig:
            tx_url = sig if sig.startswith("http") else f"https://solscan.io/tx/{sig}"

        human = (
            f"✅ {side.upper()} FILLED\n"
            f"pool={pool}  symbol={symbol or '-'}\n"
            f"mint={mint}\n"
            f"spendSOL={spend_sol if spend_sol is not None else '-'}  "
            f"outRaw={out_amount if out_amount is not None else '-'}\n"
            f"tx={(tx_url or '—')}"
        )
        await self._send_tg(human)

        # ── Notion row (Sniper Trades schema)
        side_select = "Buy" if str(side).strip().lower().startswith("b") else "Sell"
        now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")

        props: Dict[str, Any] = {
            "Name":     {"title": [{"text": {"content": f"{side_select} {symbol or mint[:6]}…"}}]},
            "Side":     {"select": {"name": side_select}},
            "Symbol":   {"rich_text": [{"text": {"content": symbol or ""}}]},
            "Pool":     {"rich_text": [{"text": {"content": pool}}]},
            "Mint":     {"rich_text": [{"text": {"content": mint}}]},
            "Time":     {"date": {"start": now_iso}},
        }
        if spend_sol is not None:
            props["SpendSOL"] = {"number": float(spend_sol)}
        if out_amount is not None:
            props["OutRaw"] = {"number": float(out_amount)}
        if tx_url:
            props["TxSig"] = {"url": tx_url}

        # Allow Notion even in DRYRUN if ALERTS_NOTION_ALLOW_DRYRUN=1
        if self.notion_enabled and (self.allow_dryrun_notion or not _dryrun_env_on()):
            await self._notion_insert_trade(props)
        else:
            if self.debug:
                log.info("ALERT_NOTION_SKIPPED", extra={"dryrun": _dryrun_env_on(), "allow_dryrun": self.allow_dryrun_notion})
