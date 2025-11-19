# utils_pkg/alert_manager.py
from __future__ import annotations

import os
import json
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Mapping

import httpx

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
    return _b("DRYRUN", False) or _b("DRY_RUN", False) or _b("IS_DRYRUN", False)


# ───────────────────────────────────────────────────────────────────────────────
# MOMO constants
# ───────────────────────────────────────────────────────────────────────────────

# Events we explicitly allow to reach Telegram/Discord
GOOD_MOMO_EVENTS = {
    "MOMO_OK",
    "MOMO_TAKE",
    "MOMO_LONG",
    "MOMO_SCALP",
    # add more "positive" events here if/when you introduce them
}

# Optional: friendly labels for a few common ones
MOMO_LABELS: Dict[str, str] = {
    "MOMO_OK": "Momentum OK",
    "MOMO_TAKE": "Momentum TAKE",
    "MOMO_LONG": "Momentum LONG",
    "MOMO_SCALP": "Momentum SCALP",
}


# ───────────────────────────────────────────────────────────────────────────────
# AlertManager
# ───────────────────────────────────────────────────────────────────────────────

class AlertManager:
    """
    Single place to fan out alerts to Telegram, Discord, and Notion.

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
        self.tg_parse_mode = _s("TELEGRAM_PARSE_MODE", "")  # e.g. "HTML" if you want formatting

        # Discord (optional, via simple webhook)
        self.discord_enabled = _b("ALERTS_DISCORD_ENABLED", False)
        self.discord_webhook = _s("DISCORD_WEBHOOK_URL")

        # Notion config (separate DBs for PnL and Trades; we use TRADES here)
        # Uses NOTION_API_KEY / NOTION_TRADES_DB_ID so it does not conflict
        # with any other Notion usage for daily PnL, etc.
        self.notion_token     = _s("NOTION_API_KEY")
        self.notion_trades_db = _s("NOTION_TRADES_DB_ID")

        if self.debug:
            log.info(
                "ALERTS_READY",
                extra={
                    "enabled": self.enabled,
                    "tg": self._tg_ok(),
                    "discord": self._discord_ok(),
                    "notion": self._notion_ok(),
                    "dryrun": _dryrun_env_on(),
                },
            )

    # ────────────────────────────────────────────────────────────────────
    # Internal capability checks
    # ────────────────────────────────────────────────────────────────────

    def _tg_ok(self) -> bool:
        return (
            self.enabled
            and self.tg_enabled
            and bool(self.tg_token and self.tg_chat)
        )

    def _discord_ok(self) -> bool:
        return (
            self.enabled
            and self.discord_enabled
            and bool(self.discord_webhook)
        )

    def _notion_ok(self) -> bool:
        return (
            self.enabled
            and self.notion_enabled
            and bool(self.notion_token and self.notion_trades_db)
        )

    # ────────────────────────────────────────────────────────────────────
    # Low-level HTTP helpers
    # ────────────────────────────────────────────────────────────────────

    async def _http_post_json(
        self,
        url: str,
        *,
        json_body: Any | None = None,
        data: Any | None = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: float = 15.0,
    ) -> Optional[httpx.Response]:
        """
        Small wrapper to reduce duplication and give consistent logging.
        """
        try:
            async with httpx.AsyncClient(timeout=timeout) as c:
                if json_body is not None:
                    r = await c.post(url, headers=headers, json=json_body)
                else:
                    r = await c.post(url, headers=headers, data=data)
            return r
        except Exception as e:
            if self.debug:
                log.warning(
                    "ALERT_HTTP_POST_EXC",
                    extra={"url": url, "err": str(e)[:300]},
                )
            return None

    # ────────────────────────────────────────────────────────────────────
    # Telegram
    # ────────────────────────────────────────────────────────────────────

    async def _send_tg(self, text: str) -> None:
        if not self._tg_ok():
            return

        url = f"https://api.telegram.org/bot{self.tg_token}/sendMessage"
        payload: Dict[str, Any] = {
            "chat_id": self.tg_chat,
            "text": text,
            "disable_web_page_preview": True,
        }
        if self.tg_parse_mode:
            payload["parse_mode"] = self.tg_parse_mode

        r = await self._http_post_json(url, data=payload, timeout=10.0)
        if r is None:
            return

        if r.status_code != 200:
            log.warning(
                "ALERT_TG_SEND_FAIL",
                extra={"status": r.status_code, "body": r.text[:400]},
            )
        elif self.debug:
            log.info("ALERT_TG_SEND_OK")

    # ────────────────────────────────────────────────────────────────────
    # Discord (optional)
    # ────────────────────────────────────────────────────────────────────

    async def _send_discord(self, text: str) -> None:
        if not self._discord_ok():
            return

        payload = {"content": text}

        r = await self._http_post_json(
            self.discord_webhook,
            json_body=payload,
            timeout=10.0,
        )
        if r is None:
            return

        if r.status_code >= 300:
            log.warning(
                "ALERT_DISCORD_SEND_FAIL",
                extra={"status": r.status_code, "body": r.text[:400]},
            )
        elif self.debug:
            log.info("ALERT_DISCORD_SEND_OK")

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
        body = {
            "parent": {"database_id": self.notion_trades_db},
            "properties": props,
        }

        r = await self._http_post_json(
            "https://api.notion.com/v1/pages",
            headers=headers,
            json_body=body,
            timeout=15.0,
        )
        if r is None:
            return

        if r.status_code >= 300:
            log.warning(
                "ALERT_NOTION_FAIL",
                extra={"status": r.status_code, "body": r.text[:600]},
            )
        elif self.debug:
            log.info("ALERT_NOTION_OK")

    # ────────────────────────────────────────────────────────────────────
    # Public helpers: generic text / errors
    # ────────────────────────────────────────────────────────────────────

    async def notify_text(self, text: str) -> None:
        """
        Plain text ping (Telegram + Discord, if enabled).
        """
        await self._send_tg(text)
        await self._send_discord(text)

    async def notify_error(
        self,
        label: str,
        err: Exception | str,
        extra: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """
        Convenience helper for error alerts with some context.
        """
        msg_lines = [f"⚠️ {label}"]
        msg_lines.append(f"error={err}")
        if extra:
            for k, v in extra.items():
                msg_lines.append(f"{k}={v}")
        text = "\n".join(msg_lines)

        await self._send_tg(text)
        await self._send_discord(text)

    # ────────────────────────────────────────────────────────────────────
    # Public helpers: trade fill -> TG + Notion
    # ────────────────────────────────────────────────────────────────────

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
        Fan out a trade fill to Telegram, Discord, and Notion (Trades DB).
        """

        # ── Normalize tx URL
        tx_url = None
        if sig:
            tx_url = sig if sig.startswith("http") else f"https://solscan.io/tx/{sig}"

        side_upper = str(side or "").strip().upper() or "BUY"
        side_select = "Buy" if side_upper.startswith("B") else "Sell"

        human = (
            f"✅ {side_upper} FILLED\n"
            f"pool={pool}  symbol={symbol or '-'}\n"
            f"mint={mint}\n"
            f"spendSOL={spend_sol if spend_sol is not None else '-'}  "
            f"outRaw={out_amount if out_amount is not None else '-'}\n"
            f"tx={(tx_url or '—')}"
        )

        # Telegram + Discord notifications
        await self._send_tg(human)
        await self._send_discord(human)

        # ── Notion row (Sniper Trades schema)
        now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")

        props: Dict[str, Any] = {
            "Name": {
                "title": [
                    {
                        "text": {
                            "content": f"{side_select} {symbol or mint[:6]}…"
                        }
                    }
                ]
            },
            "Side": {"select": {"name": side_select}},
            "Symbol": {
                "rich_text": [{"text": {"content": symbol or ""}}]
            },
            "Pool": {
                "rich_text": [{"text": {"content": pool}}]
            },
            "Mint": {
                "rich_text": [{"text": {"content": mint}}]
            },
            "Time": {"date": {"start": now_iso}},
        }
        if spend_sol is not None:
            props["SpendSOL"] = {"number": float(spend_sol)}
        if out_amount is not None:
            props["OutRaw"] = {"number": float(out_amount)}
        if tx_url:
            props["TxSig"] = {"url": tx_url}

        # Allow Notion even in DRYRUN if ALERTS_NOTION_ALLOW_DRYRUN=1
        if self.notion_enabled and (
            self.allow_dryrun_notion or not _dryrun_env_on()
        ):
            await self._notion_insert_trade(props)
        else:
            if self.debug and self.notion_enabled:
                log.info(
                    "ALERT_NOTION_SKIPPED",
                    extra={
                        "dryrun": _dryrun_env_on(),
                        "allow_dryrun": self.allow_dryrun_notion,
                    },
                )

    # ────────────────────────────────────────────────────────────────────
    # Public helpers: MOMO events (scanner signals)
    # ────────────────────────────────────────────────────────────────────

    async def send_momo_event(self, event: str, ctx: Dict[str, Any]) -> None:
        """
        Central MOMO notifier used by scanners.

        Behaviour:
          - Completely ignore skip events (MOMO_SKIP_*)
          - Only allow an explicit whitelist of "good" events (GOOD_MOMO_EVENTS)
        """

        event = str(event or "").strip().upper()
        if not event:
            return

        # 1) Ignore everything that is clearly a skip (these caused the spam)
        if event.startswith("MOMO_SKIP"):
            if self.debug:
                log.debug("Ignoring MOMO skip event: %s", event)
            return

        # 2) Only allow whitelisted "good" events
        if event not in GOOD_MOMO_EVENTS:
            if self.debug:
                log.debug("Ignoring non-whitelisted MOMO event: %s", event)
            return

        symbol = (
            ctx.get("symbol")
            or ctx.get("mint")
            or ctx.get("name")
            or "?"
        )
        pool   = ctx.get("pool") or ctx.get("pair") or "-"
        reason = ctx.get("reason") or ""

        label = MOMO_LABELS.get(event, event)
        header = f"📈 {label}"
        body_lines = [header, f"symbol={symbol}", f"pool={pool}"]

        # Add a few common optional fields if present
        if "age_sec" in ctx:
            body_lines.append(f"age={ctx['age_sec']}s")
        if "liq_usd" in ctx:
            body_lines.append(f"liq≈${ctx['liq_usd']}")
        if "vol_5m" in ctx:
            body_lines.append(f"vol_5m≈${ctx['vol_5m']}")
        if "up_pct" in ctx:
            body_lines.append(f"up={ctx['up_pct']}%")
        if "fraud_score" in ctx:
            body_lines.append(f"fraudScore={ctx['fraud_score']}")
        if reason:
            body_lines.append(f"reason={reason}")

        # Optional: tx link if present in ctx
        sig = ctx.get("signature") or ctx.get("tx") or ctx.get("tx_sig")
        if isinstance(sig, str) and sig:
            tx_url = sig if sig.startswith("http") else f"https://solscan.io/tx/{sig}"
            body_lines.append(f"tx={tx_url}")

        text = "\n".join(body_lines)

        await self._send_tg(text)
        await self._send_discord(text)
