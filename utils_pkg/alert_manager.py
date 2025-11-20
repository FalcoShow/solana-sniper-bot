from __future__ import annotations

"""
AlertManager
------------

Single place to fan out alerts to:

  • Telegram
  • Discord (webhook)
  • Notion (Trades DB)
  • Momentum (MOMO_*) scanner events with Redis-based cooldown

Public methods used by the rest of the bot:

  - AlertManager.notify_text(text)
  - AlertManager.notify_error(label, err, extra=None)
  - AlertManager.notify_trade_fill(...)
  - AlertManager.send_momo_event(event, ctx)

MOMO behaviour:

  - Only whitelisted events are allowed (GOOD_MOMO_EVENTS).
  - All MOMO_SKIP_* events are ignored.
  - Optional Redis cooldown per (event, symbol) controlled by
    MOMENTUM_ALERT_COOLDOWN_SEC (seconds; default 300).
"""

import os
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Mapping

import httpx

from logging_setup import get_logger

log = get_logger()

# Optional Redis (for MOMO cooldowns)
try:  # pragma: no cover - best effort
    import redis.asyncio as aioredis  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    aioredis = None  # type: ignore[assignment]


# ──────────────────────────────────────────────────────────────────────────────
# Env helpers
# ──────────────────────────────────────────────────────────────────────────────


def _b(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "t", "yes", "y", "on")


def _s(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else v.strip()


def _i(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except Exception:
        return default


def _dryrun_env_on() -> bool:
    return _b("DRYRUN", False) or _b("DRY_RUN", False) or _b("IS_DRYRUN", False)


# ──────────────────────────────────────────────────────────────────────────────
# MOMO constants & config
# ──────────────────────────────────────────────────────────────────────────────

# Events we explicitly allow to reach Telegram/Discord
GOOD_MOMO_EVENTS = {
    "MOMO_OK",
    "MOMO_TAKE",
    "MOMO_LONG",
    "MOMO_SCALP",
    # add more "positive" events here if/when you introduce them
}

# Optional: friendly labels for common MOMO events
MOMO_LABELS: Dict[str, str] = {
    "MOMO_OK": "Momentum OK",
    "MOMO_TAKE": "Momentum TAKE",
    "MOMO_LONG": "Momentum LONG",
    "MOMO_SCALP": "Momentum SCALP",
}

# Redis cooldown configuration (per (event, symbol) pair)
MOMO_REDIS_PREFIX = _s("MOMENTUM_ALERT_REDIS_PREFIX", "momo:alert")
MOMO_COOLDOWN_SEC = _i("MOMENTUM_ALERT_COOLDOWN_SEC", 300)  # 5 minutes default
REDIS_URL = _s("REDIS_URL", "")


# ──────────────────────────────────────────────────────────────────────────────
# AlertManager
# ──────────────────────────────────────────────────────────────────────────────


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
        self.enabled = _b("ALERTS_ENABLED", True)
        self.tg_enabled = _b("ALERTS_TELEGRAM_ENABLED", True)
        self.notion_enabled = _b("ALERTS_NOTION_ENABLED", False)
        self.debug = _b("PF_DEBUG_ALERTS", True)

        # Allow Notion inserts even when DRYRUN=1 (recommended default ON)
        self.allow_dryrun_notion = _b("ALERTS_NOTION_ALLOW_DRYRUN", True)

        # Telegram config
        self.tg_token = _s("TELEGRAM_BOT_TOKEN")
        self.tg_chat = _s("TELEGRAM_CHAT_ID")
        self.tg_parse_mode = _s("TELEGRAM_PARSE_MODE", "")  # e.g. "HTML"

        # Discord (optional, via simple webhook)
        self.discord_enabled = _b("ALERTS_DISCORD_ENABLED", False)
        # NOTE: older env name was DISCORD_WEBHOOK; keep that as fallback
        self.discord_webhook = _s(
            "DISCORD_WEBHOOK_URL",
            _s("DISCORD_WEBHOOK", ""),
        )

        # Notion config (separate DB for trade fills)
        self.notion_token = _s("NOTION_API_KEY")
        self.notion_trades_db = _s("NOTION_TRADES_DB_ID")

        # MOMO Redis cooldown
        self.momo_cooldown_sec = max(0, MOMO_COOLDOWN_SEC)
        self._redis_url = REDIS_URL
        self._redis = None
        self._momo_memory_cache: Dict[str, float] = {}  # fallback if Redis missing

        if aioredis and self._redis_url:
            try:  # pragma: no cover
                self._redis = aioredis.from_url(
                    self._redis_url,
                    encoding="utf-8",
                    decode_responses=True,
                )
            except Exception as e:  # pragma: no cover
                if self.debug:
                    log.warning(
                        "ALERT_REDIS_INIT_FAIL",
                        extra={"url": self._redis_url, "err": str(e)[:200]},
                    )
                self._redis = None

        if self.debug:
            log.info(
                "ALERTS_READY",
                extra={
                    "enabled": self.enabled,
                    "tg": self._tg_ok(),
                    "discord": self._discord_ok(),
                    "notion": self._notion_ok(),
                    "dryrun": _dryrun_env_on(),
                    "momo_cooldown_sec": self.momo_cooldown_sec,
                    "redis": bool(self._redis),
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
    # Low-level HTTP helper
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
        msg_lines = [f"⚠️ {label}", f"error={err}"]
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
        side: str,  # "BUY" or "SELL" (case-insensitive)
        mint: str,
        symbol: Optional[str],
        pool: str,
        sig: Optional[str],  # tx hash or full URL (we'll normalize)
        spend_sol: Optional[float],
        out_amount: Optional[int],
    ) -> None:
        """
        Fan out a trade fill to Telegram, Discord, and Notion (Trades DB).
        """
        # Normalize tx URL
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

        await self._send_tg(human)
        await self._send_discord(human)

        # Notion row (Sniper Trades schema)
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
                "rich_text": [{"text": {"content": symbol or ""}}],
            },
            "Pool": {
                "rich_text": [{"text": {"content": pool}}],
            },
            "Mint": {
                "rich_text": [{"text": {"content": mint}}],
            },
            "Time": {"date": {"start": now_iso}},
        }
        if spend_sol is not None:
            props["SpendSOL"] = {"number": float(spend_sol)}
        if out_amount is not None:
            props["OutRaw"] = {"number": float(out_amount)}
        if tx_url:
            props["TxSig"] = {"url": tx_url}

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
    # MOMO cooldown helpers
    # ────────────────────────────────────────────────────────────────────

    async def _momo_hit_and_check_cooldown(self, event: str, symbol: str) -> bool:
        """
        Returns True if this (event, symbol) should be suppressed because
        it is still in cooldown; False if it's allowed and the cooldown
        has been (re)started.

        Uses Redis when available; otherwise falls back to an in-memory
        dict (per-process).
        """
        if self.momo_cooldown_sec <= 0:
            return False  # cooldown disabled

        key = f"{MOMO_REDIS_PREFIX}:{event}:{symbol}".lower()

        # Redis path
        if self._redis is not None:  # pragma: no cover - network path
            try:
                exists = await self._redis.exists(key)
                if exists:
                    if self.debug:
                        log.debug(
                            "MOMO_COOLDOWN_HIT",
                            extra={"event": event, "symbol": symbol, "key": key},
                        )
                    return True
                # set key with TTL
                await self._redis.set(key, "1", ex=self.momo_cooldown_sec)
                return False
            except Exception as e:
                if self.debug:
                    log.warning(
                        "MOMO_COOLDOWN_REDIS_FAIL",
                        extra={"key": key, "err": str(e)[:200]},
                    )
                # fall through to memory cache

        # In-memory fallback (per process, approximate)
        import time

        now = time.time()
        t = self._momo_memory_cache.get(key)
        if t is not None and (now - t) < self.momo_cooldown_sec:
            if self.debug:
                log.debug(
                    "MOMO_COOLDOWN_HIT_MEM",
                    extra={"event": event, "symbol": symbol, "key": key},
                )
            return True

        self._momo_memory_cache[key] = now
        return False

    # ────────────────────────────────────────────────────────────────────
    # Public helpers: MOMO events (scanner signals)
    # ────────────────────────────────────────────────────────────────────

    async def send_momo_event(self, event: str, ctx: Dict[str, Any]) -> None:
        """
        Central MOMO notifier used by scanners.

        Behaviour:
          - Completely ignore skip events (MOMO_SKIP_*)
          - Only allow a whitelist of "good" events (GOOD_MOMO_EVENTS)
          - Apply Redis-based cooldown per (event, symbol) so we don't
            spam the same SNAIL 50x in a row.
        """
        event = str(event or "").strip().upper()
        if not event:
            return

        # 1) Ignore everything that is clearly a skip (these caused spam)
        if event.startswith("MOMO_SKIP"):
            if self.debug:
                log.debug("MOMO_SKIP_IGNORED", extra={"event": event})
            return

        # 2) Only allow whitelisted "good" events
        if event not in GOOD_MOMO_EVENTS:
            if self.debug:
                log.debug("MOMO_EVENT_NOT_WHITELISTED", extra={"event": event})
            return

        symbol = (
            ctx.get("symbol")
            or ctx.get("mint")
            or ctx.get("name")
            or "?"
        )
        symbol_str = str(symbol)

        # 3) Cooldown check
        if await self._momo_hit_and_check_cooldown(event, symbol_str):
            # Cooldown hit → silently drop
            return

        pool = ctx.get("pool") or ctx.get("pair") or "-"
        reason = ctx.get("reason") or ""

        label = MOMO_LABELS.get(event, event)
        header = f"📈 {label}"
        body_lines = [header, f"symbol={symbol_str}", f"pool={pool}"]

        # Add a few common optional fields if present
        if "age_sec" in ctx:
            body_lines.append(f"age={ctx['age_sec']}s")
        if "age_min" in ctx:
            body_lines.append(f"age={ctx['age_min']}m")
        if "liq_usd" in ctx:
            body_lines.append(f"liq≈${ctx['liq_usd']}")
        if "vol_5m" in ctx:
            body_lines.append(f"vol_5m≈${ctx['vol_5m']}")
        if "vol_5m_usd" in ctx:
            body_lines.append(f"vol_5m≈${ctx['vol_5m_usd']}")
        if "up_pct" in ctx:
            body_lines.append(f"up={ctx['up_pct']}%")
        if "fraud_score" in ctx:
            body_lines.append(f"fraudScore={ctx['fraud_score']}")
        if "momentum" in ctx:
            body_lines.append(f"momentum={ctx['momentum']}")
        if "final" in ctx:
            body_lines.append(f"final={ctx['final']}")

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
