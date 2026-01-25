# utils_pkg/alert_manager.py
from __future__ import annotations

"""
utils_pkg/alert_manager.py

AlertManager — God-Tier / production-grade (V3+++)
==================================================

Single canonical fanout for alerts:
  • Telegram
  • Discord (webhook)
  • Notion (optional: direct DB row OR via core_utils.log_trade_event)

Hard requirements (LOCK-IN)
---------------------------
- Batched + rate-limited sending (avoid spam / bans / throttling)
- Critical-only mode switch
- MOMO events:
    - only whitelisted positive events allowed
    - MOMO_SKIP_* ignored
    - cooldown per (event, symbol) using core_utils Redis (no competing redis clients)
- Accepts `session=` kwarg (core_utils may instantiate AlertManager(session=session))
- Must NOT create competing global clients. No global aiohttp sessions. No global redis.
- Never crash the bot: all send paths are best-effort; failures are swallowed + logged.

Public async API
----------------
  - await am.notify_text(text, level="INFO", key="", channels=("tg","discord"))
  - await am.notify_error(label, err, extra=None, level="ERROR", key="")
  - await am.notify_trade_fill(...)
  - await am.notify_reject(bucket, ctx)
  - await am.send_momo_event(event, ctx)

Compatibility helpers (used by older code)
------------------------------------------
  - await am.send_buy_alert(...)
  - await am.send_sell_alert(...)
"""

import asyncio
import contextlib
import json
import os
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import aiohttp

# logger (structured-friendly)
try:  # pragma: no cover
    from logging_setup import get_logger  # type: ignore
except Exception:  # pragma: no cover
    import logging

    def get_logger(name: str | None = None):  # type: ignore
        return logging.getLogger(name or "alert_manager")


log = get_logger(__name__)

# metrics bump (optional)
try:  # pragma: no cover
    from logging_setup import bump  # type: ignore
except Exception:  # pragma: no cover

    def bump(*_a: Any, **_k: Any) -> None:  # type: ignore
        return None


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
    return default if v is None else str(v).strip()


def _i(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    try:
        return int(str(v).strip())
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
    s = (s or "").strip()
    return s if len(s) <= n else s[: max(0, n - 1)] + "…"


def _json(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return "{}"


def _parse_csv_env(name: str) -> list[str]:
    raw = _s(name, "")
    if not raw:
        return []
    parts = [p.strip() for p in raw.split(",")]
    return [p for p in parts if p]


# ──────────────────────────────────────────────────────────────────────────────
# Levels
# ──────────────────────────────────────────────────────────────────────────────
_LEVELS: tuple[str, ...] = ("DEBUG", "INFO", "IMPORTANT", "CRITICAL", "ERROR")


def _lvl_norm(x: str) -> str:
    t = (x or "").strip().upper()
    return t if t in _LEVELS else "INFO"


def _lvl_rank(x: str) -> int:
    return _LEVELS.index(_lvl_norm(x))


# ──────────────────────────────────────────────────────────────────────────────
# MOMO config
# ──────────────────────────────────────────────────────────────────────────────
_DEFAULT_GOOD_MOMO = {"MOMO_OK", "MOMO_TAKE", "MOMO_LONG", "MOMO_SCALP"}
GOOD_MOMO_EVENTS = set(_parse_csv_env("MOMO_GOOD_EVENTS")) or _DEFAULT_GOOD_MOMO

MOMO_LABELS: dict[str, str] = {
    "MOMO_OK": "Momentum OK",
    "MOMO_TAKE": "Momentum TAKE",
    "MOMO_LONG": "Momentum LONG",
    "MOMO_SCALP": "Momentum SCALP",
}

MOMO_REDIS_PREFIX = _s("MOMENTUM_ALERT_REDIS_PREFIX", "momo:alert")
MOMO_COOLDOWN_SEC = _i("MOMENTUM_ALERT_COOLDOWN_SEC", 300)


# ──────────────────────────────────────────────────────────────────────────────
# Internal alert item
# ──────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class _AlertItem:
    text: str
    level: str
    channels: tuple[str, ...]
    key: str
    ts: float


# ──────────────────────────────────────────────────────────────────────────────
# AlertManager
# ──────────────────────────────────────────────────────────────────────────────
class AlertManager:
    """
    Batched + rate-limited alert fanout.

    Channels:
      - "tg"      Telegram
      - "discord" Discord webhook

    Notes:
      - This is designed to be instantiated once and reused.
      - It is safe to instantiate multiple times, but avoid it in production.
    """

    # hard-ish platform limits
    _TG_MAX = 3500  # telegram sendMessage: 4096, keep headroom
    _DISCORD_MAX = 1800  # discord content: 2000, keep headroom

    def __init__(self, session: aiohttp.ClientSession | None = None) -> None:
        # master switch
        self.enabled = _b("ALERTS_ENABLED", True)

        # critical-only mode
        self.critical_only = (
            _b("ALERTS_CRITICAL_ONLY", False)
            or _b("ALERT_CRITICAL_ONLY", False)
            or _b("PF_CRITICAL_ONLY", False)
        )
        self.critical_min_level = _lvl_norm(_s("ALERTS_CRITICAL_MIN_LEVEL", "IMPORTANT"))

        # allow alerts during DRY_RUN?
        self.allow_dryrun = _b("ALERTS_ALLOW_DRYRUN", True)

        # channel toggles
        self.tg_enabled = _b("ALERTS_TELEGRAM_ENABLED", True)
        self.discord_enabled = _b("ALERTS_DISCORD_ENABLED", False)

        # batching + queue
        self.batch_enabled = _b("ALERTS_BATCH_ENABLED", True)
        self.flush_sec = max(0.10, _f("ALERTS_FLUSH_SEC", 1.0))
        self.max_per_flush = max(1, _i("ALERTS_MAX_PER_FLUSH", 25))
        self.max_queue = max(100, _i("ALERTS_MAX_QUEUE", 2000))

        # rate limits
        self.tg_min_interval = max(0.0, _f("ALERTS_TG_MIN_INTERVAL_SEC", 0.8))
        self.discord_min_interval = max(0.0, _f("ALERTS_DISCORD_MIN_INTERVAL_SEC", 1.2))

        # dedupe
        self.dedupe_ttl_sec = max(0, _i("ALERTS_DEDUPE_TTL_SEC", 25))

        # http
        self.http_timeout_sec = max(3.0, _f("ALERTS_HTTP_TIMEOUT_SEC", 12.0))

        # MOMO cooldown
        self.momo_cooldown_sec = max(0, MOMO_COOLDOWN_SEC)

        # Telegram config (compat with core_utils)
        self.tg_token = _s("TELEGRAM_BOT_TOKEN")
        self.tg_chat = _s("TELEGRAM_CHAT_ID")
        self.tg_parse_mode = _s("TELEGRAM_PARSE_MODE", "HTML").strip()  # HTML default

        # Discord config
        self.discord_webhook = _s("DISCORD_WEBHOOK_URL", _s("DISCORD_WEBHOOK", ""))

        # Notion optional
        self.notion_enabled = _b("ALERTS_NOTION_ENABLED", False)
        self.allow_dryrun_notion = _b("ALERTS_NOTION_ALLOW_DRYRUN", True)
        self.notion_token = _s("NOTION_TOKEN", _s("NOTION_API_KEY", ""))
        self.notion_trades_db = _s("NOTION_TRADES_DB_ID", "")
        self.notion_version = _s("NOTION_VERSION", "2022-06-28")

        # reject alerts
        self.rejects_enabled = _b("ALERTS_REJECTS_ENABLED", True)
        self.rejects_level = _lvl_norm(_s("ALERTS_REJECTS_LEVEL", "INFO"))

        # lifecycle
        self._external_session = session is not None
        self._session: aiohttp.ClientSession | None = session
        self._queue: asyncio.Queue[_AlertItem] = asyncio.Queue(maxsize=self.max_queue)
        self._worker: asyncio.Task[None] | None = None
        self._closed = False

        # send throttles
        self._last_send: dict[str, float] = {"tg": 0.0, "discord": 0.0}

        # in-memory dedupe (fast path)
        self._dedupe_mem: dict[str, float] = {}
        self._momo_mem: dict[str, float] = {}

        log.info(
            "ALERT_MANAGER_READY",
            extra={
                "enabled": self.enabled,
                "critical_only": self.critical_only,
                "critical_min_level": self.critical_min_level,
                "tg_enabled": int(self._tg_ok()),
                "discord_enabled": int(self._discord_ok()),
                "batch_enabled": self.batch_enabled,
                "flush_sec": self.flush_sec,
                "dryrun": int(_dryrun_on()),
            },
        )

    # ────────────────────────────────────────────────────────────────────
    # lifecycle
    # ────────────────────────────────────────────────────────────────────
    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is not None and not self._session.closed:
            return self._session
        # create only if not provided
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.http_timeout_sec)
        )
        return self._session

    async def _ensure_worker(self) -> None:
        if self._worker is not None and not self._worker.done():
            return
        self._worker = asyncio.create_task(self._flush_loop(), name="alert_manager.flush_loop")

    async def close(self) -> None:
        self._closed = True
        if self._worker is not None:
            self._worker.cancel()
            with contextlib.suppress(Exception):
                await self._worker
        self._worker = None

        if not self._external_session and self._session is not None:
            with contextlib.suppress(Exception):
                await self._session.close()
        self._session = None

    # ────────────────────────────────────────────────────────────────────
    # gates
    # ────────────────────────────────────────────────────────────────────
    def _allowed_by_level(self, level: str) -> bool:
        if not self.critical_only:
            return True
        return _lvl_rank(level) >= _lvl_rank(self.critical_min_level)

    def _allowed_in_dryrun(self) -> bool:
        return self.allow_dryrun or (not _dryrun_on())

    def _tg_ok(self) -> bool:
        return bool(self.enabled and self.tg_enabled and self.tg_token and self.tg_chat)

    def _discord_ok(self) -> bool:
        return bool(self.enabled and self.discord_enabled and self.discord_webhook)

    def _notion_ok(self) -> bool:
        return bool(
            self.enabled and self.notion_enabled and self.notion_token and self.notion_trades_db
        )

    # ────────────────────────────────────────────────────────────────────
    # dedupe (in-memory)
    # ────────────────────────────────────────────────────────────────────
    def _dedupe_hit(self, key: str) -> bool:
        if self.dedupe_ttl_sec <= 0:
            return False
        k = (key or "").strip()
        if not k:
            return False
        now = time.time()
        t = self._dedupe_mem.get(k)
        if t is not None and (now - t) < float(self.dedupe_ttl_sec):
            return True
        self._dedupe_mem[k] = now

        # occasional prune to bound memory
        if len(self._dedupe_mem) > 8000:
            cutoff = now - float(self.dedupe_ttl_sec) * 3.0
            self._dedupe_mem = {kk: vv for kk, vv in self._dedupe_mem.items() if vv >= cutoff}
        return False

    # ────────────────────────────────────────────────────────────────────
    # enqueue
    # ────────────────────────────────────────────────────────────────────
    async def _enqueue(self, item: _AlertItem) -> None:
        if self._closed or not self.enabled:
            return
        if not self._allowed_in_dryrun():
            return

        level = _lvl_norm(item.level)
        if not self._allowed_by_level(level):
            return

        key = (item.key or "").strip()
        if key and self._dedupe_hit(key):
            return

        if not self.batch_enabled:
            await self._send_item_immediate(item)
            return

        await self._ensure_worker()

        # Backpressure / drop policy:
        # - if queue is full: drop DEBUG/INFO first
        # - keep CRITICAL/ERROR by evicting one older low-priority item if possible
        try:
            self._queue.put_nowait(_AlertItem(item.text, level, item.channels, key, item.ts))
        except asyncio.QueueFull:
            if _lvl_rank(level) < _lvl_rank("CRITICAL"):
                bump("alerts_drop")
                return

            # best-effort eviction: pop a few items and keep the last critical
            evicted = 0
            kept: list[_AlertItem] = []
            try:
                # drain a small window, keep critical-ish ones
                for _ in range(20):
                    try:
                        old = self._queue.get_nowait()
                    except Exception:
                        break
                    if _lvl_rank(old.level) >= _lvl_rank("CRITICAL") and len(kept) < 5:
                        kept.append(old)
                    else:
                        evicted += 1
            except Exception:
                pass

            # reinsert kept items
            for it in kept:
                try:
                    self._queue.put_nowait(it)
                except Exception:
                    break

            # try insert critical item
            with contextlib.suppress(Exception):
                self._queue.put_nowait(_AlertItem(item.text, level, item.channels, key, item.ts))

            bump("alerts_drop_evicted")
            log.warning("ALERT_QUEUE_FULL", extra={"evicted": evicted, "kept": len(kept)})

    async def _send_item_immediate(self, item: _AlertItem) -> None:
        # direct fanout without batching (still throttled)
        if "tg" in item.channels:
            await self._throttle("tg")
            await self._send_tg(item.text)
        if "discord" in item.channels:
            await self._throttle("discord")
            await self._send_discord(item.text)

    # ────────────────────────────────────────────────────────────────────
    # batching loop
    # ────────────────────────────────────────────────────────────────────
    async def _flush_loop(self) -> None:
        try:
            while not self._closed:
                batch: list[_AlertItem] = []
                # wait for at least one, or time out to flush cycle
                try:
                    it = await asyncio.wait_for(self._queue.get(), timeout=self.flush_sec)
                    batch.append(it)
                except TimeoutError:
                    batch = []

                # drain quickly up to cap
                while len(batch) < self.max_per_flush:
                    try:
                        batch.append(self._queue.get_nowait())
                    except Exception:
                        break

                if not batch:
                    continue

                tg_parts: list[str] = []
                dc_parts: list[str] = []

                for it in batch:
                    if "tg" in it.channels:
                        tg_parts.append(it.text)
                    if "discord" in it.channels:
                        dc_parts.append(it.text)

                if tg_parts and self._tg_ok():
                    await self._send_batched("tg", tg_parts)
                if dc_parts and self._discord_ok():
                    await self._send_batched("discord", dc_parts)

        except asyncio.CancelledError:
            return
        except Exception as e:
            log.warning("ALERT_FLUSH_LOOP_CRASH", extra={"err": _clip(str(e), 240)})

    async def _send_batched(self, channel: str, parts: Sequence[str]) -> None:
        max_len = self._TG_MAX if channel == "tg" else self._DISCORD_MAX
        joiner = "\n\n"

        chunks: list[str] = []
        cur = ""

        for p in parts:
            p = (p or "").strip()
            if not p:
                continue
            if len(p) > max_len:
                p = _clip(p, max_len)

            if not cur:
                cur = p
                continue

            cand = cur + joiner + p
            if len(cand) <= max_len:
                cur = cand
            else:
                chunks.append(cur)
                cur = p

        if cur:
            chunks.append(cur)

        for msg in chunks:
            await self._throttle(channel)
            if channel == "tg":
                await self._send_tg(msg)
            else:
                await self._send_discord(msg)

    async def _throttle(self, channel: str) -> None:
        now = time.time()
        last = self._last_send.get(channel, 0.0)
        min_int = self.tg_min_interval if channel == "tg" else self.discord_min_interval
        wait = (last + min_int) - now
        if wait > 0:
            await asyncio.sleep(min(wait, 2.0))
        self._last_send[channel] = time.time()

    # ────────────────────────────────────────────────────────────────────
    # Telegram / Discord senders (best-effort)
    # ────────────────────────────────────────────────────────────────────
    async def _send_tg(self, text: str) -> None:
        if not self._tg_ok():
            return
        s = await self._ensure_session()
        url = f"https://api.telegram.org/bot{self.tg_token}/sendMessage"
        payload: dict[str, Any] = {
            "chat_id": self.tg_chat,
            "text": _clip(text, self._TG_MAX),
            "disable_web_page_preview": True,
        }
        if self.tg_parse_mode:
            payload["parse_mode"] = self.tg_parse_mode

        try:
            async with s.post(url, json=payload) as resp:
                # Telegram can return 429 (rate limit) with retry_after in JSON.
                if resp.status == 429:
                    try:
                        data = await resp.json()
                        retry = float((data.get("parameters") or {}).get("retry_after") or 1.0)
                        await asyncio.sleep(min(max(0.5, retry), 5.0))
                        bump("alerts_tg_429")
                    except Exception:
                        bump("alerts_tg_429")
                    return
                if resp.status != 200:
                    body = await resp.text()
                    log.warning(
                        "ALERT_TG_FAIL", extra={"status": resp.status, "body": _clip(body, 400)}
                    )
                    bump("alerts_tg_fail")
                else:
                    bump("alerts_tg_ok")
        except Exception as e:
            log.warning("ALERT_TG_EXC", extra={"err": _clip(str(e), 240)})
            bump("alerts_tg_exc")

    async def _send_discord(self, text: str) -> None:
        if not self._discord_ok():
            return
        s = await self._ensure_session()
        payload = {"content": _clip(text, self._DISCORD_MAX)}
        try:
            async with s.post(self.discord_webhook, json=payload) as resp:
                if resp.status == 429:
                    # Discord webhook rate limits; try honor Retry-After if present
                    retry_after = resp.headers.get("Retry-After")
                    try:
                        if retry_after:
                            await asyncio.sleep(min(max(0.5, float(retry_after)), 5.0))
                    except Exception:
                        pass
                    bump("alerts_dc_429")
                    return
                if resp.status >= 300:
                    body = await resp.text()
                    log.warning(
                        "ALERT_DISCORD_FAIL",
                        extra={"status": resp.status, "body": _clip(body, 400)},
                    )
                    bump("alerts_dc_fail")
                else:
                    bump("alerts_dc_ok")
        except Exception as e:
            log.warning("ALERT_DISCORD_EXC", extra={"err": _clip(str(e), 240)})
            bump("alerts_dc_exc")

    # ────────────────────────────────────────────────────────────────────
    # Notion (optional direct DB insert)
    # ────────────────────────────────────────────────────────────────────
    async def _notion_insert_trade_row(self, props: dict[str, Any]) -> None:
        if not self._notion_ok():
            return
        if _dryrun_on() and not self.allow_dryrun_notion:
            return

        s = await self._ensure_session()
        headers = {
            "Authorization": f"Bearer {self.notion_token}",
            "Notion-Version": self.notion_version,
            "Content-Type": "application/json",
        }
        body = {"parent": {"database_id": self.notion_trades_db}, "properties": props}

        try:
            async with s.post(
                "https://api.notion.com/v1/pages", headers=headers, json=body
            ) as resp:
                if resp.status >= 300:
                    txt = await resp.text()
                    log.warning(
                        "ALERT_NOTION_FAIL", extra={"status": resp.status, "body": _clip(txt, 600)}
                    )
                    bump("alerts_notion_fail")
                else:
                    bump("alerts_notion_ok")
        except Exception as e:
            log.warning("ALERT_NOTION_EXC", extra={"err": _clip(str(e), 240)})
            bump("alerts_notion_exc")

    # ────────────────────────────────────────────────────────────────────
    # Public API
    # ────────────────────────────────────────────────────────────────────
    async def notify_text(
        self,
        text: str,
        *,
        level: str = "INFO",
        key: str = "",
        channels: tuple[str, ...] = ("tg", "discord"),
    ) -> None:
        t = (text or "").strip()
        if not t:
            return
        await self._enqueue(
            _AlertItem(
                text=t, level=_lvl_norm(level), channels=channels, key=key or t, ts=time.time()
            )
        )

    async def notify_error(
        self,
        label: str,
        err: Exception | str,
        extra: Mapping[str, Any] | None = None,
        *,
        level: str = "ERROR",
        key: str = "",
        channels: tuple[str, ...] = ("tg", "discord"),
    ) -> None:
        lines = [f"⚠️ {label}", f"err={_clip(str(err), 500)}"]
        if extra:
            for k, v in extra.items():
                lines.append(f"{k}={_clip(str(v), 250)}")
        msg = "\n".join(lines)
        await self._enqueue(
            _AlertItem(
                text=msg,
                level=_lvl_norm(level),
                channels=channels,
                key=key or f"err:{label}",
                ts=time.time(),
            )
        )

    async def notify_reject(
        self,
        bucket: str,
        ctx: Mapping[str, Any],
        *,
        level: str | None = None,
        channels: tuple[str, ...] = ("tg", "discord"),
    ) -> None:
        """
        Standardized reject alert (helps eliminate "silent no-buy").
        """
        if not self.rejects_enabled:
            return

        lvl = _lvl_norm(level or self.rejects_level)
        mint = str(ctx.get("mint") or ctx.get("token") or "")
        sym = str(ctx.get("symbol") or ctx.get("sym") or "")
        pool = str(ctx.get("pool") or ctx.get("pair") or "")
        score = ctx.get("score")
        why = ctx.get("why") or ctx.get("reason") or ctx.get("reject_reason") or ""

        msg = (
            f"⛔ REJECT {bucket}\n"
            f"symbol={sym or '-'} pool={pool or '-'}\n"
            f"mint={mint or '-'}\n"
            f"score={score if score is not None else '-'}\n"
            f"why={_clip(str(why), 600)}"
        )
        dedupe_key = f"rej:{bucket}:{sym or mint}:{pool}".lower()
        await self._enqueue(
            _AlertItem(text=msg, level=lvl, channels=channels, key=dedupe_key, ts=time.time())
        )

    async def notify_trade_fill(
        self,
        *,
        side: str,  # BUY / SELL
        mint: str,
        symbol: str | None,
        pool: str = "",
        sig: str | None = None,
        spend_sol: float | None = None,
        out_amount_raw: int | None = None,
        reason: str = "FILL",
        level: str = "IMPORTANT",
        extra: dict[str, Any] | None = None,
        channels: tuple[str, ...] = ("tg", "discord"),
    ) -> None:
        """
        Trade fill fanout:
          - Telegram/Discord (batched)
          - Notion direct insert (if configured) else best-effort core_utils.log_trade_event fallback
        """
        side_u = (side or "").strip().upper() or "BUY"
        tx_url = ""
        if sig:
            tx_url = sig if sig.startswith("http") else f"https://solscan.io/tx/{sig}"

        lines = [
            f"✅ {side_u} FILLED",
            f"symbol={symbol or '-'} pool={pool or '-'}",
            f"mint={mint}",
            f"spendSOL={spend_sol if spend_sol is not None else '-'} outRaw={out_amount_raw if out_amount_raw is not None else '-'}",
            f"reason={_clip(reason, 240)}",
        ]
        if tx_url:
            lines.append(f"tx={tx_url}")
        if extra:
            # keep it compact
            lines.append(f"extra={_clip(_json(extra), 700)}")

        msg = "\n".join(lines)
        await self._enqueue(
            _AlertItem(
                text=msg,
                level=_lvl_norm(level),
                channels=channels,
                key=f"fill:{side_u}:{mint}:{pool}".lower(),
                ts=time.time(),
            )
        )

        # Notion (direct)
        if self._notion_ok():
            now_iso = datetime.now(UTC).isoformat(timespec="seconds")
            side_name = "Buy" if side_u.startswith("B") else "Sell"
            props: dict[str, Any] = {
                "Name": {"title": [{"text": {"content": f"{side_name} {symbol or mint[:6]}"}}]},
                "Side": {"select": {"name": side_name}},
                "Symbol": {"rich_text": [{"text": {"content": symbol or ""}}]},
                "Pool": {"rich_text": [{"text": {"content": pool or ""}}]},
                "Mint": {"rich_text": [{"text": {"content": mint}}]},
                "Time": {"date": {"start": now_iso}},
            }
            if spend_sol is not None:
                props["SpendSOL"] = {"number": float(spend_sol)}
            if out_amount_raw is not None:
                props["OutRaw"] = {"number": float(out_amount_raw)}
            if tx_url:
                props["TxSig"] = {"url": tx_url}
            await self._notion_insert_trade_row(props)
            return

        # Fallback to core_utils unified logger (best-effort)
        try:
            from . import core_utils as cu  # local import (avoid cycles)

            fn = getattr(cu, "log_trade_event", None)
            if callable(fn):
                await fn(
                    token=mint,
                    symbol=str(symbol or ""),
                    balance=float(spend_sol or 0.0),
                    entry=0.0,
                    current=0.0,
                    profit=0.0,
                    reason=f"{side_u}:{reason}",
                    side=side_u.lower(),
                    signature=sig,
                )
        except Exception:
            pass

    # Compatibility helpers (older code paths)
    async def send_buy_alert(
        self,
        symbol: str,
        price: float,
        url: str = "",
        *,
        sol_value: float | None = None,
        reason: str = "BUY",
        level: str = "IMPORTANT",
    ) -> None:
        msg = f"🟢 BUY {symbol}\nprice={price}\nsol={sol_value if sol_value is not None else '-'}"
        if url:
            msg += f"\n{url}"
        await self.notify_text(msg, level=level, key=f"buy:{symbol}:{reason}".lower())

    async def send_sell_alert(
        self,
        symbol: str,
        price: float,
        profit_sol: float = 0.0,
        url: str = "",
        *,
        sol_value: float | None = None,
        reason: str = "SELL",
        level: str = "IMPORTANT",
    ) -> None:
        msg = (
            f"🔴 SELL {symbol}\n"
            f"price={price}\n"
            f"pnlSOL={profit_sol:.6f}\n"
            f"sizeSOL={sol_value if sol_value is not None else '-'}\n"
            f"reason={reason}"
        )
        if url:
            msg += f"\n{url}"
        await self.notify_text(msg, level=level, key=f"sell:{symbol}:{reason}".lower())

    # ────────────────────────────────────────────────────────────────────
    # MOMO events
    # ────────────────────────────────────────────────────────────────────
    async def _momo_cooldown_hit(self, event: str, symbol: str) -> bool:
        """
        Cooldown per (event, symbol). Uses core_utils Redis if available,
        otherwise falls back to in-memory cooldown.
        """
        if self.momo_cooldown_sec <= 0:
            return False

        key = f"{MOMO_REDIS_PREFIX}:{event}:{symbol}".lower()

        # Redis via core_utils (no competing clients)
        try:
            from . import core_utils as cu  # local import

            r = await cu.get_redis_async()
            if r is not None:
                try:
                    # Upstash REST wrapper might not expose exists(); use get as fallback.
                    if hasattr(r, "exists"):
                        exists = await r.exists(key)  # type: ignore[attr-defined]
                        if int(exists or 0) > 0:
                            return True
                    else:
                        v = await r.get(key)
                        if v is not None:
                            return True
                    await r.set(key, "1", ex=int(self.momo_cooldown_sec))
                    return False
                except Exception:
                    pass
        except Exception:
            pass

        # in-memory fallback
        now = time.time()
        t = self._momo_mem.get(key)
        if t is not None and (now - t) < float(self.momo_cooldown_sec):
            return True
        self._momo_mem[key] = now
        return False

    async def send_momo_event(self, event: str, ctx: dict[str, Any]) -> None:
        """
        - Ignore MOMO_SKIP_*
        - Only allow GOOD_MOMO_EVENTS
        - Apply cooldown per (event, symbol)
        """
        event_u = (event or "").strip().upper()
        if not event_u:
            return
        if event_u.startswith("MOMO_SKIP"):
            return
        if event_u not in GOOD_MOMO_EVENTS:
            return

        symbol = (
            str(
                ctx.get("symbol") or ctx.get("sym") or ctx.get("name") or ctx.get("mint") or "?"
            ).strip()
            or "?"
        )
        if await self._momo_cooldown_hit(event_u, symbol):
            return

        pool = str(ctx.get("pool") or ctx.get("pair") or "-").strip()
        label = MOMO_LABELS.get(event_u, event_u)

        lines: list[str] = [f"📈 {label}", f"symbol={symbol}", f"pool={pool}"]

        # only include common numeric fields if present
        for k in (
            "age_sec",
            "age_min",
            "liq",
            "liq_usd",
            "vol5",
            "vol_5m",
            "vol_5m_usd",
            "buys5",
            "sells5",
            "chg5",
            "up_pct",
            "fraud_score",
            "momentum",
            "final",
            "score",
        ):
            if k in ctx and ctx[k] is not None:
                lines.append(f"{k}={ctx[k]}")

        reason = str(ctx.get("reason") or ctx.get("why") or "").strip()
        if reason:
            lines.append(f"reason={_clip(reason, 420)}")

        sig = ctx.get("signature") or ctx.get("tx") or ctx.get("tx_sig")
        if isinstance(sig, str) and sig.strip():
            tx_url = sig if sig.startswith("http") else f"https://solscan.io/tx/{sig}"
            lines.append(f"tx={tx_url}")

        msg = "\n".join(lines)
        await self.notify_text(
            msg, level="IMPORTANT", key=f"momo:{event_u}:{symbol}:{pool}".lower()
        )

    # ────────────────────────────────────────────────────────────────────
    # optional: lightweight health snapshot
    # ────────────────────────────────────────────────────────────────────
    def health(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "critical_only": self.critical_only,
            "critical_min_level": self.critical_min_level,
            "queue_size": int(self._queue.qsize()),
            "tg_ok": int(self._tg_ok()),
            "discord_ok": int(self._discord_ok()),
            "batch_enabled": int(self.batch_enabled),
            "flush_sec": float(self.flush_sec),
            "last_send": dict(self._last_send),
        }


__all__ = ["GOOD_MOMO_EVENTS", "MOMO_LABELS", "AlertManager"]
