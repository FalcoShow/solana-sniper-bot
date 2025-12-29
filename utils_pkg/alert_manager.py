# utils_pkg/alert_manager.py
from __future__ import annotations

"""
utils_pkg/alert_manager.py

AlertManager (God-Tier / production-grade)
==========================================

Single place to fan out alerts to:

  • Telegram
  • Discord (webhook)
  • Notion (optional: per-fill DB row OR via core_utils.log_trade_event)

Core requirements
-----------------
- Batched + rate-limited sending (prevents spam + API bans)
- "Critical-only" mode switch (keep the bot quiet unless it matters)
- MOMO events:
    - Only whitelisted positive events allowed
    - MOMO_SKIP_* ignored
    - Cooldown per (event, symbol) using core_utils Redis (no duplicate redis clients)

Integration
-----------
core_utils.py attempts:
  from .alert_manager import AlertManager
and may instantiate with:
  AlertManager(session=session)

So this class MUST accept `session` kwarg and must not create competing global clients.

Public async API used by the rest of the bot:
  - await am.notify_text(text, level="INFO")
  - await am.notify_error(label, err, extra=None)
  - await am.notify_trade_fill(...)
  - await am.notify_reject(bucket, ctx)   (helps eliminate "silent no-buy")
  - await am.send_momo_event(event, ctx)

"""

import asyncio
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import aiohttp

# safe logger fallback
try:  # pragma: no cover
    from logging_setup import get_logger  # type: ignore
except Exception:  # pragma: no cover
    import logging

    def get_logger(name: str | None = None):  # type: ignore
        return logging.getLogger(name or "alert_manager")


log = get_logger(__name__)

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
    return s if len(s) <= n else s[: n - 1] + "…"


def _json_dumps(obj: Any) -> str:
    try:
        return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)
    except Exception:
        return "{}"


def _parse_csv_env(name: str) -> List[str]:
    raw = _s(name, "")
    if not raw:
        return []
    parts = [p.strip() for p in raw.split(",")]
    return [p for p in parts if p]


# ──────────────────────────────────────────────────────────────────────────────
# Levels + alert item
# ──────────────────────────────────────────────────────────────────────────────


_LEVELS = ("DEBUG", "INFO", "IMPORTANT", "CRITICAL", "ERROR")


def _lvl_norm(x: str) -> str:
    t = (x or "").strip().upper()
    return t if t in _LEVELS else "INFO"


def _lvl_rank(x: str) -> int:
    x = _lvl_norm(x)
    return _LEVELS.index(x)


@dataclass
class _AlertItem:
    text: str
    level: str = "INFO"
    channels: Tuple[str, ...] = ("tg", "discord")  # tg, discord
    key: str = ""  # for dedupe/cooldown (optional)
    ts: float = 0.0


# ──────────────────────────────────────────────────────────────────────────────
# MOMO config
# ──────────────────────────────────────────────────────────────────────────────

_DEFAULT_GOOD_MOMO = {"MOMO_OK", "MOMO_TAKE", "MOMO_LONG", "MOMO_SCALP"}
GOOD_MOMO_EVENTS = set(_parse_csv_env("MOMO_GOOD_EVENTS")) or _DEFAULT_GOOD_MOMO

MOMO_LABELS: Dict[str, str] = {
    "MOMO_OK": "Momentum OK",
    "MOMO_TAKE": "Momentum TAKE",
    "MOMO_LONG": "Momentum LONG",
    "MOMO_SCALP": "Momentum SCALP",
}

MOMO_REDIS_PREFIX = _s("MOMENTUM_ALERT_REDIS_PREFIX", "momo:alert")
MOMO_COOLDOWN_SEC = _i("MOMENTUM_ALERT_COOLDOWN_SEC", 300)


# ──────────────────────────────────────────────────────────────────────────────
# AlertManager
# ──────────────────────────────────────────────────────────────────────────────


class AlertManager:
    """
    Batched + rate-limited alert fanout.
    """

    # Telegram safe limit: 4096; keep margin for parse_mode
    _TG_MAX = 3500
    # Discord message limit: 2000
    _DISCORD_MAX = 1800

    def __init__(self, session: Optional[aiohttp.ClientSession] = None) -> None:
        # master switches
        self.enabled = _b("ALERTS_ENABLED", True)

        # "critical-only" mode (quiet unless level >= IMPORTANT/CRITICAL/ERROR)
        self.critical_only = (
            _b("ALERTS_CRITICAL_ONLY", False)
            or _b("ALERT_CRITICAL_ONLY", False)
            or _b("PF_CRITICAL_ONLY", False)
        )
        self.critical_min_level = _lvl_norm(_s("ALERTS_CRITICAL_MIN_LEVEL", "IMPORTANT"))

        # channel toggles
        self.tg_enabled = _b("ALERTS_TELEGRAM_ENABLED", True)
        self.discord_enabled = _b("ALERTS_DISCORD_ENABLED", False)

        # allow sending in DRYRUN?
        self.allow_dryrun = _b("ALERTS_ALLOW_DRYRUN", True)

        # batching
        self.batch_enabled = _b("ALERTS_BATCH_ENABLED", True)
        self.flush_sec = max(0.1, _f("ALERTS_FLUSH_SEC", 1.0))
        self.max_per_flush = max(1, _i("ALERTS_MAX_PER_FLUSH", 25))
        self.max_queue = max(100, _i("ALERTS_MAX_QUEUE", 2000))

        # rate limits
        self.tg_min_interval = max(0.0, _f("ALERTS_TG_MIN_INTERVAL_SEC", 0.8))
        self.discord_min_interval = max(0.0, _f("ALERTS_DISCORD_MIN_INTERVAL_SEC", 1.2))

        # dedupe (avoid repeating same key/text rapidly)
        self.dedupe_ttl_sec = max(0, _i("ALERTS_DEDUPE_TTL_SEC", 25))

        # MOMO cooldown
        self.momo_cooldown_sec = max(0, MOMO_COOLDOWN_SEC)

        # Telegram config (compat with core_utils envs)
        self.tg_token = _s("TELEGRAM_BOT_TOKEN")
        self.tg_chat = _s("TELEGRAM_CHAT_ID")
        self.tg_parse_mode = _s("TELEGRAM_PARSE_MODE", "HTML").strip()  # default HTML like core_utils

        # Discord webhook (compat)
        self.discord_webhook = _s("DISCORD_WEBHOOK_URL", _s("DISCORD_WEBHOOK", ""))

        # Notion per-fill DB (optional). If missing, we can fall back to core_utils.log_trade_event.
        self.notion_enabled = _b("ALERTS_NOTION_ENABLED", False)
        self.allow_dryrun_notion = _b("ALERTS_NOTION_ALLOW_DRYRUN", True)
        self.notion_token = _s("NOTION_TOKEN", _s("NOTION_API_KEY", ""))
        self.notion_trades_db = _s("NOTION_TRADES_DB_ID", "")
        self.notion_version = _s("NOTION_VERSION", "2022-06-28")

        # Optional: log reject buckets (helps kill silent no-buy)
        self.rejects_enabled = _b("ALERTS_REJECTS_ENABLED", True)
        self.rejects_level = _lvl_norm(_s("ALERTS_REJECTS_LEVEL", "INFO"))

        # lifecycle
        self._external_session = session is not None
        self._session: Optional[aiohttp.ClientSession] = session
        self._queue: "asyncio.Queue[_AlertItem]" = asyncio.Queue(maxsize=self.max_queue)
        self._worker: Optional[asyncio.Task[None]] = None
        self._closed = False

        # throttles
        self._last_send: Dict[str, float] = {"tg": 0.0, "discord": 0.0}

        # dedupe cache (in-memory fast path)
        self._dedupe_mem: Dict[str, float] = {}
        self._momo_mem: Dict[str, float] = {}

        log.info(
            "ALERT_MANAGER_READY",
            extra={
                "enabled": self.enabled,
                "critical_only": self.critical_only,
                "critical_min_level": self.critical_min_level,
                "tg": bool(self.tg_token and self.tg_chat) and self.tg_enabled,
                "discord": bool(self.discord_webhook) and self.discord_enabled,
                "batch": self.batch_enabled,
                "flush_sec": self.flush_sec,
                "dryrun": _dryrun_on(),
            },
        )

    # ────────────────────────────────────────────────────────────────────
    # Session / worker lifecycle
    # ────────────────────────────────────────────────────────────────────

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is not None and not self._session.closed:
            return self._session
        # local session only if none provided (avoid duplicates)
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=_f("ALERTS_HTTP_TIMEOUT_SEC", 12.0))
        )
        return self._session

    async def _ensure_worker(self) -> None:
        if self._worker is not None and not self._worker.done():
            return
        self._worker = asyncio.create_task(self._flush_loop(), name="alert_manager:flush_loop")

    async def close(self) -> None:
        self._closed = True
        if self._worker is not None:
            self._worker.cancel()
            try:
                await self._worker
            except Exception:
                pass
        self._worker = None
        if not self._external_session and self._session is not None:
            try:
                await self._session.close()
            except Exception:
                pass
        self._session = None

    # ────────────────────────────────────────────────────────────────────
    # Filtering: critical-only + dryrun
    # ────────────────────────────────────────────────────────────────────

    def _allowed_by_level(self, level: str) -> bool:
        if not self.critical_only:
            return True
        return _lvl_rank(level) >= _lvl_rank(self.critical_min_level)

    def _allowed_in_dryrun(self) -> bool:
        return self.allow_dryrun or (not _dryrun_on())

    def _tg_ok(self) -> bool:
        return self.enabled and self.tg_enabled and bool(self.tg_token and self.tg_chat)

    def _discord_ok(self) -> bool:
        return self.enabled and self.discord_enabled and bool(self.discord_webhook)

    def _notion_ok(self) -> bool:
        return self.enabled and self.notion_enabled and bool(self.notion_token and self.notion_trades_db)

    # ────────────────────────────────────────────────────────────────────
    # Dedupe (in-memory; Redis optional for momo cooldown only)
    # ────────────────────────────────────────────────────────────────────

    def _dedupe_hit(self, key: str) -> bool:
        if self.dedupe_ttl_sec <= 0:
            return False
        k = (key or "").strip()
        if not k:
            return False
        now = time.time()
        t = self._dedupe_mem.get(k)
        if t is not None and (now - t) < self.dedupe_ttl_sec:
            return True
        self._dedupe_mem[k] = now
        # prune occasionally
        if len(self._dedupe_mem) > 5000:
            cutoff = now - float(self.dedupe_ttl_sec) * 2
            self._dedupe_mem = {kk: vv for kk, vv in self._dedupe_mem.items() if vv >= cutoff}
        return False

    # ────────────────────────────────────────────────────────────────────
    # Enqueue / immediate send paths
    # ────────────────────────────────────────────────────────────────────

    async def _enqueue(self, item: _AlertItem) -> None:
        if self._closed or not self.enabled:
            return
        if not self._allowed_in_dryrun():
            return
        item.level = _lvl_norm(item.level)
        if not self._allowed_by_level(item.level):
            return
        if item.key and self._dedupe_hit(item.key):
            return

        if not self.batch_enabled:
            await self._send_item_immediate(item)
            return

        await self._ensure_worker()
        try:
            self._queue.put_nowait(item)
        except asyncio.QueueFull:
            # If queue is full, drop non-critical first
            if _lvl_rank(item.level) >= _lvl_rank("CRITICAL"):
                # force put by removing one old item
                try:
                    _ = self._queue.get_nowait()
                    self._queue.put_nowait(item)
                except Exception:
                    pass

    async def _send_item_immediate(self, item: _AlertItem) -> None:
        # no batching: direct fanout
        if "tg" in item.channels:
            await self._send_tg(item.text)
        if "discord" in item.channels:
            await self._send_discord(item.text)

    # ────────────────────────────────────────────────────────────────────
    # Flush loop (batching)
    # ────────────────────────────────────────────────────────────────────

    async def _flush_loop(self) -> None:
        try:
            while not self._closed:
                batch: List[_AlertItem] = []
                try:
                    it = await asyncio.wait_for(self._queue.get(), timeout=self.flush_sec)
                    batch.append(it)
                except asyncio.TimeoutError:
                    batch = []

                # drain quickly up to cap
                while len(batch) < self.max_per_flush:
                    try:
                        batch.append(self._queue.get_nowait())
                    except Exception:
                        break

                if not batch:
                    continue

                # group per channel
                tg_texts: List[str] = []
                dc_texts: List[str] = []
                for it in batch:
                    if "tg" in it.channels:
                        tg_texts.append(it.text)
                    if "discord" in it.channels:
                        dc_texts.append(it.text)

                if tg_texts:
                    await self._send_batched("tg", tg_texts)
                if dc_texts:
                    await self._send_batched("discord", dc_texts)

        except asyncio.CancelledError:
            return
        except Exception as e:
            log.warning("ALERT_FLUSH_LOOP_CRASH", extra={"err": str(e)[:240]})

    async def _send_batched(self, channel: str, parts: List[str]) -> None:
        """
        Join parts into chunks under the channel max length, then send with throttle.
        """
        if channel == "tg" and not self._tg_ok():
            return
        if channel == "discord" and not self._discord_ok():
            return

        max_len = self._TG_MAX if channel == "tg" else self._DISCORD_MAX
        joiner = "\n\n"  # readable separation
        chunks: List[str] = []
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
        """
        Simple min-interval throttle.
        """
        now = time.time()
        last = self._last_send.get(channel, 0.0)
        min_int = self.tg_min_interval if channel == "tg" else self.discord_min_interval
        wait = (last + min_int) - now
        if wait > 0:
            await asyncio.sleep(min(wait, 2.0))
        self._last_send[channel] = time.time()

    # ────────────────────────────────────────────────────────────────────
    # Telegram / Discord implementations
    # ────────────────────────────────────────────────────────────────────

    async def _send_tg(self, text: str) -> None:
        if not self._tg_ok():
            return
        s = await self._ensure_session()
        url = f"https://api.telegram.org/bot{self.tg_token}/sendMessage"
        payload: Dict[str, Any] = {
            "chat_id": self.tg_chat,
            "text": text,
            "disable_web_page_preview": True,
        }
        if self.tg_parse_mode:
            payload["parse_mode"] = self.tg_parse_mode

        try:
            async with s.post(url, json=payload) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    log.warning("ALERT_TG_FAIL", extra={"status": resp.status, "body": body[:400]})
        except Exception as e:
            log.warning("ALERT_TG_EXC", extra={"err": str(e)[:240]})

    async def _send_discord(self, text: str) -> None:
        if not self._discord_ok():
            return
        s = await self._ensure_session()
        try:
            async with s.post(self.discord_webhook, json={"content": text}) as resp:
                if resp.status >= 300:
                    body = await resp.text()
                    log.warning("ALERT_DISCORD_FAIL", extra={"status": resp.status, "body": body[:400]})
        except Exception as e:
            log.warning("ALERT_DISCORD_EXC", extra={"err": str(e)[:240]})

    # ────────────────────────────────────────────────────────────────────
    # Notion: optional per-fill DB insert (if NOTION_TRADES_DB_ID set)
    # ────────────────────────────────────────────────────────────────────

    async def _notion_insert_trade_row(self, props: Dict[str, Any]) -> None:
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
            async with s.post("https://api.notion.com/v1/pages", headers=headers, json=body) as resp:
                if resp.status >= 300:
                    txt = await resp.text()
                    log.warning("ALERT_NOTION_FAIL", extra={"status": resp.status, "body": txt[:600]})
        except Exception as e:
            log.warning("ALERT_NOTION_EXC", extra={"err": str(e)[:240]})

    # ────────────────────────────────────────────────────────────────────
    # Public API
    # ────────────────────────────────────────────────────────────────────

    async def notify_text(self, text: str, *, level: str = "INFO", key: str = "") -> None:
        text = (text or "").strip()
        if not text:
            return
        await self._enqueue(_AlertItem(text=text, level=level, channels=("tg", "discord"), key=key or text, ts=time.time()))

    async def notify_error(
        self,
        label: str,
        err: Exception | str,
        extra: Optional[Mapping[str, Any]] = None,
        *,
        level: str = "ERROR",
        key: str = "",
    ) -> None:
        lines = [f"⚠️ {label}", f"err={_clip(str(err), 400)}"]
        if extra:
            for k, v in extra.items():
                lines.append(f"{k}={_clip(str(v), 200)}")
        msg = "\n".join(lines)
        await self._enqueue(_AlertItem(text=msg, level=level, channels=("tg", "discord"), key=key or f"err:{label}", ts=time.time()))

    async def notify_reject(self, bucket: str, ctx: Mapping[str, Any], *, level: Optional[str] = None) -> None:
        """
        Standardized reject alert (optional).
        Use this when scanners/sniper reject a candidate to prevent "silent no-buy".
        """
        if not self.rejects_enabled:
            return
        lvl = _lvl_norm(level or self.rejects_level)
        mint = str(ctx.get("mint") or ctx.get("token") or "")
        sym = str(ctx.get("symbol") or ctx.get("sym") or "")
        pool = str(ctx.get("pool") or ctx.get("pair") or "")
        score = ctx.get("score")
        why = ctx.get("why") or ctx.get("reason") or ""

        msg = (
            f"⛔ REJECT {bucket}\n"
            f"symbol={sym or '-'} pool={pool or '-'}\n"
            f"mint={mint or '-'}\n"
            f"score={score if score is not None else '-'}\n"
            f"why={_clip(str(why), 500)}"
        )
        dedupe_key = f"rej:{bucket}:{sym or mint}:{pool}"
        await self._enqueue(_AlertItem(text=msg, level=lvl, channels=("tg", "discord"), key=dedupe_key, ts=time.time()))

    async def notify_trade_fill(
        self,
        *,
        side: str,  # BUY / SELL
        mint: str,
        symbol: Optional[str],
        pool: str,
        sig: Optional[str],
        spend_sol: Optional[float] = None,
        out_amount_raw: Optional[int] = None,
        reason: str = "FILL",
        level: str = "IMPORTANT",
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Trade fill fanout:
          - Telegram/Discord message (batched)
          - Notion (optional): either per-fill DB insert OR core_utils.log_trade_event
        """
        side_u = (side or "").strip().upper() or "BUY"
        tx_url = ""
        if sig:
            tx_url = sig if sig.startswith("http") else f"https://solscan.io/tx/{sig}"

        msg = (
            f"✅ {side_u} FILLED\n"
            f"symbol={symbol or '-'} pool={pool or '-'}\n"
            f"mint={mint}\n"
            f"spendSOL={spend_sol if spend_sol is not None else '-'} outRaw={out_amount_raw if out_amount_raw is not None else '-'}\n"
            f"reason={_clip(reason, 220)}\n"
            f"tx={tx_url or '—'}"
        )
        await self._enqueue(_AlertItem(text=msg, level=level, channels=("tg", "discord"), key=f"fill:{side_u}:{mint}:{pool}", ts=time.time()))

        # Notion per-fill DB (optional)
        if self._notion_ok():
            now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
            side_select = "Buy" if side_u.startswith("B") else "Sell"
            props: Dict[str, Any] = {
                "Name": {"title": [{"text": {"content": f"{side_select} {symbol or mint[:6]}…"}}]},
                "Side": {"select": {"name": side_select}},
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

        # Fallback: use core_utils unified logging (PG + Notion mapping) if available
        try:
            from . import core_utils as cu  # local import to avoid cycles

            # logs to PG + (optional) Notion via core_utils
            await cu.log_trade_event(
                token=mint,
                symbol=str(symbol or ""),
                balance=float(spend_sol or 0.0),
                entry=float(0.0),
                current=float(0.0),
                profit=float(0.0),
                reason=f"{side_u}:{reason}",
                side=side_u.lower(),
                signature=sig,
            )
        except Exception:
            pass

    # ────────────────────────────────────────────────────────────────────
    # MOMO events (scanner signals)
    # ────────────────────────────────────────────────────────────────────

    async def _momo_cooldown_hit(self, event: str, symbol: str) -> bool:
        """
        Cooldown per (event, symbol). Uses core_utils Redis if available,
        otherwise falls back to in-memory cooldown.
        """
        if self.momo_cooldown_sec <= 0:
            return False

        key = f"{MOMO_REDIS_PREFIX}:{event}:{symbol}".lower()

        # Try redis via core_utils (native or Upstash REST)
        try:
            from . import core_utils as cu  # local import

            r = await cu.get_redis_async()
            if r is not None:
                # Upstash REST wrapper may not have EXISTS; use GET as exists check.
                try:
                    if hasattr(r, "exists"):
                        exists = await r.exists(key)  # type: ignore[attr-defined]
                        if int(exists or 0) > 0:
                            return True
                    else:
                        v = await r.get(key)
                        if v is not None:
                            return True
                    await r.set(key, "1", ex=self.momo_cooldown_sec)
                    return False
                except Exception:
                    # fall through to memory
                    pass
        except Exception:
            pass

        # In-memory fallback (per-process)
        now = time.time()
        t = self._momo_mem.get(key)
        if t is not None and (now - t) < self.momo_cooldown_sec:
            return True
        self._momo_mem[key] = now
        return False

    async def send_momo_event(self, event: str, ctx: Dict[str, Any]) -> None:
        """
        - Ignore MOMO_SKIP_*
        - Only allow GOOD_MOMO_EVENTS
        - Apply cooldown per (event,symbol)
        """
        event_u = (event or "").strip().upper()
        if not event_u:
            return
        if event_u.startswith("MOMO_SKIP"):
            return
        if event_u not in GOOD_MOMO_EVENTS:
            return

        symbol = str(ctx.get("symbol") or ctx.get("sym") or ctx.get("mint") or ctx.get("name") or "?").strip()
        if not symbol:
            symbol = "?"

        if await self._momo_cooldown_hit(event_u, symbol):
            return

        pool = str(ctx.get("pool") or ctx.get("pair") or "-").strip()
        reason = str(ctx.get("reason") or "").strip()

        label = MOMO_LABELS.get(event_u, event_u)
        lines = [f"📈 {label}", f"symbol={symbol}", f"pool={pool}"]

        # common extras (only if present)
        for k in ("age_sec", "age_min", "liq_usd", "vol_5m", "vol_5m_usd", "up_pct", "fraud_score", "momentum", "final"):
            if k in ctx and ctx[k] is not None:
                lines.append(f"{k}={ctx[k]}")

        if reason:
            lines.append(f"reason={_clip(reason, 420)}")

        sig = ctx.get("signature") or ctx.get("tx") or ctx.get("tx_sig")
        if isinstance(sig, str) and sig.strip():
            tx_url = sig if sig.startswith("http") else f"https://solscan.io/tx/{sig}"
            lines.append(f"tx={tx_url}")

        msg = "\n".join(lines)
        await self.notify_text(msg, level="IMPORTANT", key=f"momo:{event_u}:{symbol}:{pool}")


__all__ = ["AlertManager", "GOOD_MOMO_EVENTS", "MOMO_LABELS"]
