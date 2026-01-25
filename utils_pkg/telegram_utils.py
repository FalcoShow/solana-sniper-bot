# utils_pkg/telegram_utils.py
# Async Telegram helpers (production-grade): text/photos/docs with retries, chunking,
# min-interval rate limiting, and proper 429 handling. Safe-by-default: never crashes callers.

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import aiohttp

# logger (structured-friendly if your logging_setup is present)
try:  # pragma: no cover
    from logging_setup import get_logger  # type: ignore
except Exception:  # pragma: no cover
    import logging

    def get_logger(name: str | None = None):  # type: ignore
        return logging.getLogger(name or "telegram_utils")


log = get_logger(__name__)

# optional metrics bump hook
try:  # pragma: no cover
    from logging_setup import bump  # type: ignore
except Exception:  # pragma: no cover

    def bump(*_a: Any, **_k: Any) -> None:  # type: ignore
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Env / defaults (contract)
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


TELEGRAM_ENABLED = _b("TELEGRAM_ENABLED", True)
TELEGRAM_BOT_TOKEN = _s("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = _s("TELEGRAM_CHAT_ID", "")
TELEGRAM_THREAD_ID = _s("TELEGRAM_THREAD_ID", "")  # optional forum topic id (message_thread_id)
TELEGRAM_PARSE_MODE = _s("TELEGRAM_PARSE_MODE", "HTML")  # "Markdown" | "MarkdownV2" | "HTML"
TELEGRAM_TIMEOUT = _f("TELEGRAM_TIMEOUT", 15.0)
TELEGRAM_RPS = _f("TELEGRAM_RPS", 1.0)  # requests per second cap (module default client)
TELEGRAM_RETRIES = max(1, _i("TELEGRAM_RETRIES", 3))
TELEGRAM_BACKOFF_BASE = _f("TELEGRAM_BACKOFF_BASE", 0.75)
TELEGRAM_API_BASE = _s("TELEGRAM_API_BASE", "https://api.telegram.org")

TG_MAX_TEXT = 4096  # Telegram hard limit per message


def _parse_mode(x: str | None) -> str:
    m = (x or "").strip()
    if m in ("Markdown", "MarkdownV2", "HTML"):
        return m
    return (
        TELEGRAM_PARSE_MODE if TELEGRAM_PARSE_MODE in ("Markdown", "MarkdownV2", "HTML") else "HTML"
    )


# ──────────────────────────────────────────────────────────────────────────────
# Text helpers
# ──────────────────────────────────────────────────────────────────────────────
def escape_markdown_v2(text: str) -> str:
    """
    Minimal MarkdownV2 escape. Only use when parse_mode == "MarkdownV2".
    """
    specials = set(r"_*[]()~`>#+-=|{}.!")  # per Telegram MarkdownV2 rules
    out: list[str] = []
    for ch in text:
        out.append("\\" + ch if ch in specials else ch)
    return "".join(out)


def _chunk_text(text: str, limit: int = TG_MAX_TEXT) -> list[str]:
    """
    Split text safely under Telegram's 4096 char limit.
    Prefers paragraph and line boundaries, then hard cuts.
    """
    t = (text or "").strip()
    if not t:
        return []
    if len(t) <= limit:
        return [t]

    parts: list[str] = []
    for para in t.split("\n\n"):
        para = para.strip()
        if not para:
            continue
        if len(para) <= limit:
            parts.append(para)
            continue

        buf = ""
        for line in para.splitlines():
            line = line.rstrip("\n")
            cand = (buf + "\n" + line) if buf else line
            if len(cand) <= limit:
                buf = cand
                continue

            if buf:
                parts.append(buf)
                buf = ""

            # if a single line is too long, hard cut
            while len(line) > limit:
                parts.append(line[:limit])
                line = line[limit:]
            buf = line

        if buf:
            parts.append(buf)

    # final safety
    return [p[:limit] for p in parts if p]


def _build_inline_keyboard(
    button_rows: Sequence[Sequence[dict[str, str]]] | None,
) -> dict[str, Any] | None:
    """
    button_rows example:
      [
        [{"text":"Solscan","url":"https://..."}],
        [{"text":"OK","callback_data":"ok"}],
      ]
    """
    if not button_rows:
        return None
    return {"inline_keyboard": [[btn for btn in row] for row in button_rows]}


def _endpoint(bot_token: str, method: str) -> str:
    base = TELEGRAM_API_BASE.rstrip("/")
    return f"{base}/bot{bot_token}/{method}"


# ──────────────────────────────────────────────────────────────────────────────
# Rate limiter (min interval)
# ──────────────────────────────────────────────────────────────────────────────
class _RateGate:
    def __init__(self, rps: float) -> None:
        self._lock = asyncio.Lock()
        self._next_ts = 0.0
        self._rps = max(0.0, float(rps or 0.0))

    async def wait(self) -> None:
        if self._rps <= 0:
            return
        min_interval = 1.0 / self._rps
        async with self._lock:
            now = time.monotonic()
            if now < self._next_ts:
                await asyncio.sleep(self._next_ts - now)
            self._next_ts = time.monotonic() + min_interval


# ──────────────────────────────────────────────────────────────────────────────
# Telegram client
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class TelegramConfig:
    enabled: bool = TELEGRAM_ENABLED
    bot_token: str = TELEGRAM_BOT_TOKEN
    chat_id: str = TELEGRAM_CHAT_ID
    thread_id: str = TELEGRAM_THREAD_ID
    parse_mode: str = TELEGRAM_PARSE_MODE
    timeout_sec: float = TELEGRAM_TIMEOUT
    rps: float = TELEGRAM_RPS
    retries: int = TELEGRAM_RETRIES
    backoff_base: float = TELEGRAM_BACKOFF_BASE


class TelegramClient:
    """
    Low-level Telegram API helper.

    - Supports external aiohttp session injection (preferred if you already own one).
    - Implements rate limiting, retry/backoff, and Telegram 429 retry_after handling.
    - Best-effort: never raises to callers by default.
    """

    def __init__(
        self, *, session: aiohttp.ClientSession | None = None, cfg: TelegramConfig | None = None
    ) -> None:
        self.cfg = cfg or TelegramConfig()
        self._external_session = session is not None
        self._session: aiohttp.ClientSession | None = session
        self._session_lock = asyncio.Lock()
        self._gate = _RateGate(self.cfg.rps)
        self._closed = False

    def ok(self) -> bool:
        return bool(self.cfg.enabled and self.cfg.bot_token and self.cfg.chat_id)

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is not None and not self._session.closed:
            return self._session
        async with self._session_lock:
            if self._session is not None and not self._session.closed:
                return self._session
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=max(3.0, float(self.cfg.timeout_sec)))
            )
            return self._session

    async def close(self) -> None:
        self._closed = True
        if not self._external_session and self._session is not None:
            with contextlib.suppress(Exception):
                await self._session.close()
        self._session = None

    async def _post_json(self, method: str, payload: dict[str, Any]) -> dict[str, Any] | None:
        """
        Best-effort POST. Returns parsed JSON on success, else None.
        """
        if self._closed or not self.cfg.enabled:
            return None
        if not self.cfg.bot_token:
            return None

        url = _endpoint(self.cfg.bot_token, method)
        s = await self._ensure_session()

        for attempt in range(1, int(self.cfg.retries) + 1):
            await self._gate.wait()

            try:
                async with s.post(url, json=payload) as resp:
                    # Telegram returns JSON even on errors (usually)
                    # but don't assume it always does.
                    if resp.status == 200:
                        try:
                            data = await resp.json()
                        except Exception:
                            text = await resp.text()
                            log.warning("TG_BAD_JSON", extra={"method": method, "body": text[:400]})
                            bump("tg_bad_json")
                            return None
                        if data.get("ok") is True:
                            bump("tg_ok")
                            return data
                        bump("tg_api_not_ok")
                        return data

                    # 429: respect retry_after if present
                    if resp.status == 429:
                        retry_after = 1.0
                        try:
                            data = await resp.json()
                            retry_after = float(
                                (data.get("parameters") or {}).get("retry_after") or 1.0
                            )
                        except Exception:
                            pass
                        bump("tg_429")
                        await asyncio.sleep(min(max(0.5, retry_after), 8.0))
                        continue

                    # transient-ish: retry on 5xx
                    if 500 <= resp.status <= 599:
                        body = await resp.text()
                        log.warning(
                            "TG_5XX",
                            extra={"method": method, "status": resp.status, "body": body[:250]},
                        )
                        bump("tg_5xx")
                    else:
                        body = await resp.text()
                        log.warning(
                            "TG_FAIL",
                            extra={"method": method, "status": resp.status, "body": body[:400]},
                        )
                        bump("tg_fail")
                        return None

            except Exception as e:
                log.warning(
                    "TG_EXC", extra={"method": method, "attempt": attempt, "err": str(e)[:240]}
                )
                bump("tg_exc")

            # backoff
            await asyncio.sleep(float(self.cfg.backoff_base) * attempt)

        return None

    # ────────────────────────────────────────────────────────────────────
    # Public senders
    # ────────────────────────────────────────────────────────────────────
    async def send_message(
        self,
        text: str,
        *,
        chat_id: str | None = None,
        thread_id: int | None = None,
        parse_mode: str | None = None,
        disable_web_page_preview: bool = True,
        disable_notification: bool = False,
        protect_content: bool = False,
        reply_to_message_id: int | None = None,
        buttons: Sequence[Sequence[dict[str, str]]] | None = None,
        escape: bool = True,
    ) -> list[int]:
        """
        Send a (possibly chunked) text message. Returns list of message_ids (may be empty).
        """
        if not self.cfg.enabled:
            return []

        bot_chat = chat_id or self.cfg.chat_id
        if not self.cfg.bot_token or not bot_chat:
            log.warning(
                "TG_MISSING_CONFIG",
                extra={"has_token": bool(self.cfg.bot_token), "has_chat": bool(bot_chat)},
            )
            return []

        pm = _parse_mode(parse_mode or self.cfg.parse_mode)
        chunks = _chunk_text(text, TG_MAX_TEXT)
        if not chunks:
            return []

        kb = _build_inline_keyboard(buttons)
        mids: list[int] = []

        for idx, chunk in enumerate(chunks):
            out_text = chunk
            if pm == "MarkdownV2" and escape:
                out_text = escape_markdown_v2(out_text)

            # If escaping increased length beyond TG limit, hard cut (safe last resort).
            if len(out_text) > TG_MAX_TEXT:
                out_text = out_text[:TG_MAX_TEXT]

            payload: dict[str, Any] = {
                "chat_id": bot_chat,
                "text": out_text,
                "disable_web_page_preview": bool(disable_web_page_preview),
                "disable_notification": bool(disable_notification),
                "protect_content": bool(protect_content),
            }
            if pm:
                payload["parse_mode"] = pm
            if reply_to_message_id and idx == 0:
                payload["reply_to_message_id"] = int(reply_to_message_id)

            tid = (
                thread_id
                if thread_id is not None
                else (int(self.cfg.thread_id) if str(self.cfg.thread_id).strip() else None)
            )
            if tid is not None:
                payload["message_thread_id"] = int(tid)

            if kb:
                payload["reply_markup"] = kb  # JSON object

            data = await self._post_json("sendMessage", payload)
            if data and data.get("ok") is True:
                with contextlib.suppress(Exception):
                    mids.append(int(data["result"]["message_id"]))

        return mids

    async def send_photo(
        self,
        photo: str | bytes | bytearray,
        *,
        caption: str | None = None,
        chat_id: str | None = None,
        thread_id: int | None = None,
        parse_mode: str | None = None,
        disable_notification: bool = False,
        buttons: Sequence[Sequence[dict[str, str]]] | None = None,
        content_type: str = "image/jpeg",
        filename: str = "image.jpg",
        escape: bool = True,
    ) -> int | None:
        """
        photo: URL (str), local file path (str), or raw bytes.
        returns message_id or None
        """
        if not self.cfg.enabled:
            return None

        bot_chat = chat_id or self.cfg.chat_id
        if not self.cfg.bot_token or not bot_chat:
            log.warning(
                "TG_MISSING_CONFIG",
                extra={"has_token": bool(self.cfg.bot_token), "has_chat": bool(bot_chat)},
            )
            return None

        pm = _parse_mode(parse_mode or self.cfg.parse_mode)
        tid = (
            thread_id
            if thread_id is not None
            else (int(self.cfg.thread_id) if str(self.cfg.thread_id).strip() else None)
        )
        kb = _build_inline_keyboard(buttons)

        # URL or file upload?
        if (
            isinstance(photo, str)
            and photo.strip()
            and (photo.startswith("http://") or photo.startswith("https://"))
        ):
            payload: dict[str, Any] = {
                "chat_id": bot_chat,
                "photo": photo,
                "disable_notification": bool(disable_notification),
            }
            if caption:
                cap = caption
                if pm == "MarkdownV2" and escape:
                    cap = escape_markdown_v2(cap)
                payload["caption"] = cap[:1024]  # Telegram caption limits are smaller; safe clip
                payload["parse_mode"] = pm
            if tid is not None:
                payload["message_thread_id"] = int(tid)
            if kb:
                payload["reply_markup"] = kb
            data = await self._post_json("sendPhoto", payload)
            if data and data.get("ok") is True:
                try:
                    return int(data["result"]["message_id"])
                except Exception:
                    return None
            return None

        # Upload bytes or local file path
        url = _endpoint(self.cfg.bot_token, "sendPhoto")
        s = await self._ensure_session()

        for attempt in range(1, int(self.cfg.retries) + 1):
            await self._gate.wait()
            try:
                form = aiohttp.FormData()
                form.add_field("chat_id", bot_chat)
                form.add_field("disable_notification", str(bool(disable_notification)).lower())
                if tid is not None:
                    form.add_field("message_thread_id", str(int(tid)))
                if caption:
                    cap = caption
                    if pm == "MarkdownV2" and escape:
                        cap = escape_markdown_v2(cap)
                    form.add_field("caption", cap[:1024])
                    form.add_field("parse_mode", pm)
                if kb:
                    form.add_field("reply_markup", json.dumps(kb, separators=(",", ":")))

                if isinstance(photo, bytes | bytearray):
                    form.add_field(
                        "photo", bytes(photo), filename=filename, content_type=content_type
                    )
                    async with s.post(url, data=form) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            if data.get("ok") is True:
                                bump("tg_photo_ok")
                                return int(data["result"]["message_id"])
                        if resp.status == 429:
                            bump("tg_429")
                            ra = 1
                            try:
                                data = await resp.json()
                                ra = int((data.get("parameters") or {}).get("retry_after") or 1)
                            except Exception:
                                pass
                            await asyncio.sleep(min(max(1, ra), 8))
                            continue
                        body = await resp.text()
                        log.warning(
                            "TG_PHOTO_FAIL", extra={"status": resp.status, "body": body[:350]}
                        )
                else:
                    # local file path
                    path = str(photo)
                    f = open(path, "rb")
                    try:
                        form.add_field("photo", f, filename=os.path.basename(path) or filename)
                        async with s.post(url, data=form) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                if data.get("ok") is True:
                                    bump("tg_photo_ok")
                                    return int(data["result"]["message_id"])
                            if resp.status == 429:
                                bump("tg_429")
                                ra = 1
                                try:
                                    data = await resp.json()
                                    ra = int((data.get("parameters") or {}).get("retry_after") or 1)
                                except Exception:
                                    pass
                                await asyncio.sleep(min(max(1, ra), 8))
                                continue
                            body = await resp.text()
                            log.warning(
                                "TG_PHOTO_FAIL", extra={"status": resp.status, "body": body[:350]}
                            )
                    finally:
                        with contextlib.suppress(Exception):
                            f.close()

            except Exception as e:
                log.warning("TG_PHOTO_EXC", extra={"attempt": attempt, "err": str(e)[:240]})
                bump("tg_photo_exc")

            await asyncio.sleep(float(self.cfg.backoff_base) * attempt)

        return None

    async def send_document(
        self,
        document: str | bytes | bytearray,
        *,
        caption: str | None = None,
        chat_id: str | None = None,
        thread_id: int | None = None,
        parse_mode: str | None = None,
        disable_notification: bool = False,
        filename: str = "file.bin",
        content_type: str = "application/octet-stream",
        escape: bool = True,
    ) -> int | None:
        """
        document: local file path (str) or raw bytes.
        returns message_id or None
        """
        if not self.cfg.enabled:
            return None

        bot_chat = chat_id or self.cfg.chat_id
        if not self.cfg.bot_token or not bot_chat:
            log.warning(
                "TG_MISSING_CONFIG",
                extra={"has_token": bool(self.cfg.bot_token), "has_chat": bool(bot_chat)},
            )
            return None

        pm = _parse_mode(parse_mode or self.cfg.parse_mode)
        tid = (
            thread_id
            if thread_id is not None
            else (int(self.cfg.thread_id) if str(self.cfg.thread_id).strip() else None)
        )

        url = _endpoint(self.cfg.bot_token, "sendDocument")
        s = await self._ensure_session()

        for attempt in range(1, int(self.cfg.retries) + 1):
            await self._gate.wait()
            try:
                form = aiohttp.FormData()
                form.add_field("chat_id", bot_chat)
                form.add_field("disable_notification", str(bool(disable_notification)).lower())
                if tid is not None:
                    form.add_field("message_thread_id", str(int(tid)))

                if caption:
                    cap = caption
                    if pm == "MarkdownV2" and escape:
                        cap = escape_markdown_v2(cap)
                    form.add_field("caption", cap[:1024])
                    form.add_field("parse_mode", pm)

                if isinstance(document, bytes | bytearray):
                    form.add_field(
                        "document", bytes(document), filename=filename, content_type=content_type
                    )
                    async with s.post(url, data=form) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            if data.get("ok") is True:
                                bump("tg_doc_ok")
                                return int(data["result"]["message_id"])
                        if resp.status == 429:
                            bump("tg_429")
                            ra = 1
                            try:
                                data = await resp.json()
                                ra = int((data.get("parameters") or {}).get("retry_after") or 1)
                            except Exception:
                                pass
                            await asyncio.sleep(min(max(1, ra), 8))
                            continue
                        body = await resp.text()
                        log.warning(
                            "TG_DOC_FAIL", extra={"status": resp.status, "body": body[:350]}
                        )
                else:
                    path = str(document)
                    f = open(path, "rb")
                    try:
                        form.add_field("document", f, filename=os.path.basename(path) or filename)
                        async with s.post(url, data=form) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                if data.get("ok") is True:
                                    bump("tg_doc_ok")
                                    return int(data["result"]["message_id"])
                            if resp.status == 429:
                                bump("tg_429")
                                ra = 1
                                try:
                                    data = await resp.json()
                                    ra = int((data.get("parameters") or {}).get("retry_after") or 1)
                                except Exception:
                                    pass
                                await asyncio.sleep(min(max(1, ra), 8))
                                continue
                            body = await resp.text()
                            log.warning(
                                "TG_DOC_FAIL", extra={"status": resp.status, "body": body[:350]}
                            )
                    finally:
                        with contextlib.suppress(Exception):
                            f.close()

            except Exception as e:
                log.warning("TG_DOC_EXC", extra={"attempt": attempt, "err": str(e)[:240]})
                bump("tg_doc_exc")

            await asyncio.sleep(float(self.cfg.backoff_base) * attempt)

        return None


# ──────────────────────────────────────────────────────────────────────────────
# Module-level default client (backwards compatible API)
# ──────────────────────────────────────────────────────────────────────────────
_DEFAULT_CLIENT: TelegramClient | None = None
_DEFAULT_LOCK = asyncio.Lock()


async def _get_default_client() -> TelegramClient:
    global _DEFAULT_CLIENT
    if _DEFAULT_CLIENT is not None:
        return _DEFAULT_CLIENT
    async with _DEFAULT_LOCK:
        if _DEFAULT_CLIENT is None:
            _DEFAULT_CLIENT = TelegramClient()
        return _DEFAULT_CLIENT


async def close_session() -> None:
    """
    Backwards compatible. Closes the module-level default client session (if owned).
    """
    global _DEFAULT_CLIENT
    async with _DEFAULT_LOCK:
        if _DEFAULT_CLIENT is not None:
            await _DEFAULT_CLIENT.close()
        _DEFAULT_CLIENT = None


# ──────────────────────────────────────────────────────────────────────────────
# Backwards-compatible wrappers
# ──────────────────────────────────────────────────────────────────────────────
async def send_message(
    text: str,
    *,
    chat_id: str | None = None,
    parse_mode: str | None = None,
    disable_web_page_preview: bool = True,
    disable_notification: bool = False,
    reply_to_message_id: int | None = None,
    thread_id: int | None = None,
    buttons: Sequence[Sequence[dict[str, str]]] | None = None,
    protect_content: bool = False,
    bot_token: str | None = None,
) -> list[int]:
    """
    Backwards compatible entrypoint.

    Note: bot_token override is supported by rebuilding a temporary config
    (no global mutation).
    """
    c = await _get_default_client()
    if bot_token and bot_token != c.cfg.bot_token:
        tmp = TelegramClient(cfg=TelegramConfig(bot_token=bot_token, chat_id=c.cfg.chat_id))
        try:
            return await tmp.send_message(
                text,
                chat_id=chat_id,
                thread_id=thread_id,
                parse_mode=parse_mode,
                disable_web_page_preview=disable_web_page_preview,
                disable_notification=disable_notification,
                protect_content=protect_content,
                reply_to_message_id=reply_to_message_id,
                buttons=buttons,
            )
        finally:
            await tmp.close()

    return await c.send_message(
        text,
        chat_id=chat_id,
        thread_id=thread_id,
        parse_mode=parse_mode,
        disable_web_page_preview=disable_web_page_preview,
        disable_notification=disable_notification,
        protect_content=protect_content,
        reply_to_message_id=reply_to_message_id,
        buttons=buttons,
    )


async def send_photo(
    photo: str | bytes | bytearray,
    *,
    caption: str | None = None,
    chat_id: str | None = None,
    parse_mode: str | None = None,
    disable_notification: bool = False,
    thread_id: int | None = None,
    buttons: Sequence[Sequence[dict[str, str]]] | None = None,
    bot_token: str | None = None,
) -> int | None:
    c = await _get_default_client()
    if bot_token and bot_token != c.cfg.bot_token:
        tmp = TelegramClient(cfg=TelegramConfig(bot_token=bot_token, chat_id=c.cfg.chat_id))
        try:
            return await tmp.send_photo(
                photo,
                caption=caption,
                chat_id=chat_id,
                thread_id=thread_id,
                parse_mode=parse_mode,
                disable_notification=disable_notification,
                buttons=buttons,
            )
        finally:
            await tmp.close()

    return await c.send_photo(
        photo,
        caption=caption,
        chat_id=chat_id,
        thread_id=thread_id,
        parse_mode=parse_mode,
        disable_notification=disable_notification,
        buttons=buttons,
    )


async def send_document(
    document: str | bytes | bytearray,
    *,
    caption: str | None = None,
    chat_id: str | None = None,
    parse_mode: str | None = None,
    disable_notification: bool = False,
    thread_id: int | None = None,
    bot_token: str | None = None,
) -> int | None:
    c = await _get_default_client()
    if bot_token and bot_token != c.cfg.bot_token:
        tmp = TelegramClient(cfg=TelegramConfig(bot_token=bot_token, chat_id=c.cfg.chat_id))
        try:
            return await tmp.send_document(
                document,
                caption=caption,
                chat_id=chat_id,
                thread_id=thread_id,
                parse_mode=parse_mode,
                disable_notification=disable_notification,
            )
        finally:
            await tmp.close()

    return await c.send_document(
        document,
        caption=caption,
        chat_id=chat_id,
        thread_id=thread_id,
        parse_mode=parse_mode,
        disable_notification=disable_notification,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Convenience wrappers (keep names used elsewhere)
# ──────────────────────────────────────────────────────────────────────────────
async def notify_trade_fill(
    symbol: str, side: str, size_sol: float, price: float, tx: str | None = None
) -> None:
    side_u = (side or "").strip().lower()
    side_emoji = "🟢 BUY" if side_u.startswith("b") else "🔴 SELL"
    lines = [
        f"{side_emoji} {symbol}",
        f"sizeSOL={size_sol:.6f}",
        f"priceSOL={price:.10f}",
    ]
    if tx:
        lines.append(f"tx={tx}")
    await send_message("\n".join(lines), parse_mode=_parse_mode(None))


async def notify_error(context: str, err: str) -> None:
    pm = _parse_mode(None)
    ctx = (context or "ERROR").strip()
    e = (err or "").strip()
    if pm == "HTML":
        # Keep it safe; no fancy HTML needed.
        msg = f"⚠️ <b>{ctx}</b>\n<pre>{_clip(e, 3000)}</pre>"
        await send_message(msg, parse_mode="HTML")
    else:
        # Markdown / MarkdownV2: avoid code fences that can break formatting; keep it simple.
        msg = f"⚠️ {ctx}\n{_clip(e, 3500)}"
        await send_message(msg, parse_mode=pm)


def _clip(s: str, n: int) -> str:
    s = (s or "").strip()
    return s if len(s) <= n else s[: max(0, n - 1)] + "…"


__all__ = [
    "TelegramClient",
    "TelegramConfig",
    "close_session",
    "escape_markdown_v2",
    "notify_error",
    "notify_trade_fill",
    "send_document",
    "send_message",
    "send_photo",
]
