# utils_pkg/telegram_utils.py
# Async Telegram helpers: send text, photos, and docs with retries, chunking, rate limiting, and 429 handling.

from __future__ import annotations

import os
import asyncio
import logging
import time
from typing import Any, Dict, List, Optional, Sequence, Union

import aiohttp

logger = logging.getLogger("telegram_utils")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

# ---------------- Env / Defaults ----------------

TELEGRAM_ENABLED = os.getenv("TELEGRAM_ENABLED", "true").lower() == "true"
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_THREAD_ID = os.getenv("TELEGRAM_THREAD_ID")  # optional forum topic id
TELEGRAM_PARSE_MODE = os.getenv("TELEGRAM_PARSE_MODE", "Markdown")  # Markdown | MarkdownV2 | HTML
TELEGRAM_TIMEOUT = float(os.getenv("TELEGRAM_TIMEOUT", "15"))
TELEGRAM_RPS = float(os.getenv("TELEGRAM_RPS", "1.0"))  # requests per second cap
TELEGRAM_RETRIES = int(os.getenv("TELEGRAM_RETRIES", "3"))
TELEGRAM_BACKOFF_BASE = float(os.getenv("TELEGRAM_BACKOFF_BASE", "0.75"))
TELEGRAM_API_BASE = os.getenv("TELEGRAM_API_BASE", "https://api.telegram.org")

TG_MAX_TEXT = 4096  # hard limit per message

# ---------------- Globals ----------------

_SESSION: Optional[aiohttp.ClientSession] = None
_SESSION_LOCK = asyncio.Lock()
_RATE_LOCK = asyncio.Lock()
_NEXT_AVAILABLE_TS = 0.0

__all__ = [
    "send_message",
    "send_photo",
    "send_document",
    "notify_trade_fill",
    "notify_error",
    "close_session",
]

# ---------------- Session / Rate limiting ----------------


async def _ensure_session() -> aiohttp.ClientSession:
    global _SESSION
    async with _SESSION_LOCK:
        if _SESSION and not _SESSION.closed:
            return _SESSION
        timeout = aiohttp.ClientTimeout(total=TELEGRAM_TIMEOUT)
        _SESSION = aiohttp.ClientSession(timeout=timeout)
        return _SESSION


async def close_session():
    global _SESSION
    async with _SESSION_LOCK:
        if _SESSION and not _SESSION.closed:
            await _SESSION.close()
        _SESSION = None


async def _rate_gate():
    """Very simple RPS gate to keep below TELEGRAM_RPS."""
    if TELEGRAM_RPS <= 0:
        return
    global _NEXT_AVAILABLE_TS
    async with _RATE_LOCK:
        now = time.monotonic()
        min_interval = 1.0 / TELEGRAM_RPS
        if now < _NEXT_AVAILABLE_TS:
            await asyncio.sleep(_NEXT_AVAILABLE_TS - now)
        _NEXT_AVAILABLE_TS = time.monotonic() + min_interval


# ---------------- Utils ----------------


def escape_markdown(text: str) -> str:
    """Lightweight MarkdownV2 escape (only when parse mode is MarkdownV2)."""
    if TELEGRAM_PARSE_MODE != "MarkdownV2":
        return text
    specials = set("_*[]()~`>#+-=|{}.!")
    out_chars: List[str] = []
    for ch in text:
        if ch in specials:
            out_chars.append("\\" + ch)
        else:
            out_chars.append(ch)
    return "".join(out_chars)


def _chunk_text(text: str, limit: int = TG_MAX_TEXT) -> List[str]:
    """Split text safely under Telegram's 4096 char limit."""
    if len(text) <= limit:
        return [text]

    parts: List[str] = []
    # Prefer splitting on paragraph boundaries, then lines, then hard cuts
    for para in text.split("\n\n"):
        if len(para) + 2 <= limit:
            parts.append(para)
            continue
        # too big — split by lines
        buf = ""
        for line in para.splitlines():
            candidate = (buf + "\n" + line) if buf else line
            if len(candidate) <= limit:
                buf = candidate
            else:
                if buf:
                    parts.append(buf)
                # if single line is too big, hard chunk
                while len(line) > limit:
                    parts.append(line[:limit])
                    line = line[limit:]
                buf = line
        if buf:
            parts.append(buf)
    return [p[:limit] for p in parts]


def _build_inline_keyboard(
    button_rows: Optional[Sequence[Sequence[Dict[str, str]]]]
) -> Optional[Dict[str, Any]]:
    if not button_rows:
        return None
    # each button: {"text": "...", "url": "..."} or {"text":"...", "callback_data":"..."}
    return {"inline_keyboard": [[btn for btn in row] for row in button_rows]}


def _endpoint(bot_token: str, method: str) -> str:
    base = TELEGRAM_API_BASE.rstrip("/")
    return f"{base}/bot{bot_token}/{method}"


async def _post_json(url: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    session = await _ensure_session()
    for attempt in range(1, TELEGRAM_RETRIES + 1):
        await _rate_gate()
        try:
            async with session.post(url, json=payload) as resp:
                if resp.status == 200:
                    return await resp.json()
                txt = await resp.text()
                # Handle 429 with retry_after
                if resp.status == 429:
                    try:
                        data = await resp.json()
                        retry_after = int(data.get("parameters", {}).get("retry_after", 1))
                    except Exception:
                        retry_after = 1
                    logger.warning(f"[tg] 429; sleeping {retry_after}s (attempt {attempt})")
                    await asyncio.sleep(retry_after)
                    continue
                logger.warning(f"[tg] {url.split('/')[-1]} {resp.status}: {txt}")
        except Exception as e:
            logger.warning(f"[tg] {url.split('/')[-1]} attempt {attempt} failed: {e}")
        await asyncio.sleep(TELEGRAM_BACKOFF_BASE * attempt)
    return None


# ---------------- Core senders ----------------


async def send_message(
    text: str,
    *,
    chat_id: Optional[str] = None,
    parse_mode: Optional[str] = None,
    disable_web_page_preview: bool = True,
    disable_notification: bool = False,
    reply_to_message_id: Optional[int] = None,
    thread_id: Optional[int] = None,
    buttons: Optional[Sequence[Sequence[Dict[str, str]]]] = None,
    protect_content: bool = False,
    bot_token: Optional[str] = None,
) -> List[int]:
    """
    Send a (possibly chunked) text message. Returns list of Telegram message_ids.
    """
    if not TELEGRAM_ENABLED:
        logger.info("[tg] disabled; skipping send_message.")
        return []

    bot_token = bot_token or TELEGRAM_BOT_TOKEN
    chat_id = chat_id or TELEGRAM_CHAT_ID
    if not bot_token or not chat_id:
        logger.warning("[tg] missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
        return []

    url = _endpoint(bot_token, "sendMessage")
    parse_mode = parse_mode or TELEGRAM_PARSE_MODE
    chunks = _chunk_text(text)

    message_ids: List[int] = []
    for idx, chunk in enumerate(chunks):
        payload: Dict[str, Any] = {
            "chat_id": chat_id,
            "text": escape_markdown(chunk),
            "disable_web_page_preview": disable_web_page_preview,
            "disable_notification": disable_notification,
            "protect_content": protect_content,
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode
        if reply_to_message_id and idx == 0:
            payload["reply_to_message_id"] = reply_to_message_id
        if thread_id or TELEGRAM_THREAD_ID:
            payload["message_thread_id"] = int(thread_id or TELEGRAM_THREAD_ID)
        kb = _build_inline_keyboard(buttons)
        if kb:
            payload["reply_markup"] = kb

        data = await _post_json(url, payload)
        if data and data.get("ok"):
            try:
                mid = int(data["result"]["message_id"])
                message_ids.append(mid)
            except Exception:
                # ignore extraction error
                pass
    return message_ids


async def send_photo(
    photo: Union[str, bytes],
    *,
    caption: Optional[str] = None,
    chat_id: Optional[str] = None,
    parse_mode: Optional[str] = None,
    disable_notification: bool = False,
    thread_id: Optional[int] = None,
    buttons: Optional[Sequence[Sequence[Dict[str, str]]]] = None,
    bot_token: Optional[str] = None,
) -> Optional[int]:
    """
    photo: URL (str) or raw bytes
    returns message_id or None
    """
    if not TELEGRAM_ENABLED:
        return None

    bot_token = bot_token or TELEGRAM_BOT_TOKEN
    chat_id = chat_id or TELEGRAM_CHAT_ID
    if not bot_token or not chat_id:
        logger.warning("[tg] missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
        return None

    session = await _ensure_session()
    url = _endpoint(bot_token, "sendPhoto")
    parse_mode = parse_mode or TELEGRAM_PARSE_MODE

    data: Dict[str, Any] = {
        "chat_id": chat_id,
        "disable_notification": disable_notification,
    }
    if caption:
        data["caption"] = escape_markdown(caption)
        data["parse_mode"] = parse_mode
    if thread_id or TELEGRAM_THREAD_ID:
        data["message_thread_id"] = int(thread_id or TELEGRAM_THREAD_ID)
    kb = _build_inline_keyboard(buttons)
    if kb:
        data["reply_markup"] = kb

    for attempt in range(1, TELEGRAM_RETRIES + 1):
        await _rate_gate()
        try:
            if isinstance(photo, (bytes, bytearray)):
                form = aiohttp.FormData()
                form.add_field("chat_id", chat_id)
                form.add_field("disable_notification", str(disable_notification).lower())
                if "caption" in data:
                    form.add_field("caption", data["caption"])
                    form.add_field("parse_mode", parse_mode or "")
                if thread_id or TELEGRAM_THREAD_ID:
                    form.add_field("message_thread_id", str(int(thread_id or TELEGRAM_THREAD_ID)))
                if kb:
                    form.add_field("reply_markup", aiohttp.JsonPayload(kb))
                form.add_field("photo", photo, filename="image.jpg", content_type="image/jpeg")

                async with session.post(url, data=form) as resp:
                    if resp.status == 200:
                        js = await resp.json()
                        return int(js["result"]["message_id"])
                    if resp.status == 429:
                        try:
                            js = await resp.json()
                            ra = int(js.get("parameters", {}).get("retry_after", 1))
                        except Exception:
                            ra = 1
                        logger.warning(f"[tg] sendPhoto 429; sleeping {ra}s")
                        await asyncio.sleep(ra)
                        continue
                    txt = await resp.text()
                    logger.warning(f"[tg] sendPhoto {resp.status}: {txt}")
            else:
                payload = dict(data, photo=photo)
                js = await _post_json(url, payload)
                if js and js.get("ok"):
                    return int(js["result"]["message_id"])
        except Exception as e:
            logger.warning(f"[tg] sendPhoto attempt {attempt} failed: {e}")
        await asyncio.sleep(TELEGRAM_BACKOFF_BASE * attempt)
    return None


async def send_document(
    document: Union[str, bytes],
    *,
    caption: Optional[str] = None,
    chat_id: Optional[str] = None,
    parse_mode: Optional[str] = None,
    disable_notification: bool = False,
    thread_id: Optional[int] = None,
    bot_token: Optional[str] = None,
) -> Optional[int]:
    """
    document: local file path or raw bytes
    returns message_id or None
    """
    if not TELEGRAM_ENABLED:
        return None

    bot_token = bot_token or TELEGRAM_BOT_TOKEN
    chat_id = chat_id or TELEGRAM_CHAT_ID
    if not bot_token or not chat_id:
        logger.warning("[tg] missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
        return None

    session = await _ensure_session()
    url = _endpoint(bot_token, "sendDocument")
    parse_mode = parse_mode or TELEGRAM_PARSE_MODE

    for attempt in range(1, TELEGRAM_RETRIES + 1):
        await _rate_gate()
        try:
            form = aiohttp.FormData()
            form.add_field("chat_id", chat_id)
            form.add_field("disable_notification", str(disable_notification).lower())
            if thread_id or TELEGRAM_THREAD_ID:
                form.add_field("message_thread_id", str(int(thread_id or TELEGRAM_THREAD_ID)))
            if caption:
                form.add_field("caption", escape_markdown(caption))
                form.add_field("parse_mode", parse_mode)

            if isinstance(document, (bytes, bytearray)):
                form.add_field("document", document, filename="file.bin", content_type="application/octet-stream")
            else:
                with open(document, "rb") as f:
                    form.add_field("document", f, filename=os.path.basename(document))

            async with session.post(url, data=form) as resp:
                if resp.status == 200:
                    js = await resp.json()
                    return int(js["result"]["message_id"])
                if resp.status == 429:
                    try:
                        js = await resp.json()
                        ra = int(js.get("parameters", {}).get("retry_after", 1))
                    except Exception:
                        ra = 1
                    logger.warning(f"[tg] sendDocument 429; sleeping {ra}s")
                    await asyncio.sleep(ra)
                    continue
                txt = await resp.text()
                logger.warning(f"[tg] sendDocument {resp.status}: {txt}")
        except Exception as e:
            logger.warning(f"[tg] sendDocument attempt {attempt} failed: {e}")
        await asyncio.sleep(TELEGRAM_BACKOFF_BASE * attempt)
    return None


# ---------------- Convenience wrappers ----------------


async def notify_trade_fill(symbol: str, side: str, size_sol: float, price: float, tx: Optional[str] = None):
    side_emoji = "🟢 BUY" if side.lower() == "buy" else "🔴 SELL"
    lines = [
        f"{side_emoji} {symbol}",
        f"Size: {size_sol:.4f} SOL",
        f"Price: {price:.8f} SOL",
    ]
    if tx:
        lines.append(f"Tx: {tx}")
    await send_message("\n".join(lines))


async def notify_error(context: str, err: str):
    await send_message(f"⚠️ *{context}*\n```\n{err}\n```", parse_mode="Markdown")


# ---------------- Self-test ----------------

if __name__ == "__main__":
    async def _t():
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            print("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID to test.")
            return
        mids = await send_message(
            "Hello from *telegram_utils* — chunking, retries, and 429 handling ✅",
            parse_mode="Markdown",
        )
        print("message ids:", mids)
        await close_session()

    asyncio.run(_t())
