from __future__ import annotations

"""
utils_pkg/pumpportal_client.py

PumpPortal websocket client — V3+++ (production / Render-safe)
==============================================================

Role
----
A resilient websocket helper for PumpPortal (pumpportal.fun) used by scanners
and optional standalone tools.

Design constraints (LOCK-IN)
----------------------------
- No network work at import time
- Backwards compatible with current main.py usage:
    * client = PumpPortalClient(...)
    * log client.masked_uri()
    * main.py manages the websocket connection itself
    * main.py calls: await client._subscribe(ws) after connect
- ALSO supports self-managed mode via `events()` async generator
- Robust to wrong `ws_like` passed (string / None / wrong object): no hard crash
- Exponential backoff with jitter, bounded
- Chunking subscriptions for large mint lists
- Defensive JSON parsing & message normalization

Env contract (compatible + extended)
------------------------------------
Required:
- PF_PP_KEY (or PUMPPORTAL_KEY)         : API key

Optional:
- PF_PP_URI (or PUMPPORTAL_URI)         : base wss uri (default: wss://pumpportal.fun/api/data)
- PF_PP_ACCOUNT (or PUMPPORTAL_ACCOUNT) : account pubkey to subscribe
- PF_PP_MINTS (or PUMPPORTAL_MINTS)     : comma-separated mint list

Websocket knobs:
- PF_PP_WS_PING_INTERVAL   (default 20.0)
- PF_PP_WS_PING_TIMEOUT    (default 30.0)
- PF_PP_WS_CLOSE_TIMEOUT   (default 5.0)
- PF_PP_WS_MAX_SIZE        (default 2**23 ~ 8MB)
- PF_PP_WS_CONNECT_TIMEOUT (default 15.0)
- PF_PP_WS_BACKOFF_MAX     (default 30.0)
- PF_PP_WS_BACKOFF_BASE    (default 0.8)
- PF_PP_WS_BACKOFF_JITTER  (default 0.25)

Subscription knobs:
- PF_PP_SUB_CHUNK_SIZE     (default 120)
- PF_PP_VALIDATE_MINTS     (default 1)
- PF_PP_RESUB_COOLDOWN_SEC (default 5.0)
"""

import asyncio
import contextlib
import json
import logging
import os
import random
import re
import time
from collections.abc import AsyncGenerator, AsyncIterator, Iterable
from dataclasses import dataclass
from typing import Any, Protocol, cast
from urllib.parse import quote_plus

import websockets


# -----------------------------------------------------------------------------
# Logging (NO monkeypatching; safe shim)
# -----------------------------------------------------------------------------
class _LogShim:
    __slots__ = ("_supplier", "_store")

    def __init__(self, supplier) -> None:
        self._supplier = supplier
        self._store: dict[str, float] = {}

    def _log(self) -> Any:
        with contextlib.suppress(Exception):
            return self._supplier()
        return None

    def _emit_fallback(self, level: str, msg: str) -> None:
        lg = self._log()
        if lg is None:
            return
        fn = getattr(lg, level, None)
        if callable(fn):
            with contextlib.suppress(Exception):
                fn(msg)
                return

    def event(self, name: str, **fields: Any) -> None:
        lg = self._log()
        if lg is None:
            return

        fn = getattr(lg, "event", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                fn(name, **fields)
                return

        # fallback to stdlib logging-style info
        info = getattr(lg, "info", None)
        if callable(info):
            with contextlib.suppress(Exception):
                info("%s %s", name, fields)
                return

        self._emit_fallback("info", f"{name} {fields}")

    def every(self, key: str, n: int = 30, msg: str | None = None, **fields: Any) -> None:
        try:
            now = time.monotonic()
            sec = float(max(1, int(n)))
            last = float(self._store.get(key, 0.0))
            if (now - last) >= sec:
                self._store[key] = now
                self.event(msg or key, **fields)
        except Exception:
            return

    def info(self, msg: str) -> None:
        self._emit_fallback("info", msg)

    def warning(self, msg: str) -> None:
        self._emit_fallback("warning", msg)

    def error(self, msg: str) -> None:
        self._emit_fallback("error", msg)


_DEFAULT_LOGGER: Any | None = None


def _default_logger_supplier() -> Any:
    global _DEFAULT_LOGGER
    if _DEFAULT_LOGGER is not None:
        return _DEFAULT_LOGGER

    # Prefer repo log_compat
    with contextlib.suppress(Exception):
        from utils_pkg.log_compat import get_logger as gl  # type: ignore

        _DEFAULT_LOGGER = gl(__name__)
        return _DEFAULT_LOGGER

    # fallback to logging_setup
    with contextlib.suppress(Exception):
        from utils_pkg.logging_setup import get_logger as gl2  # type: ignore

        _DEFAULT_LOGGER = gl2(__name__)
        return _DEFAULT_LOGGER

    lg = logging.getLogger(__name__)
    if not lg.handlers:
        logging.basicConfig(level=logging.INFO)
    _DEFAULT_LOGGER = lg
    return _DEFAULT_LOGGER


LOG = _LogShim(_default_logger_supplier)


# -----------------------------------------------------------------------------
# Minimal websocket-like protocol (avoids websockets version/type drift)
# -----------------------------------------------------------------------------
class _WsSendOnly(Protocol):
    async def send(self, data: str) -> Any:  # pragma: no cover
        ...


class _WsIterable(_WsSendOnly, Protocol):
    def __aiter__(self) -> AsyncIterator[Any]:  # pragma: no cover
        ...

    async def close(self, code: int = 1000) -> Any:  # pragma: no cover
        ...


# -----------------------------------------------------------------------------
# Env helpers
# -----------------------------------------------------------------------------
_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _env_any(names: list[str], default: str = "") -> str:
    for n in names:
        v = os.getenv(n)
        if v is not None and v.strip():
            return v.strip()
    return default


def _envi(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    s = raw.strip()
    if not s:
        return default
    try:
        return int(float(s))
    except Exception:
        return default


def _envf(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    s = raw.strip()
    if not s:
        return default
    try:
        return float(s)
    except Exception:
        return default


def _envb(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    s = raw.strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return default


# -----------------------------------------------------------------------------
# Knobs
# -----------------------------------------------------------------------------
_WS_PING_INTERVAL = _envf("PF_PP_WS_PING_INTERVAL", 20.0)
_WS_PING_TIMEOUT = _envf("PF_PP_WS_PING_TIMEOUT", 30.0)
_WS_CLOSE_TIMEOUT = _envf("PF_PP_WS_CLOSE_TIMEOUT", 5.0)
_WS_MAX_SIZE = _envi("PF_PP_WS_MAX_SIZE", 2**23)  # ~8MB
_WS_CONNECT_TIMEOUT = _envf("PF_PP_WS_CONNECT_TIMEOUT", 15.0)

_WS_BACKOFF_MAX = _envf("PF_PP_WS_BACKOFF_MAX", 30.0)
_WS_BACKOFF_BASE = _envf("PF_PP_WS_BACKOFF_BASE", 0.8)
_WS_BACKOFF_JITTER = _envf("PF_PP_WS_BACKOFF_JITTER", 0.25)

_SUB_CHUNK_SIZE = max(10, _envi("PF_PP_SUB_CHUNK_SIZE", 120))
_VALIDATE_MINTS = _envb("PF_PP_VALIDATE_MINTS", True)
_RESUB_COOLDOWN_SEC = max(0.5, _envf("PF_PP_RESUB_COOLDOWN_SEC", 5.0))

# base58 (Solana mints/accounts)
_BASE58 = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,64}$")


def _valid_base58(x: str) -> bool:
    return isinstance(x, str) and _BASE58.match(x) is not None


def _dedup_keep_order(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in items:
        s = (x or "").strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _chunk(items: list[str], n: int) -> list[list[str]]:
    if n <= 0:
        return [items]
    return [items[i : i + n] for i in range(0, len(items), n)]


def _safe_json_loads(raw: str | bytes | bytearray) -> dict[str, Any] | None:
    try:
        if isinstance(raw, (bytes, bytearray)):
            s = raw.decode("utf-8", "ignore")
        else:
            s = str(raw)
        s = s.strip()
        if not s:
            return None
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _now_ts() -> int:
    return int(time.time())


@dataclass(frozen=True, slots=True)
class PumpPortalEvent:
    """
    Normalized event emitted by `events()`.

    kind:
      - new_token  (txType=create)
      - trade      (txType=buy/sell)
      - info       (message present)
      - warning    (errors present)
      - unknown    (fallback)
    """

    kind: str
    ts: int
    raw: dict[str, Any]


class PumpPortalClient:
    """
    PumpPortal websocket helper.

    Two modes:
      (A) EXTERNAL WS MODE (used by main.py):
          - You create client
          - You connect websocket elsewhere
          - You call: await client._subscribe(ws)

      (B) SELF-MANAGED MODE (tools/tests):
          - Iterate: async for ev in client.events(): ...
          - Client handles reconnect/backoff
    """

    def __init__(
        self,
        api_key: str | None = None,
        *,
        account: str | None = None,
        tokens: list[str] | None = None,
        uri: str | None = None,
        logger: Any | None = None,
    ) -> None:
        # Wrap any provided logger (duck-typed) in a shim; no monkeypatching.
        if logger is not None:
            self._log = _LogShim(lambda: logger)
        else:
            self._log = LOG

        self.api_key: str = (api_key or _env_any(["PF_PP_KEY", "PUMPPORTAL_KEY"], "")).strip()
        if not self.api_key:
            raise RuntimeError("PumpPortal API key missing: set PF_PP_KEY (or PUMPPORTAL_KEY)")

        self.account: str = (
            account or _env_any(["PF_PP_ACCOUNT", "PUMPPORTAL_ACCOUNT"], "")
        ).strip()

        mints_env = _env_any(["PF_PP_MINTS", "PUMPPORTAL_MINTS"], "")
        env_tokens = [t.strip() for t in mints_env.split(",") if t.strip()]
        toks = list(tokens) if tokens is not None else env_tokens
        toks = _dedup_keep_order(toks)

        if _VALIDATE_MINTS:
            toks = [t for t in toks if _valid_base58(t)]
            if self.account and not _valid_base58(self.account):
                self._log.warning(
                    "pumpportal: PF_PP_ACCOUNT does not look base58; ignoring account subscription"
                )
                self.account = ""

        self.tokens: list[str] = toks

        base_uri = (
            uri or _env_any(["PF_PP_URI", "PUMPPORTAL_URI"], "")
        ).strip() or "wss://pumpportal.fun/api/data"
        sep = "&" if "?" in base_uri else "?"
        self.uri: str = f"{base_uri}{sep}api-key={quote_plus(self.api_key)}"

        # self-managed ws state
        self._ws: _WsIterable | None = None
        self._closed: bool = False

        # subscription bookkeeping (avoid re-sending identical subs too often)
        self._last_sub_sig: str = ""
        self._last_sub_ts: float = 0.0
        self._sub_lock = asyncio.Lock()

    # ------------------------------------------------------------------ #
    # closed-state helper
    # ------------------------------------------------------------------ #
    def is_closed(self) -> bool:
        # getattr avoids mypy "unreachable" when code evolves.
        return bool(getattr(self, "_closed", False))

    # ------------------------------------------------------------------ #
    # misc helpers
    # ------------------------------------------------------------------ #
    def masked_uri(self) -> str:
        """URI safe for logs (api key masked)."""
        key = self.api_key or ""
        masked = "<hidden>" if len(key) <= 12 else (key[:4] + "..." + key[-4:])
        return re.sub(r"(api-key=)[^&]+", r"\1" + masked, self.uri)

    @staticmethod
    def classify(msg: dict[str, Any]) -> str:
        tx = msg.get("txType")
        if tx == "create":
            return "new_token"
        if tx in ("buy", "sell"):
            return "trade"
        if "errors" in msg:
            return "warning"
        if "message" in msg:
            return "info"
        return "unknown"

    @staticmethod
    def normalize_message(msg: dict[str, Any]) -> dict[str, Any]:
        """
        Normalize a raw PumpPortal payload into a stable dict shape.
        Keep it conservative: do not drop fields.
        """
        out = dict(msg)
        data = out.get("data")
        if isinstance(data, dict):
            for k, v in data.items():
                out.setdefault(k, v)
        if "mint" not in out and isinstance(out.get("token"), str):
            out["mint"] = out["token"]
        return out

    # ------------------------------------------------------------------ #
    # ws normalization helper (external mode)
    # ------------------------------------------------------------------ #
    @staticmethod
    def _normalize_ws(ws_like: Any) -> _WsSendOnly | None:
        if ws_like is None:
            return None
        if isinstance(ws_like, str):
            return None
        send_fn = getattr(ws_like, "send", None)
        if callable(send_fn):
            return cast(_WsSendOnly, ws_like)
        return None

    async def _safe_send(self, ws: _WsSendOnly, payload: dict[str, Any]) -> None:
        try:
            data = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
            await ws.send(data)
        except Exception:
            return

    # ------------------------------------------------------------------ #
    # subscription (EXTERNAL WS MODE)
    # ------------------------------------------------------------------ #
    async def _subscribe(self, ws_like: Any) -> None:
        ws = self._normalize_ws(ws_like)
        if ws is None:
            return

        sig = f"acct={self.account}|mints={','.join(self.tokens)}"

        async with self._sub_lock:
            now = time.time()
            if sig == self._last_sub_sig and (now - self._last_sub_ts) < _RESUB_COOLDOWN_SEC:
                return
            self._last_sub_sig = sig
            self._last_sub_ts = now

        # Always subscribe to new token stream
        await self._safe_send(ws, {"method": "subscribeNewToken"})

        # token trade streams (chunked)
        if self.tokens:
            for batch in _chunk(self.tokens, _SUB_CHUNK_SIZE):
                await self._safe_send(ws, {"method": "subscribeTokenTrade", "keys": batch})

        # account trade stream
        if self.account:
            await self._safe_send(ws, {"method": "subscribeAccountTrade", "keys": [self.account]})

    # ------------------------------------------------------------------ #
    # lifecycle for SELF-MANAGED MODE
    # ------------------------------------------------------------------ #
    async def __aenter__(self) -> PumpPortalClient:
        self._closed = False
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: Any,
    ) -> None:
        await self.close()

    async def close(self) -> None:
        self._closed = True
        ws = self._ws
        self._ws = None
        if ws is None:
            return

        close_fn = getattr(ws, "close", None)
        if not callable(close_fn):
            return

        try:
            try:
                await asyncio.wait_for(close_fn(code=1000), timeout=_WS_CLOSE_TIMEOUT)  # type: ignore[misc]
            except TypeError:
                await asyncio.wait_for(close_fn(1000), timeout=_WS_CLOSE_TIMEOUT)  # type: ignore[misc]
        except Exception:
            return

    # ------------------------------------------------------------------ #
    # connect/backoff helpers
    # ------------------------------------------------------------------ #
    def _backoff_sleep(self, attempt: int) -> float:
        exp = _WS_BACKOFF_BASE * (1.7 ** max(0, attempt - 1))
        base = min(_WS_BACKOFF_MAX, exp)
        return min(_WS_BACKOFF_MAX, base + random.uniform(0.0, _WS_BACKOFF_JITTER))

    def _connect_cm(self) -> Any:
        """
        Build a websockets.connect(...) context manager across websockets versions.

        Some versions support open_timeout; some don't. We try with it first,
        then retry without if TypeError.
        """
        connect_fn = cast(Any, websockets.connect)
        kwargs: dict[str, Any] = dict(
            ping_interval=_WS_PING_INTERVAL,
            ping_timeout=_WS_PING_TIMEOUT,
            close_timeout=_WS_CLOSE_TIMEOUT,
            max_size=_WS_MAX_SIZE,
            open_timeout=_WS_CONNECT_TIMEOUT,
        )
        try:
            return connect_fn(self.uri, **kwargs)
        except TypeError:
            kwargs.pop("open_timeout", None)
            return connect_fn(self.uri, **kwargs)

    # ------------------------------------------------------------------ #
    # SELF-MANAGED event stream
    # ------------------------------------------------------------------ #
    async def events(self) -> AsyncGenerator[PumpPortalEvent, None]:
        attempt = 0

        while True:
            if self.is_closed():
                return

            cm = self._connect_cm()
            try:
                async with cm as ws_any:
                    if self.is_closed():
                        return

                    ws = cast(_WsIterable, ws_any)
                    self._ws = ws
                    attempt = 0  # reset after successful connect

                    self._log.event("PUMPPORTAL_WS_CONNECTED", uri=self.masked_uri())
                    await self._subscribe(ws)

                    async for raw in ws:
                        if self.is_closed():
                            return

                        msg = _safe_json_loads(raw)
                        if msg is None:
                            continue

                        # Known API-key warning noise sometimes present; ignore but log occasionally.
                        if (
                            msg.get("errors")
                            == "Invalid API key. PumpSwap data will not be streamed."
                        ):
                            self._log.every(
                                "PUMPPORTAL_INVALID_KEY", n=60, msg="PUMPPORTAL_INVALID_KEY"
                            )
                            continue

                        norm = self.normalize_message(msg)
                        yield PumpPortalEvent(kind=self.classify(norm), ts=_now_ts(), raw=norm)

            except asyncio.CancelledError:
                return
            except Exception as e:
                self._log.every(
                    "PUMPPORTAL_WS_ERR",
                    n=10,
                    msg="PUMPPORTAL_WS_ERR",
                    err=type(e).__name__,
                )
            finally:
                self._ws = None

            if self.is_closed():
                return

            attempt = max(1, attempt + 1)
            await asyncio.sleep(self._backoff_sleep(attempt))

    # ------------------------------------------------------------------ #
    # Mutators (optional)
    # ------------------------------------------------------------------ #
    def set_account(self, account: str) -> None:
        account = (account or "").strip()
        if _VALIDATE_MINTS and account and not _valid_base58(account):
            raise ValueError("Account does not look like a base58 Solana pubkey")
        self.account = account

    def set_tokens(self, tokens: list[str]) -> None:
        toks = _dedup_keep_order(tokens or [])
        if _VALIDATE_MINTS:
            toks = [t for t in toks if _valid_base58(t)]
        self.tokens = toks


__all__ = ["PumpPortalClient", "PumpPortalEvent"]
