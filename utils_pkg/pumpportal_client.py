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
"""

import asyncio
import contextlib
import json
import logging
import os
import random
import re
import time
from collections.abc import AsyncGenerator, Iterable
from dataclasses import dataclass
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosed, ConnectionClosedError, ConnectionClosedOK

# Type import drift across websockets versions
try:  # websockets<=11 legacy path
    from websockets.legacy.client import WebSocketClientProtocol as WSProto  # type: ignore
except Exception:  # websockets>=12
    try:
        from websockets.client import ClientConnection as WSProto  # type: ignore
    except Exception:  # fallback structural
        WSProto = Any  # type: ignore


# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
try:
    from utils_pkg.logging_setup import get_logger as _get_logger  # preferred
except Exception:  # pragma: no cover
    try:
        from logging_setup import get_logger as _get_logger  # root shim
    except Exception:  # pragma: no cover
        _get_logger = None  # type: ignore

if _get_logger:
    try:
        LOG = _get_logger("utils.pumpportal")  # type: ignore[misc]
    except TypeError:  # pragma: no cover
        LOG = _get_logger()  # type: ignore[misc]
else:
    LOG = logging.getLogger("utils.pumpportal")
    if not LOG.handlers:
        logging.basicConfig(level=logging.INFO)


# -----------------------------------------------------------------------------
# Env helpers
# -----------------------------------------------------------------------------
_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _env_any(names: list[str], default: str = "") -> str:
    for n in names:
        v = os.getenv(n)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return default


def _envi(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(float(v)) if v not in (None, "") else default
    except Exception:
        return default


def _envf(name: str, default: float) -> float:
    v = os.getenv(name)
    try:
        return float(v) if v not in (None, "") else default
    except Exception:
        return default


def _envb(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    s = str(v).strip().lower()
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

# base58 (Solana mints / accounts)
_BASE58 = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,64}$")


def _valid_base58(x: str) -> bool:
    return isinstance(x, str) and _BASE58.match(x) is not None


def _dedup_keep_order(items: Iterable[str]) -> list[str]:
    seen = set()
    out: list[str] = []
    for x in items:
        x = (x or "").strip()
        if not x:
            continue
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def _chunk(items: list[str], n: int) -> list[list[str]]:
    if n <= 0:
        return [items]
    return [items[i : i + n] for i in range(0, len(items), n)]


def _safe_json_loads(raw: str | bytes) -> Any | None:
    try:
        s = raw.decode("utf-8", "ignore") if isinstance(raw, bytes | bytearray) else str(raw)
        s = s.strip()
        if not s:
            return None
        return json.loads(s)
    except Exception:
        return None


@dataclass(frozen=True)
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


# -----------------------------------------------------------------------------
# Client
# -----------------------------------------------------------------------------
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
        logger: logging.Logger | None = None,
    ) -> None:
        self._log = logger or LOG

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
        # Ensure api-key is present as query param (PumpPortal expects "api-key=")
        sep = "&" if "?" in base_uri else "?"
        self.uri: str = f"{base_uri}{sep}api-key={self.api_key}"

        # self-managed ws state
        self._ws: WSProto | None = None
        self._closed: bool = False

        # subscription bookkeeping (avoid re-sending identical subs too often)
        self._last_sub_sig: str = ""
        self._last_sub_ts: float = 0.0
        self._sub_lock = asyncio.Lock()

    # ------------------------------------------------------------------ #
    # misc helpers
    # ------------------------------------------------------------------ #
    def masked_uri(self) -> str:
        """URI safe for logs (api key masked)."""
        key = self.api_key or ""
        masked = "<hidden>" if len(key) <= 12 else key[:4] + "..." + key[-4:]

        # Replace only the value of api-key=
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

    # ------------------------------------------------------------------ #
    # ws normalization helper (external mode)
    # ------------------------------------------------------------------ #
    @staticmethod
    def _normalize_ws(ws_like: Any) -> Any | None:
        """
        Normalize ws-like objects coming from other modules.

        Returns:
          - websocket protocol-ish object with `.send` coroutine
          - or None if unusable
        """
        if ws_like is None:
            return None
        if isinstance(ws_like, str):
            # common footgun: passing URI instead of protocol
            return None
        if hasattr(ws_like, "send"):
            return ws_like
        return None

    # ------------------------------------------------------------------ #
    # subscription (EXTERNAL WS MODE)
    # ------------------------------------------------------------------ #
    async def _subscribe(self, ws_like: Any) -> None:
        """
        Subscribe a CONNECTED websocket to PumpPortal channels.

        This method is intentionally forgiving; it will no-op if passed a bad ws.
        """
        ws = self._normalize_ws(ws_like)
        if ws is None:
            return

        # Create a subscription signature so we don't spam re-sends on reconnect loops
        sig = f"acct={self.account}|mints={','.join(self.tokens)}"
        async with self._sub_lock:
            now = time.time()
            # allow re-subscribe if signature changed or enough time passed
            if sig == self._last_sub_sig and (now - self._last_sub_ts) < 5.0:
                return
            self._last_sub_sig = sig
            self._last_sub_ts = now

        # Always subscribe to new token stream
        await self._safe_send(ws, {"method": "subscribeNewToken"})

        # Token trade stream (chunked)
        if self.tokens:
            for batch in _chunk(self.tokens, _SUB_CHUNK_SIZE):
                await self._safe_send(ws, {"method": "subscribeTokenTrade", "keys": batch})

        # Account trade stream
        if self.account:
            await self._safe_send(ws, {"method": "subscribeAccountTrade", "keys": [self.account]})

    async def _safe_send(self, ws: Any, payload: dict[str, Any]) -> None:
        try:
            await ws.send(json.dumps(payload, separators=(",", ":"), ensure_ascii=False))
        except Exception:
            # don't crash the outer loop; caller handles reconnect
            return

    # ------------------------------------------------------------------ #
    # lifecycle for SELF-MANAGED MODE
    # ------------------------------------------------------------------ #
    async def __aenter__(self) -> PumpPortalClient:
        self._closed = False
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def close(self) -> None:
        """Close any internally managed websocket (best-effort)."""
        self._closed = True
        ws = self._ws
        self._ws = None
        if ws is None:
            return
        try:
            await asyncio.wait_for(ws.close(code=1000), timeout=_WS_CLOSE_TIMEOUT)  # type: ignore[attr-defined]
        except Exception:
            pass

    async def _open_ws(self) -> tuple[Any, Any]:
        """
        Open websocket using websockets.connect as an async CM, with a connect timeout.
        Returns (ws, connect_cm) where connect_cm is the context manager to close.
        """
        connect_cm = websockets.connect(
            self.uri,
            ping_interval=_WS_PING_INTERVAL,
            ping_timeout=_WS_PING_TIMEOUT,
            close_timeout=_WS_CLOSE_TIMEOUT,
            max_size=_WS_MAX_SIZE,
        )
        # websockets.connect returns an awaitable/async-cm object; use __aenter__/__aexit__
        ws = await asyncio.wait_for(connect_cm.__aenter__(), timeout=_WS_CONNECT_TIMEOUT)
        return ws, connect_cm

    async def _close_ws_cm(self, connect_cm: Any) -> None:
        with contextlib.suppress(Exception):
            await connect_cm.__aexit__(None, None, None)

    def _backoff_sleep(self, attempt: int) -> float:
        base = min(_WS_BACKOFF_MAX, _WS_BACKOFF_BASE * (1.7 ** max(0, attempt - 1)))
        return min(_WS_BACKOFF_MAX, base + random.uniform(0.0, _WS_BACKOFF_JITTER))

    # ------------------------------------------------------------------ #
    # SELF-MANAGED event stream
    # ------------------------------------------------------------------ #
    async def events(self) -> AsyncGenerator[PumpPortalEvent, None]:
        """
        Async generator that:
          - manages its own websocket connection
          - auto-reconnects with exponential backoff + jitter
          - yields PumpPortalEvent(kind, ts, raw_dict)

        NOTE: main sniper loop may not use this; it’s great for tests/tools.
        """
        attempt = 0
        while not self._closed:
            connect_cm = None
            ws = None
            try:
                attempt += 1
                ws, connect_cm = await self._open_ws()
                self._ws = ws
                attempt = 0  # reset after successful connect

                # Subscribe after connect
                await self._subscribe(ws)

                async for raw in ws:
                    if self._closed:
                        break

                    msg_any = _safe_json_loads(raw)
                    if not isinstance(msg_any, dict):
                        continue

                    # Known PumpPortal warning when key invalid for some streams:
                    # "Invalid API key. PumpSwap data will not be streamed."
                    # We keep connection alive; new-token is typically still free.
                    if (
                        msg_any.get("errors")
                        == "Invalid API key. PumpSwap data will not be streamed."
                    ):
                        continue

                    kind = self.classify(msg_any)
                    yield PumpPortalEvent(kind=kind, ts=int(time.time()), raw=msg_any)

            except asyncio.CancelledError:
                break
            except (ConnectionClosedOK, ConnectionClosed, ConnectionClosedError):
                # transient unless we are closing
                pass
            except TimeoutError:
                # connect timeout
                pass
            except Exception:
                # keep resilient; backoff will apply
                pass
            finally:
                self._ws = None
                if connect_cm is not None:
                    await self._close_ws_cm(connect_cm)

            if self._closed:
                break

            # backoff
            sleep_s = self._backoff_sleep(attempt=max(1, attempt))
            await asyncio.sleep(sleep_s)

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
