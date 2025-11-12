from __future__ import annotations

import os
import re
import json
import asyncio
from typing import AsyncGenerator, Optional, List, Dict, Any

import websockets
from websockets.exceptions import (
    ConnectionClosed,
    ConnectionClosedError,
    ConnectionClosedOK,
)
from websockets.legacy.client import WebSocketClientProtocol

# ──────────────────────────────────────────────────────────────────────────────
# ENV + small helpers
# ──────────────────────────────────────────────────────────────────────────────

def _envs(k: str, d: str = "") -> str:
    return (os.getenv(k) or d).strip()


def _envi(k: str, d: int) -> int:
    try:
        return int(os.getenv(k, str(d)))
    except Exception:
        return d


def _envf(k: str, d: float) -> float:
    try:
        return float(os.getenv(k, str(d)))
    except Exception:
        return d


# connection / ping knobs
_WS_PING_INTERVAL     = _envf("PF_PP_WS_PING_INTERVAL", 20.0)
_WS_PING_TIMEOUT      = _envf("PF_PP_WS_PING_TIMEOUT", 30.0)
_WS_CLOSE_TIMEOUT     = _envf("PF_PP_WS_CLOSE_TIMEOUT", 5.0)
_WS_MAX_SIZE          = _envi("PF_PP_WS_MAX_SIZE", 2**23)  # ~8MB
_WS_BACKOFF_MAX       = _envf("PF_PP_WS_BACKOFF_MAX", 30.0)
_WS_CONNECT_TIMEOUT   = _envf("PF_PP_WS_CONNECT_TIMEOUT", 15.0)

__all__ = ["PumpPortalClient"]


# ──────────────────────────────────────────────────────────────────────────────
# Client
# ──────────────────────────────────────────────────────────────────────────────

class PumpPortalClient:
    """
    Minimal PumpPortal websocket helper.

    It is designed to work with the current main.py which:
      * constructs a PumpPortalClient
      * logs client.masked_uri()
      * manages the actual websocket connection itself
      * calls client._subscribe(ws) once a connection is open

    Public bits used by the rest of the codebase:

      - masked_uri() -> str
      - uri          -> str
      - account      -> str (optional)
      - tokens       -> list[str] (optional)
      - async _subscribe(ws_like)  # called by scripts/main.py

    There is also an optional `events()` async generator that manages its own
    connection for tests or standalone usage, but main.py does not rely on it.
    """

    # ------------------------------------------------------------------ #
    # construction / config
    # ------------------------------------------------------------------ #
    def __init__(
        self,
        api_key: Optional[str] = None,
        *,
        account: Optional[str] = None,
        tokens: Optional[List[str]] = None,
        uri: Optional[str] = None,
    ) -> None:
        # API key is required
        self.api_key: str = api_key or _envs("PF_PP_KEY")
        if not self.api_key:
            raise RuntimeError("PF_PP_KEY not set")

        # Optional account + token list (comma-separated env)
        self.account: str = account or _envs("PF_PP_ACCOUNT") or ""
        toks_env = [t.strip() for t in _envs("PF_PP_MINTS").split(",") if t.strip()]
        self.tokens: List[str] = list(tokens) if tokens is not None else toks_env

        # Base URI + api-key query param
        base_uri = uri or _envs("PF_PP_URI") or "wss://pumpportal.fun/api/data"
        sep = "&" if "?" in base_uri else "?"
        self.uri: str = f"{base_uri}{sep}api-key={self.api_key}"

        # internal tracking of last ws if we manage it ourselves
        self._ws: Optional[WebSocketClientProtocol] = None
        self._closed: bool = False

    # ------------------------------------------------------------------ #
    # context manager (used when client manages its own ws)
    # ------------------------------------------------------------------ #
    async def __aenter__(self) -> "PumpPortalClient":
        self._closed = False
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def close(self) -> None:
        """Gracefully close any internally managed websocket."""
        self._closed = True
        ws = self._ws
        self._ws = None
        if ws is None:
            return
        try:
            await asyncio.wait_for(ws.close(code=1000), timeout=_WS_CLOSE_TIMEOUT)
        except Exception:
            # best-effort cleanup; underlying transport may already be dead
            pass

    # ------------------------------------------------------------------ #
    # misc helpers
    # ------------------------------------------------------------------ #
    def masked_uri(self) -> str:
        """Return the websocket URI with the api-key masked for logging."""
        def mask(s: str) -> str:
            return s[:4] + "..." + s[-4:] if s and len(s) > 12 else "<hidden>"

        # only mask the value part of api-key
        return re.sub(r"(api-key=)[^&]+", r"\1" + mask(self.api_key), self.uri)

    @staticmethod
    def _classify(msg: Dict[str, Any]) -> str:
        """Classify PumpPortal message into a simple kind string."""
        tx = msg.get("txType")
        if tx == "create":
            return "new_token"
        if tx in ("buy", "sell"):
            return "trade"
        if "message" in msg:
            return "info"
        if "errors" in msg:
            return "warning"
        return "unknown"

    # ------------------------------------------------------------------ #
    # ws normalization helper
    # ------------------------------------------------------------------ #
    @staticmethod
    def _normalize_ws(ws_like: Any) -> Optional[WebSocketClientProtocol]:
        """
        Normalize the `ws` object passed in from main.py.

        - If it's already a WebSocketClientProtocol, return it.
        - If it's None, a string, or anything without `.send`, return None.
          (This avoids `AttributeError: 'str' object has no attribute 'send'`.)
        """
        if isinstance(ws_like, WebSocketClientProtocol):
            return ws_like

        # Common bug: `ws_like` accidentally a URI or sentinel string.
        if isinstance(ws_like, str):
            return None

        # Generic structural check – if there's no send coroutine, we can't use it.
        if not hasattr(ws_like, "send"):
            return None

        # If it looks like a websocket but isn't our type, still try to use it.
        return ws_like  # type: ignore[return-value]

    # ------------------------------------------------------------------ #
    # subscription used by scripts/main.py
    # ------------------------------------------------------------------ #
    async def _subscribe(self, ws_like: Any) -> None:
        """
        Subscribe a *connected* websocket to PumpPortal channels.

        main.py is responsible for:
          - building the websocket connection
          - passing the connection object into this method

        This method is robust against being handed a string or other bad type:
          it will simply no-op instead of raising.
        """
        ws = self._normalize_ws(ws_like)
        if ws is None:
            # Nothing we can do – caller handed us a bad object.
            # We intentionally do NOT raise here to keep the main loop alive.
            return

        # Public new-token stream (free)
        await ws.send(json.dumps({"method": "subscribeNewToken"}))

        # Token-trade stream (optional)
        if self.tokens:
            await ws.send(
                json.dumps(
                    {
                        "method": "subscribeTokenTrade",
                        "keys": self.tokens,
                    }
                )
            )

        # Account-trade stream (optional)
        if self.account:
            await ws.send(
                json.dumps(
                    {
                        "method": "subscribeAccountTrade",
                        "keys": [self.account],
                    }
                )
            )

    # ------------------------------------------------------------------ #
    # Optional self-managed event stream (not used by main.py currently)
    # ------------------------------------------------------------------ #
    async def events(self) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Async generator that:
          - manages its own websocket connection
          - auto-reconnects with exponential backoff
          - yields dicts of the form:
                {"kind": <str>, "raw": <PumpPortal message dict>}

        This is mainly useful for tests or standalone tools.
        The main sniper loop currently uses its own connection and only calls
        `_subscribe(ws)` from this class.
        """
        backoff = 1.0
        while not self._closed:
            try:
                connect_ctx = websockets.connect(
                    self.uri,
                    ping_interval=_WS_PING_INTERVAL,
                    ping_timeout=_WS_PING_TIMEOUT,
                    close_timeout=_WS_CLOSE_TIMEOUT,
                    max_size=_WS_MAX_SIZE,
                )

                # protect connect with timeout
                async with asyncio.timeout(_WS_CONNECT_TIMEOUT):
                    ws = await connect_ctx.__aenter__()

                try:
                    self._ws = ws
                    await self._subscribe(ws)
                    backoff = 1.0  # reset after successful connect

                    async for raw in ws:
                        if self._closed:
                            break

                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        if not isinstance(msg, dict):
                            continue

                        # Known server text when API key is invalid – keep socket
                        if msg.get("errors") == "Invalid API key. PumpSwap data will not be streamed.":
                            # New-token feed is still free; ignore this line.
                            continue

                        yield {"kind": self._classify(msg), "raw": msg}

                finally:
                    self._ws = None
                    try:
                        await connect_ctx.__aexit__(None, None, None)
                    except Exception:
                        pass

            except asyncio.TimeoutError:
                # connect timeout — fall through to backoff
                pass
            except asyncio.CancelledError:
                # caller cancelled iteration
                break
            except (ConnectionClosedOK, ConnectionClosed, ConnectionClosedError):
                if self._closed:
                    break
                # transient; fall through to backoff
            except Exception:
                # Generic transient error — let backoff handle retries.
                pass

            if self._closed:
                break

            # exponential backoff with cap
            await asyncio.sleep(backoff)
            backoff = min(_WS_BACKOFF_MAX, backoff * 1.7)
