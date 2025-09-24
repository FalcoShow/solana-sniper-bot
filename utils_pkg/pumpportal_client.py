# utils_pkg/pumpportal_client.py
from __future__ import annotations

import os
import re
import json
import asyncio
from typing import AsyncGenerator, Optional, List, Dict, Any

import websockets
from websockets.exceptions import ConnectionClosed, ConnectionClosedError, ConnectionClosedOK

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

_WS_PING_INTERVAL = _envf("PF_PP_WS_PING_INTERVAL", 20.0)
_WS_PING_TIMEOUT  = _envf("PF_PP_WS_PING_TIMEOUT", 30.0)
_WS_CLOSE_TIMEOUT = _envf("PF_PP_WS_CLOSE_TIMEOUT", 5.0)
_WS_MAX_SIZE      = _envi("PF_PP_WS_MAX_SIZE", 2**23)  # ~8MB
_WS_BACKOFF_MAX   = _envf("PF_PP_WS_BACKOFF_MAX", 30.0)

# ──────────────────────────────────────────────────────────────────────────────
# Client
# ──────────────────────────────────────────────────────────────────────────────
class PumpPortalClient:
    """
    Minimal PumpPortal websocket client with:
      - Async context manager (prevents resource leaks)
      - Auto-reconnect inside .events()
      - Masked URI for logs
      - Subscriptions: new tokens, optional token trades, optional account trades
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        *,
        account: Optional[str] = None,
        tokens: Optional[List[str]] = None,
        uri: Optional[str] = None,
    ) -> None:
        self.api_key = api_key or _envs("PF_PP_KEY")
        if not self.api_key:
            raise RuntimeError("PF_PP_KEY not set")

        self.account = account or _envs("PF_PP_ACCOUNT") or ""
        self.tokens  = tokens if tokens is not None else [t for t in _envs("PF_PP_MINTS").split(",") if t.strip()]
        base_uri = uri or _envs("PF_PP_URI") or "wss://pumpportal.fun/api/data"
        sep = "&" if "?" in base_uri else "?"
        self.uri = f"{base_uri}{sep}api-key={self.api_key}"

        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._closed = False

    # ── context manager (used by scripts/main.py) ─────────────────────────────
    async def __aenter__(self) -> "PumpPortalClient":
        self._closed = False
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def close(self) -> None:
        """Gracefully close the websocket (if connected) and mark client closed."""
        self._closed = True
        ws = self._ws
        self._ws = None
        if ws is not None:
            try:
                await asyncio.wait_for(ws.close(code=1000), timeout=_WS_CLOSE_TIMEOUT)
            except Exception:
                # swallow; socket may already be gone
                pass

    # ── misc helpers ──────────────────────────────────────────────────────────
    def masked_uri(self) -> str:
        def mask(s: str) -> str:
            return s[:4] + "..." + s[-4:] if s and len(s) > 12 else "<hidden>"
        return re.sub(r"(api-key=)[^&]+", r"\1" + mask(self.api_key), self.uri)

    @staticmethod
    def _classify(msg: Dict[str, Any]) -> str:
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

    async def _subscribe(self, ws: websockets.WebSocketClientProtocol) -> None:
        # Public new-token stream (free)
        await ws.send(json.dumps({"method": "subscribeNewToken"}))
        # Token-trade stream (optional)
        if self.tokens:
            await ws.send(json.dumps({"method": "subscribeTokenTrade", "keys": self.tokens}))
        # Account-trade stream (optional)
        if self.account:
            await ws.send(json.dumps({"method": "subscribeAccountTrade", "keys": [self.account]}))

    # ── main stream ───────────────────────────────────────────────────────────
    async def events(self) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Async generator that auto-reconnects and yields dicts:
            {"kind": "new_token|trade|info|warning|unknown", "raw": <message_dict>}

        Stops when:
          - the consumer cancels iteration,
          - `close()` is called (e.g., context exit),
          - or a fatal connection error occurs during shutdown.
        """
        backoff = 1.0
        while not self._closed:
            try:
                async with websockets.connect(
                    self.uri,
                    ping_interval=_WS_PING_INTERVAL,
                    ping_timeout=_WS_PING_TIMEOUT,
                    close_timeout=_WS_CLOSE_TIMEOUT,
                    max_size=_WS_MAX_SIZE,
                ) as ws:
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

                        # Common server-side complaint for bad API keys — ignore line noise
                        if msg.get("errors") == "Invalid API key. PumpSwap data will not be streamed.":
                            # Keep the socket alive (we still want new_token feed which is free)
                            continue

                        yield {"kind": self._classify(msg), "raw": msg}

            except asyncio.CancelledError:
                # Consumer cancelled; respect it
                break
            except (ConnectionClosedOK, ConnectionClosed, ConnectionClosedError):
                if self._closed:
                    break
                # transient; fall through to backoff/retry
            except Exception:
                # transient; backoff below
                pass
            finally:
                self._ws = None

            if self._closed:
                break
            # backoff with gentle growth
            await asyncio.sleep(backoff)
            backoff = min(_WS_BACKOFF_MAX, backoff * 1.7)
