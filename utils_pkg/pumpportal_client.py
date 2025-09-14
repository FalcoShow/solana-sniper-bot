import os, asyncio, json, re
import websockets

class PumpPortalClient:
    """Minimal PumpPortal websocket client."""
    def __init__(self, api_key: str | None = None, *, account: str | None = None,
                 tokens: list[str] | None = None, uri: str | None = None):
        self.api_key = api_key or os.getenv("PF_PP_KEY")
        if not self.api_key:
            raise RuntimeError("PF_PP_KEY not set")
        self.account = (account or os.getenv("PF_PP_ACCOUNT") or "").strip()
        self.tokens  = tokens or [t for t in (os.getenv("PF_PP_MINTS") or "").split(",") if t]
        self.uri     = uri or f"wss://pumpportal.fun/api/data?api-key={self.api_key}"
        self._ws     = None

    def masked_uri(self) -> str:
        def mask(s): return s[:4] + "..." + s[-4:] if s and len(s) > 12 else "<short>"
        return re.sub(r"(api-key=)[^&]+", r"\1" + mask(self.api_key), self.uri)

    async def _subscribe(self, ws):
        await ws.send(json.dumps({"method": "subscribeNewToken"}))  # free feed
        if self.tokens:
            await ws.send(json.dumps({"method": "subscribeTokenTrade", "keys": self.tokens}))
        if self.account:
            await ws.send(json.dumps({"method": "subscribeAccountTrade", "keys": [self.account]}))

    @staticmethod
    def _classify(msg: dict) -> str:
        if not isinstance(msg, dict):
            return "unknown"
        tx = msg.get("txType")
        if tx == "create": return "new_token"
        if tx in ("buy", "sell"): return "trade"
        if "message" in msg: return "info"
        if "errors" in msg: return "warning"
        return "unknown"

    async def events(self):
        """Async generator that auto-reconnects and yields {'kind','raw'}."""
        backoff = 1.0
        while True:
            try:
                async with websockets.connect(
                    self.uri, ping_interval=20, ping_timeout=30,
                    close_timeout=5, max_size=2**23
                ) as ws:
                    self._ws = ws
                    await self._subscribe(ws)
                    backoff = 1.0
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        if msg.get("errors") == "Invalid API key. PumpSwap data will not be streamed.":
                            continue
                        yield {"kind": self._classify(msg), "raw": msg}
            except asyncio.CancelledError:
                raise
            except Exception:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.7, 30.0)
            finally:
                self._ws = None
