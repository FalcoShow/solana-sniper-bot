#!/usr/bin/env python3
# jup_relay.py
import os, socket, ssl, traceback
from urllib.parse import urlparse
from aiohttp import web, ClientSession, TCPConnector, ClientTimeout
from aiohttp.resolver import ThreadedResolver

# ---------- Config ----------
BIND = os.getenv("RELAY_BIND", "127.0.0.1")
PORT = int(os.getenv("RELAY_PORT", "18080"))

UPSTREAM = os.getenv("JUP_UPSTREAM", os.getenv("JUP_BASE_URL", "https://public.jupiterapi.com")).rstrip("/")
API_KEY = os.getenv("JUP_API_KEY", "").strip()

# Optional: pin DNS resolution of quote-api.jup.ag -> specific IPs
RESOLVE_IPS = [ip.strip() for ip in os.getenv("JUP_RESOLVE_IPS", "").split(",") if ip.strip()]

DEBUG = os.getenv("RELAY_DEBUG", "0") == "1"

# Hop-by-hop headers that must not be forwarded (RFC 7230 §6.1)
HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailer", "transfer-encoding", "upgrade"
}

# Good default UA + CORS-friendly defaults for Jupiter API
# NOTE: We *force* identity to avoid Brotli/Zstd/Gzip coming back.
DEFAULT_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "identity",  # ← key line: no compression upstream
    "Origin": "https://quote-api.jup.ag",
    "Referer": "https://quote-api.jup.ag/",
}

# ---------- Logging ----------
def log(msg: str):
    try:
        with open("/tmp/jup_relay.log", "a", encoding="utf-8") as f:
            f.write(msg.rstrip() + "\n")
    except Exception:
        pass

# ---------- Resolver (optional IP pinning without aiodns) ----------
class PinnedResolver(ThreadedResolver):
    """Pin a hostname to the provided IP list (fallback to normal resolution)."""
    def __init__(self, hostname: str, ips: list[str]):
        super().__init__()
        self.hostname = hostname
        self.ips = ips

    async def resolve(self, host, port=0, family=socket.AF_INET):
        if self.ips and host == self.hostname:
            return [
                {"hostname": host, "host": ip, "port": port,
                 "family": family, "proto": 0, "flags": 0}
                for ip in self.ips
            ]
        return await super().resolve(host, port, family)

# ---------- HTTP Client ----------
async def make_session(upstream: str) -> ClientSession:
    parsed = urlparse(upstream)
    hostname = parsed.hostname or "quote-api.jup.ag"

    resolver = PinnedResolver(hostname, RESOLVE_IPS) if RESOLVE_IPS else ThreadedResolver()
    ssl_ctx = ssl.create_default_context()
    connector = TCPConnector(limit=100, ttl_dns_cache=300, resolver=resolver, ssl=ssl_ctx)
    timeout = ClientTimeout(total=20)  # generous but bounded
    return ClientSession(connector=connector, timeout=timeout, trust_env=False)

# ---------- Lifecycle ----------
async def on_start(app: web.Application):
    app["session"] = await make_session(UPSTREAM)
    log(f"[relay] up: upstream={UPSTREAM} ips={RESOLVE_IPS or '[]'} api_key={'yes' if API_KEY else 'no'}")

async def on_cleanup(app: web.Application):
    try:
        await app["session"].close()
    finally:
        log("[relay] down")

# ---------- Utils ----------
def _forward_headers(req: web.Request) -> dict:
    # Start with incoming headers (minus hop-by-hop), then overlay defaults.
    fwd = {k: v for k, v in req.headers.items() if k.lower() not in HOP_BY_HOP}
    for k, v in DEFAULT_HEADERS.items():
        fwd[k] = v  # override to enforce identity

    # Always set the upstream Host (important when using pinned IPs)
    host = urlparse(UPSTREAM).hostname or "quote-api.jup.ag"
    fwd["Host"] = host

    # Pass API key if provided and not already present
    if API_KEY and "x-api-key" not in {k.lower(): v for k, v in fwd.items()}:
        fwd["x-api-key"] = API_KEY

    return fwd

def _strip_response_headers(headers: "web.CIMultiDictProxy[str]") -> list[tuple[str, str]]:
    out = []
    for k, v in headers.items():
        kl = k.lower()
        if kl in HOP_BY_HOP:
            continue
        if kl in ("content-encoding", "transfer-encoding"):
            # We deliver raw bytes as-is; present them as identity (no TE).
            continue
        # Some CDNs send stale Content-Length when encoding is stripped.
        if kl == "content-length":
            # We'll recompute length from body below.
            continue
        out.append((k, v))
    return out

# ---------- Handlers ----------
async def debug_handler(_: web.Request):
    return web.json_response({
        "ok": True,
        "upstream": UPSTREAM,
        "resolve_ips": RESOLVE_IPS or None,
        "has_api_key": bool(API_KEY),
        "note": "relay healthy",
    })

async def proxy_handler(request: web.Request):
    tail = request.match_info["tail"]
    url = f"{UPSTREAM}/{tail}"
    try:
        body = await request.read()
        headers = _forward_headers(request)

        if DEBUG:
            log(f"[proxy] -> {url} q={dict(request.rel_url.query)}")

        async with request.app["session"].request(
            method=request.method,
            url=url,
            params=request.rel_url.query,
            headers=headers,
            data=body if body else None,
            allow_redirects=False,
        ) as resp:
            raw = await resp.read()

            # Build clean response headers: no hop-by-hop, no encodings, fresh length
            out_headers = _strip_response_headers(resp.headers)
            out_headers.append(("Content-Length", str(len(raw))))

            # Preserve content type if provided
            ctype = resp.headers.get("Content-Type")
            if ctype is not None and not any(k.lower() == "content-type" for k, _ in out_headers):
                out_headers.append(("Content-Type", ctype))

            if DEBUG or resp.status >= 400:
                log(f"[proxy] <- {resp.status} len={len(raw)} via={url}")

            return web.Response(status=resp.status, headers=out_headers, body=raw)

    except Exception as e:
        if DEBUG:
            log(f"[proxy][ERR] {e}\n{traceback.format_exc()}")
        else:
            log(f"[proxy][ERR] {e}")
        return web.json_response(
            {"ok": False, "error": str(e), "upstream": UPSTREAM},
            status=502
        )

# ---------- App ----------
def build_app() -> web.Application:
    app = web.Application()
    app.on_startup.append(on_start)
    app.on_cleanup.append(on_cleanup)
    app.add_routes([
        web.get("/__debug", debug_handler),
        web.route("*", "/{tail:.*}", proxy_handler),
    ])
    return app

def main():
    web.run_app(build_app(), host=BIND, port=PORT)

if __name__ == "__main__":
    main()
