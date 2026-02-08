#!/usr/bin/env python3
from __future__ import annotations

"""
jup_relay.py — Jupiter relay (IPv4-friendly + host pin + fallbacks)
==================================================================

Purpose
-------
A small HTTP reverse-proxy you can run locally or on Render to:
- force IPv4 (some environments have flaky IPv6 / DNS)
- optionally pin upstream hosts to specific IPs (curl --resolve style)
- attach x-api-key automatically
- provide upstream fallback list
- rewrite legacy paths to modern /swap/v1/* paths (and also support /v6 upstreams)

This is NOT your canonical router. Your canonical router remains:
  utils_pkg/router.py

Routes
------
Health:
  GET /ping
  GET /__debug

Proxy (incoming -> upstream mapping depends on upstream kind):
  /v6/*              -> swap/v1/* on "root" upstreams, or passthrough tail on "/v6" upstreams
  /quote             -> /swap/v1/quote (root) OR /quote (v6)
  /swap              -> /swap/v1/swap (root) OR /swap (v6)
  /swap-instructions -> /swap/v1/swap-instructions (root) OR /swap-instructions (v6)
  /swap/v1/*         -> passthrough (root) OR tail passthrough (v6)
  everything else    -> passthrough (no rewrite)

Env (NO new keys introduced)
----------------------------
Bind:
  RELAY_BIND=127.0.0.1
  RELAY_PORT=8081

Upstream base (any one):
  RELAY_UPSTREAM=https://api.jup.ag           (root style; uses /swap/v1 for known endpoints)
  JUP_BASE_URL=https://api.jup.ag             (fallback if RELAY_UPSTREAM missing)

Fallbacks (comma-separated):
  JUP_ALT_BASE_URLS=https://api.jup.ag,https://quote-api.jup.ag/v6

API key (any one; injected as x-api-key if caller didn’t provide):
  JUP_API_KEY=...
  JUP_PRO_KEY=...
  JUPITER_API_KEY=...

Networking:
  RELAY_FORCE_IPV4=1
  RELAY_HOST_MAP="api.jup.ag=1.2.3.4,quote-api.jup.ag=5.6.7.8"
  RELAY_TIMEOUT_S=12
  RELAY_CONNECT_TIMEOUT_S=4
  RELAY_MAX_BODY_MB=2
  RELAY_MAX_UPSTREAM_BYTES=0   (0 = unlimited; else hard cap)

Logging:
  LOG_LEVEL=INFO
"""

import asyncio
import contextlib
import logging
import os
import socket
import time
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass
from typing import Any, Final, Literal, Protocol, cast

from aiohttp import ClientSession, ClientTimeout, TCPConnector, web
from aiohttp.abc import AbstractResolver
from aiohttp.client_exceptions import ClientConnectorError, ClientError
from aiohttp.client_reqrep import ClientResponse

# =============================================================================
# Phase A Logging contract: EventLogger Protocol + safe get_logger
# =============================================================================


class EventLogger(Protocol):
    # Keep handlers in the typing contract (some code checks .handlers)
    handlers: list[Any]

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def info(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def error(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def log(self, level: int, msg: str, *args: Any, **kwargs: Any) -> None: ...


class _EventCapable(EventLogger, Protocol):
    def event(self, name: str, **fields: Any) -> None: ...


_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}


def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    return (v if v is not None else d).strip()


def _envb(k: str, d: bool = False) -> bool:
    v = _envs(k, "")
    if not v:
        return bool(d)
    s = v.lower().strip()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return bool(d)


def _envi(k: str, d: int) -> int:
    try:
        return int(float(_envs(k, str(d))))
    except Exception:
        return int(d)


def _envf(k: str, d: float) -> float:
    try:
        return float(_envs(k, str(d)))
    except Exception:
        return float(d)


def _first_env(keys: Iterable[str], default: str = "") -> str:
    for k in keys:
        v = _envs(k, "")
        if v:
            return v
    return default


class _LoggerProxy:
    """
    Small adapter that guarantees `.handlers` exists for typing compatibility,
    while forwarding standard logging methods. No monkey-patching of methods.
    """

    def __init__(self, base: Any):
        self._base = base
        self.handlers: list[Any] = []
        with contextlib.suppress(Exception):
            h = getattr(base, "handlers", None)
            if isinstance(h, list):
                self.handlers = h

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        return cast(Any, self._base).debug(msg, *args, **kwargs)

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        return cast(Any, self._base).info(msg, *args, **kwargs)

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        fn = getattr(self._base, "warning", None) or getattr(self._base, "warn", None)
        if callable(fn):
            fn(msg, *args, **kwargs)
            return
        self.info(msg, *args, **kwargs)

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        return cast(Any, self._base).error(msg, *args, **kwargs)

    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None:
        fn = getattr(self._base, "exception", None)
        if callable(fn):
            fn(msg, *args, **kwargs)
            return
        self.error(msg, *args, **kwargs)

    def log(self, level: int, msg: str, *args: Any, **kwargs: Any) -> None:
        return cast(Any, self._base).log(level, msg, *args, **kwargs)

    def event(self, name: str, **fields: Any) -> None:
        fn = getattr(self._base, "event", None)
        if callable(fn):
            fn(name, **fields)
            return
        self.info("%s %s", name, fields)


def get_logger(name: str | None = None) -> EventLogger:
    """
    Single signature. Always returns an EventLogger-compatible object.

    Priority:
      1) utils_pkg.log_compat.get_event_logger / get_logger
      2) logging_setup.get_logger
      3) stdlib logging.getLogger (basicConfig if needed)
    """
    nm = (name or "jup_relay").strip() or "jup_relay"

    with contextlib.suppress(Exception):
        from utils_pkg.log_compat import get_event_logger  # type: ignore

        return cast(EventLogger, _LoggerProxy(get_event_logger(nm)))

    with contextlib.suppress(Exception):
        from utils_pkg.log_compat import get_logger as gl  # type: ignore

        return cast(EventLogger, _LoggerProxy(gl(nm)))

    with contextlib.suppress(Exception):
        from logging_setup import get_logger as gl2  # type: ignore

        return cast(EventLogger, _LoggerProxy(gl2(nm)))

    # stdlib fallback
    lg = logging.getLogger(nm)
    if not lg.handlers:
        level = _envs("LOG_LEVEL", "INFO").upper() or "INFO"
        logging.basicConfig(
            level=getattr(logging, level, logging.INFO),
            format="[relay] %(asctime)s %(levelname)s %(name)s %(message)s",
        )
    return cast(EventLogger, _LoggerProxy(lg))


log: EventLogger = get_logger("jup_relay")


def _event(name: str, **fields: Any) -> None:
    """Best-effort structured log event; never raises."""
    try:
        fn = getattr(log, "event", None)
        if callable(fn):
            cast(_EventCapable, log).event(str(name), **fields)
        else:
            log.info("%s %s", str(name), fields)
    except Exception:
        return


# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
UpstreamKind = Literal["root", "swap_v1", "v6"]


@dataclass(frozen=True)
class Upstream:
    raw: str
    base: str
    kind: UpstreamKind
    root_base: str  # if kind is swap_v1, this is the derived root; else == base


@dataclass(frozen=True)
class RelayConfig:
    bind: str
    port: int

    upstream_raw: str
    upstream_primary: Upstream
    upstream_fallbacks: list[Upstream]

    api_key: str
    force_ipv4: bool
    hostmap: dict[str, str]

    timeout_s: float
    connect_timeout_s: float

    max_body_bytes: int
    max_upstream_bytes: int  # 0 = unlimited


def _parse_hostmap(raw: str) -> dict[str, str]:
    out: dict[str, str] = {}
    s = (raw or "").strip()
    if not s:
        return out
    for item in s.split(","):
        it = item.strip()
        if not it or "=" not in it:
            continue
        host, ip = it.split("=", 1)
        host = host.strip().lower()
        ip = ip.strip()
        if host and ip:
            out[host] = ip
    return out


def _split_csv(raw: str) -> list[str]:
    out: list[str] = []
    for p in (raw or "").split(","):
        s = p.strip()
        if s:
            out.append(s.rstrip("/"))
    return out


def _mk_upstream(raw: str) -> Upstream:
    r = (raw or "").strip().rstrip("/")
    if not r:
        r = "https://api.jup.ag"

    if r.endswith("/swap/v1"):
        root = r[: -len("/swap/v1")].rstrip("/") or r
        return Upstream(raw=raw, base=r, kind="swap_v1", root_base=root)

    if r.endswith("/v6"):
        return Upstream(raw=raw, base=r, kind="v6", root_base=r)

    return Upstream(raw=raw, base=r, kind="root", root_base=r)


def load_config() -> RelayConfig:
    bind = _envs("RELAY_BIND", "127.0.0.1")
    port = _envi("RELAY_PORT", 8081)

    upstream_raw = _first_env(["RELAY_UPSTREAM", "JUP_BASE_URL"], "https://api.jup.ag").rstrip("/")
    primary = _mk_upstream(upstream_raw)

    fallbacks_raw = _envs("JUP_ALT_BASE_URLS", "")
    fallbacks = [_mk_upstream(u) for u in _split_csv(fallbacks_raw)]

    api_key = _first_env(["JUP_API_KEY", "JUP_PRO_KEY", "JUPITER_API_KEY"], "")

    force_ipv4 = _envb("RELAY_FORCE_IPV4", True)
    hostmap = _parse_hostmap(_envs("RELAY_HOST_MAP", ""))

    timeout_s = float(max(3.0, _envf("RELAY_TIMEOUT_S", 12.0)))
    connect_timeout_s = float(max(1.0, _envf("RELAY_CONNECT_TIMEOUT_S", 4.0)))

    max_body_mb = float(_envf("RELAY_MAX_BODY_MB", 2.0))
    max_body_bytes = int(max(64 * 1024, max_body_mb * 1024 * 1024))

    max_upstream_bytes = _envi("RELAY_MAX_UPSTREAM_BYTES", 0)  # 0 = unlimited

    # Dedup upstreams by (kind, base)
    seen: set[tuple[UpstreamKind, str]] = set()
    dedup: list[Upstream] = []
    for u in [primary, *fallbacks]:
        key = (u.kind, u.base)
        if u.base and key not in seen:
            seen.add(key)
            dedup.append(u)

    upstream_primary = dedup[0] if dedup else primary
    upstream_fallbacks = dedup[1:] if len(dedup) > 1 else []

    return RelayConfig(
        bind=bind,
        port=port,
        upstream_raw=upstream_raw,
        upstream_primary=upstream_primary,
        upstream_fallbacks=upstream_fallbacks,
        api_key=api_key,
        force_ipv4=force_ipv4,
        hostmap=hostmap,
        timeout_s=timeout_s,
        connect_timeout_s=connect_timeout_s,
        max_body_bytes=max_body_bytes,
        max_upstream_bytes=max_upstream_bytes,
    )


CFG = load_config()

# ──────────────────────────────────────────────────────────────────────────────
# Resolver (host pin)
# ──────────────────────────────────────────────────────────────────────────────


class HostMapResolver(AbstractResolver):
    """
    Static host->IP resolver (curl --resolve equivalent), with fallback to OS DNS.
    """

    def __init__(self, mapping: dict[str, str]):
        self._map = {k.lower(): v for k, v in (mapping or {}).items() if k and v}

    async def resolve(
        self, host: str, port: int = 0, family: int = socket.AF_UNSPEC
    ) -> list[dict[str, Any]]:
        ip = self._map.get((host or "").lower())
        if ip:
            fam = socket.AF_INET6 if ":" in ip else socket.AF_INET
            return [
                {
                    "hostname": host,
                    "host": ip,
                    "port": port,
                    "family": fam,
                    "proto": 0,
                    "flags": 0,
                }
            ]

        loop = asyncio.get_running_loop()
        infos = await loop.getaddrinfo(host, port, family=family, type=socket.SOCK_STREAM)

        out: list[dict[str, Any]] = []
        for a in infos:
            out.append(
                {
                    "hostname": host,
                    "host": a[4][0],
                    "port": a[4][1],
                    "family": a[0],
                    "proto": a[2],
                    "flags": 0,
                }
            )
        return out

    async def close(self) -> None:
        return


# ──────────────────────────────────────────────────────────────────────────────
# Header sanitization + path rewrite planning
# ──────────────────────────────────────────────────────────────────────────────

_HOP_BY_HOP: Final[set[str]] = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
}


def _sanitize_request_headers(h: dict[str, str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for k, v in (h or {}).items():
        kl = k.lower()
        if kl in _HOP_BY_HOP:
            continue
        if kl in {"host", "content-length"}:
            continue
        out[k] = v
    return out


def _sanitize_response_headers(h: Any) -> dict[str, str]:
    out: dict[str, str] = {}
    try:
        items = (h or {}).items()
    except Exception:
        return out
    for k, v in items:
        kl = str(k).lower()
        if kl in _HOP_BY_HOP:
            continue
        if kl == "content-length":
            continue
        out[str(k)] = str(v)
    return out


def _plan_paths(path: str) -> tuple[str, str, bool]:
    """
    Returns (swap_v1_path, v6_path, is_known).

    swap_v1_path includes the /swap/v1 prefix for known endpoints.
    v6_path is the v6-style equivalent (no /v6 prefix; intended for base that ends with /v6).
    """
    p = path or "/"

    if p.startswith("/swap/v1/"):
        tail = p[len("/swap/v1") :]  # includes leading '/'
        return p, tail, True

    if p.startswith("/v6/"):
        tail = p[len("/v6") :]  # includes leading '/'
        return "/swap/v1" + tail, tail, True

    if p == "/quote":
        return "/swap/v1/quote", "/quote", True
    if p == "/swap":
        return "/swap/v1/swap", "/swap", True
    if p == "/swap-instructions":
        return "/swap/v1/swap-instructions", "/swap-instructions", True

    return p, p, False


def _join_url(base: str, path: str, query: str) -> str:
    b = (base or "").rstrip("/")
    p = (path or "/").lstrip("/")
    url = f"{b}/{p}"
    if query:
        url = f"{url}?{query}"
    return url


def _build_try_list() -> list[Upstream]:
    out = [CFG.upstream_primary, *CFG.upstream_fallbacks]
    seen: set[tuple[UpstreamKind, str]] = set()
    dedup: list[Upstream] = []
    for u in out:
        key = (u.kind, u.base)
        if u.base and key not in seen:
            seen.add(key)
            dedup.append(u)
    return dedup


def _url_for_upstream(u: Upstream, req_path: str, query: str) -> str:
    swap_path, v6_path, known = _plan_paths(req_path)

    if u.kind == "v6":
        chosen = v6_path if known else req_path
        return _join_url(u.base, chosen, query)

    if u.kind == "swap_v1":
        if known and swap_path.startswith("/swap/v1/"):
            tail = swap_path[len("/swap/v1") :]  # includes leading '/'
            return _join_url(u.base, tail, query)
        base = u.root_base or u.base
        return _join_url(base, req_path, query)

    chosen2 = swap_path if known else req_path
    return _join_url(u.base, chosen2, query)


async def _aclose_upstream(resp: ClientResponse | None) -> None:
    """
    Ensure upstream is released/closed correctly.
    - ClientResponse.release() is sync
    - wait_for_close() is async
    """
    if resp is None:
        return
    with contextlib.suppress(Exception):
        resp.release()
    with contextlib.suppress(Exception):
        await resp.wait_for_close()


# ──────────────────────────────────────────────────────────────────────────────
# Runtime state / counters
# ──────────────────────────────────────────────────────────────────────────────


class Counters:
    def __init__(self) -> None:
        self.started = time.time()
        self.total = 0
        self.ok = 0
        self.fail = 0
        self.by_status: dict[str, int] = {}
        self.fallback_used = 0

    def bump(self, status: int | None, ok: bool, used_fallback: bool) -> None:
        self.total += 1
        if ok:
            self.ok += 1
        else:
            self.fail += 1
        if status is not None:
            s = str(int(status))
            self.by_status[s] = self.by_status.get(s, 0) + 1
        if used_fallback:
            self.fallback_used += 1


CTR = Counters()

# ──────────────────────────────────────────────────────────────────────────────
# Handlers
# ──────────────────────────────────────────────────────────────────────────────


async def ping(_req: web.Request) -> web.Response:
    return web.json_response({"ok": True, "relay": "up"})


async def debug(_req: web.Request) -> web.Response:
    return web.json_response(
        {
            "ok": True,
            "bind": f"{CFG.bind}:{CFG.port}",
            "upstream_raw": CFG.upstream_raw,
            "primary": {"kind": CFG.upstream_primary.kind, "base": CFG.upstream_primary.base},
            "fallbacks": [{"kind": u.kind, "base": u.base} for u in CFG.upstream_fallbacks],
            "api_key_present": bool(CFG.api_key),
            "force_ipv4": CFG.force_ipv4,
            "hostmap": CFG.hostmap,
            "timeouts": {"connect_s": CFG.connect_timeout_s, "total_s": CFG.timeout_s},
            "limits": {
                "max_body_bytes": CFG.max_body_bytes,
                "max_upstream_bytes": CFG.max_upstream_bytes,
            },
            "counters": {
                "uptime_s": int(time.time() - CTR.started),
                "total": CTR.total,
                "ok": CTR.ok,
                "fail": CTR.fail,
                "fallback_used": CTR.fallback_used,
                "by_status": CTR.by_status,
            },
        }
    )


async def _read_body_limited(req: web.Request) -> bytes | None:
    if not req.can_read_body:
        return b""
    total = 0
    chunks: list[bytes] = []
    async for chunk in req.content.iter_chunked(64 * 1024):
        total += len(chunk)
        if total > int(CFG.max_body_bytes):
            return None
        chunks.append(chunk)
    return b"".join(chunks)


async def _proxy_once(
    *,
    session: ClientSession,
    method: str,
    url: str,
    headers: dict[str, str],
    body: bytes | None,
) -> tuple[int, dict[str, str], ClientResponse]:
    timeout = ClientTimeout(total=float(CFG.timeout_s), connect=float(CFG.connect_timeout_s))
    resp = await session.request(
        method=method,
        url=url,
        headers=headers,
        data=body if body else None,
        allow_redirects=False,
        timeout=timeout,
    )
    return int(resp.status), _sanitize_response_headers(resp.headers), resp


async def passthrough(req: web.Request) -> web.StreamResponse:
    # protect health endpoints
    if req.path in ("/ping", "/__debug"):
        return web.Response(status=404, text="not found")

    started = time.perf_counter()
    method = req.method.upper()

    # request headers + key injection
    headers = _sanitize_request_headers(dict(req.headers))
    if CFG.api_key and not any(k.lower() == "x-api-key" for k in headers):
        headers["x-api-key"] = CFG.api_key

    # request body limit
    try:
        body = await _read_body_limited(req)
    except asyncio.CancelledError:
        raise
    except Exception:
        body = None

    if body is None:
        return web.json_response({"ok": False, "error": "request body too large"}, status=413)

    tries = _build_try_list()
    session = cast(ClientSession, req.app[SESSION_KEY])

    last_err: str | None = None
    used_fallback_any = False

    for idx, u in enumerate(tries):
        used_fallback = idx > 0
        used_fallback_any = used_fallback_any or used_fallback

        candidate = _url_for_upstream(u, req.path, req.query_string)

        upstream: ClientResponse | None = None
        try:
            st, resp_headers, upstream_resp = await _proxy_once(
                session=session,
                method=method,
                url=candidate,
                headers=headers,
                body=body,
            )
            upstream = upstream_resp  # for cleanup in finally

            # status-based fallback
            if st in (401, 403, 404) and idx < (len(tries) - 1):
                last_err = f"status:{st}"
                await _aclose_upstream(upstream_resp)
                upstream = None
                continue

            # hard-cap before streaming when Content-Length is known
            if CFG.max_upstream_bytes > 0:
                with contextlib.suppress(Exception):
                    cl = upstream_resp.headers.get("Content-Length")
                    if cl is not None and int(cl) > int(CFG.max_upstream_bytes):
                        last_err = "upstream_body_too_large"
                        await _aclose_upstream(upstream_resp)
                        upstream = None
                        CTR.bump(status=502, ok=False, used_fallback=used_fallback)
                        return web.json_response(
                            {
                                "ok": False,
                                "error": "upstream body too large",
                                "limit_bytes": int(CFG.max_upstream_bytes),
                            },
                            status=502,
                        )

            out = web.StreamResponse(status=st, headers=resp_headers)
            await out.prepare(req)

            total_out = 0
            stream_ok = True

            try:
                async for chunk in upstream_resp.content.iter_chunked(128 * 1024):
                    if not chunk:
                        continue
                    total_out += len(chunk)
                    if CFG.max_upstream_bytes > 0 and total_out > int(CFG.max_upstream_bytes):
                        stream_ok = False
                        last_err = "upstream_body_exceeded_limit"
                        break
                    await out.write(chunk)
            except asyncio.CancelledError:
                raise
            except (ConnectionResetError, BrokenPipeError):
                stream_ok = False
                last_err = "client_disconnected"
            except Exception:
                stream_ok = False
                last_err = "stream_error"

            await _aclose_upstream(upstream_resp)
            upstream = None

            with contextlib.suppress(Exception):
                await out.write_eof()

            ok = bool(stream_ok and (200 <= st < 400))
            CTR.bump(status=st if ok else 502, ok=ok, used_fallback=used_fallback)

            dur_ms = int((time.perf_counter() - started) * 1000.0)
            if ok:
                log.debug(
                    "proxy ok method=%s path=%s st=%s ms=%s fb=%s",
                    method,
                    req.path,
                    st,
                    dur_ms,
                    used_fallback,
                )
            else:
                log.info(
                    "proxy fail method=%s path=%s st=%s ms=%s fb=%s detail=%s",
                    method,
                    req.path,
                    st,
                    dur_ms,
                    used_fallback,
                    last_err,
                )
            return out

        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            last_err = "timeout"
            continue
        except ClientConnectorError as e:
            cause = getattr(e, "__cause__", None)
            last_err = (
                "dns_resolution_failed" if isinstance(cause, socket.gaierror) else "connect_error"
            )
            continue
        except ClientError:
            last_err = "client_error"
            continue
        except Exception as e:
            last_err = f"upstream_error:{type(e).__name__}"
            break
        finally:
            if upstream is not None:
                with contextlib.suppress(Exception):
                    await _aclose_upstream(upstream)

    CTR.bump(status=502, ok=False, used_fallback=bool(used_fallback_any))
    return web.json_response(
        {
            "ok": False,
            "error": "failed to contact upstreams",
            "detail": last_err,
            "path": req.path,
        },
        status=502,
    )


# ──────────────────────────────────────────────────────────────────────────────
# App factory
# ──────────────────────────────────────────────────────────────────────────────

SESSION_KEY: Final[str] = "relay_session"


def make_app() -> web.Application:
    app = web.Application()

    app.add_routes(
        [
            web.get("/ping", ping),
            web.get("/__debug", debug),
            # legacy short forms
            web.route("*", "/quote", passthrough),
            web.route("*", "/swap", passthrough),
            web.route("*", "/swap-instructions", passthrough),
            # v6 + v1 paths
            web.route("*", "/v6/{tail:.*}", passthrough),
            web.route("*", "/swap/v1/{tail:.*}", passthrough),
            # catch-all
            web.route("*", "/{tail:.*}", passthrough),
        ]
    )

    family = socket.AF_INET if CFG.force_ipv4 else 0
    resolver: AbstractResolver | None = HostMapResolver(CFG.hostmap) if CFG.hostmap else None

    async def _session_ctx(_app: web.Application) -> AsyncIterator[None]:
        _event(
            "RELAY_START",
            bind=f"{CFG.bind}:{CFG.port}",
            upstream_raw=CFG.upstream_raw,
            primary=f"{CFG.upstream_primary.kind}:{CFG.upstream_primary.base}",
            fallbacks=[f"{u.kind}:{u.base}" for u in CFG.upstream_fallbacks],
            ipv4=CFG.force_ipv4,
            hostmap=bool(CFG.hostmap),
        )

        connector = TCPConnector(family=family, resolver=resolver, ttl_dns_cache=60)
        session = ClientSession(
            connector=connector,
            headers={"user-agent": "pnlsniper-relay"},
        )
        _app[SESSION_KEY] = session
        try:
            yield
        finally:
            with contextlib.suppress(Exception):
                await session.close()

    app.cleanup_ctx.append(_session_ctx)
    return app


def main() -> None:
    web.run_app(make_app(), host=CFG.bind, port=int(CFG.port))


if __name__ == "__main__":
    main()


__all__ = [
    "EventLogger",
    "HostMapResolver",
    "get_logger",
    "load_config",
    "make_app",
    "main",
]
