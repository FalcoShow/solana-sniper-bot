from __future__ import annotations

"""
utils_pkg/core_utils.py — V3+++ Core Runtime (LOCK-IN / PROD)
============================================================

Core runtime + state layer:

✅ One shared aiohttp session (NO LEAKS)
✅ Async Redis (native) + Upstash REST fallback
✅ Optional async Postgres engine (asyncpg-correct SSL + DSN normalization)
✅ Solana AsyncClient singleton + optional RPC failover integration
✅ SQLite entry-price cache (cheap local fallback) + compat aliases
✅ Zero network at import-time (only inside init_resources / explicit calls)
✅ Backwards-compatible function names/signatures used across your stack

Dec 2025 ship-blockers fixed:
- asyncpg DOES NOT accept ?sslmode= in URL → we strip and provide connect_args["ssl"]=SSLContext
- aiohttp drain on all paths to avoid “Unclosed client session/connector”
"""

import asyncio
import base64
import contextlib
import inspect
import json
import os
import random
import socket
import ssl
import struct
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Awaitable, Dict, List, Optional, Sequence, Tuple, Union
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import aiohttp
import aiosqlite
import certifi
from sqlalchemy import BigInteger, Column, DateTime, Float, MetaData, String, Table, UniqueConstraint, text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from solana.rpc.async_api import AsyncClient
from solana.rpc.commitment import Confirmed
from solana.rpc.types import TokenAccountOpts, TxOpts
from solders.keypair import Keypair
from solders.pubkey import Pubkey as PublicKey

try:  # pragma: no cover
    from solana.system_program import TransferParams, transfer  # type: ignore
except Exception:  # pragma: no cover
    TransferParams = None  # type: ignore
    transfer = None  # type: ignore


# ──────────────────────────────────────────────────────────────────────────────
# Logging (safe fallback + .event/.every shim)
# ──────────────────────────────────────────────────────────────────────────────
try:  # pragma: no cover
    from logging_setup import get_logger  # type: ignore
except Exception:  # pragma: no cover
    import logging

    def get_logger(name: str | None = None):  # type: ignore
        return logging.getLogger(name or "core_utils")


log = get_logger(__name__)


def _short(x: Any, n: int = 280) -> str:
    try:
        s = str(x)
        return s if len(s) <= n else (s[:n] + "…")
    except Exception:
        return "<?>"


if not hasattr(log, "event"):

    def _event(name: str, **kw: Any) -> None:  # pragma: no cover
        with contextlib.suppress(Exception):
            log.info("%s %s", name, kw)

    setattr(log, "event", _event)  # type: ignore[attr-defined]

if not hasattr(log, "every"):

    def _every(key: str, n: int = 30, msg: Optional[str] = None, **kw: Any) -> None:  # pragma: no cover
        try:
            now = time.monotonic()
            store = getattr(_every, "_store", {})  # type: ignore[attr-defined]
            last = float(store.get(key, 0.0))
            if (now - last) >= float(max(1, int(n))):
                store[key] = now
                setattr(_every, "_store", store)  # type: ignore[attr-defined]
                getattr(log, "event")(msg or key, **kw)
        except Exception:
            return None

    setattr(log, "every", _every)  # type: ignore[attr-defined]


# ──────────────────────────────────────────────────────────────────────────────
# Optional imports (never hard-fail core runtime)
# ──────────────────────────────────────────────────────────────────────────────
try:  # pragma: no cover
    from .alert_manager import AlertManager  # type: ignore
except Exception:  # pragma: no cover
    AlertManager = None  # type: ignore

try:  # pragma: no cover
    from utils_pkg import solana_wallet as SW  # type: ignore
except Exception:  # pragma: no cover
    SW = None  # type: ignore

try:  # pragma: no cover
    from utils_pkg import lightning_impl as L  # type: ignore
except Exception:  # pragma: no cover
    L = None  # type: ignore

try:  # pragma: no cover
    from utils_pkg.mint_guard import guard_jupiter_quote_args  # type: ignore
except Exception:  # pragma: no cover
    guard_jupiter_quote_args = None  # type: ignore

try:  # pragma: no cover
    from utils_pkg.mints import SOL_MINT as WSOL_MINT  # type: ignore
except Exception:  # pragma: no cover
    WSOL_MINT = "So11111111111111111111111111111111111111112"

try:  # pragma: no cover
    from utils_pkg import rpc_failover as RF  # type: ignore
except Exception:  # pragma: no cover
    RF = None  # type: ignore


# ──────────────────────────────────────────────────────────────────────────────
# Config import (with env fallbacks)
# ──────────────────────────────────────────────────────────────────────────────
try:  # pragma: no cover
    from config import (  # type: ignore
        RPC_URL,
        REDIS_URL as CFG_REDIS_URL,
        TELEGRAM_BOT_TOKEN,
        TELEGRAM_CHAT_ID,
        DISCORD_WEBHOOK,
        BIRDEYE_API_KEY,
        BIRDEYE_API_URL,
        NOTION_TOKEN,
        NOTION_DATABASE_ID,
        ENABLE_NOTION_LOGGING,
        SQLITE_DB_PATH,
        PG_ENABLED,
        JUPITER_MAX_SLIPPAGE,
        SAVINGS_WALLET,
        FRAUD_API_URL,
        FRAUD_API_KEY,
        SCANNER_MIN_FRAUD_SCORE,
    )
except Exception:  # pragma: no cover
    RPC_URL = os.getenv("RPC_URL", "https://api.mainnet-beta.solana.com")
    CFG_REDIS_URL = os.getenv("REDIS_URL", "")
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
    DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK", "")
    BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY", "")
    BIRDEYE_API_URL = os.getenv("BIRDEYE_API_URL", "https://public-api.birdeye.so")
    NOTION_TOKEN = os.getenv("NOTION_TOKEN", "")
    NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID", "")
    ENABLE_NOTION_LOGGING = (os.getenv("ENABLE_NOTION_LOGGING", "false") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "y",
        "on",
    )
    SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "./state.sqlite3")
    PG_ENABLED = (os.getenv("PG_ENABLED", "false") or "").strip().lower() in ("1", "true", "yes", "y", "on")
    JUPITER_MAX_SLIPPAGE = float(os.getenv("JUPITER_MAX_SLIPPAGE", "0.005") or 0.005)
    SAVINGS_WALLET = os.getenv("SAVINGS_WALLET", "")
    FRAUD_API_URL = os.getenv("FRAUD_API_URL", "")
    FRAUD_API_KEY = os.getenv("FRAUD_API_KEY", "")
    SCANNER_MIN_FRAUD_SCORE = float(os.getenv("SCANNER_MIN_FRAUD_SCORE", "70") or 70.0)


# ──────────────────────────────────────────────────────────────────────────────
# Env helpers + constants
# ──────────────────────────────────────────────────────────────────────────────
_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(_env_str(name, str(default))))
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    try:
        return float(_env_str(name, str(default)))
    except Exception:
        return float(default)


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, (bytes, bytearray)):
            x = x.decode("utf-8", "ignore")
        return float(x)
    except Exception:
        return default


def _jitter(min_ms: int = 20, max_ms: int = 120) -> float:
    return random.uniform(min_ms / 1000.0, max_ms / 1000.0)


def _json_compact(x: Any) -> str:
    return json.dumps(x, ensure_ascii=False, separators=(",", ":"), default=str)


LAMPORTS_PER_SOL = 1_000_000_000
LAMPORTS_PER_SOL_F = float(LAMPORTS_PER_SOL)
TOKEN_PROGRAM_ID = PublicKey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")

COINGECKO_SOL_API = "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd"
DEXSCREENER_TOKEN_URL = "https://api.dexscreener.com/latest/dex/tokens/{mint}"

_ssl_context = ssl.create_default_context(cafile=certifi.where())


# ──────────────────────────────────────────────────────────────────────────────
# Globals populated by init_resources()
# ──────────────────────────────────────────────────────────────────────────────
session: Optional[aiohttp.ClientSession] = None
solana_client: Optional[AsyncClient] = None
wallet: Optional[Keypair] = None
redis = None  # type: ignore[assignment]
ALERT_MANAGER: Optional[Any] = None

_BACKGROUND_TASKS: "set[asyncio.Task[Any]]" = set()
_INIT_LOCK = asyncio.Lock()
_INITED: bool = False

_SOL_CACHE_TS: float = 0.0
_SOL_CACHE_PX: float = 0.0
_TOKEN_CACHE: Dict[str, Tuple[float, float]] = {}  # mint -> (ts, px)

PG_METADATA = MetaData()
_PG_ENGINE: Optional[AsyncEngine] = None

_SQLITE_INITED: bool = False
_SQLITE_LOCK = asyncio.Lock()

_RPC_SELECTED_HTTP: str = ""
_RPC_SELECTED_WS: str = ""

_NOTION_LAST_TS: float = 0.0
_TELEGRAM_LAST_TS: float = 0.0


# ──────────────────────────────────────────────────────────────────────────────
# Postgres schema (TRADES + POSITIONS) — legacy-compatible
# ──────────────────────────────────────────────────────────────────────────────
TRADES = Table(
    "trades",
    PG_METADATA,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("ts", DateTime, nullable=False, server_default=text("now()")),
    Column("token", String(64), nullable=False),
    Column("symbol", String(64), nullable=True),
    Column("side", String(16), nullable=True),
    Column("amount", Float, nullable=True),
    Column("entry", Float, nullable=True),
    Column("current", Float, nullable=True),
    Column("profit", Float, nullable=True),
    Column("reason", String(256), nullable=True),
    Column("signature", String(128), nullable=True),
)

POSITIONS = Table(
    "positions",
    PG_METADATA,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("wallet", String(64), nullable=False),
    Column("mint", String(64), nullable=False),
    Column("pool", String(64), nullable=False),
    Column("symbol", String(64), nullable=True),
    Column("size_raw", Float, nullable=True),
    Column("size_sol", Float, nullable=True),
    Column("entry_price", Float, nullable=True),
    Column("opened_at", DateTime, nullable=False, server_default=text("now()")),
    Column("updated_at", DateTime, nullable=False, server_default=text("now()")),
    Column("status", String(16), nullable=False, server_default=text("'open'")),
    UniqueConstraint("wallet", "mint", "pool", name="uq_positions_wallet_mint_pool"),
)


# ──────────────────────────────────────────────────────────────────────────────
# HTTP helper (shared session required)
# ──────────────────────────────────────────────────────────────────────────────
def _session_closed(s: Any) -> bool:
    try:
        return bool(getattr(s, "closed", False))
    except Exception:
        return True


async def _ensure_session() -> aiohttp.ClientSession:
    global session
    if session is not None and not _session_closed(session):
        return session

    session = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(
            ssl=_ssl_context,
            limit=_env_int("HTTP_CONN_LIMIT", 100),
            ttl_dns_cache=_env_int("HTTP_DNS_TTL_SEC", 60),
            enable_cleanup_closed=True,
        ),
        timeout=aiohttp.ClientTimeout(total=_env_float("HTTP_TIMEOUT_SEC", 18.0)),
        json_serialize=_json_compact,
    )
    return session


async def _http_json(
    method: str,
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    json_body: Any = None,
    timeout_s: Optional[float] = None,
    retries: int = 1,
) -> Optional[Dict[str, Any]]:
    """
    Safe JSON request with small retry for transient 429/5xx/network.
    Never raises to callers (best-effort infra).

    CRITICAL: drains response body on every path to avoid aiohttp leaks.
    """
    s = session
    if s is None or _session_closed(s):
        s = await _ensure_session()

    tmo = aiohttp.ClientTimeout(total=timeout_s or _env_float("HTTP_TIMEOUT_SEC", 18.0))
    hdrs: Dict[str, str] = {"user-agent": (_env_str("HTTP_UA", "pnlsniper") or "pnlsniper")}
    if headers:
        hdrs.update(headers)

    transient = {429, 500, 502, 503, 504}

    for attempt in range(1, max(1, retries) + 2):
        try:
            async with s.request(method.upper(), url, params=params, json=json_body, headers=hdrs, timeout=tmo) as r:
                raw = await r.read()  # ALWAYS DRAIN

                if r.status == 204:
                    return {}

                if r.status in transient and attempt <= retries + 1:
                    await asyncio.sleep(_jitter() + (0.2 * attempt))
                    continue

                if r.status != 200:
                    return None

                if not raw:
                    return {}

                try:
                    data = json.loads(raw)
                except Exception:
                    try:
                        data = json.loads(raw.decode("utf-8", "ignore"))
                    except Exception:
                        return None

                if isinstance(data, dict):
                    return data
                if isinstance(data, (list, tuple)):
                    return {"data": list(data)}
                return None
        except asyncio.CancelledError:
            raise
        except Exception:
            if attempt <= retries + 1:
                await asyncio.sleep(_jitter() + (0.15 * attempt))
                continue
            return None
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Postgres DSN normalization (asyncpg-correct SSL)
# ──────────────────────────────────────────────────────────────────────────────
def _normalize_pg_scheme(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return raw
    if raw.startswith("postgresql+asyncpg://"):
        return raw
    if raw.startswith("postgres://"):
        return "postgresql+asyncpg://" + raw[len("postgres://") :]
    if raw.startswith("postgresql://"):
        return "postgresql+asyncpg://" + raw[len("postgresql://") :]
    return raw


def _pop_query_param(url: str, key: str) -> tuple[str, Optional[str]]:
    u = urlparse(url)
    q = list(parse_qsl(u.query, keep_blank_values=True))
    popped = None
    nq: list[tuple[str, str]] = []
    for k, v in q:
        if k == key and popped is None:
            popped = v
        else:
            nq.append((k, v))
    new_q = urlencode(nq, doseq=True)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, u.fragment)), popped


def _pg_ssl_ctx(sslmode: str) -> Optional[ssl.SSLContext]:
    mode = (sslmode or "").strip().lower()
    if mode in ("", "disable", "0", "false", "off"):
        return None

    # Render external expects SSL; default is secure verification.
    ctx = ssl.create_default_context(cafile=certifi.where())

    # Escape hatch ONLY if you must (not recommended):
    if _env_bool("PG_SSL_INSECURE", False):
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

    return ctx


def _sanitize_pg_url_for_asyncpg(url: str) -> tuple[str, dict]:
    """
    asyncpg/sqlalchemy+asyncpg does NOT accept libpq query params like sslmode.
    We strip them and translate into connect_args.
    """
    u = _normalize_pg_scheme(url)

    # Always strip known-bad params for asyncpg
    u, sslmode = _pop_query_param(u, "sslmode")
    u, _ = _pop_query_param(u, "statement_cache_size")
    u, _ = _pop_query_param(u, "application_name")

    connect_args: dict[str, Any] = {}
    ssl_ctx = _pg_ssl_ctx(sslmode or "")
    if ssl_ctx is not None:
        connect_args["ssl"] = ssl_ctx

    # defaults that reduce weird driver issues
    connect_args["statement_cache_size"] = int(_env_int("PG_STATEMENT_CACHE_SIZE", 0))
    connect_args["command_timeout"] = float(_env_float("PG_COMMAND_TIMEOUT_S", 20.0))
    connect_args["server_settings"] = {"application_name": _env_str("PG_APP_NAME", "pnlsniper")}

    return u, connect_args


def _get_pg_dsn_raw() -> Optional[str]:
    for key in (
        "POSITIONS_PG_DSN",
        "SNIPER_DB_DSN",
        "DATABASE_URL",
        "POSTGRES_DSN",
        "PG_DSN",
        "PG_DSN_INTERNAL",
        "PG_DSN_EXTERNAL",
    ):
        v = os.getenv(key)
        if v and v.strip():
            return v.strip()
    return None


def _pg_enabled() -> bool:
    flag = os.getenv("POSITIONS_PG_ENABLED")
    if flag is not None:
        return flag.strip().lower() in _TRUE
    if isinstance(PG_ENABLED, bool):
        return PG_ENABLED
    return _get_pg_dsn_raw() is not None


async def _ensure_pg_engine() -> Optional[AsyncEngine]:
    global _PG_ENGINE
    if _PG_ENGINE is not None:
        return _PG_ENGINE

    backend = (_env_str("PF_POSITIONS_BACKEND", "pg") or "pg").strip().lower()
    if backend != "pg" or not _pg_enabled():
        return None

    raw = _get_pg_dsn_raw()
    if not raw:
        return None

    dsn, connect_args = _sanitize_pg_url_for_asyncpg(raw)

    pool_size = _env_int("PG_POOL_SIZE", 2)
    max_overflow = _env_int("PG_MAX_OVERFLOW", 2)

    try:
        _PG_ENGINE = create_async_engine(
            dsn,
            connect_args=connect_args,
            pool_pre_ping=True,
            pool_size=max(1, int(pool_size)),
            max_overflow=max(0, int(max_overflow)),
        )
        async with _PG_ENGINE.begin() as conn:
            await conn.execute(text("SELECT 1"))
            await conn.run_sync(PG_METADATA.create_all)
        log.every("PG_INIT_OK", n=30, msg="PG_INIT_OK")
        return _PG_ENGINE
    except Exception as e:
        log.every("PG_INIT_FAIL", n=30, msg="PG_INIT_FAIL", err=_short(e))
        with contextlib.suppress(Exception):
            if _PG_ENGINE is not None:
                await _PG_ENGINE.dispose()
        _PG_ENGINE = None
        return None


async def get_pg() -> Optional[AsyncEngine]:
    return await _ensure_pg_engine()


def get_pg_engine() -> Optional[AsyncEngine]:
    return _PG_ENGINE


# ──────────────────────────────────────────────────────────────────────────────
# Redis (native preferred + Upstash REST fallback)
# ──────────────────────────────────────────────────────────────────────────────
class UpstashRestRedis:
    def __init__(self, rest_url: str, rest_token: str) -> None:
        self._url = (rest_url or "").rstrip("/")
        self._token = (rest_token or "").strip()

    def _headers(self) -> Dict[str, str]:
        h = {"content-type": "application/json", "user-agent": _env_str("HTTP_UA", "pnlsniper")}
        if self._token:
            h["Authorization"] = f"Bearer {self._token}"
        return h

    async def _post(self, cmd: str, *args: Any) -> Any:
        s = session
        if s is None or _session_closed(s):
            s = await _ensure_session()
        payload = {"command": [cmd, *args]}
        async with s.post(self._url, headers=self._headers(), json=payload) as resp:
            data = await resp.json(content_type=None)
            if resp.status != 200:
                raise RuntimeError(f"upstash_http_{resp.status}:{_short(data)}")
            return data.get("result") if isinstance(data, dict) and "result" in data else data

    async def ping(self) -> Any:
        return await self._post("PING")

    async def get(self, key: str) -> Any:
        return await self._post("GET", key)

    async def set(self, key: str, value: Any, ex: Optional[int] = None) -> Any:
        if ex is not None:
            return await self._post("SET", key, str(value), "EX", int(ex))
        return await self._post("SET", key, str(value))

    async def delete(self, key: str) -> Any:
        return await self._post("DEL", key)

    async def hgetall(self, key: str) -> Dict[str, Any]:
        res = await self._post("HGETALL", key)
        if isinstance(res, list):
            out: Dict[str, Any] = {}
            for i in range(0, len(res) - 1, 2):
                out[str(res[i])] = res[i + 1]
            return out
        if isinstance(res, dict):
            return res
        return {}

    async def hset(self, key: str, mapping: Dict[str, Any]) -> Any:
        args: List[Any] = [key]
        for k, v in mapping.items():
            args.extend([str(k), str(v)])
        return await self._post("HSET", *args)

    async def zadd(self, key: str, mapping: Dict[str, float]) -> Any:
        args: List[Any] = [key]
        for member, score in mapping.items():
            args.extend([float(score), str(member)])
        return await self._post("ZADD", *args)

    async def zrem(self, key: str, *members: str) -> Any:
        return await self._post("ZREM", key, *members)

    async def zcard(self, key: str) -> int:
        try:
            res = await self._post("ZCARD", key)
            return int(res or 0)
        except Exception:
            return 0

    async def zrange(self, key: str, start: int, stop: int, withscores: bool = False) -> Any:
        if withscores:
            return await self._post("ZRANGE", key, int(start), int(stop), "WITHSCORES")
        return await self._post("ZRANGE", key, int(start), int(stop))

    async def close(self) -> None:
        return None


def _effective_redis_url() -> str:
    return (_env_str("REDIS_URL") or _env_str("REDIS_TLS_URL") or (CFG_REDIS_URL or "").strip() or "").strip()


def _looks_like_upstash(host: str) -> bool:
    h = (host or "").lower()
    return h.endswith(".upstash.io") or "upstash" in h


def _upgrade_to_tls_if_needed(u: str) -> str:
    if not u:
        return u
    if _env_bool("REDIS_DISABLE_TLS", False):
        return u
    try:
        p = urlparse(u)
        scheme = (p.scheme or "").lower()
        host = (p.hostname or "").lower()
        force = _env_bool("REDIS_FORCE_TLS", False) or _looks_like_upstash(host)
        if scheme == "redis" and force:
            return urlunparse(("rediss", p.netloc, p.path, p.params, p.query, p.fragment))
    except Exception:
        pass
    return u


async def _init_redis_if_needed() -> None:
    global redis
    if redis is not None:
        return

    url = _effective_redis_url()
    if url:
        try:
            import redis.asyncio as r_async  # type: ignore[attr-defined]

            url_eff = _upgrade_to_tls_if_needed(url)
            kwargs: Dict[str, Any] = {
                "encoding": "utf-8",
                "decode_responses": True,
                "socket_timeout": float(_env_float("REDIS_SOCKET_TIMEOUT_SEC", 5.0)),
                "socket_connect_timeout": float(_env_float("REDIS_CONNECT_TIMEOUT_SEC", 5.0)),
                "health_check_interval": int(_env_int("REDIS_HEALTH_CHECK_SEC", 15)),
                "retry_on_timeout": True,
            }
            if url_eff.lower().startswith("rediss://"):
                kwargs.update(
                    {
                        "ssl_cert_reqs": ssl.CERT_REQUIRED,
                        "ssl_ca_certs": certifi.where(),
                        "ssl_check_hostname": True,
                    }
                )

            r = r_async.from_url(url_eff, **kwargs)
            await r.ping()
            redis = r
            log.every("REDIS_INIT_OK", n=30, msg="REDIS_INIT_OK")
            return
        except Exception as e:
            log.every("REDIS_NATIVE_FAIL", n=30, msg="REDIS_NATIVE_FAIL", err=_short(e))

    rest_url = (_env_str("UPSTASH_REDIS_REST_URL") or "").strip()
    rest_tok = (_env_str("UPSTASH_REDIS_REST_TOKEN") or "").strip()
    if rest_url and rest_tok:
        redis = UpstashRestRedis(rest_url, rest_tok)
        log.every("REDIS_UPSTASH_REST_OK", n=30, msg="REDIS_UPSTASH_REST_OK")
        return

    redis = None


def get_redis():
    return redis


async def get_redis_async():
    with contextlib.suppress(Exception):
        if session is None or _session_closed(session):
            await _ensure_session()
    await _init_redis_if_needed()
    return redis


# ──────────────────────────────────────────────────────────────────────────────
# SQLite entry price cache (+ compat shims)
# ──────────────────────────────────────────────────────────────────────────────
async def init_sqlite() -> None:
    global _SQLITE_INITED
    if _SQLITE_INITED:
        return
    async with _SQLITE_LOCK:
        if _SQLITE_INITED:
            return
        os.makedirs(os.path.dirname(SQLITE_DB_PATH) or ".", exist_ok=True)
        async with aiosqlite.connect(SQLITE_DB_PATH) as db:
            await db.execute(
                "CREATE TABLE IF NOT EXISTS sniped_tokens ("
                "token TEXT PRIMARY KEY,"
                "price REAL,"
                "timestamp INTEGER,"
                "unit TEXT"
                ")"
            )
            with contextlib.suppress(Exception):
                await db.execute("ALTER TABLE sniped_tokens ADD COLUMN unit TEXT")
            await db.commit()
        _SQLITE_INITED = True


async def remember_entry_price(token: str, price: float, ts: Optional[int] = None, unit: str = "usd") -> None:
    try:
        p = float(price)
        if p <= 0:
            return
    except Exception:
        return
    if ts is None:
        ts = int(time.time())
    unit = (unit or "usd").strip().lower()
    if unit not in ("usd", "sol"):
        unit = "usd"

    try:
        await init_sqlite()
        async with aiosqlite.connect(SQLITE_DB_PATH) as db:
            await db.execute(
                "INSERT OR REPLACE INTO sniped_tokens (token, price, timestamp, unit) VALUES (?, ?, ?, ?)",
                (str(token), float(p), int(ts), unit),
            )
            await db.commit()
    except Exception:
        pass

    try:
        r = await get_redis_async()
        if r is not None:
            await r.set(f"entry_price_ui:{token}", str(p), ex=_env_int("ENTRY_PRICE_TTL_SEC", 86400))
            await r.set(f"entry_price_unit:{token}", unit, ex=_env_int("ENTRY_PRICE_TTL_SEC", 86400))
    except Exception:
        pass


async def _get_entry_price_sqlite_full(token: str) -> Optional[Tuple[float, int, str]]:
    try:
        await init_sqlite()
        async with aiosqlite.connect(SQLITE_DB_PATH) as db:
            async with db.execute(
                "SELECT price, timestamp, unit FROM sniped_tokens WHERE token = ?",
                (str(token),),
            ) as cur:
                row = await cur.fetchone()
                if not row:
                    return None
                price = _safe_float(row[0], 0.0)
                ts = int(row[1] or 0)
                unit = str(row[2] or "usd").strip().lower() or "usd"
                if unit not in ("usd", "sol"):
                    unit = "usd"
                return (price, ts, unit)
    except Exception:
        return None


async def get_entry_price(arg1: Any, arg2: Optional[str] = None) -> Optional[float]:
    if arg2 is None:
        token = str(arg1)
        info = await _get_entry_price_sqlite_full(token)
        if not info:
            return None
        px, _, _unit = info
        return px if px > 0 else None

    r = arg1
    token = str(arg2)
    try:
        if r is not None:
            raw = await r.get(f"entry_price_ui:{token}")
            if raw is not None:
                if isinstance(raw, (bytes, bytearray)):
                    raw = raw.decode("utf-8", "ignore")
                v = float(raw)
                if v > 0:
                    return v
    except Exception:
        pass

    info = await _get_entry_price_sqlite_full(token)
    if not info:
        return None
    px, _, _unit = info
    return px if px > 0 else None


async def clear_entry_price(token: str) -> None:
    try:
        await init_sqlite()
        async with aiosqlite.connect(SQLITE_DB_PATH) as db:
            await db.execute("DELETE FROM sniped_tokens WHERE token = ?", (str(token),))
            await db.commit()
    except Exception:
        pass
    try:
        r = await get_redis_async()
        if r is not None:
            await r.delete(f"entry_price_ui:{token}")
            await r.delete(f"entry_price_unit:{token}")
    except Exception:
        pass


# compat aliases used across older modules/tests
async def save_entry_price(token: str, price: float, ts: Optional[int] = None, unit: str = "usd") -> None:
    await remember_entry_price(token, price, ts=ts, unit=unit)


async def cache_sniped_token(token: str, ttl_sec: int = 86400) -> None:
    try:
        r = await get_redis_async()
        if r is not None:
            await r.set(f"sniped:{token}", "1", ex=int(ttl_sec))
    except Exception:
        pass


def dca_profit_exit(
    profit_pct: float,
    tiers: Optional[List[Tuple[float, float]]] = None,
) -> float:
    """
    Pure helper: given profit_pct, return fraction_to_sell (0..1).
    Env override: DCA_PROFIT_TIERS="10:0.25,20:0.25,40:0.25,80:0.25"
    """
    if tiers is None:
        tiers = []
        raw = _env_str("DCA_PROFIT_TIERS", "10:0.25,20:0.25,40:0.25,80:0.25")
        for part in raw.split(","):
            part = part.strip()
            if not part:
                continue
            try:
                a, b = part.split(":")
                tiers.append((float(a), float(b)))
            except Exception:
                continue
    p = float(profit_pct or 0.0)
    frac = 0.0
    for thr, sell in tiers:
        if p >= float(thr):
            frac += float(sell)
    return max(0.0, min(1.0, frac))


# ──────────────────────────────────────────────────────────────────────────────
# RPC selection + resource lifecycle
# ──────────────────────────────────────────────────────────────────────────────
def _base_rpc_url() -> str:
    v = _env_str("SOL_RPC_URL", "") or _env_str("RPC_URL", "") or str(RPC_URL or "")
    v = (v or "").strip()
    return v or "https://api.mainnet-beta.solana.com"


async def _select_rpc_urls() -> Tuple[str, str]:
    global _RPC_SELECTED_HTTP, _RPC_SELECTED_WS
    enabled = _env_bool("RPC_FAILOVER_ENABLED", True) and (RF is not None)
    if not enabled:
        http = _base_rpc_url().rstrip("/")
        ws = _env_str("WS_URL", "") or _env_str("SOLANA_WS_URL", "") or ""
        if not ws:
            ws = http.replace("https://", "wss://").replace("http://", "ws://").rstrip("/")
        return http, ws

    try:
        http = await RF.get_http_url(force_probe=False)  # type: ignore[attr-defined]
        ws = await RF.get_ws_url(force_probe=False)  # type: ignore[attr-defined]
        http = (http or "").rstrip("/") or _base_rpc_url().rstrip("/")
        ws = (ws or "").rstrip("/") or http.replace("https://", "wss://").replace("http://", "ws://").rstrip("/")

        if http and http != _RPC_SELECTED_HTTP:
            _RPC_SELECTED_HTTP = http
            os.environ["RPC_URL"] = http
            log.every("RPC_SELECTED_HTTP", n=30, msg="RPC_SELECTED_HTTP", url=http)

        if ws and ws != _RPC_SELECTED_WS:
            _RPC_SELECTED_WS = ws
            os.environ["WS_URL"] = ws
            log.every("RPC_SELECTED_WS", n=30, msg="RPC_SELECTED_WS", url=ws)

        return http, ws
    except Exception as e:
        log.every("RPC_FAILOVER_ERR", n=30, msg="RPC_FAILOVER_ERR", err=_short(e))
        http = _base_rpc_url().rstrip("/")
        ws = http.replace("https://", "wss://").replace("http://", "ws://").rstrip("/")
        return http, ws


async def _ensure_solana_client() -> AsyncClient:
    global solana_client
    http, _ws = await _select_rpc_urls()

    if solana_client is not None:
        try:
            cur = getattr(solana_client, "_provider", None)
            cur_url = getattr(cur, "endpoint_uri", None) if cur is not None else None
            if cur_url and str(cur_url).rstrip("/") == http.rstrip("/"):
                return solana_client
        except Exception:
            pass
        with contextlib.suppress(Exception):
            await solana_client.close()
        solana_client = None

    solana_client = AsyncClient(http, commitment=Confirmed)
    return solana_client


async def _load_wallet_if_needed(*, require: bool) -> Optional[Keypair]:
    global wallet
    if wallet is not None:
        return wallet
    if not require:
        return None

    if SW is not None and hasattr(SW, "load_keypair"):
        wallet = SW.load_keypair()  # type: ignore[attr-defined]
        return wallet

    pk_raw = _env_str("WALLET_PRIVATE_KEY", "") or _env_str("KEYPAIR_PATH", "")
    if not pk_raw:
        raise RuntimeError("Wallet missing: set WALLET_PRIVATE_KEY (json array) or KEYPAIR_PATH")

    if pk_raw.strip().startswith("["):
        arr = json.loads(pk_raw)
    else:
        with open(pk_raw, "r", encoding="utf-8") as f:
            arr = json.loads(f.read().strip())
    b = bytes(arr)
    wallet = Keypair.from_bytes(b) if len(b) == 64 else Keypair.from_seed(b)
    return wallet


async def init_resources(require_wallet: Optional[bool] = None) -> None:
    global _INITED
    async with _INIT_LOCK:
        if _INITED and session is not None and not _session_closed(session) and solana_client is not None:
            return

        await _ensure_session()
        await _ensure_pg_engine()
        await init_sqlite()
        await _init_redis_if_needed()
        await _ensure_solana_client()

        req = _env_bool("WALLET_REQUIRED", False) if require_wallet is None else bool(require_wallet)
        with contextlib.suppress(Exception):
            await _load_wallet_if_needed(require=req)

        _INITED = True
        log.every("CORE_INIT_OK", n=30, msg="CORE_INIT_OK", rpc=os.environ.get("RPC_URL", ""))


async def close_resources() -> None:
    global session, solana_client, wallet, redis, _PG_ENGINE, ALERT_MANAGER, _INITED

    tasks = list(_BACKGROUND_TASKS)
    _BACKGROUND_TASKS.clear()
    for t in tasks:
        with contextlib.suppress(Exception):
            t.cancel()
    if tasks:
        with contextlib.suppress(Exception):
            await asyncio.gather(*tasks, return_exceptions=True)

    with contextlib.suppress(Exception):
        if solana_client is not None:
            await solana_client.close()
    solana_client = None

    try:
        r = redis
        if r is not None:
            if hasattr(r, "aclose"):
                await r.aclose()  # type: ignore[attr-defined]
            elif hasattr(r, "close"):
                out = r.close()  # type: ignore[attr-defined]
                if asyncio.iscoroutine(out):
                    await out
    except Exception:
        pass
    redis = None  # type: ignore[assignment]

    with contextlib.suppress(Exception):
        if _PG_ENGINE is not None:
            await _PG_ENGINE.dispose()
    _PG_ENGINE = None

    with contextlib.suppress(Exception):
        if session is not None and not _session_closed(session):
            await session.close()
    session = None

    ALERT_MANAGER = None
    wallet = None
    _INITED = False


@asynccontextmanager
async def managed_resources(require_wallet: Optional[bool] = None):
    await init_resources(require_wallet=require_wallet)
    try:
        yield
    finally:
        await close_resources()


def get_session():
    return session


def get_solana_client():
    return solana_client


def get_wallet_pubkey() -> str:
    try:
        return str(wallet.pubkey()) if wallet is not None else ""
    except Exception:
        return ""


async def get_wallet_keypair(required: bool = True) -> Optional[Keypair]:
    await init_resources(require_wallet=bool(required))
    return await _load_wallet_if_needed(require=bool(required))


# ──────────────────────────────────────────────────────────────────────────────
# Background tasks
# ──────────────────────────────────────────────────────────────────────────────
def _track_task(task: asyncio.Task[Any]) -> None:
    _BACKGROUND_TASKS.add(task)

    def _done(t: asyncio.Task[Any]) -> None:
        _BACKGROUND_TASKS.discard(t)
        try:
            exc = t.exception()
        except asyncio.CancelledError:
            return
        except Exception:
            return
        if exc:
            log.every("BG_TASK_ERR", n=10, msg="BG_TASK_ERR", err=_short(exc))

    task.add_done_callback(_done)


def create_task(coro: Awaitable[Any], *, name: Optional[str] = None) -> asyncio.Task[Any]:
    try:
        task = asyncio.create_task(coro, name=name) if name else asyncio.create_task(coro)
    except Exception:
        task = asyncio.create_task(coro)
    _track_task(task)
    return task


# ──────────────────────────────────────────────────────────────────────────────
# Pricing (SOL + token USD)
# ──────────────────────────────────────────────────────────────────────────────
async def get_sol_usd() -> float:
    global _SOL_CACHE_TS, _SOL_CACHE_PX
    now = time.time()
    if _SOL_CACHE_PX > 0 and (now - _SOL_CACHE_TS) < float(_env_float("SOL_PRICE_CACHE_SEC", 15.0)):
        return _SOL_CACHE_PX

    await init_resources()
    data = await _http_json("GET", COINGECKO_SOL_API, retries=1, timeout_s=10)
    if data:
        px = _safe_float(((data.get("solana") or {}).get("usd")), 0.0)
        if px > 0:
            _SOL_CACHE_PX = px
            _SOL_CACHE_TS = now
            return px
    return _SOL_CACHE_PX or 0.0


async def convert_sol_to_usd(amount_sol: float) -> float:
    px = await get_sol_usd()
    return float(amount_sol) * px if px > 0 else 0.0


async def convert_sol_to_usd_amount(amount_sol: float) -> float:
    return await convert_sol_to_usd(amount_sol)


async def convert_usd_to_sol(usd_amount: float) -> float:
    px = await get_sol_usd()
    return (float(usd_amount) / px) if px > 0 else 0.0


async def get_pair_snapshot_dexscreener(token_address: str) -> Dict[str, Any]:
    await init_resources()
    url = DEXSCREENER_TOKEN_URL.format(mint=token_address)
    data = await _http_json("GET", url, retries=1, timeout_s=12)
    if not data:
        return {}
    pairs = data.get("pairs") or []
    if not pairs:
        return {}

    def liq(p: Dict[str, Any]) -> float:
        return _safe_float(((p.get("liquidity") or {}).get("usd")), 0.0)

    try:
        return max(pairs, key=liq)
    except Exception:
        return {}


async def get_token_price(token_address: str) -> float:
    ts_px = _TOKEN_CACHE.get(token_address)
    if ts_px:
        ts, px = ts_px
        if px > 0 and (time.time() - ts) < float(_env_float("TOKEN_PRICE_CACHE_SEC", 30.0)):
            return px

    px = 0.0
    snap = await get_pair_snapshot_dexscreener(token_address)
    if snap:
        px = _safe_float(snap.get("priceUsd"), 0.0)

    if px > 0:
        _TOKEN_CACHE[token_address] = (time.time(), float(px))
    return px


# ──────────────────────────────────────────────────────────────────────────────
# Jupiter helpers (prefer lightning_impl)
# ──────────────────────────────────────────────────────────────────────────────
def _slp01_to_bps(v: float) -> int:
    try:
        x = float(v)
    except Exception:
        return int(_env_int("JUP_SLIPPAGE_BPS", 50))
    if x <= 0:
        return int(_env_int("JUP_SLIPPAGE_BPS", 50))
    if x <= 0.2:
        bps = int(round(x * 10_000))
    else:
        bps = int(round((x / 100.0) * 10_000))
    return int(max(1, min(9900, bps)))


def _jup_headers() -> Dict[str, str]:
    hdrs: Dict[str, str] = {}
    if L is not None and hasattr(L, "_jup_headers"):
        with contextlib.suppress(Exception):
            out = L._jup_headers()  # type: ignore[attr-defined]
            if isinstance(out, dict):
                hdrs.update({str(k): str(v) for k, v in out.items() if v is not None})
    api_key = (_env_str("JUP_API_KEY", "") or _env_str("JUP_PRO_KEY", "")).strip()
    if api_key and "x-api-key" not in {k.lower(): v for k, v in hdrs.items()}:
        hdrs["x-api-key"] = api_key
    hdrs.setdefault("user-agent", _env_str("HTTP_UA", "pnlsniper"))
    return hdrs


async def get_jupiter_quote(
    input_mint: str,
    output_mint: str,
    amount_lamports: Optional[int] = None,
    slippage_pct: float = JUPITER_MAX_SLIPPAGE,
    **kwargs: Any,
) -> Optional[Dict[str, Any]]:
    if amount_lamports is None:
        amount_lamports = int(kwargs.get("amount", 0) or 0)
    if int(amount_lamports) <= 0:
        return None

    if guard_jupiter_quote_args is not None:
        with contextlib.suppress(Exception):
            input_mint, output_mint, amount_lamports = guard_jupiter_quote_args(
                input_mint, output_mint, amount_raw=int(amount_lamports), require_anchor=False
            )

    if L is not None and hasattr(L, "_get_quote"):
        with contextlib.suppress(Exception):
            res = await L._get_quote(  # type: ignore[attr-defined]
                input_mint=input_mint,
                output_mint=output_mint,
                amount=int(amount_lamports),
                slippage_bps=_slp01_to_bps(float(slippage_pct)),
            )
            if isinstance(res, dict):
                return res

    await init_resources()
    base = (_env_str("JUP_BASE_URL", "https://api.jup.ag") or "https://api.jup.ag").rstrip("/")
    url = f"{base}/swap/v1/quote"
    params = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": int(amount_lamports),
        "slippageBps": _slp01_to_bps(float(slippage_pct)),
        "swapMode": "ExactIn",
        "onlyDirectRoutes": 1 if _env_bool("JUP_ONLY_DIRECT", False) else 0,
        "asLegacyTransaction": 1 if _env_bool("JUP_AS_LEGACY", False) else 0,
    }
    return await _http_json("GET", url, params=params, headers=_jup_headers(), retries=1, timeout_s=12)


async def swap_token_jupiter(
    input_mint: str,
    output_mint: str,
    amount_float: float,
    *,
    decimals: int = 9,
    slippage: float = JUPITER_MAX_SLIPPAGE,
    buy: Optional[bool] = None,
) -> Tuple[bool, str]:
    await init_resources(require_wallet=True)
    kp = await get_wallet_keypair(required=True)
    if kp is None:
        return (False, "")

    # Prefer lightning_impl’s buy/sell paths if present
    if input_mint == WSOL_MINT and output_mint != WSOL_MINT and L is not None and hasattr(L, "buy_via_lightning"):
        with contextlib.suppress(Exception):
            sig = await L.buy_via_lightning(  # type: ignore[attr-defined]
                mint=output_mint,
                amount_sol=float(amount_float),
                slippage_bps=_slp01_to_bps(float(slippage)),
                return_meta=False,
            )
            return (bool(sig), str(sig or ""))

    if output_mint == WSOL_MINT and input_mint != WSOL_MINT and L is not None and hasattr(L, "sell_via_lightning"):
        with contextlib.suppress(Exception):
            raw = int(round(float(amount_float) * (10 ** int(decimals))))
            sig = await L.sell_via_lightning(  # type: ignore[attr-defined]
                mint=input_mint,
                token_program=None,
                amount_raw=int(raw),
                slippage_bps=_slp01_to_bps(float(slippage)),
            )
            return (bool(sig), str(sig or ""))

    # Generic fallback: quote only (swap building lives in router/lightning)
    q = await get_jupiter_quote(input_mint, output_mint, int(round(float(amount_float) * (10 ** int(decimals)))), slippage_pct=float(slippage))
    if not q:
        return (False, "")
    return (False, "")


# ──────────────────────────────────────────────────────────────────────────────
# Wallet token discovery
# ──────────────────────────────────────────────────────────────────────────────
def _parse_token_account_data(data: bytes) -> Dict[str, Any]:
    if len(data) != 165:
        raise ValueError("Invalid SPL token account length")
    mint = str(PublicKey.from_bytes(bytes(data[0:32])))
    owner = str(PublicKey.from_bytes(bytes(data[32:64])))
    amount = struct.unpack("<Q", data[64:72])[0]
    return {"mint": mint, "owner": owner, "amount": amount}


def _extract_b64_account_data(acc: Any) -> Optional[str]:
    try:
        d = acc.account.data
        if isinstance(d, (list, tuple)) and d and isinstance(d[0], str):
            return d[0]
        if isinstance(d, dict):
            dv = d.get("data")
            if isinstance(dv, (list, tuple)) and dv and isinstance(dv[0], str):
                return dv[0]
        if isinstance(d, str):
            return d
    except Exception:
        return None
    return None


async def get_wallet_tokens(wallet_pubkey: str) -> List[Dict[str, Any]]:
    await init_resources()
    c = solana_client
    if c is None:
        return []
    try:
        owner = PublicKey.from_string(wallet_pubkey)
        resp = await c.get_token_accounts_by_owner(owner, TokenAccountOpts(program_id=TOKEN_PROGRAM_ID))
        out: List[Dict[str, Any]] = []
        for acc in getattr(resp, "value", []) or []:
            raw_b64 = _extract_b64_account_data(acc)
            if not raw_b64:
                continue
            raw = base64.b64decode(raw_b64)
            parsed = _parse_token_account_data(raw)
            out.append({"mint": parsed["mint"], "amount_raw": parsed["amount"]})
        return out
    except Exception:
        return []


async def get_sol_balance(pubkey: Union[str, PublicKey]) -> float:
    await init_resources()
    c = solana_client
    if c is None:
        return 0.0
    try:
        pk = PublicKey.from_string(pubkey) if isinstance(pubkey, str) else pubkey
        resp = await c.get_balance(pk)
        lamports = getattr(resp, "value", None)
        return float(lamports or 0) / LAMPORTS_PER_SOL_F
    except Exception:
        return 0.0


# ──────────────────────────────────────────────────────────────────────────────
# __all__
# ──────────────────────────────────────────────────────────────────────────────
__all__ = [
    "init_resources",
    "close_resources",
    "managed_resources",
    "get_session",
    "get_solana_client",
    "get_wallet_pubkey",
    "get_wallet_keypair",
    "get_redis",
    "get_redis_async",
    "get_pg_engine",
    "get_pg",
    "create_task",
    "get_sol_usd",
    "convert_sol_to_usd",
    "convert_sol_to_usd_amount",
    "convert_usd_to_sol",
    "get_token_price",
    "get_pair_snapshot_dexscreener",
    "get_jupiter_quote",
    "swap_token_jupiter",
    "get_wallet_tokens",
    "get_sol_balance",
    "remember_entry_price",
    "get_entry_price",
    "clear_entry_price",
    "save_entry_price",
    "cache_sniped_token",
    "dca_profit_exit",
]
