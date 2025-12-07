# utils_pkg/core_utils.py
#
# Core infra + quant-grade utilities for the sniper stack (V3+++):
#
#   - Central resource lifecycle:
#       • aiohttp session (shared HTTP client)
#       • Solana AsyncClient + wallet
#       • Redis / Upstash
#       • Async Postgres engine (TRADES + POSITIONS)
#       • SQLite entry-price cache
#
#   - Pricing / market data:
#       • SOL→USD with in-process caching
#       • Token→USD via Dexscreener (primary) + Birdeye (fallback)
#       • Lightweight orderflow / volume snapshots
#
#   - Alerts & telemetry:
#       • Telegram + Discord helpers
#       • AlertManager singleton
#       • Background task tracking for "fire-and-forget" jobs
#
#   - Trade / position logging:
#       • pg_log_trade / log_trade_event
#       • Notion logging (configurable column map)
#       • Positions upsert/close helpers for strategy / risk_manager
#
#   - AI / risk helpers:
#       • Entry-price cache + get_entry_price / clear_entry_price
#       • record_sell_result with AI outcome logging (ai_db)
#       • load_position_context (Redis + PG fusion)
#       • AI fraud / whale detection via external API + Birdeye
#
#   - Jupiter / routing helpers:
#       • get_jupiter_quote / perform_jupiter_swap / swap_token_jupiter
#
#   - Misc:
#       • Wallet token discovery
#       • Savings transfer
#       • Simple indicators (EMA, RSI, ATR)
#       • Convenience helpers used by diagnostics / healthcheck
#
# NOTE: Keep function names / signatures stable – other modules depend on this.

from __future__ import annotations

import asyncio
import base64
import inspect
import json
import logging
import os
import ssl
import struct
import time
from datetime import datetime, timezone
from typing import (
    Any,
    Awaitable,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
)

import aiohttp
import aiosqlite
import certifi
from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Float,
    MetaData,
    String,
    Table,
    UniqueConstraint,
    text,
)
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from logging_setup import get_logger
from .alert_manager import AlertManager

# ──────────────────────────────────────────────────────────────────────────────
# Solana + solders imports
# ──────────────────────────────────────────────────────────────────────────────

from solana.rpc.async_api import AsyncClient
from solana.rpc.commitment import Confirmed
from solana.rpc.types import TokenAccountOpts, TxOpts

from solders.keypair import Keypair
from solders.pubkey import Pubkey as PublicKey

try:
    from solders.transaction import VersionedTransaction as SVTx

    _HAS_VERSIONED = True
except Exception:  # pragma: no cover
    _HAS_VERSIONED = False

try:
    from solders.transaction import Transaction as SLegacyTx  # type: ignore
except Exception:  # pragma: no cover
    SLegacyTx = None  # type: ignore

# system_program import moved between solana-py versions; support both styles
try:
    from solana.system_program import TransferParams, transfer
except Exception:  # pragma: no cover
    TransferParams = None
    transfer = None

# ──────────────────────────────────────────────────────────────────────────────
# Config import (with env fallbacks)
# ──────────────────────────────────────────────────────────────────────────────

try:
    from config import (
        RPC_URL,
        REDIS_URL as CFG_REDIS_URL,
        TELEGRAM_BOT_TOKEN,
        TELEGRAM_CHAT_ID,
        DISCORD_WEBHOOK,
        BIRDEYE_API_KEY,
        BIRDEYE_API_URL,
        JUPITER_SWAP_API,
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
    CFG_REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
    DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK", "")
    BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY", "")
    BIRDEYE_API_URL = os.getenv("BIRDEYE_API_URL", "https://public-api.birdeye.so")
    JUPITER_SWAP_API = os.getenv("JUPITER_SWAP_API", "https://quote-api.jup.ag/v6")
    NOTION_TOKEN = os.getenv("NOTION_TOKEN", "")
    NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID", "")
    ENABLE_NOTION_LOGGING = os.getenv("ENABLE_NOTION_LOGGING", "false").strip().lower() in (
        "1",
        "true",
        "yes",
        "y",
        "on",
    )
    SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "./state.sqlite3")
    PG_ENABLED = os.getenv("PG_ENABLED", "false").strip().lower() in (
        "1",
        "true",
        "yes",
        "y",
        "on",
    )
    JUPITER_MAX_SLIPPAGE = float(os.getenv("JUPITER_MAX_SLIPPAGE", "0.005"))
    SAVINGS_WALLET = os.getenv("SAVINGS_WALLET", "")
    FRAUD_API_URL = os.getenv("FRAUD_API_URL", "")
    FRAUD_API_KEY = os.getenv("FRAUD_API_KEY", "")
    SCANNER_MIN_FRAUD_SCORE = float(os.getenv("SCANNER_MIN_FRAUD_SCORE", "70"))

# These are always read from env so they work regardless of config import
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "").strip()
HELIUS_URL = (
    os.getenv("HELIUS_URL", "").strip()
    or (f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}" if HELIUS_API_KEY else "")
)
RPC_FALLBACK_URL = os.getenv("RPC_FALLBACK_URL", "").strip()

# Redis envs (we may override config value)
REDIS_URL = (os.getenv("REDIS_URL") or CFG_REDIS_URL or "").strip()
UPSTASH_REDIS_REST_URL = (os.getenv("UPSTASH_REDIS_REST_URL") or "").strip()
UPSTASH_REDIS_REST_TOKEN = (os.getenv("UPSTASH_REDIS_REST_TOKEN") or "").strip()

# Price cache knobs (seconds)
SOL_PRICE_CACHE_SEC = float(os.getenv("SOL_PRICE_CACHE_SEC", "15"))
TOKEN_PRICE_CACHE_SEC = float(os.getenv("TOKEN_PRICE_CACHE_SEC", "30"))

# Constants
TOKEN_PROGRAM_ID = PublicKey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
WSOL_MINT = "So11111111111111111111111111111111111111112"
LAMPORTS_PER_SOL = 1_000_000_000
LAMPORTS_PER_SOL_F = float(LAMPORTS_PER_SOL)
COINGECKO_SOL_API = "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd"
DEXSCREENER_TOKEN_URL = "https://api.dexscreener.com/latest/dex/tokens/{mint}"

# Logging
logger = logging.getLogger("core_utils")
log = get_logger(__name__)
jlog = logging.getLogger("core_utils.jito")

# SSL for aiohttp
ssl_context = ssl.create_default_context(cafile=certifi.where())

# ──────────────────────────────────────────────────────────────────────────────
# Globals populated by init_resources()
# ──────────────────────────────────────────────────────────────────────────────

session: Optional[aiohttp.ClientSession] = None
solana_client: Optional[AsyncClient] = None
wallet: Optional[Keypair] = None

redis = None  # type: ignore[assignment]

ALERT_MANAGER: Optional[AlertManager] = None
_BACKGROUND_TASKS: "set[asyncio.Task[Any]]" = set()

# In-process price caches
_SOL_CACHE_TS: float = 0.0
_SOL_CACHE_PX: float = 0.0
_TOKEN_CACHE: Dict[str, Tuple[float, float]] = {}  # mint -> (ts, px)

# PG globals
PG_METADATA = MetaData()
_PG_ENGINE: Optional[AsyncEngine] = None

# ──────────────────────────────────────────────────────────────────────────────
# Postgres schema (TRADES + POSITIONS)
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
    Column("profit", Float, nullable=True),  # legacy "profit" (use as pnl_usd)
    Column("reason", String(128), nullable=True),
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
# PG helpers (DSN + enable flags)
# ──────────────────────────────────────────────────────────────────────────────

def _pg_enabled() -> bool:
    """
    Decide if Postgres should be used.

    Priority:
      - POSITIONS_PG_ENABLED env
      - PG_ENABLED (config/env)
      - default: True if any DSN is present
    """
    env_flag = os.getenv("POSITIONS_PG_ENABLED")
    if env_flag is not None:
        return env_flag.strip().lower() in {"1", "true", "yes", "y", "on"}

    try:
        if isinstance(PG_ENABLED, bool):
            return PG_ENABLED
    except Exception:
        pass

    return _get_pg_dsn() is not None


def _normalize_dsn(raw: str) -> str:
    """
    Normalize Render-style DSNs into asyncpg-compatible URLs when needed.

    - postgres:// → postgresql+asyncpg://
    - postgresql:// without driver → postgresql+asyncpg://
    - already has +asyncpg → unchanged
    """
    if "+asyncpg" in raw:
        return raw

    if raw.startswith("postgres://"):
        return "postgresql+asyncpg://" + raw[len("postgres://") :]

    if raw.startswith("postgresql://"):
        return "postgresql+asyncpg://" + raw[len("postgresql://") :]

    return raw


def _get_pg_dsn() -> Optional[str]:
    """
    Choose the best DSN from env.

    Priority:
      1) POSITIONS_PG_DSN
      2) SNIPER_DB_DSN / DATABASE_URL
      3) PG_DSN / PG_DSN_INTERNAL / PG_DSN_EXTERNAL
    """
    for key in (
        "POSITIONS_PG_DSN",
        "SNIPER_DB_DSN",
        "DATABASE_URL",
        "PG_DSN",
        "PG_DSN_INTERNAL",
        "PG_DSN_EXTERNAL",
    ):
        v = os.getenv(key)
        if v and v.strip():
            return _normalize_dsn(v.strip())
    return None


async def _ensure_pg_engine() -> Optional[AsyncEngine]:
    """
    Create the global async SQLAlchemy engine if enabled & DSN present.
    Safe to call multiple times.
    """
    global _PG_ENGINE

    if _PG_ENGINE is not None:
        return _PG_ENGINE

    backend = (os.getenv("PF_POSITIONS_BACKEND", "pg") or "pg").strip().lower()
    enabled = _pg_enabled() and backend == "pg"

    if not enabled:
        logger.info("PG_INIT_SKIP", extra={"enabled": enabled, "backend": backend})
        return None

    dsn = _get_pg_dsn()
    if not dsn:
        logger.warning("PG_INIT_SKIP: no_dsn_env")
        return None

    try:
        _PG_ENGINE = create_async_engine(
            dsn,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=5,
        )
        mask = dsn.split("@", 1)[-1] if "@" in dsn else dsn
        logger.info("PG_INIT_OK", extra={"dsn_mask": mask})
        async with _PG_ENGINE.begin() as conn:
            await conn.execute(text("SELECT 1"))
            await conn.run_sync(PG_METADATA.create_all)
        return _PG_ENGINE
    except Exception as e:
        logger.error("PG_INIT_FAIL", extra={"err": str(e)[:200]})
        if _PG_ENGINE is not None:
            try:
                await _PG_ENGINE.dispose()
            except Exception:
                pass
        _PG_ENGINE = None
        return None


def get_pg_engine() -> Optional[AsyncEngine]:
    """Return global AsyncEngine (or None)."""
    return _PG_ENGINE


async def get_pg() -> Optional[AsyncEngine]:
    """Async helper: ensure Postgres is initialized and return engine."""
    await _ensure_pg_engine()
    return _PG_ENGINE


# ──────────────────────────────────────────────────────────────────────────────
# SQLite (entry price cache)
# ──────────────────────────────────────────────────────────────────────────────

async def init_sqlite() -> None:
    os.makedirs(os.path.dirname(SQLITE_DB_PATH) or ".", exist_ok=True)
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        await db.execute(
            "CREATE TABLE IF NOT EXISTS sniped_tokens "
            "(token TEXT PRIMARY KEY, price REAL, timestamp INTEGER)"
        )
        await db.commit()


async def remember_entry_price(token: str, price: float, ts: Optional[int] = None) -> None:
    """
    Cache last known entry price for a token in sqlite (token -> price, timestamp).

    price can be USD or SOL; callers should be consistent.
    """
    try:
        p = float(price)
        if p <= 0:
            return
    except Exception:
        return

    if ts is None:
        ts = int(time.time())

    try:
        async with aiosqlite.connect(SQLITE_DB_PATH) as db:
            await db.execute(
                "INSERT OR REPLACE INTO sniped_tokens (token, price, timestamp) VALUES (?, ?, ?)",
                (str(token), float(p), int(ts)),
            )
            await db.commit()
    except Exception as e:
        logger.warning(f"remember_entry_price failed: {e}")


async def _get_entry_price_sqlite(token: str) -> Optional[Tuple[float, int]]:
    """
    Internal helper: fetch (price, ts) from sqlite, or None.
    """
    try:
        async with aiosqlite.connect(SQLITE_DB_PATH) as db:
            async with db.execute(
                "SELECT price, timestamp FROM sniped_tokens WHERE token = ?",
                (str(token),),
            ) as cur:
                row = await cur.fetchone()
                if not row:
                    return None
                price, ts = row
                return float(price or 0.0), int(ts or 0)
    except Exception as e:
        logger.warning(f"_get_entry_price_sqlite failed: {e}")
        return None


async def get_entry_price(arg1: Any, arg2: Optional[str] = None) -> Optional[float]:
    """
    Flexible entry price loader used by ai_sell_logic and exit logging.

    Supported call styles:
      - await get_entry_price(token)
      - await get_entry_price(redis, token)

    When Redis is provided, we first check entry_price_ui:{mint}, then sqlite.
    """
    # style: get_entry_price(token)
    if arg2 is None:
        token = str(arg1)
        info = await _get_entry_price_sqlite(token)
        if not info:
            return None
        price, _ = info
        return float(price) if price > 0 else None

    # style: get_entry_price(redis, token)
    redis_client = arg1
    token = str(arg2)
    price_val: Optional[float] = None

    try:
        if redis_client is not None:
            raw = await redis_client.get(f"entry_price_ui:{token}")
            if raw is not None:
                if isinstance(raw, (bytes, bytearray)):
                    raw = raw.decode("utf-8", "ignore")
                try:
                    v = float(raw)
                    if v > 0:
                        price_val = v
                except Exception:
                    price_val = None
    except Exception:
        price_val = None

    if price_val is None or price_val <= 0:
        info = await _get_entry_price_sqlite(token)
        if info is not None:
            price_val, _ = info

    if price_val is None or price_val <= 0:
        return None
    return float(price_val)


async def clear_entry_price(token: str) -> None:
    """
    Remove a token's cached entry price from sqlite and Redis.
    """
    try:
        async with aiosqlite.connect(SQLITE_DB_PATH) as db:
            await db.execute("DELETE FROM sniped_tokens WHERE token = ?", (str(token),))
            await db.commit()
    except Exception as e:
        logger.warning(f"clear_entry_price failed: {e}")

    try:
        if redis is not None:
            await redis.delete(f"entry_price_ui:{token}")
    except Exception:
        pass


# ──────────────────────────────────────────────────────────────────────────────
# Wallet parsing / RPC URL selection
# ──────────────────────────────────────────────────────────────────────────────

def _parse_wallet_from_env() -> Keypair:
    pk_raw = os.getenv("WALLET_PRIVATE_KEY")
    if not pk_raw:
        raise RuntimeError("WALLET_PRIVATE_KEY missing in environment")

    try:
        txt = pk_raw.strip()
        if txt.startswith("["):
            arr = json.loads(txt)
        else:
            with open(txt, "r", encoding="utf-8") as f:
                arr = json.loads(f.read().strip())

        b = bytes(arr)
        if len(b) == 64:
            return Keypair.from_bytes(b)
        if len(b) == 32:
            return Keypair.from_seed(b)
        raise ValueError(f"Invalid key length {len(b)}; expected 32 or 64 bytes")
    except Exception as e:
        raise RuntimeError(
            "WALLET_PRIVATE_KEY must be a JSON array of 32/64 ints, "
            "or an absolute path to a file containing that JSON."
        ) from e


def _effective_rpc_url() -> str:
    if HELIUS_URL:
        return HELIUS_URL
    if RPC_URL:
        return RPC_URL
    if RPC_FALLBACK_URL:
        return RPC_FALLBACK_URL
    return "https://api.mainnet-beta.solana.com"


# ──────────────────────────────────────────────────────────────────────────────
# Initialization / shutdown (HTTP, Solana, wallet, PG, sqlite)
# ──────────────────────────────────────────────────────────────────────────────

async def init_resources() -> None:
    """
    Initialize aiohttp session, Postgres, sqlite, Solana client, and wallet.
    Redis is initialized at import time (if available).
    """
    global session, solana_client, wallet

    # If everything important already exists, keep it
    if session and not session.closed and solana_client and wallet:
        return

    await _ensure_pg_engine()
    await init_sqlite()

    if session is None or session.closed:
        session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=ssl_context, limit=100),
            timeout=aiohttp.ClientTimeout(total=20),
            json_serialize=lambda x: json.dumps(x, separators=(",", ":"), ensure_ascii=False),
        )

    if solana_client is None:
        solana_client = AsyncClient(_effective_rpc_url())

    if wallet is None:
        wallet = _parse_wallet_from_env()

    logger.info("init_resources ready")


async def close_resources() -> None:
    global session, solana_client, redis, wallet, _PG_ENGINE

    try:
        if session and not session.closed:
            await session.close()
    finally:
        session = None

    try:
        if _PG_ENGINE is not None:
            await _PG_ENGINE.dispose()
    except Exception:
        pass
    finally:
        _PG_ENGINE = None

    try:
        if solana_client:
            await solana_client.close()
    finally:
        solana_client = None

    try:
        if redis is not None:
            await redis.close()  # type: ignore[attr-defined]
    except Exception:
        pass
    finally:
        redis = None  # type: ignore[assignment]

    wallet = None
    logger.info("close_resources done")


def get_redis():
    return redis


def get_session():
    return session


def get_solana_client():
    return solana_client


def get_wallet_pubkey() -> str:
    try:
        return str(wallet.pubkey()) if wallet else ""
    except Exception:
        return ""


# ──────────────────────────────────────────────────────────────────────────────
# Redis / Upstash init
# ──────────────────────────────────────────────────────────────────────────────

try:
    import redis.asyncio as _redis_async  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    _redis_async = None
    try:
        import aioredis as _redis_async  # type: ignore[assignment, import]
    except Exception:
        _redis_async = None  # type: ignore[assignment]


def _mask_redis_url(u: str) -> str:
    if not u:
        return ""
    try:
        return u.split("@", 1)[-1]
    except Exception:
        return "<redis>"


if REDIS_URL and _redis_async is not None:
    try:
        redis = _redis_async.from_url(  # type: ignore[attr-defined]
            REDIS_URL,
            encoding="utf-8",
            decode_responses=True,
        )
        log.event("REDIS_INIT_OK", url_mask=_mask_redis_url(REDIS_URL))
    except Exception as e:  # pragma: no cover
        redis = None
        log.event(
            "REDIS_INIT_FAIL", error=str(e)[:200], url_mask=_mask_redis_url(REDIS_URL)
        )
else:
    reason = "no_url" if not REDIS_URL else "no_client_lib"
    log.event("REDIS_INIT_SKIP", reason=reason)


# ──────────────────────────────────────────────────────────────────────────────
# Price helpers (SOL + tokens) – used across stack
# ──────────────────────────────────────────────────────────────────────────────

async def get_sol_usd() -> float:
    """
    Fetch SOL→USD price with in-process caching.
    """
    global _SOL_CACHE_TS, _SOL_CACHE_PX

    now = time.time()
    if _SOL_CACHE_PX > 0 and (now - _SOL_CACHE_TS) < SOL_PRICE_CACHE_SEC:
        return _SOL_CACHE_PX

    if not session:
        return _SOL_CACHE_PX or 0.0

    try:
        async with session.get(COINGECKO_SOL_API) as resp:
            if resp.status == 200:
                data = await resp.json()
                px = float(data.get("solana", {}).get("usd", 0) or 0)
                if px > 0:
                    _SOL_CACHE_PX = px
                    _SOL_CACHE_TS = now
                    return px
    except Exception:
        pass

    return _SOL_CACHE_PX or 0.0


async def convert_sol_to_usd(amount_sol: float) -> float:
    """
    Convert a SOL amount → USD using cached SOL price.
    Used by healthcheck, risk and diagnostics.
    """
    px = await get_sol_usd()
    return float(amount_sol) * px if px > 0 else 0.0


async def convert_sol_to_usd_amount(amount_sol: float) -> float:
    """
    Backwards-compatible alias for convert_sol_to_usd.
    """
    return await convert_sol_to_usd(amount_sol)


async def convert_usd_to_sol(usd_amount: float) -> float:
    px = await get_sol_usd()
    return (usd_amount / px) if px > 0 else 0.0


async def _get_cached_token_price(mint: str) -> Optional[float]:
    now = time.time()
    ts_px = _TOKEN_CACHE.get(mint)
    if not ts_px:
        return None
    ts, px = ts_px
    if px > 0 and (now - ts) < TOKEN_PRICE_CACHE_SEC:
        return px
    return None


def _set_cached_token_price(mint: str, px: float) -> None:
    if px <= 0:
        return
    _TOKEN_CACHE[mint] = (time.time(), float(px))


async def get_token_price(token_address: str) -> float:
    """
    Return token price in USD with Dexscreener-first, Birdeye fallback + caching.
    """
    cached = await _get_cached_token_price(token_address)
    if cached is not None:
        return cached

    if not session:
        return 0.0

    px: float = 0.0

    # 1) Dexscreener
    try:
        async with session.get(DEXSCREENER_TOKEN_URL.format(mint=token_address)) as resp:
            if resp.status == 200:
                data = await resp.json()
                pairs = data.get("pairs") or []
                if pairs:
                    top = max(
                        pairs,
                        key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0.0),
                    )
                    v = float(top.get("priceUsd") or 0.0)
                    if v > 0:
                        px = v
    except Exception:
        px = 0.0

    # 2) Birdeye fallback
    if px <= 0:
        try:
            headers = {"X-API-KEY": BIRDEYE_API_KEY} if BIRDEYE_API_KEY else {}
            url = f"{BIRDEYE_API_URL.rstrip('/')}/public/price?address={token_address}"
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    val = data.get("data", {}).get("value")
                    if val is not None:
                        px = float(val)
        except Exception:
            px = 0.0

    if px > 0:
        _set_cached_token_price(token_address, px)
    return px


async def get_pair_snapshot_dexscreener(token_address: str) -> Dict[str, Any]:
    """
    Raw pair snapshot from Dexscreener (highest-liquidity pair).
    """
    out: Dict[str, Any] = {}
    if not session:
        return out
    try:
        async with session.get(DEXSCREENER_TOKEN_URL.format(mint=token_address)) as resp:
            if resp.status != 200:
                return out
            data = await resp.json()
            pairs = data.get("pairs") or []
            if not pairs:
                return out

            def liq(p: Dict[str, Any]) -> float:
                try:
                    return float((p.get("liquidity") or {}).get("usd") or 0)
                except Exception:
                    return 0.0

            top = max(pairs, key=liq)
            return top
    except Exception:
        return out


async def get_orderflow_snapshot(mint: str) -> Dict[str, Any]:
    """
    Lightweight recent orderflow snapshot from Dexscreener 5m data.

    Returns:
      {
        "liq_usd": float,
        "price_usd": float,
        "buys": int,
        "sells": int,
        "skew": float
      }
    """
    snap = await get_pair_snapshot_dexscreener(mint)
    if not snap:
        return {
            "liq_usd": 0.0,
            "price_usd": 0.0,
            "buys": 0,
            "sells": 0,
            "skew": 1.0,
        }

    liq_usd = float((snap.get("liquidity") or {}).get("usd") or 0.0)
    price_usd = float(snap.get("priceUsd") or 0.0)
    tx = (snap.get("txns") or {}).get("m5") or {}
    buys = int(tx.get("buys", 0) or 0)
    sells = int(tx.get("sells", 0) or 0)
    skew = (buys + 1) / (sells + 1)
    return {
        "liq_usd": liq_usd,
        "price_usd": price_usd,
        "buys": buys,
        "sells": sells,
        "skew": skew,
    }


async def get_volume_data(token_address: str) -> Dict[str, Any]:
    """
    Lightweight trend/volume snapshot used by ai_sell_logic.

    Returns:
      {
        "direction": "up" | "down" | "flat",
        "pct_change_h1": float,
        "candles": []    # reserved for future OHLC data
      }
    """
    base: Dict[str, Any] = {
        "direction": "flat",
        "pct_change_h1": 0.0,
        "candles": [],
    }
    snap = await get_pair_snapshot_dexscreener(token_address)
    if not snap:
        return base

    pct_h1 = 0.0
    try:
        change_blob = snap.get("priceChange") or {}
        pct_h1 = float(change_blob.get("h1") or 0.0)
    except Exception:
        pct_h1 = 0.0

    direction = "flat"
    if pct_h1 > 3.0:
        direction = "up"
    elif pct_h1 < -3.0:
        direction = "down"

    base["direction"] = direction
    base["pct_change_h1"] = pct_h1
    return base


# ──────────────────────────────────────────────────────────────────────────────
# Alerts (Telegram / Discord) + AlertManager
# ──────────────────────────────────────────────────────────────────────────────

async def send_telegram(message: str, sol_value: Optional[float] = None) -> None:
    """
    Send a Telegram message. If sol_value is provided, approximate USD value.
    """
    if not session or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        if sol_value is not None:
            px = await get_sol_usd()
            if px > 0:
                usd = sol_value * px
                message += f"\n~${usd:.2f} USD"
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "disable_web_page_preview": True,
            "parse_mode": "HTML",
        }
        async with session.post(url, json=payload) as resp:
            if resp.status != 200:
                logger.warning(f"Telegram [{resp.status}] {await resp.text()}")
    except Exception as e:
        logger.warning(f"send_telegram error: {e}")


async def send_discord_alert(title: str, fields: List[Dict[str, Any]]) -> None:
    if not session or not DISCORD_WEBHOOK:
        return
    embed = {
        "title": title,
        "color": 0x2F80ED,
        "fields": fields,
        "timestamp": datetime.utcnow().isoformat(),
    }
    try:
        async with session.post(DISCORD_WEBHOOK, json={"embeds": [embed]}) as resp:
            if resp.status not in (200, 201, 204):
                logger.warning(f"Discord [{resp.status}] {await resp.text()}")
    except Exception as e:
        logger.warning(f"send_discord_alert error: {e}")


def get_alert_manager() -> Optional[AlertManager]:
    """
    Lazily create / return a shared AlertManager instance.
    """
    global ALERT_MANAGER
    if ALERT_MANAGER is not None:
        return ALERT_MANAGER

    try:
        sig = inspect.signature(AlertManager)
        if "session" in sig.parameters:
            ALERT_MANAGER = AlertManager(session=session)  # type: ignore[call-arg]
        else:
            ALERT_MANAGER = AlertManager()  # type: ignore[call-arg]
    except Exception as e:
        logger.warning(f"get_alert_manager init failed: {e}")
        ALERT_MANAGER = None
    return ALERT_MANAGER


def get_am() -> Optional[AlertManager]:
    return get_alert_manager()


def am() -> Optional[AlertManager]:
    return get_alert_manager()


def _am() -> Optional[AlertManager]:
    return get_alert_manager()


# ──────────────────────────────────────────────────────────────────────────────
# Background task helper (used by exit worker + AI logging)
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
            logger.warning(f"[bg_task_error] {exc}")

    task.add_done_callback(_done)


def create_task(coro: Awaitable[Any], *, name: Optional[str] = None) -> asyncio.Task[Any]:
    """
    Thin wrapper around asyncio.create_task that tracks background tasks and logs errors.
    """
    if name is not None and hasattr(asyncio, "create_task"):
        task = asyncio.create_task(coro, name=name)  # type: ignore[arg-type]
    else:
        task = asyncio.create_task(coro)
    _track_task(task)
    return task


# ──────────────────────────────────────────────────────────────────────────────
# Indicators (EMA, RSI, ATR) – for quick quant experiments
# ──────────────────────────────────────────────────────────────────────────────

def ema(xs: List[float], n: int) -> List[float]:
    if not xs:
        return []
    k = 2 / (n + 1)
    out = [xs[0]]
    for x in xs[1:]:
        out.append(out[-1] + k * (x - out[-1]))
    return out


def rsi(closes: List[float], period: int = 14) -> List[float]:
    if len(closes) < period + 1:
        return [50.0] * len(closes)
    gains: List[float] = []
    losses: List[float] = []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    out: List[float] = [50.0] * period
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
        rs = (avg_g / avg_l) if avg_l > 1e-12 else 999.0
        out.append(100 - 100 / (1 + rs))
    return [50.0] + out


def atr(ohlc: List[Dict[str, float]], period: int = 14) -> List[float]:
    if not ohlc:
        return []
    trs: List[float] = []
    prev_c = ohlc[0]["close"]
    for c in ohlc:
        hi, lo, cl = c["high"], c["low"], c["close"]
        tr = max(hi - lo, abs(hi - prev_c), abs(lo - prev_c))
        trs.append(tr)
        prev_c = cl
    return ema(trs, period)


# ──────────────────────────────────────────────────────────────────────────────
# AI fraud / whale helpers
# ──────────────────────────────────────────────────────────────────────────────

async def ai_fraud_score(token_address: str) -> float:
    """
    Call external FRAUD_API_URL (if configured) to get 0..1 fraud score.
    """
    if not session or not FRAUD_API_URL:
        return 0.0
    try:
        headers = {"Authorization": f"Bearer {FRAUD_API_KEY}"} if FRAUD_API_KEY else {}
        url = f"{FRAUD_API_URL.rstrip('/')}?token={token_address}"
        async with session.get(url, headers=headers, timeout=5) as resp:
            if resp.status == 200:
                data = await resp.json()
                score = float(data.get("fraud_score", 0.0))
                return max(0.0, min(1.0, score))
            return 0.0
    except Exception:
        return 0.0


async def fraud_score_ai(token_address: str) -> float:
    """
    Returns fraud score as 0..100 for scanners that expect percentage.
    """
    s01 = await ai_fraud_score(token_address)
    return round(max(0.0, min(1.0, s01)) * 100, 2)


async def check_rug_pull_risk(token_address: str) -> bool:
    """
    Returns True if token's fraud score is above SCANNER_MIN_FRAUD_SCORE (0..100).
    """
    score = await fraud_score_ai(token_address)
    return score >= float(SCANNER_MIN_FRAUD_SCORE)


async def is_whale_wallet(token_mint: str, usd_threshold: float = 50_000) -> bool:
    """
    Simple whale detection via Birdeye top holders API.
    """
    if not session or not BIRDEYE_API_KEY:
        return False
    headers = {"X-API-KEY": BIRDEYE_API_KEY}
    urls = [
        f"{BIRDEYE_API_URL.rstrip('/')}/defi/holders?address={token_mint}&page=1&limit=20",
        f"{BIRDEYE_API_URL.rstrip('/')}/token/{token_mint}/holders",
    ]
    for url in urls:
        try:
            async with session.get(url, headers=headers, timeout=8) as resp:
                if resp.status != 200:
                    continue
                data = await resp.json()
                holders = data.get("data", {}).get("items") or data.get("data") or []
                for h in holders:
                    val = h.get("value_usd") or h.get("valueUsd") or h.get("usdValue") or 0
                    try:
                        if float(val) >= usd_threshold:
                            return True
                    except Exception:
                        continue
        except Exception:
            continue
    return False


# ──────────────────────────────────────────────────────────────────────────────
# Risk / position context helpers
# ──────────────────────────────────────────────────────────────────────────────

async def load_position_context(wallet_pubkey: Optional[str] = None) -> Dict[str, Any]:
    """
    Centralized "risk context" snapshot, used by:
      - ai_entry_logic (for context-aware entries)
      - data_logger (position_context field)

    It primarily reads from Redis hash risk:ctx (if present) with keys like:
      - open_positions
      - max_positions
      - daily_realized_pnl_pct
      - daily_drawdown_pct
      - risk_mode (0=off,1=normal,2=aggressive,3=defensive)

    If Redis is missing/empty, it falls back to safe defaults.
    Optionally augments open_positions using PG positions table.
    """
    ctx: Dict[str, Any] = {
        "open_positions": 0.0,
        "max_positions": 0.0,
        "daily_realized_pnl_pct": 0.0,
        "daily_drawdown_pct": 0.0,
        "risk_mode": 0.0,
    }

    r = get_redis()
    if r is not None:
        try:
            raw = await r.hgetall("risk:ctx")
            if raw:
                def _flt(k: str, default: float = 0.0) -> float:
                    v = raw.get(k)
                    if v is None:
                        return default
                    try:
                        return float(v)
                    except Exception:
                        return default

                ctx["open_positions"] = _flt("open_positions", 0.0)
                ctx["max_positions"] = _flt("max_positions", 0.0)
                ctx["daily_realized_pnl_pct"] = _flt("daily_realized_pnl_pct", 0.0)
                ctx["daily_drawdown_pct"] = _flt("daily_drawdown_pct", 0.0)
                ctx["risk_mode"] = _flt("risk_mode", 0.0)
        except Exception:
            pass

    # Optional augmentation from Postgres (for wallet-specific open positions)
    if wallet_pubkey:
        eng = await get_pg()
        if eng is not None:
            try:
                async with eng.connect() as conn:
                    result = await conn.execute(
                        text(
                            """
                            SELECT COUNT(*) AS cnt
                            FROM positions
                            WHERE wallet = :w
                              AND status = 'open'
                            """
                        ),
                        {"w": wallet_pubkey},
                    )
                    row = result.fetchone()
                    if row and row[0] is not None:
                        ctx["open_positions"] = float(row[0])
            except Exception:
                pass

    return ctx


# ──────────────────────────────────────────────────────────────────────────────
# Notion + Postgres trade logging
# ──────────────────────────────────────────────────────────────────────────────

def _notion_fields_map() -> Dict[str, str]:
    def env_first(keys: List[str], default: str) -> str:
        for k in keys:
            v = os.getenv(k)
            if v and str(v).strip():
                return str(v).strip()
        return default

    defaults: Dict[str, str] = {
        "title": env_first(["NOTION_COL_TITLE", "NOTION_TITLE_PROP"], "Token"),
        "mint": env_first(["NOTION_COL_MINT", "NOTION_PROP_TOKEN"], "Mint"),
        "balance": env_first(["NOTION_COL_BALANCE", "NOTION_PROP_BALANCE"], ""),
        "entry": env_first(["NOTION_COL_ENTRY", "NOTION_PROP_ENTRY"], "Entry Price"),
        "current": env_first(["NOTION_COL_CURRENT", "NOTION_PROP_CURRENT"], "Current Price"),
        "profit": env_first(["NOTION_COL_PROFIT", "NOTION_PROP_PROFIT"], "Profit"),
        "reason": env_first(["NOTION_COL_REASON", "NOTION_PROP_REASON"], "Reason"),
        "timestamp": env_first(["NOTION_COL_TIMESTAMP", "NOTION_PROP_TIME"], "Timestamp"),
    }
    try:
        blob = os.getenv("NOTION_FIELDS_JSON") or os.getenv("NOTION_MAP_JSON") or ""
        if blob.strip():
            extra = json.loads(blob)
            if isinstance(extra, dict):
                defaults.update({str(k): str(v) for k, v in extra.items()})
    except Exception:
        pass
    return defaults


async def log_to_notion(
    token: str,
    symbol: str,
    balance: float,
    entry: float,
    current: float,
    profit: float,
    reason: str,
) -> None:
    if (
        not ENABLE_NOTION_LOGGING
        or not NOTION_TOKEN
        or not NOTION_DATABASE_ID
        or not session
    ):
        return

    fields = _notion_fields_map()
    props: Dict[str, Any] = {
        fields["title"]: {"title": [{"text": {"content": symbol or token}}]},
        fields["mint"]: {"rich_text": [{"text": {"content": token}}]},
    }
    if fields.get("balance"):
        props[fields["balance"]] = {"number": float(balance)}
    if fields.get("entry"):
        props[fields["entry"]] = {"number": float(entry) if entry is not None else None}
    if fields.get("current"):
        props[fields["current"]] = {"number": float(current) if current is not None else None}
    if fields.get("profit"):
        props[fields["profit"]] = {"number": float(profit) if profit is not None else None}
    if fields.get("reason") and reason:
        props[fields["reason"]] = {"rich_text": [{"text": {"content": reason}}]}
    if fields.get("timestamp"):
        props[fields["timestamp"]] = {"date": {"start": datetime.utcnow().isoformat()}}

    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }
    body = {"parent": {"database_id": NOTION_DATABASE_ID}, "properties": props}
    try:
        async with session.post("https://api.notion.com/v1/pages", headers=headers, json=body) as resp:
            if resp.status not in (200, 201):
                txt = await resp.text()
                logger.warning(f"[notion_alert] HTTP {resp.status}: {txt}")
    except Exception as e:
        logger.warning(f"log_to_notion failed: {e}")


async def pg_log_trade(
    *,
    token: str,
    symbol: Optional[str],
    side: Optional[str],
    amount: Optional[float],
    entry: Optional[float],
    current: Optional[float],
    profit: Optional[float],
    reason: Optional[str],
    signature: Any | None = None,
) -> None:
    eng = await get_pg()
    if eng is None:
        return

    sig_txt: Optional[str] = None
    try:
        if isinstance(signature, str):
            sig_txt = signature
        else:
            from solders.signature import Signature as SoldersSig  # type: ignore

            if isinstance(signature, SoldersSig):
                sig_txt = str(signature)
    except Exception:
        pass

    try:
        async with eng.begin() as conn:
            await conn.execute(
                TRADES.insert().values(
                    ts=datetime.utcnow(),
                    token=token,
                    symbol=symbol,
                    side=side,
                    amount=amount,
                    entry=entry,
                    current=current,
                    profit=profit,
                    reason=reason,
                    signature=sig_txt,
                )
            )
    except Exception as e:
        logger.warning(f"pg_log_trade failed: {e}")


async def log_trade_event(
    token: str,
    symbol: str,
    balance: float,
    entry: float,
    current: float,
    profit: float,
    reason: str,
    *,
    side: Optional[str] = None,
    signature: Any | None = None,
) -> None:
    """
    Backwards-compatible trade logger (used mainly for exits / PnL).
    """
    await pg_log_trade(
        token=token,
        symbol=symbol,
        side=side,
        amount=balance,
        entry=entry,
        current=current,
        profit=profit,
        reason=reason,
        signature=signature,
    )
    try:
        await log_to_notion(
            token=token,
            symbol=symbol,
            balance=balance,
            entry=entry,
            current=current,
            profit=profit,
            reason=reason,
        )
    except Exception:
        pass


# ──────────────────────────────────────────────────────────────────────────────
# AI outcome logger used by auto_exit_worker (record_sell_result)
# ──────────────────────────────────────────────────────────────────────────────

def record_sell_result(
    *,
    ok: bool,
    reason: str,
    mint: str,
    sig: Any | None = None,
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Best-effort outcome logger used by auto_exit_worker.

    - Computes approximate PnL from entry_price vs current price (USD basis).
    - Estimates hold_seconds using Redis buytime:{mint} if available.
    - Writes a row into AI tables via ai_db.insert_entry_outcome(...) (if present).
    - Logs to Postgres TRADES + Notion via log_trade_event.
    - Clears the entry-price cache.
    """
    meta = dict(meta or {})

    async def _do() -> None:
        entry_price: Optional[float] = None
        entry_ts: Optional[datetime] = None

        # 1) Entry price + buy time
        try:
            entry_price = await get_entry_price(mint)
        except Exception as e:
            logger.debug(f"record_sell_result get_entry_price fail: {e}")

        try:
            r = get_redis()
            if r is not None:
                bt_raw = await r.get(f"buytime:{mint}")
                if bt_raw:
                    if isinstance(bt_raw, (bytes, bytearray)):
                        bt_raw = bt_raw.decode("utf-8", "ignore")
                    try:
                        dt = datetime.fromisoformat(str(bt_raw))
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        entry_ts = dt
                    except Exception:
                        entry_ts = None
        except Exception:
            entry_ts = None

        # 2) Current price (USD)
        cur_price: float = 0.0
        try:
            cur_price = await get_token_price(mint)
        except Exception:
            cur_price = 0.0

        # 3) PnL + hold_seconds
        pnl_pct: Optional[float] = None
        profit_abs: float = 0.0
        hold_seconds: Optional[int] = None

        if entry_price and entry_price > 0 and cur_price > 0:
            try:
                pnl_pct = (cur_price / entry_price - 1.0) * 100.0
                profit_abs = cur_price - entry_price
            except Exception:
                pnl_pct = None
                profit_abs = 0.0

        if entry_ts is not None:
            hold_seconds = int((datetime.now(timezone.utc) - entry_ts).total_seconds())

        # 4) AI DB logging (if ai_db is available)
        try:
            from utils_pkg import ai_db  # type: ignore

            if hasattr(ai_db, "insert_entry_outcome"):
                candidate_id = meta.get("candidate_id")
                extra = dict(meta)
                extra.update(
                    {
                        "pnl_pct": pnl_pct,
                        "hold_seconds": hold_seconds,
                        "ok": ok,
                        "reason": reason,
                        "sig": str(sig) if sig is not None else None,
                    }
                )
                try:
                    await ai_db.insert_entry_outcome(  # type: ignore[attr-defined]
                        candidate_id=candidate_id,
                        mint=mint,
                        ok=bool(ok),
                        reason=str(reason),
                        pnl_pct=float(pnl_pct) if pnl_pct is not None else None,
                        hold_seconds=int(hold_seconds) if hold_seconds is not None else None,
                        meta=extra,
                    )
                except TypeError:
                    logger.debug(
                        "ai_db.insert_entry_outcome signature mismatch; skipping strict call"
                    )
                except Exception as e:
                    logger.warning(f"ai_db.insert_entry_outcome failed: {e}")
        except Exception:
            pass

        # 5) Postgres + Notion trade log
        try:
            symbol = meta.get("symbol") or meta.get("name") or mint[:6]
            size_ui = float(meta.get("amount_ui") or 0.0)
            await log_trade_event(
                token=mint,
                symbol=str(symbol),
                balance=size_ui,
                entry=entry_price or 0.0,
                current=cur_price or 0.0,
                profit=profit_abs,
                reason=reason,
                side="exit" if ok else "exit_fail",
                signature=sig,
            )
        except Exception as e:
            logger.debug(f"record_sell_result log_trade_event fail: {e}")

        # 6) Clear entry cache so future trades do not re-use stale data
        try:
            await clear_entry_price(mint)
        except Exception:
            pass

    try:
        create_task(_do(), name="record_sell_result")
    except Exception as e:
        logger.warning(f"record_sell_result scheduling failed: {e}")


# ──────────────────────────────────────────────────────────────────────────────
# Detailed entry logger from Jupiter / lightning meta
# ──────────────────────────────────────────────────────────────────────────────

async def log_entry_from_meta(
    *,
    mint: str,
    symbol: str,
    meta: Dict[str, Any],
    decimals: Optional[int] = None,
    reason: str = "ENTRY",
    signature: Any | None = None,
) -> None:
    """
    Use Jupiter / lightning entry meta to log exact entry size + price.

    Expects meta from lightning_impl.buy_via_lightning(return_meta=True):
      meta["amount_raw"]    -> raw out token amount
      meta["in_amount_raw"] -> lamports in
    """
    try:
        out_raw = int(meta.get("amount_raw") or meta.get("outAmount") or 0)
    except Exception:
        out_raw = 0
    try:
        in_raw = int(meta.get("in_amount_raw") or meta.get("inAmount") or 0)
    except Exception:
        in_raw = 0

    if out_raw <= 0 or in_raw <= 0:
        amount_ui = 0.0
        entry_price_sol = 0.0
    else:
        d = 9 if decimals is None else max(0, min(int(decimals), 18))
        scale = float(10**d)
        amount_ui = out_raw / scale if out_raw > 0 else 0.0
        sol_spent = in_raw / LAMPORTS_PER_SOL_F
        entry_price_sol = sol_spent / amount_ui if amount_ui > 0 else 0.0

    entry_price_usd = 0.0
    try:
        px_sol_usd = await get_sol_usd()
        if px_sol_usd > 0 and entry_price_sol > 0:
            entry_price_usd = entry_price_sol * px_sol_usd
    except Exception:
        entry_price_usd = 0.0

    try:
        await remember_entry_price(mint, entry_price_usd or entry_price_sol or 0.0)
    except Exception:
        pass

    await pg_log_trade(
        token=mint,
        symbol=symbol,
        side="entry",
        amount=amount_ui if amount_ui > 0 else None,
        entry=entry_price_usd or entry_price_sol or None,
        current=None,
        profit=None,
        reason=reason,
        signature=signature,
    )

    try:
        await log_to_notion(
            token=mint,
            symbol=symbol or mint,
            balance=amount_ui if amount_ui > 0 else 0.0,
            entry=entry_price_usd or entry_price_sol or 0.0,
            current=0.0,
            profit=0.0,
            reason=reason,
        )
    except Exception:
        pass


# ──────────────────────────────────────────────────────────────────────────────
# Postgres helpers for positions (BUY/SELL wiring)
# ──────────────────────────────────────────────────────────────────────────────

async def pg_upsert_position(
    *,
    wallet: str,
    mint: str,
    pool: str,
    symbol: Optional[str] = None,
    size_raw: Optional[float] = None,
    size_sol: Optional[float] = None,
    entry_price: Optional[float] = None,
    status: str = "open",
) -> None:
    """
    Insert or update an open/active position.
    """
    eng = await get_pg()
    if eng is None:
        return

    try:
        async with eng.begin() as conn:
            await conn.execute(
                text(
                    """
                    INSERT INTO positions (
                        wallet, mint, pool, symbol,
                        size_raw, size_sol, entry_price,
                        opened_at, updated_at, status
                    )
                    VALUES (
                        :wallet, :mint, :pool, :symbol,
                        :size_raw, :size_sol, :entry_price,
                        now(), now(), :status
                    )
                    ON CONFLICT (wallet, mint, pool)
                    DO UPDATE SET
                        symbol      = EXCLUDED.symbol,
                        size_raw    = EXCLUDED.size_raw,
                        size_sol    = EXCLUDED.size_sol,
                        entry_price = EXCLUDED.entry_price,
                        status      = EXCLUDED.status,
                        updated_at  = now();
                    """
                ),
                {
                    "wallet": wallet,
                    "mint": mint,
                    "pool": pool,
                    "symbol": symbol,
                    "size_raw": size_raw,
                    "size_sol": size_sol,
                    "entry_price": entry_price,
                    "status": status or "open",
                },
            )
    except Exception as e:
        logger.warning(f"pg_upsert_position failed: {e}")


async def pg_close_position(
    *,
    wallet: str,
    mint: str,
    pool: str,
    final_size_raw: Optional[float] = None,
    final_size_sol: Optional[float] = None,
    status: str = "closed",
) -> None:
    """
    Mark an existing position as closed (or any other terminal status).
    """
    eng = await get_pg()
    if eng is None:
        return

    try:
        async with eng.begin() as conn:
            await conn.execute(
                text(
                    """
                    UPDATE positions
                    SET
                        status     = :status,
                        size_raw   = COALESCE(:final_size_raw, size_raw),
                        size_sol   = COALESCE(:final_size_sol, size_sol),
                        updated_at = now()
                    WHERE wallet = :wallet
                      AND mint   = :mint
                      AND pool   = :pool;
                    """
                ),
                {
                    "wallet": wallet,
                    "mint": mint,
                    "pool": pool,
                    "status": status or "closed",
                    "final_size_raw": final_size_raw,
                    "final_size_sol": final_size_sol,
                },
            )
    except Exception as e:
        logger.warning(f"pg_close_position failed: {e}")


# ──────────────────────────────────────────────────────────────────────────────
# Jupiter v6 swap helpers
# ──────────────────────────────────────────────────────────────────────────────

async def get_jupiter_quote(
    input_mint: str,
    output_mint: str,
    amount_lamports: Optional[int] = None,
    slippage_pct: float = JUPITER_MAX_SLIPPAGE,
    **kwargs: Any,
) -> Optional[Dict[str, Any]]:
    if amount_lamports is None:
        amount_lamports = int(kwargs.get("amount", 0) or 0)
    if not session or not wallet:
        return None

    params = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": str(int(amount_lamports)),
        "slippageBps": int(max(1, min(9900, slippage_pct * 10_000))),
        "userPublicKey": str(wallet.pubkey()),
        "onlyDirectRoutes": "false",
    }
    base = JUPITER_SWAP_API.rstrip("/")
    url = base if base.endswith("/quote") else f"{base}/quote"
    try:
        async with session.get(url, params=params) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data
            logger.warning(f"Jupiter quote [{resp.status}]: {await resp.text()}")
    except Exception as e:
        logger.warning(f"get_jupiter_quote error: {e}")
    return None


async def perform_jupiter_swap(quote_json: Dict[str, Any]) -> Tuple[bool, str]:
    if not session or not solana_client or not wallet:
        return (False, "")

    routes = quote_json.get("data") or []
    route = (routes[0].get("route") if routes else None) or quote_json.get("route")
    if not route:
        logger.warning("perform_jupiter_swap: no route in quote")
        return (False, "")

    payload: Dict[str, Any] = {
        "route": route,
        "userPublicKey": str(wallet.pubkey()),
        "wrapUnwrapSOL": True,
    }

    # Jito / compute units hints (optional)
    try:
        jito_enabled = os.getenv("JITO_ENABLED", "false").strip().lower() in {"1", "true", "yes", "y"}
        fee_micro = int(float(os.getenv("JITO_FEE_MICROLAMPORTS", "0") or 0))
        tip_lamports = int(float(os.getenv("JITO_TIP_LAMPORTS", "0") or 0))
        if jito_enabled:
            if fee_micro > 0:
                payload["computeUnitPriceMicroLamports"] = fee_micro
            elif tip_lamports > 0:
                payload["jitoTipLamports"] = tip_lamports
    except Exception:
        pass

    try:
        async with session.post(f"{JUPITER_SWAP_API.rstrip('/')}/swap", json=payload) as resp:
            if resp.status != 200:
                logger.warning(f"Jupiter swap [{resp.status}]: {await resp.text()}")
                return (False, "")
            data = await resp.json()
            tx_b64 = data.get("swapTransaction")
            if not tx_b64:
                logger.warning("perform_jupiter_swap: no swapTransaction")
                return (False, "")
            raw_bytes = base64.b64decode(tx_b64)
            raw_to_send: Optional[bytes] = None

            # Try versioned first
            if _HAS_VERSIONED:
                try:
                    vtx = SVTx.from_bytes(raw_bytes)
                    msg = vtx.message
                    new_vtx = SVTx(msg, [wallet])
                    raw_to_send = bytes(new_vtx)
                except Exception:
                    pass

            # Legacy fallback
            if raw_to_send is None and SLegacyTx is not None:
                try:
                    tx = SLegacyTx.from_bytes(raw_bytes)
                    tx.sign([wallet])
                    raw_to_send = tx.serialize()
                except Exception:
                    pass

            if raw_to_send is None:
                raw_to_send = raw_bytes

            sig = await solana_client.send_raw_transaction(
                raw_to_send, opts=TxOpts(skip_preflight=True)
            )
            await solana_client.confirm_transaction(sig.value, commitment=Confirmed)
            return (True, sig.value)
    except Exception as e:
        logger.warning(f"perform_jupiter_swap error: {e}")
    return (False, "")


async def swap_token_jupiter(
    input_mint: str,
    output_mint: str,
    amount_float: float,
    *,
    decimals: int = 9,
    slippage: float = JUPITER_MAX_SLIPPAGE,
    buy: Optional[bool] = None,  # accepted but ignored
) -> Tuple[bool, str]:
    try:
        if not input_mint or not output_mint or amount_float is None:
            return (False, "")
        amount_lamports = int(round(float(amount_float) * (10 ** int(decimals))))
        if amount_lamports <= 0:
            return (False, "")
        quote = await get_jupiter_quote(
            input_mint,
            output_mint,
            amount_lamports,
            slippage_pct=slippage,
        )
        if not quote:
            return (False, "")
        ok, sig = await perform_jupiter_swap(quote)
        if not ok:
            return (False, "")
        return (True, str(sig) if isinstance(sig, str) else str(sig))
    except Exception as e:
        logger.warning(f"swap_token_jupiter error: {e}")
        return (False, "")


# ──────────────────────────────────────────────────────────────────────────────
# Wallet / accounts
# ──────────────────────────────────────────────────────────────────────────────

def _parse_token_account_data(data: bytes) -> Dict[str, Any]:
    if len(data) != 165:
        raise ValueError("Invalid SPL token account length")
    return {
        "mint": str(PublicKey(data[0:32])),
        "owner": str(PublicKey(data[32:64])),
        "amount": struct.unpack("<Q", data[64:72])[0],
    }


async def get_wallet_tokens(wallet_pubkey: str) -> List[Dict[str, Any]]:
    if not solana_client:
        return []
    try:
        owner = PublicKey.from_string(wallet_pubkey)
        resp = await solana_client.get_token_accounts_by_owner(
            owner, TokenAccountOpts(program_id=TOKEN_PROGRAM_ID)
        )
        out: List[Dict[str, Any]] = []
        for acc in resp.value:
            raw_b64 = (
                acc.account.data[0]
                if isinstance(acc.account.data, (list, tuple))
                else acc.account.data
            )
            raw = base64.b64decode(raw_b64)
            parsed = _parse_token_account_data(raw)
            out.append(
                {
                    "mint": parsed["mint"],
                    "balance": parsed["amount"] / 1e9,  # assume 9 decimals as rough default
                }
            )
        return out
    except Exception as e:
        logger.warning(f"get_wallet_tokens error: {e}")
        return []


async def get_sol_balance(wallet_address: str) -> float:
    if not solana_client:
        return 0.0
    try:
        res = await solana_client.get_balance(PublicKey.from_string(wallet_address))
        return (res.value or 0) / 1e9
    except Exception:
        return 0.0


# ──────────────────────────────────────────────────────────────────────────────
# Savings transfer (lazy import of solana.transaction)
# ──────────────────────────────────────────────────────────────────────────────

async def transfer_to_savings(amount_sol: float) -> Optional[str]:
    if not solana_client or not wallet:
        logger.warning("transfer_to_savings: resources not initialized")
        return None

    dest = (SAVINGS_WALLET or "").strip()
    if not dest:
        logger.warning("transfer_to_savings: SAVINGS_WALLET not set")
        return None
    try:
        to_pub = PublicKey.from_string(dest)
    except Exception:
        logger.warning("transfer_to_savings: invalid SAVINGS_WALLET pubkey")
        return None

    lamports = int(amount_sol * LAMPORTS_PER_SOL)
    if lamports <= 0:
        return None

    try:
        from solana.transaction import Transaction  # type: ignore
    except Exception:
        logger.warning("transfer_to_savings: solana.transaction.Transaction not available")
        return None

    try:
        if transfer and TransferParams:
            ix = transfer(
                TransferParams(
                    from_pubkey=wallet.pubkey(),
                    to_pubkey=to_pub,
                    lamports=lamports,
                )
            )
            tx = Transaction().add(ix)
            sig = await solana_client.send_transaction(
                tx, wallet, opts=TxOpts(skip_preflight=True)
            )
            await solana_client.confirm_transaction(sig.value, commitment=Confirmed)
            return sig.value
    except Exception as e:
        logger.warning(f"transfer_to_savings error: {e}")
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Convenience: token info for alerts / diagnostics
# ──────────────────────────────────────────────────────────────────────────────

async def get_token_info(mint: str) -> Dict[str, Any]:
    snap = await get_pair_snapshot_dexscreener(mint)
    if not snap:
        return {
            "symbol": "",
            "priceUsd": 0.0,
            "liquidityUsd": 0.0,
        }
    sym = snap.get("baseToken", {}).get("symbol") or snap.get("symbol") or ""
    liq = float((snap.get("liquidity") or {}).get("usd") or 0.0)
    px = float(snap.get("priceUsd") or 0.0)
    return {"symbol": sym, "priceUsd": px, "liquidityUsd": liq}


# ──────────────────────────────────────────────────────────────────────────────
# Misc
# ──────────────────────────────────────────────────────────────────────────────

def log_sniper_event(msg: str) -> None:
    logger.info(f"[Sniper] {msg}")


def is_wallet_drained(balance: float) -> bool:
    return balance <= 0.0001
