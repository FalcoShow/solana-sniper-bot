# utils_pkg/core_utils.py
# Core utilities for the sniper:
# - resource init/close (Redis, HTTP, Solana, optional Postgres, SQLite)
# - pricing helpers (Coingecko, Birdeye, Dexscreener)
# - alerts (Telegram/Discord)
# - simple indicators
# - Jupiter v6 swap helpers (versioned and legacy)
# - entry price cache and small utils
# - savings transfer (lazy import of Transaction to avoid module import errors)
# This file intentionally uses ASCII-only characters for maximum compatibility.

from __future__ import annotations

import os
import ssl
import json
import time
import base64
import struct
import logging
from datetime import datetime, timezone
from typing import Optional, Tuple, List, Dict, Any, Callable
import inspect

import aiohttp
import aiosqlite

from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    BigInteger,
    String,
    Float,
    DateTime,
    text,
)

from .alert_manager import AlertManager
from logging_setup import get_logger

import certifi

# Solana
from solana.rpc.async_api import AsyncClient
from solana.rpc.types import TokenAccountOpts, TxOpts
from solana.rpc.commitment import Confirmed

from solders.keypair import Keypair
from solders.pubkey import Pubkey as PublicKey

# Try both transaction styles for broad compatibility (Jupiter swap deserialization)
try:
    from solders.transaction import VersionedTransaction as SVTx
    _HAS_VERSIONED = True
except Exception:
    _HAS_VERSIONED = False

try:
    from solders.transaction import Transaction as SLegacyTx  # type: ignore
except Exception:
    SLegacyTx = None  # type: ignore

# system_program import changed across solana-py versions; support both
try:
    from solana.system_program import TransferParams, transfer
except Exception:
    TransferParams = None
    transfer = None

# --------------------------------------------------------------------------
# Config import (env fallbacks when config.py not present)
# --------------------------------------------------------------------------

try:
    from config import (
        RPC_URL,
        REDIS_URL,
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
except Exception:
    RPC_URL = os.getenv("RPC_URL", "https://api.mainnet-beta.solana.com")
    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
    DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK", "")
    BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY", "")
    BIRDEYE_API_URL = os.getenv("BIRDEYE_API_URL", "https://public-api.birdeye.so")
    JUPITER_SWAP_API = os.getenv("JUPITER_SWAP_API", "https://quote-api.jup.ag/v6")
    NOTION_TOKEN = os.getenv("NOTION_TOKEN", "")
    NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID", "")
    ENABLE_NOTION_LOGGING = os.getenv("ENABLE_NOTION_LOGGING", "false").lower() in (
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
    or (
        f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
        if HELIUS_API_KEY
        else ""
    )
)
RPC_FALLBACK_URL = os.getenv("RPC_FALLBACK_URL", "").strip()

# Logging
logger = logging.getLogger("core_utils")
logging.basicConfig(level=logging.INFO)
jlog = logging.getLogger("core_utils.jito")
log = get_logger()

# SSL for aiohttp
ssl_context = ssl.create_default_context(cafile=certifi.where())

# --------------------------------------------------------------------------
# Globals populated by init_resources()
# --------------------------------------------------------------------------
session: Optional[aiohttp.ClientSession] = None
solana_client: Optional[AsyncClient] = None
wallet: Optional[Keypair] = None

# Redis is created in the dedicated Redis section below
redis = None  # type: ignore[assignment]

# Constants
TOKEN_PROGRAM_ID = PublicKey.from_string(
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
)
WSOL_MINT = "So11111111111111111111111111111111111111112"
COINGECKO_SOL_API = (
    "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd"
)
DEXSCREENER_TOKEN_URL = "https://api.dexscreener.com/latest/dex/tokens/{mint}"

# --------------------------------------------------------------------------
# Optional Postgres
# --------------------------------------------------------------------------
PG_METADATA = MetaData()
_pg_engine: AsyncEngine | None = None

TRADES = Table(
    "trades",
    PG_METADATA,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("ts", DateTime, nullable=False, default=datetime.utcnow),
    Column("token", String(64), nullable=False),
    Column("symbol", String(32), nullable=True),
    Column("side", String(8), nullable=True),
    Column("amount", Float, nullable=True),
    Column("entry", Float, nullable=True),
    Column("current", Float, nullable=True),
    Column("profit", Float, nullable=True),
    Column("reason", String(64), nullable=True),
    Column("signature", String(128), nullable=True),
)


async def init_postgres() -> None:
    global _pg_engine
    if not PG_ENABLED or _pg_engine is not None:
        return
    try:
        from utils_pkg.db_dsn import pick_pg_dsn  # local import avoids cycles

        dsn, connect_args = pick_pg_dsn()
    except Exception as e:
        logger.warning(f"Postgres init skipped: DSN picker error: {e}")
        return
    if not dsn:
        logger.warning("Postgres init skipped: no DSN found")
        return
    eng = create_async_engine(
        dsn,
        echo=False,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_size=5,
        max_overflow=5,
        connect_args=connect_args or {},
    )
    try:
        async with eng.begin() as conn:
            await conn.execute(text("SELECT 1"))
            await conn.run_sync(PG_METADATA.create_all)
        _pg_engine = eng
        logger.info("Postgres ready")
    except Exception as e:
        logger.warning(f"Postgres init failed: {e}")
        try:
            await eng.dispose()
        finally:
            _pg_engine = None


def get_pg_engine() -> AsyncEngine | None:
    return _pg_engine


# --------------------------------------------------------------------------
# SQLite
# --------------------------------------------------------------------------


async def init_sqlite() -> None:
    os.makedirs(os.path.dirname(SQLITE_DB_PATH) or ".", exist_ok=True)
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        await db.execute(
            "CREATE TABLE IF NOT EXISTS sniped_tokens "
            "(token TEXT PRIMARY KEY, price REAL, timestamp INTEGER)"
        )
        await db.commit()


# --------------------------------------------------------------------------
# Wallet parsing
# --------------------------------------------------------------------------


def _parse_wallet_from_env() -> Keypair:
    pk_raw = os.getenv("WALLET_PRIVATE_KEY")
    if not pk_raw:
        raise RuntimeError("WALLET_PRIVATE_KEY missing in environment")

    try:
        txt = pk_raw.strip()
        # If it looks like JSON already, use it; otherwise treat it as a file path
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


# --------------------------------------------------------------------------
# RPC URL selection (Helius + fallback)
# --------------------------------------------------------------------------


def _effective_rpc_url() -> str:
    # Order: HELIUS_URL (if set) -> RPC_URL -> RPC_FALLBACK_URL -> hard default
    if HELIUS_URL:
        return HELIUS_URL
    if RPC_URL:
        return RPC_URL
    if RPC_FALLBACK_URL:
        return RPC_FALLBACK_URL
    return "https://api.mainnet-beta.solana.com"


# --------------------------------------------------------------------------
# Initialization / Shutdown (HTTP, Solana, wallet)
# --------------------------------------------------------------------------


async def init_resources() -> None:
    """
    Initialize aiohttp session, Postgres, sqlite, Solana client, and wallet.

    Redis is initialized in the Redis section below and imported as a module-level
    global; we do not re-create it here.
    """
    global session, solana_client, wallet

    if session and not session.closed and solana_client and wallet:
        return

    await init_postgres()
    await init_sqlite()

    if session is None or session.closed:
        session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=ssl_context, limit=100),
            timeout=aiohttp.ClientTimeout(total=20),
            json_serialize=lambda x: json.dumps(
                x, separators=(",", ":"), ensure_ascii=False
            ),
        )

    if solana_client is None:
        solana_client = AsyncClient(_effective_rpc_url())

    if wallet is None:
        wallet = _parse_wallet_from_env()

    logger.info("init_resources ready")


async def close_resources() -> None:
    global session, solana_client, redis, wallet
    try:
        if session and not session.closed:
            await session.close()
    finally:
        session = None

    try:
        if _pg_engine is not None:
            await _pg_engine.dispose()
    except Exception:
        pass

    try:
        if solana_client:
            await solana_client.close()
    finally:
        solana_client = None

    # Redis is created at import time; we can close it here if present
    try:
        if redis is not None:
            await redis.close()  # type: ignore[attr-defined]
    except Exception:
        pass
    finally:
        redis = None  # type: ignore[assignment]

    wallet = None
    logger.info("close_resources done")


# Accessors
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


# ─────────────────────────────────────────────────────────────
# Redis / Upstash
# ─────────────────────────────────────────────────────────────

# env overrides config.REDIS_URL
REDIS_URL = (os.getenv("REDIS_URL") or REDIS_URL or "").strip()
UPSTASH_REDIS_REST_URL = (os.getenv("UPSTASH_REDIS_REST_URL") or "").strip()
UPSTASH_REDIS_REST_TOKEN = (os.getenv("UPSTASH_REDIS_REST_TOKEN") or "").strip()

# try redis.asyncio first, fall back to old aioredis if present
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


# Initialize module-level redis client
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
            "REDIS_INIT_FAIL",
            error=str(e)[:200],
            url_mask=_mask_redis_url(REDIS_URL),
        )
else:
    reason = "no_url" if not REDIS_URL else "no_client_lib"
    log.event("REDIS_INIT_SKIP", reason=reason)


# --------------------------------------------------------------------------
# Alerts (Telegram / Discord)
# --------------------------------------------------------------------------


async def get_sol_usd() -> float:
    """
    Fetch SOL->USD price from Coingecko (simple, low rate).
    """
    if not session:
        return 0.0
    try:
        async with session.get(COINGECKO_SOL_API) as resp:
            if resp.status == 200:
                data = await resp.json()
                return float(data.get("solana", {}).get("usd", 0) or 0)
    except Exception:
        pass
    return 0.0


async def send_telegram(message: str, sol_value: Optional[float] = None) -> None:
    """
    Send a Telegram message. If sol_value is provided, we multiply by SOL/USD
    and append an approximate USD value.
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
        async with session.post(
            DISCORD_WEBHOOK, json={"embeds": [embed]}
        ) as resp:
            if resp.status not in (200, 201, 204):
                logger.warning(f"Discord [{resp.status}] {await resp.text()}")
    except Exception as e:
        logger.warning(f"send_discord_alert error: {e}")


# --------------------------------------------------------------------------
# Prices / Volatility
# --------------------------------------------------------------------------


async def convert_sol_to_usd_amount(amount_sol: float) -> float:
    """
    Convert a SOL amount into USD using get_sol_usd().
    """
    px = await get_sol_usd()
    return float(amount_sol) * px if px > 0 else 0.0


async def convert_usd_to_sol(usd_amount: float) -> float:
    px = await get_sol_usd()
    return (usd_amount / px) if px > 0 else 0.0


async def get_token_price(token_address: str) -> float:
    """
    Return token price in USD.
    - First tries Dexscreener.
    - Falls back to Birdeye (returns USD as well).
    """
    if not session:
        return 0.0

    # 1) Dexscreener first
    try:
        async with session.get(
            DEXSCREENER_TOKEN_URL.format(mint=token_address)
        ) as resp:
            if resp.status == 200:
                data = await resp.json()
                pairs = data.get("pairs") or []
                if pairs:
                    top = max(
                        pairs,
                        key=lambda p: float(
                            (p.get("liquidity") or {}).get("usd") or 0.0
                        ),
                    )
                    return float(top.get("priceUsd") or 0.0)
    except Exception:
        pass

    # 2) Birdeye fallback (also USD)
    try:
        headers = {"X-API-KEY": BIRDEYE_API_KEY} if BIRDEYE_API_KEY else {}
        url = (
            f"{BIRDEYE_API_URL.rstrip('/')}/public/price?address={token_address}"
        )
        async with session.get(url, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                val = data.get("data", {}).get("value")
                if val is not None:
                    return float(val)
    except Exception:
        pass

    return 0.0


async def get_pair_snapshot_dexscreener(token_address: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if not session:
        return out
    try:
        async with session.get(
            DEXSCREENER_TOKEN_URL.format(mint=token_address)
        ) as resp:
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


# --------------------------------------------------------------------------
# Indicators
# --------------------------------------------------------------------------


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
    out: List[float] = [50.0] * (period)
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


# --------------------------------------------------------------------------
# AI Fraud Score & Risk wrappers
# --------------------------------------------------------------------------


async def ai_fraud_score(token_address: str) -> float:
    if not session or not FRAUD_API_URL:
        return 0.0
    try:
        headers = (
            {"Authorization": f"Bearer {FRAUD_API_KEY}"} if FRAUD_API_KEY else {}
        )
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
    s01 = await ai_fraud_score(token_address)
    return round(max(0.0, min(1.0, s01)) * 100, 2)


async def check_rug_pull_risk(token_address: str) -> bool:
    score = await fraud_score_ai(token_address)
    return score >= float(SCANNER_MIN_FRAUD_SCORE)


async def is_whale_wallet(
    token_mint: str, usd_threshold: float = 50000
) -> bool:
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
                holders = (
                    data.get("data", {}).get("items")
                    or data.get("data")
                    or []
                )
                for h in holders:
                    val = (
                        h.get("value_usd")
                        or h.get("valueUsd")
                        or h.get("usdValue")
                        or 0
                    )
                    try:
                        if float(val) >= usd_threshold:
                            return True
                    except Exception:
                        continue
        except Exception:
            continue
    return False


# --------------------------------------------------------------------------
# Notion logging
# --------------------------------------------------------------------------


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
        "balance": env_first(
            ["NOTION_COL_BALANCE", "NOTION_PROP_BALANCE"], ""
        ),
        "entry": env_first(
            ["NOTION_COL_ENTRY", "NOTION_PROP_ENTRY"], "Entry Price"
        ),
        "current": env_first(
            ["NOTION_COL_CURRENT", "NOTION_PROP_CURRENT"], "Current Price"
        ),
        "profit": env_first(
            ["NOTION_COL_PROFIT", "NOTION_PROP_PROFIT"], "Profit"
        ),
        "reason": env_first(
            ["NOTION_COL_REASON", "NOTION_PROP_REASON"], "Reason"
        ),
        "timestamp": env_first(
            ["NOTION_COL_TIMESTAMP", "NOTION_PROP_TIME"], "Timestamp"
        ),
    }
    try:
        blob = (
            os.getenv("NOTION_FIELDS_JSON")
            or os.getenv("NOTION_MAP_JSON")
            or ""
        )
        if blob.strip():
            extra = json.loads(blob)
            if isinstance(extra, dict):
                defaults.update(
                    {str(k): str(v) for k, v in extra.items()}
                )
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
        fields["title"]: {
            "title": [{"text": {"content": symbol or token}}]
        },
        fields["mint"]: {
            "rich_text": [{"text": {"content": token}}]
        },
    }
    if fields.get("balance"):
        props[fields["balance"]] = {"number": float(balance)}
    if fields.get("entry"):
        props[fields["entry"]] = {
            "number": float(entry) if entry is not None else None
        }
    if fields.get("current"):
        props[fields["current"]] = {
            "number": float(current) if current is not None else None
        }
    if fields.get("profit"):
        props[fields["profit"]] = {
            "number": float(profit) if profit is not None else None
        }
    if fields.get("reason") and reason:
        props[fields["reason"]] = {
            "rich_text": [{"text": {"content": reason}}]
        }
    if fields.get("timestamp"):
        props[fields["timestamp"]] = {
            "date": {"start": datetime.utcnow().isoformat()}
        }
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }
    body = {"parent": {"database_id": NOTION_DATABASE_ID}, "properties": props}
    try:
        async with session.post(
            "https://api.notion.com/v1/pages",
            headers=headers,
            json=body,
        ) as resp:
            if resp.status not in (200, 201):
                txt = await resp.text()
                logger.warning(f"[notion_alert] HTTP {resp.status}: {txt}")
    except Exception as e:
        logger.warning(f"log_to_notion failed: {e}")


async def pg_log_trade(
    *,
    token: str,
    symbol: str | None,
    side: str | None,
    amount: float | None,
    entry: float | None,
    current: float | None,
    profit: float | None,
    reason: str | None,
    signature: Any | None = None,
) -> None:
    if _pg_engine is None:
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
        async with _pg_engine.begin() as conn:
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
    side: str | None = None,
    signature: Any | None = None,
) -> None:
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


# --------------------------------------------------------------------------
# Jupiter v6 swap helpers
# --------------------------------------------------------------------------


async def get_jupiter_quote(
    input_mint: str,
    output_mint: str,
    amount_lamports: int | None = None,
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
        "slippageBps": int(
            max(1, min(9900, slippage_pct * 10000))
        ),
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
            logger.warning(
                f"Jupiter quote [{resp.status}]: {await resp.text()}"
            )
    except Exception as e:
        logger.warning(f"get_jupiter_quote error: {e}")
    return None


async def perform_jupiter_swap(quote_json: Dict[str, Any]) -> Tuple[bool, str]:
    if not session or not solana_client or not wallet:
        return (False, "")
    routes = quote_json.get("data") or []
    route = (routes[0].get("route") if routes else None) or quote_json.get(
        "route"
    )
    if not route:
        logger.warning("perform_jupiter_swap: no route in quote")
        return (False, "")
    payload: Dict[str, Any] = {
        "route": route,
        "userPublicKey": str(wallet.pubkey()),
        "wrapUnwrapSOL": True,
    }
    try:
        jito_enabled = (
            os.getenv("JITO_ENABLED", "false").strip().lower()
            in ("1", "true", "yes", "y")
        )
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
        async with session.post(
            f"{JUPITER_SWAP_API.rstrip('/')}/swap", json=payload
        ) as resp:
            if resp.status != 200:
                logger.warning(
                    f"Jupiter swap [{resp.status}]: {await resp.text()}"
                )
                return (False, "")
            data = await resp.json()
            tx_b64 = data.get("swapTransaction")
            if not tx_b64:
                logger.warning("perform_jupiter_swap: no swapTransaction")
                return (False, "")
            raw_bytes = base64.b64decode(tx_b64)
            raw_to_send: Optional[bytes] = None
            if _HAS_VERSIONED:
                try:
                    vtx = SVTx.from_bytes(raw_bytes)
                    msg = vtx.message
                    new_vtx = SVTx(msg, [wallet])
                    raw_to_send = bytes(new_vtx)
                except Exception:
                    pass
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
            await solana_client.confirm_transaction(
                sig.value, commitment=Confirmed
            )
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
    buy: bool | None = None,  # accepted but ignored (API is symmetric)
) -> Tuple[bool, str]:
    try:
        if not input_mint or not output_mint or amount_float is None:
            return (False, "")
        amount_lamports = int(
            round(float(amount_float) * (10 ** int(decimals)))
        )
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


# --------------------------------------------------------------------------
# Wallet / Accounts
# --------------------------------------------------------------------------


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
                    "balance": parsed["amount"] / 1e9,
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
        res = await solana_client.get_balance(
            PublicKey.from_string(wallet_address)
        )
        return (res.value or 0) / 1e9
    except Exception:
        return 0.0


# --------------------------------------------------------------------------
# Savings transfer (lazy import of solana.transaction)
# --------------------------------------------------------------------------


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
        logger.warning(
            "transfer_to_savings: invalid SAVINGS_WALLET pubkey"
        )
        return None
    lamports = int(amount_sol * 1_000_000_000)
    if lamports <= 0:
        return None
    # Import Transaction lazily to avoid ModuleNotFoundError at module import time
    try:
        from solana.transaction import Transaction  # type: ignore
    except Exception:
        logger.warning(
            "transfer_to_savings: solana.transaction.Transaction not available"
        )
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
            await solana_client.confirm_transaction(
                sig.value, commitment=Confirmed
            )
            return sig.value
    except Exception as e:
        logger.warning(f"transfer_to_savings error: {e}")
    return None


# --------------------------------------------------------------------------
# Convenience: token info for alerts
# --------------------------------------------------------------------------


async def get_token_info(mint: str) -> Dict[str, Any]:
    snap = await get_pair_snapshot_dexscreener(mint)
    if not snap:
        return {
            "symbol": "",
            "priceUsd": 0.0,
            "liquidityUsd": 0.0,
        }
    sym = (
        snap.get("baseToken", {}).get("symbol")
        or snap.get("symbol")
        or ""
    )
    liq = float((snap.get("liquidity") or {}).get("usd") or 0.0)
    px = float(snap.get("priceUsd") or 0.0)
    return {"symbol": sym, "priceUsd": px, "liquidityUsd": liq}


# --------------------------------------------------------------------------
# Misc
# --------------------------------------------------------------------------


def log_sniper_event(msg: str) -> None:
    logger.info(f"[Sniper] {msg}")


def is_wallet_drained(balance: float) -> bool:
    return balance <= 0.0001
