# utils_pkg/core_utils.py
# Core utilities: resources (redis/aiohttp/solana wallet), pricing, Jupiter swaps,
# alerts (Telegram/Discord), Notion logging, entry-price cache, volatility helpers,
# Jito tips, Postgres logging — plus day-trading helpers (indicators, risk sizing,
# OHLC fetchers, orderflow snapshot) and an AI scoring gate wrapper.
from __future__ import annotations

import os
import ssl
import json
import time
import base64
import certifi
import struct
import math
import logging
import random
from datetime import datetime
from typing import Optional, Tuple, List, Dict, Any

import aiohttp
import aiosqlite
import redis.asyncio as aioredis

from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from sqlalchemy import MetaData, Table, Column, BigInteger, String, Float, DateTime, text
from sqlalchemy.engine.url import make_url

# Solana
from solana.rpc.async_api import AsyncClient
from solana.rpc.types import TokenAccountOpts, TxOpts
from solana.rpc.commitment import Confirmed

from solders.keypair import Keypair
from solders.pubkey import Pubkey as PublicKey

# Try both transaction styles for broad compatibility
try:
    from solders.transaction import VersionedTransaction as SVTx  # Jupiter v0 tx path
    _HAS_VERSIONED = True
except Exception:
    _HAS_VERSIONED = False

try:
    from solders.transaction import Transaction as SLegacyTx  # legacy path
except Exception:
    SLegacyTx = None  # type: ignore

# system_program import changed across solana-py versions; support both
try:
    from solana.system_program import TransferParams, transfer
except Exception:
    TransferParams = None
    transfer = None

# ───────────────────────────────────────────────────────────────────────────────
# Config import (with safe fallbacks to envs)
# ───────────────────────────────────────────────────────────────────────────────
try:
    from config import (
        # networking
        RPC_URL, REDIS_URL,
        # alerts
        TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, DISCORD_WEBHOOK,
        # APIs
        BIRDEYE_API_KEY, BIRDEYE_API_URL, JUPITER_SWAP_API,
        # Notion
        NOTION_TOKEN, NOTION_DATABASE_ID, ENABLE_NOTION_LOGGING,
        # storage
        SQLITE_DB_PATH,
        # optional Postgres
        PG_ENABLED, PG_DSN,
        # trading / swaps
        JUPITER_MAX_SLIPPAGE, SAVINGS_WALLET,
        # fraud & scanners
        FRAUD_API_URL, FRAUD_API_KEY, SCANNER_MIN_FRAUD_SCORE,
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
    ENABLE_NOTION_LOGGING = os.getenv("ENABLE_NOTION_LOGGING", "false").lower() == "true"
    SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "./state.sqlite3")
    PG_ENABLED = os.getenv("PG_ENABLED", "false").strip().lower() in ("1","true","yes","y","on")
    PG_DSN = os.getenv("PG_DSN", "")
    JUPITER_MAX_SLIPPAGE = float(os.getenv("JUPITER_MAX_SLIPPAGE", "0.005"))
    SAVINGS_WALLET = os.getenv("SAVINGS_WALLET", "")
    FRAUD_API_URL = os.getenv("FRAUD_API_URL", "")
    FRAUD_API_KEY = os.getenv("FRAUD_API_KEY", "")
    SCANNER_MIN_FRAUD_SCORE = float(os.getenv("SCANNER_MIN_FRAUD_SCORE", "70"))  # 0..100

# Day-trader add-ons (env flags & risk defaults)
DAYTRADER_ENABLED = os.getenv("DAYTRADER_ENABLED", "false").strip().lower() in ("1","true","yes","y","on")
MAX_RISK_PCT = float(os.getenv("DT_MAX_RISK_PCT", "0.02"))
ATR_SL_MULT = float(os.getenv("DT_ATR_SL_MULT", "2.0"))
ATR_TP_MULT = float(os.getenv("DT_ATR_TP_MULT", "3.0"))

# Logging
logger = logging.getLogger("core_utils")
logging.basicConfig(level=logging.INFO)
jlog = logging.getLogger("core_utils.jito")

# SSL for aiohttp
ssl_context = ssl.create_default_context(cafile=certifi.where())

# Globals populated by init_resources()
session: Optional[aiohttp.ClientSession] = None
redis: Optional[aioredis.Redis] = None
solana_client: Optional[AsyncClient] = None
wallet: Optional[Keypair] = None

# Constants
TOKEN_PROGRAM_ID = PublicKey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
WSOL_MINT = "So11111111111111111111111111111111111111112"
COINGECKO_SOL_API = "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd"
DEXSCREENER_TOKEN_URL = "https://api.dexscreener.com/latest/dex/tokens/{mint}"

# ───────────────────────────────────────────────────────────────────────────────
# Optional Postgres
# ───────────────────────────────────────────────────────────────────────────────
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
    """Initialize optional Postgres engine and ensure tables (with TLS when needed)."""
    from utils_pkg.db_dsn import pick_pg_dsn  # local import avoids cycles
    global _pg_engine
    if not PG_ENABLED or _pg_engine is not None:
        return

    try:
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
        logger.info("✅ Postgres ready")
    except Exception as e:
        logger.warning(f"Postgres init failed: {e}")
        try:
            await eng.dispose()
        finally:
            _pg_engine = None

    def _valid_dsn(s: str) -> bool:
        if not s or not isinstance(s, str):
            return False
        try:
            make_url(s.strip()); return True
        except Exception:
            return False

    candidates: List[Tuple[str, str]] = []
    exp = os.getenv("PG_DSN", "").strip()
    internal = os.getenv("PG_DSN_INTERNAL", "").strip()
    external = os.getenv("PG_DSN_EXTERNAL", "").strip()
    if _valid_dsn(exp): candidates.append(("PG_DSN", exp))
    if _valid_dsn(internal): candidates.append(("INTERNAL", internal))
    if _valid_dsn(external): candidates.append(("EXTERNAL", external))

    if not candidates:
        logger.warning("Postgres init skipped: no valid DSN found")
        return

    last_err: Optional[Exception] = None
    for label, dsn in candidates:
        try:
            eng = create_async_engine(dsn, echo=False, pool_pre_ping=True)
            async with eng.begin() as conn:
                await conn.execute(text("SELECT 1"))
                await conn.run_sync(PG_METADATA.create_all)
            _pg_engine = eng
            logger.info(f"Postgres DSN selected: {label}")
            logger.info("✅ Postgres ready")
            return
        except Exception as e:
            last_err = e
            logger.warning(f"Postgres {label} DSN failed: {e}")
            try:
                await eng.dispose()  # type: ignore[name-defined]
            except Exception:
                pass
    _pg_engine = None
    if last_err:
        logger.warning(f"Postgres init failed: {last_err}")

def get_pg_engine() -> AsyncEngine | None:
    return _pg_engine

# ───────────────────────────────────────────────────────────────────────────────
# Initialization / Shutdown
# ───────────────────────────────────────────────────────────────────────────────
async def init_sqlite() -> None:
    """Create local SQLite tables used for basic caching."""
    os.makedirs(os.path.dirname(SQLITE_DB_PATH) or ".", exist_ok=True)
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS sniped_tokens (
              token TEXT PRIMARY KEY,
              price REAL,
              timestamp INTEGER
            )
        """)
        await db.commit()

def _parse_wallet_from_env() -> Keypair:
    """
    Accept WALLET_PRIVATE_KEY as:
      - JSON array of 64 ints (secret key)
      - JSON array of 32 ints (seed)
    """
    pk_raw = os.getenv("WALLET_PRIVATE_KEY")
    if not pk_raw:
        raise RuntimeError("WALLET_PRIVATE_KEY missing in environment")
    try:
        arr = json.loads(pk_raw)
        b = bytes(arr)
        if len(b) == 64:
            return Keypair.from_bytes(b)
        if len(b) == 32:
            return Keypair.from_seed(b)
        raise ValueError(f"Invalid key length {len(b)}; expected 32 or 64 bytes")
    except Exception as e:
        raise RuntimeError("WALLET_PRIVATE_KEY must be JSON array of 64 or 32 ints") from e

async def init_resources() -> None:
    """Initialize Redis, Postgres (optional), aiohttp session, Solana client, and wallet."""
    global redis, session, solana_client, wallet
    if session and not session.closed:
        return

    redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    await init_postgres()

    session = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=ssl_context, limit=100),
        timeout=aiohttp.ClientTimeout(total=20),
        json_serialize=lambda x: json.dumps(x, separators=(",", ":"), ensure_ascii=False),
    )
    solana_client = AsyncClient(RPC_URL)
    wallet = _parse_wallet_from_env()
    await init_sqlite()
    logger.info("✅ init_resources: Redis/HTTP/Solana/SQLite ready")

async def close_resources() -> None:
    """Gracefully close async resources."""
    global session, solana_client, redis
    try:
        if session and not session.closed:
            await session.close()
        if _pg_engine is not None:
            await _pg_engine.dispose()
    finally:
        session = None

    try:
        if solana_client:
            await solana_client.close()
    finally:
        solana_client = None

    try:
        if redis:
            await redis.close()
    finally:
        redis = None

    logger.info("🛑 close_resources: all closed")

# Accessors
def get_redis(): return redis
def get_session(): return session
def get_solana_client(): return solana_client
def get_wallet_pubkey() -> str:
    try: return str(wallet.pubkey()) if wallet else ""
    except Exception: return ""

# ───────────────────────────────────────────────────────────────────────────────
# Redis / SQLite helpers
# ───────────────────────────────────────────────────────────────────────────────
async def cache_sniped_token(token: str, price: float) -> None:
    ts = int(time.time())
    if redis:
        await redis.setex(f"sniped:{token}", 86400, "1")
    async with aiosqlite.connect(SQLITE_DB_PATH) as db:
        await db.execute(
            "REPLACE INTO sniped_tokens (token, price, timestamp) VALUES (?,?,?)",
            (token, price, ts),
        )
        await db.commit()

async def was_token_sniped(token: str) -> bool:
    if not redis: return False
    try: return (await redis.exists(f"sniped:{token}")) == 1
    except Exception: return False

async def save_entry_price(redis_client: aioredis.Redis, token_address: str, entry_price: float) -> None:
    payload = {"price": entry_price, "timestamp": datetime.utcnow().isoformat()}
    try:
        await redis_client.set(f"entry_price:{token_address}", json.dumps(payload), ex=172800)  # 48h
    except Exception as e:
        logger.warning(f"save_entry_price failed for {token_address}: {e}")

async def get_entry_price(redis_client: aioredis.Redis, token_address: str) -> Optional[float]:
    try:
        raw = await redis_client.get(f"entry_price:{token_address}")
        if not raw: return None
        return float(json.loads(raw).get("price"))
    except Exception:
        return None

# Back-compat aliases
async def get_entry_price_from_redis(token_address: str):
    if redis is None: raise RuntimeError("Redis not initialized. Call init_resources() first.")
    return await get_entry_price(redis, token_address)

async def save_entry_price_to_redis(token_address: str, entry_price: float):
    if redis is None: raise RuntimeError("Redis not initialized. Call init_resources() first.")
    await save_entry_price(redis, token_address, entry_price)

def dca_profit_exit(current: float, entry: float, target: float = 2.0) -> bool:
    try:
        if entry <= 0: return False
        return float(current) >= float(entry) * float(target)
    except Exception:
        return False

# ───────────────────────────────────────────────────────────────────────────────
# Jito tip accounts helper
# ───────────────────────────────────────────────────────────────────────────────
_JITO_TIP_STATIC_LIST = [
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
    "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
    "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
    "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
    "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
    "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
]

# ───────────────────────────────────────────────────────────────────────────────
# Alerts (Telegram / Discord)
# ───────────────────────────────────────────────────────────────────────────────
async def send_telegram(message: str, sol_value: Optional[float] = None) -> None:
    if not session or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        if sol_value is not None:
            usd = await convert_sol_to_usd(sol_value)
            message += f"\n💵 ~${usd:.2f} USD"
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "disable_web_page_preview": True, "parse_mode": "HTML"}
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

# ───────────────────────────────────────────────────────────────────────────────
# Prices / Volatility
# ───────────────────────────────────────────────────────────────────────────────
async def get_sol_usd() -> float:
    if not session: return 0.0
    try:
        async with session.get(COINGECKO_SOL_API) as resp:
            if resp.status == 200:
                data = await resp.json()
                return float(data.get("solana", {}).get("usd", 0) or 0)
    except Exception:
        pass
    return 0.0

async def convert_sol_to_usd(sol_amount: float) -> float:
    px = await get_sol_usd()
    return round(sol_amount * px, 2) if px > 0 else 0.0

async def convert_usd_to_sol(usd_amount: float) -> float:
    px = await get_sol_usd()
    return (usd_amount / px) if px > 0 else 0.0

async def get_token_price(token_address: str) -> float:
    """Primary: Birdeye (USD) -> convert to SOL; fallback: Dexscreener. Returns SOL price or 0."""
    if not session: return 0.0
    # Birdeye
    try:
        headers = {"X-API-KEY": BIRDEYE_API_KEY} if BIRDEYE_API_KEY else {}
        url = f"{BIRDEYE_API_URL.rstrip('/')}/public/price?address={token_address}"
        async with session.get(url, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                val = data.get("data", {}).get("value")
                if val is not None:
                    sol_usd = await get_sol_usd()
                    return float(val) / sol_usd if sol_usd > 0 else 0.0
    except Exception:
        pass
    # Dexscreener fallback
    try:
        async with session.get(DEXSCREENER_TOKEN_URL.format(mint=token_address)) as resp:
            if resp.status == 200:
                data = await resp.json()
                pairs = data.get("pairs") or []
                if pairs:
                    top = max(pairs, key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0))
                    price_usd = float(top.get("priceUsd") or 0)
                    sol_usd = await get_sol_usd()
                    return price_usd / sol_usd if sol_usd > 0 and price_usd > 0 else 0.0
    except Exception:
        pass
    return 0.0

async def get_pair_snapshot_dexscreener(token_address: str) -> Dict[str, Any]:
    """Returns a best pair snapshot from Dexscreener: priceUsd, liquidity.usd, txns.m5.buy/sell, volume.h24, etc."""
    out: Dict[str, Any] = {}
    if not session: return out
    try:
        async with session.get(DEXSCREENER_TOKEN_URL.format(mint=token_address)) as resp:
            if resp.status != 200: return out
            data = await resp.json()
            pairs = data.get("pairs") or []
            if not pairs: return out
            def liq(p):
                try: return float((p.get("liquidity") or {}).get("usd") or 0)
                except Exception: return 0.0
            top = max(pairs, key=liq)
            return top
    except Exception:
        return out

# Simple orderflow snapshot
async def get_orderflow_snapshot(mint: str) -> Dict[str, Any]:
    snap = await get_pair_snapshot_dexscreener(mint)
    if not snap:
        return {"liq_usd": 0.0, "price_usd": 0.0, "buys": 0, "sells": 0, "skew": 1.0}
    liq_usd = float((snap.get("liquidity") or {}).get("usd") or 0.0)
    price_usd = float(snap.get("priceUsd") or 0.0)
    tx = (snap.get("txns") or {}).get("m5") or {}
    buys = int(tx.get("buys", 0) or 0)
    sells = int(tx.get("sells", 0) or 0)
    skew = (buys + 1) / (sells + 1)
    return {"liq_usd": liq_usd, "price_usd": price_usd, "buys": buys, "sells": sells, "skew": skew}

# ───────────────────────────────────────────────────────────────────────────────
# Indicators
# ───────────────────────────────────────────────────────────────────────────────
def ema(xs: List[float], n: int) -> List[float]:
    if not xs: return []
    k = 2/(n+1)
    out = [xs[0]]
    for x in xs[1:]:
        out.append(out[-1] + k*(x - out[-1]))
    return out

def rsi(closes: List[float], period: int = 14) -> List[float]:
    if len(closes) < period + 1: return [50.0]*len(closes)
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i]-closes[i-1]
        gains.append(max(d,0)); losses.append(max(-d,0))
    avg_g = sum(gains[:period])/period
    avg_l = sum(losses[:period])/period
    out = [50.0]*(period)
    for i in range(period, len(gains)):
        avg_g = (avg_g*(period-1) + gains[i]) / period
        avg_l = (avg_l*(period-1) + losses[i]) / period
        rs = (avg_g / avg_l) if avg_l > 1e-12 else 999.0
        out.append(100 - 100/(1+rs))
    return [50.0] + out

def atr(ohlc: List[Dict[str, float]], period: int = 14) -> List[float]:
    if not ohlc: return []
    trs = []
    prev_c = ohlc[0]["close"]
    for c in ohlc:
        hi, lo, cl = c["high"], c["low"], c["close"]
        tr = max(hi-lo, abs(hi-prev_c), abs(lo-prev_c))
        trs.append(tr); prev_c = cl
    return ema(trs, period)

# ───────────────────────────────────────────────────────────────────────────────
# AI Fraud Score & Risk wrappers (what scanners/sniper expect)
# ───────────────────────────────────────────────────────────────────────────────
async def ai_fraud_score(token_address: str) -> float:
    """
    Legacy function name retained (0..1). Prefer fraud_score_ai() below for 0..100.
    """
    if not session or not FRAUD_API_URL:
        return 0.9
    try:
        headers = {"Authorization": f"Bearer {FRAUD_API_KEY}"} if FRAUD_API_KEY else {}
        url = f"{FRAUD_API_URL.rstrip('/')}?token={token_address}"
        async with session.get(url, headers=headers, timeout=5) as resp:
            if resp.status == 200:
                data = await resp.json()
                score = float(data.get("fraud_score", 0.9))
                return max(0.0, min(1.0, score))
            return 0.9
    except Exception:
        return 0.9

async def fraud_score_ai(token_address: str) -> float:
    """
    Returns 0..100 for easier thresholds in scanners.
    If external API not set, return 90 (safe bias).
    """
    s01 = await ai_fraud_score(token_address)
    return round(max(0.0, min(1.0, s01)) * 100, 2)

async def check_rug_pull_risk(token_address: str) -> bool:
    """
    Boolean guard: pass if fraud score >= SCANNER_MIN_FRAUD_SCORE.
    """
    score = await fraud_score_ai(token_address)
    return score >= float(SCANNER_MIN_FRAUD_SCORE)

async def is_whale_wallet(token_mint: str, usd_threshold: float = 50_000) -> bool:
    """
    Heuristic: check top holders USD value via Birdeye (shape tolerant).
    Returns True if any holder value > threshold. Fails closed (False).
    """
    if not session or not BIRDEYE_API_KEY:
        return False
    headers = {"X-API-KEY": BIRDEYE_API_KEY}
    # Birdeye holder endpoint varies by plan; keep tolerant:
    # Try /defi/holders?address=... else /token/<mint>/holders (older docs)
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

# ───────────────────────────────────────────────────────────────────────────────
# Notion logging (direct API, configurable columns)
# ───────────────────────────────────────────────────────────────────────────────
def _notion_fields_map() -> Dict[str, str]:
    def env_first(keys, default):
        for k in keys:
            v = os.getenv(k)
            if v and str(v).strip():
                return str(v).strip()
        return default

    defaults = {
        "title":     env_first(["NOTION_COL_TITLE", "NOTION_TITLE_PROP"], "Token"),
        "mint":      env_first(["NOTION_COL_MINT", "NOTION_PROP_TOKEN"], "Mint"),
        "balance":   env_first(["NOTION_COL_BALANCE", "NOTION_PROP_BALANCE"], ""),
        "entry":     env_first(["NOTION_COL_ENTRY", "NOTION_PROP_ENTRY"], "Entry Price"),
        "current":   env_first(["NOTION_COL_CURRENT", "NOTION_PROP_CURRENT"], "Current Price"),
        "profit":    env_first(["NOTION_COL_PROFIT", "NOTION_PROP_PROFIT"], "Profit"),
        "reason":    env_first(["NOTION_COL_REASON", "NOTION_PROP_REASON"], "Reason"),
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

async def log_to_notion(token: str, symbol: str, balance: float, entry: float, current: float, profit: float, reason: str) -> None:
    if not ENABLE_NOTION_LOGGING or not NOTION_TOKEN or not NOTION_DATABASE_ID or not session:
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

# Dual logging: Postgres (if enabled) + Notion (if enabled)
async def pg_log_trade(*, token: str, symbol: str | None, side: str | None, amount: float | None,
                       entry: float | None, current: float | None, profit: float | None,
                       reason: str | None, signature: Any | None = None) -> None:
    if not PG_ENABLED or _pg_engine is None:
        return
    sig_txt = None
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
            await conn.execute(TRADES.insert().values(
                ts=datetime.utcnow(),
                token=token, symbol=symbol, side=side, amount=amount,
                entry=entry, current=current, profit=profit,
                reason=reason, signature=sig_txt,
            ))
    except Exception as e:
        logger.warning(f"pg_log_trade failed: {e}")

async def log_trade_event(token: str, symbol: str, balance: float, entry: float, current: float, profit: float, reason: str,
                          *, side: str | None = None, signature: Any | None = None) -> None:
    await pg_log_trade(token=token, symbol=symbol, side=side, amount=balance,
                       entry=entry, current=current, profit=profit,
                       reason=reason, signature=signature)
    try:
        await log_to_notion(token=token, symbol=symbol, balance=balance, entry=entry, current=current, profit=profit, reason=reason)
    except Exception:
        pass

# ───────────────────────────────────────────────────────────────────────────────
# Jupiter v6 swap helpers
# ───────────────────────────────────────────────────────────────────────────────
async def get_jupiter_quote(
    input_mint: str,
    output_mint: str,
    amount_lamports: int,
    slippage_pct: float = JUPITER_MAX_SLIPPAGE,
) -> Optional[Dict[str, Any]]:
    if not session or not wallet:
        return None
    params = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": str(amount_lamports),
        "slippageBps": int(max(1, min(9900, slippage_pct * 10_000))),
        "userPublicKey": str(wallet.pubkey()),
        "onlyDirectRoutes": "false",
    }
    try:
        async with session.get(f"{JUPITER_SWAP_API.rstrip('/')}/quote", params=params) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data and (data.get("route") or (data.get("data") and len(data["data"]) > 0)):
                    return data
                logger.warning("Jupiter quote OK but no routes in payload.")
                return data
            logger.warning(f"Jupiter quote [{resp.status}]: {await resp.text()}")
    except Exception as e:
        logger.warning(f"get_jupiter_quote error: {e}")
    return None

async def perform_jupiter_swap(quote_json: Dict[str, Any]) -> Tuple[bool, str]:
    """Sign and send the serialized tx returned by Jupiter /swap (v6)."""
    if not session or not solana_client or not wallet:
        return (False, "")
    routes = quote_json.get("data") or []
    route = routes[0].get("route") if routes else quote_json.get("route")
    if not route:
        logger.warning("perform_jupiter_swap: no route in quote")
        return (False, "")
    payload: Dict[str, Any] = {"route": route, "userPublicKey": str(wallet.pubkey()), "wrapUnwrapSOL": True}

    # Optional priority fees (Jito-like)
    try:
        _jito_enabled = os.getenv("JITO_ENABLED", "false").strip().lower() in ("1","true","yes","y")
        _fee_micro = int(float(os.getenv("JITO_FEE_MICROLAMPORTS", "0")))
        _tip_lamports = int(float(os.getenv("JITO_TIP_LAMPORTS", "0")))
        if _jito_enabled:
            if _fee_micro > 0:
                payload["computeUnitPriceMicroLamports"] = _fee_micro
            elif _tip_lamports > 0:
                payload["jitoTipLamports"] = _tip_lamports
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

            # Try versioned (preferred)
            raw_to_send: Optional[bytes] = None
            if _HAS_VERSIONED:
                try:
                    vtx = SVTx.from_bytes(raw_bytes)
                    msg = vtx.message
                    new_vtx = SVTx(msg, [wallet])
                    raw_to_send = bytes(new_vtx)
                except Exception as e:
                    logger.debug(f"versioned path failed: {e}")

            # Fallback: legacy
            if raw_to_send is None and SLegacyTx is not None:
                try:
                    tx = SLegacyTx.from_bytes(raw_bytes)
                    tx.sign([wallet])
                    raw_to_send = tx.serialize()
                except Exception as e:
                    logger.debug(f"legacy path failed: {e}")

            # Last resort: send what we got (if already fully signed)
            if raw_to_send is None:
                raw_to_send = raw_bytes

            sig = await solana_client.send_raw_transaction(raw_to_send, opts=TxOpts(skip_preflight=True))
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
) -> Tuple[bool, str]:
    """Generic swap using Jupiter v6: amount_float in input token units (decimals default 9)."""
    try:
        amount_lamports = int(round(amount_float * (10 ** decimals)))
        quote = await get_jupiter_quote(input_mint, output_mint, amount_lamports, slippage_pct=slippage)
        if not quote:
            return (False, "")
        return await perform_jupiter_swap(quote)
    except Exception as e:
        logger.warning(f"swap_token_jupiter error: {e}")
        return (False, "")

# ───────────────────────────────────────────────────────────────────────────────
# Wallet / Accounts
# ───────────────────────────────────────────────────────────────────────────────
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
        resp = await solana_client.get_token_accounts_by_owner(owner, TokenAccountOpts(program_id=TOKEN_PROGRAM_ID))
        out: List[Dict[str, Any]] = []
        for acc in resp.value:
            raw_b64 = acc.account.data[0] if isinstance(acc.account.data, (list, tuple)) else acc.account.data
            raw = base64.b64decode(raw_b64)
            parsed = _parse_token_account_data(raw)
            out.append({"mint": parsed["mint"], "balance": parsed["amount"] / 1e9})
        return out
    except Exception as e:
        logger.warning(f"get_wallet_tokens error: {e}")
        return []

async def get_sol_balance(wallet_address: str) -> float:
    if not solana_client: return 0.0
    try:
        res = await solana_client.get_balance(PublicKey.from_string(wallet_address))
        return (res.value or 0) / 1e9
    except Exception:
        return 0.0

# 
# ───────────────────────────────────────────────────────────────────────────────
# Savings transfers (used by reinvestor.py)
# ───────────────────────────────────────────────────────────────────────────────
async def transfer_sol(to_pubkey: str, amount_sol: float) -> str:
    """
    Send native SOL from our wallet to `to_pubkey`. Returns the signature.
    """
    if not solana_client or not wallet:
        raise RuntimeError("Resources not initialized (solana_client/wallet)")

    if amount_sol <= 0:
        raise ValueError("amount_sol must be > 0")

    lamports = int(round(float(amount_sol) * 1_000_000_000))

    if transfer is None or TransferParams is None:
        raise RuntimeError("solana.system_program.transfer not available in this solana-py version")

    from solana.transaction import Transaction  # local import for compatibility

    tx = Transaction().add(
        transfer(TransferParams(
            from_pubkey=wallet.pubkey(),
            to_pubkey=PublicKey.from_string(to_pubkey),
            lamports=lamports,
        ))
    )
    resp = await solana_client.send_transaction(tx, wallet, opts=TxOpts(skip_preflight=True))
    await solana_client.confirm_transaction(resp.value, commitment=Confirmed)
    return resp.value


async def transfer_to_savings(amount_sol: float) -> Optional[str]:
    """
    Move `amount_sol` to the SAVINGS_WALLET (set in env). Returns signature or None.
    """
    dest = (os.getenv("SAVINGS_WALLET") or "").strip()
    if not dest:
        logger.warning("SAVINGS_WALLET not set; skipping transfer_to_savings")
        return None
    if amount_sol <= 0:
        return None
    try:
        sig = await transfer_sol(dest, amount_sol)
        try:
            await send_telegram(f"💸 Sent {amount_sol:.4f} SOL to savings\nsig: {sig}")
        except Exception:
            pass
        return sig
    except Exception as e:
        logger.warning(f"transfer_to_savings failed: {e}")
        return None


async def sweep_to_savings(min_reserve_sol: float = 0.05) -> Optional[str]:
    """
    Send all SOL above `min_reserve_sol` to SAVINGS_WALLET.
    Keeps a small buffer in the hot wallet for fees.
    """
    try:
        me = str(wallet.pubkey())
        bal = await get_sol_balance(me)
        amount = round(max(0.0, bal - float(min_reserve_sol)), 9)
        if amount <= 0.0:
            return None
        return await transfer_to_savings(amount)
    except Exception as e:
        logger.warning(f"sweep_to_savings failed: {e}")
        return None
#
───────────────────────────────────────────────────────────────────────────────
# Convenience: token info for alerts (symbol via Dexscreener top pair)
# ───────────────────────────────────────────────────────────────────────────────
async def get_token_info(mint: str) -> Dict[str, Any]:
    snap = await get_pair_snapshot_dexscreener(mint)
    if not snap:
        return {"symbol": "", "priceUsd": 0.0, "liquidityUsd": 0.0}
    sym = snap.get("baseToken", {}).get("symbol") or snap.get("symbol") or ""
    liq = float((snap.get("liquidity") or {}).get("usd") or 0.0)
    px = float(snap.get("priceUsd") or 0.0)
    return {"symbol": sym, "priceUsd": px, "liquidityUsd": liq}

# ───────────────────────────────────────────────────────────────────────────────
# Misc
# ───────────────────────────────────────────────────────────────────────────────
def log_sniper_event(msg: str) -> None:
    logger.info(f"[Sniper] {msg}")

def is_wallet_drained(balance: float) -> bool:
    return balance <= 0.0001
