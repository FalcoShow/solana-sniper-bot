# config.py
# Centralized config for the Solana Sniper + DayTrader bot
# - Rich env parsing with sane defaults
# - Backward-compat aliases (PG_DSN*, NOTION_* aliases, RAYDIUM_PAIRS_API, etc.)
# - Switches for AI scoring, adaptive sell, scanners, social, whale filters, daytrader
# - Storage: Redis + SQLite + optional Postgres

from __future__ import annotations

import os
import json
import logging
from typing import Dict, List

# ---------------------------- helpers ----------------------------
def _get_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")

def _get_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except Exception:
        return float(default)

def _get_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except Exception:
        return int(default)

def _get_list(name: str, default: List[str] | None = None, sep: str = ",") -> List[str]:
    raw = os.getenv(name)
    if not raw:
        return list(default or [])
    return [s.strip() for s in raw.split(sep) if s.strip()]

def _get_json(name: str, default):
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default

def _get_prob(name: str, default: float) -> float:
    """Accept [0..1] or percent [0..100]."""
    v = _get_float(name, default)
    return v / 100.0 if v > 1 else v

# ---------------------------- app/logging ----------------------------
APP_NAME = os.getenv("APP_NAME", "solana-sniper-bot-pro")
ENV = os.getenv("ENV", "dev")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
STRICT_CONFIG = _get_bool("STRICT_CONFIG", False)

ENABLE_METRICS = _get_bool("ENABLE_METRICS", True)
METRICS_PORT = _get_int("METRICS_PORT", 9108)

# ---------------------------- rpc / chain ----------------------------
RPC_URL = os.getenv("RPC_URL", "https://api.mainnet-beta.solana.com")
RPC_URLS = _get_list("RPC_URLS", [RPC_URL])
WEBSOCKET_URL = os.getenv("WEBSOCKET_URL", "")
COMMITMENT = os.getenv("COMMITMENT", "confirmed")  # processed|confirmed|finalized

# Back-compat for legacy code
SOLANA_ENDPOINT = RPC_URL

# Helius (optional)
HELIUS_RPC_URL = os.getenv("HELIUS_RPC_URL", "")

# Jito (optional)
ENABLE_JITO = _get_bool("ENABLE_JITO", False)
JITO_BLOCK_ENGINE_URL = os.getenv("JITO_BLOCK_ENGINE_URL", "")
JITO_TIP_LAMPORTS = _get_int("JITO_TIP_LAMPORTS", 50_000)  # ~0.00005 SOL
JITO_FEE_MICROLAMPORTS = _get_int("JITO_FEE_MICROLAMPORTS", 0)  # optional compute unit price

# ---------------------------- wallet(s) ----------------------------
# WALLET_PRIVATE_KEY should be a JSON array of 64 or 32 integers (preferred).
WALLET_PRIVATE_KEY = os.getenv("WALLET_PRIVATE_KEY", "")
WALLET_PUBKEY = os.getenv("WALLET_PUBKEY", "")
SAVINGS_WALLET = os.getenv("SAVINGS_WALLET", "")

# ---------------------------- external apis ----------------------------
COINGECKO_SOL_API = os.getenv(
    "COINGECKO_SOL_API",
    "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd",
)

# Scanners
RAYDIUM_API_URL = os.getenv("RAYDIUM_API_URL", "https://api.raydium.io/pairs")
DEXSCREENER_API_URL = os.getenv("DEXSCREENER_API_URL", "https://api.dexscreener.com/latest/dex/tokens")
PUMPFUN_API_URL = os.getenv("PUMPFUN_API_URL", "https://pump.fun/api/v1/tokens")
BIRDEYE_API_URL = os.getenv("BIRDEYE_API_URL", "https://public-api.birdeye.so")

# Jupiter v6 (expose both base and explicit)
JUPITER_API_BASE = os.getenv("JUPITER_API_BASE", "https://quote-api.jup.ag/v6")
JUPITER_QUOTE_API = os.getenv("JUPITER_QUOTE_API", f"{JUPITER_API_BASE}/quote")
JUPITER_SWAP_API  = os.getenv("JUPITER_SWAP_API",  JUPITER_API_BASE)  # core_utils appends /quote and /swap

# ---------------------------- credentials / keys ----------------------------
# Alerts
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")
DISCORD_WEBHOOK    = os.getenv("DISCORD_WEBHOOK", "")

# Notion (support both naming styles you used)
NOTION_TOKEN         = os.getenv("NOTION_TOKEN", os.getenv("NOTION_API_KEY", ""))
NOTION_DATABASE_ID   = os.getenv("NOTION_DATABASE_ID", os.getenv("NOTION_DB_TRADES", ""))
ENABLE_NOTION_LOGGING = _get_bool("ENABLE_NOTION_LOGGING", True)

# Optional: Notion property names override (used by core_utils)
NOTION_TITLE_PROP    = os.getenv("NOTION_TITLE_PROP", "Token")
NOTION_PROP_TOKEN    = os.getenv("NOTION_PROP_TOKEN", "Mint")
NOTION_PROP_ENTRY    = os.getenv("NOTION_PROP_ENTRY", "Entry Price")
NOTION_PROP_CURRENT  = os.getenv("NOTION_PROP_CURRENT", "Current Price")
NOTION_PROP_PROFIT   = os.getenv("NOTION_PROP_PROFIT", "Profit")
NOTION_PROP_REASON   = os.getenv("NOTION_PROP_REASON", "Reason")
NOTION_PROP_TIME     = os.getenv("NOTION_PROP_TIME", "Timestamp")

# Birdeye
BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY", "")

# OpenAI (optional, if you later enable LLM features)
OPENAI_API_KEY   = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL     = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
OPENAI_BASE_URL  = os.getenv("OPENAI_BASE_URL", "")
FINE_TUNED_MODEL = os.getenv("FINE_TUNED_MODEL", "")

# ---------------------------- storage ----------------------------
REDIS_URL      = os.getenv("REDIS_URL", "redis://localhost:6379/0")
SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", os.path.join("data", "state.db"))

# Positions store backend
POSITIONS_BACKEND   = os.getenv("POSITIONS_BACKEND", "json").lower()  # "json" | "sqlite"
POSITIONS_JSON_PATH = os.getenv("POSITIONS_JSON_PATH", os.path.join("data", "positions.json"))

# Postgres (support INTERNAL/EXTERNAL scheme + PG_ENABLED)
PG_ENABLED        = _get_bool("PG_ENABLED", True)
PG_DSN            = os.getenv("PG_DSN", "")
PG_DSN_INTERNAL   = os.getenv("PG_DSN_INTERNAL", "")
PG_DSN_EXTERNAL   = os.getenv("PG_DSN_EXTERNAL", "")
POSTGRES_DSN      = os.getenv("POSTGRES_DSN", PG_DSN or PG_DSN_INTERNAL or PG_DSN_EXTERNAL)

# ---------------------------- trading / execution ----------------------------
# Slippage for Jupiter routes
JUPITER_MAX_SLIPPAGE = _get_float("JUPITER_MAX_SLIPPAGE", 0.005)  # 0.5%

TRANSACTION_TIMEOUT = _get_int("TRANSACTION_TIMEOUT", 20)
MAX_RETRIES         = _get_int("MAX_RETRIES", 3)
EXEC_DELAY          = _get_int("SNIPER_EXEC_DELAY", _get_int("EXEC_DELAY", 0))  # tolerate both names

# Dynamic sizing
MAX_RISK_PER_TRADE = _get_float("MAX_RISK_PER_TRADE", 0.05)  # 5% of SOL stack
MIN_SOL_BALANCE    = _get_float("MIN_SOL_BALANCE", 0.20)
MIN_VOLATILITY     = _get_float("MIN_VOLATILITY", 0.02)
PARTIAL_SELL_CHUNKS = _get_int("PARTIAL_SELL_CHUNKS", 4)

# Targets / SL / hold
TARGET_PROFIT_PERCENT   = _get_float("TARGET_PROFIT_PERCENT", 200.0)  # 200% = 2x
STOP_LOSS_PERCENT       = _get_float("STOP_LOSS_PERCENT", 20.0)
TRAILING_STOP_ENABLED   = _get_bool("TRAILING_STOP_ENABLED", True)
MAX_HOLD_TIME_MINUTES   = _get_int("MAX_HOLD_TIME_MINUTES", 360)
HOLD_IF_SCORE_ABOVE     = _get_prob("HOLD_IF_SCORE_ABOVE", 0.85)  # 0..1

# Reinvestor
REINVEST_PERCENT             = _get_float("REINVEST_PERCENT", 80.0)
REINVESTMENT_PERCENT_CAP     = _get_float("REINVESTMENT_PERCENT_CAP", 50.0)
REINVESTMENT_VOLUME_MIN      = _get_float("REINVESTMENT_VOLUME_MIN", 5_000.0)
REINVESTMENT_MIN_PROFIT_SOL  = _get_float("REINVESTMENT_MIN_PROFIT_SOL", 0.01)
REINVESTMENT_HONEYPOT_CHECK  = _get_bool("REINVESTMENT_HONEYPOT_CHECK", True)

# ---------------------------- scanners / arbitrage ----------------------------
ENABLE_RAYDIUM_SCANNER     = _get_bool("ENABLE_RAYDIUM_SCANNER", True)
ENABLE_DEXSCREENER_SCANNER = _get_bool("ENABLE_DEXSCREENER_SCANNER", True)
ENABLE_PUMPFUN_SCANNER     = _get_bool("ENABLE_PUMPFUN_SCANNER", True)
ENABLE_ARBITRAGE           = _get_bool("ENABLE_ARBITRAGE", True)
DEXES: List[str]           = _get_list("DEXES", ["jupiter", "raydium", "orca"])

# Scanner thresholds & alerts
SCANNER_MIN_LIQ_USD       = _get_float("SCANNER_MIN_LIQ_USD", 1_000.0)
SCANNER_MIN_FDV_USD       = _get_float("SCANNER_MIN_FDV_USD", 10_000.0)
SCANNER_MIN_FRAUD_SCORE   = _get_float("SCANNER_MIN_FRAUD_SCORE", 70.0)  # 0..100
SCANNER_ALERTS_ENABLED    = _get_bool("SCANNER_ALERTS_ENABLED", True)

# Loops / cooldowns
ALERT_COOLDOWN_SECONDS     = _get_int("ALERT_COOLDOWN_SECONDS", 900)
SCAN_INTERVAL              = _get_int("SCAN_INTERVAL", 60)
POSITION_MONITOR_INTERVAL  = _get_int("POSITION_MONITOR_INTERVAL", 20)
REINVESTMENT_CHECK_INTERVAL = _get_int("REINVESTMENT_CHECK_INTERVAL", 60)

# AI strategy loop
AI_ADAPTIVE_SELL         = _get_bool("AI_ADAPTIVE_SELL", True)
STRATEGY_REDIS_PREFIX    = os.getenv("STRATEGY_REDIS_PREFIX", "strategy:v1")
STRATEGY_UPDATE_INTERVAL = _get_int("STRATEGY_UPDATE_INTERVAL", 1800)  # 30 min

# Whale watch
ENABLE_WHALE_FILTERS   = _get_bool("ENABLE_WHALE_FILTERS", True)
DYNAMIC_WHALE_DISCOVERY = _get_bool("DYNAMIC_WHALE_DISCOVERY", True)

# Curated list (CSV or JSON)
CURATED_WHALE_WALLETS = _get_list("CURATED_WHALE_WALLETS", [])
WHALE_WALLETS_JSON    = _get_json("WHALE_WALLETS_JSON", {})

# Final merged view (name->address)
WHALE_WALLETS: Dict[str, str] = {}
if isinstance(WHALE_WALLETS_JSON, dict) and WHALE_WALLETS_JSON:
    WHALE_WALLETS.update(WHALE_WALLETS_JSON)
for i, addr in enumerate(CURATED_WHALE_WALLETS):
    WHALE_WALLETS.setdefault(f"CURATED_{i+1}", addr)

# Social scraping / alpha feeds (off by default to save cost)
ENABLE_SOCIAL_SCRAPING   = _get_bool("ENABLE_SOCIAL_SCRAPING", False)
X_BEARER_TOKEN           = os.getenv("X_BEARER_TOKEN", "")
TELEGRAM_SCRAPE_BOT      = os.getenv("TELEGRAM_SCRAPE_BOT", "")
DISCORD_SEARCH_CHANNEL_ID = os.getenv("DISCORD_SEARCH_CHANNEL_ID", "")

# ---------------------------- daytrader (pro) ----------------------------
ENABLE_DAYTRADER = _get_bool("ENABLE_DAYTRADER", False)

DAYTRADER_SYMBOLS     = _get_list("DAYTRADER_SYMBOLS", ["SOL/USDC"])
DAYTRADER_TIMEFRAMES  = _get_list("DAYTRADER_TIMEFRAMES", ["1m", "5m", "15m", "1h"])
DAYTRADER_EXCHANGES   = _get_list("DAYTRADER_EXCHANGES", ["binance", "bybit", "okx"])

DT_FEATURE_SET        = _get_list("DT_FEATURE_SET", ["rsi", "macd", "bbands", "atr", "orderflow"])
DT_MODEL_KIND         = os.getenv("DT_MODEL_KIND", "xgboost")
DT_PROB_THRESHOLD     = _get_prob("DT_PROB_THRESHOLD", 0.62)
DT_MAX_CONCURRENT     = _get_int("DT_MAX_CONCURRENT", 3)
DT_MAX_LEVERAGE       = _get_float("DT_MAX_LEVERAGE", 1.0)
DT_MAX_RISK_PER_TRADE = _get_float("DT_MAX_RISK_PER_TRADE", 0.02)
DT_TAKE_PROFIT_BPS    = _get_int("DT_TAKE_PROFIT_BPS", 120)
DT_STOP_LOSS_BPS      = _get_int("DT_STOP_LOSS_BPS", 60)
DT_TRAILING_BPS       = _get_int("DT_TRAILING_BPS", 50)
DT_COOLDOWN_SEC       = _get_int("DT_COOLDOWN_SEC", 60)
DT_PAPER_TRADING      = _get_bool("DT_PAPER_TRADING", True)

# ---------------------------- compat exports / aliases ----------------------------
# For older modules that import these names directly
RAYDIUM_PAIRS_API = RAYDIUM_API_URL  # alias expected by raydium_scanner
DEXSCREENER_TOKENS_API = DEXSCREENER_API_URL
PUMPFUN_TOKENS_API = PUMPFUN_API_URL

# ---------------------------- validation (optional) ----------------------------
_CRITICAL = [
    "WALLET_PRIVATE_KEY",
    "REDIS_URL",
    "BIRDEYE_API_KEY",
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_CHAT_ID",
]
_missing = [k for k in _CRITICAL if not os.getenv(k)]

if _missing and STRICT_CONFIG:
    raise EnvironmentError(
        f"[CONFIG] Missing required environment variables: {', '.join(_missing)}"
    )
elif _missing:
    logging.getLogger(__name__).warning(
        "⚠️ Missing env vars (running in non-strict mode): %s", ", ".join(_missing)
    )

# ---------------------------- summary ----------------------------
def summarize() -> dict:
    """Convenient for /health endpoints & debugging."""
    return {
        "app": APP_NAME,
        "env": ENV,
        "log_level": LOG_LEVEL,
        "rpc_primary": RPC_URL,
        "rpc_pool_count": len(RPC_URLS),
        "commitment": COMMITMENT,
        "jito_enabled": ENABLE_JITO,
        "metrics": {"enabled": ENABLE_METRICS, "port": METRICS_PORT},
        "redis_url": REDIS_URL,
        "sqlite_db": SQLITE_DB_PATH,
        "pg": {
            "enabled": PG_ENABLED,
            "dsn": bool(POSTGRES_DSN or PG_DSN or PG_DSN_INTERNAL or PG_DSN_EXTERNAL),
        },
        "notion": {
            "enabled": ENABLE_NOTION_LOGGING and bool(NOTION_TOKEN and NOTION_DATABASE_ID),
            "props": {
                "title": NOTION_TITLE_PROP,
                "mint": NOTION_PROP_TOKEN,
                "entry": NOTION_PROP_ENTRY,
                "current": NOTION_PROP_CURRENT,
                "profit": NOTION_PROP_PROFIT,
                "reason": NOTION_PROP_REASON,
                "time": NOTION_PROP_TIME,
            },
        },
        "openai": {
            "model": OPENAI_MODEL or FINE_TUNED_MODEL,
            "base_url": bool(OPENAI_BASE_URL),
        },
        "scanners": {
            "raydium": ENABLE_RAYDIUM_SCANNER,
            "dexscreener": ENABLE_DEXSCREENER_SCANNER,
            "pumpfun": ENABLE_PUMPFUN_SCANNER,
            "arbitrage": ENABLE_ARBITRAGE,
            "min_liq_usd": SCANNER_MIN_LIQ_USD,
            "min_fdv_usd": SCANNER_MIN_FDV_USD,
            "min_fraud_score": SCANNER_MIN_FRAUD_SCORE,
            "alerts": SCANNER_ALERTS_ENABLED,
        },
        "trading": {
            "slippage": JUPITER_MAX_SLIPPAGE,
            "max_risk_per_trade": MAX_RISK_PER_TRADE,
            "min_sol_balance": MIN_SOL_BALANCE,
            "min_volatility": MIN_VOLATILITY,
            "partial_sell_chunks": PARTIAL_SELL_CHUNKS,
            "target_profit_pct": TARGET_PROFIT_PERCENT,
            "stop_loss_pct": STOP_LOSS_PERCENT,
            "trailing_stop": TRAILING_STOP_ENABLED,
            "max_hold_minutes": MAX_HOLD_TIME_MINUTES,
            "hold_score_threshold": HOLD_IF_SCORE_ABOVE,
        },
        "daytrader": {
            "enabled": ENABLE_DAYTRADER,
            "symbols": DAYTRADER_SYMBOLS,
            "tfs": DAYTRADER_TIMEFRAMES,
            "exchanges": DAYTRADER_EXCHANGES,
            "model": DT_MODEL_KIND,
            "prob_threshold": DT_PROB_THRESHOLD,
            "max_concurrent": DT_MAX_CONCURRENT,
            "paper_trading": DT_PAPER_TRADING,
        },
        "dexes": DEXES,
        "whales_enabled": ENABLE_WHALE_FILTERS,
        "dynamic_whales": DYNAMIC_WHALE_DISCOVERY,
        "whale_list_size": len(WHALE_WALLETS),
    }
