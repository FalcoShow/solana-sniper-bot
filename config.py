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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def _get_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return int(default)
    try:
        return int(raw)
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
    """
    Accept [0..1] or percent [0..100].
    If value > 1 we assume it is percent and divide by 100.
    """
    v = _get_float(name, default)
    return v / 100.0 if v > 1 else v


# ---------------------------------------------------------------------------
# App / logging
# ---------------------------------------------------------------------------

APP_NAME = os.getenv("APP_NAME", "solana-sniper-bot-pro")
ENV = os.getenv("ENV", "dev")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
STRICT_CONFIG = _get_bool("STRICT_CONFIG", False)

ENABLE_METRICS = _get_bool("ENABLE_METRICS", True)
METRICS_PORT = _get_int("METRICS_PORT", 9108)


# ---------------------------------------------------------------------------
# RPC / chain
# ---------------------------------------------------------------------------

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
JITO_FEE_MICROLAMPORTS = _get_int("JITO_FEE_MICROLAMPORTS", 0)  # optional CU price


# ---------------------------------------------------------------------------
# Wallet(s)
# ---------------------------------------------------------------------------

# WALLET_PRIVATE_KEY should be a JSON array of 64 or 32 integers (preferred).
WALLET_PRIVATE_KEY = os.getenv("WALLET_PRIVATE_KEY", "")
WALLET_PUBKEY = os.getenv("WALLET_PUBKEY", os.getenv("WALLET_PUBLIC_KEY", ""))
SAVINGS_WALLET = os.getenv("SAVINGS_WALLET", "")


# ---------------------------------------------------------------------------
# External APIs / scanners
# ---------------------------------------------------------------------------

COINGECKO_SOL_API = os.getenv(
    "COINGECKO_SOL_API",
    "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd",
)

# Legacy scanner endpoints (still used by some scripts)
RAYDIUM_API_URL = os.getenv("RAYDIUM_API_URL", "https://api.raydium.io/pairs")
DEXSCREENER_API_URL = os.getenv(
    "DEXSCREENER_API_URL",
    "https://api.dexscreener.com/latest/dex/tokens",
)
PUMPFUN_API_URL = os.getenv("PUMPFUN_API_URL", "https://pump.fun/api/v1/tokens")
BIRDEYE_API_URL = os.getenv("BIRDEYE_API_URL", "https://public-api.birdeye.so")

# Dexscreener base (for raydium_scanner via /latest/dex/search)
DEXSCREENER_BASE_URL = os.getenv(
    "DEXSCREENER_BASE_URL",
    "https://api.dexscreener.com",
).rstrip("/")

# Pump.fun scanner tuning
PUMPFUN_API_BASE = os.getenv("PUMPFUN_API_BASE", "").rstrip("/")
PUMPFUN_HTTP_TIMEOUT_S = _get_int("PUMPFUN_HTTP_TIMEOUT_S", 15)
PUMPFUN_HTTP_CONN_LIMIT = _get_int("PUMPFUN_HTTP_CONN_LIMIT", 30)
PUMPFUN_API_LIMIT = _get_int("PUMPFUN_API_LIMIT", 20)
PUMPFUN_DOM_LIMIT = _get_int("PUMPFUN_DOM_LIMIT", PUMPFUN_API_LIMIT)
PUMPFUN_SKIP_NEXT = _get_bool("PUMPFUN_SKIP_NEXT", False)
PUMPFUN_SKIP_HTMLNEXT = _get_bool("PUMPFUN_SKIP_HTMLNEXT", False)


# ---------------------------------------------------------------------------
# Jupiter v6 / router
# ---------------------------------------------------------------------------

# lightning_impl.py mainly uses the JUP_* envs directly; these are for convenience.
JUP_BASE_URL = os.getenv("JUP_BASE_URL", "https://api.jup.ag").rstrip("/")
JUP_ALT_BASE_URLS = _get_list("JUP_ALT_BASE_URLS", [JUP_BASE_URL])
JUP_AS_LEGACY = _get_bool("JUP_AS_LEGACY", True)
JUP_USE_SHARED_ACCOUNTS = _get_bool("JUP_USE_SHARED_ACCOUNTS", True)
JUP_ONLY_DIRECT = _get_bool("JUP_ONLY_DIRECT", False)
JUP_PRO_KEY = os.getenv("JUP_PRO_KEY", os.getenv("JUP_API_KEY", ""))

# Old core_utils Jupiter usage (safe to keep)
JUPITER_API_BASE = os.getenv("JUPITER_API_BASE", "https://quote-api.jup.ag/v6")
JUPITER_QUOTE_API = os.getenv("JUPITER_QUOTE_API", f"{JUPITER_API_BASE}/quote")
JUPITER_SWAP_API = os.getenv("JUPITER_SWAP_API", JUPITER_API_BASE)


# ---------------------------------------------------------------------------
# Credentials / keys
# ---------------------------------------------------------------------------

# Alerts
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK", "")

# Notion (support both naming styles you used)
NOTION_TOKEN = os.getenv("NOTION_TOKEN", os.getenv("NOTION_API_KEY", ""))
NOTION_DATABASE_ID = os.getenv(
    "NOTION_DATABASE_ID", os.getenv("NOTION_DB_TRADES", "")
)
ENABLE_NOTION_LOGGING = _get_bool("ENABLE_NOTION_LOGGING", True)

# Optional: Notion property names override (used by core_utils)
NOTION_TITLE_PROP = os.getenv("NOTION_TITLE_PROP", "Token")
NOTION_PROP_TOKEN = os.getenv("NOTION_PROP_TOKEN", "Mint")
NOTION_PROP_ENTRY = os.getenv("NOTION_PROP_ENTRY", "Entry Price")
NOTION_PROP_CURRENT = os.getenv("NOTION_PROP_CURRENT", "Current Price")
NOTION_PROP_PROFIT = os.getenv("NOTION_PROP_PROFIT", "Profit")
NOTION_PROP_REASON = os.getenv("NOTION_PROP_REASON", "Reason")
NOTION_PROP_TIME = os.getenv("NOTION_PROP_TIME", "Timestamp")

# Birdeye
BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY", "")

# OpenAI
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "")
FINE_TUNED_MODEL = os.getenv("FINE_TUNED_MODEL", "")


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
SQLITE_DB_PATH = os.getenv(
    "SQLITE_DB_PATH",
    os.path.join("data", "state.db"),
)

# Positions store backend
POSITIONS_BACKEND = os.getenv("POSITIONS_BACKEND", "json").lower()  # "json" | "sqlite"
POSITIONS_JSON_PATH = os.getenv(
    "POSITIONS_JSON_PATH",
    os.path.join("data", "positions.json"),
)

# Postgres (support INTERNAL/EXTERNAL scheme + PG_ENABLED)
PG_ENABLED = _get_bool("PG_ENABLED", True)
PG_DSN = os.getenv("PG_DSN", "")
PG_DSN_INTERNAL = os.getenv("PG_DSN_INTERNAL", "")
PG_DSN_EXTERNAL = os.getenv("PG_DSN_EXTERNAL", "")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", PG_DSN or PG_DSN_INTERNAL or PG_DSN_EXTERNAL)


# ---------------------------------------------------------------------------
# Trading / execution (shared)
# ---------------------------------------------------------------------------

# High-level mode for the sniper
AUTO_BUY_ENABLED = _get_bool("AUTO_BUY_ENABLED", True)
AUTO_SELL_ENABLED = _get_bool("AUTO_SELL_ENABLED", True)
PAPER_TRADING = _get_bool("PAPER_TRADING", False)  # sniper-level paper trading

# Global safety toggles used by sniper/sniper.py
PAUSE_TRADING = _get_bool("PAUSE_TRADING", False)
DRY_RUN = _get_bool("DRY_RUN", False)

# Slippage for Jupiter routes
JUPITER_MAX_SLIPPAGE = _get_float("JUPITER_MAX_SLIPPAGE", 0.005)  # 0.5%

TRANSACTION_TIMEOUT = _get_int("TRANSACTION_TIMEOUT", 20)
MAX_RETRIES = _get_int("MAX_RETRIES", 3)
EXEC_DELAY = _get_int("SNIPER_EXEC_DELAY", _get_int("EXEC_DELAY", 0))

# Dynamic sizing / risk
MAX_RISK_PER_TRADE = _get_float("MAX_RISK_PER_TRADE", 0.05)  # 5% of SOL stack
MIN_SOL_BALANCE = _get_float("MIN_SOL_BALANCE", 0.20)
MIN_VOLATILITY = _get_float("MIN_VOLATILITY", 0.02)
PARTIAL_SELL_CHUNKS = _get_int("PARTIAL_SELL_CHUNKS", 4)

# Ticket sizing (used by risk_manager + sniper fallbacks)
TICKET_PCT_EQUITY = _get_float("TICKET_PCT_EQUITY", 0.04)  # 4% of equity
TICKET_USD_CAP = _get_float("TICKET_USD_CAP", 300.0)

# Targets / SL / hold
TARGET_PROFIT_PERCENT = _get_float("TARGET_PROFIT_PERCENT", 200.0)  # 200% = 2x
STOP_LOSS_PERCENT = _get_float("STOP_LOSS_PERCENT", 20.0)
TRAILING_STOP_ENABLED = _get_bool("TRAILING_STOP_ENABLED", True)
MAX_HOLD_TIME_MINUTES = _get_int("MAX_HOLD_TIME_MINUTES", 360)
HOLD_IF_SCORE_ABOVE = _get_prob("HOLD_IF_SCORE_ABOVE", 0.85)  # 0..1

# Take-profit ladder used by sniper._buy_token
TAKE_PROFIT1_PCT = _get_float("TAKE_PROFIT1_PCT", 30.0)
TAKE_PROFIT2_PCT = _get_float("TAKE_PROFIT2_PCT", 60.0)
TAKE_PROFIT3_PCT = _get_float("TAKE_PROFIT3_PCT", 120.0)

# Reinvestor
REINVEST_PERCENT = _get_float("REINVEST_PERCENT", 80.0)
REINVESTMENT_PERCENT_CAP = _get_float("REINVESTMENT_PERCENT_CAP", 50.0)
REINVESTMENT_VOLUME_MIN = _get_float("REINVESTMENT_VOLUME_MIN", 5_000.0)
REINVESTMENT_MIN_PROFIT_SOL = _get_float("REINVESTMENT_MIN_PROFIT_SOL", 0.01)
REINVESTMENT_HONEYPOT_CHECK = _get_bool("REINVESTMENT_HONEYPOT_CHECK", True)

# Entry momentum guard (Track A / Track B)
ENTRY_MOMO_MIN_TRADES = _get_int("ENTRY_MOMO_MIN_TRADES", 3)
ENTRY_MOMO_MIN_UP_PCT = _get_float("ENTRY_MOMO_MIN_UP_PCT", 2.5)
ENTRY_MOMO_ALLOW_NO_DATA_FOR_PUMP = _get_bool("ENTRY_MOMO_ALLOW_NO_DATA_FOR_PUMP", False)


# ---------------------------------------------------------------------------
# Sniper loop / scanners tuning (matches sniper/sniper.py)
# ---------------------------------------------------------------------------

# Scan cadence
SCAN_INTERVAL = _get_int("SCAN_INTERVAL", 60)           # generic
SCANNER_PERIOD_SEC = _get_int("SCANNER_PERIOD_SEC", 4)  # main loop

# Buy / concurrency
BUY_COOLDOWN_SEC = _get_int("BUY_COOLDOWN_SEC", 3)
MAX_CONCURRENT_BUYS = _get_int("MAX_CONCURRENT_BUYS", 2)
# PF_MAX_CONCURRENT is what the pipeline actually uses; expose as alias
PF_MAX_CONCURRENT = _get_int("PF_MAX_CONCURRENT", MAX_CONCURRENT_BUYS)

PREWARM_TOP_N = _get_int("PREWARM_TOP_N", 5)

# Global min-liquidity filter used by scanners + sniper basic filters
SCANNER_MIN_LIQ_USD = _get_float("SCANNER_MIN_LIQ_USD", 3_500.0)
# Local per-sniper filter (defaults to scanner min-liq)
MIN_LIQ_USD = _get_float("MIN_LIQ_USD", SCANNER_MIN_LIQ_USD)
# Alias for old typo used in summarize()
LIQ_MIN_USD = MIN_LIQ_USD

# Basic pool filters
MIN_POOL_AGE_MIN = _get_int("MIN_POOL_AGE_MIN", 1)

# RouteGuard cooldown (no-route backoff)
NOROUTE_COOLDOWN_SEC = _get_int("NOROUTE_COOLDOWN_SEC", 600)

# Block/allow lists
BLOCKLIST_MINTS = _get_list("BLOCKLIST_MINTS", [])
ALLOWLIST_MINTS = _get_list("ALLOWLIST_MINTS", [])

# Strategy preset
STRATEGY_PRESET = os.getenv("STRATEGY_PRESET", "max-pnl").lower()

# Heartbeats / Telegram / PnL
TELEGRAM_HEARTBEATS = _get_bool("TELEGRAM_HEARTBEATS", True)
HEARTBEAT_EVERY_SEC = _get_int("HEARTBEAT_EVERY_SEC", 300)
HEARTBEAT_ONLY_ON_CHANGE = _get_bool("HEARTBEAT_ONLY_ON_CHANGE", True)
TELEGRAM_NOTIFY_ROUTE_BLOCKS = _get_bool("TELEGRAM_NOTIFY_ROUTE_BLOCKS", True)

PNL_TELEGRAM_HEARTBEAT = _get_bool("PNL_TELEGRAM_HEARTBEAT", True)

# Moonshot budget (tiny new/low-liq plays)
MOONSHOT_ENABLED = _get_bool("MOONSHOT_ENABLED", True)
MOONSHOT_DAILY_USD = _get_float("MOONSHOT_DAILY_USD", 100.0)
MOONSHOT_MAX_TRADE_USD = _get_float("MOONSHOT_MAX_TRADE_USD", 25.0)
MOONSHOT_AGE_CUTOFF_MIN = _get_int("MOONSHOT_AGE_CUTOFF_MIN", 30)
MOONSHOT_LIQ_CUTOFF_USD = _get_float("MOONSHOT_LIQ_CUTOFF_USD", 20_000.0)
MOONSHOT_IMPACT_CUTOFF_BPS = _get_float("MOONSHOT_IMPACT_CUTOFF_BPS", 150.0)


# ---------------------------------------------------------------------------
# Scanners / arbitrage (legacy, still used by some scripts)
# ---------------------------------------------------------------------------

ENABLE_RAYDIUM_SCANNER = _get_bool("ENABLE_RAYDIUM_SCANNER", True)
ENABLE_DEXSCREENER_SCANNER = _get_bool("ENABLE_DEXSCREENER_SCANNER", True)
ENABLE_PUMPFUN_SCANNER = _get_bool("ENABLE_PUMPFUN_SCANNER", True)
ENABLE_ARBITRAGE = _get_bool("ENABLE_ARBITRAGE", True)
DEXES: List[str] = _get_list("DEXES", ["jupiter", "raydium", "orca"])

# Base scanner thresholds (older system – kept for compatibility)
SCANNER_MIN_FDV_USD = _get_float("SCANNER_MIN_FDV_USD", 10_000.0)
SCANNER_MIN_FRAUD_SCORE = _get_float("SCANNER_MIN_FRAUD_SCORE", 70.0)  # 0..100
SCANNER_ALERTS_ENABLED = _get_bool("SCANNER_ALERTS_ENABLED", True)

SCANNER_MIN_BUYS_5M = _get_int("SCANNER_MIN_BUYS_5M", 5)
SCANNER_MIN_BUYS_15M = _get_int("SCANNER_MIN_BUYS_15M", 8)
SCANNER_MIN_VOLUME_5M_USD = _get_float("SCANNER_MIN_VOLUME_5M_USD", 2_500.0)
SCANNER_MIN_VOLUME_15M_USD = _get_float("SCANNER_MIN_VOLUME_15M_USD", 5_000.0)

SCANNER_REQUIRE_BUY_PRESSURE = _get_bool("SCANNER_REQUIRE_BUY_PRESSURE", True)
SCANNER_MIN_BUY_RATIO = _get_float("SCANNER_MIN_BUY_RATIO", 0.60)  # buys / (buys+sells)

SCANNER_MIN_PRICE_CHANGE_5M = _get_float("SCANNER_MIN_PRICE_CHANGE_5M", 5.0)    # %
SCANNER_MIN_PRICE_CHANGE_15M = _get_float("SCANNER_MIN_PRICE_CHANGE_15M", 10.0)  # %

SCANNER_MAX_AGE_MIN = _get_int("SCANNER_MAX_AGE_MIN", 90)
SCANNER_MAX_MC_USD = _get_float("SCANNER_MAX_MC_USD", 2_000_000.0)
SCANNER_MIN_HOLDERS = _get_int("SCANNER_MIN_HOLDERS", 50)

# Loops / cooldowns for aux scanners
ALERT_COOLDOWN_SECONDS = _get_int("ALERT_COOLDOWN_SECONDS", 900)
POSITION_MONITOR_INTERVAL = _get_int("POSITION_MONITOR_INTERVAL", 20)
REINVESTMENT_CHECK_INTERVAL = _get_int("REINVESTMENT_CHECK_INTERVAL", 60)


# ---------------------------------------------------------------------------
# NEW: PairFilter-centric thresholds (used by utils_pkg.pair_filters)
# These are the knobs that directly control which coins are considered
# "viable" and how momentum / risk scoring behaves.
# ---------------------------------------------------------------------------

# Hard ranges for liquidity / mcap / age
PF_SCAN_MIN_LIQ_USD = _get_float("PF_SCAN_MIN_LIQ_USD", SCANNER_MIN_LIQ_USD)
PF_SCAN_MAX_LIQ_USD = _get_float("PF_SCAN_MAX_LIQ_USD", 250_000.0)

PF_SCAN_MIN_MCAP_USD = _get_float("PF_SCAN_MIN_MCAP_USD", 3_000.0)
PF_SCAN_MAX_MCAP_USD = _get_float("PF_SCAN_MAX_MCAP_USD", 750_000.0)

# Max age (minutes) for a token to even be considered
PF_SCAN_MAX_TOKEN_AGE_MIN = _get_float("PF_SCAN_MAX_TOKEN_AGE_MIN", 240.0)  # 4h

# Recent activity thresholds (5m window)
PF_SCAN_MIN_5M_VOL_USD = _get_float("PF_SCAN_MIN_5M_VOL_USD", 1_000.0)
PF_SCAN_MIN_5M_BUYS = _get_int("PF_SCAN_MIN_5M_BUYS", 8)
PF_SCAN_MIN_BUY_TO_SELL_RATIO = _get_float("PF_SCAN_MIN_BUY_TO_SELL_RATIO", 1.2)

# Whale presence
PF_SCAN_MIN_WHALE_SIZE_SOL = _get_float("PF_SCAN_MIN_WHALE_SIZE_SOL", 3.0)
PF_SCAN_MIN_WHALE_COUNT_5M = _get_int("PF_SCAN_MIN_WHALE_COUNT_5M", 1)

# AI rug-risk + scoring thresholds
PF_MAX_RUG_RISK_SCORE = _get_float("PF_MAX_RUG_RISK_SCORE", 0.65)  # 0..1

# Minimum scores to pass filter
PF_MIN_MOMENTUM_SCORE = _get_float("PF_MIN_MOMENTUM_SCORE", 0.25)  # 0..1
PF_MIN_FINAL_SCORE = _get_float("PF_MIN_FINAL_SCORE", 0.20)        # 0..1

# How strongly AI rug score penalizes the final score (0..1)
PF_RISK_PENALTY_WEIGHT = _get_float("PF_RISK_PENALTY_WEIGHT", 0.7)


# ---------------------------------------------------------------------------
# AI strategy loop
# ---------------------------------------------------------------------------

AI_ADAPTIVE_SELL = _get_bool("AI_ADAPTIVE_SELL", True)
STRATEGY_REDIS_PREFIX = os.getenv("STRATEGY_REDIS_PREFIX", "strategy:v1")
STRATEGY_UPDATE_INTERVAL = _get_int("STRATEGY_UPDATE_INTERVAL", 1800)  # 30 min


# ---------------------------------------------------------------------------
# Whale watch
# ---------------------------------------------------------------------------

ENABLE_WHALE_FILTERS = _get_bool("ENABLE_WHALE_FILTERS", True)
DYNAMIC_WHALE_DISCOVERY = _get_bool("DYNAMIC_WHALE_DISCOVERY", True)

CURATED_WHALE_WALLETS = _get_list("CURATED_WHALE_WALLETS", [])
WHALE_WALLETS_JSON = _get_json("WHALE_WALLETS_JSON", {})

WHALE_WALLETS: Dict[str, str] = {}
if isinstance(WHALE_WALLETS_JSON, dict) and WHALE_WALLETS_JSON:
    WHALE_WALLETS.update(WHALE_WALLETS_JSON)
for i, addr in enumerate(CURATED_WHALE_WALLETS):
    WHALE_WALLETS.setdefault(f"CURATED_{i+1}", addr)

# Social scraping / alpha feeds (off by default to save cost)
ENABLE_SOCIAL_SCRAPING = _get_bool("ENABLE_SOCIAL_SCRAPING", False)
X_BEARER_TOKEN = os.getenv("X_BEARER_TOKEN", "")
TELEGRAM_SCRAPE_BOT = os.getenv("TELEGRAM_SCRAPE_BOT", "")
DISCORD_SEARCH_CHANNEL_ID = os.getenv("DISCORD_SEARCH_CHANNEL_ID", "")


# ---------------------------------------------------------------------------
# Daytrader (pro)
# ---------------------------------------------------------------------------

ENABLE_DAYTRADER = _get_bool("ENABLE_DAYTRADER", False)

DAYTRADER_SYMBOLS = _get_list("DAYTRADER_SYMBOLS", ["SOL/USDC"])
DAYTRADER_TIMEFRAMES = _get_list("DAYTRADER_TIMEFRAMES", ["1m", "5m", "15m", "1h"])
DAYTRADER_EXCHANGES = _get_list(
    "DAYTRADER_EXCHANGES",
    ["binance", "bybit", "okx"],
)

DT_FEATURE_SET = _get_list(
    "DT_FEATURE_SET",
    ["rsi", "macd", "bbands", "atr", "orderflow"],
)
DT_MODEL_KIND = os.getenv("DT_MODEL_KIND", "xgboost")
DT_PROB_THRESHOLD = _get_prob("DT_PROB_THRESHOLD", 0.62)
DT_MAX_CONCURRENT = _get_int("DT_MAX_CONCURRENT", 3)
DT_MAX_LEVERAGE = _get_float("DT_MAX_LEVERAGE", 1.0)
DT_MAX_RISK_PER_TRADE = _get_float("DT_MAX_RISK_PER_TRADE", 0.02)
DT_TAKE_PROFIT_BPS = _get_int("DT_TAKE_PROFIT_BPS", 120)
DT_STOP_LOSS_BPS = _get_int("DT_STOP_LOSS_BPS", 60)
DT_TRAILING_BPS = _get_int("DT_TRAILING_BPS", 50)
DT_COOLDOWN_SEC = _get_int("DT_COOLDOWN_SEC", 60)
DT_PAPER_TRADING = _get_bool("DT_PAPER_TRADING", True)


# ---------------------------------------------------------------------------
# Compat exports / aliases
# ---------------------------------------------------------------------------

# For older modules that import these names directly
RAYDIUM_PAIRS_API = RAYDIUM_API_URL          # alias expected by old raydium scanner
DEXSCREENER_TOKENS_API = DEXSCREENER_API_URL
PUMPFUN_TOKENS_API = PUMPFUN_API_URL


# ---------------------------------------------------------------------------
# Validation (optional)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Summary helper
# ---------------------------------------------------------------------------

def summarize():
    """
    Lightweight summary of the main knobs we care about for debugging
    and sanity checks.  This is only for introspection, not core logic.
    """
    return {
        "sniper": {
            "pause_trading": PAUSE_TRADING,
            "dry_run": DRY_RUN,
            "scanner_period_sec": SCANNER_PERIOD_SEC,
            "buy_cooldown_sec": BUY_COOLDOWN_SEC,
            "max_concurrent_buys": PF_MAX_CONCURRENT,
            "prewarm_top_n": PREWARM_TOP_N,
            "min_liq_usd": LIQ_MIN_USD,
            "min_pool_age_min": MIN_POOL_AGE_MIN,
            "strategy_preset": STRATEGY_PRESET,
            "telegram_heartbeats": PNL_TELEGRAM_HEARTBEAT,
            "route_blocks_notify": TELEGRAM_NOTIFY_ROUTE_BLOCKS,
            "moonshot_enabled": MOONSHOT_ENABLED,
            "moonshot_daily_usd": MOONSHOT_DAILY_USD,
            # entry momentum guard
            "entry_momo_min_trades": ENTRY_MOMO_MIN_TRADES,
            "entry_momo_min_up_pct": ENTRY_MOMO_MIN_UP_PCT,
            "entry_momo_allow_no_data_for_pump": ENTRY_MOMO_ALLOW_NO_DATA_FOR_PUMP,
        },

        "scanners_legacy": {
            "raydium": ENABLE_RAYDIUM_SCANNER,
            "dexscreener": ENABLE_DEXSCREENER_SCANNER,
            "pumpfun": ENABLE_PUMPFUN_SCANNER,
            "arbitrage": ENABLE_ARBITRAGE,
            "min_liq_usd": SCANNER_MIN_LIQ_USD,
            "min_fdv_usd": SCANNER_MIN_FDV_USD,
            "min_fraud_score": SCANNER_MIN_FRAUD_SCORE,
            "alerts": SCANNER_ALERTS_ENABLED,
            "min_buys_5m": SCANNER_MIN_BUYS_5M,
            "min_buys_15m": SCANNER_MIN_BUYS_15M,
            "min_vol_5m": SCANNER_MIN_VOLUME_5M_USD,
            "min_vol_15m": SCANNER_MIN_VOLUME_15M_USD,
            "min_buy_ratio": SCANNER_MIN_BUY_RATIO,
            "require_buy_pressure": SCANNER_REQUIRE_BUY_PRESSURE,
            "min_chg_5m": SCANNER_MIN_PRICE_CHANGE_5M,
            "min_chg_15m": SCANNER_MIN_PRICE_CHANGE_15M,
            "max_age_min": SCANNER_MAX_AGE_MIN,
            "max_mc_usd": SCANNER_MAX_MC_USD,
            "min_holders": SCANNER_MIN_HOLDERS,
        },

        "pair_filters": {
            "pf_scan_min_liq_usd": PF_SCAN_MIN_LIQ_USD,
            "pf_scan_max_liq_usd": PF_SCAN_MAX_LIQ_USD,
            "pf_scan_min_mcap_usd": PF_SCAN_MIN_MCAP_USD,
            "pf_scan_max_mcap_usd": PF_SCAN_MAX_MCAP_USD,
            "pf_scan_max_token_age_min": PF_SCAN_MAX_TOKEN_AGE_MIN,
            "pf_scan_min_5m_vol_usd": PF_SCAN_MIN_5M_VOL_USD,
            "pf_scan_min_5m_buys": PF_SCAN_MIN_5M_BUYS,
            "pf_scan_min_buy_to_sell_ratio": PF_SCAN_MIN_BUY_TO_SELL_RATIO,
            "pf_scan_min_whale_size_sol": PF_SCAN_MIN_WHALE_SIZE_SOL,
            "pf_scan_min_whale_count_5m": PF_SCAN_MIN_WHALE_COUNT_5M,
            "pf_max_rug_risk_score": PF_MAX_RUG_RISK_SCORE,
            "pf_min_momentum_score": PF_MIN_MOMENTUM_SCORE,
            "pf_min_final_score": PF_MIN_FINAL_SCORE,
            "pf_risk_penalty_weight": PF_RISK_PENALTY_WEIGHT,
        },
    }
