# config.py
# Centralized config for the Solana Sniper + DayTrader bot (V3+++ / quant-grade)
#
# - Rich env parsing with sane defaults
# - Backward-compat aliases for older modules
# - Knobs for:
#     • core infra (RPC, Redis, Postgres, Notion, alerts)
#     • scanners (Raydium, Pump.fun, Dexscreener)
#     • Jupiter/router / swap settings
#     • risk manager + daily drawdown / kill-switch
#     • AI stack (entry, rug/fraud, sell, buy scoring, self-learning)
#     • PairFilter thresholds (PF_*)
#     • regime detection
#     • logging / diagnostics / backtests
#
# This file should stay *dumb*: just parse env → constants. No heavy logic.

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


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


def _get_list(
    name: str,
    default: Optional[List[str]] = None,
    sep: str = ",",
) -> List[str]:
    raw = os.getenv(name)
    if not raw:
        return list(default or [])
    return [s.strip() for s in raw.split(sep) if s.strip()]


def _get_json(name: str, default: Any) -> Any:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def _get_prob(name: str, default: float) -> float:
    """
    Accept [0..1] or percent [0..100]. If value > 1 we assume it is percent
    and divide by 100.
    """
    v = _get_float(name, default)
    return v / 100.0 if v > 1 else v


# ---------------------------------------------------------------------------
# App / logging / environment
# ---------------------------------------------------------------------------

APP_NAME = os.getenv("APP_NAME", "solana-sniper-bot-pro")
ENV = os.getenv("ENV", "dev").lower()
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
STRICT_CONFIG = _get_bool("STRICT_CONFIG", False)

IS_PROD = ENV in {"prod", "production"}
ENABLE_METRICS = _get_bool("ENABLE_METRICS", True)
METRICS_PORT = _get_int("METRICS_PORT", 9108)


# ---------------------------------------------------------------------------
# RPC / chain
# ---------------------------------------------------------------------------

NETWORK = os.getenv("NETWORK", "").strip() or os.getenv("SOLANA_CLUSTER", "mainnet")

# Core RPC URL (backed by your QuickNode / etc)
RPC_URL = (
    os.getenv("RPC_URL")
    or os.getenv("SOLANA_RPC_URL")
    or os.getenv("SOLANA_RPC")
    or os.getenv("SOL_RPC_URL")
    or "https://api.mainnet-beta.solana.com"
)
RPC_URLS = _get_list("RPC_URLS", [RPC_URL])

WEBSOCKET_URL = (
    os.getenv("WEBSOCKET_URL")
    or os.getenv("RPC_WS_URL")
    or os.getenv("SOLANA_WS_URL")
    or ""
)

COMMITMENT = os.getenv("COMMITMENT", os.getenv("RPC_COMMITMENT", "confirmed"))

# Back-compat aliases
SOLANA_ENDPOINT = RPC_URL
SOLANA_RPC_URL = RPC_URL
SOL_RPC_URL = RPC_URL
TX_RPC_URL = os.getenv("TX_RPC_URL", RPC_URL)

# Helius (optional)
HELIUS_RPC_URL = os.getenv("HELIUS_RPC_URL", "")

# Jito (optional)
ENABLE_JITO = _get_bool("ENABLE_JITO", _get_bool("JITO_ENABLED", False))
JITO_BLOCK_ENGINE_URL = os.getenv("JITO_BLOCK_ENGINE_URL", "")
JITO_TIP_LAMPORTS = _get_int("JITO_TIP_LAMPORTS", 50_000)  # ~0.00005 SOL
JITO_FEE_MICROLAMPORTS = _get_int("JITO_FEE_MICROLAMPORTS", 0)


# ---------------------------------------------------------------------------
# Wallet(s)
# ---------------------------------------------------------------------------

# WALLET_PRIVATE_KEY should be a JSON array of 64 or 32 integers (preferred).
WALLET_PRIVATE_KEY = os.getenv("WALLET_PRIVATE_KEY", "")
WALLET_PUBKEY = os.getenv("WALLET_PUBKEY", os.getenv("WALLET_PUBLIC_KEY", ""))
SAVINGS_WALLET = os.getenv("SAVINGS_WALLET", "")

# Owner aliases (for modules that use OWNER / OWNER_B58)
OWNER = os.getenv("OWNER", WALLET_PUBKEY)
OWNER_B58 = os.getenv("OWNER_B58", OWNER)

# Minimum free SOL to keep as safety buffer
MIN_SOL_BALANCE = _get_float("MIN_SOL_BALANCE", 0.20)


# ---------------------------------------------------------------------------
# External APIs / scanners
# ---------------------------------------------------------------------------

COINGECKO_SOL_API = os.getenv(
    "COINGECKO_SOL_API",
    "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd",
)

# Raydium / Dexscreener / Pump.fun / Birdeye
RAYDIUM_API_URL = os.getenv("RAYDIUM_API_URL", "https://api.raydium.io/pairs")
RAYDIUM_SEARCH_QUERIES = _get_list("RAYDIUM_SEARCH_QUERIES", ["raydium", "solana"])

DEXSCREENER_API_URL = os.getenv(
    "DEXSCREENER_API_URL",
    "https://api.dexscreener.com/latest/dex/tokens",
)
DEXSCREENER_BASE_URL = os.getenv(
    "DEXSCREENER_BASE_URL",
    "https://api.dexscreener.com",
).rstrip("/")

PUMPFUN_API_URL = os.getenv("PUMPFUN_API_URL", "https://pump.fun/api/v1/tokens")
PUMPFUN_API_BASE = (
    os.getenv("PUMPFUN_API_BASE", os.getenv("PUMPFUN_API_BASES", "")).rstrip("/")
)
PUMPFUN_HTTP_TIMEOUT_S = _get_int("PUMPFUN_HTTP_TIMEOUT_S", 15)
PUMPFUN_HTTP_CONN_LIMIT = _get_int("PUMPFUN_HTTP_CONN_LIMIT", 30)
PUMPFUN_API_LIMIT = _get_int("PUMPFUN_API_LIMIT", 20)
PUMPFUN_DOM_LIMIT = _get_int("PUMPFUN_DOM_LIMIT", PUMPFUN_API_LIMIT)
PUMPFUN_SKIP_NEXT = _get_bool("PUMPFUN_SKIP_NEXT", False)
PUMPFUN_SKIP_HTMLNEXT = _get_bool("PUMPFUN_SKIP_HTMLNEXT", False)

BIRDEYE_API_URL = os.getenv("BIRDEYE_API_URL", "https://public-api.birdeye.so")
BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY", "")


# ---------------------------------------------------------------------------
# Jupiter v6 / router
# ---------------------------------------------------------------------------

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

# Slippage for Jupiter routes (fraction; 0.005 = 0.5%)
JUPITER_MAX_SLIPPAGE = _get_float("JUPITER_MAX_SLIPPAGE", 0.005)


# ---------------------------------------------------------------------------
# Credentials / keys
# ---------------------------------------------------------------------------

# Alerts
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK", "")

# Notion
NOTION_TOKEN = os.getenv("NOTION_TOKEN", os.getenv("NOTION_API_KEY", ""))
NOTION_DATABASE_ID = os.getenv(
    "NOTION_DATABASE_ID",
    os.getenv("NOTION_DB_TRADES", ""),
)
ENABLE_NOTION_LOGGING = _get_bool("ENABLE_NOTION_LOGGING", _get_bool("NOTION_ENABLED", True))

# Optional: Notion property names override (used by core_utils)
NOTION_TITLE_PROP = os.getenv("NOTION_TITLE_PROP", "Token")
NOTION_PROP_TOKEN = os.getenv("NOTION_PROP_TOKEN", "Mint")
NOTION_PROP_ENTRY = os.getenv("NOTION_PROP_ENTRY", "Entry Price")
NOTION_PROP_CURRENT = os.getenv("NOTION_PROP_CURRENT", "Current Price")
NOTION_PROP_PROFIT = os.getenv("NOTION_PROP_PROFIT", "Profit")
NOTION_PROP_REASON = os.getenv("NOTION_PROP_REASON", "Reason")
NOTION_PROP_TIME = os.getenv("NOTION_PROP_TIME", "Timestamp")

# OpenAI / AI models
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
    os.getenv("SQLITE_DB", os.path.join("data", "state.db")),
)

# Positions store backend
POSITIONS_BACKEND = os.getenv("POSITIONS_BACKEND", "json").lower()  # "json" | "sqlite" | "pg"
POSITIONS_JSON_PATH = os.getenv(
    "POSITIONS_JSON_PATH",
    os.path.join("data", "positions.json"),
)

# Postgres (support INTERNAL/EXTERNAL scheme + PG_ENABLED)
PG_ENABLED = _get_bool("PG_ENABLED", True)
PG_DSN = os.getenv("PG_DSN", "")
PG_DSN_INTERNAL = os.getenv("PG_DSN_INTERNAL", "")
PG_DSN_EXTERNAL = os.getenv("PG_DSN_EXTERNAL", "")

POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN",
    PG_DSN or PG_DSN_INTERNAL or PG_DSN_EXTERNAL,
)

# Extra DSNs for PnL / sniper / positions engines
SNIPER_DB_DSN = os.getenv("SNIPER_DB_DSN", POSTGRES_DSN)
PNL_DB_DSN = os.getenv("PNL_DB_DSN", POSTGRES_DSN)
PNL_DSN = os.getenv("PNL_DSN", POSTGRES_DSN)
POSITIONS_PG_DSN = os.getenv("POSITIONS_PG_DSN", POSTGRES_DSN)
POSITIONS_PG_ENABLED = _get_bool("POSITIONS_PG_ENABLED", _get_bool("POSITIONS_PG_ENABLED", True))


# ---------------------------------------------------------------------------
# Trading / execution (shared sniper knobs)
# ---------------------------------------------------------------------------

# High-level mode for the sniper
AUTO_BUY_ENABLED = _get_bool("AUTO_BUY_ENABLED", _get_bool("BUY_ENGINE_ENABLED", True))
AUTO_SELL_ENABLED = _get_bool(
    "AUTO_SELL_ENABLED",
    _get_bool("ENABLE_AUTO_SELL", _get_bool("EXIT_ENGINE_ENABLED", True)),
)

# Global safety toggles
PAUSE_TRADING = _get_bool("PAUSE_TRADING", False)
DRY_RUN = _get_bool("DRY_RUN", _get_bool("DRYRUN", False))
DRYRUN = DRY_RUN  # back-compat

TRANSACTION_TIMEOUT = _get_int("TRANSACTION_TIMEOUT", 20)
MAX_RETRIES = _get_int("MAX_RETRIES", 3)
EXEC_DELAY = _get_int("SNIPER_EXEC_DELAY", _get_int("EXEC_DELAY", 0))

# Dynamic sizing / risk (legacy baseline)
MAX_RISK_PER_TRADE = _get_float("MAX_RISK_PER_TRADE", 0.05)  # 5% of SOL stack
MIN_VOLATILITY = _get_float("MIN_VOLATILITY", 0.02)
PARTIAL_SELL_CHUNKS = _get_int("PARTIAL_SELL_CHUNKS", 4)

# Ticket sizing (used by risk_manager + sniper fallbacks)
TICKET_PCT_EQUITY = _get_float("TICKET_PCT_EQUITY", 0.04)  # 4% of equity
TICKET_USD_CAP = _get_float("TICKET_USD_CAP", 300.0)

# Minimum SOL per initial buy used by some scanners/strategies
ENTRY_MIN_INITIAL_BUY_SOL = _get_float("ENTRY_MIN_INITIAL_BUY_SOL", 0.0025)

# Targets / SL / hold (generic baseline)
TARGET_PROFIT_PERCENT = _get_float("TARGET_PROFIT_PERCENT", 200.0)  # 200% = 2x
STOP_LOSS_PERCENT = _get_float("STOP_LOSS_PERCENT", 20.0)
TRAILING_STOP_ENABLED = _get_bool("TRAILING_STOP_ENABLED", True)
MAX_HOLD_TIME_MINUTES = _get_int("MAX_HOLD_TIME_MINUTES", 360)
HOLD_IF_SCORE_ABOVE = _get_prob("HOLD_IF_SCORE_ABOVE", 0.85)  # 0..1

# Take-profit ladder used by some sniper flows
TAKE_PROFIT1_PCT = _get_float("TAKE_PROFIT1_PCT", 30.0)
TAKE_PROFIT2_PCT = _get_float("TAKE_PROFIT2_PCT", 60.0)
TAKE_PROFIT3_PCT = _get_float("TAKE_PROFIT3_PCT", 120.0)

# Reinvestor
REINVEST_PERCENT = _get_float("REINVEST_PERCENT", 80.0)
REINVESTMENT_PERCENT_CAP = _get_float("REINVESTMENT_PERCENT_CAP", 50.0)
REINVESTMENT_VOLUME_MIN = _get_float("REINVESTMENT_VOLUME_MIN", 5_000.0)
REINVESTMENT_MIN_PROFIT_SOL = _get_float("REINVESTMENT_MIN_PROFIT_SOL", 0.01)
REINVESTMENT_HONEYPOT_CHECK = _get_bool("REINVESTMENT_HONEYPOT_CHECK", True)


# ---------------------------------------------------------------------------
# Quant-grade risk manager (used by utils_pkg.risk_manager)
# ---------------------------------------------------------------------------

RISK_PROFILE = os.getenv("RISK_PROFILE", "balanced").lower()
RISK_MODE_OVERRIDE = os.getenv("RISK_MODE_OVERRIDE", "").strip() or None

# Per-trade & per-coin equity caps
RISK_MAX_EQUITY_PCT_PER_TRADE = _get_float(
    "RISK_MAX_EQUITY_PCT_PER_TRADE",
    TICKET_PCT_EQUITY,
)
RISK_MAX_EQUITY_PCT_PER_COIN = _get_float(
    "RISK_MAX_EQUITY_PCT_PER_COIN",
    0.10,  # 10% default cap per mint
)

# Global concurrency / positions
RISK_MAX_OPEN_POSITIONS = _get_int("RISK_MAX_OPEN_POSITIONS", 6)
RISK_MAX_PARALLEL_BUYS = _get_int("RISK_MAX_PARALLEL_BUYS", 3)

# Daily limits / kill-switch
RISK_MAX_DAILY_DRAWDOWN_PCT = _get_float(
    "RISK_MAX_DAILY_DRAWDOWN_PCT",
    _get_float("PF_DAILY_MAX_DRAWDOWN_PCT", 18.0),
)
RISK_MAX_DAILY_LOSS_SOL = _get_float("RISK_MAX_DAILY_LOSS_SOL", 9999.0)
RISK_CIRCUIT_BREAKER_MINUTES = _get_int("RISK_CIRCUIT_BREAKER_MINUTES", 45)

STOP_AFTER_DAILY_PROFIT_PCT = _get_float(
    "STOP_AFTER_DAILY_PROFIT_PCT",
    _get_float("PF_STOP_AFTER_PROFIT_PCT", 25.0),
)
DAILY_SPEND_CAP_PCT = _get_float("DAILY_SPEND_CAP_PCT", 35.0)

RISK_HARD_KILL_ON_DRAWNDOWN = _get_bool("RISK_HARD_KILL_ON_DRAWNDOWN", True)
RISK_ALLOW_REENTRY_AFTER_BREAK = _get_bool("RISK_ALLOW_REENTRY_AFTER_BREAK", True)

# Back-compat for modules that import these directly
MAX_CONCURRENT_POS = _get_int("MAX_CONCURRENT_POS", RISK_MAX_OPEN_POSITIONS)


# ---------------------------------------------------------------------------
# Exit engine / auto-sell (used by utils_pkg.exit_engine & auto_sell)
# ---------------------------------------------------------------------------

EXIT_STOP_LOSS_PCT = _get_float("EXIT_STOP_LOSS_PCT", STOP_LOSS_PERCENT)
EXIT_TP1_PCT = _get_float("EXIT_TP1_PCT", TAKE_PROFIT1_PCT)
EXIT_TP2_PCT = _get_float("EXIT_TP2_PCT", TAKE_PROFIT2_PCT)
EXIT_TP3_PCT = _get_float("EXIT_TP3_PCT", TAKE_PROFIT3_PCT)

EXIT_TSL_ENABLED = _get_bool("EXIT_TSL_ENABLED", True)
EXIT_TSL_ARM_PCT = _get_float("EXIT_TSL_ARM_PCT", 12.0)
EXIT_TSL_STEP_PCT = _get_float("EXIT_TSL_STEP_PCT", 4.5)

# Legacy PF_* aliases for trailing stop / SL
PF_TSL_ARM_PCT = _get_float("PF_TSL_ARM_PCT", EXIT_TSL_ARM_PCT)
PF_TSL_PCT = _get_float("PF_TSL_PCT", EXIT_TSL_STEP_PCT)
PF_SL_PCT = _get_float("PF_SL_PCT", EXIT_STOP_LOSS_PCT)

EXIT_STAGNANT_MAX_MIN = _get_int("EXIT_STAGNANT_MAX_MIN", 10)
EXIT_STAGNANT_MIN_MOVE_PCT = _get_float("EXIT_STAGNANT_MIN_MOVE_PCT", 5.0)
EXIT_DEAD_COIN_MAX_MIN = _get_int("EXIT_DEAD_COIN_MAX_MIN", 45)
EXIT_DEAD_MIN_LIQ_USD = _get_float("EXIT_DEAD_MIN_LIQ_USD", 400.0)
EXIT_FORCE_AFTER_X_MULT = _get_float("EXIT_FORCE_AFTER_X_MULT", 8.0)

EXIT_LOCK_PROFIT_AFTER_MULT = _get_float("EXIT_LOCK_PROFIT_AFTER_MULT", 3.0)
EXIT_LOCK_PROFIT_TSL_STEP_PCT = _get_float("EXIT_LOCK_PROFIT_TSL_STEP_PCT", 3.5)


# ---------------------------------------------------------------------------
# Entry momentum guard (used by Strategy / ai_entry_logic pre-gate)
# ---------------------------------------------------------------------------

ENTRY_MOMENTUM_ENABLED = _get_bool(
    "ENTRY_MOMENTUM_ENABLED",
    _get_bool("AI_MOMO_FILTER", True),
)

ENTRY_MOMO_WINDOW_SEC = _get_int(
    "ENTRY_MOMO_WINDOW_SEC",
    _get_int("MOMO_WINDOW_SEC", 25),
)

ENTRY_MOMO_MIN_TRADES = _get_int(
    "ENTRY_MOMO_MIN_TRADES",
    _get_int("MOMO_MIN_TRADES", 2),
)
ENTRY_MOMO_MIN_UP_PCT = _get_float(
    "ENTRY_MOMO_MIN_UP_PCT",
    _get_float("MOMO_MIN_UP_PCT", 2.0),
)

ENTRY_MOMO_ALLOW_NO_DATA_FOR_PUMP = _get_bool(
    "ENTRY_MOMO_ALLOW_NO_DATA_FOR_PUMP",
    _get_bool("MOMO_ALLOW_NO_DATA_FOR_PUMP", True),
)


# ---------------------------------------------------------------------------
# Sniper loop / scanners tuning (matches Strategy / scripts.main)
# ---------------------------------------------------------------------------

SCAN_INTERVAL = _get_int("SCAN_INTERVAL", 60)
SCANNER_PERIOD_SEC = _get_int("SCANNER_PERIOD_SEC", 4)

BUY_COOLDOWN_SEC = _get_int("BUY_COOLDOWN_SEC", 3)
MAX_CONCURRENT_BUYS = _get_int("MAX_CONCURRENT_BUYS", 2)
PF_MAX_CONCURRENT = _get_int("PF_MAX_CONCURRENT", MAX_CONCURRENT_BUYS)

PREWARM_TOP_N = _get_int("PREWARM_TOP_N", 5)

SCANNER_MIN_LIQ_USD = _get_float("SCANNER_MIN_LIQ_USD", 150.0)
MIN_LIQ_USD = _get_float("MIN_LIQ_USD", SCANNER_MIN_LIQ_USD)
LIQ_MIN_USD = MIN_LIQ_USD

MIN_POOL_AGE_MIN = _get_int("MIN_POOL_AGE_MIN", 1)

NOROUTE_COOLDOWN_SEC = _get_int("NOROUTE_COOLDOWN_SEC", 600)

BLOCKLIST_MINTS = _get_list("BLOCKLIST_MINTS", [])
ALLOWLIST_MINTS = _get_list("ALLOWLIST_MINTS", [])

STRATEGY_PRESET = os.getenv("STRATEGY_PRESET", "max-pnl").lower()

TELEGRAM_HEARTBEATS = _get_bool("TELEGRAM_HEARTBEATS", True)
HEARTBEAT_EVERY_SEC = _get_int("HEARTBEAT_EVERY_SEC", 300)
HEARTBEAT_ONLY_ON_CHANGE = _get_bool("HEARTBEAT_ONLY_ON_CHANGE", True)
TELEGRAM_NOTIFY_ROUTE_BLOCKS = _get_bool(
    "TELEGRAM_NOTIFY_ROUTE_BLOCKS",
    _get_bool("TELEGRAM_NOTIFY_ROUTE_BLOCKS", True),
)

PNL_TELEGRAM_HEARTBEAT = _get_bool("PNL_TELEGRAM_HEARTBEAT", True)

# Moonshot budget (tiny new/low-liq plays)
MOONSHOT_ENABLED = _get_bool("MOONSHOT_ENABLED", True)
MOONSHOT_DAILY_USD = _get_float("MOONSHOT_DAILY_USD", 100.0)
MOONSHOT_MAX_TRADE_USD = _get_float("MOONSHOT_MAX_TRADE_USD", 25.0)
MOONSHOT_AGE_CUTOFF_MIN = _get_int("MOONSHOT_AGE_CUTOFF_MIN", 30)
MOONSHOT_LIQ_CUTOFF_USD = _get_float("MOONSHOT_LIQ_CUTOFF_USD", 20_000.0)
MOONSHOT_IMPACT_CUTOFF_BPS = _get_float("MOONSHOT_IMPACT_CUTOFF_BPS", 150.0)


# ---------------------------------------------------------------------------
# Scanners / arbitrage (legacy toggles still used)
# ---------------------------------------------------------------------------

ENABLE_RAYDIUM_SCANNER = _get_bool("ENABLE_RAYDIUM_SCANNER", True)
ENABLE_DEXSCREENER_SCANNER = _get_bool("ENABLE_DEXSCREENER_SCANNER", True)
ENABLE_PUMPFUN_SCANNER = _get_bool("ENABLE_PUMPFUN_SCANNER", True)
ENABLE_ARBITRAGE = _get_bool("ENABLE_ARBITRAGE", False)
DEXES: List[str] = _get_list("DEXES", ["jupiter", "raydium", "orca"])

SCANNER_MIN_FDV_USD = _get_float("SCANNER_MIN_FDV_USD", 0.0)
SCANNER_MIN_FRAUD_SCORE = _get_float("SCANNER_MIN_FRAUD_SCORE", 0.0)
SCANNER_ALERTS_ENABLED = _get_bool("SCANNER_ALERTS_ENABLED", True)

SCANNER_MIN_BUYS_5M = _get_int("SCANNER_MIN_BUYS_5M", 0)
SCANNER_MIN_BUYS_15M = _get_int("SCANNER_MIN_BUYS_15M", 0)
SCANNER_MIN_VOLUME_5M_USD = _get_float("SCANNER_MIN_VOLUME_5M_USD", 20.0)
SCANNER_MIN_VOLUME_15M_USD = _get_float("SCANNER_MIN_VOLUME_15M_USD", 60.0)

SCANNER_REQUIRE_BUY_PRESSURE = _get_bool("SCANNER_REQUIRE_BUY_PRESSURE", False)
SCANNER_MIN_BUY_RATIO = _get_float("SCANNER_MIN_BUY_RATIO", 0.0)

SCANNER_MIN_PRICE_CHANGE_5M = _get_float("SCANNER_MIN_PRICE_CHANGE_5M", 0.0)
SCANNER_MIN_PRICE_CHANGE_15M = _get_float("SCANNER_MIN_PRICE_CHANGE_15M", 0.0)

SCANNER_MAX_AGE_MIN = _get_int("SCANNER_MAX_AGE_MIN", 720)
SCANNER_MAX_MC_USD = _get_float("SCANNER_MAX_MC_USD", 50_000_000.0)
SCANNER_MIN_HOLDERS = _get_int("SCANNER_MIN_HOLDERS", 0)

ALERT_COOLDOWN_SECONDS = _get_int("ALERT_COOLDOWN_SECONDS", 900)
POSITION_MONITOR_INTERVAL = _get_int("POSITION_MONITOR_INTERVAL", 20)
REINVESTMENT_CHECK_INTERVAL = _get_int("REINVESTMENT_CHECK_INTERVAL", 60)


# ---------------------------------------------------------------------------
# PairFilter-centric thresholds (used by utils_pkg.pair_filters)
# ---------------------------------------------------------------------------

PF_SCAN_MIN_LIQ_USD = _get_float("PF_SCAN_MIN_LIQ_USD", 400.0)
PF_SCAN_MAX_LIQ_USD = _get_float("PF_SCAN_MAX_LIQ_USD", 300_000.0)

PF_SCAN_MIN_MCAP_USD = _get_float("PF_SCAN_MIN_MCAP_USD", 800.0)
PF_SCAN_MAX_MCAP_USD = _get_float("PF_SCAN_MAX_MCAP_USD", 1_500_000.0)

PF_SCAN_MAX_TOKEN_AGE_MIN = _get_float("PF_SCAN_MAX_TOKEN_AGE_MIN", 180.0)

PF_SCAN_MIN_5M_VOL_USD = _get_float("PF_SCAN_MIN_5M_VOL_USD", 50.0)
PF_SCAN_MIN_5M_BUYS = _get_int("PF_SCAN_MIN_5M_BUYS", 1)
PF_SCAN_MIN_BUY_TO_SELL_RATIO = _get_float("PF_SCAN_MIN_BUY_TO_SELL_RATIO", 1.02)

PF_SCAN_MIN_WHALE_SIZE_SOL = _get_float("PF_SCAN_MIN_WHALE_SIZE_SOL", 1.5)
PF_SCAN_MIN_WHALE_COUNT_5M = _get_int("PF_SCAN_MIN_WHALE_COUNT_5M", 0)

PF_MAX_RUG_RISK_SCORE = _get_float("PF_MAX_RUG_RISK_SCORE", 0.80)

PF_MIN_MOMENTUM_SCORE = _get_float("PF_MIN_MOMENTUM_SCORE", 0.14)
PF_MIN_FINAL_SCORE = _get_float("PF_MIN_FINAL_SCORE", 0.10)

PF_RISK_PENALTY_WEIGHT = _get_float("PF_RISK_PENALTY_WEIGHT", 0.6)


# ---------------------------------------------------------------------------
# AI strategy / entry / sell / buy scoring (used by ai_* modules)
# ---------------------------------------------------------------------------

AI_ENABLED = _get_bool("AI_ENABLED", True)

# ---- AI entry gate / score thresholds used by Strategy + ai_buy_scoring ----

# Master switch for AI gate
AI_ENTRY_ENABLED = _get_bool("AI_ENTRY_ENABLED", _get_bool("AIGATE_AI_ENABLED", True))

# Base model for entry reasoning (used by ai_entry_logic)
AI_ENTRY_MODEL = os.getenv("AI_ENTRY_MODEL", os.getenv("ENTRY_MODEL", FINE_TUNED_MODEL or OPENAI_MODEL))

# New explicit entry score knobs (aligned with your env)
AI_ENTRY_SCORE_MIN = _get_prob(
    "AI_ENTRY_SCORE_MIN",
    _get_prob("ENTRY_MIN_SCORE", 0.60),
)  # soft gate (target)
AI_ENTRY_SCORE_HARD_MIN = _get_prob(
    "AI_ENTRY_SCORE_HARD_MIN",
    0.50,
)
AI_ENTRY_RISK_ADJ_MIN = _get_prob(
    "AI_ENTRY_RISK_ADJ_MIN",
    0.50,
)

# Older "global" gate values used directly by ai_buy_scoring
AI_ENTRY_THRESHOLD = _get_prob("AI_ENTRY_THRESHOLD", AI_ENTRY_SCORE_MIN)
AI_SCORE_GATE = _get_prob("AI_SCORE_GATE", AI_ENTRY_THRESHOLD)
AI_SCORE_SOFT_FLOOR = _get_prob("AI_SCORE_SOFT_FLOOR", AI_ENTRY_SCORE_HARD_MIN)
AI_SCORE_HARD_FLOOR = _get_prob("AI_SCORE_HARD_FLOOR", 0.38)

AI_ENTRY_MAX_CONCURRENCY = _get_int("AI_ENTRY_MAX_CONCURRENCY", 8)

# Pre-score guard (cheap heuristic gate before full AI)
AI_PRE_SCORE_ENABLED = _get_bool("AI_PRE_SCORE_ENABLED", True)
AI_PRE_SCORE_MIN = _get_prob("AI_PRE_SCORE_MIN", 0.10)

# Risk capacity gating
ENTRY_REQUIRE_RISK_CAPACITY = _get_bool("ENTRY_REQUIRE_RISK_CAPACITY", True)
ENTRY_RISK_CAPACITY_MIN = _get_prob("ENTRY_RISK_CAPACITY_MIN", 0.30)

# Rug / fraud AI (used by scanners + router guard)
AI_RUG_ENABLED = _get_bool("AI_RUG_ENABLED", True)
AI_RUG_MODEL = os.getenv("AI_RUG_MODEL", FINE_TUNED_MODEL or OPENAI_MODEL)
AI_RUG_MAX_SCORE = _get_float("AI_RUG_MAX_SCORE", PF_MAX_RUG_RISK_SCORE)
AI_RUG_MIN_CONFIDENCE = _get_prob("AI_RUG_MIN_CONFIDENCE", 0.55)

# Additional gating knobs used by ai_buy_scoring
AIGATE_MAX_PRICE_IMPACT_BPS = _get_int("AIGATE_MAX_PRICE_IMPACT_BPS", 550)
AI_BAD_THRESHOLD = _get_prob("AI_BAD_THRESHOLD", 0.35)
AI_GOOD_THRESHOLD = _get_prob("AI_GOOD_THRESHOLD", 0.70)

AI_WHALE_ECHO = _get_bool("AI_WHALE_ECHO", True)
AI_WHALE_ECHO_MIN_SIZE_USD = _get_float("AI_WHALE_ECHO_MIN_SIZE_USD", 300.0)

# Sell / exit AI
AI_ADAPTIVE_SELL = _get_bool("AI_ADAPTIVE_SELL", True)
AI_SELL_ENABLED = _get_bool("AI_SELL_ENABLED", True)
AI_SELL_MODEL = os.getenv("AI_SELL_MODEL", FINE_TUNED_MODEL or OPENAI_MODEL)
AI_SELL_HOLD_THRESHOLD = _get_prob("AI_SELL_HOLD_THRESHOLD", 0.60)
AI_SELL_TRIM_THRESHOLD = _get_prob("AI_SELL_TRIM_THRESHOLD", 0.72)
AI_SELL_EXIT_THRESHOLD = _get_prob("AI_SELL_EXIT_THRESHOLD", 0.84)

# Self-learning strategy brain
STRATEGY_REDIS_PREFIX = os.getenv("STRATEGY_REDIS_PREFIX", "strategy:v1")
STRATEGY_UPDATE_INTERVAL = _get_int("STRATEGY_UPDATE_INTERVAL", 1800)

AI_FEATURE_VERSION = os.getenv("AI_FEATURE_VERSION", "v3+++")
AI_BUY_FEATURE_VERSION = os.getenv("AI_BUY_FEATURE_VERSION", AI_FEATURE_VERSION)


# ---------------------------------------------------------------------------
# Regime detection (used by utils_pkg.regime.regime_state)
# ---------------------------------------------------------------------------

REGIME_ENABLED = _get_bool("REGIME_ENABLED", True)
REGIME_UPDATE_INTERVAL_SEC = _get_int("REGIME_UPDATE_INTERVAL_SEC", 60)

REGIME_SOL_MIN_24H_VOL_USD = _get_float("REGIME_SOL_MIN_24H_VOL_USD", 200_000_000.0)
REGIME_MEME_MIN_24H_VOL_USD = _get_float("REGIME_MEME_MIN_24H_VOL_USD", 50_000_000.0)
REGIME_MAX_ACCEPTABLE_RUG_RATE = _get_float("REGIME_MAX_ACCEPTABLE_RUG_RATE", 0.25)


# ---------------------------------------------------------------------------
# AI data logging / training (used by utils_pkg.data_logger / ai_db)
# ---------------------------------------------------------------------------

LOG_ENTRY_CANDIDATES = _get_bool("LOG_ENTRY_CANDIDATES", True)
LOG_TRADES = _get_bool("LOG_TRADES", True)
LOG_REGIME_SNAPSHOTS = _get_bool("LOG_REGIME_SNAPSHOTS", True)

TRAIN_LABEL_WINDOW_MIN = _get_int("TRAIN_LABEL_WINDOW_MIN", 60)
TRAIN_MIN_HOLD_MIN = _get_int("TRAIN_MIN_HOLD_MIN", 10)
TRAIN_MAX_HOLD_MIN = _get_int("TRAIN_MAX_HOLD_MIN", 720)


# ---------------------------------------------------------------------------
# Whale watch & smart wallets (used by smart_wallets / scanners)
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

ENABLE_SOCIAL_SCRAPING = _get_bool("ENABLE_SOCIAL_SCRAPING", False)
X_BEARER_TOKEN = os.getenv("X_BEARER_TOKEN", "")
TELEGRAM_SCRAPE_BOT = os.getenv("TELEGRAM_SCRAPE_BOT", "")
DISCORD_SEARCH_CHANNEL_ID = os.getenv("DISCORD_SEARCH_CHANNEL_ID", "")


# ---------------------------------------------------------------------------
# Daytrader (pro / optional)
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
# Backtest / diagnostics
# ---------------------------------------------------------------------------

BACKTEST_ENABLED = _get_bool("BACKTEST_ENABLED", False)
BACKTEST_SOURCE = os.getenv("BACKTEST_SOURCE", "pg")  # pg | csv | json
BACKTEST_MAX_WORKERS = _get_int("BACKTEST_MAX_WORKERS", 4)

DIAG_BT_DAYS_BACK = _get_int("DIAG_BT_DAYS_BACK", 5)
DIAG_BT_LIMIT_CANDIDATES = _get_int("DIAG_BT_LIMIT_CANDIDATES", 200)

BT_DAYS_BACK = os.getenv("BT_DAYS_BACK", "")
BT_LIMIT_CANDIDATES = os.getenv("BT_LIMIT_CANDIDATES", "")


# ---------------------------------------------------------------------------
# Compat exports / aliases
# ---------------------------------------------------------------------------

RAYDIUM_PAIRS_API = RAYDIUM_API_URL
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
        "Missing env vars (running in non-strict mode): %s",
        ", ".join(_missing),
    )


# ---------------------------------------------------------------------------
# Summary helper (for quick debugging / sanity checks)
# ---------------------------------------------------------------------------


def summarize() -> Dict[str, Any]:
    """
    Lightweight summary of the main knobs we care about for debugging
    and sanity checks.  This is only for introspection, not core logic.
    """
    return {
        "sniper": {
            "pause_trading": PAUSE_TRADING,
            "dry_run": DRYRUN,
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
            "entry_momo_enabled": ENTRY_MOMENTUM_ENABLED,
            "entry_momo_window_sec": ENTRY_MOMO_WINDOW_SEC,
            "entry_momo_min_trades": ENTRY_MOMO_MIN_TRADES,
            "entry_momo_min_up_pct": ENTRY_MOMO_MIN_UP_PCT,
            "entry_momo_allow_no_data_for_pump": ENTRY_MOMO_ALLOW_NO_DATA_FOR_PUMP,
        },
        "risk_manager": {
            "risk_profile": RISK_PROFILE,
            "risk_mode_override": RISK_MODE_OVERRIDE,
            "max_equity_pct_per_trade": RISK_MAX_EQUITY_PCT_PER_TRADE,
            "max_equity_pct_per_coin": RISK_MAX_EQUITY_PCT_PER_COIN,
            "max_open_positions": RISK_MAX_OPEN_POSITIONS,
            "max_parallel_buys": RISK_MAX_PARALLEL_BUYS,
            "max_daily_drawdown_pct": RISK_MAX_DAILY_DRAWDOWN_PCT,
            "max_daily_loss_sol": RISK_MAX_DAILY_LOSS_SOL,
            "circuit_breaker_minutes": RISK_CIRCUIT_BREAKER_MINUTES,
            "hard_kill_on_dd": RISK_HARD_KILL_ON_DRAWNDOWN,
            "stop_after_daily_profit_pct": STOP_AFTER_DAILY_PROFIT_PCT,
            "daily_spend_cap_pct": DAILY_SPEND_CAP_PCT,
        },
        "exit_engine": {
            "stop_loss_pct": EXIT_STOP_LOSS_PCT,
            "tp1_pct": EXIT_TP1_PCT,
            "tp2_pct": EXIT_TP2_PCT,
            "tp3_pct": EXIT_TP3_PCT,
            "tsl_enabled": EXIT_TSL_ENABLED,
            "tsl_arm_pct": EXIT_TSL_ARM_PCT,
            "tsl_step_pct": EXIT_TSL_STEP_PCT,
            "stagnant_max_min": EXIT_STAGNANT_MAX_MIN,
            "stagnant_min_move_pct": EXIT_STAGNANT_MIN_MOVE_PCT,
            "dead_coin_max_min": EXIT_DEAD_COIN_MAX_MIN,
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
        "ai": {
            "enabled": AI_ENABLED,
            "entry_enabled": AI_ENTRY_ENABLED,
            "entry_model": AI_ENTRY_MODEL,
            "entry_score_min": AI_ENTRY_SCORE_MIN,
            "entry_score_hard_min": AI_ENTRY_SCORE_HARD_MIN,
            "entry_risk_adj_min": AI_ENTRY_RISK_ADJ_MIN,
            "entry_threshold": AI_ENTRY_THRESHOLD,
            "score_gate": AI_SCORE_GATE,
            "score_soft_floor": AI_SCORE_SOFT_FLOOR,
            "score_hard_floor": AI_SCORE_HARD_FLOOR,
            "entry_require_risk_capacity": ENTRY_REQUIRE_RISK_CAPACITY,
            "entry_risk_capacity_min": ENTRY_RISK_CAPACITY_MIN,
            "rug_enabled": AI_RUG_ENABLED,
            "rug_max_score": AI_RUG_MAX_SCORE,
            "adaptive_sell": AI_ADAPTIVE_SELL,
            "sell_enabled": AI_SELL_ENABLED,
            "feature_version": AI_FEATURE_VERSION,
        },
        "regime": {
            "enabled": REGIME_ENABLED,
            "update_interval_sec": REGIME_UPDATE_INTERVAL_SEC,
            "sol_min_24h_vol_usd": REGIME_SOL_MIN_24H_VOL_USD,
            "meme_min_24h_vol_usd": REGIME_MEME_MIN_24H_VOL_USD,
            "max_acceptable_rug_rate": REGIME_MAX_ACCEPTABLE_RUG_RATE,
        },
    }
