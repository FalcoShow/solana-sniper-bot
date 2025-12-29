# config.py
# Centralized config for the Solana Sniper + DayTrader bot (V3+++ / quant-grade) — LOCK-IN (production-ready)
#
# RULES (NON-NEGOTIABLE):
# - This file stays *dumb*: parse env → constants. No network calls. No side effects beyond safe warnings.
# - All env vars are parsed once, typed, bounded, and safe-defaulted.
# - Backward-compat aliases MUST remain intact so older modules don’t break.
# - Any secret printed/logged must be REDACTED.
#
# Tip: you can safely log `config.summarize()` at boot.

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple


# =============================================================================
# Meta
# =============================================================================

CONFIG_VERSION = "v3+++_lockin_2025-12-22"
CONFIG_LOADED_AT_UNIX = int(time.time())


# =============================================================================
# Parsing primitives (robust, permissive input; strict output)
# =============================================================================

_LOG = logging.getLogger(__name__)

_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}

_ALLOWED_ENVS = {"dev", "development", "test", "staging", "prod", "production"}
_ALLOWED_QUEUE_DROP = {"drop_newest", "drop_oldest", "block"}
_ALLOWED_POS_BACKENDS = {"json", "sqlite", "pg"}
_ALLOWED_BACKTEST_SOURCE = {"pg", "csv", "json"}
_ALLOWED_COMMITMENT = {"processed", "confirmed", "finalized"}


def _raw(name: str) -> Optional[str]:
    v = os.getenv(name)
    if v is None:
        return None
    s = str(v).strip()
    return s if s != "" else None


def _pick_first(names: Sequence[str], default: Optional[str] = None) -> Optional[str]:
    for n in names:
        v = _raw(n)
        if v is not None:
            return v
    return default


def _coerce_num_str(v: str) -> str:
    # allow "1_000", "  3.0  ", etc.
    return v.strip().replace("_", "")


def _get_str(name: str, default: str = "") -> str:
    v = _raw(name)
    return v if v is not None else default


def _get_bool(name: str, default: bool = False) -> bool:
    v = _raw(name)
    if v is None:
        return bool(default)
    s = v.lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return bool(default)


def _get_int(name: str, default: int) -> int:
    v = _raw(name)
    if v is None:
        return int(default)
    try:
        return int(float(_coerce_num_str(v)))
    except Exception:
        return int(default)


def _get_float(name: str, default: float) -> float:
    v = _raw(name)
    if v is None:
        return float(default)
    try:
        return float(_coerce_num_str(v))
    except Exception:
        return float(default)


def _clamp_f(x: float, lo: float, hi: float) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _clamp_i(x: int, lo: int, hi: int) -> int:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _get_prob(name: str, default: float) -> float:
    """
    Accepts:
      - fraction: 0.62
      - percent: 62 or "62%"
    Returns fraction clamped to [0..1].
    """
    v = _raw(name)
    if v is None:
        return _clamp_f(float(default), 0.0, 1.0)

    s = v.strip().lower()
    try:
        if s.endswith("%"):
            x = float(_coerce_num_str(s[:-1])) / 100.0
        else:
            x = float(_coerce_num_str(s))
            if x > 1.0:
                x = x / 100.0
        return _clamp_f(x, 0.0, 1.0)
    except Exception:
        return _clamp_f(float(default), 0.0, 1.0)


def _get_pct(name: str, default_pct: float, lo: float = 0.0, hi: float = 10_000.0) -> float:
    """
    Percent-value helper:
      - accepts "8", "8%", "0.08" (treated as 0.08% ONLY if user literally sets 0.08)
      - we DO NOT auto-convert fractions to percent here (use _get_prob for fractions).
    """
    v = _raw(name)
    if v is None:
        return _clamp_f(float(default_pct), lo, hi)
    s = v.strip().lower()
    try:
        if s.endswith("%"):
            x = float(_coerce_num_str(s[:-1]))
        else:
            x = float(_coerce_num_str(s))
        return _clamp_f(x, lo, hi)
    except Exception:
        return _clamp_f(float(default_pct), lo, hi)


def _get_bps(name: str, default_bps: int) -> int:
    """
    Accepts:
      - basis points: 50
      - percent: "0.5%" -> 50 bps
    """
    v = _raw(name)
    if v is None:
        return int(default_bps)
    s = v.strip().lower()
    try:
        if s.endswith("%"):
            pct = float(_coerce_num_str(s[:-1]))
            return int(round(pct * 100.0))
        return int(float(_coerce_num_str(s)))
    except Exception:
        return int(default_bps)


def _get_list(name: str, default: Optional[Sequence[str]] = None, sep: str = ",") -> List[str]:
    """
    Accepts:
      - CSV: "a,b,c"
      - JSON list: ["a","b","c"]
    Strips entries, removes empties, preserves order, de-dupes.
    """
    v = _raw(name)
    items: List[str]
    if v is None:
        items = list(default or [])
    else:
        s = v.strip()
        if s.startswith("[") and s.endswith("]"):
            try:
                arr = json.loads(s)
                if isinstance(arr, list):
                    items = [str(x).strip() for x in arr]
                else:
                    items = [p.strip() for p in s.split(sep)]
            except Exception:
                items = [p.strip() for p in s.split(sep)]
        else:
            items = [p.strip() for p in s.split(sep)]

    out: List[str] = []
    seen = set()
    for it in items:
        if not it:
            continue
        if it in seen:
            continue
        seen.add(it)
        out.append(it)
    return out


def _get_json(name: str, default: Any) -> Any:
    v = _raw(name)
    if v is None:
        return default
    try:
        return json.loads(v)
    except Exception:
        return default


def _mask(s: str, keep_head: int = 4, keep_tail: int = 0) -> str:
    if not s:
        return ""
    s = str(s)
    if len(s) <= keep_head + keep_tail:
        return "*" * len(s)
    head = s[:keep_head] if keep_head > 0 else ""
    tail = s[-keep_tail:] if keep_tail > 0 else ""
    return f"{head}…{'*' * 6}{tail}"


def _redact_dsn(dsn: str) -> str:
    """
    Redact credentials inside DSNs (best-effort, no urllib dependency).
    Examples:
      postgres://user:pass@host/db  -> postgres://user:***@host/db
      postgresql+asyncpg://u:p@h/db -> postgresql+asyncpg://u:***@h/db
    """
    if not dsn:
        return ""
    s = str(dsn)
    # Find "://", then redact "user:pass@" section if present
    try:
        i = s.find("://")
        if i == -1:
            return _mask(s, 6, 0)
        prefix = s[: i + 3]
        rest = s[i + 3 :]
        at = rest.find("@")
        if at == -1:
            return prefix + _mask(rest, 6, 0)
        creds = rest[:at]
        after = rest[at:]
        if ":" in creds:
            user = creds.split(":", 1)[0]
            return f"{prefix}{user}:***{after}"
        return f"{prefix}{_mask(creds, 3, 0)}{after}"
    except Exception:
        return _mask(s, 6, 0)


def _warn(msg: str, *args: Any) -> None:
    try:
        _LOG.warning(msg, *args)
    except Exception:
        return


# =============================================================================
# App / environment / logging
# =============================================================================

APP_NAME = _get_str("APP_NAME", "solana-sniper-bot-pro")
ENV = _get_str("ENV", "dev").strip().lower()
if ENV not in _ALLOWED_ENVS:
    _warn("[CONFIG] ENV=%r not recognized; treating as 'dev'", ENV)
    ENV = "dev"

LOG_LEVEL = _get_str("LOG_LEVEL", "INFO").strip().upper()
STRICT_CONFIG = _get_bool("STRICT_CONFIG", False)

IS_PROD = ENV in {"prod", "production"}
ENABLE_METRICS = _get_bool("ENABLE_METRICS", True)
METRICS_PORT = _clamp_i(_get_int("METRICS_PORT", 9108), 1, 65535)

# Optional crash reporting (no dependency here; modules may read these)
SENTRY_DSN = _get_str("SENTRY_DSN", "")
SENTRY_ENV = _get_str("SENTRY_ENV", ENV)

# Useful identity (read-only; may be absent)
GIT_COMMIT = (_get_str("GIT_COMMIT", "")[:40] or _get_str("RENDER_GIT_COMMIT", "")[:40]).strip()
RUN_ID = _get_str("RUN_ID", "").strip()


# =============================================================================
# Master Roadmap knobs (scripts/main.py expects these)
# =============================================================================

MASTER_PHASE = _clamp_i(_get_int("MASTER_PHASE", 7), 0, 7)  # 0..7
USE_LEGACY_MAIN = _get_bool("USE_LEGACY_MAIN", False)

ENTRY_WORKERS = _clamp_i(_get_int("ENTRY_WORKERS", 2), 1, 64)
CANDIDATE_QUEUE_MAX = _clamp_i(_get_int("CANDIDATE_QUEUE_MAX", 2000), 100, 2_000_000)
TICK_QUEUE_MAX = _clamp_i(_get_int("TICK_QUEUE_MAX", 4000), 100, 2_000_000)

SUPERVISOR_SEC = _clamp_i(_get_int("SUPERVISOR_SEC", 5), 1, 60)
RESTART_BACKOFF_SEC = _clamp_f(_get_float("RESTART_BACKOFF_SEC", 2.0), 0.1, 600.0)
RESTART_MAX_BACKOFF_SEC = _clamp_f(_get_float("RESTART_MAX_BACKOFF_SEC", 60.0), 1.0, 3600.0)
RESTART_MAX_PER_HOUR = _clamp_i(_get_int("RESTART_MAX_PER_HOUR", 60), 1, 10_000)

HEALTH_LOG_SEC = _clamp_i(_get_int("HEALTH_LOG_SEC", 30), 5, 3600)
STARVATION_WARN_SEC = _clamp_f(_get_float("STARVATION_WARN_SEC", 180.0), 5.0, 24 * 3600.0)
NO_BUY_WARN_SEC = _clamp_f(_get_float("NO_BUY_WARN_SEC", 300.0), 5.0, 24 * 3600.0)

MAIN_IDLE_SLEEP_SEC = _clamp_f(_get_float("MAIN_IDLE_SLEEP_SEC", 0.5), 0.01, 10.0)
SHUTDOWN_GRACE_SEC = _clamp_f(_get_float("SHUTDOWN_GRACE_SEC", 15.0), 1.0, 300.0)

QUEUE_DROP_POLICY = _get_str("QUEUE_DROP_POLICY", "drop_newest").strip().lower()
if QUEUE_DROP_POLICY not in _ALLOWED_QUEUE_DROP:
    _warn("[CONFIG] QUEUE_DROP_POLICY=%r invalid; using 'drop_newest'", QUEUE_DROP_POLICY)
    QUEUE_DROP_POLICY = "drop_newest"

SCANNER_MODULES = _get_list("SCANNER_MODULES", [])  # optional override list in scripts/main.py

# Phase 7 crash loop controls (scripts/main.py may read env directly; we expose for consistency)
MAIN_RESTART_ON_CRASH = _get_bool("MAIN_RESTART_ON_CRASH", False)
MAIN_CRASH_BACKOFF_SEC = _clamp_f(_get_float("MAIN_CRASH_BACKOFF_SEC", 2.0), 0.1, 600.0)
MAIN_CRASH_BACKOFF_MAX_SEC = _clamp_f(_get_float("MAIN_CRASH_BACKOFF_MAX_SEC", 60.0), 1.0, 3600.0)

# Orchestrator queue dedupe knobs
CAND_DEDUP_ENABLED = _get_bool("CAND_DEDUP_ENABLED", True)
CAND_DEDUP_TTL_SEC = _clamp_f(_get_float("CAND_DEDUP_TTL_SEC", 10.0), 0.1, 3600.0)
CAND_DEDUP_MAX = _clamp_i(_get_int("CAND_DEDUP_MAX", 75_000), 1000, 10_000_000)

TICK_DEDUP_ENABLED = _get_bool("TICK_DEDUP_ENABLED", True)
TICK_DEDUP_TTL_SEC = _clamp_f(_get_float("TICK_DEDUP_TTL_SEC", 2.0), 0.05, 3600.0)
TICK_DEDUP_MAX = _clamp_i(_get_int("TICK_DEDUP_MAX", 200_000), 1000, 20_000_000)

FORWARD_TICKS_FROM_SCANNERS = _get_bool("FORWARD_TICKS_FROM_SCANNERS", False)

# Orchestrator behavior toggles
MAIN_FAST_FAIL = _get_bool("MAIN_FAST_FAIL", False)
MAIN_VALIDATE_ONLY = _get_bool("MAIN_VALIDATE_ONLY", False)
MAIN_THREADIFY_SYNC_MODULES = _get_bool("MAIN_THREADIFY_SYNC_MODULES", False)


# =============================================================================
# Kill-switch helpers (env + optional file toggles)
# =============================================================================

KILL_SWITCH = _get_bool("KILL_SWITCH", False)
DISABLE_BUYS = _get_bool("DISABLE_BUYS", False)
DISABLE_SELLS = _get_bool("DISABLE_SELLS", False)
DISABLE_SCANNERS = _get_bool("DISABLE_SCANNERS", False)
PAUSE_TRADING = _get_bool("PAUSE_TRADING", False)

KILL_SWITCH_FILE = _get_str("KILL_SWITCH_FILE", "")
DISABLE_BUYS_FILE = _get_str("DISABLE_BUYS_FILE", "")
DISABLE_SELLS_FILE = _get_str("DISABLE_SELLS_FILE", "")
DISABLE_SCANNERS_FILE = _get_str("DISABLE_SCANNERS_FILE", "")

KILL_WATCHDOG_SEC = _clamp_f(_get_float("KILL_WATCHDOG_SEC", 2.0), 0.1, 30.0)
KILL_SHUTDOWN_ON_TRIGGER = _get_bool("KILL_SHUTDOWN_ON_TRIGGER", False)


# =============================================================================
# RPC / chain
# =============================================================================

NETWORK = _get_str("NETWORK", _get_str("SOLANA_CLUSTER", "mainnet")).strip() or "mainnet"

RPC_URL = (
    _pick_first(["RPC_URL", "SOLANA_RPC_URL", "SOLANA_RPC", "SOL_RPC_URL"], default=None)
    or "https://api.mainnet-beta.solana.com"
)
RPC_URLS = _get_list("RPC_URLS", [RPC_URL])

WEBSOCKET_URL = _get_str("WEBSOCKET_URL", _get_str("RPC_WS_URL", _get_str("SOLANA_WS_URL", ""))).strip()
COMMITMENT = _get_str("COMMITMENT", _get_str("RPC_COMMITMENT", "confirmed")).strip().lower()
if COMMITMENT not in _ALLOWED_COMMITMENT:
    _warn("[CONFIG] COMMITMENT=%r invalid; using 'confirmed'", COMMITMENT)
    COMMITMENT = "confirmed"

# Back-compat aliases
SOLANA_ENDPOINT = RPC_URL
SOLANA_RPC_URL = RPC_URL
SOL_RPC_URL = RPC_URL
TX_RPC_URL = _get_str("TX_RPC_URL", RPC_URL).strip() or RPC_URL

# Optional RPC providers
HELIUS_RPC_URL = _get_str("HELIUS_RPC_URL", "")

# Jito (optional)
ENABLE_JITO = _get_bool("ENABLE_JITO", _get_bool("JITO_ENABLED", False))
JITO_BLOCK_ENGINE_URL = _get_str("JITO_BLOCK_ENGINE_URL", "")
JITO_TIP_LAMPORTS = _clamp_i(_get_int("JITO_TIP_LAMPORTS", 50_000), 0, 10_000_000)  # ~0.00005 SOL default
JITO_FEE_MICROLAMPORTS = _clamp_i(_get_int("JITO_FEE_MICROLAMPORTS", 0), 0, 10_000_000)

# Useful constants (common in router/scanners)
SOL_MINT = _get_str("SOL_MINT", "So11111111111111111111111111111111111111112")


# =============================================================================
# Wallet(s)
# =============================================================================

# Preferred: WALLET_PRIVATE_KEY as JSON array (32/64 ints)
WALLET_PRIVATE_KEY = _get_str("WALLET_PRIVATE_KEY", "")
# Optional alternative supported by some repos
KEYPAIR_PATH = _get_str("KEYPAIR_PATH", _get_str("WALLET_KEYPAIR_PATH", ""))

WALLET_PUBKEY = _get_str("WALLET_PUBKEY", _get_str("WALLET_PUBLIC_KEY", ""))
SAVINGS_WALLET = _get_str("SAVINGS_WALLET", "")

# Owner aliases (for modules that use OWNER / OWNER_B58)
OWNER = _get_str("OWNER", WALLET_PUBKEY)
OWNER_B58 = _get_str("OWNER_B58", OWNER)

# Minimum free SOL to keep as safety buffer
MIN_SOL_BALANCE = _clamp_f(_get_float("MIN_SOL_BALANCE", 0.20), 0.0, 10_000.0)


# =============================================================================
# External APIs / scanners
# =============================================================================

COINGECKO_SOL_API = _get_str(
    "COINGECKO_SOL_API",
    "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd",
)

RAYDIUM_API_URL = _get_str("RAYDIUM_API_URL", "https://api.raydium.io/pairs").strip()
RAYDIUM_SEARCH_QUERIES = _get_list("RAYDIUM_SEARCH_QUERIES", ["raydium", "solana"])

DEXSCREENER_API_URL = _get_str("DEXSCREENER_API_URL", "https://api.dexscreener.com/latest/dex/tokens").strip()
DEXSCREENER_BASE_URL = _get_str("DEXSCREENER_BASE_URL", "https://api.dexscreener.com").strip().rstrip("/")

PUMPFUN_API_URL = _get_str("PUMPFUN_API_URL", "https://pump.fun/api/v1/tokens").strip()
PUMPFUN_API_BASE = _get_str("PUMPFUN_API_BASE", _get_str("PUMPFUN_API_BASES", "")).strip().rstrip("/")
PUMPFUN_HTTP_TIMEOUT_S = _clamp_i(_get_int("PUMPFUN_HTTP_TIMEOUT_S", 15), 2, 120)
PUMPFUN_HTTP_CONN_LIMIT = _clamp_i(_get_int("PUMPFUN_HTTP_CONN_LIMIT", 30), 1, 500)
PUMPFUN_API_LIMIT = _clamp_i(_get_int("PUMPFUN_API_LIMIT", 20), 1, 500)
PUMPFUN_DOM_LIMIT = _clamp_i(_get_int("PUMPFUN_DOM_LIMIT", PUMPFUN_API_LIMIT), 1, 2000)
PUMPFUN_SKIP_NEXT = _get_bool("PUMPFUN_SKIP_NEXT", False)
PUMPFUN_SKIP_HTMLNEXT = _get_bool("PUMPFUN_SKIP_HTMLNEXT", False)

BIRDEYE_API_URL = _get_str("BIRDEYE_API_URL", "https://public-api.birdeye.so").strip().rstrip("/")
BIRDEYE_API_KEY = _get_str("BIRDEYE_API_KEY", "")
BIRDEYE_ENABLED = _get_bool("BIRDEYE_ENABLED", bool(BIRDEYE_API_KEY))


# =============================================================================
# Jupiter v6 / router
# =============================================================================

JUP_BASE_URL = _get_str("JUP_BASE_URL", "https://api.jup.ag").strip().rstrip("/")
JUP_ALT_BASE_URLS = _get_list("JUP_ALT_BASE_URLS", [JUP_BASE_URL])
JUP_AS_LEGACY = _get_bool("JUP_AS_LEGACY", True)
JUP_USE_SHARED_ACCOUNTS = _get_bool("JUP_USE_SHARED_ACCOUNTS", True)
JUP_ONLY_DIRECT = _get_bool("JUP_ONLY_DIRECT", False)

# Key aliases
JUP_PRO_KEY = _get_str("JUP_PRO_KEY", _get_str("JUP_API_KEY", _get_str("JUPITER_API_KEY", "")))

# Old Jupiter base (kept for backward compat; router/lightning should prefer JUP_BASE_URL + variant flags)
JUPITER_API_BASE = _get_str("JUPITER_API_BASE", "https://quote-api.jup.ag/v6").strip().rstrip("/")
JUPITER_QUOTE_API = _get_str("JUPITER_QUOTE_API", f"{JUPITER_API_BASE}/quote").strip()
JUPITER_SWAP_API = _get_str("JUPITER_SWAP_API", JUPITER_API_BASE).strip()

# Slippage: router/tx expects bps (0..5000)
JUPITER_MAX_SLIPPAGE = _clamp_f(_get_float("JUPITER_MAX_SLIPPAGE", 0.005), 0.0, 0.50)  # fraction
JUPITER_SLIPPAGE_BPS = int(round(JUPITER_MAX_SLIPPAGE * 10_000))

# Router / tx knobs seen in your repo history
PF_SLIPPAGE_BPS = _clamp_i(_get_bps("PF_SLIPPAGE_BPS", max(5, min(5000, JUPITER_SLIPPAGE_BPS))), 1, 5000)
PF_WRAP_UNWRAP_SOL = _get_bool("PF_WRAP_UNWRAP_SOL", True)
PF_COMPUTE_UNIT_LIMIT = _clamp_i(_get_int("PF_COMPUTE_UNIT_LIMIT", 0), 0, 3_000_000)  # 0 = let upstream decide
PF_DEBUG_TX = _get_bool("PF_DEBUG_TX", False)


# =============================================================================
# Credentials / alerts
# =============================================================================

TELEGRAM_BOT_TOKEN = _get_str("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = _get_str("TELEGRAM_CHAT_ID", "")
DISCORD_WEBHOOK = _get_str("DISCORD_WEBHOOK", "")

ENABLE_TELEGRAM = _get_bool("ENABLE_TELEGRAM", bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID))
ENABLE_DISCORD = _get_bool("ENABLE_DISCORD", bool(DISCORD_WEBHOOK))

NOTION_TOKEN = _get_str("NOTION_TOKEN", _get_str("NOTION_API_KEY", ""))
NOTION_DATABASE_ID = _get_str("NOTION_DATABASE_ID", _get_str("NOTION_DB_TRADES", ""))
NOTION_ENABLED = _get_bool("NOTION_ENABLED", True)
ENABLE_NOTION_LOGGING = _get_bool("ENABLE_NOTION_LOGGING", NOTION_ENABLED)

# Optional: Notion property names override (used by core_utils)
NOTION_TITLE_PROP = _get_str("NOTION_TITLE_PROP", "Token")
NOTION_PROP_TOKEN = _get_str("NOTION_PROP_TOKEN", "Mint")
NOTION_PROP_ENTRY = _get_str("NOTION_PROP_ENTRY", "Entry Price")
NOTION_PROP_CURRENT = _get_str("NOTION_PROP_CURRENT", "Current Price")
NOTION_PROP_PROFIT = _get_str("NOTION_PROP_PROFIT", "Profit")
NOTION_PROP_REASON = _get_str("NOTION_PROP_REASON", "Reason")
NOTION_PROP_TIME = _get_str("NOTION_PROP_TIME", "Timestamp")

# OpenAI / AI models (names only; call sites handle the API usage)
OPENAI_API_KEY = _get_str("OPENAI_API_KEY", "")
OPENAI_MODEL = _get_str("OPENAI_MODEL", "gpt-4o-mini")
OPENAI_BASE_URL = _get_str("OPENAI_BASE_URL", "")
FINE_TUNED_MODEL = _get_str("FINE_TUNED_MODEL", "")


# =============================================================================
# Storage / state backends
# =============================================================================

REDIS_URL = _get_str("REDIS_URL", "redis://localhost:6379/0")
REDIS_PREFIX = _get_str("REDIS_PREFIX", "sniper:").strip() or "sniper:"
if not REDIS_PREFIX.endswith(":"):
    REDIS_PREFIX += ":"

SQLITE_DB_PATH = _get_str("SQLITE_DB_PATH", _get_str("SQLITE_DB", os.path.join("data", "state.db")))

# Positions store backend: "json" | "sqlite" | "pg"
POSITIONS_BACKEND = _get_str("POSITIONS_BACKEND", "json").strip().lower()
if POSITIONS_BACKEND not in _ALLOWED_POS_BACKENDS:
    _warn("[CONFIG] POSITIONS_BACKEND=%r invalid; using 'json'", POSITIONS_BACKEND)
    POSITIONS_BACKEND = "json"

POSITIONS_JSON_PATH = _get_str("POSITIONS_JSON_PATH", os.path.join("data", "positions.json"))

# Postgres
PG_ENABLED = _get_bool("PG_ENABLED", True)
PG_DSN = _get_str("PG_DSN", "")
PG_DSN_INTERNAL = _get_str("PG_DSN_INTERNAL", "")
PG_DSN_EXTERNAL = _get_str("PG_DSN_EXTERNAL", "")

POSTGRES_DSN = _get_str("POSTGRES_DSN", PG_DSN or PG_DSN_INTERNAL or PG_DSN_EXTERNAL)

SNIPER_DB_DSN = _get_str("SNIPER_DB_DSN", POSTGRES_DSN)
PNL_DB_DSN = _get_str("PNL_DB_DSN", POSTGRES_DSN)
PNL_DSN = _get_str("PNL_DSN", POSTGRES_DSN)
POSITIONS_PG_DSN = _get_str("POSITIONS_PG_DSN", POSTGRES_DSN)
POSITIONS_PG_ENABLED = _get_bool("POSITIONS_PG_ENABLED", bool(POSITIONS_PG_DSN))


# =============================================================================
# Trading / execution core (compat-heavy)
# =============================================================================

AUTO_BUY_ENABLED = _get_bool("AUTO_BUY_ENABLED", _get_bool("BUY_ENGINE_ENABLED", True))
AUTO_SELL_ENABLED = _get_bool(
    "AUTO_SELL_ENABLED",
    _get_bool("ENABLE_AUTO_SELL", _get_bool("EXIT_ENGINE_ENABLED", True)),
)

DRY_RUN = _get_bool("DRY_RUN", _get_bool("DRYRUN", False))
DRYRUN = DRY_RUN  # back-compat

TRANSACTION_TIMEOUT = _clamp_i(_get_int("TRANSACTION_TIMEOUT", 20), 5, 180)
MAX_RETRIES = _clamp_i(_get_int("MAX_RETRIES", 3), 0, 20)
EXEC_DELAY = _clamp_i(_get_int("SNIPER_EXEC_DELAY", _get_int("EXEC_DELAY", 0)), 0, 30)

# Legacy sizing knobs used across earlier modules
BUY_SOL_PER_TRADE = _clamp_f(_get_float("BUY_SOL_PER_TRADE", 0.02), 0.0, 100.0)
ENTRY_MIN_BUY_SOL = _clamp_f(_get_float("ENTRY_MIN_BUY_SOL", 0.015), 0.0, 100.0)
ENTRY_MAX_BUY_SOL = _clamp_f(_get_float("ENTRY_MAX_BUY_SOL", 0.10), 0.0, 100.0)
ENTRY_MIN_INITIAL_BUY_SOL = _clamp_f(_get_float("ENTRY_MIN_INITIAL_BUY_SOL", 0.0025), 0.0, 100.0)

# Legacy baseline sizing / risk
MAX_RISK_PER_TRADE = _clamp_f(_get_float("MAX_RISK_PER_TRADE", 0.05), 0.0, 1.0)  # legacy fraction
MIN_VOLATILITY = _clamp_f(_get_float("MIN_VOLATILITY", 0.02), 0.0, 10.0)
PARTIAL_SELL_CHUNKS = _clamp_i(_get_int("PARTIAL_SELL_CHUNKS", 4), 1, 20)

# Ticket sizing (newer risk manager prefers these)
TICKET_PCT_EQUITY = _get_prob("TICKET_PCT_EQUITY", 0.04)  # fraction
TICKET_USD_CAP = _clamp_f(_get_float("TICKET_USD_CAP", 300.0), 0.0, 1_000_000.0)

# Targets / SL / hold (percent values)
TARGET_PROFIT_PERCENT = _clamp_f(_get_float("TARGET_PROFIT_PERCENT", 200.0), 0.0, 100_000.0)  # 200% = 2x
STOP_LOSS_PERCENT = _clamp_f(_get_float("STOP_LOSS_PERCENT", 20.0), 0.0, 99.9)
TRAILING_STOP_ENABLED = _get_bool("TRAILING_STOP_ENABLED", True)
MAX_HOLD_TIME_MINUTES = _clamp_i(_get_int("MAX_HOLD_TIME_MINUTES", 360), 1, 30_000)
HOLD_IF_SCORE_ABOVE = _get_prob("HOLD_IF_SCORE_ABOVE", 0.85)  # fraction

# Take-profit ladder (percent)
TAKE_PROFIT1_PCT = _clamp_f(_get_float("TAKE_PROFIT1_PCT", 30.0), 0.0, 100_000.0)
TAKE_PROFIT2_PCT = _clamp_f(_get_float("TAKE_PROFIT2_PCT", 60.0), 0.0, 100_000.0)
TAKE_PROFIT3_PCT = _clamp_f(_get_float("TAKE_PROFIT3_PCT", 120.0), 0.0, 100_000.0)

# Reinvestor (compat) (percent values)
REINVESTOR_ENABLED = _get_bool("REINVESTOR_ENABLED", True)
REINVEST_PERCENT = _clamp_f(_get_float("REINVEST_PERCENT", 80.0), 0.0, 100.0)
REINVESTMENT_PERCENT_CAP = _clamp_f(_get_float("REINVESTMENT_PERCENT_CAP", 50.0), 0.0, 100.0)
REINVESTMENT_VOLUME_MIN = _clamp_f(_get_float("REINVESTMENT_VOLUME_MIN", 5_000.0), 0.0, 1_000_000_000.0)
REINVESTMENT_MIN_PROFIT_SOL = _clamp_f(_get_float("REINVESTMENT_MIN_PROFIT_SOL", 0.01), 0.0, 1000.0)
REINVESTMENT_HONEYPOT_CHECK = _get_bool("REINVESTMENT_HONEYPOT_CHECK", True)


# =============================================================================
# Quant-grade risk manager (utils_pkg.risk_manager)
# =============================================================================

RISK_PROFILE = _get_str("RISK_PROFILE", "balanced").strip().lower()
RISK_MODE_OVERRIDE = _raw("RISK_MODE_OVERRIDE")  # None if unset

RISK_MAX_EQUITY_PCT_PER_TRADE = _get_prob("RISK_MAX_EQUITY_PCT_PER_TRADE", TICKET_PCT_EQUITY)
RISK_MAX_EQUITY_PCT_PER_COIN = _get_prob("RISK_MAX_EQUITY_PCT_PER_COIN", 0.10)

RISK_MAX_OPEN_POSITIONS = _clamp_i(_get_int("RISK_MAX_OPEN_POSITIONS", 6), 0, 500)
RISK_MAX_PARALLEL_BUYS = _clamp_i(_get_int("RISK_MAX_PARALLEL_BUYS", 3), 0, 100)

RISK_MAX_DAILY_DRAWDOWN_PCT = _clamp_f(
    _get_float("RISK_MAX_DAILY_DRAWDOWN_PCT", _get_float("PF_DAILY_MAX_DRAWDOWN_PCT", 18.0)),
    0.0,
    100.0,
)
RISK_MAX_DAILY_LOSS_SOL = _clamp_f(_get_float("RISK_MAX_DAILY_LOSS_SOL", 9999.0), 0.0, 1_000_000.0)
RISK_CIRCUIT_BREAKER_MINUTES = _clamp_i(_get_int("RISK_CIRCUIT_BREAKER_MINUTES", 45), 1, 10_000)

STOP_AFTER_DAILY_PROFIT_PCT = _clamp_f(_get_float("STOP_AFTER_DAILY_PROFIT_PCT", _get_float("PF_STOP_AFTER_PROFIT_PCT", 25.0)), 0.0, 10_000.0)
DAILY_SPEND_CAP_PCT = _clamp_f(_get_float("DAILY_SPEND_CAP_PCT", 35.0), 0.0, 100.0)

RISK_HARD_KILL_ON_DRAWNDOWN = _get_bool("RISK_HARD_KILL_ON_DRAWNDOWN", True)
RISK_ALLOW_REENTRY_AFTER_BREAK = _get_bool("RISK_ALLOW_REENTRY_AFTER_BREAK", True)

# Back-compat imports
MAX_CONCURRENT_POS = _get_int("MAX_CONCURRENT_POS", RISK_MAX_OPEN_POSITIONS)
MAX_CONCURRENT_BUYS = _get_int("MAX_CONCURRENT_BUYS", RISK_MAX_PARALLEL_BUYS)
MAX_CONCURRENCY = _get_int("MAX_CONCURRENCY", MAX_CONCURRENT_BUYS)  # older name


# =============================================================================
# Exit engine / auto-sell (utils_pkg.exit_engine)
# =============================================================================

EXIT_STOP_LOSS_PCT = _clamp_f(_get_float("EXIT_STOP_LOSS_PCT", STOP_LOSS_PERCENT), 0.0, 99.9)
EXIT_TP1_PCT = _clamp_f(_get_float("EXIT_TP1_PCT", TAKE_PROFIT1_PCT), 0.0, 100_000.0)
EXIT_TP2_PCT = _clamp_f(_get_float("EXIT_TP2_PCT", TAKE_PROFIT2_PCT), 0.0, 100_000.0)
EXIT_TP3_PCT = _clamp_f(_get_float("EXIT_TP3_PCT", TAKE_PROFIT3_PCT), 0.0, 100_000.0)

EXIT_TSL_ENABLED = _get_bool("EXIT_TSL_ENABLED", True)
EXIT_TSL_ARM_PCT = _clamp_f(_get_float("EXIT_TSL_ARM_PCT", 12.0), 0.0, 100_000.0)
EXIT_TSL_STEP_PCT = _clamp_f(_get_float("EXIT_TSL_STEP_PCT", 4.5), 0.1, 100_000.0)

# Legacy PF_* aliases
PF_TSL_ARM_PCT = _clamp_f(_get_float("PF_TSL_ARM_PCT", EXIT_TSL_ARM_PCT), 0.0, 100_000.0)
PF_TSL_PCT = _clamp_f(_get_float("PF_TSL_PCT", EXIT_TSL_STEP_PCT), 0.1, 100_000.0)
PF_SL_PCT = _clamp_f(_get_float("PF_SL_PCT", EXIT_STOP_LOSS_PCT), 0.0, 99.9)

EXIT_STAGNANT_MAX_MIN = _clamp_i(_get_int("EXIT_STAGNANT_MAX_MIN", 10), 1, 10_000)
EXIT_STAGNANT_MIN_MOVE_PCT = _clamp_f(_get_float("EXIT_STAGNANT_MIN_MOVE_PCT", 5.0), 0.0, 10_000.0)
EXIT_DEAD_COIN_MAX_MIN = _clamp_i(_get_int("EXIT_DEAD_COIN_MAX_MIN", 45), 1, 10_000)
EXIT_DEAD_MIN_LIQ_USD = _clamp_f(_get_float("EXIT_DEAD_MIN_LIQ_USD", 400.0), 0.0, 1_000_000_000.0)
EXIT_FORCE_AFTER_X_MULT = _clamp_f(_get_float("EXIT_FORCE_AFTER_X_MULT", 8.0), 1.0, 10_000.0)

EXIT_LOCK_PROFIT_AFTER_MULT = _clamp_f(_get_float("EXIT_LOCK_PROFIT_AFTER_MULT", 3.0), 1.0, 10_000.0)
EXIT_LOCK_PROFIT_TSL_STEP_PCT = _clamp_f(_get_float("EXIT_LOCK_PROFIT_TSL_STEP_PCT", 3.5), 0.1, 100_000.0)

AUTO_EXIT_WORKER_ENABLED = _get_bool("AUTO_EXIT_WORKER_ENABLED", True)


# =============================================================================
# Entry momentum guard (Strategy / ai_entry_logic pre-gate)
# =============================================================================

ENTRY_MOMENTUM_ENABLED = _get_bool("ENTRY_MOMENTUM_ENABLED", _get_bool("AI_MOMO_FILTER", True))
ENTRY_MOMO_WINDOW_SEC = _clamp_i(_get_int("ENTRY_MOMO_WINDOW_SEC", _get_int("MOMO_WINDOW_SEC", 25)), 1, 600)
ENTRY_MOMO_MIN_TRADES = _clamp_i(_get_int("ENTRY_MOMO_MIN_TRADES", _get_int("MOMO_MIN_TRADES", 2)), 0, 10_000)
ENTRY_MOMO_MIN_UP_PCT = _clamp_f(_get_float("ENTRY_MOMO_MIN_UP_PCT", _get_float("MOMO_MIN_UP_PCT", 2.0)), -100.0, 10_000.0)
ENTRY_MOMO_ALLOW_NO_DATA_FOR_PUMP = _get_bool(
    "ENTRY_MOMO_ALLOW_NO_DATA_FOR_PUMP",
    _get_bool("MOMO_ALLOW_NO_DATA_FOR_PUMP", True),
)


# =============================================================================
# Sniper loop / scanners tuning
# =============================================================================

SCAN_INTERVAL = _clamp_i(_get_int("SCAN_INTERVAL", 60), 1, 3600)
SCANNER_PERIOD_SEC = _clamp_i(_get_int("SCANNER_PERIOD_SEC", 4), 1, 60)

BUY_COOLDOWN_SEC = _clamp_i(_get_int("BUY_COOLDOWN_SEC", 3), 0, 600)

PF_MAX_CONCURRENT = _clamp_i(_get_int("PF_MAX_CONCURRENT", MAX_CONCURRENT_BUYS), 0, 1000)
PREWARM_TOP_N = _clamp_i(_get_int("PREWARM_TOP_N", 5), 0, 1000)

SCANNER_MIN_LIQ_USD = _clamp_f(_get_float("SCANNER_MIN_LIQ_USD", 150.0), 0.0, 1_000_000_000.0)
MIN_LIQ_USD = _clamp_f(_get_float("MIN_LIQ_USD", SCANNER_MIN_LIQ_USD), 0.0, 1_000_000_000.0)
LIQ_MIN_USD = MIN_LIQ_USD

MIN_POOL_AGE_MIN = _clamp_i(_get_int("MIN_POOL_AGE_MIN", 1), 0, 10_000)

NOROUTE_COOLDOWN_SEC = _clamp_i(_get_int("NOROUTE_COOLDOWN_SEC", 600), 0, 86_400)

BLOCKLIST_MINTS = _get_list("BLOCKLIST_MINTS", [])
ALLOWLIST_MINTS = _get_list("ALLOWLIST_MINTS", [])

STRATEGY_PRESET = _get_str("STRATEGY_PRESET", "max-pnl").strip().lower()

TELEGRAM_HEARTBEATS = _get_bool("TELEGRAM_HEARTBEATS", True)
HEARTBEAT_EVERY_SEC = _clamp_i(_get_int("HEARTBEAT_EVERY_SEC", 300), 10, 86_400)
HEARTBEAT_ONLY_ON_CHANGE = _get_bool("HEARTBEAT_ONLY_ON_CHANGE", True)
TELEGRAM_NOTIFY_ROUTE_BLOCKS = _get_bool("TELEGRAM_NOTIFY_ROUTE_BLOCKS", True)

PNL_TELEGRAM_HEARTBEAT = _get_bool("PNL_TELEGRAM_HEARTBEAT", True)

# Moonshot budget
MOONSHOT_ENABLED = _get_bool("MOONSHOT_ENABLED", True)
MOONSHOT_DAILY_USD = _clamp_f(_get_float("MOONSHOT_DAILY_USD", 100.0), 0.0, 1_000_000.0)
MOONSHOT_MAX_TRADE_USD = _clamp_f(_get_float("MOONSHOT_MAX_TRADE_USD", 25.0), 0.0, 1_000_000.0)
MOONSHOT_AGE_CUTOFF_MIN = _clamp_i(_get_int("MOONSHOT_AGE_CUTOFF_MIN", 30), 0, 10_000)
MOONSHOT_LIQ_CUTOFF_USD = _clamp_f(_get_float("MOONSHOT_LIQ_CUTOFF_USD", 20_000.0), 0.0, 1_000_000_000.0)
MOONSHOT_IMPACT_CUTOFF_BPS = _clamp_f(_get_float("MOONSHOT_IMPACT_CUTOFF_BPS", 150.0), 0.0, 50_000.0)


# =============================================================================
# Scanners toggles (compat + scripts/main.py expectations)
# =============================================================================

ENABLE_RAYDIUM_SCANNER = _get_bool("ENABLE_RAYDIUM_SCANNER", True)

# unify naming: pumpfun vs pump
ENABLE_PUMPFUN_SCANNER = _get_bool("ENABLE_PUMPFUN_SCANNER", _get_bool("ENABLE_PUMP_SCANNER", True))
ENABLE_PUMP_SCANNER = ENABLE_PUMPFUN_SCANNER  # scripts/main.py expects this name

ENABLE_DEXSCREENER_SCANNER = _get_bool("ENABLE_DEXSCREENER_SCANNER", True)

ENABLE_ARBITRAGE = _get_bool("ENABLE_ARBITRAGE", False)
DEXES: List[str] = _get_list("DEXES", ["jupiter", "raydium", "orca"])

SCANNER_MIN_FDV_USD = _clamp_f(_get_float("SCANNER_MIN_FDV_USD", 0.0), 0.0, 1_000_000_000_000.0)
SCANNER_MIN_FRAUD_SCORE = _clamp_f(_get_float("SCANNER_MIN_FRAUD_SCORE", 0.0), 0.0, 1.0)
SCANNER_ALERTS_ENABLED = _get_bool("SCANNER_ALERTS_ENABLED", True)

SCANNER_MIN_BUYS_5M = _clamp_i(_get_int("SCANNER_MIN_BUYS_5M", 0), 0, 1_000_000)
SCANNER_MIN_BUYS_15M = _clamp_i(_get_int("SCANNER_MIN_BUYS_15M", 0), 0, 1_000_000)
SCANNER_MIN_VOLUME_5M_USD = _clamp_f(_get_float("SCANNER_MIN_VOLUME_5M_USD", 20.0), 0.0, 1_000_000_000.0)
SCANNER_MIN_VOLUME_15M_USD = _clamp_f(_get_float("SCANNER_MIN_VOLUME_15M_USD", 60.0), 0.0, 1_000_000_000.0)

SCANNER_REQUIRE_BUY_PRESSURE = _get_bool("SCANNER_REQUIRE_BUY_PRESSURE", False)
SCANNER_MIN_BUY_RATIO = _clamp_f(_get_float("SCANNER_MIN_BUY_RATIO", 0.0), 0.0, 1000.0)

SCANNER_MIN_PRICE_CHANGE_5M = _clamp_f(_get_float("SCANNER_MIN_PRICE_CHANGE_5M", 0.0), -100.0, 100_000.0)
SCANNER_MIN_PRICE_CHANGE_15M = _clamp_f(_get_float("SCANNER_MIN_PRICE_CHANGE_15M", 0.0), -100.0, 100_000.0)

SCANNER_MAX_AGE_MIN = _clamp_i(_get_int("SCANNER_MAX_AGE_MIN", 720), 0, 1_000_000)
SCANNER_MAX_MC_USD = _clamp_f(_get_float("SCANNER_MAX_MC_USD", 50_000_000.0), 0.0, 10_000_000_000_000.0)
SCANNER_MIN_HOLDERS = _clamp_i(_get_int("SCANNER_MIN_HOLDERS", 0), 0, 1_000_000_000)

ALERT_COOLDOWN_SECONDS = _clamp_i(_get_int("ALERT_COOLDOWN_SECONDS", 900), 0, 86_400)
POSITION_MONITOR_INTERVAL = _clamp_i(_get_int("POSITION_MONITOR_INTERVAL", 20), 1, 3600)
REINVESTMENT_CHECK_INTERVAL = _clamp_i(_get_int("REINVESTMENT_CHECK_INTERVAL", 60), 1, 3600)


# =============================================================================
# PairFilter thresholds (utils_pkg.pair_filters)
# =============================================================================

PF_SCAN_MIN_LIQ_USD = _clamp_f(_get_float("PF_SCAN_MIN_LIQ_USD", 400.0), 0.0, 1_000_000_000.0)
PF_SCAN_MAX_LIQ_USD = _clamp_f(_get_float("PF_SCAN_MAX_LIQ_USD", 300_000.0), 0.0, 1_000_000_000.0)

PF_SCAN_MIN_MCAP_USD = _clamp_f(_get_float("PF_SCAN_MIN_MCAP_USD", 800.0), 0.0, 10_000_000_000_000.0)
PF_SCAN_MAX_MCAP_USD = _clamp_f(_get_float("PF_SCAN_MAX_MCAP_USD", 1_500_000.0), 0.0, 10_000_000_000_000.0)

PF_SCAN_MAX_TOKEN_AGE_MIN = _clamp_f(_get_float("PF_SCAN_MAX_TOKEN_AGE_MIN", 180.0), 0.0, 10_000_000.0)

PF_SCAN_MIN_5M_VOL_USD = _clamp_f(_get_float("PF_SCAN_MIN_5M_VOL_USD", 50.0), 0.0, 1_000_000_000.0)
PF_SCAN_MIN_5M_BUYS = _clamp_i(_get_int("PF_SCAN_MIN_5M_BUYS", 1), 0, 1_000_000)
PF_SCAN_MIN_BUY_TO_SELL_RATIO = _clamp_f(_get_float("PF_SCAN_MIN_BUY_TO_SELL_RATIO", 1.02), 0.0, 1000.0)

PF_SCAN_MIN_WHALE_SIZE_SOL = _clamp_f(_get_float("PF_SCAN_MIN_WHALE_SIZE_SOL", 1.5), 0.0, 1_000_000.0)
PF_SCAN_MIN_WHALE_COUNT_5M = _clamp_i(_get_int("PF_SCAN_MIN_WHALE_COUNT_5M", 0), 0, 1_000_000)

PF_MAX_RUG_RISK_SCORE = _clamp_f(_get_float("PF_MAX_RUG_RISK_SCORE", 0.80), 0.0, 1.0)

PF_MIN_MOMENTUM_SCORE = _clamp_f(_get_float("PF_MIN_MOMENTUM_SCORE", 0.14), 0.0, 1.0)
PF_MIN_FINAL_SCORE = _clamp_f(_get_float("PF_MIN_FINAL_SCORE", 0.10), 0.0, 1.0)

PF_RISK_PENALTY_WEIGHT = _clamp_f(_get_float("PF_RISK_PENALTY_WEIGHT", 0.6), 0.0, 10.0)


# =============================================================================
# AI strategy / entry / sell / buy scoring
# =============================================================================

AI_ENABLED = _get_bool("AI_ENABLED", True)

AI_ENTRY_ENABLED = _get_bool("AI_ENTRY_ENABLED", _get_bool("AIGATE_AI_ENABLED", True))
AI_ENTRY_MODEL = _get_str("AI_ENTRY_MODEL", _get_str("ENTRY_MODEL", FINE_TUNED_MODEL or OPENAI_MODEL))

AI_ENTRY_SCORE_MIN = _get_prob("AI_ENTRY_SCORE_MIN", _get_prob("ENTRY_MIN_SCORE", 0.60))
AI_ENTRY_SCORE_HARD_MIN = _get_prob("AI_ENTRY_SCORE_HARD_MIN", 0.50)
AI_ENTRY_RISK_ADJ_MIN = _get_prob("AI_ENTRY_RISK_ADJ_MIN", 0.50)

AI_ENTRY_THRESHOLD = _get_prob("AI_ENTRY_THRESHOLD", AI_ENTRY_SCORE_MIN)
AI_SCORE_GATE = _get_prob("AI_SCORE_GATE", AI_ENTRY_THRESHOLD)
AI_SCORE_SOFT_FLOOR = _get_prob("AI_SCORE_SOFT_FLOOR", AI_ENTRY_SCORE_HARD_MIN)
AI_SCORE_HARD_FLOOR = _get_prob("AI_SCORE_HARD_FLOOR", 0.38)

AI_ENTRY_MAX_CONCURRENCY = _clamp_i(_get_int("AI_ENTRY_MAX_CONCURRENCY", 8), 1, 256)

AI_PRE_SCORE_ENABLED = _get_bool("AI_PRE_SCORE_ENABLED", True)
AI_PRE_SCORE_MIN = _get_prob("AI_PRE_SCORE_MIN", 0.10)

ENTRY_REQUIRE_RISK_CAPACITY = _get_bool("ENTRY_REQUIRE_RISK_CAPACITY", True)
ENTRY_RISK_CAPACITY_MIN = _get_prob("ENTRY_RISK_CAPACITY_MIN", 0.30)

AI_RUG_ENABLED = _get_bool("AI_RUG_ENABLED", True)
AI_RUG_MODEL = _get_str("AI_RUG_MODEL", FINE_TUNED_MODEL or OPENAI_MODEL)
AI_RUG_MAX_SCORE = _clamp_f(_get_float("AI_RUG_MAX_SCORE", PF_MAX_RUG_RISK_SCORE), 0.0, 1.0)
AI_RUG_MIN_CONFIDENCE = _get_prob("AI_RUG_MIN_CONFIDENCE", 0.55)

AIGATE_MAX_PRICE_IMPACT_BPS = _clamp_i(_get_int("AIGATE_MAX_PRICE_IMPACT_BPS", 550), 0, 50_000)
AI_BAD_THRESHOLD = _get_prob("AI_BAD_THRESHOLD", 0.35)
AI_GOOD_THRESHOLD = _get_prob("AI_GOOD_THRESHOLD", 0.70)

AI_WHALE_ECHO = _get_bool("AI_WHALE_ECHO", True)
AI_WHALE_ECHO_MIN_SIZE_USD = _clamp_f(_get_float("AI_WHALE_ECHO_MIN_SIZE_USD", 300.0), 0.0, 1_000_000_000.0)

AI_ADAPTIVE_SELL = _get_bool("AI_ADAPTIVE_SELL", True)
AI_SELL_ENABLED = _get_bool("AI_SELL_ENABLED", True)
AI_SELL_MODEL = _get_str("AI_SELL_MODEL", FINE_TUNED_MODEL or OPENAI_MODEL)
AI_SELL_HOLD_THRESHOLD = _get_prob("AI_SELL_HOLD_THRESHOLD", 0.60)
AI_SELL_TRIM_THRESHOLD = _get_prob("AI_SELL_TRIM_THRESHOLD", 0.72)
AI_SELL_EXIT_THRESHOLD = _get_prob("AI_SELL_EXIT_THRESHOLD", 0.84)

STRATEGY_REDIS_PREFIX = _get_str("STRATEGY_REDIS_PREFIX", "strategy:v1").strip()
STRATEGY_UPDATE_INTERVAL = _clamp_i(_get_int("STRATEGY_UPDATE_INTERVAL", 1800), 30, 86_400)

AI_FEATURE_VERSION = _get_str("AI_FEATURE_VERSION", "v3+++")
AI_BUY_FEATURE_VERSION = _get_str("AI_BUY_FEATURE_VERSION", AI_FEATURE_VERSION)


# =============================================================================
# Regime detection
# =============================================================================

REGIME_ENABLED = _get_bool("REGIME_ENABLED", True)
REGIME_UPDATE_INTERVAL_SEC = _clamp_i(_get_int("REGIME_UPDATE_INTERVAL_SEC", 60), 5, 3600)

REGIME_SOL_MIN_24H_VOL_USD = _clamp_f(_get_float("REGIME_SOL_MIN_24H_VOL_USD", 200_000_000.0), 0.0, 1e12)
REGIME_MEME_MIN_24H_VOL_USD = _clamp_f(_get_float("REGIME_MEME_MIN_24H_VOL_USD", 50_000_000.0), 0.0, 1e12)
REGIME_MAX_ACCEPTABLE_RUG_RATE = _clamp_f(_get_float("REGIME_MAX_ACCEPTABLE_RUG_RATE", 0.25), 0.0, 1.0)


# =============================================================================
# AI data logging / training
# =============================================================================

LOG_ENTRY_CANDIDATES = _get_bool("LOG_ENTRY_CANDIDATES", True)
LOG_TRADES = _get_bool("LOG_TRADES", True)
LOG_REGIME_SNAPSHOTS = _get_bool("LOG_REGIME_SNAPSHOTS", True)

TRAIN_LABEL_WINDOW_MIN = _clamp_i(_get_int("TRAIN_LABEL_WINDOW_MIN", 60), 1, 10_000)
TRAIN_MIN_HOLD_MIN = _clamp_i(_get_int("TRAIN_MIN_HOLD_MIN", 10), 0, 10_000)
TRAIN_MAX_HOLD_MIN = _clamp_i(_get_int("TRAIN_MAX_HOLD_MIN", 720), 1, 100_000)


# =============================================================================
# Whale watch & smart wallets
# =============================================================================

SMART_WALLETS_ENABLED = _get_bool("SMART_WALLETS_ENABLED", True)
ENABLE_WHALE_FILTERS = _get_bool("ENABLE_WHALE_FILTERS", True)
DYNAMIC_WHALE_DISCOVERY = _get_bool("DYNAMIC_WHALE_DISCOVERY", True)

CURATED_WHALE_WALLETS = _get_list("CURATED_WHALE_WALLETS", [])
WHALE_WALLETS_JSON = _get_json("WHALE_WALLETS_JSON", {})

WHALE_WALLETS: Dict[str, str] = {}
if isinstance(WHALE_WALLETS_JSON, dict) and WHALE_WALLETS_JSON:
    WHALE_WALLETS.update({str(k): str(v) for k, v in WHALE_WALLETS_JSON.items() if str(v).strip()})
for i, addr in enumerate(CURATED_WHALE_WALLETS):
    if addr.strip():
        WHALE_WALLETS.setdefault(f"CURATED_{i+1}", addr.strip())

ENABLE_SOCIAL_SCRAPING = _get_bool("ENABLE_SOCIAL_SCRAPING", False)
X_BEARER_TOKEN = _get_str("X_BEARER_TOKEN", "")
TELEGRAM_SCRAPE_BOT = _get_str("TELEGRAM_SCRAPE_BOT", "")
DISCORD_SEARCH_CHANNEL_ID = _get_str("DISCORD_SEARCH_CHANNEL_ID", "")


# =============================================================================
# Daytrader (optional)
# =============================================================================

ENABLE_DAYTRADER = _get_bool("ENABLE_DAYTRADER", False)

DAYTRADER_SYMBOLS = _get_list("DAYTRADER_SYMBOLS", ["SOL/USDC"])
DAYTRADER_TIMEFRAMES = _get_list("DAYTRADER_TIMEFRAMES", ["1m", "5m", "15m", "1h"])
DAYTRADER_EXCHANGES = _get_list("DAYTRADER_EXCHANGES", ["binance", "bybit", "okx"])

DT_FEATURE_SET = _get_list("DT_FEATURE_SET", ["rsi", "macd", "bbands", "atr", "orderflow"])
DT_MODEL_KIND = _get_str("DT_MODEL_KIND", "xgboost")
DT_PROB_THRESHOLD = _get_prob("DT_PROB_THRESHOLD", 0.62)
DT_MAX_CONCURRENT = _clamp_i(_get_int("DT_MAX_CONCURRENT", 3), 0, 200)
DT_MAX_LEVERAGE = _clamp_f(_get_float("DT_MAX_LEVERAGE", 1.0), 0.0, 100.0)
DT_MAX_RISK_PER_TRADE = _get_prob("DT_MAX_RISK_PER_TRADE", 0.02)
DT_TAKE_PROFIT_BPS = _clamp_i(_get_int("DT_TAKE_PROFIT_BPS", 120), 0, 50_000)
DT_STOP_LOSS_BPS = _clamp_i(_get_int("DT_STOP_LOSS_BPS", 60), 0, 50_000)
DT_TRAILING_BPS = _clamp_i(_get_int("DT_TRAILING_BPS", 50), 0, 50_000)
DT_COOLDOWN_SEC = _clamp_i(_get_int("DT_COOLDOWN_SEC", 60), 0, 86_400)
DT_PAPER_TRADING = _get_bool("DT_PAPER_TRADING", True)


# =============================================================================
# Backtest / diagnostics
# =============================================================================

BACKTEST_ENABLED = _get_bool("BACKTEST_ENABLED", False)
BACKTEST_SOURCE = _get_str("BACKTEST_SOURCE", "pg").strip().lower()
if BACKTEST_SOURCE not in _ALLOWED_BACKTEST_SOURCE:
    _warn("[CONFIG] BACKTEST_SOURCE=%r invalid; using 'pg'", BACKTEST_SOURCE)
    BACKTEST_SOURCE = "pg"
BACKTEST_MAX_WORKERS = _clamp_i(_get_int("BACKTEST_MAX_WORKERS", 4), 1, 256)

DIAG_BT_DAYS_BACK = _clamp_i(_get_int("DIAG_BT_DAYS_BACK", 5), 0, 3650)
DIAG_BT_LIMIT_CANDIDATES = _clamp_i(_get_int("DIAG_BT_LIMIT_CANDIDATES", 200), 1, 10_000_000)

BT_DAYS_BACK = _get_str("BT_DAYS_BACK", "")
BT_LIMIT_CANDIDATES = _get_str("BT_LIMIT_CANDIDATES", "")


# =============================================================================
# Compat exports / aliases (API endpoints)
# =============================================================================

RAYDIUM_PAIRS_API = RAYDIUM_API_URL
DEXSCREENER_TOKENS_API = DEXSCREENER_API_URL
PUMPFUN_TOKENS_API = PUMPFUN_API_URL


# =============================================================================
# Validation (STRICT_CONFIG gate) — required vs optional + contradictions
# =============================================================================

def _collect_issues() -> Tuple[List[str], List[str]]:
    """
    Returns (errors, warnings).
    In STRICT_CONFIG: any errors raise immediately.
    """
    errors: List[str] = []
    warns: List[str] = []

    # wallet: either private key OR keypair path should exist
    if not WALLET_PRIVATE_KEY and not KEYPAIR_PATH:
        errors.append("Missing wallet credentials: WALLET_PRIVATE_KEY (or KEYPAIR_PATH)")

    # Redis: required for queues/state in your architecture
    if not REDIS_URL:
        errors.append("Missing REDIS_URL")

    # Alerts only if enabled
    if ENABLE_TELEGRAM and (not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID):
        errors.append("ENABLE_TELEGRAM=1 but TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID missing")

    if ENABLE_DISCORD and not DISCORD_WEBHOOK:
        errors.append("ENABLE_DISCORD=1 but DISCORD_WEBHOOK missing")

    # Notion only if enabled
    if ENABLE_NOTION_LOGGING and (not NOTION_TOKEN or not NOTION_DATABASE_ID):
        errors.append("ENABLE_NOTION_LOGGING=1 but NOTION_TOKEN/NOTION_DATABASE_ID missing")

    # Postgres only if enabled and required by selected mode
    if PG_ENABLED and (POSITIONS_BACKEND == "pg" or BACKTEST_SOURCE == "pg") and not POSTGRES_DSN:
        errors.append("PG enabled/needed but POSTGRES_DSN (or PG_DSN/PG_DSN_INTERNAL/PG_DSN_EXTERNAL) missing")

    # OpenAI only if AI enabled
    if AI_ENABLED and not OPENAI_API_KEY:
        warns.append("AI_ENABLED=1 but OPENAI_API_KEY missing (AI layers should fall back safely)")

    # Contradictions / footguns
    if KILL_SWITCH and not MAIN_VALIDATE_ONLY:
        warns.append("KILL_SWITCH=1 will stop trading; consider MAIN_VALIDATE_ONLY=1 when testing deploys")

    if DISABLE_SCANNERS and (ENABLE_RAYDIUM_SCANNER or ENABLE_PUMPFUN_SCANNER or ENABLE_DEXSCREENER_SCANNER):
        warns.append("DISABLE_SCANNERS=1 overrides scanner enables (expected)")

    if DISABLE_BUYS and AUTO_BUY_ENABLED:
        warns.append("DISABLE_BUYS=1 overrides AUTO_BUY_ENABLED (expected)")

    if DISABLE_SELLS and AUTO_SELL_ENABLED:
        warns.append("DISABLE_SELLS=1 overrides AUTO_SELL_ENABLED (expected)")

    if ENTRY_MAX_BUY_SOL < ENTRY_MIN_BUY_SOL:
        errors.append("ENTRY_MAX_BUY_SOL < ENTRY_MIN_BUY_SOL (invalid sizing)")

    if PF_SCAN_MAX_LIQ_USD and PF_SCAN_MAX_LIQ_USD < PF_SCAN_MIN_LIQ_USD:
        errors.append("PF_SCAN_MAX_LIQ_USD < PF_SCAN_MIN_LIQ_USD (invalid filter bounds)")

    if PF_SCAN_MAX_MCAP_USD and PF_SCAN_MAX_MCAP_USD < PF_SCAN_MIN_MCAP_USD:
        errors.append("PF_SCAN_MAX_MCAP_USD < PF_SCAN_MIN_MCAP_USD (invalid filter bounds)")

    if IS_PROD and DRYRUN:
        warns.append("ENV is prod but DRY_RUN=1 (you will not trade)")

    if COMMITMENT not in _ALLOWED_COMMITMENT:
        errors.append(f"COMMITMENT={COMMITMENT!r} invalid")

    # Jupiter sanity
    if not JUP_BASE_URL.startswith("http"):
        errors.append("JUP_BASE_URL must be http(s) URL")

    return errors, warns


_CONFIG_ERRORS, _CONFIG_WARNS = _collect_issues()

if _CONFIG_WARNS:
    _warn("[CONFIG] Warnings: %s", " | ".join(_CONFIG_WARNS))

if _CONFIG_ERRORS and STRICT_CONFIG:
    raise EnvironmentError("[CONFIG] Errors: " + " | ".join(_CONFIG_ERRORS))
elif _CONFIG_ERRORS:
    _warn("[CONFIG] Errors (non-strict): %s", " | ".join(_CONFIG_ERRORS))


# =============================================================================
# Summary helper (safe for logs)
# =============================================================================

def summarize() -> Dict[str, Any]:
    """
    Safe, redacted snapshot of the main knobs for debugging.
    """
    return {
        "meta": {
            "config_version": CONFIG_VERSION,
            "loaded_at_unix": CONFIG_LOADED_AT_UNIX,
            "git_commit": (GIT_COMMIT[:12] if GIT_COMMIT else ""),
        },
        "env": {"app": APP_NAME, "env": ENV, "log_level": LOG_LEVEL, "phase": MASTER_PHASE},
        "orchestrator": {
            "entry_workers": ENTRY_WORKERS,
            "cand_q_max": CANDIDATE_QUEUE_MAX,
            "tick_q_max": TICK_QUEUE_MAX,
            "queue_drop_policy": QUEUE_DROP_POLICY,
            "supervisor_sec": SUPERVISOR_SEC,
            "restart_on_crash": MAIN_RESTART_ON_CRASH,
            "threadify_sync_modules": MAIN_THREADIFY_SYNC_MODULES,
            "fast_fail": MAIN_FAST_FAIL,
            "validate_only": MAIN_VALIDATE_ONLY,
        },
        "kill": {
            "kill_switch": KILL_SWITCH,
            "disable_buys": DISABLE_BUYS,
            "disable_sells": DISABLE_SELLS,
            "disable_scanners": DISABLE_SCANNERS,
            "pause_trading": PAUSE_TRADING,
            "kill_file": bool(KILL_SWITCH_FILE),
        },
        "rpc": {"rpc_url": RPC_URL, "rpc_urls": RPC_URLS[:3], "ws": WEBSOCKET_URL, "commitment": COMMITMENT, "jito": ENABLE_JITO},
        "wallet": {"pubkey": WALLET_PUBKEY, "has_privkey": bool(WALLET_PRIVATE_KEY), "keypair_path": KEYPAIR_PATH},
        "toggles": {"dry_run": DRYRUN, "auto_buy": AUTO_BUY_ENABLED, "auto_sell": AUTO_SELL_ENABLED},
        "scanners": {
            "raydium": ENABLE_RAYDIUM_SCANNER,
            "pumpfun": ENABLE_PUMPFUN_SCANNER,
            "dexscreener": ENABLE_DEXSCREENER_SCANNER,
            "scanner_period_sec": SCANNER_PERIOD_SEC,
            "min_liq_usd": MIN_LIQ_USD,
        },
        "risk": {
            "profile": RISK_PROFILE,
            "max_open_positions": RISK_MAX_OPEN_POSITIONS,
            "max_parallel_buys": RISK_MAX_PARALLEL_BUYS,
            "max_daily_dd_pct": RISK_MAX_DAILY_DRAWDOWN_PCT,
            "stop_after_profit_pct": STOP_AFTER_DAILY_PROFIT_PCT,
            "daily_spend_cap_pct": DAILY_SPEND_CAP_PCT,
        },
        "ai": {
            "enabled": AI_ENABLED,
            "entry_enabled": AI_ENTRY_ENABLED,
            "entry_model": AI_ENTRY_MODEL,
            "score_gate": AI_SCORE_GATE,
            "rug_enabled": AI_RUG_ENABLED,
            "sell_enabled": AI_SELL_ENABLED,
            "feature_version": AI_FEATURE_VERSION,
            "openai_key": _mask(OPENAI_API_KEY),
            "ft_model": FINE_TUNED_MODEL,
        },
        "integrations": {
            "telegram_enabled": ENABLE_TELEGRAM,
            "discord_enabled": ENABLE_DISCORD,
            "notion_enabled": ENABLE_NOTION_LOGGING,
            "birdeye_enabled": BIRDEYE_ENABLED,
            "jup_base": JUP_BASE_URL,
            "jup_key": _mask(JUP_PRO_KEY),
        },
        "storage": {
            "redis": REDIS_URL,
            "redis_prefix": REDIS_PREFIX,
            "pg_enabled": PG_ENABLED,
            "postgres_dsn_set": bool(POSTGRES_DSN),
            "postgres_dsn_redacted": _redact_dsn(POSTGRES_DSN),
            "positions_backend": POSITIONS_BACKEND,
        },
        "config_health": {
            "strict": STRICT_CONFIG,
            "errors": list(_CONFIG_ERRORS),
            "warnings": list(_CONFIG_WARNS),
        },
    }
