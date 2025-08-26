#!/usr/bin/env python3
"""
bootstrap_pro_overlay.py

Turn-key readiness + bootstrap for the Solana sniper/daytrader stack.

What it does
============
- Loads .env (if present) and validates critical settings
- Checks connectivity:
  * Helius/Solana RPC
  * Jupiter v6 quote API
  * Redis
  * Postgres (optional: migrate schema)
  * Notion DB schema sanity
  * Telegram token format (optional send ping)
- Emits a readiness matrix and optional Render start instructions

Usage
=====
  python bootstrap_pro_overlay.py --check-all
  python bootstrap_pro_overlay.py --check-all --migrate-db
  python bootstrap_pro_overlay.py --emit-render-readme

Flags
=====
  --check-all          : run every connectivity check
  --migrate-db         : create/alter 'trades' table + indexes if PG enabled
  --notion-check       : validate Notion DB properties match your mapping
  --emit-render-readme : print a snippet you can paste into README for Render
  --timeout SEC        : network timeout (default 12)
  --verbose            : more logs
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# third-party
try:
    from dotenv import load_dotenv, find_dotenv
except Exception:
    load_dotenv = None
    find_dotenv = None

import aiohttp

try:
    import redis.asyncio as aioredis
except Exception:
    aioredis = None

try:
    from sqlalchemy.ext.asyncio import create_async_engine
except Exception:
    create_async_engine = None


# ---------------------------
# Logging
# ---------------------------
LOG = logging.getLogger("bootstrap")
_hdlr = logging.StreamHandler(sys.stdout)
_hdlr.setFormatter(logging.Formatter("%(levelname)s:%(name)s: %(message)s"))
LOG.addHandler(_hdlr)
LOG.setLevel(logging.INFO)


# ---------------------------
# Helpers
# ---------------------------
def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name, "").strip().lower()
    if v in ("1", "true", "yes", "y", "on"):
        return True
    if v in ("0", "false", "no", "n", "off"):
        return False
    return default


def red(s: str) -> str:
    return f"\033[31m{s}\033[0m"


def green(s: str) -> str:
    return f"\033[32m{s}\033[0m"


def yellow(s: str) -> str:
    return f"\033[33m{s}\033[0m"


def bold(s: str) -> str:
    return f"\033[1m{s}\033[0m"


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: str = ""


# ---------------------------
# Env validation
# ---------------------------
def validate_env() -> List[CheckResult]:
    out: List[CheckResult] = []

    # Wallet
    wp = os.getenv("WALLET_PUBKEY", "")
    wpk = os.getenv("WALLET_PRIVATE_KEY", "")
    out.append(CheckResult("WALLET_PUBKEY", bool(wp), f"value={'set' if wp else 'missing'}"))
    out.append(CheckResult("WALLET_PRIVATE_KEY", bool(wpk), f"value={'set' if wpk else 'missing'}"))

    # RPC
    rpc = os.getenv("HELIUS_RPC_URL", "") or os.getenv("RPC_URL", "")
    out.append(CheckResult("HELIUS_RPC_URL/RPC_URL", bool(rpc), f"value={'set' if rpc else 'missing'}"))

    # Redis
    red_url = os.getenv("REDIS_URL", "")
    out.append(CheckResult("REDIS_URL", bool(red_url), f"value={'set' if red_url else 'missing'}"))

    # Postgres
    pg_enabled = env_bool("PG_ENABLED", False)
    out.append(CheckResult("PG_ENABLED", True, f"value={'true' if pg_enabled else 'false'}"))
    if pg_enabled:
        pg_dsn = os.getenv("PG_DSN", "")
        pg_dsn_i = os.getenv("PG_DSN_INTERNAL", "")
        pg_dsn_e = os.getenv("PG_DSN_EXTERNAL", "")
        any_dsn = bool(pg_dsn or pg_dsn_i or pg_dsn_e)
        detail = (
            f"PG_DSN={'set' if pg_dsn else 'empty'} | "
            f"INTERNAL={'set' if pg_dsn_i else 'empty'} | "
            f"EXTERNAL={'set' if pg_dsn_e else 'empty'}"
        )
        out.append(CheckResult("PG DSNs", any_dsn, detail))

    # Notion
    notion_key = os.getenv("NOTION_API_KEY", "")
    notion_db = os.getenv("NOTION_DB_TRADES", "")
    out.append(CheckResult("NOTION_API_KEY", True, f"value={'set' if notion_key else 'missing (ok if disabled)'}"))
    out.append(CheckResult("NOTION_DB_TRADES", True, f"value={'set' if notion_db else 'missing (ok if disabled)'}"))

    # Telegram
    tg_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    tg_chat = os.getenv("TELEGRAM_CHAT_ID", "")
    out.append(CheckResult("TELEGRAM_BOT_TOKEN", True, f"value={'set' if tg_token else 'missing (ok if disabled)'}"))
    out.append(CheckResult("TELEGRAM_CHAT_ID", True, f"value={'set' if tg_chat else 'missing (ok if disabled)'}"))

    # Jupiter
    out.append(CheckResult("JUPITER v6", True, "public; no API key required"))

    # OpenAI (optional)
    openai_key = os.getenv("OPENAI_API_KEY", "")
    out.append(CheckResult("OPENAI_API_KEY", True, f"value={'set' if openai_key else 'missing (ok if disabled)'}"))

    return out


# ---------------------------
# Connectivity checks
# ---------------------------
async def check_rpc(session: aiohttp.ClientSession, timeout: int = 12) -> CheckResult:
    rpc = os.getenv("HELIUS_RPC_URL") or os.getenv("RPC_URL")
    if not rpc:
        return CheckResult("RPC ping", False, "RPC env missing")
    try:
        payload = {"jsonrpc": "2.0", "id": 1, "method": "getHealth"}
        async with session.post(rpc, json=payload, timeout=timeout) as resp:
            txt = await resp.text()
            ok = resp.status == 200 and "ok" in txt.lower()
            return CheckResult("RPC ping", ok, f"HTTP {resp.status}: {txt[:120]}")
    except Exception as e:
        return CheckResult("RPC ping", False, f"{e}")


async def check_jupiter(session: aiohttp.ClientSession, timeout: int = 12) -> CheckResult:
    # USDC -> SOL small quote
    usdc = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    sol = "So11111111111111111111111111111111111111112"
    url = (
        "https://quote-api.jup.ag/v6/quote"
        f"?inputMint={usdc}&outputMint={sol}&amount=100000&swapMode=ExactIn"
    )
    try:
        async with session.get(url, timeout=timeout) as resp:
            data = await resp.json()
            ok = bool(data.get("outAmount") or data.get("routePlan"))
            return CheckResult("Jupiter v6", ok, f"HTTP {resp.status} keys={list(data.keys())[:6]}")
    except Exception as e:
        return CheckResult("Jupiter v6", False, f"{e}")


async def check_redis() -> CheckResult:
    url = os.getenv("REDIS_URL", "")
    if not url:
        return CheckResult("Redis", True, "disabled (no URL)")
    if aioredis is None:
        return CheckResult("Redis", False, "redis package not installed")
    try:
        r = aioredis.from_url(url, decode_responses=True)
        await r.ping()
        await r.close()
        return CheckResult("Redis", True, "PING ok")
    except Exception as e:
        return CheckResult("Redis", False, f"{e}")


async def check_pg_connect() -> CheckResult:
    if not env_bool("PG_ENABLED", False):
        return CheckResult("Postgres", True, "disabled")
    dsn = os.getenv("PG_DSN") or os.getenv("PG_DSN_INTERNAL") or os.getenv("PG_DSN_EXTERNAL")
    if not dsn:
        return CheckResult("Postgres", False, "no DSN")
    if create_async_engine is None:
        return CheckResult("Postgres", False, "sqlalchemy[asyncpg] not installed")
    try:
        eng = create_async_engine(dsn, future=True)
        async with eng.connect() as conn:
            await conn.execute("SELECT 1")
        await eng.dispose()
        return CheckResult("Postgres", True, "connected")
    except Exception as e:
        return CheckResult("Postgres", False, f"{e}")


async def migrate_pg_schema() -> CheckResult:
    if not env_bool("PG_ENABLED", False):
        return CheckResult("PG migrate", True, "disabled")
    dsn = os.getenv("PG_DSN") or os.getenv("PG_DSN_INTERNAL") or os.getenv("PG_DSN_EXTERNAL")
    if not dsn:
        return CheckResult("PG migrate", False, "no DSN")
    if create_async_engine is None:
        return CheckResult("PG migrate", False, "sqlalchemy[asyncpg] not installed")
    stmts = [
        """
        CREATE TABLE IF NOT EXISTS trades (
          id SERIAL PRIMARY KEY,
          ts TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
          token VARCHAR(64),
          symbol VARCHAR(64),
          side VARCHAR(16),
          amount DOUBLE PRECISION,
          entry DOUBLE PRECISION,
          current DOUBLE PRECISION,
          profit DOUBLE PRECISION,
          reason VARCHAR(64),
          signature VARCHAR(128)
        );
        """,
        "CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades (ts);",
        "CREATE INDEX IF NOT EXISTS idx_trades_token ON trades (token);",
        "CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades (symbol);",
        "CREATE INDEX IF NOT EXISTS idx_trades_side ON trades (side);",
    ]
    try:
        eng = create_async_engine(dsn, future=True)
        async with eng.begin() as conn:
            for s in stmts:
                await conn.execute(s)
        await eng.dispose()
        return CheckResult("PG migrate", True, "trades table ready")
    except Exception as e:
        return CheckResult("PG migrate", False, f"{e}")


async def notion_db_check(session: aiohttp.ClientSession, timeout: int = 12) -> CheckResult:
    key = os.getenv("NOTION_API_KEY", "")
    dbid = os.getenv("NOTION_DB_TRADES", "")
    if not key or not dbid:
        return CheckResult("Notion DB", True, "disabled")
    try:
        headers = {
            "Authorization": f"Bearer {key}",
            "Notion-Version": "2022-06-28",
            "Content-Type": "application/json",
        }
        url = f"https://api.notion.com/v1/databases/{dbid}"
        async with session.get(url, headers=headers, timeout=timeout) as resp:
            data = await resp.json()
            if resp.status != 200:
                return CheckResult("Notion DB", False, f"HTTP {resp.status}: {data.get('message')}")
            props = (data.get("properties") or {}).keys()
            # expected props (configurable via env mapping)
            title_prop = os.getenv("NOTION_TITLE_PROP", "Token")
            req = {title_prop, "Mint", "Entry Price", "Current Price", "Profit", "Reason", "Timestamp"}
            missing = [p for p in req if p not in props]
            if missing:
                return CheckResult("Notion DB", False, f"missing properties: {', '.join(missing)}")
            return CheckResult("Notion DB", True, "properties OK")
    except Exception as e:
        return CheckResult("Notion DB", False, f"{e}")


def check_telegram_format() -> CheckResult:
    tok = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat = os.getenv("TELEGRAM_CHAT_ID", "")
    if not tok or not chat:
        return CheckResult("Telegram", True, "disabled")
    ok = bool(re.fullmatch(r"\d+:[A-Za-z0-9_\-]+", tok)) and bool(re.fullmatch(r"-?\d+", chat))
    detail = "token/chat look valid" if ok else "format looks off"
    return CheckResult("Telegram", ok, detail)


# ---------------------------
# Render README helper
# ---------------------------
RENDER_START = (
    "python -m scripts.sniper_bot --mode prod --enable-daytrader --enable-sniper "
    "--log-level INFO"
)

ENV_KEYS = [
    # critical
    "WALLET_PUBKEY",
    "WALLET_PRIVATE_KEY",
    "HELIUS_RPC_URL",
    "REDIS_URL",
    "PG_ENABLED",
    "PG_DSN",
    "PG_DSN_INTERNAL",
    "PG_DSN_EXTERNAL",
    # telemetry/alerts
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_CHAT_ID",
    "NOTION_API_KEY",
    "NOTION_DB_TRADES",
    "NOTION_TITLE_PROP",
    "NOTION_PROP_TOKEN",
    "NOTION_PROP_ENTRY",
    "NOTION_PROP_CURRENT",
    "NOTION_PROP_TIME",
    # ai
    "OPENAI_API_KEY",
    "OPENAI_MODEL",
    "OPENAI_BASE_URL",
    # strategy
    "ENABLE_WHALE_FILTER",
    "DYNAMIC_WHALE_DISCOVERY",
    "CURATED_WHALE_WALLETS",
]

def emit_render_readme() -> None:
    print("\n" + "=" * 72)
    print("Render: Start Command & ENV checklist")
    print("=" * 72)
    print("\nStart Command:\n")
    print(f"    {RENDER_START}\n")
    print("ENV (set these in Render → Environment):\n")
    for k in ENV_KEYS:
        v = os.getenv(k, "")
        mark = "✓" if v else "⛔"
        print(f"  {mark} {k}={v if v else '<set me>'}")
    print("")


# ---------------------------
# Orchestrator
# ---------------------------
async def run_checks(args: argparse.Namespace) -> int:
    results: List[CheckResult] = []

    # Env pass
    results.extend(validate_env())

    timeout = args.timeout
    async with aiohttp.ClientSession() as session:
        # Connectivity
        if args.check_all:
            results.append(await check_rpc(session, timeout))
            results.append(await check_jupiter(session, timeout))
            results.append(await check_redis())
            results.append(await check_pg_connect())
            if args.migrate_db:
                results.append(await migrate_pg_schema())
            if args.notion_check:
                results.append(await notion_db_check(session, timeout))
            results.append(check_telegram_format())

    # Pretty print
    print("\n" + "=" * 72)
    print("Readiness Matrix")
    print("=" * 72)
    bad = 0
    for r in results:
        icon = green("✔") if r.ok else red("✖")
        detail = f" — {r.detail}" if r.detail else ""
        print(f"{icon} {bold(r.name)}{detail}")
        if not r.ok:
            bad += 1
    print("")

    if args.emit_render_readme:
        emit_render_readme()

    return 0 if bad == 0 else 2


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Bootstrap/Readiness for sniper/daytrader stack")
    p.add_argument("--check-all", action="store_true", help="run all connectivity checks")
    p.add_argument("--migrate-db", action="store_true", help="create/alter trades table & indexes")
    p.add_argument("--notion-check", action="store_true", help="validate Notion properties")
    p.add_argument("--emit-render-readme", action="store_true", help="print Render Start & env list")
    p.add_argument("--timeout", type=int, default=12, help="HTTP timeouts (sec)")
    p.add_argument("--verbose", action="store_true")
    return p.parse_args(argv)


def maybe_load_dotenv():
    if load_dotenv and find_dotenv:
        try:
            path = find_dotenv()
            if path:
                load_dotenv(path)
                LOG.info(f"loaded .env from {path}")
        except Exception as e:
            LOG.warning(f".env load failed: {e}")


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    if args.verbose:
        LOG.setLevel(logging.DEBUG)
    maybe_load_dotenv()
    return asyncio.run(run_checks(args))


if __name__ == "__main__":
    sys.exit(main())
