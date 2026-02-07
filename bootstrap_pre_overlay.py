#!/usr/bin/env python3
"""
bootstrap_pre_overlay.py

Turn-key readiness + bootstrap for the Solana sniper/daytrader stack.

What it does
============
- Loads .env (if present) and validates critical settings
- Checks connectivity:
  * Helius/Solana RPC
  * Jupiter v6 quote API
  * Redis
  * Postgres (optional: migrate schema)  [uses asyncpg; NO greenlet needed]
  * Notion DB schema sanity
  * Telegram token format (optional)
- Emits a readiness matrix and optional Render start instructions

Usage
=====
  python bootstrap_pre_overlay.py --check-all
  python bootstrap_pre_overlay.py --check-all --migrate-db
  python bootstrap_pre_overlay.py --emit-render-readme

Exit contract
=============
- Exit 0 if all REQUIRED checks pass (optional failures are warnings)
- Exit 2 if any REQUIRED check fails
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import socket
import sys
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

# ──────────────────────────────────────────────────────────────────────────────
# Optional third-party imports (script remains importable without them)
# Keep them typed as Any|None to avoid mypy locking them to Module types.
# ──────────────────────────────────────────────────────────────────────────────

find_dotenv: Any | None = None
load_dotenv: Any | None = None
aiohttp: Any | None = None
redis_asyncio: Any | None = None
asyncpg: Any | None = None

try:
    from dotenv import find_dotenv as _find_dotenv, load_dotenv as _load_dotenv
except Exception:  # pragma: no cover
    pass
else:
    find_dotenv = _find_dotenv
    load_dotenv = _load_dotenv

try:
    import aiohttp as _aiohttp
except Exception:  # pragma: no cover
    pass
else:
    aiohttp = _aiohttp

try:
    import redis.asyncio as _redis_asyncio
except Exception:  # pragma: no cover
    pass
else:
    redis_asyncio = _redis_asyncio

try:
    import asyncpg as _asyncpg
except Exception:  # pragma: no cover
    pass
else:
    asyncpg = _asyncpg

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────

LOG = logging.getLogger("bootstrap")


def _setup_logging(verbose: bool) -> None:
    if any(isinstance(h, logging.StreamHandler) for h in LOG.handlers):
        LOG.setLevel(logging.DEBUG if verbose else logging.INFO)
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(levelname)s:%(name)s: %(message)s"))
    LOG.addHandler(handler)
    LOG.setLevel(logging.DEBUG if verbose else logging.INFO)


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name, "").strip().lower()
    if v in _TRUE:
        return True
    if v in _FALSE:
        return False
    return default


def _looks_like_hosted_env() -> bool:
    hosted_keys = (
        "RENDER",
        "RENDER_SERVICE_ID",
        "RENDER_SERVICE_NAME",
        "DYNO",
        "K_SERVICE",
        "AWS_EXECUTION_ENV",
    )
    if any(os.getenv(k) for k in hosted_keys):
        return True
    return os.getenv("ENV", "").strip().lower() in ("prod", "production")


def _supports_color(no_color: bool) -> bool:
    if no_color:
        return False
    return bool(getattr(sys.stdout, "isatty", lambda: False)())


def _c(code: str, s: str, enabled: bool) -> str:
    if not enabled:
        return s
    return f"\033[{code}m{s}\033[0m"


def _red(s: str, enabled: bool) -> str:
    return _c("31", s, enabled)


def _green(s: str, enabled: bool) -> str:
    return _c("32", s, enabled)


def _yellow(s: str, enabled: bool) -> str:
    return _c("33", s, enabled)


def _bold(s: str, enabled: bool) -> str:
    return _c("1", s, enabled)


def _redact_value(key: str, value: str) -> str:
    if not value:
        return "<set me>"
    k = key.upper()
    is_secret = any(
        frag in k for frag in ("PRIVATE", "SECRET", "TOKEN", "API_KEY", "PASSWORD", "DSN")
    )
    if not is_secret:
        return value
    if len(value) <= 8:
        return "<redacted>"
    if value.lstrip().startswith("[") and value.rstrip().endswith("]"):
        return "<redacted>"
    return f"{value[:3]}…{value[-3:]}"


async def _safe_read_json(resp: Any) -> tuple[dict[str, Any], str]:
    text = ""
    try:
        text = await resp.text()
    except Exception:
        text = ""
    try:
        data = await resp.json(content_type=None)
        if isinstance(data, dict):
            return data, text[:240]
        return {"_non_dict_json": True}, text[:240]
    except Exception:
        return {"_json_parse_failed": True}, text[:240]


def _pick_pg_dsn_raw() -> str:
    return (
        os.getenv("PG_DSN") or os.getenv("PG_DSN_INTERNAL") or os.getenv("PG_DSN_EXTERNAL") or ""
    ).strip()


def _normalize_pg_dsn_for_asyncpg(raw: str) -> str:
    """
    asyncpg expects postgres:// or postgresql:// style DSNs.
    If someone stored postgresql+asyncpg://, convert it.
    """
    dsn = raw.strip()
    if not dsn:
        return dsn
    if dsn.startswith("postgresql+asyncpg://"):
        return "postgresql://" + dsn[len("postgresql+asyncpg://") :]
    return dsn


def _dns_probe(url: str) -> tuple[bool, str]:
    """
    Best-effort DNS probe so we can emit a clean warning when DNS is the issue.
    """
    try:
        host = urlparse(url).hostname
        if not host:
            return False, "invalid URL (no hostname)"
        socket.getaddrinfo(host, 443)
        return True, "dns_ok"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


# ──────────────────────────────────────────────────────────────────────────────
# Data structures
# ──────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class CheckResult:
    name: str
    ok: bool
    detail: str = ""
    severity: str = "required"  # required | optional


# ──────────────────────────────────────────────────────────────────────────────
# Env validation
# ──────────────────────────────────────────────────────────────────────────────


def validate_env() -> list[CheckResult]:
    out: list[CheckResult] = []

    wp = os.getenv("WALLET_PUBKEY", "").strip()
    wpk = os.getenv("WALLET_PRIVATE_KEY", "").strip()
    out.append(CheckResult("WALLET_PUBKEY", bool(wp), "set" if wp else "missing"))
    out.append(CheckResult("WALLET_PRIVATE_KEY", bool(wpk), "set" if wpk else "missing"))

    rpc = (os.getenv("HELIUS_RPC_URL") or os.getenv("RPC_URL") or "").strip()
    out.append(CheckResult("HELIUS_RPC_URL/RPC_URL", bool(rpc), "set" if rpc else "missing"))

    red_url = os.getenv("REDIS_URL", "").strip()
    out.append(CheckResult("REDIS_URL", bool(red_url), "set" if red_url else "missing"))

    pg_enabled = env_bool("PG_ENABLED", False)
    out.append(
        CheckResult("PG_ENABLED", True, "true" if pg_enabled else "false", severity="optional")
    )
    if pg_enabled:
        pg_dsn = (os.getenv("PG_DSN") or "").strip()
        pg_dsn_i = (os.getenv("PG_DSN_INTERNAL") or "").strip()
        pg_dsn_e = (os.getenv("PG_DSN_EXTERNAL") or "").strip()
        any_dsn = bool(pg_dsn or pg_dsn_i or pg_dsn_e)
        detail = (
            f"PG_DSN={'set' if pg_dsn else 'empty'} | "
            f"INTERNAL={'set' if pg_dsn_i else 'empty'} | "
            f"EXTERNAL={'set' if pg_dsn_e else 'empty'}"
        )
        out.append(CheckResult("PG DSNs", any_dsn, detail))

    notion_key = (os.getenv("NOTION_API_KEY") or "").strip()
    notion_db = (os.getenv("NOTION_DB_TRADES") or "").strip()
    out.append(
        CheckResult(
            "NOTION_API_KEY",
            True,
            "set" if notion_key else "missing (ok if disabled)",
            severity="optional",
        )
    )
    out.append(
        CheckResult(
            "NOTION_DB_TRADES",
            True,
            "set" if notion_db else "missing (ok if disabled)",
            severity="optional",
        )
    )

    tg_token = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    tg_chat = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()
    out.append(
        CheckResult(
            "TELEGRAM_BOT_TOKEN",
            True,
            "set" if tg_token else "missing (ok if disabled)",
            severity="optional",
        )
    )
    out.append(
        CheckResult(
            "TELEGRAM_CHAT_ID",
            True,
            "set" if tg_chat else "missing (ok if disabled)",
            severity="optional",
        )
    )

    out.append(CheckResult("JUPITER v6", True, "public; no API key required", severity="optional"))

    openai_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    out.append(
        CheckResult(
            "OPENAI_API_KEY",
            True,
            "set" if openai_key else "missing (ok if disabled)",
            severity="optional",
        )
    )

    return out


# ──────────────────────────────────────────────────────────────────────────────
# Connectivity checks
# ──────────────────────────────────────────────────────────────────────────────


async def check_rpc(session: Any) -> CheckResult:
    rpc = (os.getenv("HELIUS_RPC_URL") or os.getenv("RPC_URL") or "").strip()
    if not rpc:
        return CheckResult("RPC ping", False, "RPC env missing")
    payload = {"jsonrpc": "2.0", "id": 1, "method": "getHealth"}
    try:
        async with session.post(rpc, json=payload) as resp:
            data, snippet = await _safe_read_json(resp)
            ok = (resp.status == 200) and (
                str(data.get("result", "")).lower() == "ok" or "ok" in snippet.lower()
            )
            detail = f"HTTP {resp.status} result={data.get('result', '')!r}"
            return CheckResult("RPC ping", ok, detail)
    except Exception as e:
        return CheckResult("RPC ping", False, f"{type(e).__name__}: {e}")


async def check_jupiter(session: Any) -> CheckResult:
    """
    Optional check. If DNS is broken locally, show a warning (not a hard fail).
    Allow override with:
      JUPITER_QUOTE_URL (full URL) OR JUPITER_QUOTE_BASE (base host)
    """
    usdc = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    sol = "So11111111111111111111111111111111111111112"

    base = (os.getenv("JUPITER_QUOTE_BASE") or "https://quote-api.jup.ag").strip().rstrip("/")
    override = (os.getenv("JUPITER_QUOTE_URL") or "").strip()
    url = override or (
        f"{base}/v6/quote?inputMint={usdc}&outputMint={sol}&amount=100000&swapMode=ExactIn"
    )

    dns_ok, dns_detail = _dns_probe(url)
    if not dns_ok:
        return CheckResult("Jupiter v6", False, f"DNS: {dns_detail}", severity="optional")

    try:
        async with session.get(url) as resp:
            data, snippet = await _safe_read_json(resp)
            ok = bool(data.get("outAmount") or data.get("routePlan"))
            keys = list(data.keys())[:8]
            detail = f"HTTP {resp.status} ok={ok} keys={keys} snippet={snippet[:80]!r}"
            return CheckResult("Jupiter v6", ok and resp.status == 200, detail, severity="optional")
    except Exception as e:
        return CheckResult("Jupiter v6", False, f"{type(e).__name__}: {e}", severity="optional")


async def check_redis() -> CheckResult:
    url = (os.getenv("REDIS_URL") or "").strip()
    if not url:
        return CheckResult("Redis", True, "disabled (no URL)", severity="optional")
    if redis_asyncio is None:
        return CheckResult("Redis", False, "redis package not installed")

    try:
        r = redis_asyncio.from_url(
            url,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
        await r.ping()

        # redis-py deprecates close() in favor of aclose()
        aclose = getattr(r, "aclose", None)
        if callable(aclose):
            await aclose()
        else:  # pragma: no cover
            await r.close()

        return CheckResult("Redis", True, "PING ok")
    except Exception as e:
        return CheckResult("Redis", False, f"{type(e).__name__}: {e}")


async def check_pg_connect(timeout: int) -> CheckResult:
    if not env_bool("PG_ENABLED", False):
        return CheckResult("Postgres", True, "disabled", severity="optional")

    if asyncpg is None:
        return CheckResult("Postgres", False, "asyncpg not installed", severity="optional")

    dsn = _normalize_pg_dsn_for_asyncpg(_pick_pg_dsn_raw())
    if not dsn:
        return CheckResult("Postgres", False, "no DSN", severity="optional")

    try:
        conn = await asyncpg.connect(dsn=dsn, timeout=timeout)
        try:
            await conn.execute("SELECT 1")
        finally:
            await conn.close()
        return CheckResult("Postgres", True, "connected", severity="optional")
    except Exception as e:
        return CheckResult("Postgres", False, f"{type(e).__name__}: {e}", severity="optional")


async def migrate_pg_schema(timeout: int) -> CheckResult:
    if not env_bool("PG_ENABLED", False):
        return CheckResult("PG migrate", True, "disabled", severity="optional")

    if asyncpg is None:
        return CheckResult("PG migrate", False, "asyncpg not installed", severity="optional")

    dsn = _normalize_pg_dsn_for_asyncpg(_pick_pg_dsn_raw())
    if not dsn:
        return CheckResult("PG migrate", False, "no DSN", severity="optional")

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
        conn = await asyncpg.connect(dsn=dsn, timeout=timeout)
        try:
            for s in stmts:
                await conn.execute(s)
        finally:
            await conn.close()
        return CheckResult("PG migrate", True, "trades table ready", severity="optional")
    except Exception as e:
        return CheckResult("PG migrate", False, f"{type(e).__name__}: {e}", severity="optional")


async def notion_db_check(session: Any) -> CheckResult:
    key = (os.getenv("NOTION_API_KEY") or "").strip()
    dbid = (os.getenv("NOTION_DB_TRADES") or "").strip()
    if not key or not dbid:
        return CheckResult("Notion DB", True, "disabled", severity="optional")

    try:
        headers = {
            "Authorization": f"Bearer {key}",
            "Notion-Version": "2022-06-28",
            "Content-Type": "application/json",
        }
        url = f"https://api.notion.com/v1/databases/{dbid}"
        async with session.get(url, headers=headers) as resp:
            data, snippet = await _safe_read_json(resp)
            if resp.status != 200:
                msg = str(data.get("message") or snippet[:140])
                return CheckResult(
                    "Notion DB", False, f"HTTP {resp.status}: {msg}", severity="optional"
                )

            props = data.get("properties") or {}
            if not isinstance(props, dict):
                return CheckResult(
                    "Notion DB", False, "properties missing/invalid", severity="optional"
                )

            prop_keys = set(props.keys())
            title_prop = os.getenv("NOTION_TITLE_PROP", "Token")
            required = {
                title_prop,
                "Mint",
                "Entry Price",
                "Current Price",
                "Profit",
                "Reason",
                "Timestamp",
            }
            missing = sorted(p for p in required if p not in prop_keys)
            if missing:
                return CheckResult(
                    "Notion DB",
                    False,
                    f"missing properties: {', '.join(missing)}",
                    severity="optional",
                )

            return CheckResult("Notion DB", True, "properties OK", severity="optional")
    except Exception as e:
        return CheckResult("Notion DB", False, f"{type(e).__name__}: {e}", severity="optional")


def check_telegram_format() -> CheckResult:
    tok = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    chat = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()
    if not tok or not chat:
        return CheckResult("Telegram", True, "disabled", severity="optional")

    ok_tok = bool(re.fullmatch(r"\d+:[A-Za-z0-9_\-]+", tok))
    ok_chat = bool(re.fullmatch(r"-?\d+", chat))
    ok = ok_tok and ok_chat
    detail = "token/chat look valid" if ok else "format looks off"
    return CheckResult("Telegram", ok, detail, severity="optional")


# ──────────────────────────────────────────────────────────────────────────────
# Render README helper
# ──────────────────────────────────────────────────────────────────────────────

DEFAULT_RENDER_START = (
    "python -m scripts.sniper_bot --mode prod --enable-daytrader --enable-sniper --log-level INFO"
)

ENV_KEYS: list[str] = [
    "WALLET_PUBKEY",
    "WALLET_PRIVATE_KEY",
    "HELIUS_RPC_URL",
    "RPC_URL",
    "REDIS_URL",
    "PG_ENABLED",
    "PG_DSN",
    "PG_DSN_INTERNAL",
    "PG_DSN_EXTERNAL",
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_CHAT_ID",
    "NOTION_API_KEY",
    "NOTION_DB_TRADES",
    "NOTION_TITLE_PROP",
    "NOTION_PROP_TOKEN",
    "NOTION_PROP_ENTRY",
    "NOTION_PROP_CURRENT",
    "NOTION_PROP_TIME",
    "OPENAI_API_KEY",
    "OPENAI_MODEL",
    "OPENAI_BASE_URL",
    "ENABLE_WHALE_FILTER",
    "DYNAMIC_WHALE_DISCOVERY",
    "CURATED_WHALE_WALLETS",
]


def emit_render_readme(color: bool) -> None:
    start_cmd = (os.getenv("RENDER_START_CMD") or DEFAULT_RENDER_START).strip()

    print("\n" + "=" * 72)
    print("Render: Start Command & ENV checklist")
    print("=" * 72)
    print("\nStart Command:\n")
    print(f"    {start_cmd}\n")
    print("ENV (set these in Render → Environment):\n")

    for k in ENV_KEYS:
        v = (os.getenv(k) or "").strip()
        shown = _redact_value(k, v)
        mark = "✓" if v else "⛔"
        mark = _green(mark, color) if v else _red(mark, color)
        print(f"  {mark} {k}={shown}")

    print("")


# ──────────────────────────────────────────────────────────────────────────────
# Orchestrator
# ──────────────────────────────────────────────────────────────────────────────


def _icon_for(result: CheckResult, color: bool) -> str:
    if result.ok:
        return _green("✔", color)
    if result.severity == "optional":
        return _yellow("⚠", color)
    return _red("✖", color)


async def run_checks(args: argparse.Namespace) -> tuple[int, list[CheckResult], dict[str, Any]]:
    color = _supports_color(bool(args.no_color))
    results: list[CheckResult] = []

    results.extend(validate_env())

    optional_warnings = 0
    required_failures = 0

    if args.check_all:
        results.append(await check_redis())

        if aiohttp is None:
            results.append(CheckResult("aiohttp", False, "aiohttp not installed"))
        else:
            timeout = int(args.timeout)
            ct = aiohttp.ClientTimeout(total=timeout)
            async with aiohttp.ClientSession(timeout=ct) as session:
                results.append(await check_rpc(session))
                results.append(await check_jupiter(session))
                results.append(await check_pg_connect(timeout))
                if args.migrate_db:
                    results.append(await migrate_pg_schema(timeout))
                if args.notion_check:
                    results.append(await notion_db_check(session))
                results.append(check_telegram_format())

    print("\n" + "=" * 72)
    print("Readiness Matrix")
    print("=" * 72)

    for r in results:
        icon = _icon_for(r, color)
        name = _bold(r.name, color)
        detail = f" — {r.detail}" if r.detail else ""
        sev = "" if r.severity == "required" else _yellow(" [optional]", color)
        print(f"{icon} {name}{sev}{detail}")

        if not r.ok:
            if r.severity == "required":
                required_failures += 1
            else:
                optional_warnings += 1

    print("")

    if args.emit_render_readme:
        emit_render_readme(color)

    summary: dict[str, Any] = {
        "ok": required_failures == 0,
        "required_failures": required_failures,
        "optional_warnings": optional_warnings,
        "hosted_env_detected": _looks_like_hosted_env(),
        "timeout_sec": int(args.timeout),
        "checks": [
            {"name": r.name, "ok": r.ok, "detail": r.detail, "severity": r.severity}
            for r in results
        ],
    }

    return (0 if required_failures == 0 else 2), results, summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Bootstrap/Readiness for sniper/daytrader stack")
    p.add_argument("--check-all", action="store_true", help="run all connectivity checks")
    p.add_argument("--migrate-db", action="store_true", help="create/alter trades table & indexes")
    p.add_argument("--notion-check", action="store_true", help="validate Notion properties")
    p.add_argument(
        "--emit-render-readme", action="store_true", help="print Render Start & env list"
    )
    p.add_argument("--timeout", type=int, default=12, help="HTTP timeouts (sec)")
    p.add_argument("--verbose", action="store_true", help="enable debug logging")
    p.add_argument("--no-color", action="store_true", help="disable ANSI colors")
    p.add_argument("--json", action="store_true", help="emit machine-readable JSON summary")
    return p.parse_args(argv)


def maybe_load_dotenv() -> None:
    if load_dotenv is None or find_dotenv is None:
        return
    try:
        path = find_dotenv()
        if path:
            load_dotenv(path)
            LOG.info("loaded .env from %s", path)
    except Exception as e:
        LOG.warning(".env load failed: %s", e)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    _setup_logging(bool(args.verbose))
    maybe_load_dotenv()

    exit_code, _results, summary = asyncio.run(run_checks(args))

    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
