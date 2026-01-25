# scripts/smoke_core.py
from __future__ import annotations

"""
scripts/smoke_core.py — Smoke: Core Wiring (Phase 0–7 LOCK-IN)
==============================================================

Purpose
-------
A *non-trading* end-to-end "core wiring" smoke test:
  - init_resources() (wallet optional by default)
  - session/http client availability (if exposed)
  - redis set/get roundtrip (optional)
  - entry-cache roundtrip (optional; sqlite legacy OR pg-backed, depending on implementation)
  - pricing sanity (SOL/USD)
  - jupiter quote sanity (optional; no swaps / no transactions)

Design goals
------------
- Deterministic exit codes:
    0 = pass
    2 = fail
  130 = Ctrl-C
- Secret-safe output: aggressive redaction of env secret values and token-ish blobs
- Human logs -> STDERR
- JSON summary -> STDOUT when enabled (preflight-friendly)
- No trades, no signing, no token transfers

Env knobs (optional)
-------------------
SMOKE_CORE_JSON=0|1
SMOKE_CORE_JSON_ONLY=0|1
SMOKE_CORE_TIMEOUT_S=60
SMOKE_CORE_REQUIRE_WALLET=0|1            (default 0)
SMOKE_CORE_SKIP_REDIS=0|1                (default 0)
SMOKE_CORE_SKIP_CACHE=0|1                (default 0)
SMOKE_CORE_SKIP_PRICING=0|1              (default 0)
SMOKE_CORE_SKIP_QUOTE=0|1                (default 0)
SMOKE_CORE_REQUIRE_QUOTE=0|1             (default 0)
SMOKE_CORE_QUOTE_OUT_MINT=<mint>         (default USDC)
SMOKE_CORE_QUOTE_AMOUNT_SOL=0.001        (default 0.001)
SMOKE_CORE_QUOTE_SLIPPAGE_BPS=50         (default 50 bps)
SMOKE_CORE_NO_DOTENV=0|1                 (default 0)
PREFLIGHT_REDACT_KEYS=K1,K2,...

Notes
-----
- This script is intentionally defensive: it will *skip* optional checks if
  the required helper function does not exist in utils_pkg.core_utils.
"""

import argparse
import asyncio
import contextlib
import importlib
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

RC_OK = 0
RC_FAIL = 2
RC_CTRL_C = 130

_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}

SOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"


# =============================================================================
# repo-root bootstrap (direct-run friendly)
# =============================================================================
def _bootstrap_repo_root() -> str:
    try:
        here = Path(__file__).resolve()
    except Exception:
        return ""

    markers = ("pyproject.toml", "requirements.txt", "setup.cfg", ".git")
    candidates = [here.parent, *list(here.parents)]

    root: Path | None = None
    for p in candidates:
        with contextlib.suppress(Exception):
            if any((p / m).exists() for m in markers) or (p / "src").is_dir():
                root = p
                break

    if root is None:
        root = here.parents[1] if len(here.parents) > 1 else here.parent

    root_str = str(root)
    if root_str and root_str not in sys.path:
        sys.path.insert(0, root_str)

    os.environ.setdefault("REPO_ROOT", root_str)
    return root_str


# =============================================================================
# env helpers
# =============================================================================
def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    s = v if v is not None else d
    s = str(s).strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    return s


def _envb(k: str, d: bool = False) -> bool:
    v = _envs(k, "")
    if v == "":
        return bool(d)
    s = v.lower().strip()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return bool(d)


def _envf(k: str, d: float) -> float:
    try:
        return float(_envs(k, str(d)))
    except Exception:
        return float(d)


def _envi(k: str, d: int) -> int:
    try:
        return int(float(_envs(k, str(d))))
    except Exception:
        return int(d)


def _ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


# =============================================================================
# safe stderr writes (Windows cp1252-safe)
# =============================================================================
def _safe_write(file, s: str) -> None:
    try:
        file.write(s)
        file.flush()
        return
    except UnicodeEncodeError:
        enc = getattr(file, "encoding", None) or "utf-8"
        b = s.encode(enc, errors="replace")
        with contextlib.suppress(Exception):
            if hasattr(file, "buffer"):
                file.buffer.write(b)
                file.buffer.flush()
                return
        with contextlib.suppress(Exception):
            file.write(b.decode(enc, errors="replace"))
            file.flush()
            return
    except Exception:
        return


def _hlog(msg: str, *, json_only: bool) -> None:
    if json_only:
        return
    _safe_write(sys.stderr, f"[smoke_core] {_ts()} {msg}\n")


# =============================================================================
# redaction / safety
# =============================================================================
_URL_CRED_RE = re.compile(r"(://)([^/@\s]+)@")
_APIKEY_HDR_RE = re.compile(r"(?i)(x-api-key:\s*)([^\s]+)")
_BEARER_HDR_RE = re.compile(r"(?i)(authorization:\s*bearer\s+)([^\s]+)")
_KV_SECRET_RE = re.compile(
    r"(?i)\b(api[_-]?key|token|secret|password|pass)\b(\s*[:=]\s*)([^\s,;]+)"
)
_LONG_TOKENISH_RE = re.compile(r"\b[A-Za-z0-9_\-]{28,}\b")


def _redact_keys() -> list[str]:
    base = [
        "JUP_PRO_KEY",
        "JUP_API_KEY",
        "OPENAI_API_KEY",
        "TELEGRAM_BOT_TOKEN",
        "DISCORD_TOKEN",
        "DISCORD_WEBHOOK_URL",
        "NOTION_TOKEN",
        "NOTION_DATABASE_ID",
        "WALLET_JSON",
        "WALLET_JSON_B64",
        "WALLET_PRIVATE_KEY",
        "KEYPAIR_B64",
        "KEYPAIR_PATH",
        "DATABASE_URL",
        "POSTGRES_DSN",
        "PG_DSN",
        "PG_DSN_INTERNAL",
        "PG_DSN_EXTERNAL",
        "PNL_DSN",
        "REDIS_URL",
        "REDIS_TLS_URL",
        "RPC_URL",
        "SOL_RPC_URL",
        "SOL_RPC_URLS",
    ]
    extra = [
        p.strip()
        for p in _envs("PREFLIGHT_REDACT_KEYS", "").replace(";", ",").split(",")
        if p.strip()
    ]
    out: list[str] = []
    seen = set()
    for k in base + extra:
        if k and k not in seen:
            out.append(k)
            seen.add(k)
    return out


def _sanitize(x: Any) -> str:
    if x is None:
        return ""
    s = str(x)

    s = _URL_CRED_RE.sub(r"\1***@", s)
    s = _APIKEY_HDR_RE.sub(r"\1***", s)
    s = _BEARER_HDR_RE.sub(r"\1***", s)
    s = _KV_SECRET_RE.sub(r"\1\2***", s)

    for k in _redact_keys():
        v = os.getenv(k, "")
        if v and len(v) >= 6:
            s = s.replace(v, "***")

    if _envb("PREFLIGHT_MASK_LONG_TOKENS", True):
        s = _LONG_TOKENISH_RE.sub("***", s)

    return s


# =============================================================================
# optional dotenv (best-effort)
# =============================================================================
def _maybe_load_dotenv() -> None:
    if _envb("SMOKE_CORE_NO_DOTENV", False) or _envb("SMOKE_INIT_NO_DOTENV", False):
        return
    with contextlib.suppress(Exception):
        from dotenv import load_dotenv  # type: ignore

        load_dotenv(override=False)


# =============================================================================
# helpers: optional call patterns
# =============================================================================
def _has(obj: Any, name: str) -> bool:
    return bool(obj is not None and hasattr(obj, name))


async def _call_init_resources(cu: Any, require_wallet: bool) -> None:
    """
    Support multiple signatures:
      - await init_resources()
      - await init_resources(require_wallet=False)
      - await init_resources(require_wallet=0)
    """
    fn = cu.init_resources
    try:
        await fn(require_wallet=require_wallet)
        return
    except TypeError:
        pass
    try:
        await fn(require_wallet=int(require_wallet))
        return
    except TypeError:
        pass
    await fn()


async def _call_close_resources(cu: Any) -> None:
    if not _has(cu, "close_resources"):
        return
    fn = cu.close_resources
    with contextlib.suppress(TypeError):
        await fn()
        return
    await fn()


async def _probe_session(cu: Any) -> tuple[bool, str]:
    # Many builds expose get_session() for aiohttp session
    if not _has(cu, "get_session"):
        return True, "session_skipped(no_get_session)"
    try:
        s = cu.get_session()
        return (s is not None), f"session={'yes' if s is not None else 'no'}"
    except Exception as e:
        return False, f"session_err:{_sanitize(type(e).__name__)}:{_sanitize(e)}"


async def _probe_redis(cu: Any) -> tuple[bool, str]:
    getter = None
    for name in ("get_redis_async", "get_redis"):
        if _has(cu, name):
            getter = getattr(cu, name)
            break
    if getter is None:
        return True, "redis_skipped(no_getter)"

    try:
        r = await getter()
        if r is None:
            return False, "redis_none"
        key = f"smoke:core:{int(time.time())}"
        if _has(r, "set") and _has(r, "get"):
            await r.set(key, "1", ex=60)
            v = await r.get(key)
            return (str(v) == "1"), "redis_set_get_ok"
        return True, "redis_skipped(no_set_get)"
    except Exception as e:
        return False, f"redis_err:{_sanitize(type(e).__name__)}:{_sanitize(e)}"


async def _probe_entry_cache(cu: Any) -> tuple[bool, str]:
    """
    Entry-cache roundtrip if helpers exist:
      - remember_entry_price(mint, px, unit="usd")
      - get_entry_price(mint)
      - clear_entry_price(mint)

    Works whether the backing store is sqlite (legacy) or pg.
    """
    need = ("remember_entry_price", "get_entry_price", "clear_entry_price")
    if not all(_has(cu, n) for n in need):
        return True, "cache_skipped(missing_funcs)"

    mint = "SMOKE_TEST_MINT"
    try:
        await cu.remember_entry_price(mint, 1.23, unit="usd")
        px = await cu.get_entry_price(mint)
        await cu.clear_entry_price(mint)

        # accept dict or float
        if isinstance(px, dict):
            v = px.get("price") or px.get("px") or px.get("value")
            try:
                v = float(v)
            except Exception:
                v = None
            ok = (v is not None) and (abs(float(v) - 1.23) < 1e-6)
            return ok, "cache_roundtrip_ok(dict)"
        try:
            v = float(px)
        except Exception:
            v = None
        ok = (v is not None) and (abs(float(v) - 1.23) < 1e-6)
        return ok, "cache_roundtrip_ok"
    except Exception as e:
        # best-effort cleanup
        with contextlib.suppress(Exception):
            await cu.clear_entry_price(mint)
        return False, f"cache_err:{_sanitize(type(e).__name__)}:{_sanitize(e)}"


async def _probe_pricing(cu: Any) -> tuple[bool, str]:
    if not _has(cu, "get_sol_usd"):
        return True, "pricing_skipped(no_get_sol_usd)"
    try:
        v = await cu.get_sol_usd()
        vv = float(v or 0.0)
        if vv <= 0:
            return False, f"sol_usd_bad:{vv}"
        return True, f"sol_usd_ok:{vv:.4f}"
    except Exception as e:
        return False, f"pricing_err:{_sanitize(type(e).__name__)}:{_sanitize(e)}"


async def _probe_quote(
    cu: Any,
    out_mint: str,
    amount_sol: float,
    slippage_bps: int,
) -> tuple[bool, str]:
    """
    Quote sanity (no swap).
    Prefer cu.get_jupiter_quote if present; otherwise skip (smoke_lightning handles deeper testing).
    """
    if not _has(cu, "get_jupiter_quote"):
        return True, "quote_skipped(no_get_jupiter_quote)"

    try:
        amt_raw = int(max(1, float(amount_sol)) * 1e9)  # SOL lamports
        slippage_pct = max(0.0, float(slippage_bps) / 10_000.0)

        q = await cu.get_jupiter_quote(SOL_MINT, out_mint, amt_raw, slippage_pct=slippage_pct)
        if not isinstance(q, dict):
            return False, f"quote_bad_type:{type(q).__name__}"

        # very light validation: any route/amount keys
        keys = set(q.keys())
        okish = bool(keys)
        # common keys: outAmount, inAmount, routePlan, contextSlot, otherAmountThreshold
        if not okish:
            return False, "quote_empty_dict"

        return True, "quote_ok"
    except Exception as e:
        return False, f"quote_err:{_sanitize(type(e).__name__)}:{_sanitize(e)}"


def _fix_hints_from_error(err_text: str) -> list[str]:
    blob = (err_text or "").lower()
    hints: list[str] = []
    if "missing env" in blob or "missing env vars" in blob:
        hints.append(
            "Missing environment variables. Verify Render Secrets and .env/.env.extra wiring."
        )
    if "redis" in blob and ("connection" in blob or "refused" in blob or "timeout" in blob):
        hints.append("Redis connectivity issue. Verify REDIS_URL (and TLS) and service uptime.")
    if "postgres" in blob or "asyncpg" in blob:
        hints.append(
            "Postgres connectivity issue. Verify POSTGRES_DSN/PG_DSN and outbound networking."
        )
    if "rpc" in blob and ("timeout" in blob or "blockhash" in blob or "http" in blob):
        hints.append("RPC connectivity issue. Verify RPC_URL/SOL_RPC_URL and provider health.")
    if "unauthorized" in blob or "forbidden" in blob or " 401" in blob or " 403" in blob:
        hints.append("Auth error. Verify API keys/endpoints (do not print secrets).")
    # de-dupe
    out: list[str] = []
    seen = set()
    for h in hints:
        if h and h not in seen:
            out.append(h)
            seen.add(h)
    return out[:12]


# =============================================================================
# main
# =============================================================================
async def amain(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Smoke test core wiring (no trades).")
    ap.add_argument("--json", action="store_true", help="Emit JSON summary to STDOUT.")
    ap.add_argument("--json-only", action="store_true", help="Only emit JSON (no human logs).")
    ap.add_argument(
        "--timeout",
        type=float,
        default=_envf("SMOKE_CORE_TIMEOUT_S", 60.0),
        help="Overall timeout (seconds).",
    )

    ap.add_argument(
        "--require-wallet",
        action="store_true",
        help="Require wallet at init_resources (defaults false).",
    )

    ap.add_argument("--skip-redis", action="store_true", help="Skip redis probe.")
    ap.add_argument("--skip-cache", action="store_true", help="Skip entry-cache probe.")
    ap.add_argument("--skip-pricing", action="store_true", help="Skip SOL/USD pricing probe.")
    ap.add_argument("--skip-quote", action="store_true", help="Skip Jupiter quote probe.")

    ap.add_argument(
        "--quote-out-mint",
        type=str,
        default=_envs("SMOKE_CORE_QUOTE_OUT_MINT", USDC_MINT),
        help="Output mint for quote (default USDC).",
    )
    ap.add_argument(
        "--quote-amount-sol",
        type=float,
        default=_envf("SMOKE_CORE_QUOTE_AMOUNT_SOL", 0.001),
        help="Amount SOL for quote (default 0.001).",
    )
    ap.add_argument(
        "--quote-slippage-bps",
        type=int,
        default=_envi("SMOKE_CORE_QUOTE_SLIPPAGE_BPS", 50),
        help="Slippage bps for quote (default 50).",
    )

    args = ap.parse_args(argv if argv is not None else None)

    json_only = bool(args.json_only or _envb("SMOKE_CORE_JSON_ONLY", False))
    emit_json = bool(
        args.json
        or args.json_only
        or _envb("SMOKE_CORE_JSON", False)
        or _envb("PREFLIGHT_JSON", False)
    )

    # Safety flags: discourage any trade path anywhere
    os.environ.setdefault("SMOKE", "1")
    os.environ.setdefault("SMOKE_CORE", "1")
    os.environ.setdefault("DRYRUN", os.getenv("DRYRUN", "1"))

    _bootstrap_repo_root()
    _maybe_load_dotenv()

    # Env toggles (script-level)
    require_wallet = bool(args.require_wallet or _envb("SMOKE_CORE_REQUIRE_WALLET", False))
    skip_redis = bool(args.skip_redis or _envb("SMOKE_CORE_SKIP_REDIS", False))
    skip_cache = bool(args.skip_cache or _envb("SMOKE_CORE_SKIP_CACHE", False))
    skip_pricing = bool(args.skip_pricing or _envb("SMOKE_CORE_SKIP_PRICING", False))
    skip_quote = bool(args.skip_quote or _envb("SMOKE_CORE_SKIP_QUOTE", False))
    require_quote = bool(_envb("SMOKE_CORE_REQUIRE_QUOTE", False))

    checks: list[dict[str, Any]] = []
    warnings: list[str] = []
    fix_hints: list[str] = []
    ok = False
    detail = ""

    t0 = time.monotonic()

    cu = None

    async def _run_all() -> None:
        nonlocal cu, ok, detail

        cu = importlib.import_module("utils_pkg.core_utils")

        # Required: init + close exist?
        if not _has(cu, "init_resources"):
            raise RuntimeError("utils_pkg.core_utils.init_resources is missing")

        # Init resources
        await _call_init_resources(cu, require_wallet=require_wallet)
        checks.append(
            {
                "name": "init_resources",
                "ok": True,
                "detail": f"require_wallet={int(require_wallet)}",
            }
        )

        # Session probe
        s_ok, s_msg = await _probe_session(cu)
        checks.append({"name": "session", "ok": bool(s_ok), "detail": s_msg})
        if not s_ok:
            warnings.append("HTTP session not available (may impact RPC/Jupiter/feeds).")

        # Redis
        if skip_redis:
            checks.append({"name": "redis", "ok": True, "skipped": True, "detail": "skip_redis"})
        else:
            r_ok, r_msg = await _probe_redis(cu)
            checks.append({"name": "redis", "ok": bool(r_ok), "detail": r_msg})
            if not r_ok:
                warnings.append("Redis probe failed (caching/queues may not work).")

        # Entry cache
        if skip_cache:
            checks.append(
                {"name": "entry_cache", "ok": True, "skipped": True, "detail": "skip_cache"}
            )
        else:
            c_ok, c_msg = await _probe_entry_cache(cu)
            checks.append({"name": "entry_cache", "ok": bool(c_ok), "detail": c_msg})
            if not c_ok:
                warnings.append("Entry-cache probe failed (may affect entry bookkeeping).")

        # Pricing
        if skip_pricing:
            checks.append(
                {"name": "pricing", "ok": True, "skipped": True, "detail": "skip_pricing"}
            )
        else:
            p_ok, p_msg = await _probe_pricing(cu)
            checks.append({"name": "pricing", "ok": bool(p_ok), "detail": p_msg})
            if not p_ok:
                warnings.append("Pricing probe failed (SOL/USD may be unavailable).")

        # Quote
        if skip_quote:
            checks.append({"name": "quote", "ok": True, "skipped": True, "detail": "skip_quote"})
        else:
            q_ok, q_msg = await _probe_quote(
                cu,
                out_mint=str(args.quote_out_mint).strip() or USDC_MINT,
                amount_sol=float(args.quote_amount_sol),
                slippage_bps=int(args.quote_slippage_bps),
            )
            checks.append({"name": "quote", "ok": bool(q_ok), "detail": q_msg})
            if not q_ok:
                if require_quote:
                    raise RuntimeError(f"Quote required but failed: {q_msg}")
                warnings.append("Quote probe failed (router/Jupiter may be degraded).")

        ok = True
        detail = "core_ok"

    try:
        await asyncio.wait_for(_run_all(), timeout=max(8.0, float(args.timeout)))
    except TimeoutError:
        ok = False
        detail = "timeout"
        err = _sanitize(f"timeout after {float(args.timeout):.0f}s")
        checks.append({"name": "smoke_core", "ok": False, "detail": err})
        fix_hints.append(
            "Increase SMOKE_CORE_TIMEOUT_S or fix slow dependencies (RPC/DB/Redis/Jupiter)."
        )
    except Exception as e:
        ok = False
        err_s = _sanitize(f"{type(e).__name__}: {e}")
        detail = err_s
        checks.append({"name": "smoke_core", "ok": False, "detail": err_s})
        fix_hints.extend(_fix_hints_from_error(err_s))
    finally:
        # Always try to close resources
        if cu is not None:
            with contextlib.suppress(Exception):
                await _call_close_resources(cu)
                checks.append({"name": "close_resources", "ok": True})

    sec = time.monotonic() - t0

    def _dedupe(xs: list[str], cap: int) -> list[str]:
        out: list[str] = []
        seen = set()
        for x in xs:
            x = (x or "").strip()
            if x and x not in seen:
                out.append(x)
                seen.add(x)
        return out[:cap]

    warnings = _dedupe(warnings, 12)
    fix_hints = _dedupe(fix_hints, 12)

    summary: dict[str, Any] = {
        "ts": _ts(),
        "ok": bool(ok),
        "step": "smoke_core",
        "detail": detail,
        "sec": round(float(sec), 3),
        "warnings": warnings,
        "fix_hints": fix_hints,
        "checks": checks,
        "meta": {
            "python": _sanitize(sys.version.split()[0]),
            "pid": os.getpid(),
            "cwd": _sanitize(os.getcwd()),
            "repo_root": _sanitize(os.getenv("REPO_ROOT", "")),
            "rpc_url_set": int(bool(_envs("RPC_URL", "") or _envs("SOL_RPC_URL", ""))),
            "dryrun": _sanitize(os.getenv("DRYRUN", "")),
            "require_wallet": int(require_wallet),
            "require_quote": int(require_quote),
        },
    }

    if emit_json:
        print(json.dumps(summary, ensure_ascii=False, default=str))

    if ok:
        _hlog("OK core wiring smoke passed", json_only=json_only)
        return RC_OK

    _hlog(f"FAIL {detail}", json_only=json_only)
    return RC_FAIL


def main(argv: list[str] | None = None) -> None:
    # Windows subprocess stability not needed here, but keep loop policy consistent across scripts
    with contextlib.suppress(Exception):
        if os.name == "nt" and hasattr(asyncio, "WindowsProactorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())  # type: ignore[attr-defined]

    try:
        rc = asyncio.run(amain(argv))
    except KeyboardInterrupt:
        rc = RC_CTRL_C
    raise SystemExit(int(rc))


if __name__ == "__main__":
    main()
