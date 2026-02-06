# -*- coding: utf-8 -*-
from __future__ import annotations

"""
ai_gate.py — Compatibility Gate (God-Tier Overhaul / P0→P2)
==========================================================

IMPORTANT (architecture lock-in)
--------------------------------
This module is intentionally NOT a second “entry brain”.
Your canonical entry decision remains:
    utils_pkg.ai_entry_logic.should_enter(features)

This file exists for legacy callers that still do:
    ok, report = await evaluate_candidate(mint)

Design goals
------------
- Fail-soft: never raise for normal errors (always returns (ok, report))
- Cancellation-safe: never swallow asyncio.CancelledError
- Deterministic defaults; network calls are optional and timeboxed
- No hard dependency on OpenAI at import time (lazy import)
- Minimal duplication: honeypot + quote + optional AI hint only
- Production-safe: concurrency limits, micro-cache, structured reasons
- Canonical logging: uses utils_pkg.logging_setup.get_logger -> EventLogger (single contract)

Public API
----------
    async evaluate_candidate(mint: str, *, context: dict | None = None) -> (bool, dict)

Context (optional)
------------------
You may pass pre-fetched data to avoid duplicated calls:
  context = {
      "features": {...}                 # if you already built feature dict
      "honeypot": (hp_ok, hp_report)    # if you already ran honeypot_check
      "jup_buy_quote": {...}            # if you already fetched quote
      "decimals": 6                     # if you already know mint decimals
      "entry_solin": 0.00012            # if you already computed entry price (SOL/token)
  }
"""

import asyncio
import base64
import json
import logging
import os
import threading
import time
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

Json = dict[str, Any]

# ──────────────────────────────────────────────────────────────────────────────
# Canonical logging (EventLogger)
# ──────────────────────────────────────────────────────────────────────────────
if TYPE_CHECKING:
    from utils_pkg.log_compat import EventLogger
else:
    EventLogger = Any  # type: ignore[assignment,misc]

_LOG_LOCK = threading.Lock()
_LOG: EventLogger | None = None

_ONCE_LOCK = threading.Lock()
_ONCE_SEEN: dict[str, float] = {}


def _logger() -> EventLogger:
    """
    Lazy-get canonical EventLogger.
    No import-time logging configuration side effects.
    """
    global _LOG
    if _LOG is None:
        with _LOG_LOCK:
            if _LOG is None:
                from utils_pkg.logging_setup import get_logger as _get_logger

                _LOG = _get_logger("ai_gate")
    return _LOG


def _level_to_str(level: int | str) -> str | int:
    # Keep compatible: EventLogger accepts str|int. Preserve ints.
    if isinstance(level, int):
        return level
    s = str(level).upper().strip()
    return s or "INFO"


def _log_once(
    event: str,
    *,
    ttl: float = 30.0,
    level: int | str = logging.INFO,
    msg: str | None = None,
    **fields: Any,
) -> bool:
    """
    Dedup bursts of identical events (per process).
    """
    now = time.monotonic()
    with _ONCE_LOCK:
        last = float(_ONCE_SEEN.get(event, 0.0))
        if now - last < float(ttl):
            return False
        _ONCE_SEEN[event] = now

    try:
        _logger().event(event, msg=(msg or event), level=_level_to_str(level), **fields)
    except Exception:
        # never let logging break gate
        pass
    return True


def _short(x: Any, n: int = 240) -> str:
    try:
        s = str(x)
        return s if len(s) <= n else (s[:n] + "…")
    except Exception:
        return "<unprintable>"


# ──────────────────────────────────────────────────────────────────────────────
# Small env helpers (NO new env key names introduced)
# ──────────────────────────────────────────────────────────────────────────────
def _env_str(key: str, default: str = "") -> str:
    v = os.getenv(key)
    return default if v is None else str(v).strip()


def _env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def _env_int(key: str, default: int) -> int:
    v = os.getenv(key)
    if v is None or str(v).strip() == "":
        return int(default)
    try:
        return int(float(str(v).strip()))
    except Exception:
        return int(default)


def _env_float(key: str, default: float) -> float:
    v = os.getenv(key)
    if v is None or str(v).strip() == "":
        return float(default)
    try:
        return float(str(v).strip())
    except Exception:
        return float(default)


def _clip01(x: float) -> float:
    try:
        f = float(x)
    except Exception:
        return 0.0
    if f < 0.0:
        return 0.0
    if f > 1.0:
        return 1.0
    return f


def _now() -> float:
    return time.time()


# ──────────────────────────────────────────────────────────────────────────────
# Config (kept compatible with your existing env names)
# ──────────────────────────────────────────────────────────────────────────────
AIGATE_ENABLED = _env_bool("AIGATE_ENABLED", True)

DEFAULT_SOL_IN_LAMPORTS = _env_int("AIGATE_SOL_IN_LAMPORTS", 1_000_000)  # 0.001 SOL probe
QUOTE_SLIPPAGE_BPS = _env_int("AIGATE_QUOTE_SLIPPAGE_BPS", 300)  # 3% probe

MAX_PRICE_IMPACT_BPS = _env_int("AIGATE_MAX_PRICE_IMPACT_BPS", 1000)  # 10%
REQUIRE_HONEYPOT_OK = _env_bool("AIGATE_REQUIRE_HONEYPOT_OK", True)
AIGATE_REQUIRE_ENTRY_PRICE = _env_bool("AIGATE_REQUIRE_ENTRY_PRICE", False)

AIGATE_MAX_PARALLEL = max(1, _env_int("AIGATE_MAX_PARALLEL", 12))
AIGATE_NET_TIMEOUT_SEC = max(1.0, _env_float("AIGATE_NET_TIMEOUT_SEC", 8.0))
AIGATE_CACHE_TTL_SEC = max(0.0, _env_float("AIGATE_CACHE_TTL_SEC", 3.0))

# AI classifier (optional hint/block)
AIGATE_AI_ENABLED = _env_bool("AIGATE_AI_ENABLED", True) and bool(_env_str("OPENAI_API_KEY", ""))
OPENAI_MODEL = _env_str("OPENAI_MODEL", "gpt-4o-mini")
FINE_TUNED_MODEL = _env_str("FINE_TUNED_MODEL", "") or None
AI_BAD_THRESHOLD = float(_env_float("AI_BAD_THRESHOLD", 0.30))
AI_GOOD_THRESHOLD = float(_env_float("AI_GOOD_THRESHOLD", 0.70))
AIGATE_AI_TIMEOUT_SEC = max(1.0, _env_float("AIGATE_AI_TIMEOUT_SEC", 6.0))
AIGATE_AI_COOLDOWN_SEC = max(0.0, _env_float("AIGATE_AI_COOLDOWN_SEC", 8.0))

# Optional Birdeye fallback (only used if your honeypot module exposes it)
AIGATE_SOL_USD = _env_float("AIGATE_SOL_USD", 0.0)  # USD->SOL fallback only if >0


# ──────────────────────────────────────────────────────────────────────────────
# Concurrency + micro-cache
# ──────────────────────────────────────────────────────────────────────────────
_SEMA = asyncio.Semaphore(AIGATE_MAX_PARALLEL)
_CACHE: dict[str, tuple[float, Json]] = {}  # mint -> (ts, report)
_AI_LAST_CALL: dict[str, float] = {}  # mint -> ts


# ──────────────────────────────────────────────────────────────────────────────
# External dependencies (imported lazily / best-effort)
# ──────────────────────────────────────────────────────────────────────────────
def _try_import_honeypot() -> Any | None:
    try:
        from utils_pkg.honeypot import honeypot_check  # type: ignore[import-not-found]

        return honeypot_check
    except Exception:
        return None


def _try_import_honeypot_bits() -> dict[str, Any]:
    """
    Best-effort reuse of constants/helpers if your project exposes them.
    No network calls here.
    """
    bits: dict[str, Any] = {}
    try:
        from utils_pkg import honeypot as hp  # type: ignore[import-not-found]

        bits["WSOL_MINT"] = getattr(hp, "WSOL_MINT", "So11111111111111111111111111111111111111112")
        bits["JUPITER_QUOTE_URL"] = getattr(
            hp, "JUPITER_QUOTE_URL", "https://quote-api.jup.ag/v6/quote"
        )
        bits["BIRDEYE_API_URL"] = getattr(hp, "BIRDEYE_API_URL", "")
        bits["_headers_birdeye"] = getattr(hp, "_headers_birdeye", None)
        bits["_get_session"] = getattr(hp, "_get_session", None)
        bits["SHARED_SOLANA_CLIENT"] = getattr(hp, "SHARED_SOLANA_CLIENT", None)
    except Exception:
        bits["WSOL_MINT"] = "So11111111111111111111111111111111111111112"
        bits["JUPITER_QUOTE_URL"] = "https://quote-api.jup.ag/v6/quote"
        bits["BIRDEYE_API_URL"] = ""
        bits["_headers_birdeye"] = None
        bits["_get_session"] = None
        bits["SHARED_SOLANA_CLIENT"] = None
    return bits


_HP_BITS = _try_import_honeypot_bits()
WSOL_MINT = str(_HP_BITS.get("WSOL_MINT"))
JUPITER_QUOTE_URL = str(_HP_BITS.get("JUPITER_QUOTE_URL"))
BIRDEYE_API_URL = str(_HP_BITS.get("BIRDEYE_API_URL") or "")
_headers_birdeye = _HP_BITS.get("_headers_birdeye", None)
_get_session = _HP_BITS.get("_get_session", None)
SHARED_SOLANA_CLIENT = _HP_BITS.get("SHARED_SOLANA_CLIENT", None)


# ──────────────────────────────────────────────────────────────────────────────
# Policy snapshot (structured)
# ──────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class GatePolicy:
    enabled: bool = True
    require_honeypot_ok: bool = True
    max_price_impact_bps: int = 1000
    probe_sol_in_lamports: int = 1_000_000
    quote_slippage_bps: int = 300
    require_entry_price: bool = False

    ai_enabled: bool = False
    ai_bad_threshold: float = 0.30
    ai_good_threshold: float = 0.70

    net_timeout_sec: float = 8.0
    cache_ttl_sec: float = 3.0


def _policy_snapshot() -> Json:
    p = GatePolicy(
        enabled=AIGATE_ENABLED,
        require_honeypot_ok=REQUIRE_HONEYPOT_OK,
        max_price_impact_bps=MAX_PRICE_IMPACT_BPS,
        probe_sol_in_lamports=DEFAULT_SOL_IN_LAMPORTS,
        quote_slippage_bps=QUOTE_SLIPPAGE_BPS,
        require_entry_price=AIGATE_REQUIRE_ENTRY_PRICE,
        ai_enabled=AIGATE_AI_ENABLED,
        ai_bad_threshold=AI_BAD_THRESHOLD,
        ai_good_threshold=AI_GOOD_THRESHOLD,
        net_timeout_sec=AIGATE_NET_TIMEOUT_SEC,
        cache_ttl_sec=AIGATE_CACHE_TTL_SEC,
    )
    out = asdict(p)
    out["version"] = "v3_compat_gate"
    return out


# ──────────────────────────────────────────────────────────────────────────────
# Safe numeric helpers
# ──────────────────────────────────────────────────────────────────────────────
def safe_float(x: Any) -> float | None:
    try:
        return float(x)
    except Exception:
        return None


def safe_int(x: Any) -> int | None:
    try:
        return int(x)
    except Exception:
        return None


def _price_impact_bps(q: Json | None) -> int | None:
    """
    Jupiter priceImpactPct is typically a fraction: 0.0123 == 1.23%.
    Convert to bps: 0.0123 * 10_000 == 123 bps.
    """
    if not q:
        return None
    f = safe_float(q.get("priceImpactPct"))
    if f is None:
        return None
    return int(round(float(f) * 10_000))


# ──────────────────────────────────────────────────────────────────────────────
# Networking: use shared aiohttp session from honeypot (fail-soft if unavailable)
# ──────────────────────────────────────────────────────────────────────────────
async def _http_get_json(
    url: str, *, params: dict[str, str], headers: dict[str, str] | None = None
) -> Json | None:
    sess = None
    try:
        if callable(_get_session):
            sess = await _get_session()
    except asyncio.CancelledError:
        raise
    except Exception:
        sess = None

    if sess is None:
        return None

    try:
        async with sess.get(url, params=params, headers=headers) as resp:
            status = int(getattr(resp, "status", 0) or 0)
            if status != 200:
                return None
            js = await resp.json()
            return js if isinstance(js, dict) else None
    except asyncio.CancelledError:
        raise
    except Exception:
        return None


async def jup_quote_buy(mint: str, sol_in_lamports: int = DEFAULT_SOL_IN_LAMPORTS) -> Json | None:
    """
    Quote BUY: WSOL -> token (ExactIn). Fail-soft returns None.
    """
    params = {
        "inputMint": WSOL_MINT,
        "outputMint": mint,
        "amount": str(int(sol_in_lamports)),
        "swapMode": "ExactIn",
        "onlyDirectRoutes": "false",
        "slippageBps": str(int(QUOTE_SLIPPAGE_BPS)),
    }
    try:
        return await asyncio.wait_for(
            _http_get_json(JUPITER_QUOTE_URL, params=params),
            timeout=float(AIGATE_NET_TIMEOUT_SEC),
        )
    except asyncio.CancelledError:
        raise
    except Exception:
        return None


async def token_decimals(mint: str, *, context: Json | None = None) -> int:
    """
    Best-effort SPL mint decimals. Falls back to 9.
    Accepts optional context["decimals"] override.
    """
    try:
        if context and "decimals" in context:
            d = safe_int(context.get("decimals"))
            if d is not None and 0 <= int(d) <= 18:
                return int(d)
    except Exception:
        pass

    client = SHARED_SOLANA_CLIENT
    if client is None:
        return 9

    try:
        from solders.pubkey import Pubkey  # type: ignore[import-not-found]

        resp = await client.get_account_info(Pubkey.from_string(mint))
        val = getattr(resp, "value", None)
        if not val:
            return 9

        data = getattr(val, "data", None)
        if not data or not isinstance(data, (tuple, list)) or len(data) < 1:
            return 9

        data_b64 = data[0]
        raw = base64.b64decode(data_b64)
        dec = raw[44] if len(raw) > 44 else 9
        dec_i = int(dec)
        return dec_i if 0 <= dec_i <= 18 else 9
    except asyncio.CancelledError:
        raise
    except Exception:
        return 9


async def estimate_entry_sol_per_token(
    mint: str,
    *,
    sol_in_lamports: int = DEFAULT_SOL_IN_LAMPORTS,
    quote: Json | None = None,
    context: Json | None = None,
) -> float | None:
    """
    Prefer Jupiter quote to compute SOL/token. Fallback to Birdeye USD price if configured.
    Fail-soft returns None.
    """
    q = quote or await jup_quote_buy(mint, sol_in_lamports=sol_in_lamports)
    if q and q.get("outAmount"):
        out_raw = safe_int(q.get("outAmount"))
        if out_raw is not None and out_raw > 0:
            dec = await token_decimals(mint, context=context)
            tokens = float(out_raw) / float(10**dec)
            sol_in = float(sol_in_lamports) / 1_000_000_000.0
            if tokens > 0.0:
                return sol_in / tokens

    # Optional Birdeye fallback if available + SOL_USD configured
    if BIRDEYE_API_URL and AIGATE_SOL_USD > 0 and callable(_headers_birdeye):
        try:
            js = await asyncio.wait_for(
                _http_get_json(
                    f"{BIRDEYE_API_URL.rstrip('/')}/public/price",
                    params={"address": mint},
                    headers=_headers_birdeye(),
                ),
                timeout=float(AIGATE_NET_TIMEOUT_SEC),
            )
            usd = safe_float(((js or {}).get("data") or {}).get("value"))
            if usd is not None and usd > 0.0:
                return float(usd) / float(AIGATE_SOL_USD)
        except asyncio.CancelledError:
            raise
        except Exception:
            pass

    return None


# ──────────────────────────────────────────────────────────────────────────────
# AI classifier (optional): fail-soft + cooldown + strict-ish JSON parsing
# ──────────────────────────────────────────────────────────────────────────────
def _ensure_openai_client() -> tuple[Any | None, str]:
    """
    Returns (client, kind) where kind is "sdk" | "legacy" | "none"
    """
    api_key = _env_str("OPENAI_API_KEY", "")
    if not api_key:
        return None, "none"

    try:
        from openai import OpenAI  # type: ignore[import-not-found]

        return OpenAI(api_key=api_key), "sdk"
    except Exception:
        pass

    try:
        import openai  # type: ignore[import-not-found]

        if not getattr(openai, "api_key", None):
            openai.api_key = api_key
        return openai, "legacy"
    except Exception:
        return None, "none"


async def _call_ai_classifier(context: Json) -> Json:
    """
    Returns:
      { "model": str, "label": "GOOD|BAD|NEUTRAL"|None, "score": float|None, "why": str|None, "raw": str }
    or { "error": "...", "model": str }
    """
    if not AIGATE_AI_ENABLED:
        return {"error": "ai_disabled"}

    mint = str(context.get("mint") or "").strip()
    if mint:
        last = float(_AI_LAST_CALL.get(mint, 0.0))
        if AIGATE_AI_COOLDOWN_SEC > 0.0 and (_now() - last) < float(AIGATE_AI_COOLDOWN_SEC):
            return {"error": "ai_cooldown"}
        _AI_LAST_CALL[mint] = _now()

    client, kind = _ensure_openai_client()
    if client is None:
        return {"error": "ai_unavailable"}

    model = FINE_TUNED_MODEL or OPENAI_MODEL

    hp = context.get("honeypot") or {}
    q = context.get("jup_buy_quote") or {}
    entry_solin = context.get("entry_solin")
    impact_bps = _price_impact_bps(q if isinstance(q, dict) else None)

    payload = {
        "mint": mint,
        "impact_bps": impact_bps,
        "entry_solin": entry_solin,
        "honeypot": {
            "ok": (hp or {}).get("ok") if isinstance(hp, dict) else None,
            "reasons": (hp or {}).get("reasons") if isinstance(hp, dict) else None,
            "sources": (hp or {}).get("sources") if isinstance(hp, dict) else None,
        },
        "quote": {
            "outAmount": (q or {}).get("outAmount") if isinstance(q, dict) else None,
            "priceImpactPct": (q or {}).get("priceImpactPct") if isinstance(q, dict) else None,
            "routePlan": (q or {}).get("routePlan") if isinstance(q, dict) else None,
        },
        "task": "classify_setup_for_entry",
    }

    system_msg = (
        "You are a risk + PnL assistant for an automated trading system. "
        "You must output ONLY strict JSON: "
        '{"label":"GOOD|BAD|NEUTRAL","score":<number 0..1>,"why":"short"} '
        "No other text."
    )
    user_msg = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)

    try:
        if kind == "sdk":

            def _do_req_sdk() -> Any:
                try:
                    return client.chat.completions.create(
                        model=model,
                        messages=[
                            {"role": "system", "content": system_msg},
                            {"role": "user", "content": user_msg},
                        ],
                        temperature=0.1,
                        max_tokens=140,
                        response_format={"type": "json_object"},
                    )
                except TypeError:
                    return client.chat.completions.create(
                        model=model,
                        messages=[
                            {"role": "system", "content": system_msg},
                            {"role": "user", "content": user_msg},
                        ],
                        temperature=0.1,
                        max_tokens=140,
                    )

            resp = await asyncio.wait_for(
                asyncio.to_thread(_do_req_sdk), timeout=float(AIGATE_AI_TIMEOUT_SEC)
            )
            raw = (resp.choices[0].message.content or "").strip()  # type: ignore[attr-defined]
        else:

            def _do_req_legacy() -> Any:
                return client.ChatCompletion.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": system_msg},
                        {"role": "user", "content": user_msg},
                    ],
                    temperature=0.1,
                    max_tokens=140,
                )

            resp = await asyncio.wait_for(
                asyncio.to_thread(_do_req_legacy), timeout=float(AIGATE_AI_TIMEOUT_SEC)
            )
            raw = str(resp["choices"][0]["message"]["content"]).strip()  # type: ignore[index]

        label: str | None = None
        score: float | None = None
        why: str | None = None

        try:
            obj = json.loads(raw)
            label_s = str(obj.get("label") or "").upper().strip()
            label = label_s if label_s in ("GOOD", "BAD", "NEUTRAL") else None
            score = safe_float(obj.get("score"))
            why = str(obj.get("why") or "").strip() or None
        except Exception:
            up = raw.upper()
            if "BAD" in up:
                label = "BAD"
            elif "GOOD" in up:
                label = "GOOD"
            elif "NEUTRAL" in up:
                label = "NEUTRAL"
            score = 0.5

        if score is not None:
            score = _clip01(score)

        return {"model": model, "label": label, "score": score, "why": why, "raw": raw[:1500]}

    except asyncio.CancelledError:
        raise
    except Exception as e:
        return {"error": f"{type(e).__name__}: {str(e)[:200]}", "model": model}


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────
async def evaluate_candidate(mint: str, *, context: Json | None = None) -> tuple[bool, Json]:
    """
    Legacy-compatible gate.

    Decision rules (default):
      - If AIGATE_ENABLED=0 -> ok=True (still returns policy + any available sources)
      - Honeypot must be ok if REQUIRE_HONEYPOT_OK=1
      - Price impact must be <= MAX_PRICE_IMPACT_BPS if readable
      - Entry price:
          * required if AIGATE_REQUIRE_ENTRY_PRICE=1
          * otherwise missing is SOFT (does not block)
      - AI (if enabled):
          * BAD with score>=AI_BAD_THRESHOLD blocks
          * GOOD with score>=AI_GOOD_THRESHOLD adds positive reason (does not force entry)
    """
    mint_s = (mint or "").strip()
    ctx: Json = dict(context or {})

    if not mint_s:
        empty_report: Json = {
            "mint": "",
            "ok": False,
            "reasons": ["mint:empty"],
            "entry_solin": None,
            "impact_bps": None,
            "sources": {},
            "policy": _policy_snapshot(),
        }
        return False, empty_report

    # micro-cache (dedupe bursts)
    if AIGATE_CACHE_TTL_SEC > 0.0:
        hit = _CACHE.get(mint_s)
        if hit:
            ts, rep = hit
            if (_now() - float(ts)) <= float(AIGATE_CACHE_TTL_SEC):
                return bool(rep.get("ok")), rep

    async with _SEMA:
        policy = _policy_snapshot()
        sources: Json = {}
        reasons: list[str] = []

        # Gate disabled: do not block; still include honeypot if available.
        if not AIGATE_ENABLED:
            hp_rep_local: Json = {"ok": True, "reasons": ["aigate_disabled"]}

            if "honeypot" in ctx and isinstance(ctx.get("honeypot"), (tuple, list)):
                tup = ctx.get("honeypot")
                if isinstance(tup, (tuple, list)) and len(tup) == 2:
                    ok_val = bool(tup[0])
                    hp_rep_local = dict(tup[1] or {})
                    if "ok" not in hp_rep_local:
                        hp_rep_local["ok"] = ok_val
            else:
                hp_fn = _try_import_honeypot()
                if callable(hp_fn):
                    try:
                        ok_val, rep = await asyncio.wait_for(
                            hp_fn(mint_s), timeout=float(AIGATE_NET_TIMEOUT_SEC)
                        )
                        hp_rep_local = dict(rep or {})
                        if "ok" not in hp_rep_local:
                            hp_rep_local["ok"] = bool(ok_val)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        hp_rep_local = {"ok": True, "reasons": ["honeypot_unavailable_soft"]}

            sources["honeypot"] = hp_rep_local

            disabled_report: Json = {
                "mint": mint_s,
                "ok": True,
                "reasons": ["aigate:disabled"],
                "entry_solin": None,
                "impact_bps": None,
                "sources": sources,
                "policy": policy,
            }
            if AIGATE_CACHE_TTL_SEC > 0.0:
                _CACHE[mint_s] = (_now(), disabled_report)
            return True, disabled_report

        # 1) Honeypot (best-effort, fail-soft)
        hp_ok: bool = True
        hp_rep: Json = {"ok": True, "reasons": ["honeypot_skipped_soft"]}

        if "honeypot" in ctx and isinstance(ctx.get("honeypot"), (tuple, list)):
            tup2 = ctx.get("honeypot")
            if isinstance(tup2, (tuple, list)) and len(tup2) == 2:
                hp_ok = bool(tup2[0])
                hp_rep = dict(tup2[1] or {})
        else:
            hp_fn2 = _try_import_honeypot()
            if callable(hp_fn2):
                try:
                    hp_ok, hp_rep = await asyncio.wait_for(
                        hp_fn2(mint_s), timeout=float(AIGATE_NET_TIMEOUT_SEC)
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    hp_ok, hp_rep = (
                        True,
                        {"ok": True, "reasons": [f"honeypot_error_soft:{type(e).__name__}"]},
                    )
            else:
                hp_ok, hp_rep = True, {"ok": True, "reasons": ["honeypot_missing_soft"]}

        sources["honeypot"] = hp_rep

        if REQUIRE_HONEYPOT_OK and not hp_ok:
            reasons.append("honeypot:not_ok")

        # 2) Jupiter buy quote (soft)
        q: Json | None
        if "jup_buy_quote" in ctx and isinstance(ctx.get("jup_buy_quote"), dict):
            q = ctx.get("jup_buy_quote")  # type: ignore[assignment]
        else:
            q = await jup_quote_buy(mint_s, sol_in_lamports=DEFAULT_SOL_IN_LAMPORTS)

        sources["jup_buy_quote"] = q or {}
        impact_bps = _price_impact_bps(q)

        if (
            impact_bps is not None
            and int(MAX_PRICE_IMPACT_BPS) > 0
            and int(impact_bps) > int(MAX_PRICE_IMPACT_BPS)
        ):
            reasons.append(f"impact_bps>{int(MAX_PRICE_IMPACT_BPS)}")

        # 3) Entry price estimate (soft unless require_entry_price)
        entry_solin: float | None = None
        if "entry_solin" in ctx:
            entry_solin = safe_float(ctx.get("entry_solin"))
        else:
            try:
                entry_solin = await asyncio.wait_for(
                    estimate_entry_sol_per_token(
                        mint_s, sol_in_lamports=DEFAULT_SOL_IN_LAMPORTS, quote=q, context=ctx
                    ),
                    timeout=float(AIGATE_NET_TIMEOUT_SEC),
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                entry_solin = None

        sources["entry_solin"] = entry_solin
        if entry_solin is None:
            if AIGATE_REQUIRE_ENTRY_PRICE:
                reasons.append("entry:none")
            else:
                reasons.append("entry:none_soft" if q else "jup:no_quote_soft")

        # 4) AI classifier (optional; fail-soft)
        ai_block = False
        if AIGATE_AI_ENABLED:
            ai_ctx = {
                "mint": mint_s,
                "honeypot": hp_rep,
                "jup_buy_quote": q or {},
                "entry_solin": entry_solin,
            }
            ai_res = await _call_ai_classifier(ai_ctx)
            sources["ai"] = ai_res

            label = str(ai_res.get("label") or "").upper().strip()
            score = safe_float(ai_res.get("score"))
            if label == "BAD" and score is not None and float(score) >= float(AI_BAD_THRESHOLD):
                reasons.append(f"ai:bad@{float(score):.3f}")
                ai_block = True
            elif label == "GOOD" and score is not None and float(score) >= float(AI_GOOD_THRESHOLD):
                reasons.append(f"ai:good_hint@{float(score):.3f}")
            elif ai_res.get("error"):
                reasons.append(f"ai:error:{_short(ai_res.get('error'))}")

        # 5) Final decision
        ok_honeypot = (not REQUIRE_HONEYPOT_OK) or hp_ok
        ok_impact = (
            (int(MAX_PRICE_IMPACT_BPS) <= 0)
            or (impact_bps is None)
            or (int(impact_bps) <= int(MAX_PRICE_IMPACT_BPS))
        )
        ok_entry = (not AIGATE_REQUIRE_ENTRY_PRICE) or (entry_solin is not None)

        ok = bool(ok_honeypot and ok_impact and ok_entry and (not ai_block))

        final_report: Json = {
            "mint": mint_s,
            "ok": ok,
            "reasons": reasons,
            "entry_solin": entry_solin,
            "impact_bps": impact_bps,
            "sources": sources,
            "policy": policy,
        }

        # Optional: minimal observability without noise (deduped)
        if not ok and reasons:
            _log_once(
                "AIGATE_BLOCK",
                ttl=10.0,
                level="INFO",
                msg="ai_gate blocked candidate",
                mint=mint_s,
                reasons=reasons[:8],
            )

        if AIGATE_CACHE_TTL_SEC > 0.0:
            _CACHE[mint_s] = (_now(), final_report)

        return ok, final_report


__all__ = [
    "estimate_entry_sol_per_token",
    "evaluate_candidate",
    "jup_quote_buy",
    "token_decimals",
]
