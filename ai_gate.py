# ai_gate.py
from __future__ import annotations

"""
ai_gate.py — Compatibility Gate (God-Tier Overhaul / P0→P2)
==========================================================

IMPORTANT (architecture lock-in)
--------------------------------
This module is intentionally **NOT** a second “entry brain”.
Your canonical entry decision is:
    utils_pkg.ai_entry_logic.should_enter(features)

This file exists for legacy callers that still do:
    ok, report = await evaluate_candidate(mint)

Design goals
------------
- Never raise (fail-soft): always returns (ok, report)
- Deterministic defaults; network calls are optional and timeboxed
- No hard dependency on OpenAI at import time
- Minimal duplication: honeypot + quote + optional AI hint only
- Production-safe: caching, timeouts, rate limits, structured reasons

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
  }
"""

import asyncio
import base64
import json
import os
import time
from dataclasses import asdict, dataclass
from typing import Any

from logging_setup import get_logger

log = get_logger(__name__)

Json = dict[str, Any]


# ──────────────────────────────────────────────────────────────────────────────
# Small env helpers
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
        x = float(x)
    except Exception:
        return 0.0
    return 0.0 if x < 0.0 else (1.0 if x > 1.0 else x)


def _now() -> float:
    return time.time()


# ──────────────────────────────────────────────────────────────────────────────
# Config (kept compatible with your existing env names; adds a few safe knobs)
# ──────────────────────────────────────────────────────────────────────────────
AIGATE_ENABLED = _env_bool("AIGATE_ENABLED", True)

DEFAULT_SOL_IN_LAMPORTS = _env_int("AIGATE_SOL_IN_LAMPORTS", 1_000_000)  # 0.001 SOL probe
QUOTE_SLIPPAGE_BPS = _env_int("AIGATE_QUOTE_SLIPPAGE_BPS", 300)  # 3% probe

MAX_PRICE_IMPACT_BPS = _env_int("AIGATE_MAX_PRICE_IMPACT_BPS", 1000)  # 10%
REQUIRE_HONEYPOT_OK = _env_bool("AIGATE_REQUIRE_HONEYPOT_OK", True)
AIGATE_REQUIRE_ENTRY_PRICE = _env_bool("AIGATE_REQUIRE_ENTRY_PRICE", False)

AIGATE_MAX_PARALLEL = max(1, _env_int("AIGATE_MAX_PARALLEL", 12))
AIGATE_NET_TIMEOUT_SEC = max(1.0, _env_float("AIGATE_NET_TIMEOUT_SEC", 8.0))
AIGATE_CACHE_TTL_SEC = max(
    0.0, _env_float("AIGATE_CACHE_TTL_SEC", 3.0)
)  # short TTL to dedupe bursts

# AI classifier (optional hint/block)
AIGATE_AI_ENABLED = _env_bool("AIGATE_AI_ENABLED", True) and bool(_env_str("OPENAI_API_KEY", ""))
OPENAI_MODEL = _env_str("OPENAI_MODEL", "gpt-4o-mini")
FINE_TUNED_MODEL = _env_str("FINE_TUNED_MODEL", "") or None
AI_BAD_THRESHOLD = float(_env_float("AI_BAD_THRESHOLD", 0.30))
AI_GOOD_THRESHOLD = float(_env_float("AI_GOOD_THRESHOLD", 0.70))
AIGATE_AI_TIMEOUT_SEC = max(1.0, _env_float("AIGATE_AI_TIMEOUT_SEC", 6.0))
AIGATE_AI_COOLDOWN_SEC = max(0.0, _env_float("AIGATE_AI_COOLDOWN_SEC", 8.0))

# Optional Birdeye fallback (only used if your honeypot module exposes it)
AIGATE_SOL_USD = _env_float("AIGATE_SOL_USD", 0.0)  # if you want USD->SOL conversion fallback


# ──────────────────────────────────────────────────────────────────────────────
# Concurrency + micro-cache
# ──────────────────────────────────────────────────────────────────────────────
_SEMA = asyncio.Semaphore(AIGATE_MAX_PARALLEL)
_CACHE: dict[str, tuple[float, Json]] = {}  # mint -> (ts, report)
_AI_LAST_CALL: dict[str, float] = {}  # mint -> ts


# ──────────────────────────────────────────────────────────────────────────────
# External dependencies (imported lazily)
# ──────────────────────────────────────────────────────────────────────────────
def _try_import_honeypot() -> Any | None:
    try:
        from utils_pkg.honeypot import honeypot_check  # type: ignore

        return honeypot_check
    except Exception:
        return None


def _try_import_honeypot_bits() -> dict[str, Any]:
    """
    Best-effort to reuse constants/helpers if your project has them.
    We do NOT require these to exist.
    """
    bits: dict[str, Any] = {}
    try:
        from utils_pkg import honeypot as hp  # type: ignore

        # Optional bits
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
# Policy + reason codes (structured for analytics)
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
    if not q:
        return None
    f = safe_float(q.get("priceImpactPct"))
    if f is None:
        return None
    # Jupiter returns fraction: 0.0123 = 1.23%
    return int(round(f * 10_000))


# ──────────────────────────────────────────────────────────────────────────────
# Networking: Jupiter quote (ExactIn) + Birdeye fallback (optional)
# ──────────────────────────────────────────────────────────────────────────────
async def _http_get_json(
    url: str, params: dict[str, str], headers: dict[str, str] | None = None
) -> Json | None:
    """
    Uses your project's shared session if present; otherwise fails-soft.
    """
    sess = None
    try:
        if callable(_get_session):
            sess = await _get_session()
    except Exception:
        sess = None

    if sess is None:
        return None

    try:
        async with sess.get(url, params=params, headers=headers) as resp:
            if resp.status != 200:
                return None
            return await resp.json()
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
            timeout=AIGATE_NET_TIMEOUT_SEC,
        )
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
            if d is not None and 0 <= d <= 18:
                return int(d)
    except Exception:
        pass

    client = None
    try:
        client = SHARED_SOLANA_CLIENT
    except Exception:
        client = None

    if client is None:
        return 9

    try:
        from solders.pubkey import Pubkey  # type: ignore

        resp = await client.get_account_info(Pubkey.from_string(mint))
        if not getattr(resp, "value", None):
            return 9

        data_b64, _enc = resp.value.data
        raw = base64.b64decode(data_b64)
        # Classic SPL Mint layout: decimals byte at offset 44
        dec = raw[44] if len(raw) > 44 else 9
        dec_i = int(dec)
        return dec_i if 0 <= dec_i <= 18 else 9
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
            tokens = float(out_raw) / (10**dec)
            sol_in = float(sol_in_lamports) / 1_000_000_000.0
            if tokens > 0:
                return sol_in / tokens

    # Optional Birdeye fallback if available + SOL_USD configured
    if BIRDEYE_API_URL and AIGATE_SOL_USD > 0 and callable(_headers_birdeye):
        try:
            js = await asyncio.wait_for(
                _http_get_json(
                    f"{BIRDEYE_API_URL}/public/price",
                    params={"address": mint},
                    headers=_headers_birdeye(),
                ),
                timeout=AIGATE_NET_TIMEOUT_SEC,
            )
            usd = safe_float(((js or {}).get("data") or {}).get("value"))
            if usd is not None and usd > 0:
                return usd / float(AIGATE_SOL_USD)
        except Exception:
            pass

    return None


# ──────────────────────────────────────────────────────────────────────────────
# AI classifier (optional): fail-soft + cooldown + JSON parsing
# ──────────────────────────────────────────────────────────────────────────────
def _ensure_openai_client() -> tuple[Any | None, str]:
    """
    Returns (client, kind) where kind is "sdk" | "legacy" | "none"
    """
    api_key = _env_str("OPENAI_API_KEY", "")
    if not api_key:
        return None, "none"

    # SDK (openai>=1.x)
    try:
        from openai import OpenAI  # type: ignore

        return OpenAI(api_key=api_key), "sdk"
    except Exception:
        pass

    # Legacy (openai<=0.28)
    try:
        import openai  # type: ignore

        if not getattr(openai, "api_key", None):
            openai.api_key = api_key
        return openai, "legacy"
    except Exception:
        return None, "none"


async def _call_ai_classifier(context: Json) -> Json:
    """
    Returns:
      { "model": str, "label": "GOOD|BAD|NEUTRAL"|None, "score": float|None, "raw": str }
    or { "error": "...", "model": str }
    """
    if not AIGATE_AI_ENABLED:
        return {"error": "ai_disabled"}

    mint = str(context.get("mint") or "").strip()
    if mint:
        last = _AI_LAST_CALL.get(mint, 0.0)
        if AIGATE_AI_COOLDOWN_SEC > 0 and (_now() - last) < AIGATE_AI_COOLDOWN_SEC:
            return {"error": "ai_cooldown"}
        _AI_LAST_CALL[mint] = _now()

    client, kind = _ensure_openai_client()
    if client is None:
        return {"error": "ai_unavailable"}

    model = FINE_TUNED_MODEL or OPENAI_MODEL

    hp = context.get("honeypot") or {}
    q = context.get("jup_buy_quote") or {}
    entry_solin = context.get("entry_solin")
    impact_bps = _price_impact_bps(q)

    # Keep prompt compact + structured
    payload = {
        "mint": mint,
        "impact_bps": impact_bps,
        "entry_solin": entry_solin,
        "honeypot": {
            "ok": hp.get("ok"),
            "reasons": hp.get("reasons"),
            "sources": hp.get("sources"),
        },
        "quote": {
            "outAmount": q.get("outAmount"),
            "priceImpactPct": q.get("priceImpactPct"),
            "routePlan": q.get("routePlan"),
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
                # response_format supported on newer SDKs; fall back if not
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
                asyncio.to_thread(_do_req_sdk), timeout=AIGATE_AI_TIMEOUT_SEC
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
                asyncio.to_thread(_do_req_legacy), timeout=AIGATE_AI_TIMEOUT_SEC
            )
            raw = str(resp["choices"][0]["message"]["content"]).strip()  # type: ignore[index]

        label = None
        score = None
        why = None
        try:
            obj = json.loads(raw)
            label = str(obj.get("label") or "").upper().strip() or None
            score = safe_float(obj.get("score"))
            why = str(obj.get("why") or "").strip() or None
        except Exception:
            # very soft fallback
            up = raw.upper()
            if "BAD" in up:
                label = "BAD"
            elif "GOOD" in up:
                label = "GOOD"
            elif "NEUTRAL" in up:
                label = "NEUTRAL"
            score = 0.5

        if label not in ("GOOD", "BAD", "NEUTRAL"):
            label = None
        if score is not None:
            score = _clip01(score)

        return {"model": model, "label": label, "score": score, "why": why, "raw": raw[:1500]}

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
    mint = (mint or "").strip()
    ctx: Json = dict(context or {})

    # micro-cache (dedupe bursts)
    if AIGATE_CACHE_TTL_SEC > 0:
        hit = _CACHE.get(mint)
        if hit:
            ts, rep = hit
            if (_now() - ts) <= AIGATE_CACHE_TTL_SEC:
                return bool(rep.get("ok")), rep

    async with _SEMA:
        policy = _policy_snapshot()
        sources: Json = {}
        reasons: list[str] = []

        if not AIGATE_ENABLED:
            # Still try honeypot if provided; otherwise best-effort
            hp_ok, hp_rep = True, {"ok": True, "reasons": ["aigate_disabled"]}
            if (
                "honeypot" in ctx
                and isinstance(ctx["honeypot"], tuple | list)
                and len(ctx["honeypot"]) == 2
            ):
                hp_ok, hp_rep = bool(ctx["honeypot"][0]), dict(ctx["honeypot"][1] or {})
            else:
                hp_fn = _try_import_honeypot()
                if callable(hp_fn):
                    try:
                        hp_ok, hp_rep = await asyncio.wait_for(
                            hp_fn(mint), timeout=AIGATE_NET_TIMEOUT_SEC
                        )
                    except Exception:
                        hp_ok, hp_rep = True, {"ok": True, "reasons": ["honeypot_unavailable_soft"]}
            sources["honeypot"] = hp_rep

            report = {
                "mint": mint,
                "ok": True,
                "reasons": ["aigate:disabled"],
                "entry_solin": None,
                "impact_bps": None,
                "sources": sources,
                "policy": policy,
            }
            if AIGATE_CACHE_TTL_SEC > 0:
                _CACHE[mint] = (_now(), report)
            return True, report

        # 1) Honeypot / risk suite (best-effort)
        hp_ok: bool = True
        hp_rep: Json = {"ok": True, "reasons": ["honeypot_skipped_soft"]}
        if (
            "honeypot" in ctx
            and isinstance(ctx["honeypot"], tuple | list)
            and len(ctx["honeypot"]) == 2
        ):
            hp_ok, hp_rep = bool(ctx["honeypot"][0]), dict(ctx["honeypot"][1] or {})
        else:
            hp_fn = _try_import_honeypot()
            if callable(hp_fn):
                try:
                    hp_ok, hp_rep = await asyncio.wait_for(
                        hp_fn(mint), timeout=AIGATE_NET_TIMEOUT_SEC
                    )
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
        q: Json | None = None
        if "jup_buy_quote" in ctx and isinstance(ctx["jup_buy_quote"], dict):
            q = ctx["jup_buy_quote"]
        else:
            q = await jup_quote_buy(mint, sol_in_lamports=DEFAULT_SOL_IN_LAMPORTS)

        sources["jup_buy_quote"] = q or {}
        impact_bps = _price_impact_bps(q)

        if (
            impact_bps is not None
            and MAX_PRICE_IMPACT_BPS > 0
            and impact_bps > MAX_PRICE_IMPACT_BPS
        ):
            reasons.append(f"impact_bps>{MAX_PRICE_IMPACT_BPS}")

        # 3) Entry price estimate (soft unless require_entry_price)
        entry_solin: float | None = None
        if "entry_solin" in ctx:
            entry_solin = safe_float(ctx.get("entry_solin"))
        else:
            try:
                entry_solin = await asyncio.wait_for(
                    estimate_entry_sol_per_token(
                        mint, sol_in_lamports=DEFAULT_SOL_IN_LAMPORTS, quote=q, context=ctx
                    ),
                    timeout=AIGATE_NET_TIMEOUT_SEC,
                )
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
                "mint": mint,
                "honeypot": hp_rep,
                "jup_buy_quote": q or {},
                "entry_solin": entry_solin,
            }
            ai_res = await _call_ai_classifier(ai_ctx)
            sources["ai"] = ai_res

            label = str(ai_res.get("label") or "").upper().strip()
            score = safe_float(ai_res.get("score"))
            if label == "BAD" and score is not None and score >= AI_BAD_THRESHOLD:
                reasons.append(f"ai:bad@{score:.3f}")
                ai_block = True
            elif label == "GOOD" and score is not None and score >= AI_GOOD_THRESHOLD:
                reasons.append(f"ai:good_hint@{score:.3f}")
            elif ai_res.get("error"):
                reasons.append(f"ai:error:{ai_res.get('error')}")

        # 5) Final decision
        ok_honeypot = (not REQUIRE_HONEYPOT_OK) or hp_ok
        ok_impact = (
            (MAX_PRICE_IMPACT_BPS <= 0)
            or (impact_bps is None)
            or (impact_bps <= MAX_PRICE_IMPACT_BPS)
        )
        ok_entry = (not AIGATE_REQUIRE_ENTRY_PRICE) or (entry_solin is not None)

        ok = bool(ok_honeypot and ok_impact and ok_entry and (not ai_block))

        report: Json = {
            "mint": mint,
            "ok": ok,
            "reasons": reasons,
            "entry_solin": entry_solin,
            "impact_bps": impact_bps,
            "sources": sources,
            "policy": policy,
        }

        # cache
        if AIGATE_CACHE_TTL_SEC > 0:
            _CACHE[mint] = (_now(), report)

        return ok, report


__all__ = ["estimate_entry_sol_per_token", "evaluate_candidate", "jup_quote_buy", "token_decimals"]
