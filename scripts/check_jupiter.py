# -*- coding: utf-8 -*-
from __future__ import annotations

"""
scripts/check_jupiter.py

God-Tier Jupiter Precheck (LOCK-IN / PROD)
=========================================

Why you're seeing 401 now
------------------------
Jupiter now requires an API key (x-api-key) for https://api.jup.ag endpoints.
If you don't send one, the server returns 401 Unauthorized. This script treats
that as a hard failure by default (production-safe), with an escape hatch for
local/dev where you want "connectivity-only" checks.

Design goals
------------
- ✅ Secret-safe (redacts URLs/headers, masks long tokens)
- ✅ Deterministic & automation-friendly (JSON-first, stable exit codes)
- ✅ Mirrors bot wiring (optional lightning_impl introspection + header reuse)
- ✅ Works in Render Shell + local (httpx -> aiohttp -> urllib fallback)
- ✅ Supports multiple endpoint shapes (api.jup.ag swap/v1, legacy /quote, quote-api v6)

Exit codes
----------
0   OK / skipped
1   Connect/respond failure for all candidates
2   Requirements failed (no viable quote, strict gates failed, missing required API key)
3   Bad URL format
124 All attempts timed out

Usage
-----
py -m scripts.check_jupiter
py -m scripts.check_jupiter --strict --try-all
py -m scripts.check_jupiter --swap-test --try-all
py -m scripts.check_jupiter --json-only
py -m scripts.check_jupiter --base https://api.jup.ag

Env (most relevant)
-------------------
Base(s):
JUP_BASE_URL=https://api.jup.ag
JUPITER_SWAP_API=https://api.jup.ag              (alias coverage)
JUP_ALT_BASE_URLS=csv                            (optional)

Paths (defaults are correct for api.jup.ag):
JUP_QUOTE_PATH=/swap/v1/quote
JUP_SWAP_PATH=/swap/v1/swap

Auth (sent as x-api-key):
JUP_PRO_KEY / JUP_API_KEY / JUP_KEY

Behavior / gates:
CHECK_JUP_DISABLED=0|1
CHECK_JUP_STRICT=0|1
CHECK_JUP_TRY_ALL=0|1
CHECK_JUP_TIMEOUT_S=12
CHECK_JUP_SWAP_TEST=0|1
CHECK_JUP_REQUIRE_SWAP_OK=0|1

API key enforcement:
- By default, api.jup.ag requires API key.
  Override only for local/dev:
  CHECK_JUP_ALLOW_NO_KEY=1   (lets the script attempt calls and report 401s without early-aborting)
  CHECK_JUP_REQUIRE_API_KEY=0 (forces "not required" policy even for api.jup.ag)

Compatibility knobs:
CHECK_JUP_USE_LIGHTNING_HEADERS=1|0   (default: 1 if lightning_impl provides headers)
CHECK_JUP_ADD_USER_PUBLIC_KEY=1|0     (default: 1, best-effort pubkey load)
CHECK_JUP_ADD_AS_LEGACY=auto|0|1      (default: auto; true for /swap/v1/quote)
"""

import argparse
import asyncio
import contextlib
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

# -----------------------------
# Constants / patterns
# -----------------------------
_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}

_URL_CRED_RE = re.compile(r"(://)([^/@\s]+)@")
_QS_SECRET_KEY_RE = re.compile(
    r"(?i)^(api[-_]?key|apikey|token|key|secret|sig|signature|auth|authorization|access[-_]?token)$"
)
_LONG_TOKENISH_RE = re.compile(r"\b[A-Za-z0-9_\-]{28,}\b")
_HDR_SENSITIVE_RE = re.compile(r"(?i)(authorization|x-api-key|api-key|token|secret|signature)")

KNOWN_SOL_MINT = "So11111111111111111111111111111111111111112"
KNOWN_USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

PUBLIC_HOSTS = {"api.jup.ag", "public.jupiterapi.com", "quote-api.jup.ag", "lite-api.jup.ag"}


# -----------------------------
# Env helpers
# -----------------------------
def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    return (v if v is not None else d).strip()


def _env_has(k: str) -> bool:
    return os.getenv(k) is not None


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


def _split_csv(s: str) -> list[str]:
    return [p.strip() for p in (s or "").replace(";", ",").split(",") if p.strip()]


def _ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _safe_print(s: str, *, file) -> None:
    try:
        file.write(s + ("" if s.endswith("\n") else "\n"))
        file.flush()
    except UnicodeEncodeError:
        enc = getattr(file, "encoding", None) or "utf-8"
        b = s.encode(enc, errors="replace")
        with contextlib.suppress(Exception):
            file.buffer.write(b + b"\n")
            file.buffer.flush()


def _hlog(msg: str, *, json_only: bool) -> None:
    if json_only:
        return
    _safe_print(f"[check_jupiter] {_ts()} {msg}", file=sys.stderr)


def _mask_long_tokens_enabled() -> bool:
    if _env_has("CHECK_JUP_MASK_LONG_TOKENS"):
        return _envb("CHECK_JUP_MASK_LONG_TOKENS", True)
    return _envb("PREFLIGHT_MASK_LONG_TOKENS", True)


# -----------------------------
# Redaction / safety
# -----------------------------
def _has_scheme(url: str) -> bool:
    u = (url or "").strip().lower()
    return u.startswith("http://") or u.startswith("https://")


def _host_from_url(url: str) -> str:
    try:
        p = urlparse(url)
        return (p.hostname or "").strip().lower()
    except Exception:
        return ""


def _redact_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    u = _URL_CRED_RE.sub(r"\1***@", u)

    try:
        p = urlparse(u)
        q_out: list[tuple[str, str]] = []
        for k, v in parse_qsl(p.query, keep_blank_values=True):
            kk = (k or "").strip()
            vv = "" if v is None else str(v)
            if _QS_SECRET_KEY_RE.match(kk) or len(vv) >= 24:
                q_out.append((kk, "***"))
            else:
                q_out.append((kk, vv))
        query = urlencode(q_out, doseq=True)

        path = p.path or ""
        if len(path) > 80:
            parts = [x for x in path.split("/") if x]
            path = "/" + (parts[0] if parts else "…") + "/…"

        out = urlunparse(p._replace(path=path, query=query))
    except Exception:
        out = u.split("?", 1)[0] + "?..." if "?" in u else u

    if _mask_long_tokens_enabled():
        out = _LONG_TOKENISH_RE.sub("***", out)
    return out


def _sanitize_headers(h: dict[str, str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for k, v in (h or {}).items():
        kk = str(k or "")
        vv = str(v or "")
        if _HDR_SENSITIVE_RE.search(kk):
            out[kk] = "***"
        else:
            out[kk] = "***" if (_mask_long_tokens_enabled() and len(vv) >= 24) else vv
    return out


def _looks_like_placeholder_key(key: str) -> bool:
    s = (key or "").strip()
    if not s:
        return False
    if "$" in s or "{" in s or "}" in s:
        return True
    return s.lower() in {"changeme", "replace_me", "your_key_here"}


def _extract_error(j: Any, fallback: str = "") -> str:
    if not isinstance(j, dict):
        return fallback
    for k in ("error", "message", "msg"):
        v = j.get(k)
        if v:
            return str(v)[:400]
    return fallback


def _coerce_dict(x: Any) -> dict[str, Any] | None:
    if isinstance(x, dict):
        return x
    if isinstance(x, (list, tuple)):
        ds = [i for i in x if isinstance(i, dict)]
        if not ds:
            return None
        out = dict(ds[0])
        if len(ds) >= 2:
            out["_meta"] = dict(ds[1])
        return out
    return None


def _mask_pubkey(pk: str) -> str:
    s = (pk or "").strip()
    if len(s) <= 12:
        return "***"
    return f"{s[:4]}…{s[-4:]}"


# -----------------------------
# HTTP client (httpx preferred; aiohttp fallback; urllib last)
# -----------------------------
class _Http:
    def __init__(self, timeout_s: float) -> None:
        self.timeout_s = float(timeout_s)
        self._httpx = None
        self._aiohttp = None

    async def __aenter__(self) -> "_Http":
        with contextlib.suppress(Exception):
            import httpx  # type: ignore

            self._httpx = httpx.AsyncClient(timeout=httpx.Timeout(self.timeout_s))
            return self

        with contextlib.suppress(Exception):
            import aiohttp  # type: ignore

            self._aiohttp = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.timeout_s)
            )
            return self

        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        with contextlib.suppress(Exception):
            if self._httpx is not None:
                await self._httpx.aclose()
        with contextlib.suppress(Exception):
            if self._aiohttp is not None:
                await self._aiohttp.close()

    async def get_json(
        self, url: str, params: dict[str, Any], headers: dict[str, str]
    ) -> tuple[int, Any, str]:
        if self._httpx is not None:
            r = await self._httpx.get(url, params=params, headers=headers)
            st = int(getattr(r, "status_code", 0) or 0)
            try:
                return st, r.json(), ""
            except Exception:
                txt = (getattr(r, "text", "") or "").strip()
                return st, None, (txt[:400] + "…") if len(txt) > 400 else txt

        if self._aiohttp is not None:
            async with self._aiohttp.get(url, params=params, headers=headers) as r:
                st = int(r.status)
                txt = ""
                try:
                    j = await r.json(content_type=None)
                    return st, j, ""
                except Exception:
                    with contextlib.suppress(Exception):
                        txt = (await r.text())[:400]
                    return st, None, txt or "bad_json"

        return await _http_get_json_urllib(url, params, self.timeout_s, headers)

    async def post_json(
        self, url: str, body: dict[str, Any], headers: dict[str, str]
    ) -> tuple[int, Any, str]:
        if self._httpx is not None:
            r = await self._httpx.post(url, json=body, headers=headers)
            st = int(getattr(r, "status_code", 0) or 0)
            try:
                return st, r.json(), ""
            except Exception:
                txt = (getattr(r, "text", "") or "").strip()
                return st, None, (txt[:400] + "…") if len(txt) > 400 else txt

        if self._aiohttp is not None:
            async with self._aiohttp.post(url, json=body, headers=headers) as r:
                st = int(r.status)
                txt = ""
                try:
                    j = await r.json(content_type=None)
                    return st, j, ""
                except Exception:
                    with contextlib.suppress(Exception):
                        txt = (await r.text())[:400]
                    return st, None, txt or "bad_json"

        return await _http_post_json_urllib(url, body, self.timeout_s, headers)


async def _http_get_json_urllib(
    url: str, params: dict[str, Any], timeout_s: float, headers: dict[str, str]
) -> tuple[int, Any, str]:
    import urllib.request

    def _sync() -> tuple[int, Any, str]:
        q = urlencode({k: v for k, v in (params or {}).items() if v is not None}, doseq=True)
        full = url + ("&" if "?" in url else "?") + q if q else url
        req = urllib.request.Request(
            url=full,
            headers={**{"accept": "application/json"}, **(headers or {})},
            method="GET",
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                status = int(getattr(resp, "status", 200) or 200)
                body = resp.read().decode("utf-8", "ignore")
        except Exception as e:
            return 0, None, f"{type(e).__name__}: {e}"
        try:
            return status, json.loads(body), ""
        except Exception:
            b = (body or "").strip()
            return status, None, (b[:400] + "…") if len(b) > 400 else b

    return await asyncio.to_thread(_sync)


async def _http_post_json_urllib(
    url: str, body: dict[str, Any], timeout_s: float, headers: dict[str, str]
) -> tuple[int, Any, str]:
    import urllib.request

    def _sync() -> tuple[int, Any, str]:
        data = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(
            url=url,
            data=data,
            headers={
                **{"content-type": "application/json", "accept": "application/json"},
                **(headers or {}),
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                status = int(getattr(resp, "status", 200) or 200)
                body_txt = resp.read().decode("utf-8", "ignore")
        except Exception as e:
            return 0, None, f"{type(e).__name__}: {e}"
        try:
            return status, json.loads(body_txt), ""
        except Exception:
            b = (body_txt or "").strip()
            return status, None, (b[:400] + "…") if len(b) > 400 else b

    return await asyncio.to_thread(_sync)


# -----------------------------
# Attempt model
# -----------------------------
@dataclass
class Attempt:
    label: str
    base_url: str
    base_url_redacted: str
    quote_path: str
    swap_path: str
    quote_url_redacted: str
    swap_url_redacted: str
    ok_quote: bool
    ok_swap_build: bool
    elapsed_ms: int
    http_status_quote: int
    http_status_swap: int
    warnings: list[str]
    error: str
    quote_brief: dict[str, Any]
    swap_brief: dict[str, Any]
    score: float

    def public(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "base_url": self.base_url_redacted,
            "quote_path": self.quote_path,
            "swap_path": self.swap_path,
            "quote_url": self.quote_url_redacted,
            "swap_url": self.swap_url_redacted,
            "ok_quote": int(self.ok_quote),
            "ok_swap_build": int(self.ok_swap_build),
            "elapsed_ms": int(self.elapsed_ms),
            "http_quote": int(self.http_status_quote),
            "http_swap": int(self.http_status_swap),
            "score": round(float(self.score), 6),
            "warnings": list(self.warnings or []),
            "error": self.error,
            "quote": self.quote_brief,
            "swap": self.swap_brief,
        }


def _score_attempt(a: Attempt, *, strict: bool, swap_test: bool) -> float:
    s = 0.0
    if a.ok_quote:
        s += 12.0
    if swap_test and a.ok_swap_build:
        s += 4.0
    if strict and a.ok_quote:
        s += 2.0

    if a.http_status_quote == 429 or a.http_status_swap == 429:
        s -= 4.0
    if a.http_status_quote == 401:
        s -= 6.0
    if a.error:
        s -= 4.0

    s -= min(4.0, float(a.elapsed_ms) / 650.0)
    s -= min(2.0, 0.35 * len(a.warnings or []))
    return s


# -----------------------------
# Resolution helpers
# -----------------------------
def _resolve_slippage_bps(cli_override: int) -> tuple[int, str]:
    if cli_override >= 0:
        return int(cli_override), "CLI"
    for k in ("PF_SLIPPAGE_BPS", "SLIPPAGE_BPS", "JUP_SLIPPAGE_BPS"):
        v = _envs(k, "")
        if v:
            with contextlib.suppress(Exception):
                return int(float(v)), k
    return 50, "default(50)"


def _resolve_api_key() -> tuple[str, str]:
    for k in ("JUP_PRO_KEY", "JUP_API_KEY", "JUP_KEY"):
        v = _envs(k, "")
        if v:
            return v, k
    return "", ""


def _skip_by_default() -> bool:
    if _envb("CHECK_JUP_DISABLED", False):
        return True
    return bool(
        _envb("KILL_SWITCH", False) or _envb("DISABLE_BUYS", False) or _envb("DRYRUN", False)
    )


def _combine_base_path(base: str, path: str) -> str:
    """
    Safe combiner that supports base URLs that already include a path prefix.
    Example:
      base=https://api.jup.ag/swap/v1, path=/quote  -> https://api.jup.ag/swap/v1/quote
    """
    b = (base or "").strip()
    pth = (path or "").strip()

    if not pth.startswith("/"):
        pth = "/" + pth if pth else ""

    try:
        u = urlparse(b)
        prefix = (u.path or "").rstrip("/")
        new_path = (prefix + pth) if prefix else pth
        return urlunparse(u._replace(path=new_path))
    except Exception:
        # best-effort fallback
        b2 = b.rstrip("/")
        return b2 + pth


def _candidate_bases(cli_bases: list[str]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for i, b in enumerate(cli_bases):
        if b:
            out.append((f"CLI_{i + 1}", b.strip()))

    for k in ("JUPITER_SWAP_API", "JUP_BASE_URL"):
        v = _envs(k, "")
        if v:
            out.append((k, v.strip()))

    for u in _split_csv(_envs("JUP_ALT_BASE_URLS", "")):
        out.append(("JUP_ALT_BASE_URLS", u.strip()))

    # Always include quote-api as a *secondary* option if user enables public fallback.
    # (This can help in some network/DNS setups.)
    if _envb("JUP_ALLOW_PUBLIC_FALLBACK", False):
        out.append(("PUBLIC_FALLBACK", "https://api.jup.ag"))
        out.append(("PUBLIC_FALLBACK", "https://quote-api.jup.ag"))

    seen: set[str] = set()
    uniq: list[tuple[str, str]] = []
    for lbl, u in out:
        uu = u.strip()
        if not uu or uu in seen:
            continue
        seen.add(uu)
        uniq.append((lbl, uu))
    return uniq


def _path_variants_for_base(base: str, quote_path: str, swap_path: str) -> list[tuple[str, str]]:
    """
    Known shapes:
      api.jup.ag      -> /swap/v1/quote , /swap/v1/swap   (preferred)
                      -> /quote , /swap                   (legacy-ish)
      quote-api.jup.ag-> /v6/quote                        (quote-only)
      public.jupiter  -> /quote , /swap

    We generate a small, deterministic set of variants and score them.
    """
    host = _host_from_url(base)
    q = (quote_path or "").strip() or "/swap/v1/quote"
    s = (swap_path or "").strip() or "/swap/v1/swap"

    variants: list[tuple[str, str]] = []

    if host == "api.jup.ag":
        # Prefer swap/v1
        variants.append(("/swap/v1/quote", "/swap/v1/swap"))
        variants.append(("/quote", "/swap"))
    elif host == "quote-api.jup.ag":
        variants.append(("/v6/quote", ""))  # quote-only
    elif host == "public.jupiterapi.com":
        variants.append(("/quote", "/swap"))
        variants.append(("/swap/v1/quote", "/swap/v1/swap"))
    else:
        # Whatever user provided + a sensible fallback
        variants.append((q, s))
        if q != "/swap/v1/quote":
            variants.append(("/swap/v1/quote", "/swap/v1/swap"))

    seen: set[tuple[str, str]] = set()
    out: list[tuple[str, str]] = []
    for qq, ss in variants:
        key = (qq, ss)
        if key in seen:
            continue
        seen.add(key)
        out.append((qq, ss))
    return out


def _load_user_pubkey_best_effort() -> str:
    for k in ("CHECK_JUP_USER_PUBKEY", "WALLET_PUBKEY", "PUBLIC_KEY"):
        v = _envs(k, "")
        if v:
            return v.strip()

    with contextlib.suppress(Exception):
        from utils_pkg.solana_wallet import get_pubkey_str, load_keypair  # type: ignore

        kp = load_keypair()
        return get_pubkey_str(kp)

    return ""


def _lightning_diag() -> dict[str, Any]:
    info: dict[str, Any] = {"available": 0}
    with contextlib.suppress(Exception):
        from utils_pkg import lightning_impl as L  # type: ignore

        info["available"] = 1
        info["BASE"] = _redact_url(str(getattr(L, "BASE", "") or ""))
        info["AS_LEGACY"] = int(bool(getattr(L, "AS_LEGACY", False)))
        with contextlib.suppress(Exception):
            h = L._jup_headers()
            if isinstance(h, dict):
                info["headers"] = _sanitize_headers({str(k): str(v) for k, v in h.items()})
                info["has_x_api_key"] = int("x-api-key" in {str(k).lower() for k in h})
        with contextlib.suppress(Exception):
            vf = L._variant_flags()
            info["variant_flags"] = vf if isinstance(vf, dict) else str(vf)
    return info


def _build_headers(*, api_key: str, use_lightning_headers: bool) -> dict[str, str]:
    """
    Best-effort mirror of bot headers without importing heavy deps.
    - If lightning_impl provides headers and CHECK_JUP_USE_LIGHTNING_HEADERS=1, use them.
    - Otherwise use safe defaults that tend to work with Jupiter.
    """
    base: dict[str, str] = {
        "accept": "application/json",
        "content-type": "application/json",
        "user-agent": "pnlsniper",
        "origin": "https://jup.ag",
        "referer": "https://jup.ag/",
    }

    if use_lightning_headers:
        with contextlib.suppress(Exception):
            from utils_pkg import lightning_impl as L  # type: ignore

            h = L._jup_headers()
            if isinstance(h, dict):
                # merge, lightning wins (except we may set x-api-key below)
                base.update({str(k): str(v) for k, v in h.items()})

    if api_key:
        base["x-api-key"] = api_key
    return base


def _resolve_key_policy(bases: list[tuple[str, str]], *, strict: bool) -> bool:
    """
    Policy:
    - api.jup.ag now requires API key by default.
    - STRICT_AUTH / ROUTER_STRICT_AUTH / JUP_STRICT_AUTH force requirement.
    - CHECK_JUP_REQUIRE_API_KEY overrides everything if explicitly set.
    """
    if _env_has("CHECK_JUP_REQUIRE_API_KEY"):
        return _envb("CHECK_JUP_REQUIRE_API_KEY", False)

    if (
        _envb("STRICT_AUTH", False)
        or _envb("ROUTER_STRICT_AUTH", False)
        or _envb("JUP_STRICT_AUTH", False)
    ):
        return True

    if strict:
        return True

    # If any candidate base is api.jup.ag, require key by default (current Jupiter policy).
    for _, b in bases:
        if _host_from_url(b) == "api.jup.ag":
            return True

    return False


def _fix_hints(attempts: list[Attempt], *, require_api_key: bool, has_api_key: bool) -> list[str]:
    hints: list[str] = []
    if not attempts:
        return hints

    any_401 = any(a.http_status_quote == 401 for a in attempts)
    any_429 = any(a.http_status_quote == 429 or a.http_status_swap == 429 for a in attempts)
    any_bad_url = any("bad_url_format" in (a.error or "") for a in attempts)
    any_placeholder_key = any("placeholder_api_key" in (a.warnings or []) for a in attempts)
    any_quote_fail = all(not a.ok_quote for a in attempts)

    if any_bad_url:
        hints.append(
            "At least one base URL is missing http(s):// — fix JUP_BASE_URL/JUPITER_SWAP_API/JUP_ALT_BASE_URLS."
        )

    if require_api_key and not has_api_key:
        hints.append("Missing API key. Set JUP_PRO_KEY (preferred) or JUP_API_KEY and retry.")
    elif any_401:
        hints.append(
            "401 Unauthorized from Jupiter. Ensure x-api-key is being sent (JUP_PRO_KEY / JUP_API_KEY)."
        )

    if any_placeholder_key:
        hints.append(
            "Your API key looks like a placeholder (e.g. '$JUP_API_KEY'). Set JUP_PRO_KEY to the actual key value."
        )

    if any_429:
        hints.append(
            "Rate limiting detected (429). Add API key and/or reduce QPS, or rotate bases via JUP_ALT_BASE_URLS."
        )

    if any_quote_fail:
        hints.append(
            "No viable quote returned. Check mints, amount, slippage, and base/path combination (swap/v1 vs legacy vs v6)."
        )

    out: list[str] = []
    seen: set[str] = set()
    for h in hints:
        if h not in seen:
            seen.add(h)
            out.append(h)
    return out[:10]


def _quote_ok_brief(jd: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
    out_amt = jd.get("outAmount")
    rp = jd.get("routePlan")
    routes = len(rp) if isinstance(rp, list) else (0 if rp is not None else 1)
    ok = bool(out_amt) and routes >= 1
    brief = {
        "ok": int(ok),
        "outAmount": str(out_amt)[:80] if out_amt is not None else None,
        "routes": int(routes),
        "contextSlot": jd.get("contextSlot"),
        "timeTaken": jd.get("timeTaken"),
    }
    return ok, brief


def _resolve_as_legacy_flag(quote_path: str) -> bool:
    """
    CHECK_JUP_ADD_AS_LEGACY:
      - "auto" (default): true only for /swap/v1/quote
      - "1"/"0": forced
    """
    v = _envs("CHECK_JUP_ADD_AS_LEGACY", "auto").strip().lower()
    if v in _TRUE:
        return True
    if v in _FALSE:
        return False
    return "/swap/v1/quote" in (quote_path or "")


# -----------------------------
# Probe logic
# -----------------------------
async def _probe_one(
    *,
    label: str,
    base_url: str,
    quote_path: str,
    swap_path: str,
    timeout_s: float,
    strict: bool,
    require_api_key: bool,
    allow_no_key: bool,
    swap_test: bool,
    require_swap_ok: bool,
    headers: dict[str, str],
    quote_params: dict[str, Any],
    user_pubkey: str,
) -> Attempt:
    t0 = time.monotonic()

    base_red = _redact_url(base_url)
    quote_url = _combine_base_path(base_url, quote_path)
    swap_url = _combine_base_path(base_url, swap_path) if swap_path else ""

    warnings: list[str] = []
    err = ""
    st_q = 0
    st_s = 0
    ok_quote = False
    ok_swap_build = False

    if not _has_scheme(base_url):
        a = Attempt(
            label=label,
            base_url=base_url,
            base_url_redacted=base_red,
            quote_path=quote_path,
            swap_path=swap_path,
            quote_url_redacted=_redact_url(quote_url),
            swap_url_redacted=_redact_url(swap_url),
            ok_quote=False,
            ok_swap_build=False,
            elapsed_ms=int((time.monotonic() - t0) * 1000),
            http_status_quote=0,
            http_status_swap=0,
            warnings=[],
            error="bad_url_format: expected http(s)://...",
            quote_brief={"ok": 0, "err": "bad_url_format"},
            swap_brief={"enabled": int(bool(swap_test)), "ok": 0, "err": "bad_url_format"},
            score=-999.0,
        )
        a.score = _score_attempt(a, strict=strict, swap_test=swap_test)
        return a

    has_key = bool(headers.get("x-api-key"))
    if require_api_key and not has_key:
        warnings.append("missing_api_key_required")

    if has_key and _looks_like_placeholder_key(headers.get("x-api-key", "")):
        warnings.append("placeholder_api_key")

    # Quote params: broaden compatibility + mirror bot-ish params when relevant.
    qp = dict(quote_params)
    qp.setdefault("swapMode", "ExactIn")

    # Add userPublicKey (best-effort) if enabled: some Jupiter setups expect it.
    if _envb("CHECK_JUP_ADD_USER_PUBLIC_KEY", True) and user_pubkey:
        qp.setdefault("userPublicKey", user_pubkey)

    # Add asLegacyTransaction (auto) for swap/v1 quote path
    as_legacy = _resolve_as_legacy_flag(quote_path)
    if "/swap/v1/quote" in (quote_path or ""):
        qp.setdefault("asLegacyTransaction", "true" if as_legacy else "false")

    # Ensure onlyDirectRoutes is string-ish for broad compatibility
    if "onlyDirectRoutes" in qp:
        qp["onlyDirectRoutes"] = "true" if bool(qp["onlyDirectRoutes"]) else "false"

    quote_brief: dict[str, Any] = {"ok": 0}
    swap_brief: dict[str, Any] = {"enabled": int(bool(swap_test)), "ok": 0}

    try:
        async with _Http(timeout_s=timeout_s) as H:
            st_q, j, e = await H.get_json(quote_url, qp, headers)
            st_q = int(st_q or 0)

            if st_q == 429:
                warnings.append("http_429_rate_limited_quote")

            if st_q == 401:
                warnings.append("http_401_unauthorized_quote")

            if st_q == 0:
                err = err or (e or "connect_failed")
            else:
                jd = _coerce_dict(j)
                if jd is None:
                    err = err or (e or f"http_{st_q}_bad_json")
                else:
                    j_err = _extract_error(jd, "")
                    if j_err and st_q >= 400:
                        err = err or j_err
                        quote_brief = {"ok": 0, "err": j_err[:240]}
                    else:
                        ok_quote, quote_brief = _quote_ok_brief(jd)
                        if not ok_quote and st_q >= 400 and not err:
                            err = err or f"http_{st_q}"

            # Optional swap build only if swap_url present + quote ok
            if swap_test and ok_quote and swap_url:
                if not user_pubkey:
                    swap_brief = {"enabled": 1, "ok": 0, "err": "missing_user_pubkey_for_swap_test"}
                    if strict and require_swap_ok:
                        err = err or "strict_fail: missing_user_pubkey_for_swap_test"
                else:
                    body: dict[str, Any] = {
                        "quoteResponse": _coerce_dict(j) or j,
                        "userPublicKey": user_pubkey,
                        "asLegacyTransaction": bool(as_legacy),
                        "useSharedAccounts": bool(_envb("JUP_USE_SHARED_ACCOUNTS", False)),
                        "wrapAndUnwrapSol": bool(_envb("PF_WRAP_UNWRAP_SOL", True)),
                    }
                    st_s, j2, e2 = await H.post_json(swap_url, body, headers)
                    st_s = int(st_s or 0)

                    if st_s == 429:
                        warnings.append("http_429_rate_limited_swap")
                    if st_s == 401:
                        warnings.append("http_401_unauthorized_swap")

                    if st_s == 0:
                        if strict and require_swap_ok:
                            err = err or (e2 or "swap_connect_failed")
                        swap_brief = {
                            "enabled": 1,
                            "ok": 0,
                            "http_status": 0,
                            "err": e2 or "swap_connect_failed",
                        }
                    else:
                        jd2 = _coerce_dict(j2)
                        if jd2 is None:
                            if strict and require_swap_ok:
                                err = err or (e2 or f"swap_http_{st_s}_bad_json")
                            swap_brief = {
                                "enabled": 1,
                                "ok": 0,
                                "http_status": st_s,
                                "err": e2 or "swap_bad_json",
                            }
                        else:
                            j2_err = _extract_error(jd2, "")
                            if j2_err and st_s >= 400:
                                if strict and require_swap_ok:
                                    err = err or j2_err
                                swap_brief = {
                                    "enabled": 1,
                                    "ok": 0,
                                    "http_status": st_s,
                                    "err": j2_err[:240],
                                }
                            else:
                                tx = (
                                    jd2.get("swapTransaction")
                                    or jd2.get("swapTransactionB64")
                                    or None
                                )
                                ok_swap_build = bool(tx)
                                swap_brief = {
                                    "enabled": 1,
                                    "ok": int(ok_swap_build),
                                    "http_status": st_s,
                                    "has_tx": int(bool(tx)),
                                    "tx_len": int(len(str(tx))) if tx else 0,
                                    "lastValidBlockHeight": jd2.get("lastValidBlockHeight"),
                                }
                                if strict and require_swap_ok and not ok_swap_build:
                                    err = err or "strict_fail: swap_missing_transaction"

    except asyncio.CancelledError:
        raise
    except Exception as ex:
        err = err or f"{type(ex).__name__}: {ex}"

    # If key is required and missing, we want deterministic error classification unless allow_no_key is set.
    if require_api_key and not headers.get("x-api-key") and not allow_no_key:
        err = err or "missing_api_key_required"

    elapsed_ms = int((time.monotonic() - t0) * 1000)
    a = Attempt(
        label=label,
        base_url=base_url,
        base_url_redacted=base_red,
        quote_path=quote_path,
        swap_path=swap_path,
        quote_url_redacted=_redact_url(quote_url),
        swap_url_redacted=_redact_url(swap_url),
        ok_quote=bool(ok_quote),
        ok_swap_build=bool(ok_swap_build),
        elapsed_ms=elapsed_ms,
        http_status_quote=int(st_q),
        http_status_swap=int(st_s),
        warnings=warnings,
        error=err,
        quote_brief=quote_brief,
        swap_brief=swap_brief,
        score=0.0,
    )
    a.score = _score_attempt(a, strict=strict, swap_test=swap_test)
    return a


# -----------------------------
# Main
# -----------------------------
async def _amain() -> int:
    RC_OK = 0
    RC_CONNECT_FAIL = 1
    RC_REQUIREMENTS_FAIL = 2
    RC_BAD_URL = 3
    RC_TIMEOUT = 124

    ap = argparse.ArgumentParser(description="God-Tier Jupiter precheck (LOCK-IN / PROD).")
    ap.add_argument("--base", action="append", default=[], help="Jupiter base URL (repeatable).")
    ap.add_argument(
        "--quote-path", default=_envs("JUP_QUOTE_PATH", "/swap/v1/quote"), help="Quote path."
    )
    ap.add_argument(
        "--swap-path", default=_envs("JUP_SWAP_PATH", "/swap/v1/swap"), help="Swap path."
    )
    ap.add_argument(
        "--timeout",
        type=float,
        default=float(_envf("CHECK_JUP_TIMEOUT_S", 12.0)),
        help="Per-call timeout seconds.",
    )
    ap.add_argument(
        "--strict", action="store_true", help="Strict gates (requires API key if policy says so)."
    )
    ap.add_argument(
        "--try-all", action="store_true", help="Try all candidates and pick best by score."
    )
    ap.add_argument(
        "--swap-test",
        action="store_true",
        help="Also try swap-build (unsigned tx) if quote succeeds.",
    )
    ap.add_argument(
        "--require-swap-ok", action="store_true", help="In strict+swap-test, require swap build OK."
    )

    ap.add_argument("--json", action="store_true", help="Emit JSON summary to STDOUT.")
    ap.add_argument("--json-only", action="store_true", help="Only JSON (no human logs).")

    ap.add_argument(
        "--input-mint", default=_envs("CHECK_JUP_INPUT_MINT", KNOWN_SOL_MINT), help="Input mint."
    )
    ap.add_argument(
        "--output-mint",
        default=_envs("CHECK_JUP_OUTPUT_MINT", KNOWN_USDC_MINT),
        help="Output mint.",
    )
    ap.add_argument(
        "--amount-sol",
        type=float,
        default=float(_envf("CHECK_JUP_AMOUNT_SOL", 0.005)),
        help="Input amount in SOL (if input is SOL).",
    )
    ap.add_argument(
        "--amount-raw",
        type=int,
        default=int(_envi("CHECK_JUP_AMOUNT_RAW", 0)),
        help="Override amount in base units (lamports for SOL).",
    )
    ap.add_argument("--slippage-bps", type=int, default=-1, help="Override slippage bps.")
    args = ap.parse_args()

    strict = bool(args.strict or _envb("CHECK_JUP_STRICT", False))
    try_all = bool(args.try_all or _envb("CHECK_JUP_TRY_ALL", False))

    emit_json = bool(
        args.json
        or args.json_only
        or _envb("CHECK_JUP_JSON", False)
        or _envb("CHECK_JUP_JSON_ONLY", False)
    )
    json_only = bool(args.json_only or _envb("CHECK_JUP_JSON_ONLY", False))

    explicit_intent = bool(
        args.base
        or args.strict
        or args.try_all
        or args.swap_test
        or emit_json
        or args.amount_raw > 0
    )

    if _envb("CHECK_JUP_DISABLED", False):
        out = {"ok": True, "skipped": True, "reason": "CHECK_JUP_DISABLED=1", "ts": _ts()}
        if emit_json:
            print(json.dumps(out, ensure_ascii=False))
        _hlog("SKIP (CHECK_JUP_DISABLED=1)", json_only=json_only)
        return RC_OK

    if _skip_by_default() and not explicit_intent:
        out = {
            "ok": True,
            "skipped": True,
            "reason": "buys disabled (DRYRUN/KILL_SWITCH/DISABLE_BUYS)",
            "ts": _ts(),
        }
        if emit_json:
            print(json.dumps(out, ensure_ascii=False))
        _hlog("SKIP (buys disabled; no explicit intent)", json_only=json_only)
        return RC_OK

    timeout_s = max(2.0, float(args.timeout))

    # Bases
    bases = _candidate_bases([b for b in args.base if b])
    if not bases:
        msg = "No Jupiter base URL found. Set JUP_BASE_URL (or JUPITER_SWAP_API) or pass --base."
        out = {"ok": False, "error": "missing_jupiter_base", "detail": msg, "ts": _ts()}
        if emit_json:
            print(json.dumps(out, ensure_ascii=False))
        _hlog("FAIL " + msg, json_only=json_only)
        return RC_CONNECT_FAIL

    # API key + headers
    api_key, api_key_src = _resolve_api_key()
    use_lightning_headers = _envb("CHECK_JUP_USE_LIGHTNING_HEADERS", True)
    headers = _build_headers(api_key=api_key, use_lightning_headers=use_lightning_headers)

    placeholder_key = int(bool(api_key and _looks_like_placeholder_key(api_key)))

    # Key policy
    require_api_key = _resolve_key_policy(bases, strict=strict)
    allow_no_key = _envb("CHECK_JUP_ALLOW_NO_KEY", False)

    # Swap test knobs
    swap_test = bool(args.swap_test or _envb("CHECK_JUP_SWAP_TEST", False))
    require_swap_ok = bool(args.require_swap_ok or _envb("CHECK_JUP_REQUIRE_SWAP_OK", False))

    # Load pubkey (best effort)
    user_pubkey = _load_user_pubkey_best_effort()

    # Slippage
    slippage_bps, slippage_src = _resolve_slippage_bps(args.slippage_bps)

    # Amount
    if args.amount_raw > 0:
        amount = int(args.amount_raw)
        amount_src = "CLI_amount_raw"
    else:
        if args.input_mint.strip() == KNOWN_SOL_MINT:
            amount = int(max(1, round(float(args.amount_sol) * 1_000_000_000)))
            amount_src = "amount_sol_to_lamports"
        else:
            amount = 0
            amount_src = "missing_amount_raw"

    only_direct = bool(_envb("JUP_ONLY_DIRECT", False))

    quote_params: dict[str, Any] = {
        "inputMint": args.input_mint.strip(),
        "outputMint": args.output_mint.strip(),
        "amount": int(amount),
        "slippageBps": int(slippage_bps),
        "onlyDirectRoutes": bool(only_direct),
    }

    # Optional mint guard (best-effort)
    guard_note = ""
    with contextlib.suppress(Exception):
        from utils_pkg.mint_guard import guard_jupiter_quote_args  # type: ignore

        im, om, amt = guard_jupiter_quote_args(
            quote_params["inputMint"],
            quote_params["outputMint"],
            amount_raw=int(amount),
            require_anchor=False,
        )  # type: ignore
        quote_params["inputMint"], quote_params["outputMint"], quote_params["amount"] = (
            str(im),
            str(om),
            int(amt),
        )
        guard_note = "mint_guard_applied"

    # lightning diag
    ldiag = _lightning_diag()

    _hlog(
        "BOOT "
        f"strict={int(strict)} try_all={int(try_all)} timeout_s={timeout_s:.1f} "
        f"has_api_key={int(bool(api_key))} api_key_src={api_key_src or '-'} placeholder_key={placeholder_key} "
        f"require_api_key={int(require_api_key)} allow_no_key={int(allow_no_key)} "
        f"swap_test={int(swap_test)} require_swap_ok={int(require_swap_ok)} "
        f"slippage_bps={slippage_bps}({slippage_src}) only_direct={int(only_direct)} "
        f"amount={int(quote_params['amount'])} amount_src={amount_src} {guard_note} "
        f"headers={_sanitize_headers(headers)} bases={[lbl for lbl, _ in bases]} "
        f"user_pubkey={'-' if not user_pubkey else _mask_pubkey(user_pubkey)} "
        f"lightning={ldiag}",
        json_only=json_only,
    )

    # Deterministic early fail if key required and missing (unless allow_no_key)
    if require_api_key and not headers.get("x-api-key") and not allow_no_key:
        summary = {
            "ts": _ts(),
            "ok": False,
            "strict": int(strict),
            "try_all": int(try_all),
            "timeout_s": float(timeout_s),
            "has_api_key": 0,
            "api_key_src": "",
            "placeholder_api_key": 0,
            "require_api_key": 1,
            "swap_test": int(swap_test),
            "require_swap_ok": int(require_swap_ok),
            "picked": None,
            "picked_base": None,
            "picked_quote": None,
            "picked_swap": None,
            "slippage_bps": int(slippage_bps),
            "quote_params": {
                "inputMint": quote_params["inputMint"],
                "outputMint": quote_params["outputMint"],
                "amount": int(quote_params["amount"]),
                "onlyDirectRoutes": int(bool(only_direct)),
            },
            "lightning_impl": ldiag,
            "results": [],
            "fix_hints": ["Missing API key. Set JUP_PRO_KEY (preferred) or JUP_API_KEY and retry."],
        }
        if emit_json or json_only:
            print(json.dumps(summary, ensure_ascii=False, default=str))
        _hlog("SUMMARY ok=0 (missing required API key)", json_only=json_only)
        return RC_REQUIREMENTS_FAIL

    # Probe attempts
    attempts: list[Attempt] = []
    any_bad_url = False
    attempt_watchdog = max(6.0, timeout_s * 4.5)

    pre_warnings: list[str] = []
    if int(quote_params["amount"]) <= 0:
        pre_warnings.append(
            "missing_amount: for non-SOL inputMint you must set CHECK_JUP_AMOUNT_RAW or --amount-raw"
        )

    for base_lbl, base in bases:
        path_vars = _path_variants_for_base(base, args.quote_path, args.swap_path)
        for qi, (qpath, spath) in enumerate(path_vars, start=1):
            lbl = f"{base_lbl}#{qi}" if len(path_vars) > 1 else base_lbl
            _hlog(f"TRY {lbl} base={_redact_url(base)} q={qpath} s={spath}", json_only=json_only)

            try:
                a = await asyncio.wait_for(
                    _probe_one(
                        label=lbl,
                        base_url=base,
                        quote_path=qpath,
                        swap_path=spath,
                        timeout_s=timeout_s,
                        strict=strict,
                        require_api_key=require_api_key,
                        allow_no_key=allow_no_key,
                        swap_test=swap_test,
                        require_swap_ok=require_swap_ok,
                        headers=headers,
                        quote_params=quote_params,
                        user_pubkey=user_pubkey,
                    ),
                    timeout=attempt_watchdog,
                )
            except TimeoutError:
                a = Attempt(
                    label=lbl,
                    base_url=base,
                    base_url_redacted=_redact_url(base),
                    quote_path=qpath,
                    swap_path=spath,
                    quote_url_redacted=_redact_url(_combine_base_path(base, qpath)),
                    swap_url_redacted=_redact_url(_combine_base_path(base, spath) if spath else ""),
                    ok_quote=False,
                    ok_swap_build=False,
                    elapsed_ms=int(attempt_watchdog * 1000),
                    http_status_quote=0,
                    http_status_swap=0,
                    warnings=["attempt_timeout"],
                    error="attempt_timeout",
                    quote_brief={"ok": 0, "err": "timeout"},
                    swap_brief={"enabled": int(bool(swap_test)), "ok": 0, "err": "timeout"},
                    score=-999.0,
                )
                a.score = _score_attempt(a, strict=strict, swap_test=swap_test)

            if pre_warnings:
                a.warnings = list(pre_warnings) + list(a.warnings or [])

            any_bad_url = any_bad_url or ("bad_url_format" in (a.error or ""))

            attempts.append(a)

            if not json_only:
                _hlog(
                    f"RES {lbl} ok_quote={int(a.ok_quote)} ok_swap={int(a.ok_swap_build)} "
                    f"http_q={a.http_status_quote} http_s={a.http_status_swap} elapsed_ms={a.elapsed_ms} score={a.score:.3f}",
                    json_only=False,
                )
                for w in (a.warnings or [])[:10]:
                    _hlog(f"WARN {lbl} {w}", json_only=False)
                if a.error:
                    _hlog(f"ERR  {lbl} {a.error}", json_only=False)

            if not try_all:
                if strict:
                    if a.ok_quote and (not swap_test or a.ok_swap_build or not require_swap_ok):
                        break
                else:
                    if a.ok_quote:
                        break

        if not try_all and attempts and attempts[-1].ok_quote:
            break

    ok_quotes = [a for a in attempts if a.ok_quote]
    picked = max(ok_quotes, key=lambda x: x.score) if ok_quotes else None

    final_ok = bool(picked is not None)

    summary: dict[str, Any] = {
        "ts": _ts(),
        "ok": final_ok,
        "strict": int(strict),
        "try_all": int(try_all),
        "timeout_s": float(timeout_s),
        "has_api_key": int(bool(headers.get("x-api-key"))),
        "api_key_src": api_key_src or "",
        "placeholder_api_key": int(bool(api_key and _looks_like_placeholder_key(api_key))),
        "require_api_key": int(require_api_key),
        "swap_test": int(swap_test),
        "require_swap_ok": int(require_swap_ok),
        "picked": picked.label if picked else None,
        "picked_base": picked.base_url_redacted if picked else None,
        "picked_quote": picked.quote_url_redacted if picked else None,
        "picked_swap": picked.swap_url_redacted if picked else None,
        "slippage_bps": int(slippage_bps),
        "quote_params": {
            "inputMint": quote_params["inputMint"],
            "outputMint": quote_params["outputMint"],
            "amount": int(quote_params["amount"]),
            "onlyDirectRoutes": int(bool(only_direct)),
        },
        "lightning_impl": ldiag,
        "results": [a.public() for a in sorted(attempts, key=lambda x: x.score, reverse=True)],
        "fix_hints": _fix_hints(
            attempts, require_api_key=require_api_key, has_api_key=bool(headers.get("x-api-key"))
        ),
    }

    if emit_json or json_only:
        print(json.dumps(summary, ensure_ascii=False, default=str))

    if final_ok:
        _hlog("SUMMARY ok=1 (jupiter viable)", json_only=json_only)
        return RC_OK

    if any_bad_url:
        _hlog("SUMMARY ok=0 (bad url format present)", json_only=json_only)
        return RC_BAD_URL

    if attempts and all((a.error == "attempt_timeout") for a in attempts):
        _hlog("SUMMARY ok=0 (all attempts timed out)", json_only=json_only)
        return RC_TIMEOUT

    _hlog("SUMMARY ok=0 (requirements/connectivity failed)", json_only=json_only)
    return RC_REQUIREMENTS_FAIL if require_api_key else RC_CONNECT_FAIL


def main() -> None:
    try:
        rc = asyncio.run(_amain())
    except KeyboardInterrupt:
        rc = 130
    except Exception as e:
        _safe_print(
            f"[check_jupiter] {_ts()} FAIL unhandled {type(e).__name__}: {e}", file=sys.stderr
        )
        rc = 1
    raise SystemExit(rc)


if __name__ == "__main__":
    with contextlib.suppress(Exception):
        if os.name == "nt" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]
    main()
