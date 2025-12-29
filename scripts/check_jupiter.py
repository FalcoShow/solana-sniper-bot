# -*- coding: utf-8 -*-
from __future__ import annotations

"""
scripts/check_jupiter.py

God-Tier Jupiter Precheck (LOCK-IN / PROD)
=========================================

What this does
--------------
A deterministic, secret-safe sanity check for Jupiter QUOTE + (optional) SWAP
endpoints, designed to mirror how your bot actually talks to Jupiter.

It is intentionally:
- ✅ Strict about not leaking secrets (redaction everywhere)
- ✅ Resilient to endpoint variants (api.jup.ag vs public.jupiterapi.com, v1 paths)
- ✅ Useful in Render Shell (clean exit codes, JSON output for automation)
- ✅ Compatible with your stack (optionally introspects utils_pkg.lightning_impl)

Exit codes
----------
0   OK / skipped
1   Connect/respond failure for all candidates
2   Requirements failed (no viable quote, strict gates failed)
3   Bad URL format
124 All attempts timed out

Usage
-----
py -m scripts.check_jupiter
py -m scripts.check_jupiter --strict --try-all
py -m scripts.check_jupiter --swap-test --try-all
py -m scripts.check_jupiter --json-only
py -m scripts.check_jupiter --base https://api.jup.ag --quote-path /swap/v1/quote

Env (most relevant)
-------------------
JUP_BASE_URL=https://api.jup.ag
JUPITER_SWAP_API=...                 (optional override base)
JUP_ALT_BASE_URLS=csv                (optional)

Paths:
JUP_QUOTE_PATH=/swap/v1/quote
JUP_SWAP_PATH=/swap/v1/swap

Auth:
JUP_PRO_KEY / JUP_API_KEY / JUP_KEY  (sent as x-api-key)
CHECK_JUP_REQUIRE_API_KEY=0|1        (strict implies 1 for public endpoints)

Behavior:
CHECK_JUP_DISABLED=0|1
CHECK_JUP_STRICT=0|1
CHECK_JUP_TRY_ALL=0|1
CHECK_JUP_TIMEOUT_S=12
CHECK_JUP_SWAP_TEST=0|1
CHECK_JUP_REQUIRE_SWAP_OK=0|1

Gates (skip unless explicit intent):
DISABLE_BUYS=1 / KILL_SWITCH=1 / DRYRUN=1 -> skip by default
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
from typing import Any, Dict, List, Optional, Sequence, Tuple
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

# Jupiter “typical” bases
PUBLIC_HOSTS = {"api.jup.ag", "public.jupiterapi.com"}


# -----------------------------
# Env helpers
# -----------------------------
def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    return (v if v is not None else d).strip()


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


def _split_csv(s: str) -> List[str]:
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
    if "CHECK_JUP_MASK_LONG_TOKENS" in os.environ:
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
        q_out: List[Tuple[str, str]] = []
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


def _sanitize_headers(h: Dict[str, str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
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
    if s.lower() in {"changeme", "replace_me", "your_key_here"}:
        return True
    return False


def _extract_error(j: Any, fallback: str = "") -> str:
    if not isinstance(j, dict):
        return fallback
    for k in ("error", "message", "msg"):
        v = j.get(k)
        if v:
            return str(v)[:400]
    return fallback


def _coerce_dict(x: Any) -> Optional[Dict[str, Any]]:
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


# -----------------------------
# HTTP client (httpx preferred; aiohttp fallback)
# -----------------------------
class _Http:
    def __init__(self, timeout_s: float) -> None:
        self.timeout_s = float(timeout_s)
        self._httpx = None
        self._aiohttp = None

    async def __aenter__(self) -> "_Http":
        # Prefer httpx if available
        with contextlib.suppress(Exception):
            import httpx  # type: ignore

            self._httpx = httpx.AsyncClient(timeout=httpx.Timeout(self.timeout_s))
            return self

        # Fallback: aiohttp if available
        with contextlib.suppress(Exception):
            import aiohttp  # type: ignore

            self._aiohttp = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout_s))
            return self

        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        with contextlib.suppress(Exception):
            if self._httpx is not None:
                await self._httpx.aclose()
        with contextlib.suppress(Exception):
            if self._aiohttp is not None:
                await self._aiohttp.close()

    async def get_json(self, url: str, params: Dict[str, Any], headers: Dict[str, str]) -> Tuple[int, Any, str]:
        # httpx path
        if self._httpx is not None:
            r = await self._httpx.get(url, params=params, headers=headers)
            st = int(getattr(r, "status_code", 0) or 0)
            try:
                return st, r.json(), ""
            except Exception:
                txt = (getattr(r, "text", "") or "").strip()
                return st, None, (txt[:400] + "…") if len(txt) > 400 else txt

        # aiohttp path
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

        # Last resort: urllib in thread
        return await _http_get_json_urllib(url, params, self.timeout_s, headers)

    async def post_json(self, url: str, body: Dict[str, Any], headers: Dict[str, str]) -> Tuple[int, Any, str]:
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
    url: str, params: Dict[str, Any], timeout_s: float, headers: Dict[str, str]
) -> Tuple[int, Any, str]:
    import urllib.request

    def _sync() -> Tuple[int, Any, str]:
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
    url: str, body: Dict[str, Any], timeout_s: float, headers: Dict[str, str]
) -> Tuple[int, Any, str]:
    import urllib.request

    def _sync() -> Tuple[int, Any, str]:
        data = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(
            url=url,
            data=data,
            headers={**{"content-type": "application/json", "accept": "application/json"}, **(headers or {})},
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
    warnings: List[str]
    error: str
    quote_brief: Dict[str, Any]
    swap_brief: Dict[str, Any]
    score: float

    def public(self) -> Dict[str, Any]:
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
    if a.error:
        s -= 4.0

    s -= min(4.0, float(a.elapsed_ms) / 650.0)
    s -= min(2.0, 0.35 * len(a.warnings or []))
    return s


# -----------------------------
# Resolution helpers
# -----------------------------
def _resolve_slippage_bps(cli_override: int) -> Tuple[int, str]:
    if cli_override >= 0:
        return int(cli_override), "CLI"
    for k in ("PF_SLIPPAGE_BPS", "SLIPPAGE_BPS", "JUP_SLIPPAGE_BPS"):
        v = _envs(k, "")
        if v:
            with contextlib.suppress(Exception):
                return int(float(v)), k
    return 50, "default(50)"


def _resolve_api_key() -> Tuple[str, str]:
    for k in ("JUP_PRO_KEY", "JUP_API_KEY", "JUP_KEY"):
        v = _envs(k, "")
        if v:
            return v, k
    return "", ""


def _skip_by_default() -> bool:
    if _envb("CHECK_JUP_DISABLED", False):
        return True
    if _envb("KILL_SWITCH", False) or _envb("DISABLE_BUYS", False) or _envb("DRYRUN", False):
        return True
    return False


def _join(base: str, path: str) -> str:
    b = (base or "").rstrip("/")
    p = (path or "").strip()
    if not p.startswith("/"):
        p = "/" + p if p else ""
    return b + p


def _candidate_bases(cli_bases: Sequence[str]) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    for i, b in enumerate(cli_bases):
        if b:
            out.append((f"CLI_{i+1}", b.strip()))

    for k in ("JUPITER_SWAP_API", "JUP_BASE_URL"):
        v = _envs(k, "")
        if v:
            out.append((k, v.strip()))

    for u in _split_csv(_envs("JUP_ALT_BASE_URLS", "")):
        out.append(("JUP_ALT_BASE_URLS", u.strip()))

    if _envb("JUP_ALLOW_PUBLIC_FALLBACK", False):
        out.append(("PUBLIC_FALLBACK", "https://api.jup.ag"))
        out.append(("PUBLIC_FALLBACK", "https://public.jupiterapi.com"))

    # de-dupe
    seen = set()
    uniq: List[Tuple[str, str]] = []
    for lbl, u in out:
        uu = u.strip()
        if not uu or uu in seen:
            continue
        seen.add(uu)
        uniq.append((lbl, uu))
    return uniq


def _path_variants_for_base(base: str, quote_path: str, swap_path: str) -> List[Tuple[str, str]]:
    """
    Jupiter has two common shapes:
      - New:  https://api.jup.ag + /swap/v1/quote , /swap/v1/swap
      - Legacy-ish public base: https://public.jupiterapi.com + /quote , /swap

    This function tries to be forgiving if your env mixes base+path.
    """
    b_host = _host_from_url(base)
    q = (quote_path or "").strip() or "/swap/v1/quote"
    s = (swap_path or "").strip() or "/swap/v1/swap"

    variants: List[Tuple[str, str]] = [(q, s)]

    # If using public.jupiterapi.com and paths are v1, add legacy candidates
    if b_host == "public.jupiterapi.com":
        if q.startswith("/swap/v1/"):
            variants.append(("/quote", "/swap"))
        if q == "/quote":
            variants.append(("/swap/v1/quote", "/swap/v1/swap"))

    # If using api.jup.ag and paths are legacy, add v1 candidates
    if b_host == "api.jup.ag":
        if q == "/quote":
            variants.append(("/swap/v1/quote", "/swap/v1/swap"))
        if q.startswith("/swap/v1/"):
            variants.append(("/quote", "/swap"))

    # De-dupe
    seen = set()
    out: List[Tuple[str, str]] = []
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

    # Optional import (do not hard-require)
    with contextlib.suppress(Exception):
        from utils_pkg.solana_wallet import load_keypair, get_pubkey_str  # type: ignore

        kp = load_keypair()
        return get_pubkey_str(kp)

    return ""


def _fix_hints(attempts: List[Attempt], *, strict: bool, require_api_key: bool) -> List[str]:
    hints: List[str] = []
    if not attempts:
        return hints

    any_429 = any(a.http_status_quote == 429 or a.http_status_swap == 429 for a in attempts)
    any_bad_url = any("bad_url_format" in (a.error or "") for a in attempts)
    any_missing_key_warn = any("missing_api_key_public" in (a.warnings or []) for a in attempts)
    any_placeholder_key_warn = any("placeholder_api_key" in (a.warnings or []) for a in attempts)
    any_quote_fail = all(not a.ok_quote for a in attempts)

    if any_bad_url:
        hints.append("At least one base URL is missing http(s):// — fix JUP_BASE_URL/JUPITER_SWAP_API/JUP_ALT_BASE_URLS.")
    if any_placeholder_key_warn:
        hints.append("Your API key looks like a literal placeholder (e.g. '$JUP_API_KEY'). Set JUP_PRO_KEY to the actual key value.")
    if any_429:
        hints.append("Rate limiting detected (429). Use JUP_PRO_KEY and/or add multiple bases via JUP_ALT_BASE_URLS.")
    if strict and require_api_key and any_missing_key_warn:
        hints.append("Strict mode requires an API key for public endpoints. Set JUP_PRO_KEY (preferred) or JUP_API_KEY.")
    if any_quote_fail:
        hints.append("No viable quote returned. Check mints, amount, slippage, and your base/path combination (v1 vs legacy).")

    out: List[str] = []
    seen = set()
    for h in hints:
        if h not in seen:
            seen.add(h)
            out.append(h)
    return out[:10]


# -----------------------------
# Optional lightning_impl introspection (mirrors bot wiring)
# -----------------------------
def _lightning_diag() -> Dict[str, Any]:
    info: Dict[str, Any] = {"available": 0}
    with contextlib.suppress(Exception):
        from utils_pkg import lightning_impl as L  # type: ignore

        info["available"] = 1
        info["BASE"] = _redact_url(str(getattr(L, "BASE", "") or ""))
        info["AS_LEGACY"] = int(bool(getattr(L, "AS_LEGACY", False)))
        with contextlib.suppress(Exception):
            h = getattr(L, "_jup_headers")()
            if isinstance(h, dict):
                info["headers"] = _sanitize_headers({str(k): str(v) for k, v in h.items()})
                info["has_x_api_key"] = int("x-api-key" in {str(k).lower() for k in h.keys()})
        with contextlib.suppress(Exception):
            vf = getattr(L, "_variant_flags")()
            info["variant_flags"] = vf if isinstance(vf, dict) else str(vf)
    return info


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
    swap_test: bool,
    require_swap_ok: bool,
    headers: Dict[str, str],
    quote_params: Dict[str, Any],
    swap_user_pubkey: str,
) -> Attempt:
    t0 = time.monotonic()

    base_red = _redact_url(base_url)
    quote_url = _join(base_url, quote_path)
    swap_url = _join(base_url, swap_path)

    warnings: List[str] = []
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

    host = _host_from_url(base_url)
    is_publicish = host in PUBLIC_HOSTS or host.endswith("jup.ag")
    has_key = bool(headers.get("x-api-key"))

    if require_api_key and is_publicish and not has_key:
        warnings.append("missing_api_key_public")

    # Quote request params: make booleans explicit strings for broad compatibility
    qp = dict(quote_params)
    # Jupiter expects onlyDirectRoutes as boolean-ish; send "true/false" to be safe
    if "onlyDirectRoutes" in qp:
        qp["onlyDirectRoutes"] = "true" if bool(qp["onlyDirectRoutes"]) else "false"
    qp.setdefault("swapMode", "ExactIn")

    quote_brief: Dict[str, Any] = {"ok": 0}
    swap_brief: Dict[str, Any] = {"enabled": int(bool(swap_test)), "ok": 0}

    # If key looks like placeholder, treat as missing for strict gate logic (but still send if user set it)
    if has_key and _looks_like_placeholder_key(headers.get("x-api-key", "")):
        warnings.append("placeholder_api_key")

    try:
        async with _Http(timeout_s=timeout_s) as H:
            # ---- Quote ----
            st_q, j, e = await H.get_json(quote_url, qp, headers)
            st_q = int(st_q or 0)

            if st_q == 429:
                warnings.append("http_429_rate_limited_quote")

            if st_q == 0:
                err = err or (e or "connect_failed")
            else:
                jd = _coerce_dict(j)
                if jd is None:
                    err = err or (e or f"http_{st_q}_bad_json")
                else:
                    j_err = _extract_error(jd, "")
                    if j_err:
                        err = err or j_err
                    else:
                        out_amt = jd.get("outAmount")
                        rp = jd.get("routePlan")
                        routes = len(rp) if isinstance(rp, list) else (0 if rp is not None else 1)
                        ok_quote = bool(out_amt) and routes >= 1
                        quote_brief = {
                            "ok": int(ok_quote),
                            "outAmount": str(out_amt)[:80] if out_amt is not None else None,
                            "routes": int(routes),
                            "contextSlot": jd.get("contextSlot"),
                            "timeTaken": jd.get("timeTaken"),
                        }
                        if not ok_quote:
                            err = err or "quote_missing_outAmount_or_routes"

            # ---- Optional swap build ----
            if swap_test and ok_quote:
                if not swap_user_pubkey:
                    swap_brief = {"enabled": 1, "ok": 0, "err": "missing_user_pubkey_for_swap_test"}
                    if strict and require_swap_ok:
                        err = err or "strict_fail: missing_user_pubkey_for_swap_test"
                else:
                    body: Dict[str, Any] = {
                        "quoteResponse": _coerce_dict(j) or j,
                        "userPublicKey": swap_user_pubkey,
                        # safe hints
                        "asLegacyTransaction": bool(_envb("JUP_AS_LEGACY", False)),
                        "useSharedAccounts": bool(_envb("JUP_USE_SHARED_ACCOUNTS", False)),
                        "wrapAndUnwrapSol": bool(_envb("PF_WRAP_UNWRAP_SOL", True)),
                    }
                    st_s, j2, e2 = await H.post_json(swap_url, body, headers)
                    st_s = int(st_s or 0)

                    if st_s == 429:
                        warnings.append("http_429_rate_limited_swap")

                    if st_s == 0:
                        if strict and require_swap_ok:
                            err = err or (e2 or "swap_connect_failed")
                        swap_brief = {"enabled": 1, "ok": 0, "http_status": 0, "err": e2 or "swap_connect_failed"}
                    else:
                        jd2 = _coerce_dict(j2)
                        if jd2 is None:
                            if strict and require_swap_ok:
                                err = err or (e2 or f"swap_http_{st_s}_bad_json")
                            swap_brief = {"enabled": 1, "ok": 0, "http_status": st_s, "err": e2 or "swap_bad_json"}
                        else:
                            j2_err = _extract_error(jd2, "")
                            if j2_err:
                                if strict and require_swap_ok:
                                    err = err or j2_err
                                swap_brief = {"enabled": 1, "ok": 0, "http_status": st_s, "err": j2_err[:240]}
                            else:
                                tx = jd2.get("swapTransaction") or jd2.get("swapTransactionB64") or None
                                ok_swap_build = bool(tx)
                                swap_brief = {
                                    "enabled": 1,
                                    "ok": int(ok_swap_build),
                                    "http_status": st_s,
                                    "has_tx": int(bool(tx)),
                                    "tx_len": int(len(str(tx))) if tx else 0,
                                    "lastValidBlockHeight": jd2.get("lastValidBlockHeight"),
                                    "prioritizationFeeLamports": jd2.get("prioritizationFeeLamports"),
                                }
                                if strict and require_swap_ok and not ok_swap_build:
                                    err = err or "strict_fail: swap_missing_transaction"

    except asyncio.CancelledError:
        raise
    except Exception as ex:
        err = err or f"{type(ex).__name__}: {ex}"

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
    ap.add_argument("--quote-path", default=_envs("JUP_QUOTE_PATH", "/swap/v1/quote"), help="Quote path.")
    ap.add_argument("--swap-path", default=_envs("JUP_SWAP_PATH", "/swap/v1/swap"), help="Swap path.")
    ap.add_argument("--timeout", type=float, default=float(_envf("CHECK_JUP_TIMEOUT_S", 12.0)), help="Per-call timeout seconds.")
    ap.add_argument("--strict", action="store_true", help="Strict gates (API key requirement on public endpoints).")
    ap.add_argument("--try-all", action="store_true", help="Try all candidates and pick best by score.")
    ap.add_argument("--swap-test", action="store_true", help="Also try swap-build (unsigned tx) if quote succeeds.")
    ap.add_argument("--require-swap-ok", action="store_true", help="In strict+swap-test, require swap build OK.")

    ap.add_argument("--json", action="store_true", help="Emit JSON summary to STDOUT.")
    ap.add_argument("--json-only", action="store_true", help="Only JSON (no human logs).")

    ap.add_argument("--input-mint", default=_envs("CHECK_JUP_INPUT_MINT", KNOWN_SOL_MINT), help="Input mint.")
    ap.add_argument("--output-mint", default=_envs("CHECK_JUP_OUTPUT_MINT", KNOWN_USDC_MINT), help="Output mint.")
    ap.add_argument("--amount-sol", type=float, default=float(_envf("CHECK_JUP_AMOUNT_SOL", 0.005)), help="Input amount in SOL (if input is SOL).")
    ap.add_argument("--amount-raw", type=int, default=int(_envi("CHECK_JUP_AMOUNT_RAW", 0)), help="Override amount in base units (lamports for SOL).")
    ap.add_argument("--slippage-bps", type=int, default=-1, help="Override slippage bps.")
    args = ap.parse_args()

    strict = bool(args.strict or _envb("CHECK_JUP_STRICT", False))
    try_all = bool(args.try_all or _envb("CHECK_JUP_TRY_ALL", False))

    emit_json = bool(args.json or args.json_only or _envb("CHECK_JUP_JSON", False) or _envb("CHECK_JUP_JSON_ONLY", False))
    json_only = bool(args.json_only or _envb("CHECK_JUP_JSON_ONLY", False))

    # Skip gates unless explicit intent
    explicit_intent = bool(args.base or args.strict or args.try_all or args.swap_test or emit_json or args.amount_raw > 0)
    if _envb("CHECK_JUP_DISABLED", False):
        out = {"ok": True, "skipped": True, "reason": "CHECK_JUP_DISABLED=1", "ts": _ts()}
        if emit_json:
            print(json.dumps(out, ensure_ascii=False))
        _hlog("SKIP (CHECK_JUP_DISABLED=1)", json_only=json_only)
        return RC_OK

    if _skip_by_default() and not explicit_intent:
        out = {"ok": True, "skipped": True, "reason": "buys disabled (DRYRUN/KILL_SWITCH/DISABLE_BUYS)", "ts": _ts()}
        if emit_json:
            print(json.dumps(out, ensure_ascii=False))
        _hlog("SKIP (buys disabled; no explicit intent)", json_only=json_only)
        return RC_OK

    timeout_s = max(2.0, float(args.timeout))

    # Resolve API key header
    api_key, api_key_src = _resolve_api_key()
    headers: Dict[str, str] = {"user-agent": "pnlsniper-check-jupiter"}
    if api_key:
        headers["x-api-key"] = api_key

    # Strict API key requirement (public endpoints) unless overridden off
    require_api_key = bool(_envb("CHECK_JUP_REQUIRE_API_KEY", False) or strict)

    # Swap test knobs
    swap_test = bool(args.swap_test or _envb("CHECK_JUP_SWAP_TEST", False))
    require_swap_ok = bool(args.require_swap_ok or _envb("CHECK_JUP_REQUIRE_SWAP_OK", False))
    swap_user_pubkey = _load_user_pubkey_best_effort()

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

    quote_params: Dict[str, Any] = {
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

        im, om, amt = guard_jupiter_quote_args(quote_params["inputMint"], quote_params["outputMint"], amount_raw=int(amount), require_anchor=False)  # type: ignore
        quote_params["inputMint"], quote_params["outputMint"], quote_params["amount"] = str(im), str(om), int(amt)
        guard_note = "mint_guard_applied"

    # Bases
    bases = _candidate_bases([b for b in args.base if b])
    if not bases:
        msg = "No Jupiter base URL found. Set JUP_BASE_URL (or JUPITER_SWAP_API) or pass --base."
        out = {"ok": False, "error": "missing_jupiter_base", "detail": msg, "ts": _ts()}
        if emit_json:
            print(json.dumps(out, ensure_ascii=False))
        _hlog("FAIL " + msg, json_only=json_only)
        return RC_CONNECT_FAIL

    # Strict placeholder key warning
    placeholder_key = int(bool(api_key and _looks_like_placeholder_key(api_key)))

    # Lightning diag (mirrors bot wiring, safe)
    ldiag = _lightning_diag()

    # Boot log
    _hlog(
        "BOOT "
        f"strict={int(strict)} try_all={int(try_all)} timeout_s={timeout_s:.1f} "
        f"has_api_key={int(bool(api_key))} api_key_src={api_key_src or '-'} placeholder_key={placeholder_key} "
        f"require_api_key={int(require_api_key)} swap_test={int(swap_test)} require_swap_ok={int(require_swap_ok)} "
        f"slippage_bps={slippage_bps}({slippage_src}) only_direct={int(only_direct)} "
        f"amount={int(quote_params['amount'])} amount_src={amount_src} {guard_note} "
        f"paths={{quote:{args.quote_path}, swap:{args.swap_path}}} "
        f"headers={_sanitize_headers(headers)} bases={[lbl for lbl, _ in bases]} "
        f"lightning={ldiag}",
        json_only=json_only,
    )

    # Pre-error if amount missing
    pre_warnings: List[str] = []
    if int(quote_params["amount"]) <= 0:
        pre_warnings.append("missing_amount: for non-SOL inputMint you must set CHECK_JUP_AMOUNT_RAW or --amount-raw")

    # Probe all candidates (base + path variants)
    attempts: List[Attempt] = []
    any_timeout = False
    any_bad_url = False

    # Watchdog: per attempt
    attempt_watchdog = max(6.0, timeout_s * 4.5)

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
                        swap_test=swap_test,
                        require_swap_ok=require_swap_ok,
                        headers=headers,
                        quote_params=quote_params,
                        swap_user_pubkey=swap_user_pubkey,
                    ),
                    timeout=attempt_watchdog,
                )
            except asyncio.TimeoutError:
                any_timeout = True
                a = Attempt(
                    label=lbl,
                    base_url=base,
                    base_url_redacted=_redact_url(base),
                    quote_path=qpath,
                    swap_path=spath,
                    quote_url_redacted=_redact_url(_join(base, qpath)),
                    swap_url_redacted=_redact_url(_join(base, spath)),
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

            # stop early unless try_all
            if not try_all:
                if strict:
                    if a.ok_quote and (not swap_test or a.ok_swap_build or not require_swap_ok):
                        break
                else:
                    if a.ok_quote:
                        break
        if not try_all and attempts and attempts[-1].ok_quote:
            break

    # Pick best attempt (prefer ok_quote)
    ok_quotes = [a for a in attempts if a.ok_quote]
    picked = max(ok_quotes, key=lambda x: x.score) if ok_quotes else None

    ok = bool(picked is not None)
    strict_fail = False

    if ok and strict and require_api_key:
        host = _host_from_url(picked.base_url)
        is_publicish = host in PUBLIC_HOSTS or host.endswith("jup.ag")
        # treat placeholder as missing for strict gate
        key_ok = bool(headers.get("x-api-key")) and not _looks_like_placeholder_key(headers.get("x-api-key", ""))
        if is_publicish and not key_ok:
            strict_fail = True

    if ok and strict and swap_test and require_swap_ok and not picked.ok_swap_build:
        strict_fail = True

    final_ok = bool(ok and not strict_fail)

    # Build summary JSON
    summary: Dict[str, Any] = {
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
        "fix_hints": _fix_hints(attempts, strict=strict, require_api_key=require_api_key),
    }

    if emit_json or json_only:
        print(json.dumps(summary, ensure_ascii=False, default=str))

    # Human summary + exit code decisions
    if final_ok:
        _hlog("SUMMARY ok=1 (jupiter viable)", json_only=json_only)
        return RC_OK

    if any_bad_url:
        _hlog("SUMMARY ok=0 (bad url format present)", json_only=json_only)
        return RC_BAD_URL

    if attempts and all((a.error == "attempt_timeout") for a in attempts):
        _hlog("SUMMARY ok=0 (all attempts timed out)", json_only=json_only)
        return RC_TIMEOUT

    if strict_fail or any((a.http_status_quote > 0 or a.http_status_swap > 0) for a in attempts):
        _hlog("SUMMARY ok=0 (requirements/strict gate failed)", json_only=json_only)
        return RC_REQUIREMENTS_FAIL

    _hlog("SUMMARY ok=0 (connect/respond failure)", json_only=json_only)
    return RC_CONNECT_FAIL


def main() -> None:
    try:
        rc = asyncio.run(_amain())
    except KeyboardInterrupt:
        rc = 130
    except Exception as e:
        _safe_print(f"[check_jupiter] {_ts()} FAIL unhandled {type(e).__name__}: {e}", file=sys.stderr)
        rc = 1
    raise SystemExit(rc)


if __name__ == "__main__":
    with contextlib.suppress(Exception):
        if os.name == "nt" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]
    main()
