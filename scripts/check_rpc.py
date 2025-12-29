# -*- coding: utf-8 -*-
from __future__ import annotations

"""
scripts/check_rpc.py — God-Tier Solana RPC Precheck (Phase 0–7 LOCK-IN)
======================================================================

Goal
----
Deterministic, secret-safe sanity check for Solana JSON-RPC endpoint(s).

Read-only checks
----------------
- URL format sanity + redaction safety (never prints creds/tokens)
- DNS resolve (best-effort; optional strict promotion via --strict)
- JSON-RPC responsiveness
- getLatestBlockhash  (REQUIRED: minimum viability)
- getSlot             (REQUIRED: minimum viability)
- getHealth           (OPTIONAL unless strict/require_health)
- getVersion          (best-effort)
- Optional progress probe: getSlot twice (require >= or > via CHECK_RPC_REQUIRE_ADVANCE)

Must align with utils_pkg/rpc_failover.py intent
-----------------------------------------------
- Produces a stable “best RPC” selection + recommended ordering for SOL_RPC_URLS.
- Avoids thrashing: conservative scoring; never mutates banlists; only reports.
- Reads common failover knobs if present (RPC_FAILOVER_*), and reports their presence (masked).

Usage
-----
py -m scripts.check_rpc
py -m scripts.check_rpc --url https://...
py -m scripts.check_rpc --strict --progress
py -m scripts.check_rpc --try-all --json-only
py -m scripts.check_rpc --force

Env
---
SOL_RPC_URL / RPC_URL / SOLANA_RPC_URL          primary
SOL_RPC_URLS / RPC_URLS                         csv fallbacks

CHECK_RPC_TIMEOUT_S=12
CHECK_RPC_RETRIES=1                              (per-method retry count; total attempts per method = 1+retries)
CHECK_RPC_STRICT=0|1
CHECK_RPC_JSON=0|1                               (default 1)
CHECK_RPC_JSON_ONLY=0|1                          (default 0)
CHECK_RPC_TRY_ALL=0|1
CHECK_RPC_REQUIRE_HEALTH=0|1                     (default: strict implies require)
CHECK_RPC_PROGRESS=0|1
CHECK_RPC_PROGRESS_DELAY_S=0.4
CHECK_RPC_REQUIRE_ADVANCE=0|1                    (default 0)
CHECK_RPC_DISABLED=0|1                           (skip)
DISABLE_SCANNERS=0|1                             (skip unless explicit intent or --force)
KILL_SWITCH=0|1                                  (skip unless explicit intent or --force)

PREFLIGHT_MASK_LONG_TOKENS=0|1                   (default 1)
CHECK_RPC_MASK_LONG_TOKENS=0|1                   (override)

Optional headers (some providers require them)
----------------------------------------------
RPC_HEADERS_JSON='{"Authorization":"Bearer ...","x-api-key":"..."}'
RPC_HEADERS_KV='Authorization=Bearer ...;x-api-key=...'
SOL_RPC_HEADERS_JSON / CHECK_RPC_HEADERS_JSON    (alternatives)

Exit codes
----------
0   OK / skipped
1   Missing URL or all candidates failed to connect/respond
2   RPC responded but requirements failed (health/blockhash/slot/progress)
3   Bad URL format (all candidates invalid scheme)
124 All attempts timed out (attempt-level watchdog tripped)
"""

import argparse
import asyncio
import contextlib
import json
import os
import random
import re
import socket
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}

RC_OK = 0
RC_CONNECT_FAIL = 1
RC_REQUIREMENTS_FAIL = 2
RC_BAD_URL = 3
RC_TIMEOUT = 124

# URL + header redaction
_URL_CRED_RE = re.compile(r"(://)([^/@\s]+)@")
_QS_SECRET_KEY_RE = re.compile(r"(?i)^(api[-_]?key|apikey|token|key|secret|sig|signature|auth|authorization|access[-_]?token)$")
_LONG_TOKENISH_RE = re.compile(r"\b[A-Za-z0-9_\-]{28,}\b")  # conservative
_HDR_SENSITIVE_RE = re.compile(r"(?i)^(authorization|x-api-key|api-key|token|secret|signature)$")


# =============================================================================
# env + io
# =============================================================================
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


def _envi(k: str, d: int) -> int:
    s = _envs(k, str(d)).replace("_", "")
    try:
        return int(float(s))
    except Exception:
        return int(d)


def _envf(k: str, d: float) -> float:
    try:
        return float(_envs(k, str(d)))
    except Exception:
        return float(d)


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
            file.buffer.write(b + b"\n")  # type: ignore[attr-defined]
            file.buffer.flush()  # type: ignore[attr-defined]
    except Exception:
        pass


def _hlog(msg: str, *, json_only: bool) -> None:
    if json_only:
        return
    _safe_print(f"[check_rpc] {_ts()} {msg}", file=sys.stderr)


def _split_csv(s: str) -> List[str]:
    return [p.strip() for p in (s or "").replace(";", ",").replace("\n", ",").split(",") if p.strip()]


def _mask_long_tokens_enabled() -> bool:
    if "CHECK_RPC_MASK_LONG_TOKENS" in os.environ:
        return _envb("CHECK_RPC_MASK_LONG_TOKENS", True)
    return _envb("PREFLIGHT_MASK_LONG_TOKENS", True)


# =============================================================================
# redaction / safety
# =============================================================================
def _has_scheme(url: str) -> bool:
    u = (url or "").strip()
    return u.startswith("http://") or u.startswith("https://")


def _redact_url(url: str) -> str:
    """Redact URL creds + secret-ish query params; shorten long paths."""
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
        if len(path) > 48:
            parts = [x for x in path.split("/") if x]
            path = "/" + (parts[0] if parts else "…") + "/…"

        out = urlunparse(p._replace(path=path, query=query))
    except Exception:
        out = u.split("?", 1)[0] + "?..." if "?" in u else u

    if _mask_long_tokens_enabled():
        out = _LONG_TOKENISH_RE.sub("***", out)
    return out


def _host_from_url(url: str) -> str:
    try:
        p = urlparse(url)
        return (p.hostname or "").strip()
    except Exception:
        return ""


def _resolves(host: str) -> bool:
    try:
        if not host:
            return False
        socket.getaddrinfo(host, None)
        return True
    except OSError:
        return False


def _sanitize_headers(h: Dict[str, str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k, v in (h or {}).items():
        kk = str(k or "")
        vv = str(v or "")
        if _HDR_SENSITIVE_RE.match(kk.strip().lower()):
            out[kk] = "***"
        else:
            out[kk] = "***" if (_mask_long_tokens_enabled() and len(vv) >= 24) else vv
    return out


# =============================================================================
# header parsing (optional)
# =============================================================================
def _parse_headers(cli_headers_json: str = "", cli_headers_kv: str = "") -> Dict[str, str]:
    """
    Optional headers for providers that require them.
    Priority:
      0) CLI --headers-json / --headers-kv
      1) RPC_HEADERS_JSON / SOL_RPC_HEADERS_JSON / CHECK_RPC_HEADERS_JSON
      2) RPC_HEADERS_KV
    """
    if cli_headers_json:
        with contextlib.suppress(Exception):
            obj = json.loads(cli_headers_json)
            if isinstance(obj, dict):
                return {str(a): str(b) for a, b in obj.items()}

    if cli_headers_kv:
        out: Dict[str, str] = {}
        parts = [p.strip() for p in cli_headers_kv.replace(",", ";").split(";") if p.strip()]
        for p in parts:
            if "=" in p:
                a, b = p.split("=", 1)
                out[a.strip()] = b.strip()
        if out:
            return out

    for k in ("RPC_HEADERS_JSON", "SOL_RPC_HEADERS_JSON", "CHECK_RPC_HEADERS_JSON"):
        raw = _envs(k, "")
        if raw:
            with contextlib.suppress(Exception):
                obj = json.loads(raw)
                if isinstance(obj, dict):
                    return {str(a): str(b) for a, b in obj.items()}

    kv = _envs("RPC_HEADERS_KV", "")
    if kv:
        out2: Dict[str, str] = {}
        parts = [p.strip() for p in kv.replace(",", ";").split(";") if p.strip()]
        for p in parts:
            if "=" in p:
                a, b = p.split("=", 1)
                out2[a.strip()] = b.strip()
        return out2

    return {}


# =============================================================================
# candidate collection
# =============================================================================
def _candidate_urls(cli_urls: Sequence[str]) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    for i, u in enumerate(cli_urls):
        if u:
            out.append((f"CLI_{i+1}", u.strip()))

    for k in ("SOL_RPC_URL", "RPC_URL", "SOLANA_RPC_URL"):
        v = _envs(k, "")
        if v:
            out.append((k, v.strip()))

    for k in ("SOL_RPC_URLS", "RPC_URLS"):
        for u in _split_csv(_envs(k, "")):
            out.append((k, u.strip()))

    seen = set()
    uniq: List[Tuple[str, str]] = []
    for lbl, u in out:
        uu = u.strip()
        if not uu or uu in seen:
            continue
        seen.add(uu)
        uniq.append((lbl, uu))
    return uniq


def _read_failover_knobs() -> Dict[str, Any]:
    """
    Best-effort alignment with utils_pkg/rpc_failover.py:
    we don't require these vars, but we report presence and values (masked).
    """
    keys = [
        "RPC_FAILOVER_COOLDOWN_S",
        "RPC_FAILOVER_STICKY_S",
        "RPC_FAILOVER_BANLIST",
        "RPC_FAILOVER_MAX_FAILS",
        "RPC_FAILOVER_MAX_BAN",
        "RPC_FAILOVER_SAMPLE_MS",
        "RPC_FAILOVER_MIN_OK_RATIO",
        "RPC_FAILOVER_JITTER_S",
        "RPC_FAILOVER_DEBUG",
    ]
    out: Dict[str, Any] = {}
    for k in keys:
        if k in os.environ:
            v = os.getenv(k, "") or ""
            out[k] = "***" if (_mask_long_tokens_enabled() and len(v) >= 24) else v
    return out


# =============================================================================
# rpc transport
# =============================================================================
async def _post_jsonrpc_httpx(client: Any, url: str, payload: Dict[str, Any], headers: Dict[str, str]) -> Tuple[int, Any, str]:
    r = await client.post(url, json=payload, headers=headers)
    status = int(getattr(r, "status_code", 0) or 0)
    try:
        j = r.json()
        return status, j, ""
    except Exception:
        txt = (getattr(r, "text", "") or "").strip()
        return status, None, (txt[:260] + "…") if len(txt) > 260 else txt


async def _post_jsonrpc_urllib(url: str, payload: Dict[str, Any], timeout_s: float, headers: Dict[str, str]) -> Tuple[int, Any, str]:
    import urllib.request

    def _sync() -> Tuple[int, Any, str]:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url=url,
            data=data,
            headers={**{"content-type": "application/json"}, **(headers or {})},
            method="POST",
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
            return status, None, (b[:260] + "…") if len(b) > 260 else b

    return await asyncio.to_thread(_sync)


async def _rpc_call_once(
    *,
    client: Any,
    use_httpx: bool,
    url: str,
    method: str,
    params: Optional[Any],
    timeout_s: float,
    headers: Dict[str, str],
    rid: int,
) -> Tuple[bool, Any, str, int, int]:
    """
    Returns (ok, result, err, http_status, elapsed_ms)
    ok means JSON-RPC result present (no error field).
    """
    payload: Dict[str, Any] = {"jsonrpc": "2.0", "id": rid, "method": method}
    if params is not None:
        payload["params"] = params

    t0 = time.monotonic()
    try:
        if use_httpx:
            status, j, err = await _post_jsonrpc_httpx(client, url, payload, headers)
        else:
            status, j, err = await _post_jsonrpc_urllib(url, payload, timeout_s, headers)
    except Exception as e:
        return False, None, f"{type(e).__name__}: {e}", 0, int((time.monotonic() - t0) * 1000)

    elapsed_ms = int((time.monotonic() - t0) * 1000)

    if status == 0 and err:
        return False, None, err, 0, elapsed_ms

    if not isinstance(j, dict):
        if status == 429:
            return False, None, "http_429_rate_limited", status, elapsed_ms
        if status >= 400:
            return False, None, f"http_{status}:{err or 'bad_response'}", status, elapsed_ms
        return False, None, err or "bad_response", status, elapsed_ms

    if j.get("error"):
        e = j["error"]
        msg = ""
        try:
            if isinstance(e, dict):
                msg = str(e.get("message") or e.get("data") or e)
            else:
                msg = str(e)
        except Exception:
            msg = "rpc_error"
        msg = msg.strip()
        low = msg.lower()
        if "too many requests" in low or "rate" in low:
            return False, None, "rpc_rate_limited", status, elapsed_ms
        if "method not found" in low:
            return False, None, "rpc_method_not_found", status, elapsed_ms
        return False, None, (msg[:300] + "…") if len(msg) > 300 else msg, status, elapsed_ms

    return True, j.get("result"), "", status, elapsed_ms


async def _rpc_call(
    *,
    client: Any,
    use_httpx: bool,
    url: str,
    method: str,
    params: Optional[Any],
    timeout_s: float,
    headers: Dict[str, str],
    rid: int,
    retries: int,
    base_backoff_s: float = 0.25,
) -> Tuple[bool, Any, str, int, Dict[str, Any]]:
    """
    Retry wrapper. Retries only on timeouts/rate-limit/transport-ish errors.
    Returns (ok, result, err, http_status, meta)
    """
    meta = {"attempts": 0, "lat_ms": [], "last_http": 0}
    last_err = ""
    last_status = 0
    for i in range(max(1, 1 + int(retries))):
        meta["attempts"] += 1
        ok, res, err, st, ms = await _rpc_call_once(
            client=client,
            use_httpx=use_httpx,
            url=url,
            method=method,
            params=params,
            timeout_s=timeout_s,
            headers=headers,
            rid=rid + i,
        )
        meta["lat_ms"].append(int(ms))
        meta["last_http"] = int(st or 0)
        if ok:
            return True, res, "", st, meta

        last_err, last_status = err, st

        retryable = (
            err == "http_429_rate_limited"
            or err == "rpc_rate_limited"
            or "timeout" in (err or "").lower()
            or "connect" in (err or "").lower()
            or "temporar" in (err or "").lower()
        )
        if i < retries and retryable:
            # jittered backoff
            await asyncio.sleep(base_backoff_s * (2 ** i) * (0.75 + 0.5 * random.random()))
            continue
        break

    return False, None, last_err or "rpc_call_failed", last_status, meta


# =============================================================================
# result model + scoring
# =============================================================================
@dataclass
class Attempt:
    label: str
    url: str
    url_redacted: str
    host: str
    host_resolves: int

    ok_min: bool
    ok_strict: bool

    elapsed_ms: int
    lat: Dict[str, Any]

    warnings: List[str]
    http_notes: List[str]
    error: str

    health: Dict[str, Any]
    version: Dict[str, Any]
    slot: Dict[str, Any]
    slot_progress: Dict[str, Any]
    latest_blockhash: Dict[str, Any]

    score: float

    def public(self) -> Dict[str, Any]:
        return {
            "label": self.label,
            "url": self.url_redacted,
            "host": self.host,
            "host_resolves": self.host_resolves,
            "ok_min": int(self.ok_min),
            "ok_strict": int(self.ok_strict),
            "elapsed_ms": self.elapsed_ms,
            "score": round(float(self.score), 6),
            "warnings": list(self.warnings or []),
            "http_notes": list(self.http_notes or []),
            "error": self.error,
            "lat": self.lat,
            "health": self.health,
            "version": self.version,
            "slot": self.slot,
            "slot_progress": self.slot_progress,
            "latest_blockhash": self.latest_blockhash,
        }


def _to_int(x: Any, default: int = 0) -> int:
    try:
        if x is None or isinstance(x, bool):
            return int(default)
        if isinstance(x, int):
            return int(x)
        if isinstance(x, float):
            return int(x)
        s = str(x).strip()
        return int(float(s)) if s else int(default)
    except Exception:
        return int(default)


def _percentile(vals: List[int], p: float) -> Optional[int]:
    if not vals:
        return None
    xs = sorted(int(x) for x in vals)
    if len(xs) == 1:
        return xs[0]
    k = max(0, min(len(xs) - 1, int(round((p / 100.0) * (len(xs) - 1)))))
    return xs[k]


def _score_attempt(a: Attempt, *, strict: bool, progress: bool) -> float:
    s = 0.0
    if a.ok_min:
        s += 12.0
    if strict and a.ok_strict:
        s += 10.0

    if a.health.get("ok") == 1 and str(a.health.get("val") or "").lower() == "ok":
        s += 2.0

    if a.latest_blockhash.get("ok") == 1 and a.latest_blockhash.get("blockhash"):
        s += 6.0

    if a.slot.get("ok") == 1 and _to_int(a.slot.get("val"), 0) > 0:
        s += 1.0

    if progress and a.slot_progress.get("enabled") == 1 and a.slot_progress.get("ok") == 1:
        s += 1.0

    # penalties
    s -= min(2.5, 0.30 * len(a.warnings or []))
    if a.error:
        s -= 3.5

    # latency penalty: use p50 of total method latencies if present
    p50 = _to_int(a.lat.get("p50_ms") or 0, 0)
    if p50 > 0:
        s -= min(5.0, float(p50) / 250.0)
    else:
        s -= min(4.0, float(a.elapsed_ms) / 500.0)

    return s


def _fix_hints(attempts: List[Attempt], *, strict: bool, progress: bool) -> List[str]:
    if not attempts:
        return []

    def _errtxt(a: Attempt) -> str:
        return (a.error or "").lower()

    any_429 = any(("429" in _errtxt(a) or "rate" in _errtxt(a) or "http_429" in " ".join(a.http_notes)) for a in attempts)
    any_timeout = any((a.error == "attempt_timeout" or "timeout" in _errtxt(a)) for a in attempts)
    any_dns = any((a.host and a.host_resolves == 0) for a in attempts)
    any_blockhash_fail = any((a.latest_blockhash.get("ok") != 1) for a in attempts)
    any_slot_fail = any((a.slot.get("ok") != 1 or _to_int(a.slot.get("val"), 0) <= 0) for a in attempts)
    any_health_blocked = any((a.health.get("err") == "rpc_method_not_found") for a in attempts)

    hints: List[str] = []
    if any_dns:
        hints.append("DNS failed for at least one RPC host. Check outbound DNS/network or switch RPC provider.")
    if any_timeout:
        hints.append("RPC timeout detected. Use a faster/paid endpoint, add SOL_RPC_URLS failovers, or raise CHECK_RPC_TIMEOUT_S.")
    if any_429:
        hints.append("Rate limiting detected. Add multiple RPCs via SOL_RPC_URLS or use a paid/dedicated endpoint.")
    if any_blockhash_fail:
        hints.append("getLatestBlockhash failed: endpoint is not viable for trading; replace it.")
    if any_slot_fail:
        hints.append("getSlot failed: endpoint may be unhealthy or blocked; replace it.")
    if strict and any_health_blocked:
        hints.append("Strict requires getHealth=='ok'. If your provider blocks getHealth, set CHECK_RPC_REQUIRE_HEALTH=0 or avoid strict.")

    if progress:
        hints.append("If slot progress is flaky, set CHECK_RPC_REQUIRE_ADVANCE=0 or disable CHECK_RPC_PROGRESS.")

    out: List[str] = []
    seen = set()
    for h in hints:
        if h not in seen:
            seen.add(h)
            out.append(h)
    return out[:10]


# =============================================================================
# probe logic
# =============================================================================
async def _probe_one(
    *,
    label: str,
    url: str,
    timeout_s: float,
    strict: bool,
    require_health: bool,
    progress: bool,
    progress_delay_s: float,
    require_advance: bool,
    headers: Dict[str, str],
    retries: int,
) -> Attempt:
    t0 = time.monotonic()
    host = _host_from_url(url)
    url_red = _redact_url(url)

    host_res = int(bool(host and _resolves(host)))

    if not _has_scheme(url):
        a = Attempt(
            label=label,
            url=url,
            url_redacted=url_red,
            host=host,
            host_resolves=host_res,
            ok_min=False,
            ok_strict=False,
            elapsed_ms=int((time.monotonic() - t0) * 1000),
            lat={},
            warnings=[],
            http_notes=[],
            error="bad_url_format: expected http(s)://...",
            health={"ok": 0, "val": None, "err": "bad_url_format", "http_status": 0},
            version={"ok": 0, "val": None, "err": "bad_url_format", "http_status": 0},
            slot={"ok": 0, "val": None, "err": "bad_url_format", "http_status": 0},
            slot_progress={"enabled": int(bool(progress)), "ok": 0, "from": None, "to": None, "err": "bad_url_format"},
            latest_blockhash={"ok": 0, "blockhash": None, "lastValidBlockHeight": None, "err": "bad_url_format", "http_status": 0},
            score=-999.0,
        )
        a.score = _score_attempt(a, strict=strict, progress=progress)
        return a

    # Prefer httpx if available
    use_httpx = False
    client: Any = None
    http_notes: List[str] = []
    try:
        import httpx  # type: ignore

        use_httpx = True
        client = httpx.AsyncClient(timeout=httpx.Timeout(timeout_s))
    except Exception:
        use_httpx = False
        client = object()
        http_notes.append("httpx_unavailable_using_urllib")

    warnings: List[str] = []
    err = ""
    ok_strict = True
    rid = random.randint(1, 100000)

    lat_all: List[int] = []

    # getHealth (optional unless strict/require_health)
    ok_h, res_h, err_h, st_h, meta_h = await _rpc_call(
        client=client,
        use_httpx=use_httpx,
        url=url,
        method="getHealth",
        params=[],
        timeout_s=timeout_s,
        headers=headers,
        rid=rid,
        retries=retries,
    )
    rid += (1 + retries)
    lat_all += [int(x) for x in (meta_h.get("lat_ms") or [])]
    health = {"ok": int(ok_h), "val": (res_h if ok_h else None), "err": err_h, "http_status": st_h, "meta": meta_h}
    if ok_h and isinstance(res_h, str) and res_h.lower() != "ok":
        warnings.append(f"health_not_ok:{res_h}")
    if (require_health or strict):
        if not (ok_h and isinstance(res_h, str) and res_h.lower() == "ok"):
            ok_strict = False
            err = err or "strict_fail: health!=ok"

    # getVersion (best-effort)
    ok_v, res_v, err_v, st_v, meta_v = await _rpc_call(
        client=client,
        use_httpx=use_httpx,
        url=url,
        method="getVersion",
        params=[],
        timeout_s=timeout_s,
        headers=headers,
        rid=rid,
        retries=max(0, retries - 1),
    )
    rid += (1 + max(0, retries - 1))
    lat_all += [int(x) for x in (meta_v.get("lat_ms") or [])]
    version = {"ok": int(ok_v), "val": (res_v if ok_v else None), "err": err_v, "http_status": st_v, "meta": meta_v}

    # getLatestBlockhash (REQUIRED)
    ok_b, res_b, err_b, st_b, meta_b = await _rpc_call(
        client=client,
        use_httpx=use_httpx,
        url=url,
        method="getLatestBlockhash",
        params=[{"commitment": "processed"}],
        timeout_s=timeout_s,
        headers=headers,
        rid=rid,
        retries=retries,
    )
    rid += (1 + retries)
    lat_all += [int(x) for x in (meta_b.get("lat_ms") or [])]

    blockhash = None
    lvbh = None
    bh_ok = False
    if ok_b and isinstance(res_b, dict):
        val = res_b.get("value") if isinstance(res_b.get("value"), dict) else {}
        if isinstance(val, dict):
            blockhash = val.get("blockhash") or None
            lvbh = val.get("lastValidBlockHeight") or None
        bh_ok = bool(blockhash)

    latest_blockhash = {
        "ok": 1 if bh_ok else 0,
        "blockhash": blockhash,
        "lastValidBlockHeight": lvbh,
        "err": "" if bh_ok else (err_b or "missing_blockhash_in_response"),
        "http_status": st_b,
        "meta": meta_b,
    }
    if not bh_ok:
        ok_strict = False
        err = err or "fail: latest_blockhash_missing"

    # getSlot (REQUIRED)
    ok_s, res_s, err_s, st_s, meta_s = await _rpc_call(
        client=client,
        use_httpx=use_httpx,
        url=url,
        method="getSlot",
        params=[{"commitment": "processed"}],
        timeout_s=timeout_s,
        headers=headers,
        rid=rid,
        retries=retries,
    )
    rid += (1 + retries)
    lat_all += [int(x) for x in (meta_s.get("lat_ms") or [])]

    slot0 = _to_int(res_s, 0) if ok_s else 0
    slot = {"ok": int(ok_s), "val": (slot0 if ok_s else None), "err": err_s, "http_status": st_s, "meta": meta_s}

    if not (ok_s and slot0 > 0):
        ok_strict = False
        err = err or "fail: slot_unavailable"

    # optional progress: getSlot twice
    slot_progress: Dict[str, Any] = {"enabled": int(bool(progress)), "ok": None, "from": None, "to": None, "err": ""}
    if progress and ok_s and slot0 > 0:
        await asyncio.sleep(max(0.05, float(progress_delay_s)))
        ok_s2, res_s2, err_s2, st_s2, meta_s2 = await _rpc_call(
            client=client,
            use_httpx=use_httpx,
            url=url,
            method="getSlot",
            params=[{"commitment": "processed"}],
            timeout_s=timeout_s,
            headers=headers,
            rid=rid,
            retries=max(0, retries - 1),
        )
        rid += (1 + max(0, retries - 1))
        lat_all += [int(x) for x in (meta_s2.get("lat_ms") or [])]

        slot1 = _to_int(res_s2, 0) if ok_s2 else 0
        if ok_s2 and slot1 > 0:
            prog_ok = bool(slot1 > slot0) if require_advance else bool(slot1 >= slot0)
            slot_progress = {"enabled": 1, "ok": int(prog_ok), "from": slot0, "to": slot1, "err": err_s2, "http_status": st_s2, "meta": meta_s2}
            if slot1 == slot0:
                warnings.append("slot_not_advancing_in_window")
            if strict and not prog_ok:
                ok_strict = False
                err = err or "strict_fail: slot_progress_failed"
        else:
            slot_progress = {"enabled": 1, "ok": 0, "from": slot0, "to": None, "err": err_s2 or "slot_progress_unavailable", "http_status": st_s2, "meta": meta_s2}
            if strict:
                ok_strict = False
                err = err or "strict_fail: slot_progress_failed"
    elif progress and strict:
        ok_strict = False
        err = err or "strict_fail: slot_progress_failed"

    ok_min = bool(bh_ok and ok_s and slot0 > 0)

    # close client
    if use_httpx:
        with contextlib.suppress(Exception):
            await client.aclose()

    # latency summary
    lat = {
        "count": len(lat_all),
        "p50_ms": _percentile(lat_all, 50) if lat_all else None,
        "p95_ms": _percentile(lat_all, 95) if lat_all else None,
        "max_ms": max(lat_all) if lat_all else None,
    }

    # HTTP notes
    for st in (st_h, st_v, st_b, st_s):
        if st == 429:
            http_notes.append("http_429_rate_limited")
            break

    elapsed_ms = int((time.monotonic() - t0) * 1000)
    a = Attempt(
        label=label,
        url=url,
        url_redacted=url_red,
        host=host,
        host_resolves=host_res,
        ok_min=ok_min,
        ok_strict=bool(ok_strict),
        elapsed_ms=elapsed_ms,
        lat=lat,
        warnings=warnings,
        http_notes=http_notes,
        error=err,
        health=health,
        version=version,
        slot=slot,
        slot_progress=slot_progress,
        latest_blockhash=latest_blockhash,
        score=0.0,
    )
    a.score = _score_attempt(a, strict=strict, progress=progress)
    return a


def _should_skip(args: argparse.Namespace) -> Tuple[bool, str]:
    if _envb("CHECK_RPC_DISABLED", False):
        return True, "CHECK_RPC_DISABLED=1"

    if args.force:
        return False, ""

    kill = _envb("KILL_SWITCH", False)
    dis_scanners = _envb("DISABLE_SCANNERS", False)

    explicit_cli_intent = bool(args.url or args.strict or args.progress or args.try_all or args.headers_json or args.headers_kv)
    if (kill or dis_scanners) and (not explicit_cli_intent):
        if kill:
            return True, "KILL_SWITCH=1 (no explicit CLI intent; use --force to override)"
        return True, "DISABLE_SCANNERS=1 (no explicit CLI intent; use --force to override)"

    return False, ""


async def _amain() -> int:
    ap = argparse.ArgumentParser(description="God-Tier Solana RPC precheck (Phase 0–7 LOCK-IN).")
    ap.add_argument("--url", action="append", default=[], help="RPC URL (repeatable). If omitted, uses env SOL_RPC_URL/RPC_URL.")
    ap.add_argument("--timeout", type=float, default=float(_envf("CHECK_RPC_TIMEOUT_S", 12.0)), help="Per-call timeout seconds.")
    ap.add_argument("--retries", type=int, default=int(_envi("CHECK_RPC_RETRIES", 1)), help="Retry count per RPC method (default 1).")
    ap.add_argument("--strict", action="store_true", help="Strict: require min viability + (health==ok if required) + progress if enabled.")
    ap.add_argument("--json", action="store_true", help="Emit JSON summary to STDOUT.")
    ap.add_argument("--json-only", action="store_true", help="Only emit JSON (no human logs).")
    ap.add_argument("--try-all", action="store_true", help="Try all candidates and pick best by score (instead of first OK).")
    ap.add_argument("--progress", action="store_true", help="Sample slot twice; strict may require progress depending on env.")
    ap.add_argument("--force", action="store_true", help="Run even if DISABLE_SCANNERS=1 / KILL_SWITCH=1.")
    ap.add_argument("--headers-json", default="", help="Override headers via JSON object string.")
    ap.add_argument("--headers-kv", default="", help="Override headers via k=v;... string.")
    args = ap.parse_args()

    strict = bool(args.strict or _envb("CHECK_RPC_STRICT", False))
    require_health = bool(_envb("CHECK_RPC_REQUIRE_HEALTH", False) or strict)
    progress = bool(args.progress or _envb("CHECK_RPC_PROGRESS", False))
    require_advance = bool(_envb("CHECK_RPC_REQUIRE_ADVANCE", False))
    progress_delay_s = float(_envf("CHECK_RPC_PROGRESS_DELAY_S", 0.4))
    timeout_s = max(2.0, float(args.timeout))
    retries = max(0, min(5, int(args.retries)))
    try_all = bool(args.try_all or _envb("CHECK_RPC_TRY_ALL", False))

    # JSON defaults: ON (preflight-friendly)
    emit_json = bool(args.json or args.json_only or _envb("CHECK_RPC_JSON", True) or _envb("CHECK_RPC_JSON_ONLY", False))
    json_only = bool(args.json_only or _envb("CHECK_RPC_JSON_ONLY", False))

    skip, reason = _should_skip(args)
    if skip:
        out = {"ts": _ts(), "ok": True, "skipped": True, "reason": reason}
        if emit_json:
            print(json.dumps(out, ensure_ascii=False, default=str))
        _hlog(f"SKIP ({reason})", json_only=json_only)
        return RC_OK

    headers = _parse_headers(args.headers_json, args.headers_kv)
    cands = _candidate_urls([u for u in args.url if u])

    if not cands:
        msg = "No RPC URL found. Set SOL_RPC_URL (or RPC_URL) or pass --url."
        out = {"ts": _ts(), "ok": False, "error": "missing_rpc_url", "detail": msg}
        if emit_json:
            print(json.dumps(out, ensure_ascii=False, default=str))
        _hlog("FAIL " + msg, json_only=json_only)
        return RC_CONNECT_FAIL

    _hlog(
        "BOOT "
        f"strict={int(strict)} require_health={int(require_health)} progress={int(progress)} "
        f"require_advance={int(require_advance)} timeout_s={timeout_s:.1f} retries={retries} try_all={int(try_all)} "
        f"candidates={[lbl for lbl, _ in cands]} headers={_sanitize_headers(headers)}",
        json_only=json_only,
    )

    attempts: List[Attempt] = []
    any_bad_url = False

    # Attempt-level watchdog prevents hang even if transport wedges.
    attempt_watchdog = max(8.0, timeout_s * (6.0 + 1.5 * retries))

    for lbl, url in cands:
        url_red = _redact_url(url)
        _hlog(f"TRY {lbl} url={url_red}", json_only=json_only)

        try:
            a = await asyncio.wait_for(
                _probe_one(
                    label=lbl,
                    url=url,
                    timeout_s=timeout_s,
                    strict=strict,
                    require_health=require_health,
                    progress=progress,
                    progress_delay_s=progress_delay_s,
                    require_advance=require_advance,
                    headers=headers,
                    retries=retries,
                ),
                timeout=attempt_watchdog,
            )
        except asyncio.TimeoutError:
            a = Attempt(
                label=lbl,
                url=url,
                url_redacted=url_red,
                host=_host_from_url(url),
                host_resolves=int(bool(_resolves(_host_from_url(url)))),
                ok_min=False,
                ok_strict=False,
                elapsed_ms=int(attempt_watchdog * 1000),
                lat={"count": 0, "p50_ms": None, "p95_ms": None, "max_ms": None},
                warnings=[],
                http_notes=["attempt_timeout"],
                error="attempt_timeout",
                health={"ok": 0, "val": None, "err": "timeout", "http_status": 0},
                version={"ok": 0, "val": None, "err": "timeout", "http_status": 0},
                slot={"ok": 0, "val": None, "err": "timeout", "http_status": 0},
                slot_progress={"enabled": int(bool(progress)), "ok": 0, "from": None, "to": None, "err": "timeout"},
                latest_blockhash={"ok": 0, "blockhash": None, "lastValidBlockHeight": None, "err": "timeout", "http_status": 0},
                score=-999.0,
            )
            a.score = _score_attempt(a, strict=strict, progress=progress)

        any_bad_url = any_bad_url or ("bad_url_format" in (a.error or ""))
        attempts.append(a)

        if not json_only:
            _hlog(
                f"RES {lbl} ok_min={int(a.ok_min)} ok_strict={int(a.ok_strict)} "
                f"health={(a.health.get('val') or '-') } slot={(a.slot.get('val') or '-') } "
                f"blockhash={'yes' if a.latest_blockhash.get('blockhash') else 'no'} "
                f"p50_ms={(a.lat.get('p50_ms') or '-') } elapsed_ms={a.elapsed_ms} score={a.score:.3f}",
                json_only=False,
            )
            for w in (a.warnings or [])[:10]:
                _hlog(f"WARN {lbl} {w}", json_only=False)
            if a.error:
                _hlog(f"ERR  {lbl} {a.error}", json_only=False)

        # early exit if viable and not try_all
        if not try_all:
            if strict and a.ok_min and a.ok_strict:
                break
            if (not strict) and a.ok_min:
                break

    picked: Optional[Attempt] = None
    if attempts:
        if strict:
            strict_ok = [x for x in attempts if x.ok_min and x.ok_strict]
            picked = max(strict_ok, key=lambda x: x.score) if strict_ok else None
        if picked is None:
            min_ok = [x for x in attempts if x.ok_min]
            picked = max(min_ok, key=lambda x: x.score) if min_ok else None

    ok = bool(picked is not None)

    # recommended ordering for SOL_RPC_URLS: sort by score desc (prefer viable)
    ordered = sorted(attempts, key=lambda x: x.score, reverse=True)
    recommended_urls = [a.url_redacted for a in ordered[:10]]

    summary: Dict[str, Any] = {
        "ts": _ts(),
        "ok": ok,
        "strict": int(strict),
        "require_health": int(require_health),
        "progress": int(progress),
        "require_advance": int(require_advance),
        "timeout_s": float(timeout_s),
        "retries": int(retries),
        "try_all": int(try_all),
        "env": {
            "SOL_RPC_URL_set": int(bool(_envs("SOL_RPC_URL", ""))),
            "RPC_URL_set": int(bool(_envs("RPC_URL", ""))),
            "SOL_RPC_URLS_count": len(_split_csv(_envs("SOL_RPC_URLS", ""))),
            "RPC_FAILOVER_knobs": _read_failover_knobs(),
        },
        "picked": (picked.label if picked else None),
        "picked_url": (picked.url_redacted if picked else None),
        "recommended_SOL_RPC_URLS_order": recommended_urls,
        "results": [a.public() for a in attempts],
        "fix_hints": _fix_hints(attempts, strict=strict, progress=progress),
        "headers_used": _sanitize_headers(headers),
    }

    if emit_json:
        print(json.dumps(summary, ensure_ascii=False, default=str))

    if ok:
        _hlog("SUMMARY ok=1 (rpc viable)", json_only=json_only)
        return RC_OK

    # classify failure
    if any_bad_url and all(("bad_url_format" in (a.error or "")) for a in attempts):
        _hlog("SUMMARY ok=0 (all candidates bad url format)", json_only=json_only)
        return RC_BAD_URL

    if attempts and all((a.error == "attempt_timeout") for a in attempts):
        _hlog("SUMMARY ok=0 (all attempts timed out)", json_only=json_only)
        return RC_TIMEOUT

    # If any candidate responded but failed requirements, call it requirements fail.
    any_responded = any(
        (a.health.get("http_status") or 0) or (a.slot.get("http_status") or 0) or (a.latest_blockhash.get("http_status") or 0)
        for a in attempts
    )
    if any_responded:
        _hlog("SUMMARY ok=0 (rpc responded but requirements failed)", json_only=json_only)
        return RC_REQUIREMENTS_FAIL

    _hlog("SUMMARY ok=0 (connect/respond failure)", json_only=json_only)
    return RC_CONNECT_FAIL


def main() -> None:
    try:
        rc = asyncio.run(_amain())
    except KeyboardInterrupt:
        rc = 130
    except Exception as e:
        _safe_print(f"[check_rpc] {_ts()} FAIL unhandled {type(e).__name__}: {e}", file=sys.stderr)
        rc = 1
    raise SystemExit(rc)


if __name__ == "__main__":
    # Windows-friendly event loop policy
    with contextlib.suppress(Exception):
        if os.name == "nt" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]
    main()
