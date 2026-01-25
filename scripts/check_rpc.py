from __future__ import annotations

"""
scripts/check_rpc.py — God-Tier Solana RPC Precheck (P0 LOCK-IN)
================================================================

Goal
----
Deterministic, secret-safe sanity check for Solana JSON-RPC endpoint(s).

✅ Option D included (best long-term):
-------------------------------------
This script auto-loads .env files (best-effort) so running locally behaves
like Render/start.sh and you don’t get “missing env vars” surprises.

Auto dotenv load order (unless disabled)
----------------------------------------
1) CHECK_RPC_ENV_FILE / ENV_FILE / DOTENV_PATH (csv/; supported)
2) /etc/secrets/.env.extra          (Render secrets convention)
3) <repo_root>/.env
4) <repo_root>/.env.local
5) <repo_root>/.env.extra

Policy:
- By default, dotenv load does NOT override existing OS env vars.
- Use --env-override (or CHECK_RPC_DOTENV_OVERRIDE=1) to override.
- Use --no-dotenv (or CHECK_RPC_NO_DOTENV=1) to disable.

Read-only checks
----------------
- URL format sanity + redaction (never prints creds/tokens unless explicitly asked)
- Optional DNS resolve (best-effort; strict can promote failures)
- JSON-RPC responsiveness
- getLatestBlockhash  (REQUIRED: minimum viability)
- getSlot             (REQUIRED: minimum viability)
- getHealth           (OPTIONAL unless strict/require_health)
- getVersion          (best-effort)
- Optional progress probe: getSlot twice (require >= or > via CHECK_RPC_REQUIRE_ADVANCE)

Alignment intent: utils_pkg/rpc_failover.py
------------------------------------------
- This script does NOT mutate banlists, state, or Redis.
- It produces a conservative “best RPC” pick + recommended ordering for SOL_RPC_URLS.
- It reports presence of RPC_FAILOVER_* knobs (masked).

Usage
-----
py -m scripts.check_rpc
py -m scripts.check_rpc --url https://...
py -m scripts.check_rpc --strict --progress
py -m scripts.check_rpc --try-all --json-only
py -m scripts.check_rpc --force
py -m scripts.check_rpc --no-health
py -m scripts.check_rpc --dns
py -m scripts.check_rpc --try-all --unsafe-print-urls   (DANGEROUS: prints full URLs)

Exit codes
----------
0   OK / skipped
1   Missing URL or all candidates failed to connect/respond
2   RPC responded but requirements failed (blockhash/slot/health/progress)
3   Bad URL format (all candidates invalid scheme)
124 All attempts timed out (attempt-level watchdog tripped)
130 Ctrl-C
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
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

# -----------------------------
# constants / exit codes
# -----------------------------
_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}

RC_OK = 0
RC_CONNECT_FAIL = 1
RC_REQUIREMENTS_FAIL = 2
RC_BAD_URL = 3
RC_TIMEOUT = 124

# redaction
_URL_CRED_RE = re.compile(r"(://)([^/@\s]+)@")
_QS_SECRET_KEY_RE = re.compile(
    r"(?i)^(api[-_]?key|apikey|token|key|secret|sig|signature|auth|authorization|access[-_]?token)$"
)
_LONG_TOKENISH_RE = re.compile(r"\b[A-Za-z0-9_\-]{28,}\b")  # conservative
_HDR_SENSITIVE_RE = re.compile(r"(?i)^(authorization|x-api-key|api-key|token|secret|signature)$")


# =============================================================================
# tiny stdout/stderr helpers (safe before logging is configured)
# =============================================================================
def _ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _safe_stderr(line: str) -> None:
    try:
        sys.stderr.write(line + ("" if line.endswith("\n") else "\n"))
        sys.stderr.flush()
    except Exception:
        pass


# =============================================================================
# repo bootstrap (keeps behavior consistent on Windows + Render)
# =============================================================================
def _bootstrap_repo_root() -> str:
    """
    Detect repo root via common markers and export:
      - os.environ["REPO_ROOT"]
    Also adds repo root and repo_root/src to sys.path so scripts can import
    logging_setup + utils_pkg without relying on cwd quirks.
    """
    try:
        here = Path(__file__).resolve()
    except Exception:
        return os.getenv("REPO_ROOT", "") or ""

    markers = ("pyproject.toml", "requirements.txt", "setup.cfg", ".git")
    candidates = [here.parent, *list(here.parents)]
    root: Path | None = None

    for p in candidates:
        try:
            if any((p / m).exists() for m in markers):
                root = p
                break
            if (p / "src").is_dir():
                root = p
                break
        except Exception:
            continue

    if root is None:
        root = here.parents[1] if len(here.parents) > 1 else here.parent

    os.environ.setdefault("REPO_ROOT", str(root))

    rs = str(root)
    if rs and rs not in sys.path:
        sys.path.insert(0, rs)

    src = root / "src"
    if src.is_dir():
        ss = str(src)
        if ss not in sys.path:
            sys.path.insert(0, ss)

    return str(root)


_REPO_ROOT = _bootstrap_repo_root()


# =============================================================================
# env helpers (raw os.environ; dotenv loader sits below)
# =============================================================================
def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    s = v if v is not None else d
    s = str(s).strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    return s


def _envb(k: str, d: bool = False) -> bool:
    s = _envs(k, "")
    if s == "":
        return bool(d)
    x = s.lower().strip()
    if x in _TRUE:
        return True
    if x in _FALSE:
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


def _split_csv(s: str) -> list[str]:
    return [
        p.strip() for p in (s or "").replace(";", ",").replace("\n", ",").split(",") if p.strip()
    ]


def _mask_long_tokens_enabled() -> bool:
    if "CHECK_RPC_MASK_LONG_TOKENS" in os.environ:
        return _envb("CHECK_RPC_MASK_LONG_TOKENS", True)
    return _envb("PREFLIGHT_MASK_LONG_TOKENS", True)


def _unsafe_print_urls_enabled() -> bool:
    if "CHECK_RPC_UNSAFE_PRINT_URLS" in os.environ:
        return _envb("CHECK_RPC_UNSAFE_PRINT_URLS", False)
    return False


# =============================================================================
# dotenv (Option D) — best-effort, no external deps
# =============================================================================
_DOTENV_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _strip_inline_comment(raw: str) -> str:
    """
    Removes inline comments for unquoted values:
      KEY=abc # comment  -> abc
    Keeps '#' inside quotes intact.
    """
    s = raw.rstrip()
    if not s:
        return s

    # if starts with quote, keep as-is (we'll unquote later)
    if s.lstrip().startswith(("'", '"')):
        return s

    # find first ' #' pattern
    idx = s.find(" #")
    if idx != -1:
        return s[:idx].rstrip()
    return s


def _unquote_env_value(v: str) -> str:
    t = v.strip()
    if not t:
        return ""
    if (t.startswith('"') and t.endswith('"')) or (t.startswith("'") and t.endswith("'")):
        inner = t[1:-1]
        # minimal unescape for common sequences (safe, not fancy)
        inner = inner.replace("\\n", "\n").replace("\\r", "\r").replace("\\t", "\t")
        inner = inner.replace('\\"', '"').replace("\\'", "'").replace("\\\\", "\\")
        return inner
    return t


def _parse_dotenv_lines(text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for raw in (text or "").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        if not _DOTENV_KEY_RE.match(k):
            continue
        v = _strip_inline_comment(v)
        out[k] = _unquote_env_value(v)
    return out


def _dotenv_default_paths(repo_root: str) -> list[Path]:
    rr = Path(repo_root) if repo_root else Path.cwd()
    paths: list[Path] = []

    # explicit env file hints (csv or ;)
    for key in ("CHECK_RPC_ENV_FILE", "ENV_FILE", "DOTENV_PATH"):
        val = _envs(key, "")
        if val:
            for part in _split_csv(val):
                p = Path(os.path.expanduser(os.path.expandvars(part))).resolve()
                paths.append(p)

    # Render secrets convention
    paths.append(Path("/etc/secrets/.env.extra"))

    # repo-local
    paths.append((rr / ".env").resolve())
    paths.append((rr / ".env.local").resolve())
    paths.append((rr / ".env.extra").resolve())

    # de-dupe, keep order
    uniq: list[Path] = []
    seen = set()
    for p in paths:
        sp = str(p)
        if sp in seen:
            continue
        seen.add(sp)
        uniq.append(p)
    return uniq


def _load_dotenv_files(
    *,
    repo_root: str,
    extra_paths: Sequence[str],
    enable: bool,
    override: bool,
) -> dict[str, Any]:
    """
    Best-effort .env loader. Never prints secrets. Returns structured report.
    """
    report: dict[str, Any] = {
        "enabled": int(bool(enable)),
        "override": int(bool(override)),
        "loaded_files": [],
        "missing_files": [],
        "parse_errors": [],
        "set_vars": 0,
        "skipped_existing": 0,
        "repo_root": repo_root,
    }
    if not enable:
        return report

    paths = _dotenv_default_paths(repo_root)
    for ep in extra_paths:
        if not ep:
            continue
        for part in _split_csv(ep):
            p = Path(os.path.expanduser(os.path.expandvars(part))).resolve()
            paths.append(p)

    # de-dupe again with extra paths appended
    uniq: list[Path] = []
    seen = set()
    for p in paths:
        sp = str(p)
        if sp in seen:
            continue
        seen.add(sp)
        uniq.append(p)

    for p in uniq:
        try:
            if not p.exists() or not p.is_file():
                report["missing_files"].append(str(p))
                continue
        except Exception:
            report["missing_files"].append(str(p))
            continue

        try:
            text = p.read_text(encoding="utf-8", errors="ignore")
            kv = _parse_dotenv_lines(text)
        except Exception as e:
            report["parse_errors"].append({"file": str(p), "err": f"{type(e).__name__}: {e}"})
            continue

        set_count = 0
        skip_count = 0
        for k, v in kv.items():
            if not override and (k in os.environ):
                skip_count += 1
                continue
            os.environ[k] = v
            set_count += 1

        report["loaded_files"].append(
            {"file": str(p), "vars": len(kv), "set": set_count, "skipped_existing": skip_count}
        )
        report["set_vars"] += int(set_count)
        report["skipped_existing"] += int(skip_count)

    return report


# =============================================================================
# logging (canonical root logging_setup.py, but fully optional for scripts)
# =============================================================================
def _get_logger():
    with contextlib.suppress(Exception):
        from logging_setup import get_logger  # type: ignore

        return get_logger()
    return None


_LOG = None  # assigned after dotenv load inside _amain


def _hlog(msg: str, *, json_only: bool) -> None:
    if json_only:
        return
    global _LOG
    if _LOG is not None:
        with contextlib.suppress(Exception):
            _LOG.info(msg)  # type: ignore[attr-defined]
            return
    _safe_stderr(f"[check_rpc] {_ts()} {msg}")


# =============================================================================
# redaction / safety
# =============================================================================
def _has_http_scheme(url: str) -> bool:
    u = (url or "").strip().lower()
    return u.startswith("http://") or u.startswith("https://")


def _redact_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    u = _URL_CRED_RE.sub(r"\1***@", u)

    low = u.lower().strip()
    if low.startswith("ws://") or low.startswith("wss://"):
        return "ws(s)://… (INVALID: use http(s) JSON-RPC URL)"

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
        if len(path) > 64:
            parts = [x for x in path.split("/") if x]
            path = "/" + (parts[0] if parts else "…") + "/…"

        out = urlunparse(p._replace(path=path, query=query))
    except Exception:
        out = u.split("?", 1)[0] + "?..." if "?" in u else u

    if _mask_long_tokens_enabled():
        out = _LONG_TOKENISH_RE.sub("***", out)
    return out


def _normalize_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    u = u.strip()
    if u.endswith("/") and len(u) > 9:
        u = u.rstrip("/")
    return u


def _host_from_url(url: str) -> str:
    try:
        return (urlparse(url).hostname or "").strip()
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


def _sanitize_headers(h: dict[str, str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for k, v in (h or {}).items():
        kk = str(k or "")
        vv = str(v or "")
        if _HDR_SENSITIVE_RE.match(kk.strip().lower()):
            out[kk] = "***"
        else:
            out[kk] = "***" if (_mask_long_tokens_enabled() and len(vv) >= 24) else vv
    return out


def _safe_json(x: Any) -> Any:
    try:
        json.dumps(x, default=str)
        return x
    except Exception:
        return str(x)


# =============================================================================
# headers parsing
# =============================================================================
def _parse_headers(cli_headers_json: str = "", cli_headers_kv: str = "") -> dict[str, str]:
    """
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
        out: dict[str, str] = {}
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
        out2: dict[str, str] = {}
        parts = [p.strip() for p in kv.replace(",", ";").split(";") if p.strip()]
        for p in parts:
            if "=" in p:
                a, b = p.split("=", 1)
                out2[a.strip()] = b.strip()
        return out2

    return {}


# =============================================================================
# candidates + failover knobs
# =============================================================================
def _candidate_urls(cli_urls: Sequence[str]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []

    # CLI (repeatable)
    for i, u in enumerate(cli_urls):
        uu = (u or "").strip()
        if not uu:
            continue
        if "," in uu or ";" in uu:
            for part in _split_csv(uu):
                out.append((f"CLI_{i + 1}", _normalize_url(part)))
        else:
            out.append((f"CLI_{i + 1}", _normalize_url(uu)))

    # ENV primary
    for k in ("SOL_RPC_URL", "RPC_URL", "SOLANA_RPC_URL"):
        v = _envs(k, "")
        if v:
            if "," in v or ";" in v:
                for part in _split_csv(v):
                    out.append((k, _normalize_url(part)))
            else:
                out.append((k, _normalize_url(v.strip())))

    # ENV fallbacks
    for k in ("SOL_RPC_URLS", "RPC_URLS"):
        for u in _split_csv(_envs(k, "")):
            out.append((k, _normalize_url(u.strip())))

    # dedupe by exact URL
    seen = set()
    uniq: list[tuple[str, str]] = []
    for lbl, u in out:
        if not u or u in seen:
            continue
        seen.add(u)
        uniq.append((lbl, u))
    return uniq


def _read_failover_knobs() -> dict[str, Any]:
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
    out: dict[str, Any] = {}
    for k in keys:
        if k in os.environ:
            v = os.getenv(k, "") or ""
            out[k] = "***" if (_mask_long_tokens_enabled() and len(v) >= 24) else v
    return out


# =============================================================================
# optional proxy (best-effort)
# =============================================================================
def _proxy_cfg() -> tuple[str | None, Any, bool]:
    """
    Returns (proxy_url, proxy_auth, insecure_ssl) if available.
    Uses utils_pkg.proxy_client.proxy_cfg() (canonical proxy config).
    """
    with contextlib.suppress(Exception):
        from utils_pkg.proxy_client import proxy_cfg  # type: ignore

        val = proxy_cfg()
        if isinstance(val, tuple | list) and len(val) >= 3:
            proxy = str(val[0]) if val[0] else None
            return proxy, val[1], bool(val[2])
        if isinstance(val, tuple | list) and len(val) == 2:
            proxy = str(val[0]) if val[0] else None
            return proxy, val[1], False
    return None, None, False


# =============================================================================
# transport
# =============================================================================
class _Transport:
    async def aclose(self) -> None:
        return None

    async def post_json(
        self, url: str, payload: dict[str, Any], headers: dict[str, str]
    ) -> tuple[int, dict[str, Any] | None, str]:
        raise NotImplementedError


class _AiohttpTransport(_Transport):
    def __init__(self, *, timeout_s: float, proxy: str | None, proxy_auth: Any, insecure_ssl: bool):
        import aiohttp  # type: ignore

        self._timeout = aiohttp.ClientTimeout(total=max(1.0, float(timeout_s)))
        self._proxy = proxy
        self._proxy_auth = proxy_auth
        connector = aiohttp.TCPConnector(ssl=False) if insecure_ssl else None
        self._session = aiohttp.ClientSession(timeout=self._timeout, connector=connector)

    async def aclose(self) -> None:
        with contextlib.suppress(Exception):
            await self._session.close()

    async def post_json(
        self, url: str, payload: dict[str, Any], headers: dict[str, str]
    ) -> tuple[int, dict[str, Any] | None, str]:
        try:
            async with self._session.post(
                url,
                json=payload,
                headers=headers,
                proxy=self._proxy,
                proxy_auth=self._proxy_auth,
            ) as r:
                status = int(r.status)
                txt = await r.text()
        except Exception as e:
            return 0, None, f"{type(e).__name__}: {e}"

        try:
            j = json.loads(txt)
            return status, j if isinstance(j, dict) else None, ""
        except Exception:
            t = (txt or "").strip()
            return status, None, (t[:300] + "…") if len(t) > 300 else t


class _HttpxTransport(_Transport):
    def __init__(self, *, timeout_s: float):
        import httpx  # type: ignore

        self._client = httpx.AsyncClient(timeout=httpx.Timeout(timeout_s))

    async def aclose(self) -> None:
        with contextlib.suppress(Exception):
            await self._client.aclose()

    async def post_json(
        self, url: str, payload: dict[str, Any], headers: dict[str, str]
    ) -> tuple[int, dict[str, Any] | None, str]:
        try:
            r = await self._client.post(url, json=payload, headers=headers)
            status = int(getattr(r, "status_code", 0) or 0)
            with contextlib.suppress(Exception):
                j = r.json()
                return status, j if isinstance(j, dict) else None, ""
            txt = (getattr(r, "text", "") or "").strip()
            return status, None, (txt[:300] + "…") if len(txt) > 300 else txt
        except Exception as e:
            return 0, None, f"{type(e).__name__}: {e}"


class _UrllibTransport(_Transport):
    def __init__(self, *, timeout_s: float):
        self._timeout_s = float(timeout_s)

    async def post_json(
        self, url: str, payload: dict[str, Any], headers: dict[str, str]
    ) -> tuple[int, dict[str, Any] | None, str]:
        import urllib.request

        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url=url,
            data=data,
            headers={**{"content-type": "application/json"}, **(headers or {})},
            method="POST",
        )

        def _sync() -> tuple[int, dict[str, Any] | None, str]:
            try:
                with urllib.request.urlopen(req, timeout=self._timeout_s) as resp:
                    status = int(getattr(resp, "status", 200) or 200)
                    body = resp.read().decode("utf-8", "ignore")
            except Exception as e:
                return 0, None, f"{type(e).__name__}: {e}"
            try:
                j = json.loads(body)
                return status, j if isinstance(j, dict) else None, ""
            except Exception:
                t = (body or "").strip()
                return status, None, (t[:300] + "…") if len(t) > 300 else t

        return await asyncio.to_thread(_sync)


async def _make_transport(
    *, timeout_s: float, use_proxy: bool
) -> tuple[_Transport, dict[str, Any]]:
    """
    Picks a transport:
      - If use_proxy and aiohttp available -> aiohttp (proxy supported)
      - Else prefer httpx
      - Else aiohttp
      - Else urllib
    """
    meta: dict[str, Any] = {"transport": "", "proxy_enabled": 0}
    proxy = None
    proxy_auth = None
    insecure_ssl = False

    if use_proxy:
        proxy, proxy_auth, insecure_ssl = _proxy_cfg()
        if proxy:
            meta["proxy_enabled"] = 1
            meta["proxy_masked"] = _redact_url(proxy)
            meta["proxy_insecure_ssl"] = int(bool(insecure_ssl))
        else:
            meta["proxy_enabled"] = 0
            meta["proxy_error"] = (
                "CHECK_RPC_USE_PROXY=1 but proxy_cfg unavailable or missing PROXY_* env"
            )

    if use_proxy and proxy:
        with contextlib.suppress(Exception):
            t = _AiohttpTransport(
                timeout_s=timeout_s, proxy=proxy, proxy_auth=proxy_auth, insecure_ssl=insecure_ssl
            )
            meta["transport"] = "aiohttp"
            return t, meta

    with contextlib.suppress(Exception):
        t2 = _HttpxTransport(timeout_s=timeout_s)
        meta["transport"] = "httpx"
        return t2, meta

    with contextlib.suppress(Exception):
        t3 = _AiohttpTransport(timeout_s=timeout_s, proxy=None, proxy_auth=None, insecure_ssl=False)
        meta["transport"] = "aiohttp"
        return t3, meta

    meta["transport"] = "urllib"
    return _UrllibTransport(timeout_s=timeout_s), meta


# =============================================================================
# json-rpc calls
# =============================================================================
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


def _percentile(vals: list[int], p: float) -> int | None:
    if not vals:
        return None
    xs = sorted(int(x) for x in vals)
    if len(xs) == 1:
        return xs[0]
    k = max(0, min(len(xs) - 1, int(round((p / 100.0) * (len(xs) - 1)))))
    return xs[k]


async def _rpc_call_once(
    *,
    tr: _Transport,
    url: str,
    method: str,
    params: Any | None,
    headers: dict[str, str],
    rid: int,
) -> tuple[bool, Any, str, int, int]:
    """
    Returns: (ok, result, err, http_status, elapsed_ms)
    ok means JSON-RPC returned result and did not include "error".
    """
    payload: dict[str, Any] = {"jsonrpc": "2.0", "id": rid, "method": method}
    if params is not None:
        payload["params"] = params

    t0 = time.monotonic()
    status, j, text_err = await tr.post_json(url, payload, headers)
    elapsed_ms = int((time.monotonic() - t0) * 1000)

    if status == 0 and (text_err or j is None):
        return False, None, text_err or "transport_error", 0, elapsed_ms

    if not isinstance(j, dict):
        if status == 429:
            return False, None, "http_429_rate_limited", status, elapsed_ms
        if status >= 400:
            return False, None, f"http_{status}:{text_err or 'bad_response'}", status, elapsed_ms
        return False, None, text_err or "bad_response", status, elapsed_ms

    if j.get("error"):
        e = j["error"]
        msg = ""
        try:
            msg = str(e.get("message") or e.get("data") or e) if isinstance(e, dict) else str(e)
        except Exception:
            msg = "rpc_error"
        low = msg.lower()
        if "too many requests" in low or "rate" in low:
            return False, None, "rpc_rate_limited", status, elapsed_ms
        if "method not found" in low:
            return False, None, "rpc_method_not_found", status, elapsed_ms
        msg = msg.strip()
        return False, None, (msg[:320] + "…") if len(msg) > 320 else msg, status, elapsed_ms

    return True, j.get("result"), "", status, elapsed_ms


async def _rpc_call(
    *,
    tr: _Transport,
    url: str,
    method: str,
    params: Any | None,
    headers: dict[str, str],
    rid: int,
    retries: int,
    base_backoff_s: float = 0.25,
) -> tuple[bool, Any, str, int, dict[str, Any]]:
    """
    Retry wrapper. Retries only on timeouts/rate-limit/transport-ish errors.
    Returns: (ok, result, err, http_status, meta)
    """
    meta: dict[str, Any] = {"attempts": 0, "lat_ms": [], "last_http": 0}
    last_err = ""
    last_http = 0

    for i in range(max(1, 1 + int(retries))):
        meta["attempts"] += 1
        ok, res, err, http_status, ms = await _rpc_call_once(
            tr=tr,
            url=url,
            method=method,
            params=params,
            headers=headers,
            rid=rid + i,
        )
        meta["lat_ms"].append(int(ms))
        meta["last_http"] = int(http_status or 0)

        if ok:
            return True, res, "", http_status, meta

        last_err, last_http = err or "rpc_call_failed", http_status or 0

        low = (err or "").lower()
        retryable = (
            err in {"http_429_rate_limited", "rpc_rate_limited"}
            or "timeout" in low
            or "connect" in low
            or "temporar" in low
            or "reset" in low
            or "tls" in low
        )

        if i < retries and retryable:
            await asyncio.sleep(base_backoff_s * (2**i) * (0.75 + 0.5 * random.random()))
            continue
        break

    return False, None, last_err, last_http, meta


# =============================================================================
# result model + scoring
# =============================================================================
@dataclass
class Attempt:
    label: str
    url: str
    url_masked: str
    host: str
    host_resolves: int

    ok_min: bool
    ok_strict: bool

    elapsed_ms: int
    lat: dict[str, Any]

    warnings: list[str]
    http_notes: list[str]
    error: str

    health: dict[str, Any]
    version: dict[str, Any]
    slot: dict[str, Any]
    slot_progress: dict[str, Any]
    latest_blockhash: dict[str, Any]

    score: float

    def public(self, *, unsafe_urls: bool) -> dict[str, Any]:
        return {
            "label": self.label,
            "url": (self.url if unsafe_urls else self.url_masked),
            "host": self.host,
            "host_resolves": int(self.host_resolves),
            "ok_min": int(self.ok_min),
            "ok_strict": int(self.ok_strict),
            "elapsed_ms": int(self.elapsed_ms),
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


def _score_attempt(a: Attempt, *, strict: bool, progress: bool) -> float:
    s = 0.0

    if a.ok_min:
        s += 14.0
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

    if a.host and a.host_resolves == 1:
        s += 0.25

    s -= min(3.0, 0.35 * len(a.warnings or []))
    if "http_429" in " ".join(a.http_notes or []):
        s -= 1.5
    if a.error:
        s -= 3.5

    p50 = _to_int(a.lat.get("p50_ms") or 0, 0)
    if p50 > 0:
        s -= min(6.0, float(p50) / 225.0)
    else:
        s -= min(5.0, float(a.elapsed_ms) / 450.0)

    return s


def _fix_hints(attempts: list[Attempt], *, strict: bool, progress: bool) -> list[str]:
    if not attempts:
        return []

    def _errtxt(a: Attempt) -> str:
        return (a.error or "").lower()

    any_429 = any(
        ("429" in _errtxt(a) or "rate" in _errtxt(a) or "http_429" in " ".join(a.http_notes))
        for a in attempts
    )
    any_timeout = any((a.error == "attempt_timeout" or "timeout" in _errtxt(a)) for a in attempts)
    any_dns = any((a.host and a.host_resolves == 0) for a in attempts)
    any_blockhash_fail = any((a.latest_blockhash.get("ok") != 1) for a in attempts)
    any_slot_fail = any(
        (a.slot.get("ok") != 1 or _to_int(a.slot.get("val"), 0) <= 0) for a in attempts
    )
    any_health_blocked = any((a.health.get("err") == "rpc_method_not_found") for a in attempts)

    hints: list[str] = []
    if any_dns:
        hints.append(
            "DNS failed for at least one RPC host. Check outbound DNS/network or switch RPC provider."
        )
    if any_timeout:
        hints.append(
            "RPC timeout detected. Use a faster/paid endpoint, add SOL_RPC_URLS failovers, or raise CHECK_RPC_TIMEOUT_S."
        )
    if any_429:
        hints.append(
            "Rate limiting detected. Add multiple RPCs via SOL_RPC_URLS or use a paid/dedicated endpoint."
        )
    if any_blockhash_fail:
        hints.append("getLatestBlockhash failed: endpoint is not viable for trading; replace it.")
    if any_slot_fail:
        hints.append("getSlot failed: endpoint may be unhealthy or blocked; replace it.")
    if strict and any_health_blocked:
        hints.append(
            "Strict requires getHealth=='ok'. If your provider blocks getHealth, use --no-health or set CHECK_RPC_REQUIRE_HEALTH=0."
        )

    if progress:
        hints.append(
            "If slot progress is flaky, set CHECK_RPC_REQUIRE_ADVANCE=0 or disable CHECK_RPC_PROGRESS."
        )

    out: list[str] = []
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
    tr: _Transport,
    label: str,
    url: str,
    strict: bool,
    require_health: bool,
    progress: bool,
    progress_delay_s: float,
    require_advance: bool,
    dns_check: bool,
    headers: dict[str, str],
    retries: int,
    commitment: str,
) -> Attempt:
    t0 = time.monotonic()
    url = _normalize_url(url)
    url_masked = _redact_url(url)
    host = _host_from_url(url)

    host_res = 0
    if dns_check:
        host_res = int(bool(host and _resolves(host)))

    if not _has_http_scheme(url):
        a = Attempt(
            label=label,
            url=url,
            url_masked=url_masked,
            host=host,
            host_resolves=host_res,
            ok_min=False,
            ok_strict=False,
            elapsed_ms=int((time.monotonic() - t0) * 1000),
            lat={"count": 0, "p50_ms": None, "p95_ms": None, "max_ms": None},
            warnings=[],
            http_notes=[],
            error="bad_url_format: expected http(s)://... (not ws:// or wss://)",
            health={"ok": 0, "val": None, "err": "bad_url_format", "http_status": 0},
            version={"ok": 0, "val": None, "err": "bad_url_format", "http_status": 0},
            slot={"ok": 0, "val": None, "err": "bad_url_format", "http_status": 0},
            slot_progress={
                "enabled": int(bool(progress)),
                "ok": 0,
                "from": None,
                "to": None,
                "err": "bad_url_format",
            },
            latest_blockhash={
                "ok": 0,
                "blockhash": None,
                "lastValidBlockHeight": None,
                "err": "bad_url_format",
                "http_status": 0,
            },
            score=-999.0,
        )
        a.score = _score_attempt(a, strict=strict, progress=progress)
        return a

    warnings: list[str] = []
    http_notes: list[str] = []
    err = ""
    ok_strict = True
    rid = random.randint(1, 100_000)
    lat_all: list[int] = []

    # getHealth (optional unless strict/require_health)
    ok_h, res_h, err_h, st_h, meta_h = await _rpc_call(
        tr=tr,
        url=url,
        method="getHealth",
        params=[],
        headers=headers,
        rid=rid,
        retries=retries,
    )
    rid += 1 + retries
    lat_all += [int(x) for x in (meta_h.get("lat_ms") or [])]
    health = {
        "ok": int(ok_h),
        "val": (res_h if ok_h else None),
        "err": err_h,
        "http_status": st_h,
        "meta": meta_h,
    }

    if ok_h and isinstance(res_h, str) and res_h.lower() != "ok":
        warnings.append(f"health_not_ok:{res_h}")

    if require_health and not (ok_h and isinstance(res_h, str) and res_h.lower() == "ok"):
        ok_strict = False
        err = err or "strict_fail: health!=ok"

    # getVersion (best-effort)
    ok_v, res_v, err_v, st_v, meta_v = await _rpc_call(
        tr=tr,
        url=url,
        method="getVersion",
        params=[],
        headers=headers,
        rid=rid,
        retries=max(0, retries - 1),
    )
    rid += 1 + max(0, retries - 1)
    lat_all += [int(x) for x in (meta_v.get("lat_ms") or [])]
    version = {
        "ok": int(ok_v),
        "val": (res_v if ok_v else None),
        "err": err_v,
        "http_status": st_v,
        "meta": meta_v,
    }

    # getLatestBlockhash (REQUIRED)
    ok_b, res_b, err_b, st_b, meta_b = await _rpc_call(
        tr=tr,
        url=url,
        method="getLatestBlockhash",
        params=[{"commitment": commitment}],
        headers=headers,
        rid=rid,
        retries=retries,
    )
    rid += 1 + retries
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
        tr=tr,
        url=url,
        method="getSlot",
        params=[{"commitment": commitment}],
        headers=headers,
        rid=rid,
        retries=retries,
    )
    rid += 1 + retries
    lat_all += [int(x) for x in (meta_s.get("lat_ms") or [])]

    slot0 = _to_int(res_s, 0) if ok_s else 0
    slot = {
        "ok": int(ok_s),
        "val": (slot0 if ok_s else None),
        "err": err_s,
        "http_status": st_s,
        "meta": meta_s,
    }
    if not (ok_s and slot0 > 0):
        ok_strict = False
        err = err or "fail: slot_unavailable"

    # optional progress: getSlot twice
    slot_progress: dict[str, Any] = {
        "enabled": int(bool(progress)),
        "ok": None,
        "from": None,
        "to": None,
        "err": "",
    }
    if progress and ok_s and slot0 > 0:
        await asyncio.sleep(max(0.05, float(progress_delay_s)))
        ok_s2, res_s2, err_s2, st_s2, meta_s2 = await _rpc_call(
            tr=tr,
            url=url,
            method="getSlot",
            params=[{"commitment": commitment}],
            headers=headers,
            rid=rid,
            retries=max(0, retries - 1),
        )
        rid += 1 + max(0, retries - 1)
        lat_all += [int(x) for x in (meta_s2.get("lat_ms") or [])]

        slot1 = _to_int(res_s2, 0) if ok_s2 else 0
        if ok_s2 and slot1 > 0:
            prog_ok = bool(slot1 > slot0) if require_advance else bool(slot1 >= slot0)
            slot_progress = {
                "enabled": 1,
                "ok": int(prog_ok),
                "from": slot0,
                "to": slot1,
                "err": err_s2,
                "http_status": st_s2,
                "meta": meta_s2,
            }
            if slot1 == slot0:
                warnings.append("slot_not_advancing_in_window")
            if strict and not prog_ok:
                ok_strict = False
                err = err or "strict_fail: slot_progress_failed"
        else:
            slot_progress = {
                "enabled": 1,
                "ok": 0,
                "from": slot0,
                "to": None,
                "err": err_s2 or "slot_progress_unavailable",
                "http_status": st_s2,
                "meta": meta_s2,
            }
            if strict:
                ok_strict = False
                err = err or "strict_fail: slot_progress_failed"
    elif progress and strict:
        ok_strict = False
        err = err or "strict_fail: slot_progress_failed"

    # HTTP notes
    for st in (st_h, st_v, st_b, st_s):
        if st == 429:
            http_notes.append("http_429_rate_limited")
            break

    ok_min = bool(bh_ok and ok_s and slot0 > 0)

    lat = {
        "count": len(lat_all),
        "p50_ms": _percentile(lat_all, 50) if lat_all else None,
        "p95_ms": _percentile(lat_all, 95) if lat_all else None,
        "max_ms": max(lat_all) if lat_all else None,
    }

    elapsed_ms = int((time.monotonic() - t0) * 1000)
    a = Attempt(
        label=label,
        url=url,
        url_masked=url_masked,
        host=host,
        host_resolves=0 if not dns_check else int(bool(host and _resolves(host))),
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


def _should_skip(args: argparse.Namespace) -> tuple[bool, str]:
    if _envb("CHECK_RPC_DISABLED", False):
        return True, "CHECK_RPC_DISABLED=1"

    if args.force:
        return False, ""

    kill = _envb("KILL_SWITCH", False)
    dis_scanners = _envb("DISABLE_SCANNERS", False)

    explicit_cli_intent = bool(
        args.url
        or args.strict
        or args.progress
        or args.try_all
        or args.headers_json
        or args.headers_kv
        or args.use_proxy
        or args.dns
        or args.no_health
        or args.require_health
        or args.env
    )

    if (kill or dis_scanners) and (not explicit_cli_intent):
        if kill:
            return True, "KILL_SWITCH=1 (no explicit CLI intent; use --force to override)"
        return True, "DISABLE_SCANNERS=1 (no explicit CLI intent; use --force to override)"

    return False, ""


# =============================================================================
# main
# =============================================================================
async def _amain() -> int:
    # Stage-0: parse dotenv toggles first, load dotenv, then proceed with full parse.
    ap0 = argparse.ArgumentParser(add_help=False)
    ap0.add_argument("--no-dotenv", action="store_true", help="Disable automatic .env loading.")
    ap0.add_argument(
        "--env",
        action="append",
        default=[],
        help="Additional .env file path(s) to load (repeatable; csv/; ok).",
    )
    ap0.add_argument(
        "--env-override",
        action="store_true",
        help="Allow dotenv vars to override existing OS env vars.",
    )
    pre, _rest = ap0.parse_known_args()

    dotenv_enabled = not bool(pre.no_dotenv or _envb("CHECK_RPC_NO_DOTENV", False))
    dotenv_override = bool(pre.env_override or _envb("CHECK_RPC_DOTENV_OVERRIDE", False))
    dotenv_report = _load_dotenv_files(
        repo_root=_REPO_ROOT,
        extra_paths=pre.env or [],
        enable=dotenv_enabled,
        override=dotenv_override,
    )

    # Now that env is loaded, initialize logger (it may depend on env).
    global _LOG
    _LOG = _get_logger()

    ap = argparse.ArgumentParser(
        description="God-Tier Solana RPC precheck (P0 LOCK-IN).", parents=[ap0]
    )
    ap.add_argument(
        "--url", action="append", default=[], help="RPC URL (repeatable; may include comma/; list)."
    )

    # Defaults resolved *after* dotenv load:
    ap.add_argument(
        "--timeout",
        type=float,
        default=None,
        help="Per-call timeout seconds (default from CHECK_RPC_TIMEOUT_S).",
    )
    ap.add_argument(
        "--retries",
        type=int,
        default=None,
        help="Retry count per RPC method (default from CHECK_RPC_RETRIES).",
    )

    ap.add_argument(
        "--strict",
        action="store_true",
        help="Strict: require min viability + health (unless --no-health) + progress if enabled.",
    )
    ap.add_argument(
        "--require-health", action="store_true", help="Require getHealth=='ok' even if not strict."
    )
    ap.add_argument(
        "--no-health",
        action="store_true",
        help="Do NOT require getHealth=='ok' (overrides strict default).",
    )
    ap.add_argument(
        "--dns",
        action="store_true",
        help="Run DNS checks (best-effort; strict may promote failures).",
    )

    ap.add_argument("--json", action="store_true", help="Emit JSON summary to STDOUT.")
    ap.add_argument("--json-only", action="store_true", help="Only emit JSON (no human logs).")
    ap.add_argument(
        "--try-all",
        action="store_true",
        help="Try all candidates and pick best by score (instead of first OK).",
    )
    ap.add_argument(
        "--progress",
        action="store_true",
        help="Sample slot twice; strict may require progress depending on env.",
    )
    ap.add_argument(
        "--force", action="store_true", help="Run even if DISABLE_SCANNERS=1 / KILL_SWITCH=1."
    )
    ap.add_argument("--headers-json", default="", help="Override headers via JSON object string.")
    ap.add_argument("--headers-kv", default="", help="Override headers via k=v;... string.")
    ap.add_argument(
        "--use-proxy",
        action="store_true",
        help="Route RPC calls through proxy_cfg() if available (best-effort).",
    )
    ap.add_argument(
        "--unsafe-print-urls",
        action="store_true",
        help="DANGEROUS: print full URLs in JSON/logs (may leak tokens).",
    )
    args = ap.parse_args()

    # resolve env-derived defaults
    timeout_s = max(
        2.0, float(args.timeout if args.timeout is not None else _envf("CHECK_RPC_TIMEOUT_S", 12.0))
    )
    retries = max(
        0, min(5, int(args.retries if args.retries is not None else _envi("CHECK_RPC_RETRIES", 1)))
    )

    strict = bool(args.strict or _envb("CHECK_RPC_STRICT", False))
    progress = bool(args.progress or _envb("CHECK_RPC_PROGRESS", False))
    require_advance = bool(_envb("CHECK_RPC_REQUIRE_ADVANCE", False))
    progress_delay_s = float(_envf("CHECK_RPC_PROGRESS_DELAY_S", 0.4))
    try_all = bool(args.try_all or _envb("CHECK_RPC_TRY_ALL", False))
    dns_check = bool(args.dns or _envb("CHECK_RPC_DNS", False))
    commitment = (_envs("CHECK_RPC_COMMITMENT", "processed") or "processed").strip()

    # health requirement policy:
    require_health_env = _envb("CHECK_RPC_REQUIRE_HEALTH", False)
    require_health = bool(
        require_health_env or args.require_health or (strict and not args.no_health)
    )

    # JSON defaults: ON (preflight-friendly)
    emit_json = bool(
        args.json
        or args.json_only
        or _envb("CHECK_RPC_JSON", True)
        or _envb("CHECK_RPC_JSON_ONLY", False)
    )
    json_only = bool(args.json_only or _envb("CHECK_RPC_JSON_ONLY", False))

    unsafe_urls = bool(args.unsafe_print_urls or _unsafe_print_urls_enabled())

    skip, reason = _should_skip(args)
    if skip:
        out = {
            "ts": _ts(),
            "ok": True,
            "skipped": True,
            "reason": reason,
            "exit_code": RC_OK,
            "dotenv": dotenv_report,
        }
        if emit_json:
            print(json.dumps(out, ensure_ascii=False, default=str))
        _hlog(f"SKIP ({reason})", json_only=json_only)
        return RC_OK

    use_proxy = bool(args.use_proxy or _envb("CHECK_RPC_USE_PROXY", False))
    headers = _parse_headers(args.headers_json, args.headers_kv)
    cands = _candidate_urls([u for u in args.url if u])

    if not cands:
        msg = "No RPC URL found. Set SOL_RPC_URL (or RPC_URL) or pass --url."
        out = {
            "ts": _ts(),
            "ok": False,
            "exit_code": RC_CONNECT_FAIL,
            "fatals": [
                {
                    "code": "missing_rpc_url",
                    "detail": msg,
                    "hint": "Set SOL_RPC_URL=https://... (http/s JSON-RPC endpoint).",
                }
            ],
            "dotenv": dotenv_report,
        }
        if emit_json:
            print(json.dumps(out, ensure_ascii=False, default=str))
        _hlog("FAIL " + msg, json_only=json_only)
        return RC_CONNECT_FAIL

    tr, tr_meta = await _make_transport(timeout_s=timeout_s, use_proxy=use_proxy)

    _hlog(
        "BOOT "
        f"strict={int(strict)} require_health={int(require_health)} progress={int(progress)} require_advance={int(require_advance)} "
        f"dns={int(dns_check)} commitment={commitment} timeout_s={timeout_s:.1f} retries={retries} try_all={int(try_all)} use_proxy={int(use_proxy)} "
        f"transport={tr_meta.get('transport')} repo_root={_REPO_ROOT} headers={_sanitize_headers(headers)} candidates={[lbl for lbl, _ in cands]} "
        f"dotenv_enabled={dotenv_report.get('enabled')} dotenv_set_vars={dotenv_report.get('set_vars')}",
        json_only=json_only,
    )

    attempts: list[Attempt] = []
    any_bad_url = False

    # attempt-level watchdog to prevent a wedged transport from hanging forever
    attempt_watchdog = max(8.0, timeout_s * (6.0 + 1.5 * retries))

    # parallel probing settings
    conc = max(1, min(12, int(_envi("CHECK_RPC_CONCURRENCY", 4))))
    sem = asyncio.Semaphore(conc)

    async def _run_one(lbl: str, url: str) -> Attempt:
        async with sem:
            try:
                return await asyncio.wait_for(
                    _probe_one(
                        tr=tr,
                        label=lbl,
                        url=url,
                        strict=strict,
                        require_health=require_health,
                        progress=progress,
                        progress_delay_s=progress_delay_s,
                        require_advance=require_advance,
                        dns_check=dns_check,
                        headers=headers,
                        retries=retries,
                        commitment=commitment,
                    ),
                    timeout=attempt_watchdog,
                )
            except TimeoutError:
                host = _host_from_url(url)
                host_res = int(bool(host and _resolves(host))) if dns_check else 0
                a = Attempt(
                    label=lbl,
                    url=_normalize_url(url),
                    url_masked=_redact_url(url),
                    host=host,
                    host_resolves=host_res,
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
                    slot_progress={
                        "enabled": int(bool(progress)),
                        "ok": 0,
                        "from": None,
                        "to": None,
                        "err": "timeout",
                    },
                    latest_blockhash={
                        "ok": 0,
                        "blockhash": None,
                        "lastValidBlockHeight": None,
                        "err": "timeout",
                        "http_status": 0,
                    },
                    score=-999.0,
                )
                a.score = _score_attempt(a, strict=strict, progress=progress)
                return a

    try:
        if try_all:
            tasks = [asyncio.create_task(_run_one(lbl, url)) for (lbl, url) in cands]
            for t in asyncio.as_completed(tasks):
                a = await t
                any_bad_url = (
                    any_bad_url
                    or ("bad_url_format" in (a.error or ""))
                    or ("INVALID" in (a.url_masked or ""))
                )
                attempts.append(a)
        else:
            # sequential, early-exit
            for lbl, url in cands:
                _hlog(f"TRY {lbl} url={_redact_url(url)}", json_only=json_only)
                a = await _run_one(lbl, url)
                any_bad_url = (
                    any_bad_url
                    or ("bad_url_format" in (a.error or ""))
                    or ("INVALID" in (a.url_masked or ""))
                )
                attempts.append(a)

                if not json_only:
                    _hlog(
                        f"RES {lbl} ok_min={int(a.ok_min)} ok_strict={int(a.ok_strict)} "
                        f"health={(a.health.get('val') or '-')} slot={(a.slot.get('val') or '-')} "
                        f"blockhash={'yes' if a.latest_blockhash.get('blockhash') else 'no'} "
                        f"p50_ms={(a.lat.get('p50_ms') or '-')} elapsed_ms={a.elapsed_ms} score={a.score:.3f}",
                        json_only=False,
                    )
                    for w in (a.warnings or [])[:10]:
                        _hlog(f"WARN {lbl} {w}", json_only=False)
                    if a.error:
                        _hlog(f"ERR  {lbl} {a.error}", json_only=False)

                if strict and a.ok_min and a.ok_strict:
                    break
                if (not strict) and a.ok_min:
                    break
    finally:
        with contextlib.suppress(Exception):
            await tr.aclose()

    picked: Attempt | None = None
    if attempts:
        if strict:
            strict_ok = [x for x in attempts if x.ok_min and x.ok_strict]
            picked = max(strict_ok, key=lambda x: x.score) if strict_ok else None
        if picked is None:
            min_ok = [x for x in attempts if x.ok_min]
            picked = max(min_ok, key=lambda x: x.score) if min_ok else None

    ok = bool(picked is not None)
    ordered = sorted(attempts, key=lambda x: x.score, reverse=True)
    recommended_urls = [(a.url if unsafe_urls else a.url_masked) for a in ordered[:10]]

    # warnings/fatals in a more "preflight-like" contract
    fatals: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    if not ok:
        if not attempts:
            fatals.append(
                {
                    "code": "no_attempts",
                    "detail": "No candidates were probed.",
                    "hint": "Provide SOL_RPC_URL or --url.",
                }
            )
        else:
            fatals.append(
                {
                    "code": "rpc_not_viable",
                    "detail": "No RPC endpoint met minimum viability (blockhash + slot).",
                    "hint": "Use a paid/dedicated endpoint and/or set multiple SOL_RPC_URLS for failover.",
                }
            )

    for a in ordered[:10]:
        if a.http_notes:
            warnings.append(
                {"code": "http_notes", "label": a.label, "detail": ",".join(a.http_notes[:5])}
            )
        if a.warnings:
            warnings.append(
                {"code": "rpc_warnings", "label": a.label, "detail": ",".join(a.warnings[:5])}
            )

    summary: dict[str, Any] = {
        "ts": _ts(),
        "ok": ok,
        "exit_code": (RC_OK if ok else RC_REQUIREMENTS_FAIL),
        "fatals": fatals,
        "warnings": warnings[:30],
        "picked": (picked.public(unsafe_urls=unsafe_urls) if picked else None),
        "recommended_SOL_RPC_URLS_order": recommended_urls,
        "results": [a.public(unsafe_urls=unsafe_urls) for a in ordered],
        "fix_hints": _fix_hints(ordered, strict=strict, progress=progress),
        "dotenv": dotenv_report,
        "info": {
            "runtime": {
                "python": sys.version.split()[0],
                "platform": sys.platform,
                "os_name": os.name,
                "pid": os.getpid(),
                "cwd": os.getcwd(),
                "repo_root": _REPO_ROOT,
            },
            "policy": {
                "strict": int(strict),
                "require_health": int(require_health),
                "progress": int(progress),
                "require_advance": int(require_advance),
                "dns_check": int(dns_check),
                "commitment": commitment,
                "timeout_s": float(timeout_s),
                "retries": int(retries),
                "try_all": int(try_all),
                "concurrency": int(conc),
            },
            "transport": tr_meta,
            "headers_used": _sanitize_headers(headers),
            "env": {
                "SOL_RPC_URL_set": int(bool(_envs("SOL_RPC_URL", ""))),
                "RPC_URL_set": int(bool(_envs("RPC_URL", ""))),
                "SOL_RPC_URLS_count": len(_split_csv(_envs("SOL_RPC_URLS", ""))),
                "RPC_FAILOVER_knobs": _read_failover_knobs(),
            },
            "unsafe_print_urls": int(bool(unsafe_urls)),
        },
    }

    if emit_json:
        print(json.dumps(_safe_json(summary), ensure_ascii=False, default=str))

    if ok:
        _hlog("SUMMARY ok=1 (rpc viable)", json_only=json_only)
        return RC_OK

    if (
        any_bad_url
        and attempts
        and all(
            ("bad_url_format" in (a.error or "")) or ("INVALID" in (a.url_masked or ""))
            for a in attempts
        )
    ):
        _hlog("SUMMARY ok=0 (all candidates bad url format)", json_only=json_only)
        return RC_BAD_URL

    if attempts and all((a.error == "attempt_timeout") for a in attempts):
        _hlog("SUMMARY ok=0 (all attempts timed out)", json_only=json_only)
        return RC_TIMEOUT

    any_responded = any(
        (a.health.get("http_status") or 0)
        or (a.slot.get("http_status") or 0)
        or (a.latest_blockhash.get("http_status") or 0)
        for a in attempts
    )
    if any_responded:
        _hlog("SUMMARY ok=0 (rpc responded but requirements failed)", json_only=json_only)
        return RC_REQUIREMENTS_FAIL

    _hlog("SUMMARY ok=0 (connect/respond failure)", json_only=json_only)
    return RC_CONNECT_FAIL


def main() -> None:
    # Windows-friendly event loop policy
    with contextlib.suppress(Exception):
        if os.name == "nt" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]

    try:
        rc = asyncio.run(_amain())
    except KeyboardInterrupt:
        rc = 130
    except Exception as e:
        _safe_stderr(f"[check_rpc] {_ts()} FAIL unhandled {type(e).__name__}: {e}")
        rc = 1
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
