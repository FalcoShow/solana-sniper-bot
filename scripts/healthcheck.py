# -*- coding: utf-8 -*-
# scripts/healthcheck.py
from __future__ import annotations

"""
scripts/healthcheck.py — Canonical Healthcheck (P0 LOCK-IN)
===========================================================

Goal
----
Answer ONE question deterministically and fast:
"Is this deployment READY (to trade)?"

Two modes
---------
(A) HTTP mode (preferred in production)
    - Calls local health server endpoint (default /readyz from scripts/health.py)

(B) Local probe mode (fallback / preflight)
    - Runs lightweight internal probes (imports + rpc + router + optional redis/db/jup)

Critical fix vs prior timeouts
------------------------------
This client is *hard-bounded* by:
- a small HTTP budget in auto mode (fast fail -> local)
- a hard overall budget for local probes
- per-probe timeouts that never exceed remaining budget

Skip / disable
--------------
Supports common skip toggles used by preflight:
- SKIP_PREFLIGHT_HEALTHCHECK=1   -> exit 0 (skipped)
- PREFLIGHT_SKIP_HEALTHCHECK=1   -> exit 0 (skipped)
- SKIP_HEALTHCHECK=1 / HEALTHCHECK_DISABLED=1 -> exit 0 (skipped)

Exit codes
----------
0  Ready (or skipped)
1  Not ready
2  Misconfigured (bad args / invalid mode)
130 Ctrl-C

CLI
---
py scripts/healthcheck.py --json-only
py scripts/healthcheck.py --mode http --url http://127.0.0.1:8080/readyz --json-only
py scripts/healthcheck.py --mode local --strict --json-only
"""

import argparse
import asyncio
import contextlib
import inspect
import json
import os
import platform
import re
import socket
import sys
import time
import traceback
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

# =============================================================================
# exit codes
# =============================================================================
RC_READY = 0
RC_NOT_READY = 1
RC_MISCONFIG = 2
RC_CTRL_C = 130

_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}


# =============================================================================
# repo-root bootstrap (reliable for `python scripts/healthcheck.py`)
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

    root_s = str(root)
    if root_s and root_s not in sys.path:
        sys.path.insert(0, root_s)
    os.environ.setdefault("REPO_ROOT", root_s)
    return root_s


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


def _envi(k: str, d: int) -> int:
    try:
        return int(float(_envs(k, str(d))))
    except Exception:
        return int(d)


def _envf(k: str, d: float) -> float:
    try:
        return float(_envs(k, str(d)))
    except Exception:
        return float(d)


def _parse_csv(s: str) -> list[str]:
    if not s:
        return []
    parts = [p.strip() for p in s.replace(";", ",").split(",")]
    return [p for p in parts if p]


def _ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _render_detected() -> bool:
    return bool(
        _envs("RENDER_SERVICE_ID", "")
        or _envs("RENDER_INSTANCE_ID", "")
        or (_envs("RENDER", "").lower() in _TRUE)
    )


def _skip_dsn_required() -> bool:
    return _envb("SKIP_DSN_REQUIRED", False)


def _has_any_dsn() -> bool:
    keys = (
        "DATABASE_URL",
        "POSTGRES_DSN",
        "PG_DSN",
        "PNL_DSN",
        "PG_DSN_INTERNAL",
        "PG_DSN_EXTERNAL",
    )
    return any(bool(_envs(k, "")) for k in keys)


def _has_any_redis() -> bool:
    return bool(_envs("REDIS_URL", "") or _envs("REDIS_TLS_URL", ""))


def _maybe_load_dotenv() -> None:
    if _envb("HEALTHCHECK_NO_DOTENV", False):
        return
    with contextlib.suppress(Exception):
        from dotenv import load_dotenv  # type: ignore

        repo = _envs("REPO_ROOT", "")
        if repo:
            for nm in (".env.extra", ".env"):
                p = Path(repo) / nm
                if p.exists():
                    load_dotenv(str(p), override=False)
        load_dotenv(override=False)


def _should_skip_entirely() -> tuple[bool, str]:
    # Common preflight toggles (you already used SKIP_PREFLIGHT_HEALTHCHECK=1)
    for k in (
        "SKIP_PREFLIGHT_HEALTHCHECK",
        "PREFLIGHT_SKIP_HEALTHCHECK",
        "SKIP_HEALTHCHECK",
        "HEALTHCHECK_DISABLED",
    ):
        if _envb(k, False):
            return True, f"{k}=1"
    return False, ""


# =============================================================================
# safe output
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
    except Exception:
        pass


def _hlog(msg: str, *, json_only: bool) -> None:
    if json_only:
        return
    _safe_write(sys.stderr, f"[healthcheck] {_ts()} {msg}\n")


# =============================================================================
# redaction / safety
# =============================================================================
_URL_CRED_RE = re.compile(r"(://)([^/@\s]+)@")
_LONG_TOKENISH_RE = re.compile(r"\b[A-Za-z0-9_\-]{28,}\b")
_KV_SECRET_RE = re.compile(
    r"(?i)\b(api[_-]?key|token|secret|password|pass)\b(\s*[:=]\s*)([^\s,;]+)"
)


def _redact_keys() -> list[str]:
    base = [
        "OPENAI_API_KEY",
        "TELEGRAM_BOT_TOKEN",
        "DISCORD_TOKEN",
        "DISCORD_WEBHOOK_URL",
        "NOTION_TOKEN",
        "NOTION_API_KEY",
        "WALLET_JSON",
        "WALLET_JSON_B64",
        "WALLET_PRIVATE_KEY",
        "KEYPAIR_B64",
        "KEYPAIR_PATH",
        "DATABASE_URL",
        "PG_DSN",
        "PG_DSN_INTERNAL",
        "PG_DSN_EXTERNAL",
        "POSTGRES_DSN",
        "PNL_DSN",
        "REDIS_URL",
        "REDIS_TLS_URL",
        "SOL_RPC_URL",
        "RPC_URL",
        "SOL_RPC_URLS",
        "RPC_URLS",
        "JUP_PRO_KEY",
        "JUP_API_KEY",
        "JUP_KEY",
    ]
    extra = _parse_csv(_envs("HEALTHCHECK_REDACT_KEYS", ""))
    out = base + extra
    seen: set[str] = set()
    uniq: list[str] = []
    for k in out:
        if k and k not in seen:
            uniq.append(k)
            seen.add(k)
    return uniq


def _sanitize(x: Any) -> str:
    if x is None:
        return ""
    s = str(x)
    s = _URL_CRED_RE.sub(r"\1***@", s)
    s = _KV_SECRET_RE.sub(r"\1\2***", s)
    for k in _redact_keys():
        v = os.getenv(k, "")
        if v and len(v) >= 6:
            s = s.replace(v, "***")
    if _envb("HEALTHCHECK_MASK_LONG_TOKENS", True):
        s = _LONG_TOKENISH_RE.sub("***", s)
    return s


def _mask_url(u: str) -> str:
    if not u:
        return ""
    out = _URL_CRED_RE.sub(r"\1***@", str(u))
    out = _KV_SECRET_RE.sub(r"\1\2***", out)
    if "?" in out:
        out = out.split("?", 1)[0] + "?..."
    return out


def _safe_jsonable(x: Any) -> Any:
    try:
        json.dumps(x, default=str)
        return x
    except Exception:
        return str(x)


def _short_exc(e: BaseException, n: int = 360) -> str:
    s = _sanitize(f"{type(e).__name__}: {e}")
    return (s[:n] + "…") if len(s) > n else s


# =============================================================================
# defensive import + signature tolerant calls
# =============================================================================
def _try_import(name: str) -> Any:
    with contextlib.suppress(Exception):
        return __import__(name, fromlist=["*"])
    return None


def _filter_kwargs(fn: Any, kwargs: dict[str, Any]) -> dict[str, Any]:
    try:
        sig = inspect.signature(fn)
        params = sig.parameters.values()
        if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params):
            return kwargs
        allowed = {p.name for p in params}
        return {k: v for k, v in kwargs.items() if k in allowed}
    except Exception:
        return kwargs


async def _maybe_await(v: Any) -> Any:
    return await v if inspect.isawaitable(v) else v


# =============================================================================
# models
# =============================================================================
@dataclass
class ProbeResult:
    key: str
    name: str
    ok: bool
    fatal: bool
    skipped: bool = False
    error: str | None = None
    details: dict[str, Any] = field(default_factory=dict)
    duration_ms: int = 0


@dataclass(frozen=True)
class ProbeSpec:
    key: str
    name: str
    fn: Callable[[float], Awaitable[dict[str, Any]]]
    skip_env: str
    default_enabled: bool = True
    default_fatal: bool = True
    tags: tuple[str, ...] = ()


# =============================================================================
# HTTP health client (preferred)
# =============================================================================
async def _http_get(url: str, timeout_s: float) -> tuple[int, Any, str]:
    """
    Returns (status_code, parsed_json_or_text, content_type).
    Prefers httpx if available, else urllib (threaded).
    """
    url = str(url).strip()
    if not url:
        raise RuntimeError("empty url")

    with contextlib.suppress(Exception):
        import httpx  # type: ignore

        async with httpx.AsyncClient(timeout=timeout_s) as c:
            r = await c.get(url, headers={"accept": "application/json"})
            ctype = (r.headers.get("content-type") or "").lower()
            if "application/json" in ctype:
                return int(r.status_code), r.json(), ctype
            return int(r.status_code), r.text, ctype

    import urllib.request

    def _do() -> tuple[int, Any, str]:
        req = urllib.request.Request(url, headers={"accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            code = int(getattr(resp, "status", 200))
            ctype = (resp.headers.get("content-type") or "").lower()
            body = resp.read().decode("utf-8", "ignore")
            if "application/json" in ctype:
                with contextlib.suppress(Exception):
                    return code, json.loads(body), ctype
            return code, body, ctype

    return await asyncio.to_thread(_do)


def _default_health_url() -> str:
    u = _envs("HEALTHCHECK_URL", "").strip()
    if u:
        return u

    port = _envi("PORT", 8080)
    hp = _envs("HEALTH_PORT", "").strip()
    if hp:
        with contextlib.suppress(Exception):
            port = int(hp)

    path = _envs("HEALTHCHECK_HTTP_PATH", "/readyz").strip() or "/readyz"
    if not path.startswith("/"):
        path = "/" + path

    return f"http://127.0.0.1:{port}{path}"


def _interpret_ready(payload: Any) -> tuple[bool, str]:
    if isinstance(payload, dict):
        ok = payload.get("ok")
        ready = payload.get("ready")
        status = payload.get("status")
        why = payload.get("why") or payload.get("reason") or payload.get("message") or ""
        if (
            ok is True
            or ready is True
            or (isinstance(status, str) and status.lower() in ("ok", "ready", "healthy"))
        ):
            return True, str(why or "ok")
        if ok is False or ready is False:
            return False, str(why or "not_ready")
        return False, f"unrecognized_payload_keys={sorted(payload.keys())[:12]}"
    if isinstance(payload, str):
        s = payload.strip().lower()
        if s in ("ok", "ready") or ("ready" in s and "not" not in s):
            return True, "text_ok"
        return False, "text_not_ready"
    return False, "unknown_payload_type"


async def _tcp_probe(host: str, port: int, timeout_s: float) -> bool:
    """
    Fast loopback port probe so auto-mode can fall back immediately when server isn't running.
    """
    try:
        return await asyncio.wait_for(
            _tcp_probe_inner(host, port), timeout=max(0.05, float(timeout_s))
        )
    except Exception:
        return False


async def _tcp_probe_inner(host: str, port: int) -> bool:
    reader = writer = None
    try:
        reader, writer = await asyncio.open_connection(host, port)
        return True
    finally:
        with contextlib.suppress(Exception):
            if writer is not None:
                writer.close()
                with contextlib.suppress(Exception):
                    await writer.wait_closed()


def _parse_host_port_from_url(url: str) -> tuple[str, int] | None:
    # Only for simple http://host:port/path patterns (loopback probe helper)
    try:
        if "://" not in url:
            return None
        rest = url.split("://", 1)[1]
        hostport = rest.split("/", 1)[0]
        if ":" in hostport:
            h, p = hostport.rsplit(":", 1)
            return h.strip() or "127.0.0.1", int(p)
        return hostport.strip(), 80
    except Exception:
        return None


async def run_http_healthcheck(
    *,
    url: str,
    retries: int,
    retry_delay_s: float,
    timeout_s: float,
    json_only: bool,
    verbose: bool,
    auto_mode: bool,
) -> dict[str, Any]:
    url = str(url).strip()
    retries = max(1, int(retries))
    retry_delay_s = max(0.01, float(retry_delay_s))
    timeout_s = max(0.2, float(timeout_s))

    # In auto mode, keep HTTP budget small unless explicitly overridden
    # so we never blow preflight timeouts if the server isn't running.
    if auto_mode and os.getenv("HEALTHCHECK_RETRIES") is None and retries > 3:
        retries = 2
    if auto_mode and os.getenv("HEALTHCHECK_HTTP_TIMEOUT_S") is None and timeout_s > 2.0:
        timeout_s = 1.2
    if auto_mode and os.getenv("HEALTHCHECK_RETRY_DELAY_S") is None and retry_delay_s > 0.5:
        retry_delay_s = 0.15

    # Fast port probe on loopback: if closed, skip retry loop immediately
    hp = _parse_host_port_from_url(url)
    if auto_mode and hp and hp[0] in ("127.0.0.1", "localhost"):
        if not await _tcp_probe(hp[0], hp[1], timeout_s=0.15):
            return {"ok": False, "url": _mask_url(url), "error": "loopback_port_closed"}

    last: dict[str, Any] = {}
    for i in range(retries):
        try:
            code, data, ctype = await _http_get(url, timeout_s=timeout_s)
            ready, why = _interpret_ready(data)
            last = {
                "attempt": i + 1,
                "http_status": int(code),
                "content_type": str(ctype),
                "ready": bool(ready),
                "why": _sanitize(str(why)),
            }
            if int(code) == 200 and ready:
                return {"ok": True, "url": _mask_url(url), **last}

            if verbose:
                _hlog(f"http: not ready (status={code}) why={_sanitize(why)}", json_only=json_only)
        except Exception as e:
            last = {"attempt": i + 1, "error": _sanitize(_short_exc(e))}
            if verbose:
                _hlog(f"http: error {last['error']}", json_only=json_only)

        if i + 1 < retries:
            await asyncio.sleep(retry_delay_s)

    return {"ok": False, "url": _mask_url(url), **last}


# =============================================================================
# Local probes
# =============================================================================
async def _probe_env(timeout_s: float) -> dict[str, Any]:
    _ = timeout_s
    rpc_keys = ["RPC_URL", "SOL_RPC_URL", "RPC_URLS", "SOL_RPC_URLS", "SOLANA_RPC_URLS"]
    rpc_present = any(bool(_envs(k, "")) for k in rpc_keys)

    warnings: list[str] = []
    for k in ("PAUSE_TRADING", "DRYRUN", "DRY_RUN", "KILL_SWITCH", "DISABLE_BUYS", "DISABLE_SELLS"):
        if _envb(k, False):
            warnings.append(f"{k}=1")

    return {
        "render": _render_detected(),
        "service": _envs("SERVICE_NAME", _envs("APP_NAME", "sniper")),
        "SKIP_DSN_REQUIRED": int(_skip_dsn_required()),
        "has_any_dsn": int(_has_any_dsn()),
        "has_any_redis": int(_has_any_redis()),
        "rpc_present": int(rpc_present),
        "warnings": warnings[:12],
    }


def _as_listish(v: Any) -> list[str]:
    if v is None:
        return []
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return []
        # allow csv strings too
        if "," in s or ";" in s:
            return _parse_csv(s)
        return [s]
    if isinstance(v, (list, tuple, set)):
        out: list[str] = []
        for x in v:
            out.extend(_as_listish(x))
        return out
    return [str(v).strip()]


def _candidate_rpcs_from_config() -> list[str]:
    """
    Best-effort extraction of RPC candidates from config.C without assuming
    exact attribute names (keeps healthcheck resilient across config refactors).
    """
    mod = _try_import("config")
    if mod is None:
        return []

    C = getattr(mod, "C", None)
    if C is None:
        return []

    candidates: list[str] = []

    def _pull(obj: Any) -> None:
        if obj is None:
            return

        # common string attrs
        for nm in (
            "RPC_URL",
            "SOL_RPC_URL",
            "rpc_url",
            "sol_rpc_url",
            "rpc",
            "url",
            "primary_rpc",
            "primary",
        ):
            with contextlib.suppress(Exception):
                candidates.extend(_as_listish(getattr(obj, nm, None)))

        # common list attrs
        for nm in (
            "RPC_URLS",
            "SOL_RPC_URLS",
            "SOLANA_RPC_URLS",
            "rpc_urls",
            "sol_rpc_urls",
            "urls",
            "candidates",
            "rpc_candidates",
        ):
            with contextlib.suppress(Exception):
                candidates.extend(_as_listish(getattr(obj, nm, None)))

    # try C plus likely nested namespaces if you use them
    _pull(C)
    for nested in ("rpc", "solana", "network", "chains", "runtime"):
        with contextlib.suppress(Exception):
            _pull(getattr(C, nested, None))

    # normalize + dedupe
    uniq: list[str] = []
    seen: set[str] = set()
    for u in candidates:
        u = (u or "").strip()
        if not u:
            continue
        if u not in seen:
            uniq.append(u)
            seen.add(u)

    return uniq


def _candidate_rpcs() -> list[str]:
    out: list[str] = []

    # env first (highest priority)
    for k in ("RPC_URL", "SOL_RPC_URL"):
        v = _envs(k, "")
        if v:
            out.extend(_as_listish(v))

    for k in ("RPC_URLS", "SOL_RPC_URLS", "SOLANA_RPC_URLS"):
        out.extend(_parse_csv(_envs(k, "")))

    # config fallback (critical for preflight where env may not include RPC_URLS)
    out.extend(_candidate_rpcs_from_config())

    # dedupe
    seen: set[str] = set()
    uniq: list[str] = []
    for u in out:
        u = (u or "").strip()
        if not u:
            continue
        if u not in seen:
            uniq.append(u)
            seen.add(u)

    return uniq


async def _rpc_post(url: str, payload: dict[str, Any], timeout_s: float) -> dict[str, Any]:
    url = str(url).strip()
    if not url:
        raise RuntimeError("empty rpc url")

    with contextlib.suppress(Exception):
        import httpx  # type: ignore

        async with httpx.AsyncClient(timeout=timeout_s) as c:
            r = await c.post(url, json=payload, headers={"content-type": "application/json"})
            if int(r.status_code) != 200:
                raise RuntimeError(f"rpc status={r.status_code} body={r.text[:180]!r}")
            return r.json()

    import urllib.request

    def _do() -> dict[str, Any]:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url, data=data, headers={"content-type": "application/json"}, method="POST"
        )
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            body = resp.read().decode("utf-8", "ignore")
            return json.loads(body)

    return await asyncio.to_thread(_do)


async def _probe_rpc_select(timeout_s: float) -> dict[str, Any]:
    rf = _try_import("utils_pkg.rpc_failover")
    if rf is not None:
        fn = getattr(rf, "apply_failover_to_env", None)
        if callable(fn):
            kw = _filter_kwargs(fn, {"timeout_s": timeout_s, "timeout": timeout_s})
            v = await asyncio.wait_for(_maybe_await(fn(**kw)), timeout=max(1.0, timeout_s))
            chosen = _envs("RPC_URL", "") or _envs("SOL_RPC_URL", "")
            if chosen:
                os.environ["RPC_URL"] = chosen
                os.environ["SOL_RPC_URL"] = chosen
            return {
                "via": "utils_pkg.rpc_failover.apply_failover_to_env",
                "chosen_rpc": _mask_url(chosen)[:180],
                "ret": _safe_jsonable(v),
            }

    cands = _candidate_rpcs()
    if not cands:
        raise RuntimeError("no RPC candidates (set RPC_URL/SOL_RPC_URL or RPC_URLS/SOL_RPC_URLS)")

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getLatestBlockhash",
        "params": [{"commitment": "processed"}],
    }
    last_err: str | None = None

    for u in cands[:16]:
        try:
            data = await asyncio.wait_for(
                _rpc_post(u, payload, timeout_s=timeout_s), timeout=max(1.0, timeout_s + 0.5)
            )
            bh = data.get("result", {}).get("value", {}).get("blockhash")
            if not bh:
                continue
            os.environ["RPC_URL"] = u
            os.environ["SOL_RPC_URL"] = u
            return {
                "via": "internal_first_reachable",
                "chosen_rpc": _mask_url(u)[:180],
                "blockhash": str(bh),
            }
        except Exception as e:
            last_err = _short_exc(e)
            continue

    raise RuntimeError(f"no reachable RPC from {len(cands)} candidates; last_err={last_err}")


async def _probe_core_import(timeout_s: float) -> dict[str, Any]:
    _ = timeout_s
    core = _try_import("utils_pkg.core_utils")
    if core is None:
        raise RuntimeError("utils_pkg.core_utils not importable")
    return {"imported": True}


async def _probe_router(timeout_s: float) -> dict[str, Any]:
    _ = timeout_s
    router = _try_import("utils_pkg.router")
    if router is None:
        raise RuntimeError("utils_pkg.router not importable")

    for nm in ("get_quote", "get_buy_quote", "quote"):
        if callable(getattr(router, nm, None)):
            return {"imported": True, "has_quote": nm}

    raise RuntimeError("utils_pkg.router has no quote fn (expected get_quote/get_buy_quote/quote)")


async def _probe_redis(timeout_s: float) -> dict[str, Any]:
    # If redis isn't configured and not explicitly required, treat as skipped (not a failure).
    if not _has_any_redis() and not _envb("HEALTHCHECK_FORCE_REDIS", False):
        return {
            "skipped": True,
            "reason": "no REDIS_URL/REDIS_TLS_URL (set HEALTHCHECK_FORCE_REDIS=1 to require)",
        }

    core = _try_import("utils_pkg.core_utils")
    r = None
    if core is not None:
        fn = getattr(core, "get_redis_async", None)
        if callable(fn):
            r = await asyncio.wait_for(_maybe_await(fn()), timeout=max(1.0, timeout_s))

    if r is not None and hasattr(r, "ping"):
        pong = await asyncio.wait_for(_maybe_await(r.ping()), timeout=max(1.0, timeout_s))
        return {"via": "core_utils.get_redis_async", "ping": bool(pong)}

    redis_url = _envs("REDIS_URL", "") or _envs("REDIS_TLS_URL", "")
    if not redis_url:
        return {"skipped": True, "reason": "redis not configured"}

    with contextlib.suppress(Exception):
        import redis.asyncio as aioredis  # type: ignore

        rr = aioredis.from_url(redis_url, decode_responses=True)
        pong = await asyncio.wait_for(rr.ping(), timeout=max(1.0, timeout_s))
        with contextlib.suppress(Exception):
            await rr.close()
        return {
            "via": "redis.asyncio.from_url",
            "ping": bool(pong),
            "redis_url": _mask_url(redis_url)[:180],
        }

    raise RuntimeError(
        "redis client not installed (pip install redis) or set HEALTHCHECK_SKIP_REDIS=1"
    )


async def _probe_solana(timeout_s: float) -> dict[str, Any]:
    rpc = _envs("RPC_URL", "") or _envs("SOL_RPC_URL", "")
    if not rpc:
        raise RuntimeError("RPC_URL/SOL_RPC_URL not set (rpc_select should run first)")

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getLatestBlockhash",
        "params": [{"commitment": "processed"}],
    }
    data = await asyncio.wait_for(
        _rpc_post(rpc, payload, timeout_s=timeout_s), timeout=max(1.0, timeout_s + 0.5)
    )
    bh = data.get("result", {}).get("value", {}).get("blockhash")
    if not bh:
        raise RuntimeError("could not parse getLatestBlockhash response")
    return {"rpc": _mask_url(rpc)[:180], "blockhash": str(bh)}


async def _probe_db(timeout_s: float) -> dict[str, Any]:
    if _skip_dsn_required():
        return {"skipped": True, "reason": "SKIP_DSN_REQUIRED=1"}

    if not _has_any_dsn() and not _envb("HEALTHCHECK_FORCE_DB", False):
        return {
            "skipped": True,
            "reason": "no DSN configured (set HEALTHCHECK_FORCE_DB=1 to require)",
        }

    if not _has_any_dsn():
        raise RuntimeError("no Postgres DSN set (DATABASE_URL/POSTGRES_DSN/PG_DSN/PNL_DSN...)")

    ai_db = _try_import("utils_pkg.ai_db")
    if ai_db is None:
        raise RuntimeError("utils_pkg.ai_db not importable")

    for nm in ("ping_async", "ping", "healthcheck", "check"):
        fn = getattr(ai_db, nm, None)
        if not callable(fn):
            continue
        kw = _filter_kwargs(fn, {"timeout_s": timeout_s, "timeout": timeout_s})
        v = await asyncio.wait_for(_maybe_await(fn(**kw)), timeout=max(1.0, timeout_s))
        if isinstance(v, bool) and not v:
            raise RuntimeError(f"{nm} returned False")
        return {"via": f"ai_db.{nm}", "ok": True}

    ensure = getattr(ai_db, "ensure_schema", None)
    if callable(ensure):
        kw = _filter_kwargs(ensure, {"timeout_s": timeout_s, "timeout": timeout_s})
        await asyncio.wait_for(_maybe_await(ensure(**kw)), timeout=max(1.0, timeout_s))
        return {"via": "ai_db.ensure_schema", "ok": True}

    raise RuntimeError(
        "utils_pkg.ai_db has no ping/check function (expected ping_async/ping/healthcheck/check)"
    )


async def _probe_dns(timeout_s: float) -> dict[str, Any]:
    jup = (
        _envs("JUP_BASE_URL", "") or _envs("JUPITER_SWAP_API", "") or "https://api.jup.ag"
    ).strip()
    host = ""
    with contextlib.suppress(Exception):
        host = jup.split("://", 1)[1].split("/", 1)[0].split(":", 1)[0].strip()
    if not host:
        raise RuntimeError("could not parse host from JUP_BASE_URL/JUPITER_SWAP_API")

    loop = asyncio.get_running_loop()
    infos = await asyncio.wait_for(loop.getaddrinfo(host, None), timeout=max(0.5, timeout_s))
    ips: list[str] = []
    for fam, *_rest, sockaddr in infos:
        if fam in (socket.AF_INET, socket.AF_INET6):
            ip = sockaddr[0]
            if ip not in ips:
                ips.append(ip)
    return {"host": host, "ips": ips[:8]}


async def _probe_data_dir(timeout_s: float) -> dict[str, Any]:
    _ = timeout_s
    data_dir = _envs("DATA_DIR", "").strip()
    if not data_dir:
        data_dir = "/opt/render/data" if _render_detected() else "./data"
    data_dir = os.path.abspath(data_dir)
    os.makedirs(data_dir, exist_ok=True)
    p = os.path.join(data_dir, f".hc_write_{int(time.time())}.tmp")
    with open(p, "w", encoding="utf-8") as f:
        f.write("ok")
    os.remove(p)
    return {"data_dir": data_dir, "write_ok": True}


async def _probe_jup(timeout_s: float) -> dict[str, Any]:
    # Optional / heavy; only run when enabled
    mints = _try_import("utils_pkg.mints")
    if mints is None:
        raise RuntimeError("utils_pkg.mints not importable")

    sol = getattr(mints, "SOL_MINT", None) or _envs("SOL_MINT", "")
    usdc = getattr(mints, "USDC_MINT", None) or _envs("USDC_MINT", "")
    if not sol or not usdc:
        raise RuntimeError("missing SOL_MINT/USDC_MINT (mints contract)")

    router = _try_import("utils_pkg.router")
    if router is None:
        raise RuntimeError("utils_pkg.router not importable")

    qfn = (
        getattr(router, "get_quote", None)
        or getattr(router, "quote", None)
        or getattr(router, "get_buy_quote", None)
    )
    if not callable(qfn):
        raise RuntimeError("router quote fn missing (get_quote/quote/get_buy_quote)")

    lamports = int(float(_envs("HEALTHCHECK_JUP_AMOUNT_SOL", "0.0025")) * 1_000_000_000)
    slippage_bps = _envi("HEALTHCHECK_JUP_SLIPPAGE_BPS", 200)

    kw = _filter_kwargs(
        qfn,
        {
            "inputMint": sol,
            "outputMint": usdc,
            "amount": lamports,
            "slippageBps": slippage_bps,
            "timeout_s": timeout_s,
            "timeout": timeout_s,
        },
    )
    q = await asyncio.wait_for(_maybe_await(qfn(**kw)), timeout=max(1.0, timeout_s))
    if not isinstance(q, dict):
        raise RuntimeError("quote returned non-dict")
    out_amt = q.get("outAmount") or q.get("out_amount_raw")
    return {
        "via": "router.quote",
        "outAmount": (str(out_amt)[:32] if out_amt is not None else None),
    }


def _build_registry() -> list[ProbeSpec]:
    return [
        ProbeSpec(
            "env",
            "ENV_CONTRACT",
            _probe_env,
            "HEALTHCHECK_SKIP_ENV",
            default_enabled=True,
            default_fatal=True,
            tags=("core",),
        ),
        ProbeSpec(
            "dns",
            "DNS_JUP_BASE",
            _probe_dns,
            "HEALTHCHECK_SKIP_DNS",
            default_enabled=False,
            default_fatal=False,
            tags=("optional",),
        ),
        ProbeSpec(
            "data_dir",
            "DATA_DIR",
            _probe_data_dir,
            "HEALTHCHECK_SKIP_DATA_DIR",
            default_enabled=False,
            default_fatal=False,
            tags=("optional",),
        ),
        ProbeSpec(
            "rpc_select",
            "RPC_SELECT",
            _probe_rpc_select,
            "HEALTHCHECK_SKIP_RPC_SELECT",
            default_enabled=True,
            default_fatal=True,
            tags=("core",),
        ),
        ProbeSpec(
            "core_import",
            "CORE_IMPORT",
            _probe_core_import,
            "HEALTHCHECK_SKIP_CORE_IMPORT",
            default_enabled=True,
            default_fatal=True,
            tags=("core",),
        ),
        ProbeSpec(
            "router",
            "ROUTER_READY",
            _probe_router,
            "HEALTHCHECK_SKIP_ROUTER",
            default_enabled=True,
            default_fatal=True,
            tags=("core",),
        ),
        ProbeSpec(
            "redis",
            "REDIS",
            _probe_redis,
            "HEALTHCHECK_SKIP_REDIS",
            default_enabled=True,
            default_fatal=False,
            tags=("core",),
        ),
        ProbeSpec(
            "solana",
            "SOLANA_RPC",
            _probe_solana,
            "HEALTHCHECK_SKIP_SOLANA",
            default_enabled=True,
            default_fatal=True,
            tags=("core",),
        ),
        ProbeSpec(
            "db",
            "AI_DB",
            _probe_db,
            "HEALTHCHECK_SKIP_DB",
            default_enabled=True,
            default_fatal=False,
            tags=("core",),
        ),
        ProbeSpec(
            "jup",
            "JUPITER_QUOTE",
            _probe_jup,
            "HEALTHCHECK_SKIP_JUP",
            default_enabled=False,
            default_fatal=False,
            tags=("optional", "heavy"),
        ),
    ]


def _probe_enabled(spec: ProbeSpec, probes_only: set[str]) -> bool:
    # explicit list wins
    if probes_only:
        return spec.key in probes_only

    # explicit skip env wins
    if _envb(spec.skip_env, False):
        return False

    # dynamic disables (avoid false negatives + timeouts when integrations are intentionally off)
    if spec.key == "db" and (
        _skip_dsn_required() or (not _has_any_dsn() and not _envb("HEALTHCHECK_FORCE_DB", False))
    ):
        return False
    if spec.key == "redis" and (
        not _has_any_redis() and not _envb("HEALTHCHECK_FORCE_REDIS", False)
    ):
        return False

    if spec.default_enabled:
        return True

    # Optional probes require opt-in
    if _envb("HEALTHCHECK_ENABLE_OPTIONAL", False):
        return True

    # If someone set skip env explicitly to "0" treat it as enabled
    raw = os.getenv(spec.skip_env)
    if raw is None:
        return False
    return not _envb(spec.skip_env, False)


def _probe_fatal(spec: ProbeSpec) -> bool:
    fatal = bool(spec.default_fatal)
    make_fatal = set(_parse_csv(_envs("HEALTHCHECK_FATAL_PROBES", "")))
    make_nonfatal = set(_parse_csv(_envs("HEALTHCHECK_NONFATAL_PROBES", "")))
    if spec.key in make_fatal:
        fatal = True
    if spec.key in make_nonfatal:
        fatal = False
    if spec.key == "db" and (_skip_dsn_required() or not _has_any_dsn()):
        fatal = False
    if spec.key == "redis" and not _has_any_redis():
        fatal = False
    return bool(fatal)


async def _run_probe(
    spec: ProbeSpec,
    *,
    enabled: bool,
    fatal: bool,
    timeout_s: float,
    json_only: bool,
    verbose: bool,
) -> ProbeResult:
    if not enabled:
        if verbose:
            _hlog(f"{spec.name}: skipped", json_only=json_only)
        return ProbeResult(
            key=spec.key,
            name=spec.name,
            ok=True,
            fatal=fatal,
            skipped=True,
            details={"skipped": True},
        )

    t0 = time.monotonic()
    try:
        details = await asyncio.wait_for(spec.fn(timeout_s), timeout=max(0.2, timeout_s))
        dt = int((time.monotonic() - t0) * 1000)
        if verbose:
            _hlog(f"{spec.name}: OK ({dt}ms)", json_only=json_only)
        return ProbeResult(
            key=spec.key,
            name=spec.name,
            ok=True,
            fatal=fatal,
            details={k: _safe_jsonable(v) for k, v in (details or {}).items()},
            duration_ms=dt,
        )
    except Exception as e:
        dt = int((time.monotonic() - t0) * 1000)
        msg = _sanitize(_short_exc(e))
        if verbose:
            _hlog(f"{spec.name}: FAIL ({dt}ms) err={msg}", json_only=json_only)
        return ProbeResult(
            key=spec.key,
            name=spec.name,
            ok=False,
            fatal=fatal,
            error=msg,
            details={},
            duration_ms=dt,
        )


async def _best_effort_close(timeout_s: float) -> dict[str, Any]:
    out: dict[str, Any] = {}

    core = _try_import("utils_pkg.core_utils")
    if core is not None:
        for nm in ("close_resources", "close", "shutdown", "teardown"):
            fn = getattr(core, nm, None)
            if callable(fn):
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(_maybe_await(fn()), timeout=max(0.2, timeout_s))
                out["core_close"] = nm
                break

    rf = _try_import("utils_pkg.rpc_failover")
    if rf is not None:
        fn = getattr(rf, "close_http", None) or getattr(rf, "close", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                await asyncio.wait_for(_maybe_await(fn()), timeout=max(0.2, timeout_s))
            out["rpc_failover_close"] = True

    router = _try_import("utils_pkg.router")
    if router is not None:
        fn = getattr(router, "close_http", None) or getattr(router, "close", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                await asyncio.wait_for(_maybe_await(fn()), timeout=max(0.2, timeout_s))
            out["router_close"] = True

    return out


def _fix_hints_from_results(results: dict[str, ProbeResult]) -> list[str]:
    hints: list[str] = []
    r = results

    if "rpc_select" in r and not r["rpc_select"].ok:
        hints.append(
            "RPC_SELECT failed: set RPC_URL/SOL_RPC_URL or RPC_URLS/SOL_RPC_URLS to reachable endpoints."
        )
    if "router" in r and not r["router"].ok:
        hints.append(
            "ROUTER failed: ensure utils_pkg/router.py is canonical and exports get_quote/get_buy_quote/quote."
        )
    if "solana" in r and not r["solana"].ok:
        hints.append(
            "SOLANA_RPC failed: chosen RPC may be unhealthy; rotate endpoints or increase timeouts."
        )
    if "core_import" in r and not r["core_import"].ok:
        hints.append("CORE_IMPORT failed: fix import/deps/env parsing in utils_pkg/core_utils.py.")
    if "redis" in r and not r["redis"].ok:
        hints.append("REDIS failed: set REDIS_URL/REDIS_TLS_URL or set HEALTHCHECK_SKIP_REDIS=1.")
    if "db" in r and not r["db"].ok:
        hints.append(
            "AI_DB failed: set DATABASE_URL/POSTGRES_DSN/PG_DSN or set SKIP_DSN_REQUIRED=1 for scanner-only."
        )

    out: list[str] = []
    seen: set[str] = set()
    for h in hints:
        if h and h not in seen:
            out.append(h)
            seen.add(h)
    return out[:12]


async def run_local_healthcheck(
    *,
    strict: bool,
    overall_timeout_s: float,
    probe_timeout_s: float,
    max_parallel: int,
    probes_only: set[str],
    probes_skip: set[str],
    json_only: bool,
    verbose: bool = False,
    **_ignored: Any,
) -> dict[str, Any]:
    """
    This is the function scripts/health.py /readyz can call.

    IMPORTANT: Keep this signature stable. Extra kwargs are ignored.
    """
    registry = _build_registry()

    selected: list[ProbeSpec] = []
    for p in registry:
        if probes_skip and p.key in probes_skip:
            continue
        selected.append(p)

    enabled_map = {p.key: _probe_enabled(p, probes_only=probes_only) for p in selected}
    fatal_map = {p.key: _probe_fatal(p) for p in selected}

    start = time.monotonic()
    # Hard bound (and keep it sane)
    overall_timeout_s = float(max(2.0, min(overall_timeout_s, 30.0)))
    probe_timeout_s = float(max(0.4, min(probe_timeout_s, overall_timeout_s)))
    deadline = start + overall_timeout_s

    results: dict[str, ProbeResult] = {}

    def _time_left() -> float:
        return max(0.05, deadline - time.monotonic())

    async def _run_one(p: ProbeSpec, to_s: float) -> None:
        # Never allow a probe to exceed remaining budget
        to_s = float(max(0.2, min(to_s, _time_left())))
        results[p.key] = await _run_probe(
            p,
            enabled=enabled_map[p.key],
            fatal=fatal_map[p.key],
            timeout_s=to_s,
            json_only=json_only,
            verbose=verbose,
        )

    # Phase 1: cheap probes
    for key in ("env", "dns", "data_dir"):
        p = next((x for x in selected if x.key == key), None)
        if not p:
            continue
        await _run_one(p, min(probe_timeout_s, 2.0))
        if strict and results[p.key].fatal and not results[p.key].ok:
            return {
                "ok": False,
                "fatal_failures": 1,
                "duration_ms": int((time.monotonic() - start) * 1000),
                "selected": [pp.key for pp in selected if enabled_map.get(pp.key)],
                "fix_hints": _fix_hints_from_results(results),
                "results": {k: asdict(v) for k, v in results.items()},
            }

    # Phase 2: rpc_select (dependency for solana)
    p = next((x for x in selected if x.key == "rpc_select"), None)
    if p:
        await _run_one(p, min(probe_timeout_s, 5.0))
        if strict and results[p.key].fatal and not results[p.key].ok:
            return {
                "ok": False,
                "fatal_failures": 1,
                "duration_ms": int((time.monotonic() - start) * 1000),
                "selected": [pp.key for pp in selected if enabled_map.get(pp.key)],
                "fix_hints": _fix_hints_from_results(results),
                "results": {k: asdict(v) for k, v in results.items()},
            }

    # Phase 3: core_import (cheap, but validates module wiring)
    p = next((x for x in selected if x.key == "core_import"), None)
    if p:
        await _run_one(p, min(probe_timeout_s, 4.0))
        if strict and results[p.key].fatal and not results[p.key].ok:
            return {
                "ok": False,
                "fatal_failures": 1,
                "duration_ms": int((time.monotonic() - start) * 1000),
                "selected": [pp.key for pp in selected if enabled_map.get(pp.key)],
                "fix_hints": _fix_hints_from_results(results),
                "results": {k: asdict(v) for k, v in results.items()},
            }

    # Phase 4: rest parallel (bounded), but never beyond deadline
    rest = [x for x in selected if x.key not in results]
    sem = asyncio.Semaphore(max(1, int(max_parallel)))

    async def _wrapped(pp: ProbeSpec) -> None:
        async with sem:
            await _run_one(pp, probe_timeout_s)

    tasks = [asyncio.create_task(_wrapped(pp)) for pp in rest]
    if tasks:
        try:
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=_time_left())
        except TimeoutError:
            results["overall_timeout"] = ProbeResult(
                key="overall_timeout",
                name="OVERALL_TIMEOUT",
                ok=False,
                fatal=True,
                error=f"timeout after {overall_timeout_s:.2f}s",
                duration_ms=int(overall_timeout_s * 1000),
            )
        finally:
            for t in tasks:
                if not t.done():
                    t.cancel()
                    with contextlib.suppress(Exception):
                        await t

    close = {}
    with contextlib.suppress(Exception):
        close = await asyncio.shield(_best_effort_close(timeout_s=min(1.5, probe_timeout_s)))

    fatal_fail = sum(1 for rr in results.values() if rr.fatal and not rr.ok)
    ok = fatal_fail == 0

    return {
        "ok": bool(ok),
        "fatal_failures": int(fatal_fail),
        "duration_ms": int((time.monotonic() - start) * 1000),
        "selected": [p.key for p in selected if enabled_map.get(p.key)],
        "meta": {
            "host": _sanitize(platform.node()),
            "pid": os.getpid(),
            "python": _sanitize(sys.version.split()[0]),
            "platform": _sanitize(platform.platform()),
            "render": _render_detected(),
            "SKIP_DSN_REQUIRED": int(_skip_dsn_required()),
            "has_any_dsn": int(_has_any_dsn()),
            "has_any_redis": int(_has_any_redis()),
            "RPC_URL": _mask_url(_envs("RPC_URL", ""))[:180],
            "SOL_RPC_URL": _mask_url(_envs("SOL_RPC_URL", ""))[:180],
            "close": _safe_jsonable(close),
        },
        "fix_hints": _fix_hints_from_results(results),
        "results": {k: asdict(v) for k, v in results.items()},
    }


# =============================================================================
# main entrypoint
# =============================================================================
async def _amain(args: argparse.Namespace) -> int:
    json_only = bool(args.json_only or _envb("HEALTHCHECK_JSON_ONLY", False))
    emit_json = bool(args.json or _envb("HEALTHCHECK_JSON", False) or json_only)
    strict = bool(args.strict)
    verbose = bool(args.verbose or _envb("HEALTHCHECK_VERBOSE", False))

    # global skip (preflight-friendly)
    skip, reason = _should_skip_entirely()
    if skip:
        payload = {"ts": _ts(), "ok": True, "skipped": True, "reason": reason}
        if emit_json:
            print(json.dumps(_safe_jsonable(payload), ensure_ascii=False, default=str))
        return RC_READY

    mode = (args.mode or _envs("HEALTHCHECK_MODE", "auto")).strip().lower()
    if mode not in ("auto", "http", "local"):
        payload = {"ts": _ts(), "ok": False, "error": f"bad mode={mode!r}"}
        if emit_json:
            print(json.dumps(payload, ensure_ascii=False))
        return RC_MISCONFIG

    url = (args.url or _default_health_url()).strip()

    # Defaults tuned to never trip preflight timeouts, but still configurable by env/CLI
    retries = int(args.retries or _envi("HEALTHCHECK_RETRIES", 0) or (2 if mode == "auto" else 8))
    retry_delay_s = float(
        args.retry_delay
        or _envf("HEALTHCHECK_RETRY_DELAY_S", 0.0)
        or (0.15 if mode == "auto" else 0.5)
    )
    http_timeout_s = float(
        args.http_timeout
        or _envf("HEALTHCHECK_HTTP_TIMEOUT_S", 0.0)
        or (1.2 if mode == "auto" else 2.5)
    )
    allow_local_fallback = _envb("HEALTHCHECK_ALLOW_LOCAL_FALLBACK", True)

    overall_timeout_s = float(args.timeout or _envf("HEALTHCHECK_TIMEOUT_S", 0.0) or 12.0)
    probe_timeout_s = float(args.probe_timeout or _envf("HEALTHCHECK_PROBE_TIMEOUT_S", 0.0) or 4.0)
    max_parallel = int(args.max_parallel or _envi("HEALTHCHECK_MAX_PARALLEL", 0) or 6)
    probes_only = set(_parse_csv(args.probes or ""))
    probes_skip = set(_parse_csv(args.skip or ""))

    if verbose:
        _hlog(
            f"boot: mode={mode} render={int(_render_detected())} url={_mask_url(url)}",
            json_only=json_only,
        )

    payload: dict[str, Any] = {
        "ts": _ts(),
        "ok": False,
        "mode": mode,
        "render": _render_detected(),
        "http": None,
        "local": None,
        "meta": {
            "service": _envs("SERVICE_NAME", _envs("APP_NAME", "sniper")),
            "version": _envs("VERSION", _envs("APP_VERSION", "v1")),
            "git_sha": _envs("GIT_SHA", "")[:16],
        },
    }

    if mode in ("auto", "http"):
        http_res = await run_http_healthcheck(
            url=url,
            retries=retries,
            retry_delay_s=retry_delay_s,
            timeout_s=http_timeout_s,
            json_only=json_only,
            verbose=verbose,
            auto_mode=(mode == "auto"),
        )
        payload["http"] = http_res
        if http_res.get("ok") is True:
            payload["ok"] = True
            if emit_json:
                print(json.dumps(_safe_jsonable(payload), ensure_ascii=False, default=str))
            return RC_READY

        if mode == "http":
            if emit_json:
                print(json.dumps(_safe_jsonable(payload), ensure_ascii=False, default=str))
            return RC_NOT_READY

        if not allow_local_fallback:
            if emit_json:
                print(json.dumps(_safe_jsonable(payload), ensure_ascii=False, default=str))
            return RC_NOT_READY

        if verbose:
            _hlog("auto: http not ready -> local fallback", json_only=json_only)

    # local probes
    local_res = await run_local_healthcheck(
        strict=strict,
        overall_timeout_s=overall_timeout_s,
        probe_timeout_s=probe_timeout_s,
        max_parallel=max_parallel,
        probes_only=probes_only,
        probes_skip=probes_skip,
        json_only=json_only,
        verbose=verbose,
    )
    payload["local"] = local_res
    payload["ok"] = bool(local_res.get("ok"))

    if emit_json:
        print(json.dumps(_safe_jsonable(payload), ensure_ascii=False, default=str))

    return RC_READY if payload["ok"] else RC_NOT_READY


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Canonical healthcheck (HTTP client + local fallback probes)"
    )

    ap.add_argument(
        "--mode", type=str, default="", help="auto|http|local (default: HEALTHCHECK_MODE or auto)"
    )
    ap.add_argument(
        "--url",
        type=str,
        default="",
        help="Health URL (default: HEALTHCHECK_URL or http://127.0.0.1:${PORT}/readyz)",
    )
    ap.add_argument("--retries", type=int, default=0, help="HTTP retries (env HEALTHCHECK_RETRIES)")
    ap.add_argument(
        "--retry-delay",
        type=float,
        default=0.0,
        help="HTTP retry delay seconds (env HEALTHCHECK_RETRY_DELAY_S)",
    )
    ap.add_argument(
        "--http-timeout",
        type=float,
        default=0.0,
        help="HTTP timeout seconds (env HEALTHCHECK_HTTP_TIMEOUT_S)",
    )

    ap.add_argument(
        "--timeout",
        type=float,
        default=0.0,
        help="Local overall timeout seconds (env HEALTHCHECK_TIMEOUT_S)",
    )
    ap.add_argument(
        "--probe-timeout",
        type=float,
        default=0.0,
        help="Local per-probe timeout seconds (env HEALTHCHECK_PROBE_TIMEOUT_S)",
    )
    ap.add_argument(
        "--max-parallel",
        type=int,
        default=0,
        help="Local max parallel probes (env HEALTHCHECK_MAX_PARALLEL)",
    )
    ap.add_argument("--probes", type=str, default="", help="Local: run only these probe keys (csv)")
    ap.add_argument("--skip", type=str, default="", help="Local: skip these probe keys (csv)")
    ap.add_argument(
        "--strict", action="store_true", help="Local: fail fast on first fatal probe failure"
    )

    ap.add_argument("--json", action="store_true", help="Emit JSON summary to STDOUT")
    ap.add_argument("--json-only", action="store_true", help="Only JSON output (no human logs)")
    ap.add_argument(
        "--verbose",
        action="store_true",
        help="Verbose human logs to STDERR (or HEALTHCHECK_VERBOSE=1)",
    )

    args = ap.parse_args()

    with contextlib.suppress(Exception):
        _bootstrap_repo_root()
    with contextlib.suppress(Exception):
        _maybe_load_dotenv()

    # Windows asyncio stability: selector is safest across mixed libs
    with contextlib.suppress(Exception):
        if os.name == "nt" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]

    try:
        rc = asyncio.run(_amain(args))
    except KeyboardInterrupt:
        rc = RC_CTRL_C
    except Exception:
        err = _sanitize(traceback.format_exc())
        emit_json = bool(
            args.json
            or args.json_only
            or _envb("HEALTHCHECK_JSON", False)
            or _envb("HEALTHCHECK_JSON_ONLY", False)
        )
        if emit_json:
            print(json.dumps({"ok": False, "ts": _ts(), "error": err[:900]}, ensure_ascii=False))
        else:
            _hlog(f"FAIL: unhandled\n{err}", json_only=False)
        rc = RC_NOT_READY

    raise SystemExit(int(rc))


if __name__ == "__main__":
    main()
