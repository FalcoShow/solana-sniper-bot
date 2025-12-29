"""
scripts/healthcheck.py — God-Tier Healthcheck (Phase 0–7 LOCK-IN)
=================================================================

Goal
----
Single, tunable healthcheck suitable for Render probes + local debugging.

Hard guarantees
---------------
- No secrets printed (DSNs/keys/wallets masked).
- JSON to STDOUT; human logs to STDERR.
- Defensive imports (never hard-require optional deps).
- Per-probe timeouts + overall timeout.
- Fatal vs non-fatal probes (configurable).
- Minimal side effects:
    * RPC selection MAY set RPC_URL/SOL_RPC_URL in env (intended).
    * No Jupiter calls unless probe enabled.
- Always attempts cleanup (core_utils close + lightning/router/rpc_failover http close where available).

Exit code
---------
0  all enabled fatal probes passed
1  any enabled fatal probe failed
130 Ctrl-C

CLI
---
python -m scripts.healthcheck
python -m scripts.healthcheck --json
python -m scripts.healthcheck --json-only
python -m scripts.healthcheck --strict
python -m scripts.healthcheck --list
python -m scripts.healthcheck --print-config

Env toggles (1 = skip)
----------------------
HEALTHCHECK_SKIP_RPC_SELECT=1
HEALTHCHECK_SKIP_CORE_INIT=1
HEALTHCHECK_SKIP_ROUTER=1
HEALTHCHECK_SKIP_REDIS=1
HEALTHCHECK_SKIP_SOLANA=1
HEALTHCHECK_SKIP_DB=1

DB rule (important)
-------------------
- If SKIP_DSN_REQUIRED=1 then the DB probe AUTO-SKIPS (even if HEALTHCHECK_SKIP_DB is unset).
- If SKIP_DSN_REQUIRED=0 and no DSN is present, DB probe FAILS with a clear hint.

Optional probes (default disabled; enable by setting SKIP=0)
-----------------------------------------------------------
HEALTHCHECK_SKIP_JUP=0        (Jupiter quote-only sanity; use sparingly)
HEALTHCHECK_SKIP_PRICE=0      (SOL/USD pricing sanity)
HEALTHCHECK_SKIP_RISK=0       (risk_manager + regime snapshot)
HEALTHCHECK_SKIP_AI=0         (AI stack import/describe)
HEALTHCHECK_SKIP_DATA_DIR=0   (DATA_DIR writable check)
HEALTHCHECK_SKIP_DNS=0        (DNS resolve check)

Escalation (1 = make fatal)
---------------------------
HEALTHCHECK_DNS_FATAL=1
HEALTHCHECK_JUP_FATAL=1
HEALTHCHECK_PRICE_FATAL=1
HEALTHCHECK_RISK_FATAL=1
HEALTHCHECK_AI_FATAL=1
HEALTHCHECK_DATA_DIR_FATAL=1

Output
------
HEALTHCHECK_JSON=1
HEALTHCHECK_JSON_ONLY=1

Timeouts
--------
HEALTHCHECK_TIMEOUT_S=18
HEALTHCHECK_PROBE_TIMEOUT_S=8
HEALTHCHECK_RPC_TIMEOUT_S=6

RPC sources (optional)
----------------------
RPC_URL / SOL_RPC_URL
RPC_URLS / SOL_RPC_URLS / SOLANA_RPC_URLS  (csv/semicolon list)

Notes
-----
- RPC selection runs early (so later probes use working RPC_URL/SOL_RPC_URL).
- Best-effort calls core_utils.init_resources() once, then probes, then close_resources().
- Router readiness is import + signature inspection only by default (keeps probes cheap).
- Jupiter probe is QUOTE-ONLY and disabled by default.
"""

from __future__ import annotations

# ──────────────────────────────────────────────────────────────────────────────
# repo-root bootstrap (so "py scripts/x.py" can import utils_pkg)
# IMPORTANT: must be AFTER __future__ import.
# ──────────────────────────────────────────────────────────────────────────────
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import argparse
import asyncio
import contextlib
import inspect
import json
import os
import platform
import re
import socket
import time
import traceback
from dataclasses import asdict, dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence, Set, Tuple

# =============================================================================
# env helpers
# =============================================================================
_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}


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
    try:
        return int(float(_envs(k, str(d))))
    except Exception:
        return int(d)


def _envf(k: str, d: float) -> float:
    try:
        return float(_envs(k, str(d)))
    except Exception:
        return float(d)


def _ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _parse_csv(s: str) -> List[str]:
    if not s:
        return []
    parts = [p.strip() for p in s.replace(";", ",").split(",")]
    return [p for p in parts if p]


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


# =============================================================================
# logging: human -> stderr, json -> stdout
# =============================================================================
def _hlog(msg: str, *, json_only: bool) -> None:
    if json_only:
        return
    try:
        sys.stderr.write(f"[healthcheck] {_ts()} {msg}\n")
        sys.stderr.flush()
    except Exception:
        pass


def _short_exc(e: BaseException, n: int = 360) -> str:
    s = f"{type(e).__name__}: {e}"
    return (s[:n] + "…") if len(s) > n else s


# =============================================================================
# redaction / safety
# =============================================================================
_URL_CRED_RE = re.compile(r"(://)([^/@\s]+)@")
_APIKEY_RE = re.compile(r"(?i)(x-api-key:\s*)([^\s]+)")
_BEARER_RE = re.compile(r"(?i)(authorization:\s*bearer\s+)([^\s]+)")
_LONG_TOKENISH_RE = re.compile(r"\b[A-Za-z0-9_\-]{28,}\b")


def _sensitive_env_keys() -> List[str]:
    return [
        "JUP_PRO_KEY",
        "JUP_API_KEY",
        "JUP_KEY",
        "OPENAI_API_KEY",
        "TELEGRAM_BOT_TOKEN",
        "DISCORD_TOKEN",
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
    ]


def _sanitize(text: Any) -> str:
    if text is None:
        return ""
    s = str(text)
    s = _URL_CRED_RE.sub(r"\1***@", s)
    s = _APIKEY_RE.sub(r"\1***", s)
    s = _BEARER_RE.sub(r"\1***", s)

    for k in _sensitive_env_keys():
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
    if "?" in out:
        out = out.split("?", 1)[0] + "?..."
    return out


def _safe_jsonable(x: Any) -> Any:
    try:
        json.dumps(x, default=str)
        return x
    except Exception:
        return str(x)


# =============================================================================
# defensive imports
# =============================================================================
def _try_import(name: str) -> Any:
    with contextlib.suppress(Exception):
        return __import__(name, fromlist=["*"])
    return None


# =============================================================================
# signature-drift tolerant calls
# =============================================================================
def _filter_kwargs(fn: Any, kwargs: Dict[str, Any]) -> Dict[str, Any]:
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


async def _call(fn: Any, *args: Any, **kwargs: Any) -> Any:
    return await _maybe_await(fn(*args, **kwargs))


async def _call_named(
    target: Any,
    names: Tuple[str, ...],
    *,
    args: Tuple[Any, ...] = (),
    kwargs: Optional[Dict[str, Any]] = None,
    timeout_s: float = 8.0,
) -> Tuple[bool, Any, str]:
    last = "no_callable_found"
    for nm in names:
        fn = getattr(target, nm, None)
        if not callable(fn):
            continue
        try:
            kw = _filter_kwargs(fn, dict(kwargs or {}))
            val = await asyncio.wait_for(_call(fn, *args, **kw), timeout=max(1.0, float(timeout_s)))
            return True, val, nm
        except TypeError as e:
            last = f"{nm}: {e}"
            continue
        except asyncio.TimeoutError:
            last = f"{nm}: timeout"
            continue
        except Exception as e:
            return False, None, f"{nm}: {_short_exc(e)}"
    return False, None, last


# =============================================================================
# models
# =============================================================================
@dataclass
class ProbeResult:
    name: str
    ok: bool
    fatal: bool
    skipped: bool = False
    error: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)
    duration_ms: int = 0


@dataclass(frozen=True)
class ProbeSpec:
    key: str
    name: str
    fn: Callable[[float], Awaitable[Dict[str, Any]]]
    enabled_env_skip: str
    fatal_env: Optional[str] = None
    default_enabled: bool = True
    default_fatal: bool = False


# =============================================================================
# RPC helpers (httpx preferred; urllib fallback)
# =============================================================================
async def _rpc_post(url: str, payload: Dict[str, Any], timeout_s: float) -> Dict[str, Any]:
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

    def _do() -> Dict[str, Any]:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers={"content-type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:  # noqa: S310
            body = resp.read().decode("utf-8", "ignore")
            return json.loads(body)

    return await asyncio.to_thread(_do)


async def _rpc_get_latest_blockhash(url: str, timeout_s: float) -> str:
    payload = {"jsonrpc": "2.0", "id": 1, "method": "getLatestBlockhash", "params": [{"commitment": "processed"}]}
    data = await _rpc_post(url, payload, timeout_s=timeout_s)
    bh = None
    with contextlib.suppress(Exception):
        bh = data["result"]["value"]["blockhash"]
    if not bh:
        raise RuntimeError("could not parse getLatestBlockhash response")
    return str(bh)


async def _rpc_get_version(url: str, timeout_s: float) -> str:
    payload = {"jsonrpc": "2.0", "id": 1, "method": "getVersion"}
    data = await _rpc_post(url, payload, timeout_s=timeout_s)
    ver = None
    with contextlib.suppress(Exception):
        ver = data["result"]["solana-core"]
    return str(ver) if ver else "unknown"


def _candidate_rpcs() -> List[str]:
    out: List[str] = []
    for k in ("RPC_URL", "SOL_RPC_URL"):
        v = _envs(k, "")
        if v:
            out.append(v)
    for k in ("RPC_URLS", "SOL_RPC_URLS", "SOLANA_RPC_URLS"):
        out.extend(_parse_csv(_envs(k, "")))

    seen: Set[str] = set()
    uniq: List[str] = []
    for x in out:
        x = x.strip()
        if not x or x in seen:
            continue
        uniq.append(x)
        seen.add(x)
    return uniq


# =============================================================================
# probes
# =============================================================================
async def probe_dns(timeout_s: float) -> Dict[str, Any]:
    jup = _envs("JUP_BASE_URL", "https://api.jup.ag").strip()
    host = ""
    with contextlib.suppress(Exception):
        host = jup.split("://", 1)[1].split("/", 1)[0].strip().split(":", 1)[0]
    if not host:
        raise RuntimeError("could not parse host from JUP_BASE_URL")

    async def _aresolve() -> List[str]:
        loop = asyncio.get_running_loop()
        ips: List[str] = []
        infos = await loop.getaddrinfo(host, None)
        for fam, _, _, _, sockaddr in infos:
            if fam in (socket.AF_INET, socket.AF_INET6):
                ip = sockaddr[0]
                if ip not in ips:
                    ips.append(ip)
        return ips[:8]

    ips = await asyncio.wait_for(_aresolve(), timeout=max(1.0, timeout_s))
    return {"host": host, "ips": ips}


async def probe_data_dir(timeout_s: float) -> Dict[str, Any]:
    _ = timeout_s
    data_dir = _envs("DATA_DIR", "")
    if not data_dir:
        data_dir = "/opt/render/data" if (_envs("RENDER_SERVICE_ID", "") or _envs("RENDER_INSTANCE_ID", "")) else "./data"
    data_dir = os.path.abspath(data_dir)
    os.makedirs(data_dir, exist_ok=True)

    test_path = os.path.join(data_dir, f".hc_write_{int(time.time())}.tmp")
    with open(test_path, "w", encoding="utf-8") as f:
        f.write("ok")
    os.remove(test_path)
    return {"data_dir": data_dir, "write_ok": True}


async def probe_rpc_select(timeout_s: float) -> Dict[str, Any]:
    rf = _try_import("utils_pkg.rpc_failover")
    if rf is not None:
        fn = getattr(rf, "apply_failover_to_env", None)
        if callable(fn):
            ok, val, used = await _call_named(
                rf,
                ("apply_failover_to_env",),
                kwargs={"timeout_s": timeout_s, "timeout": timeout_s},
                timeout_s=timeout_s,
            )
            if ok:
                chosen = os.getenv("RPC_URL") or os.getenv("SOL_RPC_URL") or ""
                notes = None
                if isinstance(val, (tuple, list)) and len(val) >= 1:
                    chosen = str(val[0] or chosen or "")
                    notes = val[1] if len(val) >= 2 else None
                if chosen:
                    os.environ["RPC_URL"] = str(chosen)
                    os.environ["SOL_RPC_URL"] = str(chosen)
                return {"via": f"rpc_failover:{used}", "chosen_rpc": chosen[:160], "notes": _safe_jsonable(notes)}

    cands = _candidate_rpcs()
    if not cands:
        raise RuntimeError("no RPC candidates (set RPC_URL or SOL_RPC_URL or *_URLS)")

    last_err = None
    best: Optional[Tuple[str, int, str]] = None  # (url, ms, blockhash)

    for u in cands[:12]:
        t0 = time.time()
        try:
            bh = await asyncio.wait_for(_rpc_get_latest_blockhash(u, timeout_s=timeout_s), timeout=max(1.0, timeout_s + 1.0))
            ms = int((time.time() - t0) * 1000)
            if best is None or ms < best[1]:
                best = (u, ms, bh)
        except Exception as e:
            last_err = _short_exc(e)
            continue

    if not best:
        raise RuntimeError(f"no reachable RPC from {len(cands)} candidates; last_err={last_err}")

    u, ms, bh = best
    os.environ["RPC_URL"] = u
    os.environ["SOL_RPC_URL"] = u
    return {"via": "internal_probe_best_latency", "chosen_rpc": u[:160], "latency_ms": ms, "blockhash": bh}


async def probe_core_init(timeout_s: float) -> Dict[str, Any]:
    core = _try_import("utils_pkg.core_utils")
    if core is None:
        raise RuntimeError("utils_pkg.core_utils not importable")

    ok, _, used = await _call_named(core, ("init_resources", "init", "bootstrap", "setup", "warmup"), timeout_s=timeout_s)
    if not ok:
        raise RuntimeError(f"core init failed: {used}")
    return {"via": used}


async def _best_effort_close(timeout_s: float) -> Dict[str, Any]:
    """
    Close as much as we can without throwing:
    - core_utils.close_resources / close / shutdown
    - rpc_failover.close_http (if available)
    - lightning_impl.close_http
    - router.close_http if present
    """
    out: Dict[str, Any] = {}

    core = _try_import("utils_pkg.core_utils")
    if core is not None:
        ok, _, used = await _call_named(core, ("close_resources", "close", "shutdown", "teardown"), timeout_s=timeout_s)
        out["core_close"] = {"ok": ok, "via": used}

    rf = _try_import("utils_pkg.rpc_failover")
    if rf is not None:
        fn = getattr(rf, "close_http", None) or getattr(rf, "close", None) or getattr(rf, "shutdown", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                await asyncio.wait_for(_maybe_await(fn()), timeout=max(1.0, timeout_s))
            out["rpc_failover_close_http"] = True

    L = _try_import("utils_pkg.lightning_impl")
    if L is not None:
        fn = getattr(L, "close_http", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                await asyncio.wait_for(_maybe_await(fn()), timeout=max(1.0, timeout_s))
            out["lightning_close_http"] = True

    router = _try_import("utils_pkg.router")
    if router is not None:
        fn = getattr(router, "close_http", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                await asyncio.wait_for(_maybe_await(fn()), timeout=max(1.0, timeout_s))
            out["router_close_http"] = True

    return out


async def probe_router_ready(timeout_s: float) -> Dict[str, Any]:
    _ = timeout_s
    router = _try_import("utils_pkg.router")
    if router is None:
        raise RuntimeError("utils_pkg.router not importable")

    wanted = ["get_buy_quote", "get_sell_quote", "get_quote", "quote", "build_swap_tx", "build_swap"]
    found: Dict[str, str] = {}
    missing: List[str] = []

    for nm in wanted:
        fn = getattr(router, nm, None)
        if callable(fn):
            sig = ""
            with contextlib.suppress(Exception):
                sig = str(inspect.signature(fn))
            found[nm] = sig or "(signature unavailable)"
        else:
            missing.append(nm)

    has_quote = any(k in found for k in ("get_buy_quote", "get_quote", "quote"))
    if not has_quote:
        raise RuntimeError("router has no quote function (expected get_buy_quote/get_quote/quote)")

    lightning = _try_import("utils_pkg.lightning_impl")
    lightning_ok = bool(lightning is not None)
    lightning_flags = None
    if lightning_ok:
        with contextlib.suppress(Exception):
            vf = getattr(lightning, "_variant_flags", None)
            if callable(vf):
                lightning_flags = _safe_jsonable(vf())

    return {
        "has_quote": has_quote,
        "router_fns_found": found,
        "router_missing_count": len(missing),
        "lightning_impl_importable": lightning_ok,
        "lightning_variant_flags": lightning_flags,
    }


async def probe_redis(timeout_s: float) -> Dict[str, Any]:
    core = _try_import("utils_pkg.core_utils")

    r = None
    if core is not None:
        for getter in ("get_redis_async", "get_redis", "redis"):
            fn = getattr(core, getter, None)
            if callable(fn):
                r = await _maybe_await(fn())
                if r is not None:
                    break

    if r is not None:
        if hasattr(r, "ping"):
            await asyncio.wait_for(_maybe_await(r.ping()), timeout=timeout_s)

        key = f"hc:ping:{int(time.time())}"
        val = str(time.time())
        if hasattr(r, "set"):
            await asyncio.wait_for(_maybe_await(r.set(key, val, ex=30)), timeout=timeout_s)
        got = None
        if hasattr(r, "get"):
            got = await asyncio.wait_for(_maybe_await(r.get(key)), timeout=timeout_s)

        if isinstance(got, bytes):
            got = got.decode("utf-8", "ignore")
        if got != val:
            raise RuntimeError(f"redis echo mismatch expected={val} got={got}")

        return {"via": "core_utils.redis", "key": key, "echo": got}

    redis_url = _envs("REDIS_URL", "") or _envs("REDIS_TLS_URL", "")
    if not redis_url:
        raise RuntimeError("no redis handle and REDIS_URL/REDIS_TLS_URL not set")

    with contextlib.suppress(Exception):
        import redis.asyncio as redis  # type: ignore

        rr = redis.from_url(redis_url, socket_timeout=timeout_s)  # type: ignore
        await asyncio.wait_for(rr.ping(), timeout=timeout_s)
        return {"via": "redis.asyncio.from_url", "redis_url_masked": _mask_url(redis_url)[:140]}

    raise RuntimeError("redis.asyncio not installed (install or set HEALTHCHECK_SKIP_REDIS=1)")


async def probe_solana(timeout_s: float) -> Dict[str, Any]:
    rpc = _envs("RPC_URL", "") or _envs("SOL_RPC_URL", "")
    if not rpc:
        raise RuntimeError("RPC_URL/SOL_RPC_URL not set (rpc_select should run first)")

    bh = await _rpc_get_latest_blockhash(rpc, timeout_s=timeout_s)
    ver = await _rpc_get_version(rpc, timeout_s=timeout_s)
    return {"rpc": rpc[:160], "blockhash": bh, "solana_core": ver}


async def probe_db(timeout_s: float) -> Dict[str, Any]:
    """
    DB probe expects utils_pkg.ai_db.ping(...) to exist.

    Rules:
    - If SKIP_DSN_REQUIRED=1 => caller should skip this probe (handled in _probe_enabled),
      but we also return a safe SKIP if invoked directly.
    - If SKIP_DSN_REQUIRED=0 and no DSN present => FAIL with a clear hint.
    """
    if _skip_dsn_required():
        return {"skipped": True, "reason": "SKIP_DSN_REQUIRED=1"}

    if not _has_any_dsn():
        raise RuntimeError(
            "no Postgres DSN env set (set DATABASE_URL/POSTGRES_DSN/PG_DSN/PNL_DSN or set SKIP_DSN_REQUIRED=1 for scanner-only runs)"
        )

    ai_db = _try_import("utils_pkg.ai_db")
    if ai_db is None:
        raise RuntimeError("utils_pkg.ai_db not importable")

    out: Dict[str, Any] = {"dsns_present": True}

    desc = getattr(ai_db, "describe_backend", None) or getattr(ai_db, "describe", None)
    if callable(desc):
        with contextlib.suppress(Exception):
            out["backend"] = _safe_jsonable(desc())

    ping = getattr(ai_db, "ping", None)
    if not callable(ping):
        raise RuntimeError("ai_db.ping missing (implement ping() or set HEALTHCHECK_SKIP_DB=1)")

    kw = _filter_kwargs(ping, {"timeout_s": timeout_s, "timeout": timeout_s})
    ok = await asyncio.wait_for(_maybe_await(ping(**kw)), timeout=timeout_s)
    if not ok:
        raise RuntimeError("ai_db.ping returned False")

    out["ping"] = True
    for k in ("PNL_DSN", "PG_DSN", "DATABASE_URL", "PG_DSN_INTERNAL", "PG_DSN_EXTERNAL", "POSTGRES_DSN"):
        v = os.getenv(k, "")
        if v:
            out[f"{k}_set"] = True
            out[f"{k}_masked"] = _mask_url(v)[:140]
    return out


async def probe_jupiter(timeout_s: float) -> Dict[str, Any]:
    """
    Quote-only Jupiter sanity (disabled by default; enable with HEALTHCHECK_SKIP_JUP=0).
    Prefer lightning_impl.quote/get_quote if present, else router quote.
    """
    mints = _try_import("utils_pkg.mints")
    if mints is None:
        raise RuntimeError("utils_pkg.mints not importable")

    SOL_MINT = getattr(mints, "SOL_MINT", None) or _envs("SOL_MINT", "")
    USDC_MINT = getattr(mints, "USDC_MINT", None) or _envs("USDC_MINT", "")
    if not SOL_MINT or not USDC_MINT:
        raise RuntimeError("missing SOL_MINT/USDC_MINT (utils_pkg.mints should define them)")

    out_mint = _envs("HEALTHCHECK_JUP_OUT_MINT", "") or str(USDC_MINT)
    lamports = int(float(_envs("HEALTHCHECK_JUP_AMOUNT_SOL", "0.0025")) * 1_000_000_000)
    slippage_bps = _envi("HEALTHCHECK_JUP_SLIPPAGE_BPS", 200)

    L = _try_import("utils_pkg.lightning_impl")
    if L is not None:
        for nm in ("quote", "get_quote", "get_buy_quote"):
            fn = getattr(L, nm, None)
            if not callable(fn):
                continue

            if nm in ("quote", "get_quote"):
                want = {
                    "inputMint": SOL_MINT,
                    "outputMint": out_mint,
                    "amount": lamports,
                    "slippageBps": slippage_bps,
                    "input_mint": SOL_MINT,
                    "output_mint": out_mint,
                    "amount_raw": lamports,
                    "slippage_bps": slippage_bps,
                    "timeout_s": timeout_s,
                    "timeout": timeout_s,
                }
                kwargs = _filter_kwargs(fn, want)
                q = await asyncio.wait_for(_maybe_await(fn(**kwargs)), timeout=max(1.0, timeout_s))
            else:
                want = {"mint": out_mint, "lamports_in": lamports, "slippage_bps": slippage_bps, "timeout_s": timeout_s, "timeout": timeout_s}
                kwargs = _filter_kwargs(fn, want)
                q = await asyncio.wait_for(_maybe_await(fn(**kwargs)), timeout=max(1.0, timeout_s))

            if isinstance(q, dict):
                out_amt = q.get("outAmount") or q.get("out_amount_raw")
                return {"via": f"lightning_impl.{nm}", "outAmount": (str(out_amt)[:32] if out_amt is not None else None)}

    router = _try_import("utils_pkg.router")
    if router is None:
        raise RuntimeError("utils_pkg.router not importable (and lightning_impl not usable)")

    qfn = getattr(router, "get_quote", None) or getattr(router, "quote", None) or getattr(router, "jup_quote", None)
    if not callable(qfn):
        raise RuntimeError("no router quote function found")

    kwargs2 = {"inputMint": SOL_MINT, "outputMint": out_mint, "amount": lamports, "slippageBps": slippage_bps}
    kwargs2 = _filter_kwargs(qfn, kwargs2)
    q = await asyncio.wait_for(_maybe_await(qfn(**kwargs2)), timeout=max(1.0, timeout_s))
    if not isinstance(q, dict):
        raise RuntimeError("router quote returned non-dict")
    out_amt = q.get("outAmount") or q.get("out_amount_raw")
    return {"via": "router.quote", "outAmount": (str(out_amt)[:32] if out_amt is not None else None)}


async def probe_price(timeout_s: float) -> Dict[str, Any]:
    core = _try_import("utils_pkg.core_utils")
    if core is None:
        raise RuntimeError("utils_pkg.core_utils not importable")

    for nm in ("get_sol_price_usd", "fetch_sol_price_usd", "sol_to_usd", "convert_sol_to_usd"):
        fn = getattr(core, nm, None)
        if not callable(fn):
            continue

        if nm in ("get_sol_price_usd", "fetch_sol_price_usd"):
            kw = _filter_kwargs(fn, {"timeout_s": timeout_s, "timeout": timeout_s})
            usd = await asyncio.wait_for(_maybe_await(fn(**kw)), timeout=max(1.0, timeout_s))
        else:
            usd = await asyncio.wait_for(_maybe_await(fn(1.0)), timeout=max(1.0, timeout_s))

        usd_f = float(usd)
        if usd_f <= 0:
            raise RuntimeError(f"{nm} returned non-positive")
        return {"via": f"core_utils.{nm}", "sol_usd": usd_f}

    raise RuntimeError("no price function found (set HEALTHCHECK_SKIP_PRICE=1 or implement in core_utils)")


async def probe_risk(timeout_s: float) -> Dict[str, Any]:
    rm = _try_import("utils_pkg.risk_manager")
    reg = _try_import("utils_pkg.regime")
    if rm is None or reg is None:
        raise RuntimeError("risk_manager/regime not importable")

    snap_fn = getattr(rm, "risk_snapshot", None)
    if not callable(snap_fn):
        raise RuntimeError("risk_manager.risk_snapshot not callable")

    snap = await asyncio.wait_for(_maybe_await(snap_fn()), timeout=max(1.0, timeout_s))
    out: Dict[str, Any] = {"risk_snapshot": _safe_jsonable(snap)}

    regime_state = getattr(reg, "regime_state", None)
    feats_fn = getattr(regime_state, "features", None) if regime_state is not None else None
    if callable(feats_fn):
        feats = await asyncio.wait_for(_maybe_await(feats_fn()), timeout=max(1.0, timeout_s))
        out["regime_features"] = _safe_jsonable(feats)

    return out


async def probe_ai(timeout_s: float) -> Dict[str, Any]:
    _ = timeout_s
    out: Dict[str, Any] = {}
    entry = _try_import("utils_pkg.ai_entry_logic")
    scoring = _try_import("utils_pkg.ai_buy_scoring")
    strat = _try_import("utils_pkg.self_learning_ai_strategy")

    if entry is None:
        raise RuntimeError("utils_pkg.ai_entry_logic not importable")

    out["ai_entry_logic"] = {"imported": True}
    out["ai_buy_scoring"] = {"imported": bool(scoring is not None)}
    out["self_learning_ai_strategy"] = {"imported": bool(strat is not None)}

    for mod, key, names in [
        (entry, "ai_entry_logic", ("describe_entry_backend", "describe_backend", "describe")),
        (scoring, "ai_buy_scoring", ("describe_scorer", "describe_backend", "describe")),
        (strat, "self_learning_ai_strategy", ("describe_strategy_backend", "describe_backend", "describe")),
    ]:
        if mod is None:
            continue
        for nm in names:
            fn = getattr(mod, nm, None)
            if callable(fn):
                with contextlib.suppress(Exception):
                    out[key]["describe"] = _safe_jsonable(fn())
                break

    await asyncio.sleep(0)
    return out


# =============================================================================
# probe runner
# =============================================================================
async def _run_probe(
    *,
    name: str,
    fatal: bool,
    enabled: bool,
    fn: Callable[[float], Awaitable[Dict[str, Any]]],
    timeout_s: float,
    json_only: bool,
) -> ProbeResult:
    if not enabled:
        _hlog(f"{name}: skipped", json_only=json_only)
        return ProbeResult(name=name, ok=True, fatal=fatal, skipped=True, details={"skipped": True})

    t0 = time.time()
    try:
        details = await asyncio.wait_for(fn(timeout_s), timeout=max(1.0, float(timeout_s) + 1.0))
        dt = int((time.time() - t0) * 1000)
        _hlog(f"{name}: OK ({dt}ms)", json_only=json_only)
        return ProbeResult(
            name=name,
            ok=True,
            fatal=fatal,
            details={k: _safe_jsonable(v) for k, v in (details or {}).items()},
            duration_ms=dt,
        )
    except Exception as e:
        dt = int((time.time() - t0) * 1000)
        msg = _sanitize(_short_exc(e))
        _hlog(f"{name}: FAIL ({dt}ms) err={msg}", json_only=json_only)
        return ProbeResult(name=name, ok=False, fatal=fatal, error=msg, details={}, duration_ms=dt)


def _build_registry() -> List[ProbeSpec]:
    """
    Registry order matters:
      - cheap signals first
      - rpc_select before solana/jupiter
      - core_init before redis/db/price/risk
    """
    return [
        # optional (disabled by default; enable via SKIP=0)
        ProbeSpec("dns", "DNS_JUP_BASE", probe_dns, "HEALTHCHECK_SKIP_DNS", "HEALTHCHECK_DNS_FATAL", default_enabled=False, default_fatal=False),
        ProbeSpec("data_dir", "DATA_DIR", probe_data_dir, "HEALTHCHECK_SKIP_DATA_DIR", "HEALTHCHECK_DATA_DIR_FATAL", default_enabled=False, default_fatal=False),
        # core fatals
        ProbeSpec("rpc_select", "RPC_SELECT", probe_rpc_select, "HEALTHCHECK_SKIP_RPC_SELECT", None, default_enabled=True, default_fatal=True),
        ProbeSpec("core_init", "CORE_INIT", probe_core_init, "HEALTHCHECK_SKIP_CORE_INIT", None, default_enabled=True, default_fatal=True),
        ProbeSpec("router", "ROUTER_READY", probe_router_ready, "HEALTHCHECK_SKIP_ROUTER", None, default_enabled=True, default_fatal=True),
        ProbeSpec("redis", "REDIS", probe_redis, "HEALTHCHECK_SKIP_REDIS", None, default_enabled=True, default_fatal=True),
        ProbeSpec("solana", "SOLANA_RPC", probe_solana, "HEALTHCHECK_SKIP_SOLANA", None, default_enabled=True, default_fatal=True),
        ProbeSpec("db", "AI_DB", probe_db, "HEALTHCHECK_SKIP_DB", None, default_enabled=True, default_fatal=True),
        # optional heavy probes (disabled by default; enable via SKIP=0)
        ProbeSpec("jup", "JUPITER_QUOTE", probe_jupiter, "HEALTHCHECK_SKIP_JUP", "HEALTHCHECK_JUP_FATAL", default_enabled=False, default_fatal=False),
        ProbeSpec("price", "PRICE", probe_price, "HEALTHCHECK_SKIP_PRICE", "HEALTHCHECK_PRICE_FATAL", default_enabled=False, default_fatal=False),
        ProbeSpec("risk", "RISK_REGIME", probe_risk, "HEALTHCHECK_SKIP_RISK", "HEALTHCHECK_RISK_FATAL", default_enabled=False, default_fatal=False),
        ProbeSpec("ai", "AI_STACK", probe_ai, "HEALTHCHECK_SKIP_AI", "HEALTHCHECK_AI_FATAL", default_enabled=False, default_fatal=False),
    ]


def _probe_enabled(spec: ProbeSpec) -> bool:
    # DB auto-skip for scanner-only/local runs
    if spec.key == "db" and _skip_dsn_required():
        return False

    # toggle is "1=skip"
    if _envb(spec.enabled_env_skip, False):
        return False

    # Optional probes default disabled unless user explicitly sets SKIP var (typically to 0)
    if not spec.default_enabled:
        raw = os.getenv(spec.enabled_env_skip)
        if raw is None:
            return False
        return not _envb(spec.enabled_env_skip, False)

    return True


def _probe_fatal(spec: ProbeSpec) -> bool:
    if spec.fatal_env:
        return _envb(spec.fatal_env, spec.default_fatal)
    return spec.default_fatal


def _render_detected() -> bool:
    return bool(_envs("RENDER_SERVICE_ID", "") or _envs("RENDER_INSTANCE_ID", "") or _envs("RENDER", "").lower() in _TRUE)


async def _amain(args: argparse.Namespace) -> int:
    json_only = bool(args.json_only or _envb("HEALTHCHECK_JSON_ONLY", False))
    emit_json = bool(args.json or _envb("HEALTHCHECK_JSON", False) or json_only)
    strict = bool(args.strict)

    overall_timeout = max(6.0, float(args.timeout or _envf("HEALTHCHECK_TIMEOUT_S", 18.0)))
    per_probe_timeout = max(3.0, float(args.probe_timeout or _envf("HEALTHCHECK_PROBE_TIMEOUT_S", 8.0)))
    rpc_timeout = max(2.0, float(args.rpc_timeout or _envf("HEALTHCHECK_RPC_TIMEOUT_S", 6.0)))

    registry = _build_registry()

    if args.list:
        for p in registry:
            enabled = _probe_enabled(p)
            fatal = _probe_fatal(p)
            _hlog(f"{p.key:10} name={p.name:14} enabled={int(enabled)} fatal={int(fatal)} skip_env={p.enabled_env_skip}", json_only=json_only)
        return 0

    if args.print_config:
        cfg = {
            "ts": _ts(),
            "render": _render_detected(),
            "SKIP_DSN_REQUIRED": int(_skip_dsn_required()),
            "has_any_dsn": int(_has_any_dsn()),
            "timeouts": {"overall": overall_timeout, "probe_default": per_probe_timeout, "rpc": rpc_timeout},
            "probes": [{"key": p.key, "name": p.name, "enabled": _probe_enabled(p), "fatal": _probe_fatal(p)} for p in registry],
        }
        print(json.dumps(_safe_jsonable(cfg), ensure_ascii=False, default=str))
        return 0

    start = time.time()
    _hlog("boot: starting healthcheck", json_only=json_only)
    _hlog(f"python={sys.version.split()[0]} platform={platform.platform()}", json_only=json_only)

    probes: Dict[str, ProbeResult] = {}

    # 1) optional cheap probes (dns, data_dir)
    for key in ("dns", "data_dir"):
        p = next((x for x in registry if x.key == key), None)
        if not p:
            continue
        enabled = _probe_enabled(p)
        fatal = _probe_fatal(p)
        pr = await _run_probe(name=p.name, fatal=fatal, enabled=enabled, fn=p.fn, timeout_s=min(per_probe_timeout, 4.0), json_only=json_only)
        probes[p.key] = pr
        if strict and pr.fatal and not pr.ok:
            with contextlib.suppress(Exception):
                await _best_effort_close(timeout_s=min(per_probe_timeout, 10.0))
            return 1

    # 2) rpc_select (fatal) before solana/jupiter
    p_rpc = next((x for x in registry if x.key == "rpc_select"), None)
    if p_rpc:
        enabled = _probe_enabled(p_rpc)
        fatal = _probe_fatal(p_rpc)
        pr = await _run_probe(name=p_rpc.name, fatal=fatal, enabled=enabled, fn=p_rpc.fn, timeout_s=rpc_timeout, json_only=json_only)
        probes[p_rpc.key] = pr
        if strict and pr.fatal and not pr.ok:
            with contextlib.suppress(Exception):
                await _best_effort_close(timeout_s=min(per_probe_timeout, 10.0))
            return 1

    # 3) core_init (fatal) before redis/db/price/risk
    p_core = next((x for x in registry if x.key == "core_init"), None)
    if p_core:
        enabled = _probe_enabled(p_core)
        fatal = _probe_fatal(p_core)
        pr = await _run_probe(name=p_core.name, fatal=fatal, enabled=enabled, fn=p_core.fn, timeout_s=min(per_probe_timeout, 12.0), json_only=json_only)
        probes[p_core.key] = pr
        if strict and pr.fatal and not pr.ok:
            with contextlib.suppress(Exception):
                await _best_effort_close(timeout_s=min(per_probe_timeout, 10.0))
            return 1

    # 4) remaining probes in parallel (cheap enough; timeboxed by overall_timeout)
    async def run_parallel_rest() -> None:
        tasks: List[Tuple[str, asyncio.Task[ProbeResult]]] = []
        for p in registry:
            if p.key in probes:
                continue
            enabled = _probe_enabled(p)
            fatal = _probe_fatal(p)
            tasks.append(
                (p.key, asyncio.create_task(_run_probe(name=p.name, fatal=fatal, enabled=enabled, fn=p.fn, timeout_s=per_probe_timeout, json_only=json_only)))
            )

        if not tasks:
            return

        results = await asyncio.gather(*(t for _, t in tasks), return_exceptions=False)
        for (k, _t), res in zip(tasks, results):
            probes[k] = res

    try:
        await asyncio.wait_for(run_parallel_rest(), timeout=overall_timeout)
    except asyncio.TimeoutError:
        probes["overall_timeout"] = ProbeResult(
            name="OVERALL_TIMEOUT",
            ok=False,
            fatal=True,
            error=f"timeout after {overall_timeout}s",
            details={},
            duration_ms=int(overall_timeout * 1000),
        )

    # Always attempt close
    close_details: Dict[str, Any] = {}
    with contextlib.suppress(Exception):
        close_details = await _best_effort_close(timeout_s=min(per_probe_timeout, 10.0))

    fatal_failures = sum(1 for p in probes.values() if p.fatal and not p.ok)
    nonfatal_failures = sum(1 for p in probes.values() if (not p.fatal) and (not p.ok))
    ok = fatal_failures == 0
    dur_ms = int((time.time() - start) * 1000)

    if not json_only:
        _hlog(f"summary: ok={int(ok)} fatal_failures={fatal_failures} nonfatal_failures={nonfatal_failures} dur_ms={dur_ms}", json_only=False)

    if emit_json:
        meta_env = {
            "SKIP_DSN_REQUIRED": int(_skip_dsn_required()),
            "has_any_dsn": int(_has_any_dsn()),
            "RPC_URL": _mask_url(_envs("RPC_URL", ""))[:140],
            "SOL_RPC_URL": _mask_url(_envs("SOL_RPC_URL", ""))[:140],
            "JUP_BASE_URL": _mask_url(_envs("JUP_BASE_URL", ""))[:140],
            "DATABASE_URL": ("set" if bool(_envs("DATABASE_URL", "")) else "missing"),
            "PG_DSN": ("set" if bool(_envs("PG_DSN", "")) else "missing"),
            "POSTGRES_DSN": ("set" if bool(_envs("POSTGRES_DSN", "")) else "missing"),
            "PNL_DSN": ("set" if bool(_envs("PNL_DSN", "")) else "missing"),
            "REDIS_URL": ("set" if bool(_envs("REDIS_URL", "") or _envs("REDIS_TLS_URL", "")) else "missing"),
        }
        payload = {
            "ok": bool(ok),
            "fatal_failures": int(fatal_failures),
            "nonfatal_failures": int(nonfatal_failures),
            "duration_ms": int(dur_ms),
            "ts": _ts(),
            "meta": {
                "host": _sanitize(platform.node()),
                "pid": os.getpid(),
                "python": sys.version.split()[0],
                "platform": platform.platform(),
                "render": _render_detected(),
                "env": meta_env,
                "close": _safe_jsonable(close_details),
            },
            "probes": {k: asdict(v) for k, v in probes.items()},
        }
        print(json.dumps(_safe_jsonable(payload), ensure_ascii=False, default=str))

    return 0 if ok else 1


def main() -> None:
    ap = argparse.ArgumentParser(description="God-Tier healthcheck (Phase 0–7 LOCK-IN)")
    ap.add_argument("--timeout", type=float, default=0.0, help="Overall timeout seconds (0 uses env/default)")
    ap.add_argument("--probe-timeout", type=float, default=0.0, help="Per-probe timeout seconds (0 uses env/default)")
    ap.add_argument("--rpc-timeout", type=float, default=0.0, help="RPC probe timeout seconds (0 uses env/default)")
    ap.add_argument("--json", action="store_true", help="Emit JSON summary to STDOUT")
    ap.add_argument("--json-only", action="store_true", help="Only JSON output (no human logs)")
    ap.add_argument("--strict", action="store_true", help="Fail fast after first fatal blocker")
    ap.add_argument("--list", action="store_true", help="List probes and resolved enable/fatal flags")
    ap.add_argument("--print-config", action="store_true", help="Print resolved config JSON and exit")
    args = ap.parse_args()

    try:
        rc = asyncio.run(_amain(args))
    except KeyboardInterrupt:
        rc = 130
    except Exception:
        err = _sanitize(traceback.format_exc())
        if args.json or args.json_only or _envb("HEALTHCHECK_JSON", False) or _envb("HEALTHCHECK_JSON_ONLY", False):
            print(json.dumps({"ok": False, "ts": _ts(), "error": err[:900]}, ensure_ascii=False))
        else:
            _hlog(f"FAIL: unhandled\n{err}", json_only=False)
        rc = 1

    raise SystemExit(rc)


if __name__ == "__main__":
    with contextlib.suppress(Exception):
        if os.name == "nt" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]
    main()
