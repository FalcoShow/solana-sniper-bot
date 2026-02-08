# -*- coding: utf-8 -*-
from __future__ import annotations

"""
utils_pkg/rpc_failover.py — RPC selection + failover (V3+++ LOCK-IN)
====================================================================

Goal
----
Pick and keep the best Solana RPC endpoint without thrashing:
- latency-aware + health-aware scoring
- banlist support (static + dynamic)
- sticky selection + hysteresis (never flip-flop)
- cooldowns after failures
- async-only probing (no network at import time)
- concurrency-safe: single-flight probe/selection (no races)

Public API (stable)
-------------------
Core:
- gather_http_candidates(env=os.environ) -> list[str]
- gather_ws_candidates(env=os.environ) -> list[str]
- get_http_url(force_probe: bool=False) -> str                [async]
- get_ws_url(force_probe: bool=False) -> str                  [async]
- resolve_http_first_alive(candidates=None, timeout=...) -> (chosen, notes)   [async]
- apply_failover_to_env(timeout=...) -> (chosen, notes)       [async]

Runtime hooks (used by wallet/client code):
- report_ok(url, latency_ms=None, note="") -> None
- report_fail(url, note="", *, status=None) -> None
- ban(url, reason="", ttl_sec=None) -> None
- unban(url) -> None
- stats() -> dict
- close_http() -> None                                        [async]
- pick_http_url_sync(timeout=...) -> str                       [sync helper]

Design rules
------------
- No method/attr monkeypatching on loggers (Phase 2 rule).
- No aiohttp session creation or networking at import time.
- If aiohttp isn't installed, we only fail when probing is requested.
"""

import asyncio
import contextlib
import os
import random
import time
from dataclasses import dataclass
from typing import Any, Mapping

# aiohttp is optional at import-time (so local envs don’t hard-crash).
try:
    import aiohttp  # type: ignore
except Exception:  # pragma: no cover
    aiohttp = None  # type: ignore


# ──────────────────────────────────────────────────────────────────────────────
# Logging shim (repo-compatible: log.event + log.every) WITHOUT monkeypatching
# ──────────────────────────────────────────────────────────────────────────────
class _LogShim:
    """
    Minimal logger shim providing .event() and .every() with best-effort behavior.
    Does NOT assign attributes onto the underlying logger (no monkeypatch).
    """

    __slots__ = ("_base", "_last")

    def __init__(self, base: Any) -> None:
        self._base = base
        self._last: dict[str, float] = {}

    def event(self, name: str, **fields: Any) -> None:
        # Prefer EventLogger-style .event(name, **fields)
        fn = getattr(self._base, "event", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                fn(name, **fields)
                return

        # Fallback to stdlib logging: log info
        with contextlib.suppress(Exception):
            info = getattr(self._base, "info", None)
            if callable(info):
                info("%s %s", name, fields)
                return

        # Last resort: do nothing
        return

    def every(self, key: str, n: int = 30, msg: str | None = None, **fields: Any) -> None:
        # Time-gated throttle: once per N seconds per key
        try:
            now = time.monotonic()
            sec = float(max(1, int(n)))
            last = float(self._last.get(key, 0.0))
            if (now - last) >= sec:
                self._last[key] = now
                self.event(msg or key, **fields)
        except Exception:
            return

    # Convenience passthroughs (best-effort)
    def debug(self, *a: Any, **kw: Any) -> None:
        fn = getattr(self._base, "debug", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                fn(*a, **kw)

    def info(self, *a: Any, **kw: Any) -> None:
        fn = getattr(self._base, "info", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                fn(*a, **kw)

    def warning(self, *a: Any, **kw: Any) -> None:
        fn = getattr(self._base, "warning", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                fn(*a, **kw)

    def error(self, *a: Any, **kw: Any) -> None:
        fn = getattr(self._base, "error", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                fn(*a, **kw)


def _get_logger() -> _LogShim:
    # Prefer your compat wrapper (stable across repo)
    try:
        from utils_pkg.logging_setup import get_logger  # type: ignore

        return _LogShim(get_logger(__name__))  # type: ignore[arg-type]
    except Exception:  # pragma: no cover
        try:
            import logging

            return _LogShim(logging.getLogger(__name__))
        except Exception:
            return _LogShim(object())


log = _get_logger()


def _short(x: Any, n: int = 260) -> str:
    try:
        s = str(x)
        return s if len(s) <= n else (s[:n] + "…")
    except Exception:
        return "<?>"


# ──────────────────────────────────────────────────────────────────────────────
# Defaults / Env helpers
# ──────────────────────────────────────────────────────────────────────────────
PUBLIC_HTTP = "https://api.mainnet-beta.solana.com"
PUBLIC_WS = "wss://api.mainnet-beta.solana.com"

_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _env_str(name: str, default: str = "", env: Mapping[str, str] | None = None) -> str:
    e = os.environ if env is None else env
    v = e.get(name)
    return default if v is None else str(v).strip()


def _env_int(name: str, default: int, env: Mapping[str, str] | None = None) -> int:
    try:
        return int(float(_env_str(name, str(default), env=env)))
    except Exception:
        return int(default)


def _env_float(name: str, default: float, env: Mapping[str, str] | None = None) -> float:
    try:
        return float(_env_str(name, str(default), env=env))
    except Exception:
        return float(default)


def _env_bool(name: str, default: bool = False, env: Mapping[str, str] | None = None) -> bool:
    e = os.environ if env is None else env
    v = e.get(name)
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return default


def _mono() -> float:
    return time.monotonic()


def _wall() -> float:
    return time.time()


def _split_list(val: str) -> list[str]:
    s = (val or "").strip()
    if not s:
        return []
    s = s.replace(";", ",").replace("\n", ",").replace("\t", ",")
    parts = [p.strip() for p in s.split(",")]
    return [p for p in parts if p]


def _dedupe_preserve(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for it in items:
        it2 = (it or "").strip()
        if it2 and it2 not in seen:
            out.append(it2)
            seen.add(it2)
    return out


def _normalize_http_url(u: str) -> str:
    s = (u or "").strip()
    if not s:
        return ""
    if "://" not in s:
        s = "https://" + s
    return s.rstrip("/")


def _normalize_ws_url(u: str) -> str:
    s = (u or "").strip()
    if not s:
        return ""
    if "://" not in s:
        s = "wss://" + s
    return s.rstrip("/")


def _http_to_ws(u: str) -> str:
    h = _normalize_http_url(u)
    if not h:
        return ""
    if h.startswith("https://"):
        return "wss://" + h[len("https://") :]
    if h.startswith("http://"):
        return "ws://" + h[len("http://") :]
    return h.replace("https:", "wss:").replace("http:", "ws:")


# ──────────────────────────────────────────────────────────────────────────────
# Candidate gathering (backwards compatible)
# ──────────────────────────────────────────────────────────────────────────────
def gather_http_candidates(env: Mapping[str, str] = os.environ) -> list[str]:
    primaries = [
        "HEALTHCHECK_RPC_URL",
        "RPC_URL",
        "SOL_RPC_URL",
        "SOLANA_RPC_URL",
        "HELIUS_RPC_URL",
        "HEALTHCHECK_RPC_HTTP",
    ]
    lists = [
        "SOLANA_RPC_FALLBACKS",
        "RPC_URLS",
        "RPC_URL_FALLBACKS",
        "SOLANA_RPC_URLS",
        "SOL_RPC_URLS",
        "SOLANA_RPC_FALLBACK_URLS",
    ]
    numbered = [
        "RPC_URL_FALLBACK_1",
        "RPC_URL_FALLBACK_2",
        "RPC_URL_FALLBACK_3",
        "SOLANA_RPC_FALLBACK_1",
        "SOLANA_RPC_FALLBACK_2",
    ]

    urls: list[str] = []
    for k in primaries:
        v = (env.get(k) or "").strip()
        if v:
            urls.append(v)

    for k in lists:
        v = (env.get(k) or "").strip()
        if v:
            urls.extend(_split_list(v))

    for k in numbered:
        v = (env.get(k) or "").strip()
        if v:
            urls.append(v)

    urls = [_normalize_http_url(x) for x in urls if x.strip()]
    urls = _dedupe_preserve([u for u in urls if u])

    if PUBLIC_HTTP not in urls:
        urls.append(PUBLIC_HTTP)
    return urls


def gather_ws_candidates(env: Mapping[str, str] = os.environ) -> list[str]:
    primaries = [
        "HEALTHCHECK_WS_URL",
        "WS_URL",
        "SOL_WS_URL",
        "SOLANA_WS_URL",
        "HELIUS_WS_URL",
        "HEALTHCHECK_RPC_WS",
    ]
    lists = [
        "SOLANA_WS_FALLBACKS",
        "WS_URLS",
        "WS_URL_FALLBACKS",
        "SOLANA_WS_URLS",
        "SOL_WS_URLS",
    ]
    numbered = [
        "WS_URL_FALLBACK_1",
        "WS_URL_FALLBACK_2",
        "WS_URL_FALLBACK_3",
        "SOLANA_WS_FALLBACK_1",
        "SOLANA_WS_FALLBACK_2",
    ]

    urls: list[str] = []
    for k in primaries:
        v = (env.get(k) or "").strip()
        if v:
            urls.append(v)

    for k in lists:
        v = (env.get(k) or "").strip()
        if v:
            urls.extend(_split_list(v))

    for k in numbered:
        v = (env.get(k) or "").strip()
        if v:
            urls.append(v)

    urls = [_normalize_ws_url(x) for x in urls if x.strip()]
    urls = _dedupe_preserve([u for u in urls if u])

    if PUBLIC_WS not in urls:
        urls.append(PUBLIC_WS)
    return urls


# ──────────────────────────────────────────────────────────────────────────────
# Policy
# ──────────────────────────────────────────────────────────────────────────────
def _policy() -> dict[str, Any]:
    # NOTE: Read env dynamically to allow runtime tuning (no restart needed).
    return {
        "sticky_ttl_sec": max(10, _env_int("RPC_STICKY_TTL_SEC", 120)),
        "min_switch_sec": max(10, _env_int("RPC_MIN_SWITCH_SEC", 45)),
        "recheck_sec": max(5, _env_int("RPC_RECHECK_SEC", 20)),
        "probe_timeout_sec": max(1.0, _env_float("RPC_PROBE_TIMEOUT_SEC", 3.5)),
        "probe_parallel": max(1, _env_int("RPC_PROBE_PARALLEL", 4)),
        "max_probe_candidates": max(3, _env_int("RPC_MAX_PROBE_CANDIDATES", 12)),
        "fails_to_cooldown": max(1, _env_int("RPC_FAILS_TO_COOLDOWN", 2)),
        "fail_cooldown_sec": max(5, _env_int("RPC_FAIL_COOLDOWN_SEC", 30)),
        "fails_to_eject": max(2, _env_int("RPC_FAILS_TO_EJECT", 6)),
        "eject_ttl_sec": max(60, _env_int("RPC_EJECT_TTL_SEC", 900)),
        "hysteresis_margin": max(0.05, float(_env_float("RPC_HYSTERESIS_MARGIN", 0.20))),
        "ewma_alpha": min(0.9, max(0.05, float(_env_float("RPC_EWMA_ALPHA", 0.20)))),
        "health_method": (_env_str("RPC_HEALTH_METHOD", "getHealth") or "getHealth").strip(),
        "prefer_env_order": _env_bool("RPC_PREFER_ENV_ORDER", True),
        "randomize_subset": _env_bool("RPC_RANDOMIZE_SUBSET", False),
        "aiohttp_limit": max(10, _env_int("RPC_AIOHTTP_LIMIT", 50)),
        "probe_jitter_ms": max(0, _env_int("RPC_PROBE_JITTER_MS", 50)),
        "treat_429_hard_fail": _env_bool("RPC_429_HARD_FAIL", True),
        # Optional: fast-disable probes (keeps behavior stable if unset)
        "disable_probes": _env_bool("RPC_DISABLE_PROBES", False),
    }


# ──────────────────────────────────────────────────────────────────────────────
# State
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class _RPCState:
    url: str
    ewma_ms: float = 9999.0
    ok_count: int = 0
    fail_count: int = 0
    last_ok_mono: float = 0.0
    last_fail_mono: float = 0.0
    cooldown_until_mono: float = 0.0
    eject_until_mono: float = 0.0
    note: str = ""

    def allowed(self, tmono: float) -> bool:
        return tmono >= self.cooldown_until_mono and tmono >= self.eject_until_mono

    def score(self, tmono: float) -> float:
        # Higher is better. Disallowed is hard-negative.
        if not self.allowed(tmono):
            return -1e9

        latency = max(1.0, float(self.ewma_ms))
        base = 10000.0 / latency

        # stale penalty: if no ok yet, mild penalty; otherwise proportional to age
        if self.last_ok_mono <= 0:
            stale_pen = 1.0
        else:
            age_s = max(0.0, tmono - self.last_ok_mono)
            stale_pen = min(3.0, age_s / 60.0)

        # fail penalty: small linear
        fail_pen = float(self.fail_count) * 2.0
        return float(base - fail_pen - stale_pen)


@dataclass
class _Ban:
    reason: str
    until_mono: float


_LOCK = asyncio.Lock()

_STATES: dict[str, _RPCState] = {}
_BANS: dict[str, _Ban] = {}

_CHOSEN_HTTP: str | None = None
_CHOSEN_WS: str | None = None

_STICKY_UNTIL_HTTP_MONO: float = 0.0
_STICKY_UNTIL_WS_MONO: float = 0.0
_LAST_SWITCH_HTTP_MONO: float = 0.0
_LAST_SWITCH_WS_MONO: float = 0.0
_LAST_PROBE_HTTP_MONO: float = 0.0
_LAST_PROBE_WS_MONO: float = 0.0

_SESSION: Any = None
_SESSION_LOCK = asyncio.Lock()


def _ensure_aiohttp() -> None:
    if aiohttp is None:
        raise RuntimeError("aiohttp_missing: install with `py -m pip install aiohttp`")


async def _get_session() -> Any:
    global _SESSION
    _ensure_aiohttp()
    if _SESSION is not None and not getattr(_SESSION, "closed", True):
        return _SESSION

    async with _SESSION_LOCK:
        if _SESSION is not None and not getattr(_SESSION, "closed", True):
            return _SESSION

        pol = _policy()
        timeout = aiohttp.ClientTimeout(total=float(pol["probe_timeout_sec"]))  # type: ignore[attr-defined]
        connector = aiohttp.TCPConnector(  # type: ignore[attr-defined]
            limit=int(pol["aiohttp_limit"]),
            ttl_dns_cache=60,
            enable_cleanup_closed=True,
        )
        _SESSION = aiohttp.ClientSession(  # type: ignore[attr-defined]
            timeout=timeout,
            connector=connector,
            trust_env=True,
            headers={
                "content-type": "application/json",
                "accept": "application/json",
                "user-agent": "solana-sniper-bot/1",
            },
        )
        return _SESSION


async def close_http() -> None:
    global _SESSION
    s = _SESSION
    _SESSION = None
    if s is not None and not getattr(s, "closed", True):
        with contextlib.suppress(Exception):
            await s.close()


def _ensure_state(url: str) -> _RPCState:
    st = _STATES.get(url)
    if st is None:
        st = _RPCState(url=url)
        _STATES[url] = st
    return st


def _is_banned(url: str, tmono: float | None = None) -> bool:
    t = _mono() if tmono is None else float(tmono)
    b = _BANS.get(url)
    if not b:
        return False
    if t >= float(b.until_mono):
        _BANS.pop(url, None)
        return False
    return True


def _apply_static_banlist(candidates: list[str]) -> list[str]:
    raw = _env_str("RPC_BANLIST", "")
    if not raw:
        return candidates
    banned = set(_normalize_http_url(x) for x in _split_list(raw))
    out: list[str] = []
    for u in candidates:
        uu = _normalize_http_url(u)
        if uu and uu not in banned:
            out.append(uu)
    return out


def ban(url: str, reason: str = "", ttl_sec: int | None = None) -> None:
    u = _normalize_http_url(url)
    if not u:
        return
    pol = _policy()
    ttl = int(ttl_sec or pol["eject_ttl_sec"])
    _BANS[u] = _Ban(reason=reason or "banned", until_mono=_mono() + float(ttl))
    log.every("RPC_BAN", n=5, msg="RPC_BAN", url=u, ttl_sec=ttl, reason=reason or "banned")


def unban(url: str) -> None:
    u = _normalize_http_url(url)
    if u:
        _BANS.pop(u, None)


def report_ok(url: str, latency_ms: float | None = None, note: str = "") -> None:
    u = _normalize_http_url(url)
    if not u:
        return
    pol = _policy()
    st = _ensure_state(u)

    st.ok_count += 1
    st.last_ok_mono = _mono()
    if note:
        st.note = note

    if latency_ms is not None:
        alpha = float(pol["ewma_alpha"])
        ms = float(max(1.0, latency_ms))
        st.ewma_ms = (1.0 - alpha) * float(st.ewma_ms) + alpha * ms

    if st.fail_count > 0:
        st.fail_count = max(0, st.fail_count - 1)

    st.cooldown_until_mono = 0.0
    st.eject_until_mono = 0.0


def report_fail(url: str, note: str = "", *, status: int | None = None) -> None:
    u = _normalize_http_url(url)
    if not u:
        return
    pol = _policy()
    st = _ensure_state(u)

    st.fail_count += 1
    st.last_fail_mono = _mono()
    if note:
        st.note = note

    if status == 429 and bool(pol["treat_429_hard_fail"]):
        st.fail_count += int(pol["fails_to_cooldown"])

    if st.fail_count >= int(pol["fails_to_cooldown"]):
        st.cooldown_until_mono = max(
            st.cooldown_until_mono, _mono() + float(pol["fail_cooldown_sec"])
        )

    if st.fail_count >= int(pol["fails_to_eject"]):
        st.eject_until_mono = max(st.eject_until_mono, _mono() + float(pol["eject_ttl_sec"]))
        log.every(
            "RPC_EJECT",
            n=5,
            msg="RPC_EJECT",
            url=u,
            ttl_sec=int(pol["eject_ttl_sec"]),
            note=note,
            status=status,
        )


# ──────────────────────────────────────────────────────────────────────────────
# Probing (no network until called)
# ──────────────────────────────────────────────────────────────────────────────
async def _rpc_call(
    session: Any, url: str, payload: dict[str, Any], timeout_sec: float
) -> tuple[int, Any]:
    _ensure_aiohttp()
    to = aiohttp.ClientTimeout(total=max(1.0, float(timeout_sec)))  # type: ignore[attr-defined]
    async with session.post(url, json=payload, timeout=to) as r:
        status = int(r.status)
        try:
            data = await r.json(content_type=None)
        except Exception:
            data = {"text": (await r.text())[:2000]}
        return status, data


def _is_health_ok(method: str, status: int, data: Any) -> bool:
    if status != 200:
        return False
    if not isinstance(data, dict):
        return False
    if data.get("error"):
        return False

    if method == "getHealth":
        res = data.get("result")
        return res == "ok" or res == {"Ok": {}} or res == {"ok": True}

    if method == "getLatestBlockhash":
        return bool(data.get("result"))

    return bool(data.get("result"))


async def _probe_http(url: str, timeout: float) -> tuple[bool, float, str, int | None]:
    u = _normalize_http_url(url)
    if not u:
        return False, 9999.0, "bad_url", None

    pol = _policy()
    if bool(pol["disable_probes"]):
        # Behave as "unknown but not failed": allow selection code to work
        # while avoiding network IO (useful for constrained test envs).
        return True, 9999.0, "probes_disabled", 200

    tmono = _mono()
    if _is_banned(u, tmono=tmono):
        return False, 9999.0, "banned", None

    st = _ensure_state(u)
    if not st.allowed(tmono):
        return False, 9999.0, "cooldown", None

    method = str(pol["health_method"] or "getHealth").strip()

    payload_health: dict[str, Any] = {"jsonrpc": "2.0", "id": 1, "method": method}
    payload_bh: dict[str, Any] = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getLatestBlockhash",
        "params": [{"commitment": "processed"}],
    }

    session = await _get_session()

    jitter_ms = int(pol["probe_jitter_ms"])
    if jitter_ms > 0:
        await asyncio.sleep(random.random() * (jitter_ms / 1000.0))

    t0 = _mono()
    try:
        status, data = await _rpc_call(session, u, payload_health, timeout)
        dt_ms = (_mono() - t0) * 1000.0
        if _is_health_ok(method, status, data):
            return True, float(dt_ms), f"{method}:ok", status

        t1 = _mono()
        status2, data2 = await _rpc_call(session, u, payload_bh, timeout)
        dt_ms2 = (_mono() - t1) * 1000.0
        if _is_health_ok("getLatestBlockhash", status2, data2):
            return True, float(dt_ms2), "latestBlockhash:ok", status2

        return False, float(max(dt_ms, dt_ms2)), f"unhealthy:{status}/{status2}", status2
    except asyncio.CancelledError:
        raise
    except Exception as e:
        dt_ms = (_mono() - t0) * 1000.0
        return False, float(dt_ms), f"{type(e).__name__}:{_short(e, 180)}", None


# ──────────────────────────────────────────────────────────────────────────────
# Selection
# ──────────────────────────────────────────────────────────────────────────────
def _score_url(url: str, tmono: float) -> float:
    return _ensure_state(url).score(tmono)


def _pick_subset(candidates: list[str], max_n: int, randomize: bool) -> list[str]:
    c = candidates[:]
    if randomize and len(c) > 1:
        head, tail = c[:1], c[1:]
        random.shuffle(tail)
        c = head + tail
    return c[: max(1, int(max_n))]


async def _select_best_http(
    candidates: list[str],
    *,
    force_probe: bool,
) -> tuple[str, list[tuple[str, bool, str, float]]]:
    global _CHOSEN_HTTP, _STICKY_UNTIL_HTTP_MONO, _LAST_SWITCH_HTTP_MONO, _LAST_PROBE_HTTP_MONO

    pol = _policy()
    tmono = _mono()

    candidates = [_normalize_http_url(u) for u in candidates if u]
    candidates = _dedupe_preserve([u for u in candidates if u])
    candidates = _apply_static_banlist(candidates)
    candidates = [u for u in candidates if not _is_banned(u, tmono=tmono)]
    if not candidates:
        candidates = [PUBLIC_HTTP]

    # Sticky hold
    if (
        (not force_probe)
        and _CHOSEN_HTTP
        and (tmono < float(_STICKY_UNTIL_HTTP_MONO))
        and (_normalize_http_url(_CHOSEN_HTTP) in candidates)
    ):
        return _CHOSEN_HTTP, [(_CHOSEN_HTTP, True, "sticky", 0.0)]

    # Recheck throttle to avoid frequent probes
    if _CHOSEN_HTTP and (not force_probe):
        cur = _normalize_http_url(_CHOSEN_HTTP)
        if cur in candidates:
            st = _ensure_state(cur)
            if st.allowed(tmono) and (tmono - float(_LAST_PROBE_HTTP_MONO)) < float(
                pol["recheck_sec"]
            ):
                _STICKY_UNTIL_HTTP_MONO = tmono + float(pol["sticky_ttl_sec"])
                return cur, [(cur, True, "recheck_throttled", 0.0)]

    _LAST_PROBE_HTTP_MONO = tmono

    subset = _pick_subset(
        candidates,
        int(pol["max_probe_candidates"]),
        randomize=bool(pol["randomize_subset"]) and not bool(pol["prefer_env_order"]),
    )

    timeout = float(pol["probe_timeout_sec"])
    sem = asyncio.Semaphore(int(pol["probe_parallel"]))

    async def _one(u: str) -> tuple[str, bool, str, float, int | None]:
        uu = _normalize_http_url(u)
        if not uu:
            return u, False, "bad_url", 9999.0, None
        async with sem:
            ok, ms, note, status = await _probe_http(uu, timeout=timeout)
            return uu, ok, note, float(ms), status

    tasks = [asyncio.create_task(_one(u)) for u in subset]
    notes: list[tuple[str, bool, str, float]] = []

    try:
        for coro in asyncio.as_completed(tasks):
            u, ok, note, ms, status = await coro
            notes.append((u, ok, note, ms))
            if ok:
                report_ok(u, latency_ms=ms, note=note)
            else:
                report_fail(u, note=note, status=status)
    finally:
        for tsk in tasks:
            if not tsk.done():
                tsk.cancel()

    ok_urls = _dedupe_preserve([u for (u, ok, _note, _ms) in notes if ok])

    if not ok_urls:
        # Prefer keeping current if still allowed
        if _CHOSEN_HTTP and _normalize_http_url(_CHOSEN_HTTP) in candidates:
            _STICKY_UNTIL_HTTP_MONO = tmono + float(pol["sticky_ttl_sec"])
            return _normalize_http_url(_CHOSEN_HTTP), notes
        return candidates[-1], notes

    ok_urls.sort(key=lambda u: _score_url(u, tmono), reverse=True)
    winner = ok_urls[0]

    # Hysteresis / min-switch
    if _CHOSEN_HTTP:
        cur = _normalize_http_url(_CHOSEN_HTTP)
        if cur and cur in candidates and _ensure_state(cur).allowed(tmono):
            if (tmono - float(_LAST_SWITCH_HTTP_MONO)) < float(pol["min_switch_sec"]):
                winner = cur
            elif winner != cur:
                cur_score = _ensure_state(cur).score(tmono)
                win_score = _ensure_state(winner).score(tmono)
                margin = float(pol["hysteresis_margin"])
                if win_score < (cur_score * (1.0 + margin)):
                    winner = cur

    if winner != _CHOSEN_HTTP:
        _LAST_SWITCH_HTTP_MONO = tmono
        log.every("RPC_SWITCH_HTTP", n=10, msg="RPC_SWITCH_HTTP", frm=_CHOSEN_HTTP, to=winner)

    _CHOSEN_HTTP = winner
    _STICKY_UNTIL_HTTP_MONO = tmono + float(pol["sticky_ttl_sec"])
    return winner, notes


async def _select_best_ws(
    ws_candidates: list[str],
    http_chosen: str,
    *,
    force_probe: bool,
) -> tuple[str, list[tuple[str, bool, str, float]]]:
    global _CHOSEN_WS, _STICKY_UNTIL_WS_MONO, _LAST_SWITCH_WS_MONO, _LAST_PROBE_WS_MONO

    pol = _policy()
    tmono = _mono()

    ws_candidates = [_normalize_ws_url(u) for u in ws_candidates if u]
    ws_candidates = _dedupe_preserve([u for u in ws_candidates if u])

    if not ws_candidates:
        derived = _http_to_ws(http_chosen)
        ws_candidates = [derived] if derived else []
        if PUBLIC_WS not in ws_candidates:
            ws_candidates.append(PUBLIC_WS)

    # Sticky hold
    if (
        (not force_probe)
        and _CHOSEN_WS
        and (tmono < float(_STICKY_UNTIL_WS_MONO))
        and (_normalize_ws_url(_CHOSEN_WS) in ws_candidates)
    ):
        return _CHOSEN_WS, [(_CHOSEN_WS, True, "sticky", 0.0)]

    # Recheck throttle
    if _CHOSEN_WS and (not force_probe):
        if (tmono - float(_LAST_PROBE_WS_MONO)) < float(pol["recheck_sec"]):
            _STICKY_UNTIL_WS_MONO = tmono + float(pol["sticky_ttl_sec"])
            return _CHOSEN_WS, [(_CHOSEN_WS, True, "recheck_throttled", 0.0)]

    _LAST_PROBE_WS_MONO = tmono

    # Explicit WS overrides (if user set them)
    explicit = (
        _env_str("HEALTHCHECK_WS_URL")
        or _env_str("WS_URL")
        or _env_str("SOLANA_WS_URL")
        or _env_str("SOL_WS_URL")
        or _env_str("HELIUS_WS_URL")
        or ""
    )
    explicit = _normalize_ws_url(explicit)

    if explicit and explicit in ws_candidates:
        winner = explicit
    else:
        derived = _normalize_ws_url(_http_to_ws(http_chosen)) if http_chosen else ""
        winner = derived or (ws_candidates[0] if ws_candidates else PUBLIC_WS)

    # min-switch guard
    if _CHOSEN_WS and winner != _CHOSEN_WS:
        if (tmono - float(_LAST_SWITCH_WS_MONO)) < float(pol["min_switch_sec"]):
            winner = _CHOSEN_WS

    if winner != _CHOSEN_WS:
        _LAST_SWITCH_WS_MONO = tmono
        log.every("RPC_SWITCH_WS", n=10, msg="RPC_SWITCH_WS", frm=_CHOSEN_WS, to=winner)

    _CHOSEN_WS = winner
    _STICKY_UNTIL_WS_MONO = tmono + float(pol["sticky_ttl_sec"])
    return winner, [(winner, True, "chosen", 0.0)]


# ──────────────────────────────────────────────────────────────────────────────
# Public async getters (concurrency-safe)
# ──────────────────────────────────────────────────────────────────────────────
async def _get_http_url_locked(force_probe: bool) -> str:
    candidates = gather_http_candidates()
    chosen, _ = await _select_best_http(candidates, force_probe=bool(force_probe))
    return chosen


async def get_http_url(force_probe: bool = False) -> str:
    async with _LOCK:
        return await _get_http_url_locked(bool(force_probe))


async def get_ws_url(force_probe: bool = False) -> str:
    async with _LOCK:
        http_chosen = await _get_http_url_locked(bool(force_probe))
        ws_candidates = gather_ws_candidates()
        ws, _ = await _select_best_ws(ws_candidates, http_chosen, force_probe=bool(force_probe))
        return ws


# ──────────────────────────────────────────────────────────────────────────────
# Backwards-compatible helpers
# ──────────────────────────────────────────────────────────────────────────────
async def resolve_http_first_alive(
    candidates: list[str] | None = None,
    timeout: float = 3.5,
) -> tuple[str, list[tuple[str, bool, str]]]:
    if candidates is None:
        candidates = gather_http_candidates()

    old = os.getenv("RPC_PROBE_TIMEOUT_SEC")
    os.environ["RPC_PROBE_TIMEOUT_SEC"] = str(timeout)
    try:
        async with _LOCK:
            chosen, notes4 = await _select_best_http(
                [_normalize_http_url(u) for u in candidates], force_probe=True
            )
        notes3 = [(u, ok, why) for (u, ok, why, _ms) in notes4]
        return chosen, notes3
    finally:
        if old is None:
            os.environ.pop("RPC_PROBE_TIMEOUT_SEC", None)
        else:
            os.environ["RPC_PROBE_TIMEOUT_SEC"] = old


def pick_http_url_sync(timeout: float = 3.5) -> str:
    # Protect against calling asyncio.run from inside a running loop.
    try:
        _ = asyncio.get_running_loop()
        raise RuntimeError(
            "pick_http_url_sync called inside running event loop; use await get_http_url()"
        )
    except RuntimeError as e:
        if "no running event loop" not in str(e).lower():
            raise

    old = os.getenv("RPC_PROBE_TIMEOUT_SEC")
    os.environ["RPC_PROBE_TIMEOUT_SEC"] = str(timeout)
    try:
        return asyncio.run(get_http_url(force_probe=True))
    finally:
        if old is None:
            os.environ.pop("RPC_PROBE_TIMEOUT_SEC", None)
        else:
            os.environ["RPC_PROBE_TIMEOUT_SEC"] = old


async def apply_failover_to_env(
    timeout_s: float | None = None,
    timeout: float | None = None,
    *,
    env: Any = os.environ,
) -> tuple[str, Any]:
    """
    Probe + choose best HTTP RPC, then choose WS, and write them into env.
    Returns: (chosen_http, rich_info)
    """
    to = float(timeout_s if timeout_s is not None else (timeout if timeout is not None else 3.5))

    # One lock pass: pick HTTP + WS consistently.
    old = os.getenv("RPC_PROBE_TIMEOUT_SEC")
    os.environ["RPC_PROBE_TIMEOUT_SEC"] = str(to)
    try:
        async with _LOCK:
            http_candidates = gather_http_candidates()
            chosen_http, notes4 = await _select_best_http(http_candidates, force_probe=True)

            ws_candidates = gather_ws_candidates()
            chosen_ws, _ = await _select_best_ws(ws_candidates, chosen_http, force_probe=True)

        env["RPC_URL"] = str(chosen_http)
        env["SOL_RPC_URL"] = str(chosen_http)

        if chosen_ws:
            env["WS_URL"] = str(chosen_ws)

        notes3 = [(u, ok, why) for (u, ok, why, _ms) in notes4]
        rich = {
            "chosen_http": chosen_http,
            "chosen_ws": chosen_ws or None,
            "notes": notes3,
            "policy": _policy(),
        }
        return chosen_http, rich
    finally:
        if old is None:
            os.environ.pop("RPC_PROBE_TIMEOUT_SEC", None)
        else:
            os.environ["RPC_PROBE_TIMEOUT_SEC"] = old


# ──────────────────────────────────────────────────────────────────────────────
# Diagnostics
# ──────────────────────────────────────────────────────────────────────────────
def stats() -> dict[str, Any]:
    pol = _policy()
    tmono = _mono()

    states: dict[str, Any] = {}
    for u, s in _STATES.items():
        states[u] = {
            "ewma_ms": float(s.ewma_ms),
            "ok": int(s.ok_count),
            "fail": int(s.fail_count),
            "last_ok_age_s": None
            if s.last_ok_mono <= 0
            else float(max(0.0, tmono - s.last_ok_mono)),
            "last_fail_age_s": None
            if s.last_fail_mono <= 0
            else float(max(0.0, tmono - s.last_fail_mono)),
            "cooldown_left_s": float(max(0.0, s.cooldown_until_mono - tmono)),
            "eject_left_s": float(max(0.0, s.eject_until_mono - tmono)),
            "note": s.note,
            "score": float(s.score(tmono)),
            "banned": bool(_is_banned(u, tmono=tmono)),
        }

    bans = {
        u: {"reason": b.reason, "ttl_left_s": float(max(0.0, b.until_mono - tmono))}
        for u, b in _BANS.items()
    }

    return {
        "policy": pol,
        "chosen_http": _CHOSEN_HTTP,
        "chosen_ws": _CHOSEN_WS,
        "sticky_left_http_s": float(max(0.0, _STICKY_UNTIL_HTTP_MONO - tmono)),
        "sticky_left_ws_s": float(max(0.0, _STICKY_UNTIL_WS_MONO - tmono)),
        "last_switch_http_age_s": None
        if _LAST_SWITCH_HTTP_MONO <= 0
        else float(max(0.0, tmono - _LAST_SWITCH_HTTP_MONO)),
        "last_switch_ws_age_s": None
        if _LAST_SWITCH_WS_MONO <= 0
        else float(max(0.0, tmono - _LAST_SWITCH_WS_MONO)),
        "last_probe_http_age_s": None
        if _LAST_PROBE_HTTP_MONO <= 0
        else float(max(0.0, tmono - _LAST_PROBE_HTTP_MONO)),
        "last_probe_ws_age_s": None
        if _LAST_PROBE_WS_MONO <= 0
        else float(max(0.0, tmono - _LAST_PROBE_WS_MONO)),
        "states": states,
        "bans": bans,
        "ts": _wall(),
    }


__all__ = [
    "PUBLIC_HTTP",
    "PUBLIC_WS",
    "apply_failover_to_env",
    "ban",
    "close_http",
    "gather_http_candidates",
    "gather_ws_candidates",
    "get_http_url",
    "get_ws_url",
    "pick_http_url_sync",
    "report_fail",
    "report_ok",
    "resolve_http_first_alive",
    "stats",
    "unban",
]
