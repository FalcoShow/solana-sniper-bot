from __future__ import annotations

"""
utils_pkg/rpc_failover.py — RPC selection + failover (V3+++ LOCK-IN)
====================================================================

Goal
----
Pick and keep the *best* Solana RPC endpoint without thrashing:
- latency-aware + health-aware scoring
- banlist support (static + dynamic)
- sticky selection + hysteresis (never flip-flop)
- cooldowns after failures
- async-only probing (no network at import time)

This module is intentionally dependency-light (aiohttp only).

Public API (stable)
-------------------
Core:
- gather_http_candidates(env=os.environ) -> list[str]
- gather_ws_candidates(env=os.environ) -> list[str]
- get_http_url(force_probe: bool=False) -> str
- get_ws_url(force_probe: bool=False) -> str
- resolve_http_first_alive(candidates=None, timeout=...) -> (chosen, notes)
- apply_failover_to_env(timeout=...) -> (chosen, notes)

Runtime hooks (used by wallet/client code):
- report_ok(url, latency_ms=None, note="") -> None
- report_fail(url, note="", *, status=None) -> None
- ban(url, reason="", ttl_sec=None) -> None
- unban(url) -> None
- stats() -> dict
- close_http() -> None

Env knobs (safe defaults)
-------------------------
# Candidates (HTTP):
RPC_URL / SOL_RPC_URL / SOLANA_RPC_URL / HELIUS_RPC_URL / HEALTHCHECK_RPC_URL
SOLANA_RPC_FALLBACKS / RPC_URLS / RPC_URL_FALLBACKS / RPC_URL_FALLBACK_1/2
SOLANA_RPC_URLS / SOL_RPC_URLS / SOLANA_RPC_FALLBACK_URLS (csv/semicolon)

# Policy:
RPC_BANLIST="https://bad1,..."
RPC_STICKY_TTL_SEC=120
RPC_MIN_SWITCH_SEC=45
RPC_RECHECK_SEC=20
RPC_FAIL_COOLDOWN_SEC=30
RPC_FAILS_TO_COOLDOWN=2
RPC_FAILS_TO_EJECT=6
RPC_EJECT_TTL_SEC=900
RPC_PROBE_TIMEOUT_SEC=3.5
RPC_PROBE_PARALLEL=4
RPC_MAX_PROBE_CANDIDATES=12
RPC_HEALTH_METHOD="getHealth" (fallbacks to getLatestBlockhash if needed)
RPC_HYSTERESIS_MARGIN=0.20   (20% relative improvement needed to switch)
RPC_EWMA_ALPHA=0.20
RPC_PREFER_ENV_ORDER=1

# WS candidates:
WS_URL / SOLANA_WS_URL / SOL_WS_URL / HELIUS_WS_URL / HEALTHCHECK_WS_URL
SOLANA_WS_FALLBACKS / WS_URLS / WS_URL_FALLBACKS / WS_URL_FALLBACK_1/2
SOLANA_WS_URLS / SOL_WS_URLS

Notes
-----
- “Never thrash” is achieved via: sticky TTL + min switch interval + hysteresis margin.
- No network at import time; probing only happens inside async functions.
"""

import asyncio
import contextlib
import os
import random
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

# ──────────────────────────────────────────────────────────────────────────────
# Logging shim (repo-compatible: log.event + log.every)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from logging_setup import get_logger  # type: ignore

    log = get_logger(__name__)  # type: ignore
except Exception:  # pragma: no cover
    import logging

    log = logging.getLogger(__name__)  # type: ignore[assignment]


def _short(x: Any, n: int = 260) -> str:
    try:
        s = str(x)
        return s if len(s) <= n else (s[:n] + "…")
    except Exception:
        return "<?>"


if not hasattr(log, "event"):

    def _event(name: str, **kw: Any) -> None:  # pragma: no cover
        with contextlib.suppress(Exception):
            log.info("%s %s", name, kw)

    setattr(log, "event", _event)  # type: ignore[attr-defined]

if not hasattr(log, "every"):

    def _every(key: str, n: int = 30, msg: Optional[str] = None, **kw: Any) -> None:  # pragma: no cover
        try:
            now = time.monotonic()
            store = getattr(_every, "_store", {})  # type: ignore[attr-defined]
            last = float(store.get(key, 0.0))
            if (now - last) >= float(max(1, int(n))):
                store[key] = now
                setattr(_every, "_store", store)  # type: ignore[attr-defined]
                getattr(log, "event")(msg or key, **kw)
        except Exception:
            return None

    setattr(log, "every", _every)  # type: ignore[attr-defined]


# ──────────────────────────────────────────────────────────────────────────────
# Defaults / Env helpers
# ──────────────────────────────────────────────────────────────────────────────
PUBLIC_HTTP = "https://api.mainnet-beta.solana.com"
PUBLIC_WS = "wss://api.mainnet-beta.solana.com"

_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(_env_str(name, str(default))))
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    try:
        return float(_env_str(name, str(default)))
    except Exception:
        return float(default)


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
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


def _split_list(val: str) -> List[str]:
    s = (val or "").strip()
    if not s:
        return []
    # allow comma / semicolon / whitespace / newline
    s = s.replace(";", ",").replace("\n", ",").replace("\t", ",")
    parts = [p.strip() for p in s.split(",")]
    return [p for p in parts if p]


def _dedupe_preserve(items: List[str]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for it in items:
        it = (it or "").strip()
        if it and it not in seen:
            out.append(it)
            seen.add(it)
    return out


def _normalize_http_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    if "://" not in u:
        u = "https://" + u
    return u.rstrip("/")


def _normalize_ws_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    if "://" not in u:
        u = "wss://" + u
    return u.rstrip("/")


def _http_to_ws(u: str) -> str:
    u = _normalize_http_url(u)
    if u.startswith("https://"):
        return "wss://" + u[len("https://") :]
    if u.startswith("http://"):
        return "ws://" + u[len("http://") :]
    return u.replace("https:", "wss:").replace("http:", "ws:")


# ──────────────────────────────────────────────────────────────────────────────
# Candidate gathering (backwards compatible)
# ──────────────────────────────────────────────────────────────────────────────
def gather_http_candidates(env=os.environ) -> List[str]:
    primaries = [
        "HEALTHCHECK_RPC_URL",
        "RPC_URL",
        "SOL_RPC_URL",
        "SOLANA_RPC_URL",
        "HELIUS_RPC_URL",
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

    urls: List[str] = []
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


def gather_ws_candidates(env=os.environ) -> List[str]:
    primaries = [
        "HEALTHCHECK_WS_URL",
        "WS_URL",
        "SOL_WS_URL",
        "SOLANA_WS_URL",
        "HELIUS_WS_URL",
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

    urls: List[str] = []
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
# State / scoring
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
        """
        Higher is better.
        We prefer:
          - lower latency (ewma)
          - recently ok
          - fewer fails
          - not in cooldown/eject
        """
        if not self.allowed(tmono):
            return -1e9

        latency = max(1.0, float(self.ewma_ms))
        base = 10000.0 / latency  # 0..10000-ish

        # staleness penalty: if no ok in last 60s, penalize gradually
        if self.last_ok_mono <= 0:
            stale_pen = 1.0
        else:
            age_s = max(0.0, tmono - self.last_ok_mono)
            stale_pen = min(3.0, age_s / 60.0)  # 0..3

        fail_pen = float(self.fail_count) * 2.0
        return float(base - fail_pen - stale_pen)


@dataclass
class _Ban:
    reason: str
    until_mono: float


_LOCK = asyncio.Lock()
_STATES: Dict[str, _RPCState] = {}
_BANS: Dict[str, _Ban] = {}

# Sticky selection memory
_CHOSEN_HTTP: Optional[str] = None
_CHOSEN_WS: Optional[str] = None
_STICKY_UNTIL_HTTP_MONO: float = 0.0
_STICKY_UNTIL_WS_MONO: float = 0.0
_LAST_SWITCH_HTTP_MONO: float = 0.0
_LAST_SWITCH_WS_MONO: float = 0.0
_LAST_PROBE_HTTP_MONO: float = 0.0
_LAST_PROBE_WS_MONO: float = 0.0

# Lazy shared aiohttp session
_SESSION: Optional[aiohttp.ClientSession] = None
_SESSION_LOCK = asyncio.Lock()


def _policy() -> Dict[str, Any]:
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
        "hysteresis_margin": max(0.05, float(_env_float("RPC_HYSTERESIS_MARGIN", 0.20))),  # relative improvement
        "ewma_alpha": min(0.9, max(0.05, float(_env_float("RPC_EWMA_ALPHA", 0.20)))),
        "health_method": (_env_str("RPC_HEALTH_METHOD", "getHealth") or "getHealth").strip(),
        "prefer_env_order": _env_bool("RPC_PREFER_ENV_ORDER", True),
    }


async def _get_session() -> aiohttp.ClientSession:
    global _SESSION
    if _SESSION is not None and not _SESSION.closed:
        return _SESSION
    async with _SESSION_LOCK:
        if _SESSION is not None and not _SESSION.closed:
            return _SESSION
        timeout = aiohttp.ClientTimeout(total=max(1.0, _env_float("RPC_PROBE_TIMEOUT_SEC", 3.5)))
        connector = aiohttp.TCPConnector(
            limit=max(10, _env_int("RPC_AIOHTTP_LIMIT", 50)),
            ttl_dns_cache=60,
            enable_cleanup_closed=True,
        )
        _SESSION = aiohttp.ClientSession(timeout=timeout, connector=connector)
        return _SESSION


async def close_http() -> None:
    """Close shared aiohttp session (optional)."""
    global _SESSION
    if _SESSION is not None and not _SESSION.closed:
        with contextlib.suppress(Exception):
            await _SESSION.close()
    _SESSION = None


def _ensure_state(url: str) -> _RPCState:
    st = _STATES.get(url)
    if st is None:
        st = _RPCState(url=url)
        _STATES[url] = st
    return st


def _is_banned(url: str, tmono: Optional[float] = None) -> bool:
    t = _mono() if tmono is None else float(tmono)
    b = _BANS.get(url)
    if not b:
        return False
    if t >= float(b.until_mono):
        _BANS.pop(url, None)
        return False
    return True


def _apply_static_banlist(candidates: List[str]) -> List[str]:
    raw = _env_str("RPC_BANLIST", "")
    if not raw:
        return candidates
    banned = set(_normalize_http_url(x) for x in _split_list(raw))
    out: List[str] = []
    for u in candidates:
        uu = _normalize_http_url(u)
        if uu and uu not in banned:
            out.append(uu)
    return out


def ban(url: str, reason: str = "", ttl_sec: Optional[int] = None) -> None:
    """Dynamically ban an RPC URL for a time window."""
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


def report_ok(url: str, latency_ms: Optional[float] = None, note: str = "") -> None:
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

    # success gently forgives failure streaks
    if st.fail_count > 0:
        st.fail_count = max(0, st.fail_count - 1)

    # clear cooldown/eject on healthy ok (eject is also cleared; we keep bans separate)
    st.cooldown_until_mono = 0.0
    st.eject_until_mono = 0.0


def report_fail(url: str, note: str = "", *, status: Optional[int] = None) -> None:
    u = _normalize_http_url(url)
    if not u:
        return
    pol = _policy()
    st = _ensure_state(u)

    st.fail_count += 1
    st.last_fail_mono = _mono()
    if note:
        st.note = note

    # cooldown after small cluster of fails
    if st.fail_count >= int(pol["fails_to_cooldown"]):
        st.cooldown_until_mono = max(st.cooldown_until_mono, _mono() + float(pol["fail_cooldown_sec"]))

    # eject after repeated fails (internal; separate from explicit ban())
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
# Probing
# ──────────────────────────────────────────────────────────────────────────────
async def _rpc_call(session: aiohttp.ClientSession, url: str, payload: Dict[str, Any], timeout_sec: float) -> Tuple[int, Any]:
    to = aiohttp.ClientTimeout(total=max(1.0, float(timeout_sec)))
    async with session.post(url, json=payload, timeout=to, headers={"content-type": "application/json"}) as r:
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

    if method == "getHealth":
        res = data.get("result")
        # Common variants: "ok" / {"Ok":{}} / {"ok": true}
        return res == "ok" or res == {"Ok": {}} or res == {"ok": True}

    if method == "getLatestBlockhash":
        res = data.get("result")
        return bool(res)

    return bool(data.get("result"))


async def _probe_http(url: str, timeout: float) -> Tuple[bool, float, str]:
    """
    Returns: (ok, latency_ms, note)
    """
    u = _normalize_http_url(url)
    if not u:
        return False, 9999.0, "bad_url"

    tmono = _mono()
    if _is_banned(u, tmono=tmono):
        return False, 9999.0, "banned"

    st = _ensure_state(u)
    if not st.allowed(tmono):
        return False, 9999.0, "cooldown"

    pol = _policy()
    method = str(pol["health_method"] or "getHealth").strip()
    payload_health: Dict[str, Any] = {"jsonrpc": "2.0", "id": 1, "method": method}
    payload_bh: Dict[str, Any] = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getLatestBlockhash",
        "params": [{"commitment": "processed"}],
    }

    session = await _get_session()

    # small jitter to avoid herd effect
    await asyncio.sleep(random.random() * 0.05)

    t0 = _mono()
    try:
        status, data = await _rpc_call(session, u, payload_health, timeout)
        dt_ms = (_mono() - t0) * 1000.0
        if _is_health_ok(method, status, data):
            return True, float(dt_ms), f"{method}:ok"

        # fallback to getLatestBlockhash (some RPCs block getHealth)
        t1 = _mono()
        status2, data2 = await _rpc_call(session, u, payload_bh, timeout)
        dt_ms2 = (_mono() - t1) * 1000.0
        if _is_health_ok("getLatestBlockhash", status2, data2):
            return True, float(dt_ms2), "latestBlockhash:ok"

        return False, float(max(dt_ms, dt_ms2)), f"unhealthy:{status}/{status2}"
    except asyncio.CancelledError:
        raise
    except Exception as e:
        dt_ms = (_mono() - t0) * 1000.0
        return False, float(dt_ms), f"{type(e).__name__}:{_short(e, 180)}"


# ──────────────────────────────────────────────────────────────────────────────
# Selection
# ──────────────────────────────────────────────────────────────────────────────
def _score_url(url: str, tmono: float) -> float:
    return _ensure_state(url).score(tmono)


def _pick_subset(candidates: List[str], max_n: int) -> List[str]:
    # preserve order but cap length (probing too many is wasteful)
    return candidates[: max(1, int(max_n))]


async def _select_best_http(candidates: List[str], *, force_probe: bool) -> Tuple[str, List[Tuple[str, bool, str, float]]]:
    """
    Returns: (chosen_url, notes) where notes entries are (url, ok, note, latency_ms)
    """
    global _CHOSEN_HTTP, _STICKY_UNTIL_HTTP_MONO, _LAST_SWITCH_HTTP_MONO, _LAST_PROBE_HTTP_MONO

    pol = _policy()
    tmono = _mono()

    # sanitize candidates
    candidates = [_normalize_http_url(u) for u in candidates if u]
    candidates = _dedupe_preserve([u for u in candidates if u])

    # apply static banlist from env
    candidates = _apply_static_banlist(candidates)

    # filter dynamic bans
    candidates = [u for u in candidates if not _is_banned(u, tmono=tmono)]
    if not candidates:
        candidates = [PUBLIC_HTTP]

    # sticky: if still within sticky window, return current (no probing)
    if (
        (not force_probe)
        and _CHOSEN_HTTP
        and (tmono < float(_STICKY_UNTIL_HTTP_MONO))
        and (_normalize_http_url(_CHOSEN_HTTP) in candidates)
    ):
        return _CHOSEN_HTTP, [(_CHOSEN_HTTP, True, "sticky", 0.0)]

    # if we have a chosen and it's currently allowed, we can throttle probes
    if _CHOSEN_HTTP and (not force_probe):
        chosen_norm = _normalize_http_url(_CHOSEN_HTTP)
        if chosen_norm in candidates:
            st = _ensure_state(chosen_norm)
            if st.allowed(tmono):
                # too soon since last probe? keep chosen
                if (tmono - float(_LAST_PROBE_HTTP_MONO)) < float(pol["recheck_sec"]):
                    _STICKY_UNTIL_HTTP_MONO = tmono + float(pol["sticky_ttl_sec"])
                    return chosen_norm, [(chosen_norm, True, "recheck_throttled", 0.0)]

    # commit probe time
    _LAST_PROBE_HTTP_MONO = tmono

    # probe a capped subset (preserve env order)
    subset = _pick_subset(candidates, int(pol["max_probe_candidates"]))
    timeout = float(pol["probe_timeout_sec"])
    sem = asyncio.Semaphore(int(pol["probe_parallel"]))

    async def _one(u: str) -> Tuple[str, bool, str, float]:
        uu = _normalize_http_url(u)
        if not uu:
            return u, False, "bad_url", 9999.0
        async with sem:
            ok, ms, note = await _probe_http(uu, timeout=timeout)
            return uu, ok, note, float(ms)

    tasks = [asyncio.create_task(_one(u)) for u in subset]
    notes: List[Tuple[str, bool, str, float]] = []

    for coro in asyncio.as_completed(tasks):
        u, ok, note, ms = await coro
        notes.append((u, ok, note, ms))
        if ok:
            report_ok(u, latency_ms=ms, note=note)
        else:
            report_fail(u, note=note)

    for tsk in tasks:
        if not tsk.done():
            tsk.cancel()

    ok_urls = [u for (u, ok, _note, _ms) in notes if ok]
    ok_urls = _dedupe_preserve(ok_urls)

    if not ok_urls:
        # If nothing is OK, keep current if it exists in candidates; else last resort.
        if _CHOSEN_HTTP and _normalize_http_url(_CHOSEN_HTTP) in candidates:
            _STICKY_UNTIL_HTTP_MONO = tmono + float(pol["sticky_ttl_sec"])
            return _normalize_http_url(_CHOSEN_HTTP), notes
        return candidates[-1], notes

    # choose highest score among OK urls
    ok_urls.sort(key=lambda u: _score_url(u, tmono), reverse=True)
    winner = ok_urls[0]

    # hysteresis: avoid switching away from current unless meaningfully better
    if _CHOSEN_HTTP:
        cur = _normalize_http_url(_CHOSEN_HTTP)
        if cur and cur in candidates:
            cur_st = _ensure_state(cur)

            if cur_st.allowed(tmono):
                # min switch interval
                if (tmono - float(_LAST_SWITCH_HTTP_MONO)) < float(pol["min_switch_sec"]):
                    winner = cur
                else:
                    if winner != cur:
                        cur_score = cur_st.score(tmono)
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


async def _select_best_ws(ws_candidates: List[str], http_chosen: str, *, force_probe: bool) -> Tuple[str, List[Tuple[str, bool, str, float]]]:
    """
    WS endpoints are not probed via websocket handshake here (keeps it cheap).
    Policy:
      - If WS_URL (or SOLANA_WS_URL/HELIUS_WS_URL) explicitly set, prefer it.
      - Else derive from chosen HTTP.
      - Else first candidate/public.
    """
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

    # sticky
    if (
        (not force_probe)
        and _CHOSEN_WS
        and (tmono < float(_STICKY_UNTIL_WS_MONO))
        and (_normalize_ws_url(_CHOSEN_WS) in ws_candidates)
    ):
        return _CHOSEN_WS, [(_CHOSEN_WS, True, "sticky", 0.0)]

    # throttle
    if _CHOSEN_WS and (not force_probe):
        if (tmono - float(_LAST_PROBE_WS_MONO)) < float(pol["recheck_sec"]):
            _STICKY_UNTIL_WS_MONO = tmono + float(pol["sticky_ttl_sec"])
            return _CHOSEN_WS, [(_CHOSEN_WS, True, "recheck_throttled", 0.0)]

    _LAST_PROBE_WS_MONO = tmono

    explicit = _normalize_ws_url(
        _env_str(
            "WS_URL",
            _env_str("SOLANA_WS_URL", _env_str("SOL_WS_URL", _env_str("HELIUS_WS_URL", ""))),
        )
    )

    if explicit and explicit in ws_candidates:
        winner = explicit
    else:
        derived = _http_to_ws(http_chosen)
        if derived:
            winner = _normalize_ws_url(derived)
        else:
            winner = ws_candidates[0] if ws_candidates else PUBLIC_WS

    # no thrash: min switch interval for WS too
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
# Public async getters
# ──────────────────────────────────────────────────────────────────────────────
async def get_http_url(force_probe: bool = False) -> str:
    async with _LOCK:
        candidates = gather_http_candidates()
    chosen, _ = await _select_best_http(candidates, force_probe=bool(force_probe))
    return chosen


async def get_ws_url(force_probe: bool = False) -> str:
    http_chosen = await get_http_url(force_probe=bool(force_probe))
    async with _LOCK:
        ws_candidates = gather_ws_candidates()
    ws, _ = await _select_best_ws(ws_candidates, http_chosen, force_probe=bool(force_probe))
    return ws


# ──────────────────────────────────────────────────────────────────────────────
# Backwards-compatible helpers
# ──────────────────────────────────────────────────────────────────────────────
async def resolve_http_first_alive(
    candidates: Optional[List[str]] = None,
    timeout: float = 3.5,
) -> Tuple[str, List[Tuple[str, bool, str]]]:
    """
    Backwards compatible: returns (chosen_url, notes)
    notes: list[(url, ok, why)]
    """
    if candidates is None:
        candidates = gather_http_candidates()

    # Temporarily honor provided timeout via env override for probe only.
    old = os.getenv("RPC_PROBE_TIMEOUT_SEC")
    os.environ["RPC_PROBE_TIMEOUT_SEC"] = str(timeout)
    try:
        chosen, notes4 = await _select_best_http([_normalize_http_url(u) for u in candidates], force_probe=True)
        notes3 = [(u, ok, why) for (u, ok, why, _ms) in notes4]
        return chosen, notes3
    finally:
        if old is None:
            os.environ.pop("RPC_PROBE_TIMEOUT_SEC", None)
        else:
            os.environ["RPC_PROBE_TIMEOUT_SEC"] = old


def pick_http_url_sync(timeout: float = 3.5) -> str:
    """
    Sync helper (avoid using inside an event loop).
    """
    try:
        _ = asyncio.get_running_loop()
        raise RuntimeError("pick_http_url_sync called inside running event loop; use await get_http_url()")
    except RuntimeError as e:
        if "no running event loop" not in str(e).lower():
            raise
    return asyncio.run(get_http_url(force_probe=True))


async def apply_failover_to_env(
    timeout_s: Optional[float] = None,
    timeout: Optional[float] = None,
    *,
    env=os.environ,
) -> Tuple[str, Any]:
    """
    Picks best HTTP and writes env["RPC_URL"] + env["SOL_RPC_URL"] (and best-effort WS_URL).
    Returns (chosen, notes). Notes are backwards compatible with older callers (list-of-tuples),
    plus extra diagnostics in a dict if you want them.
    """
    # accept either arg name
    to = float(timeout_s if timeout_s is not None else (timeout if timeout is not None else 3.5))

    chosen, notes3 = await resolve_http_first_alive(timeout=to)

    # write both common names for downstream compatibility
    env["RPC_URL"] = str(chosen)
    env["SOL_RPC_URL"] = str(chosen)

    # best-effort WS update (cheap, no probing)
    ws = ""
    with contextlib.suppress(Exception):
        ws = await get_ws_url(force_probe=True)
        if ws:
            env["WS_URL"] = str(ws)

    # include richer notes without breaking old consumers
    rich = {
        "chosen_http": chosen,
        "chosen_ws": ws or None,
        "notes": notes3,
        "policy": _policy(),
    }
    return chosen, rich


# ──────────────────────────────────────────────────────────────────────────────
# Diagnostics
# ──────────────────────────────────────────────────────────────────────────────
def stats() -> Dict[str, Any]:
    pol = _policy()
    tmono = _mono()

    states: Dict[str, Any] = {}
    for u, s in _STATES.items():
        states[u] = {
            "ewma_ms": float(s.ewma_ms),
            "ok": int(s.ok_count),
            "fail": int(s.fail_count),
            "last_ok_age_s": (None if s.last_ok_mono <= 0 else float(max(0.0, tmono - s.last_ok_mono))),
            "last_fail_age_s": (None if s.last_fail_mono <= 0 else float(max(0.0, tmono - s.last_fail_mono))),
            "cooldown_left_s": float(max(0.0, s.cooldown_until_mono - tmono)),
            "eject_left_s": float(max(0.0, s.eject_until_mono - tmono)),
            "note": s.note,
            "score": float(s.score(tmono)),
            "banned": bool(_is_banned(u, tmono=tmono)),
        }

    bans = {u: {"reason": b.reason, "ttl_left_s": float(max(0.0, b.until_mono - tmono))} for u, b in _BANS.items()}

    return {
        "policy": pol,
        "chosen_http": _CHOSEN_HTTP,
        "chosen_ws": _CHOSEN_WS,
        "sticky_left_http_s": float(max(0.0, _STICKY_UNTIL_HTTP_MONO - tmono)),
        "sticky_left_ws_s": float(max(0.0, _STICKY_UNTIL_WS_MONO - tmono)),
        "last_switch_http_age_s": (None if _LAST_SWITCH_HTTP_MONO <= 0 else float(max(0.0, tmono - _LAST_SWITCH_HTTP_MONO))),
        "last_switch_ws_age_s": (None if _LAST_SWITCH_WS_MONO <= 0 else float(max(0.0, tmono - _LAST_SWITCH_WS_MONO))),
        "last_probe_http_age_s": (None if _LAST_PROBE_HTTP_MONO <= 0 else float(max(0.0, tmono - _LAST_PROBE_HTTP_MONO))),
        "last_probe_ws_age_s": (None if _LAST_PROBE_WS_MONO <= 0 else float(max(0.0, tmono - _LAST_PROBE_WS_MONO))),
        "states": states,
        "bans": bans,
        "ts": _wall(),
    }


__all__ = [
    "PUBLIC_HTTP",
    "PUBLIC_WS",
    "gather_http_candidates",
    "gather_ws_candidates",
    "get_http_url",
    "get_ws_url",
    "resolve_http_first_alive",
    "pick_http_url_sync",
    "apply_failover_to_env",
    "report_ok",
    "report_fail",
    "ban",
    "unban",
    "stats",
    "close_http",
]
