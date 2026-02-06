from __future__ import annotations

"""
utils_pkg/onchain_scanner.py

God-Tier Onchain Scanner — V3+++ (production lock-in)
=====================================================

Watches Solana program logs via WebSocket logsSubscribe and yields structured
events (OnchainLogEvent). Designed for Pump.fun / launch programs, but works
for any Solana program id.

Key features
------------
- Robust WS reconnect loop (exp backoff + jitter)
- logsSubscribe with mentions filter (multi-program)
- Correct mapping: logsNotification.params.subscription -> program_id
- Signature dedup (TTL)
- Mint parsing from logs + optional getTransaction RPC resolution
- Filters out WSOL / stable mints by default
- Async generator API + backwards-compatible callback loop

Env
---
ONCHAIN_ENABLED=1
SOL_RPC_URL=...
SOL_WS_URL=...                  (optional; derived from SOL_RPC_URL)
ONCHAIN_COMMITMENT=confirmed
ONCHAIN_MAX_SIZE=10000000
ONCHAIN_PING_INTERVAL=20
ONCHAIN_PING_TIMEOUT=20

ONCHAIN_RECONNECT_BASE_BACKOFF=0.6
ONCHAIN_RECONNECT_MAX_BACKOFF=30
ONCHAIN_DEDUP_TTL_SEC=900

ONCHAIN_KEYWORDS=Initialize,Create,Bonding,launch,mint   (comma-separated; optional)
ONCHAIN_REQUIRE_KEYWORDS=1                              (if 1, skip events with no hits)

# Programs
PUMP_PROGRAM_ID=...             (single)
ONCHAIN_PROGRAM_IDS=...         (comma-separated list; overrides PUMP_PROGRAM_ID)

# Mint resolution via RPC (when logs omit mint)
ONCHAIN_RESOLVE_MINT_VIA_RPC=1
ONCHAIN_RPC_TIMEOUT_TOTAL=12
ONCHAIN_RPC_TIMEOUT_CONNECT=4
ONCHAIN_RPC_RETRIES=1
ONCHAIN_RPC_BACKOFF_BASE=0.35

# Mint filtering (recommended defaults)
ONCHAIN_REQUIRE_NON_WSOL_MINT=1
ONCHAIN_EXCLUDE_MINTS=So11111111111111111111111111111111111111112,EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v,Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB
"""

import asyncio
import contextlib
import inspect
import json
import os
import random
import re
import time
from collections.abc import AsyncGenerator, Iterable
from dataclasses import dataclass
from typing import Any

import aiohttp
import websockets


# -----------------------------------------------------------------------------
# Logging shim (never hard-fail)
# -----------------------------------------------------------------------------
def _get_logger() -> Any:
    # Prefer the project-wide compat shim if present.
    try:  # pragma: no cover
        from utils_pkg.log_compat import get_logger as _gl  # type: ignore

        return _gl(__name__)
    except Exception:
        pass

    # Fall back to older name if repo still uses it.
    try:  # pragma: no cover
        from logging_setup import get_logger as _gl2  # type: ignore

        return _gl2(__name__)
    except Exception:
        pass

    import logging

    return logging.getLogger(__name__)


log: Any = _get_logger()

_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _log_event(name: str, **kw: Any) -> None:
    with contextlib.suppress(Exception):
        ev = getattr(log, "event", None)
        if callable(ev):
            ev(name, **kw)
        else:
            # Fallback: standard logger
            msg = f"{name} {kw}"
            getattr(log, "info", print)(msg)


def _short_exc(e: BaseException, n: int = 240) -> str:
    s = f"{type(e).__name__}: {e}"
    return (s[:n] + "…") if len(s) > n else s


# -----------------------------------------------------------------------------
# Env helpers
# -----------------------------------------------------------------------------
def _env_str(k: str, d: str = "") -> str:
    v = os.getenv(k)
    return d if v is None else str(v).strip()


def _env_int(k: str, d: int) -> int:
    try:
        return int(float(_env_str(k, str(d))))
    except Exception:
        return d


def _env_float(k: str, d: float) -> float:
    try:
        return float(_env_str(k, str(d)))
    except Exception:
        return d


def _env_bool(k: str, d: bool) -> bool:
    v = os.getenv(k)
    if v is None:
        return d
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return d


def _split_csv(s: str) -> list[str]:
    return [x.strip() for x in (s or "").replace(";", ",").split(",") if x.strip()]


# -----------------------------------------------------------------------------
# URL helpers
# -----------------------------------------------------------------------------
def derive_ws_url(http_url: str) -> str:
    """
    Accept either ws/wss or http/https and coerce to wss.
    """
    u = (http_url or "").strip()
    if not u:
        return ""
    if u.startswith("wss://"):
        return u
    if u.startswith("ws://"):
        return "wss://" + u[len("ws://") :]
    if u.startswith("https://"):
        return "wss://" + u[len("https://") :]
    if u.startswith("http://"):
        return "wss://" + u[len("http://") :]
    return "wss://" + u


# -----------------------------------------------------------------------------
# Mint extraction
# -----------------------------------------------------------------------------
PUBKEY_RE = r"([1-9A-HJ-NP-Za-km-z]{32,44})"

_MINT_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(rf"(?i)\bmint[:=\s]+{PUBKEY_RE}"),
    re.compile(rf"(?i)\btoken mint[:=\s]+{PUBKEY_RE}"),
    re.compile(rf"(?i)\bcreated mint[:=\s]+{PUBKEY_RE}"),
    re.compile(rf"(?i)\bMint[:=\s]+{PUBKEY_RE}"),
    re.compile(rf"(?i)\bInitializeMint\w*\b.*?{PUBKEY_RE}"),
)


def _extract_mint_from_logs(logs: Iterable[str]) -> str | None:
    logs_list = list(logs)
    if not logs_list:
        return None

    text = "\n".join(logs_list)

    for pat in _MINT_PATTERNS:
        m = pat.search(text)
        if m:
            return m.group(1)

    # line-by-line heuristic
    for line in logs_list:
        if "mint" not in str(line).lower():
            continue
        m2 = re.search(PUBKEY_RE, str(line))
        if m2:
            return m2.group(1)

    return None


# -----------------------------------------------------------------------------
# Defaults: exclude WSOL + common stables unless you override
# -----------------------------------------------------------------------------
WSOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"


def _exclude_mints() -> set[str]:
    raw = _env_str("ONCHAIN_EXCLUDE_MINTS", f"{WSOL_MINT},{USDC_MINT},{USDT_MINT}")
    return set(_split_csv(raw))


def _is_excluded_mint(mint: str | None, exclude: set[str]) -> bool:
    if not mint:
        return True
    return mint in exclude


# -----------------------------------------------------------------------------
# Keywords
# -----------------------------------------------------------------------------
def _keywords() -> list[str]:
    return _split_csv(_env_str("ONCHAIN_KEYWORDS", "Initialize,Create,Bonding,launch,mint"))


def _hit_keywords(logs: Iterable[str], kws: list[str]) -> list[str]:
    if not kws:
        return []
    text = "\n".join([str(x) for x in logs]).lower()
    return [k for k in kws if k.lower() in text]


# -----------------------------------------------------------------------------
# Program IDs
# -----------------------------------------------------------------------------
def _program_ids() -> list[str]:
    pids = _split_csv(_env_str("ONCHAIN_PROGRAM_IDS", ""))
    if pids:
        return pids
    single = _env_str("PUMP_PROGRAM_ID", "")
    return [single] if single else []


# -----------------------------------------------------------------------------
# Data model
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class OnchainLogEvent:
    program_id: str
    signature: str
    slot: int
    ts: int
    logs: tuple[str, ...]
    mint: str | None
    keywords_hit: tuple[str, ...]
    raw: dict[str, Any]


# -----------------------------------------------------------------------------
# Signature dedup
# -----------------------------------------------------------------------------
class _SigDedup:
    def __init__(self, ttl_sec: int) -> None:
        self.ttl = max(30, int(ttl_sec))
        self._m: dict[str, int] = {}  # sig -> expires_ts

    def seen(self, sig: str, now_ts: int | None = None) -> bool:
        now = int(now_ts or time.time())
        exp = self._m.get(sig)
        if exp is not None and exp > now:
            return True
        self._m[sig] = now + self.ttl

        # cheap prune
        if len(self._m) > 7000:
            cutoff = now
            for k in list(self._m.keys())[:3000]:
                if self._m.get(k, 0) <= cutoff:
                    self._m.pop(k, None)
        return False


# -----------------------------------------------------------------------------
# RPC client (shared session)
# -----------------------------------------------------------------------------
class _RpcClient:
    def __init__(self, rpc_http: str) -> None:
        self.rpc_http = (rpc_http or "").strip()
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session and not self._session.closed:
            return self._session
        timeout_total = max(6, _env_int("ONCHAIN_RPC_TIMEOUT_TOTAL", 12))
        timeout_connect = max(2, _env_int("ONCHAIN_RPC_TIMEOUT_CONNECT", 4))
        timeout = aiohttp.ClientTimeout(total=timeout_total, connect=timeout_connect)
        self._session = aiohttp.ClientSession(
            timeout=timeout,
            headers={"user-agent": "pnlsniper/1.0 (+onchain_scanner)"},
        )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def post_json(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        if not self.rpc_http:
            return None

        retries = max(0, _env_int("ONCHAIN_RPC_RETRIES", 1))
        backoff_base = max(0.1, _env_float("ONCHAIN_RPC_BACKOFF_BASE", 0.35))
        sess = await self._get_session()

        for attempt in range(retries + 1):
            try:
                async with sess.post(self.rpc_http, json=payload) as r:
                    if r.status == 200:
                        data = await r.json(content_type=None)
                        return data if isinstance(data, dict) else None

                    # retry on 429/5xx
                    if r.status in (429, 500, 502, 503, 504) and attempt < retries:
                        await asyncio.sleep(
                            backoff_base * (attempt + 1) + random.uniform(0.0, 0.25)
                        )
                        continue

                    return None
            except Exception:
                if attempt < retries:
                    await asyncio.sleep(backoff_base * (attempt + 1) + random.uniform(0.0, 0.25))
                    continue
                return None
        return None


def _mint_from_gettx(resp: dict[str, Any] | None, exclude: set[str]) -> str | None:
    """
    Best-effort mint inference from getTransaction(jsonParsed):
      - Prefer mints in postTokenBalances that were NOT in preTokenBalances
      - Skip excluded mints (WSOL/stables)
      - Fallback to first non-excluded mint in postTokenBalances
    """
    if not resp or "result" not in resp:
        return None
    r = resp.get("result")
    if not isinstance(r, dict):
        return None

    meta = r.get("meta") or {}
    if not isinstance(meta, dict):
        return None

    pre = meta.get("preTokenBalances") or []
    post = meta.get("postTokenBalances") or []

    pre_mints: set[str] = set()
    if isinstance(pre, list):
        for b in pre:
            if isinstance(b, dict):
                m = b.get("mint")
                if isinstance(m, str) and m:
                    pre_mints.add(m)

    post_mints: list[str] = []
    if isinstance(post, list):
        for b in post:
            if isinstance(b, dict):
                m = b.get("mint")
                if isinstance(m, str) and m:
                    post_mints.append(m)

    for m in post_mints:
        if m not in pre_mints and m not in exclude:
            return m
    for m in post_mints:
        if m not in exclude:
            return m
    return None


async def _resolve_mint_via_rpc(
    sig: str, rpc: _RpcClient, commitment: str, exclude: set[str]
) -> str | None:
    payload: dict[str, Any] = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [
            sig,
            {
                "encoding": "jsonParsed",
                "commitment": commitment,
                "maxSupportedTransactionVersion": 0,
            },
        ],
    }
    resp = await rpc.post_json(payload)
    return _mint_from_gettx(resp, exclude)


# -----------------------------------------------------------------------------
# Core stream
# -----------------------------------------------------------------------------
def _jitter_sleep(base_backoff: float, max_backoff: float) -> tuple[float, float]:
    b = max(0.1, float(base_backoff))
    m = max(b, float(max_backoff))
    wait_s = min(m, b + random.uniform(0.0, b))
    next_b = min(m, b * 1.6)
    return wait_s, next_b


async def stream_events(
    *,
    rpc_http: str,
    rpc_ws: str,
    program_ids: list[str],
    commitment: str = "confirmed",
) -> AsyncGenerator[OnchainLogEvent, None]:
    """
    Async generator of OnchainLogEvent across one or more program IDs.
    """
    if not _env_bool("ONCHAIN_ENABLED", True):
        return

    rpc_ws = (rpc_ws or "").strip()
    rpc_http = (rpc_http or "").strip()
    program_ids = [p.strip() for p in (program_ids or []) if p and p.strip()]

    if not rpc_ws:
        raise RuntimeError("SOL_WS_URL (or derivation from SOL_RPC_URL) is empty")
    if not program_ids:
        raise RuntimeError("No program IDs set (PUMP_PROGRAM_ID or ONCHAIN_PROGRAM_IDS)")

    ping_interval = max(5, _env_int("ONCHAIN_PING_INTERVAL", 20))
    ping_timeout = max(5, _env_int("ONCHAIN_PING_TIMEOUT", 20))
    max_size = max(1_000_000, _env_int("ONCHAIN_MAX_SIZE", 10_000_000))

    base_backoff = max(0.2, _env_float("ONCHAIN_RECONNECT_BASE_BACKOFF", 0.6))
    max_backoff = max(base_backoff, _env_float("ONCHAIN_RECONNECT_MAX_BACKOFF", 30.0))

    dedup = _SigDedup(_env_int("ONCHAIN_DEDUP_TTL_SEC", 900))
    kws = _keywords()
    require_keywords = _env_bool("ONCHAIN_REQUIRE_KEYWORDS", True)

    exclude = _exclude_mints()
    require_non_wsol = _env_bool("ONCHAIN_REQUIRE_NON_WSOL_MINT", True)

    resolve_via_rpc = _env_bool("ONCHAIN_RESOLVE_MINT_VIA_RPC", True) and bool(rpc_http)
    rpc_client = _RpcClient(rpc_http) if resolve_via_rpc else None

    _log_event(
        "ONCHAIN_START",
        rpc_ws=rpc_ws,
        rpc_http=bool(rpc_http),
        program_ids=program_ids,
        commitment=commitment,
        resolve_via_rpc=bool(rpc_client),
        require_keywords=require_keywords,
        require_non_wsol=require_non_wsol,
    )

    try:
        while True:
            try:
                async with websockets.connect(
                    rpc_ws,
                    max_size=max_size,
                    ping_interval=ping_interval,
                    ping_timeout=ping_timeout,
                    open_timeout=10,
                    close_timeout=5,
                ) as ws:
                    # Map request id -> pid, then ack result subscription -> pid
                    reqid_to_pid: dict[int, str] = {}
                    subid_to_pid: dict[int, str] = {}

                    req_id = 1
                    for pid in program_ids:
                        reqid_to_pid[req_id] = pid
                        sub = {
                            "jsonrpc": "2.0",
                            "id": req_id,
                            "method": "logsSubscribe",
                            "params": [{"mentions": [pid]}, {"commitment": commitment}],
                        }
                        await ws.send(json.dumps(sub))
                        req_id += 1

                    # Read acks until we have one subscription per program
                    needed = len(program_ids)
                    while needed > 0:
                        raw = await ws.recv()
                        try:
                            ack = json.loads(raw)
                        except Exception:
                            continue

                        if not isinstance(ack, dict):
                            continue
                        if "error" in ack:
                            raise RuntimeError(f"logsSubscribe error: {ack}")

                        rid = ack.get("id")
                        sub_id = ack.get("result")
                        if isinstance(rid, int) and isinstance(sub_id, int):
                            pid = reqid_to_pid.get(rid, "")
                            if pid:
                                subid_to_pid[int(sub_id)] = pid
                                needed -= 1

                    _log_event(
                        "ONCHAIN_SUBSCRIBED",
                        program_ids=program_ids,
                        ws=rpc_ws,
                        commitment=commitment,
                        subscriptions=subid_to_pid,
                    )

                    # reset backoff on successful connect
                    base_backoff = max(0.2, _env_float("ONCHAIN_RECONNECT_BASE_BACKOFF", 0.6))

                    while True:
                        raw = await ws.recv()
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        if not isinstance(msg, dict):
                            continue
                        if msg.get("method") != "logsNotification":
                            continue

                        params = msg.get("params")
                        if not isinstance(params, dict):
                            continue

                        sub_id = params.get("subscription")
                        pid = subid_to_pid.get(int(sub_id), "") if isinstance(sub_id, int) else ""

                        result = params.get("result")
                        if not isinstance(result, dict):
                            continue

                        ctx = result.get("context") or {}
                        slot = int(ctx.get("slot") or 0) if isinstance(ctx, dict) else 0

                        value = result.get("value")
                        if not isinstance(value, dict):
                            continue

                        logs_any = value.get("logs") or []
                        if not isinstance(logs_any, list):
                            continue
                        logs_list = [str(x) for x in logs_any]

                        sig = str(value.get("signature") or "").strip()
                        if not sig:
                            continue
                        if dedup.seen(sig):
                            continue

                        hits = _hit_keywords(logs_list, kws)
                        if require_keywords and kws and not hits:
                            continue

                        mint = _extract_mint_from_logs(logs_list)
                        if mint in exclude:
                            mint = None

                        if not mint and rpc_client is not None:
                            try:
                                mint = await _resolve_mint_via_rpc(
                                    sig, rpc_client, commitment, exclude
                                )
                            except Exception:
                                mint = None

                        if require_non_wsol and _is_excluded_mint(mint, exclude):
                            continue

                        yield OnchainLogEvent(
                            program_id=pid,
                            signature=sig,
                            slot=slot,
                            ts=int(time.time()),
                            logs=tuple(logs_list),
                            mint=mint,
                            keywords_hit=tuple(hits),
                            raw=msg,
                        )

            except asyncio.CancelledError:
                raise
            except Exception as e:
                wait_s, base_backoff = _jitter_sleep(base_backoff, max_backoff)
                _log_event("ONCHAIN_RECONNECT", err=_short_exc(e), sleep_s=round(wait_s, 2))
                await asyncio.sleep(wait_s)
                continue

    finally:
        if rpc_client is not None:
            with contextlib.suppress(Exception):
                await rpc_client.close()


async def onchain_scan(loop_cb: Any) -> None:
    """
    Backwards-compatible callback loop.
    Calls loop_cb(event_dict) for each event.

    loop_cb may be sync or async; we detect awaitables safely.
    """
    rpc_http = _env_str("SOL_RPC_URL", "").strip()
    rpc_ws = _env_str("SOL_WS_URL", "").strip() or derive_ws_url(rpc_http)
    program_ids = _program_ids()
    commitment = _env_str("ONCHAIN_COMMITMENT", "confirmed") or "confirmed"

    if not rpc_ws or not program_ids:
        raise SystemExit(
            "Set SOL_RPC_URL (or SOL_WS_URL) and PUMP_PROGRAM_ID or ONCHAIN_PROGRAM_IDS in env"
        )

    async for ev in stream_events(
        rpc_http=rpc_http,
        rpc_ws=rpc_ws,
        program_ids=program_ids,
        commitment=commitment,
    ):
        payload: dict[str, Any] = {
            "slot": ev.slot,
            "signature": ev.signature,
            "mint": ev.mint,
            "program_id": ev.program_id,
            "logs": list(ev.logs),
            "ts": ev.ts,
            "keywords_hit": list(ev.keywords_hit),
            "raw": ev.raw,
        }
        res = loop_cb(payload)
        if inspect.isawaitable(res):
            await res


__all__ = ["OnchainLogEvent", "derive_ws_url", "onchain_scan", "stream_events"]
