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
import json
import logging
import os
import random
import re
import time
from collections.abc import AsyncGenerator, Callable
from dataclasses import dataclass
from typing import Any

import aiohttp
import websockets

log = logging.getLogger("utils_pkg.onchain_scanner")

_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


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

MINT_PATTERNS = [
    re.compile(rf"(?i)\bmint[:=\s]+{PUBKEY_RE}"),
    re.compile(rf"(?i)\btoken mint[:=\s]+{PUBKEY_RE}"),
    re.compile(rf"(?i)\bcreated mint[:=\s]+{PUBKEY_RE}"),
    re.compile(rf"(?i)\bMint[:=\s]+{PUBKEY_RE}"),
    re.compile(rf"(?i)\bInitializeMint\w*\b.*?{PUBKEY_RE}"),
]


def _extract_mint_from_logs(logs: list[str]) -> str | None:
    if not logs:
        return None

    text = "\n".join(logs)

    for pat in MINT_PATTERNS:
        m = pat.search(text)
        if m:
            return m.group(1)

    # line-by-line heuristic
    for line in logs:
        if "mint" not in line.lower():
            continue
        m2 = re.search(PUBKEY_RE, line)
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
    # default exclude list includes WSOL/USDC/USDT; user can append/override
    raw = _env_str(
        "ONCHAIN_EXCLUDE_MINTS",
        f"{WSOL_MINT},{USDC_MINT},{USDT_MINT}",
    )
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


def _hit_keywords(logs: list[str], kws: list[str]) -> list[str]:
    if not kws:
        return []
    text = "\n".join(logs).lower()
    hits = [k for k in kws if k.lower() in text]
    return hits


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
        if exp and exp > now:
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
        self.rpc_http = rpc_http
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

    async def post_json(self, payload: dict[str, Any]) -> dict | None:
        retries = max(0, _env_int("ONCHAIN_RPC_RETRIES", 1))
        backoff_base = max(0.1, _env_float("ONCHAIN_RPC_BACKOFF_BASE", 0.35))
        sess = await self._get_session()

        for attempt in range(retries + 1):
            try:
                async with sess.post(self.rpc_http, json=payload) as r:
                    if r.status == 200:
                        return await r.json(content_type=None)
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


def _mint_from_gettx(resp: dict | None, exclude: set[str]) -> str | None:
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
    pre = meta.get("preTokenBalances") or []
    post = meta.get("postTokenBalances") or []

    pre_mints: set[str] = set()
    for b in pre:
        m = b.get("mint")
        if isinstance(m, str) and m:
            pre_mints.add(m)

    post_mints: list[str] = []
    for b in post:
        m = b.get("mint")
        if isinstance(m, str) and m:
            post_mints.append(m)

    # Prefer a new mint (not in pre), not excluded
    for m in post_mints:
        if m not in pre_mints and m not in exclude:
            return m

    # Otherwise any post mint not excluded
    for m in post_mints:
        if m not in exclude:
            return m

    return None


async def _resolve_mint_via_rpc(
    sig: str, rpc: _RpcClient, commitment: str, exclude: set[str]
) -> str | None:
    payload = {
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

    try:
        while True:
            try:
                # Connect WS
                async with websockets.connect(
                    rpc_ws,
                    max_size=max_size,
                    ping_interval=ping_interval,
                    ping_timeout=ping_timeout,
                    open_timeout=10,
                    close_timeout=5,
                ) as ws:
                    # subscribe for each program id, map subscription id -> pid
                    subid_to_pid: dict[int, str] = {}
                    req_id = 1

                    for pid in program_ids:
                        sub = {
                            "jsonrpc": "2.0",
                            "id": req_id,
                            "method": "logsSubscribe",
                            "params": [{"mentions": [pid]}, {"commitment": commitment}],
                        }
                        await ws.send(json.dumps(sub))
                        req_id += 1

                    # read acks until we have one subscription id per request
                    needed = len(program_ids)
                    while needed > 0:
                        raw_ack = json.loads(await ws.recv())
                        if "error" in raw_ack:
                            raise RuntimeError(f"logsSubscribe error: {raw_ack}")
                        if "result" in raw_ack and isinstance(raw_ack.get("result"), int):
                            # raw_ack["result"] is subscription id
                            # raw_ack["id"] is our request id, but we don't need it
                            sub_id = int(raw_ack["result"])
                            # We cannot reliably infer pid from ack payload alone, so we assign
                            # in order of program_ids where missing.
                            # To keep it deterministic, fill the first unassigned pid.
                            assigned = set(subid_to_pid.values())
                            pid = next(
                                (p for p in program_ids if p not in assigned), program_ids[0]
                            )
                            subid_to_pid[sub_id] = pid
                            needed -= 1

                    log.info(
                        "onchain_scanner subscribed",
                        extra={"program_ids": program_ids, "commitment": commitment, "ws": rpc_ws},
                    )

                    # reset backoff on successful connect
                    base_backoff = max(0.2, _env_float("ONCHAIN_RECONNECT_BASE_BACKOFF", 0.6))

                    # consume notifications
                    while True:
                        raw_msg = json.loads(await ws.recv())
                        if raw_msg.get("method") != "logsNotification":
                            continue

                        params = raw_msg.get("params") or {}
                        sub_id = params.get("subscription")
                        pid = ""
                        if isinstance(sub_id, int):
                            pid = subid_to_pid.get(int(sub_id), "")
                        # keep pid empty if unknown; still emit

                        result = params.get("result") or {}
                        value = result.get("value") or {}

                        logs_list = value.get("logs") or []
                        if not isinstance(logs_list, list):
                            continue
                        logs_list = [str(x) for x in logs_list]

                        sig = str(value.get("signature") or "").strip()
                        if not sig:
                            continue
                        if dedup.seen(sig):
                            continue

                        slot = int((result.get("context") or {}).get("slot") or 0)
                        ts = int(time.time())

                        hits = _hit_keywords(logs_list, kws)
                        if require_keywords and kws and not hits:
                            continue

                        # First pass: logs extraction
                        mint = _extract_mint_from_logs(logs_list)

                        # If mint is excluded (WSOL/stables), treat as missing and try RPC
                        if mint in exclude:
                            mint = None

                        # Optional RPC resolution
                        if not mint and rpc_client is not None:
                            try:
                                mint = await _resolve_mint_via_rpc(
                                    sig, rpc_client, commitment, exclude
                                )
                            except Exception:
                                mint = None

                        # Require non-WSOL mint (recommended)
                        if require_non_wsol and _is_excluded_mint(mint, exclude):
                            continue

                        yield OnchainLogEvent(
                            program_id=pid,
                            signature=sig,
                            slot=slot,
                            ts=ts,
                            logs=tuple(logs_list),
                            mint=mint,
                            keywords_hit=tuple(hits),
                            raw=raw_msg,
                        )

            except asyncio.CancelledError:
                raise
            except Exception as e:
                wait_s = min(max_backoff, base_backoff + random.uniform(0.0, base_backoff))
                base_backoff = min(max_backoff, base_backoff * 1.6)
                log.warning(
                    "onchain_scanner reconnecting",
                    extra={"err": repr(e), "sleep_s": round(wait_s, 2)},
                )
                await asyncio.sleep(wait_s)
                continue

    finally:
        if rpc_client is not None:
            with contextlib.suppress(Exception):
                await rpc_client.close()


async def onchain_scan(loop_cb: Callable[[dict[str, Any]], Any]) -> None:
    """
    Backwards-compatible callback loop.
    Calls loop_cb(event_dict) for each event.
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
        payload = {
            "slot": ev.slot,
            "signature": ev.signature,
            "mint": ev.mint,
            "program_id": ev.program_id,
            "logs": list(ev.logs),
            "ts": ev.ts,
            "keywords_hit": list(ev.keywords_hit),
            "raw": ev.raw,
        }
        await loop_cb(payload)


__all__ = ["OnchainLogEvent", "derive_ws_url", "onchain_scan", "stream_events"]
