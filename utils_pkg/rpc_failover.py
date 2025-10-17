# utils_pkg/rpc_failover.py
from __future__ import annotations
import os, asyncio
import aiohttp

PUBLIC_HTTP = "https://api.mainnet-beta.solana.com"
PUBLIC_WS = "wss://api.mainnet-beta.solana.com"

def _split_list(val: str) -> list[str]:
    return [x.strip() for x in val.split(",") if x.strip()]

def _dedupe_preserve(items: list[str]) -> list[str]:
    seen, out = set(), []
    for it in items:
        if it and it not in seen:
            out.append(it); seen.add(it)
    return out

def gather_http_candidates(env=os.environ) -> list[str]:
    primaries = ["HEALTHCHECK_RPC_URL", "RPC_URL", "SOLANA_RPC_URL", "HELIUS_RPC_URL"]
    fallbacks = ["SOLANA_RPC_FALLBACKS", "RPC_URLS", "RPC_URL_FALLBACKS", "RPC_URL_FALLBACK_1", "RPC_URL_FALLBACK_2"]
    urls: list[str] = []
    for n in primaries:
        v = (env.get(n) or "").strip()
        if v: urls.append(v)
    for n in fallbacks:
        v = (env.get(n) or "").strip()
        if not v: continue
        urls.extend(_split_list(v) if ("," in v) else [v])
    urls = _dedupe_preserve(urls)
    if PUBLIC_HTTP not in urls:
        urls.append(PUBLIC_HTTP)
    return urls

def gather_ws_candidates(env=os.environ) -> list[str]:
    primaries = ["HEALTHCHECK_WS_URL", "WS_URL", "SOLANA_WS_URL", "HELIUS_WS_URL"]
    fallbacks = ["SOLANA_WS_FALLBACKS", "WS_URLS", "WS_URL_FALLBACKS", "WS_URL_FALLBACK_1", "WS_URL_FALLBACK_2"]
    urls: list[str] = []
    for n in primaries:
        v = (env.get(n) or "").strip()
        if v: urls.append(v)
    for n in fallbacks:
        v = (env.get(n) or "").strip()
        if not v: continue
        urls.extend(_split_list(v) if ("," in v) else [v])
    urls = _dedupe_preserve(urls)
    if PUBLIC_WS not in urls:
        urls.append(PUBLIC_WS)
    return urls

async def _probe_http(url: str, timeout: float = 3.5) -> tuple[bool, str]:
    payload1 = {"jsonrpc": "2.0", "id": 1, "method": "getHealth"}
    payload2 = {"jsonrpc": "2.0", "id": 1, "method": "getLatestBlockhash", "params": [{"commitment": "processed"}]}
    try:
        to = aiohttp.ClientTimeout(total=timeout)
        async with aiohttp.ClientSession(timeout=to) as s:
            async with s.post(url, json=payload1) as r:
                if r.status == 200:
                    data = await r.json(content_type=None)
                    if isinstance(data, dict) and data.get("result") in ("ok", {"Ok": {}}):
                        return True, "getHealth:ok"
            async with s.post(url, json=payload2) as r2:
                if r2.status == 200:
                    data = await r2.json(content_type=None)
                    if isinstance(data, dict) and data.get("result"):
                        return True, "latestBlockhash"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"
    return False, "unhealthy"

async def resolve_http_first_alive(candidates: list[str] | None = None, timeout: float = 3.5):
    if candidates is None:
        candidates = gather_http_candidates()
    notes = []
    for u in candidates:
        ok, why = await _probe_http(u, timeout=timeout)
        notes.append((u, ok, why))
        if ok:
            return u, notes
    return candidates[-1], notes

def pick_http_url_sync(timeout: float = 3.5):
    return asyncio.get_event_loop().run_until_complete(resolve_http_first_alive(timeout=timeout))

async def apply_failover_to_env(timeout: float = 3.5):
    chosen, notes = await resolve_http_first_alive(timeout=timeout)
    os.environ["RPC_URL"] = chosen
    return chosen, notes
