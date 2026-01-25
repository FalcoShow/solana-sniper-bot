# scripts/paper_label_worker.py
from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any

import aiohttp

from logging_setup import get_logger
from utils_pkg import data_logger

try:
    from utils_pkg import core_utils as cu  # provides get_redis_async()
except Exception:
    cu = None  # type: ignore

try:
    from utils_pkg import ai_db  # canonical DB helper
except Exception:
    ai_db = None  # type: ignore

log = get_logger(__name__)

_TRUE = {"1", "true", "yes", "y", "on", "t"}


def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    return (v if v is not None else d).strip()


def _envb(k: str, d: bool = False) -> bool:
    v = _envs(k, "")
    if v == "":
        return bool(d)
    return v.lower() in _TRUE


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


# Redis keys
INBOX_KEY = _envs("PAPER_LABEL_INBOX_KEY", "paper_label:inbox")
DUE_KEY = _envs("PAPER_LABEL_DUE_KEY", "paper_label:due")
STATE_KEY_PREFIX = _envs("PAPER_LABEL_STATE_PREFIX", "paper_label:state:")

# Horizons (seconds) - tune these for speed vs quality
# Fast labeling: "60,180,300,600"
# Slower/stronger: "60,300,900,1800,3600"
HORIZONS = [
    int(x.strip())
    for x in _envs("PAPER_LABEL_HORIZONS_SEC", "60,180,300,600").split(",")
    if x.strip()
]

BATCH = _envi("PAPER_LABEL_BATCH", 200)
POLL_MS = _envi("PAPER_LABEL_POLL_MS", 500)

CONCURRENCY = _envi("PAPER_LABEL_CONCURRENCY", 20)
MAX_RETRIES = _envi("PAPER_LABEL_MAX_RETRIES", 10)
RETRY_DELAY_S = _envf("PAPER_LABEL_RETRY_DELAY_S", 15.0)

# Label thresholds (%)
MOON_PCT = _envf("PAPER_LABEL_MOON_PCT", 50.0)
BIG_WIN_PCT = _envf("PAPER_LABEL_BIG_WIN_PCT", 20.0)
WIN_PCT = _envf("PAPER_LABEL_WIN_PCT", 5.0)
LOSS_PCT = _envf("PAPER_LABEL_LOSS_PCT", -10.0)
DISASTER_PCT = _envf("PAPER_LABEL_DISASTER_PCT", -30.0)

DRY_RUN = _envb("PAPER_LABEL_DRY_RUN", False)


def _now() -> float:
    return time.time()


async def _fetch_candidate_minimal(candidate_id: int) -> dict[str, Any] | None:
    """
    Pull mint/symbol/source/regime/ui from entry_candidates.
    Uses ai_db if available.
    """
    if ai_db is None or not hasattr(ai_db, "fetchrow"):
        return None
    row = await ai_db.fetchrow(
        "SELECT id, mint, symbol, source, regime, decision, ui FROM entry_candidates WHERE id=$1",
        int(candidate_id),
    )
    if not row:
        return None
    # asyncpg row behaves like dict-ish
    return {
        "id": int(row["id"]),
        "mint": str(row.get("mint") or ""),
        "symbol": str(row.get("symbol") or ""),
        "source": str(row.get("source") or ""),
        "regime": str(row.get("regime") or ""),
        "decision": str(row.get("decision") or ""),
        "ui": row.get("ui"),
    }


async def _has_outcome(candidate_id: int) -> bool:
    if ai_db is None or not hasattr(ai_db, "fetchrow"):
        return False
    row = await ai_db.fetchrow(
        "SELECT 1 FROM entry_outcomes WHERE candidate_id=$1 LIMIT 1",
        int(candidate_id),
    )
    return bool(row)


async def _price_jup(session: aiohttp.ClientSession, mint: str) -> float | None:
    # Jupiter price endpoints vary; handle both common shapes.
    for url in (
        f"https://price.jup.ag/v6/price?ids={mint}",
        f"https://api.jup.ag/price/v2?ids={mint}",
    ):
        try:
            async with session.get(url, timeout=10) as r:
                if r.status != 200:
                    continue
                j = await r.json()
                data = j.get("data") or {}
                node = data.get(mint) or data.get(str(mint))
                if isinstance(node, dict) and "price" in node:
                    return float(node["price"])
                # v2 shape sometimes: data: {mint: {price:...}}
        except Exception:
            continue
    return None


async def _price_dexscreener(session: aiohttp.ClientSession, mint: str) -> float | None:
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
        async with session.get(url, timeout=12) as r:
            if r.status != 200:
                return None
            j = await r.json()
            pairs = j.get("pairs") or []
            # pick the best pair by liquidityUsd if present
            best = None
            best_liq = -1.0
            for p in pairs:
                try:
                    liq = float(((p.get("liquidity") or {}).get("usd")) or 0.0)
                except Exception:
                    liq = 0.0
                if liq > best_liq:
                    best_liq = liq
                    best = p
            if best and best.get("priceUsd") is not None:
                return float(best["priceUsd"])
    except Exception:
        return None
    return None


async def fetch_price_usd(session: aiohttp.ClientSession, mint: str) -> float | None:
    p = await _price_jup(session, mint)
    if p is not None and p > 0:
        return p
    p = await _price_dexscreener(session, mint)
    if p is not None and p > 0:
        return p
    return None


def classify(max_ret: float, min_ret: float) -> str:
    # Use max upside + min downside to label
    if max_ret >= MOON_PCT:
        return "paper_moon"
    if max_ret >= BIG_WIN_PCT:
        return "paper_big_win"
    if max_ret >= WIN_PCT:
        return "paper_win"
    if min_ret <= DISASTER_PCT:
        return "paper_disaster"
    if min_ret <= LOSS_PCT:
        return "paper_loss"
    return "paper_flat"


def _state_key(cid: int) -> str:
    return f"{STATE_KEY_PREFIX}{cid}"


async def intake_loop(r, session: aiohttp.ClientSession) -> None:
    """
    BLPOP candidate_ids from INBOX_KEY, create state, schedule due checks.
    """
    while True:
        try:
            item = await r.blpop(INBOX_KEY, timeout=5)
            if not item:
                continue
            _, raw = item
            cid = int(raw)
            if cid <= 0:
                continue

            # Skip if already labeled
            if await _has_outcome(cid):
                continue

            cand = await _fetch_candidate_minimal(cid)
            if not cand:
                continue

            mint = cand["mint"]
            if not mint:
                continue

            base_price = await fetch_price_usd(session, mint)
            if base_price is None or base_price <= 0:
                # retry later
                await r.lpush(INBOX_KEY, cid)
                await asyncio.sleep(1.0)
                continue

            st = {
                "cid": cid,
                "mint": mint,
                "symbol": cand.get("symbol", ""),
                "source": cand.get("source", ""),
                "regime": cand.get("regime", ""),
                "t0": _now(),
                "p0": float(base_price),
                "prices": {},  # horizon -> price
                "retries": 0,
                "horizons": HORIZONS,
            }
            await r.set(_state_key(cid), json.dumps(st), ex=60 * 60 * 6)  # keep up to 6h

            # schedule checks
            zadd_map = {}
            now = _now()
            for h in HORIZONS:
                zadd_map[f"{cid}:{h}"] = now + float(h)
            if zadd_map:
                await r.zadd(DUE_KEY, zadd_map)

        except Exception:
            if log.every("paper_label_intake_fail", 30):
                log.exception("paper_label_worker: intake loop error")
            await asyncio.sleep(1.0)


async def process_due_item(
    r, session: aiohttp.ClientSession, item: str, sem: asyncio.Semaphore
) -> None:
    async with sem:
        try:
            cid_s, h_s = item.split(":", 1)
            cid = int(cid_s)
            h = int(h_s)
            key = _state_key(cid)
            blob = await r.get(key)
            if not blob:
                return
            st = json.loads(blob)
            mint = st.get("mint", "")
            if not mint:
                return

            p = await fetch_price_usd(session, mint)
            if p is None or p <= 0:
                st["retries"] = int(st.get("retries", 0)) + 1
                if st["retries"] <= MAX_RETRIES:
                    await r.set(key, json.dumps(st), ex=60 * 60 * 6)
                    await r.zadd(DUE_KEY, {item: _now() + RETRY_DELAY_S})
                return

            prices = st.get("prices") or {}
            prices[str(h)] = float(p)
            st["prices"] = prices
            await r.set(key, json.dumps(st), ex=60 * 60 * 6)

            # If all horizons done -> finalize label
            if len(prices.keys()) >= len(st.get("horizons", HORIZONS)):
                p0 = float(st.get("p0", 0.0) or 0.0)
                if p0 <= 0:
                    await r.delete(key)
                    return

                rets: dict[str, float] = {}
                max_ret = -1e9
                max_h = 0
                min_ret = 1e9
                for hs, ph in prices.items():
                    try:
                        hh = int(hs)
                        ret = (float(ph) - p0) / p0 * 100.0
                        rets[str(hh)] = float(ret)
                        if ret > max_ret:
                            max_ret = ret
                            max_h = hh
                        if ret < min_ret:
                            min_ret = ret
                    except Exception:
                        continue

                label = classify(max_ret, min_ret)

                extra = {
                    "mint": mint,
                    "symbol": st.get("symbol", ""),
                    "source": st.get("source", ""),
                    "regime": st.get("regime", ""),
                    "paper": {
                        "p0": p0,
                        "t0": st.get("t0"),
                        "prices": prices,
                        "returns_pct": rets,
                        "max_ret": max_ret,
                        "max_horizon_sec": max_h,
                        "min_ret": min_ret,
                    },
                }

                if DRY_RUN:
                    log.info(
                        "DRY_RUN label cid=%s mint=%s label=%s max_ret=%.2f",
                        cid,
                        mint,
                        label,
                        max_ret,
                    )
                else:
                    await data_logger.a_log_entry_outcome(
                        candidate_id=cid,
                        pnl_pct=float(max_ret),
                        hold_seconds=float(max_h),
                        got_stopped=False,
                        label=label,
                        notes="paper label (did not buy)",
                        extra=extra,
                    )

                await r.delete(key)

        except Exception:
            if log.every("paper_label_due_fail", 30):
                log.exception("paper_label_worker: due item failed")


async def due_loop(r, session: aiohttp.ClientSession) -> None:
    sem = asyncio.Semaphore(max(1, CONCURRENCY))
    while True:
        try:
            now = _now()
            items: list[str] = await r.zrangebyscore(
                DUE_KEY, min=-1e18, max=now, start=0, num=BATCH
            )
            if not items:
                await asyncio.sleep(POLL_MS / 1000.0)
                continue

            # Remove first (avoid duplicates if we crash mid-process)
            await r.zrem(DUE_KEY, *items)

            tasks = [asyncio.create_task(process_due_item(r, session, it, sem)) for it in items]
            if tasks:
                await asyncio.gather(*tasks)

        except Exception:
            if log.every("paper_label_due_loop_fail", 30):
                log.exception("paper_label_worker: due loop error")
            await asyncio.sleep(1.0)


async def main() -> None:
    if cu is None or not hasattr(cu, "get_redis_async"):
        raise RuntimeError(
            "core_utils.get_redis_async not available; cannot run paper_label_worker"
        )

    await data_logger.ensure_schema()

    r = await cu.get_redis_async()

    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        log.info(
            "paper_label_worker online inbox=%s due=%s horizons=%s conc=%s",
            INBOX_KEY,
            DUE_KEY,
            HORIZONS,
            CONCURRENCY,
        )
        await asyncio.gather(
            intake_loop(r, session),
            due_loop(r, session),
        )


if __name__ == "__main__":
    asyncio.run(main())
