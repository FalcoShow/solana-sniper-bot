from __future__ import annotations

"""
scripts/build_whales.py

God-Tier Whale List Builder (V3+++)
==================================

Purpose
-------
Build/refresh `config/whales.csv` with a curated list of large holders ("whales")
for a set of Solana token mints. This file is consumed by smart-wallet / whale
filters and alpha modules.

Key design goals
----------------
- Deterministic output schema (matches your current whales.csv header)
- Safe updates that preserve manual edits (tags/notes/cluster overrides)
- Robust RPC path (Solana JSON-RPC) with Birdeye optional acceleration
- Retry/backoff + rate limiting to avoid API/RPC blowups
- Dedupe by (wallet, token)
- Produces quality metadata fields:
    kind, cluster, weight, confidence, tags, notes

Output schema (CSV)
-------------------
wallet,source,token,holder_rank,est_balance,kind,cluster,weight,confidence,tags,notes

Usage examples
--------------
# RPC only
py scripts/build_whales.py --no-birdeye --rpc %HELIUS_RPC_URL% --mints <MINT1>,<MINT2>

# Birdeye + RPC fallback
set BIRDEYE_API_KEY=...
py scripts/build_whales.py --rpc %HELIUS_RPC_URL% --mints <MINT1>,<MINT2> --limit 50

Notes
-----
- If Birdeye schema changes, you can override endpoint via:
    BIRDEYE_BASE_URL (default https://public-api.birdeye.so)
    BIRDEYE_HOLDERS_ENDPOINT (default solana/token/holders)
"""

import argparse
import csv
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

# Load .env (best-effort)
try:
    from dotenv import find_dotenv, load_dotenv

    load_dotenv(find_dotenv())
except Exception:
    pass


# -----------------------------
# Defaults / knobs
# -----------------------------
DEFAULT_MINTS: list[str] = [
    # Keep defaults small and stable; pass --mints for your full list.
    # BONK
    "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
    # RAY
    "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R",
]

DEFAULT_OUT = "config/whales.csv"

# Ranking -> kind heuristics
DEFAULT_WHALE_RANK_MAX = int(os.getenv("WHALES_WHALE_RANK_MAX", "10"))

# HTTP/RPC pacing
DEFAULT_SLEEP_S = float(os.getenv("WHALES_SLEEP_S", "0.20"))  # between mint calls
DEFAULT_HTTP_TIMEOUT = float(os.getenv("WHALES_HTTP_TIMEOUT", "30"))

# Birdeye
DEFAULT_BIRDEYE_BASE = os.getenv("BIRDEYE_BASE_URL", "https://public-api.birdeye.so").rstrip("/")
DEFAULT_BIRDEYE_HOLDERS_ENDPOINT = os.getenv(
    "BIRDEYE_HOLDERS_ENDPOINT", "solana/token/holders"
).lstrip("/")

# RPC env fallbacks
ENV_RPC_CANDIDATES = ("HELIUS_RPC_URL", "SOLANA_RPC_URL", "RPC_URL")


# -----------------------------
# Models
# -----------------------------
@dataclass(frozen=True)
class HolderRow:
    wallet: str
    token: str
    holder_rank: int
    est_balance: float
    source: str  # "rpc" | "birdeye"

    # enriched fields (defaults computed)
    kind: str = ""
    cluster: str = ""
    weight: float = 0.0
    confidence: float = 0.0
    tags: str = ""
    notes: str = ""


CSV_COLS = [
    "wallet",
    "source",
    "token",
    "holder_rank",
    "est_balance",
    "kind",
    "cluster",
    "weight",
    "confidence",
    "tags",
    "notes",
]


# -----------------------------
# Small helpers
# -----------------------------
def short(pk: str) -> str:
    if not pk:
        return ""
    return pk[:6] + "..." + pk[-4:] if len(pk) > 12 else pk


def _to_float(x: Any) -> float:
    try:
        if x is None:
            return 0.0
        if isinstance(x, int | float):
            return float(x)
        s = str(x).strip()
        if not s:
            return 0.0
        return float(s)
    except Exception:
        return 0.0


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def _norm_wallet(w: str) -> str:
    return (w or "").strip()


def _norm_mint(m: str) -> str:
    return (m or "").strip()


def _tag_merge(existing: str, add: str) -> str:
    """
    Merge comma-separated tags, keeping order-ish and uniqueness.
    """
    base = [t.strip() for t in (existing or "").split(",") if t.strip()]
    extra = [t.strip() for t in (add or "").split(",") if t.strip()]
    out: list[str] = []
    seen = set()
    for t in base + extra:
        tl = t.lower()
        if tl in seen:
            continue
        seen.add(tl)
        out.append(t)
    return ",".join(out)


# -----------------------------
# RPC calls
# -----------------------------
class RpcError(RuntimeError):
    pass


@retry(
    retry=retry_if_exception_type((requests.RequestException, RpcError)),
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=0.7, min=0.7, max=6.0),
)
def rpc_call(rpc_url: str, method: str, params: Any) -> Any:
    headers = {"Content-Type": "application/json"}
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    try:
        r = requests.post(
            rpc_url, headers=headers, data=json.dumps(payload), timeout=DEFAULT_HTTP_TIMEOUT
        )
        if r.status_code != 200:
            raise RpcError(f"RPC {method} HTTP {r.status_code}: {r.text[:500]}")
        try:
            data = r.json()
        except Exception as err:
            raise RpcError(f"RPC {method} JSON decode failed: {err}") from err
    except requests.RequestException:
        raise

    if isinstance(data, dict) and "error" in data:
        raise RpcError(f"RPC {method} error: {data['error']}")
    if not isinstance(data, dict):
        raise RpcError(f"RPC {method} unexpected response type: {type(data)}")
    return data.get("result")


def get_token_largest_accounts(rpc_url: str, mint: str, limit: int) -> list[dict[str, Any]]:
    res = rpc_call(rpc_url, "getTokenLargestAccounts", [mint, {"commitment": "confirmed"}])
    vals = (res or {}).get("value") or []
    # each item: {"address":..., "amount":..., "decimals":..., "uiAmount":..., "uiAmountString":...}
    return list(vals)[: max(0, limit)]


def get_multiple_accounts_parsed(rpc_url: str, pubkeys: list[str]) -> list[dict[str, Any] | None]:
    if not pubkeys:
        return []
    res = rpc_call(
        rpc_url,
        "getMultipleAccounts",
        [pubkeys, {"encoding": "jsonParsed", "commitment": "confirmed"}],
    )
    return (res or {}).get("value") or []


def rpc_top_holders(rpc_url: str, mint: str, limit: int) -> list[tuple[str, int, float]]:
    """
    Returns list of tuples (owner_wallet, rank, ui_amount)
    """
    largest = get_token_largest_accounts(rpc_url, mint, limit)
    token_accounts = [x.get("address", "") for x in largest if x.get("address")]
    infos = get_multiple_accounts_parsed(rpc_url, token_accounts)

    out: list[tuple[str, int, float]] = []
    for idx, (acct, info) in enumerate(zip(largest, infos, strict=False), start=1):
        if not info:
            continue

        owner = ""
        ui_amt = 0.0

        # owner from parsed token account
        data = info.get("data")
        if isinstance(data, dict):
            parsed = data.get("parsed", {})
            if isinstance(parsed, dict):
                info2 = parsed.get("info", {})
                if isinstance(info2, dict):
                    owner = _norm_wallet(info2.get("owner", ""))

                    # more reliable amount from parsed tokenAmount if present
                    tok_amt = info2.get("tokenAmount", {})
                    if isinstance(tok_amt, dict):
                        ui_amt = _to_float(
                            tok_amt.get("uiAmount") or tok_amt.get("uiAmountString") or 0.0
                        )

        # fallback to getTokenLargestAccounts uiAmount*
        if ui_amt <= 0:
            ui_amt = _to_float(acct.get("uiAmount") or acct.get("uiAmountString") or 0.0)

        if owner:
            out.append((owner, idx, ui_amt))

    return out


# -----------------------------
# Birdeye
# -----------------------------
class BirdeyeError(RuntimeError):
    pass


@retry(
    retry=retry_if_exception_type((requests.RequestException, BirdeyeError)),
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=0.7, min=0.7, max=6.0),
)
def birdeye_holders(api_key: str, mint: str, limit: int) -> list[tuple[str, int, float]]:
    """
    Returns list of tuples (owner_wallet, rank, amount/uiAmount).
    We keep this schema-flexible because Birdeye occasionally changes payload shape.
    """
    url = f"{DEFAULT_BIRDEYE_BASE}/{DEFAULT_BIRDEYE_HOLDERS_ENDPOINT}"
    headers = {"X-API-KEY": api_key, "x-chain": "solana"}
    params = {"address": mint, "offset": 0, "limit": limit}

    r = requests.get(url, headers=headers, params=params, timeout=DEFAULT_HTTP_TIMEOUT)
    if r.status_code == 429:
        raise BirdeyeError("Birdeye rate limited (HTTP 429)")
    if r.status_code != 200:
        # Not fatal; upstream may 404 some mints; treat as empty.
        return []

    try:
        payload = r.json()
    except Exception as err:
        # B904: keep exception chaining
        raise BirdeyeError(f"Birdeye JSON decode failed: {err}") from err

    # Try to find "items" list in common places.
    items: Any = None
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, dict) and isinstance(data.get("items"), list):
            items = data.get("items")
        elif isinstance(payload.get("items"), list):
            items = payload.get("items")
        elif isinstance(data, list):
            items = data

    if not isinstance(items, list):
        return []

    out: list[tuple[str, int, float]] = []
    for idx, it in enumerate(items, start=1):
        if not isinstance(it, dict):
            continue
        w = _norm_wallet(it.get("owner") or it.get("address") or it.get("wallet") or "")
        amt = _to_float(it.get("uiAmount") or it.get("amount") or it.get("balance") or 0.0)
        if w:
            out.append((w, idx, amt))
        if len(out) >= limit:
            break

    return out


# -----------------------------
# CSV read/write (preserve manual fields)
# -----------------------------
def read_existing_csv(path: str) -> dict[tuple[str, str], dict[str, str]]:
    """
    Returns mapping (wallet, token) -> existing row dict (string values).
    If the file doesn't exist, returns {}.
    """
    p = Path(path)
    if not p.exists():
        return {}

    existing: dict[tuple[str, str], dict[str, str]] = {}
    with p.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            w = _norm_wallet(row.get("wallet", ""))
            t = _norm_mint(row.get("token", ""))
            if not w or not t:
                continue
            existing[(w, t)] = {k: (row.get(k, "") or "").strip() for k in row}
    return existing


def ensure_parent(path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def _atomic_write_csv(rows: list[dict[str, Any]], path: str) -> None:
    """
    Atomic write to avoid partial/corrupt whales.csv if interrupted mid-write.
    """
    out_path = Path(path)
    ensure_parent(path)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLS, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in CSV_COLS})
    tmp.replace(out_path)


# -----------------------------
# Enrichment heuristics
# -----------------------------
def derive_kind(rank: int, whale_rank_max: int) -> str:
    return "whale" if rank > 0 and rank <= whale_rank_max else "holder"


def derive_cluster() -> str:
    # Cluster bucket name for this script's output.
    return "ct_token_holders"


def derive_weight(rank: int, limit: int) -> float:
    """
    Rank-based weight 0..1. Higher rank => higher weight.
    Stable, monotonic, and gentle slope.
    """
    if rank <= 0 or limit <= 0:
        return 0.10
    # top rank -> ~0.40, bottom -> ~0.10
    frac = max(0.0, min(1.0, 1.0 - (rank - 1) / float(limit)))
    return round(0.10 + 0.30 * frac, 3)


def derive_confidence(source: str) -> float:
    # Birdeye slightly higher confidence than raw RPC parsing (but close).
    if source == "birdeye":
        return 0.70
    if source == "rpc":
        return 0.55
    return 0.50


def derive_tags(rank: int, whale_rank_max: int) -> str:
    tags = ["holder"]
    if rank <= whale_rank_max:
        tags.append("rank_top_whale")
    elif rank <= whale_rank_max * 2:
        tags.append("rank_mid")
    else:
        tags.append("rank_tail")
    return ",".join(tags)


# -----------------------------
# CLI / Main
# -----------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Build/refresh curated whale list CSV (Birdeye + RPC fallback)."
    )
    p.add_argument(
        "--mints",
        type=str,
        default=",".join(DEFAULT_MINTS),
        help="Comma-separated mint list. Keep it small if your RPC is weak; defaults to BONK + RAY.",
    )
    p.add_argument("--limit", type=int, default=50, help="Top holders per mint (default 50).")
    p.add_argument(
        "--rpc",
        type=str,
        default="",
        help="RPC URL. If omitted, uses HELIUS_RPC_URL or SOLANA_RPC_URL env vars.",
    )
    p.add_argument("--no-birdeye", action="store_true", help="Skip Birdeye and use RPC only.")
    p.add_argument(
        "--out", type=str, default=DEFAULT_OUT, help="Output CSV path (default config/whales.csv)."
    )
    p.add_argument(
        "--whale-rank-max",
        type=int,
        default=DEFAULT_WHALE_RANK_MAX,
        help=f"Rank <= this becomes kind=whale (default {DEFAULT_WHALE_RANK_MAX}).",
    )
    p.add_argument(
        "--min-balance",
        type=float,
        default=float(os.getenv("WHALES_MIN_BALANCE", "0")),
        help="Filter out holders below this ui balance (default 0).",
    )
    p.add_argument(
        "--sleep",
        type=float,
        default=DEFAULT_SLEEP_S,
        help=f"Sleep seconds between mint scans (default {DEFAULT_SLEEP_S}).",
    )
    p.add_argument("--verbose", action="store_true", help="Verbose logs.")
    return p.parse_args()


def pick_rpc_url(cli_rpc: str) -> str:
    if cli_rpc and cli_rpc.strip():
        return cli_rpc.strip()
    for k in ENV_RPC_CANDIDATES:
        v = os.getenv(k, "").strip()
        if v:
            return v
    return ""


def main() -> int:
    args = parse_args()

    rpc_url = pick_rpc_url(args.rpc)
    birdeye_key = (os.getenv("BIRDEYE_API_KEY") or "").strip()
    have_birdeye = bool(birdeye_key) and not args.no_birdeye

    if not rpc_url and not have_birdeye:
        print("❌ No data sources available.")
        print(
            "   Provide --rpc (or set HELIUS_RPC_URL / SOLANA_RPC_URL), and/or set BIRDEYE_API_KEY."
        )
        return 2

    mints = [_norm_mint(m) for m in args.mints.split(",") if _norm_mint(m)]
    if not mints:
        print("❌ No mints provided.")
        return 2

    existing = read_existing_csv(args.out)

    if have_birdeye:
        print("✅ Using Birdeye holders (BIRDEYE_API_KEY set).")
        print(f"   Birdeye base: {DEFAULT_BIRDEYE_BASE}")
        print(f"   Holders endpoint: {DEFAULT_BIRDEYE_HOLDERS_ENDPOINT}")
    else:
        if args.no_birdeye:
            print("ℹ️ Birdeye disabled via --no-birdeye (RPC only).")
        else:
            print("⚠️ No BIRDEYE_API_KEY found — Birdeye disabled (RPC only).")

    if rpc_url:
        print(f"✅ Using RPC: {rpc_url}")
    else:
        print("⚠️ No RPC URL set — RPC fallback disabled.")

    print(f"Scanning mints ({len(mints)}): " + ", ".join(short(m) for m in mints))
    print(f"Top holders per mint: {args.limit}")
    print(f"Whale rank max: {args.whale_rank_max}")
    if args.min_balance > 0:
        print(f"Min balance filter: {args.min_balance}")

    merged: dict[tuple[str, str], dict[str, Any]] = {}

    def upsert(wallet: str, token: str, source: str, rank: int, bal: float) -> None:
        w = _norm_wallet(wallet)
        t = _norm_mint(token)
        if not w or not t:
            return
        if args.min_balance > 0 and bal < args.min_balance:
            return

        key = (w, t)
        prev = existing.get(key, {})

        prev_kind = prev.get("kind", "").strip()
        prev_cluster = prev.get("cluster", "").strip()
        prev_weight = prev.get("weight", "").strip()
        prev_conf = prev.get("confidence", "").strip()
        prev_tags = prev.get("tags", "").strip()
        prev_notes = prev.get("notes", "").strip()

        kind = prev_kind or derive_kind(rank, args.whale_rank_max)
        cluster = prev_cluster or derive_cluster()
        weight = _to_float(prev_weight) if prev_weight else derive_weight(rank, args.limit)
        confidence = _to_float(prev_conf) if prev_conf else derive_confidence(source)
        tags = _tag_merge(prev_tags, derive_tags(rank, args.whale_rank_max))
        notes = prev_notes

        merged[key] = {
            "wallet": w,
            "source": source,
            "token": t,
            "holder_rank": rank,
            "est_balance": round(float(bal), 6),
            "kind": kind,
            "cluster": cluster,
            "weight": round(float(weight), 6),
            "confidence": round(float(confidence), 6),
            "tags": tags,
            "notes": notes,
        }

    for mint in mints:
        rows: list[tuple[str, int, float]] = []
        used_source = ""

        if have_birdeye:
            try:
                rows = birdeye_holders(birdeye_key, mint, args.limit)
                used_source = "birdeye" if rows else ""
            except Exception as e:
                if args.verbose:
                    print(f"⚠️ Birdeye failed for {short(mint)}: {e}")
                rows = []
                used_source = ""

        if (not rows) and rpc_url:
            try:
                rows = rpc_top_holders(rpc_url, mint, args.limit)
                used_source = "rpc" if rows else ""
            except Exception as e:
                print(f"⚠️ RPC failed for {short(mint)}: {e}")
                rows = []
                used_source = ""

        if args.verbose:
            print(f"[{short(mint)}] got {len(rows)} holders via {used_source or 'none'}")

        for wallet, rank, bal in rows:
            upsert(wallet=wallet, token=mint, source=used_source or "rpc", rank=rank, bal=bal)

        time.sleep(max(0.0, float(args.sleep)))

    # Carry over rows that exist but weren't refreshed this run (preserve manual seeds)
    for (w, t), prev in existing.items():
        if (w, t) in merged:
            continue
        carry = {k: (prev.get(k, "") or "").strip() for k in CSV_COLS}
        if carry.get("wallet") and carry.get("token"):
            merged[(w, t)] = {
                "wallet": carry.get("wallet", w),
                "source": carry.get("source", ""),
                "token": carry.get("token", t),
                "holder_rank": _safe_int(carry.get("holder_rank", 0), 0),
                "est_balance": _to_float(carry.get("est_balance", 0.0)),
                "kind": carry.get("kind", ""),
                "cluster": carry.get("cluster", ""),
                "weight": _to_float(carry.get("weight", 0.0)),
                "confidence": _to_float(carry.get("confidence", 0.0)),
                "tags": carry.get("tags", ""),
                "notes": carry.get("notes", ""),
            }

    out_rows = list(merged.values())
    out_rows.sort(
        key=lambda r: (
            r.get("token", ""),
            _safe_int(r.get("holder_rank", 10**9), 10**9),
            -_to_float(r.get("est_balance", 0.0)),
            r.get("wallet", ""),
        )
    )

    _atomic_write_csv(out_rows, args.out)

    print(f"✅ Wrote {len(out_rows)} rows -> {args.out}")
    if not out_rows:
        print(
            "Tip: ensure --rpc is set (or HELIUS_RPC_URL/SOLANA_RPC_URL), and/or set BIRDEYE_API_KEY."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
