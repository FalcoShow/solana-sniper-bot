
import os
import sys
import csv
import json
import time
import argparse
from pathlib import Path
from typing import List, Dict, Any, Optional

# 3rd party
try:
    import requests
except ImportError:
    print("Please: pip install requests")
    sys.exit(1)

# Load .env
try:
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv())
except Exception:
    pass

DEFAULT_MINTS = [
    # Smaller-cap DeFi tokens that won't blow up RPC scans
    # BONK, WIF, RAY, JUP
    "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
    "8V8Nr5az28G8e2x9k3iN3bQJmGm5q9nXYVJHsxgq1D5",  # WIF (example common community mint; replace if differs)
    "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R",
    "JUP2jxvopbhzUKxbB3sUAWv78sF6Y6z4QBQ8D2K8kDr",
]

def short(pk: str) -> str:
    return pk[:6] + "..." + pk[-4:] if len(pk) > 12 else pk

def rpc_call(rpc_url: str, method: str, params: Any) -> Any:
    headers = {"Content-Type": "application/json"}
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    r = requests.post(rpc_url, headers=headers, data=json.dumps(payload), timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"RPC {method} HTTP {r.status_code}: {r.text}")
    data = r.json()
    if "error" in data:
        raise RuntimeError(f"RPC {method} error: {data['error']}")
    return data["result"]

def get_largest_token_accounts(rpc_url: str, mint: str, limit: int = 20) -> List[Dict[str, Any]]:
    res = rpc_call(rpc_url, "getTokenLargestAccounts", [mint, {"commitment": "confirmed"}])
    accounts = res.get("value", [])
    # Keep top N
    return accounts[:limit]

def get_account_infos(rpc_url: str, pubkeys: List[str]) -> List[Optional[Dict[str, Any]]]:
    # Use getMultipleAccounts for efficiency
    if not pubkeys:
        return []
    res = rpc_call(rpc_url, "getMultipleAccounts", [pubkeys, {"encoding": "jsonParsed", "commitment": "confirmed"}])
    return res.get("value", [])

def owners_from_token_accounts(rpc_url: str, token_accts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    pubkeys = [a["address"] for a in token_accts if "address" in a]
    infos = get_account_infos(rpc_url, pubkeys)
    out = []
    for rank, (acct, info) in enumerate(zip(token_accts, infos), start=1):
        if not info or not info.get("data"):
            continue
        data = info["data"]
        if isinstance(data, dict) and data.get("parsed") and data["parsed"].get("info"):
            owner = data["parsed"]["info"].get("owner")
        else:
            owner = None
        amount = acct.get("uiAmount") or 0
        out.append({
            "wallet": owner or "",
            "token_account": acct.get("address", ""),
            "holder_rank": rank,
            "uiAmount": amount,
        })
    return [x for x in out if x["wallet"]]

def birdeye_holders(api_key: str, mint: str, limit: int = 50) -> List[Dict[str, Any]]:
    # Birdeye sometimes 404s on certain mints. We'll catch and return empty.
    # Endpoint shape (public API): we use /solana/token/holders with headers:
    #   X-API-KEY + x-chain: solana
    # If this 404s, we return [] and let RPC fallback do the work.
    url = f"https://public-api.birdeye.so/solana/token/holders"
    headers = {"X-API-KEY": api_key, "x-chain": "solana"}
    params = {"address": mint, "offset": 0, "limit": limit}
    r = requests.get(url, headers=headers, params=params, timeout=30)
    if r.status_code != 200:
        # print a short warning for the user; return []
        print(f"⚠️ holders fetch failed for {short(mint)}: Birdeye holders HTTP {r.status_code}: {r.text}")
        return []
    data = r.json()
    # expected shape: {"success": true, "data": {"items": [{"owner":"...", "amount":...}, ...]}}
    items = []
    try:
        items = data["data"]["items"]
    except Exception:
        # different schema? gracefully fallback
        return []
    out = []
    for idx, it in enumerate(items, start=1):
        wallet = it.get("owner") or it.get("address") or ""
        bal = it.get("amount") or it.get("uiAmount") or 0
        if wallet:
            out.append({"wallet": wallet, "holder_rank": idx, "uiAmount": bal})
    return out

def write_csv(rows: List[Dict[str, Any]], path: str) -> str:
    path_obj = Path(path)
    path_obj.parent.mkdir(parents=True, exist_ok=True)
    cols = ["wallet", "source", "token", "holder_rank", "est_balance"]
    with path_obj.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in rows:
            w.writerow([r.get(c, "") for c in cols])
    return str(path_obj)

def parse_args():
    p = argparse.ArgumentParser(description="Build curated whale list (CSV) from Birdeye and/or RPC.")
    p.add_argument("--mints", type=str, default=",".join(DEFAULT_MINTS),
                   help="Comma-separated mint list. Defaults to common DeFi mints (BONK/WIF/RAY/JUP).")
    p.add_argument("--limit", type=int, default=50, help="Top holders per mint (default 50).")
    p.add_argument("--rpc", type=str, default=os.getenv("HELIUS_RPC_URL") or os.getenv("SOLANA_RPC_URL") or "",
                   help="RPC URL (defaults to HELIUS_RPC_URL or SOLANA_RPC_URL).")
    p.add_argument("--no-birdeye", action="store_true", help="Skip Birdeye, force RPC path only.")
    p.add_argument("--out", type=str, default="config/whales.csv", help="Output CSV path.")
    return p.parse_args()

def main():
    args = parse_args()
    birdeye_key = os.getenv("BIRDEYE_API_KEY") or ""
    have_birdeye = bool(birdeye_key) and not args.no_birdeye

    # Log what we're using
    if have_birdeye:
        print("Using Birdeye for holders (BIRDEYE_API_KEY found).")
    else:
        if not birdeye_key and not args.no_birdeye:
            print("⚠️ No BIRDEYE_API_KEY set — we'll rely on RPC only.")

    rpc_url = args.rpc.strip()
    if not rpc_url:
        print("⚠️ No RPC provided (HELIUS_RPC_URL / SOLANA_RPC_URL not set and --rpc missing). "
              "RPC fallback features will be limited.")
    else:
        print(f"Using RPC: {rpc_url}")

    # Expand mint list (allow shorthand with ellipses passed earlier; here require full mints)
    mints = [m.strip() for m in args.mints.split(",") if m.strip()]
    print(f"Scanning mints ({len(mints)}): " + ", ".join([short(m) for m in mints]))
    print(f"Top accounts per mint: {args.limit}")

    out_rows: List[Dict[str, Any]] = []
    seen = set()

    for mint in mints:
        rows: List[Dict[str, Any]] = []
        # Try Birdeye first (if available)
        if have_birdeye:
            try:
                rows = birdeye_holders(birdeye_key, mint, args.limit)
                source = "birdeye"
            except Exception as e:
                print(f"⚠️ holders fetch failed for {short(mint)} (birdeye): {e}")
                rows = []

        # Fallback to RPC if Birdeye empty AND we have an RPC URL
        if not rows and rpc_url:
            try:
                tok_accts = get_largest_token_accounts(rpc_url, mint, args.limit)
                owners = owners_from_token_accounts(rpc_url, tok_accts)
                rows = [{"wallet": o["wallet"], "holder_rank": o["holder_rank"], "uiAmount": o["uiAmount"]} for o in owners]
                source = "rpc"
            except Exception as e:
                print(f"⚠️ RPC holders fetch failed for {short(mint)}: {e}")
                rows = []

        # Accumulate
        for r in rows:
            wallet = r["wallet"]
            key = (wallet, mint)
            if key in seen:
                continue
            seen.add(key)
            out_rows.append({
                "wallet": wallet,
                "source": source if rows else "",
                "token": mint,
                "holder_rank": r.get("holder_rank", ""),
                "est_balance": r.get("uiAmount", ""),
            })
        # be gentle
        time.sleep(0.2)

    path = write_csv(out_rows, args.out)
    print(f"✅ Wrote {len(out_rows)} rows -> {path}")
    if not out_rows:
        print("Tip: try --no-birdeye to force RPC, ensure --rpc is set, or use --mints with popular DeFi mints (BONK/WIF/RAY/JUP).")

if __name__ == "__main__":
    main()
