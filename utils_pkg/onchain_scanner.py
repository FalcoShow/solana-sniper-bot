# utils_pkg/onchain_scanner.py
import os, asyncio, json, re, time
import websockets

def env(name, default=""):
    v = os.environ.get(name)
    return v if v is not None else default

def derive_ws_url(http_url: str) -> str:
    # Accept either ws/wss or http/https and coerce to wss
    u = http_url.strip()
    if u.startswith("wss://") or u.startswith("ws://"):
        return u.replace("ws://", "wss://")
    if u.startswith("https://"):
        return "wss://" + u[len("https://"):]
    if u.startswith("http://"):
        return "wss://" + u[len("http://"):]
    # bare host
    return "wss://" + u

MINT_RE = re.compile(r"(?i)\bmint[:=]\s*([1-9A-HJ-NP-Za-km-z]{32,44})")

async def logs_stream(ws_url: str, program_id: str, commitment="confirmed"):
    sub = {
        "jsonrpc":"2.0","id":1,"method":"logsSubscribe",
        "params":[{"mentions":[program_id]},{"commitment":commitment}]
    }
    async with websockets.connect(ws_url, max_size=10_000_000, ping_interval=20) as ws:
        await ws.send(json.dumps(sub))
        sub_resp = json.loads(await ws.recv())
        if "error" in sub_resp:
            raise RuntimeError(f"subscription error: {sub_resp}")
        print("Subscribed:", sub_resp)
        while True:
            msg = json.loads(await ws.recv())
            if msg.get("method") != "logsNotification":
                continue
            v = msg["params"]["result"]["value"]
            logs = v.get("logs") or []
            sig  = v.get("signature")
            slot = msg["params"]["result"].get("context",{}).get("slot")
            # Very loose heuristics—tune these to your patterns of Pump deploys
            text = "\n".join(logs)
            if any(x in text for x in (
                "Initialize", "Create", "Bonding", "bonding", "launch", "mint"
            )):
                m = MINT_RE.search(text)
                mint = m.group(1) if m else None
                yield {
                    "slot": slot, "signature": sig, "mint": mint,
                    "logs": logs, "ts": int(time.time())
                }

async def onchain_scan(loop_cb):
    rpc_http = env("SOL_RPC_URL", "").strip()
    rpc_ws   = env("SOL_WS_URL", "").strip() or derive_ws_url(rpc_http)
    program  = env("PUMP_PROGRAM_ID", "").strip()
    if not (rpc_ws and program):
        raise SystemExit("Set SOL_RPC_URL (or SOL_WS_URL) and PUMP_PROGRAM_ID in env")

    async for ev in logs_stream(rpc_ws, program):
        await loop_cb(ev)
