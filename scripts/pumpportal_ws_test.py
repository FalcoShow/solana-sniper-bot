import os, asyncio, json, websockets, re

API_KEY = os.getenv("PF_PP_KEY")  # make sure this is set
ACCOUNT = os.getenv("PF_PP_ACCOUNT", "")  # optional
TOKENS  = [t for t in os.getenv("PF_PP_MINTS","").split(",") if t]

assert API_KEY, "PF_PP_KEY is not set"
URI = f"wss://pumpportal.fun/api/data?api-key={API_KEY}"

def mask(s):
    return s[:4] + "..." + s[-4:] if s and len(s) > 12 else "<short>"

async def main():
    print("Connecting:", re.sub(r"(api-key=)[^&]+", r"\1" + mask(API_KEY), URI))
    async with websockets.connect(URI, ping_interval=20, ping_timeout=30, close_timeout=5, max_size=2**23) as ws:
        # FREE feed
        await ws.send(json.dumps({"method":"subscribeNewToken"}))

        # PAID feeds (require API key)
        if TOKENS:
            await ws.send(json.dumps({"method":"subscribeTokenTrade","keys":TOKENS}))
            print("Subscribed token trades for:", TOKENS)
        if ACCOUNT:
            await ws.send(json.dumps({"method":"subscribeAccountTrade","keys":[ACCOUNT]}))
            print("Subscribed account trades for:", ACCOUNT)

        while True:
            msg = json.loads(await ws.recv())
            # filter the banner
            if msg.get("errors") == "Invalid API key. PumpSwap data will not be streamed.":
                continue
            print("EVENT:", msg)

asyncio.run(main())
