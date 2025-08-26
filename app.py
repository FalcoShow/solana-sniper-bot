from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
import os, aiohttp, asyncio

app = FastAPI()
API_KEY = os.getenv("FRAUD_API_KEY", "")
DEX_URL = "https://api.dexscreener.com/latest/dex/tokens/{mint}"

def score_from_liq(liq_usd: float) -> float:
    # 0..1 cap; 0 at $0 liq, ~1 by $250k+
    return max(0.0, min(1.0, liq_usd / 250_000.0))

def score_from_age_minutes(age_min: float) -> float:
    # brand new pools score a bit lower; > 2h ~ 1.0
    return max(0.0, min(1.0, age_min / 120.0))

@app.get("/score")
async def score(token: str, request: Request):
    # auth
    auth = request.headers.get("authorization", "")
    if not API_KEY or not auth.startswith("Bearer ") or auth.split(" ", 1)[1] != API_KEY:
        raise HTTPException(status_code=401, detail="unauthorized")

    # fetch a top pair from Dexscreener
    liq_usd, age_min = 0.0, 0.0
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as s:
            async with s.get(DEX_URL.format(mint=token)) as r:
                if r.status != 200:
                    return JSONResponse({"fraud_score": 0.5})
                data = await r.json()
                pairs = data.get("pairs") or []
                if not pairs:
                    return JSONResponse({"fraud_score": 0.4})
                top = max(pairs, key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0))
                liq_usd = float((top.get("liquidity") or {}).get("usd") or 0.0)
                # createdAt may be ms or s
                created = top.get("pairCreatedAt") or top.get("createdAt")
                if created:
                    ts = float(created)
                    if ts > 10_000_000_000: ts /= 1000.0
                    age_min = max(0.0, (asyncio.get_event_loop().time() - ts) / 60.0)
    except Exception:
        return JSONResponse({"fraud_score": 0.6})

    # combine simple signals
    s_liq = score_from_liq(liq_usd)
    s_age = score_from_age_minutes(age_min)
    score = max(0.0, min(1.0, 0.7 * s_liq + 0.3 * s_age))
    return {"fraud_score": round(score, 3)}
