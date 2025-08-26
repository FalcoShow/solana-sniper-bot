import aiohttp
import logging
import json
from config import DEXSCREENER_API_URL
from utils import (
    check_rug_pull_risk,
    fraud_score_ai,
    is_whale_wallet,
    send_telegram,
    track_profit_notion,
    redis_cache_token,
)

logger = logging.getLogger("scanner.dexscreener")

SNIPED_MINTS_FILE = "data/sniped_mints_dexscreener.json"
REDIS_CACHE_KEY = "dexscreener:sniped"

# Load local file cache
async def load_local_cache():
    try:
        with open(SNIPED_MINTS_FILE, "r") as f:
            return set(json.load(f))
    except (FileNotFoundError, json.JSONDecodeError):
        return set()

# Save new token to local cache
async def save_to_file_cache(token_mint):
    try:
        current = await load_local_cache()
        current.add(token_mint)
        with open(SNIPED_MINTS_FILE, "w") as f:
            json.dump(list(current), f)
    except Exception as e:
        logger.error(f"Failed to save {token_mint} to local cache: {e}")

# Async generator scanning Dexscreener tokens
async def scan_dexscreener(redis):
    logger.info("🔎 Starting Dexscreener scan...")
    local_cache = await load_local_cache()

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(DEXSCREENER_API_URL) as resp:
                if resp.status != 200:
                    logger.warning(f"Dexscreener API failed with status {resp.status}")
                    return
                data = await resp.json()
    except Exception as e:
        logger.error(f"Failed to fetch Dexscreener data: {e}")
        return

    tokens = data.get("tokens", [])
    for token in tokens:
        try:
            token_mint = token.get("address") or token.get("mint")
            price = float(token.get("priceUsd", 0))
            symbol = token.get("symbol", "Unknown")

            if not token_mint or price == 0:
                continue

            # Skip if already sniped
            if await redis_cache_token(redis, token_mint, REDIS_CACHE_KEY) or token_mint in local_cache:
                continue

            logger.info(f"🔍 Checking {symbol} ({token_mint})")

            # === MARKET FILTER ===
            market_cap = float(token.get("fdv", 0))
            liquidity = float(token.get("liquidity", {}).get("usd", 0))
            if market_cap < 10_000 or liquidity < 1_000:
                logger.info(f"❌ Skipping {symbol} - low market cap or liquidity")
                continue

            # === RUG & FRAUD DETECTION ===
            if not await check_rug_pull_risk(token_mint):
                logger.info(f"❌ Honeypot detected: {symbol}")
                continue

            score = await fraud_score_ai(token_mint)
            if score < 60:
                logger.info(f"❌ Fraud score too low: {score}")
                continue

            # === WHALE FILTER ===
            if not await is_whale_wallet(token_mint):
                logger.info(f"🐋 Skipping {symbol} - no whales found")
                continue

            # === PASSED ALL FILTERS ===
            logger.info(f"✅ {symbol} passed all filters, price: ${price:.4f}")

            # Notify
            dex_link = f"https://dexscreener.com/solana/{token_mint}"
            msg = f"🟢 **Dexscreener Alert**\nSymbol: {symbol}\nPrice: ${price:.4f}\nFraud Score: {score}\n[View Token]({dex_link})"
            await send_telegram(msg)
            await track_profit_notion(token_mint, 0)  # No profit yet, just logging

            # Cache
            await redis.sadd(REDIS_CACHE_KEY, token_mint)
            await save_to_file_cache(token_mint)

            # Yield to sniper
            yield {
                "mint": token_mint,
                "symbol": symbol,
                "price": price
            }

        except Exception as token_error:
            logger.error(f"Error processing token: {token_error}")
