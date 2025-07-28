import json
import asyncio
import time
from solana.rpc.async_api import AsyncClient
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from telegram import Bot

# === CONFIG ===
TELEGRAM_BOT_TOKEN = "8280288862:AAFRrxCIfJoRCkI49QcJeChaKh9I_Q9xwKY"
TELEGRAM_USER_ID = 6106592279  # int, not string

PRIVATE_KEY_PATH = "keypair.json"
WALLET_ADDRESS_STR = "87tfhxkSeEjSq5tTETcBpZhXX5JDHVhHtgWy9B2VkdzJ"
WALLET_ADDRESS = Pubkey.from_string(WALLET_ADDRESS_STR)

bot = Bot(token=TELEGRAM_BOT_TOKEN)

# Load keypair from JSON file
with open(PRIVATE_KEY_PATH, "r") as f:
    secret_key_list = json.load(f)  # should be a list of ints
    secret_key_bytes = bytes(secret_key_list)
    keypair = Keypair.from_bytes(secret_key_bytes)

# Async Telegram notifier
async def notify(msg: str):
    try:
        await bot.send_message(chat_id=TELEGRAM_USER_ID, text=msg)
    except Exception as e:
        print("Telegram Error:", e)

# Main async sniper loop
async def run_sniper():
    await notify("🚀 Sniper Bot is now live and scanning!")
    client = AsyncClient("https://api.mainnet-beta.solana.com")

    while True:
        try:
            resp = await client.get_balance(WALLET_ADDRESS)
            balance = resp.value / 1e9  # balance in SOL
            print(f"Wallet balance: {balance} SOL")
            await asyncio.sleep(60)
        except Exception as e:
            print("Error:", e)
            await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(run_sniper())
