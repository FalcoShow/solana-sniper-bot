import os
import json
import asyncio
from solana.rpc.async_api import AsyncClient
from solana.keypair import Keypair
from solana.publickey import PublicKey
from telegram import Bot
from dotenv import load_dotenv

load_dotenv()  # Loads environment variables from a .env file if present

# Environment variables
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_USER_ID = int(os.getenv("TELEGRAM_USER_ID"))
PRIVATE_KEY_PATH = os.getenv("PRIVATE_KEY_PATH", "keypair.json")
SOLANA_RPC_URL = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")

bot = Bot(token=TELEGRAM_BOT_TOKEN)

async def notify(msg: str):
    try:
        await bot.send_message(chat_id=TELEGRAM_USER_ID, text=msg)
    except Exception as e:
        print("Telegram Error:", e)

async def get_wallet_balance(pubkey: PublicKey):
    async with AsyncClient(SOLANA_RPC_URL) as client:
        resp = await client.get_balance(pubkey)
        if resp["result"]:
            lamports = resp["result"]["value"]
            sol = lamports / 1e9
            return sol
        else:
            return None

async def main():
    # Load private key from file
    try:
        with open(PRIVATE_KEY_PATH, "r") as f:
            secret_key = json.load(f)  # This should be a list of ints
    except Exception as e:
        print(f"Failed to load keypair file: {e}")
        return

    keypair = Keypair.from_secret_key(bytes(secret_key))
    public_key = keypair.public_key

    balance = await get_wallet_balance(public_key)
    print(f"Wallet balance: {balance} SOL")

    await notify(f"Sniper bot is now live!\nWallet: {public_key}\nBalance: {balance} SOL")

    # Here you can add your sniper bot scanning loop or logic

if __name__ == "__main__":
    asyncio.run(main())
