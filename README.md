# Render deployment (Worker)

## Service
Worker

## Runtime
Python 3.11

## Build
pip install -r requirements.txt

## Start (pick one)
# Full loop:
python -m sniper.sniper_bot
# or simple:
# python scripts/auto_sell.py
# or smoke:
# python -m scripts.healthcheck

## Environment (Secrets unless noted)
WALLET_PRIVATE_KEY=...
WALLET_PUBKEY=...
HELIUS_RPC_URL=https://mainnet.helius-rpc.com/?api-key=...

JUPITER_URL=https://quote-api.jup.ag/v6
SLIPPAGE_BPS=100
PRIORITY_FEE_MICROLAMPORTS=0

REDIS_URL=redis://default:<pwd>@<host>:6379/0
PG_DSN_EXTERNAL=postgresql+asyncpg://user:pass@host:5432/db
PG_DSN_INTERNAL=postgresql+asyncpg://user:pass@internal-host:5432/db

NOTION_API_KEY=secret_xxx
NOTION_DB_TRADES=<db_id>
NOTION_TITLE_PROP=Token
NOTION_PROP_TOKEN=Mint
NOTION_PROP_ENTRY=Entry Price
NOTION_PROP_CURRENT=Current Price
NOTION_PROP_PROFIT=Profit
NOTION_PROP_REASON=Reason
NOTION_PROP_TIME=Timestamp

TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...

OPENAI_API_KEY=...    # leave OPENAI_BASE_URL unset unless using a proxy
BIRDEYE_API_KEY=...

ENABLE_WHALE_FILTER=true
DYNAMIC_WHALE_DISCOVERY=true
# CURATED_WHALE_WALLETS=@config/whales.csv

AI_MIN_SCORE=68
AI_W_LIQ=0.25
AI_W_AGE=0.10
AI_W_IMPACT=0.15
AI_W_VOL=0.10
AI_W_WHALES=0.25
AI_W_CONC=-0.15
AI_W_SOCIAL=0.0s

STRICT_ENV=true
