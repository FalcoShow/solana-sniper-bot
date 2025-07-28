# config.py

# ======= WALLET CONFIG =======
PRIVATE_KEY_PATH = "keypair.json"  # Your Solana private key file
WALLET_ADDRESS = "87tfhxkSeEjSq5tTETcBpZhXX5JDHVhHtgWy9B2VkdzJ"  # Your public Solana wallet

# ======= TELEGRAM CONFIG =======
TELEGRAM_BOT_TOKEN = "8280288862:AAFRrxCIfJoRCkI49QcJeChaKh9I_Q9xwKY"
TELEGRAM_USER_ID = "3308616341"  # Your personal Telegram user ID

# ======= SNIPING RULES =======
MAX_BUY_AMOUNT_SOL = 0.3           # Max SOL to spend per token
MIN_LIQUIDITY_SOL = 2              # Minimum liquidity in SOL to snipe
AUTO_SELL_PROFIT_PERCENT = 120     # Sell when token is up 120%
DCA_EXIT_ENABLED = True
DCA_EXIT_STEPS = [120, 150, 200]   # Auto sell % stages
SLIPPAGE_PERCENT = 2               # 2% slippage protection

# ======= RISK & PROTECTIONS =======
ENABLE_AI_RISK_SCORE = True
ENABLE_HONEYPOT_CHECK = True
ENABLE_WHITELIST_ONLY = False
TOKEN_WHITELIST = []               # Leave empty for now
TOKEN_BLACKLIST = []               # Optional

# ======= SCANNING TARGETS =======
ENABLE_RAYDIUM_SCAN = True
ENABLE_PUMPFUN_SCAN = True
RAYDIUM_INTERVAL_SEC = 4
PUMPFUN_INTERVAL_SEC = 3

# ======= NOTION (Optional) =======
NOTION_ENABLED = False
NOTION_KEY = ""
NOTION_DB_ID = ""

# ======= ADVANCED LOGGING =======
LOG_FILE = "sniper.log"
ENABLE_VERBOSE_LOGGING = True
