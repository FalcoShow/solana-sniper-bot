# utils_pkg/__init__.py
# Defensive re-export layer so imports never break if core_utils changes.

from . import core_utils as _cu  # noqa: F401

# List everything we *might* want to export. We’ll only export what exists.
_CANDIDATE_EXPORTS = [
    # init / teardown
    "init_resources", "close_resources", "init_sqlite", "init_postgres",
    "get_session", "get_redis", "get_solana_client", "get_wallet_pubkey",

    # caching / seen
    "cache_sniped_token", "was_token_sniped",
    "save_entry_price", "get_entry_price",
    "get_entry_price_from_redis", "save_entry_price_to_redis",
    "cache_arbitrage_hit", "was_arbitrage_hit",  # ok if missing

    # pricing / info
    "get_sol_usd", "convert_sol_to_usd", "convert_usd_to_sol",
    "get_token_price", "get_pair_snapshot_dexscreener", "get_volume_data",
    "get_ohlcv_birdeye", "get_orderflow_snapshot", "get_recent_volatility",

    # alerts / logging
    "send_telegram", "send_discord_alert", "log_sniper_event", "log_trade_event",

    # wallet
    "get_wallet_tokens", "get_sol_balance", "is_wallet_drained",

    # ai
    "ai_fraud_score", "ai_gate_candidate",

    # jupiter
    "get_jupiter_quote", "perform_jupiter_swap", "swap_token_jupiter",

    # savings
    "transfer_to_savings",
]

# Export only what actually exists in core_utils
__all__ = []
for _name in _CANDIDATE_EXPORTS:
    if hasattr(_cu, _name):
        globals()[_name] = getattr(_cu, _name)
        __all__.append(_name)
