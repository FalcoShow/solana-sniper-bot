"""
daytrader
=========

Optional, self-contained *Daytrader* subsystem.

God-Tier Overhaul contract (NO DRIFT / NO COMPETING ENGINES)
- This package must NOT auto-run anything on import (no hidden loops, no side effects).
- This package must NOT silently compete with the sniper path.
- This package is safe to import even when optional deps are missing:
  it exposes what is available and degrades gracefully.

Design
- data_feed.py   : async market data helpers (USD bars + caching)
- features.py    : TA + rule/AI blend decision helpers (StrategyDirector, intents)
- alpha.py       : lightweight alpha model interface (optional)
- broker.py      : execution bridge (calls into core_utils swap)
- position.py    : daytrader-only position state (atomic JSON)
- risk.py        : sizing + portfolio gates + trailing helpers
- runner.py      : orchestration loop (optional entrypoint for daytrader)

Import guidance
- Prefer importing explicitly from submodules in production code:
    from daytrader.runner import DaytraderRunner
    from daytrader.data_feed import get_minute_bars
- This __init__ only provides convenient re-exports for interactive use.

Versioning
- Keep a simple __version__ string for troubleshooting.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

__version__ = "0.1.0"

# -----------------------------------------------------------------------------
# Lazy, safe re-exports (PEP 562)
#
# Why lazy?
# - Zero side effects on import.
# - Avoids ruff F401 unused-import warnings.
# - Optional modules can fail without breaking `import daytrader`.
# -----------------------------------------------------------------------------

# attr_name -> (submodule, symbol_name)
_EXPORTS: dict[str, tuple[str, str]] = {
    # --- data_feed -----------------------------------------------------------
    "MINTS": ("data_feed", "MINTS"),
    "get_minute_bars": ("data_feed", "get_minute_bars"),
    "get_recent_price": ("data_feed", "get_recent_price"),
    # --- features ------------------------------------------------------------
    "ema": ("features", "ema"),
    "sma": ("features", "sma"),
    "atr": ("features", "atr"),
    "atr_pct": ("features", "atr_pct"),
    "rsi": ("features", "rsi"),
    "vwap": ("features", "vwap"),
    "donchian": ("features", "donchian"),
    "volume_burst": ("features", "volume_burst"),
    "liquidity_usd_proxy": ("features", "liquidity_usd_proxy"),
    "zscore": ("features", "zscore"),
    "pack_features": ("features", "pack_features"),
    "StrategyConfig": ("features", "StrategyConfig"),
    "TradeIntent": ("features", "TradeIntent"),
    "RulesEngineStrategy": ("features", "RulesEngineStrategy"),
    "StrategyDirector": ("features", "StrategyDirector"),
    # --- alpha ---------------------------------------------------------------
    "AlphaModel": ("alpha", "AlphaModel"),
    "generate_signal": ("alpha", "generate_signal"),
    # --- broker --------------------------------------------------------------
    "TOKENS": ("broker", "TOKENS"),
    "place_order": ("broker", "place_order"),
    # --- position ------------------------------------------------------------
    "Position": ("position", "Position"),
    "load_position": ("position", "load_position"),
    "save_position": ("position", "save_position"),
    "list_positions": ("position", "list_positions"),
    "close_position": ("position", "close_position"),
    "close_all_positions": ("position", "close_all_positions"),
    "new_position": ("position", "new_position"),
    # --- risk ----------------------------------------------------------------
    "RiskManager": ("risk", "RiskManager"),
    "OpenContext": ("risk", "OpenContext"),
    "RiskDecision": ("risk", "RiskDecision"),
    "can_open_position": ("risk", "can_open_position"),
    "compute_trade_size": ("risk", "compute_trade_size"),
    "stops_from_atr": ("risk", "stops_from_atr"),
    "update_trailing_stop": ("risk", "update_trailing_stop"),
    "risk_summary": ("risk", "risk_summary"),
    # --- runner --------------------------------------------------------------
    "DaytraderRunner": ("runner", "DaytraderRunner"),
    "main": ("runner", "main"),
}

__all__ = sorted(_EXPORTS.keys())


def __getattr__(name: str) -> Any:
    """
    Lazy attribute resolver for re-exports.

    - If the requested export exists, attempt to import its submodule and return
      the symbol. Cache it into this module's globals to make subsequent access fast.
    - If the submodule/symbol isn't available (optional deps missing), raise
      AttributeError (clean degradation).
    """
    spec = _EXPORTS.get(name)
    if spec is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    submodule, symbol = spec
    try:
        mod = import_module(f"{__name__}.{submodule}")
        obj = getattr(mod, symbol)
    except Exception as e:
        # Degrade gracefully: behave like attribute doesn't exist.
        raise AttributeError(
            f"{__name__!r} export {name!r} unavailable (failed importing {submodule}.{symbol}): {e}"
        ) from None

    globals()[name] = obj  # cache
    return obj


def __dir__() -> list[str]:
    # Improve IDE/REPL discovery without importing submodules.
    return sorted(set(globals().keys()) | set(__all__))
