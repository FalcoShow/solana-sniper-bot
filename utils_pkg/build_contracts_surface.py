from __future__ import annotations

import importlib.util
from typing import Any


def _exists(mod: str) -> bool:
    try:
        return importlib.util.find_spec(mod) is not None
    except Exception:
        return False


def build_contracts_surface() -> dict[str, Any]:
    """
    Safe, no-network contract surface used by scripts.main.build_app(...).

    This MUST be import-safe:
    - No wallet/redis/db/network initialization
    - Only reports what appears present in the repo
    """
    return {
        "present_modules": {
            "utils_pkg.router": _exists("utils_pkg.router"),
            "utils_pkg.ai_db": _exists("utils_pkg.ai_db"),
            "utils_pkg.ai_risk_model": _exists("utils_pkg.ai_risk_model"),
            "utils_pkg.ai_buy_scoring": _exists("utils_pkg.ai_buy_scoring"),
            "utils_pkg.exit_engine": _exists("utils_pkg.exit_engine"),
            "utils_pkg.auto_exit_worker.exit_engine": _exists(
                "utils_pkg.auto_exit_worker.exit_engine"
            ),
            "utils_pkg.reinvestor": _exists("utils_pkg.reinvestor"),
            "utils_pkg.smart_wallets": _exists("utils_pkg.smart_wallets"),
            "scanner.raydium_scanner": _exists("scanner.raydium_scanner"),
            "scanner.pumpfun_scanner": _exists("scanner.pumpfun_scanner"),
            "scanner.dexscreener_scanner": _exists("scanner.dexscreener_scanner"),
        },
        "contract_version": "v1",
    }
