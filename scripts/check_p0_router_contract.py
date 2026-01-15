# scripts/check_p0_router_contract.py
from __future__ import annotations

import importlib
from pathlib import Path
import sys


REQ = (
    "get_buy_quote",
    "get_sell_quote",
    "swap_from_quote",
    "sell_to_sol",
    "build_swap_payload",
)


def _ensure_repo_root_on_path() -> Path:
    # scripts/ -> repo root
    root = Path(__file__).resolve().parents[1]
    root_str = str(root)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)
    return root


def main() -> int:
    root = _ensure_repo_root_on_path()

    r = importlib.import_module("router")
    ur = importlib.import_module("utils_pkg.router")

    if r is not ur:
        raise SystemExit(
            "router != utils_pkg.router "
            f"(router={getattr(r, '__file__', None)} utils_pkg.router={getattr(ur, '__file__', None)} repo_root={root})"
        )

    missing = [n for n in REQ if not hasattr(r, n)]
    if missing:
        raise SystemExit(f"Missing required exports: {missing}")

    print("OK: P0 router contract locked:", getattr(r, "__file__", None))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
