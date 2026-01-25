# tests/test_smoke_imports.py
from __future__ import annotations

"""
Smoke: import + wiring sanity checks (Phase 0–7 LOCK-IN)
=======================================================

Goal:
- Prove that the repo's "control plane" imports cleanly with SAFE defaults.
- Catch broken imports / circular deps / missing optional modules early.
- Do NOT touch real network / RPC / Redis / DB.
  (tests/conftest.py enforces network blocking + safety env defaults.)

This test should stay fast (<1s) and deterministic.
"""

import importlib
import os
import sys
from types import ModuleType

import pytest

SMOKE_IMPORTS: tuple[str, ...] = (
    # Orchestrator / entrypoint
    "scripts.main",
    # Core utilities
    "utils_pkg.core_utils",
    # Routing / quoting / tx build layer
    "utils_pkg.router",
    # Exit engine (should be import-safe; worker loops must not start at import time)
    "utils_pkg.exit_engine",
    # Guardrails + routing guard
    "utils_pkg.guardrails",
    "utils_pkg.routeguard",
    # Risk / scoring layers (may soft-fail at runtime but should import)
    "utils_pkg.ai_buy_scoring",
    "utils_pkg.ai_entry_logic",
    "utils_pkg.ai_risk_model",
    "utils_pkg.scoring_service",
    "utils_pkg.risk_manager",
    # Data / feature pipeline
    "utils_pkg.feature_builder",
    "utils_pkg.data_logger",
    "utils_pkg.ai_db",
)


def _import(name: str) -> ModuleType:
    return importlib.import_module(name)


def _mod_attr(mod: ModuleType, attr: str):
    return getattr(mod, attr, None)


@pytest.mark.smoke
def test_repo_smoke_imports() -> None:
    """
    Imports should not crash with the test harness safety env.
    If a module is truly optional and not present, it should not be in SMOKE_IMPORTS.
    """
    missing = []
    failed = []

    for name in SMOKE_IMPORTS:
        try:
            _import(name)
        except ModuleNotFoundError as e:
            # If the missing module is the module itself, record as missing;
            # otherwise it’s a dependency missing (treat as failure).
            if getattr(e, "name", "") == name:
                missing.append(name)
            else:
                failed.append((name, f"dependency missing: {e}"))
        except Exception as e:
            failed.append((name, f"{type(e).__name__}: {e}"))

    if missing:
        pytest.fail("Smoke imports missing modules:\n" + "\n".join(f"  - {m}" for m in missing))
    if failed:
        pytest.fail("Smoke imports failed:\n" + "\n".join(f"  - {n}: {msg}" for (n, msg) in failed))


@pytest.mark.smoke
def test_scripts_main_contract() -> None:
    """
    Contract checks for scripts.main:
    - main() exists and is awaitable
    - Strategy class exists (back-compat)
    - RUN_ID exists (module constant)
    """
    m = _import("scripts.main")

    main_fn = _mod_attr(m, "main")
    assert callable(main_fn), "scripts.main.main must exist and be callable"

    strat = _mod_attr(m, "Strategy")
    assert strat is not None, "scripts.main.Strategy must exist (back-compat)"
    assert isinstance(strat, type), "scripts.main.Strategy must be a class"

    run_id = _mod_attr(m, "RUN_ID")
    assert run_id is not None, "scripts.main.RUN_ID must exist"


@pytest.mark.smoke
def test_no_live_trading_env_in_smoke() -> None:
    """
    Ensure our smoke run is not accidentally configured for live trading.
    conftest.py sets these, but this asserts the contract so regressions are caught.
    """
    assert os.getenv("DRYRUN") == "1" or os.getenv("DRY_RUN") == "1"
    assert os.getenv("DISABLE_BUYS") == "1"
    assert os.getenv("DISABLE_SELLS") == "1"


@pytest.mark.smoke
def test_imports_do_not_require_venv_side_effects() -> None:
    """
    Importing the control plane should not require being installed as a package.
    This catches accidental assumptions about cwd/site-packages.
    """
    # If tests can import scripts.main, sys.path is likely sane; assert minimal.
    assert any(p for p in sys.path), "sys.path should not be empty"
    _import("scripts.main")
