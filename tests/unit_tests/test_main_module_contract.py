from __future__ import annotations

import inspect
import os


def test_scripts_main_exports_expected_symbols():
    # Ensure imports are stable and not dependent on env
    os.environ.setdefault("MASTER_PHASE", "0")
    os.environ.setdefault("MAIN_DISABLE_RANDOM_JITTER", "1")

    import scripts.main as m

    # Required exports (what your unit_tests folder is meant to validate)
    required = {
        "Strategy",
        "main",
        "run",
        "loop",
        "run_forever",
        "build_app",
        "build_runtime",
        "build_system",
        "build_services",
    }

    missing = sorted([x for x in required if not hasattr(m, x)])
    assert not missing, f"scripts.main missing exports: {missing}"

    # __all__ should include the public surface (helps integration/import tooling)
    if hasattr(m, "__all__"):
        for name in ("Strategy", "main", "run", "loop", "run_forever"):
            assert name in m.__all__, f"{name} not present in scripts.main.__all__"

    assert inspect.isclass(m.Strategy), "scripts.main.Strategy must be a class"
    assert callable(m.main)
    assert callable(m.run)
    assert callable(m.loop)
    assert callable(m.run_forever)

    for name in ("build_app", "build_runtime", "build_system", "build_services"):
        fn = getattr(m, name)
        assert callable(fn), f"{name} must be callable"


def test_build_app_is_safe_and_returns_contract_dict():
    # This must be safe-mode friendly and must not require network/env secrets
    os.environ["MASTER_PHASE"] = "0"
    os.environ["MAIN_DISABLE_RANDOM_JITTER"] = "1"

    import scripts.main as m

    out = m.build_app(safe=True)
    assert isinstance(out, dict)
    assert out.get("safe") is True
    assert "contracts" in out
    assert isinstance(out["contracts"], dict)
    assert "kwargs" in out
    assert isinstance(out["kwargs"], dict)
