# conftest.py (REPO ROOT)
from __future__ import annotations

import contextlib
import importlib.util
import sys
from pathlib import Path
from types import ModuleType

"""
Repo-root pytest bootstrap (stdlib-only)

Hard guarantees:
- Repo root is importable (works under pytest importlib mode too).
- router resolves to repo-root router.py and is aliased to utils_pkg.router.
- mint_guard resolves to repo-root mint_guard.py shim (or utils_pkg.mint_guard fallback).
- CRITICAL: ensure name `mg` always resolves (via builtins), even if tests swallow
  an import error and never assign `mg` in their module globals.
"""

ROOT = Path(__file__).resolve().parent

# Ensure repo root is importable
root_s = str(ROOT)
if root_s not in sys.path:
    sys.path.insert(0, root_s)


def _force_load_module(name: str, path: Path) -> ModuleType:
    """
    Force-load a module from an explicit file path into sys.modules[name].
    Registers in sys.modules BEFORE exec to avoid recursion issues.
    """
    path = path.resolve()
    spec = importlib.util.spec_from_file_location(name, str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not create module spec for {name} at {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


# ──────────────────────────────────────────────────────────────────────────────
# router: always bind to repo-root router.py
# ──────────────────────────────────────────────────────────────────────────────
def _load_router() -> ModuleType:
    p = ROOT / "router.py"
    if not p.exists():
        raise RuntimeError(f"router.py not found at {p}")
    return _force_load_module("router", p)


try:
    import router as _router
except Exception:
    _router = _load_router()

sys.modules["router"] = _router
sys.modules["utils_pkg.router"] = _router

with contextlib.suppress(Exception):
    import utils_pkg  # type: ignore

    utils_pkg.router = _router  # type: ignore[attr-defined]

with contextlib.suppress(Exception):
    import builtins as _builtins

    _builtins.router = _router  # type: ignore[attr-defined]


# ──────────────────────────────────────────────────────────────────────────────
# mint_guard: force repo-root shim, fallback to utils_pkg.mint_guard
# ──────────────────────────────────────────────────────────────────────────────
def _load_mint_guard() -> ModuleType | None:
    # 1) Prefer repo-root shim if present
    p = ROOT / "mint_guard.py"
    if p.exists():
        try:
            return _force_load_module("mint_guard", p)
        except Exception:
            return None

    # 2) Fallback to package module
    with contextlib.suppress(Exception):
        import utils_pkg.mint_guard as _pkg_mg  # type: ignore

        sys.modules["mint_guard"] = _pkg_mg
        return _pkg_mg

    return None


_mg_mod = _load_mint_guard()
if _mg_mod is not None:
    sys.modules["mint_guard"] = _mg_mod

    # Optional convenience alias on utils_pkg
    with contextlib.suppress(Exception):
        import utils_pkg  # type: ignore

        utils_pkg.mint_guard = _mg_mod  # type: ignore[attr-defined]

    # THE FIX: make `mg` resolvable even if tests never assign it
    with contextlib.suppress(Exception):
        import builtins as _builtins

        _builtins.mint_guard = _mg_mod  # type: ignore[attr-defined]
        _builtins.mg = _mg_mod  # type: ignore[attr-defined]
