from __future__ import annotations

"""
scripts/test_pg_render.py

DEPRECATED WRAPPER (P0 DEDUP) — Render-friendly PG canary
========================================================

Why this exists
---------------
Historically you had multiple PG test scripts (test_pg.py, test_pg_render.py, etc.)
that slowly drifted apart and started competing.

LOCK-IN (P0):
- scripts/test_pg.py is the ONE canonical Postgres canary.
- This file is a *thin wrapper* kept only for backwards compatibility.

What it does
------------
- (Optional) biases DSN selection toward INTERNAL on Render, EXTERNAL locally
  WITHOUT implementing any DSN parsing logic.
- Delegates 100% of logic to:  python -m scripts.test_pg

Usage
-----
Preferred:
  python -m scripts.test_pg

Legacy:
  python -m scripts.test_pg_render
  python scripts/test_pg_render.py

Wrapper flags (removed before forwarding to scripts.test_pg)
------------------------------------------------------------
--no-bias          Disable DSN biasing (env untouched)
--internal-only    Force INTERNAL bias (sets POSTGRES_DSN=PG_DSN_INTERNAL if present)
--external-only    Force EXTERNAL bias (sets POSTGRES_DSN=PG_DSN_EXTERNAL if present)

Env
---
TEST_PG_RENDER_NO_BIAS=1   (same as --no-bias)

Exit codes
----------
Matches scripts.test_pg
"""

import argparse
import contextlib
import os
import sys
from pathlib import Path

_TRUE = {"1", "true", "yes", "y", "on", "t"}


def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    s = v if v is not None else d
    s = str(s).strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    return s


def _envb(k: str, d: bool = False) -> bool:
    s = _envs(k, "")
    if not s:
        return bool(d)
    return s.strip().lower() in _TRUE


def _is_render() -> bool:
    return bool(_envs("RENDER_SERVICE_ID") or _envs("RENDER_INSTANCE_ID") or _envb("RENDER", False))


def _bootstrap_repo_root() -> None:
    try:
        here = Path(__file__).resolve()
    except Exception:
        return

    markers = ("pyproject.toml", "requirements.txt", "setup.cfg", ".git")
    for p in [here.parent, *list(here.parents)]:
        with contextlib.suppress(Exception):
            if any((p / m).exists() for m in markers) or (p / "src").is_dir():
                root = str(p)
                if root and root not in sys.path:
                    sys.path.insert(0, root)
                os.environ.setdefault("REPO_ROOT", root)
                return


def _apply_bias(mode: str) -> None:
    """
    Bias DSN selection WITHOUT parsing DSNs:
    - If POSTGRES_DSN already set, do nothing.
    - Else set POSTGRES_DSN to the desired candidate if present.
    """
    if _envs("POSTGRES_DSN", ""):
        return

    if mode == "internal":
        v = _envs("PG_DSN_INTERNAL", "")
        if v:
            os.environ["POSTGRES_DSN"] = v
        return

    if mode == "external":
        v = _envs("PG_DSN_EXTERNAL", "")
        if v:
            os.environ["POSTGRES_DSN"] = v
        return

    # auto
    if _is_render():
        v = _envs("PG_DSN_INTERNAL", "")
        if v:
            os.environ["POSTGRES_DSN"] = v
    else:
        v = _envs("PG_DSN_EXTERNAL", "")
        if v:
            os.environ["POSTGRES_DSN"] = v


def main() -> None:
    _bootstrap_repo_root()

    # Parse wrapper-only flags (and remove them before forwarding)
    ap = argparse.ArgumentParser(add_help=False)
    ap.add_argument("--no-bias", action="store_true")
    ap.add_argument("--internal-only", action="store_true")
    ap.add_argument("--external-only", action="store_true")

    known, unknown = ap.parse_known_args(sys.argv[1:])

    no_bias = bool(known.no_bias or _envb("TEST_PG_RENDER_NO_BIAS", False))
    if not no_bias:
        if known.internal_only and known.external_only:
            # contradictory; safest is no bias
            pass
        elif known.internal_only:
            _apply_bias("internal")
        elif known.external_only:
            _apply_bias("external")
        else:
            _apply_bias("auto")

    # Forward ALL remaining args to scripts.test_pg
    sys.argv = [sys.argv[0].replace("test_pg_render.py", "test_pg.py"), *unknown]

    # Delegate to canonical script
    try:
        from scripts import test_pg as canonical  # type: ignore
    except Exception:
        # Try module import fallback
        import importlib

        canonical = importlib.import_module("scripts.test_pg")

    canonical.main()  # owns exit codes


if __name__ == "__main__":
    main()
