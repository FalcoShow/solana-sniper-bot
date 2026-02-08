#!/usr/bin/env python3
"""
bootstrap_pre_overlay.py (compatibility shim)

Canonical implementation lives at:
  scripts/bootstrap_pre_overlay.py

This shim exists so any older docs/commands that run:
  python bootstrap_pre_overlay.py
continue to work without ambiguity.
"""

from __future__ import annotations

from scripts.bootstrap_pre_overlay import main

if __name__ == "__main__":
    raise SystemExit(main())
