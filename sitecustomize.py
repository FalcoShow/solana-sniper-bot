# sitecustomize.py
from __future__ import annotations

import os

# Only disable pytest plugin autoload if you explicitly ask for it.
# This prevents breaking pytest-asyncio ini options during normal runs.
if os.getenv("SNIPER_DISABLE_PYTEST_PLUGIN_AUTOLOAD", "").strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
    "on",
    "t",
}:
    os.environ.setdefault("PYTEST_DISABLE_PLUGIN_AUTOLOAD", "1")
