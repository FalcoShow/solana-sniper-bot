from __future__ import annotations
import os

_USE_PG = str(os.getenv("PG_ENABLED","false")).lower() in {"1","true","yes","on"}
try:
    if _USE_PG:
        from utils_pkg.positions_pg import *  # noqa
    else:
        from utils_pkg.positions import *     # noqa  (your SQLite file)
except Exception:
    # hard fallback to sqlite
    from utils_pkg.positions import *         # noqa
