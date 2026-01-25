# mint_guard.py (repo root)
"""
Compatibility shim.

Some test setups or legacy code may do: `import mint_guard as mg`
instead of `import utils_pkg.mint_guard as mg`.

This shim guarantees that import always works.
"""

from utils_pkg.mint_guard import *  # noqa: F403
