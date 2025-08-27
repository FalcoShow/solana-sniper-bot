# utils.py
# Compatibility shim so modules that do `from utils import ...`
# continue to work. We re-export the public API from utils_pkg.core_utils.
from utils_pkg.core_utils import *  # noqa: F401,F403
