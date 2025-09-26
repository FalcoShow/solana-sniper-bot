# ==============================================
# shadow_utils.py
# ==============================================
import os, random
from typing import Dict
from utils_pkg.core_utils import send_telegram

_DEF_SAMPLE = float(os.getenv("SHADOW_SAMPLE_RATE", "1.0"))  # 1.0 = log all


def shadow_enabled(worker_env_prefix: str) -> bool:
    """Return True if shadow mode is ON globally or for this worker.
    Env:
      SHADOW_MODE_GLOBAL=true|false
      <PREFIX>_SHADOW_MODE=true|false  e.g., WHALE_SHADOW_MODE
    """
    global_on = os.getenv("SHADOW_MODE_GLOBAL", "false").lower() == "true"
    local_on = os.getenv(f"{worker_env_prefix}_SHADOW_MODE", "false").lower() == "true"
    return global_on or local_on


def shadow_take(worker_env_prefix: str, sample: float = None) -> bool:
    """Sampling gate for shadow logging (doesn't execute trades).
    SHADOW_SAMPLE_RATE=0..1 determines fraction of signals to log.
    """
    if not shadow_enabled(worker_env_prefix):
        return False
    p = float(os.getenv("SHADOW_SAMPLE_RATE", str(sample or _DEF_SAMPLE)))
    p = max(0.0, min(1.0, p))
    return random.random() < p


def shadow_log(worker: str, title: str, ctx: Dict):
    """Send a consistent shadow-mode message."""
    pretty = "\n".join([f"• {k}: {v}" for k, v in ctx.items()])
    return send_telegram(f"👥 SHADOW [{worker}] {title}\n{pretty}")

