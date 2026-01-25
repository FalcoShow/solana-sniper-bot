# ==============================================
# shadow_utils.py  (ARCHIVE / compat-safe)
# ==============================================
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import random
import time
from collections.abc import Mapping
from typing import Any

from utils_pkg.core_utils import send_telegram  # legacy dependency; may be sync or async

# ----------------------------
# Env contract (compat)
# ----------------------------
# Global switch:
#   SHADOW_MODE_GLOBAL=true|false|1|0
# Per-worker switch:
#   <PREFIX>_SHADOW_MODE=true|false|1|0   e.g. WHALE_SHADOW_MODE
#
# Sampling:
#   SHADOW_SAMPLE_RATE=0..1  (default 1.0)
#
# Rate limit (per process):
#   SHADOW_RATE_LIMIT_PER_MIN=0 disables (default 60)
#
# Redaction:
#   SHADOW_REDACT_KEYS=token,secret,key,pass,private,dsn
#

_DEFAULT_SAMPLE = float(os.getenv("SHADOW_SAMPLE_RATE", "1.0") or "1.0")
_DEFAULT_RL = int(float(os.getenv("SHADOW_RATE_LIMIT_PER_MIN", "60") or "60"))
_REDACT_KEYS = {
    k.strip().lower()
    for k in (
        os.getenv("SHADOW_REDACT_KEYS", "token,secret,key,pass,password,private,dsn") or ""
    ).split(",")
    if k.strip()
}

# simple per-process rate limiter
_rl_window_start = 0.0
_rl_count = 0


def _truthy(v: str | None) -> bool:
    if v is None:
        return False
    v = str(v).strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def shadow_enabled(worker_env_prefix: str) -> bool:
    """
    True if shadow mode is ON globally or for this worker prefix.
    """
    global_on = _truthy(os.getenv("SHADOW_MODE_GLOBAL", "false"))
    local_on = _truthy(os.getenv(f"{worker_env_prefix}_SHADOW_MODE", "false"))
    return bool(global_on or local_on)


def _clamp01(x: float) -> float:
    try:
        x = float(x)
    except Exception:
        return 0.0
    return max(0.0, min(1.0, x))


def _rate_limit_ok() -> bool:
    """
    Best-effort local rate limit so we don't spam Telegram.
    """
    global _rl_window_start, _rl_count
    limit = _DEFAULT_RL
    if limit <= 0:
        return True
    now = time.time()
    if _rl_window_start <= 0 or (now - _rl_window_start) >= 60.0:
        _rl_window_start = now
        _rl_count = 0
    if _rl_count >= limit:
        return False
    _rl_count += 1
    return True


def shadow_take(
    worker_env_prefix: str, sample: float | None = None, *, key: str | None = None
) -> bool:
    """
    Sampling gate for shadow logging (doesn't execute trades).
    - If key is provided, sampling becomes stable per key (no flicker).
    """
    if not shadow_enabled(worker_env_prefix):
        return False

    p = _clamp01(sample if sample is not None else _DEFAULT_SAMPLE)
    if p <= 0:
        return False
    if p >= 1:
        return True

    if key:
        # stable hash sampling: same key => same decision for this process
        h = hashlib.sha256(key.encode("utf-8")).digest()
        n = int.from_bytes(h[:8], "big") / float(2**64)
        return n < p

    return random.random() < p


def _redact_pair(k: str, v: Any) -> Any:
    kl = (k or "").lower()
    if any(bad in kl for bad in _REDACT_KEYS):
        return "***REDACTED***"
    return v


def _safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, default=str)
    except Exception:
        return str(obj)


def _fmt_ctx(ctx: Mapping[str, Any], *, max_items: int = 30) -> str:
    items = list(ctx.items())[:max_items]
    lines = []
    for k, v in items:
        vv = _redact_pair(str(k), v)
        lines.append(f"• {k}: {vv}")
    if len(ctx) > max_items:
        lines.append(f"• ...: (+{len(ctx) - max_items} more)")
    return "\n".join(lines)


async def _maybe_await(x: Any) -> None:
    if asyncio.iscoroutine(x):
        await x


async def shadow_log_async(
    worker: str, title: str, ctx: dict[str, Any], *, hard: bool = False
) -> bool:
    """
    Async shadow logger.
    - hard=False: best effort; swallow all errors.
    - hard=True: raise if send fails (rarely desired).
    """
    try:
        if not _rate_limit_ok():
            return False

        safe_ctx = dict(ctx or {})
        pretty = _fmt_ctx(safe_ctx)
        msg = f"👥 SHADOW [{worker}] {title}\n{pretty}".strip()

        res = send_telegram(msg)
        await _maybe_await(res)
        return True
    except Exception:
        if hard:
            raise
        return False


def shadow_log(worker: str, title: str, ctx: dict[str, Any], *, hard: bool = False) -> bool:
    """
    Sync wrapper for shadow_log_async.
    Safe to call from non-async code.
    """
    try:
        coro = shadow_log_async(worker, title, ctx, hard=hard)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            asyncio.run(coro)
        else:
            loop.create_task(coro)
        return True
    except Exception:
        if hard:
            raise
        return False
