from __future__ import annotations

"""
utils_pkg/ai_entry_logic.py

Entry IQ engine (V3+++ / God-Tier Overhaul) — production lock-in
================================================================

Role
----
Given an ENTRY-TIME feature snapshot, decide:
  • score in [0, 1]            -> score_entry()
  • enter? + reason            -> should_enter()
  • decision dict (json-safe)  -> decide_entry()
  • structured decision object -> decide_entry_obj()

Hard requirements
-----------------
- Deterministic baseline ALWAYS works (no network; no randomness in scoring).
- Optional OpenAI overlay (fail-soft; throttled; timeboxed; bounded concurrency).
- Reasons are ALWAYS non-empty for decisions.
- Avoid import cycles (risk_manager/data_logger/regime imported locally & optional).
- Mutates input features ONLY by attaching/merging `features["entry_meta"]` (a dict).
  (No other top-level keys are added/modified.)

Phase A — Logging + typing contract
-----------------------------------
- Eliminate Logger ↔ EventLogger incompatibilities:
  ✅ .handlers present
  ✅ .info/.warning/.error/.debug present
  ✅ .event/.every present
- No monkeypatching. No method-assign.

Public API (signature-stable, test-friendly)
--------------------------------------------
  async score_entry(features: dict, ctx: dict | None = None) -> float
  async should_enter(features: dict, ctx: dict | None = None) -> tuple[bool, float, str]
  def score_entry_sync(features: dict, ctx: dict | None = None) -> float

  async decide_entry_obj(features: dict, ctx: dict | None = None) -> EntryDecision
  def decide_entry_sync_obj(features: dict, ctx: dict | None = None) -> EntryDecision

  async decide_entry(features: dict, ctx: dict | None = None) -> dict   # JSON-safe
  def decide_entry_sync(features: dict, ctx: dict | None = None) -> dict

  async diag_snapshot() -> dict
  async ai_maintenance_loop(stop_evt: asyncio.Event) -> None
"""

import asyncio
import contextlib
import json
import math
import os
import re
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

# -----------------------------------------------------------------------------
# Contract guards (NO IO / NO NETWORK)
# -----------------------------------------------------------------------------
_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}

_JSON_EXTRACT_RE = re.compile(r"\{.*\}", re.DOTALL)


def _contract_no_io() -> bool:
    """
    Hard guard used by contract harness / CI:
    When active, this module must avoid Redis/network calls and any best-effort IO.
    """
    v = os.getenv("NETWORK", "").strip().lower()
    if v in ("off", "0", "false", "no"):
        return True

    # Common contract/CI flags
    if os.getenv("SNIPER_TEST_CONTRACT", "").strip() == "1":
        return True
    if os.getenv("CONTRACT_TEST", "").strip() == "1":
        return True
    if os.getenv("PYTEST_MODE", "").strip().lower() in ("1", "true", "yes", "on"):
        return True
    if os.getenv("NO_NETWORK", "").strip().lower() in ("1", "true", "yes", "on"):
        return True
    if os.getenv("DISABLE_NETWORK", "").strip().lower() in ("1", "true", "yes", "on"):
        return True

    # Optional: if you use these toggles elsewhere
    if os.getenv("DISABLE_REDIS", "").strip().lower() in ("1", "true", "yes", "on"):
        return True
    return os.getenv("REDIS_ENABLED", "").strip().lower() in ("0", "false", "off", "no")


# -----------------------------------------------------------------------------
# Logger contract (Phase A): handlers + classic methods + event/every
# -----------------------------------------------------------------------------
class LoggerLike(Protocol):
    @property
    def handlers(self) -> Sequence[Any]: ...

    def info(self, msg: str, *a: Any, **k: Any) -> Any: ...
    def warning(self, msg: str, *a: Any, **k: Any) -> Any: ...
    def error(self, msg: str, *a: Any, **k: Any) -> Any: ...
    def debug(self, msg: str, *a: Any, **k: Any) -> Any: ...

    def event(self, name: str, **fields: Any) -> Any: ...
    def every(self, key: str, n: int = 30, msg: str | None = None, **fields: Any) -> Any: ...


class _LoggerAdapter:
    """
    Adapter that guarantees:
      - .handlers exists (read-only)
      - .info/.warning/.error/.debug exist
      - .event/.every exist
    WITHOUT mutating the underlying logger.
    """

    __slots__ = ("_get", "_every_store")

    def __init__(self, supplier: Callable[[], Any]) -> None:
        self._get = supplier
        self._every_store: dict[str, float] = {}

    def _safe(self) -> Any:
        with contextlib.suppress(Exception):
            return self._get()
        return None

    @property
    def handlers(self) -> Sequence[Any]:
        lg = self._safe()
        hs = getattr(lg, "handlers", None) if lg is not None else None
        if isinstance(hs, (list, tuple)):
            return hs
        return []

    def _call(self, method: str, msg: str, *a: Any, **k: Any) -> None:
        lg = self._safe()
        if lg is None:
            return
        fn = getattr(lg, method, None)
        if callable(fn):
            with contextlib.suppress(Exception):
                fn(msg, *a, **k)

    def info(self, msg: str, *a: Any, **k: Any) -> None:
        self._call("info", msg, *a, **k)

    def warning(self, msg: str, *a: Any, **k: Any) -> None:
        self._call("warning", msg, *a, **k)

    def error(self, msg: str, *a: Any, **k: Any) -> None:
        self._call("error", msg, *a, **k)

    def debug(self, msg: str, *a: Any, **k: Any) -> None:
        self._call("debug", msg, *a, **k)

    def event(self, name: str, **fields: Any) -> None:
        lg = self._safe()
        if lg is None:
            return
        fn = getattr(lg, "event", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                fn(name, **fields)
                return
        self.info("%s %s", name, fields)

    def every(self, key: str, n: int = 30, msg: str | None = None, **fields: Any) -> None:
        try:
            now = time.monotonic()
            sec = float(max(1, int(n)))
            last = float(self._every_store.get(key, 0.0))
            if (now - last) >= sec:
                self._every_store[key] = now
                self.event(msg or key, **fields)
        except Exception:
            return


def _resolve_logger() -> Any:
    # Prefer your repo's compat shim if present (Phase A)
    with contextlib.suppress(Exception):
        from utils_pkg.log_compat import get_logger as gl  # type: ignore

        return gl("utils_pkg.ai_entry_logic")

    # Legacy path: some repos keep logging_setup at root or under utils_pkg
    with contextlib.suppress(Exception):
        from utils_pkg.logging_setup import get_logger as gl2  # type: ignore

        try:
            return gl2("utils_pkg.ai_entry_logic")
        except TypeError:
            return gl2()

    with contextlib.suppress(Exception):
        from logging_setup import get_logger as gl3  # type: ignore

        try:
            return gl3("utils_pkg.ai_entry_logic")
        except TypeError:
            return gl3()

    import logging

    lg = logging.getLogger("utils_pkg.ai_entry_logic")
    if not getattr(lg, "handlers", None):
        logging.basicConfig(level=logging.INFO)
    return lg


log: LoggerLike = _LoggerAdapter(_resolve_logger)


def _log_event(name: str, **fields: Any) -> None:
    """Best-effort structured log. Never throws."""
    with contextlib.suppress(Exception):
        log.event(name, **fields)


def _log_every(key: str, n: int = 200) -> bool:
    """Cheap sampling guard (logging-only)."""
    try:
        return (hash(key) ^ int(time.time() // 5)) % max(1, int(n)) == 0
    except Exception:
        return False


# -----------------------------------------------------------------------------
# Env helpers (keep keys stable; do not invent new)
# -----------------------------------------------------------------------------
def _env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return default


def _env_float(key: str, default: float) -> float:
    v = os.getenv(key)
    if v is None or str(v).strip() == "":
        return float(default)
    try:
        return float(v)
    except Exception:
        return float(default)


def _env_int(key: str, default: int) -> int:
    v = os.getenv(key)
    if v is None or str(v).strip() == "":
        return int(default)
    try:
        return int(float(v))
    except Exception:
        return int(default)


def _env_json_dict(key: str) -> dict[str, Any] | None:
    raw = os.getenv(key)
    if not raw:
        return None
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else None
    except Exception:
        _log_event("ENTRY_ENV_JSON_PARSE_FAIL", key=key)
        return None


# -----------------------------------------------------------------------------
# Numeric safety
# -----------------------------------------------------------------------------
def _now() -> float:
    return time.time()


def _finite(x: Any, default: float = 0.0) -> float:
    try:
        xf = float(x)
        return xf if math.isfinite(xf) else float(default)
    except Exception:
        return float(default)


def _clip(x: Any, lo: float, hi: float) -> float:
    xf = _finite(x, lo)
    if xf < lo:
        return lo
    if xf > hi:
        return hi
    return xf


def _clip01(x: Any) -> float:
    return _clip(x, 0.0, 1.0)


def _sigmoid(x: Any) -> float:
    xf = _finite(x, 0.0)
    if xf >= 35.0:
        return 1.0
    if xf <= -35.0:
        return 0.0
    return 1.0 / (1.0 + math.exp(-xf))


def _log1p_pos(x: Any) -> float:
    xf = _finite(x, 0.0)
    if xf <= 0.0:
        return 0.0
    return math.log1p(xf)


def _f(d: dict[str, Any], key: str, default: float = 0.0) -> float:
    return _finite(d.get(key, default), default)


def _i(d: dict[str, Any], key: str, default: int = 0) -> int:
    try:
        v = d.get(key, default)
        if v is None:
            return int(default)
        return int(float(v))
    except Exception:
        return int(default)


def _reason(s: str | None) -> str:
    s2 = (s or "").strip()
    return s2 if s2 else "unknown"


# -----------------------------------------------------------------------------
# Meta helpers (ONLY allowed mutation surface)
# -----------------------------------------------------------------------------
def _ensure_meta(features: dict[str, Any]) -> dict[str, Any]:
    m = features.get("entry_meta")
    if isinstance(m, dict):
        return m
    m2: dict[str, Any] = {}
    features["entry_meta"] = m2
    return m2


def _meta_ctx(meta: dict[str, Any]) -> dict[str, Any]:
    ctx = meta.get("ctx")
    if isinstance(ctx, dict):
        return ctx
    ctx2: dict[str, Any] = {}
    meta["ctx"] = ctx2
    return ctx2


def _merge_ctx_best_effort(features: dict[str, Any], ctx: dict | None) -> None:
    """
    Merge external ctx into entry_meta.ctx (best-effort, non-destructive).
    """
    if not isinstance(ctx, dict) or not ctx:
        return
    meta = _ensure_meta(features)
    mctx = _meta_ctx(meta)
    for k, v in ctx.items():
        if k not in mctx:
            mctx[k] = v


def _append_flag(meta: dict[str, Any], flag: str) -> None:
    try:
        flags = meta.get("flags")
        if not isinstance(flags, list):
            flags = []
        if flag not in flags:
            flags.append(flag)
        meta["flags"] = flags
    except Exception:
        return


def _ctx_get(features: dict[str, Any], meta: dict[str, Any], key: str, default: Any = None) -> Any:
    """
    Read order:
      1) features[key]
      2) entry_meta.ctx[key]
      3) entry_meta[key]
      4) default
    """
    if key in features:
        return features.get(key)
    ctx = meta.get("ctx")
    if isinstance(ctx, dict) and key in ctx:
        return ctx.get(key)
    if key in meta:
        return meta.get(key)
    return default


def _one_line_explain(features: dict[str, Any], prefix: str = "") -> str:
    prefix = _reason(prefix)
    try:
        meta = features.get("entry_meta") or {}
        b = meta.get("breakdown") or {}
        if not isinstance(b, dict) or not b:
            return prefix

        items: list[tuple[str, float]] = []
        for k, v in b.items():
            fv = _finite(v, 0.0)
            if abs(fv) < 0.06:
                continue
            items.append((k, fv))

        if not items:
            return prefix

        pos = sorted([it for it in items if it[1] > 0], key=lambda x: -x[1])[:2]
        neg = sorted([it for it in items if it[1] < 0], key=lambda x: x[1])[:2]

        parts: list[str] = []
        if pos:
            parts.append("top+=" + ",".join([f"{k}:{v:+.2f}" for k, v in pos]))
        if neg:
            parts.append("top-=" + ",".join([f"{k}:{v:+.2f}" for k, v in neg]))

        expl = " ".join(parts).strip()
        return f"{prefix} | {expl}" if expl else prefix
    except Exception:
        return prefix


# -----------------------------------------------------------------------------
# Config (env keys kept stable)
# -----------------------------------------------------------------------------
ENTRY_MODEL = (os.getenv("ENTRY_MODEL_NAME", "") or os.getenv("ENTRY_MODEL", "")).strip()
ENTRY_MODEL_ROUTING = _env_json_dict("ENTRY_MODEL_ROUTING_JSON") or {}
ENTRY_DEFAULT_HEAD = (os.getenv("ENTRY_DEFAULT_HEAD", "default") or "default").strip()

ENTRY_AI_ENABLED = _env_bool("ENTRY_AI_ENABLED", True)
ENTRY_AI_TIMEOUT_SEC = max(1.0, _env_float("ENTRY_AI_TIMEOUT_SEC", 8.0))
ENTRY_AI_TEMPERATURE = _clip(_env_float("ENTRY_AI_TEMPERATURE", 0.0), 0.0, 0.8)
ENTRY_AI_MAX_TOKENS = max(32, _env_int("ENTRY_AI_MAX_TOKENS", 140))
ENTRY_AI_STRICT = _env_bool("ENTRY_AI_STRICT", False)
ENTRY_AI_COOLDOWN_SEC = max(0.0, _env_float("ENTRY_AI_COOLDOWN_SEC", 0.0))
ENTRY_AI_MAX_PARALLEL = max(1, _env_int("ENTRY_AI_MAX_PARALLEL", 3))

ENTRY_AI_WEIGHT = _clip01(_env_float("ENTRY_AI_WEIGHT", 0.70))
ENTRY_MIN_AI_SCORE = _clip01(_env_float("ENTRY_MIN_AI_SCORE", 0.0))

ENTRY_AI_CALL_POLICY = (
    (os.getenv("ENTRY_AI_CALL_POLICY", "borderline") or "borderline").strip().lower()
)
ENTRY_AI_BORDERLINE_MARGIN = _clip(_env_float("ENTRY_AI_BORDERLINE_MARGIN", 0.08), 0.0, 0.5)
ENTRY_AI_SHADOW_MODE = _env_bool("ENTRY_AI_SHADOW_MODE", False)

ENTRY_MIN_SCORE = _clip01(_env_float("ENTRY_MIN_SCORE", 0.55))
ENTRY_DYNAMIC_MIN_ENABLED = _env_bool("ENTRY_DYNAMIC_MIN_ENABLED", True)
ENTRY_DYNAMIC_MIN_LO = _clip01(_env_float("ENTRY_DYNAMIC_MIN_LO", 0.48))
ENTRY_DYNAMIC_MIN_HI = _clip01(_env_float("ENTRY_DYNAMIC_MIN_HI", 0.72))
ENTRY_DYNAMIC_STEP = max(0.001, min(0.05, _env_float("ENTRY_DYNAMIC_STEP", 0.01)))
ENTRY_AI_MAINT_INTERVAL_SEC = max(30.0, _env_float("ENTRY_AI_MAINT_INTERVAL_SEC", 180.0))

ENTRY_MAX_FEATURE_BYTES = max(5_000, _env_int("ENTRY_MAX_FEATURE_BYTES", 60_000))
ENTRY_DEBUG_LOG = _env_bool("ENTRY_DEBUG_LOG", False)
ENTRY_DEBUG_LOG_FAIL = _env_bool("ENTRY_DEBUG_LOG_FAIL", True)
ENTRY_ATTACH_META = _env_bool("ENTRY_ATTACH_META", True)
ENTRY_LOG_DECISIONS = _env_bool("ENTRY_LOG_DECISIONS", True)

ENTRY_MAX_AGE_MIN = max(0.0, _env_float("ENTRY_MAX_AGE_MIN", 0.0))
ENTRY_MIN_LIQ_USD = max(0.0, _env_float("ENTRY_MIN_LIQ_USD", 10_000.0))
ENTRY_MAX_RUG_SCORE = _clip(_env_float("ENTRY_MAX_RUG_SCORE", 0.80), 0.0, 1.0)
ENTRY_MIN_5M_BUYS = max(0, _env_int("ENTRY_MIN_5M_BUYS", 0))
ENTRY_MIN_5M_VOL_USD = max(0.0, _env_float("ENTRY_MIN_5M_VOL_USD", 0.0))

ENTRY_REGIME_TIGHTEN_ENABLED = _env_bool("ENTRY_REGIME_TIGHTEN_ENABLED", True)
ENTRY_RISK_OFF_EXTRA_MARGIN = _clip(_env_float("ENTRY_RISK_OFF_EXTRA_MARGIN", 0.10), 0.0, 0.3)
ENTRY_MANIP_HIGH = _clip(_env_float("ENTRY_MANIP_HIGH", 0.75), 0.0, 1.0)

ENTRY_CTX_MAX_DRAWDOWN_PCT = abs(_env_float("ENTRY_CTX_MAX_DRAWDOWN_PCT", 12.0))
ENTRY_CTX_MAX_OPEN_POS_RATIO = _clip(_env_float("ENTRY_CTX_MAX_OPEN_POS_RATIO", 1.0), 0.1, 2.0)
ENTRY_CTX_LOCK_AFTER_TARGET_PNL = _env_float("ENTRY_CTX_LOCK_AFTER_TARGET_PNL", 0.0)

ENTRY_REQUIRE_RISK_CAPACITY = _env_bool("ENTRY_REQUIRE_RISK_CAPACITY", False)
ENTRY_MIN_RISK_CAPACITY = _clip01(_env_float("ENTRY_MIN_RISK_CAPACITY", 0.20))

ENTRY_USE_PF_SCORES = _env_bool("ENTRY_USE_PF_SCORES", True)
ENTRY_MIN_PF_FINAL = _clip01(_env_float("ENTRY_MIN_PF_FINAL", 0.10))
ENTRY_MIN_PF_RISK_ADJ = _clip01(_env_float("ENTRY_MIN_PF_RISK_ADJ", 0.08))
ENTRY_PF_HARD_FAIL = _env_bool("ENTRY_PF_HARD_FAIL", False)

ENTRY_USE_BUY_SCORE = _env_bool("ENTRY_USE_BUY_SCORE", True)
ENTRY_MIN_BUY_SCORE = _clip01(_env_float("ENTRY_MIN_BUY_SCORE", 0.0))
ENTRY_BUY_SCORE_WEIGHT = _clip01(_env_float("ENTRY_BUY_SCORE_WEIGHT", 0.20))
ENTRY_BUY_HARD_FAIL = _env_bool("ENTRY_BUY_HARD_FAIL", False)

ENTRY_USE_HINT_SCORE = _env_bool("ENTRY_USE_HINT_SCORE", True)
ENTRY_MIN_HINT_SCORE = _clip01(_env_float("ENTRY_MIN_HINT_SCORE", 0.0))
ENTRY_HINT_SCORE_WEIGHT = _clip01(_env_float("ENTRY_HINT_SCORE_WEIGHT", 0.10))
ENTRY_HINT_HARD_FAIL = _env_bool("ENTRY_HINT_HARD_FAIL", False)

ENTRY_SMART_WALLET_BOOST = _env_bool("ENTRY_SMART_WALLET_BOOST", True)
ENTRY_MIN_ENTRY_MICRO = _clip(_env_float("ENTRY_MIN_ENTRY_MICRO", 0.0), 0.0, 1.5)
ENTRY_MIN_STRUCTURAL_QUALITY = _clip(_env_float("ENTRY_MIN_STRUCTURAL_QUALITY", 0.0), 0.0, 1.5)
ENTRY_FORBID_DEADLIKE = _env_bool("ENTRY_FORBID_DEADLIKE", False)

ENTRY_PUMP_CHASE_MAX_PENALTY = _clip(_env_float("ENTRY_PUMP_CHASE_MAX_PENALTY", 0.35), 0.0, 0.7)

ENTRY_USE_ENTRY_QUALITY = _env_bool("ENTRY_USE_ENTRY_QUALITY", True)
ENTRY_MIN_ENTRY_QUALITY = _clip(_env_float("ENTRY_MIN_ENTRY_QUALITY", 0.0), 0.0, 1.5)

ENTRY_REQUIRE_POS_SIZE = _env_bool("ENTRY_REQUIRE_POS_SIZE", False)
ENTRY_MIN_POS_SIZE = _clip(_env_float("ENTRY_MIN_POS_SIZE", 0.0), 0.0, 1.0)

ENTRY_USE_EDGE_SCORE = _env_bool("ENTRY_USE_EDGE_SCORE", True)
ENTRY_MIN_EDGE_SCORE = _clip01(_env_float("ENTRY_MIN_EDGE_SCORE", 0.0))

ENTRY_USE_KELLY = _env_bool("ENTRY_USE_KELLY", False)
ENTRY_MIN_KELLY = _clip01(_env_float("ENTRY_MIN_KELLY", 0.0))
ENTRY_MAX_KELLY = _clip01(_env_float("ENTRY_MAX_KELLY", 0.25))

ENTRY_USE_PLAN_GAP_TILT = _env_bool("ENTRY_USE_PLAN_GAP_TILT", True)
ENTRY_PLAN_GAP_TILT = _clip(_env_float("ENTRY_PLAN_GAP_TILT", 0.03), 0.0, 0.2)
ENTRY_PLAN_GAP_MAX_ABS = max(1.0, _env_float("ENTRY_PLAN_GAP_MAX_ABS", 25.0))

ENTRY_PLAN_STATUS_BLOCK_ENABLED = _env_bool("ENTRY_PLAN_STATUS_BLOCK_ENABLED", True)
ENTRY_PLAN_STATUS_BLOCK_KEYWORDS = (
    os.getenv("ENTRY_PLAN_STATUS_BLOCK_KEYWORDS", "halt,no_trade").strip().lower()
)

ENTRY_SOURCE_SCORE_OVERRIDES = _env_json_dict("ENTRY_SOURCE_SCORE_OVERRIDES_JSON") or {}
ENTRY_SOURCE_SCORE_BONUS = _env_json_dict("ENTRY_SOURCE_SCORE_BONUS_JSON") or {}
ENTRY_PLAYBOOK_SCORE_OVERRIDES = _env_json_dict("ENTRY_PLAYBOOK_SCORE_OVERRIDES_JSON") or {}
ENTRY_PLAYBOOK_SCORE_BONUS = _env_json_dict("ENTRY_PLAYBOOK_SCORE_BONUS_JSON") or {}

RISK_PROFILE = (
    os.getenv("ENTRY_RISK_PROFILE", "") or os.getenv("RISK_PROFILE", "")
).strip().lower() or "balanced"
ENTRY_REGIME_RISK_MODE_LOCK = _env_int("ENTRY_REGIME_RISK_MODE_LOCK", -1)

_ENTRY_DYNAMIC_MIN_SCORE: float | None = None
_DYNAMIC_LOCK = asyncio.Lock()

_AI_LAST_CALL: dict[str, float] = {}
_AI_LAST_PRUNE_TS: float = 0.0

_AI_SEMA = asyncio.Semaphore(ENTRY_AI_MAX_PARALLEL)


# -----------------------------------------------------------------------------
# Decision object (structured; used by other modules)
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class EntryDecision:
    ok: bool
    score: float
    threshold: float
    reason: str
    explain: str
    meta: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        m = self.meta if isinstance(self.meta, dict) else {}
        return {
            "ok": bool(self.ok),
            "enter": bool(self.ok),  # backward-friendly
            "should_enter": bool(self.ok),  # backward-friendly
            "score": float(self.score),
            "threshold": float(self.threshold),
            "reason": _reason(self.reason),
            "explain": _reason(self.explain),
            "meta": m,
        }


# -----------------------------------------------------------------------------
# OpenAI client shim (SDK + legacy; lazy)
# -----------------------------------------------------------------------------
_CLIENT: Any = None
_CLIENT_KIND: str = "none"  # "sdk" | "legacy" | "none"


def _ensure_client() -> Any | None:
    global _CLIENT, _CLIENT_KIND
    if _CLIENT is not None:
        return _CLIENT

    # Never attempt in contract mode
    if _contract_no_io():
        _CLIENT = None
        _CLIENT_KIND = "none"
        return None

    try:
        from openai import OpenAI  # type: ignore

        _CLIENT = OpenAI()
        _CLIENT_KIND = "sdk"
        _log_event("ENTRY_OPENAI_CLIENT", kind="sdk")
        return _CLIENT
    except Exception:
        pass

    try:
        import openai  # type: ignore

        if not getattr(openai, "api_key", None):
            openai.api_key = os.getenv("OPENAI_API_KEY", "")
        _CLIENT = openai
        _CLIENT_KIND = "legacy"
        _log_event("ENTRY_OPENAI_CLIENT", kind="legacy")
        return _CLIENT
    except Exception:
        _CLIENT = None
        _CLIENT_KIND = "none"
        return None


def _pick_entry_model(features: dict[str, Any]) -> str | None:
    explicit = str(features.get("entry_model_name") or "").strip()
    if explicit:
        return explicit

    if ENTRY_MODEL_ROUTING:
        head = (
            str(features.get("entry_head") or "").strip()
            or str(features.get("segment") or "").strip()
            or str(features.get("regime_label") or "").strip()
            or ENTRY_DEFAULT_HEAD
        )
        if head in ENTRY_MODEL_ROUTING:
            return str(ENTRY_MODEL_ROUTING[head])
        if "default" in ENTRY_MODEL_ROUTING:
            return str(ENTRY_MODEL_ROUTING["default"])

    return ENTRY_MODEL or None


def _extract_sdk_content(resp: Any) -> str | None:
    try:
        ch0 = resp.choices[0]
        msg = getattr(ch0, "message", None)
        if msg is not None:
            c = getattr(msg, "content", None)
            if c:
                return str(c)
        if isinstance(msg, dict) and msg.get("content"):
            return str(msg["content"])
    except Exception:
        pass
    return None


# -----------------------------------------------------------------------------
# Keys / profiles / thresholds
# -----------------------------------------------------------------------------
def _source_key(features: dict[str, Any]) -> str:
    src = (
        features.get("scanner_source")
        or features.get("source")
        or features.get("src")
        or features.get("segment")
        or ""
    )
    if isinstance(src, (int, float)):
        return f"id_{int(src)}"
    return str(src).strip().lower()


def _playbook_key(features: dict[str, Any]) -> str:
    pb = (
        features.get("entry_playbook")
        or features.get("playbook_id")
        or features.get("playbook_name")
        or features.get("strategy_playbook")
        or ""
    )
    return str(pb).strip().lower()


def _effective_risk_profile(features: dict[str, Any]) -> str:
    rp = str(features.get("risk_profile") or "").strip().lower()
    if rp in ("conservative", "balanced", "aggressive"):
        return rp
    return (
        RISK_PROFILE if RISK_PROFILE in ("conservative", "balanced", "aggressive") else "balanced"
    )


def _risk_profile_threshold_adjust(base: float, features: dict[str, Any]) -> float:
    rp = _effective_risk_profile(features)
    if rp == "conservative":
        return _clip01(base + 0.03)
    if rp == "aggressive":
        return _clip01(base - 0.03)
    return _clip01(base)


def _effective_regime_mode(features: dict[str, Any], meta: dict[str, Any] | None = None) -> int:
    """
    Return -1/0/1/2 regime lock mode.
    """
    try:
        v = features.get("regime_risk_mode_lock")
        if v is not None:
            m = int(v)
            if m in (-1, 0, 1, 2):
                return m
    except Exception:
        pass

    if meta is not None:
        try:
            v2 = _ctx_get(features, meta, "regime_risk_mode_lock", None)
            if v2 is not None:
                m2 = int(v2)
                if m2 in (-1, 0, 1, 2):
                    return m2
        except Exception:
            pass

    if ENTRY_REGIME_RISK_MODE_LOCK in (0, 1, 2):
        return ENTRY_REGIME_RISK_MODE_LOCK

    try:
        rm = float(_finite(features.get("ctx_risk_mode", features.get("risk_mode_raw", 1.0)), 1.0))
        if rm <= 0.0:
            return 0
        if rm >= 2.0:
            return 2
    except Exception:
        pass

    return -1


def _global_min_threshold() -> float:
    if not ENTRY_DYNAMIC_MIN_ENABLED:
        return ENTRY_MIN_SCORE
    if _ENTRY_DYNAMIC_MIN_SCORE is None:
        return ENTRY_MIN_SCORE
    return _clip01(_ENTRY_DYNAMIC_MIN_SCORE)


def _source_min_threshold(features: dict[str, Any]) -> float:
    base = _global_min_threshold()
    key = _source_key(features)
    if not key:
        return base
    try:
        if key in ENTRY_SOURCE_SCORE_OVERRIDES:
            return max(base, _clip01(float(ENTRY_SOURCE_SCORE_OVERRIDES[key])))
    except Exception:
        pass
    return base


def _source_score_bonus(features: dict[str, Any]) -> float:
    key = _source_key(features)
    if not key:
        return 0.0
    try:
        v = ENTRY_SOURCE_SCORE_BONUS.get(key)
        return _finite(v, 0.0) if v is not None else 0.0
    except Exception:
        return 0.0


def _playbook_min_threshold(base: float, features: dict[str, Any]) -> float:
    key = _playbook_key(features)
    if not key:
        return base
    try:
        v = ENTRY_PLAYBOOK_SCORE_OVERRIDES.get(key)
        if v is None:
            return base
        return max(base, _clip01(float(v)))
    except Exception:
        return base


def _playbook_score_bonus(features: dict[str, Any]) -> float:
    key = _playbook_key(features)
    if not key:
        return 0.0
    try:
        v = ENTRY_PLAYBOOK_SCORE_BONUS.get(key)
        return _finite(v, 0.0) if v is not None else 0.0
    except Exception:
        return 0.0


# -----------------------------------------------------------------------------
# Deterministic heuristic scoring (baseline)
# -----------------------------------------------------------------------------
def _norm_fraud_01(features: dict[str, Any]) -> float:
    raw = _f(
        features,
        "ai_fraud_score",
        _f(features, "ai_rug_score_01", _f(features, "ai_rug_score", 0.0)),
    )
    if raw > 1.5:
        return _clip(raw / 100.0, 0.0, 1.0)
    return _clip(raw, 0.0, 1.0)


def _heuristic_score(features: dict[str, Any]) -> float:
    """
    Deterministic baseline score in [0,1].
    Only mutation allowed: entry_meta.breakdown when ENTRY_ATTACH_META=1.
    """
    meta = _ensure_meta(features) if ENTRY_ATTACH_META else None
    breakdown: dict[str, float] = {}

    pf_mom = _f(features, "pf_momentum_score", 0.0)
    pf_final = _f(features, "pf_final_score", 0.0)
    pf_risk_adj = _f(features, "pf_risk_adjusted_final", pf_final)

    pf_component = 0.0
    if ENTRY_USE_PF_SCORES:
        pf_component = 0.55 * pf_risk_adj + 0.25 * pf_final + 0.15 * pf_mom

    buy_component = 0.0
    if ENTRY_USE_BUY_SCORE:
        buy_score = _f(features, "ai_buy_score", 0.0)
        buy_ok_hard = bool(
            features.get("ai_buy_ok_hard")
            or features.get("ai_buy_pass_gate")
            or features.get("ai_buy_pass_gate_hard")
        )
        if buy_score > 0.0 and (ENTRY_MIN_BUY_SCORE <= 0.0 or buy_score >= ENTRY_MIN_BUY_SCORE):
            buy_component = ENTRY_BUY_SCORE_WEIGHT * _clip01(buy_score)
        elif ENTRY_BUY_HARD_FAIL and buy_score > 0.0 and not buy_ok_hard:
            buy_component -= 1.2

    hint_component = 0.0
    if ENTRY_USE_HINT_SCORE:
        hint_score = _f(features, "ai_entry_hint_score", _f(features, "entry_hint_score", 0.0))
        if hint_score > 0.0 and (ENTRY_MIN_HINT_SCORE <= 0.0 or hint_score >= ENTRY_MIN_HINT_SCORE):
            hint_component = ENTRY_HINT_SCORE_WEIGHT * _clip01(hint_score)

    chg_1m = _clip(_f(features, "price_change_1m_pct", 0.0), -60.0, 80.0)
    chg_5m = _clip(_f(features, "price_change_5m_pct", _f(features, "chg5", chg_1m)), -60.0, 120.0)
    chg_15m = _clip(_f(features, "price_change_15m_pct", 0.0), -80.0, 180.0)
    chg_1h = _clip(_f(features, "price_change_1h_pct", 0.0), -90.0, 250.0)

    mom_score = 0.0
    mom_score += max(0.0, chg_1m) * 0.006
    mom_score += max(0.0, chg_5m) * 0.004
    mom_score += max(0.0, chg_15m) * 0.003
    mom_score += max(0.0, chg_1h) * 0.001

    pump_chase = _clip(_f(features, "is_pump_chasing_risk", 0.0), 0.0, 1.0)
    if pump_chase > 0.0:
        mom_score -= ENTRY_PUMP_CHASE_MAX_PENALTY * pump_chase

    vol_score = 0.0
    vol15 = _clip(_f(features, "vol_15s", 0.0) * 100.0, 0.0, 10_000.0)
    vol60 = _clip(_f(features, "vol_60s", 0.0) * 100.0, 0.0, 10_000.0)
    vol300 = _clip(_f(features, "vol_300s", 0.0) * 100.0, 0.0, 10_000.0)
    for v in (vol15, vol60, vol300):
        if v <= 0.2:
            vol_score -= 0.12
        elif 0.3 <= v <= 3.0:
            vol_score += 0.12
        elif 3.0 < v <= 8.0:
            vol_score += 0.04
        elif v > 12.0:
            vol_score -= 0.18

    buys_5m = _i(features, "buys_5m", 0)
    sells_5m = _i(features, "sells_5m", 0)
    trades_5m = _f(features, "txns_5m", float(buys_5m + sells_5m))
    imbalance_5m = _clip(_f(features, "orderflow_skew_5m", 0.0), -1.0, 1.0)
    buy_ratio_5m = _clip(_f(features, "buy_ratio_5m", 0.5), 0.0, 1.0)
    vol_5m_usd = _clip(_f(features, "vol_5m_usd", 0.0), 0.0, 1e12)

    flow_score = 0.0
    flow_score += min(buys_5m, 60) * 0.007
    flow_score += min(trades_5m, 120.0) * 0.003
    flow_score += max(0.0, imbalance_5m) * 0.25
    flow_score += (buy_ratio_5m - 0.5) * 0.6
    flow_score += min(vol_5m_usd / 5_000.0, 5.0) * 0.015

    liq = _clip(_f(features, "liq_usd", 0.0), 0.0, 1e12)
    mcap = _clip(_f(features, "mcap_usd", 0.0), 0.0, 1e15)
    log_liq = _log1p_pos(liq)
    log_mcap = _log1p_pos(mcap)

    liq_score = 0.0
    has_buckets = any(
        k in features
        for k in (
            "liq_bucket_ultra_low",
            "liq_bucket_low",
            "liq_bucket_sweet",
            "liq_bucket_mid",
            "liq_bucket_high",
        )
    )
    if has_buckets:
        liq_score += _f(features, "liq_bucket_ultra_low", 0.0) * -0.40
        liq_score += _f(features, "liq_bucket_low", 0.0) * -0.06
        liq_score += _f(features, "liq_bucket_sweet", 0.0) * 0.22
        liq_score += _f(features, "liq_bucket_mid", 0.0) * 0.16
        liq_score += _f(features, "liq_bucket_high", 0.0) * 0.08
    else:
        if liq < 8_000:
            liq_score -= 0.40
        elif liq < 20_000:
            liq_score -= 0.05
        elif liq <= 60_000:
            liq_score += 0.12
        elif liq <= 400_000:
            liq_score += 0.18
        else:
            liq_score += 0.10

    liq_score += (log_liq / 20.0) * 0.05
    liq_score += (log_mcap / 20.0) * 0.03

    age_min = _clip(_f(features, "age_min", _f(features, "token_age_min", 0.0)), 0.0, 1e9)
    age_score = 0.0
    if age_min <= 30.0:
        age_score += 0.20
    elif age_min <= 180.0:
        age_score += 0.10
    elif age_min > 240.0:
        age_score -= 0.18
    if age_min > 24 * 60:
        age_score -= 0.35

    smart_wallet_score = _clip(_f(features, "smart_wallet_score", 0.0), 0.0, 10.0)
    structural_quality = _clip(_f(features, "structural_quality_score", 0.0), -2.0, 2.0)
    entry_micro = _clip(_f(features, "entry_micro_alpha", 0.0), -2.0, 2.0)
    is_dead_like = _clip(_f(features, "is_dead_like", 0.0), 0.0, 1.0)

    wallet_score = 0.0
    if ENTRY_SMART_WALLET_BOOST:
        wallet_score += smart_wallet_score * 0.70

    structure_score = 0.0
    structure_score += structural_quality * 0.30
    structure_score += entry_micro * 0.60
    structure_score -= is_dead_like * 0.60

    regime_trend = _clip(_f(features, "regime_trend", _f(features, "regime_score", 0.0)), -2.0, 2.0)
    regime_liq = _clip(_f(features, "regime_liquidity", 0.5), 0.0, 1.0)
    regime_manip = _clip(_f(features, "regime_manipulation_risk", 0.0), 0.0, 1.0)

    regime_component = (
        0.25 * regime_trend + 0.25 * (regime_liq - 0.5) - 0.60 * max(0.0, regime_manip - 0.5)
    )

    ai_fraud = _norm_fraud_01(features)
    impact_1k_bps = _clip(_f(features, "impact_1k_bps", 0.0), 0.0, 100_000.0)
    impact_penalty = -0.25 if impact_1k_bps > 400 else (-0.12 if impact_1k_bps > 200 else 0.0)
    rug_penalty = -ai_fraud * 0.70

    raw = (
        pf_component
        + buy_component
        + hint_component
        + mom_score
        + vol_score
        + flow_score
        + liq_score
        + age_score
        + wallet_score
        + structure_score
        + regime_component
        + impact_penalty
        + rug_penalty
    )
    if ai_fraud >= 0.95:
        raw -= 4.0

    score = _clip01(_sigmoid(raw))

    if ENTRY_ATTACH_META and meta is not None:
        breakdown.update(
            {
                "pf": float(pf_component),
                "buy": float(buy_component),
                "hint": float(hint_component),
                "mom": float(mom_score),
                "vol": float(vol_score),
                "flow": float(flow_score),
                "liq": float(liq_score),
                "age": float(age_score),
                "wallet": float(wallet_score),
                "structure": float(structure_score),
                "regime": float(regime_component),
                "impact_penalty": float(impact_penalty),
                "rug_penalty": float(rug_penalty),
            }
        )
        meta["breakdown"] = breakdown

    if ENTRY_DEBUG_LOG and _log_every("ENTRY_HEURISTIC", 200):
        sym = features.get("symbol") or features.get("mint") or "?"
        with contextlib.suppress(Exception):
            log.debug("ENTRY_HEURISTIC sym=%s score=%.3f", sym, score)

    return score


# -----------------------------------------------------------------------------
# RiskManager enrichment helper (cycle-safe) — meta-only
# -----------------------------------------------------------------------------
async def _maybe_enrich_risk_context(features: dict[str, Any]) -> None:
    """
    Enrich entry_meta.ctx with risk context. Never mutates top-level keys.
    Contract/NO_NETWORK: does nothing (but may set a meta flag).
    """
    meta = _ensure_meta(features)
    ctx = _meta_ctx(meta)

    if _contract_no_io():
        _append_flag(meta, "contract_no_io")
        return

    wants = (
        "risk_capacity_score",
        "ctx_open_positions",
        "ctx_max_positions",
        "ctx_daily_drawdown_pct",
        "ctx_daily_realized_pnl_pct",
        "ctx_risk_mode",
        "risk_profile",
        "ctx_hard_halt",
        "ctx_plan_gap_pct",
        "ctx_plan_status",
        "ctx_entry_score_gate",
        "risk_win_rate_pct",
        "risk_loss_streak",
    )
    if all(ctx.get(k) is not None for k in wants):
        return

    try:
        from utils_pkg import risk_manager as _rm  # local import

        snap: Any = await _rm.risk_capacity_snapshot()
        if not isinstance(snap, dict):
            _append_flag(meta, "risk_snapshot_not_dict")
            return

        ctx.setdefault("risk_capacity_score", float(_finite(snap.get("risk_capacity", 1.0), 1.0)))
        ctx.setdefault("ctx_open_positions", float(_finite(snap.get("open_positions", 0.0), 0.0)))
        ctx.setdefault("ctx_max_positions", float(_finite(snap.get("max_positions", 0.0), 0.0)))
        ctx.setdefault(
            "ctx_daily_drawdown_pct",
            float(_finite(snap.get("daily_drawdown_pct", snap.get("max_dd_pct", 0.0)), 0.0)),
        )
        ctx.setdefault(
            "ctx_daily_realized_pnl_pct",
            float(_finite(snap.get("daily_realized_pnl_pct", 0.0), 0.0)),
        )
        ctx.setdefault("ctx_risk_mode", float(_finite(snap.get("risk_mode", 1.0), 1.0)))

        ctx.setdefault(
            "risk_win_rate_pct", float(_finite(snap.get("rolling_win_rate_pct", 0.0), 0.0))
        )
        ctx.setdefault("risk_loss_streak", float(_finite(snap.get("recent_loss_streak", 0.0), 0.0)))

        ctx.setdefault(
            "risk_profile", str(snap.get("risk_profile") or ctx.get("risk_profile") or "balanced")
        )
        ctx.setdefault("ctx_hard_halt", bool(snap.get("hard_halt", snap.get("HARD_HALT", False))))

        ctx.setdefault("ctx_plan_gap_pct", float(_finite(snap.get("plan_gap_pct", 0.0), 0.0)))
        ctx.setdefault("ctx_plan_status", str(snap.get("plan_status", "n/a")))

        if "entry_score_gate" in snap:
            ctx.setdefault(
                "ctx_entry_score_gate", float(_finite(snap.get("entry_score_gate", 0.0), 0.0))
            )

    except Exception as e:
        if ENTRY_DEBUG_LOG:
            _log_event("ENTRY_RISK_CTX_FAIL", err=str(e)[:200])


# -----------------------------------------------------------------------------
# OpenAI overlay (fail-soft)
# -----------------------------------------------------------------------------
def _prune_ai_last_calls(max_age_sec: float = 900.0, max_items: int = 5000) -> None:
    global _AI_LAST_PRUNE_TS
    now = _now()
    if now - _AI_LAST_PRUNE_TS < 60.0:
        return
    _AI_LAST_PRUNE_TS = now
    try:
        if len(_AI_LAST_CALL) > max_items:
            items = sorted(_AI_LAST_CALL.items(), key=lambda kv: kv[1])
            for k, _t in items[: max(1, len(items) // 4)]:
                _AI_LAST_CALL.pop(k, None)
        cutoff = now - max_age_sec
        for k, t in list(_AI_LAST_CALL.items()):
            if t < cutoff:
                _AI_LAST_CALL.pop(k, None)
    except Exception:
        return


def _safe_dumps(obj: Any, max_bytes: int | None = None) -> str:
    try:
        s = json.dumps(obj, separators=(",", ":"), ensure_ascii=False, default=str)
    except Exception:
        try:
            s = json.dumps(str(obj), separators=(",", ":"), ensure_ascii=False)
        except Exception:
            s = "{}"
    if max_bytes is None:
        return s
    b = s.encode("utf-8", errors="ignore")
    if len(b) <= max_bytes:
        return s
    return b[: max(0, max_bytes - 3)].decode("utf-8", errors="ignore") + "..."


def _build_gate_prompt(features_for_ai: dict[str, Any]) -> tuple[str, str]:
    ts = features_for_ai.get("ts") or features_for_ai.get("timestamp") or ""
    src = (
        features_for_ai.get("src")
        or features_for_ai.get("source")
        or features_for_ai.get("scanner_source")
        or ""
    )
    regime = (
        features_for_ai.get("regime")
        or features_for_ai.get("regime_label")
        or features_for_ai.get("scanner_regime")
        or ""
    )
    symbol = features_for_ai.get("symbol") or ""
    mint = features_for_ai.get("mint") or ""
    wallet_tag = features_for_ai.get("wallet_tag") or ""
    fver = features_for_ai.get("feature_version") or features_for_ai.get("features_version") or ""

    system = (
        "You are an AI assistant for a Solana trading system.\n"
        "You will receive an ENTRY-TIME snapshot of a token candidate, including features.\n"
        "Decide whether this setup should be allowed through the entry gate.\n\n"
        'Respond with exactly ONE JSON object: {"verdict":"BUY|PASS","score":0.0-1.0}\n'
        "No extra keys. No explanation.\n\n"
        "Task profile: entry_gate_v3"
    )

    user = (
        "ENTRY snapshot:\n"
        f"ts: {ts}\n"
        f"src: {src}\n"
        f"regime: {regime}\n"
        f"symbol: {symbol}\n"
        f"mint: {mint}\n"
        f"wallet_tag: {wallet_tag}\n"
        f"feature_version: {fver}\n"
        f"features_json: {_safe_dumps(features_for_ai, max_bytes=ENTRY_MAX_FEATURE_BYTES)}\n"
        'Respond ONLY with {"verdict":"BUY|PASS","score":0.0-1.0}.'
    )
    return system, user


def _parse_ai_gate(content: str) -> tuple[str | None, float | None]:
    if not content:
        return None, None

    text = content.strip()
    m = _JSON_EXTRACT_RE.search(text)
    if m:
        text = m.group(0).strip()

    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            verdict = obj.get("verdict")
            score = obj.get("score")

            verdict_s: str | None = None
            if verdict is not None:
                vv = str(verdict).strip().upper()
                if vv in ("BUY", "PASS"):
                    verdict_s = vv

            score_f: float | None = None
            if score is not None:
                score_f = _clip01(score)

            if verdict_s is None and score_f is not None:
                return None, score_f
            return verdict_s, score_f
    except Exception:
        pass

    try:
        return None, _clip01(float(text))
    except Exception:
        return None, None


def _should_call_ai(heuristic: float, threshold: float) -> bool:
    if not ENTRY_AI_ENABLED:
        return False
    if _contract_no_io():
        return False

    pol = ENTRY_AI_CALL_POLICY
    if pol == "never":
        return False
    if pol == "always":
        return True
    if pol == "pass_only":
        return heuristic >= threshold
    # default borderline policy
    return heuristic >= max(0.0, threshold - ENTRY_AI_BORDERLINE_MARGIN)


def _shrink_for_ai(features: dict[str, Any]) -> dict[str, Any]:
    try:
        blob = _safe_dumps(features).encode("utf-8", errors="ignore")
        if len(blob) <= ENTRY_MAX_FEATURE_BYTES:
            out = dict(features)
            out.pop("entry_meta", None)
            out.pop("raw", None)
            out.pop("logger", None)
            out.pop("client", None)
            out.pop("session", None)
            return out

        keep = {
            "ts",
            "src",
            "source",
            "scanner_source",
            "regime",
            "regime_label",
            "symbol",
            "mint",
            "wallet_tag",
            "feature_version",
            "pf_final_score",
            "pf_risk_adjusted_final",
            "pf_momentum_score",
            "entry_quality_score",
            "entry_micro_alpha",
            "structural_quality_score",
            "liq_usd",
            "mcap_usd",
            "age_min",
            "token_age_min",
            "vol_5m_usd",
            "buys_5m",
            "sells_5m",
            "buy_ratio_5m",
            "orderflow_skew_5m",
            "smart_wallet_score",
            "unique_buyers_5m",
            "ai_fraud_score",
            "ai_rug_score_01",
            "regime_trend",
            "regime_liquidity",
            "regime_manipulation_risk",
            "risk_capacity_score",
            "ctx_open_positions",
            "ctx_max_positions",
            "ctx_daily_drawdown_pct",
            "ctx_daily_realized_pnl_pct",
            "ctx_risk_mode",
            "risk_win_rate_pct",
            "risk_loss_streak",
            "ctx_plan_gap_pct",
            "ctx_plan_status",
            "ctx_entry_score_gate",
        }
        return {k: features.get(k) for k in keep if k in features}
    except Exception:
        out = dict(features)
        out.pop("entry_meta", None)
        return out


async def _ai_gate(
    features: dict[str, Any], heuristic: float, threshold: float
) -> tuple[str | None, float | None, str | None]:
    if _contract_no_io():
        return None, None, "contract_no_io"

    if not os.getenv("OPENAI_API_KEY"):
        return None, None, "no_api_key"

    model = _pick_entry_model(features)
    if not model:
        return None, None, "no_model"

    if not _should_call_ai(heuristic, threshold):
        return None, None, "policy_skip"

    if ENTRY_AI_COOLDOWN_SEC > 0:
        key = str(features.get("mint") or features.get("symbol") or "").strip()
        if key:
            _prune_ai_last_calls()
            now = _now()
            last = _AI_LAST_CALL.get(key, 0.0)
            if now - last < ENTRY_AI_COOLDOWN_SEC:
                return None, None, "cooldown_skip"
            _AI_LAST_CALL[key] = now

    client = _ensure_client()
    if client is None:
        if ENTRY_AI_STRICT:
            return "PASS", 0.0, "strict_fail_closed_client"
        return None, None, "no_client"

    features_for_ai = _shrink_for_ai(features)
    system, user = _build_gate_prompt(features_for_ai)

    async with _AI_SEMA:
        try:
            if _CLIENT_KIND == "sdk":

                def _do_req_sdk() -> Any:
                    try:
                        return client.chat.completions.create(
                            model=model,
                            messages=[
                                {"role": "system", "content": system},
                                {"role": "user", "content": user},
                            ],
                            temperature=ENTRY_AI_TEMPERATURE,
                            max_tokens=ENTRY_AI_MAX_TOKENS,
                            response_format={"type": "json_object"},
                        )
                    except TypeError:
                        return client.chat.completions.create(
                            model=model,
                            messages=[
                                {"role": "system", "content": system},
                                {"role": "user", "content": user},
                            ],
                            temperature=ENTRY_AI_TEMPERATURE,
                            max_tokens=ENTRY_AI_MAX_TOKENS,
                        )

                resp = await asyncio.wait_for(
                    asyncio.to_thread(_do_req_sdk), timeout=ENTRY_AI_TIMEOUT_SEC
                )
                content_str = _extract_sdk_content(resp) or ""
            else:

                def _do_req_legacy() -> Any:
                    return client.ChatCompletion.create(
                        model=model,
                        messages=[
                            {"role": "system", "content": system},
                            {"role": "user", "content": user},
                        ],
                        temperature=ENTRY_AI_TEMPERATURE,
                        max_tokens=ENTRY_AI_MAX_TOKENS,
                    )

                resp = await asyncio.wait_for(
                    asyncio.to_thread(_do_req_legacy), timeout=ENTRY_AI_TIMEOUT_SEC
                )
                content_str = str(resp["choices"][0]["message"]["content"])  # type: ignore[index]

            verdict, score = _parse_ai_gate(content_str)
            if verdict in ("BUY", "PASS") and score is None:
                score = 0.90 if verdict == "BUY" else 0.10

            if score is None:
                if ENTRY_DEBUG_LOG_FAIL:
                    _log_event("ENTRY_AI_PARSE_FAIL", content=content_str[:220])
                if ENTRY_AI_STRICT:
                    return "PASS", 0.0, "strict_fail_closed_parse"
                return None, None, "parse_fail"

            if ENTRY_MIN_AI_SCORE > 0 and score < ENTRY_MIN_AI_SCORE:
                return verdict, None, "below_min_ai_score"

            return verdict, float(score), None

        except Exception as e:
            if ENTRY_DEBUG_LOG_FAIL:
                _log_event("ENTRY_AI_CALL_FAIL", err=str(e)[:220], strict=int(ENTRY_AI_STRICT))
            if ENTRY_AI_STRICT:
                return "PASS", 0.0, "strict_fail_closed_exception"
            return None, None, "call_fail"


# -----------------------------------------------------------------------------
# Public scoring API
# -----------------------------------------------------------------------------
async def score_entry(features: dict[str, Any], ctx: dict | None = None) -> float:
    _merge_ctx_best_effort(features, ctx)
    await _maybe_enrich_risk_context(features)

    meta = _ensure_meta(features)
    heuristic = _heuristic_score(features)

    base_threshold = _source_min_threshold(features)
    base_threshold = _playbook_min_threshold(base_threshold, features)
    base_threshold = _risk_profile_threshold_adjust(base_threshold, features)

    ai_verdict: str | None = None
    ai_score: float | None = None
    ai_err: str | None = None

    if ENTRY_AI_ENABLED:
        ai_verdict, ai_score, ai_err = await _ai_gate(
            features, heuristic=heuristic, threshold=base_threshold
        )

    used_ai = ai_score is not None
    if ENTRY_AI_SHADOW_MODE:
        blended = heuristic
    else:
        if ai_score is None:
            blended = heuristic
        else:
            if ai_verdict == "PASS":
                ai_score = 0.0
            blended = ENTRY_AI_WEIGHT * float(ai_score) + (1.0 - ENTRY_AI_WEIGHT) * heuristic

    blended = _clip01(blended + _source_score_bonus(features) + _playbook_score_bonus(features))

    if ENTRY_ATTACH_META:
        meta.update(
            {
                "heuristic_score": float(heuristic),
                "ai_verdict": ai_verdict,
                "ai_score": float(ai_score) if ai_score is not None else None,
                "ai_error": ai_err,
                "used_ai": bool(used_ai),
                "ai_shadow_mode": bool(ENTRY_AI_SHADOW_MODE),
                "blended_score": float(blended),
                "source_key": _source_key(features),
                "playbook_key": _playbook_key(features),
                "risk_profile": _effective_risk_profile(features),
                "regime_risk_mode": _effective_regime_mode(features, meta),
                "ai_call_policy": ENTRY_AI_CALL_POLICY,
            }
        )

    return blended


async def _log_entry_decision_best_effort(
    features: dict[str, Any], ok: bool, score: float, reason: str
) -> None:
    if not ENTRY_LOG_DECISIONS:
        return
    if _contract_no_io():
        return
    try:
        from utils_pkg import data_logger as _dl  # type: ignore

        fn = getattr(_dl, "log_entry_decision", None)
        if not callable(fn):
            return

        meta = features.get("entry_meta") or {}
        payload = dict(
            candidate_id=features.get("candidate_id")
            or (meta.get("candidate_id") if isinstance(meta, dict) else None),
            mint=features.get("mint"),
            symbol=features.get("symbol"),
            src=features.get("src") or features.get("source") or features.get("scanner_source"),
            verdict="BUY" if ok else "PASS",
            score=float(score),
            reason=_reason(reason),
            meta=meta if isinstance(meta, dict) else {},
        )
        out = fn(**payload)
        if asyncio.iscoroutine(out):
            await out
    except Exception:
        return


# -----------------------------------------------------------------------------
# Core structured decision API (canonical for internal modules)
# -----------------------------------------------------------------------------
async def decide_entry_obj(features: dict[str, Any], ctx: dict | None = None) -> EntryDecision:
    _merge_ctx_best_effort(features, ctx)
    score = await score_entry(features, ctx=ctx)

    meta = _ensure_meta(features)
    ctxm = _meta_ctx(meta)

    liq = _f(features, "liq_usd", 0.0)
    age_min = _f(features, "age_min", _f(features, "token_age_min", 0.0))
    ai_fraud = _norm_fraud_01(features)

    buys_5m = _i(features, "buys_5m", 0)
    vol_5m_usd = _f(features, "vol_5m_usd", 0.0)

    ctx_open = _finite(
        _ctx_get(features, meta, "ctx_open_positions", ctxm.get("ctx_open_positions") or 0.0), 0.0
    )
    ctx_max = _finite(
        _ctx_get(features, meta, "ctx_max_positions", ctxm.get("ctx_max_positions") or 0.0), 0.0
    )
    ctx_dd = _finite(
        _ctx_get(
            features, meta, "ctx_daily_drawdown_pct", ctxm.get("ctx_daily_drawdown_pct") or 0.0
        ),
        0.0,
    )
    ctx_pnl = _finite(
        _ctx_get(
            features,
            meta,
            "ctx_daily_realized_pnl_pct",
            ctxm.get("ctx_daily_realized_pnl_pct") or 0.0,
        ),
        0.0,
    )
    ctx_risk_mode = _finite(
        _ctx_get(features, meta, "ctx_risk_mode", ctxm.get("ctx_risk_mode") or 1.0), 1.0
    )
    risk_capacity = _finite(
        _ctx_get(features, meta, "risk_capacity_score", ctxm.get("risk_capacity_score") or 1.0), 1.0
    )

    hard_halt = bool(_ctx_get(features, meta, "ctx_hard_halt", ctxm.get("ctx_hard_halt") or False))
    plan_gap_pct = _finite(
        _ctx_get(features, meta, "ctx_plan_gap_pct", ctxm.get("ctx_plan_gap_pct") or 0.0), 0.0
    )
    plan_status = str(
        _ctx_get(features, meta, "ctx_plan_status", ctxm.get("ctx_plan_status") or "n/a")
    )

    regime_trend = _finite(features.get("regime_trend", features.get("regime_score", 0.0)), 0.0)
    regime_manip = _finite(features.get("regime_manipulation_risk", 0.0), 0.0)
    regime_mode_lock = _effective_regime_mode(features, meta)

    # ---- HARD BLOCKS ----
    if hard_halt:
        reason = "risk_api_hard_halt"
        meta.update({"reason": reason, "pass": False, "hard_halt": True})
        _append_flag(meta, "hard_halt")
        await _log_entry_decision_best_effort(features, ok=False, score=score, reason=reason)
        return EntryDecision(
            False, float(score), 1.0, reason, reason, meta if isinstance(meta, dict) else None
        )

    if ENTRY_PLAN_STATUS_BLOCK_ENABLED and plan_status:
        ps_lc = plan_status.lower()
        for kw in ENTRY_PLAN_STATUS_BLOCK_KEYWORDS.split(","):
            kw = kw.strip()
            if kw and kw in ps_lc:
                reason = f"plan_status_block({ps_lc})"
                meta.update({"reason": reason, "pass": False, "plan_status": plan_status})
                _append_flag(meta, "plan_status_block")
                await _log_entry_decision_best_effort(
                    features, ok=False, score=score, reason=reason
                )
                return EntryDecision(
                    False,
                    float(score),
                    1.0,
                    reason,
                    reason,
                    meta if isinstance(meta, dict) else None,
                )

    if ENTRY_REQUIRE_RISK_CAPACITY and risk_capacity < ENTRY_MIN_RISK_CAPACITY:
        reason = f"risk_capacity_low({risk_capacity:.3f}<{ENTRY_MIN_RISK_CAPACITY:.3f})"
        meta.update({"reason": reason, "pass": False, "risk_capacity": float(risk_capacity)})
        _append_flag(meta, "risk_capacity_low")
        await _log_entry_decision_best_effort(features, ok=False, score=score, reason=reason)
        return EntryDecision(
            False, float(score), 1.0, reason, reason, meta if isinstance(meta, dict) else None
        )

    if risk_capacity <= 0.0:
        reason = "risk_capacity_zero"
        meta.update({"reason": reason, "pass": False, "risk_capacity": float(risk_capacity)})
        _append_flag(meta, "risk_capacity_zero")
        await _log_entry_decision_best_effort(features, ok=False, score=score, reason=reason)
        return EntryDecision(
            False, float(score), 1.0, reason, reason, meta if isinstance(meta, dict) else None
        )

    if liq < ENTRY_MIN_LIQ_USD:
        reason = f"low_liq({liq:.0f}<{ENTRY_MIN_LIQ_USD:.0f})"
        meta.update({"reason": reason, "pass": False})
        _append_flag(meta, "low_liq")
        await _log_entry_decision_best_effort(features, ok=False, score=score, reason=reason)
        return EntryDecision(
            False, float(score), 1.0, reason, reason, meta if isinstance(meta, dict) else None
        )

    if ENTRY_MAX_AGE_MIN > 0 and age_min > ENTRY_MAX_AGE_MIN:
        reason = f"too_old({age_min:.1f}>{ENTRY_MAX_AGE_MIN:.1f})"
        meta.update({"reason": reason, "pass": False})
        _append_flag(meta, "too_old")
        await _log_entry_decision_best_effort(features, ok=False, score=score, reason=reason)
        return EntryDecision(
            False, float(score), 1.0, reason, reason, meta if isinstance(meta, dict) else None
        )

    if ENTRY_FORBID_DEADLIKE:
        is_dead_like = _clip(_f(features, "is_dead_like", 0.0), 0.0, 1.0)
        if is_dead_like >= 0.60:
            reason = f"dead_like({is_dead_like:.2f})"
            meta.update({"reason": reason, "pass": False})
            _append_flag(meta, "dead_like")
            await _log_entry_decision_best_effort(features, ok=False, score=score, reason=reason)
            return EntryDecision(
                False, float(score), 1.0, reason, reason, meta if isinstance(meta, dict) else None
            )

    if ENTRY_CTX_MAX_DRAWDOWN_PCT > 0 and ctx_dd <= -ENTRY_CTX_MAX_DRAWDOWN_PCT * 0.5:
        reason = f"session_drawdown_limit({ctx_dd:.2f}<=-{ENTRY_CTX_MAX_DRAWDOWN_PCT:.1f})"
        meta.update({"reason": reason, "pass": False, "dd_lock": True})
        _append_flag(meta, "dd_lock")
        await _log_entry_decision_best_effort(features, ok=False, score=score, reason=reason)
        return EntryDecision(
            False, float(score), 1.0, reason, reason, meta if isinstance(meta, dict) else None
        )

    if (
        ENTRY_CTX_LOCK_AFTER_TARGET_PNL > 0
        and ctx_pnl >= ENTRY_CTX_LOCK_AFTER_TARGET_PNL
        and ctx_risk_mode <= 1.0
    ):
        reason = f"session_target_hit({ctx_pnl:.2f}>={ENTRY_CTX_LOCK_AFTER_TARGET_PNL:.2f})"
        meta.update({"reason": reason, "pass": False, "target_lock": True})
        _append_flag(meta, "target_lock")
        await _log_entry_decision_best_effort(features, ok=False, score=score, reason=reason)
        return EntryDecision(
            False, float(score), 1.0, reason, reason, meta if isinstance(meta, dict) else None
        )

    if ctx_max > 0 and ENTRY_CTX_MAX_OPEN_POS_RATIO > 0:
        ratio = ctx_open / ctx_max
        if ratio >= ENTRY_CTX_MAX_OPEN_POS_RATIO:
            reason = f"too_many_positions({ctx_open:.0f}/{ctx_max:.0f},ratio={ratio:.2f})"
            meta.update({"reason": reason, "pass": False, "pos_full": True})
            _append_flag(meta, "pos_full")
            await _log_entry_decision_best_effort(features, ok=False, score=score, reason=reason)
            return EntryDecision(
                False, float(score), 1.0, reason, reason, meta if isinstance(meta, dict) else None
            )

    if ai_fraud >= ENTRY_MAX_RUG_SCORE:
        reason = f"high_fraud_score({ai_fraud:.2f}>={ENTRY_MAX_RUG_SCORE:.2f})"
        meta.update({"reason": reason, "pass": False, "fraud_lock": True})
        _append_flag(meta, "fraud_lock")
        await _log_entry_decision_best_effort(features, ok=False, score=score, reason=reason)
        return EntryDecision(
            False, float(score), 1.0, reason, reason, meta if isinstance(meta, dict) else None
        )

    if ENTRY_MIN_5M_BUYS > 0 and buys_5m < ENTRY_MIN_5M_BUYS:
        reason = f"low_recent_flow(buys_5m={buys_5m}<{ENTRY_MIN_5M_BUYS})"
        meta.update({"reason": reason, "pass": False})
        _append_flag(meta, "low_recent_buys")
        await _log_entry_decision_best_effort(features, ok=False, score=score, reason=reason)
        return EntryDecision(
            False, float(score), 1.0, reason, reason, meta if isinstance(meta, dict) else None
        )

    if ENTRY_MIN_5M_VOL_USD > 0 and vol_5m_usd < ENTRY_MIN_5M_VOL_USD:
        reason = f"low_recent_volume(vol_5m={vol_5m_usd:.0f}<{ENTRY_MIN_5M_VOL_USD:.0f})"
        meta.update({"reason": reason, "pass": False})
        _append_flag(meta, "low_recent_volume")
        await _log_entry_decision_best_effort(features, ok=False, score=score, reason=reason)
        return EntryDecision(
            False, float(score), 1.0, reason, reason, meta if isinstance(meta, dict) else None
        )

    # ---- Thresholding (adaptive) ----
    base_threshold = _source_min_threshold(features)
    base_threshold = _playbook_min_threshold(base_threshold, features)
    base_threshold = _risk_profile_threshold_adjust(base_threshold, features)

    ctx_entry_gate = _finite(
        _ctx_get(features, meta, "ctx_entry_score_gate", ctxm.get("ctx_entry_score_gate") or 0.0),
        0.0,
    )
    if ctx_entry_gate > 0.0:
        base_threshold = max(base_threshold, _clip01(ctx_entry_gate))

    tightened = base_threshold
    risk_off_flag = False
    manip_flag = False

    if regime_mode_lock == 0:
        regime_trend = min(regime_trend, -0.8)
    elif regime_mode_lock == 2:
        regime_trend = max(regime_trend, 0.8)

    if ENTRY_REGIME_TIGHTEN_ENABLED:
        if regime_trend <= -0.7:
            tightened = max(tightened + ENTRY_RISK_OFF_EXTRA_MARGIN, 0.80)
            risk_off_flag = True
        if regime_manip >= ENTRY_MANIP_HIGH:
            tightened = max(tightened + 0.08, tightened)
            manip_flag = True

    prof_threshold = _risk_profile_threshold_adjust(tightened, features)

    plan_delta = 0.0
    if ENTRY_USE_PLAN_GAP_TILT and plan_gap_pct != 0.0:
        gap = _clip(plan_gap_pct, -ENTRY_PLAN_GAP_MAX_ABS, ENTRY_PLAN_GAP_MAX_ABS)
        before = prof_threshold
        if gap > 0.0 and ctx_dd > -ENTRY_CTX_MAX_DRAWDOWN_PCT * 0.5:
            prof_threshold = max(
                ENTRY_DYNAMIC_MIN_LO,
                prof_threshold - (gap / ENTRY_PLAN_GAP_MAX_ABS) * ENTRY_PLAN_GAP_TILT,
            )
        elif gap < 0.0:
            prof_threshold = min(
                1.0,
                prof_threshold + (abs(gap) / ENTRY_PLAN_GAP_MAX_ABS) * (ENTRY_PLAN_GAP_TILT * 0.5),
            )
        prof_threshold = max(base_threshold, prof_threshold)
        plan_delta = prof_threshold - before

    meta.update(
        {
            "effective_threshold": float(prof_threshold),
            "base_threshold": float(base_threshold),
            "risk_off_tightening": bool(risk_off_flag),
            "manip_tightening": bool(manip_flag),
            "risk_profile": _effective_risk_profile(features),
            "regime_risk_mode": int(regime_mode_lock),
            "dynamic_floor": float(_global_min_threshold()),
            "ctx_entry_score_gate": float(ctx_entry_gate) if ctx_entry_gate > 0 else None,
            "ctx_plan_gap_pct": float(plan_gap_pct),
            "ctx_plan_status": plan_status,
            "plan_gap_threshold_delta": float(plan_delta),
            "source_key": _source_key(features),
            "playbook_key": _playbook_key(features),
        }
    )

    passed = score >= prof_threshold
    if not passed:
        reason = f"score_below_threshold({score:.3f}<{prof_threshold:.3f})"
        meta.update({"reason": reason, "pass": False})
        _append_flag(meta, "score_below_threshold")
        await _log_entry_decision_best_effort(features, ok=False, score=score, reason=reason)
        return EntryDecision(
            False,
            float(score),
            float(prof_threshold),
            reason,
            _one_line_explain(features, reason),
            meta if isinstance(meta, dict) else None,
        )

    reason = "ok"
    meta.update({"reason": reason, "pass": True})
    await _log_entry_decision_best_effort(features, ok=True, score=score, reason=reason)
    return EntryDecision(
        True,
        float(score),
        float(prof_threshold),
        reason,
        _one_line_explain(features, reason),
        meta if isinstance(meta, dict) else None,
    )


# -----------------------------------------------------------------------------
# Test-friendly + JSON-safe decision API
# -----------------------------------------------------------------------------
async def decide_entry(features: dict[str, Any], ctx: dict | None = None) -> dict[str, Any]:
    d = await decide_entry_obj(features, ctx=ctx)
    return d.to_dict()


# -----------------------------------------------------------------------------
# Backwards-compatible API
# -----------------------------------------------------------------------------
async def should_enter(
    features: dict[str, Any], ctx: dict | None = None
) -> tuple[bool, float, str]:
    d = await decide_entry_obj(features, ctx=ctx)
    return d.ok, d.score, d.explain


def score_entry_sync(features: dict[str, Any], ctx: dict | None = None) -> float:
    """
    Safe sync wrapper. If called inside a running loop, returns heuristic-only.
    """
    _merge_ctx_best_effort(features, ctx)
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        if ENTRY_DEBUG_LOG:
            _log_event("ENTRY_SYNC_IN_ASYNC_CONTEXT", mode="heuristic_only")
        return _heuristic_score(features)

    return asyncio.run(score_entry(features, ctx=ctx))


def decide_entry_sync_obj(features: dict[str, Any], ctx: dict | None = None) -> EntryDecision:
    """
    Sync wrapper returning structured decision. If called inside a running loop,
    returns a deterministic heuristic-only decision (no IO).
    """
    _merge_ctx_best_effort(features, ctx)
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        meta = _ensure_meta(features)
        score = _heuristic_score(features)
        base = _risk_profile_threshold_adjust(
            _playbook_min_threshold(_source_min_threshold(features), features), features
        )
        meta.update(
            {
                "reason": "sync_in_async_context",
                "pass": bool(score >= base),
                "effective_threshold": float(base),
            }
        )
        ok = bool(score >= base)
        reason = "ok" if ok else f"score_below_threshold({score:.3f}<{base:.3f})"
        return EntryDecision(
            ok, float(score), float(base), reason, _one_line_explain(features, reason), meta
        )

    return asyncio.run(decide_entry_obj(features, ctx=ctx))


def decide_entry_sync(features: dict[str, Any], ctx: dict | None = None) -> dict[str, Any]:
    d = decide_entry_sync_obj(features, ctx=ctx)
    return d.to_dict()


# -----------------------------------------------------------------------------
# Dynamic calibration loop (optional)
# -----------------------------------------------------------------------------
async def ai_maintenance_loop(stop_evt: asyncio.Event) -> None:
    global _ENTRY_DYNAMIC_MIN_SCORE

    if not ENTRY_DYNAMIC_MIN_ENABLED:
        _log_event("AI_MAINT_DISABLED", reason="ENTRY_DYNAMIC_MIN_ENABLED=0")
        return

    interval_sec = ENTRY_AI_MAINT_INTERVAL_SEC
    backoff = interval_sec

    async with _DYNAMIC_LOCK:
        if _ENTRY_DYNAMIC_MIN_SCORE is None:
            _ENTRY_DYNAMIC_MIN_SCORE = ENTRY_MIN_SCORE

    _log_event(
        "AI_MAINT_START",
        interval_sec=interval_sec,
        base_min=ENTRY_MIN_SCORE,
        lo=ENTRY_DYNAMIC_MIN_LO,
        hi=ENTRY_DYNAMIC_MIN_HI,
        step=ENTRY_DYNAMIC_STEP,
    )

    while not stop_evt.is_set():
        try:
            snap: dict[str, Any] = {}
            if not _contract_no_io():
                try:
                    from utils_pkg import risk_manager as _rm  # type: ignore

                    snap = await _rm.risk_capacity_snapshot()
                except Exception as e:
                    if ENTRY_DEBUG_LOG:
                        _log_event("AI_MAINT_RISK_SNAPSHOT_FAIL", err=str(e)[:200])

            win_rate = float(_finite(snap.get("rolling_win_rate_pct", 0.0), 0.0))
            daily_pnl_pct = float(_finite(snap.get("daily_realized_pnl_pct", 0.0), 0.0))
            daily_dd_pct = float(
                _finite(snap.get("daily_drawdown_pct", snap.get("max_dd_pct", 0.0)), 0.0)
            )
            loss_streak = float(_finite(snap.get("recent_loss_streak", 0.0), 0.0))
            plan_gap_pct = float(_finite(snap.get("plan_gap_pct", 0.0), 0.0))

            prev_floor = _global_min_threshold()
            new_floor = prev_floor

            dd_bad = daily_dd_pct <= -ENTRY_CTX_MAX_DRAWDOWN_PCT * 0.5
            win_good = win_rate >= 55.0
            win_excellent = win_rate >= 65.0
            pnl_good = daily_pnl_pct >= 4.0
            pnl_bad = daily_pnl_pct <= -3.0
            streak_bad = loss_streak >= 4.0

            if dd_bad or pnl_bad or streak_bad:
                new_floor = min(ENTRY_DYNAMIC_MIN_HI, prev_floor + ENTRY_DYNAMIC_STEP)
            elif (win_excellent and daily_pnl_pct >= 6.0) or (win_good and pnl_good):
                new_floor = max(ENTRY_DYNAMIC_MIN_LO, prev_floor - ENTRY_DYNAMIC_STEP)

            if ENTRY_USE_PLAN_GAP_TILT and new_floor == prev_floor and plan_gap_pct != 0.0:
                gap = _clip(plan_gap_pct, -ENTRY_PLAN_GAP_MAX_ABS, ENTRY_PLAN_GAP_MAX_ABS)
                if gap > 0.0 and daily_dd_pct > -ENTRY_CTX_MAX_DRAWDOWN_PCT * 0.3:
                    new_floor = max(ENTRY_DYNAMIC_MIN_LO, prev_floor - ENTRY_DYNAMIC_STEP * 0.5)
                elif gap < 0.0 and daily_pnl_pct >= 0.0:
                    new_floor = min(ENTRY_DYNAMIC_MIN_HI, prev_floor + ENTRY_DYNAMIC_STEP * 0.5)

            new_floor = max(ENTRY_DYNAMIC_MIN_LO, min(ENTRY_DYNAMIC_MIN_HI, new_floor))

            async with _DYNAMIC_LOCK:
                _ENTRY_DYNAMIC_MIN_SCORE = new_floor

            _log_event(
                "AI_MAINT_STEP",
                prev_floor=float(prev_floor),
                new_floor=float(new_floor),
                win_rate=win_rate,
                daily_pnl_pct=daily_pnl_pct,
                daily_dd_pct=daily_dd_pct,
                loss_streak=loss_streak,
                plan_gap_pct=plan_gap_pct,
            )

            backoff = interval_sec

        except asyncio.CancelledError:
            raise
        except Exception as e:
            _log_event("AI_MAINT_ERROR", error=str(e)[:240])
            backoff = min(backoff * 1.5, max(600.0, interval_sec * 4))

        remaining = backoff
        while remaining > 0 and not stop_evt.is_set():
            sl = min(5.0, remaining)
            await asyncio.sleep(sl)
            remaining -= sl

    _log_event("AI_MAINT_STOP")


# -----------------------------------------------------------------------------
# Diagnostics
# -----------------------------------------------------------------------------
async def diag_snapshot() -> dict[str, Any]:
    out: dict[str, Any] = {
        "config": {
            "ENTRY_AI_ENABLED": ENTRY_AI_ENABLED,
            "ENTRY_AI_WEIGHT": ENTRY_AI_WEIGHT,
            "ENTRY_AI_TIMEOUT_SEC": ENTRY_AI_TIMEOUT_SEC,
            "ENTRY_AI_TEMPERATURE": ENTRY_AI_TEMPERATURE,
            "ENTRY_AI_MAX_TOKENS": ENTRY_AI_MAX_TOKENS,
            "ENTRY_AI_MAX_PARALLEL": ENTRY_AI_MAX_PARALLEL,
            "ENTRY_AI_CALL_POLICY": ENTRY_AI_CALL_POLICY,
            "ENTRY_AI_BORDERLINE_MARGIN": ENTRY_AI_BORDERLINE_MARGIN,
            "ENTRY_AI_SHADOW_MODE": ENTRY_AI_SHADOW_MODE,
            "ENTRY_AI_STRICT": ENTRY_AI_STRICT,
            "ENTRY_MIN_SCORE": ENTRY_MIN_SCORE,
            "ENTRY_DYNAMIC_MIN_ENABLED": ENTRY_DYNAMIC_MIN_ENABLED,
            "ENTRY_DYNAMIC_MIN_LO": ENTRY_DYNAMIC_MIN_LO,
            "ENTRY_DYNAMIC_MIN_HI": ENTRY_DYNAMIC_MIN_HI,
            "ENTRY_DYNAMIC_STEP": ENTRY_DYNAMIC_STEP,
            "ENTRY_MIN_AI_SCORE": ENTRY_MIN_AI_SCORE,
            "ENTRY_MIN_LIQ_USD": ENTRY_MIN_LIQ_USD,
            "ENTRY_MAX_RUG_SCORE": ENTRY_MAX_RUG_SCORE,
            "ENTRY_MIN_5M_BUYS": ENTRY_MIN_5M_BUYS,
            "ENTRY_MIN_5M_VOL_USD": ENTRY_MIN_5M_VOL_USD,
            "ENTRY_REGIME_TIGHTEN_ENABLED": ENTRY_REGIME_TIGHTEN_ENABLED,
            "ENTRY_RISK_OFF_EXTRA_MARGIN": ENTRY_RISK_OFF_EXTRA_MARGIN,
            "ENTRY_MANIP_HIGH": ENTRY_MANIP_HIGH,
            "ENTRY_FORBID_DEADLIKE": ENTRY_FORBID_DEADLIKE,
            "ENTRY_CTX_MAX_DRAWDOWN_PCT": ENTRY_CTX_MAX_DRAWDOWN_PCT,
            "ENTRY_CTX_MAX_OPEN_POS_RATIO": ENTRY_CTX_MAX_OPEN_POS_RATIO,
            "ENTRY_CTX_LOCK_AFTER_TARGET_PNL": ENTRY_CTX_LOCK_AFTER_TARGET_PNL,
            "ENTRY_REQUIRE_RISK_CAPACITY": ENTRY_REQUIRE_RISK_CAPACITY,
            "ENTRY_MIN_RISK_CAPACITY": ENTRY_MIN_RISK_CAPACITY,
            "RISK_PROFILE": RISK_PROFILE,
            "ENTRY_REGIME_RISK_MODE_LOCK": ENTRY_REGIME_RISK_MODE_LOCK,
        },
        "dynamic_floor": _global_min_threshold(),
        "dynamic_min_score_raw": _ENTRY_DYNAMIC_MIN_SCORE,
        "openai_client_kind": _CLIENT_KIND,
        "has_openai_key": bool(os.getenv("OPENAI_API_KEY")),
        "ai_last_call_items": len(_AI_LAST_CALL),
        "model": _pick_entry_model({}),
        "contract_no_io": _contract_no_io(),
    }

    try:
        from utils_pkg import risk_manager as _rm  # type: ignore

        snap = await _rm.risk_capacity_snapshot()
        if isinstance(snap, dict):
            out["risk_capacity_snapshot"] = {
                "risk_capacity": snap.get("risk_capacity"),
                "risk_mode": snap.get("risk_mode"),
                "open_positions": snap.get("open_positions"),
                "max_positions": snap.get("max_positions"),
                "daily_realized_pnl_pct": snap.get("daily_realized_pnl_pct"),
                "daily_drawdown_pct": snap.get("daily_drawdown_pct", snap.get("max_dd_pct")),
                "rolling_win_rate_pct": snap.get("rolling_win_rate_pct"),
                "recent_loss_streak": snap.get("recent_loss_streak"),
                "hard_halt": snap.get("hard_halt"),
                "entry_score_gate": snap.get("entry_score_gate"),
                "plan_gap_pct": snap.get("plan_gap_pct"),
                "plan_status": snap.get("plan_status"),
            }
    except Exception as e:
        out["risk_error"] = f"risk_manager_snapshot_failed: {e}"

    try:
        from utils_pkg.regime import regime_state  # type: ignore

        feats = await regime_state.features()
        if isinstance(feats, dict):
            out["regime_features"] = {
                "regime_label": feats.get("regime_label"),
                "regime_trend": feats.get("regime_trend"),
                "regime_volatility": feats.get("regime_volatility"),
                "regime_liquidity": feats.get("regime_liquidity"),
                "regime_manipulation_risk": feats.get("regime_manipulation_risk"),
                "regime_risk_multiplier": feats.get("regime_risk_multiplier"),
            }
    except Exception as e:
        out["regime_error"] = f"regime_state_features_failed: {e}"

    return out


__all__ = [
    "EntryDecision",
    "_heuristic_score",
    "ai_maintenance_loop",
    "decide_entry",
    "decide_entry_obj",
    "decide_entry_sync",
    "decide_entry_sync_obj",
    "diag_snapshot",
    "score_entry",
    "score_entry_sync",
    "should_enter",
]
