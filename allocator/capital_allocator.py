# allocator/capital_allocator.py
from __future__ import annotations

"""
Capital Allocator (P2 / optional — MUST NOT silently compete with the live sniper path)

What this module does
---------------------
Computes capital "sleeve weights" for strategies (default: daytrader / sniper / perps)
using trailing daily returns + a regime label, then publishes weights to Redis.

Key principles
--------------
- SAFE BY DEFAULT: allocator is OFF unless ALLOCATOR_ENABLED=1
- No hard dependency on numpy
- Defensive math (clips outliers, handles small samples)
- Deterministic normalization with caps + floors
- Optional smoothing to avoid thrash (EWMA)
- Redis is optional (no crash if redis pkg missing or REDIS_URL absent)

Environment variables
---------------------
Core:
  ALLOCATOR_ENABLED=0|1                 (default 0)
  ALLOCATOR_REDIS_KEY=alloc:weights     (default alloc:weights)
  ALLOCATOR_INTERVAL_SEC=600            (default 600)
  ALLOCATOR_MIN_SAMPLES=20              (default 20)
  ALLOCATOR_CLIP_ABS_RET=0.25           (default 0.25)  # clip daily returns to +/- this
  ALLOCATOR_ANNUALIZE_DAYS=365          (default 365)

Scoring:
  ALLOCATOR_W_SHARPE=0.5                (default 0.5)
  ALLOCATOR_W_SORTINO=0.5               (default 0.5)
  ALLOCATOR_W_MEAN=0.0                  (default 0.0)   # optional extra mean-return weight

Smoothing:
  ALLOCATOR_SMOOTH_ALPHA=0.25           (default 0.25)  # 0=no smoothing, 1=use new fully

Caps/Floors:
  SLEEVE_WEIGHT_CAP=0.60                (default 0.60)
  SLEEVE_WEIGHT_FLOOR=0.05              (default 0.05)
  SLEEVE_CAP_DAYTRADER / SLEEVE_CAP_SNIPER / SLEEVE_CAP_PERPS (optional per-sleeve caps)
  SLEEVE_FLOOR_DAYTRADER / ...                               (optional per-sleeve floors)

Redis:
  REDIS_URL=redis://... or rediss://...
"""

import json
import logging
import math
import os
import time
from collections.abc import Callable, Mapping, MutableMapping, Sequence
from dataclasses import dataclass

try:
    import redis as _redis  # type: ignore
except Exception:
    _redis = None  # pragma: no cover


SLEEVES_DEFAULT: tuple[str, ...] = ("daytrader", "sniper", "perps")


# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
def _get_logger() -> logging.Logger:
    # Prefer your canonical logging spine if present, but never hard-depend.
    try:
        from logging_setup import get_logger  # type: ignore

        return get_logger("allocator")
    except Exception:
        log = logging.getLogger("allocator")
        if not log.handlers:
            h = logging.StreamHandler()
            fmt = logging.Formatter("[%(levelname)s] %(name)s: %(message)s")
            h.setFormatter(fmt)
            log.addHandler(h)
            log.setLevel(logging.INFO)
        return log


log = _get_logger()


# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
def _env_float(key: str, default: float) -> float:
    v = (os.getenv(key) or "").strip()
    if not v:
        return default
    try:
        return float(v)
    except Exception:
        return default


def _env_int(key: str, default: int) -> int:
    v = (os.getenv(key) or "").strip()
    if not v:
        return default
    try:
        return int(float(v))
    except Exception:
        return default


def _env_bool(key: str, default: bool = False) -> bool:
    v = (os.getenv(key) or "").strip().lower()
    if not v:
        return default
    return v in ("1", "true", "yes", "y", "on", "t")


@dataclass(frozen=True)
class AllocatorConfig:
    enabled: bool = False
    redis_key: str = "alloc:weights"
    interval_sec: int = 600

    sleeves: tuple[str, ...] = SLEEVES_DEFAULT

    min_samples: int = 20
    clip_abs_ret: float = 0.25
    annualize_days: int = 365

    w_sharpe: float = 0.5
    w_sortino: float = 0.5
    w_mean: float = 0.0

    smooth_alpha: float = 0.25  # 0=no smoothing, 1=no memory

    weight_cap: float = 0.60
    weight_floor: float = 0.05

    @staticmethod
    def from_env() -> AllocatorConfig:
        enabled = _env_bool("ALLOCATOR_ENABLED", False)
        redis_key = (os.getenv("ALLOCATOR_REDIS_KEY") or "alloc:weights").strip() or "alloc:weights"
        interval_sec = _env_int("ALLOCATOR_INTERVAL_SEC", 600)

        min_samples = _env_int("ALLOCATOR_MIN_SAMPLES", 20)
        clip_abs_ret = _env_float("ALLOCATOR_CLIP_ABS_RET", 0.25)
        annualize_days = _env_int("ALLOCATOR_ANNUALIZE_DAYS", 365)

        w_sharpe = _env_float("ALLOCATOR_W_SHARPE", 0.5)
        w_sortino = _env_float("ALLOCATOR_W_SORTINO", 0.5)
        w_mean = _env_float("ALLOCATOR_W_MEAN", 0.0)

        smooth_alpha = _env_float("ALLOCATOR_SMOOTH_ALPHA", 0.25)
        smooth_alpha = max(0.0, min(1.0, smooth_alpha))

        weight_cap = _env_float("SLEEVE_WEIGHT_CAP", 0.60)
        weight_floor = _env_float("SLEEVE_WEIGHT_FLOOR", 0.05)

        # Optional sleeves override (rare). Keep deterministic ordering.
        sleeves_raw = (os.getenv("ALLOCATOR_SLEEVES") or "").strip()
        sleeves = SLEEVES_DEFAULT
        if sleeves_raw:
            parts = [
                p.strip().lower() for p in sleeves_raw.replace(";", ",").split(",") if p.strip()
            ]
            if parts:
                sleeves = tuple(dict.fromkeys(parts))  # dedupe, preserve order

        return AllocatorConfig(
            enabled=enabled,
            redis_key=redis_key,
            interval_sec=interval_sec,
            sleeves=sleeves,
            min_samples=min_samples,
            clip_abs_ret=clip_abs_ret,
            annualize_days=annualize_days,
            w_sharpe=w_sharpe,
            w_sortino=w_sortino,
            w_mean=w_mean,
            smooth_alpha=smooth_alpha,
            weight_cap=weight_cap,
            weight_floor=weight_floor,
        )


# -----------------------------------------------------------------------------
# Redis (optional)
# -----------------------------------------------------------------------------
def _redis_client():
    url = (os.getenv("REDIS_URL") or "").strip()
    if not url or _redis is None:
        return None
    try:
        # redis-py handles rediss:// with ssl=True; be explicit.
        return _redis.from_url(url, decode_responses=True, ssl=url.startswith("rediss://"))
    except Exception as e:
        log.warning("Redis unavailable: %s", e)
        return None


# -----------------------------------------------------------------------------
# Math helpers (no numpy)
# -----------------------------------------------------------------------------
def _clip(xs: Sequence[float], clip_abs: float) -> list[float]:
    if clip_abs <= 0:
        return list(xs)
    lo, hi = -abs(clip_abs), abs(clip_abs)
    out: list[float] = []
    for x in xs:
        try:
            xf = float(x)
        except Exception:
            continue
        if xf < lo:
            xf = lo
        elif xf > hi:
            xf = hi
        out.append(xf)
    return out


def _mean(xs: Sequence[float]) -> float:
    n = len(xs)
    if n <= 0:
        return 0.0
    return float(sum(xs)) / float(n)


def _std(xs: Sequence[float]) -> float:
    # population std (stable for our purpose)
    n = len(xs)
    if n <= 1:
        return 0.0
    mu = _mean(xs)
    var = sum((x - mu) * (x - mu) for x in xs) / float(n)
    return math.sqrt(max(0.0, var))


def _downside_std(xs: Sequence[float]) -> float:
    # downside deviation based on negative returns only
    downs = [min(0.0, float(x)) for x in xs]
    # If no downside, std is 0 -> handled by epsilon later
    return _std(downs)


def _sharpe(xs: Sequence[float], annualize_days: int) -> float:
    if len(xs) < 2:
        return 0.0
    mu = _mean(xs)
    sd = _std(xs)
    if sd <= 1e-12:
        return 0.0
    return (mu / sd) * math.sqrt(max(1, annualize_days))


def _sortino(xs: Sequence[float], annualize_days: int) -> float:
    if len(xs) < 2:
        return 0.0
    mu = _mean(xs)
    dd = _downside_std(xs)
    if dd <= 1e-12:
        # no downside observed: treat as strong but bounded signal
        return (mu / 1e-6) * math.sqrt(max(1, annualize_days))
    return (mu / dd) * math.sqrt(max(1, annualize_days))


def _normalize_positive(scores: Mapping[str, float], sleeves: Sequence[str]) -> dict[str, float]:
    # Only positive scores contribute; if all <= 0 => uniform
    pos = {k: max(0.0, float(scores.get(k, 0.0))) for k in sleeves}
    s = sum(pos.values())
    if s <= 1e-12:
        u = 1.0 / float(len(sleeves)) if sleeves else 0.0
        return {k: u for k in sleeves}
    return {k: pos[k] / s for k in sleeves}


def _apply_caps_floors(
    w: MutableMapping[str, float],
    sleeves: Sequence[str],
    global_cap: float,
    global_floor: float,
) -> dict[str, float]:
    # Per-sleeve overrides (optional)
    caps: dict[str, float] = {}
    floors: dict[str, float] = {}
    for s in sleeves:
        caps[s] = _env_float(f"SLEEVE_CAP_{s.upper()}", global_cap)
        floors[s] = _env_float(f"SLEEVE_FLOOR_{s.upper()}", global_floor)

    # Clamp
    for s in sleeves:
        w[s] = float(w.get(s, 0.0))
        w[s] = max(floors[s], min(caps[s], w[s]))

    # Renormalize (while respecting floors)
    # If floors sum > 1, fallback to proportional floors.
    floor_sum = sum(floors[s] for s in sleeves)
    if floor_sum >= 1.0:
        return _normalize_positive(floors, sleeves)

    # Allocate remaining mass proportionally to (w - floor)
    residual = 1.0 - floor_sum
    prefs = {s: max(0.0, w[s] - floors[s]) for s in sleeves}
    pref_sum = sum(prefs.values())

    out: dict[str, float] = {}
    if pref_sum <= 1e-12:
        # all at floors + uniform residual
        add = residual / float(len(sleeves)) if sleeves else 0.0
        for s in sleeves:
            out[s] = floors[s] + add
        return out

    for s in sleeves:
        out[s] = floors[s] + residual * (prefs[s] / pref_sum)

    # Final safety normalize (floating error)
    tot = sum(out.values())
    if tot > 0:
        for s in sleeves:
            out[s] /= tot
    return out


def _blend(
    prev: Mapping[str, float], new: Mapping[str, float], sleeves: Sequence[str], alpha: float
) -> dict[str, float]:
    # alpha = 1 -> all new, alpha=0 -> all prev
    out: dict[str, float] = {}
    for s in sleeves:
        pv = float(prev.get(s, 0.0))
        nv = float(new.get(s, 0.0))
        out[s] = (1.0 - alpha) * pv + alpha * nv
    # Normalize hard
    tot = sum(max(0.0, v) for v in out.values())
    if tot <= 1e-12:
        u = 1.0 / float(len(sleeves)) if sleeves else 0.0
        return {s: u for s in sleeves}
    return {s: max(0.0, out[s]) / tot for s in sleeves}


# -----------------------------------------------------------------------------
# Core allocation
# -----------------------------------------------------------------------------
def allocate(
    trailing: Mapping[str, Sequence[float]], regime: str, cfg: AllocatorConfig | None = None
) -> dict[str, float]:
    """
    trailing: sleeve -> list of daily returns (floats, e.g. 0.003 for +0.3%)
    regime: string label, e.g. "low_vol_trend", "high_vol_trend", "high_vol_chop", "risk_off", ...
    """
    cfg = cfg or AllocatorConfig.from_env()
    sleeves = list(cfg.sleeves)

    # Compute raw scores
    scores: dict[str, float] = {}
    for s in sleeves:
        xs = list(trailing.get(s, []) or [])
        xs = _clip(xs, cfg.clip_abs_ret)
        if len(xs) < max(2, cfg.min_samples):
            # Soft score for low samples: still allow but dampen strongly.
            damp = max(0.1, float(len(xs)) / float(max(1, cfg.min_samples)))
        else:
            damp = 1.0

        sh = _sharpe(xs, cfg.annualize_days)
        so = _sortino(xs, cfg.annualize_days)
        mu = _mean(xs) * math.sqrt(max(1, cfg.annualize_days))  # scaled mean proxy

        score = cfg.w_sharpe * sh + cfg.w_sortino * so + cfg.w_mean * mu
        scores[s] = float(score) * float(damp)

    w = _normalize_positive(scores, sleeves)

    # Regime bias (bounded)
    r = (regime or "").strip().lower()
    mult: dict[str, float] = {s: 1.0 for s in sleeves}

    if r in ("low_vol_trend", "high_vol_trend", "trend"):
        # trend regimes: allow momentum/continuation sleeves more room
        if "daytrader" in mult:
            mult["daytrader"] *= 1.15
        if "perps" in mult:
            mult["perps"] *= 1.15
        if "sniper" in mult:
            mult["sniper"] *= 0.90
    elif r in ("high_vol_chop", "chop"):
        # chop favors opportunistic/mean-reversion: sniper slightly up, daytrader down
        if "sniper" in mult:
            mult["sniper"] *= 1.10
        if "daytrader" in mult:
            mult["daytrader"] *= 0.92
        if "perps" in mult:
            mult["perps"] *= 0.95
    elif r in ("risk_off", "panic", "crash"):
        # no cash sleeve here; just de-risk all equally a bit (downstream should also reduce sizing)
        for s in mult:
            mult[s] *= 0.95

    # Apply regime multipliers then renormalize
    w2 = {s: w.get(s, 0.0) * mult.get(s, 1.0) for s in sleeves}
    w = _normalize_positive(w2, sleeves)

    # Enforce caps/floors (global + per sleeve overrides)
    w = _apply_caps_floors(dict(w), sleeves, cfg.weight_cap, cfg.weight_floor)

    return w


def publish(weights: Mapping[str, float], cfg: AllocatorConfig | None = None, r=None) -> None:
    cfg = cfg or AllocatorConfig.from_env()
    r = r if r is not None else _redis_client()
    if r is None:
        return

    payload = {
        "ts": int(time.time()),
        "weights": {k: float(v) for k, v in weights.items()},
    }
    try:
        r.set(cfg.redis_key, json.dumps(payload, separators=(",", ":")))
        # helpful side keys (optional)
        r.set(cfg.redis_key + ":ts", str(payload["ts"]))
    except Exception as e:
        log.warning("publish failed: %s", e)


def _load_prev_weights(r, cfg: AllocatorConfig) -> dict[str, float]:
    if r is None:
        return {}
    try:
        raw = r.get(cfg.redis_key)
        if not raw:
            return {}
        obj = json.loads(raw)
        if isinstance(obj, dict) and "weights" in obj and isinstance(obj["weights"], dict):
            return {k: float(v) for k, v in obj["weights"].items()}
        if isinstance(obj, dict):
            # support legacy flat dict
            return {k: float(v) for k, v in obj.items()}
        return {}
    except Exception:
        return {}


def run_allocator(
    get_trailing_returns: Callable[[], Mapping[str, Sequence[float]]],
    get_regime_returns: Callable[[], Sequence[float]],
    interval_sec: int | None = None,
) -> None:
    """
    Backwards-compatible runner.

    - get_trailing_returns(): -> dict sleeve -> list of daily returns (float, e.g. 0.003)
    - get_regime_returns(): -> sequence of recent equity returns for regime classification
    Publishes JSON to Redis at ALLOCATOR_REDIS_KEY (default alloc:weights).

    NOTE: This is P2 and should only be used if explicitly enabled.
    """
    cfg = AllocatorConfig.from_env()
    if not cfg.enabled:
        log.info("Allocator disabled (set ALLOCATOR_ENABLED=1 to enable). Exiting.")
        return

    # Import classify_regime only when enabled (no competing code paths)
    from allocator.regime import classify_regime  # type: ignore

    interval = int(interval_sec or cfg.interval_sec)
    interval = max(5, interval)

    r = _redis_client()
    log.info(
        "Allocator started: key=%s interval=%ss sleeves=%s",
        cfg.redis_key,
        interval,
        ",".join(cfg.sleeves),
    )

    while True:
        try:
            trailing = get_trailing_returns() or {}
            regime_arr = list(get_regime_returns() or [])
            regime = str(classify_regime(regime_arr) or "").strip().lower()

            new_w = allocate(trailing, regime, cfg=cfg)

            # Optional smoothing vs last published
            if cfg.smooth_alpha < 1.0:
                prev = _load_prev_weights(r, cfg)
                if prev:
                    new_w = _blend(prev, new_w, cfg.sleeves, alpha=cfg.smooth_alpha)

            publish(new_w, cfg=cfg, r=r)
            log.info(
                "Published weights regime=%s weights=%s",
                regime,
                {k: round(v, 4) for k, v in new_w.items()},
            )

        except Exception as e:
            log.exception("Allocator loop error: %s", e)

        time.sleep(interval)


# Optional CLI: `python -m allocator.capital_allocator`
def _default_noop_returns() -> Mapping[str, Sequence[float]]:
    return {s: [] for s in AllocatorConfig.from_env().sleeves}


def _default_noop_regime_returns() -> Sequence[float]:
    return []


def main() -> None:
    cfg = AllocatorConfig.from_env()
    if not cfg.enabled:
        log.info("Allocator disabled. Set ALLOCATOR_ENABLED=1 to run.")
        return
    run_allocator(
        _default_noop_returns, _default_noop_regime_returns, interval_sec=cfg.interval_sec
    )


if __name__ == "__main__":
    main()
