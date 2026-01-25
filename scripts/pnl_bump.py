#!/usr/bin/env python3
from __future__ import annotations

"""
scripts/pnl_bump.py — DEV-ONLY: bump realized daily PnL in Redis (V3+++)
======================================================================

This is a *quarantine / testing* tool to simulate daily realized PnL moves
for dashboards, alerts, circuit-breakers, and risk_manager wiring.

LOCK-IN (canonical key contract)
--------------------------------
We write/read the canonical key used by the V3+++ suite:

    pnl:day:realized_usd:YYYYMMDD

Backward-compatibility (optional)
---------------------------------
Some older scripts used:

    pnl:day:YYYYMMDD:realized_usd

You can bump legacy keys with flags.

Safety rails (IMPORTANT)
------------------------
This tool can poison your risk controls if run in production by mistake.
By default, it will refuse to run unless you provide one of:
  - --force
  - or environment variable PNL_BUMP_ALLOW=1

Usage
-----
python -m scripts.pnl_bump --usd 0.25
python -m scripts.pnl_bump --usd -1.00 --date 20260104
python -m scripts.pnl_bump --set 10.0
python -m scripts.pnl_bump --usd 0.10 --both-keys
python -m scripts.pnl_bump --usd 0.10 --json
"""

import argparse
import asyncio
import contextlib
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

# ──────────────────────────────────────────────────────────────
# Repo root / .env bootstrap (best-effort)
# ──────────────────────────────────────────────────────────────

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

with contextlib.suppress(Exception):
    from dotenv import find_dotenv, load_dotenv  # type: ignore

    load_dotenv(find_dotenv(), override=False)

# ──────────────────────────────────────────────────────────────
# Logging (canonical root logging_setup.py)
# ──────────────────────────────────────────────────────────────


def _get_logger():
    try:
        from logging_setup import get_logger  # type: ignore

        return get_logger()
    except Exception:
        import logging

        return logging.getLogger("pnl_bump")


logger = _get_logger()

# ──────────────────────────────────────────────────────────────
# Core utils (resources / redis)
# ──────────────────────────────────────────────────────────────

import utils_pkg.core_utils as cu  # type: ignore

# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}


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
    if v is None or not str(v).strip():
        return float(default)
    try:
        return float(str(v).strip())
    except Exception:
        return float(default)


def _now_utc_yyyymmdd() -> str:
    return datetime.now(UTC).strftime("%Y%m%d")


def _validate_yyyymmdd(s: str) -> str:
    s = (s or "").strip()
    if len(s) != 8 or not s.isdigit():
        raise ValueError("date must be YYYYMMDD")
    # basic sanity parse
    datetime(int(s[0:4]), int(s[4:6]), int(s[6:8]), tzinfo=UTC)
    return s


def _key_canonical(date_yyyymmdd: str) -> str:
    # LOCK-IN: used by V3+++ suite
    return f"pnl:day:realized_usd:{date_yyyymmdd}"


def _key_legacy(date_yyyymmdd: str) -> str:
    # legacy ordering observed in older scripts
    return f"pnl:day:{date_yyyymmdd}:realized_usd"


def _ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _stderr(msg: str) -> None:
    with contextlib.suppress(Exception):
        sys.stderr.write(f"[pnl_bump] {_ts()} {msg}\n")
        sys.stderr.flush()


async def _maybe_await(x: Any) -> Any:
    return await x if asyncio.iscoroutine(x) else x


async def _get_redis() -> Any:
    """
    Prefer async redis getter if core_utils provides it; fall back to cu.redis.
    """
    for nm in ("get_redis_async", "get_redis"):
        fn = getattr(cu, nm, None)
        if callable(fn):
            with contextlib.suppress(Exception):
                r = await _maybe_await(fn())
                if r is not None:
                    return r
    return getattr(cu, "redis", None)


async def _redis_get_float(redis: Any, key: str) -> float | None:
    if redis is None:
        return None
    try:
        v = await redis.get(key)
        if v is None:
            return None
        if isinstance(v, bytes):
            v = v.decode("utf-8", "ignore")
        return float(str(v).strip())
    except Exception:
        return None


# ──────────────────────────────────────────────────────────────
# Config / CLI
# ──────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Config:
    date: str
    delta_usd: float | None
    set_usd: float | None
    dry_run: bool
    json_out: bool
    canonical: bool
    legacy: bool
    force: bool
    strict: bool
    max_abs: float


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="DEV-ONLY: bump today's realized PnL (USD) in Redis.")

    p.add_argument(
        "--date",
        type=str,
        default=_now_utc_yyyymmdd(),
        help="Target day (UTC) as YYYYMMDD.",
    )

    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--usd", type=float, default=None, help="Delta in USD to add (can be negative).")
    g.add_argument("--set", type=float, default=None, help="Set the key to an absolute USD value.")

    p.add_argument(
        "--dry-run",
        action="store_true",
        default=_env_bool("PNL_BUMP_DRY_RUN", False),
        help="Do not write; only show what would change.",
    )
    p.add_argument(
        "--json",
        action="store_true",
        default=_env_bool("PNL_BUMP_JSON", False),
        help="Emit JSON summary to STDOUT.",
    )

    # key targeting
    kg = p.add_mutually_exclusive_group()
    kg.add_argument(
        "--canonical-only",
        action="store_true",
        help="Only touch canonical key (default behavior).",
    )
    kg.add_argument("--legacy-only", action="store_true", help="Only touch legacy key.")
    kg.add_argument(
        "--both-keys", action="store_true", help="Touch both canonical and legacy keys."
    )

    p.add_argument(
        "--force",
        action="store_true",
        help="Override safety guard (otherwise requires PNL_BUMP_ALLOW=1).",
    )
    p.add_argument(
        "--strict",
        action="store_true",
        default=_env_bool("PNL_BUMP_STRICT", False),
        help="Non-zero exit if redis/key ops fail.",
    )
    p.add_argument(
        "--max-abs",
        type=float,
        default=_env_float("PNL_BUMP_MAX_ABS", 250.0),
        help="Safety clamp: refuse |delta| or |set| above this unless --force (default 250).",
    )

    return p


def _resolve_key_flags(args: argparse.Namespace) -> tuple[bool, bool]:
    """
    Returns (canonical, legacy)
    """
    if getattr(args, "both_keys", False):
        return True, True
    if getattr(args, "legacy_only", False):
        return False, True
    # default / canonical-only (also if --canonical-only explicitly set)
    return True, False


def _require_allow(force: bool) -> None:
    if force:
        return
    if _env_bool("PNL_BUMP_ALLOW", False):
        return
    raise RuntimeError("Refusing to run: set PNL_BUMP_ALLOW=1 or pass --force (DEV-ONLY tool).")


def _check_magnitude(v: float, max_abs: float, force: bool) -> None:
    if force:
        return
    if abs(float(v)) > float(max_abs):
        raise ValueError(f"Refusing change {v:+} > max_abs={max_abs}. Use --force to override.")


# ──────────────────────────────────────────────────────────────
# Core action
# ──────────────────────────────────────────────────────────────


async def _apply_delta(
    redis: Any, key: str, delta: float, *, dry_run: bool
) -> tuple[float | None, float | None]:
    """
    Returns (before, after). If key missing, before=None.
    """
    before = await _redis_get_float(redis, key)
    after = (float(before or 0.0) + float(delta)) if before is not None else float(delta)

    if dry_run:
        return before, after

    # Atomic write via INCRBYFLOAT
    await redis.incrbyfloat(key, float(delta))
    after2 = await _redis_get_float(redis, key)
    return before, after2


async def _apply_set(
    redis: Any, key: str, value: float, *, dry_run: bool
) -> tuple[float | None, float | None]:
    before = await _redis_get_float(redis, key)
    if dry_run:
        return before, float(value)

    await redis.set(key, float(value))
    after = await _redis_get_float(redis, key)
    return before, after


async def run(cfg: Config) -> dict[str, Any]:
    _require_allow(cfg.force)

    if cfg.delta_usd is not None:
        _check_magnitude(cfg.delta_usd, cfg.max_abs, cfg.force)
    if cfg.set_usd is not None:
        _check_magnitude(cfg.set_usd, cfg.max_abs, cfg.force)

    # Ensure we always attempt to close resources, even on partial init
    try:
        await cu.init_resources()

        redis = await _get_redis()
        if redis is None:
            raise RuntimeError(
                "Redis client is not initialized. Check REDIS_URL / init_resources()."
            )

        targets: dict[str, str] = {}
        if cfg.canonical:
            targets["canonical"] = _key_canonical(cfg.date)
        if cfg.legacy:
            targets["legacy"] = _key_legacy(cfg.date)
        if not targets:
            raise RuntimeError("No key targets selected (canonical/legacy).")

        results: dict[str, Any] = {
            "ok": True,
            "ts": datetime.now(UTC).isoformat(),
            "date": cfg.date,
            "dry_run": cfg.dry_run,
            "mode": "delta" if cfg.delta_usd is not None else "set",
            "delta_usd": cfg.delta_usd,
            "set_usd": cfg.set_usd,
            "targets": {},
        }

        for name, key in targets.items():
            if cfg.delta_usd is not None:
                before, after = await _apply_delta(redis, key, cfg.delta_usd, dry_run=cfg.dry_run)
            else:
                assert cfg.set_usd is not None
                before, after = await _apply_set(redis, key, cfg.set_usd, dry_run=cfg.dry_run)

            results["targets"][name] = {"key": key, "before": before, "after": after}

        return results

    finally:
        await cu.close_resources()


# ──────────────────────────────────────────────────────────────
# Entrypoint
# ──────────────────────────────────────────────────────────────


def main() -> None:
    args = build_arg_parser().parse_args()

    date = _validate_yyyymmdd(args.date)
    canonical, legacy = _resolve_key_flags(args)

    cfg = Config(
        date=date,
        delta_usd=args.usd,
        set_usd=args.set,
        dry_run=bool(args.dry_run),
        json_out=bool(args.json),
        canonical=bool(canonical),
        legacy=bool(legacy),
        force=bool(args.force),
        strict=bool(args.strict),
        max_abs=float(args.max_abs),
    )

    if not cfg.json_out:
        _stderr("DEV-ONLY tool. This mutates Redis PnL keys. Use with care.")
        _stderr(
            f"date={cfg.date} mode={'delta' if cfg.delta_usd is not None else 'set'} "
            f"targets={'both' if (cfg.canonical and cfg.legacy) else ('canonical' if cfg.canonical else 'legacy')} "
            f"dry_run={cfg.dry_run}"
        )

    try:
        res = asyncio.run(run(cfg))
        if cfg.json_out:
            print(json.dumps(res, ensure_ascii=False, default=str))
        else:
            for nm, info in (res.get("targets") or {}).items():
                print(
                    f"{nm:9s} {info.get('key')}  before={info.get('before')}  after={info.get('after')}"
                )

    except KeyboardInterrupt:
        if not cfg.json_out:
            _stderr("stopped")
        raise SystemExit(0) from None

    except Exception as e:
        msg = str(e)
        if cfg.json_out:
            print(json.dumps({"ok": False, "error": msg}, ensure_ascii=False))
        else:
            _stderr(f"ERROR: {msg}")
        raise SystemExit(2 if cfg.strict else 1) from e


if __name__ == "__main__":
    main()
