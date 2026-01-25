#!/usr/bin/env python3
# scripts/alloc_seed.py
# -*- coding: utf-8 -*-
from __future__ import annotations

"""
scripts/alloc_seed.py

Allocator cache seeder (DEV/TOOLS — quarantined)
================================================

This script seeds *local/dev* allocator inputs so you can test the allocator
/ risk / portfolio plumbing without relying on live PnL history.

Per your P0 rules:
- This is NOT part of the live trading pipeline.
- Keep it deterministic, safe, and non-secret.
- Prefer JSON outputs; write .npy only if numpy is available.

Outputs (in DATA_DIR, default ./data):
- trailing_returns.json      (dict of streams -> list[float], length=days)
- equity_returns.npy         (optional; numpy required)

Usage
-----
    python -m scripts.alloc_seed
    python -m scripts.alloc_seed --days 30 --mode dummy
    python -m scripts.alloc_seed --mode random --seed 123 --equity-len 320
    python -m scripts.alloc_seed --data-dir data --force
    python -m scripts.alloc_seed --print-only
"""

import argparse
import contextlib
import json
import os
import random
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

# ─────────────────────────────────────────────────────────────────────────────
# Project root / logging (canonical)
# ─────────────────────────────────────────────────────────────────────────────

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Optional: load .env for local convenience (Render uses env vars)
try:  # pragma: no cover
    from dotenv import find_dotenv, load_dotenv  # type: ignore
except Exception:  # pragma: no cover
    load_dotenv = None  # type: ignore
    find_dotenv = None  # type: ignore

if load_dotenv and find_dotenv:
    with contextlib.suppress(Exception):
        load_dotenv(find_dotenv(), override=False)

try:
    from logging_setup import get_logger  # type: ignore

    log = get_logger(__name__)
except Exception:  # pragma: no cover
    import logging

    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger("alloc_seed")

# Optional numpy (only needed for .npy)
try:  # pragma: no cover
    import numpy as np  # type: ignore
except Exception:  # pragma: no cover
    np = None  # type: ignore


# ─────────────────────────────────────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────────────────────────────────────


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def _atomic_write_json(path: Path, payload: Any) -> None:
    _atomic_write_text(path, json.dumps(payload, indent=2, sort_keys=True))


def _atomic_write_npy(path: Path, arr: Any) -> None:
    """
    Atomic-ish write for numpy arrays: write to temp then replace.

    NOTE: use a temp filename that cannot collide with the final .npy path.
    """
    if np is None:
        raise RuntimeError("numpy not available; cannot write .npy")

    path.parent.mkdir(parents=True, exist_ok=True)

    # If target is foo.npy -> temp is foo.tmp.npy (distinct and stable)
    if path.suffix.lower() == ".npy":
        tmp = path.with_suffix(".tmp.npy")
    else:
        tmp = path.with_name(path.name + ".tmp.npy")

    # Write using a file handle so numpy never mutates the filename/extension.
    with open(tmp, "wb") as f:
        np.save(f, arr)

    tmp.replace(path)


def _default_data_dir() -> Path:
    return Path(os.getenv("DATA_DIR", "data")).resolve()


@dataclass(frozen=True)
class StreamSpec:
    mean: float
    vol: float


def _gen_random_series(rng: random.Random, days: int, spec: StreamSpec) -> list[float]:
    out: list[float] = []
    for _ in range(days):
        out.append(rng.gauss(spec.mean, spec.vol))
    return out


def _gen_dummy_series(days: int, pattern: list[float]) -> list[float]:
    if not pattern:
        return [0.0] * days
    out: list[float] = []
    for i in range(days):
        out.append(float(pattern[i % len(pattern)]))
    return out


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _sanitize_series(xs: list[float], *, clip: float) -> list[float]:
    """
    Ensure finite floats and clip extreme values.
    """
    out: list[float] = []
    for v in xs:
        fv = _safe_float(v, 0.0)
        if fv != fv or fv in (float("inf"), float("-inf")):  # NaN/inf
            fv = 0.0
        if clip > 0:
            fv = _clamp(fv, -clip, clip)
        out.append(float(fv))
    return out


def _build_trailing_returns(
    *,
    days: int,
    mode: str,
    seed: int,
    clip: float,
    daytrader: StreamSpec,
    sniper: StreamSpec,
    perps: StreamSpec,
) -> dict[str, list[float]]:
    rng = random.Random(seed)

    if mode == "flat":
        data = {
            "daytrader": [daytrader.mean] * days,
            "sniper": [sniper.mean] * days,
            "perps": [perps.mean] * days,
        }
    elif mode == "random":
        data = {
            "daytrader": _gen_random_series(rng, days, daytrader),
            "sniper": _gen_random_series(rng, days, sniper),
            "perps": _gen_random_series(rng, days, perps),
        }
    else:
        # "dummy" pattern (matches your original spirit)
        data = {
            "daytrader": _gen_dummy_series(days, [0.002, -0.001, 0.003, 0.0, 0.0015]),
            "sniper": _gen_dummy_series(days, [0.004, -0.003, 0.002, 0.001, -0.001]),
            "perps": [0.0] * days,
        }

    return {k: _sanitize_series(v, clip=clip) for k, v in data.items()}


def _build_equity_returns(
    *,
    length: int,
    mode: str,
    seed: int,
    clip: float,
    mean: float,
    vol: float,
) -> list[float]:
    rng = random.Random(seed + 9991)

    if mode == "flat":
        xs = [mean] * length
    elif mode == "random":
        xs = [rng.gauss(mean, vol) for _ in range(length)]
    else:
        base = [0.001, -0.0005, 0.002, 0.0008]
        xs = _gen_dummy_series(length, base)

    return _sanitize_series(xs, clip=clip)


def _validate_targets(
    data_dir: Path,
    *,
    force: bool,
) -> tuple[Path, Path]:
    data_dir.mkdir(parents=True, exist_ok=True)

    trailing_path = data_dir / "trailing_returns.json"
    equity_path = data_dir / "equity_returns.npy"

    if not force:
        for p in (trailing_path, equity_path):
            if p.exists():
                raise FileExistsError(
                    f"{p} exists. Use --force to overwrite, or choose a different --data-dir."
                )
    return trailing_path, equity_path


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Seed allocator cache (DEV tool).")
    p.add_argument(
        "--data-dir", type=str, default=None, help="Target data dir (default: $DATA_DIR or ./data)."
    )
    p.add_argument("--days", type=int, default=30, help="Trailing days to seed (default: 30).")
    p.add_argument(
        "--equity-len",
        type=int,
        default=320,
        help="Length of equity_returns series (default: 320).",
    )
    p.add_argument(
        "--mode",
        type=str,
        choices=["dummy", "flat", "random"],
        default="dummy",
        help="Seed mode (default: dummy).",
    )
    p.add_argument("--seed", type=int, default=1337, help="RNG seed (default: 1337).")
    p.add_argument(
        "--clip",
        type=float,
        default=0.25,
        help="Clip absolute daily returns to this value (default: 0.25 = 25%%). Use 0 to disable.",
    )
    p.add_argument("--force", action="store_true", help="Overwrite existing files.")
    p.add_argument("--print-only", action="store_true", help="Do not write; just print a summary.")
    p.add_argument("--json", action="store_true", help="Print machine-readable JSON summary.")

    # Random mode controls (means/vols)
    p.add_argument("--daytrader-mean", type=float, default=0.0010)
    p.add_argument("--daytrader-vol", type=float, default=0.0030)
    p.add_argument("--sniper-mean", type=float, default=0.0015)
    p.add_argument("--sniper-vol", type=float, default=0.0060)
    p.add_argument("--perps-mean", type=float, default=0.0000)
    p.add_argument("--perps-vol", type=float, default=0.0020)

    p.add_argument("--equity-mean", type=float, default=0.0008)
    p.add_argument("--equity-vol", type=float, default=0.0025)

    return p


def _summarize_series(xs: list[float]) -> dict[str, Any]:
    if not xs:
        return {"n": 0}
    n = len(xs)
    mn = min(xs)
    mx = max(xs)
    avg = sum(xs) / n
    var = sum((x - avg) ** 2 for x in xs) / max(1, n - 1)
    return {
        "n": n,
        "min": mn,
        "max": mx,
        "mean": avg,
        "stdev": var**0.5,
    }


def main() -> None:
    args = build_parser().parse_args()

    days = max(1, int(args.days))
    equity_len = max(1, int(args.equity_len))
    clip = float(args.clip)

    data_dir = Path(args.data_dir).resolve() if args.data_dir else _default_data_dir()

    trailing = _build_trailing_returns(
        days=days,
        mode=args.mode,
        seed=int(args.seed),
        clip=clip,
        daytrader=StreamSpec(mean=float(args.daytrader_mean), vol=float(args.daytrader_vol)),
        sniper=StreamSpec(mean=float(args.sniper_mean), vol=float(args.sniper_vol)),
        perps=StreamSpec(mean=float(args.perps_mean), vol=float(args.perps_vol)),
    )

    equity = _build_equity_returns(
        length=equity_len,
        mode=args.mode,
        seed=int(args.seed),
        clip=clip,
        mean=float(args.equity_mean),
        vol=float(args.equity_vol),
    )

    summary = {
        "ok": True,
        "generated_at": datetime.now(UTC).isoformat(),
        "data_dir": str(data_dir),
        "mode": args.mode,
        "seed": int(args.seed),
        "clip": clip,
        "outputs": {
            "trailing_returns.json": str((data_dir / "trailing_returns.json").resolve()),
            "equity_returns.npy": str((data_dir / "equity_returns.npy").resolve()),
        },
        "numpy_available": bool(np is not None),
        "stats": {
            "daytrader": _summarize_series(trailing["daytrader"]),
            "sniper": _summarize_series(trailing["sniper"]),
            "perps": _summarize_series(trailing["perps"]),
            "equity": _summarize_series(equity),
        },
    }

    if args.print_only:
        if args.json:
            print(json.dumps(summary, indent=2, sort_keys=True, default=str))
        else:
            print("Allocator seed (print-only)")
            print("---------------------------")
            print(f"data_dir : {summary['data_dir']}")
            print(f"mode     : {summary['mode']}")
            print(f"seed     : {summary['seed']}")
            print(f"clip     : {summary['clip']}")
            print(f"numpy    : {summary['numpy_available']}")
            for k, v in summary["stats"].items():
                print(
                    f"{k:10s}: n={v.get('n')} mean={_safe_float(v.get('mean')):.6f} stdev={_safe_float(v.get('stdev')):.6f}"
                )
        return

    # Validate and write
    try:
        trailing_path, equity_path = _validate_targets(data_dir, force=bool(args.force))
    except Exception as err:
        # B904: raise from err for proper exception chaining
        summary["ok"] = False
        summary["error"] = str(err)
        if args.json:
            print(json.dumps(summary, indent=2, sort_keys=True, default=str))
        else:
            print(f"❌ {err}", file=sys.stderr)
        raise SystemExit(1) from err

    payload = {
        "meta": {
            "generated_at": summary["generated_at"],
            "mode": summary["mode"],
            "seed": summary["seed"],
            "clip": summary["clip"],
            "days": days,
        },
        "streams": trailing,
    }
    _atomic_write_json(trailing_path, payload)

    wrote_npy = False
    if np is not None:
        arr = np.array(equity, dtype=float)
        _atomic_write_npy(equity_path, arr)
        wrote_npy = True
    else:
        fallback = data_dir / "equity_returns.json"
        _atomic_write_json(
            fallback,
            {
                "meta": {
                    "generated_at": summary["generated_at"],
                    "mode": summary["mode"],
                    "seed": summary["seed"],
                    "clip": summary["clip"],
                    "length": equity_len,
                    "note": "numpy unavailable; .npy not written",
                },
                "equity_returns": equity,
            },
        )
        summary["outputs"]["equity_returns.json"] = str(fallback.resolve())

    summary["wrote"] = {
        "trailing_returns.json": True,
        "equity_returns.npy": wrote_npy,
    }

    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True, default=str))
    else:
        print("✅ Seeded allocator cache.")
        print(f"   trailing: {trailing_path}")
        if wrote_npy:
            print(f"   equity  : {equity_path}")
        else:
            print("   equity  : numpy not available; wrote equity_returns.json fallback")

    return


if __name__ == "__main__":
    main()
