# tools/sim_minter.py
from __future__ import annotations

"""
DEV-ONLY (ARCHIVE) — sim_minter.py
=================================

Purpose
-------
Deterministic simulator that "emits" synthetic pump-style events into your Strategy
to exercise:
  - on_new_token(...)
  - on_trade(...)
  - momentum windows / counters
  - cooldowns
  - basic pipeline wiring

This is NOT a trading script.
It is a harness for logic + instrumentation.

Safety contract (hard fences)
-----------------------------
- Refuses to run unless BOTH are provided:
    (1) --yes-i-am-sure
    (2) ALLOW_SIM_MINTER=1
- Refuses to run in hosted/prod-like env unless --force-on-hosted
- Forces DRY_RUN=1 unless --allow-non-dry (still blocks swaps via stubs by default)
- Stubs out swap/quote calls when possible to avoid network / chain activity.

Usage (local)
-------------
set ALLOW_SIM_MINTER=1
python tools/sim_minter.py --yes-i-am-sure --n 5 --trades 20 --pace 0.4 --trade-pace 0.25

Target a Strategy elsewhere:
python tools/sim_minter.py --yes-i-am-sure --strategy-target "scripts.main:Strategy"

Notes
-----
- "mint" values are fake by default to discourage any real on-chain actions.
- If you want more realism, use --mint-mode base58_like (still fake).
"""

import argparse
import asyncio
import contextlib
import importlib
import importlib.abc
import importlib.machinery
import json
import os
import random
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from types import ModuleType
from typing import Any, Callable

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
_TRUE = {"1", "true", "t", "yes", "y", "on"}


def _env_true(v: str | None) -> bool:
    if v is None:
        return False
    return v.strip().lower() in _TRUE


def _looks_like_hosted_env() -> bool:
    hosted_keys = (
        "RENDER",
        "RENDER_SERVICE_ID",
        "RENDER_SERVICE_NAME",
        "DYNO",
        "K_SERVICE",
        "AWS_EXECUTION_ENV",
    )
    if any(os.getenv(k) for k in hosted_keys):
        return True
    return os.getenv("ENV", "").strip().lower() in ("prod", "production")


def _now_ms() -> int:
    return int(time.time() * 1000)


async def _maybe_await(x: Any) -> Any:
    if hasattr(x, "__await__"):
        return await x
    return x


def _safe_float_env(name: str, default: float) -> float:
    raw = os.getenv(name, "")
    if not raw:
        return default
    try:
        return float(raw)
    except Exception:
        return default


def _safe_int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "")
    if not raw:
        return default
    try:
        return int(float(raw))
    except Exception:
        return default


# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class SimConfig:
    n: int = 3
    trades: int = 12
    pace: float = 0.5  # seconds between new_token emits
    trade_pace: float = 0.25  # seconds between trade rounds
    settle_sec: float = 0.0  # additional settle time after last trade (0=auto)
    seed: int = 1337
    mint_mode: str = "fake"  # fake | base58_like
    strategy_target: str = "scripts.main:Strategy"
    allow_network: bool = False
    allow_non_dry: bool = False
    verbose: bool = True
    json_output: bool = False

    # Safety fences
    yes_i_am_sure: bool = False
    force_on_hosted: bool = False


def _require_opt_in(cfg: SimConfig) -> None:
    if not cfg.yes_i_am_sure:
        raise SystemExit("Refusing to run: pass --yes-i-am-sure (dev-only harness).")
    if not _env_true(os.getenv("ALLOW_SIM_MINTER")):
        raise SystemExit("Refusing to run: set ALLOW_SIM_MINTER=1 (dev-only harness).")

    if _looks_like_hosted_env() and not cfg.force_on_hosted:
        raise SystemExit(
            "Refusing to run in hosted/prod-like environment. "
            "If you truly intend to run here, pass --force-on-hosted (NOT recommended)."
        )


# ──────────────────────────────────────────────────────────────────────────────
# Target resolution
# ──────────────────────────────────────────────────────────────────────────────
def _resolve_target(path: str) -> Any:
    """
    Resolve "module.path:AttrName" (supports dotted attr path after colon).
    Examples:
      scripts.main:Strategy
      pkg.mod:factory.build
    """
    if ":" not in path:
        raise ValueError("strategy-target must be like 'module.path:AttrName'")
    mod_name, attr_path = path.split(":", 1)
    mod = importlib.import_module(mod_name)

    obj: Any = mod
    for part in attr_path.split("."):
        if not hasattr(obj, part):
            raise AttributeError(f"{path} not found (missing: {part})")
        obj = getattr(obj, part)
    return obj


def _maybe_instantiate(obj: Any) -> Any:
    """
    If obj is a class, instantiate it.
    If obj is a 0-arg factory, call it.
    Otherwise return as-is.
    """
    if isinstance(obj, type):
        return obj()
    if callable(obj):
        try:
            return obj()
        except TypeError:
            return obj
    return obj


# ──────────────────────────────────────────────────────────────────────────────
# Safety stubs / post-import patching
# ──────────────────────────────────────────────────────────────────────────────
_B58_ALPH = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def _b58_like(rng: random.Random, n: int = 44) -> str:
    return "".join(rng.choice(_B58_ALPH) for _ in range(n))


def _make_mint(rng: random.Random, i: int, mode: str) -> str:
    if mode == "fake":
        return f"SIM{i:03d}pump"
    if mode == "base58_like":
        return _b58_like(rng, 44)
    raise ValueError(f"unknown mint mode: {mode}")


def _mk_token(rng: random.Random, i: int, mint_mode: str) -> dict[str, Any]:
    now = _now_ms()
    sym = f"SIM{i:03d}"
    return {
        "type": "create",
        "symbol": sym,
        "name": f"SimToken-{i}",
        "mint": _make_mint(rng, i, mint_mode),
        "initialBuy": 10.0,
        "createdAt": now,
        "poolCreatedAt": now,  # satisfy age gates
    }


def _mk_trade_tick(mint: str) -> dict[str, Any]:
    return {"type": "trade", "mint": mint, "ts": _now_ms()}


PatchMap = dict[str, dict[str, Callable[..., Any]]]


class _PatchLoader(importlib.abc.Loader):
    def __init__(self, fullname: str, wrapped: importlib.abc.Loader, patch_map: PatchMap):
        self._fullname = fullname
        self._wrapped = wrapped
        self._patch_map = patch_map

    def create_module(self, spec: importlib.machinery.ModuleSpec) -> Any:  # pragma: no cover
        cm = getattr(self._wrapped, "create_module", None)
        if callable(cm):
            return cm(spec)
        return None

    def exec_module(self, module: Any) -> None:
        em = getattr(self._wrapped, "exec_module", None)
        if not callable(em):
            raise ImportError(f"loader for {self._fullname} has no exec_module")
        em(module)
        _apply_patches_to_module(self._fullname, module, self._patch_map)


class _PatchFinder(importlib.abc.MetaPathFinder):
    def __init__(self, patch_map: PatchMap):
        self._patch_map = patch_map

    # IMPORTANT: signature matches MetaPathFinderProtocol (mypy)
    def find_spec(
        self,
        fullname: str,
        path: Sequence[str] | None,
        target: ModuleType | None = None,
        /,
    ) -> importlib.machinery.ModuleSpec | None:
        if fullname not in self._patch_map:
            return None

        spec = importlib.machinery.PathFinder.find_spec(fullname, path, target)
        if spec is None or spec.loader is None:
            return spec

        # Wrap loader to patch after import without forcing imports ourselves.
        spec.loader = _PatchLoader(fullname, spec.loader, self._patch_map)
        return spec


def _apply_patches_to_module(mod_name: str, module: Any, patch_map: PatchMap) -> list[str]:
    applied: list[str] = []
    mapping = patch_map.get(mod_name, {})
    for attr, fn in mapping.items():
        if hasattr(module, attr):
            with contextlib.suppress(Exception):
                setattr(module, attr, fn)
                applied.append(f"{mod_name}.{attr}")
    return applied


def _install_post_import_patcher(patch_map: PatchMap) -> None:
    # Avoid installing duplicate finders.
    for f in sys.meta_path:
        if isinstance(f, _PatchFinder):
            return
    sys.meta_path.insert(0, _PatchFinder(patch_map))


def _install_safety_stubs(cfg: SimConfig, rng: random.Random) -> list[str]:
    """
    Best-effort monkeypatching to prevent accidental swaps/quotes.
    Returns list of applied patches for reporting.
    """
    applied: list[str] = []

    # Force DRY_RUN unless explicitly allowed
    if not cfg.allow_non_dry:
        os.environ["DRY_RUN"] = "1"

    # Common safety toggles (harmless if unused)
    os.environ.setdefault("PERPS_ENABLED", "false")
    os.environ.setdefault("JUPITER_ENABLED", "0")
    os.environ.setdefault("SWAPS_ENABLED", "0")
    os.environ.setdefault("LIVE_TRADING", "0")

    if cfg.allow_network:
        return applied

    # Stubs
    async def _stub_quote_async(*_a: Any, **_k: Any) -> Any:
        return None

    def _stub_quote_sync(*_a: Any, **_k: Any) -> Any:
        return None

    async def _stub_swap_async(*_a: Any, **_k: Any) -> tuple[bool, str]:
        tag = f"SIM-SWAP-{rng.randint(100000, 999999)}"
        return True, tag

    async def _stub_balance_async(*_a: Any, **_k: Any) -> float:
        return _safe_float_env("SIM_SOL_BAL", 10.0)

    # Patch plan: patch when/if these modules load (and patch already-loaded too)
    patch_map: PatchMap = {
        "utils_pkg.core_utils": {
            "swap_token_jupiter": _stub_swap_async,
            "get_token_price": _stub_quote_async,
            "prewarm_quotes": _stub_quote_async,
            "get_sol_balance": _stub_balance_async,
            "get_wallet_balance": _stub_balance_async,
        },
        "utils_pkg.router": {
            "quote": _stub_quote_async,
            "get_quote": _stub_quote_async,
            "quote_jupiter": _stub_quote_async,
        },
        "utils_pkg.jupiter": {
            "quote": _stub_quote_async,
            "get_quote": _stub_quote_async,
            "swap": _stub_swap_async,
        },
        # Optional broader names some repos use:
        "utils_pkg.swap": {
            "quote": _stub_quote_async,
            "swap": _stub_swap_async,
        },
        "utils_pkg.quotes": {
            "quote": _stub_quote_async,
            "get_quote": _stub_quote_async,
        },
        # Requests-like or sync fallbacks (best-effort)
        "utils_pkg.http": {
            "get": _stub_quote_sync,
            "post": _stub_quote_sync,
        },
    }

    _install_post_import_patcher(patch_map)

    # Patch anything already imported (without importing new modules)
    for mod_name in list(patch_map.keys()):
        mod = sys.modules.get(mod_name)
        if mod is not None:
            applied.extend(_apply_patches_to_module(mod_name, mod, patch_map))

    return applied


# ──────────────────────────────────────────────────────────────────────────────
# Strategy adapter
# ──────────────────────────────────────────────────────────────────────────────
class StrategyAdapter:
    """
    Wraps the Strategy to normalize method names and capture basic stats.
    Accepts sync or async handlers; awaits if needed.
    """

    def __init__(self, strat: Any, *, verbose: bool = True):
        self.strat = strat
        self.verbose = verbose
        self.stats: dict[str, int] = {"new_token_sent": 0, "trade_tick_sent": 0, "errors": 0}

        self._on_new = getattr(strat, "on_new_token", None)
        self._on_trade = getattr(strat, "on_trade", None)

        if not callable(self._on_new) or not callable(self._on_trade):
            raise RuntimeError(
                "Strategy must implement on_new_token(event) and on_trade(event). "
                f"Got on_new_token={bool(callable(self._on_new))}, on_trade={bool(callable(self._on_trade))}."
            )

    async def on_new_token(self, ev: dict[str, Any]) -> None:
        try:
            await _maybe_await(self._on_new(ev))  # type: ignore[misc]
            self.stats["new_token_sent"] += 1
        except Exception as e:
            self.stats["errors"] += 1
            if self.verbose:
                print(f"[sim] on_new_token error: {type(e).__name__}: {e}")

    async def on_trade(self, ev: dict[str, Any]) -> None:
        try:
            await _maybe_await(self._on_trade(ev))  # type: ignore[misc]
            self.stats["trade_tick_sent"] += 1
        except Exception as e:
            self.stats["errors"] += 1
            if self.verbose:
                print(f"[sim] on_trade error: {type(e).__name__}: {e}")


# ──────────────────────────────────────────────────────────────────────────────
# Runner
# ──────────────────────────────────────────────────────────────────────────────
async def run_sim(cfg: SimConfig) -> dict[str, Any]:
    rng = random.Random(cfg.seed)

    patches = _install_safety_stubs(cfg, rng)

    # Resolve Strategy (after stubs and env safety toggles)
    target_obj = _resolve_target(cfg.strategy_target)
    strat_obj = _maybe_instantiate(target_obj)

    adapter = StrategyAdapter(strat_obj, verbose=cfg.verbose)

    tokens = [_mk_token(rng, i, cfg.mint_mode) for i in range(1, cfg.n + 1)]

    if cfg.verbose:
        print("[sim] starting")
        print(
            f"[sim] target={cfg.strategy_target} n={cfg.n} trades={cfg.trades} seed={cfg.seed} mint_mode={cfg.mint_mode}"
        )
        print(
            f"[sim] dry_run={os.getenv('DRY_RUN')} allow_network={cfg.allow_network} allow_non_dry={cfg.allow_non_dry}"
        )
        if patches:
            print(f"[sim] installed stubs: {', '.join(patches)}")

    # Emit create events
    for t in tokens:
        await adapter.on_new_token(t)
        await asyncio.sleep(max(0.0, cfg.pace))

    # Emit trade ticks across the window
    for _ in range(max(0, cfg.trades)):
        for t in tokens:
            await adapter.on_trade(_mk_trade_tick(str(t["mint"])))
        await asyncio.sleep(max(0.0, cfg.trade_pace))

    # Let any windows/gates settle
    window = _safe_int_env("ENTRY_MOMO_WINDOW_SEC", 10)
    settle = cfg.settle_sec if cfg.settle_sec > 0 else float(window + 2)
    await asyncio.sleep(max(0.0, settle))

    out: dict[str, Any] = {
        "ok": True,
        "strategy_target": cfg.strategy_target,
        "n_tokens": cfg.n,
        "trades_per_token": cfg.trades,
        "mint_mode": cfg.mint_mode,
        "dry_run": os.getenv("DRY_RUN", ""),
        "allow_network": cfg.allow_network,
        "patched_symbols": patches,
        "stats": dict(adapter.stats),
        "tokens": [{"symbol": t.get("symbol"), "mint": t.get("mint")} for t in tokens],
    }

    if cfg.verbose:
        print("[sim] done")
        print(f"[sim] stats={out['stats']}")

    return out


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────
def _parse_args(argv: list[str]) -> SimConfig:
    ap = argparse.ArgumentParser(
        prog="sim_minter",
        description="DEV-ONLY simulator for Strategy on_new_token/on_trade.",
    )
    ap.add_argument("--n", type=int, default=3, help="number of simulated tokens")
    ap.add_argument("--trades", type=int, default=12, help="trade ticks per token over the window")
    ap.add_argument("--pace", type=float, default=0.5, help="seconds between new_token emits")
    ap.add_argument("--trade-pace", type=float, default=0.25, help="seconds between trade rounds")
    ap.add_argument(
        "--settle-sec",
        type=float,
        default=0.0,
        help="override settle time after last trade (0=auto)",
    )
    ap.add_argument("--seed", type=int, default=1337, help="rng seed for determinism")
    ap.add_argument(
        "--mint-mode",
        choices=("fake", "base58_like"),
        default="fake",
        help="mint id mode",
    )
    ap.add_argument(
        "--strategy-target",
        default="scripts.main:Strategy",
        help="module:Attr for Strategy",
    )
    ap.add_argument(
        "--allow-network",
        action="store_true",
        help="allow network calls (NOT recommended)",
    )
    ap.add_argument(
        "--allow-non-dry",
        action="store_true",
        help="do not force DRY_RUN=1 (still stubs swaps/quotes when possible)",
    )
    ap.add_argument("--quiet", action="store_true", help="less output")
    ap.add_argument("--json", action="store_true", help="print JSON summary to stdout")
    ap.add_argument("--yes-i-am-sure", action="store_true", help="required opt-in")
    ap.add_argument(
        "--force-on-hosted",
        action="store_true",
        help="override hosted/prod fence (NOT recommended)",
    )

    args = ap.parse_args(argv)

    return SimConfig(
        n=max(1, int(args.n)),
        trades=max(0, int(args.trades)),
        pace=max(0.0, float(args.pace)),
        trade_pace=max(0.0, float(args.trade_pace)),
        settle_sec=max(0.0, float(args.settle_sec)),
        seed=int(args.seed),
        mint_mode=str(args.mint_mode),
        strategy_target=str(args.strategy_target),
        allow_network=bool(args.allow_network),
        allow_non_dry=bool(args.allow_non_dry),
        verbose=not bool(args.quiet),
        json_output=bool(args.json),
        yes_i_am_sure=bool(args.yes_i_am_sure),
        force_on_hosted=bool(args.force_on_hosted),
    )


def main(argv: list[str] | None = None) -> int:
    cfg = _parse_args(argv or sys.argv[1:])
    _require_opt_in(cfg)

    # Helpful defaults for exercising momentum logic without trading
    os.environ.setdefault("ENTRY_MOMENTUM_ENABLED", "1")
    os.environ.setdefault("PF_DEBUG_TX", "1")
    os.environ.setdefault("ENTRY_MOMO_WINDOW_SEC", os.getenv("ENTRY_MOMO_WINDOW_SEC", "10"))

    out = asyncio.run(run_sim(cfg))

    if cfg.json_output:
        print(json.dumps(out, indent=2, sort_keys=True))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
