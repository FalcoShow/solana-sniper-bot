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
import importlib
import os
import random
import sys
import time
from dataclasses import dataclass
from typing import Any

# ------------------------------ safety fences --------------------------------


def _env_true(v: str | None) -> bool:
    if v is None:
        return False
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


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


def _require_opt_in(args: argparse.Namespace) -> None:
    if not args.yes_i_am_sure:
        raise SystemExit("Refusing to run: pass --yes-i-am-sure (dev-only harness).")
    if not _env_true(os.getenv("ALLOW_SIM_MINTER")):
        raise SystemExit("Refusing to run: set ALLOW_SIM_MINTER=1 (dev-only harness).")

    if _looks_like_hosted_env() and not args.force_on_hosted:
        raise SystemExit(
            "Refusing to run in hosted/prod-like environment. "
            "If you truly intend to run here, pass --force-on-hosted (NOT recommended)."
        )


# ------------------------------ resolution utils -----------------------------


def _resolve_target(path: str) -> Any:
    """
    Resolve "module:attr" (class/function/object). Returns resolved object.
    """
    if ":" not in path:
        raise ValueError("strategy-target must be like 'module.path:AttrName'")
    mod_name, attr_name = path.split(":", 1)
    mod = importlib.import_module(mod_name)
    if not hasattr(mod, attr_name):
        raise AttributeError(f"{path} not found")
    return getattr(mod, attr_name)


def _maybe_instantiate(obj: Any) -> Any:
    """
    If obj is a class, instantiate it. If callable factory, call it. Otherwise return obj.
    """
    # Class-like: has __mro__ and callable
    if isinstance(obj, type):
        return obj()
    # Factory function that returns Strategy
    if callable(obj):
        try:
            return obj()
        except TypeError:
            # callable but requires args => return as-is and let caller handle
            return obj
    return obj


# ------------------------------ stubs / monkeypatch --------------------------


def _install_safety_stubs(seed: int, allow_network: bool, allow_non_dry: bool) -> list[str]:
    """
    Best-effort monkeypatching to prevent accidental swaps/quotes.
    Returns list of applied patches for reporting.
    """
    applied: list[str] = []

    # Force DRY_RUN unless explicitly allowed
    if not allow_non_dry:
        os.environ["DRY_RUN"] = "1"

    # Also set some common safety toggles (harmless if unused)
    os.environ.setdefault("PERPS_ENABLED", "false")
    os.environ.setdefault("JUPITER_ENABLED", "0")
    os.environ.setdefault("SWAPS_ENABLED", "0")
    os.environ.setdefault("LIVE_TRADING", "0")

    # Deterministic randomness
    random.seed(seed)

    if allow_network:
        return applied  # user explicitly allows network calls

    # Core stubs
    def _stub_quote(*_a: Any, **_k: Any) -> Any:
        # Quote returns "no quote" so callers should fail closed.
        return None

    async def _stub_quote_async(*_a: Any, **_k: Any) -> Any:
        return None

    async def _stub_swap_async(*_a: Any, **_k: Any) -> tuple[bool, str]:
        # Never hits network; returns a synthetic tx id.
        tag = f"SIM-SWAP-{random.randint(100000, 999999)}"
        return True, tag

    async def _stub_balance_async(*_a: Any, **_k: Any) -> float:
        # Provide a nonzero balance to let sizing logic proceed in dry-run paths.
        return float(os.getenv("SIM_SOL_BAL", "10.0"))

    # Attempt patches in common modules (best-effort; ignore failures)
    candidates = [
        (
            "utils_pkg.core_utils",
            {
                "swap_token_jupiter": _stub_swap_async,
                "get_token_price": _stub_quote_async,
                "prewarm_quotes": _stub_quote_async,
                "get_sol_balance": _stub_balance_async,
                "get_wallet_balance": _stub_balance_async,
            },
        ),
        (
            "utils_pkg.router",
            {
                "quote": _stub_quote_async,
                "get_quote": _stub_quote_async,
                "quote_jupiter": _stub_quote_async,
            },
        ),
        (
            "utils_pkg.jupiter",
            {
                "quote": _stub_quote_async,
                "get_quote": _stub_quote_async,
                "swap": _stub_swap_async,
            },
        ),
    ]

    for mod_name, mapping in candidates:
        try:
            mod = importlib.import_module(mod_name)
        except Exception:
            continue
        for attr, fn in mapping.items():
            if hasattr(mod, attr):
                try:
                    setattr(mod, attr, fn)  # type: ignore
                    applied.append(f"{mod_name}.{attr}")
                except Exception:
                    pass

    return applied


# ------------------------------ simulation model -----------------------------


def _now_ms() -> int:
    return int(time.time() * 1000)


def _fake_base58_like(n: int = 44) -> str:
    # Not guaranteed valid base58 pubkey; good enough as "looks like"
    alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    return "".join(random.choice(alphabet) for _ in range(n))


def _make_mint(i: int, mode: str) -> str:
    if mode == "fake":
        return f"SIM{i:03d}pump"
    if mode == "base58_like":
        return _fake_base58_like(44)
    raise ValueError(f"unknown mint mode: {mode}")


def _mk_token(i: int, mint_mode: str) -> dict[str, Any]:
    now = _now_ms()
    sym = f"SIM{i:03d}"
    return {
        "type": "create",
        "symbol": sym,
        "name": f"SimToken-{i}",
        "mint": _make_mint(i, mint_mode),
        "initialBuy": 10.0,
        "createdAt": now,
        "poolCreatedAt": now,  # satisfy age gates
    }


def _mk_trade_tick(mint: str) -> dict[str, Any]:
    return {"type": "trade", "mint": mint, "ts": _now_ms()}


@dataclass
class SimConfig:
    n: int = 3
    trades: int = 12
    pace: float = 0.5  # seconds between new_token emits
    trade_pace: float = 0.25  # seconds between trade rounds
    settle_sec: float = 0.0  # additional settle time after last trade
    seed: int = 1337
    mint_mode: str = "fake"  # fake | base58_like
    strategy_target: str = "scripts.main:Strategy"
    allow_network: bool = False
    allow_non_dry: bool = False
    verbose: bool = True


# ------------------------------ strategy adapter -----------------------------


class StrategyAdapter:
    """
    Wraps the Strategy to normalize method names and capture basic stats.
    """

    def __init__(self, strat: Any, verbose: bool = True):
        self.strat = strat
        self.verbose = verbose
        self.stats: dict[str, int] = {
            "new_token_sent": 0,
            "trade_tick_sent": 0,
            "errors": 0,
        }

        # Determine handlers
        self._on_new = getattr(strat, "on_new_token", None)
        self._on_trade = getattr(strat, "on_trade", None)

        if not callable(self._on_new) or not callable(self._on_trade):
            raise RuntimeError(
                "Strategy must implement async on_new_token(event) and on_trade(event). "
                f"Got on_new_token={bool(callable(self._on_new))}, on_trade={bool(callable(self._on_trade))}."
            )

    async def on_new_token(self, ev: dict[str, Any]) -> None:
        try:
            await self._on_new(ev)  # type: ignore[misc]
            self.stats["new_token_sent"] += 1
        except Exception as e:
            self.stats["errors"] += 1
            if self.verbose:
                print(f"[sim] on_new_token error: {e}")

    async def on_trade(self, ev: dict[str, Any]) -> None:
        try:
            await self._on_trade(ev)  # type: ignore[misc]
            self.stats["trade_tick_sent"] += 1
        except Exception as e:
            self.stats["errors"] += 1
            if self.verbose:
                print(f"[sim] on_trade error: {e}")


# ------------------------------ runner ---------------------------------------


async def run_sim(cfg: SimConfig) -> dict[str, Any]:
    patches = _install_safety_stubs(cfg.seed, cfg.allow_network, cfg.allow_non_dry)

    # Resolve Strategy
    target_obj = _resolve_target(cfg.strategy_target)
    strat_obj = _maybe_instantiate(target_obj)

    adapter = StrategyAdapter(strat_obj, verbose=cfg.verbose)

    tokens = [_mk_token(i, cfg.mint_mode) for i in range(1, cfg.n + 1)]

    if cfg.verbose:
        print("[sim] starting")
        print(f"[sim] target={cfg.strategy_target} n={cfg.n} trades={cfg.trades} seed={cfg.seed}")
        print(
            f"[sim] mint_mode={cfg.mint_mode} dry_run={os.getenv('DRY_RUN')} allow_network={cfg.allow_network}"
        )
        if patches:
            print(f"[sim] installed stubs: {', '.join(patches)}")

    # Emit create events
    for t in tokens:
        await adapter.on_new_token(t)
        await asyncio.sleep(max(0.0, cfg.pace))

    # Emit trade ticks across the window
    for _ in range(cfg.trades):
        for t in tokens:
            await adapter.on_trade(_mk_trade_tick(t["mint"]))
        await asyncio.sleep(max(0.0, cfg.trade_pace))

    # Let any windows/gates settle
    window = int(float(os.getenv("ENTRY_MOMO_WINDOW_SEC", "10")))
    settle = cfg.settle_sec if cfg.settle_sec > 0 else float(window + 2)
    await asyncio.sleep(max(0.0, settle))

    out = {
        "ok": True,
        "strategy_target": cfg.strategy_target,
        "n_tokens": cfg.n,
        "trades_per_token": cfg.trades,
        "mint_mode": cfg.mint_mode,
        "dry_run": os.getenv("DRY_RUN", ""),
        "allow_network": cfg.allow_network,
        "patched_symbols": patches,
        "stats": dict(adapter.stats),
        "tokens": [{"symbol": t["symbol"], "mint": t["mint"]} for t in tokens],
    }

    if cfg.verbose:
        print("[sim] done")
        print(f"[sim] stats={out['stats']}")

    return out


# ------------------------------ CLI ------------------------------------------


def _parse_args(argv: list[str]) -> SimConfig:
    ap = argparse.ArgumentParser(
        prog="sim_minter", description="DEV-ONLY simulator for Strategy on_new_token/on_trade."
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
        "--mint-mode", choices=("fake", "base58_like"), default="fake", help="mint id mode"
    )
    ap.add_argument(
        "--strategy-target", default="scripts.main:Strategy", help="module:Attr for Strategy"
    )
    ap.add_argument(
        "--allow-network", action="store_true", help="allow network calls (NOT recommended)"
    )
    ap.add_argument(
        "--allow-non-dry",
        action="store_true",
        help="do not force DRY_RUN=1 (still stubs swaps by default)",
    )
    ap.add_argument("--quiet", action="store_true", help="less output")
    ap.add_argument("--yes-i-am-sure", action="store_true", help="required opt-in")
    ap.add_argument(
        "--force-on-hosted",
        action="store_true",
        help="override hosted/prod fence (NOT recommended)",
    )
    args = ap.parse_args(argv)

    cfg = SimConfig(
        n=max(1, args.n),
        trades=max(0, args.trades),
        pace=max(0.0, args.pace),
        trade_pace=max(0.0, args.trade_pace),
        settle_sec=max(0.0, args.settle_sec),
        seed=args.seed,
        mint_mode=args.mint_mode,
        strategy_target=args.strategy_target,
        allow_network=bool(args.allow_network),
        allow_non_dry=bool(args.allow_non_dry),
        verbose=not bool(args.quiet),
    )

    # attach guard flags for _require_opt_in
    cfg._yes_i_am_sure = bool(args.yes_i_am_sure)  # type: ignore[attr-defined]
    cfg._force_on_hosted = bool(args.force_on_hosted)  # type: ignore[attr-defined]
    return cfg


def main(argv: list[str] | None = None) -> int:
    cfg = _parse_args(argv or sys.argv[1:])

    # map hidden fields back to args-like namespace for fences
    class _A:
        yes_i_am_sure = bool(getattr(cfg, "_yes_i_am_sure", False))
        force_on_hosted = bool(getattr(cfg, "_force_on_hosted", False))

    _require_opt_in(_A)

    # Helpful defaults for exercising momentum logic without trading
    os.environ.setdefault("ENTRY_MOMENTUM_ENABLED", "1")
    os.environ.setdefault("PF_DEBUG_TX", "1")
    os.environ.setdefault("ENTRY_MOMO_WINDOW_SEC", os.getenv("ENTRY_MOMO_WINDOW_SEC", "10"))

    asyncio.run(run_sim(cfg))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
