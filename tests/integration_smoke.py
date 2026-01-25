# tests/integration_smoke.py
from __future__ import annotations

"""
tests/integration_smoke.py — V3+++ End-to-End DRYRUN Integration Smoke (LOCK-IN)
===============================================================================

Goal
----
Prove the *strategy wiring* can process a "new token" event + trade ticks
end-to-end WITHOUT touching real money, and without requiring network.

This smoke validates:
  - scripts.main imports cleanly under DRYRUN + offline guards
  - Strategy can be instantiated (signature-drift tolerant)
  - Strategy can accept a new-token event and trade ticks (name/signature tolerant)
  - Buy path is safely stubbed (NO RPC / signing / Jupiter / swaps)
  - Exit hooks exist and do not crash (stubbed)

Hard rules
----------
- Deterministic and bounded (no unbounded sleeps / loops).
- Must never execute real buys/sells (DRYRUN=1 + stubs).
- Must never require live network resources (socket connect blocked).
- Must be safe to run on Render or locally.

Run
---
pytest -q
python tests/integration_smoke.py

Exit codes (as script)
----------------------
0 = success
2 = failure
"""

import asyncio
import contextlib
import importlib
import inspect
import os
import sys
import time
import unittest
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any


# ──────────────────────────────────────────────────────────────────────────────
# Environment contract (set BEFORE importing scripts.main)
# ──────────────────────────────────────────────────────────────────────────────
def _setdefault_env(k: str, v: str) -> None:
    if os.getenv(k) is None:
        os.environ[k] = v


# Enforce dryrun/offline in every execution mode
_setdefault_env("DRYRUN", "1")
_setdefault_env("IS_DRYRUN", "1")
_setdefault_env("DISABLE_NETWORK", "1")
_setdefault_env("DISABLE_HTTP", "1")
_setdefault_env("DISABLE_RPC", "1")
_setdefault_env("SANDBOX_MODE", "1")

# Make gates permissive + fast for smoke (some modules read at import-time)
_setdefault_env("ENTRY_MOMENTUM_ENABLED", "1")
_setdefault_env("ENTRY_MOMO_MIN_TRADES", "1")
_setdefault_env("ENTRY_MOMO_MIN_UP_PCT", "0.0")
_setdefault_env("ENTRY_MOMO_ALLOW_NO_DATA_FOR_PUMP", "1")
_setdefault_env("ENTRY_MOMO_WINDOW_SEC", "1")
_setdefault_env("ENTRY_MOMO_PRICE_TRIES", "1")
_setdefault_env("ENTRY_MOMO_PRICE_DELAY_S", "0")

# Keep things quiet/stable
_setdefault_env("LOG_LEVEL", os.getenv("LOG_LEVEL", "WARNING"))
_setdefault_env("LOG_JSON", "0")
_setdefault_env("SMOKE_DEBUG", "0")


# ──────────────────────────────────────────────────────────────────────────────
# Repo root on sys.path
# ──────────────────────────────────────────────────────────────────────────────
def _repo_root() -> Path:
    here = Path(__file__).resolve()
    markers = ("pyproject.toml", "requirements.txt", "setup.cfg", ".git")
    for p in [here.parent, *list(here.parents)]:
        if (
            any((p / m).exists() for m in markers)
            or (p / "scripts").is_dir()
            or (p / "utils_pkg").is_dir()
        ):
            return p
    return here.parents[1] if len(here.parents) > 1 else here.parent


ROOT = _repo_root()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# ──────────────────────────────────────────────────────────────────────────────
# Offline guard: block outbound socket connect (best-effort)
# ──────────────────────────────────────────────────────────────────────────────
@contextlib.contextmanager
def _block_socket_connect():
    """
    Prevent accidental network use. We patch socket.socket.connect to raise.
    This is best-effort: it stops most HTTP/RPC mistakes without requiring
    deep patching across client libs.
    """
    import socket  # local import for patching

    orig_connect = socket.socket.connect

    def _deny_connect(self, address):  # type: ignore[no-untyped-def]
        raise RuntimeError(f"[smoke] Network disabled (attempted socket connect to {address!r})")

    socket.socket.connect = _deny_connect  # type: ignore[assignment]
    try:
        yield
    finally:
        socket.socket.connect = orig_connect  # type: ignore[assignment]


# ──────────────────────────────────────────────────────────────────────────────
# Utilities: async helpers + signature drift tolerant calling
# ──────────────────────────────────────────────────────────────────────────────
async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


def _call_best_effort(fn: Callable[..., Any], attempts: tuple[tuple[tuple, dict], ...]) -> Any:
    last_te: TypeError | None = None
    for args, kwargs in attempts:
        try:
            return fn(*args, **kwargs)
        except TypeError as te:
            last_te = te
            continue
    if last_te is not None:
        raise last_te
    raise AssertionError("No call attempts provided")


async def _call_async_best_effort(
    fn: Callable[..., Any], attempts: tuple[tuple[tuple, dict], ...]
) -> Any:
    return await _maybe_await(_call_best_effort(fn, attempts))


async def _call_method_best_effort(
    obj: Any,
    method_names: tuple[str, ...],
    *args: Any,
    **kwargs: Any,
) -> Any:
    """
    Try multiple method names and tolerate signature drift by attempting:
      1) args+kwargs
      2) kwargs-only
      3) positional-only
    """
    last_err: BaseException | None = None
    for name in method_names:
        fn = getattr(obj, name, None)
        if not callable(fn):
            continue

        # Attempt patterns
        for mode in ("args_kw", "kw", "args"):
            try:
                if mode == "args_kw":
                    return await _maybe_await(fn(*args, **kwargs))
                if mode == "kw":
                    return await _maybe_await(fn(**kwargs))
                return await _maybe_await(fn(*args))
            except TypeError as e:
                last_err = e
                continue
            except BaseException:
                # Real runtime error is meaningful; surface it.
                raise

    if last_err:
        raise last_err
    raise AttributeError(f"None of methods {method_names} exist on {type(obj).__name__}")


def _fixed_now_ms() -> int:
    # Deterministic timestamp for the smoke
    return 1_700_000_000_000


def _make_fake_event(*, mint: str, now_ms: int) -> dict[str, Any]:
    """
    Create a "new token" style payload with extra fields to tolerate schema drift.
    """
    return {
        # common scanner fields
        "mint": mint,
        "symbol": "TEST",
        "sym": "TEST",
        "name": "Integration Smoke Token",
        "pool": "pump",
        "source": "smoke",
        "created_at": now_ms,
        "ts": now_ms,
        "txType": "create",
        "initialBuy": 0.1,
        # optional fields some strategies expect
        "liq": 5000.0,
        "liq_usd": 5000.0,
        "vol5": 100.0,
        "buys5": 2,
        "sells5": 0,
        "chg5": 1.0,
        "age_min": 0.0,
    }


def _make_trade_tick(*, mint: str, now_ms: int, price: float) -> dict[str, Any]:
    return {
        "mint": mint,
        "ts": now_ms,
        "price": price,
        "side": "buy",
        "size": 1.0,
        # drift-friendly aliases
        "px": price,
        "price_ui": price,
    }


@dataclass
class BuySpy:
    calls: int = 0
    last: dict[str, Any] | None = None
    last_sig: str | None = None


class FakeExitEngine:
    """
    Exit engine stub. Provide common hooks so Strategy wiring won't crash.
    """

    async def on_trade_tick(self, *_a: Any, **_k: Any) -> None:
        return None

    async def on_buy_filled(self, *_a: Any, **_k: Any) -> None:
        return None

    async def schedule_after_buy(self, *_a: Any, **_k: Any) -> None:
        return None

    async def schedule_force_exit(self, *_a: Any, **_k: Any) -> None:
        return None


def _load_main_module():
    """
    Import (and reload) scripts.main after env defaults are set.
    We import under socket-connect block to enforce offline at import-time too.
    """
    with _block_socket_connect():
        import scripts.main as M  # type: ignore

        return importlib.reload(M)


def _patch_if_present(mod: Any, name: str, value: Any) -> Any | None:
    """
    Patch mod.<name> if present; return original, else None.
    """
    if hasattr(mod, name):
        orig = getattr(mod, name)
        setattr(mod, name, value)
        return orig
    return None


def _patch_many(mod: Any, mapping: dict[str, Any]) -> dict[str, Any]:
    """
    Patch a set of attributes; returns dict of originals (only for those patched).
    """
    originals: dict[str, Any] = {}
    for k, v in mapping.items():
        if hasattr(mod, k):
            originals[k] = getattr(mod, k)
            setattr(mod, k, v)
    return originals


# ──────────────────────────────────────────────────────────────────────────────
# The actual integration smoke test
# ──────────────────────────────────────────────────────────────────────────────
class IntegrationSmoke(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.M = _load_main_module()

        # Strongly enforce dryrun/offline at runtime too
        os.environ["DRYRUN"] = "1"
        os.environ["IS_DRYRUN"] = "1"
        os.environ["DISABLE_NETWORK"] = "1"

        # Deterministic time for logic that uses time.time()
        self._orig_time_time = time.time
        time.time = lambda: _fixed_now_ms() / 1000.0  # type: ignore[assignment]

        # Spy & stubs
        self.spy = BuySpy()
        self.fake_exit = FakeExitEngine()

        async def fake_screen_token(*args: Any, **kwargs: Any) -> tuple[bool, dict[str, Any]]:
            mint = None
            if args:
                mint = args[0]
            mint = kwargs.get("mint", mint)
            return True, {"ok": True, "mint": str(mint or ""), "decimals": 9, "priceImpactPct": 0.1}

        async def fake_maybe_buy(*args: Any, **kwargs: Any) -> str:
            self.spy.calls += 1
            self.spy.last = {"args": list(args), "kwargs": dict(kwargs)}
            sig = "FAKE_TX_SIG_123"
            self.spy.last_sig = sig
            return sig

        async def fake_swap_token_jupiter(*args: Any, **kwargs: Any) -> tuple[bool, str]:
            # In DRYRUN, swaps must never happen; still return a deterministic fail-safe
            self.spy.calls += 1
            self.spy.last = {
                "args": list(args),
                "kwargs": dict(kwargs),
                "path": "swap_token_jupiter",
            }
            return False, ""

        # Patch module-level hooks commonly used by scripts.main strategy wiring
        self._orig = {}
        self._orig.update(
            _patch_many(
                self.M,
                {
                    "screen_token": fake_screen_token,
                    "maybe_buy": fake_maybe_buy,
                    "exit_engine": self.fake_exit,
                },
            )
        )

        # Patch deeper common endpoints if present (avoid real trading calls)
        with contextlib.suppress(Exception):
            import utils_pkg.core_utils as cu  # type: ignore

            self._orig_cu = _patch_many(
                cu,
                {
                    "swap_token_jupiter": fake_swap_token_jupiter,
                },
            )
        with contextlib.suppress(Exception):
            import utils_pkg.router as router  # type: ignore

            # best-effort: prevent any router "swap" style calls if present
            async def _deny_router(*_a: Any, **_k: Any):
                raise RuntimeError("[smoke] router call blocked (offline/dryrun smoke)")

            self._orig_router = _patch_many(
                router,
                {
                    "swap": _deny_router,
                    "swap_jupiter": _deny_router,
                    "execute_swap": _deny_router,
                    "get_quote": _deny_router,
                    "get_buy_quote": _deny_router,
                    "get_sell_quote": _deny_router,
                },
            )

        # If module exposes these constants, make them fast for smoke
        with contextlib.suppress(Exception):
            if hasattr(self.M, "ENTRY_MOMO_WINDOW_SEC"):
                self.M.ENTRY_MOMO_WINDOW_SEC = 0.25
            if hasattr(self.M, "ENTRY_MOMO_PRICE_DELAY_S"):
                self.M.ENTRY_MOMO_PRICE_DELAY_S = 0
            if hasattr(self.M, "ENTRY_MOMO_PRICE_TRIES"):
                self.M.ENTRY_MOMO_PRICE_TRIES = 1

    async def asyncTearDown(self) -> None:
        # Restore time
        time.time = self._orig_time_time  # type: ignore[assignment]

        # Restore module patches
        for k, v in getattr(self, "_orig", {}).items():
            setattr(self.M, k, v)

        # Restore deep patches
        with contextlib.suppress(Exception):
            for k, v in getattr(self, "_orig_cu", {}).items():
                import utils_pkg.core_utils as cu  # type: ignore

                setattr(cu, k, v)
        with contextlib.suppress(Exception):
            for k, v in getattr(self, "_orig_router", {}).items():
                import utils_pkg.router as router  # type: ignore

                setattr(router, k, v)

        # Best-effort: cancel leftover tasks created during this test
        await asyncio.sleep(0)
        current = asyncio.current_task()
        for t in list(asyncio.all_tasks()):
            if t is current:
                continue
            if not t.done():
                t.cancel()
        await asyncio.sleep(0)

    async def test_end_to_end_dryrun_flow(self) -> None:
        # Import contract
        self.assertTrue(hasattr(self.M, "Strategy"), "scripts.main must export Strategy")
        Strat = self.M.Strategy

        # Instantiate strategy (signature drift tolerant)
        strat = _call_best_effort(
            Strat,
            attempts=(
                ((), {}),
                (({"dryrun": True},), {}),
                ((), {"dryrun": True}),
                ((), {"ctx": {"dryrun": True}}),
            ),
        )

        mint = "11111111111111111111111111111111"  # base58-ish, deterministic
        now_ms = _fixed_now_ms()

        raw_event = _make_fake_event(mint=mint, now_ms=now_ms)
        tick1 = _make_trade_tick(mint=mint, now_ms=now_ms + 10, price=0.000001)
        tick2 = _make_trade_tick(mint=mint, now_ms=now_ms + 20, price=0.0000012)

        # Strict offline: any attempted socket connect must fail
        with _block_socket_connect():
            # New-token event
            await _call_method_best_effort(
                strat,
                (
                    "on_new_token",
                    "on_token",
                    "handle_new_token",
                    "on_candidate",
                    "handle_candidate",
                    "ingest_candidate",
                ),
                raw_event,
                event=raw_event,
                candidate=raw_event,
                snap=raw_event,
            )

            # Trade ticks
            for tk in (tick1, tick2):
                await _call_method_best_effort(
                    strat,
                    ("on_trade", "on_trade_tick", "handle_trade", "on_tick", "ingest_trade"),
                    tk,
                    event=tk,
                    trade=tk,
                    tick=tk,
                    mint=mint,
                )

            # Let any internal momentum windows evaluate (bounded)
            window = float(getattr(self.M, "ENTRY_MOMO_WINDOW_SEC", 0.25) or 0.25)
            await asyncio.sleep(min(0.75, max(0.05, window + 0.1)))

        # If a buy triggers, it MUST be the fake signature
        if self.spy.calls > 0:
            self.assertEqual(self.spy.last_sig, "FAKE_TX_SIG_123")

        # Ensure DRYRUN still enforced
        self.assertEqual(os.getenv("DRYRUN"), "1")
        self.assertEqual(os.getenv("IS_DRYRUN"), "1")
        self.assertEqual(os.getenv("DISABLE_NETWORK"), "1")


# ──────────────────────────────────────────────────────────────────────────────
# Script entrypoint: exit code 0/2
# ──────────────────────────────────────────────────────────────────────────────
def _run_as_script() -> int:
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(IntegrationSmoke)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return 0 if result.wasSuccessful() else 2


if __name__ == "__main__":
    raise SystemExit(_run_as_script())
