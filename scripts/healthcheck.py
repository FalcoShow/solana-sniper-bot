# scripts/healthcheck.py
from __future__ import annotations

"""
Quant-grade, tunable healthcheck for Render / infra (V3+++).

Probes (all async, independent, and tagged as fatal / non-fatal):

  1) RPC failover (FATAL, always on)
       - utils_pkg.rpc_failover.apply_failover_to_env()
       - chooses a working RPC_URL or fails fast

  2) core_utils bootstrap (FATAL, always on)
       - init_resources() / close_resources()

  3) Redis (FATAL unless skipped)
       - ping via get_redis(), set/get test key

  4) Solana + wallet (FATAL unless skipped)
       - get_latest_blockhash()
       - wallet pubkey + SOL balance

  5) Postgres / AI DB (FATAL unless skipped)
       - ai_db.describe_backend()
       - ai_db.ping()

  6) Price infra (NON-FATAL, optional)
       - convert_sol_to_usd(1.0)
       - get_token_price(wSOL)

  7) Risk / regime snapshot (NON-FATAL, optional)
       - risk_manager.risk_snapshot()
       - regime_state.features()

  8) Jupiter quote (NON-FATAL, optional)
       - direct quote via utils_pkg.lightning_impl / Jupiter API

  9) AI stack / strategy brain (NON-FATAL by default, can be fatal)
       - utils_pkg.ai_entry_logic / ai_buy_scoring / self_learning_ai_strategy
       - import + optional describe_* style metadata

Exit code:
  - 0 if all *non-skipped fatal* probes pass.
  - 1 if any fatal probe fails.

Env flags (all optional):
  - HEALTHCHECK_SKIP_REDIS=1        → skip Redis probe
  - HEALTHCHECK_SKIP_SOLANA=1       → skip Solana RPC + wallet probe
  - HEALTHCHECK_SKIP_DB=1           → skip Postgres / ai_db probe
  - HEALTHCHECK_SKIP_RISK=1         → skip risk/regime probe
  - HEALTHCHECK_SKIP_AI=1           → skip AI stack probe
  - HEALTHCHECK_AI_FATAL=1          → treat AI stack probe as FATAL

  - HEALTHCHECK_INCLUDE_PRICE=1     → enable price infra probe
  - HEALTHCHECK_INCLUDE_JUP=1       → enable Jupiter quote probe

  - HEALTHCHECK_JSON=1              → print final JSON summary (plus human logs)
  - HEALTHCHECK_JSON_ONLY=1         → only print JSON summary (no human logs)

Notes:
  • This script is meant to be used as a Render health check (command).
  • All probes are written to be defensive and never raise at the top level.
"""

import asyncio
import json
import os
import sys
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional, Awaitable, Callable

from dotenv import load_dotenv, find_dotenv

# Load .env early so SNIPER_DB_DSN / REDIS_URL / RPC_URL, etc. are present locally
load_dotenv(find_dotenv())

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from logging_setup import get_logger  # type: ignore[import]
from utils_pkg.rpc_failover import apply_failover_to_env
from utils_pkg.core_utils import (
    init_resources,
    close_resources,
    get_redis,
    get_solana_client,
    get_wallet_pubkey,
    get_sol_balance,
    convert_sol_to_usd,
    get_token_price,
)
from utils_pkg import ai_db  # Postgres / AI DB health

# Optional imports (risk / regime / jup / AI stack)
try:  # pragma: no cover - optional wiring
    from utils_pkg import risk_manager  # type: ignore[import]
    from utils_pkg.regime import regime_state  # type: ignore[import]
except Exception:  # pragma: no cover
    risk_manager = None  # type: ignore[assignment]
    regime_state = None  # type: ignore[assignment]

try:  # pragma: no cover
    from utils_pkg import lightning_impl as L
except Exception:  # pragma: no cover
    L = None  # type: ignore[assignment]

# AI stack (all optional / best-effort)
try:  # pragma: no cover
    from utils_pkg import ai_entry_logic  # type: ignore[import]
except Exception:  # pragma: no cover
    ai_entry_logic = None  # type: ignore[assignment]

try:  # pragma: no cover
    from utils_pkg import ai_buy_scoring  # type: ignore[import]
except Exception:  # pragma: no cover
    ai_buy_scoring = None  # type: ignore[assignment]

try:  # pragma: no cover
    from utils_pkg import self_learning_ai_strategy  # type: ignore[import]
except Exception:  # pragma: no cover
    self_learning_ai_strategy = None  # type: ignore[assignment]

log = get_logger(__name__)
WSOL_MINT = "So11111111111111111111111111111111111111112"


# ════════════════════════════════════════════════════════
# Helpers / config
# ════════════════════════════════════════════════════════


def _b(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass
class ProbeResult:
    name: str
    ok: bool
    fatal: bool
    error: Optional[str] = None
    details: Dict[str, Any] | None = None
    duration_sec: float | None = None


@dataclass
class HealthSummary:
    ok: bool
    fatal_failures: int
    nonfatal_failures: int
    probes: Dict[str, ProbeResult]
    ts: float
    duration_sec: float


def _probe_dict(pr: ProbeResult) -> Dict[str, Any]:
    d = asdict(pr)
    # Convert any non-serializable detail values
    if d.get("details") is not None:
        safe_details: Dict[str, Any] = {}
        for k, v in (d["details"] or {}).items():
            try:
                json.dumps(v, default=str)
                safe_details[k] = v
            except Exception:
                safe_details[k] = str(v)
        d["details"] = safe_details
    return d


async def _run_probe(
    *,
    name: str,
    fatal: bool,
    func: Callable[[], Awaitable[Dict[str, Any]]],
    json_only: bool,
) -> ProbeResult:
    """
    Run a single probe coroutine, capture errors, and return ProbeResult.
    """
    if not json_only:
        print(f"\n=== [{name}] ===")
    t0 = time.time()
    try:
        details = await func()
        dt = time.time() - t0
        if not json_only:
            print(f"[{name}] OK in {dt:.3f}s ✅")
        return ProbeResult(
            name=name,
            ok=True,
            fatal=fatal,
            details=details or {},
            duration_sec=dt,
        )
    except Exception as e:  # noqa: BLE001
        dt = time.time() - t0
        msg = f"{type(e).__name__}: {e}"
        if not json_only:
            print(f"[{name}] FAILED in {dt:.3f}s → {msg}")
        log.exception("Health probe '%s' failed", name)
        return ProbeResult(
            name=name,
            ok=False,
            fatal=fatal,
            error=msg,
            details={},
            duration_sec=dt,
        )


# ════════════════════════════════════════════════════════
# Individual probes
# ════════════════════════════════════════════════════════


async def probe_rpc_failover() -> Dict[str, Any]:
    chosen, notes = await apply_failover_to_env(timeout=4.0)
    return {
        "chosen_rpc": chosen,
        "candidates": [
            {"url": url, "ok": ok, "why": why} for (url, ok, why) in notes
        ],
    }


async def probe_core_utils() -> Dict[str, Any]:
    await init_resources()
    # Nothing else to test here; Redis/solana are separate probes.
    return {"init_resources": "ok"}


async def probe_redis() -> Dict[str, Any]:
    r = get_redis()
    if not r:
        raise RuntimeError("redis client not initialized (get_redis() returned None)")
    key = "hc:ping"
    ts = str(time.time())
    await r.set(key, ts, ex=30)
    got = await r.get(key)
    if not got:
        raise RuntimeError("redis get returned empty/None")
    if isinstance(got, bytes):
        got = got.decode()
    if got != ts:
        raise RuntimeError(f"redis echo mismatch: expected {ts}, got {got}")
    return {"key": key, "echo": got}


async def probe_solana_and_wallet() -> Dict[str, Any]:
    client = get_solana_client()
    if not client:
        raise RuntimeError("solana client not initialized (get_solana_client() is None)")

    # Latest blockhash
    res = await client.get_latest_blockhash()
    blockhash = None
    try:
        blockhash = getattr(getattr(res, "value", None), "blockhash", None)
    except Exception:
        pass
    if not blockhash:
        try:
            blockhash = res["result"]["value"]["blockhash"]
        except Exception:
            pass
    if not blockhash:
        raise RuntimeError("failed to fetch latest blockhash from Solana RPC")

    # Wallet balance
    pub = get_wallet_pubkey()
    if not pub:
        raise RuntimeError("wallet pubkey not available (WALLET_PUBKEY/ADDRESS unset?)")
    bal = await get_sol_balance(pub)

    return {
        "blockhash": blockhash,
        "wallet_pubkey": str(pub),
        "wallet_sol_balance": float(bal),
    }


async def probe_ai_db() -> Dict[str, Any]:
    details: Dict[str, Any] = {}
    try:
        if hasattr(ai_db, "describe_backend"):
            details["backend"] = ai_db.describe_backend()
    except Exception as e:  # noqa: BLE001
        details["backend_error"] = str(e)

    if not hasattr(ai_db, "ping"):
        raise RuntimeError("ai_db.ping() not available")
    ok = ai_db.ping()
    details["ping"] = bool(ok)
    if not ok:
        raise RuntimeError("ai_db.ping() returned False")
    return details


async def probe_price_infra() -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    usd = await convert_sol_to_usd(1.0)
    if not usd or usd <= 0:
        raise RuntimeError("convert_sol_to_usd(1.0) returned non-positive")
    out["sol_usd"] = float(usd)

    wsol_price = await get_token_price(WSOL_MINT)
    if wsol_price is None or wsol_price <= 0:
        raise RuntimeError("get_token_price(wSOL) returned non-positive/None")
    out["wsol_price_in_sol"] = float(wsol_price)
    return out


async def probe_risk_and_regime() -> Dict[str, Any]:
    if risk_manager is None or regime_state is None:
        raise RuntimeError("risk_manager / regime_state not importable")

    snap = await risk_manager.risk_snapshot()  # type: ignore[func-returns-value]
    feats = await regime_state.features()  # type: ignore[func-returns-value]

    return {
        "equity_usd": float(snap.get("equity_usd", 0.0)),
        "ticket_usd": float(snap.get("ticket_usd", 0.0)),
        "max_concurrent": int(snap.get("max_concurrent", 0)),
        "risk_capacity": float(snap.get("risk_capacity", 0.0)),
        "risk_mode": int(snap.get("risk_mode", 1)),
        "regime_label": feats.get("regime_label"),
        "regime_risk_multiplier": feats.get("regime_risk_multiplier"),
    }


async def probe_jupiter_quote() -> Dict[str, Any]:
    if L is None:
        raise RuntimeError("utils_pkg.lightning_impl not importable")
    import httpx  # local import to keep base module light

    base = getattr(L, "BASE", os.getenv("JUP_BASE_URL", "https://api.jup.ag"))
    url = base.rstrip("/") + "/swap/v1/quote"
    params = {
        "inputMint": WSOL_MINT,
        "outputMint": WSOL_MINT,
        "amount": 1_000_000,  # 0.001 SOL
        "slippageBps": 50,
        "swapMode": "ExactIn",
        "onlyDirectRoutes": "false",
        "asLegacyTransaction": "false",
    }
    timeout = float(os.getenv("HEALTHCHECK_JUP_TIMEOUT", "6"))
    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.get(url, params=params, headers=L._jup_headers())  # type: ignore[attr-defined]
        if r.status_code != 200:
            raise RuntimeError(f"Jupiter quote status={r.status_code}, body={r.text[:200]!r}")
        data = r.json()
        return {
            "base": base,
            "status": r.status_code,
            "keys": list(data.keys())[:6],
        }


async def probe_ai_stack() -> Dict[str, Any]:
    """
    AI stack health:

      - Imports ai_entry_logic, ai_buy_scoring, self_learning_ai_strategy if present.
      - Gathers describe_* style metadata when available.
      - Does NOT call OpenAI / network; purely local introspection.

    This is non-fatal by default (can be made fatal via HEALTHCHECK_AI_FATAL=1).
    """
    details: Dict[str, Any] = {}

    # ai_entry_logic
    entry_info: Dict[str, Any] = {}
    if ai_entry_logic is None:
        entry_info["imported"] = False
        entry_info["error"] = "ai_entry_logic not importable"
    else:
        entry_info["imported"] = True
        try:
            describe = getattr(ai_entry_logic, "describe_entry_backend", None)
            if callable(describe):
                entry_info["describe"] = describe()
        except Exception as e:  # noqa: BLE001
            entry_info["describe_error"] = str(e)
    details["ai_entry_logic"] = entry_info

    # ai_buy_scoring
    buy_info: Dict[str, Any] = {}
    if ai_buy_scoring is None:
        buy_info["imported"] = False
        buy_info["error"] = "ai_buy_scoring not importable"
    else:
        buy_info["imported"] = True
        try:
            describe = getattr(ai_buy_scoring, "describe_scorer", None)
            if callable(describe):
                buy_info["describe"] = describe()
        except Exception as e:  # noqa: BLE001
            buy_info["describe_error"] = str(e)
    details["ai_buy_scoring"] = buy_info

    # self_learning_ai_strategy
    sl_info: Dict[str, Any] = {}
    if self_learning_ai_strategy is None:
        sl_info["imported"] = False
        sl_info["error"] = "self_learning_ai_strategy not importable"
    else:
        sl_info["imported"] = True
        try:
            describe = getattr(self_learning_ai_strategy, "describe_strategy_backend", None)
            if callable(describe):
                sl_info["describe"] = describe()
        except Exception as e:  # noqa: BLE001
            sl_info["describe_error"] = str(e)
    details["self_learning_ai_strategy"] = sl_info

    # Decide whether this is considered OK:
    # We require at least ai_entry_logic import for "OK".
    if not entry_info.get("imported"):
        raise RuntimeError("ai_entry_logic core module not importable")

    return details


# ════════════════════════════════════════════════════════
# Main / orchestration
# ════════════════════════════════════════════════════════


async def main() -> None:
    t_start = time.time()

    include_price = _b("HEALTHCHECK_INCLUDE_PRICE", False)
    skip_redis = _b("HEALTHCHECK_SKIP_REDIS", False)
    skip_solana = _b("HEALTHCHECK_SKIP_SOLANA", False)
    skip_db = _b("HEALTHCHECK_SKIP_DB", False)
    skip_risk = _b("HEALTHCHECK_SKIP_RISK", False)
    skip_ai = _b("HEALTHCHECK_SKIP_AI", False)
    include_jup = _b("HEALTHCHECK_INCLUDE_JUP", False)

    ai_fatal = _b("HEALTHCHECK_AI_FATAL", False)

    json_mode = _b("HEALTHCHECK_JSON", False) or _b("HEALTHCHECK_JSON_ONLY", False)
    json_only = _b("HEALTHCHECK_JSON_ONLY", False)

    probes: Dict[str, ProbeResult] = {}

    # 1) RPC failover (fatal, always on)
    probes["rpc_failover"] = await _run_probe(
        name="RPC_FAILOVER",
        fatal=True,
        func=probe_rpc_failover,
        json_only=json_only,
    )

    # 2) core_utils init_resources (fatal, always on)
    probes["core_utils"] = await _run_probe(
        name="CORE_UTILS_INIT",
        fatal=True,
        func=probe_core_utils,
        json_only=json_only,
    )

    # 3) Redis (fatal unless skipped)
    if skip_redis:
        probes["redis"] = ProbeResult(
            name="REDIS",
            ok=True,
            fatal=False,
            error=None,
            details={"skipped": True},
            duration_sec=0.0,
        )
        if not json_only:
            print("\n=== [REDIS] ===")
            print("Skipping Redis probe (HEALTHCHECK_SKIP_REDIS=1)")
    else:
        probes["redis"] = await _run_probe(
            name="REDIS",
            fatal=True,
            func=probe_redis,
            json_only=json_only,
        )

    # 4) Solana + wallet (fatal unless skipped)
    if skip_solana:
        probes["solana"] = ProbeResult(
            name="SOLANA",
            ok=True,
            fatal=False,
            error=None,
            details={"skipped": True},
            duration_sec=0.0,
        )
        if not json_only:
            print("\n=== [SOLANA] ===")
            print("Skipping Solana RPC + wallet probe (HEALTHCHECK_SKIP_SOLANA=1)")
    else:
        probes["solana"] = await _run_probe(
            name="SOLANA_RPC_WALLET",
            fatal=True,
            func=probe_solana_and_wallet,
            json_only=json_only,
        )

    # 5) AI DB / Postgres (fatal unless skipped)
    if skip_db:
        probes["ai_db"] = ProbeResult(
            name="AI_DB",
            ok=True,
            fatal=False,
            error=None,
            details={"skipped": True},
            duration_sec=0.0,
        )
        if not json_only:
            print("\n=== [AI_DB] ===")
            print("Skipping DB probe (HEALTHCHECK_SKIP_DB=1)")
    else:
        probes["ai_db"] = await _run_probe(
            name="AI_DB",
            fatal=True,
            func=probe_ai_db,
            json_only=json_only,
        )

    # 6) Price infra (non-fatal, optional)
    if include_price:
        probes["price"] = await _run_probe(
            name="PRICE_INFRA",
            fatal=False,
            func=probe_price_infra,
            json_only=json_only,
        )
    else:
        probes["price"] = ProbeResult(
            name="PRICE_INFRA",
            ok=True,
            fatal=False,
            error=None,
            details={"skipped": True},
            duration_sec=0.0,
        )
        if not json_only:
            print("\n=== [PRICE_INFRA] ===")
            print("Skipping price probes (HEALTHCHECK_INCLUDE_PRICE=0)")

    # 7) Risk / regime (non-fatal, optional)
    if skip_risk:
        probes["risk"] = ProbeResult(
            name="RISK_REGIME",
            ok=True,
            fatal=False,
            error=None,
            details={"skipped": True},
            duration_sec=0.0,
        )
        if not json_only:
            print("\n=== [RISK_REGIME] ===")
            print("Skipping risk/regime probe (HEALTHCHECK_SKIP_RISK=1)")
    else:
        probes["risk"] = await _run_probe(
            name="RISK_REGIME",
            fatal=False,
            func=probe_risk_and_regime,
            json_only=json_only,
        )

    # 8) Jupiter quote (non-fatal, optional)
    if include_jup:
        probes["jupiter"] = await _run_probe(
            name="JUPITER_QUOTE",
            fatal=False,
            func=probe_jupiter_quote,
            json_only=json_only,
        )
    else:
        probes["jupiter"] = ProbeResult(
            name="JUPITER_QUOTE",
            ok=True,
            fatal=False,
            error=None,
            details={"skipped": True},
            duration_sec=0.0,
        )
        if not json_only:
            print("\n=== [JUPITER_QUOTE] ===")
            print("Skipping Jupiter quote probe (HEALTHCHECK_INCLUDE_JUP=0)")

    # 9) AI stack / strategy brain (non-fatal by default, but can be fatal)
    if skip_ai:
        probes["ai_stack"] = ProbeResult(
            name="AI_STACK",
            ok=True,
            fatal=False,
            error=None,
            details={"skipped": True},
            duration_sec=0.0,
        )
        if not json_only:
            print("\n=== [AI_STACK] ===")
            print("Skipping AI stack probe (HEALTHCHECK_SKIP_AI=1)")
    else:
        probes["ai_stack"] = await _run_probe(
            name="AI_STACK",
            fatal=bool(ai_fatal),
            func=probe_ai_stack,
            json_only=json_only,
        )

    # Always attempt to close resources at the end
    try:
        await close_resources()
    except Exception:  # noqa: BLE001
        log.exception("close_resources() failed during healthcheck shutdown")

    # Build final summary
    fatal_failures = sum(
        1 for p in probes.values() if p.fatal and not p.ok
    )
    nonfatal_failures = sum(
        1 for p in probes.values() if not p.fatal and not p.ok
    )
    ok = fatal_failures == 0
    duration = time.time() - t_start

    summary = HealthSummary(
        ok=ok,
        fatal_failures=fatal_failures,
        nonfatal_failures=nonfatal_failures,
        probes=probes,
        ts=time.time(),
        duration_sec=duration,
    )

    # Human summary
    if not json_only:
        print("\n==================== HEALTH SUMMARY ====================")
        for name, p in probes.items():
            status = "OK" if p.ok else ("FAIL" if p.fatal else "WARN")
            extra = ""
            if p.details and p.details.get("skipped"):
                extra = " (skipped)"
            elif p.error:
                extra = f" – {p.error}"
            dur = f" [{p.duration_sec:.3f}s]" if p.duration_sec is not None else ""
            print(f"[{status}] {name}{dur}{extra}")
        print(
            f"\nResult: {'OK' if ok else 'FAIL'} "
            f"(fatal_failures={fatal_failures}, nonfatal_failures={nonfatal_failures})"
        )
        print(f"Total duration: {duration:.3f}s")
        print("========================================================\n")

    # JSON summary (for observability dashboards / curl checks)
    if json_mode:
        payload = {
            "ok": summary.ok,
            "fatal_failures": summary.fatal_failures,
            "nonfatal_failures": summary.nonfatal_failures,
            "ts": summary.ts,
            "duration_sec": summary.duration_sec,
            "probes": {
                name: _probe_dict(p) for name, p in summary.probes.items()
            },
        }
        print(json.dumps(payload, default=str))

    # Exit code for Render / infra
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except SystemExit:
        raise
    except Exception as e:  # noqa: BLE001
        print(f"❌ Healthcheck failed at top level: {type(e).__name__}: {e}")
        sys.exit(1)
