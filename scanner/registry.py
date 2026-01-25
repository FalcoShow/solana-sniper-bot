from __future__ import annotations

"""
scanner/registry.py — Canonical Scanner Registry (P0 LOCK-IN, V3+++)
====================================================================

This module is the single coordinator for all scanners.

Hard guarantees (contract)
--------------------------
1) Lazy imports: scanner modules are imported only when invoked.
2) Cancellation-safe: never swallow asyncio.CancelledError.
3) Isolation: one scanner can fail without taking down the loop (fail-open by default).
4) Canonical output type: scanners ultimately emit utils_pkg.contracts.Candidate.
5) Back-compat: scanners may emit dict-like mappings; we canonicalize via ensure_candidate().

Scanner module contract (recommended)
-------------------------------------
Each scanner module SHOULD expose:

    async scan_<name>(redis=None, session=None, **kw) -> list[Candidate|Mapping]

Optional:
    <name>_scanner_health() -> dict
    async shutdown_<name>_scanner() -> None

Registry will also fall back to: scan, scan_<name>, scan_once

Environment controls
--------------------
SCANNERS_ENABLED="pumpfun,raydium"          (optional; ordered list)
SCANNERS_DISABLED="dexscreener"             (optional)
SCANNER_<NAME>_ENABLED=0/1                  (overrides lists)

SCANNER_CONCURRENCY=3                       (default: 3)
SCANNER_TIMEOUT_SEC=10                      (default: 10)

SCANNER_FAIL_OPEN=1                         (default: 1) if 0, scan errors raise
SCANNER_CANDIDATE_LIMIT=0                   (0 = no limit; else cap per scanner)

SCANNER_STRICT_CANDIDATES=0                 (if 1, invalid candidates raise contract errors
                                             instead of being dropped/marked)
SCANNER_DROP_INVALID=1                      (default: 1) if 0, keep invalid but mark extra["invalid"]=True
SCANNER_WARNINGS_MAX=20                     (default: 20) cap warnings list stored in extra
SCANNER_LOG_EVERY_SEC=8                     (default: 8) throttle "SCAN_OK" logs
"""

import asyncio
import contextlib
import dataclasses
import importlib
import logging
import os
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from utils_pkg import mint_guard as mg
from utils_pkg.contracts import Candidate, Source, ensure_candidate, looks_like_base58

# ──────────────────────────────────────────────────────────────────────────────
# Logger (safe, no hard dependency)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from logging_setup import get_logger  # type: ignore

    log = get_logger("scanner.registry")
except Exception:  # pragma: no cover
    log = logging.getLogger("scanner.registry")
    if not log.handlers:
        logging.basicConfig(level=logging.INFO)


def _ensure_log_helpers() -> None:
    """
    Guarantee log.event + log.every exist with safe fallbacks.
    """
    if not hasattr(log, "event"):

        def _event(name: str, **kw: Any) -> None:  # pragma: no cover
            with contextlib.suppress(Exception):
                log.info("%s %s", name, kw)

        log.event = _event

    if not hasattr(log, "every"):

        def _every(
            key: str, n: int = 60, msg: str | None = None, **kw: Any
        ) -> bool:  # pragma: no cover
            now = time.time()
            store = getattr(_every, "_store", {})  # type: ignore[attr-defined]
            last = float(store.get(key, 0.0))
            if (now - last) >= float(n):
                store[key] = now
                _every._store = store  # type: ignore[attr-defined]
                with contextlib.suppress(Exception):
                    log.event(msg or key, **kw)
                return True
            return False

        log.every = _every


_ensure_log_helpers()


# ──────────────────────────────────────────────────────────────────────────────
# Env helpers
# ──────────────────────────────────────────────────────────────────────────────
_TRUE = {"1", "true", "t", "yes", "y", "on"}


def _b(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return bool(default)
    return str(v).strip().lower() in _TRUE


def _s(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()


def _i(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(float(v)) if v not in (None, "") else int(default)
    except Exception:
        return int(default)


def _csv_list(val: str) -> list[str]:
    out: list[str] = []
    for x in (val or "").split(","):
        x = x.strip().lower()
        if x:
            out.append(x)
    return out


def _csv_set(val: str) -> set[str]:
    return set(_csv_list(val))


def _short(x: Any, n: int = 240) -> str:
    try:
        s = str(x)
        return s if len(s) <= n else (s[:n] + "…")
    except Exception:
        return "<?>"


# ──────────────────────────────────────────────────────────────────────────────
# Types
# ──────────────────────────────────────────────────────────────────────────────
RawCandidate = Candidate | Mapping[str, Any]
ScanFn = Callable[..., Awaitable[list[RawCandidate]]]


# ──────────────────────────────────────────────────────────────────────────────
# Exceptions
# ──────────────────────────────────────────────────────────────────────────────
class ScannerError(RuntimeError):
    pass


class ScannerContractError(ScannerError):
    pass


# ──────────────────────────────────────────────────────────────────────────────
# Settings (env-driven)
# ──────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class RegistrySettings:
    enabled: list[str]
    disabled: set[str]
    concurrency: int
    timeout_s: float
    fail_open: bool
    candidate_limit: int
    strict_candidates: bool
    drop_invalid: bool
    warnings_max: int
    log_every_sec: int


_SETTINGS_CACHE: tuple[float, RegistrySettings | None] = (0.0, None)
_SETTINGS_TTL_S = 1.0


def _settings() -> RegistrySettings:
    now = time.time()
    ts, st = _SETTINGS_CACHE
    if st is not None and (now - ts) < _SETTINGS_TTL_S:
        return st

    enabled = _csv_list(_s("SCANNERS_ENABLED", ""))
    disabled = _csv_set(_s("SCANNERS_DISABLED", ""))
    concurrency = max(1, _i("SCANNER_CONCURRENCY", 3))
    timeout_s = max(1.0, float(_i("SCANNER_TIMEOUT_SEC", 10)))
    fail_open = _b("SCANNER_FAIL_OPEN", True)
    candidate_limit = max(0, _i("SCANNER_CANDIDATE_LIMIT", 0))
    strict_candidates = _b("SCANNER_STRICT_CANDIDATES", False)
    drop_invalid = _b("SCANNER_DROP_INVALID", True)
    warnings_max = max(0, _i("SCANNER_WARNINGS_MAX", 20))
    log_every_sec = max(1, _i("SCANNER_LOG_EVERY_SEC", 8))

    st = RegistrySettings(
        enabled=enabled,
        disabled=disabled,
        concurrency=concurrency,
        timeout_s=timeout_s,
        fail_open=fail_open,
        candidate_limit=candidate_limit,
        strict_candidates=strict_candidates,
        drop_invalid=drop_invalid,
        warnings_max=warnings_max,
        log_every_sec=log_every_sec,
    )
    globals()["_SETTINGS_CACHE"] = (now, st)
    return st


# ──────────────────────────────────────────────────────────────────────────────
# Scanner spec
# ──────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class ScannerSpec:
    """
    Declarative wiring for a scanner.

    name: canonical short name ("pumpfun", "raydium")
    module: import path ("scanner.pumpfun_scanner")
    scan_attr: attribute name of async scan fn ("scan_pumpfun")
    health_attr: optional health fn name
    shutdown_attr: optional shutdown fn name
    enabled_env: env override key, defaults to SCANNER_<NAME>_ENABLED
    tags: freeform metadata (dex, chain, etc.)
    """

    name: str
    module: str
    scan_attr: str
    health_attr: str | None = None
    shutdown_attr: str | None = None
    enabled_env: str | None = None
    tags: dict[str, Any] = field(default_factory=dict)

    def normalized(self) -> "ScannerSpec":
        n = (self.name or "").strip().lower()
        if not n:
            raise ValueError("ScannerSpec.name required")
        m = (self.module or "").strip()
        if not m:
            raise ValueError("ScannerSpec.module required")
        sa = (self.scan_attr or "").strip()
        if not sa:
            raise ValueError("ScannerSpec.scan_attr required")
        return ScannerSpec(
            name=n,
            module=m,
            scan_attr=sa,
            health_attr=(self.health_attr or f"{n}_scanner_health"),
            shutdown_attr=(self.shutdown_attr or f"shutdown_{n}_scanner"),
            enabled_env=(self.enabled_env or f"SCANNER_{n.upper()}_ENABLED"),
            tags=dict(self.tags or {}),
        )


# ──────────────────────────────────────────────────────────────────────────────
# Candidate canonicalization + validation
# ──────────────────────────────────────────────────────────────────────────────
def _with_extra(c: Candidate, **kw: Any) -> Candidate:
    """
    Prefer Candidate.with_extra, fallback to dataclasses.replace.
    """
    fn = getattr(c, "with_extra", None)
    if callable(fn):
        with contextlib.suppress(Exception):
            return fn(**kw)
    extra = dict(getattr(c, "extra", {}) or {})
    extra.update(kw)
    return dataclasses.replace(c, extra=extra)


def _coerce_source(scanner_name: str) -> Source:
    n = (scanner_name or "").strip().lower()
    try:
        return Source(n)
    except Exception:
        return Source.MANUAL


def _add_scanner_meta(c: Candidate, *, spec: ScannerSpec) -> Candidate:
    return _with_extra(
        c,
        scanner=spec.name,
        scanner_module=spec.module,
        scanner_tags=dict(spec.tags or {}),
    )


def _is_non_mint(mint: str) -> bool:
    """
    Guard known non-mint program IDs.
    """
    fn = getattr(mg, "is_non_mint_address", None)
    if callable(fn):
        with contextlib.suppress(Exception):
            return bool(fn(mint))
    return False


def _validate_candidate(c: Candidate) -> tuple[bool, list[str]]:
    """
    Lightweight validation. Not an entry gate, just a schema sanity check.
    Returns (ok, warnings).
    """
    warns: list[str] = []
    mint = (getattr(c, "mint", "") or "").strip()
    if not mint:
        return False, ["missing_mint"]

    if _is_non_mint(mint):
        return False, ["non_mint_program_id"]

    if not looks_like_base58(mint, min_len=20, max_len=60):
        warns.append("mint_not_base58ish")

    # Price can be 0 for brand new pools; warn only.
    price_usd = float(getattr(c, "price_usd", 0.0) or 0.0)
    price_sol = float(getattr(c, "price_sol", 0.0) or 0.0)
    if price_usd <= 0.0 and price_sol <= 0.0:
        warns.append("missing_price")

    liq_usd = float(getattr(c, "liq_usd", 0.0) or 0.0)
    if liq_usd <= 0.0:
        warns.append("missing_liquidity")

    return True, warns


def canonicalize_candidate(raw: RawCandidate, *, spec: ScannerSpec) -> Candidate:
    """
    Convert Candidate|Mapping -> Candidate and stamp Source + registry metadata.
    """
    c = raw if isinstance(raw, Candidate) else ensure_candidate(raw)

    # If scanner didn't set Source meaningfully, stamp it.
    try:
        src = getattr(c, "source", None)
        if not isinstance(src, Source) or src == Source.MANUAL:
            c = dataclasses.replace(c, source=_coerce_source(spec.name))
    except Exception:
        pass

    c = _add_scanner_meta(c, spec=spec)
    return c


# ──────────────────────────────────────────────────────────────────────────────
# Registry
# ──────────────────────────────────────────────────────────────────────────────
class ScannerRegistry:
    """
    Canonical scanner registry.

    Primary APIs:
      - enabled_names()
      - scan_one(...)
      - scan_all(...)
      - scan_flat(...)
      - health()
      - shutdown_all()
    """

    def __init__(self) -> None:
        self._specs: dict[str, ScannerSpec] = {}
        self._module_cache: dict[str, Any] = {}
        self._scan_fn_cache: dict[str, ScanFn] = {}

    # ----- registration -----
    def register(self, spec: ScannerSpec) -> None:
        s = spec.normalized()
        self._specs[s.name] = s

    def register_many(self, specs: Sequence[ScannerSpec]) -> None:
        for s in specs:
            self.register(s)

    def specs(self) -> dict[str, ScannerSpec]:
        return dict(self._specs)

    # ----- enable/disable/order -----
    def _enabled_by_env(self, name: str, spec: ScannerSpec) -> bool:
        """
        Precedence:
          1) SCANNER_<NAME>_ENABLED explicit override
          2) SCANNERS_DISABLED contains name -> disabled
          3) SCANNERS_ENABLED (if non-empty) -> only those in list enabled
          4) default enabled
        """
        st = _settings()
        env_key = (spec.enabled_env or f"SCANNER_{name.upper()}_ENABLED").strip()
        if os.getenv(env_key) not in (None, ""):
            return _b(env_key, True)

        if name in st.disabled:
            return False

        if st.enabled:
            return name in set(st.enabled)

        return True

    def enabled_names(self) -> list[str]:
        st = _settings()
        names = [n for n, s in self._specs.items() if self._enabled_by_env(n, s)]

        # stable ordering: respect SCANNERS_ENABLED order if provided
        if st.enabled:
            idx = {n: i for i, n in enumerate(st.enabled)}
            names.sort(key=lambda n: idx.get(n, 10_000))
        else:
            names.sort()
        return names

    # ----- lazy load -----
    def _load_module(self, module_path: str) -> Any:
        m = self._module_cache.get(module_path)
        if m is not None:
            return m
        m = importlib.import_module(module_path)
        self._module_cache[module_path] = m
        return m

    def _resolve_scan_fn(self, spec: ScannerSpec) -> ScanFn:
        """
        Resolve and cache scanner scan callable.
        """
        if spec.name in self._scan_fn_cache:
            return self._scan_fn_cache[spec.name]

        mod = self._load_module(spec.module)
        fn = getattr(mod, spec.scan_attr, None)
        if callable(fn):
            self._scan_fn_cache[spec.name] = fn  # type: ignore[assignment]
            return fn  # type: ignore[return-value]

        # compatibility fallbacks (common in older modules)
        for cand in ("scan", f"scan_{spec.name}", "scan_once"):
            fn2 = getattr(mod, cand, None)
            if callable(fn2):
                self._scan_fn_cache[spec.name] = fn2  # type: ignore[assignment]
                return fn2  # type: ignore[return-value]

        raise ScannerContractError(
            f"Scanner '{spec.name}' missing scan function '{spec.scan_attr}' in {spec.module}"
        )

    # ----- scanning -----
    async def scan_one(
        self,
        name: str,
        *,
        redis: Any = None,
        session: Any = None,
        timeout_s: float | None = None,
        **kw: Any,
    ) -> list[Candidate]:
        """
        Run one scanner and return canonical Candidates.

        Never raises unless:
          - SCANNER_FAIL_OPEN=0 and scanner call fails, or
          - SCANNER_STRICT_CANDIDATES=1 and candidate contract violations occur.
        """
        st = _settings()
        n = (name or "").strip().lower()
        spec = self._specs.get(n)
        if spec is None:
            raise KeyError(f"Unknown scanner: {n}")

        if not self._enabled_by_env(n, spec):
            return []

        timeout = float(timeout_s if timeout_s is not None else st.timeout_s)
        strict = bool(st.strict_candidates)
        fail_open = bool(st.fail_open)
        drop_invalid = bool(st.drop_invalid)
        limit = int(st.candidate_limit)

        fn = self._resolve_scan_fn(spec)
        t0 = time.time()

        async def _call() -> list[RawCandidate]:
            # Most scanners accept kw; fall back to positional if needed.
            try:
                return await fn(redis=redis, session=session, **kw)
            except TypeError:
                try:
                    return await fn(redis, session, **kw)  # type: ignore[misc]
                except TypeError:
                    return await fn(redis)  # type: ignore[misc]

        try:
            raw = await asyncio.wait_for(_call(), timeout=timeout)

            if not isinstance(raw, list):
                msg = f"Scanner '{n}' returned {type(raw).__name__}, expected list"
                if strict:
                    raise ScannerContractError(msg)
                log.every(
                    "SCAN_BAD_SHAPE", n=15, msg="SCAN_BAD_SHAPE", scanner=n, got=type(raw).__name__
                )
                return []

            if limit and len(raw) > limit:
                raw = raw[:limit]

            out: list[Candidate] = []
            dropped = 0
            warned = 0

            for item in raw:
                try:
                    c = canonicalize_candidate(item, spec=spec)
                    ok, warns = _validate_candidate(c)

                    if warns:
                        warned += 1
                        # preserve warnings but cap growth
                        cur = (c.extra or {}).get("warnings")
                        w_list: list[str] = list(cur) if isinstance(cur, list) else []
                        w_list.extend(warns)
                        if st.warnings_max > 0:
                            w_list = w_list[: st.warnings_max]
                        c = _with_extra(c, warnings=w_list)

                    if not ok:
                        if strict:
                            raise ScannerContractError(f"Invalid candidate from '{n}': {warns}")
                        if drop_invalid:
                            dropped += 1
                            continue
                        c = _with_extra(c, invalid=True, invalid_reasons=warns[:10])

                    out.append(c)

                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    if strict:
                        raise
                    dropped += 1
                    log.every(
                        "SCAN_ITEM_COERCE_FAIL",
                        n=10,
                        msg="SCAN_ITEM_COERCE_FAIL",
                        scanner=n,
                        err=_short(e),
                    )

            ms = int((time.time() - t0) * 1000)
            log.every(
                f"SCAN_OK:{n}",
                n=st.log_every_sec,
                msg="SCAN_OK",
                scanner=n,
                count=len(out),
                dropped=dropped,
                warned=warned,
                ms=ms,
            )
            return out

        except asyncio.CancelledError:
            raise
        except TimeoutError:
            log.every("SCAN_TIMEOUT", n=5, msg="SCAN_TIMEOUT", scanner=n, timeout_s=timeout)
            return []
        except Exception as e:
            log.every(
                "SCAN_FAIL",
                n=5,
                msg="SCAN_FAIL",
                scanner=n,
                err=_short(type(e).__name__),
                detail=_short(e),
            )
            if fail_open:
                return []
            raise

    async def scan_all(
        self,
        *,
        redis: Any = None,
        session: Any = None,
        names: Sequence[str] | None = None,
        concurrency: int | None = None,
        timeout_s: float | None = None,
        **kw: Any,
    ) -> dict[str, list[Candidate]]:
        """
        Run enabled scanners concurrently and return {scanner_name: [Candidate, ...]}.
        """
        st = _settings()
        enabled = [n.strip().lower() for n in (names or self.enabled_names()) if str(n).strip()]
        if not enabled:
            return {}

        conc = max(1, int(concurrency if concurrency is not None else st.concurrency))
        sem = asyncio.Semaphore(conc)

        out: dict[str, list[Candidate]] = {}

        async def _one(n: str) -> None:
            async with sem:
                out[n] = await self.scan_one(
                    n, redis=redis, session=session, timeout_s=timeout_s, **kw
                )

        tasks = [asyncio.create_task(_one(n), name=f"scan:{n}") for n in enabled]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # scan_one already absorbs errors when fail_open=1, but log unexpected task exceptions
        for n, r in zip(enabled, results, strict=False):
            if isinstance(r, Exception):
                log.every(
                    "SCAN_TASK_EXCEPTION", n=10, msg="SCAN_TASK_EXCEPTION", scanner=n, err=_short(r)
                )

        return out

    async def scan_flat(
        self,
        *,
        redis: Any = None,
        session: Any = None,
        names: Sequence[str] | None = None,
        concurrency: int | None = None,
        timeout_s: float | None = None,
        **kw: Any,
    ) -> list[Candidate]:
        """
        Convenience: run scan_all and flatten into a single list of Candidates.
        """
        by = await self.scan_all(
            redis=redis,
            session=session,
            names=names,
            concurrency=concurrency,
            timeout_s=timeout_s,
            **kw,
        )

        order = [n.strip().lower() for n in (names or self.enabled_names()) if str(n).strip()]
        flat: list[Candidate] = []
        for n in order:
            flat.extend(by.get(n, []))
        return flat

    # ----- health/shutdown -----
    def health(self, *, names: Sequence[str] | None = None) -> dict[str, Any]:
        """
        Best-effort: may import scanner modules to access health functions.
        """
        enabled = [n.strip().lower() for n in (names or self.enabled_names()) if str(n).strip()]
        out: dict[str, Any] = {"enabled": enabled, "scanners": {}}

        for n in enabled:
            spec = self._specs.get(n)
            if spec is None:
                continue
            try:
                mod = self._load_module(spec.module)
                fn = getattr(mod, spec.health_attr or "", None)
                if callable(fn):
                    out["scanners"][n] = fn()
                else:
                    out["scanners"][n] = {"ok": True, "health": "n/a"}
            except Exception as e:
                out["scanners"][n] = {"ok": False, "err": _short(e)}
        return out

    async def shutdown_all(self, *, names: Sequence[str] | None = None) -> None:
        """
        Best-effort: call shutdown hooks if present.
        """
        target = [n.strip().lower() for n in (names or list(self._specs.keys())) if str(n).strip()]
        for n in target:
            spec = self._specs.get(n)
            if spec is None:
                continue
            try:
                mod = self._load_module(spec.module)
                fn = getattr(mod, spec.shutdown_attr or "", None)
                if fn is None:
                    fn = getattr(mod, f"shutdown_{n}_scanner", None)

                if fn is None:
                    continue

                if asyncio.iscoroutinefunction(fn):
                    await fn()
                else:
                    await asyncio.to_thread(fn)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.every(
                    "SCANNER_SHUTDOWN_FAIL",
                    n=10,
                    msg="SCANNER_SHUTDOWN_FAIL",
                    scanner=n,
                    err=_short(e),
                )


# ──────────────────────────────────────────────────────────────────────────────
# Default registry wiring (lazy imports)
# ──────────────────────────────────────────────────────────────────────────────
REGISTRY = ScannerRegistry()
REGISTRY.register_many(
    [
        ScannerSpec(
            name="raydium",
            module="scanner.raydium_scanner",
            scan_attr="scan_raydium",
            health_attr="raydium_scanner_health",
            shutdown_attr="shutdown_raydium_scanner",
            tags={"dex": "raydium", "chain": "solana"},
        ),
        ScannerSpec(
            name="pumpfun",
            module="scanner.pumpfun_scanner",
            scan_attr="scan_pumpfun",
            health_attr="pumpfun_scanner_health",
            shutdown_attr="shutdown_pumpfun_scanner",
            tags={"dex": "pumpfun", "chain": "solana"},
        ),
        ScannerSpec(
            name="dexscreener",
            module="scanner.dexscreener_scanner",
            scan_attr="scan_dexscreener",
            health_attr="dexscreener_scanner_health",
            shutdown_attr="shutdown_dexscreener_scanner",
            tags={"dex": "dexscreener", "chain": "multi"},
        ),
        ScannerSpec(
            name="axiom",
            module="scanner.axiom_scanner",
            scan_attr="scan_axiom",
            health_attr="axiom_scanner_health",
            shutdown_attr="shutdown_axiom_scanner",
            tags={"dex": "axiom", "chain": "solana"},
        ),
    ]
)


# ──────────────────────────────────────────────────────────────────────────────
# Convenience module-level wrappers (keeps rest of codebase simple)
# ──────────────────────────────────────────────────────────────────────────────
def enabled_scanners() -> list[str]:
    return REGISTRY.enabled_names()


async def scan_one(name: str, **kw: Any) -> list[Candidate]:
    return await REGISTRY.scan_one(name, **kw)


async def scan_all(**kw: Any) -> dict[str, list[Candidate]]:
    return await REGISTRY.scan_all(**kw)


async def scan_flat(**kw: Any) -> list[Candidate]:
    return await REGISTRY.scan_flat(**kw)


def health(**kw: Any) -> dict[str, Any]:
    return REGISTRY.health(**kw)


async def shutdown_all(**kw: Any) -> None:
    await REGISTRY.shutdown_all(**kw)


__all__ = [
    "Candidate",
    "Source",
    "REGISTRY",
    "ScannerContractError",
    "ScannerError",
    "ScannerRegistry",
    "ScannerSpec",
    "RegistrySettings",
    "canonicalize_candidate",
    "enabled_scanners",
    "scan_one",
    "scan_all",
    "scan_flat",
    "health",
    "shutdown_all",
]
