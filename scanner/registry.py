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

Registry also falls back to: scan, scan_<name>, scan_once

Environment controls
--------------------
SCANNERS_ENABLED="pumpfun,raydium"          (optional; ordered list)
SCANNERS_DISABLED="dexscreener"             (optional)
SCANNER_<NAME>_ENABLED=0/1                  (overrides lists)

SCANNER_CONCURRENCY=3                       (default: 3)
SCANNER_TIMEOUT_SEC=10                      (default: 10; float ok)

SCANNER_FAIL_OPEN=1                         (default: 1) if 0, scan errors raise
SCANNER_CANDIDATE_LIMIT=0                   (0 = no limit; else cap per scanner)

SCANNER_STRICT_CANDIDATES=0                 (if 1, invalid candidates raise contract errors
                                             instead of being dropped/marked)
SCANNER_DROP_INVALID=1                      (default: 1) if 0, keep invalid but mark extra["invalid"]=True
SCANNER_WARNINGS_MAX=20                     (default: 20) cap warnings list stored in extra
SCANNER_LOG_EVERY_SEC=8                     (default: 8) throttle "SCAN_OK" logs

Hard rule: this registry performs NO network calls. Scanner modules may, but are imported lazily.
"""

import asyncio
import contextlib
import dataclasses
import importlib
import inspect
import logging
import os
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, TypeAlias

from utils_pkg import mint_guard as mg
from utils_pkg.contracts import Candidate, Source, ensure_candidate, looks_like_base58

# ──────────────────────────────────────────────────────────────────────────────
# Logger (safe, no hard dependency)
# ──────────────────────────────────────────────────────────────────────────────
log: Any
try:
    from logging_setup import get_logger  # type: ignore

    log = get_logger("scanner.registry")
except Exception:  # pragma: no cover
    log = logging.getLogger("scanner.registry")
    if not log.handlers:
        log.addHandler(logging.NullHandler())


def _ensure_log_helpers() -> None:
    """Guarantee log.event + log.every exist with safe fallbacks."""

    if not hasattr(log, "event"):

        def _event(name: str, **kw: Any) -> None:  # pragma: no cover
            with contextlib.suppress(Exception):
                log.info("%s %s", name, kw)

        log.event = _event  # type: ignore[attr-defined]

    if not hasattr(log, "every"):

        def _every(key: str, n: float = 60.0, msg: str | None = None, **kw: Any) -> bool:
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

        log.every = _every  # type: ignore[attr-defined]


_ensure_log_helpers()

# ──────────────────────────────────────────────────────────────────────────────
# Env helpers
# ──────────────────────────────────────────────────────────────────────────────
_TRUE = {"1", "true", "t", "yes", "y", "on"}


def _b(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return bool(default)
    return v.strip().lower() in _TRUE


def _s(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else v.strip()


def _i(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        return int(default)
    s = v.strip()
    if s == "":
        return int(default)
    try:
        return int(float(s))
    except Exception:
        return int(default)


def _f(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None:
        return float(default)
    s = v.strip()
    if s == "":
        return float(default)
    try:
        return float(s)
    except Exception:
        return float(default)


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
RawCandidate: TypeAlias = Candidate | Mapping[str, Any]
ScanFn: TypeAlias = Callable[..., Awaitable[list[RawCandidate]]]


# ──────────────────────────────────────────────────────────────────────────────
# Exceptions
# ──────────────────────────────────────────────────────────────────────────────
class ScannerError(RuntimeError):
    """Base error for scanner registry failures."""


class ScannerContractError(ScannerError):
    """Raised when a scanner violates the registry contract in strict modes."""


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
    global _SETTINGS_CACHE

    now = time.time()
    ts, cached = _SETTINGS_CACHE
    if cached is not None and (now - ts) < _SETTINGS_TTL_S:
        return cached

    enabled = _csv_list(_s("SCANNERS_ENABLED", ""))
    disabled = _csv_set(_s("SCANNERS_DISABLED", ""))
    concurrency = max(1, _i("SCANNER_CONCURRENCY", 3))
    timeout_s = max(1.0, _f("SCANNER_TIMEOUT_SEC", 10.0))
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
    _SETTINGS_CACHE = (now, st)
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
            health_attr=self.health_attr or f"{n}_scanner_health",
            shutdown_attr=self.shutdown_attr or f"shutdown_{n}_scanner",
            enabled_env=self.enabled_env or f"SCANNER_{n.upper()}_ENABLED",
            tags=dict(self.tags or {}),
        )


# ──────────────────────────────────────────────────────────────────────────────
# Candidate canonicalization + validation
# ──────────────────────────────────────────────────────────────────────────────
def _with_extra(c: Candidate, **kw: Any) -> Candidate:
    """Prefer Candidate.with_extra, fallback to dataclasses.replace."""
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
        scanned_at_ts=time.time(),
    )


def _is_non_mint(mint: str) -> bool:
    """Guard known non-mint program IDs."""
    fn = getattr(mg, "is_non_mint_address", None)
    if callable(fn):
        with contextlib.suppress(Exception):
            return bool(fn(mint))
    return False


def _validate_candidate(c: Candidate) -> tuple[bool, list[str]]:
    """
    Lightweight validation. Not an entry gate, just schema sanity.
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

    price_usd = float(getattr(c, "price_usd", 0.0) or 0.0)
    price_sol = float(getattr(c, "price_sol", 0.0) or 0.0)
    if price_usd <= 0.0 and price_sol <= 0.0:
        warns.append("missing_price")

    liq_usd = float(getattr(c, "liq_usd", 0.0) or 0.0)
    if liq_usd <= 0.0:
        warns.append("missing_liquidity")

    return True, warns


def canonicalize_candidate(raw: RawCandidate, *, spec: ScannerSpec) -> Candidate:
    """Convert Candidate|Mapping -> Candidate and stamp Source + registry metadata."""
    c = raw if isinstance(raw, Candidate) else ensure_candidate(raw)

    with contextlib.suppress(Exception):
        src = getattr(c, "source", None)
        if not isinstance(src, Source) or src == Source.MANUAL:
            c = dataclasses.replace(c, source=_coerce_source(spec.name))

    return _add_scanner_meta(c, spec=spec)


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

    # ── registration ──────────────────────────────────────────────────────────
    def register(self, spec: ScannerSpec) -> None:
        s = spec.normalized()
        self._specs[s.name] = s

    def register_many(self, specs: Sequence[ScannerSpec]) -> None:
        for s in specs:
            self.register(s)

    def specs(self) -> dict[str, ScannerSpec]:
        return dict(self._specs)

    # ── enablement logic ──────────────────────────────────────────────────────
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

        if st.enabled:
            idx = {n: i for i, n in enumerate(st.enabled)}
            names.sort(key=lambda n: idx.get(n, 10_000))
        else:
            names.sort()
        return names

    # ── module + fn resolution ────────────────────────────────────────────────
    def _load_module(self, module_path: str) -> Any:
        m = self._module_cache.get(module_path)
        if m is not None:
            return m
        m = importlib.import_module(module_path)
        self._module_cache[module_path] = m
        return m

    def _resolve_scan_fn(self, spec: ScannerSpec) -> ScanFn:
        cached = self._scan_fn_cache.get(spec.name)
        if cached is not None:
            return cached

        mod = self._load_module(spec.module)

        fn = getattr(mod, spec.scan_attr, None)
        if callable(fn):
            self._scan_fn_cache[spec.name] = fn  # type: ignore[assignment]
            return fn  # type: ignore[return-value]

        for cand in ("scan", f"scan_{spec.name}", "scan_once"):
            fn2 = getattr(mod, cand, None)
            if callable(fn2):
                self._scan_fn_cache[spec.name] = fn2  # type: ignore[assignment]
                return fn2  # type: ignore[return-value]

        raise ScannerContractError(
            f"Scanner '{spec.name}' missing scan function '{spec.scan_attr}' in {spec.module}"
        )

    async def _call_maybe_await(self, fn: Any, /, *args: Any, **kwargs: Any) -> Any:
        """
        Call fn and await if it returns an awaitable.
        Cancellation-safe: CancelledError always propagates.
        """
        try:
            res = fn(*args, **kwargs)
            if inspect.isawaitable(res):
                return await res
            return res
        except asyncio.CancelledError:
            raise

    async def _invoke_scan_fn(
        self,
        fn: ScanFn,
        *,
        redis: Any,
        session: Any,
        kw: dict[str, Any],
    ) -> list[RawCandidate]:
        """
        Call a scanner function with deterministic signature compatibility.

        We avoid using "catch TypeError to guess signature", because a TypeError
        inside the scanner body would be incorrectly treated as a mismatch.
        """
        try:
            sig = inspect.signature(fn)
            params = sig.parameters
        except Exception:
            # Fallback: best-effort common contract call
            out = await self._call_maybe_await(fn, redis=redis, session=session, **kw)
            return out if isinstance(out, list) else (list(out) if out is not None else [])

        has_var_kw = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values())
        accepts = set(params.keys())

        call_kwargs: dict[str, Any] = {}
        # only pass redis/session if the function can accept them
        if has_var_kw or "redis" in accepts:
            call_kwargs["redis"] = redis
        if has_var_kw or "session" in accepts:
            call_kwargs["session"] = session

        # merge user kw
        call_kwargs.update(dict(kw or {}))

        # if no **kwargs, filter unknown keys to avoid unexpected-keyword TypeError
        if not has_var_kw:
            call_kwargs = {k: v for k, v in call_kwargs.items() if k in accepts}

        # If scanner takes no kwargs at all, call without kwargs.
        if not call_kwargs:
            out = await self._call_maybe_await(fn)
            return out if isinstance(out, list) else (list(out) if out is not None else [])

        out = await self._call_maybe_await(fn, **call_kwargs)
        if out is None:
            return []
        if isinstance(out, list):
            return out
        if isinstance(out, Sequence) and not isinstance(out, (str, bytes, bytearray)):
            return list(out)
        return []

    # ── scan APIs ─────────────────────────────────────────────────────────────
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

        Raises only when:
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
            return await self._invoke_scan_fn(fn, redis=redis, session=session, kw=dict(kw))

        try:
            raw_list = await asyncio.wait_for(_call(), timeout=timeout)

            if not raw_list:
                raw_list = []

            if limit and len(raw_list) > limit:
                raw_list = raw_list[:limit]

            out: list[Candidate] = []
            dropped = 0
            warned = 0

            for item in raw_list:
                try:
                    c = canonicalize_candidate(item, spec=spec)
                    ok, warns = _validate_candidate(c)

                    if warns:
                        warned += 1
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
                n=float(st.log_every_sec),
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
        """Run enabled scanners concurrently and return {scanner_name: [Candidate, ...]}."""
        st = _settings()
        enabled = [n.strip().lower() for n in (names or self.enabled_names()) if str(n).strip()]
        if not enabled:
            return {}

        conc = max(1, int(concurrency if concurrency is not None else st.concurrency))
        sem = asyncio.Semaphore(conc)

        async def _one(n: str) -> tuple[str, list[Candidate]]:
            async with sem:
                res = await self.scan_one(
                    n, redis=redis, session=session, timeout_s=timeout_s, **kw
                )
                return n, res

        tasks = [asyncio.create_task(_one(n), name=f"scan:{n}") for n in enabled]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        out: dict[str, list[Candidate]] = {}
        first_err: BaseException | None = None

        for n, r in zip(enabled, results, strict=True):
            if isinstance(r, asyncio.CancelledError):
                raise r
            if isinstance(r, BaseException):
                # Should only happen if scan_one was configured to raise (fail_open=0 / strict)
                log.every(
                    "SCAN_FAIL_ALL",
                    n=3,
                    msg="SCAN_FAIL_ALL",
                    scanner=n,
                    err=_short(type(r).__name__),
                    detail=_short(r),
                )
                out[n] = []
                if first_err is None:
                    first_err = r
            else:
                out[n] = r[1] if isinstance(r, tuple) and len(r) == 2 else []  # defensive
                # With our _one() contract, r should be (name, list)
                if isinstance(r, tuple) and len(r) == 2 and isinstance(r[1], list):
                    out[r[0]] = r[1]

        if first_err is not None and not st.fail_open:
            raise first_err

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
        """Convenience: run scan_all and flatten into a single list of Candidates."""
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

    # ── health + shutdown ─────────────────────────────────────────────────────
    def health(self, *, names: Sequence[str] | None = None) -> dict[str, Any]:
        """Best-effort: may import scanner modules to access health functions."""
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
        """Best-effort: call shutdown hooks if present."""
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
    "RawCandidate",
    "RegistrySettings",
    "ScanFn",
    "ScannerContractError",
    "ScannerError",
    "ScannerRegistry",
    "ScannerSpec",
    "canonicalize_candidate",
    "enabled_scanners",
    "health",
    "scan_all",
    "scan_flat",
    "scan_one",
    "shutdown_all",
]
