# metrics/metrics_exporter.py
from __future__ import annotations

"""
metrics/metrics_exporter.py

God-Tier Prometheus exporter (production-grade)
===============================================

Purpose
-------
Expose *operational + performance* metrics via Prometheus, powered by Redis keys
written by the bot (router, risk, playbook levers, pnl, budgets, halts, workers).

Hard goals
----------
- Safe: never crashes the bot; exporter errors are counted + logged.
- Deterministic: Prometheus Counters are monotonic (we delta-from-redis-absolute).
- Low-cardinality: lever string values don't explode labelsets (we remove old label values).
- Works local + Render: configurable bind addr/port + Redis URL.
- Import-safe: importing this module never requires prometheus_client unless you run exporter.

Run
---
python -m metrics.metrics_exporter

Env (core)
---------
  METRICS_ENABLED=1                  (default 1)
  METRICS_ADDR=0.0.0.0               (default 0.0.0.0)
  METRICS_PORT=9102                  (default 9102)
  EXPORTER_POLL_SEC=10               (default 10)
  REDIS_URL=redis://localhost:6379   (default localhost)
  EXPORTER_SCAN_LIMIT=5000           (default 5000; cap scanned keys per pattern per tick)

Metric naming
-------------
  METRICS_PREFIX=bot_                (default bot_)
    -> prefixes ALL exported metrics (including exporter health)

Staleness / logs
----------------
  EXPORTER_STALE_TTL_SEC=1800        (default 1800)
  EXPORTER_LOG_EVERY_SEC=60          (default 60)

Redis keys (defaults; override any via env below)
------------------------------------------------
Core PnL truth:
  METRICS_KEY_EQUITY_HWM=metrics:equity_hwm_usd
  METRICS_KEY_WINRATE20=metrics:winrate20
  METRICS_KEY_ROLLING_DD=metrics:rolling_dd_pct
  METRICS_KEY_TRADES_TOTAL=metrics:trades_total
  METRICS_KEY_WINS_TOTAL=metrics:wins_total
  METRICS_KEY_LOSSES_TOTAL=metrics:losses_total   (optional)
  METRICS_KEY_CANDIDATES_SEEN=metrics:candidates_seen_total
  METRICS_KEY_PASSED_GATE=metrics:passed_gate_total
  METRICS_KEY_BUYS_TOTAL=metrics:buys_total
  METRICS_KEY_SELLS_TOTAL=metrics:sells_total

Daily series:
  PNL_PREFIX=pnl:day:realized_usd
  BUDGET_KEY_PREFIX=budget:sniper:spent_usd

Flags / patterns:
  halt:* (checked: halt:manual, halt:infra, infra:halt, halt:daily_loss)
  route:dex_used:* / route:dex_watchdog:*
  playbook:lever:*:value / :changes / :last_change_ts
"""

import asyncio
import contextlib
import os
import re
import signal
import time
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────
try:
    from logging_setup import get_logger  # type: ignore
except Exception:  # pragma: no cover
    import logging

    def get_logger(name: str | None = None):  # type: ignore
        return logging.getLogger(name or "metrics_exporter")


log = get_logger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Optional Redis plumbing
# ──────────────────────────────────────────────────────────────────────────────
try:
    import redis.asyncio as aioredis  # type: ignore
except Exception:  # pragma: no cover
    aioredis = None

# Prefer your canonical redis getter if present
try:
    from utils_pkg.core_utils import get_redis_async as _get_redis_async  # type: ignore
except Exception:  # pragma: no cover
    _get_redis_async = None  # type: ignore


# ──────────────────────────────────────────────────────────────────────────────
# Env parsing
# ──────────────────────────────────────────────────────────────────────────────
_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _b(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return default


def _s(name: str, default: str) -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()


def _i(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    try:
        return int(str(v).strip())
    except Exception:
        return default


def _f(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    try:
        return float(str(v).strip())
    except Exception:
        return default


def _sanitize_prefix(p: str) -> str:
    p = (p or "").strip()
    if not p:
        return ""
    # Prometheus metric name regex: [a-zA-Z_:][a-zA-Z0-9_:]*
    # We'll restrict prefix to [a-zA-Z_][a-zA-Z0-9_]* plus trailing underscore recommended
    p = re.sub(r"[^a-zA-Z0-9_]", "_", p)
    if not re.match(r"^[a-zA-Z_]", p):
        p = "_" + p
    return p


# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class ExporterConfig:
    enabled: bool
    addr: str
    port: int
    poll_sec: float
    redis_url: str
    metrics_prefix: str
    scan_limit: int
    stale_ttl_sec: int
    log_every_sec: int

    # key overrides
    key_equity_hwm: str
    key_winrate20: str
    key_rolling_dd: str
    key_trades_total: str
    key_wins_total: str
    key_losses_total: str
    key_candidates_seen: str
    key_passed_gate: str
    key_buys_total: str
    key_sells_total: str

    # daily prefixes
    pnl_prefix: str
    budget_prefix: str


def load_config() -> ExporterConfig:
    prefix = _sanitize_prefix(_s("METRICS_PREFIX", "bot_"))
    return ExporterConfig(
        enabled=_b("METRICS_ENABLED", True),
        addr=_s("METRICS_ADDR", "0.0.0.0"),
        port=_i("METRICS_PORT", 9102),
        poll_sec=max(1.0, _f("EXPORTER_POLL_SEC", 10.0)),
        redis_url=_s("REDIS_URL", "redis://localhost:6379"),
        metrics_prefix=prefix,
        scan_limit=max(100, _i("EXPORTER_SCAN_LIMIT", 5000)),
        stale_ttl_sec=max(60, _i("EXPORTER_STALE_TTL_SEC", 1800)),
        log_every_sec=max(5, _i("EXPORTER_LOG_EVERY_SEC", 60)),
        key_equity_hwm=_s("METRICS_KEY_EQUITY_HWM", "metrics:equity_hwm_usd"),
        key_winrate20=_s("METRICS_KEY_WINRATE20", "metrics:winrate20"),
        key_rolling_dd=_s("METRICS_KEY_ROLLING_DD", "metrics:rolling_dd_pct"),
        key_trades_total=_s("METRICS_KEY_TRADES_TOTAL", "metrics:trades_total"),
        key_wins_total=_s("METRICS_KEY_WINS_TOTAL", "metrics:wins_total"),
        key_losses_total=_s("METRICS_KEY_LOSSES_TOTAL", "metrics:losses_total"),
        key_candidates_seen=_s("METRICS_KEY_CANDIDATES_SEEN", "metrics:candidates_seen_total"),
        key_passed_gate=_s("METRICS_KEY_PASSED_GATE", "metrics:passed_gate_total"),
        key_buys_total=_s("METRICS_KEY_BUYS_TOTAL", "metrics:buys_total"),
        key_sells_total=_s("METRICS_KEY_SELLS_TOTAL", "metrics:sells_total"),
        pnl_prefix=_s("PNL_PREFIX", "pnl:day:realized_usd"),
        budget_prefix=_s("BUDGET_KEY_PREFIX", "budget:sniper:spent_usd"),
    )


# ──────────────────────────────────────────────────────────────────────────────
# Key helpers
# ──────────────────────────────────────────────────────────────────────────────
def _today_tag_utc() -> str:
    now = datetime.now(UTC)
    return f"{now.year:04d}{now.month:02d}{now.day:02d}"


def _daily_key(prefix: str) -> str:
    return f"{prefix}:{_today_tag_utc()}"


def _worker_state(enabled_env: str, shadow_env: str) -> int:
    """
    0=off, 1=shadow, 2=live
    """
    en = _s(enabled_env, "true").strip().lower() in _TRUE
    sh = _s(shadow_env, "false").strip().lower() in _TRUE
    if not en:
        return 0
    return 1 if sh else 2


# ──────────────────────────────────────────────────────────────────────────────
# Prometheus wiring (lazy import: only required if exporter actually runs)
# ──────────────────────────────────────────────────────────────────────────────
def _require_prometheus():
    try:
        from prometheus_client import (
            CollectorRegistry,
            Counter,
            Gauge,
            Histogram,
            start_http_server,
        )

        return Counter, Gauge, Histogram, CollectorRegistry, start_http_server
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "prometheus_client is required for metrics exporter. "
            "Install with: pip install prometheus-client"
        ) from e


# ──────────────────────────────────────────────────────────────────────────────
# Exporter implementation
# ──────────────────────────────────────────────────────────────────────────────
class MetricsExporter:
    def __init__(self, cfg: ExporterConfig):
        self.cfg = cfg

        Counter, Gauge, Histogram, CollectorRegistry, _start_http_server = _require_prometheus()
        self._start_http_server = _start_http_server

        self.registry = CollectorRegistry(auto_describe=True)

        # metric name helper
        def m(suffix: str) -> str:
            p = self.cfg.metrics_prefix
            return f"{p}{suffix}" if p else suffix

        # Exporter health
        self.m_errors = Counter(
            m("exporter_errors_total"),
            "Exporter collection errors (total)",
            registry=self.registry,
        )
        self.g_redis_up = Gauge(
            m("exporter_redis_up"),
            "Exporter redis connectivity (1=up)",
            registry=self.registry,
        )
        self.g_last_collect_ts = Gauge(
            m("exporter_last_collect_ts"),
            "Unix timestamp of last successful collect",
            registry=self.registry,
        )
        self.g_loop_lag_sec = Gauge(
            m("exporter_loop_lag_seconds"),
            "Loop lag vs poll interval (seconds)",
            registry=self.registry,
        )
        self.h_collect_sec = Histogram(
            m("exporter_collect_seconds"),
            "Exporter collect duration (seconds)",
            registry=self.registry,
        )

        # Bot PnL truth / gauges
        self.g_equity_hwm = Gauge(
            m("equity_hwm_usd"), "High-water mark equity (USD)", registry=self.registry
        )
        self.g_winrate20 = Gauge(
            m("winrate20"), "Rolling win-rate over last 20 trades (0..1)", registry=self.registry
        )
        self.g_rolling_dd = Gauge(
            m("rolling_drawdown"), "Rolling drawdown vs HWM (0..1)", registry=self.registry
        )
        self.g_realized = Gauge(
            m("realized_pnl_today_usd"), "Realized PnL for today (USD)", registry=self.registry
        )
        self.g_budget = Gauge(
            m("spend_today_usd"), "Spend counted toward daily budget (USD)", registry=self.registry
        )
        self.g_infrahalt = Gauge(
            m("infra_halt"), "Infra halt flag (1=halt)", registry=self.registry
        )

        # Absolute->delta counters (cumulative)
        self.c_trades = Counter(m("trades_total"), "Total trades observed", registry=self.registry)
        self.c_wins = Counter(m("wins_total"), "Total wins observed", registry=self.registry)
        self.c_losses = Counter(m("losses_total"), "Total losses observed", registry=self.registry)

        self.c_candidates = Counter(
            m("candidates_seen_total"), "Candidates seen (total)", registry=self.registry
        )
        self.c_passed_gate = Counter(
            m("passed_gate_total"), "Candidates passed gate (total)", registry=self.registry
        )
        self.c_buys = Counter(m("buys_total"), "Buy executions (total)", registry=self.registry)
        self.c_sells = Counter(m("sells_total"), "Sell executions (total)", registry=self.registry)

        # Worker states
        self.g_worker_state = Gauge(
            m("worker_state"),
            "Worker state: 0=off,1=shadow,2=live",
            ["worker"],
            registry=self.registry,
        )

        # Router / route safety
        self.c_route_used = Counter(
            m("route_used_total"),
            "Route usage count by DEX (cumulative)",
            ["dex"],
            registry=self.registry,
        )
        self.c_route_watchdog = Counter(
            m("route_watchdog_hits_total"),
            "Watchdog hits by DEX (cumulative)",
            ["dex"],
            registry=self.registry,
        )

        # Playbook levers
        self.g_lever_value = Gauge(
            m("lever_value"),
            "Current lever numeric value (if parseable; else 0)",
            ["lever"],
            registry=self.registry,
        )
        self.g_lever_value_text = Gauge(
            m("lever_value_text"),
            "Current lever string value (stored as label; old values are removed per lever)",
            ["lever", "value"],
            registry=self.registry,
        )
        self.g_lever_last_change = Gauge(
            m("lever_last_change"),
            "Unix timestamp of last lever change",
            ["lever"],
            registry=self.registry,
        )
        self.c_lever_changes = Counter(
            m("lever_change_total"),
            "Total lever change events",
            ["lever"],
            registry=self.registry,
        )

        # runtime state
        self._redis = None
        self._last_abs: dict[str, float] = {}
        self._last_seen_label_ts: dict[tuple[str, str, str], float] = {}
        self._lever_last_str: dict[str, str] = {}
        self._last_redis_up: float = 0.0
        self._stop_event = asyncio.Event()

    # ──────────────────────────────────────────────────────────────────────
    # Redis client
    # ──────────────────────────────────────────────────────────────────────
    async def _get_redis(self):
        if self._redis is not None:
            return self._redis

        # Prefer canonical core_utils if available (preferred)
        if _get_redis_async is not None:
            try:
                self._redis = await _get_redis_async()
                return self._redis
            except Exception as e:
                log.warning("exporter redis: core_utils.get_redis_async failed: %s", e)

        if aioredis is None:
            self._redis = None
            return None

        try:
            self._redis = aioredis.from_url(self.cfg.redis_url, decode_responses=True)
            return self._redis
        except Exception as e:
            log.warning("exporter redis: from_url failed: %s", e)
            self._redis = None
            return None

    async def _read_float(self, r, key: str, default: float = 0.0) -> float:
        try:
            v = await r.get(key)
            if v is None:
                return default
            return float(v)
        except Exception:
            return default

    async def _read_str(self, r, key: str) -> str | None:
        try:
            v = await r.get(key)
            return None if v is None else str(v)
        except Exception:
            return None

    # ──────────────────────────────────────────────────────────────────────
    # Prometheus counter delta helpers
    # ──────────────────────────────────────────────────────────────────────
    def _counter_delta(self, abs_key: str, new_abs: float) -> float:
        """
        Prometheus Counters can only increase.
        Redis stores absolute totals — we convert to deltas locally.

        First observation returns 0.0 (we don't backfill history).
        """
        old = self._last_abs.get(abs_key)
        self._last_abs[abs_key] = float(new_abs)
        if old is None:
            return 0.0
        d = float(new_abs) - float(old)
        return d if d > 0 else 0.0

    def _mark_label_seen(self, family: str, a: str, b: str = "") -> None:
        self._last_seen_label_ts[(family, a, b)] = time.time()

    def _prune_stale_labels(self) -> None:
        ttl = float(self.cfg.stale_ttl_sec)
        if ttl <= 0:
            return
        now = time.time()
        stale = [k for k, ts in self._last_seen_label_ts.items() if (now - ts) > ttl]
        for fam, a, b in stale:
            try:
                if fam == "route_used":
                    self.c_route_used.remove(a)
                elif fam == "route_watchdog":
                    self.c_route_watchdog.remove(a)
                elif fam == "lever_value":
                    self.g_lever_value.remove(a)
                elif fam == "lever_value_text":
                    self.g_lever_value_text.remove(a, b)
                elif fam == "lever_last_change":
                    self.g_lever_last_change.remove(a)
                elif fam == "lever_changes":
                    self.c_lever_changes.remove(a)
            except Exception:
                pass
            self._last_seen_label_ts.pop((fam, a, b), None)

    async def _scan_keys(self, r, match: str, limit: int) -> Iterable[str]:
        n = 0
        async for key in r.scan_iter(match=match):
            yield key
            n += 1
            if n >= limit:
                break

    # ──────────────────────────────────────────────────────────────────────
    # Collection
    # ──────────────────────────────────────────────────────────────────────
    async def collect_once(self) -> None:
        r = await self._get_redis()
        if r is None:
            self._last_redis_up = 0.0
            self.g_redis_up.set(0.0)
            return

        # connectivity
        try:
            pong = await r.ping()
            self._last_redis_up = 1.0 if pong else 0.0
            self.g_redis_up.set(self._last_redis_up)
        except Exception:
            self._last_redis_up = 0.0
            self.g_redis_up.set(0.0)
            return

        # Gauges (direct reads)
        self.g_equity_hwm.set(await self._read_float(r, self.cfg.key_equity_hwm, 0.0))
        self.g_winrate20.set(await self._read_float(r, self.cfg.key_winrate20, 0.0))
        self.g_rolling_dd.set(await self._read_float(r, self.cfg.key_rolling_dd, 0.0))
        self.g_realized.set(await self._read_float(r, _daily_key(self.cfg.pnl_prefix), 0.0))
        self.g_budget.set(await self._read_float(r, _daily_key(self.cfg.budget_prefix), 0.0))

        # Halt flags (any non-zero)
        halted = 0.0
        for k in ("halt:manual", "halt:infra", "infra:halt", "halt:daily_loss"):
            v = await self._read_float(r, k, 0.0)
            if v not in (0.0, -0.0):
                halted = 1.0
                break
        self.g_infrahalt.set(halted)

        # Core counters from absolute totals
        core_abs = {
            self.cfg.key_trades_total: (self.c_trades, 0.0),
            self.cfg.key_wins_total: (self.c_wins, 0.0),
            self.cfg.key_losses_total: (self.c_losses, 0.0),
            self.cfg.key_candidates_seen: (self.c_candidates, 0.0),
            self.cfg.key_passed_gate: (self.c_passed_gate, 0.0),
            self.cfg.key_buys_total: (self.c_buys, 0.0),
            self.cfg.key_sells_total: (self.c_sells, 0.0),
        }
        for abs_key, (ctr, default) in core_abs.items():
            if not abs_key:
                continue
            new_abs = await self._read_float(r, abs_key, default)
            d = self._counter_delta(abs_key, new_abs)
            if d:
                ctr.inc(d)

        # Worker states (env-based)
        self.g_worker_state.labels("scalper").set(
            _worker_state("DAYTRADER_ENABLED", "SCALPER_SHADOW_MODE")
        )
        self.g_worker_state.labels("whales").set(
            _worker_state("WHALE_FEED_ENABLED", "WHALE_SHADOW_MODE")
        )
        self.g_worker_state.labels("velocity").set(
            _worker_state("VELOCITY_ENABLED", "VELOCITY_SHADOW_MODE")
        )
        self.g_worker_state.labels("hotlist").set(
            _worker_state("HOTLIST_ENABLED", "HOTLIST_SHADOW_MODE")
        )
        self.g_worker_state.labels("sniper").set(
            _worker_state("SNIPER_ENABLED", "SNIPER_SHADOW_MODE")
        )

        # Routes (counters per dex) - absolute totals in redis -> delta here
        async for key in self._scan_keys(r, "route:dex_used:*", self.cfg.scan_limit):
            dex = key.split(":")[-1]
            abs_val = await self._read_float(r, key, 0.0)
            delta = self._counter_delta(key, abs_val)
            if delta:
                self.c_route_used.labels(dex).inc(delta)
            self._mark_label_seen("route_used", dex)

        async for key in self._scan_keys(r, "route:dex_watchdog:*", self.cfg.scan_limit):
            dex = key.split(":")[-1]
            abs_val = await self._read_float(r, key, 0.0)
            delta = self._counter_delta(key, abs_val)
            if delta:
                self.c_route_watchdog.labels(dex).inc(delta)
            self._mark_label_seen("route_watchdog", dex)

        # Playbook levers (bounded scan)
        async for key in self._scan_keys(r, "playbook:lever:*:value", self.cfg.scan_limit):
            parts = key.split(":")
            # playbook:lever:<name>:value
            if len(parts) < 4:
                continue
            lever = parts[2]
            raw = (await self._read_str(r, key)) or ""
            raw = raw.strip()

            # numeric gauge
            try:
                num = float(raw)
            except Exception:
                num = 0.0
            self.g_lever_value.labels(lever).set(num)
            self._mark_label_seen("lever_value", lever)

            # text label gauge: remove previous value for this lever
            prev = self._lever_last_str.get(lever)
            if prev is not None and prev != raw:
                with contextlib.suppress(Exception):
                    self.g_lever_value_text.remove(lever, prev)
            self.g_lever_value_text.labels(lever, raw).set(1.0)
            self._lever_last_str[lever] = raw
            self._mark_label_seen("lever_value_text", lever, raw)

            # lever change counter from absolute
            abs_changes = await self._read_float(r, f"playbook:lever:{lever}:changes", 0.0)
            dch = self._counter_delta(f"playbook:lever:{lever}:changes", abs_changes)
            if dch:
                self.c_lever_changes.labels(lever).inc(dch)
            self._mark_label_seen("lever_changes", lever)

            # last change ts
            ts = await self._read_float(r, f"playbook:lever:{lever}:last_change_ts", 0.0)
            self.g_lever_last_change.labels(lever).set(ts)
            self._mark_label_seen("lever_last_change", lever)

        self._prune_stale_labels()
        self.g_last_collect_ts.set(time.time())

    # ──────────────────────────────────────────────────────────────────────
    # Lifecycle
    # ──────────────────────────────────────────────────────────────────────
    def request_stop(self) -> None:
        self._stop_event.set()

    async def run(self) -> None:
        if not self.cfg.enabled:
            log.info("metrics exporter disabled (METRICS_ENABLED=0)")
            return

        # Start HTTP server
        self._start_http_server(self.cfg.port, addr=self.cfg.addr, registry=self.registry)
        log.info("Prometheus exporter listening on %s:%s", self.cfg.addr, self.cfg.port)

        # Signal handling (Render sends SIGTERM)
        try:
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop.add_signal_handler(sig, self.request_stop)
                except Exception:
                    # not supported on some platforms / event loops
                    pass
        except Exception:
            pass

        poll = float(self.cfg.poll_sec)
        log_every = float(self.cfg.log_every_sec)
        last_heartbeat = 0.0
        next_deadline = time.monotonic()

        while not self._stop_event.is_set():
            t0 = time.monotonic()
            next_deadline = max(next_deadline + poll, t0)

            try:
                with self.h_collect_sec.time():
                    await self.collect_once()
            except Exception as e:
                self.m_errors.inc(1.0)
                log.error("[exporter] collect error: %s", e, exc_info=True)

            # loop lag
            t1 = time.monotonic()
            lag = max(0.0, t1 - next_deadline)
            self.g_loop_lag_sec.set(lag)

            now = time.time()
            if (now - last_heartbeat) >= log_every:
                last_heartbeat = now
                log.info(
                    "exporter heartbeat",
                    extra={
                        "poll_sec": poll,
                        "loop_lag_sec": lag,
                        "redis_up": self._last_redis_up,
                        "scan_limit": self.cfg.scan_limit,
                    },
                )

            # sleep until next deadline (or stop)
            sleep_for = max(0.0, next_deadline - time.monotonic())
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop_event.wait(), timeout=sleep_for)

        log.info("exporter stopping (stop requested)")


# ──────────────────────────────────────────────────────────────────────────────
# Entrypoint
# ──────────────────────────────────────────────────────────────────────────────
async def run_exporter() -> None:
    cfg = load_config()
    ex = MetricsExporter(cfg)
    await ex.run()


def main() -> None:
    asyncio.run(run_exporter())


if __name__ == "__main__":
    main()
