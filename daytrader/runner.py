# daytrader/runner.py
from __future__ import annotations

"""
daytrader.runner
----------------
Single, canonical orchestration loop for the *Daytrader* subsystem.

Key contract (God-Tier Overhaul: no competing engines)
- Daytrader-only; must NOT silently compete with sniper path.
- Safe to import; safe to run; fails closed on missing runtime deps.
- Uses:
    - daytrader.data_feed  (market data, cached, async)
    - daytrader.broker     (execution bridge -> core_utils.swap_token_jupiter)
    - daytrader.position   (daytrader positions on disk)
    - daytrader.risk       (portfolio gates + sizing + trailing rules)
    - daytrader.features/daytrader.alpha (optional; runner remains robust if absent)

Pairs & config (env)
- DT_PAIRS                e.g. "SOL/USDC,JUP/USDC" (default "SOL/USDC")
- DT_INTERVAL_SEC         bar interval seconds (default 60)
- DT_LOOKBACK             bars to fetch (default 240)
- DT_POLL_SEC             loop cadence seconds (default 20)
- DT_SLIPPAGE             default slippage (optional; if unset uses env SLIPPAGE fallback)
- DT_MIN_BARS             minimum bars needed (default 80)
- DT_MAX_PARALLEL_FETCH   concurrent pair ticks (default 3)

Sizing / execution
- Uses daytrader.risk.RiskManager to compute size_frac when equity can be inferred.
- Buys require either:
    (a) spendable quote balance (best-effort), OR
    (b) DT_FIXED_QUOTE_PER_TRADE > 0
- DT_QUOTE_RESERVE_FRAC        keep fraction of quote unspent (default 0.05)
- DT_FIXED_QUOTE_PER_TRADE     fixed quote buy amount when equity unknown (default 0)

Take-profit partials (optional)
- DT_TP_PARTIAL_FRAC           e.g. 0.5 sells half on TP hit (default 0 = full exit)

Notes about prices
- data_feed returns USD-denominated bars for a mint.
- For BASE/USDC, USD ~= USDC so bars are directly usable.
- For BASE/QUOTE (non-USDC), we derive QUOTE bars by dividing base USD bars by quote USD bars.

CLI
- python -m daytrader.runner --pairs "SOL/USDC,JUP/USDC" --once
"""

import asyncio
import contextlib
import os
import signal
import time
from dataclasses import dataclass
from typing import Any, Final

# ----------------------------- logging ----------------------------------------

try:
    from logging_setup import get_logger  # type: ignore

    log = get_logger(__name__)  # type: ignore
except Exception:  # pragma: no cover
    try:
        from utils_pkg.core_utils import logger as log  # type: ignore
    except Exception:  # pragma: no cover
        import logging

        log = logging.getLogger("daytrader.runner")
        if not log.handlers:
            logging.basicConfig(level=logging.INFO)


def _log_info(msg: str, **kw: Any) -> None:
    try:
        # Prefer structured logger if present
        log.event(msg, **kw)  # type: ignore[attr-defined]
        return
    except Exception:
        pass
    try:
        log.info("%s %s", msg, kw if kw else "")
    except Exception:
        print(msg)


def _log_warn(msg: str, **kw: Any) -> None:
    try:
        log.event(msg, **kw)  # type: ignore[attr-defined]
        return
    except Exception:
        pass
    try:
        log.warning("%s %s", msg, kw if kw else "")
    except Exception:
        print(msg)


def _log_exc(msg: str, **kw: Any) -> None:
    try:
        log.exception("%s %s", msg, kw if kw else "")
    except Exception:
        print(msg)


# ----------------------------- required deps ----------------------------------

from daytrader.position import (  # type: ignore
    close_position,
    list_positions,
    load_position,
    new_position,
    save_position,
)
from daytrader.risk import RiskManager, risk_summary  # type: ignore

try:
    from daytrader.risk import should_move_to_break_even, update_trailing_stop  # type: ignore
except Exception:  # pragma: no cover
    should_move_to_break_even = None  # type: ignore
    update_trailing_stop = None  # type: ignore

# Execution bridge (canonical for Daytrader)
try:
    from daytrader.broker import (  # type: ignore
        TOKENS as BROKER_TOKENS,
        place_order as broker_place_order,
    )
except Exception as err:  # pragma: no cover
    broker_place_order = None  # type: ignore
    BROKER_TOKENS = {}  # type: ignore
    _log_warn("[daytrader.runner] broker import failed", err=str(err))

# Market data (canonical for Daytrader)
try:
    from daytrader.data_feed import MINTS as FEED_MINTS, get_minute_bars  # type: ignore
except Exception as err:  # pragma: no cover
    get_minute_bars = None  # type: ignore
    FEED_MINTS = {}  # type: ignore
    _log_warn("[daytrader.runner] data_feed import failed", err=str(err))

# Optional decision engine (best-effort; keep compatibility)
RulesEngineStrategy = None
StrategyDirector = None
StrategyConfig = None
TradeIntent = None
AlphaModel = None

try:
    # Newer canonical names (your current features.py provides these)
    from daytrader.features import (
        RulesEngineStrategy as _RES,
        StrategyConfig as _SC,
        TradeIntent as _TI,
    )  # type: ignore

    RulesEngineStrategy, StrategyConfig, TradeIntent = _RES, _SC, _TI
except Exception:
    pass

try:
    # Legacy name (if it exists in some branches)
    from daytrader.features import StrategyDirector as _SD  # type: ignore

    StrategyDirector = _SD
except Exception:
    pass

try:
    from daytrader.alpha import AlphaModel as _AM  # type: ignore

    AlphaModel = _AM
except Exception:
    pass

# Wallet/balances (best effort)
try:
    from utils_pkg.core_utils import get_wallet_balance  # type: ignore
except Exception:
    get_wallet_balance = None  # type: ignore

try:
    import utils_pkg.core_utils as _cu  # type: ignore
except Exception:
    _cu = None  # type: ignore


# ----------------------------- env helpers ------------------------------------


def _get_str(name: str, default: str) -> str:
    raw = os.getenv(name)
    if raw is None:
        return default
    s = str(raw).strip()
    return default if s == "" else s


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        return int(float(raw))
    except Exception:
        return int(default)


def _get_float(name: str, default: float) -> float:
    raw = os.getenv(name, str(default))
    try:
        return float(raw)
    except Exception:
        return float(default)


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        val = float(x)
        if val != val or val in (float("inf"), float("-inf")):
            return float(default)
        return float(val)
    except Exception:
        return float(default)


def _safe_float_opt(x: Any) -> float | None:
    if x in (None, "", "None"):
        return None
    try:
        val = float(x)
        if val != val or val in (float("inf"), float("-inf")):
            return None
        return float(val)
    except Exception:
        return None


def _clamp(x: float, lo: float, hi: float) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _clamp_int(x: int, lo: int, hi: int) -> int:
    v = int(x)
    if v < lo:
        return lo
    if v > hi:
        return hi
    return v


# ----------------------------- config -----------------------------------------


@dataclass(frozen=True)
class PairSpec:
    pair: str
    base_sym: str
    quote_sym: str
    base_mint: str
    quote_mint: str
    interval_sec: int
    lookback: int


@dataclass
class RunnerSettings:
    poll_sec: int = 20
    min_bars: int = 80
    slippage: float | None = None
    quote_reserve_frac: float = 0.05
    fixed_quote_per_trade: float = 0.0
    tp_partial_frac: float = 0.0
    max_parallel_fetch: int = 3
    fetch_timeout_sec: float = 20.0
    exec_timeout_sec: float = 45.0


def _parse_pair(pair_text: str) -> tuple[str, str]:
    if "/" not in pair_text:
        raise ValueError(f"pair must be BASE/QUOTE, got '{pair_text}'")
    left, right = pair_text.split("/", 1)
    return left.strip().upper(), right.strip().upper()


def _mint_for_symbol(sym: str) -> str | None:
    entry = BROKER_TOKENS.get(sym)
    if isinstance(entry, dict):
        mint_val = str(entry.get("mint") or "").strip()
        return mint_val or None
    if sym in FEED_MINTS:
        mint_val2 = str(FEED_MINTS[sym]).strip()
        return mint_val2 or None
    return None


def _resolve_pairs() -> list[PairSpec]:
    pairs_raw = _get_str("DT_PAIRS", "SOL/USDC")
    interval = _get_int("DT_INTERVAL_SEC", 60)
    lookback = _get_int("DT_LOOKBACK", 240)

    specs: list[PairSpec] = []
    for raw_pair in [x.strip() for x in pairs_raw.split(",") if x.strip()]:
        base_sym, quote_sym = _parse_pair(raw_pair)
        base_mint = _mint_for_symbol(base_sym)
        quote_mint = _mint_for_symbol(quote_sym)
        if not base_mint or not quote_mint:
            raise RuntimeError(
                f"Unknown symbol in pair '{raw_pair}'. "
                f"Known broker={list(BROKER_TOKENS.keys())} feed={list(FEED_MINTS.keys())}"
            )

        specs.append(
            PairSpec(
                pair=f"{base_sym}/{quote_sym}",
                base_sym=base_sym,
                quote_sym=quote_sym,
                base_mint=base_mint,
                quote_mint=quote_mint,
                interval_sec=int(interval),
                lookback=int(lookback),
            )
        )
    return specs


def _settings() -> RunnerSettings:
    s = RunnerSettings()
    s.poll_sec = _clamp_int(_get_int("DT_POLL_SEC", 20), 1, 3600)
    s.min_bars = _clamp_int(_get_int("DT_MIN_BARS", 80), 10, 10_000)

    slip_raw = _get_str("DT_SLIPPAGE", "").strip()
    s.slippage = float(slip_raw) if slip_raw not in ("", "None") else None

    s.quote_reserve_frac = _clamp(_get_float("DT_QUOTE_RESERVE_FRAC", 0.05), 0.0, 0.90)
    s.fixed_quote_per_trade = max(0.0, _get_float("DT_FIXED_QUOTE_PER_TRADE", 0.0))
    s.tp_partial_frac = _clamp(_get_float("DT_TP_PARTIAL_FRAC", 0.0), 0.0, 0.95)
    s.max_parallel_fetch = _clamp_int(_get_int("DT_MAX_PARALLEL_FETCH", 3), 1, 10)

    s.fetch_timeout_sec = _clamp(_get_float("DT_FETCH_TIMEOUT_SEC", 20.0), 3.0, 180.0)
    s.exec_timeout_sec = _clamp(_get_float("DT_EXEC_TIMEOUT_SEC", 45.0), 5.0, 300.0)
    return s


# ----------------------------- bars / price -----------------------------------

Bar = dict[str, float]


def _normalize_bars(raw: list[dict[str, Any]]) -> list[Bar]:
    out: list[Bar] = []
    for b in raw or []:
        ts = int(_safe_float(b.get("t"), 0.0))
        opn = _safe_float(b.get("o"), 0.0)
        high_px = _safe_float(b.get("h"), 0.0)
        low_px = _safe_float(b.get("l"), 0.0)  # E741-safe: not named `l`
        close_px = _safe_float(b.get("c"), 0.0)
        vol = _safe_float(b.get("v"), 0.0)

        if ts <= 0 or close_px <= 0 or high_px <= 0 or low_px <= 0:
            continue

        out.append({"t": float(ts), "o": opn, "h": high_px, "l": low_px, "c": close_px, "v": vol})

    out.sort(key=lambda x: x["t"])
    return out


async def _fetch_mint_bars_usd(mint: str, limit: int, timeout: float) -> list[Bar]:
    if get_minute_bars is None:
        return []
    try:
        raw = await asyncio.wait_for(get_minute_bars(mint, limit=limit), timeout=timeout)  # type: ignore[misc]
        return _normalize_bars(raw)
    except Exception:
        return []


async def _build_pair_bars(spec: PairSpec, timeout: float) -> list[Bar]:
    limit: Final[int] = max(60, int(spec.lookback))

    if spec.quote_sym == "USDC":
        return await _fetch_mint_bars_usd(spec.base_mint, limit, timeout)

    base_task = asyncio.create_task(_fetch_mint_bars_usd(spec.base_mint, limit, timeout))
    quote_task = asyncio.create_task(_fetch_mint_bars_usd(spec.quote_mint, limit, timeout))
    base_bars, quote_bars = await asyncio.gather(base_task, quote_task)

    if not base_bars or not quote_bars:
        return []

    quote_by_ts: dict[int, Bar] = {int(b["t"]): b for b in quote_bars}

    out: list[Bar] = []
    for b in base_bars:
        ts = int(b["t"])
        qb = quote_by_ts.get(ts)
        if not qb:
            continue
        quote_close = qb["c"]
        if quote_close <= 0:
            continue

        out.append(
            {
                "t": float(ts),
                "o": b["o"] / quote_close,
                "h": b["h"] / quote_close,
                "l": b["l"] / quote_close,
                "c": b["c"] / quote_close,
                "v": b["v"],
            }
        )

    out.sort(key=lambda x: x["t"])
    return out


def _atr_abs(bars: list[Bar], period: int = 14) -> float | None:
    p = int(period)
    if p <= 1 or len(bars) < p + 1:
        return None

    true_ranges: list[float] = []
    for idx in range(1, len(bars)):
        prev_close = bars[idx - 1]["c"]
        high_px = bars[idx]["h"]
        low_px = bars[idx]["l"]  # E741-safe
        tr_val = max(high_px - low_px, abs(high_px - prev_close), abs(low_px - prev_close))
        if tr_val > 0:
            true_ranges.append(float(tr_val))

    if len(true_ranges) < p:
        return None
    return sum(true_ranges[-p:]) / float(p)


# ----------------------------- decisions (fallback) ----------------------------


@dataclass
class _FallbackIntent:
    side: str
    size_frac: float
    stop: float
    take_profit: float
    reason: str
    confidence: float


def _fallback_decide(pair: str, bars: list[Bar]) -> _FallbackIntent | None:
    if len(bars) < 80:
        return None

    closes = [b["c"] for b in bars]
    last_px = closes[-1]
    if last_px <= 0:
        return None

    def _sma(xs: list[float], n: int) -> float:
        n2 = max(1, min(int(n), len(xs)))
        return sum(xs[-n2:]) / float(n2)

    fast = _sma(closes, 12)
    slow = _sma(closes, 48)
    trend = 1.0 if fast > slow else -1.0

    ret10 = (closes[-1] / closes[-10] - 1.0) if closes[-10] > 0 else 0.0
    score = 0.55 * trend + 0.45 * _clamp(ret10 * 10.0, -1.0, 1.0)

    atr_abs = _atr_abs(bars, 14)
    if not atr_abs or atr_abs <= 0:
        return None

    conf = abs(score)
    if conf < 0.55:
        return None

    side = "buy" if score > 0 else "sell"
    size_frac = _clamp(0.10 + 0.25 * conf, 0.05, 0.35)

    if side == "buy":
        stop = max(1e-12, last_px - 1.5 * atr_abs)
        tp = max(1e-12, last_px + 3.0 * atr_abs)
    else:
        stop = max(1e-12, last_px + 1.5 * atr_abs)
        tp = max(1e-12, last_px - 3.0 * atr_abs)

    reason = f"fallback score={score:.2f} ret10={ret10:.2%} atr%={(atr_abs / last_px):.3f}"
    return _FallbackIntent(
        side=side,
        size_frac=size_frac,
        stop=stop,
        take_profit=tp,
        reason=reason,
        confidence=conf,
    )


# ----------------------------- balances ---------------------------------------


async def _get_quote_balance(quote_sym: str, quote_mint: str) -> float | None:
    if quote_sym == "SOL":
        if get_wallet_balance:
            try:
                v = await get_wallet_balance()
                return float(v)
            except Exception:
                return None
        return _safe_float_opt(os.getenv("DT_SOL_BAL_FALLBACK", ""))

    if _cu is None:
        return _safe_float_opt(os.getenv(f"DT_{quote_sym}_BAL_FALLBACK", ""))

    for fn_name in (
        "get_token_balance",
        "get_spl_balance",
        "get_token_balance_float",
        "get_spl_balance_float",
    ):
        fn = getattr(_cu, fn_name, None)
        if fn is None:
            continue
        try:
            v = fn(quote_mint)
            if asyncio.iscoroutine(v):
                v = await v
            return float(v)
        except Exception:
            continue

    return _safe_float_opt(os.getenv(f"DT_{quote_sym}_BAL_FALLBACK", ""))


# ----------------------------- runner core ------------------------------------


class DaytraderRunner:
    def __init__(self, specs: list[PairSpec], settings: RunnerSettings) -> None:
        if broker_place_order is None:
            raise RuntimeError(
                "daytrader.broker.place_order unavailable (execution bridge missing)"
            )
        if get_minute_bars is None:
            raise RuntimeError(
                "daytrader.data_feed.get_minute_bars unavailable (market data missing)"
            )

        self.specs = list(specs)
        self.settings = settings

        self.risk = RiskManager()
        self._exec_lock = asyncio.Lock()
        self._stop = asyncio.Event()

        self._high_water: dict[str, float] = {}

        # optional engines
        self._directors: dict[str, Any] = {}
        self._rule_engines: dict[str, Any] = {}

        self._alpha: Any | None = None
        self._alpha_fn: Any | None = None

        if AlphaModel is not None:
            with contextlib.suppress(Exception):
                self._alpha = AlphaModel()
                if hasattr(self._alpha, "score") and callable(self._alpha.score):
                    self._alpha_fn = lambda feats: float(self._alpha.score(feats))  # type: ignore[misc]
                elif callable(self._alpha):
                    self._alpha_fn = self._alpha

        # StrategyDirector (legacy) OR RulesEngineStrategy (current canonical)
        if StrategyDirector is not None and StrategyConfig is not None:
            with contextlib.suppress(Exception):
                cfg = StrategyConfig()
                for sp in self.specs:
                    self._directors[sp.pair] = StrategyDirector(  # type: ignore[misc]
                        broker=None, risk_manager=None, cfg=cfg, alpha=self._alpha
                    )

        if not self._directors and RulesEngineStrategy is not None:
            with contextlib.suppress(Exception):
                cfg = StrategyConfig() if StrategyConfig is not None else None
                for sp in self.specs:
                    self._rule_engines[sp.pair] = (
                        RulesEngineStrategy(cfg) if cfg is not None else RulesEngineStrategy()
                    )  # type: ignore[misc]

    def request_stop(self) -> None:
        self._stop.set()

    async def _execute(self, pair: str, side: str, amount_input: float, reason: str) -> str | None:
        amt = float(amount_input)
        if amt <= 0.0:
            return None

        slip = self.settings.slippage
        if slip is None:
            slip = _get_float("SLIPPAGE", 0.005)

        async with self._exec_lock:
            try:
                res = broker_place_order(  # type: ignore[misc]
                    pair=pair,
                    side=side,
                    amount_input=amt,
                    slippage=float(slip),
                    reason=str(reason or ""),
                )
                if asyncio.iscoroutine(res):
                    res = await asyncio.wait_for(res, timeout=float(self.settings.exec_timeout_sec))
                if isinstance(res, tuple) and len(res) >= 2:
                    ok, sig = res[0], res[1]
                    return str(sig) if ok else None
                if isinstance(res, str) and res.strip():
                    return res.strip()
                return None
            except TypeError:
                try:
                    res2 = broker_place_order(pair, side, amt, float(slip), str(reason or ""))  # type: ignore[misc]
                    if asyncio.iscoroutine(res2):
                        res2 = await asyncio.wait_for(
                            res2, timeout=float(self.settings.exec_timeout_sec)
                        )
                    if isinstance(res2, tuple) and len(res2) >= 2:
                        ok2, sig2 = res2[0], res2[1]
                        return str(sig2) if ok2 else None
                    if isinstance(res2, str) and res2.strip():
                        return res2.strip()
                    return None
                except Exception:
                    _log_warn("[daytrader] execute failed", pair=pair, side=side)
                    return None
            except Exception:
                _log_warn("[daytrader] execute failed", pair=pair, side=side)
                return None

    async def _tick_pair(self, spec: PairSpec) -> None:
        pair = spec.pair
        base_mint = spec.base_mint

        bars = await _build_pair_bars(spec, timeout=float(self.settings.fetch_timeout_sec))
        if len(bars) < max(60, self.settings.min_bars):
            return

        last_px = bars[-1]["c"]
        if last_px <= 0.0:
            return

        pos = load_position(base_mint)
        is_flat = bool(getattr(pos, "is_flat", True)) if pos else True

        # ---------------- manage open position ----------------
        if pos and not is_flat:
            with contextlib.suppress(Exception):
                pos.mark(last_px)

            entry_px = _safe_float(getattr(pos, "entry_sol", 0.0), 0.0) or last_px
            hi = self._high_water.get(base_mint, entry_px)
            hi = max(hi, last_px)
            self._high_water[base_mint] = hi

            atr_abs = _atr_abs(bars, 14)
            if (
                atr_abs
                and atr_abs > 0
                and callable(should_move_to_break_even)
                and callable(update_trailing_stop)
            ):
                with contextlib.suppress(Exception):
                    stop_now = _safe_float(getattr(pos, "stop_sol", 0.0), 0.0)
                    if should_move_to_break_even(entry_px, last_px, atr_abs):  # type: ignore[misc]
                        if stop_now < entry_px:
                            pos.stop_sol = entry_px
                            stop_now = entry_px
                    pos.stop_sol = update_trailing_stop(stop_now, hi, atr_abs, side="buy")  # type: ignore[misc]

            stop_px = _safe_float(getattr(pos, "stop_sol", 0.0), 0.0)
            tp_px = _safe_float(getattr(pos, "tp_sol", 0.0), 0.0)

            hit_stop = stop_px > 0.0 and last_px <= stop_px
            hit_tp = tp_px > 0.0 and last_px >= tp_px

            if hit_stop or hit_tp:
                qty_total = _safe_float(getattr(pos, "qty_token", 0.0), 0.0)
                if qty_total <= 0.0:
                    close_position(base_mint)
                    return

                sell_qty = qty_total
                if hit_tp and self.settings.tp_partial_frac > 0.0:
                    sell_qty = max(0.0, qty_total * float(self.settings.tp_partial_frac))

                sig = await self._execute(
                    pair, "sell", sell_qty, reason=("tp" if hit_tp else "stop")
                )
                if sig:
                    with contextlib.suppress(Exception):
                        pos.add_fill("sell", sell_qty, last_px, fee_sol=0.0)

                    if (
                        hit_stop
                        or sell_qty >= qty_total - 1e-12
                        or self.settings.tp_partial_frac <= 0.0
                    ):
                        close_position(base_mint)
                    else:
                        with contextlib.suppress(Exception):
                            save_position(pos)
                return

            # persist trailing updates
            with contextlib.suppress(Exception):
                save_position(pos)

        # ---------------- decide intent ----------------
        intent_obj: Any | None = None

        director = self._directors.get(pair)
        if director is not None:
            with contextlib.suppress(Exception):
                if hasattr(director, "evaluate"):
                    intent_obj = director.evaluate(pair, bars)
                elif hasattr(director, "decide"):
                    intent_obj = director.decide(pair, bars)

        if intent_obj is None:
            engine = self._rule_engines.get(pair)
            if engine is not None and hasattr(engine, "decide"):
                with contextlib.suppress(Exception):
                    intent_obj = engine.decide(pair, bars, alpha_score=self._alpha_fn)  # type: ignore[misc]

        if intent_obj is None:
            intent_obj = _fallback_decide(pair, bars)

        if intent_obj is None:
            return

        side = str(getattr(intent_obj, "side", "")).lower().strip()
        conf = _safe_float(getattr(intent_obj, "confidence", 0.0), 0.0)
        if conf < 0.60:
            return

        # ---------------- exit intent (risk-off) ----------------
        if pos and not is_flat and side == "sell":
            qty_total = _safe_float(getattr(pos, "qty_token", 0.0), 0.0)
            if qty_total <= 0.0:
                close_position(base_mint)
                return

            sig = await self._execute(
                pair,
                "sell",
                qty_total,
                reason=str(getattr(intent_obj, "reason", "signal_sell")),
            )
            if sig:
                with contextlib.suppress(Exception):
                    pos.add_fill("sell", qty_total, last_px, fee_sol=0.0)
                close_position(base_mint)
            return

        # ---------------- entry intent ----------------
        if (pos is None or is_flat) and side == "buy":
            quote_bal = await _get_quote_balance(spec.quote_sym, spec.quote_mint)

            spendable: float | None = None
            if quote_bal is not None and quote_bal > 0.0:
                spendable = max(
                    0.0, float(quote_bal) * (1.0 - float(self.settings.quote_reserve_frac))
                )

            # compute opens + rough exposure (best-effort)
            opens = 0
            gross_exposure = 0.0
            with contextlib.suppress(Exception):
                for pos_item in list_positions():
                    if not pos_item:
                        continue
                    if bool(getattr(pos_item, "is_flat", True)):
                        continue
                    opens += 1
                    v = getattr(pos_item, "size_value_sol", None)
                    if callable(v):
                        vv = v()
                        if vv:
                            gross_exposure += float(vv)

            # Risk refine (best effort; may reject)
            eq_for_risk = 0.0
            if spendable is not None and spendable > 0.0:
                eq_for_risk = spendable
            elif quote_bal is not None and quote_bal > 0.0:
                eq_for_risk = float(quote_bal)

            refined = None
            with contextlib.suppress(Exception):
                refined = self.risk.refine_intent(
                    pair,
                    bars,
                    intent_obj,
                    equity_quote=(eq_for_risk if eq_for_risk > 0.0 else None),
                    equity_peak_quote=None,
                    open_positions=opens,
                    gross_exposure_quote=gross_exposure,
                    day_realized_pnl_quote=0.0,
                )
            if refined is None:
                return
            intent_obj = refined

            size_frac = _clamp(_safe_float(getattr(intent_obj, "size_frac", 0.0), 0.0), 0.0, 1.0)

            quote_amount = 0.0
            if spendable is not None and spendable > 0.0 and size_frac > 0.0:
                quote_amount = spendable * size_frac
            elif self.settings.fixed_quote_per_trade > 0.0:
                quote_amount = float(self.settings.fixed_quote_per_trade)

            if quote_amount <= 0.0:
                return
            if spendable is not None and quote_amount > spendable:
                quote_amount = spendable

            reason = str(getattr(intent_obj, "reason", "buy"))
            sig = await self._execute(pair, "buy", quote_amount, reason=reason)
            if not sig:
                return

            est_qty = quote_amount / max(1e-12, last_px)

            stop = _safe_float(getattr(intent_obj, "stop", 0.0), 0.0)
            tp = _safe_float(getattr(intent_obj, "take_profit", 0.0), 0.0)

            new_pos_obj = new_position(
                mint=base_mint,
                symbol=pair,
                qty_token=est_qty,
                entry_sol=last_px,
                stop_sol=(stop if stop > 0.0 else 0.0),
                tp_sol=(tp if tp > 0.0 else 0.0),
                side="long",
                leverage=1.0,
            )
            with contextlib.suppress(Exception):
                new_pos_obj.add_fill("buy", est_qty, last_px, fee_sol=0.0)
            with contextlib.suppress(Exception):
                save_position(new_pos_obj)

            self._high_water[base_mint] = last_px

    async def run_once(self) -> None:
        sem = asyncio.Semaphore(int(self.settings.max_parallel_fetch))

        async def _wrap(sp: PairSpec) -> None:
            async with sem:
                try:
                    await self._tick_pair(sp)
                except Exception as err:
                    _log_warn("[daytrader] tick error", pair=sp.pair, err=str(err))

        await asyncio.gather(*[_wrap(sp) for sp in self.specs])

    async def run_forever(self) -> None:
        _log_info("[daytrader] starting runner")
        with contextlib.suppress(Exception):
            _log_info(risk_summary())

        while not self._stop.is_set():
            t0 = time.time()
            try:
                await self.run_once()
            except Exception as err:
                _log_exc("[daytrader] loop error", err=str(err))
            dt = time.time() - t0
            sleep_for = max(0.0, float(self.settings.poll_sec) - dt)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), timeout=sleep_for)

        _log_info("[daytrader] stopped")


# ----------------------------- CLI --------------------------------------------


def _parse_args() -> Any:
    import argparse

    p = argparse.ArgumentParser(description="Daytrader runner (optional subsystem)")
    p.add_argument(
        "--pairs", default=None, help='Comma list like "SOL/USDC,JUP/USDC" (overrides DT_PAIRS)'
    )
    p.add_argument("--once", action="store_true", help="Run one cycle and exit")
    p.add_argument("--poll", type=int, default=None, help="Override DT_POLL_SEC")
    p.add_argument("--lookback", type=int, default=None, help="Override DT_LOOKBACK")
    p.add_argument("--interval", type=int, default=None, help="Override DT_INTERVAL_SEC")
    return p.parse_args()


async def _amain() -> None:
    args = _parse_args()

    if args.pairs:
        os.environ["DT_PAIRS"] = str(args.pairs)
    if args.poll is not None:
        os.environ["DT_POLL_SEC"] = str(int(args.poll))
    if args.lookback is not None:
        os.environ["DT_LOOKBACK"] = str(int(args.lookback))
    if args.interval is not None:
        os.environ["DT_INTERVAL_SEC"] = str(int(args.interval))

    specs = _resolve_pairs()
    settings = _settings()
    runner = DaytraderRunner(specs, settings)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(Exception):
            loop.add_signal_handler(sig, runner.request_stop)

    if args.once:
        await runner.run_once()
    else:
        await runner.run_forever()


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
