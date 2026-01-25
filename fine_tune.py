# fine_tune.py
from __future__ import annotations

import argparse
import asyncio
import csv
import dataclasses
import hashlib
import json
import logging
import os
import random
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

log = logging.getLogger("fine_tune")

REPO_ROOT = Path(__file__).resolve().parent
DATA_DIR = REPO_ROOT / "data" / "ai"
DATA_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_TRAIN = DATA_DIR / "train.jsonl"
DEFAULT_VAL = DATA_DIR / "val.jsonl"
DEFAULT_MODEL_CACHE = DATA_DIR / "last_ft_model.txt"

# ----------------------------
# Optional dotenv
# ----------------------------
try:
    from dotenv import find_dotenv, load_dotenv  # type: ignore

    load_dotenv(find_dotenv(), override=False)
except Exception:
    pass


# ----------------------------
# Dependencies (checked lazily)
# ----------------------------
def _require_asyncpg():
    try:
        import asyncpg  # type: ignore

        return asyncpg
    except Exception as e:
        raise SystemExit("Missing dependency: asyncpg. Install with: pip install asyncpg") from e


def _require_openai():
    # New SDK (preferred)
    try:
        from openai import OpenAI  # type: ignore

        return ("new", OpenAI)
    except Exception:
        pass

    # Legacy SDK fallback
    try:
        import openai  # type: ignore

        return ("legacy", openai)
    except Exception as e:
        raise SystemExit("OpenAI SDK not installed. Install with: pip install openai") from e


def _require_redis():
    try:
        import redis.asyncio as redis_async  # type: ignore

        return redis_async
    except Exception:
        return None


# ----------------------------
# Env helpers
# ----------------------------
def env(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip()
    return v if v else default


def env_required(name: str) -> str:
    v = env(name)
    if not v:
        raise SystemExit(f"Missing required env var: {name}")
    return v


def resolve_pg_dsn() -> str:
    """
    Prefer your canonical DSN env.
    Accept common aliases so this tool doesn't break when you rename.
    """
    for k in ("POSTGRES_DSN", "PG_DSN", "PG_DSN_INTERNAL", "PG_DSN_EXTERNAL", "DATABASE_URL"):
        v = env(k)
        if v:
            return v
    raise SystemExit(
        "Missing Postgres DSN. Set one of: POSTGRES_DSN / PG_DSN / PG_DSN_INTERNAL / PG_DSN_EXTERNAL / DATABASE_URL"
    )


# ----------------------------
# Training schemas
# ----------------------------
@dataclass
class MarketFeatures:
    # Keep flexible; DB features are passed through as-is.
    values: dict[str, Any]


@dataclass
class StrategyParams:
    stop_loss_percent: float
    target_profit_percent: float
    reinvest_percent: float
    max_hold_time_minutes: int
    partial_take_profits: int
    hold_if_score_above: float
    slippage: float


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(float(x))
    except Exception:
        return default


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


# ----------------------------
# JSONL row builder (Chat format)
# ----------------------------
def chat_record(features: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
    """
    Chat fine-tune row:
    - system defines strict output format
    - user provides features JSON
    - assistant provides target params JSON
    """
    return {
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are a trading parameter tuner for a Solana sniper bot. "
                    "Given recent market + bot features in JSON, respond ONLY with a single valid JSON object "
                    "containing exactly these keys:\n"
                    "stop_loss_percent (float), target_profit_percent (float), reinvest_percent (float), "
                    "max_hold_time_minutes (int), partial_take_profits (int), hold_if_score_above (float 0..1), slippage (float).\n"
                    "No extra text. JSON only."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(features, separators=(",", ":"), ensure_ascii=False),
            },
            {
                "role": "assistant",
                "content": json.dumps(params, separators=(",", ":"), ensure_ascii=False),
            },
        ]
    }


def write_jsonl(rows: Iterable[dict[str, Any]], path: Path) -> int:
    n = 0
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
            n += 1
    return n


# ----------------------------
# Heuristic labeling (DB row -> params)
# ----------------------------
def params_from_row_heuristic(
    *,
    features: dict[str, Any],
    outcome_label: str | None,
    pnl_pct: float | None,
    hold_sec: int | None,
    max_runup_pct: float | None,
    max_drawdown_pct: float | None,
) -> StrategyParams:
    """
    Create a *supervised target* from your historical outcomes.
    This isn't "perfect optimal", but it's consistent and gives the model a learnable signal.

    Uses:
      - drawdown -> stop loss
      - runup -> profit target and TP count
      - hold time -> max hold
      - pnl + label -> reinvest / threshold tweaks
    """
    dd = abs(_safe_float(max_drawdown_pct, 0.0))
    ru = max(0.0, _safe_float(max_runup_pct, 0.0))
    pnl = _safe_float(pnl_pct, 0.0)
    hold_m = max(1, _safe_int(hold_sec, 0) // 60)

    # Baselines
    sl = _clamp(2.0 + 1.15 * dd, 6.0, 25.0)  # 6%..25%
    tp = _clamp(30.0 + 0.70 * ru, 60.0, 500.0)  # 60%..500%
    max_hold = int(_clamp(float(hold_m), 5.0, 720.0))  # 5m..12h

    # Partial take profits: more runup => more steps
    if ru >= 250:
        ptp = 6
    elif ru >= 160:
        ptp = 5
    elif ru >= 90:
        ptp = 4
    else:
        ptp = 3

    # Hold threshold: tighten if rugs / heavy DD, loosen if strong pnl
    thr = 0.80
    lbl = (outcome_label or "").lower()
    if "rug" in lbl or "honeypot" in lbl:
        thr = 0.88
        sl = _clamp(sl * 0.85, 6.0, 20.0)  # tighter
        tp = _clamp(tp * 0.75, 60.0, 250.0)
    elif pnl < -5.0 or dd > 25.0:
        thr = 0.86
    elif pnl > 15.0 and ru > 120:
        thr = 0.76

    # Slippage: if features include a hint, use it; else safe default
    # Accept bps or fraction
    slip = features.get("slippage")
    slip_bps = features.get("slippage_bps") or features.get("est_slippage_bps")
    if slip is not None:
        slippage = float(slip)
        if slippage > 1.0:  # if user passed bps by mistake
            slippage = _clamp(slippage / 10_000.0, 0.002, 0.02)
        else:
            slippage = _clamp(slippage, 0.002, 0.02)
    elif slip_bps is not None:
        slippage = _clamp(_safe_float(slip_bps) / 10_000.0, 0.002, 0.02)
    else:
        slippage = 0.005

    # Reinvest: more aggressive if pnl positive and not rug
    reinvest = 70.0
    if "rug" in lbl or "honeypot" in lbl:
        reinvest = 55.0
    elif pnl > 0.0:
        reinvest = _clamp(70.0 + min(20.0, pnl), 60.0, 95.0)
    else:
        reinvest = 60.0

    return StrategyParams(
        stop_loss_percent=float(round(sl, 3)),
        target_profit_percent=float(round(tp, 3)),
        reinvest_percent=float(round(reinvest, 3)),
        max_hold_time_minutes=int(max_hold),
        partial_take_profits=int(ptp),
        hold_if_score_above=float(round(_clamp(thr, 0.6, 0.95), 3)),
        slippage=float(round(slippage, 6)),
    )


# ----------------------------
# DB fetcher
# ----------------------------
async def fetch_training_rows(
    dsn: str,
    limit: int,
    schema: str | None,
    view_name: str,
    where: str | None,
) -> list[dict[str, Any]]:
    asyncpg = _require_asyncpg()
    view = f"{schema}.{view_name}" if schema else view_name
    cond = f"WHERE {where}" if where else ""
    sql = f"""
        SELECT
            candidate_id,
            ts,
            src,
            mint,
            symbol,
            regime,
            wallet_tag,
            feature_version,
            features,
            raw,
            outcome_label,
            pnl_pct,
            hold_sec,
            max_runup_pct,
            max_drawdown_pct,
            notes_json
        FROM {view}
        {cond}
        ORDER BY ts DESC
        LIMIT $1
    """
    conn = await asyncpg.connect(dsn)
    try:
        rows = await conn.fetch(sql, limit)
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(dict(r))
        return out
    finally:
        await conn.close()


def _jsonify(x: Any) -> Any:
    if x is None:
        return None
    if isinstance(x, dict | list | int | float | bool):
        return x
    # asyncpg may return JSON as str sometimes depending on schema
    if isinstance(x, str):
        s = x.strip()
        if s.startswith("{") or s.startswith("["):
            try:
                return json.loads(s)
            except Exception:
                return x
    return x


def build_feature_payload(row: dict[str, Any]) -> dict[str, Any]:
    """
    Compose user-features JSON:
    - Start from FeatureBuilder features dict
    - Add a small amount of stable context (src/regime/etc)
    """
    feat = _jsonify(row.get("features")) or {}
    if not isinstance(feat, dict):
        feat = {"features_raw": feat}

    payload = dict(feat)
    payload.update(
        {
            "src": row.get("src"),
            "regime": row.get("regime"),
            "wallet_tag": row.get("wallet_tag"),
            "symbol": row.get("symbol"),
            "mint": row.get("mint"),
            "feature_version": row.get("feature_version"),
        }
    )
    # optional add raw snapshot if you want (kept compact)
    # payload["raw"] = _jsonify(row.get("raw"))
    return payload


def deterministic_split(
    rows: list[dict[str, Any]], val_ratio: float, seed: int
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Stable split so re-runs don't reshuffle labels unpredictably.
    Uses candidate_id hash.
    """
    train: list[dict[str, Any]] = []
    val: list[dict[str, Any]] = []
    for r in rows:
        cid = str(r.get("candidate_id") or "")
        h = hashlib.sha256((cid + str(seed)).encode("utf-8")).digest()
        u = int.from_bytes(h[:4], "big") / 2**32
        (val if u < val_ratio else train).append(r)
    return train, val


# ----------------------------
# CLI commands
# ----------------------------
def cmd_prepare_csv(args: argparse.Namespace) -> None:
    rows: list[dict[str, Any]] = []
    path = Path(args.csv).resolve()
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for line in reader:
            feat = {}
            params = {}
            # everything before params_ is features
            for k, v in line.items():
                if k is None:
                    continue
                if k.startswith("param_"):
                    params[k.replace("param_", "", 1)] = _cast(v)
                else:
                    feat[k] = _cast(v)
            rows.append(chat_record(feat, params))

    if not rows:
        raise SystemExit("No rows parsed from CSV.")

    out_train = Path(args.train or DEFAULT_TRAIN)
    out_val = Path(args.val or DEFAULT_VAL)

    # shuffle, then split
    rnd = random.Random(args.seed)
    rnd.shuffle(rows)
    split = max(1, int(len(rows) * (1.0 - args.val_ratio)))
    n_train = write_jsonl(rows[:split], out_train)
    n_val = write_jsonl(rows[split:] or rows[:1], out_val)

    log.info("Prepared CSV JSONL: total=%d train=%d val=%d", len(rows), n_train, n_val)
    print(str(out_train))
    print(str(out_val))


def cmd_prepare_db(args: argparse.Namespace) -> None:
    dsn = resolve_pg_dsn()
    limit = int(args.limit)
    schema = args.schema
    view = args.view
    where = args.where

    rows = asyncio.run(
        fetch_training_rows(dsn, limit=limit, schema=schema, view_name=view, where=where)
    )
    if not rows:
        raise SystemExit("No rows returned from DB (check DSN/view/where).")

    train_rows, val_rows = deterministic_split(
        rows, val_ratio=float(args.val_ratio), seed=int(args.seed)
    )

    def gen_jsonl(rws: list[dict[str, Any]]) -> Iterable[dict[str, Any]]:
        for r in rws:
            payload = build_feature_payload(r)
            params = params_from_row_heuristic(
                features=payload,
                outcome_label=r.get("outcome_label"),
                pnl_pct=_safe_float(r.get("pnl_pct"), 0.0),
                hold_sec=_safe_int(r.get("hold_sec"), 0),
                max_runup_pct=_safe_float(r.get("max_runup_pct"), 0.0),
                max_drawdown_pct=_safe_float(r.get("max_drawdown_pct"), 0.0),
            )
            yield chat_record(payload, dataclasses.asdict(params))

    out_train = Path(args.train or DEFAULT_TRAIN)
    out_val = Path(args.val or DEFAULT_VAL)

    n_train = write_jsonl(gen_jsonl(train_rows), out_train)
    n_val = write_jsonl(gen_jsonl(val_rows or train_rows[:1]), out_val)

    log.info("Prepared DB JSONL: total=%d train=%d val=%d view=%s", len(rows), n_train, n_val, view)
    print(str(out_train))
    print(str(out_val))


def cmd_tune(args: argparse.Namespace) -> None:
    mode, sdk = _require_openai()
    api_key = env_required("OPENAI_API_KEY")
    base_model = env("OPENAI_BASE_MODEL", args.model) or args.model
    if not base_model:
        raise SystemExit("Provide --model or set OPENAI_BASE_MODEL")

    train = Path(args.train or DEFAULT_TRAIN)
    val = Path(args.val or DEFAULT_VAL)
    if not train.exists():
        raise SystemExit(f"Training file missing: {train} (run prepare-db/prepare-csv first)")

    if mode == "new":
        OpenAI = sdk
        client = OpenAI(api_key=api_key)
        with train.open("rb") as f:
            train_file = client.files.create(file=f, purpose="fine-tune")  #
        val_file_id = None
        if val.exists():
            with val.open("rb") as f:
                val_file = client.files.create(file=f, purpose="fine-tune")  #
            val_file_id = val_file.id

        job = client.fine_tuning.jobs.create(  #
            training_file=train_file.id,
            validation_file=val_file_id,
            model=base_model,
            suffix=args.suffix or "sniper-params",
        )
        print(job.id)
        return

    # legacy fallback
    openai = sdk
    openai.api_key = api_key
    train_id = openai.File.create(file=train.open("rb"), purpose="fine-tune")["id"]
    val_id = (
        openai.File.create(file=val.open("rb"), purpose="fine-tune")["id"] if val.exists() else None
    )
    job = openai.FineTuningJob.create(
        training_file=train_id,
        validation_file=val_id,
        model=base_model,
        suffix=args.suffix or "sniper-params",
    )
    print(job["id"])


async def _store_model_id(model_id: str, redis_url: str | None) -> None:
    if not model_id:
        return
    if redis_url:
        redis_async = _require_redis()
        if redis_async:
            r = redis_async.from_url(redis_url, decode_responses=True)
            try:
                await r.set("ai:ft_model", model_id)
                await r.close()
                log.info("Stored model id in Redis key ai:ft_model")
                return
            except Exception as e:
                log.warning("Redis store failed: %s", e)

    DEFAULT_MODEL_CACHE.write_text(model_id, encoding="utf-8")
    log.info("Stored model id in %s", DEFAULT_MODEL_CACHE)


def _load_model_id(redis_url: str | None) -> str | None:
    if redis_url:
        # best-effort sync load via asyncio
        redis_async = _require_redis()
        if redis_async:

            async def _get():
                r = redis_async.from_url(redis_url, decode_responses=True)
                try:
                    return await r.get("ai:ft_model")
                finally:
                    await r.close()

            try:
                return asyncio.run(_get())
            except Exception:
                pass

    if DEFAULT_MODEL_CACHE.exists():
        return DEFAULT_MODEL_CACHE.read_text(encoding="utf-8").strip() or None
    return None


def cmd_status(args: argparse.Namespace) -> None:
    mode, sdk = _require_openai()
    api_key = env_required("OPENAI_API_KEY")
    redis_url = env("REDIS_URL") or env("REDIS_TLS_URL") or args.redis_url

    if mode == "new":
        OpenAI = sdk
        client = OpenAI(api_key=api_key)
        job = client.fine_tuning.jobs.retrieve(args.job_id)
        d = job.to_dict()
        print(json.dumps(d, indent=2))
        model_id = d.get("fine_tuned_model")
        if args.persist and model_id:
            asyncio.run(_store_model_id(model_id, redis_url))
        return

    openai = sdk
    openai.api_key = api_key
    job = openai.FineTuningJob.retrieve(args.job_id)
    print(json.dumps(job, indent=2))
    model_id = job.get("fine_tuned_model")
    if args.persist and model_id:
        asyncio.run(_store_model_id(model_id, redis_url))


def cmd_set_model(args: argparse.Namespace) -> None:
    redis_url = env("REDIS_URL") or env("REDIS_TLS_URL") or args.redis_url
    asyncio.run(_store_model_id(args.model_id.strip(), redis_url))
    print(f"Stored model id: {args.model_id.strip()}")


def cmd_recommend(args: argparse.Namespace) -> None:
    mode, sdk = _require_openai()
    api_key = env_required("OPENAI_API_KEY")
    redis_url = env("REDIS_URL") or env("REDIS_TLS_URL") or args.redis_url

    model = args.model or _load_model_id(redis_url) or env("OPENAI_BASE_MODEL")
    if not model:
        raise SystemExit(
            "No model provided and no stored model found. Use --model or run set-model."
        )

    # features input
    if args.features:
        feats = json.loads(Path(args.features).read_text(encoding="utf-8"))
    else:
        feats = {
            "vol_24h_usd": 1_250_000,
            "liquidity_usd": 300_000,
            "age_minutes": 220,
            "whale_buys_1h": 8,
            "rug_risk": 0.18,
            "momentum_5m": 4.2,
            "momentum_15m": 9.1,
            "momentum_1h": 15.0,
            "volatility_1h": 3.5,
            "slippage_bps": 12.0,
            "holding_winrate_7d": 0.62,
            "avg_profit_7d": 27.0,
            "avg_loss_7d": -9.0,
        }

    messages = [
        {
            "role": "system",
            "content": (
                "Return ONLY valid JSON keys: "
                "stop_loss_percent,target_profit_percent,reinvest_percent,max_hold_time_minutes,partial_take_profits,hold_if_score_above,slippage"
            ),
        },
        {"role": "user", "content": json.dumps(feats, separators=(",", ":"), ensure_ascii=False)},
    ]

    if mode == "new":
        OpenAI = sdk
        client = OpenAI(api_key=api_key)
        resp = client.chat.completions.create(model=model, messages=messages, temperature=0.2)
        text = resp.choices[0].message.content.strip()
    else:
        openai = sdk
        openai.api_key = api_key
        resp = openai.ChatCompletion.create(model=model, messages=messages, temperature=0.2)
        text = resp["choices"][0]["message"]["content"].strip()

    try:
        out = json.loads(text)
    except Exception:
        # attempt to extract JSON object
        import re

        m = re.search(r"\{.*\}", text, re.S)
        if not m:
            raise
        out = json.loads(m.group(0))

    print(json.dumps(out, indent=2))


def _cast(v: Any) -> Any:
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None
    ls = s.lower()
    if ls in ("true", "false"):
        return ls == "true"
    try:
        if "." in s:
            return float(s)
        return int(s)
    except Exception:
        return s


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    ap = argparse.ArgumentParser(
        description="DB->JSONL and fine-tune helper for sniper param tuning."
    )
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_db = sub.add_parser(
        "prepare-db", help="Export from Postgres view v_training_rows into train/val JSONL."
    )
    p_db.add_argument("--limit", type=int, default=20000, help="Rows to pull (default 20000).")
    p_db.add_argument("--schema", help="Optional schema (default none).")
    p_db.add_argument(
        "--view", default="v_training_rows", help="View name (default v_training_rows)."
    )
    p_db.add_argument("--where", help="Optional SQL condition (no WHERE keyword).")
    p_db.add_argument("--train", help=f"Train JSONL (default {DEFAULT_TRAIN})")
    p_db.add_argument("--val", help=f"Val JSONL (default {DEFAULT_VAL})")
    p_db.add_argument(
        "--val-ratio", type=float, default=0.1, help="Validation ratio (default 0.1)."
    )
    p_db.add_argument("--seed", type=int, default=1337, help="Split seed (default 1337).")
    p_db.set_defaults(func=cmd_prepare_db)

    p_csv = sub.add_parser(
        "prepare-csv", help="Prepare JSONL from labeled CSV (features + param_* columns)."
    )
    p_csv.add_argument("--csv", required=True, help="Input CSV path.")
    p_csv.add_argument("--train", help=f"Train JSONL (default {DEFAULT_TRAIN})")
    p_csv.add_argument("--val", help=f"Val JSONL (default {DEFAULT_VAL})")
    p_csv.add_argument(
        "--val-ratio", type=float, default=0.1, help="Validation ratio (default 0.1)."
    )
    p_csv.add_argument("--seed", type=int, default=1337, help="Shuffle seed (default 1337).")
    p_csv.set_defaults(func=cmd_prepare_csv)

    p_t = sub.add_parser("tune", help="Upload JSONL files and start fine-tune job.")
    p_t.add_argument("--train", help=f"Train JSONL (default {DEFAULT_TRAIN})")
    p_t.add_argument("--val", help=f"Val JSONL (default {DEFAULT_VAL})")
    p_t.add_argument("--model", help="Base model (or set OPENAI_BASE_MODEL).")
    p_t.add_argument("--suffix", default="sniper-params", help="Job suffix.")
    p_t.set_defaults(func=cmd_tune)

    p_s = sub.add_parser("status", help="Check fine-tune job status.")
    p_s.add_argument("job_id", help="Fine-tune job id")
    p_s.add_argument(
        "--persist", action="store_true", help="Persist model id to Redis/file if available."
    )
    p_s.add_argument("--redis-url", help="Override Redis URL (else uses env).")
    p_s.set_defaults(func=cmd_status)

    p_m = sub.add_parser("set-model", help="Persist a model id to Redis/file.")
    p_m.add_argument("--model-id", required=True)
    p_m.add_argument("--redis-url", help="Override Redis URL (else uses env).")
    p_m.set_defaults(func=cmd_set_model)

    p_r = sub.add_parser("recommend", help="Query tuned/base model with features JSON.")
    p_r.add_argument("--model", help="Model id (default: stored ai:ft_model or OPENAI_BASE_MODEL).")
    p_r.add_argument("--features", help="Path to features JSON file (optional).")
    p_r.add_argument("--redis-url", help="Override Redis URL (else uses env).")
    p_r.set_defaults(func=cmd_recommend)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
