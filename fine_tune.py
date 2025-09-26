# fine_tune.py
# End-to-end helper for creating a parameter-tuning model:
# - prepare: build chat JSONL from trade history / stats
# - tune: upload files & start a fine-tune
# - status: check fine-tune job
# - set-model: persist chosen model id in Redis (ai:ft_model)
# - recommend: run one-shot parameter recommendation

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import sys
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Ensure repo root imports work
REPO_ROOT = Path(__file__).resolve().parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Optional dotenv load (don’t fail if missing)
try:
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv())
except Exception:
    pass

from config import (
    OPENAI_API_KEY,
    OPENAI_BASE_MODEL,     # e.g. "gpt-4o-mini-2024-07-18" or stable base model name
    REDIS_URL,
)

# Our infra helpers
from utils_pkg.core_utils import init_resources, close_resources, redis  # type: ignore

# OpenAI SDK (support both new and legacy)
NEW_SDK = False
try:
    from openai import OpenAI  # >= v1.x
    _client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
    NEW_SDK = True
except Exception:  # legacy
    _client = None

if not NEW_SDK:
    try:
        import openai  # legacy
        if OPENAI_API_KEY:
            openai.api_key = OPENAI_API_KEY
    except Exception:
        openai = None  # type: ignore

log = logging.getLogger("fine_tune")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

DATA_DIR = REPO_ROOT / "data" / "ai"
DATA_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_TRAIN = DATA_DIR / "train.jsonl"
DEFAULT_VAL = DATA_DIR / "val.jsonl"
DEFAULT_MODEL_PICKLE = DATA_DIR / "last_ft_model.txt"  # fallback cache


# --------------------------
# Data schema for features
# --------------------------

@dataclass
class MarketFeatures:
    vol_24h_usd: float
    liquidity_usd: float
    age_minutes: float
    whale_buys_1h: int
    rug_risk: float             # 0..1
    momentum_5m: float          # % change
    momentum_15m: float         # % change
    momentum_1h: float          # % change
    volatility_1h: float        # stdev %
    gas_fee_bp: float           # swap fees/slippage proxy bps
    holding_winrate_7d: float   # your bot winrate last 7d 0..1
    avg_profit_7d: float        # %
    avg_loss_7d: float          # %
    # add new fields over time safely


@dataclass
class StrategyParams:
    stop_loss_percent: float
    target_profit_percent: float
    reinvest_percent: float
    max_hold_time_minutes: int
    partial_take_profits: int
    hold_if_score_above: float  # 0..1
    slippage: float             # e.g. 0.005


def _example_features() -> MarketFeatures:
    return MarketFeatures(
        vol_24h_usd=1_250_000,
        liquidity_usd=300_000,
        age_minutes=220,
        whale_buys_1h=8,
        rug_risk=0.18,
        momentum_5m=4.2,
        momentum_15m=9.1,
        momentum_1h=15.0,
        volatility_1h=3.5,
        gas_fee_bp=12.0,
        holding_winrate_7d=0.62,
        avg_profit_7d=27.0,
        avg_loss_7d=-9.0,
    )


def _example_params() -> StrategyParams:
    return StrategyParams(
        stop_loss_percent=18.0,
        target_profit_percent=220.0,
        reinvest_percent=80.0,
        max_hold_time_minutes=360,
        partial_take_profits=4,
        hold_if_score_above=0.80,
        slippage=0.005,
    )


# --------------------------
# JSONL builders (chat format)
# --------------------------

def _chat_record(features: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build one chat training row:
    - system: role description
    - user: JSON of features
    - assistant: JSON of target params
    """
    return {
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are a trading parameter tuner for a Solana sniper bot. "
                    "Given recent market features (JSON), respond ONLY with a compact JSON containing the following keys: "
                    "stop_loss_percent (float), target_profit_percent (float), reinvest_percent (float), "
                    "max_hold_time_minutes (int), partial_take_profits (int), hold_if_score_above (float 0..1), slippage (float). "
                    "Keep responses strictly valid JSON without additional text."
                ),
            },
            {"role": "user", "content": json.dumps(features, separators=(",", ":"))},
            {"role": "assistant", "content": json.dumps(params, separators=(",", ":"))},
        ]
    }


def write_jsonl(rows: List[Dict[str, Any]], path: Path) -> None:
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


# --------------------------
# Data preparation
# --------------------------

def prepare_from_csv(csv_path: Path, out_train: Path, out_val: Path, train_split: float = 0.9) -> None:
    """
    CSV columns expected (example):
      vol_24h_usd,liquidity_usd,age_minutes,whale_buys_1h,rug_risk,momentum_5m,momentum_15m,momentum_1h,
      volatility_1h,gas_fee_bp,holding_winrate_7d,avg_profit_7d,avg_loss_7d,
      stop_loss_percent,target_profit_percent,reinvest_percent,max_hold_time_minutes,partial_take_profits,hold_if_score_above,slippage
    """
    rows: List[Dict[str, Any]] = []
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for line in reader:
            feat = {k: _safe_cast(line.get(k)) for k in [
                "vol_24h_usd","liquidity_usd","age_minutes","whale_buys_1h","rug_risk",
                "momentum_5m","momentum_15m","momentum_1h","volatility_1h","gas_fee_bp",
                "holding_winrate_7d","avg_profit_7d","avg_loss_7d"
            ]}
            params = {k: _safe_cast(line.get(k)) for k in [
                "stop_loss_percent","target_profit_percent","reinvest_percent","max_hold_time_minutes",
                "partial_take_profits","hold_if_score_above","slippage"
            ]}
            rows.append(_chat_record(feat, params))

    if not rows:
        raise SystemExit("No rows parsed from CSV.")

    split = int(len(rows) * train_split)
    train, val = rows[:split], rows[split:]
    write_jsonl(train, out_train)
    write_jsonl(val or train[:1], out_val)  # ensure non-empty
    log.info("Prepared JSONL: %s rows -> train=%d, val=%d", len(rows), len(train), len(val or train[:1]))


def prepare_synthetic(n: int, out_train: Path, out_val: Path) -> None:
    """
    Use a basic synthetic generator (safe fallback).
    It anchors around _example_features/_example_params with noise.
    """
    import random
    rows: List[Dict[str, Any]] = []
    for i in range(n):
        base_f = _example_features()
        feat = asdict(base_f)
        # inject mild randomness
        feat["vol_24h_usd"] *= random.uniform(0.5, 2.0)
        feat["liquidity_usd"] *= random.uniform(0.5, 2.0)
        feat["whale_buys_1h"] = max(0, int(feat["whale_buys_1h"] * random.uniform(0.6, 1.8)))
        feat["rug_risk"] = min(0.95, max(0.01, feat["rug_risk"] * random.uniform(0.6, 1.6)))
        feat["momentum_5m"] *= random.uniform(0.4, 1.8)
        feat["momentum_15m"] *= random.uniform(0.4, 1.8)
        feat["momentum_1h"] *= random.uniform(0.4, 1.8)
        feat["volatility_1h"] *= random.uniform(0.6, 1.6)
        feat["gas_fee_bp"] *= random.uniform(0.8, 1.2)
        feat["holding_winrate_7d"] = min(0.95, max(0.1, feat["holding_winrate_7d"] * random.uniform(0.8, 1.2)))
        feat["avg_profit_7d"] *= random.uniform(0.8, 1.2)
        feat["avg_loss_7d"] *= random.uniform(0.8, 1.2)

        # rule-of-thumb mapping to targets (seed signal)
        p = _example_params()
        params = asdict(p)
        rr = feat["rug_risk"]
        wr = feat["holding_winrate_7d"]
        mom = 0.25*feat["momentum_5m"] + 0.35*feat["momentum_15m"] + 0.40*feat["momentum_1h"]
        vol = feat["volatility_1h"]

        params["stop_loss_percent"] = max(10.0, min(30.0, 22.0 - 6.0*(wr-0.5) + 4.0*rr + 0.5*vol))
        params["target_profit_percent"] = max(120.0, min(500.0, 180.0 + 8.0*mom + 2.0*feat["whale_buys_1h"]))
        params["reinvest_percent"] = max(50.0, min(95.0, 70.0 + 20.0*(wr-0.5) - 10.0*rr))
        params["max_hold_time_minutes"] = int(max(60, min(720, 360 + 20.0*(wr-0.5) + 10.0*mom - 5.0*vol)))
        params["partial_take_profits"] = int(max(3, min(6, 4 + (1 if mom > 8 else 0) - (1 if rr > 0.4 else 0))))
        params["hold_if_score_above"] = float(min(0.95, max(0.6, 0.8 + 0.1*(wr-0.5) - 0.15*(rr-0.2))))
        params["slippage"] = float(min(0.01, max(0.002, 0.005 + 0.001*(vol/3.0))))

        rows.append(_chat_record(feat, params))

    # 90/10 split
    split = max(1, int(n*0.9))
    write_jsonl(rows[:split], out_train)
    write_jsonl(rows[split:] or rows[:1], out_val)
    log.info("Prepared synthetic dataset: %d rows", n)


def _safe_cast(v: Any) -> Any:
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None
    # try int, float, bool, else leave string
    for caster in (int, float):
        try:
            return caster(s)
        except Exception:
            pass
    ls = s.lower()
    if ls in ("true", "false"):
        return ls == "true"
    return v


# --------------------------
# OpenAI helpers
# --------------------------

def _require_openai():
    if NEW_SDK and _client:
        return _client
    if not NEW_SDK and "openai" in globals() and openai:
        return openai
    raise SystemExit("OpenAI SDK not available and/or OPENAI_API_KEY missing.")

def upload_file(path: Path) -> str:
    """Return file id for fine-tune."""
    client = _require_openai()
    if NEW_SDK:
        with path.open("rb") as f:
            file_obj = client.files.create(file=f, purpose="fine-tune")
        return file_obj.id
    else:
        with path.open("rb") as f:
            file_obj = client.File.create(file=f, purpose="fine-tune")  # type: ignore[attr-defined]
        return file_obj["id"]

def start_finetune(training_file_id: str, validation_file_id: Optional[str], base_model: str, suffix: Optional[str]) -> str:
    """Return fine-tuning job id."""
    client = _require_openai()
    if NEW_SDK:
        job = client.fine_tuning.jobs.create(  # type: ignore[attr-defined]
            training_file=training_file_id,
            validation_file=validation_file_id,
            model=base_model,
            suffix=suffix or "sniper-params"
        )
        return job.id
    else:
        job = client.FineTuningJob.create(  # type: ignore[attr-defined]
            training_file=training_file_id,
            validation_file=validation_file_id,
            model=base_model,
            suffix=suffix or "sniper-params"
        )
        return job["id"]

def get_job(job_id: str) -> Dict[str, Any]:
    client = _require_openai()
    if NEW_SDK:
        job = client.fine_tuning.jobs.retrieve(job_id)  # type: ignore[attr-defined]
        return job.to_dict()
    else:
        job = client.FineTuningJob.retrieve(job_id)  # type: ignore[attr-defined]
        return dict(job)

def last_model_from_job(job: Dict[str, Any]) -> Optional[str]:
    # both SDKs expose 'fine_tuned_model'
    return job.get("fine_tuned_model")

def store_model_in_redis(model_id: str) -> None:
    if not redis:
        log.warning("Redis not available; writing to %s", DEFAULT_MODEL_PICKLE)
        DEFAULT_MODEL_PICKLE.write_text(model_id, encoding="utf-8")
        return
    # also store under a stable key used by the strategy
    asyncio.run(redis.set("ai:ft_model", model_id))  # quick set in blocking context


def load_model_from_store() -> Optional[str]:
    if redis:
        try:
            val = asyncio.run(redis.get("ai:ft_model"))
            if val:
                return str(val)
        except Exception:
            pass
    if DEFAULT_MODEL_PICKLE.exists():
        return DEFAULT_MODEL_PICKLE.read_text(encoding="utf-8").strip()
    return None


def chat_completion(model: str, features: Dict[str, Any]) -> Dict[str, Any]:
    """Query the (fine-tuned) model and parse JSON response."""
    client = _require_openai()
    messages = [
        {
            "role": "system",
            "content": (
                "You are a trading parameter tuner for a Solana sniper bot. "
                "Given market features (JSON), respond ONLY with compact JSON keys: "
                "stop_loss_percent,target_profit_percent,reinvest_percent,max_hold_time_minutes,partial_take_profits,hold_if_score_above,slippage"
            ),
        },
        {"role": "user", "content": json.dumps(features, separators=(",", ":"))},
    ]
    if NEW_SDK:
        resp = client.chat.completions.create(model=model, messages=messages, temperature=0.2)  # type: ignore[attr-defined]
        text = resp.choices[0].message.content.strip()
    else:
        resp = client.ChatCompletion.create(model=model, messages=messages, temperature=0.2)  # type: ignore[attr-defined]
        text = resp["choices"][0]["message"]["content"].strip()
    try:
        return json.loads(text)
    except Exception:
        # attempt to extract JSON
        import re
        m = re.search(r"\{.*\}", text, re.S)
        if m:
            return json.loads(m.group(0))
        raise


# --------------------------
# CLI Tasks
# --------------------------

def cmd_prepare(args: argparse.Namespace) -> None:
    out_train = Path(args.train or DEFAULT_TRAIN)
    out_val = Path(args.val or DEFAULT_VAL)

    if args.csv:
        prepare_from_csv(Path(args.csv), out_train, out_val, train_split=args.split)
        return

    # fallback: synthetic dataset if no CSV provided
    prepare_synthetic(n=args.synthetic_rows, out_train=out_train, out_val=out_val)
    log.info("Prepared: %s | %s", out_train, out_val)

def cmd_tune(args: argparse.Namespace) -> None:
    train = Path(args.train or DEFAULT_TRAIN)
    val = Path(args.val or DEFAULT_VAL)
    if not train.exists():
        raise SystemExit(f"Training file missing: {train}. Run 'prepare' first.")
    if not OPENAI_API_KEY:
        raise SystemExit("OPENAI_API_KEY not set.")

    train_id = upload_file(train)
    val_id = upload_file(val) if val.exists() else None
    base = args.model or OPENAI_BASE_MODEL
    job_id = start_finetune(train_id, val_id, base, suffix=args.suffix)
    print(job_id)

def cmd_status(args: argparse.Namespace) -> None:
    job = get_job(args.job_id)
    print(json.dumps(job, indent=2))
    model = last_model_from_job(job)
    if model:
        log.info("Fine-tuned model: %s", model)
        # optionally persist
        if args.persist:
            store_model_in_redis(model)
            log.info("Persisted to Redis (ai:ft_model) / file cache.")

def cmd_set_model(args: argparse.Namespace) -> None:
    mid = args.model_id.strip()
    if not mid:
        raise SystemExit("Provide --model-id")
    store_model_in_redis(mid)
    print(f"Stored model id: {mid}")

def cmd_recommend(args: argparse.Namespace) -> None:
    model = args.model or load_model_from_store() or OPENAI_BASE_MODEL
    feats = _example_features() if not args.features else json.loads(Path(args.features).read_text(encoding="utf-8"))
    rec = chat_completion(model, asdict(feats) if isinstance(feats, MarketFeatures) else feats)
    print(json.dumps(rec, indent=2))


# --------------------------
# Async wrapper to bring up Redis
# --------------------------

async def _with_resources(coro, *a, **kw):
    await init_resources()
    try:
        return await asyncio.to_thread(coro, *a, **kw)
    finally:
        await close_resources()


def main():
    ap = argparse.ArgumentParser(description="Fine-tune a strategy parameter model for the sniper bot.")
    sub = ap.add_subparsers(dest="cmd", required=True)

    # prepare
    ap_p = sub.add_parser("prepare", help="Create JSONL training/validation data.")
    ap_p.add_argument("--csv", help="Path to labeled CSV (optional).")
    ap_p.add_argument("--train", help=f"Train JSONL (default: {DEFAULT_TRAIN})")
    ap_p.add_argument("--val", help=f"Val JSONL (default: {DEFAULT_VAL})")
    ap_p.add_argument("--split", type=float, default=0.9, help="Train split for CSV mode.")
    ap_p.add_argument("--synthetic-rows", type=int, default=500, help="Rows for synthetic generator if no CSV.")
    ap_p.set_defaults(func=lambda args: cmd_prepare(args))

    # tune
    ap_t = sub.add_parser("tune", help="Upload files and start a fine-tune.")
    ap_t.add_argument("--train", help=f"Train JSONL (default: {DEFAULT_TRAIN})")
    ap_t.add_argument("--val", help=f"Val JSONL (default: {DEFAULT_VAL})")
    ap_t.add_argument("--model", help=f"Base model (default: OPENAI_BASE_MODEL)")
    ap_t.add_argument("--suffix", help="Job suffix", default="sniper-params")
    ap_t.set_defaults(func=lambda args: cmd_tune(args))

    # status
    ap_s = sub.add_parser("status", help="Check fine-tune job status.")
    ap_s.add_argument("job_id", help="Fine-tune job id")
    ap_s.add_argument("--persist", action="store_true", help="Persist resulting model id if available.")
    ap_s.set_defaults(func=lambda args: cmd_status(args))

    # set-model
    ap_m = sub.add_parser("set-model", help="Persist a chosen fine-tuned model id.")
    ap_m.add_argument("--model-id", required=True)
    ap_m.set_defaults(func=lambda args: cmd_set_model(args))

    # recommend (inference)
    ap_r = sub.add_parser("recommend", help="Query tuned/base model with features JSON and print params JSON.")
    ap_r.add_argument("--model", help="Model id (default: ai:ft_model or OPENAI_BASE_MODEL)")
    ap_r.add_argument("--features", help="Path to features JSON. If omitted, uses example.")
    ap_r.set_defaults(func=lambda args: cmd_recommend(args))

    args = ap.parse_args()

    # Bring up shared resources (Redis) only for commands that need it
    if args.cmd in ("status", "set-model", "recommend"):
        asyncio.run(_with_resources(args.func, args))
    else:
        # prepare/tune don’t strictly need Redis
        args.func(args)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.")
