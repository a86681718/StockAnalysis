#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import sys
import json
import time
import argparse
import logging
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed

ANALYSIS_DIR = Path(__file__).resolve().parent
if str(ANALYSIS_DIR) not in sys.path:
    sys.path.insert(0, str(ANALYSIS_DIR))

from Analysis_BsReport_v3 import (  # noqa: E402
    Config,
    add_stage1_soft_score,
    make_stage1_events_soft,
    add_forward_returns,
    add_mfe_mae,
)


LOG = logging.getLogger("tune_v3")


# -----------------------------
# Defaults (tune target: avg fwd ret, risk guard: MAE)
# -----------------------------
TRAIN_START = "2025-04-09"
TRAIN_END = "2025-09-30"
VALID_START = "2025-10-01"
VALID_END = "2025-12-31"
HORIZONS = (5, 10, 20)

# Reasonable MAE guard: mean(p10 of 5/10/20d) >= -10%
MAE_P10_MEAN_MIN = -0.10
MIN_EVENTS_TRAIN = 30
MIN_EVENTS_VALID = 30

# Base rolling params defaults
BASE_LOOKBACK = 120
BASE_MINP = 60
BASE_SCORE_LOOKBACK = 60
BASE_SCORE_MINP = 20


# -----------------------------
# IO paths
# -----------------------------
DATA_DIR = Path("./data")
DERIVED = DATA_DIR / "_derived"
FEATURES = DERIVED / "features_stock_only.parquet"
FLOW_STOCK = DERIVED / "flow_stock_daily.parquet"
OHLC = DERIVED / "ohlc.parquet"


# -----------------------------
# Logging
# -----------------------------
def init_logging(level: str = "INFO", log_path: Optional[Path] = None):
    lvl = getattr(logging, level.upper(), logging.INFO)
    handlers = [logging.StreamHandler()]
    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_path, encoding="utf-8"))
    logging.basicConfig(
        level=lvl,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=handlers,
    )


def df_range(df: pd.DataFrame) -> str:
    if df.empty:
        return "EMPTY"
    return f"{df['date'].min().date()}~{df['date'].max().date()} rows={len(df):,} symbols={df['symbol'].nunique():,}"


# -----------------------------
# Load features
# -----------------------------
def load_features() -> pd.DataFrame:
    """
    Prefer features_stock_only.parquet (already merged).
    Else: merge flow_stock_daily + ohlc
    """
    if FEATURES.exists():
        df = pd.read_parquet(FEATURES)
        df["date"] = pd.to_datetime(df["date"])
        return df

    if not FLOW_STOCK.exists() or not OHLC.exists():
        raise FileNotFoundError("Need either features_stock_only.parquet or both flow_stock_daily.parquet and ohlc.parquet")

    flow = pd.read_parquet(FLOW_STOCK)
    ohlc = pd.read_parquet(OHLC)
    flow["date"] = pd.to_datetime(flow["date"])
    ohlc["date"] = pd.to_datetime(ohlc["date"])

    df = flow.merge(ohlc[["symbol", "date", "open", "high", "low", "close", "volume"]], on=["symbol", "date"], how="inner")
    return df


def build_base_cache(base_cache: Path) -> pd.DataFrame:
    feat = load_features()
    needed = {"symbol", "date", "posnet_total", "hhi_posnet", "high", "low", "close"}
    miss = needed - set(feat.columns)
    if miss:
        raise ValueError(f"Missing columns in features: {miss}")

    feat = feat.dropna(subset=["symbol", "date", "close", "high", "low"])
    feat["symbol"] = feat["symbol"].astype(str).str.strip()
    feat["date"] = pd.to_datetime(feat["date"])

    LOG.info(f"Features: {df_range(feat)}")

    t0 = time.perf_counter()
    base = add_forward_returns(feat, horizons=HORIZONS)
    base = add_mfe_mae(base, horizons=HORIZONS)
    base.to_parquet(base_cache, index=False)
    LOG.info(f"Wrote base cache: {base_cache} | {df_range(base)} | elapsed={time.perf_counter()-t0:.1f}s")
    return base


def required_base_columns() -> List[str]:
    cols = ["symbol", "date", "posnet_total", "hhi_posnet", "high", "low", "close"]
    for h in HORIZONS:
        cols.append(f"fwd_ret_{h}d")
        cols.append(f"mfe_{h}d")
        cols.append(f"mae_{h}d")
    return cols


# -----------------------------
# Trial config
# -----------------------------
@dataclass(frozen=True)
class TrialConfig:
    th: float
    W: int
    wI: float
    cooldown: int

    def to_key(self) -> str:
        return f"th={self.th:.3f}|W={self.W}|wI={self.wI:.3f}|cd={self.cooldown}"


def sample_random_configs(rng: np.random.Generator, n: int) -> List[TrialConfig]:
    th_grid = np.round(np.arange(0.93, 0.991, 0.005), 3)
    wI_grid = np.round(np.arange(0.40, 0.901, 0.05), 3)

    configs = []
    for _ in range(n):
        th = float(rng.choice(th_grid))
        W = int(rng.integers(5, 21))
        wI = float(rng.choice(wI_grid))
        cooldown = int(rng.integers(3, 11))
        configs.append(TrialConfig(th=th, W=W, wI=wI, cooldown=cooldown))
    return configs


def grid_configs(grid: Dict[str, List]) -> List[TrialConfig]:
    th_list = grid.get("th", [])
    W_list = grid.get("W", [])
    wI_list = grid.get("wI", [])
    cooldown_list = grid.get("cooldown", [])
    if not (th_list and W_list and wI_list and cooldown_list):
        raise ValueError("Grid must include non-empty lists: th, W, wI, cooldown")

    configs: List[TrialConfig] = []
    for th in th_list:
        for W in W_list:
            for wI in wI_list:
                for cooldown in cooldown_list:
                    configs.append(TrialConfig(th=float(th), W=int(W), wI=float(wI), cooldown=int(cooldown)))
    return configs


# -----------------------------
# Evaluation
# -----------------------------
@dataclass
class SplitMetrics:
    n_events: int
    mean_mfe: float
    mae_p10_mean: float


def eval_split(df: pd.DataFrame, start_date: pd.Timestamp, end_date: pd.Timestamp) -> SplitMetrics:
    ev = df[(df["is_event"] == 1) & (df["date"] >= start_date) & (df["date"] <= end_date)].copy()
    if ev.empty:
        return SplitMetrics(0, np.nan, np.nan)

    mean_list = []
    for h in HORIZONS:
        col = f"mfe_{h}d"
        if col not in ev.columns:
            continue
        r = ev[col].dropna()
        if len(r):
            mean_list.append(float(r.mean()))
    mean_mfe = float(np.mean(mean_list)) if mean_list else np.nan

    mae_list = []
    for h in HORIZONS:
        col = f"mae_{h}d"
        if col in ev.columns:
            v = ev[col].quantile(0.10)
            if np.isfinite(v):
                mae_list.append(float(v))
    mae_p10_mean = float(np.mean(mae_list)) if mae_list else np.nan
    return SplitMetrics(int(len(ev)), mean_mfe, mae_p10_mean)


def eval_split_by_symbols(df: pd.DataFrame, symbols: set) -> SplitMetrics:
    ev = df[(df["is_event"] == 1) & (df["symbol"].isin(symbols))].copy()
    if ev.empty:
        return SplitMetrics(0, np.nan, np.nan)

    mean_list = []
    for h in HORIZONS:
        col = f"mfe_{h}d"
        if col not in ev.columns:
            continue
        r = ev[col].dropna()
        if len(r):
            mean_list.append(float(r.mean()))
    mean_mfe = float(np.mean(mean_list)) if mean_list else np.nan

    mae_list = []
    for h in HORIZONS:
        col = f"mae_{h}d"
        if col in ev.columns:
            v = ev[col].quantile(0.10)
            if np.isfinite(v):
                mae_list.append(float(v))
    mae_p10_mean = float(np.mean(mae_list)) if mae_list else np.nan
    return SplitMetrics(int(len(ev)), mean_mfe, mae_p10_mean)


def run_one_trial_worker(args: dict) -> dict:
    trial_id = args["trial_id"]
    cfg_dict = args["config"]
    base_path = args["base_path"]
    train_start = pd.to_datetime(args["train_start"])
    train_end = pd.to_datetime(args["train_end"])
    valid_start = pd.to_datetime(args["valid_start"])
    valid_end = pd.to_datetime(args["valid_end"])
    split_mode = args["split_mode"]
    train_symbols = set(args.get("train_symbols", []))
    valid_symbols = set(args.get("valid_symbols", []))
    mae_p10_mean_min = args["mae_p10_mean_min"]
    min_events_train = args["min_events_train"]
    min_events_valid = args["min_events_valid"]
    base_lookback = args["base_lookback"]
    base_minp = args["base_minp"]
    score_lookback = args["score_lookback"]
    score_minp = args["score_minp"]

    base = pd.read_parquet(base_path)
    base["date"] = pd.to_datetime(base["date"])

    cfg = Config()
    cfg.lookback = int(base_lookback)
    cfg.minp = int(base_minp)
    cfg.score_lookback = int(score_lookback)
    cfg.score_minp = int(score_minp)
    cfg.th = float(cfg_dict["th"])
    cfg.W = int(cfg_dict["W"])
    cfg.wI = float(cfg_dict["wI"])
    cfg.wC = 1.0 - cfg.wI
    cfg.cooldown = int(cfg_dict["cooldown"])

    scored = add_stage1_soft_score(base, cfg)
    scored = make_stage1_events_soft(scored, cfg)

    if split_mode == "symbol":
        train = eval_split_by_symbols(scored, train_symbols)
        valid = eval_split_by_symbols(scored, valid_symbols)
    else:
        train = eval_split(scored, train_start, train_end)
        valid = eval_split(scored, valid_start, valid_end)

    train_pass = (train.n_events >= min_events_train) and (np.isfinite(train.mae_p10_mean) and train.mae_p10_mean >= mae_p10_mean_min)
    valid_pass = (valid.n_events >= min_events_valid) and (np.isfinite(valid.mae_p10_mean) and valid.mae_p10_mean >= mae_p10_mean_min)
    passed = bool(train_pass and valid_pass and np.isfinite(valid.mean_mfe))

    return {
        "trial_id": trial_id,
        **cfg_dict,
        "train_start": str(train_start.date()),
        "train_end": str(train_end.date()),
        "valid_start": str(valid_start.date()),
        "valid_end": str(valid_end.date()),
        "split_mode": split_mode,
        "train_n_events": train.n_events,
        "train_mean_mfe": train.mean_mfe,
        "train_mae_p10_mean": train.mae_p10_mean,
        "valid_n_events": valid.n_events,
        "valid_mean_mfe": valid.mean_mfe,
        "valid_mae_p10_mean": valid.mae_p10_mean,
        "passed": passed,
    }


# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trials", type=int, default=300, help="random search trials (ignored if --grid is used)")
    ap.add_argument("--grid", type=str, default="", help="json path with lists for th/W/wI/cooldown")
    ap.add_argument("--workers", type=int, default=0, help="0=auto")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--log-level", type=str, default="INFO")
    ap.add_argument("--train-start", type=str, default=TRAIN_START, help="YYYY-MM-DD")
    ap.add_argument("--train-end", type=str, default=TRAIN_END, help="YYYY-MM-DD")
    ap.add_argument("--valid-start", type=str, default=VALID_START, help="YYYY-MM-DD")
    ap.add_argument("--valid-end", type=str, default=VALID_END, help="YYYY-MM-DD")
    ap.add_argument("--split-mode", type=str, choices=["time", "symbol"], default="symbol")
    ap.add_argument("--symbol-split", type=float, default=0.8, help="train split ratio by symbol")
    ap.add_argument("--mae-p10-mean-min", type=float, default=MAE_P10_MEAN_MIN)
    ap.add_argument("--min-events-train", type=int, default=MIN_EVENTS_TRAIN)
    ap.add_argument("--min-events-valid", type=int, default=MIN_EVENTS_VALID)
    ap.add_argument("--base-lookback", type=int, default=BASE_LOOKBACK)
    ap.add_argument("--base-minp", type=int, default=BASE_MINP)
    ap.add_argument("--score-lookback", type=int, default=BASE_SCORE_LOOKBACK)
    ap.add_argument("--score-minp", type=int, default=BASE_SCORE_MINP)
    args = ap.parse_args()

    DERIVED.mkdir(parents=True, exist_ok=True)
    init_logging(args.log_level, DERIVED / "tuning_stage1_v3.log")

    workers = args.workers if args.workers > 0 else max(1, (os.cpu_count() or 4) - 1)
    rng = np.random.default_rng(args.seed)

    LOG.info(f"Split mode={args.split_mode}")
    LOG.info(f"Train={args.train_start}..{args.train_end} Valid={args.valid_start}..{args.valid_end} Horizons={HORIZONS}")
    LOG.info(f"Risk guard: mean(mae_p10_5/10/20d) >= {args.mae_p10_mean_min:.3f}")
    LOG.info(f"Min events: train>={args.min_events_train} valid>={args.min_events_valid}")
    LOG.info(
        "Base rolling: lookback=%s, minp=%s, score_lookback=%s, score_minp=%s"
        % (args.base_lookback, args.base_minp, args.score_lookback, args.score_minp)
    )

    base_cache = DERIVED / "tuning_stage1_v3_base.parquet"
    need_cols = set(required_base_columns())
    if base_cache.exists():
        base = pd.read_parquet(base_cache)
        base["date"] = pd.to_datetime(base["date"])
        miss = need_cols - set(base.columns)
        if miss:
            LOG.warning(f"Base cache missing columns: {sorted(miss)}; rebuilding")
            base = build_base_cache(base_cache)
        else:
            LOG.info(f"Load base cache: {base_cache} | {df_range(base)}")
    else:
        base = build_base_cache(base_cache)

    if args.grid:
        grid_path = Path(args.grid)
        grid = json.loads(grid_path.read_text(encoding="utf-8"))
        configs = grid_configs(grid)
        LOG.info(f"Grid configs: {len(configs)}")
    else:
        configs = sample_random_configs(rng, args.trials)
        LOG.info(f"Random trials: {len(configs)} | workers={workers}")

    train_symbols = []
    valid_symbols = []
    if args.split_mode == "symbol":
        uniq = sorted(base["symbol"].dropna().astype(str).unique())
        rng.shuffle(uniq)
        cut = int(len(uniq) * float(args.symbol_split))
        train_symbols = uniq[:cut]
        valid_symbols = uniq[cut:]
        LOG.info(f"Symbol split: train={len(train_symbols)} valid={len(valid_symbols)}")

    # Run trials
    rows: List[dict] = []
    t0 = time.perf_counter()
    ok = 0
    with ProcessPoolExecutor(max_workers=workers) as ex:
        futs = []
        for i, cfg in enumerate(configs, start=1):
            futs.append(ex.submit(run_one_trial_worker, {
                "trial_id": i,
                "config": asdict(cfg),
                "base_path": str(base_cache),
                "train_start": args.train_start,
                "train_end": args.train_end,
                "valid_start": args.valid_start,
                "valid_end": args.valid_end,
                "split_mode": args.split_mode,
                "train_symbols": train_symbols,
                "valid_symbols": valid_symbols,
                "mae_p10_mean_min": args.mae_p10_mean_min,
                "min_events_train": args.min_events_train,
                "min_events_valid": args.min_events_valid,
                "base_lookback": args.base_lookback,
                "base_minp": args.base_minp,
                "score_lookback": args.score_lookback,
                "score_minp": args.score_minp,
            }))

        for j, fut in enumerate(as_completed(futs), start=1):
            res = fut.result()
            rows.append(res)
            ok += 1
            if j % 25 == 0 or j == len(futs):
                dt = time.perf_counter() - t0
                LOG.info(f"Progress {j}/{len(futs)} elapsed={dt:.1f}s avg={dt/j:.2f}s/trial")

    df = pd.DataFrame(rows)
    df_pass = df[df["passed"] == True].copy()
    df_pass = df_pass.sort_values(
        ["valid_mean_mfe", "train_mean_mfe", "valid_n_events"],
        ascending=[False, False, False],
    )

    out_all = DERIVED / "tuning_stage1_v3_all.csv"
    out_best = DERIVED / "tuning_stage1_v3_best.csv"
    df.to_csv(out_all, index=False, encoding="utf-8-sig")
    df_pass.head(200).to_csv(out_best, index=False, encoding="utf-8-sig")

    LOG.info(f"Wrote all trials: {out_all}")
    LOG.info(f"Wrote leaderboard (top 200 passed): {out_best}")
    LOG.info(f"Passed={len(df_pass):,} / Total={len(df):,}")
    LOG.info("DONE.")


if __name__ == "__main__":
    main()
