#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import json
import time
import math
import argparse
import logging
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed


LOG = logging.getLogger("tune")


# -----------------------------
# Fixed trading / backtest params (per your decision)
# -----------------------------
TP = 0.08
SL = -0.09
HORIZON = 10
TIE_BREAK = "sl_first"  # "sl_first" | "tp_first" | "skip"


# -----------------------------
# Stage1 base params (kept fixed in this tuning run)
# (We tune only th/W/wI/cooldown)
# -----------------------------
BASE_LOOKBACK = 120
BASE_MINP = 60
BASE_SCORE_LOOKBACK = 60
BASE_SCORE_MINP = 20


# -----------------------------
# Walk-forward CV params (good for 8~9 months)
# We split by global trading-day index.
# -----------------------------
TRAIN_LENS = [60, 80, 100, 120]     # trading days
TEST_LEN = 20                       # trading days
GAP = HORIZON                        # purge gap to avoid leakage


# Constraints / scoring
MIN_EVENTS_PER_FOLD = 8
MIN_EVENTS_TOTAL = 40
LAMBDA_TP_RATE = 0.02  # score = EV + lambda * tp_rate


# -----------------------------
# IO paths
# -----------------------------
DATA_DIR = Path("./data")
DERIVED = DATA_DIR / "_derived"
FEATURES = DERIVED / "features_stock_only.parquet"      # preferred
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
# Stage1 math
# -----------------------------
def rolling_percentile_last(x: np.ndarray) -> float:
    last = x[-1]
    return float(np.sum(x <= last) / len(x))


def add_base_percentiles(feat: pd.DataFrame) -> pd.DataFrame:
    """
    Precompute:
      posnet_total_pct, hhi_posnet_pct
    using fixed BASE_LOOKBACK / BASE_MINP
    """
    out = feat.sort_values(["symbol", "date"]).copy()
    g = out.groupby("symbol", group_keys=False)

    def _pct(s: pd.Series) -> pd.Series:
        return s.rolling(BASE_LOOKBACK, min_periods=BASE_MINP).apply(rolling_percentile_last, raw=True)

    out["posnet_total_pct"] = g["posnet_total"].apply(_pct)
    out["hhi_posnet_pct"] = g["hhi_posnet"].apply(_pct)
    return out


def compute_stage1_score_and_events(
    base: pd.DataFrame,
    wI: float,
    W: int,
    th: float,
    cooldown: int,
) -> pd.DataFrame:
    """
    Compute:
      s_stage1, s_stage1_W, s_stage1_W_pct, is_event
    using fixed BASE_SCORE_LOOKBACK / BASE_SCORE_MINP
    """
    out = base.sort_values(["symbol", "date"]).copy()
    wC = 1.0 - wI

    out["s_stage1"] = wI * out["posnet_total_pct"] + wC * out["hhi_posnet_pct"]

    g = out.groupby("symbol", group_keys=False)
    out["s_stage1_W"] = g["s_stage1"].rolling(W, min_periods=W).sum().reset_index(level=0, drop=True)

    def _pct_score(s: pd.Series) -> pd.Series:
        return s.rolling(BASE_SCORE_LOOKBACK, min_periods=BASE_SCORE_MINP).apply(rolling_percentile_last, raw=True)

    out["s_stage1_W_pct"] = g["s_stage1_W"].apply(_pct_score)

    prev = g["s_stage1_W_pct"].shift(1)
    out["is_event_candidate"] = ((out["s_stage1_W_pct"] >= th) & (prev < th)).astype(int)

    # apply per-symbol cooldown in trading-day index space
    out["is_event"] = 0
    for sym, sub in out.groupby("symbol"):
        idxs = sub.index[sub["is_event_candidate"] == 1].to_list()
        if not idxs:
            continue
        picked = []
        idx_list = list(sub.index)
        pos_of = {idx: i for i, idx in enumerate(idx_list)}
        last_pos = -10**9
        for idx in idxs:
            pos = pos_of[idx]
            if pos - last_pos >= cooldown:
                picked.append(idx)
                last_pos = pos
        out.loc[picked, "is_event"] = 1

    return out


# -----------------------------
# TP/SL backtest (single horizon fixed = 10d)
# -----------------------------
def backtest_tp_sl_single_horizon(
    df: pd.DataFrame,
    tp: float = TP,
    sl: float = SL,
    horizon: int = HORIZON,
    tie_break: str = TIE_BREAK,
) -> pd.DataFrame:
    """
    For each symbol, for rows where is_event==1:
      entry = close[t]
      scan t+1..t+h:
        low <= entry*(1+sl) => SL
        high >= entry*(1+tp) => TP
      else TIME exit close[t+h]
    """
    out = df.sort_values(["symbol", "date"]).copy()
    out["tp_sl_ret_10d"] = np.nan
    out["tp_sl_outcome_10d"] = None

    for sym, sub in out.groupby("symbol"):
        sub = sub.sort_values("date").reset_index()  # original index in "index"
        idx = sub["index"].values
        close = sub["close"].to_numpy(dtype=float)
        high = sub["high"].to_numpy(dtype=float)
        low  = sub["low"].to_numpy(dtype=float)
        is_ev = sub["is_event"].to_numpy(dtype=int)
        n = len(sub)
        if n <= horizon + 1:
            continue

        for i in range(n):
            if is_ev[i] != 1:
                continue
            entry = close[i]
            if not np.isfinite(entry) or entry <= 0:
                continue
            j_end = i + horizon
            if j_end >= n:
                continue

            tp_px = entry * (1.0 + tp)
            sl_px = entry * (1.0 + sl)

            outcome = "TIME"
            exit_px = close[j_end]

            for j in range(i + 1, i + horizon + 1):
                sl_hit = np.isfinite(low[j]) and (low[j] <= sl_px)
                tp_hit = np.isfinite(high[j]) and (high[j] >= tp_px)

                if sl_hit and tp_hit:
                    if tie_break == "skip":
                        outcome = "SKIP"
                        exit_px = np.nan
                        break
                    elif tie_break == "tp_first":
                        outcome = "TP"
                        exit_px = tp_px
                        break
                    else:
                        outcome = "SL"
                        exit_px = sl_px
                        break

                if sl_hit:
                    outcome = "SL"
                    exit_px = sl_px
                    break

                if tp_hit:
                    outcome = "TP"
                    exit_px = tp_px
                    break

            base_idx = idx[i]
            out.loc[base_idx, "tp_sl_outcome_10d"] = outcome
            if outcome != "SKIP":
                out.loc[base_idx, "tp_sl_ret_10d"] = (exit_px / entry) - 1.0

    return out


# -----------------------------
# Walk-forward folds
# -----------------------------
@dataclass(frozen=True)
class Fold:
    fold_id: int
    train_start: pd.Timestamp
    train_end: pd.Timestamp
    test_start: pd.Timestamp
    test_end: pd.Timestamp


def build_folds(unique_dates: List[pd.Timestamp]) -> List[Fold]:
    """
    Build folds in trading-day index space:
      For each train_len in TRAIN_LENS:
        train = [0 .. train_len-1]
        gap = GAP
        test = [train_len+gap .. train_len+gap+TEST_LEN-1]
    Then shift window forward so it stays within available dates.
    We make folds anchored at start, which is fine for 8~9 months.
    """
    d = unique_dates
    n = len(d)
    folds: List[Fold] = []
    fid = 1
    for train_len in TRAIN_LENS:
        t0 = 0
        train_end_i = t0 + train_len - 1
        test_start_i = train_end_i + GAP + 1
        test_end_i = test_start_i + TEST_LEN - 1
        if test_end_i >= n:
            break

        folds.append(Fold(
            fold_id=fid,
            train_start=d[t0],
            train_end=d[train_end_i],
            test_start=d[test_start_i],
            test_end=d[test_end_i],
        ))
        fid += 1

    return folds


def filter_test_events(df: pd.DataFrame, fold: Fold) -> pd.DataFrame:
    m = (df["date"] >= fold.test_start) & (df["date"] <= fold.test_end) & (df["is_event"] == 1)
    return df[m].copy()


# -----------------------------
# Scoring
# -----------------------------
@dataclass
class TrialConfig:
    th: float
    W: int
    wI: float
    cooldown: int

    def to_key(self) -> str:
        return f"th={self.th:.3f}|W={self.W}|wI={self.wI:.3f}|cd={self.cooldown}"


@dataclass
class FoldMetrics:
    fold_id: int
    n_events: int
    n_trades: int
    ev: float
    tp_rate: float
    sl_rate: float
    time_rate: float
    score: float


def compute_fold_metrics(df: pd.DataFrame, fold: Fold) -> FoldMetrics:
    ev_df = filter_test_events(df, fold)
    if ev_df.empty:
        return FoldMetrics(fold.fold_id, 0, 0, np.nan, np.nan, np.nan, np.nan, -np.inf)

    # use only non-skip trades
    o = ev_df["tp_sl_outcome_10d"].dropna()
    r = ev_df["tp_sl_ret_10d"].dropna()
    if len(o) == 0 or len(r) == 0:
        return FoldMetrics(fold.fold_id, int(len(ev_df)), 0, np.nan, np.nan, np.nan, np.nan, -np.inf)

    mask = o != "SKIP"
    o = o[mask]
    r = r.loc[o.index.intersection(r.index)]

    n_trades = int(len(r))
    if n_trades == 0:
        return FoldMetrics(fold.fold_id, int(len(ev_df)), 0, np.nan, np.nan, np.nan, np.nan, -np.inf)

    ev = float(r.mean())
    tp_rate = float((o == "TP").mean())
    sl_rate = float((o == "SL").mean())
    time_rate = float((o == "TIME").mean())

    score = ev + LAMBDA_TP_RATE * tp_rate
    return FoldMetrics(int(fold.fold_id), int(len(ev_df)), n_trades, ev, tp_rate, sl_rate, time_rate, score)


@dataclass
class TrialResult:
    trial_id: int
    config: TrialConfig
    fold_metrics: List[FoldMetrics]

    mean_score: float
    std_score: float
    mean_ev: float
    mean_tp_rate: float
    mean_sl_rate: float
    mean_n_events: float
    total_events: int
    total_trades: int
    passed: bool

    def to_rows(self) -> List[dict]:
        rows = []
        for fm in self.fold_metrics:
            rows.append({
                "trial_id": self.trial_id,
                **asdict(self.config),
                "fold_id": fm.fold_id,
                "n_events": fm.n_events,
                "n_trades": fm.n_trades,
                "ev": fm.ev,
                "tp_rate": fm.tp_rate,
                "sl_rate": fm.sl_rate,
                "time_rate": fm.time_rate,
                "score": fm.score,
            })
        return rows


# -----------------------------
# Random search / Local search
# -----------------------------
def sample_random_configs(rng: np.random.Generator, n: int) -> List[TrialConfig]:
    """
    Sample stage1 configs:
      th: [0.90..0.99] step 0.005
      W: 3..15
      wI: [0.40..0.90] step 0.05
      cooldown: 3..20
    """
    th_grid = np.round(np.arange(0.90, 0.991, 0.005), 3)
    wI_grid = np.round(np.arange(0.40, 0.901, 0.05), 3)

    configs = []
    for _ in range(n):
        th = float(rng.choice(th_grid))
        W = int(rng.integers(3, 16))
        wI = float(rng.choice(wI_grid))
        cooldown = int(rng.integers(3, 21))
        configs.append(TrialConfig(th=th, W=W, wI=wI, cooldown=cooldown))
    return configs


def sample_local_configs(rng: np.random.Generator, seeds: List[TrialConfig], n: int) -> List[TrialConfig]:
    """
    Local search around top configs.
    """
    configs = []
    for _ in range(n):
        base = seeds[int(rng.integers(0, len(seeds)))]
        th = float(np.clip(base.th + rng.normal(0, 0.01), 0.88, 0.995))
        th = float(np.round(th / 0.005) * 0.005)

        W = int(np.clip(base.W + int(rng.integers(-2, 3)), 3, 18))

        wI = float(np.clip(base.wI + rng.normal(0, 0.06), 0.30, 0.95))
        wI = float(np.round(wI / 0.05) * 0.05)

        cooldown = int(np.clip(base.cooldown + int(rng.integers(-5, 6)), 2, 30))
        configs.append(TrialConfig(th=th, W=W, wI=wI, cooldown=cooldown))
    return configs


# -----------------------------
# Worker (run one trial)
# NOTE: we keep data small by passing only parquet path; each worker loads base_pct cache.
# This is slower than fork-sharing memory, but works cross-platform (mac included).
# -----------------------------
def run_one_trial_worker(args: dict) -> dict:
    trial_id = args["trial_id"]
    cfg = TrialConfig(**args["config"])
    base_path = args["base_path"]
    folds = args["folds"]

    base = pd.read_parquet(base_path)
    base["date"] = pd.to_datetime(base["date"])

    df = compute_stage1_score_and_events(base, wI=cfg.wI, W=cfg.W, th=cfg.th, cooldown=cfg.cooldown)
    df = backtest_tp_sl_single_horizon(df)

    fold_metrics = []
    for f in folds:
        fold = Fold(**f)
        fm = compute_fold_metrics(df, fold)
        fold_metrics.append(fm)

    scores = [fm.score for fm in fold_metrics if np.isfinite(fm.score)]
    evs = [fm.ev for fm in fold_metrics if np.isfinite(fm.ev)]
    tp_rates = [fm.tp_rate for fm in fold_metrics if np.isfinite(fm.tp_rate)]
    sl_rates = [fm.sl_rate for fm in fold_metrics if np.isfinite(fm.sl_rate)]
    n_events = [fm.n_events for fm in fold_metrics]

    total_events = int(sum(n_events))
    total_trades = int(sum([fm.n_trades for fm in fold_metrics]))

    # constraints
    passed = True
    if total_events < MIN_EVENTS_TOTAL:
        passed = False
    if any(ne < MIN_EVENTS_PER_FOLD for ne in n_events):
        passed = False
    if len(scores) == 0:
        passed = False

    mean_score = float(np.mean(scores)) if scores else -np.inf
    std_score = float(np.std(scores)) if scores else np.inf
    mean_ev = float(np.mean(evs)) if evs else np.nan
    mean_tp_rate = float(np.mean(tp_rates)) if tp_rates else np.nan
    mean_sl_rate = float(np.mean(sl_rates)) if sl_rates else np.nan
    mean_n_events = float(np.mean(n_events)) if n_events else 0.0

    result = TrialResult(
        trial_id=trial_id,
        config=cfg,
        fold_metrics=fold_metrics,
        mean_score=mean_score,
        std_score=std_score,
        mean_ev=mean_ev,
        mean_tp_rate=mean_tp_rate,
        mean_sl_rate=mean_sl_rate,
        mean_n_events=mean_n_events,
        total_events=total_events,
        total_trades=total_trades,
        passed=passed,
    )

    # Return as JSONable dict
    return {
        "trial_id": trial_id,
        "config": asdict(cfg),
        "passed": passed,
        "mean_score": mean_score,
        "std_score": std_score,
        "mean_ev": mean_ev,
        "mean_tp_rate": mean_tp_rate,
        "mean_sl_rate": mean_sl_rate,
        "mean_n_events": mean_n_events,
        "total_events": total_events,
        "total_trades": total_trades,
        "fold_rows": result.to_rows(),
    }


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


# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trials", type=int, default=300, help="random search trials")
    ap.add_argument("--local-trials", type=int, default=0, help="extra local search trials after random stage")
    ap.add_argument("--workers", type=int, default=0, help="0=auto")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--log-level", type=str, default="INFO")
    ap.add_argument("--topk", type=int, default=20, help="topk used as seeds for local search")
    args = ap.parse_args()

    DERIVED.mkdir(parents=True, exist_ok=True)
    init_logging(args.log_level, DERIVED / "tuning_run.log")

    workers = args.workers if args.workers > 0 else max(1, (os.cpu_count() or 4) - 1)
    rng = np.random.default_rng(args.seed)

    LOG.info(f"Fixed backtest: tp={TP}, sl={SL}, horizon={HORIZON}, tie_break={TIE_BREAK}")
    LOG.info(f"Base rolling: lookback={BASE_LOOKBACK}, minp={BASE_MINP}, score_lookback={BASE_SCORE_LOOKBACK}, score_minp={BASE_SCORE_MINP}")
    LOG.info(f"CV: TRAIN_LENS={TRAIN_LENS}, TEST_LEN={TEST_LEN}, GAP={GAP}")
    LOG.info(f"Constraints: MIN_EVENTS_PER_FOLD={MIN_EVENTS_PER_FOLD}, MIN_EVENTS_TOTAL={MIN_EVENTS_TOTAL}")
    LOG.info(f"Score: EV + {LAMBDA_TP_RATE} * tp_rate")

    # Load feature table
    feat = load_features()
    needed = {"symbol", "date", "posnet_total", "hhi_posnet", "open", "high", "low", "close"}
    miss = needed - set(feat.columns)
    if miss:
        raise ValueError(f"Missing columns in features: {miss}")

    feat = feat.dropna(subset=["symbol", "date", "close", "high", "low", "open"])
    feat["symbol"] = feat["symbol"].astype(str).str.strip()
    feat["date"] = pd.to_datetime(feat["date"])
    LOG.info(f"Features: {df_range(feat)}")

    # Build folds
    unique_dates = sorted(feat["date"].drop_duplicates().tolist())
    folds = build_folds(unique_dates)
    if not folds:
        raise RuntimeError("Not enough dates to build folds. Check your date range / caches.")
    LOG.info("Folds:")
    for f in folds:
        LOG.info(f"  fold{f.fold_id}: train={f.train_start.date()}..{f.train_end.date()} test={f.test_start.date()}..{f.test_end.date()}")

    # Precompute base percentiles ONCE then write cache for workers
    base_cache = DERIVED / "tuning_base_pct.parquet"
    if base_cache.exists():
        base = pd.read_parquet(base_cache)
        base["date"] = pd.to_datetime(base["date"])
        LOG.info(f"Load base pct cache: {base_cache} | {df_range(base)}")
    else:
        t0 = time.perf_counter()
        base = add_base_percentiles(feat)
        base.to_parquet(base_cache, index=False)
        LOG.info(f"Wrote base pct cache: {base_cache} | {df_range(base)} | elapsed={time.perf_counter()-t0:.1f}s")

    # Generate configs
    configs = sample_random_configs(rng, args.trials)
    LOG.info(f"Random trials: {len(configs)} | workers={workers}")

    # Prepare folds for worker (jsonable)
    folds_json = [asdict(f) for f in folds]

    # Run trials
    all_fold_rows: List[dict] = []
    summary_rows: List[dict] = []

    t0 = time.perf_counter()
    ok = 0
    with ProcessPoolExecutor(max_workers=workers) as ex:
        futs = []
        for i, cfg in enumerate(configs, start=1):
            futs.append(ex.submit(run_one_trial_worker, {
                "trial_id": i,
                "config": asdict(cfg),
                "base_path": str(base_cache),
                "folds": folds_json,
            }))

        for j, fut in enumerate(as_completed(futs), start=1):
            res = fut.result()
            ok += 1
            summary_rows.append({k: res[k] for k in [
                "trial_id", "passed", "mean_score", "std_score", "mean_ev", "mean_tp_rate",
                "mean_sl_rate", "mean_n_events", "total_events", "total_trades"
            ]} | res["config"])
            all_fold_rows.extend(res["fold_rows"])

            if j % 25 == 0 or j == len(futs):
                dt = time.perf_counter() - t0
                LOG.info(f"Progress {j}/{len(futs)} elapsed={dt:.1f}s avg={dt/j:.2f}s/trial")

    df_summary = pd.DataFrame(summary_rows)
    df_folds = pd.DataFrame(all_fold_rows)

    # Rank
    df_pass = df_summary[df_summary["passed"] == True].copy()
    df_fail = df_summary[df_summary["passed"] == False].copy()
    df_pass = df_pass.sort_values(["mean_score", "std_score", "total_events"], ascending=[False, True, False])

    # Save
    out_parquet = DERIVED / "tuning_results.parquet"
    out_leader = DERIVED / "tuning_leaderboard.csv"
    out_all = DERIVED / "tuning_all_trials.csv"

    df_folds.to_parquet(out_parquet, index=False)
    df_summary.to_csv(out_all, index=False, encoding="utf-8-sig")
    df_pass.head(200).to_csv(out_leader, index=False, encoding="utf-8-sig")

    LOG.info(f"Wrote folds detail: {out_parquet}")
    LOG.info(f"Wrote all trials summary: {out_all}")
    LOG.info(f"Wrote leaderboard (top 200 passed): {out_leader}")
    LOG.info(f"Passed={len(df_pass):,} / Total={len(df_summary):,}")

    # Optional local search around topk
    if args.local_trials > 0 and len(df_pass) > 0:
        seeds = []
        for _, r in df_pass.head(args.topk).iterrows():
            seeds.append(TrialConfig(th=float(r["th"]), W=int(r["W"]), wI=float(r["wI"]), cooldown=int(r["cooldown"])))

        local_cfgs = sample_local_configs(rng, seeds, args.local_trials)
        LOG.info(f"Local trials: {len(local_cfgs)} around top{len(seeds)} seeds")

        # run local stage (append)
        t1 = time.perf_counter()
        start_id = int(df_summary["trial_id"].max()) + 1
        summary_rows2 = []
        fold_rows2 = []

        with ProcessPoolExecutor(max_workers=workers) as ex:
            futs = []
            for k, cfg in enumerate(local_cfgs, start=0):
                futs.append(ex.submit(run_one_trial_worker, {
                    "trial_id": start_id + k,
                    "config": asdict(cfg),
                    "base_path": str(base_cache),
                    "folds": folds_json,
                }))

            for j, fut in enumerate(as_completed(futs), start=1):
                res = fut.result()
                summary_rows2.append({k: res[k] for k in [
                    "trial_id", "passed", "mean_score", "std_score", "mean_ev", "mean_tp_rate",
                    "mean_sl_rate", "mean_n_events", "total_events", "total_trades"
                ]} | res["config"])
                fold_rows2.extend(res["fold_rows"])

                if j % 25 == 0 or j == len(futs):
                    dt = time.perf_counter() - t1
                    LOG.info(f"Local progress {j}/{len(futs)} elapsed={dt:.1f}s avg={dt/j:.2f}s/trial")

        df_summary2 = pd.DataFrame(summary_rows2)
        df_folds2 = pd.DataFrame(fold_rows2)

        # append and re-rank
        df_summary_all = pd.concat([df_summary, df_summary2], ignore_index=True)
        df_folds_all = pd.concat([df_folds, df_folds2], ignore_index=True)

        df_pass_all = df_summary_all[df_summary_all["passed"] == True].copy()
        df_pass_all = df_pass_all.sort_values(["mean_score", "std_score", "total_events"], ascending=[False, True, False])

        # overwrite outputs
        df_folds_all.to_parquet(out_parquet, index=False)
        df_summary_all.to_csv(out_all, index=False, encoding="utf-8-sig")
        df_pass_all.head(200).to_csv(out_leader, index=False, encoding="utf-8-sig")

        LOG.info("[LOCAL DONE] Rewrote outputs with local trials included.")
        LOG.info(f"Passed={len(df_pass_all):,} / Total={len(df_summary_all):,}")

    LOG.info("DONE.")


if __name__ == "__main__":
    main()
