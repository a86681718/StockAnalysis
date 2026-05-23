#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stage1(吃貨) Soft-score + 累積分數事件偵測 + 分桶回測 + MFE/MAE
==============================================================

新增：
- MFE_H (Maximum Favorable Excursion): 事件日 t 的 close(t) 起算，
  在未來 H 日內最高價 max(high[t+1..t+H]) 能到多少： MFE = max_high/close(t) - 1
- MAE_H (Maximum Adverse Excursion): 未來 H 日內最低價 min(low[t+1..t+H])： MAE = min_low/close(t) - 1
- 賣超側特徵：negnet_total / hhi_negnet / top_negnet_ratio / dyn_k_neg
- 事件分數加入賣方扣分、dyn_k 罰則，並加入布林通道條件

輸出：
- backtest_summary.csv（原本：fwd close return）
- backtest_summary_by_bucket.csv（原本：分桶）
- mfe_summary.csv（新增：MFE/MAE 全體）
- mfe_summary_by_bucket.csv（新增：MFE/MAE 分桶）
"""

from __future__ import annotations

import os
import re
import time
import json
import logging
import argparse
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import Optional, Tuple, List, Dict

import numpy as np
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed


# -----------------------------
# Logging / Timing
# -----------------------------
LOG = logging.getLogger("chip")

def init_logging(log_path: Optional[Path] = None, level: str = "INFO"):
    lvl = getattr(logging, level.upper(), logging.INFO)
    handlers: List[logging.Handler] = [logging.StreamHandler()]
    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_path, encoding="utf-8"))
    logging.basicConfig(
        level=lvl,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=handlers,
    )

class timed_stage:
    def __init__(self, name: str):
        self.name = name
        self.t0 = None
    def __enter__(self):
        self.t0 = time.perf_counter()
        LOG.info(f"[STAGE START] {self.name}")
        return self
    def __exit__(self, exc_type, exc, tb):
        dt = time.perf_counter() - (self.t0 or time.perf_counter())
        if exc:
            LOG.exception(f"[STAGE FAIL] {self.name} ({dt:.2f}s)")
        else:
            LOG.info(f"[STAGE END] {self.name} ({dt:.2f}s)")

def log_df_stats(df: pd.DataFrame, name: str, extra: Optional[Dict[str, str]] = None):
    if df is None or df.empty:
        LOG.info(f"[DATA] {name}: EMPTY")
        return
    info = {
        "rows": f"{len(df):,}",
        "symbols": f"{df['symbol'].nunique():,}" if "symbol" in df.columns else "-",
        "dates": f"{df['date'].nunique():,}" if "date" in df.columns else "-",
        "date_min": str(df["date"].min()) if "date" in df.columns else "-",
        "date_max": str(df["date"].max()) if "date" in df.columns else "-",
    }
    if extra:
        info.update(extra)
    LOG.info("[DATA] " + name + ": " + " | ".join([f"{k}={v}" for k, v in info.items()]))

def log_quantiles(s: pd.Series, name: str):
    s = s.dropna()
    if s.empty:
        LOG.info(f"[Q] {name}: EMPTY")
        return
    qs = s.quantile([0.0, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 1.0])
    LOG.info(f"[Q] {name}: " + ", ".join([f"p{int(k*100):02d}={v:.4f}" for k, v in qs.items()]))


# -----------------------------
# Utils
# -----------------------------
def hhi_from_shares(shares: pd.Series) -> float:
    shares = shares.dropna()
    if shares.empty:
        return np.nan
    return float((shares ** 2).sum())

def dynamic_top_cover(net_pos_by_broker: pd.Series, cover_x: float) -> pd.Index:
    s = net_pos_by_broker.dropna()
    if s.sum() <= 0:
        return s.index[:0]
    s = s.sort_values(ascending=False)
    c = s.cumsum() / s.sum()
    under = s.index[c <= cover_x]
    over_first = s.index[c > cover_x][:1]
    return under.append(over_first)

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)
    return p


# -----------------------------
# Config
# -----------------------------
@dataclass
class Config:
    data_dir: Path = Path("./data")
    derived_dir: Path = Path("./data/_derived")

    cover_x: float = 0.6

    workers: Optional[int] = None
    log_every: int = 200

    # Base rolling percentile for posnet/hhi
    lookback: int = 120
    minp: int = 60

    # Stage1 soft-score + accumulation
    W: int = 10
    wI: float = 0.6
    wC: float = 0.4
    wS: float = 0.3   # sell pressure penalty (sell_ratio)
    wH: float = 0.1   # sell concentration penalty (hhi_negnet_pct)
    wK: float = 0.05  # dyn_k penalty weight

    # Score percentile (救事件，與 base 分開)
    score_lookback: int = 60
    score_minp: int = 20

    # Event
    th: float = 0.95
    cooldown: int = 5

    # Sell pressure / dyn_k penalty
    sell_ratio_cap: float = 3.0
    dyn_k_th: int = 10

    # Bollinger condition
    bb_window: int = 20
    bb_k: float = 2.0
    bb_max: float = 0.85

    # Backtest horizons
    horizons: Tuple[int, ...] = (1, 5, 10, 20, 40, 60)

    # Buckets (based on s_stage1_W_pct)
    bucket_bins: Tuple[float, ...] = (0.95, 0.97, 0.98, 0.99, 1.00001)
    bucket_labels: Tuple[str, ...] = ("0.95-0.97", "0.97-0.98", "0.98-0.99", "0.99-1.00")

    @property
    def bs_dirs(self) -> List[Path]:
        return [
            self.data_dir / "bs_report" / "parquet_twse",
            self.data_dir / "bs_report" / "parquet_tpex",
        ]

    @property
    def ohlc_dir(self) -> Path:
        return self.data_dir / "ohlc"

    @property
    def warrant_map_path(self) -> Path:
        return self.data_dir / "warrant" / "warrant_list_dedup.csv"


# -----------------------------
# 1) Load warrant mapping (for classifying warrant files)
# -----------------------------
def load_warrant_mapping(cfg: Config) -> pd.DataFrame:
    wm = pd.read_csv(cfg.warrant_map_path, dtype=str)
    keep = ["權證代號", "標的代號"]
    missing = [c for c in keep if c not in wm.columns]
    if missing:
        raise ValueError(f"warrant mapping missing columns: {missing}")
    wm = wm[keep].copy()
    wm.rename(columns={"權證代號": "warrant", "標的代號": "underlying"}, inplace=True)
    wm = wm.dropna(subset=["warrant", "underlying"])
    return wm


# -----------------------------
# 2) Load & normalize OHLC
# -----------------------------
def load_ohlc_all(cfg: Config) -> pd.DataFrame:
    rows = []
    patt = re.compile(r"^(twse|tpex)-(\d{8})\.csv$")
    files = sorted(cfg.ohlc_dir.glob("*.csv"))
    LOG.info(f"OHLC files: {len(files)}")

    for p in files:
        m = patt.match(p.name)
        if not m:
            continue
        market, ymd = m.group(1), m.group(2)
        date = pd.to_datetime(ymd, format="%Y%m%d").date()

        df = pd.read_csv(p, dtype=str)
        df["date"] = date
        df["market"] = market

        if market == "tpex":
            rename = {"代號": "symbol", "開盤": "open", "最高": "high", "最低": "low", "收盤": "close", "成交股數": "volume"}
        else:
            rename = {"證券代號": "symbol", "開盤價": "open", "最高價": "high", "最低價": "low", "收盤價": "close", "成交股數": "volume"}

        df = df.rename(columns=rename)
        keep_cols = ["symbol", "date", "open", "high", "low", "close", "volume", "market"]
        df = df[[c for c in keep_cols if c in df.columns]].copy()

        def _to_num(s: pd.Series) -> pd.Series:
            return pd.to_numeric(s.astype(str).str.replace(",", ""), errors="coerce")

        for c in ["open", "high", "low", "close"]:
            if c in df.columns:
                df[c] = _to_num(df[c])
        if "volume" in df.columns:
            df["volume"] = _to_num(df["volume"])

        df = df.dropna(subset=["symbol", "date", "close", "high", "low"])
        rows.append(df)

    if not rows:
        raise RuntimeError("No OHLC loaded. Check ./data/ohlc filenames.")

    ohlc = pd.concat(rows, ignore_index=True).sort_values(["symbol", "date"])
    return ohlc


# -----------------------------
# 3) Parquet processing (parallel)
# -----------------------------
def _read_bs_parquet(parquet_path: Path) -> pd.DataFrame:
    cols = ["價格", "券商", "日期", "買進股數", "賣出股數"]
    try:
        df = pd.read_parquet(parquet_path, columns=cols)
    except TypeError:
        df = pd.read_parquet(parquet_path)
        df = df[[c for c in cols if c in df.columns]].copy()

    df = df.rename(columns={"日期": "date", "券商": "broker", "買進股數": "buy", "賣出股數": "sell", "價格": "price"})
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["buy"] = pd.to_numeric(df["buy"], errors="coerce").fillna(0.0)
    df["sell"] = pd.to_numeric(df["sell"], errors="coerce").fillna(0.0)
    df["net"] = df["buy"] - df["sell"]
    df["net_pos"] = df["net"].clip(lower=0)
    df["symbol"] = parquet_path.stem
    return df[["symbol", "date", "broker", "net", "net_pos"]]


def compute_stock_daily_summary_worker(args: Tuple[str, float]) -> pd.DataFrame:
    parquet_path_str, cover_x = args
    p = Path(parquet_path_str)

    df = _read_bs_parquet(p)
    g = df.groupby(["symbol", "date", "broker"], as_index=False).agg(net=("net", "sum"), net_pos=("net_pos", "sum"))

    out_rows = []
    sym = p.stem

    for d, sub in g.groupby("date"):
        sub = sub.copy()
        sub["net_neg"] = (-sub["net"]).clip(lower=0)

        pos = pd.Series(sub["net_pos"].values, index=sub["broker"].values)
        pos_sum = float(pos.sum())
        if pos_sum > 0:
            shares = pos / pos_sum
            hhi = hhi_from_shares(shares)
            top_idx = dynamic_top_cover(pos, cover_x=cover_x)
            top_sum = float(pos.loc[top_idx].sum()) if len(top_idx) else 0.0
            top_ratio = top_sum / pos_sum
            dyn_k = int(len(top_idx))
        else:
            hhi, top_ratio, dyn_k = np.nan, np.nan, 0

        neg = pd.Series(sub["net_neg"].values, index=sub["broker"].values)
        neg_sum = float(neg.sum())
        if neg_sum > 0:
            shares_neg = neg / neg_sum
            hhi_neg = hhi_from_shares(shares_neg)
            top_idx_neg = dynamic_top_cover(neg, cover_x=cover_x)
            top_sum_neg = float(neg.loc[top_idx_neg].sum()) if len(top_idx_neg) else 0.0
            top_ratio_neg = top_sum_neg / neg_sum
            dyn_k_neg = int(len(top_idx_neg))
        else:
            hhi_neg, top_ratio_neg, dyn_k_neg = np.nan, np.nan, 0

        net_total = float(sub["net"].sum())
        out_rows.append({
            "symbol": sym,
            "date": d,
            "net_total": net_total,
            "posnet_total": pos_sum,
            "hhi_posnet": hhi,
            "top_posnet_ratio": top_ratio,
            "dyn_k": dyn_k,
            "negnet_total": neg_sum,
            "hhi_negnet": hhi_neg,
            "top_negnet_ratio": top_ratio_neg,
            "dyn_k_neg": dyn_k_neg,
        })

    return pd.DataFrame(out_rows)


def compute_warrant_broker_daily_worker(args: Tuple[str]) -> pd.DataFrame:
    (parquet_path_str,) = args
    p = Path(parquet_path_str)
    df = _read_bs_parquet(p)
    g = df.groupby(["symbol", "date", "broker"], as_index=False).agg(net=("net", "sum"), net_pos=("net_pos", "sum"))
    return g


def run_parallel(tasks: List[Tuple], worker_fn, workers: int, log_every: int, task_name: str) -> List[pd.DataFrame]:
    total = len(tasks)
    if total == 0:
        return []
    ok = 0
    fail = 0
    t0 = time.perf_counter()
    results: List[pd.DataFrame] = []

    LOG.info(f"{task_name}: tasks={total}, workers={workers}")

    with ProcessPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(worker_fn, t) for t in tasks]
        for i, fut in enumerate(as_completed(futures), start=1):
            try:
                results.append(fut.result())
                ok += 1
            except Exception:
                fail += 1
                LOG.exception(f"{task_name}: one task failed")

            if (i % log_every) == 0 or i == total:
                dt = time.perf_counter() - t0
                LOG.info(f"{task_name}: {i}/{total} ok={ok} fail={fail} elapsed={dt:.1f}s avg={dt/max(1,i):.3f}s/task")

    return results


# -----------------------------
# 4) Warrant aggregate to underlying daily (cache only)
# -----------------------------
def warrant_to_underlying_daily(
    warrant_broker_daily: pd.DataFrame,
    warrant_to_underlying: Dict[str, str],
    cfg: Config
) -> pd.DataFrame:
    df = warrant_broker_daily.copy()
    df["underlying"] = df["symbol"].map(warrant_to_underlying)
    df = df.dropna(subset=["underlying"])
    # Drop original warrant symbol to avoid duplicate 'symbol' columns after rename
    df = df.drop(columns=["symbol"])
    df = df.rename(columns={"underlying": "symbol"})

    g = df.groupby(["symbol", "date", "broker"], as_index=False).agg(net=("net", "sum"), net_pos=("net_pos", "sum"))

    out_rows = []
    for (sym, d), sub in g.groupby(["symbol", "date"]):
        sub = sub.copy()
        sub["net_neg"] = (-sub["net"]).clip(lower=0)

        pos = pd.Series(sub["net_pos"].values, index=sub["broker"].values)
        pos_sum = float(pos.sum())
        if pos_sum > 0:
            shares = pos / pos_sum
            hhi = hhi_from_shares(shares)
            top_idx = dynamic_top_cover(pos, cover_x=cfg.cover_x)
            top_sum = float(pos.loc[top_idx].sum()) if len(top_idx) else 0.0
            top_ratio = top_sum / pos_sum
            dyn_k = int(len(top_idx))
        else:
            hhi, top_ratio, dyn_k = np.nan, np.nan, 0

        neg = pd.Series(sub["net_neg"].values, index=sub["broker"].values)
        neg_sum = float(neg.sum())
        if neg_sum > 0:
            shares_neg = neg / neg_sum
            hhi_neg = hhi_from_shares(shares_neg)
            top_idx_neg = dynamic_top_cover(neg, cover_x=cfg.cover_x)
            top_sum_neg = float(neg.loc[top_idx_neg].sum()) if len(top_idx_neg) else 0.0
            top_ratio_neg = top_sum_neg / neg_sum
            dyn_k_neg = int(len(top_idx_neg))
        else:
            hhi_neg, top_ratio_neg, dyn_k_neg = np.nan, np.nan, 0

        net_total = float(sub["net"].sum())
        out_rows.append({
            "symbol": sym,
            "date": d,
            "net_total": net_total,
            "posnet_total": pos_sum,
            "hhi_posnet": hhi,
            "top_posnet_ratio": top_ratio,
            "dyn_k": dyn_k,
            "negnet_total": neg_sum,
            "hhi_negnet": hhi_neg,
            "top_negnet_ratio": top_ratio_neg,
            "dyn_k_neg": dyn_k_neg,
        })

    return pd.DataFrame(out_rows).sort_values(["symbol", "date"])


# -----------------------------
# 5) Stage1 soft score + events
# -----------------------------
def rolling_percentile_last(x: np.ndarray) -> float:
    last = x[-1]
    return float(np.sum(x <= last) / len(x))

def add_stage1_soft_score(df: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    out = df.sort_values(["symbol", "date"]).copy()
    g = out.groupby("symbol", group_keys=False)

    def _pct_base(s: pd.Series) -> pd.Series:
        return s.rolling(cfg.lookback, min_periods=cfg.minp).apply(rolling_percentile_last, raw=True)

    def _pct_score(s: pd.Series) -> pd.Series:
        return s.rolling(cfg.score_lookback, min_periods=cfg.score_minp).apply(rolling_percentile_last, raw=True)

    out["posnet_total_pct"] = g["posnet_total"].apply(_pct_base)
    out["hhi_posnet_pct"] = g["hhi_posnet"].apply(_pct_base)
    out["negnet_total_pct"] = g["negnet_total"].apply(_pct_base)
    out["hhi_negnet_pct"] = g["hhi_negnet"].apply(_pct_base)

    out["s_stage1"] = cfg.wI * out["posnet_total_pct"] + cfg.wC * out["hhi_posnet_pct"]
    out["s_stage1_W"] = g["s_stage1"].rolling(cfg.W, min_periods=cfg.W).sum().reset_index(level=0, drop=True)
    out["s_stage1_W_pct"] = g["s_stage1_W"].apply(_pct_score)

    sell_ratio = out["negnet_total"] / out["posnet_total"].replace(0, np.nan)
    out["sell_ratio"] = sell_ratio.clip(lower=0, upper=cfg.sell_ratio_cap)
    out["dyn_k_penalty"] = (out["dyn_k"] - cfg.dyn_k_th).clip(lower=0) / float(cfg.dyn_k_th)

    out["s_stage1_adj"] = (
        out["s_stage1"]
        - cfg.wS * out["sell_ratio"]
        - cfg.wH * out["hhi_negnet_pct"]
        - cfg.wK * out["dyn_k_penalty"]
    )
    out["s_stage1_adj_W"] = g["s_stage1_adj"].rolling(cfg.W, min_periods=cfg.W).sum().reset_index(level=0, drop=True)
    out["s_stage1_adj_W_pct"] = g["s_stage1_adj_W"].apply(_pct_score)

    # Bollinger %B
    ma = g["close"].rolling(cfg.bb_window, min_periods=cfg.bb_window).mean().reset_index(level=0, drop=True)
    std = g["close"].rolling(cfg.bb_window, min_periods=cfg.bb_window).std().reset_index(level=0, drop=True)
    upper = ma + cfg.bb_k * std
    lower = ma - cfg.bb_k * std
    out["bb_pos"] = (out["close"] - lower) / (upper - lower)

    return out

def make_stage1_events_soft(df: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    out = df.sort_values(["symbol", "date"]).copy()
    g = out.groupby("symbol")

    score_col = "s_stage1_adj_W_pct" if "s_stage1_adj_W_pct" in out.columns else "s_stage1_W_pct"
    prev = g[score_col].shift(1)
    cand = (out[score_col] >= cfg.th) & (prev < cfg.th)
    if "bb_pos" in out.columns:
        cand &= out["bb_pos"] <= cfg.bb_max
    out["is_event_candidate"] = cand.astype(int)

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
            if pos - last_pos >= cfg.cooldown:
                picked.append(idx)
                last_pos = pos
        out.loc[picked, "is_event"] = 1

    return out


# -----------------------------
# 6) Buckets
# -----------------------------
def add_stage1_buckets(df: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    out = df.copy()
    score_col = "s_stage1_adj_W_pct" if "s_stage1_adj_W_pct" in out.columns else "s_stage1_W_pct"
    out["stage1_bucket"] = pd.cut(
        out[score_col],
        bins=list(cfg.bucket_bins),
        labels=list(cfg.bucket_labels),
        right=False
    )
    return out


# -----------------------------
# 7) Backtest: fwd close returns
# -----------------------------
def add_forward_returns(df: pd.DataFrame, horizons: Tuple[int, ...]) -> pd.DataFrame:
    df = df.sort_values(["symbol", "date"]).copy()
    g = df.groupby("symbol")
    for h in horizons:
        df[f"fwd_ret_{h}d"] = g["close"].pct_change(periods=h).shift(-h)
    return df


def summarize_returns(df: pd.DataFrame, horizons: Tuple[int, ...]) -> pd.DataFrame:
    ev = df[df["is_event"] == 1].copy()
    rows = []
    for h in horizons:
        r = ev[f"fwd_ret_{h}d"].dropna()
        rows.append({
            "horizon": f"{h}d",
            "n_events": int(r.shape[0]),
            "mean": float(r.mean()) if len(r) else np.nan,
            "median": float(r.median()) if len(r) else np.nan,
            "win_rate": float((r > 0).mean()) if len(r) else np.nan,
            "p25": float(r.quantile(0.25)) if len(r) else np.nan,
            "p75": float(r.quantile(0.75)) if len(r) else np.nan,
        })
    return pd.DataFrame(rows)

def summarize_returns_by_bucket(df: pd.DataFrame, horizons: Tuple[int, ...]) -> pd.DataFrame:
    rows = []
    ev = df[(df["is_event"] == 1) & (df["stage1_bucket"].notna())].copy()
    for bucket, sub in ev.groupby("stage1_bucket"):
        for h in horizons:
            r = sub[f"fwd_ret_{h}d"].dropna()
            rows.append({
                "bucket": str(bucket),
                "horizon": f"{h}d",
                "n_events": int(r.shape[0]),
                "mean": float(r.mean()) if len(r) else np.nan,
                "median": float(r.median()) if len(r) else np.nan,
                "win_rate": float((r > 0).mean()) if len(r) else np.nan,
                "p25": float(r.quantile(0.25)) if len(r) else np.nan,
                "p75": float(r.quantile(0.75)) if len(r) else np.nan,
            })
    return pd.DataFrame(rows)


# -----------------------------
# 8) MFE / MAE
# -----------------------------
def add_mfe_mae(df: pd.DataFrame, horizons: Tuple[int, ...]) -> pd.DataFrame:
    """
    For each horizon h:
      max_high_next_h = max(high[t+1..t+h])
      min_low_next_h  = min(low[t+1..t+h])
      mfe_hd = max_high_next_h / close(t) - 1
      mae_hd = min_low_next_h  / close(t) - 1
    """
    out = df.sort_values(["symbol", "date"]).copy()
    g = out.groupby("symbol", group_keys=False)

    for h in horizons:
        # shift(-1) makes window start at t+1
        max_high = g["high"].apply(lambda s: s.shift(-1).rolling(window=h, min_periods=h).max().shift(-(h-1)))
        min_low  = g["low"].apply(lambda s: s.shift(-1).rolling(window=h, min_periods=h).min().shift(-(h-1)))

        out[f"max_high_{h}d"] = max_high
        out[f"min_low_{h}d"] = min_low

        out[f"mfe_{h}d"] = out[f"max_high_{h}d"] / out["close"] - 1.0
        out[f"mae_{h}d"] = out[f"min_low_{h}d"] / out["close"] - 1.0

    return out


def summarize_mfe_mae(df: pd.DataFrame, horizons: Tuple[int, ...]) -> pd.DataFrame:
    """
    Summarize MFE/MAE on event rows.
    """
    ev = df[df["is_event"] == 1].copy()
    rows = []
    for h in horizons:
        mfe = ev[f"mfe_{h}d"].dropna()
        mae = ev[f"mae_{h}d"].dropna()

        rows.append({
            "horizon": f"{h}d",
            "n_events": int(mfe.shape[0]),

            # MFE distribution
            "mfe_mean": float(mfe.mean()) if len(mfe) else np.nan,
            "mfe_median": float(mfe.median()) if len(mfe) else np.nan,
            "mfe_p25": float(mfe.quantile(0.25)) if len(mfe) else np.nan,
            "mfe_p75": float(mfe.quantile(0.75)) if len(mfe) else np.nan,
            "mfe_p90": float(mfe.quantile(0.90)) if len(mfe) else np.nan,
            "mfe_max": float(mfe.max()) if len(mfe) else np.nan,   # ★新增

            # MAE distribution
            "mae_mean": float(mae.mean()) if len(mae) else np.nan,
            "mae_median": float(mae.median()) if len(mae) else np.nan,
            "mae_p25": float(mae.quantile(0.25)) if len(mae) else np.nan,
            "mae_p75": float(mae.quantile(0.75)) if len(mae) else np.nan,
            "mae_p10": float(mae.quantile(0.10)) if len(mae) else np.nan,
        })
    return pd.DataFrame(rows)


def summarize_mfe_mae_by_bucket(df: pd.DataFrame, horizons: Tuple[int, ...]) -> pd.DataFrame:
    ev = df[(df["is_event"] == 1) & (df["stage1_bucket"].notna())].copy()
    rows = []
    for bucket, sub in ev.groupby("stage1_bucket"):
        for h in horizons:
            mfe = sub[f"mfe_{h}d"].dropna()
            mae = sub[f"mae_{h}d"].dropna()
            rows.append({
                "bucket": str(bucket),
                "horizon": f"{h}d",
                "n_events": int(mfe.shape[0]),

                "mfe_median": float(mfe.median()) if len(mfe) else np.nan,
                "mfe_p75": float(mfe.quantile(0.75)) if len(mfe) else np.nan,
                "mfe_p90": float(mfe.quantile(0.90)) if len(mfe) else np.nan,
                "mfe_max": float(mfe.max()) if len(mfe) else np.nan,    # ★新增

                "mae_median": float(mae.median()) if len(mae) else np.nan,
                "mae_p10": float(mae.quantile(0.10)) if len(mae) else np.nan,
            })
    return pd.DataFrame(rows)


# -----------------------------
# Main
# -----------------------------
def main():
    parser = argparse.ArgumentParser(description="Stage1 Soft-score 吃貨事件 + 分桶回測 + MFE/MAE")
    parser.add_argument("--config", type=str, default="", help="optional json config path")
    parser.add_argument("--log-level", type=str, default="INFO")
    parser.add_argument("--workers", type=int, default=0, help="0=auto")
    parser.add_argument("--no-cache", action="store_true", help="ignore caches and rebuild")
    args = parser.parse_args()

    cfg = Config()
    if args.config:
        cfg_dict = json.loads(Path(args.config).read_text(encoding="utf-8"))
        for k, v in cfg_dict.items():
            if hasattr(cfg, k):
                setattr(cfg, k, v)

    if args.workers and args.workers > 0:
        cfg.workers = args.workers

    ensure_dir(cfg.derived_dir)
    init_logging(cfg.derived_dir / "run.log", level=args.log_level)

    if cfg.workers is None:
        cfg.workers = max(1, (os.cpu_count() or 4) - 1)

    LOG.info("Config: " + json.dumps(asdict(cfg), ensure_ascii=False, default=str))
    LOG.info(f"Using workers={cfg.workers} | no_cache={args.no_cache}")

    # ---- warrant mapping (for classification)
    with timed_stage("Load warrant mapping"):
        wm = load_warrant_mapping(cfg)
        warrant_to_underlying = dict(zip(wm["warrant"], wm["underlying"]))
        warrant_set = set(warrant_to_underlying.keys())
        LOG.info(f"Mapping warrants={len(warrant_set):,}, underlyings={wm['underlying'].nunique():,}")

    # ---- OHLC cache
    ohlc_cache = cfg.derived_dir / "ohlc.parquet"
    with timed_stage("Load OHLC"):
        if (not args.no_cache) and ohlc_cache.exists():
            LOG.info("Load cached ohlc.parquet")
            ohlc = pd.read_parquet(ohlc_cache)
        else:
            ohlc = load_ohlc_all(cfg)
            ohlc.to_parquet(ohlc_cache, index=False)
        log_df_stats(ohlc, "OHLC")

    # ---- list parquet
    parquet_files: List[Path] = []
    for d in cfg.bs_dirs:
        parquet_files.extend(sorted(d.glob("*.parquet")))
    LOG.info(f"bs_report parquet files total={len(parquet_files):,}")

    # ---- split tasks: stock vs warrant
    stock_tasks: List[Tuple[str, float]] = []
    warrant_tasks: List[Tuple[str]] = []
    for p in parquet_files:
        stem = p.stem
        if stem in warrant_set:
            warrant_tasks.append((str(p),))
        else:
            stock_tasks.append((str(p), cfg.cover_x))

    LOG.info(f"Stock files={len(stock_tasks):,}, Warrant files={len(warrant_tasks):,}")

    stock_cache = cfg.derived_dir / "flow_stock_daily.parquet"
    warrant_broker_cache = cfg.derived_dir / "flow_warrant_broker_daily.parquet"
    warrant_under_cache = cfg.derived_dir / "flow_warrant_daily_by_underlying.parquet"

    # ---- stock daily flow
    with timed_stage("Build stock daily flow (parallel)"):
        if (not args.no_cache) and stock_cache.exists():
            LOG.info("Load cached flow_stock_daily.parquet")
            flow_stock_daily = pd.read_parquet(stock_cache)
        else:
            res = run_parallel(stock_tasks, compute_stock_daily_summary_worker, cfg.workers, cfg.log_every, "STOCK")
            flow_stock_daily = pd.concat(res, ignore_index=True) if res else pd.DataFrame()
            flow_stock_daily.to_parquet(stock_cache, index=False)

        log_df_stats(flow_stock_daily, "Flow-StockDaily",
                     extra={"posnet_total_mean": f"{flow_stock_daily['posnet_total'].mean():.2f}" if not flow_stock_daily.empty else "-"})

    # ---- warrant caches (optional)
    with timed_stage("Build warrant broker-daily flow (parallel) [cache only]"):
        if (not args.no_cache) and warrant_broker_cache.exists():
            LOG.info("Load cached flow_warrant_broker_daily.parquet")
            warrant_broker_daily = pd.read_parquet(warrant_broker_cache)
        else:
            res = run_parallel(warrant_tasks, compute_warrant_broker_daily_worker, cfg.workers, cfg.log_every, "WARRANT")
            warrant_broker_daily = pd.concat(res, ignore_index=True) if res else pd.DataFrame()
            warrant_broker_daily.to_parquet(warrant_broker_cache, index=False)
        log_df_stats(warrant_broker_daily, "Flow-WarrantBrokerDaily")

    with timed_stage("Aggregate warrants -> underlying daily summary [cache only]"):
        if (not args.no_cache) and warrant_under_cache.exists():
            LOG.info("Load cached flow_warrant_daily_by_underlying.parquet")
            _ = pd.read_parquet(warrant_under_cache)
        else:
            flow_warrant_under_daily = warrant_to_underlying_daily(warrant_broker_daily, warrant_to_underlying, cfg)
            flow_warrant_under_daily.to_parquet(warrant_under_cache, index=False)
            log_df_stats(flow_warrant_under_daily, "Flow-WarrantUnderlyingDaily")

    # ---- merge features (stock flow + OHLC)  ★改：帶 high/low
    with timed_stage("Merge stock flow with OHLC"):
        feat = flow_stock_daily.merge(
            ohlc[["symbol", "date", "open", "close", "high", "low"]],
            on=["symbol", "date"],
            how="inner",
        )
        feat_path = cfg.derived_dir / "features_stock_only.parquet"
        feat.to_parquet(feat_path, index=False)
        log_df_stats(feat, "Features-StockOnly")

    # ---- Stage1 soft score
    with timed_stage("Stage1 Soft-score + Accumulation"):
        feat2 = add_stage1_soft_score(feat, cfg)
        feat2_path = cfg.derived_dir / "features_with_stage1_soft.parquet"
        feat2.to_parquet(feat2_path, index=False)

        log_quantiles(feat2["posnet_total_pct"], "posnet_total_pct")
        log_quantiles(feat2["hhi_posnet_pct"], "hhi_posnet_pct")
        log_quantiles(feat2["negnet_total_pct"], "negnet_total_pct")
        log_quantiles(feat2["hhi_negnet_pct"], "hhi_negnet_pct")
        log_quantiles(feat2["s_stage1"], "s_stage1")
        log_quantiles(feat2["s_stage1_W"], "s_stage1_W")
        log_quantiles(feat2["s_stage1_W_pct"], "s_stage1_W_pct")
        log_quantiles(feat2["s_stage1_adj"], "s_stage1_adj")
        log_quantiles(feat2["s_stage1_adj_W"], "s_stage1_adj_W")
        log_quantiles(feat2["s_stage1_adj_W_pct"], "s_stage1_adj_W_pct")
        log_quantiles(feat2["sell_ratio"], "sell_ratio")
        log_quantiles(feat2["dyn_k_penalty"], "dyn_k_penalty")
        log_quantiles(feat2["bb_pos"], "bb_pos")

        valid_ratio = float(feat2["s_stage1_W_pct"].notna().mean())
        LOG.info(f"[CHECK] s_stage1_W_pct valid ratio={valid_ratio:.4f} (need enough history)")

    # ---- Events + returns + MFE/MAE + buckets
    with timed_stage("Events + Returns + MFE/MAE + Buckets"):
        scored = add_forward_returns(feat2, horizons=cfg.horizons)
        scored = add_mfe_mae(scored, horizons=cfg.horizons)     # ★新增
        scored = make_stage1_events_soft(scored, cfg)
        scored = add_stage1_buckets(scored, cfg)

        scored_path = cfg.derived_dir / "scored.parquet"
        scored.to_parquet(scored_path, index=False)

        n_events = int(scored["is_event"].sum())
        LOG.info(f"[EVENT] total events={n_events:,} | event_rate={scored['is_event'].mean():.6f}")

        # close-return summary
        ret_summary = summarize_returns(scored, horizons=cfg.horizons)
        ret_summary_path = cfg.derived_dir / "backtest_summary.csv"
        ret_summary.to_csv(ret_summary_path, index=False, encoding="utf-8-sig")
        LOG.info("Backtest summary (close returns):\n" + ret_summary.to_string(index=False))
        LOG.info(f"Wrote: {ret_summary_path}")

        ret_bucket = summarize_returns_by_bucket(scored, horizons=cfg.horizons)
        ret_bucket_path = cfg.derived_dir / "backtest_summary_by_bucket.csv"
        ret_bucket.to_csv(ret_bucket_path, index=False, encoding="utf-8-sig")
        LOG.info("Backtest summary by bucket (close returns):\n" + ret_bucket.to_string(index=False))
        LOG.info(f"Wrote: {ret_bucket_path}")

        # MFE/MAE summary  ★新增
        mfe_summary = summarize_mfe_mae(scored, horizons=cfg.horizons)
        mfe_summary_path = cfg.derived_dir / "mfe_summary.csv"
        mfe_summary.to_csv(mfe_summary_path, index=False, encoding="utf-8-sig")
        LOG.info("MFE/MAE summary:\n" + mfe_summary.to_string(index=False))
        LOG.info(f"Wrote: {mfe_summary_path}")

        mfe_bucket = summarize_mfe_mae_by_bucket(scored, horizons=cfg.horizons)
        mfe_bucket_path = cfg.derived_dir / "mfe_summary_by_bucket.csv"
        mfe_bucket.to_csv(mfe_bucket_path, index=False, encoding="utf-8-sig")
        LOG.info("MFE/MAE by bucket:\n" + mfe_bucket.to_string(index=False))
        LOG.info(f"Wrote: {mfe_bucket_path}")

        # events csv for inspection
        events_path = cfg.derived_dir / "events.csv"
        cols = [
            "symbol", "date",
            "posnet_total", "hhi_posnet",
            "negnet_total", "hhi_negnet",
            "posnet_total_pct", "hhi_posnet_pct",
            "negnet_total_pct", "hhi_negnet_pct",
            "sell_ratio", "dyn_k", "dyn_k_penalty",
            "s_stage1", "s_stage1_W", "s_stage1_W_pct",
            "s_stage1_adj", "s_stage1_adj_W", "s_stage1_adj_W_pct",
            "bb_pos",
            "stage1_bucket",
        ]
        scored[scored["is_event"] == 1][[c for c in cols if c in scored.columns]].to_csv(
            events_path, index=False, encoding="utf-8-sig"
        )
        LOG.info(f"Wrote: {events_path}")
        LOG.info(f"Wrote: {scored_path}")

    LOG.info("DONE.")


if __name__ == "__main__":
    main()
