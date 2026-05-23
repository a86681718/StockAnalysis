#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主力佈局（含權證合併到標的）→ Event-based 回測驗證
- 精準 logging：每個 stage、每 N 檔進度、耗時、輸出筆數
- 多進程加速：parquet 檔案層級並行處理（macOS/Windows 也可用）
- 權證集中度：用「券商層級」合併後再算 HHI / 動態K（不是 proxy）

資料結構（依你描述）：
1) bs_report parquet
   ./data/bs_report/parquet_tpex/*.parquet
   ./data/bs_report/parquet_twse/*.parquet
   欄位 ['價格','券商','日期','買進股數','賣出股數']
   一檔一代號（正股或權證）

2) OHLC
   ./data/ohlc/tpex-YYYYMMDD.csv
   ./data/ohlc/twse-YYYYMMDD.csv

3) 權證 mapping
   ./data/warrant/warrant_list_dedup.csv
   欄位含 [權證代號, 標的代號, ...]
"""

from __future__ import annotations

import os
import re
import time
import json
import logging
import argparse
from pathlib import Path
from dataclasses import dataclass
from typing import Iterable, Optional, Tuple, List

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


# -----------------------------
# Utils
# -----------------------------
def safe_div(a, b):
    return np.where(b == 0, np.nan, a / b)

def hhi_from_shares(shares: pd.Series) -> float:
    shares = shares.dropna()
    if shares.empty:
        return np.nan
    return float((shares ** 2).sum())

def dynamic_top_cover(net_pos_by_broker: pd.Series, cover_x: float) -> pd.Index:
    """Return broker index selected by dynamic K to cover cover_x of positive net."""
    s = net_pos_by_broker.dropna()
    if s.sum() <= 0:
        return s.index[:0]
    s = s.sort_values(ascending=False)
    c = s.cumsum() / s.sum()
    # include first broker that crosses threshold
    under = s.index[c <= cover_x]
    over_first = s.index[c > cover_x][:1]
    return under.append(over_first)

def pct_rank_by_date(df: pd.DataFrame, col: str) -> pd.Series:
    return df.groupby("date")[col].rank(pct=True, method="average")

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
    cover_x: float = 0.6          # 動態K：覆蓋正淨買的比例
    alpha: float = 0.4           # 權證意圖加權
    pct_th: float = 0.95         # 橫斷面分位門檻（前5%）
    persist_days: int = 3        # 需連續幾天達標才算事件
    cooldown: int = 20           # 同股事件冷卻天數（避免重複觸發）
    workers: Optional[int] = None
    log_every: int = 200
    minp_ratio: float = 1/3      # rolling min_periods 比例（w * minp_ratio，至少5）

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
# 1) Load warrant mapping
# -----------------------------
def load_warrant_mapping(cfg: Config) -> pd.DataFrame:
    wm = pd.read_csv(cfg.warrant_map_path, dtype=str)
    # keep minimal columns (你提供的欄位裡必有：權證代號、標的代號)
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
            # [代號, ... 開盤,最高,最低,收盤,成交股數 ...]
            rename = {"代號": "symbol", "開盤": "open", "最高": "high", "最低": "low", "收盤": "close", "成交股數": "volume"}
        else:
            # [證券代號, ... 開盤價,最高價,最低價,收盤價,成交股數 ...]
            rename = {"證券代號": "symbol", "開盤價": "open", "最高價": "high", "最低價": "low", "收盤價": "close", "成交股數": "volume"}

        df = df.rename(columns=rename)
        keep = ["symbol", "date", "open", "high", "low", "close", "volume", "market"]
        df = df[[c for c in keep if c in df.columns]].copy()

        for c in ["open", "high", "low", "close"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df["volume"] = pd.to_numeric(df.get("volume"), errors="coerce")

        df = df.dropna(subset=["symbol", "date", "close"])
        rows.append(df)

    if not rows:
        raise RuntimeError("No OHLC loaded. Check ./data/ohlc filenames.")

    ohlc = pd.concat(rows, ignore_index=True).sort_values(["symbol", "date"])
    ohlc["ret_1d"] = ohlc.groupby("symbol")["close"].pct_change()

    # trend_strength_w = mean(ret)/std(ret)
    for w in [20, 40]:
        minp = max(5, int(w * cfg.minp_ratio))
        grp = ohlc.groupby("symbol")["ret_1d"]
        m = grp.rolling(w, min_periods=minp).mean().reset_index(level=0, drop=True)
        s = grp.rolling(w, min_periods=minp).std().reset_index(level=0, drop=True)
        ohlc[f"trend_strength_{w}"] = safe_div(m, s)

    return ohlc


# -----------------------------
# 3) Parquet processing (parallel)
# -----------------------------
def _read_bs_parquet(parquet_path: Path) -> pd.DataFrame:
    """
    Read minimal columns with coercion.
    Columns: ['價格','券商','日期','買進股數','賣出股數']
    """
    cols = ["價格", "券商", "日期", "買進股數", "賣出股數"]
    try:
        df = pd.read_parquet(parquet_path, columns=cols)
    except TypeError:
        # some parquet engines don't support columns=; fallback
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
    """
    Worker for stock symbol file:
      returns daily summary per symbol/date with:
        net_total, posnet_total, hhi_posnet, top_posnet_ratio, dyn_k
    """
    parquet_path_str, cover_x = args
    p = Path(parquet_path_str)

    df = _read_bs_parquet(p)
    # broker-date
    g = df.groupby(["symbol", "date", "broker"], as_index=False).agg(net=("net", "sum"), net_pos=("net_pos", "sum"))

    # per date compute HHI + dynamic K
    out_rows = []
    sym = p.stem
    for d, sub in g.groupby("date"):
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

        net_total = float(sub["net"].sum())
        out_rows.append({
            "symbol": sym,
            "date": d,
            "net_total": net_total,
            "posnet_total": pos_sum,
            "hhi_posnet": hhi,
            "top_posnet_ratio": top_ratio,
            "dyn_k": dyn_k,
        })

    return pd.DataFrame(out_rows)


def compute_warrant_broker_daily_worker(args: Tuple[str]) -> pd.DataFrame:
    """
    Worker for warrant symbol file:
      returns broker-date net/net_pos per warrant symbol.
    We'll map warrant -> underlying in main and aggregate at broker level.
    """
    (parquet_path_str,) = args
    p = Path(parquet_path_str)
    df = _read_bs_parquet(p)
    # broker-date per warrant
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
# 4) Rolling features
# -----------------------------
def add_rolling_features(flow_daily: pd.DataFrame, prefix: str, cfg: Config, windows=(10, 20, 40)) -> pd.DataFrame:
    df = flow_daily.sort_values(["symbol", "date"]).copy()
    df["is_net_buy"] = (df["net_total"] > 0).astype(int)

    grp = df.groupby("symbol", group_keys=False)

    for w in windows:
        minp = max(5, int(w * cfg.minp_ratio))
        df[f"{prefix}net_buy_days_{w}"] = grp["is_net_buy"].rolling(w, min_periods=minp).sum().reset_index(level=0, drop=True)
        df[f"{prefix}hhi_posnet_{w}"] = grp["hhi_posnet"].rolling(w, min_periods=minp).mean().reset_index(level=0, drop=True)
        df[f"{prefix}top_posnet_ratio_{w}"] = grp["top_posnet_ratio"].rolling(w, min_periods=minp).mean().reset_index(level=0, drop=True)
        df[f"{prefix}dyn_k_{w}"] = grp["dyn_k"].rolling(w, min_periods=minp).mean().reset_index(level=0, drop=True)

    keep = ["symbol", "date", "net_total", "posnet_total", "hhi_posnet", "top_posnet_ratio", "dyn_k"] + \
           [c for c in df.columns if c.startswith(prefix)]
    return df[keep].copy()


# -----------------------------
# 5) Warrant aggregate to underlying (broker-level) -> daily summary
# -----------------------------
def warrant_to_underlying_daily(
    warrant_broker_daily: pd.DataFrame,
    warrant_to_underlying: dict,
    cfg: Config
) -> pd.DataFrame:
    """
    Input: broker-date net/net_pos per warrant symbol.
    Steps:
      1) map warrant->underlying
      2) aggregate to (underlying, date, broker)
      3) compute per (underlying, date) daily summary with HHI + dynamic K
    """
    df = warrant_broker_daily.copy()
    df["underlying"] = df["symbol"].map(warrant_to_underlying)
    df = df.dropna(subset=["underlying"])
    df = df.rename(columns={"underlying": "symbol"})  # reuse symbol field as underlying

    # aggregate broker level across all warrants of the same underlying
    g = df.groupby(["symbol", "date", "broker"], as_index=False).agg(net=("net", "sum"), net_pos=("net_pos", "sum"))

    out_rows = []
    for (sym, d), sub in g.groupby(["symbol", "date"]):
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

        net_total = float(sub["net"].sum())
        out_rows.append({
            "symbol": sym,
            "date": d,
            "net_total": net_total,
            "posnet_total": pos_sum,
            "hhi_posnet": hhi,
            "top_posnet_ratio": top_ratio,
            "dyn_k": dyn_k,
        })

    out = pd.DataFrame(out_rows).sort_values(["symbol", "date"])
    return out


# -----------------------------
# 6) Scoring + Events + Backtest
# -----------------------------
def make_score_table(features: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    df = features.copy()

    # components (use 20d as baseline)
    # stock
    df["C_stock"] = df["stock_hhi_posnet_20"]
    df["P_stock"] = df["stock_net_buy_days_20"]
    # price suppression: lower trend_strength => more suppression => higher score
    df["S_price"] = -df["trend_strength_20"]

    # warrant (merged to underlying)
    df["C_w"] = df["warrant_hhi_posnet_20"]
    df["P_w"] = df["warrant_net_buy_days_20"]

    for c in ["C_stock", "P_stock", "S_price", "C_w", "P_w"]:
        df[f"r_{c}"] = pct_rank_by_date(df, c)

    df["score_raw"] = df["r_C_stock"] + df["r_P_stock"] + df["r_S_price"] + cfg.alpha * (df["r_C_w"] + df["r_P_w"])
    df["rank_pct"] = pct_rank_by_date(df, "score_raw")
    return df


def add_forward_returns(df: pd.DataFrame, horizons=(1, 5, 10, 20)) -> pd.DataFrame:
    df = df.sort_values(["symbol", "date"]).copy()
    g = df.groupby("symbol")
    for h in horizons:
        df[f"fwd_ret_{h}d"] = g["close"].pct_change(periods=h).shift(-h)
    return df


def make_events(df: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    """
    Event when rank_pct >= pct_th for persist_days consecutive days.
    Event day = first day of that streak, with cooldown per symbol.
    """
    df = df.sort_values(["symbol", "date"]).copy()
    df["hit"] = (df["rank_pct"] >= cfg.pct_th).astype(int)

    events = []
    for sym, sub in df.groupby("symbol"):
        sub = sub.copy()
        sub["hit_run"] = sub["hit"].rolling(cfg.persist_days, min_periods=cfg.persist_days).sum()
        candidates_end = sub.index[sub["hit_run"] == cfg.persist_days].to_list()

        # Convert to positions to apply cooldown and pick first day of streak
        idx_list = list(sub.index)
        pos_of = {idx: i for i, idx in enumerate(idx_list)}

        picked = []
        last_event_pos = -10**9
        for end_idx in candidates_end:
            end_pos = pos_of[end_idx]
            first_pos = end_pos - (cfg.persist_days - 1)
            if first_pos - last_event_pos >= cfg.cooldown:
                picked.append(idx_list[first_pos])
                last_event_pos = first_pos

        sub["is_event"] = 0
        sub.loc[picked, "is_event"] = 1
        events.append(sub[["symbol", "date", "is_event"]])

    ev = pd.concat(events, ignore_index=True) if events else pd.DataFrame(columns=["symbol", "date", "is_event"])
    out = df.merge(ev, on=["symbol", "date"], how="left")
    out["is_event"] = out["is_event"].fillna(0).astype(int)
    return out


def summarize_backtest(df: pd.DataFrame, horizons=(1, 5, 10, 20)) -> pd.DataFrame:
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


# -----------------------------
# Main pipeline
# -----------------------------
def main():
    parser = argparse.ArgumentParser(description="主力佈局（含權證）Event-based 回測")
    parser.add_argument("--config", type=str, default="", help="optional json config path")
    parser.add_argument("--log-level", type=str, default="INFO")
    parser.add_argument("--workers", type=int, default=0, help="0=auto")
    args = parser.parse_args()

    cfg = Config()
    if args.config:
        cfg_dict = json.loads(Path(args.config).read_text(encoding="utf-8"))
        cfg = Config(**{**cfg.__dict__, **cfg_dict})  # shallow override

    if args.workers and args.workers > 0:
        cfg.workers = args.workers

    ensure_dir(cfg.derived_dir)
    init_logging(cfg.derived_dir / "run.log", level=args.log_level)

    LOG.info("Config: " + json.dumps(cfg.__dict__, ensure_ascii=False, default=str))

    # auto workers
    if cfg.workers is None:
        cfg.workers = max(1, (os.cpu_count() or 4) - 1)
    LOG.info(f"Using workers={cfg.workers}")

    # Load mapping
    with timed_stage("Load warrant mapping"):
        wm = load_warrant_mapping(cfg)
        warrant_to_underlying = dict(zip(wm["warrant"], wm["underlying"]))
        warrant_set = set(warrant_to_underlying.keys())
        LOG.info(f"Mapping warrants={len(warrant_set):,}, underlyings={wm['underlying'].nunique():,}")

    # OHLC (cache)
    ohlc_cache = cfg.derived_dir / "ohlc.parquet"
    with timed_stage("Load OHLC"):
        if ohlc_cache.exists():
            LOG.info("Load cached ohlc.parquet")
            ohlc = pd.read_parquet(ohlc_cache)
        else:
            ohlc = load_ohlc_all(cfg)
            ohlc.to_parquet(ohlc_cache, index=False)
        LOG.info(f"OHLC rows={len(ohlc):,}, symbols={ohlc['symbol'].nunique():,}")

    # List parquet files
    parquet_files: List[Path] = []
    for d in cfg.bs_dirs:
        parquet_files.extend(sorted(d.glob("*.parquet")))
    LOG.info(f"bs_report parquet files total={len(parquet_files):,}")

    # Split tasks: stock vs warrant (by filename stem)
    stock_tasks: List[Tuple[str, float]] = []
    warrant_tasks: List[Tuple[str]] = []
    for p in parquet_files:
        stem = p.stem
        if stem in warrant_set:
            warrant_tasks.append((str(p),))
        else:
            stock_tasks.append((str(p), cfg.cover_x))

    LOG.info(f"Stock files={len(stock_tasks):,}, Warrant files={len(warrant_tasks):,}")

    # Stock daily summary (cache)
    stock_cache = cfg.derived_dir / "flow_stock_daily.parquet"
    warrant_broker_cache = cfg.derived_dir / "flow_warrant_broker_daily.parquet"
    warrant_under_cache = cfg.derived_dir / "flow_warrant_daily_by_underlying.parquet"

    with timed_stage("Build stock daily flow (parallel)"):
        if stock_cache.exists():
            LOG.info("Load cached flow_stock_daily.parquet")
            flow_stock_daily = pd.read_parquet(stock_cache)
        else:
            res = run_parallel(
                tasks=stock_tasks,
                worker_fn=compute_stock_daily_summary_worker,
                workers=cfg.workers,
                log_every=cfg.log_every,
                task_name="STOCK",
            )
            flow_stock_daily = pd.concat(res, ignore_index=True) if res else pd.DataFrame()
            flow_stock_daily.to_parquet(stock_cache, index=False)
        LOG.info(f"Stock flow daily rows={len(flow_stock_daily):,}, symbols={flow_stock_daily['symbol'].nunique() if not flow_stock_daily.empty else 0:,}")

    with timed_stage("Build warrant broker-daily flow (parallel)"):
        # warrant broker daily can be large; cache it
        if warrant_broker_cache.exists():
            LOG.info("Load cached flow_warrant_broker_daily.parquet")
            warrant_broker_daily = pd.read_parquet(warrant_broker_cache)
        else:
            res = run_parallel(
                tasks=warrant_tasks,
                worker_fn=compute_warrant_broker_daily_worker,
                workers=cfg.workers,
                log_every=cfg.log_every,
                task_name="WARRANT",
            )
            warrant_broker_daily = pd.concat(res, ignore_index=True) if res else pd.DataFrame()
            warrant_broker_daily.to_parquet(warrant_broker_cache, index=False)
        LOG.info(f"Warrant broker-daily rows={len(warrant_broker_daily):,}, warrants={warrant_broker_daily['symbol'].nunique() if not warrant_broker_daily.empty else 0:,}")

    with timed_stage("Aggregate warrants -> underlying daily summary"):
        if warrant_under_cache.exists():
            LOG.info("Load cached flow_warrant_daily_by_underlying.parquet")
            flow_warrant_under_daily = pd.read_parquet(warrant_under_cache)
        else:
            flow_warrant_under_daily = warrant_to_underlying_daily(warrant_broker_daily, warrant_to_underlying, cfg)
            flow_warrant_under_daily.to_parquet(warrant_under_cache, index=False)
        LOG.info(f"Warrant-underlying daily rows={len(flow_warrant_under_daily):,}, underlyings={flow_warrant_under_daily['symbol'].nunique() if not flow_warrant_under_daily.empty else 0:,}")

    # Rolling features
    with timed_stage("Add rolling features (stock)"):
        stock_feat = add_rolling_features(flow_stock_daily, prefix="stock_", cfg=cfg, windows=(10, 20, 40))
        stock_feat.to_parquet(cfg.derived_dir / "flow_stock_daily_rolling.parquet", index=False)
        LOG.info(f"Stock rolling rows={len(stock_feat):,}")

    with timed_stage("Add rolling features (warrant->underlying)"):
        warrant_feat = add_rolling_features(flow_warrant_under_daily, prefix="warrant_", cfg=cfg, windows=(10, 20, 40))
        warrant_feat.to_parquet(cfg.derived_dir / "flow_warrant_underlying_daily_rolling.parquet", index=False)
        LOG.info(f"Warrant rolling rows={len(warrant_feat):,}")

    # Merge features + OHLC (inner join on OHLC to ensure forward returns valid)
    with timed_stage("Merge features + OHLC"):
        feat = stock_feat.merge(
            warrant_feat[["symbol", "date", "warrant_net_buy_days_20", "warrant_hhi_posnet_20"]],
            on=["symbol", "date"],
            how="left",
        )
        feat = feat.merge(
            ohlc[["symbol", "date", "close", "trend_strength_20"]],
            on=["symbol", "date"],
            how="inner",
        )

        # missing warrants => 0 intent (conservative)
        feat["warrant_net_buy_days_20"] = feat["warrant_net_buy_days_20"].fillna(0.0)
        feat["warrant_hhi_posnet_20"] = feat["warrant_hhi_posnet_20"].fillna(0.0)

        feat_cache = cfg.derived_dir / "features.parquet"
        feat.to_parquet(feat_cache, index=False)
        LOG.info(f"Features rows={len(feat):,}, symbols={feat['symbol'].nunique():,}")

    # Score, events, backtest
    with timed_stage("Score + Events + Forward returns"):
        scored = make_score_table(feat, cfg)
        scored = add_forward_returns(scored, horizons=(1, 5, 10, 20))
        scored = make_events(scored, cfg)

        scored.to_parquet(cfg.derived_dir / "scored.parquet", index=False)

    with timed_stage("Summarize backtest"):
        summary = summarize_backtest(scored, horizons=(1, 5, 10, 20))
        summary_path = cfg.derived_dir / "backtest_summary.csv"
        summary.to_csv(summary_path, index=False, encoding="utf-8-sig")

        events_path = cfg.derived_dir / "events.csv"
        scored[scored["is_event"] == 1][
            ["symbol", "date", "rank_pct", "score_raw", "C_stock", "P_stock", "S_price", "C_w", "P_w"]
        ].to_csv(events_path, index=False, encoding="utf-8-sig")

        LOG.info("Backtest summary:\n" + summary.to_string(index=False))
        LOG.info(f"Wrote: {summary_path}")
        LOG.info(f"Wrote: {events_path}")

    LOG.info("DONE.")


if __name__ == "__main__":
    # Required for multiprocessing on macOS/Windows
    main()


# Backtest summary:
#   horizon  n_events      mean    median  win_rate       p25       p75
# 0      1d      1168 -0.037791 -0.004944  0.283390 -0.026025  0.002100
# 1      5d       995 -0.038988 -0.011204  0.322613 -0.042135  0.007273
# 2     10d       854 -0.018506 -0.013146  0.333724 -0.041996  0.011721
# 3     20d       718 -0.014560 -0.019239  0.332869 -0.060953  0.014921