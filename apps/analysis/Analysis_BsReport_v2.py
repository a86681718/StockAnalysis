#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
吃貨狀態事件（rolling percentile）→ Event-based 回測驗證

你選的版本：A) rolling percentile
事件定義（不看券商名字）：
- 強度 Intensity：posnet_total 在該股 lookback 視窗內的 rolling percentile >= I_th
- 集中度 Concentration：hhi_posnet 在該股 lookback 視窗內的 rolling percentile >= C_th
- 狀態 state = Intensity & Concentration
- 事件 event = 近 W 日 state 天數第一次從 <P_min 跨越到 >=P_min（狀態進入點），並套用 cooldown

保留：
- 多進程平行讀 parquet（檔案粒度）
- 精準 logging（每 stage、每 N 檔進度、耗時）
- 權證合併到標的：券商層級合併後再算 HHI / 動態K（非 proxy）
- OHLC 用於 forward return 回測

輸出（./data/_derived/）：
- run.log
- backtest_summary.csv
- events.csv
- scored.parquet
- 以及各種 cache parquet（加速重跑）
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

    # Broker distribution
    cover_x: float = 0.6  # 動態K：覆蓋正淨買的比例（用於 dyn_k/top_ratio；不是事件核心，但仍保留）

    # Multiprocessing / logging
    workers: Optional[int] = None
    log_every: int = 200

    # Rolling min periods ratio for generic rolling
    minp_ratio: float = 1/3

    # Eat-up state (rolling percentile) config
    lookback: int = 120     # rolling percentile lookback
    minp: int = 60          # rolling percentile min periods
    I_th: float = 0.95      # intensity threshold
    C_th: float = 0.90      # concentration threshold
    W: int = 10             # persistence window
    P_min: int = 4          # minimum state days within W to trigger event
    cooldown: int = 20      # avoid repeated events for same symbol

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

        for c in ["open", "high", "low", "close"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df["volume"] = pd.to_numeric(df.get("volume"), errors="coerce")

        df = df.dropna(subset=["symbol", "date", "close"])
        rows.append(df)

    if not rows:
        raise RuntimeError("No OHLC loaded. Check ./data/ohlc filenames.")

    ohlc = pd.concat(rows, ignore_index=True).sort_values(["symbol", "date"])
    ohlc["ret_1d"] = ohlc.groupby("symbol")["close"].pct_change()
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
# 4) Warrant aggregate to underlying (broker-level) -> daily summary
# -----------------------------
def warrant_to_underlying_daily(
    warrant_broker_daily: pd.DataFrame,
    warrant_to_underlying: Dict[str, str],
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
# 5) rolling percentile based state + events
# -----------------------------
def rolling_percentile_last(x: np.ndarray) -> float:
    """Percentile rank of the last element within x (0..1)."""
    # rank within window: (count <= last)/n
    last = x[-1]
    return float(np.sum(x <= last) / len(x))

def add_accumulation_state(df: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    """
    Adds:
      posnet_total_pct, hhi_posnet_pct (rolling percentile)
      is_intensity, is_concentration, is_state
    Note: computed per symbol (time-series), not cross-sectional.
    """
    out = df.sort_values(["symbol", "date"]).copy()
    g = out.groupby("symbol", group_keys=False)

    def _pct_series(s: pd.Series) -> pd.Series:
        return s.rolling(cfg.lookback, min_periods=cfg.minp).apply(rolling_percentile_last, raw=True)

    out["posnet_total_pct"] = g["posnet_total"].apply(_pct_series)
    out["hhi_posnet_pct"] = g["hhi_posnet"].apply(_pct_series)

    out["is_intensity"] = (out["posnet_total_pct"] >= cfg.I_th).astype(int)
    out["is_concentration"] = (out["hhi_posnet_pct"] >= cfg.C_th).astype(int)
    out["is_state"] = ((out["is_intensity"] == 1) & (out["is_concentration"] == 1)).astype(int)
    return out

def make_accumulation_events(df: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    """
    Event when in last W days, state_days crosses >= P_min for the first time.
    Then apply cooldown per symbol.
    Adds:
      state_days_W, is_event
    """
    out = df.sort_values(["symbol", "date"]).copy()
    g = out.groupby("symbol", group_keys=False)

    out["state_days_W"] = g["is_state"].rolling(cfg.W, min_periods=cfg.W).sum().reset_index(level=0, drop=True)

    prev = out.groupby("symbol")["state_days_W"].shift(1)
    out["is_event_candidate"] = ((out["state_days_W"] >= cfg.P_min) & (prev < cfg.P_min)).astype(int)

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
# 6) Backtest helpers
# -----------------------------
def add_forward_returns(df: pd.DataFrame, horizons=(1, 5, 10, 20)) -> pd.DataFrame:
    df = df.sort_values(["symbol", "date"]).copy()
    g = df.groupby("symbol")
    for h in horizons:
        df[f"fwd_ret_{h}d"] = g["close"].pct_change(periods=h).shift(-h)
    return df

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
# Main
# -----------------------------
def main():
    parser = argparse.ArgumentParser(description="吃貨狀態事件（rolling percentile）回測")
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

    # auto workers
    if cfg.workers is None:
        cfg.workers = max(1, (os.cpu_count() or 4) - 1)

    LOG.info("Config: " + json.dumps(cfg.__dict__, ensure_ascii=False, default=str))
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

    # Merge stock + warrant to one flow table (per symbol/date)
    # 事件是「不看券商名字」的吃貨狀態，最核心用 stock 的 posnet_total/hhi_posnet
    # 但這版仍保留 warrant under daily 方便你後續擴充（目前不納入 state）
    with timed_stage("Merge stock flow with OHLC"):
        feat = flow_stock_daily.merge(
            ohlc[["symbol", "date", "close"]],
            on=["symbol", "date"],
            how="inner",
        )
        feat_cache = cfg.derived_dir / "features_stock_only.parquet"
        feat.to_parquet(feat_cache, index=False)
        LOG.info(f"Features rows={len(feat):,}, symbols={feat['symbol'].nunique():,}")

    # Add state + events + forward returns + summarize
    with timed_stage("Detect accumulation state (rolling percentile)"):
        feat2 = add_accumulation_state(feat, cfg)
        feat2.to_parquet(cfg.derived_dir / "features_with_state.parquet", index=False)
        LOG.info("State coverage: "
                 f"intensity={feat2['is_intensity'].mean():.4f}, "
                 f"concentration={feat2['is_concentration'].mean():.4f}, "
                 f"state={feat2['is_state'].mean():.4f}")

    with timed_stage("Forward returns + Events"):
        scored = add_forward_returns(feat2, horizons=(1, 5, 10, 20))
        scored = make_accumulation_events(scored, cfg)
        scored.to_parquet(cfg.derived_dir / "scored.parquet", index=False)
        LOG.info(f"Events count={int(scored['is_event'].sum())}")

    with timed_stage("Summarize backtest"):
        summary = summarize_backtest(scored, horizons=(1, 5, 10, 20))
        summary_path = cfg.derived_dir / "backtest_summary.csv"
        summary.to_csv(summary_path, index=False, encoding="utf-8-sig")

        events_path = cfg.derived_dir / "events.csv"
        scored[scored["is_event"] == 1][
            ["symbol", "date",
             "posnet_total", "hhi_posnet",
             "posnet_total_pct", "hhi_posnet_pct",
             "state_days_W"]
        ].to_csv(events_path, index=False, encoding="utf-8-sig")

        LOG.info("Backtest summary:\n" + summary.to_string(index=False))
        LOG.info(f"Wrote: {summary_path}")
        LOG.info(f"Wrote: {events_path}")

    LOG.info("DONE.")


if __name__ == "__main__":
    # Required for multiprocessing on macOS/Windows
    main()
