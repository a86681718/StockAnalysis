#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import argparse
import logging
from pathlib import Path
from typing import Optional, Tuple, List

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle


LOG = logging.getLogger("chip.plot")


# -----------------------------
# Config paths (follow your project layout)
# -----------------------------
DATA_DIR = Path("./data")
DERIVED_DIR = DATA_DIR / "_derived"
BS_DIRS = [
    DATA_DIR / "bs_report" / "parquet_twse",
    DATA_DIR / "bs_report" / "parquet_tpex",
]
OHLC_CACHE = DERIVED_DIR / "ohlc.parquet"
SCORED_TP_SL = DERIVED_DIR / "scored_with_tp_sl.parquet"

OUT_EVENTS_ALL = DERIVED_DIR / "events_all.csv"
OUT_EVENTS_TOP = DERIVED_DIR / "events_top20_profit.csv"
PLOT_DIR = DERIVED_DIR / "plots_top_profit"


# -----------------------------
# Logging
# -----------------------------
def init_logging(level: str = "INFO"):
    lvl = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=lvl,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[logging.StreamHandler()],
    )


def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)
    return p


# -----------------------------
# IO helpers
# -----------------------------
def find_bs_parquet(symbol: str) -> Optional[Path]:
    for d in BS_DIRS:
        p = d / f"{symbol}.parquet"
        if p.exists():
            return p
    return None


def load_ohlc_cache() -> pd.DataFrame:
    if not OHLC_CACHE.exists():
        raise FileNotFoundError(f"Missing OHLC cache: {OHLC_CACHE}")
    ohlc = pd.read_parquet(OHLC_CACHE)
    # Ensure types
    ohlc["date"] = pd.to_datetime(ohlc["date"])
    for c in ["open", "high", "low", "close", "volume"]:
        if c in ohlc.columns:
            ohlc[c] = pd.to_numeric(ohlc[c], errors="coerce")
    ohlc = ohlc.dropna(subset=["symbol", "date", "open", "high", "low", "close"])
    return ohlc.sort_values(["symbol", "date"])


def load_scored_tp_sl() -> pd.DataFrame:
    if not SCORED_TP_SL.exists():
        raise FileNotFoundError(f"Missing scored parquet: {SCORED_TP_SL}")
    df = pd.read_parquet(SCORED_TP_SL)
    df["date"] = pd.to_datetime(df["date"])
    return df


def load_bs_window(symbol: str, date_min: pd.Timestamp, date_max: pd.Timestamp) -> pd.DataFrame:
    """
    Load broker flow parquet and return broker net per day in [date_min, date_max].
    Columns: date, broker, buy, sell => net
    """
    p = find_bs_parquet(symbol)
    if p is None:
        LOG.warning(f"No bs parquet found for {symbol}")
        return pd.DataFrame(columns=["date", "broker", "net", "buy", "sell"])

    cols = ["日期", "券商", "買進股數", "賣出股數"]
    try:
        df = pd.read_parquet(p, columns=cols)
    except TypeError:
        df = pd.read_parquet(p)
        df = df[[c for c in cols if c in df.columns]].copy()

    df = df.rename(columns={"日期": "date", "券商": "broker", "買進股數": "buy", "賣出股數": "sell"})
    df["date"] = pd.to_datetime(df["date"])
    df["buy"] = pd.to_numeric(df["buy"], errors="coerce").fillna(0.0)
    df["sell"] = pd.to_numeric(df["sell"], errors="coerce").fillna(0.0)
    df["net"] = df["buy"] - df["sell"]

    df = df[(df["date"] >= date_min) & (df["date"] <= date_max)]
    if df.empty:
        return pd.DataFrame(columns=["date", "broker", "net", "buy", "sell"])

    # Sum per broker/day (same broker can appear multiple rows)
    g = df.groupby(["date", "broker"], as_index=False).agg(
        net=("net", "sum"),
        buy=("buy", "sum"),
        sell=("sell", "sum"),
    )
    return g.sort_values(["date", "broker"])


# -----------------------------
# Event export
# -----------------------------
def export_events_csv(scored: pd.DataFrame, horizon: int) -> pd.DataFrame:
    """
    Export all events to CSV. Returns the event DataFrame for further use.
    """
    ret_col = f"tp_sl_ret_{horizon}d"
    out_col = f"tp_sl_outcome_{horizon}d"
    exd_col = f"tp_sl_exit_date_{horizon}d"
    exp_col = f"tp_sl_exit_px_{horizon}d"

    need_cols = [
        "symbol", "date", "stage1_bucket",
        "posnet_total", "hhi_posnet",
        "s_stage1_W_pct",
        ret_col, out_col, exd_col, exp_col,
    ]
    have = [c for c in need_cols if c in scored.columns]

    ev = scored[scored["is_event"] == 1].copy()
    ev = ev[have].sort_values(["date", "symbol"])
    ev.to_csv(OUT_EVENTS_ALL, index=False, encoding="utf-8-sig")
    LOG.info(f"Wrote: {OUT_EVENTS_ALL} | events={len(ev):,}")
    return ev


def select_top_profit_events(events: pd.DataFrame, horizon: int, topn: int) -> pd.DataFrame:
    ret_col = f"tp_sl_ret_{horizon}d"
    if ret_col not in events.columns:
        raise ValueError(f"Missing {ret_col} in events. Did you run TP/SL backtest?")

    # Drop SKIP / NaN
    top = events.dropna(subset=[ret_col]).copy()
    # if you recorded SKIP as string in outcome col, exclude it
    out_col = f"tp_sl_outcome_{horizon}d"
    if out_col in top.columns:
        top = top[top[out_col] != "SKIP"]

    top = top.sort_values(ret_col, ascending=False).head(topn)
    top.to_csv(OUT_EVENTS_TOP, index=False, encoding="utf-8-sig")
    LOG.info(f"Wrote: {OUT_EVENTS_TOP} | topn={len(top):,}")
    return top


# -----------------------------
# Plotting helpers
# -----------------------------
def plot_candlestick(ax, ohlc_window: pd.DataFrame):
    """
    Draw candlestick using rectangles and vlines.
    Assumes ohlc_window has date(open/high/low/close), date is datetime64.
    """
    x = mdates.date2num(ohlc_window["date"].dt.to_pydatetime())
    o = ohlc_window["open"].values
    h = ohlc_window["high"].values
    l = ohlc_window["low"].values
    c = ohlc_window["close"].values

    width = 0.6  # in days

    for xi, oi, hi, li, ci in zip(x, o, h, l, c):
        if not np.isfinite([oi, hi, li, ci]).all():
            continue

        # wick
        ax.vlines(xi, li, hi, linewidth=1)

        # body
        bottom = min(oi, ci)
        height = abs(ci - oi)
        if height == 0:
            height = 1e-9
        color = "g" if ci >= oi else "r"
        rect = Rectangle((xi - width/2, bottom), width, height, facecolor=color, edgecolor=color, alpha=0.6)
        ax.add_patch(rect)

    ax.xaxis_date()
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    ax.tick_params(axis="x", rotation=45)
    ax.set_ylabel("Price")


def plot_volume(ax, ohlc_window: pd.DataFrame):
    x = mdates.date2num(ohlc_window["date"].dt.to_pydatetime())
    v = ohlc_window["volume"].fillna(0).values
    ax.bar(x, v, width=0.6)
    ax.xaxis_date()
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    ax.tick_params(axis="x", rotation=45)
    ax.set_ylabel("Volume")


def plot_broker_flow(ax, bs_g: pd.DataFrame, top_brokers: int = 5):
    """
    bs_g columns: date, broker, net
    Plot:
      - total net (bar, all brokers)
      - top N brokers net lines (by sum abs net in window)
    """
    if bs_g.empty:
        ax.text(0.5, 0.5, "No broker flow data", ha="center", va="center", transform=ax.transAxes)
        ax.set_ylabel("Net")
        return

    # Pivot broker net
    pivot = bs_g.pivot_table(index="date", columns="broker", values="net", aggfunc="sum").fillna(0.0)
    pivot = pivot.sort_index()

    # choose top brokers by abs sum
    abs_sum = pivot.abs().sum(axis=0).sort_values(ascending=False)
    top_cols = list(abs_sum.head(top_brokers).index)

    total_net = pivot.sum(axis=1)

    x = mdates.date2num(pivot.index.to_pydatetime())
    ax.bar(x, total_net.values, width=0.6, label="Total net")

    # plot top brokers as lines
    for b in top_cols:
        ax.plot(x, pivot[b].values, marker="o", linewidth=1, label=str(b))

    ax.axhline(0, linewidth=1)
    ax.xaxis_date()
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    ax.tick_params(axis="x", rotation=45)
    ax.set_ylabel("Broker net (shares)")
    ax.legend(loc="upper left", fontsize=8, ncol=2)


def plot_event_case(
    symbol: str,
    event_date: pd.Timestamp,
    ohlc_all: pd.DataFrame,
    horizon: int,
    event_ret: float,
    out_path: Path,
    window_days: int = 15,
):
    """
    Plot event around +/- window_days trading days:
      - Panel1: candlestick
      - Panel2: volume
      - Panel3: broker net flow (total + top brokers)
    """
    s = ohlc_all[ohlc_all["symbol"] == symbol].sort_values("date").reset_index(drop=True)
    if s.empty:
        LOG.warning(f"No OHLC for {symbol}")
        return

    # Find event index by trading-day alignment
    ix = s.index[s["date"] == event_date]
    if len(ix) == 0:
        LOG.warning(f"Event date not found in OHLC for {symbol}: {event_date.date()}")
        return
    i = int(ix[0])

    i0 = max(0, i - window_days)
    i1 = min(len(s) - 1, i + window_days)

    win = s.iloc[i0:i1 + 1].copy()
    date_min = win["date"].min()
    date_max = win["date"].max()

    # Broker flow window
    bs_g = load_bs_window(symbol, date_min=date_min, date_max=date_max)

    # Mark event & exit date (if available in events CSV later, you can extend)
    fig = plt.figure(figsize=(14, 10))
    gs = fig.add_gridspec(3, 1, height_ratios=[3, 1, 2], hspace=0.25)

    ax1 = fig.add_subplot(gs[0, 0])
    ax2 = fig.add_subplot(gs[1, 0], sharex=ax1)
    ax3 = fig.add_subplot(gs[2, 0], sharex=ax1)

    fig.suptitle(f"{symbol} | event={event_date.date()} | horizon={horizon}d | tp/sl_ret={event_ret:.4f}")

    # Panel1: candlestick
    plot_candlestick(ax1, win)

    # Vertical line at event day
    x_event = mdates.date2num(event_date.to_pydatetime())
    ax1.axvline(x_event, linestyle="--", linewidth=1, label="event")
    ax1.legend(loc="upper left", fontsize=8)

    # Panel2: volume
    if "volume" in win.columns:
        plot_volume(ax2, win)
        ax2.axvline(x_event, linestyle="--", linewidth=1)

    # Panel3: broker flow
    plot_broker_flow(ax3, bs_g, top_brokers=5)
    ax3.axvline(x_event, linestyle="--", linewidth=1)

    plt.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=150)
    plt.close(fig)


# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--horizon", type=int, default=10, help="rank by tp_sl_ret_{h}d")
    ap.add_argument("--topn", type=int, default=20)
    ap.add_argument("--window", type=int, default=15, help="trading days before/after event")
    ap.add_argument("--log-level", type=str, default="INFO")
    args = ap.parse_args()

    init_logging(args.log_level)
    ensure_dir(PLOT_DIR)

    LOG.info(f"Load scored: {SCORED_TP_SL}")
    scored = load_scored_tp_sl()

    LOG.info(f"Load OHLC: {OHLC_CACHE}")
    ohlc = load_ohlc_cache()

    # 1) export all events csv
    events = export_events_csv(scored, horizon=args.horizon)

    # 2) pick top profit events
    top = select_top_profit_events(events, horizon=args.horizon, topn=args.topn)

    # 3) plot top cases
    ret_col = f"tp_sl_ret_{args.horizon}d"
    for k, row in enumerate(top.itertuples(index=False), start=1):
        sym = row.symbol
        dt = pd.to_datetime(row.date)
        r = getattr(row, ret_col)
        out_png = PLOT_DIR / f"top{k:02d}_{sym}_{dt.date()}_h{args.horizon}_ret{r:.4f}.png"
        LOG.info(f"[PLOT] {k}/{len(top)} {sym} {dt.date()} ret={r:.4f} -> {out_png.name}")
        plot_event_case(
            symbol=sym,
            event_date=dt,
            ohlc_all=ohlc,
            horizon=args.horizon,
            event_ret=float(r),
            out_path=out_png,
            window_days=args.window,
        )

    LOG.info("DONE.")


if __name__ == "__main__":
    main()
