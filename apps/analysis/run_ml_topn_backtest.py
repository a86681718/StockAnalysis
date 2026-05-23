#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Top-N backtest using model predictions.

Entry: next day's open (default) or same-day close
Exit: close at t + horizon (trading days)
Optional: skip if next_open gap exceeds threshold
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def build_ohlc_features(ohlc: pd.DataFrame, horizon: int) -> pd.DataFrame:
    ohlc = ohlc.sort_values(["symbol", "date"]).copy()
    g = ohlc.groupby("symbol", group_keys=False)
    ohlc["next_open"] = g["open"].shift(-1)
    ohlc["close_h"] = g["close"].shift(-horizon)
    ohlc["gap_1d"] = ohlc["next_open"] / ohlc["close"] - 1.0

    days_above = []
    for _, sub in ohlc.groupby("symbol"):
        c = sub["close"].to_numpy(float)
        n = len(c)
        out = np.full(n, np.nan)
        for i in range(n):
            j_end = i + horizon
            if j_end >= n:
                continue
            base = c[i]
            if not np.isfinite(base):
                continue
            out[i] = float(np.sum(c[i + 1 : j_end + 1] > base))
        days_above.append(pd.Series(out, index=sub.index))
    ohlc["days_above_h"] = pd.concat(days_above).sort_index()

    return ohlc[["symbol", "date", "open", "close", "next_open", "close_h", "gap_1d", "days_above_h"]]


def summarize_by_date(trades: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for d, sub in trades.groupby("date"):
        r = sub["ret"].dropna()
        n_total = int(len(sub))
        n_exec = int(r.shape[0])
        rows.append(
            {
                "date": d,
                "n_total": n_total,
                "n_exec": n_exec,
                "mean": float(r.mean()) if n_exec else np.nan,
                "median": float(r.median()) if n_exec else np.nan,
                "win_rate": float((r > 0).mean()) if n_exec else np.nan,
                "skip_rate": float(sub["skipped"].mean()) if n_total else np.nan,
            }
        )
    return pd.DataFrame(rows)


def main() -> None:
    ap = argparse.ArgumentParser(description="Top-N backtest using prediction ranks")
    ap.add_argument("--preds", type=str, required=True, help="predictions csv with symbol,date,pred")
    ap.add_argument("--ohlc", type=str, default="data/_derived/ohlc.parquet")
    ap.add_argument("--pred-col", type=str, default="pred")
    ap.add_argument("--topn", type=int, default=10)
    ap.add_argument("--top-pct", type=float, default=0.0, help="select top pct per date (e.g. 0.01=top 1%)")
    ap.add_argument("--horizon", type=int, default=10)
    ap.add_argument("--entry", type=str, default="next_open", choices=["next_open", "close"])
    ap.add_argument("--gap-th", type=float, default=None, help="skip if next_open/close - 1 > gap_th")
    ap.add_argument("--symbol-len", type=int, default=4, help="0=keep all; else filter by symbol length")
    ap.add_argument("--min-days-above", type=int, default=0, help="skip if days_above_h < this threshold")
    ap.add_argument("--out-trades", type=str, default="data/_derived/ml_runs/topn_trades.csv")
    ap.add_argument("--out-summary", type=str, default="data/_derived/ml_runs/topn_summary.csv")
    args = ap.parse_args()

    preds_path = Path(args.preds)
    if not preds_path.exists():
        raise FileNotFoundError(preds_path)

    ohlc_path = Path(args.ohlc)
    if not ohlc_path.exists():
        raise FileNotFoundError(ohlc_path)

    preds = pd.read_csv(preds_path)
    if preds.empty:
        raise RuntimeError("preds csv is empty")

    if args.pred_col not in preds.columns:
        raise ValueError(f"Missing pred column: {args.pred_col}")

    preds["date"] = pd.to_datetime(preds["date"])
    preds["symbol"] = preds["symbol"].astype(str)
    preds = preds.dropna(subset=["symbol", "date", args.pred_col])
    if args.symbol_len and args.symbol_len > 0:
        preds = preds[preds["symbol"].str.len() == args.symbol_len]

    preds = preds.sort_values(["date", args.pred_col], ascending=[True, False])
    if args.top_pct and args.top_pct > 0:
        def _head_pct(sub: pd.DataFrame) -> pd.DataFrame:
            n = max(1, int(len(sub) * args.top_pct))
            return sub.head(n)
        topn = preds.groupby("date", group_keys=False).apply(_head_pct).reset_index(drop=True)
    else:
        topn = preds.groupby("date", group_keys=False).head(args.topn).copy()

    ohlc = pd.read_parquet(ohlc_path, columns=["symbol", "date", "open", "close"])
    ohlc["date"] = pd.to_datetime(ohlc["date"])
    ohlc["symbol"] = ohlc["symbol"].astype(str)
    ohlc_feat = build_ohlc_features(ohlc, horizon=args.horizon)

    merged = topn.merge(ohlc_feat, on=["symbol", "date"], how="left")
    merged["skipped"] = False
    merged["skip_reason"] = None

    if args.entry == "close":
        merged["entry_px"] = merged["close"]
    else:
        merged["entry_px"] = merged["next_open"]

    if args.gap_th is not None:
        merged.loc[merged["gap_1d"] > args.gap_th, "skipped"] = True
        merged.loc[merged["gap_1d"] > args.gap_th, "skip_reason"] = "gap"

    if args.min_days_above and args.min_days_above > 0:
        mask_days = merged["days_above_h"] < args.min_days_above
        merged.loc[mask_days, "skipped"] = True
        merged.loc[mask_days, "skip_reason"] = "days_above"

    merged["ret"] = np.nan
    valid = (
        (~merged["skipped"])
        & merged["entry_px"].notna()
        & merged["close_h"].notna()
        & (merged["entry_px"] > 0)
        & (merged["close_h"] > 0)
    )
    merged.loc[valid, "ret"] = merged.loc[valid, "close_h"] / merged.loc[valid, "entry_px"] - 1.0

    out_trades = Path(args.out_trades)
    out_trades.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(out_trades, index=False)

    summ = summarize_by_date(merged)
    r_all = merged["ret"].dropna()
    overall = pd.DataFrame(
        [
            {
                "date": "ALL",
                "n_total": int(len(merged)),
                "n_exec": int(r_all.shape[0]),
                "mean": float(r_all.mean()) if len(r_all) else np.nan,
                "median": float(r_all.median()) if len(r_all) else np.nan,
                "win_rate": float((r_all > 0).mean()) if len(r_all) else np.nan,
                "skip_rate": float(merged["skipped"].mean()) if len(merged) else np.nan,
            }
        ]
    )
    summary = pd.concat([overall, summ], ignore_index=True)

    out_summary = Path(args.out_summary)
    out_summary.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(out_summary, index=False)

    print(f"Wrote: {out_trades}")
    print(f"Wrote: {out_summary}")


if __name__ == "__main__":
    main()
