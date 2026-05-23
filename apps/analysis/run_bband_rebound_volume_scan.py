#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scan Bollinger-lower-band rebound rules with volume filters.

Outputs:
- rule scan metrics on train/valid/all
- top valid rules by 10d return / win-rate
- optional net-return check (next-open buy, +10d close sell, with costs)
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


def parse_float_list(s: str) -> list[float]:
    return [float(x) for x in s.split(",") if x.strip()]


def net_return_per_trade(
    entry_px: np.ndarray,
    exit_px: np.ndarray,
    notional: float,
    fee_rate: float,
    fee_discount: float,
    fee_min: float,
    tax_sell: float,
    slippage: float,
) -> np.ndarray:
    eff_fee = fee_rate * fee_discount
    entry_exec = entry_px * (1.0 + slippage)
    exit_exec = exit_px * (1.0 - slippage)
    shares = notional / entry_exec
    buy_value = shares * entry_exec
    buy_fee = np.maximum(buy_value * eff_fee, fee_min)
    sell_value = shares * exit_exec
    sell_fee = np.maximum(sell_value * eff_fee, fee_min)
    sell_tax = sell_value * tax_sell
    pnl = sell_value - sell_fee - sell_tax - buy_value - buy_fee
    return pnl / (buy_value + buy_fee)


def evaluate_rows(sub: pd.DataFrame) -> dict[str, float]:
    return {
        "n": int(sub.shape[0]),
        "ret5_mean": float(sub["fwd_ret_5d"].mean()),
        "ret5_win": float((sub["fwd_ret_5d"] > 0).mean()),
        "ret10_mean": float(sub["fwd_ret_10d"].mean()),
        "ret10_win": float((sub["fwd_ret_10d"] > 0).mean()),
        "mfe10_mean": float(sub["mfe_10d"].mean()),
        "mae10_mean": float(sub["mae_10d"].mean()),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Bollinger rebound + volume rule scan")
    ap.add_argument("--scored", type=str, default="data/_derived/scored.parquet")
    ap.add_argument("--ohlc", type=str, default="data/_derived/ohlc.parquet")
    ap.add_argument("--symbol-len", type=int, default=4)
    ap.add_argument("--valid-start", type=str, default="2025-12-01")
    ap.add_argument("--valid-end", type=str, default="2026-02-28")

    ap.add_argument("--vol-ratio-list", type=str, default="1.0,1.2,1.5,2.0,2.5,3.0")
    ap.add_argument("--vol-z-list", type=str, default="0.0,0.5,1.0,1.5,2.0")
    ap.add_argument("--drop-list", type=str, default="0.0,-0.01,-0.02,-0.03,-0.05")
    ap.add_argument("--min-valid-n", type=int, default=20)

    ap.add_argument("--trade-notional", type=float, default=100000.0)
    ap.add_argument("--fee-rate", type=float, default=0.001425)
    ap.add_argument("--fee-discount", type=float, default=0.28)
    ap.add_argument("--fee-min", type=float, default=20.0)
    ap.add_argument("--tax-rate-sell", type=float, default=0.003)
    ap.add_argument("--slippage", type=float, default=0.0005)
    ap.add_argument("--net-topk", type=int, default=5)

    ap.add_argument(
        "--out-scan",
        type=str,
        default="data/_derived/ml_runs/bband_rebound_volume_scan.csv",
    )
    ap.add_argument(
        "--out-best",
        type=str,
        default="data/_derived/ml_runs/bband_rebound_volume_best_valid.csv",
    )
    ap.add_argument(
        "--out-net",
        type=str,
        default="data/_derived/ml_runs/bband_rebound_volume_valid_net.csv",
    )
    args = ap.parse_args()

    vol_ratio_list = parse_float_list(args.vol_ratio_list)
    vol_z_list = parse_float_list(args.vol_z_list)
    drop_list = parse_float_list(args.drop_list)

    scored = pd.read_parquet(
        args.scored,
        columns=[
            "symbol",
            "date",
            "open",
            "close",
            "fwd_ret_5d",
            "fwd_ret_10d",
            "mfe_10d",
            "mae_10d",
        ],
    )
    ohlc = pd.read_parquet(args.ohlc, columns=["symbol", "date", "volume"])

    for df in (scored, ohlc):
        df["symbol"] = df["symbol"].astype(str)
        df["date"] = pd.to_datetime(df["date"])

    if args.symbol_len > 0:
        scored = scored[scored["symbol"].str.len() == args.symbol_len].copy()
        ohlc = ohlc[ohlc["symbol"].str.len() == args.symbol_len].copy()

    df = scored.merge(ohlc, on=["symbol", "date"], how="left")
    df = df.sort_values(["symbol", "date"]).copy()
    g = df.groupby("symbol", group_keys=False)

    ma20 = g["close"].rolling(20, min_periods=20).mean().reset_index(level=0, drop=True)
    std20 = g["close"].rolling(20, min_periods=20).std().reset_index(level=0, drop=True)
    bb_lower = ma20 - 2.0 * std20

    vol_ma20 = g["volume"].rolling(20, min_periods=20).mean().reset_index(level=0, drop=True)
    vol_std20 = g["volume"].rolling(20, min_periods=20).std().reset_index(level=0, drop=True)

    prev_close = g["close"].shift(1)
    prev_lower = bb_lower.groupby(df["symbol"]).shift(1)

    df["ret_1d"] = g["close"].pct_change(1)
    df["bb_break_down"] = df["close"] < bb_lower
    df["bb_cross_down"] = (df["close"] < bb_lower) & (prev_close >= prev_lower)
    df["vol_ratio20"] = df["volume"] / vol_ma20
    df["vol_z20"] = (df["volume"] - vol_ma20) / vol_std20

    df["next_open"] = g["open"].shift(-1)
    df["close_10d"] = g["close"].shift(-10)

    base = df[df["fwd_ret_10d"].notna()].copy()
    valid_mask = (base["date"] >= pd.to_datetime(args.valid_start)) & (
        base["date"] <= pd.to_datetime(args.valid_end)
    )

    rows: list[dict] = []
    trigger_cols = ["bb_break_down", "bb_cross_down"]
    split_masks = {
        "train": ~valid_mask,
        "valid": valid_mask,
        "all": pd.Series(True, index=base.index),
    }

    for trigger in trigger_cols:
        for vol_th in vol_ratio_list:
            for drop_th in drop_list:
                mask = (
                    base[trigger]
                    & (base["vol_ratio20"] >= vol_th)
                    & (base["ret_1d"] <= drop_th)
                )
                for split_name, split_mask in split_masks.items():
                    sub = base[mask & split_mask]
                    if sub.empty:
                        continue
                    m = evaluate_rows(sub)
                    rows.append(
                        {
                            "type": "ratio",
                            "trigger": trigger,
                            "vol_th": vol_th,
                            "drop_th": drop_th,
                            "split": split_name,
                            **m,
                        }
                    )

        for vol_th in vol_z_list:
            for drop_th in drop_list:
                mask = (
                    base[trigger]
                    & (base["vol_z20"] >= vol_th)
                    & (base["ret_1d"] <= drop_th)
                )
                for split_name, split_mask in split_masks.items():
                    sub = base[mask & split_mask]
                    if sub.empty:
                        continue
                    m = evaluate_rows(sub)
                    rows.append(
                        {
                            "type": "zscore",
                            "trigger": trigger,
                            "vol_th": vol_th,
                            "drop_th": drop_th,
                            "split": split_name,
                            **m,
                        }
                    )

    scan = pd.DataFrame(rows)
    out_scan = Path(args.out_scan)
    out_scan.parent.mkdir(parents=True, exist_ok=True)
    scan.to_csv(out_scan, index=False)

    valid_view = scan[(scan["split"] == "valid") & (scan["n"] >= args.min_valid_n)].copy()
    best_by_ret = valid_view.sort_values("ret10_mean", ascending=False).head(10)
    best_by_win = valid_view.sort_values("ret10_win", ascending=False).head(10)
    best = pd.concat(
        [best_by_ret.assign(rank_type="ret10_mean"), best_by_win.assign(rank_type="ret10_win")],
        ignore_index=True,
    ).drop_duplicates(subset=["type", "trigger", "vol_th", "drop_th", "rank_type"])
    out_best = Path(args.out_best)
    out_best.parent.mkdir(parents=True, exist_ok=True)
    best.to_csv(out_best, index=False)

    net_rows: list[dict] = []
    net_rules = [
        ("baseline_cross", "ratio", "bb_cross_down", 1.0, 0.0),
    ]
    for _, r in best_by_ret.head(args.net_topk).iterrows():
        key = f"top_ret_{r['type']}_{r['trigger']}_v{r['vol_th']}_d{r['drop_th']}"
        net_rules.append((key, r["type"], r["trigger"], float(r["vol_th"]), float(r["drop_th"])))

    for name, type_, trigger, vol_th, drop_th in net_rules:
        if type_ == "ratio":
            mask = (
                base[trigger]
                & (base["vol_ratio20"] >= vol_th)
                & (base["ret_1d"] <= drop_th)
                & valid_mask
            )
        else:
            mask = (
                base[trigger]
                & (base["vol_z20"] >= vol_th)
                & (base["ret_1d"] <= drop_th)
                & valid_mask
            )
        sub = base[mask].copy()
        sub = sub[
            sub["next_open"].notna()
            & sub["close_10d"].notna()
            & (sub["next_open"] > 0)
            & (sub["close_10d"] > 0)
        ]
        if sub.empty:
            continue
        r = net_return_per_trade(
            sub["next_open"].to_numpy(float),
            sub["close_10d"].to_numpy(float),
            notional=args.trade_notional,
            fee_rate=args.fee_rate,
            fee_discount=args.fee_discount,
            fee_min=args.fee_min,
            tax_sell=args.tax_rate_sell,
            slippage=args.slippage,
        )
        net_rows.append(
            {
                "rule": name,
                "type": type_,
                "trigger": trigger,
                "vol_th": vol_th,
                "drop_th": drop_th,
                "n_trades": int(len(r)),
                "net_mean_10d": float(np.mean(r)),
                "net_median_10d": float(np.median(r)),
                "net_win_rate": float(np.mean(r > 0)),
            }
        )

    net_df = pd.DataFrame(net_rows).sort_values("net_mean_10d", ascending=False)
    out_net = Path(args.out_net)
    out_net.parent.mkdir(parents=True, exist_ok=True)
    net_df.to_csv(out_net, index=False)

    summary = {
        "valid_range": [args.valid_start, args.valid_end],
        "scan_rows": int(scan.shape[0]),
        "valid_rows_min_n": int(valid_view.shape[0]),
        "best_ret_top1": best_by_ret.head(1).to_dict(orient="records"),
        "best_win_top1": best_by_win.head(1).to_dict(orient="records"),
        "net_top1": net_df.head(1).to_dict(orient="records"),
        "files": {
            "scan": str(out_scan),
            "best": str(out_best),
            "net": str(out_net),
        },
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
