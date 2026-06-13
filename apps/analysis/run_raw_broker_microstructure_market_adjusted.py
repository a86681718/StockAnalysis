#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Market-adjusted checks for raw broker microstructure strategies."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import run_raw_broker_microstructure_research as research


PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = PROJECT_ROOT / "outputs" / "analysis" / "raw_broker_microstructure"
ROBUSTNESS_DIR = OUT_DIR / "robustness"
FAMILIES = [
    "diffuse_buyer_accumulation",
    "role_reversal",
    "sell_pressure_absorption",
]


def _metrics(ret: pd.Series) -> dict[str, Any]:
    clean = pd.to_numeric(ret, errors="coerce").dropna()
    if clean.empty:
        return {"trades": 0, "win_rate": np.nan, "avg": np.nan, "median": np.nan, "min": np.nan}
    return {
        "trades": int(len(clean)),
        "win_rate": float((clean > 0).mean()),
        "avg": float(clean.mean()),
        "median": float(clean.median()),
        "min": float(clean.min()),
    }


def _build_market_proxy(ohlc: pd.DataFrame) -> pd.DataFrame:
    work = ohlc.copy()
    work = work.sort_values(["symbol", "date"])
    work["prev_close"] = work.groupby("symbol")["close"].shift(1)
    work["open_to_close"] = work["close"] / work["open"] - 1.0
    work["close_to_close"] = work["close"] / work["prev_close"] - 1.0
    market = work.groupby("date", as_index=False).agg(
        market_open_to_close=("open_to_close", "mean"),
        market_close_to_close=("close_to_close", "mean"),
        benchmark_symbols=("symbol", "nunique"),
    )
    market["market_close_to_close"] = market["market_close_to_close"].fillna(0.0)
    market["market_open_to_close"] = market["market_open_to_close"].fillna(0.0)
    return market.sort_values("date").reset_index(drop=True)


def _market_window_return(market: pd.DataFrame, entry_date: pd.Timestamp, exit_date: pd.Timestamp) -> float:
    window = market[(market["date"] >= entry_date) & (market["date"] <= exit_date)].copy()
    if window.empty:
        return np.nan
    entry_mask = window["date"] == entry_date
    factors = []
    if entry_mask.any():
        factors.append(1.0 + float(window.loc[entry_mask, "market_open_to_close"].iloc[0]))
    rest = window.loc[~entry_mask, "market_close_to_close"]
    factors.extend((1.0 + rest).tolist())
    return float(np.prod(factors) - 1.0)


def _load_trades(family: str) -> pd.DataFrame:
    path = OUT_DIR / f"{family}_best_trades.csv"
    trades = pd.read_csv(path, dtype={"symbol": str})
    for col in ["signal_date", "entry_date", "exit_date"]:
        trades[col] = pd.to_datetime(trades[col], errors="coerce")
    trades["net_ret"] = pd.to_numeric(trades["net_ret"], errors="coerce")
    return trades.dropna(subset=["entry_date", "exit_date", "net_ret"]).copy()


def _adjust_family(family: str, market: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame]:
    trades = _load_trades(family)
    trades["market_ret"] = [
        _market_window_return(market, pd.Timestamp(row.entry_date), pd.Timestamp(row.exit_date))
        for row in trades.itertuples(index=False)
    ]
    trades["excess_net_ret"] = trades["net_ret"] - trades["market_ret"]
    raw = _metrics(trades["net_ret"])
    market_metrics = _metrics(trades["market_ret"])
    excess = _metrics(trades["excess_net_ret"])
    row = {
        "event_family": family,
        "raw_trades": raw["trades"],
        "raw_win_rate": raw["win_rate"],
        "raw_avg_net_ret": raw["avg"],
        "raw_median_net_ret": raw["median"],
        "avg_market_ret": market_metrics["avg"],
        "median_market_ret": market_metrics["median"],
        "excess_win_rate": excess["win_rate"],
        "excess_avg_net_ret": excess["avg"],
        "excess_median_net_ret": excess["median"],
        "excess_min_net_ret": excess["min"],
        "excess_target_pass": bool(excess["trades"] >= 20 and excess["win_rate"] > 0.60 and excess["avg"] > 0.10),
    }
    return row, trades


def _write_report(summary: pd.DataFrame) -> None:
    lines = [
        "# Raw Broker Microstructure Market-Adjusted Check",
        "",
        "Purpose: estimate whether optimized strategy returns remain after removing broad market trend beta.",
        "",
        "Benchmark proxy:",
        "",
        "- Built only from raw `data/ohlc/*.csv`.",
        "- Uses all 4-digit non-ETF stocks.",
        "- Daily benchmark return is equal-weighted across available stocks.",
        "- For each strategy trade, benchmark return is aligned to the same entry date and exit date.",
        "- Excess return = strategy net return - benchmark window return.",
        "",
        "Limitations:",
        "",
        "- This is an equal-weight stock-universe proxy, not official TAIEX.",
        "- It adjusts broad market drift, but not sector/factor exposure.",
        "",
        "## Result",
        "",
        "| family | excess_pass | trades | raw_avg | avg_market | excess_avg | excess_win | excess_median |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _, row in summary.iterrows():
        lines.append(
            f"| `{row['event_family']}` | `{bool(row['excess_target_pass'])}` | "
            f"{int(row['raw_trades'])} | {row['raw_avg_net_ret']:.4f} | "
            f"{row['avg_market_ret']:.4f} | {row['excess_avg_net_ret']:.4f} | "
            f"{row['excess_win_rate']:.4f} | {row['excess_median_net_ret']:.4f} |"
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- If excess return remains positive and passes the original win/average gates, the result is less likely to be explained only by the bull market.",
            "- If excess return collapses, the strategy is mostly market beta and should not be treated as an independent edge.",
        ]
    )
    (ROBUSTNESS_DIR / "raw_microstructure_market_adjusted_report.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="Market-adjusted check for raw broker microstructure strategies")
    ap.add_argument("--start", default="2025-04-14")
    ap.add_argument("--end", default="2026-06-12")
    args = ap.parse_args()

    ROBUSTNESS_DIR.mkdir(parents=True, exist_ok=True)
    ohlc = research.load_raw_ohlc(args.start, args.end, exclude_etf=True)
    market = _build_market_proxy(ohlc)
    market.to_csv(ROBUSTNESS_DIR / "market_equal_weight_proxy.csv", index=False)
    rows: list[dict[str, Any]] = []
    adjusted_parts: list[pd.DataFrame] = []
    for family in FAMILIES:
        row, trades = _adjust_family(family, market)
        rows.append(row)
        trades["event_family"] = family
        adjusted_parts.append(trades)
    summary = pd.DataFrame(rows)
    adjusted = pd.concat(adjusted_parts, ignore_index=True)
    summary.to_csv(ROBUSTNESS_DIR / "market_adjusted_summary.csv", index=False)
    adjusted.to_csv(ROBUSTNESS_DIR / "market_adjusted_trades.csv", index=False)
    output_summary = {
        "benchmark": "equal_weight_non_etf_raw_ohlc",
        "families": summary["event_family"].tolist(),
        "excess_pass_count": int(summary["excess_target_pass"].sum()),
        "excess_fail_count": int((~summary["excess_target_pass"]).sum()),
    }
    (ROBUSTNESS_DIR / "market_adjusted_summary.json").write_text(
        json.dumps(output_summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    _write_report(summary)
    print(ROBUSTNESS_DIR / "raw_microstructure_market_adjusted_report.md")


if __name__ == "__main__":
    main()
