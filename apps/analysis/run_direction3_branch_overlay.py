#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Overlay true branch-level broker features on the direction-3 breakout candidate.

This is a diagnostic bridge between:
- direction 1: specific broker branch abnormal buying, and
- direction 3: breakout / early surge continuation.

It does not search a new strategy. It checks whether the saved direction-3
candidate is directly covered by the strict standalone key-branch signal, and
then computes looser branch features only for the direction-3 candidate symbols
and dates.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from apps.analysis.run_key_broker_branch_scan import (  # noqa: E402
    _read_broker_parquet,
    build_ohlc_maps,
    is_branch_name,
    iter_stock_parquets,
    load_broker_lookup,
    load_ohlc,
    parse_csv_ints,
)
from src.stockanalysis.config import ensure_dir, resolve_data, resolve_output  # noqa: E402


ML_RUNS = PROJECT_ROOT / "data" / "_derived" / "ml_runs"
DEFAULT_CANDIDATES = ML_RUNS / "breakout10_predictions_wf_repair_branch_topratio0610_daybuy20ge4.csv"
DEFAULT_TRADES = ML_RUNS / "repair_branch_topratio0610_daybuy20ge4_trades.csv"
DEFAULT_STRICT_SIGNALS = PROJECT_ROOT / "outputs" / "analysis" / "key_broker_branch" / "best_key_broker_branch_signals.parquet"


def _safe_div(a: pd.Series, b: pd.Series) -> pd.Series:
    out = a / b.replace(0, np.nan)
    return out.replace([np.inf, -np.inf], np.nan)


def _target_trading_dates(trading_dates: pd.DatetimeIndex, signal_dates: set[pd.Timestamp], lookback_days: int) -> set[pd.Timestamp]:
    out: set[pd.Timestamp] = set()
    for date in signal_dates:
        start = date - pd.Timedelta(days=lookback_days)
        mask = (trading_dates >= start) & (trading_dates <= date)
        out.update(pd.Timestamp(d) for d in trading_dates[mask])
    return out


def _build_symbol_candidate_features(
    path: Path,
    trading_dates: pd.DatetimeIndex,
    broker_lookup: dict[str, str],
    signal_dates: set[pd.Timestamp],
    args: argparse.Namespace,
    window_days_list: list[int],
) -> pd.DataFrame:
    symbol = path.stem
    target_dates = _target_trading_dates(trading_dates, signal_dates, args.lookback_days)
    if not target_dates:
        return pd.DataFrame()

    raw = _read_broker_parquet(path, args.history_start_ts, args.signal_end_ts)
    if raw.empty:
        return pd.DataFrame()

    daily_all = (
        raw.groupby(["日期", "券商"], as_index=False)
        .agg(buy_shares=("買進股數", "sum"), sell_shares=("賣出股數", "sum"))
        .rename(columns={"日期": "date", "券商": "broker"})
    )
    daily_all["net_shares"] = daily_all["buy_shares"] - daily_all["sell_shares"]
    daily_all["positive_net"] = daily_all["net_shares"].clip(lower=0.0)

    stock_daily = (
        daily_all.groupby("date", as_index=False)
        .agg(stock_buy_shares=("buy_shares", "sum"), stock_positive_net=("positive_net", "sum"))
        .set_index("date")
        .reindex(trading_dates)
        .fillna(0.0)
        .rename_axis("date")
        .reset_index()
    )

    daily_all["broker_name"] = daily_all["broker"].map(broker_lookup).fillna("")
    daily_all["is_branch"] = daily_all["broker_name"].map(lambda x: is_branch_name(x, args.min_branch_suffix_len))
    daily = daily_all[daily_all["is_branch"]].copy()
    if daily.empty:
        return pd.DataFrame()

    brokers = sorted(daily["broker"].dropna().unique())
    full_idx = pd.MultiIndex.from_product([brokers, trading_dates], names=["broker", "date"])
    daily = daily.set_index(["broker", "date"])[["buy_shares", "sell_shares", "net_shares", "positive_net"]]
    daily = daily.reindex(full_idx).reset_index()
    for col in ("buy_shares", "sell_shares", "net_shares", "positive_net"):
        daily[col] = pd.to_numeric(daily[col], errors="coerce").fillna(0.0)
    daily["broker_name"] = daily["broker"].map(broker_lookup).fillna("")
    daily["buy_flag"] = (daily["buy_shares"] > 0).astype(float)
    daily["positive_flag"] = (daily["net_shares"] > 0).astype(float)
    daily = daily.sort_values(["broker", "date"]).reset_index(drop=True)

    rows: list[pd.DataFrame] = []
    grouped = daily.groupby("broker", group_keys=False)
    prior_trading_days = grouped.cumcount()
    prior_buy_days = grouped["buy_flag"].cumsum() - daily["buy_flag"]
    prior_positive_sum = grouped["positive_net"].cumsum() - daily["positive_net"]
    history_pos_mean_day = prior_positive_sum / prior_trading_days.replace(0, np.nan)

    for window_days in window_days_list:
        work = daily[["broker", "date", "broker_name"]].copy()
        work["window_days"] = window_days
        work["prior_trading_days"] = prior_trading_days
        work["prior_buy_days"] = prior_buy_days
        work["history_pos_mean_day"] = history_pos_mean_day
        work["expected_window_net"] = history_pos_mean_day * window_days
        work["window_net_sum"] = (
            grouped["positive_net"].rolling(window_days, min_periods=window_days).sum().reset_index(level=0, drop=True)
        )
        work["window_buy_sum"] = (
            grouped["buy_shares"].rolling(window_days, min_periods=window_days).sum().reset_index(level=0, drop=True)
        )
        work["window_positive_days"] = (
            grouped["positive_flag"].rolling(window_days, min_periods=window_days).sum().reset_index(level=0, drop=True)
        )
        work["window_net_buy_ratio"] = _safe_div(work["window_net_sum"], work["window_buy_sum"])
        work["window_net_ratio"] = _safe_div(work["window_net_sum"], work["expected_window_net"])

        stock = stock_daily.copy()
        stock["stock_buy_window"] = stock["stock_buy_shares"].rolling(window_days, min_periods=window_days).sum()
        stock["stock_positive_net_window"] = stock["stock_positive_net"].rolling(window_days, min_periods=window_days).sum()
        work = work.merge(stock[["date", "stock_buy_window", "stock_positive_net_window"]], on="date", how="left")
        work["branch_buy_share"] = _safe_div(work["window_buy_sum"], work["stock_buy_window"])
        work["branch_posnet_share"] = _safe_div(work["window_net_sum"], work["stock_positive_net_window"])
        work["score"] = (
            work["window_net_ratio"].replace([np.inf, -np.inf], np.nan).fillna(0.0)
            * work["branch_buy_share"].replace([np.inf, -np.inf], np.nan).fillna(0.0)
            * np.sqrt(work["window_net_sum"].clip(lower=0.0))
        )
        filtered = work[
            work["date"].isin(target_dates)
            & (work["prior_buy_days"] >= args.min_history_buy_days)
            & (work["window_net_sum"] >= args.min_window_net)
            & (work["window_positive_days"] >= args.min_positive_days)
            & (work["window_buy_sum"] > 0)
            & (work["score"] > 0)
        ].copy()
        if filtered.empty:
            continue
        filtered.insert(0, "symbol", symbol)
        rows.append(filtered)

    if not rows:
        return pd.DataFrame()
    keep = [
        "symbol",
        "date",
        "broker",
        "broker_name",
        "window_days",
        "prior_buy_days",
        "window_net_sum",
        "window_buy_sum",
        "window_positive_days",
        "window_net_buy_ratio",
        "expected_window_net",
        "window_net_ratio",
        "branch_buy_share",
        "branch_posnet_share",
        "score",
    ]
    return pd.concat(rows, ignore_index=True)[keep]


def _best_exact(features: pd.DataFrame) -> pd.DataFrame:
    if features.empty:
        return features
    return features.sort_values(["symbol", "date", "score"], ascending=[True, True, False]).drop_duplicates(
        ["symbol", "date"], keep="first"
    )


def _lookback_best(base: pd.DataFrame, features: pd.DataFrame, date_col: str, lookback_days: int, prefix: str) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    feature_groups = {symbol: sub.sort_values("score", ascending=False) for symbol, sub in features.groupby("symbol")}
    for _, row in base.iterrows():
        symbol = str(row["symbol"])
        date = pd.Timestamp(row[date_col])
        sub = feature_groups.get(symbol)
        if sub is None:
            rows.append({})
            continue
        hit = sub[(sub["date"] <= date) & (sub["date"] >= date - pd.Timedelta(days=lookback_days))]
        if hit.empty:
            rows.append({})
            continue
        best = hit.iloc[0].to_dict()
        rows.append({f"{prefix}{key}": value for key, value in best.items()})
    return pd.DataFrame(rows)


def _prefixed(frame: pd.DataFrame, prefix: str) -> pd.DataFrame:
    return frame.rename(columns={col: f"{prefix}{col}" for col in frame.columns})


def _fmt(value: float) -> str:
    if value is None or pd.isna(value):
        return "nan"
    if math.isinf(float(value)):
        return "inf"
    return f"{float(value):.4f}"


def _rate(mask: pd.Series) -> float:
    return float(mask.mean()) if len(mask) else np.nan


def _metrics_for_flag(df: pd.DataFrame, flag_col: str) -> dict[str, object]:
    sub = df[df[flag_col]].copy()
    return {
        "flag": flag_col,
        "rows": int(len(sub)),
        "coverage": float(len(sub) / len(df)) if len(df) else np.nan,
        "label_hit": float(sub["breakout_10d_ge10"].mean()) if len(sub) and "breakout_10d_ge10" in sub else np.nan,
        "avg_pred": float(sub["pred"].mean()) if len(sub) else np.nan,
    }


def _build_report(
    candidates: pd.DataFrame,
    trades: pd.DataFrame,
    day_summary: pd.DataFrame,
    threshold_summary: pd.DataFrame,
    args: argparse.Namespace,
) -> str:
    strict_trade_hits = int(trades["strict_exact_match"].sum())
    strict_candidate_hits = int(candidates["strict_exact_match"].sum())
    broad_trade_hits = int(trades["broad_exact_match"].sum())
    broad_candidate_hits = int(candidates["broad_exact_match"].sum())
    strong_trade_hits = int(trades["broad_strong_exact"].sum())
    strong_candidate_hits = int(candidates["broad_strong_exact"].sum())
    lookback_trade_hits = int(trades["broad_lookback_match"].sum())

    lines = [
        "# Direction 3 Branch Overlay Report",
        "",
        "## Scope",
        "",
        "- direction 3 candidate: `repair_branch_topratio0610_daybuy20ge4`",
        "- strict branch signal source: `outputs/analysis/key_broker_branch/best_key_broker_branch_signals.parquet`",
        f"- loose branch feature windows: `{args.window_days_list}`",
        f"- branch lookback window: `{args.lookback_days}` calendar days",
        "",
        "## Coverage",
        "",
        f"- strict standalone branch exact coverage on selected trades: `{strict_trade_hits} / {len(trades)}`",
        f"- strict standalone branch exact coverage on day candidates: `{strict_candidate_hits} / {len(candidates)}`",
        f"- loose branch exact coverage on selected trades: `{broad_trade_hits} / {len(trades)}`",
        f"- loose branch exact coverage on day candidates: `{broad_candidate_hits} / {len(candidates)}`",
        f"- loose strong-branch exact coverage on selected trades: `{strong_trade_hits} / {len(trades)}`",
        f"- loose strong-branch exact coverage on day candidates: `{strong_candidate_hits} / {len(candidates)}`",
        f"- loose branch lookback coverage on selected trades: `{lookback_trade_hits} / {len(trades)}`",
        "",
        "## Selected Trade Overlay",
        "",
    ]
    display_cols = [
        "symbol",
        "signal_date",
        "net_ret",
        "broad_broker_name",
        "broad_window_days",
        "broad_window_net_ratio",
        "broad_branch_buy_share",
        "broad_branch_posnet_share",
        "broad_score",
    ]
    for _, row in trades.sort_values("signal_date").iterrows():
        lines.append(
            f"- `{row['signal_date'].date()} {row['symbol']}`: "
            f"`net={row['net_ret']:.4f}`, "
            f"`broker={row.get('broad_broker_name', '')}`, "
            f"`window={int(row['broad_window_days']) if pd.notna(row.get('broad_window_days')) else 'nan'}`, "
            f"`net_ratio={_fmt(row.get('broad_window_net_ratio'))}`, "
            f"`buy_share={_fmt(row.get('broad_branch_buy_share'))}`, "
            f"`posnet_share={_fmt(row.get('broad_branch_posnet_share'))}`"
        )

    lines.extend(["", "## Candidate Label Diagnostics", ""])
    for _, row in threshold_summary.iterrows():
        lines.append(
            f"- `{row['flag']}`: `rows={int(row['rows'])}`, "
            f"`coverage={_fmt(row['coverage'])}`, `label_hit={_fmt(row['label_hit'])}`, "
            f"`avg_pred={_fmt(row['avg_pred'])}`"
        )

    lines.extend(["", "## Signal-Day Coverage", ""])
    for _, row in day_summary.iterrows():
        lines.append(
            f"- `{row['date'].date()}`: `candidates={int(row['candidates'])}`, "
            f"`strict={int(row['strict_exact_matches'])}`, `broad={int(row['broad_exact_matches'])}`, "
            f"`strong={int(row['broad_strong_exact_matches'])}`, `label_hit={_fmt(row['label_hit'])}`"
        )

    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- The strict standalone key-branch rule should not be attached directly to the direction-3 entry rule: exact coverage is effectively zero for selected trades.",
            "- Looser true branch features are present on every selected direction-3 trade, but most selected trades do not meet standalone-level branch-share thresholds.",
            "- This supports the current architecture: direction 1 contributes useful chip context, while the actual entry trigger remains the breakout/day-quality setup.",
            "- The next useful branch-level step is to derive lighter branch context features for the direction-3 model or day filter, not to gate entries on the standalone branch scanner.",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description="Overlay key-broker branch features on direction-3 candidates")
    ap.add_argument("--candidates", type=Path, default=DEFAULT_CANDIDATES)
    ap.add_argument("--trades", type=Path, default=DEFAULT_TRADES)
    ap.add_argument("--strict-signals", type=Path, default=DEFAULT_STRICT_SIGNALS)
    ap.add_argument("--broker-dirs", nargs="+", type=Path, default=[resolve_data("bs_report", "parquet_twse"), resolve_data("bs_report", "parquet_tpex")])
    ap.add_argument("--ohlc", type=Path, default=resolve_data("_derived", "ohlc.parquet"))
    ap.add_argument("--broker-list", type=Path, default=resolve_data("broker_list.csv"))
    ap.add_argument("--history-start", default="2025-07-01")
    ap.add_argument("--signal-start", default="2025-11-01")
    ap.add_argument("--signal-end", default="2026-02-03")
    ap.add_argument("--window-days-list", default="3,5,10")
    ap.add_argument("--lookback-days", type=int, default=10)
    ap.add_argument("--min-branch-suffix-len", type=int, default=2)
    ap.add_argument("--min-history-buy-days", type=int, default=3)
    ap.add_argument("--min-positive-days", type=float, default=1)
    ap.add_argument("--min-window-net", type=float, default=1.0)
    ap.add_argument("--strong-window-net-ratio", type=float, default=5.0)
    ap.add_argument("--strong-branch-buy-share", type=float, default=0.02)
    ap.add_argument("--strong-branch-posnet-share", type=float, default=0.02)
    ap.add_argument("--output-dir", type=Path, default=resolve_output("analysis", "direction3_branch_overlay"))
    args = ap.parse_args()

    args.history_start_ts = pd.Timestamp(args.history_start)
    args.signal_start_ts = pd.Timestamp(args.signal_start)
    args.signal_end_ts = pd.Timestamp(args.signal_end)
    window_days_list = parse_csv_ints(args.window_days_list)
    out_dir = ensure_dir(args.output_dir)

    candidates = pd.read_csv(args.candidates, low_memory=False)
    trades = pd.read_csv(args.trades, parse_dates=["signal_date", "entry_date", "exit_date"])
    strict = pd.read_parquet(args.strict_signals)
    for frame, date_col in [(candidates, "date"), (strict, "date")]:
        frame["symbol"] = frame["symbol"].astype(str)
        frame[date_col] = pd.to_datetime(frame[date_col])
    trades["symbol"] = trades["symbol"].astype(str)

    symbols = set(candidates["symbol"].astype(str).unique())
    max_exit = args.signal_end_ts + pd.Timedelta(days=45)
    ohlc = load_ohlc(args.ohlc, args.history_start_ts, max_exit)
    date_map, _ = build_ohlc_maps(ohlc)
    files = iter_stock_parquets(args.broker_dirs, symbols, exclude_etf=True)
    files = [path for path in files if path.stem in date_map]
    broker_lookup = load_broker_lookup(args.broker_list)

    signal_dates_by_symbol = {
        symbol: {pd.Timestamp(d) for d in sub["date"].unique()}
        for symbol, sub in candidates.groupby("symbol")
    }
    feature_parts: list[pd.DataFrame] = []
    feature_args = SimpleNamespace(**vars(args))
    for i, path in enumerate(files, start=1):
        feat = _build_symbol_candidate_features(
            path,
            date_map[path.stem],
            broker_lookup,
            signal_dates_by_symbol.get(path.stem, set()),
            feature_args,
            window_days_list,
        )
        if not feat.empty:
            feature_parts.append(feat)
        if i % 50 == 0:
            print(f"processed {i}/{len(files)} symbols, feature_parts={len(feature_parts)}", flush=True)

    loose_features = pd.concat(feature_parts, ignore_index=True) if feature_parts else pd.DataFrame()
    exact_loose = _best_exact(loose_features)
    strict_exact = _best_exact(strict)

    candidates = candidates.merge(
        _prefixed(exact_loose, "broad_"),
        left_on=["symbol", "date"],
        right_on=["broad_symbol", "broad_date"],
        how="left",
    )
    candidates = candidates.merge(
        _prefixed(strict_exact, "strict_"),
        left_on=["symbol", "date"],
        right_on=["strict_symbol", "strict_date"],
        how="left",
    )
    candidates = pd.concat(
        [candidates.reset_index(drop=True), _lookback_best(candidates, loose_features, "date", args.lookback_days, "broad_lb_")],
        axis=1,
    )
    candidates["broad_exact_match"] = candidates["broad_score"].notna()
    candidates["strict_exact_match"] = candidates["strict_score"].notna()
    candidates["broad_lookback_match"] = candidates["broad_lb_score"].notna()
    candidates["broad_strong_exact"] = (
        candidates["broad_exact_match"]
        & (candidates["broad_window_net_ratio"] >= args.strong_window_net_ratio)
        & (candidates["broad_branch_buy_share"] >= args.strong_branch_buy_share)
        & (candidates["broad_branch_posnet_share"] >= args.strong_branch_posnet_share)
    )
    candidates["strict_like_exact"] = (
        candidates["broad_exact_match"]
        & (candidates["broad_window_net_ratio"] >= 8.0)
        & (candidates["broad_branch_buy_share"] >= 0.12)
        & (candidates["broad_window_net_buy_ratio"] >= 0.60)
        & (candidates["broad_branch_posnet_share"] >= 0.20)
    )

    trades = trades.merge(
        candidates,
        left_on=["symbol", "signal_date"],
        right_on=["symbol", "date"],
        how="left",
        suffixes=("", "_candidate"),
    )

    day_summary = (
        candidates.groupby("date")
        .agg(
            candidates=("symbol", "size"),
            strict_exact_matches=("strict_exact_match", "sum"),
            broad_exact_matches=("broad_exact_match", "sum"),
            broad_strong_exact_matches=("broad_strong_exact", "sum"),
            broad_lookback_matches=("broad_lookback_match", "sum"),
            label_hit=("breakout_10d_ge10", "mean"),
            avg_pred=("pred", "mean"),
        )
        .reset_index()
    )
    threshold_summary = pd.DataFrame(
        [
            _metrics_for_flag(candidates, "broad_exact_match"),
            _metrics_for_flag(candidates, "broad_strong_exact"),
            _metrics_for_flag(candidates, "strict_exact_match"),
            _metrics_for_flag(candidates, "strict_like_exact"),
            _metrics_for_flag(candidates, "broad_lookback_match"),
        ]
    )

    candidates.to_csv(out_dir / "direction3_branch_overlay_candidates.csv", index=False)
    trades.to_csv(out_dir / "direction3_branch_overlay_trades.csv", index=False)
    day_summary.to_csv(out_dir / "direction3_branch_overlay_signal_days.csv", index=False)
    threshold_summary.to_csv(out_dir / "direction3_branch_overlay_threshold_summary.csv", index=False)
    report = _build_report(candidates, trades, day_summary, threshold_summary, args)
    (out_dir / "direction3_branch_overlay_report.md").write_text(report, encoding="utf-8")


if __name__ == "__main__":
    main()
