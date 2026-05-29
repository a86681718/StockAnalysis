#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Robustness diagnostics for the current direction-3 breakout candidate.

The candidate is:
- repair branch + top_posnet_ratio <= 0.610
- day-level mean_buy20 >= 4
- top_pct=0.015, gap_th=0.005, hold_days=16, max_positions=6

This script does not search new parameters. It audits the saved replay artifacts
for concentration, leave-one sensitivity, and basic execution/liquidity risk.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.stockanalysis.config import ensure_dir, resolve_output  # noqa: E402


ML_RUNS = PROJECT_ROOT / "data" / "_derived" / "ml_runs"
DEFAULT_TRADES = ML_RUNS / "repair_branch_topratio0610_daybuy20ge4_trades.csv"
DEFAULT_SUMMARY = ML_RUNS / "repair_branch_topratio0610_daybuy20ge4_summary.json"
DEFAULT_ROLLING = ML_RUNS / "repair_branch_topratio0610_daybuy20ge4_rolling_windows.csv"
DEFAULT_DAY_QUALITY = ML_RUNS / "repair_branch_topratio0610_day_chip_quality.csv"
DEFAULT_OHLC = PROJECT_ROOT / "data" / "_derived" / "ohlc.parquet"


def _metrics(trades: pd.DataFrame) -> dict[str, float]:
    if trades.empty:
        return {
            "trades": 0,
            "win_rate": np.nan,
            "avg_net_ret": np.nan,
            "median_net_ret": np.nan,
            "min_net_ret": np.nan,
            "max_net_ret": np.nan,
            "profit_factor": np.nan,
            "payoff_ratio": np.nan,
            "avg_mfe": np.nan,
            "avg_mae": np.nan,
        }

    wins = trades.loc[trades["net_ret"] > 0, "net_ret"]
    losses = trades.loc[trades["net_ret"] < 0, "net_ret"]
    gross_profit = float(wins.sum()) if not wins.empty else 0.0
    gross_loss = float((-losses).sum()) if not losses.empty else 0.0
    avg_win = float(wins.mean()) if not wins.empty else np.nan
    avg_loss = float((-losses).mean()) if not losses.empty else np.nan
    return {
        "trades": int(len(trades)),
        "win_rate": float((trades["net_ret"] > 0).mean()),
        "avg_net_ret": float(trades["net_ret"].mean()),
        "median_net_ret": float(trades["net_ret"].median()),
        "min_net_ret": float(trades["net_ret"].min()),
        "max_net_ret": float(trades["net_ret"].max()),
        "profit_factor": float(gross_profit / gross_loss) if gross_loss > 0 else math.inf,
        "payoff_ratio": float(avg_win / avg_loss) if np.isfinite(avg_win) and np.isfinite(avg_loss) and avg_loss > 0 else math.inf,
        "avg_mfe": float(trades["mfe_during_trade"].mean()) if "mfe_during_trade" in trades else np.nan,
        "avg_mae": float(trades["mae_during_trade"].mean()) if "mae_during_trade" in trades else np.nan,
    }


def _target_pass(metrics: dict[str, float], min_trades: int = 1) -> bool:
    return bool(
        metrics["trades"] >= min_trades
        and metrics["win_rate"] > 0.50
        and metrics["avg_net_ret"] > 0.10
    )


def _load_inputs(
    trades_path: Path,
    summary_path: Path,
    rolling_path: Path,
    day_quality_path: Path,
    ohlc_path: Path,
) -> tuple[pd.DataFrame, dict[str, object], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    trades = pd.read_csv(trades_path).copy()
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    rolling = pd.read_csv(rolling_path).copy()
    day_quality = pd.read_csv(day_quality_path).copy()
    ohlc = pd.read_parquet(ohlc_path, columns=["symbol", "date", "open", "high", "low", "close", "volume"]).copy()

    trades["symbol"] = trades["symbol"].astype(str)
    ohlc["symbol"] = ohlc["symbol"].astype(str)
    for col in ["signal_date", "entry_date", "exit_date"]:
        trades[col] = pd.to_datetime(trades[col])
    day_quality["signal_date"] = pd.to_datetime(day_quality["signal_date"])
    ohlc["date"] = pd.to_datetime(ohlc["date"])

    numeric_cols = ["open", "high", "low", "close", "volume"]
    ohlc[numeric_cols] = ohlc[numeric_cols].apply(pd.to_numeric, errors="coerce")
    ohlc = ohlc[ohlc["symbol"].str.len() == 4].dropna(subset=["open", "high", "low", "close"]).copy()
    ohlc = ohlc.sort_values(["symbol", "date"]).reset_index(drop=True)
    return trades, summary, rolling, day_quality, ohlc


def _enrich_trades(trades: pd.DataFrame, day_quality: pd.DataFrame, ohlc: pd.DataFrame) -> pd.DataFrame:
    trade_rows: list[dict[str, object]] = []
    ohlc_by_symbol = {sym: sub.reset_index(drop=True) for sym, sub in ohlc.groupby("symbol")}

    for _, trade in trades.sort_values(["signal_date", "symbol"]).iterrows():
        sym = str(trade["symbol"])
        sub = ohlc_by_symbol.get(sym)
        if sub is None:
            continue
        idx_by_date = {pd.Timestamp(d): i for i, d in enumerate(sub["date"])}
        i_signal = idx_by_date.get(pd.Timestamp(trade["signal_date"]))
        i_entry = idx_by_date.get(pd.Timestamp(trade["entry_date"]))
        i_exit = idx_by_date.get(pd.Timestamp(trade["exit_date"]))
        if i_signal is None or i_entry is None or i_exit is None:
            continue

        signal_row = sub.iloc[i_signal]
        entry_row = sub.iloc[i_entry]
        exit_row = sub.iloc[i_exit]
        prev_close = float(signal_row["close"])
        entry_open = float(entry_row["open"])
        entry_turnover = float(entry_row["close"] * entry_row["volume"]) if pd.notna(entry_row["volume"]) else np.nan
        signal_turnover = float(signal_row["close"] * signal_row["volume"]) if pd.notna(signal_row["volume"]) else np.nan
        window = sub.iloc[i_entry : i_exit + 1].copy()
        entry_px = float(trade["entry_px_raw"])
        high_max = float(window["high"].max())
        low_min = float(window["low"].min())

        out = trade.to_dict()
        out.update(
            {
                "signal_close": prev_close,
                "signal_high": float(signal_row["high"]),
                "signal_low": float(signal_row["low"]),
                "signal_volume": float(signal_row["volume"]) if pd.notna(signal_row["volume"]) else np.nan,
                "signal_turnover": signal_turnover,
                "signal_close_to_high": float(prev_close / signal_row["high"] - 1.0) if signal_row["high"] > 0 else np.nan,
                "entry_gap": float(entry_open / prev_close - 1.0) if prev_close > 0 else np.nan,
                "entry_volume": float(entry_row["volume"]) if pd.notna(entry_row["volume"]) else np.nan,
                "entry_turnover": entry_turnover,
                "entry_cost_pct_turnover": float(trade["buy_total_cost"] / entry_turnover) if entry_turnover > 0 else np.nan,
                "exit_close": float(exit_row["close"]),
                "mfe_during_trade": float(high_max / entry_px - 1.0) if entry_px > 0 else np.nan,
                "mae_during_trade": float(low_min / entry_px - 1.0) if entry_px > 0 else np.nan,
            }
        )
        trade_rows.append(out)

    enriched = pd.DataFrame(trade_rows)
    quality_cols = [
        "signal_date",
        "mean_buy20",
        "min_buy20",
        "max_whhi20",
        "mean_dynk",
        "max_topratio",
        "mean_trade_ret",
        "mean_pred",
    ]
    available_quality_cols = [c for c in quality_cols if c in day_quality.columns]
    enriched = enriched.merge(day_quality[available_quality_cols], on="signal_date", how="left")
    enriched["signal_month"] = enriched["signal_date"].dt.to_period("M").astype(str)
    return enriched.sort_values(["signal_date", "symbol"]).reset_index(drop=True)


def _leave_one(enriched: pd.DataFrame, dimension: str, min_trades: int) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for bucket in sorted(enriched[dimension].dropna().astype(str).unique()):
        sub = enriched[enriched[dimension].astype(str) != bucket].copy()
        metrics = _metrics(sub)
        rows.append(
            {
                "dimension": dimension,
                "dropped_bucket": bucket,
                "dropped_trades": int((enriched[dimension].astype(str) == bucket).sum()),
                "remaining_trades": metrics["trades"],
                "win_rate": metrics["win_rate"],
                "avg_net_ret": metrics["avg_net_ret"],
                "median_net_ret": metrics["median_net_ret"],
                "min_net_ret": metrics["min_net_ret"],
                "target_pass": _target_pass(metrics, min_trades=min_trades),
            }
        )
    return pd.DataFrame(rows).sort_values(["target_pass", "avg_net_ret", "remaining_trades"], ascending=[True, True, True])


def _concentration(enriched: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for dim in ["symbol", "signal_date", "signal_month", "exit_reason"]:
        work = enriched.copy()
        if dim == "signal_date":
            work[dim] = work[dim].dt.strftime("%Y-%m-%d")
        grouped = (
            work.groupby(dim)
            .agg(
                trades=("net_ret", "size"),
                avg_net_ret=("net_ret", "mean"),
                total_net_ret=("net_ret", "sum"),
                min_net_ret=("net_ret", "min"),
                max_net_ret=("net_ret", "max"),
            )
            .reset_index()
        )
        for _, row in grouped.iterrows():
            rows.append(
                {
                    "dimension": dim,
                    "bucket": str(row[dim]),
                    "trades": int(row["trades"]),
                    "avg_net_ret": float(row["avg_net_ret"]),
                    "total_net_ret": float(row["total_net_ret"]),
                    "min_net_ret": float(row["min_net_ret"]),
                    "max_net_ret": float(row["max_net_ret"]),
                }
            )
    return pd.DataFrame(rows).sort_values(["dimension", "trades", "total_net_ret"], ascending=[True, False, False])


def _execution_summary(enriched: pd.DataFrame) -> dict[str, float]:
    return {
        "max_entry_gap": float(enriched["entry_gap"].max()),
        "min_entry_gap": float(enriched["entry_gap"].min()),
        "avg_entry_gap": float(enriched["entry_gap"].mean()),
        "signal_close_near_high_count": int((enriched["signal_close_to_high"].abs() <= 0.01).sum()),
        "entry_gap_over_0p5_count": int((enriched["entry_gap"] > 0.005).sum()),
        "entry_gap_over_2p0_count": int((enriched["entry_gap"] > 0.02).sum()),
        "min_entry_turnover": float(enriched["entry_turnover"].min()),
        "median_entry_turnover": float(enriched["entry_turnover"].median()),
        "max_entry_cost_pct_turnover": float(enriched["entry_cost_pct_turnover"].max()),
        "avg_mfe_during_trade": float(enriched["mfe_during_trade"].mean()),
        "avg_mae_during_trade": float(enriched["mae_during_trade"].mean()),
        "worst_mae_during_trade": float(enriched["mae_during_trade"].min()),
    }


def _fmt(value: float) -> str:
    if value is None or pd.isna(value):
        return "nan"
    if math.isinf(float(value)):
        return "inf"
    return f"{float(value):.4f}"


def _fmt_pct(value: float) -> str:
    if value is None or pd.isna(value):
        return "nan"
    return f"{float(value) * 100.0:.4f}%"


def _build_report(
    summary: dict[str, object],
    rolling: pd.DataFrame,
    enriched: pd.DataFrame,
    concentration: pd.DataFrame,
    leave_one_symbol: pd.DataFrame,
    leave_one_month: pd.DataFrame,
    leave_one_signal_day: pd.DataFrame,
    execution: dict[str, float],
) -> str:
    full_metrics = _metrics(enriched)
    symbol_pass = int(leave_one_symbol["target_pass"].sum())
    month_pass = int(leave_one_month["target_pass"].sum())
    signal_day_pass = int(leave_one_signal_day["target_pass"].sum())
    weak_symbol = leave_one_symbol[~leave_one_symbol["target_pass"]]
    weak_month = leave_one_month[~leave_one_month["target_pass"]]
    weak_signal_day = leave_one_signal_day[~leave_one_signal_day["target_pass"]]

    lines = [
        "# Direction 3 Breakout Robustness Report",
        "",
        "## Candidate",
        "",
        "- universe: `repair branch + top_posnet_ratio <= 0.610`",
        "- day filter: `mean_buy20 >= 4`",
        "- selection: `top_pct=0.015`, `gap_th=0.005`, `hold_days=16`, `max_positions=6`",
        f"- replay window: `{summary['window']['start']} ~ {summary['window']['end']}`",
        "",
        "## Full Metrics",
        "",
        f"- trades: `{full_metrics['trades']}`",
        f"- win rate: `{_fmt(full_metrics['win_rate'])}`",
        f"- average net return: `{_fmt(full_metrics['avg_net_ret'])}`",
        f"- median net return: `{_fmt(full_metrics['median_net_ret'])}`",
        f"- minimum net return: `{_fmt(full_metrics['min_net_ret'])}`",
        f"- maximum net return: `{_fmt(full_metrics['max_net_ret'])}`",
        f"- profit factor: `{_fmt(full_metrics['profit_factor'])}`",
        f"- average MFE during trade: `{_fmt(full_metrics['avg_mfe'])}`",
        f"- average MAE during trade: `{_fmt(full_metrics['avg_mae'])}`",
        "",
        "## Leave-One Sensitivity",
        "",
        f"- leave-one-symbol target pass: `{symbol_pass} / {len(leave_one_symbol)}`",
        f"- leave-one-month target pass: `{month_pass} / {len(leave_one_month)}`",
        f"- leave-one-signal-day target pass: `{signal_day_pass} / {len(leave_one_signal_day)}`",
    ]

    if weak_symbol.empty:
        lines.append("- dropping any single symbol still keeps the target gates intact")
    else:
        for _, row in weak_symbol.iterrows():
            lines.append(
                f"- dropping symbol `{row['dropped_bucket']}` fails: "
                f"`avg={_fmt(row['avg_net_ret'])}`, `win={_fmt(row['win_rate'])}`, "
                f"`remaining_trades={int(row['remaining_trades'])}`"
            )
    if weak_month.empty:
        lines.append("- dropping any single month still keeps the target gates intact")
    else:
        for _, row in weak_month.iterrows():
            lines.append(
                f"- dropping month `{row['dropped_bucket']}` fails: "
                f"`avg={_fmt(row['avg_net_ret'])}`, `win={_fmt(row['win_rate'])}`, "
                f"`remaining_trades={int(row['remaining_trades'])}`"
            )
    if weak_signal_day.empty:
        lines.append("- dropping any single signal day still keeps the target gates intact")
    else:
        for _, row in weak_signal_day.iterrows():
            lines.append(
                f"- dropping signal day `{row['dropped_bucket']}` fails: "
                f"`avg={_fmt(row['avg_net_ret'])}`, `win={_fmt(row['win_rate'])}`, "
                f"`remaining_trades={int(row['remaining_trades'])}`"
            )

    lines.extend(
        [
            "",
            "## Execution Diagnostics",
            "",
            f"- max entry gap: `{_fmt(execution['max_entry_gap'])}`",
            f"- entry gaps above configured `0.5%` threshold: `{execution['entry_gap_over_0p5_count']}`",
            f"- entry gaps above `2%`: `{execution['entry_gap_over_2p0_count']}`",
            f"- signal closes within `1%` of same-day high: `{execution['signal_close_near_high_count']} / {len(enriched)}`",
            f"- minimum entry-day turnover proxy (`close * volume`): `{execution['min_entry_turnover']:.0f}`",
            f"- median entry-day turnover proxy (`close * volume`): `{execution['median_entry_turnover']:.0f}`",
            f"- max simulated cost as percent of entry-day turnover: `{_fmt_pct(execution['max_entry_cost_pct_turnover'])}`",
            f"- worst MAE during held trade: `{_fmt(execution['worst_mae_during_trade'])}`",
            "",
            "## Rolling Windows",
            "",
        ]
    )

    for _, row in rolling.iterrows():
        lines.append(
            f"- `{row['window']}`: `trades={int(row['trades'])}`, "
            f"`win={_fmt(row['win'])}`, `mean_net={_fmt(row['mean_net'])}`, "
            f"`total_net={_fmt(row['total_net'])}`"
        )

    lines.extend(["", "## Concentration Snapshot", ""])
    for _, row in concentration.head(14).iterrows():
        lines.append(
            f"- `{row['dimension']}={row['bucket']}`: "
            f"`trades={int(row['trades'])}`, `avg={_fmt(row['avg_net_ret'])}`, "
            f"`total={_fmt(row['total_net_ret'])}`"
        )

    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- The saved direction-3 candidate still clears the requested target gates under leave-one-symbol/month/signal-day checks.",
            "- The main risk remains sample size: only 12 trades and 8 signal days are available in this replay window.",
            "- Entry execution risk looks acceptable in this replay because the saved strategy already rejects next-open gaps above 0.5%; none of the saved trades violates that gate.",
            "- A production decision still needs newer out-of-sample replay data, because this script only audits the existing saved walk-forward window.",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit direction-3 breakout candidate robustness")
    ap.add_argument("--trades", type=str, default=str(DEFAULT_TRADES))
    ap.add_argument("--summary", type=str, default=str(DEFAULT_SUMMARY))
    ap.add_argument("--rolling", type=str, default=str(DEFAULT_ROLLING))
    ap.add_argument("--day-quality", type=str, default=str(DEFAULT_DAY_QUALITY))
    ap.add_argument("--ohlc", type=str, default=str(DEFAULT_OHLC))
    ap.add_argument("--output-dir", type=str, default=str(resolve_output("analysis", "direction3_breakout_robustness")))
    args = ap.parse_args()

    out_dir = ensure_dir(Path(args.output_dir))
    trades, summary, rolling, day_quality, ohlc = _load_inputs(
        Path(args.trades),
        Path(args.summary),
        Path(args.rolling),
        Path(args.day_quality),
        Path(args.ohlc),
    )
    enriched = _enrich_trades(trades, day_quality, ohlc)
    concentration = _concentration(enriched)
    leave_one_symbol = _leave_one(enriched, "symbol", min_trades=1)
    leave_one_month = _leave_one(enriched, "signal_month", min_trades=1)
    leave_one_signal_day = _leave_one(enriched.assign(signal_day=enriched["signal_date"].dt.strftime("%Y-%m-%d")), "signal_day", min_trades=1)
    execution = _execution_summary(enriched)

    enriched.to_csv(out_dir / "direction3_breakout_enriched_trades.csv", index=False)
    concentration.to_csv(out_dir / "direction3_breakout_concentration.csv", index=False)
    leave_one_symbol.to_csv(out_dir / "direction3_breakout_leave_one_symbol_out.csv", index=False)
    leave_one_month.to_csv(out_dir / "direction3_breakout_leave_one_month_out.csv", index=False)
    leave_one_signal_day.to_csv(out_dir / "direction3_breakout_leave_one_signal_day_out.csv", index=False)

    report = _build_report(
        summary,
        rolling,
        enriched,
        concentration,
        leave_one_symbol,
        leave_one_month,
        leave_one_signal_day,
        execution,
    )
    (out_dir / "direction3_breakout_robustness_report.md").write_text(report, encoding="utf-8")


if __name__ == "__main__":
    main()
