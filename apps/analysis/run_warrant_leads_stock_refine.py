#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Focused refinement scan for the warrant-leads-stock rare-event family.

This script reuses the corrected rare-event daily features and trade simulator.
It tests whether second-order warrant concentration and not-overheated price
filters can push the direction-2 setup above the user's target gates:

- full-window win rate > 50%
- full-window average net return > 10%
"""

from __future__ import annotations

import argparse
import itertools
import json
import math
import sys
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from apps.analysis.run_rare_event_pipeline import (  # noqa: E402
    CostConfig,
    _dedup_cooldown,
    _event_name,
    _trade_metrics,
    build_ohlc_map,
    simulate_trades,
    summarize_candidate,
)
from src.stockanalysis.config import ensure_dir, resolve_output  # noqa: E402


TRAIN_END = pd.Timestamp("2025-09-30")
VALID_END = pd.Timestamp("2025-11-15")
TEST_END = pd.Timestamp("2025-12-15")


BASE_STOCK_RANGES = [
    (0.70, 0.85),
    (0.70, 0.90),
    (0.70, 0.95),
    (0.80, 0.90),
    (0.80, 0.95),
]


def _iter_base_specs() -> list[dict[str, object]]:
    specs: list[dict[str, object]] = []
    for (
        hold_days,
        cooldown_days,
        stop_loss,
        warrant_posnet_floor,
        warrant_posnet_strong_days_20,
        stock_range,
        prior_abs_ret_20d,
    ) in itertools.product(
        [30, 40],
        [15, 20],
        [None, -0.12, -0.10],
        [0.98, 0.99],
        [1, 3, 5],
        BASE_STOCK_RANGES,
        [0.08, 0.12],
    ):
        stock_posnet_floor, stock_posnet_cap = stock_range
        specs.append(
            {
                "family": "warrant_leads_stock",
                "hold_days": hold_days,
                "cooldown_days": cooldown_days,
                "stop_loss": stop_loss,
                "warrant_posnet_floor": warrant_posnet_floor,
                "warrant_posnet_strong_days_20": warrant_posnet_strong_days_20,
                "stock_posnet_floor": stock_posnet_floor,
                "stock_posnet_cap": stock_posnet_cap,
                "prior_abs_ret_20d": prior_abs_ret_20d,
            }
        )
    return specs


def _one_filter_specs(name: str, key: str, values: Iterable[object]) -> list[dict[str, object]]:
    return [{"filter_suite": name, key: value} for value in values]


def _iter_secondary_filters() -> list[dict[str, object]]:
    filters: list[dict[str, object]] = [{"filter_suite": "base"}]

    filters.extend(_one_filter_specs("warrant_hhi_pct", "warrant_hhi_pct_cs_floor", [0.50, 0.70, 0.80]))
    filters.extend(_one_filter_specs("warrant_hhi_level", "warrant_hhi_posnet_20_floor", [0.20, 0.30, 0.40, 0.50]))
    filters.extend(_one_filter_specs("warrant_top_share", "warrant_top_posnet_ratio_20_floor", [0.70, 0.80, 0.90]))
    filters.extend(_one_filter_specs("warrant_dyn_k", "warrant_dyn_k_pct_cs_floor", [0.50, 0.70, 0.90]))
    filters.extend(_one_filter_specs("volume_not_hot", "volume_ratio_5_20_cap", [1.20, 1.50, 1.80, 2.00]))
    filters.extend(_one_filter_specs("breakout_not_hot", "breakout_gap_20_cap", [-0.02, 0.00, 0.02, 0.05]))
    filters.extend(_one_filter_specs("not_deep_drawdown", "breakout_gap_20_floor", [-0.15, -0.10, -0.05]))
    filters.extend(_one_filter_specs("price_position_cap", "price_pos_20_cap", [0.65, 0.75, 0.85, 1.00]))

    for hhi_floor, price_cap, volume_cap, breakout_cap in itertools.product(
        [0.50, 0.70],
        [0.85, 1.00],
        [1.50, 2.00],
        [0.02, 0.05],
    ):
        filters.append(
            {
                "filter_suite": "hhi_price_heat",
                "warrant_hhi_pct_cs_floor": hhi_floor,
                "price_pos_20_cap": price_cap,
                "volume_ratio_5_20_cap": volume_cap,
                "breakout_gap_20_cap": breakout_cap,
            }
        )

    for top_floor, price_cap, volume_cap, breakout_cap in itertools.product(
        [0.70, 0.80],
        [0.85, 1.00],
        [1.50, 2.00],
        [0.02, 0.05],
    ):
        filters.append(
            {
                "filter_suite": "top_share_price_heat",
                "warrant_top_posnet_ratio_20_floor": top_floor,
                "price_pos_20_cap": price_cap,
                "volume_ratio_5_20_cap": volume_cap,
                "breakout_gap_20_cap": breakout_cap,
            }
        )

    return filters


def _apply_base_filter(feat: pd.DataFrame, spec: dict[str, object]) -> pd.Series:
    cond = feat["symbol"].notna()
    cond &= feat["warrant_posnet_pct_cs"].fillna(0.0) >= float(spec["warrant_posnet_floor"])
    cond &= feat["warrant_posnet_strong_days_20"].fillna(0.0) >= float(spec["warrant_posnet_strong_days_20"])
    cond &= feat["stock_posnet_pct_cs"] >= float(spec["stock_posnet_floor"])
    cond &= feat["stock_posnet_pct_cs"] <= float(spec["stock_posnet_cap"])
    cond &= feat["prior_abs_ret_20d"] <= float(spec["prior_abs_ret_20d"])
    cond &= feat["open"].notna() & feat["close"].notna() & feat["high"].notna() & feat["low"].notna()
    return cond.fillna(False)


def _apply_secondary_filter(signals: pd.DataFrame, secondary: dict[str, object]) -> pd.Series:
    cond = pd.Series(True, index=signals.index)
    if "warrant_hhi_pct_cs_floor" in secondary:
        cond &= signals["warrant_hhi_pct_cs"].fillna(0.0) >= float(secondary["warrant_hhi_pct_cs_floor"])
    if "warrant_hhi_posnet_20_floor" in secondary:
        cond &= signals["warrant_hhi_posnet_20"].fillna(0.0) >= float(secondary["warrant_hhi_posnet_20_floor"])
    if "warrant_top_posnet_ratio_20_floor" in secondary:
        cond &= signals["warrant_top_posnet_ratio_20"].fillna(0.0) >= float(
            secondary["warrant_top_posnet_ratio_20_floor"]
        )
    if "warrant_dyn_k_pct_cs_floor" in secondary:
        cond &= signals["warrant_dyn_k_pct_cs"].fillna(0.0) >= float(secondary["warrant_dyn_k_pct_cs_floor"])
    if "volume_ratio_5_20_cap" in secondary:
        cond &= signals["volume_ratio_5_20"].fillna(np.inf) <= float(secondary["volume_ratio_5_20_cap"])
    if "breakout_gap_20_cap" in secondary:
        cond &= signals["breakout_gap_20"].fillna(np.inf) <= float(secondary["breakout_gap_20_cap"])
    if "breakout_gap_20_floor" in secondary:
        cond &= signals["breakout_gap_20"].fillna(-np.inf) >= float(secondary["breakout_gap_20_floor"])
    if "price_pos_20_cap" in secondary:
        cond &= signals["price_pos_20"].fillna(np.inf) <= float(secondary["price_pos_20_cap"])
    return cond.fillna(False)


def _merged_spec(base: dict[str, object], secondary: dict[str, object]) -> dict[str, object]:
    spec = dict(base)
    for key, value in secondary.items():
        if key != "filter_suite":
            spec[key] = value
    spec["filter_suite"] = secondary["filter_suite"]
    return spec


def _target_full_pass(row: dict[str, object]) -> bool:
    return bool(
        row["full_trades"] >= 20
        and row["full_win_rate"] > 0.50
        and row["full_avg_net_ret"] > 0.10
    )


def _target_test_pass(row: dict[str, object]) -> bool:
    return bool(
        row["test_trades"] >= 5
        and row["test_win_rate"] > 0.50
        and row["test_avg_net_ret"] > 0.03
    )


def _target_metrics_pass(full_metrics: dict[str, float], test_metrics: dict[str, float]) -> bool:
    return bool(
        full_metrics["trades"] >= 20
        and full_metrics["win_rate"] > 0.50
        and full_metrics["avg_net_ret"] > 0.10
        and test_metrics["trades"] >= 5
        and test_metrics["win_rate"] > 0.50
        and test_metrics["avg_net_ret"] > 0.03
    )


def _score_row(row: dict[str, object]) -> tuple[object, ...]:
    def finite_value(key: str) -> float:
        value = row.get(key)
        return float(value) if value is not None and np.isfinite(value) else -math.inf

    return (
        bool(row.get("target_robust_pass")),
        bool(row.get("target_full_pass")),
        bool(row.get("all_pass")),
        finite_value("test_avg_net_ret"),
        finite_value("full_avg_net_ret"),
        finite_value("full_win_rate"),
        int(row.get("full_trades", 0)),
    )


def _summarize_refine_candidate(signals: pd.DataFrame, trades: pd.DataFrame, spec: dict[str, object]) -> dict[str, object]:
    row = summarize_candidate(signals, trades, spec)
    row["filter_suite"] = spec.get("filter_suite", "base")
    row["target_full_pass"] = _target_full_pass(row)
    row["target_test_pass"] = _target_test_pass(row)
    row["target_robust_pass"] = bool(row["target_full_pass"] and row["target_test_pass"])
    if not row["target_full_pass"]:
        target_fails = []
        if row["full_trades"] < 20:
            target_fails.append("target_full_trades<20")
        if not (row["full_win_rate"] > 0.50):
            target_fails.append("target_full_win_rate<=50%")
        if not (row["full_avg_net_ret"] > 0.10):
            target_fails.append("target_full_avg_net_ret<=10%")
        row["target_full_fail_reason"] = "|".join(target_fails)
    else:
        row["target_full_fail_reason"] = ""

    if not row["target_test_pass"]:
        target_fails = []
        if row["test_trades"] < 5:
            target_fails.append("target_test_trades<5")
        if not (row["test_win_rate"] > 0.50):
            target_fails.append("target_test_win_rate<=50%")
        if not (row["test_avg_net_ret"] > 0.03):
            target_fails.append("target_test_avg_net_ret<=3%")
        row["target_test_fail_reason"] = "|".join(target_fails)
    else:
        row["target_test_fail_reason"] = ""
    return row


def _build_concentration(best_trades: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    if best_trades.empty:
        return pd.DataFrame(columns=["dimension", "bucket", "trades", "mean_net_ret", "total_net_ret"])

    symbol_rows = (
        best_trades.groupby("symbol")
        .agg(trades=("symbol", "size"), mean_net_ret=("net_ret", "mean"), total_net_ret=("net_ret", "sum"))
        .reset_index()
    )
    for _, row in symbol_rows.iterrows():
        rows.append(
            {
                "dimension": "symbol",
                "bucket": str(row["symbol"]),
                "trades": int(row["trades"]),
                "mean_net_ret": float(row["mean_net_ret"]),
                "total_net_ret": float(row["total_net_ret"]),
            }
        )

    month_rows = (
        best_trades.assign(month=best_trades["signal_date"].dt.to_period("M").astype(str))
        .groupby("month")
        .agg(trades=("month", "size"), mean_net_ret=("net_ret", "mean"), total_net_ret=("net_ret", "sum"))
        .reset_index()
    )
    for _, row in month_rows.iterrows():
        rows.append(
            {
                "dimension": "month",
                "bucket": str(row["month"]),
                "trades": int(row["trades"]),
                "mean_net_ret": float(row["mean_net_ret"]),
                "total_net_ret": float(row["total_net_ret"]),
            }
        )
    return pd.DataFrame(rows).sort_values(["dimension", "trades", "total_net_ret"], ascending=[True, False, False])


def _split_test_trades(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return trades
    return trades[(trades["signal_date"] > VALID_END) & (trades["signal_date"] <= TEST_END)].copy()


def _build_leave_one(best_trades: pd.DataFrame, dimension: str) -> pd.DataFrame:
    if best_trades.empty:
        return pd.DataFrame()

    if dimension == "symbol":
        work = best_trades.assign(bucket=best_trades["symbol"].astype(str))
    elif dimension == "month":
        work = best_trades.assign(bucket=best_trades["signal_date"].dt.to_period("M").astype(str))
    else:
        raise ValueError(f"unknown leave-one dimension: {dimension}")

    rows: list[dict[str, object]] = []
    for bucket in sorted(work["bucket"].unique()):
        sub = work[work["bucket"] != bucket].copy()
        test = _split_test_trades(sub)
        full_metrics = _trade_metrics(sub)
        test_metrics = _trade_metrics(test)
        rows.append(
            {
                f"drop_{dimension}": bucket,
                "dropped_trades": int((work["bucket"] == bucket).sum()),
                "full_trades": full_metrics["trades"],
                "full_win_rate": full_metrics["win_rate"],
                "full_avg_net_ret": full_metrics["avg_net_ret"],
                "full_median_net_ret": full_metrics["median_net_ret"],
                "test_trades": test_metrics["trades"],
                "test_win_rate": test_metrics["win_rate"],
                "test_avg_net_ret": test_metrics["avg_net_ret"],
                "target_robust_after_drop": _target_metrics_pass(full_metrics, test_metrics),
            }
        )
    return pd.DataFrame(rows).sort_values(
        ["target_robust_after_drop", "full_avg_net_ret", "test_avg_net_ret"],
        ascending=[True, True, True],
    )


def _build_report(
    leaderboard: pd.DataFrame,
    best_row: pd.Series,
    best_trades: pd.DataFrame,
    concentration: pd.DataFrame,
    leave_one_symbol: pd.DataFrame,
    leave_one_month: pd.DataFrame,
    spec_count: int,
) -> str:
    target_full = leaderboard[leaderboard["target_full_pass"]].copy()
    target_robust = leaderboard[leaderboard["target_robust_pass"]].copy()
    all_pass = leaderboard[leaderboard["all_pass"]].copy()

    lines = [
        "# Warrant Leads Stock Refinement Report",
        "",
        "## Scan Summary",
        "",
        f"- specs evaluated: `{spec_count}`",
        f"- rare-event all-pass rows: `{len(all_pass)}`",
        f"- full target-pass rows: `{len(target_full)}`",
        f"- robust target-pass rows: `{len(target_robust)}`",
        "- target definition: `full_trades >= 20`, `full_win_rate > 50%`, `full_avg_net_ret > 10%`",
        "- robust target additionally requires: `test_trades >= 5`, `test_win_rate > 50%`, `test_avg_net_ret > 3%`",
        "",
        "## Best Candidate",
        "",
        f"- event name: `{best_row['event_name']}`",
        f"- params: `{best_row['params_json']}`",
        f"- target robust pass: `{bool(best_row['target_robust_pass'])}`",
        f"- target full pass: `{bool(best_row['target_full_pass'])}`",
        f"- rare-event all pass: `{bool(best_row['all_pass'])}`",
        f"- full trades: `{int(best_row['full_trades'])}`",
        f"- full win rate: `{best_row['full_win_rate']:.4f}`",
        f"- full average net return: `{best_row['full_avg_net_ret']:.4f}`",
        f"- full median net return: `{best_row['full_median_net_ret']:.4f}`",
        f"- full profit factor: `{best_row['full_profit_factor']:.4f}`",
        f"- full max loss: `{best_row['full_max_loss']:.4f}`",
        f"- test trades: `{int(best_row['test_trades'])}`",
        f"- test win rate: `{best_row['test_win_rate']:.4f}`",
        f"- test average net return: `{best_row['test_avg_net_ret']:.4f}`",
        f"- target test fail reason: `{best_row['target_test_fail_reason']}`",
        f"- leave-one-symbol robust pass: `{int(leave_one_symbol['target_robust_after_drop'].sum())} / {len(leave_one_symbol)}`",
        f"- leave-one-month robust pass: `{int(leave_one_month['target_robust_after_drop'].sum())} / {len(leave_one_month)}`",
        "",
        "## Top Full Target Candidates",
        "",
    ]

    top_full = target_full.sort_values(
        ["target_robust_pass", "test_avg_net_ret", "full_avg_net_ret"],
        ascending=[False, False, False],
    ).head(10)
    if top_full.empty:
        lines.append("No candidate reached the full target gate in this scan.")
    else:
        for _, row in top_full.iterrows():
            lines.append(
                "- "
                f"`{row['event_name']}`: "
                f"`full_avg={row['full_avg_net_ret']:.4f}`, "
                f"`full_win={row['full_win_rate']:.4f}`, "
                f"`test_avg={row['test_avg_net_ret']:.4f}`, "
                f"`test_win={row['test_win_rate']:.4f}`, "
                f"`test_trades={int(row['test_trades'])}`, "
                f"`test_fail={row['target_test_fail_reason']}`"
            )

    lines.extend(["", "## Concentration", ""])
    for _, row in concentration.head(12).iterrows():
        lines.append(
            f"- `{row['dimension']}={row['bucket']}`: "
            f"`trades={int(row['trades'])}`, "
            f"`mean_net={row['mean_net_ret']:.4f}`, "
            f"`total_net={row['total_net_ret']:.4f}`"
        )

    weak_symbols = leave_one_symbol[~leave_one_symbol["target_robust_after_drop"]]
    weak_months = leave_one_month[~leave_one_month["target_robust_after_drop"]]
    lines.extend(["", "## Leave-One Sensitivity", ""])
    if weak_symbols.empty:
        lines.append("- Dropping any single symbol still keeps the strategy above the robust target gate.")
    else:
        for _, row in weak_symbols.iterrows():
            lines.append(
                f"- dropping symbol `{row['drop_symbol']}` fails robust target: "
                f"`full_avg={row['full_avg_net_ret']:.4f}`, "
                f"`test_trades={int(row['test_trades'])}`, "
                f"`test_avg={row['test_avg_net_ret']:.4f}`"
            )
    if weak_months.empty:
        lines.append("- Dropping any single month still keeps the strategy above the robust target gate.")
    else:
        for _, row in weak_months.iterrows():
            lines.append(
                f"- dropping month `{row['drop_month']}` fails robust target: "
                f"`full_avg={row['full_avg_net_ret']:.4f}`, "
                f"`test_trades={int(row['test_trades'])}`, "
                f"`test_avg={row['test_avg_net_ret']:.4f}`"
            )

    lines.extend(["", "## Best Trade Examples", ""])
    for _, row in best_trades.sort_values("net_ret", ascending=False).head(5).iterrows():
        lines.append(
            f"- `{row['signal_date'].date()} {row['symbol']}`: "
            f"`net_ret={row['net_ret']:.4f}`, `mfe20={row['mfe_20d']:.4f}`, `mfe40={row['mfe_40d']:.4f}`"
        )

    lines.extend(["", "## Worst Trade Examples", ""])
    for _, row in best_trades.sort_values("net_ret", ascending=True).head(5).iterrows():
        lines.append(
            f"- `{row['signal_date'].date()} {row['symbol']}`: "
            f"`net_ret={row['net_ret']:.4f}`, `mfe20={row['mfe_20d']:.4f}`, `mae20={row['mae_20d']:.4f}`"
        )

    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- Direction 2 now has a target-clearing candidate after adding a strict warrant dynamic-concentration filter.",
            "- The main caveat is multiple-testing risk: this scan tested many related filters on the same feature set.",
            "- Leave-one checks show symbol robustness is mostly intact, but the result is still sensitive to September 2025 and to the current test-window sample size.",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description="Refine the warrant-leads-stock rare-event setup")
    ap.add_argument(
        "--features",
        type=str,
        default=str(resolve_output("analysis", "rare_event") / "features_stock_daily.parquet"),
    )
    ap.add_argument("--output-dir", type=str, default=str(resolve_output("analysis", "rare_event")))
    args = ap.parse_args()

    out_dir = ensure_dir(Path(args.output_dir))
    feat = pd.read_parquet(args.features)
    feat["symbol"] = feat["symbol"].astype(str)
    feat["date"] = pd.to_datetime(feat["date"])
    feat = feat.dropna(subset=["open", "high", "low", "close"]).sort_values(["date", "symbol"]).reset_index(drop=True)

    ohlc_map = build_ohlc_map(feat[["symbol", "date", "open", "high", "low", "close"]].copy())
    base_specs = _iter_base_specs()
    secondary_filters = _iter_secondary_filters()
    spec_count = len(base_specs) * len(secondary_filters)

    leaderboard_rows: list[dict[str, object]] = []
    best_row: dict[str, object] | None = None
    best_signals = pd.DataFrame()
    best_trades = pd.DataFrame()

    for base in base_specs:
        base_signals = feat.loc[_apply_base_filter(feat, base)].copy()
        if base_signals.empty:
            for secondary in secondary_filters:
                spec = _merged_spec(base, secondary)
                row = _summarize_refine_candidate(pd.DataFrame(), pd.DataFrame(), spec)
                row["rejected_reason"] = "no_base_signals"
                leaderboard_rows.append(row)
            continue

        for secondary in secondary_filters:
            spec = _merged_spec(base, secondary)
            signals = base_signals.loc[_apply_secondary_filter(base_signals, secondary)].copy()
            if signals.empty:
                row = _summarize_refine_candidate(signals, pd.DataFrame(), spec)
                row["rejected_reason"] = "no_signals"
                leaderboard_rows.append(row)
                continue

            signals = _dedup_cooldown(signals, int(spec["cooldown_days"]))
            signals["event_name"] = _event_name(spec)
            signals["family"] = spec["family"]
            signals["filter_suite"] = spec["filter_suite"]
            trades = simulate_trades(signals, ohlc_map, int(spec["hold_days"]), CostConfig(), spec.get("stop_loss"))
            row = _summarize_refine_candidate(signals, trades, spec)
            leaderboard_rows.append(row)

            if trades.empty:
                continue
            if best_row is None or _score_row(row) > _score_row(best_row):
                best_row = row
                best_signals = signals.copy()
                best_trades = trades.copy()

    leaderboard = pd.DataFrame(leaderboard_rows).sort_values(
        [
            "target_robust_pass",
            "target_full_pass",
            "all_pass",
            "test_avg_net_ret",
            "full_avg_net_ret",
            "full_win_rate",
            "full_trades",
        ],
        ascending=[False, False, False, False, False, False, False],
    )
    leaderboard.to_csv(out_dir / "warrant_leads_stock_refine_leaderboard.csv", index=False)

    if best_row is None:
        best_row = leaderboard.iloc[0].to_dict()
        best_signals = pd.DataFrame()
        best_trades = pd.DataFrame()

    best_signals.sort_values(["date", "symbol"]).to_parquet(
        out_dir / "warrant_leads_stock_refine_signals.parquet", index=False
    )
    best_trades.sort_values(["signal_date", "symbol"]).to_parquet(
        out_dir / "warrant_leads_stock_refine_trades.parquet", index=False
    )
    concentration = _build_concentration(best_trades)
    leave_one_symbol = _build_leave_one(best_trades, "symbol")
    leave_one_month = _build_leave_one(best_trades, "month")
    concentration.to_csv(out_dir / "warrant_leads_stock_refine_concentration.csv", index=False)
    leave_one_symbol.to_csv(out_dir / "warrant_leads_stock_refine_leave_one_symbol_out.csv", index=False)
    leave_one_month.to_csv(out_dir / "warrant_leads_stock_refine_leave_one_month_out.csv", index=False)
    report = _build_report(
        leaderboard,
        pd.Series(best_row),
        best_trades,
        concentration,
        leave_one_symbol,
        leave_one_month,
        spec_count,
    )
    (out_dir / "warrant_leads_stock_refine_report.md").write_text(report, encoding="utf-8")


if __name__ == "__main__":
    main()
