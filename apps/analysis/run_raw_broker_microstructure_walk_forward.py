#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Rolling walk-forward test for raw broker microstructure strategies."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import run_raw_broker_microstructure_research as research


PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = PROJECT_ROOT / "outputs" / "analysis" / "raw_broker_microstructure"
ROBUSTNESS_DIR = OUT_DIR / "robustness"
FEATURE_PATH = OUT_DIR / "raw_micro_features.parquet"


@dataclass(frozen=True)
class Fold:
    name: str
    train_start: pd.Timestamp
    train_end: pd.Timestamp
    test_start: pd.Timestamp
    test_end: pd.Timestamp


def _metrics(trades: pd.DataFrame) -> dict[str, Any]:
    return research.trade_metrics(trades)


def _target_pass(metrics: dict[str, Any], min_trades: int) -> bool:
    return bool(
        metrics["trades"] >= min_trades
        and metrics["win_rate"] > 0.60
        and metrics["avg_net_ret"] > 0.10
    )


def _select_score(metrics: dict[str, Any], min_trades: int) -> tuple[Any, ...]:
    return (
        _target_pass(metrics, min_trades),
        float(metrics["avg_net_ret"]) if np.isfinite(metrics["avg_net_ret"]) else -np.inf,
        float(metrics["win_rate"]) if np.isfinite(metrics["win_rate"]) else -np.inf,
        int(metrics["trades"]),
    )


def build_folds(
    start: str,
    end: str,
    train_months: int,
    test_months: int,
    step_months: int,
) -> list[Fold]:
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    folds: list[Fold] = []
    test_start = start_ts + pd.DateOffset(months=train_months)
    while test_start <= end_ts:
        train_start = test_start - pd.DateOffset(months=train_months)
        train_end = test_start - pd.Timedelta(days=1)
        test_end = min(test_start + pd.DateOffset(months=test_months) - pd.Timedelta(days=1), end_ts)
        if test_end <= test_start:
            break
        folds.append(
            Fold(
                name=f"{test_start.strftime('%Y%m%d')}_{test_end.strftime('%Y%m%d')}",
                train_start=train_start,
                train_end=train_end,
                test_start=test_start,
                test_end=test_end,
            )
        )
        test_start = test_start + pd.DateOffset(months=step_months)
    return folds


def _window(trades: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    dates = pd.to_datetime(trades["signal_date"])
    return trades.loc[(dates >= start) & (dates <= end)]


def _candidate_trades(
    feat: pd.DataFrame,
    ohlc_maps: dict[str, dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for family, params_list in research.iter_family_params().items():
        candidates: list[dict[str, Any]] = []
        for param_i, params in enumerate(params_list, start=1):
            signals0 = research._make_signals(feat, family, params)
            for hold_days, stop_loss, cooldown_days in research.itertools.product(
                [10, 20, 40, 60],
                [None, -0.10, -0.15],
                [10, 20],
            ):
                signals = research._dedup_cooldown(signals0, cooldown_days)
                trades = research.simulate_trades(signals, ohlc_maps, hold_days, stop_loss)
                if trades.empty:
                    continue
                candidates.append(
                    {
                        "hold_days": hold_days,
                        "stop_loss": stop_loss,
                        "cooldown_days": cooldown_days,
                        "params_json": json.dumps(params, sort_keys=True),
                        "trades": trades,
                    }
                )
            if param_i % 25 == 0 or param_i == len(params_list):
                print(f"cached {family} params {param_i}/{len(params_list)}: candidates={len(candidates)}", flush=True)
        out[family] = candidates
    return out


def run_walk_forward(
    candidates_by_family: dict[str, list[dict[str, Any]]],
    folds: list[Fold],
    train_min_trades: int,
    fold_test_min_trades: int,
    aggregate_test_min_trades: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    fold_rows: list[dict[str, Any]] = []
    selected_tests: list[pd.DataFrame] = []
    for family, candidates in candidates_by_family.items():
        for fold in folds:
            best_score: tuple[Any, ...] | None = None
            best_row: dict[str, Any] | None = None
            best_test: pd.DataFrame | None = None
            for candidate in candidates:
                trades = candidate["trades"]
                train = _window(trades, fold.train_start, fold.train_end)
                test = _window(trades, fold.test_start, fold.test_end)
                train_metrics = _metrics(train)
                score = _select_score(train_metrics, train_min_trades)
                if best_score is None or score > best_score:
                    test_metrics = _metrics(test)
                    best_score = score
                    best_test = test.copy()
                    best_row = {
                        "event_family": family,
                        "fold": fold.name,
                        "train_start": fold.train_start.strftime("%Y-%m-%d"),
                        "train_end": fold.train_end.strftime("%Y-%m-%d"),
                        "test_start": fold.test_start.strftime("%Y-%m-%d"),
                        "test_end": fold.test_end.strftime("%Y-%m-%d"),
                        "hold_days": candidate["hold_days"],
                        "stop_loss": candidate["stop_loss"],
                        "cooldown_days": candidate["cooldown_days"],
                        "params_json": candidate["params_json"],
                        "train_target_pass": _target_pass(train_metrics, train_min_trades),
                        **{f"train_{k}": v for k, v in train_metrics.items()},
                        "test_target_pass": _target_pass(test_metrics, fold_test_min_trades),
                        **{f"test_{k}": v for k, v in test_metrics.items()},
                    }
            if best_row is not None:
                fold_rows.append(best_row)
                if best_test is not None and not best_test.empty:
                    test_copy = best_test.copy()
                    test_copy["selected_family"] = family
                    test_copy["fold"] = fold.name
                    selected_tests.append(test_copy)

    fold_results = pd.DataFrame(fold_rows)
    all_tests = pd.concat(selected_tests, ignore_index=True) if selected_tests else pd.DataFrame()
    aggregate_rows: list[dict[str, Any]] = []
    for family, sub in all_tests.groupby("selected_family", sort=False):
        metrics = _metrics(sub)
        folds_sub = fold_results.loc[fold_results["event_family"] == family]
        evaluable = folds_sub.loc[folds_sub["test_trades"] >= fold_test_min_trades]
        aggregate_rows.append(
            {
                "event_family": family,
                "folds": int(folds_sub["fold"].nunique()),
                "evaluable_folds": int(evaluable["fold"].nunique()),
                "no_closed_test_folds": int((folds_sub["test_trades"] == 0).sum()),
                "folds_test_passed": int(evaluable["test_target_pass"].sum()),
                "folds_train_passed": int(folds_sub["train_target_pass"].sum()),
                "aggregate_test_target_pass": _target_pass(metrics, aggregate_test_min_trades),
                **{f"aggregate_test_{k}": v for k, v in metrics.items()},
            }
        )
    return fold_results, pd.DataFrame(aggregate_rows)


def _write_report(folds: list[Fold], fold_results: pd.DataFrame, aggregate: pd.DataFrame) -> None:
    lines = [
        "# Raw Broker Microstructure Rolling Walk-Forward",
        "",
        "Method:",
        "",
        "- Build rolling train/test folds.",
        "- For each fold and family, select parameters only on that fold's train window.",
        "- Evaluate only on that fold's later test window.",
        "- Aggregate selected test-window trades across folds.",
        "",
        "## Folds",
        "",
        "| fold | train | test |",
        "|---|---|---|",
    ]
    for fold in folds:
        lines.append(
            f"| `{fold.name}` | `{fold.train_start.date()} ~ {fold.train_end.date()}` | "
            f"`{fold.test_start.date()} ~ {fold.test_end.date()}` |"
        )
    lines.extend(["", "## Aggregate Test Result", ""])
    lines.append("| family | aggregate_pass | folds_passed | no_closed_folds | trades | win | avg | median | max_loss |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    for _, row in aggregate.iterrows():
        lines.append(
            f"| `{row['event_family']}` | `{bool(row['aggregate_test_target_pass'])}` | "
            f"{int(row['folds_test_passed'])}/{int(row['evaluable_folds'])} | "
            f"{int(row['no_closed_test_folds'])} | "
            f"{int(row['aggregate_test_trades'])} | {row['aggregate_test_win_rate']:.4f} | "
            f"{row['aggregate_test_avg_net_ret']:.4f} | {row['aggregate_test_median_net_ret']:.4f} | "
            f"{row['aggregate_test_max_loss']:.4f} |"
        )
    lines.extend(["", "## Fold Detail", ""])
    for _, row in fold_results.iterrows():
        lines.append(
            f"- `{row['event_family']}` `{row['fold']}`: train pass `{bool(row['train_target_pass'])}`, "
            f"test pass `{bool(row['test_target_pass'])}`, test trades `{int(row['test_trades'])}`, "
            f"test win `{row['test_win_rate']:.4f}`, test avg `{row['test_avg_net_ret']:.4f}`, "
            f"hold `{int(row['hold_days'])}`"
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- Aggregate pass means the selected out-of-sample trades across folds still meet the original target gate.",
            "- Fold-level failures identify periods where the train-selected configuration did not generalize immediately.",
            "- Folds with no closed trades are listed separately because a 60-trading-day exit can extend beyond the available data end.",
            "- This is stronger than the fixed holdout, but sample size remains limited because the raw broker dataset starts in 2025-04.",
        ]
    )
    (ROBUSTNESS_DIR / "raw_microstructure_walk_forward_report.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="Rolling walk-forward for raw broker microstructure strategies")
    ap.add_argument("--start", default="2025-04-14")
    ap.add_argument("--end", default="2026-06-12")
    ap.add_argument("--train-months", type=int, default=6)
    ap.add_argument("--test-months", type=int, default=2)
    ap.add_argument("--step-months", type=int, default=2)
    ap.add_argument("--train-min-trades", type=int, default=20)
    ap.add_argument("--fold-test-min-trades", type=int, default=5)
    ap.add_argument("--aggregate-test-min-trades", type=int, default=20)
    args = ap.parse_args()

    ROBUSTNESS_DIR.mkdir(parents=True, exist_ok=True)
    if not FEATURE_PATH.exists():
        raise RuntimeError(f"missing feature cache: {FEATURE_PATH}")
    feat = pd.read_parquet(FEATURE_PATH)
    ohlc = research.load_raw_ohlc(args.start, args.end, exclude_etf=True)
    ohlc_maps = research.build_ohlc_maps(ohlc)
    folds = build_folds(args.start, args.end, args.train_months, args.test_months, args.step_months)
    candidates_by_family = _candidate_trades(feat, ohlc_maps)
    fold_results, aggregate = run_walk_forward(
        candidates_by_family,
        folds,
        args.train_min_trades,
        args.fold_test_min_trades,
        args.aggregate_test_min_trades,
    )
    fold_results.to_csv(ROBUSTNESS_DIR / "raw_microstructure_walk_forward_folds.csv", index=False)
    aggregate.to_csv(ROBUSTNESS_DIR / "raw_microstructure_walk_forward_aggregate.csv", index=False)
    summary = {
        "train_months": args.train_months,
        "test_months": args.test_months,
        "step_months": args.step_months,
        "folds": [fold.name for fold in folds],
        "aggregate_pass_count": int(aggregate["aggregate_test_target_pass"].sum()) if not aggregate.empty else 0,
        "aggregate_fail_count": int((~aggregate["aggregate_test_target_pass"]).sum()) if not aggregate.empty else 0,
    }
    (ROBUSTNESS_DIR / "raw_microstructure_walk_forward_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    _write_report(folds, fold_results, aggregate)
    print(ROBUSTNESS_DIR / "raw_microstructure_walk_forward_report.md")


if __name__ == "__main__":
    main()
