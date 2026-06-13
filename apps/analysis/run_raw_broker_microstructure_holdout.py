#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Holdout parameter-selection test for raw broker microstructure strategies."""

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
FEATURE_PATH = OUT_DIR / "raw_micro_features.parquet"


def _metrics(trades: pd.DataFrame) -> dict[str, Any]:
    return research.trade_metrics(trades)


def _target_pass(metrics: dict[str, Any], min_trades: int) -> bool:
    return bool(
        metrics["trades"] >= min_trades
        and metrics["win_rate"] > 0.60
        and metrics["avg_net_ret"] > 0.10
    )


def _select_score(metrics: dict[str, Any], min_trades: int) -> tuple[Any, ...]:
    target_pass = _target_pass(metrics, min_trades)
    return (
        target_pass,
        float(metrics["avg_net_ret"]) if np.isfinite(metrics["avg_net_ret"]) else -np.inf,
        float(metrics["win_rate"]) if np.isfinite(metrics["win_rate"]) else -np.inf,
        int(metrics["trades"]),
    )


def run_holdout(
    feat: pd.DataFrame,
    ohlc_maps: dict[str, dict[str, Any]],
    cutoff: str,
    train_min_trades: int,
    test_min_trades: int,
) -> pd.DataFrame:
    cutoff_ts = pd.Timestamp(cutoff)
    rows: list[dict[str, Any]] = []
    for family, params_list in research.iter_family_params().items():
        best_score: tuple[Any, ...] | None = None
        best_row: dict[str, Any] | None = None
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
                signal_dates = pd.to_datetime(trades["signal_date"])
                train = trades.loc[signal_dates < cutoff_ts]
                test = trades.loc[signal_dates >= cutoff_ts]
                train_metrics = _metrics(train)
                score = _select_score(train_metrics, train_min_trades)
                if best_score is None or score > best_score:
                    test_metrics = _metrics(test)
                    best_score = score
                    best_row = {
                        "event_family": family,
                        "cutoff": cutoff,
                        "hold_days": hold_days,
                        "stop_loss": stop_loss,
                        "cooldown_days": cooldown_days,
                        "params_json": json.dumps(params, sort_keys=True),
                        "train_target_pass": _target_pass(train_metrics, train_min_trades),
                        **{f"train_{k}": v for k, v in train_metrics.items()},
                        "test_target_pass": _target_pass(test_metrics, test_min_trades),
                        **{f"test_{k}": v for k, v in test_metrics.items()},
                    }
            if param_i % 25 == 0 or param_i == len(params_list):
                print(f"holdout searched {family} params {param_i}/{len(params_list)}: best={best_score}", flush=True)
        if best_row is not None:
            rows.append(best_row)
    return pd.DataFrame(rows)


def _write_report(holdout: pd.DataFrame) -> None:
    lines = [
        "# Raw Broker Microstructure Holdout Test",
        "",
        "This is a stricter overfitting check than the robustness summary.",
        "",
        "Method:",
        "",
        "- Use the full parameter grid from `run_raw_broker_microstructure_research.py`.",
        "- Select each family only by train-period metrics before the cutoff.",
        "- Evaluate the selected row on signal dates after the cutoff.",
        "- This still uses one fixed cutoff, not rolling walk-forward.",
        "",
        "## Result",
        "",
        "| family | train_pass | train_trades | train_avg | train_win | test_pass | test_trades | test_avg | test_win | selected_hold |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _, row in holdout.iterrows():
        lines.append(
            f"| `{row['event_family']}` | `{bool(row['train_target_pass'])}` | "
            f"{int(row['train_trades'])} | {row['train_avg_net_ret']:.4f} | {row['train_win_rate']:.4f} | "
            f"`{bool(row['test_target_pass'])}` | {int(row['test_trades'])} | "
            f"{row['test_avg_net_ret']:.4f} | {row['test_win_rate']:.4f} | {int(row['hold_days'])} |"
        )
    lines.extend(["", "## Selected Parameters", ""])
    for _, row in holdout.iterrows():
        lines.append(f"- `{row['event_family']}`: `{row['params_json']}`")
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- Passing the test period reduces the likelihood that the optimized idea is only a full-sample artifact.",
            "- Failing the test period means the optimized full-sample result should be treated as overfit until redesigned.",
        ]
    )
    (ROBUSTNESS_DIR / "raw_microstructure_holdout_report.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="Holdout test for raw broker microstructure strategies")
    ap.add_argument("--start", default="2025-04-14")
    ap.add_argument("--end", default="2026-06-12")
    ap.add_argument("--cutoff", default="2026-01-01")
    ap.add_argument("--train-min-trades", type=int, default=20)
    ap.add_argument("--test-min-trades", type=int, default=20)
    args = ap.parse_args()

    ROBUSTNESS_DIR.mkdir(parents=True, exist_ok=True)
    if not FEATURE_PATH.exists():
        raise RuntimeError(f"missing feature cache: {FEATURE_PATH}")
    feat = pd.read_parquet(FEATURE_PATH)
    ohlc = research.load_raw_ohlc(args.start, args.end, exclude_etf=True)
    ohlc_maps = research.build_ohlc_maps(ohlc)
    holdout = run_holdout(feat, ohlc_maps, args.cutoff, args.train_min_trades, args.test_min_trades)
    holdout.to_csv(ROBUSTNESS_DIR / "raw_microstructure_holdout.csv", index=False)
    summary = {
        "cutoff": args.cutoff,
        "families": holdout["event_family"].tolist(),
        "test_pass_count": int(holdout["test_target_pass"].sum()),
        "test_fail_count": int((~holdout["test_target_pass"]).sum()),
    }
    (ROBUSTNESS_DIR / "raw_microstructure_holdout_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    _write_report(holdout)
    print(ROBUSTNESS_DIR / "raw_microstructure_holdout_report.md")


if __name__ == "__main__":
    main()
