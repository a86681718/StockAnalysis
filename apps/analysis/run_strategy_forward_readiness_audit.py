#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Audit whether the current target-clearing strategies can be forward-tested with
the artifacts currently present in the repo.

This script does not create new signals. It records the date coverage of the
trusted prediction/feature artifacts, the latest completed exits, and which
signals are still not fully observable.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.stockanalysis.config import ensure_dir, resolve_output  # noqa: E402


ML_RUNS = PROJECT_ROOT / "data" / "_derived" / "ml_runs"
RARE_EVENT = PROJECT_ROOT / "outputs" / "analysis" / "rare_event"


def _date_stats(path: Path, date_col: str = "date") -> dict[str, object]:
    if path.suffix == ".parquet":
        df = pd.read_parquet(path)
    elif path.suffix == ".csv":
        df = pd.read_csv(path, low_memory=False)
    else:
        raise ValueError(f"unsupported file type: {path}")
    if date_col not in df.columns:
        return {"path": str(path), "rows": int(len(df)), "date_col": date_col, "min_date": None, "max_date": None, "unique_dates": 0}
    dates = pd.to_datetime(df[date_col], errors="coerce")
    return {
        "path": str(path),
        "rows": int(len(df)),
        "date_col": date_col,
        "min_date": dates.min().date().isoformat() if dates.notna().any() else None,
        "max_date": dates.max().date().isoformat() if dates.notna().any() else None,
        "unique_dates": int(dates.nunique()),
    }


def _load_ohlc_dates(path: Path) -> pd.DatetimeIndex:
    ohlc = pd.read_parquet(path, columns=["date"])
    dates = pd.DatetimeIndex(pd.to_datetime(ohlc["date"]).dropna().unique()).sort_values()
    return dates


def _latest_complete_signal_date(trading_dates: pd.DatetimeIndex, hold_days: int) -> pd.Timestamp | None:
    if len(trading_dates) <= hold_days + 1:
        return None
    return pd.Timestamp(trading_dates[-(hold_days + 2)])


def _direction3_audit(ohlc_dates: pd.DatetimeIndex) -> tuple[dict[str, object], pd.DataFrame]:
    filtered = pd.read_csv(ML_RUNS / "breakout10_predictions_wf_repair_branch_topratio0610_daybuy20ge4.csv")
    base_wf = pd.read_csv(ML_RUNS / "breakout10_predictions_wf.csv")
    all_wf = pd.read_csv(ML_RUNS / "breakout10_predictions_all_wf.csv")
    day_quality = pd.read_csv(ML_RUNS / "repair_branch_topratio0610_day_chip_quality.csv")
    trades = pd.read_csv(ML_RUNS / "repair_branch_topratio0610_daybuy20ge4_trades.csv")
    summary = json.loads((ML_RUNS / "repair_branch_topratio0610_daybuy20ge4_summary.json").read_text(encoding="utf-8"))

    for frame, col in [(filtered, "date"), (base_wf, "date"), (all_wf, "date"), (day_quality, "signal_date"), (trades, "signal_date"), (trades, "exit_date")]:
        frame[col] = pd.to_datetime(frame[col], errors="coerce")

    complete_signal_date = _latest_complete_signal_date(ohlc_dates, hold_days=16)
    filtered_dates = pd.DatetimeIndex(filtered["date"].dropna().unique()).sort_values()
    post_filtered_dates = filtered_dates[filtered_dates > pd.Timestamp(summary["window"]["end"])]

    rows = [
        _date_stats(ML_RUNS / "breakout10_predictions_wf.csv"),
        _date_stats(ML_RUNS / "breakout10_predictions_all_wf.csv"),
        _date_stats(ML_RUNS / "breakout10_predictions_wf_repair_branch_topratio0610.csv"),
        _date_stats(ML_RUNS / "breakout10_predictions_wf_repair_branch_topratio0610_daybuy20ge4.csv"),
        _date_stats(ML_RUNS / "repair_branch_topratio0610_day_chip_quality.csv", "signal_date"),
        _date_stats(ML_RUNS / "repair_branch_topratio0610_daybuy20ge4_trades.csv", "signal_date"),
    ]
    artifact_dates = pd.DataFrame(rows)

    audit = {
        "trusted_wf_max_date": str(pd.to_datetime(base_wf["date"]).max().date()),
        "all_wf_max_date": str(pd.to_datetime(all_wf["date"]).max().date()),
        "filtered_signal_max_date": str(filtered["date"].max().date()),
        "saved_trade_signal_max_date": str(trades["signal_date"].max().date()),
        "saved_trade_exit_max_date": str(trades["exit_date"].max().date()),
        "last_complete_signal_date_from_ohlc": str(complete_signal_date.date()) if complete_signal_date is not None else None,
        "filtered_dates_after_summary_end": int(len(post_filtered_dates)),
        "full_trades": int(summary["summary"]["trades"]),
        "full_win_rate": float(summary["summary"]["win_net_ret"]),
        "full_avg_net_ret": float(summary["summary"]["mean_net_ret"]),
        "can_extend_with_trusted_wf": bool(pd.to_datetime(base_wf["date"]).max() > pd.Timestamp(summary["window"]["end"])),
        "can_extend_filtered_strategy_now": bool(filtered["date"].max() > pd.Timestamp(summary["window"]["end"])),
        "reason": "Trusted breakout10_predictions_wf stops at the current replay end; all_wf extends later but is a weaker evidence source and lacks the saved day-quality filtered replay.",
    }
    return audit, artifact_dates


def _direction2_audit(ohlc_dates: pd.DatetimeIndex) -> tuple[dict[str, object], pd.DataFrame, pd.DataFrame]:
    signals = pd.read_parquet(RARE_EVENT / "warrant_leads_stock_refine_signals.parquet")
    trades = pd.read_parquet(RARE_EVENT / "warrant_leads_stock_refine_trades.parquet")
    leaderboard = pd.read_csv(RARE_EVENT / "warrant_leads_stock_refine_leaderboard.csv")
    report_path = RARE_EVENT / "warrant_leads_stock_refine_report.md"

    for frame, col in [(signals, "date"), (trades, "signal_date"), (trades, "entry_date"), (trades, "exit_date")]:
        if col in frame.columns:
            frame[col] = pd.to_datetime(frame[col], errors="coerce")

    complete_signal_date = _latest_complete_signal_date(ohlc_dates, hold_days=40)
    if {"symbol", "signal_date"}.issubset(trades.columns) and "symbol" in signals.columns:
        saved_trade_keys = set(zip(trades["symbol"].astype(str), trades["signal_date"].dt.normalize()))
        signal_keys = list(zip(signals["symbol"].astype(str), signals["date"].dt.normalize()))
        signals["has_saved_full_trade"] = [key in saved_trade_keys for key in signal_keys]
    else:
        saved_trade_signal_dates = set(pd.to_datetime(trades["signal_date"]).dt.normalize()) if "signal_date" in trades.columns else set()
        signals["has_saved_full_trade"] = signals["date"].dt.normalize().isin(saved_trade_signal_dates)

    pending = signals[~signals["has_saved_full_trade"]].copy()
    completed = signals[signals["has_saved_full_trade"]].copy()
    post_complete_pending = pending[pending["date"] > complete_signal_date].copy() if complete_signal_date is not None else pending

    artifact_dates = pd.DataFrame(
        [
            _date_stats(RARE_EVENT / "features_stock_daily.parquet"),
            _date_stats(RARE_EVENT / "warrant_leads_stock_refine_signals.parquet"),
            _date_stats(RARE_EVENT / "warrant_leads_stock_refine_trades.parquet", "signal_date"),
            _date_stats(RARE_EVENT / "warrant_leads_stock_refine_leaderboard.csv"),
        ]
    )
    best = leaderboard.iloc[0]
    audit = {
        "feature_max_date": str(pd.read_parquet(RARE_EVENT / "features_stock_daily.parquet", columns=["date"])["date"].max().date()),
        "signal_max_date": str(signals["date"].max().date()),
        "saved_trade_signal_max_date": str(trades["signal_date"].max().date()),
        "saved_trade_exit_max_date": str(trades["exit_date"].max().date()),
        "last_complete_signal_date_from_ohlc": str(complete_signal_date.date()) if complete_signal_date is not None else None,
        "signals_total": int(len(signals)),
        "signals_with_saved_full_trade": int(len(completed)),
        "signals_without_saved_full_trade": int(len(pending)),
        "pending_after_complete_horizon": int(len(post_complete_pending)),
        "pending_signal_min_date": str(pending["date"].min().date()) if len(pending) else None,
        "pending_signal_max_date": str(pending["date"].max().date()) if len(pending) else None,
        "best_full_trades": int(best["full_trades"]),
        "best_full_win_rate": float(best["full_win_rate"]),
        "best_full_avg_net_ret": float(best["full_avg_net_ret"]),
        "best_test_trades": int(best["test_trades"]),
        "best_test_avg_net_ret": float(best["test_avg_net_ret"]),
        "report_path": str(report_path),
        "reason": "Direction 2 has newer signals, but the 40-trading-day hold/MFE horizon is not fully observable for signals after mid-December with OHLC ending 2026-02-26.",
    }
    pending_cols = [
        "symbol",
        "date",
        "event_name",
        "warrant_posnet_pct_cs",
        "warrant_dyn_k_pct_cs",
        "stock_posnet_pct_cs",
    ]
    pending_export = pending[[col for col in pending_cols if col in pending.columns]].copy()
    return audit, artifact_dates, pending_export


def _build_report(
    ohlc_dates: pd.DatetimeIndex,
    direction3: dict[str, object],
    direction3_artifacts: pd.DataFrame,
    direction2: dict[str, object],
    direction2_artifacts: pd.DataFrame,
) -> str:
    lines = [
        "# Strategy Forward Readiness Audit",
        "",
        "## Data Coverage",
        "",
        f"- OHLC min date: `{ohlc_dates.min().date()}`",
        f"- OHLC max date: `{ohlc_dates.max().date()}`",
        f"- OHLC unique trading dates: `{len(ohlc_dates)}`",
        "",
        "## Direction 3 Readiness",
        "",
        f"- trusted `breakout10_predictions_wf` max date: `{direction3['trusted_wf_max_date']}`",
        f"- `breakout10_predictions_all_wf` max date: `{direction3['all_wf_max_date']}`",
        f"- filtered strategy signal max date: `{direction3['filtered_signal_max_date']}`",
        f"- saved trade exit max date: `{direction3['saved_trade_exit_max_date']}`",
        f"- last complete 16-day signal date from OHLC: `{direction3['last_complete_signal_date_from_ohlc']}`",
        f"- can extend with trusted WF source now: `{direction3['can_extend_with_trusted_wf']}`",
        f"- can extend saved filtered strategy now: `{direction3['can_extend_filtered_strategy_now']}`",
        f"- current full trades / win / avg: `{direction3['full_trades']}` / `{direction3['full_win_rate']:.4f}` / `{direction3['full_avg_net_ret']:.4f}`",
        f"- reason: {direction3['reason']}",
        "",
        "### Direction 3 Artifact Dates",
    ]
    for _, row in direction3_artifacts.iterrows():
        lines.append(
            f"- `{Path(row['path']).name}`: `rows={int(row['rows'])}`, "
            f"`{row['date_col']}={row['min_date']}~{row['max_date']}`, `unique={int(row['unique_dates'])}`"
        )

    lines.extend(
        [
            "",
            "## Direction 2 Readiness",
            "",
            f"- feature max date: `{direction2['feature_max_date']}`",
            f"- signal max date: `{direction2['signal_max_date']}`",
            f"- saved full-trade signal max date: `{direction2['saved_trade_signal_max_date']}`",
            f"- saved trade exit max date: `{direction2['saved_trade_exit_max_date']}`",
            f"- last complete 40-day signal date from OHLC: `{direction2['last_complete_signal_date_from_ohlc']}`",
            f"- signals total: `{direction2['signals_total']}`",
            f"- signals with saved full trade: `{direction2['signals_with_saved_full_trade']}`",
            f"- signals without saved full trade: `{direction2['signals_without_saved_full_trade']}`",
            f"- pending signals after complete horizon: `{direction2['pending_after_complete_horizon']}`",
            f"- pending signal date range: `{direction2['pending_signal_min_date']}` to `{direction2['pending_signal_max_date']}`",
            "- pending signal export: `direction2_pending_signals.csv`",
            f"- best full trades / win / avg: `{direction2['best_full_trades']}` / `{direction2['best_full_win_rate']:.4f}` / `{direction2['best_full_avg_net_ret']:.4f}`",
            f"- best test trades / avg: `{direction2['best_test_trades']}` / `{direction2['best_test_avg_net_ret']:.4f}`",
            f"- reason: {direction2['reason']}",
            "",
            "### Direction 2 Artifact Dates",
        ]
    )
    for _, row in direction2_artifacts.iterrows():
        lines.append(
            f"- `{Path(row['path']).name}`: `rows={int(row['rows'])}`, "
            f"`{row['date_col']}={row['min_date']}~{row['max_date']}`, `unique={int(row['unique_dates'])}`"
        )

    lines.extend(
        [
            "",
            "## Conclusion",
            "",
            "- Current repo state is enough to audit and harden the saved target-clearing candidates, but not enough to claim a newer forward replay beyond the saved trusted windows.",
            "- Direction 3 needs refreshed trusted OOF/walk-forward breakout predictions after `2026-02-03` before the same rule can be extended cleanly.",
            "- Direction 2 already has later signals, but its 40-trading-day horizon makes the later signals pending until more OHLC data is available.",
            "- The next meaningful data-dependent step is to regenerate OOF predictions/features on later data, then replay the same fixed rules without changing thresholds.",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit forward-readiness of target-clearing strategy artifacts")
    ap.add_argument("--ohlc", type=Path, default=PROJECT_ROOT / "data" / "_derived" / "ohlc.parquet")
    ap.add_argument("--output-dir", type=Path, default=resolve_output("analysis", "strategy_forward_readiness"))
    args = ap.parse_args()

    out_dir = ensure_dir(args.output_dir)
    ohlc_dates = _load_ohlc_dates(args.ohlc)
    direction3, direction3_artifacts = _direction3_audit(ohlc_dates)
    direction2, direction2_artifacts, direction2_pending = _direction2_audit(ohlc_dates)

    direction3_artifacts.to_csv(out_dir / "direction3_artifact_date_coverage.csv", index=False)
    direction2_artifacts.to_csv(out_dir / "direction2_artifact_date_coverage.csv", index=False)
    direction2_pending.to_csv(out_dir / "direction2_pending_signals.csv", index=False)
    Path(out_dir / "strategy_forward_readiness_summary.json").write_text(
        json.dumps({"direction3": direction3, "direction2": direction2}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    report = _build_report(ohlc_dates, direction3, direction3_artifacts, direction2, direction2_artifacts)
    (out_dir / "strategy_forward_readiness_report.md").write_text(report, encoding="utf-8")


if __name__ == "__main__":
    main()
