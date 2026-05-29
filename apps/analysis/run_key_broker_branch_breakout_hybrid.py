#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Focused hybrid scan for the key-broker-branch strategy line.

The standalone branch scan is too noisy. This script starts from the strict
true branch-level signal artifact, then tests a small set of price/volume
context filters around the hypothesis:

    abnormal branch accumulation + price reclaiming prior highs + non-blowoff volume

It is intentionally a focused scan, not a broad parameter search.
"""

from __future__ import annotations

import argparse
import itertools
import json
import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from apps.analysis.run_key_broker_branch_scan import CostConfig  # noqa: E402
from src.stockanalysis.config import ensure_dir, resolve_data, resolve_output  # noqa: E402


DEFAULT_SIGNALS = resolve_output("analysis", "key_broker_branch", "best_key_broker_branch_signals.parquet")


def display_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)


def _profit_factor(values: pd.Series) -> float:
    pos = values[values > 0]
    neg = values[values < 0]
    gross_profit = float(pos.sum()) if not pos.empty else 0.0
    gross_loss = float((-neg).sum()) if not neg.empty else 0.0
    return gross_profit / gross_loss if gross_loss > 0 else math.inf


def trade_metrics(trades: pd.DataFrame) -> dict[str, float]:
    if trades.empty:
        return {
            "trades": 0,
            "win_rate": np.nan,
            "avg_net_ret": np.nan,
            "median_net_ret": np.nan,
            "profit_factor": np.nan,
            "avg_mfe": np.nan,
            "avg_mae": np.nan,
            "max_loss": np.nan,
        }
    return {
        "trades": int(len(trades)),
        "win_rate": float((trades["net_ret"] > 0).mean()),
        "avg_net_ret": float(trades["net_ret"].mean()),
        "median_net_ret": float(trades["net_ret"].median()),
        "profit_factor": _profit_factor(trades["net_ret"]),
        "avg_mfe": float(trades["mfe"].mean()),
        "avg_mae": float(trades["mae"].mean()),
        "max_loss": float(trades["net_ret"].min()),
    }


def target_pass(metrics: dict[str, float], min_trades: int) -> bool:
    return bool(metrics["trades"] >= min_trades and metrics["win_rate"] > 0.50 and metrics["avg_net_ret"] > 0.10)


def load_ohlc(path: Path) -> pd.DataFrame:
    ohlc = pd.read_parquet(path, columns=["symbol", "date", "open", "high", "low", "close", "volume"])
    ohlc["symbol"] = ohlc["symbol"].astype(str)
    ohlc["date"] = pd.to_datetime(ohlc["date"])
    ohlc = ohlc[ohlc["symbol"].str.len() == 4].sort_values(["symbol", "date"]).reset_index(drop=True)
    return ohlc


def add_price_context(signals: pd.DataFrame, ohlc: pd.DataFrame) -> pd.DataFrame:
    price = ohlc.copy()
    grouped = price.groupby("symbol", group_keys=False)
    price["turnover_proxy"] = price["close"] * price["volume"]
    price["ret_5d"] = grouped["close"].pct_change(5)
    price["ret_20d"] = grouped["close"].pct_change(20)
    price["abs_ret_20d"] = price["ret_20d"].abs()
    price["vol_ma5"] = grouped["volume"].transform(lambda s: s.rolling(5, min_periods=5).mean())
    price["vol_ma20"] = grouped["volume"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    price["volume_ratio_5_20"] = price["vol_ma5"] / price["vol_ma20"]
    price["high20_prev"] = grouped["high"].transform(lambda s: s.shift(1).rolling(20, min_periods=20).max())
    price["low20_prev"] = grouped["low"].transform(lambda s: s.shift(1).rolling(20, min_periods=20).min())
    price["price_pos_20"] = (price["close"] - price["low20_prev"]) / (price["high20_prev"] - price["low20_prev"])
    price["breakout_gap_20"] = price["close"] / price["high20_prev"] - 1.0

    keep = [
        "symbol",
        "date",
        "turnover_proxy",
        "ret_5d",
        "ret_20d",
        "abs_ret_20d",
        "volume_ratio_5_20",
        "price_pos_20",
        "breakout_gap_20",
    ]
    out = signals.merge(price[keep], on=["symbol", "date"], how="left")
    return out


def build_ohlc_maps(ohlc: pd.DataFrame) -> dict[str, dict[str, object]]:
    out: dict[str, dict[str, object]] = {}
    for symbol, sub in ohlc.groupby("symbol"):
        sub = sub.sort_values("date").reset_index(drop=True)
        dates = pd.DatetimeIndex(sub["date"])
        out[str(symbol)] = {
            "dates": dates.to_numpy(dtype="datetime64[ns]"),
            "open": sub["open"].to_numpy(float),
            "high": sub["high"].to_numpy(float),
            "low": sub["low"].to_numpy(float),
            "close": sub["close"].to_numpy(float),
            "date_to_idx": {pd.Timestamp(d): int(i) for i, d in enumerate(dates)},
        }
    return out


def simulate_trades(signals: pd.DataFrame, ohlc_maps: dict[str, dict[str, object]], hold_days: int, stop_loss: float | None) -> pd.DataFrame:
    cost = CostConfig()
    eff_fee = cost.fee_rate * cost.fee_discount
    rows: list[dict[str, object]] = []

    for _, signal in signals.iterrows():
        symbol = str(signal["symbol"])
        data = ohlc_maps.get(symbol)
        if data is None:
            continue
        i_signal = data["date_to_idx"].get(pd.Timestamp(signal["date"]))
        if i_signal is None:
            continue
        i_entry = int(i_signal) + 1
        i_exit = i_entry + hold_days
        i_mfe = i_entry + min(20, hold_days)
        if i_entry >= len(data["open"]) or i_exit >= len(data["close"]):
            continue

        entry_raw = float(data["open"][i_entry])
        exit_raw = float(data["close"][i_exit])
        if not np.isfinite(entry_raw) or entry_raw <= 0 or not np.isfinite(exit_raw):
            continue

        exit_reason = "TIME"
        if stop_loss is not None:
            stop_raw = entry_raw * (1.0 + stop_loss)
            for j in range(i_entry + 1, i_exit + 1):
                low_j = float(data["low"][j])
                if np.isfinite(low_j) and low_j <= stop_raw:
                    exit_raw = stop_raw
                    i_exit = j
                    exit_reason = "SL"
                    break

        buy_px = entry_raw * (1.0 + cost.slippage)
        sell_px = exit_raw * (1.0 - cost.slippage)
        net_ret = (sell_px * (1.0 - eff_fee - cost.tax_rate_sell)) / (buy_px * (1.0 + eff_fee)) - 1.0
        high = float(np.nanmax(data["high"][i_entry : i_mfe + 1]))
        low = float(np.nanmin(data["low"][i_entry : i_mfe + 1]))

        out = signal.to_dict()
        out.update(
            {
                "signal_date": pd.Timestamp(signal["date"]),
                "entry_date": pd.Timestamp(data["dates"][i_entry]),
                "exit_date": pd.Timestamp(data["dates"][i_exit]),
                "entry_px_raw": entry_raw,
                "exit_px_raw": exit_raw,
                "net_ret": float(net_ret),
                "mfe": float(high / buy_px - 1.0),
                "mae": float(low / buy_px - 1.0),
                "exit_reason": exit_reason,
            }
        )
        rows.append(out)
    return pd.DataFrame(rows)


def select_signals(signals: pd.DataFrame, spec: dict[str, object], score_thresholds: dict[float, float]) -> pd.DataFrame:
    mask = (
        (signals["score"] >= score_thresholds[float(spec["score_quantile"])])
        & (signals["price_pos_20"] >= float(spec["price_pos_20_floor"]))
        & (signals["breakout_gap_20"] >= float(spec["breakout_gap_20_floor"]))
        & (signals["volume_ratio_5_20"] >= float(spec["volume_ratio_5_20_floor"]))
        & (signals["volume_ratio_5_20"] <= float(spec["volume_ratio_5_20_cap"]))
        & (signals["abs_ret_20d"] <= float(spec["abs_ret_20d_cap"]))
        & (signals["ret_5d"] <= float(spec["ret_5d_cap"]))
    )
    return signals.loc[mask].copy()


def evaluate_spec(
    signals: pd.DataFrame,
    ohlc_maps: dict[str, dict[str, object]],
    spec: dict[str, object],
    score_thresholds: dict[float, float],
    min_trades: int,
) -> tuple[dict[str, object], pd.DataFrame, pd.DataFrame]:
    selected = select_signals(signals, spec, score_thresholds)
    trades = simulate_trades(selected, ohlc_maps, int(spec["hold_days"]), spec["stop_loss"])
    metrics = trade_metrics(trades)
    stability = stability_summary(trades, min_trades)
    row = {
        "target_pass": target_pass(metrics, min_trades),
        "signal_count": int(len(selected)),
        "params_json": json.dumps(spec, sort_keys=True),
        **spec,
        **metrics,
        **stability,
    }
    return row, selected, trades


def leave_one_table(trades: pd.DataFrame, group_col: str, min_trades: int) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    if trades.empty or group_col not in trades.columns:
        return pd.DataFrame()
    for value in sorted(trades[group_col].dropna().astype(str).unique()):
        sub = trades[trades[group_col].astype(str) != value].copy()
        metrics = trade_metrics(sub)
        rows.append(
            {
                "removed": value,
                "target_pass": target_pass(metrics, min_trades),
                **metrics,
            }
        )
    return pd.DataFrame(rows).sort_values(["target_pass", "avg_net_ret", "win_rate"], ascending=[True, True, True])


def month_leave_one(trades: pd.DataFrame, min_trades: int) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame()
    work = trades.copy()
    work["signal_month"] = pd.to_datetime(work["signal_date"]).dt.to_period("M").astype(str)
    return leave_one_table(work, "signal_month", min_trades)


def stability_summary(trades: pd.DataFrame, min_trades: int) -> dict[str, object]:
    checks = {
        "symbol": leave_one_table(trades, "symbol", min_trades),
        "month": month_leave_one(trades, min_trades),
        "broker": leave_one_table(trades, "broker", min_trades),
    }
    out: dict[str, object] = {}
    for name, table in checks.items():
        groups = int(len(table))
        pass_count = int(table["target_pass"].sum()) if not table.empty else 0
        out[f"leave_one_{name}_groups"] = groups
        out[f"leave_one_{name}_pass_count"] = pass_count
        out[f"leave_one_{name}_pass_ratio"] = float(pass_count / groups) if groups else np.nan
        out[f"leave_one_{name}_worst_avg_net_ret"] = float(table["avg_net_ret"].min()) if not table.empty else np.nan
        out[f"leave_one_{name}_min_trades"] = int(table["trades"].min()) if not table.empty else 0
    return out


def _fmt_float(value: object, digits: int = 4) -> str:
    try:
        value_f = float(value)
    except Exception:
        return str(value)
    if not np.isfinite(value_f):
        return "inf" if value_f > 0 else "-inf"
    return f"{value_f:.{digits}f}"


def build_report(
    baseline: dict[str, float],
    best: pd.Series | None,
    leaderboard: pd.DataFrame,
    trades: pd.DataFrame,
    loo_symbol: pd.DataFrame,
    loo_month: pd.DataFrame,
    loo_broker: pd.DataFrame,
    args: argparse.Namespace,
) -> str:
    passing = leaderboard[leaderboard["target_pass"]].copy() if not leaderboard.empty else pd.DataFrame()
    if passing.empty:
        best_passing_month = 0
        best_passing_month_groups = 0
    else:
        idx = passing["leave_one_month_pass_count"].idxmax()
        best_passing_month = int(passing.loc[idx, "leave_one_month_pass_count"])
        best_passing_month_groups = int(passing.loc[idx, "leave_one_month_groups"])

    lines = [
        "# Key Broker Branch Breakout Hybrid Scan",
        "",
        "## Scope",
        "",
        f"- base signals: `{display_path(args.signals)}`",
        f"- OHLC: `{display_path(args.ohlc)}`",
        f"- min target gate: `trades >= {args.min_trades}`, `win_rate > 0.50`, `avg_net_ret > 0.10`",
        f"- scanned candidates: `{len(leaderboard)}`",
        f"- passing candidates: `{int(leaderboard['target_pass'].sum()) if not leaderboard.empty else 0}`",
        f"- best leave-one-month pass among passing candidates: `{best_passing_month} / {best_passing_month_groups}`",
        "",
        "## Baseline",
        "",
        "- baseline is the strict branch-level signal set with `hold_days=20` and no additional price/volume context.",
        f"- trades: `{baseline['trades']}`",
        f"- win rate: `{_fmt_float(baseline['win_rate'])}`",
        f"- average net return: `{_fmt_float(baseline['avg_net_ret'])}`",
        f"- median net return: `{_fmt_float(baseline['median_net_ret'])}`",
        "",
    ]
    if best is None:
        lines.append("No hybrid candidate produced trades.")
        return "\n".join(lines)

    lines.extend(
        [
            "## Best Hybrid Candidate",
            "",
            f"- target pass: `{bool(best['target_pass'])}`",
            f"- params: `{best['params_json']}`",
            f"- signals: `{int(best['signal_count'])}`",
            f"- trades: `{int(best['trades'])}`",
            f"- win rate: `{_fmt_float(best['win_rate'])}`",
            f"- average net return: `{_fmt_float(best['avg_net_ret'])}`",
            f"- median net return: `{_fmt_float(best['median_net_ret'])}`",
            f"- profit factor: `{_fmt_float(best['profit_factor'])}`",
            f"- average MFE / MAE: `{_fmt_float(best['avg_mfe'])}` / `{_fmt_float(best['avg_mae'])}`",
            f"- max loss: `{_fmt_float(best['max_loss'])}`",
            f"- leaderboard leave-one-month pass: `{int(best['leave_one_month_pass_count'])} / {int(best['leave_one_month_groups'])}`",
            f"- leaderboard leave-one-symbol pass: `{int(best['leave_one_symbol_pass_count'])} / {int(best['leave_one_symbol_groups'])}`",
            f"- leaderboard leave-one-broker pass: `{int(best['leave_one_broker_pass_count'])} / {int(best['leave_one_broker_groups'])}`",
            "",
            "Interpretation:",
            "",
            "- Strict branch-level abnormal buying is not enough by itself.",
            "- The useful pattern is a much narrower hybrid: strongest branch anomaly score plus the stock reclaiming prior 20-day highs, with volume expansion capped to avoid blowoff entries.",
            "- Leaderboard ranking is now stability-aware, so target-passing rows with better leave-one-month behavior outrank higher-return but more fragile rows.",
            "- In this focused scan, no target-passing row improves beyond the same weak leave-one-month result, so the month/regime weakness remains.",
            "",
            "## Robustness",
            "",
            f"- leave-one-symbol pass: `{int(loo_symbol['target_pass'].sum()) if not loo_symbol.empty else 0} / {len(loo_symbol)}`",
            f"- leave-one-month pass: `{int(loo_month['target_pass'].sum()) if not loo_month.empty else 0} / {len(loo_month)}`",
            f"- leave-one-broker pass: `{int(loo_broker['target_pass'].sum()) if not loo_broker.empty else 0} / {len(loo_broker)}`",
            "",
        ]
    )

    if not trades.empty:
        top = trades.sort_values("net_ret", ascending=False).head(5)
        worst = trades.sort_values("net_ret").head(5)
        lines.append("## Best Trades")
        for _, row in top.iterrows():
            lines.append(
                f"- `{pd.Timestamp(row['signal_date']).date()} {row['symbol']} {row.get('broker_name') or row.get('broker')}`: "
                f"`net_ret={row['net_ret']:.4f}`, `mfe={row['mfe']:.4f}`, `mae={row['mae']:.4f}`"
            )
        lines.extend(["", "## Worst Trades"])
        for _, row in worst.iterrows():
            lines.append(
                f"- `{pd.Timestamp(row['signal_date']).date()} {row['symbol']} {row.get('broker_name') or row.get('broker')}`: "
                f"`net_ret={row['net_ret']:.4f}`, `mfe={row['mfe']:.4f}`, `mae={row['mae']:.4f}`"
            )
        lines.extend(["", "## Concentration"])
        for col, label in [("symbol", "symbol"), ("broker", "broker")]:
            counts = trades[col].astype(str).value_counts().head(8)
            for key, value in counts.items():
                lines.append(f"- {label} `{key}`: `{value}` trades")
        lines.append("")

    lines.append("## Top Leaderboard Rows")
    for _, row in leaderboard.head(10).iterrows():
        lines.append(
            f"- `{row['params_json']}`: `pass={row['target_pass']}`, "
            f"`trades={int(row['trades'])}`, `win={row['win_rate']:.4f}`, `avg={row['avg_net_ret']:.4f}`, "
            f"`loo_month={int(row['leave_one_month_pass_count'])}/{int(row['leave_one_month_groups'])}`"
        )
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description="Focused key-broker branch + breakout context scan")
    ap.add_argument("--signals", type=Path, default=DEFAULT_SIGNALS)
    ap.add_argument("--ohlc", type=Path, default=resolve_data("_derived", "ohlc.parquet"))
    ap.add_argument("--output-dir", type=Path, default=resolve_output("analysis", "key_broker_branch_hybrid"))
    ap.add_argument("--min-trades", type=int, default=20)
    args = ap.parse_args()

    signals = pd.read_parquet(args.signals).reset_index(drop=True)
    signals["symbol"] = signals["symbol"].astype(str)
    signals["date"] = pd.to_datetime(signals["date"])
    ohlc = load_ohlc(args.ohlc)
    signals = add_price_context(signals, ohlc)
    ohlc_maps = build_ohlc_maps(ohlc)

    score_quantiles = [0.75, 0.90]
    score_thresholds = {q: float(signals["score"].quantile(q)) for q in score_quantiles}

    specs: list[dict[str, object]] = []
    for values in itertools.product(
        [16, 20, 30],
        [None, -0.10],
        score_quantiles,
        [0.50, 0.85],
        [-0.03, 0.00, 0.02],
        [0.50, 0.80, 1.00],
        [2.00, 3.00],
        [0.35, 10.00],
        [0.30, 10.00],
    ):
        specs.append(
            {
                "hold_days": values[0],
                "stop_loss": values[1],
                "score_quantile": values[2],
                "price_pos_20_floor": values[3],
                "breakout_gap_20_floor": values[4],
                "volume_ratio_5_20_floor": values[5],
                "volume_ratio_5_20_cap": values[6],
                "abs_ret_20d_cap": values[7],
                "ret_5d_cap": values[8],
            }
        )

    baseline_trades = simulate_trades(signals, ohlc_maps, hold_days=20, stop_loss=None)
    baseline = trade_metrics(baseline_trades)

    rows: list[dict[str, object]] = []
    best_row: pd.Series | None = None
    best_signals = pd.DataFrame()
    best_trades = pd.DataFrame()
    for spec in specs:
        row, selected, trades = evaluate_spec(signals, ohlc_maps, spec, score_thresholds, args.min_trades)
        rows.append(row)
        score = (
            bool(row["target_pass"]),
            int(row["leave_one_month_pass_count"]),
            int(row["leave_one_symbol_pass_count"]),
            int(row["leave_one_broker_pass_count"]),
            float(row["avg_net_ret"]) if pd.notna(row["avg_net_ret"]) else -np.inf,
            float(row["win_rate"]) if pd.notna(row["win_rate"]) else -np.inf,
            int(row["trades"]),
        )
        if best_row is None:
            best_row = pd.Series(row)
            best_signals = selected
            best_trades = trades
            continue
        best_score = (
            bool(best_row["target_pass"]),
            int(best_row["leave_one_month_pass_count"]),
            int(best_row["leave_one_symbol_pass_count"]),
            int(best_row["leave_one_broker_pass_count"]),
            float(best_row["avg_net_ret"]) if pd.notna(best_row["avg_net_ret"]) else -np.inf,
            float(best_row["win_rate"]) if pd.notna(best_row["win_rate"]) else -np.inf,
            int(best_row["trades"]),
        )
        if score > best_score:
            best_row = pd.Series(row)
            best_signals = selected
            best_trades = trades

    leaderboard = pd.DataFrame(rows)
    leaderboard = leaderboard.sort_values(
        [
            "target_pass",
            "leave_one_month_pass_count",
            "leave_one_symbol_pass_count",
            "leave_one_broker_pass_count",
            "avg_net_ret",
            "win_rate",
            "trades",
            "profit_factor",
        ],
        ascending=[False, False, False, False, False, False, False, False],
    ).reset_index(drop=True)

    loo_symbol = leave_one_table(best_trades, "symbol", args.min_trades)
    loo_month = month_leave_one(best_trades, args.min_trades)
    loo_broker = leave_one_table(best_trades, "broker", args.min_trades)

    out_dir = ensure_dir(args.output_dir)
    leaderboard.to_csv(out_dir / "key_broker_branch_hybrid_leaderboard.csv", index=False)
    best_signals.to_parquet(out_dir / "key_broker_branch_hybrid_signals.parquet", index=False)
    best_trades.to_csv(out_dir / "key_broker_branch_hybrid_trades.csv", index=False)
    loo_symbol.to_csv(out_dir / "key_broker_branch_hybrid_leave_one_symbol_out.csv", index=False)
    loo_month.to_csv(out_dir / "key_broker_branch_hybrid_leave_one_month_out.csv", index=False)
    loo_broker.to_csv(out_dir / "key_broker_branch_hybrid_leave_one_broker_out.csv", index=False)

    summary = {
        "baseline": baseline,
        "best": best_row.to_dict() if best_row is not None else None,
        "score_thresholds": score_thresholds,
        "artifacts": {
            "leaderboard": display_path(out_dir / "key_broker_branch_hybrid_leaderboard.csv"),
            "signals": display_path(out_dir / "key_broker_branch_hybrid_signals.parquet"),
            "trades": display_path(out_dir / "key_broker_branch_hybrid_trades.csv"),
            "report": display_path(out_dir / "key_broker_branch_hybrid_report.md"),
        },
    }
    (out_dir / "key_broker_branch_hybrid_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    report = build_report(baseline, best_row, leaderboard, best_trades, loo_symbol, loo_month, loo_broker, args)
    (out_dir / "key_broker_branch_hybrid_report.md").write_text(report, encoding="utf-8")
    print(report)


if __name__ == "__main__":
    main()
