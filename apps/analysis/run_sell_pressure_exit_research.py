#!/usr/bin/env python3
"""Research out-of-sample exit policies for sell-pressure absorption signals."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SIGNALS = PROJECT_ROOT / "outputs/analysis/raw_broker_microstructure/sell_pressure_absorption_best_signals.csv"
DEFAULT_OHLC = PROJECT_ROOT / "data/_derived/ohlc.parquet"
DEFAULT_OUT = PROJECT_ROOT / "outputs/analysis/sell_pressure_exit_research"


@dataclass(frozen=True)
class CostConfig:
    fee_rate: float = 0.001425
    fee_discount: float = 0.28
    tax_rate_sell: float = 0.003
    slippage: float = 0.0005


@dataclass(frozen=True)
class ExitPolicy:
    policy_type: str
    max_hold: int
    stop_loss: float | None = None
    trail_pct: float | None = None
    activation_ret: float | None = None
    ma_window: int | None = None
    min_hold: int = 1

    @property
    def policy_id(self) -> str:
        parts = [self.policy_type, f"h{self.max_hold}"]
        if self.stop_loss is not None:
            parts.append(f"sl{abs(self.stop_loss):.0%}")
        if self.trail_pct is not None:
            parts.extend([f"trail{self.trail_pct:.0%}", f"act{self.activation_ret:.0%}"])
        if self.ma_window is not None:
            parts.extend([f"ma{self.ma_window}", f"min{self.min_hold}"])
        return "_".join(parts)


def load_inputs(signals_path: Path, ohlc_path: Path, max_hold: int) -> tuple[pd.DataFrame, dict[str, dict[str, Any]]]:
    signals = pd.read_csv(signals_path, dtype={"symbol": str})
    required = {"symbol", "date"}
    if not required.issubset(signals.columns):
        raise ValueError(f"signals missing columns: {sorted(required - set(signals.columns))}")
    signals["date"] = pd.to_datetime(signals["date"])
    signals = signals.sort_values(["date", "symbol"]).reset_index(drop=True)

    cols = ["symbol", "date", "open", "high", "low", "close"]
    ohlc = pd.read_parquet(ohlc_path, columns=cols)
    ohlc["symbol"] = ohlc["symbol"].astype(str)
    ohlc = ohlc[ohlc["symbol"].isin(signals["symbol"].unique())].copy()
    ohlc["date"] = pd.to_datetime(ohlc["date"])
    for col in ["open", "high", "low", "close"]:
        ohlc[col] = pd.to_numeric(ohlc[col], errors="coerce")
    ohlc = ohlc.dropna(subset=["open", "high", "low", "close"])
    if ohlc.duplicated(["symbol", "date"]).any():
        raise ValueError("OHLC contains duplicate symbol/date rows")

    maps: dict[str, dict[str, Any]] = {}
    for symbol, sub in ohlc.groupby("symbol", sort=False):
        sub = sub.sort_values("date").reset_index(drop=True)
        dates = sub["date"].to_numpy(dtype="datetime64[ns]")
        maps[symbol] = {
            "dates": dates,
            "open": sub["open"].to_numpy(float),
            "high": sub["high"].to_numpy(float),
            "low": sub["low"].to_numpy(float),
            "close": sub["close"].to_numpy(float),
            "date_to_idx": {pd.Timestamp(date): i for i, date in enumerate(dates)},
        }

    complete_rows = []
    for row in signals.itertuples(index=False):
        data = maps.get(row.symbol)
        if data is None:
            continue
        signal_idx = data["date_to_idx"].get(pd.Timestamp(row.date))
        if signal_idx is None:
            continue
        entry_idx = signal_idx + 1
        if entry_idx + max_hold >= len(data["close"]):
            continue
        complete_rows.append(row._asdict())
    complete = pd.DataFrame(complete_rows)
    if complete.empty:
        raise RuntimeError("no signals have a complete forward horizon")
    complete["date"] = pd.to_datetime(complete["date"])
    return complete, maps


def net_return(entry_raw: float, exit_raw: float, cost: CostConfig) -> float:
    fee = cost.fee_rate * cost.fee_discount
    buy_px = entry_raw * (1.0 + cost.slippage)
    sell_px = exit_raw * (1.0 - cost.slippage)
    return float((sell_px * (1.0 - fee - cost.tax_rate_sell)) / (buy_px * (1.0 + fee)) - 1.0)


def stop_fill(open_px: float, stop_px: float) -> float:
    return float(open_px if open_px <= stop_px else stop_px)


def simulate_policy(
    signals: pd.DataFrame,
    ohlc_maps: dict[str, dict[str, Any]],
    policy: ExitPolicy,
    cost: CostConfig,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for signal in signals.itertuples(index=False):
        data = ohlc_maps[signal.symbol]
        signal_idx = data["date_to_idx"][pd.Timestamp(signal.date)]
        entry_idx = signal_idx + 1
        last_idx = entry_idx + policy.max_hold
        entry_raw = float(data["open"][entry_idx])
        if not np.isfinite(entry_raw) or entry_raw <= 0:
            continue

        exit_idx = last_idx
        exit_raw = float(data["close"][last_idx])
        exit_reason = "TIME"
        peak_high = float(data["high"][entry_idx])

        for idx in range(entry_idx + 1, last_idx + 1):
            elapsed = idx - entry_idx
            open_px = float(data["open"][idx])
            low_px = float(data["low"][idx])
            close_px = float(data["close"][idx])

            if policy.stop_loss is not None:
                stop_px = entry_raw * (1.0 + policy.stop_loss)
                if low_px <= stop_px:
                    exit_idx, exit_raw, exit_reason = idx, stop_fill(open_px, stop_px), "SL"
                    break

            if policy.trail_pct is not None and policy.activation_ret is not None:
                activated = peak_high >= entry_raw * (1.0 + policy.activation_ret)
                trail_px = peak_high * (1.0 - policy.trail_pct)
                if activated and low_px <= trail_px:
                    exit_idx, exit_raw, exit_reason = idx, stop_fill(open_px, trail_px), "TRAIL"
                    break

            if policy.ma_window is not None and elapsed >= policy.min_hold:
                ma_start = idx - policy.ma_window + 1
                if ma_start >= 0:
                    moving_average = float(np.mean(data["close"][ma_start : idx + 1]))
                    if close_px < moving_average:
                        exit_idx, exit_raw, exit_reason = idx, close_px, "MA"
                        break

            # The current high becomes usable only for the next day's trailing stop.
            peak_high = max(peak_high, float(data["high"][idx]))

        rows.append(
            {
                "symbol": signal.symbol,
                "signal_date": pd.Timestamp(signal.date),
                "entry_date": pd.Timestamp(data["dates"][entry_idx]),
                "exit_date": pd.Timestamp(data["dates"][exit_idx]),
                "entry_raw": entry_raw,
                "exit_raw": exit_raw,
                "hold_days": exit_idx - entry_idx,
                "net_ret": net_return(entry_raw, exit_raw, cost),
                "exit_reason": exit_reason,
                "policy_id": policy.policy_id,
            }
        )
    return pd.DataFrame(rows)


def policy_grid() -> list[ExitPolicy]:
    policies = [ExitPolicy("fixed", hold) for hold in [10, 15, 20, 30, 40, 50, 60]]
    policies.extend(
        ExitPolicy("stop_time", hold, stop_loss=stop)
        for hold in [20, 30, 40, 60]
        for stop in [-0.10, -0.15, -0.20]
    )
    policies.extend(
        ExitPolicy("trailing", hold, stop_loss=stop, trail_pct=trail, activation_ret=activation)
        for hold in [30, 40, 60]
        for trail in [0.08, 0.12, 0.16, 0.20]
        for activation in [0.10, 0.20]
        for stop in [None, -0.15]
    )
    policies.extend(
        ExitPolicy("moving_average", hold, stop_loss=stop, ma_window=window, min_hold=min_hold)
        for hold in [30, 40, 60]
        for window in [5, 10, 20]
        for min_hold in [5, 10]
        for stop in [None, -0.15]
    )
    return policies


def trade_metrics(trades: pd.DataFrame) -> dict[str, float | int]:
    returns = trades["net_ret"].astype(float)
    if returns.empty:
        return {"trades": 0}
    trim_cutoff = returns.quantile(0.95)
    trimmed = returns[returns <= trim_cutoff]
    losses = -returns[returns < 0].sum()
    profit_factor = returns[returns > 0].sum() / losses if losses > 0 else np.inf
    avg_hold = float(trades["hold_days"].mean())
    return {
        "trades": int(len(returns)),
        "win_rate": float((returns > 0).mean()),
        "avg_net_ret": float(returns.mean()),
        "median_net_ret": float(returns.median()),
        "trimmed_avg_net_ret": float(trimmed.mean()),
        "max_loss": float(returns.min()),
        "profit_factor": float(profit_factor),
        "avg_hold_days": avg_hold,
        "median_hold_days": float(trades["hold_days"].median()),
        "efficiency_20d": float(trimmed.mean() * 20.0 / avg_hold),
    }


def build_forward_path(
    signals: pd.DataFrame, ohlc_maps: dict[str, dict[str, Any]], cost: CostConfig, horizon: int
) -> pd.DataFrame:
    rows = []
    for signal in signals.itertuples(index=False):
        data = ohlc_maps[signal.symbol]
        entry_idx = data["date_to_idx"][pd.Timestamp(signal.date)] + 1
        entry_raw = float(data["open"][entry_idx])
        peak = float(data["high"][entry_idx])
        trough = float(data["low"][entry_idx])
        for day in range(1, horizon + 1):
            idx = entry_idx + day
            peak = max(peak, float(data["high"][idx]))
            trough = min(trough, float(data["low"][idx]))
            rows.append(
                {
                    "day": day,
                    "net_ret": net_return(entry_raw, float(data["close"][idx]), cost),
                    "mfe": peak / entry_raw - 1.0,
                    "mae": trough / entry_raw - 1.0,
                }
            )
    paths = pd.DataFrame(rows)
    return paths.groupby("day", as_index=False).agg(
        trades=("net_ret", "size"),
        win_rate=("net_ret", lambda values: float((values > 0).mean())),
        avg_net_ret=("net_ret", "mean"),
        median_net_ret=("net_ret", "median"),
        q25_net_ret=("net_ret", lambda values: values.quantile(0.25)),
        q75_net_ret=("net_ret", lambda values: values.quantile(0.75)),
        avg_mfe=("mfe", "mean"),
        avg_mae=("mae", "mean"),
    )


def evaluate_policies(
    signals: pd.DataFrame, ohlc_maps: dict[str, dict[str, Any]], policies: list[ExitPolicy], cost: CostConfig
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    metrics_rows = []
    trades_by_policy = {}
    for policy in policies:
        trades = simulate_policy(signals, ohlc_maps, policy, cost)
        trades_by_policy[policy.policy_id] = trades
        metrics_rows.append({"policy_id": policy.policy_id, **asdict(policy), **trade_metrics(trades)})
    leaderboard = pd.DataFrame(metrics_rows).sort_values(
        ["efficiency_20d", "median_net_ret", "avg_net_ret"], ascending=False
    )
    return leaderboard.reset_index(drop=True), trades_by_policy


def choose_policy(train_metrics: pd.DataFrame) -> pd.Series:
    eligible = train_metrics[
        (train_metrics["trades"] >= 50)
        & (train_metrics["win_rate"] >= 0.55)
        & (train_metrics["trimmed_avg_net_ret"] > 0)
        & (train_metrics["max_loss"] >= -0.40)
    ]
    pool = eligible if not eligible.empty else train_metrics
    return pool.sort_values(["efficiency_20d", "median_net_ret", "avg_net_ret"], ascending=False).iloc[0]


def run_walk_forward(
    trades_by_policy: dict[str, pd.DataFrame], leaderboard: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    folds = [
        ("2025-10-01", "2025-11-30"),
        ("2025-12-01", "2026-01-31"),
        ("2026-02-01", "2026-03-31"),
    ]
    fold_rows = []
    test_parts = []
    for fold_id, (test_start_raw, test_end_raw) in enumerate(folds, start=1):
        test_start, test_end = pd.Timestamp(test_start_raw), pd.Timestamp(test_end_raw)
        train_rows = []
        for policy_id, trades in trades_by_policy.items():
            # Purge trades whose outcome was not known when this fold began.
            train = trades[(trades["signal_date"] < test_start) & (trades["exit_date"] < test_start)]
            train_rows.append({"policy_id": policy_id, **trade_metrics(train)})
        selected = choose_policy(pd.DataFrame(train_rows))
        policy_id = str(selected["policy_id"])
        test = trades_by_policy[policy_id]
        test = test[(test["signal_date"] >= test_start) & (test["signal_date"] <= test_end)].copy()
        baseline = trades_by_policy["fixed_h60"]
        baseline = baseline[(baseline["signal_date"] >= test_start) & (baseline["signal_date"] <= test_end)]
        selected_metrics = trade_metrics(test)
        baseline_metrics = trade_metrics(baseline)
        policy_spec = leaderboard.loc[leaderboard["policy_id"] == policy_id].iloc[0]
        fold_rows.append(
            {
                "fold": fold_id,
                "test_start": test_start,
                "test_end": test_end,
                "selected_policy": policy_id,
                "selected_policy_type": policy_spec["policy_type"],
                "train_trades": int(selected["trades"]),
                "train_efficiency_20d": selected["efficiency_20d"],
                **{f"test_{key}": value for key, value in selected_metrics.items()},
                **{f"baseline_{key}": value for key, value in baseline_metrics.items()},
            }
        )
        test["fold"] = fold_id
        test_parts.append(test)
    return pd.DataFrame(fold_rows), pd.concat(test_parts, ignore_index=True)


def format_pct(value: float) -> str:
    return "n/a" if pd.isna(value) else f"{value:.2%}"


def write_report(
    out_path: Path,
    signals: pd.DataFrame,
    leaderboard: pd.DataFrame,
    folds: pd.DataFrame,
    wf_trades: pd.DataFrame,
    trades_by_policy: dict[str, pd.DataFrame],
    extended_path: pd.DataFrame,
) -> None:
    baseline = trade_metrics(trades_by_policy["fixed_h60"])
    wf_metrics = trade_metrics(wf_trades)
    baseline_parts = []
    for row in folds.itertuples(index=False):
        baseline = trades_by_policy["fixed_h60"]
        mask = (baseline["signal_date"] >= row.test_start) & (baseline["signal_date"] <= row.test_end)
        baseline_parts.append(baseline[mask])
    wf_baseline = trade_metrics(pd.concat(baseline_parts, ignore_index=True))
    risk_controlled_parts = []
    for row in folds.itertuples(index=False):
        risk_controlled = trades_by_policy["stop_time_h60_sl20%"]
        mask = (risk_controlled["signal_date"] >= row.test_start) & (risk_controlled["signal_date"] <= row.test_end)
        risk_controlled_parts.append(risk_controlled[mask])
    wf_risk_controlled = trade_metrics(pd.concat(risk_controlled_parts, ignore_index=True))
    top = leaderboard.head(10).copy()
    extended_best = extended_path.sort_values(["avg_net_ret", "median_net_ret"], ascending=False).iloc[0]

    lines = [
        "# 賣壓吸收策略出場時機研究",
        "",
        "## 研究設計",
        "",
        f"- 訊號數：{len(signals)}，僅使用具完整 60 交易日資料的同一批訊號。",
        "- 進場：訊號次一交易日開盤；報酬已扣手續費、交易稅與滑價。",
        "- 比較：固定持有、硬停損、啟動後移動停利、收盤跌破移動平均。",
        "- 選擇標準：先通過交易數、勝率與最大虧損門檻，再最大化去除前 5% 極端獲利後的每 20 日效率。",
        "- Walk-forward 訓練只納入測試開始前已完成出場的交易，避免未來資訊洩漏。",
        "",
        "## Walk-forward 結果",
        "",
        f"- 動態出場：{wf_metrics['trades']} 筆，平均 {format_pct(wf_metrics['avg_net_ret'])}，中位數 {format_pct(wf_metrics['median_net_ret'])}，勝率 {format_pct(wf_metrics['win_rate'])}，平均持有 {wf_metrics['avg_hold_days']:.1f} 日。",
        f"- 固定 60 日基準：{wf_baseline['trades']} 筆，平均 {format_pct(wf_baseline['avg_net_ret'])}，中位數 {format_pct(wf_baseline['median_net_ret'])}，勝率 {format_pct(wf_baseline['win_rate'])}，平均持有 {wf_baseline['avg_hold_days']:.1f} 日。",
        f"- 60 日加 -20% 硬停損：{wf_risk_controlled['trades']} 筆，平均 {format_pct(wf_risk_controlled['avg_net_ret'])}，中位數 {format_pct(wf_risk_controlled['median_net_ret'])}，最大虧損 {format_pct(wf_risk_controlled['max_loss'])}。",
        "",
        "| Fold | 測試期間 | 訓練選出的出場 | 測試筆數 | 平均報酬 | 中位數 | 勝率 | 平均持有 | 60 日平均 |",
        "|---:|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in folds.itertuples(index=False):
        lines.append(
            f"| {row.fold} | {row.test_start:%Y-%m-%d} ~ {row.test_end:%Y-%m-%d} | `{row.selected_policy}` | "
            f"{row.test_trades} | {format_pct(row.test_avg_net_ret)} | {format_pct(row.test_median_net_ret)} | "
            f"{format_pct(row.test_win_rate)} | {row.test_avg_hold_days:.1f} | {format_pct(row.baseline_avg_net_ret)} |"
        )
    lines.extend(
        [
            "",
            "## 固定持有時間是否已找到高點",
            "",
            f"使用另外 {int(extended_path.iloc[0]['trades'])} 筆具完整 120 交易日資料的共同樣本，平均報酬最高仍是第 {int(extended_best['day'])} 日（{format_pct(extended_best['avg_net_ret'])}）。",
            "這表示目前只能確認原本的 60 日是搜尋上限造成的結果；在現有資料內，尚未觀察到固定持有報酬的明確反轉點。",
            "",
            "| 持有日 | 平均報酬 | 中位數 | 勝率 | 平均 MFE | 平均 MAE |",
            "|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for day in [20, 40, 60, 75, 90, 105, 120]:
        row = extended_path.loc[extended_path["day"] == day].iloc[0]
        lines.append(
            f"| {day} | {format_pct(row.avg_net_ret)} | {format_pct(row.median_net_ret)} | "
            f"{format_pct(row.win_rate)} | {format_pct(row.avg_mfe)} | {format_pct(row.avg_mae)} |"
        )
    lines.extend(
        [
            "",
            "## 全樣本前十名",
            "",
            "全樣本排名僅供解釋，不應直接作為正式參數選擇依據。",
            "",
            "| 出場規則 | 平均報酬 | 中位數 | 勝率 | 最大虧損 | 平均持有 | 20 日效率 |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in top.itertuples(index=False):
        lines.append(
            f"| `{row.policy_id}` | {format_pct(row.avg_net_ret)} | {format_pct(row.median_net_ret)} | "
            f"{format_pct(row.win_rate)} | {format_pct(row.max_loss)} | {row.avg_hold_days:.1f} | {format_pct(row.efficiency_20d)} |"
        )
    lines.extend(
        [
            "",
            "## 解讀限制",
            "",
            "- 日線無法得知盤中高低價先後；移動停利只使用前一日以前的峰值，採保守處理。",
            "- 訊號可能重疊，本研究比較單筆交易品質，不等同完整資金配置回測。",
            "- 歷史區間仍偏短；正式採用前應持續累積新的 forward 樣本。",
            "",
        ]
    )
    out_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--signals", type=Path, default=DEFAULT_SIGNALS)
    parser.add_argument("--ohlc", type=Path, default=DEFAULT_OHLC)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    cost = CostConfig()
    signals, ohlc_maps = load_inputs(args.signals, args.ohlc, max_hold=60)
    extended_signals, _ = load_inputs(args.signals, args.ohlc, max_hold=120)
    policies = policy_grid()
    path_summary = build_forward_path(signals, ohlc_maps, cost, horizon=60)
    extended_path = build_forward_path(extended_signals, ohlc_maps, cost, horizon=120)
    leaderboard, trades_by_policy = evaluate_policies(signals, ohlc_maps, policies, cost)
    folds, wf_trades = run_walk_forward(trades_by_policy, leaderboard)

    path_summary.to_csv(args.out_dir / "forward_path_summary.csv", index=False)
    extended_path.to_csv(args.out_dir / "extended_120d_forward_path_summary.csv", index=False)
    leaderboard.to_csv(args.out_dir / "exit_policy_leaderboard.csv", index=False)
    folds.to_csv(args.out_dir / "walk_forward_folds.csv", index=False)
    wf_trades.to_csv(args.out_dir / "walk_forward_trades.csv", index=False)
    best_full_sample = str(leaderboard.iloc[0]["policy_id"])
    trades_by_policy[best_full_sample].to_csv(args.out_dir / "best_full_sample_policy_trades.csv", index=False)
    write_report(
        args.out_dir / "sell_pressure_exit_research_report.md",
        signals,
        leaderboard,
        folds,
        wf_trades,
        trades_by_policy,
        extended_path,
    )
    summary = {
        "complete_signals": len(signals),
        "candidate_policies": len(policies),
        "best_full_sample_policy": best_full_sample,
        "walk_forward_metrics": trade_metrics(wf_trades),
    }
    (args.out_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
