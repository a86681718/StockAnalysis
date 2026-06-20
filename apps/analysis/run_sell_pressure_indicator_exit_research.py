#!/usr/bin/env python3
"""Evaluate indicator-driven exits for sell-pressure absorption signals."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from run_sell_pressure_exit_research import CostConfig, net_return


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SIGNALS = PROJECT_ROOT / "outputs/analysis/raw_broker_microstructure/sell_pressure_absorption_best_signals.csv"
DEFAULT_FEATURES = PROJECT_ROOT / "outputs/analysis/raw_broker_microstructure/raw_micro_features.parquet"
DEFAULT_OUT = PROJECT_ROOT / "outputs/analysis/sell_pressure_indicator_exit_research"


@dataclass(frozen=True)
class IndicatorPolicy:
    policy_type: str
    atr_multiple: float | None = None
    ema_span: int | None = None
    balance_threshold: float | None = None
    breadth_threshold: float | None = None

    @property
    def policy_id(self) -> str:
        parts = [self.policy_type]
        if self.atr_multiple is not None:
            parts.append(f"atr{self.atr_multiple:g}")
        if self.ema_span is not None:
            parts.append(f"ema{self.ema_span}")
        if self.balance_threshold is not None:
            parts.append(f"balance{self.balance_threshold:g}")
        if self.breadth_threshold is not None:
            parts.append(f"breadth{self.breadth_threshold:g}")
        return "_".join(parts)


def load_inputs(signals_path: Path, features_path: Path) -> tuple[pd.DataFrame, dict[str, dict[str, Any]]]:
    signals = pd.read_csv(signals_path, dtype={"symbol": str})
    signals["date"] = pd.to_datetime(signals["date"])

    columns = [
        "symbol",
        "date",
        "open",
        "high",
        "low",
        "close",
        "pos_neg_balance",
        "buyer_count_pct_cs",
    ]
    features = pd.read_parquet(features_path, columns=columns)
    features["symbol"] = features["symbol"].astype(str)
    features = features[features["symbol"].isin(signals["symbol"].unique())].copy()
    features["date"] = pd.to_datetime(features["date"])
    features = features.sort_values(["symbol", "date"])

    grouped = features.groupby("symbol", group_keys=False)
    previous_close = grouped["close"].shift()
    true_range = pd.concat(
        [
            features["high"] - features["low"],
            (features["high"] - previous_close).abs(),
            (features["low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    features["atr14"] = true_range.groupby(features["symbol"]).transform(
        lambda values: values.ewm(alpha=1 / 14, adjust=False).mean()
    )
    for span in [20, 50]:
        features[f"ema{span}"] = grouped["close"].transform(
            lambda values, span=span: values.ewm(span=span, adjust=False).mean()
        )

    maps: dict[str, dict[str, Any]] = {}
    for symbol, sub in features.groupby("symbol", sort=False):
        sub = sub.sort_values("date").reset_index(drop=True)
        maps[symbol] = {
            "frame": sub,
            "date_to_idx": {pd.Timestamp(date): idx for idx, date in enumerate(sub["date"])},
        }
    return signals.sort_values(["date", "symbol"]).reset_index(drop=True), maps


def policy_grid() -> list[IndicatorPolicy]:
    policies = [IndicatorPolicy("hold_open")]
    policies.extend(IndicatorPolicy("chandelier", atr_multiple=value) for value in [3.0, 4.0, 5.0])
    policies.extend(
        IndicatorPolicy("price_flow", ema_span=span, balance_threshold=balance)
        for span in [20, 50]
        for balance in [0.6, 1.0]
    )
    policies.extend(
        IndicatorPolicy("thesis_failure", balance_threshold=balance, breadth_threshold=breadth)
        for balance in [0.4, 0.6]
        for breadth in [0.3, 0.5]
    )
    policies.extend(
        IndicatorPolicy(
            "hybrid",
            atr_multiple=atr,
            ema_span=50,
            balance_threshold=balance,
            breadth_threshold=0.5,
        )
        for atr in [4.0, 5.0]
        for balance in [0.6, 1.0]
    )
    return policies


def indicator_reason(row: pd.Series, peak_high: float, policy: IndicatorPolicy) -> str | None:
    balance_weak = bool(row["pos_neg_balance"] < policy.balance_threshold) if policy.balance_threshold is not None else False
    breadth_weak = bool(row["buyer_count_pct_cs"] < policy.breadth_threshold) if policy.breadth_threshold is not None else False
    trend_weak = bool(row["close"] < row[f"ema{policy.ema_span}"]) if policy.ema_span is not None else False
    chandelier_weak = False
    if policy.atr_multiple is not None:
        chandelier_weak = bool(row["close"] < peak_high - policy.atr_multiple * row["atr14"])

    if policy.policy_type == "chandelier" and chandelier_weak:
        return "CHANDELIER"
    if policy.policy_type == "price_flow" and trend_weak and balance_weak:
        return "PRICE_FLOW"
    if policy.policy_type == "thesis_failure" and balance_weak and breadth_weak:
        return "THESIS_FAILURE"
    if policy.policy_type == "hybrid":
        if chandelier_weak:
            return "CHANDELIER"
        if trend_weak and balance_weak and breadth_weak:
            return "PRICE_FLOW_FAILURE"
    return None


def simulate_policy(
    signals: pd.DataFrame,
    feature_maps: dict[str, dict[str, Any]],
    policy: IndicatorPolicy,
    evaluation_end: pd.Timestamp,
    cost: CostConfig,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for signal in signals.itertuples(index=False):
        symbol_data = feature_maps.get(signal.symbol)
        if symbol_data is None:
            continue
        data = symbol_data["frame"]
        date_to_idx = symbol_data["date_to_idx"]
        signal_idx = date_to_idx.get(pd.Timestamp(signal.date))
        if signal_idx is None or signal_idx + 1 >= len(data):
            continue
        entry_idx = signal_idx + 1
        if pd.Timestamp(data.loc[entry_idx, "date"]) > evaluation_end:
            continue
        eligible = data.index[(data.index >= entry_idx) & (data["date"] <= evaluation_end)]
        if eligible.empty:
            continue

        entry_raw = float(data.loc[entry_idx, "open"])
        peak_high = float(data.loc[entry_idx, "high"])
        exit_idx: int | None = None
        exit_reason = "OPEN"
        for idx in eligible:
            row = data.loc[idx]
            peak_high = max(peak_high, float(row["high"]))
            reason = indicator_reason(row, peak_high, policy)
            if reason is not None:
                exit_idx = int(idx)
                exit_reason = reason
                break

        marked_open = exit_idx is None
        valuation_idx = int(eligible[-1]) if marked_open else int(exit_idx)
        exit_raw = float(data.loc[valuation_idx, "close"])
        rows.append(
            {
                "symbol": signal.symbol,
                "signal_date": pd.Timestamp(signal.date),
                "entry_date": pd.Timestamp(data.loc[entry_idx, "date"]),
                "valuation_date": pd.Timestamp(data.loc[valuation_idx, "date"]),
                "entry_raw": entry_raw,
                "exit_or_mark_raw": exit_raw,
                "holding_observations": valuation_idx - entry_idx,
                "net_ret": net_return(entry_raw, exit_raw, cost),
                "position_status": "OPEN_MARKED" if marked_open else "CLOSED",
                "exit_reason": exit_reason,
                "policy_id": policy.policy_id,
            }
        )
    return pd.DataFrame(rows)


def metrics(trades: pd.DataFrame) -> dict[str, float | int]:
    if trades.empty:
        return {"positions": 0}
    returns = trades["net_ret"].astype(float)
    cutoff = returns.quantile(0.95)
    closed = trades[trades["position_status"] == "CLOSED"]
    return {
        "positions": int(len(trades)),
        "closed_rate": float(len(closed) / len(trades)),
        "marked_win_rate": float((returns > 0).mean()),
        "marked_avg_net_ret": float(returns.mean()),
        "marked_median_net_ret": float(returns.median()),
        "marked_trimmed_avg_net_ret": float(returns[returns <= cutoff].mean()),
        "marked_max_loss": float(returns.min()),
        "avg_holding_observations": float(trades["holding_observations"].mean()),
        "closed_avg_net_ret": float(closed["net_ret"].mean()) if not closed.empty else np.nan,
    }


def evaluate(
    signals: pd.DataFrame,
    maps: dict[str, dict[str, Any]],
    policies: list[IndicatorPolicy],
    evaluation_end: pd.Timestamp,
    cost: CostConfig,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    rows = []
    trades_by_policy = {}
    for policy in policies:
        trades = simulate_policy(signals, maps, policy, evaluation_end, cost)
        trades_by_policy[policy.policy_id] = trades
        rows.append({"policy_id": policy.policy_id, **asdict(policy), **metrics(trades)})
    leaderboard = pd.DataFrame(rows).sort_values(
        ["marked_trimmed_avg_net_ret", "marked_median_net_ret"], ascending=False
    )
    return leaderboard.reset_index(drop=True), trades_by_policy


def choose_policy(leaderboard: pd.DataFrame) -> str:
    candidates = leaderboard[
        (leaderboard["policy_type"] != "hold_open")
        & (leaderboard["positions"] >= 50)
        & (leaderboard["marked_max_loss"] >= -0.40)
    ]
    if candidates.empty:
        candidates = leaderboard[leaderboard["policy_type"] != "hold_open"]
    return str(candidates.iloc[0]["policy_id"])


def walk_forward(
    signals: pd.DataFrame, maps: dict[str, dict[str, Any]], policies: list[IndicatorPolicy], cost: CostConfig
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    folds = [
        ("2025-10-01", "2025-11-30"),
        ("2025-12-01", "2026-01-31"),
        ("2026-02-01", "2026-03-31"),
        ("2026-04-01", "2026-06-17"),
    ]
    fold_rows = []
    test_parts = []
    baseline_parts = []
    for fold_id, (start_raw, end_raw) in enumerate(folds, start=1):
        start, end = pd.Timestamp(start_raw), pd.Timestamp(end_raw)
        train_signals = signals[signals["date"] < start]
        train_board, _ = evaluate(train_signals, maps, policies, start - pd.Timedelta(days=1), cost)
        selected = choose_policy(train_board)
        test_signals = signals[(signals["date"] >= start) & (signals["date"] <= end)]
        policy = next(item for item in policies if item.policy_id == selected)
        test = simulate_policy(test_signals, maps, policy, end, cost)
        baseline = simulate_policy(test_signals, maps, IndicatorPolicy("hold_open"), end, cost)
        fold_rows.append(
            {
                "fold": fold_id,
                "test_start": start,
                "test_end": end,
                "selected_policy": selected,
                **{f"test_{key}": value for key, value in metrics(test).items()},
                **{f"hold_open_{key}": value for key, value in metrics(baseline).items()},
            }
        )
        test["fold"] = fold_id
        baseline["fold"] = fold_id
        test_parts.append(test)
        baseline_parts.append(baseline)
    return (
        pd.DataFrame(fold_rows),
        pd.concat(test_parts, ignore_index=True),
        pd.concat(baseline_parts, ignore_index=True),
    )


def pct(value: float) -> str:
    return "n/a" if pd.isna(value) else f"{value:.2%}"


def write_report(
    path: Path,
    leaderboard: pd.DataFrame,
    folds: pd.DataFrame,
    wf_trades: pd.DataFrame,
    wf_baseline: pd.DataFrame,
) -> None:
    wf = metrics(wf_trades)
    baseline = metrics(wf_baseline)
    top = leaderboard[leaderboard["policy_type"] != "hold_open"].head(8)
    lines = [
        "# 賣壓吸收策略：指標出場研究",
        "",
        "## 原則",
        "",
        "- 不設定固定持有天數，也不設定最長持有期限。",
        "- 指標觸發才出場；未觸發者維持未平倉，只在評估截止日按收盤價標記損益。",
        "- ATR 與 EMA 的數字是指標回看週期或波動倍數，不是持有期限。",
        "- Walk-forward 每一折只用測試開始日前可見的資料選規則。",
        "",
        "## Walk-forward",
        "",
        f"- 合計 {wf['positions']} 個部位，指標已出場比例 {pct(wf['closed_rate'])}。",
        f"- 含未平倉市價評估：平均 {pct(wf['marked_avg_net_ret'])}，中位數 {pct(wf['marked_median_net_ret'])}，勝率 {pct(wf['marked_win_rate'])}，最大虧損 {pct(wf['marked_max_loss'])}。",
        f"- 完全不出場基準：平均 {pct(baseline['marked_avg_net_ret'])}，中位數 {pct(baseline['marked_median_net_ret'])}，最大虧損 {pct(baseline['marked_max_loss'])}。",
        "- 目前指標出場未提升樣本外平均報酬；較明確的用途是限制部分下跌路徑，仍不能視為已確認的超額出場訊號。",
        "",
        "| Fold | 評估區間 | 訓練選出的指標 | 部位 | 已出場 | 平均 | 中位數 | 最大虧損 | 不出場基準平均 |",
        "|---:|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in folds.itertuples(index=False):
        lines.append(
            f"| {row.fold} | {row.test_start:%Y-%m-%d} ~ {row.test_end:%Y-%m-%d} | `{row.selected_policy}` | "
            f"{row.test_positions} | {pct(row.test_closed_rate)} | {pct(row.test_marked_avg_net_ret)} | "
            f"{pct(row.test_marked_median_net_ret)} | {pct(row.test_marked_max_loss)} | {pct(row.hold_open_marked_avg_net_ret)} |"
        )
    lines.extend(
        [
            "",
            "## 全樣本指標比較",
            "",
            "| 指標規則 | 已出場 | 標記平均 | 標記中位數 | 勝率 | 最大虧損 | 已出場交易平均 |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in top.itertuples(index=False):
        lines.append(
            f"| `{row.policy_id}` | {pct(row.closed_rate)} | {pct(row.marked_avg_net_ret)} | "
            f"{pct(row.marked_median_net_ret)} | {pct(row.marked_win_rate)} | {pct(row.marked_max_loss)} | {pct(row.closed_avg_net_ret)} |"
        )
    lines.extend(
        [
            "",
            "## 指標定義",
            "",
            "- `chandelier_atrN`：收盤跌破持有期間最高價減 N 倍 ATR14。",
            "- `price_flow`：收盤低於 EMA，且正向買盤相對賣壓的比值同步低於門檻。",
            "- `thesis_failure`：買賣平衡與買方分點廣度同時失效。",
            "- `hybrid`：Chandelier 觸發，或 EMA50、買賣平衡、買方廣度三者同時轉弱。",
            "",
            "未平倉部位的標記報酬不是已實現報酬；必須同時閱讀已出場比例，不能只比較平均值。",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--signals", type=Path, default=DEFAULT_SIGNALS)
    parser.add_argument("--features", type=Path, default=DEFAULT_FEATURES)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--evaluation-end", default="2026-06-17")
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    signals, maps = load_inputs(args.signals, args.features)
    policies = policy_grid()
    cost = CostConfig()
    leaderboard, trades_by_policy = evaluate(signals, maps, policies, pd.Timestamp(args.evaluation_end), cost)
    folds, wf_trades, wf_baseline = walk_forward(signals, maps, policies, cost)

    leaderboard.to_csv(args.out_dir / "indicator_exit_leaderboard.csv", index=False)
    folds.to_csv(args.out_dir / "walk_forward_folds.csv", index=False)
    wf_trades.to_csv(args.out_dir / "walk_forward_positions.csv", index=False)
    wf_baseline.to_csv(args.out_dir / "walk_forward_hold_open_positions.csv", index=False)
    selected = choose_policy(leaderboard)
    trades_by_policy[selected].to_csv(args.out_dir / "selected_policy_positions.csv", index=False)
    write_report(args.out_dir / "indicator_exit_report.md", leaderboard, folds, wf_trades, wf_baseline)
    summary = {
        "evaluation_end": args.evaluation_end,
        "signals": int(len(signals)),
        "candidate_policies": len(policies) - 1,
        "full_sample_selected_policy": selected,
        "walk_forward_metrics": metrics(wf_trades),
    }
    (args.out_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
