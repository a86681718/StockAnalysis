#!/usr/bin/env python3
"""Walk-forward indicator exit research for the six tracked strategies."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from run_sell_pressure_exit_research import CostConfig, net_return


PROJECT_ROOT = Path(__file__).resolve().parents[2]
OHLC_PATH = PROJECT_ROOT / "data/_derived/ohlc.parquet"
OUT_DIR = PROJECT_ROOT / "outputs/analysis/six_strategy_indicator_exit_research"

STRATEGY_SOURCES = {
    "key_broker_branch_hybrid": (
        PROJECT_ROOT / "outputs/analysis/key_broker_branch_hybrid/key_broker_branch_hybrid_signals.parquet",
        "date",
    ),
    "warrant_leads_stock": (
        PROJECT_ROOT / "outputs/analysis/rare_event/warrant_leads_stock_refine_signals.parquet",
        "date",
    ),
    "direction3_breakout": (
        PROJECT_ROOT / "outputs/analysis/direction3_breakout_robustness/direction3_breakout_enriched_trades.csv",
        "signal_date",
    ),
    "diffuse_buyer_accumulation": (
        PROJECT_ROOT / "outputs/analysis/raw_broker_microstructure/diffuse_buyer_accumulation_best_signals.csv",
        "date",
    ),
    "role_reversal": (
        PROJECT_ROOT / "outputs/analysis/raw_broker_microstructure/role_reversal_best_signals.csv",
        "date",
    ),
    "sell_pressure_absorption": (
        PROJECT_ROOT / "outputs/analysis/raw_broker_microstructure/sell_pressure_absorption_best_signals.csv",
        "date",
    ),
}


@dataclass(frozen=True)
class ExitPolicy:
    policy_id: str
    kind: str
    value: Any = None


POLICIES = [
    ExitPolicy("hold_open", "hold_open"),
    ExitPolicy("rsi_below_45", "rsi_below", 45),
    ExitPolicy("rsi_overbought_reversal_70", "rsi_reversal", 70),
    ExitPolicy("macd_bear_cross", "macd_cross"),
    ExitPolicy("ema20_bear_cross", "ema_cross", 20),
    ExitPolicy("ema50_bear_cross", "ema_cross", 50),
    ExitPolicy("bollinger_mid_bear_cross", "bollinger_mid_cross"),
    ExitPolicy("bollinger_lower_break", "bollinger_lower_break"),
    ExitPolicy("chandelier_atr3", "chandelier", 3.0),
    ExitPolicy("chandelier_atr4", "chandelier", 4.0),
    ExitPolicy("chandelier_atr5", "chandelier", 5.0),
    ExitPolicy("macd_below_and_under_ema20", "macd_ema"),
    ExitPolicy("rsi50_and_macd_bear", "rsi_macd"),
    ExitPolicy("bollinger_mid_and_macd_bear", "bollinger_macd"),
    ExitPolicy("market_risk_confirmed", "market_confirmed"),
    ExitPolicy("profit_chandelier_atr3_act10", "profit_chandelier", (3.0, 0.10)),
    ExitPolicy("profit_chandelier_atr4_act10", "profit_chandelier", (4.0, 0.10)),
    ExitPolicy("profit_chandelier_atr3_act20", "profit_chandelier", (3.0, 0.20)),
    ExitPolicy("profit_chandelier_atr4_act20", "profit_chandelier", (4.0, 0.20)),
    ExitPolicy("profit_macd_cross_act10", "profit_macd", 0.10),
    ExitPolicy("profit_macd_cross_act20", "profit_macd", 0.20),
    ExitPolicy("bollinger_upper_reentry", "bollinger_upper_reentry"),
]


def load_signals() -> dict[str, pd.DataFrame]:
    result = {}
    for strategy, (path, date_column) in STRATEGY_SOURCES.items():
        frame = pd.read_parquet(path) if path.suffix == ".parquet" else pd.read_csv(path, dtype={"symbol": str})
        frame["symbol"] = frame["symbol"].astype(str)
        frame["date"] = pd.to_datetime(frame[date_column])
        frame = frame[["symbol", "date"]].dropna().drop_duplicates().sort_values(["date", "symbol"])
        result[strategy] = frame.reset_index(drop=True)
    return result


def _rsi(close: pd.Series, period: int = 14) -> pd.Series:
    change = close.diff()
    gain = change.clip(lower=0).ewm(alpha=1 / period, adjust=False).mean()
    loss = (-change.clip(upper=0)).ewm(alpha=1 / period, adjust=False).mean()
    relative_strength = gain / loss.replace(0, np.nan)
    return (100 - 100 / (1 + relative_strength)).fillna(100.0)


def add_indicators(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.sort_values("date").reset_index(drop=True).copy()
    close = frame["close"]
    previous_close = close.shift()
    true_range = pd.concat(
        [
            frame["high"] - frame["low"],
            (frame["high"] - previous_close).abs(),
            (frame["low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    frame["atr14"] = true_range.ewm(alpha=1 / 14, adjust=False).mean()
    frame["rsi14"] = _rsi(close)
    frame["ema12"] = close.ewm(span=12, adjust=False).mean()
    frame["ema20"] = close.ewm(span=20, adjust=False).mean()
    frame["ema26"] = close.ewm(span=26, adjust=False).mean()
    frame["ema50"] = close.ewm(span=50, adjust=False).mean()
    frame["macd"] = frame["ema12"] - frame["ema26"]
    frame["macd_signal"] = frame["macd"].ewm(span=9, adjust=False).mean()
    frame["bb_mid"] = close.rolling(20).mean()
    bb_std = close.rolling(20).std(ddof=0)
    frame["bb_lower"] = frame["bb_mid"] - 2.0 * bb_std
    frame["bb_upper"] = frame["bb_mid"] + 2.0 * bb_std
    return frame


def load_market_data(ohlc_path: Path, symbols: set[str]) -> tuple[dict[str, dict[str, Any]], dict[pd.Timestamp, bool]]:
    wanted = sorted(symbols | {"0050"})
    columns = ["symbol", "date", "open", "high", "low", "close"]
    ohlc = pd.read_parquet(ohlc_path, columns=columns)
    ohlc["symbol"] = ohlc["symbol"].astype(str)
    ohlc = ohlc[ohlc["symbol"].isin(wanted)].copy()
    ohlc["date"] = pd.to_datetime(ohlc["date"])
    for column in ["open", "high", "low", "close"]:
        ohlc[column] = pd.to_numeric(ohlc[column], errors="coerce")
    ohlc = ohlc.dropna(subset=["open", "high", "low", "close"])

    maps = {}
    for symbol, sub in ohlc.groupby("symbol", sort=False):
        enriched = add_indicators(sub)
        maps[symbol] = {
            "frame": enriched,
            "date_to_idx": {pd.Timestamp(date): idx for idx, date in enumerate(enriched["date"])},
        }
    benchmark = maps.get("0050")
    if benchmark is None:
        raise RuntimeError("0050 benchmark is unavailable in OHLC")
    benchmark_frame = benchmark["frame"]
    market_risk_off = {
        pd.Timestamp(row.date): bool(row.close < row.ema50 and row.macd < row.macd_signal)
        for row in benchmark_frame.itertuples(index=False)
    }
    return maps, market_risk_off


def exit_trigger(
    frame: pd.DataFrame,
    idx: int,
    peak_high: float,
    entry_raw: float,
    state: dict[str, bool],
    policy: ExitPolicy,
    market_risk_off: dict[pd.Timestamp, bool],
) -> str | None:
    row = frame.iloc[idx]
    previous = frame.iloc[idx - 1] if idx > 0 else row
    if policy.kind == "rsi_below" and row.rsi14 < float(policy.value):
        return "RSI_WEAK"
    if policy.kind == "rsi_reversal" and previous.rsi14 >= float(policy.value) and row.rsi14 < float(policy.value):
        return "RSI_REVERSAL"
    if policy.kind == "macd_cross" and previous.macd >= previous.macd_signal and row.macd < row.macd_signal:
        return "MACD_CROSS"
    if policy.kind == "ema_cross":
        column = f"ema{int(policy.value)}"
        if previous.close >= previous[column] and row.close < row[column]:
            return "EMA_CROSS"
    if policy.kind == "bollinger_mid_cross" and previous.close >= previous.bb_mid and row.close < row.bb_mid:
        return "BB_MID_CROSS"
    if policy.kind == "bollinger_lower_break" and row.close < row.bb_lower:
        return "BB_LOWER_BREAK"
    if policy.kind == "chandelier" and row.close < peak_high - float(policy.value) * row.atr14:
        return "CHANDELIER"
    if policy.kind == "macd_ema" and row.macd < row.macd_signal and row.close < row.ema20:
        return "MACD_EMA"
    if policy.kind == "rsi_macd" and row.rsi14 < 50 and row.macd < row.macd_signal:
        return "RSI_MACD"
    if policy.kind == "bollinger_macd" and row.close < row.bb_mid and row.macd < row.macd_signal:
        return "BB_MACD"
    if policy.kind == "market_confirmed":
        risk_off = market_risk_off.get(pd.Timestamp(row.date), False)
        if risk_off and row.close < row.ema20 and row.macd < row.macd_signal:
            return "MARKET_CONFIRMED"
    if policy.kind == "profit_chandelier":
        atr_multiple, activation_return = policy.value
        state["profit_armed"] = state["profit_armed"] or peak_high >= entry_raw * (1.0 + activation_return)
        if state["profit_armed"] and row.close < peak_high - atr_multiple * row.atr14:
            return "PROFIT_CHANDELIER"
    if policy.kind == "profit_macd":
        state["profit_armed"] = state["profit_armed"] or peak_high >= entry_raw * (1.0 + float(policy.value))
        if state["profit_armed"] and previous.macd >= previous.macd_signal and row.macd < row.macd_signal:
            return "PROFIT_MACD"
    if policy.kind == "bollinger_upper_reentry":
        if row.close > row.bb_upper:
            state["upper_band_armed"] = True
        if state["upper_band_armed"] and previous.close >= previous.bb_upper and row.close < row.bb_upper:
            return "BB_UPPER_REENTRY"
    return None


def simulate(
    signals: pd.DataFrame,
    maps: dict[str, dict[str, Any]],
    policy: ExitPolicy,
    evaluation_end: pd.Timestamp,
    market_risk_off: dict[pd.Timestamp, bool],
    cost: CostConfig,
) -> pd.DataFrame:
    rows = []
    for signal in signals.itertuples(index=False):
        symbol_data = maps.get(signal.symbol)
        if symbol_data is None:
            continue
        frame = symbol_data["frame"]
        signal_idx = symbol_data["date_to_idx"].get(pd.Timestamp(signal.date))
        if signal_idx is None or signal_idx + 1 >= len(frame):
            continue
        entry_idx = signal_idx + 1
        if pd.Timestamp(frame.iloc[entry_idx].date) > evaluation_end:
            continue
        end_candidates = frame.index[frame["date"] <= evaluation_end]
        if end_candidates.empty or int(end_candidates[-1]) < entry_idx:
            continue
        end_idx = int(end_candidates[-1])
        entry_raw = float(frame.iloc[entry_idx].open)
        peak_high = float(frame.iloc[entry_idx].high)
        trough_low = float(frame.iloc[entry_idx].low)
        state = {"profit_armed": False, "upper_band_armed": False}
        exit_idx = None
        exit_reason = "OPEN"
        if policy.kind != "hold_open":
            for idx in range(entry_idx, end_idx + 1):
                peak_high = max(peak_high, float(frame.iloc[idx].high))
                trough_low = min(trough_low, float(frame.iloc[idx].low))
                reason = exit_trigger(frame, idx, peak_high, entry_raw, state, policy, market_risk_off)
                if reason is not None:
                    exit_idx = idx
                    exit_reason = reason
                    break
        is_open = exit_idx is None
        valuation_idx = end_idx if is_open else int(exit_idx)
        if is_open:
            trough_low = min(trough_low, float(frame.loc[entry_idx:valuation_idx, "low"].min()))
        exit_raw = float(frame.iloc[valuation_idx].close)
        rows.append(
            {
                "symbol": signal.symbol,
                "signal_date": pd.Timestamp(signal.date),
                "entry_date": pd.Timestamp(frame.iloc[entry_idx].date),
                "valuation_date": pd.Timestamp(frame.iloc[valuation_idx].date),
                "net_ret": net_return(entry_raw, exit_raw, cost),
                "mae": trough_low / entry_raw - 1.0,
                "holding_observations": valuation_idx - entry_idx,
                "position_status": "OPEN_MARKED" if is_open else "CLOSED",
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
        "closed_rate": float((trades["position_status"] == "CLOSED").mean()),
        "win_rate": float((returns > 0).mean()),
        "avg_net_ret": float(returns.mean()),
        "median_net_ret": float(returns.median()),
        "trimmed_avg_net_ret": float(returns[returns <= cutoff].mean()),
        "max_loss": float(returns.min()),
        "avg_mae": float(trades["mae"].mean()),
        "worst_mae": float(trades["mae"].min()),
        "closed_avg_holding": float(closed["holding_observations"].mean()) if not closed.empty else np.nan,
    }


def make_folds(signals: pd.DataFrame) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    dates = pd.Series(sorted(signals["date"].drop_duplicates()))
    if len(dates) < 5:
        return []
    boundaries = sorted({min(len(dates) - 1, int(len(dates) * fraction)) for fraction in [0.4, 0.6, 0.8]})
    folds = []
    for position, start_idx in enumerate(boundaries):
        end_idx = boundaries[position + 1] - 1 if position + 1 < len(boundaries) else len(dates) - 1
        folds.append((pd.Timestamp(dates.iloc[start_idx]), pd.Timestamp(dates.iloc[end_idx])))
    return folds


def evaluate_policy_set(
    signals: pd.DataFrame,
    maps: dict[str, dict[str, Any]],
    policies: list[ExitPolicy],
    evaluation_end: pd.Timestamp,
    market_risk_off: dict[pd.Timestamp, bool],
    cost: CostConfig,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    rows = []
    trades_by_policy = {}
    for policy in policies:
        trades = simulate(signals, maps, policy, evaluation_end, market_risk_off, cost)
        trades_by_policy[policy.policy_id] = trades
        rows.append({"policy_id": policy.policy_id, **metrics(trades)})
    return pd.DataFrame(rows), trades_by_policy


def select_train_policy(board: pd.DataFrame) -> str:
    baseline = board.loc[board["policy_id"] == "hold_open"].iloc[0]
    candidates = board[
        (board["policy_id"] != "hold_open")
        & (board["positions"] >= 8)
        & (board["worst_mae"] >= baseline["worst_mae"])
        & (board["trimmed_avg_net_ret"] >= baseline["trimmed_avg_net_ret"] - 0.01)
    ]
    if candidates.empty:
        candidates = board[board["policy_id"] != "hold_open"].copy()
        candidates["fallback_score"] = candidates["trimmed_avg_net_ret"] + 0.20 * candidates["worst_mae"]
        return str(candidates.sort_values(["fallback_score", "median_net_ret"], ascending=False).iloc[0].policy_id)
    return str(candidates.sort_values(["trimmed_avg_net_ret", "median_net_ret"], ascending=False).iloc[0].policy_id)


def research_strategy(
    strategy: str,
    signals: pd.DataFrame,
    maps: dict[str, dict[str, Any]],
    market_risk_off: dict[pd.Timestamp, bool],
    cost: CostConfig,
) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    folds = make_folds(signals)
    fold_rows = []
    fixed_parts: dict[str, list[pd.DataFrame]] = {policy.policy_id: [] for policy in POLICIES}
    selected_parts = []
    for fold_id, (test_start, test_end) in enumerate(folds, start=1):
        train_signals = signals[signals["date"] < test_start]
        train_board, _ = evaluate_policy_set(
            train_signals, maps, POLICIES, test_start - pd.Timedelta(days=1), market_risk_off, cost
        )
        selected_id = select_train_policy(train_board)
        test_signals = signals[(signals["date"] >= test_start) & (signals["date"] <= test_end)]
        test_board, test_trades = evaluate_policy_set(test_signals, maps, POLICIES, test_end, market_risk_off, cost)
        selected_metrics = test_board.loc[test_board["policy_id"] == selected_id].iloc[0]
        baseline_metrics = test_board.loc[test_board["policy_id"] == "hold_open"].iloc[0]
        fold_rows.append(
            {
                "strategy": strategy,
                "fold": fold_id,
                "test_start": test_start,
                "test_end": test_end,
                "selected_policy": selected_id,
                **{f"selected_{key}": selected_metrics[key] for key in metrics(test_trades[selected_id])},
                **{f"baseline_{key}": baseline_metrics[key] for key in metrics(test_trades["hold_open"])},
            }
        )
        selected_parts.append(test_trades[selected_id])
        for policy_id, trades in test_trades.items():
            fixed_parts[policy_id].append(trades)

    fixed_rows = []
    for policy_id, parts in fixed_parts.items():
        combined = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
        fixed_rows.append({"strategy": strategy, "policy_id": policy_id, **metrics(combined)})
    comparison = pd.DataFrame(fixed_rows)
    baseline = comparison.loc[comparison["policy_id"] == "hold_open"].iloc[0]
    candidates = comparison[comparison["policy_id"] != "hold_open"].copy()
    candidates["dual_improvement"] = (
        (candidates["closed_rate"] >= 0.05)
        & (candidates["avg_net_ret"] > baseline["avg_net_ret"])
        & (candidates["worst_mae"] > baseline["worst_mae"])
    )
    pool = candidates[candidates["dual_improvement"]]
    if pool.empty:
        pool = candidates[
            (candidates["closed_rate"] >= 0.05)
            & (candidates["worst_mae"] > baseline["worst_mae"] + 0.005)
        ]
    if pool.empty:
        pool = candidates
    best = pool.sort_values(["dual_improvement", "avg_net_ret", "worst_mae"], ascending=False).iloc[0]
    selected_oos = pd.concat(selected_parts, ignore_index=True) if selected_parts else pd.DataFrame()
    selected_metrics = metrics(selected_oos)
    baseline_metrics = metrics(pd.concat(fixed_parts["hold_open"], ignore_index=True))
    selected_ids = [row["selected_policy"] for row in fold_rows]
    summary = {
        "strategy": strategy,
        "signals": int(len(signals)),
        "signal_start": str(signals["date"].min().date()),
        "signal_end": str(signals["date"].max().date()),
        "folds": len(folds),
        "walk_forward_positions": selected_metrics.get("positions", 0),
        "walk_forward_selected_avg": selected_metrics.get("avg_net_ret", np.nan),
        "walk_forward_selected_max_loss": selected_metrics.get("max_loss", np.nan),
        "walk_forward_selected_worst_mae": selected_metrics.get("worst_mae", np.nan),
        "walk_forward_baseline_avg": baseline_metrics.get("avg_net_ret", np.nan),
        "walk_forward_baseline_worst_mae": baseline_metrics.get("worst_mae", np.nan),
        "walk_forward_dual_improvement": bool(
            selected_metrics.get("avg_net_ret", -np.inf) > baseline_metrics.get("avg_net_ret", np.inf)
            and selected_metrics.get("worst_mae", -np.inf) > baseline_metrics.get("worst_mae", np.inf)
        ),
        "modal_selected_policy": Counter(selected_ids).most_common(1)[0][0] if selected_ids else None,
        "research_best_fixed_policy": str(best["policy_id"]),
        "dual_improvement": bool(best["dual_improvement"]),
        "recommendation_status": "dual_improvement" if bool(best["dual_improvement"]) else "risk_tradeoff",
        "best_avg_net_ret": float(best["avg_net_ret"]),
        "best_median_net_ret": float(best["median_net_ret"]),
        "best_max_loss": float(best["max_loss"]),
        "best_worst_mae": float(best["worst_mae"]),
        "best_closed_rate": float(best["closed_rate"]),
        "best_closed_avg_holding": float(best["closed_avg_holding"]),
        "baseline_avg_net_ret": float(baseline["avg_net_ret"]),
        "baseline_median_net_ret": float(baseline["median_net_ret"]),
        "baseline_max_loss": float(baseline["max_loss"]),
        "baseline_worst_mae": float(baseline["worst_mae"]),
        "confidence": "low" if len(signals) < 50 else "moderate",
    }
    comparison["dual_improvement"] = comparison["policy_id"].map(
        candidates.set_index("policy_id")["dual_improvement"]
    ).eq(True)
    return summary, pd.DataFrame(fold_rows), comparison


def percentage(value: float) -> str:
    return "n/a" if pd.isna(value) else f"{value:.2%}"


def write_report(path: Path, summary: pd.DataFrame, folds: pd.DataFrame) -> None:
    lines = [
        "# 六策略非固定天數出場研究",
        "",
        "## 方法",
        "",
        "- 所有策略皆於訊號次一交易日開盤進場。",
        "- 不設固定持有日或最長持有日；指標未觸發者維持未平倉並按 fold 截止日市價評估。",
        "- 候選包含 RSI14、MACD、EMA20/50、布林通道、ATR14 Chandelier，以及 0050 市場風險確認。",
        "- 每個策略使用自己的 expanding walk-forward；參數只由當時可見的訓練資料選擇。",
        "- `dual_improvement` 僅表示固定規則在共同樣本外區間同時提高平均報酬並降低持有期間最差 MAE，仍需更多 forward 資料驗證。",
        "",
        "## 策略結果",
        "",
        "以下是固定規則在共同樣本外區間的事後研究候選，仍有多重比較偏差，不等於已驗證上線規則。",
        "",
        "| 策略 | 訊號 | 研究候選 | 狀態 | 平均：候選 / 不出場 | 最差 MAE：候選 / 不出場 | 已出場 / 平均觀察數 | 信心 |",
        "|---|---:|---|---|---:|---:|---:|---|",
    ]
    for row in summary.itertuples(index=False):
        lines.append(
            f"| `{row.strategy}` | {row.signals} | `{row.research_best_fixed_policy}` | `{row.recommendation_status}` | "
            f"{percentage(row.best_avg_net_ret)} / {percentage(row.baseline_avg_net_ret)} | "
            f"{percentage(row.best_worst_mae)} / {percentage(row.baseline_worst_mae)} | "
            f"{percentage(row.best_closed_rate)} / {row.best_closed_avg_holding:.1f} | {row.confidence} |"
        )
    lines.extend(
        [
            "",
            "## 真正 Walk-forward 聚合",
            "",
            "每一折只使用當時可見的訓練資料選規則；此表才是較嚴格的未來資料模擬。",
            "",
            "| 策略 | 雙改善 | 平均：動態規則 / 不出場 | 最差 MAE：動態規則 / 不出場 |",
            "|---|---:|---:|---:|",
        ]
    )
    for row in summary.itertuples(index=False):
        lines.append(
            f"| `{row.strategy}` | `{bool(row.walk_forward_dual_improvement)}` | "
            f"{percentage(row.walk_forward_selected_avg)} / {percentage(row.walk_forward_baseline_avg)} | "
            f"{percentage(row.walk_forward_selected_worst_mae)} / {percentage(row.walk_forward_baseline_worst_mae)} |"
        )
    lines.extend(
        [
            "",
            "## Walk-forward 每折選擇",
            "",
            "| 策略 | Fold | 測試區間 | 訓練選出規則 | 平均：規則 / 不出場 | 最大虧損：規則 / 不出場 |",
            "|---|---:|---|---|---:|---:|",
        ]
    )
    for row in folds.itertuples(index=False):
        lines.append(
            f"| `{row.strategy}` | {row.fold} | {row.test_start:%Y-%m-%d} ~ {row.test_end:%Y-%m-%d} | "
            f"`{row.selected_policy}` | {percentage(row.selected_avg_net_ret)} / {percentage(row.baseline_avg_net_ret)} | "
            f"{percentage(row.selected_max_loss)} / {percentage(row.baseline_max_loss)} |"
        )
    lines.extend(
        [
            "",
            "## 限制",
            "",
            "- 這是單筆訊號的共同截止日市價比較，尚未包含持倉上限與現金再配置的 portfolio NAV。",
            "- Direction 3 只有 27 筆歷史交易訊號，任何出場結論都屬低信心。",
            "- 技術指標使用本地 OHLC 計算，避免外部資料的除權息、時區與缺漏不一致；市場狀態使用本地 0050。",
            "- 若沒有 `dual_improvement`，不應因全樣本排名直接替換正式出場規則。",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ohlc", type=Path, default=OHLC_PATH)
    parser.add_argument("--out-dir", type=Path, default=OUT_DIR)
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    strategy_signals = load_signals()
    symbols = {symbol for signals in strategy_signals.values() for symbol in signals["symbol"]}
    maps, market_risk_off = load_market_data(args.ohlc, symbols)
    cost = CostConfig()
    summaries = []
    fold_parts = []
    comparison_parts = []
    for strategy, signals in strategy_signals.items():
        print(f"researching {strategy}: {len(signals)} signals", flush=True)
        summary, folds, comparison = research_strategy(strategy, signals, maps, market_risk_off, cost)
        summaries.append(summary)
        fold_parts.append(folds)
        comparison_parts.append(comparison)

    summary_frame = pd.DataFrame(summaries)
    folds_frame = pd.concat(fold_parts, ignore_index=True)
    comparison_frame = pd.concat(comparison_parts, ignore_index=True)
    summary_frame.to_csv(args.out_dir / "strategy_exit_summary.csv", index=False)
    folds_frame.to_csv(args.out_dir / "walk_forward_folds.csv", index=False)
    comparison_frame.to_csv(args.out_dir / "policy_comparison.csv", index=False)
    write_report(args.out_dir / "six_strategy_indicator_exit_report.md", summary_frame, folds_frame)
    result = {
        "strategies": len(summary_frame),
        "research_candidate_dual_count": int(summary_frame["dual_improvement"].sum()),
        "walk_forward_dual_count": int(summary_frame["walk_forward_dual_improvement"].sum()),
        "results": summaries,
    }
    (args.out_dir / "summary.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
