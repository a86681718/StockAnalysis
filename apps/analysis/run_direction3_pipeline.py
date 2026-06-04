#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build the full direction-3 artifact chain from trusted breakout predictions.

This productizes the saved research rule:

1. start from `breakout10_predictions_wf.csv`
2. keep the repair-branch universe:
   - stock_net_buy_days_20 >= 1
   - warrant_hhi_posnet_20 <= 0.80
3. add `top_posnet_ratio <= 0.610`
4. replay the fixed breakout rule and compute day-level chip quality
5. keep only signal days where `mean_buy20 >= 4`
6. replay again and export the final direction-3 summary / trades / rolling windows
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from apps.analysis.run_strategy_search_oof import BacktestConfig, CostConfig, _effective_fee, _parse_exit, _select_candidates


ML_RUNS = PROJECT_ROOT / "data" / "_derived" / "ml_runs"
DATA_DERIVED = PROJECT_ROOT / "data" / "_derived"


@dataclass(frozen=True)
class ReplayArtifacts:
    summary: dict[str, object]
    trades: pd.DataFrame
    selected: pd.DataFrame


def _latest_complete_signal_date(ohlc_map: dict[str, dict[str, object]], hold_days: int) -> pd.Timestamp | None:
    trading_dates = sorted({pd.Timestamp(d) for data in ohlc_map.values() for d in data["dates"]})
    if len(trading_dates) <= hold_days + 1:
        return None
    return pd.Timestamp(trading_dates[-(hold_days + 2)])


def _prediction_label_col(df: pd.DataFrame) -> str | None:
    for col in df.columns:
        if col not in {"symbol", "date", "pred", "fold"}:
            return col
    return None


def _load_breakout_predictions(path: Path) -> tuple[pd.DataFrame, str | None]:
    df = pd.read_csv(path, low_memory=False)
    if df.empty:
        raise RuntimeError(f"prediction file is empty: {path}")
    required = {"symbol", "date", "pred"}
    if not required.issubset(df.columns):
        raise ValueError(f"prediction file missing columns: {required}")
    df["symbol"] = df["symbol"].astype(str)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["pred"] = pd.to_numeric(df["pred"], errors="coerce")
    label_col = _prediction_label_col(df)
    if label_col is not None:
        df[label_col] = pd.to_numeric(df[label_col], errors="coerce")
    df = df.dropna(subset=["symbol", "date", "pred"]).copy()
    df = df[df["symbol"].str.len() == 4].sort_values(["date", "pred"], ascending=[True, False]).reset_index(drop=True)
    return df, label_col


def _load_ohlc_map(path: Path) -> dict[str, dict[str, object]]:
    cols = ["symbol", "date", "open", "high", "low", "close", "volume"]
    ohlc = pd.read_parquet(path, columns=cols).copy()
    ohlc["symbol"] = ohlc["symbol"].astype(str)
    ohlc["date"] = pd.to_datetime(ohlc["date"])
    for col in ["open", "high", "low", "close", "volume"]:
        ohlc[col] = pd.to_numeric(ohlc[col], errors="coerce")
    ohlc = ohlc[ohlc["symbol"].str.len() == 4].dropna(subset=["open", "high", "low", "close"]).copy()
    out: dict[str, dict[str, object]] = {}
    for symbol, sub in ohlc.groupby("symbol"):
        sub = sub.sort_values("date").reset_index(drop=True)
        dates = sub["date"].to_numpy(dtype="datetime64[ns]")
        close = sub["close"].to_numpy(float)
        volume = sub["volume"].to_numpy(float)
        turnover = close * np.where(np.isfinite(volume), volume, np.nan)
        out[str(symbol)] = {
            "dates": dates,
            "open": sub["open"].to_numpy(float),
            "high": sub["high"].to_numpy(float),
            "low": sub["low"].to_numpy(float),
            "close": close,
            "volume": volume,
            "turnover": turnover,
            "date_to_idx": {pd.Timestamp(d): int(i) for i, d in enumerate(dates)},
        }
    return out


def _build_symbol_date_base(scored_path: Path) -> pd.DataFrame:
    scored = pd.read_parquet(scored_path, columns=["symbol", "date", "top_posnet_ratio", "dyn_k"]).copy()
    scored["symbol"] = scored["symbol"].astype(str)
    scored["date"] = pd.to_datetime(scored["date"])
    scored["top_posnet_ratio"] = pd.to_numeric(scored["top_posnet_ratio"], errors="coerce")
    scored["dyn_k"] = pd.to_numeric(scored["dyn_k"], errors="coerce")
    return scored[scored["symbol"].str.len() == 4].sort_values(["symbol", "date"]).reset_index(drop=True)


def _build_repair_features(scored_path: Path, stock_path: Path, warrant_path: Path) -> pd.DataFrame:
    base = _build_symbol_date_base(scored_path)

    stock = pd.read_parquet(stock_path, columns=["symbol", "date", "net_total"]).copy()
    stock["symbol"] = stock["symbol"].astype(str)
    stock["date"] = pd.to_datetime(stock["date"])
    stock["net_total"] = pd.to_numeric(stock["net_total"], errors="coerce")

    warrant = pd.read_parquet(warrant_path, columns=["symbol", "date", "hhi_posnet"]).copy()
    warrant["symbol"] = warrant["symbol"].astype(str)
    warrant["date"] = pd.to_datetime(warrant["date"])
    warrant["hhi_posnet"] = pd.to_numeric(warrant["hhi_posnet"], errors="coerce")

    feat = base.merge(stock, on=["symbol", "date"], how="left").merge(warrant, on=["symbol", "date"], how="left")
    feat = feat.sort_values(["symbol", "date"]).reset_index(drop=True)

    feat["stock_positive_day"] = (feat["net_total"].fillna(0.0) > 0).astype(float)
    feat["stock_net_buy_days_20"] = (
        feat.groupby("symbol")["stock_positive_day"]
        .transform(lambda s: s.rolling(20, min_periods=6).sum())
    )
    feat["warrant_hhi_posnet_20"] = (
        feat.groupby("symbol")["hhi_posnet"]
        .transform(lambda s: s.rolling(20, min_periods=6).mean())
    )
    feat = feat.rename(columns={"dyn_k": "stock_dyn_k"})
    keep = [
        "symbol",
        "date",
        "top_posnet_ratio",
        "stock_dyn_k",
        "stock_net_buy_days_20",
        "warrant_hhi_posnet_20",
    ]
    return feat[keep].copy()


def _merge_predictions_with_features(preds: pd.DataFrame, features: pd.DataFrame) -> pd.DataFrame:
    merged = preds.merge(features, on=["symbol", "date"], how="left")
    merged = merged.sort_values(["date", "pred"], ascending=[True, False]).reset_index(drop=True)
    return merged


def _save_prediction_csv(df: pd.DataFrame, path: Path, label_col: str | None) -> None:
    cols = ["symbol", "date", "pred"]
    if label_col is not None and label_col in df.columns:
        cols.append(label_col)
    out = df[cols].copy()
    out["date"] = pd.to_datetime(out["date"]).dt.strftime("%Y-%m-%d")
    path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(path, index=False)


def _write_live_summary(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _build_pred_map(preds: pd.DataFrame) -> dict[pd.Timestamp, pd.DataFrame]:
    out: dict[pd.Timestamp, pd.DataFrame] = {}
    for date, sub in preds.groupby("date"):
        out[pd.Timestamp(date)] = sub.sort_values("pred", ascending=False).reset_index(drop=True)
    return out


def _replay_with_selected(
    preds: pd.DataFrame,
    label_col: str | None,
    ohlc_map: dict[str, dict[str, object]],
    cfg: BacktestConfig,
    cost_cfg: CostConfig,
    start: pd.Timestamp,
    end: pd.Timestamp,
    capital: float,
    min_turnover: float,
    max_turnover_pct: float,
) -> ReplayArtifacts:
    pred_map = _build_pred_map(preds)
    dates = sorted([d for d in pred_map.keys() if start <= d <= end])
    eff_fee = _effective_fee(cost_cfg.fee_rate, cost_cfg.fee_discount)

    cash = float(capital)
    positions: list[dict[str, object]] = []
    trades: list[dict[str, object]] = []
    selected_rows: list[dict[str, object]] = []
    label_hits: list[float] = []
    signal_days = 0
    pick_count = 0

    for current_date in dates:
        still_open: list[dict[str, object]] = []
        for pos in positions:
            data = ohlc_map[pos["symbol"]]
            idx_map = data["date_to_idx"]
            i_now = idx_map.get(pd.Timestamp(current_date))
            if i_now is None:
                still_open.append(pos)
                continue
            hit, exit_px_raw, reason = _parse_exit(
                cfg,
                pos,
                int(i_now),
                float(data["high"][i_now]),
                float(data["low"][i_now]),
                float(data["close"][i_now]),
            )
            if not hit:
                still_open.append(pos)
                continue

            sell_px = exit_px_raw * (1.0 - cost_cfg.slippage)
            sell_value = pos["shares"] * sell_px
            sell_fee = max(sell_value * eff_fee, cost_cfg.fee_min)
            tax = sell_value * cost_cfg.tax_rate_sell
            proceeds = sell_value - sell_fee - tax
            cash += proceeds

            pnl = proceeds - pos["buy_total_cost"]
            net_ret = pnl / pos["buy_total_cost"] if pos["buy_total_cost"] > 0 else np.nan
            trades.append(
                {
                    "symbol": pos["symbol"],
                    "signal_date": pos["signal_date"],
                    "entry_date": pos["entry_date"],
                    "exit_date": pd.Timestamp(data["dates"][i_now]),
                    "entry_px_raw": pos["entry_px_raw"],
                    "exit_px_raw": float(exit_px_raw),
                    "sell_px_net": float(sell_px),
                    "shares": pos["shares"],
                    "buy_total_cost": pos["buy_total_cost"],
                    "proceeds": float(proceeds),
                    "pnl": float(pnl),
                    "net_ret": float(net_ret) if np.isfinite(net_ret) else np.nan,
                    "exit_reason": reason,
                    "pred": pos["pred"],
                    "label": pos["label"],
                }
            )
        positions = still_open

        sub = pred_map.get(pd.Timestamp(current_date))
        if sub is None or sub.empty:
            continue

        selected = _select_candidates(sub, cfg)
        if selected.empty:
            continue

        slots = max(0, int(cfg.max_positions) - len(positions))
        if slots <= 0 or cash <= 0:
            continue
        per_cash = float(cash / max(1, slots))
        day_entries = 0

        for _, row in selected.iterrows():
            if slots <= 0 or cash <= 0:
                break
            symbol = str(row["symbol"])
            if any(p["symbol"] == symbol for p in positions):
                continue
            data = ohlc_map.get(symbol)
            if data is None:
                continue
            idx_map = data["date_to_idx"]
            i_signal = idx_map.get(pd.Timestamp(current_date))
            if i_signal is None:
                continue
            i_entry = int(i_signal) + 1 if cfg.entry_mode == "next_open" else int(i_signal)
            if i_entry >= len(data["open"]) or i_entry + cfg.hold_days >= len(data["close"]):
                continue

            entry_px_raw = float(data["open"][i_entry]) if cfg.entry_mode == "next_open" else float(data["close"][i_entry])
            if not np.isfinite(entry_px_raw) or entry_px_raw <= 0:
                continue
            prev_close = float(data["close"][i_signal])
            if cfg.entry_mode == "next_open":
                if prev_close <= 0:
                    continue
                gap = entry_px_raw / prev_close - 1.0
                if cfg.gap_th is not None and gap > cfg.gap_th:
                    continue

            turnover = float(data["turnover"][i_entry]) if np.isfinite(data["turnover"][i_entry]) else np.nan
            if min_turnover > 0 and (not np.isfinite(turnover) or turnover < min_turnover):
                continue

            entry_px = entry_px_raw * (1.0 + cost_cfg.slippage)
            shares = per_cash / entry_px if per_cash > 0 else 0.0
            if shares <= 0:
                continue
            buy_value = shares * entry_px
            buy_fee = max(buy_value * eff_fee, cost_cfg.fee_min)
            total_cost = buy_value + buy_fee
            if max_turnover_pct > 0 and np.isfinite(turnover) and total_cost > turnover * max_turnover_pct:
                continue
            if total_cost > cash:
                shares = max((cash - cost_cfg.fee_min) / entry_px, 0.0)
                buy_value = shares * entry_px
                buy_fee = max(buy_value * eff_fee, cost_cfg.fee_min)
                total_cost = buy_value + buy_fee
                if shares <= 0 or total_cost > cash:
                    continue

            cash -= total_cost
            label_value = np.nan
            if label_col is not None and label_col in row.index and pd.notna(row[label_col]):
                label_value = float(row[label_col])
                label_hits.append(label_value)
            positions.append(
                {
                    "symbol": symbol,
                    "entry_idx": int(i_entry),
                    "entry_px_raw": float(entry_px_raw),
                    "peak_high": float(entry_px_raw),
                    "shares": float(shares),
                    "buy_total_cost": float(total_cost),
                    "signal_date": pd.Timestamp(current_date),
                    "entry_date": pd.Timestamp(data["dates"][i_entry]),
                    "pred": float(row["pred"]),
                    "label": label_value,
                }
            )
            selected_rows.append(row.to_dict())
            day_entries += 1
            slots -= 1
            pick_count += 1

        if day_entries > 0:
            signal_days += 1

    for pos in positions:
        data = ohlc_map[pos["symbol"]]
        i_end = min(int(pos["entry_idx"]) + cfg.hold_days, len(data["close"]) - 1)
        exit_px_raw = float(data["close"][i_end])
        sell_px = exit_px_raw * (1.0 - cost_cfg.slippage)
        sell_value = pos["shares"] * sell_px
        sell_fee = max(sell_value * eff_fee, cost_cfg.fee_min)
        tax = sell_value * cost_cfg.tax_rate_sell
        proceeds = sell_value - sell_fee - tax
        cash += proceeds

        pnl = proceeds - pos["buy_total_cost"]
        net_ret = pnl / pos["buy_total_cost"] if pos["buy_total_cost"] > 0 else np.nan
        trades.append(
            {
                "symbol": pos["symbol"],
                "signal_date": pos["signal_date"],
                "entry_date": pos["entry_date"],
                "exit_date": pd.Timestamp(data["dates"][i_end]),
                "entry_px_raw": pos["entry_px_raw"],
                "exit_px_raw": float(exit_px_raw),
                "sell_px_net": float(sell_px),
                "shares": pos["shares"],
                "buy_total_cost": pos["buy_total_cost"],
                "proceeds": float(proceeds),
                "pnl": float(pnl),
                "net_ret": float(net_ret) if np.isfinite(net_ret) else np.nan,
                "exit_reason": "FORCE_CLOSE",
                "pred": pos["pred"],
                "label": pos["label"],
            }
        )

    trades_df = pd.DataFrame(trades).sort_values(["signal_date", "symbol"]).reset_index(drop=True)
    summary = {
        "trades": int(len(trades_df)),
        "signals": int(signal_days),
        "coverage_days": int(signal_days),
        "pick_count": int(pick_count),
        "avg_picks_signal_day": float(pick_count / signal_days) if signal_days else np.nan,
        "label_hit": float(np.mean(label_hits)) if label_hits else np.nan,
        "mean_net_ret": float(trades_df["net_ret"].mean()) if not trades_df.empty else np.nan,
        "median_net_ret": float(trades_df["net_ret"].median()) if not trades_df.empty else np.nan,
        "win_net_ret": float((trades_df["net_ret"] > 0).mean()) if not trades_df.empty else np.nan,
        "final_capital": float(cash),
        "total_net_ret": float(cash / capital - 1.0) if capital > 0 else np.nan,
    }
    selected_df = pd.DataFrame(selected_rows).sort_values(["date", "pred"], ascending=[True, False]).reset_index(drop=True)
    return ReplayArtifacts(summary=summary, trades=trades_df, selected=selected_df)


def _build_day_quality(selected: pd.DataFrame, trades: pd.DataFrame) -> pd.DataFrame:
    if selected.empty:
        return pd.DataFrame(
            columns=[
                "signal_date",
                "trades",
                "mean_trade_ret",
                "win_day",
                "mean_pred",
                "min_pred",
                "mean_buy20",
                "min_buy20",
                "mean_whhi20",
                "max_whhi20",
                "mean_dynk",
                "min_dynk",
                "mean_topratio",
                "max_topratio",
            ]
        )

    work = selected.copy()
    work["date"] = pd.to_datetime(work["date"])
    grouped = work.groupby("date", as_index=False).agg(
        trades=("symbol", "size"),
        mean_pred=("pred", "mean"),
        min_pred=("pred", "min"),
        mean_buy20=("stock_net_buy_days_20", "mean"),
        min_buy20=("stock_net_buy_days_20", "min"),
        mean_whhi20=("warrant_hhi_posnet_20", "mean"),
        max_whhi20=("warrant_hhi_posnet_20", "max"),
        mean_dynk=("stock_dyn_k", "mean"),
        min_dynk=("stock_dyn_k", "min"),
        mean_topratio=("top_posnet_ratio", "mean"),
        max_topratio=("top_posnet_ratio", "max"),
    )
    if trades.empty:
        trade_day = grouped[["date"]].copy()
        trade_day["mean_trade_ret"] = np.nan
        trade_day["win_day"] = np.nan
    else:
        trade_day = trades.copy()
        trade_day["signal_date"] = pd.to_datetime(trade_day["signal_date"])
        trade_day = trade_day.groupby("signal_date", as_index=False).agg(
            mean_trade_ret=("net_ret", "mean"),
        )
        trade_day["win_day"] = (trade_day["mean_trade_ret"] > 0).astype(float)
        trade_day = trade_day.rename(columns={"signal_date": "date"})

    out = grouped.merge(trade_day, on="date", how="left").rename(columns={"date": "signal_date"})
    return out.sort_values("signal_date").reset_index(drop=True)


def _summary_payload(cfg: BacktestConfig, start: pd.Timestamp, end: pd.Timestamp, summary: dict[str, object], trades_path: Path) -> dict[str, object]:
    return {
        "config": {
            "select_mode": cfg.select_mode,
            "topk": cfg.topk,
            "top_pct": cfg.top_pct,
            "entry_mode": cfg.entry_mode,
            "gap_th": cfg.gap_th,
            "hold_days": cfg.hold_days,
            "tp": cfg.tp,
            "sl": cfg.sl,
            "trail": cfg.trail,
            "alloc_mode": cfg.alloc_mode,
            "fixed_cash": cfg.fixed_cash,
            "max_positions": cfg.max_positions,
        },
        "window": {"start": str(start.date()), "end": str(end.date())},
        "summary": summary,
        "artifacts": {"trades_csv": str(trades_path)},
    }


def _write_replay_outputs(
    trades: pd.DataFrame,
    summary: dict[str, object],
    cfg: BacktestConfig,
    start: pd.Timestamp,
    end: pd.Timestamp,
    trades_path: Path,
    summary_path: Path,
) -> None:
    trades_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    trades.to_csv(trades_path, index=False)
    payload = _summary_payload(cfg, start, end, summary, trades_path)
    summary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _legacy_rolling_windows() -> list[tuple[str, str, str]]:
    return [
        ("w1", "2025-10-01", "2025-11-15"),
        ("w2", "2025-10-15", "2025-11-30"),
        ("w3", "2025-11-01", "2025-12-15"),
        ("w4", "2025-11-15", "2025-12-31"),
        ("w5", "2025-12-01", "2026-01-15"),
        ("w6", "2025-12-15", "2026-02-03"),
    ]


def _build_rolling_windows(
    preds: pd.DataFrame,
    label_col: str | None,
    ohlc_map: dict[str, dict[str, object]],
    cfg: BacktestConfig,
    cost_cfg: CostConfig,
    capital: float,
    min_turnover: float,
    max_turnover_pct: float,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for window, start, end in _legacy_rolling_windows():
        artifacts = _replay_with_selected(
            preds,
            label_col,
            ohlc_map,
            cfg,
            cost_cfg,
            pd.Timestamp(start),
            pd.Timestamp(end),
            capital,
            min_turnover,
            max_turnover_pct,
        )
        rows.append(
            {
                "window": window,
                "trades": int(artifacts.summary["trades"]),
                "signals": int(artifacts.summary["signals"]),
                "win": float(artifacts.summary["win_net_ret"]) if not pd.isna(artifacts.summary["win_net_ret"]) else np.nan,
                "mean_net": float(artifacts.summary["mean_net_ret"]) if not pd.isna(artifacts.summary["mean_net_ret"]) else np.nan,
                "total_net": float(artifacts.summary["total_net_ret"]) if not pd.isna(artifacts.summary["total_net_ret"]) else np.nan,
            }
        )
    return pd.DataFrame(rows)


def _build_latest_live_candidates(
    preds: pd.DataFrame,
    label_col: str | None,
    cfg: BacktestConfig,
    repair_stock_buy20_min: float,
    repair_warrant_hhi20_max: float,
    topratio_max: float,
    day_buy20_min: float,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, object]]:
    if preds.empty:
        empty = pd.DataFrame(columns=["symbol", "date", "pred"])
        return empty, empty, empty, {
            "latest_signal_date": None,
            "repair_count": 0,
            "topratio_count": 0,
            "selected_count_pre_gap": 0,
            "mean_buy20_selected": None,
            "mean_topratio_selected": None,
            "passes_day_buy20_gate": False,
            "candidate_symbols": [],
            "note": "No latest predictions available.",
        }

    latest_date = pd.Timestamp(preds["date"].max())
    latest = preds[preds["date"] == latest_date].copy().sort_values("pred", ascending=False).reset_index(drop=True)
    repair = latest[
        (latest["stock_net_buy_days_20"] >= repair_stock_buy20_min)
        & (latest["warrant_hhi_posnet_20"] <= repair_warrant_hhi20_max)
    ].copy()
    topratio = repair[repair["top_posnet_ratio"] <= topratio_max].copy()
    selected = _select_candidates(topratio, cfg).copy()

    mean_buy20 = float(selected["stock_net_buy_days_20"].mean()) if not selected.empty else np.nan
    mean_topratio = float(selected["top_posnet_ratio"].mean()) if not selected.empty else np.nan
    passes_day_gate = bool(not selected.empty and np.isfinite(mean_buy20) and mean_buy20 >= day_buy20_min)
    final_candidates = selected.copy() if passes_day_gate else selected.iloc[0:0].copy()

    summary = {
        "latest_signal_date": str(latest_date.date()),
        "repair_count": int(len(repair)),
        "topratio_count": int(len(topratio)),
        "selected_count_pre_gap": int(len(selected)),
        "mean_buy20_selected": mean_buy20 if np.isfinite(mean_buy20) else None,
        "mean_topratio_selected": mean_topratio if np.isfinite(mean_topratio) else None,
        "passes_day_buy20_gate": passes_day_gate,
        "candidate_symbols": final_candidates["symbol"].astype(str).tolist(),
        "note": "Candidates are pre-open only; next-open gap filter still applies at execution time.",
    }
    return repair, topratio, final_candidates, summary


def main() -> None:
    ap = argparse.ArgumentParser(description="Build the full direction-3 artifact chain")
    ap.add_argument("--preds", type=Path, default=ML_RUNS / "breakout10_predictions_wf.csv")
    ap.add_argument("--scored", type=Path, default=DATA_DERIVED / "scored.parquet")
    ap.add_argument("--flow-stock", type=Path, default=DATA_DERIVED / "flow_stock_daily.parquet")
    ap.add_argument("--flow-warrant", type=Path, default=DATA_DERIVED / "flow_warrant_daily_by_underlying.parquet")
    ap.add_argument("--ohlc", type=Path, default=DATA_DERIVED / "ohlc.parquet")
    ap.add_argument("--start", default="2025-11-01")
    ap.add_argument("--end", default="")
    ap.add_argument("--repair-stock-buy20-min", type=float, default=1.0)
    ap.add_argument("--repair-warrant-hhi20-max", type=float, default=0.80)
    ap.add_argument("--topratio-max", type=float, default=0.610)
    ap.add_argument("--day-buy20-min", type=float, default=4.0)
    ap.add_argument("--capital", type=float, default=100000.0)
    ap.add_argument("--gap-th", type=float, default=0.005)
    ap.add_argument("--hold-days", type=int, default=16)
    ap.add_argument("--topk", type=int, default=3)
    ap.add_argument("--top-pct", type=float, default=0.015)
    ap.add_argument("--max-positions", type=int, default=6)
    ap.add_argument("--min-turnover", type=float, default=0.0)
    ap.add_argument("--max-turnover-pct", type=float, default=0.05)

    ap.add_argument("--out-repair-preds", type=Path, default=ML_RUNS / "breakout10_predictions_wf_repair_branch.csv")
    ap.add_argument("--out-topratio-preds", type=Path, default=ML_RUNS / "breakout10_predictions_wf_repair_branch_topratio0610.csv")
    ap.add_argument("--out-day-quality", type=Path, default=ML_RUNS / "repair_branch_topratio0610_day_chip_quality.csv")
    ap.add_argument("--out-day-filter-preds", type=Path, default=ML_RUNS / "breakout10_predictions_wf_repair_branch_topratio0610_daybuy20ge4.csv")
    ap.add_argument("--out-topratio-trades", type=Path, default=ML_RUNS / "repair_branch_topratio0610_best_trades.csv")
    ap.add_argument("--out-topratio-summary", type=Path, default=ML_RUNS / "repair_branch_topratio0610_best_summary.json")
    ap.add_argument("--out-topratio-rolling", type=Path, default=ML_RUNS / "repair_branch_topratio0610_best_rolling_windows.csv")
    ap.add_argument("--out-final-trades", type=Path, default=ML_RUNS / "repair_branch_topratio0610_daybuy20ge4_trades.csv")
    ap.add_argument("--out-final-summary", type=Path, default=ML_RUNS / "repair_branch_topratio0610_daybuy20ge4_summary.json")
    ap.add_argument("--out-final-rolling", type=Path, default=ML_RUNS / "repair_branch_topratio0610_daybuy20ge4_rolling_windows.csv")
    ap.add_argument("--latest-preds", type=Path, default=ML_RUNS / "breakout10_latest_wf.csv")
    ap.add_argument("--out-latest-repair-preds", type=Path, default=ML_RUNS / "breakout10_latest_wf_repair_branch.csv")
    ap.add_argument("--out-latest-topratio-preds", type=Path, default=ML_RUNS / "breakout10_latest_wf_repair_branch_topratio0610.csv")
    ap.add_argument("--out-latest-final-candidates", type=Path, default=ML_RUNS / "breakout10_latest_wf_repair_branch_topratio0610_daybuy20ge4_candidates.csv")
    ap.add_argument("--out-latest-summary", type=Path, default=ML_RUNS / "breakout10_latest_wf_repair_branch_topratio0610_daybuy20ge4_summary.json")
    args = ap.parse_args()

    start = pd.Timestamp(args.start)
    preds, label_col = _load_breakout_predictions(args.preds)
    features = _build_repair_features(args.scored, args.flow_stock, args.flow_warrant)
    merged = _merge_predictions_with_features(preds, features)
    ohlc_map = _load_ohlc_map(args.ohlc)
    cfg = BacktestConfig(
        select_mode="top_pct",
        topk=int(args.topk),
        top_pct=float(args.top_pct),
        entry_mode="next_open",
        gap_th=float(args.gap_th),
        hold_days=int(args.hold_days),
        tp=None,
        sl=None,
        trail=None,
        alloc_mode="equal",
        fixed_cash=None,
        max_positions=int(args.max_positions),
    )
    cost_cfg = CostConfig(
        fee_rate=0.001425,
        fee_discount=0.28,
        fee_min=20.0,
        tax_rate_sell=0.003,
        slippage=0.0005,
    )
    auto_end = _latest_complete_signal_date(ohlc_map, cfg.hold_days)
    end = pd.Timestamp(args.end) if str(args.end).strip() else auto_end
    if end is None:
        raise RuntimeError("unable to derive a complete replay end date from OHLC coverage")
    end = min(end, pd.Timestamp(preds["date"].max()))

    repair = merged[
        (merged["stock_net_buy_days_20"] >= args.repair_stock_buy20_min)
        & (merged["warrant_hhi_posnet_20"] <= args.repair_warrant_hhi20_max)
    ].copy()
    topratio = repair[repair["top_posnet_ratio"] <= args.topratio_max].copy()

    topratio_replay = _replay_with_selected(
        topratio,
        label_col,
        ohlc_map,
        cfg,
        cost_cfg,
        start,
        end,
        float(args.capital),
        float(args.min_turnover),
        float(args.max_turnover_pct),
    )
    day_quality = _build_day_quality(topratio_replay.selected, topratio_replay.trades)

    good_days = set(day_quality.loc[day_quality["mean_buy20"] >= args.day_buy20_min, "signal_date"])
    day_filtered = topratio[topratio["date"].isin(good_days)].copy()
    final_replay = _replay_with_selected(
        day_filtered,
        label_col,
        ohlc_map,
        cfg,
        cost_cfg,
        start,
        end,
        float(args.capital),
        float(args.min_turnover),
        float(args.max_turnover_pct),
    )

    topratio_rolling = _build_rolling_windows(
        topratio,
        label_col,
        ohlc_map,
        cfg,
        cost_cfg,
        float(args.capital),
        float(args.min_turnover),
        float(args.max_turnover_pct),
    )
    final_rolling = _build_rolling_windows(
        day_filtered,
        label_col,
        ohlc_map,
        cfg,
        cost_cfg,
        float(args.capital),
        float(args.min_turnover),
        float(args.max_turnover_pct),
    )

    _save_prediction_csv(repair, args.out_repair_preds, label_col)
    _save_prediction_csv(topratio, args.out_topratio_preds, label_col)
    _save_prediction_csv(day_filtered, args.out_day_filter_preds, label_col)
    args.out_day_quality.parent.mkdir(parents=True, exist_ok=True)
    day_quality.to_csv(args.out_day_quality, index=False)
    _write_replay_outputs(topratio_replay.trades, topratio_replay.summary, cfg, start, end, args.out_topratio_trades, args.out_topratio_summary)
    _write_replay_outputs(final_replay.trades, final_replay.summary, cfg, start, end, args.out_final_trades, args.out_final_summary)
    args.out_topratio_rolling.parent.mkdir(parents=True, exist_ok=True)
    args.out_final_rolling.parent.mkdir(parents=True, exist_ok=True)
    topratio_rolling.to_csv(args.out_topratio_rolling, index=False)
    final_rolling.to_csv(args.out_final_rolling, index=False)

    latest_preds, latest_label_col = _load_breakout_predictions(args.latest_preds)
    latest_merged = _merge_predictions_with_features(latest_preds, features)
    latest_repair, latest_topratio, latest_final, latest_summary = _build_latest_live_candidates(
        latest_merged,
        latest_label_col,
        cfg,
        float(args.repair_stock_buy20_min),
        float(args.repair_warrant_hhi20_max),
        float(args.topratio_max),
        float(args.day_buy20_min),
    )
    _save_prediction_csv(latest_repair, args.out_latest_repair_preds, latest_label_col)
    _save_prediction_csv(latest_topratio, args.out_latest_topratio_preds, latest_label_col)
    _save_prediction_csv(latest_final, args.out_latest_final_candidates, latest_label_col)
    _write_live_summary(args.out_latest_summary, latest_summary)

    print(f"Wrote: {args.out_repair_preds}")
    print(f"Wrote: {args.out_topratio_preds}")
    print(f"Wrote: {args.out_day_quality}")
    print(f"Wrote: {args.out_day_filter_preds}")
    print(f"Wrote: {args.out_topratio_trades}")
    print(f"Wrote: {args.out_topratio_summary}")
    print(f"Wrote: {args.out_topratio_rolling}")
    print(f"Wrote: {args.out_final_trades}")
    print(f"Wrote: {args.out_final_summary}")
    print(f"Wrote: {args.out_final_rolling}")
    print(f"Wrote: {args.out_latest_repair_preds}")
    print(f"Wrote: {args.out_latest_topratio_preds}")
    print(f"Wrote: {args.out_latest_final_candidates}")
    print(f"Wrote: {args.out_latest_summary}")


if __name__ == "__main__":
    main()
