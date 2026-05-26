#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Replay a single strategy config and export summary + trade details.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from apps.analysis.run_strategy_search_oof import (
    BacktestConfig,
    CostConfig,
    _effective_fee,
    _parse_exit,
    _select_candidates,
    build_pred_map,
    load_ohlc_map,
    load_predictions,
)


def simulate_period_with_trades(
    cfg: BacktestConfig,
    pred_map: dict[pd.Timestamp, pd.DataFrame],
    ohlc_map: dict[str, dict[str, np.ndarray | dict]],
    label_col: str | None,
    start: pd.Timestamp,
    end: pd.Timestamp,
    capital: float,
    cost_cfg: CostConfig,
    min_turnover: float,
    max_turnover_pct: float,
) -> tuple[dict[str, float], pd.DataFrame]:
    eff_fee = _effective_fee(cost_cfg.fee_rate, cost_cfg.fee_discount)
    dates = sorted([d for d in pred_map.keys() if start <= d <= end])
    if not dates:
        empty = {
            "trades": 0,
            "signals": 0,
            "coverage_days": 0,
            "pick_count": 0,
            "avg_picks_signal_day": np.nan,
            "label_hit": np.nan,
            "mean_net_ret": np.nan,
            "median_net_ret": np.nan,
            "win_net_ret": np.nan,
            "final_capital": capital,
            "total_net_ret": 0.0,
        }
        return empty, pd.DataFrame()

    cash = float(capital)
    positions: list[dict] = []
    trade_rows: list[dict] = []
    label_hits: list[float] = []
    signal_days = 0
    pick_count = 0

    for d in dates:
        still_open: list[dict] = []
        for pos in positions:
            data = ohlc_map[pos["symbol"]]
            idx_map = data["date_to_idx"]
            i_now = idx_map.get(pd.Timestamp(d))
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
            trade_rows.append(
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

        sub = pred_map.get(pd.Timestamp(d))
        if sub is None or sub.empty:
            continue

        selected = _select_candidates(sub, cfg)
        if selected.empty:
            continue

        slots = max(0, int(cfg.max_positions) - len(positions))
        if slots <= 0 or cash <= 0:
            continue

        if cfg.alloc_mode == "fixed" and cfg.fixed_cash is not None and cfg.fixed_cash > 0:
            per_cash = float(cfg.fixed_cash)
        else:
            per_cash = float(cash / max(1, slots))

        day_entries = 0
        for _, row in selected.iterrows():
            if slots <= 0 or cash <= 0:
                break
            sym = str(row["symbol"])
            if any(p["symbol"] == sym for p in positions):
                continue
            if sym not in ohlc_map:
                continue

            data = ohlc_map[sym]
            idx_map = data["date_to_idx"]
            i_signal = idx_map.get(pd.Timestamp(d))
            if i_signal is None:
                continue

            if cfg.entry_mode == "next_open":
                i_entry = int(i_signal) + 1
                if i_entry >= len(data["open"]):
                    continue
                entry_px_raw = float(data["open"][i_entry])
                prev_close = float(data["close"][i_signal])
                if prev_close <= 0:
                    continue
                gap = entry_px_raw / prev_close - 1.0
                if cfg.gap_th is not None and gap > cfg.gap_th:
                    continue
            else:
                i_entry = int(i_signal)
                entry_px_raw = float(data["close"][i_entry])

            if not np.isfinite(entry_px_raw) or entry_px_raw <= 0:
                continue
            if i_entry + cfg.hold_days >= len(data["close"]):
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
            positions.append(
                {
                    "symbol": sym,
                    "entry_idx": int(i_entry),
                    "entry_px_raw": float(entry_px_raw),
                    "peak_high": float(entry_px_raw),
                    "shares": float(shares),
                    "buy_total_cost": float(total_cost),
                    "signal_date": pd.Timestamp(d),
                    "entry_date": pd.Timestamp(data["dates"][i_entry]),
                    "pred": float(row["pred"]),
                    "label": float(row[label_col]) if label_col is not None and pd.notna(row.get(label_col)) else np.nan,
                }
            )
            if label_col is not None and pd.notna(row.get(label_col)):
                label_hits.append(float(row[label_col]))
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
        trade_rows.append(
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

    trades = pd.DataFrame(trade_rows)
    mean_net = float(trades["net_ret"].mean()) if not trades.empty else np.nan
    summary = {
        "trades": int(len(trades)),
        "signals": int(signal_days),
        "coverage_days": int(signal_days),
        "pick_count": int(pick_count),
        "avg_picks_signal_day": float(pick_count / signal_days) if signal_days else np.nan,
        "label_hit": float(np.mean(label_hits)) if label_hits else np.nan,
        "mean_net_ret": mean_net,
        "median_net_ret": float(trades["net_ret"].median()) if not trades.empty else np.nan,
        "win_net_ret": float((trades["net_ret"] > 0).mean()) if not trades.empty else np.nan,
        "final_capital": float(cash),
        "total_net_ret": float(cash / capital - 1.0) if capital > 0 else np.nan,
    }
    return summary, trades


def main() -> None:
    ap = argparse.ArgumentParser(description="Replay one strategy config and export trade details")
    ap.add_argument("--preds", type=str, required=True)
    ap.add_argument("--ohlc", type=str, default="data/_derived/ohlc.parquet")
    ap.add_argument("--symbol-len", type=int, default=4)
    ap.add_argument("--capital", type=float, default=100000.0)
    ap.add_argument("--start", type=str, required=True)
    ap.add_argument("--end", type=str, required=True)
    ap.add_argument("--select-mode", type=str, default="topk")
    ap.add_argument("--topk", type=int, default=4)
    ap.add_argument("--top-pct", type=float, default=0.0)
    ap.add_argument("--entry-mode", type=str, default="next_open")
    ap.add_argument("--gap-th", type=float, default=0.01)
    ap.add_argument("--hold-days", type=int, default=15)
    ap.add_argument("--tp", type=float, default=np.nan)
    ap.add_argument("--sl", type=float, default=np.nan)
    ap.add_argument("--trail", type=float, default=np.nan)
    ap.add_argument("--alloc-mode", type=str, default="equal")
    ap.add_argument("--fixed-cash", type=float, default=np.nan)
    ap.add_argument("--max-positions", type=int, default=5)
    ap.add_argument("--fee-rate", type=float, default=0.001425)
    ap.add_argument("--fee-discount", type=float, default=0.28)
    ap.add_argument("--fee-min", type=float, default=20.0)
    ap.add_argument("--tax-rate-sell", type=float, default=0.003)
    ap.add_argument("--slippage", type=float, default=0.0005)
    ap.add_argument("--min-turnover", type=float, default=0.0)
    ap.add_argument("--max-turnover-pct", type=float, default=0.05)
    ap.add_argument("--out-trades", type=str, required=True)
    ap.add_argument("--out-summary", type=str, required=True)
    args = ap.parse_args()

    preds, label_col = load_predictions(Path(args.preds), args.symbol_len)
    pred_map = build_pred_map(preds, label_col)
    ohlc_map = load_ohlc_map(Path(args.ohlc), args.symbol_len)

    cfg = BacktestConfig(
        select_mode=args.select_mode,
        topk=int(args.topk),
        top_pct=float(args.top_pct),
        entry_mode=args.entry_mode,
        gap_th=float(args.gap_th) if np.isfinite(args.gap_th) else None,
        hold_days=int(args.hold_days),
        tp=float(args.tp) if np.isfinite(args.tp) else None,
        sl=float(args.sl) if np.isfinite(args.sl) else None,
        trail=float(args.trail) if np.isfinite(args.trail) else None,
        alloc_mode=args.alloc_mode,
        fixed_cash=float(args.fixed_cash) if np.isfinite(args.fixed_cash) else None,
        max_positions=int(args.max_positions),
    )
    cost_cfg = CostConfig(
        fee_rate=float(args.fee_rate),
        fee_discount=float(args.fee_discount),
        fee_min=float(args.fee_min),
        tax_rate_sell=float(args.tax_rate_sell),
        slippage=float(args.slippage),
    )
    summary, trades = simulate_period_with_trades(
        cfg,
        pred_map,
        ohlc_map,
        label_col,
        pd.to_datetime(args.start),
        pd.to_datetime(args.end),
        float(args.capital),
        cost_cfg,
        float(args.min_turnover),
        float(args.max_turnover_pct),
    )

    out_trades = Path(args.out_trades)
    out_trades.parent.mkdir(parents=True, exist_ok=True)
    trades.to_csv(out_trades, index=False)

    payload = {
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
        "window": {"start": str(args.start), "end": str(args.end)},
        "summary": summary,
        "artifacts": {"trades_csv": str(out_trades)},
    }
    out_summary = Path(args.out_summary)
    out_summary.parent.mkdir(parents=True, exist_ok=True)
    out_summary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote: {out_trades}")
    print(f"Wrote: {out_summary}")


if __name__ == "__main__":
    main()
