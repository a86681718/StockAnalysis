#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OOF-oriented strategy search with net-return objective.

Key design:
- Evaluate only on OOF / walk-forward prediction files (avoid in-sample bias)
- Optimize by net 10d return after trading frictions
- Support quantile-based daily selection to reduce score-drift issues
- Hybrid exits: time stop + risk stop + trailing stop (+ optional TP)
"""

from __future__ import annotations

import argparse
import math
import os
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd


@dataclass
class BacktestConfig:
    select_mode: str  # topk | top_pct
    topk: int
    top_pct: float
    entry_mode: str  # next_open | close
    gap_th: float | None
    hold_days: int
    tp: float | None
    sl: float | None
    trail: float | None
    alloc_mode: str  # equal | fixed
    fixed_cash: float | None
    max_positions: int


@dataclass
class CostConfig:
    fee_rate: float
    fee_discount: float
    fee_min: float
    tax_rate_sell: float
    slippage: float


_G_PRED_MAP: Dict[pd.Timestamp, pd.DataFrame] | None = None
_G_OHLC_MAP: Dict[str, Dict[str, np.ndarray | dict]] | None = None
_G_LABEL_COL: str | None = None
_G_TRAIN_START: pd.Timestamp | None = None
_G_TRAIN_END: pd.Timestamp | None = None
_G_VALID_START: pd.Timestamp | None = None
_G_VALID_END: pd.Timestamp | None = None
_G_CAPITAL: float | None = None
_G_COST: CostConfig | None = None
_G_AUX_WEIGHT: float | None = None
_G_MIN_TURNOVER: float | None = None
_G_MAX_TURNOVER_PCT: float | None = None


def parse_float_list(s: str) -> List[float]:
    return [float(x) for x in s.split(",") if x.strip()]


def parse_int_list(s: str) -> List[int]:
    return [int(float(x)) for x in s.split(",") if x.strip()]


def load_predictions(pred_path: Path, symbol_len: int) -> tuple[pd.DataFrame, str | None]:
    df = pd.read_csv(pred_path, low_memory=False)
    if df.empty:
        raise RuntimeError(f"preds csv is empty: {pred_path}")
    required = {"symbol", "date", "pred"}
    if not required.issubset(df.columns):
        raise ValueError(f"preds csv missing required columns {required}")

    df["symbol"] = df["symbol"].astype(str)
    df["date"] = pd.to_datetime(df["date"])
    df["pred"] = pd.to_numeric(df["pred"], errors="coerce")
    df = df.dropna(subset=["symbol", "date", "pred"]).copy()
    if symbol_len > 0:
        df = df[df["symbol"].str.len() == symbol_len].copy()

    extra_cols = [c for c in df.columns if c not in {"symbol", "date", "pred"}]
    label_col = extra_cols[0] if extra_cols else None
    if label_col is not None:
        df[label_col] = pd.to_numeric(df[label_col], errors="coerce")
    return df, label_col


def load_ohlc_map(ohlc_path: Path, symbol_len: int) -> Dict[str, Dict[str, np.ndarray | dict]]:
    want_cols = ["symbol", "date", "open", "high", "low", "close", "volume"]
    try:
        ohlc = pd.read_parquet(ohlc_path, columns=want_cols)
    except Exception:
        ohlc = pd.read_parquet(ohlc_path)
        keep = [c for c in want_cols if c in ohlc.columns]
        ohlc = ohlc[keep].copy()

    for c in ["open", "high", "low", "close"]:
        if c not in ohlc.columns:
            raise ValueError(f"ohlc missing required column: {c}")
    if "volume" not in ohlc.columns:
        ohlc["volume"] = np.nan

    ohlc["symbol"] = ohlc["symbol"].astype(str)
    if symbol_len > 0:
        ohlc = ohlc[ohlc["symbol"].str.len() == symbol_len].copy()
    ohlc["date"] = pd.to_datetime(ohlc["date"])
    for c in ["open", "high", "low", "close", "volume"]:
        ohlc[c] = pd.to_numeric(ohlc[c], errors="coerce")
    ohlc = ohlc.dropna(subset=["open", "high", "low", "close"]).copy()

    out: Dict[str, Dict[str, np.ndarray | dict]] = {}
    for sym, sub in ohlc.groupby("symbol"):
        sub = sub.sort_values("date").reset_index(drop=True)
        dates = sub["date"].to_numpy(dtype="datetime64[ns]")
        arr_open = sub["open"].to_numpy(float)
        arr_high = sub["high"].to_numpy(float)
        arr_low = sub["low"].to_numpy(float)
        arr_close = sub["close"].to_numpy(float)
        arr_vol = sub["volume"].to_numpy(float)
        turnover = arr_close * np.where(np.isfinite(arr_vol), arr_vol, np.nan)
        date_to_idx = {pd.Timestamp(d): int(i) for i, d in enumerate(dates)}
        out[sym] = {
            "dates": dates,
            "open": arr_open,
            "high": arr_high,
            "low": arr_low,
            "close": arr_close,
            "volume": arr_vol,
            "turnover": turnover,
            "date_to_idx": date_to_idx,
        }
    return out


def build_pred_map(preds: pd.DataFrame, label_col: str | None) -> Dict[pd.Timestamp, pd.DataFrame]:
    cols = ["symbol", "date", "pred"]
    if label_col is not None and label_col in preds.columns:
        cols.append(label_col)
    preds = preds[cols].copy()
    out: Dict[pd.Timestamp, pd.DataFrame] = {}
    for d, sub in preds.groupby("date"):
        out[pd.Timestamp(d)] = sub.sort_values("pred", ascending=False).reset_index(drop=True)
    return out


def _effective_fee(rate: float, discount: float) -> float:
    return rate * discount


def _select_candidates(sub: pd.DataFrame, cfg: BacktestConfig) -> pd.DataFrame:
    if sub.empty:
        return sub
    if cfg.select_mode == "topk":
        return sub.head(max(1, int(cfg.topk))).copy()
    n = max(1, int(math.ceil(len(sub) * cfg.top_pct)))
    if cfg.topk > 0:
        n = min(n, int(cfg.topk))
    return sub.head(n).copy()


def _parse_exit(
    cfg: BacktestConfig,
    pos: dict,
    i_now: int,
    high_i: float,
    low_i: float,
    close_i: float,
) -> tuple[bool, float, str]:
    if i_now <= pos["entry_idx"]:
        return False, np.nan, ""

    entry_px = pos["entry_px_raw"]
    reason = ""
    exit_px = np.nan

    sl_px = entry_px * (1.0 + cfg.sl) if cfg.sl is not None else np.nan
    tp_px = entry_px * (1.0 + cfg.tp) if cfg.tp is not None else np.nan

    # Conservative intraday sequence: SL -> TRAIL -> TP.
    if cfg.sl is not None and np.isfinite(sl_px) and low_i <= sl_px:
        reason = "SL"
        exit_px = sl_px
    else:
        if cfg.trail is not None:
            pos["peak_high"] = max(float(pos["peak_high"]), float(high_i))
            tr_px = float(pos["peak_high"]) * (1.0 - cfg.trail)
            if low_i <= tr_px:
                reason = "TRAIL"
                exit_px = tr_px
        if reason == "" and cfg.tp is not None and np.isfinite(tp_px) and high_i >= tp_px:
            reason = "TP"
            exit_px = tp_px

    hold_elapsed = i_now - int(pos["entry_idx"])
    if reason == "" and hold_elapsed >= cfg.hold_days:
        reason = "TIME"
        exit_px = float(close_i)

    if reason == "":
        return False, np.nan, ""
    return True, float(exit_px), reason


def simulate_period(
    cfg: BacktestConfig,
    pred_map: Dict[pd.Timestamp, pd.DataFrame],
    ohlc_map: Dict[str, Dict[str, np.ndarray | dict]],
    label_col: str | None,
    start: pd.Timestamp,
    end: pd.Timestamp,
    capital: float,
    cost_cfg: CostConfig,
    aux_weight: float,
    min_turnover: float,
    max_turnover_pct: float,
) -> Dict[str, float]:
    eff_fee = _effective_fee(cost_cfg.fee_rate, cost_cfg.fee_discount)
    dates = sorted([d for d in pred_map.keys() if start <= d <= end])
    if not dates:
        return {
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
            "objective": np.nan,
        }

    cash = float(capital)
    positions: List[dict] = []
    trade_rets: List[float] = []
    label_hits: List[float] = []
    signal_days = 0
    pick_count = 0

    for d in dates:
        # Exit checks
        still_open: List[dict] = []
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
            if np.isfinite(net_ret):
                trade_rets.append(float(net_ret))
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

        day_entries = 0
        if cfg.alloc_mode == "fixed" and cfg.fixed_cash is not None and cfg.fixed_cash > 0:
            per_cash = float(cfg.fixed_cash)
        else:
            per_cash = float(cash / max(1, slots))

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

            # Need enough bars for max hold.
            if i_entry + cfg.hold_days >= len(data["close"]):
                continue

            turnover = float(data["turnover"][i_entry]) if np.isfinite(data["turnover"][i_entry]) else np.nan
            if min_turnover > 0 and (not np.isfinite(turnover) or turnover < min_turnover):
                continue

            entry_px = entry_px_raw * (1.0 + cost_cfg.slippage)
            if entry_px <= 0:
                continue

            shares = per_cash / entry_px if per_cash > 0 else 0.0
            if shares <= 0:
                continue
            buy_value = shares * entry_px
            buy_fee = max(buy_value * eff_fee, cost_cfg.fee_min)
            total_cost = buy_value + buy_fee
            if max_turnover_pct > 0 and np.isfinite(turnover) and total_cost > turnover * max_turnover_pct:
                continue
            if total_cost > cash:
                # Try shrinking to available cash while respecting fee minimum.
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
                    "entry_date": pd.Timestamp(data["dates"][i_entry]),
                }
            )
            day_entries += 1
            slots -= 1
            pick_count += 1

            if label_col is not None and label_col in row:
                y = row[label_col]
                if pd.notna(y):
                    label_hits.append(float(y))

        if day_entries > 0:
            signal_days += 1

    # Force close at last available close within period.
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
        if np.isfinite(net_ret):
            trade_rets.append(float(net_ret))

    r = pd.Series(trade_rets, dtype=float)
    mean_net = float(r.mean()) if len(r) else np.nan
    label_hit = float(np.mean(label_hits)) if label_hits else np.nan
    obj = mean_net
    if np.isfinite(label_hit):
        obj = obj + aux_weight * label_hit

    return {
        "trades": int(len(r)),
        "signals": int(signal_days),
        "coverage_days": int(signal_days),
        "pick_count": int(pick_count),
        "avg_picks_signal_day": float(pick_count / signal_days) if signal_days else np.nan,
        "label_hit": label_hit,
        "mean_net_ret": mean_net,
        "median_net_ret": float(r.median()) if len(r) else np.nan,
        "win_net_ret": float((r > 0).mean()) if len(r) else np.nan,
        "final_capital": float(cash),
        "total_net_ret": float(cash / capital - 1.0) if capital > 0 else np.nan,
        "objective": float(obj) if np.isfinite(obj) else np.nan,
    }


def _set_globals(
    pred_map: Dict[pd.Timestamp, pd.DataFrame],
    ohlc_map: Dict[str, Dict[str, np.ndarray | dict]],
    label_col: str | None,
    train_start: pd.Timestamp,
    train_end: pd.Timestamp,
    valid_start: pd.Timestamp,
    valid_end: pd.Timestamp,
    capital: float,
    cost_cfg: CostConfig,
    aux_weight: float,
    min_turnover: float,
    max_turnover_pct: float,
) -> None:
    global _G_PRED_MAP, _G_OHLC_MAP, _G_LABEL_COL, _G_TRAIN_START, _G_TRAIN_END
    global _G_VALID_START, _G_VALID_END, _G_CAPITAL, _G_COST, _G_AUX_WEIGHT
    global _G_MIN_TURNOVER, _G_MAX_TURNOVER_PCT
    _G_PRED_MAP = pred_map
    _G_OHLC_MAP = ohlc_map
    _G_LABEL_COL = label_col
    _G_TRAIN_START = train_start
    _G_TRAIN_END = train_end
    _G_VALID_START = valid_start
    _G_VALID_END = valid_end
    _G_CAPITAL = capital
    _G_COST = cost_cfg
    _G_AUX_WEIGHT = aux_weight
    _G_MIN_TURNOVER = min_turnover
    _G_MAX_TURNOVER_PCT = max_turnover_pct


def _init_worker(
    preds_path: str,
    ohlc_path: str,
    symbol_len: int,
    oof_only: bool,
    train_start: pd.Timestamp,
    train_end: pd.Timestamp,
    valid_start: pd.Timestamp,
    valid_end: pd.Timestamp,
    capital: float,
    cost_cfg: CostConfig,
    aux_weight: float,
    min_turnover: float,
    max_turnover_pct: float,
) -> None:
    preds_path_obj = Path(preds_path)
    if oof_only and "all" in preds_path_obj.stem.lower():
        raise RuntimeError("oof-only mode rejects preds file containing 'all' in name")
    preds, label_col = load_predictions(preds_path_obj, symbol_len)
    pred_map = build_pred_map(preds, label_col)
    ohlc_map = load_ohlc_map(Path(ohlc_path), symbol_len)
    _set_globals(
        pred_map,
        ohlc_map,
        label_col,
        train_start,
        train_end,
        valid_start,
        valid_end,
        capital,
        cost_cfg,
        aux_weight,
        min_turnover,
        max_turnover_pct,
    )


def _run_one(cfg: BacktestConfig) -> dict:
    if _G_PRED_MAP is None or _G_OHLC_MAP is None or _G_COST is None:
        raise RuntimeError("worker globals not initialized")

    train = simulate_period(
        cfg,
        _G_PRED_MAP,
        _G_OHLC_MAP,
        _G_LABEL_COL,
        _G_TRAIN_START,
        _G_TRAIN_END,
        float(_G_CAPITAL),
        _G_COST,
        float(_G_AUX_WEIGHT),
        float(_G_MIN_TURNOVER),
        float(_G_MAX_TURNOVER_PCT),
    )
    valid = simulate_period(
        cfg,
        _G_PRED_MAP,
        _G_OHLC_MAP,
        _G_LABEL_COL,
        _G_VALID_START,
        _G_VALID_END,
        float(_G_CAPITAL),
        _G_COST,
        float(_G_AUX_WEIGHT),
        float(_G_MIN_TURNOVER),
        float(_G_MAX_TURNOVER_PCT),
    )

    return {
        "select_mode": cfg.select_mode,
        "topk": cfg.topk,
        "top_pct": cfg.top_pct if cfg.top_pct > 0 else "",
        "entry": cfg.entry_mode,
        "gap_th": cfg.gap_th if cfg.gap_th is not None else "",
        "hold_days": cfg.hold_days,
        "tp": cfg.tp if cfg.tp is not None else "",
        "sl": cfg.sl if cfg.sl is not None else "",
        "trail": cfg.trail if cfg.trail is not None else "",
        "alloc_mode": cfg.alloc_mode,
        "fixed_cash": cfg.fixed_cash if cfg.fixed_cash is not None else "",
        "max_positions": cfg.max_positions,
        "train_trades": train["trades"],
        "train_win": train["win_net_ret"],
        "train_mean_net": train["mean_net_ret"],
        "train_total_net": train["total_net_ret"],
        "train_label_hit": train["label_hit"],
        "train_obj": train["objective"],
        "valid_trades": valid["trades"],
        "valid_win": valid["win_net_ret"],
        "valid_mean_net": valid["mean_net_ret"],
        "valid_total_net": valid["total_net_ret"],
        "valid_label_hit": valid["label_hit"],
        "valid_obj": valid["objective"],
        "valid_signals": valid["signals"],
        "valid_avg_picks_signal_day": valid["avg_picks_signal_day"],
    }


def _progress(i: int, total: int, t0: float, every: int) -> None:
    if i == 1 or i == total or (every > 0 and i % every == 0):
        dt = time.perf_counter() - t0
        pct = i / total * 100.0
        eta = (dt / max(1, i)) * (total - i)
        print(f"[GRID] {i}/{total} ({pct:.1f}%) elapsed={dt:.1f}s eta={eta:.1f}s")


def build_grid(args) -> List[BacktestConfig]:
    topk_list = parse_int_list(args.topk_list)
    top_pct_list = parse_float_list(args.top_pct_list)
    gap_list = [None] + parse_float_list(args.gap_list)
    hold_list = list(range(args.hold_min, args.hold_max + 1, args.hold_step))
    tp_list = parse_float_list(args.tp_list)
    sl_list = [-abs(x) for x in parse_float_list(args.sl_list)]
    trail_list = parse_float_list(args.trail_list)
    fixed_cash_list = parse_float_list(args.fixed_cash_list)
    max_pos_list = parse_int_list(args.max_positions_list)
    select_modes = [x.strip() for x in args.select_modes.split(",") if x.strip()]
    exit_profiles = [x.strip() for x in args.exit_profiles.split(",") if x.strip()]
    alloc_modes = [x.strip() for x in args.alloc_modes.split(",") if x.strip()]

    grid: List[BacktestConfig] = []
    rng = np.random.default_rng(args.seed)

    def _add_cfgs(base_kwargs: dict) -> None:
        profile = base_kwargs.pop("profile")
        if profile == "time":
            grid.append(BacktestConfig(tp=None, sl=None, trail=None, **base_kwargs))
        elif profile == "sl":
            for sl in sl_list:
                grid.append(BacktestConfig(tp=None, sl=sl, trail=None, **base_kwargs))
        elif profile == "tp_sl":
            for tp in tp_list:
                for sl in sl_list:
                    grid.append(BacktestConfig(tp=tp, sl=sl, trail=None, **base_kwargs))
        elif profile == "sl_trail":
            for sl in sl_list:
                for tr in trail_list:
                    grid.append(BacktestConfig(tp=None, sl=sl, trail=tr, **base_kwargs))
        elif profile == "tp_sl_trail":
            for tp in tp_list:
                for sl in sl_list:
                    for tr in trail_list:
                        grid.append(BacktestConfig(tp=tp, sl=sl, trail=tr, **base_kwargs))
        else:
            raise ValueError(f"unknown exit profile: {profile}")

    for mode in select_modes:
        if mode == "topk":
            sel_pairs = [("topk", int(k), 0.0) for k in topk_list]
        elif mode == "top_pct":
            # topk acts as cap for dynamic count in top_pct mode.
            sel_pairs = [("top_pct", int(k), float(q)) for k in topk_list for q in top_pct_list]
        else:
            raise ValueError(f"unknown select mode: {mode}")

        for select_mode, topk, top_pct in sel_pairs:
            for max_pos in max_pos_list:
                for entry in [x.strip() for x in args.entry_modes.split(",") if x.strip()]:
                    for gap in gap_list:
                        for hold in hold_list:
                            for profile in exit_profiles:
                                for alloc in alloc_modes:
                                    if alloc == "fixed":
                                        for fc in fixed_cash_list:
                                            _add_cfgs(
                                                dict(
                                                    profile=profile,
                                                    select_mode=select_mode,
                                                    topk=topk,
                                                    top_pct=top_pct,
                                                    entry_mode=entry,
                                                    gap_th=gap,
                                                    hold_days=hold,
                                                    alloc_mode=alloc,
                                                    fixed_cash=fc,
                                                    max_positions=max_pos,
                                                )
                                            )
                                    else:
                                        _add_cfgs(
                                            dict(
                                                profile=profile,
                                                select_mode=select_mode,
                                                topk=topk,
                                                top_pct=top_pct,
                                                entry_mode=entry,
                                                gap_th=gap,
                                                hold_days=hold,
                                                alloc_mode=alloc,
                                                fixed_cash=None,
                                                max_positions=max_pos,
                                            )
                                        )

    if args.sample > 0 and args.sample < len(grid):
        idx = rng.choice(len(grid), size=args.sample, replace=False)
        grid = [grid[i] for i in idx]

    return grid


def select_today_candidates(
    preds_today_path: Path,
    symbol_len: int,
    best_cfg: BacktestConfig,
) -> pd.DataFrame:
    preds, label_col = load_predictions(preds_today_path, symbol_len)
    latest = preds["date"].max()
    sub = preds[preds["date"] == latest].sort_values("pred", ascending=False).reset_index(drop=True)
    sub["rank"] = np.arange(1, len(sub) + 1)
    pick = _select_candidates(sub, best_cfg).copy()
    pick["latest_date"] = latest
    keep = ["symbol", "date", "pred", "rank", "latest_date"]
    if label_col is not None and label_col in pick.columns:
        keep.append(label_col)
    return pick[keep]


def main() -> None:
    ap = argparse.ArgumentParser(description="OOF strategy search with net-return objective")
    ap.add_argument("--preds", type=str, default="data/_derived/ml_runs/days_above_ge5_predictions.csv")
    ap.add_argument("--preds-today", type=str, default="", help="optional predictions_all for latest candidate output")
    ap.add_argument("--ohlc", type=str, default="data/_derived/ohlc.parquet")
    ap.add_argument("--symbol-len", type=int, default=4)
    ap.add_argument("--oof-only", action="store_true", help="reject preds file containing 'all' in filename")
    ap.add_argument("--capital", type=float, default=100000.0)
    ap.add_argument("--valid-start", type=str, default="2025-12-01")
    ap.add_argument("--valid-end", type=str, default="2026-02-28")
    ap.add_argument("--aux-weight", type=float, default=0.01, help="small weight for label hit-rate as auxiliary")

    # Costs / frictions
    ap.add_argument("--fee-rate", type=float, default=0.001425)
    ap.add_argument("--fee-discount", type=float, default=0.28)
    ap.add_argument("--fee-min", type=float, default=20.0)
    ap.add_argument("--tax-rate-sell", type=float, default=0.003)
    ap.add_argument("--slippage", type=float, default=0.0005, help="per-side price slippage ratio")
    ap.add_argument("--min-turnover", type=float, default=0.0, help="min day turnover (close*volume) to allow entry")
    ap.add_argument(
        "--max-turnover-pct",
        type=float,
        default=0.05,
        help="max trade cash / day turnover ratio; 0 disables",
    )

    # Grid
    ap.add_argument("--select-modes", type=str, default="topk,top_pct")
    ap.add_argument("--topk-list", type=str, default="1,3,5,10")
    ap.add_argument("--top-pct-list", type=str, default="0.01,0.02,0.03,0.05")
    ap.add_argument("--entry-modes", type=str, default="next_open")
    ap.add_argument("--gap-list", type=str, default="0.01,0.02,0.03,0.04,0.05")
    ap.add_argument("--hold-min", type=int, default=5)
    ap.add_argument("--hold-max", type=int, default=15)
    ap.add_argument("--hold-step", type=int, default=1)
    ap.add_argument("--exit-profiles", type=str, default="time,sl,tp_sl,sl_trail,tp_sl_trail")
    ap.add_argument("--tp-list", type=str, default="0.03,0.05,0.08,0.10,0.12,0.15")
    ap.add_argument("--sl-list", type=str, default="0.03,0.05,0.08,0.10")
    ap.add_argument("--trail-list", type=str, default="0.03,0.05,0.08,0.10")
    ap.add_argument("--alloc-modes", type=str, default="equal,fixed")
    ap.add_argument("--fixed-cash-list", type=str, default="20000")
    ap.add_argument("--max-positions-list", type=str, default="5,10")
    ap.add_argument("--sample", type=int, default=0, help="randomly sample N configs from full grid")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--min-valid-trades", type=int, default=30)
    ap.add_argument("--topk-output", type=int, default=30)

    # Runtime
    ap.add_argument("--workers", type=int, default=0)
    ap.add_argument("--backend", type=str, default="process", choices=["process", "thread"])
    ap.add_argument("--progress-every", type=int, default=200)

    # Outputs
    ap.add_argument("--out", type=str, default="data/_derived/ml_runs/strategy_search_oof.csv")
    ap.add_argument("--out-best", type=str, default="data/_derived/ml_runs/strategy_search_oof_best.csv")
    ap.add_argument("--out-latest", type=str, default="data/_derived/ml_runs/strategy_search_oof_latest.csv")
    args = ap.parse_args()

    preds_path = Path(args.preds)
    if not preds_path.exists():
        raise FileNotFoundError(preds_path)
    if args.oof_only and "all" in preds_path.stem.lower():
        raise RuntimeError("oof-only mode rejects preds file containing 'all' in name")

    cost_cfg = CostConfig(
        fee_rate=float(args.fee_rate),
        fee_discount=float(args.fee_discount),
        fee_min=float(args.fee_min),
        tax_rate_sell=float(args.tax_rate_sell),
        slippage=float(args.slippage),
    )

    preds, label_col = load_predictions(preds_path, args.symbol_len)
    pred_map = build_pred_map(preds, label_col)
    ohlc_map = load_ohlc_map(Path(args.ohlc), args.symbol_len)

    all_dates = sorted(pred_map.keys())
    if not all_dates:
        raise RuntimeError("no prediction dates")

    valid_start = pd.to_datetime(args.valid_start)
    valid_end = pd.to_datetime(args.valid_end)
    train_start = all_dates[0]
    train_end = valid_start - pd.Timedelta(days=1)
    if train_end < train_start:
        raise RuntimeError("invalid train/valid split")

    _set_globals(
        pred_map,
        ohlc_map,
        label_col,
        train_start,
        train_end,
        valid_start,
        valid_end,
        float(args.capital),
        cost_cfg,
        float(args.aux_weight),
        float(args.min_turnover),
        float(args.max_turnover_pct),
    )

    grid = build_grid(args)
    if not grid:
        raise RuntimeError("empty grid")
    total = len(grid)
    print(f"Grid size: {total}")

    if args.workers <= 0:
        args.workers = max(1, (os.cpu_count() or 4) - 1)

    rows: List[dict] = []
    t0 = time.perf_counter()
    if args.workers == 1:
        for i, cfg in enumerate(grid, start=1):
            rows.append(_run_one(cfg))
            _progress(i, total, t0, args.progress_every)
    else:
        if args.backend == "thread":
            executor_cls = ThreadPoolExecutor
            executor_kwargs = {}
        else:
            executor_cls = ProcessPoolExecutor
            executor_kwargs = {
                "initializer": _init_worker,
                "initargs": (
                    str(preds_path),
                    str(Path(args.ohlc)),
                    int(args.symbol_len),
                    bool(args.oof_only),
                    train_start,
                    train_end,
                    valid_start,
                    valid_end,
                    float(args.capital),
                    cost_cfg,
                    float(args.aux_weight),
                    float(args.min_turnover),
                    float(args.max_turnover_pct),
                ),
            }

        with executor_cls(max_workers=args.workers, **executor_kwargs) as ex:
            futures = {ex.submit(_run_one, cfg): None for cfg in grid}
            done = 0
            for fut in as_completed(futures):
                rows.append(fut.result())
                done += 1
                _progress(done, total, t0, args.progress_every)

    df = pd.DataFrame(rows)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"Wrote: {out_path}")

    best = df[df["valid_trades"] >= args.min_valid_trades].copy()
    if best.empty:
        best = df.copy()
    best = best.sort_values(["valid_obj", "valid_win", "valid_total_net"], ascending=[False, False, False]).head(
        args.topk_output
    )
    out_best = Path(args.out_best)
    out_best.parent.mkdir(parents=True, exist_ok=True)
    best.to_csv(out_best, index=False)
    print(f"Wrote: {out_best}")

    if args.preds_today:
        today_path = Path(args.preds_today)
        if today_path.exists() and not best.empty:
            top = best.iloc[0]
            cfg = BacktestConfig(
                select_mode=str(top["select_mode"]),
                topk=int(top["topk"]),
                top_pct=float(top["top_pct"]) if top["top_pct"] != "" else 0.0,
                entry_mode=str(top["entry"]),
                gap_th=float(top["gap_th"]) if top["gap_th"] != "" else None,
                hold_days=int(top["hold_days"]),
                tp=float(top["tp"]) if top["tp"] != "" else None,
                sl=float(top["sl"]) if top["sl"] != "" else None,
                trail=float(top["trail"]) if top["trail"] != "" else None,
                alloc_mode=str(top["alloc_mode"]),
                fixed_cash=float(top["fixed_cash"]) if top["fixed_cash"] != "" else None,
                max_positions=int(top["max_positions"]),
            )
            latest = select_today_candidates(today_path, args.symbol_len, cfg)
            out_latest = Path(args.out_latest)
            out_latest.parent.mkdir(parents=True, exist_ok=True)
            latest.to_csv(out_latest, index=False)
            print(f"Wrote: {out_latest}")


if __name__ == "__main__":
    main()
