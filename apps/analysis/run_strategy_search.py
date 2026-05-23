#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Grid search trading strategies using prediction ranks.
Train/valid split by date ranges.
"""

from __future__ import annotations

import argparse
import multiprocessing as mp
import os
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd


@dataclass
class StrategyConfig:
    topn: int
    entry_mode: str  # "close" or "next_open"
    gap_th: float | None
    exit_mode: str   # "hold" or "tp_sl"
    hold_days: int
    tp: float | None
    sl: float | None
    trail_th: float | None
    atr_mult: float | None
    alloc_mode: str  # "equal" or "fixed"
    fixed_cash: float | None


_GLOBAL_RANK_MAP: Dict[pd.Timestamp, List[str]] | None = None
_GLOBAL_OHLC_MAP: Dict[str, Dict[str, object]] | None = None
_GLOBAL_TRAIN_START: pd.Timestamp | None = None
_GLOBAL_TRAIN_END: pd.Timestamp | None = None
_GLOBAL_VALID_START: pd.Timestamp | None = None
_GLOBAL_VALID_END: pd.Timestamp | None = None
_GLOBAL_CAPITAL: float | None = None
_GLOBAL_MAX_POS: int | None = None


def load_predictions(pred_path: Path) -> pd.DataFrame:
    df = pd.read_csv(pred_path, low_memory=False)
    if df.empty:
        raise RuntimeError("preds csv is empty")
    df["symbol"] = df["symbol"].astype(str)
    df["date"] = pd.to_datetime(df["date"])
    df = df[df["symbol"].str.len() == 4]
    df = df.dropna(subset=["symbol", "date", "pred"])
    return df


def load_ohlc(ohlc_path: Path, atr_len: int) -> Dict[str, Dict[str, object]]:
    ohlc = pd.read_parquet(ohlc_path, columns=["symbol", "date", "open", "high", "low", "close"])
    ohlc["symbol"] = ohlc["symbol"].astype(str)
    ohlc["date"] = pd.to_datetime(ohlc["date"])
    for c in ["open", "high", "low", "close"]:
        ohlc[c] = pd.to_numeric(ohlc[c], errors="coerce")
    ohlc = ohlc.dropna(subset=["open", "high", "low", "close"])
    out: Dict[str, Dict[str, object]] = {}
    for sym, sub in ohlc.groupby("symbol"):
        sub = sub.sort_values("date").reset_index(drop=True)
        idx = np.arange(len(sub))
        sub["idx"] = idx
        prev_close = sub["close"].shift(1)
        tr = np.maximum(sub["high"] - sub["low"], (sub["high"] - prev_close).abs())
        tr = np.maximum(tr, (sub["low"] - prev_close).abs())
        sub["atr"] = tr.rolling(atr_len, min_periods=atr_len).mean()
        date_to_idx = {d: int(i) for d, i in zip(sub["date"].tolist(), idx)}
        out[sym] = {"df": sub, "date_to_idx": date_to_idx}
    return out


def build_rank_map(preds: pd.DataFrame) -> Dict[pd.Timestamp, List[str]]:
    rank_map: Dict[pd.Timestamp, List[str]] = {}
    for d, sub in preds.groupby("date"):
        sub = sub.sort_values("pred", ascending=False)
        rank_map[d] = sub["symbol"].tolist()
    return rank_map


def _scan_tp_sl(
    high: np.ndarray,
    low: np.ndarray,
    entry: float,
    tp: float,
    sl: float,
    tie_break: str = "sl_first",
) -> Tuple[int | None, float | None, str]:
    tp_px = entry * (1.0 + tp)
    sl_px = entry * (1.0 + sl)
    for j in range(len(high)):
        sl_hit = low[j] <= sl_px
        tp_hit = high[j] >= tp_px
        if sl_hit and tp_hit:
            if tie_break == "tp_first":
                return j, tp_px, "TP"
            return j, sl_px, "SL"
        if sl_hit:
            return j, sl_px, "SL"
        if tp_hit:
            return j, tp_px, "TP"
    return None, None, "TIME"


def _scan_trail_pct(
    high: np.ndarray,
    low: np.ndarray,
    entry: float,
    trail_th: float,
) -> Tuple[int | None, float | None, str]:
    peak = entry
    for j in range(len(high)):
        if high[j] > peak:
            peak = high[j]
        trail_px = peak * (1.0 - trail_th)
        if low[j] <= trail_px:
            return j, trail_px, "TRAIL"
    return None, None, "TIME"


def _scan_trail_atr(
    high: np.ndarray,
    low: np.ndarray,
    entry: float,
    atr_dist: float,
) -> Tuple[int | None, float | None, str]:
    peak = entry
    for j in range(len(high)):
        if high[j] > peak:
            peak = high[j]
        trail_px = peak - atr_dist
        if low[j] <= trail_px:
            return j, trail_px, "ATR_TRAIL"
    return None, None, "TIME"


def simulate(
    cfg: StrategyConfig,
    rank_map: Dict[pd.Timestamp, List[str]],
    ohlc_map: Dict[str, Dict[str, object]],
    start: pd.Timestamp,
    end: pd.Timestamp,
    capital: float,
    max_positions: int,
) -> Dict[str, float]:
    dates = sorted([d for d in rank_map.keys() if start <= d <= end])
    if not dates:
        return {"trades": 0, "win_rate": np.nan, "mean": np.nan, "median": np.nan, "final": capital}

    cash = capital
    positions = []
    trade_returns = []
    for d in dates:
        # exits
        still = []
        for pos in positions:
            if pos["exit_date"] == d:
                cash += pos["shares"] * pos["exit_px"]
                trade_returns.append(pos["ret"])
            else:
                still.append(pos)
        positions = still

        # entries
        candidates = rank_map.get(d, [])[: cfg.topn]
        if not candidates:
            continue

        slots = max(0, min(cfg.topn, max_positions) - len(positions))
        if slots <= 0:
            continue

        if cfg.alloc_mode == "fixed" and cfg.fixed_cash:
            per_cash = cfg.fixed_cash
        else:
            per_cash = cash / slots if slots else 0.0

        for sym in candidates:
            if slots <= 0 or cash <= 0:
                break
            if any(p["symbol"] == sym for p in positions):
                continue
            if sym not in ohlc_map:
                continue
            info = ohlc_map[sym]
            sub = info["df"]
            i = info["date_to_idx"].get(d)
            if i is None:
                continue

            if cfg.entry_mode == "next_open":
                if i + 1 >= len(sub):
                    continue
                entry_idx = i + 1
                entry_px = float(sub.loc[entry_idx, "open"])
                gap = float(entry_px / sub.loc[i, "close"] - 1.0)
                if cfg.gap_th is not None and gap > cfg.gap_th:
                    continue
            else:
                entry_idx = i
                entry_px = float(sub.loc[i, "close"])

            if not np.isfinite(entry_px) or entry_px <= 0:
                continue

            exit_idx = entry_idx + cfg.hold_days
            if exit_idx >= len(sub):
                continue

            exit_px = float(sub.loc[exit_idx, "close"])
            outcome = "TIME"
            if cfg.exit_mode == "tp_sl" and cfg.tp is not None and cfg.sl is not None:
                high = sub.loc[entry_idx + 1 : exit_idx, "high"].to_numpy(float)
                low = sub.loc[entry_idx + 1 : exit_idx, "low"].to_numpy(float)
                hit_idx, hit_px, outcome = _scan_tp_sl(high, low, entry_px, cfg.tp, cfg.sl)
                if hit_idx is not None:
                    exit_idx = entry_idx + 1 + hit_idx
                    exit_px = float(hit_px)
            elif cfg.exit_mode == "trail" and cfg.trail_th is not None:
                high = sub.loc[entry_idx + 1 : exit_idx, "high"].to_numpy(float)
                low = sub.loc[entry_idx + 1 : exit_idx, "low"].to_numpy(float)
                hit_idx, hit_px, outcome = _scan_trail_pct(high, low, entry_px, cfg.trail_th)
                if hit_idx is not None:
                    exit_idx = entry_idx + 1 + hit_idx
                    exit_px = float(hit_px)
            elif cfg.exit_mode == "atr_trail" and cfg.atr_mult is not None:
                atr_val = float(sub.loc[entry_idx, "atr"])
                if np.isfinite(atr_val) and atr_val > 0:
                    high = sub.loc[entry_idx + 1 : exit_idx, "high"].to_numpy(float)
                    low = sub.loc[entry_idx + 1 : exit_idx, "low"].to_numpy(float)
                    hit_idx, hit_px, outcome = _scan_trail_atr(high, low, entry_px, atr_val * cfg.atr_mult)
                    if hit_idx is not None:
                        exit_idx = entry_idx + 1 + hit_idx
                        exit_px = float(hit_px)

            entry_date = sub.loc[entry_idx, "date"]
            exit_date = sub.loc[exit_idx, "date"]
            shares = per_cash / entry_px if per_cash > 0 else 0.0
            cost = shares * entry_px
            if cost > cash:
                continue
            cash -= cost
            ret = exit_px / entry_px - 1.0
            positions.append(
                {
                    "symbol": sym,
                    "entry_date": entry_date,
                    "exit_date": exit_date,
                    "entry_px": entry_px,
                    "exit_px": exit_px,
                    "shares": shares,
                    "ret": ret,
                    "outcome": outcome,
                }
            )
            slots -= 1

    # liquidate any remaining positions at last available close
    for pos in positions:
        cash += pos["shares"] * pos["exit_px"]
        trade_returns.append(pos["ret"])

    r = pd.Series(trade_returns)
    return {
        "trades": int(r.shape[0]),
        "win_rate": float((r > 0).mean()) if len(r) else np.nan,
        "mean": float(r.mean()) if len(r) else np.nan,
        "median": float(r.median()) if len(r) else np.nan,
        "final": float(cash),
    }


def _set_globals(
    rank_map: Dict[pd.Timestamp, List[str]],
    ohlc_map: Dict[str, Dict[str, object]],
    train_start: pd.Timestamp,
    train_end: pd.Timestamp,
    valid_start: pd.Timestamp,
    valid_end: pd.Timestamp,
    capital: float,
    max_positions: int,
) -> None:
    global _GLOBAL_RANK_MAP, _GLOBAL_OHLC_MAP, _GLOBAL_TRAIN_START, _GLOBAL_TRAIN_END
    global _GLOBAL_VALID_START, _GLOBAL_VALID_END, _GLOBAL_CAPITAL, _GLOBAL_MAX_POS
    _GLOBAL_RANK_MAP = rank_map
    _GLOBAL_OHLC_MAP = ohlc_map
    _GLOBAL_TRAIN_START = train_start
    _GLOBAL_TRAIN_END = train_end
    _GLOBAL_VALID_START = valid_start
    _GLOBAL_VALID_END = valid_end
    _GLOBAL_CAPITAL = capital
    _GLOBAL_MAX_POS = max_positions


def _init_worker(
    preds_path: str,
    ohlc_path: str,
    atr_len: int,
    train_start: pd.Timestamp,
    train_end: pd.Timestamp,
    valid_start: pd.Timestamp,
    valid_end: pd.Timestamp,
    capital: float,
    max_positions: int,
) -> None:
    global _GLOBAL_RANK_MAP, _GLOBAL_OHLC_MAP
    if _GLOBAL_RANK_MAP is None or _GLOBAL_OHLC_MAP is None:
        preds = load_predictions(Path(preds_path))
        ohlc_map = load_ohlc(Path(ohlc_path), atr_len)
        rank_map = build_rank_map(preds)
        _set_globals(rank_map, ohlc_map, train_start, train_end, valid_start, valid_end, capital, max_positions)
    else:
        _set_globals(_GLOBAL_RANK_MAP, _GLOBAL_OHLC_MAP, train_start, train_end, valid_start, valid_end, capital, max_positions)


def _run_one(cfg: StrategyConfig) -> Dict[str, object]:
    if _GLOBAL_RANK_MAP is None or _GLOBAL_OHLC_MAP is None:
        raise RuntimeError("worker globals not initialized")
    train = simulate(
        cfg,
        _GLOBAL_RANK_MAP,
        _GLOBAL_OHLC_MAP,
        _GLOBAL_TRAIN_START,
        _GLOBAL_TRAIN_END,
        float(_GLOBAL_CAPITAL),
        int(_GLOBAL_MAX_POS),
    )
    valid = simulate(
        cfg,
        _GLOBAL_RANK_MAP,
        _GLOBAL_OHLC_MAP,
        _GLOBAL_VALID_START,
        _GLOBAL_VALID_END,
        float(_GLOBAL_CAPITAL),
        int(_GLOBAL_MAX_POS),
    )
    return {
        "topn": cfg.topn,
        "entry": cfg.entry_mode,
        "gap_th": cfg.gap_th if cfg.gap_th is not None else "",
        "exit": cfg.exit_mode,
        "hold": cfg.hold_days,
        "tp": cfg.tp if cfg.tp is not None else "",
        "sl": cfg.sl if cfg.sl is not None else "",
        "trail_th": cfg.trail_th if cfg.trail_th is not None else "",
        "atr_mult": cfg.atr_mult if cfg.atr_mult is not None else "",
        "alloc": cfg.alloc_mode,
        "fixed_cash": cfg.fixed_cash if cfg.fixed_cash is not None else "",
        "train_trades": train["trades"],
        "train_win": train["win_rate"],
        "train_mean": train["mean"],
        "train_median": train["median"],
        "train_final": train["final"],
        "valid_trades": valid["trades"],
        "valid_win": valid["win_rate"],
        "valid_mean": valid["mean"],
        "valid_median": valid["median"],
        "valid_final": valid["final"],
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--preds", type=str, default="data/_derived/ml_runs/days_above_ge5_predictions_all.csv")
    ap.add_argument("--ohlc", type=str, default="data/_derived/ohlc.parquet")
    ap.add_argument("--atr-len", type=int, default=14)
    ap.add_argument("--capital", type=float, default=100000.0)
    ap.add_argument("--capital-list", type=str, default="100000,150000,200000,250000,300000,350000,400000,450000,500000")
    ap.add_argument("--valid-start", type=str, default="2025-12-01")
    ap.add_argument("--valid-end", type=str, default="2026-02-28")
    ap.add_argument("--topn-min", type=int, default=1)
    ap.add_argument("--topn-max", type=int, default=10)
    ap.add_argument("--gap-list", type=str, default="0.01,0.02,0.03,0.04,0.05")
    ap.add_argument("--hold-min", type=int, default=3)
    ap.add_argument("--hold-max", type=int, default=15)
    ap.add_argument("--hold-step", type=int, default=1)
    ap.add_argument("--tp-list", type=str, default="0.03,0.05,0.08,0.12,0.15,0.2,0.25,0.3,0.4,0.5")
    ap.add_argument("--sl-list", type=str, default="0.03,0.05,0.08,0.1")
    ap.add_argument("--trail-list", type=str, default="0.04,0.06,0.08,0.10,0.12")
    ap.add_argument("--atr-mult-list", type=str, default="1.5,2,2.5,3")
    ap.add_argument("--max-positions", type=int, default=10)
    ap.add_argument("--fixed-cash-list", type=str, default="20000")
    ap.add_argument("--sample", type=int, default=0, help="randomly sample N strategies instead of full grid")
    ap.add_argument("--topk", type=int, default=30)
    ap.add_argument("--workers", type=int, default=0, help="0=auto")
    ap.add_argument("--backend", type=str, default="process", choices=["process", "thread"])
    ap.add_argument("--progress-every", type=int, default=200)
    ap.add_argument("--out", type=str, default="data/_derived/ml_runs/strategy_search.csv")
    ap.add_argument("--out-capital", type=str, default="data/_derived/ml_runs/strategy_search_by_capital.csv")
    args = ap.parse_args()

    preds = load_predictions(Path(args.preds))
    ohlc_map = load_ohlc(Path(args.ohlc), args.atr_len)
    rank_map = build_rank_map(preds)

    valid_start = pd.to_datetime(args.valid_start)
    valid_end = pd.to_datetime(args.valid_end)
    all_dates = sorted(rank_map.keys())
    train_start = all_dates[0]
    train_end = valid_start - pd.Timedelta(days=1)
    _set_globals(rank_map, ohlc_map, train_start, train_end, valid_start, valid_end, args.capital, args.max_positions)

    def _parse_list(s: str) -> List[float]:
        return [float(x) for x in s.split(",") if x.strip()]

    topn_range = range(args.topn_min, args.topn_max + 1)
    gap_list = [None] + _parse_list(args.gap_list)
    hold_range = range(args.hold_min, args.hold_max + 1, args.hold_step)
    tp_list = _parse_list(args.tp_list)
    sl_list = _parse_list(args.sl_list)
    trail_list = _parse_list(args.trail_list)
    atr_mult_list = _parse_list(args.atr_mult_list)
    fixed_cash_list = _parse_list(args.fixed_cash_list)

    grid: List[StrategyConfig] = []
    rng = np.random.default_rng(42)
    entry_modes = ["next_open", "close"]
    alloc_modes = ["equal", "fixed"]

    if args.sample and args.sample > 0:
        for _ in range(args.sample):
            topn = int(rng.choice(list(topn_range)))
            entry = str(rng.choice(entry_modes))
            gap = rng.choice(gap_list)
            hold = int(rng.choice(list(hold_range)))
            exit_mode = str(rng.choice(["hold", "tp_sl", "trail", "atr_trail"]))
            tp = None
            sl = None
            trail_th = None
            atr_mult = None
            if exit_mode == "tp_sl":
                tp = float(rng.choice(tp_list))
                sl = -abs(float(rng.choice(sl_list)))
            elif exit_mode == "trail":
                trail_th = float(rng.choice(trail_list))
            elif exit_mode == "atr_trail":
                atr_mult = float(rng.choice(atr_mult_list))
            alloc = str(rng.choice(alloc_modes))
            fixed_cash = float(rng.choice(fixed_cash_list)) if alloc == "fixed" else None
            grid.append(StrategyConfig(topn, entry, gap, exit_mode, hold, tp, sl, trail_th, atr_mult, alloc, fixed_cash))
    else:
        for topn in topn_range:
            for entry in entry_modes:
                for gap in gap_list:
                    # fixed hold
                    for hold in hold_range:
                        grid.append(StrategyConfig(topn, entry, gap, "hold", hold, None, None, None, None, "equal", None))
                    # tp/sl
                    for hold in hold_range:
                        for tp in tp_list:
                            for sl in sl_list:
                                grid.append(StrategyConfig(topn, entry, gap, "tp_sl", hold, tp, -abs(sl), None, None, "equal", None))
                    # trailing percent
                    for hold in hold_range:
                        for trail_th in trail_list:
                            grid.append(StrategyConfig(topn, entry, gap, "trail", hold, None, None, trail_th, None, "equal", None))
                    # trailing ATR
                    for hold in hold_range:
                        for atr_mult in atr_mult_list:
                            grid.append(StrategyConfig(topn, entry, gap, "atr_trail", hold, None, None, None, atr_mult, "equal", None))

        # fixed cash mode, cap positions
        for fixed_cash in fixed_cash_list:
            for topn in topn_range:
                for entry in ["next_open"]:
                    for gap in gap_list:
                        for hold in hold_range:
                            grid.append(StrategyConfig(topn, entry, gap, "hold", hold, None, None, None, None, "fixed", fixed_cash))

    total = len(grid)
    rows = []
    if args.workers <= 0:
        args.workers = max(1, (os.cpu_count() or 4) - 1)

    def _log_progress(i: int, total_tasks: int, t0: float, prefix: str) -> None:
        if i == 1 or i == total_tasks or (args.progress_every > 0 and i % args.progress_every == 0):
            dt = time.perf_counter() - t0
            rate = dt / max(1, i)
            eta = rate * (total_tasks - i)
            pct = i / total_tasks * 100.0
            print(f"[{prefix}] {i}/{total_tasks} ({pct:.1f}%) elapsed={dt:.1f}s eta={eta:.1f}s")

    t0 = time.perf_counter()
    if args.workers == 1:
        for i, cfg in enumerate(grid, start=1):
            rows.append(_run_one(cfg))
            _log_progress(i, total, t0, "GRID")
    else:
        if args.backend == "thread":
            executor_cls = ThreadPoolExecutor
            executor_kwargs = {}
        else:
            executor_cls = ProcessPoolExecutor
            try:
                ctx = mp.get_context("fork")
            except ValueError:
                ctx = mp.get_context()
            executor_kwargs = {
                "mp_context": ctx,
                "initializer": _init_worker,
                "initargs": (
                    args.preds,
                    args.ohlc,
                    args.atr_len,
                    train_start,
                    train_end,
                    valid_start,
                    valid_end,
                    args.capital,
                    args.max_positions,
                ),
            }
        with executor_cls(max_workers=args.workers, **executor_kwargs) as ex:
            inflight = max(1, args.workers * 4)
            it = iter(grid)
            futures = {}
            for _ in range(min(inflight, total)):
                cfg = next(it, None)
                if cfg is None:
                    break
                futures[ex.submit(_run_one, cfg)] = None

            completed = 0
            while futures:
                for fut in as_completed(list(futures.keys()), timeout=None):
                    del futures[fut]
                    rows.append(fut.result())
                    completed += 1
                    _log_progress(completed, total, t0, "GRID")
                    cfg = next(it, None)
                    if cfg is not None:
                        futures[ex.submit(_run_one, cfg)] = None

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(out, index=False)
    print(f"Wrote: {out}")

    # evaluate top strategies across capital list
    cap_list = _parse_list(args.capital_list)
    df = pd.DataFrame(rows)
    df = df[df["valid_trades"] >= 20].copy()
    df = df.sort_values(["valid_win", "valid_final"], ascending=[False, False]).head(args.topk)
    cap_rows = []
    cap_total = int(df.shape[0]) * len(cap_list)
    cap_t0 = time.perf_counter()
    cap_i = 0
    for _, row in df.iterrows():
        cfg = StrategyConfig(
            int(row["topn"]),
            row["entry"],
            float(row["gap_th"]) if row["gap_th"] != "" else None,
            row["exit"],
            int(row["hold"]),
            float(row["tp"]) if row["tp"] != "" else None,
            float(row["sl"]) if row["sl"] != "" else None,
            float(row["trail_th"]) if "trail_th" in row and row["trail_th"] != "" else None,
            float(row["atr_mult"]) if "atr_mult" in row and row["atr_mult"] != "" else None,
            row["alloc"],
            float(row["fixed_cash"]) if row["fixed_cash"] != "" else None,
        )
        for cap in cap_list:
            train = simulate(cfg, rank_map, ohlc_map, train_start, train_end, cap, args.max_positions)
            valid = simulate(cfg, rank_map, ohlc_map, valid_start, valid_end, cap, args.max_positions)
            cap_rows.append(
                {
                    "capital": cap,
                    "topn": cfg.topn,
                    "entry": cfg.entry_mode,
                    "gap_th": cfg.gap_th if cfg.gap_th is not None else "",
                    "exit": cfg.exit_mode,
                    "hold": cfg.hold_days,
                    "tp": cfg.tp if cfg.tp is not None else "",
                    "sl": cfg.sl if cfg.sl is not None else "",
                    "trail_th": cfg.trail_th if cfg.trail_th is not None else "",
                    "atr_mult": cfg.atr_mult if cfg.atr_mult is not None else "",
                    "alloc": cfg.alloc_mode,
                    "fixed_cash": cfg.fixed_cash if cfg.fixed_cash is not None else "",
                    "valid_trades": valid["trades"],
                    "valid_win": valid["win_rate"],
                    "valid_mean": valid["mean"],
                    "valid_median": valid["median"],
                    "valid_final": valid["final"],
                }
            )
            cap_i += 1
            if cap_total:
                _log_progress(cap_i, cap_total, cap_t0, "CAP")

    out_cap = Path(args.out_capital)
    out_cap.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(cap_rows).to_csv(out_cap, index=False)
    print(f"Wrote: {out_cap}")


if __name__ == "__main__":
    main()
