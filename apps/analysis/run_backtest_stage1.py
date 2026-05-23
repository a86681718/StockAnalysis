#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import json
import argparse
import logging
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import numpy as np
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed


LOG = logging.getLogger("bt")

DERIVED = Path("./data/_derived")
DERIVED.mkdir(parents=True, exist_ok=True)

BUCKET_COL = "bucket"


# Walk-forward (適合 8~9 個月)
TRAIN_LENS = [60, 80, 100, 120]
TEST_LEN = 20


@dataclass(frozen=True)
class Fold:
    fold_id: int
    train_start: pd.Timestamp
    train_end: pd.Timestamp
    test_start: pd.Timestamp
    test_end: pd.Timestamp


def init_logging(level: str = "INFO", log_path: Optional[Path] = None):
    lvl = getattr(logging, level.upper(), logging.INFO)
    handlers = [logging.StreamHandler()]
    if log_path:
        handlers.append(logging.FileHandler(log_path, encoding="utf-8"))
    logging.basicConfig(
        level=lvl,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=handlers,
    )


def build_folds(unique_dates: List[pd.Timestamp], horizon: int) -> List[Fold]:
    """train: from start; test: after purge gap=horizon"""
    folds: List[Fold] = []
    n = len(unique_dates)
    fid = 1
    for train_len in TRAIN_LENS:
        t0 = 0
        train_end_i = t0 + train_len - 1
        test_start_i = train_end_i + horizon + 1  # purge gap = horizon
        test_end_i = test_start_i + TEST_LEN - 1
        if test_end_i >= n:
            break
        folds.append(Fold(
            fold_id=fid,
            train_start=unique_dates[t0],
            train_end=unique_dates[train_end_i],
            test_start=unique_dates[test_start_i],
            test_end=unique_dates[test_end_i],
        ))
        fid += 1
    return folds


# ---------------------------
# MFE/MAE (for fitting dynamic TP/SL on train)
# ---------------------------
def compute_mfe_mae_for_events(
    df: pd.DataFrame,
    horizon: int,
    entry_mode: str,
    gap_th: float | None,
) -> pd.DataFrame:
    out = df.sort_values(["symbol", "date"]).copy()
    out["mfe"] = np.nan
    out["mae"] = np.nan
    out["gap_1d"] = np.nan

    for sym, sub in out.groupby("symbol"):
        sub = sub.sort_values("date").reset_index()
        idx = sub["index"].to_numpy()
        close = sub["close"].to_numpy(float)
        open_ = sub["open"].to_numpy(float)
        high = sub["high"].to_numpy(float)
        low  = sub["low"].to_numpy(float)
        ev   = sub["is_event"].to_numpy(int)
        n = len(sub)

        for i in range(n):
            if ev[i] != 1:
                continue
            j_end = i + horizon
            if j_end >= n:
                continue

            if entry_mode == "next_open":
                if i + 1 >= n:
                    continue
                entry = open_[i + 1]
                if np.isfinite(entry) and np.isfinite(close[i]) and close[i] > 0:
                    gap = (entry / close[i]) - 1.0
                    out.loc[idx[i], "gap_1d"] = gap
                    if gap_th is not None and gap > gap_th:
                        continue
            else:
                entry = close[i]

            if not np.isfinite(entry) or entry <= 0:
                continue

            hmax = np.nanmax(high[i+1:j_end+1])
            lmin = np.nanmin(low[i+1:j_end+1])
            out.loc[idx[i], "mfe"] = (hmax / entry) - 1.0
            out.loc[idx[i], "mae"] = (lmin / entry) - 1.0

    return out


def fit_dynamic_tp_sl(
    train_events: pd.DataFrame,
    tp_q: float,
    sl_q: float,
    min_events_bucket: int,
) -> Dict[str, Tuple[float, float]]:
    """bucket -> (tp, sl) from train events' MFE/MAE quantiles"""
    te = train_events.dropna(subset=["mfe", "mae", BUCKET_COL]).copy()
    if te.empty:
        return {}

    g_tp = float(te["mfe"].quantile(tp_q))
    g_sl = float(te["mae"].quantile(sl_q))

    m: Dict[str, Tuple[float, float]] = {}
    for b, sub in te.groupby(BUCKET_COL):
        if len(sub) >= min_events_bucket:
            tp = float(sub["mfe"].quantile(tp_q))
            sl = float(sub["mae"].quantile(sl_q))
        else:
            tp, sl = g_tp, g_sl

        tp = max(tp, 0.0)
        sl = min(sl, 0.0)
        m[str(b)] = (tp, sl)

    return m


# ---------------------------
# Backtest engines
# ---------------------------
def _scan_tp_sl(
    entry: float,
    high: np.ndarray,
    low: np.ndarray,
    close_end: float,
    tp: float,
    sl: float,
    tie_break: str,
) -> Tuple[str, float]:
    tp_px = entry * (1.0 + tp)
    sl_px = entry * (1.0 + sl)

    outcome = "TIME"
    exit_px = close_end

    for j in range(len(high)):
        sl_hit = np.isfinite(low[j]) and (low[j] <= sl_px)
        tp_hit = np.isfinite(high[j]) and (high[j] >= tp_px)

        if sl_hit and tp_hit:
            if tie_break == "skip":
                return "SKIP", np.nan
            if tie_break == "tp_first":
                return "TP", tp_px
            return "SL", sl_px

        if sl_hit:
            return "SL", sl_px
        if tp_hit:
            return "TP", tp_px

    return outcome, exit_px


def backtest_fixed(
    df: pd.DataFrame,
    horizon: int,
    tp: float,
    sl: float,
    tie_break: str,
    entry_mode: str,
    gap_th: float | None,
) -> pd.DataFrame:
    out = df.sort_values(["symbol", "date"]).copy()
    out["tp_used"] = np.where(out["is_event"] == 1, tp, np.nan)
    out["sl_used"] = np.where(out["is_event"] == 1, sl, np.nan)
    out["ret"] = np.nan
    out["outcome"] = None
    out["gap_1d"] = np.nan
    out["skip_reason"] = None

    for sym, sub in out.groupby("symbol"):
        sub = sub.sort_values("date").reset_index()
        idx = sub["index"].to_numpy()
        close = sub["close"].to_numpy(float)
        open_ = sub["open"].to_numpy(float)
        high  = sub["high"].to_numpy(float)
        low   = sub["low"].to_numpy(float)
        ev    = sub["is_event"].to_numpy(int)
        n = len(sub)

        for i in range(n):
            if ev[i] != 1:
                continue
            j_end = i + horizon
            if j_end >= n:
                continue

            if entry_mode == "next_open":
                if i + 1 >= n:
                    continue
                entry = open_[i + 1]
                if np.isfinite(entry) and np.isfinite(close[i]) and close[i] > 0:
                    gap = (entry / close[i]) - 1.0
                    out.loc[idx[i], "gap_1d"] = gap
                    if gap_th is not None and gap > gap_th:
                        out.loc[idx[i], "outcome"] = "SKIP"
                        out.loc[idx[i], "skip_reason"] = "gap"
                        continue
            else:
                entry = close[i]

            if not np.isfinite(entry) or entry <= 0:
                continue

            outcome, exit_px = _scan_tp_sl(
                entry=entry,
                high=high[i+1:j_end+1],
                low=low[i+1:j_end+1],
                close_end=close[j_end],
                tp=tp,
                sl=sl,
                tie_break=tie_break,
            )
            out.loc[idx[i], "outcome"] = outcome
            if outcome != "SKIP":
                out.loc[idx[i], "ret"] = (exit_px / entry) - 1.0

    return out


def backtest_dynamic(
    df: pd.DataFrame,
    horizon: int,
    tp_sl_map: Dict[str, Tuple[float, float]],
    tie_break: str,
    entry_mode: str,
    gap_th: float | None,
) -> pd.DataFrame:
    out = df.sort_values(["symbol", "date"]).copy()
    out["tp_used"] = np.nan
    out["sl_used"] = np.nan
    out["ret"] = np.nan
    out["outcome"] = None
    out["gap_1d"] = np.nan
    out["skip_reason"] = None

    for sym, sub in out.groupby("symbol"):
        sub = sub.sort_values("date").reset_index()
        idx = sub["index"].to_numpy()
        close = sub["close"].to_numpy(float)
        open_ = sub["open"].to_numpy(float)
        high  = sub["high"].to_numpy(float)
        low   = sub["low"].to_numpy(float)
        ev    = sub["is_event"].to_numpy(int)
        bkt   = sub[BUCKET_COL].astype(str).to_numpy()
        n = len(sub)

        for i in range(n):
            if ev[i] != 1:
                continue
            j_end = i + horizon
            if j_end >= n:
                continue
            b = str(bkt[i])
            if b not in tp_sl_map:
                continue

            tp, sl = tp_sl_map[b]

            if entry_mode == "next_open":
                if i + 1 >= n:
                    continue
                entry = open_[i + 1]
                if np.isfinite(entry) and np.isfinite(close[i]) and close[i] > 0:
                    gap = (entry / close[i]) - 1.0
                    out.loc[idx[i], "gap_1d"] = gap
                    if gap_th is not None and gap > gap_th:
                        out.loc[idx[i], "outcome"] = "SKIP"
                        out.loc[idx[i], "skip_reason"] = "gap"
                        continue
            else:
                entry = close[i]

            if not np.isfinite(entry) or entry <= 0:
                continue

            outcome, exit_px = _scan_tp_sl(
                entry=entry,
                high=high[i+1:j_end+1],
                low=low[i+1:j_end+1],
                close_end=close[j_end],
                tp=tp,
                sl=sl,
                tie_break=tie_break,
            )
            out.loc[idx[i], "tp_used"] = tp
            out.loc[idx[i], "sl_used"] = sl
            out.loc[idx[i], "outcome"] = outcome
            if outcome != "SKIP":
                out.loc[idx[i], "ret"] = (exit_px / entry) - 1.0

    return out


# ---------------------------
# Summaries
# ---------------------------
def summarize_events(ev: pd.DataFrame) -> pd.DataFrame:
    e = ev[(ev["is_event"] == 1) & ev["outcome"].notna()].copy()
    e = e[e["outcome"] != "SKIP"]
    if e.empty:
        return pd.DataFrame([{
            "n_events": 0,
            "EV": np.nan,
            "TP_rate": np.nan,
            "SL_rate": np.nan,
            "TIME_rate": np.nan,
            "ret_median": np.nan,
            "ret_p25": np.nan,
            "ret_p75": np.nan,
        }])

    return pd.DataFrame([{
        "n_events": int(len(e)),
        "EV": float(e["ret"].mean()),
        "TP_rate": float((e["outcome"] == "TP").mean()),
        "SL_rate": float((e["outcome"] == "SL").mean()),
        "TIME_rate": float((e["outcome"] == "TIME").mean()),
        "ret_median": float(e["ret"].median()),
        "ret_p25": float(e["ret"].quantile(0.25)),
        "ret_p75": float(e["ret"].quantile(0.75)),
    }])


def summarize_by_bucket(ev: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for b, sub in ev[(ev["is_event"] == 1) & ev["outcome"].notna()].groupby(BUCKET_COL):
        s = summarize_events(sub)
        s.insert(0, "bucket", str(b))
        rows.append(s)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


# ---------------------------
# Fold runner (dynamic)
# ---------------------------
def run_dynamic_fold(args: dict) -> dict:
    fold: Fold = Fold(**args["fold"])
    base_path = args["base_path"]
    horizon = args["horizon"]
    tpq = args["tpq"]
    slq = args["slq"]
    min_events_bucket = args["min_events_bucket"]
    tie_break = args["tie_break"]
    entry_mode = args["entry_mode"]
    gap_th = args["gap_th"]

    df = pd.read_parquet(base_path)
    df["date"] = pd.to_datetime(df["date"])

    # compute MFE/MAE once per worker fold (still OK; data is mid-size)
    df2 = compute_mfe_mae_for_events(df, horizon=horizon, entry_mode=entry_mode, gap_th=gap_th)

    train_mask = (df2["date"] >= fold.train_start) & (df2["date"] <= fold.train_end) & (df2["is_event"] == 1)
    test_mask  = (df2["date"] >= fold.test_start) & (df2["date"] <= fold.test_end)

    tp_sl_map = fit_dynamic_tp_sl(df2[train_mask], tp_q=tpq, sl_q=slq, min_events_bucket=min_events_bucket)

    bt = backtest_dynamic(
        df2,
        horizon=horizon,
        tp_sl_map=tp_sl_map,
        tie_break=tie_break,
        entry_mode=entry_mode,
        gap_th=gap_th,
    )

    test = bt[test_mask].copy()
    test["fold_id"] = fold.fold_id

    summ_all = summarize_events(test)
    summ_all.insert(0, "scope", "overall")
    summ_all.insert(0, "fold_id", fold.fold_id)

    summ_b = summarize_by_bucket(test)
    if not summ_b.empty:
        summ_b.insert(0, "scope", "by_bucket")
        summ_b.insert(0, "fold_id", fold.fold_id)

    return {
        "fold_id": fold.fold_id,
        "tp_sl_map": tp_sl_map,
        "test_events": test[test["is_event"] == 1].copy(),
        "summary": pd.concat([summ_all, summ_b], ignore_index=True),
    }


# ---------------------------
# Main
# ---------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, type=str)
    ap.add_argument("--horizon", type=int, default=10)
    ap.add_argument("--tp", type=float, default=0.08)
    ap.add_argument("--sl", type=float, default=-0.09)
    ap.add_argument("--tpq", type=float, default=0.75, help="dynamic TP quantile of train MFE")
    ap.add_argument("--slq", type=float, default=0.10, help="dynamic SL quantile of train MAE (negative)")
    ap.add_argument("--min-events-bucket", type=int, default=30)
    ap.add_argument("--tie-break", type=str, default="sl_first", choices=["sl_first","tp_first","skip"])
    ap.add_argument("--entry", type=str, default="close", choices=["close", "next_open"])
    ap.add_argument("--gap-th", type=float, default=None, help="skip trade if next_open/close - 1 > gap_th")
    ap.add_argument("--workers", type=int, default=0)
    ap.add_argument("--log-level", type=str, default="INFO")
    args = ap.parse_args()

    init_logging(args.log_level, DERIVED / "backtest_run.log")

    workers = args.workers if args.workers > 0 else max(1, (os.cpu_count() or 4) - 1)

    inp = Path(args.input)
    if not inp.exists():
        raise FileNotFoundError(inp)

    df = pd.read_parquet(inp)
    df["date"] = pd.to_datetime(df["date"])

    need = {"symbol","date","open","high","low","close","is_event",BUCKET_COL}
    miss = need - set(df.columns)
    if miss:
        raise ValueError(f"Missing columns in input parquet: {miss}")

    df = df.sort_values(["symbol","date"]).copy()
    LOG.info(f"Input: {inp} | date={df['date'].min().date()}~{df['date'].max().date()} rows={len(df):,} symbols={df['symbol'].nunique():,}")

    unique_dates = sorted(df["date"].drop_duplicates().tolist())
    folds = build_folds(unique_dates, horizon=args.horizon)
    if not folds:
        raise RuntimeError("Not enough dates to build folds. (Need more history.)")

    for f in folds:
        LOG.info(f"fold{f.fold_id}: train={f.train_start.date()}..{f.train_end.date()} test={f.test_start.date()}..{f.test_end.date()}")

    # Write a base cache for multi-process reading
    base_cache = DERIVED / "bt_base_input.parquet"
    df.to_parquet(base_cache, index=False)

    # -----------------------
    # 1) Fixed TP/SL (no OOS learning needed; but we still evaluate only in test folds)
    # -----------------------
    fixed_all = backtest_fixed(
        df,
        horizon=args.horizon,
        tp=args.tp,
        sl=args.sl,
        tie_break=args.tie_break,
        entry_mode=args.entry,
        gap_th=args.gap_th,
    )

    fixed_rows = []
    fixed_summ = []
    for f in folds:
        test_mask = (fixed_all["date"] >= f.test_start) & (fixed_all["date"] <= f.test_end)
        test = fixed_all[test_mask].copy()
        test["fold_id"] = f.fold_id
        fixed_rows.append(test[test["is_event"] == 1].copy())

        s_all = summarize_events(test)
        s_all.insert(0, "scope", "overall")
        s_all.insert(0, "fold_id", f.fold_id)

        s_b = summarize_by_bucket(test)
        if not s_b.empty:
            s_b.insert(0, "scope", "by_bucket")
            s_b.insert(0, "fold_id", f.fold_id)

        fixed_summ.append(pd.concat([s_all, s_b], ignore_index=True))

    fixed_events = pd.concat(fixed_rows, ignore_index=True) if fixed_rows else pd.DataFrame()
    fixed_summary = pd.concat(fixed_summ, ignore_index=True) if fixed_summ else pd.DataFrame()

    out_fixed_ev = DERIVED / f"bt_fixed_events_h{args.horizon}.csv"
    out_fixed_sm = DERIVED / f"bt_summary_fixed_h{args.horizon}.csv"
    fixed_events.to_csv(out_fixed_ev, index=False, encoding="utf-8-sig")
    fixed_summary.to_csv(out_fixed_sm, index=False, encoding="utf-8-sig")
    LOG.info(f"[FIXED] wrote: {out_fixed_ev} rows={len(fixed_events):,}")
    LOG.info(f"[FIXED] wrote: {out_fixed_sm} rows={len(fixed_summary):,}")

    # -----------------------
    # 2) Dynamic TP/SL (OOS: fit from train, apply on test)
    # -----------------------
    fold_args = [{
        "fold": asdict(f),
        "base_path": str(base_cache),
        "horizon": args.horizon,
        "tpq": args.tpq,
        "slq": args.slq,
        "min_events_bucket": args.min_events_bucket,
        "tie_break": args.tie_break,
        "entry_mode": args.entry,
        "gap_th": args.gap_th,
    } for f in folds]

    dyn_events_list = []
    dyn_summary_list = []
    dyn_map = {}

    LOG.info(f"[DYNAMIC] workers={workers} folds={len(folds)} tpq={args.tpq} slq={args.slq} min_events_bucket={args.min_events_bucket}")

    with ProcessPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(run_dynamic_fold, a) for a in fold_args]
        for fut in as_completed(futs):
            res = fut.result()
            fid = res["fold_id"]
            dyn_map[f"fold{fid}"] = {k: {"tp": v[0], "sl": v[1]} for k, v in res["tp_sl_map"].items()}
            dyn_events_list.append(res["test_events"])
            dyn_summary_list.append(res["summary"])

    dyn_events = pd.concat(dyn_events_list, ignore_index=True) if dyn_events_list else pd.DataFrame()
    dyn_summary = pd.concat(dyn_summary_list, ignore_index=True) if dyn_summary_list else pd.DataFrame()

    out_dyn_ev = DERIVED / f"bt_dynamic_events_h{args.horizon}.csv"
    out_dyn_sm = DERIVED / f"bt_summary_dynamic_h{args.horizon}.csv"
    out_dyn_map = DERIVED / f"bt_dynamic_tpsl_map_h{args.horizon}.json"

    dyn_events.to_csv(out_dyn_ev, index=False, encoding="utf-8-sig")
    dyn_summary.to_csv(out_dyn_sm, index=False, encoding="utf-8-sig")
    with open(out_dyn_map, "w", encoding="utf-8") as f:
        json.dump(dyn_map, f, ensure_ascii=False, indent=2)

    LOG.info(f"[DYNAMIC] wrote: {out_dyn_ev} rows={len(dyn_events):,}")
    LOG.info(f"[DYNAMIC] wrote: {out_dyn_sm} rows={len(dyn_summary):,}")
    LOG.info(f"[DYNAMIC] wrote: {out_dyn_map}")

    # quick print
    def _overall(dfsm: pd.DataFrame) -> pd.DataFrame:
        x = dfsm[(dfsm["scope"] == "overall")].copy()
        return x[["fold_id","n_events","EV","TP_rate","SL_rate","TIME_rate","ret_median","ret_p25","ret_p75"]].sort_values("fold_id")

    if not fixed_summary.empty:
        LOG.info("[FIXED overall by fold]\n" + _overall(fixed_summary).to_string(index=False))
    if not dyn_summary.empty:
        LOG.info("[DYNAMIC overall by fold]\n" + _overall(dyn_summary).to_string(index=False))

    LOG.info("DONE.")


if __name__ == "__main__":
    main()
