#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bollinger-band surge model:
- Focus on "above upper band -> potential band-walk surge"
- Train classifier with walk-forward validation (OOF evaluation)
- Backtest top-N picks with transaction costs
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT))

from apps.analysis.run_ml_mfe10 import FEATURE_COLS, build_features


def add_bollinger_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.sort_values(["symbol", "date"]).copy()
    g = out.groupby("symbol", group_keys=False)

    ma20 = g["close"].rolling(20, min_periods=20).mean().reset_index(level=0, drop=True)
    std20 = g["close"].rolling(20, min_periods=20).std().reset_index(level=0, drop=True)
    upper = ma20 + 2.0 * std20
    lower = ma20 - 2.0 * std20

    out["bb_ma20"] = ma20
    out["bb_upper"] = upper
    out["bb_lower"] = lower
    out["bb_break"] = (out["close"] > out["bb_upper"]).astype(float)

    prev_close = g["close"].shift(1)
    prev_upper = g["bb_upper"].shift(1)
    out["bb_cross_up"] = ((out["close"] > out["bb_upper"]) & (prev_close <= prev_upper)).astype(float)
    out["bb_dist_upper"] = out["close"] / out["bb_upper"] - 1.0
    out["bb_dist_lower"] = out["close"] / out["bb_lower"] - 1.0
    out["bb_upper_slope_3d"] = g["bb_upper"].pct_change(3, fill_method=None)
    out["bb_above_upper_5d"] = g["bb_break"].rolling(5, min_periods=1).sum().reset_index(level=0, drop=True)
    out["bb_touch_upper"] = (out["high"] >= out["bb_upper"]).astype(float)
    out["bb_touch_upper_5d"] = g["bb_touch_upper"].rolling(5, min_periods=1).sum().reset_index(level=0, drop=True)
    out["bb_above_ma20"] = (out["close"] > out["bb_ma20"]).astype(float)
    out["bb_squeeze_20q"] = g["bb_width"].rolling(20, min_periods=20).rank(pct=True).reset_index(level=0, drop=True)
    return out


def add_label_and_returns(
    df: pd.DataFrame,
    horizon: int,
    surge_th: float,
    min_band_days: int,
) -> pd.DataFrame:
    out = df.sort_values(["symbol", "date"]).reset_index(drop=True).copy()
    g = out.groupby("symbol", group_keys=False)

    out["next_open"] = g["open"].shift(-1)
    out["close_h"] = g["close"].shift(-horizon)

    mfe = np.full(len(out), np.nan)
    band_days = np.full(len(out), np.nan)

    for _, sub in out.groupby("symbol"):
        idx = sub.index.to_numpy()
        close_arr = sub["close"].to_numpy(float)
        high_arr = sub["high"].to_numpy(float)
        upper_arr = sub["bb_upper"].to_numpy(float)
        n = len(sub)
        for i in range(n):
            j_end = i + horizon
            if j_end >= n:
                continue
            base = close_arr[i]
            if not np.isfinite(base) or base <= 0:
                continue
            fut_high = high_arr[i + 1 : j_end + 1]
            fut_close = close_arr[i + 1 : j_end + 1]
            fut_upper = upper_arr[i + 1 : j_end + 1]
            if not np.isfinite(fut_high).any():
                continue
            max_high = float(np.nanmax(fut_high))
            mfe_val = max_high / base - 1.0
            band_cnt = int(np.sum(np.isfinite(fut_close) & np.isfinite(fut_upper) & (fut_close > fut_upper)))
            mfe[idx[i]] = mfe_val
            band_days[idx[i]] = band_cnt

    out[f"mfe_{horizon}d"] = mfe
    out[f"band_days_above_upper_{horizon}d"] = band_days
    label_col = f"bb_surge_{horizon}d_ge{int(surge_th*100)}_bd{min_band_days}"
    out[label_col] = np.where(
        out[f"mfe_{horizon}d"].notna(),
        (
            (out[f"mfe_{horizon}d"] >= surge_th)
            & (out[f"band_days_above_upper_{horizon}d"] >= min_band_days)
        ).astype(float),
        np.nan,
    )
    return out


def fit_winsor_bounds(
    train_df: pd.DataFrame,
    feature_names: list[str],
    lower_q: float,
    upper_q: float,
) -> dict[str, tuple[float, float]]:
    bounds: dict[str, tuple[float, float]] = {}
    if lower_q <= 0 and upper_q >= 1:
        return bounds
    for f in feature_names:
        s = pd.to_numeric(train_df[f], errors="coerce")
        lo = float(s.quantile(lower_q)) if lower_q > 0 else -np.inf
        hi = float(s.quantile(upper_q)) if upper_q < 1 else np.inf
        bounds[f] = (lo, hi)
    return bounds


def apply_winsor(
    df: pd.DataFrame,
    feature_names: list[str],
    bounds: dict[str, tuple[float, float]],
) -> pd.DataFrame:
    out = df.copy()
    if not bounds:
        return out
    for f in feature_names:
        lo, hi = bounds.get(f, (-np.inf, np.inf))
        out[f] = pd.to_numeric(out[f], errors="coerce").clip(lower=lo, upper=hi)
    return out


def train_lgbm_classifier(
    X_train: np.ndarray,
    y_train: np.ndarray,
    seed: int,
    params: dict | None = None,
) -> tuple[Callable[[np.ndarray], np.ndarray], np.ndarray, str]:
    import lightgbm as lgb

    base_params = dict(
        n_estimators=500,
        learning_rate=0.03,
        num_leaves=63,
        min_child_samples=50,
        subsample=0.8,
        subsample_freq=1,
        colsample_bytree=0.8,
        reg_alpha=0.0,
        reg_lambda=0.0,
        random_state=seed,
        objective="binary",
        verbosity=-1,
        force_row_wise=True,
    )
    if params:
        base_params.update(params)
    model = lgb.LGBMClassifier(**base_params)
    model.fit(X_train, y_train)
    imp = model.booster_.feature_importance(importance_type="gain")
    return lambda X: model.predict_proba(X)[:, 1], imp, "lgbm"


def train_logit_classifier(
    X_train: np.ndarray,
    y_train: np.ndarray,
    seed: int,
) -> tuple[Callable[[np.ndarray], np.ndarray], np.ndarray, str]:
    from sklearn.linear_model import LogisticRegression

    model = LogisticRegression(
        max_iter=2000,
        C=1.0,
        class_weight="balanced",
        random_state=seed,
    )
    model.fit(X_train, y_train)
    coef = np.abs(model.coef_).ravel()
    return lambda X: model.predict_proba(X)[:, 1], coef, "logit"


def train_classifier(
    model_name: str,
    X_train: np.ndarray,
    y_train: np.ndarray,
    seed: int,
    params: dict | None = None,
) -> tuple[Callable[[np.ndarray], np.ndarray], np.ndarray, str]:
    if model_name == "lgbm":
        try:
            return train_lgbm_classifier(X_train, y_train, seed, params=params)
        except Exception as exc:
            print(f"[WARN] LightGBM unavailable, fallback to logit: {exc}")
    return train_logit_classifier(X_train, y_train, seed)


def build_walk_forward_folds(
    dates: pd.Series,
    train_months: int,
    valid_months: int,
    step_months: int,
) -> list[dict]:
    dmin = pd.to_datetime(dates.min())
    dmax = pd.to_datetime(dates.max())
    valid_start = (dmin + pd.offsets.MonthBegin(train_months)).normalize()
    folds: list[dict] = []
    fid = 1
    while valid_start <= dmax:
        train_end = valid_start - pd.Timedelta(days=1)
        valid_end = (valid_start + pd.DateOffset(months=valid_months)) - pd.Timedelta(days=1)
        folds.append(
            {
                "fold": fid,
                "train_start": dmin,
                "train_end": train_end,
                "valid_start": valid_start,
                "valid_end": min(valid_end, dmax),
            }
        )
        fid += 1
        valid_start = valid_start + pd.DateOffset(months=step_months)
    return folds


def fillna_by_train_median(
    train_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    all_df: pd.DataFrame,
    feature_names: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    med = train_df[feature_names].median(numeric_only=True)
    train = train_df.copy()
    valid = valid_df.copy()
    allx = all_df.copy()
    train[feature_names] = train[feature_names].fillna(med)
    valid[feature_names] = valid[feature_names].fillna(med)
    allx[feature_names] = allx[feature_names].fillna(med)
    return train, valid, allx


def eval_topn(df: pd.DataFrame, label_col: str, topn: int) -> dict:
    rows = []
    for _, sub in df.groupby("date"):
        s = sub.sort_values("pred", ascending=False).head(topn)
        if s.empty:
            continue
        rows.append(float(s[label_col].mean()))
    if not rows:
        return {"n_dates": 0, "topn_hit_rate": np.nan}
    return {"n_dates": int(len(rows)), "topn_hit_rate": float(np.mean(rows))}


def net_return_per_trade(
    entry_px: pd.Series,
    exit_px: pd.Series,
    notional: float,
    fee_rate: float,
    fee_discount: float,
    fee_min: float,
    tax_sell: float,
    slippage: float,
) -> pd.Series:
    eff_fee = fee_rate * fee_discount
    entry_exec = entry_px * (1.0 + slippage)
    exit_exec = exit_px * (1.0 - slippage)
    shares = notional / entry_exec
    buy_value = shares * entry_exec
    buy_fee = np.maximum(buy_value * eff_fee, fee_min)
    sell_value = shares * exit_exec
    sell_fee = np.maximum(sell_value * eff_fee, fee_min)
    sell_tax = sell_value * tax_sell
    pnl = sell_value - sell_fee - sell_tax - buy_value - buy_fee
    return pnl / (buy_value + buy_fee)


def backtest_topn_modes(
    df: pd.DataFrame,
    label_col: str,
    horizon: int,
    topn_list: list[int],
    trade_notional: float,
    fee_rate: float,
    fee_discount: float,
    fee_min: float,
    tax_sell: float,
    slippage: float,
) -> pd.DataFrame:
    modes = {
        "all": pd.Series(True, index=df.index),
        "bb_break": df["bb_break"] > 0.5,
        "bb_cross_up": df["bb_cross_up"] > 0.5,
    }
    rows = []
    for mode, mask in modes.items():
        base = df[mask].copy()
        for topn in topn_list:
            picks = []
            for _, sub in base.groupby("date"):
                picks.append(sub.sort_values("pred", ascending=False).head(topn))
            if not picks:
                continue
            pick = pd.concat(picks, ignore_index=True)
            valid = (
                pick["next_open"].notna()
                & pick["close_h"].notna()
                & (pick["next_open"] > 0)
                & (pick["close_h"] > 0)
            )
            r = pd.Series([], dtype=float)
            if valid.any():
                r = net_return_per_trade(
                    pick.loc[valid, "next_open"],
                    pick.loc[valid, "close_h"],
                    notional=trade_notional,
                    fee_rate=fee_rate,
                    fee_discount=fee_discount,
                    fee_min=fee_min,
                    tax_sell=tax_sell,
                    slippage=slippage,
                )
            rows.append(
                {
                    "mode": mode,
                    "topn": topn,
                    "horizon": horizon,
                    "n_picks": int(len(pick)),
                    "n_exec": int(r.shape[0]),
                    "label_hit_rate": float(pick[label_col].mean()) if len(pick) else np.nan,
                    "net_mean": float(r.mean()) if len(r) else np.nan,
                    "net_median": float(r.median()) if len(r) else np.nan,
                    "net_win_rate": float((r > 0).mean()) if len(r) else np.nan,
                }
            )
    return pd.DataFrame(rows)


def main() -> None:
    ap = argparse.ArgumentParser(description="Bollinger surge model with walk-forward OOF")
    ap.add_argument("--data", type=str, default="data/_derived/scored.parquet")
    ap.add_argument("--horizon", type=int, default=10)
    ap.add_argument("--surge-th", type=float, default=0.10)
    ap.add_argument("--min-band-days", type=int, default=3)
    ap.add_argument("--symbol-len", type=int, default=4)
    ap.add_argument("--min-history", type=int, default=60)
    ap.add_argument("--model", type=str, default="lgbm", choices=["lgbm", "logit"])
    ap.add_argument("--params", type=str, default="")
    ap.add_argument("--seed", type=int, default=42)

    ap.add_argument("--wf-train-months", type=int, default=6)
    ap.add_argument("--wf-valid-months", type=int, default=1)
    ap.add_argument("--wf-step-months", type=int, default=1)
    ap.add_argument("--wf-min-train-rows", type=int, default=5000)

    ap.add_argument("--winsor-lower", type=float, default=0.01)
    ap.add_argument("--winsor-upper", type=float, default=0.99)
    ap.add_argument("--topn", type=int, default=10)
    ap.add_argument("--topn-list", type=str, default="1,3,5,10")

    # Costs for backtest
    ap.add_argument("--trade-notional", type=float, default=100000.0)
    ap.add_argument("--fee-rate", type=float, default=0.001425)
    ap.add_argument("--fee-discount", type=float, default=0.28)
    ap.add_argument("--fee-min", type=float, default=20.0)
    ap.add_argument("--tax-rate-sell", type=float, default=0.003)
    ap.add_argument("--slippage", type=float, default=0.0005)

    ap.add_argument("--out-preds", type=str, default="data/_derived/ml_runs/bband_surge_predictions_oof.csv")
    ap.add_argument("--out-preds-all", type=str, default="data/_derived/ml_runs/bband_surge_predictions_all.csv")
    ap.add_argument("--out-latest", type=str, default="data/_derived/ml_runs/bband_surge_latest.csv")
    ap.add_argument("--out-summary", type=str, default="data/_derived/ml_runs/bband_surge_summary.json")
    ap.add_argument("--out-importance", type=str, default="data/_derived/ml_runs/bband_surge_feature_importance.csv")
    ap.add_argument("--out-backtest", type=str, default="data/_derived/ml_runs/bband_surge_backtest.csv")
    args = ap.parse_args()

    if not (0 <= args.winsor_lower < args.winsor_upper <= 1):
        raise ValueError("winsor bounds must satisfy 0 <= lower < upper <= 1")

    data_path = Path(args.data)
    if not data_path.exists():
        raise FileNotFoundError(data_path)

    base_cols = [
        "symbol",
        "date",
        "open",
        "close",
        "high",
        "low",
        "net_total",
        "posnet_total",
        "negnet_total",
        "hhi_posnet",
        "hhi_negnet",
        "top_posnet_ratio",
        "top_negnet_ratio",
        "dyn_k",
        "dyn_k_neg",
        "posnet_total_pct",
        "negnet_total_pct",
        "hhi_posnet_pct",
        "hhi_negnet_pct",
        "s_stage1",
        "s_stage1_W",
        "s_stage1_W_pct",
        "s_stage1_adj",
        "s_stage1_adj_W",
        "s_stage1_adj_W_pct",
        "sell_ratio",
        "dyn_k_penalty",
        "bb_pos",
    ]
    df = pd.read_parquet(data_path, columns=base_cols)
    df["symbol"] = df["symbol"].astype(str)
    if args.symbol_len > 0:
        df = df[df["symbol"].str.len() == args.symbol_len].copy()
    df["date"] = pd.to_datetime(df["date"])
    df = build_features(df)
    df = add_bollinger_features(df)
    df = add_label_and_returns(df, args.horizon, args.surge_th, args.min_band_days)

    df["history_days"] = df.groupby("symbol")["date"].transform("count")
    df = df[df["history_days"] >= args.min_history].copy()

    label_col = f"bb_surge_{args.horizon}d_ge{int(args.surge_th*100)}_bd{args.min_band_days}"
    df_all = df.copy()
    df = df[df[label_col].notna()].copy()
    if df.empty:
        raise RuntimeError("no training rows after filtering")

    extra_features = [
        "bb_break",
        "bb_cross_up",
        "bb_dist_upper",
        "bb_dist_lower",
        "bb_upper_slope_3d",
        "bb_above_upper_5d",
        "bb_touch_upper",
        "bb_touch_upper_5d",
        "bb_above_ma20",
        "bb_squeeze_20q",
    ]
    feature_names = FEATURE_COLS + [f for f in extra_features if f not in FEATURE_COLS]
    feature_names = [f for f in feature_names if f in df.columns]

    params = json.loads(Path(args.params).read_text(encoding="utf-8")) if args.params else None

    folds = build_walk_forward_folds(df["date"], args.wf_train_months, args.wf_valid_months, args.wf_step_months)
    oof_parts = []
    fold_rows = []
    imp_acc = np.zeros(len(feature_names), dtype=float)
    imp_n = 0
    model_name = args.model

    for fold in folds:
        train_mask = (df["date"] >= fold["train_start"]) & (df["date"] <= fold["train_end"])
        valid_mask = (df["date"] >= fold["valid_start"]) & (df["date"] <= fold["valid_end"])
        tr = df[train_mask].copy()
        va = df[valid_mask].copy()
        if tr.shape[0] < args.wf_min_train_rows or va.empty:
            continue

        bounds = fit_winsor_bounds(tr, feature_names, args.winsor_lower, args.winsor_upper)
        tr = apply_winsor(tr, feature_names, bounds)
        va = apply_winsor(va, feature_names, bounds)
        tr, va, _ = fillna_by_train_median(tr, va, va, feature_names)

        X_train = tr[feature_names].to_numpy(float)
        y_train = tr[label_col].to_numpy(float)
        X_valid = va[feature_names].to_numpy(float)

        pred_fn, imp, model_name = train_classifier(args.model, X_train, y_train, args.seed + int(fold["fold"]), params)
        va["pred"] = pred_fn(X_valid)
        va["fold"] = int(fold["fold"])

        fe = eval_topn(va, label_col, args.topn)
        fold_rows.append(
            {
                "fold": int(fold["fold"]),
                "train_rows": int(tr.shape[0]),
                "valid_rows": int(va.shape[0]),
                "topn_hit_rate": fe.get("topn_hit_rate", np.nan),
                "valid_pos_rate": float(np.nanmean(va[label_col].to_numpy(float))),
            }
        )
        oof_parts.append(va[["symbol", "date", "pred", label_col, "bb_break", "bb_cross_up", "next_open", "close_h"]])
        if len(imp) == len(imp_acc):
            imp_acc += np.asarray(imp, dtype=float)
            imp_n += 1

    if not oof_parts:
        raise RuntimeError(
            "no walk-forward folds produced OOF predictions; reduce --wf-train-months "
            "or check date coverage"
        )

    oof = pd.concat(oof_parts, ignore_index=True).sort_values(["date", "pred"], ascending=[True, False])
    fold_df = pd.DataFrame(fold_rows)
    valid_eval = eval_topn(oof, label_col, args.topn)

    # Fit final model on all labeled rows for latest ranking.
    bounds_final = fit_winsor_bounds(df, feature_names, args.winsor_lower, args.winsor_upper)
    tr_all = apply_winsor(df, feature_names, bounds_final)
    all_scored = apply_winsor(df_all, feature_names, bounds_final)
    tr_all, _, all_scored = fillna_by_train_median(tr_all, tr_all, all_scored, feature_names)
    X_all_train = tr_all[feature_names].to_numpy(float)
    y_all_train = tr_all[label_col].to_numpy(float)
    pred_fn_all, imp_all, model_name = train_classifier(args.model, X_all_train, y_all_train, args.seed, params)
    all_scored["pred"] = pred_fn_all(all_scored[feature_names].to_numpy(float))

    backtest = backtest_topn_modes(
        oof,
        label_col=label_col,
        horizon=args.horizon,
        topn_list=[int(x) for x in args.topn_list.split(",") if x.strip()],
        trade_notional=args.trade_notional,
        fee_rate=args.fee_rate,
        fee_discount=args.fee_discount,
        fee_min=args.fee_min,
        tax_sell=args.tax_rate_sell,
        slippage=args.slippage,
    )

    imp_final = imp_acc / max(1, imp_n)
    if len(imp_all) == len(imp_final):
        imp_final = (imp_final + np.asarray(imp_all, dtype=float)) / 2.0

    summary = {
        "model": model_name,
        "label": label_col,
        "horizon": args.horizon,
        "surge_th": args.surge_th,
        "min_band_days": args.min_band_days,
        "symbol_len": args.symbol_len,
        "min_history": args.min_history,
        "n_labeled_rows": int(df.shape[0]),
        "n_folds": int(fold_df.shape[0]),
        "valid_pos_rate": float(np.nanmean(oof[label_col].to_numpy(float))),
        "valid_eval_topn": valid_eval,
        "folds_eval_mean": {
            "topn_hit_rate_mean": float(fold_df["topn_hit_rate"].mean()) if not fold_df.empty else np.nan,
            "topn_hit_rate_std": float(fold_df["topn_hit_rate"].std(ddof=0)) if not fold_df.empty else np.nan,
        },
        "cost_assumption": {
            "trade_notional": args.trade_notional,
            "fee_rate": args.fee_rate,
            "fee_discount": args.fee_discount,
            "fee_min": args.fee_min,
            "tax_rate_sell": args.tax_rate_sell,
            "slippage": args.slippage,
        },
    }

    out_preds = Path(args.out_preds)
    out_preds.parent.mkdir(parents=True, exist_ok=True)
    oof[["symbol", "date", "pred", label_col, "bb_break", "bb_cross_up"]].to_csv(out_preds, index=False)

    out_all = Path(args.out_preds_all)
    out_all.parent.mkdir(parents=True, exist_ok=True)
    all_scored[["symbol", "date", "pred", label_col, "bb_break", "bb_cross_up"]].to_csv(out_all, index=False)

    latest_date = pd.to_datetime(all_scored["date"]).max()
    latest = all_scored[pd.to_datetime(all_scored["date"]) == latest_date].copy().sort_values("pred", ascending=False)
    out_latest = Path(args.out_latest)
    out_latest.parent.mkdir(parents=True, exist_ok=True)
    latest[["symbol", "date", "pred", "bb_break", "bb_cross_up"]].to_csv(out_latest, index=False)

    out_imp = Path(args.out_importance)
    out_imp.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"feature": feature_names, "importance": imp_final}).sort_values("importance", ascending=False).to_csv(
        out_imp, index=False
    )

    out_bt = Path(args.out_backtest)
    out_bt.parent.mkdir(parents=True, exist_ok=True)
    backtest.to_csv(out_bt, index=False)

    out_summary = Path(args.out_summary)
    out_summary.parent.mkdir(parents=True, exist_ok=True)
    out_summary.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Wrote: {out_preds}")
    print(f"Wrote: {out_all}")
    print(f"Wrote: {out_latest}")
    print(f"Wrote: {out_imp}")
    print(f"Wrote: {out_bt}")
    print(f"Wrote: {out_summary}")


if __name__ == "__main__":
    main()
