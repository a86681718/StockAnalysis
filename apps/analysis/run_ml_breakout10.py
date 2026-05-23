#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Separate ML pipeline for breakout detection:
label = mfe_10d >= threshold (default 10%).

Optimizations in this version:
- Classification probability output (p_breakout)
- Train-fit winsorize preprocessing (applied to valid/all)
- Optional walk-forward validation
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


def eval_topn_breakout(df: pd.DataFrame, label_col: str, pred_col: str, topn: int) -> dict:
    rows = []
    total_hits = 0.0
    total_pos = 0.0
    for d, sub in df.groupby("date"):
        sub = sub.sort_values(pred_col, ascending=False)
        sub_top = sub.head(topn)
        if sub_top.empty:
            continue
        pos = sub[label_col].fillna(0).astype(float)
        hits = float(sub_top[label_col].fillna(0).sum())
        total_hits += hits
        total_pos += float(pos.sum())
        recall = hits / float(pos.sum()) if float(pos.sum()) > 0 else np.nan
        rows.append(
            {
                "date": d,
                "n": int(len(sub_top)),
                "hit_rate": float(hits / max(1.0, len(sub_top))),
                "recall": recall,
            }
        )

    if not rows:
        return {"n_dates": 0, "n_samples": 0}

    out = pd.DataFrame(rows)
    return {
        "n_dates": int(out.shape[0]),
        "n_samples": int(out["n"].sum()),
        "topn_hit_rate": float(out["hit_rate"].mean()),
        "topn_recall": float(out["recall"].dropna().mean()) if out["recall"].notna().any() else np.nan,
        "overall_recall": float(total_hits / total_pos) if total_pos > 0 else np.nan,
    }


def fit_winsor_bounds(train_df: pd.DataFrame, feature_names: list[str], lower_q: float, upper_q: float) -> dict[str, tuple[float, float]]:
    bounds: dict[str, tuple[float, float]] = {}
    if lower_q <= 0 and upper_q >= 1:
        return bounds
    for f in feature_names:
        s = pd.to_numeric(train_df[f], errors="coerce")
        lo = float(s.quantile(lower_q)) if lower_q > 0 else -np.inf
        hi = float(s.quantile(upper_q)) if upper_q < 1 else np.inf
        bounds[f] = (lo, hi)
    return bounds


def apply_winsor(df: pd.DataFrame, feature_names: list[str], bounds: dict[str, tuple[float, float]]) -> pd.DataFrame:
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
) -> tuple[Callable[[np.ndarray], np.ndarray], np.ndarray]:
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

    def _pred_fn(X: np.ndarray) -> np.ndarray:
        return model.predict_proba(X)[:, 1]

    return _pred_fn, imp


def train_logit_classifier(X_train: np.ndarray, y_train: np.ndarray, seed: int) -> tuple[Callable[[np.ndarray], np.ndarray], np.ndarray]:
    from sklearn.linear_model import LogisticRegression

    model = LogisticRegression(
        max_iter=2000,
        C=1.0,
        class_weight="balanced",
        random_state=seed,
    )
    model.fit(X_train, y_train)
    coef = np.abs(model.coef_).ravel()

    def _pred_fn(X: np.ndarray) -> np.ndarray:
        return model.predict_proba(X)[:, 1]

    return _pred_fn, coef


def train_classifier(
    model_name: str,
    X_train: np.ndarray,
    y_train: np.ndarray,
    seed: int,
    params: dict | None,
) -> tuple[Callable[[np.ndarray], np.ndarray], np.ndarray, str]:
    if model_name == "lgbm":
        try:
            fn, imp = train_lgbm_classifier(X_train, y_train, seed, params=params)
            return fn, imp, "lgbm"
        except Exception as exc:
            print(f"[WARN] LightGBM unavailable, fallback to logit: {exc}")
    fn, imp = train_logit_classifier(X_train, y_train, seed)
    return fn, imp, "logit"


def build_walk_forward_folds(
    dates: pd.Series,
    train_months: int,
    valid_months: int,
    step_months: int,
) -> list[dict]:
    dmin = pd.to_datetime(dates.min())
    dmax = pd.to_datetime(dates.max())

    # start valid window after initial train window
    valid_start = (dmin + pd.offsets.MonthBegin(train_months)).normalize()
    folds: list[dict] = []
    fold_id = 1

    while valid_start <= dmax:
        train_end = valid_start - pd.Timedelta(days=1)
        valid_end = (valid_start + pd.DateOffset(months=valid_months)) - pd.Timedelta(days=1)
        folds.append(
            {
                "fold": fold_id,
                "train_start": dmin,
                "train_end": train_end,
                "valid_start": valid_start,
                "valid_end": min(valid_end, dmax),
            }
        )
        fold_id += 1
        valid_start = valid_start + pd.DateOffset(months=step_months)

    return folds


def run_symbol_split(
    df: pd.DataFrame,
    df_all: pd.DataFrame,
    feature_names: list[str],
    label_col: str,
    args,
    params: dict | None,
) -> tuple[pd.DataFrame, dict, np.ndarray, str, dict]:
    symbols = df["symbol"].dropna().unique().tolist()
    rng = np.random.default_rng(args.seed)
    rng.shuffle(symbols)
    n_valid = max(1, int(len(symbols) * args.valid_ratio))
    valid_symbols = set(symbols[:n_valid])

    train_df = df[~df["symbol"].isin(valid_symbols)].copy()
    valid_df = df[df["symbol"].isin(valid_symbols)].copy()
    if train_df.empty or valid_df.empty:
        raise RuntimeError("train/valid split empty, adjust valid_ratio")

    bounds = fit_winsor_bounds(train_df, feature_names, args.winsor_lower, args.winsor_upper)
    train_df = apply_winsor(train_df, feature_names, bounds)
    valid_df = apply_winsor(valid_df, feature_names, bounds)

    X_train = train_df[feature_names].to_numpy(float)
    y_train = train_df[label_col].to_numpy(float)
    X_valid = valid_df[feature_names].to_numpy(float)
    y_valid = valid_df[label_col].to_numpy(float)

    pred_fn, imp, model_name = train_classifier(args.model, X_train, y_train, args.seed, params)
    train_df["pred"] = pred_fn(X_train)
    valid_df["pred"] = pred_fn(X_valid)

    train_eval = eval_topn_breakout(train_df, label_col, "pred", args.topn)
    valid_eval = eval_topn_breakout(valid_df, label_col, "pred", args.topn)

    # final model for all-date predictions (fit on all labeled rows)
    all_labeled = df.copy()
    all_labeled = apply_winsor(all_labeled, feature_names, bounds)
    X_all_train = all_labeled[feature_names].to_numpy(float)
    y_all_train = all_labeled[label_col].to_numpy(float)
    final_pred_fn, final_imp, final_name = train_classifier(args.model, X_all_train, y_all_train, args.seed, params)

    df_all_scored = apply_winsor(df_all, feature_names, bounds)
    df_all_scored["pred"] = final_pred_fn(df_all_scored[feature_names].to_numpy(float))

    summary_extra = {
        "cv_mode": "symbol_split",
        "winsor_lower": args.winsor_lower,
        "winsor_upper": args.winsor_upper,
        "n_symbols_train": int(train_df["symbol"].nunique()),
        "n_symbols_valid": int(valid_df["symbol"].nunique()),
        "train_pos_rate": float(np.nanmean(y_train)),
        "valid_pos_rate": float(np.nanmean(y_valid)),
        "n_train": int(train_df.shape[0]),
        "n_valid": int(valid_df.shape[0]),
        "train_eval": train_eval,
        "valid_eval": valid_eval,
    }

    return valid_df, df_all_scored, final_imp, final_name, summary_extra


def run_walk_forward(
    df: pd.DataFrame,
    df_all: pd.DataFrame,
    feature_names: list[str],
    label_col: str,
    args,
    params: dict | None,
) -> tuple[pd.DataFrame, pd.DataFrame, np.ndarray, str, dict]:
    folds = build_walk_forward_folds(
        df["date"],
        train_months=args.wf_train_months,
        valid_months=args.wf_valid_months,
        step_months=args.wf_step_months,
    )
    if not folds:
        raise RuntimeError(
            "no walk-forward folds created; reduce --wf-train-months/--wf-valid-months "
            "or check available date range"
        )

    oof_rows = []
    fold_rows = []
    imp_rows = []
    model_name = ""

    for fold in folds:
        train_mask = (df["date"] >= fold["train_start"]) & (df["date"] <= fold["train_end"])
        valid_mask = (df["date"] >= fold["valid_start"]) & (df["date"] <= fold["valid_end"])

        train_df = df[train_mask].copy()
        valid_df = df[valid_mask].copy()
        if train_df.shape[0] < args.wf_min_train_rows or valid_df.empty:
            continue

        # require both classes in training fold
        y_train_fold = train_df[label_col].to_numpy(float)
        if np.nanmin(y_train_fold) == np.nanmax(y_train_fold):
            continue

        bounds = fit_winsor_bounds(train_df, feature_names, args.winsor_lower, args.winsor_upper)
        train_df = apply_winsor(train_df, feature_names, bounds)
        valid_df = apply_winsor(valid_df, feature_names, bounds)

        X_train = train_df[feature_names].to_numpy(float)
        y_train = train_df[label_col].to_numpy(float)
        X_valid = valid_df[feature_names].to_numpy(float)

        pred_fn, imp, model_name = train_classifier(args.model, X_train, y_train, args.seed + int(fold["fold"]), params)
        valid_df["pred"] = pred_fn(X_valid)
        valid_df["fold"] = int(fold["fold"])

        fe = eval_topn_breakout(valid_df, label_col, "pred", args.topn)
        fold_rows.append(
            {
                "fold": int(fold["fold"]),
                "train_start": str(fold["train_start"].date()),
                "train_end": str(fold["train_end"].date()),
                "valid_start": str(fold["valid_start"].date()),
                "valid_end": str(fold["valid_end"].date()),
                "n_train": int(train_df.shape[0]),
                "n_valid": int(valid_df.shape[0]),
                "valid_pos_rate": float(np.nanmean(valid_df[label_col].to_numpy(float))),
                "topn_hit_rate": fe.get("topn_hit_rate", np.nan),
                "topn_recall": fe.get("topn_recall", np.nan),
                "overall_recall": fe.get("overall_recall", np.nan),
            }
        )
        oof_rows.append(valid_df[["symbol", "date", label_col, "pred", "fold"]])
        imp_rows.append(pd.DataFrame({"feature": feature_names, "importance": imp}))

    if not oof_rows:
        raise RuntimeError("no valid walk-forward folds after filtering; adjust window/min rows")

    oof = pd.concat(oof_rows, ignore_index=True)
    oof = oof.sort_values(["date", "symbol", "fold"]).drop_duplicates(["date", "symbol"], keep="last")
    valid_eval = eval_topn_breakout(oof, label_col, "pred", args.topn)

    fold_df = pd.DataFrame(fold_rows)
    avg_eval = {
        "topn_hit_rate_mean": float(fold_df["topn_hit_rate"].mean()),
        "topn_hit_rate_std": float(fold_df["topn_hit_rate"].std(ddof=0)),
        "topn_recall_mean": float(fold_df["topn_recall"].mean()),
        "topn_recall_std": float(fold_df["topn_recall"].std(ddof=0)),
        "overall_recall_mean": float(fold_df["overall_recall"].mean()),
        "overall_recall_std": float(fold_df["overall_recall"].std(ddof=0)),
    }

    imp_all = pd.concat(imp_rows, ignore_index=True)
    imp_mean = imp_all.groupby("feature", as_index=False)["importance"].mean()

    # final model fit on all labeled rows for all-date predictions
    bounds_final = fit_winsor_bounds(df, feature_names, args.winsor_lower, args.winsor_upper)
    train_final = apply_winsor(df, feature_names, bounds_final)
    X_train_final = train_final[feature_names].to_numpy(float)
    y_train_final = train_final[label_col].to_numpy(float)
    final_pred_fn, _, final_model_name = train_classifier(args.model, X_train_final, y_train_final, args.seed, params)

    df_all_scored = apply_winsor(df_all, feature_names, bounds_final)
    df_all_scored["pred"] = final_pred_fn(df_all_scored[feature_names].to_numpy(float))

    summary_extra = {
        "cv_mode": "walk_forward",
        "winsor_lower": args.winsor_lower,
        "winsor_upper": args.winsor_upper,
        "wf_train_months": args.wf_train_months,
        "wf_valid_months": args.wf_valid_months,
        "wf_step_months": args.wf_step_months,
        "wf_min_train_rows": args.wf_min_train_rows,
        "n_folds": int(fold_df.shape[0]),
        "folds_eval_mean_std": avg_eval,
        "valid_eval": valid_eval,
        "valid_pos_rate": float(np.nanmean(oof[label_col].to_numpy(float))),
        "n_valid": int(oof.shape[0]),
    }

    return oof, df_all_scored, imp_mean["importance"].to_numpy(float), final_model_name, summary_extra, fold_df


def main() -> None:
    parser = argparse.ArgumentParser(description="Breakout ML: mfe_10d >= threshold")
    parser.add_argument("--data", type=str, default="data/_derived/scored.parquet")
    parser.add_argument("--label-th", type=float, default=0.10)
    parser.add_argument("--symbol-len", type=int, default=4)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--topn", type=int, default=10)
    parser.add_argument("--model", type=str, default="lgbm", choices=["lgbm", "logit", "ridge"])
    parser.add_argument("--params", type=str, default="", help="optional json file for LightGBM params")

    parser.add_argument("--cv-mode", type=str, default="walk_forward", choices=["symbol_split", "walk_forward"])
    parser.add_argument("--valid-ratio", type=float, default=0.2, help="used only in symbol_split")
    parser.add_argument("--wf-train-months", type=int, default=6)
    parser.add_argument("--wf-valid-months", type=int, default=1)
    parser.add_argument("--wf-step-months", type=int, default=1)
    parser.add_argument("--wf-min-train-rows", type=int, default=5000)

    parser.add_argument("--winsor-lower", type=float, default=0.01)
    parser.add_argument("--winsor-upper", type=float, default=0.99)

    parser.add_argument("--out-preds", type=str, default="data/_derived/ml_runs/breakout10_predictions.csv")
    parser.add_argument("--out-preds-all", type=str, default="data/_derived/ml_runs/breakout10_predictions_all.csv")
    parser.add_argument("--out-summary", type=str, default="data/_derived/ml_runs/breakout10_summary.json")
    parser.add_argument("--out-importance", type=str, default="data/_derived/ml_runs/breakout10_feature_importance.csv")
    parser.add_argument("--out-folds", type=str, default="data/_derived/ml_runs/breakout10_walkforward_folds.csv")
    parser.add_argument("--predict-latest", action="store_true", help="write predictions for latest date")
    parser.add_argument("--predict-date", type=str, default="", help="optional date override (YYYY-MM-DD)")
    parser.add_argument("--out-latest", type=str, default="data/_derived/ml_runs/breakout10_latest.csv")
    args = parser.parse_args()

    if args.model == "ridge":
        args.model = "logit"

    if not (0 <= args.winsor_lower < args.winsor_upper <= 1):
        raise ValueError("winsor bounds must satisfy 0 <= lower < upper <= 1")

    data_path = Path(args.data)
    if not data_path.exists():
        raise FileNotFoundError(f"missing {data_path}")

    use_cols = [
        "symbol",
        "date",
        "mfe_10d",
        "net_total",
        "posnet_total",
        "negnet_total",
        "hhi_posnet",
        "hhi_negnet",
        "top_posnet_ratio",
        "top_negnet_ratio",
        "dyn_k",
        "dyn_k_neg",
        "close",
        "high",
        "low",
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

    df = pd.read_parquet(data_path, columns=use_cols)
    df["symbol"] = df["symbol"].astype(str)
    if args.symbol_len > 0:
        df = df[df["symbol"].str.len() == args.symbol_len].copy()
    df["date"] = pd.to_datetime(df["date"])
    df = build_features(df)

    label_col = f"breakout_10d_ge{int(args.label_th * 100)}"
    df[label_col] = np.where(df["mfe_10d"].notna(), (df["mfe_10d"] >= args.label_th).astype(float), np.nan)

    df_all = df.copy()
    df = df[df[label_col].notna()].copy()
    if df.empty:
        raise RuntimeError("no training data after filtering")

    feature_names = FEATURE_COLS.copy()
    params = json.loads(Path(args.params).read_text(encoding="utf-8")) if args.params else None

    fold_df = None
    if args.cv_mode == "symbol_split":
        valid_preds, df_all_scored, imp, model_name, extra = run_symbol_split(
            df, df_all, feature_names, label_col, args, params
        )
    else:
        valid_preds, df_all_scored, imp, model_name, extra, fold_df = run_walk_forward(
            df, df_all, feature_names, label_col, args, params
        )

    summary = {
        "model": model_name,
        "label": label_col,
        "label_threshold": args.label_th,
        "topn": args.topn,
        **extra,
    }

    out_summary = Path(args.out_summary)
    out_summary.parent.mkdir(parents=True, exist_ok=True)
    out_summary.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    imp_df = pd.DataFrame({"feature": feature_names, "importance": imp})
    imp_df = imp_df.sort_values("importance", ascending=False)
    out_imp = Path(args.out_importance)
    out_imp.parent.mkdir(parents=True, exist_ok=True)
    imp_df.to_csv(out_imp, index=False)

    out_preds = Path(args.out_preds)
    out_preds.parent.mkdir(parents=True, exist_ok=True)
    pred_cols = ["symbol", "date", "pred", label_col]
    if "fold" in valid_preds.columns:
        pred_cols.append("fold")
    valid_preds[pred_cols].to_csv(out_preds, index=False)

    out_all = Path(args.out_preds_all)
    out_all.parent.mkdir(parents=True, exist_ok=True)
    df_all_scored[["symbol", "date", "pred", label_col]].to_csv(out_all, index=False)

    if fold_df is not None:
        out_folds = Path(args.out_folds)
        out_folds.parent.mkdir(parents=True, exist_ok=True)
        fold_df.to_csv(out_folds, index=False)
        print(f"Wrote: {out_folds}")

    if args.predict_latest or args.predict_date:
        pred_df = df_all_scored.copy()
        pred_df["date"] = pd.to_datetime(pred_df["date"])
        target = pd.to_datetime(args.predict_date) if args.predict_date else pred_df["date"].max()
        latest = pred_df[pred_df["date"] == target].copy().sort_values("pred", ascending=False)
        Path(args.out_latest).parent.mkdir(parents=True, exist_ok=True)
        latest[["symbol", "date", "pred", label_col]].to_csv(args.out_latest, index=False)
        print(f"Wrote: {args.out_latest}")

    print(f"Wrote: {out_summary}")
    print(f"Wrote: {out_imp}")
    print(f"Wrote: {out_preds}")
    print(f"Wrote: {out_all}")


if __name__ == "__main__":
    main()
