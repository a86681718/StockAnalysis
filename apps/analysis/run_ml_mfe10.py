#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Baseline supervised model for mfe_10d with symbol-level split.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


FEATURE_COLS = [
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
    "hl_range",
    "close_pos",
    "ret_1d",
    "ret_5d",
    "ret_10d",
    "ret_20d",
    "mom_5d",
    "mom_10d",
    "mom_20d",
    "vol_5d",
    "vol_10d",
    "vol_20d",
    "posnet_sum_5d",
    "posnet_sum_10d",
    "posnet_sum_20d",
    "negnet_sum_5d",
    "negnet_sum_10d",
    "negnet_sum_20d",
    "net_sum_5d",
    "net_sum_10d",
    "net_sum_20d",
    "posnet_z_20d",
    "negnet_z_20d",
    "net_z_20d",
    "hhi_posnet_z_20d",
    "hhi_negnet_z_20d",
    "posnet_share",
    "net_imbalance",
    "close_vs_min_20d",
    "close_vs_max_20d",
    "close_vs_min_60d",
    "close_vs_max_60d",
    "bb_width",
    "atr_14",
    "close_to_ma20",
    "posnet_chg",
    "negnet_chg",
    "net_chg",
    "bb_pos",
]


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["symbol", "date"]).copy()
    g = df.groupby("symbol", group_keys=False)

    for h in (1, 5, 10, 20):
        df[f"ret_{h}d"] = g["close"].pct_change(periods=h)

    df["mom_5d"] = g["ret_1d"].rolling(5, min_periods=5).mean().reset_index(level=0, drop=True)
    df["mom_10d"] = g["ret_1d"].rolling(10, min_periods=10).mean().reset_index(level=0, drop=True)
    df["mom_20d"] = g["ret_1d"].rolling(20, min_periods=20).mean().reset_index(level=0, drop=True)
    df["vol_5d"] = g["ret_1d"].rolling(5, min_periods=5).std().reset_index(level=0, drop=True)
    df["vol_10d"] = g["ret_1d"].rolling(10, min_periods=10).std().reset_index(level=0, drop=True)
    df["vol_20d"] = g["ret_1d"].rolling(20, min_periods=20).std().reset_index(level=0, drop=True)

    denom = (df["high"] - df["low"]).replace(0, np.nan)
    df["hl_range"] = (df["high"] - df["low"]) / df["close"].replace(0, np.nan)
    df["close_pos"] = (df["close"] - df["low"]) / denom

    df["posnet_chg"] = g["posnet_total"].pct_change(periods=1)
    df["negnet_chg"] = g["negnet_total"].pct_change(periods=1)
    df["net_chg"] = g["net_total"].pct_change(periods=1)

    for w in (5, 10, 20):
        df[f"posnet_sum_{w}d"] = g["posnet_total"].rolling(w, min_periods=w).sum().reset_index(level=0, drop=True)
        df[f"negnet_sum_{w}d"] = g["negnet_total"].rolling(w, min_periods=w).sum().reset_index(level=0, drop=True)
        df[f"net_sum_{w}d"] = g["net_total"].rolling(w, min_periods=w).sum().reset_index(level=0, drop=True)

    mean20_pos = g["posnet_total"].rolling(20, min_periods=20).mean().reset_index(level=0, drop=True)
    std20_pos = g["posnet_total"].rolling(20, min_periods=20).std().reset_index(level=0, drop=True)
    mean20_neg = g["negnet_total"].rolling(20, min_periods=20).mean().reset_index(level=0, drop=True)
    std20_neg = g["negnet_total"].rolling(20, min_periods=20).std().reset_index(level=0, drop=True)
    mean20_net = g["net_total"].rolling(20, min_periods=20).mean().reset_index(level=0, drop=True)
    std20_net = g["net_total"].rolling(20, min_periods=20).std().reset_index(level=0, drop=True)
    mean20_hhi_pos = g["hhi_posnet"].rolling(20, min_periods=20).mean().reset_index(level=0, drop=True)
    std20_hhi_pos = g["hhi_posnet"].rolling(20, min_periods=20).std().reset_index(level=0, drop=True)
    mean20_hhi_neg = g["hhi_negnet"].rolling(20, min_periods=20).mean().reset_index(level=0, drop=True)
    std20_hhi_neg = g["hhi_negnet"].rolling(20, min_periods=20).std().reset_index(level=0, drop=True)

    df["posnet_z_20d"] = (df["posnet_total"] - mean20_pos) / std20_pos
    df["negnet_z_20d"] = (df["negnet_total"] - mean20_neg) / std20_neg
    df["net_z_20d"] = (df["net_total"] - mean20_net) / std20_net
    df["hhi_posnet_z_20d"] = (df["hhi_posnet"] - mean20_hhi_pos) / std20_hhi_pos
    df["hhi_negnet_z_20d"] = (df["hhi_negnet"] - mean20_hhi_neg) / std20_hhi_neg

    denom_flow = (df["posnet_total"] + df["negnet_total"]).replace(0, np.nan)
    df["posnet_share"] = df["posnet_total"] / denom_flow
    df["net_imbalance"] = (df["posnet_total"] - df["negnet_total"]) / denom_flow

    min20 = g["close"].rolling(20, min_periods=20).min().reset_index(level=0, drop=True)
    max20 = g["close"].rolling(20, min_periods=20).max().reset_index(level=0, drop=True)
    min60 = g["close"].rolling(60, min_periods=60).min().reset_index(level=0, drop=True)
    max60 = g["close"].rolling(60, min_periods=60).max().reset_index(level=0, drop=True)
    df["close_vs_min_20d"] = df["close"] / min20 - 1.0
    df["close_vs_max_20d"] = df["close"] / max20 - 1.0
    df["close_vs_min_60d"] = df["close"] / min60 - 1.0
    df["close_vs_max_60d"] = df["close"] / max60 - 1.0

    window = 20
    ma = g["close"].rolling(window, min_periods=window).mean().reset_index(level=0, drop=True)
    std = g["close"].rolling(window, min_periods=window).std().reset_index(level=0, drop=True)
    upper = ma + 2 * std
    lower = ma - 2 * std
    df["bb_pos"] = (df["close"] - lower) / (upper - lower)
    df["bb_width"] = (upper - lower) / df["close"].replace(0, np.nan)
    df["close_to_ma20"] = df["close"] / ma - 1.0

    prev_close = g["close"].shift(1)
    tr1 = df["high"] - df["low"]
    tr2 = (df["high"] - prev_close).abs()
    tr3 = (df["low"] - prev_close).abs()
    df["tr"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df["atr_14"] = g["tr"].rolling(14, min_periods=14).mean().reset_index(level=0, drop=True)
    df["atr_14"] = df["atr_14"] / df["close"].replace(0, np.nan)

    return df


def _mean_ret_10d(s: pd.Series) -> pd.Series:
    future = s.shift(-1)
    mean_next = future.rolling(10, min_periods=10).mean().shift(-(10 - 1))
    return (mean_next / s) - 1.0


def _days_above_10d(s: pd.Series) -> pd.Series:
    arr = s.to_numpy(float)
    n = len(arr)
    out = np.full(n, np.nan)
    for i in range(n):
        j_end = i + 10
        if j_end >= n:
            continue
        base = arr[i]
        if not np.isfinite(base):
            continue
        window_vals = arr[i + 1 : j_end + 1]
        out[i] = float(np.sum(window_vals > base))
    return pd.Series(out, index=s.index)


def eval_topn(df: pd.DataFrame, label_col: str, pred_col: str, topn: int) -> dict:
    rows = []
    for d, sub in df.groupby("date"):
        sub = sub.sort_values(pred_col, ascending=False).head(topn)
        if sub.empty:
            continue
        r = sub[label_col]
        rows.append(
            {
                "date": d,
                "n": int(len(sub)),
                "mean": float(r.mean()),
                "median": float(r.median()),
                "win_rate": float((r > 0).mean()),
            }
        )
    if not rows:
        return {"n_dates": 0, "n_samples": 0}

    out = pd.DataFrame(rows)
    return {
        "n_dates": int(out.shape[0]),
        "n_samples": int(out["n"].sum()),
        "mean": float(out["mean"].mean()),
        "median": float(out["median"].mean()),
        "win_rate": float(out["win_rate"].mean()),
    }


def fit_ridge(X: np.ndarray, y: np.ndarray, alpha: float = 1.0) -> np.ndarray:
    X = np.asarray(X, dtype=float)
    y = np.asarray(y, dtype=float).reshape(-1, 1)
    n = X.shape[0]
    X1 = np.hstack([np.ones((n, 1)), X])
    I = np.eye(X1.shape[1])
    I[0, 0] = 0.0
    beta = np.linalg.solve(X1.T @ X1 + alpha * I, X1.T @ y)
    return beta.ravel()


def predict_ridge(beta: np.ndarray, X: np.ndarray) -> np.ndarray:
    X = np.asarray(X, dtype=float)
    X1 = np.hstack([np.ones((X.shape[0], 1)), X])
    return X1 @ beta


def spearman_corr(x: pd.Series, y: pd.Series) -> float | None:
    if x.empty or y.empty:
        return None
    xr = x.rank(method="average")
    yr = y.rank(method="average")
    vx = xr - xr.mean()
    vy = yr - yr.mean()
    denom = float(np.sqrt((vx ** 2).sum()) * np.sqrt((vy ** 2).sum()))
    if denom == 0:
        return None
    return float((vx * vy).sum() / denom)


def train_lgbm(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_valid: np.ndarray,
    seed: int,
    params: dict | None = None,
) -> tuple[callable, np.ndarray]:
    import lightgbm as lgb

    base_params = dict(
        n_estimators=400,
        learning_rate=0.05,
        max_depth=-1,
        num_leaves=63,
        min_child_samples=50,
        subsample=0.8,
        subsample_freq=1,
        colsample_bytree=0.8,
        reg_alpha=0.0,
        reg_lambda=0.0,
        verbosity=-1,
        force_row_wise=True,
        random_state=seed,
    )
    if params:
        base_params.update(params)

    model = lgb.LGBMRegressor(**base_params)
    model.fit(X_train, y_train)
    importance = model.booster_.feature_importance(importance_type="gain")
    return model.predict, importance


def train_model(
    model_name: str,
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_valid: np.ndarray,
    seed: int,
    params: dict | None = None,
) -> tuple[callable, np.ndarray, str]:
    if model_name == "lgbm":
        try:
            predict_fn, importance = train_lgbm(X_train, y_train, X_valid, seed, params=params)
            return predict_fn, importance, "lgbm"
        except Exception as exc:
            print(f"[WARN] LightGBM unavailable, fallback to ridge: {exc}")

    beta = fit_ridge(X_train, y_train, alpha=1.0)
    importance = np.abs(beta[1:])
    return lambda X: predict_ridge(beta, X), importance, "ridge"


def main() -> None:
    parser = argparse.ArgumentParser(description="Baseline ML for mfe_10d (symbol-level split)")
    parser.add_argument("--data", type=str, default="data/_derived/scored.parquet")
    parser.add_argument("--label", type=str, default="mfe_10d")
    parser.add_argument("--risk-k", type=float, default=0.5, help="risk penalty for risk_mfe10 label")
    parser.add_argument("--valid-ratio", type=float, default=0.2)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--topn", type=int, default=10)
    parser.add_argument("--model", type=str, default="lgbm", choices=["lgbm", "ridge"])
    parser.add_argument("--params", type=str, default="", help="optional json file for LightGBM params")
    parser.add_argument("--out-preds", type=str, default="data/_derived/ml_mfe10_predictions.csv")
    parser.add_argument("--out-preds-all", type=str, default="", help="optional csv for all-date predictions")
    parser.add_argument("--out-summary", type=str, default="data/_derived/ml_mfe10_summary.json")
    parser.add_argument("--out-importance", type=str, default="data/_derived/ml_mfe10_feature_importance.csv")
    parser.add_argument("--select-top", type=int, default=0, help="retrain using top N features by importance")
    parser.add_argument("--tune", type=int, default=0, help="random search trials for LightGBM")
    parser.add_argument("--out-tune", type=str, default="data/_derived/ml_mfe10_tune.csv")
    parser.add_argument("--out-best", type=str, default="data/_derived/ml_mfe10_tune_best.json")
    parser.add_argument("--predict-latest", action="store_true", help="write predictions for latest date")
    parser.add_argument("--predict-date", type=str, default="", help="optional date override (YYYY-MM-DD)")
    parser.add_argument("--out-latest", type=str, default="data/_derived/ml_mfe10_latest.csv")
    args = parser.parse_args()

    data_path = Path(args.data)
    if not data_path.exists():
        raise FileNotFoundError(f"missing {data_path}")

    use_cols = [
        "symbol",
        "date",
        "mfe_10d",
        "mae_5d",
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
    if args.label not in (
        "risk_mfe10",
        "mean_ret_10d",
        "mret10",
        "mean_ret_10d_weighted",
        "wret10",
        "mean_ret_10d_weighted_ge3",
        "wret10_ge3",
        "mean_ret_10d_weighted_ge4",
        "wret10_ge4",
        "mean_ret_10d_weighted_ge5",
        "wret10_ge5",
        "mean_ret_10d_risk",
        "mret10_risk",
        "mean_ret_10d_weighted_risk",
        "wret10_risk",
        "days_above_10d_ge4",
        "days_above_10d_ge5",
    ):
        use_cols.append(args.label)
    df = pd.read_parquet(data_path, columns=use_cols)
    df["date"] = pd.to_datetime(df["date"])

    df = build_features(df)

    label_col = args.label
    if args.label == "risk_mfe10":
        if "mfe_10d" not in df.columns or "mae_5d" not in df.columns:
            raise RuntimeError("risk_mfe10 requires mfe_10d and mae_5d")
        df["risk_mfe10"] = df["mfe_10d"] - args.risk_k * df["mae_5d"].abs()
        label_col = "risk_mfe10"
    elif args.label in (
        "mean_ret_10d",
        "mret10",
        "mean_ret_10d_weighted",
        "wret10",
        "mean_ret_10d_weighted_ge3",
        "wret10_ge3",
        "mean_ret_10d_weighted_ge4",
        "wret10_ge4",
        "mean_ret_10d_weighted_ge5",
        "wret10_ge5",
        "mean_ret_10d_risk",
        "mret10_risk",
        "mean_ret_10d_weighted_risk",
        "wret10_risk",
        "days_above_10d_ge4",
        "days_above_10d_ge5",
    ):
        g = df.groupby("symbol", group_keys=False)
        df["mean_ret_10d"] = g["close"].apply(_mean_ret_10d)
        df["days_above_10d"] = g["close"].apply(_days_above_10d)
        df["mean_ret_10d_weighted"] = df["mean_ret_10d"] * (df["days_above_10d"] / 10.0)

        if args.label in ("mean_ret_10d", "mret10"):
            label_col = "mean_ret_10d"
        elif args.label in ("mean_ret_10d_weighted", "wret10"):
            label_col = "mean_ret_10d_weighted"
        elif args.label in ("mean_ret_10d_weighted_ge3", "wret10_ge3"):
            df["mean_ret_10d_weighted_ge3"] = df["mean_ret_10d_weighted"].where(df["days_above_10d"] >= 3)
            label_col = "mean_ret_10d_weighted_ge3"
        elif args.label in ("mean_ret_10d_weighted_ge4", "wret10_ge4"):
            df["mean_ret_10d_weighted_ge4"] = df["mean_ret_10d_weighted"].where(df["days_above_10d"] >= 4)
            label_col = "mean_ret_10d_weighted_ge4"
        elif args.label in ("mean_ret_10d_weighted_ge5", "wret10_ge5"):
            df["mean_ret_10d_weighted_ge5"] = df["mean_ret_10d_weighted"].where(df["days_above_10d"] >= 5)
            label_col = "mean_ret_10d_weighted_ge5"
        elif args.label in ("mean_ret_10d_risk", "mret10_risk"):
            if "mae_5d" not in df.columns:
                raise RuntimeError("mean_ret_10d_risk requires mae_5d")
            df["mean_ret_10d_risk"] = df["mean_ret_10d"] - args.risk_k * df["mae_5d"].abs()
            label_col = "mean_ret_10d_risk"
        elif args.label in ("mean_ret_10d_weighted_risk", "wret10_risk"):
            if "mae_5d" not in df.columns:
                raise RuntimeError("mean_ret_10d_weighted_risk requires mae_5d")
            df["mean_ret_10d_weighted_risk"] = df["mean_ret_10d_weighted"] - args.risk_k * df["mae_5d"].abs()
            label_col = "mean_ret_10d_weighted_risk"
        elif args.label == "days_above_10d_ge4":
            df["days_above_10d_ge4"] = (df["days_above_10d"] >= 4).astype(float)
            label_col = "days_above_10d_ge4"
        elif args.label == "days_above_10d_ge5":
            df["days_above_10d_ge5"] = (df["days_above_10d"] >= 5).astype(float)
            label_col = "days_above_10d_ge5"

    df_all = df.copy()

    # Drop rows with missing label only; feature NaNs will be imputed
    mask = df[label_col].notna()
    df_train = df[mask].copy()

    if df_train.empty:
        raise RuntimeError("no training data after filtering")

    symbols = df_train["symbol"].dropna().unique().tolist()
    rng = np.random.default_rng(args.seed)
    rng.shuffle(symbols)
    n_valid = max(1, int(len(symbols) * args.valid_ratio))
    valid_symbols = set(symbols[:n_valid])

    train = df_train[~df_train["symbol"].isin(valid_symbols)].copy()
    valid = df_train[df_train["symbol"].isin(valid_symbols)].copy()
    if train.empty or valid.empty:
        raise RuntimeError("train/valid split empty, adjust valid_ratio")

    feature_names = FEATURE_COLS.copy()
    X_train = train[feature_names].to_numpy()
    y_train = train[label_col].to_numpy()
    X_valid = valid[feature_names].to_numpy()

    # Replace inf with NaN before imputing
    X_train = np.where(np.isfinite(X_train), X_train, np.nan)
    X_valid = np.where(np.isfinite(X_valid), X_valid, np.nan)

    # Median impute using train stats
    med = np.nanmedian(X_train, axis=0)
    X_train = np.where(np.isnan(X_train), med, X_train)
    X_valid = np.where(np.isnan(X_valid), med, X_valid)

    # Standardize using train stats
    mu = X_train.mean(axis=0)
    sigma = X_train.std(axis=0)
    sigma = np.where(sigma == 0, 1.0, sigma)
    X_train = (X_train - mu) / sigma
    X_valid = (X_valid - mu) / sigma

    rng = np.random.default_rng(args.seed)

    params_override = None
    if args.params:
        params_path = Path(args.params)
        if params_path.exists():
            params_override = json.loads(params_path.read_text(encoding="utf-8"))
            if isinstance(params_override, dict) and "best_params" in params_override:
                params_override = params_override["best_params"]

    if args.tune and args.tune > 0:
        if args.model != "lgbm":
            print("[WARN] --tune requires LightGBM; switching model to lgbm")
            args.model = "lgbm"

        results = []
        best_score = -1e9
        best_params = None
        best_pred = None
        best_importance = None

        for i in range(int(args.tune)):
            params = dict(
                num_leaves=int(rng.integers(31, 129)),
                max_depth=int(rng.choice([-1, 4, 6, 8, 10, 12])),
                min_child_samples=int(rng.integers(20, 201)),
                subsample=float(rng.uniform(0.6, 1.0)),
                subsample_freq=int(rng.integers(1, 8)),
                colsample_bytree=float(rng.uniform(0.6, 1.0)),
                reg_alpha=float(rng.uniform(0.0, 5.0)),
                reg_lambda=float(rng.uniform(0.0, 5.0)),
                learning_rate=float(10 ** rng.uniform(-2.3, -1.0)),
                n_estimators=int(rng.integers(200, 600)),
            )
            predict_fn, importance, _ = train_model("lgbm", X_train, y_train, X_valid, args.seed, params=params)
            pred = predict_fn(X_valid)
            tmp = valid.copy()
            tmp["pred"] = pred
            spearman = spearman_corr(tmp[label_col], tmp["pred"]) or 0.0
            topn_stats = eval_topn(tmp, label_col, "pred", args.topn)
            score = float(topn_stats.get("mean", 0.0) or 0.0)

            row = dict(params)
            row.update(
                {
                    "trial": i + 1,
                    "spearman": spearman,
                    "topn_mean": score,
                    "topn_win_rate": topn_stats.get("win_rate"),
                }
            )
            results.append(row)

            if score > best_score:
                best_score = score
                best_params = params
                best_pred = pred
                best_importance = importance

        out_tune = Path(args.out_tune)
        out_tune.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(results).sort_values("topn_mean", ascending=False).to_csv(out_tune, index=False)

        best_payload = {
            "best_topn_mean": best_score,
            "best_params": best_params,
        }
        Path(args.out_best).write_text(json.dumps(best_payload, ensure_ascii=False, indent=2), encoding="utf-8")

        predict_fn, importance, model_name = train_model(
            "lgbm",
            X_train,
            y_train,
            X_valid,
            args.seed,
            params=best_params,
        )
        preds = predict_fn(X_valid)
    else:
        predict_fn, importance, model_name = train_model(
            args.model,
            X_train,
            y_train,
            X_valid,
            args.seed,
            params=params_override,
        )
        preds = predict_fn(X_valid)

    valid["pred"] = preds

    if args.select_top and args.select_top > 0 and args.select_top < len(feature_names):
        top_idx = np.argsort(importance)[::-1][: args.select_top]
        feature_names = [feature_names[i] for i in top_idx]
        X_train = X_train[:, top_idx]
        X_valid = X_valid[:, top_idx]
        med = med[top_idx]
        mu = mu[top_idx]
        sigma = sigma[top_idx]
        predict_fn, importance, model_name = train_model(model_name, X_train, y_train, X_valid, args.seed)
        valid["pred"] = predict_fn(X_valid)

    spearman = spearman_corr(valid[label_col], valid["pred"])
    topn_stats = eval_topn(valid, label_col, "pred", args.topn)
    baseline = {
        "mean": float(valid[label_col].mean()),
        "median": float(valid[label_col].median()),
        "win_rate": float((valid[label_col] > 0).mean()),
    }

    summary = {
        "label": args.label,
        "n_train": int(len(train)),
        "n_valid": int(len(valid)),
        "n_symbols_train": int(train["symbol"].nunique()),
        "n_symbols_valid": int(valid["symbol"].nunique()),
        "model": model_name,
        "params_override": bool(params_override),
        "select_top": int(args.select_top),
        "n_features": int(len(feature_names)),
        "tune_trials": int(args.tune) if args.tune else 0,
        "spearman": float(spearman) if spearman == spearman else None,
        "topn": args.topn,
        "topn_stats": topn_stats,
        "baseline_valid": baseline,
    }

    out_preds = Path(args.out_preds)
    out_preds.parent.mkdir(parents=True, exist_ok=True)
    valid_out = valid[["symbol", "date", label_col, "pred"]].copy()
    valid_out.to_csv(out_preds, index=False)

    if args.out_preds_all:
        all_out = df_all[["symbol", "date"]].copy()
        X_all = df_all[feature_names].to_numpy()
        X_all = np.where(np.isfinite(X_all), X_all, np.nan)
        X_all = np.where(np.isnan(X_all), med, X_all)
        X_all = (X_all - mu) / sigma
        all_out["pred"] = predict_fn(X_all)
        if label_col in df_all.columns:
            all_out[label_col] = df_all[label_col]
        out_all = Path(args.out_preds_all)
        out_all.parent.mkdir(parents=True, exist_ok=True)
        all_out.to_csv(out_all, index=False)

    imp_out = Path(args.out_importance)
    imp_out.parent.mkdir(parents=True, exist_ok=True)
    imp_df = pd.DataFrame({"feature": feature_names, "importance": importance})
    imp_df = imp_df.sort_values("importance", ascending=False)
    imp_df.to_csv(imp_out, index=False)

    out_summary = Path(args.out_summary)
    out_summary.parent.mkdir(parents=True, exist_ok=True)
    out_summary.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    if args.predict_latest:
        pred_df = df_all.copy()
        if args.predict_date:
            pred_date = pd.to_datetime(args.predict_date)
        else:
            pred_date = pred_df["date"].max()
        pred_df = pred_df[pred_df["date"] == pred_date].copy()
        if not pred_df.empty:
            X_pred = pred_df[feature_names].to_numpy()
            X_pred = np.where(np.isfinite(X_pred), X_pred, np.nan)
            X_pred = np.where(np.isnan(X_pred), med, X_pred)
            X_pred = (X_pred - mu) / sigma
            pred_df["pred"] = predict_fn(X_pred)
            out_latest = Path(args.out_latest)
            out_latest.parent.mkdir(parents=True, exist_ok=True)
            pred_df[["symbol", "date", "pred"]].to_csv(out_latest, index=False)
            print(f"Wrote: {out_latest}")

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
