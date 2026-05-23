#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Two-stage ML for mfe_10d:
  Stage1: rule-based filter (low position + chip improvement + low sell pressure)
  Stage2: LightGBM ranking within candidates
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT))

from apps.analysis.run_ml_mfe10 import (
    FEATURE_COLS,
    build_features,
    spearman_corr,
    eval_topn,
    train_model,
)


def apply_stage1(df: pd.DataFrame, cfg: dict) -> pd.Series:
    m = pd.Series(True, index=df.index)
    if "bb_max" in cfg:
        m &= df["bb_pos"].fillna(1.0) <= cfg["bb_max"]
    if "close_pos_max" in cfg:
        m &= df["close_pos"].fillna(1.0) <= cfg["close_pos_max"]
    if "sell_ratio_max" in cfg:
        m &= df["sell_ratio"].fillna(np.inf) <= cfg["sell_ratio_max"]
    if "dyn_k_max" in cfg:
        m &= df["dyn_k"].fillna(np.inf) <= cfg["dyn_k_max"]
    if cfg.get("posnet_chg_pos"):
        m &= df["posnet_chg"].fillna(0) >= 0
    if cfg.get("negnet_chg_neg"):
        m &= df["negnet_chg"].fillna(0) <= 0
    return m


def main() -> None:
    parser = argparse.ArgumentParser(description="Two-stage ML for mfe_10d")
    parser.add_argument("--data", type=str, default="data/_derived/scored.parquet")
    parser.add_argument("--label", type=str, default="mfe_10d")
    parser.add_argument("--valid-ratio", type=float, default=0.2)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--topn", type=int, default=10)
    parser.add_argument("--model", type=str, default="lgbm", choices=["lgbm", "ridge"])
    parser.add_argument("--params", type=str, default="", help="optional json file for LightGBM params")
    parser.add_argument("--out-summary", type=str, default="data/_derived/ml_two_stage_summary.json")
    parser.add_argument("--out-trials", type=str, default="data/_derived/ml_two_stage_trials.csv")
    parser.add_argument("--out-best", type=str, default="data/_derived/ml_two_stage_best.json")
    parser.add_argument("--out-latest", type=str, default="data/_derived/ml_two_stage_latest.csv")
    args = parser.parse_args()

    data_path = Path(args.data)
    if not data_path.exists():
        raise FileNotFoundError(f"missing {data_path}")

    use_cols = [
        "symbol",
        "date",
        args.label,
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
    df["date"] = pd.to_datetime(df["date"])
    df = build_features(df)

    df_all = df.copy()
    df = df[df[args.label].notna()].copy()
    if df.empty:
        raise RuntimeError("no training data after filtering")

    symbols = df["symbol"].dropna().unique().tolist()
    rng = np.random.default_rng(args.seed)
    rng.shuffle(symbols)
    n_valid = max(1, int(len(symbols) * args.valid_ratio))
    valid_symbols = set(symbols[:n_valid])

    train_all = df[~df["symbol"].isin(valid_symbols)].copy()
    valid_all = df[df["symbol"].isin(valid_symbols)].copy()
    if train_all.empty or valid_all.empty:
        raise RuntimeError("train/valid split empty, adjust valid_ratio")

    feature_names = FEATURE_COLS.copy()

    params_override = None
    if args.params:
        params_path = Path(args.params)
        if params_path.exists():
            params_override = json.loads(params_path.read_text(encoding="utf-8"))
            if isinstance(params_override, dict) and "best_params" in params_override:
                params_override = params_override["best_params"]

    configs = []
    bb_vals = [0.45, 0.55, 0.65, 0.75]
    sell_vals = [0.8, 1.0, 1.2, 1.5]
    dyn_vals = [8, 10, 12, 15]
    close_vals = [0.6, 0.8, 1.0]
    pos_flags = [False, True]
    neg_flags = [False, True]
    for _ in range(40):
        cfg = dict(
            bb_max=float(rng.choice(bb_vals)),
            sell_ratio_max=float(rng.choice(sell_vals)),
            dyn_k_max=int(rng.choice(dyn_vals)),
            close_pos_max=float(rng.choice(close_vals)),
            posnet_chg_pos=bool(rng.choice(pos_flags)),
            negnet_chg_neg=bool(rng.choice(neg_flags)),
        )
        configs.append(cfg)

    results = []
    best_score = -1e9
    best_cfg = None
    best_predict_fn = None
    best_norm = None
    best_feature_names = None

    for cfg in configs:
        m_train = apply_stage1(train_all, cfg)
        m_valid = apply_stage1(valid_all, cfg)
        train = train_all[m_train].copy()
        valid = valid_all[m_valid].copy()
        if len(train) < 300 or len(valid) < 150:
            continue

        X_train = train[feature_names].to_numpy()
        y_train = train[args.label].to_numpy()
        X_valid = valid[feature_names].to_numpy()

        X_train = np.where(np.isfinite(X_train), X_train, np.nan)
        X_valid = np.where(np.isfinite(X_valid), X_valid, np.nan)
        med = np.nanmedian(X_train, axis=0)
        X_train = np.where(np.isnan(X_train), med, X_train)
        X_valid = np.where(np.isnan(X_valid), med, X_valid)
        mu = X_train.mean(axis=0)
        sigma = X_train.std(axis=0)
        sigma = np.where(sigma == 0, 1.0, sigma)
        X_train = (X_train - mu) / sigma
        X_valid = (X_valid - mu) / sigma

        predict_fn, importance, model_name = train_model(
            args.model, X_train, y_train, X_valid, args.seed, params=params_override
        )
        valid["pred"] = predict_fn(X_valid)
        spearman = spearman_corr(valid[args.label], valid["pred"]) or 0.0
        topn_stats = eval_topn(valid, args.label, "pred", args.topn)

        row = dict(cfg)
        row.update(
            {
                "train_rows": int(len(train)),
                "valid_rows": int(len(valid)),
                "spearman": float(spearman),
                "topn_mean": topn_stats.get("mean"),
                "topn_median": topn_stats.get("median"),
                "topn_win_rate": topn_stats.get("win_rate"),
                "topn_samples": topn_stats.get("n_samples"),
            }
        )
        results.append(row)

        score = float(topn_stats.get("median") or 0.0)
        if score > best_score:
            best_score = score
            best_cfg = cfg
            best_predict_fn = predict_fn
            best_norm = (med, mu, sigma)
            best_feature_names = feature_names

    out_trials = Path(args.out_trials)
    out_trials.parent.mkdir(parents=True, exist_ok=True)
    if results:
        pd.DataFrame(results).sort_values("topn_median", ascending=False).to_csv(out_trials, index=False)
    else:
        pd.DataFrame().to_csv(out_trials, index=False)

    best_payload = {"best_cfg": best_cfg, "best_topn_median": best_score}
    Path(args.out_best).write_text(json.dumps(best_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    summary = {
        "label": args.label,
        "model": args.model,
        "n_train": int(len(train_all)),
        "n_valid": int(len(valid_all)),
        "best_cfg": best_cfg,
        "best_topn_median": best_score,
        "trials": int(len(results)),
    }
    Path(args.out_summary).write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    # Latest prediction using best stage1 + best model
    if best_cfg and best_predict_fn and best_norm:
        med, mu, sigma = best_norm
        pred_df = df_all.copy()
        latest = pred_df["date"].max()
        pred_df = pred_df[pred_df["date"] == latest].copy()
        pred_df = pred_df[apply_stage1(pred_df, best_cfg)].copy()
        if not pred_df.empty:
            X_pred = pred_df[best_feature_names].to_numpy()
            X_pred = np.where(np.isfinite(X_pred), X_pred, np.nan)
            X_pred = np.where(np.isnan(X_pred), med, X_pred)
            X_pred = (X_pred - mu) / sigma
            pred_df["pred"] = best_predict_fn(X_pred)
            out_latest = Path(args.out_latest)
            out_latest.parent.mkdir(parents=True, exist_ok=True)
            pred_df[["symbol", "date", "pred"]].to_csv(out_latest, index=False)

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
