#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Search penalty weights on top of ML predictions to reduce chasing high positions.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT))

from apps.analysis.run_ml_mfe10 import build_features, eval_topn


def main() -> None:
    parser = argparse.ArgumentParser(description="Penalty search for ML predictions")
    parser.add_argument("--scored", type=str, default="data/_derived/scored.parquet")
    parser.add_argument("--preds", type=str, default="data/_derived/ml_runs/tuned_params_seed42_preds.csv")
    parser.add_argument("--topn", type=int, default=10)
    parser.add_argument("--out", type=str, default="data/_derived/ml_runs/penalty_search.csv")
    args = parser.parse_args()

    scored_path = Path(args.scored)
    preds_path = Path(args.preds)
    if not scored_path.exists():
        raise FileNotFoundError(f"missing {scored_path}")
    if not preds_path.exists():
        raise FileNotFoundError(f"missing {preds_path}")

    use_cols = [
        "symbol",
        "date",
        "mfe_10d",
        "bb_pos",
        "sell_ratio",
        "close",
        "high",
        "low",
        "posnet_total",
        "negnet_total",
        "net_total",
    ]
    df = pd.read_parquet(scored_path, columns=use_cols)
    df["date"] = pd.to_datetime(df["date"])
    df = build_features(df)

    preds = pd.read_csv(preds_path)
    preds["date"] = pd.to_datetime(preds["date"])

    merged = preds.merge(df, on=["symbol", "date"], how="left", suffixes=("", "_feat"))
    if "mfe_10d" not in merged.columns and "mfe_10d_feat" in merged.columns:
        merged = merged.rename(columns={"mfe_10d_feat": "mfe_10d"})
    if "mfe_10d" not in merged.columns and "mfe_10d_x" in merged.columns:
        merged = merged.rename(columns={"mfe_10d_x": "mfe_10d"})
    merged = merged.dropna(subset=["pred", "mfe_10d"])
    if merged.empty:
        raise RuntimeError("no merged rows, check preds/scored alignment")

    results = []
    for a in [0.0, 0.3, 0.6, 1.0]:
        for b in [0.0, 0.2, 0.5, 1.0]:
            for c in [0.0, 0.2, 0.5]:
                for d in [0.0, 0.2, 0.4]:
                    score = (
                        merged["pred"]
                        - a * merged["bb_pos"].fillna(1.0)
                        - b * merged["sell_ratio"].fillna(1.5)
                        - c * merged["close_pos"].fillna(1.0)
                        + d * merged["posnet_chg"].fillna(0.0)
                    )
                    tmp = merged.copy()
                    tmp["score"] = score
                    stats = eval_topn(tmp, "mfe_10d", "score", args.topn)
                    results.append(
                        {
                            "a_bb": a,
                            "b_sell": b,
                            "c_closepos": c,
                            "d_poschg": d,
                            "topn_mean": stats.get("mean"),
                            "topn_median": stats.get("median"),
                            "topn_win_rate": stats.get("win_rate"),
                            "n_samples": stats.get("n_samples"),
                        }
                    )

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(results).sort_values("topn_median", ascending=False).to_csv(out, index=False)
    print(f"Wrote: {out}")


if __name__ == "__main__":
    main()
