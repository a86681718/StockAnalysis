#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Robustness checks for raw broker microstructure strategy outputs."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
IN_DIR = PROJECT_ROOT / "outputs" / "analysis" / "raw_broker_microstructure"
OUT_DIR = IN_DIR / "robustness"
FAMILIES = [
    "diffuse_buyer_accumulation",
    "role_reversal",
    "sell_pressure_absorption",
]


def _metrics(trades: pd.DataFrame) -> dict[str, Any]:
    ret = pd.to_numeric(trades.get("net_ret", pd.Series(dtype=float)), errors="coerce").dropna()
    if ret.empty:
        return {
            "trades": 0,
            "win_rate": np.nan,
            "avg_net_ret": np.nan,
            "median_net_ret": np.nan,
            "max_loss": np.nan,
            "sum_net_ret": 0.0,
        }
    return {
        "trades": int(len(ret)),
        "win_rate": float((ret > 0).mean()),
        "avg_net_ret": float(ret.mean()),
        "median_net_ret": float(ret.median()),
        "max_loss": float(ret.min()),
        "sum_net_ret": float(ret.sum()),
    }


def _bootstrap_mean_ci(ret: pd.Series, seed: int = 7, n_iter: int = 5000) -> tuple[float, float]:
    values = pd.to_numeric(ret, errors="coerce").dropna().to_numpy(float)
    if len(values) == 0:
        return np.nan, np.nan
    rng = np.random.default_rng(seed)
    samples = rng.choice(values, size=(n_iter, len(values)), replace=True).mean(axis=1)
    return float(np.quantile(samples, 0.05)), float(np.quantile(samples, 0.95))


def _trim_top_returns(trades: pd.DataFrame, pct: float = 0.05) -> pd.DataFrame:
    if trades.empty:
        return trades.copy()
    n_remove = max(1, int(math.ceil(len(trades) * pct)))
    drop_idx = trades.sort_values("net_ret", ascending=False).head(n_remove).index
    return trades.drop(index=drop_idx)


def _monthly_rows(family: str, trades: pd.DataFrame) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    work = trades.copy()
    work["signal_month"] = pd.to_datetime(work["signal_date"]).dt.to_period("M").astype(str)
    for month, sub in work.groupby("signal_month", sort=True):
        out.append({"event_family": family, "signal_month": month, **_metrics(sub)})
    return out


def _temporal_rows(family: str, trades: pd.DataFrame) -> list[dict[str, Any]]:
    work = trades.copy()
    signal_dates = pd.to_datetime(work["signal_date"])
    cutoff = signal_dates.median()
    rows = []
    for label, mask in [
        ("early_half", signal_dates <= cutoff),
        ("late_half", signal_dates > cutoff),
    ]:
        rows.append(
            {
                "event_family": family,
                "split": label,
                "cutoff": cutoff.strftime("%Y-%m-%d"),
                **_metrics(work.loc[mask]),
            }
        )
    return rows


def _symbol_concentration(family: str, trades: pd.DataFrame) -> dict[str, Any]:
    by_symbol = trades.groupby("symbol", as_index=False).agg(
        trades=("net_ret", "size"),
        sum_net_ret=("net_ret", "sum"),
        avg_net_ret=("net_ret", "mean"),
    )
    total_profit = float(by_symbol["sum_net_ret"].clip(lower=0).sum())
    top10_profit = float(by_symbol["sum_net_ret"].clip(lower=0).sort_values(ascending=False).head(10).sum())
    top_symbol = by_symbol.sort_values("sum_net_ret", ascending=False).head(1)
    if total_profit > 0:
        weights = by_symbol["sum_net_ret"].clip(lower=0) / total_profit
        hhi = float((weights * weights).sum())
        top10_share = top10_profit / total_profit
        top_symbol_share = float(top_symbol["sum_net_ret"].clip(lower=0).iloc[0] / total_profit)
    else:
        hhi = np.nan
        top10_share = np.nan
        top_symbol_share = np.nan
    return {
        "event_family": family,
        "symbols": int(by_symbol["symbol"].nunique()),
        "top_symbol": str(top_symbol["symbol"].iloc[0]) if not top_symbol.empty else "",
        "top_symbol_sum_net_ret": float(top_symbol["sum_net_ret"].iloc[0]) if not top_symbol.empty else np.nan,
        "top_symbol_profit_share": top_symbol_share,
        "top10_profit_share": top10_share,
        "profit_hhi": hhi,
    }


def _family_robustness(family: str, trades: pd.DataFrame, comparison: pd.DataFrame) -> dict[str, Any]:
    base = _metrics(trades)
    trim = _metrics(_trim_top_returns(trades))
    boot_low, boot_high = _bootstrap_mean_ci(trades["net_ret"])
    monthly = pd.DataFrame(_monthly_rows(family, trades))
    temporal = pd.DataFrame(_temporal_rows(family, trades))
    late = temporal.loc[temporal["split"] == "late_half"].iloc[0]
    comp_row = comparison.loc[comparison["event_family"] == family].iloc[0]
    profitable_month_ratio = float((monthly["sum_net_ret"] > 0).mean()) if not monthly.empty else np.nan
    worst_month_sum = float(monthly["sum_net_ret"].min()) if not monthly.empty else np.nan
    positive_checks = {
        "avg_improved_10pct": bool(comp_row["avg_net_ret_improvement"] >= 0.10),
        "late_half_gate": bool(late["trades"] >= 20 and late["win_rate"] > 0.60 and late["avg_net_ret"] > 0.10),
        "trimmed_avg_gt_10pct": bool(trim["avg_net_ret"] > 0.10),
        "profitable_month_ratio_ge_60pct": bool(profitable_month_ratio >= 0.60),
    }
    concentration = _symbol_concentration(family, trades)
    positive_checks["top10_profit_share_le_60pct"] = bool(concentration["top10_profit_share"] <= 0.60)
    passed = sum(positive_checks.values())
    if passed == len(positive_checks):
        verdict = "robust"
    elif passed >= len(positive_checks) - 1:
        verdict = "watch"
    else:
        verdict = "overfit_risk"
    return {
        "event_family": family,
        **base,
        "trimmed_top5pct_avg_net_ret": trim["avg_net_ret"],
        "bootstrap_mean_5pct": boot_low,
        "bootstrap_mean_95pct": boot_high,
        "profitable_month_ratio": profitable_month_ratio,
        "worst_month_sum_net_ret": worst_month_sum,
        "late_half_trades": int(late["trades"]),
        "late_half_win_rate": float(late["win_rate"]),
        "late_half_avg_net_ret": float(late["avg_net_ret"]),
        **concentration,
        **positive_checks,
        "checks_passed": int(passed),
        "checks_total": int(len(positive_checks)),
        "verdict": verdict,
    }


def _load_trades(family: str) -> pd.DataFrame:
    path = IN_DIR / f"{family}_best_trades.csv"
    trades = pd.read_csv(path, dtype={"symbol": str})
    for col in ["date", "signal_date", "entry_date", "exit_date"]:
        trades[col] = pd.to_datetime(trades[col], errors="coerce")
    trades["net_ret"] = pd.to_numeric(trades["net_ret"], errors="coerce")
    return trades.dropna(subset=["signal_date", "net_ret"]).copy()


def _write_report(
    family_summary: pd.DataFrame,
    monthly: pd.DataFrame,
    temporal: pd.DataFrame,
    comparison: pd.DataFrame,
) -> None:
    lines = [
        "# Raw Broker Microstructure Robustness",
        "",
        "Purpose: test whether the optimized strategy results look like broad evidence or likely in-sample overfitting.",
        "",
        "Important limitation: this is not a true walk-forward parameter-selection test. The optimized parameters were selected on the full sample, so temporal split results are an out-of-sample proxy only.",
        "",
        "Robustness checks:",
        "",
        "- late-half gate: late-half trades >= 20, win rate > 60%, average net return > 10%",
        "- trimmed top 5%: average net return remains > 10% after removing the best 5% trades",
        "- monthly stability: at least 60% of signal months have positive summed net return",
        "- concentration: top 10 profitable symbols contribute <= 60% of positive profit",
        "- bootstrap: 5% and 95% confidence bounds for mean net return from trade-level resampling",
        "",
        "## Verdict Summary",
        "",
        "| family | verdict | checks | trades | avg | win | late_avg | trimmed_avg | profitable_month_ratio | top10_profit_share |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _, row in family_summary.iterrows():
        lines.append(
            f"| `{row['event_family']}` | `{row['verdict']}` | "
            f"{int(row['checks_passed'])}/{int(row['checks_total'])} | "
            f"{int(row['trades'])} | {row['avg_net_ret']:.4f} | {row['win_rate']:.4f} | "
            f"{row['late_half_avg_net_ret']:.4f} | {row['trimmed_top5pct_avg_net_ret']:.4f} | "
            f"{row['profitable_month_ratio']:.4f} | {row['top10_profit_share']:.4f} |"
        )
    lines.extend(["", "## Improvement Context", ""])
    for _, row in comparison.iterrows():
        lines.append(
            f"- `{row['event_family']}`: baseline avg `{row['baseline_avg_net_ret']:.4f}` -> "
            f"optimized avg `{row['optimized_avg_net_ret']:.4f}` "
            f"(`{row['avg_net_ret_improvement']:.2%}` improvement)."
        )
    lines.extend(["", "## Temporal Split", ""])
    for _, row in temporal.iterrows():
        lines.append(
            f"- `{row['event_family']}` `{row['split']}` cutoff `{row['cutoff']}`: "
            f"trades `{int(row['trades'])}`, win `{row['win_rate']:.4f}`, avg `{row['avg_net_ret']:.4f}`"
        )
    lines.extend(["", "## Monthly Weak Spots", ""])
    for family, sub in monthly.groupby("event_family", sort=False):
        worst = sub.sort_values("sum_net_ret").head(3)
        lines.append(f"### {family}")
        for _, row in worst.iterrows():
            lines.append(
                f"- `{row['signal_month']}`: trades `{int(row['trades'])}`, "
                f"win `{row['win_rate']:.4f}`, avg `{row['avg_net_ret']:.4f}`, sum `{row['sum_net_ret']:.4f}`"
            )
        lines.append("")
    lines.extend(
        [
            "## Interpretation",
            "",
            "- `robust`: all checks passed under this diagnostic framework.",
            "- `watch`: only one check failed; strategy may still be useful but needs stricter forward validation.",
            "- `overfit_risk`: two or more checks failed; current optimized result should not be trusted without redesign or true walk-forward confirmation.",
            "",
            "Recommended next step: implement true walk-forward parameter selection, where parameters are selected only on a training window and scored on a later holdout window.",
        ]
    )
    (OUT_DIR / "raw_microstructure_robustness_report.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    comparison = pd.read_csv(IN_DIR / "raw_microstructure_improvement_comparison.csv")
    family_rows: list[dict[str, Any]] = []
    monthly_rows: list[dict[str, Any]] = []
    temporal_rows: list[dict[str, Any]] = []
    symbol_rows: list[dict[str, Any]] = []
    for family in FAMILIES:
        trades = _load_trades(family)
        family_rows.append(_family_robustness(family, trades, comparison))
        monthly_rows.extend(_monthly_rows(family, trades))
        temporal_rows.extend(_temporal_rows(family, trades))
        symbol_rows.append(_symbol_concentration(family, trades))

    family_summary = pd.DataFrame(family_rows)
    monthly = pd.DataFrame(monthly_rows)
    temporal = pd.DataFrame(temporal_rows)
    symbol_concentration = pd.DataFrame(symbol_rows)

    family_summary.to_csv(OUT_DIR / "robustness_by_family.csv", index=False)
    monthly.to_csv(OUT_DIR / "robustness_monthly.csv", index=False)
    temporal.to_csv(OUT_DIR / "robustness_temporal_splits.csv", index=False)
    symbol_concentration.to_csv(OUT_DIR / "robustness_symbol_concentration.csv", index=False)
    summary = {
        "families": family_summary["event_family"].tolist(),
        "verdicts": dict(zip(family_summary["event_family"], family_summary["verdict"])),
        "robust_count": int((family_summary["verdict"] == "robust").sum()),
        "watch_count": int((family_summary["verdict"] == "watch").sum()),
        "overfit_risk_count": int((family_summary["verdict"] == "overfit_risk").sum()),
    }
    (OUT_DIR / "raw_microstructure_robustness_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    _write_report(family_summary, monthly, temporal, comparison)
    print(OUT_DIR / "raw_microstructure_robustness_report.md")


if __name__ == "__main__":
    main()
