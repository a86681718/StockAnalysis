#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import sys
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.stockanalysis.config import ensure_dir, resolve_data, resolve_output


OUT_DIR = resolve_output("analysis", "strategy_dashboard")
OUTPUT_HTML = OUT_DIR / "strategy_dashboard.html"

S1_DIR = resolve_output("analysis", "key_broker_branch_hybrid")
S2_DIR = resolve_output("analysis", "rare_event")
ML_DIR = resolve_data("_derived", "ml_runs")


def _safe_float(value: Any, digits: int = 4) -> float | None:
    if value is None or pd.isna(value):
        return None
    return round(float(value), digits)


def _safe_str(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value)


def _safe_date(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return pd.to_datetime(value).strftime("%Y-%m-%d")


def _pct(value: Any, digits: int = 1) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value) * 100:.{digits}f}%"


def _num(value: Any, digits: int = 2) -> str:
    if value is None or pd.isna(value):
        return "-"
    if isinstance(value, (int,)) or float(value).is_integer():
        return f"{int(value):,}"
    return f"{float(value):,.{digits}f}"


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _date_range(df: pd.DataFrame, col: str = "date") -> dict[str, Any]:
    if df.empty or col not in df.columns:
        return {"min": None, "max": None, "count": 0}
    dates = pd.to_datetime(df[col], errors="coerce")
    valid = dates.dropna()
    if valid.empty:
        return {"min": None, "max": None, "count": 0}
    return {
        "min": valid.min().strftime("%Y-%m-%d"),
        "max": valid.max().strftime("%Y-%m-%d"),
        "count": int(valid.nunique()),
    }


def _summary_metrics_from_trades(trades: pd.DataFrame, ret_col: str = "net_ret") -> dict[str, Any]:
    if trades.empty or ret_col not in trades.columns:
        return {"trades": 0, "winRate": None, "avgNetRet": None, "medianNetRet": None, "maxLoss": None}
    returns = pd.to_numeric(trades[ret_col], errors="coerce").dropna()
    if returns.empty:
        return {"trades": int(len(trades)), "winRate": None, "avgNetRet": None, "medianNetRet": None, "maxLoss": None}
    return {
        "trades": int(len(returns)),
        "winRate": float((returns > 0).mean()),
        "avgNetRet": float(returns.mean()),
        "medianNetRet": float(returns.median()),
        "maxLoss": float(returns.min()),
    }


def _latest_rows(df: pd.DataFrame, date_col: str = "date") -> pd.DataFrame:
    if df.empty or date_col not in df.columns:
        return df.head(0)
    dates = pd.to_datetime(df[date_col], errors="coerce")
    if dates.dropna().empty:
        return df.head(0)
    max_date = dates.max()
    return df.loc[dates == max_date].copy()


def _strategy_s1() -> tuple[dict[str, Any], list[dict[str, Any]]]:
    summary = _read_json(S1_DIR / "key_broker_branch_hybrid_summary.json")
    signals = pd.read_parquet(S1_DIR / "key_broker_branch_hybrid_signals.parquet")
    trades = pd.read_csv(S1_DIR / "key_broker_branch_hybrid_trades.csv")
    date_info = _date_range(signals)
    latest = _latest_rows(signals)
    best = summary.get("best", {})
    metrics = {
        "trades": best.get("trades", len(trades)),
        "winRate": best.get("win_rate"),
        "avgNetRet": best.get("avg_net_ret"),
        "medianNetRet": best.get("median_net_ret"),
        "maxLoss": best.get("max_loss"),
        "profitFactor": best.get("profit_factor"),
    }

    candidates: list[dict[str, Any]] = []
    for _, row in latest.sort_values("score", ascending=False).head(5).iterrows():
        candidates.append(
            {
                "strategy": "S1",
                "symbol": _safe_str(row.get("symbol")),
                "date": _safe_date(row.get("date")),
                "status": "stale",
                "statusLabel": "歷史最新訊號",
                "rankNote": "S1 hybrid artifact 未延伸到目前資料日，僅供回看",
                "score": _safe_float(row.get("score")),
                "price": None,
                "fields": {
                    "broker": _safe_str(row.get("broker_name")),
                    "score": _safe_float(row.get("score")),
                    "breakout_gap_20": _safe_float(row.get("breakout_gap_20")),
                    "volume_ratio_5_20": _safe_float(row.get("volume_ratio_5_20")),
                    "ret_5d": _safe_float(row.get("ret_5d")),
                },
            }
        )

    strategy = {
        "id": "S1",
        "name": "key_broker_branch_hybrid",
        "theme": "關鍵券商分點異常買超 + 價格突破脈絡",
        "status": "artifact-warning" if best.get("target_pass") is False else "candidate",
        "statusLabel": "目前 artifact 未過目標門檻" if best.get("target_pass") is False else "候選策略",
        "signalDate": date_info["max"],
        "coverage": date_info,
        "metrics": metrics,
        "rules": [
            "從 strict branch standalone 訊號開始",
            "branch anomaly score 取前 10%",
            "breakout_gap_20 >= 0.02",
            "price_pos_20 >= 0.5",
            "0.5 <= volume_ratio_5_20 <= 3.0",
            "ret_5d <= 0.30",
        ],
        "execution": ["next open 進場", "hold_days = 30", "stop_loss = none"],
        "notes": [
            "目前工作樹的 summary 顯示 target_pass=false。",
            "base branch raw signals 不能直接當 S1 hybrid 可買名單。",
        ],
        "sources": [
            "outputs/analysis/key_broker_branch_hybrid/key_broker_branch_hybrid_report.md",
            "outputs/analysis/key_broker_branch_hybrid/key_broker_branch_hybrid_summary.json",
            "apps/analysis/run_key_broker_branch_breakout_hybrid.py",
        ],
    }
    return strategy, candidates


def _strategy_s2() -> tuple[dict[str, Any], list[dict[str, Any]]]:
    trades = pd.read_parquet(S2_DIR / "warrant_leads_stock_refine_trades.parquet")
    signals = pd.read_parquet(S2_DIR / "warrant_leads_stock_refine_signals.parquet")
    date_info = _date_range(signals)
    latest = _latest_rows(signals)
    metrics = _summary_metrics_from_trades(trades)
    metrics["profitFactor"] = None

    candidates: list[dict[str, Any]] = []
    for _, row in latest.sort_values(["warrant_posnet_pct_cs", "warrant_dyn_k_pct_cs"], ascending=False).iterrows():
        candidates.append(
            {
                "strategy": "S2",
                "symbol": _safe_str(row.get("symbol")),
                "date": _safe_date(row.get("date")),
                "status": "formal",
                "statusLabel": "正式候選",
                "rankNote": "通過 S2 固定條件，需看 next-open 執行價格",
                "score": _safe_float(row.get("warrant_posnet_pct_cs")),
                "price": _safe_float(row.get("close")),
                "fields": {
                    "close": _safe_float(row.get("close")),
                    "warrant_posnet_pct_cs": _safe_float(row.get("warrant_posnet_pct_cs")),
                    "warrant_dyn_k_pct_cs": _safe_float(row.get("warrant_dyn_k_pct_cs")),
                    "warrant_posnet_strong_days_20": _safe_float(row.get("warrant_posnet_strong_days_20"), 0),
                    "stock_posnet_pct_cs": _safe_float(row.get("stock_posnet_pct_cs")),
                    "prior_abs_ret_20d": _safe_float(row.get("prior_abs_ret_20d")),
                    "volume_ratio_5_20": _safe_float(row.get("volume_ratio_5_20")),
                    "breakout_gap_20": _safe_float(row.get("breakout_gap_20")),
                },
            }
        )

    strategy = {
        "id": "S2",
        "name": "warrant_leads_stock_refine",
        "theme": "權證買盤領先現股",
        "status": "candidate",
        "statusLabel": "目前有最新候選" if candidates else "目前無最新候選",
        "signalDate": date_info["max"],
        "coverage": date_info,
        "metrics": metrics,
        "rules": [
            "warrant_posnet_pct_cs >= 0.98",
            "warrant_dyn_k_pct_cs >= 0.90",
            "warrant_posnet_strong_days_20 >= 3",
            "0.80 <= stock_posnet_pct_cs <= 0.95",
            "prior_abs_ret_20d <= 0.08",
        ],
        "execution": ["next open 進場", "hold_days = 40", "cooldown = 15 trading days", "stop_loss = -10%"],
        "notes": [
            "找權證端極強，但現股端尚未過度擁擠的標的。",
            "最新訊號仍屬 forward/pending，完整 40 日結果要等後續資料。",
        ],
        "sources": [
            "outputs/analysis/rare_event/warrant_leads_stock_refine_report.md",
            "outputs/analysis/rare_event/warrant_leads_stock_refine_signals.parquet",
            "apps/analysis/run_warrant_leads_stock_refine.py",
        ],
    }
    return strategy, candidates


def _strategy_s3() -> tuple[dict[str, Any], list[dict[str, Any]]]:
    summary = _read_json(ML_DIR / "repair_branch_topratio0610_daybuy20ge4_summary.json")
    latest_summary = _read_json(ML_DIR / "breakout10_latest_wf_repair_branch_topratio0610_daybuy20ge4_summary.json")
    trades = pd.read_csv(ML_DIR / "repair_branch_topratio0610_daybuy20ge4_trades.csv")
    formal_candidates = pd.read_csv(ML_DIR / "breakout10_latest_wf_repair_branch_topratio0610_daybuy20ge4_candidates.csv")
    rank_context = pd.read_csv(ML_DIR / "breakout10_latest_wf_repair_branch_topratio0610.csv")
    summary_metrics = summary.get("summary", {})
    metrics = {
        "trades": summary_metrics.get("trades", len(trades)),
        "winRate": summary_metrics.get("win_net_ret"),
        "avgNetRet": summary_metrics.get("mean_net_ret"),
        "medianNetRet": summary_metrics.get("median_net_ret"),
        "maxLoss": float(trades["net_ret"].min()) if not trades.empty and "net_ret" in trades.columns else None,
        "profitFactor": None,
    }

    candidates: list[dict[str, Any]] = []
    for _, row in formal_candidates.iterrows():
        candidates.append(
            {
                "strategy": "S3",
                "symbol": _safe_str(row.get("symbol")),
                "date": _safe_date(row.get("date")),
                "status": "formal",
                "statusLabel": "正式候選",
                "rankNote": "通過 day-buy20 gate，仍需 next-open gap filter",
                "score": _safe_float(row.get("pred")),
                "price": None,
                "fields": {"pred": _safe_float(row.get("pred"))},
            }
        )

    if not candidates:
        for _, row in rank_context.sort_values("pred", ascending=False).head(8).iterrows():
            candidates.append(
                {
                    "strategy": "S3",
                    "symbol": _safe_str(row.get("symbol")),
                    "date": _safe_date(row.get("date")),
                    "status": "rank-only",
                    "statusLabel": "排名參考",
                    "rankNote": "day-buy20 gate 未過，不能當正式可買",
                    "score": _safe_float(row.get("pred")),
                    "price": None,
                    "fields": {
                        "pred": _safe_float(row.get("pred")),
                        "selected_count_pre_gap": latest_summary.get("selected_count_pre_gap"),
                        "mean_buy20_selected": _safe_float(latest_summary.get("mean_buy20_selected")),
                        "passes_day_buy20_gate": latest_summary.get("passes_day_buy20_gate"),
                    },
                }
            )

    strategy = {
        "id": "S3",
        "name": "repair_branch_topratio0610_daybuy20ge4",
        "theme": "breakout10 / 熱門急漲捕捉 + 籌碼修補 gate",
        "status": "blocked" if latest_summary.get("passes_day_buy20_gate") is False else "candidate",
        "statusLabel": "latest gate 未過" if latest_summary.get("passes_day_buy20_gate") is False else "目前有最新候選",
        "signalDate": latest_summary.get("latest_signal_date"),
        "coverage": {
            "min": summary.get("window", {}).get("start"),
            "max": summary.get("window", {}).get("end"),
            "count": summary_metrics.get("coverage_days"),
        },
        "metrics": metrics,
        "rules": [
            "breakout10 walk-forward / latest score",
            "stock_net_buy_days_20 >= 1",
            "warrant_hhi_posnet_20 <= 0.80",
            "top_posnet_ratio <= 0.610",
            "mean_buy20 >= 4 day-quality gate",
            "top_pct = 1.5%, topk = 3",
        ],
        "execution": ["next open 進場", "next-open gap <= 0.5%", "hold_days = 16", "max_positions = 6"],
        "notes": [
            f"latest selected_count_pre_gap={latest_summary.get('selected_count_pre_gap')}, mean_buy20_selected={_num(latest_summary.get('mean_buy20_selected'))}。",
            "當 passes_day_buy20_gate=false 時，高分股只作排名脈絡，不列正式候選。",
        ],
        "sources": [
            "data/_derived/ml_runs/repair_branch_topratio0610_daybuy20ge4_summary.json",
            "data/_derived/ml_runs/breakout10_latest_wf_repair_branch_topratio0610_daybuy20ge4_summary.json",
            "apps/analysis/run_ml_breakout10.py",
        ],
    }
    return strategy, candidates


def _build_payload() -> dict[str, Any]:
    strategies: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []
    for factory in (_strategy_s1, _strategy_s2, _strategy_s3):
        strategy, rows = factory()
        strategies.append(strategy)
        candidates.extend(rows)

    dates = [s.get("signalDate") for s in strategies if s.get("signalDate")]
    latest_data_date = max(dates) if dates else None
    formal_count = sum(1 for c in candidates if c["status"] == "formal")
    return {
        "generatedAt": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
        "latestDataDate": latest_data_date,
        "nextOpenLabel": "2026-06-08",
        "strategies": strategies,
        "candidates": candidates,
        "summary": {
            "strategyCount": len(strategies),
            "formalCandidateCount": formal_count,
            "rankContextCount": sum(1 for c in candidates if c["status"] != "formal"),
        },
    }


def _render(payload: dict[str, Any]) -> str:
    data_json = json.dumps(payload, ensure_ascii=False)
    return f"""<!DOCTYPE html>
<html lang="zh-Hant">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>策略檢視儀表板</title>
  <style>
    :root {{
      --bg: #f6f8f7;
      --surface: #ffffff;
      --surface-2: #f1f5f4;
      --ink: #17201d;
      --muted: #66746f;
      --line: #d7dfdc;
      --line-strong: #b8c4c0;
      --teal: #0f766e;
      --teal-soft: #e6f4f1;
      --amber: #b45309;
      --amber-soft: #fff4df;
      --red: #b42318;
      --red-soft: #fff1f0;
      --blue: #1d4ed8;
      --blue-soft: #eef4ff;
      --shadow: 0 18px 46px rgba(23, 32, 29, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font-family: "Avenir Next", "PingFang TC", "Noto Sans TC", Arial, sans-serif;
      letter-spacing: 0;
    }}
    button, input, select {{ font: inherit; }}
    .app {{
      min-height: 100vh;
      display: grid;
      grid-template-columns: 288px minmax(0, 1fr);
    }}
    .sidebar {{
      position: sticky;
      top: 0;
      height: 100vh;
      padding: 22px;
      background: #0f1b18;
      color: #eff8f5;
      overflow: auto;
    }}
    .brand {{
      font-size: 1.05rem;
      font-weight: 700;
      margin-bottom: 18px;
    }}
    .side-meta {{
      display: grid;
      gap: 8px;
      padding: 14px;
      border: 1px solid rgba(255,255,255,0.14);
      background: rgba(255,255,255,0.06);
      border-radius: 8px;
      margin-bottom: 18px;
    }}
    .side-meta span {{ color: #9db4ad; font-size: 0.78rem; }}
    .side-meta strong {{ font-size: 0.94rem; }}
    .nav {{ display: grid; gap: 8px; }}
    .nav button {{
      width: 100%;
      color: inherit;
      text-align: left;
      background: transparent;
      border: 1px solid rgba(255,255,255,0.12);
      border-radius: 8px;
      padding: 12px;
      cursor: pointer;
    }}
    .nav button.active {{
      background: rgba(255,255,255,0.12);
      border-color: rgba(255,255,255,0.34);
    }}
    .nav-id {{ font-size: 0.8rem; color: #9db4ad; margin-bottom: 4px; }}
    .nav-name {{ font-size: 0.9rem; font-weight: 700; word-break: break-word; }}
    .nav-status {{ display: inline-block; margin-top: 8px; font-size: 0.75rem; color: #dbe8e4; }}
    .main {{
      padding: 22px;
      max-width: 1500px;
      width: 100%;
    }}
    .topbar {{
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: flex-start;
      margin-bottom: 18px;
    }}
    h1 {{
      margin: 0 0 8px;
      font-size: clamp(1.8rem, 3vw, 2.7rem);
      line-height: 1.04;
      font-weight: 760;
    }}
    .lede {{
      margin: 0;
      max-width: 860px;
      color: var(--muted);
      line-height: 1.65;
    }}
    .quick-stats {{
      display: grid;
      grid-template-columns: repeat(3, minmax(112px, 1fr));
      gap: 10px;
      min-width: 390px;
    }}
    .stat, .panel {{
      background: var(--surface);
      border: 1px solid var(--line);
      box-shadow: var(--shadow);
    }}
    .stat {{
      border-radius: 8px;
      padding: 12px;
    }}
    .label {{
      display: block;
      color: var(--muted);
      font-size: 0.76rem;
      line-height: 1.25;
      margin-bottom: 5px;
    }}
    .value {{
      display: block;
      font-size: 1.2rem;
      font-weight: 760;
      line-height: 1.1;
    }}
    .panel {{
      border-radius: 8px;
      padding: 16px;
    }}
    .grid {{
      display: grid;
      grid-template-columns: 1.02fr 0.98fr;
      gap: 14px;
      margin-bottom: 14px;
    }}
    .section-title {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: 12px;
    }}
    h2 {{ margin: 0; font-size: 1rem; }}
    h3 {{ margin: 0 0 8px; font-size: 0.92rem; }}
    .status-pill {{
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: 5px 9px;
      font-size: 0.75rem;
      border: 1px solid var(--line);
      color: var(--muted);
      white-space: nowrap;
    }}
    .status-candidate, .status-formal {{ color: var(--teal); background: var(--teal-soft); border-color: #b9ded8; }}
    .status-blocked, .status-rank-only {{ color: var(--amber); background: var(--amber-soft); border-color: #f0d39c; }}
    .status-artifact-warning, .status-stale {{ color: var(--red); background: var(--red-soft); border-color: #f0b8b3; }}
    .rule-list, .note-list, .source-list {{
      list-style: none;
      padding: 0;
      margin: 0;
      display: grid;
      gap: 7px;
    }}
    .rule-list li, .note-list li, .source-list li {{
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 9px 10px;
      background: #fbfcfc;
      color: #2d3935;
      font-size: 0.88rem;
      line-height: 1.45;
    }}
    .source-list li {{ font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 0.76rem; word-break: break-all; }}
    .metrics {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 10px;
    }}
    .metric {{
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px;
      background: #fbfcfc;
      min-height: 74px;
    }}
    .metric strong {{
      display: block;
      font-size: 1.25rem;
      line-height: 1.1;
      margin-top: 4px;
    }}
    .tabs {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-bottom: 12px;
    }}
    .tab {{
      border: 1px solid var(--line);
      background: var(--surface);
      border-radius: 8px;
      padding: 9px 12px;
      cursor: pointer;
      color: var(--muted);
    }}
    .tab.active {{
      color: var(--ink);
      border-color: var(--line-strong);
      background: var(--surface-2);
      font-weight: 700;
    }}
    .table-wrap {{ overflow: auto; border: 1px solid var(--line); border-radius: 8px; }}
    table {{ width: 100%; border-collapse: collapse; min-width: 760px; background: var(--surface); }}
    th, td {{
      border-bottom: 1px solid var(--line);
      padding: 10px;
      text-align: left;
      font-size: 0.86rem;
      vertical-align: top;
    }}
    th {{
      color: var(--muted);
      font-size: 0.75rem;
      font-weight: 700;
      background: #f8faf9;
      position: sticky;
      top: 0;
      z-index: 1;
    }}
    tr:last-child td {{ border-bottom: 0; }}
    .row-btn {{
      border: 0;
      background: transparent;
      padding: 0;
      color: var(--blue);
      cursor: pointer;
      font-weight: 700;
    }}
    .detail-grid {{
      display: grid;
      grid-template-columns: 340px 1fr;
      gap: 14px;
    }}
    .candidate-card {{
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 14px;
      background: #fbfcfc;
    }}
    .candidate-symbol {{
      font-size: 2.4rem;
      font-weight: 780;
      line-height: 1;
      margin: 8px 0 10px;
    }}
    .field-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
    }}
    .field {{
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 10px;
      background: var(--surface);
    }}
    .bar {{
      margin-top: 8px;
      height: 7px;
      border-radius: 99px;
      background: var(--surface-2);
      overflow: hidden;
    }}
    .bar span {{
      display: block;
      height: 100%;
      background: var(--teal);
      width: var(--w, 0%);
    }}
    .empty {{
      color: var(--muted);
      border: 1px dashed var(--line-strong);
      border-radius: 8px;
      padding: 18px;
      background: #fbfcfc;
    }}
    @media (max-width: 1080px) {{
      .app {{ grid-template-columns: 1fr; }}
      .sidebar {{ position: relative; height: auto; }}
      .topbar, .grid, .detail-grid {{ display: block; }}
      .quick-stats {{ min-width: 0; margin-top: 14px; }}
      .panel {{ margin-bottom: 14px; }}
    }}
    @media (max-width: 680px) {{
      .main, .sidebar {{ padding: 14px; }}
      .quick-stats, .metrics, .field-grid {{ grid-template-columns: 1fr; }}
      h1 {{ font-size: 1.8rem; }}
    }}
  </style>
</head>
<body>
  <div class="app">
    <aside class="sidebar">
      <div class="brand">策略檢視儀表板</div>
      <div class="side-meta">
        <span>資料最新日</span>
        <strong id="latestDataDate">-</strong>
        <span>對應觀察開盤</span>
        <strong id="nextOpenLabel">-</strong>
      </div>
      <nav class="nav" id="strategyNav"></nav>
    </aside>
    <main class="main">
      <header class="topbar">
        <div>
          <h1>S1 / S2 / S3 策略檢視</h1>
          <p class="lede">集中查看策略條件、目前 artifact 測試結果、最新候選標的與 gate 狀態。候選分正式候選與排名參考，避免把未通過 gate 的高分股誤當可買。</p>
        </div>
        <div class="quick-stats">
          <div class="stat"><span class="label">策略數</span><span class="value" id="strategyCount">-</span></div>
          <div class="stat"><span class="label">正式候選</span><span class="value" id="formalCount">-</span></div>
          <div class="stat"><span class="label">排名/歷史參考</span><span class="value" id="contextCount">-</span></div>
        </div>
      </header>

      <section class="grid">
        <div class="panel">
          <div class="section-title">
            <h2 id="strategyTitle">策略</h2>
            <span class="status-pill" id="strategyStatus">-</span>
          </div>
          <p class="lede" id="strategyTheme"></p>
          <div style="height:14px"></div>
          <div class="metrics" id="metricGrid"></div>
        </div>
        <div class="panel">
          <div class="section-title"><h2>執行規則</h2></div>
          <ul class="rule-list" id="executionList"></ul>
        </div>
      </section>

      <section class="grid">
        <div class="panel">
          <div class="section-title"><h2>篩選條件</h2></div>
          <ul class="rule-list" id="ruleList"></ul>
        </div>
        <div class="panel">
          <div class="section-title"><h2>註記與來源</h2></div>
          <ul class="note-list" id="noteList"></ul>
          <div style="height:10px"></div>
          <ul class="source-list" id="sourceList"></ul>
        </div>
      </section>

      <section class="panel">
        <div class="section-title">
          <h2>最新候選與排名脈絡</h2>
          <div class="tabs" id="candidateTabs"></div>
        </div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>策略</th>
                <th>標的</th>
                <th>日期</th>
                <th>狀態</th>
                <th>分數/價格</th>
                <th>說明</th>
              </tr>
            </thead>
            <tbody id="candidateRows"></tbody>
          </table>
        </div>
      </section>

      <section class="panel" style="margin-top:14px">
        <div class="section-title"><h2>候選細節</h2></div>
        <div class="detail-grid">
          <div class="candidate-card" id="candidateSummary"></div>
          <div>
            <div class="field-grid" id="fieldGrid"></div>
          </div>
        </div>
      </section>
    </main>
  </div>

  <script>
    const DATA = {data_json};
    let activeStrategy = DATA.candidates.find(c => c.status === 'formal')?.strategy || DATA.strategies[0]?.id || 'S1';
    let activeCandidateFilter = 'all';
    let selectedCandidate = DATA.candidates.find(c => c.status === 'formal') || DATA.candidates[0] || null;

    const fmtPct = value => value === null || value === undefined || Number.isNaN(value) ? '-' : `${{(value * 100).toFixed(1)}}%`;
    const fmtNum = value => value === null || value === undefined || Number.isNaN(value) ? '-' : Number(value).toLocaleString(undefined, {{ maximumFractionDigits: 4 }});
    const statusClass = status => `status-${{String(status || '').replaceAll('_','-')}}`;

    function setText(id, value) {{
      const el = document.getElementById(id);
      if (el) el.textContent = value;
    }}

    function renderNav() {{
      const nav = document.getElementById('strategyNav');
      nav.innerHTML = DATA.strategies.map(s => `
        <button class="${{s.id === activeStrategy ? 'active' : ''}}" data-strategy="${{s.id}}">
          <div class="nav-id">${{s.id}}</div>
          <div class="nav-name">${{s.name}}</div>
          <span class="nav-status">${{s.statusLabel}}</span>
        </button>
      `).join('');
      nav.querySelectorAll('button').forEach(btn => {{
        btn.addEventListener('click', () => {{
          activeStrategy = btn.dataset.strategy;
          const first = DATA.candidates.find(c => c.strategy === activeStrategy);
          if (first) selectedCandidate = first;
          renderAll();
        }});
      }});
    }}

    function metric(label, value) {{
      return `<div class="metric"><span class="label">${{label}}</span><strong>${{value}}</strong></div>`;
    }}

    function renderStrategy() {{
      const s = DATA.strategies.find(item => item.id === activeStrategy) || DATA.strategies[0];
      if (!s) return;
      setText('strategyTitle', `${{s.id}} ${{s.name}}`);
      setText('strategyTheme', s.theme);
      const status = document.getElementById('strategyStatus');
      status.textContent = s.statusLabel;
      status.className = `status-pill ${{statusClass(s.status)}}`;

      const m = s.metrics || {{}};
      document.getElementById('metricGrid').innerHTML = [
        metric('完成交易', fmtNum(m.trades)),
        metric('勝率', fmtPct(m.winRate)),
        metric('平均報酬', fmtPct(m.avgNetRet)),
        metric('中位報酬', fmtPct(m.medianNetRet)),
        metric('最大虧損', fmtPct(m.maxLoss)),
        metric('Profit factor', fmtNum(m.profitFactor)),
      ].join('');
      document.getElementById('ruleList').innerHTML = s.rules.map(x => `<li>${{x}}</li>`).join('');
      document.getElementById('executionList').innerHTML = s.execution.map(x => `<li>${{x}}</li>`).join('');
      document.getElementById('noteList').innerHTML = s.notes.map(x => `<li>${{x}}</li>`).join('');
      document.getElementById('sourceList').innerHTML = s.sources.map(x => `<li>${{x}}</li>`).join('');
    }}

    function renderTabs() {{
      const tabs = [
        ['all', '全部'],
        ['formal', '正式候選'],
        ['rank-only', '排名參考'],
        ['stale', '歷史最新'],
      ];
      const wrap = document.getElementById('candidateTabs');
      wrap.innerHTML = tabs.map(([id, label]) => `<button class="tab ${{activeCandidateFilter === id ? 'active' : ''}}" data-filter="${{id}}">${{label}}</button>`).join('');
      wrap.querySelectorAll('button').forEach(btn => {{
        btn.addEventListener('click', () => {{
          activeCandidateFilter = btn.dataset.filter;
          renderCandidates();
        }});
      }});
    }}

    function candidateList() {{
      return DATA.candidates.filter(c => {{
        const strategyOk = activeStrategy === 'all' || c.strategy === activeStrategy;
        const filterOk = activeCandidateFilter === 'all' || c.status === activeCandidateFilter;
        return strategyOk && filterOk;
      }});
    }}

    function renderCandidates() {{
      const tbody = document.getElementById('candidateRows');
      const rows = candidateList();
      if (!rows.length) {{
        tbody.innerHTML = `<tr><td colspan="6"><div class="empty">目前篩選下沒有候選資料。</div></td></tr>`;
        return;
      }}
      tbody.innerHTML = rows.map((c, idx) => `
        <tr>
          <td>${{c.strategy}}</td>
          <td><button class="row-btn" data-index="${{idx}}">${{c.symbol}}</button></td>
          <td>${{c.date || '-'}}</td>
          <td><span class="status-pill ${{statusClass(c.status)}}">${{c.statusLabel}}</span></td>
          <td>score ${{fmtNum(c.score)}}<br>price ${{fmtNum(c.price)}}</td>
          <td>${{c.rankNote}}</td>
        </tr>
      `).join('');
      tbody.querySelectorAll('button').forEach((btn, idx) => {{
        btn.addEventListener('click', () => {{
          selectedCandidate = rows[idx];
          renderCandidateDetail();
        }});
      }});
      if (!selectedCandidate || !rows.some(c => c.strategy === selectedCandidate.strategy && c.symbol === selectedCandidate.symbol && c.status === selectedCandidate.status)) {{
        selectedCandidate = rows[0];
        renderCandidateDetail();
      }}
    }}

    function fieldWidth(value) {{
      if (value === true) return 100;
      if (value === false || value === null || value === undefined || Number.isNaN(value)) return 0;
      const n = Number(value);
      if (!Number.isFinite(n)) return 0;
      if (Math.abs(n) <= 1.5) return Math.max(0, Math.min(100, n * 100));
      return Math.max(0, Math.min(100, n));
    }}

    function fieldValue(value) {{
      if (value === true) return 'true';
      if (value === false) return 'false';
      if (value === null || value === undefined || Number.isNaN(value)) return '-';
      if (typeof value === 'number') return fmtNum(value);
      return value;
    }}

    function renderCandidateDetail() {{
      const card = document.getElementById('candidateSummary');
      const grid = document.getElementById('fieldGrid');
      if (!selectedCandidate) {{
        card.innerHTML = '<div class="empty">沒有候選可檢視。</div>';
        grid.innerHTML = '';
        return;
      }}
      card.innerHTML = `
        <span class="status-pill ${{statusClass(selectedCandidate.status)}}">${{selectedCandidate.statusLabel}}</span>
        <div class="candidate-symbol">${{selectedCandidate.symbol}}</div>
        <div class="label">策略</div>
        <strong>${{selectedCandidate.strategy}}</strong>
        <div style="height:12px"></div>
        <div class="label">日期</div>
        <strong>${{selectedCandidate.date || '-'}}</strong>
        <div style="height:12px"></div>
        <p class="lede">${{selectedCandidate.rankNote}}</p>
      `;
      const fields = selectedCandidate.fields || {{}};
      grid.innerHTML = Object.entries(fields).map(([key, value]) => `
        <div class="field">
          <span class="label">${{key}}</span>
          <strong>${{fieldValue(value)}}</strong>
          <div class="bar"><span style="--w:${{fieldWidth(value)}}%"></span></div>
        </div>
      `).join('');
    }}

    function renderAll() {{
      setText('latestDataDate', DATA.latestDataDate || '-');
      setText('nextOpenLabel', DATA.nextOpenLabel || '-');
      setText('strategyCount', DATA.summary.strategyCount);
      setText('formalCount', DATA.summary.formalCandidateCount);
      setText('contextCount', DATA.summary.rankContextCount);
      renderNav();
      renderStrategy();
      renderTabs();
      renderCandidates();
      renderCandidateDetail();
    }}

    renderAll();
  </script>
</body>
</html>
"""


def main() -> None:
    ensure_dir(OUT_DIR)
    payload = _build_payload()
    OUTPUT_HTML.write_text(_render(payload), encoding="utf-8")
    print(OUTPUT_HTML)


if __name__ == "__main__":
    main()
