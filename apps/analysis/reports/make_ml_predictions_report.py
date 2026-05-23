#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generate a simple HTML report for ML predictions
with date filter and pred sorting.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def add_top10_counts(df: pd.DataFrame, pred_col: str, lookback: int = 10, topn: int = 10) -> pd.DataFrame:
    df = df.copy()
    df["symbol"] = df["symbol"].astype(str)
    df["date"] = pd.to_datetime(df["date"])

    dates = sorted(df["date"].dropna().unique().tolist())
    top10_by_date = {}
    for d in dates:
        sub = df[df["date"] == d].sort_values(pred_col, ascending=False)
        top10_by_date[d] = set(sub["symbol"].head(topn))

    df["top10_count_prev10"] = 0
    df["top10_first_prev10_date"] = ""
    for i, d in enumerate(dates):
        prev_dates = dates[max(0, i - lookback): i]
        if not prev_dates:
            continue
        prev_sets = [top10_by_date[pd] for pd in prev_dates]
        mask = df["date"] == d
        syms = df.loc[mask, "symbol"].astype(str)
        counts = []
        first_dates = []
        for sym in syms:
            count = sum(sym in s for s in prev_sets)
            counts.append(count)
            first_date = ""
            if count:
                for pd_date, pd_set in zip(prev_dates, prev_sets):
                    if sym in pd_set:
                        first_date = pd_date.strftime("%Y-%m-%d")
                        break
            first_dates.append(first_date)
        df.loc[mask, "top10_count_prev10"] = counts
        df.loc[mask, "top10_first_prev10_date"] = first_dates

    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    return df


def add_scored_metrics(df: pd.DataFrame, scored_path: Path) -> pd.DataFrame:
    if not scored_path.exists():
        return df

    use_cols = [
        "symbol",
        "date",
        "close",
        "net_total",
        "posnet_total",
        "negnet_total",
        "hhi_posnet",
        "hhi_negnet",
        "top_posnet_ratio",
        "top_negnet_ratio",
        "dyn_k",
        "dyn_k_neg",
        "s_stage1_W_pct",
        "s_stage1_adj_W_pct",
        "sell_ratio",
    ]
    scored_cols = set(pd.read_parquet(scored_path, engine="pyarrow").columns)
    if "mfe_10d" in scored_cols:
        use_cols.append("mfe_10d")

    scored = pd.read_parquet(scored_path, columns=use_cols)
    scored["symbol"] = scored["symbol"].astype(str)
    scored["date"] = pd.to_datetime(scored["date"])

    scored["history_days"] = scored.groupby("symbol")["date"].transform("count")

    g = scored.groupby("symbol", group_keys=False)
    future = g["close"].shift(-1)
    mean_next = future.rolling(10, min_periods=10).mean().shift(-(10 - 1))
    scored["mean_ret_10d"] = mean_next / scored["close"] - 1.0

    out = df.copy()
    out["date"] = pd.to_datetime(out["date"])
    cols = [
        "symbol",
        "date",
        "mean_ret_10d",
        "history_days",
        "net_total",
        "posnet_total",
        "negnet_total",
        "hhi_posnet",
        "hhi_negnet",
        "top_posnet_ratio",
        "top_negnet_ratio",
        "dyn_k",
        "dyn_k_neg",
        "s_stage1_W_pct",
        "s_stage1_adj_W_pct",
        "sell_ratio",
    ]
    if "mfe_10d" in scored.columns:
        cols.append("mfe_10d")
    out = out.merge(scored[cols], on=["symbol", "date"], how="left")
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    return out


def add_strategy_flags(
    df: pd.DataFrame,
    pred_col: str = "pred",
    a_pred_th: float = 0.70,
    b_pred_th: float = 0.75,
    b_topn: int = 10,
) -> pd.DataFrame:
    out = df.copy()
    if pred_col not in out.columns:
        out["rank_pred"] = pd.NA
        out["strategy_A"] = False
        out["strategy_B"] = False
        return out

    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out[pred_col] = pd.to_numeric(out[pred_col], errors="coerce")
    out = out.sort_values(["date", pred_col], ascending=[True, False]).copy()
    out["rank_pred"] = out.groupby("date").cumcount() + 1

    out["strategy_A"] = (out["rank_pred"] == 1) & (out[pred_col] >= a_pred_th)
    out["strategy_B"] = (out["rank_pred"] <= b_topn) & (out[pred_col] >= b_pred_th)

    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    return out


def build_html(
    records: list[dict],
    dates: list[str],
    default_date: str,
    columns: list[str],
    title: str,
    percent_cols: list[str],
    int_cols: list[str],
    bool_cols: list[str],
    min_history: int,
) -> str:
    data_json = json.dumps(records, ensure_ascii=False)
    dates_json = json.dumps(dates, ensure_ascii=False)
    default_date_json = json.dumps(default_date, ensure_ascii=False)
    columns_json = json.dumps(columns, ensure_ascii=False)
    percent_cols_json = json.dumps(percent_cols, ensure_ascii=False)
    int_cols_json = json.dumps(int_cols, ensure_ascii=False)
    bool_cols_json = json.dumps(bool_cols, ensure_ascii=False)

    ths = []
    for col in columns:
        th_id = " id=\"predHeader\"" if col == "pred" else ""
        ths.append(f"<th{th_id}>{col}</th>")
    th_html = "\n        ".join(ths)

    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>{title}</title>
  <style>
    body {{
      font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
      margin: 20px;
      color: #111827;
    }}
    .controls {{
      display: flex;
      align-items: center;
      gap: 12px;
      margin-bottom: 16px;
    }}
    select {{
      padding: 6px 10px;
      font-size: 14px;
    }}
    table {{
      border-collapse: collapse;
      width: 100%;
      font-size: 13px;
    }}
    th, td {{
      padding: 6px 8px;
      border-bottom: 1px solid #e5e7eb;
      text-align: right;
    }}
    th:first-child, td:first-child {{
      text-align: left;
    }}
    th {{
      font-weight: 700;
      cursor: pointer;
      user-select: none;
    }}
    .muted {{
      color: #6b7280;
      font-size: 12px;
    }}
  </style>
</head>
<body>
  <h2>{title}</h2>
  <div class="controls">
    <label for="dateSelect">Date</label>
    <select id="dateSelect"></select>
    <label for="strategySelect">Strategy</label>
    <select id="strategySelect">
      <option value="all">All</option>
      <option value="strategy_a">A: Top1 + pred>=0.70</option>
      <option value="strategy_b">B: Top10 + pred>=0.75</option>
      <option value="either">A or B</option>
    </select>
    <span id="count" class="muted"></span>
    <span id="sortHint" class="muted"></span>
  </div>

  <table id="predTable">
    <thead>
      <tr>
        {th_html}
      </tr>
    </thead>
    <tbody></tbody>
  </table>

  <script>
    const data = {data_json};
    const dates = {dates_json};
    const defaultDate = {default_date_json};
    const columns = {columns_json};
    const percentCols = new Set({percent_cols_json});
    const intCols = new Set({int_cols_json});
    const boolCols = new Set({bool_cols_json});
    const minHistory = {min_history};
    let sortDesc = true;

    const dateSelect = document.getElementById('dateSelect');
    const strategySelect = document.getElementById('strategySelect');
    const tbody = document.querySelector('#predTable tbody');
    const count = document.getElementById('count');
    const sortHint = document.getElementById('sortHint');
    const predHeader = document.getElementById('predHeader');
    const textCols = new Set(['symbol', 'date']);
    const sparkCache = new Map();
    const symbolSeries = new Map();

    function buildSeries() {{
      for (const r of data) {{
        if (!r.symbol || !r.date) continue;
        const sym = String(r.symbol);
        const date = String(r.date).slice(0, 10);
        const pred = Number(r.pred);
        if (Number.isNaN(pred)) continue;
        if (!symbolSeries.has(sym)) {{
          symbolSeries.set(sym, {{ dates: [], preds: [] }});
        }}
        const s = symbolSeries.get(sym);
        s.dates.push(date);
        s.preds.push(pred);
      }}
      for (const [sym, s] of symbolSeries.entries()) {{
        const idx = s.dates.map((d, i) => [d, i]).sort((a, b) => a[0].localeCompare(b[0]));
        const dates = idx.map(x => x[0]);
        const preds = idx.map(x => s.preds[x[1]]);
        symbolSeries.set(sym, {{ dates, preds }});
      }}
    }}

    function findIndex(dates, target) {{
      let lo = 0, hi = dates.length - 1, ans = -1;
      while (lo <= hi) {{
        const mid = Math.floor((lo + hi) / 2);
        if (dates[mid] <= target) {{
          ans = mid;
          lo = mid + 1;
        }} else {{
          hi = mid - 1;
        }}
      }}
      return ans;
    }}

    function sparkline(values) {{
      if (!values.length) return '';
      const w = 120, h = 24, pad = 2;
      const vmin = Math.min(...values);
      const vmax = Math.max(...values);
      const span = vmax - vmin || 1;
      const step = (w - 2 * pad) / Math.max(1, values.length - 1);
      const pts = values.map((v, i) => {{
        const x = pad + step * i;
        const y = h - pad - ((v - vmin) / span) * (h - 2 * pad);
        return `${{x.toFixed(1)}},${{y.toFixed(1)}}`;
      }}).join(' ');
      return `<svg width="${{w}}" height="${{h}}" viewBox="0 0 ${{w}} ${{h}}" xmlns="http://www.w3.org/2000/svg"><polyline fill="none" stroke="#111827" stroke-width="1.5" points="${{pts}}"/></svg>`;
    }}

    function getSpark(sym, date) {{
      const key = sym + '|' + date;
      if (sparkCache.has(key)) return sparkCache.get(key);
      const s = symbolSeries.get(sym);
      if (!s) return '';
      const idx = findIndex(s.dates, date);
      if (idx < 0) return '';
      const start = Math.max(0, idx - 19);
      const vals = s.preds.slice(start, idx + 1);
      const svg = sparkline(vals);
      sparkCache.set(key, svg);
      return svg;
    }}

    function fmt(x) {{
      if (x === null || x === undefined || x === '') return '';
      const n = Number(x);
      if (Number.isNaN(n)) return x;
      return n.toFixed(2);
    }}

    function fmtPct(x) {{
      if (x === null || x === undefined || x === '') return '';
      const n = Number(x);
      if (Number.isNaN(n)) return x;
      return (n * 100).toFixed(2) + '%';
    }}

    function fmtInt(x) {{
      if (x === null || x === undefined || x === '') return '';
      const n = Number(x);
      if (Number.isNaN(n)) return x;
      return Math.round(n).toLocaleString('en-US');
    }}

    function fmtBool(x) {{
      if (x === null || x === undefined || x === '') return '';
      if (x === true || x === 'true' || x === 1 || x === '1') return 'true';
      if (x === false || x === 'false' || x === 0 || x === '0') return 'false';
      return String(x);
    }}

    function renderTable(rows) {{
      tbody.innerHTML = '';
      for (const r of rows) {{
        const tr = document.createElement('tr');
        const tds = columns.map((c) => {{
          const v = r[c];
          if (textCols.has(c)) return `<td>${{v ?? ''}}</td>`;
          if (boolCols.has(c)) return `<td>${{fmtBool(v)}}</td>`;
          if (intCols.has(c)) return `<td>${{fmtInt(v)}}</td>`;
          if (percentCols.has(c)) return `<td>${{fmtPct(v)}}</td>`;
          if (c === 'pred_trend_20d') {{
            const sym = String(r.symbol ?? '');
            const date = String(r.date ?? '').slice(0, 10);
            return `<td>${{getSpark(sym, date)}}</td>`;
          }}
          return `<td>${{fmt(v)}}</td>`;
        }});
        tr.innerHTML = tds.join('');
        tbody.appendChild(tr);
      }}
      count.textContent = `rows: ${{rows.length}}`;
      sortHint.textContent = predHeader ? (sortDesc ? 'sorted by pred (desc)' : 'sorted by pred (asc)') : '';
    }}

    function update() {{
      const d = dateSelect.value;
      const mode = strategySelect.value;
      const filtered = data.filter(r => {{
        if (r.date !== d) return false;
        const sym = r.symbol === null || r.symbol === undefined ? '' : String(r.symbol);
        if (sym.length !== 4) return false;
        const h = Number(r.history_days);
        if (Number.isNaN(h)) return false;
        if (h < minHistory) return false;
        const isA = (r.strategy_A === true || r.strategy_A === 'true' || r.strategy_A === 1 || r.strategy_A === '1');
        const isB = (r.strategy_B === true || r.strategy_B === 'true' || r.strategy_B === 1 || r.strategy_B === '1');
        if (mode === 'strategy_a' && !isA) return false;
        if (mode === 'strategy_b' && !isB) return false;
        if (mode === 'either' && !(isA || isB)) return false;
        return true;
      }});
      if (predHeader) {{
        filtered.sort((a, b) => sortDesc ? (b.pred - a.pred) : (a.pred - b.pred));
      }}
      renderTable(filtered);
    }}

    for (const d of dates) {{
      const opt = document.createElement('option');
      opt.value = d;
      opt.textContent = d;
      if (d === defaultDate) opt.selected = true;
      dateSelect.appendChild(opt);
    }}

    dateSelect.addEventListener('change', update);
    strategySelect.addEventListener('change', update);
    if (predHeader) {{
      predHeader.addEventListener('click', () => {{
        sortDesc = !sortDesc;
        update();
      }});
    }}

    buildSeries();
    update();
  </script>
</body>
</html>
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Make ML predictions HTML report")
    parser.add_argument("--csv", type=str, default="data/_derived/ml_runs/days_above_ge5_predictions_all.csv")
    parser.add_argument("--scored", type=str, default="data/_derived/scored.parquet")
    parser.add_argument("--out", type=str, default="data/_derived/ml_mfe10_report.html")
    parser.add_argument("--min-history", type=int, default=60)
    parser.add_argument("--a-pred-th", type=float, default=0.70)
    parser.add_argument("--b-pred-th", type=float, default=0.75)
    parser.add_argument("--b-topn", type=int, default=10)
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise FileNotFoundError(f"missing {csv_path}")

    df = pd.read_csv(csv_path, low_memory=False)
    if df.empty:
        raise RuntimeError("empty predictions csv")

    df["date"] = df["date"].astype(str)
    df["symbol"] = df["symbol"].astype(str)

    if "top10_count_prev10" not in df.columns and "pred" in df.columns:
        df = add_top10_counts(df, pred_col="pred", lookback=10, topn=10)
    df = add_scored_metrics(df, Path(args.scored))
    df = add_strategy_flags(
        df,
        pred_col="pred",
        a_pred_th=args.a_pred_th,
        b_pred_th=args.b_pred_th,
        b_topn=args.b_topn,
    )
    if "history_days" not in df.columns:
        df["history_days"] = df.groupby("symbol")["date"].transform("count")
    else:
        df["history_days"] = df["history_days"].fillna(
            df.groupby("symbol")["date"].transform("count")
        )
    if "days_above_10d_ge5" in df.columns:
        df["days_above_10d_ge5"] = df["days_above_10d_ge5"].astype(bool)
    if "top10_count_prev10" in df.columns:
        df["top10_count_prev10"] = df["top10_count_prev10"].fillna(0).astype(int)
    if "top10_first_prev10_date" in df.columns:
        df["top10_first_prev10_date"] = df["top10_first_prev10_date"].fillna("").astype(str)
    dates = sorted(df["date"].unique().tolist())
    default_date = dates[-1]

    base_cols = [c for c in ["symbol", "date", "pred"] if c in df.columns]
    left_meta = [
        c
        for c in [
            "rank_pred",
            "strategy_A",
            "strategy_B",
            "days_above_10d_ge5",
            "top10_count_prev10",
            "top10_first_prev10_date",
        ]
        if c in df.columns and c not in base_cols
    ]
    feature_cols = [
        c
        for c in [
            "mean_ret_10d",
            "mfe_10d",
            "mean_ret_10d_weighted_ge5",
            "label",
            "net_total",
            "posnet_total",
            "negnet_total",
            "hhi_posnet",
            "hhi_negnet",
            "top_posnet_ratio",
            "top_negnet_ratio",
            "dyn_k",
            "dyn_k_neg",
            "s_stage1_W_pct",
            "s_stage1_adj_W_pct",
            "sell_ratio",
        ]
        if c in df.columns and c not in base_cols + left_meta
    ]
    remaining = [
        c
        for c in df.columns
        if c not in base_cols + left_meta + feature_cols and c != "history_days"
    ]
    columns = base_cols + left_meta + ["pred_trend_20d"] + feature_cols + remaining

    data_cols = [c for c in columns if c in df.columns]
    if "history_days" in df.columns and "history_days" not in data_cols:
        data_cols.append("history_days")

    title = "ML WRET10_GE5 Latest Predictions"
    percent_cols = [
        c
        for c in [
            "mean_ret_10d",
            "mfe_10d",
            "top_posnet_ratio",
            "top_negnet_ratio",
            "s_stage1_W_pct",
            "s_stage1_adj_W_pct",
            "sell_ratio",
        ]
        if c in columns
    ]
    int_cols = [
        c
        for c in [
            "rank_pred",
            "top10_count_prev10",
            "posnet_total",
            "negnet_total",
            "dyn_k",
            "dyn_k_neg",
        ]
        if c in columns
    ]
    bool_cols = [c for c in ["strategy_A", "strategy_B", "days_above_10d_ge5"] if c in columns]
    col_labels = {
        "rank_pred": "rank",
        "strategy_A": "strategy_A",
        "strategy_B": "strategy_B",
        "days_above_10d_ge5": "ge5",
        "top10_count_prev10": "top10_cnt10",
        "top10_first_prev10_date": "top10_first10",
    }
    display_cols = [col_labels.get(c, c) for c in columns]
    records = []
    for _, row in df.iterrows():
        rec = {}
        for raw, disp in zip(columns, display_cols):
            if raw == "pred_trend_20d":
                continue
            if raw not in df.columns:
                continue
            rec[disp] = row.get(raw)
        if "history_days" in df.columns:
            rec["history_days"] = row.get("history_days")
        records.append(rec)

    percent_cols = [col_labels.get(c, c) for c in percent_cols]
    int_cols = [col_labels.get(c, c) for c in int_cols]
    bool_cols = [col_labels.get(c, c) for c in bool_cols]

    html = build_html(
        records,
        dates,
        default_date,
        display_cols,
        title,
        percent_cols,
        int_cols,
        bool_cols,
        args.min_history,
    )

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
    print(f"Wrote: {out_path}")


if __name__ == "__main__":
    main()
