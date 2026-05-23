#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

import pandas as pd


DEFAULT_SCORED = Path("data/_derived/scored.parquet")
DEFAULT_OUT = Path("data/_derived/event_report.html")


def load_scored(path: Path, cols: List[str]) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing scored.parquet: {path}")
    df = pd.read_parquet(path, columns=cols)
    df["date"] = pd.to_datetime(df["date"])
    return df


def pick_report_date(df: pd.DataFrame, date_str: str | None) -> pd.Timestamp:
    if date_str:
        return pd.to_datetime(date_str)
    ev = df[df["is_event"] == 1]
    if ev.empty:
        return df["date"].max()
    return ev["date"].max()


def format_table(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    fmt = {
        "close": "{:.2f}".format,
        "net_total": "{:,.0f}".format,
        "posnet_total": "{:,.0f}".format,
        "hhi_posnet": "{:.4f}".format,
        "top_posnet_ratio": "{:.3f}".format,
        "posnet_total_pct": "{:.3f}".format,
        "hhi_posnet_pct": "{:.3f}".format,
        "s_stage1": "{:.4f}".format,
        "s_stage1_W": "{:.4f}".format,
        "s_stage1_W_pct": "{:.4f}".format,
    }
    for c, f in fmt.items():
        if c in out.columns:
            out[c] = out[c].map(lambda v: f(v) if pd.notna(v) else "")
    return out


def build_html(df: pd.DataFrame, report_date: pd.Timestamp, scored_path: Path) -> str:
    title = f"Event Report ({report_date.date()})"
    subtitle = f"Source: {scored_path}"
    table_html = df.to_html(index=False, escape=False)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{title}</title>
  <style>
    :root {{
      --bg: #f7f7f4;
      --fg: #1f1f1f;
      --muted: #6b6b6b;
      --accent: #2d5f5d;
      --table-bg: #ffffff;
      --border: #dddddd;
    }}
    body {{
      margin: 24px;
      background: var(--bg);
      color: var(--fg);
      font-family: "Helvetica Neue", Helvetica, Arial, sans-serif;
    }}
    h1 {{
      margin: 0 0 6px 0;
      font-size: 22px;
      color: var(--accent);
    }}
    .meta {{
      margin-bottom: 18px;
      color: var(--muted);
      font-size: 13px;
    }}
    .table-wrap {{
      overflow-x: auto;
      background: var(--table-bg);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 12px;
    }}
    table {{
      border-collapse: collapse;
      width: 100%;
      font-size: 12px;
    }}
    th, td {{
      border: 1px solid var(--border);
      padding: 6px 8px;
      text-align: right;
      white-space: nowrap;
    }}
    th:first-child, td:first-child {{
      text-align: left;
    }}
    th:nth-child(2), td:nth-child(2) {{
      text-align: left;
    }}
    thead th {{
      background: #f0f0ec;
      position: sticky;
      top: 0;
      z-index: 1;
    }}
  </style>
</head>
<body>
  <h1>{title}</h1>
  <div class="meta">{subtitle}</div>
  <div class="table-wrap">
    {table_html}
  </div>
</body>
</html>
"""


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate daily event HTML report.")
    ap.add_argument("--scored", type=str, default=str(DEFAULT_SCORED))
    ap.add_argument("--date", type=str, default="", help="YYYY-MM-DD, default: latest event date")
    ap.add_argument("--out", type=str, default=str(DEFAULT_OUT))
    args = ap.parse_args()

    scored_path = Path(args.scored)
    out_path = Path(args.out)

    keep_cols = [
        "symbol",
        "date",
        "close",
        "net_total",
        "posnet_total",
        "hhi_posnet",
        "top_posnet_ratio",
        "dyn_k",
        "posnet_total_pct",
        "hhi_posnet_pct",
        "s_stage1",
        "s_stage1_W",
        "s_stage1_W_pct",
        "stage1_bucket",
        "is_event",
    ]

    df = load_scored(scored_path, keep_cols)
    report_date = pick_report_date(df, args.date or None)
    target_day = report_date.date()

    ev = df[(df["is_event"] == 1) & (df["date"].dt.date == target_day)].copy()
    ev = ev.sort_values(["s_stage1_W_pct", "symbol"], ascending=[False, True])

    if ev.empty:
        out_html = build_html(pd.DataFrame(columns=keep_cols), report_date, scored_path)
        out_path.write_text(out_html, encoding="utf-8")
        print(f"No events for {target_day}. Wrote {out_path}")
        return

    ev["date"] = ev["date"].dt.date.astype(str)
    ev = format_table(ev)

    out_html = build_html(ev, report_date, scored_path)
    out_path.write_text(out_html, encoding="utf-8")
    print(f"Wrote {out_path} (events={len(ev)})")


if __name__ == "__main__":
    main()
