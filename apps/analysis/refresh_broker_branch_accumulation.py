#!/usr/bin/env python3
"""Refresh broker-branch anomaly events and dependent review reports.

This is the operational entrypoint for the detection-only key-broker branch
workflow. It runs the heavy detector in chunks so routine refreshes do not get
stuck building one very large rolling window table.
"""

from __future__ import annotations

import argparse
import math
import shutil
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stockanalysis.analysis.broker_branch_accumulation import (  # noqa: E402
    DetectionConfig,
    build_parquet_index,
    load_warrant_mapping,
    parse_symbols,
    run_detection,
    select_underlyings,
)
from stockanalysis.config import resolve_data, resolve_output  # noqa: E402


def _latest_ohlc_date(path: Path) -> pd.Timestamp:
    ohlc = pd.read_parquet(path, columns=["date"])
    if ohlc.empty:
        raise ValueError(f"{path} has no rows")
    return pd.to_datetime(ohlc["date"], errors="coerce").max().normalize()


def _parse_date(value: str | None) -> pd.Timestamp | None:
    if not value:
        return None
    return pd.Timestamp(value).normalize()


def _case_rows(recent_events: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    if recent_events.empty:
        return pd.DataFrame()

    for (symbol, broker, broker_name), sub in recent_events.groupby(
        ["underlying_stock_id", "broker", "broker_name"],
        dropna=False,
    ):
        sub = sub.sort_values(["anomaly_score", "date"], ascending=[False, True])
        top = sub.iloc[0]

        def max_num(col: str) -> float:
            return float(pd.to_numeric(sub[col], errors="coerce").max()) if col in sub else np.nan

        rows.append(
            {
                "symbol": str(symbol),
                "broker": str(broker),
                "broker_name": "" if pd.isna(broker_name) else str(broker_name),
                "case_start": pd.to_datetime(sub["date"]).min().date().isoformat(),
                "case_end": pd.to_datetime(sub["date"]).max().date().isoformat(),
                "n_events": int(len(sub)),
                "event_types": ",".join(sorted(sub["event_type"].dropna().astype(str).unique())),
                "max_score": float(top["anomaly_score"]),
                "top_date": pd.to_datetime(top["date"]).date().isoformat(),
                "top_event_type": str(top["event_type"]),
                "stock_trigger_any": bool(sub["stock_trigger"].fillna(False).any()) if "stock_trigger" in sub else False,
                "warrant_trigger_any": bool(sub["warrant_trigger"].fillna(False).any()) if "warrant_trigger" in sub else False,
                "stock_window_net_buy_max": max_num("stock_window_net_buy"),
                "stock_window_branch_share_max": max_num("stock_window_branch_share"),
                "stock_window_net_ratio_max": max_num("stock_window_net_ratio"),
                "warrant_window_net_buy_max": max_num("warrant_window_net_buy"),
                "warrant_window_branch_share_max": max_num("warrant_window_branch_share"),
                "warrant_window_net_ratio_max": max_num("warrant_window_net_ratio"),
                "warrant_window_count_max": int(pd.to_numeric(sub["warrant_window_count"], errors="coerce").fillna(0).max())
                if "warrant_window_count" in sub
                else 0,
                "warrant_window_ids_top": top.get("warrant_window_ids", ""),
                "explanation_top": top.get("explanation", ""),
            }
        )

    out = pd.DataFrame(rows)
    return out.sort_values(["max_score", "top_date", "symbol", "broker"], ascending=[False, True, True, True]).reset_index(drop=True)


def _write_report(
    out_dir: Path,
    start: pd.Timestamp,
    end: pd.Timestamp,
    recent_start: pd.Timestamp,
    chunk_count: int,
    merged_rows: int,
    main_events: pd.DataFrame,
    feature_rows: int,
    recent_events: pd.DataFrame,
    recent_cases: pd.DataFrame,
    save_features: bool,
) -> None:
    lines = [
        "# Broker Branch Abnormal Accumulation Events",
        "",
        "- scope: detection only; no future returns, entry/exit rules, strategy optimization, or backtest ranking",
        "- run mode: chunked full-market event scan to avoid all-symbol memory pressure",
        f"- history window loaded: `{start.date()}` to `{end.date()}`",
        f"- recent review window: `{recent_start.date()}` to `{end.date()}`",
        f"- chunks: `{chunk_count}`",
        f"- merged event rows before global top cut: `{merged_rows}`",
        f"- saved main event rows: `{len(main_events)}`",
        f"- feature rows evaluated: `{feature_rows}`",
        f"- feature parquet: `{'refreshed' if save_features else 'not refreshed in this run; event outputs were refreshed only'}`",
        f"- recent event rows: `{len(recent_events)}`",
        f"- recent case rows: `{len(recent_cases)}`",
        "",
    ]
    if recent_cases.empty:
        lines.append("No recent abnormal branch accumulation cases met the configured thresholds.")
    else:
        lines.extend(["## Top Recent Cases", ""])
        for row in recent_cases.head(30).itertuples(index=False):
            lines.append(
                f"- `{row.symbol}` `{row.broker_name or row.broker}` `{row.top_event_type}` "
                f"top_date=`{row.top_date}` score=`{row.max_score:.2f}` events=`{row.n_events}`"
            )
    (out_dir / "broker_branch_accumulation_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def refresh_events(args: argparse.Namespace) -> Path:
    ohlc_path = Path(args.ohlc)
    end = _parse_date(args.end_date) or _latest_ohlc_date(ohlc_path)
    start = _parse_date(args.start_date) or (end - pd.Timedelta(days=int(args.lookback_days))).normalize()
    recent_start = _parse_date(args.recent_start) or (end - pd.Timedelta(days=int(args.recent_days) - 1)).normalize()

    out_dir = Path(args.output_dir)
    tmp_root = Path(args.tmp_dir)
    if tmp_root.exists():
        shutil.rmtree(tmp_root)
    tmp_root.mkdir(parents=True, exist_ok=True)

    base_cfg = DetectionConfig(
        broker_dirs=tuple(Path(p) for p in args.broker_dirs),
        warrant_list_path=Path(args.warrant_list),
        broker_list_path=Path(args.broker_list),
        ohlc_path=ohlc_path,
        output_dir=tmp_root / "base",
        start_date=start,
        end_date=end,
        symbols=None,
        max_symbols=0,
        exclude_etf=bool(args.exclude_etf),
        include_unknown_brokers=bool(args.include_unknown_brokers),
        min_branch_suffix_len=int(args.min_branch_suffix_len),
        window_days=int(args.window_days),
        min_history_days=int(args.min_history_days),
        min_positive_days=int(args.min_positive_days),
        min_window_net=float(args.min_window_net),
        min_window_net_ratio=float(args.min_window_net_ratio),
        min_net_buy_ratio=float(args.min_net_buy_ratio),
        min_branch_window_share=float(args.min_branch_window_share),
        max_single_day_share=float(args.max_single_day_share),
        combined_other_min_net=float(args.combined_other_min_net),
        combined_bonus=float(args.combined_bonus),
        max_avg_volume_20d=float(args.max_avg_volume_20d),
        top_n=int(args.chunk_top_n),
        save_features=bool(args.save_features),
    )

    parquet_index = build_parquet_index(base_cfg.broker_dirs)
    warrant_mapping = load_warrant_mapping(base_cfg.warrant_list_path)
    underlyings = select_underlyings(parquet_index, warrant_mapping, parse_symbols(args.symbols), 0, base_cfg.exclude_etf)
    chunk_size = math.ceil(len(underlyings) / int(args.chunks))
    chunks = [underlyings[i : i + chunk_size] for i in range(0, len(underlyings), chunk_size)]
    print(f"chunked event scan underlyings={len(underlyings)} chunks={len(chunks)} chunk_size={chunk_size}", flush=True)

    event_parts: list[pd.DataFrame] = []
    feature_rows = 0
    for idx, symbols in enumerate(chunks):
        chunk_name = f"out_{idx:02d}"
        cfg = replace(base_cfg, output_dir=tmp_root / chunk_name, symbols=set(symbols))
        print(f"[{chunk_name}] start symbols={len(symbols)} first={symbols[0]} last={symbols[-1]}", flush=True)
        events, features = run_detection(cfg)
        feature_rows += len(features)
        print(f"[{chunk_name}] features={len(features)} events={len(events)}", flush=True)
        if not events.empty:
            events = events.copy()
            events["chunk_source"] = chunk_name
            event_parts.append(events)
        del events, features

    merged = pd.concat(event_parts, ignore_index=True) if event_parts else pd.DataFrame()
    out_dir.mkdir(parents=True, exist_ok=True)
    if not merged.empty:
        merged["date"] = pd.to_datetime(merged["date"])
        merged = merged.sort_values(
            ["anomaly_score", "date", "underlying_stock_id", "broker"],
            ascending=[False, True, True, True],
        ).reset_index(drop=True)
        main_events = merged.head(int(args.global_top_n)).copy()
        main_events["rank"] = np.arange(1, len(main_events) + 1)
    else:
        main_events = merged

    main_events.to_csv(out_dir / "broker_branch_accumulation_events.csv", index=False, encoding="utf-8-sig")
    main_events.to_parquet(out_dir / "broker_branch_accumulation_events.parquet", index=False)

    recent_events = merged[merged["date"] >= recent_start].copy() if not merged.empty else merged.copy()
    if not recent_events.empty:
        recent_events = recent_events.sort_values(
            ["anomaly_score", "date", "underlying_stock_id", "broker"],
            ascending=[False, True, True, True],
        ).reset_index(drop=True)
        recent_events["recent_rank"] = np.arange(1, len(recent_events) + 1)
    recent_cases = _case_rows(recent_events)

    suffix = f"{recent_start:%Y%m%d}_{end:%Y%m%d}"
    recent_events_path = out_dir / f"recent_events_{suffix}.csv"
    recent_cases_path = out_dir / f"recent_cases_{suffix}.csv"
    recent_events.to_csv(recent_events_path, index=False, encoding="utf-8-sig")
    recent_cases.to_csv(recent_cases_path, index=False, encoding="utf-8-sig")
    _write_report(out_dir, start, end, recent_start, len(chunks), len(merged), main_events, feature_rows, recent_events, recent_cases, bool(args.save_features))

    if not args.keep_tmp:
        shutil.rmtree(tmp_root, ignore_errors=True)
    print(f"wrote {out_dir}")
    print(f"main_events={len(main_events)} recent_events={len(recent_events)} recent_cases={len(recent_cases)} feature_rows={feature_rows}")
    return recent_cases_path


def rebuild_trigger_report(args: argparse.Namespace, broker_branch_cases: Path) -> None:
    if args.skip_trigger_report:
        return
    cmd = [
        sys.executable,
        str(ROOT / "apps" / "analysis" / "build_trigger_days_gt5_case_review.py"),
        "--broker-branch-cases",
        str(broker_branch_cases),
    ]
    if not args.fetch_trigger_news:
        cmd.append("--skip-news")
    print("rebuilding trigger-days report:", " ".join(cmd), flush=True)
    subprocess.run(cmd, cwd=ROOT, check=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh broker-branch anomaly events and dependent reports.")
    parser.add_argument("--broker-dirs", nargs="+", type=Path, default=[resolve_data("bs_report", "parquet_twse"), resolve_data("bs_report", "parquet_tpex")])
    parser.add_argument("--warrant-list", type=Path, default=resolve_data("warrant", "warrant_list_dedup.csv"))
    parser.add_argument("--broker-list", type=Path, default=resolve_data("broker_list.csv"))
    parser.add_argument("--ohlc", type=Path, default=resolve_data("_derived", "ohlc.parquet"))
    parser.add_argument("--output-dir", type=Path, default=resolve_output("analysis", "broker_branch_accumulation"))
    parser.add_argument("--tmp-dir", type=Path, default=Path("/private/tmp/broker_branch_accumulation_refresh"))
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--lookback-days", type=int, default=98)
    parser.add_argument("--recent-start")
    parser.add_argument("--recent-days", type=int, default=7)
    parser.add_argument("--symbols", default=None, help="Comma-separated underlying stock ids to scan.")
    parser.add_argument("--chunks", type=int, default=20)
    parser.add_argument("--chunk-top-n", type=int, default=250)
    parser.add_argument("--global-top-n", type=int, default=1000)
    parser.add_argument("--exclude-etf", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--include-unknown-brokers", action="store_true")
    parser.add_argument("--min-branch-suffix-len", type=int, default=2)
    parser.add_argument("--window-days", type=int, default=5)
    parser.add_argument("--min-history-days", type=int, default=20)
    parser.add_argument("--min-positive-days", type=int, default=2)
    parser.add_argument("--min-window-net", type=float, default=10_000.0)
    parser.add_argument("--min-window-net-ratio", type=float, default=3.0)
    parser.add_argument("--min-net-buy-ratio", type=float, default=0.5)
    parser.add_argument("--min-branch-window-share", type=float, default=0.10)
    parser.add_argument("--max-single-day-share", type=float, default=0.80)
    parser.add_argument("--combined-other-min-net", type=float, default=1.0)
    parser.add_argument("--combined-bonus", type=float, default=0.25)
    parser.add_argument("--max-avg-volume-20d", type=float, default=100_000_000.0)
    parser.add_argument("--save-features", action="store_true", help="Write per-chunk feature parquet files under the temporary directory.")
    parser.add_argument("--keep-tmp", action="store_true")
    parser.add_argument("--skip-trigger-report", action="store_true")
    parser.add_argument("--fetch-trigger-news", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    recent_cases = refresh_events(args)
    rebuild_trigger_report(args, recent_cases)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
