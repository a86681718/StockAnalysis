"""Detect broad stock-side broker-flow accumulation anomalies.

The detector compares each stock with its own trailing broker-flow history. It
uses only same-day and trailing price context. It does not use future returns,
company locations, or broker allowlists.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from stockanalysis.analysis.broker_branch_accumulation import (
    build_parquet_index,
    load_broker_lookup,
    parse_date,
    parse_symbols,
    read_broker_parquet,
)
from stockanalysis.analysis.single_branch_persistent_accumulation import select_stock_paths
from stockanalysis.config import ensure_dir, resolve_data, resolve_output


@dataclass(frozen=True)
class GeneralAnomalyConfig:
    broker_dirs: tuple[Path, ...]
    broker_list_path: Path
    output_dir: Path
    start_date: Optional[pd.Timestamp]
    end_date: Optional[pd.Timestamp]
    symbols: Optional[set[str]]
    max_symbols: int
    recent_window: int
    baseline_window: int
    min_baseline_days: int
    pressure_quantile: float
    min_positive_days: int
    min_buyer_retention: float
    min_buy_participation: float
    min_pressure_multiple: float
    min_pressure_days: int
    max_single_day_pressure_share: float
    ohlc_path: Path
    price_window: int
    max_price_position: float
    episode_gap_sessions: int
    min_episode_triggers: int


def load_symbol_daily(
    symbol: str,
    paths: list[Path],
    broker_lookup: dict[str, str],
    start_date: Optional[pd.Timestamp],
    end_date: Optional[pd.Timestamp],
) -> pd.DataFrame:
    frames = [read_broker_parquet(path, start_date, end_date) for path in paths]
    frames = [frame for frame in frames if not frame.empty]
    if not frames:
        return pd.DataFrame()
    raw = pd.concat(frames, ignore_index=True)
    raw = raw.groupby(["date", "broker"], as_index=False).agg(
        buy_volume=("buy_volume", "sum"),
        sell_volume=("sell_volume", "sum"),
    )
    raw["net_buy"] = raw["buy_volume"] - raw["sell_volume"]
    raw["broker_name"] = raw["broker"].map(broker_lookup).fillna(raw["broker"])
    raw.insert(0, "symbol", symbol)
    return raw.sort_values(["date", "broker"]).reset_index(drop=True)


def load_price_context(
    path: Path,
    symbols: set[str],
    start_date: Optional[pd.Timestamp],
    end_date: Optional[pd.Timestamp],
    price_window: int,
) -> pd.DataFrame:
    if not path.exists() or not symbols:
        return pd.DataFrame()
    ohlc = pd.read_parquet(path, columns=["symbol", "date", "high", "low", "close"])
    if ohlc.empty:
        return ohlc
    ohlc["symbol"] = ohlc["symbol"].astype(str)
    ohlc["date"] = pd.to_datetime(ohlc["date"], errors="coerce").dt.normalize()
    ohlc = ohlc[ohlc["symbol"].isin(symbols) & ohlc["date"].notna()].copy()
    if start_date is not None:
        ohlc = ohlc[ohlc["date"] >= start_date]
    if end_date is not None:
        ohlc = ohlc[ohlc["date"] <= end_date]
    if ohlc.empty:
        return ohlc
    for col in ("high", "low", "close"):
        ohlc[col] = pd.to_numeric(ohlc[col], errors="coerce")
    ohlc = ohlc.sort_values(["symbol", "date"]).reset_index(drop=True)
    grouped = ohlc.groupby("symbol", group_keys=False)
    rolling_low = grouped["low"].transform(lambda s: s.rolling(price_window, min_periods=10).min())
    rolling_high = grouped["high"].transform(lambda s: s.rolling(price_window, min_periods=10).max())
    price_range = rolling_high - rolling_low
    ohlc["price_position"] = (ohlc["close"] - rolling_low) / price_range.replace(0, np.nan)
    return ohlc[["symbol", "date", "close", "price_position"]]


def build_window_metrics(
    raw: pd.DataFrame,
    cfg: GeneralAnomalyConfig,
    price_context: Optional[pd.DataFrame | dict[str, pd.DataFrame]] = None,
) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame()
    price_by_date = pd.DataFrame()
    if price_context is not None:
        symbol = str(raw["symbol"].iloc[0])
        if isinstance(price_context, dict):
            price_by_date = price_context.get(symbol, pd.DataFrame())
        else:
            price_by_date = price_context[price_context["symbol"] == symbol].set_index("date")
    dates = pd.DatetimeIndex(raw["date"].dropna().sort_values().unique())
    rows: list[dict[str, object]] = []
    for end_index in range(cfg.recent_window - 1, len(dates)):
        window_dates = dates[end_index - cfg.recent_window + 1 : end_index + 1]
        window = raw[raw["date"].isin(window_dates)]
        daily_pressure = (
            window.assign(positive_net=lambda frame: frame["net_buy"].clip(lower=0))
            .groupby("date", as_index=False)["positive_net"]
            .sum()
            .rename(columns={"positive_net": "daily_positive_pressure"})
        )
        pressure_days = daily_pressure[daily_pressure["daily_positive_pressure"] > 0]
        broker_window = window.groupby(["broker", "broker_name"], as_index=False).agg(
            buy_volume=("buy_volume", "sum"), sell_volume=("sell_volume", "sum")
        )
        broker_window["net_buy"] = broker_window["buy_volume"] - broker_window["sell_volume"]
        buyers = broker_window[broker_window["net_buy"] > 0].sort_values("net_buy", ascending=False)
        positive_pressure = float(buyers["net_buy"].sum())
        shares = buyers["net_buy"] / positive_pressure if positive_pressure > 0 else pd.Series(dtype=float)
        buyer_codes = set(buyers["broker"].astype(str))
        positive_days = (
            window[window["broker"].astype(str).isin(buyer_codes)]
            .assign(positive=lambda frame: frame["net_buy"] > 0)
            .groupby("broker")["positive"]
            .sum()
        )
        names = buyers["broker_name"].astype(str).head(10).tolist()
        buyer_buy_volume = float(buyers["buy_volume"].sum())
        max_daily_pressure = (
            float(daily_pressure["daily_positive_pressure"].max()) if not daily_pressure.empty else 0.0
        )
        max_single_day_pressure_share = (
            max_daily_pressure / positive_pressure if positive_pressure > 0 else np.nan
        )
        price_position = np.nan
        latest_close = np.nan
        if not price_by_date.empty:
            price_window = price_by_date.reindex(window_dates)
            if "close" in price_window:
                latest_close = (
                    float(price_window["close"].dropna().iloc[-1])
                    if price_window["close"].notna().any()
                    else np.nan
                )
            if "price_position" in price_window and not pressure_days.empty:
                weights = pressure_days.set_index("date")["daily_positive_pressure"]
                aligned = price_window["price_position"].reindex(weights.index).dropna()
                weights = weights.reindex(aligned.index)
                if len(aligned) and float(weights.sum()) > 0:
                    price_position = float(np.average(aligned, weights=weights))
        rows.append(
            {
                "symbol": raw["symbol"].iloc[0],
                "date": dates[end_index],
                "recent_positive_pressure": positive_pressure,
                "recent_net_buy": positive_pressure,
                "recent_total_buy": float(window["buy_volume"].sum()),
                "recent_positive_days": float(positive_days.max()) if len(positive_days) else 0.0,
                "recent_mean_buyer_count": float(len(buyers)),
                "recent_mean_top_share": float(shares.iloc[0]) if len(shares) else np.nan,
                "recent_mean_hhi": float((shares**2).sum()) if len(shares) else np.nan,
                "recent_pressure_days": int(len(pressure_days)),
                "max_single_day_pressure_share": float(max_single_day_pressure_share),
                "recent_price_position": price_position,
                "latest_close": latest_close,
                "recent_buyer_names": ",".join(names),
                "recent_buyer_union_count": len(buyers),
                "top_buyer": names[0] if names else "",
                "buyer_retention": positive_pressure / buyer_buy_volume if buyer_buy_volume > 0 else np.nan,
            }
        )
    return pd.DataFrame(rows)


def build_features(frame: pd.DataFrame, cfg: GeneralAnomalyConfig) -> pd.DataFrame:
    if frame.empty or len(frame) < cfg.min_baseline_days:
        return pd.DataFrame()
    frame = frame.copy().sort_values("date").reset_index(drop=True)

    prior_pressure = frame["recent_positive_pressure"].shift(1)
    baseline = prior_pressure.rolling(cfg.baseline_window, min_periods=cfg.min_baseline_days)
    frame["baseline_pressure_median"] = baseline.median()
    frame["baseline_pressure_threshold"] = baseline.quantile(cfg.pressure_quantile)
    frame["pressure_multiple"] = frame["recent_positive_pressure"] / frame[
        "baseline_pressure_median"
    ].replace(0, np.nan)
    frame["buy_participation"] = frame["recent_positive_pressure"] / frame["recent_total_buy"].replace(0, np.nan)

    prior_hhi = frame["recent_mean_hhi"].shift(1).rolling(
        cfg.baseline_window, min_periods=cfg.min_baseline_days
    ).median()
    prior_breadth = frame["recent_mean_buyer_count"].shift(1).rolling(
        cfg.baseline_window, min_periods=cfg.min_baseline_days
    ).median()
    frame["hhi_shift"] = frame["recent_mean_hhi"] - prior_hhi
    frame["breadth_multiple"] = frame["recent_mean_buyer_count"] / prior_breadth.replace(0, np.nan)
    frame["qualifies"] = (
        frame["baseline_pressure_threshold"].notna()
        & (frame["recent_positive_pressure"] >= frame["baseline_pressure_threshold"])
        & (frame["pressure_multiple"] >= cfg.min_pressure_multiple)
        & (frame["recent_positive_days"] >= cfg.min_positive_days)
        & (frame["recent_pressure_days"] >= cfg.min_pressure_days)
        & (frame["max_single_day_pressure_share"] <= cfg.max_single_day_pressure_share)
        & (frame["recent_price_position"] <= cfg.max_price_position)
        & (frame["buyer_retention"] >= cfg.min_buyer_retention)
        & (frame["buy_participation"] >= cfg.min_buy_participation)
    )
    frame["pattern"] = frame.apply(classify_pattern, axis=1)
    return frame


def classify_pattern(row: pd.Series) -> str:
    if not bool(row.get("qualifies", False)):
        return "none"
    if row["hhi_shift"] <= -0.05 and row["breadth_multiple"] >= 1.25:
        return "breadth_expansion"
    if row["recent_mean_top_share"] >= 0.55:
        return "concentrated_surge"
    if row["recent_mean_top_share"] <= 0.35 and row["recent_buyer_union_count"] >= 5:
        return "distributed_surge"
    return "mixed_accumulation"


def build_episodes(
    features: pd.DataFrame,
    cfg: GeneralAnomalyConfig,
    raw: Optional[pd.DataFrame] = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    triggers = features[features["qualifies"]].copy().sort_values("date").reset_index(drop=True)
    if triggers.empty:
        return pd.DataFrame(), triggers
    positions = {pd.Timestamp(date): index for index, date in enumerate(features["date"])}
    groups: list[list[int]] = [[0]]
    for index in range(1, len(triggers)):
        previous = positions[pd.Timestamp(triggers.iloc[groups[-1][-1]]["date"])]
        current = positions[pd.Timestamp(triggers.iloc[index]["date"])]
        if current - previous <= cfg.episode_gap_sessions:
            groups[-1].append(index)
        else:
            groups.append([index])

    rows: list[dict[str, object]] = []
    for indices in groups:
        group = triggers.iloc[indices]
        if len(group) < cfg.min_episode_triggers:
            continue
        latest = group.iloc[-1]
        pattern_counts = group["pattern"].value_counts()
        primary_buyer = latest["top_buyer"]
        primary_buyer_net_buy = np.nan
        primary_buyer_share = np.nan
        if raw is not None and not raw.empty:
            episode_raw = raw[
                (raw["date"] >= pd.Timestamp(group.iloc[0]["date"]))
                & (raw["date"] <= pd.Timestamp(latest["date"]))
            ]
            broker_totals = episode_raw.groupby(["broker", "broker_name"], as_index=False).agg(
                buy_volume=("buy_volume", "sum"), sell_volume=("sell_volume", "sum")
            )
            broker_totals["net_buy"] = broker_totals["buy_volume"] - broker_totals["sell_volume"]
            positive_totals = broker_totals[broker_totals["net_buy"] > 0].sort_values(
                "net_buy", ascending=False
            )
            if not positive_totals.empty:
                primary = positive_totals.iloc[0]
                positive_total = float(positive_totals["net_buy"].sum())
                primary_buyer = primary["broker_name"]
                primary_buyer_net_buy = float(primary["net_buy"])
                primary_buyer_share = primary_buyer_net_buy / positive_total if positive_total > 0 else np.nan
        severity = (
            np.log1p(float(group["pressure_multiple"].max()))
            * float(group["buyer_retention"].median())
            * np.sqrt(len(group))
            * (0.5 + float(group["buy_participation"].max()))
        )
        rows.append(
            {
                "symbol": group.iloc[0]["symbol"],
                "episode_start": group.iloc[0]["date"],
                "last_qualifying_date": latest["date"],
                "qualifying_dates": len(group),
                "dominant_pattern": pattern_counts.index[0],
                "pattern_sequence": ",".join(group["pattern"].drop_duplicates()),
                "max_pressure_multiple": float(group["pressure_multiple"].max()),
                "max_recent_positive_pressure": float(group["recent_positive_pressure"].max()),
                "max_recent_net_buy": float(group["recent_net_buy"].max()),
                "median_buyer_retention": float(group["buyer_retention"].median()),
                "max_buy_participation": float(group["buy_participation"].max()),
                "median_pressure_days": float(group["recent_pressure_days"].median()),
                "max_single_day_pressure_share": float(group["max_single_day_pressure_share"].max()),
                "median_price_position": float(group["recent_price_position"].median()),
                "latest_close": float(latest["latest_close"]),
                "max_buyer_union_count": int(group["recent_buyer_union_count"].max()),
                "min_mean_top_share": float(group["recent_mean_top_share"].min()),
                "max_mean_top_share": float(group["recent_mean_top_share"].max()),
                "primary_buyer": primary_buyer,
                "primary_buyer_net_buy": primary_buyer_net_buy,
                "primary_buyer_share": primary_buyer_share,
                "latest_window_top_buyer": latest["top_buyer"],
                "buyer_names": latest["recent_buyer_names"],
                "severity_score": float(severity),
            }
        )
    return pd.DataFrame(rows), triggers


def write_outputs(
    episodes: pd.DataFrame,
    triggers: pd.DataFrame,
    cfg: GeneralAnomalyConfig,
    scanned_symbols: int,
) -> None:
    ensure_dir(cfg.output_dir)
    if not episodes.empty:
        episodes = episodes.sort_values(
            ["severity_score", "last_qualifying_date"], ascending=[False, False]
        ).reset_index(drop=True)
        episodes.insert(0, "rank", np.arange(1, len(episodes) + 1))
    review_cases = episodes.drop_duplicates("symbol").reset_index(drop=True) if not episodes.empty else episodes.copy()
    if not review_cases.empty:
        review_cases["review_rank"] = np.arange(1, len(review_cases) + 1)
    episodes.to_csv(cfg.output_dir / "general_broker_flow_anomaly_episodes.csv", index=False)
    episodes.to_parquet(cfg.output_dir / "general_broker_flow_anomaly_episodes.parquet", index=False)
    review_cases.to_csv(cfg.output_dir / "general_broker_flow_anomaly_review_cases.csv", index=False)
    review_cases.to_parquet(cfg.output_dir / "general_broker_flow_anomaly_review_cases.parquet", index=False)
    triggers.to_parquet(cfg.output_dir / "general_broker_flow_anomaly_daily_triggers.parquet", index=False)
    summary = {
        "config": {key: str(value) if isinstance(value, Path) else value for key, value in asdict(cfg).items()},
        "scanned_symbols": scanned_symbols,
        "episode_count": int(len(episodes)),
        "unique_symbols": int(episodes["symbol"].nunique()) if not episodes.empty else 0,
        "review_case_count": int(len(review_cases)),
        "daily_trigger_count": int(len(triggers)),
        "pattern_counts": episodes["dominant_pattern"].value_counts().to_dict() if not episodes.empty else {},
    }
    (cfg.output_dir / "general_broker_flow_anomaly_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
    )
    top = review_cases.head(50) if not review_cases.empty else review_cases
    lines = [
        "# General Broker-Flow Anomaly Report",
        "",
        "> Detection-only output. Uses same-day and trailing price context; no future outcome is used.",
        "",
        f"- scanned symbols: `{scanned_symbols}`",
        f"- distinct episodes: `{len(episodes)}`",
        f"- unique symbols: `{summary['unique_symbols']}`",
        f"- one-per-symbol review cases: `{summary['review_case_count']}`",
        f"- daily triggers: `{len(triggers)}`",
        f"- minimum pressure days: `{cfg.min_pressure_days}`",
        f"- maximum single-day pressure share: `{cfg.max_single_day_pressure_share:.0%}`",
        f"- maximum weighted {cfg.price_window}-session price position: `{cfg.max_price_position:.0%}`",
        f"- pattern counts: `{json.dumps(summary['pattern_counts'], ensure_ascii=False)}`",
        "",
        "## Top 50 Episodes",
        "",
        "| Rank | Symbol | Pattern | Start | Last | Trigger Days | Pressure Multiple | Net Buy | Participation | Pressure Days | Single-Day Share | Price Position | Primary Buyer |",
        "|---:|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in top.itertuples(index=False):
        lines.append(
            f"| {row.review_rank} | {row.symbol} | {row.dominant_pattern} | {pd.Timestamp(row.episode_start):%Y-%m-%d} | "
            f"{pd.Timestamp(row.last_qualifying_date):%Y-%m-%d} | {row.qualifying_dates} | "
            f"{row.max_pressure_multiple:.2f}x | {row.max_recent_net_buy:,.0f} | "
            f"{row.max_buy_participation:.1%} | {row.median_pressure_days:.1f} | "
            f"{row.max_single_day_pressure_share:.1%} | {row.median_price_position:.1%} | "
            f"{row.primary_buyer} |"
        )
    (cfg.output_dir / "general_broker_flow_anomaly_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Detect broad broker-flow accumulation anomalies")
    parser.add_argument("--broker-dirs", nargs="+", type=Path, default=[resolve_data("bs_report", "parquet_twse"), resolve_data("bs_report", "parquet_tpex")])
    parser.add_argument("--broker-list", type=Path, default=resolve_data("broker_list.csv"))
    parser.add_argument("--output-dir", type=Path, default=resolve_output("analysis", "general_broker_flow_anomaly"))
    parser.add_argument("--start-date", default="2025-07-01")
    parser.add_argument("--end-date", default="")
    parser.add_argument("--symbols", default="")
    parser.add_argument("--max-symbols", type=int, default=0)
    parser.add_argument("--recent-window", type=int, default=5)
    parser.add_argument("--baseline-window", type=int, default=60)
    parser.add_argument("--min-baseline-days", type=int, default=30)
    parser.add_argument("--pressure-quantile", type=float, default=0.95)
    parser.add_argument("--min-positive-days", type=int, default=3)
    parser.add_argument("--min-buyer-retention", type=float, default=0.50)
    parser.add_argument("--min-buy-participation", type=float, default=0.08)
    parser.add_argument("--min-pressure-multiple", type=float, default=1.50)
    parser.add_argument("--min-pressure-days", type=int, default=3)
    parser.add_argument("--max-single-day-pressure-share", type=float, default=0.65)
    parser.add_argument("--ohlc", type=Path, default=resolve_data("_derived", "ohlc.parquet"))
    parser.add_argument("--price-window", type=int, default=60)
    parser.add_argument("--max-price-position", type=float, default=0.65)
    parser.add_argument("--episode-gap-sessions", type=int, default=5)
    parser.add_argument("--min-episode-triggers", type=int, default=3)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = GeneralAnomalyConfig(
        broker_dirs=tuple(args.broker_dirs),
        broker_list_path=args.broker_list,
        output_dir=args.output_dir,
        start_date=parse_date(args.start_date),
        end_date=parse_date(args.end_date),
        symbols=parse_symbols(args.symbols),
        max_symbols=args.max_symbols,
        recent_window=args.recent_window,
        baseline_window=args.baseline_window,
        min_baseline_days=args.min_baseline_days,
        pressure_quantile=args.pressure_quantile,
        min_positive_days=args.min_positive_days,
        min_buyer_retention=args.min_buyer_retention,
        min_buy_participation=args.min_buy_participation,
        min_pressure_multiple=args.min_pressure_multiple,
        min_pressure_days=max(1, int(args.min_pressure_days)),
        max_single_day_pressure_share=args.max_single_day_pressure_share,
        ohlc_path=args.ohlc,
        price_window=max(10, int(args.price_window)),
        max_price_position=args.max_price_position,
        episode_gap_sessions=args.episode_gap_sessions,
        min_episode_triggers=args.min_episode_triggers,
    )
    lookup = load_broker_lookup(cfg.broker_list_path)
    paths = select_stock_paths(build_parquet_index(cfg.broker_dirs), cfg.symbols, set(), cfg.max_symbols)
    price_context = load_price_context(
        cfg.ohlc_path,
        {symbol for symbol, _ in paths},
        cfg.start_date,
        cfg.end_date,
        cfg.price_window,
    )
    if price_context.empty:
        raise FileNotFoundError(f"No price context loaded from {cfg.ohlc_path}")
    price_context_by_symbol = {
        symbol: frame.set_index("date")
        for symbol, frame in price_context.groupby("symbol", sort=False)
    }
    all_episodes: list[pd.DataFrame] = []
    all_triggers: list[pd.DataFrame] = []
    for index, (symbol, symbol_paths) in enumerate(paths, start=1):
        raw = load_symbol_daily(symbol, symbol_paths, lookup, cfg.start_date, cfg.end_date)
        features = build_features(build_window_metrics(raw, cfg, price_context_by_symbol), cfg)
        if not features.empty:
            episodes, triggers = build_episodes(features, cfg, raw)
            if not episodes.empty:
                all_episodes.append(episodes)
            if not triggers.empty:
                all_triggers.append(triggers)
        if index % 100 == 0 or index == len(paths):
            print(
                f"processed {index}/{len(paths)} symbols, episodes={sum(len(frame) for frame in all_episodes)}",
                flush=True,
            )
    episodes = pd.concat(all_episodes, ignore_index=True) if all_episodes else pd.DataFrame()
    triggers = pd.concat(all_triggers, ignore_index=True) if all_triggers else pd.DataFrame()
    write_outputs(episodes, triggers, cfg, len(paths))
    print(cfg.output_dir / "general_broker_flow_anomaly_report.md")


if __name__ == "__main__":
    main()
