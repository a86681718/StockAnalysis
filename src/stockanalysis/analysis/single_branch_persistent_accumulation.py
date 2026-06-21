"""Detect persistent stock accumulation by one local broker branch.

This detector is intentionally limited to stock-side broker behavior. It does
not use prices, future returns, warrants, or multi-branch aggregation.
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
    normalize_code,
    parse_date,
    parse_symbols,
    read_broker_parquet,
)
from stockanalysis.config import ensure_dir, resolve_data, resolve_output


DEFAULT_EXCLUDED_DESK_TERMS = (
    "總公司",
    "營業",
    "經紀部",
    "法人部",
    "法人",
    "自營",
    "承銷",
    "國際部",
    "國際證券",
    "金融交易部",
    "債券部",
    "網路",
)
DEFAULT_EXCLUDED_SYMBOLS = frozenset({"2317", "2330"})


@dataclass(frozen=True)
class PersistentAccumulationConfig:
    broker_dirs: tuple[Path, ...]
    broker_list_path: Path
    output_dir: Path
    start_date: Optional[pd.Timestamp]
    end_date: Optional[pd.Timestamp]
    symbols: Optional[set[str]]
    excluded_symbols: set[str]
    max_symbols: int
    short_window: int
    long_window: int
    baseline_window: int
    min_baseline_days: int
    min_short_positive_days: int
    min_short_net_buy: float
    min_short_purity: float
    min_short_buy_share: float
    min_long_positive_days: int
    min_long_net_buy: float
    min_long_purity: float
    min_long_buy_share: float
    episode_gap_sessions: int
    acceleration_ratio: float
    broad_broker_symbol_threshold: int


def is_local_branch_name(name: object, excluded_terms: tuple[str, ...] = DEFAULT_EXCLUDED_DESK_TERMS) -> bool:
    text = str(name or "").strip().replace("－", "-")
    if "-" not in text:
        return False
    parent, suffix = text.rsplit("-", 1)
    if not parent.strip() or len(suffix.strip()) < 2:
        return False
    return not any(term in text for term in excluded_terms)


def select_stock_paths(
    parquet_index: dict[str, list[Path]],
    symbols: Optional[set[str]],
    excluded_symbols: set[str],
    max_symbols: int,
) -> list[tuple[str, list[Path]]]:
    selected = symbols if symbols is not None else {symbol for symbol in parquet_index if symbol.isdigit() and len(symbol) == 4}
    selected = {symbol for symbol in selected if not symbol.startswith("00") and symbol not in excluded_symbols}
    ordered = sorted(selected)
    if max_symbols > 0:
        ordered = ordered[:max_symbols]
    return [(symbol, parquet_index.get(symbol, [])) for symbol in ordered if parquet_index.get(symbol)]


def load_symbol_stock_daily(
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

    daily = pd.concat(frames, ignore_index=True)
    daily = (
        daily.groupby(["date", "broker"], as_index=False)
        .agg(buy_volume=("buy_volume", "sum"), sell_volume=("sell_volume", "sum"))
    )
    daily["broker"] = daily["broker"].map(normalize_code)
    daily["broker_name"] = daily["broker"].map(broker_lookup).fillna("")
    daily["is_local_branch"] = daily["broker_name"].map(is_local_branch_name)
    daily["net_buy"] = daily["buy_volume"] - daily["sell_volume"]
    daily.insert(0, "symbol", symbol)
    return daily.sort_values(["date", "broker"]).reset_index(drop=True)


def _rolling_sum(grouped: pd.core.groupby.SeriesGroupBy, window: int) -> pd.Series:
    return grouped.rolling(window, min_periods=window).sum().reset_index(level=0, drop=True)


def _rolling_max(grouped: pd.core.groupby.SeriesGroupBy, window: int) -> pd.Series:
    return grouped.rolling(window, min_periods=window).max().reset_index(level=0, drop=True)


def build_symbol_features(daily: pd.DataFrame, cfg: PersistentAccumulationConfig) -> pd.DataFrame:
    if daily.empty:
        return daily.copy()

    dates = pd.DatetimeIndex(daily["date"].dropna().sort_values().unique())
    eligible = daily[daily["is_local_branch"]].copy()
    if eligible.empty or len(dates) < cfg.short_window:
        return pd.DataFrame()

    brokers = sorted(eligible["broker"].unique())
    names = eligible.drop_duplicates("broker").set_index("broker")["broker_name"].to_dict()
    stock_daily = daily.groupby("date", as_index=True)["buy_volume"].sum().reindex(dates, fill_value=0.0)
    full_index = pd.MultiIndex.from_product([brokers, dates], names=["broker", "date"])
    frame = (
        eligible.set_index(["broker", "date"])[["buy_volume", "sell_volume", "net_buy"]]
        .reindex(full_index)
        .fillna(0.0)
        .reset_index()
    )
    frame.insert(0, "symbol", str(daily["symbol"].iloc[0]))
    frame["broker_name"] = frame["broker"].map(names).fillna("")
    frame["stock_buy_volume"] = frame["date"].map(stock_daily).fillna(0.0)
    frame["buy_day"] = (frame["buy_volume"] > 0).astype(float)
    frame["positive_day"] = (frame["net_buy"] > 0).astype(float)
    frame["positive_net_buy"] = frame["net_buy"].clip(lower=0.0)
    frame = frame.sort_values(["broker", "date"]).reset_index(drop=True)

    grouped = frame.groupby("broker", group_keys=False)
    for window, label in ((cfg.short_window, "short"), (cfg.long_window, "long")):
        frame[f"{label}_buy_volume"] = _rolling_sum(grouped["buy_volume"], window)
        frame[f"{label}_sell_volume"] = _rolling_sum(grouped["sell_volume"], window)
        frame[f"{label}_net_buy"] = _rolling_sum(grouped["net_buy"], window)
        frame[f"{label}_positive_days"] = _rolling_sum(grouped["positive_day"], window)
        frame[f"{label}_buy_days"] = _rolling_sum(grouped["buy_day"], window)
        frame[f"{label}_max_positive_net"] = _rolling_max(grouped["positive_net_buy"], window)
        stock_window_buy = frame.groupby("broker", group_keys=False)["stock_buy_volume"].rolling(
            window, min_periods=window
        ).sum().reset_index(level=0, drop=True)
        frame[f"{label}_purity"] = frame[f"{label}_net_buy"] / frame[f"{label}_buy_volume"].replace(0, np.nan)
        frame[f"{label}_buy_share"] = frame[f"{label}_buy_volume"] / stock_window_buy.replace(0, np.nan)
        frame[f"{label}_max_day_share"] = frame[f"{label}_max_positive_net"] / frame[
            f"{label}_net_buy"
        ].replace(0, np.nan)

    shifted_buy_day = grouped["buy_day"].shift(cfg.short_window)
    shifted_net_buy = grouped["net_buy"].shift(cfg.short_window)
    shifted_buy_volume = grouped["buy_volume"].shift(cfg.short_window)
    baseline_group = frame.assign(
        baseline_buy_day=shifted_buy_day,
        baseline_net_buy=shifted_net_buy,
        baseline_buy_volume=shifted_buy_volume,
    ).groupby("broker", group_keys=False)
    frame["baseline_buy_day_rate"] = baseline_group["baseline_buy_day"].rolling(
        cfg.baseline_window, min_periods=cfg.min_baseline_days
    ).mean().reset_index(level=0, drop=True)
    frame["baseline_mean_net_buy"] = baseline_group["baseline_net_buy"].rolling(
        cfg.baseline_window, min_periods=cfg.min_baseline_days
    ).mean().reset_index(level=0, drop=True)
    frame["baseline_mean_buy_volume"] = baseline_group["baseline_buy_volume"].rolling(
        cfg.baseline_window, min_periods=cfg.min_baseline_days
    ).mean().reset_index(level=0, drop=True)
    frame["recent_buy_day_rate"] = frame["short_buy_days"] / cfg.short_window
    frame["participation_shift"] = frame["recent_buy_day_rate"] - frame["baseline_buy_day_rate"]
    expected_short_buy = frame["baseline_mean_buy_volume"] * cfg.short_window
    frame["activity_multiple"] = frame["short_buy_volume"] / expected_short_buy.replace(0, np.nan)

    frame["onset_gate"] = (
        frame["baseline_buy_day_rate"].notna()
        & (frame["short_positive_days"] >= cfg.min_short_positive_days)
        & (frame["short_net_buy"] >= cfg.min_short_net_buy)
        & (frame["short_purity"] >= cfg.min_short_purity)
        & (frame["short_buy_share"] >= cfg.min_short_buy_share)
    )
    frame["confirmation_gate"] = (
        (frame["long_positive_days"] >= cfg.min_long_positive_days)
        & (frame["long_net_buy"] >= cfg.min_long_net_buy)
        & (frame["long_purity"] >= cfg.min_long_purity)
        & (frame["long_buy_share"] >= cfg.min_long_buy_share)
    )
    short_pace = frame["short_net_buy"] / cfg.short_window
    long_prior_pace = (frame["long_net_buy"] - frame["short_net_buy"]) / max(1, cfg.long_window - cfg.short_window)
    frame["acceleration_ratio"] = short_pace / long_prior_pace.replace(0, np.nan)
    frame["qualifies"] = frame["onset_gate"] | frame["confirmation_gate"]
    return frame.sort_values(["symbol", "broker", "date"]).reset_index(drop=True)


def _episode_status(latest: pd.Series, cfg: PersistentAccumulationConfig) -> str:
    if latest["short_net_buy"] < 0 or latest["short_purity"] < 0:
        return "unwinding"
    if latest["confirmation_gate"] and latest["acceleration_ratio"] >= cfg.acceleration_ratio:
        return "accelerating"
    if latest["confirmation_gate"]:
        return "confirmed"
    if latest["onset_gate"]:
        return "onset"
    return "cooling"


def build_episodes(features: pd.DataFrame, cfg: PersistentAccumulationConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    if features.empty:
        return pd.DataFrame(), pd.DataFrame()

    daily_triggers = features[features["qualifies"]].copy()
    if daily_triggers.empty:
        return pd.DataFrame(), daily_triggers

    episodes: list[dict[str, object]] = []
    for (symbol, broker), sub in features.groupby(["symbol", "broker"], sort=True):
        sub = sub.sort_values("date").reset_index(drop=True)
        trigger_positions = sub.index[sub["qualifies"]].tolist()
        if not trigger_positions:
            continue
        groups: list[list[int]] = [[trigger_positions[0]]]
        for position in trigger_positions[1:]:
            if position - groups[-1][-1] <= cfg.episode_gap_sessions:
                groups[-1].append(position)
            else:
                groups.append([position])

        for positions in groups:
            start_pos = positions[0]
            last_trigger_pos = positions[-1]
            ongoing = len(sub) - 1 - last_trigger_pos <= cfg.episode_gap_sessions
            observation_end_pos = len(sub) - 1 if ongoing else last_trigger_pos
            span = sub.iloc[start_pos : observation_end_pos + 1]
            first = sub.iloc[start_pos]
            latest = sub.iloc[observation_end_pos]
            cumulative_buy = float(span["buy_volume"].sum())
            cumulative_sell = float(span["sell_volume"].sum())
            cumulative_net = cumulative_buy - cumulative_sell
            stock_buy = float(span["stock_buy_volume"].sum())
            status = _episode_status(latest, cfg) if ongoing else "ended"
            episodes.append(
                {
                    "symbol": symbol,
                    "broker": broker,
                    "broker_name": first["broker_name"],
                    "episode_start": first["date"],
                    "last_qualifying_date": sub.iloc[last_trigger_pos]["date"],
                    "status_as_of": latest["date"],
                    "status": status,
                    "ongoing": ongoing,
                    "observed_sessions": len(span),
                    "active_sessions": int(((span["buy_volume"] + span["sell_volume"]) > 0).sum()),
                    "buy_sessions": int((span["buy_volume"] > 0).sum()),
                    "positive_net_sessions": int((span["net_buy"] > 0).sum()),
                    "cumulative_buy": cumulative_buy,
                    "cumulative_sell": cumulative_sell,
                    "cumulative_net_buy": cumulative_net,
                    "cumulative_purity": cumulative_net / cumulative_buy if cumulative_buy > 0 else np.nan,
                    "cumulative_buy_share": cumulative_buy / stock_buy if stock_buy > 0 else np.nan,
                    "retention_ratio": 1.0 - cumulative_sell / cumulative_buy if cumulative_buy > 0 else np.nan,
                    "baseline_buy_day_rate_at_start": first["baseline_buy_day_rate"],
                    "participation_shift_at_start": first["participation_shift"],
                    "activity_multiple_at_start": first["activity_multiple"],
                    "start_short_net_buy": first["short_net_buy"],
                    "start_short_positive_days": first["short_positive_days"],
                    "start_short_purity": first["short_purity"],
                    "start_short_buy_share": first["short_buy_share"],
                    "max_short_net_buy": float(span["short_net_buy"].max()),
                    "max_short_buy_share": float(span["short_buy_share"].max()),
                    "latest_short_net_buy": latest["short_net_buy"],
                    "latest_short_positive_days": latest["short_positive_days"],
                    "latest_short_purity": latest["short_purity"],
                    "latest_short_buy_share": latest["short_buy_share"],
                    "latest_long_net_buy": latest["long_net_buy"],
                    "latest_long_positive_days": latest["long_positive_days"],
                    "latest_long_purity": latest["long_purity"],
                    "latest_long_buy_share": latest["long_buy_share"],
                    "latest_acceleration_ratio": latest["acceleration_ratio"],
                    "qualifying_dates": len(positions),
                }
            )

    out = pd.DataFrame(episodes)
    if out.empty:
        return out, daily_triggers
    return out.reset_index(drop=True), daily_triggers


def classify_and_rank_episodes(episodes: pd.DataFrame, cfg: PersistentAccumulationConfig) -> pd.DataFrame:
    if episodes.empty:
        return episodes.copy()
    out = episodes.copy()
    out["positive_session_rate"] = out["positive_net_sessions"] / out["observed_sessions"].replace(0, np.nan)
    common_quality = (out["cumulative_purity"] >= 0.80) & (out["cumulative_buy_share"] >= 0.05)
    out["confidence"] = np.select(
        [
            common_quality
            & (out["observed_sessions"] >= 40)
            & (out["positive_net_sessions"] >= 25)
            & (out["qualifying_dates"] >= 20),
            common_quality
            & (out["observed_sessions"] >= 10)
            & (out["positive_net_sessions"] >= 8)
            & (out["qualifying_dates"] >= 5),
            common_quality & (out["positive_net_sessions"] >= 3) & (out["qualifying_dates"] >= 3),
        ],
        ["established", "confirmed", "developing"],
        default="preliminary",
    )
    ongoing_breadth = (
        out[out["ongoing"]]
        .groupby("broker")["symbol"]
        .nunique()
    )
    out["broker_ongoing_symbol_count"] = out["broker"].map(ongoing_breadth).fillna(0).astype(int)
    out["broad_broker_flag"] = out["broker_ongoing_symbol_count"] >= cfg.broad_broker_symbol_threshold
    out["review_eligible"] = (
        out["ongoing"]
        & out["confidence"].isin(["established", "confirmed", "developing"])
        & ~out["broad_broker_flag"]
    )

    confidence_order = {"established": 0, "confirmed": 1, "developing": 2, "preliminary": 3}
    status_order = {"accelerating": 0, "confirmed": 1, "onset": 2, "cooling": 3, "unwinding": 4, "ended": 5}
    out["confidence_order"] = out["confidence"].map(confidence_order).fillna(9)
    out["status_order"] = out["status"].map(status_order).fillna(9)
    out = out.sort_values(
        [
            "review_eligible",
            "ongoing",
            "confidence_order",
            "status_order",
            "qualifying_dates",
            "cumulative_buy_share",
            "cumulative_net_buy",
        ],
        ascending=[False, False, True, True, False, False, False],
    ).drop(columns=["confidence_order", "status_order"]).reset_index(drop=True)
    if "rank" in out.columns:
        out = out.drop(columns="rank")
    out.insert(0, "rank", np.arange(1, len(out) + 1))
    return out


def write_outputs(
    episodes: pd.DataFrame,
    daily_triggers: pd.DataFrame,
    cfg: PersistentAccumulationConfig,
    scanned_symbols: int,
) -> None:
    out_dir = ensure_dir(cfg.output_dir)
    episodes.to_csv(out_dir / "single_branch_persistent_episodes.csv", index=False, encoding="utf-8-sig")
    episodes.to_parquet(out_dir / "single_branch_persistent_episodes.parquet", index=False)
    daily_triggers.to_parquet(out_dir / "single_branch_persistent_daily_triggers.parquet", index=False)
    summary = {
        "scope": "single local-branch persistent stock accumulation; detection only",
        "scanned_symbols": scanned_symbols,
        "episode_count": len(episodes),
        "ongoing_episode_count": int(episodes["ongoing"].sum()) if not episodes.empty else 0,
        "review_eligible_count": int(episodes["review_eligible"].sum()) if not episodes.empty else 0,
        "daily_trigger_count": len(daily_triggers),
        "excluded_symbols": sorted(cfg.excluded_symbols),
        "config": {
            key: value
            for key, value in asdict(cfg).items()
            if key not in {"broker_dirs", "broker_list_path", "output_dir", "symbols", "excluded_symbols"}
        },
    }
    for key, value in list(summary["config"].items()):
        if isinstance(value, pd.Timestamp):
            summary["config"][key] = value.strftime("%Y-%m-%d")
    (out_dir / "single_branch_persistent_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    lines = [
        "# Single Local-Branch Persistent Accumulation",
        "",
        "- scope: stock-side detection only; no prices, future returns, warrants, or multi-branch aggregation",
        f"- scanned symbols: `{scanned_symbols}`",
        f"- excluded symbols: `{','.join(sorted(cfg.excluded_symbols)) or 'none'}`",
        f"- windows: short `{cfg.short_window}`, long `{cfg.long_window}`, baseline `{cfg.baseline_window}` sessions",
        f"- episodes: `{len(episodes)}`; ongoing: `{summary['ongoing_episode_count']}`",
        f"- review eligible: `{summary['review_eligible_count']}`",
        "",
        "## Ongoing Episodes",
        "",
    ]
    ongoing = episodes[episodes["review_eligible"]].head(50) if not episodes.empty else episodes
    if ongoing.empty:
        lines.append("No ongoing episodes met the configured gates.")
    else:
        for row in ongoing.itertuples(index=False):
            lines.append(
                f"- `{row.rank}` `{row.symbol}` `{row.broker_name}` `{row.status}` `{row.confidence}` "
                f"start=`{pd.Timestamp(row.episode_start).date()}` net=`{row.cumulative_net_buy:.0f}` "
                f"purity=`{row.cumulative_purity:.2%}` share=`{row.cumulative_buy_share:.2%}` "
                f"positive=`{row.positive_net_sessions}/{row.observed_sessions}`"
            )
    (out_dir / "single_branch_persistent_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_detection(cfg: PersistentAccumulationConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    broker_lookup = load_broker_lookup(cfg.broker_list_path)
    parquet_index = build_parquet_index(cfg.broker_dirs)
    selected = select_stock_paths(parquet_index, cfg.symbols, cfg.excluded_symbols, cfg.max_symbols)
    all_episodes: list[pd.DataFrame] = []
    all_daily_triggers: list[pd.DataFrame] = []
    for index, (symbol, paths) in enumerate(selected, start=1):
        daily = load_symbol_stock_daily(symbol, paths, broker_lookup, cfg.start_date, cfg.end_date)
        features = build_symbol_features(daily, cfg)
        if not features.empty:
            episodes, daily_triggers = build_episodes(features, cfg)
            if not episodes.empty:
                all_episodes.append(episodes)
            if not daily_triggers.empty:
                all_daily_triggers.append(daily_triggers)
        if index % 250 == 0:
            print(f"processed {index}/{len(selected)} stock symbols", flush=True)
    episodes = pd.concat(all_episodes, ignore_index=True) if all_episodes else pd.DataFrame()
    daily_triggers = pd.concat(all_daily_triggers, ignore_index=True) if all_daily_triggers else pd.DataFrame()
    episodes = classify_and_rank_episodes(episodes, cfg)
    write_outputs(episodes, daily_triggers, cfg, len(selected))
    return episodes, daily_triggers


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Detect single local-branch persistent stock accumulation.")
    parser.add_argument(
        "--broker-dirs",
        nargs="+",
        type=Path,
        default=[resolve_data("bs_report", "parquet_twse"), resolve_data("bs_report", "parquet_tpex")],
    )
    parser.add_argument("--broker-list", type=Path, default=resolve_data("broker_list.csv"))
    parser.add_argument(
        "--output-dir", type=Path, default=resolve_output("analysis", "single_branch_persistent_accumulation")
    )
    parser.add_argument("--start-date", default="2025-07-01")
    parser.add_argument("--end-date")
    parser.add_argument("--symbols")
    parser.add_argument("--excluded-symbols", default=",".join(sorted(DEFAULT_EXCLUDED_SYMBOLS)))
    parser.add_argument("--max-symbols", type=int, default=0)
    parser.add_argument("--short-window", type=int, default=5)
    parser.add_argument("--long-window", type=int, default=20)
    parser.add_argument("--baseline-window", type=int, default=60)
    parser.add_argument("--min-baseline-days", type=int, default=20)
    parser.add_argument("--min-short-positive-days", type=int, default=3)
    parser.add_argument("--min-short-net-buy", type=float, default=100_000.0)
    parser.add_argument("--min-short-purity", type=float, default=0.80)
    parser.add_argument("--min-short-buy-share", type=float, default=0.05)
    parser.add_argument("--min-long-positive-days", type=int, default=8)
    parser.add_argument("--min-long-net-buy", type=float, default=300_000.0)
    parser.add_argument("--min-long-purity", type=float, default=0.80)
    parser.add_argument("--min-long-buy-share", type=float, default=0.05)
    parser.add_argument("--episode-gap-sessions", type=int, default=10)
    parser.add_argument("--acceleration-ratio", type=float, default=1.5)
    parser.add_argument("--broad-broker-symbol-threshold", type=int, default=20)
    return parser.parse_args(argv)


def config_from_args(args: argparse.Namespace) -> PersistentAccumulationConfig:
    excluded = parse_symbols(args.excluded_symbols) or set()
    return PersistentAccumulationConfig(
        broker_dirs=tuple(Path(path).expanduser().resolve() for path in args.broker_dirs),
        broker_list_path=Path(args.broker_list).expanduser().resolve(),
        output_dir=Path(args.output_dir).expanduser().resolve(),
        start_date=parse_date(args.start_date),
        end_date=parse_date(args.end_date),
        symbols=parse_symbols(args.symbols),
        excluded_symbols=excluded,
        max_symbols=max(0, int(args.max_symbols)),
        short_window=max(2, int(args.short_window)),
        long_window=max(int(args.short_window), int(args.long_window)),
        baseline_window=max(1, int(args.baseline_window)),
        min_baseline_days=max(1, int(args.min_baseline_days)),
        min_short_positive_days=max(1, int(args.min_short_positive_days)),
        min_short_net_buy=float(args.min_short_net_buy),
        min_short_purity=float(args.min_short_purity),
        min_short_buy_share=float(args.min_short_buy_share),
        min_long_positive_days=max(1, int(args.min_long_positive_days)),
        min_long_net_buy=float(args.min_long_net_buy),
        min_long_purity=float(args.min_long_purity),
        min_long_buy_share=float(args.min_long_buy_share),
        episode_gap_sessions=max(1, int(args.episode_gap_sessions)),
        acceleration_ratio=float(args.acceleration_ratio),
        broad_broker_symbol_threshold=max(1, int(args.broad_broker_symbol_threshold)),
    )


def main(argv: Optional[list[str]] = None) -> int:
    cfg = config_from_args(parse_args(argv))
    episodes, _daily = run_detection(cfg)
    print(f"Detected {len(episodes)} episodes. Outputs written to {cfg.output_dir}")
    if not episodes.empty:
        print(episodes.head(20).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
