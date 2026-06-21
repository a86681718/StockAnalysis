"""Detect stock accumulation distributed across several local broker branches."""

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
)
from stockanalysis.analysis.single_branch_persistent_accumulation import (
    DEFAULT_EXCLUDED_SYMBOLS,
    load_symbol_stock_daily,
    select_stock_paths,
)
from stockanalysis.config import ensure_dir, resolve_data, resolve_output


@dataclass(frozen=True)
class DistributedAccumulationConfig:
    broker_dirs: tuple[Path, ...]
    broker_list_path: Path
    broad_broker_source: Path
    output_dir: Path
    start_date: Optional[pd.Timestamp]
    end_date: Optional[pd.Timestamp]
    symbols: Optional[set[str]]
    excluded_symbols: set[str]
    max_symbols: int
    short_window: int
    baseline_window: int
    min_baseline_days: int
    min_member_positive_days: int
    min_member_net_buy: float
    min_member_purity: float
    min_member_buy_share: float
    min_participation_shift: float
    min_activity_multiple: float
    min_branches: int
    min_parent_brokers: int
    min_cluster_net_buy: float
    min_cluster_purity: float
    min_cluster_buy_share: float
    max_largest_branch_share: float
    min_residual_net_buy: float
    min_residual_buy_share: float
    episode_gap_sessions: int
    min_member_overlap_ratio: float


def parent_broker_name(branch_name: object) -> str:
    return str(branch_name or "").replace("－", "-").split("-", 1)[0].strip()


def load_broad_brokers(path: Path) -> set[str]:
    if not path.exists():
        return set()
    frame = pd.read_parquet(path, columns=["broker", "broad_broker_flag"])
    return set(frame.loc[frame["broad_broker_flag"].fillna(False), "broker"].astype(str).unique())


def _rolling(grouped: pd.core.groupby.SeriesGroupBy, window: int, operation: str) -> pd.Series:
    rolling = grouped.rolling(window, min_periods=window)
    result = rolling.sum() if operation == "sum" else rolling.mean()
    return result.reset_index(level=0, drop=True)


def build_member_features(
    daily: pd.DataFrame,
    cfg: DistributedAccumulationConfig,
    excluded_brokers: set[str],
) -> pd.DataFrame:
    if daily.empty:
        return pd.DataFrame()
    dates = pd.DatetimeIndex(daily["date"].dropna().sort_values().unique())
    eligible = daily[daily["is_local_branch"] & ~daily["broker"].isin(excluded_brokers)].copy()
    if eligible.empty or len(dates) < cfg.short_window:
        return pd.DataFrame()

    brokers = sorted(eligible["broker"].unique())
    names = eligible.drop_duplicates("broker").set_index("broker")["broker_name"].to_dict()
    full_index = pd.MultiIndex.from_product([brokers, dates], names=["broker", "date"])
    frame = (
        eligible.set_index(["broker", "date"])[["buy_volume", "sell_volume", "net_buy"]]
        .reindex(full_index)
        .fillna(0.0)
        .reset_index()
    )
    frame.insert(0, "symbol", str(daily["symbol"].iloc[0]))
    frame["broker_name"] = frame["broker"].map(names).fillna("")
    frame["parent_broker"] = frame["broker_name"].map(parent_broker_name)
    frame["positive_day"] = (frame["net_buy"] > 0).astype(float)
    frame["buy_day"] = (frame["buy_volume"] > 0).astype(float)
    frame = frame.sort_values(["broker", "date"]).reset_index(drop=True)
    grouped = frame.groupby("broker", group_keys=False)

    frame["member_window_buy"] = _rolling(grouped["buy_volume"], cfg.short_window, "sum")
    frame["member_window_sell"] = _rolling(grouped["sell_volume"], cfg.short_window, "sum")
    frame["member_window_net"] = _rolling(grouped["net_buy"], cfg.short_window, "sum")
    frame["member_positive_days"] = _rolling(grouped["positive_day"], cfg.short_window, "sum")
    frame["member_buy_days"] = _rolling(grouped["buy_day"], cfg.short_window, "sum")
    frame["member_purity"] = frame["member_window_net"] / frame["member_window_buy"].replace(0, np.nan)

    stock_buy = daily.groupby("date")["buy_volume"].sum().reindex(dates, fill_value=0.0)
    stock_buy_window = stock_buy.rolling(cfg.short_window, min_periods=cfg.short_window).sum()
    frame["stock_window_buy"] = frame["date"].map(stock_buy_window)
    frame["member_buy_share"] = frame["member_window_buy"] / frame["stock_window_buy"].replace(0, np.nan)

    shifted_buy_day = grouped["buy_day"].shift(cfg.short_window)
    shifted_buy_volume = grouped["buy_volume"].shift(cfg.short_window)
    baseline = frame.assign(
        baseline_buy_day=shifted_buy_day,
        baseline_buy_volume=shifted_buy_volume,
    ).groupby("broker", group_keys=False)
    frame["baseline_buy_day_rate"] = baseline["baseline_buy_day"].rolling(
        cfg.baseline_window, min_periods=cfg.min_baseline_days
    ).mean().reset_index(level=0, drop=True)
    frame["baseline_mean_buy"] = baseline["baseline_buy_volume"].rolling(
        cfg.baseline_window, min_periods=cfg.min_baseline_days
    ).mean().reset_index(level=0, drop=True)
    frame["participation_shift"] = frame["member_buy_days"] / cfg.short_window - frame["baseline_buy_day_rate"]
    frame["activity_multiple"] = frame["member_window_buy"] / (
        frame["baseline_mean_buy"] * cfg.short_window
    ).replace(0, np.nan)
    novelty = (frame["participation_shift"] >= cfg.min_participation_shift) | (
        frame["activity_multiple"] >= cfg.min_activity_multiple
    )
    frame["member_qualifies"] = (
        frame["baseline_buy_day_rate"].notna()
        & (frame["member_positive_days"] >= cfg.min_member_positive_days)
        & (frame["member_window_net"] >= cfg.min_member_net_buy)
        & (frame["member_purity"] >= cfg.min_member_purity)
        & (frame["member_buy_share"] >= cfg.min_member_buy_share)
        & novelty
    )
    return frame


def build_cluster_daily(member_features: pd.DataFrame, cfg: DistributedAccumulationConfig) -> pd.DataFrame:
    if member_features.empty or "member_qualifies" not in member_features.columns:
        return pd.DataFrame()
    members = member_features[member_features["member_qualifies"]].copy()
    if members.empty:
        return pd.DataFrame()

    rows: list[dict[str, object]] = []
    for date, group in members.groupby("date", sort=True):
        positive_nets = group["member_window_net"].clip(lower=0.0)
        largest_index = positive_nets.idxmax()
        largest = group.loc[largest_index]
        cluster_buy = float(group["member_window_buy"].sum())
        cluster_sell = float(group["member_window_sell"].sum())
        cluster_net = cluster_buy - cluster_sell
        positive_total = float(positive_nets.sum())
        largest_positive = float(largest["member_window_net"])
        stock_window_buy = float(group["stock_window_buy"].iloc[0])
        branch_count = int(group["broker"].nunique())
        parent_count = int(group["parent_broker"].nunique())
        cluster_purity = cluster_net / cluster_buy if cluster_buy > 0 else np.nan
        cluster_share = cluster_buy / stock_window_buy if stock_window_buy > 0 else np.nan
        dominance = largest_positive / positive_total if positive_total > 0 else np.nan
        residual_net = cluster_net - largest_positive
        residual_buy = cluster_buy - float(largest["member_window_buy"])
        residual_share = residual_buy / stock_window_buy if stock_window_buy > 0 else np.nan
        qualifies = (
            branch_count >= cfg.min_branches
            and parent_count >= cfg.min_parent_brokers
            and cluster_net >= cfg.min_cluster_net_buy
            and cluster_purity >= cfg.min_cluster_purity
            and cluster_share >= cfg.min_cluster_buy_share
            and dominance <= cfg.max_largest_branch_share
            and residual_net >= cfg.min_residual_net_buy
            and residual_share >= cfg.min_residual_buy_share
        )
        rows.append(
            {
                "symbol": group["symbol"].iloc[0],
                "date": date,
                "qualifies": qualifies,
                "branch_count": branch_count,
                "parent_broker_count": parent_count,
                "cluster_buy": cluster_buy,
                "cluster_sell": cluster_sell,
                "cluster_net_buy": cluster_net,
                "cluster_purity": cluster_purity,
                "cluster_buy_share": cluster_share,
                "largest_branch": largest["broker"],
                "largest_branch_name": largest["broker_name"],
                "largest_branch_net_share": dominance,
                "residual_net_buy": residual_net,
                "residual_buy_share": residual_share,
                "member_brokers": ",".join(sorted(group["broker"].unique())),
                "member_names": ",".join(sorted(group["broker_name"].unique())),
                "parent_brokers": ",".join(sorted(group["parent_broker"].unique())),
            }
        )
    return pd.DataFrame(rows)


def build_cluster_episodes(cluster_daily: pd.DataFrame, all_dates: pd.DatetimeIndex, cfg: DistributedAccumulationConfig) -> pd.DataFrame:
    triggers = cluster_daily[cluster_daily["qualifies"]].sort_values("date").reset_index(drop=True)
    if triggers.empty:
        return pd.DataFrame()
    date_positions = {date: index for index, date in enumerate(all_dates)}
    groups: list[list[int]] = [[0]]
    for index in range(1, len(triggers)):
        previous_row = triggers.iloc[groups[-1][-1]]
        current_row = triggers.iloc[index]
        previous = date_positions[pd.Timestamp(previous_row["date"])]
        current = date_positions[pd.Timestamp(current_row["date"])]
        previous_members = {value for value in str(previous_row["member_brokers"]).split(",") if value}
        current_members = {value for value in str(current_row["member_brokers"]).split(",") if value}
        smaller_count = min(len(previous_members), len(current_members))
        overlap = len(previous_members & current_members) / smaller_count if smaller_count else 0.0
        if current - previous <= cfg.episode_gap_sessions and overlap >= cfg.min_member_overlap_ratio:
            groups[-1].append(index)
        else:
            groups.append([index])

    rows: list[dict[str, object]] = []
    latest_position = len(all_dates) - 1
    for indices in groups:
        group = triggers.iloc[indices]
        last_position = date_positions[pd.Timestamp(group.iloc[-1]["date"])]
        ongoing = latest_position - last_position <= cfg.episode_gap_sessions
        latest = group.iloc[-1]
        member_names = sorted({name for value in group["member_names"] for name in str(value).split(",") if name})
        parent_names = sorted({name for value in group["parent_brokers"] for name in str(value).split(",") if name})
        rows.append(
            {
                "symbol": group["symbol"].iloc[0],
                "episode_start": group.iloc[0]["date"],
                "last_qualifying_date": latest["date"],
                "ongoing": ongoing,
                "qualifying_dates": len(group),
                "max_branch_count": int(group["branch_count"].max()),
                "max_parent_broker_count": int(group["parent_broker_count"].max()),
                "max_cluster_net_buy": float(group["cluster_net_buy"].max()),
                "max_cluster_buy_share": float(group["cluster_buy_share"].max()),
                "min_largest_branch_net_share": float(group["largest_branch_net_share"].min()),
                "max_residual_net_buy": float(group["residual_net_buy"].max()),
                "latest_cluster_net_buy": latest["cluster_net_buy"],
                "latest_cluster_purity": latest["cluster_purity"],
                "latest_cluster_buy_share": latest["cluster_buy_share"],
                "latest_largest_branch_net_share": latest["largest_branch_net_share"],
                "latest_residual_net_buy": latest["residual_net_buy"],
                "latest_residual_buy_share": latest["residual_buy_share"],
                "member_names": ",".join(member_names),
                "parent_brokers": ",".join(parent_names),
            }
        )
    return pd.DataFrame(rows)


def classify_and_rank(episodes: pd.DataFrame) -> pd.DataFrame:
    if episodes.empty:
        return episodes.copy()
    out = episodes.copy()
    out["confidence"] = np.select(
        [out["qualifying_dates"] >= 20, out["qualifying_dates"] >= 8, out["qualifying_dates"] >= 3],
        ["established", "confirmed", "developing"],
        default="preliminary",
    )
    out["review_eligible"] = out["ongoing"] & out["confidence"].ne("preliminary")
    confidence_order = {"established": 0, "confirmed": 1, "developing": 2, "preliminary": 3}
    out["confidence_order"] = out["confidence"].map(confidence_order)
    out = out.sort_values(
        ["review_eligible", "ongoing", "confidence_order", "qualifying_dates", "max_cluster_buy_share"],
        ascending=[False, False, True, False, False],
    ).drop(columns="confidence_order").reset_index(drop=True)
    out.insert(0, "rank", np.arange(1, len(out) + 1))
    return out


def write_outputs(
    episodes: pd.DataFrame,
    daily_triggers: pd.DataFrame,
    cfg: DistributedAccumulationConfig,
    scanned_symbols: int,
    excluded_brokers: set[str],
) -> None:
    out_dir = ensure_dir(cfg.output_dir)
    episodes.to_csv(out_dir / "distributed_branch_episodes.csv", index=False, encoding="utf-8-sig")
    episodes.to_parquet(out_dir / "distributed_branch_episodes.parquet", index=False)
    daily_triggers.to_parquet(out_dir / "distributed_branch_daily_triggers.parquet", index=False)
    summary = {
        "scope": "distributed multi-branch stock accumulation; detection only",
        "scanned_symbols": scanned_symbols,
        "episode_count": len(episodes),
        "ongoing_episode_count": int(episodes["ongoing"].sum()) if not episodes.empty else 0,
        "review_eligible_count": int(episodes["review_eligible"].sum()) if not episodes.empty else 0,
        "daily_trigger_count": len(daily_triggers),
        "excluded_symbols": sorted(cfg.excluded_symbols),
        "excluded_broad_brokers": sorted(excluded_brokers),
        "config": {
            key: value
            for key, value in asdict(cfg).items()
            if key not in {"broker_dirs", "broker_list_path", "broad_broker_source", "output_dir", "symbols", "excluded_symbols"}
        },
    }
    for key, value in list(summary["config"].items()):
        if isinstance(value, pd.Timestamp):
            summary["config"][key] = value.strftime("%Y-%m-%d")
    (out_dir / "distributed_branch_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    lines = [
        "# Distributed Multi-Branch Accumulation",
        "",
        "- scope: stock-side detection only; no prices, returns, geography, warrants, or ownership inference",
        f"- scanned symbols: `{scanned_symbols}`",
        f"- episodes: `{len(episodes)}`; review eligible: `{summary['review_eligible_count']}`",
        f"- excluded broad brokers from Direction 1: `{','.join(sorted(excluded_brokers)) or 'none'}`",
        "",
        "## Review Queue",
        "",
    ]
    review = episodes[episodes["review_eligible"]].head(50) if not episodes.empty else episodes
    if review.empty:
        lines.append("No review-eligible distributed accumulation episodes.")
    else:
        for row in review.itertuples(index=False):
            lines.append(
                f"- `{row.rank}` `{row.symbol}` `{row.confidence}` start=`{pd.Timestamp(row.episode_start).date()}` "
                f"branches=`{row.max_branch_count}` parents=`{row.max_parent_broker_count}` "
                f"net=`{row.latest_cluster_net_buy:.0f}` share=`{row.latest_cluster_buy_share:.2%}` "
                f"dominance=`{row.latest_largest_branch_net_share:.2%}` members=`{row.member_names}`"
            )
    (out_dir / "distributed_branch_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_detection(cfg: DistributedAccumulationConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    broker_lookup = load_broker_lookup(cfg.broker_list_path)
    parquet_index = build_parquet_index(cfg.broker_dirs)
    selected = select_stock_paths(parquet_index, cfg.symbols, cfg.excluded_symbols, cfg.max_symbols)
    excluded_brokers = load_broad_brokers(cfg.broad_broker_source)
    episode_frames: list[pd.DataFrame] = []
    trigger_frames: list[pd.DataFrame] = []
    for index, (symbol, paths) in enumerate(selected, start=1):
        daily = load_symbol_stock_daily(symbol, paths, broker_lookup, cfg.start_date, cfg.end_date)
        members = build_member_features(daily, cfg, excluded_brokers)
        clusters = build_cluster_daily(members, cfg)
        if not clusters.empty:
            triggers = clusters[clusters["qualifies"]].copy()
            if not triggers.empty:
                trigger_frames.append(triggers)
                dates = pd.DatetimeIndex(daily["date"].dropna().sort_values().unique())
                episodes = build_cluster_episodes(clusters, dates, cfg)
                if not episodes.empty:
                    episode_frames.append(episodes)
        if index % 250 == 0:
            print(f"processed {index}/{len(selected)} stock symbols", flush=True)
    episodes = pd.concat(episode_frames, ignore_index=True) if episode_frames else pd.DataFrame()
    triggers = pd.concat(trigger_frames, ignore_index=True) if trigger_frames else pd.DataFrame()
    episodes = classify_and_rank(episodes)
    write_outputs(episodes, triggers, cfg, len(selected), excluded_brokers)
    return episodes, triggers


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Detect distributed multi-branch stock accumulation.")
    parser.add_argument("--broker-dirs", nargs="+", type=Path, default=[resolve_data("bs_report", "parquet_twse"), resolve_data("bs_report", "parquet_tpex")])
    parser.add_argument("--broker-list", type=Path, default=resolve_data("broker_list.csv"))
    parser.add_argument("--broad-broker-source", type=Path, default=resolve_output("analysis", "single_branch_persistent_accumulation", "single_branch_persistent_episodes.parquet"))
    parser.add_argument("--output-dir", type=Path, default=resolve_output("analysis", "distributed_branch_accumulation"))
    parser.add_argument("--start-date", default="2025-07-01")
    parser.add_argument("--end-date")
    parser.add_argument("--symbols")
    parser.add_argument("--excluded-symbols", default=",".join(sorted(DEFAULT_EXCLUDED_SYMBOLS)))
    parser.add_argument("--max-symbols", type=int, default=0)
    parser.add_argument("--short-window", type=int, default=5)
    parser.add_argument("--baseline-window", type=int, default=60)
    parser.add_argument("--min-baseline-days", type=int, default=20)
    parser.add_argument("--min-member-positive-days", type=int, default=2)
    parser.add_argument("--min-member-net-buy", type=float, default=30_000.0)
    parser.add_argument("--min-member-purity", type=float, default=0.75)
    parser.add_argument("--min-member-buy-share", type=float, default=0.01)
    parser.add_argument("--min-participation-shift", type=float, default=0.15)
    parser.add_argument("--min-activity-multiple", type=float, default=3.0)
    parser.add_argument("--min-branches", type=int, default=3)
    parser.add_argument("--min-parent-brokers", type=int, default=2)
    parser.add_argument("--min-cluster-net-buy", type=float, default=300_000.0)
    parser.add_argument("--min-cluster-purity", type=float, default=0.80)
    parser.add_argument("--min-cluster-buy-share", type=float, default=0.10)
    parser.add_argument("--max-largest-branch-share", type=float, default=0.60)
    parser.add_argument("--min-residual-net-buy", type=float, default=100_000.0)
    parser.add_argument("--min-residual-buy-share", type=float, default=0.04)
    parser.add_argument("--episode-gap-sessions", type=int, default=10)
    parser.add_argument("--min-member-overlap-ratio", type=float, default=0.50)
    return parser.parse_args(argv)


def config_from_args(args: argparse.Namespace) -> DistributedAccumulationConfig:
    return DistributedAccumulationConfig(
        broker_dirs=tuple(Path(path).expanduser().resolve() for path in args.broker_dirs),
        broker_list_path=Path(args.broker_list).expanduser().resolve(),
        broad_broker_source=Path(args.broad_broker_source).expanduser().resolve(),
        output_dir=Path(args.output_dir).expanduser().resolve(),
        start_date=parse_date(args.start_date),
        end_date=parse_date(args.end_date),
        symbols=parse_symbols(args.symbols),
        excluded_symbols=parse_symbols(args.excluded_symbols) or set(),
        max_symbols=max(0, int(args.max_symbols)),
        short_window=max(2, int(args.short_window)),
        baseline_window=max(1, int(args.baseline_window)),
        min_baseline_days=max(1, int(args.min_baseline_days)),
        min_member_positive_days=max(1, int(args.min_member_positive_days)),
        min_member_net_buy=float(args.min_member_net_buy),
        min_member_purity=float(args.min_member_purity),
        min_member_buy_share=float(args.min_member_buy_share),
        min_participation_shift=float(args.min_participation_shift),
        min_activity_multiple=float(args.min_activity_multiple),
        min_branches=max(2, int(args.min_branches)),
        min_parent_brokers=max(2, int(args.min_parent_brokers)),
        min_cluster_net_buy=float(args.min_cluster_net_buy),
        min_cluster_purity=float(args.min_cluster_purity),
        min_cluster_buy_share=float(args.min_cluster_buy_share),
        max_largest_branch_share=float(args.max_largest_branch_share),
        min_residual_net_buy=float(args.min_residual_net_buy),
        min_residual_buy_share=float(args.min_residual_buy_share),
        episode_gap_sessions=max(1, int(args.episode_gap_sessions)),
        min_member_overlap_ratio=float(args.min_member_overlap_ratio),
    )


def main(argv: Optional[list[str]] = None) -> int:
    cfg = config_from_args(parse_args(argv))
    episodes, _triggers = run_detection(cfg)
    print(f"Detected {len(episodes)} distributed episodes. Outputs written to {cfg.output_dir}")
    if not episodes.empty:
        print(episodes.head(20).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
