"""Detect unusual branch accumulation in the company's registered city."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from stockanalysis.analysis.broker_branch_accumulation import build_parquet_index, load_broker_lookup, normalize_code, parse_date, parse_symbols
from stockanalysis.analysis.distributed_branch_accumulation import (
    DistributedAccumulationConfig,
    build_cluster_daily,
    build_cluster_episodes,
    build_member_features,
    classify_and_rank,
    load_broad_brokers,
)
from stockanalysis.analysis.single_branch_persistent_accumulation import DEFAULT_EXCLUDED_SYMBOLS, load_symbol_stock_daily, select_stock_paths
from stockanalysis.config import ensure_dir, resolve_data, resolve_output


CITY_ALIASES = {
    "臺北市": "台北市", "台北市": "台北市", "新北市": "新北市", "臺北縣": "新北市", "台北縣": "新北市",
    "桃園市": "桃園市", "桃園縣": "桃園市", "臺中市": "台中市", "台中市": "台中市", "臺中縣": "台中市", "台中縣": "台中市",
    "臺南市": "台南市", "台南市": "台南市", "臺南縣": "台南市", "台南縣": "台南市", "高雄市": "高雄市", "高雄縣": "高雄市",
    "基隆市": "基隆市", "新竹市": "新竹市", "新竹縣": "新竹縣", "苗栗縣": "苗栗縣", "彰化縣": "彰化縣",
    "南投縣": "南投縣", "雲林縣": "雲林縣", "嘉義市": "嘉義市", "嘉義縣": "嘉義縣", "屏東縣": "屏東縣",
    "宜蘭縣": "宜蘭縣", "花蓮縣": "花蓮縣", "臺東縣": "台東縣", "台東縣": "台東縣", "澎湖縣": "澎湖縣",
    "金門縣": "金門縣", "連江縣": "連江縣",
}
ENGLISH_CITY_ALIASES = {
    "taipei city": "台北市", "new taipei city": "新北市", "taoyuan city": "桃園市", "taichung city": "台中市",
    "tainan city": "台南市", "kaohsiung city": "高雄市", "keelung city": "基隆市", "hsinchu city": "新竹市",
    "hsinchu county": "新竹縣", "miaoli county": "苗栗縣", "changhua county": "彰化縣", "nantou county": "南投縣",
    "yunlin county": "雲林縣", "chiayi city": "嘉義市", "chiayi county": "嘉義縣", "pingtung county": "屏東縣",
    "yilan county": "宜蘭縣", "hualien county": "花蓮縣", "taitung county": "台東縣", "penghu county": "澎湖縣",
    "kinmen county": "金門縣", "lienchiang county": "連江縣",
}


@dataclass(frozen=True)
class RegionalAccumulationConfig(DistributedAccumulationConfig):
    company_profile_paths: tuple[Path, ...]
    min_city_buy_share_lift: float
    min_city_buy_share_delta: float


def extract_city(address: object) -> str:
    text = str(address or "").strip()
    for alias, normalized in sorted(CITY_ALIASES.items(), key=lambda item: len(item[0]), reverse=True):
        if alias in text:
            return normalized
    lowered = text.lower()
    for alias, normalized in sorted(ENGLISH_CITY_ALIASES.items(), key=lambda item: len(item[0]), reverse=True):
        if alias in lowered:
            return normalized
    return ""


def load_company_profiles(paths: tuple[Path, ...]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in paths:
        if not path.exists():
            continue
        raw = pd.read_json(path, dtype=False)
        if "公司代號" in raw.columns:
            frame = pd.DataFrame({
                "symbol": raw["公司代號"].map(normalize_code),
                "company_name": raw["公司簡稱"].fillna("").astype(str),
                "company_address": raw["住址"].fillna("").astype(str),
            })
        elif "SecuritiesCompanyCode" in raw.columns:
            frame = pd.DataFrame({
                "symbol": raw["SecuritiesCompanyCode"].map(normalize_code),
                "company_name": raw["CompanyAbbreviation"].fillna("").astype(str),
                "company_address": raw["Address"].fillna("").astype(str),
            })
        else:
            raise ValueError(f"Unsupported company profile schema: {path}")
        frames.append(frame)
    if not frames:
        return pd.DataFrame(columns=["symbol", "company_name", "company_address", "company_city"])
    out = pd.concat(frames, ignore_index=True).drop_duplicates("symbol", keep="first")
    out["company_city"] = out["company_address"].map(extract_city)
    return out[out["symbol"].ne("")].reset_index(drop=True)


def load_broker_cities(path: Path) -> dict[str, str]:
    raw = pd.read_csv(path, dtype=str, encoding="utf-8-sig").fillna("")
    code_col = next((column for column in ("證券商代號", "代號", "broker_id", "code") if column in raw.columns), None)
    address_col = next((column for column in ("地址", "address") if column in raw.columns), None)
    if not code_col or not address_col:
        return {}
    raw[code_col] = raw[code_col].map(normalize_code)
    raw["broker_city"] = raw[address_col].map(extract_city)
    return dict(zip(raw[code_col], raw["broker_city"]))


def filter_company_city_members(member_features: pd.DataFrame, company_city: str, broker_cities: dict[str, str]) -> pd.DataFrame:
    if member_features.empty or not company_city:
        return pd.DataFrame(columns=member_features.columns)
    out = member_features.copy()
    out["broker_city"] = out["broker"].map(broker_cities).fillna("")
    return out[out["broker_city"].eq(company_city)].copy()


def add_city_baseline_context(
    daily: pd.DataFrame,
    clusters: pd.DataFrame,
    company_city: str,
    broker_cities: dict[str, str],
    cfg: RegionalAccumulationConfig,
) -> pd.DataFrame:
    if clusters.empty:
        return clusters.copy()
    work = daily.copy()
    work["broker_city"] = work["broker"].map(broker_cities).fillna("")
    dates = pd.DatetimeIndex(work["date"].dropna().sort_values().unique())
    total_buy = work.groupby("date")["buy_volume"].sum().reindex(dates, fill_value=0.0)
    city_buy = work[work["is_local_branch"] & work["broker_city"].eq(company_city)].groupby("date")["buy_volume"].sum().reindex(dates, fill_value=0.0)
    window_total = total_buy.rolling(cfg.short_window, min_periods=cfg.short_window).sum()
    window_city = city_buy.rolling(cfg.short_window, min_periods=cfg.short_window).sum()
    city_share = window_city / window_total.replace(0, np.nan)
    baseline = city_share.shift(cfg.short_window).rolling(
        cfg.baseline_window, min_periods=cfg.min_baseline_days
    ).mean()
    context = pd.DataFrame({
        "date": dates,
        "city_window_buy_share": city_share.to_numpy(),
        "baseline_city_buy_share": baseline.to_numpy(),
    })
    out = clusters.merge(context, on="date", how="left")
    out["city_buy_share_lift"] = out["city_window_buy_share"] / out["baseline_city_buy_share"].replace(0, np.nan)
    out["city_buy_share_delta"] = out["city_window_buy_share"] - out["baseline_city_buy_share"]
    out["qualifies"] = (
        out["qualifies"]
        & out["baseline_city_buy_share"].notna()
        & (out["city_buy_share_lift"] >= cfg.min_city_buy_share_lift)
        & (out["city_buy_share_delta"] >= cfg.min_city_buy_share_delta)
    )
    return out


def write_outputs(episodes: pd.DataFrame, triggers: pd.DataFrame, cfg: RegionalAccumulationConfig, scanned: int, addressable: int) -> None:
    out_dir = ensure_dir(cfg.output_dir)
    episodes.to_csv(out_dir / "regional_branch_episodes.csv", index=False, encoding="utf-8-sig")
    episodes.to_parquet(out_dir / "regional_branch_episodes.parquet", index=False)
    triggers.to_parquet(out_dir / "regional_branch_daily_triggers.parquet", index=False)
    summary = {
        "scope": "same-registered-city broker accumulation; detection only",
        "scanned_symbols": scanned,
        "addressable_symbols": addressable,
        "episode_count": len(episodes),
        "ongoing_episode_count": int(episodes["ongoing"].sum()) if not episodes.empty else 0,
        "review_eligible_count": int(episodes["review_eligible"].sum()) if not episodes.empty else 0,
        "daily_trigger_count": len(triggers),
        "excluded_symbols": sorted(cfg.excluded_symbols),
        "company_profile_paths": [str(path) for path in cfg.company_profile_paths],
        "config": {
            key: value for key, value in asdict(cfg).items()
            if key not in {"broker_dirs", "broker_list_path", "broad_broker_source", "output_dir", "symbols", "excluded_symbols", "company_profile_paths"}
        },
    }
    for key, value in list(summary["config"].items()):
        if isinstance(value, pd.Timestamp):
            summary["config"][key] = value.strftime("%Y-%m-%d")
    (out_dir / "regional_branch_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    lines = [
        "# Regional Broker-Branch Accumulation",
        "",
        "- scope: company registered city only; no prices, returns, warrants, or ownership inference",
        "- limitation: registered address is not necessarily headquarters or factory location",
        f"- scanned symbols: `{scanned}`; addressable: `{addressable}`",
        f"- episodes: `{len(episodes)}`; review eligible: `{summary['review_eligible_count']}`",
        "",
        "## Review Queue",
        "",
    ]
    review = episodes[episodes["review_eligible"]].head(50) if not episodes.empty else episodes
    if review.empty:
        lines.append("No review-eligible regional accumulation episodes.")
    else:
        for row in review.itertuples(index=False):
            lines.append(
                f"- `{row.rank}` `{row.symbol}` `{row.company_name}` `{row.company_city}` `{row.confidence}` "
                f"start=`{pd.Timestamp(row.episode_start).date()}` branches=`{row.max_branch_count}` "
                f"parents=`{row.max_parent_broker_count}` net=`{row.latest_cluster_net_buy:.0f}` "
                f"share=`{row.latest_cluster_buy_share:.2%}` city_lift=`{row.latest_city_buy_share_lift:.2f}x` "
                f"members=`{row.member_names}`"
            )
    (out_dir / "regional_branch_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_detection(cfg: RegionalAccumulationConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    profiles = load_company_profiles(cfg.company_profile_paths)
    profile_map = profiles.set_index("symbol").to_dict("index") if not profiles.empty else {}
    broker_lookup = load_broker_lookup(cfg.broker_list_path)
    broker_cities = load_broker_cities(cfg.broker_list_path)
    broad_brokers = load_broad_brokers(cfg.broad_broker_source)
    selected = select_stock_paths(build_parquet_index(cfg.broker_dirs), cfg.symbols, cfg.excluded_symbols, cfg.max_symbols)
    episode_frames: list[pd.DataFrame] = []
    trigger_frames: list[pd.DataFrame] = []
    addressable = 0
    for index, (symbol, paths) in enumerate(selected, start=1):
        profile = profile_map.get(symbol, {})
        company_city = str(profile.get("company_city", ""))
        if not company_city:
            continue
        addressable += 1
        daily = load_symbol_stock_daily(symbol, paths, broker_lookup, cfg.start_date, cfg.end_date)
        members = build_member_features(daily, cfg, broad_brokers)
        regional_members = filter_company_city_members(members, company_city, broker_cities)
        clusters = build_cluster_daily(regional_members, cfg)
        if not clusters.empty:
            clusters = add_city_baseline_context(daily, clusters, company_city, broker_cities, cfg)
            clusters["company_name"] = profile.get("company_name", "")
            clusters["company_address"] = profile.get("company_address", "")
            clusters["company_city"] = company_city
            triggers = clusters[clusters["qualifies"]].copy()
            if not triggers.empty:
                trigger_frames.append(triggers)
                dates = pd.DatetimeIndex(daily["date"].dropna().sort_values().unique())
                episodes = build_cluster_episodes(clusters, dates, cfg)
                if not episodes.empty:
                    latest_context = triggers.set_index("date")[[
                        "city_window_buy_share", "baseline_city_buy_share", "city_buy_share_lift", "city_buy_share_delta"
                    ]]
                    episodes["latest_city_window_buy_share"] = episodes["last_qualifying_date"].map(latest_context["city_window_buy_share"])
                    episodes["latest_baseline_city_buy_share"] = episodes["last_qualifying_date"].map(latest_context["baseline_city_buy_share"])
                    episodes["latest_city_buy_share_lift"] = episodes["last_qualifying_date"].map(latest_context["city_buy_share_lift"])
                    episodes["latest_city_buy_share_delta"] = episodes["last_qualifying_date"].map(latest_context["city_buy_share_delta"])
                    episodes["company_name"] = profile.get("company_name", "")
                    episodes["company_address"] = profile.get("company_address", "")
                    episodes["company_city"] = company_city
                    episode_frames.append(episodes)
        if index % 250 == 0:
            print(f"processed {index}/{len(selected)} stock symbols", flush=True)
    episodes = pd.concat(episode_frames, ignore_index=True) if episode_frames else pd.DataFrame()
    triggers = pd.concat(trigger_frames, ignore_index=True) if trigger_frames else pd.DataFrame()
    episodes = classify_and_rank(episodes)
    write_outputs(episodes, triggers, cfg, len(selected), addressable)
    return episodes, triggers


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Detect broker accumulation in each company's registered city.")
    parser.add_argument("--broker-dirs", nargs="+", type=Path, default=[resolve_data("bs_report", "parquet_twse"), resolve_data("bs_report", "parquet_tpex")])
    parser.add_argument("--broker-list", type=Path, default=resolve_data("broker_list.csv"))
    parser.add_argument("--company-profiles", nargs="+", type=Path, default=[resolve_data("reference", "company_profiles_twse.json"), resolve_data("reference", "company_profiles_tpex.json")])
    parser.add_argument("--broad-broker-source", type=Path, default=resolve_output("analysis", "single_branch_persistent_accumulation", "single_branch_persistent_episodes.parquet"))
    parser.add_argument("--output-dir", type=Path, default=resolve_output("analysis", "regional_branch_accumulation"))
    parser.add_argument("--start-date", default="2025-07-01")
    parser.add_argument("--end-date")
    parser.add_argument("--symbols")
    parser.add_argument("--excluded-symbols", default=",".join(sorted(DEFAULT_EXCLUDED_SYMBOLS)))
    parser.add_argument("--max-symbols", type=int, default=0)
    parser.add_argument("--short-window", type=int, default=5)
    parser.add_argument("--baseline-window", type=int, default=60)
    parser.add_argument("--min-baseline-days", type=int, default=20)
    parser.add_argument("--min-member-positive-days", type=int, default=2)
    parser.add_argument("--min-member-net-buy", type=float, default=20_000.0)
    parser.add_argument("--min-member-purity", type=float, default=0.75)
    parser.add_argument("--min-member-buy-share", type=float, default=0.005)
    parser.add_argument("--min-participation-shift", type=float, default=0.15)
    parser.add_argument("--min-activity-multiple", type=float, default=3.0)
    parser.add_argument("--min-branches", type=int, default=2)
    parser.add_argument("--min-parent-brokers", type=int, default=2)
    parser.add_argument("--min-cluster-net-buy", type=float, default=150_000.0)
    parser.add_argument("--min-cluster-purity", type=float, default=0.80)
    parser.add_argument("--min-cluster-buy-share", type=float, default=0.05)
    parser.add_argument("--max-largest-branch-share", type=float, default=0.75)
    parser.add_argument("--min-residual-net-buy", type=float, default=50_000.0)
    parser.add_argument("--min-residual-buy-share", type=float, default=0.02)
    parser.add_argument("--episode-gap-sessions", type=int, default=10)
    parser.add_argument("--min-member-overlap-ratio", type=float, default=0.50)
    parser.add_argument("--min-city-buy-share-lift", type=float, default=1.50)
    parser.add_argument("--min-city-buy-share-delta", type=float, default=0.03)
    return parser.parse_args(argv)


def config_from_args(args: argparse.Namespace) -> RegionalAccumulationConfig:
    return RegionalAccumulationConfig(
        broker_dirs=tuple(Path(path).expanduser().resolve() for path in args.broker_dirs), broker_list_path=Path(args.broker_list).expanduser().resolve(),
        broad_broker_source=Path(args.broad_broker_source).expanduser().resolve(), output_dir=Path(args.output_dir).expanduser().resolve(),
        start_date=parse_date(args.start_date), end_date=parse_date(args.end_date), symbols=parse_symbols(args.symbols),
        excluded_symbols=parse_symbols(args.excluded_symbols) or set(), max_symbols=max(0, int(args.max_symbols)), short_window=max(2, int(args.short_window)),
        baseline_window=max(1, int(args.baseline_window)), min_baseline_days=max(1, int(args.min_baseline_days)),
        min_member_positive_days=max(1, int(args.min_member_positive_days)), min_member_net_buy=float(args.min_member_net_buy),
        min_member_purity=float(args.min_member_purity), min_member_buy_share=float(args.min_member_buy_share),
        min_participation_shift=float(args.min_participation_shift), min_activity_multiple=float(args.min_activity_multiple),
        min_branches=max(2, int(args.min_branches)), min_parent_brokers=max(2, int(args.min_parent_brokers)),
        min_cluster_net_buy=float(args.min_cluster_net_buy), min_cluster_purity=float(args.min_cluster_purity),
        min_cluster_buy_share=float(args.min_cluster_buy_share), max_largest_branch_share=float(args.max_largest_branch_share),
        min_residual_net_buy=float(args.min_residual_net_buy), min_residual_buy_share=float(args.min_residual_buy_share),
        episode_gap_sessions=max(1, int(args.episode_gap_sessions)), min_member_overlap_ratio=float(args.min_member_overlap_ratio),
        company_profile_paths=tuple(Path(path).expanduser().resolve() for path in args.company_profiles),
        min_city_buy_share_lift=float(args.min_city_buy_share_lift),
        min_city_buy_share_delta=float(args.min_city_buy_share_delta),
    )


def main(argv: Optional[list[str]] = None) -> int:
    cfg = config_from_args(parse_args(argv))
    episodes, _triggers = run_detection(cfg)
    print(f"Detected {len(episodes)} regional episodes. Outputs written to {cfg.output_dir}")
    if not episodes.empty:
        print(episodes.head(20).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
