from __future__ import annotations

import argparse
import logging
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from tqdm import tqdm

BROKER_DATA_DIR = Path("/Users/fang/Desktop/bs_report/parquet_twse")
WARRANT_LIST_PATH = Path("/Users/fang/StockAnalysis/data/warrant_list_dedup.csv")
BROKER_LIST_PATH = Path("/Users/fang/StockAnalysis/data/broker_list.csv")
OHLC_DIR = Path("/Users/fang/StockAnalysis/data/ohlc")
OUTPUT_DIR = Path("/Users/fang/StockAnalysis/analysis_outputs/warrant_concentration")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


@dataclass
class AnalysisResult:
    underlying_code: str
    underlying_name: str
    top_brokers: pd.DataFrame
    daily_stats: pd.DataFrame
    short_hhi_latest: float
    long_hhi_latest: float
    short_hhi_mean: float
    long_hhi_mean: float


def load_warrant_meta() -> pd.DataFrame:
    if not WARRANT_LIST_PATH.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(WARRANT_LIST_PATH, dtype=str, encoding="utf-8-sig").fillna("")
    except Exception:
        logging.warning("Failed to read %s", WARRANT_LIST_PATH)
        return pd.DataFrame()


WARRANT_META = load_warrant_meta()


def load_broker_lookup() -> dict[str, str]:
    if not BROKER_LIST_PATH.exists():
        return {}
    try:
        df = pd.read_csv(BROKER_LIST_PATH, dtype=str, encoding="utf-8-sig").fillna("")
    except Exception:
        logging.warning("Failed to read %s", BROKER_LIST_PATH)
        return {}
    code_col = None
    name_col = None
    for candidate in ("證券商代號", "代號", "broker_id", "code"):
        if candidate in df.columns:
            code_col = candidate
            break
    for candidate in ("證券商名稱", "名稱", "broker_name", "name"):
        if candidate in df.columns:
            name_col = candidate
            break
    if not code_col or not name_col:
        return {}
    df[code_col] = df[code_col].astype(str).str.strip()
    df[name_col] = df[name_col].astype(str).str.strip()
    return dict(zip(df[code_col], df[name_col]))


BROKER_LOOKUP = load_broker_lookup()


def resolve_underlying(code: str) -> tuple[str, str]:
    if WARRANT_META.empty:
        return code, ""
    hit = WARRANT_META.loc[WARRANT_META["權證代號"].str.strip() == code]
    if hit.empty:
        return code, ""
    row = hit.iloc[0]
    return row.get("標的代號", code), row.get("標的名稱", "")


def load_trades(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    df = df.rename(columns=str.strip)
    expected_cols = {"日期", "券商", "買進股數", "賣出股數"}
    missing = expected_cols.difference(df.columns)
    if missing:
        raise ValueError(f"{path} 缺少欄位 {sorted(missing)}")
    df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
    df = df.dropna(subset=["日期"])
    numeric_cols = ["買進股數", "賣出股數", "買進金額", "賣出金額", "價格"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "買進金額" not in df.columns and "價格" in df.columns:
        df["買進金額"] = df["價格"] * df.get("買進股數", 0)
    if "賣出金額" not in df.columns and "價格" in df.columns:
        df["賣出金額"] = df["價格"] * df.get("賣出股數", 0)
    df["券商"] = df["券商"].astype(str).str.strip()
    return df


def aggregate_per_broker(df: pd.DataFrame) -> pd.DataFrame:
    per_broker = (
        df.groupby(["日期", "券商"], as_index=False)
        .agg(
            buy_shares=("買進股數", "sum"),
            sell_shares=("賣出股數", "sum"),
            buy_amount=("買進金額", "sum"),
            sell_amount=("賣出金額", "sum"),
        )
    )
    per_broker["buy_shares"] = per_broker["buy_shares"].fillna(0.0)
    per_broker["sell_shares"] = per_broker["sell_shares"].fillna(0.0)
    per_broker["buy_amount"] = per_broker["buy_amount"].fillna(0.0)
    per_broker["sell_amount"] = per_broker["sell_amount"].fillna(0.0)
    per_broker["net_shares"] = per_broker["buy_shares"] - per_broker["sell_shares"]
    per_broker["net_amount"] = per_broker["buy_amount"] - per_broker["sell_amount"]
    return per_broker


def compute_daily(per_broker: pd.DataFrame) -> pd.DataFrame:
    daily = (
        per_broker.groupby("日期")[[
            "buy_shares",
            "sell_shares",
            "net_shares",
            "buy_amount",
            "sell_amount",
            "net_amount",
        ]]
        .sum()
    )
    daily.sort_index(inplace=True)
    hhi_rows: list[tuple[pd.Timestamp, float, float]] = []
    for day, chunk in per_broker.groupby("日期"):
        positives = chunk.loc[chunk["net_shares"] > 0, "net_shares"]
        total = positives.sum()
        if total > 0:
            shares = positives / total
            hhi = np.square(shares).sum()
            top = shares.max()
        else:
            hhi = np.nan
            top = np.nan
        hhi_rows.append((day, hhi, top))
    hhi_df = pd.DataFrame(hhi_rows, columns=["日期", "hhi", "top_share"]).set_index("日期")
    daily = daily.join(hhi_df, how="left")
    return daily


def load_price_series(code: str) -> tuple[pd.DataFrame, str]:
    frames = []
    stock_name = ""
    if not OHLC_DIR.exists():
        return pd.DataFrame(columns=["date", "close", "volume"]), stock_name
    for csv_file in OHLC_DIR.glob("*.csv"):
        try:
            df = pd.read_csv(csv_file, dtype=str)
        except Exception:
            continue
        if "證券代號" not in df.columns or "收盤價" not in df.columns:
            continue
        hits = df.loc[df["證券代號"].str.strip() == code]
        if hits.empty:
            continue
        date_token = csv_file.stem.split("-")[-1]
        try:
            trade_date = pd.to_datetime(date_token, format="%Y%m%d")
        except ValueError:
            continue
        price_raw = hits.iloc[0]["收盤價"].replace(",", "").strip()
        vol_raw = hits.iloc[0].get("成交股數", "0").replace(",", "").strip()
        try:
            price = float(price_raw)
        except ValueError:
            price = np.nan
        try:
            volume = float(vol_raw)
        except ValueError:
            volume = np.nan
        if not stock_name:
            stock_name = hits.iloc[0].get("證券名稱", "")
        frames.append((trade_date, price, volume))
    if not frames:
        return pd.DataFrame(columns=["date", "close", "volume"]), stock_name
    price_df = pd.DataFrame(frames, columns=["date", "close", "volume"]).dropna(subset=["date"]).sort_values("date")
    return price_df, stock_name


def summarize_brokers(per_broker: pd.DataFrame) -> pd.DataFrame:
    agg = (
        per_broker.groupby("券商")
        .agg(
            buy_shares=("buy_shares", "sum"),
            sell_shares=("sell_shares", "sum"),
            net_shares=("net_shares", "sum"),
            buy_amount=("buy_amount", "sum"),
            sell_amount=("sell_amount", "sum"),
            net_amount=("net_amount", "sum"),
        )
        .sort_values("net_shares", ascending=False)
    )
    net_buy_mask = agg["net_shares"] > 0
    total_net_buy = agg.loc[net_buy_mask, "net_shares"].sum()
    if total_net_buy > 0:
        agg.loc[net_buy_mask, "net_share_ratio"] = agg.loc[net_buy_mask, "net_shares"] / total_net_buy
        agg.loc[~net_buy_mask, "net_share_ratio"] = np.nan
    else:
        agg["net_share_ratio"] = np.nan
    total_buy = agg["buy_shares"].sum()
    if total_buy != 0:
        agg["buy_share_ratio"] = agg["buy_shares"] / total_buy
    else:
        agg["buy_share_ratio"] = np.nan
    agg["net_share_ratio_pct"] = agg["net_share_ratio"] * 100
    return agg


def plot_underlying(
    underlying_code: str,
    underlying_name: str,
    daily: pd.DataFrame,
    per_broker: pd.DataFrame,
    top_brokers: pd.DataFrame,
    price_df: pd.DataFrame,
    output_dir: Path,
    short_window: int,
    long_window: int,
    short_threshold: float,
    long_threshold: float,
):
    n_brokers = min(len(top_brokers), 5)
    if n_brokers == 0:
        return

    total_panels = n_brokers + 2
    fig_height = 7 + n_brokers * 2
    fig, axes = plt.subplots(
        total_panels,
        1,
        figsize=(12, fig_height),
        sharex=True,
        gridspec_kw={"height_ratios": [3, 2] + [2] * n_brokers},
    )
    axes = np.atleast_1d(axes)

    dates = daily.index
    ax_price = axes[0]
    if not price_df.empty:
        price_window_df = price_df.loc[
            (price_df["date"] >= dates.min()) & (price_df["date"] <= dates.max())
        ]
        if price_window_df.empty:
            price_window_df = price_df
        ax_price.plot(price_window_df["date"], price_window_df["close"], color="#1f77b4", label="Close")
        vol_ax = ax_price.twinx()
        volume_series = (
            price_window_df.get("volume", pd.Series(index=price_window_df.index, dtype=float))
            .fillna(0)
            / 1_000_000
        )
        vol_ax.bar(price_window_df["date"], volume_series, color="lightgray", alpha=0.4, label="Volume (M)")
        vol_ax.set_ylabel("Volume (M)")
        vol_ax.legend(loc="upper right")
        ax_price.set_ylabel("Price")
        ax_price.legend(loc="upper left")
    else:
        ax_price.plot(dates, daily["net_shares"], color="#1f77b4", label="Net Shares")
        ax_price.set_ylabel("Net Shares")
        ax_price.legend(loc="upper left")

    ax_hhi = axes[1]
    ax_hhi.plot(dates, daily["hhi"], label="Daily HHI", color="#999999", alpha=0.6)
    short_col = f"hhi_{short_window}d"
    long_col = f"hhi_{long_window}d"
    if short_col in daily.columns:
        ax_hhi.plot(dates, daily[short_col], label=f"{short_window}日HHI", color="#2ca02c")
    if long_col in daily.columns:
        ax_hhi.plot(dates, daily[long_col], label=f"{long_window}日HHI", color="#d62728")
    ax_hhi.axhline(short_threshold, color="#2ca02c", linestyle="--", linewidth=1, label=f"{short_window}日門檻")
    ax_hhi.axhline(long_threshold, color="#d62728", linestyle=":", linewidth=1, label=f"{long_window}日門檻")
    ax_hhi.set_ylabel("HHI")
    ax_hhi.set_ylim(0, 1)
    ax_hhi.legend(loc="upper left")
    ax_hhi.grid(True, linestyle=":", linewidth=0.5, alpha=0.6)

    per_broker = per_broker.copy()
    per_broker["日期"] = pd.to_datetime(per_broker["日期"])
    for idx, (broker, row) in enumerate(top_brokers.head(n_brokers).iterrows(), start=2):
        ax = axes[idx]
        broker_code = str(broker).strip()
        broker_series = per_broker.loc[per_broker["券商"] == broker_code]
        broker_daily = broker_series.groupby("日期").agg(
            buy_shares=("buy_shares", "sum"),
            sell_shares=("sell_shares", "sum"),
        )
        broker_daily = broker_daily.reindex(dates, fill_value=0.0)
        ax.bar(dates, broker_daily["buy_shares"], color="#d62728")
        ax.bar(dates, -broker_daily["sell_shares"], color="#2ca02c")
        ax.axhline(0, color="#666666", linewidth=0.8)
        ratio_pct = row.get("net_share_ratio_pct", np.nan)
        raw_name = row.get("broker_name", "")
        broker_name = ""
        if pd.notna(raw_name):
            broker_name = str(raw_name).strip()
        if (not broker_name) and (broker_code in BROKER_LOOKUP):
            broker_name = BROKER_LOOKUP[broker_code]
        title_parts = [broker_code]
        if broker_name:
            title_parts.append(broker_name)
        if pd.notna(ratio_pct):
            title_parts.append(f"20天買超比率 {ratio_pct:.1f}%")
        ax.set_ylabel("Shares")
        ax.set_title(" ".join(title_parts), loc="left", fontsize=10)

    plt.xticks(rotation=45)
    title = underlying_code
    if underlying_name:
        title += f" ({underlying_name})"
    title += " – Warrant Concentration"
    fig.suptitle(title)
    plt.tight_layout()
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"{underlying_code}_warrant_concentration.png"
    plt.savefig(out_path, dpi=200)
    plt.close(fig)
    logging.info("Saved plot for %s to %s", underlying_code, out_path)


def analyze_underlying(
    underlying_code: str,
    paths: list[Path],
    underlying_name: str,
    short_window: int,
    long_window: int,
    short_threshold: float,
    long_threshold: float,
    output_dir: Path,
) -> Optional[AnalysisResult]:
    per_broker_frames: list[pd.DataFrame] = []
    for path in paths:
        try:
            trades = load_trades(path)
        except Exception as exc:
            logging.warning("Skip %s due to %s", path, exc)
            continue
        if trades.empty:
            continue
        per_broker_frames.append(aggregate_per_broker(trades))
    if not per_broker_frames:
        return None

    combined_per_broker = pd.concat(per_broker_frames, ignore_index=True)
    combined_per_broker["日期"] = pd.to_datetime(combined_per_broker["日期"])
    combined_per_broker = (
        combined_per_broker
        .groupby(["日期", "券商"], as_index=False)
        .agg(
            buy_shares=("buy_shares", "sum"),
            sell_shares=("sell_shares", "sum"),
            buy_amount=("buy_amount", "sum"),
            sell_amount=("sell_amount", "sum"),
            net_shares=("net_shares", "sum"),
            net_amount=("net_amount", "sum"),
        )
    )
    combined_daily = compute_daily(combined_per_broker)
    if combined_daily.empty:
        return None

    combined_daily.sort_index(inplace=True)
    combined_daily[f"hhi_{short_window}d"] = combined_daily["hhi"].rolling(short_window, min_periods=short_window).mean()
    combined_daily[f"hhi_{long_window}d"] = combined_daily["hhi"].rolling(long_window, min_periods=long_window).mean()

    end_date = combined_daily.index.max()
    short_cutoff = end_date - pd.Timedelta(days=short_window - 1)
    long_cutoff = end_date - pd.Timedelta(days=long_window - 1)
    short_window_daily = combined_daily.loc[combined_daily.index >= short_cutoff]
    long_window_daily = combined_daily.loc[combined_daily.index >= long_cutoff]

    short_latest = short_window_daily[f"hhi_{short_window}d"].iloc[-1] if f"hhi_{short_window}d" in short_window_daily.columns else np.nan
    long_latest = long_window_daily[f"hhi_{long_window}d"].iloc[-1] if f"hhi_{long_window}d" in long_window_daily.columns else np.nan

    passes_short = pd.notna(short_latest) and short_latest >= short_threshold
    passes_long = pd.notna(long_latest) and long_latest >= long_threshold
    if not (passes_short or passes_long):
        return None

    recent_per_broker = combined_per_broker.loc[combined_per_broker["日期"] >= short_cutoff]
    if recent_per_broker.empty:
        return None
    top_brokers = summarize_brokers(recent_per_broker)
    top_brokers = top_brokers.loc[top_brokers["net_shares"] > 0]
    if top_brokers.empty:
        return None
    if "broker_name" not in top_brokers.columns:
        top_brokers["broker_name"] = top_brokers.index.map(BROKER_LOOKUP)

    price_df, price_name = load_price_series(underlying_code)
    if not underlying_name and price_name:
        underlying_name = price_name

    plot_underlying(
        underlying_code=underlying_code,
        underlying_name=underlying_name,
        daily=combined_daily,
        per_broker=combined_per_broker,
        top_brokers=top_brokers,
        price_df=price_df,
        output_dir=output_dir,
        short_window=short_window,
        long_window=long_window,
        short_threshold=short_threshold,
        long_threshold=long_threshold,
    )

    short_mean = short_window_daily["hhi"].mean()
    long_mean = long_window_daily["hhi"].mean()

    summary = top_brokers.head(10).copy().reset_index()
    summary.rename(columns={"券商": "broker"}, inplace=True)
    summary["underlying_code"] = underlying_code
    summary["underlying_name"] = underlying_name
    summary["short_hhi_mean"] = short_mean
    summary["short_hhi_latest"] = short_latest
    summary["long_hhi_mean"] = long_mean
    summary["long_hhi_latest"] = long_latest

    return AnalysisResult(
        underlying_code=underlying_code,
        underlying_name=underlying_name,
        top_brokers=summary,
        daily_stats=combined_daily,
        short_hhi_latest=float(short_latest) if pd.notna(short_latest) else np.nan,
        long_hhi_latest=float(long_latest) if pd.notna(long_latest) else np.nan,
        short_hhi_mean=float(short_mean) if pd.notna(short_mean) else np.nan,
        long_hhi_mean=float(long_mean) if pd.notna(long_mean) else np.nan,
    )


def collect_underlying_files(data_root: Path) -> dict[str, dict]:
    mapping: dict[str, dict] = defaultdict(lambda: {"paths": [], "name": ""})
    for path in sorted(data_root.glob("*.parquet")):
        code = path.stem.strip()
        if len(code) <= 4:
            continue
        underlying_code, underlying_name = resolve_underlying(code)
        if underlying_code == code or not underlying_code:
            continue
        entry = mapping[underlying_code]
        entry["paths"].append(path)
        if underlying_name and not entry["name"]:
            entry["name"] = underlying_name
    return mapping


def parse_cli_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Detect warrant concentration by underlying")
    parser.add_argument("--data-root", default=BROKER_DATA_DIR, type=Path, help="Parquet directory")
    parser.add_argument("--short-window", type=int, default=20, help="Short rolling window (days)")
    parser.add_argument("--long-window", type=int, default=60, help="Long rolling window (days)")
    parser.add_argument("--short-hhi-threshold", type=float, default=0.25, help="HHI threshold for short window")
    parser.add_argument("--long-hhi-threshold", type=float, default=0.25, help="HHI threshold for long window")
    parser.add_argument("--limit", type=int, help="Limit number of underlyings to analyze")
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    parser.add_argument("--font-family", help="Matplotlib font family for Chinese labels")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_cli_args(argv)
    if not args.data_root.exists():
        logging.error("Data root %s not found", args.data_root)
        return 1

    global BROKER_DATA_DIR
    BROKER_DATA_DIR = args.data_root

    if args.font_family:
        plt.rcParams["font.sans-serif"] = [args.font_family]
        plt.rcParams["axes.unicode_minus"] = False

    underlying_map = collect_underlying_files(args.data_root)
    if not underlying_map:
        logging.info("No warrant underlyings found in %s", args.data_root)
        return 0

    items = sorted(underlying_map.items())
    if args.limit:
        items = items[: args.limit]

    results: list[AnalysisResult] = []
    for underlying_code, info in tqdm(items, desc="Scanning underlyings"):
        paths = info["paths"]
        if not paths:
            continue
        res = analyze_underlying(
            underlying_code=underlying_code,
            paths=paths,
            underlying_name=info["name"],
            short_window=args.short_window,
            long_window=args.long_window,
            short_threshold=args.short_hhi_threshold,
            long_threshold=args.long_hhi_threshold,
            output_dir=args.output_dir,
        )
        if res:
            results.append(res)

    if not results:
        logging.info("No underlyings met the concentration criteria.")
        return 0

    broker_rows = [res.top_brokers for res in results]
    all_brokers = pd.concat(broker_rows, ignore_index=True)
    summary_rows = []
    for res in results:
        summary_rows.append(
            {
                "underlying_code": res.underlying_code,
                "underlying_name": res.underlying_name,
                "short_hhi_mean": res.short_hhi_mean,
                "short_hhi_latest": res.short_hhi_latest,
                "long_hhi_mean": res.long_hhi_mean,
                "long_hhi_latest": res.long_hhi_latest,
            }
        )
    summary_df = pd.DataFrame(summary_rows)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    broker_csv = args.output_dir / "warrant_concentration_brokers.csv"
    summary_csv = args.output_dir / "warrant_concentration_summary.csv"
    all_brokers.to_csv(broker_csv, index=False, encoding="utf-8-sig")
    summary_df.to_csv(summary_csv, index=False, encoding="utf-8-sig")
    logging.info("Saved broker summary to %s", broker_csv)
    logging.info("Saved underlying summary to %s", summary_csv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
