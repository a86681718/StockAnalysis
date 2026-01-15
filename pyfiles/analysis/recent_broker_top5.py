"""Visualize recent top broker net-buy activity for a single stock.

The script loads per-stock broker parquet data, finds the last N trading days
with positive net-buy contributions, highlights the top brokers, and builds a
multi-panel chart plus a CSV summary for quick review.
"""

from __future__ import annotations

import argparse
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


# ---------- Data utilities ----------


SUFFIXES = (".parquet", ".pq", ".parq")
LOGGER = logging.getLogger("recent_broker_top5")


@dataclass
class Config:
    stock_code: str
    data_root: Path
    ohlc_root: Path
    broker_list: Optional[Path]
    output_dir: Path
    window_days: int
    price_window: int
    top_n: int
    font_family: Optional[str]


def parse_args(argv: Optional[list[str]] = None) -> Config:
    parser = argparse.ArgumentParser(
        description="Visualize a stock's recent top broker net-buy activity.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("stock_code", help="Target stock code, e.g. 2330")
    parser.add_argument(
        "--data-root",
        default="data/bs_data",
        help="Directory containing per-stock parquet files (or a single parquet file).",
    )
    parser.add_argument(
        "--ohlc-root",
        default="data/ohlc",
        help="Directory containing twse-YYYYMMDD.csv price files.",
    )
    parser.add_argument(
        "--broker-list",
        default="data/broker_list.csv",
        help="Optional CSV mapping 證券商代號 -> 證券商名稱.",
    )
    parser.add_argument(
        "--output-dir",
        default="pyfiles/analysis/tmp/recent_top_brokers",
        help="Directory for charts and CSV summaries.",
    )
    parser.add_argument("--window", type=int, default=20, help="Lookback window for net-buy ranking (days).")
    parser.add_argument("--price-window", type=int, default=60, help="Lookback for price/volume panel (days).")
    parser.add_argument("--top", type=int, default=5, help="How many brokers to visualize.")
    parser.add_argument("--font-family", help="Matplotlib font family for rendering Chinese labels.")
    parser.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, ...).")
    args = parser.parse_args(argv)

    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s [%(levelname)s] %(message)s")

    return Config(
        stock_code=args.stock_code.strip(),
        data_root=Path(args.data_root).expanduser().resolve(),
        ohlc_root=Path(args.ohlc_root).expanduser().resolve(),
        broker_list=Path(args.broker_list).expanduser().resolve() if args.broker_list else None,
        output_dir=Path(args.output_dir).expanduser().resolve(),
        window_days=max(1, args.window),
        price_window=max(args.window, args.price_window),
        top_n=max(1, args.top),
        font_family=args.font_family,
    )


def resolve_parquet_paths(data_root: Path, code: str) -> list[Path]:
    if data_root.is_file() and data_root.suffix.lower() in SUFFIXES:
        return [data_root]

    if not data_root.exists():
        return []

    matches: list[Path] = []
    for suffix in SUFFIXES:
        candidate = data_root / f"{code}{suffix}"
        if candidate.exists():
            matches.append(candidate)
    if matches:
        return sorted(set(matches))

    for suffix in SUFFIXES:
        matches.extend(
            path
            for path in data_root.glob(f"**/{code}{suffix}")
            if path.is_file()
        )
    if matches:
        return sorted(set(matches))

    def _normalize(text: str) -> str:
        return re.sub(r"[^A-Za-z0-9]", "", text)

    fallback: list[Path] = []
    for suffix in SUFFIXES:
        for path in data_root.glob(f"**/*{code}*{suffix}"):
            if not path.is_file():
                continue
            stem = path.stem
            parent = path.parent.name
            if stem == code or parent == code or _normalize(stem) == code or _normalize(parent) == code:
                fallback.append(path)
    return sorted(set(fallback))


def load_broker_lookup(path: Optional[Path]) -> dict[str, str]:
    if not path or not path.exists():
        return {}
    try:
        df = pd.read_csv(path, dtype=str)
    except Exception as exc:  # pragma: no cover - defensive
        LOGGER.warning("Failed to read broker list %s: %s", path, exc)
        return {}
    df = df.replace({np.nan: ""})
    code_col = next((col for col in ("證券商代號", "代號", "broker_id", "code") if col in df.columns), None)
    name_col = next((col for col in ("證券商名稱", "名稱", "broker_name", "name") if col in df.columns), None)
    if not code_col or not name_col:
        return {}
    df[code_col] = df[code_col].astype(str).str.strip()
    df[name_col] = df[name_col].astype(str).str.strip()
    return dict(zip(df[code_col], df[name_col]))


def normalize_broker_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip()
    required = {"券商", "日期", "買進股數", "賣出股數"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"缺少欄位: {', '.join(sorted(missing))}")

    for col in ["價格", "買進股數", "賣出股數", "買進金額", "賣出金額"]:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(",", "", regex=False)
                .str.replace(" ", "", regex=False)
                .replace({"": "0", "nan": "0", "None": "0"})
            )
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    df["券商"] = df["券商"].astype(str).str.strip()
    df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
    df = df.dropna(subset=["日期"])
    if "買進金額" not in df.columns and "價格" in df.columns:
        df["買進金額"] = df["價格"] * df["買進股數"]
    if "賣出金額" not in df.columns and "價格" in df.columns:
        df["賣出金額"] = df["價格"] * df["賣出股數"]
    return df


def load_trade_data(paths: Iterable[Path]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in paths:
        LOGGER.info("Reading %s", path)
        df = pd.read_parquet(path)
        frames.append(normalize_broker_df(df))
    if not frames:
        raise FileNotFoundError("No parquet data loaded")
    return pd.concat(frames, ignore_index=True)


def aggregate_per_broker(df: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        df.groupby(["日期", "券商"], as_index=False)
        .agg(
            buy_shares=("買進股數", "sum"),
            sell_shares=("賣出股數", "sum"),
            buy_amount=("買進金額", "sum"),
            sell_amount=("賣出金額", "sum"),
        )
        .sort_values(["日期", "券商"])
    )
    grouped["net_shares"] = grouped["buy_shares"] - grouped["sell_shares"]
    grouped["net_amount"] = grouped["buy_amount"] - grouped["sell_amount"]
    return grouped


def compute_daily_metrics(per_broker: pd.DataFrame) -> pd.DataFrame:
    daily = (
        per_broker.groupby("日期")[["buy_shares", "sell_shares", "net_shares", "buy_amount", "sell_amount", "net_amount"]]
        .sum()
        .sort_index()
    )
    hhi_rows = []
    for date, chunk in per_broker.groupby("日期"):
        positives = chunk.loc[chunk["net_shares"] > 0, "net_shares"]
        total = positives.sum()
        if total > 0:
            shares = positives / total
            hhi = float(np.square(shares).sum())
            top_share = float(shares.max())
        else:
            hhi = np.nan
            top_share = np.nan
        hhi_rows.append((date, hhi, top_share))
    hhi_df = pd.DataFrame(hhi_rows, columns=["日期", "hhi", "top_share"]).set_index("日期")
    daily = daily.join(hhi_df, how="left")
    return daily


def select_recent_dates(dates: pd.Index, window: int) -> pd.DatetimeIndex:
    dt_index = pd.DatetimeIndex(dates)
    if len(dt_index) <= window:
        return dt_index
    return dt_index[-window:]


def summarize_top_brokers(per_broker: pd.DataFrame, dates: pd.Index, top_n: int, broker_lookup: dict[str, str]) -> pd.DataFrame:
    window_df = per_broker.loc[per_broker["日期"].isin(dates)].copy()
    if window_df.empty:
        return pd.DataFrame()
    window_df["positive_net"] = np.where(window_df["net_shares"] > 0, window_df["net_shares"], 0.0)

    agg = (
        window_df.groupby("券商")
        .agg(
            buy_shares=("buy_shares", "sum"),
            sell_shares=("sell_shares", "sum"),
            net_shares=("net_shares", "sum"),
            buy_amount=("buy_amount", "sum"),
            sell_amount=("sell_amount", "sum"),
            net_amount=("net_amount", "sum"),
            positive_net=("positive_net", "sum"),
        )
        .sort_values("net_shares", ascending=False)
    )
    agg = agg.loc[agg["net_shares"] > 0]
    agg["broker_name"] = [broker_lookup.get(str(code), "") for code in agg.index]
    agg["buy_avg_price"] = np.where(agg["buy_shares"] > 0, agg["buy_amount"] / agg["buy_shares"], np.nan)
    agg["sell_avg_price"] = np.where(agg["sell_shares"] > 0, agg["sell_amount"] / agg["sell_shares"], np.nan)
    return agg.head(top_n)


def longest_positive_streak(series: Iterable[float]) -> int:
    longest = 0
    current = 0
    for value in series:
        if value > 0:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def compute_broker_daily_stats(per_broker: pd.DataFrame, broker: str, dates: pd.Index) -> pd.DataFrame:
    broker_df = (
        per_broker.loc[per_broker["券商"] == broker, ["日期", "buy_shares", "sell_shares", "net_shares"]]
        .set_index("日期")
        .reindex(dates, fill_value=0.0)
        .sort_index()
    )
    return broker_df


def net_buy_totals(series: pd.Series, days: int) -> float:
    if series.empty:
        return 0.0
    window_series = series.iloc[-min(days, len(series)) :]
    return float(np.maximum(window_series, 0).sum())


def enrich_broker_rows(per_broker: pd.DataFrame, summary: pd.DataFrame, dates: pd.Index) -> pd.DataFrame:
    records = []
    for broker, row in summary.iterrows():
        daily = compute_broker_daily_stats(per_broker, broker, dates)
        net_series = daily["net_shares"]
        buy_days = int((net_series > 0).sum())
        sell_days = int((net_series < 0).sum())
        longest = longest_positive_streak(net_series.values)
        record = {
            "broker": broker,
            "broker_name": row.get("broker_name", ""),
            "buy_shares": row["buy_shares"],
            "sell_shares": row["sell_shares"],
            "net_shares": row["net_shares"],
            "buy_amount": row["buy_amount"],
            "sell_amount": row["sell_amount"],
            "net_amount": row["net_amount"],
            "buy_avg_price": row.get("buy_avg_price", np.nan),
            "sell_avg_price": row.get("sell_avg_price", np.nan),
            "positive_net": row.get("positive_net", np.nan),
            "buy_days": buy_days,
            "sell_days": sell_days,
            "longest_buy_streak": longest,
            "net_buy_5d": net_buy_totals(net_series, 5),
            "net_buy_10d": net_buy_totals(net_series, 10),
            "net_buy_20d": net_buy_totals(net_series, 20),
            "net_buy_50d": net_buy_totals(net_series, 50),
        }
        records.append(record)
    return pd.DataFrame.from_records(records)


# ---------- Price utilities ----------


def load_price_series(code: str, ohlc_root: Path, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    if not ohlc_root.exists():
        return pd.DataFrame(columns=["date", "close", "volume", "name"])
    frames = []
    for csv_path in sorted(ohlc_root.glob("twse-*.csv")):
        try:
            date_token = csv_path.stem.split("-")[-1]
            trade_date = pd.to_datetime(date_token, format="%Y%m%d")
        except ValueError:
            continue
        if trade_date < start or trade_date > end:
            continue
        try:
            df = pd.read_csv(csv_path, dtype=str)
        except Exception:
            continue
        mask = df["證券代號"].astype(str).str.strip() == code
        if not mask.any():
            continue
        row = df.loc[mask].iloc[0]
        close_raw = row.get("收盤價", "0").replace(",", "").strip()
        volume_raw = row.get("成交股數", "0").replace(",", "").strip()
        try:
            close = float(close_raw)
        except ValueError:
            close = np.nan
        try:
            volume = float(volume_raw)
        except ValueError:
            volume = np.nan
        frames.append((trade_date, close, volume, row.get("證券名稱", "")))
    if not frames:
        return pd.DataFrame(columns=["date", "close", "volume", "name"])
    price_df = pd.DataFrame(frames, columns=["date", "close", "volume", "name"]).dropna(subset=["date"])
    return price_df.sort_values("date")


# ---------- Plotting ----------


def plot_recent_activity(
    cfg: Config,
    daily: pd.DataFrame,
    recent_dates: pd.Index,
    broker_summary: pd.DataFrame,
    broker_lookup: dict[str, str],
    broker_daily_stats: dict[str, pd.DataFrame],
    price_df: pd.DataFrame,
    stock_name: str,
) -> Path:
    broker_count = len(broker_summary)
    total_panels = 1 + broker_count
    fig_height = 5 + broker_count * 2.5
    fig, axes = plt.subplots(
        total_panels,
        1,
        figsize=(14, fig_height),
        constrained_layout=True,
    )
    axes = np.atleast_1d(axes)

    # Panel 1: price + volume + concentration (last price_window days)
    overview_ax = axes[0]
    price_start = daily.index[-min(cfg.price_window, len(daily))] if not daily.empty else None
    price_end = daily.index[-1] if not daily.empty else None
    if price_start is not None and price_end is not None:
        range_mask = (daily.index >= price_start) & (daily.index <= price_end)
        conc_series = daily.loc[range_mask, "hhi"]
        date_span = daily.index[range_mask]
    else:
        conc_series = pd.Series(dtype=float)
        date_span = pd.Index([])

    overview_ax.set_title(
        f"{cfg.stock_code} {stock_name} - Recent activity",
        loc="left",
        fontsize=12,
    )

    if not price_df.empty:
        price_window_df = price_df.loc[
            (price_df["date"] >= (price_start or price_df["date"].min()))
            & (price_df["date"] <= (price_end or price_df["date"].max()))
        ]
        if price_window_df.empty:
            price_window_df = price_df
        overview_ax.plot(price_window_df["date"], price_window_df["close"], color="#1f77b4", label="Close")
        overview_ax.set_ylabel("Price")

        vol_ax = overview_ax.twinx()
        vol_ax.bar(
            price_window_df["date"],
            price_window_df["volume"] / 1_000_000,
            color="lightgray",
            alpha=0.5,
            label="Volume (M)",
        )
        vol_ax.set_ylabel("Volume (M)")
        vol_ax.tick_params(axis="y", colors="gray")
    else:
        overview_ax.plot(date_span, daily.loc[range_mask, "net_shares"], color="#1f77b4", label="Net Shares")
        overview_ax.set_ylabel("Net Shares")

    if not conc_series.empty:
        conc_ax = overview_ax.twinx()
        conc_ax.spines["right"].set_position(("axes", 1.05))
        conc_ax.plot(date_span, conc_series, color="#d62728", linestyle="--", label="HHI (net buy)")
        conc_ax.set_ylim(0, 1)
        conc_ax.set_ylabel("HHI")
        conc_ax.tick_params(axis="y", colors="#d62728")

    overview_ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))
    overview_ax.grid(True, linestyle=":", linewidth=0.6, alpha=0.6)

    # Broker panels
    for idx, (broker, row) in enumerate(broker_summary.iterrows(), start=1):
        ax = axes[idx]
        daily_df = broker_daily_stats[broker]
        dates = daily_df.index
        ax.bar(dates, daily_df["buy_shares"], color="#d62728", label="Buy")
        ax.bar(dates, -daily_df["sell_shares"], color="#2ca02c", label="Sell")
        ax.axhline(0, color="#333333", linewidth=0.8)
        broker_name = broker_lookup.get(broker, row.get("broker_name", ""))
        title = f"{broker} {broker_name}".strip()
        subtitle = f"20d淨買超 {row['net_shares']:.0f} 股, 買超天數 {int((daily_df['net_shares']>0).sum())}"
        ax.set_title(f"{title} - {subtitle}", loc="left", fontsize=10)
        ax.set_ylabel("Shares")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))
        ax.grid(True, linestyle=":", linewidth=0.5, alpha=0.5)
        if idx == 1:
            ax.legend(loc="upper left")
        ax.set_xlim(dates.min(), dates.max())

    for ax in axes:
        for label in ax.get_xticklabels():
            label.set_rotation(30)
            label.set_ha("right")

    output_dir = cfg.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    file_path = output_dir / f"{cfg.stock_code}_top_brokers.png"
    fig.savefig(file_path, dpi=200)
    plt.close(fig)
    LOGGER.info("Saved chart to %s", file_path)
    return file_path


# ---------- Main workflow ----------


def run(cfg: Config) -> int:
    if cfg.font_family:
        plt.rcParams["font.family"] = [cfg.font_family]
        plt.rcParams["axes.unicode_minus"] = False

    parquet_paths = resolve_parquet_paths(cfg.data_root, cfg.stock_code)
    if not parquet_paths:
        LOGGER.error("找不到 %s 的 parquet", cfg.stock_code)
        return 1

    trades = load_trade_data(parquet_paths)
    if trades.empty:
        LOGGER.error("沒有交易資料")
        return 1

    per_broker = aggregate_per_broker(trades)
    daily = compute_daily_metrics(per_broker)
    if daily.empty:
        LOGGER.error("無法彙總每日資料")
        return 1

    all_dates = pd.DatetimeIndex(per_broker["日期"].sort_values().unique())
    recent_dates = select_recent_dates(all_dates, cfg.window_days)
    broker_lookup = load_broker_lookup(cfg.broker_list)
    top_brokers = summarize_top_brokers(per_broker, recent_dates, cfg.top_n, broker_lookup)
    if top_brokers.empty:
        LOGGER.warning("近 %d 天沒有買超券商", cfg.window_days)
        return 0

    enriched = enrich_broker_rows(per_broker, top_brokers, recent_dates)
    if enriched.empty:
        LOGGER.warning("無法計算券商統計")
        return 0

    broker_daily_stats = {
        broker: compute_broker_daily_stats(per_broker, broker, recent_dates) for broker in enriched["broker"]
    }

    price_start = (recent_dates.min() - pd.Timedelta(days=max(5, cfg.price_window))) if len(recent_dates) else pd.Timestamp("1900-01-01")
    price_end = recent_dates.max() if len(recent_dates) else daily.index.max()
    price_df = load_price_series(cfg.stock_code, cfg.ohlc_root, price_start, price_end)
    stock_name = price_df["name"].dropna().iloc[-1] if not price_df.empty else ""

    chart_path = plot_recent_activity(
        cfg=cfg,
        daily=daily,
        recent_dates=recent_dates,
        broker_summary=top_brokers,
        broker_lookup=broker_lookup,
        broker_daily_stats=broker_daily_stats,
        price_df=price_df,
        stock_name=stock_name,
    )

    summary_path = cfg.output_dir / f"{cfg.stock_code}_top_brokers_summary.csv"
    enriched.to_csv(summary_path, index=False, encoding="utf-8-sig")
    LOGGER.info("Saved summary to %s", summary_path)

    LOGGER.info("Chart saved to %s", chart_path)
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    cfg = parse_args(argv)
    try:
        return run(cfg)
    except Exception as exc:  # pragma: no cover - safeguard
        LOGGER.exception("分析失敗: %s", exc)
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
