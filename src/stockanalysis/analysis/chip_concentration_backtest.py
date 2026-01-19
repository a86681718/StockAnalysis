import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List

import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze broker chip concentration, detect unusual broker activity, and optionally backtest future returns."
    )
    parser.add_argument(
        "--mode",
        choices=("anomalies", "backtest"),
        default="anomalies",
        help="Select 'anomalies' to surface unusual broker activity or 'backtest' to evaluate returns.",
    )
    parser.add_argument(
        "--broker-dir",
        type=Path,
        required=True,
        help="Directory containing per-stock broker activity parquet files.",
    )
    parser.add_argument(
        "--ohlc-dir",
        type=Path,
        default=Path("data/ohlc"),
        help="Directory containing daily TWSE OHLC CSV files.",
    )
    parser.add_argument(
        "--window-size",
        type=int,
        default=5,
        help="Rolling window (trading days) used to compute chip concentration.",
    )
    parser.add_argument(
        "--quantiles",
        type=float,
        nargs="*",
        default=[0.9, 0.95, 0.99],
        help="Chip concentration quantiles to evaluate.",
    )
    parser.add_argument(
        "--min-sample",
        type=int,
        default=30,
        help="Minimum number of signals required to report a backtest bucket.",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Optional inclusive start date (YYYYMMDD).",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="Optional inclusive end date (YYYYMMDD).",
    )
    parser.add_argument(
        "--anomaly-streak",
        type=int,
        default=3,
        help="Minimum consecutive positive net-buy days required to flag an anomaly.",
    )
    parser.add_argument(
        "--anomaly-share",
        type=float,
        default=0.3,
        help="Minimum share of total stock buy volume during the streak window.",
    )
    parser.add_argument(
        "--anomaly-min-buy",
        type=float,
        default=0.0,
        help="Minimum total buy volume accumulated over the streak window.",
    )
    parser.add_argument(
        "--anomaly-top",
        type=int,
        default=20,
        help="Number of anomalies to display (set <=0 to print all).",
    )
    return parser.parse_args()


def _parse_date(date_str: str) -> pd.Timestamp:
    return pd.to_datetime(date_str, format="%Y%m%d")


def load_price_data(ohlc_dir: Path, start: pd.Timestamp | None, end: pd.Timestamp | None) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    for csv_path in sorted(ohlc_dir.glob("twse-*.csv")):
        date_token = csv_path.stem.split("-")[-1]
        date = _parse_date(date_token)
        if start and date < start:
            continue
        if end and date > end:
            continue

        df = pd.read_csv(csv_path, dtype=str).fillna("")
        df["date"] = date
        df.rename(columns={"證券代號": "stock_id"}, inplace=True)

        numeric_cols = [
            "收盤價",
            "開盤價",
            "最高價",
            "最低價",
        ]
        for col in numeric_cols:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(",", "", regex=False)
                .str.replace("--", "", regex=False)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

        frames.append(df[["stock_id", "date", "收盤價"]])

    if not frames:
        raise FileNotFoundError(f"No twse-*.csv files found under {ohlc_dir}")

    price_df = pd.concat(frames, ignore_index=True)
    price_df.rename(columns={"收盤價": "close"}, inplace=True)
    price_df.sort_values(["stock_id", "date"], inplace=True)
    return price_df


def load_broker_data(
    broker_dir: Path, start: pd.Timestamp | None, end: pd.Timestamp | None
) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    for parquet_path in sorted(broker_dir.glob("*.parquet")):
        stock_id = parquet_path.stem
        df = pd.read_parquet(parquet_path)
        if df.empty:
            continue

        df = df.copy()
        df["stock_id"] = stock_id
        df.rename(
            columns={
                "日期": "date",
                "券商": "broker",
                "買進股數": "buy_volume",
                "賣出股數": "sell_volume",
                "價格": "price",
            },
            inplace=True,
        )
        df["date"] = pd.to_datetime(df["date"])
        if start:
            df = df[df["date"] >= start]
        if end:
            df = df[df["date"] <= end]
        if df.empty:
            continue

        for col in ["buy_volume", "sell_volume", "price"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        frames.append(df[["stock_id", "date", "broker", "buy_volume", "sell_volume"]])

    if not frames:
        raise FileNotFoundError(f"No parquet files found under {broker_dir}")

    broker_df = pd.concat(frames, ignore_index=True)
    broker_df.dropna(subset=["stock_id", "date", "broker"], inplace=True)
    return broker_df


def aggregate_broker_daily(broker_df: pd.DataFrame) -> pd.DataFrame:
    if broker_df.empty:
        return pd.DataFrame(
            columns=["stock_id", "date", "broker", "buy_volume", "sell_volume", "net_volume"]
        )

    df = broker_df.copy()
    for col in ["buy_volume", "sell_volume"]:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    daily = (
        df.groupby(["stock_id", "date", "broker"], as_index=False)
        .agg({"buy_volume": "sum", "sell_volume": "sum"})
    )
    daily["net_volume"] = daily["buy_volume"] - daily["sell_volume"]
    daily.sort_values(["stock_id", "broker", "date"], inplace=True)
    daily.reset_index(drop=True, inplace=True)
    return daily


def compute_rolling_chip_features(
    daily: pd.DataFrame, window_size: int
) -> pd.DataFrame:
    if daily.empty:
        return pd.DataFrame(
            columns=[
                "stock_id",
                "date",
                "chip_concentration",
                "top3_share",
                "total_positive_net",
                "total_net",
                "positive_broker_count",
                "active_broker_count",
            ]
        )

    rolling = daily.copy()
    rolling.sort_values(["stock_id", "broker", "date"], inplace=True)
    rolling["net_volume_window"] = (
        rolling.groupby(["stock_id", "broker"], group_keys=False)["net_volume"]
        .apply(lambda s: s.rolling(window=window_size, min_periods=1).sum())
    )

    def _chip_metrics(group: pd.DataFrame) -> pd.Series:
        window_net = group["net_volume_window"]
        total_net = window_net.sum()
        positive = window_net[window_net > 0]
        total_positive = positive.sum()
        active_brokers = len(group)
        positive_brokers = int((window_net > 0).sum())

        if total_positive <= 0:
            concentration = 0.0
            top3_share = 0.0
        else:
            shares = positive / total_positive
            concentration = float((shares**2).sum())
            top3_share = float(shares.nlargest(3).sum())

        return pd.Series(
            {
                "chip_concentration": concentration,
                "top3_share": top3_share,
                "total_positive_net": total_positive,
                "total_net": total_net,
                "positive_broker_count": positive_brokers,
                "active_broker_count": active_brokers,
            }
        )

    features = rolling.groupby(["stock_id", "date"], sort=True).apply(_chip_metrics).reset_index()
    features.sort_values(["stock_id", "date"], inplace=True)
    return features


def _compute_positive_streak(flags: pd.Series) -> pd.Series:
    streak = []
    count = 0
    for flag in flags.astype(bool):
        if flag:
            count += 1
        else:
            count = 0
        streak.append(count)
    return pd.Series(streak, index=flags.index, dtype=int)


def find_broker_buying_anomalies(
    daily: pd.DataFrame,
    streak_days: int,
    share_threshold: float,
    min_window_buy: float,
    top_n: int | None = None,
) -> pd.DataFrame:
    if daily.empty:
        return pd.DataFrame(
            columns=[
                "stock_id",
                "broker",
                "window_start_date",
                "date",
                "positive_streak",
                "window_buy_share",
                "buy_window",
                "stock_buy_window",
                "net_window",
                "avg_daily_net",
            ]
        )

    if streak_days <= 0:
        raise ValueError("streak_days must be positive")

    work = daily.copy()
    work.sort_values(["stock_id", "broker", "date"], inplace=True)
    work["positive_flag"] = work["net_volume"] > 0
    work["positive_streak"] = (
        work.groupby(["stock_id", "broker"], group_keys=False)["positive_flag"].apply(
            _compute_positive_streak
        )
    )

    work["rolling_positive_days"] = (
        work.groupby(["stock_id", "broker"], group_keys=False)["positive_flag"]
        .apply(lambda s: s.astype(int).rolling(window=streak_days, min_periods=streak_days).sum())
        .astype(float)
    )

    work["buy_window"] = (
        work.groupby(["stock_id", "broker"], group_keys=False)["buy_volume"]
        .apply(lambda s: s.rolling(window=streak_days, min_periods=streak_days).sum())
    )
    work["net_window"] = (
        work.groupby(["stock_id", "broker"], group_keys=False)["net_volume"]
        .apply(lambda s: s.rolling(window=streak_days, min_periods=streak_days).sum())
    )

    stock_buy = (
        work.groupby(["stock_id", "date"], as_index=False)["buy_volume"].sum()
        .rename(columns={"buy_volume": "stock_buy_volume"})
    )
    stock_buy.sort_values(["stock_id", "date"], inplace=True)
    stock_buy["stock_buy_window"] = stock_buy.groupby("stock_id")["stock_buy_volume"].transform(
        lambda s: s.rolling(window=streak_days, min_periods=streak_days).sum()
    )
    work = work.merge(stock_buy, on=["stock_id", "date"], how="left")

    work["window_buy_share"] = np.where(
        work["stock_buy_window"] > 0,
        work["buy_window"] / work["stock_buy_window"],
        np.nan,
    )

    work["rolling_positive_days"] = work["rolling_positive_days"].fillna(0).astype(int)

    if streak_days > 1:
        work["window_start_date"] = work.groupby(["stock_id", "broker"])["date"].shift(streak_days - 1)
    else:
        work["window_start_date"] = work["date"]

    mask = (
        (work["rolling_positive_days"] == streak_days)
        & (work["window_buy_share"] >= share_threshold)
        & (work["buy_window"].fillna(0) >= min_window_buy)
        & (work["net_window"].fillna(0) > 0)
        & work["window_start_date"].notna()
    )

    columns = [
        "stock_id",
        "broker",
        "window_start_date",
        "date",
        "positive_streak",
        "window_buy_share",
        "buy_window",
        "stock_buy_window",
        "net_window",
        "avg_daily_net",
    ]

    anomalies = work.loc[mask].copy()
    if anomalies.empty:
        anomalies = anomalies.assign(avg_daily_net=pd.Series(dtype=float))
        return anomalies[columns]

    anomalies["avg_daily_net"] = anomalies["net_window"] / streak_days
    anomalies.sort_values(
        ["window_buy_share", "net_window"], ascending=[False, False], inplace=True
    )
    if top_n and top_n > 0:
        anomalies = anomalies.head(top_n).copy()

    return anomalies[columns]


def print_anomaly_summary(anomalies: pd.DataFrame, streak_days: int) -> None:
    if anomalies.empty:
        print("No broker anomalies met the specified criteria.")
        return

    for _, row in anomalies.iterrows():
        start_ts = row.get("window_start_date")
        end_ts = row.get("date")
        start_str = start_ts.date() if isinstance(start_ts, pd.Timestamp) else start_ts
        end_str = end_ts.date() if isinstance(end_ts, pd.Timestamp) else end_ts

        buy_share = row.get("window_buy_share", np.nan)
        net_window = row.get("net_window", np.nan)
        buy_window = row.get("buy_window", np.nan)
        stock_buy_window = row.get("stock_buy_window", np.nan)
        avg_daily_net = row.get("avg_daily_net", np.nan)
        positive_streak = int(row.get("positive_streak", streak_days))

        share_text = f"{buy_share:.1%}" if pd.notna(buy_share) else "n/a"
        net_text = f"{net_window:,.0f}" if pd.notna(net_window) else "n/a"
        buy_text = f"{buy_window:,.0f}" if pd.notna(buy_window) else "n/a"
        stock_buy_text = f"{stock_buy_window:,.0f}" if pd.notna(stock_buy_window) else "n/a"
        avg_net_text = f"{avg_daily_net:,.0f}" if pd.notna(avg_daily_net) else "n/a"

        print(
            f"{row['stock_id']} | {row['broker']} | {start_str} -> {end_str} "
            f"(window_days={streak_days}, positive_streak={positive_streak})\n"
            f"  buy_share={share_text}, net_buy={net_text}, window_buy={buy_text} / total_buy={stock_buy_text}, "
            f"avg_daily_net={avg_net_text}"
        )


def prepare_dataset(
    price_df: pd.DataFrame, chip_df: pd.DataFrame, horizons: Iterable[int]
) -> pd.DataFrame:
    merged = price_df.merge(chip_df, on=["stock_id", "date"], how="inner")
    merged.sort_values(["stock_id", "date"], inplace=True)

    for horizon in horizons:
        merged[f"future_return_{horizon}"] = (
            merged.groupby("stock_id")["close"].shift(-horizon) / merged["close"]
        ) - 1.0

    merged.dropna(subset=[f"future_return_{h}" for h in horizons], inplace=True)
    return merged


@dataclass
class BacktestResult:
    quantile: float
    signals: int
    avg_concentration: float
    metrics: dict


def evaluate_quantiles(
    dataset: pd.DataFrame,
    quantiles: List[float],
    horizons: Iterable[int],
    min_sample: int,
) -> List[BacktestResult]:
    results: List[BacktestResult] = []
    concentration_values = dataset["chip_concentration"].replace([np.inf, -np.inf], np.nan).dropna()
    if concentration_values.empty:
        return results

    thresholds = {}
    for q in quantiles:
        try:
            thresholds[q] = concentration_values.quantile(q)
        except ValueError:
            continue

    for q, threshold in thresholds.items():
        subset = dataset[dataset["chip_concentration"] >= threshold]
        subset = subset[subset["total_positive_net"] > 0]
        if len(subset) < min_sample:
            continue

        metrics = {}
        for horizon in horizons:
            col = f"future_return_{horizon}"
            success_rate = float((subset[col] > 0).mean())
            baseline = float((dataset[col] > 0).mean())
            metrics[horizon] = {
                "success_rate": success_rate,
                "baseline": baseline,
                "lift": success_rate / baseline if baseline > 0 else np.nan,
                "avg_return": float(subset[col].mean()),
                "median_return": float(subset[col].median()),
            }

        results.append(
            BacktestResult(
                quantile=q,
                signals=len(subset),
                avg_concentration=float(subset["chip_concentration"].mean()),
                metrics=metrics,
            )
        )

    return results


def print_summary(results: List[BacktestResult], horizons: Iterable[int]) -> None:
    if not results:
        print("No quantile buckets met the minimum sample requirement.")
        return

    for result in sorted(results, key=lambda r: r.quantile):
        print(
            f"Quantile >= {result.quantile:.2f}: signals={result.signals}, avg_concentration={result.avg_concentration:.3f}"
        )
        for horizon in horizons:
            metric = result.metrics.get(horizon)
            if not metric:
                continue
            print(
                f"  Horizon {horizon}d -> success={metric['success_rate']:.2%} (baseline {metric['baseline']:.2%}), "
                f"lift={metric['lift']:.2f}, avg_return={metric['avg_return']:.2%}, median_return={metric['median_return']:.2%}"
            )
        print("---")


def main() -> None:
    args = parse_args()
    start = _parse_date(args.start_date) if args.start_date else None
    end = _parse_date(args.end_date) if args.end_date else None
    horizons = (5, 10, 20)

    broker_df = load_broker_data(args.broker_dir, start, end)
    print(
        f"Loaded broker trades: {len(broker_df)} rows across {broker_df['stock_id'].nunique()} stocks"
        f" and {broker_df['date'].nunique()} trading days"
    )

    daily_broker = aggregate_broker_daily(broker_df)
    print(
        f"Aggregated broker-day activity: {len(daily_broker)} rows covering {daily_broker['stock_id'].nunique()} stocks"
        f" and {daily_broker['broker'].nunique()} brokers"
    )

    if args.mode == "anomalies":
        anomalies = find_broker_buying_anomalies(
            daily_broker,
            streak_days=args.anomaly_streak,
            share_threshold=args.anomaly_share,
            min_window_buy=args.anomaly_min_buy,
            top_n=args.anomaly_top,
        )
        print_anomaly_summary(anomalies, args.anomaly_streak)
        return

    price_df = load_price_data(args.ohlc_dir, start, end)
    print(
        f"Loaded price data: {len(price_df)} rows spanning {price_df['date'].min().date()}"
        f" to {price_df['date'].max().date()} (stocks={price_df['stock_id'].nunique()})"
    )

    chip_df = compute_rolling_chip_features(daily_broker, window_size=args.window_size)
    print(f"Computed chip concentration features: {len(chip_df)} stock-days")

    dataset = prepare_dataset(price_df, chip_df, horizons)
    print(f"Merged dataset contains {len(dataset)} stock-days with future returns")

    results = evaluate_quantiles(dataset, args.quantiles, horizons, args.min_sample)
    print_summary(results, horizons)


if __name__ == "__main__":
    main()
