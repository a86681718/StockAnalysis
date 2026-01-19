from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from tqdm import tqdm

from stockanalysis.config import resolve_data, resolve_output

# Default paths – override via CLI args if needed.
BROKER_DATA_DIR = resolve_data("bs_report", "parquet_twse")
WARRANT_LIST_PATH = resolve_data("warrant_list_dedup.csv")
BROKER_LIST_PATH = resolve_data("broker_list.csv")
OHLC_DIR = resolve_data("ohlc")
OUTPUT_DIR = resolve_output("analysis", "high_concentration")

HHI_WINDOWS = (5, 10, 20, 60)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def load_warrant_meta() -> pd.DataFrame:
    if not WARRANT_LIST_PATH.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(WARRANT_LIST_PATH, dtype=str, encoding="utf-8-sig").fillna("")
    except Exception:
        logging.warning("Failed to read %s", WARRANT_LIST_PATH)
        return pd.DataFrame()
    return df


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
    if WARRANT_META.empty or len(code.strip()) <= 4:
        return code, ""
    hit = WARRANT_META.loc[WARRANT_META["權證代號"].str.strip() == code]
    if hit.empty:
        return code, ""
    row = hit.iloc[0]
    return row.get("標的代號", code), row.get("標的名稱", "")


def load_trades(code: str) -> pd.DataFrame:
    path = BROKER_DATA_DIR / f"{code}.parquet"
    if not path.exists():
        raise FileNotFoundError(path)
    df = pd.read_parquet(path)
    df = df.rename(columns=str.strip)
    for col in ["日期", "券商"]:
        if col not in df.columns:
            raise ValueError(f"{path} 缺少欄位 {col}")
    df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
    df = df.dropna(subset=["日期"])
    # convert numeric
    for col in ["價格", "買進股數", "賣出股數", "買進金額", "賣出金額", "買進股數", "賣出股數"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "買進金額" not in df.columns and "價格" in df.columns:
        df["買進金額"] = df["價格"] * df.get("買進股數", 0)
    if "賣出金額" not in df.columns and "價格" in df.columns:
        df["賣出金額"] = df["價格"] * df.get("賣出股數", 0)
    return df


def aggregate_daily(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    per_broker = (
        df.groupby(["日期", "券商"], as_index=False)
          .agg(
              buy_shares=("買進股數", "sum"),
              sell_shares=("賣出股數", "sum"),
              buy_amount=("買進金額", "sum"),
              sell_amount=("賣出金額", "sum"),
          )
    )
    per_broker["券商"] = per_broker["券商"].astype(str).str.strip()
    per_broker["net_shares"] = per_broker["buy_shares"].fillna(0) - per_broker["sell_shares"].fillna(0)
    per_broker["net_amount"] = per_broker["buy_amount"].fillna(0) - per_broker["sell_amount"].fillna(0)

    daily = (
        per_broker.groupby("日期")[["buy_shares", "sell_shares", "net_shares", "buy_amount", "sell_amount", "net_amount"]]
        .sum()
    )
    daily.sort_index(inplace=True)

    # concentration metrics
    hhi_rows = []
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
    return daily, per_broker


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


def find_high_concentration(
    windowed_daily: pd.DataFrame,
    hhi_threshold: float,
    top_share_threshold: float,
    window_days: int,
) -> bool:
    if windowed_daily.empty:
        return False
    hhi_col = f"hhi_{window_days}d"
    if hhi_col in windowed_daily.columns:
        recent_hhi = windowed_daily[hhi_col].iloc[-1]
    else:
        recent_hhi = (
            windowed_daily["hhi"].rolling(window_days, min_periods=1).mean().iloc[-1]
        )
    if pd.isna(recent_hhi) or recent_hhi < hhi_threshold:
        return False
    if top_share_threshold is None:
        return True
    mean_top = windowed_daily["top_share"].mean()
    return pd.notna(mean_top) and (mean_top >= top_share_threshold)


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


def plot_stock(
    stock_code: str,
    daily: pd.DataFrame,
    per_broker: pd.DataFrame,
    top_brokers: pd.DataFrame,
    price_df: pd.DataFrame,
    target_name: str,
    output_dir: Path,
    hhi_windows: tuple[int, ...],
    hhi_threshold: float,
    window_days: int,
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
    ax_price_panel = axes[0]
    if not price_df.empty:
        price_window_df = price_df.loc[
            (price_df["date"] >= dates.min()) & (price_df["date"] <= dates.max())
        ]
        if price_window_df.empty:
            price_window_df = price_df
        ax_price_panel.plot(
            price_window_df["date"],
            price_window_df["close"],
            color="#1f77b4",
            label="Close Price",
        )
        vol_ax = ax_price_panel.twinx()
        volume_series = (
            price_window_df.get("volume", pd.Series(index=price_window_df.index, dtype=float))
            .fillna(0)
            / 1_000_000
        )
        vol_ax.bar(
            price_window_df["date"],
            volume_series,
            color="lightgray",
            alpha=0.4,
            label="Volume (M)",
        )
        vol_ax.set_ylabel("Volume (M)")
        vol_ax.legend(loc="upper right")
        ax_price_panel.set_ylabel("Price")
        ax_price_panel.legend(loc="upper left")
    else:
        ax_price_panel.plot(dates, daily["net_shares"], color="#1f77b4", label="Net Shares")
        ax_price_panel.set_ylabel("Net Shares")
        ax_price_panel.legend(loc="upper left")

    ax_hhi = axes[1]
    hhi_colors = {
        5: "#1f77b4",
        10: "#ff7f0e",
        20: "#2ca02c",
        60: "#d62728",
    }
    for window in hhi_windows:
        col = f"hhi_{window}d"
        if col not in daily.columns:
            continue
        color = hhi_colors.get(window, None)
        ax_hhi.plot(dates, daily[col], label=f"{window}日HHI", color=color)
    threshold_label = f"{window_days}日HHI分界線 ({hhi_threshold:.0%})"
    ax_hhi.axhline(hhi_threshold, color="#444444", linestyle="--", linewidth=1, label=threshold_label)
    ax_hhi.set_ylabel("HHI")
    ax_hhi.set_ylim(0, 1)
    ax_hhi.legend(loc="upper left")
    ax_hhi.grid(True, linestyle=":", linewidth=0.5, alpha=0.6)

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
        broker_name = row.get("broker_name", "")
        if not broker_name and broker_code in BROKER_LOOKUP:
            broker_name = BROKER_LOOKUP[broker_code]
        title_parts = [broker_code]
        if broker_name:
            title_parts.append(broker_name)
        if pd.notna(ratio_pct):
            title_parts.append(f"20天買超比率 {ratio_pct:.1f}%")
        title = " ".join(title_parts)
        ax.set_ylabel("Shares")
        ax.set_title(title, loc="left", fontsize=10)

    plt.xticks(rotation=45)
    title = f"{stock_code}"
    if target_name:
        title += f" ({target_name})"
    title += " – Concentration Overview"
    fig.suptitle(title)
    plt.tight_layout()

    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"{stock_code}_concentration.png"
    plt.savefig(out_path, dpi=200)
    plt.close(fig)
    logging.info("Saved plot for %s to %s", stock_code, out_path)


def analyze_stock(
    code: str,
    window_days: int,
    hhi_threshold: float,
    top_share_threshold: float,
    output_dir: Path,
) -> Optional[dict]:
    try:
        trades = load_trades(code)
    except FileNotFoundError:
        return None
    except Exception as exc:
        logging.warning("Failed to load %s: %s", code, exc)
        return None

    trades.sort_values("日期", inplace=True)
    end_date = trades["日期"].max()
    daily, per_broker = aggregate_daily(trades)
    if daily.empty:
        return None

    daily.sort_index(inplace=True)
    rolling_targets = set(HHI_WINDOWS)
    if window_days not in rolling_targets:
        rolling_targets.add(window_days)
    for window in sorted(rolling_targets):
        daily[f"hhi_{window}d"] = daily["hhi"].rolling(window, min_periods=window).mean()

    recent_daily = daily.loc[daily.index >= end_date - pd.Timedelta(days=window_days - 1)]
    if recent_daily.empty:
        recent_daily = daily.tail(window_days)
    if recent_daily.empty:
        return None

    if not find_high_concentration(recent_daily, hhi_threshold, top_share_threshold, window_days):
        return None

    recent_dates = recent_daily.index
    recent_per_broker = per_broker.loc[per_broker["日期"].isin(recent_dates)]
    if recent_per_broker.empty:
        return None
    top_brokers = summarize_brokers(recent_per_broker)
    top_brokers = top_brokers.loc[top_brokers["net_shares"] > 0]
    if top_brokers.empty:
        return None
    underlying_code, underlying_name = resolve_underlying(code)
    price_df, price_name = load_price_series(underlying_code)
    if not underlying_name and price_name:
        underlying_name = price_name

    plot_stock(
        stock_code=code,
        daily=daily,
        per_broker=per_broker,
        top_brokers=top_brokers,
        price_df=price_df,
        target_name=underlying_name,
        output_dir=output_dir,
        hhi_windows=tuple(sorted(HHI_WINDOWS)),
        hhi_threshold=hhi_threshold,
        window_days=window_days,
    )

    summary = top_brokers.head(10).copy().reset_index()
    summary.rename(columns={"券商": "broker"}, inplace=True)
    summary["stock_code"] = code
    summary["underlying_name"] = underlying_name
    hhi_col = f"hhi_{window_days}d"
    summary["window_hhi_mean"] = recent_daily["hhi"].mean()
    summary["window_top_share_mean"] = recent_daily["top_share"].mean()
    summary["window_hhi_latest"] = recent_daily[hhi_col].iloc[-1] if hhi_col in recent_daily.columns else np.nan
    if "broker_name" not in summary.columns:
        summary["broker_name"] = summary["broker"].map(BROKER_LOOKUP)
    summary["net_share_ratio_pct"] = summary["net_share_ratio"] * 100
    summary = summary[
        [
            "stock_code",
            "underlying_name",
            "broker",
            "broker_name",
            "buy_shares",
            "sell_shares",
            "net_shares",
            "buy_amount",
            "sell_amount",
            "net_amount",
            "net_share_ratio",
            "net_share_ratio_pct",
            "buy_share_ratio",
            "window_hhi_mean",
            "window_hhi_latest",
            "window_top_share_mean",
        ]
    ]

    return {
        "stock_code": code,
        "underlying_name": underlying_name,
        "top_brokers": summary,
        "daily_stats": daily,
    }


def parse_cli_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Find stocks with concentrated warrant buying")
    parser.add_argument("--data-root", default=BROKER_DATA_DIR, type=Path, help="Parquet directory")
    parser.add_argument("--window", type=int, default=20, help="Window size (days)")
    parser.add_argument("--hhi-threshold", type=float, default=0.25, help="Minimum average HHI")
    parser.add_argument("--top-share-threshold", type=float, default=0.4, help="Minimum average top-share")
    parser.add_argument("--limit", type=int, help="Limit number of stocks to analyze")
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    parser.add_argument(
        "--font-family",
        help="Matplotlib font family to support Chinese labels",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_cli_args(argv)
    if not args.data_root.exists():
        logging.error("Data root %s not found", args.data_root)
        return 1

    global BROKER_DATA_DIR
    BROKER_DATA_DIR = args.data_root
    results = []

    if args.font_family:
        plt.rcParams["font.sans-serif"] = [args.font_family]
        plt.rcParams["axes.unicode_minus"] = False

    parquet_files = sorted(args.data_root.glob("*.parquet"))
    if args.limit:
        parquet_files = parquet_files[:args.limit]

    for path in tqdm(parquet_files, desc="Scanning"):
        code = path.stem
        if len(code.strip()) > 4:
            continue
        res = analyze_stock(
            code=code,
            window_days=args.window,
            hhi_threshold=args.hhi_threshold,
            top_share_threshold=args.top_share_threshold,
            output_dir=args.output_dir,
        )
        if res:
            results.append(res)

    if not results:
        logging.info("No stocks met the concentration criteria.")
        return 0

    all_brokers = pd.concat([item["top_brokers"] for item in results], ignore_index=True)
    print(all_brokers)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = args.output_dir / "high_concentration_brokers.csv"
    all_brokers.to_csv(csv_path, index=False, encoding="utf-8-sig")
    logging.info("Saved broker summary to %s", csv_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
