"""Broker anomaly analysis for TWSE broker buy/sell reports.

This script aggregates per-broker buy/sell data for a single stock across
available TWSE bsreport CSV files and highlights unusually strong buying.
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
from dataclasses import dataclass, replace
from pathlib import Path
from time import perf_counter
from typing import Any, Iterable, Iterator, Optional

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


@dataclass
class AnalyzerConfig:
    stock_code: str
    data_root: Path
    warrant_list_path: Path
    start_date: Optional[str]
    end_date: Optional[str]
    min_history_days: int
    top_n: int
    plot: bool
    plot_path: Optional[Path]
    detection_window: int
    window_ratio_threshold: float
    min_window_net: float
    min_net_buy_ratio: float
    max_single_day_share: float
    ohlc_root: Optional[Path]
    font_family: Optional[str]
    broker_list_path: Optional[Path]
    min_hyphenated_length: int
    concentration_window: int
    run_all: bool


class BrokerAnomalyAnalyzer:
    FORWARD_HORIZONS: tuple[int, ...] = (5, 10, 20)

    def __init__(self, cfg: AnalyzerConfig, warrant_lookup: Optional[pd.DataFrame] = None) -> None:
        self.cfg = cfg
        self._analysis_window: Optional[tuple[pd.Timestamp, pd.Timestamp]] = None
        self._broker_lookup: dict[str, str] = {}
        self._warrant_lookup: Optional[pd.DataFrame] = warrant_lookup.copy() if warrant_lookup is not None else None
        self._current_warrants: Optional[pd.DataFrame] = None
        self._warrant_meta: dict[str, dict[str, str]] = {}
        self._target_warrant_codes: list[str] = []
        self._underlying_name: str = ""
        self._per_warrant_rows: list[dict] = []
        self._per_warrant_df: pd.DataFrame = pd.DataFrame()

    def run(self) -> int:
        overall_start = perf_counter()
        logging.info("[%s] Starting warrant anomaly analysis", self.cfg.stock_code)
        self._broker_lookup = self._load_broker_lookup()
        self._ensure_warrant_lookup()
        self._current_warrants = self._select_warrants_for_underlying(self.cfg.stock_code)
        if self._current_warrants is None or self._current_warrants.empty:
            print(f"No warrants found for underlying {self.cfg.stock_code}.")
            return 1
        if "標的名稱" in self._current_warrants.columns and not self._current_warrants["標的名稱"].empty:
            self._underlying_name = str(self._current_warrants["標的名稱"].iloc[0])
        self._target_warrant_codes = self._current_warrants["權證代號"].tolist()
        self._warrant_meta = (
            self._current_warrants.set_index("權證代號")[["權證簡稱", "標的代號", "標的名稱"]].to_dict("index")
        )
        self._per_warrant_rows = []
        logging.info(
            "[%s] Using %d warrants (underlying name: %s)",
            self.cfg.stock_code,
            len(self._target_warrant_codes),
            self._underlying_name or "N/A",
        )

        load_start = perf_counter()
        records = list(self._load_broker_records())
        logging.info(
            "[%s] Loaded %d broker records in %.2fs",
            self.cfg.stock_code,
            len(records),
            perf_counter() - load_start,
        )
        if not records:
            print(
                f"No broker files found for underlying {self.cfg.stock_code} "
                f"between {self.cfg.start_date or 'beginning'} and {self.cfg.end_date or 'end'}"
            )
            return 1

        df_start = perf_counter()
        data = pd.DataFrame.from_records(records)
        logging.info("[%s] Constructed raw DataFrame (%d rows) in %.2fs", self.cfg.stock_code, len(data), perf_counter() - df_start)
        if data.empty:
            print("No broker transactions were parsed from the input files.")
            return 1

        if self._per_warrant_rows:
            self._per_warrant_df = pd.DataFrame(self._per_warrant_rows)
            if not self._per_warrant_df.empty:
                self._per_warrant_df["date"] = pd.to_datetime(self._per_warrant_df["date"], format="%Y%m%d")
        else:
            self._per_warrant_df = pd.DataFrame()

        date_series = pd.to_datetime(data["date"], format="%Y%m%d", errors="coerce")
        data = data.loc[~date_series.isna()].copy()
        if data.empty:
            print("No valid date entries found in the broker data.")
            return 1

        data["date_dt"] = date_series.loc[data.index]
        available_start = data["date_dt"].min()
        available_end = data["date_dt"].max()

        if self.cfg.start_date:
            start_dt = pd.to_datetime(self.cfg.start_date, format="%Y%m%d", errors="coerce")
        else:
            start_dt = available_start
        if self.cfg.end_date:
            end_dt = pd.to_datetime(self.cfg.end_date, format="%Y%m%d", errors="coerce")
        else:
            end_dt = available_end

        if start_dt is None or end_dt is None or pd.isna(start_dt) or pd.isna(end_dt):
            print("Invalid analysis window. Please verify start/end dates in the data.")
            return 1

        if end_dt < start_dt:
            print("Analysis end date precedes start date. Adjust your inputs.")
            return 1

        filtered = data[(data["date_dt"] >= start_dt) & (data["date_dt"] <= end_dt)].copy()
        if filtered.empty:
            print(
                "No broker data within the requested date range. "
                f"Available range: {available_start.date()} to {available_end.date()}"
            )
            return 1

        trading_days = self._trading_days_between(start_dt, end_dt)
        filtered["date"] = filtered["date_dt"]
        agg_start = perf_counter()
        filtered = self._aggregate_by_broker(filtered)
        logging.info(
            "[%s] Aggregated broker data to %d rows in %.2fs",
            self.cfg.stock_code,
            len(filtered),
            perf_counter() - agg_start,
        )
        filtered["date_dt"] = filtered["date"]
        if trading_days is not None and not trading_days.empty:
            align_start = perf_counter()
            filtered = self._align_with_trading_days(filtered, trading_days)
            logging.info(
                "[%s] Aligned trading days in %.2fs",
                self.cfg.stock_code,
                perf_counter() - align_start,
            )
        else:
            filtered = self._apply_broker_names(filtered)
        filtered.sort_values(["date", "broker"], inplace=True)
        filtered["date"] = pd.to_datetime(filtered["date"], errors="coerce")
        filtered = filtered.loc[~filtered["date"].isna()].copy()
        filtered["date"] = filtered["date"].dt.strftime("%Y%m%d")
        filtered.drop(columns="date_dt", inplace=True, errors="ignore")
        data = filtered
        self._analysis_window = (start_dt, end_dt)

        enrich_start = perf_counter()
        enriched = self._enrich_with_statistics(data)
        logging.info(
            "[%s] Enriched statistics (%d rows) in %.2fs",
            self.cfg.stock_code,
            len(enriched),
            perf_counter() - enrich_start,
        )

        metrics_start = perf_counter()
        windowed = self._apply_window_metrics(enriched)
        logging.info(
            "[%s] Computed window metrics in %.2fs",
            self.cfg.stock_code,
            perf_counter() - metrics_start,
        )

        price_series = self._load_price_series(max(self.FORWARD_HORIZONS))
        forward_start = perf_counter()
        windowed = self._attach_forward_returns(windowed, price_series)
        logging.info(
            "[%s] Attached forward returns in %.2fs",
            self.cfg.stock_code,
            perf_counter() - forward_start,
        )
        anomalies = self._detect_anomalies(windowed)
        self._print_summary(anomalies, windowed)
        if self.cfg.plot or self.cfg.plot_path:
            self._maybe_plot(anomalies, windowed)
        logging.info(
            "[%s] Completed analysis in %.2fs",
            self.cfg.stock_code,
            perf_counter() - overall_start,
        )
        return 0

    def _load_broker_records(self) -> Iterator[dict]:
        for warrant_code, parquet_path in self._resolve_parquet_files():
            yield from self._load_from_parquet(warrant_code, parquet_path)

    def _resolve_parquet_files(self) -> list[tuple[str, Path]]:
        suffixes = (".parquet", ".pq", ".parq")
        root = self.cfg.data_root
        files: list[tuple[str, Path]] = []
        resolve_start = perf_counter()
        for code in self._target_warrant_codes:
            matched: Optional[Path] = None
            for suffix in suffixes:
                candidate = root / f"{code}{suffix}"
                if candidate.exists():
                    matched = candidate
                    break
            if matched is None:
                for suffix in suffixes:
                    matches = list(root.rglob(f"{code}{suffix}"))
                    if matches:
                        matched = matches[0]
                        break
            if matched is None:
                logging.debug("Skipping warrant %s (file not found)", code)
                continue
            files.append((code, matched))
        logging.info(
            "[%s] Resolved %d/%d warrant files in %.2fs",
            self.cfg.stock_code,
            len(files),
            len(self._target_warrant_codes),
            perf_counter() - resolve_start,
        )
        return files

    def _load_from_parquet(self, warrant_code: str, parquet_path: Path) -> Iterator[dict]:
        start_time = perf_counter()
        try:
            df = pd.read_parquet(parquet_path)
        except Exception as exc:  # pragma: no cover - defensive
            print(f"Failed to parse {parquet_path}: {exc}", file=sys.stderr)
            return

        normalized = self._normalize_broker_df(df)
        if normalized.empty:
            return

        normalized["date_key"] = normalized["日期"].dt.strftime("%Y%m%d")
        for date_key, daily_df in normalized.groupby("date_key"):
            if daily_df.empty:
                continue
            summary = self._summarize_daily(daily_df)
            for row in summary.itertuples(index=False):
                meta = self._warrant_meta.get(warrant_code, {})
                self._per_warrant_rows.append(
                    {
                        "date": date_key,
                        "broker": str(row.券商),
                        "warrant_code": warrant_code,
                        "warrant_name": meta.get("權證簡稱", ""),
                        "buy_shares": row.buy_shares,
                        "sell_shares": row.sell_shares,
                        "net_shares": row.net_shares,
                    }
                )
                yield {
                    "date": date_key,
                    "broker": str(row.券商),
                    "buy_shares": row.buy_shares,
                    "sell_shares": row.sell_shares,
                    "net_shares": row.net_shares,
                    "buy_amount": row.buy_amount,
                    "sell_amount": row.sell_amount,
                    "avg_buy_price": row.avg_buy_price,
                    "avg_sell_price": row.avg_sell_price,
                    "avg_price": row.avg_price,
                    "total_buy_shares": row.total_buy_shares,
                    "total_net_shares": row.total_net_shares,
                    "warrant_code": warrant_code,
                    "warrant_name": meta.get("權證簡稱", ""),
                    "underlying_code": meta.get("標的代號", self.cfg.stock_code),
                    "underlying_name": meta.get("標的名稱", ""),
                }
        logging.debug(
            "[%s] Processed warrant %s (%d rows) in %.2fs",
            self.cfg.stock_code,
            warrant_code,
            len(normalized),
            perf_counter() - start_time,
        )
        return

    @staticmethod
    def _normalize_broker_df(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.columns = df.columns.str.strip()
        expected_cols = {"價格", "券商", "日期", "買進股數", "賣出股數"}
        missing = expected_cols.difference(df.columns)
        if missing:
            print(f"Data missing columns: {sorted(missing)}", file=sys.stderr)
            return pd.DataFrame()

        numeric_cols = ["價格", "買進股數", "賣出股數"]
        for col in numeric_cols:
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
        df = df.loc[~df["日期"].isna()].copy()
        return df

    def _aggregate_by_broker(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        start_time = perf_counter()
        df = df.copy()
        if "warrant_code" not in df.columns:
            df["warrant_code"] = ""
        if "warrant_name" not in df.columns:
            df["warrant_name"] = ""
        if "underlying_code" not in df.columns:
            df["underlying_code"] = self.cfg.stock_code
        if "underlying_name" not in df.columns:
            df["underlying_name"] = self._underlying_name
        drop_cols = [
            "avg_buy_price",
            "avg_sell_price",
            "avg_price",
            "total_buy_shares",
            "total_net_shares",
        ]
        df.drop(columns=[col for col in drop_cols if col in df.columns], inplace=True, errors="ignore")

        numeric_cols = ["buy_shares", "sell_shares", "net_shares", "buy_amount", "sell_amount"]
        aggregated_numeric = df.groupby(["date", "broker"], as_index=False)[numeric_cols].sum()

        meta = df.groupby(["date", "broker"]).agg(
            warrant_code=pd.NamedAgg(
                column="warrant_code",
                aggfunc=lambda x: sorted({str(v).strip() for v in x if isinstance(v, str) and v.strip()}),
            ),
            warrant_name=pd.NamedAgg(
                column="warrant_name",
                aggfunc=lambda x: sorted({str(v).strip() for v in x if isinstance(v, str) and v.strip()}),
            ),
            underlying_code=pd.NamedAgg(
                column="underlying_code",
                aggfunc=lambda x: next((str(v).strip() for v in x if isinstance(v, str) and v.strip()), self.cfg.stock_code),
            ),
            underlying_name=pd.NamedAgg(
                column="underlying_name",
                aggfunc=lambda x: next((str(v).strip() for v in x if isinstance(v, str) and v.strip()), self._underlying_name),
            ),
        ).reset_index()

        aggregated = aggregated_numeric.merge(meta, on=["date", "broker"], how="left")
        aggregated["warrant_codes"] = aggregated["warrant_code"].apply(
            lambda codes: ",".join(codes) if isinstance(codes, list) else ""
        )
        aggregated["warrant_names"] = aggregated["warrant_name"].apply(
            lambda names: ",".join(names) if isinstance(names, list) else ""
        )
        aggregated.drop(columns=["warrant_code", "warrant_name"], inplace=True, errors="ignore")
        aggregated["underlying_code"] = aggregated["underlying_code"].fillna(self.cfg.stock_code)
        aggregated["underlying_name"] = aggregated["underlying_name"].fillna(self._underlying_name)
        logging.debug(
            "[%s] Aggregated to %d broker rows in %.2fs",
            self.cfg.stock_code,
            len(aggregated),
            perf_counter() - start_time,
        )
        return aggregated

    def _load_price_series(self, extra_days: int) -> pd.Series:
        if self.cfg.ohlc_root is None or self._analysis_window is None:
            return pd.Series(dtype=float)
        start_dt, end_dt = self._analysis_window
        extended_end = end_dt + pd.Timedelta(days=extra_days)
        price_df = self._load_price_window(start_dt, extended_end)
        if price_df.empty:
            logging.debug("[%s] No OHLC data for price series", self.cfg.stock_code)
            return pd.Series(dtype=float)
        series = price_df.set_index("date")["close"].astype(float)
        series.index = pd.to_datetime(series.index)
        series = series.sort_index()
        logging.debug("[%s] Loaded price series with %d points", self.cfg.stock_code, len(series))
        return series

    def _attach_forward_returns(self, df: pd.DataFrame, price_series: pd.Series) -> pd.DataFrame:
        start_time = perf_counter()
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])
        if price_series.empty:
            for horizon in self.FORWARD_HORIZONS:
                df[f"forward_ret_{horizon}d"] = np.nan
            logging.debug("[%s] No price data available for forward returns", self.cfg.stock_code)
            return df
        price_series = price_series.sort_index()
        for horizon in self.FORWARD_HORIZONS:
            future = price_series.shift(-horizon)
            returns = (future / price_series - 1) * 100
            ret_map = returns.to_dict()
            df[f"forward_ret_{horizon}d"] = df["date"].map(ret_map)
        logging.debug(
            "[%s] Calculated forward returns in %.2fs",
            self.cfg.stock_code,
            perf_counter() - start_time,
        )
        return df

    @staticmethod
    def _summarize_daily(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["buy_amount"] = df["價格"] * df["買進股數"]
        df["sell_amount"] = df["價格"] * df["賣出股數"]
        grouped = df.groupby("券商", as_index=False).agg(
            buy_shares=("買進股數", "sum"),
            sell_shares=("賣出股數", "sum"),
            buy_amount=("buy_amount", "sum"),
            sell_amount=("sell_amount", "sum"),
        )
        grouped["net_shares"] = grouped["buy_shares"] - grouped["sell_shares"]
        grouped["avg_buy_price"] = np.where(
            grouped["buy_shares"] > 0,
            grouped["buy_amount"] / grouped["buy_shares"],
            np.nan,
        )
        grouped["avg_sell_price"] = np.where(
            grouped["sell_shares"] > 0,
            grouped["sell_amount"] / grouped["sell_shares"],
            np.nan,
        )
        total_shares = grouped["buy_shares"] + grouped["sell_shares"]
        grouped["avg_price"] = np.where(
            total_shares > 0,
            (grouped["buy_amount"] + grouped["sell_amount"]) / total_shares,
            np.nan,
        )
        total_buy = grouped["buy_shares"].sum()
        total_net = grouped["net_shares"].sum()
        grouped["total_buy_shares"] = total_buy
        grouped["total_net_shares"] = total_net
        return grouped

    def _enrich_with_statistics(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")
        df.sort_values(["date", "broker"], inplace=True)
        df["broker_name"] = df["broker"].map(self._broker_lookup)

        stats = (
            df.groupby("broker")["net_shares"]
            .agg(["mean", "std", "count"])
            .rename(columns={"mean": "broker_mean", "std": "broker_std", "count": "history_count"})
        )
        enriched = df.merge(stats, left_on="broker", right_index=True, how="left")
        enriched["broker_std"] = enriched["broker_std"].replace(0, np.nan)
        enriched["z_score"] = (
            enriched["net_shares"] - enriched["broker_mean"]
        ) / enriched["broker_std"]

        daily_totals = df.groupby("date")["buy_shares"].transform("sum")
        daily_net_totals = df.groupby("date")["net_shares"].transform("sum")
        enriched["buy_share_ratio"] = np.where(
            daily_totals > 0,
            enriched["buy_shares"] / daily_totals,
            np.nan,
        )
        enriched["net_share_ratio"] = np.where(
            daily_net_totals != 0,
            enriched["net_shares"] / daily_net_totals,
            np.nan,
        )
        return enriched

    def _apply_window_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        start_time = perf_counter()
        window = max(1, self.cfg.detection_window)
        df = df.copy()
        df.sort_values(["broker", "date"], inplace=True)

        grouped = df.groupby("broker", group_keys=False)
        prior_counts = grouped.cumcount()
        df["appearances_to_date"] = prior_counts + 1
        df["prior_appearances"] = prior_counts

        df["positive_net"] = np.where(df["net_shares"] > 0, df["net_shares"], 0.0)
        df["net_positive_flag"] = (df["net_shares"] > 0).astype(int)

        rolling_net = grouped["positive_net"].rolling(window, min_periods=1).sum()
        df["window_net_sum"] = rolling_net.reset_index(level=0, drop=True)

        rolling_positive_days = (
            grouped["net_positive_flag"].rolling(window, min_periods=1).sum()
        )
        df["window_positive_days"] = rolling_positive_days.reset_index(level=0, drop=True)

        rolling_buy_sum = grouped["buy_shares"].rolling(window, min_periods=1).sum()
        df["window_buy_sum"] = rolling_buy_sum.reset_index(level=0, drop=True)

        rolling_max_net = grouped["positive_net"].rolling(window, min_periods=1).max()
        df["window_max_net"] = rolling_max_net.reset_index(level=0, drop=True)

        cumulative_positive_net = grouped["positive_net"].cumsum() - df["positive_net"]
        with np.errstate(divide="ignore", invalid="ignore"):
            history_mean = np.where(
                prior_counts > 0,
                cumulative_positive_net / prior_counts,
                np.nan,
            )
        df["history_net_mean"] = history_mean
        df["expected_window_net"] = df["history_net_mean"] * window
        df["window_net_ratio"] = np.where(
            df["expected_window_net"] > 0,
            df["window_net_sum"] / df["expected_window_net"],
            np.nan,
        )
        df["window_net_diff"] = df["window_net_sum"] - df["expected_window_net"]
        df["window_net_buy_ratio"] = np.where(
            df["window_buy_sum"] > 0,
            df["window_net_sum"] / df["window_buy_sum"],
            np.nan,
        )
        df["window_max_share"] = np.where(
            df["window_net_sum"] > 0,
            df["window_max_net"] / df["window_net_sum"],
            np.nan,
        )
        total_window_net = df.groupby("date")["window_net_sum"].transform("sum")

        def _hhi(series: pd.Series) -> float:
            total = series.sum()
            if total <= 0:
                return np.nan
            shares = series / total
            return float(np.square(shares).sum())

        df["window_concentration"] = df.groupby("date")["window_net_sum"].transform(_hhi)
        df["window_top_share"] = np.where(
            total_window_net > 0,
            df.groupby("date")["window_net_sum"].transform("max") / total_window_net,
            np.nan,
        )
        df["window_market_share"] = np.where(
            total_window_net > 0,
            df["window_net_sum"] / total_window_net,
            np.nan,
        )
        df.sort_values(["date", "broker"], inplace=True)
        df["broker_name"] = df["broker"].map(self._broker_lookup)
        min_len = max(0, self.cfg.min_hyphenated_length)
        df["is_branch"] = df["broker_name"].fillna("").apply(
            lambda name: self._is_branch_name(name, min_len)
        )
        logging.debug(
            "[%s] Generated window metrics for %d rows in %.2fs",
            self.cfg.stock_code,
            len(df),
            perf_counter() - start_time,
        )
        return df

    def _detect_anomalies(self, df: pd.DataFrame) -> pd.DataFrame:
        min_positive_days = max(1, (self.cfg.detection_window // 2) + 1)
        mask = (
            (df["prior_appearances"] >= self.cfg.min_history_days)
            & (df["window_net_sum"] >= self.cfg.min_window_net)
            & (df["window_positive_days"] >= min_positive_days)
            & (df["window_net_ratio"] >= self.cfg.window_ratio_threshold)
            & (df["expected_window_net"] > 0)
            & (df["window_net_buy_ratio"] >= self.cfg.min_net_buy_ratio)
            & (df["window_max_share"] <= self.cfg.max_single_day_share)
            & (df["is_branch"])
        )
        anomalies = df.loc[mask].copy()
        if anomalies.empty:
            return anomalies

        anomalies["anomaly_type"] = "window_net_spike"
        anomalies["anomaly_score"] = anomalies["window_net_ratio"]
        anomalies.sort_values(
            ["anomaly_score", "date", "broker"],
            ascending=[False, True, True],
            inplace=True,
        )
        if self.cfg.top_n > 0:
            anomalies = anomalies.head(self.cfg.top_n)
        anomalies.sort_values(["date", "broker"], inplace=True)
        return anomalies

    def _print_summary(self, anomalies: pd.DataFrame, enriched: pd.DataFrame) -> None:
        if self._analysis_window:
            start_dt, end_dt = self._analysis_window
            print(f"Analysis window: {start_dt.date()} to {end_dt.date()}")
        if anomalies.empty:
            print(
                "No brokers met the anomaly criteria. Adjust the rolling window or thresholds to broaden detection."
            )
            return

        cols = [
            "date",
            "broker",
            "broker_name",
            "underlying_code",
            "underlying_name",
            "warrant_codes",
            "warrant_names",
            "window_net_sum",
            "expected_window_net",
            "window_net_ratio",
            "window_positive_days",
            "window_buy_sum",
            "window_net_buy_ratio",
            "window_max_share",
            "window_concentration",
            "window_top_share",
            "window_market_share",
            "history_net_mean",
            "prior_appearances",
            "net_shares",
            "buy_shares",
            "sell_shares",
        ]
        for horizon in self.FORWARD_HORIZONS:
            col = f"forward_ret_{horizon}d"
            if col in anomalies.columns:
                cols.append(col)
        available = [col for col in cols if col in anomalies.columns]
        if "anomaly_score" in anomalies.columns and "anomaly_score" not in available:
            available.append("anomaly_score")
        print("Window net-buy anomalies (window sum vs historical average):")
        print(self._format_frame(anomalies[available]))

    def _maybe_plot(self, anomalies: pd.DataFrame, enriched: pd.DataFrame) -> None:
        if anomalies.empty:
            if self.cfg.plot or self.cfg.plot_path:
                print("Skipping plot because no anomalies were detected.")
            return
        if not self.cfg.plot and not self.cfg.plot_path:
            return

        try:
            import matplotlib
            if not self.cfg.plot:
                matplotlib.use("Agg", force=True)
            import matplotlib.pyplot as plt
        except ImportError as exc:  # pragma: no cover - optional dependency
            print(f"matplotlib is required for plotting: {exc}")
            return
        except Exception as exc:  # pragma: no cover - backend issues
            try:
                import matplotlib
                matplotlib.use("Agg", force=True)
                import matplotlib.pyplot as plt
            except Exception as inner_exc:  # pragma: no cover - fallback failed
                print(f"Failed to initialise matplotlib backend ({exc}); {inner_exc}")
                return

        if self.cfg.font_family:
            try:
                matplotlib.rcParams["font.family"] = [self.cfg.font_family]
            except Exception as exc:  # pragma: no cover
                print(f"Could not set font family '{self.cfg.font_family}': {exc}")

        anomalies = anomalies.copy()
        anomalies.sort_values(["date", "broker"], inplace=True)
        anomalies = anomalies.drop_duplicates(["date", "broker"])

        brokers = sorted(anomalies["broker"].dropna().unique())
        if not brokers:
            return

        full_timeline = (
            enriched["date"].drop_duplicates().sort_values().reset_index(drop=True)
        )

        if full_timeline.empty:
            return

        price_df = self._load_price_window(full_timeline.iloc[0], full_timeline.iloc[-1])
        price_df = price_df.sort_values("date").drop_duplicates("date") if not price_df.empty else pd.DataFrame()

        per_warrant_df = self._per_warrant_df.copy() if not self._per_warrant_df.empty else pd.DataFrame()

        for broker in brokers:
            display_name = self._broker_plot_label(str(broker))
            timeline = pd.DatetimeIndex(full_timeline)
            broker_series = (
                enriched.loc[
                    enriched["broker"] == broker,
                    [
                        "date",
                        "buy_shares",
                        "sell_shares",
                        "net_shares",
                        "window_concentration",
                        "window_market_share",
                    ],
                ]
                .drop_duplicates("date")
                .set_index("date")
                .reindex(timeline)
            )

            for col in ("buy_shares", "sell_shares", "net_shares"):
                broker_series[col] = broker_series.get(col, pd.Series(index=timeline, dtype=float)).fillna(0.0)

            price_series = (
                price_df.set_index("date")["close"].astype(float).reindex(timeline)
                if not price_df.empty
                else pd.Series(index=timeline, dtype=float)
            ).ffill().bfill()

            def _series_or_empty(column: str) -> pd.Series:
                series = broker_series.get(column)
                if series is None:
                    return pd.Series(index=timeline, dtype=float)
                return series.astype(float).ffill().bfill().fillna(0.0)

            concentration_series = _series_or_empty("window_concentration")
            market_share_series = _series_or_empty("window_market_share")

            broker_anomalies = pd.DataFrame()
            event_warrants: list[str] = []
            event_row = None
            codes_text = ""
            if anomalies is not None and not anomalies.empty:
                broker_anomalies = anomalies.loc[anomalies["broker"] == broker]
                if not broker_anomalies.empty:
                    event_row = broker_anomalies.iloc[0]
                    codes_text = str(event_row.get("warrant_codes", "") or "")
                    event_warrants = [code.strip() for code in codes_text.split(",") if code.strip()]

            include_warrant_panel = bool(event_warrants) and not per_warrant_df.empty

            panel_count = 2 + (len(event_warrants) if include_warrant_panel else 0)
            height_ratios = [3, 1] + ([1] * len(event_warrants) if include_warrant_panel else [])
            fig_height = 6 + len(event_warrants)
            fig, axes = plt.subplots(
                panel_count,
                1,
                figsize=(max(10, len(timeline) * 0.25), fig_height),
                gridspec_kw={"height_ratios": height_ratios},
                sharex=True,
            )

            if panel_count == 2:
                ax_volume, ax_concentration = axes
                warrant_axes: list[Any] = []
            else:
                ax_volume = axes[0]
                ax_concentration = axes[1]
                warrant_axes = list(axes[2:])

            x = np.arange(len(timeline))
            bar_width = 0.35
            ax_volume.bar(
                x,
                broker_series["buy_shares"].values,
                width=bar_width,
                color="#d62728",
                label="Buy Shares",
            )
            ax_volume.bar(
                x,
                -broker_series["sell_shares"].values,
                width=bar_width,
                color="#2ca02c",
                label="Sell Shares",
            )
            ax_volume.axhline(0, color="#666666", linewidth=0.8)
            ax_volume.set_ylabel("Shares")

            ax_price = ax_volume.twinx()
            ax_price.plot(
                x,
                price_series.values,
                color="#1f77b4",
                linewidth=2,
                label="Close Price",
            )
            ax_price.set_ylabel("Price")

            if not broker_anomalies.empty:
                for event_date in broker_anomalies["date"].unique():
                    event_date = pd.to_datetime(event_date)
                    try:
                        event_idx = int(np.where(timeline == event_date.normalize())[0][0])
                    except IndexError:
                        continue
                    ax_volume.axvline(
                        event_idx,
                        color="#ff7f0e",
                        linestyle="--",
                        linewidth=1.5,
                    )

            ax_volume.set_xticks(x)
            ax_volume.set_xticklabels([])

            handles_vol, labels_vol = ax_volume.get_legend_handles_labels()
            handles_price, labels_price = ax_price.get_legend_handles_labels()
            ax_price.legend(
                handles_vol + handles_price,
                labels_vol + labels_price,
                loc="upper left",
            )

            ax_concentration.plot(
                x,
                concentration_series.values,
                color="#9467bd",
                label="Concentration (HHI)",
            )
            ax_concentration.plot(
                x,
                market_share_series.values,
                color="#ff9896",
                label="Broker Share",
            )
            ax_concentration.set_ylim(0, 1)
            ax_concentration.set_ylabel("集中度" if self.cfg.font_family else "Concentration")
            ax_concentration.set_xticks(x)
            if include_warrant_panel:
                ax_concentration.set_xticklabels([])
            else:
                ax_concentration.set_xticklabels(
                    [ts.strftime("%Y-%m-%d") for ts in timeline], rotation=45, ha="right"
                )
            ax_concentration.grid(axis="y", linestyle="--", alpha=0.4)

            handles_conc, labels_conc = ax_concentration.get_legend_handles_labels()
            if handles_conc:
                ax_concentration.legend(loc="upper left")

            for axis, warrant_code in zip(warrant_axes, event_warrants):
                warrant_subset = pd.DataFrame()
                if not per_warrant_df.empty:
                    raw_subset = per_warrant_df.loc[
                        (per_warrant_df["broker"] == broker)
                        & (per_warrant_df["warrant_code"] == warrant_code)
                    ].copy()
                    if not raw_subset.empty:
                        warrant_subset = raw_subset.groupby("date").agg(
                            buy_shares=("buy_shares", "sum"),
                            sell_shares=("sell_shares", "sum"),
                        )
                        warrant_subset = warrant_subset.reindex(timeline, fill_value=0.0)
                if warrant_subset.empty:
                    warrant_subset = pd.DataFrame(
                        {
                            "buy_shares": np.zeros(len(timeline)),
                            "sell_shares": np.zeros(len(timeline)),
                        },
                        index=timeline,
                    )

                axis.bar(
                    x,
                    warrant_subset["buy_shares"].values,
                    width=bar_width,
                    color="#d62728",
                    label="Warrant Buy",
                )
                axis.bar(
                    x,
                    -warrant_subset["sell_shares"].values,
                    width=bar_width,
                    color="#2ca02c",
                    label="Warrant Sell",
                )
                axis.axhline(0, color="#666666", linewidth=0.8)
                meta = self._warrant_meta.get(warrant_code, {})
                title_code = warrant_code
                title_name = meta.get("權證簡稱", "")
                axis.set_ylabel(title_code)
                if title_name:
                    axis.set_title(title_name, fontsize=9, pad=6)
                axis.set_xticks(x)
                axis.set_xticklabels(
                    [ts.strftime("%Y-%m-%d") for ts in timeline], rotation=45, ha="right"
                )
                axis.grid(axis="y", linestyle="--", alpha=0.4)
                handles_share, labels_share = axis.get_legend_handles_labels()
                if handles_share:
                    axis.legend(loc="upper left")
                if not broker_anomalies.empty:
                    for event_date in broker_anomalies["date"].unique():
                        event_date = pd.to_datetime(event_date)
                        try:
                            event_idx = int(np.where(timeline == event_date.normalize())[0][0])
                        except IndexError:
                            continue
                        axis.axvline(
                            event_idx,
                            color="#ff7f0e",
                            linestyle="--",
                            linewidth=1.2,
                        )

            underlying_label = self.cfg.stock_code
            if self._underlying_name:
                underlying_label = f"{underlying_label} {self._underlying_name}"

            date_range = f"({timeline[0].strftime('%Y-%m-%d')} ~ {timeline[-1].strftime('%Y-%m-%d')})"
            if self.cfg.font_family:
                title = f"{underlying_label} – 券商 {display_name} {date_range}"
                anomaly_text = "異常日以橘色虛線標示"
            else:
                title = f"{underlying_label} – Broker {display_name} {date_range}"
                anomaly_text = "Anomaly day marked with orange dashed line"

            if not broker_anomalies.empty:
                title += f"\n{anomaly_text}"

            forward_parts: list[str] = []
            if event_row is not None:
                for horizon in self.FORWARD_HORIZONS:
                    col = f"forward_ret_{horizon}d"
                    if col in event_row and pd.notna(event_row[col]):
                        forward_parts.append(f"{horizon}d {float(event_row[col]):.1f}%")

            if forward_parts and event_row is not None:
                title += "\n" + ("後續報酬:" if self.cfg.font_family else "Forward returns:")
                title += " " + ", ".join(forward_parts)
            if codes_text and event_row is not None:
                title += "\n" + ("權證:" if self.cfg.font_family else "Warrants:") + f" {codes_text}"

            fig.suptitle(title)
            fig.tight_layout(rect=(0, 0, 1, 0.94))

            output_path = self._resolve_plot_output_for_broker(str(broker), display_name)
            if output_path is not None:
                output_path.parent.mkdir(parents=True, exist_ok=True)
                fig.savefig(output_path, dpi=200)
                print(f"Saved plot to {output_path}")

            if self.cfg.plot:
                plt.show()
            else:
                plt.close(fig)

    def _load_price_window(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        if self.cfg.ohlc_root is None:
            return pd.DataFrame()
        rows = []
        day = start.normalize()
        end_day = end.normalize()
        while day <= end_day:
            file_name = f"twse-{day.strftime('%Y%m%d')}.csv"
            csv_path = self.cfg.ohlc_root / file_name
            if csv_path.exists():
                try:
                    df = pd.read_csv(csv_path, dtype=str)
                except Exception as exc:  # pragma: no cover - defensive
                    print(f"Failed to parse {csv_path}: {exc}", file=sys.stderr)
                    df = pd.DataFrame()
                if not df.empty and "證券代號" in df.columns:
                    symbol_col = df["證券代號"].astype(str).str.strip()
                    mask = symbol_col == self.cfg.stock_code
                    if mask.any():
                        row = df.loc[mask].iloc[0]
                        close = (
                            str(row.get("收盤價", "0"))
                            .replace(",", "")
                            .replace(" ", "")
                        )
                        close_val = pd.to_numeric(close, errors="coerce")
                        if pd.notna(close_val):
                            rows.append({"date": day, "close": float(close_val)})
            day += pd.Timedelta(days=1)
        return pd.DataFrame(rows)

    def _resolve_plot_output_for_broker(self, broker: str, display_name: str) -> Optional[Path]:
        if self.cfg.plot_path is None:
            return None
        base = self.cfg.plot_path
        broker = str(broker)
        date_str = f"{self._analysis_window[0].strftime('%Y%m%d')}_{self._analysis_window[1].strftime('%Y%m%d')}" if self._analysis_window else "range"
        safe_type = "summary"
        safe_broker = re.sub(r"[^A-Za-z0-9]+", "_", display_name).strip("_") or (
            re.sub(r"[^A-Za-z0-9]+", "_", broker).strip("_") or "broker"
        )

        if base.exists() and base.is_dir():
            target_dir = base
        elif base.suffix:
            target_dir = base.parent
        else:
            target_dir = base
        target_dir.mkdir(parents=True, exist_ok=True)

        if base.exists() and base.is_dir():
            filename = f"{self.cfg.stock_code}_{date_str}_{safe_broker}_{safe_type}.png"
            return target_dir / filename

        if base.suffix:
            stem = base.stem
            suffix = base.suffix
            filename = f"{stem}_{self.cfg.stock_code}_{date_str}_{safe_broker}_{safe_type}{suffix}"
            return base.with_name(filename)

        filename = f"{self.cfg.stock_code}_{date_str}_{safe_broker}_{safe_type}.png"
        return target_dir / filename

    def _trading_days_between(self, start: pd.Timestamp, end: pd.Timestamp) -> Optional[pd.DatetimeIndex]:
        if self.cfg.ohlc_root is None:
            return None
        days: list[pd.Timestamp] = []
        current = start.normalize()
        end_day = end.normalize()
        while current <= end_day:
            file_path = self.cfg.ohlc_root / f"twse-{current.strftime('%Y%m%d')}.csv"
            if file_path.exists():
                days.append(current)
            current += pd.Timedelta(days=1)
        if not days:
            return None
        return pd.DatetimeIndex(sorted(set(days)))

    def _align_with_trading_days(
        self, df: pd.DataFrame, trading_days: pd.DatetimeIndex
    ) -> pd.DataFrame:
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.loc[~df["date"].isna()].copy()
        if df.empty:
            return df
        brokers = sorted(df["broker"].dropna().unique())
        if not brokers:
            return df
        full_index = pd.MultiIndex.from_product(
            [trading_days, brokers], names=["date", "broker"]
        )
        df = df.drop_duplicates(subset=["date", "broker"], keep="last")
        df = df.set_index(["date", "broker"])
        df = df.reindex(full_index)

        zero_fill_cols = [
            "buy_shares",
            "sell_shares",
            "net_shares",
            "buy_amount",
            "sell_amount",
            "total_buy_shares",
            "total_net_shares",
        ]
        for col in zero_fill_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0.0)
        for col in ["avg_buy_price", "avg_sell_price", "avg_price"]:
            if col in df.columns:
                df[col] = df[col].fillna(0.0)

        df = df.reset_index()
        default_underlying_name = self._underlying_name or ""
        if "warrant_codes" in df.columns:
            df["warrant_codes"] = df["warrant_codes"].fillna("")
        if "warrant_names" in df.columns:
            df["warrant_names"] = df["warrant_names"].fillna("")
        if "underlying_code" in df.columns:
            df["underlying_code"] = df["underlying_code"].fillna(self.cfg.stock_code)
        if "underlying_name" in df.columns:
            df["underlying_name"] = df["underlying_name"].fillna(default_underlying_name)
        return self._apply_broker_names(df)

    def _load_broker_lookup(self) -> dict[str, str]:
        path = self.cfg.broker_list_path
        if not path or not path.exists():
            return {}
        try:
            df = pd.read_csv(path, dtype=str)
        except Exception as exc:  # pragma: no cover - defensive
            print(f"Failed to parse broker list {path}: {exc}", file=sys.stderr)
            return {}

        df = df.replace({np.nan: ""})
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
        if code_col is None or name_col is None:
            print(
                f"Broker list {path} missing required columns for code/name; available: {list(df.columns)}",
                file=sys.stderr,
            )
            return {}

        df[code_col] = df[code_col].astype(str).str.strip()
        df[name_col] = df[name_col].astype(str).str.strip()
        return {
            row[code_col]: row[name_col]
            for _, row in df.iterrows()
            if row[code_col]
        }

    def _apply_broker_names(self, df: pd.DataFrame) -> pd.DataFrame:
        if "broker" not in df.columns:
            return df
        if not self._broker_lookup:
            return df
        df = df.copy()
        df["broker"] = df["broker"].astype(str)
        df["broker_name"] = df["broker"].map(self._broker_lookup)
        return df

    def _ensure_warrant_lookup(self) -> None:
        if self._warrant_lookup is not None:
            return
        self._warrant_lookup = _load_warrant_lookup(self.cfg.warrant_list_path)

    def _select_warrants_for_underlying(self, underlying: str) -> pd.DataFrame:
        if self._warrant_lookup is None or self._warrant_lookup.empty:
            return pd.DataFrame()
        df = self._warrant_lookup
        mask = df["標的代號"].astype(str).str.strip() == str(underlying)
        subset = df.loc[mask].copy()
        if subset.empty:
            return subset
        subset["權證代號"] = subset["權證代號"].astype(str).str.strip()
        subset = subset[subset["權證代號"].str.len() > 4]
        subset.drop_duplicates("權證代號", inplace=True)
        return subset

    def _broker_display_name(self, broker: str) -> str:
        broker = str(broker)
        return self._broker_lookup.get(broker, broker)

    def _broker_plot_label(self, broker: str) -> str:
        name = self._broker_lookup.get(str(broker), "")
        if not name:
            return str(broker)
        if self.cfg.font_family:
            return name
        return name if self._is_ascii(name) else str(broker)

    @staticmethod
    def _is_ascii(text: str) -> bool:
        try:
            text.encode("ascii")
            return True
        except UnicodeEncodeError:
            return False

    @staticmethod
    def _is_branch_name(name: str, min_branch_len: int) -> bool:
        if not name:
            return False
        for hyphen in ("-", "－", "—", "–"):
            if hyphen in name:
                parts = [part.strip() for part in name.split(hyphen) if part.strip()]
                if len(parts) >= 2 and all(len(part) >= min_branch_len for part in parts[1:]):
                    return True
        return False

    @staticmethod
    def _format_frame(df: pd.DataFrame) -> str:
        display = df.copy()
        if "date" in display.columns:
            display["date"] = display["date"].dt.strftime("%Y-%m-%d")
        numeric_cols = display.select_dtypes(include=["float", "float64", "int", "int64"]).columns
        display[numeric_cols] = display[numeric_cols].apply(pd.to_numeric, errors="coerce")
        display[numeric_cols] = display[numeric_cols].round(2)
        return display.to_string(index=False)


def _load_warrant_lookup(path: Path) -> pd.DataFrame:
    if not path.exists():
        logging.warning("Warrant list %s not found", path)
        return pd.DataFrame()
    try:
        df = pd.read_csv(path, dtype=str, encoding="utf-8-sig")
    except Exception as exc:  # pragma: no cover
        logging.warning("Failed to read warrant list %s: %s", path, exc)
        return pd.DataFrame()
    df = df.replace({np.nan: ""})
    if "權證代號" not in df.columns or "標的代號" not in df.columns:
        logging.warning("Warrant list missing required columns: %s", df.columns.tolist())
        return pd.DataFrame()
    df["權證代號"] = df["權證代號"].astype(str).str.strip()
    df["標的代號"] = df["標的代號"].astype(str).str.strip()
    if "權證類型" in df.columns:
        df = df[df["權證類型"].fillna("").str.contains("認購")]
    df = df[df["權證代號"].str.len() > 4]
    df.drop_duplicates(subset="權證代號", keep="last", inplace=True)
    return df


def _collect_stock_sources(data_root: Path, warrant_lookup: pd.DataFrame) -> list[str]:
    if not data_root.exists() or warrant_lookup.empty:
        return []
    suffixes = (".parquet", ".pq", ".parq")
    available_codes: set[str] = set()
    for suffix in suffixes:
        for path in data_root.glob(f"**/*{suffix}"):
            if path.is_file():
                available_codes.add(path.stem)

    underlyings: list[str] = []
    for underlying, group in warrant_lookup.groupby("標的代號"):
        codes = [code for code in group["權證代號"] if code in available_codes]
        if codes and underlying:
            underlyings.append(underlying)
    return sorted(set(underlyings))


def parse_args(argv: list[str]) -> AnalyzerConfig:
    parser = argparse.ArgumentParser(
        description="Highlight brokers with abnormal net buying activity for a stock.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("stock_code", nargs="?", help="Underlying stock code, e.g. 2330")
    parser.add_argument(
        "--data-root",
        default="data/bs_data",
        help="Root directory containing TWSE broker CSV folders (YYYYMMDD/twse).",
    )
    parser.add_argument(
        "--start-date",
        help="Inclusive start date (YYYYMMDD). Defaults to the earliest date available in the data.",
    )
    parser.add_argument(
        "--end-date",
        help="Inclusive end date (YYYYMMDD). Defaults to the latest date available in the data.",
    )
    parser.add_argument(
        "--min-history",
        type=int,
        default=5,
        help="Minimum number of prior appearances for the broker before testing the window statistic.",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=10,
        help="Maximum number of anomalies to display (0 for all).",
    )
    parser.add_argument(
        "--window",
        type=int,
        default=10,
        help="Rolling window (trading days) used for anomaly detection.",
    )
    parser.add_argument(
        "--window-ratio-threshold",
        type=float,
        default=2.0,
        help=(
            "Minimum multiple of the broker's historical average daily buy volume to "
            "flag the window as anomalous (computed on a window sum vs expected sum)."
        ),
    )
    parser.add_argument(
        "--min-window-net",
        type=float,
        default=100_000,
        help="Minimum淨買超總量 (window_sum) 才視為異常候選.",
    )
    parser.add_argument(
        "--min-net-buy-ratio",
        type=float,
        default=0.75,
        help="Window 內的淨買超總量至少要占買進總量的比率 (0-1).",
    )
    parser.add_argument(
        "--max-single-day-share",
        type=float,
        default=0.3,
        help="單一日淨買超量占整個 window 淨買超的最大比例，避免單日大單觸發異常.",
    )
    parser.add_argument(
        "--min-branch-length",
        type=int,
        default=1,
        help="券商名稱中分店標示(－)後面至少要保留的字數，否則視為總公司並排除。",
    )
    parser.add_argument(
        "--concentration-window",
        type=int,
        help="Rolling window (days) for computing chip concentration; default同 --window。",
    )
    parser.add_argument(
        "--ohlc-root",
        help="Directory containing daily TWSE OHLC CSV files (e.g. twse-YYYYMMDD.csv).",
    )
    parser.add_argument(
        "--font-family",
        help="Matplotlib font family name for rendering labels (set to a CJK-capable font for Chinese).",
    )
    parser.add_argument(
        "--broker-list",
        default="data/broker_list.csv",
        help="CSV file mapping broker codes to names (columns: 證券商代號, 證券商名稱, ...).",
    )
    parser.add_argument(
        "--warrant-list",
        default="data/warrant_list_dedup.csv",
        help="CSV file mapping warrant codes to underlying instruments.",
    )
    parser.add_argument(
        "--run-all",
        action="store_true",
        help="Loop through all stock parquet files under data-root and run anomaly detection.",
    )
    parser.add_argument(
        "--plot",
        action="store_true",
        help="Display a matplotlib chart for detected anomalies.",
    )
    parser.add_argument(
        "--save-plot",
        help="Save the anomalies chart to the specified file path.",
    )
    args = parser.parse_args(argv)

    data_root = Path(args.data_root).expanduser().resolve()
    if not args.run_all and not args.stock_code:
        parser.error("You must provide STOCK_CODE unless --run-all is specified")

    if args.start_date and (len(args.start_date) != 8 or not args.start_date.isdigit()):
        parser.error("--start-date must be in YYYYMMDD format")
    if args.end_date and (len(args.end_date) != 8 or not args.end_date.isdigit()):
        parser.error("--end-date must be in YYYYMMDD format")

    plot_path = Path(args.save_plot).expanduser().resolve() if args.save_plot else None
    ohlc_root = Path(args.ohlc_root).expanduser().resolve() if args.ohlc_root else None

    if args.window <= 0:
        parser.error("--window must be a positive integer")
    if args.window_ratio_threshold <= 0:
        parser.error("--window-ratio-threshold must be positive")
    if args.min_window_net < 0:
        parser.error("--min-window-net must be non-negative")
    if not (0 < args.min_net_buy_ratio <= 1):
        parser.error("--min-net-buy-ratio must be within (0, 1]")
    if not (0 < args.max_single_day_share <= 1):
        parser.error("--max-single-day-share must be within (0, 1]")
    if args.min_branch_length < 0:
        parser.error("--min-branch-length must be non-negative")
    if args.concentration_window is not None and args.concentration_window <= 0:
        parser.error("--concentration-window must be positive")

    broker_list_path = Path(args.broker_list).expanduser().resolve() if args.broker_list else None

    warrant_list_path = Path(args.warrant_list).expanduser().resolve()

    return AnalyzerConfig(
        stock_code=args.stock_code,
        data_root=data_root,
        warrant_list_path=warrant_list_path,
        start_date=args.start_date,
        end_date=args.end_date,
        min_history_days=args.min_history,
        top_n=args.top,
        plot=args.plot,
        plot_path=plot_path,
        detection_window=args.window,
        window_ratio_threshold=args.window_ratio_threshold,
        min_window_net=args.min_window_net,
        min_net_buy_ratio=args.min_net_buy_ratio,
        max_single_day_share=args.max_single_day_share,
        ohlc_root=ohlc_root,
        font_family=args.font_family,
        broker_list_path=broker_list_path,
        min_hyphenated_length=args.min_branch_length,
        concentration_window=args.concentration_window or args.window,
        run_all=args.run_all,
    )


def main(argv: Optional[list[str]] = None) -> int:
    cfg = parse_args(argv or sys.argv[1:])
    warrant_lookup = _load_warrant_lookup(cfg.warrant_list_path)
    if cfg.run_all:
        stock_sources = _collect_stock_sources(cfg.data_root, warrant_lookup)
        if not stock_sources:
            print(f"No eligible warrants found under {cfg.data_root}")
            return 1
        exit_code = 0
        for code in stock_sources:
            print(f"=== Processing underlying {code} ===")
            run_cfg = replace(cfg, stock_code=code, run_all=False)
            analyzer = BrokerAnomalyAnalyzer(run_cfg, warrant_lookup=warrant_lookup)
            result = analyzer.run()
            if result != 0:
                exit_code = result
        return exit_code

    analyzer = BrokerAnomalyAnalyzer(cfg, warrant_lookup=warrant_lookup)
    return analyzer.run()


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
