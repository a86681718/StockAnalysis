"""Detection-only broker branch accumulation pipeline.

The pipeline builds an auditable feature table at:

    underlying_stock_id, date, broker

It detects unusual branch-level net buying on the stock side, warrant side, or
both. It does not use future returns, entry/exit rules, or trading performance.
"""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import pandas as pd

from stockanalysis.config import ensure_dir, resolve_data, resolve_output


BROKER_COLUMNS = ["價格", "券商", "日期", "買進股數", "賣出股數"]
NUMERIC_DAILY_COLUMNS = [
    "stock_buy_volume",
    "stock_sell_volume",
    "stock_net_buy",
    "stock_buy_amount",
    "stock_sell_amount",
    "warrant_buy_volume",
    "warrant_sell_volume",
    "warrant_net_buy",
    "warrant_buy_amount",
    "warrant_sell_amount",
]
SIDE_PREFIXES = ("stock", "warrant", "combined")


@dataclass(frozen=True)
class DetectionConfig:
    broker_dirs: tuple[Path, ...]
    warrant_list_path: Path
    broker_list_path: Path
    ohlc_path: Path
    output_dir: Path
    start_date: Optional[pd.Timestamp]
    end_date: Optional[pd.Timestamp]
    symbols: Optional[set[str]]
    max_symbols: int
    exclude_etf: bool
    include_unknown_brokers: bool
    min_branch_suffix_len: int
    window_days: int
    min_history_days: int
    min_positive_days: int
    min_window_net: float
    min_window_net_ratio: float
    min_net_buy_ratio: float
    min_branch_window_share: float
    max_single_day_share: float
    combined_other_min_net: float
    combined_bonus: float
    max_avg_volume_20d: float
    top_n: int
    save_features: bool


def normalize_code(value: object) -> str:
    return str(value or "").strip().upper()


def parse_symbols(text: Optional[str]) -> Optional[set[str]]:
    if not text:
        return None
    symbols = {normalize_code(token) for token in text.replace("\n", ",").split(",")}
    symbols.discard("")
    return symbols or None


def parse_date(value: Optional[str]) -> Optional[pd.Timestamp]:
    if not value:
        return None
    return pd.Timestamp(value).normalize()


def is_branch_name(name: object, min_suffix_len: int = 2) -> bool:
    text = str(name or "").strip()
    if "-" not in text and "－" not in text:
        return False
    suffix = text.replace("－", "-").rsplit("-", 1)[-1].strip()
    return len(suffix) >= max(0, min_suffix_len)


def load_broker_lookup(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    df = pd.read_csv(path, dtype=str, encoding="utf-8-sig").fillna("")
    code_col = next((c for c in ("證券商代號", "代號", "broker_id", "code") if c in df.columns), None)
    name_col = next((c for c in ("證券商名稱", "名稱", "broker_name", "name") if c in df.columns), None)
    if not code_col or not name_col:
        return {}
    df[code_col] = df[code_col].map(normalize_code)
    df[name_col] = df[name_col].astype(str).str.strip()
    df = df[(df[code_col] != "") & (df[name_col] != "")]
    return dict(zip(df[code_col], df[name_col]))


def _first_nonempty(values: Iterable[object]) -> str:
    for value in values:
        text = str(value or "").strip()
        if text and text.lower() != "nan":
            return text
    return ""


def _parse_mapping_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series.astype(str).str.strip().replace({"": np.nan, "nan": np.nan}), errors="coerce")


def load_warrant_mapping(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(
            columns=[
                "warrant_id",
                "underlying_stock_id",
                "warrant_name",
                "underlying_name",
                "start_date",
                "end_date",
            ]
        )

    raw = pd.read_csv(path, dtype=str, encoding="utf-8-sig").fillna("")
    col_map = {
        "warrant_id": next((c for c in ("warrant_id", "權證代號") if c in raw.columns), None),
        "underlying_stock_id": next((c for c in ("underlying_stock_id", "標的代號") if c in raw.columns), None),
        "warrant_name": next((c for c in ("warrant_name", "權證簡稱") if c in raw.columns), None),
        "underlying_name": next((c for c in ("underlying_name", "標的名稱") if c in raw.columns), None),
    }
    if not col_map["warrant_id"] or not col_map["underlying_stock_id"]:
        raise ValueError(f"{path} must contain warrant and underlying columns")

    listing_col = next((c for c in ("start_date", "上市日期", "上櫃日期") if c in raw.columns), None)
    end_col = next((c for c in ("end_date", "最後交易日", "履約截止日") if c in raw.columns), None)

    out = pd.DataFrame(
        {
            "warrant_id": raw[col_map["warrant_id"]].map(normalize_code),
            "underlying_stock_id": raw[col_map["underlying_stock_id"]].map(normalize_code),
            "warrant_name": raw[col_map["warrant_name"]].astype(str).str.strip() if col_map["warrant_name"] else "",
            "underlying_name": raw[col_map["underlying_name"]].astype(str).str.strip() if col_map["underlying_name"] else "",
            "start_date": _parse_mapping_date(raw[listing_col]) if listing_col else pd.NaT,
            "end_date": _parse_mapping_date(raw[end_col]) if end_col else pd.NaT,
        }
    )
    out = out[(out["warrant_id"] != "") & (out["underlying_stock_id"] != "")].copy()
    if out.empty:
        return out

    grouped = (
        out.groupby("warrant_id", as_index=False)
        .agg(
            underlying_stock_id=("underlying_stock_id", _first_nonempty),
            warrant_name=("warrant_name", _first_nonempty),
            underlying_name=("underlying_name", _first_nonempty),
            start_date=("start_date", "min"),
            end_date=("end_date", "max"),
        )
        .reset_index(drop=True)
    )
    return grouped


def build_parquet_index(dirs: Iterable[Path]) -> dict[str, list[Path]]:
    index: dict[str, list[Path]] = {}
    for directory in dirs:
        if not directory.exists():
            continue
        for path in sorted(directory.glob("*.parquet")):
            index.setdefault(normalize_code(path.stem), []).append(path)
    return index


def is_default_stock_symbol(symbol: str) -> bool:
    return bool(re.fullmatch(r"\d{4}", symbol))


def _looks_like_etf(symbol: str) -> bool:
    return symbol.startswith("00")


def select_underlyings(
    parquet_index: dict[str, list[Path]],
    warrant_mapping: pd.DataFrame,
    symbols: Optional[set[str]],
    max_symbols: int,
    exclude_etf: bool,
) -> list[str]:
    if symbols is not None:
        selected = set(symbols)
    else:
        stock_symbols = {s for s in parquet_index if is_default_stock_symbol(s)}
        warrant_symbols = set()
        if not warrant_mapping.empty:
            warrant_symbols = {
                normalize_code(s)
                for s in warrant_mapping["underlying_stock_id"].dropna().unique()
                if is_default_stock_symbol(normalize_code(s))
            }
        selected = stock_symbols | warrant_symbols

    if exclude_etf:
        selected = {s for s in selected if not _looks_like_etf(s)}

    ordered = sorted(s for s in selected if s)
    if max_symbols > 0:
        ordered = ordered[:max_symbols]
    return ordered


def read_broker_parquet(path: Path, start: Optional[pd.Timestamp], end: Optional[pd.Timestamp]) -> pd.DataFrame:
    filters = []
    if start is not None:
        filters.append(("日期", ">=", start))
    if end is not None:
        filters.append(("日期", "<=", end))

    try:
        if filters:
            df = pd.read_parquet(path, columns=BROKER_COLUMNS, filters=filters)
        else:
            df = pd.read_parquet(path, columns=BROKER_COLUMNS)
    except Exception:
        df = pd.read_parquet(path)
        missing = [col for col in BROKER_COLUMNS if col not in df.columns]
        if missing:
            raise ValueError(f"{path} missing broker columns: {missing}")
        df = df[BROKER_COLUMNS].copy()

    if df.empty:
        return pd.DataFrame(columns=["date", "broker", "buy_volume", "sell_volume", "buy_amount", "sell_amount"])

    df = df.rename(
        columns={
            "日期": "date",
            "券商": "broker",
            "買進股數": "buy_volume",
            "賣出股數": "sell_volume",
            "價格": "price",
        }
    )
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
    df = df.loc[df["date"].notna()].copy()
    if start is not None:
        df = df[df["date"] >= start]
    if end is not None:
        df = df[df["date"] <= end]
    if df.empty:
        return pd.DataFrame(columns=["date", "broker", "buy_volume", "sell_volume", "buy_amount", "sell_amount"])

    df["broker"] = df["broker"].map(normalize_code)
    for col in ("price", "buy_volume", "sell_volume"):
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.replace(" ", "", regex=False)
            .replace({"": "0", "nan": "0", "None": "0"})
        )
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    df["buy_amount"] = df["price"] * df["buy_volume"]
    df["sell_amount"] = df["price"] * df["sell_volume"]
    grouped = (
        df.groupby(["date", "broker"], as_index=False)
        .agg(
            buy_volume=("buy_volume", "sum"),
            sell_volume=("sell_volume", "sum"),
            buy_amount=("buy_amount", "sum"),
            sell_amount=("sell_amount", "sum"),
        )
    )
    return grouped


def load_stock_daily(
    parquet_index: dict[str, list[Path]],
    underlyings: list[str],
    start: Optional[pd.Timestamp],
    end: Optional[pd.Timestamp],
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for i, underlying in enumerate(underlyings, start=1):
        for path in parquet_index.get(underlying, []):
            daily = read_broker_parquet(path, start, end)
            if daily.empty:
                continue
            daily.insert(0, "underlying_stock_id", underlying)
            frames.append(daily)
        if i % 250 == 0:
            print(f"loaded stock broker files for {i}/{len(underlyings)} underlyings", flush=True)

    if not frames:
        return pd.DataFrame(
            columns=[
                "underlying_stock_id",
                "date",
                "broker",
                "stock_buy_volume",
                "stock_sell_volume",
                "stock_net_buy",
                "stock_buy_amount",
                "stock_sell_amount",
            ]
        )

    out = pd.concat(frames, ignore_index=True)
    out = (
        out.groupby(["underlying_stock_id", "date", "broker"], as_index=False)
        .agg(
            stock_buy_volume=("buy_volume", "sum"),
            stock_sell_volume=("sell_volume", "sum"),
            stock_buy_amount=("buy_amount", "sum"),
            stock_sell_amount=("sell_amount", "sum"),
        )
    )
    out["stock_net_buy"] = out["stock_buy_volume"] - out["stock_sell_volume"]
    return out


def _join_limited(values: pd.Series, limit: int = 20) -> str:
    items = sorted(
        {
            str(v).strip()
            for v in values
            if str(v).strip() and str(v).strip().lower() != "nan"
        }
    )
    suffix = "" if len(items) <= limit else f",+{len(items) - limit} more"
    return ",".join(items[:limit]) + suffix


def load_warrant_daily(
    parquet_index: dict[str, list[Path]],
    warrant_mapping: pd.DataFrame,
    underlyings: list[str],
    start: Optional[pd.Timestamp],
    end: Optional[pd.Timestamp],
) -> pd.DataFrame:
    if warrant_mapping.empty:
        return pd.DataFrame(
            columns=[
                "underlying_stock_id",
                "date",
                "broker",
                "warrant_buy_volume",
                "warrant_sell_volume",
                "warrant_net_buy",
                "warrant_buy_amount",
                "warrant_sell_amount",
                "warrant_count",
                "warrant_ids",
                "warrant_names",
            ]
        )

    target_set = set(underlyings)
    mapping = warrant_mapping[warrant_mapping["underlying_stock_id"].isin(target_set)].copy()
    frames: list[pd.DataFrame] = []
    total = len(mapping)
    for i, row in enumerate(mapping.itertuples(index=False), start=1):
        warrant_id = normalize_code(row.warrant_id)
        paths = parquet_index.get(warrant_id, [])
        if not paths:
            continue
        row_start = getattr(row, "start_date", pd.NaT)
        row_end = getattr(row, "end_date", pd.NaT)
        effective_start = max(start, row_start) if start is not None and pd.notna(row_start) else (row_start if pd.notna(row_start) else start)
        effective_end = min(end, row_end) if end is not None and pd.notna(row_end) else (row_end if pd.notna(row_end) else end)
        for path in paths:
            daily = read_broker_parquet(path, effective_start, effective_end)
            if daily.empty:
                continue
            daily.insert(0, "underlying_stock_id", row.underlying_stock_id)
            daily["warrant_id"] = warrant_id
            daily["warrant_name"] = str(getattr(row, "warrant_name", "") or "").strip()
            frames.append(daily)
        if i % 1000 == 0:
            print(f"loaded warrant broker files for {i}/{total} mapped warrants", flush=True)

    if not frames:
        return pd.DataFrame(
            columns=[
                "underlying_stock_id",
                "date",
                "broker",
                "warrant_buy_volume",
                "warrant_sell_volume",
                "warrant_net_buy",
                "warrant_buy_amount",
                "warrant_sell_amount",
                "warrant_count",
                "warrant_ids",
                "warrant_names",
            ]
        )

    out = pd.concat(frames, ignore_index=True)
    out = (
        out.groupby(["underlying_stock_id", "date", "broker"], as_index=False)
        .agg(
            warrant_buy_volume=("buy_volume", "sum"),
            warrant_sell_volume=("sell_volume", "sum"),
            warrant_buy_amount=("buy_amount", "sum"),
            warrant_sell_amount=("sell_amount", "sum"),
            warrant_count=("warrant_id", "nunique"),
            warrant_ids=("warrant_id", _join_limited),
            warrant_names=("warrant_name", _join_limited),
        )
    )
    out["warrant_net_buy"] = out["warrant_buy_volume"] - out["warrant_sell_volume"]
    return out


def build_core_feature_table(
    stock_daily: pd.DataFrame,
    warrant_daily: pd.DataFrame,
    broker_lookup: dict[str, str],
    min_branch_suffix_len: int,
) -> pd.DataFrame:
    keys = ["underlying_stock_id", "date", "broker"]
    core = pd.merge(stock_daily, warrant_daily, on=keys, how="outer")
    if core.empty:
        return pd.DataFrame(columns=keys)

    core["underlying_stock_id"] = core["underlying_stock_id"].map(normalize_code)
    core["broker"] = core["broker"].map(normalize_code)
    core["date"] = pd.to_datetime(core["date"], errors="coerce").dt.normalize()
    core = core.loc[core["date"].notna()].copy()

    for col in NUMERIC_DAILY_COLUMNS:
        if col not in core.columns:
            core[col] = 0.0
        core[col] = pd.to_numeric(core[col], errors="coerce").fillna(0.0)

    for col in ("warrant_count",):
        if col not in core.columns:
            core[col] = 0
        core[col] = pd.to_numeric(core[col], errors="coerce").fillna(0).astype(int)
    for col in ("warrant_ids", "warrant_names"):
        if col not in core.columns:
            core[col] = ""
        core[col] = core[col].fillna("").astype(str)

    core["combined_buy_volume"] = core["stock_buy_volume"] + core["warrant_buy_volume"]
    core["combined_sell_volume"] = core["stock_sell_volume"] + core["warrant_sell_volume"]
    core["combined_net_buy"] = core["stock_net_buy"] + core["warrant_net_buy"]
    core["broker_name"] = core["broker"].map(broker_lookup).fillna("")
    core["is_branch"] = core["broker_name"].map(lambda name: is_branch_name(name, min_branch_suffix_len))
    return core.sort_values(keys).reset_index(drop=True)


def load_ohlc_context(
    path: Path,
    symbols: list[str],
    start: Optional[pd.Timestamp],
    end: Optional[pd.Timestamp],
) -> pd.DataFrame:
    columns = ["symbol", "date", "open", "high", "low", "close", "volume"]
    if not path.exists():
        return pd.DataFrame(columns=columns)

    ohlc = pd.read_parquet(path, columns=columns)
    if ohlc.empty:
        return ohlc
    symbol_set = set(symbols)
    ohlc["symbol"] = ohlc["symbol"].map(normalize_code)
    ohlc["date"] = pd.to_datetime(ohlc["date"], errors="coerce").dt.normalize()
    ohlc = ohlc.loc[ohlc["date"].notna() & ohlc["symbol"].isin(symbol_set)].copy()
    if start is not None:
        ohlc = ohlc[ohlc["date"] >= start]
    if end is not None:
        ohlc = ohlc[ohlc["date"] <= end]
    if ohlc.empty:
        return ohlc

    for col in ("open", "high", "low", "close", "volume"):
        ohlc[col] = pd.to_numeric(ohlc[col], errors="coerce")
    ohlc = ohlc.sort_values(["symbol", "date"]).reset_index(drop=True)
    grouped = ohlc.groupby("symbol", group_keys=False)
    ohlc["volume_ma20"] = grouped["volume"].transform(lambda s: s.rolling(20, min_periods=5).mean())
    ohlc["turnover_proxy"] = ohlc["close"] * ohlc["volume"]
    ohlc["turnover_ma20"] = grouped["turnover_proxy"].transform(lambda s: s.rolling(20, min_periods=5).mean())
    ohlc["ret_1d"] = grouped["close"].pct_change()
    ohlc["ret_5d"] = grouped["close"].pct_change(5)
    ohlc["ret_20d"] = grouped["close"].pct_change(20)
    return ohlc


def _hhi(series: pd.Series) -> float:
    total = float(series.sum())
    if total <= 0:
        return np.nan
    shares = series / total
    return float(np.square(shares).sum())


def _split_token_list(text: object) -> list[str]:
    tokens: list[str] = []
    for token in str(text or "").split(","):
        token = token.strip()
        if not token or token.lower() == "nan" or token.startswith("+"):
            continue
        tokens.append(token)
    return tokens


def _rolling_join_tokens(series: pd.Series, window_days: int, limit: int = 30) -> pd.Series:
    values = series.fillna("").astype(str).tolist()
    out: list[str] = []
    for i in range(len(values)):
        tokens: set[str] = set()
        for text in values[max(0, i - window_days + 1) : i + 1]:
            tokens.update(_split_token_list(text))
        ordered = sorted(tokens)
        suffix = "" if len(ordered) <= limit else f",+{len(ordered) - limit} more"
        out.append(",".join(ordered[:limit]) + suffix)
    return pd.Series(out, index=series.index)


def _add_side_window_metrics(frame: pd.DataFrame, prefix: str, window_days: int) -> None:
    net_col = f"{prefix}_net_buy"
    buy_col = f"{prefix}_buy_volume"
    pos_col = f"{prefix}_positive_net_buy"
    flag_col = f"{prefix}_positive_day"
    win_net_col = f"{prefix}_window_net_buy"
    win_buy_col = f"{prefix}_window_buy_volume"
    max_col = f"{prefix}_window_max_net_buy"

    frame[pos_col] = frame[net_col].clip(lower=0.0)
    frame[flag_col] = (frame[net_col] > 0).astype(float)

    grouped = frame.groupby("broker", group_keys=False)
    prior_days = grouped.cumcount()
    prior_positive_sum = grouped[pos_col].cumsum() - frame[pos_col]

    frame[f"{prefix}_prior_trading_days"] = prior_days
    frame[f"{prefix}_history_mean_net_buy"] = np.where(prior_days > 0, prior_positive_sum / prior_days, np.nan)
    frame[f"{prefix}_expected_window_net_buy"] = frame[f"{prefix}_history_mean_net_buy"] * window_days
    frame[win_net_col] = grouped[pos_col].rolling(window_days, min_periods=window_days).sum().reset_index(level=0, drop=True)
    frame[win_buy_col] = grouped[buy_col].rolling(window_days, min_periods=window_days).sum().reset_index(level=0, drop=True)
    frame[f"{prefix}_window_positive_days"] = grouped[flag_col].rolling(window_days, min_periods=window_days).sum().reset_index(level=0, drop=True)
    frame[max_col] = grouped[pos_col].rolling(window_days, min_periods=window_days).max().reset_index(level=0, drop=True)
    frame[f"{prefix}_window_net_ratio"] = frame[win_net_col] / frame[f"{prefix}_expected_window_net_buy"].replace(0, np.nan)
    frame[f"{prefix}_window_net_buy_ratio"] = frame[win_net_col] / frame[win_buy_col].replace(0, np.nan)
    frame[f"{prefix}_window_max_single_day_share"] = frame[max_col] / frame[win_net_col].replace(0, np.nan)

    date_group = frame.groupby("date")[win_net_col]
    total_window_net = date_group.transform("sum")
    frame[f"{prefix}_window_branch_share"] = frame[win_net_col] / total_window_net.replace(0, np.nan)
    frame[f"{prefix}_window_hhi"] = date_group.transform(_hhi)


def add_rolling_metrics(core: pd.DataFrame, ohlc: pd.DataFrame, cfg: DetectionConfig) -> pd.DataFrame:
    if core.empty:
        return core.copy()

    frames: list[pd.DataFrame] = []
    ohlc_dates = {
        symbol: pd.DatetimeIndex(sub["date"].dropna().sort_values().unique())
        for symbol, sub in ohlc.groupby("symbol")
    } if not ohlc.empty else {}

    for underlying, sub in core.groupby("underlying_stock_id", sort=True):
        dates = ohlc_dates.get(underlying)
        if dates is None or dates.empty:
            dates = pd.DatetimeIndex(sub["date"].dropna().sort_values().unique())
        if cfg.start_date is not None:
            dates = dates[dates >= cfg.start_date]
        if cfg.end_date is not None:
            dates = dates[dates <= cfg.end_date]
        if dates.empty:
            continue

        brokers = sorted(sub["broker"].dropna().unique())
        broker_name_map = sub.drop_duplicates("broker").set_index("broker")["broker_name"].to_dict()
        full_index = pd.MultiIndex.from_product([dates, brokers], names=["date", "broker"])
        frame = sub.set_index(["date", "broker"]).reindex(full_index).reset_index()
        frame["underlying_stock_id"] = underlying
        for col in NUMERIC_DAILY_COLUMNS:
            if col not in frame.columns:
                frame[col] = 0.0
            frame[col] = pd.to_numeric(frame[col], errors="coerce").fillna(0.0)
        frame["combined_buy_volume"] = frame["stock_buy_volume"] + frame["warrant_buy_volume"]
        frame["combined_sell_volume"] = frame["stock_sell_volume"] + frame["warrant_sell_volume"]
        frame["combined_net_buy"] = frame["stock_net_buy"] + frame["warrant_net_buy"]
        frame["broker_name"] = frame["broker"].map(broker_name_map).fillna("")
        frame["is_branch"] = frame["broker_name"].map(lambda name: is_branch_name(name, cfg.min_branch_suffix_len))
        frame["warrant_count"] = pd.to_numeric(frame.get("warrant_count", 0), errors="coerce").fillna(0).astype(int)
        for col in ("warrant_ids", "warrant_names"):
            if col not in frame.columns:
                frame[col] = ""
            frame[col] = frame[col].fillna("").astype(str)
        frame = frame.sort_values(["broker", "date"]).reset_index(drop=True)
        grouped_by_broker = frame.groupby("broker", group_keys=False)
        frame["warrant_window_ids"] = grouped_by_broker["warrant_ids"].apply(
            lambda s: _rolling_join_tokens(s, cfg.window_days)
        )
        frame["warrant_window_names"] = grouped_by_broker["warrant_names"].apply(
            lambda s: _rolling_join_tokens(s, cfg.window_days)
        )
        frame["warrant_window_count"] = frame["warrant_window_ids"].map(lambda text: len(_split_token_list(text)))

        for prefix in SIDE_PREFIXES:
            _add_side_window_metrics(frame, prefix, cfg.window_days)

        frames.append(frame.sort_values(["date", "broker"]))

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)
    if not ohlc.empty:
        context_cols = [
            "symbol",
            "date",
            "close",
            "volume",
            "volume_ma20",
            "turnover_proxy",
            "turnover_ma20",
            "ret_1d",
            "ret_5d",
            "ret_20d",
        ]
        context = ohlc[[c for c in context_cols if c in ohlc.columns]].rename(columns={"symbol": "underlying_stock_id"})
        out = out.merge(context, on=["underlying_stock_id", "date"], how="left")
    return out.sort_values(["underlying_stock_id", "date", "broker"]).reset_index(drop=True)


def _side_score(df: pd.DataFrame, prefix: str) -> pd.Series:
    window_net = pd.to_numeric(df[f"{prefix}_window_net_buy"], errors="coerce").fillna(0.0).clip(lower=0.0)
    ratio = pd.to_numeric(df[f"{prefix}_window_net_ratio"], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0).clip(lower=0.0)
    share = pd.to_numeric(df[f"{prefix}_window_branch_share"], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0).clip(lower=0.0)
    days = pd.to_numeric(df[f"{prefix}_window_positive_days"], errors="coerce").fillna(0.0).clip(lower=0.0)
    max_days = days.max(skipna=True)
    persistence = 1.0 + (days / max_days) if max_days > 0 else 1.0
    return np.log1p(window_net) * ratio * share * persistence


def _side_trigger(df: pd.DataFrame, prefix: str, cfg: DetectionConfig, eligible_broker: pd.Series) -> pd.Series:
    expected = pd.to_numeric(df[f"{prefix}_expected_window_net_buy"], errors="coerce")
    return (
        eligible_broker
        & (pd.to_numeric(df[f"{prefix}_prior_trading_days"], errors="coerce") >= cfg.min_history_days)
        & (pd.to_numeric(df[f"{prefix}_window_net_buy"], errors="coerce") >= cfg.min_window_net)
        & (pd.to_numeric(df[f"{prefix}_window_positive_days"], errors="coerce") >= cfg.min_positive_days)
        & (expected > 0)
        & (pd.to_numeric(df[f"{prefix}_window_net_ratio"], errors="coerce") >= cfg.min_window_net_ratio)
        & (pd.to_numeric(df[f"{prefix}_window_net_buy_ratio"], errors="coerce") >= cfg.min_net_buy_ratio)
        & (pd.to_numeric(df[f"{prefix}_window_branch_share"], errors="coerce") >= cfg.min_branch_window_share)
        & (pd.to_numeric(df[f"{prefix}_window_max_single_day_share"], errors="coerce") <= cfg.max_single_day_share)
    )


def build_event_explanation(row: pd.Series) -> str:
    broker_name = str(row.get("broker_name") or row.get("broker") or "")
    event_type = str(row.get("event_type") or "")
    parts = [
        f"{event_type} accumulation by {broker_name}",
        f"stock_window_net={row.get('stock_window_net_buy', np.nan):.0f}",
        f"warrant_window_net={row.get('warrant_window_net_buy', np.nan):.0f}",
        f"stock_ratio={row.get('stock_window_net_ratio', np.nan):.2f}",
        f"warrant_ratio={row.get('warrant_window_net_ratio', np.nan):.2f}",
        f"stock_share={row.get('stock_window_branch_share', np.nan):.2%}",
        f"warrant_share={row.get('warrant_window_branch_share', np.nan):.2%}",
    ]
    if pd.notna(row.get("volume_ma20", np.nan)):
        parts.append(f"volume_ma20={row.get('volume_ma20'):.0f}")
    if pd.notna(row.get("ret_5d", np.nan)):
        parts.append(f"prior_5d_return={row.get('ret_5d'):.2%}")
    warrant_ids = str(row.get("warrant_window_ids", "") or "").strip()
    if warrant_ids:
        parts.append(f"warrant_window_ids={warrant_ids}")
    return "; ".join(parts)


def detect_events(features: pd.DataFrame, cfg: DetectionConfig) -> pd.DataFrame:
    if features.empty:
        return features.copy()

    df = features.copy()
    eligible_broker = df["is_branch"].fillna(False)
    if cfg.include_unknown_brokers:
        eligible_broker = eligible_broker | (df["broker_name"].fillna("") == "")

    if cfg.max_avg_volume_20d > 0 and "volume_ma20" in df.columns:
        liquid_ok = df["volume_ma20"].isna() | (pd.to_numeric(df["volume_ma20"], errors="coerce") <= cfg.max_avg_volume_20d)
    else:
        liquid_ok = pd.Series(True, index=df.index)
    eligible_broker = eligible_broker & liquid_ok

    df["stock_trigger"] = _side_trigger(df, "stock", cfg, eligible_broker)
    df["warrant_trigger"] = _side_trigger(df, "warrant", cfg, eligible_broker)
    stock_active = (
        pd.to_numeric(df["stock_window_net_buy"], errors="coerce").fillna(0.0) >= cfg.combined_other_min_net
    ) & (pd.to_numeric(df["stock_window_positive_days"], errors="coerce").fillna(0.0) > 0)
    warrant_active = (
        pd.to_numeric(df["warrant_window_net_buy"], errors="coerce").fillna(0.0) >= cfg.combined_other_min_net
    ) & (pd.to_numeric(df["warrant_window_positive_days"], errors="coerce").fillna(0.0) > 0)
    df["combined_trigger"] = (df["stock_trigger"] & warrant_active) | (df["warrant_trigger"] & stock_active)

    df["stock_score"] = _side_score(df, "stock")
    df["warrant_score"] = _side_score(df, "warrant")
    df["combined_score"] = np.maximum(df["stock_score"], df["warrant_score"]) * (1.0 + cfg.combined_bonus)
    df["event_type"] = np.select(
        [df["combined_trigger"], df["stock_trigger"], df["warrant_trigger"]],
        ["combined", "stock", "warrant"],
        default="",
    )
    df["anomaly_score"] = np.select(
        [df["combined_trigger"], df["stock_trigger"], df["warrant_trigger"]],
        [df["combined_score"], df["stock_score"], df["warrant_score"]],
        default=0.0,
    )

    events = df[df["event_type"] != ""].copy()
    if events.empty:
        return events

    events["rank"] = events["anomaly_score"].rank(method="first", ascending=False).astype(int)
    events["explanation"] = events.apply(build_event_explanation, axis=1)
    events = events.sort_values(["anomaly_score", "date", "underlying_stock_id", "broker"], ascending=[False, True, True, True])
    if cfg.top_n > 0:
        events = events.head(cfg.top_n).copy()
    events["rank"] = np.arange(1, len(events) + 1)

    keep_cols = [
        "rank",
        "underlying_stock_id",
        "date",
        "broker",
        "broker_name",
        "event_type",
        "anomaly_score",
        "stock_trigger",
        "warrant_trigger",
        "combined_trigger",
        "stock_window_net_buy",
        "stock_expected_window_net_buy",
        "stock_window_net_ratio",
        "stock_window_positive_days",
        "stock_window_buy_volume",
        "stock_window_net_buy_ratio",
        "stock_window_branch_share",
        "stock_window_hhi",
        "warrant_window_net_buy",
        "warrant_expected_window_net_buy",
        "warrant_window_net_ratio",
        "warrant_window_positive_days",
        "warrant_window_buy_volume",
        "warrant_window_net_buy_ratio",
        "warrant_window_branch_share",
        "warrant_window_hhi",
        "combined_window_net_buy",
        "combined_window_positive_days",
        "warrant_count",
        "warrant_ids",
        "warrant_window_count",
        "warrant_window_ids",
        "warrant_window_names",
        "close",
        "volume",
        "volume_ma20",
        "turnover_ma20",
        "ret_1d",
        "ret_5d",
        "ret_20d",
        "explanation",
    ]
    return events[[c for c in keep_cols if c in events.columns]].reset_index(drop=True)


def write_report(events: pd.DataFrame, features: pd.DataFrame, cfg: DetectionConfig, output_path: Path) -> None:
    lines = [
        "# Broker Branch Abnormal Accumulation Events",
        "",
        "- scope: detection only; no future returns, entry/exit rules, strategy optimization, or backtest ranking",
        f"- rolling window: `{cfg.window_days}` trading days",
        f"- minimum history: `{cfg.min_history_days}` trading days",
        f"- minimum positive days: `{cfg.min_positive_days}`",
        f"- minimum window / expected ratio: `{cfg.min_window_net_ratio}`",
        f"- minimum branch window share: `{cfg.min_branch_window_share}`",
        f"- max 20d average volume filter: `{cfg.max_avg_volume_20d}` (`0` disables)",
        f"- feature rows: `{len(features)}`",
        f"- event rows: `{len(events)}`",
        "",
    ]
    if cfg.start_date is not None or cfg.end_date is not None:
        lines.append(f"- date range: `{cfg.start_date.date() if cfg.start_date is not None else 'begin'}` to `{cfg.end_date.date() if cfg.end_date is not None else 'end'}`")
        lines.append("")

    if events.empty:
        lines.append("No abnormal branch accumulation events met the configured thresholds.")
    else:
        lines.append("## Top Events")
        lines.append("")
        for row in events.head(30).itertuples(index=False):
            broker_label = getattr(row, "broker_name", "") or getattr(row, "broker", "")
            date_val = pd.Timestamp(getattr(row, "date")).date()
            lines.append(
                f"- `{getattr(row, 'rank')}` `{date_val}` `{getattr(row, 'underlying_stock_id')}` "
                f"`{broker_label}` `{getattr(row, 'event_type')}` score `{getattr(row, 'anomaly_score'):.2f}`: "
                f"{getattr(row, 'explanation')}"
            )
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_detection(cfg: DetectionConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    broker_lookup = load_broker_lookup(cfg.broker_list_path)
    warrant_mapping = load_warrant_mapping(cfg.warrant_list_path)
    parquet_index = build_parquet_index(cfg.broker_dirs)
    underlyings = select_underlyings(
        parquet_index=parquet_index,
        warrant_mapping=warrant_mapping,
        symbols=cfg.symbols,
        max_symbols=cfg.max_symbols,
        exclude_etf=cfg.exclude_etf,
    )
    if not underlyings:
        return pd.DataFrame(), pd.DataFrame()

    print(f"selected {len(underlyings)} underlyings", flush=True)
    ohlc = load_ohlc_context(cfg.ohlc_path, underlyings, cfg.start_date, cfg.end_date)
    stock_daily = load_stock_daily(parquet_index, underlyings, cfg.start_date, cfg.end_date)
    warrant_daily = load_warrant_daily(parquet_index, warrant_mapping, underlyings, cfg.start_date, cfg.end_date)
    core = build_core_feature_table(stock_daily, warrant_daily, broker_lookup, cfg.min_branch_suffix_len)
    features = add_rolling_metrics(core, ohlc, cfg)
    events = detect_events(features, cfg)

    out_dir = ensure_dir(cfg.output_dir)
    events.to_csv(out_dir / "broker_branch_accumulation_events.csv", index=False, encoding="utf-8-sig")
    events.to_parquet(out_dir / "broker_branch_accumulation_events.parquet", index=False)
    if cfg.save_features:
        features.to_parquet(out_dir / "broker_branch_accumulation_features.parquet", index=False)
    write_report(events, features, cfg, out_dir / "broker_branch_accumulation_report.md")
    return events, features


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Detect abnormal broker-branch accumulation without backtesting.")
    parser.add_argument(
        "--broker-dirs",
        nargs="+",
        type=Path,
        default=[resolve_data("bs_report", "parquet_twse"), resolve_data("bs_report", "parquet_tpex")],
        help="Directories containing one broker buy/sell parquet per stock or warrant.",
    )
    parser.add_argument("--warrant-list", type=Path, default=resolve_data("warrant", "warrant_list_dedup.csv"))
    parser.add_argument("--broker-list", type=Path, default=resolve_data("broker_list.csv"))
    parser.add_argument("--ohlc", type=Path, default=resolve_data("_derived", "ohlc.parquet"))
    parser.add_argument("--output-dir", type=Path, default=resolve_output("analysis", "broker_branch_accumulation"))
    parser.add_argument("--start-date", help="Inclusive start date, e.g. 2026-04-01.")
    parser.add_argument("--end-date", help="Inclusive end date, e.g. 2026-05-05.")
    parser.add_argument("--symbols", help="Comma-separated underlying stock ids to scan. Defaults to all 4-digit stocks.")
    parser.add_argument("--max-symbols", type=int, default=0, help="Limit scanned underlyings after sorting; 0 scans all.")
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
    parser.add_argument(
        "--max-avg-volume-20d",
        type=float,
        default=100_000_000.0,
        help="Drop event rows above this 20d average share volume proxy; set 0 to disable.",
    )
    parser.add_argument("--top-n", type=int, default=200)
    parser.add_argument("--save-features", action=argparse.BooleanOptionalAction, default=True)
    return parser.parse_args(argv)


def config_from_args(args: argparse.Namespace) -> DetectionConfig:
    return DetectionConfig(
        broker_dirs=tuple(Path(p).expanduser().resolve() for p in args.broker_dirs),
        warrant_list_path=Path(args.warrant_list).expanduser().resolve(),
        broker_list_path=Path(args.broker_list).expanduser().resolve(),
        ohlc_path=Path(args.ohlc).expanduser().resolve(),
        output_dir=Path(args.output_dir).expanduser().resolve(),
        start_date=parse_date(args.start_date),
        end_date=parse_date(args.end_date),
        symbols=parse_symbols(args.symbols),
        max_symbols=max(0, int(args.max_symbols)),
        exclude_etf=bool(args.exclude_etf),
        include_unknown_brokers=bool(args.include_unknown_brokers),
        min_branch_suffix_len=int(args.min_branch_suffix_len),
        window_days=max(1, int(args.window_days)),
        min_history_days=max(0, int(args.min_history_days)),
        min_positive_days=max(1, int(args.min_positive_days)),
        min_window_net=float(args.min_window_net),
        min_window_net_ratio=float(args.min_window_net_ratio),
        min_net_buy_ratio=float(args.min_net_buy_ratio),
        min_branch_window_share=float(args.min_branch_window_share),
        max_single_day_share=float(args.max_single_day_share),
        combined_other_min_net=float(args.combined_other_min_net),
        combined_bonus=float(args.combined_bonus),
        max_avg_volume_20d=float(args.max_avg_volume_20d),
        top_n=int(args.top_n),
        save_features=bool(args.save_features),
    )


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    cfg = config_from_args(args)
    events, _features = run_detection(cfg)
    if events.empty:
        print(f"No abnormal events found. Outputs written to {cfg.output_dir}")
    else:
        print(f"Detected {len(events)} abnormal events. Outputs written to {cfg.output_dir}")
        print(events.head(min(20, len(events))).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
