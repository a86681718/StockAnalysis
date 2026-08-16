from __future__ import annotations

from pathlib import Path
import re

import pandas as pd


OHLC_PATTERN = re.compile(r"^(twse|tpex)-(\d{8})\.csv$")


def to_number(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.astype(str).str.replace(",", "", regex=False), errors="coerce"
    )


def load_ohlc_csv(path: Path) -> pd.DataFrame | None:
    match = OHLC_PATTERN.match(path.name)
    if not match:
        return None

    market, ymd = match.group(1), match.group(2)
    date = pd.to_datetime(ymd, format="%Y%m%d").date()
    frame = pd.read_csv(path, dtype=str)
    frame["date"] = date
    frame["market"] = market

    if market == "tpex":
        rename = {
            "代號": "symbol",
            "開盤": "open",
            "最高": "high",
            "最低": "low",
            "收盤": "close",
            "成交股數": "volume",
        }
    else:
        rename = {
            "證券代號": "symbol",
            "開盤價": "open",
            "最高價": "high",
            "最低價": "low",
            "收盤價": "close",
            "成交股數": "volume",
        }

    frame = frame.rename(columns=rename)
    keep_columns = [
        "symbol",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "market",
    ]
    frame = frame[
        [column for column in keep_columns if column in frame.columns]
    ].copy()

    for column in ["open", "high", "low", "close", "volume"]:
        if column in frame.columns:
            frame[column] = to_number(frame[column])

    return frame.dropna(subset=["symbol", "date", "close", "high", "low"])


def build_ohlc(input_dir: Path) -> pd.DataFrame:
    rows = []
    for path in sorted(input_dir.glob("*.csv")):
        frame = load_ohlc_csv(path)
        if frame is not None and not frame.empty:
            rows.append(frame)

    if not rows:
        raise RuntimeError(f"No OHLC rows loaded from {input_dir}")

    return (
        pd.concat(rows, ignore_index=True)
        .sort_values(["symbol", "date"])
        .reset_index(drop=True)
    )
