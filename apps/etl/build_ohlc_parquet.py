from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import pandas as pd


_SRC_ROOT = Path(__file__).resolve().parents[2] / "src"
if str(_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(_SRC_ROOT))

from stockanalysis.config import ensure_dir, resolve_data


OHLC_PATTERN = re.compile(r"^(twse|tpex)-(\d{8})\.csv$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build data/_derived/ohlc.parquet from raw TWSE/TPEX OHLC CSV files.")
    parser.add_argument("--input-dir", type=Path, default=resolve_data("ohlc"))
    parser.add_argument("--output", type=Path, default=resolve_data("_derived", "ohlc.parquet"))
    return parser.parse_args()


def to_number(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.replace(",", "", regex=False), errors="coerce")


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
    keep_cols = ["symbol", "date", "open", "high", "low", "close", "volume", "market"]
    frame = frame[[column for column in keep_cols if column in frame.columns]].copy()

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

    return pd.concat(rows, ignore_index=True).sort_values(["symbol", "date"]).reset_index(drop=True)


def main() -> None:
    args = parse_args()
    ohlc = build_ohlc(args.input_dir)
    ensure_dir(args.output.parent)
    ohlc.to_parquet(args.output, index=False)

    print(f"wrote {args.output}")
    print(f"rows {len(ohlc)}")
    print(f"symbols {ohlc['symbol'].nunique()}")
    print(f"date_min {ohlc['date'].min()}")
    print(f"date_max {ohlc['date'].max()}")


if __name__ == "__main__":
    main()
