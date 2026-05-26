import argparse
import json
import os
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import requests

from stockanalysis.config import resolve_data

DATA_FOLDER = str(resolve_data())
SUBFOLDER = "ohlc"
FILENAME_PREFIX = "twse-"
DATE_FMT = "%Y%m%d"
API_DATE_FMT = "%Y%m%d"


def ensure_output_dir() -> str:
    path = os.path.join(DATA_FOLDER, SUBFOLDER)
    os.makedirs(path, exist_ok=True)
    return path


def latest_existing_date(path: str) -> datetime:
    files = [f for f in os.listdir(path) if f.startswith(FILENAME_PREFIX) and f.endswith(".csv")]
    if not files:
        return datetime.now() - timedelta(days=1)
    dates = [datetime.strptime(f[len(FILENAME_PREFIX):len(FILENAME_PREFIX) + 8], DATE_FMT) for f in files]
    return max(dates)


def fetch_twse_daily(target: datetime) -> pd.DataFrame:
    api_date = target.strftime(API_DATE_FMT)
    url = f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={api_date}&type=ALL&response=json"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    payload = json.loads(resp.content.decode("utf8"))
    tables = payload.get("tables", [])
    if not tables:
        return pd.DataFrame()

    for table in tables:
        title = table.get("title", "")
        if "每日收盤行情" in title:
            fields = table.get("fields", [])
            data = table.get("data", [])
            return pd.DataFrame(data=data, columns=fields)

    return pd.DataFrame()


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch TWSE daily OHLC data")
    parser.add_argument(
        "--start-date",
        help="Inclusive start date in yyyy/mm/dd format",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    output_dir = ensure_output_dir()
    if args.start_date:
        try:
            start_dt = datetime.strptime(args.start_date, "%Y/%m/%d")
        except ValueError:
            print("Invalid --start-date; expected yyyy/mm/dd")
            return 1
    else:
        start_dt = latest_existing_date(output_dir) + timedelta(days=1)

    end_date = datetime.now().date()
    current_dt = start_dt

    if current_dt.date() > end_date:
        return 0

    while current_dt.date() <= end_date:
        date_str = current_dt.strftime(DATE_FMT)
        try:
            df = fetch_twse_daily(current_dt)
        except Exception as exc:
            print(f"Fetch failed for {date_str}: {exc}")
            current_dt += timedelta(days=1)
            continue
        if df.empty:
            current_dt += timedelta(days=1)
            continue
        output_path = os.path.join(output_dir, f"{FILENAME_PREFIX}{date_str}.csv")
        df.to_csv(output_path, index=False, encoding="utf-8")
        print(f"Saved {output_path}")
        current_dt += timedelta(days=1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
