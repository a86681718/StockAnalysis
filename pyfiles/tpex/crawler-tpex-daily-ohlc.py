import argparse
import os
import json
from datetime import datetime, timedelta
from typing import Optional
from urllib import parse

import pandas as pd
import requests

DATA_FOLDER = '/Users/fang/StockAnalysis/data'
SUBFOLDER = 'ohlc'
FILENAME_PREFIX = 'tpex-'
DATE_FMT = '%Y%m%d'
API_DATE_FMT = '%Y/%m/%d'


def ensure_output_dir() -> str:
    path = os.path.join(DATA_FOLDER, SUBFOLDER)
    os.makedirs(path, exist_ok=True)
    return path


def latest_existing_date(path: str) -> datetime:
    files = [f for f in os.listdir(path) if f.startswith(FILENAME_PREFIX) and f.endswith('.csv')]
    if not files:
        return datetime.now() - timedelta(days=1)
    dates = [datetime.strptime(f[len(FILENAME_PREFIX):len(FILENAME_PREFIX) + 8], DATE_FMT) for f in files]
    return max(dates)


def fetch_tpex_daily(target: datetime) -> pd.DataFrame:
    api_date = target.strftime(API_DATE_FMT)
    url = f"https://www.tpex.org.tw/www/zh-tw/afterTrading/otc?date={parse.quote_plus(api_date)}&type=AL&id=&response=json"
    resp = requests.post(url, timeout=30)
    resp.raise_for_status()
    payload = json.loads(resp.text)
    tables = payload.get('tables', [])
    if not tables:
        return pd.DataFrame()
    table = tables[0]
    columns = [col.strip() for col in table.get('fields', [])]
    data = table.get('data', [])
    return pd.DataFrame(data, columns=columns)


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch TPEX daily OHLC data")
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
            df = fetch_tpex_daily(current_dt)
        except Exception as exc:
            print(f"Fetch failed for {date_str}: {exc}")
            continue
        if df.empty:
            current_dt += timedelta(days=1)
            continue
        output_path = os.path.join(output_dir, f"{FILENAME_PREFIX}{date_str}.csv")
        df.to_csv(output_path, index=False, encoding='utf-8')
        print(f"Saved {output_path}")
        current_dt += timedelta(days=1)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
