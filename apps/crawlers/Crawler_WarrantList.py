import argparse
import logging
from calendar import monthrange
from datetime import date
from io import StringIO
from pathlib import Path
from typing import List

import pandas as pd
import requests
import urllib3
from bs4 import BeautifulSoup
from stockanalysis.config import resolve_data

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

HTML_PARSER = "html.parser"
MARKET_TYPES = ["1", "2"]  # 1: 上市, 2: 上櫃
EXPIRED_FLAGS = ["0", "1"]  # 0: 已到期, 1: 未到期


def resolve_output_path() -> Path:
    output_path = resolve_data("warrant", "warrant_list_dedup.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    return output_path


def clean_code(value: str) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if text.startswith('="') and text.endswith('"'):
        text = text[2:-1]
    text = text.replace('"', '').replace("'", "").replace('=', '')
    return text.strip()


def fetch_warrant_dataframe(
    market_type: str,
    expired_flag: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    resp = requests.post(
        "https://mopsov.twse.com.tw/mops/web/ajax_t90sb01?encodeURIComponent=1&step=1&firstin=1&off=1",
        data={
            "r": market_type,
            "rc": expired_flag,
            "start_date": start_date,
            "end_date": end_date,
        },
        verify=False,
    )
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, HTML_PARSER)
    filename = next(
        (element.get("value") for element in soup.find_all("input", type="hidden") if element.get("name") == "filename"),
        "",
    )
    if not filename:
        logging.warning("No filename returned for market_type=%s expired=%s", market_type, expired_flag)
        return pd.DataFrame()

    csv_resp = requests.post(
        "https://mopsov.twse.com.tw/server-java/t105sb02?firstin=true&step=10",
        params={"filename": filename},
        verify=False,
    )
    csv_resp.raise_for_status()
    decoded = csv_resp.content.decode("cp950", errors="ignore")
    df = pd.read_csv(StringIO(decoded))
    if '權證代號' in df.columns:
        df['權證代號'] = df['權證代號'].apply(clean_code)
    df["market_type"] = market_type
    df["expired_flag"] = expired_flag
    return df


def main() -> None:
    output_path = resolve_output_path()
    frames: List[pd.DataFrame] = []
    for market_type in MARKET_TYPES:
        for expired_flag in EXPIRED_FLAGS:
            logging.info(
                "Fetching market_type=%s expired=%s for range %s-%s",
                market_type,
                expired_flag,
                args.start_date,
                args.end_date,
            )
            df = fetch_warrant_dataframe(market_type, expired_flag, args.start_date, args.end_date)
            if not df.empty:
                frames.append(df)

    if not frames:
        logging.warning("No warrant data retrieved in the specified range.")
        return

    combined = pd.concat(frames, ignore_index=True)
    deduped = combined.drop_duplicates(subset=["權證代號", "權證簡稱"]).reset_index(drop=True)
    logging.info("Aggregated %d records, reduced to %d unique warrants", len(combined), len(deduped))

    read_csv_kwargs = {"encoding": "utf-8-sig", "dtype": str}
    if output_path.exists():
        logging.info("Existing file %s detected, merging before saving", output_path)
        existing = pd.read_csv(output_path, **read_csv_kwargs)
        merged = pd.concat([existing, deduped], ignore_index=True)
        deduped = merged.drop_duplicates(subset=["權證代號", "權證簡稱"]).reset_index(drop=True)
        logging.info("After merging with existing data: %d unique rows", len(deduped))

    deduped.to_csv(output_path, index=False, encoding="utf-8-sig")
    logging.info("Saved deduplicated list to %s", output_path)


def roc_year_month(target_date: date) -> str:
    roc_year = target_date.year - 1911
    return f"{roc_year:03d}{target_date.month:02d}"


def date_from_roc_year_month(roc_str: str) -> date:
    if len(roc_str) < 4:
        raise ValueError("ROC date must be at least 4 digits (YYYMM)")
    roc_year = int(roc_str[:-2])
    month = int(roc_str[-2:])
    return date(roc_year + 1911, month, 1)


def months_before(target_date: date, months: int) -> date:
    total_months = target_date.year * 12 + target_date.month - 1 - months
    year = total_months // 12
    month = total_months % 12 + 1
    day = min(target_date.day, monthrange(year, month)[1])
    return date(year, month, day)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch TWSE warrant list and deduplicate by code/name")
    parser.add_argument("--start-date", help="ROC 年月 (例如 11404)")
    parser.add_argument("--end-date", help="ROC 年月 (例如 11410)")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    today = date.today()
    default_end_date = roc_year_month(today)
    end_roc = args.end_date or default_end_date

    try:
        end_date_obj = date_from_roc_year_month(end_roc)
    except ValueError as exc:
        raise SystemExit(f"Invalid --end-date: {exc}")

    if args.start_date:
        start_roc = args.start_date
    else:
        six_months_prior = months_before(end_date_obj, 6)
        start_roc = roc_year_month(six_months_prior)

    # attach resolved dates to args for downstream use
    args.start_date = start_roc
    args.end_date = end_roc

    logging.info("Using date range %s ~ %s", start_roc, end_roc)
    main()
