from __future__ import annotations

import traceback
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
import threading

import numpy as np
import pandas as pd
import tqdm


@dataclass
class ProcessStats:
    processed_folders: int = 0
    processed_csv_files: int = 0
    updated_parquet_files: int = 0
    failed_folders: int = 0


class BsReportEtl:
    def __init__(self, input_dir: Path, output_dir: Path, max_workers: int = 6):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.max_workers = max_workers
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._file_locks: dict[str, threading.Lock] = {}
        self._file_locks_lock = threading.Lock()
        self._stats_lock = threading.Lock()
        self._updated_stocks: set[str] = set()

    def run(self, folders: Iterable[Path]) -> tuple[ProcessStats, dict[str, dict[str, int | bool]]]:
        folder_list = sorted(folders)
        stats = ProcessStats()
        folder_results: dict[str, dict[str, int | bool]] = {}
        if not folder_list:
            return stats, folder_results

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            results = list(
                tqdm.tqdm(
                    executor.map(self.process_daily_folder, folder_list),
                    total=len(folder_list),
                )
            )

        for folder, result in zip(folder_list, results):
            folder_results[folder.name] = result
            if result["ok"]:
                stats.processed_folders += 1
                stats.processed_csv_files += result["csv_files"]
            else:
                stats.failed_folders += 1
        stats.updated_parquet_files = len(self._updated_stocks)
        return stats, folder_results

    def process_daily_folder(self, day_path: Path) -> dict[str, int | bool]:
        try:
            date = pd.to_datetime(day_path.name, format="%Y%m%d", errors="raise")
        except ValueError:
            print(f"[WARN] skip non-date folder: {day_path}")
            return {"ok": False, "csv_files": 0}

        csv_files = sorted(day_path.glob("*.csv"))
        if not csv_files:
            print(f"[WARN] no csv files found: {day_path}")
            return {"ok": False, "csv_files": 0}

        processed_count = 0
        for csv_path in csv_files:
            stock_id = csv_path.stem
            try:
                df = pd.read_csv(csv_path, dtype=str).fillna("")
                df["日期"] = pd.to_datetime(date)
                for col in ["價格", "買進股數", "賣出股數"]:
                    if col in df.columns:
                        df[col] = self.safe_convert_numeric(df[col]).astype("float64")
                self.write_parquet_incremental(stock_id, df)
                processed_count += 1
            except Exception as exc:
                print(f"[WARN] failed to read {csv_path}: {exc}")
                traceback.print_exc()
        return {"ok": True, "csv_files": processed_count}

    def write_parquet_incremental(self, stock_id: str, df: pd.DataFrame) -> None:
        out_file = self.output_dir / f"{stock_id}.parquet"
        lock = self.get_lock(stock_id)
        with lock:
            df = df.copy()
            df["日期"] = pd.to_datetime(df["日期"])

            if out_file.exists():
                existing_df = pd.read_parquet(out_file)
                existing_df["日期"] = pd.to_datetime(existing_df["日期"], errors="coerce")

                for col in existing_df.columns:
                    if col not in df.columns:
                        df[col] = np.nan
                for col in df.columns:
                    if col not in existing_df.columns:
                        existing_df[col] = np.nan

                df = df[existing_df.columns]
                key_cols = [c for c in ["日期", "券商", "價格", "買進股數", "賣出股數"] if c in df.columns]
                combined = pd.concat([existing_df, df], ignore_index=True)
                if key_cols:
                    combined.sort_values(key_cols, inplace=True)
                    combined.drop_duplicates(subset=key_cols, inplace=True, keep="last")
                combined.sort_values("日期", inplace=True)
            else:
                combined = df

            combined.to_parquet(out_file, index=False, compression="zstd")

        with self._stats_lock:
            self._updated_stocks.add(stock_id)

    def get_lock(self, stock_id: str) -> threading.Lock:
        with self._file_locks_lock:
            if stock_id not in self._file_locks:
                self._file_locks[stock_id] = threading.Lock()
            return self._file_locks[stock_id]

    @staticmethod
    def safe_convert_numeric(series: pd.Series) -> pd.Series:
        return pd.to_numeric(series.astype(str).str.replace(",", "", regex=False), errors="coerce")
