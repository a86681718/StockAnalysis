from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
import threading
from typing import Iterable

import numpy as np
import pandas as pd
import tqdm


@dataclass
class ProcessStats:
    processed_folders: int = 0
    expected_csv_files: int = 0
    processed_csv_files: int = 0
    failed_csv_files: int = 0
    updated_parquet_files: int = 0
    failed_folders: int = 0


@dataclass(frozen=True)
class FolderResult:
    ok: bool
    expected_csv_files: int
    processed_csv_files: int
    updated_parquet_files: int
    failed_filenames: tuple[str, ...] = ()
    error_summaries: dict[str, str] = field(default_factory=dict)
    folder_error: str | None = None

    @property
    def failed_csv_files(self) -> int:
        return len(self.failed_filenames)


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

    def run(self, folders: Iterable[Path]) -> tuple[ProcessStats, dict[str, FolderResult]]:
        folder_list = sorted(folders)
        stats = ProcessStats()
        folder_results: dict[str, FolderResult] = {}
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
            stats.expected_csv_files += result.expected_csv_files
            stats.processed_csv_files += result.processed_csv_files
            stats.failed_csv_files += result.failed_csv_files
            if result.ok:
                stats.processed_folders += 1
            else:
                stats.failed_folders += 1
        stats.updated_parquet_files = len(self._updated_stocks)
        return stats, folder_results

    def process_daily_folder(self, day_path: Path) -> FolderResult:
        try:
            date = pd.to_datetime(day_path.name, format="%Y%m%d", errors="raise")
        except ValueError:
            print(f"[WARN] skip non-date folder: {day_path}")
            return FolderResult(
                ok=False,
                expected_csv_files=0,
                processed_csv_files=0,
                updated_parquet_files=0,
                folder_error="Invalid date folder name; expected YYYYMMDD",
            )

        csv_files = sorted(day_path.glob("*.csv"))
        if not csv_files:
            print(f"[WARN] no csv files found: {day_path}")
            return FolderResult(
                ok=False,
                expected_csv_files=0,
                processed_csv_files=0,
                updated_parquet_files=0,
                folder_error="No CSV files found",
            )

        processed_count = 0
        updated_stocks: set[str] = set()
        failed_filenames: list[str] = []
        error_summaries: dict[str, str] = {}
        for csv_path in csv_files:
            stock_id = csv_path.stem
            try:
                frame = pd.read_csv(csv_path, dtype=str).fillna("")
                frame["日期"] = pd.to_datetime(date)
                for column in ["價格", "買進股數", "賣出股數"]:
                    if column in frame.columns:
                        frame[column] = self.safe_convert_numeric(frame[column]).astype(
                            "float64"
                        )
                self.write_parquet_incremental(stock_id, frame)
                processed_count += 1
                updated_stocks.add(stock_id)
            except Exception as exc:
                print(f"[WARN] failed to read {csv_path}: {exc}")
                failed_filenames.append(csv_path.name)
                error_summaries[csv_path.name] = f"{type(exc).__name__}: {exc}"

        return FolderResult(
            ok=not failed_filenames and processed_count == len(csv_files),
            expected_csv_files=len(csv_files),
            processed_csv_files=processed_count,
            updated_parquet_files=len(updated_stocks),
            failed_filenames=tuple(failed_filenames),
            error_summaries=error_summaries,
        )

    def write_parquet_incremental(self, stock_id: str, frame: pd.DataFrame) -> None:
        output_file = self.output_dir / f"{stock_id}.parquet"
        with self.get_lock(stock_id):
            frame = frame.copy()
            frame["日期"] = pd.to_datetime(frame["日期"])

            if output_file.exists():
                existing = pd.read_parquet(output_file)
                existing["日期"] = pd.to_datetime(existing["日期"], errors="coerce")
                for column in existing.columns:
                    if column not in frame.columns:
                        frame[column] = np.nan
                for column in frame.columns:
                    if column not in existing.columns:
                        existing[column] = np.nan
                frame = frame[existing.columns]
                key_columns = [
                    column
                    for column in ["日期", "券商", "價格", "買進股數", "賣出股數"]
                    if column in frame.columns
                ]
                combined = pd.concat([existing, frame], ignore_index=True)
                if key_columns:
                    combined.sort_values(key_columns, inplace=True)
                    combined.drop_duplicates(
                        subset=key_columns, inplace=True, keep="last"
                    )
                combined.sort_values("日期", inplace=True)
            else:
                combined = frame

            combined.to_parquet(output_file, index=False, compression="zstd")

        with self._stats_lock:
            self._updated_stocks.add(stock_id)

    def get_lock(self, stock_id: str) -> threading.Lock:
        with self._file_locks_lock:
            if stock_id not in self._file_locks:
                self._file_locks[stock_id] = threading.Lock()
            return self._file_locks[stock_id]

    @staticmethod
    def safe_convert_numeric(series: pd.Series) -> pd.Series:
        return pd.to_numeric(
            series.astype(str).str.replace(",", "", regex=False), errors="coerce"
        )
