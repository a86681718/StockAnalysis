from __future__ import annotations

import argparse
import contextlib
import io
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from unittest import mock

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
ETL_DIR = ROOT / "apps/etl"
FIXTURES = Path(__file__).resolve().parent / "fixtures"
if str(ETL_DIR) not in sys.path:
    sys.path.insert(0, str(ETL_DIR))

from bs_report_pipeline import BsReportEtl  # noqa: E402
import run_bs_report_etl as runner  # noqa: E402


def make_args(**overrides) -> argparse.Namespace:
    values = {
        "archive": True,
        "dry_run": False,
        "gcs_base_uri": "gs://unused-in-tests/bs_report",
        "limit": None,
        "market": "tpex",
        "max_workers": 1,
        "since": None,
        "sync": False,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def make_paths(root: Path) -> runner.MarketPaths:
    paths = runner.MarketPaths(
        market="tpex",
        inbox_dir=root / "inbox/tpex",
        archive_dir=root / "archive/tpex",
        output_dir=root / "parquet_tpex",
        manifest_path=root / "manifests/tpex_processed.json",
    )
    paths.inbox_dir.mkdir(parents=True)
    paths.archive_dir.mkdir(parents=True)
    paths.output_dir.mkdir(parents=True)
    paths.manifest_path.parent.mkdir(parents=True)
    return paths


class FolderOutcomeTests(unittest.TestCase):
    def test_empty_and_invalid_date_folders_fail_without_csv_failures(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            empty = root / "20260815"
            invalid = root / "not-a-date"
            empty.mkdir()
            invalid.mkdir()
            etl = BsReportEtl(root, root / "output", max_workers=1)

            with contextlib.redirect_stdout(io.StringIO()):
                empty_result = etl.process_daily_folder(empty)
                invalid_result = etl.process_daily_folder(invalid)

            self.assertFalse(empty_result.ok)
            self.assertEqual(empty_result.failed_csv_files, 0)
            self.assertEqual(empty_result.folder_error, "No CSV files found")
            self.assertFalse(invalid_result.ok)
            self.assertEqual(invalid_result.failed_csv_files, 0)
            self.assertIn("YYYYMMDD", invalid_result.folder_error or "")

    def test_existing_parquet_keeps_sort_and_dedup_behavior(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            day = root / "input/20260815"
            shutil.copytree(FIXTURES / "etl/complete/20260815", day)
            etl = BsReportEtl(root / "input", root / "output", max_workers=1)

            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                first = etl.process_daily_folder(day)
                second = etl.process_daily_folder(day)

            frame = pd.read_parquet(root / "output/2330.parquet")
            self.assertTrue(first.ok)
            self.assertTrue(second.ok)
            self.assertEqual(len(frame), 2)
            self.assertTrue(frame["日期"].is_monotonic_increasing)


class MarketCompletionTests(unittest.TestCase):
    def test_partial_failure_stays_in_inbox_and_records_manifest_details(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            paths = make_paths(Path(temp_dir))
            day = paths.inbox_dir / "20260815"
            shutil.copytree(FIXTURES / "etl/partial_failure/20260815", day)

            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                summary = runner.process_market(paths, make_args())

            manifest = json.loads(paths.manifest_path.read_text())
            entry = manifest["20260815"]
            self.assertTrue(day.exists())
            self.assertFalse((paths.archive_dir / day.name).exists())
            self.assertEqual(entry["status"], "failed")
            self.assertEqual(entry["expected_csv_files"], 2)
            self.assertEqual(entry["processed_csv_files"], 1)
            self.assertEqual(entry["failed_csv_files"], 1)
            self.assertEqual(entry["failed_filenames"], ["broken.csv"])
            self.assertIn("ParserError", entry["error_summaries"]["broken.csv"])
            self.assertEqual(summary["failed_folders"], 1)
            self.assertEqual(summary["failed_csv_files"], 1)

    def test_complete_folder_replaces_existing_archive_destination(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            paths = make_paths(Path(temp_dir))
            day = paths.inbox_dir / "20260815"
            shutil.copytree(FIXTURES / "etl/complete/20260815", day)
            old_archive = paths.archive_dir / day.name
            old_archive.mkdir()
            (old_archive / "stale.txt").write_text("stale")

            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                summary = runner.process_market(paths, make_args())

            manifest = json.loads(paths.manifest_path.read_text())
            self.assertFalse(day.exists())
            self.assertTrue((old_archive / "2330.csv").exists())
            self.assertFalse((old_archive / "stale.txt").exists())
            self.assertEqual(manifest["20260815"]["status"], "success")
            self.assertEqual(manifest["20260815"]["csv_files"], 1)
            self.assertEqual(summary["failed_folders"], 0)

    def test_dry_run_does_not_write_or_archive(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            paths = make_paths(Path(temp_dir))
            day = paths.inbox_dir / "20260815"
            shutil.copytree(FIXTURES / "etl/complete/20260815", day)

            with contextlib.redirect_stdout(io.StringIO()):
                summary = runner.process_market(paths, make_args(dry_run=True))

            self.assertTrue(day.exists())
            self.assertFalse(paths.manifest_path.exists())
            self.assertEqual(list(paths.output_dir.iterdir()), [])
            self.assertEqual(summary["failed_folders"], 0)

    def test_old_success_manifest_entry_remains_compatible(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            folder = Path(temp_dir) / "20260815"
            folder.mkdir()

            self.assertFalse(runner.should_process(folder, {"status": "success", "csv_files": 1}, None))

    def test_main_returns_nonzero_when_any_market_has_failed_folders(self):
        summary = {
            "synced_folders": 0,
            "processed_folders": 0,
            "expected_csv_files": 2,
            "processed_csv_files": 1,
            "failed_csv_files": 1,
            "updated_parquet_files": 1,
            "failed_folders": 1,
            "skipped_folders": 0,
        }
        fake_paths = mock.Mock(manifest_path=Path("unused.json"))

        with (
            mock.patch.object(runner, "parse_args", return_value=make_args()),
            mock.patch.object(runner, "build_paths", return_value=fake_paths),
            mock.patch.object(runner, "load_manifest", return_value={}),
            mock.patch.object(runner, "process_market", return_value=summary),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            exit_code = runner.main()

        self.assertEqual(exit_code, 1)


if __name__ == "__main__":
    unittest.main()
