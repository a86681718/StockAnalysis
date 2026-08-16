from __future__ import annotations

import importlib
from pathlib import Path
import sys
import tempfile
import unittest

from scripts.stage_cloud_run_source import SERVICE_NAMES, stage_service_source


ROOT = Path(__file__).resolve().parents[1]


class CloudRunSourceStagingTests(unittest.TestCase):
    def test_each_service_is_staged_with_its_build_files_and_contract(self):
        for service_name in SERVICE_NAMES:
            with self.subTest(service=service_name), tempfile.TemporaryDirectory() as temp_dir:
                destination = Path(temp_dir) / service_name

                stage_service_source(service_name, destination)

                self.assertTrue((destination / "main.py").is_file())
                self.assertTrue((destination / "requirements.txt").is_file())
                self.assertTrue((destination / "project.toml").is_file())
                self.assertTrue((destination / "stockanalysis/contracts/crawl_jobs.py").is_file())

    def test_staged_contract_is_importable_without_repository_src_path(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            destination = Path(temp_dir) / "trigger-twse-job"
            stage_service_source("trigger-twse-job", destination)
            original_path = sys.path[:]
            previous_modules = {
                name: sys.modules.pop(name, None)
                for name in ("stockanalysis", "stockanalysis.contracts", "stockanalysis.contracts.crawl_jobs")
            }
            try:
                repository_src = (ROOT / "src").resolve()
                sys.path = [
                    str(destination),
                    *(entry for entry in original_path if entry and Path(entry).resolve() != repository_src),
                ]
                module = importlib.import_module("stockanalysis.contracts.crawl_jobs")
                payload = module.parse_crawl_job_payload(
                    {"symbols": ["2330"], "date": "20260815"},
                    max_symbols=1,
                )
                self.assertEqual(payload.compact_date, "20260815")
            finally:
                sys.path = original_path
                for name in ("stockanalysis", "stockanalysis.contracts", "stockanalysis.contracts.crawl_jobs"):
                    sys.modules.pop(name, None)
                for name, module in previous_modules.items():
                    if module is not None:
                        sys.modules[name] = module

    def test_rejects_unknown_service_and_nonempty_destination(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            with self.assertRaises(ValueError):
                stage_service_source("unknown", root / "unknown")

            destination = root / "nonempty"
            destination.mkdir()
            (destination / "existing.txt").write_text("do not overwrite")
            with self.assertRaises(ValueError):
                stage_service_source("prepare-twse-list", destination)


if __name__ == "__main__":
    unittest.main()
