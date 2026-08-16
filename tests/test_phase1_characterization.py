from __future__ import annotations

import importlib.util
import contextlib
import io
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).resolve().parent / "fixtures"


def load_module(name: str, relative_path: str):
    path = ROOT / relative_path
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class Phase1FixtureContractTests(unittest.TestCase):
    def test_trigger_payload_fixtures_capture_current_date_forms(self):
        twse = json.loads((FIXTURES / "cloud_payloads/twse_trigger.json").read_text())
        tpex = json.loads((FIXTURES / "cloud_payloads/tpex_trigger.json").read_text())

        self.assertEqual(twse["date"], "2026/08/15")
        self.assertEqual(tpex["date"], "20260815")
        self.assertTrue(all(isinstance(symbol, str) for symbol in twse["symbols"] + tpex["symbols"]))

    def test_firestore_status_fixture_records_current_vocabulary(self):
        statuses = json.loads((FIXTURES / "contracts/firestore_statuses.json").read_text())

        self.assertIn("pending", statuses["prepare"])
        self.assertEqual(statuses["trigger"], ["running"])
        self.assertEqual(statuses["crawler_success_action"], "delete_document")
        self.assertEqual(statuses["etl_manifest_terminal"], ["success", "failed"])

    def test_dashboard_fixture_records_file_contracts(self):
        inputs = json.loads((FIXTURES / "dashboard/minimal_inputs.json").read_text())

        self.assertEqual(inputs["ohlc_columns"][0:2], ["symbol", "date"])
        self.assertIn("scored.parquet", inputs["required_artifacts"])
        self.assertIn("日期", inputs["broker_report_columns"])

    def test_portal_fixture_records_current_prerequisite_groups(self):
        manifest = json.loads((FIXTURES / "contracts/portal_prerequisites.json").read_text())

        self.assertIn("episodes", manifest["artifacts"])
        self.assertIn("broker_branch_cases", manifest["artifacts"])
        self.assertEqual(manifest["portal"], "trigger_days_gt5_case_review/index.html")


class Phase1EtlCharacterizationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.pipeline = load_module("phase1_bs_report_pipeline", "apps/etl/bs_report_pipeline.py")
        cls.ohlc = load_module("phase1_build_ohlc", "apps/etl/build_ohlc_parquet.py")

    def test_complete_broker_report_folder_writes_expected_parquet(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            day = temp / "input" / "20260815"
            shutil.copytree(FIXTURES / "etl/complete/20260815", day)
            etl = self.pipeline.BsReportEtl(temp / "input", temp / "output", max_workers=1)

            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                stats, results = etl.run([day])

            self.assertEqual(stats.processed_folders, 1)
            self.assertEqual(stats.expected_csv_files, 1)
            self.assertEqual(stats.processed_csv_files, 1)
            self.assertTrue(results["20260815"].ok)
            self.assertEqual(results["20260815"].processed_csv_files, 1)
            self.assertEqual(results["20260815"].updated_parquet_files, 1)
            self.assertTrue((temp / "output/2330.parquet").exists())

    def test_partial_csv_failure_marks_the_folder_failed(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            day = temp / "input" / "20260815"
            shutil.copytree(FIXTURES / "etl/partial_failure/20260815", day)
            etl = self.pipeline.BsReportEtl(temp / "input", temp / "output", max_workers=1)

            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                stats, results = etl.run([day])

            result = results["20260815"]
            self.assertEqual(stats.processed_folders, 0)
            self.assertEqual(stats.failed_folders, 1)
            self.assertEqual(stats.expected_csv_files, 2)
            self.assertEqual(stats.processed_csv_files, 1)
            self.assertEqual(stats.failed_csv_files, 1)
            self.assertFalse(result.ok)
            self.assertEqual(result.failed_filenames, ("broken.csv",))
            self.assertIn("ParserError", result.error_summaries["broken.csv"])
            self.assertFalse((temp / "output/broken.parquet").exists())

    def test_ohlc_fixtures_produce_the_current_normalized_schema(self):
        frame = self.ohlc.build_ohlc(FIXTURES / "ohlc")

        self.assertEqual(
            list(frame.columns),
            ["symbol", "date", "open", "high", "low", "close", "volume", "market"],
        )
        self.assertEqual(set(frame["market"]), {"twse", "tpex"})
        self.assertEqual(set(frame["symbol"]), {"2330", "6488"})


class Phase1RuntimeChainTests(unittest.TestCase):
    def test_twse_container_delegates_to_canonical_module(self):
        dockerfile = (ROOT / "apps/twse/Dockerfile").read_text()
        wrapper = (ROOT / "apps/twse/crawler-twse-bsreport-new.py").read_text()

        self.assertIn("crawler-twse-bsreport-new.py", dockerfile)
        self.assertIn('ENTRYPOINT ["python", "pyfiles/crawler-twse-bsreport.py"]', dockerfile)
        self.assertIn("stockanalysis.runtime.crawlers.twse_bs_report", wrapper)

    def test_tpex_container_delegates_to_canonical_module(self):
        dockerfile = (ROOT / "apps/tpex/Dockerfile").read_text()
        entrypoint = (ROOT / "apps/tpex/entrypoint.sh").read_text()
        wrapper = (ROOT / "apps/tpex/crawler-tpex-bsreport.py").read_text()

        self.assertIn("crawler-tpex-bsreport.py", dockerfile)
        self.assertIn("python crawler-tpex-bsreport.py", entrypoint)
        self.assertIn("stockanalysis.runtime.crawlers.tpex_local_runner", wrapper)

    def test_local_runner_container_executes_package_module_directly(self):
        dockerfile = (ROOT / "apps/tpex/Dockerfile.local-runner").read_text()
        entrypoint = (ROOT / "apps/tpex/entrypoint-local-runner.sh").read_text()

        self.assertIn("entrypoint-local-runner.sh", dockerfile)
        self.assertIn("stockanalysis/runtime/crawlers/tpex_local_runner.py", entrypoint)


if __name__ == "__main__":
    unittest.main()
