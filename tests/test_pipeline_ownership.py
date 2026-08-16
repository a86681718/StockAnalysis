from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest

from stockanalysis.pipelines.broker_reports import BsReportEtl, FolderResult, ProcessStats
from stockanalysis.pipelines.ohlc import build_ohlc, load_ohlc_csv, to_number


ROOT = Path(__file__).resolve().parents[1]


def load_app_module(name: str, relative_path: str):
    path = ROOT / relative_path
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class PipelineOwnershipTests(unittest.TestCase):
    def test_broker_report_compatibility_module_reexports_canonical_types(self):
        compatibility = load_app_module(
            "phase8_bs_report_compatibility",
            "apps/etl/bs_report_pipeline.py",
        )

        self.assertIs(compatibility.BsReportEtl, BsReportEtl)
        self.assertIs(compatibility.FolderResult, FolderResult)
        self.assertIs(compatibility.ProcessStats, ProcessStats)

    def test_ohlc_cli_reexports_canonical_pipeline_functions(self):
        cli = load_app_module(
            "phase8_build_ohlc_cli",
            "apps/etl/build_ohlc_parquet.py",
        )

        self.assertIs(cli.build_ohlc, build_ohlc)
        self.assertIs(cli.load_ohlc_csv, load_ohlc_csv)
        self.assertIs(cli.to_number, to_number)


if __name__ == "__main__":
    unittest.main()
