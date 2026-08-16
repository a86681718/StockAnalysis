from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest

from stockanalysis.pipelines.broker_reports import BsReportEtl, FolderResult, ProcessStats


ROOT = Path(__file__).resolve().parents[1]


def load_compatibility_module():
    path = ROOT / "apps/etl/bs_report_pipeline.py"
    spec = importlib.util.spec_from_file_location("phase8_bs_report_compatibility", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class PipelineOwnershipTests(unittest.TestCase):
    def test_broker_report_compatibility_module_reexports_canonical_types(self):
        compatibility = load_compatibility_module()

        self.assertIs(compatibility.BsReportEtl, BsReportEtl)
        self.assertIs(compatibility.FolderResult, FolderResult)
        self.assertIs(compatibility.ProcessStats, ProcessStats)


if __name__ == "__main__":
    unittest.main()
