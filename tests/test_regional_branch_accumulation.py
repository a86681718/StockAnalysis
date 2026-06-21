from __future__ import annotations

import unittest
from types import SimpleNamespace

import pandas as pd

from stockanalysis.analysis.regional_branch_accumulation import add_city_baseline_context, extract_city, filter_company_city_members


class RegionalBranchAccumulationTest(unittest.TestCase):
    def test_extract_city_handles_chinese_legacy_and_english_addresses(self) -> None:
        self.assertEqual(extract_city("台南市永康區仁愛街398號"), "台南市")
        self.assertEqual(extract_city("臺南縣麻豆鎮興中路127號"), "台南市")
        self.assertEqual(extract_city("Taipei City 100028, Taiwan"), "台北市")
        self.assertEqual(extract_city("Tainan City 710, Taiwan"), "台南市")

    def test_only_company_city_members_remain(self) -> None:
        members = pd.DataFrame({"broker": ["A", "B", "C"], "value": [1, 2, 3]})
        result = filter_company_city_members(members, "台南市", {"A": "台南市", "B": "台北市", "C": "台南市"})
        self.assertEqual(result["broker"].tolist(), ["A", "C"])

    def test_missing_company_city_returns_empty(self) -> None:
        result = filter_company_city_members(pd.DataFrame({"broker": ["A"]}), "", {"A": "台南市"})
        self.assertTrue(result.empty)

    def test_city_share_must_rise_above_its_own_baseline(self) -> None:
        dates = pd.bdate_range("2026-01-01", periods=12)
        rows = []
        for index, date in enumerate(dates):
            local_buy = 10.0 if index < 10 else 40.0
            rows.extend([
                {"date": date, "broker": "LOCAL", "is_local_branch": True, "buy_volume": local_buy},
                {"date": date, "broker": "OTHER", "is_local_branch": True, "buy_volume": 100.0 - local_buy},
            ])
        clusters = pd.DataFrame({"date": [dates[-1]], "qualifies": [True]})
        cfg = SimpleNamespace(
            short_window=2,
            baseline_window=5,
            min_baseline_days=3,
            min_city_buy_share_lift=1.5,
            min_city_buy_share_delta=0.03,
        )
        result = add_city_baseline_context(
            pd.DataFrame(rows), clusters, "台南市", {"LOCAL": "台南市", "OTHER": "台北市"}, cfg
        )
        self.assertTrue(result.loc[0, "qualifies"])
        self.assertGreater(result.loc[0, "city_buy_share_lift"], 1.5)


if __name__ == "__main__":
    unittest.main()
