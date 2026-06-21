from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from stockanalysis.analysis.single_branch_persistent_accumulation import (
    PersistentAccumulationConfig,
    build_episodes,
    build_symbol_features,
    is_local_branch_name,
)


def _config(tmp_path: Path) -> PersistentAccumulationConfig:
    return PersistentAccumulationConfig(
        broker_dirs=(),
        broker_list_path=tmp_path / "brokers.csv",
        output_dir=tmp_path,
        start_date=pd.Timestamp("2026-01-01"),
        end_date=pd.Timestamp("2026-03-31"),
        symbols={"1532"},
        excluded_symbols={"2317", "2330"},
        max_symbols=0,
        short_window=5,
        long_window=10,
        baseline_window=20,
        min_baseline_days=10,
        min_short_positive_days=3,
        min_short_net_buy=100_000.0,
        min_short_purity=0.8,
        min_short_buy_share=0.05,
        min_long_positive_days=6,
        min_long_net_buy=300_000.0,
        min_long_purity=0.8,
        min_long_buy_share=0.05,
        episode_gap_sessions=5,
        acceleration_ratio=1.5,
        broad_broker_symbol_threshold=20,
    )


class SingleBranchPersistentAccumulationTest(unittest.TestCase):
    def test_local_branch_excludes_central_desks(self) -> None:
        self.assertTrue(is_local_branch_name("新光-新竹"))
        self.assertTrue(is_local_branch_name("國票-長城"))
        self.assertFalse(is_local_branch_name("元大"))
        self.assertFalse(is_local_branch_name("元大-總公司"))
        self.assertFalse(is_local_branch_name("元富-營業"))
        self.assertFalse(is_local_branch_name("測試-法人部"))

    def test_dormant_pair_becomes_one_persistent_episode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg = _config(Path(tmp))
            dates = pd.bdate_range("2026-01-01", periods=45)
            rows = []
            for index, date in enumerate(dates):
                branch_buy = 0.0 if index < 22 else 80_000.0
                branch_sell = 0.0 if index < 22 else 2_000.0
                rows.extend(
                    [
                        {
                            "symbol": "1532",
                            "date": date,
                            "broker": "779U",
                            "broker_name": "國票-長城",
                            "is_local_branch": True,
                            "buy_volume": branch_buy,
                            "sell_volume": branch_sell,
                            "net_buy": branch_buy - branch_sell,
                        },
                        {
                            "symbol": "1532",
                            "date": date,
                            "broker": "OTHER",
                            "broker_name": "",
                            "is_local_branch": False,
                            "buy_volume": 320_000.0,
                            "sell_volume": 300_000.0,
                            "net_buy": 20_000.0,
                        },
                    ]
                )
            daily = pd.DataFrame(rows)
            features = build_symbol_features(daily, cfg)
            episodes, triggers = build_episodes(features, cfg)

        self.assertFalse(triggers.empty)
        self.assertEqual(len(episodes), 1)
        episode = episodes.iloc[0]
        self.assertEqual(episode["symbol"], "1532")
        self.assertEqual(episode["broker_name"], "國票-長城")
        self.assertTrue(episode["ongoing"])
        self.assertGreaterEqual(episode["positive_net_sessions"], 15)
        self.assertGreater(episode["cumulative_purity"], 0.9)
        self.assertGreater(episode["cumulative_buy_share"], 0.1)

    def test_one_day_burst_does_not_trigger(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg = _config(Path(tmp))
            dates = pd.bdate_range("2026-01-01", periods=35)
            rows = []
            for index, date in enumerate(dates):
                buy = 1_000_000.0 if index == 25 else 0.0
                rows.extend(
                    [
                        {
                            "symbol": "1234",
                            "date": date,
                            "broker": "B1",
                            "broker_name": "新光-新竹",
                            "is_local_branch": True,
                            "buy_volume": buy,
                            "sell_volume": 0.0,
                            "net_buy": buy,
                        },
                        {
                            "symbol": "1234",
                            "date": date,
                            "broker": "OTHER",
                            "broker_name": "",
                            "is_local_branch": False,
                            "buy_volume": 500_000.0,
                            "sell_volume": 500_000.0,
                            "net_buy": 0.0,
                        },
                    ]
                )
            features = build_symbol_features(pd.DataFrame(rows), cfg)
            episodes, triggers = build_episodes(features, cfg)

        self.assertTrue(triggers.empty)
        self.assertTrue(episodes.empty)


if __name__ == "__main__":
    unittest.main()
