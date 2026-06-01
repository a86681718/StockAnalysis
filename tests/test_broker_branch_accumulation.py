from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from stockanalysis.analysis.broker_branch_accumulation import (
    DetectionConfig,
    add_rolling_metrics,
    build_core_feature_table,
    detect_events,
    is_branch_name,
)


def _test_config(tmp_path: Path) -> DetectionConfig:
    return DetectionConfig(
        broker_dirs=(),
        warrant_list_path=tmp_path / "warrant.csv",
        broker_list_path=tmp_path / "brokers.csv",
        ohlc_path=tmp_path / "ohlc.parquet",
        output_dir=tmp_path,
        start_date=pd.Timestamp("2026-01-01"),
        end_date=pd.Timestamp("2026-01-12"),
        symbols={"1234"},
        max_symbols=0,
        exclude_etf=True,
        include_unknown_brokers=False,
        min_branch_suffix_len=2,
        window_days=3,
        min_history_days=3,
        min_positive_days=2,
        min_window_net=100.0,
        min_window_net_ratio=2.0,
        min_net_buy_ratio=0.5,
        min_branch_window_share=0.5,
        max_single_day_share=0.8,
        combined_other_min_net=1.0,
        combined_bonus=0.25,
        max_avg_volume_20d=0.0,
        top_n=20,
        save_features=False,
    )


class BrokerBranchAccumulationTest(unittest.TestCase):
    def test_branch_name_detection_handles_local_branch_examples(self) -> None:
        for name in ("凱基-松山", "元大-竹北", "永豐金-台中", "富邦-建國", "群益金鼎-高雄"):
            self.assertTrue(is_branch_name(name, min_suffix_len=2))

        self.assertFalse(is_branch_name("合庫", min_suffix_len=2))
        self.assertFalse(is_branch_name("元大-A", min_suffix_len=2))

    def test_combined_event_uses_stock_and_warrant_same_branch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg = _test_config(Path(tmp))
            dates = pd.date_range("2026-01-01", periods=8, freq="D")
            stock_daily = pd.DataFrame(
                {
                    "underlying_stock_id": ["1234"] * len(dates),
                    "date": dates,
                    "broker": ["B1"] * len(dates),
                    "stock_buy_volume": [10, 10, 10, 10, 100, 100, 100, 0],
                    "stock_sell_volume": [0] * len(dates),
                    "stock_buy_amount": [100] * len(dates),
                    "stock_sell_amount": [0] * len(dates),
                }
            )
            stock_daily["stock_net_buy"] = stock_daily["stock_buy_volume"] - stock_daily["stock_sell_volume"]
            warrant_daily = pd.DataFrame(
                {
                    "underlying_stock_id": ["1234", "1234", "1234"],
                    "date": dates[4:7],
                    "broker": ["B1", "B1", "B1"],
                    "warrant_buy_volume": [5, 5, 5],
                    "warrant_sell_volume": [0, 0, 0],
                    "warrant_buy_amount": [50, 50, 50],
                    "warrant_sell_amount": [0, 0, 0],
                    "warrant_count": [1, 1, 1],
                    "warrant_ids": ["030001", "030001", "030001"],
                    "warrant_names": ["W", "W", "W"],
                }
            )
            warrant_daily["warrant_net_buy"] = warrant_daily["warrant_buy_volume"] - warrant_daily["warrant_sell_volume"]
            broker_lookup = {"B1": "凱基-松山"}
            ohlc = pd.DataFrame(
                {
                    "symbol": ["1234"] * len(dates),
                    "date": dates,
                    "open": [10.0] * len(dates),
                    "high": [10.5] * len(dates),
                    "low": [9.5] * len(dates),
                    "close": [10.0] * len(dates),
                    "volume": [1_000_000.0] * len(dates),
                }
            )

            core = build_core_feature_table(stock_daily, warrant_daily, broker_lookup, cfg.min_branch_suffix_len)
            features = add_rolling_metrics(core, ohlc, cfg)
            events = detect_events(features, cfg)

        self.assertFalse(events.empty)
        top = events.iloc[0]
        self.assertEqual(top["underlying_stock_id"], "1234")
        self.assertEqual(top["broker"], "B1")
        self.assertEqual(top["event_type"], "combined")
        self.assertGreaterEqual(top["stock_window_net_buy"], 300.0)
        self.assertIn("030001", top["warrant_window_ids"])


if __name__ == "__main__":
    unittest.main()
