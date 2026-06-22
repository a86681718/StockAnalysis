from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import pandas as pd

from stockanalysis.analysis.general_broker_flow_anomaly import (
    GeneralAnomalyConfig,
    build_episodes,
    build_features,
)


def make_config(output_dir: Path) -> GeneralAnomalyConfig:
    return GeneralAnomalyConfig(
        broker_dirs=(), broker_list_path=Path("unused"), output_dir=output_dir,
        start_date=None, end_date=None, symbols=None, max_symbols=0,
        recent_window=3, baseline_window=6, min_baseline_days=4,
        pressure_quantile=0.8, min_positive_days=2, min_buyer_retention=0.5,
        min_buy_participation=0.08, min_pressure_multiple=1.5,
        episode_gap_sessions=2, min_episode_triggers=2,
    )


class GeneralBrokerFlowAnomalyTests(unittest.TestCase):
    def test_baseline_is_shifted_and_detects_sustained_surge(self) -> None:
        with TemporaryDirectory() as tmp:
            cfg = make_config(Path(tmp))
            dates = pd.date_range("2026-01-01", periods=14, freq="D")
            pressure = [100.0] * 10 + [1000.0] * 4
            daily = pd.DataFrame({
                "symbol": "1234", "date": dates,
                "recent_positive_pressure": pressure,
                "recent_net_buy": pressure,
                "recent_total_buy": [2000.0] * 14,
                "recent_positive_days": [3.0] * 14,
                "recent_mean_buyer_count": [2.0] * 10 + [8.0] * 4,
                "recent_mean_top_share": [0.7] * 10 + [0.25] * 4,
                "recent_mean_hhi": [0.55] * 10 + [0.15] * 4,
                "recent_buyer_names": ["A,B"] * 10 + ["A,B,C,D,E"] * 4,
                "recent_buyer_union_count": [2] * 10 + [5] * 4,
                "top_buyer": ["A"] * 14,
                "buyer_retention": [0.8] * 14,
            })
            features = build_features(daily, cfg)
            triggers = features[features["qualifies"]]
            self.assertGreaterEqual(len(triggers), 2)
            self.assertEqual(triggers.iloc[0]["pattern"], "breadth_expansion")
            self.assertLess(triggers.iloc[0]["baseline_pressure_threshold"], triggers.iloc[0]["recent_positive_pressure"])

    def test_adjacent_triggers_form_one_distinct_episode(self) -> None:
        with TemporaryDirectory() as tmp:
            cfg = make_config(Path(tmp))
            features = pd.DataFrame({
                "symbol": ["1234"] * 5,
                "date": pd.date_range("2026-01-01", periods=5),
                "qualifies": [False, True, True, True, False],
                "pattern": ["none", "mixed_accumulation", "distributed_surge", "distributed_surge", "none"],
                "pressure_multiple": [1.0, 2.0, 2.5, 3.0, 1.0],
                "buyer_retention": [0.0, 0.6, 0.7, 0.8, 0.0],
                "buy_participation": [0.0, 0.1, 0.2, 0.3, 0.0],
                "recent_positive_pressure": [0, 100, 200, 300, 0],
                "recent_net_buy": [0, 60, 140, 240, 0],
                "recent_buyer_union_count": [0, 4, 5, 6, 0],
                "recent_mean_top_share": [0, 0.4, 0.3, 0.2, 0],
                "top_buyer": ["", "A", "A", "B", ""],
                "recent_buyer_names": ["", "A,B", "A,B,C", "A,B,C,D", ""],
            })
            episodes, triggers = build_episodes(features, cfg)
            self.assertEqual(len(triggers), 3)
            self.assertEqual(len(episodes), 1)
            self.assertEqual(int(episodes.iloc[0]["qualifying_dates"]), 3)
            self.assertEqual(episodes.iloc[0]["dominant_pattern"], "distributed_surge")


if __name__ == "__main__":
    unittest.main()
