from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd


ANALYSIS_DIR = Path(__file__).resolve().parents[1] / "apps" / "analysis"
sys.path.insert(0, str(ANALYSIS_DIR))

from run_six_strategy_indicator_exit_research import ExitPolicy, exit_trigger, make_folds  # noqa: E402


class SixStrategyIndicatorExitResearchTest(unittest.TestCase):
    def test_profit_chandelier_requires_profit_activation(self) -> None:
        frame = pd.DataFrame(
            {
                "date": pd.date_range("2026-01-01", periods=2),
                "close": [100.0, 100.0],
                "high": [100.0, 115.0],
                "atr14": [2.0, 2.0],
                "rsi14": [60.0, 60.0],
                "macd": [1.0, 1.0],
                "macd_signal": [0.0, 0.0],
                "ema20": [95.0, 95.0],
                "ema50": [95.0, 95.0],
                "bb_mid": [95.0, 95.0],
                "bb_lower": [90.0, 90.0],
                "bb_upper": [110.0, 110.0],
            }
        )
        policy = ExitPolicy("profit_chandelier_atr3_act10", "profit_chandelier", (3.0, 0.10))
        state = {"profit_armed": False, "upper_band_armed": False}

        before_activation = exit_trigger(frame, 0, 100.0, 100.0, state, policy, {})
        after_activation = exit_trigger(frame, 1, 115.0, 100.0, state, policy, {})

        self.assertIsNone(before_activation)
        self.assertEqual(after_activation, "PROFIT_CHANDELIER")

    def test_walk_forward_folds_are_chronological_and_non_overlapping(self) -> None:
        signals = pd.DataFrame(
            {
                "symbol": ["1234"] * 10,
                "date": pd.date_range("2026-01-01", periods=10),
            }
        )

        folds = make_folds(signals)

        self.assertEqual(len(folds), 3)
        self.assertTrue(all(start <= end for start, end in folds))
        self.assertTrue(all(folds[idx][1] < folds[idx + 1][0] for idx in range(len(folds) - 1)))


if __name__ == "__main__":
    unittest.main()
