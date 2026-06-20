from __future__ import annotations

import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd


ANALYSIS_DIR = Path(__file__).resolve().parents[1] / "apps" / "analysis"
sys.path.insert(0, str(ANALYSIS_DIR))

from run_sell_pressure_exit_research import CostConfig, ExitPolicy, simulate_policy  # noqa: E402


def _ohlc_map(open_prices: list[float], high: list[float], low: list[float], close: list[float]) -> dict:
    dates = pd.date_range("2026-01-01", periods=len(open_prices), freq="D").to_numpy(dtype="datetime64[ns]")
    return {
        "1234": {
            "dates": dates,
            "open": np.array(open_prices, dtype=float),
            "high": np.array(high, dtype=float),
            "low": np.array(low, dtype=float),
            "close": np.array(close, dtype=float),
            "date_to_idx": {pd.Timestamp(date): i for i, date in enumerate(dates)},
        }
    }


class SellPressureExitResearchTest(unittest.TestCase):
    def setUp(self) -> None:
        self.signals = pd.DataFrame({"symbol": ["1234"], "date": [pd.Timestamp("2026-01-01")]})
        self.no_cost = CostConfig(fee_rate=0.0, fee_discount=0.0, tax_rate_sell=0.0, slippage=0.0)

    def test_stop_loss_uses_gap_open_when_price_opens_below_stop(self) -> None:
        maps = _ohlc_map(
            [100.0, 100.0, 80.0, 80.0],
            [100.0, 105.0, 85.0, 85.0],
            [100.0, 95.0, 75.0, 75.0],
            [100.0, 100.0, 80.0, 80.0],
        )
        trades = simulate_policy(self.signals, maps, ExitPolicy("stop_time", 2, stop_loss=-0.10), self.no_cost)

        self.assertEqual(trades.loc[0, "exit_reason"], "SL")
        self.assertEqual(trades.loc[0, "exit_raw"], 80.0)
        self.assertAlmostEqual(trades.loc[0, "net_ret"], -0.20)

    def test_trailing_stop_uses_only_prior_day_peak(self) -> None:
        maps = _ohlc_map(
            [100.0, 100.0, 110.0, 108.0, 108.0],
            [100.0, 110.0, 120.0, 112.0, 112.0],
            [100.0, 100.0, 100.0, 107.0, 107.0],
            [100.0, 105.0, 115.0, 108.0, 108.0],
        )
        policy = ExitPolicy("trailing", 3, trail_pct=0.10, activation_ret=0.10)
        trades = simulate_policy(self.signals, maps, policy, self.no_cost)

        self.assertEqual(trades.loc[0, "exit_reason"], "TRAIL")
        self.assertEqual(trades.loc[0, "hold_days"], 2)
        self.assertAlmostEqual(trades.loc[0, "exit_raw"], 108.0)


if __name__ == "__main__":
    unittest.main()
