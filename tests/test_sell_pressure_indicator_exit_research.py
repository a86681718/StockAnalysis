from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd


ANALYSIS_DIR = Path(__file__).resolve().parents[1] / "apps" / "analysis"
sys.path.insert(0, str(ANALYSIS_DIR))

from run_sell_pressure_exit_research import CostConfig  # noqa: E402
from run_sell_pressure_indicator_exit_research import IndicatorPolicy, simulate_policy  # noqa: E402


def _feature_map(close: list[float], high: list[float], atr: list[float]) -> dict:
    dates = pd.date_range("2026-01-01", periods=len(close), freq="D")
    frame = pd.DataFrame(
        {
            "date": dates,
            "open": close,
            "high": high,
            "low": close,
            "close": close,
            "pos_neg_balance": [1.0] * len(close),
            "buyer_count_pct_cs": [0.9] * len(close),
            "atr14": atr,
            "ema20": close,
            "ema50": close,
        }
    )
    return {"1234": {"frame": frame, "date_to_idx": {date: idx for idx, date in enumerate(dates)}}}


class SellPressureIndicatorExitResearchTest(unittest.TestCase):
    def setUp(self) -> None:
        self.signals = pd.DataFrame({"symbol": ["1234"], "date": [pd.Timestamp("2026-01-01")]})
        self.no_cost = CostConfig(fee_rate=0.0, fee_discount=0.0, tax_rate_sell=0.0, slippage=0.0)

    def test_chandelier_exits_only_when_indicator_triggers(self) -> None:
        maps = _feature_map([100.0, 100.0, 110.0, 96.0], [100.0, 102.0, 112.0, 105.0], [4.0] * 4)
        trades = simulate_policy(
            self.signals,
            maps,
            IndicatorPolicy("chandelier", atr_multiple=3.0),
            pd.Timestamp("2026-01-04"),
            self.no_cost,
        )

        self.assertEqual(trades.loc[0, "position_status"], "CLOSED")
        self.assertEqual(trades.loc[0, "valuation_date"], pd.Timestamp("2026-01-04"))
        self.assertEqual(trades.loc[0, "exit_reason"], "CHANDELIER")

    def test_untriggered_position_is_marked_open_not_forced_closed(self) -> None:
        maps = _feature_map([100.0, 100.0, 105.0, 110.0], [100.0, 102.0, 107.0, 112.0], [4.0] * 4)
        trades = simulate_policy(
            self.signals,
            maps,
            IndicatorPolicy("chandelier", atr_multiple=3.0),
            pd.Timestamp("2026-01-04"),
            self.no_cost,
        )

        self.assertEqual(trades.loc[0, "position_status"], "OPEN_MARKED")
        self.assertEqual(trades.loc[0, "exit_reason"], "OPEN")
        self.assertAlmostEqual(trades.loc[0, "net_ret"], 0.10)


if __name__ == "__main__":
    unittest.main()
