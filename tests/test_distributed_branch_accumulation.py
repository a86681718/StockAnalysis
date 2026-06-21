from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from stockanalysis.analysis.distributed_branch_accumulation import (
    DistributedAccumulationConfig,
    build_cluster_daily,
    build_cluster_episodes,
    build_member_features,
)


def _config(tmp_path: Path) -> DistributedAccumulationConfig:
    return DistributedAccumulationConfig(
        broker_dirs=(),
        broker_list_path=tmp_path / "brokers.csv",
        broad_broker_source=tmp_path / "broad.parquet",
        output_dir=tmp_path,
        start_date=pd.Timestamp("2026-01-01"),
        end_date=pd.Timestamp("2026-03-31"),
        symbols={"1234"},
        excluded_symbols={"2317", "2330"},
        max_symbols=0,
        short_window=5,
        baseline_window=20,
        min_baseline_days=10,
        min_member_positive_days=2,
        min_member_net_buy=20_000.0,
        min_member_purity=0.75,
        min_member_buy_share=0.01,
        min_participation_shift=0.15,
        min_activity_multiple=3.0,
        min_branches=3,
        min_parent_brokers=2,
        min_cluster_net_buy=100_000.0,
        min_cluster_purity=0.80,
        min_cluster_buy_share=0.10,
        max_largest_branch_share=0.60,
        min_residual_net_buy=50_000.0,
        min_residual_buy_share=0.04,
        episode_gap_sessions=5,
        min_member_overlap_ratio=0.5,
    )


def _daily(branch_buys: tuple[float, float, float], same_parent: bool = False) -> pd.DataFrame:
    dates = pd.bdate_range("2026-01-01", periods=40)
    names = ("元大-新竹", "元大-竹北", "元大-台南") if same_parent else ("新光-新竹", "國票-長城", "元大-竹北")
    rows = []
    for index, date in enumerate(dates):
        for broker_index, (broker, name, active_buy) in enumerate(zip(("B1", "B2", "B3"), names, branch_buys)):
            buy = active_buy if index >= 30 else 0.0
            rows.append(
                {
                    "symbol": "1234",
                    "date": date,
                    "broker": broker,
                    "broker_name": name,
                    "is_local_branch": True,
                    "buy_volume": buy,
                    "sell_volume": 0.0,
                    "net_buy": buy,
                }
            )
        rows.append(
            {
                "symbol": "1234",
                "date": date,
                "broker": "OTHER",
                "broker_name": "",
                "is_local_branch": False,
                "buy_volume": 400_000.0,
                "sell_volume": 400_000.0,
                "net_buy": 0.0,
            }
        )
    return pd.DataFrame(rows)


class DistributedBranchAccumulationTest(unittest.TestCase):
    def test_empty_member_table_returns_empty_cluster_table(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            clusters = build_cluster_daily(pd.DataFrame(), _config(Path(tmp)))
        self.assertTrue(clusters.empty)

    def test_independent_branches_form_cluster(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg = _config(Path(tmp))
            members = build_member_features(_daily((50_000.0, 50_000.0, 50_000.0)), cfg, set())
            clusters = build_cluster_daily(members, cfg)
        self.assertTrue(clusters["qualifies"].any())
        latest = clusters[clusters["qualifies"]].iloc[-1]
        self.assertEqual(latest["branch_count"], 3)
        self.assertEqual(latest["parent_broker_count"], 3)
        self.assertLessEqual(latest["largest_branch_net_share"], 0.60)

    def test_dominant_branch_fails_residual_control(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg = _config(Path(tmp))
            members = build_member_features(_daily((250_000.0, 30_000.0, 30_000.0)), cfg, set())
            clusters = build_cluster_daily(members, cfg)
        self.assertFalse(clusters["qualifies"].any())

    def test_one_parent_broker_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg = _config(Path(tmp))
            members = build_member_features(_daily((50_000.0, 50_000.0, 50_000.0), same_parent=True), cfg, set())
            clusters = build_cluster_daily(members, cfg)
        self.assertFalse(clusters["qualifies"].any())

    def test_one_day_distributed_burst_fails_member_persistence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg = _config(Path(tmp))
            daily = _daily((0.0, 0.0, 0.0))
            burst_date = daily["date"].max()
            mask = daily["broker"].isin(["B1", "B2", "B3"]) & daily["date"].eq(burst_date)
            daily.loc[mask, ["buy_volume", "net_buy"]] = 500_000.0
            members = build_member_features(daily, cfg, set())
            clusters = build_cluster_daily(members, cfg)
        self.assertTrue(clusters.empty or not clusters["qualifies"].any())

    def test_disjoint_member_sets_form_separate_episodes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg = _config(Path(tmp))
            dates = pd.bdate_range("2026-01-01", periods=4)
            clusters = pd.DataFrame(
                {
                    "symbol": ["1234", "1234"],
                    "date": [dates[1], dates[2]],
                    "qualifies": [True, True],
                    "branch_count": [3, 3],
                    "parent_broker_count": [3, 3],
                    "cluster_net_buy": [300_000.0, 300_000.0],
                    "cluster_purity": [1.0, 1.0],
                    "cluster_buy_share": [0.2, 0.2],
                    "largest_branch_net_share": [1 / 3, 1 / 3],
                    "residual_net_buy": [200_000.0, 200_000.0],
                    "residual_buy_share": [0.1, 0.1],
                    "member_brokers": ["A,B,C", "D,E,F"],
                    "member_names": ["甲-一,乙-二,丙-三", "丁-四,戊-五,己-六"],
                    "parent_brokers": ["甲,乙,丙", "丁,戊,己"],
                }
            )
            episodes = build_cluster_episodes(clusters, pd.DatetimeIndex(dates), cfg)
        self.assertEqual(len(episodes), 2)


if __name__ == "__main__":
    unittest.main()
