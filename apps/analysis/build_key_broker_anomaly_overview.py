#!/usr/bin/env python3
"""Build a plain-language overview of current key-broker anomaly detectors."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
OUTPUT_ROOT = ROOT / "outputs" / "analysis"


def _read(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path) if path.exists() else pd.DataFrame()


def _pct(value: object) -> str:
    return "-" if pd.isna(value) else f"{float(value):.1%}"


def _number(value: object) -> str:
    return "-" if pd.isna(value) else f"{float(value):,.0f}"


def _single_rows(frame: pd.DataFrame, limit: int = 10) -> list[str]:
    rows = []
    for row in frame[frame["review_eligible"]].head(limit).itertuples(index=False):
        rows.append(
            f"| {row.rank} | {row.symbol} | {row.broker_name} | {row.confidence} / {row.status} | "
            f"{pd.Timestamp(row.episode_start).date()} | {_number(row.cumulative_net_buy)} | "
            f"{_pct(row.cumulative_buy_share)} | {_pct(row.cumulative_purity)} |"
        )
    return rows


def _distributed_rows(frame: pd.DataFrame, limit: int = 10) -> list[str]:
    rows = []
    for row in frame[frame["review_eligible"]].head(limit).itertuples(index=False):
        rows.append(
            f"| {row.rank} | {row.symbol} | {row.confidence} | {pd.Timestamp(row.episode_start).date()} | "
            f"{row.max_branch_count} | {row.max_parent_broker_count} | {_number(row.latest_cluster_net_buy)} | "
            f"{_pct(row.latest_cluster_buy_share)} | {_pct(row.latest_largest_branch_net_share)} |"
        )
    return rows


def _regional_rows(frame: pd.DataFrame, limit: int = 15) -> list[str]:
    rows = []
    for row in frame[frame["review_eligible"]].head(limit).itertuples(index=False):
        rows.append(
            f"| {row.rank} | {row.symbol} {row.company_name} | {row.company_city} | {row.confidence} | "
            f"{pd.Timestamp(row.episode_start).date()} | {row.max_branch_count} | {_number(row.latest_cluster_net_buy)} | "
            f"{_pct(row.latest_cluster_buy_share)} | {row.latest_city_buy_share_lift:.2f}x |"
        )
    return rows


def _general_rows(frame: pd.DataFrame, limit: int = 10) -> list[str]:
    labels = {
        "concentrated_surge": "集中突增",
        "distributed_surge": "分散突增",
        "breadth_expansion": "買方廣度擴張",
        "mixed_accumulation": "混合累積",
    }
    rows = []
    for row in frame.head(limit).itertuples(index=False):
        rows.append(
            f"| {row.review_rank} | {row.symbol} | {labels.get(row.dominant_pattern, row.dominant_pattern)} | "
            f"{pd.Timestamp(row.episode_start).date()} | {pd.Timestamp(row.last_qualifying_date).date()} | "
            f"{row.qualifying_dates} | {row.max_pressure_multiple:.2f}x | {_number(row.max_recent_net_buy)} | "
            f"{_pct(row.max_buy_participation)} | {row.primary_buyer} |"
        )
    return rows


def main() -> int:
    single = _read(OUTPUT_ROOT / "single_branch_persistent_accumulation" / "single_branch_persistent_episodes.parquet")
    distributed = _read(OUTPUT_ROOT / "distributed_branch_accumulation" / "distributed_branch_episodes.parquet")
    regional = _read(OUTPUT_ROOT / "regional_branch_accumulation" / "regional_branch_episodes.parquet")
    general = _read(OUTPUT_ROOT / "general_broker_flow_anomaly" / "general_broker_flow_anomaly_review_cases.parquet")

    single_review = single[single["review_eligible"]] if not single.empty else single
    distributed_review = distributed[distributed["review_eligible"]] if not distributed.empty else distributed
    regional_review = regional[regional["review_eligible"]] if not regional.empty else regional
    case_single = single[(single["symbol"] == "1532") & (single["broker"] == "779U")]
    case_distributed = distributed[(distributed["symbol"] == "1532") & distributed["review_eligible"]]

    lines = [
        "# 關鍵分點異常偵測總覽",
        "",
        "> 這些是異常行為觀察清單，不是買進建議。偵測條件沒有使用事件後股價或報酬。",
        "",
        "## 先看懂四個方向",
        "",
        "| 方向 | 回答的問題 | 目前 review 數 |",
        "|---|---|---:|",
        f"| 單一分點長期累積 | 哪一個地區分點持續、近乎只買不賣？ | {len(single_review)} |",
        f"| 多分點分散累積 | 是否有多家券商分點同時異常買進，且不是單一分點撐起來？ | {len(distributed_review)} |",
        f"| 公司所在地群聚 | 公司登記縣市內的分點是否共同異常，且高於該地區過去基準？ | {len(regional_review)} |",
        f"| 通用分點流異常 | 不預設券商、地區或集中型態，哪些股票的五日買壓相對自身歷史異常？ | {len(general)} |",
        "",
        "## 狀態怎麼看",
        "",
        "- `developing`：至少連續出現 3 個合格日期，仍屬早期觀察。",
        "- `confirmed`：至少 8 個合格日期，持續性較明確。",
        "- `established`：至少 20 個合格日期，屬長期成立的行為事件。",
        "- `accelerating / confirmed / cooling / unwinding`：描述最近是在加速、持續、降溫或回吐。",
        "",
        "## 1532 勤美目前怎麼解讀",
        "",
    ]
    if not case_single.empty:
        row = case_single.iloc[0]
        lines.extend([
            f"- 單一分點：`國票-長城` 從 `{pd.Timestamp(row['episode_start']).date()}` 開始，",
            f"  累積淨買 `{_number(row['cumulative_net_buy'])}` 股、買量占比 `{_pct(row['cumulative_buy_share'])}`、",
            f"  純度 `{_pct(row['cumulative_purity'])}`，屬 `{row['confidence']}`。這是目前最清楚的長期事件。",
        ])
    if not case_distributed.empty:
        row = case_distributed.iloc[0]
        lines.extend([
            f"- 多分點：`{pd.Timestamp(row['episode_start']).date()}` 至 `{pd.Timestamp(row['last_qualifying_date']).date()}` "
            f"出現 `{int(row['max_branch_count'])}` 個分點、`{int(row['max_parent_broker_count'])}` 家母券商共同買進，",
            f"  最新合計淨買 `{_number(row['latest_cluster_net_buy'])}` 股；但只有 `{int(row['qualifying_dates'])}` 個合格日期，",
            f"  所以只列為 `{row['confidence']}`，不能推定是同一主力。",
        ])
    lines.extend([
        "",
        "## 2474 可成目前怎麼解讀",
        "",
        "- 公司登記地址為台南市永康區。",
        "- 2025 年 10 至 11 月確實看到 `富邦-台南` 大量持續買進。",
        "- 同期其他台南分點不足，且最大分點占比過高，因此屬單一在地分點事件，不是多分點地緣群聚。",
        "- 目前地緣 review 清單沒有 2474；這是偵測結果，不為了符合案例印象而放寬門檻。",
        "",
        "## 單一分點長期累積 Top 10",
        "",
        "| 排名 | 股票 | 分點 | 層級 / 狀態 | 開始日 | 累積淨買股數 | 買量占比 | 純度 |",
        "|---:|---|---|---|---|---:|---:|---:|",
        *_single_rows(single),
        "",
        "## 多分點分散累積 Top 10",
        "",
        "| 排名 | 股票 | 層級 | 開始日 | 最大分點數 | 母券商數 | 最新淨買股數 | 買量占比 | 最大分點占比 |",
        "|---:|---|---|---|---:|---:|---:|---:|---:|",
        *_distributed_rows(distributed),
        "",
        "## 公司所在地群聚",
        "",
        "| 排名 | 股票 | 公司縣市 | 層級 | 開始日 | 最大分點數 | 最新淨買股數 | 買量占比 | 相對歷史地區占比 |",
        "|---:|---|---|---|---|---:|---:|---:|---:|",
        *_regional_rows(regional),
        "",
        "## 通用分點流異常 Top 10",
        "",
        "這一方向不使用指定股票、券商、公司地區或未來報酬。每檔股票以自身過去 60 個分點報告日為基準，",
        "最近 5 日正向分點壓力必須超過歷史 95 分位，且至少連續出現 3 個 qualifying dates 才形成案例。",
        "",
        "| 排名 | 股票 | 型態 | 開始日 | 最後合格日 | 合格日數 | 壓力倍數 | 五日淨買壓 | 買量參與 | 主要買方 |",
        "|---:|---|---|---|---|---:|---:|---:|---:|---|",
        *_general_rows(general),
        "",
        "## 目前不能下的結論",
        "",
        "- 不能由分點資料證明實際受益人、內線或同一主力。",
        "- 公司登記地址不一定是總部或工廠所在地。",
        "- review 排名只代表行為持續性與市場影響，不代表未來報酬排名。",
    ])
    output = OUTPUT_ROOT / "key_broker_anomaly_overview.md"
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
