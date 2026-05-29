# Strategy Direction Summary

Updated: 2026-05-29

This note maps the three requested trading directions to the current evidence in this repo.

## Current Answer

The strongest validated strategy so far is direction 3, with direction 1 used as a chip-quality filter:

- theme: breakout-chase / early surge continuation
- universe: `repair branch + top_posnet_ratio <= 0.610`
- day-level filter: `mean_buy20 >= 4`
- selection: `top_pct=0.015`, `gap_th=0.005`, `hold_days=16`, `max_positions=6`
- entry: next open after signal
- exit: close after 16 trading days
- stop/take-profit: none in the current best replay
- artifact summary: `data/_derived/ml_runs/repair_branch_topratio0610_daybuy20ge4_summary.json`
- artifact trades: `data/_derived/ml_runs/repair_branch_topratio0610_daybuy20ge4_trades.csv`
- artifact rolling windows: `data/_derived/ml_runs/repair_branch_topratio0610_daybuy20ge4_rolling_windows.csv`

Verified result on the current walk-forward validation window:

- window: `2025-11-01` to `2026-02-03`
- trades: `12`
- signals: `8`
- win rate: `1.0000`
- mean net return: `0.2770`
- median net return: `0.2317`
- total net return: `0.6625`
- rolling windows: `6 / 6` windows have positive strict pass behavior in the saved rolling check

This is the only current candidate that clearly exceeds both target gates:

- win rate greater than `50%`
- average return greater than `10%`

It is still sparse, so it should be treated as the strongest validated candidate in the current data, not as a finished production strategy.

## Direction 1: Key Broker Branch / Abnormal Broker Accumulation

Closest current strategy family:

- `accumulation_pre_breakout`
- source artifact: `outputs/analysis/rare_event/rare_event_leaderboard.csv`
- supporting feature role in the final candidate:
  - `stock_net_buy_days_20`
  - `top_posnet_ratio`
  - day-level `mean_buy20`

Best current standalone rare-event candidate in this family:

- event name: `accumulation_pre_breakout__h30__cd15__breakout_gap_20_cap-0p02__stock_posnet_pct_cs-0p98__stock_posnet_strong_days_20-3__stop_loss--0p1__volume_ratio_5_20_cap-1p8__warrant_posnet_floor-0p5`
- full trades: `533`
- full win rate: `0.5347`
- full mean net return: `0.0733`
- test trades: `76`
- test win rate: `0.6447`
- test mean net return: `0.1411`

Verdict:

- This direction has signal value, especially as a filter for direction 3.
- As a standalone strategy, the current full-window average return is below the `10%` target.
- The next useful research step is to convert this from aggregate concentration into true branch-level key-broker behavior:
  - broker-specific abnormal buy streaks
  - branch persistence by stock
  - branch concentration before price reaction
  - exclusion of already overheated names

## Direction 2: Warrant Concentrated Buying / Warrant Leads Stock

Closest current strategy family:

- `warrant_leads_stock`
- source artifact: `outputs/analysis/rare_event/best_rare_event_report.md`

Current best rare-event strategy:

- event name: `warrant_leads_stock__h40__cd15__prior_abs_ret_20d-0p08__stock_posnet_cap-0p9__stock_posnet_floor-0p7__stop_loss--0p1__warrant_posnet_floor-0p98__warrant_posnet_strong_days_20-3`
- rule summary:
  - warrant positive-net percentile at least `0.98`
  - stock positive-net percentile between `0.70` and `0.90`
  - prior 20-day absolute return at most `8%`
  - stop loss `-10%`
  - hold up to `40` trading days
- full trades: `81`
- full win rate: `0.5062`
- full mean net return: `0.0913`
- full MFE 40d: `0.2395`
- test trades: `14`
- test mean net return: `0.0321`

Verdict:

- This direction is valid as an asymmetric rare-event setup.
- It barely clears the `50%` win-rate target, but the full-window average net return is still below `10%`.
- It should stay as a secondary candidate and as a confirmation layer for breakout or broker-flow strategies.

## Direction 3: Breakout / Hot Surge Chase

Closest current strategy:

- `breakout10` walk-forward prediction plus broker-flow repair filters
- source artifacts:
  - `data/_derived/ml_runs/repair_branch_topratio0610_daybuy20ge4_summary.json`
  - `data/_derived/ml_runs/repair_branch_topratio0610_daybuy20ge4_trades.csv`
  - `data/_derived/ml_runs/repair_branch_topratio0610_daybuy20ge4_rolling_windows.csv`

Current best rule:

- start from `breakout10_predictions_wf`
- keep refined chip universe:
  - `stock_net_buy_days_20 >= 1`
  - `warrant_hhi_posnet_20 <= 0.80`
  - `top_posnet_ratio <= 0.610`
- only trade signal days where the selected candidates have:
  - `mean_buy20 >= 4`
- select top `1.5%` by prediction score
- require `gap_th >= 0.005`
- cap positions at `6`
- hold `16` trading days

Verified result:

- trades: `12`
- win rate: `1.0000`
- mean net return: `0.2770`
- rolling strict pass: `6 / 6`

Verdict:

- This is the current primary strategy candidate.
- It satisfies the target gates in the current saved walk-forward evidence.
- Main weakness is sample size, not return quality.

## Ranking

1. Direction 3 plus direction 1 filter: primary candidate, target met.
2. Direction 2: good rare-event candidate, target almost met, useful confirmation layer.
3. Direction 1 standalone: useful signal family, but current standalone average return is not high enough.

## Next Research Step

The next high-value step is not another broad grid search. It is to harden the primary candidate:

- extend the same replay to newer OOF data when available
- add leave-one-symbol-out and leave-one-month-out checks for `repair_branch_topratio0610_daybuy20ge4`
- inspect the 12 trades manually for liquidity, limit-up execution risk, and repeated-symbol concentration
- build a true key-broker branch feature set for direction 1 instead of only using aggregate chip concentration
