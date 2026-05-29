# Strategy Direction Summary

Updated: 2026-05-29

This note maps the three requested trading directions to the current evidence in this repo.

## Current Answer

The strongest high-return strategy so far is direction 3, with direction 1 used as a chip-quality filter:

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
- robustness report: `outputs/analysis/direction3_breakout_robustness/direction3_breakout_robustness_report.md`
- forward-readiness report: `outputs/analysis/strategy_forward_readiness/strategy_forward_readiness_report.md`

Verified result on the current walk-forward validation window:

- window: `2025-11-01` to `2026-02-03`
- trades: `12`
- signals: `8`
- win rate: `1.0000`
- mean net return: `0.2770`
- median net return: `0.2317`
- total net return: `0.6625`
- rolling windows: `6 / 6` windows have positive strict pass behavior in the saved rolling check
- leave-one-symbol target pass: `9 / 9`
- leave-one-month target pass: `3 / 3`
- leave-one-signal-day target pass: `8 / 8`
- max entry gap in saved trades: `0.0047`
- entry gaps above the configured `0.5%` cap: `0`
- minimum entry-day turnover proxy (`close * volume`): `529,363,695`

This candidate clearly exceeds both target gates:

- win rate greater than `50%`
- average return greater than `10%`

It is still sparse, so it should be treated as the strongest validated candidate in the current data, not as a finished production strategy.

Direction 2 now also has a target-clearing rare-event candidate after the focused warrant refinement scan:

- theme: warrant concentrated/dynamic buying leads the underlying stock
- script: `apps/analysis/run_warrant_leads_stock_refine.py`
- report: `outputs/analysis/rare_event/warrant_leads_stock_refine_report.md`
- leaderboard: `outputs/analysis/rare_event/warrant_leads_stock_refine_leaderboard.csv`
- rule:
  - `warrant_posnet_pct_cs >= 0.98`
  - `warrant_dyn_k_pct_cs >= 0.90`
  - `warrant_posnet_strong_days_20 >= 3`
  - `stock_posnet_pct_cs` between `0.80` and `0.95`
  - `prior_abs_ret_20d <= 0.08`
  - `hold_days = 40`
  - `cooldown_days = 15`
  - `stop_loss = -10%`
- full trades: `52`
- full win rate: `0.6154`
- full mean net return: `0.1143`
- full median net return: `0.0873`
- test trades: `12`
- test win rate: `0.7500`
- test mean net return: `0.1342`
- leave-one-symbol robust pass: `27 / 28`
- leave-one-month robust pass: `6 / 8`

This direction now clears the requested gates, but it has higher multiple-testing risk than the direction 3 candidate because the refinement scan evaluated `43,920` related specs. It is also sensitive to dropping `2025-09`, which pulls full average return below `10%`.

## Direction 1: Key Broker Branch / Abnormal Broker Accumulation

Closest current strategy family:

- `accumulation_pre_breakout`
- source artifact: `outputs/analysis/rare_event/rare_event_leaderboard.csv`
- supporting feature role in the final candidate:
  - `stock_net_buy_days_20`
  - `top_posnet_ratio`
  - day-level `mean_buy20`

True branch-level scan:

- script: `apps/analysis/run_key_broker_branch_scan.py`
- artifact report: `outputs/analysis/key_broker_branch/key_broker_branch_report.md`
- artifact leaderboard: `outputs/analysis/key_broker_branch/key_broker_branch_leaderboard.csv`
- direction 3 overlay script: `apps/analysis/run_direction3_branch_overlay.py`
- direction 3 overlay report: `outputs/analysis/direction3_branch_overlay/direction3_branch_overlay_report.md`
- scope:
  - all current 4-digit non-ETF stock parquet files with OHLC coverage
  - signal window `2025-10-01` to `2026-02-03`
  - history start `2025-07-01`
  - feature rows evaluated: `193459`
  - focused grid candidates: `16`
- result:
  - passing candidates: `0`
  - best branch-level standalone rule:
    - `window_days=3`
    - `window_net_ratio >= 8.0`
    - `branch_buy_share >= 0.12`
    - `window_net_buy_ratio >= 0.60`
    - `branch_posnet_share >= 0.20`
    - `hold_days=20`
    - `cooldown_days=15`
  - trades: `2131`
  - win rate: `0.4139`
  - mean net return: `0.0073`
  - median net return: `-0.0094`

Direction 3 overlay result:

- strict standalone branch exact coverage on selected direction-3 trades: `0 / 12`
- strict standalone branch exact coverage on direction-3 day candidates: `1 / 690`
- looser true-branch exact coverage on selected direction-3 trades: `12 / 12`
- looser strong-branch exact coverage on selected direction-3 trades: `5 / 12`
- looser strong-branch exact coverage on direction-3 day candidates: `206 / 690`
- candidate label hit under looser strong branch: `0.3786`, versus `0.3971` for all day candidates

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
- As a standalone strategy, both the aggregate rare-event version and the true branch-level scan are below the target.
- The branch-level result is especially important: naive "specific branch abnormal buying" alone is too noisy and does not beat the target.
- The direction-3 overlay is also important: strict standalone branch signals should not be used as a hard gate for the current breakout candidate, because they would eliminate all selected trades.
- Looser branch-level behavior is present in the selected breakout trades, but it is too broad to improve selection by itself.
- The next useful research step is to keep branch-level features, but attach them to price/warrant context instead of using them as a standalone entry trigger:
  - branch-specific abnormal buy streaks before breakout
  - branch persistence by stock combined with low prior price reaction
  - branch concentration plus warrant confirmation
  - exclusion of already overheated names

## Direction 2: Warrant Concentrated Buying / Warrant Leads Stock

Closest current strategy family:

- `warrant_leads_stock`
- source artifacts:
  - `outputs/analysis/rare_event/best_rare_event_report.md`
  - `outputs/analysis/rare_event/warrant_leads_stock_refine_report.md`

Current best refined rare-event strategy:

- event name: `warrant_leads_stock__h40__cd15__filter_suite-warrant_dyn_k__prior_abs_ret_20d-0p08__stock_posnet_cap-0p95__stock_posnet_floor-0p8__stop_loss--0p1__warrant_dyn_k_pct_cs_floor-0p9__warrant_posnet_floor-0p98__warrant_posnet_strong_days_20-3`
- rule summary:
  - warrant positive-net percentile at least `0.98`
  - warrant dynamic concentration percentile at least `0.90`
  - stock positive-net percentile between `0.80` and `0.95`
  - prior 20-day absolute return at most `8%`
  - stop loss `-10%`
  - hold up to `40` trading days
- scan size: `43,920` specs
- robust target-pass specs: `1,030`
- full trades: `52`
- full win rate: `0.6154`
- full mean net return: `0.1143`
- full median net return: `0.0873`
- full profit factor: `4.2203`
- full max loss: `-0.1043`
- test trades: `12`
- test win rate: `0.7500`
- test mean net return: `0.1342`
- leave-one-symbol robust pass: `27 / 28`
- leave-one-month robust pass: `6 / 8`

Verdict:

- This direction now clears both requested target gates in the current evidence.
- It is a stronger standalone candidate than the previous direction-2 baseline.
- Main caveat: it came from a broad refinement scan, and leave-one-month shows sensitivity to `2025-09` and the current test-window sample size.
- It should be treated as a target-clearing rare-event candidate that still needs out-of-sample hardening.

## Direction 3: Breakout / Hot Surge Chase

Closest current strategy:

- `breakout10` walk-forward prediction plus broker-flow repair filters
- source artifacts:
  - `data/_derived/ml_runs/repair_branch_topratio0610_daybuy20ge4_summary.json`
  - `data/_derived/ml_runs/repair_branch_topratio0610_daybuy20ge4_trades.csv`
  - `data/_derived/ml_runs/repair_branch_topratio0610_daybuy20ge4_rolling_windows.csv`
  - `outputs/analysis/direction3_breakout_robustness/direction3_breakout_robustness_report.md`
  - `outputs/analysis/direction3_branch_overlay/direction3_branch_overlay_report.md`

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
- leave-one-symbol target pass: `9 / 9`
- leave-one-month target pass: `3 / 3`
- leave-one-signal-day target pass: `8 / 8`
- worst single-trade MAE while held: `-0.1602`
- entry gap violations above `0.5%`: `0`
- strict branch standalone exact coverage: `0 / 12`
- looser true-branch exact coverage: `12 / 12`

Verdict:

- This is the current primary strategy candidate.
- It satisfies the target gates in the current saved walk-forward evidence.
- New robustness checks show it is not dependent on one symbol, month, or signal day inside the saved replay.
- True branch-level overlay supports keeping branch behavior as a context feature, not as a hard gate.
- Main weakness is still sample size and the need for newer out-of-sample replay data, not current return quality.

## Forward Readiness / Newer Data Boundary

The current repo state can audit the saved target-clearing candidates, but it cannot honestly claim a newer forward replay beyond the saved trusted windows.

Audit artifact:

- script: `apps/analysis/run_strategy_forward_readiness_audit.py`
- report: `outputs/analysis/strategy_forward_readiness/strategy_forward_readiness_report.md`
- summary: `outputs/analysis/strategy_forward_readiness/strategy_forward_readiness_summary.json`

Direction 3 boundary:

- OHLC max date: `2026-02-26`
- trusted `breakout10_predictions_wf.csv` max date: `2026-02-03`
- weaker `breakout10_predictions_all_wf.csv` max date: `2026-02-26`
- saved filtered strategy signal max date: `2026-01-22`
- saved trade exit max date: `2026-02-25`
- last complete 16-trading-day signal date from OHLC: `2026-01-23`
- conclusion: do not extend the current direction-3 conclusion with `all_wf`; regenerate trusted OOF / walk-forward predictions after `2026-02-03`, then replay the exact fixed rule.

Direction 2 boundary:

- feature max date: `2026-02-26`
- refined signal max date: `2026-02-23`
- saved full-trade signal max date: `2025-12-16`
- last complete 40-trading-day signal date from OHLC: `2025-12-18`
- pending signals: `16`, dated `2025-12-23` to `2026-02-23`
- pending signal export: `outputs/analysis/strategy_forward_readiness/direction2_pending_signals.csv`
- conclusion: direction 2 already has later fixed-rule signals, but the 40-day horizon is not fully observable until more OHLC data arrives.

## Ranking

1. Direction 3 plus direction 1 filter: primary candidate, target met.
2. Direction 2: target-clearing rare-event candidate after refinement, larger sample than direction 3 but higher multiple-testing and month-sensitivity risk.
3. Direction 1 standalone: useful signal family, but current true branch-level standalone scan fails both win-rate and return targets.

## Next Research Step

The next high-value step is not another broad grid search. It is to harden the two target-clearing candidates:

- extend the same replay to newer OOF data when available
- for direction 3, regenerate trusted OOF / walk-forward breakout predictions after `2026-02-03`, then replay the same fixed thresholds
- for direction 2, wait for enough OHLC to complete the `2025-12-23` to `2026-02-23` pending signals, then score them without changing thresholds
- inspect direction 3 trades manually for qualitative market context; quantitative liquidity/gap checks are now saved
- derive lighter branch-level context features for the direction 3 model/day filter, because strict standalone branch gating failed the overlay check
- add a stricter out-of-sample or forward replay for `warrant_leads_stock_refine`, because the current direction-2 result came from a broad parameter scan
