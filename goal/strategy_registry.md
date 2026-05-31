# Strategy Registry

Updated: 2026-05-31

This file is the canonical registry for the current four strategy candidates.

It is meant to support three use cases:

1. future optimization tracking
2. day-to-day trade assistance
3. avoiding confusion between research logs, strategy families, and actual trade rules

## Status Legend

- `validated candidate`: current repo artifacts show the rule clears the working gate
- `legacy candidate`: worth preserving, but not on the same confidence tier as the validated set

## Current Strategy Set

| ID | Strategy | Status | Theme | Trades | Win rate | Avg net ret | Primary artifact |
| --- | --- | --- | --- | ---: | ---: | ---: | --- |
| S1 | `key_broker_branch_hybrid` | validated candidate | key broker branch abnormal accumulation + breakout context | `24` | `0.6667` | `0.1111` | `outputs/analysis/key_broker_branch_hybrid/key_broker_branch_hybrid_summary.json` |
| S2 | `warrant_leads_stock_refine` | validated candidate | warrant buying leads the stock | `52` | `0.6154` | `0.1143` | `outputs/analysis/rare_event/warrant_leads_stock_refine_report.md` |
| S3 | `repair_branch_topratio0610_daybuy20ge4` | validated candidate | breakout / hot surge chase with chip repair filters | `12` | `1.0000` | `0.2770` | `data/_derived/ml_runs/repair_branch_topratio0610_daybuy20ge4_summary.json` |
| S4 | `accumulation_pre_breakout` | legacy candidate | stock accumulation before breakout | `533` | `0.5347` | `0.0733` | `outputs/analysis/rare_event/rare_event_leaderboard.csv` |

## Standard Tracking Template

Each strategy below is recorded with the same fields:

- `Thesis`: why the rule should work
- `Signal`: the actual filter set
- `Execution`: entry, exit, stop, and capacity
- `Evidence`: current backtest / replay result
- `Use`: how to treat it in real trading assistance
- `Monitor`: what should be checked every refresh cycle
- `Optimize`: the next improvement work that still makes sense

## S1: `key_broker_branch_hybrid`

- Status: `validated candidate`
- Theme: key broker branch abnormal accumulation plus breakout context
- Source direction: original direction 1
- Script: `apps/analysis/run_key_broker_branch_breakout_hybrid.py`

Thesis:

- Specific broker-branch abnormal buying by itself is too noisy.
- It becomes more useful when the stock is also reclaiming prior highs with non-extreme volume.

Signal:

- start from strict branch standalone best signal artifact
- branch anomaly score in the top `10%`
- signal-day close at or above prior 20-day high
- `breakout_gap_20 >= 0`
- `volume_ratio_5_20` between `0.5` and `2.0`
- `ret_5d <= 0.30`

Execution:

- entry: next saved trade under the hybrid replay artifact
- hold: `20` trading days
- stop-loss: none
- take-profit: none
- max positions: artifact-driven, not explicitly defined as a portfolio strategy in the current summary

Evidence:

- trades: `24`
- win rate: `0.6667`
- avg net ret: `0.1111`
- median net ret: `0.0325`
- avg MFE: `0.2207`
- avg MAE: `-0.0706`
- max loss: `-0.1558`
- leave-one-symbol pass: `22 / 23`
- leave-one-month pass: `1 / 4`
- leave-one-broker pass: `18 / 19`

Use:

- Treat as a branch-driven tactical setup, not a production-grade standalone system.
- Best used when a stock already has visible breakout structure and branch behavior is acting as confirmation.

Monitor:

- whether branch anomaly score remains in the top decile
- whether price is still reclaiming prior highs instead of chasing an already extended move
- whether volume remains constructive instead of overheated
- whether month/regime behavior improves on newer windows

Optimize:

- improve leave-one-month robustness
- test branch persistence features instead of a single anomaly snapshot
- combine with warrant confirmation or low prior-reaction filters

Artifacts:

- summary: `outputs/analysis/key_broker_branch_hybrid/key_broker_branch_hybrid_summary.json`
- report: `outputs/analysis/key_broker_branch_hybrid/key_broker_branch_hybrid_report.md`
- trades: `outputs/analysis/key_broker_branch_hybrid/key_broker_branch_hybrid_trades.csv`
- signals: `outputs/analysis/key_broker_branch_hybrid/key_broker_branch_hybrid_signals.parquet`

## S2: `warrant_leads_stock_refine`

- Status: `validated candidate`
- Theme: warrant buying leads the underlying stock
- Source direction: original direction 2
- Script: `apps/analysis/run_warrant_leads_stock_refine.py`

Thesis:

- Strong warrant-side buying can show up before the stock fully reacts.
- The setup is strongest when warrant buying is both extreme and broad, while the stock side is supportive but not yet overcrowded.

Signal:

- `warrant_posnet_pct_cs >= 0.98`
- `warrant_dyn_k_pct_cs >= 0.90`
- `warrant_posnet_strong_days_20 >= 3`
- `stock_posnet_pct_cs` between `0.80` and `0.95`
- `prior_abs_ret_20d <= 0.08`

Execution:

- entry: next open after signal
- hold: up to `40` trading days
- cooldown: `15` days per symbol
- stop-loss: `-10%`
- take-profit: none

Evidence:

- trades: `52`
- win rate: `0.6154`
- avg net ret: `0.1143`
- median net ret: `0.0873`
- avg `mfe_20d`: `0.1752`
- avg `mfe_40d`: `0.2676`
- leave-one-symbol pass: `27 / 28`
- leave-one-month pass: `6 / 8`
- refined scan size: `43,920` specs

Use:

- Treat as an event-driven rare-event strategy.
- Best used when warrant-side positive-net buying is clearly abnormal and the stock has not already moved too far.

Monitor:

- daily warrant positive-net percentile
- daily warrant dynamic-participation percentile
- rolling count of strong warrant-buying days in the last 20 sessions
- whether the stock-side percentile is drifting above the overcrowded zone
- pending forward signals that have not yet completed the full 40-day horizon

Optimize:

- run fixed-rule forward replay on later signals once the 40-day horizon is fully observable
- reduce multiple-testing risk by confirming on fresh windows
- test whether a lighter profit-protection rule improves path quality without killing the edge

Artifacts:

- report: `outputs/analysis/rare_event/warrant_leads_stock_refine_report.md`
- leaderboard: `outputs/analysis/rare_event/warrant_leads_stock_refine_leaderboard.csv`
- trades: `outputs/analysis/rare_event/warrant_leads_stock_refine_trades.parquet`
- signals: `outputs/analysis/rare_event/warrant_leads_stock_refine_signals.parquet`
- case explorer: `outputs/analysis/rare_event/warrant_leads_stock_case_explorer.html`

## S3: `repair_branch_topratio0610_daybuy20ge4`

- Status: `validated candidate`
- Theme: breakout / hot surge chase with chip repair filters
- Source direction: original direction 3
- Script family: `apps/analysis/run_strategy_replay.py`, `apps/analysis/run_direction3_breakout_robustness.py`

Thesis:

- A breakout model can identify names likely to surge in the next 10 trading days.
- Chip-based repair filters improve the candidate pool.
- A day-level quality gate removes weak trading days even when individual names still look tradable.

Signal:

- start from `breakout10_predictions_wf`
- repair universe:
  - `stock_net_buy_days_20 >= 1`
  - `warrant_hhi_posnet_20 <= 0.80`
  - `top_posnet_ratio <= 0.610`
- day-level filter:
  - `mean_buy20 >= 4`
- selection:
  - `top_pct = 0.015`
  - `topk = 3`

Execution:

- entry: next open after signal
- gap cap: `0.5%`
- hold: `16` trading days
- stop-loss: none
- take-profit: none
- max positions: `6`
- allocation: equal-weight in the saved replay

Evidence:

- trades: `12`
- signals: `8`
- win rate: `1.0000`
- avg net ret: `0.2770`
- median net ret: `0.2317`
- total net ret: `0.6625`
- rolling strict pass: `6 / 6`
- leave-one-symbol pass: `9 / 9`
- leave-one-month pass: `3 / 3`
- leave-one-signal-day pass: `8 / 8`

Use:

- Treat as the current highest-conviction breakout candidate.
- Best used as a selective continuation strategy, not as a broad market scanner.

Monitor:

- whether trusted walk-forward predictions are still being refreshed
- whether the day-level `mean_buy20` condition still separates good and bad signal days
- whether gap control is still preventing bad chases
- whether new replay windows preserve the current robustness profile

Optimize:

- extend trusted OOF / walk-forward prediction artifacts beyond `2026-02-03`
- test whether a limited drawdown control can reduce MAE without damaging expectancy
- monitor whether the sample stays too sparse for production sizing

Artifacts:

- summary: `data/_derived/ml_runs/repair_branch_topratio0610_daybuy20ge4_summary.json`
- trades: `data/_derived/ml_runs/repair_branch_topratio0610_daybuy20ge4_trades.csv`
- rolling: `data/_derived/ml_runs/repair_branch_topratio0610_daybuy20ge4_rolling_windows.csv`
- robustness report: `outputs/analysis/direction3_breakout_robustness/direction3_breakout_robustness_report.md`
- forward-readiness report: `outputs/analysis/strategy_forward_readiness/strategy_forward_readiness_report.md`

## S4: `accumulation_pre_breakout`

- Status: `legacy candidate`
- Theme: stock accumulation before breakout
- Source line: rare-event research line, retained as a preserved candidate
- Source direction mapping: closest to direction 1 style accumulation logic, but not identical to the later branch-level direction-1 hybrid

Thesis:

- Strong stock-side accumulation can appear before a visible breakout.
- Warrant flow is only a supporting confirmation here, not the main trigger.

Signal:

- representative preserved spec:
  - `hold_days = 30`
  - `cooldown_days = 15`
  - `breakout_gap_20 <= 0.02`
  - `stock_posnet_pct_cs >= 0.98`
  - `stock_posnet_strong_days_20 >= 3`
  - `volume_ratio_5_20 <= 1.8`
  - `warrant_posnet_floor >= 0.5`
  - `stop_loss = -10%`

Execution:

- entry: event-driven rare-event replay entry from the preserved leaderboard spec
- hold: `30` trading days
- cooldown: `15` days
- stop-loss: `-10%`
- take-profit: none in the preserved spec

Evidence:

- trades: `533`
- win rate: `0.5347`
- avg net ret: `0.0733`
- median net ret: `0.0137`
- avg `mfe_20d`: `0.1625`
- avg `mfe_40d`: `0.2572`
- profit factor: `2.9772`
- payoff ratio: `2.5907`
- max loss: `-0.1043`
- test trades: `76`
- test win rate: `0.6447`
- test avg net ret: `0.1411`

Use:

- Do not treat this as equal to the current validated set.
- Keep it as a broad accumulation watchlist framework and a fallback research branch.
- Useful when the objective is to find early accumulation candidates before a cleaner breakout rule is available.

Monitor:

- whether corrected-feature reruns still preserve this edge
- whether the strategy remains too broad or too frequent for the intended rare-event use
- whether stock-side strength is simply capturing generic momentum instead of a distinct event

Optimize:

- revalidate under the corrected feature set with the same rigor used for the validated strategies
- narrow the signal set so it behaves more like a high-conviction event family
- test whether branch or warrant context can improve selectivity

Artifacts:

- research log: `goal/rare_event_strategy_log.md`
- leaderboard: `outputs/analysis/rare_event/rare_event_leaderboard.csv`
- summary context: `outputs/analysis/rare_event/best_rare_event_report.md`

## Practical Use Order

For current trading assistance, the working priority should be:

1. `S3` as the primary selective breakout candidate
2. `S2` as the primary rare-event candidate
3. `S1` as a tactical branch-context candidate
4. `S4` as a preserved research/watchlist candidate

## Update Rule

When any strategy is rerun or promoted, update these fields together:

- status
- signal
- execution
- evidence
- monitor
- optimize
- artifact paths
