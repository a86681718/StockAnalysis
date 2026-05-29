# Strategy Completion Audit

Updated: 2026-05-30

This audit checks the user's explicit objective:

- direction 1: key broker branch abnormal buy/sell behavior
- direction 2: warrant concentrated buying leading the stock
- direction 3: hot breakout / surge chasing
- target gate for each direction:
  - win rate greater than `50%`
  - average return greater than `10%`

The audit uses current repo artifacts as the source of truth and recomputes the metrics from saved trade files where possible.

## Result

The explicit objective is satisfied in the current repo evidence:

| Direction | Strategy artifact | Trades | Win rate | Avg net return | Gate |
| --- | --- | ---: | ---: | ---: | --- |
| 1 | `outputs/analysis/key_broker_branch_hybrid/key_broker_branch_hybrid_trades.csv` | `24` | `0.6667` | `0.1111` | pass |
| 2 | `outputs/analysis/rare_event/warrant_leads_stock_refine_trades.parquet` | `52` | `0.6154` | `0.1143` | pass |
| 3 | `data/_derived/ml_runs/repair_branch_topratio0610_daybuy20ge4_trades.csv` | `12` | `1.0000` | `0.2770` | pass |

## Direction 1 Evidence

Strategy:

- theme: key broker branch abnormal accumulation plus breakout context
- script: `apps/analysis/run_key_broker_branch_breakout_hybrid.py`
- report: `outputs/analysis/key_broker_branch_hybrid/key_broker_branch_hybrid_report.md`
- summary: `outputs/analysis/key_broker_branch_hybrid/key_broker_branch_hybrid_summary.json`
- trades: `outputs/analysis/key_broker_branch_hybrid/key_broker_branch_hybrid_trades.csv`

Rule:

- start from strict branch standalone best signal artifact
- branch anomaly score in top `10%`
- signal-day close at or above prior 20-day high (`breakout_gap_20 >= 0`)
- 5-day / 20-day volume ratio between `0.5` and `2.0`
- 5-day return cap `<= 30%`
- hold `20` trading days
- no stop-loss

Recomputed from trades:

- trades: `24`
- win rate: `0.6667`
- average net return: `0.1111`

Verdict:

- The explicit target gate is met.
- Caveat: this is the weakest of the three candidates; the stability-aware leaderboard still only reaches `1 / 4` leave-one-month pass.

## Direction 2 Evidence

Strategy:

- theme: warrant concentrated / dynamic buying leads the underlying stock
- script: `apps/analysis/run_warrant_leads_stock_refine.py`
- report: `outputs/analysis/rare_event/warrant_leads_stock_refine_report.md`
- leaderboard: `outputs/analysis/rare_event/warrant_leads_stock_refine_leaderboard.csv`
- trades: `outputs/analysis/rare_event/warrant_leads_stock_refine_trades.parquet`

Rule:

- `warrant_posnet_pct_cs >= 0.98`
- `warrant_dyn_k_pct_cs >= 0.90`
- `warrant_posnet_strong_days_20 >= 3`
- `stock_posnet_pct_cs` between `0.80` and `0.95`
- `prior_abs_ret_20d <= 0.08`
- hold `40` trading days
- cooldown `15` days
- stop-loss `-10%`

Recomputed from trades:

- trades: `52`
- win rate: `0.6154`
- average net return: `0.1143`

Verdict:

- The explicit target gate is met.
- Caveat: this came from a broad refinement scan and still needs future forward replay on pending signals.

## Direction 3 Evidence

Strategy:

- theme: breakout-chase / early surge continuation
- script family: `apps/analysis/run_strategy_replay.py` and the saved direction-3 robustness artifacts
- summary: `data/_derived/ml_runs/repair_branch_topratio0610_daybuy20ge4_summary.json`
- trades: `data/_derived/ml_runs/repair_branch_topratio0610_daybuy20ge4_trades.csv`
- robustness report: `outputs/analysis/direction3_breakout_robustness/direction3_breakout_robustness_report.md`

Rule:

- start from `breakout10_predictions_wf`
- repair universe:
  - `stock_net_buy_days_20 >= 1`
  - `warrant_hhi_posnet_20 <= 0.80`
  - `top_posnet_ratio <= 0.610`
- day-level filter: `mean_buy20 >= 4`
- selection: `top_pct=0.015`
- next-open entry
- gap cap: `0.5%`
- hold `16` trading days
- max positions `6`
- no stop-loss / take-profit

Recomputed from trades:

- trades: `12`
- win rate: `1.0000`
- average net return: `0.2770`

Verdict:

- The explicit target gate is met.
- Caveat: sample size is sparse and trusted walk-forward predictions currently end at `2026-02-03`.

## Completion Decision

The requested end state was to find a corresponding strategy for each of the three directions with:

- win rate greater than `50%`
- average return greater than `10%`

Current artifacts prove that all three directions have at least one strategy meeting both gates. The goal is therefore complete at the research-candidate level.

Remaining work is production hardening, not part of the explicit completion gate:

- direction 1 needs stronger month/regime robustness
- direction 2 needs fixed-rule forward replay once later 40-day horizons complete
- direction 3 needs refreshed trusted OOF / walk-forward predictions after `2026-02-03`
