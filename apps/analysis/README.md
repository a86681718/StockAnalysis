# Analysis Layout

`apps/analysis/` remains a script workspace, but the files are grouped conceptually as follows.

## Layout

- `configs/`
  - shared JSON config and parameter files
- `reports/`
  - report generation and plotting scripts
- repo root of `apps/analysis/`
  - main experiment and training entry points

Compatibility wrappers remain at the root for moved report scripts.

## BsReport lineage

- `Analysis_BsReport_v1.py`
- `Analysis_BsReport_v2.py`
- `Analysis_BsReport_v3.py`
- `Analysis_BsReport_v4.py`
- `Plot_BsReport_event.py`

These are the main bs-report research iterations. If you need a current baseline, start with `Analysis_BsReport_v4.py`.

## ML and search jobs

- `run_ml_bband_surge.py`
- `run_ml_breakout10.py`
- `run_direction3_pipeline.py`
- `run_ml_mfe10.py`
- `run_ml_mfe10_two_stage.py`
- `run_ml_mfe10_penalty_search.py`
- `run_ml_topn_backtest.py`
- `run_strategy_search.py`
- `run_strategy_search_oof.py`

Related parameter files are now under `configs/`:

- `configs/config.json`
- `configs/config_v4.json`
- `configs/config_stage1_best.json`
- `configs/grid_stage1_1000.json`
- `configs/top10_stage1_params.json`

## Event and report generation

- `run_backtest_stage1.py`
- `run_tune_stage1_v3.py`
- `run_bband_rebound_volume_scan.py`
- `reports/make_event_report.py`
- `reports/make_ml_predictions_report.py`
- `reports/Plot_BsReport_event.py`

## Older exploratory scripts

- `Analysis_KeyInvestorDetection.py`
- `Analysis_OneNightTrading.py`
- `Analysis_RealTimeDetectBbandSign.py`
- `Analysis_WarrantKeyInvestor.py`
- `Tuning_v1.py`

## Why files were not moved

Several scripts resolve config paths relative to `__file__`. To avoid breaking those jobs during the infrastructure cleanup, this directory is documented first and can be moved in a later pass with targeted script fixes.

Phase 8 also found nine direct script-to-script imports in this directory,
concentrated in the ML, strategy, Direction 3, key-broker, and warrant research
commands. They remain research-level coupling rather than package APIs. No
module under `src/stockanalysis/` imports `apps.analysis`; promote a calculation
into the package only after its behavior and callers have dedicated tests.
