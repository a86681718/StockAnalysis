# Project Structure

## Target layout

This repository now treats `deployment/` as the public GCP deployment surface and `src/stockanalysis/runtime/` as the implementation layer for reusable local runtime code such as crawlers.

```text
deployment/
  prepare-twse-list/
  prepare-tpex-list/
  trigger-twse-job/
  trigger-tpex-job/

src/stockanalysis/runtime/
  crawlers/
    twse_bs_report.py
    tpex_local_runner.py
    tpex_debug_browser.py
    tpex_turnstile_smoke.py
    twse_daily_ohlc.py
    tpex_daily_ohlc.py

src/stockanalysis/workflows/
  cloud_dispatch.py

apps/
  twse/
  tpex/
  analysis/
    configs/
    reports/
  etl/
    run_bs_report_etl.py
    bs_report_pipeline.py
  visualization/

legacy/
  apps/
    twse/
    tpex/
```

## Responsibilities

- `deployment/`
  - Cloud Run source directories
  - stable deploy paths referenced by `commands`
- `src/stockanalysis/runtime/crawlers/`
  - actual crawler implementations used by local wrappers and containers
- `src/stockanalysis/workflows/`
  - shared prepare/trigger orchestration; deployment wrappers provide market configuration and GCP clients
- `apps/twse/` and `apps/tpex/`
  - thin runnable wrappers plus Docker and Cloud Build files
- `apps/analysis/configs/`
  - shared analysis JSON configs and parameter presets
- `apps/analysis/reports/`
  - report-generation scripts, with compatibility wrappers kept at the analysis root
- `apps/etl/run_bs_report_etl.py`
  - main local bs-report ETL orchestration entry point
- `legacy/`
  - non-primary service layouts preserved for reference

## Data path policy

The repository should not assume repo-local datasets as the primary working mode.

Use environment variables instead:

- `STOCKANALYSIS_DATA_DIR`
- `STOCKANALYSIS_OUTPUT_DIR`
- `STOCKANALYSIS_ASSETS_DIR`
- `STOCKANALYSIS_CONF_DIR`

Example:

```bash
export STOCKANALYSIS_DATA_DIR=/Volumes/stockanalysis/data
export STOCKANALYSIS_OUTPUT_DIR=/Volumes/stockanalysis/outputs
```

The defaults still point to repo-local paths for compatibility, but future operational use should prefer external directories.
