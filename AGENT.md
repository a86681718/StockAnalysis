# AGENT.md

## Overview

`StockAnalysis` is a Python 3.11 repository for Taiwan market data collection, ETL, exploratory analysis, and a Dash-based visualization app.

The repo is not organized as a single application. Treat it as a collection of:

- reusable library code under `src/stockanalysis`
- runnable data jobs under `apps/`
- cloud deployment handlers under `deployment/`
- notebooks for exploration under `notebooks/`

## Repo Layout

- `src/stockanalysis/`
  - shared config/path helpers in `config.py`
  - common utilities in `commonlib.py`
  - newer CLI-style analysis modules in `analysis/`
- `apps/crawlers/`
  - older standalone crawlers and data fetchers
- `apps/twse/`, `apps/tpex/`
  - exchange-specific crawler services and containerized jobs
- `apps/etl/`
  - ETL scripts and a separate `uv` workspace member
- `apps/analysis/`
  - one-off and batch analysis scripts, mostly `argparse` driven
- `apps/visualization/app.py`
  - Dash app for OHLC + broker/event inspection
- `deployment/`
  - Cloud Function / Cloud Run helper code
- `conf/default.properties`
  - default path config
- `data/`
  - local datasets and derived parquet/csv inputs
- `outputs/`
  - generated artifacts and temporary outputs

## Environment

- Python: `>=3.11,<3.12` at the repo root
- Package manager/build metadata:
  - root project uses `pyproject.toml`
  - `apps/etl/pyproject.toml` is a nested workspace member
- There is an existing `.venv/` in the repo

Prefer these commands from the repo root:

```bash
python3 -m compileall src apps deployment
python3 apps/visualization/app.py
python3 -m stockanalysis.analysis.chip_concentration_backtest --help
python3 -m stockanalysis.analysis.high_concentration_scan --help
python3 -m stockanalysis.analysis.warrant_concentration_scan --help
```

If dependency sync is needed, inspect `pyproject.toml` and `uv.lock` first and keep the root project and `apps/etl` workspace consistent.

## Data And Paths

Use the helpers in `src/stockanalysis/config.py`:

- `PROJECT_ROOT`
- `resolve_data(...)`
- `resolve_output(...)`
- `resolve_assets(...)`
- `ensure_dir(...)`

Prefer those helpers over hardcoded relative paths. Some older code still uses direct `Path("data/...")` access; new changes should not add more of that pattern.

`conf/default.properties` currently points all environments back to repo-local paths:

- `./data`
- `./outputs`
- `./assets`

## Working Conventions

- Prefer small, local edits. This repo mixes legacy scripts with newer library code.
- Reuse existing `argparse` CLIs when extending analysis workflows.
- Keep side effects out of import time when touching deployment code. Several cloud handlers currently initialize GCP metadata and clients at module import; avoid spreading that pattern.
- For crawlers, preserve request headers and source-specific parsing logic unless the site contract is clearly being updated.
- For new output locations, create directories through `ensure_dir(...)`.

## Verification

Minimum verification for Python changes:

```bash
python3 -m compileall src apps deployment
```

Then run the narrowest relevant entry point:

- analysis module: run its `--help` or a small local sample
- Dash app: start `python3 apps/visualization/app.py`
- crawler/deployment script: prefer function-level smoke checks over full remote calls unless credentials/network are available

There is no established automated test suite in the repository right now, so compile checks and targeted smoke runs matter.

## Known Project Risks

These are current repo-level hazards worth keeping in mind before making changes:

- `apps/crawlers/Crawler_HistoricalDividendInfo.py` does not compile because of a bad indentation block.
- `apps/visualization/app.py` reads from hardcoded `data/...` paths instead of the shared path helpers, which makes the app sensitive to working directory and env overrides.
- several network callers use `verify=False`; be careful not to extend that pattern without a strong reason.
- some deployment modules resolve GCP metadata at import time, which makes local import and test workflows brittle outside GCP.
- `apps/etl/main.py` is still a placeholder and should not be treated as the real ETL entry point.

## Good First Files To Read

- `README.md`
- `pyproject.toml`
- `src/stockanalysis/config.py`
- `src/stockanalysis/commonlib.py`
- `apps/visualization/app.py`
- one target script in `apps/analysis/` or `apps/twse/` depending on the task

## When Updating This File

Update this document when any of these change:

- top-level layout
- standard run commands
- dependency manager workflow
- path/config conventions
- test/verification expectations
