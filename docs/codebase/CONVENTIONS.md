# Coding Conventions

## Core Sections (Required)

### 1) Naming Rules

| Item | Rule | Example | Evidence |
|------|------|---------|----------|
| Files | New package modules generally use lowercase snake case; older scripts retain mixed PascalCase/versioned names | `broker_branch_accumulation.py`; `Analysis_BsReport_v4.py` | `src/stockanalysis/analysis/`, `apps/analysis/` |
| Functions/methods | Lowercase snake case | `build_core_feature_table`, `resolve_output` | `src/stockanalysis/analysis/broker_branch_accumulation.py`, `src/stockanalysis/config.py` |
| Types/classes | PascalCase | `DetectionConfig`, `BsReportEtl`, `Turnstile` | `src/stockanalysis/analysis/broker_branch_accumulation.py`, `apps/etl/bs_report_pipeline.py` |
| Constants/env vars | Upper snake case; internal module constants may begin with `_` | `DATA_DIR`, `STOCK_CRAWLER_BUCKET`, `_DEFAULT_ROOT` | `src/stockanalysis/config.py`, `src/stockanalysis/runtime/crawlers/twse_bs_report.py` |

### 2) Formatting and Linting

- Formatter: `[TODO]` no formatter config was found.
- Linter: `[TODO]` no root Ruff, Flake8, Pylint, or equivalent config was found.
- Enforced rules: `[TODO]` no automated repository-wide rules are evidenced.
- Run commands: no verified lint/format command. Tests run with `PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p 'test_*.py'`.
- Observed modern modules commonly use `from __future__ import annotations`, type hints, `pathlib.Path`, and dataclasses; older scripts often use module-level execution and f-string logging.

### 3) Import and Module Conventions

- Standard-library imports normally precede third-party and local imports, but no formatter enforces grouping.
- Reusable code imports from `stockanalysis.*`; direct scripts either require `PYTHONPATH=src` or insert the repo `src` path into `sys.path`.
- No barrel/public-export convention is present; `__init__.py` files are minimal.
- Compatibility wrappers use `runpy.run_module(..., run_name="__main__")` to delegate to runtime modules.

### 4) Error and Logging Conventions

- Cloud HTTP services validate basic payload shape, catch top-level exceptions, log them, and return HTTP status/text or JSON.
- Crawlers use bounded retry loops and log attempt-specific failures; ETL uses exceptions from subprocess/file operations plus manifest status.
- Logging is primarily Python `logging` with `%(asctime)s [%(levelname)s] %(message)s`; some older scripts use `print`.
- Sensitive-data redaction policy: `[TODO]` no documented or enforced policy was found. Current TPEX proxy helpers deliberately format proxy labels without passwords, but this is not repository-wide policy.

### 5) Testing Conventions

- Tests live in `tests/`, use `test_*.py`, classes derived from `unittest.TestCase`, and descriptive `test_*` methods.
- Tests construct small pandas frames and use `tempfile.TemporaryDirectory`; no general fixture framework is present.
- Coverage expectation: `[TODO]` no coverage configuration or threshold was found.

### 6) Evidence

- `src/stockanalysis/config.py`
- `src/stockanalysis/analysis/broker_branch_accumulation.py`
- `apps/etl/run_bs_report_etl.py`
- `apps/tpex/crawler-tpex-bsreport.py`
- `deployment/trigger-twse-job/main.py`
- `tests/test_broker_branch_accumulation.py`
- `pyproject.toml`
