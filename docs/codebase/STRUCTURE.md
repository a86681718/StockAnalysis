# Codebase Structure

## Core Sections (Required)

### 1) Top-Level Map

| Path | Purpose | Evidence |
|------|---------|----------|
| `src/stockanalysis/` | Reusable configuration, analysis modules, and crawler runtime implementations | `src/stockanalysis/config.py`, `src/stockanalysis/analysis/`, `src/stockanalysis/runtime/crawlers/` |
| `apps/` | Runnable crawler wrappers, ETL jobs, analysis/report scripts, tools, services, and Dash UI | `apps/*`, `README.md` |
| `deployment/` | Active GCP source deployments for prepare and trigger services | `deployment/*/main.py`, `scripts/deploy_gcp_environment.sh` |
| `tests/` | `unittest` suites for analysis logic | `tests/test_*.py` |
| `docs/` | Operator runbooks and project documentation | `docs/project-structure.md`, `docs/gcp-bootstrap.md`, `docs/data-refresh-runbook.md` |
| `openspec/` | Specifications and change artifacts | `openspec/specs/`, `openspec/changes/` |
| `notebooks/` | Exploratory research notebooks; not stable runtime entry points | `notebooks/`, `README.md` |
| `legacy/` | Retained, non-primary service layouts | `legacy/README.md`, `legacy/apps/` |
| `data/` | Repo-local compatibility data root; may be redirected with environment variables | `src/stockanalysis/config.py`, `docs/project-structure.md` |
| `outputs/` | Generated analysis, report, and temporary outputs; may be redirected | `src/stockanalysis/config.py`, `.gitignore` |
| `assets/` and `conf/` | Captcha/model/static assets and legacy properties | `assets/models/`, `conf/default.properties` |
| `scripts/` | Operational shell helpers and GCP bootstrap/deployment | `scripts/deploy_gcp_environment.sh` |

### 2) Entry Points

- There is no single application entry point; the repository is a collection of jobs and applications.
- Cloud service entries: `deployment/prepare-{twse,tpex}-list/main.py` and `deployment/trigger-{twse,tpex}-job/main.py`, selected by `GOOGLE_FUNCTION_TARGET` in each `project.toml`.
- Crawler job wrappers: `apps/twse/crawler-twse-bsreport-new.py`, `apps/tpex/crawler-tpex-bsreport.py`, and daily-OHLC wrappers. These delegate to modules under `src/stockanalysis/runtime/crawlers/`.
- Current bs-report ETL entry: `apps/etl/run_bs_report_etl.py`.
- Analysis entries: many directly runnable scripts in `apps/analysis/`; reusable detection logic is under `src/stockanalysis/analysis/`.
- Interactive UI: `apps/visualization/app.py`.
- Entry selection is by shell command, Docker `ENTRYPOINT`, Cloud Build/Docker config, or Cloud Run buildpack target.

### 3) Module Boundaries

| Boundary | What belongs here | What must not be here |
|----------|-------------------|------------------------|
| `src/stockanalysis/` | Reusable logic and shared path/config helpers | Deployment-only service packaging and generated outputs |
| `apps/twse/`, `apps/tpex/` | Thin runnable wrappers and image/build files | New duplicated crawler implementations when a runtime module exists |
| `apps/etl/` | Local data synchronization and transformation jobs | Cloud service provisioning |
| `apps/analysis/` | Experiment orchestration, searches, backtests, and report builders | Claims of stable library API without tests/evidence |
| `deployment/` | Deployable prepare/trigger service source | Research notebooks or local analysis output |
| `legacy/` | Historical reference implementations | New active production entry points |
| `data/`, `outputs/` | Input/intermediate and generated artifacts | Reusable Python source |

These boundaries describe current documented and observed placement; “must not” entries are operational navigation rules, not proof of automated enforcement.

### 4) Naming and Organization Rules

- Package modules mostly use `snake_case.py`; older runnable scripts use mixed `PascalCase`, version suffixes, and descriptive snake case (`Analysis_BsReport_v4.py`, `run_strategy_search.py`).
- Market-specific code is feature-grouped under `apps/twse/`, `apps/tpex/`, and `src/stockanalysis/runtime/crawlers/`; analysis is a mixture of reusable modules and script workspace.
- Scripts commonly prepend `src/` to `sys.path` or rely on `PYTHONPATH=src`; package code imports via `stockanalysis.*`.
- Generated or historical areas (`outputs/`, `data/`, notebook checkpoints, `legacy/`) are not sources for coding conventions.

### 5) Evidence

- `README.md`
- `docs/project-structure.md`
- `src/stockanalysis/config.py`
- `apps/tpex/crawler-tpex-bsreport.py`
- `apps/etl/run_bs_report_etl.py`
- `deployment/trigger-tpex-job/project.toml`
- `legacy/README.md`
