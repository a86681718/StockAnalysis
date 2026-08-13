# Technology Stack

## Core Sections (Required)

### 1) Runtime Summary

| Area | Value | Evidence |
|------|-------|----------|
| Primary language | Python | `pyproject.toml`, `src/stockanalysis/` |
| Runtime + version | CPython `>=3.11,<3.12`; deployment buildpacks and crawler images use Python 3.11 | `pyproject.toml`, `deployment/*/project.toml`, `apps/*/Dockerfile*` |
| Package manager | `uv` workspace with a Poetry-compatible build backend; `pip` installs per-image requirements | `pyproject.toml`, `uv.lock`, `apps/*/Dockerfile*` |
| Module/build system | Installable `stockanalysis` package under `src/`; Poetry Core build backend; Cloud Build builds Docker images | `pyproject.toml`, `apps/twse/cloudbuild.yaml`, `apps/tpex/cloudbuild.yaml` |

### 2) Production Frameworks and Dependencies

Versions below are the declared root constraints. Deployment services and crawler images also maintain separate `requirements.txt` files, several without pins.

| Dependency | Version | Role in system | Evidence |
|------------|---------|----------------|----------|
| pandas | `>=2.2.3,<3.0.0` | Tabular ETL and analysis | `pyproject.toml` |
| NumPy | `>=2.4.1` | Numerical operations | `pyproject.toml` |
| PyArrow | `>=23.0.0` | Parquet storage | `pyproject.toml` |
| Requests / Beautiful Soup | Requests `>=2.32.3,<3`; bs4 `>=0.0.2,<0.0.3` | HTTP crawling and HTML parsing | `pyproject.toml` |
| Google Cloud Storage | `>=3.1.0,<4.0.0` | Crawler object output and ETL sync source | `pyproject.toml`, `src/stockanalysis/runtime/crawlers/twse_bs_report.py` |
| Google Cloud Firestore, Tasks, Run | Per-service requirements; Run pinned to `0.10.17` in trigger services | Crawl status, task dispatch, Cloud Run Job invocation | `deployment/*/requirements.txt` |
| Dash / Plotly | Dash `>=4.0.0`, Plotly `>=6.5.2` | Interactive visualization | `pyproject.toml`, `apps/visualization/app.py` |
| TensorFlow / OpenCV | TensorFlow `<=2.15`, OpenCV contrib `>=4.11.0.86,<5` | TWSE captcha model inference and image handling | `pyproject.toml`, `src/stockanalysis/runtime/crawlers/twse_bs_report.py` |
| Matplotlib / tqdm | Matplotlib `>=3.10.8`, tqdm `>=4.67.1` | Reports and batch progress | `pyproject.toml` |

### 3) Development Toolchain

| Tool | Purpose | Evidence |
|------|---------|----------|
| `uv` | Dependency resolution, workspace install, command execution | `pyproject.toml`, `uv.lock`, `README.md` |
| `unittest` | Automated tests | `tests/test_*.py` |
| JupyterLab | Exploratory notebooks | `pyproject.toml`, `notebooks/` |
| Docker / Cloud Build | Reproducible crawler images | `apps/twse/Dockerfile`, `apps/tpex/Dockerfile`, `apps/*/cloudbuild.yaml` |
| `gcloud` CLI | Bootstrap, deploy, GCS sync | `scripts/deploy_gcp_environment.sh`, `apps/etl/run_bs_report_etl.py` |
| Formatter/linter | `[TODO]` No repository-level formatter or linter configuration was found | root file/config inventory |

### 4) Key Commands

```bash
uv sync
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p 'test_*.py'
uv run apps/etl/run_bs_report_etl.py --market all --sync
PROJECT_ID=<project> scripts/deploy_gcp_environment.sh deploy
```

There is no verified repository-wide lint command: `[TODO]`.

### 5) Environment and Config

- Central path configuration: `src/stockanalysis/config.py`; legacy property access remains in `src/stockanalysis/commonlib.py` and `conf/default.properties`.
- Root path variables: `STOCKANALYSIS_ROOT`, `STOCKANALYSIS_DATA_DIR`, `STOCKANALYSIS_OUTPUT_DIR`, `STOCKANALYSIS_ASSETS_DIR`, `STOCKANALYSIS_CONF_DIR`, `STOCKANALYSIS_CONF_SECTION`.
- Crawler/cloud variables include `STOCK_CRAWLER_BUCKET`, `LOCATION`, `JOB_NAME`, `QUEUE_NAME`, `FUNCTION_URL`, `CLOUD_RUN_JOB`, region variables, retry variables, and TPEX proxy/diagnostic variables.
- Container runtime needs Chrome/Chromium; TPEX images also start Xvfb. TWSE includes a checked-in captcha model asset.
- No `.env.example`, `.env.template`, or committed equivalent was found: `[TODO]` document a canonical environment contract.

### 6) Evidence

- `pyproject.toml`
- `uv.lock`
- `apps/etl/pyproject.toml`
- `src/stockanalysis/config.py`
- `apps/twse/Dockerfile`
- `apps/tpex/Dockerfile`
- `deployment/trigger-twse-job/requirements.txt`
- `scripts/deploy_gcp_environment.sh`
