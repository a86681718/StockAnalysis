# Supported Entrypoints Baseline

Status: Phase 1 characterization baseline
Observed: 2026-08-16
Repository revision: `32757f2e7d3e16ffc51975481aae20c424bbec22`

This registry records the entrypoints that are supported by repository or
deployment evidence before the folder refactor. It documents current behavior;
it does not make every observed behavior a permanent requirement.

## Classification

- `production`: active deployment or documented primary workflow.
- `diagnostic`: manually invoked troubleshooting path.
- `research`: exploratory or batch analysis without a production support claim.
- `unknown`: plausible manual entrypoint whose current operator use is unverified.

## Production and operational entrypoints

| Entrypoint | Classification | Command or runtime | Implementation owner | Inputs | Outputs | Evidence |
|---|---|---|---|---|---|---|
| TWSE broker crawler | production | Cloud Run Job `twse-crawler`; `python apps/twse/crawler-twse-bsreport-new.py ...` | `src/stockanalysis/runtime/crawlers/twse_bs_report.py` | symbol list, date, captcha model, Firestore/GCS configuration | broker CSV in GCS; Firestore crawl status | `apps/twse/Dockerfile`, active Job generation 7 |
| TPEX broker crawler | production | Cloud Run Job `tpex-crawler`; `python apps/tpex/crawler-tpex-bsreport.py ...` | `src/stockanalysis/runtime/crawlers/tpex_local_runner.py` | symbol list, date, browser/proxy configuration | broker CSV in GCS; Firestore crawl status | `apps/tpex/Dockerfile`, active Job generation 13 |
| TPEX local runner | production local operation | `docker build -f apps/tpex/Dockerfile.local-runner ...` | `src/stockanalysis/runtime/crawlers/tpex_local_runner.py` | symbols, `--date`, local proxy/browser configuration | local broker CSV | local-runner Dockerfile and entrypoint |
| TWSE daily OHLC | production command | `python apps/twse/crawler-twse-daily-ohlc.py` | `src/stockanalysis/runtime/crawlers/twse_daily_ohlc.py` | trading date, TWSE endpoint | `data/ohlc/twse-YYYYMMDD.csv` | wrapper delegates to package module |
| TPEX daily OHLC | production command | `python apps/tpex/crawler-tpex-daily-ohlc.py` | `src/stockanalysis/runtime/crawlers/tpex_daily_ohlc.py` | trading date, TPEX endpoint | `data/ohlc/tpex-YYYYMMDD.csv` | wrapper delegates to package module |
| TWSE prepare service | production | Cloud Run source service `deployment/prepare-twse-list` | `deployment/prepare-twse-list/main.py` | optional date and batch size | Firestore work state and Cloud Tasks | deployment source and bootstrap script |
| TPEX prepare service | production | Cloud Run source service `deployment/prepare-tpex-list` | `deployment/prepare-tpex-list/main.py` | optional date and batch size | Firestore work state and Cloud Tasks | deployment source and bootstrap script |
| TWSE trigger service | production | Cloud Run source service `deployment/trigger-twse-job` | `deployment/trigger-twse-job/main.py` | JSON `symbols` and `date` | Firestore `running`; Cloud Run Job operation | deployment source and bootstrap script |
| TPEX trigger service | production | Cloud Run source service `deployment/trigger-tpex-job` | `deployment/trigger-tpex-job/main.py` | JSON `symbols` and `date` | Firestore `running`; synchronous Job operation result | deployment source and bootstrap script |
| Broker-report ETL | production local operation | `uv run apps/etl/run_bs_report_etl.py --market ...` | `apps/etl/bs_report_pipeline.py` | GCS/local dated CSV folders and manifest | per-symbol Parquet, manifest, archive | `apps/etl/README.md` |
| OHLC ETL | production local operation | `uv run apps/etl/build_ohlc_parquet.py` | same file | TWSE/TPEX daily OHLC CSV | `data/_derived/ohlc.parquet` | current data layout and Dash consumer |
| Broker accumulation refresh | production local operation | `uv run apps/analysis/refresh_broker_branch_accumulation.py` | broker Parquet and OHLC artifacts | analysis prerequisites and case-review portal | active report workflow documentation |
| Case-review portal | production report | called by the refresh command or directly | `apps/analysis/build_trigger_days_gt5_case_review.py` | compatible analysis prerequisites | `outputs/trigger_days_gt5_case_review/index.html` | primary portal workflow |
| Dash visualization | production local UI | `uv run apps/visualization/app.py` | OHLC, broker and scored Parquet artifacts | local Dash web UI | README data consumer map |

## Non-production and unresolved entrypoints

| Entrypoint group | Classification | Current evidence | Refactor treatment |
|---|---|---|---|
| `apps/tpex/debug_*`, debug image | diagnostic | coherent browser/Turnstile troubleshooting path | retain until operator confirms recovery need |
| `src/stockanalysis/runtime/crawlers/test.py` | diagnostic | manually runnable token/proxy benchmark; not an automated test | rename only after Phase 1 |
| `apps/analysis/Analysis_BsReport_v4.py` | research baseline | identified as current research baseline by `apps/analysis/README.md` | do not treat as production package logic |
| other `apps/analysis/run_*`, `build_*`, versioned analyses | research or unknown | direct commands and script-to-script imports exist; no central scheduler | classify individually before moving/deleting |
| older `apps/crawlers/` commands | unknown | plausible operator commands; current use is not centrally recorded | preserve until an operator/reference audit |
| `apps/services/` and `legacy/apps/` | removed in Phase 2 | active GCP source archives and deployment commands use `deployment/`; removed sources remain recoverable from Git | do not restore duplicate runtime implementations |

## Deployment provenance

The following values were read with `gcloud` on 2026-08-16. They are a
time-specific baseline, not credentials and not a deployment request.

| Resource | Project / region | Generation | Image | Digest |
|---|---|---:|---|---|
| `twse-crawler` | `project-3b72568d-c2fc-4e6c-89b` / `asia-east1` | 7 | `asia-east1-docker.pkg.dev/project-3b72568d-c2fc-4e6c-89b/my-repo/twse-crawler:20260601-1` | `sha256:56784b85875d4af51385ed5652356843ae20ab0f9def0b6b81daee8003dfffd8` |
| `tpex-crawler` | `project-3b72568d-c2fc-4e6c-89b` / `asia-east1` | 13 | `asia-east1-docker.pkg.dev/project-3b72568d-c2fc-4e6c-89b/my-repo/tpex-crawler:20260719-1954cfd` | `sha256:f846eae9f9e4570d76931d4e8bf96bfb3cb45b0d9b7c8de43faa34dac6d30423` |

The active gcloud account was verified locally but is intentionally omitted
from this tracked registry. Re-run the auth/project/region preflight before any
deployment.

## Baseline findings and unknowns

- The existing analysis suite passes 27 tests at the recorded revision.
- `uv lock --check` succeeds.
- The local `.venv` cannot import the TWSE broker crawler because TensorFlow is
  absent. `apps/twse/requirements.txt` declares TensorFlow for the image, so the
  local check is recorded as an environment gap rather than a crawler defect.
- Current ETL behavior marks a dated folder successful when at least one CSV
  fails but another CSV is processed. The Phase 1 test deliberately records
  this behavior; Phase 3 owns the correction.
- The repository does not currently prove which older crawler and research
  commands are still invoked manually outside the repository.
