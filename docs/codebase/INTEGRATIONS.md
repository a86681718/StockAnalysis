# External Integrations

## Core Sections (Required)

### 1) Integration Inventory

| System | Type (API/DB/Queue/etc) | Purpose | Auth model | Criticality | Evidence |
|--------|---------------------------|---------|------------|-------------|----------|
| TWSE endpoints | Public HTTP sites/APIs | Listings, OHLC, broker buy/sell data, realtime data | Public HTTP; broker report uses captcha solving | High | `src/stockanalysis/runtime/crawlers/twse_*.py`, `apps/crawlers/` |
| TPEX endpoints | Public HTTP sites/APIs | Listings, OHLC, broker buy/sell data | Public HTTP; broker report uses browser/Turnstile flow and optional proxy | High | `src/stockanalysis/runtime/crawlers/tpex_*.py` |
| MOPS | Public HTTP site | Dividend, conference, and report links/data | Public HTTP | Medium | `apps/crawlers/Crawler_DividendInfo.py`, `apps/crawlers/Crawler_InstitutionalInvestorConference.py` |
| TAIFEX and ISIN TWSE | Public HTTP sites | Futures and security master data | Public HTTP | Medium | `apps/crawlers/Crawler_FutureList.py`, `apps/crawlers/Crawler_StockList.py` |
| Google Cloud Storage | Object store | Raw crawler output and local ETL synchronization | Application Default Credentials / service account | High | `src/stockanalysis/runtime/crawlers/*_bs_report.py`, `apps/etl/run_bs_report_etl.py` |
| Firestore | Document database | Per-date/per-symbol crawl status | Application Default Credentials / service account | High | `deployment/*/main.py`, runtime crawler modules |
| Cloud Tasks | Queue | Batch dispatch from prepare services to trigger services | Service account OIDC token | High | `deployment/prepare-*/main.py` |
| Cloud Run Jobs | Job runtime/API | Execute market crawler containers | Application Default Credentials and IAM | High | `deployment/trigger-*/main.py`, `scripts/deploy_gcp_environment.sh` |
| Cloud Scheduler | Scheduler | Weekday prepare-service invocation | Service account OIDC token | High | `scripts/deploy_gcp_environment.sh` |
| Artifact Registry / Cloud Build | Build and registry | Build and store crawler images | `gcloud` identity / Cloud Build service account | High | `apps/*/cloudbuild.yaml`, `scripts/deploy_gcp_environment.sh` |
| Google News RSS / Yahoo Finance links | Public HTTP/link integration | Enrich generated case-review reports | Public HTTP | Low | `apps/analysis/build_trigger_days_gt5_case_review.py` |

### 2) Data Stores

| Store | Role | Access layer | Key risk | Evidence |
|-------|------|--------------|----------|----------|
| GCS bucket | Durable raw crawler output and remote ETL source | Google Storage client and `gcloud storage` CLI | Bucket URI/config can diverge; one ETL default embeds a project-specific bucket | `src/stockanalysis/runtime/crawlers/twse_bs_report.py`, `apps/etl/run_bs_report_etl.py` |
| Firestore | Distributed crawl work/status coordination | Deployment and runtime crawler modules | Collection names and status strings form an implicit contract | `deployment/prepare-twse-list/main.py`, `deployment/trigger-twse-job/main.py` |
| Local CSV/Parquet/JSON | ETL inputs, manifests, features, analysis/report contracts | pandas/PyArrow and direct file I/O | No schema registry or migration mechanism | `apps/etl/bs_report_pipeline.py`, `src/stockanalysis/analysis/`, `apps/visualization/app.py` |
| Model/static assets | TWSE captcha inference and report assets | Files under `assets/` resolved by config | Runtime image must copy expected files | `apps/twse/Dockerfile`, `src/stockanalysis/runtime/crawlers/twse_bs_report.py` |

### 3) Secrets and Credentials Handling

- Google integrations use Application Default Credentials attached to deployed service accounts; Cloud Tasks and Scheduler use an explicitly configured invoker service account for OIDC.
- Runtime values are passed by environment variables and deployment flags. TPEX accepts proxy URLs via `TPEX_PROXIES` or `PROXY`; those URLs may include credentials.
- No `.env` template or Secret Manager wiring is present in the active bootstrap script. `[ASK USER]` Confirm whether proxy credentials and any other secrets are injected outside this repository through Secret Manager, CI, or manual deployment configuration.
- Hardcoding check: no committed API token was identified in inspected source; a project-specific default GCS URI and historical/current project IDs are present in operational code/docs.
- Rotation/lifecycle: `[TODO]` not defined in repository evidence.

### 4) Reliability and Failure Behavior

- Prepare services and market crawlers implement bounded retries; behavior and timeouts vary by endpoint.
- HTTP timeouts range from 2 seconds for metadata lookup to 600 seconds for TWSE list retrieval; some older crawler calls have no explicit timeout.
- Cloud Tasks have a 1,800-second dispatch deadline; queue retry settings and Cloud Run Job retries are configured in the deployment script.
- No circuit-breaker implementation was found. Fallbacks include Firestore-to-live symbol lookup, Chromium/browser alternatives, and retrying transient responses.
- Some calls deliberately set `verify=False`, reducing TLS verification guarantees.

### 5) Observability for Integrations

- Python logging surrounds most current deployment and runtime crawler calls; Cloud Tasks enables full queue log sampling in the bootstrap script.
- No repository evidence of distributed tracing, custom metrics, dashboards, alerts, or defined SLOs was found: `[TODO]`.
- Several errors are converted to log messages or empty results, which can hide the distinction between “no market data” and integration failure in older scripts.

### 6) Evidence

- `scripts/deploy_gcp_environment.sh`
- `deployment/prepare-tpex-list/main.py`
- `deployment/trigger-tpex-job/main.py`
- `src/stockanalysis/runtime/crawlers/tpex_bs_report_new.py`
- `src/stockanalysis/runtime/crawlers/twse_bs_report.py`
- `apps/etl/run_bs_report_etl.py`
- `apps/analysis/build_trigger_days_gt5_case_review.py`
