# Architecture

## Core Sections (Required)

### 1) Architectural Style

- Primary style: batch/data-pipeline workspace with script entry points, reusable Python modules, and event-triggered GCP crawl orchestration.
- Why this classification: crawlers create market data, ETL turns daily folders into Parquet, analysis modules/scripts consume those datasets, and reports/Dash read generated artifacts. In cloud operation, Scheduler calls prepare services, Cloud Tasks calls trigger services, and trigger services start Cloud Run Jobs.
- Primary constraints: external market-site availability and anti-bot behavior; file-oriented data contracts; GCP identity/region/resource configuration; mixed production and exploratory workloads in one repository.

This classification describes observed mechanics, not an inferred target architecture.

### 2) System Flow

Primary cloud crawl flow:

```text
Cloud Scheduler -> prepare service -> Firestore + Cloud Tasks -> trigger service -> Cloud Run crawler Job -> GCS
```

1. `scripts/deploy_gcp_environment.sh` provisions Scheduler jobs that POST to a prepare service.
2. `deployment/prepare-*/main.py` loads or fetches the day’s symbols, records status in Firestore, and creates OIDC-authenticated Cloud Tasks in batches.
3. `deployment/trigger-*/main.py` validates the task payload, marks symbols running, and invokes a Cloud Run Job with container arguments.
4. Docker entry points run market wrappers, which delegate to `stockanalysis.runtime.crawlers` implementations.
5. Crawlers call TWSE/TPEX endpoints, write crawl output to GCS, and update Firestore status.

Downstream local data flow:

```text
GCS/raw folders -> apps/etl/run_bs_report_etl.py -> Parquet/data -> analysis modules/scripts -> outputs -> Dash/static reports
```

### 3) Layer/Module Responsibilities

| Layer or module | Owns | Must not own | Evidence |
|-----------------|------|--------------|----------|
| Deployment services | HTTP/task boundary, batching, status transitions, Cloud Run invocation | Market parsing and reusable analysis algorithms | `deployment/*/main.py` |
| Runtime crawlers | Browser/HTTP interaction, market parsing, upload/status completion | Dashboard rendering | `src/stockanalysis/runtime/crawlers/` |
| ETL | GCS folder sync, manifest skip logic, CSV-to-Parquet processing, archival | Cloud resource provisioning | `apps/etl/run_bs_report_etl.py`, `apps/etl/bs_report_pipeline.py` |
| Analysis package | Configurable feature construction and event detection | HTTP service deployment | `src/stockanalysis/analysis/` |
| Analysis/report apps | Batch orchestration, experiments, static report generation | Stable package API guarantees | `apps/analysis/` |
| Visualization | Reads data/output artifacts and serves Dash UI | Crawler scheduling | `apps/visualization/app.py` |
| Shared configuration | Resolve root/data/output/assets/conf paths | Domain calculations | `src/stockanalysis/config.py` |

### 4) Reused Patterns

| Pattern | Where found | Why it exists |
|---------|-------------|---------------|
| Thin wrapper/delegation | `apps/twse/crawler-twse-bsreport-new.py`, `apps/tpex/crawler-tpex-bsreport.py` | Keeps stable runnable/container paths while implementation lives under `src/` |
| Dataclass configuration | `src/stockanalysis/analysis/*` | Makes analysis thresholds and paths explicit and testable |
| Manifest-based incremental processing | `apps/etl/run_bs_report_etl.py` | Avoids re-copying and reprocessing successful date folders |
| Environment-overridable path resolver | `src/stockanalysis/config.py` | Supports repo-local compatibility and external operational datasets |
| Retry loop | prepare services and runtime crawlers | Handles transient HTTP, browser, and market endpoint failures |
| File-mediated integration | analysis modules, reports, Dash app | Parquet/CSV/JSON/HTML artifacts decouple separately invoked jobs |

### 5) Known Architectural Risks

- No single orchestrator or typed cross-stage schema enforces crawler-to-ETL-to-analysis file contracts; column/path drift can surface only at runtime.
- Production services and exploratory scripts coexist, while several very large scripts contain orchestration, transformation, and rendering together.
- Deployment code is duplicated between TWSE and TPEX prepare/trigger services, so fixes can diverge.
- Some cloud clients and metadata requests initialize at import time, coupling module import to GCP runtime availability.

### 6) Evidence

- `scripts/deploy_gcp_environment.sh`
- `deployment/prepare-twse-list/main.py`
- `deployment/trigger-twse-job/main.py`
- `apps/twse/Dockerfile`
- `apps/tpex/entrypoint.sh`
- `apps/etl/run_bs_report_etl.py`
- `src/stockanalysis/analysis/broker_branch_accumulation.py`
- `apps/visualization/app.py`
