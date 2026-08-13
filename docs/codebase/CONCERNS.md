# Codebase Concerns

## Core Sections (Required)

### 1) Top Risks (Prioritized)

| Severity | Concern | Evidence | Impact | Suggested action |
|----------|---------|----------|--------|------------------|
| Medium | Runtime dependencies are fragmented across the root lock, ETL workspace, crawler images, and four service requirement files; most service/image requirements are unpinned | `pyproject.toml`, `uv.lock`, `apps/*/requirements.txt`, `deployment/*/requirements.txt` | Local, image, and service environments can resolve different versions | Define and test an environment matrix; `uv lock --check` passed for the root workspace on 2026-08-14 |
| High | Several active HTTP calls disable TLS certificate verification | `deployment/prepare-twse-list/main.py`, `deployment/prepare-tpex-list/main.py`, `src/stockanalysis/runtime/crawlers/tpex_daily_ohlc.py` | Susceptibility to interception and inability to authenticate upstream responses | Document the certificate failure and replace with trusted CA handling where possible |
| High | External/browser/cloud paths lack automated integration or end-to-end tests | `tests/` inventory, `deployment/`, runtime crawlers | Production failures can pass the 27 analysis-focused unit tests | Add isolated handler/ETL tests, then opt-in live smoke tests |
| Medium | File paths, columns, Firestore status strings, and container arguments are implicit cross-component contracts | `apps/etl/bs_report_pipeline.py`, `deployment/*/main.py`, `apps/visualization/app.py` | Drift is detected late at runtime | Record schemas/contracts and add contract tests before changing producers |
| Medium | Very large scripts combine data loading, logic, rendering, and orchestration | `apps/visualization/app.py` (1,798 lines), `apps/analysis/build_trigger_days_gt5_case_review.py` (1,599 lines) | Reviews and localized testing are difficult | First add characterization tests; defer refactoring to a separately approved change |
| Medium | No repository-level lint, format, coverage, or CI policy is visible | `pyproject.toml`, root config inventory | Style and regression checks depend on local practice | Clarify external CI, then add only the agreed checks |

### 2) Technical Debt

| Debt item | Why it exists | Where | Risk if ignored | Suggested fix |
|-----------|---------------|-------|-----------------|---------------|
| Mixed modern and legacy script styles | Research evolution and compatibility entry points are retained | `apps/analysis/`, `apps/crawlers/`, `legacy/` | Duplicate paths and unclear “current” entry points | Maintain an explicit entry-point registry; retire only with usage evidence |
| Duplicated TWSE/TPEX deployment services | Market-specific deployable directories | `deployment/prepare-*`, `deployment/trigger-*` | Behavior and fixes diverge | Compare contracts and test first; do not consolidate without deployment evidence |
| Multiple dependency manifests | Root workspace, ETL subproject, service and image requirements | `pyproject.toml`, `apps/etl/pyproject.toml`, `deployment/*/requirements.txt`, `apps/*/requirements.txt` | Version drift across local and cloud runtimes | Document an environment matrix and add resolution checks |
| Project-specific GCS default | Operational convenience | `apps/etl/run_bs_report_etl.py` | Accidental reads from the wrong project/bucket | Require explicit configuration for non-local sync after confirming operator needs |
| Repo-local data/output compatibility | Historical workflow | `src/stockanalysis/config.py`, `docs/project-structure.md` | Large artifacts and state can obscure reproducibility | Keep env-overridable paths and document canonical external storage |

### 3) Security Concerns

| Risk | OWASP category (if applicable) | Evidence | Current mitigation | Gap |
|------|--------------------------------|----------|--------------------|-----|
| TLS verification disabled | A02 Cryptographic Failures | `verify=False` calls in current crawlers/services | Requests still use HTTPS; some warnings suppressed | Peer identity is not verified |
| Broad infrastructure IAM grants | A01 Broken Access Control | `scripts/deploy_gcp_environment.sh` grants project roles such as object admin and run developer | Dedicated invoker identity and OIDC are used | Least-privilege review and environment-specific policy are not documented |
| Proxy credentials supplied in URLs | A02 / A07 | `src/stockanalysis/runtime/crawlers/tpex_bs_report_new.py` | Logged proxy labels omit passwords in current helper | Secret source, rotation, and accidental exception/log exposure are not governed |
| Raw request/body and exception logging paths | A09 Security Logging and Monitoring Failures | `deployment/trigger-*/main.py` debug logging | Debug level is not the default | No documented redaction policy |
| Public ingress configured for services | A01 Broken Access Control | `scripts/deploy_gcp_environment.sh` uses `--ingress all` | Scheduler/Tasks send OIDC tokens; invoker IAM is provisioned | Whether unauthenticated invocation is disabled is not explicitly asserted in script |

### 4) Performance and Scaling Concerns

| Concern | Evidence | Current symptom | Scaling risk | Suggested improvement |
|---------|----------|-----------------|-------------|-----------------------|
| Large in-memory pandas workloads | 41,915 Python lines; multiple analysis files above 900 lines; pandas-heavy modules | Batch jobs materialize and transform DataFrames | Memory/time grows with market history and feature width | Measure per pipeline before optimizing; use partitioned reads where demonstrated |
| Dash module performs file discovery and substantial transformation | `apps/visualization/app.py` (1,798 lines) | UI depends on local artifact availability and caches module globals | Startup/callback latency and stale data across long-lived processes | Add timing/refresh tests before changing cache strategy |
| TPEX browser automation is sequential and retry-heavy | `src/stockanalysis/runtime/crawlers/tpex_local_runner.py`, TPEX Dockerfiles | External anti-bot/browser stability limits throughput | More symbols increase wall-clock time | Preserve sequential stability until a measured, user-approved experiment exists |
| ETL defaults to six workers and rewrites per-symbol Parquet outputs | `apps/etl/run_bs_report_etl.py`, `apps/etl/bs_report_pipeline.py` | Local resource usage depends on date-folder volume | I/O and memory contention on large refreshes | Benchmark representative refreshes and expose documented resource guidance |

### 5) Fragile/High-Churn Areas

| Area | Why fragile | Churn signal | Safe change strategy |
|------|-------------|-------------|----------------------|
| `apps/visualization/app.py` | Large UI/data-loading module with artifact contracts | 24 commits in the last 90 days (highest Python/config churn) | Characterize affected callback/data path and test with representative files |
| `src/stockanalysis/runtime/crawlers/tpex_local_runner.py` | Browser, Turnstile, proxy, retry, and file behavior meet in one runtime | 13 commits in the last 90 days | Validate token-only/small-symbol path before full crawl |
| `src/stockanalysis/runtime/crawlers/tpex_bs_report.py` | Cloud/browser integration and anti-bot behavior | 8 commits in the last 90 days | Trace exact deployed wrapper/image before editing |
| `apps/analysis/build_trigger_days_gt5_case_review.py` | 1,599-line report builder consuming many upstream artifacts | 8 commits in the last 90 days | Refresh/validate upstream inputs before judging generated HTML |
| Deployment/bootstrap files | Resource names, IAM, queues, services, jobs, and schedules are coupled | `scripts/deploy_gcp_environment.sh` changed 5 times; deployment services also changed | Use dry/read-only config checks and deploy one bounded component at a time |

### 6) `[ASK USER]` Questions

1. [ASK USER] Which entry points are considered production-supported today beyond the four `deployment/` services, two crawler images, bs-report ETL, and Dash app?
2. [ASK USER] Is CI configured outside this checkout, or are the 27 `unittest` tests intentionally local-only?
3. [ASK USER] Where are `TPEX_PROXIES`/`PROXY` and other operational secrets injected and rotated?
4. [ASK USER] Is the current live GCP project still the one named in `docs/gcp-bootstrap.md`, or should that document be treated strictly as historical?
5. [ASK USER] Which external directory or bucket layout is canonical for production data and outputs?

### 7) Evidence

- `pyproject.toml`
- Git history: top changed Python/config files over the 90 days ending 2026-08-13
- `scripts/deploy_gcp_environment.sh`
- `src/stockanalysis/runtime/crawlers/tpex_local_runner.py`
- `deployment/prepare-twse-list/main.py`
- `apps/etl/run_bs_report_etl.py`
- `apps/visualization/app.py`
- `tests/`
