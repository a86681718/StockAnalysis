# StockAnalysis Folder Structure Migration Blueprint

Status: proposal only
Observed repository date: 2026-08-14
Production code changes: none

This blueprint translates the verified runtime map and recommended target
architecture into a folder migration plan. It does not treat the current layout
as intentional, and it does not authorize moves or deletions.

## Classification legend

| Label | Meaning in this document |
|---|---|
| **CANONICAL** | Confirmed or best-supported home for an active responsibility today. |
| **DUPLICATED** | Another implementation or folder owns substantially the same responsibility. |
| **LEGACY** | Explicitly historical or superseded, whether or not still retained for reference. |
| **GENERATED** | Runtime/build/tool output; not source architecture. |
| **EXPERIMENTAL** | Research, diagnostic, prototype, or exploratory material without a production support claim. |
| **ORPHANED** | No meaningful current caller, deployment path, or operator reference was found. |
| **MISPLACED** | Useful content exists, but the current path communicates the wrong ownership or lifecycle. |
| **UNCERTAIN** | Evidence is insufficient because the item may be manually invoked or externally managed. |

Labels describe evidence, not code quality. A path may have more than one label.

# CURRENT STRUCTURE

## Current top-level map

```text
StockAnalysis/
├── src/stockanalysis/       reusable package, but mixed lifecycle inside crawlers
├── apps/                    entrypoints + implementations + research + old services
├── deployment/              active Cloud Run source services
├── scripts/                 deployment and local operational shell commands
├── tests/                   analysis-focused unit tests
├── assets/                  runtime model and static assets
├── conf/                    legacy property configuration
├── data/                    local input/derived data root
├── outputs/                 generated analysis/report output root
├── docs/                    architecture, runbooks, research notes, generated HTML
├── notebooks/               exploratory notebooks plus embedded datasets/checkpoints
├── goal/                    research decisions, audits, and strategy registry
├── openspec/                change/spec workflow metadata
├── legacy/                  old cloud service source trees
├── .agents/                 project-local agent skills
├── .gcloud/                 ignored local gcloud state
├── .venv/                   local Python environment
├── .vscode/                 editor configuration
└── root files               package, lock, env, diagrams, agent/tool metadata
```

This is a Python workspace rather than a true multi-project monorepo. It has
several independently deployable GCP components, but the repository is not a
microservice system: ETL, analysis, reporting, and Dash remain local processes
joined by files.

## Important top-level directories

| Current path | Actual responsibility | Classification | Structural finding |
|---|---|---|---|
| `src/stockanalysis/` | Importable Python package for paths/config, analysis logic, and active crawler implementations | **CANONICAL**, partly **DUPLICATED** | Correct home for reusable logic. Internal `analysis` and `runtime/crawlers` names do not fully represent target ownership, and alternate TPEX implementations coexist with the active one. |
| `apps/` | Runnable scripts, Docker build contexts, ETL implementation, analysis research, report builders, Dash app, old crawlers/services, utilities | **CANONICAL**, **MISPLACED**, **DUPLICATED**, **EXPERIMENTAL**, partly **ORPHANED** | The largest structural problem. `apps` means both composition root and implementation library; support status cannot be inferred from location. |
| `deployment/` | Four active Cloud Run prepare/trigger service source directories | **CANONICAL**, internally **DUPLICATED** | Correct deployable boundary today. Repeated business/orchestration behavior should eventually delegate into the package, while packaging stays here. |
| `scripts/` | GCP bootstrap/deploy and local browser/process/crawler shell helpers | **CANONICAL**, partly **UNCERTAIN**, partly **DUPLICATED** | Deployment script is active evidence. Several local launchers have no verified current use; `twse.sh` points to the older TWSE crawler. |
| `tests/` | Eight `unittest` modules covering analysis and research calculations | **CANONICAL** | Correct top-level test home, but structure mirrors neither package boundaries nor future integration/contract scopes. |
| `assets/` | TWSE captcha model, alphabet images, and shared static assets | **CANONICAL** | Runtime assets belong outside Python source, but ownership should be grouped by consumer to avoid an undifferentiated asset bucket. |
| `conf/` | `default.properties` used through legacy `commonlib.py` | **LEGACY**, **MISPLACED** | A second configuration system beside env/CLI and `config.py`. It should disappear after callers migrate, not be expanded. |
| `data/` | Repo-local compatibility root for source, reference, and derived datasets | **CANONICAL** as runtime convention; contents mostly **GENERATED** | Useful stable mount/path, but source/reference/derived/cache lifecycles are not obvious from one flat conceptual root. |
| `outputs/` | Ignored generated analysis, report, portal, and temporary artifacts | **GENERATED**, **CANONICAL** as output convention | Correctly ignored. Producer, schema, and freshness are implicit in subfolder/file naming. |
| `docs/` | Runbooks, GCP notes, codebase maps, architecture blueprints, research log, and checked-in HTML explorers | **CANONICAL**, partly **GENERATED**, partly **MISPLACED** | Durable documentation belongs here. Generated interactive HTML is mixed with authored Markdown and should have a named generated/report lifecycle. |
| `notebooks/` | Exploratory notebooks, checkpoint copies, and small CSV/XLS fixtures/results | **EXPERIMENTAL**, partly **GENERATED**, partly **MISPLACED** | Valid research area, but Jupyter checkpoints are generated and data files lack fixture/input/result classification. |
| `goal/` | Strategy evolution logs, audits, direction summaries, and a registry | **CANONICAL** content, **MISPLACED** name | These are durable research governance documents, not runtime “goals.” The folder name is vague to humans and agents. |
| `openspec/` | Spec/change workflow configuration and archived changes | **CANONICAL** tool metadata | Keep as a tool-owned top-level boundary while OpenSpec is used. Do not mix runtime design docs into it unless they are actual change artifacts. |
| `legacy/` | Explicitly non-primary copies of TWSE/TPEX prepare/trigger services | **LEGACY**, **DUPLICATED**, **ORPHANED** | No active build/deploy/caller evidence. Candidate for deletion after provenance and recovery requirements are confirmed. |
| `.agents/` | Project-local Codex skills | **CANONICAL** tool metadata | Not application architecture. Keep tool-managed and separate from product source. |
| `.gcloud/` | Ignored local gcloud CLI configuration/log state | **GENERATED**, local-only | Must remain ignored; it is not deployable configuration and must never become a source of truth. |
| `.venv/` | Local Python environment | **GENERATED** | Correctly external to source architecture and should remain ignored. |
| `.vscode/` | Shared editor settings/tasks | **CANONICAL** developer tooling, **UNCERTAIN** support scope | Keep only settings intentionally shared by the team. |

## `src/stockanalysis/`

| Current path | Actual responsibility | Classification | Structural finding |
|---|---|---|---|
| `src/stockanalysis/config.py` | Resolve root, data, output, asset, and config paths | **CANONICAL** | Correct shared package ownership; target name `settings.py` should be considered only when runtime settings are actually consolidated. |
| `src/stockanalysis/commonlib.py` | Legacy property access plus unrelated numeric/text helpers | **LEGACY**, **MISPLACED** | A general helper module combines configuration and utilities. Migrate callers by responsibility, then delete rather than rename wholesale. |
| `src/stockanalysis/analysis/` | Reusable DataFrame-heavy detectors, accumulation logic, scans, and backtests | **CANONICAL**, partly **EXPERIMENTAL** | Stable production-consumed signals and research algorithms are mixed. Target should distinguish `signals` from research without introducing subpackages for every algorithm. |
| `src/stockanalysis/runtime/` | Container/runtime-oriented code; currently only crawlers | **MISPLACED** name | “runtime” does not identify business ownership. Active modules are market adapters and should live under `crawling`. |
| `src/stockanalysis/runtime/crawlers/twse_bs_report.py` | Active TWSE cloud broker-report crawler | **CANONICAL** | Target production owner for TWSE broker crawling. |
| `src/stockanalysis/runtime/crawlers/tpex_local_runner.py` | Active sequential TPEX browser/Turnstile broker crawler | **CANONICAL** | Target production owner for TPEX broker crawling after deployed-source parity is established. |
| `src/stockanalysis/runtime/crawlers/{twse,tpex}_daily_ohlc.py` | Daily OHLC adapters and CLI-capable modules | **CANONICAL** | Market data adapters should move with crawling ownership; CLI behavior should stay in `apps`. |
| `src/stockanalysis/runtime/crawlers/tpex_bs_report.py` | Older/alternate TPEX browser implementation; helper imported by debug app | **DUPLICATED**, **UNCERTAIN** | Not in active image path, but diagnostic dependency prevents immediate orphan classification. |
| `src/stockanalysis/runtime/crawlers/tpex_bs_report_new.py` | Alternate TPEX implementation with proxy/browser variants | **DUPLICATED**, **EXPERIMENTAL**, **UNCERTAIN** | Imported only by `test.py`; should not remain beside the canonical crawler without an explicit diagnostic role. |
| `src/stockanalysis/runtime/crawlers/test.py` | Manually runnable TPEX diagnostic/benchmark | **EXPERIMENTAL**, **MISPLACED** | It is not an automated test and should not be named `test.py` inside production source. |
| `__pycache__/`, `.DS_Store` under source | Interpreter/OS output | **GENERATED**, **MISPLACED** | Never part of the source structure; remove safely and keep ignored. |

## `apps/`

| Current path | Actual responsibility | Classification | Structural finding |
|---|---|---|---|
| `apps/twse/` | TWSE Docker/Cloud Build context plus broker/OHLC launchers | **CANONICAL**, partly **DUPLICATED** | Correct container packaging boundary. Two broker scripts obscure which is current. |
| `apps/tpex/` | TPEX production, debug, and local-runner Docker contexts and launchers | **CANONICAL**, **EXPERIMENTAL**, partly **DUPLICATED** | Production and diagnostics share a folder without lifecycle separation. Retain only explicit image/entrypoint packaging after crawler consolidation. |
| `apps/etl/` | ETL CLIs and reusable ETL implementation, plus its own package manifest | **CANONICAL**, **MISPLACED**, partly **LEGACY** | Entrypoints belong here; `BsReportEtl` implementation belongs in the package. Older `ETL_*` scripts require caller classification. |
| `apps/analysis/` | Production-consumed refresh/report commands, research pipelines, numbered generations, script-to-script reusable logic, configs, reports | **CANONICAL**, **EXPERIMENTAL**, **MISPLACED**, **UNCERTAIN**, partly **DUPLICATED** | Support lifecycle and import boundary are invisible. This folder needs separation by command role, not more numbered versions. |
| `apps/visualization/` | Dash application and Dash-specific assets | **CANONICAL**, internally **MISPLACED** | App shell belongs here; reusable file loading/query/calculation logic currently trapped in `app.py` belongs in the package. |
| `apps/crawlers/` | Older standalone market/reference crawlers | **UNCERTAIN**, partly **LEGACY**, one confirmed **ORPHANED/DUPLICATED** | Some may still be operator-run. `Crawler_TPEXBuySellReport.py` duplicates the canonical TPEX path; other sources need per-command ownership evidence. |
| `apps/services/` | Earlier TPEX prepare/trigger service scripts | **DUPLICATED**, **ORPHANED**, **LEGACY** | Responsibility is actively owned by `deployment/`; no current deploy reference found. |
| `apps/tools/` | One text-to-image helper script | **MISPLACED**, **UNCERTAIN** | A top-level tools category for one script is vague. Move its reusable behavior to a concrete reporting/utility owner or delete if unused. |
| `apps/**/__pycache__`, `.ipynb_checkpoints`, `.DS_Store` | Runtime/editor/OS artifacts | **GENERATED**, **MISPLACED** | Exclude from the conceptual and tracked structure. |

### High-value file classifications inside `apps`

| Current file/group | Classification | Evidence |
|---|---|---|
| `apps/twse/crawler-twse-bsreport-new.py` | **CANONICAL** wrapper, poorly named | Docker copies it as the active container entry script. |
| `apps/twse/crawler-twse-bsreport.py` | **DUPLICATED**, **UNCERTAIN** | Older full implementation; still targeted by `scripts/twse.sh`. |
| `apps/tpex/crawler-tpex-bsreport.py` | **CANONICAL** wrapper | Active Docker/entrypoint chain delegates through it or directly to package code. |
| `apps/tpex/debug_*`, `Dockerfile.debug`, `entrypoint-debug.sh`, `cloudbuild-debug.yaml` | **EXPERIMENTAL**, **UNCERTAIN** | Coherent debug path exists, but current operational use was not verified. |
| `apps/tpex/Dockerfile.local-runner`, local runner entrypoint/build config | **CANONICAL** local operational packaging | Verified source/build chain; local image runtime was not verified in the earlier inspection. |
| `apps/etl/run_bs_report_etl.py`, `build_ohlc_parquet.py` | **CANONICAL** commands | Active runbook paths and current artifacts. |
| `apps/etl/bs_report_pipeline.py` | **CANONICAL** logic, **MISPLACED** | Reusable ETL implementation located in an entrypoint folder. |
| `apps/etl/ETL_*`, `main.py` | **LEGACY** or **UNCERTAIN** | Parallel naming/generations exist; active workflow uses `run_bs_report_etl.py`. |
| `apps/analysis/refresh_broker_branch_accumulation.py` | **CANONICAL** command | Active producer that calls the case-review builder. |
| `apps/analysis/build_trigger_days_gt5_case_review.py` | **CANONICAL** command plus **MISPLACED** reusable/rendering logic | Active portal producer, but 1,599 lines combine composition, data, news, and rendering. |
| `apps/analysis/Analysis_BsReport_v1.py` to `v4.py` | **EXPERIMENTAL**, **DUPLICATED**, **UNCERTAIN** | Iterative research lineage is documented; v4 is called the baseline, older versions still have references. |
| Most `apps/analysis/run_*`, other `build_*`, `make_*` | **EXPERIMENTAL**, **UNCERTAIN** | Plausible direct operator CLIs and artifacts exist, but no central support registry or runtime scheduler establishes use. |
| `apps/analysis/configs/`, `reports/` | **EXPERIMENTAL**, partly **GENERATED/UNCERTAIN** | Located under research scripts; exact authored-versus-generated lifecycle must be recorded before moving. |
| `apps/visualization/app.py` | **CANONICAL**, internally **MISPLACED** | Supported UI entrypoint, but owns data/query logic as well as Dash composition. |

## `deployment/`, `legacy/`, and operational scripts

| Current path | Actual responsibility | Classification | Structural finding |
|---|---|---|---|
| `deployment/prepare-{twse,tpex}-list/` | Market-specific Cloud Run prepare services | **CANONICAL**, **DUPLICATED** behavior | Keep deployable packaging; extract only shared tested behavior. |
| `deployment/trigger-{twse,tpex}-job/` | Market-specific Cloud Run Job trigger services | **CANONICAL**, **DUPLICATED** behavior | Keep deployable packaging; normalize async contract through shared workflow code. |
| Per-service `requirements.txt`, `project.toml`, `command` | Buildpack/runtime packaging | **CANONICAL** | These are legitimate deployment-bound files, though dependency drift should be documented. |
| `legacy/apps/{twse,tpex}/{prepare-service,trigger-service}` | Historical copies of current cloud responsibilities | **LEGACY**, **DUPLICATED**, **ORPHANED** | Delete after confirming Git history is sufficient for recovery. |
| `scripts/deploy_gcp_environment.sh` | GCP bootstrap/deployment source | **CANONICAL** | Keep under operations scripts unless infrastructure becomes declarative. |
| `scripts/twse.sh` | Starts older TWSE crawler | **DUPLICATED**, **UNCERTAIN** | Must either target the canonical command or be deleted after operator check. |
| `scripts/tpex.sh` | Starts active TPEX wrapper locally | **UNCERTAIN** | Plausible operator command, but no active process was verified. |
| Browser/display/process helper scripts | Local GUI/browser operations | **UNCERTAIN**, partly **EXPERIMENTAL** | Group as local development/diagnostic operations if retained. |
| `scripts/commands.sh` and root `commands` | Command notes/helpers | **DUPLICATED**, **UNCERTAIN** | Two vague command surfaces should not remain without documented roles. |

## Tests, documentation, research, and artifacts

| Current path | Actual responsibility | Classification | Structural finding |
|---|---|---|---|
| `tests/test_*analysis*.py` | Unit/characterization tests for package and research functions | **CANONICAL** | Retain at top level; target subfolders should mirror runtime contracts, not implementation trivia. |
| `docs/codebase/` | Repository onboarding facts | **CANONICAL** | Good evidence-oriented boundary. |
| `docs/runtime-workflow-map.md` | Runtime system/workflow truth map | **CANONICAL** | Keep as current runtime evidence. |
| `docs/architecture-blueprint.md` | Current/target architecture model | **CANONICAL** | Target source for this migration plan. |
| `docs/gcp-*.md`, `data-refresh-runbook.md` | Operations/runbooks | **CANONICAL**, freshness-sensitive | Target an `operations/` docs grouping; retain dates and verification scope. |
| `docs/research/` | Authored research log | **CANONICAL** | Merge with the semantically similar `goal/` documents. |
| `docs/*.html` | Generated explorers/reports | **GENERATED**, **MISPLACED** | Separate from authored documentation or keep only when intentionally published as durable examples. |
| `goal/` | Research governance and experiment history | **MISPLACED**, otherwise **CANONICAL** | Move under `docs/research/`; do not delete valuable experimental history. |
| `notebooks/*.ipynb` | Exploratory analyses/crawlers/ETL | **EXPERIMENTAL**, **UNCERTAIN** | Keep a single research notebook area; add lifecycle metadata only where useful. |
| `notebooks/.ipynb_checkpoints/` | Jupyter-generated backups | **GENERATED**, **MISPLACED** | Delete safely and ignore. |
| `notebooks/*.csv`, `*.xls` | Notebook data or result files | **MISPLACED**, **UNCERTAIN** | Move to named fixtures/samples only if required; otherwise regenerate or store under ignored data. |
| `data/` contents | Inputs, references, caches, and derived data | Mostly **GENERATED**, some canonical reference inputs | Preserve the root path but define `raw`, `reference`, `derived`, and `cache` ownership incrementally. |
| `outputs/` contents | Generated analysis/report products | **GENERATED** | Keep ignored; target manifests should describe producer and source horizon. |
| `assets/models/` | Captcha model artifacts | **CANONICAL** | Target consumer-specific `assets/crawling/twse/`. |
| `assets/img/` | Shared/legacy images and alphabet data | **CANONICAL** or **UNCERTAIN** per consumer | Move only after actual references are mapped. |

## Root files

| Current path | Classification | Target disposition |
|---|---|---|
| `pyproject.toml`, `uv.lock` | **CANONICAL** | Keep at root as the Python workspace authority. |
| `.env.example`, `.gitignore`, `.dockerignore`, `.gcloudignore` | **CANONICAL** | Keep at root; align contents with target paths during migration. |
| `README.md` | **CANONICAL** | Keep as the entrypoint registry and navigation page. |
| `AGENT.md`, untracked `AGENTS.md`, `.agents/`, `skills-lock.json` | Tool/agent governance; **CANONICAL** if intentionally maintained | Keep at root/tool-owned paths; reconcile singular/plural instruction files separately. |
| `architecture.key` | **UNCERTAIN**, likely authored presentation | Move under `docs/design/` if retained and document its source/export relationship. |
| `data-flow.drawio` | **CANONICAL** authored diagram but stale reference present | Move under `docs/diagrams/` after updating its old crawler path. |
| root `commands` | **UNCERTAIN**, **MISPLACED** | Merge into a runbook or delete after comparison with `scripts/commands.sh`. |
| root `.DS_Store` | **GENERATED**, **MISPLACED** | Delete and keep ignored. |

# TARGET STRUCTURE

## Proposed target tree

```text
StockAnalysis/
├── src/
│   └── stockanalysis/
│       ├── settings.py
│       ├── contracts/
│       │   ├── artifacts.py
│       │   ├── crawl_jobs.py
│       │   └── datasets.py
│       ├── crawling/
│       │   ├── twse.py
│       │   ├── tpex.py
│       │   └── reference_data.py
│       ├── pipelines/
│       │   ├── broker_reports.py
│       │   └── ohlc.py
│       ├── signals/
│       │   ├── broker_branch_accumulation.py
│       │   ├── general_broker_flow.py
│       │   └── persistent_accumulation.py
│       ├── reporting/
│       │   └── case_review.py
│       ├── query/
│       │   └── dashboard.py
│       └── workflows/
│           ├── cloud_dispatch.py
│           ├── refresh_data.py
│           └── publish_case_review.py
├── apps/
│   ├── crawling/
│   │   ├── twse/
│   │   └── tpex/
│   ├── data/
│   ├── analysis/
│   ├── reporting/
│   ├── visualization/
│   └── diagnostics/
├── deployment/
│   ├── prepare-twse-list/
│   ├── prepare-tpex-list/
│   ├── trigger-twse-job/
│   └── trigger-tpex-job/
├── research/
│   ├── experiments/
│   └── notebooks/
├── tests/
│   ├── unit/
│   ├── contracts/
│   ├── integration/
│   └── smoke/
├── assets/
│   ├── crawling/
│   │   └── twse/
│   └── reporting/
├── data/
│   ├── raw/
│   ├── reference/
│   ├── derived/
│   └── cache/
├── outputs/                  ignored generated products
├── docs/
│   ├── architecture/
│   ├── codebase/
│   ├── operations/
│   ├── research/
│   └── diagrams/
├── scripts/
│   ├── operations/
│   └── development/
├── openspec/                 tool-owned
├── .agents/                  tool-owned
├── pyproject.toml
├── uv.lock
├── README.md
└── root tool/config files
```

This tree is an end state, not a request to create empty folders. A target
directory should appear only when the first real owner is moved into it.

## Target top-level concepts

| Top-level path | Single meaning |
|---|---|
| `src/` | Importable, reusable product logic. |
| `apps/` | Thin human/container/UI entrypoints and packaging. |
| `deployment/` | Thin cloud framework wrappers and deployment manifests. |
| `research/` | Executable experiments and notebooks with no production support implication. |
| `tests/` | Automated checks grouped by boundary/scope. |
| `assets/` | Versioned non-code runtime assets grouped by consumer. |
| `data/` | Local input/derived/cache contract root; mostly ignored runtime state. |
| `outputs/` | Ignored generated human/machine outputs. |
| `docs/` | Authored knowledge, operations, research decisions, and diagrams. |
| `scripts/` | Shell-only operations/development helpers, not application logic. |
| `openspec/`, `.agents/` | Tool-owned metadata, not product modules. |

No top-level `legacy/`, `goal/`, `conf/`, or generic `tools/` remains in the
target. Their useful contents either gain explicit ownership or are deleted.

## Source and entrypoint migrations

| CURRENT PATH | → TARGET PATH | Action | Why |
|---|---|---|---|
| `src/stockanalysis/config.py` | → `src/stockanalysis/settings.py` | Rename/evolve after callers are ready | Communicate validated runtime/path settings ownership. Avoid doing a cosmetic rename before consolidating configuration. |
| `src/stockanalysis/commonlib.py` | → specific owner modules, then delete | Split by actual caller need | Configuration moves to `settings`; text/image behavior to reporting if still used; numeric market helpers to the consuming module. Do not create another generic utils file. |
| `src/stockanalysis/runtime/crawlers/` | → `src/stockanalysis/crawling/` | Rename and consolidate | “Crawling” states the responsibility; “runtime” adds no boundary. Keep one implementation per market/data product. |
| `.../twse_bs_report.py` | → `crawling/twse.py` or `crawling/twse_broker_reports.py` | Move/rename | Canonical TWSE adapter; choose the longer name only if multiple TWSE products make `twse.py` too broad. |
| `.../tpex_local_runner.py` | → `crawling/tpex.py` or `crawling/tpex_broker_reports.py` | Move/rename | Canonical TPEX adapter; remove “local runner” from reusable implementation ownership. |
| `.../{twse,tpex}_daily_ohlc.py` | → `crawling/{twse,tpex}.py` or explicit sibling modules | Merge only where cohesive | OHLC and broker endpoints may share market parsing/config, but do not force a large market module. |
| `.../tpex_bs_report.py` | → `apps/diagnostics/tpex_browser.py` or delete | Extract diagnostic seam/delete duplicate | Retain only the helper actually needed for an approved recovery diagnostic. |
| `.../tpex_bs_report_new.py` | → `research/experiments/tpex_browser_variants.py` or delete | Reclassify/delete | It is not production ownership; preserve only if an experiment owner exists. |
| `.../test.py` | → `apps/diagnostics/tpex_token_smoke.py` | Move/rename | It is a manual smoke/benchmark, not a package unit test. |
| `src/stockanalysis/analysis/` stable modules | → `src/stockanalysis/signals/` | Rename selectively | “Signals” distinguishes reusable domain calculations from entrypoint/research scripts. |
| Research-only modules under `src/.../analysis/` | → `research/experiments/` | Move after caller audit | Production package should not imply support for exploratory algorithms. |
| `apps/etl/bs_report_pipeline.py` | → `src/stockanalysis/pipelines/broker_reports.py` | Move implementation | Reusable ETL logic belongs in the package and can be tested without CLI startup. |
| `apps/etl/build_ohlc_parquet.py` reusable functions | → `src/stockanalysis/pipelines/ohlc.py` | Extract implementation | Make the file/schema boundary testable; keep argument parsing in `apps/data`. |
| `apps/etl/run_bs_report_etl.py` | → `apps/data/refresh_broker_reports.py` | Move/rename as thin CLI | Name describes operator outcome rather than implementation acronym. |
| Active `apps/etl/build_ohlc_parquet.py` CLI | → `apps/data/build_ohlc.py` | Move/rename | Groups supported data preparation commands. |
| `apps/etl/ETL_*.py`, `main.py` | → canonical `apps/data/` command, `research/experiments/`, or delete | Merge/classify | Avoid parallel entrypoints for the same pipeline. Human usage must be checked first. |
| `apps/analysis/refresh_broker_branch_accumulation.py` | → `apps/analysis/refresh_broker_signals.py` | Rename after workflow extraction | Thin supported signal command; reusable behavior remains in `signals/workflows`. |
| Portal composition/render logic in `build_trigger_days_gt5_case_review.py` | → `src/stockanalysis/reporting/case_review.py` | Extract implementation | Gives static reporting one package owner and permits freshness/renderer tests. |
| Portal CLI in the same file | → `apps/reporting/build_case_review.py` | Move/rename | Separates publishing from research analyses and makes the supported output obvious. |
| Reused functions imported between `apps/analysis/run_*` | → `src/stockanalysis/signals/` or `research/experiments/_shared/` | Move by support status | `apps` stops acting as an undeclared library. Research sharing must not leak into production packages. |
| `Analysis_BsReport_v1.py` to `v4.py` | → `research/experiments/bs_report/` with descriptive names, then prune | Reclassify/rename/delete | Preserve research lineage while removing numbered files from the supported-command namespace. |
| Other unverified `run_*`, `build_*`, `make_*` | → `research/experiments/<topic>/` or approved `apps/{analysis,reporting}/` | Classify first | Operator-invoked scripts need evidence before deletion; supported commands should be few and explicit. |
| Dash data loaders/calculations in `apps/visualization/app.py` | → `src/stockanalysis/query/dashboard.py` | Extract implementation | Test read models without Dash and remove direct storage/domain coupling from callbacks. |
| Remaining Dash shell | → `apps/visualization/app.py` | Keep/thin | Layout, callbacks, and process startup are legitimate app ownership. |
| `apps/tools/Text2Img.py` | → `src/stockanalysis/reporting/text_image.py` or delete | Move/delete | Place by reporting responsibility; do not preserve a generic one-file tools bucket. |

## Cloud and operational migrations

| CURRENT PATH | → TARGET PATH | Action | Why |
|---|---|---|---|
| Repeated prepare logic in `deployment/prepare-*/main.py` | → `src/stockanalysis/workflows/cloud_dispatch.py` | Extract shared functions | Keep service wrappers deployable while making payload/status/batching behavior one tested owner. |
| Repeated trigger logic in `deployment/trigger-*/main.py` | → same `cloud_dispatch.py` | Extract shared functions | Normalize async acknowledgement and job payload construction without merging cloud resources. |
| Four `deployment/*` directories | → same paths | Keep/thin | Separate resources remain justified by packaging and market runtime differences. Renaming them creates deployment risk without structural value. |
| `apps/services/` | → delete | Delete after verification | Duplicates active deployment ownership and has no current deploy/caller evidence. |
| `legacy/apps/...` | → delete | Delete after recovery check | Git history is the appropriate archive for confirmed orphaned source. |
| `scripts/deploy_gcp_environment.sh` | → `scripts/operations/deploy_gcp_environment.sh` | Move | Groups authoritative operational scripts and leaves room for explicit local development helpers. |
| `scripts/get-cloud-run-service.sh` | → `scripts/operations/inspect_cloud_run_service.sh` | Move/rename | Make read-only operational intent visible. |
| `scripts/{twse,tpex}.sh` | → supported `apps/crawling/*` commands or `scripts/development/` | Merge or move | Shell wrappers should not select alternate business implementations. Keep only if they add real local process behavior. |
| Browser/display/process shell helpers | → `scripts/development/browser/` or delete | Group/classify | Keep local environment operations separate from deploy/runbook authority. |
| `scripts/commands.sh`, root `commands` | → `docs/operations/command-reference.md` or delete | Merge | Command documentation should be readable/searchable documentation, not two vague executable-looking files. |

## Research, docs, tests, and artifact migrations

| CURRENT PATH | → TARGET PATH | Action | Why |
|---|---|---|---|
| `notebooks/*.ipynb` | → `research/notebooks/<topic>/` | Move/group | Makes experimental lifecycle explicit and keeps the root concept small. |
| `notebooks/.ipynb_checkpoints/` | → delete | Delete generated files | Jupyter recreates them; they are not source or recovery history. |
| `notebooks/*.csv`, `*.xls` | → `tests/fixtures/`, `data/reference/`, ignored `data/raw/`, or delete | Classify individually | Placement must reflect whether each file is a small stable fixture, canonical reference, reproducible input, or generated result. |
| `goal/*.md` | → `docs/research/` | Merge/move | These are durable research logs/registries; `goal` is an unclear top-level ownership label. |
| `docs/research/` | → same path | Keep/receive goal docs | One authored research-knowledge home. |
| `docs/codebase/*.md` | → same path | Keep | Current evidence-oriented repository knowledge base. |
| `docs/runtime-workflow-map.md` | → `docs/architecture/runtime-workflow-map.md` | Move | Group runtime and target architecture views. |
| `docs/architecture-blueprint.md` | → `docs/architecture/system-architecture.md` | Move/rename | Stable descriptive name inside an explicit architecture area. |
| This blueprint | → `docs/architecture/folder-migration.md` after migration begins | Move/rename later | Co-locate structural governance; keep current filename until links are updated. |
| `docs/gcp-*.md`, `docs/data-refresh-runbook.md` | → `docs/operations/` | Move/group | Runbooks and cloud state have operational ownership and freshness requirements. |
| `docs/project-structure.md` | → merge into this blueprint or `docs/codebase/STRUCTURE.md`, then delete duplicate | Merge | Avoid competing structure documents with different freshness. |
| `docs/*.html` generated explorers | → ignored `outputs/reports/` or explicitly published `docs/examples/` | Move/classify | Generated output should not look like authored architecture docs. Keep checked-in examples only by explicit policy. |
| `data-flow.drawio` | → `docs/diagrams/data-flow.drawio` | Move/update | Authored source diagram belongs with docs; update stale crawler labels during the move. |
| `architecture.key` | → `docs/design/architecture.key` or delete | Move/classify | Keep only if it is an intentionally maintained source document. |
| Current `tests/test_*.py` | → `tests/unit/signals/` | Move gradually | Mirrors stable signal ownership and keeps unit scope explicit. |
| New payload/schema/freshness tests | → `tests/contracts/` | Add in later implementation | Guard the file and cloud contracts driving the target structure. |
| New ETL/filesystem/cloud-client tests | → `tests/integration/` | Add in later implementation | Separate boundary tests from pure calculation tests. |
| Token/single-symbol/container checks | → `tests/smoke/` for automated harnesses; `apps/diagnostics/` for manual commands | Separate | Do not confuse manual diagnostics with automated tests. |
| `assets/models/*` and TWSE alphabet assets | → `assets/crawling/twse/` | Move after reference updates | Consumer-specific ownership prevents an undifferentiated assets directory. |
| Report/UI-only images | → `assets/reporting/` or `apps/visualization/assets/` | Move by consumer | Shared report assets and Dash-served assets have different runtime packaging needs. |
| `conf/default.properties` | → settings/env contract, then delete `conf/` | Migrate/delete | Remove the legacy second configuration authority after all callers move. |

## Data and output target layout

The repository should keep file-based storage. The migration is semantic and
incremental; it is not a bulk move of all historic data.

```text
data/
├── raw/           immutable or source-shaped local copies
│   ├── broker_reports/
│   └── ohlc/
├── reference/     broker, symbol, company, warrant metadata
├── derived/       reproducible Parquet/features used downstream
└── cache/         replaceable news/network/process caches

outputs/
├── analysis/      signal/experiment results with manifests
├── reports/       HTML/CSV/JSON/Markdown publications
└── tmp/           disposable intermediate work
```

| CURRENT PATH | → TARGET PATH | Why |
|---|---|---|
| Existing raw daily/broker folders under `data/` | → `data/raw/<dataset>/` | Separate source-shaped material from transformations. |
| Existing lookup/reference CSV/JSON | → `data/reference/<domain>/` | Make manually maintained or externally sourced reference ownership explicit. |
| `data/_derived/` and derived Parquet trees | → `data/derived/<dataset>/` | Remove special underscore convention and group reproducible outputs. |
| Network/news caches mixed with outputs or analysis trees | → `data/cache/<integration>/` | Caches have replacement semantics, not publication semantics. |
| `outputs/analysis/trigger_days_gt5_case_review/` | → `outputs/reports/case_review/` | It is a publication assembled from analyses, not itself an analysis module. |
| Temporary crawler/report folders | → `outputs/tmp/<workflow>/<run-id>/` | Give disposable runs a bounded, identifiable location. |

All new derived/output trees should include a small manifest with producer,
source horizon/fingerprint, schema version, revision, record count, and creation
time. Existing historical data should be read compatibly and migrated only when
rewritten by a normal workflow.

## Naming rules for the target

- Python packages and modules: lowercase `snake_case`.
- Human/container entrypoints: action plus object, such as
  `refresh_broker_reports.py`, `build_case_review.py`, or
  `tpex_token_smoke.py`.
- Do not use `new`, `v2`, `v3`, `final`, or `test` to communicate lifecycle.
  Use Git history for production evolution and descriptive experiment names for
  research branches.
- `apps/` files may import `stockanalysis.*`; `src/stockanalysis` must never
  import `apps`, `deployment`, or `research`.
- A target folder must have one sentence of ownership that excludes neighboring
  folders. If it cannot, do not create it.
- Avoid `utils`, `common`, `misc`, `new`, and one-file top-level categories.
- Generated files never share an authored-source folder unless an explicit
  checked-in example policy names them.

## Migration order and safety gates

| Phase | Structural work | Required gate before proceeding |
|---|---|---|
| 0. Registry | Record supported entrypoints, owners, image revisions, inputs, outputs, and current classifications | Human confirmation for manual research/diagnostic commands. |
| 1. Generated cleanup | Remove ignored caches/checkpoints/OS files; correct ignore rules | Exact target enumeration; no source/reference data included. |
| 2. Tests and contracts | Add contract folders/tests and characterization fixtures | Current behavior captured, especially ETL partial success and trigger semantics. |
| 3. Package seams | Move ETL, reporting, query, workflow, then signal modules behind compatibility entrypoints | Tests pass after each single-responsibility move; no import from package to apps. |
| 4. Canonical crawlers | Establish image-to-commit parity and consolidate TWSE/TPEX implementations | Bounded live/container validation and operator diagnostic inventory. |
| 5. Entrypoint cleanup | Thin/rehome apps; group scripts and operations docs | All runbooks, Dockerfiles, Cloud Build, and deployed commands updated together. |
| 6. Research/documentation | Move goal/notebooks/research CLIs; consolidate duplicate docs | Owners confirm supported versus experimental status. |
| 7. Deletion | Delete old services, duplicate crawlers, obsolete versions, legacy config | No supported references; recoverable Git commit/tag; deployment parity verified. |
| 8. Data layout | Adopt new paths for newly generated data; compatibility reads for history | Manifests and path migration checks prevent silent split-brain datasets. |

## Explicit non-goals

- Do not convert the repository into multiple Python projects solely because it
  has multiple runtime commands.
- Do not create a directory for every class, algorithm, or Cloud Run resource.
- Do not mirror a generic clean-architecture template with `domain`,
  `application`, `infrastructure`, and `interfaces` layers when the concrete
  workflow-oriented names are clearer.
- Do not move all historic data or outputs in one operation.
- Do not archive dead code into another permanent source folder; retain it in
  Git history once deletion is verified.
- Do not classify a script as orphaned only because imports do not reveal human
  execution.
- Do not merge TWSE and TPEX implementations where their browser, captcha,
  Turnstile, proxy, timeout, or resource behavior genuinely differs.

## Target completion criteria

- Every supported executable is in `apps/` or `deployment/` and is a thin
  composition root.
- Every reusable production behavior has one owner under `src/stockanalysis/`.
- `research/` clearly contains unsupported experiments and notebooks.
- `legacy/`, `goal/`, `conf/`, and generic `apps/tools`/`apps/services` no longer
  exist.
- No canonical runtime path contains `new`, numbered production versions, or an
  ambiguous `test.py`.
- Generated checkpoints, caches, local cloud state, data, and outputs are
  excluded from source navigation.
- Documentation, operational runbooks, research decisions, and generated reports
  have distinct homes.
- Tests are organized by unit, contract, integration, and smoke boundaries.
- Existing Docker, Cloud Build, scripts, runbooks, and deployed resource paths
  all point to the same canonical crawler implementations.
- The resulting top level still communicates only ten product concepts:
  source, apps, deployment, research, tests, assets, data, outputs, docs, and
  scripts, plus tool-owned metadata.

## Decisions requiring human confirmation before migration

1. Which `apps/analysis` and `apps/crawlers` commands are still run manually?
2. Which TPEX debug/local-runner paths are required recovery tools rather than
   experiments?
3. Should generated HTML explorers be published from Git, or always regenerated
   under ignored `outputs/`?
4. Which notebook CSV/XLS files are stable fixtures/reference inputs versus
   disposable results?
5. Is `architecture.key` an actively maintained source document?
6. Should `AGENT.md` or `AGENTS.md` be the sole repository instruction file?
7. Are any `legacy/` files required for an external deployment not represented
   in the inspected GCP project?

## Blueprint maintenance

Update this document only when runtime evidence, supported entrypoints, or the
accepted target architecture changes. During migration, mark individual mapping
rows complete through a dedicated change record rather than silently editing the
CURRENT classification. Re-run source/import, Docker/deployment, operator-command,
and generated-artifact checks before declaring any uncertain path orphaned.
