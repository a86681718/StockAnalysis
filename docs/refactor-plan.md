# StockAnalysis 分階段重構計畫

狀態：僅規劃，尚未開始實作
依據日期：2026-08-14

本計畫以以下文件及實際程式碼為依據：

- `docs/codebase/*`
- `docs/runtime-workflow-map.md`
- `docs/architecture-blueprint.md`
- `docs/folder-structure-migration-blueprint.md`
- 現行 Docker、Cloud Build、deployment handlers、ETL、分析、報表與 Dash 程式碼

目標不是重寫系統，而是在保留可觀察行為的前提下，逐步讓現有專案收斂到建議的 TARGET architecture。每個階段必須能獨立審查、驗證、提交及回滾。

## 一、目前狀態

StockAnalysis 是 Python 3.11 的批次資料處理工作區，主要 runtime 包含：

1. Cloud Scheduler → prepare service → Cloud Tasks → trigger service → Cloud Run crawler → GCS／Firestore。
2. 本機 GCS 同步與 CSV-to-Parquet ETL。
3. 以檔案作為契約的訊號分析與靜態報表產生流程。
4. 讀取本機分析產物的 Dash 應用程式。
5. 大量研究、診斷、舊版和替代實作。

目前最清楚的依賴方向為：

```text
apps/ 與 deployment/
    → src/stockanalysis/
        → 本機檔案、外部市場、瀏覽器與 GCP SDK
```

主要問題：

- 現有 27 個測試集中在分析邏輯，缺乏 ETL、cloud handler、crawler contract、報表 freshness 與 Dash query 測試。
- TPEX trigger 同步等待 Cloud Run Job；TWSE trigger 則立即回傳 operation。
- ETL 即使部分 CSV 失敗，仍可能將整個日期資料夾標記成功並封存。
- TPEX/TWSE crawler 存在多個重複或替代實作。
- `deployment/` 的 active services 與 `apps/services/`、`legacy/` 重複。
- `apps/analysis/` 同時是 entrypoint、研究區與非正式 library。
- portal 可能混用不同更新日期的輸入。
- 已部署 TPEX image 與目前 checkout source revision 不一致。
- `apps/visualization/app.py` 同時負責資料讀取、運算、cache、callback 與 UI。

## 二、目標狀態

目標仍是簡單的模組化批次系統，而不是微服務化或 clean architecture 套版：

```text
薄的 apps/deployment entrypoints
    → 明確的 workflow functions
        → crawling / pipelines / signals / reporting / query
            → contracts / settings
```

原則：

- 每個 production responsibility 只有一個 canonical implementation。
- `src/stockanalysis/` 是 reusable production logic 的唯一擁有者。
- `apps/` 只保留 CLI、container 與 UI composition roots。
- `deployment/` 保留四個部署邊界，但共享經測試的 orchestration logic。
- 研究與診斷程式不再混入 production namespace。
- 保留 GCS、Firestore、Parquet、CSV、JSON 與 HTML，不導入新資料庫或 repository framework。
- 不導入 DI container、event bus、workflow engine 或無實際需求的 abstraction。

## 三、不可改變的核心行為

除非另立明確的行為修正 change，所有重構必須保留：

- TPEX sequential crawling，不加入 `--workers` 或平行瀏覽器。
- TPEX 僅處理有交易的四碼 `EW` 股票及名稱含「購」或「售」的 `WW` 權證。
- `--date YYYYMMDD` 與 `--date=YYYYMMDD` 的支援，以及日期傳遞至來源請求、輸出資料夾與 CSV。
- 每次 retry 的失敗原因與最終 `FINAL FAILURE`，且不得記錄 token 或 proxy 密碼。
- TWSE captcha/model 與 TPEX Xvfb/Chromium/Turnstile/proxy runtime 行為。
- 現有 GCS object path、Firestore collection、Parquet/CSV schema 與 portal link 結構，直到有 compatibility migration。
- 現有獨立 analysis commands 的可執行性。
- `trigger_days_gt5_case_review/index.html` 是「關鍵分點買超」的主要 portal。

## 四、主要受影響範圍

| 路徑 | 預期變更 | 前置依賴 |
|---|---|---|
| `tests/` | 新增、補強、後期重新分類 | 必須先於 production refactor |
| `src/stockanalysis/config.py` | 補強，最後才考慮 rename | 多數 apps/package callers |
| `src/stockanalysis/commonlib.py` | 依責任拆分後刪除 | 舊 crawlers 與 analyses |
| `src/stockanalysis/runtime/crawlers/` | consolidate、move、rename | Docker、wrapper、script、Cloud Run Job |
| `src/stockanalysis/analysis/` | 保留邏輯，選擇性改為 `signals` | tests、portal、Dash |
| `apps/etl/` | 修正、extract、thin | manifests、archive、data paths |
| `apps/analysis/` | classify、extract、reorganize | outputs、subprocess、互相 import |
| `apps/visualization/app.py` | extract query/read model | local artifact schemas |
| `apps/{twse,tpex}/` | normalize wrappers/build paths | Cloud Build、Cloud Run Jobs |
| `apps/crawlers/` | classify/consolidate | 人工執行狀況不完整 |
| `apps/services/` | 刪除 | 最終 reference/deployment audit |
| `deployment/*` | 測試與 thin | Tasks、Firestore、Run API |
| `legacy/` | 刪除 | 外部 deployment 確認 |
| `scripts/` | 後期更新及分類 | Docker paths、operator commands、runbooks |
| `docs/`、`goal/`、`notebooks/` | 後期整併 | production paths 穩定後 |

# 五、執行階段

## Phase 1：建立行為安全基線

### 目標

在任何 implementation 變更前，建立 entrypoint registry、characterization tests 與代表性 fixtures。

### 受影響檔案／模組

- `tests/`
- `README.md` 或新的 supported-entrypoint registry
- Dockerfiles、Cloud Build files、deployment `project.toml`
- active wrappers、runtime modules 與代表性 artifact fixtures

### 前置條件

- 記錄目前 Git revision、active image tags/digests、GCP project/region。
- 保留目前 mixed worktree，不暫存無關的 skills、`AGENTS.md` 或 `skills-lock.json`。
- 標記每個 entrypoint 為 production、diagnostic、research 或 unknown。

### 修改前測試

- 現有 27-test suite。
- `uv lock --check`。
- canonical modules import check。
- ETL temporary-directory dry run。
- Dockerfile → entrypoint → wrapper → source module 靜態鏈路檢查。

### 精確變更

1. 建立 entrypoint registry，記錄 command、owner、classification、input、output、runtime 與 image revision。
2. 建立以下 fixtures：
   - 完整及部分損壞的 ETL folder。
   - TWSE/TPEX trigger payload。
   - Firestore status transitions。
   - 最小 OHLC 與 broker-report schemas。
   - portal prerequisite manifests。
   - Dash loader inputs。
3. 新增 characterization tests，先忠實記錄現況，不在本階段修正行為。
4. 新增 dependency boundary scan，確保 `src/stockanalysis` 不 import `apps`、`deployment` 或 `research`。

### 必須保持不變

全部 runtime behavior。

### 可刪除項目

無。

### 風險

- Characterization test 可能把錯誤行為誤當永久需求。
- Import deployment modules 可能在 import-time 初始化 metadata/GCP clients。

### 驗證方法

```bash
uv lock --check
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p 'test_*.py'
rg -n '^(from|import) (apps|deployment|research)' src/stockanalysis
```

### 回滾策略

直接 revert Phase 1 的單一文件／測試 commit；本階段不得改變 external state。

## Phase 2：移除確定 generated 與確定 dead code

### 目標

移除不具 runtime 價值的內容，且不碰人工使用狀況仍不明的 scripts。

### 受影響檔案／模組

- tracked `.DS_Store`、`__pycache__`、Jupyter checkpoints
- `apps/services/`
- `legacy/apps/`
- `apps/crawlers/Crawler_TPEXBuySellReport.py`

### 前置條件

- 完成 Phase 1 registry。
- 搜尋所有 imports、shell、Docker、Cloud Build、docs 與 deployment references。
- 確認 active GCP resources 使用 `deployment/`。
- 確認沒有未受檢查的 external deployment 使用 `legacy/`。

### 修改前測試

- 完整 baseline suite。
- Old/new service responsibility comparison。
- Exact reference scan。

### 精確變更

1. 移除 tracked generated artifacts 並補足精確 ignore rules。
2. 刪除 `apps/services/Service_PrepareTPEXTaskList.py` 與 `Service_TriggerTPEXRunJob.py`。
3. 刪除四個 orphaned `legacy/apps/...` service directories。
4. `Crawler_TPEXBuySellReport.py` 僅在人工使用確認後刪除。
5. 不建立另一個 archive directory；以 Git history 保存歷史。

### 必須保持不變

- 四個 active deployment services。
- Docker image entrypoints。
- scheduled crawling、local commands、data 與 outputs。

### 可刪除項目

僅限上述已確認 paths；alternate package crawlers 與 research CLI 尚不刪除。

### 風險

Repo 外部腳本可能仍參考看似 dead 的 path。

### 驗證方法

```bash
rg -n 'apps/services|legacy/apps|Crawler_TPEXBuySellReport' \
  --glob '!docs/**' --glob '!*.md' .
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p 'test_*.py'
git diff --check
```

### 回滾策略

刪除必須獨立 commit；刪除前建立 rollback commit/tag，必要時整個 revert。

## Phase 3：修正 ETL completion semantics

### 目標

避免部分 CSV 失敗的 folder 被標記並封存為成功。

### 受影響檔案／模組

- `apps/etl/bs_report_pipeline.py`
- `apps/etl/run_bs_report_etl.py`
- ETL tests/fixtures

### 前置條件

- Characterize current behavior。
- 決定 partial folder 留在 inbox 或進入 quarantine。
- 備份代表性 manifest 與 inbox/archive metadata。

### 修改前測試

- 全部有效 CSV。
- 一個有效、一個損壞 CSV。
- 無 CSV、無效日期 folder。
- 既有 Parquet、既有 archive destination、dry-run。

### 精確變更

1. 以明確 result type 取代鬆散 dictionary，記錄 expected、processed、failed、updated counts。
2. 只有所有預期 CSV 都被正確處理時才算 success。
3. Manifest 加入 failed filenames 與 error summaries。
4. 僅封存完整成功的 folder。
5. 存在未接受 failure 時回傳 non-zero CLI status。
6. 本階段保留既有 worker count、Parquet sort、deduplication 與 compression。

### 必須保持不變

- CLI flags/defaults。
- GCS folder discovery。
- market paths。
- Parquet column alignment、sorting、deduplication 與 compression。
- 舊 successful manifest 可讀性。

### 可刪除項目

Compatibility 完成後移除舊 result fields；不刪 historical manifests。

### 風險

過去被視為成功的 partial folder 可能重新進入處理流程。

### 驗證方法

- 所有 folder outcome unit tests。
- Temporary-directory integration test。
- 真實 manifest 副本 dry-run。
- 對完整 folder 比對前後 Parquet row count 與內容。

### 回滾策略

Revert phase commit 並恢復 manifest 備份；初期驗證不得移除原始 inbox。

## Phase 4：建立 cloud payload 與 status contracts

### 目標

讓 Scheduler、Tasks、service 與 Job 邊界變成明確且可測試的契約。

### 受影響檔案／模組

- 新增 `src/stockanalysis/contracts/crawl_jobs.py`
- 四個 `deployment/*/main.py`
- contract tests

### 前置條件

- Characterize current request acceptance。
- 盤點 queued payload formats 與 Firestore status values。

### 修改前測試

- Non-JSON、missing/empty/invalid symbols。
- `YYYY/MM/DD` 與 `YYYYMMDD`。
- Firestore filtering 後無 symbols。
- TWSE/TPEX job argument serialization。
- 既有 status compatibility。

### 精確變更

1. 建立小型 payload parser/normalizer。
2. 定義 status constants。
3. 明確拒絕缺少日期、空 batch 或超出上限的 batch。
4. 保留有限期間的 legacy payload compatibility。
5. 先以 optional metadata 加入 run ID/image revision。

### 必須保持不變

- Compatibility period 內的 collection naming。
- 舊 task body 可被接受。
- Symbol order、date propagation、GCP resource names 與 IAM/OIDC。

### 可刪除項目

所有 handlers 採用 contract 後，移除重複 validation/date normalization。

### 風險

已排隊的 legacy task 可能被新 parser 拒絕。

### 驗證方法

以 fake Firestore/Run/Tasks clients 執行 handler contract tests；第一個 commit 不部署。

### 回滾策略

保留 legacy parser；必要時只 revert handler integration。

## Phase 5：統一 trigger acknowledgement

### 目標

移除 TPEX trigger 的同步等待，且不與其他 cloud consolidation 混在同一 change。

### 受影響檔案／模組

- `deployment/trigger-tpex-job/main.py`
- trigger tests
- deployment/runbook docs

### 前置條件

- Phase 4 完成。
- 確認 Cloud Tasks retry expectations。
- 記錄現有 service/job timeout 與 retries。

### 修改前測試

- `run_job()` operation handling。
- Operation ID response。
- Client exception、no runnable symbols。
- 確認 handler 不呼叫 `operation.result()`。

### 精確變更

1. TPEX 改為與 TWSE 相同的 asynchronous acknowledgement。
2. 立即回傳 operation ID。
3. Completion 由 Firestore/job logs 表達，不由 HTTP response 表達。
4. 不更改 TPEX Job resources。

### 必須保持不變

Payload、Firestore `running`、job name/region/args、proxy secret、image、crawler 與 task authentication。

### 可刪除項目

Blocking `response.result()` 與相關等待 logging。

### 風險

HTTP 200 將只代表成功啟動 Job，不代表 crawl 完成。

### 驗證方法

- Unresolved fake operation unit test。
- 只部署 TPEX trigger service。
- 以單一 bounded batch 驗證快速 HTTP response、Job creation、Firestore 與 GCS。

### 回滾策略

只回復前一個 trigger service revision，不同時改 crawler Job。

## Phase 6：合併 crawler implementations

### 目標

每個市場只保留一個 production crawler，並保留明確命名的必要 diagnostics。

### 受影響檔案／模組

- `tpex_local_runner.py`
- `tpex_bs_report.py`
- `tpex_bs_report_new.py`
- crawler `test.py`
- TWSE runtime crawler 與兩個 wrappers
- `apps/{tpex,twse}/`
- `scripts/{tpex,twse}.sh`
- Docker/Cloud Build files

### 前置條件

- 建立 deployed image-to-commit provenance。
- 由 operator 決定 debug/local recovery paths。
- 具備 token-only 與 one-symbol smoke harness。

### 修改前測試

- 兩種 `--date` 語法。
- EW/WW traded filtering。
- Retry reason 與 `FINAL FAILURE`。
- TPEX `操作逾時` transient handling。
- Output paths、CSV schema、Docker entrypoint chain。
- Token-only 與 one-symbol live/container validation。

### 精確變更

1. 指定 `tpex_local_runner.py` 為 canonical TPEX implementation。
2. 從 alternate implementations 只抽出仍需要的 diagnostic helpers。
3. 將 `test.py` 改為明確的 manual diagnostic command。
4. 無 supported caller 後刪除 `tpex_bs_report.py` 與 `tpex_bs_report_new.py`。
5. 指定 package `twse_bs_report.py` 為 canonical TWSE implementation。
6. 讓兩個市場 wrappers 都成為薄入口。
7. 更新 `scripts/twse.sh`，不再指向舊版完整 crawler。
8. TWSE/TPEX 只共享經證明相同的小型 helpers。

### 必須保持不變

- TPEX sequential crawling。
- EW/WW filtering 與日期傳遞。
- Retry/failure diagnostics 且不得洩漏 secrets。
- Xvfb/Chromium/container、GCS、Firestore 與 TWSE captcha 行為。

### 可刪除項目

- 兩個 alternate TPEX package crawlers。
- 舊 TWSE full implementation。
- Operator 確認無用的 debug build paths。

### 風險

這是高風險階段：browser、Turnstile、proxy、deployed-source drift 與 recovery tools 同時交會。

### 驗證方法

依序執行 static tests → image build → token-only → 指定日期單一 symbol → 小型 sequential batch → GCS/Firestore/schema comparison。

### 回滾策略

TWSE/TPEX 分開 commit/deploy，保存 prior image digest 與 Job revision；canonical runtime 未驗證前不刪 alternate code。

## Phase 7：合併 cloud orchestration logic

### 目標

移除 prepare/trigger 重複控制流程，但保留四個 deployment boundaries。

### 受影響檔案／模組

- 新增 `src/stockanalysis/workflows/cloud_dispatch.py`
- 四個 `deployment/*/main.py`
- handler/contract tests

### 前置條件

- Phase 4、5 完成。
- 將 market-specific differences 明確列為 configuration。
- Cloud clients 可被注入，避免 import-time metadata calls。

### 修改前測試

- TWSE security block。
- TPEX stock/warrant filtering。
- Batch construction、Firestore initialization/filtering。
- Task OIDC/deadline、Run Job overrides、HTTP errors。

### 精確變更

1. 將 metadata/client construction 移至 composition boundary。
2. 抽出共用 payload、batching、status 與 job-start functions。
3. 保留 market-specific list fetching 與 resource configuration。
4. 四個 `main.py` 只保留 framework wrapper。
5. 暫不合併 service directories 或 requirements。

### 必須保持不變

四個 service names/build targets、Scheduler/queue topology、market batch defaults、OIDC、dispatch deadline 與 security-block behavior。

### 可刪除項目

重複的 validation、Firestore update、task creation 與 Run Job request code。

### 風險

過度抽象會掩蓋真實的 TWSE/TPEX 差異。

### 驗證方法

比對重構前後建立的 task/job requests；每次只部署一組 market prepare/trigger。

### 回滾策略

每個 wrapper 可獨立回復 inline implementation；逐一回復 service revision。

## Phase 8：正規化 pipeline 與 signal ownership

### 目標

將 reusable implementation 從 `apps/` 移入 package，建立單向依賴。

### 受影響檔案／模組

- `apps/etl/bs_report_pipeline.py`
- `apps/etl/build_ohlc_parquet.py`
- 新增 `src/stockanalysis/pipelines/`
- `src/stockanalysis/analysis/`
- 被其他 scripts import 的 `apps/analysis` modules

### 前置條件

- ETL correctness tests。
- 完整盤點 `apps.analysis` script-to-script imports。
- 判斷 reusable logic 是 production signal 或 research-only。

### 修改前測試

- ETL output equivalence。
- Existing analysis suite。
- Old CLI import tests。
- Representative signal output comparison。

### 精確變更

1. `BsReportEtl` 移到 `stockanalysis.pipelines.broker_reports`。
2. OHLC reusable functions 移到 `stockanalysis.pipelines.ohlc`。
3. Old CLI 保留一個 transition phase 的 compatibility import。
4. 僅將 stable reused calculation 移入 package。
5. 初期保留 `stockanalysis.analysis` 名稱；所有 callers 收斂後才改為 `signals`。
6. 禁止 package import `apps`、`deployment` 或 `research`。

### 必須保持不變

CLI、output paths/schemas/order、deduplication、compression、analysis thresholds/ranking 與 individual stage commands。

### 可刪除項目

所有 callers 移轉後刪除 compatibility imports 與 app-level duplicate implementations。

### 風險

即使邏輯不變，move 仍可能破壞 relative path 或 `sys.path`。

### 驗證方法

同 fixtures 比對前後 outputs、所有 affected CLI `--help`、full tests 與 import-boundary scan。

### 回滾策略

保留 old entrypoints/import shims；每個 module move 獨立 commit。

## Phase 9：正規化 reporting 與 freshness ownership

### 目標

拆分 case-review computation/rendering/CLI，防止發布 mixed-horizon portal。

### 受影響檔案／模組

- `build_trigger_days_gt5_case_review.py`
- `refresh_broker_branch_accumulation.py`
- 新增 `stockanalysis.reporting.case_review`
- 新增 `stockanalysis.workflows.publish_case_review`
- portal tests/fixtures

### 前置條件

- 保存代表性 portal inputs/outputs。
- 決定不同 signal horizons 的相容規則。
- 定義 provenance manifest fields。

### 修改前測試

- Case selection/order、`review_eligible`、`ongoing`、rank。
- Missing news/cache 與 required inputs。
- Mixed freshness detection。
- HTML/detail/CSV/JSON inventory。

### 精確變更

1. 抽出 loading、scoring、case model 與 rendering functions。
2. CLI 只保留 arguments 與 exit codes。
3. 建立執行或驗證三個 prerequisites 的 publish workflow。
4. 寫入 source horizon/fingerprint metadata。
5. Production publish 預設拒絕 incompatible prerequisites。
6. 以明確 diagnostic override 取代 silent stale input。

### 必須保持不變

Case rules/order、portal filenames/detail links、optional news 與 standalone signal commands。

### 可刪除項目

抽出後的重複 path discovery 與 builder fragments。

### 風險

HTML 可能因 timestamp/news 而不 deterministic。

### 驗證方法

正規化 volatile fields 後，比對 case counts、IDs、order、links、CSV/JSON schema 與 detail inventory；確認目標是 `trigger_days_gt5_case_review/index.html`。

### 回滾策略

Old builder 保留至 extracted implementation 等價；entrypoint switch 另立 commit。

## Phase 10：抽出 Dash read model

### 目標

不重做 UI，只讓資料讀取及計算可獨立測試。

### 受影響檔案／模組

- `apps/visualization/app.py`
- 新增 `src/stockanalysis/query/dashboard.py`
- Dash/query tests

### 前置條件

- Representative fixtures。
- 盤點 global caches 與 callback dependencies。
- Characterize missing-file/empty-frame behavior。

### 修改前測試

- OHLC/broker discovery、name mapping、filtering、aggregation、case derivation、missing inputs 與 cache behavior。

### 精確變更

1. 一次抽出一條 loader/calculation path。
2. Extracted functions 接受明確 paths/DataFrames。
3. Dash layout、callbacks、stores 與 startup 留在 `app.py`。
4. 不建立額外 API/service/state framework。

### 必須保持不變

Layout、callback IDs、charts/tables/filters、port、missing-data 與 cache semantics。

### 可刪除項目

成功抽出後留在 `app.py` 的 duplicate loaders/calculations。

### 風險

Module globals 與 callback order 可能包含 hidden state。

### 驗證方法

Read-model unit tests、app import test、本機 browser smoke test 與重要 chart/table 值比對。

### 回滾策略

每條 loader/callback extraction 單獨 commit。

## Phase 11：簡化 configuration 與 abstractions

### 目標

建立單一設定權威並移除 `commonlib` 類模糊 ownership。

### 受影響檔案／模組

- `config.py`
- `commonlib.py`
- `conf/default.properties`
- active apps 的 CLI/env parsing
- dependency manifests

### 前置條件

- 盤點每個 `commonlib` function/property caller。
- 記錄現有 defaults、data/output roots 與 secret injection。

### 修改前測試

- Environment overrides、default paths、CLI-over-env precedence、container assets 與 legacy properties。

### 精確變更

1. 建立每個 runtime 所需的小型 settings dataclasses/functions。
2. Settings 只在 entrypoint 建立一次並明確傳入。
3. `commonlib` functions 依實際責任移動。
4. 最後一個 caller 移轉後刪除 properties access 與 `conf/`。
5. 建立 runtime dependency matrix。
6. 不導入 DI container、generic repository、Pydantic settings 或新的 generic utils。

### 必須保持不變

Env names/defaults、path overrides、Docker paths、market/analysis defaults，且不得記錄 secrets。

### 可刪除項目

`commonlib.py`、`conf/default.properties`、重複 path bootstrap 與經驗證 unused dependencies。

### 風險

Default 改變可能讓程式讀寫不同資料位置。

### 驗證方法

Config matrix tests、sanitized dry-run、從 repo root/外部執行 CLI、crawler image builds。

### 回滾策略

保留一個 transition phase 的 compatibility adapter；每個 entrypoint 個別回滾。

## Phase 12：分類 research 並刪除剩餘 duplicates

### 目標

將 supported commands 與 experiments 分離，清除確定 obsolete 的 numbered implementations。

### 受影響檔案／模組

- `Analysis_BsReport_v1.py` 至 `v4.py`
- `apps/analysis/run_*`、`build_*`、`make_*`
- research-oriented package modules
- `notebooks/`、`goal/`

### 前置條件

- Human owner/usage inventory。
- Artifact producer mapping。
- Command history/runbook evidence。

### 修改前測試

- 所有預計升級為 production 的 reusable logic tests。
- Import/reference scan。
- Retained research reproduction commands。

### 精確變更

1. 每個 script 分類為 supported、retained research、superseded 或 unknown。
2. Supported reusable logic 移入 `src`。
3. Retained experiments 移入 `research/experiments/<topic>/`。
4. Notebooks 移入 `research/notebooks/`。
5. Numbered experiments 改成描述性名稱。
6. 無 caller/output dependency 後才刪 superseded versions。
7. `goal/*.md` 移到 `docs/research/`。

### 必須保持不變

Supported CLI、retained research reproducibility、production outputs 與 research history。

### 可刪除項目

Confirmed superseded analyses、unused builders、checkpoints 與 redundant notebook data。

### 風險

人工執行不會出現在 import graph。

### 驗證方法

Owner review、reference scan、bounded research commands 與 full tests。

### 回滾策略

Move-only 與 deletion commits 分開；刪除前建立 rollback tag。

## Phase 13：重新整理 folders

### 目標

Ownership 與 dependency 已穩定後，再套用 target folder structure。

### 受影響檔案／模組

- `src/stockanalysis/{runtime/crawlers,analysis}`
- `apps/{etl,analysis,tools}`
- `scripts/`、`tests/`、`assets/`
- `docs/`、`goal/`、`notebooks/`
- `data/` 與 `outputs/` conventions

### 前置條件

- Canonical implementations 已建立。
- Package 不再 import app scripts。
- Registry 完整且所有 old-path consumers 已盤點。

### 修改前測試

Complete suite、所有 supported CLI `--help`、Docker/Cloud Build path、docs links、data-path compatibility。

### 精確變更

每個 move 獨立 commit：

1. `runtime/crawlers` → `crawling`。
2. Stable `analysis` → `signals`。
3. ETL commands → `apps/data`。
4. Portal command → `apps/reporting`。
5. Diagnostics → `apps/diagnostics`。
6. Research → `research/`。
7. Tests → `unit/contracts/integration/smoke`。
8. Operations docs/scripts 分類。
9. Assets 依 consumer 分類。
10. 新 data/output paths 只用於新產物，歷史資料保留 compatibility reads。

### 必須保持不變

Runtime commands、output contracts、deployment resources 與 historical data compatibility。

### 可刪除項目

Transition period 後刪除 import shims 與 path aliases。

### 風險

Folder moves 可能同時破壞 Docker `COPY`、Cloud Build context、shell、`sys.path`、subprocess 與 runbooks。

### 驗證方法

每次 move 後搜尋 old path、跑 tests/CLI checks、build images、local bounded smoke，並靜態驗證 deployment targets。

### 回滾策略

一個 logical move 一個 commit；禁止同時修改 behavior。

## Phase 14：更新文件並防止 architecture drift

### 目標

讓 target architecture 可由 engineers 與 coding agents 長期維持。

### 受影響檔案／模組

- `README.md`
- `docs/codebase/*`
- runtime、architecture、folder blueprints
- runbooks
- `AGENT.md`／`AGENTS.md`
- `pyproject.toml`
- 經確認後的 CI configuration

### 前置條件

- Folder/ownership changes 完成。
- 決定唯一 agent instruction file。
- 確認 repo 外部是否已有 CI。

### 修改前測試

- Documentation links/path scan。
- Registry 與 executable inventory comparison。
- Import-boundary scan。
- 每種 runtime 的 dependency resolution。

### 精確變更

1. 依實際 paths 更新 codebase/runtime/architecture/structure docs。
2. 合併 stale/duplicated structure docs。
3. 文件化 supported entrypoints、data contracts、ownership 與 deprecation policy。
4. 新增輕量結構檢查：
   - `src` 不 import `apps`、`deployment`、`research`。
   - production modules 不使用 `new`、numbered version 或模糊 `test.py`。
   - generated artifacts 不得 tracked。
   - deployable entrypoints 必須存在 registry。
   - Docker/Cloud Build referenced paths 必須存在。
5. 只加入團隊同意的最小 lint/test CI，不做全 repo style rewrite。

### 必須保持不變

所有 runtime behavior。

### 可刪除項目

Stale runbooks、duplicate structure docs、obsolete compatibility notes，以及確認後多餘的 agent instruction file。

### 風險

過嚴的規則可能阻擋合法 research 或 deployment packaging。

### 驗證方法

先讓 enforcement rules 以 report-only 執行，再逐條啟用。

### 回滾策略

每條 rule 獨立 commit；只回滾造成 false positive 的規則。

## 六、最終驗證

```bash
uv lock --check
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p 'test_*.py'
rg -n '^(from|import) (apps|deployment|research)' src/stockanalysis
git ls-files | rg '(__pycache__|\.pyc$|\.DS_Store$|\.ipynb_checkpoints/)'
git diff --check
```

此外必須完成：

1. Build TWSE/TPEX images。
2. 驗證 Dockerfile → entrypoint → wrapper → canonical module。
3. TPEX token-only → one-symbol → small sequential batch。
4. Bounded TWSE validation。
5. ETL temporary-copy integration，確認 partial folder 不會 success/archive。
6. Refresh 三個 portal prerequisites，驗證 freshness 後重建 `trigger_days_gt5_case_review/index.html`。
7. 以代表性 artifacts 啟動 Dash 並檢查主要畫面。
8. 部署前重新檢查 GCP account/project/region。
9. 每次只部署一個 component，保留 previous revision/image digest。
10. 確認所有 supported paths 不再參考 deleted/compatibility-only modules。

## 七、全域回滾規則

- 每個 phase 使用獨立、focused commits。
- 不將 behavior fix、module move 與 deletion 混在同一 commit。
- Mixed worktree 只 stage exact paths，禁止 `git add -A`。
- Material deletion 或 cloud deployment 前建立 rollback point。
- Old entrypoints 保留為 compatibility wrappers，直到 equivalence 驗證通過。
- Deployment 期間保留 previous Cloud Run revision 與 image digest。
- Source move 不得順便搬移或刪除 historical data。
- Static tests 與 live validation 不一致時，立即回滾該 phase，並將缺少的測試補回 Phase 1。

## 八、開始條件

本文件不代表已授權實作。開始任何 production change 前，必須由使用者明確確認要執行的 phase。預設起點為 Phase 1：建立行為安全基線。
