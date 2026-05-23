# StockAnalysis

Taiwan stock market data collection, ETL, analysis, and visualization.

這個專案目前同時包含 4 類內容：

- `crawler`: 從 TWSE / TPEX 與其他來源抓資料
- `etl`: 將原始資料整理成 parquet 或分析可用格式
- `analysis`: 針對券商籌碼、權證、事件偵測與回測做研究
- `visualization / deployment`: 提供 Dash 視覺化與雲端排程腳本

README 的目標不是完整列出每支腳本，而是讓你先在 3 分鐘內知道：

- 哪些資料夾是核心
- 哪些內容偏正式腳本，哪些偏研究或實驗
- 從哪裡開始看
- 主要資料流怎麼走

## Quick Map

```text
StockAnalysis/
├── src/stockanalysis/      # 共用 library、路徑設定、分析模組、runtime crawler 實作
├── apps/
│   ├── crawlers/           # 通用 crawler 腳本
│   ├── twse/               # TWSE wrapper、container 與 cloudbuild 設定
│   ├── tpex/               # TPEX wrapper、container 與 cloudbuild 設定
│   ├── etl/                # ETL 腳本
│   ├── analysis/           # 研究型分析與回測腳本
│   ├── visualization/      # Dash app
│   └── services/           # 其他本地輔助服務
├── deployment/             # 正式 Cloud Run 部署入口
├── docs/                   # 架構與維護文件
├── legacy/                 # 已退出主流程但保留的舊服務/舊結構
├── data/                   # 相容性預設資料區，正式使用建議改由環境變數指到 repo 外
├── outputs/                # 相容性預設輸出區
├── notebooks/              # 探索式 notebook
├── conf/                   # 路徑與環境設定
├── assets/                 # 模型與靜態資產
└── scripts/                # 本地輔助 shell scripts
```

## How To Read This Repo

如果你是第一次接手，建議閱讀順序：

1. `pyproject.toml`
   先看 Python 版本、依賴與 workspace 設定。
2. `src/stockanalysis/config.py`
   先理解專案怎麼找 `data/`、`outputs/`、`assets/`。
3. `deployment/`
   這是目前正式 GCP 流程的入口。
4. `docs/project-structure.md`
   先看目前建議的主幹結構與 data path policy。
5. `apps/visualization/app.py`
   這支可以快速看出目前分析結果最後怎麼被使用。
6. `apps/analysis/` 與 `src/stockanalysis/analysis/`
   前者偏研究腳本與批次執行，後者偏可重用分析模組。

## Directory Guide

### `src/stockanalysis/`

共用 Python package。

- `config.py`: 統一路徑解析，支援 `STOCKANALYSIS_ROOT` / `STOCKANALYSIS_DATA_DIR` 等環境變數
- `commonlib.py`: 舊有共用函式，例如設定讀取、日期轉換、影像輸出
- `analysis/`: 較模組化的分析腳本，例如高集中度掃描、近期券商買超摘要、異常券商偵測

這層是最接近「可重用核心」的位置。

### `apps/crawlers/`

較通用的資料抓取腳本，像是：

- 股票清單
- 券商清單
- 權證清單
- 每日交易資料
- 法說會 / 股利 / 三大法人資料

如果要補資料來源，通常先從這層開始看。

### `apps/twse/` 與 `apps/tpex/`

偏向市場別切分的 crawler wrapper 與 build 包。

- `crawler-*-bsreport.py`: 券商買賣超明細
- `crawler-*-daily-ohlc.py`: 日 OHLC
- `Dockerfile`, `cloudbuild.yaml`, `requirements.txt`: 容器化與雲端建置

目前正式 GCP service 入口不在這裡，而是在 `deployment/`。

### `apps/etl/`

將原始資料轉成分析可用格式，例如 parquet 或壓縮整理。

目前內容看起來還混有實驗檔與舊腳本，但責任很明確：資料整理層。

### `apps/analysis/`

偏研究導向、批次分析與回測。

這裡目前聚集了幾類工作：

- `Analysis_BsReport_v*.py`: 籌碼 / 券商資料分析演進版本
- `run_ml_*.py`: ML / 預測 / 搜參 /報表生成
- `run_backtest_stage1.py`, `run_tune_stage1_v3.py`: 事件偵測與回測
- `make_event_report.py`, `make_ml_predictions_report.py`: 輸出報表

這層不是低耦合 library，而是「可以直接跑的研究腳本集合」。
目前設定檔已集中到 `apps/analysis/configs/`，報表生成腳本實體則在 `apps/analysis/reports/`。

### `apps/visualization/`

Dash 視覺化入口。

目前會直接讀：

- `data/_derived/ohlc.parquet`
- `data/bs_report/parquet_twse/*.parquet`
- `data/bs_report/parquet_tpex/*.parquet`
- `data/_derived/scored.parquet`

所以這支程式也順便告訴你分析階段預期會產出哪些中介資料。

### `deployment/`

正式 Cloud Run 部署入口，對應準備任務與觸發任務的雲端執行。

如果要調整 Cloud Run / Scheduler 相關流程，從這裡看比直接翻 `commands` 更清楚。

### `legacy/`

已不再是主流程入口、但暫時保留的舊服務結構。

這一層的目的是把歷史殘留從主幹挪開，不讓正式結構繼續膨脹。

### `notebooks/`

探索式工作區。

特徵：

- 主題很多
- 歷史脈絡強
- 適合找想法，不適合當穩定入口

可以把它視為研究筆記，不是專案主幹。

## Data Layout

目前資料目錄大致可分成：

- `data/ohlc/`: 每日市場價格 CSV
- `data/bs_report/`: 券商買賣超明細，含 parquet 子目錄
- `data/warrant/`: 權證相關資料
- `data/_derived/`: 分析中介產物與回測結果
- `outputs/`: 圖片、HTML 報表、暫存輸出

一個典型流程是：

```text
crawler -> raw csv / parquet -> ETL -> data/_derived -> analysis -> outputs / dashboard
```

## Main Entry Points

如果只想快速上手，先看這些：

- `apps/twse/crawler-twse-bsreport-new.py`
- `apps/twse/crawler-twse-daily-ohlc.py`
- `apps/tpex/crawler-tpex-bsreport.py`
- `apps/tpex/crawler-tpex-daily-ohlc.py`
- `apps/analysis/Analysis_BsReport_v4.py`
- `src/stockanalysis/analysis/high_concentration_scan.py`
- `src/stockanalysis/analysis/recent_broker_top5.py`
- `apps/visualization/app.py`

## Environment

專案根目錄使用 `pyproject.toml`。

關鍵條件：

- Python: `>=3.11,<3.12`
- 套件管理可走 `uv` 或 Poetry 相容路線
- `apps/etl/` 另有自己的 `pyproject.toml`

最基本安裝概念：

```bash
uv sync
```

必要時可用環境變數覆蓋：

- `STOCKANALYSIS_ROOT`
- `STOCKANALYSIS_DATA_DIR`
- `STOCKANALYSIS_OUTPUT_DIR`
- `STOCKANALYSIS_ASSETS_DIR`
- `STOCKANALYSIS_CONF_DIR`

正式使用建議把 `data/` 與 `outputs/` 放在 repo 之外，例如：

```bash
export STOCKANALYSIS_DATA_DIR=/path/to/stockanalysis-data
export STOCKANALYSIS_OUTPUT_DIR=/path/to/stockanalysis-outputs
```

## Current Structure Assessment

目前結構整體上是合理的，但有幾個明顯特徵需要記住：

- `deployment/` 是正式 GCP 主流程入口
- `src/stockanalysis/` 是較穩定的共用層與 runtime 實作層
- `apps/twse`、`apps/tpex` 已收斂為 wrapper + build 設定
- `notebooks/` 與 `apps/analysis/` 都承載研究歷史，命名與成熟度不完全一致
- `data/`、`outputs/` 仍保留相容性預設值，但長期建議移到 repo 外

換句話說，這個專案比較像「研究與生產並存的資料工作台」，不是純乾淨封裝的 library。

## Recommended Operating Convention

如果之後要繼續整理，建議遵守這個邊界：

- 新的共用邏輯放 `src/stockanalysis/`
- 新的 GCP service 放 `deployment/`
- 新的 crawler 實作放 `src/stockanalysis/runtime/crawlers/`
- `apps/twse`、`apps/tpex` 只保留 wrapper 與 build 相關檔案
- 臨時探索放 `notebooks/`
- 中介資料放 `data/_derived/`
- 對外輸出放 `outputs/`

這樣可以在不大改現況的前提下，讓 repo 持續維持可讀性。

## Notes

- `commands` 目前比較像操作備忘錄，不是正式文件
- `assets/` 內含模型與圖像資產，部分 crawler 或工具會依賴它
- `conf/default.properties` 仍被舊腳本使用

## Summary

這個專案最值得先掌握的不是每一支腳本，而是 3 個核心觀念：

1. `apps/` 負責執行任務
2. `deployment/` 負責正式 GCP 流程
3. `src/stockanalysis/` 負責共用能力與可重用 runtime

掌握這三點後，再往特定 crawler、ETL 或分析腳本深入，理解成本會低很多。
