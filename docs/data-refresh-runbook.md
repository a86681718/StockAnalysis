# Data Refresh Runbook

This file records the current local execution flow for the official data-refresh steps in this repo:

1. OHLC CSV
2. derived OHLC parquet
3. warrant dedup
4. bs_report sync + ETL

Use this as the default reference when you want to refresh local data yourself or when an agent needs to do it again later.

## Environment

Run from repo root:

```bash
cd /Users/ttfang/Documents/workspace/StockAnalysis
```

Use the project virtualenv:

```bash
.venv/bin/python ...
```

Path resolution follows `src/stockanalysis/config.py`:

- data root: `data/`
- output root: `outputs/`

## 1. OHLC

### Purpose

Refresh daily TWSE and TPEX OHLC CSV files under:

- `data/ohlc/twse-YYYYMMDD.csv`
- `data/ohlc/tpex-YYYYMMDD.csv`

### Entrypoints

- `apps/twse/crawler-twse-daily-ohlc.py`
- `apps/tpex/crawler-tpex-daily-ohlc.py`

### Normal execution

```bash
.venv/bin/python apps/twse/crawler-twse-daily-ohlc.py
.venv/bin/python apps/tpex/crawler-tpex-daily-ohlc.py
```

Behavior:

- each crawler looks at the latest local file date
- starts from the next date
- skips dates where the source returns empty data
- writes one CSV per day

### Optional execution from a fixed date

```bash
.venv/bin/python apps/twse/crawler-twse-daily-ohlc.py --start-date 2026/05/27
.venv/bin/python apps/tpex/crawler-tpex-daily-ohlc.py --start-date 2026/05/27
```

### Verify latest date

```bash
find data/ohlc -maxdepth 1 -type f -name 'twse-*.csv' | sed 's#data/ohlc/##' | sort | tail -n 5
find data/ohlc -maxdepth 1 -type f -name 'tpex-*.csv' | sed 's#data/ohlc/##' | sort | tail -n 5
```

## 2. Derived OHLC Parquet

### Purpose

Rebuild the normalized OHLC parquet used by the visualization app and strategy analysis:

- `data/_derived/ohlc.parquet`

The daily OHLC crawlers only write raw CSV files. Always rebuild this parquet after refreshing OHLC CSVs.

### Entrypoint

- `apps/etl/build_ohlc_parquet.py`

### Normal execution

```bash
.venv/bin/python apps/etl/build_ohlc_parquet.py
```

### Verify output

```bash
.venv/bin/python - <<'PY'
import pandas as pd
df = pd.read_parquet('data/_derived/ohlc.parquet', columns=['symbol', 'date'])
print('rows', len(df))
print('symbols', df['symbol'].nunique())
print('date_max', df['date'].max())
PY
```

## 3. Warrant Dedup

### Purpose

Refresh the deduplicated warrant list:

- `data/warrant/warrant_list_dedup.csv`

### Entrypoint

- `apps/crawlers/Crawler_WarrantList.py`

### Normal execution

```bash
.venv/bin/python apps/crawlers/Crawler_WarrantList.py
```

Behavior:

- fetches all four combinations:
  - listed / OTC
  - expired / unexpired
- default date range is recent rolling months derived inside the script
- merges with existing `warrant_list_dedup.csv` if present
- deduplicates by:
  - `權證代號`
  - `權證簡稱`

### Optional execution with explicit ROC date range

Example:

```bash
.venv/bin/python apps/crawlers/Crawler_WarrantList.py --start-date 11411 --end-date 11505
```

### Verify output

```bash
stat -f '%Sm %N' data/warrant/warrant_list_dedup.csv

.venv/bin/python - <<'PY'
import pandas as pd
df = pd.read_csv('data/warrant/warrant_list_dedup.csv', dtype=str)
print('rows', len(df))
print('unique_code_name', df[['權證代號', '權證簡稱']].drop_duplicates().shape[0])
PY
```

If `rows == unique_code_name`, dedup is behaving as expected.

## 4. bs_report Sync + ETL

### Purpose

Refresh broker buy/sell source folders from GCS, process them into incremental parquet, update manifests, and archive processed daily folders.

### Entrypoint

- `apps/etl/run_bs_report_etl.py`

### Core ETL

- `apps/etl/bs_report_pipeline.py`

### Data layout

```text
data/bs_report/
├── inbox/
│   ├── twse/
│   └── tpex/
├── archive/
│   ├── twse/
│   └── tpex/
├── parquet_twse/
├── parquet_tpex/
└── manifests/
```

### Important current behavior

`--sync` is now manifest-aware.

It does **not** do a full recursive `gcloud storage rsync` anymore.
Instead it:

1. reads the local manifest
2. lists remote GCS date folders for the target market
3. copies only dates whose manifest status is not `success`
4. runs ETL on the resulting local inbox folders

This is the preferred and safe default.

Default GCS source:

```text
gs://stock-crawler-bucket-project-3b72568d-c2fc-4e6c-89b/bs_report
```

### Normal execution

```bash
.venv/bin/python apps/etl/run_bs_report_etl.py --market all --sync
```

Or one market at a time:

```bash
.venv/bin/python apps/etl/run_bs_report_etl.py --market twse --sync
.venv/bin/python apps/etl/run_bs_report_etl.py --market tpex --sync
```

### Useful variants

Dry run:

```bash
.venv/bin/python apps/etl/run_bs_report_etl.py --market all --sync --dry-run
```

Only process local inbox, skip GCS:

```bash
.venv/bin/python apps/etl/run_bs_report_etl.py --market all --no-sync
```

Only consider dates on or after a threshold:

```bash
.venv/bin/python apps/etl/run_bs_report_etl.py --market all --sync --since 20260525
```

Limit the number of pending folders:

```bash
.venv/bin/python apps/etl/run_bs_report_etl.py --market all --sync --limit 2
```

### Verify manifest state

```bash
python3 - <<'PY'
import json
for p in ['data/bs_report/manifests/twse_processed.json', 'data/bs_report/manifests/tpex_processed.json']:
    with open(p) as f:
        d = json.load(f)
    print(p, len(d), sorted(d)[-5:])
PY
```

### Verify archive movement

```bash
find data/bs_report/archive/twse -maxdepth 1 -mindepth 1 -type d | sort | tail -n 10
find data/bs_report/archive/tpex -maxdepth 1 -mindepth 1 -type d | sort | tail -n 10
```

### Verify parquet samples

```bash
.venv/bin/python - <<'PY'
import pandas as pd
for p in [
    'data/bs_report/parquet_twse/2330.parquet',
    'data/bs_report/parquet_tpex/8069.parquet',
]:
    df = pd.read_parquet(p, columns=['日期'])
    print(p, str(df['日期'].max().date()), len(df))
PY
```

## Recommended Refresh Order

If the goal is a normal local data refresh for analysis:

1. refresh OHLC
2. rebuild `data/_derived/ohlc.parquet`
3. refresh warrant dedup
4. refresh `bs_report`

Commands:

```bash
.venv/bin/python apps/twse/crawler-twse-daily-ohlc.py
.venv/bin/python apps/tpex/crawler-tpex-daily-ohlc.py
.venv/bin/python apps/etl/build_ohlc_parquet.py
.venv/bin/python apps/crawlers/Crawler_WarrantList.py
.venv/bin/python apps/etl/run_bs_report_etl.py --market all --sync
```

## Known Caveats

- These crawlers need network access. In a restricted sandbox they may fail with DNS or connection errors even when the code is fine.
- `bs_report` truth is not represented by one directory alone. Check:
  - `inbox`
  - `archive`
  - `manifests`
  - `parquet_twse`
  - `parquet_tpex`
- Old processed folders may still exist in `inbox/` from prior runs. Manifest-aware sync prevents re-copying already successful dates, but `inbox/` may still need occasional cleanup if you want it visually tidy.
- `data/_derived/ohlc.parquet` is not refreshed by the OHLC crawler itself. It is now a required runbook step immediately after OHLC CSV refresh.

## Current Baseline After Latest Refresh

At the time this runbook was updated:

- OHLC latest local date: `20260529`
- bs_report manifest latest local date:
  - TWSE: `20260529`
  - TPEX: `20260529`
- warrant dedup output file:
  - `data/warrant/warrant_list_dedup.csv`
