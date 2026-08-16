# ETL

This directory now contains both the legacy one-off ETL scripts and the formalized bs-report pipeline entry point.

## Main entry point

Use this for the current bs-report local workflow:

```bash
uv run apps/etl/run_bs_report_etl.py --market tpex --sync
uv run apps/etl/run_bs_report_etl.py --market twse --sync
uv run apps/etl/run_bs_report_etl.py --market all --sync
```

When `--sync` is enabled, the script now reads the local manifest first, lists remote GCS date folders, and only copies folders that do not already have `status=success` in the manifest. It no longer performs a full recursive `rsync` of the market prefix.

A dated folder is successful only when every expected CSV is processed. Partial
failures remain in `inbox/`, are recorded in the manifest with failed filenames
and error summaries, are not archived, and make the command exit non-zero.

## Bs-report pipeline layout

The pipeline expects these directories under `data/bs_report/`:

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

## Files

- `run_bs_report_etl.py`
  - orchestration entry point
  - optional manifest-aware GCS sync
  - manifest-based skip logic
  - archive processed folders
- `bs_report_pipeline.py`
  - compatibility imports for `stockanalysis.pipelines.broker_reports`
- `src/stockanalysis/pipelines/broker_reports.py`
  - core ETL logic for per-day folder processing
  - incremental parquet writes with duplicate removal
- `src/stockanalysis/pipelines/ohlc.py`
  - reusable TWSE/TPEX OHLC normalization and combined-frame construction
- `ETL_BsReport.py`
  - legacy tpex-focused ETL script kept for backward compatibility
- `ETL_BsReportZip.py`
  - legacy zip-based variant

## Typical usage

Dry run:

```bash
uv run apps/etl/run_bs_report_etl.py --market tpex --sync --dry-run
```

Process local inbox only:

```bash
uv run apps/etl/run_bs_report_etl.py --market tpex --no-sync
```
