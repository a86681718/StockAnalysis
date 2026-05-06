# NEW ETL Prototype

This folder contains a new bs-report ETL flow prototype that does not modify the existing `apps/etl` scripts.

## Goal

Replace the current manual workflow:

1. `gcloud storage rsync`
2. run ETL for `tpex`
3. edit paths to switch to `twse`
4. run ETL again
5. manually move processed raw folders

With a single market-aware pipeline:

```bash
uv run NEW/run_bs_report_etl.py --market tpex --sync
uv run NEW/run_bs_report_etl.py --market twse --sync
uv run NEW/run_bs_report_etl.py --market all --sync
```

## Layout

The new flow expects these directories under `data/bs_report/`:

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

- `etl_bs_report.py`
  - core ETL logic
  - parameterized input/output paths
  - incremental parquet updates with duplicate removal
- `run_bs_report_etl.py`
  - orchestration entry point
  - optional GCS sync
  - manifest-based skip logic
  - archive processed folders
  - summary output

## Typical usage

Dry run:

```bash
uv run NEW/run_bs_report_etl.py --market tpex --sync --dry-run
```

Process local inbox only:

```bash
uv run NEW/run_bs_report_etl.py --market tpex --no-sync
```

Process both markets:

```bash
uv run NEW/run_bs_report_etl.py --market all --sync
```

## Default GCS layout

The runner assumes this source layout by default:

```text
gs://stock-crawler-bucket-20260302/bs_report/tpex/
gs://stock-crawler-bucket-20260302/bs_report/twse/
```

You can override it with `--gcs-base-uri`.
