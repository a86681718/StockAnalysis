# GCP Auth Notes

This note records the known-good `gcloud` context for StockAnalysis refresh work on this machine.

## Known-good context

- account: `m86681718@gmail.com`
- project: `project-3b72568d-c2fc-4e6c-89b`
- bs_report bucket: `gs://stock-crawler-bucket-project-3b72568d-c2fc-4e6c-89b/bs_report`

## Use Before Refresh

Set the active account before running the data refresh runbook:

```bash
gcloud config set account m86681718@gmail.com
gcloud config set project project-3b72568d-c2fc-4e6c-89b
```

Verify the active context:

```bash
gcloud auth list
gcloud config list account project
```

## Known Bad Context

- `cassnomad270@gmail.com` previously failed to access the `bs_report` bucket for this project.
- If a refresh fails with GCS permission errors, check the active account first before retrying ETL.
