# GCP Bootstrap

This repo includes a repeatable bootstrap script for rebuilding the current GCP deployment layout in a new project:

- `scripts/deploy_gcp_environment.sh`

## What it creates

The script is based on:

- the current live project `project-f1177eeb-a19a-4c9a-8bf`
- the `deployment/` services in this repo
- the image builds defined in `apps/twse/cloudbuild.yaml` and `apps/tpex/cloudbuild.yaml`

It provisions:

- required Google APIs
- Artifact Registry repo `my-repo`
- GCS bucket for crawler output
- Firestore native database
- IAM bindings for:
  - runtime service account
  - Cloud Tasks / Scheduler invoker service account
  - Cloud Build writer access to Artifact Registry
- Cloud Tasks queues:
  - `twse-crawl-queue`
  - `tpex-crawl-queue`
- Cloud Run jobs:
  - `twse-crawler`
  - `tpex-crawler`
- Cloud Run services:
  - `prepare-twse-list`
  - `prepare-tpex-list`
  - `trigger-twse-job`
  - `trigger-tpex-job`
- Cloud Scheduler jobs:
  - `prepare-twse-crawler`
  - `prepare-tpex-crawler`

## Usage

Bootstrap and deploy everything:

```bash
PROJECT_ID=my-new-project scripts/deploy_gcp_environment.sh all
```

Bootstrap infra only:

```bash
PROJECT_ID=my-new-project scripts/deploy_gcp_environment.sh bootstrap
```

Deploy apps only:

```bash
PROJECT_ID=my-new-project IMAGE_TAG=20260523 scripts/deploy_gcp_environment.sh deploy
```

Override the bucket name:

```bash
PROJECT_ID=my-new-project \
BUCKET_NAME=stock-crawler-bucket-prod \
scripts/deploy_gcp_environment.sh all
```

## Important variables

- `PROJECT_ID`
  - required
- `REGION`
  - default `asia-east1`
- `REPO_NAME`
  - default `my-repo`
- `IMAGE_TAG`
  - default current `YYYYMMDD`
- `BUCKET_NAME`
  - default `stock-crawler-bucket-YYYYMMDD`
- `RUNTIME_SERVICE_ACCOUNT`
  - default `<PROJECT_NUMBER>-compute@developer.gserviceaccount.com`
- `INVOKER_SERVICE_ACCOUNT`
  - default `cloud-run@<PROJECT_ID>.iam.gserviceaccount.com`
- `TWSE_BATCH_SIZE`
  - default `3500`
- `TPEX_BATCH_SIZE`
  - default `10`

## Notes

- The script is idempotent for the main infra resources:
  - bucket
  - Artifact Registry repo
  - Firestore database
  - Cloud Tasks queues
  - Cloud Scheduler jobs
- `gcloud run deploy` and `gcloud run jobs deploy` act as upserts, so rerunning the script updates services and jobs in place.
- The runtime service account defaults to the project default compute service account because that matches the current live deployment.
- Cloud Tasks in `prepare-*` code mint OIDC tokens as `cloud-run@<PROJECT_ID>.iam.gserviceaccount.com`, so the script explicitly creates and grants that service account.
