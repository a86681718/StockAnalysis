#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

PROJECT_ID="${PROJECT_ID:-}"
REGION="${REGION:-asia-east1}"
REPO_NAME="${REPO_NAME:-my-repo}"
IMAGE_TAG="${IMAGE_TAG:-$(date +%Y%m%d)}"
BUCKET_NAME="${BUCKET_NAME:-stock-crawler-bucket-$(date +%Y%m%d)}"
RUNTIME_SERVICE_ACCOUNT="${RUNTIME_SERVICE_ACCOUNT:-}"
INVOKER_SERVICE_ACCOUNT="${INVOKER_SERVICE_ACCOUNT:-}"
TWSE_BATCH_SIZE="${TWSE_BATCH_SIZE:-3500}"
TPEX_BATCH_SIZE="${TPEX_BATCH_SIZE:-10}"

usage() {
  cat <<'EOF'
Usage:
  scripts/deploy_gcp_environment.sh [bootstrap|deploy|all]

Environment variables:
  PROJECT_ID                 Required. Target GCP project id.
  REGION                     Optional. Default: asia-east1
  REPO_NAME                  Optional. Default: my-repo
  IMAGE_TAG                  Optional. Default: current YYYYMMDD
  BUCKET_NAME                Optional. Default: stock-crawler-bucket-YYYYMMDD
  RUNTIME_SERVICE_ACCOUNT    Optional. Default: <PROJECT_NUMBER>-compute@developer.gserviceaccount.com
  INVOKER_SERVICE_ACCOUNT    Optional. Default: cloud-run@<PROJECT_ID>.iam.gserviceaccount.com
  TWSE_BATCH_SIZE            Optional. Default: 3500
  TPEX_BATCH_SIZE            Optional. Default: 10
EOF
}

log() {
  printf '\n[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

require_project() {
  if [[ -z "${PROJECT_ID}" ]]; then
    echo "PROJECT_ID is required." >&2
    usage
    exit 1
  fi
}

configure_context() {
  require_project
  gcloud config set project "${PROJECT_ID}" >/dev/null
  PROJECT_NUMBER="$(gcloud projects describe "${PROJECT_ID}" --format='value(projectNumber)')"
  export PROJECT_NUMBER

  if [[ -z "${RUNTIME_SERVICE_ACCOUNT}" ]]; then
    RUNTIME_SERVICE_ACCOUNT="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
  fi
  if [[ -z "${INVOKER_SERVICE_ACCOUNT}" ]]; then
    INVOKER_SERVICE_ACCOUNT="cloud-run@${PROJECT_ID}.iam.gserviceaccount.com"
  fi
  export RUNTIME_SERVICE_ACCOUNT INVOKER_SERVICE_ACCOUNT

  TWSE_IMAGE_URI="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/twse-crawler:${IMAGE_TAG}"
  TPEX_IMAGE_URI="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/tpex-crawler:${IMAGE_TAG}"
  export TWSE_IMAGE_URI TPEX_IMAGE_URI
}

enable_apis() {
  log "Enable required Google APIs"
  gcloud services enable \
    artifactregistry.googleapis.com \
    cloudbuild.googleapis.com \
    cloudscheduler.googleapis.com \
    cloudtasks.googleapis.com \
    firestore.googleapis.com \
    run.googleapis.com
}

ensure_service_account() {
  local account_id="$1"
  local display_name="$2"

  if gcloud iam service-accounts describe "${account_id}@${PROJECT_ID}.iam.gserviceaccount.com" >/dev/null 2>&1; then
    log "Service account ${account_id} already exists"
    return
  fi

  log "Create service account ${account_id}"
  gcloud iam service-accounts create "${account_id}" \
    --display-name="${display_name}"
}

grant_project_role() {
  local member="$1"
  local role="$2"

  gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="${member}" \
    --role="${role}" \
    --quiet >/dev/null
}

grant_service_account_role() {
  local service_account_email="$1"
  local member="$2"
  local role="$3"

  gcloud iam service-accounts add-iam-policy-binding "${service_account_email}" \
    --member="${member}" \
    --role="${role}" \
    --quiet >/dev/null
}

ensure_artifact_repo() {
  if gcloud artifacts repositories describe "${REPO_NAME}" --location="${REGION}" >/dev/null 2>&1; then
    log "Artifact Registry repo ${REPO_NAME} already exists"
    return
  fi

  log "Create Artifact Registry repo ${REPO_NAME}"
  gcloud artifacts repositories create "${REPO_NAME}" \
    --repository-format=docker \
    --location="${REGION}" \
    --description="Docker repository for StockAnalysis"
}

ensure_bucket() {
  if gcloud storage buckets describe "gs://${BUCKET_NAME}" >/dev/null 2>&1; then
    log "Bucket ${BUCKET_NAME} already exists"
    return
  fi

  log "Create bucket ${BUCKET_NAME}"
  gcloud storage buckets create "gs://${BUCKET_NAME}" --location="${REGION}"
}

ensure_firestore() {
  if gcloud firestore databases describe --database='(default)' >/dev/null 2>&1; then
    log "Firestore database already exists"
    return
  fi

  log "Create Firestore database"
  gcloud firestore databases create \
    --location="${REGION}" \
    --type=firestore-native \
    --quiet
}

upsert_queue() {
  local queue_name="$1"
  local max_attempts="$2"

  if gcloud tasks queues describe "${queue_name}" --location="${REGION}" >/dev/null 2>&1; then
    log "Update Cloud Tasks queue ${queue_name}"
    gcloud tasks queues update "${queue_name}" \
      --location="${REGION}" \
      --max-dispatches-per-second=3 \
      --max-concurrent-dispatches=10 \
      --max-attempts="${max_attempts}" \
      --log-sampling-ratio=1.0
    return
  fi

  log "Create Cloud Tasks queue ${queue_name}"
  gcloud tasks queues create "${queue_name}" \
    --location="${REGION}" \
    --max-dispatches-per-second=3 \
    --max-concurrent-dispatches=10 \
    --max-attempts="${max_attempts}" \
    --log-sampling-ratio=1.0
}

configure_iam() {
  log "Ensure service accounts"
  ensure_service_account "cloud-run" "Invoker for Cloud Tasks and Cloud Scheduler"

  log "Grant IAM roles"
  grant_project_role "serviceAccount:${RUNTIME_SERVICE_ACCOUNT}" "roles/storage.objectAdmin"
  grant_project_role "serviceAccount:${RUNTIME_SERVICE_ACCOUNT}" "roles/datastore.user"
  grant_project_role "serviceAccount:${RUNTIME_SERVICE_ACCOUNT}" "roles/cloudtasks.enqueuer"
  grant_project_role "serviceAccount:${RUNTIME_SERVICE_ACCOUNT}" "roles/run.developer"
  grant_project_role "serviceAccount:${RUNTIME_SERVICE_ACCOUNT}" "roles/artifactregistry.writer"
  grant_project_role "serviceAccount:${RUNTIME_SERVICE_ACCOUNT}" "roles/logging.logWriter"
  grant_project_role "serviceAccount:${INVOKER_SERVICE_ACCOUNT}" "roles/run.invoker"
  grant_project_role "serviceAccount:${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com" "roles/artifactregistry.writer"
  grant_service_account_role "${INVOKER_SERVICE_ACCOUNT}" "serviceAccount:${RUNTIME_SERVICE_ACCOUNT}" "roles/iam.serviceAccountUser"
}

build_images() {
  log "Build TWSE crawler image ${TWSE_IMAGE_URI}"
  gcloud builds submit \
    --config "${ROOT_DIR}/apps/twse/cloudbuild.yaml" \
    --substitutions="_IMAGE_URI=${TWSE_IMAGE_URI}" \
    "${ROOT_DIR}"

  log "Build TPEX crawler image ${TPEX_IMAGE_URI}"
  gcloud builds submit \
    --config "${ROOT_DIR}/apps/tpex/cloudbuild.yaml" \
    --substitutions="_IMAGE_URI=${TPEX_IMAGE_URI}" \
    "${ROOT_DIR}"
}

deploy_jobs() {
  log "Deploy Cloud Run job twse-crawler"
  gcloud run jobs deploy twse-crawler \
    --image="${TWSE_IMAGE_URI}" \
    --region="${REGION}" \
    --tasks=1 \
    --parallelism=1 \
    --memory=6Gi \
    --cpu=2 \
    --max-retries=1 \
    --task-timeout=36000s \
    --service-account="${RUNTIME_SERVICE_ACCOUNT}" \
    --set-env-vars="STOCK_CRAWLER_BUCKET=${BUCKET_NAME}"

  log "Deploy Cloud Run job tpex-crawler"
  gcloud run jobs deploy tpex-crawler \
    --image="${TPEX_IMAGE_URI}" \
    --region="${REGION}" \
    --tasks=1 \
    --memory=1Gi \
    --cpu=2 \
    --max-retries=3 \
    --task-timeout=3600s \
    --service-account="${RUNTIME_SERVICE_ACCOUNT}" \
    --set-env-vars="STOCK_CRAWLER_BUCKET=${BUCKET_NAME}"
}

deploy_services() {
  log "Deploy Cloud Run service prepare-twse-list"
  gcloud run deploy prepare-twse-list \
    --source "${ROOT_DIR}/deployment/prepare-twse-list" \
    --region "${REGION}" \
    --service-account "${RUNTIME_SERVICE_ACCOUNT}" \
    --cpu 2 \
    --memory 8Gi \
    --concurrency 1 \
    --timeout 3600 \
    --min-instances 0 \
    --max-instances 5 \
    --set-env-vars "QUEUE_NAME=twse-crawl-queue,FUNCTION_URL=https://trigger-twse-job-${PROJECT_NUMBER}.${REGION}.run.app,LOCATION=${REGION}" \
    --ingress all \
    --cpu-boost \
    --quiet

  log "Deploy Cloud Run service prepare-tpex-list"
  gcloud run deploy prepare-tpex-list \
    --source "${ROOT_DIR}/deployment/prepare-tpex-list" \
    --region "${REGION}" \
    --service-account "${RUNTIME_SERVICE_ACCOUNT}" \
    --cpu 1 \
    --memory 512Mi \
    --concurrency 80 \
    --timeout 3600 \
    --min-instances 1 \
    --max-instances 10 \
    --set-env-vars "QUEUE_NAME=tpex-crawl-queue,FUNCTION_URL=https://trigger-tpex-job-${PROJECT_NUMBER}.${REGION}.run.app,LOCATION=${REGION}" \
    --ingress all \
    --cpu-boost \
    --quiet

  log "Deploy Cloud Run service trigger-twse-job"
  gcloud run deploy trigger-twse-job \
    --source "${ROOT_DIR}/deployment/trigger-twse-job" \
    --region "${REGION}" \
    --service-account "${RUNTIME_SERVICE_ACCOUNT}" \
    --cpu 1 \
    --memory 512Mi \
    --concurrency 80 \
    --timeout 300 \
    --min-instances 0 \
    --max-instances 20 \
    --set-env-vars "JOB_NAME=twse-crawler,LOCATION=${REGION}" \
    --ingress all \
    --cpu-boost \
    --quiet

  log "Deploy Cloud Run service trigger-tpex-job"
  gcloud run deploy trigger-tpex-job \
    --source "${ROOT_DIR}/deployment/trigger-tpex-job" \
    --region "${REGION}" \
    --service-account "${RUNTIME_SERVICE_ACCOUNT}" \
    --cpu 1 \
    --memory 512Mi \
    --concurrency 50 \
    --timeout 300 \
    --min-instances 0 \
    --max-instances 10 \
    --set-env-vars "JOB_NAME=tpex-crawler,LOCATION=${REGION}" \
    --ingress all \
    --cpu-boost \
    --quiet
}

upsert_scheduler_job() {
  local job_name="$1"
  local schedule="$2"
  local uri="$3"
  local body="$4"

  if gcloud scheduler jobs describe "${job_name}" --location="${REGION}" >/dev/null 2>&1; then
    log "Update scheduler job ${job_name}"
    gcloud scheduler jobs update http "${job_name}" \
      --location="${REGION}" \
      --schedule="${schedule}" \
      --time-zone="Asia/Taipei" \
      --uri="${uri}" \
      --http-method=POST \
      --headers="Content-Type=application/json,User-Agent=Google-Cloud-Scheduler" \
      --message-body="${body}" \
      --oidc-service-account-email="${INVOKER_SERVICE_ACCOUNT}" \
      --oidc-token-audience="${uri}" \
      --attempt-deadline="15m" \
      --quiet
    return
  fi

  log "Create scheduler job ${job_name}"
  gcloud scheduler jobs create http "${job_name}" \
    --location="${REGION}" \
    --schedule="${schedule}" \
    --time-zone="Asia/Taipei" \
    --uri="${uri}" \
    --http-method=POST \
    --headers="Content-Type=application/json,User-Agent=Google-Cloud-Scheduler" \
    --message-body="${body}" \
    --oidc-service-account-email="${INVOKER_SERVICE_ACCOUNT}" \
    --oidc-token-audience="${uri}" \
    --attempt-deadline="15m" \
    --quiet
}

deploy_schedulers() {
  upsert_scheduler_job \
    "prepare-tpex-crawler" \
    "0 17 * * 1-5" \
    "https://prepare-tpex-list-${PROJECT_NUMBER}.${REGION}.run.app/" \
    "{\"batch_size\": ${TPEX_BATCH_SIZE}}"

  upsert_scheduler_job \
    "prepare-twse-crawler" \
    "0 18 * * 1-5" \
    "https://prepare-twse-list-${PROJECT_NUMBER}.${REGION}.run.app/" \
    "{\"batch_size\": ${TWSE_BATCH_SIZE}}"
}

bootstrap() {
  configure_context
  enable_apis
  ensure_artifact_repo
  ensure_bucket
  ensure_firestore
  configure_iam
  upsert_queue "twse-crawl-queue" "5"
  upsert_queue "tpex-crawl-queue" "1"
}

deploy() {
  configure_context
  build_images
  deploy_jobs
  deploy_services
  deploy_schedulers
}

main() {
  local action="${1:-all}"
  case "${action}" in
    bootstrap)
      bootstrap
      ;;
    deploy)
      deploy
      ;;
    all)
      bootstrap
      deploy
      ;;
    -h|--help|help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown action: ${action}" >&2
      usage
      exit 1
      ;;
  esac

  log "Done"
  log "PROJECT_ID=${PROJECT_ID}"
  log "REGION=${REGION}"
  log "BUCKET_NAME=${BUCKET_NAME}"
  log "IMAGE_TAG=${IMAGE_TAG}"
}

main "$@"
