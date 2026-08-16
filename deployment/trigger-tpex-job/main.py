import logging
import os

import functions_framework
import requests
from flask import Request
from google.cloud import firestore, run_v2
from google.cloud.run_v2.types import RunJobRequest

from stockanalysis.contracts.crawl_jobs import PayloadValidationError
from stockanalysis.workflows.cloud_dispatch import TriggerConfig, trigger_crawl_job


LOCATION = os.getenv("LOCATION", "asia-east1")
JOB_NAME = os.getenv("JOB_NAME", "tpex-crawler")
MAX_BATCH_SIZE = int(os.getenv("MAX_BATCH_SIZE", "10"))

_firestore_client = None
_run_client = None
_project_id = os.getenv("GOOGLE_CLOUD_PROJECT")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def get_project_id():
    global _project_id
    if _project_id:
        return _project_id
    response = requests.get(
        "http://metadata.google.internal/computeMetadata/v1/project/project-id",
        headers={"Metadata-Flavor": "Google"},
        timeout=2,
    )
    _project_id = response.text
    return _project_id


def get_clients():
    global _firestore_client, _run_client
    if _firestore_client is None:
        _firestore_client = firestore.Client()
    if _run_client is None:
        _run_client = run_v2.JobsClient()
    return _firestore_client, _run_client


@functions_framework.http
def trigger_run_job(request: Request):
    if not request.is_json:
        return "Invalid content type, expected application/json", 400
    data = request.get_json(silent=True)
    if not data:
        return "Malformed JSON", 400

    try:
        firestore_client, run_client = get_clients()
        result = trigger_crawl_job(
            data,
            config=TriggerConfig(
                market="tpex",
                project_id=get_project_id(),
                location=LOCATION,
                job_name=JOB_NAME,
                max_batch_size=MAX_BATCH_SIZE,
            ),
            firestore_client=firestore_client,
            run_client=run_client,
            run_job_request_type=RunJobRequest,
        )
        if not result.symbols:
            logging.warning(
                "No runnable symbols remain for %s after Firestore filtering.",
                result.collection_name,
            )
            return f"No runnable symbols for {result.collection_name}", 200
        symbols = list(result.symbols)
        return (
            f"Job triggered for symbols: {symbols}. Operation ID: {result.operation_id}",
            200,
        )
    except PayloadValidationError as exc:
        logging.warning("Invalid crawl job payload: %s", exc)
        return str(exc), 400
    except Exception as exc:
        logging.exception("Failed to trigger TPEX crawl job")
        return f"Error: {exc}", 500
