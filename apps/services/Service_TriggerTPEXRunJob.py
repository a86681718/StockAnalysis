import os
import logging
import functions_framework
from google.cloud import firestore, run_v2
from google.cloud.run_v2.types import RunJobRequest
from datetime import datetime
from flask import Request

# 參數設定
PROJECT_ID = "bionic-region-455813-b0"
LOCATION = "asia-east1"
JOB_NAME = "tpex-crawler"

firestore_client = firestore.Client()
run_client = run_v2.JobsClient()
symbol = None 

# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

@functions_framework.http
def trigger_run_job(request: Request):
    try:
        logging.debug("===== Trigger Run Job =====")
        logging.debug(f"Timestamp: {datetime.utcnow().isoformat()} UTC")
        logging.debug(f"Headers: {dict(request.headers)}")
        logging.debug(f"Raw body: {request.get_data()}")
        logging.debug(f"Content-Type: {request.content_type}")
        logging.debug(f"is_json: {request.is_json}")

        if not request.is_json:
            logging.warning("Invalid content type, expected JSON")
            return "Invalid content type, expected application/json", 400

        data = request.get_json(silent=True)
        logging.debug(f"Parsed JSON: {data}")

        if not data:
            logging.warning("Malformed or missing JSON payload")
            return "Malformed JSON", 400

        symbols = data.get("symbols")
        dt = data.get("date")
        if not symbols or not isinstance(symbols, list):
            logging.warning("Missing or invalid symbols field in request")
            return "Missing or invalid symbols", 400

        logging.info(f"Received symbol: {symbols}")

        # Firestore 更新狀態為 running
        dt = dt.replace("/", "")
        collection_name = f"crawl_status_{dt}"
        for symbol in symbols:
            doc_ref = firestore_client.collection(collection_name).document(symbol)
            doc_ref.update({"status": "running"})
            logging.info(f"[{symbol}] Updated Firestore status to 'running'")

        # 準備 Cloud Run Job 路徑
        parent = f"projects/{PROJECT_ID}/locations/{LOCATION}"
        job_path = f"{parent}/jobs/{JOB_NAME}"
        logging.debug(f"[{symbols}] Prepared job path: {job_path}")

        # 建立執行請求
        run_request = RunJobRequest(
            name=job_path,
            overrides=RunJobRequest.Overrides(
                container_overrides=[
                    RunJobRequest.Overrides.ContainerOverride(args=[str(symbols), dt.replace('_', '')])
                ]
            ),
        )
        logging.info(f"[{symbols}] Start to run job ")

        # 執行 Job
        response = run_client.run_job(request=run_request)
        result = str(response.result()).replace('\n', '')
        logging.info(f"Run job response: {result}")

        return f"Job triggered for symbols: {symbols}", 200

    except Exception as e:
        logging.error(f"[{symbol}] Exception occurred: {str(e)}", exc_info=True)
        return f"Error: {str(e)}", 500