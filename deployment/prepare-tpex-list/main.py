import logging
import os
import time

import pandas as pd
import requests
from flask import Request
from google.cloud import firestore, tasks_v2
from google.protobuf import duration_pb2, timestamp_pb2

from stockanalysis.contracts.crawl_jobs import PayloadValidationError, STATUS_PENDING
from stockanalysis.workflows.cloud_dispatch import (
    PrepareConfig,
    create_crawl_task,
    prepare_crawl_tasks,
)


QUEUE_NAME = os.environ.get("QUEUE_NAME")
LOCATION = os.environ.get("LOCATION", "asia-east1")
FUNCTION_URL = os.environ.get("FUNCTION_URL")
_project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
_firestore_client = None
_tasks_client = None

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
    global _firestore_client, _tasks_client
    if _firestore_client is None:
        _firestore_client = firestore.Client()
    if _tasks_client is None:
        _tasks_client = tasks_v2.CloudTasksClient()
    return _firestore_client, _tasks_client


def prepare_config() -> PrepareConfig:
    return PrepareConfig(
        market="tpex",
        project_id=get_project_id(),
        location=LOCATION,
        queue_name=QUEUE_NAME,
        function_url=FUNCTION_URL,
        default_batch_size=5,
        maximum_batch_size=10,
        initialization_status=STATUS_PENDING,
    )


def parse_table(obj):
    if not obj or "tables" not in obj:
        return None
    table = None
    for candidate in obj["tables"]:
        if candidate:
            table = candidate
    if table:
        return pd.DataFrame(
            table["data"],
            columns=[field.strip() for field in table["fields"]],
        )
    return None


def fetch_json(url, payload, retries=3, delay=2):
    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
    }
    for attempt in range(1, retries + 1):
        try:
            response = requests.post(
                url, headers=headers, data=payload, timeout=10, verify=False
            )
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            logging.warning("[Attempt %s] Request failed: %s", attempt, exc)
            if attempt < retries:
                time.sleep(delay)
    return None


def get_stock_list(url, data_date):
    frame = parse_table(
        fetch_json(
            url,
            {"date": data_date, "type": "EW", "id": "", "response": "json"},
        )
    )
    if frame is None:
        return []
    frame = frame[frame["成交筆數"] != 0]
    return frame[frame["代號"].str.len() == 4]["代號"].to_list()


def get_warrant_list(url, data_date):
    frame = parse_table(
        fetch_json(
            url,
            {"date": data_date, "type": "WW", "id": "", "response": "json"},
        )
    )
    if frame is None:
        return []
    frame["成交股數"] = pd.to_numeric(
        frame["成交股數"].str.replace(",", ""), errors="coerce"
    )
    frame["成交筆數"] = pd.to_numeric(
        frame["成交筆數"].str.replace(",", ""), errors="coerce"
    )
    frame = frame[frame["成交筆數"] != 0].sort_values("成交股數", ascending=False)
    return frame["代號"].to_list()


def fetch_symbols(data_date: str) -> list[str]:
    url = "https://www.tpex.org.tw/www/zh-tw/afterTrading/otc"
    return get_stock_list(url, data_date) + get_warrant_list(url, data_date)


def create_task(
    symbols: list,
    data_date: str,
    run_id: str | None = None,
    image_revision: str | None = None,
):
    _, tasks_client = get_clients()
    create_crawl_task(
        symbols,
        data_date,
        run_id=run_id,
        image_revision=image_revision,
        config=prepare_config(),
        tasks_client=tasks_client,
        http_method_post=tasks_v2.HttpMethod.POST,
        duration_type=duration_pb2.Duration,
        timestamp_type=timestamp_pb2.Timestamp,
    )


def main(request: Request):
    data = request.get_json(silent=True) or {}
    try:
        firestore_client, tasks_client = get_clients()
        prepare_crawl_tasks(
            data,
            config=prepare_config(),
            firestore_client=firestore_client,
            tasks_client=tasks_client,
            fetch_symbols=fetch_symbols,
            server_timestamp=firestore.SERVER_TIMESTAMP,
            http_method_post=tasks_v2.HttpMethod.POST,
            duration_type=duration_pb2.Duration,
            timestamp_type=timestamp_pb2.Timestamp,
        )
        return "Tasks created"
    except PayloadValidationError as exc:
        return str(exc), 400


if __name__ == "__main__":
    main()
