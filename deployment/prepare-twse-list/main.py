import json
import logging
import os
import time
from datetime import datetime

import pandas as pd
import requests
import urllib3
from flask import Request, jsonify
from google.cloud import firestore, tasks_v2
from google.protobuf import duration_pb2, timestamp_pb2

from stockanalysis.contracts.crawl_jobs import (
    PayloadValidationError,
    STATUS_INITIALIZED,
)
from stockanalysis.workflows.cloud_dispatch import (
    PrepareConfig,
    create_crawl_task,
    prepare_crawl_tasks,
)


class TwseSecurityBlockError(RuntimeError):
    """Raised when TWSE responds with the security block splash page."""


SECURITY_BLOCK_MARKERS = (
    "THE PAGE CANNOT BE ACCESSED",
    "頁面無法執行",
    "因為安全性考量",
)
QUEUE_NAME = os.environ.get("QUEUE_NAME")
LOCATION = os.environ.get("LOCATION", "asia-east1")
FUNCTION_URL = os.environ.get("FUNCTION_URL")
_project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
_firestore_client = None
_tasks_client = None

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
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
        market="twse",
        project_id=get_project_id(),
        location=LOCATION,
        queue_name=QUEUE_NAME,
        function_url=FUNCTION_URL,
        default_batch_size=3500,
        maximum_batch_size=3500,
        initialization_status=STATUS_INITIALIZED,
        exclude_dummy_document=True,
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


def fetch_json(url, retries=3, delay=2):
    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/58.0.3029.110 Safari/537.3"
        ),
    }
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, headers=headers, timeout=600, verify=False)
            response.raise_for_status()
            body = response.content.decode("utf8", errors="ignore")
            if any(marker in body for marker in SECURITY_BLOCK_MARKERS):
                raise TwseSecurityBlockError("TWSE security block page encountered")
            return json.loads(body)
        except Exception as exc:
            logging.warning("[Attempt %s] Request failed: %s", attempt, exc)
            if isinstance(exc, TwseSecurityBlockError):
                raise
            if attempt < retries:
                time.sleep(delay)
    return None


def get_stock_list(data_date, retries=3, delay=2):
    etf_url = (
        "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX"
        f"?date={data_date}&type=0099P&response=json"
    )
    stocks_url = (
        "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX"
        f"?date={data_date}&type=ALLBUT0999&response=json"
    )
    for attempt in range(1, retries + 1):
        try:
            etf_frame = parse_table(fetch_json(etf_url))
            stocks_frame = parse_table(fetch_json(stocks_url))
            if etf_frame is None or stocks_frame is None:
                continue
            stocks_frame = stocks_frame[stocks_frame["成交筆數"] != 0]
            stocks_frame = stocks_frame[
                ~stocks_frame["證券代號"].isin(etf_frame["證券代號"].to_list())
            ]
            return stocks_frame[stocks_frame["證券代號"].str.len() == 4][
                "證券代號"
            ].to_list()
        except Exception as exc:
            if isinstance(exc, TwseSecurityBlockError):
                raise
            if attempt < retries:
                time.sleep(delay)
    return []


def get_warrant_list(data_date, retries=3, delay=2):
    frames = []
    for attempt in range(1, retries + 1):
        try:
            for warrant_type in ("0999", "0999P"):
                url = (
                    "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX"
                    f"?date={data_date}&type={warrant_type}&response=json"
                )
                frame = parse_table(fetch_json(url))
                if frame is not None:
                    frame["成交股數"] = pd.to_numeric(
                        frame["成交股數"].str.replace(",", ""), errors="coerce"
                    )
                    frame["成交筆數"] = pd.to_numeric(
                        frame["成交筆數"].str.replace(",", ""), errors="coerce"
                    )
                    frames.append(frame[frame["成交筆數"] != 0])
            if frames:
                result = pd.concat(frames, axis=0).sort_values(
                    "成交股數", ascending=False
                )
                return result["證券代號"].to_list()
            return []
        except Exception as exc:
            if isinstance(exc, TwseSecurityBlockError):
                raise
            if attempt < retries:
                time.sleep(delay)
    return []


def fetch_symbols(data_date: str) -> list[str]:
    return get_stock_list(data_date) + get_warrant_list(data_date)


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


def main(request: Request | None = None):
    data = request.get_json(silent=True) or {} if request is not None else {}
    try:
        firestore_client, tasks_client = get_clients()
        result = prepare_crawl_tasks(
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
        response = {
            "status": "success",
            "date": result.date,
            "batch_size": result.batch_size,
            "total_symbols": result.total_symbols,
            "batches_created": result.batches_created,
        }
        if request is None:
            return response
        return jsonify(response), 200
    except PayloadValidationError as exc:
        if request is None:
            raise
        return jsonify({"status": "error", "message": str(exc)}), 400
    except Exception as exc:
        logging.exception("Failed to prepare TWSE list")
        if request is None:
            raise
        return jsonify({"status": "error", "message": str(exc)}), 500


if __name__ == "__main__":
    main()
