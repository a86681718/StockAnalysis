import os
import json
import time
from google.cloud import firestore, tasks_v2
from google.protobuf import timestamp_pb2, duration_pb2
from datetime import datetime, timedelta, timezone
from flask import Request, request, jsonify
import pandas as pd
import logging
import requests

def get_project_id():
    METADATA_URL = "http://metadata.google.internal/computeMetadata/v1/project/project-id"
    headers = {"Metadata-Flavor": "Google"}
    response = requests.get(METADATA_URL, headers=headers, timeout=2)
    return response.text

# 設定專案資訊
PROJECT_ID = get_project_id()
QUEUE_NAME = os.environ.get("QUEUE_NAME")  # 你事先建立的 Queue 名稱
LOCATION = os.environ.get("LOCATION", "asia-east1")
FUNCTION_URL = os.environ.get("FUNCTION_URL")  # Cloud Function B 的 URL

client = firestore.Client()
tasks_client = tasks_v2.CloudTasksClient()

# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logging.info(f"Project ID: {PROJECT_ID}")

def parse_table(obj):
    """Extract table data from JSON object."""
    if not obj or 'tables' not in obj:
        return None
    table = None
    for t in obj['tables']:
        if len(t) != 0:
            table = t
    if table: 
        columns = [f.strip() for f in table['fields']]
        return pd.DataFrame(table['data'], columns=columns)
    return None

def fetch_json(url, retries=3, delay=2):
    headers = {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
               "X-Requested-With": "XMLHttpRequest",
               "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"}
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=10, verify=False)
            resp.raise_for_status()
            print(resp.content.decode('utf8'))
            return json.loads(resp.content.decode('utf8'))
        except Exception as e:
            logging.warning(f"[Attempt {attempt}] Request failed: {e}")
            if attempt < retries:
                time.sleep(delay)
            else:
                logging.error(f"All {retries} attempts failed.")
                return None

def get_stock_list(dt, retries=3, delay=2):
    logging.info(f"Fetch the stock list for a given date: {dt}")
    """Fetch the stock list for a given date."""
    etf_url = f'https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={dt}&type=0099P&response=json'
    all_stocks_url = f'https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={dt}&type=ALLBUT0999&response=json'

    for attempt in range(1, retries + 1):
        try:
            etf_pdf = parse_table(fetch_json(etf_url))
            if etf_pdf is None:
                logging.warning(f"Failed to fetch ETF data for date: {dt}")
                continue

            etf_list = etf_pdf['證券代號'].to_list()

            all_stocks_pdf = parse_table(fetch_json(all_stocks_url))
            if all_stocks_pdf is None:
                logging.warning(f"Failed to fetch stock data for date: {dt}")
                continue

            all_stocks_pdf = all_stocks_pdf[all_stocks_pdf['成交筆數'] != 0]
            all_stocks_pdf = all_stocks_pdf[~all_stocks_pdf['證券代號'].isin(etf_list)]
            return all_stocks_pdf[all_stocks_pdf['證券代號'].str.len() == 4]['證券代號'].to_list()
        except Exception as e:
            logging.warning(f"[Attempt {attempt}] Failed to fetch stock list: {e}")
            if attempt < retries:
                time.sleep(delay)
            else:
                logging.error(f"All {retries} attempts to fetch stock list failed.")
                return []

def get_warrant_list(dt, retries=3, delay=2):
    logging.info(f"Fetch the warrant list for a given date: {dt}")
    """Fetch the warrant list for a given date."""
    warrant_types = ['0999', '0999P']
    pdf_list = []

    for attempt in range(1, retries + 1):
        try:
            for w_type in warrant_types:
                url = f'https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={dt}&type={w_type}&response=json'
                pdf = parse_table(fetch_json(url))
                if pdf is not None:
                    pdf['成交股數'] = pd.to_numeric(pdf['成交股數'].str.replace(',', ''), errors='coerce')
                    pdf['成交筆數'] = pd.to_numeric(pdf['成交筆數'].str.replace(',', ''), errors='coerce')
                    pdf = pdf[pdf['成交筆數'] != 0]
                    pdf_list.append(pdf)

            if pdf_list:
                concat_pdf = pd.concat(pdf_list, axis=0).sort_values('成交股數', ascending=False)
                return concat_pdf[concat_pdf['成交筆數'] != 0]['證券代號'].to_list()
            return []
        except Exception as e:
            logging.warning(f"[Attempt {attempt}] Failed to fetch warrant list: {e}")
            if attempt < retries:
                time.sleep(delay)
            else:
                logging.error(f"All {retries} attempts to fetch warrant list failed.")
                return []

def create_task(symbols: list, dt: str):
    parent = tasks_client.queue_path(PROJECT_ID, LOCATION, QUEUE_NAME)
    SERVICE_ACCOUNT = f"cloud-run@{PROJECT_ID}.iam.gserviceaccount.com"
    payload = {"symbols": symbols, 'date': dt}
    task = {
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": FUNCTION_URL,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(payload).encode(),
            "oidc_token": {
                "service_account_email": SERVICE_ACCOUNT,
            },
        },
        'dispatch_deadline': duration_pb2.Duration(seconds=1800)
    }

    # Optional: 延遲幾秒觸發，避免過載
    delay = timestamp_pb2.Timestamp()
    delay.FromDatetime(datetime.now(timezone.utc) + timedelta(seconds=5))
    task["schedule_time"] = delay

    response = tasks_client.create_task(parent=parent, task=task)
    logging.info(f"Created task for {symbols}: {response.name}")

def create_collection_if_not_exists(collection_name):
    doc_ref = client.collection(collection_name).document("dummy")
    try:
        # 檢查該 document 是否存在
        doc = doc_ref.get()
        if not doc.exists:
            # 如果文件不存在，這意味著 collection 還沒創建
            doc_ref.set({
                "status": "collection_created"
            })
            logging.info(f"Collection '{collection_name}' was not found, and has been created.")
        else:
            logging.info(f"Collection '{collection_name}' already exists.")
    except Exception as e:
        logging.info(f"Error checking collection: {e}")

def main(request: Request | None = None):
    """
    Main function to handle the request and create tasks.
    """
    try:
        data = {}
        if request is not None:
            data = request.get_json(silent=True) or {}
            logging.info(f'request.get_json(): {data}')

        if data and "date" in data:
            data_dt = data["date"]
        else:
            data_dt = datetime.now().strftime('%Y/%m/%d')
        logging.info(f"Date of data: {data_dt}")

        batch_size = data.get("batch_size", 3500) if data else 3500
        logging.info(f"Batch size: {batch_size}")

        collection_name = "twse_crawl_status_" + data_dt.replace("/", "")
        collection_ref = client.collection(collection_name)
        docs = collection_ref.stream()
        symbols = [doc.id for doc in docs]

        if symbols:
            symbols = [symbol for symbol in symbols if symbol != "dummy"]
            logging.info(f"Fetched {len(symbols)} symbols from Firestore collection {collection_name}")
        else:
            logging.warning(f"No symbols found in Firestore collection {collection_name}, fallback to crawl")
            symbols = get_stock_list(data_dt) + get_warrant_list(data_dt)

            create_collection_if_not_exists(collection_name)
            for symbol in symbols:
                doc_ref = client.collection(collection_name).document(symbol)
                doc_ref.set({"status": "initialized", "updatedAt": firestore.SERVER_TIMESTAMP})
            logging.info(f"Initialized Firestore collection {collection_name} with {len(symbols)} symbols")

        total_symbols = len(symbols)
        batches_created = 0

        for i in range(0, total_symbols, batch_size):
            batch = symbols[i:i + batch_size]
            logging.info(f"Processing batch: {batch}")

            for symbol in batch:
                doc_ref = client.collection(collection_name).document(symbol)
                doc_ref.set({"status": "pending", "updatedAt": firestore.SERVER_TIMESTAMP})

            create_task(batch, data_dt)
            batches_created += 1

        response_payload = {
            "status": "success",
            "date": data_dt,
            "batch_size": batch_size,
            "total_symbols": total_symbols,
            "batches_created": batches_created,
        }

        if request is None:
            logging.info(f"Completed prepare-twse-list run: {response_payload}")
            return response_payload

        return jsonify(response_payload), 200

    except Exception as exc:
        logging.exception("Failed to prepare TWSE list")
        if request is None:
            raise
        return jsonify({"status": "error", "message": str(exc)}), 500

if __name__ == "__main__":
    main()
