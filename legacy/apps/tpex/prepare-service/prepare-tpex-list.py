import os
import json
from google.cloud import firestore, tasks_v2
from google.protobuf import timestamp_pb2, duration_pb2
from datetime import datetime, timedelta, timezone
from flask import Request
import pandas as pd
import logging
import requests

# 設定專案資訊
PROJECT_ID = "sturdy-willow-471513-q1"
QUEUE_NAME = "stock-crawl-queue"   # 你事先建立的 Queue 名稱
LOCATION = "asia-east1"
FUNCTION_URL = f"https://trigger-run-job-514324377659.asia-east1.run.app"  # Cloud Function B 的 URL

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

def fetch_json(url, payload, retries=3, delay=2):
    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
    }

    for attempt in range(1, retries + 1):
        try:
            response = requests.post(url, headers=headers, data=payload, timeout=10, verify=False)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logging.warning(f"[Attempt {attempt}] Request failed: {e}")
            if attempt < retries:
                time.sleep(delay)
            else:
                logging.error(f"All {retries} attempts failed.")
                return None

def get_stock_list(url, dt):
    """Retrieve the stock list."""
    stock_payload = {
        "date": dt,
        "type": "EW",
        "id": "",
        "response": "json",
    }
    all_stocks_pdf = parse_table(fetch_json(url, stock_payload))
    if all_stocks_pdf is not None:
        all_stocks_pdf = all_stocks_pdf[all_stocks_pdf['成交筆數'] != 0]
        return all_stocks_pdf[all_stocks_pdf['代號'].str.len() == 4]['代號'].to_list()
    return []

def get_warrant_list(url, dt):
    """Retrieve the warrant list."""
    warrant_payload = {
        "date": dt,
        "type": "WW",
        "id": "",
        "response": "json",
    }
    all_warrant_pdf = parse_table(fetch_json(url, warrant_payload))
    if all_warrant_pdf is not None:
        all_warrant_pdf['成交股數'] = pd.to_numeric(all_warrant_pdf['成交股數'].str.replace(',', ''), errors='coerce')
        all_warrant_pdf['成交筆數'] = pd.to_numeric(all_warrant_pdf['成交筆數'].str.replace(',', ''), errors='coerce')
        all_warrant_pdf = all_warrant_pdf[all_warrant_pdf['成交筆數'] != 0].sort_values('成交股數', ascending=False)
        return all_warrant_pdf['代號'].to_list()
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
    print(f"Created task for {symbols}: {response.name}")

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
            print(f"Collection '{collection_name}' was not found, and has been created.")
        else:
            print(f"Collection '{collection_name}' already exists.")
    except Exception as e:
        print(f"Error checking collection: {e}")

def main(request: Request):
    """
    Main function to handle the request and create tasks.
    """
    data_dt = None
    try:
        data = request.get_json(silent=True) or {}
        logging.info(f'request.get_json(): {data}')
        if data and "date" in data:
            data_dt = data["date"]
        else:
            data_dt = datetime.now().strftime('%Y/%m/%d')
    except Exception:
        data_dt = datetime.now().strftime('%Y/%m/%d')
    logging.info(f"Date of data: {data_dt}")

    # Get the batch size from the request or use the default value
    batch_size = data.get("batch_size", 5) if data else 5
    logging.info(f"Batch size: {batch_size}")

    # Fetch stock and warrant lists
    url = "https://www.tpex.org.tw/www/zh-tw/afterTrading/otc"
    collection_name = "crawl_status_" + data_dt.replace("/", "")
    collection_ref = client.collection(collection_name)

    # try to get all doc ID in Firestore collection 
    docs = collection_ref.stream()
    symbols = [doc.id for doc in docs]

    if symbols:
        logging.info(f"Fetched {len(symbols)} symbols from Firestore collection {collection_name}")
    else:
        logging.warning(f"No symbols found in Firestore collection {collection_name}, fallback to crawl")
        symbols = get_stock_list(url, data_dt) + get_warrant_list(url, data_dt)

        # fallback 抓資料時，先建 Firestore collection 並初始化 document 狀態
        create_collection_if_not_exists(collection_name)
        for symbol in symbols:
            doc_ref = client.collection(collection_name).document(symbol)
            doc_ref.set({"status": "pending", "updatedAt": firestore.SERVER_TIMESTAMP})
        logging.info(f"Initialized Firestore collection {collection_name} with {len(symbols)} symbols")

    # Split symbols into batches and create tasks
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        logging.info(f"Processing batch: {batch}")

        # Update Firestore documents for the batch
        for symbol in batch:
            doc_ref = client.collection(collection_name).document(symbol)
            doc_ref.set({"status": "pending", "updatedAt": firestore.SERVER_TIMESTAMP})

        # Create Cloud Task for the batch
        create_task(batch, data_dt)

    return "Tasks created"

if __name__ == "__main__":
    main()
