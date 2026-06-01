#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import cv2
import shutil
import logging
import requests
import traceback
import numpy as np
import pandas as pd
import urllib3
import time
from collections import deque
from dataclasses import dataclass
from enum import Enum, auto
from io import StringIO
from datetime import datetime, timedelta
from typing import List, Optional, Sequence
from keras.models import load_model
from bs4 import BeautifulSoup
from google.cloud import firestore, storage
from google.cloud import run_v2

from stockanalysis.config import PROJECT_ROOT, ensure_dir, resolve_assets, resolve_data, resolve_output
# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Global variables
LOGGING_LEVEL = os.environ.get('LOGGING_LEVEL', 'INFO').upper()
BUCKET_NAME = os.environ.get('STOCK_CRAWLER_BUCKET')
ROOT_URL = 'https://bsr.twse.com.tw/bshtm/'
ALLOWED_CHARS = 'ACDEFGHJKLNPQRTUVXYZ2346789'
REQUEST_TIMEOUT = 2
MAX_SYMBOL_RETRIES = max(1, int(os.environ.get('SYMBOL_MAX_RETRIES', '3')))
MAX_RERUN_ATTEMPTS = 5
CONSECUTIVE_FAILURE_THRESHOLD = 10
SECURITY_BLOCK_MARKERS = (
    "THE PAGE CANNOT BE ACCESSED",
    "FOR SECURITY REASONS",
    "頁面無法執行",
)
NO_DATA_SENTINEL = "__NO_DATA__"
BS_MENU_ENDPOINT = 'bsMenu.aspx'
BS_CONTENT_ENDPOINT = 'bsContent.aspx'
HTML_PARSER = 'html.parser'
CAPTCHA_TMP_NAME = 'tmp.png'
CAPTCHA_PREPROCESSED_NAME = 'preprocessing.jpg'
DEFAULT_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/140.0.0.0 Safari/537.36 Edg/140.0.0.0'
    )
}

_rerun_triggered = False
_run_jobs_client: Optional[run_v2.JobsClient] = None
current_rerun_count = 0
BS_DATA_DIR = resolve_data("bs_data")
CAPTCHA_DIR = ensure_dir(resolve_output("tmp", "captcha"))
MODEL_PATH = resolve_assets("models", "twse_cnn_model.hdf5")


@dataclass(frozen=True)
class CrawlerPaths:
    root: str
    img_dir: str
    data_dir: str
    report_dir: str

    @property
    def captcha_tmp_path(self) -> str:
        return os.path.join(self.img_dir, CAPTCHA_TMP_NAME)

    @property
    def captcha_processed_path(self) -> str:
        return os.path.join(self.img_dir, CAPTCHA_PREPROCESSED_NAME)

    def report_path(self, stock_code: str) -> str:
        return os.path.join(self.report_dir, f"{stock_code}.csv")


class SymbolResult(Enum):
    COMPLETED = auto()
    RETRY = auto()
    SKIP = auto()


def build_paths(data_dt: str) -> CrawlerPaths:
    report_dir = str(BS_DATA_DIR / data_dt / "twse")
    return CrawlerPaths(
        root=str(PROJECT_ROOT),
        img_dir=str(CAPTCHA_DIR),
        data_dir=str(resolve_data()),
        report_dir=report_dir,
    )


def ensure_directories(paths: CrawlerPaths) -> None:
    for path in (paths.img_dir, paths.report_dir):
        os.makedirs(path, exist_ok=True)


def parse_symbols_arg(raw: str) -> List[str]:
    cleaned = raw.strip()
    if cleaned.startswith('[') and cleaned.endswith(']'):
        cleaned = cleaned[1:-1]
    cleaned = cleaned.replace("'", "")
    symbols = [token.strip() for token in cleaned.split(',') if token.strip()]
    return symbols


def resolve_data_date(cli_arg: Optional[str]) -> str:
    if cli_arg:
        return cli_arg
    today = datetime.now()
    if today.hour < 9:
        today -= timedelta(hours=9)
    return today.strftime('%Y%m%d')


def load_captcha_model(paths: CrawlerPaths):
    logging.info("Loading captcha model from %s", MODEL_PATH)
    return load_model(str(MODEL_PATH))


def format_symbol_list(symbols: Sequence[str]) -> str:
    return '[' + ','.join(symbols) + ']'


def extract_rerun_count(args: Sequence[str]) -> tuple[int, List[str]]:
    rerun_count = 0
    remaining: List[str] = []
    for arg in args:
        if arg.startswith("--rerun-count="):
            value = arg.split("=", 1)[1]
            try:
                rerun_count = int(value)
            except ValueError:
                logging.warning("Invalid rerun count '%s', defaulting to 0", value)
                rerun_count = 0
            continue
        remaining.append(arg)
    return rerun_count, remaining


def create_http_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    return session


def build_url(endpoint: str) -> str:
    return endpoint if endpoint.startswith('http') else ROOT_URL + endpoint


def http_get(session: requests.Session, endpoint: str, **kwargs) -> requests.Response:
    return session.get(build_url(endpoint), timeout=REQUEST_TIMEOUT, **kwargs)


def http_post(session: requests.Session, endpoint: str, **kwargs) -> requests.Response:
    return session.post(build_url(endpoint), timeout=REQUEST_TIMEOUT, **kwargs)


def extract_hidden_inputs(soup: BeautifulSoup) -> dict:
    return {
        element.get('name'): element.get('value', '')
        for element in soup.find_all('input', type='hidden')
        if element.get('name')
    }


def extract_captcha_url(soup: BeautifulSoup) -> str:
    images = soup.find_all('img')
    if len(images) < 2:
        raise RuntimeError(
            f"Unexpected number of <img> tags on bsMenu.aspx: found {len(images)}"
        )
    return build_url(images[1]['src'])


def download_captcha(session: requests.Session, url: str, destination: str) -> None:
    response = http_get(session, url, stream=True)
    response.raise_for_status()
    with open(destination, 'wb') as file_handle:
        shutil.copyfileobj(response.raw, file_handle)


def read_twse_report(csv_text: str, data_dt: str) -> pd.DataFrame:
    raw_df = pd.read_csv(StringIO(csv_text), sep=',', skiprows=2)
    first_half = raw_df.iloc[:, :5]
    second_half = raw_df.iloc[:, 6:]
    second_half.columns = first_half.columns
    merged = pd.concat([first_half, second_half], axis=0).sort_values('序號')
    merged['券商'] = merged['券商'].str[:4]
    merged['日期'] = datetime.strptime(data_dt, '%Y%m%d').strftime('%Y/%m/%d')
    return merged.drop(columns=['序號'])


def save_report(df: pd.DataFrame, destination: str) -> None:
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    df.to_csv(destination, encoding='utf8', index=False)


def _get_metadata_project_id() -> Optional[str]:
    metadata_url = "http://metadata.google.internal/computeMetadata/v1/project/project-id"
    headers = {"Metadata-Flavor": "Google"}
    try:
        response = requests.get(metadata_url, headers=headers, timeout=2)
        if response.status_code == 200:
            return response.text.strip()
    except Exception as exc:
        logging.debug("Unable to read project ID from metadata: %s", exc)
    return None


def trigger_cloud_run_job_rerun(argv: Sequence[str], rerun_count: int) -> None:
    """Trigger the same Cloud Run Job with identical container arguments."""
    global _rerun_triggered, _run_jobs_client
    if _rerun_triggered:
        return

    if rerun_count >= MAX_RERUN_ATTEMPTS:
        logging.error(
            "Max rerun attempts reached (%s); skipping rerun trigger.",
            MAX_RERUN_ATTEMPTS,
        )
        return

    job_name = os.environ.get("CLOUD_RUN_JOB")
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT") or _get_metadata_project_id()
    region = (
        os.environ.get("CLOUD_RUN_REGION")
        or os.environ.get("CLOUD_RUN_LOCATION")
        or os.environ.get("GOOGLE_CLOUD_REGION")
        or os.environ.get("LOCATION")
        or "asia-east1"
    )

    if not job_name:
        logging.error("CLOUD_RUN_JOB not set; cannot trigger rerun.")
        return
    if not project_id:
        logging.error("Project ID missing; cannot trigger rerun.")
        return
    if not region:
        logging.error("Cloud Run region missing (set CLOUD_RUN_REGION); cannot trigger rerun.")
        return

    try:
        if _run_jobs_client is None:
            _run_jobs_client = run_v2.JobsClient()
        job_path = run_v2.JobsClient.job_path(project_id, region, job_name)
    except Exception as exc:
        logging.error("Failed to initialise Cloud Run Jobs client: %s", exc)
        return

    container_args = []
    for arg in argv:
        if arg is None:
            continue
        if isinstance(arg, str) and arg.startswith("--rerun-count="):
            continue
        container_args.append(str(arg))

    next_rerun_count = rerun_count + 1
    container_args.append(f"--rerun-count={next_rerun_count}")
    overrides = None
    if container_args:
        overrides = run_v2.RunJobRequest.Overrides(
            container_overrides=[
                run_v2.RunJobRequest.Overrides.ContainerOverride(args=container_args)
            ]
        )

    run_request = run_v2.RunJobRequest(name=job_path, overrides=overrides)

    try:
        operation = _run_jobs_client.run_job(request=run_request)
    except Exception as exc:
        logging.error("Failed to trigger Cloud Run job rerun: %s", exc)
        return

    _rerun_triggered = True
    execution_name = getattr(getattr(operation, "metadata", None), "name", None)
    if not execution_name:
        execution_name = getattr(operation, "name", "<unknown>")
    logging.critical(
        "Triggered Cloud Run job rerun #%s for %s in %s (execution: %s)",
        next_rerun_count,
        job_name,
        region,
        execution_name,
    )

# Logging configuration
logging.basicConfig(level=logging.getLevelName(LOGGING_LEVEL), format="%(asctime)s [%(levelname)s] %(message)s")

# Utility functions
def preprocess_image(from_filename, to_filename, width=200, height=60, crop_left=10, crop_top=10, crop_bottom=10):
    """Preprocess an image for captcha solving."""
    if not os.path.isfile(from_filename):
        return
    img = cv2.imread(from_filename)
    denoised = cv2.fastNlMeansDenoisingColored(img, None, 30, 30, 7, 21)
    kernel = np.ones((4, 4), np.uint8)
    erosion = cv2.erode(denoised, kernel, iterations=1)
    blurred = cv2.GaussianBlur(erosion, (5, 5), 0)
    edged = cv2.Canny(blurred, 30, 150)
    dilation = cv2.dilate(edged, kernel, iterations=1)
    crop_img = dilation[crop_top:height - crop_bottom, crop_left:width]
    cv2.imwrite(to_filename, crop_img)

def one_hot_decoding(prediction, allowed_chars):
    """Decode one-hot encoded predictions."""
    return ''.join(allowed_chars[np.argmax(predict[0])] for predict in prediction)

def solve_captcha(source_img_path: str, processed_img_path: str, model):
    """Solve captcha using a pre-trained model."""
    preprocess_image(source_img_path, processed_img_path)
    train_data = np.stack([np.array(cv2.imread(processed_img_path)) / 255.0])
    prediction = model.predict(train_data)
    return one_hot_decoding(prediction, ALLOWED_CHARS)

# Main functions
def crawl_data(stock_code: str, model, data_dt: str, paths: CrawlerPaths, max_retries: int = 50):
    """Crawl data for the given stock code. Returns the output file path on success."""
    attempts = 0
    bs_menu_failures = 0

    while attempts < max_retries:
        attempts += 1
        if attempts % 10 == 0:
            logging.info(
                "Retrying crawl for %s (attempt %s/%s)",
                stock_code,
                attempts,
                max_retries,
            )

        session = create_http_session()
        try:
            try:
                response = http_get(session, BS_MENU_ENDPOINT)
                response.raise_for_status()
            except requests.RequestException as exc:
                bs_menu_failures += 1
                if bs_menu_failures % 5 == 0:
                    logging.warning(
                        "Failed to load %s (failure %s): %s",
                        BS_MENU_ENDPOINT,
                        bs_menu_failures,
                        exc,
                    )
                time.sleep(1)
                continue

            if any(marker in response.text for marker in SECURITY_BLOCK_MARKERS):
                logging.critical(
                    "Received TWSE security block page while requesting %s; exiting container.",
                    BS_MENU_ENDPOINT,
                )
                trigger_cloud_run_job_rerun(sys.argv[1:], current_rerun_count)
                raise SystemExit("TWSE security block page encountered")

            soup = BeautifulSoup(response.text, HTML_PARSER)
            try:
                captcha_url = extract_captcha_url(soup)
            except RuntimeError as exc:
                logging.error("%s", exc)
                continue

            hidden_elements = extract_hidden_inputs(soup)
            try:
                download_captcha(session, captcha_url, paths.captcha_tmp_path)
            except requests.RequestException as exc:
                logging.error("Failed to download captcha for %s: %s", stock_code, exc)
                continue

            captcha_num = solve_captcha(paths.captcha_tmp_path, paths.captcha_processed_path, model)
            if not captcha_num:
                logging.error("Captcha solving produced empty result for %s", stock_code)
                continue

            payload = {
                'RadioButton_Normal': 'RadioButton_Normal',
                'TextBox_Stkno': stock_code,
                'btnOK': '查詢',
                'CaptchaControl1': captcha_num,
                **hidden_elements,
            }
            post_response = http_post(session, BS_MENU_ENDPOINT, data=payload)
            if '查無資料' in post_response.text:
                logging.info("No data found for stock: %s", stock_code)
                return NO_DATA_SENTINEL

            try:
                bs_report = http_get(session, BS_CONTENT_ENDPOINT)
                bs_report.raise_for_status()
            except requests.RequestException as exc:
                logging.warning("Failed to load %s: %s", BS_CONTENT_ENDPOINT, exc)
                continue

            report_df = read_twse_report(bs_report.text, data_dt)
            output_path = paths.report_path(stock_code)
            save_report(report_df, output_path)
            logging.info("Successfully crawled data for stock: %s", stock_code)
            return output_path
        except Exception as exc:
            logging.error("Error while crawling data for stock %s: %s", stock_code, exc)
            logging.debug(traceback.format_exc())
        finally:
            session.close()

    logging.error("Failed to crawl data for stock %s after %s attempts", stock_code, max_retries)
    return None

def upload_to_gcs(bucket_name, source_file_path, destination_blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_path)
    logging.info("Uploaded %s to gs://%s/%s", source_file_path, bucket_name, destination_blob_name)


def finalize_symbol(doc_ref, symbol, processed_symbols, log_message: str | None = None) -> bool:
    if log_message:
        logging.info(log_message)
    try:
        doc_ref.delete()
        logging.info("Deleted Firestore document for symbol: %s", symbol)
    except Exception as exc:
        logging.error("Failed to delete Firestore document for symbol %s: %s", symbol, exc)
        return False
    processed_symbols.append(symbol)
    return True


def process_symbol(
    symbol: str,
    fs_client: firestore.Client,
    collection_name: str,
    model,
    data_dt: str,
    paths: CrawlerPaths,
    processed_symbols: List[str],
) -> SymbolResult:
    doc_ref = fs_client.collection(collection_name).document(symbol)

    try:
        doc = doc_ref.get()
    except Exception as exc:
        logging.error("Failed to access Firestore for symbol %s: %s", symbol, exc)
        return SymbolResult.RETRY

    if not doc.exists:
        logging.debug("Symbol %s not found in Firestore, skipping.", symbol)
        return SymbolResult.SKIP

    crawl_result = crawl_data(symbol, model, data_dt, paths)
    if crawl_result == NO_DATA_SENTINEL:
        if finalize_symbol(
            doc_ref,
            symbol,
            processed_symbols,
            log_message=(
                f"TWSE returned no data for stock {symbol}; removing Firestore doc without upload."
            ),
        ):
            return SymbolResult.COMPLETED
        return SymbolResult.RETRY

    if not crawl_result or not os.path.exists(crawl_result):
        logging.error("Failed to crawl data for stock %s, skipping upload.", symbol)
        return SymbolResult.RETRY

    try:
        gcs_blob_name = f'bs_report/twse/{data_dt}/{symbol}.csv'
        upload_to_gcs(BUCKET_NAME, crawl_result, gcs_blob_name)
    except Exception as exc:
        logging.error("Failed to upload %s to GCS: %s", crawl_result, exc)
        return SymbolResult.RETRY

    if finalize_symbol(doc_ref, symbol, processed_symbols):
        return SymbolResult.COMPLETED
    return SymbolResult.RETRY

# Main execution
if __name__ == "__main__":
    logging.info(f"Start time: {datetime.now()}")
    start_time = datetime.now()

    if not BUCKET_NAME:
        logging.error("Missing STOCK_CRAWLER_BUCKET environment variable.")
        sys.exit(1)

    rerun_count, remaining_args = extract_rerun_count(sys.argv[1:])
    sys.argv = [sys.argv[0]] + remaining_args

    current_rerun_count = rerun_count
    if current_rerun_count >= MAX_RERUN_ATTEMPTS:
        logging.warning(
            "Job started with rerun count %s (max %s); further rerun triggers will be skipped.",
            current_rerun_count,
            MAX_RERUN_ATTEMPTS,
        )

    if len(sys.argv) < 2:
        logging.error("No symbols were provided. Expecting a symbols argument in the first position.")
        sys.exit(1)

    symbols = parse_symbols_arg(sys.argv[1])

    if not symbols:
        logging.warning("Symbol list is empty after parsing, nothing to crawl.")
        sys.exit(0)

    data_dt = resolve_data_date(sys.argv[2] if len(sys.argv) > 2 else None)

    logging.info("Received symbols: %s", format_symbol_list(symbols))
    logging.info("Data date: %s", data_dt)

    paths = build_paths(data_dt)
    ensure_directories(paths)

    logging.info("Loading model...")
    model = load_captcha_model(paths)
    logging.info("Model loaded successfully.")

    fs_client = firestore.Client()
    processed_symbols = []
    failed_symbols = []
    failed_symbol_set: set[str] = set()
    collection_name = f"twse_crawl_status_{data_dt}"

    symbol_queue = deque((symbol, 0) for symbol in symbols)
    consecutive_retry_iterations = 0

    while symbol_queue:
        symbol, attempt = symbol_queue.popleft()
        logging.info(
            "Starting to crawl stock %s (attempt %s/%s)",
            symbol,
            attempt + 1,
            MAX_SYMBOL_RETRIES,
        )

        result = process_symbol(
            symbol,
            fs_client,
            collection_name,
            model,
            data_dt,
            paths,
            processed_symbols,
        )

        if result is SymbolResult.RETRY:
            consecutive_retry_iterations += 1
            reached_failure_guard = (
                CONSECUTIVE_FAILURE_THRESHOLD > 0
                and consecutive_retry_iterations >= CONSECUTIVE_FAILURE_THRESHOLD
            )

            if attempt + 1 < MAX_SYMBOL_RETRIES and not reached_failure_guard:
                logging.info(
                    "Requeuing stock %s for retry (attempt %s/%s)",
                    symbol,
                    attempt + 2,
                    MAX_SYMBOL_RETRIES,
                )
                symbol_queue.append((symbol, attempt + 1))
                continue

            if attempt + 1 >= MAX_SYMBOL_RETRIES:
                logging.error("Max retries reached for stock %s, giving up.", symbol)
            else:
                logging.error(
                    "Stopping retries for stock %s due to consecutive failure threshold (%s iterations).",
                    symbol,
                    consecutive_retry_iterations,
                )

            if symbol not in failed_symbol_set:
                failed_symbol_set.add(symbol)
                failed_symbols.append(symbol)

            if reached_failure_guard:
                logging.critical(
                    "Detected %s consecutive failing iterations; aborting remaining %s symbols for rerun.",
                    consecutive_retry_iterations,
                    len(symbol_queue),
                )
                drained_count = 0
                while symbol_queue:
                    pending_symbol, _ = symbol_queue.popleft()
                    if (
                        pending_symbol in processed_symbols
                        or pending_symbol in failed_symbol_set
                    ):
                        continue
                    failed_symbol_set.add(pending_symbol)
                    failed_symbols.append(pending_symbol)
                    drained_count += 1
                logging.critical(
                    "Added %s pending symbols to failed list due to consecutive failures.",
                    drained_count,
                )
                break
        else:
            consecutive_retry_iterations = 0

    end_time = datetime.now()
    logging.info(f"Successfully crawled stocks: {len(processed_symbols)}")
    logging.info("Processed symbols: %s", format_symbol_list(processed_symbols))
    if failed_symbols:
        logging.warning(
            "Failed symbols after retries: %s",
            format_symbol_list(failed_symbols),
        )
        rerun_symbols = format_symbol_list(failed_symbols)
        if len(sys.argv) > 1:
            sys.argv[1] = rerun_symbols
        else:
            sys.argv.append(rerun_symbols)
        logging.info("Triggering rerun with symbols: %s", rerun_symbols)
        trigger_cloud_run_job_rerun(sys.argv[1:], current_rerun_count)
    logging.info(f"Execution time: {end_time - start_time}")
    logging.info(f"End time: {end_time}")
