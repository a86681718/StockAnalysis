#!/userap/anaconda3/bin/python3
# -*- coding: utf-8 -*-
import os
import cv2
import sys
import shutil
import logging
import requests
import traceback
import numpy as np
import pandas as pd
import urllib3
from collections import deque
from io import StringIO
from datetime import datetime, timedelta
from keras.models import load_model
from bs4 import BeautifulSoup
from google.cloud import firestore, storage

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Global variables
LOGGING_LEVEL = os.environ.get('LOGGING_LEVEL', 'INFO').upper()
BUCKET_NAME = os.environ.get('STOCK_CRAWLER_BUCKET', 'stock-crawler-bucket-20250908')
ROOT_URL = 'https://bsr.twse.com.tw/bshtm/'
ROOT_PATH = "./"
DATA_SUBFOLDER = 'data'
IMG_SUBFOLDER = 'img'
ALLOWED_CHARS = 'ACDEFGHJKLNPQRTUVXYZ2346789'
REQUEST_TIMEOUT = 2
MAX_SYMBOL_RETRIES = max(1, int(os.environ.get('SYMBOL_MAX_RETRIES', '3')))

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

def solve_captcha(source_img_path, model):
    """Solve captcha using a pre-trained model."""
    processed_img_path = os.path.join(ROOT_PATH, IMG_SUBFOLDER, 'preprocessing.jpg')
    preprocess_image(source_img_path, processed_img_path)
    train_data = np.stack([np.array(cv2.imread(processed_img_path)) / 255.0])
    prediction = model.predict(train_data)
    return one_hot_decoding(prediction, ALLOWED_CHARS)

# Main functions
def crawl_data(stock_code, model, data_dt, max_retries=30):
    """Crawl data for the given stock code. Returns the output file path on success."""
    attempts = 0
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36 Edg/140.0.0.0'
    }

    bs_menu_failures = 0
    while attempts < max_retries:
        attempts += 1
        logging.info(f"Trying to crawl data: {stock_code} (attempt {attempts}/{max_retries})")
        session = requests.Session()
        try:
            response = session.get(
                ROOT_URL + 'bsMenu.aspx',
                headers=headers,
                timeout=REQUEST_TIMEOUT
            )
            if not response.ok:
                bs_menu_failures += 1
                if bs_menu_failures <= 3 or bs_menu_failures % 5 == 0:
                    logging.warning(f"Failed to load bsMenu.aspx, retrying... (failures: {bs_menu_failures})")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            hidden_elements = {e['name']: e['value'] for e in soup.find_all('input', type='hidden')}
            images = soup.find_all('img')
            if len(images) < 2:
                logging.error(
                    "Unexpected img tags on bsMenu.aspx for %s: found %s elements."
                    " Response snippet: %s",
                    stock_code,
                    len(images),
                    response.text
                )
                continue
            img_url = ROOT_URL + images[1]['src']
            img = session.get(
                img_url,
                stream=True,
                headers=headers,
                timeout=REQUEST_TIMEOUT
            )
            if not img.ok:
                raise RuntimeError("Failed to download captcha image")

            img_path = os.path.join(ROOT_PATH, IMG_SUBFOLDER, 'tmp.png')
            with open(img_path, 'wb') as f:
                shutil.copyfileobj(img.raw, f)

            captcha_num = solve_captcha(img_path, model)
            if not captcha_num:
                raise RuntimeError("Captcha solving failed")

            payload = {
                'RadioButton_Normal': 'RadioButton_Normal',
                'TextBox_Stkno': stock_code,
                'btnOK': '查詢',
                'CaptchaControl1': captcha_num,
                **hidden_elements
            }
            post_response = session.post(
                ROOT_URL + 'bsMenu.aspx',
                data=payload,
                headers=headers,
                timeout=REQUEST_TIMEOUT
            )
            if '查無資料' in post_response.text:
                logging.info(f"No data found for stock: {stock_code}")
                return None

            bs_report = session.get(
                ROOT_URL + 'bsContent.aspx',
                headers=headers,
                timeout=REQUEST_TIMEOUT
            )
            if not bs_report.ok:
                logging.warning("Failed to load bsContent.aspx, retrying...")
                continue

            bs_df = pd.read_csv(StringIO(bs_report.text), sep=',', skiprows=2)
            df_part1 = bs_df.iloc[:, :5]
            df_part2 = bs_df.iloc[:, 6:]
            df_part2.columns = df_part1.columns
            df_merged = pd.concat([df_part1, df_part2], axis=0).sort_values('序號')
            df_merged['券商'] = df_merged['券商'].str[:4]
            df_merged['日期'] = datetime.now().strftime('%Y/%m/%d')
            df_merged.drop(columns=['序號'], inplace=True)

            output_path = os.path.join(ROOT_PATH, DATA_SUBFOLDER, 'bs_data', data_dt, 'twse', f'{stock_code}.csv')
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            df_merged.to_csv(output_path, encoding='utf8', index=False)
            logging.info(f"Successfully crawled data for stock: {stock_code}")
            return output_path
        except Exception as e:
            logging.error(f"Error while crawling data for stock {stock_code}: {e}")
            logging.debug(traceback.format_exc())
        finally:
            session.close()

    logging.error(f"Failed to crawl data for stock {stock_code} after {max_retries} attempts")
    return None

def upload_to_gcs(bucket_name, source_file_path, destination_blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_path)
    print(f"Uploaded to gs://{bucket_name}/{destination_blob_name}")

# Main execution
if __name__ == "__main__":
    logging.info(f"Start time: {datetime.now()}")
    start_time = datetime.now()

    if len(sys.argv) < 2:
        logging.error("No symbols were provided. Expecting a symbols argument in the first position.")
        sys.exit(1)

    symbols_arg = sys.argv[1]
    cleaned_symbols = symbols_arg.strip()
    if cleaned_symbols.startswith('[') and cleaned_symbols.endswith(']'):
        cleaned_symbols = cleaned_symbols[1:-1]
    cleaned_symbols = cleaned_symbols.replace("'", '')
    symbols = [symbol.strip() for symbol in cleaned_symbols.split(',') if symbol.strip()]

    if not symbols:
        logging.warning("Symbol list is empty after parsing, nothing to crawl.")
        sys.exit(0)

    if len(sys.argv) > 2 and sys.argv[2]:
        data_dt = sys.argv[2]
    else:
        today_dt = datetime.now()
        if today_dt.hour < 9:
            today_dt -= timedelta(hours=9)
        data_dt = today_dt.strftime('%Y%m%d')

    logging.info(f"Received symbols: {symbols}")
    logging.info(f"Data date: {data_dt}")

    os.makedirs(os.path.join(ROOT_PATH, IMG_SUBFOLDER), exist_ok=True)
    os.makedirs(os.path.join(ROOT_PATH, DATA_SUBFOLDER, 'bs_data', data_dt, 'twse'), exist_ok=True)

    logging.info("Loading model...")
    model_path = os.path.join(ROOT_PATH, DATA_SUBFOLDER, "twse_cnn_model.hdf5")
    model = load_model(model_path)
    logging.info("Model loaded successfully.")

    fs_client = firestore.Client()
    processed_symbols = []
    failed_symbols = []
    bucket_name = 'stock-crawler-bucket-20250908'
    collection_name = f"twse_crawl_status_{data_dt}"

    symbol_queue = deque((symbol, 0) for symbol in symbols)

    while symbol_queue:
        symbol, attempt = symbol_queue.popleft()
        logging.info(f"Starting to crawl stock {symbol} (attempt {attempt + 1}/{MAX_SYMBOL_RETRIES})")

        should_retry = False
        doc_ref = fs_client.collection(collection_name).document(symbol)

        try:
            doc = doc_ref.get()
        except Exception as e:
            logging.error(f"Failed to access Firestore for symbol {symbol}: {e}")
            should_retry = True
        else:
            if not doc.exists:
                logging.debug(f"Symbol {symbol} not found in Firestore, skipping.")
                continue

            output_path = crawl_data(symbol, model, data_dt)
            if not output_path or not os.path.exists(output_path):
                logging.error(f"Failed to crawl data for stock {symbol}, skipping upload.")
                should_retry = True
            else:
                try:
                    gcs_blob_name = f'bs_report/twse/{data_dt}/{symbol}.csv'
                    upload_to_gcs(bucket_name, output_path, gcs_blob_name)
                except Exception as e:
                    logging.error(f"Failed to upload {output_path} to GCS: {e}")
                    should_retry = True
                else:
                    try:
                        doc_ref.delete()
                        logging.info(f"Deleted Firestore document for symbol: {symbol}")
                    except Exception as e:
                        logging.error(f"Failed to delete Firestore document for symbol {symbol}: {e}")
                        should_retry = True
                    else:
                        processed_symbols.append(symbol)
                        continue

        if should_retry:
            if attempt + 1 < MAX_SYMBOL_RETRIES:
                logging.info(
                    f"Requeuing stock {symbol} for retry (attempt {attempt + 2}/{MAX_SYMBOL_RETRIES})"
                )
                symbol_queue.append((symbol, attempt + 1))
            else:
                logging.error(f"Max retries reached for stock {symbol}, giving up.")
                failed_symbols.append(symbol)

    end_time = datetime.now()
    logging.info(f"Successfully crawled stocks: {len(processed_symbols)}")
    logging.info(f"Processed symbols: {processed_symbols}")
    if failed_symbols:
        logging.warning(f"Failed symbols after retries: {failed_symbols}")
    logging.info(f"Execution time: {end_time - start_time}")
    logging.info(f"End time: {end_time}")
