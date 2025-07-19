#!/userap/anaconda3/bin/python3
# -*- coding: utf-8 -*-
import os
import cv2
import sys
import json
import shutil
import logging
import requests
import traceback
import numpy as np
import pandas as pd
import zipfile
from io import StringIO
from datetime import datetime, timedelta
from keras.models import load_model
from bs4 import BeautifulSoup
from google.cloud import storage

# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Global variables
ROOT_URL = 'https://bsr.twse.com.tw/bshtm/'
ROOT_PATH = "./"
DATA_SUBFOLDER = 'data'
IMG_SUBFOLDER = 'img'
ALLOWED_CHARS = 'ACDEFGHJKLNPQRTUVXYZ2346789'

# Utility functions
def fetch_json(url):
    """Fetch and parse JSON from a URL."""
    try:
        resp = requests.get(url, verify=False)
        resp.raise_for_status()
        return json.loads(resp.content.decode('utf8'))
    except Exception as e:
        logging.error(f"Failed to fetch JSON from {url}: {e}")
        return None

def parse_table(obj):
    """Extract table data from JSON object."""
    if not obj or 'tables' not in obj:
        return None
    table = None
    for t in obj['tables']:
        if len(t) != 0:
            table = t
    if table: 
        return pd.DataFrame(table['data'], columns=table['fields'])
    return None

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

def one_hot_encoding(text, allowed_chars):
    """Perform one-hot encoding for a given text."""
    return [[1 if c == char else 0 for c in allowed_chars] for char in text]

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
def get_stock_list(dt):
    """Fetch the stock list for a given date."""
    etf_url = f'https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={dt}&type=0099P&response=json'
    all_stocks_url = f'https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={dt}&type=ALLBUT0999&response=json'

    etf_pdf = parse_table(fetch_json(etf_url))
    if etf_pdf is None:
        logging.warning(f"Failed to fetch ETF data for date: {dt}")
        return []

    etf_list = etf_pdf['證券代號'].to_list()

    all_stocks_pdf = parse_table(fetch_json(all_stocks_url))
    if all_stocks_pdf is None:
        logging.warning(f"Failed to fetch stock data for date: {dt}")
        return []

    all_stocks_pdf = all_stocks_pdf[all_stocks_pdf['成交筆數'] != 0]
    all_stocks_pdf = all_stocks_pdf[~all_stocks_pdf['證券代號'].isin(etf_list)]
    return all_stocks_pdf[all_stocks_pdf['證券代號'].str.len() == 4]['證券代號'].to_list()

def get_warrant_list(dt):
    """Fetch the warrant list for a given date."""
    warrant_types = ['0999', '0999P']
    pdf_list = []

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

def crawl_data(stock_list, model, data_dt):
    """Crawl data for the given stock list."""  
    actual_stock_list = []

    for stock_code in stock_list:
        logging.info(f"Trying to crawl data: {stock_code}")
        failed = True
        failed_count = 0  
        while failed:
            try:
                session = requests.Session()
                response = session.get(ROOT_URL + 'bsMenu.aspx', headers={'User-Agent': 'Mozilla/5.0'})
                if not response.ok:
                    continue

                soup = BeautifulSoup(response.text, 'html.parser')
                hidden_elements = {e['name']: e['value'] for e in soup.find_all('input', type='hidden')}
                img_url = ROOT_URL + soup.find_all('img')[1]['src']
                img = session.get(img_url, stream=True)
                if not img.ok:
                    raise Exception("Failed to download captcha image")

                with open(os.path.join(ROOT_PATH, IMG_SUBFOLDER, 'tmp.png'), 'wb') as f:
                    shutil.copyfileobj(img.raw, f)

                captcha_num = solve_captcha(os.path.join(ROOT_PATH, IMG_SUBFOLDER, 'tmp.png'), model)
                if not captcha_num:
                    failed_count += 1
                    raise Exception("Captcha solving failed")

                payload = {
                    'RadioButton_Normal': 'RadioButton_Normal',
                    'TextBox_Stkno': stock_code,
                    'btnOK': '查詢',
                    'CaptchaControl1': captcha_num,
                    **hidden_elements
                }
                post_response = session.post(ROOT_URL + 'bsMenu.aspx', data=payload)
                if '查無資料' in post_response.text:
                    logging.info(f"No data found for stock: {stock_code}")
                    failed = False
                    continue

                bs_report = session.get(ROOT_URL + 'bsContent.aspx')
                if not bs_report.ok:
                    failed_count += 1
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
                actual_stock_list.append(stock_code)
                failed = False
                logging.info(f"Successfully crawled data for stock: {stock_code}")
            except Exception as e:
                logging.error(f"Error while crawling data for stock {stock_code}: {e}")
                logging.debug(traceback.format_exc())

    return actual_stock_list

def zip_folder(folder_path, output_zip_path):
    with zipfile.ZipFile(output_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, folder_path)
                zipf.write(file_path, arcname)
    print(f"Folder zipped to {output_zip_path}")

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

    # Determine the date
    if len(sys.argv) > 1:
        data_dt = sys.argv[1]
    else:
        today_dt = datetime.now()
        if today_dt.hour < 9:
            today_dt -= timedelta(hours=9)
        data_dt = today_dt.strftime('%Y%m%d')

    # Load model
    logging.info("Loading model...")
    model = load_model(os.path.join(ROOT_PATH, DATA_SUBFOLDER, "twse_cnn_model.hdf5"))
    logging.info("Model loaded successfully.")

    # Fetch stock and warrant lists
    logging.info("Fetching stock and warrant lists...")
    stock_list = get_stock_list(data_dt) + get_warrant_list(data_dt)

    # Keep track of crawled stocks
    logging.info("Check if any stocks/warrants had been crawled")
    data_path = os.path.join(ROOT_PATH, DATA_SUBFOLDER, "bs_data", data_dt)
    if not os.path.exists(os.path.join(data_path, 'twse')):
        # check its parent folder
        if not os.path.exists(data_path):
            os.mkdir(data_path)
        os.mkdir(os.path.join(data_path, 'twse'))
    crawled_list = [file_name.replace('.csv', '') for file_name in os.listdir(data_path) if '.csv' in file_name]
    final_stock_list = [stock_no for stock_no in stock_list if stock_no not in crawled_list]

    # Crawl data
    logging.info("Starting data crawling...")
    actual_stock_list = crawl_data(final_stock_list, model, data_dt)

    # Zip files and upload to GCS
    zip_name = f"{data_dt}.zip"
    zip_folder(data_path, zip_name)

    # Step 2: 上傳
    bucket_name = 'stock-crawler-bucket'

    gcs_blob_name = f'bs_report/' + zip_name
    upload_to_gcs(bucket_name, zip_name, gcs_blob_name)

    # Summary
    end_time = datetime.now()
    logging.info(f"Total stocks: {len(stock_list)}")
    logging.info(f"Successfully crawled stocks: {len(actual_stock_list)}")
    logging.info(f"Execution time: {end_time - start_time}")
    logging.info(f"End time: {end_time}")