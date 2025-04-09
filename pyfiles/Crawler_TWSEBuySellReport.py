#!/userap/anaconda3/bin/python3
# -*- coding: utf-8 -*-
import os
import cv2
import sys
import json
import shutil
import logging
import requests
import operator
import traceback
import collections
import numpy as np
import pandas as pd

from io import StringIO
from os import listdir
from time import sleep, time
from random import randint
from datetime import datetime, timedelta
from keras.models import load_model
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logging.info('Start time: {}'.format(datetime.now()))
start = time()

def getStockList(dt):
    resp = requests.get(f'https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={dt}&type=0099P&response=json')
    obj = json.loads(resp.content.decode('utf8'))
    table = None
    for t in obj['tables']:
        if len(t) != 0:
            table = t
    fields = table['fields']
    data = table['data']
    pdf = pd.DataFrame(data, columns=fields)
    etf_list = pdf['證券代號'].to_list()

    resp = requests.get(f'https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={dt}&type=ALLBUT0999&response=json')
    obj = json.loads(resp.content.decode('utf8'))
    table = None
    if obj.get('tables'):
        for t in obj['tables']:
            if len(t) != 0:
                table = t
    else:
        logging.warning(f'Get empty table for date: {dt}')
        return []
    fields = table['fields']
    data = table['data']
    pdf = pd.DataFrame(data, columns=fields)
    pdf = pdf[pdf['成交筆數']!=0]
    pdf = pdf[~pdf['證券代號'].isin(etf_list)]
    stock_list = pdf[pdf['證券代號'].apply(lambda x: len(x) == 4)]['證券代號'].to_list()
    return stock_list


def getWarrantList(dt):
    pdf_list = []
    # call: 0999 / put: 0999P
    warrant_type = ['0999', '0999P']
    for w_type in warrant_type:
        resp = requests.get(f'https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={dt}&type={w_type}&response=json')
        obj = json.loads(resp.content.decode('utf8'))
        table = None
        for t in obj['tables']:
            if len(t) != 0:
                table = t
        fields = table['fields']
        data = table['data']
        pdf = pd.DataFrame(data, columns=fields)
        pdf['成交股數'] = pd.to_numeric(pdf['成交股數'].str.replace(',', ''), errors='coerce')
        pdf['成交筆數'] = pd.to_numeric(pdf['成交筆數'].str.replace(',', ''), errors='coerce')
        pdf = pdf[pdf['成交筆數']!=0]
        pdf_list.append(pdf)
    concat_pdf = pd.concat(pdf_list, axis=0)
    concat_pdf = concat_pdf.sort_values('成交股數', ascending=False)
    wrn_list = concat_pdf[concat_pdf['成交筆數']!=0]['證券代號'].to_list()
    return wrn_list

def preprocessing(from_filename, to_filename):
    WIDTH = 200
    HEIGHT = 60
    CROP_LEFT = 10
    CROP_TOP = 10
    CROP_BOTTON = 10
    
    if not os.path.isfile(from_filename):
        return
    img = cv2.imread(from_filename)
    denoised = cv2.fastNlMeansDenoisingColored(img, None, 30, 30, 7, 21)
    
    kernel = np.ones((4,4), np.uint8)
    erosion = cv2.erode(denoised, kernel, iterations=1)
    burred = cv2.GaussianBlur(erosion, (5, 5), 0)
    
    edged = cv2.Canny(burred, 30, 150)
    dilation = cv2.dilate(edged, kernel, iterations=1)
    
    crop_img = dilation[CROP_TOP:HEIGHT - CROP_BOTTON, CROP_LEFT:WIDTH]

    cv2.imwrite(to_filename, crop_img)
    return

def one_hot_encoding(text, allowedChars):
    label_list = []
    for c in text:
        onehot = [0] * len(allowedChars)
        onehot[allowedChars.index(c)] = 1
        label_list.append(onehot)
    return label_list

def one_hot_decoding(prediction, allowedChars):
    text = ''
    for predict in prediction:
        value = np.argmax(predict[0])
        text += allowedChars[value]
    return text

def captchaSolver(source_img_path):
    img_subfolder = 'img'
    file_name = 'preprocessing.jpg'
    processed_img_path = os.sep.join([root_path, img_subfolder, file_name])
    allowed_chars = 'ACDEFGHJKLNPQRTUVXYZ2346789';
    
    preprocessing(source_img_path, processed_img_path)
    train_data = np.stack([np.array(cv2.imread(processed_img_path))/255.0])
    prediction = model.predict(train_data)
    predict_captcha = one_hot_decoding(prediction, allowed_chars)
    return predict_captcha

session = None
root_url = 'https://bsr.twse.com.tw/bshtm/'
twse_page = 'http://isin.twse.com.tw/isin/C_public.jsp?strMode=2'
root_path = "./"
data_subfolder = 'data'

date_dt = None
if len(sys.argv) > 1:
    data_dt = sys.argv[1]
else:
    today_dt = datetime.now()
    hour = today_dt.hour
    if hour < 9:
        today_dt = today_dt - timedelta(hours=9)
    data_dt = today_dt.strftime('%Y%m%d')

logging.info('get stock list')
stock_list = getStockList(data_dt) + getWarrantList(data_dt)

logging.info('Check if data path is not existed')
data_path = root_path + os.sep + 'data' + os.sep + 'bs_data' + os.sep + data_dt 
if not os.path.exists(data_path + os.sep + 'twse'):
    # check its parent folder
    if not os.path.exists(data_path):
        os.mkdir(data_path)
    os.mkdir(data_path + os.sep + 'twse')
data_path = data_path + os.sep + 'twse'

logging.info('Check if any stocks/warrants had been crawled')
stock_file_list = [file_name.replace('.csv', '') for file_name in os.listdir(data_path) if '.csv' in file_name and len(file_name) == 8]
warrant_file_list = [file_name.replace('.csv', '') for file_name in os.listdir(data_path) if '.csv' in file_name and len(file_name) == 10]
file_list = sorted(stock_file_list) + sorted(warrant_file_list)
last_index = 0
if len(file_list) != 0:
    last_index = stock_list.index(sorted(file_list)[-1]) + 1
final_stock_list = stock_list[last_index:]
actual_stock_list = []

logging.debug('model loading...')
model = load_model(os.sep.join([root_path, data_subfolder, "twse_cnn_model.hdf5"]))
logging.debug('loading completed')


logging.info('Start to crawl data')
for stock_code in final_stock_list:
    headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36' }
    failed = True
    failed_cnt = 0
    logging.info(f"Trying to crawl data: {stock_code}")
    while failed:
        try:
            if session != None:
                session.close()
            session = requests.Session()
            response = session.get(root_url + 'bsMenu.aspx', headers=headers)
            if response.ok:
                soup = BeautifulSoup(response.text, 'html.parser')
                hidden_elements = soup.find_all('input', type='hidden')

                # get captcha image to solve
                img_id = soup.find_all('img')[1].attrs['src']
                img_url = root_url + img_id
                img = session.get(img_url, stream = True)
                if img.ok:
                    logging.debug('download captcha image')
                    f = open(root_path + os.sep + 'img' + os.sep + 'tmp.png', 'wb')
                    shutil.copyfileobj(img.raw, f)
                    f.close()
                    
                    logging.debug('solve captcha by image recognition')
                    captcha_num = captchaSolver(root_path + os.sep + 'img' + os.sep + 'tmp.png')
                    if captcha_num == None:
                        logging.debug('captcha failed')
                        failed_cnt += 1
                        continue

                    logging.debug('captcha pass')
                    logging.debug('send post request to get data')
                    payload = {'RadioButton_Normal':'RadioButton_Normal', 
                               'TextBox_Stkno': stock_code, 
                               'btnOK':'查詢',
                               'CaptchaControl1': captcha_num}
                    for element in hidden_elements:
                        payload[element.attrs['name']] = element.attrs['value']
                    post_response = session.post(root_url + 'bsMenu.aspx', data = payload, headers = headers)
                    if '查無資料' in post_response.text:
                        logging.info('no result found.')
                        open(root_path + os.sep + "data" + os.sep + 'bs_data' + os.sep + data_dt + os.sep + 'twse' + os.sep + '{}.csv'.format(stock_code), "a")
                        failed = False
                        continue
                    if 'HyperLink_DownloadCSV' not in post_response.text:
                        continue

                    logging.debug('extract transaction date from html page')
                    bs_page = session.get(root_url + 'bsContent.aspx?v=t', headers = headers)
                    html_content = BeautifulSoup(bs_page.text, 'html.parser')
                    main_table = html_content.select_one('table table table')
                    if not main_table:
                        failed_cnt += 1
                        logging.warning('main_table is None')
                        continue
                    tx_date = main_table.select('tr td')[1].text.strip().replace('\r\n', '')

                    logging.debug('parse and download buy/sell report data')
                    bs_report = session.get(root_url + 'bsContent.aspx', headers = headers)
                    if not bs_report.ok:
                        failed += 1
                        logging.warning('403 forbidden while crawling data')
                        continue
                    bs_df = pd.read_csv(StringIO(bs_report.text), sep=',', skiprows=2)
                    df_part1 = bs_df.iloc[:, :5]
                    df_part2 = bs_df.iloc[:, 6:]
                    df_part2.columns = df_part1.columns
                    df_merged = pd.concat([df_part1,df_part2], axis=0).sort_values('序號')
                    df_merged['券商'] = df_merged['券商'].str[:4]
                    del df_merged['序號']
                    df_merged['日期'] = tx_date
                    df_merged.to_csv(root_path + os.sep + "data" + os.sep + 'bs_data' + os.sep + data_dt + os.sep + 'twse' + os.sep + '{}.csv'.format(stock_code), encoding='utf8', index=False)
                    actual_stock_list.append(stock_code)
                    logging.info(f'{stock_code} has been crawled with {failed_cnt} failures')
                    failed = False
        except Exception as e:
            logging.debug(traceback.format_exc())
end = time()
logging.info(f"Total count: {len(final_stock_list)}")
logging.info(f"Actual count: {len(set(actual_stock_list))}")
logging.info(f"Missed stock: {set(final_stock_list)-set(actual_stock_list)}")
logging.info(f"Execution Time: {end-start}")
logging.info(f"End time: {datetime.now()}")