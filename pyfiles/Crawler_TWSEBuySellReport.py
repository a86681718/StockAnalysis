#!/userap/anaconda3/bin/python3
# -*- coding: utf-8 -*-
import os
import cv2
import shutil
import logging
import requests
import operator
import traceback
import collections
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from io import StringIO
from os import listdir
from time import sleep, time
from random import randint
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logging.info('Start time: {}'.format(datetime.now()))
start = time()

''' 
# this version of getStockList only includes stocks but not warrants
def getStockList(url):
    df = pd.read_html(url,encoding='big5hkscs',header=0)[0]
    raw_list = df['有價證券代號及名稱']
    code_list = []
    for code in raw_list:
        code = code.split('　')[0]
        if len(code) == 4:
            code_list.append(code)
    return code_list
'''

def getStockList(url):
    df = pd.read_html(url,encoding='big5hkscs',header=0)[0]
    stock_list = []
    warrant_list = []
    stock_flag = False
    except_str = ['C', 'B', 'X', 'Y']
    for row in df.iterrows():
        stock_no = row[1]['有價證券代號及名稱'].split('\u3000')[0]

        if stock_no == '股票':
            stock_flag = True
            continue
        elif stock_no == '上市認購(售)權證':
            stock_flag = False
            continue
        elif stock_no == 'ETN':
            break

        if stock_flag:
            stock_list.append(stock_no[:4])
        else:
            if any(x in stock_no for x in except_str):
                pass
            else:
                warrant_list.append(stock_no[:6])
    return sorted(stock_list) + sorted(warrant_list)

def mse(imageA, imageB):
    err = np.sum((imageA.astype("float") - imageB.astype("float"))**2)
    err /= float(imageA.shape[0] * imageA.shape[1])
    return err

def getMseTuples(pic):
    mse_dict = {}
    for alphabet in alphabet_dict:
        mse_dict[alphabet] = mse(alphabet_dict[alphabet], pic)
    sorted_mse = sorted(mse_dict.items(), key=operator.itemgetter(1))
    return sorted_mse

def captchaSolver(img_path):
    image = cv2.imread(img_path)
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    ret, threshold = cv2.threshold(gray, 200, 255, cv2.THRESH_BINARY)
    kernel = np.ones((2,2), np.uint16)
    erosion = cv2.erode(threshold, kernel, iterations=2)
    blurred = cv2.GaussianBlur(erosion, (3,3), 0)
    contours, hierarchy = cv2.findContours(blurred.copy(), cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)
    cnts = sorted([(c, cv2.boundingRect(c)[0]) for c in contours], key = lambda x: x[1]) 
    
    ary = []
    for (c, _) in cnts:
        (x, y, w, h) = cv2.boundingRect(c)
        if w>15 and h>15:
            ary.append((x, y, w, h))      

    index = 0
    prev = (-100, -100, 100, 100)
    remove_list = []
    for _, obj in enumerate(ary):
        if (obj[0]-prev[0] <= 15
            and obj[1]-prev[1] <= 15
            and obj[2] < prev[2]
            and obj[3] < prev[3]):
            remove_list.append(obj)
        prev = obj
    
    for obj in remove_list:
        ary.remove(obj)
        
    if len(ary) != 5:
        return None
    
    result = ''
    for id, (x, y, w, h) in enumerate(ary):
        roi = blurred[y:y+h, x:x+w]
        thresh = roi.copy()
        res = cv2.resize(thresh, (50, 50))
        kernel = np.ones((3,3), np.uint16)
        edged = cv2.Canny(res, 100, 200)
        res = cv2.dilate(edged, kernel, iterations=2)
        
        mse_dict = getMseTuples(res)
        first = mse_dict[0]
        second = mse_dict[1]
        if second[1]-first[1] <= 1000:
            return None
        if first[1] > 18000:
            return None
            
        result += first[0]
    return result


logging.info("Loading alphabet pictures for captchaSolver")
root_path = '/userap/BuySellReport'
alphabet_dict = {}
for png in os.listdir(root_path + os.sep + 'alphabet'):
    if '.png' not in png:
        continue
    alphabet = png.replace('.png', '')

    image = ~(cv2.imread(root_path + os.sep + 'alphabet' + os.sep + png))

    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    ret, threshold = cv2.threshold(gray, 200, 255, cv2.THRESH_BINARY)
    
    kernel = np.ones((3,3), np.uint16)
    erosion = cv2.dilate(threshold, kernel, iterations=1)
    
    contours, hierarchy = cv2.findContours(erosion.copy(), cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)
    cnts = sorted([(c, cv2.boundingRect(c)[0]) for c in contours], key = lambda x: x[1])

    for (c, _) in cnts:
        (x, y, w, h) = cv2.boundingRect(c)
        contour = erosion[y:y+h, x:x+w]
        res = cv2.resize(contour, (50, 50))
        ret, threshold = cv2.threshold(res, 200, 255, cv2.THRESH_BINARY)
        alphabet_dict[alphabet] = threshold
        break

session = None
root_url = 'https://bsr.twse.com.tw/bshtm/'
twse_page = 'http://isin.twse.com.tw/isin/C_public.jsp?strMode=2'

logging.info('get stock list')
stock_list = getStockList(twse_page)

today_dt = datetime.now()
hour = today_dt.hour
if hour < 9:
    today_dt = today_dt - timedelta(hours=9)
data_dt = today_dt.strftime('%Y%m%d')

logging.info('Check if data path is not existed')
data_path = root_path + os.sep + 'bs_data' + os.sep + data_dt 
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
    last_index = stock_list.index(file_list[-1]) + 1
final_stock_list = stock_list[last_index:]
actual_stock_list = []

logging.info('Start to crawl data')
for stock_code in final_stock_list:
    headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36' }
    failed = True
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
                    logging.info('download captcha image')
                    f = open(root_path + os.sep + 'tmp.png', 'wb')
                    shutil.copyfileobj(img.raw, f)
                    f.close()
                    
                    logging.info('solve captcha by image recognition')
                    captcha_num = captchaSolver(root_path + os.sep + 'tmp.png')
                    if captcha_num == None:
                        logging.info('captcha failed')
                        continue
                    
                    logging.info('captch pass')
                    logging.info('send post request to get data')
                    payload = {'RadioButton_Normal':'RadioButton_Normal', 
                               'TextBox_Stkno': stock_code, 
                               'btnOK':'查詢',
                               'CaptchaControl1': captcha_num}
                    for element in hidden_elements:
                        payload[element.attrs['name']] = element.attrs['value']
                    post_response = session.post(root_url + 'bsMenu.aspx', data = payload, headers = headers)
                    if '查無資料' in post_response.text:
                        failed = False
                        continue
                    if 'HyperLink_DownloadCSV' not in post_response.text:
                        continue

                    logging.info('extract transaction date from html page')
                    bs_page = session.get(root_url + 'bsContent.aspx?v=t', headers = headers)
                    html_content = BeautifulSoup(bs_page.text, 'html.parser')
                    main_table = html_content.select_one('table table table')
                    tx_date = main_table.select('tr td')[1].text.strip().replace('\r\n', '')
                    
                    logging.info('parse and download buy/sell report data')
                    bs_report = session.get(root_url + 'bsContent.aspx', headers = headers)
                    bs_df = pd.read_csv(StringIO(bs_report.text), sep=',', skiprows=2)
                    df_part1 = bs_df.iloc[:, :5]
                    df_part2 = bs_df.iloc[:, 6:]
                    df_part2.columns = df_part1.columns
                    df_merged = df_part1.append(df_part2).sort_values('序號')
                    df_merged['券商'] = df_merged['券商'].str[:4]
                    del df_merged['序號']
                    df_merged['日期'] = tx_date
                    df_merged.to_csv(root_path + os.sep + 'bs_data' + os.sep + data_dt + os.sep + 'twse' + os.sep + '{}.csv'.format(stock_code), encoding='utf8', index=False)
                    actual_stock_list.append(stock_code)
                    logging.info(stock_code + 'has been crawled')
                    failed = False
        except Exception as e:
            logging.info(traceback.format_exc())
end = time()
logging.info("Total count:", len(final_stock_list))
logging.info("Actual count:", len(set(actual_stock_list)))
logging.info("Missed stock:", set(final_stock_list)-set(actual_stock_list))
logging.info("Execution Time:", end-start)
logging.info('End time: {}'.format(datetime.now()))