import os
import sys
import math
import json
import talib
import smtplib
import logging
from tabulate import tabulate
import requests
import traceback
import pandas as pd
from time import sleep
from datetime import datetime, timedelta
from email.mime.text import MIMEText

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
root_path = "/userap/BuySellReport/data/ohlc"

def getStockList(url):
    df = pd.read_html(url,encoding='big5hkscs',header=0)[0]
    raw_list = df['有價證券代號及名稱']
    code_list = []
    for code in raw_list:
        code = code.split('　')[0]
        if len(code) == 4:
            code_list.append(code)
    return code_list

# logging.info('get stock list')
twse_url = 'http://isin.twse.com.tw/isin/C_public.jsp?strMode=2'
tpex_url = 'https://isin.twse.com.tw/isin/C_public.jsp?strMode=4'

twse_list = getStockList(twse_url)
tpex_list = getStockList(tpex_url)

# logging.info('get stock price')
session = requests.Session()
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'}
final_result = []

# TPEX stock price
query = ''
for i in range(len(tpex_list)):
    if i % 100 == 0 and query != '':
        resp = None
        while not resp:
            try:    
                resp = session.get('https://mis.twse.com.tw/stock/api/getStockInfo.jsp?json=1&delay=0&ex_ch=' + query[:-1], headers=headers)
            except:
                logging.warning('retry')
                sleep(20)
        final_result.append(resp.text)
        query = ''
        sleep(10)
        logging.info('tpex batch: {}'.format(math.ceil(i / 100)))
    query += 'otc_%s.tw|' % tpex_list[i]
resp = None
while not resp:
    try:
        resp = session.get('https://mis.twse.com.tw/stock/api/getStockInfo.jsp?json=1&delay=0&ex_ch=' + query[:-1], headers=headers)
    except:
        logging.warning('retry')
        sleep(20)
final_result.append(resp.text)
logging.info('tpex batch: {}'.format(math.ceil(i / 100)))

# TWSE stock price
query = ''
for i in range(len(twse_list)):
    if i % 100 == 0 and query != '':
        resp = None
        while not resp:    
            try:    
                resp = session.get('https://mis.twse.com.tw/stock/api/getStockInfo.jsp?json=1&delay=0&ex_ch=' + query[:-1], headers=headers)
            except:
                logging.warning('retry')
                sleep(20)
        final_result.append(resp.text)
        query = ''
        sleep(10)
        logging.info('twse batch: {}'.format(math.ceil(i / 100)))
    query += 'tse_%s.tw|' % twse_list[i]
resp = None
while not resp:     
    try:
        resp = session.get('https://mis.twse.com.tw/stock/api/getStockInfo.jsp?json=1&delay=0&ex_ch=' + query[:-1], headers=headers)
    except:
        logging.warning('retry')
        sleep(20)
final_result.append(resp.text)
logging.info('twse batch: {}'.format(math.ceil(i / 100)))

logging.info('parse data')
json_dict = {}
for result in final_result:
    try:
        for json_obj in json.loads(result).get('msgArray'):
            data = {}
            data['股票代號'] = json_obj.get('c')
            data['日期'] = datetime.strptime(json_obj.get('d'), '%Y%m%d').strftime('%Y-%m-%d')

            try:
                data['成交股數'] = int(json_obj.get('v'))
                data['成交金額'] = int(float(json_obj.get('h')) * int(json_obj.get('v')) * 1000)
                data['開盤價'] = float(json_obj.get('o'))
                data['最高價'] = float(json_obj.get('h'))
                data['最低價'] = float(json_obj.get('l'))
                data['收盤價'] = float(json_obj.get('h'))
                data['漲跌價差'] = round(float(json_obj.get('h')) - float(json_obj.get('y')), 2)
                data['漲跌幅'] = round(data['漲跌價差']/float(json_obj.get('y'))*100, 2)
            except:
                pass
            json_dict[data['股票代號']] = data
    except:
        logging.error(result)
df_list = []
now = datetime.now()
start_dt = (now - timedelta(days=60)).strftime('%Y-%m-%d')
end_dt = now.strftime('%Y-%m-%d')
bband_slope_change_criteria = 0.5
bband_width_change_criteria = 0.25
prev_bband_width_criteria = 5
for file in sorted(os.listdir(root_path)):
    if '.csv' not in file:
        continue
    logging.info(file.replace('.csv', ''))
    
    # load stock price data
    file_path = root_path + os.sep + file
    try:
        stock_df = pd.read_csv(file_path, index_col=None, header=0, dtype={'股票代號':str}, engine='python', encoding='utf-8').drop_duplicates().sort_values('日期')
        if file.split('.')[0] in json_dict:
            stock_df = stock_df.append(json_dict[file.split('.')[0]], ignore_index=True)
        stock_df.loc[:, '日期'] = pd.to_datetime(stock_df['日期'].str.replace('＊', ''))
        stock_df = stock_df[(stock_df['日期'] >= start_dt) & (stock_df['日期'] <= end_dt)]
        if len(stock_df) < 40:
            continue
        stock_df = stock_df.sort_values('日期')
        stock_df['收盤價'].fillna(method='ffill', inplace=True)

        # calculate BBands and some features
        closed = stock_df['收盤價'].values
        upper,middle,lower = talib.BBANDS(closed, 20, 2, 2, matype=talib.MA_Type.SMA)
        previous_upper = pd.Series(upper).shift(periods=1).to_numpy()
        previous_lower = pd.Series(lower).shift(periods=1).to_numpy()
        upper_change = pd.Series((upper - previous_upper) / previous_upper*100)
        lower_change = (lower - previous_lower) / previous_lower*100
        bband_width =  pd.Series((upper-lower) / middle*100)
        bband_width_ma = pd.Series(talib.SMA(bband_width, 5))

        # add BBands data to stock price dataframe
        stock_df = stock_df.reset_index().drop('index', axis=1)
        stock_df['bband_width'] = bband_width
        stock_df['prev_bband_width'] = stock_df['bband_width'].shift(periods=1)
        stock_df['bband_slope'] = upper_change
        stock_df['prev_bband_slope'] = stock_df['bband_slope'].shift(periods=1)
        stock_df['bband_slope_change'] = abs(stock_df['bband_slope'] - stock_df['prev_bband_slope']) / (abs(stock_df['prev_bband_slope'] + 0.00000001))
        stock_df['bband_width_change'] = (stock_df['bband_width'] - stock_df['prev_bband_width']) / (stock_df['prev_bband_width'] + 0.00000001)

        # filter data if meet the alert conditions
        filtered_df = stock_df[(stock_df['bband_slope_change']>bband_slope_change_criteria)
                               & (stock_df['bband_width_change']>bband_width_change_criteria)
                               & (stock_df['prev_bband_width']<prev_bband_width_criteria)]
        df_list.append(filtered_df)
    except:
        stock_df = pd.read_csv(file_path, index_col=None, header=0, dtype={'股票代號':str}, engine='python').drop_duplicates().sort_values('日期')

if len(df_list)==0:
    logging.info("no result found.")
    exit()
alert_df = pd.concat(df_list, axis=0, ignore_index=True, sort=False)
selected_columns = ['股票代號', '成交股數', '成交金額', '收盤價', '漲跌幅', 'prev_bband_width','prev_bband_slope', 'bband_slope_change', 'bband_width_change']
#alert_df = alert_df[alert_df['日期']==end_dt].sort_values(['日期', '成交金額'])[selected_columns].round(2)
alert_df = alert_df[alert_df['日期']==end_dt].sort_values(['日期', '成交金額'])[selected_columns].round(2)
logging.info("\n" + tabulate(alert_df, headers='keys', tablefmt='psql', showindex=False, floatfmt=".2f"))

# Send alert mail
mime = MIMEText(tabulate(alert_df, headers='keys', tablefmt='psql', showindex=False, floatfmt=".2f"), "plain", "utf-8")
mime["Subject"] = "BBand Alert"
mime["From"] = "b86681718@gmail.com"
mime["To"] = "a86681718@gmail.com"
msg=mime.as_string()

smtp=smtplib.SMTP('smtp.gmail.com', 587)
smtp.ehlo()
smtp.starttls()
smtp.login("b86681718@gmail.com",'gjfsqhwfhrqcuene')
from_addr="b86681718@gmail.com"
to_addr=sys.argv[1]
status=smtp.sendmail(from_addr, to_addr, msg)
if status=={}:
    logging.info("Send alert mail successfully.")
else:
    logging.info("Send alert mail failed.")
smtp.quit()
