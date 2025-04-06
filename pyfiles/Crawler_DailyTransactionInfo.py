import os 
import sys
import json
import logging
import requests
import pandas as pd
from commonlib import getConf
from io import StringIO
from time import sleep
from random import randint
from datetime import datetime, timedelta

def toNumeric(x):
    try:
        return float(str(x).replace(',', ''))
    except:
        return None
    
def strToFloat(pdf, num_cols):
    tmp = pdf.copy()
    for col in num_cols:
        tmp.loc[:, col] = tmp[col].apply(toNumeric)
    return tmp

def toRocYear(date_str, sep='/'):
    YEAR_OFFSET = 1911
    split = date_str.split(sep)
    ori_year = split[0]
    new_year = str(int(ori_year) - YEAR_OFFSET)
    new_date_str = date_str.replace(ori_year, new_year)
    return new_date_str

conf = getConf()

start_dt = None
end_dt = None
if len(sys.argv) > 1:
    start_dt = sys.argv[1]
    end_dt = sys.argv[2]
else:
    now = datetime.now()
    start_dt = now.strftime('%Y/%m/%d')
    end_dt = now.strftime('%Y/%m/%d')

# start_dt = '2021/07/09'
# end_dt = '2021/07/09'

data_path = conf.get('data.path')
ohlc_subfolder = 'ohlc'
warrant_subfolder = 'warrant'
common_subfolder = 'common'
stock_output_path = data_path + os.sep + ohlc_subfolder
warrant_output_path = data_path + os.sep + warrant_subfolder

print(f"stock_output_path: {stock_output_path}")
print(f"warrant_output_path: {warrant_output_path}")

if not os.path.exists(stock_output_path):
    os.makedirs(stock_output_path, exist_ok=True)

if not os.path.exists(warrant_output_path):
    os.makedirs(warrant_output_path, exist_ok=True)

dt = datetime.strptime(start_dt, '%Y/%m/%d')
end_dt = datetime.strptime(end_dt, '%Y/%m/%d')

tpex_url = 'https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php?l=zh-tw&d=%s'
twse_url = 'https://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=%s&type=ALL'
tpex_header = ['股票代號', '名稱', '收盤價', '漲跌價差', '開盤價', '最高價', '最低價', '均價', '成交股數', '成交金額', 
               '成交筆數', '最後買價', '最後買量', '最後賣價', '最後賣量', '發行股數', '次日參考價', '次日漲停價', '次日跌停價']
header_list = ['股票代號', '成交股數', '成交金額', '開盤價', '最高價', '最低價', '收盤價', '漲跌價差', '成交筆數']
header_str = '股票代號,日期,成交股數,成交金額,開盤價,最高價,最低價,收盤價,漲跌價差,漲跌幅,成交筆數\n'

warrant_df = pd.read_csv(data_path + os.sep + common_subfolder + os.sep + 'warrantList.csv')
while dt <= end_dt:
    if dt.weekday() in range(0, 5):
        print(dt.strftime('%Y/%m/%d'))
        
        # ---------- TPEX ----------
        roc_dt = toRocYear(dt.strftime('%Y/%m/%d'))
        tpex_resp = requests.get(tpex_url % roc_dt)
        json_obj = json.loads(tpex_resp.text)
        json_data = json_obj['aaData']
        tpex_pdf = pd.DataFrame(json_data, columns=tpex_header)
        
        #for stock
        tpex_stock_pdf = tpex_pdf[tpex_pdf['股票代號'].str.len() == 4]
        tpex_stock_pdf = tpex_stock_pdf[header_list]
        
        #for warrant
        tpex_warrant_pdf = tpex_pdf.copy().rename(columns={"股票代號": "權證代號"})
        tpex_warrant_pdf = strToFloat(tpex_warrant_pdf, ['收盤價', '開盤價', '漲跌價差', '最高價', '最低價', '成交股數', '成交金額', '成交筆數'])
        tpex_warrant_pdf = pd.merge(tpex_warrant_pdf, warrant_df, on='權證代號')
        tpex_warrant_pdf = tpex_warrant_pdf[tpex_warrant_pdf['成交股數']>0].sort_values('成交金額', ascending=False)
        tpex_warrant_file_name = 'warrant_tpex_order_' + dt.strftime('%Y-%m-%d') + '.csv'
        tpex_warrant_pdf[['權證代號']].to_csv(warrant_output_path + os.sep + tpex_warrant_file_name , index=False)
        
        # ---------- TWSE ----------
        twse_resp = requests.get(twse_url % dt.strftime('%Y%m%d'))
        if twse_resp.text != '':
            twse_pdf = pd.read_csv(StringIO(twse_resp.text.replace("=", "")), header=["證券代號" in l for l in twse_resp.text.split("\n")].index(True)-1)
            
            # for stock
            twse_stock_pdf = twse_pdf[twse_pdf['證券代號'].str.len() == 4].copy()
            twse_stock_pdf.loc[:, '漲跌(+/-)'] = twse_stock_pdf.loc[:, '漲跌(+/-)'].map(lambda x: 1 if x=='+' else -1)
            twse_stock_pdf.loc[:, '漲跌價差'] = twse_stock_pdf['漲跌價差']*twse_stock_pdf['漲跌(+/-)']
            twse_stock_pdf = twse_stock_pdf.rename(columns={'證券代號':'股票代號', '證券名稱':'名稱'})
            twse_stock_pdf = twse_stock_pdf[header_list]
            
            # for warrant
            twse_warrant_pdf = twse_pdf.copy().rename(columns={"證券代號": "權證代號"})
            twse_warrant_pdf = strToFloat(twse_warrant_pdf, ['收盤價', '開盤價', '漲跌價差', '最高價', '最低價', '成交股數', '成交金額', '成交筆數'])
            twse_warrant_pdf = pd.merge(twse_warrant_pdf, warrant_df, on='權證代號')
            twse_warrant_pdf = twse_warrant_pdf[twse_warrant_pdf['成交股數']>0].sort_values('成交金額', ascending=False)
            twse_warrant_file_name = 'warrant_twse_order_' + dt.strftime('%Y-%m-%d') + '.csv'
            twse_warrant_pdf[['權證代號']].to_csv(warrant_output_path + os.sep + twse_warrant_file_name, index=False)
        else:
            dt = dt + timedelta(days=1)
            sleep(randint(3, 5))
            continue
        
        stock_df = tpex_stock_pdf.append(twse_stock_pdf)
        stock_df = strToFloat(stock_df, ['收盤價', '開盤價', '漲跌價差', '最高價', '最低價', '成交股數', '成交金額', '成交筆數'])
        stock_df.loc[:, '前日收盤價'] = stock_df['收盤價']-stock_df['漲跌價差']
        stock_df.loc[:, '漲跌幅'] = stock_df['漲跌價差']/stock_df['前日收盤價']*100
        stock_df = stock_df.round(2)

        for row in stock_df.iterrows():
            data = row[1]
            stock_no = data['股票代號']
            volume = data['成交股數']
            transaction = data['成交筆數']
            turnover = data['成交金額']
            close_price = data['收盤價']
            open_price = data['開盤價']
            high_price = data['最高價']
            low_price = data['最低價']
            change_price = data['漲跌價差']
            change_ratio = data['漲跌幅']
            row = "{},{},{},{},{},{},{},{},{},{},{}\n".format(stock_no, dt.strftime('%Y-%m-%d'), volume, turnover, 
                                                   open_price, high_price, low_price, close_price,
                                                   change_price, change_ratio, transaction)
            file_path = stock_output_path + os.sep + stock_no + '.csv'
            
            if os.path.exists(file_path):
                file = open(file_path, 'a')
                file.write(row)
            else:
                file = open(file_path, 'w')
                file.write(header_str)
                file.write(row)
            file.close()
    dt = dt + timedelta(days=1)
    sleep(randint(1, 4))
