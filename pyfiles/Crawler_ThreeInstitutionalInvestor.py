import os 
import sys
import json
import logging
import requests
import pandas as pd
from time import sleep
from random import randint
from datetime import datetime, timedelta
from commonlib import getConf, strToFloat, toRocYear

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
conf = getConf()

logging.info("checking output path")
output_path = conf.get("data.path") + os.sep + "threeInstitutionalInvestor"
if not os.path.exists(output_path):
    logging.info("creating output path")
    os.mkdir(output_path)
    
start_dt = None
end_dt = None
if len(sys.argv) > 1:
    start_dt = sys.argv[1]
    end_dt = sys.argv[2]
else:
    now = datetime.now()
    start_dt = now.strftime('%Y/%m/%d')
    end_dt = now.strftime('%Y/%m/%d')

logging.info(f"crawling {start_dt} ~ {end_dt}")
dt = datetime.strptime(start_dt, '%Y/%m/%d')
end_dt = datetime.strptime(end_dt, '%Y/%m/%d')

tpex_url = 'https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&se=EW&t=D&d=%s'
twse_url = 'https://www.twse.com.tw/fund/T86?response=json&date=%s&selectType=ALLBUT0999'
tpex_header_old = ["股票代號","名稱","外陸資買進股數(不含外資自營商)","外陸資賣出股數(不含外資自營商)","外陸資買賣超股數(不含外資自營商)",
                   "投信買進股數","投信賣出股數","投信買賣超股數","自營商買賣超股數","自營商買進股數(自行買賣)","自營商賣出股數(自行買賣)",
                   "自營商買賣超股數(自行買賣)","自營商買進股數(避險)","自營商賣出股數(避險)","自營商買賣超股數(避險)",
                   "三大法人買賣超股數", "其他"]
tpex_header_new = ["股票代號","名稱","外陸資買進股數(不含外資自營商)","外陸資賣出股數(不含外資自營商)","外陸資買賣超股數(不含外資自營商)",
                   "外資自營商買進股數","外資自營商賣出股數","外資自營商買賣超股數","外資及陸資-買進股數","外資及陸資-賣出股數",
                   "外資及陸資-買賣超股數","投信買進股數","投信賣出股數","投信買賣超股數","自營商買進股數(自行買賣)",
                   "自營商賣出股數(自行買賣)","自營商買賣超股數(自行買賣)","自營商買進股數(避險)","自營商賣出股數(避險)",
                   "自營商買賣超股數(避險)","自營商買進股數","自營商賣出股數","自營商買賣超股數","三大法人買賣超股數", "其他"]
header_list = ['股票代號', '名稱','外資買賣超股數', '投信買賣超股數', '自營商買賣超股數', '自營商買賣超股數(避險)', '三大法人買賣超股數']
header_str = '股票代號,日期,外資買賣超股數,投信買賣超股數,自營商買賣超股數,自營商買賣超股數(避險),三大法人買賣超股數\n'
while dt <= end_dt:
    if dt.weekday() in range(0, 5):
        roc_dt = toRocYear(dt.strftime('%Y/%m/%d'))
        tpex_resp = requests.get(tpex_url % roc_dt)
        json_obj = json.loads(tpex_resp.text)
        tpex_data = json_obj['aaData']
        tpex_pdf = None
        try:
            tpex_pdf = pd.DataFrame(tpex_data, columns=tpex_header_new)
        except:
            tpex_pdf = pd.DataFrame(tpex_data, columns=tpex_header_old)
        tpex_pdf = tpex_pdf[tpex_pdf['股票代號'].str.len() == 4]
        tpex_pdf = tpex_pdf.rename(columns={'外陸資買賣超股數(不含外資自營商)': '外資買賣超股數'})
        tpex_pdf = tpex_pdf[header_list]
        
        twse_resp = requests.get(twse_url % dt.strftime('%Y%m%d'))
        if 'data' in twse_resp.text:
            json_obj = json.loads(twse_resp.text)
            twse_data = json_obj['data']
            twse_header = json_obj['fields']
            twse_pdf = pd.DataFrame(twse_data, columns=twse_header)
            twse_pdf = twse_pdf[twse_pdf['證券代號'].str.len() == 4]
            twse_pdf = twse_pdf.rename(columns={'證券代號':'股票代號', '證券名稱':'名稱', '外陸資買賣超股數(不含外資自營商)': '外資買賣超股數'})
            twse_pdf = twse_pdf[header_list]
        else:
            dt = dt + timedelta(days=1)
            sleep(randint(3, 5))
            continue
        
        pdf = tpex_pdf.append(twse_pdf)
        tmp_pdf = pdf[pdf['三大法人買賣超股數'].isna()].copy()
        pdf.loc[pdf['三大法人買賣超股數'].isna(), '三大法人買賣超股數'] = tmp_pdf['外資買賣超股數'] + tmp_pdf['投信買賣超股數'] + tmp_pdf['自營商買賣超股數']
        pdf.loc[(pdf['自營商買賣超股數(避險)'].isna()) & (pdf['自營商買賣超股數']==0), '自營商買賣超股數(避險)'] = 0
        pdf = strToFloat(pdf, ['外資買賣超股數', '投信買賣超股數', '自營商買賣超股數', '自營商買賣超股數(避險)', '三大法人買賣超股數'])

        for row in pdf.iterrows():
            data = row[1]
            stock_no = data['股票代號']
            foreign_institution = data['外資買賣超股數']
            domestic_institution = data['投信買賣超股數']
            dealer_institution = data['自營商買賣超股數']
            dealer_hedging = data['自營商買賣超股數(避險)']
            institutional_investors = data['三大法人買賣超股數']
            row = "{},{},{},{},{},{},{}\n".format(stock_no, dt.strftime('%Y-%m-%d'), foreign_institution, 
                                        domestic_institution, dealer_institution, dealer_hedging, institutional_investors)
            file_path = output_path + os.sep + stock_no + '.csv'
            
            if os.path.exists(file_path):
                file = open(file_path, 'a')
                file.write(row)
            else:
                file = open(file_path, 'w')
                file.write(header_str)
                file.write(row)
            file.close()
        logging.info("{} is done".format(dt.strftime('%Y/%m/%d')))
    dt = dt + timedelta(days=1)
    sleep(randint(2, 4))
