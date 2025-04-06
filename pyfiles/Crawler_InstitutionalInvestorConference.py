import os
import logging
import requests
import pandas as pd
from time import sleep
from bs4 import BeautifulSoup
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def genYearMonthList(start_ym, end_ym):
    ym_list = []
    start_year = int(start_ym.split('/')[0])
    start_month = int(start_ym.split('/')[1])
    end_year = int(end_ym.split('/')[0])
    end_month = int(end_ym.split('/')[1])
    for y in range(start_year, end_year+1):
        if y == start_year:
            for m in range(start_month, 13):
                ym_list.append(str(y) + "/" + str(m).zfill(2))
        elif y == end_year:
            for m in range(1, end_month+1):
                ym_list.append(str(y) + "/" + str(m).zfill(2))
        else:
            for m in range(1, 13):
                ym_list.append(str(y) + "/" + str(m).zfill(2))
    return ym_list

result = []
type_list = ['sii', 'otc']
headers = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'}
url = 'https://mops.twse.com.tw/mops/web/ajax_t100sb02_1?encodeURIComponent=1&step=1&firstin=1&off=1&TYPEK=%s&year=%s&month=%s&co_id='
now_date = datetime.now()
now_year = now_date.year - 1911
now_month = now_date.month
for t in type_list:
    for i in range(3):
        month = str(now_month + i).zfill(2)
        logging.info(f'{t}-{now_year}/{month}')
        resp = requests.get(url % (t, now_year, month), headers=headers)
        soup = BeautifulSoup(resp.text, 'html.parser')
        rows = soup.find_all("tr", class_="odd") + soup.find_all("tr", class_="even")
        for row in rows:
            cells = row.find_all("td")
            data = dict()
            data['股票代號'] = cells[0].text
            dt = cells[2].text.split(" ")[0].split("/")
            dt[0] = str(int(dt[0]) + 1911)
            data['法說會日期'] = "/".join(dt)
            data['地點'] = cells[4].text
            data['擇要訊息'] = cells[5].text
            data['中文檔案'] = cells[6].text
            data['英文檔案'] = cells[7].text
            result.append(data)
pdf = pd.DataFrame(result).sort_values(['股票代號', '法說會日期'], ascending=False)

root_path = '/userap/BuySellReport/data/institutionalInvestorConference/'
pdf['年份'] = pdf['法說會日期'].map(lambda x: x.split('/')[0])
for year in pdf['年份'].unique():
    file_name = f'institutionalInvestorConference_{year}.csv'
    tmp_pdf = pdf[pdf['年份']==str(year)].drop('年份', 1)
    if os.path.exists(root_path + file_name):
        old_pdf = pd.read_csv(root_path + file_name)
        old_pdf['股票代號'] = old_pdf['股票代號'].map(lambda x: str(x))
        new_pdf = old_pdf.append(tmp_pdf).drop_duplicates()
        new_pdf.sort_values(['法說會日期', '股票代號']).to_csv(root_path + file_name, index=False)
    else:
        tmp_pdf.sort_values(['法說會日期', '股票代號']).to_csv(root_path + file_name, index=False)
    logging.info(f'output {file_name}')
