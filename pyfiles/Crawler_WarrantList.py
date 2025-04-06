import os
import logging
import requests
import pandas as pd
from bs4 import BeautifulSoup
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
session = requests.session()
url = 'https://mops.twse.com.tw/mops/web/ajax_t90sbfa01'
header = {"Content-Type": "application/x-www-form-urlencoded"}
outputPath = '/userap/BuySellReport/data/common'
warrantInfoList = []
for marketType in ["1", "2"]:
    data = {"encodeURIComponent":"1",
        "step":"1",
        "ver":"1.9",
        "TYPEK":"",
        "market": marketType,
        "wrn_class":"all",
        "wrn_no":"",
        "co_id":"all",
        "wrn_type":"all",
        "left_month":"all",
        "return_rate":"all",
        "price_down":"",
        "price_up":"",
        "price_inout":"all",
        "newprice_down":"",
        "newprice_up":"",
        "fin_down":"",
        "fin_up":"",
        "sort":"1",
        "stock_no": ""}
    resp = session.post(url, data=data)
    soup = BeautifulSoup(resp.text, 'html.parser')
    table = soup.find_all('table')[1]
    rows = [row for row in table.find_all('tr') if row.attrs['class'] != ['tblHead']]
    for row in rows:
        cells = row.find_all('td')
        data = {}
        data['權證代號'] = cells[0].text
        data['權證名稱'] = cells[1].text
        data['履約形式'] = cells[2].text
        data['認購認售'] = cells[3].text
        data['發行人'] = cells[4].text
        data['上市日期'] = cells[7].text
        data['最後交易日'] = cells[8].text
        data['到期日'] = cells[9].text
        data['股票代號'] = cells[12].text
        data['股票名稱'] = cells[13].text
        data['價內外'] = cells[19].text
        warrantInfoList.append(data)
logging.info(f'Warrant list: {len(warrantInfoList)}')
warrantDf = pd.DataFrame(warrantInfoList)
warrantDf.sort_values(['股票代號', '權證代號']).to_csv(outputPath + os.sep + 'warrantList.csv', index=False)
