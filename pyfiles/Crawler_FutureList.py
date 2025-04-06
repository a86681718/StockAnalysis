import os
import logging
import requests
import pandas as pd
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
session = requests.session()
url = 'https://www.taifex.com.tw/cht/2/stockLists'
resp = session.get(url)
logging.info(f'GET status code: {resp.status_code}')
soup = BeautifulSoup(resp.text, 'html.parser')
table = soup.find_all('table')[1]
rows = table.find_all('tr')
outputPath = '/userap/BuySellReport/data/common'
futureInfoList = []
for row in rows:
    cells = row.find_all('td')
    if len(cells) == 0 or cells[0].text == "":
        continue
    data = {}
    data['商品代號'] = cells[0].text
    data['股票代號'] = cells[2].text
    data['股票名稱'] = cells[3].text
    data['期貨'] = cells[4].text.replace('\r', '').replace('\n', '').replace('\t', '')
    data['選擇權'] = cells[5].text.replace('\r', '').replace('\n', '').replace('\t', '')
    futureInfoList.append(data)
logging.info(f'Future list: {len(futureInfoList)}')
futureDf = pd.DataFrame(futureInfoList)
futureDf.sort_values('股票代號').to_csv(outputPath + os.sep + 'futureList.csv', index=False)
