from datetime import datetime
import pandas as pd
import requests
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# download warrant info from 群益權民最大網
if len(sys.argv) > 1:
    date = sys.argv[1]
else:
    date = datetime.now().strftime('%Y-%m-%d')
url = f'https://iwarrant.capital.com.tw/wdataV2/canonical/capital-newvol/%E6%AC%8A%E8%AD%89%E9%81%94%E4%BA%BA%E5%AF%B6%E5%85%B8_NEWVOL_{date}.xls'
logging.info('GET: ' + url)
outputPath = '/userap/BuySellReport/data/warrant/'
fileName = f'warrant_raw_{date}'
extension = '.xls'
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'}
resp = requests.get(url, headers=headers)
if resp.status_code == 200:
    with open(outputPath + fileName + extension, 'wb') as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)

    # extract information to new CSV file
    pdf = pd.read_excel(outputPath + fileName + extension, sheet_name='summary', skiprows=4)
    colNames = ["權證代碼", "權證名稱", "發行券商", "權證價格", "權證漲跌", "權證漲跌幅", "權證成交量", "權證買價", "權證賣價", "權證買賣價差", "溢價比率", "價內價外", "理論價格", "隱含波動率", "有效槓桿", "剩餘天數", "最新行使比例", "標的代碼", "標的名稱", "標的價格", "標的漲跌", "標的漲跌幅", "最新履約價", "最新界限價", "標的20日波動率", "標的60日波動率", "標的120日波動率", "權證DELTA", "權證GAMMA", "權證VEGA", "權證THETA", "內含價值", "時間價值", "流通在外估計張數", "流通在外增減張數", "上市日期", "到期日期", "最新發行量", "權證發行價", "認購認售"]
    pdf.columns = colNames
    extension = '.csv'
    pdf.to_csv(outputPath + fileName.replace('_raw', '') + extension, index=False)
else:
    logging.error('status code:' + str(resp.status_code))
