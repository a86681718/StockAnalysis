# +----------------------+
# |	Broker List Download |
# +----------------------+

import requests
import pandas as pd
from io import BytesIO

output_path = 'broker_list.csv'
url = 'https://www.twse.com.tw/rwd/zh/brokerService/outPutExcel'
headers = { 'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36' }
session = requests.Session()
response = session.get(url, headers=headers)
response = session.get(url)
pd.read_excel(BytesIO(response.content), engine='xlrd').to_csv(output_path, index=False)