import os
import sys
import json
import requests
import pandas as pd
from datetime import datetime, timedelta

data_folder_name = '/Users/fang/StockAnalysis/data'
subfolder_name = 'ohlc'
time_format = "%Y%m%d"

# 確保資料夾存在
data_folder_path = os.path.join(data_folder_name, subfolder_name)
os.makedirs(data_folder_path, exist_ok=True)

# 從資料夾中取得已存在的檔案，並解析日期
existing_files = [f for f in os.listdir(data_folder_path) if f.startswith("twse-") and f.endswith(".csv")]
if existing_files:
    # 取得檔案中的最新日期
    existing_dates = [datetime.strptime(f.split('-')[1].split('.')[0], time_format) for f in existing_files]
    start_dt = max(existing_dates)
else:
    # 如果沒有檔案，則預設為前一天
    start_dt = datetime.now() - timedelta(days=1)

end_dt = datetime.now()

num_days = (end_dt - start_dt).days
date_list = [start_dt + timedelta(days=x) for x in range(num_days)]
for d in date_list:
    date_str = d.strftime("%Y%m%d")
    print(date_str)
    url = f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={date_str}&type=ALL&response=json"
    resp = requests.get(url)
    twse_result = resp.content.decode('utf8')
    try:
        twse_result_json = json.loads(twse_result)
        if 'tables' not in twse_result_json:
            continue
        tables = twse_result_json['tables']
        
        for table in tables:
            for k in table.keys():
                if '每日收盤行情' in table['title']:
                    fields = table['fields']
                    data = table['data']
        
        df = pd.DataFrame(data=data, columns=fields)
        df.to_csv(os.sep.join([data_folder_name, subfolder_name, f"twse-{date_str}.csv"]), index=False)
    except Exception as e:
        raise e