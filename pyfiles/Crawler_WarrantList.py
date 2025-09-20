import os
import logging
import requests
import urllib3
import pandas as pd
from bs4 import BeautifulSoup

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

session = requests.session()
init_url = "https://mopsov.twse.com.tw/mops/web/t90sbfa01"
header = {"Content-Type": "application/x-www-form-urlencoded"}
output_path = '../data/'
warrant_info_list = []
header = []
for marketType in ["1", "2"]:
    data = {
        "encodeURIComponent": "1",
        "step": "1",
        "ver": "1.9",
        "TYPEK": "",
        "market": "1",
        "wrn_class": "all",
        "stock_no": "",
        "wrn_no": "",
        "co_id": "all",
        "wrn_type": "all",
        "left_month": "all",
        "return_rate": "all",
        "price_down": "",
        "price_up": "",
        "price_inout": "all",
        "newprice_down": "",
        "newprice_up": "",
        "fin_down": "",
        "fin_up": "",
        "sort": "1"
    }
    resp = session.post(init_url, data=data, verify=False)

    soup = BeautifulSoup(resp.text, 'html.parser')
    filename = soup.find("input", {"name":"filename"})['value']

    download_url = "https://mopsov.twse.com.tw/server-java/t105sb02"
    data["filename"] = filename
    data["firstin"] = "1"
    data["isShowForm"] = "1"
    data["pageno"] = ""
    data["pagesize"] = ""
    data["step"] = 10
    data.pop("encodeURIComponent")
    
    resp = session.post(download_url, data=data, verify=False).content
    decoded_resp = resp.decode('cp950')
    rows = decoded_resp.split("\r\n")
    for row in rows:
        if "權證代號" in row:
            header = row[1:-1].split('","')
        else:
            warrant_info_list.append(row.replace("=", "")[1:-1].split('","'))
pdf = pd.DataFrame(warrant_info_list, columns=header)
pdf.to_csv(os.path.join(output_path, "warrant_list.csv"), index=False, encoding='utf-8-sig')