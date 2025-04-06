import os
import logging
import requests
import pandas as pd
from time import sleep
from commonlib import getConf, strToFloat, toAdDate
from bs4 import BeautifulSoup
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
conf = getConf()

def stringReplace(pdf, str_columns):
    copy_pdf = pdf.copy()
    for col in str_columns:
        copy_pdf[col] = copy_pdf[col].str.replace(u'\xa0', u'')
    return copy_pdf

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

result = []
type_list = ['sii', 'otc']
url = "https://mops.twse.com.tw/mops/web/ajax_t108sb27?encodeURIComponent=1&step=1&firstin=1&off=1&keyword4=&code1=&TYPEK2=&checkbtn=&queryName=&TYPEK=%s&co_id_1=&co_id_2=&year=%s&month=%s&b_date=&e_date=&type="
now_date = datetime.now()
now_year = now_date.year - 1911
now_month = str(now_date.month).zfill(2)
for t in type_list:
    logging.info(f'{t}-{now_year}/{now_month}')
    resp = requests.get(url % (t, now_year, now_month))
    logging.info(url % (t, now_year, now_month))
    logging.info('Status code: ' + str(resp.status_code))
    soup = BeautifulSoup(resp.text, 'html.parser')
    rows = soup.find_all("tr", class_="odd") + soup.find_all("tr", class_="even")
    logging.info('len(rows): ' + str(len(rows)))
    if now_year > 104: 
        for row in rows:
            cells = row.find_all("td")
            data = dict()
            data['股票代號'] = cells[0].text
            data['股利所屬期間'] = cells[2].text
            data['權利分派日'] = cells[3].text
            data['股票股利盈餘'] = cells[4].text
            data['股票股利公積'] = cells[5].text
            data['除權交易日'] = cells[6].text
            data['現金股利盈餘'] = cells[7].text
            data['現金股利公積'] = cells[8].text
            data['除息交易日'] = cells[9].text
            data['現金股利發放日'] = cells[10].text
            data['現金增資總股數'] = cells[11].text
            data['現金增資認股比率'] = cells[12].text
            data['現金增資認購價'] = cells[13].text
            if now_year > 107:
                data['參加分派總股數'] = cells[14].text
                data['公告時間'] = cells[15].text + ' ' + cells[16].text
            else:
                data['公告時間'] = cells[14].text + ' ' + cells[15].text
            result.append(data)
    else:
        for row in rows:
            cells = row.find_all("td")
            data = dict()
            data['股票代號'] = cells[0].text
            data['股利所屬期間'] = cells[2].text
            data['權利分派日'] = cells[3].text
            data['股票股利盈餘'] = cells[4].text
            data['股票股利公積'] = cells[5].text
            data['除權交易日'] = cells[6].text
            data['現金股利盈餘'] = cells[11].text
            data['現金股利公積'] = cells[12].text
            data['除息交易日'] = cells[13].text
            data['現金股利發放日'] = cells[14].text
            data['現金增資總股數'] = cells[16].text
            data['現金增資認股比率'] = cells[17].text
            data['現金增資認購價'] = cells[18].text
            data['參加分派總股數'] = ''
            data['公告時間'] = cells[20].text + ' ' + cells[21].text
            result.append(data)
    sleep(3)
logging.info(len(result))

if len(result) > 0:
    pdf = pd.DataFrame(result).sort_values(['股票代號', '除息交易日'], ascending=False)

    # data processing 
    new_pdf = stringReplace(pdf, pdf.columns)
    new_pdf['股利所屬期間'] = new_pdf['股利所屬期間'].apply(lambda x: x.replace('\u3000', '') if '不' not in x else '')
    new_pdf = strToFloat(new_pdf, ['股票股利盈餘', '股票股利公積', '現金股利盈餘', '現金股利公積', '現金增資總股數', '現金增資認股比率', '現金增資認購價', '參加分派總股數'])
    new_pdf['權利分派日'] = new_pdf['權利分派日'].apply(lambda x: toAdDate(x))
    new_pdf['除權交易日'] = new_pdf['除權交易日'].apply(lambda x: toAdDate(x))
    new_pdf['除息交易日'] = new_pdf['除息交易日'].apply(lambda x: toAdDate(x))
    new_pdf['現金股利發放日'] = new_pdf['現金股利發放日'].apply(lambda x: toAdDate(x))
    new_pdf['公告時間'] = new_pdf['公告時間'].apply(lambda x: toAdDate(x))

    # Output to file
    dividend_path = conf.get('data.path') + os.sep + 'dividend'
    columns = ['股票代號', '除權交易日', '權利分派日', '股票股利盈餘', '股票股利公積', '公告時間']
    stock_dividend = new_pdf[~new_pdf['除權交易日'].isnull()][columns]
    stock_dividend['年份'] = stock_dividend['除權交易日'].map(lambda x: x.split('/')[0])
    for year in stock_dividend['年份'].unique():
        stock_dividend_file_name = f'stock_dividend_{year}.csv'
        old_stock_dividend_file = os.sep.join([dividend_path, 'stock', stock_dividend_file_name])
        old_stock_dividend = None
        if os.path.exists(old_stock_dividend_file):
            old_stock_dividend = pd.read_csv(old_stock_dividend_file)
            old_stock_dividend['股票代號'] = old_stock_dividend['股票代號'].map(lambda x: str(x))
        new_stock_dividend = stock_dividend[stock_dividend['年份']==year].drop('年份', 1)
        new_stock_dividend = pd.concat([old_stock_dividend, new_stock_dividend], axis=0).drop_duplicates()
        new_stock_dividend.to_csv(os.sep.join([dividend_path, 'stock', stock_dividend_file_name]), index=False)

    columns = ['股票代號', '除息交易日', '現金股利發放日', '現金股利盈餘', '現金股利公積', '公告時間']
    cash_dividend = new_pdf[~new_pdf['除息交易日'].isnull()][columns]
    cash_dividend['年份'] = cash_dividend['除息交易日'].map(lambda x: x.split('/')[0])
    for year in cash_dividend['年份'].unique():
        cash_dividend_file_name = f'cash_dividend_{year}.csv'
        old_cash_dividend_file = os.sep.join([dividend_path, 'cash', cash_dividend_file_name])
        old_cash_dividend = None
        if os.path.exists(old_cash_dividend_file):
            old_cash_dividend = pd.read_csv(old_cash_dividend_file)
            old_cash_dividend['股票代號'] = old_cash_dividend['股票代號'].map(lambda x: str(x))
        new_cash_dividend = cash_dividend[cash_dividend['年份']==year].drop('年份', 1)
        new_cash_dividend = pd.concat([old_cash_dividend, new_cash_dividend], axis=0).drop_duplicates()
        new_cash_dividend.to_csv(os.sep.join([dividend_path, 'cash', cash_dividend_file_name]), index = False)
