import logging
import requests
import pandas as pd
from time import sleep
from bs4 import BeautifulSoup
from datetime import datetime

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

def toAdDate(roc_dt):
    if roc_dt == '':
        return None
    else:
        roc_year = roc_dt.split('/')[0]
        ad_year = str(int(roc_year) + 1911)
        month = roc_dt.split('/')[1]
        day = roc_dt.split('/')[2]
        return '/'.join([ad_year, month, day])

result = []
type_list = ['sii', 'otc']
url = "https://mops.twse.com.tw/mops/web/ajax_t108sb27?encodeURIComponent=1&step=1&firstin=1&off=1&keyword4=&code1=&TYPEK2=&checkbtn=&queryName=&TYPEK=%s&co_id_1=&co_id_2=&year=%s&month=&b_date=&e_date=&type="
start_year = 2000
end_year = 2020
for t in type_list:
    print(t)
    for year in range(start_year, end_year+1):
        print(year)
        year = year-1911
        resp = requests.get(url % (t, year))
        soup = BeautifulSoup(resp.text)
        rows = soup.find_all("tr", class_="odd") + soup.find_all("tr", class_="even")
        if year > 104:
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
                if year > 107:
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
pdf = pd.DataFrame(result).sort_values(['股票代號', '除息交易日'], ascending=False)

new_pdf = stringReplace(pdf, pdf.columns)
new_pdf['股利所屬期間'] = new_pdf['股利所屬期間'].apply(lambda x: x.replace('\u3000', '') if '不' not in x else '')
new_pdf = strToFloat(new_pdf, ['股票股利盈餘', '股票股利公積', '現金股利盈餘', '現金股利公積', '現金增資總股數', '現金增資認股比率', '現金增資認購價', '參加分派總股數'])
new_pdf['權利分派日'] = new_pdf['權利分派日'].apply(lambda x: toAdDate(x))
new_pdf['除權交易日'] = new_pdf['除權交易日'].apply(lambda x: toAdDate(x))
new_pdf['除息交易日'] = new_pdf['除息交易日'].apply(lambda x: toAdDate(x))
new_pdf['現金股利發放日'] = new_pdf['現金股利發放日'].apply(lambda x: toAdDate(x))
new_pdf['公告時間'] = new_pdf['公告時間'].apply(lambda x: toAdDate(x))

# Output to file
columns = ['股票代號', '除權交易日', '權利分派日', '股票股利盈餘', '股票股利公積']
stock_dividend = new_pdf[~new_pdf['除權交易日'].isnull()][columns]
stock_dividend.to_csv('stock_dividend.csv', index = False)

columns = ['股票代號', '除息交易日', '現金股利發放日', '現金股利盈餘', '現金股利公積']
cash_dividend = new_pdf[~new_pdf['除息交易日'].isnull()][columns]
cash_dividend.to_csv('cash_dividend.csv', index = False)