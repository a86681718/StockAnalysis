import os
import logging
import pandas as pd
from commonlib import getConf
from datetime import datetime

def getStockList(url):
    input_df = pd.read_html(url,encoding='big5hkscs',header=0)[0]
    stock_list = []
    warrant_list = []
    stock_flag = False
    except_str = ['C', 'B', 'X', 'Y']
    for row in input_df.iterrows():
        stock_no = row[1]['有價證券代號及名稱'].split('\u3000')[0]

        if stock_no == '股票':
            stock_flag = True
            continue
        elif stock_no == '上市認購(售)權證':
            stock_flag = False
            continue
        elif stock_no == 'ETN':
            break

        if stock_flag:
            stock_list.append(stock_no[:4])
        else:
            if any(x in stock_no for x in except_str):
                pass
            else:
                warrant_list.append(stock_no[:6])
    return sorted(stock_list) + sorted(warrant_list)

def splitSpace(s):
    if '\u3000' in  s:
        return s.split('\u3000')
    elif '\u0020' in s:
        return s.split('\u0020')
    else:
        return None

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logging.info('Start time: {}'.format(datetime.now()))

conf = getConf()

twse_page = 'http://isin.twse.com.tw/isin/C_public.jsp?strMode=2'
otc_page = 'https://isin.twse.com.tw/isin/C_public.jsp?strMode=4'

twse_df = pd.read_html(twse_page, encoding='big5hkscs',header=0)[0]
start_index = twse_df.index[twse_df['有價證券代號及名稱']=='股票'][0] + 1
end_index = twse_df.index[twse_df['有價證券代號及名稱']=='上市認購(售)權證'][0]
twse_df = twse_df.iloc[start_index:end_index]

tpex_df = pd.read_html(otc_page, encoding='big5hkscs',header=0)[0]
start_index = tpex_df.index[tpex_df['有價證券代號及名稱']=='股票'][0] + 1
end_index = tpex_df.index[tpex_df['有價證券代號及名稱']=='特別股'][0]
tpex_df = tpex_df.iloc[start_index:end_index]

sub_folder = 'common'
output_path = conf.get('data.path') + os.sep + sub_folder
file_name = 'stockList.csv'
output_df = twse_df.append(tpex_df, ignore_index=True)
output_df['股票代號'] = output_df['有價證券代號及名稱'].map(lambda x: splitSpace(x)[0])
output_df['股票名稱'] = output_df['有價證券代號及名稱'].map(lambda x: splitSpace(x)[1])
output_df[['股票代號', '股票名稱', '上市日', '市場別', '產業別']].sort_values(['股票代號']).to_csv(output_path + os.sep + file_name, index=False)

logging.info('End time: {}'.format(datetime.now()))                                             