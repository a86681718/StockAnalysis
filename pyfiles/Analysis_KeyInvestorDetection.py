import os
import sys
import logging
import pandas as pd
from commonlib import getConf
from datetime import datetime
from tabulate import tabulate

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
conf =  getConf()

data_path = conf.get('data.path')
common_subfolder = 'common'
mapping_filename = 'keyInvestorMapping.csv'
key_investor_path = os.sep.join([data_path, common_subfolder, mapping_filename])
key_investor_df = pd.read_csv(key_investor_path)

date_str = None
date_dash = None
if len(sys.argv) > 1:
    date_str = sys.argv[1]
    date_dash = datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d')
else:
    date_str = datetime.now().strftime('%Y%m%d')
    date_dash = datetime.now().strftime('%Y-%m-%d')
# date_str = '20210709'
# date_dash = '2021-07-09'

logging.info('date: ' + str(date_str))
bs_subfolder = 'bsReport'
ohlc_subfolder = 'ohlc'
bs_folder_path = os.sep.join([data_path, bs_subfolder, date_str])
match_count = 0
key_buy_list = []
key_sell_list = []
for row in key_investor_df.iterrows():
    stock_no = str(row[1]['股票代號'])
    key_broker_no = row[1]['券商代號']
    print(stock_no)
    
    ohlc_file_path = os.sep.join([data_path, ohlc_subfolder, stock_no + '.csv'])
    if not os.path.exists(ohlc_file_path):
        ohlc_file_path = None
        
    bs_file_path = None
    if os.path.exists(bs_folder_path + os.sep + 'twse' + os.sep + stock_no + '.csv'):
        bs_file_path = bs_folder_path + os.sep + 'twse' + os.sep + stock_no + '.csv'
    elif os.path.exists(bs_folder_path + os.sep + 'tpex' + os.sep + stock_no + '.csv'):
        bs_file_path = bs_folder_path + os.sep + 'tpex' + os.sep + stock_no + '.csv'
        
    if bs_file_path and ohlc_file_path:
        ohlc_df = pd.read_csv(ohlc_file_path)
        ohlc_df = ohlc_df[ohlc_df['日期']==date_dash]
        ohlc_record = ohlc_df.iloc[0]
        
        stock_df = pd.read_csv(bs_file_path)
        if key_broker_no in stock_df['券商'].to_list():
            key_df = stock_df[stock_df['券商']==key_broker_no]
            buy_vol = 0
            sell_vol = 0
            for key_row in key_df.iterrows():
                if float(key_row[1]['買進股數']) != 0.0:
                    buy_vol += float(key_row[1]['買進股數'])
                if float(key_row[1]['賣出股數']) != 0.0:
                    sell_vol += float(key_row[1]['賣出股數'])
            net_vol = (buy_vol - sell_vol)/1000
            if net_vol >= 10 or net_vol <= -10:
                tmp_dict = {}
                tmp_dict['股票代號'] = stock_no
                tmp_dict['股票名稱'] = row[1]['股票名稱']
                tmp_dict['券商名稱'] = row[1]['券商名稱']
                tmp_dict['漲跌幅'] = ohlc_record['漲跌幅']
                tmp_dict['買賣超比率'] = int(net_vol) / ohlc_record['成交股數'] * 1000 * 100
                tmp_dict['買進張數'] = int(buy_vol/1000)
                tmp_dict['賣出張數'] = int(sell_vol/1000)
                tmp_dict['買賣超'] = int(net_vol)
                tmp_dict['交易量比率'] = (int(buy_vol/1000) + int(sell_vol/1000)) / ohlc_record['成交股數'] * 1000 * 100
                tmp_dict['成交張數'] = ohlc_record['成交股數'] / 1000
                if net_vol >= 10:
                    key_buy_list.append(tmp_dict)
                else:
                    key_sell_list.append(tmp_dict)
                match_count += 1
key_buy_df = pd.DataFrame(key_buy_list)
key_sell_df = pd.DataFrame(key_sell_list)
#print(tabulate(key_buy_df, headers='keys', tablefmt='psql', showindex=False, floatfmt=".2f"))
#print(tabulate(key_sell_df, headers='keys', tablefmt='psql', showindex=False, floatfmt=".2f"))
#print('Total matched:', match_count)

from commonlib import txt2Img
from LineNotify import sendImg
img_path = conf.get('root.path') + os.sep + 'img'
if len(key_buy_df) > 0:
    buy_result_str = tabulate(key_buy_df.sort_values('買賣超比率', ascending=False), headers='keys', tablefmt='psql', showindex=False, floatfmt=".2f")
    img_name = f'key_buy_{date_dash}.png'
    title = f'{date_dash} 關鍵分點買超' 
    txt2Img(title, buy_result_str, img_path + os.sep + img_name)
    sendImg(img_path + os.sep + img_name, title)
    logging.info('sendImg: key_buy')


if len(key_sell_df) > 0:
    sell_result_str = tabulate(key_sell_df.sort_values('買賣超比率'), headers='keys', tablefmt='psql', showindex=False, floatfmt=".2f")
    img_name = f'key_sell_{date_dash}.png'
    title = f'{date_dash} 關鍵分點賣超' 
    txt2Img(title, sell_result_str, img_path + os.sep + img_name)
    sendImg(img_path + os.sep + img_name, title)
    logging.info('sendImg: key_sell')
