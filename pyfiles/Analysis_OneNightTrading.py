import os
import sys
import logging
import pandas as pd
from tabulate import tabulate
from datetime import datetime
from commonlib import getConf, getTick, strToFloat

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

conf = getConf()
data_path = conf.get('data.path')

date_str = None
date_dash = None
if len(sys.argv) > 1:
    date_str = sys.argv[1]
    date_dash = datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d')
else:
    date_str = datetime.now().strftime('%Y%m%d')
    date_dash = datetime.now().strftime('%Y-%m-%d')

parent_path = data_path + os.sep + 'bsReport' + os.sep + date_str
small_df_list = []
large_df_list = []
file_list = [f for f in os.listdir(parent_path + os.sep + 'tpex') + os.listdir(parent_path + os.sep + 'twse') if len(f)==8]
for file in file_list:
    if not os.path.exists(data_path + os.sep + 'ohlc' + os.sep + file):
        continue
    ohlc_df = pd.read_csv(data_path + os.sep + 'ohlc' + os.sep + file)
    
    ohlc_df = ohlc_df[ohlc_df['日期']==date_dash]
    if not len(ohlc_df):
        continue
    ohlc_record = ohlc_df.iloc[0]
    
    if ohlc_record['漲跌幅'] > 9.45 and ohlc_record['成交金額'] > 10000000 : # 假設大漲
        stock_df = None
        if os.path.exists(parent_path + os.sep + 'tpex' + os.sep + file):
            stock_df = pd.read_csv(parent_path + os.sep + 'tpex' + os.sep + file)
        elif os.path.exists(parent_path + os.sep + 'twse' + os.sep + file):
            stock_df = pd.read_csv(parent_path + os.sep + 'twse' + os.sep + file)
        elif not stock_df:
            continue
        stock_df = strToFloat(stock_df.dropna().copy(), ['買進股數', '賣出股數'])
        
        buy_df = stock_df[stock_df['買進股數'] > 0].copy()
        if type(buy_df['價格']) != float or type(buy_df['買進股數']) != float:
            print(file)
        buy_df.loc[:, '買進金額'] = buy_df['價格'] * buy_df['買進股數']
        buy_df = buy_df.loc[:, ['券商', '買進金額', '買進股數']].groupby("券商").sum().reset_index()
        buy_df.loc[:, '買進均價'] = buy_df['買進金額'] / buy_df['買進股數']
        buy_df = buy_df.dropna()

        sell_df = stock_df[stock_df['賣出股數'] > 0].copy()
        sell_df.loc[:, '賣出金額'] = sell_df['價格'] * sell_df['賣出股數']
        sell_df = sell_df.loc[:, ['券商', '賣出金額', '賣出股數']].groupby("券商").sum().reset_index()
        sell_df.loc[:, '賣出均價'] = sell_df['賣出金額'] / sell_df['賣出股數']
        sell_df = sell_df.dropna()
        
        merge_df = pd.merge(buy_df, sell_df, on='券商', how='outer').fillna(0.0)
        total_volume = merge_df['買進股數'].sum()
        merge_df['買超股數'] = merge_df['買進股數'] - merge_df['賣出股數']
        merge_df['買超張數'] = merge_df['買進股數'] / 1000
        merge_df['買超比率'] = merge_df['買超股數'] / total_volume * 100
        merge_df.loc[:, '股票代號'] = file.replace('.csv', '')
        merge_df.loc[:, '成交量'] = ohlc_record['成交股數']/1000
        merge_df.loc[:, '成交金額(萬)'] = ohlc_record['成交金額']/10000
        merge_df.loc[:, '開盤價'] = ohlc_record['開盤價']
        merge_df.loc[:, '最高價'] = ohlc_record['最高價']
        merge_df.loc[:, '最低價'] = ohlc_record['最低價']
        merge_df.loc[:, '收盤價'] = ohlc_record['收盤價']
        merge_df.loc[:, '漲跌幅'] = ohlc_record['漲跌幅']
        
        high_price = ohlc_record['最高價'] - getTick(ohlc_record['最高價'])
        if file == '3372.csv':
            print(merge_df.sort_values('買超比率', ascending=False).head(10)[['券商','買超比率', '買進股數']])
        # 累積小量買超券商(>5%), 且買均價大於
        small_df = merge_df[(merge_df['買超比率']>1.5) & (merge_df['買進均價']>=high_price)]
        if len(small_df) and small_df['買超比率'].sum() > 5 and len(merge_df['買超比率']>5)==0:
            small_df_list.append(small_df)
            
        # 單一大量買超券商(>5%), 且買均價大於
        
        large_df = merge_df[(merge_df['買超比率']>5) & (merge_df['買進均價']>=high_price)]
        if len(large_df):
            large_df_list.append(large_df)
        
    else: # 假設沒大漲
        pass
    
stock_info_df = pd.read_csv(data_path + os.sep + 'common' + os.sep + 'stockList.csv')
stock_info_df = stock_info_df[['股票代號', '股票名稱']]
stock_info_df.loc[:, '股票代號'] = stock_info_df['股票代號'].apply(lambda x: str(x))

broker_df = pd.read_csv(data_path + os.sep + 'common' + os.sep + 'brokerList.csv')
broker_df.columns = ['券商', '券商名稱', '開業日', '地址', '電話']
broker_df = broker_df[['券商', '券商名稱']]

result_df = pd.concat(large_df_list+small_df_list, axis=0, ignore_index=True, sort=False).drop_duplicates()
result_df = pd.merge(result_df, stock_info_df, on='股票代號')
result_df = pd.merge(result_df, broker_df, on='券商')
result_df = result_df[['股票代號', '股票名稱', '券商名稱', '買超張數', '買超比率', '買進均價', '收盤價', '成交量', '成交金額(萬)', '開盤價', '漲跌幅', '最高價', '最低價']]

from commonlib import txt2Img
from LineNotify import sendImg
img_path = conf.get('root.path') + os.sep + 'img'
today = datetime.now().strftime('%Y-%m-%d')
if len(result_df) > 0:
    result_str = tabulate(result_df.sort_values(['成交金額(萬)', '買超比率'], ascending=False), headers='keys', tablefmt='psql', showindex=False, floatfmt=".2f")
    img_name = f'one_night_trading_{today}.png'
    title = f'{today} 隔日沖標的' 
    txt2Img(title, result_str, img_path + os.sep + img_name)
    sendImg(img_path + os.sep + img_name, title)
    logging.info('sendImg: one_night_trading')
