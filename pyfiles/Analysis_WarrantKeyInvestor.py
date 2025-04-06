import os
import logging
import pandas as pd
import multiprocessing as mp
from tabulate import tabulate
from commonlib import getConf, strToFloat
from datetime import datetime, timedelta

def read_csv(filename):
    return pd.read_csv(filename)

conf = getConf()
data_path = conf.get('data.path')
common_subfolder = 'common'
bs_subfolder = 'bsReport'

stock_df = pd.read_csv(data_path + os.sep + common_subfolder + os.sep + 'stockList.csv')
stock_df = stock_df[['股票代號', '股票名稱', '市場別']]
stock_df = stock_df.rename(columns={'股票名稱': '標的名稱'})
stock_df.loc[:, '股票代號'] = stock_df['股票代號'].apply(lambda x: str(x))

broker_df = pd.read_csv(data_path + os.sep + common_subfolder + os.sep + 'brokerList.csv')
broker_df.columns = ['券商', '券商名稱', '開業日', '地址', '電話']
broker_df = broker_df[['券商', '券商名稱']]

cur_dt = datetime.now()
days = 14
warrant_df_list = []
use_columns = ['標的名稱', '認購認售', '權證代碼', '權證成交量', '權證價格']
for i in range(days):
    data_dt = cur_dt - timedelta(days=i)
    file_path = os.sep.join([data_path, 'warrant', 'warrant_%s.csv' % data_dt.strftime('%Y-%m-%d')])
    if os.path.exists(file_path):
        tmp_df = pd.read_csv(file_path, usecols=use_columns)
        tmp_df.loc[:, '日期'] = data_dt
        warrant_df_list.append(tmp_df)
        print(data_dt)
warrant_df = pd.concat(warrant_df_list, axis=0, ignore_index=True, sort=False).drop_duplicates()
warrant_not_zero_df = warrant_df[(warrant_df['權證成交量']>0)].copy()
warrant_not_zero_df.loc[:, '成交金額'] = warrant_not_zero_df['權證成交量'] * warrant_not_zero_df['權證價格']
warrant_top100_df = warrant_not_zero_df[['標的名稱', '認購認售', '權證代碼', '成交金額']] \
                                .groupby(['標的名稱','認購認售']) \
                                .agg({'成交金額': sum, '權證代碼': list}) \
                                .reset_index().sort_values('成交金額', ascending=False).head(100)
warrant_top100_df = pd.merge(warrant_top100_df, stock_df, on='標的名稱')

top_buyer_list = []
for row in warrant_top100_df.iterrows():
    stock_name = row[1]['標的名稱']
    stock_no = row[1]['股票代號']
    warrant_type = row[1]['認購認售']
    warrant_list = row[1]['權證代碼']
    market_type = 'twse' if row[1]['市場別']=='上市' else 'tpex'
    file_list = []
    print(stock_no, warrant_type, len(warrant_list))
    count = 0
    for warrant_no in warrant_list:
        for d in range(days):
            data_dt = cur_dt - timedelta(days=d)
            file_path = os.sep.join([data_path, bs_subfolder, data_dt.strftime('%Y%m%d'), market_type, warrant_no + '.csv'])
            if os.path.exists(file_path):
                count += 1
                file_list.append(file_path)

    with mp.Pool(processes=2) as pool:
        df_list = pool.map(read_csv, file_list)
        concat_df = pd.concat(df_list, ignore_index=True, sort=False).drop_duplicates()
        concat_df = strToFloat(concat_df.dropna().copy(), ['買進股數', '賣出股數'])

        buy_df = concat_df[concat_df['買進股數'] > 0].copy()
        buy_df.loc[:, '買進金額'] = buy_df['價格'] * buy_df['買進股數']
        buy_df = buy_df.loc[:, ['券商', '買進金額', '買進股數']].groupby("券商").sum().reset_index()
        buy_df.loc[:, '買進均價'] = buy_df['買進金額'] / buy_df['買進股數']
        buy_df = buy_df.dropna()

        sell_df = concat_df[concat_df['賣出股數'] > 0].copy()
        sell_df.loc[:, '賣出金額'] = sell_df['價格'] * sell_df['賣出股數']
        sell_df = sell_df.loc[:, ['券商', '賣出金額', '賣出股數']].groupby("券商").sum().reset_index()
        sell_df.loc[:, '賣出均價'] = sell_df['賣出金額'] / sell_df['賣出股數']
        sell_df = sell_df.dropna()

        merge_df = pd.merge(buy_df, sell_df, on='券商', how='outer').fillna(0.0)
    agg_df = pd.merge(merge_df, broker_df, on='券商')
    agg_df = agg_df[['券商名稱', '買進金額', '買進股數', '賣出金額', '賣出股數']].groupby('券商名稱').sum().reset_index()
    agg_df.loc[:, '買賣超金額'] = (agg_df['買進金額'] - agg_df['賣出金額'])
    total = agg_df['買進金額'].sum()
    agg_df.loc[:, '買超比率'] = agg_df['買賣超金額']/total*100
    agg_df.loc[:, '股票名稱'] = stock_name
    agg_df.loc[:, '股票代號'] = stock_no
    output_cols = ['股票代號', '股票名稱', '券商名稱', '買進金額', '買進股數', '賣出金額', '賣出股數', '買賣超金額', '買超比率']
    top_buyer_list.append(agg_df[output_cols].sort_values('買超比率', ascending=False).head(3))

result_df = pd.concat(top_buyer_list, ignore_index=True, sort=False).drop_duplicates()
result_df = result_df[(result_df['券商名稱'].str.contains('-'))&((result_df['買超比率']>3)|(result_df['買賣超金額']>4000000))]
result_df = result_df.sort_values(['買超比率'], ascending=False)
#.to_csv('warrant_key_buyer.csv', index=False)

from commonlib import txt2Img
from LineNotify import sendImg
img_path = conf.get('root.path') + os.sep + 'img'
today = datetime.now().strftime('%Y-%m-%d')
if len(result_df) > 0:
    result_str = tabulate(result_df, headers='keys', tablefmt='psql', showindex=False, floatfmt=".2f")
    img_name = f'warrant_key_investor_{today}.png'
    title = f'{today} 波段權證'
    txt2Img(title, result_str, img_path + os.sep + img_name)
    sendImg(img_path + os.sep + img_name, title)
    logging.info('sendImg: warrant_key_investor')
