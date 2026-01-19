import sys
from pathlib import Path

_SRC_ROOT = Path(__file__).resolve().parents[2] / "src"
if str(_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(_SRC_ROOT))
import os
import pandas as pd
from stockanalysis.commonlib import getConf

conf = getConf()

output_path = conf.get("data.path") + os.sep + "ohlc"
for file in os.listdir(output_path):
    df = pd.read_csv(output_path + os.sep + file).drop_duplicates().sort_values('日期')
    df.loc[:, '股票代號'] = df['股票代號'].apply(lambda x: str(x).zfill(4))
    df.to_csv(output_path + os.sep + file, index=False)
