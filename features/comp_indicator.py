import dataloader
import pandas as pd
import cacher
from datetime import datetime, timedelta
import numpy as np

from indicators import Indicators
import dataloader
import pandas as pd
import cacher
from datetime import datetime, timedelta
import numpy as np

def multiday_data(symbol, enddate, freq):
    totalcandles = 0
    dflist = []
    while(totalcandles < 252):
        df = dataloader.compute(symbol, enddate, freq)
        if df is not None:
            dflist.insert(0,df)
            totalcandles = totalcandles + len(df)
        enddate = enddate - timedelta(1)
        if enddate.year == 2020:
            return None
    return pd.concat(dflist)

@cacher.load_or_save_pickle(subdir='comp_indicators')
def compute(symbol, date_tm, freq):
    fetch_date_tm = pd.to_datetime(date_tm) - timedelta(seconds=30)
    df = multiday_data(symbol, fetch_date_tm, freq)
    df.reset_index(drop = True, inplace = True)
    df = df.sort_values("timestamp").reset_index(drop=True)
    ema_list = [7,14,21,44,50,63,100, 132, 200, 256]
    for ema in ema_list:
        df = Indicators.ema(df, ema)
    for length, multi in [(10,3), (15,1), (10, 2), (8,2), (8,3)]:
        df = Indicators.supertrend(df, length, multi)
    df = Indicators.vwap(df)
    df = Indicators.rsi(df, 14)
    df = Indicators.macd(df)
    df = df.dropna()
    maincolumns = ['timestamp'	,'open',	'high',	'low', 'close',	'volume']	
    for col in df.columns:
        if col in maincolumns or 'rsi' in col:
            continue
        df[col] = (df[col] - df['close'])/ df['close']
    
    df = df.tail(1)
    df.reset_index(drop = True, inplace = True)
    df['timestamp'] = pd.to_datetime(date_tm)
    return df.drop(columns = ['open',	'high',	'low', 'close',	'volume'])