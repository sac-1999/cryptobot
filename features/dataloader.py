import requests
import time
from datetime import datetime, timedelta
import pandas as pd
import pytz
import cacher
import json


def get_unix_timestamp(dt):
    return int(time.mktime(dt.timetuple()))

def load_historical_data(date_tm, symbol, freq = '1m'):
    date_tm = pd.to_datetime(date_tm)
    start_unix = get_unix_timestamp(date_tm- timedelta(1)) 
    end_unix = get_unix_timestamp(date_tm)

    url = 'https://api.india.delta.exchange/v2/history/candles'
    params = {
        'resolution': freq,
        'symbol': symbol,
        'start': start_unix,
        'end': end_unix
    }
    headers = {'Accept': 'application/json'}
    response = requests.get(url, params=params, headers=headers)

    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data['result'])
        df['time'] = pd.to_datetime(df['time'], unit='s',  utc=True).dt.tz_convert(pytz.timezone('Asia/Kolkata'))
        df = df[::-1]
        if df.isna().any().any():
            raise(f"DataFrame contains nan's Please check... \n {df}")
        return df
    else:
        raise ConnectionError(f"Failed to fetch data: {response.status_code} - {response.text}")
    
@cacher.load_or_save_dataframe(subdir='data', save_type='pkl', non_empty=False)
def compute(symbol, date_tm, freq):
    # When calling this function give lag on your own of minimim 1min, and I pass date_tm in Asia/kolkata time.
    data = load_historical_data(date_tm, symbol, freq = '1m')
    offset = '0min'
    data.rename(columns={'time': 'timestamp'}, inplace=True)
    data.index = pd.to_datetime(data['timestamp'])
    data = data.resample(freq, offset = offset).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    })
    data.reset_index(inplace=True)
    return data
