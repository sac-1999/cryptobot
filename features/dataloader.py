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
    
@cacher.load_or_save_pickle(subdir='minute_data',  )
def get(symbol, date_tm):
    # When calling this function give lag on your own of minimim 1min, and I pass date_tm in Asia/kolkata time.
    from datetime import datetime
    import pytz

    now = datetime.now(pytz.timezone('Asia/Kolkata'))
    input_time = pd.to_datetime(date_tm)

    if input_time >= now:
        raise ValueError(
            f"[Forward Bias Detected] ❌ Attempted to fetch data for future timestamp.\n"
            f"Current time: {now.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"Requested time: {input_time.strftime('%Y-%m-%d %H:%M:%S')}"
        )
    data = load_historical_data(date_tm, symbol, freq = '1m')
    data.rename(columns={'time': 'timestamp'}, inplace=True)
    return data

from datetime import datetime
import pandas as pd
import pytz

def fix_timezone(dt, tz_str='Asia/Kolkata'):
    """
    Ensures that the input datetime is timezone-aware.
    If it is naive, it is localized to the specified timezone.
    If it is already aware, it is converted to the target timezone.

    Args:
        dt (datetime or str): Input datetime (can be naive or aware, or a string)
        tz_str (str): Timezone string (e.g., 'Asia/Kolkata')

    Returns:
        datetime: Timezone-aware datetime in the specified timezone
    """
    # Convert string input to datetime
    if isinstance(dt, str):
        dt = pd.to_datetime(dt)

    tz = pytz.timezone(tz_str)

    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        # Naive datetime — localize
        return tz.localize(dt)
    else:
        # Aware datetime — convert to target timezone
        return dt.astimezone(tz)


@cacher.load_or_save_pickle(subdir='data',  )
def compute(symbol, date_tm, freq):
    date_tm = fix_timezone(date_tm)
    #Data not lagged
    df = get(symbol, date_tm)
    # Rename and set index
    # df.rename(columns={'time': 'timestamp'}, inplace=True)
    df['timestamp'] = pd.to_datetime(df['timestamp'])  # Ensures tz-aware
    df.set_index('timestamp', inplace=True)
    print(df)
    # Resample safely
    df_resampled = df.resample(freq).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna().reset_index()

    return df_resampled
# print(compute('BTCUSD', datetime(2025, 7, 26, 19, 30, 59), '10min'))