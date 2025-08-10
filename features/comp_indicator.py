import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import cacher
from indicators import Indicators
import dataloader


def multiday_data(symbol, enddate, freq, local_timezone="Asia/Kolkata"):
    """
    Get at least 252 candles ending at enddate for a given symbol & frequency.
    """
    totalcandles = 0
    dflist = []

    # Ensure datetime
    enddate = pd.to_datetime(enddate)

    while totalcandles < 252:
        # Fetch as many bars as available up to enddate
        df = dataloader.get_last_n_snapshot_bars(
            symbol=symbol,
            end_time=enddate,
            interval=freq,
            n_bars=252,  # fetch in chunks, no limit
            local_timezone=local_timezone
        )

        if df is not None and not df.empty:
            # Ensure sorted by timestamp ascending
            df = df.sort_values("timestamp").reset_index(drop=True)
            dflist.insert(0, df)
            totalcandles += len(df)

        # Go back one day
        enddate -= timedelta(days=1)

        # Safety stop
        if enddate.year <= 2020:
            break

    if not dflist:
        return None

    return pd.concat(dflist).drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)


@cacher.persistent_cache(subdir="comp_indicator", non_empty=True)
def compute(symbol, date_tm, freq, local_timezone="Asia/Kolkata"):
    """
    Compute technical indicators snapshot for a given symbol and datetime.
    """
    # Just before the snapshot
    fetch_date_tm = pd.to_datetime(date_tm) - timedelta(seconds=30)

    df = multiday_data(symbol, fetch_date_tm, freq, local_timezone=local_timezone)
    if df is None or df.empty:
        return None

    df.reset_index(drop=True, inplace=True)

    # Technical indicators
    ema_list = [7, 14, 21, 44, 50, 63, 100, 132, 200, 256]
    for ema in ema_list:
        df = Indicators.ema(df, ema)

    for length, multi in [(10, 3), (15, 1), (10, 2), (8, 2), (8, 3)]:
        df = Indicators.supertrend(df, length, multi)

    df = Indicators.vwap(df)
    df = Indicators.rsi(df, 14)
    df = Indicators.macd(df)

    # Drop incomplete rows
    df = df.dropna()
    print(df.columns)
    # Normalize indicator columns relative to close, except main OHLCV + RSI
    maincolumns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    for col in ['ema_7', 'ema_14',
       'ema_21', 'ema_44', 'ema_50', 'ema_63', 'ema_100', 'ema_132', 'ema_200',
       'ema_256', 'supertrend_10_3', 'supertrend_15_1', 'supertrend_10_2',
       'supertrend_8_2', 'supertrend_8_3', 'vwap', 'rsi_14', 'macd']:
        if col in maincolumns or 'rsi' in col or 'time' in col:
            continue
        df[col] = (df[col] - df['close']) / df['close']

    # Keep only the last row (snapshot)
    snapshot = df.tail(1).copy()
    snapshot.reset_index(drop=True, inplace=True)
    snapshot['timestamp'] = pd.to_datetime(date_tm)

    return snapshot[['timestamp','ema_7', 'ema_14',
       'ema_21', 'ema_44', 'ema_50', 'ema_63', 'ema_100', 'ema_132', 'ema_200',
       'ema_256', 'supertrend_10_3', 'supertrend_15_1', 'supertrend_10_2',
       'supertrend_8_2', 'supertrend_8_3', 'vwap', 'rsi_14', 'macd']]

# print(compute('BTCUSDT', datetime(2025, 4, 23, 15, 30), '30min', local_timezone="Asia/Kolkata"))