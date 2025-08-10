import datetime
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")

import sys
sys.path.append('/home/debjitparia/projects/cryptobot/features')
import dataset, calendar_util

start_date = pd.to_datetime('2022-01-10')
end_date = pd.to_datetime('2025-02-28')
freq = '1h'
model_train_freq = 'weekly'
train_lookback_days = 252
feature_cols = ['sma_5',
 'sma_7',
 'sma_14',
 'sma_21',
 'sma_44',
 'sma_63',
 'sma_88',
 'sma_123',
 'sma_178',
 'sma_252',
 'open_diff',
 'high_diff',
 'low_diff',
 'close_diff',
 'prev_open_to_open',
 'prev_open_to_high',
 'prev_open_to_low',
 'prev_open_to_close',
 'prev_high_to_open',
 'prev_high_to_high',
 'prev_high_to_low',
 'prev_high_to_close',
 'prev_low_to_open',
 'prev_low_to_high',
 'prev_low_to_low',
 'prev_low_to_close',
 'prev_close_to_open',
 'prev_close_to_high',
 'prev_close_to_low',
 'prev_close_to_close',
 'ret_1',
 'ret_2',
 'ret_3',
 'ret_4',
 'ret_5',
 'ret_6',
 'ret_7',
 'ret_8',
 'ret_9',
 'ret_10',
 'ret_11',
 'ret_12',
 'ret_13',
 'ret_14',
 'ret_15',
 'ret_16',
 'ret_17',
 'ret_18',
 'ret_19',
 'ret_20',
 'ret_21',
 'ret_22',
 'ret_23',
 'ret_24',
 'ret_25',
 'ret_26',
 'ret_27',
 'ret_28',
 'ret_29',
 'ret_30',
 'ema_7',
 'ema_14',
 'ema_21',
 'ema_44',
 'ema_50',
 'ema_63',
 'ema_100',
 'ema_132',
 'ema_200',
 'ema_256',
 'supertrend_10_3',
 'supertrend_15_1',
 'supertrend_10_2',
 'supertrend_8_2',
 'supertrend_8_3',
 'vwap',
 'rsi_14',
 'macd']
label_col = 'Fwd_Ret'

symbols = ['BTCUSDT', 'ETHUSDT']

import pandas as pd
from datetime import datetime
from joblib import Parallel, delayed
import dataset  # your dataset.py with train_data()
import calendar_util  # for get_training_dates

def _train_data_task(symbol, date, interval, local_timezone):
    """
    Helper task to call train_data safely.
    """
    try:
        df = dataset.train_data(symbol, date, interval, local_timezone)
        if df is not None and not df.empty:
            return df
    except Exception as e:
        print(f"[ERROR] train_data failed for {symbol} on {date}: {e}")
    return None

def create_multi_day_dataset(symbols, dates, interval, local_timezone="Asia/Kolkata", nproc=4):
    """
    Create combined dataset for multiple symbols and dates in parallel using joblib.

    Parameters
    ----------
    symbols : str or list[str]
        Single symbol or list of trading symbols.
    dates : pd.DatetimeIndex, list, or list-like of dates
        Dates to collect data for.
    interval : str
        Interval string, e.g. '15min', '30min'.
    local_timezone : str
        Timezone string.
    nproc : int
        Number of parallel workers.

    Returns
    -------
    pd.DataFrame
        Combined DataFrame of all training data.
    """
    # Normalize symbols and dates to lists
    if hasattr(dates, "__iter__") and not isinstance(dates, (str, bytes)):
        dates = [d.date() for d in pd.to_datetime(dates)]
    else:
        dates = [pd.to_datetime(dates).date()]
    if isinstance(symbols, str):
        symbols = [symbols]
    if isinstance(dates, (pd.DatetimeIndex, list, tuple)):
        dates = list(pd.to_datetime(dates).date)
    else:
        # single date input
        dates = [pd.to_datetime(dates).date()]


    # Create all (symbol, date) pairs
    tasks = [(symbol, date, interval, local_timezone) for symbol in symbols for date in dates]

    results = Parallel(n_jobs=nproc, backend='loky')(
        delayed(_train_data_task)(symbol, date, interval, local_timezone) for symbol, date, interval, local_timezone in tasks
    )

    # Filter out None or empty results
    results = [df for df in results if df is not None and not df.empty]

    if not results:
        print("[WARN] No data collected for any symbol-date pair.")
        return pd.DataFrame()

    combined_df = pd.concat(results, ignore_index=True)
    print(f"[INFO] Combined dataset contains {len(combined_df)} rows from {len(symbols)} symbols and {len(dates)} dates.")
    return combined_df


def get_train_data(df, target_date, lookback_days, lag=1, model_train_freq="weekly"):
    """
    Slice training data from df based on lookback window and lag.

    Parameters
    ----------
    df : pd.DataFrame
        The full dataset containing a 'timestamp' column.
    target_date : datetime or str
        The date to predict for (exclusive from training set).
    lookback_days : int
        How many days to look back for training data.
    lag : int
        Gap (in days) between the last training date and target date.
    model_train_freq : str
        Frequency passed to `calendar_util.get_training_dates`.

    Returns
    -------
    pd.DataFrame
        The filtered training dataset.
    """
    target_date = pd.to_datetime(target_date).date()
    df['date'] = pd.to_datetime(df['timestamp']).dt.date

    training_dates = calendar_util.get_training_dates(
        target_date, lookback_days, lag=lag, frequency=model_train_freq
    )
    return df[df['date'].isin(training_dates)].copy()

import pandas as pd

# Create a list of dates (date objects)
dates = pd.date_range(start=start_date, end=end_date, freq='D').date

# Now call with list of dates
dataset_df = create_multi_day_dataset(symbols, dates, freq, nproc=8)