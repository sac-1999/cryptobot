import pandas as pd
import requests
from datetime import datetime, timedelta
import pytz
from cacher import persistent_cache
import dataloader

@persistent_cache(subdir="sma_indicator", non_empty=True)
def get_sma_indicators(symbol, end_time, interval, local_timezone="Asia/Kolkata"):
    """
    Fetch last 252 snapshot bars for `symbol` and compute:
    - SMA percentage-from-previous-close for preset lengths.
    - OHLC percentage changes from previous bar's same value.
    - Cross differences between previous bar's OHLC and current bar's OHLC.

    Returns only the final row (most recent bar with indicators).
    """
    lengths = [5, 7, 14, 21, 44, 63, 88, 123, 178, 252]

    # Step 1: Fetch 252 bars
    bars_df = dataloader.get_last_n_snapshot_bars(
        symbol=symbol,
        end_time=end_time,
        interval=interval,
        n_bars=252,
        local_timezone=local_timezone
    ).sort_values("timestamp").reset_index(drop=True)

    # Step 2: Compute SMA % diff from previous close
    prev_close = bars_df['close'].shift(1)
    for length in lengths:
        sma = bars_df['close'].rolling(length).mean()
        col = f'sma_{length}'
        bars_df[col] = (prev_close - sma) / prev_close

    # Step 3: Same-field % diffs (prev_x -> curr_x)
    for col in ['open', 'high', 'low', 'close']:
        prev_val = bars_df[col].shift(1)
        bars_df[f'{col}_diff'] = (bars_df[col] - prev_val) / prev_val

    # Step 4: Cross-field % diffs (prev_x -> curr_y)
    ohlc = ['open', 'high', 'low', 'close']
    for prev_col in ohlc:
        prev_val = bars_df[prev_col].shift(1)
        for curr_col in ohlc:
            bars_df[f'prev_{prev_col}_to_{curr_col}'] = (bars_df[curr_col] - prev_val) / prev_val

    return bars_df.tail(1)
