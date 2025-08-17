import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
import dataloader
import cacher
from multiprocessing import Pool


@cacher.persistent_cache(subdir='hourly_return')
def compute(symbol, date_tm, freq, num_features, local_timezone="Asia/Kolkata"):
    """
    Compute the last `num_features` returns for a given symbol ending at `date_tm`.
    Uses your current dataloader.get_last_n_snapshot_bars().
    """
    # Ensure timestamp is timezone-aware
    tz = pytz.timezone(local_timezone)
    if date_tm.tzinfo is None:
        date_tm = tz.localize(date_tm)
    else:
        date_tm = date_tm.astimezone(tz)

    # Fetch last N+1 bars to compute N returns
    last_bars = dataloader.get_last_n_snapshot_bars(
        symbol=symbol,
        end_time=date_tm,
        interval=freq,
        n_bars=num_features + 1,
        local_timezone=local_timezone
    )

    last_bars = last_bars.sort_values("timestamp").reset_index(drop=True)

    # If not enough bars, pad with zeros
    if len(last_bars) < num_features + 1:
        needed = (num_features + 1) - len(last_bars)
        padding = pd.DataFrame({'close': [0.0] * needed})
        last_bars = pd.concat([padding, last_bars], ignore_index=True)

    # Compute returns
    returns_series = last_bars['close'].pct_change().fillna(0)

    # Take only last num_features
    returns_values = returns_series.values[-num_features:]
    if len(returns_values) < num_features:
        returns_values = np.pad(returns_values, (num_features - len(returns_values), 0), 'constant')

    # Build single-row DataFrame
    ret_df = pd.DataFrame([returns_values], columns=[f'ret_{i+1}' for i in range(num_features)])
    ret_df['symbol'] = symbol
    ret_df['timestamp'] = pd.to_datetime(date_tm)

    return ret_df
