import dataloader
import pandas as pd
import cacher
from datetime import datetime, timedelta
import numpy as np

@cacher.load_or_save_pickle(subdir='hourly_return')
def compute(symbol, date_tm, freq, num_features):
    # Time is passed in Asia/Kolkata time and lag is applied
    fetch_date_tm = pd.to_datetime(date_tm) - timedelta(seconds=30)

    df = dataloader.compute(symbol, fetch_date_tm, freq)
    df = df.sort_values("timestamp").reset_index(drop=True)

    # Get last N+1 rows to calculate N pct_change values
    last_25 = df.tail(num_features + 1).reset_index(drop=True)

    # If not enough data, pad with zeros
    if len(last_25) < num_features + 1:
        # Create a dummy 'close' column filled with zeros
        needed = num_features + 1 - len(last_25)
        padding = pd.DataFrame({'close': [0.0]*needed})
        last_25 = pd.concat([padding, last_25], ignore_index=True)

    # Compute returns, then drop the first NaN
    returns_series = last_25['close'].pct_change().fillna(0)

    # Trim or pad to exactly num_features
    returns_values = returns_series.values[-num_features:]
    if len(returns_values) < num_features:
        returns_values = np.pad(returns_values, (num_features - len(returns_values), 0), 'constant')

    # Convert to single-row DataFrame
    ret_df = pd.DataFrame([returns_values], columns=[f'ret_{i+1}' for i in range(num_features)])
    ret_df['timestamp'] = pd.to_datetime(date_tm)
    ret_df['fetch_timestamp'] = fetch_date_tm

    return ret_df

# Example usage
# print(compute('BTCUSD', datetime(2025, 7, 26, 19, 20), '10min', 24))
