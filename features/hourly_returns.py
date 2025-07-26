import dataloader
import pandas as pd
import cacher
from datetime import datetime, timedelta

@cacher.load_or_save_dataframe(subdir='hourly_return', save_type='pkl', non_empty=False)
def compute(symbol, date_tm):
    # Time is passed in Asia/kolkata time and lag is applied so we can use this in live as well :)
    fetch_date_tm = pd.to_datetime(date_tm) - timedelta(minutes=1) # lag applied
    df = dataloader.compute(symbol, fetch_date_tm, '1h')
    # Assuming df is your full DataFrame
    df = df.sort_values("timestamp").reset_index(drop=True)

    # Take last 25 rows (for 24 returns)
    last_25 = df.tail(25).reset_index(drop=True)

    # Compute returns between consecutive rows
    returns = []
    for i in range(24):
        pct = (last_25.loc[i + 1, 'close'] - last_25.loc[i, 'close']) / last_25.loc[i, 'close']
        returns.append(pct)

    # Create a DataFrame with a single row of ret_1 to ret_24
    ret_df = pd.DataFrame([returns], columns=[f'ret_{i+1}' for i in range(24)])
    ret_df['timestamp'] = pd.to_datetime(date_tm)
    ret_df['fetch_timestamp'] = fetch_date_tm
    return ret_df

