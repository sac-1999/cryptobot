import pandas as pd
from datetime import datetime, timedelta
import pytz
import sma  # assuming your sma module is importable
from cacher import persistent_cache
import dataloader
import fwd_return

@persistent_cache(subdir="train_data", non_empty=True)
def train_data(symbol: str, date: datetime, interval: str, local_timezone: str = "Asia/Kolkata") -> pd.DataFrame:
    """
    Collects SMA indicator snapshots spaced by `interval` throughout a given date.
    
    Args:
        symbol (str): Trading symbol (e.g. "BTCUSDT")
        date (datetime): The date for which to collect data (date part used, time ignored).
        interval (str): Interval string understood by get_sma_indicators (e.g. "1h", "15m").
        local_timezone (str): Local timezone string.

    Returns:
        pd.DataFrame: All snapshots concatenated into a single DataFrame.
    """
    
    # Get timezone-aware start and end of the given date
    tz = pytz.timezone(local_timezone)
    day_start = tz.localize(datetime(date.year, date.month, date.day, 0, 0))
    day_end = day_start + timedelta(days=1)

    # Convert interval to timedelta
    # Convert interval to timedelta
    if interval.endswith("min"):
        value = int(interval.replace("min", ""))
        delta = timedelta(minutes=value)
    elif interval.endswith("h"):
        value = int(interval.replace("h", ""))
        delta = timedelta(hours=value)
    elif interval.endswith("d"):
        value = int(interval.replace("d", ""))
        delta = timedelta(days=value)
    else:
        raise ValueError(f"Unsupported interval format: {interval}")

    # Generate all end_time points in the day
    times = []
    current_time = day_start + delta
    while current_time <= day_end:
        times.append(current_time)
        current_time += delta

    # Collect data
    dfs = []
    for end_time in times:
        df = sma.get_sma_indicators(
            symbol=symbol,
            end_time=end_time,
            interval=interval,
            local_timezone=local_timezone
        )
        dfs.append(df)

    # Combine into single DataFrame
    result = pd.concat(dfs, ignore_index=True)
    
    fwd_dfs = []
    for ts in result["timestamp"]:
        fwd_df = fwd_return.compute(symbol=symbol, date_tm=ts, freq=interval, verbose=0)
        if not fwd_df.empty:
            fwd_dfs.append(fwd_df)

    if fwd_dfs:
        fwd_all = pd.concat(fwd_dfs, ignore_index=True)
        # Merge on timestamp
        result = result.merge(fwd_all, on="timestamp", how="left")
    else:
        result["Fwd_Ret"] = None

    return result

def create_multi_day_dataset(symbol: str, dates: list, interval: str, local_timezone: str = "Asia/Kolkata"):
    """
    Create a dataset by collecting intraday SMA indicator snapshots 
    for multiple dates for the given symbol.
    
    Parameters
    ----------
    symbol : str
        Trading symbol (e.g., 'BTCUSDT').
    dates : list of datetime.date
        List of dates for which to collect data.
    interval : str
        Interval between data points (e.g., '1m', '5m', '15m').
    local_timezone : str
        Timezone to use for data collection.

    Returns
    -------
    pd.DataFrame
        Combined DataFrame with intraday SMA features for all dates.
    """
    all_data = []

    for date in dates:
        df = train_data(
            symbol=symbol,
            date=date,
            interval=interval,
            local_timezone=local_timezone
        )
        if df is not None and not df.empty:
            all_data.append(df)

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    else:
        return pd.DataFrame()
    