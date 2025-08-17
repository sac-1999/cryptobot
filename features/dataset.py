import pandas as pd
from datetime import datetime, timedelta
import pytz
import sma, freq_rets, comp_indicator  # assuming your sma module is importable
from cacher import persistent_cache
import dataloader
import fwd_return

def _single_timestamp(symbol, end_time, port_freq, local_timezone, num_return_features=30):
    """
    Collects SMA, return-based, and technical indicator features for a single snapshot time.
    
    Parameters
    ----------
    symbol : str
        Trading symbol, e.g. 'BTCUSDT'
    end_time : datetime
        The time at which to take the snapshot (timezone-aware or naive in local_timezone)
    port_freq : str
        Interval string (e.g. '1h', '15min')
    local_timezone : str
        Timezone string
    num_return_features : int
        Number of past returns to compute as features

    Returns
    -------
    pd.DataFrame
        Single-row DataFrame with merged features
    """
    # 1. SMA features
    sma_df = sma.get_sma_indicators(
        symbol=symbol,
        end_time=end_time,
        interval=port_freq,
        local_timezone=local_timezone
    )

    if sma_df is None or sma_df.empty:
        return pd.DataFrame()

    # Ensure timestamp column is datetime
    sma_df['timestamp'] = pd.to_datetime(sma_df['timestamp'])

    # 2. Return-based features
    ret_df = freq_rets.compute(
        symbol=symbol,
        date_tm=end_time,
        freq=port_freq,
        num_features=num_return_features,
        local_timezone=local_timezone
    )

    # 3. Technical indicators
    tech_df = comp_indicator.compute(
        symbol=symbol,
        date_tm=end_time,
        freq=port_freq,
        local_timezone=local_timezone
    )

    # Merge all features on timestamp
    merged = sma_df
    if ret_df is not None and not ret_df.empty:
        merged = merged.merge(ret_df, on=["timestamp", 'symbol'], how="left")
    if tech_df is not None and not tech_df.empty:
        merged = merged.merge(tech_df, on=["timestamp", 'symbol'], how="left")

    return merged

@persistent_cache(subdir="train_data", non_empty=True)
def train_data(symbol: str, date: datetime, port_freq: str, feature_interval:str, local_timezone: str = "Asia/Kolkata") -> pd.DataFrame:
    """
    Collects SMA indicator snapshots spaced by `port_freq` throughout a given date.
    
    Args:
        symbol (str): Trading symbol (e.g. "BTCUSDT")
        date (datetime): The date for which to collect data (date part used, time ignored).
        port_freq (str): Interval string understood by get_sma_indicators (e.g. "1h", "15m").
        local_timezone (str): Local timezone string.

    Returns:
        pd.DataFrame: All snapshots concatenated into a single DataFrame.
    """
    
    # Get timezone-aware start and end of the given date
    tz = pytz.timezone(local_timezone)
    day_start = tz.localize(datetime(date.year, date.month, date.day, 0, 0))
    day_end = day_start + timedelta(days=1)

    # Convert port_freq to timedelta
    # Convert port_freq to timedelta
    if port_freq.endswith("min"):
        value = int(port_freq.replace("min", ""))
        delta = timedelta(minutes=value)
    elif port_freq.endswith("h"):
        value = int(port_freq.replace("h", ""))
        delta = timedelta(hours=value)
    elif port_freq.endswith("d"):
        value = int(port_freq.replace("d", ""))
        delta = timedelta(days=value)
    else:
        raise ValueError(f"Unsupported port_freq format: {port_freq}")

    # Generate all end_time points in the day
    times = []
    current_time = day_start + delta
    while current_time <= day_end:
        times.append(current_time)
        current_time += delta

    # Collect data
    dfs = []
    for end_time in times:
        df = _single_timestamp(symbol, end_time, feature_interval, local_timezone)
        dfs.append(df)

    # Combine into single DataFrame
    result = pd.concat(dfs, ignore_index=True)

    return result