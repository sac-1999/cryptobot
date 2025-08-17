import pandas as pd
from datetime import datetime, timedelta
import pytz
import sma, freq_rets, comp_indicator  # assuming your sma module is importable
from cacher import persistent_cache
import dataloader
import fwd_return


@persistent_cache(subdir="label_data", non_empty=True)
def label_data(symbol: str, date: datetime, port_freq: str, label_bars: str, local_timezone: str = "Asia/Kolkata") -> pd.DataFrame:
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

    
    fwd_dfs = []
    for ts in times:
        fwd_df = fwd_return.compute(symbol=symbol, date_tm=ts, freq=label_bars, verbose=0)
        if not fwd_df.empty:
            fwd_dfs.append(fwd_df)

    fwd_all = pd.concat(fwd_dfs, ignore_index=True)

    return fwd_all.rename(columns = {'Fwd_Ret' : 'Label'})