import pandas as pd
import pytz
from datetime import datetime, timedelta
import cacher
import dataloader


@cacher.persistent_cache(subdir='fwd_ret')
def compute(symbol: str, date_tm: datetime, freq: str, verbose: int = 1) -> pd.DataFrame:
    """
    Calculate forward return for a symbol from `date_tm` to `date_tm + interval`.

    Args:
        symbol (str): Trading pair (e.g., 'BTCUSDT')
        date_tm (datetime | str): Start time
        freq (str): Interval (e.g., '15min', '1h', '1d')
        verbose (int): Verbosity
    """
    tz = pytz.timezone('Asia/Kolkata')

    # Ensure datetime is timezone-aware
    date_tm = pd.to_datetime(date_tm)
    if date_tm.tzinfo is None or date_tm.tzinfo.utcoffset(date_tm) is None:
        date_tm = date_tm.tz_localize(tz)
    else:
        date_tm = date_tm.tz_convert(tz)

    if verbose:
        print(f"📅 Requested time: {date_tm}, frequency: {freq}")

    # --- Snapshot at date_tm ---
    snapshot_start = dataloader.get_symbol_snapshot_bar(
        symbol=symbol,
        date_tm=date_tm,
        interval=freq,
        local_timezone='Asia/Kolkata',
    )

    if snapshot_start.empty:
        if verbose:
            print(f"⚠️ No data available for start snapshot.")
        return pd.DataFrame(columns=['timestamp', 'Fwd_Ret', 'symbol'])

    # --- Determine forward time ---
    if "min" in freq:
        delta = timedelta(minutes=int(freq.replace("min", "")))
    elif "h" in freq:
        delta = timedelta(hours=int(freq.replace("h", "")))
    elif "d" in freq:
        delta = timedelta(days=int(freq.replace("d", "")))
    else:
        raise ValueError(f"Unsupported frequency format: {freq}")

    fwd_time = date_tm + delta

    # --- Snapshot at forward time ---
    snapshot_fwd = dataloader.get_symbol_snapshot_bar(
        symbol=symbol,
        date_tm=fwd_time,
        interval=freq,
        local_timezone='Asia/Kolkata',
    )

    if snapshot_fwd.empty:
        if verbose:
            print(f"⚠️ No data available for forward snapshot.")
        return pd.DataFrame(columns=['timestamp', 'Fwd_Ret'])

    # --- Calculate forward return ---
    price_start = snapshot_start['close'].iloc[0]
    price_fwd = snapshot_fwd['close'].iloc[0]
    fwd_ret = (price_fwd - price_start) / price_start

    if verbose:
        print(f"📈 Forward return: {fwd_ret:.5f}")

    return pd.DataFrame([{
        'timestamp': snapshot_start['timestamp'].iloc[0],
        'Fwd_Ret': fwd_ret,
        'symbol' : symbol
    }])


@cacher.persistent_cache(subdir="fwd_data", non_empty=True)
def fwd_data(symbol: str, date: datetime, port_freq: str, fwd_return_interval: str, local_timezone: str = "Asia/Kolkata") -> pd.DataFrame:
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
        fwd_df = compute(symbol=symbol, date_tm=ts, freq=fwd_return_interval, verbose=0)
        if not fwd_df.empty:
            fwd_dfs.append(fwd_df)

    fwd_all = pd.concat(fwd_dfs, ignore_index=True)

    return fwd_all
