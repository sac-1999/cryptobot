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
        return pd.DataFrame(columns=['timestamp', 'Fwd_Ret'])

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
        'Fwd_Ret': fwd_ret
    }])
